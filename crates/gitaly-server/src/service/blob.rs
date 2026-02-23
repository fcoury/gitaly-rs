use std::collections::HashSet;
use std::path::{Component, Path, PathBuf};
use std::pin::Pin;
use std::sync::Arc;

use tokio::process::Command;
use tokio_stream::Stream;
use tonic::{Request, Response, Status};

use gitaly_proto::gitaly::blob_service_server::BlobService;
use gitaly_proto::gitaly::list_blobs_response::Blob as ListedBlob;
use gitaly_proto::gitaly::*;

use crate::dependencies::Dependencies;

type ServiceStream<T> = Pin<Box<dyn Stream<Item = Result<T, Status>> + Send + 'static>>;

#[derive(Debug, Clone)]
pub struct BlobServiceImpl {
    dependencies: Arc<Dependencies>,
}

impl BlobServiceImpl {
    #[must_use]
    pub fn new(dependencies: Arc<Dependencies>) -> Self {
        Self { dependencies }
    }

    fn resolve_repo_path(&self, repository: Option<Repository>) -> Result<PathBuf, Status> {
        let repository =
            repository.ok_or_else(|| Status::invalid_argument("repository is required"))?;

        if repository.storage_name.trim().is_empty() {
            return Err(Status::invalid_argument(
                "repository.storage_name is required",
            ));
        }

        if repository.relative_path.trim().is_empty() {
            return Err(Status::invalid_argument(
                "repository.relative_path is required",
            ));
        }

        validate_relative_path(&repository.relative_path)?;

        let storage_root = self
            .dependencies
            .storage_paths
            .get(&repository.storage_name)
            .ok_or_else(|| {
                Status::not_found(format!(
                    "storage `{}` is not configured",
                    repository.storage_name
                ))
            })?;

        Ok(storage_root.join(repository.relative_path))
    }
}

fn validate_relative_path(relative_path: &str) -> Result<(), Status> {
    let path = Path::new(relative_path);
    if path.is_absolute() {
        return Err(Status::invalid_argument(
            "repository.relative_path must be relative",
        ));
    }

    for component in path.components() {
        match component {
            Component::Normal(_) => {}
            _ => {
                return Err(Status::invalid_argument(
                    "repository.relative_path contains disallowed path components",
                ))
            }
        }
    }

    Ok(())
}

fn parse_output_lines(stdout: &[u8]) -> Vec<String> {
    String::from_utf8_lossy(stdout)
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .map(ToOwned::to_owned)
        .collect()
}

fn build_stream_response<T>(response: T) -> Response<ServiceStream<T>>
where
    T: Send + 'static,
{
    Response::new(Box::pin(tokio_stream::iter(vec![Ok(response)])))
}

fn build_stream_from_items<T>(items: Vec<T>) -> Response<ServiceStream<T>>
where
    T: Send + 'static,
{
    Response::new(Box::pin(tokio_stream::iter(items.into_iter().map(Ok))))
}

fn unimplemented_blob_rpc(method: &'static str) -> Status {
    Status::unimplemented(format!("BlobService::{method} is not implemented"))
}

fn decode_utf8(field: &'static str, bytes: Vec<u8>) -> Result<String, Status> {
    String::from_utf8(bytes)
        .map_err(|_| Status::invalid_argument(format!("{field} must be valid UTF-8")))
}

async fn git_output<I, S>(repo_path: &Path, args: I) -> Result<std::process::Output, Status>
where
    I: IntoIterator<Item = S>,
    S: AsRef<str>,
{
    let mut command = Command::new("git");
    command.arg("-C").arg(repo_path);
    for arg in args {
        command.arg(arg.as_ref());
    }

    command
        .output()
        .await
        .map_err(|err| Status::internal(format!("failed to execute git command: {err}")))
}

fn status_for_git_failure(args: &str, output: &std::process::Output) -> Status {
    Status::internal(format!(
        "git command failed: git {args}; stderr: {}",
        String::from_utf8_lossy(&output.stderr).trim()
    ))
}

fn git_stderr_contains(output: &std::process::Output, pattern: &str) -> bool {
    String::from_utf8_lossy(&output.stderr)
        .to_ascii_lowercase()
        .contains(&pattern.to_ascii_lowercase())
}

fn is_missing_object_error(output: &std::process::Output) -> bool {
    git_stderr_contains(output, "not a valid object name")
        || git_stderr_contains(output, "bad object")
        || git_stderr_contains(output, "invalid object")
        || git_stderr_contains(output, "unknown revision")
}

fn is_invalid_revision_error(output: &std::process::Output) -> bool {
    git_stderr_contains(output, "unknown revision")
        || git_stderr_contains(output, "bad revision")
        || git_stderr_contains(output, "invalid revision")
}

fn object_type_from_git(value: &str) -> ObjectType {
    match value {
        "blob" => ObjectType::Blob,
        "tree" => ObjectType::Tree,
        "commit" => ObjectType::Commit,
        "tag" => ObjectType::Tag,
        _ => ObjectType::Unknown,
    }
}

async fn git_resolve_object(repo_path: &Path, spec: &str) -> Result<Option<String>, Status> {
    let output = git_output(repo_path, ["rev-parse", "--verify", spec]).await?;
    if output.status.success() {
        let oid = String::from_utf8_lossy(&output.stdout).trim().to_string();
        if oid.is_empty() {
            return Ok(None);
        }

        return Ok(Some(oid));
    }

    if is_missing_object_error(&output) || is_invalid_revision_error(&output) {
        return Ok(None);
    }

    Err(status_for_git_failure(
        "-C <repo> rev-parse --verify <spec>",
        &output,
    ))
}

async fn git_cat_file_type(repo_path: &Path, oid: &str) -> Result<String, Status> {
    let output = git_output(repo_path, ["cat-file", "-t", oid]).await?;
    if !output.status.success() {
        if is_missing_object_error(&output) {
            return Err(Status::not_found(format!("object `{oid}` not found")));
        }

        return Err(status_for_git_failure(
            "-C <repo> cat-file -t <oid>",
            &output,
        ));
    }

    Ok(String::from_utf8_lossy(&output.stdout).trim().to_string())
}

async fn git_cat_file_size(repo_path: &Path, oid: &str) -> Result<i64, Status> {
    let output = git_output(repo_path, ["cat-file", "-s", oid]).await?;
    if !output.status.success() {
        if is_missing_object_error(&output) {
            return Err(Status::not_found(format!("object `{oid}` not found")));
        }

        return Err(status_for_git_failure(
            "-C <repo> cat-file -s <oid>",
            &output,
        ));
    }

    let size = String::from_utf8_lossy(&output.stdout)
        .trim()
        .parse::<i64>()
        .map_err(|err| Status::internal(format!("failed to parse object size: {err}")))?;

    Ok(size)
}

async fn git_cat_file_data(repo_path: &Path, oid: &str, limit: i64) -> Result<Vec<u8>, Status> {
    if limit == 0 {
        return Ok(Vec::new());
    }

    let output = git_output(repo_path, ["cat-file", "-p", oid]).await?;
    if !output.status.success() {
        if is_missing_object_error(&output) {
            return Err(Status::not_found(format!("object `{oid}` not found")));
        }

        return Err(status_for_git_failure(
            "-C <repo> cat-file -p <oid>",
            &output,
        ));
    }

    let mut data = output.stdout;
    if limit > 0 {
        let max_len = usize::try_from(limit).unwrap_or(usize::MAX);
        if data.len() > max_len {
            data.truncate(max_len);
        }
    }

    Ok(data)
}

async fn git_mode_for_revision_path(
    repo_path: &Path,
    revision: &str,
    path: &str,
) -> Result<i32, Status> {
    if path.is_empty() {
        return Ok(0);
    }

    let output = git_output(repo_path, ["ls-tree", revision, "--", path]).await?;
    if !output.status.success() {
        if is_missing_object_error(&output) || is_invalid_revision_error(&output) {
            return Ok(0);
        }

        return Err(status_for_git_failure(
            "-C <repo> ls-tree <revision> -- <path>",
            &output,
        ));
    }

    let stdout_text = String::from_utf8_lossy(&output.stdout);
    let line = stdout_text
        .lines()
        .find(|line| !line.trim().is_empty())
        .unwrap_or_default();

    if line.is_empty() {
        return Ok(0);
    }

    let mode = line
        .split_whitespace()
        .next()
        .and_then(|token| i32::from_str_radix(token, 8).ok())
        .unwrap_or(0);

    Ok(mode)
}

#[tonic::async_trait]
impl BlobService for BlobServiceImpl {
    type GetBlobStream = ServiceStream<GetBlobResponse>;
    type GetBlobsStream = ServiceStream<GetBlobsResponse>;
    type ListBlobsStream = ServiceStream<ListBlobsResponse>;
    type ListAllBlobsStream = ServiceStream<ListAllBlobsResponse>;
    type GetLFSPointersStream = ServiceStream<GetLfsPointersResponse>;
    type ListLFSPointersStream = ServiceStream<ListLfsPointersResponse>;
    type ListAllLFSPointersStream = ServiceStream<ListAllLfsPointersResponse>;

    async fn get_blob(
        &self,
        request: Request<GetBlobRequest>,
    ) -> Result<Response<Self::GetBlobStream>, Status> {
        let request = request.into_inner();
        if request.oid.trim().is_empty() {
            return Err(Status::invalid_argument("oid is required"));
        }

        if request.limit < -1 {
            return Err(Status::invalid_argument("limit must be >= -1"));
        }

        let repo_path = self.resolve_repo_path(request.repository)?;
        if !repo_path.exists() {
            return Err(Status::not_found("repository does not exist"));
        }

        let object_type = git_cat_file_type(&repo_path, &request.oid).await?;
        if object_type != "blob" {
            return Err(Status::not_found(format!(
                "blob `{}` not found",
                request.oid
            )));
        }

        let size = git_cat_file_size(&repo_path, &request.oid).await?;
        let data = git_cat_file_data(&repo_path, &request.oid, request.limit).await?;

        Ok(build_stream_response(GetBlobResponse {
            size,
            data,
            oid: request.oid,
        }))
    }

    async fn get_blobs(
        &self,
        request: Request<GetBlobsRequest>,
    ) -> Result<Response<Self::GetBlobsStream>, Status> {
        let request = request.into_inner();
        if request.revision_paths.is_empty() {
            return Err(Status::invalid_argument(
                "revision_paths must contain at least one entry",
            ));
        }

        if request.limit < -1 {
            return Err(Status::invalid_argument("limit must be >= -1"));
        }

        let repo_path = self.resolve_repo_path(request.repository)?;
        if !repo_path.exists() {
            return Err(Status::not_found("repository does not exist"));
        }

        let mut responses = Vec::with_capacity(request.revision_paths.len());

        for revision_path in request.revision_paths {
            if revision_path.revision.trim().is_empty() {
                return Err(Status::invalid_argument(
                    "revision_paths.revision must not be empty",
                ));
            }

            let path_string = decode_utf8("revision_paths.path", revision_path.path.clone())?;
            let spec = if path_string.is_empty() {
                revision_path.revision.clone()
            } else {
                format!("{}:{}", revision_path.revision, path_string)
            };

            let oid = git_resolve_object(&repo_path, &spec).await?;

            let Some(oid) = oid else {
                responses.push(GetBlobsResponse {
                    revision: revision_path.revision,
                    path: revision_path.path,
                    ..GetBlobsResponse::default()
                });
                continue;
            };

            let object_type_name = git_cat_file_type(&repo_path, &oid).await?;
            let object_type = object_type_from_git(&object_type_name);
            let size = git_cat_file_size(&repo_path, &oid).await?;
            let mode =
                git_mode_for_revision_path(&repo_path, &revision_path.revision, &path_string)
                    .await?;
            let data = git_cat_file_data(&repo_path, &oid, request.limit).await?;

            responses.push(GetBlobsResponse {
                size,
                data,
                oid,
                is_submodule: object_type == ObjectType::Commit && mode == 0o160000,
                mode,
                revision: revision_path.revision,
                path: revision_path.path,
                r#type: object_type as i32,
            });
        }

        Ok(build_stream_from_items(responses))
    }

    async fn list_blobs(
        &self,
        request: Request<ListBlobsRequest>,
    ) -> Result<Response<Self::ListBlobsStream>, Status> {
        let request = request.into_inner();
        if request.revisions.is_empty() {
            return Err(Status::invalid_argument(
                "revisions must contain at least one revision",
            ));
        }

        if request.bytes_limit < -1 {
            return Err(Status::invalid_argument("bytes_limit must be >= -1"));
        }

        for revision in &request.revisions {
            if revision.trim().is_empty() {
                return Err(Status::invalid_argument(
                    "revisions must not contain empty revisions",
                ));
            }

            if revision.starts_with('-') && revision != "--all" && revision != "--not" {
                return Err(Status::invalid_argument(
                    "revisions must not start with '-' unless using --all or --not",
                ));
            }
        }

        let repo_path = self.resolve_repo_path(request.repository)?;
        if !repo_path.exists() {
            return Err(Status::not_found("repository does not exist"));
        }

        let mut args = vec!["rev-list".to_string(), "--objects".to_string()];
        args.extend(request.revisions);

        let rev_list_output = git_output(&repo_path, args.iter().map(String::as_str)).await?;
        if !rev_list_output.status.success() {
            if is_invalid_revision_error(&rev_list_output)
                || is_missing_object_error(&rev_list_output)
            {
                return Err(Status::invalid_argument(
                    "one or more revisions could not be resolved",
                ));
            }

            return Err(status_for_git_failure(
                "-C <repo> rev-list --objects",
                &rev_list_output,
            ));
        }

        let mut seen = HashSet::new();
        let mut blobs = Vec::new();
        let limit = usize::try_from(request.limit).unwrap_or(usize::MAX);

        for line in parse_output_lines(&rev_list_output.stdout) {
            let (oid, path) = if let Some((oid, path)) = line.split_once(' ') {
                (oid.trim(), Some(path))
            } else {
                (line.trim(), None)
            };

            if oid.is_empty() || !seen.insert(oid.to_string()) {
                continue;
            }

            let object_type = git_cat_file_type(&repo_path, oid).await?;
            if object_type != "blob" {
                continue;
            }

            let size = git_cat_file_size(&repo_path, oid).await?;
            let data = git_cat_file_data(&repo_path, oid, request.bytes_limit).await?;
            let path = if request.with_paths {
                path.unwrap_or_default().as_bytes().to_vec()
            } else {
                Vec::new()
            };

            blobs.push(ListedBlob {
                oid: oid.to_string(),
                size,
                data,
                path,
            });

            if limit > 0 && blobs.len() >= limit {
                break;
            }
        }

        Ok(build_stream_response(ListBlobsResponse { blobs }))
    }

    async fn list_all_blobs(
        &self,
        _request: Request<ListAllBlobsRequest>,
    ) -> Result<Response<Self::ListAllBlobsStream>, Status> {
        Err(unimplemented_blob_rpc("list_all_blobs"))
    }

    async fn get_lfs_pointers(
        &self,
        _request: Request<GetLfsPointersRequest>,
    ) -> Result<Response<Self::GetLFSPointersStream>, Status> {
        Err(unimplemented_blob_rpc("get_lfs_pointers"))
    }

    async fn list_lfs_pointers(
        &self,
        _request: Request<ListLfsPointersRequest>,
    ) -> Result<Response<Self::ListLFSPointersStream>, Status> {
        Err(unimplemented_blob_rpc("list_lfs_pointers"))
    }

    async fn list_all_lfs_pointers(
        &self,
        _request: Request<ListAllLfsPointersRequest>,
    ) -> Result<Response<Self::ListAllLFSPointersStream>, Status> {
        Err(unimplemented_blob_rpc("list_all_lfs_pointers"))
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::process::Command;
    use std::sync::Arc;
    use std::time::{SystemTime, UNIX_EPOCH};

    use tokio_stream::StreamExt;
    use tonic::{Code, Request};

    use gitaly_proto::gitaly::blob_service_server::BlobService;
    use gitaly_proto::gitaly::get_blobs_request::RevisionPath;
    use gitaly_proto::gitaly::{
        GetBlobRequest, GetBlobsRequest, ListBlobsRequest, ObjectType, Repository,
    };

    use crate::dependencies::Dependencies;

    use super::BlobServiceImpl;

    fn unique_dir(name: &str) -> std::path::PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock drift")
            .as_nanos();
        std::env::temp_dir().join(format!("gitaly-rs-{name}-{nanos}"))
    }

    fn run_git(repo_path: &std::path::Path, args: &[&str]) {
        let output = Command::new("git")
            .arg("-C")
            .arg(repo_path)
            .args(args)
            .output()
            .expect("git should execute");
        assert!(
            output.status.success(),
            "git -C {} {} failed\nstdout: {}\nstderr: {}",
            repo_path.display(),
            args.join(" "),
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
    }

    fn run_git_stdout(repo_path: &std::path::Path, args: &[&str]) -> String {
        let output = Command::new("git")
            .arg("-C")
            .arg(repo_path)
            .args(args)
            .output()
            .expect("git should execute");
        assert!(
            output.status.success(),
            "git -C {} {} failed\nstdout: {}\nstderr: {}",
            repo_path.display(),
            args.join(" "),
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );

        String::from_utf8(output.stdout)
            .expect("stdout should be UTF-8")
            .trim()
            .to_string()
    }

    struct TestRepo {
        storage_root: std::path::PathBuf,
        repository: Repository,
        service: BlobServiceImpl,
        blob_oid: String,
        blob_data: Vec<u8>,
    }

    impl TestRepo {
        fn setup(name: &str) -> Self {
            let storage_root = unique_dir(name);
            let repo_path = storage_root.join("project.git");

            std::fs::create_dir_all(&storage_root).expect("storage root should be creatable");
            std::fs::create_dir_all(&repo_path).expect("repo path should be creatable");

            run_git(&repo_path, &["init", "--quiet"]);
            run_git(&repo_path, &["config", "user.name", "Blob Service Tests"]);
            run_git(
                &repo_path,
                &["config", "user.email", "blob-service-tests@example.com"],
            );

            let blob_data = b"hello from blob service\n".to_vec();
            std::fs::write(repo_path.join("README.md"), &blob_data)
                .expect("README should be writable");

            run_git(&repo_path, &["add", "README.md"]);
            run_git(&repo_path, &["commit", "--quiet", "-m", "initial"]);

            let blob_oid = run_git_stdout(&repo_path, &["rev-parse", "HEAD:README.md"]);

            let mut storage_paths = HashMap::new();
            storage_paths.insert("default".to_string(), storage_root.clone());

            let service = BlobServiceImpl::new(Arc::new(
                Dependencies::default().with_storage_paths(storage_paths),
            ));

            let repository = Repository {
                storage_name: "default".to_string(),
                relative_path: "project.git".to_string(),
                ..Repository::default()
            };

            Self {
                storage_root,
                repository,
                service,
                blob_oid,
                blob_data,
            }
        }
    }

    impl Drop for TestRepo {
        fn drop(&mut self) {
            if self.storage_root.exists() {
                let _ = std::fs::remove_dir_all(&self.storage_root);
            }
        }
    }

    #[tokio::test]
    async fn get_blob_and_list_blobs_return_committed_blob_data() {
        let test_repo = TestRepo::setup("blob-service-get-and-list");

        let mut get_blob_stream = test_repo
            .service
            .get_blob(Request::new(GetBlobRequest {
                repository: Some(test_repo.repository.clone()),
                oid: test_repo.blob_oid.clone(),
                limit: -1,
            }))
            .await
            .expect("get_blob should succeed")
            .into_inner();

        let get_blob_response = get_blob_stream
            .next()
            .await
            .expect("stream should yield one item")
            .expect("stream item should not fail");

        assert_eq!(get_blob_response.oid, test_repo.blob_oid);
        assert_eq!(
            get_blob_response.size,
            i64::try_from(test_repo.blob_data.len()).expect("blob size should fit into i64")
        );
        assert_eq!(get_blob_response.data, test_repo.blob_data);

        let mut list_blobs_stream = test_repo
            .service
            .list_blobs(Request::new(ListBlobsRequest {
                repository: Some(test_repo.repository.clone()),
                revisions: vec!["HEAD".to_string()],
                limit: 0,
                bytes_limit: -1,
                with_paths: true,
            }))
            .await
            .expect("list_blobs should succeed")
            .into_inner();

        let list_blobs_response = list_blobs_stream
            .next()
            .await
            .expect("stream should yield one item")
            .expect("stream item should not fail");

        assert!(list_blobs_response.blobs.iter().any(|blob| {
            blob.oid == test_repo.blob_oid
                && blob.data == test_repo.blob_data
                && blob.path == b"README.md"
        }));
    }

    #[tokio::test]
    async fn get_blobs_resolves_revision_path_and_returns_blob_entry() {
        let test_repo = TestRepo::setup("blob-service-get-blobs");

        let mut stream = test_repo
            .service
            .get_blobs(Request::new(GetBlobsRequest {
                repository: Some(test_repo.repository.clone()),
                revision_paths: vec![RevisionPath {
                    revision: "HEAD".to_string(),
                    path: b"README.md".to_vec(),
                }],
                limit: -1,
            }))
            .await
            .expect("get_blobs should succeed")
            .into_inner();

        let response = stream
            .next()
            .await
            .expect("stream should yield one item")
            .expect("stream item should not fail");

        assert_eq!(response.oid, test_repo.blob_oid);
        assert_eq!(response.path, b"README.md");
        assert_eq!(response.data, test_repo.blob_data);
        assert_eq!(response.r#type, ObjectType::Blob as i32);
    }

    #[tokio::test]
    async fn get_blob_returns_sensible_status_codes_for_invalid_and_missing_inputs() {
        let test_repo = TestRepo::setup("blob-service-status-codes");

        let status = test_repo
            .service
            .get_blob(Request::new(GetBlobRequest {
                repository: Some(test_repo.repository.clone()),
                oid: String::new(),
                limit: -1,
            }))
            .await
            .err()
            .expect("empty oid should return error");
        assert_eq!(status.code(), Code::InvalidArgument);

        let status = test_repo
            .service
            .get_blob(Request::new(GetBlobRequest {
                repository: Some(test_repo.repository.clone()),
                oid: test_repo.blob_oid.clone(),
                limit: -2,
            }))
            .await
            .err()
            .expect("invalid limit should return error");
        assert_eq!(status.code(), Code::InvalidArgument);

        let status = test_repo
            .service
            .get_blob(Request::new(GetBlobRequest {
                repository: Some(test_repo.repository.clone()),
                oid: "deadbeef".to_string(),
                limit: -1,
            }))
            .await
            .err()
            .expect("unknown blob should return error");
        assert_eq!(status.code(), Code::NotFound);

        let status = test_repo
            .service
            .get_blob(Request::new(GetBlobRequest {
                repository: Some(Repository {
                    storage_name: "default".to_string(),
                    relative_path: "../escape.git".to_string(),
                    ..Repository::default()
                }),
                oid: test_repo.blob_oid.clone(),
                limit: -1,
            }))
            .await
            .err()
            .expect("invalid repository path should return error");
        assert_eq!(status.code(), Code::InvalidArgument);
    }
}
