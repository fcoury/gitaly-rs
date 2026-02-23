use std::path::{Component, Path, PathBuf};
use std::pin::Pin;
use std::sync::Arc;

use tokio::process::Command;
use tokio_stream::Stream;
use tonic::{Request, Response, Status};

use gitaly_proto::gitaly::diff_service_server::DiffService;
use gitaly_proto::gitaly::*;

use crate::dependencies::Dependencies;

type ServiceStream<T> = Pin<Box<dyn Stream<Item = Result<T, Status>> + Send + 'static>>;

const STREAM_CHUNK_SIZE: usize = 32 * 1024;

#[derive(Debug, Clone)]
pub struct DiffServiceImpl {
    dependencies: Arc<Dependencies>,
}

impl DiffServiceImpl {
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

fn validate_commit_pair(left: &str, right: &str) -> Result<(), Status> {
    if left.trim().is_empty() {
        return Err(Status::invalid_argument("left_commit_id is required"));
    }
    if right.trim().is_empty() {
        return Err(Status::invalid_argument("right_commit_id is required"));
    }
    Ok(())
}

fn stream_from_values<T>(values: Vec<T>) -> Response<ServiceStream<T>>
where
    T: Send + 'static,
{
    let items = values.into_iter().map(Ok);
    Response::new(Box::pin(tokio_stream::iter(items)))
}

fn stream_bytes<T, F>(data: Vec<u8>, mut map: F) -> Response<ServiceStream<T>>
where
    T: Send + 'static,
    F: FnMut(Vec<u8>) -> T,
{
    let chunks: Vec<T> = if data.is_empty() {
        vec![map(Vec::new())]
    } else {
        data.chunks(STREAM_CHUNK_SIZE)
            .map(|chunk| map(chunk.to_vec()))
            .collect()
    };

    stream_from_values(chunks)
}

fn parse_numstat(stdout: &[u8]) -> Vec<DiffStats> {
    String::from_utf8_lossy(stdout)
        .lines()
        .filter_map(|line| {
            let mut columns = line.splitn(3, '\t');
            let additions = parse_numstat_value(columns.next()?)?;
            let deletions = parse_numstat_value(columns.next()?)?;
            let path = columns.next()?.trim();
            if path.is_empty() {
                return None;
            }

            Some(DiffStats {
                path: path.as_bytes().to_vec(),
                additions,
                deletions,
                old_path: Vec::new(),
            })
        })
        .collect()
}

fn parse_numstat_value(value: &str) -> Option<i32> {
    if value == "-" {
        return Some(0);
    }

    value.parse::<i32>().ok()
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

fn unimplemented_diff_rpc(method: &str) -> Status {
    Status::unimplemented(format!("DiffService::{method} is not implemented"))
}

#[tonic::async_trait]
impl DiffService for DiffServiceImpl {
    type CommitDiffStream = ServiceStream<CommitDiffResponse>;
    type CommitDeltaStream = ServiceStream<CommitDeltaResponse>;
    type RawDiffStream = ServiceStream<RawDiffResponse>;
    type RawPatchStream = ServiceStream<RawPatchResponse>;
    type DiffStatsStream = ServiceStream<DiffStatsResponse>;
    type FindChangedPathsStream = ServiceStream<FindChangedPathsResponse>;
    type RawRangeDiffStream = ServiceStream<RawRangeDiffResponse>;
    type RangeDiffStream = ServiceStream<RangeDiffResponse>;
    type DiffBlobsStream = ServiceStream<DiffBlobsResponse>;

    async fn commit_diff(
        &self,
        _request: Request<CommitDiffRequest>,
    ) -> Result<Response<Self::CommitDiffStream>, Status> {
        Err(unimplemented_diff_rpc("commit_diff"))
    }

    async fn commit_delta(
        &self,
        _request: Request<CommitDeltaRequest>,
    ) -> Result<Response<Self::CommitDeltaStream>, Status> {
        Err(unimplemented_diff_rpc("commit_delta"))
    }

    async fn raw_diff(
        &self,
        request: Request<RawDiffRequest>,
    ) -> Result<Response<Self::RawDiffStream>, Status> {
        let request = request.into_inner();
        let repo_path = self.resolve_repo_path(request.repository)?;
        validate_commit_pair(&request.left_commit_id, &request.right_commit_id)?;

        let output = git_output(
            &repo_path,
            [
                "diff",
                request.left_commit_id.as_str(),
                request.right_commit_id.as_str(),
            ],
        )
        .await?;

        if !output.status.success() {
            return Err(status_for_git_failure(
                "-C <repo> diff <left> <right>",
                &output,
            ));
        }

        Ok(stream_bytes(output.stdout, |data| RawDiffResponse { data }))
    }

    async fn raw_patch(
        &self,
        request: Request<RawPatchRequest>,
    ) -> Result<Response<Self::RawPatchStream>, Status> {
        let request = request.into_inner();
        let repo_path = self.resolve_repo_path(request.repository)?;
        validate_commit_pair(&request.left_commit_id, &request.right_commit_id)?;

        let commit_range = format!("{}..{}", request.left_commit_id, request.right_commit_id);
        let output = git_output(
            &repo_path,
            ["format-patch", "--stdout", commit_range.as_str()],
        )
        .await?;

        if !output.status.success() {
            return Err(status_for_git_failure(
                "-C <repo> format-patch --stdout <left>..<right>",
                &output,
            ));
        }

        Ok(stream_bytes(output.stdout, |data| RawPatchResponse {
            data,
        }))
    }

    async fn diff_stats(
        &self,
        request: Request<DiffStatsRequest>,
    ) -> Result<Response<Self::DiffStatsStream>, Status> {
        let request = request.into_inner();
        let repo_path = self.resolve_repo_path(request.repository)?;
        validate_commit_pair(&request.left_commit_id, &request.right_commit_id)?;

        let output = git_output(
            &repo_path,
            [
                "diff",
                "--numstat",
                request.left_commit_id.as_str(),
                request.right_commit_id.as_str(),
            ],
        )
        .await?;

        if !output.status.success() {
            return Err(status_for_git_failure(
                "-C <repo> diff --numstat <left> <right>",
                &output,
            ));
        }

        Ok(stream_from_values(vec![DiffStatsResponse {
            stats: parse_numstat(&output.stdout),
        }]))
    }

    async fn find_changed_paths(
        &self,
        _request: Request<FindChangedPathsRequest>,
    ) -> Result<Response<Self::FindChangedPathsStream>, Status> {
        Err(unimplemented_diff_rpc("find_changed_paths"))
    }

    async fn get_patch_id(
        &self,
        _request: Request<GetPatchIdRequest>,
    ) -> Result<Response<GetPatchIdResponse>, Status> {
        Err(unimplemented_diff_rpc("get_patch_id"))
    }

    async fn raw_range_diff(
        &self,
        _request: Request<RawRangeDiffRequest>,
    ) -> Result<Response<Self::RawRangeDiffStream>, Status> {
        Err(unimplemented_diff_rpc("raw_range_diff"))
    }

    async fn range_diff(
        &self,
        _request: Request<RangeDiffRequest>,
    ) -> Result<Response<Self::RangeDiffStream>, Status> {
        Err(unimplemented_diff_rpc("range_diff"))
    }

    async fn diff_blobs(
        &self,
        _request: Request<DiffBlobsRequest>,
    ) -> Result<Response<Self::DiffBlobsStream>, Status> {
        Err(unimplemented_diff_rpc("diff_blobs"))
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::process::Command;
    use std::sync::Arc;
    use std::time::{SystemTime, UNIX_EPOCH};

    use tokio_stream::StreamExt;
    use tonic::Request;

    use gitaly_proto::gitaly::diff_service_server::DiffService;
    use gitaly_proto::gitaly::{DiffStatsRequest, RawDiffRequest, RawPatchRequest, Repository};

    use crate::dependencies::Dependencies;

    use super::DiffServiceImpl;

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

    fn git_stdout(repo_path: &std::path::Path, args: &[&str]) -> String {
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
            .expect("git output should be UTF-8")
            .trim()
            .to_string()
    }

    #[tokio::test]
    async fn raw_diff_raw_patch_and_diff_stats_return_expected_content() {
        let storage_root = unique_dir("diff-service");
        let repo_path = storage_root.join("project.git");

        std::fs::create_dir_all(&storage_root).expect("storage root should be creatable");
        std::fs::create_dir_all(&repo_path).expect("repo path should be creatable");

        run_git(&repo_path, &["init", "--quiet"]);
        run_git(&repo_path, &["config", "user.name", "Diff Service Tests"]);
        run_git(
            &repo_path,
            &["config", "user.email", "diff-service-tests@example.com"],
        );

        std::fs::write(repo_path.join("README.md"), b"hello\n").expect("README should write");
        run_git(&repo_path, &["add", "README.md"]);
        run_git(&repo_path, &["commit", "--quiet", "-m", "initial"]);

        std::fs::write(repo_path.join("README.md"), b"hello\nworld\n")
            .expect("README should update");
        run_git(&repo_path, &["add", "README.md"]);
        run_git(&repo_path, &["commit", "--quiet", "-m", "update readme"]);

        let left = git_stdout(&repo_path, &["rev-parse", "HEAD~1"]);
        let right = git_stdout(&repo_path, &["rev-parse", "HEAD"]);

        let mut storage_paths = HashMap::new();
        storage_paths.insert("default".to_string(), storage_root.clone());

        let dependencies = Arc::new(Dependencies::default().with_storage_paths(storage_paths));
        let service = DiffServiceImpl::new(dependencies);

        let repository = Repository {
            storage_name: "default".to_string(),
            relative_path: "project.git".to_string(),
            ..Repository::default()
        };

        let mut raw_diff = service
            .raw_diff(Request::new(RawDiffRequest {
                repository: Some(repository.clone()),
                left_commit_id: left.clone(),
                right_commit_id: right.clone(),
            }))
            .await
            .expect("raw_diff should succeed")
            .into_inner();

        let mut raw_diff_data = Vec::new();
        while let Some(chunk) = raw_diff.next().await {
            let chunk = chunk.expect("raw_diff chunk should not error");
            raw_diff_data.extend(chunk.data);
        }

        let raw_diff_text = String::from_utf8(raw_diff_data).expect("raw_diff should be UTF-8");
        assert!(raw_diff_text.contains("diff --git a/README.md b/README.md"));
        assert!(raw_diff_text.contains("+world"));

        let mut raw_patch = service
            .raw_patch(Request::new(RawPatchRequest {
                repository: Some(repository.clone()),
                left_commit_id: left.clone(),
                right_commit_id: right.clone(),
            }))
            .await
            .expect("raw_patch should succeed")
            .into_inner();

        let mut raw_patch_data = Vec::new();
        while let Some(chunk) = raw_patch.next().await {
            let chunk = chunk.expect("raw_patch chunk should not error");
            raw_patch_data.extend(chunk.data);
        }

        let raw_patch_text = String::from_utf8(raw_patch_data).expect("raw_patch should be UTF-8");
        assert!(raw_patch_text.contains("Subject: [PATCH] update readme"));
        assert!(raw_patch_text.contains("diff --git a/README.md b/README.md"));

        let mut stats = service
            .diff_stats(Request::new(DiffStatsRequest {
                repository: Some(repository),
                left_commit_id: left,
                right_commit_id: right,
            }))
            .await
            .expect("diff_stats should succeed")
            .into_inner();

        let first_page = stats
            .next()
            .await
            .expect("diff_stats should return one page")
            .expect("diff_stats page should not error");
        assert_eq!(first_page.stats.len(), 1);
        assert_eq!(first_page.stats[0].path, b"README.md".to_vec());
        assert_eq!(first_page.stats[0].additions, 1);
        assert_eq!(first_page.stats[0].deletions, 0);

        if storage_root.exists() {
            std::fs::remove_dir_all(storage_root).expect("storage root should be removable");
        }
    }
}
