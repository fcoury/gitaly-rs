use std::io;
use std::path::{Component, Path, PathBuf};
use std::pin::Pin;
use std::sync::Arc;

use sha2::{Digest, Sha256};
use tokio::process::Command;
use tokio_stream::Stream;
use tonic::{Request, Response, Status};

use gitaly_proto::gitaly::repository_info_response::{self, references_info::ReferenceBackend};
use gitaly_proto::gitaly::repository_service_server::RepositoryService;
use gitaly_proto::gitaly::*;

use crate::dependencies::Dependencies;

type ServiceStream<T> = Pin<Box<dyn Stream<Item = Result<T, Status>> + Send + 'static>>;

#[derive(Debug, Clone)]
pub struct RepositoryServiceImpl {
    dependencies: Arc<Dependencies>,
}

impl RepositoryServiceImpl {
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

    fn resolve_existing_repo_path(
        &self,
        repository: Option<Repository>,
    ) -> Result<PathBuf, Status> {
        let repo_path = self.resolve_repo_path(repository)?;
        if !repo_path.exists() {
            return Err(Status::not_found("repository does not exist"));
        }

        Ok(repo_path)
    }

    async fn init_bare_repo_at(&self, repo_path: &Path) -> Result<(), Status> {
        if repo_path.exists() {
            return Err(Status::already_exists("repository already exists"));
        }

        if let Some(parent) = repo_path.parent() {
            std::fs::create_dir_all(parent).map_err(|err| {
                Status::internal(format!(
                    "failed to create repository parent directory: {err}"
                ))
            })?;
        }

        let repo_path = repo_path.to_string_lossy().into_owned();
        let args = ["init", "--bare", repo_path.as_str()];
        let output = Command::new("git")
            .args(args)
            .output()
            .await
            .map_err(|err| Status::internal(format!("failed to execute git init: {err}")))?;
        if !output.status.success() {
            return Err(status_for_git_failure("init --bare <repo>", &output));
        }

        Ok(())
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

fn decode_utf8(field: &'static str, bytes: Vec<u8>) -> Result<String, Status> {
    String::from_utf8(bytes)
        .map_err(|_| Status::invalid_argument(format!("{field} must be valid UTF-8")))
}

fn count_recursive(path: &Path) -> io::Result<u64> {
    if !path.exists() {
        return Ok(0);
    }

    let metadata = std::fs::symlink_metadata(path)?;
    if metadata.is_file() {
        return Ok(metadata.len());
    }

    if metadata.file_type().is_symlink() {
        return Ok(0);
    }

    if !metadata.is_dir() {
        return Ok(0);
    }

    let mut size = 0_u64;
    for entry in std::fs::read_dir(path)? {
        let entry = entry?;
        size = size.saturating_add(count_recursive(&entry.path())?);
    }

    Ok(size)
}

fn count_files(path: &Path) -> io::Result<u64> {
    if !path.exists() {
        return Ok(0);
    }

    let metadata = std::fs::symlink_metadata(path)?;
    if metadata.is_file() {
        return Ok(1);
    }

    if !metadata.is_dir() {
        return Ok(0);
    }

    let mut count = 0_u64;
    for entry in std::fs::read_dir(path)? {
        let entry = entry?;
        count = count.saturating_add(count_files(&entry.path())?);
    }

    Ok(count)
}

fn count_glob(path: &Path, extension: &str) -> io::Result<u64> {
    if !path.exists() {
        return Ok(0);
    }

    let mut count = 0_u64;
    for entry in std::fs::read_dir(path)? {
        let entry = entry?;
        let child = entry.path();
        let metadata = std::fs::symlink_metadata(&child)?;
        if metadata.is_dir() {
            count = count.saturating_add(count_glob(&child, extension)?);
            continue;
        }

        if metadata.is_file()
            && child.extension().and_then(|value| value.to_str()) == Some(extension)
        {
            count = count.saturating_add(1);
        }
    }

    Ok(count)
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

fn empty_service_stream<T: 'static>() -> ServiceStream<T> {
    Box::pin(tokio_stream::empty::<Result<T, Status>>())
}

async fn read_repository_from_stream<T, F>(
    stream: &mut tonic::Streaming<T>,
    mut repository_from_message: F,
) -> Result<Option<Repository>, Status>
where
    F: FnMut(T) -> Option<Repository>,
{
    let message = stream
        .message()
        .await
        .map_err(|err| Status::invalid_argument(format!("invalid request stream: {err}")))?;

    Ok(message.and_then(&mut repository_from_message))
}

#[tonic::async_trait]
impl RepositoryService for RepositoryServiceImpl {
    type GetArchiveStream = ServiceStream<GetArchiveResponse>;
    type CreateBundleStream = ServiceStream<CreateBundleResponse>;
    type CreateBundleFromRefListStream = ServiceStream<CreateBundleFromRefListResponse>;
    type GetConfigStream = ServiceStream<GetConfigResponse>;
    type GetInfoAttributesStream = ServiceStream<GetInfoAttributesResponse>;
    type GetSnapshotStream = ServiceStream<GetSnapshotResponse>;
    type GetRawChangesStream = ServiceStream<GetRawChangesResponse>;
    type SearchFilesByContentStream = ServiceStream<SearchFilesByContentResponse>;
    type SearchFilesByNameStream = ServiceStream<SearchFilesByNameResponse>;
    type BackupCustomHooksStream = ServiceStream<BackupCustomHooksResponse>;
    type GetCustomHooksStream = ServiceStream<GetCustomHooksResponse>;
    type FastExportStream = ServiceStream<FastExportResponse>;

    async fn repository_exists(
        &self,
        request: Request<RepositoryExistsRequest>,
    ) -> Result<Response<RepositoryExistsResponse>, Status> {
        let repo_path = self.resolve_repo_path(request.into_inner().repository)?;
        if !repo_path.exists() {
            return Ok(Response::new(RepositoryExistsResponse { exists: false }));
        }

        let output = git_output(&repo_path, ["rev-parse", "--git-dir"]).await?;
        Ok(Response::new(RepositoryExistsResponse {
            exists: output.status.success(),
        }))
    }

    async fn repository_size(
        &self,
        request: Request<RepositorySizeRequest>,
    ) -> Result<Response<RepositorySizeResponse>, Status> {
        let repo_path = self.resolve_repo_path(request.into_inner().repository)?;
        if !repo_path.exists() {
            return Err(Status::not_found("repository does not exist"));
        }

        let repo_path_for_size = repo_path.clone();
        let size = tokio::task::spawn_blocking(move || count_recursive(&repo_path_for_size))
            .await
            .map_err(|err| Status::internal(format!("failed to join size task: {err}")))?
            .map_err(|err| {
                Status::internal(format!("failed to calculate repository size: {err}"))
            })?;

        Ok(Response::new(RepositorySizeResponse {
            size: i64::try_from(size).unwrap_or(i64::MAX),
        }))
    }

    async fn repository_info(
        &self,
        request: Request<RepositoryInfoRequest>,
    ) -> Result<Response<RepositoryInfoResponse>, Status> {
        let repo_path = self.resolve_repo_path(request.into_inner().repository)?;
        if !repo_path.exists() {
            return Err(Status::not_found("repository does not exist"));
        }

        let repo_path_for_stats = repo_path.clone();
        let (size, references, objects) = tokio::task::spawn_blocking(move || {
            let size = count_recursive(&repo_path_for_stats)?;

            let refs_path = repo_path_for_stats.join("refs");
            let loose_count = count_files(&refs_path)?;
            let packed_size = std::fs::metadata(repo_path_for_stats.join("packed-refs"))
                .map(|metadata| metadata.len())
                .unwrap_or(0);

            let objects_path = repo_path_for_stats.join("objects");
            let objects_size = count_recursive(&objects_path)?;
            let pack_path = objects_path.join("pack");
            let packfile_count = count_glob(&pack_path, "pack")?;
            let reverse_index_count = count_glob(&pack_path, "rev")?;
            let keep_count = count_glob(&pack_path, "keep")?;

            let references = repository_info_response::ReferencesInfo {
                loose_count,
                packed_size,
                reference_backend: ReferenceBackend::Files as i32,
            };

            let objects = repository_info_response::ObjectsInfo {
                size: objects_size,
                recent_size: 0,
                stale_size: 0,
                keep_size: 0,
                packfile_count,
                reverse_index_count,
                cruft_count: 0,
                keep_count,
                loose_objects_count: 0,
                stale_loose_objects_count: 0,
                loose_objects_garbage_count: 0,
            };

            Ok::<_, io::Error>((size, references, objects))
        })
        .await
        .map_err(|err| Status::internal(format!("failed to join repository info task: {err}")))?
        .map_err(|err| Status::internal(format!("failed to collect repository info: {err}")))?;

        Ok(Response::new(RepositoryInfoResponse {
            size,
            references: Some(references),
            objects: Some(objects),
            commit_graph: Some(repository_info_response::CommitGraphInfo::default()),
            bitmap: Some(repository_info_response::BitmapInfo::default()),
            multi_pack_index: Some(repository_info_response::MultiPackIndexInfo::default()),
            multi_pack_index_bitmap: Some(repository_info_response::BitmapInfo::default()),
            alternates: Some(repository_info_response::AlternatesInfo::default()),
            is_object_pool: false,
            last_full_repack: None,
        }))
    }

    async fn objects_size(
        &self,
        request: Request<tonic::Streaming<ObjectsSizeRequest>>,
    ) -> Result<Response<ObjectsSizeResponse>, Status> {
        let mut stream = request.into_inner();
        let mut repository = None;
        let mut revisions = Vec::new();

        while let Some(message) = stream
            .message()
            .await
            .map_err(|err| Status::invalid_argument(format!("invalid request stream: {err}")))?
        {
            if repository.is_none() && message.repository.is_some() {
                repository = message.repository;
            }

            revisions.extend(
                message
                    .revisions
                    .into_iter()
                    .map(|revision| decode_utf8("revisions", revision))
                    .collect::<Result<Vec<_>, _>>()?,
            );
        }

        if revisions.is_empty() {
            return Err(Status::invalid_argument(
                "revisions must contain at least one revision",
            ));
        }

        let repo_path = self.resolve_repo_path(repository)?;
        let mut args = vec![
            "rev-list".to_string(),
            "--disk-usage".to_string(),
            "--objects".to_string(),
        ];
        args.extend(revisions);

        let output = git_output(&repo_path, args.iter().map(String::as_str)).await?;
        if !output.status.success() {
            return Err(status_for_git_failure(
                "-C <repo> rev-list --disk-usage ...",
                &output,
            ));
        }

        let size = String::from_utf8_lossy(&output.stdout)
            .trim()
            .parse::<u64>()
            .map_err(|err| Status::internal(format!("failed to parse objects size: {err}")))?;

        Ok(Response::new(ObjectsSizeResponse { size }))
    }

    async fn object_format(
        &self,
        request: Request<ObjectFormatRequest>,
    ) -> Result<Response<ObjectFormatResponse>, Status> {
        let repo_path = self.resolve_repo_path(request.into_inner().repository)?;
        let output = git_output(&repo_path, ["rev-parse", "--show-object-format"]).await?;
        if !output.status.success() {
            return Err(status_for_git_failure(
                "-C <repo> rev-parse --show-object-format",
                &output,
            ));
        }

        let format = match String::from_utf8_lossy(&output.stdout).trim() {
            "sha1" => ObjectFormat::Sha1,
            "sha256" => ObjectFormat::Sha256,
            _ => ObjectFormat::Unspecified,
        };

        Ok(Response::new(ObjectFormatResponse {
            format: format as i32,
        }))
    }

    async fn fetch_remote(
        &self,
        request: Request<FetchRemoteRequest>,
    ) -> Result<Response<FetchRemoteResponse>, Status> {
        let request = request.into_inner();
        self.resolve_existing_repo_path(request.repository)?;
        Ok(Response::new(FetchRemoteResponse::default()))
    }

    async fn create_repository(
        &self,
        request: Request<CreateRepositoryRequest>,
    ) -> Result<Response<CreateRepositoryResponse>, Status> {
        let request = request.into_inner();
        let repo_path = self.resolve_repo_path(request.repository)?;

        if repo_path.exists() {
            return Err(Status::already_exists("repository already exists"));
        }

        if let Some(parent) = repo_path.parent() {
            std::fs::create_dir_all(parent).map_err(|err| {
                Status::internal(format!(
                    "failed to create repository parent directory: {err}"
                ))
            })?;
        }

        let mut args: Vec<String> = vec!["init".to_string(), "--bare".to_string()];
        match ObjectFormat::try_from(request.object_format).ok() {
            Some(ObjectFormat::Sha256) => args.push("--object-format=sha256".to_string()),
            Some(ObjectFormat::Sha1) | Some(ObjectFormat::Unspecified) | None => {}
        }

        if !request.default_branch.is_empty() {
            let default_branch = String::from_utf8(request.default_branch).map_err(|_| {
                Status::invalid_argument("default_branch must be valid UTF-8 when provided")
            })?;
            args.push(format!("--initial-branch={default_branch}"));
        }

        args.push(repo_path.to_string_lossy().into_owned());

        let output = Command::new("git")
            .args(&args)
            .output()
            .await
            .map_err(|err| Status::internal(format!("failed to execute git init: {err}")))?;

        if !output.status.success() {
            return Err(status_for_git_failure(&args.join(" "), &output));
        }

        Ok(Response::new(CreateRepositoryResponse {}))
    }

    async fn get_archive(
        &self,
        request: Request<GetArchiveRequest>,
    ) -> Result<Response<Self::GetArchiveStream>, Status> {
        let request = request.into_inner();
        self.resolve_existing_repo_path(request.repository)?;
        Ok(Response::new(empty_service_stream()))
    }

    async fn has_local_branches(
        &self,
        request: Request<HasLocalBranchesRequest>,
    ) -> Result<Response<HasLocalBranchesResponse>, Status> {
        let repo_path = self.resolve_repo_path(request.into_inner().repository)?;
        let output = git_output(
            &repo_path,
            [
                "for-each-ref",
                "--format=%(refname)",
                "--count=1",
                "refs/heads",
            ],
        )
        .await?;
        if !output.status.success() {
            return Err(status_for_git_failure(
                "-C <repo> for-each-ref --format=%(refname) --count=1 refs/heads",
                &output,
            ));
        }

        let value = !String::from_utf8_lossy(&output.stdout).trim().is_empty();
        Ok(Response::new(HasLocalBranchesResponse { value }))
    }

    async fn fetch_source_branch(
        &self,
        request: Request<FetchSourceBranchRequest>,
    ) -> Result<Response<FetchSourceBranchResponse>, Status> {
        let request = request.into_inner();
        self.resolve_existing_repo_path(request.repository)?;
        Ok(Response::new(FetchSourceBranchResponse::default()))
    }

    async fn fsck(&self, request: Request<FsckRequest>) -> Result<Response<FsckResponse>, Status> {
        let request = request.into_inner();
        self.resolve_existing_repo_path(request.repository)?;
        Ok(Response::new(FsckResponse::default()))
    }

    async fn write_ref(
        &self,
        request: Request<WriteRefRequest>,
    ) -> Result<Response<WriteRefResponse>, Status> {
        let request = request.into_inner();
        self.resolve_existing_repo_path(request.repository)?;
        Ok(Response::new(WriteRefResponse::default()))
    }

    async fn find_merge_base(
        &self,
        request: Request<FindMergeBaseRequest>,
    ) -> Result<Response<FindMergeBaseResponse>, Status> {
        let request = request.into_inner();
        self.resolve_existing_repo_path(request.repository)?;
        Ok(Response::new(FindMergeBaseResponse::default()))
    }

    async fn create_fork(
        &self,
        request: Request<CreateForkRequest>,
    ) -> Result<Response<CreateForkResponse>, Status> {
        let request = request.into_inner();
        self.resolve_existing_repo_path(request.source_repository)?;
        let repo_path = self.resolve_repo_path(request.repository)?;
        self.init_bare_repo_at(&repo_path).await?;
        Ok(Response::new(CreateForkResponse::default()))
    }

    async fn create_repository_from_url(
        &self,
        request: Request<CreateRepositoryFromUrlRequest>,
    ) -> Result<Response<CreateRepositoryFromUrlResponse>, Status> {
        let request = request.into_inner();
        let repo_path = self.resolve_repo_path(request.repository)?;
        self.init_bare_repo_at(&repo_path).await?;
        Ok(Response::new(CreateRepositoryFromUrlResponse::default()))
    }

    async fn create_bundle(
        &self,
        request: Request<CreateBundleRequest>,
    ) -> Result<Response<Self::CreateBundleStream>, Status> {
        let request = request.into_inner();
        self.resolve_existing_repo_path(request.repository)?;
        Ok(Response::new(empty_service_stream()))
    }

    async fn create_bundle_from_ref_list(
        &self,
        request: Request<tonic::Streaming<CreateBundleFromRefListRequest>>,
    ) -> Result<Response<Self::CreateBundleFromRefListStream>, Status> {
        let mut stream = request.into_inner();
        if let Some(repository) =
            read_repository_from_stream(&mut stream, |message| message.repository).await?
        {
            self.resolve_existing_repo_path(Some(repository))?;
        }

        Ok(Response::new(empty_service_stream()))
    }

    async fn generate_bundle_uri(
        &self,
        request: Request<GenerateBundleUriRequest>,
    ) -> Result<Response<GenerateBundleUriResponse>, Status> {
        let request = request.into_inner();
        self.resolve_existing_repo_path(request.repository)?;
        Ok(Response::new(GenerateBundleUriResponse::default()))
    }

    async fn fetch_bundle(
        &self,
        request: Request<tonic::Streaming<FetchBundleRequest>>,
    ) -> Result<Response<FetchBundleResponse>, Status> {
        let mut stream = request.into_inner();
        if let Some(repository) =
            read_repository_from_stream(&mut stream, |message| message.repository).await?
        {
            self.resolve_existing_repo_path(Some(repository))?;
        }

        Ok(Response::new(FetchBundleResponse::default()))
    }

    async fn create_repository_from_bundle(
        &self,
        request: Request<tonic::Streaming<CreateRepositoryFromBundleRequest>>,
    ) -> Result<Response<CreateRepositoryFromBundleResponse>, Status> {
        let mut stream = request.into_inner();
        if let Some(repository) =
            read_repository_from_stream(&mut stream, |message| message.repository).await?
        {
            let repo_path = self.resolve_repo_path(Some(repository))?;
            self.init_bare_repo_at(&repo_path).await?;
        } else {
            return Err(Status::invalid_argument("repository is required"));
        }

        Ok(Response::new(CreateRepositoryFromBundleResponse::default()))
    }

    async fn get_config(
        &self,
        request: Request<GetConfigRequest>,
    ) -> Result<Response<Self::GetConfigStream>, Status> {
        let request = request.into_inner();
        self.resolve_existing_repo_path(request.repository)?;
        Ok(Response::new(empty_service_stream()))
    }

    async fn find_license(
        &self,
        request: Request<FindLicenseRequest>,
    ) -> Result<Response<FindLicenseResponse>, Status> {
        let request = request.into_inner();
        self.resolve_existing_repo_path(request.repository)?;
        Ok(Response::new(FindLicenseResponse::default()))
    }

    async fn get_info_attributes(
        &self,
        request: Request<GetInfoAttributesRequest>,
    ) -> Result<Response<Self::GetInfoAttributesStream>, Status> {
        let request = request.into_inner();
        self.resolve_existing_repo_path(request.repository)?;
        Ok(Response::new(empty_service_stream()))
    }

    async fn calculate_checksum(
        &self,
        request: Request<CalculateChecksumRequest>,
    ) -> Result<Response<CalculateChecksumResponse>, Status> {
        let repo_path = self.resolve_repo_path(request.into_inner().repository)?;
        if !repo_path.exists() {
            return Err(Status::not_found("repository does not exist"));
        }

        let output = git_output(
            &repo_path,
            [
                "for-each-ref",
                "--format=%(refname)\t%(objectname)",
                "refs/heads",
                "refs/tags",
                "refs/remotes",
            ],
        )
        .await?;
        if !output.status.success() {
            return Err(status_for_git_failure(
                "-C <repo> for-each-ref --format=%(refname)\\t%(objectname) refs/heads refs/tags refs/remotes",
                &output,
            ));
        }

        let mut lines = parse_output_lines(&output.stdout);
        lines.sort_unstable();

        let mut digest = Sha256::new();
        for line in &lines {
            digest.update(line.as_bytes());
            digest.update(b"\n");
        }

        let checksum = format!("{:x}", digest.finalize());

        Ok(Response::new(CalculateChecksumResponse { checksum }))
    }

    async fn get_snapshot(
        &self,
        request: Request<GetSnapshotRequest>,
    ) -> Result<Response<Self::GetSnapshotStream>, Status> {
        let request = request.into_inner();
        self.resolve_existing_repo_path(request.repository)?;
        Ok(Response::new(empty_service_stream()))
    }

    async fn create_repository_from_snapshot(
        &self,
        request: Request<CreateRepositoryFromSnapshotRequest>,
    ) -> Result<Response<CreateRepositoryFromSnapshotResponse>, Status> {
        let request = request.into_inner();
        let repo_path = self.resolve_repo_path(request.repository)?;
        self.init_bare_repo_at(&repo_path).await?;
        Ok(Response::new(
            CreateRepositoryFromSnapshotResponse::default(),
        ))
    }

    async fn get_raw_changes(
        &self,
        request: Request<GetRawChangesRequest>,
    ) -> Result<Response<Self::GetRawChangesStream>, Status> {
        let request = request.into_inner();
        self.resolve_existing_repo_path(request.repository)?;
        Ok(Response::new(empty_service_stream()))
    }

    async fn search_files_by_content(
        &self,
        request: Request<SearchFilesByContentRequest>,
    ) -> Result<Response<Self::SearchFilesByContentStream>, Status> {
        let request = request.into_inner();
        self.resolve_existing_repo_path(request.repository)?;
        Ok(Response::new(empty_service_stream()))
    }

    async fn search_files_by_name(
        &self,
        request: Request<SearchFilesByNameRequest>,
    ) -> Result<Response<Self::SearchFilesByNameStream>, Status> {
        let request = request.into_inner();
        self.resolve_existing_repo_path(request.repository)?;
        Ok(Response::new(empty_service_stream()))
    }

    async fn restore_custom_hooks(
        &self,
        request: Request<tonic::Streaming<RestoreCustomHooksRequest>>,
    ) -> Result<Response<RestoreCustomHooksResponse>, Status> {
        let mut stream = request.into_inner();
        if let Some(repository) =
            read_repository_from_stream(&mut stream, |message| message.repository).await?
        {
            self.resolve_existing_repo_path(Some(repository))?;
        }

        Ok(Response::new(RestoreCustomHooksResponse::default()))
    }

    async fn set_custom_hooks(
        &self,
        request: Request<tonic::Streaming<SetCustomHooksRequest>>,
    ) -> Result<Response<SetCustomHooksResponse>, Status> {
        let mut stream = request.into_inner();
        if let Some(repository) =
            read_repository_from_stream(&mut stream, |message| message.repository).await?
        {
            self.resolve_existing_repo_path(Some(repository))?;
        }

        Ok(Response::new(SetCustomHooksResponse::default()))
    }

    async fn backup_custom_hooks(
        &self,
        request: Request<BackupCustomHooksRequest>,
    ) -> Result<Response<Self::BackupCustomHooksStream>, Status> {
        let request = request.into_inner();
        self.resolve_existing_repo_path(request.repository)?;
        Ok(Response::new(empty_service_stream()))
    }

    async fn get_custom_hooks(
        &self,
        request: Request<GetCustomHooksRequest>,
    ) -> Result<Response<Self::GetCustomHooksStream>, Status> {
        let request = request.into_inner();
        self.resolve_existing_repo_path(request.repository)?;
        Ok(Response::new(empty_service_stream()))
    }

    async fn get_object_directory_size(
        &self,
        request: Request<GetObjectDirectorySizeRequest>,
    ) -> Result<Response<GetObjectDirectorySizeResponse>, Status> {
        let request = request.into_inner();
        let repo_path = self.resolve_existing_repo_path(request.repository)?;
        let objects_path = repo_path.join("objects");

        let object_bytes = tokio::task::spawn_blocking(move || count_recursive(&objects_path))
            .await
            .map_err(|err| {
                Status::internal(format!("failed to join object directory size task: {err}"))
            })?
            .map_err(|err| {
                Status::internal(format!("failed to calculate object directory size: {err}"))
            })?;

        let object_kib = object_bytes / 1024;
        Ok(Response::new(GetObjectDirectorySizeResponse {
            size: i64::try_from(object_kib).unwrap_or(i64::MAX),
        }))
    }

    async fn remove_repository(
        &self,
        request: Request<RemoveRepositoryRequest>,
    ) -> Result<Response<RemoveRepositoryResponse>, Status> {
        let repo_path = self.resolve_repo_path(request.into_inner().repository)?;
        if !repo_path.exists() {
            return Ok(Response::new(RemoveRepositoryResponse {}));
        }

        std::fs::remove_dir_all(&repo_path)
            .map_err(|err| Status::internal(format!("failed to remove repository: {err}")))?;

        Ok(Response::new(RemoveRepositoryResponse {}))
    }

    async fn replicate_repository(
        &self,
        request: Request<ReplicateRepositoryRequest>,
    ) -> Result<Response<ReplicateRepositoryResponse>, Status> {
        let request = request.into_inner();
        self.resolve_existing_repo_path(request.repository)?;
        Ok(Response::new(ReplicateRepositoryResponse::default()))
    }

    async fn optimize_repository(
        &self,
        request: Request<OptimizeRepositoryRequest>,
    ) -> Result<Response<OptimizeRepositoryResponse>, Status> {
        let repo_path = self.resolve_repo_path(request.into_inner().repository)?;
        if !repo_path.exists() {
            return Err(Status::not_found("repository does not exist"));
        }

        let output = git_output(&repo_path, ["gc", "--prune=now"]).await?;
        if !output.status.success() {
            return Err(status_for_git_failure("-C <repo> gc --prune=now", &output));
        }

        Ok(Response::new(OptimizeRepositoryResponse {}))
    }

    async fn prune_unreachable_objects(
        &self,
        request: Request<PruneUnreachableObjectsRequest>,
    ) -> Result<Response<PruneUnreachableObjectsResponse>, Status> {
        let request = request.into_inner();
        self.resolve_existing_repo_path(request.repository)?;
        Ok(Response::new(PruneUnreachableObjectsResponse::default()))
    }

    async fn backup_repository(
        &self,
        request: Request<BackupRepositoryRequest>,
    ) -> Result<Response<BackupRepositoryResponse>, Status> {
        let request = request.into_inner();
        self.resolve_existing_repo_path(request.repository)?;
        Ok(Response::new(BackupRepositoryResponse::default()))
    }

    async fn restore_repository(
        &self,
        request: Request<RestoreRepositoryRequest>,
    ) -> Result<Response<RestoreRepositoryResponse>, Status> {
        let request = request.into_inner();
        self.resolve_existing_repo_path(request.repository)?;
        Ok(Response::new(RestoreRepositoryResponse::default()))
    }

    async fn get_file_attributes(
        &self,
        request: Request<GetFileAttributesRequest>,
    ) -> Result<Response<GetFileAttributesResponse>, Status> {
        let request = request.into_inner();
        self.resolve_existing_repo_path(request.repository)?;
        Ok(Response::new(GetFileAttributesResponse::default()))
    }

    async fn fast_export(
        &self,
        request: Request<FastExportRequest>,
    ) -> Result<Response<Self::FastExportStream>, Status> {
        let request = request.into_inner();
        self.resolve_existing_repo_path(request.repository)?;
        Ok(Response::new(empty_service_stream()))
    }

    async fn migrate_reference_backend(
        &self,
        request: Request<MigrateReferenceBackendRequest>,
    ) -> Result<Response<MigrateReferenceBackendResponse>, Status> {
        let request = request.into_inner();
        self.resolve_existing_repo_path(request.repository)?;
        Ok(Response::new(MigrateReferenceBackendResponse::default()))
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

    use gitaly_proto::gitaly::repository_service_server::RepositoryService;
    use gitaly_proto::gitaly::{
        migrate_reference_backend_request::ReferenceBackend as MigrateReferenceBackend,
        CreateBundleRequest, CreateRepositoryRequest, FetchRemoteRequest, FindMergeBaseRequest,
        GetArchiveRequest, GetObjectDirectorySizeRequest, HasLocalBranchesRequest,
        MigrateReferenceBackendRequest, ObjectFormat, ObjectFormatRequest, RemoveRepositoryRequest,
        Repository, RepositoryExistsRequest,
    };

    use crate::dependencies::Dependencies;

    use super::RepositoryServiceImpl;

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

    async fn setup_service_with_repository(
        test_name: &str,
    ) -> (std::path::PathBuf, RepositoryServiceImpl, Repository) {
        let storage_root = unique_dir(test_name);
        std::fs::create_dir_all(&storage_root).expect("storage root should be creatable");

        let mut storage_paths = HashMap::new();
        storage_paths.insert("default".to_string(), storage_root.clone());
        let dependencies = Arc::new(Dependencies::default().with_storage_paths(storage_paths));
        let service = RepositoryServiceImpl::new(dependencies);

        let repository = Repository {
            storage_name: "default".to_string(),
            relative_path: "project.git".to_string(),
            ..Repository::default()
        };

        service
            .create_repository(Request::new(CreateRepositoryRequest {
                repository: Some(repository.clone()),
                ..CreateRepositoryRequest::default()
            }))
            .await
            .expect("create_repository should succeed");

        (storage_root, service, repository)
    }

    #[tokio::test]
    async fn create_exists_and_remove_repository_flow() {
        let storage_root = unique_dir("repository-service");
        std::fs::create_dir_all(&storage_root).expect("storage root should be creatable");

        let mut storage_paths = HashMap::new();
        storage_paths.insert("default".to_string(), storage_root.clone());

        let dependencies = Arc::new(Dependencies::default().with_storage_paths(storage_paths));
        let service = RepositoryServiceImpl::new(dependencies);

        let repository = Repository {
            storage_name: "default".to_string(),
            relative_path: "project.git".to_string(),
            ..Repository::default()
        };

        service
            .create_repository(Request::new(CreateRepositoryRequest {
                repository: Some(repository.clone()),
                ..CreateRepositoryRequest::default()
            }))
            .await
            .expect("create_repository should succeed");

        let exists_response = service
            .repository_exists(Request::new(RepositoryExistsRequest {
                repository: Some(repository.clone()),
            }))
            .await
            .expect("repository_exists should succeed")
            .into_inner();
        assert!(exists_response.exists);

        service
            .remove_repository(Request::new(RemoveRepositoryRequest {
                repository: Some(repository.clone()),
            }))
            .await
            .expect("remove_repository should succeed");

        let exists_response = service
            .repository_exists(Request::new(RepositoryExistsRequest {
                repository: Some(repository),
            }))
            .await
            .expect("repository_exists should succeed")
            .into_inner();
        assert!(!exists_response.exists);

        if storage_root.exists() {
            std::fs::remove_dir_all(storage_root).expect("storage root should be removable");
        }
    }

    #[tokio::test]
    async fn object_format_and_has_local_branches_reflect_repository_state() {
        let storage_root = unique_dir("repository-service-format-and-branches");
        std::fs::create_dir_all(&storage_root).expect("storage root should be creatable");

        let mut storage_paths = HashMap::new();
        storage_paths.insert("default".to_string(), storage_root.clone());
        let dependencies = Arc::new(Dependencies::default().with_storage_paths(storage_paths));
        let service = RepositoryServiceImpl::new(dependencies);

        let repository = Repository {
            storage_name: "default".to_string(),
            relative_path: "project.git".to_string(),
            ..Repository::default()
        };

        service
            .create_repository(Request::new(CreateRepositoryRequest {
                repository: Some(repository.clone()),
                ..CreateRepositoryRequest::default()
            }))
            .await
            .expect("create_repository should succeed");

        let object_format = service
            .object_format(Request::new(ObjectFormatRequest {
                repository: Some(repository.clone()),
            }))
            .await
            .expect("object_format should succeed")
            .into_inner();
        assert_eq!(object_format.format, ObjectFormat::Sha1 as i32);

        let no_branches = service
            .has_local_branches(Request::new(HasLocalBranchesRequest {
                repository: Some(repository.clone()),
            }))
            .await
            .expect("has_local_branches should succeed")
            .into_inner();
        assert!(!no_branches.value);

        let source_repo = storage_root.join("source");
        std::fs::create_dir_all(&source_repo).expect("source repo should be creatable");
        run_git(&source_repo, &["init", "--quiet"]);
        run_git(
            &source_repo,
            &["config", "user.name", "Repository Service Tests"],
        );
        run_git(
            &source_repo,
            &[
                "config",
                "user.email",
                "repository-service-tests@example.com",
            ],
        );
        std::fs::write(source_repo.join("README.md"), b"hello\n").expect("README should write");
        run_git(&source_repo, &["add", "README.md"]);
        run_git(&source_repo, &["commit", "--quiet", "-m", "initial"]);
        run_git(&source_repo, &["branch", "-M", "main"]);

        let bare_repo = storage_root.join("project.git");
        run_git(
            &source_repo,
            &[
                "remote",
                "add",
                "origin",
                bare_repo.to_string_lossy().as_ref(),
            ],
        );
        run_git(&source_repo, &["push", "--quiet", "origin", "main"]);

        let has_branches = service
            .has_local_branches(Request::new(HasLocalBranchesRequest {
                repository: Some(repository),
            }))
            .await
            .expect("has_local_branches should succeed")
            .into_inner();
        assert!(has_branches.value);

        if storage_root.exists() {
            std::fs::remove_dir_all(storage_root).expect("storage root should be removable");
        }
    }

    #[tokio::test]
    async fn baseline_unary_repository_methods_return_default_responses() {
        let (storage_root, service, repository) =
            setup_service_with_repository("repository-service-baseline-unary").await;

        let fetch_remote = service
            .fetch_remote(Request::new(FetchRemoteRequest {
                repository: Some(repository.clone()),
                ..FetchRemoteRequest::default()
            }))
            .await
            .expect("fetch_remote should return a baseline response")
            .into_inner();
        assert!(!fetch_remote.tags_changed);
        assert!(!fetch_remote.repo_changed);

        let merge_base = service
            .find_merge_base(Request::new(FindMergeBaseRequest {
                repository: Some(repository),
                revisions: vec![b"HEAD".to_vec(), b"refs/heads/main".to_vec()],
            }))
            .await
            .expect("find_merge_base should return a baseline response")
            .into_inner();
        assert!(merge_base.base.is_empty());

        if storage_root.exists() {
            std::fs::remove_dir_all(storage_root).expect("storage root should be removable");
        }
    }

    #[tokio::test]
    async fn baseline_streaming_repository_methods_return_empty_streams() {
        let (storage_root, service, repository) =
            setup_service_with_repository("repository-service-baseline-streaming").await;

        let mut archive_stream = service
            .get_archive(Request::new(GetArchiveRequest {
                repository: Some(repository.clone()),
                ..GetArchiveRequest::default()
            }))
            .await
            .expect("get_archive should return baseline stream")
            .into_inner();
        assert!(
            archive_stream.next().await.is_none(),
            "baseline get_archive stream should be empty"
        );

        let mut bundle_stream = service
            .create_bundle(Request::new(CreateBundleRequest {
                repository: Some(repository),
            }))
            .await
            .expect("create_bundle should return baseline stream")
            .into_inner();
        assert!(
            bundle_stream.next().await.is_none(),
            "baseline create_bundle stream should be empty"
        );

        if storage_root.exists() {
            std::fs::remove_dir_all(storage_root).expect("storage root should be removable");
        }
    }

    #[tokio::test]
    async fn baseline_additional_unary_methods_return_default_responses() {
        let (storage_root, service, repository) =
            setup_service_with_repository("repository-service-baseline-additional-unary").await;

        let object_directory_size = service
            .get_object_directory_size(Request::new(GetObjectDirectorySizeRequest {
                repository: Some(repository.clone()),
            }))
            .await
            .expect("get_object_directory_size should return baseline response")
            .into_inner();
        let expected_kib = i64::try_from(
            super::count_recursive(&storage_root.join("project.git").join("objects"))
                .expect("objects directory should be countable")
                / 1024,
        )
        .unwrap_or(i64::MAX);
        assert_eq!(object_directory_size.size, expected_kib);

        let migrated = service
            .migrate_reference_backend(Request::new(MigrateReferenceBackendRequest {
                repository: Some(repository),
                target_reference_backend: MigrateReferenceBackend::Files as i32,
            }))
            .await
            .expect("migrate_reference_backend should return baseline response")
            .into_inner();
        assert!(migrated.time.is_none());

        if storage_root.exists() {
            std::fs::remove_dir_all(storage_root).expect("storage root should be removable");
        }
    }
}
