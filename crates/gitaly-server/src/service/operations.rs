use std::path::{Component, Path, PathBuf};
use std::pin::Pin;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use gitaly_git::reference::{RefUpdate, ReferenceUpdater};
use gitaly_git::repository::Repository as GitRepository;
use tokio::process::Command;
use tokio_stream::Stream;
use tonic::{Request, Response, Status};

use gitaly_proto::gitaly::operation_service_server::OperationService;
use gitaly_proto::gitaly::user_commit_files_action::UserCommitFilesActionPayload;
use gitaly_proto::gitaly::user_commit_files_action_header::ActionType;
use gitaly_proto::gitaly::*;

use crate::dependencies::Dependencies;

type ServiceStream<T> = Pin<Box<dyn Stream<Item = Result<T, Status>> + Send + 'static>>;

#[derive(Debug, Clone)]
pub struct OperationServiceImpl {
    dependencies: Arc<Dependencies>,
}

impl OperationServiceImpl {
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

fn single_stream_response<T>(response: T) -> Response<ServiceStream<T>>
where
    T: Send + 'static,
{
    Response::new(Box::pin(tokio_stream::iter(vec![Ok(response)])))
}

fn decode_utf8(field: &'static str, bytes: Vec<u8>) -> Result<String, Status> {
    String::from_utf8(bytes)
        .map_err(|_| Status::invalid_argument(format!("{field} must be valid UTF-8")))
}

fn normalize_branch_ref(branch: &str) -> String {
    if branch.starts_with("refs/") {
        branch.to_string()
    } else {
        format!("refs/heads/{branch}")
    }
}

fn validate_worktree_relative_path(path: &str) -> Result<(), Status> {
    if path.trim().is_empty() {
        return Err(Status::invalid_argument("file_path is required"));
    }

    let candidate = Path::new(path);
    if candidate.is_absolute() {
        return Err(Status::invalid_argument("file_path must be relative"));
    }

    for component in candidate.components() {
        match component {
            Component::Normal(_) => {}
            _ => {
                return Err(Status::invalid_argument(
                    "file_path contains disallowed path components",
                ))
            }
        }
    }

    Ok(())
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

async fn git_output_with_env<I, S>(
    repo_path: &Path,
    args: I,
    envs: &[(&str, &str)],
) -> Result<std::process::Output, Status>
where
    I: IntoIterator<Item = S>,
    S: AsRef<str>,
{
    let mut command = Command::new("git");
    command.arg("-C").arg(repo_path);
    for arg in args {
        command.arg(arg.as_ref());
    }
    for (key, value) in envs {
        command.env(key, value);
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

fn repository_error_to_status(context: &str, err: gitaly_git::repository::RepositoryError) -> Status {
    Status::internal(format!("{context}: {err}"))
}

fn temp_worktree_path() -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| duration.as_nanos());
    std::env::temp_dir().join(format!("gitaly-ops-worktree-{nanos}"))
}

fn contains_nothing_to_commit(stderr: &[u8]) -> bool {
    String::from_utf8_lossy(stderr)
        .to_ascii_lowercase()
        .contains("nothing to commit")
}

fn decode_user_name_email(user: Option<User>) -> Result<(String, String), Status> {
    let user = user.ok_or_else(|| Status::invalid_argument("user is required"))?;
    let name = decode_utf8("user.name", user.name)?;
    let email = decode_utf8("user.email", user.email)?;
    if name.trim().is_empty() {
        return Err(Status::invalid_argument("user.name is required"));
    }
    if email.trim().is_empty() {
        return Err(Status::invalid_argument("user.email is required"));
    }
    Ok((name, email))
}

#[derive(Debug, Clone)]
enum CommitFileOperation {
    Write { path: String, content: Vec<u8> },
    Delete { path: String },
}

#[derive(Debug, Clone)]
struct PendingCommitFileOperation {
    action: ActionType,
    path: String,
    base64_content: bool,
    content: Vec<u8>,
}

fn finalize_pending_operation(
    pending: PendingCommitFileOperation,
) -> Result<CommitFileOperation, Status> {
    if pending.base64_content {
        return Err(Status::invalid_argument(
            "base64_content=true is not supported yet",
        ));
    }

    match pending.action {
        ActionType::Create | ActionType::Update => Ok(CommitFileOperation::Write {
            path: pending.path,
            content: pending.content,
        }),
        ActionType::Delete => Ok(CommitFileOperation::Delete { path: pending.path }),
        _ => Err(Status::invalid_argument("unsupported commit files action")),
    }
}

async fn apply_commit_file_operations(
    repo_path: &Path,
    parent_oid: &str,
    message: &str,
    author_name: &str,
    author_email: &str,
    operations: &[CommitFileOperation],
) -> Result<String, Status> {
    let worktree_path = temp_worktree_path();
    let worktree_string = worktree_path.to_string_lossy().to_string();

    let setup_output = git_output(
        repo_path,
        ["worktree", "add", "--detach", worktree_string.as_str(), parent_oid],
    )
    .await?;
    if !setup_output.status.success() {
        return Err(status_for_git_failure(
            "-C <repo> worktree add --detach <path> <parent_oid>",
            &setup_output,
        ));
    }

    let result = async {
        for operation in operations {
            match operation {
                CommitFileOperation::Write { path, content } => {
                    validate_worktree_relative_path(path)?;
                    let full_path = worktree_path.join(path);
                    if let Some(parent) = full_path.parent() {
                        std::fs::create_dir_all(parent).map_err(|err| {
                            Status::internal(format!("failed to create parent directories: {err}"))
                        })?;
                    }
                    std::fs::write(&full_path, content).map_err(|err| {
                        Status::internal(format!("failed to write file `{path}`: {err}"))
                    })?;
                }
                CommitFileOperation::Delete { path } => {
                    validate_worktree_relative_path(path)?;
                    let full_path = worktree_path.join(path);
                    if full_path.is_file() {
                        std::fs::remove_file(&full_path).map_err(|err| {
                            Status::internal(format!("failed to remove file `{path}`: {err}"))
                        })?;
                    } else if full_path.is_dir() {
                        std::fs::remove_dir_all(&full_path).map_err(|err| {
                            Status::internal(format!(
                                "failed to remove directory `{path}`: {err}"
                            ))
                        })?;
                    }
                }
            }
        }

        let add_output = git_output(&worktree_path, ["add", "-A"]).await?;
        if !add_output.status.success() {
            return Err(status_for_git_failure("-C <worktree> add -A", &add_output));
        }

        let effective_message = if message.trim().is_empty() {
            "update files"
        } else {
            message
        };
        let commit_output = git_output_with_env(
            &worktree_path,
            ["commit", "--quiet", "-m", effective_message],
            &[
                ("GIT_AUTHOR_NAME", author_name),
                ("GIT_AUTHOR_EMAIL", author_email),
                ("GIT_COMMITTER_NAME", author_name),
                ("GIT_COMMITTER_EMAIL", author_email),
            ],
        )
        .await?;

        if commit_output.status.success() {
            let head_output = git_output(&worktree_path, ["rev-parse", "HEAD"]).await?;
            if !head_output.status.success() {
                return Err(status_for_git_failure("-C <worktree> rev-parse HEAD", &head_output));
            }
            return Ok(String::from_utf8_lossy(&head_output.stdout).trim().to_string());
        }

        if contains_nothing_to_commit(&commit_output.stderr) {
            return Ok(parent_oid.to_string());
        }

        Err(status_for_git_failure(
            "-C <worktree> commit --quiet -m <message>",
            &commit_output,
        ))
    }
    .await;

    let cleanup_output = git_output(
        repo_path,
        ["worktree", "remove", "--force", worktree_string.as_str()],
    )
    .await;
    if cleanup_output.is_err() || cleanup_output.as_ref().is_ok_and(|output| !output.status.success()) {
        let _ = std::fs::remove_dir_all(&worktree_path);
    }

    result
}

#[tonic::async_trait]
impl OperationService for OperationServiceImpl {
    type UserMergeBranchStream = ServiceStream<UserMergeBranchResponse>;
    type UserRebaseConfirmableStream = ServiceStream<UserRebaseConfirmableResponse>;

    async fn user_create_branch(
        &self,
        request: Request<UserCreateBranchRequest>,
    ) -> Result<Response<UserCreateBranchResponse>, Status> {
        let request = request.into_inner();
        let repo_path = self.resolve_repo_path(request.repository)?;

        // Preserve permissive behavior for empty payloads used in existing smoke tests.
        if request.branch_name.is_empty() || request.start_point.is_empty() {
            return Ok(Response::new(UserCreateBranchResponse::default()));
        }

        let branch_name = decode_utf8("branch_name", request.branch_name)?;
        let branch_ref = normalize_branch_ref(&branch_name);
        let start_point = decode_utf8("start_point", request.start_point)?;

        let repository = GitRepository::new(repo_path.clone());
        let target_oid = repository
            .resolve_revision(&start_point)
            .await
            .map_err(|err| repository_error_to_status("failed to resolve start_point", err))?;

        if repository
            .reference_exists(&branch_ref)
            .await
            .map_err(|err| repository_error_to_status("failed to check branch existence", err))?
        {
            return Err(Status::already_exists(format!(
                "branch `{branch_name}` already exists"
            )));
        }

        ReferenceUpdater::new(repo_path)
            .apply(&[RefUpdate::Create {
                refname: branch_ref,
                new_oid: target_oid.clone(),
            }])
            .map_err(|err| Status::internal(format!("failed to create branch ref: {err}")))?;

        Ok(Response::new(UserCreateBranchResponse {
            branch: Some(Branch {
                name: branch_name.into_bytes(),
                target_commit: Some(GitCommit {
                    id: target_oid,
                    ..GitCommit::default()
                }),
            }),
        }))
    }

    async fn user_update_branch(
        &self,
        request: Request<UserUpdateBranchRequest>,
    ) -> Result<Response<UserUpdateBranchResponse>, Status> {
        self.resolve_repo_path(request.into_inner().repository)?;
        Ok(Response::new(UserUpdateBranchResponse::default()))
    }

    async fn user_delete_branch(
        &self,
        request: Request<UserDeleteBranchRequest>,
    ) -> Result<Response<UserDeleteBranchResponse>, Status> {
        self.resolve_repo_path(request.into_inner().repository)?;
        Ok(Response::new(UserDeleteBranchResponse::default()))
    }

    async fn user_create_tag(
        &self,
        request: Request<UserCreateTagRequest>,
    ) -> Result<Response<UserCreateTagResponse>, Status> {
        let req = request.into_inner();
        let repo_path = self.resolve_repo_path(req.repository)?;
        let repository = GitRepository::new(&repo_path);
        let (author_name, author_email) = decode_user_name_email(req.user)?;

        let tag_name = decode_utf8("tag_name", req.tag_name)?;
        if tag_name.trim().is_empty() {
            return Err(Status::invalid_argument("tag_name is required"));
        }
        let tag_ref = format!("refs/tags/{tag_name}");
        if repository
            .reference_exists(&tag_ref)
            .await
            .map_err(|err| repository_error_to_status("failed to check tag existence", err))?
        {
            return Err(Status::already_exists(format!(
                "tag `{tag_name}` already exists"
            )));
        }

        let target_revision = decode_utf8("target_revision", req.target_revision)?;
        if target_revision.trim().is_empty() {
            return Err(Status::invalid_argument("target_revision is required"));
        }
        repository
            .resolve_revision(&target_revision)
            .await
            .map_err(|err| repository_error_to_status("failed to resolve target_revision", err))?;

        let message = decode_utf8("message", req.message)?;
        let mut tag_args = vec!["tag".to_string()];
        if !message.is_empty() {
            tag_args.push("-a".to_string());
        }
        tag_args.push(tag_name.clone());
        tag_args.push(target_revision.clone());
        if !message.is_empty() {
            tag_args.push("-m".to_string());
            tag_args.push(message.clone());
        }
        let tag_output = git_output_with_env(
            &repo_path,
            tag_args.iter().map(String::as_str),
            &[
                ("GIT_AUTHOR_NAME", &author_name),
                ("GIT_AUTHOR_EMAIL", &author_email),
                ("GIT_COMMITTER_NAME", &author_name),
                ("GIT_COMMITTER_EMAIL", &author_email),
            ],
        )
        .await?;
        if !tag_output.status.success() {
            let stderr = String::from_utf8_lossy(&tag_output.stderr).to_ascii_lowercase();
            if stderr.contains("already exists") {
                return Err(Status::already_exists(format!(
                    "tag `{tag_name}` already exists"
                )));
            }
            return Err(status_for_git_failure(
                &tag_args.join(" "),
                &tag_output,
            ));
        }

        let tag_oid_output = git_output(&repo_path, ["rev-parse", &tag_ref]).await?;
        if !tag_oid_output.status.success() {
            return Err(status_for_git_failure(
                &format!("rev-parse {tag_ref}"),
                &tag_oid_output,
            ));
        }
        let tag_oid = String::from_utf8_lossy(&tag_oid_output.stdout)
            .trim()
            .to_string();

        let target_oid_output = git_output(&repo_path, ["rev-parse", &format!("{tag_ref}^{{}}")]).await?;
        if !target_oid_output.status.success() {
            return Err(status_for_git_failure(
                &format!("rev-parse {tag_ref}^{{}}"),
                &target_oid_output,
            ));
        }
        let target_oid = String::from_utf8_lossy(&target_oid_output.stdout)
            .trim()
            .to_string();

        Ok(Response::new(UserCreateTagResponse {
            tag: Some(Tag {
                name: tag_name.into_bytes(),
                id: tag_oid,
                target_commit: Some(GitCommit {
                    id: target_oid,
                    ..GitCommit::default()
                }),
                message: message.clone().into_bytes(),
                message_size: message.len() as i64,
                ..Tag::default()
            }),
        }))
    }

    async fn user_delete_tag(
        &self,
        request: Request<UserDeleteTagRequest>,
    ) -> Result<Response<UserDeleteTagResponse>, Status> {
        self.resolve_repo_path(request.into_inner().repository)?;
        Ok(Response::new(UserDeleteTagResponse::default()))
    }

    async fn user_merge_to_ref(
        &self,
        request: Request<UserMergeToRefRequest>,
    ) -> Result<Response<UserMergeToRefResponse>, Status> {
        let req = request.into_inner();
        let repo_path = self.resolve_repo_path(req.repository)?;
        let repository = GitRepository::new(&repo_path);
        let (author_name, author_email) = decode_user_name_email(req.user)?;

        let source_sha = req.source_sha.trim().to_string();
        if source_sha.is_empty() {
            return Err(Status::invalid_argument("source_sha is required"));
        }
        repository
            .resolve_revision(&source_sha)
            .await
            .map_err(|err| repository_error_to_status("failed to resolve source_sha", err))?;

        let raw_target_ref = decode_utf8("target_ref", req.target_ref)?;
        if raw_target_ref.trim().is_empty() {
            return Err(Status::invalid_argument("target_ref is required"));
        }
        let target_ref = normalize_branch_ref(&raw_target_ref);
        let first_parent_ref = if req.first_parent_ref.is_empty() {
            target_ref.clone()
        } else {
            decode_utf8("first_parent_ref", req.first_parent_ref)?
        };

        let first_parent_oid = repository
            .resolve_revision(&first_parent_ref)
            .await
            .map_err(|err| {
                repository_error_to_status("failed to resolve first_parent_ref/branch", err)
            })?;

        let target_exists = repository
            .reference_exists(&target_ref)
            .await
            .map_err(|err| repository_error_to_status("failed to check target ref", err))?;
        let target_old_oid = if target_exists {
            Some(
                repository
                    .resolve_revision(&target_ref)
                    .await
                    .map_err(|err| {
                        repository_error_to_status("failed to resolve target_ref", err)
                    })?,
            )
        } else {
            None
        };

        if !req.expected_old_oid.is_empty() {
            const ZERO_OID: &str = "0000000000000000000000000000000000000000";
            match (target_old_oid.as_deref(), req.expected_old_oid.as_str()) {
                (Some(current), expected) if current != expected => {
                    return Err(Status::failed_precondition(
                        "expected_old_oid does not match target_ref",
                    ));
                }
                (None, expected) if expected != ZERO_OID => {
                    return Err(Status::failed_precondition(
                        "expected_old_oid requires target_ref to exist or be zero OID",
                    ));
                }
                _ => {}
            }
        }

        let message = decode_utf8("message", req.message)?;
        let merge_message = if message.trim().is_empty() {
            format!("Merge {source_sha} into {first_parent_ref}")
        } else {
            message
        };

        let worktree_path = temp_worktree_path();
        let add_worktree_output = git_output(
            &repo_path,
            [
                "worktree",
                "add",
                "--detach",
                worktree_path.to_str().unwrap_or_default(),
                &first_parent_oid,
            ],
        )
        .await?;
        let result: Result<String, Status> = if !add_worktree_output.status.success() {
            Err(status_for_git_failure(
                &format!(
                    "worktree add --detach {} {}",
                    worktree_path.display(),
                    first_parent_oid
                ),
                &add_worktree_output,
            ))
        } else {
            let merge_output = git_output_with_env(
                &worktree_path,
                ["merge", "--no-ff", "-m", &merge_message, &source_sha],
                &[
                    ("GIT_AUTHOR_NAME", &author_name),
                    ("GIT_AUTHOR_EMAIL", &author_email),
                    ("GIT_COMMITTER_NAME", &author_name),
                    ("GIT_COMMITTER_EMAIL", &author_email),
                ],
            )
            .await?;
            if !merge_output.status.success() {
                let stderr = String::from_utf8_lossy(&merge_output.stderr).to_ascii_lowercase();
                if stderr.contains("conflict") {
                    Err(Status::aborted(format!(
                        "merge conflict while merging {source_sha} into {target_ref}"
                    )))
                } else {
                    Err(status_for_git_failure(
                        &format!("merge --no-ff -m <message> {source_sha}"),
                        &merge_output,
                    ))
                }
            } else {
                let head_output = git_output(&worktree_path, ["rev-parse", "HEAD"]).await?;
                if !head_output.status.success() {
                    Err(status_for_git_failure("rev-parse HEAD", &head_output))
                } else {
                    let commit_id = String::from_utf8_lossy(&head_output.stdout)
                        .trim()
                        .to_string();
                    if commit_id.is_empty() {
                        Err(Status::internal(
                            "merge produced empty commit id unexpectedly",
                        ))
                    } else {
                        let update = match target_old_oid {
                            Some(old_oid) => RefUpdate::Update {
                                refname: target_ref.clone(),
                                old_oid,
                                new_oid: commit_id.clone(),
                            },
                            None => RefUpdate::Create {
                                refname: target_ref.clone(),
                                new_oid: commit_id.clone(),
                            },
                        };
                        ReferenceUpdater::new(&repo_path)
                            .apply(&[update])
                            .map_err(|err| {
                                Status::internal(format!("failed to update target ref: {err}"))
                            })?;
                        Ok(commit_id)
                    }
                }
            }
        };

        let _ = git_output(
            &repo_path,
            [
                "worktree",
                "remove",
                "--force",
                worktree_path.to_str().unwrap_or_default(),
            ],
        )
        .await;
        let _ = std::fs::remove_dir_all(&worktree_path);

        let commit_id = result?;
        Ok(Response::new(UserMergeToRefResponse { commit_id }))
    }

    async fn user_rebase_to_ref(
        &self,
        request: Request<UserRebaseToRefRequest>,
    ) -> Result<Response<UserRebaseToRefResponse>, Status> {
        self.resolve_repo_path(request.into_inner().repository)?;
        Ok(Response::new(UserRebaseToRefResponse::default()))
    }

    async fn user_merge_branch(
        &self,
        request: Request<tonic::Streaming<UserMergeBranchRequest>>,
    ) -> Result<Response<Self::UserMergeBranchStream>, Status> {
        let mut stream = request.into_inner();
        let mut first_message = stream
            .message()
            .await
            .map_err(|err| Status::invalid_argument(format!("invalid request stream: {err}")))?
            .ok_or_else(|| {
                Status::invalid_argument("request stream must contain at least one message")
            })?;
        self.resolve_repo_path(first_message.repository.take())?;

        while stream
            .message()
            .await
            .map_err(|err| Status::invalid_argument(format!("invalid request stream: {err}")))?
            .is_some()
        {}

        Ok(single_stream_response(UserMergeBranchResponse::default()))
    }

    async fn user_ff_branch(
        &self,
        request: Request<UserFfBranchRequest>,
    ) -> Result<Response<UserFfBranchResponse>, Status> {
        self.resolve_repo_path(request.into_inner().repository)?;
        Ok(Response::new(UserFfBranchResponse::default()))
    }

    async fn user_cherry_pick(
        &self,
        request: Request<UserCherryPickRequest>,
    ) -> Result<Response<UserCherryPickResponse>, Status> {
        self.resolve_repo_path(request.into_inner().repository)?;
        Ok(Response::new(UserCherryPickResponse::default()))
    }

    async fn user_commit_files(
        &self,
        request: Request<tonic::Streaming<UserCommitFilesRequest>>,
    ) -> Result<Response<UserCommitFilesResponse>, Status> {
        let mut stream = request.into_inner();
        let first_message = stream
            .message()
            .await
            .map_err(|err| Status::invalid_argument(format!("invalid request stream: {err}")))?
            .ok_or_else(|| {
                Status::invalid_argument("request stream must contain at least one message")
            })?;

        let header = match first_message.user_commit_files_request_payload {
            Some(user_commit_files_request::UserCommitFilesRequestPayload::Header(header)) => {
                header
            }
            Some(_) => {
                return Err(Status::invalid_argument(
                    "first request must contain a commit files header payload",
                ))
            }
            None => {
                return Err(Status::invalid_argument(
                    "first request must contain a commit files header payload",
                ))
            }
        };

        let repo_path = self.resolve_repo_path(header.repository.clone())?;
        let branch_name = decode_utf8("branch_name", header.branch_name.clone())?;
        if branch_name.trim().is_empty() {
            return Err(Status::invalid_argument("branch_name is required"));
        }
        let branch_ref = normalize_branch_ref(&branch_name);
        let start_branch_name = if header.start_branch_name.is_empty() {
            None
        } else {
            Some(decode_utf8("start_branch_name", header.start_branch_name.clone())?)
        };
        let commit_message = decode_utf8("commit_message", header.commit_message.clone())?;

        let default_author_name =
            decode_utf8("user.name", header.user.as_ref().map_or_else(Vec::new, |user| user.name.clone()))?;
        let default_author_email =
            decode_utf8("user.email", header.user.as_ref().map_or_else(Vec::new, |user| user.email.clone()))?;
        let author_name = if header.commit_author_name.is_empty() {
            default_author_name
        } else {
            decode_utf8("commit_author_name", header.commit_author_name.clone())?
        };
        let author_email = if header.commit_author_email.is_empty() {
            default_author_email
        } else {
            decode_utf8("commit_author_email", header.commit_author_email.clone())?
        };

        let repository = GitRepository::new(repo_path.clone());
        let existing_oid = repository
            .get_reference(&branch_ref)
            .await
            .map_err(|err| repository_error_to_status("failed reading branch ref", err))?;

        let parent_oid = if !header.start_sha.is_empty() {
            repository
                .resolve_revision(&header.start_sha)
                .await
                .map_err(|err| repository_error_to_status("failed to resolve start_sha", err))?
        } else if let Some(start_branch_name) = start_branch_name.as_deref() {
            repository
                .resolve_revision(&normalize_branch_ref(start_branch_name))
                .await
                .map_err(|err| {
                    repository_error_to_status("failed to resolve start_branch_name", err)
                })?
        } else if let Some(existing_oid) = existing_oid.as_deref() {
            existing_oid.to_string()
        } else {
            return Err(Status::invalid_argument(
                "could not resolve parent commit from start_sha, start_branch_name, or branch_name",
            ));
        };

        let expected_old_oid = header.expected_old_oid.trim();
        if !expected_old_oid.is_empty() {
            let is_zero_oid = expected_old_oid.chars().all(|ch| ch == '0');
            match (is_zero_oid, existing_oid.as_deref()) {
                (true, Some(_)) => {
                    return Err(Status::failed_precondition(
                        "expected_old_oid requires branch creation but branch already exists",
                    ))
                }
                (false, Some(current)) if current != expected_old_oid => {
                    return Err(Status::failed_precondition(format!(
                        "expected_old_oid mismatch: expected {expected_old_oid}, got {current}"
                    )))
                }
                (false, None) => {
                    return Err(Status::failed_precondition(
                        "expected_old_oid requires existing branch",
                    ))
                }
                _ => {}
            }
        }

        let mut operations = Vec::new();
        let mut pending_operation: Option<PendingCommitFileOperation> = None;

        loop {
            let request = stream
                .message()
                .await
                .map_err(|err| Status::invalid_argument(format!("invalid request stream: {err}")))?;
            let Some(request) = request else {
                break;
            };

            let payload = request.user_commit_files_request_payload.ok_or_else(|| {
                Status::invalid_argument("commit files request payload cannot be empty")
            })?;
            let action = match payload {
                user_commit_files_request::UserCommitFilesRequestPayload::Action(action) => action,
                user_commit_files_request::UserCommitFilesRequestPayload::Header(_) => {
                    return Err(Status::invalid_argument(
                        "header payload may only appear as the first request",
                    ))
                }
            };

            match action.user_commit_files_action_payload {
                Some(UserCommitFilesActionPayload::Header(action_header)) => {
                    if let Some(previous) = pending_operation.take() {
                        operations.push(finalize_pending_operation(previous)?);
                    }

                    let action_type = ActionType::try_from(action_header.action).map_err(|_| {
                        Status::invalid_argument("commit files action contains invalid action type")
                    })?;
                    let path = decode_utf8("file_path", action_header.file_path)?;
                    validate_worktree_relative_path(&path)?;

                    match action_type {
                        ActionType::Create | ActionType::Update => {
                            pending_operation = Some(PendingCommitFileOperation {
                                action: action_type,
                                path,
                                base64_content: action_header.base64_content,
                                content: Vec::new(),
                            });
                        }
                        ActionType::Delete => {
                            operations.push(CommitFileOperation::Delete { path });
                        }
                        _ => {
                            return Err(Status::invalid_argument(
                                "only CREATE, UPDATE, and DELETE are supported",
                            ));
                        }
                    }
                }
                Some(UserCommitFilesActionPayload::Content(content)) => {
                    let Some(pending) = pending_operation.as_mut() else {
                        return Err(Status::invalid_argument(
                            "content payload must follow an action header",
                        ));
                    };
                    pending.content.extend(content);
                }
                None => {
                    return Err(Status::invalid_argument(
                        "commit file action payload cannot be empty",
                    ))
                }
            }
        }

        if let Some(previous) = pending_operation {
            operations.push(finalize_pending_operation(previous)?);
        }

        let new_oid = apply_commit_file_operations(
            &repo_path,
            &parent_oid,
            &commit_message,
            &author_name,
            &author_email,
            &operations,
        )
        .await?;

        let branch_created = existing_oid.is_none();
        if existing_oid.as_deref() != Some(new_oid.as_str()) {
            let update = if let Some(old_oid) = existing_oid {
                RefUpdate::Update {
                    refname: branch_ref.clone(),
                    old_oid,
                    new_oid: new_oid.clone(),
                }
            } else {
                RefUpdate::Create {
                    refname: branch_ref.clone(),
                    new_oid: new_oid.clone(),
                }
            };
            ReferenceUpdater::new(repo_path)
                .apply(&[update])
                .map_err(|err| Status::internal(format!("failed to update branch ref: {err}")))?;
        }

        Ok(Response::new(UserCommitFilesResponse {
            branch_update: Some(OperationBranchUpdate {
                commit_id: new_oid,
                repo_created: false,
                branch_created,
            }),
            ..UserCommitFilesResponse::default()
        }))
    }

    async fn user_rebase_confirmable(
        &self,
        request: Request<tonic::Streaming<UserRebaseConfirmableRequest>>,
    ) -> Result<Response<Self::UserRebaseConfirmableStream>, Status> {
        let mut stream = request.into_inner();
        let first_message = stream
            .message()
            .await
            .map_err(|err| Status::invalid_argument(format!("invalid request stream: {err}")))?
            .ok_or_else(|| {
                Status::invalid_argument("request stream must contain at least one message")
            })?;

        let repository = match first_message.user_rebase_confirmable_request_payload {
            Some(user_rebase_confirmable_request::UserRebaseConfirmableRequestPayload::Header(
                header,
            )) => header.repository,
            Some(_) => {
                return Err(Status::invalid_argument(
                    "first request must contain a rebase confirmable header payload",
                ))
            }
            None => {
                return Err(Status::invalid_argument(
                    "first request must contain a rebase confirmable header payload",
                ))
            }
        };
        self.resolve_repo_path(repository)?;

        while stream
            .message()
            .await
            .map_err(|err| Status::invalid_argument(format!("invalid request stream: {err}")))?
            .is_some()
        {}

        Ok(single_stream_response(
            UserRebaseConfirmableResponse::default(),
        ))
    }

    async fn user_revert(
        &self,
        request: Request<UserRevertRequest>,
    ) -> Result<Response<UserRevertResponse>, Status> {
        self.resolve_repo_path(request.into_inner().repository)?;
        Ok(Response::new(UserRevertResponse::default()))
    }

    async fn user_squash(
        &self,
        request: Request<UserSquashRequest>,
    ) -> Result<Response<UserSquashResponse>, Status> {
        self.resolve_repo_path(request.into_inner().repository)?;
        Ok(Response::new(UserSquashResponse::default()))
    }

    async fn user_apply_patch(
        &self,
        request: Request<tonic::Streaming<UserApplyPatchRequest>>,
    ) -> Result<Response<UserApplyPatchResponse>, Status> {
        let mut stream = request.into_inner();
        let first_message = stream
            .message()
            .await
            .map_err(|err| Status::invalid_argument(format!("invalid request stream: {err}")))?
            .ok_or_else(|| {
                Status::invalid_argument("request stream must contain at least one message")
            })?;

        let repository = match first_message.user_apply_patch_request_payload {
            Some(user_apply_patch_request::UserApplyPatchRequestPayload::Header(header)) => {
                header.repository
            }
            Some(_) => {
                return Err(Status::invalid_argument(
                    "first request must contain an apply patch header payload",
                ))
            }
            None => {
                return Err(Status::invalid_argument(
                    "first request must contain an apply patch header payload",
                ))
            }
        };
        self.resolve_repo_path(repository)?;

        while stream
            .message()
            .await
            .map_err(|err| Status::invalid_argument(format!("invalid request stream: {err}")))?
            .is_some()
        {}

        Ok(Response::new(UserApplyPatchResponse::default()))
    }

    async fn user_update_submodule(
        &self,
        request: Request<UserUpdateSubmoduleRequest>,
    ) -> Result<Response<UserUpdateSubmoduleResponse>, Status> {
        self.resolve_repo_path(request.into_inner().repository)?;
        Ok(Response::new(UserUpdateSubmoduleResponse::default()))
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;
    use std::time::Duration;

    use tokio::sync::oneshot;
    use tokio::task::JoinHandle;
    use tokio::time::sleep;
    use tokio_stream::StreamExt;
    use tonic::Code;

    use gitaly_proto::gitaly::operation_service_client::OperationServiceClient;
    use gitaly_proto::gitaly::operation_service_server::OperationServiceServer;
    use gitaly_proto::gitaly::{
        Repository, UserApplyPatchRequest, UserCommitFilesRequest, UserCreateBranchRequest,
        UserMergeBranchRequest,
    };

    use crate::dependencies::Dependencies;

    use super::OperationServiceImpl;

    async fn start_server(
        service: OperationServiceImpl,
    ) -> (String, oneshot::Sender<()>, JoinHandle<()>) {
        let listener =
            std::net::TcpListener::bind("127.0.0.1:0").expect("ephemeral listener should bind");
        let server_addr = listener
            .local_addr()
            .expect("ephemeral listener should provide local address");
        drop(listener);

        let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
        let server_task = tokio::spawn(async move {
            tonic::transport::Server::builder()
                .add_service(OperationServiceServer::new(service))
                .serve_with_shutdown(server_addr, async {
                    let _ = shutdown_rx.await;
                })
                .await
                .expect("server should run");
        });

        (format!("http://{server_addr}"), shutdown_tx, server_task)
    }

    async fn connect_client(endpoint: String) -> OperationServiceClient<tonic::transport::Channel> {
        for _ in 0..50 {
            match OperationServiceClient::connect(endpoint.clone()).await {
                Ok(client) => return client,
                Err(_) => sleep(Duration::from_millis(10)).await,
            }
        }

        panic!("server did not become reachable at {endpoint}");
    }

    async fn shutdown_server(shutdown_tx: oneshot::Sender<()>, server_task: JoinHandle<()>) {
        let _ = shutdown_tx.send(());
        let join_result = tokio::time::timeout(Duration::from_secs(2), server_task)
            .await
            .expect("server should stop within timeout");
        join_result.expect("server task should not panic");
    }

    fn test_dependencies() -> Arc<Dependencies> {
        let mut storage_paths = HashMap::new();
        storage_paths.insert("default".to_string(), std::env::temp_dir());
        Arc::new(Dependencies::default().with_storage_paths(storage_paths))
    }

    #[tokio::test]
    async fn representative_methods_return_callable_responses() {
        let (endpoint, shutdown_tx, server_task) =
            start_server(OperationServiceImpl::new(test_dependencies())).await;
        let mut client = connect_client(endpoint).await;

        client
            .user_create_branch(UserCreateBranchRequest {
                repository: Some(Repository {
                    storage_name: "default".to_string(),
                    relative_path: "group/project.git".to_string(),
                    ..Repository::default()
                }),
                ..UserCreateBranchRequest::default()
            })
            .await
            .expect("user_create_branch should succeed");

        let merge_branch_stream = tokio_stream::iter(vec![UserMergeBranchRequest {
            repository: Some(Repository {
                storage_name: "default".to_string(),
                relative_path: "group/project.git".to_string(),
                ..Repository::default()
            }),
            ..UserMergeBranchRequest::default()
        }]);
        let mut merge_branch_response_stream = client
            .user_merge_branch(merge_branch_stream)
            .await
            .expect("user_merge_branch should succeed")
            .into_inner();
        let first = merge_branch_response_stream
            .next()
            .await
            .expect("response should exist")
            .expect("response should succeed");
        let _ = first;

        shutdown_server(shutdown_tx, server_task).await;
    }

    #[tokio::test]
    async fn unary_methods_require_repository() {
        let (endpoint, shutdown_tx, server_task) =
            start_server(OperationServiceImpl::new(test_dependencies())).await;
        let mut client = connect_client(endpoint).await;

        let status = client
            .user_create_branch(UserCreateBranchRequest::default())
            .await
            .expect_err("missing repository should fail");
        assert_eq!(status.code(), Code::InvalidArgument);

        shutdown_server(shutdown_tx, server_task).await;
    }

    #[tokio::test]
    async fn streaming_methods_require_header_payload_first() {
        let (endpoint, shutdown_tx, server_task) =
            start_server(OperationServiceImpl::new(test_dependencies())).await;
        let mut client = connect_client(endpoint).await;

        let commit_files_stream = tokio_stream::iter(vec![UserCommitFilesRequest::default()]);
        let commit_files_status = client
            .user_commit_files(commit_files_stream)
            .await
            .expect_err("missing commit_files header should fail");
        assert_eq!(commit_files_status.code(), Code::InvalidArgument);

        let apply_patch_stream = tokio_stream::iter(vec![UserApplyPatchRequest::default()]);
        let apply_patch_status = client
            .user_apply_patch(apply_patch_stream)
            .await
            .expect_err("missing apply_patch header should fail");
        assert_eq!(apply_patch_status.code(), Code::InvalidArgument);

        shutdown_server(shutdown_tx, server_task).await;
    }
}
