use std::path::{Component, Path, PathBuf};
use std::pin::Pin;
use std::sync::Arc;

use tokio::process::Command;
use tokio_stream::Stream;
use tonic::{Request, Response, Status};

use gitaly_proto::gitaly::commit_service_server::CommitService;
use gitaly_proto::gitaly::*;

use crate::dependencies::Dependencies;

type ServiceStream<T> = Pin<Box<dyn Stream<Item = Result<T, Status>> + Send + 'static>>;

#[derive(Debug, Clone)]
pub struct CommitServiceImpl {
    dependencies: Arc<Dependencies>,
}

impl CommitServiceImpl {
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

fn unimplemented_commit_rpc(method: &str) -> Status {
    Status::unimplemented(format!("CommitService::{method} is not implemented"))
}

fn decode_utf8(field: &'static str, bytes: Vec<u8>) -> Result<String, Status> {
    String::from_utf8(bytes)
        .map_err(|_| Status::invalid_argument(format!("{field} must be valid UTF-8")))
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

#[tonic::async_trait]
impl CommitService for CommitServiceImpl {
    type ListCommitsStream = ServiceStream<ListCommitsResponse>;
    type ListAllCommitsStream = ServiceStream<ListAllCommitsResponse>;
    type TreeEntryStream = ServiceStream<TreeEntryResponse>;
    type GetTreeEntriesStream = ServiceStream<GetTreeEntriesResponse>;
    type ListFilesStream = ServiceStream<ListFilesResponse>;
    type FindAllCommitsStream = ServiceStream<FindAllCommitsResponse>;
    type FindCommitsStream = ServiceStream<FindCommitsResponse>;
    type RawBlameStream = ServiceStream<RawBlameResponse>;
    type ListLastCommitsForTreeStream = ServiceStream<ListLastCommitsForTreeResponse>;
    type CommitsByMessageStream = ServiceStream<CommitsByMessageResponse>;
    type ListCommitsByOidStream = ServiceStream<ListCommitsByOidResponse>;
    type ListCommitsByRefNameStream = ServiceStream<ListCommitsByRefNameResponse>;
    type FilterShasWithSignaturesStream = ServiceStream<FilterShasWithSignaturesResponse>;
    type GetCommitSignaturesStream = ServiceStream<GetCommitSignaturesResponse>;
    type GetCommitMessagesStream = ServiceStream<GetCommitMessagesResponse>;
    type CheckObjectsExistStream = ServiceStream<CheckObjectsExistResponse>;

    async fn list_commits(
        &self,
        request: Request<ListCommitsRequest>,
    ) -> Result<Response<Self::ListCommitsStream>, Status> {
        let request = request.into_inner();
        let repo_path = self.resolve_repo_path(request.repository)?;

        if request.revisions.is_empty() {
            return Err(Status::invalid_argument(
                "revisions must contain at least one revision",
            ));
        }

        let mut args = vec!["rev-list".to_string()];

        match list_commits_request::Order::try_from(request.order).ok() {
            Some(list_commits_request::Order::Topo) => args.push("--topo-order".to_string()),
            Some(list_commits_request::Order::Date) => args.push("--date-order".to_string()),
            _ => {}
        }

        if request.reverse {
            args.push("--reverse".to_string());
        }
        if request.max_parents > 0 {
            args.push(format!("--max-parents={}", request.max_parents));
        }
        if request.first_parent {
            args.push("--first-parent".to_string());
        }
        if request.skip > 0 {
            args.push(format!("--skip={}", request.skip));
        }
        if !request.author.is_empty() {
            args.push(format!(
                "--author={}",
                decode_utf8("author", request.author)?
            ));
        }

        for pattern in request.commit_message_patterns {
            args.push(format!(
                "--grep={}",
                decode_utf8("commit_message_patterns", pattern)?
            ));
        }

        args.extend(request.revisions);

        if !request.paths.is_empty() {
            args.push("--".to_string());
            args.extend(
                request
                    .paths
                    .into_iter()
                    .map(|path| decode_utf8("paths", path))
                    .collect::<Result<Vec<_>, _>>()?,
            );
        }

        let output = git_output(&repo_path, args.iter().map(String::as_str)).await?;
        if !output.status.success() {
            return Err(status_for_git_failure("-C <repo> rev-list ...", &output));
        }

        let mut commits = parse_output_lines(&output.stdout)
            .into_iter()
            .map(|id| GitCommit {
                id,
                ..GitCommit::default()
            })
            .collect::<Vec<_>>();

        if let Some(pagination) = request.pagination_params {
            if pagination.limit <= 0 {
                commits.clear();
            } else {
                commits.truncate(usize::try_from(pagination.limit).unwrap_or(usize::MAX));
            }
        }

        Ok(build_stream_response(ListCommitsResponse {
            commits,
            pagination_cursor: None,
        }))
    }

    async fn list_all_commits(
        &self,
        request: Request<ListAllCommitsRequest>,
    ) -> Result<Response<Self::ListAllCommitsStream>, Status> {
        let request = request.into_inner();
        let repo_path = self.resolve_repo_path(request.repository)?;

        let output = git_output(&repo_path, ["rev-list", "--all"]).await?;
        if !output.status.success() {
            return Err(status_for_git_failure("-C <repo> rev-list --all", &output));
        }

        let mut commits = parse_output_lines(&output.stdout)
            .into_iter()
            .map(|id| GitCommit {
                id,
                ..GitCommit::default()
            })
            .collect::<Vec<_>>();

        if let Some(pagination) = request.pagination_params {
            if pagination.limit <= 0 {
                commits.clear();
            } else {
                commits.truncate(usize::try_from(pagination.limit).unwrap_or(usize::MAX));
            }
        }

        Ok(build_stream_response(ListAllCommitsResponse { commits }))
    }

    async fn commit_is_ancestor(
        &self,
        request: Request<CommitIsAncestorRequest>,
    ) -> Result<Response<CommitIsAncestorResponse>, Status> {
        let request = request.into_inner();
        let repo_path = self.resolve_repo_path(request.repository)?;

        let output = git_output(
            &repo_path,
            [
                "merge-base",
                "--is-ancestor",
                request.ancestor_id.as_str(),
                request.child_id.as_str(),
            ],
        )
        .await?;

        let value = match output.status.code() {
            Some(0) => true,
            Some(1) => false,
            _ => {
                return Err(status_for_git_failure(
                    "-C <repo> merge-base --is-ancestor",
                    &output,
                ))
            }
        };

        Ok(Response::new(CommitIsAncestorResponse { value }))
    }

    async fn tree_entry(
        &self,
        _request: Request<TreeEntryRequest>,
    ) -> Result<Response<Self::TreeEntryStream>, Status> {
        Err(unimplemented_commit_rpc("tree_entry"))
    }

    async fn count_commits(
        &self,
        request: Request<CountCommitsRequest>,
    ) -> Result<Response<CountCommitsResponse>, Status> {
        let request = request.into_inner();
        let repo_path = self.resolve_repo_path(request.repository)?;

        let mut args = vec!["rev-list".to_string(), "--count".to_string()];

        if request.first_parent {
            args.push("--first-parent".to_string());
        }
        if request.max_count > 0 {
            args.push(format!("--max-count={}", request.max_count));
        }

        if !request.revisions.is_empty() {
            args.extend(
                request
                    .revisions
                    .into_iter()
                    .map(|rev| decode_utf8("revisions", rev))
                    .collect::<Result<Vec<_>, _>>()?,
            );
        } else if request.all {
            args.push("--all".to_string());
        } else if !request.revision.is_empty() {
            args.push(decode_utf8("revision", request.revision)?);
        } else {
            args.push("HEAD".to_string());
        }

        if !request.path.is_empty() {
            args.push("--".to_string());
            args.push(decode_utf8("path", request.path)?);
        }

        let output = git_output(&repo_path, args.iter().map(String::as_str)).await?;
        if !output.status.success() {
            return Err(status_for_git_failure(
                "-C <repo> rev-list --count ...",
                &output,
            ));
        }

        let count = String::from_utf8_lossy(&output.stdout)
            .trim()
            .parse::<i32>()
            .map_err(|err| Status::internal(format!("failed to parse commit count: {err}")))?;

        Ok(Response::new(CountCommitsResponse { count }))
    }

    async fn count_diverging_commits(
        &self,
        _request: Request<CountDivergingCommitsRequest>,
    ) -> Result<Response<CountDivergingCommitsResponse>, Status> {
        Err(unimplemented_commit_rpc("count_diverging_commits"))
    }

    async fn get_tree_entries(
        &self,
        _request: Request<GetTreeEntriesRequest>,
    ) -> Result<Response<Self::GetTreeEntriesStream>, Status> {
        Err(unimplemented_commit_rpc("get_tree_entries"))
    }

    async fn list_files(
        &self,
        _request: Request<ListFilesRequest>,
    ) -> Result<Response<Self::ListFilesStream>, Status> {
        Err(unimplemented_commit_rpc("list_files"))
    }

    async fn find_commit(
        &self,
        request: Request<FindCommitRequest>,
    ) -> Result<Response<FindCommitResponse>, Status> {
        let request = request.into_inner();
        let repo_path = self.resolve_repo_path(request.repository)?;

        let revision = decode_utf8("revision", request.revision)?;
        let revision_arg = format!("{revision}^{{commit}}");
        let output = git_output(
            &repo_path,
            ["rev-parse", "--verify", "--quiet", revision_arg.as_str()],
        )
        .await?;

        let commit = match output.status.code() {
            Some(0) => {
                let id = String::from_utf8_lossy(&output.stdout).trim().to_string();
                Some(GitCommit {
                    id,
                    ..GitCommit::default()
                })
            }
            Some(1) => None,
            _ => {
                return Err(status_for_git_failure(
                    "-C <repo> rev-parse --verify --quiet",
                    &output,
                ))
            }
        };

        Ok(Response::new(FindCommitResponse { commit }))
    }

    async fn commit_stats(
        &self,
        _request: Request<CommitStatsRequest>,
    ) -> Result<Response<CommitStatsResponse>, Status> {
        Err(unimplemented_commit_rpc("commit_stats"))
    }

    async fn find_all_commits(
        &self,
        _request: Request<FindAllCommitsRequest>,
    ) -> Result<Response<Self::FindAllCommitsStream>, Status> {
        Err(unimplemented_commit_rpc("find_all_commits"))
    }

    async fn find_commits(
        &self,
        _request: Request<FindCommitsRequest>,
    ) -> Result<Response<Self::FindCommitsStream>, Status> {
        Err(unimplemented_commit_rpc("find_commits"))
    }

    async fn commit_languages(
        &self,
        _request: Request<CommitLanguagesRequest>,
    ) -> Result<Response<CommitLanguagesResponse>, Status> {
        Err(unimplemented_commit_rpc("commit_languages"))
    }

    async fn raw_blame(
        &self,
        _request: Request<RawBlameRequest>,
    ) -> Result<Response<Self::RawBlameStream>, Status> {
        Err(unimplemented_commit_rpc("raw_blame"))
    }

    async fn last_commit_for_path(
        &self,
        _request: Request<LastCommitForPathRequest>,
    ) -> Result<Response<LastCommitForPathResponse>, Status> {
        Err(unimplemented_commit_rpc("last_commit_for_path"))
    }

    async fn list_last_commits_for_tree(
        &self,
        _request: Request<ListLastCommitsForTreeRequest>,
    ) -> Result<Response<Self::ListLastCommitsForTreeStream>, Status> {
        Err(unimplemented_commit_rpc("list_last_commits_for_tree"))
    }

    async fn commits_by_message(
        &self,
        _request: Request<CommitsByMessageRequest>,
    ) -> Result<Response<Self::CommitsByMessageStream>, Status> {
        Err(unimplemented_commit_rpc("commits_by_message"))
    }

    async fn list_commits_by_oid(
        &self,
        _request: Request<ListCommitsByOidRequest>,
    ) -> Result<Response<Self::ListCommitsByOidStream>, Status> {
        Err(unimplemented_commit_rpc("list_commits_by_oid"))
    }

    async fn list_commits_by_ref_name(
        &self,
        _request: Request<ListCommitsByRefNameRequest>,
    ) -> Result<Response<Self::ListCommitsByRefNameStream>, Status> {
        Err(unimplemented_commit_rpc("list_commits_by_ref_name"))
    }

    async fn filter_shas_with_signatures(
        &self,
        _request: Request<tonic::Streaming<FilterShasWithSignaturesRequest>>,
    ) -> Result<Response<Self::FilterShasWithSignaturesStream>, Status> {
        Err(unimplemented_commit_rpc("filter_shas_with_signatures"))
    }

    async fn get_commit_signatures(
        &self,
        _request: Request<GetCommitSignaturesRequest>,
    ) -> Result<Response<Self::GetCommitSignaturesStream>, Status> {
        Err(unimplemented_commit_rpc("get_commit_signatures"))
    }

    async fn get_commit_messages(
        &self,
        _request: Request<GetCommitMessagesRequest>,
    ) -> Result<Response<Self::GetCommitMessagesStream>, Status> {
        Err(unimplemented_commit_rpc("get_commit_messages"))
    }

    async fn check_objects_exist(
        &self,
        _request: Request<tonic::Streaming<CheckObjectsExistRequest>>,
    ) -> Result<Response<Self::CheckObjectsExistStream>, Status> {
        Err(unimplemented_commit_rpc("check_objects_exist"))
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

    use gitaly_proto::gitaly::commit_service_server::CommitService;
    use gitaly_proto::gitaly::{
        CountCommitsRequest, FindCommitRequest, ListCommitsRequest, Repository,
    };

    use crate::dependencies::Dependencies;

    use super::CommitServiceImpl;

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

    #[tokio::test]
    async fn find_commit_and_list_commits_return_known_head() {
        let storage_root = unique_dir("commit-service");
        let repo_path = storage_root.join("project.git");

        std::fs::create_dir_all(&storage_root).expect("storage root should be creatable");
        std::fs::create_dir_all(&repo_path).expect("repo path should be creatable");

        run_git(&repo_path, &["init", "--quiet"]);
        run_git(&repo_path, &["config", "user.name", "Commit Service Tests"]);
        run_git(
            &repo_path,
            &["config", "user.email", "commit-service-tests@example.com"],
        );

        std::fs::write(repo_path.join("README.md"), b"hello\n").expect("README should write");
        run_git(&repo_path, &["add", "README.md"]);
        run_git(&repo_path, &["commit", "--quiet", "-m", "initial"]);

        let head = String::from_utf8(
            Command::new("git")
                .arg("-C")
                .arg(&repo_path)
                .args(["rev-parse", "HEAD"])
                .output()
                .expect("git rev-parse should run")
                .stdout,
        )
        .expect("head should be UTF-8")
        .trim()
        .to_string();

        let mut storage_paths = HashMap::new();
        storage_paths.insert("default".to_string(), storage_root.clone());

        let dependencies = Arc::new(Dependencies::default().with_storage_paths(storage_paths));
        let service = CommitServiceImpl::new(dependencies);

        let repository = Repository {
            storage_name: "default".to_string(),
            relative_path: "project.git".to_string(),
            ..Repository::default()
        };

        let find_response = service
            .find_commit(Request::new(FindCommitRequest {
                repository: Some(repository.clone()),
                revision: b"HEAD".to_vec(),
                trailers: false,
            }))
            .await
            .expect("find_commit should succeed")
            .into_inner();

        assert_eq!(find_response.commit.expect("commit should exist").id, head);

        let mut stream = service
            .list_commits(Request::new(ListCommitsRequest {
                repository: Some(repository.clone()),
                revisions: vec!["HEAD".to_string()],
                ..ListCommitsRequest::default()
            }))
            .await
            .expect("list_commits should succeed")
            .into_inner();

        let first_page = stream
            .next()
            .await
            .expect("stream should yield first page")
            .expect("first page should not error");
        assert!(first_page.commits.iter().any(|commit| commit.id == head));

        let count = service
            .count_commits(Request::new(CountCommitsRequest {
                repository: Some(repository),
                revision: b"HEAD".to_vec(),
                ..CountCommitsRequest::default()
            }))
            .await
            .expect("count_commits should succeed")
            .into_inner();
        assert!(count.count >= 1);

        if storage_root.exists() {
            std::fs::remove_dir_all(storage_root).expect("storage root should be removable");
        }
    }
}
