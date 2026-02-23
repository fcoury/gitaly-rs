use std::path::{Component, Path, PathBuf};
use std::pin::Pin;
use std::sync::Arc;

use gitaly_git::reference::{RefUpdate, ReferenceUpdater};
use gitaly_git::repository::Repository as GitRepository;
use tokio::process::Command;
use tokio_stream::Stream;
use tonic::{Request, Response, Status};

use gitaly_proto::gitaly::ref_service_server::RefService;
use gitaly_proto::gitaly::*;

use crate::dependencies::Dependencies;

type ServiceStream<T> = Pin<Box<dyn Stream<Item = Result<T, Status>> + Send + 'static>>;

#[derive(Debug, Clone)]
pub struct RefServiceImpl {
    dependencies: Arc<Dependencies>,
}

impl RefServiceImpl {
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

fn decode_utf8(field: &'static str, bytes: Vec<u8>) -> Result<String, Status> {
    String::from_utf8(bytes)
        .map_err(|_| Status::invalid_argument(format!("{field} must be valid UTF-8")))
}

fn is_zero_oid(oid: &str) -> bool {
    !oid.is_empty() && oid.chars().all(|character| character == '0')
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

fn build_stream_response<T>(response: T) -> Response<ServiceStream<T>>
where
    T: Send + 'static,
{
    Response::new(Box::pin(tokio_stream::iter(vec![Ok(response)])))
}

fn unimplemented_ref_service(method: &'static str) -> Status {
    Status::unimplemented(format!("RefService::{method} is not implemented"))
}

#[tonic::async_trait]
impl RefService for RefServiceImpl {
    type FindLocalBranchesStream = ServiceStream<FindLocalBranchesResponse>;
    type FindAllBranchesStream = ServiceStream<FindAllBranchesResponse>;
    type FindAllTagsStream = ServiceStream<FindAllTagsResponse>;
    type FindAllRemoteBranchesStream = ServiceStream<FindAllRemoteBranchesResponse>;
    type ListBranchNamesContainingCommitStream =
        ServiceStream<ListBranchNamesContainingCommitResponse>;
    type ListTagNamesContainingCommitStream = ServiceStream<ListTagNamesContainingCommitResponse>;
    type GetTagSignaturesStream = ServiceStream<GetTagSignaturesResponse>;
    type GetTagMessagesStream = ServiceStream<GetTagMessagesResponse>;
    type ListRefsStream = ServiceStream<ListRefsResponse>;

    async fn find_default_branch_name(
        &self,
        request: Request<FindDefaultBranchNameRequest>,
    ) -> Result<Response<FindDefaultBranchNameResponse>, Status> {
        let request = request.into_inner();
        let repo_path = self.resolve_repo_path(request.repository)?;

        let output = git_output(
            &repo_path,
            ["for-each-ref", "--format=%(refname)", "refs/heads"],
        )
        .await?;
        if !output.status.success() {
            return Err(status_for_git_failure(
                "-C <repo> for-each-ref --format=%(refname) refs/heads",
                &output,
            ));
        }

        let branches = parse_output_lines(&output.stdout);
        if branches.is_empty() {
            return Ok(Response::new(FindDefaultBranchNameResponse {
                name: Vec::new(),
            }));
        }

        if request.head_only {
            let head_output = git_output(&repo_path, ["symbolic-ref", "-q", "HEAD"]).await?;
            let head_name = if head_output.status.success() {
                String::from_utf8_lossy(&head_output.stdout)
                    .trim()
                    .as_bytes()
                    .to_vec()
            } else {
                Vec::new()
            };

            return Ok(Response::new(FindDefaultBranchNameResponse {
                name: head_name,
            }));
        }

        let default_branch = if branches.len() == 1 {
            branches[0].clone()
        } else {
            let head_output = git_output(&repo_path, ["symbolic-ref", "-q", "HEAD"]).await?;
            if head_output.status.success() {
                let head = String::from_utf8_lossy(&head_output.stdout)
                    .trim()
                    .to_string();
                if branches.iter().any(|branch| branch == &head) {
                    head
                } else if branches.iter().any(|branch| branch == "refs/heads/main") {
                    "refs/heads/main".to_string()
                } else if branches.iter().any(|branch| branch == "refs/heads/master") {
                    "refs/heads/master".to_string()
                } else {
                    branches[0].clone()
                }
            } else if branches.iter().any(|branch| branch == "refs/heads/main") {
                "refs/heads/main".to_string()
            } else if branches.iter().any(|branch| branch == "refs/heads/master") {
                "refs/heads/master".to_string()
            } else {
                branches[0].clone()
            }
        };

        Ok(Response::new(FindDefaultBranchNameResponse {
            name: default_branch.into_bytes(),
        }))
    }

    async fn find_local_branches(
        &self,
        request: Request<FindLocalBranchesRequest>,
    ) -> Result<Response<Self::FindLocalBranchesStream>, Status> {
        let repo_path = self.resolve_repo_path(request.into_inner().repository)?;

        let output = git_output(
            &repo_path,
            [
                "for-each-ref",
                "--format=%(refname)\t%(objectname)",
                "refs/heads",
            ],
        )
        .await?;
        if !output.status.success() {
            return Err(status_for_git_failure(
                "-C <repo> for-each-ref --format=%(refname)\\t%(objectname) refs/heads",
                &output,
            ));
        }

        let local_branches = parse_output_lines(&output.stdout)
            .into_iter()
            .filter_map(|line| {
                let mut parts = line.splitn(2, '\t');
                let name = parts.next()?.as_bytes().to_vec();
                let target_id = parts.next()?.to_string();
                Some(Branch {
                    name,
                    target_commit: Some(GitCommit {
                        id: target_id,
                        ..GitCommit::default()
                    }),
                })
            })
            .collect();

        Ok(build_stream_response(FindLocalBranchesResponse {
            local_branches,
        }))
    }

    async fn find_all_branches(
        &self,
        _request: Request<FindAllBranchesRequest>,
    ) -> Result<Response<Self::FindAllBranchesStream>, Status> {
        Err(unimplemented_ref_service("find_all_branches"))
    }

    async fn find_all_tags(
        &self,
        request: Request<FindAllTagsRequest>,
    ) -> Result<Response<Self::FindAllTagsStream>, Status> {
        let repo_path = self.resolve_repo_path(request.into_inner().repository)?;

        let output = git_output(
            &repo_path,
            [
                "for-each-ref",
                "--format=%(refname)\t%(objectname)",
                "refs/tags",
            ],
        )
        .await?;
        if !output.status.success() {
            return Err(status_for_git_failure(
                "-C <repo> for-each-ref --format=%(refname)\\t%(objectname) refs/tags",
                &output,
            ));
        }

        let tags = parse_output_lines(&output.stdout)
            .into_iter()
            .filter_map(|line| {
                let mut parts = line.splitn(2, '\t');
                let name = parts.next()?.as_bytes().to_vec();
                let id = parts.next()?.to_string();
                Some(Tag {
                    name,
                    id,
                    ..Tag::default()
                })
            })
            .collect();

        Ok(build_stream_response(FindAllTagsResponse { tags }))
    }

    async fn find_tag(
        &self,
        _request: Request<FindTagRequest>,
    ) -> Result<Response<FindTagResponse>, Status> {
        Err(unimplemented_ref_service("find_tag"))
    }

    async fn find_all_remote_branches(
        &self,
        _request: Request<FindAllRemoteBranchesRequest>,
    ) -> Result<Response<Self::FindAllRemoteBranchesStream>, Status> {
        Err(unimplemented_ref_service("find_all_remote_branches"))
    }

    async fn ref_exists(
        &self,
        request: Request<RefExistsRequest>,
    ) -> Result<Response<RefExistsResponse>, Status> {
        let request = request.into_inner();
        let repo_path = self.resolve_repo_path(request.repository)?;

        let refname = decode_utf8("ref", request.r#ref)?;
        let repository = GitRepository::new(repo_path);
        let exists = repository
            .reference_exists(refname.as_str())
            .await
            .map_err(|err| {
                Status::internal(format!("failed to query reference existence: {err}"))
            })?;

        Ok(Response::new(RefExistsResponse { value: exists }))
    }

    async fn find_branch(
        &self,
        _request: Request<FindBranchRequest>,
    ) -> Result<Response<FindBranchResponse>, Status> {
        Err(unimplemented_ref_service("find_branch"))
    }

    async fn update_references(
        &self,
        request: Request<tonic::Streaming<UpdateReferencesRequest>>,
    ) -> Result<Response<UpdateReferencesResponse>, Status> {
        let mut stream = request.into_inner();
        let mut repository_descriptor: Option<Repository> = None;
        let mut updates_to_apply: Vec<RefUpdate> = Vec::new();

        while let Some(message) = stream
            .message()
            .await
            .map_err(|err| Status::invalid_argument(format!("invalid update stream: {err}")))?
        {
            if repository_descriptor.is_none() {
                repository_descriptor = message.repository.clone();
            }

            for update in message.updates {
                let refname = decode_utf8("updates.reference", update.reference)?;
                let old_oid = decode_utf8("updates.old_object_id", update.old_object_id)?;
                let new_oid = decode_utf8("updates.new_object_id", update.new_object_id)?;

                if new_oid.is_empty() {
                    return Err(Status::invalid_argument(
                        "updates.new_object_id must not be empty",
                    ));
                }

                if is_zero_oid(&new_oid) {
                    let effective_old_oid = if old_oid.is_empty() {
                        "0".repeat(new_oid.len().max(40))
                    } else {
                        old_oid
                    };

                    updates_to_apply.push(RefUpdate::Delete {
                        refname,
                        old_oid: effective_old_oid,
                    });
                    continue;
                }

                if old_oid.is_empty() {
                    updates_to_apply.push(RefUpdate::Create { refname, new_oid });
                } else {
                    updates_to_apply.push(RefUpdate::Update {
                        refname,
                        old_oid,
                        new_oid,
                    });
                }
            }
        }

        let repo_path = self.resolve_repo_path(repository_descriptor)?;
        let updater = ReferenceUpdater::new(repo_path);
        tokio::task::spawn_blocking(move || updater.apply(&updates_to_apply))
            .await
            .map_err(|err| Status::internal(format!("failed to join update task: {err}")))?
            .map_err(|err| Status::internal(format!("failed to update references: {err}")))?;

        Ok(Response::new(UpdateReferencesResponse {}))
    }

    async fn delete_refs(
        &self,
        request: Request<DeleteRefsRequest>,
    ) -> Result<Response<DeleteRefsResponse>, Status> {
        let request = request.into_inner();
        if !request.refs.is_empty() && !request.except_with_prefix.is_empty() {
            return Err(Status::invalid_argument(
                "refs and except_with_prefix are mutually exclusive",
            ));
        }

        if request.refs.is_empty() && request.except_with_prefix.is_empty() {
            return Err(Status::invalid_argument(
                "either refs or except_with_prefix must be provided",
            ));
        }

        let repo_path = self.resolve_repo_path(request.repository)?;
        let repository = GitRepository::new(repo_path.clone());

        let mut refs_to_delete = if !request.refs.is_empty() {
            request
                .refs
                .into_iter()
                .map(|value| decode_utf8("refs", value))
                .collect::<Result<Vec<_>, _>>()?
        } else {
            let output = git_output(&repo_path, ["for-each-ref", "--format=%(refname)"]).await?;
            if !output.status.success() {
                return Err(status_for_git_failure(
                    "-C <repo> for-each-ref --format=%(refname)",
                    &output,
                ));
            }

            parse_output_lines(&output.stdout)
        };

        if !request.except_with_prefix.is_empty() {
            let prefixes = request
                .except_with_prefix
                .into_iter()
                .map(|value| decode_utf8("except_with_prefix", value))
                .collect::<Result<Vec<_>, _>>()?;
            refs_to_delete
                .retain(|refname| !prefixes.iter().any(|prefix| refname.starts_with(prefix)));
        }

        let mut updates_to_apply = Vec::new();
        for refname in refs_to_delete {
            if let Some(old_oid) = repository
                .get_reference(&refname)
                .await
                .map_err(|err| Status::internal(format!("failed to read reference state: {err}")))?
            {
                updates_to_apply.push(RefUpdate::Delete { refname, old_oid });
            }
        }

        let updater = ReferenceUpdater::new(repo_path);
        tokio::task::spawn_blocking(move || updater.apply(&updates_to_apply))
            .await
            .map_err(|err| Status::internal(format!("failed to join delete task: {err}")))?
            .map_err(|err| Status::internal(format!("failed to delete references: {err}")))?;

        Ok(Response::new(DeleteRefsResponse {
            git_error: String::new(),
        }))
    }

    async fn list_branch_names_containing_commit(
        &self,
        _request: Request<ListBranchNamesContainingCommitRequest>,
    ) -> Result<Response<Self::ListBranchNamesContainingCommitStream>, Status> {
        Err(unimplemented_ref_service("list_branch_names_containing_commit"))
    }

    async fn list_tag_names_containing_commit(
        &self,
        _request: Request<ListTagNamesContainingCommitRequest>,
    ) -> Result<Response<Self::ListTagNamesContainingCommitStream>, Status> {
        Err(unimplemented_ref_service("list_tag_names_containing_commit"))
    }

    async fn get_tag_signatures(
        &self,
        _request: Request<GetTagSignaturesRequest>,
    ) -> Result<Response<Self::GetTagSignaturesStream>, Status> {
        Err(unimplemented_ref_service("get_tag_signatures"))
    }

    async fn get_tag_messages(
        &self,
        _request: Request<GetTagMessagesRequest>,
    ) -> Result<Response<Self::GetTagMessagesStream>, Status> {
        Err(unimplemented_ref_service("get_tag_messages"))
    }

    async fn list_refs(
        &self,
        request: Request<ListRefsRequest>,
    ) -> Result<Response<Self::ListRefsStream>, Status> {
        let request = request.into_inner();
        let repo_path = self.resolve_repo_path(request.repository)?;

        if request.patterns.is_empty() {
            return Err(Status::invalid_argument(
                "patterns must contain at least one entry",
            ));
        }

        let mut args = vec![
            "for-each-ref".to_string(),
            "--format=%(refname)\t%(objectname)".to_string(),
        ];

        if let Some(sort_by) = request.sort_by {
            let sort_key = match list_refs_request::sort_by::Key::try_from(sort_by.key).ok() {
                Some(list_refs_request::sort_by::Key::Creatordate) => "creatordate",
                Some(list_refs_request::sort_by::Key::Authordate) => "authordate",
                Some(list_refs_request::sort_by::Key::Committerdate) => "committerdate",
                Some(list_refs_request::sort_by::Key::Refname) | None => "refname",
            };
            let direction_prefix = match SortDirection::try_from(sort_by.direction)
                .ok()
                .unwrap_or(SortDirection::Ascending)
            {
                SortDirection::Ascending => "",
                SortDirection::Descending => "-",
            };
            args.push(format!("--sort={direction_prefix}{sort_key}"));
        }

        args.extend(
            request
                .patterns
                .into_iter()
                .map(|pattern| decode_utf8("patterns", pattern))
                .collect::<Result<Vec<_>, _>>()?,
        );

        let output = git_output(&repo_path, args.iter().map(String::as_str)).await?;
        if !output.status.success() {
            return Err(status_for_git_failure(
                "-C <repo> for-each-ref --format=%(refname)\\t%(objectname) <patterns>",
                &output,
            ));
        }

        let points_at_filter = request
            .pointing_at_oids
            .into_iter()
            .map(|oid| decode_utf8("pointing_at_oids", oid))
            .collect::<Result<Vec<_>, _>>()?;

        let mut references = parse_output_lines(&output.stdout)
            .into_iter()
            .filter_map(|line| {
                let mut parts = line.splitn(2, '\t');
                let name = parts.next()?.as_bytes().to_vec();
                let target = parts.next()?.to_string();

                if !points_at_filter.is_empty()
                    && !points_at_filter.iter().any(|oid| oid == &target)
                {
                    return None;
                }

                Some(list_refs_response::Reference {
                    name,
                    target,
                    peeled_target: String::new(),
                })
            })
            .collect::<Vec<_>>();

        if request.head {
            let output =
                git_output(&repo_path, ["rev-parse", "--verify", "--quiet", "HEAD"]).await?;
            if output.status.success() {
                let target = String::from_utf8_lossy(&output.stdout).trim().to_string();
                references.push(list_refs_response::Reference {
                    name: b"HEAD".to_vec(),
                    target,
                    peeled_target: String::new(),
                });
            }
        }

        if let Some(pagination) = request.pagination_params {
            if pagination.limit <= 0 {
                references.clear();
            } else {
                references.truncate(usize::try_from(pagination.limit).unwrap_or(usize::MAX));
            }
        }

        Ok(build_stream_response(ListRefsResponse {
            references,
            pagination_cursor: None,
        }))
    }

    async fn find_refs_by_oid(
        &self,
        _request: Request<FindRefsByOidRequest>,
    ) -> Result<Response<FindRefsByOidResponse>, Status> {
        Err(unimplemented_ref_service("find_refs_by_oid"))
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

    use gitaly_proto::gitaly::ref_service_server::RefService;
    use gitaly_proto::gitaly::{ListRefsRequest, RefExistsRequest, Repository};

    use crate::dependencies::Dependencies;

    use super::RefServiceImpl;

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
    async fn ref_exists_and_list_refs_work_for_local_branch() {
        let storage_root = unique_dir("ref-service");
        let repo_path = storage_root.join("project.git");

        std::fs::create_dir_all(&storage_root).expect("storage root should be creatable");
        std::fs::create_dir_all(&repo_path).expect("repo path should be creatable");

        run_git(&repo_path, &["init", "--quiet"]);
        run_git(&repo_path, &["config", "user.name", "Ref Service Tests"]);
        run_git(
            &repo_path,
            &["config", "user.email", "ref-service-tests@example.com"],
        );

        std::fs::write(repo_path.join("README.md"), b"hello\n").expect("README should write");
        run_git(&repo_path, &["add", "README.md"]);
        run_git(&repo_path, &["commit", "--quiet", "-m", "initial"]);
        run_git(&repo_path, &["branch", "feature/ref-service"]);

        let mut storage_paths = HashMap::new();
        storage_paths.insert("default".to_string(), storage_root.clone());

        let dependencies = Arc::new(Dependencies::default().with_storage_paths(storage_paths));
        let service = RefServiceImpl::new(dependencies);

        let repository = Repository {
            storage_name: "default".to_string(),
            relative_path: "project.git".to_string(),
            ..Repository::default()
        };

        let exists_response = service
            .ref_exists(Request::new(RefExistsRequest {
                repository: Some(repository.clone()),
                r#ref: b"refs/heads/feature/ref-service".to_vec(),
            }))
            .await
            .expect("ref_exists should succeed")
            .into_inner();
        assert!(exists_response.value);

        let mut list_response = service
            .list_refs(Request::new(ListRefsRequest {
                repository: Some(repository),
                patterns: vec![b"refs/heads".to_vec()],
                ..ListRefsRequest::default()
            }))
            .await
            .expect("list_refs should succeed")
            .into_inner();

        let mut entries = Vec::new();
        while let Some(page) = list_response.next().await {
            let page = page.expect("list_refs page should not error");
            entries.extend(
                page.references
                    .into_iter()
                    .map(|reference| String::from_utf8_lossy(&reference.name).into_owned()),
            );
        }

        assert!(entries
            .iter()
            .any(|entry| entry == "refs/heads/feature/ref-service"));

        if storage_root.exists() {
            std::fs::remove_dir_all(storage_root).expect("storage root should be removable");
        }
    }
}
