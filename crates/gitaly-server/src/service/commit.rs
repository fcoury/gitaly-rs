use std::collections::HashMap;
use std::path::{Component, Path, PathBuf};
use std::pin::Pin;
use std::sync::Arc;

use tokio::process::Command;
use tokio_stream::{Stream, StreamExt};
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

fn git_stderr_contains(output: &std::process::Output, pattern: &str) -> bool {
    String::from_utf8_lossy(&output.stderr)
        .to_ascii_lowercase()
        .contains(&pattern.to_ascii_lowercase())
}

fn is_invalid_revision_error(output: &std::process::Output) -> bool {
    git_stderr_contains(output, "unknown revision")
        || git_stderr_contains(output, "bad revision")
        || git_stderr_contains(output, "invalid revision")
        || git_stderr_contains(output, "needed a single revision")
}

fn is_missing_object_error(output: &std::process::Output) -> bool {
    git_stderr_contains(output, "not a valid object name")
        || git_stderr_contains(output, "bad object")
        || git_stderr_contains(output, "invalid object")
        || is_invalid_revision_error(output)
}

fn is_path_not_found_error(output: &std::process::Output) -> bool {
    git_stderr_contains(output, "does not exist in")
        || git_stderr_contains(output, "pathspec")
        || git_stderr_contains(output, "not in tree")
}

fn validate_request_path(field: &'static str, path: &str, allow_root: bool) -> Result<(), Status> {
    if allow_root && (path.is_empty() || path == "/") {
        return Ok(());
    }

    if path.is_empty() {
        return Err(Status::invalid_argument(format!("{field} is required")));
    }

    let path_obj = Path::new(path);
    if path_obj.is_absolute() {
        return Err(Status::invalid_argument(format!(
            "{field} must be relative"
        )));
    }

    for component in path_obj.components() {
        match component {
            Component::Normal(_) => {}
            _ => {
                return Err(Status::invalid_argument(format!(
                    "{field} contains disallowed path components"
                )))
            }
        }
    }

    Ok(())
}

fn normalize_request_path(path: &str) -> String {
    if path == "/" {
        return String::new();
    }

    path.trim_end_matches('/').to_string()
}

#[derive(Debug)]
struct LsTreeEntry {
    mode: i32,
    object_type: String,
    oid: String,
    path: Vec<u8>,
}

fn parse_ls_tree_record(record: &[u8]) -> Result<Option<LsTreeEntry>, Status> {
    if record.is_empty() {
        return Ok(None);
    }

    let Some(path_separator) = record.iter().position(|byte| *byte == b'\t') else {
        return Err(Status::internal(
            "failed to parse git ls-tree output: missing path separator",
        ));
    };

    let header = std::str::from_utf8(&record[..path_separator])
        .map_err(|_| Status::internal("failed to parse git ls-tree output: invalid header"))?;
    let mut parts = header.split_whitespace();

    let mode = parts
        .next()
        .ok_or_else(|| Status::internal("failed to parse git ls-tree output: missing mode"))?;
    let object_type = parts
        .next()
        .ok_or_else(|| Status::internal("failed to parse git ls-tree output: missing type"))?;
    let oid = parts
        .next()
        .ok_or_else(|| Status::internal("failed to parse git ls-tree output: missing oid"))?;

    if parts.next().is_some() {
        return Err(Status::internal(
            "failed to parse git ls-tree output: unexpected extra metadata",
        ));
    }

    let mode = i32::from_str_radix(mode, 8)
        .map_err(|err| Status::internal(format!("failed to parse tree mode: {err}")))?;

    Ok(Some(LsTreeEntry {
        mode,
        object_type: object_type.to_string(),
        oid: oid.to_string(),
        path: record[path_separator + 1..].to_vec(),
    }))
}

fn parse_ls_tree_records(stdout: &[u8]) -> Result<Vec<LsTreeEntry>, Status> {
    let mut entries = Vec::new();
    for record in stdout.split(|byte| *byte == 0) {
        if let Some(entry) = parse_ls_tree_record(record)? {
            entries.push(entry);
        }
    }

    Ok(entries)
}

fn join_path(prefix: &[u8], suffix: &[u8]) -> Vec<u8> {
    if prefix.is_empty() {
        return suffix.to_vec();
    }

    if suffix.is_empty() {
        return prefix.to_vec();
    }

    let mut joined = Vec::with_capacity(prefix.len() + 1 + suffix.len());
    joined.extend_from_slice(prefix);
    joined.push(b'/');
    joined.extend_from_slice(suffix);
    joined
}

fn tree_entry_response_type(object_type: &str) -> tree_entry_response::ObjectType {
    match object_type {
        "blob" => tree_entry_response::ObjectType::Blob,
        "tree" => tree_entry_response::ObjectType::Tree,
        "commit" => tree_entry_response::ObjectType::Commit,
        "tag" => tree_entry_response::ObjectType::Tag,
        _ => tree_entry_response::ObjectType::Tag,
    }
}

fn tree_entry_type(object_type: &str) -> tree_entry::EntryType {
    match object_type {
        "blob" => tree_entry::EntryType::Blob,
        "tree" => tree_entry::EntryType::Tree,
        "commit" => tree_entry::EntryType::Commit,
        _ => tree_entry::EntryType::Blob,
    }
}

async fn git_resolve_oid(repo_path: &Path, spec: &str) -> Result<Option<String>, Status> {
    let output = git_output(repo_path, ["rev-parse", "--verify", "--quiet", spec]).await?;
    match output.status.code() {
        Some(0) => {
            let oid = String::from_utf8_lossy(&output.stdout).trim().to_string();
            if oid.is_empty() {
                Ok(None)
            } else {
                Ok(Some(oid))
            }
        }
        Some(1) => Ok(None),
        _ => Err(status_for_git_failure(
            "-C <repo> rev-parse --verify --quiet <spec>",
            &output,
        )),
    }
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

    String::from_utf8_lossy(&output.stdout)
        .trim()
        .parse::<i64>()
        .map_err(|err| Status::internal(format!("failed to parse object size: {err}")))
}

async fn git_cat_file_data(repo_path: &Path, oid: &str, limit: i64) -> Result<Vec<u8>, Status> {
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

fn parse_numstat_value(value: &str) -> Result<i64, Status> {
    if value == "-" {
        return Ok(0);
    }

    value
        .parse::<i64>()
        .map_err(|err| Status::internal(format!("failed to parse numstat field: {err}")))
}

fn parse_null_delimited_paths(stdout: &[u8]) -> Vec<String> {
    stdout
        .split(|byte| *byte == 0)
        .filter(|segment| !segment.is_empty())
        .map(|segment| String::from_utf8_lossy(segment).to_string())
        .collect()
}

fn parse_commit_ids(stdout: &[u8]) -> Vec<GitCommit> {
    parse_output_lines(stdout)
        .into_iter()
        .map(|id| GitCommit {
            id,
            ..GitCommit::default()
        })
        .collect()
}

fn parse_commit_author_line(line: &str) -> Option<CommitAuthor> {
    let (author_and_email, date_and_timezone) = line.rsplit_once('>')?;
    let (name, email) = author_and_email.rsplit_once('<')?;
    let mut date_parts = date_and_timezone.split_whitespace();
    let _seconds = date_parts.next()?.parse::<i64>().ok()?;
    let timezone = date_parts.next().unwrap_or("+0000");

    Some(CommitAuthor {
        name: name.trim_end().as_bytes().to_vec(),
        email: email.trim().as_bytes().to_vec(),
        date: None,
        timezone: timezone.as_bytes().to_vec(),
    })
}

#[derive(Debug, Default)]
struct ParsedCommitObject {
    signature: Vec<u8>,
    author: Option<CommitAuthor>,
    committer: Option<CommitAuthor>,
}

fn parse_commit_object(raw: &[u8]) -> ParsedCommitObject {
    let Some(header_end) = raw.windows(2).position(|window| window == b"\n\n") else {
        return ParsedCommitObject::default();
    };

    let header_bytes = &raw[..header_end];
    let header = String::from_utf8_lossy(header_bytes);

    let mut parsed = ParsedCommitObject::default();
    let mut current_key: Option<String> = None;
    let mut current_value = String::new();

    let mut flush_header = |key: Option<String>, value: &str| {
        let Some(key) = key else {
            return;
        };

        match key.as_str() {
            "gpgsig" => parsed.signature = value.as_bytes().to_vec(),
            "author" => parsed.author = parse_commit_author_line(value),
            "committer" => parsed.committer = parse_commit_author_line(value),
            _ => {}
        }
    };

    for line in header.lines() {
        if let Some(continued) = line.strip_prefix(' ') {
            if !current_value.is_empty() {
                current_value.push('\n');
            }
            current_value.push_str(continued);
            continue;
        }

        flush_header(current_key.take(), &current_value);
        current_value.clear();

        let Some((key, value)) = line.split_once(' ') else {
            continue;
        };

        current_key = Some(key.to_string());
        current_value.push_str(value);
    }

    flush_header(current_key, &current_value);
    parsed
}

fn detect_language(path: &str) -> Option<&'static str> {
    let extension = Path::new(path)
        .extension()
        .and_then(|ext| ext.to_str())
        .map(str::to_ascii_lowercase)?;

    match extension.as_str() {
        "c" | "h" => Some("C"),
        "cc" | "cpp" | "cxx" | "hpp" | "hh" => Some("C++"),
        "cs" => Some("C#"),
        "css" => Some("CSS"),
        "go" => Some("Go"),
        "html" | "htm" => Some("HTML"),
        "java" => Some("Java"),
        "js" | "jsx" => Some("JavaScript"),
        "json" => Some("JSON"),
        "kt" | "kts" => Some("Kotlin"),
        "m" | "mm" => Some("Objective-C"),
        "php" => Some("PHP"),
        "py" => Some("Python"),
        "rb" => Some("Ruby"),
        "rs" => Some("Rust"),
        "scala" => Some("Scala"),
        "sh" | "bash" => Some("Shell"),
        "sql" => Some("SQL"),
        "swift" => Some("Swift"),
        "toml" => Some("TOML"),
        "ts" | "tsx" => Some("TypeScript"),
        "xml" => Some("XML"),
        "yaml" | "yml" => Some("YAML"),
        "md" => Some("Markdown"),
        _ => None,
    }
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
        request: Request<TreeEntryRequest>,
    ) -> Result<Response<Self::TreeEntryStream>, Status> {
        let request = request.into_inner();
        let repo_path = self.resolve_repo_path(request.repository)?;
        if !repo_path.exists() {
            return Err(Status::not_found("repository does not exist"));
        }

        if request.limit < 0 {
            return Err(Status::invalid_argument("limit must be >= 0"));
        }
        if request.max_size < 0 {
            return Err(Status::invalid_argument("max_size must be >= 0"));
        }

        let revision = decode_utf8("revision", request.revision)?;
        if revision.trim().is_empty() {
            return Err(Status::invalid_argument("revision is required"));
        }

        let path = decode_utf8("path", request.path)?;
        validate_request_path("path", &path, true)?;
        let path = normalize_request_path(&path);

        let (oid, object_type, mode) = if path.is_empty() {
            let tree_spec = format!("{revision}^{{tree}}");
            let Some(oid) = git_resolve_oid(&repo_path, &tree_spec).await? else {
                return Err(Status::not_found(format!(
                    "revision `{revision}` not found"
                )));
            };

            (oid, "tree".to_string(), 0o040000)
        } else {
            let pathspec = format!(":(literal){path}");
            let output = git_output(
                &repo_path,
                ["ls-tree", "-z", revision.as_str(), "--", pathspec.as_str()],
            )
            .await?;

            if !output.status.success() {
                if is_missing_object_error(&output) || is_invalid_revision_error(&output) {
                    return Err(Status::not_found(format!(
                        "revision `{revision}` not found"
                    )));
                }

                return Err(status_for_git_failure(
                    "-C <repo> ls-tree -z <revision> -- <path>",
                    &output,
                ));
            }

            let mut entries = parse_ls_tree_records(&output.stdout)?;
            let entry = if let Some(index) = entries
                .iter()
                .position(|entry| entry.path == path.as_bytes())
            {
                entries.swap_remove(index)
            } else {
                let Some(entry) = entries.into_iter().next() else {
                    return Err(Status::not_found(format!(
                        "path `{path}` not found at revision `{revision}`"
                    )));
                };
                entry
            };

            if entry.path.is_empty() {
                return Err(Status::not_found(format!(
                    "path `{path}` not found at revision `{revision}`"
                )));
            }

            (entry.oid, entry.object_type, entry.mode)
        };

        let (size, data) = if object_type == "commit" && mode == 0o160000 {
            (0_i64, Vec::new())
        } else {
            let size = git_cat_file_size(&repo_path, &oid).await?;
            if request.max_size > 0 && size > request.max_size {
                return Err(Status::failed_precondition(format!(
                    "object size {size} exceeds max_size {}",
                    request.max_size
                )));
            }

            let data = git_cat_file_data(&repo_path, &oid, request.limit).await?;
            (size, data)
        };

        Ok(build_stream_response(TreeEntryResponse {
            r#type: tree_entry_response_type(&object_type) as i32,
            oid,
            size,
            mode,
            data,
        }))
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
        request: Request<CountDivergingCommitsRequest>,
    ) -> Result<Response<CountDivergingCommitsResponse>, Status> {
        let request = request.into_inner();
        let repo_path = self.resolve_repo_path(request.repository)?;

        let from = decode_utf8("from", request.from)?;
        if from.trim().is_empty() {
            return Err(Status::invalid_argument("from is required"));
        }

        let to = decode_utf8("to", request.to)?;
        if to.trim().is_empty() {
            return Err(Status::invalid_argument("to is required"));
        }

        let mut args = vec![
            "rev-list".to_string(),
            "--left-right".to_string(),
            "--count".to_string(),
        ];
        if request.max_count > 0 {
            args.push(format!("--max-count={}", request.max_count));
        }
        args.push(format!("{from}...{to}"));

        let output = git_output(&repo_path, args.iter().map(String::as_str)).await?;
        if !output.status.success() {
            if is_invalid_revision_error(&output) || is_missing_object_error(&output) {
                return Err(Status::not_found("from or to revision was not found"));
            }

            return Err(status_for_git_failure(
                "-C <repo> rev-list --left-right --count <from>...<to>",
                &output,
            ));
        }

        let mut counts = String::from_utf8_lossy(&output.stdout)
            .split_whitespace()
            .map(str::to_string)
            .collect::<Vec<_>>();
        if counts.len() != 2 {
            return Err(Status::internal(
                "failed to parse diverging commit counts from git rev-list output",
            ));
        }

        let right_count = counts
            .pop()
            .and_then(|value| value.parse::<i32>().ok())
            .ok_or_else(|| Status::internal("failed to parse right_count"))?;
        let left_count = counts
            .pop()
            .and_then(|value| value.parse::<i32>().ok())
            .ok_or_else(|| Status::internal("failed to parse left_count"))?;

        Ok(Response::new(CountDivergingCommitsResponse {
            left_count,
            right_count,
        }))
    }

    async fn get_tree_entries(
        &self,
        request: Request<GetTreeEntriesRequest>,
    ) -> Result<Response<Self::GetTreeEntriesStream>, Status> {
        let request = request.into_inner();
        let repo_path = self.resolve_repo_path(request.repository)?;
        if !repo_path.exists() {
            return Err(Status::not_found("repository does not exist"));
        }

        let revision = decode_utf8("revision", request.revision)?;
        if revision.trim().is_empty() {
            return Err(Status::invalid_argument("revision is required"));
        }

        let path = decode_utf8("path", request.path)?;
        validate_request_path("path", &path, true)?;
        let path = normalize_request_path(&path);

        let commit_oid = git_resolve_oid(&repo_path, &format!("{revision}^{{commit}}"))
            .await?
            .unwrap_or_default();

        let mut ls_tree_entries = if path.is_empty() {
            let tree_spec = format!("{revision}^{{tree}}");
            if git_resolve_oid(&repo_path, &tree_spec).await?.is_none() {
                return Err(Status::not_found(format!(
                    "revision `{revision}` not found"
                )));
            }

            let mut args = vec!["ls-tree".to_string(), "-z".to_string()];
            if request.recursive {
                args.push("-r".to_string());
            }
            args.push(tree_spec);

            let output = git_output(&repo_path, args.iter().map(String::as_str)).await?;
            if !output.status.success() {
                return Err(status_for_git_failure("-C <repo> ls-tree -z ...", &output));
            }

            parse_ls_tree_records(&output.stdout)?
        } else {
            let treeish = format!("{revision}:{path}");
            let object_type_output =
                git_output(&repo_path, ["cat-file", "-t", treeish.as_str()]).await?;

            if !object_type_output.status.success() {
                if is_invalid_revision_error(&object_type_output)
                    || is_missing_object_error(&object_type_output)
                    || is_path_not_found_error(&object_type_output)
                {
                    return Err(Status::not_found(format!(
                        "path `{path}` not found at revision `{revision}`"
                    )));
                }

                return Err(status_for_git_failure(
                    "-C <repo> cat-file -t <revision:path>",
                    &object_type_output,
                ));
            }

            let object_type = String::from_utf8_lossy(&object_type_output.stdout)
                .trim()
                .to_string();

            if object_type == "tree" {
                let mut args = vec!["ls-tree".to_string(), "-z".to_string()];
                if request.recursive {
                    args.push("-r".to_string());
                }
                args.push(treeish);

                let output = git_output(&repo_path, args.iter().map(String::as_str)).await?;
                if !output.status.success() {
                    return Err(status_for_git_failure("-C <repo> ls-tree -z ...", &output));
                }

                let prefix = path.as_bytes().to_vec();
                parse_ls_tree_records(&output.stdout)?
                    .into_iter()
                    .map(|mut entry| {
                        entry.path = join_path(&prefix, &entry.path);
                        entry
                    })
                    .collect::<Vec<_>>()
            } else {
                let pathspec = format!(":(literal){path}");
                let output = git_output(
                    &repo_path,
                    ["ls-tree", "-z", revision.as_str(), "--", pathspec.as_str()],
                )
                .await?;
                if !output.status.success() {
                    if is_invalid_revision_error(&output) || is_missing_object_error(&output) {
                        return Err(Status::not_found(format!(
                            "revision `{revision}` not found"
                        )));
                    }

                    return Err(status_for_git_failure(
                        "-C <repo> ls-tree -z <revision> -- <path>",
                        &output,
                    ));
                }

                let mut entries = parse_ls_tree_records(&output.stdout)?;
                let entry = if let Some(index) = entries
                    .iter()
                    .position(|entry| entry.path == path.as_bytes())
                {
                    entries.swap_remove(index)
                } else {
                    let Some(entry) = entries.into_iter().next() else {
                        return Err(Status::not_found(format!(
                            "path `{path}` not found at revision `{revision}`"
                        )));
                    };
                    entry
                };

                if entry.path.is_empty() {
                    return Err(Status::not_found(format!(
                        "path `{path}` not found at revision `{revision}`"
                    )));
                }

                vec![entry]
            }
        };

        match get_tree_entries_request::SortBy::try_from(request.sort).ok() {
            Some(get_tree_entries_request::SortBy::TreesFirst) => {
                ls_tree_entries.sort_by(|left, right| {
                    let left_rank = match tree_entry_type(&left.object_type) {
                        tree_entry::EntryType::Tree => 0_u8,
                        tree_entry::EntryType::Blob => 1_u8,
                        tree_entry::EntryType::Commit => 2_u8,
                    };
                    let right_rank = match tree_entry_type(&right.object_type) {
                        tree_entry::EntryType::Tree => 0_u8,
                        tree_entry::EntryType::Blob => 1_u8,
                        tree_entry::EntryType::Commit => 2_u8,
                    };

                    left_rank
                        .cmp(&right_rank)
                        .then_with(|| left.path.cmp(&right.path))
                });
            }
            Some(get_tree_entries_request::SortBy::Filesystem) => {
                ls_tree_entries.sort_by(|left, right| left.path.cmp(&right.path));
            }
            _ => {}
        }

        let mut pagination_cursor = None;
        if let Some(pagination) = request.pagination_params {
            let mut start = 0usize;
            if !pagination.page_token.is_empty() {
                let token = pagination.page_token.as_bytes();
                let Some(index) = ls_tree_entries.iter().position(|entry| entry.path == token)
                else {
                    return Err(Status::invalid_argument(
                        "pagination_params.page_token does not match any entry",
                    ));
                };
                start = index.saturating_add(1);
            }

            if start > 0 {
                ls_tree_entries = ls_tree_entries.into_iter().skip(start).collect();
            }

            if pagination.limit <= 0 {
                ls_tree_entries.clear();
            } else {
                let limit = usize::try_from(pagination.limit).unwrap_or(usize::MAX);
                if ls_tree_entries.len() > limit {
                    ls_tree_entries.truncate(limit);
                    if let Some(last) = ls_tree_entries.last() {
                        pagination_cursor = Some(PaginationCursor {
                            next_cursor: String::from_utf8_lossy(&last.path).to_string(),
                        });
                    }
                }
            }
        }

        let entries = ls_tree_entries
            .into_iter()
            .map(|entry| TreeEntry {
                oid: entry.oid,
                path: entry.path,
                r#type: tree_entry_type(&entry.object_type) as i32,
                mode: entry.mode,
                commit_oid: commit_oid.clone(),
                flat_path: Vec::new(),
            })
            .collect();

        Ok(build_stream_response(GetTreeEntriesResponse {
            entries,
            pagination_cursor,
        }))
    }

    async fn list_files(
        &self,
        request: Request<ListFilesRequest>,
    ) -> Result<Response<Self::ListFilesStream>, Status> {
        let request = request.into_inner();
        let repo_path = self.resolve_repo_path(request.repository)?;
        if !repo_path.exists() {
            return Err(Status::not_found("repository does not exist"));
        }

        let revision = decode_utf8("revision", request.revision)?;
        if revision.trim().is_empty() {
            return Err(Status::invalid_argument("revision is required"));
        }

        let output = git_output(
            &repo_path,
            ["ls-tree", "-r", "-z", "--name-only", revision.as_str()],
        )
        .await?;
        if !output.status.success() {
            if is_invalid_revision_error(&output) || is_missing_object_error(&output) {
                return Err(Status::not_found(format!(
                    "revision `{revision}` not found"
                )));
            }

            return Err(status_for_git_failure(
                "-C <repo> ls-tree -r -z --name-only <revision>",
                &output,
            ));
        }

        let paths = output
            .stdout
            .split(|byte| *byte == 0)
            .filter(|path| !path.is_empty())
            .map(|path| path.to_vec())
            .collect::<Vec<_>>();

        Ok(build_stream_response(ListFilesResponse { paths }))
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
        request: Request<CommitStatsRequest>,
    ) -> Result<Response<CommitStatsResponse>, Status> {
        let request = request.into_inner();
        let repo_path = self.resolve_repo_path(request.repository)?;
        if !repo_path.exists() {
            return Err(Status::not_found("repository does not exist"));
        }

        let revision = decode_utf8("revision", request.revision)?;
        if revision.trim().is_empty() {
            return Err(Status::invalid_argument("revision is required"));
        }

        let Some(oid) = git_resolve_oid(&repo_path, &format!("{revision}^{{commit}}")).await?
        else {
            return Ok(Response::new(CommitStatsResponse::default()));
        };

        let output = git_output(
            &repo_path,
            ["show", "--format=", "--numstat", "--root", oid.as_str()],
        )
        .await?;
        if !output.status.success() {
            return Err(status_for_git_failure(
                "-C <repo> show --format= --numstat --root <oid>",
                &output,
            ));
        }

        let mut additions = 0_i64;
        let mut deletions = 0_i64;
        let mut files = 0_i64;

        for line in String::from_utf8_lossy(&output.stdout)
            .lines()
            .map(str::trim)
            .filter(|line| !line.is_empty())
        {
            let mut parts = line.splitn(3, '\t');
            let Some(additions_value) = parts.next() else {
                continue;
            };
            let Some(deletions_value) = parts.next() else {
                continue;
            };
            if parts.next().is_none() {
                continue;
            }

            additions = additions.saturating_add(parse_numstat_value(additions_value)?);
            deletions = deletions.saturating_add(parse_numstat_value(deletions_value)?);
            files = files.saturating_add(1);
        }

        Ok(Response::new(CommitStatsResponse {
            oid,
            additions: i32::try_from(additions).unwrap_or(i32::MAX),
            deletions: i32::try_from(deletions).unwrap_or(i32::MAX),
            files: i32::try_from(files).unwrap_or(i32::MAX),
        }))
    }

    async fn find_all_commits(
        &self,
        request: Request<FindAllCommitsRequest>,
    ) -> Result<Response<Self::FindAllCommitsStream>, Status> {
        let request = request.into_inner();
        let repo_path = self.resolve_repo_path(request.repository)?;

        let mut args = vec!["rev-list".to_string()];
        match find_all_commits_request::Order::try_from(request.order).ok() {
            Some(find_all_commits_request::Order::Topo) => args.push("--topo-order".to_string()),
            Some(find_all_commits_request::Order::Date) => args.push("--date-order".to_string()),
            _ => {}
        }

        if request.max_count > 0 {
            args.push(format!("--max-count={}", request.max_count));
        }
        if request.skip > 0 {
            args.push(format!("--skip={}", request.skip));
        }

        if request.revision.is_empty() {
            args.push("--all".to_string());
        } else {
            args.push(decode_utf8("revision", request.revision)?);
        }

        let output = git_output(&repo_path, args.iter().map(String::as_str)).await?;
        if !output.status.success() {
            return Err(status_for_git_failure("-C <repo> rev-list ...", &output));
        }

        Ok(build_stream_response(FindAllCommitsResponse {
            commits: parse_commit_ids(&output.stdout),
        }))
    }

    async fn find_commits(
        &self,
        request: Request<FindCommitsRequest>,
    ) -> Result<Response<Self::FindCommitsStream>, Status> {
        let request = request.into_inner();
        let repo_path = self.resolve_repo_path(request.repository)?;

        if request.all && !request.revision.is_empty() {
            return Err(Status::invalid_argument(
                "all and revision are mutually exclusive",
            ));
        }

        let paths = request
            .paths
            .into_iter()
            .map(|path| decode_utf8("paths", path))
            .collect::<Result<Vec<_>, _>>()?;
        if request.follow && paths.len() > 1 {
            return Err(Status::invalid_argument("follow requires at most one path"));
        }
        for path in &paths {
            validate_request_path("paths", path, false)?;
        }

        let mut args = vec!["rev-list".to_string()];
        if request.follow {
            args.push("--follow".to_string());
        }
        if request.skip_merges {
            args.push("--no-merges".to_string());
        }
        if request.first_parent {
            args.push("--first-parent".to_string());
        }
        if request.limit > 0 {
            args.push(format!("--max-count={}", request.limit));
        }
        if request.offset > 0 {
            args.push(format!("--skip={}", request.offset));
        }
        if let Some(after) = request.after {
            args.push(format!("--since=@{}", after.seconds));
        }
        if let Some(before) = request.before {
            args.push(format!("--until=@{}", before.seconds));
        }
        if !request.author.is_empty() {
            args.push(format!(
                "--author={}",
                decode_utf8("author", request.author)?
            ));
        }
        if !request.message_regex.is_empty() {
            args.push("--regexp-ignore-case".to_string());
            args.push(format!("--grep={}", request.message_regex));
        }
        if matches!(
            find_commits_request::Order::try_from(request.order).ok(),
            Some(find_commits_request::Order::Topo)
        ) {
            args.push("--topo-order".to_string());
        }

        if request.all {
            args.push("--all".to_string());
        } else if request.revision.is_empty() {
            args.push("HEAD".to_string());
        } else {
            args.push(decode_utf8("revision", request.revision)?);
        }

        if !paths.is_empty() {
            args.push("--".to_string());
            args.extend(paths.into_iter().map(|path| format!(":(literal){path}")));
        }

        let output = git_output(&repo_path, args.iter().map(String::as_str)).await?;
        if !output.status.success() {
            if is_invalid_revision_error(&output) || is_missing_object_error(&output) {
                return Err(Status::not_found("requested revision was not found"));
            }

            return Err(status_for_git_failure("-C <repo> rev-list ...", &output));
        }

        Ok(build_stream_response(FindCommitsResponse {
            commits: parse_commit_ids(&output.stdout),
        }))
    }

    async fn commit_languages(
        &self,
        request: Request<CommitLanguagesRequest>,
    ) -> Result<Response<CommitLanguagesResponse>, Status> {
        let request = request.into_inner();
        let repo_path = self.resolve_repo_path(request.repository)?;

        let revision = if request.revision.is_empty() {
            "HEAD".to_string()
        } else {
            decode_utf8("revision", request.revision)?
        };

        let output = git_output(
            &repo_path,
            ["ls-tree", "-r", "-l", "-z", revision.as_str(), "--"],
        )
        .await?;
        if !output.status.success() {
            if is_invalid_revision_error(&output) || is_missing_object_error(&output) {
                return Err(Status::not_found(format!(
                    "revision `{revision}` not found"
                )));
            }

            return Err(status_for_git_failure(
                "-C <repo> ls-tree -r -l -z <revision> --",
                &output,
            ));
        }

        let mut bytes_by_language: HashMap<String, u64> = HashMap::new();
        for record in output.stdout.split(|byte| *byte == 0) {
            if record.is_empty() {
                continue;
            }

            let Some(path_separator) = record.iter().position(|byte| *byte == b'\t') else {
                continue;
            };
            let header = String::from_utf8_lossy(&record[..path_separator]);
            let path = String::from_utf8_lossy(&record[path_separator + 1..]).to_string();

            let mut parts = header.split_whitespace();
            let _mode = parts.next();
            let object_type = parts.next();
            let _oid = parts.next();
            let size = parts.next();
            if object_type != Some("blob") {
                continue;
            }

            let bytes = size
                .and_then(|value| value.parse::<u64>().ok())
                .unwrap_or(0);
            let language = detect_language(&path).unwrap_or("Other").to_string();
            *bytes_by_language.entry(language).or_insert(0) += bytes;
        }

        if bytes_by_language.is_empty() {
            return Err(Status::failed_precondition(
                "no languages could be detected for the requested revision",
            ));
        }

        let total_bytes = bytes_by_language.values().sum::<u64>();
        let mut languages = bytes_by_language
            .into_iter()
            .map(|(name, bytes)| commit_languages_response::Language {
                name,
                share: if total_bytes == 0 {
                    0.0
                } else {
                    ((bytes as f64 / total_bytes as f64) * 100.0) as f32
                },
                color: String::new(),
                bytes,
                language_id: 0,
            })
            .collect::<Vec<_>>();
        languages.sort_by(|left, right| right.bytes.cmp(&left.bytes));

        Ok(Response::new(CommitLanguagesResponse { languages }))
    }

    async fn raw_blame(
        &self,
        request: Request<RawBlameRequest>,
    ) -> Result<Response<Self::RawBlameStream>, Status> {
        let request = request.into_inner();
        let repo_path = self.resolve_repo_path(request.repository)?;
        if !repo_path.exists() {
            return Err(Status::not_found("repository does not exist"));
        }

        let revision = decode_utf8("revision", request.revision)?;
        if revision.trim().is_empty() {
            return Err(Status::invalid_argument("revision is required"));
        }

        let path = decode_utf8("path", request.path)?;
        validate_request_path("path", &path, false)?;

        if !request.ignore_revisions_blob.is_empty() {
            return Err(Status::invalid_argument(
                "ignore_revisions_blob is not supported yet",
            ));
        }

        let mut args = vec![
            "blame".to_string(),
            "--porcelain".to_string(),
            revision,
            "--".to_string(),
            path,
        ];

        if !request.range.is_empty() {
            let range = decode_utf8("range", request.range)?;
            let Some((start, end)) = range.split_once(',') else {
                return Err(Status::invalid_argument(
                    "range must be formatted as `start,end`",
                ));
            };
            let start = start
                .trim()
                .parse::<u64>()
                .map_err(|_| Status::invalid_argument("range start must be a number"))?;
            let end = end
                .trim()
                .parse::<u64>()
                .map_err(|_| Status::invalid_argument("range end must be a number"))?;
            if start == 0 || end == 0 || end < start {
                return Err(Status::invalid_argument(
                    "range values must be positive and end must be >= start",
                ));
            }

            args.splice(
                2..2,
                ["-L".to_string(), format!("{start},{end}")].into_iter(),
            );
        }

        let output = git_output(&repo_path, args.iter().map(String::as_str)).await?;
        if !output.status.success() {
            if is_path_not_found_error(&output) {
                return Err(Status::not_found(
                    "path does not exist at the requested revision",
                ));
            }
            if is_invalid_revision_error(&output) || is_missing_object_error(&output) {
                return Err(Status::not_found("revision does not exist"));
            }

            return Err(status_for_git_failure(
                "-C <repo> blame --porcelain ...",
                &output,
            ));
        }

        Ok(build_stream_response(RawBlameResponse {
            data: output.stdout,
        }))
    }

    async fn last_commit_for_path(
        &self,
        request: Request<LastCommitForPathRequest>,
    ) -> Result<Response<LastCommitForPathResponse>, Status> {
        let request = request.into_inner();
        let repo_path = self.resolve_repo_path(request.repository)?;
        if !repo_path.exists() {
            return Err(Status::not_found("repository does not exist"));
        }

        let revision = decode_utf8("revision", request.revision)?;
        if revision.trim().is_empty() {
            return Err(Status::invalid_argument("revision is required"));
        }

        let path = decode_utf8("path", request.path)?;
        validate_request_path("path", &path, true)?;
        let path = normalize_request_path(&path);

        let literal_pathspec = request.literal_pathspec
            || request
                .global_options
                .is_some_and(|options| options.literal_pathspecs);

        let mut args = vec![
            "log".to_string(),
            "-1".to_string(),
            "--format=%H".to_string(),
            revision.clone(),
        ];
        if !path.is_empty() {
            args.push("--".to_string());
            if literal_pathspec {
                args.push(format!(":(literal){path}"));
            } else {
                args.push(path);
            }
        }

        let output = git_output(&repo_path, args.iter().map(String::as_str)).await?;
        if !output.status.success() {
            if is_invalid_revision_error(&output) || is_missing_object_error(&output) {
                return Err(Status::not_found(format!(
                    "revision `{revision}` not found"
                )));
            }

            return Err(status_for_git_failure(
                "-C <repo> log -1 --format=%H ...",
                &output,
            ));
        }

        let oid = String::from_utf8_lossy(&output.stdout).trim().to_string();
        let commit = if oid.is_empty() {
            None
        } else {
            Some(GitCommit {
                id: oid,
                ..GitCommit::default()
            })
        };

        Ok(Response::new(LastCommitForPathResponse { commit }))
    }

    async fn list_last_commits_for_tree(
        &self,
        request: Request<ListLastCommitsForTreeRequest>,
    ) -> Result<Response<Self::ListLastCommitsForTreeStream>, Status> {
        let request = request.into_inner();
        let repo_path = self.resolve_repo_path(request.repository)?;
        if !repo_path.exists() {
            return Err(Status::not_found("repository does not exist"));
        }

        if request.revision.trim().is_empty() {
            return Err(Status::invalid_argument("revision is required"));
        }

        let path = decode_utf8("path", request.path)?;
        validate_request_path("path", &path, true)?;
        let path = normalize_request_path(&path);

        let tree_paths = if path.is_empty() {
            let output = git_output(
                &repo_path,
                ["ls-tree", "-z", "--name-only", request.revision.as_str()],
            )
            .await?;
            if !output.status.success() {
                if is_invalid_revision_error(&output) || is_missing_object_error(&output) {
                    return Err(Status::not_found(format!(
                        "revision `{}` not found",
                        request.revision
                    )));
                }

                return Err(status_for_git_failure(
                    "-C <repo> ls-tree -z --name-only <revision>",
                    &output,
                ));
            }

            parse_null_delimited_paths(&output.stdout)
        } else {
            let object_type_output = git_output(
                &repo_path,
                [
                    "cat-file",
                    "-t",
                    format!("{}:{path}", request.revision).as_str(),
                ],
            )
            .await?;

            if !object_type_output.status.success() {
                if is_path_not_found_error(&object_type_output)
                    || is_invalid_revision_error(&object_type_output)
                    || is_missing_object_error(&object_type_output)
                {
                    return Err(Status::not_found(format!(
                        "path `{path}` not found at revision `{}`",
                        request.revision
                    )));
                }

                return Err(status_for_git_failure(
                    "-C <repo> cat-file -t <revision:path>",
                    &object_type_output,
                ));
            }

            let object_type = String::from_utf8_lossy(&object_type_output.stdout)
                .trim()
                .to_string();

            if object_type == "tree" {
                let treeish = format!("{}:{path}", request.revision);
                let output =
                    git_output(&repo_path, ["ls-tree", "-z", "--name-only", &treeish]).await?;
                if !output.status.success() {
                    return Err(status_for_git_failure(
                        "-C <repo> ls-tree -z --name-only <treeish>",
                        &output,
                    ));
                }

                parse_null_delimited_paths(&output.stdout)
                    .into_iter()
                    .map(|entry| format!("{path}/{entry}"))
                    .collect()
            } else {
                vec![path]
            }
        };

        let start = usize::try_from(request.offset.max(0)).unwrap_or(usize::MAX);
        let mut limited_paths = if start >= tree_paths.len() {
            Vec::new()
        } else {
            tree_paths.into_iter().skip(start).collect::<Vec<_>>()
        };
        if request.limit > 0 {
            limited_paths.truncate(usize::try_from(request.limit).unwrap_or(usize::MAX));
        }

        let mut commits = Vec::new();
        for entry_path in limited_paths {
            let output = git_output(
                &repo_path,
                [
                    "log",
                    "-1",
                    "--format=%H",
                    request.revision.as_str(),
                    "--",
                    format!(":(literal){entry_path}").as_str(),
                ],
            )
            .await?;

            if !output.status.success() {
                return Err(status_for_git_failure(
                    "-C <repo> log -1 --format=%H <revision> -- <path>",
                    &output,
                ));
            }

            let commit_id = String::from_utf8_lossy(&output.stdout).trim().to_string();
            if commit_id.is_empty() {
                continue;
            }

            commits.push(list_last_commits_for_tree_response::CommitForTree {
                commit: Some(GitCommit {
                    id: commit_id,
                    ..GitCommit::default()
                }),
                path_bytes: entry_path.into_bytes(),
            });
        }

        Ok(build_stream_response(ListLastCommitsForTreeResponse {
            commits,
        }))
    }

    async fn commits_by_message(
        &self,
        request: Request<CommitsByMessageRequest>,
    ) -> Result<Response<Self::CommitsByMessageStream>, Status> {
        let request = request.into_inner();
        let repo_path = self.resolve_repo_path(request.repository)?;
        if request.query.trim().is_empty() {
            return Err(Status::invalid_argument("query is required"));
        }

        let revision = if request.revision.is_empty() {
            "HEAD".to_string()
        } else {
            decode_utf8("revision", request.revision)?
        };

        let mut args = vec![
            "log".to_string(),
            "--format=%H".to_string(),
            "--regexp-ignore-case".to_string(),
            format!("--grep={}", request.query),
            revision,
        ];
        if request.offset > 0 {
            args.push(format!("--skip={}", request.offset));
        }
        if request.limit > 0 {
            args.push(format!("--max-count={}", request.limit));
        }
        if !request.path.is_empty() {
            let path = decode_utf8("path", request.path)?;
            validate_request_path("path", &path, false)?;
            args.push("--".to_string());
            args.push(format!(":(literal){path}"));
        }

        let output = git_output(&repo_path, args.iter().map(String::as_str)).await?;
        if !output.status.success() {
            return Err(status_for_git_failure(
                "-C <repo> log --format=%H ...",
                &output,
            ));
        }

        Ok(build_stream_response(CommitsByMessageResponse {
            commits: parse_commit_ids(&output.stdout),
        }))
    }

    async fn list_commits_by_oid(
        &self,
        request: Request<ListCommitsByOidRequest>,
    ) -> Result<Response<Self::ListCommitsByOidStream>, Status> {
        let request = request.into_inner();
        let repo_path = self.resolve_repo_path(request.repository)?;

        let mut commits = Vec::new();
        for oid in request.oid {
            if oid.trim().is_empty() {
                continue;
            }

            let spec = format!("{oid}^{{commit}}");
            if let Some(commit_id) = git_resolve_oid(&repo_path, &spec).await? {
                commits.push(GitCommit {
                    id: commit_id,
                    ..GitCommit::default()
                });
            }
        }

        Ok(build_stream_response(ListCommitsByOidResponse { commits }))
    }

    async fn list_commits_by_ref_name(
        &self,
        request: Request<ListCommitsByRefNameRequest>,
    ) -> Result<Response<Self::ListCommitsByRefNameStream>, Status> {
        let request = request.into_inner();
        let repo_path = self.resolve_repo_path(request.repository)?;

        let mut commit_refs = Vec::new();
        for ref_name in request.ref_names {
            let ref_name_text = decode_utf8("ref_names", ref_name.clone())?;
            if ref_name_text.trim().is_empty() {
                continue;
            }

            let spec = format!("{ref_name_text}^{{commit}}");
            if let Some(commit_id) = git_resolve_oid(&repo_path, &spec).await? {
                commit_refs.push(list_commits_by_ref_name_response::CommitForRef {
                    commit: Some(GitCommit {
                        id: commit_id,
                        ..GitCommit::default()
                    }),
                    ref_name,
                });
            }
        }

        Ok(build_stream_response(ListCommitsByRefNameResponse {
            commit_refs,
        }))
    }

    async fn filter_shas_with_signatures(
        &self,
        request: Request<tonic::Streaming<FilterShasWithSignaturesRequest>>,
    ) -> Result<Response<Self::FilterShasWithSignaturesStream>, Status> {
        let mut stream = request.into_inner();
        let mut repository = None;
        let mut shas = Vec::new();
        while let Some(chunk) = stream.next().await {
            let chunk = chunk?;
            if repository.is_none() && chunk.repository.is_some() {
                repository = chunk.repository;
            }
            shas.extend(chunk.shas);
        }

        let repo_path = self.resolve_repo_path(repository)?;
        let mut signed_shas = Vec::new();
        for sha in shas {
            let sha_text = decode_utf8("shas", sha.clone())?;
            if sha_text.trim().is_empty() {
                continue;
            }

            let output = git_output(
                &repo_path,
                ["show", "-s", "--format=%G?", sha_text.as_str()],
            )
            .await?;
            if !output.status.success() {
                if is_invalid_revision_error(&output) || is_missing_object_error(&output) {
                    continue;
                }

                return Err(status_for_git_failure(
                    "-C <repo> show -s --format=%G? <sha>",
                    &output,
                ));
            }

            let has_signature = String::from_utf8_lossy(&output.stdout)
                .trim()
                .chars()
                .next()
                .is_some_and(|marker| marker != 'N');
            if has_signature {
                signed_shas.push(sha);
            }
        }

        Ok(build_stream_response(FilterShasWithSignaturesResponse {
            shas: signed_shas,
        }))
    }

    async fn get_commit_signatures(
        &self,
        request: Request<GetCommitSignaturesRequest>,
    ) -> Result<Response<Self::GetCommitSignaturesStream>, Status> {
        let request = request.into_inner();
        let repo_path = self.resolve_repo_path(request.repository)?;
        if request.commit_ids.is_empty() {
            return Err(Status::invalid_argument(
                "commit_ids must contain at least one commit",
            ));
        }

        let mut responses = Vec::new();
        for commit_id in request.commit_ids {
            if commit_id.trim().is_empty() {
                continue;
            }

            let output = git_output(&repo_path, ["cat-file", "commit", commit_id.as_str()]).await?;
            if !output.status.success() {
                if is_invalid_revision_error(&output) || is_missing_object_error(&output) {
                    continue;
                }

                return Err(status_for_git_failure(
                    "-C <repo> cat-file commit <commit_id>",
                    &output,
                ));
            }

            let parsed = parse_commit_object(&output.stdout);
            if parsed.signature.is_empty() {
                continue;
            }

            responses.push(GetCommitSignaturesResponse {
                commit_id,
                signature: parsed.signature,
                signed_text: output.stdout,
                signer: get_commit_signatures_response::Signer::User as i32,
                author: parsed.author,
                committer: parsed.committer,
            });
        }

        Ok(Response::new(Box::pin(tokio_stream::iter(
            responses.into_iter().map(Ok),
        ))))
    }

    async fn get_commit_messages(
        &self,
        request: Request<GetCommitMessagesRequest>,
    ) -> Result<Response<Self::GetCommitMessagesStream>, Status> {
        let request = request.into_inner();
        let repo_path = self.resolve_repo_path(request.repository)?;
        if request.commit_ids.is_empty() {
            return Err(Status::invalid_argument(
                "commit_ids must contain at least one commit",
            ));
        }

        let mut responses = Vec::new();
        for commit_id in request.commit_ids {
            if commit_id.trim().is_empty() {
                continue;
            }

            let output = git_output(
                &repo_path,
                ["show", "-s", "--format=%B", commit_id.as_str()],
            )
            .await?;
            if !output.status.success() {
                if is_invalid_revision_error(&output) || is_missing_object_error(&output) {
                    continue;
                }

                return Err(status_for_git_failure(
                    "-C <repo> show -s --format=%B <commit_id>",
                    &output,
                ));
            }

            responses.push(GetCommitMessagesResponse {
                commit_id,
                message: output.stdout,
            });
        }

        Ok(Response::new(Box::pin(tokio_stream::iter(
            responses.into_iter().map(Ok),
        ))))
    }

    async fn check_objects_exist(
        &self,
        request: Request<tonic::Streaming<CheckObjectsExistRequest>>,
    ) -> Result<Response<Self::CheckObjectsExistStream>, Status> {
        let mut stream = request.into_inner();
        let mut repository = None;
        let mut revisions = Vec::new();
        while let Some(chunk) = stream.next().await {
            let chunk = chunk?;
            if repository.is_none() && chunk.repository.is_some() {
                repository = chunk.repository;
            }
            revisions.extend(chunk.revisions);
        }

        let repo_path = self.resolve_repo_path(repository)?;

        let mut response_revisions = Vec::new();
        for revision in revisions {
            let revision_text = decode_utf8("revisions", revision.clone())?;
            if revision_text.trim().is_empty() {
                response_revisions.push(check_objects_exist_response::RevisionExistence {
                    name: revision,
                    exists: false,
                });
                continue;
            }

            let output = git_output(
                &repo_path,
                ["rev-parse", "--verify", "--quiet", revision_text.as_str()],
            )
            .await?;

            if !output.status.success()
                && !is_invalid_revision_error(&output)
                && !is_missing_object_error(&output)
            {
                return Err(status_for_git_failure(
                    "-C <repo> rev-parse --verify --quiet <revision>",
                    &output,
                ));
            }

            response_revisions.push(check_objects_exist_response::RevisionExistence {
                name: revision,
                exists: output.status.success(),
            });
        }

        Ok(build_stream_response(CheckObjectsExistResponse {
            revisions: response_revisions,
        }))
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

    use gitaly_proto::gitaly::commit_service_server::CommitService;
    use gitaly_proto::gitaly::get_tree_entries_request;
    use gitaly_proto::gitaly::{
        tree_entry_response, CommitLanguagesRequest, CommitStatsRequest, CommitsByMessageRequest,
        CountCommitsRequest, CountDivergingCommitsRequest, FindAllCommitsRequest,
        FindCommitRequest, FindCommitsRequest, GetCommitMessagesRequest,
        GetCommitSignaturesRequest, GetTreeEntriesRequest, LastCommitForPathRequest,
        ListCommitsByOidRequest, ListCommitsByRefNameRequest, ListCommitsRequest, ListFilesRequest,
        ListLastCommitsForTreeRequest, PaginationParameter, RawBlameRequest, Repository,
        TreeEntryRequest,
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
        repo_path: std::path::PathBuf,
        service: CommitServiceImpl,
        repository: Repository,
        head_commit: String,
        feature_commit: String,
        readme_oid: String,
        readme_data: Vec<u8>,
    }

    impl TestRepo {
        fn setup(name: &str) -> Self {
            let storage_root = unique_dir(name);
            let repo_path = storage_root.join("project.git");

            std::fs::create_dir_all(&storage_root).expect("storage root should be creatable");
            std::fs::create_dir_all(&repo_path).expect("repo path should be creatable");

            run_git(&repo_path, &["init", "--quiet"]);
            run_git(&repo_path, &["config", "user.name", "Commit Service Tests"]);
            run_git(
                &repo_path,
                &["config", "user.email", "commit-service-tests@example.com"],
            );

            std::fs::write(repo_path.join("README.md"), b"line 1\nline 2\n")
                .expect("README should write");
            run_git(&repo_path, &["add", "README.md"]);
            run_git(&repo_path, &["commit", "--quiet", "-m", "initial"]);

            std::fs::create_dir_all(repo_path.join("src")).expect("src should be creatable");
            std::fs::write(repo_path.join("README.md"), b"line 1\nline 2\nline 3\n")
                .expect("README should rewrite");
            std::fs::write(repo_path.join("src/lib.rs"), b"pub fn demo() {}\n")
                .expect("lib.rs should write");
            run_git(&repo_path, &["add", "README.md", "src/lib.rs"]);
            run_git(&repo_path, &["commit", "--quiet", "-m", "second"]);

            let first_commit = run_git_stdout(&repo_path, &["rev-parse", "HEAD~1"]);
            run_git(
                &repo_path,
                &[
                    "checkout",
                    "--quiet",
                    "-b",
                    "feature",
                    first_commit.as_str(),
                ],
            );
            std::fs::create_dir_all(repo_path.join("src"))
                .expect("feature src should be creatable");
            std::fs::write(repo_path.join("src/feature.rs"), b"pub fn feature() {}\n")
                .expect("feature.rs should write");
            run_git(&repo_path, &["add", "src/feature.rs"]);
            run_git(&repo_path, &["commit", "--quiet", "-m", "feature"]);
            let feature_commit = run_git_stdout(&repo_path, &["rev-parse", "HEAD"]);
            run_git(&repo_path, &["checkout", "--quiet", "master"]);

            let head_commit = run_git_stdout(&repo_path, &["rev-parse", "HEAD"]);
            let readme_oid = run_git_stdout(&repo_path, &["rev-parse", "HEAD:README.md"]);
            let readme_data =
                std::fs::read(repo_path.join("README.md")).expect("README should read");

            let mut storage_paths = HashMap::new();
            storage_paths.insert("default".to_string(), storage_root.clone());
            let service = CommitServiceImpl::new(Arc::new(
                Dependencies::default().with_storage_paths(storage_paths),
            ));

            let repository = Repository {
                storage_name: "default".to_string(),
                relative_path: "project.git".to_string(),
                ..Repository::default()
            };

            Self {
                storage_root,
                repo_path,
                service,
                repository,
                head_commit,
                feature_commit,
                readme_oid,
                readme_data,
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
    async fn find_commit_and_list_commits_return_known_head() {
        let test_repo = TestRepo::setup("commit-service-basic");

        let find_response = test_repo
            .service
            .find_commit(Request::new(FindCommitRequest {
                repository: Some(test_repo.repository.clone()),
                revision: b"HEAD".to_vec(),
                trailers: false,
            }))
            .await
            .expect("find_commit should succeed")
            .into_inner();

        assert_eq!(
            find_response.commit.expect("commit should exist").id,
            test_repo.head_commit
        );

        let mut stream = test_repo
            .service
            .list_commits(Request::new(ListCommitsRequest {
                repository: Some(test_repo.repository.clone()),
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
        assert!(first_page
            .commits
            .iter()
            .any(|commit| commit.id == test_repo.head_commit));

        let count = test_repo
            .service
            .count_commits(Request::new(CountCommitsRequest {
                repository: Some(test_repo.repository.clone()),
                revision: b"HEAD".to_vec(),
                ..CountCommitsRequest::default()
            }))
            .await
            .expect("count_commits should succeed")
            .into_inner();
        assert!(count.count >= 1);
    }

    #[tokio::test]
    async fn tree_listing_stats_and_last_commit_use_git_data() {
        let test_repo = TestRepo::setup("commit-service-tree-and-stats");

        let mut tree_entry_stream = test_repo
            .service
            .tree_entry(Request::new(TreeEntryRequest {
                repository: Some(test_repo.repository.clone()),
                revision: b"HEAD".to_vec(),
                path: b"README.md".to_vec(),
                limit: 0,
                max_size: 0,
            }))
            .await
            .expect("tree_entry should succeed")
            .into_inner();
        let tree_entry = tree_entry_stream
            .next()
            .await
            .expect("tree entry stream should yield")
            .expect("tree entry should not error");

        assert_eq!(tree_entry.oid, test_repo.readme_oid);
        assert_eq!(
            tree_entry.r#type,
            tree_entry_response::ObjectType::Blob as i32
        );
        assert_eq!(tree_entry.mode, 0o100644);
        assert_eq!(tree_entry.data, test_repo.readme_data);

        let mut get_tree_entries_stream = test_repo
            .service
            .get_tree_entries(Request::new(GetTreeEntriesRequest {
                repository: Some(test_repo.repository.clone()),
                revision: b"HEAD".to_vec(),
                path: Vec::new(),
                recursive: true,
                sort: get_tree_entries_request::SortBy::Filesystem as i32,
                ..GetTreeEntriesRequest::default()
            }))
            .await
            .expect("get_tree_entries should succeed")
            .into_inner();
        let get_tree_entries_response = get_tree_entries_stream
            .next()
            .await
            .expect("get_tree_entries stream should yield")
            .expect("get_tree_entries response should not error");

        let listed_paths = get_tree_entries_response
            .entries
            .iter()
            .map(|entry| String::from_utf8(entry.path.clone()).expect("tree path should be UTF-8"))
            .collect::<Vec<_>>();
        assert_eq!(listed_paths, vec!["README.md", "src/lib.rs"]);

        let mut paginated_stream = test_repo
            .service
            .get_tree_entries(Request::new(GetTreeEntriesRequest {
                repository: Some(test_repo.repository.clone()),
                revision: b"HEAD".to_vec(),
                path: Vec::new(),
                recursive: true,
                sort: get_tree_entries_request::SortBy::Filesystem as i32,
                pagination_params: Some(PaginationParameter {
                    page_token: String::new(),
                    limit: 1,
                }),
                ..GetTreeEntriesRequest::default()
            }))
            .await
            .expect("paginated get_tree_entries should succeed")
            .into_inner();
        let paginated_response = paginated_stream
            .next()
            .await
            .expect("paginated stream should yield")
            .expect("paginated response should not error");
        assert_eq!(paginated_response.entries.len(), 1);
        assert!(paginated_response.pagination_cursor.is_some());

        let mut list_files_stream = test_repo
            .service
            .list_files(Request::new(ListFilesRequest {
                repository: Some(test_repo.repository.clone()),
                revision: b"HEAD".to_vec(),
            }))
            .await
            .expect("list_files should succeed")
            .into_inner();
        let list_files_response = list_files_stream
            .next()
            .await
            .expect("list_files stream should yield")
            .expect("list_files response should not error");

        let mut file_paths = list_files_response
            .paths
            .iter()
            .map(|path| String::from_utf8(path.clone()).expect("file path should be UTF-8"))
            .collect::<Vec<_>>();
        file_paths.sort();
        assert_eq!(file_paths, vec!["README.md", "src/lib.rs"]);

        let stats = test_repo
            .service
            .commit_stats(Request::new(CommitStatsRequest {
                repository: Some(test_repo.repository.clone()),
                revision: b"HEAD".to_vec(),
            }))
            .await
            .expect("commit_stats should succeed")
            .into_inner();
        assert_eq!(stats.oid, test_repo.head_commit);
        assert_eq!(stats.additions, 2);
        assert_eq!(stats.deletions, 0);
        assert_eq!(stats.files, 2);

        let missing_stats = test_repo
            .service
            .commit_stats(Request::new(CommitStatsRequest {
                repository: Some(test_repo.repository.clone()),
                revision: b"does-not-exist".to_vec(),
            }))
            .await
            .expect("missing commit_stats should still succeed")
            .into_inner();
        assert_eq!(missing_stats.oid, "");
        assert_eq!(missing_stats.additions, 0);
        assert_eq!(missing_stats.deletions, 0);
        assert_eq!(missing_stats.files, 0);

        let last_commit = test_repo
            .service
            .last_commit_for_path(Request::new(LastCommitForPathRequest {
                repository: Some(test_repo.repository.clone()),
                revision: b"HEAD".to_vec(),
                path: b"README.md".to_vec(),
                ..LastCommitForPathRequest::default()
            }))
            .await
            .expect("last_commit_for_path should succeed")
            .into_inner();
        assert_eq!(
            last_commit.commit.expect("commit should be present").id,
            test_repo.head_commit
        );

        let missing_last_commit = test_repo
            .service
            .last_commit_for_path(Request::new(LastCommitForPathRequest {
                repository: Some(test_repo.repository.clone()),
                revision: b"HEAD".to_vec(),
                path: b"missing.txt".to_vec(),
                ..LastCommitForPathRequest::default()
            }))
            .await
            .expect("missing path lookup should succeed")
            .into_inner();
        assert!(missing_last_commit.commit.is_none());
    }

    #[tokio::test]
    async fn remaining_read_methods_return_expected_data() {
        let test_repo = TestRepo::setup("commit-service-remaining-methods");

        let diverging = test_repo
            .service
            .count_diverging_commits(Request::new(CountDivergingCommitsRequest {
                repository: Some(test_repo.repository.clone()),
                from: test_repo.head_commit.as_bytes().to_vec(),
                to: test_repo.feature_commit.as_bytes().to_vec(),
                max_count: 0,
            }))
            .await
            .expect("count_diverging_commits should succeed")
            .into_inner();
        assert!(diverging.left_count >= 1);
        assert!(diverging.right_count >= 1);

        let mut find_all_stream = test_repo
            .service
            .find_all_commits(Request::new(FindAllCommitsRequest {
                repository: Some(test_repo.repository.clone()),
                revision: b"HEAD".to_vec(),
                max_count: 20,
                ..FindAllCommitsRequest::default()
            }))
            .await
            .expect("find_all_commits should succeed")
            .into_inner();
        let find_all_page = find_all_stream
            .next()
            .await
            .expect("find_all_commits should yield")
            .expect("find_all_commits page should not error");
        assert!(find_all_page
            .commits
            .iter()
            .any(|commit| commit.id == test_repo.head_commit));

        let mut find_commits_stream = test_repo
            .service
            .find_commits(Request::new(FindCommitsRequest {
                repository: Some(test_repo.repository.clone()),
                revision: b"HEAD".to_vec(),
                paths: vec![b"README.md".to_vec()],
                limit: 20,
                ..FindCommitsRequest::default()
            }))
            .await
            .expect("find_commits should succeed")
            .into_inner();
        let find_commits_page = find_commits_stream
            .next()
            .await
            .expect("find_commits should yield")
            .expect("find_commits page should not error");
        assert!(find_commits_page
            .commits
            .iter()
            .any(|commit| commit.id == test_repo.head_commit));

        let languages = test_repo
            .service
            .commit_languages(Request::new(CommitLanguagesRequest {
                repository: Some(test_repo.repository.clone()),
                revision: b"HEAD".to_vec(),
            }))
            .await
            .expect("commit_languages should succeed")
            .into_inner();
        assert!(!languages.languages.is_empty());

        let mut raw_blame_stream = test_repo
            .service
            .raw_blame(Request::new(RawBlameRequest {
                repository: Some(test_repo.repository.clone()),
                revision: b"HEAD".to_vec(),
                path: b"README.md".to_vec(),
                ..RawBlameRequest::default()
            }))
            .await
            .expect("raw_blame should succeed")
            .into_inner();
        let raw_blame_page = raw_blame_stream
            .next()
            .await
            .expect("raw_blame should yield")
            .expect("raw_blame page should not error");
        assert!(!raw_blame_page.data.is_empty());

        let mut last_commits_for_tree_stream = test_repo
            .service
            .list_last_commits_for_tree(Request::new(ListLastCommitsForTreeRequest {
                repository: Some(test_repo.repository.clone()),
                revision: "HEAD".to_string(),
                path: Vec::new(),
                limit: 10,
                offset: 0,
                ..ListLastCommitsForTreeRequest::default()
            }))
            .await
            .expect("list_last_commits_for_tree should succeed")
            .into_inner();
        let last_commits_for_tree = last_commits_for_tree_stream
            .next()
            .await
            .expect("list_last_commits_for_tree should yield")
            .expect("list_last_commits_for_tree page should not error");
        assert!(!last_commits_for_tree.commits.is_empty());

        let mut commits_by_message_stream = test_repo
            .service
            .commits_by_message(Request::new(CommitsByMessageRequest {
                repository: Some(test_repo.repository.clone()),
                revision: b"HEAD".to_vec(),
                query: "second".to_string(),
                limit: 10,
                ..CommitsByMessageRequest::default()
            }))
            .await
            .expect("commits_by_message should succeed")
            .into_inner();
        let commits_by_message_page = commits_by_message_stream
            .next()
            .await
            .expect("commits_by_message should yield")
            .expect("commits_by_message page should not error");
        assert!(commits_by_message_page
            .commits
            .iter()
            .any(|commit| commit.id == test_repo.head_commit));

        let mut commits_by_oid_stream = test_repo
            .service
            .list_commits_by_oid(Request::new(ListCommitsByOidRequest {
                repository: Some(test_repo.repository.clone()),
                oid: vec![test_repo.head_commit.clone(), "does-not-exist".to_string()],
            }))
            .await
            .expect("list_commits_by_oid should succeed")
            .into_inner();
        let commits_by_oid_page = commits_by_oid_stream
            .next()
            .await
            .expect("list_commits_by_oid should yield")
            .expect("list_commits_by_oid page should not error");
        assert_eq!(commits_by_oid_page.commits.len(), 1);
        assert_eq!(commits_by_oid_page.commits[0].id, test_repo.head_commit);

        let mut commits_by_ref_stream = test_repo
            .service
            .list_commits_by_ref_name(Request::new(ListCommitsByRefNameRequest {
                repository: Some(test_repo.repository.clone()),
                ref_names: vec![
                    b"refs/heads/master".to_vec(),
                    b"refs/heads/feature".to_vec(),
                ],
            }))
            .await
            .expect("list_commits_by_ref_name should succeed")
            .into_inner();
        let commits_by_ref_page = commits_by_ref_stream
            .next()
            .await
            .expect("list_commits_by_ref_name should yield")
            .expect("list_commits_by_ref_name page should not error");
        assert_eq!(commits_by_ref_page.commit_refs.len(), 2);

        let mut get_signatures_stream = test_repo
            .service
            .get_commit_signatures(Request::new(GetCommitSignaturesRequest {
                repository: Some(test_repo.repository.clone()),
                commit_ids: vec![test_repo.head_commit.clone()],
            }))
            .await
            .expect("get_commit_signatures should succeed")
            .into_inner();
        if let Some(signature_response) = get_signatures_stream.next().await {
            let signature_response =
                signature_response.expect("signature response should be valid");
            assert_eq!(signature_response.commit_id, test_repo.head_commit);
            assert!(!signature_response.signature.is_empty());
        }

        let mut get_messages_stream = test_repo
            .service
            .get_commit_messages(Request::new(GetCommitMessagesRequest {
                repository: Some(test_repo.repository.clone()),
                commit_ids: vec![test_repo.head_commit.clone()],
            }))
            .await
            .expect("get_commit_messages should succeed")
            .into_inner();
        let message_page = get_messages_stream
            .next()
            .await
            .expect("get_commit_messages should yield")
            .expect("get_commit_messages page should not error");
        assert_eq!(message_page.commit_id, test_repo.head_commit);
        assert!(String::from_utf8_lossy(&message_page.message).contains("second"));

        assert!(test_repo.repo_path.exists());
    }

    #[tokio::test]
    async fn tree_entry_returns_not_found_for_missing_path() {
        let test_repo = TestRepo::setup("commit-service-missing-tree-entry");

        let result = test_repo
            .service
            .tree_entry(Request::new(TreeEntryRequest {
                repository: Some(test_repo.repository.clone()),
                revision: b"HEAD".to_vec(),
                path: b"missing.txt".to_vec(),
                limit: 0,
                max_size: 0,
            }))
            .await;

        let err = match result {
            Ok(_) => panic!("tree_entry should return not found for missing path"),
            Err(err) => err,
        };

        assert_eq!(err.code(), Code::NotFound);
    }
}
