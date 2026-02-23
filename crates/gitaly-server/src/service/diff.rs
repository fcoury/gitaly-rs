use std::path::{Component, Path, PathBuf};
use std::pin::Pin;
use std::process::Stdio;
use std::sync::Arc;

use tokio::io::AsyncWriteExt;
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

fn validate_required_revision(value: &str, field: &str) -> Result<(), Status> {
    if value.trim().is_empty() {
        return Err(Status::invalid_argument(format!("{field} is required")));
    }

    Ok(())
}

fn parse_revision_bytes(value: Vec<u8>, field: &str) -> Result<String, Status> {
    String::from_utf8(value)
        .map_err(|_| Status::invalid_argument(format!("{field} must be valid UTF-8")))
}

fn parse_mode(mode: &str) -> i32 {
    i32::from_str_radix(mode, 8).unwrap_or(0)
}

fn parse_similarity_score(status_token: &str) -> i32 {
    status_token
        .chars()
        .skip(1)
        .collect::<String>()
        .parse::<i32>()
        .unwrap_or(0)
}

fn map_git_status(status: char) -> Option<i32> {
    let mapped = match status {
        'A' => changed_paths::Status::Added,
        'M' => changed_paths::Status::Modified,
        'D' => changed_paths::Status::Deleted,
        'T' => changed_paths::Status::TypeChange,
        'C' => changed_paths::Status::Copied,
        'R' => changed_paths::Status::Renamed,
        _ => return None,
    };

    Some(mapped as i32)
}

fn build_diff_filter_arg(diff_filters: &[i32]) -> Result<Option<String>, Status> {
    if diff_filters.is_empty() {
        return Ok(None);
    }

    let mut flags = String::new();
    for raw_filter in diff_filters {
        let filter = find_changed_paths_request::DiffStatus::try_from(*raw_filter)
            .map_err(|_| Status::invalid_argument("diff_filters contains unknown value"))?;

        let flag = match filter {
            find_changed_paths_request::DiffStatus::Added => 'A',
            find_changed_paths_request::DiffStatus::Modified => 'M',
            find_changed_paths_request::DiffStatus::Deleted => 'D',
            find_changed_paths_request::DiffStatus::TypeChange => 'T',
            find_changed_paths_request::DiffStatus::Copied => 'C',
            find_changed_paths_request::DiffStatus::Renamed => 'R',
            find_changed_paths_request::DiffStatus::Unspecified => {
                return Err(Status::invalid_argument(
                    "diff_filters must not contain DIFF_STATUS_UNSPECIFIED",
                ));
            }
        };

        if !flags.contains(flag) {
            flags.push(flag);
        }
    }

    if flags.is_empty() {
        return Ok(None);
    }

    Ok(Some(format!("--diff-filter={flags}")))
}

fn merge_mode_flag(mode: i32) -> Result<&'static str, Status> {
    match find_changed_paths_request::MergeCommitDiffMode::try_from(mode)
        .map_err(|_| Status::invalid_argument("merge_commit_diff_mode is unknown"))?
    {
        find_changed_paths_request::MergeCommitDiffMode::Unspecified
        | find_changed_paths_request::MergeCommitDiffMode::IncludeMerges => Ok("-m"),
        find_changed_paths_request::MergeCommitDiffMode::AllParents => Ok("-c"),
    }
}

fn parse_changed_paths_from_raw(stdout: &[u8], commit_id: Option<&str>) -> Vec<ChangedPaths> {
    let mut paths = Vec::new();
    let fields: Vec<&[u8]> = stdout
        .split(|byte| *byte == 0)
        .filter(|segment| !segment.is_empty())
        .collect();

    let mut index = 0usize;
    while index < fields.len() {
        let header = String::from_utf8_lossy(fields[index]).trim().to_string();
        index += 1;

        if !header.starts_with(':') {
            continue;
        }

        let is_combined = header.starts_with("::");
        let header_fields: Vec<&str> = header.trim_start_matches(':').split_whitespace().collect();
        if header_fields.len() < 5 {
            continue;
        }

        let status_token = header_fields.last().copied().unwrap_or_default();
        let status = status_token
            .chars()
            .find(|ch| *ch != '.')
            .unwrap_or_default();
        let Some(mapped_status) = map_git_status(status) else {
            continue;
        };

        let path_count = if matches!(status, 'R' | 'C') { 2 } else { 1 };
        if index + path_count > fields.len() {
            break;
        }

        let first_path = fields[index].to_vec();
        index += 1;
        let (old_path, path) = if path_count == 2 {
            let second_path = fields[index].to_vec();
            index += 1;
            (first_path, second_path)
        } else {
            (Vec::new(), first_path)
        };

        let (old_mode, new_mode, old_blob_id, new_blob_id) = if is_combined {
            let parent_count = status_token.chars().count();
            let minimum_fields = parent_count.saturating_mul(2).saturating_add(3);
            if parent_count > 0 && header_fields.len() >= minimum_fields {
                let new_mode_index = parent_count;
                let old_blob_index = parent_count + 1;
                let new_blob_index = parent_count * 2 + 1;

                (
                    parse_mode(header_fields[0]),
                    parse_mode(header_fields[new_mode_index]),
                    header_fields[old_blob_index].to_string(),
                    header_fields[new_blob_index].to_string(),
                )
            } else {
                (0, 0, String::new(), String::new())
            }
        } else {
            (
                parse_mode(header_fields[0]),
                parse_mode(header_fields[1]),
                header_fields
                    .get(2)
                    .copied()
                    .unwrap_or_default()
                    .to_string(),
                header_fields
                    .get(3)
                    .copied()
                    .unwrap_or_default()
                    .to_string(),
            )
        };

        paths.push(ChangedPaths {
            path,
            status: mapped_status,
            old_mode,
            new_mode,
            old_blob_id,
            new_blob_id,
            old_path,
            score: if matches!(status, 'R' | 'C') {
                parse_similarity_score(status_token)
            } else {
                0
            },
            commit_id: commit_id.unwrap_or_default().to_string(),
        });
    }

    paths
}

fn parse_range_spec_from_raw_request(
    range_spec: Option<raw_range_diff_request::RangeSpec>,
) -> Result<Vec<String>, Status> {
    match range_spec {
        Some(raw_range_diff_request::RangeSpec::RangePair(pair)) => {
            validate_required_revision(&pair.range1, "range_pair.range1")?;
            validate_required_revision(&pair.range2, "range_pair.range2")?;
            Ok(vec![pair.range1, pair.range2])
        }
        Some(raw_range_diff_request::RangeSpec::RevisionRange(range)) => {
            validate_required_revision(&range.rev1, "revision_range.rev1")?;
            validate_required_revision(&range.rev2, "revision_range.rev2")?;
            Ok(vec![format!("{}...{}", range.rev1, range.rev2)])
        }
        Some(raw_range_diff_request::RangeSpec::BaseWithRevisions(range)) => {
            validate_required_revision(&range.base, "base_with_revisions.base")?;
            validate_required_revision(&range.rev1, "base_with_revisions.rev1")?;
            validate_required_revision(&range.rev2, "base_with_revisions.rev2")?;
            Ok(vec![range.base, range.rev1, range.rev2])
        }
        None => Err(Status::invalid_argument("range_spec is required")),
    }
}

fn parse_range_spec_from_range_request(
    range_spec: Option<range_diff_request::RangeSpec>,
) -> Result<Vec<String>, Status> {
    match range_spec {
        Some(range_diff_request::RangeSpec::RangePair(pair)) => {
            validate_required_revision(&pair.range1, "range_pair.range1")?;
            validate_required_revision(&pair.range2, "range_pair.range2")?;
            Ok(vec![pair.range1, pair.range2])
        }
        Some(range_diff_request::RangeSpec::RevisionRange(range)) => {
            validate_required_revision(&range.rev1, "revision_range.rev1")?;
            validate_required_revision(&range.rev2, "revision_range.rev2")?;
            Ok(vec![format!("{}...{}", range.rev1, range.rev2)])
        }
        Some(range_diff_request::RangeSpec::BaseWithRevisions(range)) => {
            validate_required_revision(&range.base, "base_with_revisions.base")?;
            validate_required_revision(&range.rev1, "base_with_revisions.rev1")?;
            validate_required_revision(&range.rev2, "base_with_revisions.rev2")?;
            Ok(vec![range.base, range.rev1, range.rev2])
        }
        None => Err(Status::invalid_argument("range_spec is required")),
    }
}

fn parse_range_diff_pair_line(line: &str) -> Option<(String, String, i32, String)> {
    let (left_index, remainder) = line.split_once(':')?;
    let left_index = left_index.trim();
    if left_index.is_empty() {
        return None;
    }

    if !left_index
        .chars()
        .all(|character| character == '-' || character.is_ascii_digit())
    {
        return None;
    }

    let tokens: Vec<&str> = remainder.split_whitespace().collect();
    if tokens.len() < 4 || !tokens[2].ends_with(':') {
        return None;
    }

    let comparison = match tokens[1] {
        "=" => range_diff_response::Comparator::EqualUnspecified as i32,
        ">" => range_diff_response::Comparator::GreaterThan as i32,
        "<" => range_diff_response::Comparator::LessThan as i32,
        "!" => range_diff_response::Comparator::NotEqual as i32,
        _ => return None,
    };

    let from_commit_id = if tokens[0] == "-------" {
        String::new()
    } else {
        tokens[0].to_string()
    };
    let to_commit_id = if tokens[3] == "-------" {
        String::new()
    } else {
        tokens[3].to_string()
    };
    let commit_message_title = if tokens.len() > 4 {
        tokens[4..].join(" ")
    } else {
        String::new()
    };

    Some((
        from_commit_id,
        to_commit_id,
        comparison,
        commit_message_title,
    ))
}

fn push_range_diff_responses(
    responses: &mut Vec<RangeDiffResponse>,
    from_commit_id: String,
    to_commit_id: String,
    comparison: i32,
    commit_message_title: String,
    patch_data: Vec<u8>,
) {
    if patch_data.is_empty() {
        responses.push(RangeDiffResponse {
            from_commit_id,
            to_commit_id,
            comparison,
            commit_message_title,
            patch_data: Vec::new(),
            end_of_patch: true,
        });
        return;
    }

    let chunk_count = patch_data.chunks(STREAM_CHUNK_SIZE).len();
    for (index, chunk) in patch_data.chunks(STREAM_CHUNK_SIZE).enumerate() {
        responses.push(RangeDiffResponse {
            from_commit_id: from_commit_id.clone(),
            to_commit_id: to_commit_id.clone(),
            comparison,
            commit_message_title: commit_message_title.clone(),
            patch_data: chunk.to_vec(),
            end_of_patch: index + 1 == chunk_count,
        });
    }
}

fn parse_range_diff_responses(stdout: &[u8]) -> Vec<RangeDiffResponse> {
    let text = String::from_utf8_lossy(stdout);
    let mut responses = Vec::new();
    let mut current: Option<(String, String, i32, String)> = None;
    let mut patch_data = Vec::new();

    for line in text.split_inclusive('\n') {
        let pair_line = line.trim_end_matches(|character| character == '\n' || character == '\r');
        if let Some((from_commit_id, to_commit_id, comparison, commit_message_title)) =
            parse_range_diff_pair_line(pair_line)
        {
            if let Some((from, to, previous_comparison, title)) = current.take() {
                push_range_diff_responses(
                    &mut responses,
                    from,
                    to,
                    previous_comparison,
                    title,
                    std::mem::take(&mut patch_data),
                );
            }

            current = Some((
                from_commit_id,
                to_commit_id,
                comparison,
                commit_message_title,
            ));
            continue;
        }

        if current.is_some() {
            patch_data.extend_from_slice(line.as_bytes());
        }
    }

    if let Some((from_commit_id, to_commit_id, comparison, commit_message_title)) = current.take() {
        push_range_diff_responses(
            &mut responses,
            from_commit_id,
            to_commit_id,
            comparison,
            commit_message_title,
            patch_data,
        );
    }

    responses
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
        request: Request<FindChangedPathsRequest>,
    ) -> Result<Response<Self::FindChangedPathsStream>, Status> {
        enum ParsedRequest {
            Tree {
                left_tree_revision: String,
                right_tree_revision: String,
            },
            Commit {
                commit_revision: String,
                parent_commit_revisions: Vec<String>,
            },
        }

        let request = request.into_inner();
        let repo_path = self.resolve_repo_path(request.repository)?;

        #[allow(deprecated)]
        let parsed_requests = if request.requests.is_empty() {
            if request.commits.is_empty() {
                return Err(Status::invalid_argument(
                    "requests or commits must contain at least one entry",
                ));
            }

            request
                .commits
                .into_iter()
                .map(|commit_revision| {
                    validate_required_revision(&commit_revision, "commits[]")?;
                    Ok(ParsedRequest::Commit {
                        commit_revision,
                        parent_commit_revisions: Vec::new(),
                    })
                })
                .collect::<Result<Vec<_>, Status>>()?
        } else {
            request
                .requests
                .into_iter()
                .map(|request| match request.r#type {
                    Some(find_changed_paths_request::request::Type::TreeRequest(tree_request)) => {
                        validate_required_revision(
                            &tree_request.left_tree_revision,
                            "requests[].tree_request.left_tree_revision",
                        )?;
                        validate_required_revision(
                            &tree_request.right_tree_revision,
                            "requests[].tree_request.right_tree_revision",
                        )?;

                        Ok(ParsedRequest::Tree {
                            left_tree_revision: tree_request.left_tree_revision,
                            right_tree_revision: tree_request.right_tree_revision,
                        })
                    }
                    Some(find_changed_paths_request::request::Type::CommitRequest(
                        commit_request,
                    )) => {
                        validate_required_revision(
                            &commit_request.commit_revision,
                            "requests[].commit_request.commit_revision",
                        )?;

                        for parent_commit_revision in &commit_request.parent_commit_revisions {
                            validate_required_revision(
                                parent_commit_revision,
                                "requests[].commit_request.parent_commit_revisions[]",
                            )?;
                        }

                        Ok(ParsedRequest::Commit {
                            commit_revision: commit_request.commit_revision,
                            parent_commit_revisions: commit_request.parent_commit_revisions,
                        })
                    }
                    None => Err(Status::invalid_argument("requests[].type is required")),
                })
                .collect::<Result<Vec<_>, Status>>()?
        };

        let all_commit_requests = parsed_requests
            .iter()
            .all(|parsed| matches!(parsed, ParsedRequest::Commit { .. }));

        let mut base_args = vec![
            "diff-tree".to_string(),
            "--raw".to_string(),
            "-r".to_string(),
            "--no-abbrev".to_string(),
            "--no-commit-id".to_string(),
            "-z".to_string(),
        ];
        if request.find_renames {
            base_args.push("-M".to_string());
        }
        if let Some(diff_filter_arg) = build_diff_filter_arg(&request.diff_filters)? {
            base_args.push(diff_filter_arg);
        }

        let merge_mode_flag = merge_mode_flag(request.merge_commit_diff_mode)?;
        let mut changed_paths = Vec::new();

        for parsed_request in parsed_requests {
            match parsed_request {
                ParsedRequest::Tree {
                    left_tree_revision,
                    right_tree_revision,
                } => {
                    let mut args = base_args.clone();
                    args.push(left_tree_revision);
                    args.push(right_tree_revision);

                    let output = git_output(&repo_path, args.iter().map(String::as_str)).await?;
                    if !output.status.success() {
                        return Err(status_for_git_failure(
                            "-C <repo> diff-tree --raw ... <left_tree_revision> <right_tree_revision>",
                            &output,
                        ));
                    }

                    changed_paths.extend(parse_changed_paths_from_raw(&output.stdout, None));
                }
                ParsedRequest::Commit {
                    commit_revision,
                    parent_commit_revisions,
                } => {
                    let commit_id = all_commit_requests.then_some(commit_revision.clone());

                    if parent_commit_revisions.is_empty() {
                        let mut args = base_args.clone();
                        args.push(merge_mode_flag.to_string());
                        args.push("--root".to_string());
                        args.push(commit_revision.clone());

                        let output =
                            git_output(&repo_path, args.iter().map(String::as_str)).await?;
                        if !output.status.success() {
                            return Err(status_for_git_failure(
                                "-C <repo> diff-tree --raw ... <commit_revision>",
                                &output,
                            ));
                        }

                        changed_paths.extend(parse_changed_paths_from_raw(
                            &output.stdout,
                            commit_id.as_deref(),
                        ));
                    } else {
                        for parent_commit_revision in parent_commit_revisions {
                            let mut args = base_args.clone();
                            args.push(parent_commit_revision);
                            args.push(commit_revision.clone());

                            let output =
                                git_output(&repo_path, args.iter().map(String::as_str)).await?;
                            if !output.status.success() {
                                return Err(status_for_git_failure(
                                    "-C <repo> diff-tree --raw ... <parent_commit_revision> <commit_revision>",
                                    &output,
                                ));
                            }

                            changed_paths.extend(parse_changed_paths_from_raw(
                                &output.stdout,
                                commit_id.as_deref(),
                            ));
                        }
                    }
                }
            }
        }

        Ok(stream_from_values(vec![FindChangedPathsResponse {
            paths: changed_paths,
        }]))
    }

    async fn get_patch_id(
        &self,
        request: Request<GetPatchIdRequest>,
    ) -> Result<Response<GetPatchIdResponse>, Status> {
        let request = request.into_inner();
        let repo_path = self.resolve_repo_path(request.repository)?;
        let old_revision = parse_revision_bytes(request.old_revision, "old_revision")?;
        let new_revision = parse_revision_bytes(request.new_revision, "new_revision")?;

        validate_required_revision(&old_revision, "old_revision")?;
        validate_required_revision(&new_revision, "new_revision")?;

        let diff_output = git_output(
            &repo_path,
            ["diff", old_revision.as_str(), new_revision.as_str()],
        )
        .await?;
        if !diff_output.status.success() {
            return Err(status_for_git_failure(
                "-C <repo> diff <old_revision> <new_revision>",
                &diff_output,
            ));
        }

        if diff_output.stdout.is_empty() {
            return Err(Status::failed_precondition(
                "no patch-id for empty diff between old_revision and new_revision",
            ));
        }

        let mut command = Command::new("git");
        command
            .arg("-C")
            .arg(&repo_path)
            .args(["patch-id", "--stable"])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());

        let mut child = command.spawn().map_err(|error| {
            Status::internal(format!("failed to execute git patch-id: {error}"))
        })?;

        let mut stdin = child
            .stdin
            .take()
            .ok_or_else(|| Status::internal("failed to open git patch-id stdin"))?;
        stdin
            .write_all(&diff_output.stdout)
            .await
            .map_err(|error| {
                Status::internal(format!("failed to write git patch-id stdin: {error}"))
            })?;
        drop(stdin);

        let patch_id_output = child.wait_with_output().await.map_err(|error| {
            Status::internal(format!("failed to read git patch-id output: {error}"))
        })?;
        if !patch_id_output.status.success() {
            return Err(status_for_git_failure(
                "-C <repo> patch-id --stable",
                &patch_id_output,
            ));
        }

        let patch_id = String::from_utf8_lossy(&patch_id_output.stdout)
            .split_whitespace()
            .next()
            .unwrap_or_default()
            .to_string();
        if patch_id.is_empty() {
            return Err(Status::internal(
                "git patch-id produced no patch ID from diff output",
            ));
        }

        Ok(Response::new(GetPatchIdResponse { patch_id }))
    }

    async fn raw_range_diff(
        &self,
        request: Request<RawRangeDiffRequest>,
    ) -> Result<Response<Self::RawRangeDiffStream>, Status> {
        let request = request.into_inner();
        let repo_path = self.resolve_repo_path(request.repository)?;
        let range_args = parse_range_spec_from_raw_request(request.range_spec)?;

        let mut args = vec!["range-diff".to_string()];
        args.extend(range_args);

        let output = git_output(&repo_path, args.iter().map(String::as_str)).await?;
        if !output.status.success() {
            return Err(status_for_git_failure(
                "-C <repo> range-diff <range-spec>",
                &output,
            ));
        }

        Ok(stream_bytes(output.stdout, |data| RawRangeDiffResponse {
            data,
        }))
    }

    async fn range_diff(
        &self,
        request: Request<RangeDiffRequest>,
    ) -> Result<Response<Self::RangeDiffStream>, Status> {
        let request = request.into_inner();
        let repo_path = self.resolve_repo_path(request.repository)?;
        let range_args = parse_range_spec_from_range_request(request.range_spec)?;

        let mut args = vec!["range-diff".to_string()];
        args.extend(range_args);

        let output = git_output(&repo_path, args.iter().map(String::as_str)).await?;
        if !output.status.success() {
            return Err(status_for_git_failure(
                "-C <repo> range-diff <range-spec>",
                &output,
            ));
        }

        Ok(stream_from_values(parse_range_diff_responses(
            &output.stdout,
        )))
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
    use tonic::{Code, Request};

    use gitaly_proto::gitaly::diff_service_server::DiffService;
    use gitaly_proto::gitaly::{
        changed_paths, find_changed_paths_request, range_diff_request, range_diff_response,
        raw_range_diff_request, DiffStatsRequest, FindChangedPathsRequest, GetPatchIdRequest,
        RangeDiffRequest, RangePair, RawDiffRequest, RawPatchRequest, RawRangeDiffRequest,
        Repository,
    };

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

    fn init_test_repo(
        name: &str,
    ) -> (
        std::path::PathBuf,
        std::path::PathBuf,
        DiffServiceImpl,
        Repository,
    ) {
        let storage_root = unique_dir(name);
        let repo_path = storage_root.join("project.git");

        std::fs::create_dir_all(&storage_root).expect("storage root should be creatable");
        std::fs::create_dir_all(&repo_path).expect("repo path should be creatable");

        run_git(&repo_path, &["init", "--quiet"]);
        run_git(&repo_path, &["config", "user.name", "Diff Service Tests"]);
        run_git(
            &repo_path,
            &["config", "user.email", "diff-service-tests@example.com"],
        );

        let mut storage_paths = HashMap::new();
        storage_paths.insert("default".to_string(), storage_root.clone());

        let dependencies = Arc::new(Dependencies::default().with_storage_paths(storage_paths));
        let service = DiffServiceImpl::new(dependencies);
        let repository = Repository {
            storage_name: "default".to_string(),
            relative_path: "project.git".to_string(),
            ..Repository::default()
        };

        (storage_root, repo_path, service, repository)
    }

    #[tokio::test]
    async fn raw_diff_raw_patch_and_diff_stats_return_expected_content() {
        let (storage_root, repo_path, service, repository) = init_test_repo("diff-service");

        std::fs::write(repo_path.join("README.md"), b"hello\n").expect("README should write");
        run_git(&repo_path, &["add", "README.md"]);
        run_git(&repo_path, &["commit", "--quiet", "-m", "initial"]);

        std::fs::write(repo_path.join("README.md"), b"hello\nworld\n")
            .expect("README should update");
        run_git(&repo_path, &["add", "README.md"]);
        run_git(&repo_path, &["commit", "--quiet", "-m", "update readme"]);

        let left = git_stdout(&repo_path, &["rev-parse", "HEAD~1"]);
        let right = git_stdout(&repo_path, &["rev-parse", "HEAD"]);

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

    #[tokio::test]
    async fn find_changed_paths_returns_statuses_and_commit_id() {
        let (storage_root, repo_path, service, repository) = init_test_repo("find-changed-paths");

        std::fs::write(repo_path.join("README.md"), b"hello\n").expect("README should write");
        std::fs::write(repo_path.join("old_name.txt"), b"old\n").expect("old file should write");
        std::fs::write(repo_path.join("delete_me.txt"), b"delete\n")
            .expect("delete file should write");
        run_git(&repo_path, &["add", "."]);
        run_git(&repo_path, &["commit", "--quiet", "-m", "initial"]);

        std::fs::write(repo_path.join("README.md"), b"hello\nworld\n")
            .expect("README should update");
        std::fs::write(repo_path.join("added.txt"), b"added\n").expect("added file should write");
        run_git(&repo_path, &["mv", "old_name.txt", "renamed_name.txt"]);
        run_git(&repo_path, &["rm", "--quiet", "delete_me.txt"]);
        run_git(&repo_path, &["add", "."]);
        run_git(&repo_path, &["commit", "--quiet", "-m", "update files"]);

        let head = git_stdout(&repo_path, &["rev-parse", "HEAD"]);
        let mut stream = service
            .find_changed_paths(Request::new(FindChangedPathsRequest {
                repository: Some(repository.clone()),
                requests: vec![find_changed_paths_request::Request {
                    r#type: Some(find_changed_paths_request::request::Type::CommitRequest(
                        find_changed_paths_request::request::CommitRequest {
                            commit_revision: head.clone(),
                            parent_commit_revisions: Vec::new(),
                        },
                    )),
                }],
                merge_commit_diff_mode: find_changed_paths_request::MergeCommitDiffMode::Unspecified
                    as i32,
                find_renames: true,
                diff_filters: vec![
                    find_changed_paths_request::DiffStatus::Added as i32,
                    find_changed_paths_request::DiffStatus::Modified as i32,
                    find_changed_paths_request::DiffStatus::Deleted as i32,
                    find_changed_paths_request::DiffStatus::Renamed as i32,
                ],
                ..FindChangedPathsRequest::default()
            }))
            .await
            .expect("find_changed_paths should succeed")
            .into_inner();

        let mut paths = Vec::new();
        while let Some(page) = stream.next().await {
            let page = page.expect("find_changed_paths page should not error");
            paths.extend(page.paths);
        }

        let readme = paths
            .iter()
            .find(|path| path.path == b"README.md".to_vec())
            .expect("README.md should be present");
        assert_eq!(readme.status, changed_paths::Status::Modified as i32);
        assert_eq!(readme.commit_id, head);

        let added = paths
            .iter()
            .find(|path| path.path == b"added.txt".to_vec())
            .expect("added.txt should be present");
        assert_eq!(added.status, changed_paths::Status::Added as i32);

        let deleted = paths
            .iter()
            .find(|path| path.path == b"delete_me.txt".to_vec())
            .expect("delete_me.txt should be present");
        assert_eq!(deleted.status, changed_paths::Status::Deleted as i32);

        let renamed = paths
            .iter()
            .find(|path| path.path == b"renamed_name.txt".to_vec())
            .expect("renamed_name.txt should be present");
        assert_eq!(renamed.status, changed_paths::Status::Renamed as i32);
        assert_eq!(renamed.old_path, b"old_name.txt".to_vec());
        assert!(renamed.score > 0);

        let missing_request_error = match service
            .find_changed_paths(Request::new(FindChangedPathsRequest {
                repository: Some(repository),
                ..FindChangedPathsRequest::default()
            }))
            .await
        {
            Ok(_) => panic!("request without commits or requests should fail"),
            Err(error) => error,
        };
        assert_eq!(missing_request_error.code(), Code::InvalidArgument);

        if storage_root.exists() {
            std::fs::remove_dir_all(storage_root).expect("storage root should be removable");
        }
    }

    #[tokio::test]
    async fn get_patch_id_returns_hash_and_rejects_empty_diff() {
        let (storage_root, repo_path, service, repository) = init_test_repo("get-patch-id");

        std::fs::write(repo_path.join("README.md"), b"hello\n").expect("README should write");
        run_git(&repo_path, &["add", "README.md"]);
        run_git(&repo_path, &["commit", "--quiet", "-m", "initial"]);

        std::fs::write(repo_path.join("README.md"), b"hello\nworld\n")
            .expect("README should update");
        run_git(&repo_path, &["add", "README.md"]);
        run_git(&repo_path, &["commit", "--quiet", "-m", "second"]);

        let left = git_stdout(&repo_path, &["rev-parse", "HEAD~1"]);
        let right = git_stdout(&repo_path, &["rev-parse", "HEAD"]);

        let patch_id = service
            .get_patch_id(Request::new(GetPatchIdRequest {
                repository: Some(repository.clone()),
                old_revision: left.as_bytes().to_vec(),
                new_revision: right.as_bytes().to_vec(),
            }))
            .await
            .expect("get_patch_id should succeed")
            .into_inner()
            .patch_id;
        assert_eq!(patch_id.len(), 40);
        assert!(patch_id
            .chars()
            .all(|character| character.is_ascii_hexdigit()));

        let empty_diff_error = service
            .get_patch_id(Request::new(GetPatchIdRequest {
                repository: Some(repository),
                old_revision: right.as_bytes().to_vec(),
                new_revision: right.as_bytes().to_vec(),
            }))
            .await
            .expect_err("identical revisions should produce empty diff");
        assert_eq!(empty_diff_error.code(), Code::FailedPrecondition);

        if storage_root.exists() {
            std::fs::remove_dir_all(storage_root).expect("storage root should be removable");
        }
    }

    #[tokio::test]
    async fn raw_range_diff_and_range_diff_return_expected_output() {
        let (storage_root, repo_path, service, repository) = init_test_repo("range-diff");

        std::fs::write(repo_path.join("README.md"), b"base\n").expect("README should write");
        run_git(&repo_path, &["add", "README.md"]);
        run_git(&repo_path, &["commit", "--quiet", "-m", "base"]);
        let base = git_stdout(&repo_path, &["rev-parse", "HEAD"]);

        run_git(&repo_path, &["checkout", "--quiet", "-b", "range-left"]);
        std::fs::write(repo_path.join("README.md"), b"base\none\n")
            .expect("left write should succeed");
        run_git(&repo_path, &["add", "README.md"]);
        run_git(&repo_path, &["commit", "--quiet", "-m", "change one"]);
        let left_tip = git_stdout(&repo_path, &["rev-parse", "HEAD"]);

        run_git(&repo_path, &["checkout", "--quiet", base.as_str()]);
        run_git(&repo_path, &["checkout", "--quiet", "-b", "range-right"]);
        std::fs::write(repo_path.join("README.md"), b"base\none\n")
            .expect("right write should succeed");
        run_git(&repo_path, &["add", "README.md"]);
        run_git(&repo_path, &["commit", "--quiet", "-m", "different title"]);
        let right_tip = git_stdout(&repo_path, &["rev-parse", "HEAD"]);

        let range_pair = RangePair {
            range1: format!("{base}..{left_tip}"),
            range2: format!("{base}..{right_tip}"),
        };

        let mut raw_stream = service
            .raw_range_diff(Request::new(RawRangeDiffRequest {
                repository: Some(repository.clone()),
                range_spec: Some(raw_range_diff_request::RangeSpec::RangePair(
                    range_pair.clone(),
                )),
            }))
            .await
            .expect("raw_range_diff should succeed")
            .into_inner();

        let mut raw_data = Vec::new();
        while let Some(chunk) = raw_stream.next().await {
            let chunk = chunk.expect("raw_range_diff chunk should not error");
            raw_data.extend(chunk.data);
        }
        let raw_text = String::from_utf8(raw_data).expect("raw range diff should be UTF-8");
        assert!(raw_text.contains("!"));
        assert!(raw_text.contains("Commit message"));

        let mut parsed_stream = service
            .range_diff(Request::new(RangeDiffRequest {
                repository: Some(repository.clone()),
                range_spec: Some(range_diff_request::RangeSpec::RangePair(range_pair)),
            }))
            .await
            .expect("range_diff should succeed")
            .into_inner();

        let mut parsed_responses = Vec::new();
        while let Some(response) = parsed_stream.next().await {
            parsed_responses.push(response.expect("range_diff chunk should not error"));
        }

        assert!(!parsed_responses.is_empty());

        let not_equal = range_diff_response::Comparator::NotEqual as i32;
        let mut saw_not_equal_pair = false;
        let mut saw_not_equal_end = false;
        let mut not_equal_patch = Vec::new();
        for response in &parsed_responses {
            if response.comparison != not_equal {
                continue;
            }

            saw_not_equal_pair = true;
            assert!(!response.from_commit_id.is_empty());
            assert!(!response.to_commit_id.is_empty());
            not_equal_patch.extend_from_slice(&response.patch_data);
            if response.end_of_patch {
                saw_not_equal_end = true;
            }
        }

        assert!(saw_not_equal_pair);
        assert!(saw_not_equal_end);
        let patch_text = String::from_utf8(not_equal_patch).expect("patch should be UTF-8");
        assert!(patch_text.contains("Commit message"));

        let missing_range_error = match service
            .raw_range_diff(Request::new(RawRangeDiffRequest {
                repository: Some(repository),
                range_spec: None,
            }))
            .await
        {
            Ok(_) => panic!("raw_range_diff should reject missing range_spec"),
            Err(error) => error,
        };
        assert_eq!(missing_range_error.code(), Code::InvalidArgument);

        if storage_root.exists() {
            std::fs::remove_dir_all(storage_root).expect("storage root should be removable");
        }
    }
}
