//! async stream combinators for Git object traversal and enrichment.

use std::path::PathBuf;
use std::pin::Pin;

use thiserror::Error;
use tokio_stream::{Stream, StreamExt};

use crate::catfile::{CatFileError, CatFileReader, ObjectData, ObjectInfo};
use crate::command::{CommandError, CommandRunner, CommandSpec};
use crate::command_factory::{CommandFactoryError, CommandWhitelistPolicy, GitCommandFactory};

pub type ObjectIdStream = Pin<Box<dyn Stream<Item = Result<String, PipelineError>> + Send>>;
pub type ObjectInfoStream = Pin<Box<dyn Stream<Item = Result<ObjectInfo, PipelineError>> + Send>>;
pub type ObjectDataStream = Pin<Box<dyn Stream<Item = Result<ObjectData, PipelineError>> + Send>>;

#[derive(Debug, Clone)]
pub struct ObjectPipeline {
    repo_path: PathBuf,
    runner: CommandRunner,
    command_factory: GitCommandFactory,
    catfile_reader: CatFileReader,
}

impl ObjectPipeline {
    pub fn new(path: impl Into<PathBuf>) -> Self {
        let repo_path = path.into();

        Self {
            repo_path: repo_path.clone(),
            runner: CommandRunner,
            command_factory: GitCommandFactory::new(CommandWhitelistPolicy::new(["rev-list"])),
            catfile_reader: CatFileReader::new(repo_path),
        }
    }

    pub async fn rev_list_stream(&self, args: &[&str]) -> Result<ObjectIdStream, PipelineError> {
        let command = self.git_spec(args)?;
        let output = self.runner.run(&command).await?;

        if output.status_code != Some(0) {
            return Err(PipelineError::CommandFailure {
                status_code: output.status_code,
                stderr: String::from_utf8_lossy(&output.stderr).trim().to_string(),
            });
        }

        let stdout = String::from_utf8(output.stdout)?;
        let mut object_ids = Vec::new();

        for line in stdout.lines() {
            if let Some(object_id) = parse_rev_list_object_id(line)? {
                object_ids.push(object_id);
            }
        }

        Ok(Box::pin(tokio_stream::iter(
            object_ids.into_iter().map(Ok::<_, PipelineError>),
        )))
    }

    pub fn resolve_object_info_stream<S>(&self, object_ids: S) -> ObjectInfoStream
    where
        S: Stream<Item = Result<String, PipelineError>> + Send + 'static,
    {
        let reader = self.catfile_reader.clone();

        Box::pin(
            object_ids
                .then(move |object_id_result| {
                    let reader = reader.clone();

                    async move {
                        let object_id = object_id_result?;
                        reader
                            .object_info(&object_id)
                            .await
                            .map_err(PipelineError::CatFile)
                    }
                })
                .filter_map(|lookup_result| match lookup_result {
                    Ok(Some(info)) => Some(Ok(info)),
                    Ok(None) => None,
                    Err(err) => Some(Err(err)),
                }),
        )
    }

    pub fn resolve_object_data_stream<S>(&self, object_ids: S) -> ObjectDataStream
    where
        S: Stream<Item = Result<String, PipelineError>> + Send + 'static,
    {
        let reader = self.catfile_reader.clone();

        Box::pin(
            object_ids
                .then(move |object_id_result| {
                    let reader = reader.clone();

                    async move {
                        let object_id = object_id_result?;
                        reader
                            .object_contents(&object_id)
                            .await
                            .map_err(PipelineError::CatFile)
                    }
                })
                .filter_map(|lookup_result| match lookup_result {
                    Ok(Some(data)) => Some(Ok(data)),
                    Ok(None) => None,
                    Err(err) => Some(Err(err)),
                }),
        )
    }

    pub async fn rev_list_object_info_stream(
        &self,
        args: &[&str],
    ) -> Result<ObjectInfoStream, PipelineError> {
        let object_ids = self.rev_list_stream(args).await?;
        Ok(self.resolve_object_info_stream(object_ids))
    }

    pub async fn rev_list_object_data_stream(
        &self,
        args: &[&str],
    ) -> Result<ObjectDataStream, PipelineError> {
        let object_ids = self.rev_list_stream(args).await?;
        Ok(self.resolve_object_data_stream(object_ids))
    }

    fn git_spec(&self, args: &[&str]) -> Result<CommandSpec, PipelineError> {
        let command_args = std::iter::once("rev-list").chain(args.iter().copied());
        self.command_factory
            .build_for_repo(&self.repo_path, command_args)
            .map_err(PipelineError::CommandFactory)
    }
}

#[derive(Debug, Error)]
pub enum PipelineError {
    #[error("cat-file operation failed: {0}")]
    CatFile(#[from] CatFileError),
    #[error("failed to build git command: {0}")]
    CommandFactory(#[from] CommandFactoryError),
    #[error("command execution failed: {0}")]
    Command(#[from] CommandError),
    #[error("git command failed with status {status_code:?}: {stderr}")]
    CommandFailure {
        status_code: Option<i32>,
        stderr: String,
    },
    #[error("failed to parse UTF-8 output: {0}")]
    Utf8(#[from] std::string::FromUtf8Error),
    #[error("invalid rev-list output line `{line}`")]
    InvalidRevListLine { line: String },
}

fn parse_rev_list_object_id(line: &str) -> Result<Option<String>, PipelineError> {
    let line = trim_line(line).trim();

    if line.is_empty() {
        return Ok(None);
    }

    let object_id =
        line.split_whitespace()
            .next()
            .ok_or_else(|| PipelineError::InvalidRevListLine {
                line: line.to_string(),
            })?;

    Ok(Some(object_id.to_string()))
}

fn trim_line(line: &str) -> &str {
    line.trim_end_matches(['\n', '\r'])
}

#[cfg(test)]
mod tests {
    use super::{parse_rev_list_object_id, ObjectPipeline, PipelineError};
    use std::fs;
    use std::path::{Path, PathBuf};
    use std::process::{Command, Output};
    use std::sync::atomic::{AtomicU64, Ordering};
    use tokio_stream::{self as stream, StreamExt};

    #[test]
    fn parse_rev_list_line_parses_oid_only_line() {
        let line = "0123456789abcdef0123456789abcdef01234567";
        let parsed = parse_rev_list_object_id(line)
            .expect("oid line should parse")
            .expect("line should contain an oid");

        assert_eq!(parsed, line);
    }

    #[test]
    fn parse_rev_list_line_parses_objects_mode_line() {
        let parsed =
            parse_rev_list_object_id("0123456789abcdef0123456789abcdef01234567 path/to/file.txt")
                .expect("objects line should parse")
                .expect("line should contain an oid");

        assert_eq!(parsed, "0123456789abcdef0123456789abcdef01234567");
    }

    #[test]
    fn parse_rev_list_line_skips_empty_input() {
        let parsed = parse_rev_list_object_id(" \n").expect("empty line should parse");
        assert!(parsed.is_none());
    }

    #[tokio::test]
    async fn rev_list_stream_yields_commit_ids_from_real_repo() {
        let repo = TempGitRepo::new();
        let pipeline = ObjectPipeline::new(repo.path());

        let stream = pipeline
            .rev_list_stream(&["HEAD"])
            .await
            .expect("rev-list stream should initialize");
        let actual = stream
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .collect::<Result<Vec<_>, _>>()
            .expect("rev-list stream should not contain errors");
        let expected = run_git_lines(repo.path(), &["rev-list", "HEAD"]);

        assert_eq!(actual, expected);
    }

    #[tokio::test]
    async fn rev_list_object_info_stream_resolves_blob_metadata() {
        let repo = TempGitRepo::new();
        let pipeline = ObjectPipeline::new(repo.path());
        let blob_oid = run_git_stdout(repo.path(), &["rev-parse", "HEAD:payload.bin"]);

        let stream = pipeline
            .rev_list_object_info_stream(&["--objects", "HEAD"])
            .await
            .expect("object info stream should initialize");
        let infos = stream
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .collect::<Result<Vec<_>, _>>()
            .expect("object info stream should not contain errors");
        let blob = infos
            .iter()
            .find(|info| info.oid == blob_oid)
            .expect("blob oid should be present in object info stream");

        assert_eq!(blob.object_type, "blob");
        assert_eq!(blob.size, repo.payload().len());
    }

    #[tokio::test]
    async fn rev_list_object_data_stream_resolves_blob_content() {
        let repo = TempGitRepo::new();
        let pipeline = ObjectPipeline::new(repo.path());
        let blob_oid = run_git_stdout(repo.path(), &["rev-parse", "HEAD:payload.bin"]);

        let stream = pipeline
            .rev_list_object_data_stream(&["--objects", "HEAD"])
            .await
            .expect("object content stream should initialize");
        let objects = stream
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .collect::<Result<Vec<_>, _>>()
            .expect("object content stream should not contain errors");
        let blob = objects
            .iter()
            .find(|object| object.info.oid == blob_oid)
            .expect("blob oid should be present in object content stream");

        assert_eq!(blob.info.object_type, "blob");
        assert_eq!(blob.content, repo.payload());
    }

    #[tokio::test]
    async fn resolve_object_info_stream_skips_missing_objects() {
        let repo = TempGitRepo::new();
        let pipeline = ObjectPipeline::new(repo.path());
        let existing_blob = run_git_stdout(repo.path(), &["rev-parse", "HEAD:payload.bin"]);
        let missing_blob = "ffffffffffffffffffffffffffffffffffffffff".to_string();
        let input = Box::pin(stream::iter(vec![
            Ok(existing_blob.clone()),
            Ok(missing_blob),
        ]));

        let infos = pipeline
            .resolve_object_info_stream(input)
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .collect::<Result<Vec<_>, _>>()
            .expect("object info stream should not contain errors");

        assert_eq!(infos.len(), 1);
        assert_eq!(infos[0].oid, existing_blob);
    }

    #[tokio::test]
    async fn rev_list_stream_reports_git_error_for_invalid_revision() {
        let repo = TempGitRepo::new();
        let pipeline = ObjectPipeline::new(repo.path());

        let err = match pipeline
            .rev_list_stream(&["refs/heads/does-not-exist"])
            .await
        {
            Ok(_) => panic!("invalid revision should fail"),
            Err(err) => err,
        };

        assert!(matches!(err, PipelineError::CommandFailure { .. }));
    }

    struct TempGitRepo {
        path: PathBuf,
        payload: Vec<u8>,
    }

    impl TempGitRepo {
        fn new() -> Self {
            let path = unique_repo_path();
            fs::create_dir_all(&path).expect("temp repo directory should be creatable");

            run_git(&path, &["init", "--quiet"]);
            run_git(&path, &["config", "user.name", "Pipeline Tests"]);
            run_git(
                &path,
                &["config", "user.email", "pipeline-tests@example.com"],
            );

            fs::write(path.join("README.md"), b"initial commit\n")
                .expect("README should be writable");
            run_git(&path, &["add", "README.md"]);
            run_git(&path, &["commit", "--quiet", "-m", "initial commit"]);

            let payload = b"\0pipeline-stream-payload\xff\n".to_vec();
            fs::write(path.join("payload.bin"), &payload).expect("payload should be writable");
            run_git(&path, &["add", "payload.bin"]);
            run_git(&path, &["commit", "--quiet", "-m", "add payload"]);

            Self { path, payload }
        }

        fn path(&self) -> &Path {
            &self.path
        }

        fn payload(&self) -> Vec<u8> {
            self.payload.clone()
        }
    }

    impl Drop for TempGitRepo {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.path);
        }
    }

    fn unique_repo_path() -> PathBuf {
        static NEXT_ID: AtomicU64 = AtomicU64::new(0);
        let id = NEXT_ID.fetch_add(1, Ordering::Relaxed);

        std::env::temp_dir().join(format!(
            "gitaly-git-pipeline-tests-{}-{id}",
            std::process::id()
        ))
    }

    fn run_git(repo_path: &Path, args: &[&str]) {
        let output = run_git_output(repo_path, args);

        assert!(
            output.status.success(),
            "git command failed: git -C {} {}\nstdout: {}\nstderr: {}",
            repo_path.display(),
            args.join(" "),
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
    }

    fn run_git_stdout(repo_path: &Path, args: &[&str]) -> String {
        let output = run_git_output(repo_path, args);

        assert!(
            output.status.success(),
            "git command failed: git -C {} {}\nstdout: {}\nstderr: {}",
            repo_path.display(),
            args.join(" "),
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );

        String::from_utf8(output.stdout)
            .expect("git stdout should be UTF-8")
            .trim_end_matches(['\n', '\r'])
            .to_string()
    }

    fn run_git_lines(repo_path: &Path, args: &[&str]) -> Vec<String> {
        let stdout = run_git_stdout(repo_path, args);
        stdout.lines().map(str::to_string).collect()
    }

    fn run_git_output(repo_path: &Path, args: &[&str]) -> Output {
        Command::new("git")
            .arg("-C")
            .arg(repo_path)
            .args(args)
            .output()
            .expect("git command should execute")
    }
}
