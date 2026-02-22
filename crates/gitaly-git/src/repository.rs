//! repository-level Git command helpers.

use std::path::PathBuf;
use std::string::FromUtf8Error;

use thiserror::Error;

use crate::catfile::{CatFileError, CatFileReader, ObjectData};
use crate::command::{CommandRunner, CommandSpec};
use crate::command_factory::{CommandFactoryError, GitCommandFactory};

#[derive(Debug, Clone)]
pub struct Repository {
    path: PathBuf,
    runner: CommandRunner,
    command_factory: GitCommandFactory,
    catfile_reader: CatFileReader,
}

impl Repository {
    pub fn new(path: impl Into<PathBuf>) -> Self {
        let path = path.into();

        Self {
            path: path.clone(),
            runner: CommandRunner,
            command_factory: GitCommandFactory::default(),
            catfile_reader: CatFileReader::new(path),
        }
    }

    pub async fn resolve_revision(&self, rev: &str) -> Result<String, RepositoryError> {
        let spec = self.git_spec(["rev-parse", rev])?;
        let output = self.runner.run(&spec).await?;

        if output.status_code != Some(0) {
            return Err(RepositoryError::CommandFailure {
                status_code: output.status_code,
                stderr: String::from_utf8_lossy(&output.stderr).into_owned(),
            });
        }

        Ok(String::from_utf8(output.stdout)?
            .trim_end_matches(['\n', '\r'])
            .to_string())
    }

    pub async fn reference_exists(&self, name: &str) -> Result<bool, RepositoryError> {
        let spec = self.git_spec(["show-ref", "--verify", "--quiet", name])?;
        let output = self.runner.run(&spec).await?;

        match output.status_code {
            Some(0) => Ok(true),
            Some(1) => Ok(false),
            _ => Err(RepositoryError::CommandFailure {
                status_code: output.status_code,
                stderr: String::from_utf8_lossy(&output.stderr).into_owned(),
            }),
        }
    }

    pub async fn get_reference(&self, name: &str) -> Result<Option<String>, RepositoryError> {
        let spec = self.git_spec(["rev-parse", "--verify", "--quiet", name])?;
        let output = self.runner.run(&spec).await?;

        match output.status_code {
            Some(0) => Ok(Some(parse_revision_oid(output.stdout)?)),
            Some(1) => Ok(None),
            _ => Err(RepositoryError::CommandFailure {
                status_code: output.status_code,
                stderr: String::from_utf8_lossy(&output.stderr).into_owned(),
            }),
        }
    }

    pub async fn write_blob(&self, payload: &[u8]) -> Result<String, RepositoryError> {
        let spec = self.git_spec(["hash-object", "-w", "--stdin"])?;
        let output = self.runner.run_with_input(&spec, payload).await?;

        if output.status_code != Some(0) {
            return Err(RepositoryError::CommandFailure {
                status_code: output.status_code,
                stderr: String::from_utf8_lossy(&output.stderr).into_owned(),
            });
        }

        Ok(String::from_utf8(output.stdout)?
            .trim_end_matches(['\n', '\r'])
            .to_string())
    }

    pub async fn read_object(
        &self,
        object_spec: &str,
    ) -> Result<Option<ObjectData>, RepositoryError> {
        self.catfile_reader
            .object_contents(object_spec)
            .await
            .map_err(RepositoryError::CatFile)
    }

    fn git_spec<const N: usize>(&self, args: [&str; N]) -> Result<CommandSpec, RepositoryError> {
        self.command_factory
            .build_for_repo(&self.path, args)
            .map_err(RepositoryError::CommandFactory)
    }
}

fn parse_revision_oid(stdout: Vec<u8>) -> Result<String, RepositoryError> {
    let stdout = String::from_utf8(stdout)?;
    let oid = stdout.trim_end_matches(['\n', '\r']).trim();
    if oid.is_empty() {
        return Err(RepositoryError::InvalidOutput {
            reason: "rev-parse output is empty".to_string(),
        });
    }

    Ok(oid.to_string())
}

#[derive(Debug, Error)]
pub enum RepositoryError {
    #[error("cat-file operation failed: {0}")]
    CatFile(#[from] CatFileError),
    #[error("failed to build git command: {0}")]
    CommandFactory(#[from] CommandFactoryError),
    #[error("command execution failed: {0}")]
    Command(#[from] crate::command::CommandError),
    #[error("git command failed with status {status_code:?}: {stderr}")]
    CommandFailure {
        status_code: Option<i32>,
        stderr: String,
    },
    #[error("failed to parse UTF-8 output: {0}")]
    Utf8(#[from] FromUtf8Error),
    #[error("invalid git output: {reason}")]
    InvalidOutput { reason: String },
}

#[cfg(test)]
mod tests {
    use super::Repository;
    use std::fs;
    use std::path::{Path, PathBuf};
    use std::process::{Command, Output};
    use std::sync::atomic::{AtomicU64, Ordering};

    const EXISTING_BRANCH: &str = "feature/tdd-branch";
    const EXISTING_REF: &str = "refs/heads/feature/tdd-branch";
    const MISSING_REF: &str = "refs/heads/feature/does-not-exist";

    #[tokio::test]
    async fn resolve_revision_head() {
        let repo_dir = TempGitRepo::new();
        let repository = Repository::new(repo_dir.path().to_path_buf());

        let expected_head = run_git_stdout(repo_dir.path(), &["rev-parse", "HEAD"]);

        let actual_head = repository
            .resolve_revision("HEAD")
            .await
            .expect("HEAD should resolve");

        assert_eq!(actual_head, expected_head);
    }

    #[tokio::test]
    async fn reference_exists_true_for_created_branch() {
        let repo_dir = TempGitRepo::new();
        let repository = Repository::new(repo_dir.path().to_path_buf());

        let exists = repository
            .reference_exists(EXISTING_REF)
            .await
            .expect("existing branch ref should check cleanly");

        assert!(exists);
    }

    #[tokio::test]
    async fn reference_exists_false_for_missing_reference() {
        let repo_dir = TempGitRepo::new();
        let repository = Repository::new(repo_dir.path().to_path_buf());

        let exists = repository
            .reference_exists(MISSING_REF)
            .await
            .expect("missing refs should not hard-error");

        assert!(!exists);
    }

    #[tokio::test]
    async fn read_object_returns_blob_payload() {
        let repo_dir = TempGitRepo::new();
        let repository = Repository::new(repo_dir.path().to_path_buf());

        let object = repository
            .read_object("HEAD:README.md")
            .await
            .expect("reading object should not fail")
            .expect("object should exist");

        assert_eq!(object.info.object_type, "blob");
        assert_eq!(object.content, b"test repository\n");
    }

    struct TempGitRepo {
        path: PathBuf,
    }

    impl TempGitRepo {
        fn new() -> Self {
            let path = unique_repo_path();
            fs::create_dir_all(&path).expect("temp repo directory should be creatable");

            run_git(&path, &["init", "--quiet"]);
            run_git(&path, &["config", "user.name", "Repository Tests"]);
            run_git(
                &path,
                &["config", "user.email", "repository-tests@example.com"],
            );

            fs::write(path.join("README.md"), b"test repository\n")
                .expect("README should be writable");

            run_git(&path, &["add", "README.md"]);
            run_git(&path, &["commit", "--quiet", "-m", "initial commit"]);
            run_git(&path, &["branch", EXISTING_BRANCH]);

            Self { path }
        }

        fn path(&self) -> &Path {
            &self.path
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
            "gitaly-git-repository-tests-{}-{id}",
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

    fn run_git_output(repo_path: &Path, args: &[&str]) -> Output {
        Command::new("git")
            .arg("-C")
            .arg(repo_path)
            .args(args)
            .output()
            .expect("git command should execute")
    }
}
