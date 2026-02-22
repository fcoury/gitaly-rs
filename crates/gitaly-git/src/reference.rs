//! git reference name primitives.

use std::{path::PathBuf, str::FromStr};

use thiserror::Error;

use crate::command::{CommandError, CommandRunner, CommandSpec};
use crate::command_factory::{CommandFactoryError, GitCommandFactory};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReferenceName(String);

impl ReferenceName {
    pub fn new(value: impl Into<String>) -> Result<Self, ReferenceNameError> {
        let value = value.into();

        if value.is_empty() {
            return Err(ReferenceNameError::Empty);
        }

        if value.contains(' ') {
            return Err(ReferenceNameError::ContainsSpace);
        }

        if value.contains("..") {
            return Err(ReferenceNameError::ContainsDoubleDot);
        }

        if value.starts_with('/') {
            return Err(ReferenceNameError::LeadingSlash);
        }

        if value.ends_with('/') {
            return Err(ReferenceNameError::TrailingSlash);
        }

        if value.ends_with(".lock") {
            return Err(ReferenceNameError::LockSuffix);
        }

        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        self.0.as_str()
    }
}

impl FromStr for ReferenceName {
    type Err = ReferenceNameError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::new(s)
    }
}

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum ReferenceNameError {
    #[error("reference name cannot be empty")]
    Empty,
    #[error("reference name cannot contain spaces")]
    ContainsSpace,
    #[error("reference name cannot contain `..`")]
    ContainsDoubleDot,
    #[error("reference name cannot start with `/`")]
    LeadingSlash,
    #[error("reference name cannot end with `/`")]
    TrailingSlash,
    #[error("reference name cannot end with `.lock`")]
    LockSuffix,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RefUpdate {
    Create {
        refname: String,
        new_oid: String,
    },
    Update {
        refname: String,
        old_oid: String,
        new_oid: String,
    },
    Delete {
        refname: String,
        old_oid: String,
    },
}

impl RefUpdate {
    fn git_update_ref_stdin_line(&self) -> String {
        match self {
            Self::Create { refname, new_oid } => format!("create {refname} {new_oid}"),
            Self::Update {
                refname,
                old_oid,
                new_oid,
            } => format!("update {refname} {new_oid} {old_oid}"),
            Self::Delete { refname, old_oid } => format!("delete {refname} {old_oid}"),
        }
    }
}

#[derive(Debug, Clone)]
pub struct ReferenceUpdater {
    runner: CommandRunner,
    repo_path: PathBuf,
    command_factory: GitCommandFactory,
}

impl ReferenceUpdater {
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self {
            runner: CommandRunner,
            repo_path: path.into(),
            command_factory: GitCommandFactory::default(),
        }
    }

    pub fn apply(&self, updates: &[RefUpdate]) -> Result<(), ReferenceUpdateError> {
        if updates.is_empty() {
            return Ok(());
        }

        let mut stdin_script = String::from("start\n");
        for update in updates {
            stdin_script.push_str(&update.git_update_ref_stdin_line());
            stdin_script.push('\n');
        }
        stdin_script.push_str("prepare\ncommit\n");

        let spec = self
            .command_factory
            .build_for_repo(&self.repo_path, ["update-ref", "--stdin"])
            .map_err(ReferenceUpdateError::CommandFactory)?;
        let rendered_args = spec.args.join(" ");

        let output = run_command_with_runner(self.runner, spec, stdin_script.into_bytes())?;

        if output.status_code != Some(0) {
            return Err(ReferenceUpdateError::GitFailed {
                args: rendered_args,
                status_code: output.status_code,
                stderr: String::from_utf8_lossy(&output.stderr).trim().to_string(),
            });
        }

        Ok(())
    }
}

#[derive(Debug, Error)]
pub enum ReferenceUpdateError {
    #[error("failed to build git command: {0}")]
    CommandFactory(#[from] CommandFactoryError),
    #[error("failed to run `git {args}`: {source}")]
    Command {
        args: String,
        #[source]
        source: CommandError,
    },
    #[error("git command failed `git {args}` (status {status_code:?}): {stderr}")]
    GitFailed {
        args: String,
        status_code: Option<i32>,
        stderr: String,
    },
    #[error("failed to create tokio runtime for git command: {source}")]
    Runtime {
        #[source]
        source: std::io::Error,
    },
    #[error("git command worker thread panicked")]
    ThreadPanic,
}

fn run_command_with_runner(
    runner: CommandRunner,
    spec: CommandSpec,
    stdin: Vec<u8>,
) -> Result<crate::command::CommandOutput, ReferenceUpdateError> {
    let join_result = std::thread::spawn(move || {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|source| ReferenceUpdateError::Runtime { source })?;

        runtime
            .block_on(runner.run_with_input(&spec, &stdin))
            .map_err(|source| ReferenceUpdateError::Command {
                args: spec.args.join(" "),
                source,
            })
    })
    .join()
    .map_err(|_| ReferenceUpdateError::ThreadPanic)?;

    join_result
}

#[cfg(test)]
mod tests {
    use std::{
        fs,
        path::{Path, PathBuf},
        process::Command,
        time::{SystemTime, UNIX_EPOCH},
    };

    use super::{RefUpdate, ReferenceName, ReferenceNameError, ReferenceUpdater};

    #[test]
    fn accepts_common_head_reference() -> Result<(), ReferenceNameError> {
        let reference = ReferenceName::new("refs/heads/main")?;

        assert_eq!(reference.as_str(), "refs/heads/main");

        Ok(())
    }

    #[test]
    fn accepts_common_tag_reference() -> Result<(), ReferenceNameError> {
        let reference = ReferenceName::new("refs/tags/v1.0.0")?;

        assert_eq!(reference.as_str(), "refs/tags/v1.0.0");

        Ok(())
    }

    #[test]
    fn parses_reference_from_str() -> Result<(), ReferenceNameError> {
        let reference: ReferenceName = "refs/heads/main".parse()?;

        assert_eq!(reference.as_str(), "refs/heads/main");

        Ok(())
    }

    #[test]
    fn rejects_empty_reference() {
        let err = ReferenceName::new("").expect_err("must fail");
        assert!(matches!(err, ReferenceNameError::Empty));
    }

    #[test]
    fn rejects_reference_with_space() {
        let err = ReferenceName::new("refs/heads/with space").expect_err("must fail");
        assert!(matches!(err, ReferenceNameError::ContainsSpace));
    }

    #[test]
    fn rejects_reference_with_double_dot() {
        let err = ReferenceName::new("refs/heads/feature..main").expect_err("must fail");
        assert!(matches!(err, ReferenceNameError::ContainsDoubleDot));
    }

    #[test]
    fn rejects_reference_with_leading_slash() {
        let err = ReferenceName::new("/refs/heads/main").expect_err("must fail");
        assert!(matches!(err, ReferenceNameError::LeadingSlash));
    }

    #[test]
    fn rejects_reference_with_trailing_slash() {
        let err = ReferenceName::new("refs/heads/main/").expect_err("must fail");
        assert!(matches!(err, ReferenceNameError::TrailingSlash));
    }

    #[test]
    fn rejects_reference_with_lock_suffix() {
        let err = ReferenceName::new("refs/heads/main.lock").expect_err("must fail");
        assert!(matches!(err, ReferenceNameError::LockSuffix));
    }

    #[test]
    fn apply_create_creates_reference() {
        let (repo, first_oid, _) = init_repo_with_two_commits();

        let updater = ReferenceUpdater::new(repo.path.clone());
        updater
            .apply(&[RefUpdate::Create {
                refname: "refs/heads/created".to_string(),
                new_oid: first_oid.clone(),
            }])
            .expect("create should succeed");

        assert_eq!(
            git_stdout(&repo.path, &["rev-parse", "refs/heads/created"]),
            first_oid
        );
    }

    #[test]
    fn apply_update_updates_reference() {
        let (repo, first_oid, second_oid) = init_repo_with_two_commits();
        git_success(
            &repo.path,
            &["update-ref", "refs/heads/updated", first_oid.as_str()],
        );

        let updater = ReferenceUpdater::new(repo.path.clone());
        updater
            .apply(&[RefUpdate::Update {
                refname: "refs/heads/updated".to_string(),
                old_oid: first_oid,
                new_oid: second_oid.clone(),
            }])
            .expect("update should succeed");

        assert_eq!(
            git_stdout(&repo.path, &["rev-parse", "refs/heads/updated"]),
            second_oid
        );
    }

    #[test]
    fn apply_delete_deletes_reference() {
        let (repo, first_oid, _) = init_repo_with_two_commits();
        git_success(
            &repo.path,
            &["update-ref", "refs/heads/deleted", first_oid.as_str()],
        );

        let updater = ReferenceUpdater::new(repo.path.clone());
        updater
            .apply(&[RefUpdate::Delete {
                refname: "refs/heads/deleted".to_string(),
                old_oid: first_oid,
            }])
            .expect("delete should succeed");

        let output = git_output(&repo.path, &["rev-parse", "--verify", "refs/heads/deleted"]);
        assert!(
            !output.status.success(),
            "expected ref to be deleted, stdout: {}, stderr: {}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
    }

    #[test]
    fn apply_is_transactional_when_an_update_fails() {
        let (repo, first_oid, second_oid) = init_repo_with_two_commits();
        git_success(
            &repo.path,
            &["update-ref", "refs/heads/existing", first_oid.as_str()],
        );

        let updater = ReferenceUpdater::new(repo.path.clone());
        let err = updater
            .apply(&[
                RefUpdate::Create {
                    refname: "refs/heads/created".to_string(),
                    new_oid: first_oid.clone(),
                },
                RefUpdate::Update {
                    refname: "refs/heads/existing".to_string(),
                    old_oid: second_oid,
                    new_oid: first_oid.clone(),
                },
            ])
            .expect_err("transaction should fail");

        assert!(
            matches!(err, super::ReferenceUpdateError::GitFailed { .. }),
            "expected git failure, got: {err:?}"
        );

        let created_ref = git_output(&repo.path, &["rev-parse", "--verify", "refs/heads/created"]);
        assert!(
            !created_ref.status.success(),
            "expected created ref to be rolled back, stdout: {}, stderr: {}",
            String::from_utf8_lossy(&created_ref.stdout),
            String::from_utf8_lossy(&created_ref.stderr)
        );

        assert_eq!(
            git_stdout(&repo.path, &["rev-parse", "refs/heads/existing"]),
            first_oid
        );
    }

    struct TestRepo {
        path: PathBuf,
    }

    impl Drop for TestRepo {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.path);
        }
    }

    fn init_repo_with_two_commits() -> (TestRepo, String, String) {
        let repo_path = unique_temp_repo_path();
        fs::create_dir_all(&repo_path).expect("temp repo dir should be created");

        git_success(&repo_path, &["init"]);
        git_success(&repo_path, &["config", "user.email", "tests@example.com"]);
        git_success(&repo_path, &["config", "user.name", "Reference Tests"]);

        fs::write(repo_path.join("README.md"), "first\n").expect("first write should work");
        git_success(&repo_path, &["add", "README.md"]);
        git_success(&repo_path, &["commit", "-m", "first"]);
        let first_oid = git_stdout(&repo_path, &["rev-parse", "HEAD"]);

        fs::write(repo_path.join("README.md"), "second\n").expect("second write should work");
        git_success(&repo_path, &["add", "README.md"]);
        git_success(&repo_path, &["commit", "-m", "second"]);
        let second_oid = git_stdout(&repo_path, &["rev-parse", "HEAD"]);

        (TestRepo { path: repo_path }, first_oid, second_oid)
    }

    fn unique_temp_repo_path() -> PathBuf {
        let mut path = std::env::temp_dir();
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock before unix epoch")
            .as_nanos();
        path.push(format!("gitaly-reference-tests-{nanos}"));
        path
    }

    fn git_stdout(repo_path: &Path, args: &[&str]) -> String {
        let output = git_output(repo_path, args);
        assert!(
            output.status.success(),
            "git command failed: git -C {} {}\nstdout: {}\nstderr: {}",
            repo_path.display(),
            args.join(" "),
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );

        String::from_utf8_lossy(&output.stdout).trim().to_string()
    }

    fn git_success(repo_path: &Path, args: &[&str]) {
        let output = git_output(repo_path, args);
        assert!(
            output.status.success(),
            "git command failed: git -C {} {}\nstdout: {}\nstderr: {}",
            repo_path.display(),
            args.join(" "),
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
    }

    fn git_output(repo_path: &Path, args: &[&str]) -> std::process::Output {
        Command::new("git")
            .arg("-C")
            .arg(repo_path)
            .args(args)
            .output()
            .expect("git command should run")
    }
}
