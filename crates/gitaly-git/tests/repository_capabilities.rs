use gitaly_git::repository::Repository;
use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::{Command, Output, Stdio};
use std::sync::atomic::{AtomicU64, Ordering};

const EXISTING_BRANCH: &str = "feature/repository-capabilities";
const EXISTING_REF: &str = "refs/heads/feature/repository-capabilities";
const MISSING_REF: &str = "refs/heads/feature/does-not-exist";

mod repository {
    use super::*;

    #[tokio::test]
    async fn write_blob_persists_bytes_and_returns_oid() {
        let repo_dir = TempGitRepo::new();
        let repository = Repository::new(repo_dir.path().to_path_buf());
        let payload = b"integration blob payload\n";

        let oid = repository
            .write_blob(payload)
            .await
            .expect("write_blob should succeed");
        let expected_oid =
            run_git_with_input_stdout(repo_dir.path(), &["hash-object", "--stdin"], payload);
        let object_bytes = run_git_output(repo_dir.path(), &["cat-file", "-p", &oid]).stdout;

        assert_eq!(oid, expected_oid);
        assert_eq!(object_bytes, payload);
    }

    #[tokio::test]
    async fn get_reference_returns_target_oid_when_present() {
        let repo_dir = TempGitRepo::new();
        let repository = Repository::new(repo_dir.path().to_path_buf());
        let expected_oid = run_git_stdout(repo_dir.path(), &["rev-parse", EXISTING_REF]);

        let reference = repository
            .get_reference(EXISTING_REF)
            .await
            .expect("existing references should resolve");

        assert_eq!(reference, Some(expected_oid));
    }

    #[tokio::test]
    async fn get_reference_returns_none_when_missing() {
        let repo_dir = TempGitRepo::new();
        let repository = Repository::new(repo_dir.path().to_path_buf());

        let reference = repository
            .get_reference(MISSING_REF)
            .await
            .expect("missing references should not error");

        assert_eq!(reference, None);
    }
}

struct TempGitRepo {
    path: PathBuf,
}

impl TempGitRepo {
    fn new() -> Self {
        let path = unique_repo_path();
        fs::create_dir_all(&path).expect("temp repo directory should be creatable");

        run_git(&path, &["init", "--quiet"]);
        run_git(
            &path,
            &["config", "user.name", "Repository Integration Tests"],
        );
        run_git(
            &path,
            &[
                "config",
                "user.email",
                "repository-integration-tests@example.com",
            ],
        );

        fs::write(path.join("README.md"), b"integration repository\n")
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
        "gitaly-git-repository-integration-tests-{}-{id}",
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

fn run_git_with_input_stdout(repo_path: &Path, args: &[&str], input: &[u8]) -> String {
    let mut child = Command::new("git")
        .arg("-C")
        .arg(repo_path)
        .args(args)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("git command should execute");
    {
        let stdin = child
            .stdin
            .as_mut()
            .expect("stdin should be piped for git command");
        stdin
            .write_all(input)
            .expect("git command stdin should accept payload");
    }
    let output = child
        .wait_with_output()
        .expect("git command should return output");

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
