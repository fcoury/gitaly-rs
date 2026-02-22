//! git object lookup helpers backed by `git cat-file` batch modes.

use std::collections::HashMap;
use std::num::ParseIntError;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use thiserror::Error;

use crate::command::{CommandError, CommandRunner, CommandSpec};
use crate::command_factory::{CommandFactoryError, CommandWhitelistPolicy, GitCommandFactory};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObjectInfo {
    pub oid: String,
    pub object_type: String,
    pub size: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObjectData {
    pub info: ObjectInfo,
    pub content: Vec<u8>,
}

#[derive(Debug, Clone)]
pub struct CatFileReader {
    repo_path: PathBuf,
    runner: CommandRunner,
    command_factory: GitCommandFactory,
}

impl CatFileReader {
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self {
            repo_path: path.into(),
            runner: CommandRunner,
            command_factory: GitCommandFactory::new(CommandWhitelistPolicy::new(["cat-file"])),
        }
    }

    pub async fn object_info(&self, spec: &str) -> Result<Option<ObjectInfo>, CatFileError> {
        let command = self.git_spec(["cat-file", "--batch-check"])?;
        let stdin = format!("{spec}\n");
        let output = self
            .runner
            .run_with_input(&command, stdin.as_bytes())
            .await?;

        if output.status_code != Some(0) {
            return Err(CatFileError::CommandFailure {
                status_code: output.status_code,
                stderr: String::from_utf8_lossy(&output.stderr).trim().to_string(),
            });
        }

        let stdout = String::from_utf8(output.stdout)?;
        let first_line = stdout
            .lines()
            .next()
            .ok_or_else(|| CatFileError::InvalidHeader {
                header: "<empty>".to_string(),
            })?;

        parse_batch_header(first_line)
    }

    pub async fn object_contents(&self, spec: &str) -> Result<Option<ObjectData>, CatFileError> {
        let command = self.git_spec(["cat-file", "--batch"])?;
        let stdin = format!("{spec}\n");
        let output = self
            .runner
            .run_with_input(&command, stdin.as_bytes())
            .await?;

        if output.status_code != Some(0) {
            return Err(CatFileError::CommandFailure {
                status_code: output.status_code,
                stderr: String::from_utf8_lossy(&output.stderr).trim().to_string(),
            });
        }

        parse_batch_payload(&output.stdout)
    }

    fn git_spec<const N: usize>(&self, args: [&str; N]) -> Result<CommandSpec, CatFileError> {
        self.command_factory
            .build_for_repo(&self.repo_path, args)
            .map_err(CatFileError::CommandFactory)
    }
}

#[derive(Debug)]
pub struct CatFileReaderCache {
    readers: Mutex<HashMap<PathBuf, CachedReader>>,
    max_entries: usize,
    ttl: Duration,
}

#[derive(Debug)]
struct CachedReader {
    reader: Arc<CatFileReader>,
    last_accessed: Instant,
}

impl CatFileReaderCache {
    pub const DEFAULT_MAX_ENTRIES: usize = 128;
    pub const DEFAULT_TTL: Duration = Duration::from_secs(300);

    pub fn new(max_entries: usize, ttl: Duration) -> Self {
        Self {
            readers: Mutex::new(HashMap::new()),
            max_entries,
            ttl,
        }
    }

    pub fn get_or_create(&self, repo_path: impl Into<PathBuf>) -> Arc<CatFileReader> {
        let repo_path = repo_path.into();
        let now = Instant::now();
        let mut readers = self
            .readers
            .lock()
            .expect("reader cache lock should not be poisoned");

        readers.retain(|_, entry| now.duration_since(entry.last_accessed) < self.ttl);

        if let Some(entry) = readers.get_mut(&repo_path) {
            entry.last_accessed = now;
            return entry.reader.clone();
        }

        let reader = Arc::new(CatFileReader::new(repo_path.clone()));
        if self.max_entries == 0 {
            return reader;
        }

        if readers.len() >= self.max_entries {
            let lru_key = readers
                .iter()
                .min_by_key(|(_, entry)| entry.last_accessed)
                .map(|(key, _)| key.clone());

            if let Some(lru_key) = lru_key {
                readers.remove(&lru_key);
            }
        }

        readers.insert(
            repo_path,
            CachedReader {
                reader: reader.clone(),
                last_accessed: now,
            },
        );

        reader
    }
}

impl Default for CatFileReaderCache {
    fn default() -> Self {
        Self::new(Self::DEFAULT_MAX_ENTRIES, Self::DEFAULT_TTL)
    }
}

#[derive(Debug, Error)]
pub enum CatFileError {
    #[error("failed to build git command: {0}")]
    CommandFactory(#[from] CommandFactoryError),
    #[error("command execution failed: {0}")]
    Command(#[from] CommandError),
    #[error("git command failed with status {status_code:?}: {stderr}")]
    CommandFailure {
        status_code: Option<i32>,
        stderr: String,
    },
    #[error("invalid cat-file header `{header}`")]
    InvalidHeader { header: String },
    #[error("invalid object size in cat-file header `{header}`: {source}")]
    InvalidSize {
        header: String,
        #[source]
        source: ParseIntError,
    },
    #[error("failed to decode UTF-8 from git output: {0}")]
    Utf8(#[from] std::string::FromUtf8Error),
    #[error("invalid cat-file payload: {reason}")]
    InvalidPayload { reason: String },
}

fn parse_batch_header(line: &str) -> Result<Option<ObjectInfo>, CatFileError> {
    let line = trim_line(line);

    if line.is_empty() {
        return Err(CatFileError::InvalidHeader {
            header: "<empty>".to_string(),
        });
    }

    let mut fields = line.split_whitespace();
    let oid = fields.next().ok_or_else(|| CatFileError::InvalidHeader {
        header: line.to_string(),
    })?;
    let object_type = fields.next().ok_or_else(|| CatFileError::InvalidHeader {
        header: line.to_string(),
    })?;
    if object_type == "missing" {
        if fields.next().is_some() {
            return Err(CatFileError::InvalidHeader {
                header: line.to_string(),
            });
        }

        return Ok(None);
    }

    let size_text = fields.next().ok_or_else(|| CatFileError::InvalidHeader {
        header: line.to_string(),
    })?;

    if fields.next().is_some() {
        return Err(CatFileError::InvalidHeader {
            header: line.to_string(),
        });
    }

    let size = size_text
        .parse()
        .map_err(|source| CatFileError::InvalidSize {
            header: line.to_string(),
            source,
        })?;

    Ok(Some(ObjectInfo {
        oid: oid.to_string(),
        object_type: object_type.to_string(),
        size,
    }))
}

fn parse_batch_payload(stdout: &[u8]) -> Result<Option<ObjectData>, CatFileError> {
    let header_end = stdout
        .iter()
        .position(|byte| *byte == b'\n')
        .ok_or_else(|| CatFileError::InvalidPayload {
            reason: "missing header newline".to_string(),
        })?;
    let header = String::from_utf8(stdout[..header_end].to_vec())?;
    let body_start = header_end + 1;
    let Some(info) = parse_batch_header(&header)? else {
        if body_start != stdout.len() {
            return Err(CatFileError::InvalidPayload {
                reason: "missing object response must not include body bytes".to_string(),
            });
        }

        return Ok(None);
    };

    let body_end =
        body_start
            .checked_add(info.size)
            .ok_or_else(|| CatFileError::InvalidPayload {
                reason: "object size overflows payload indexing".to_string(),
            })?;
    let trailer_index = body_end;

    if stdout.len() < body_end + 1 {
        return Err(CatFileError::InvalidPayload {
            reason: format!(
                "payload shorter than expected object size {}, got {} bytes after header",
                info.size,
                stdout.len().saturating_sub(body_start)
            ),
        });
    }

    if stdout[trailer_index] != b'\n' {
        return Err(CatFileError::InvalidPayload {
            reason: "payload is missing trailing newline".to_string(),
        });
    }

    if stdout.len() != body_end + 1 {
        return Err(CatFileError::InvalidPayload {
            reason: "payload contains trailing bytes after a single object response".to_string(),
        });
    }

    Ok(Some(ObjectData {
        info,
        content: stdout[body_start..body_end].to_vec(),
    }))
}

fn trim_line(line: &str) -> &str {
    line.trim_end_matches(['\n', '\r'])
}

#[cfg(test)]
mod tests {
    use super::{
        parse_batch_header, parse_batch_payload, CatFileReader, CatFileReaderCache, ObjectData,
    };
    use std::fs;
    use std::path::{Path, PathBuf};
    use std::process::{Command, Output};
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::Arc;
    use std::thread;
    use std::time::Duration;

    #[test]
    fn parse_batch_header_parses_valid_object_line() {
        let header = parse_batch_header("1111111111111111111111111111111111111111 blob 42")
            .expect("header should parse")
            .expect("header should not be missing");

        assert_eq!(header.oid, "1111111111111111111111111111111111111111");
        assert_eq!(header.object_type, "blob");
        assert_eq!(header.size, 42);
    }

    #[test]
    fn parse_batch_header_returns_none_for_missing_object() {
        let parsed = parse_batch_header("deadbeefdeadbeefdeadbeefdeadbeefdeadbeef missing")
            .expect("missing line should parse");

        assert!(parsed.is_none());
    }

    #[test]
    fn parse_batch_payload_parses_content_and_metadata() {
        let stdout = b"1111111111111111111111111111111111111111 blob 5\nhello\n";
        let parsed = parse_batch_payload(stdout)
            .expect("payload should parse")
            .expect("payload should contain object");

        assert_eq!(
            parsed,
            ObjectData {
                info: super::ObjectInfo {
                    oid: "1111111111111111111111111111111111111111".to_string(),
                    object_type: "blob".to_string(),
                    size: 5,
                },
                content: b"hello".to_vec(),
            }
        );
    }

    #[test]
    fn parse_batch_payload_returns_none_for_missing_object() {
        let stdout = b"deadbeefdeadbeefdeadbeefdeadbeefdeadbeef missing\n";
        let parsed = parse_batch_payload(stdout).expect("missing object payload should parse");

        assert!(parsed.is_none());
    }

    #[test]
    fn reader_cache_reuses_reader_for_same_repo_path() {
        let cache = CatFileReaderCache::default();
        let first = cache.get_or_create(PathBuf::from("/tmp/repo"));
        let second = cache.get_or_create(PathBuf::from("/tmp/repo"));

        assert!(Arc::ptr_eq(&first, &second));
    }

    #[test]
    fn reader_cache_replaces_entry_after_ttl_expiry() {
        let cache = CatFileReaderCache::new(8, Duration::from_millis(20));
        let first = cache.get_or_create(PathBuf::from("/tmp/repo"));
        thread::sleep(Duration::from_millis(50));
        let second = cache.get_or_create(PathBuf::from("/tmp/repo"));

        assert!(!Arc::ptr_eq(&first, &second));
    }

    #[test]
    fn reader_cache_evicts_least_recently_used_entry_when_over_capacity() {
        let cache = CatFileReaderCache::new(2, Duration::from_secs(60));
        let a = cache.get_or_create(PathBuf::from("/tmp/repo-a"));
        thread::sleep(Duration::from_millis(2));
        let b = cache.get_or_create(PathBuf::from("/tmp/repo-b"));
        thread::sleep(Duration::from_millis(2));
        let a_hit = cache.get_or_create(PathBuf::from("/tmp/repo-a"));
        assert!(Arc::ptr_eq(&a, &a_hit));

        thread::sleep(Duration::from_millis(2));
        let _c = cache.get_or_create(PathBuf::from("/tmp/repo-c"));
        let a_after = cache.get_or_create(PathBuf::from("/tmp/repo-a"));
        let b_after = cache.get_or_create(PathBuf::from("/tmp/repo-b"));

        assert!(Arc::ptr_eq(&a, &a_after));
        assert!(!Arc::ptr_eq(&b, &b_after));
    }

    #[tokio::test]
    async fn object_info_reads_blob_metadata_from_real_repo() {
        let repo = TempGitRepo::new();
        let blob_oid = run_git_stdout(repo.path(), &["rev-parse", "HEAD:payload.bin"]);

        let reader = CatFileReader::new(repo.path());
        let info = reader
            .object_info(&blob_oid)
            .await
            .expect("object info should load")
            .expect("blob should exist");

        assert_eq!(info.oid, blob_oid);
        assert_eq!(info.object_type, "blob");
        assert_eq!(info.size, repo.payload().len());
    }

    #[tokio::test]
    async fn object_contents_reads_blob_payload_from_real_repo() {
        let repo = TempGitRepo::new();
        let blob_oid = run_git_stdout(repo.path(), &["rev-parse", "HEAD:payload.bin"]);

        let reader = CatFileReader::new(repo.path());
        let object_data = reader
            .object_contents(&blob_oid)
            .await
            .expect("object content should load")
            .expect("blob should exist");

        assert_eq!(object_data.info.oid, blob_oid);
        assert_eq!(object_data.info.object_type, "blob");
        assert_eq!(object_data.info.size, repo.payload().len());
        assert_eq!(object_data.content, repo.payload());
    }

    #[tokio::test]
    async fn object_lookup_returns_none_for_missing_object() {
        let repo = TempGitRepo::new();
        let missing_oid = "ffffffffffffffffffffffffffffffffffffffff";

        let reader = CatFileReader::new(repo.path());

        let info = reader
            .object_info(missing_oid)
            .await
            .expect("missing metadata lookup should not hard-fail");
        assert!(info.is_none());

        let content = reader
            .object_contents(missing_oid)
            .await
            .expect("missing content lookup should not hard-fail");
        assert!(content.is_none());
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
            run_git(&path, &["config", "user.name", "CatFile Tests"]);
            run_git(
                &path,
                &["config", "user.email", "catfile-tests@example.com"],
            );

            let payload = b"\0\x01\x02cat-file\xffpayload\n".to_vec();
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
            "gitaly-git-catfile-tests-{}-{id}",
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
