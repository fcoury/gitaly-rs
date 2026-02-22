use std::{
    fs, io,
    num::NonZeroUsize,
    path::{Path, PathBuf},
    sync::Arc,
};

use thiserror::Error;

const DEFAULT_BUCKET_PREFIX: &str = "gitaly-bucket";
const FNV_OFFSET_BASIS: u64 = 0xcbf29ce484222325;
const FNV_PRIME: u64 = 0x100000001b3;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CgroupConfig {
    base_path: PathBuf,
    bucket_count: NonZeroUsize,
    bucket_prefix: String,
}

impl CgroupConfig {
    pub fn new(
        base_path: impl Into<PathBuf>,
        bucket_count: usize,
    ) -> Result<Self, CgroupManagerError> {
        let bucket_count =
            NonZeroUsize::new(bucket_count).ok_or(CgroupManagerError::InvalidBucketCount)?;

        Ok(Self {
            base_path: base_path.into(),
            bucket_count,
            bucket_prefix: DEFAULT_BUCKET_PREFIX.to_string(),
        })
    }

    pub fn with_bucket_prefix(mut self, bucket_prefix: impl Into<String>) -> Self {
        self.bucket_prefix = bucket_prefix.into();
        self
    }

    pub fn base_path(&self) -> &Path {
        &self.base_path
    }

    pub fn bucket_count(&self) -> usize {
        self.bucket_count.get()
    }

    fn bucket_path(&self, bucket: usize) -> PathBuf {
        self.base_path.join(self.bucket_name(bucket))
    }

    fn bucket_name(&self, bucket: usize) -> String {
        format!("{}-{:04}", self.bucket_prefix, bucket)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommandIdentity {
    pub repo_key: String,
    pub command_id: String,
}

impl CommandIdentity {
    pub fn new(repo_key: impl Into<String>, command_id: impl Into<String>) -> Self {
        Self {
            repo_key: repo_key.into(),
            command_id: command_id.into(),
        }
    }

    pub fn with_pid(repo_key: impl Into<String>, pid: u32) -> Self {
        Self::new(repo_key, format!("pid:{pid}"))
    }
}

pub trait CgroupManager: Send + Sync {
    fn assign_bucket_path(&self, command: &CommandIdentity) -> Result<PathBuf, CgroupManagerError>;
}

pub fn build_platform_cgroup_manager(
    config: CgroupConfig,
) -> Result<Arc<dyn CgroupManager>, CgroupManagerError> {
    #[cfg(target_os = "linux")]
    {
        return Ok(Arc::new(FilesystemCgroupManager::new(config)?));
    }

    #[cfg(not(target_os = "linux"))]
    {
        Ok(Arc::new(NoopCgroupManager::new(config)))
    }
}

#[derive(Debug, Clone)]
pub struct NoopCgroupManager {
    config: CgroupConfig,
}

impl NoopCgroupManager {
    pub fn new(config: CgroupConfig) -> Self {
        Self { config }
    }
}

impl CgroupManager for NoopCgroupManager {
    fn assign_bucket_path(&self, command: &CommandIdentity) -> Result<PathBuf, CgroupManagerError> {
        let bucket = bucket_for_repo_key(&command.repo_key, self.config.bucket_count);
        Ok(self.config.bucket_path(bucket))
    }
}

#[derive(Debug, Clone)]
pub struct FilesystemCgroupManager {
    config: CgroupConfig,
}

impl FilesystemCgroupManager {
    pub fn new(config: CgroupConfig) -> Result<Self, CgroupManagerError> {
        let manager = Self { config };
        manager.create_bucket_directories()?;
        Ok(manager)
    }

    fn create_bucket_directories(&self) -> Result<(), CgroupManagerError> {
        for bucket in 0..self.config.bucket_count() {
            self.create_bucket_directory(bucket)?;
        }

        Ok(())
    }

    fn create_bucket_directory(&self, bucket: usize) -> Result<PathBuf, CgroupManagerError> {
        let bucket_path = self.config.bucket_path(bucket);
        fs::create_dir_all(&bucket_path).map_err(|source| CgroupManagerError::CreateDirectory {
            path: bucket_path.clone(),
            source,
        })?;

        Ok(bucket_path)
    }
}

impl CgroupManager for FilesystemCgroupManager {
    fn assign_bucket_path(&self, command: &CommandIdentity) -> Result<PathBuf, CgroupManagerError> {
        let bucket = bucket_for_repo_key(&command.repo_key, self.config.bucket_count);
        self.create_bucket_directory(bucket)
    }
}

#[derive(Debug, Error)]
pub enum CgroupManagerError {
    #[error("bucket count must be greater than zero")]
    InvalidBucketCount,
    #[error("failed to create cgroup bucket directory `{path}`")]
    CreateDirectory {
        path: PathBuf,
        #[source]
        source: io::Error,
    },
}

pub fn bucket_for_repo_key(repo_key: &str, bucket_count: NonZeroUsize) -> usize {
    (fnv1a_64(repo_key.as_bytes()) % bucket_count.get() as u64) as usize
}

fn fnv1a_64(input: &[u8]) -> u64 {
    let mut hash = FNV_OFFSET_BASIS;
    for byte in input {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(FNV_PRIME);
    }

    hash
}

#[cfg(test)]
mod tests {
    use std::{
        fs,
        num::NonZeroUsize,
        path::PathBuf,
        process,
        time::{SystemTime, UNIX_EPOCH},
    };

    use super::{
        bucket_for_repo_key, build_platform_cgroup_manager, CgroupConfig, CgroupManager,
        CommandIdentity, FilesystemCgroupManager, NoopCgroupManager,
    };

    #[test]
    fn noop_manager_assigns_path_without_touching_filesystem() {
        let temp_dir = unique_test_dir("noop");
        let config = CgroupConfig::new(&temp_dir, 8).expect("valid cgroup config");
        let manager = NoopCgroupManager::new(config);

        let assigned = manager
            .assign_bucket_path(&CommandIdentity::new("repo-1", "cmd-1"))
            .expect("noop assignment should succeed");

        assert!(assigned.starts_with(&temp_dir));
        assert!(
            !assigned.exists(),
            "noop manager must not create directories"
        );

        remove_dir_if_exists(&temp_dir);
    }

    #[test]
    fn bucket_assignment_is_stable_for_known_repo_keys() {
        let bucket_count = NonZeroUsize::new(16).expect("non-zero bucket count");

        let cases = [
            ("repo-alpha", 4),
            ("repo-bravo", 4),
            ("repo-charlie", 10),
            ("repo-delta", 6),
            ("repo-echo", 1),
        ];

        for (repo_key, expected_bucket) in cases {
            assert_eq!(bucket_for_repo_key(repo_key, bucket_count), expected_bucket);
        }
    }

    #[test]
    fn bucket_assignment_has_reasonable_distribution() {
        let bucket_count = NonZeroUsize::new(32).expect("non-zero bucket count");
        let mut counts = vec![0usize; bucket_count.get()];

        for idx in 0..10_000 {
            let repo_key = format!("repo-{idx}");
            let bucket = bucket_for_repo_key(&repo_key, bucket_count);
            counts[bucket] += 1;
        }

        let min = *counts.iter().min().expect("non-empty bucket counts");
        let max = *counts.iter().max().expect("non-empty bucket counts");

        assert!(
            min >= 200,
            "bucket distribution is too imbalanced: min={min}, max={max}, counts={counts:?}"
        );
        assert!(
            max <= 430,
            "bucket distribution is too imbalanced: min={min}, max={max}, counts={counts:?}"
        );
    }

    #[test]
    fn filesystem_manager_creates_bucket_directories_and_returns_assigned_path() {
        let temp_dir = unique_test_dir("fs");
        let config = CgroupConfig::new(&temp_dir, 4).expect("valid cgroup config");
        let manager = FilesystemCgroupManager::new(config).expect("filesystem manager init");

        for bucket in 0..4 {
            let path = temp_dir.join(format!("gitaly-bucket-{bucket:04}"));
            assert!(
                path.is_dir(),
                "expected bucket directory to exist: {path:?}"
            );
        }

        let assigned = manager
            .assign_bucket_path(&CommandIdentity::new("repo-42", "cmd-1"))
            .expect("filesystem assignment should succeed");

        assert!(assigned.starts_with(&temp_dir));
        assert!(assigned.is_dir());

        remove_dir_if_exists(&temp_dir);
    }

    #[test]
    fn platform_manager_selects_platform_appropriate_strategy() {
        let temp_dir = unique_test_dir("platform");
        let config = CgroupConfig::new(&temp_dir, 4).expect("valid cgroup config");
        let manager = build_platform_cgroup_manager(config).expect("platform manager should build");

        let assigned = manager
            .assign_bucket_path(&CommandIdentity::new("repo-42", "cmd-1"))
            .expect("platform manager assignment should succeed");

        #[cfg(target_os = "linux")]
        assert!(
            assigned.is_dir(),
            "linux manager should create bucket directories"
        );

        #[cfg(not(target_os = "linux"))]
        assert!(
            !assigned.exists(),
            "non-linux manager should keep cgroup assignment as no-op"
        );

        remove_dir_if_exists(&temp_dir);
    }

    fn unique_test_dir(suffix: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock drift")
            .as_nanos();
        std::env::temp_dir().join(format!(
            "gitaly-cgroups-tests-{}-{suffix}-{nanos}",
            process::id()
        ))
    }

    fn remove_dir_if_exists(path: &PathBuf) {
        if path.exists() {
            fs::remove_dir_all(path).expect("test temp directory should be removable");
        }
    }
}
