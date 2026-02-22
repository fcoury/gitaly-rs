//! Configuration loading and validation for Gitaly.

use std::collections::HashSet;

use serde::Deserialize;
use thiserror::Error;

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct Config {
    pub listen_addr: String,
    pub internal_addr: String,
    pub storages: Vec<Storage>,
    #[serde(default)]
    pub runtime: RuntimeConfig,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct Storage {
    pub name: String,
    pub path: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct RuntimeConfig {
    #[serde(default)]
    pub limiter: LimiterConfig,
    #[serde(default)]
    pub cgroups: CgroupConfig,
}

impl Default for RuntimeConfig {
    fn default() -> Self {
        Self {
            limiter: LimiterConfig::default(),
            cgroups: CgroupConfig::default(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct LimiterConfig {
    #[serde(default = "default_limiter_concurrency_limit")]
    pub concurrency_limit: usize,
    #[serde(default)]
    pub queue_limit: usize,
}

impl Default for LimiterConfig {
    fn default() -> Self {
        Self {
            concurrency_limit: default_limiter_concurrency_limit(),
            queue_limit: default_limiter_queue_limit(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct CgroupConfig {
    #[serde(default = "default_cgroup_enabled")]
    pub enabled: bool,
    #[serde(default = "default_cgroup_bucket_count")]
    pub bucket_count: usize,
}

impl Default for CgroupConfig {
    fn default() -> Self {
        Self {
            enabled: default_cgroup_enabled(),
            bucket_count: default_cgroup_bucket_count(),
        }
    }
}

#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("invalid toml: {0}")]
    InvalidToml(#[from] toml::de::Error),
    #[error("`listen_addr` must not be empty")]
    EmptyListenAddr,
    #[error("at least one storage must be configured")]
    EmptyStorages,
    #[error("storage name must not be empty")]
    EmptyStorageName,
    #[error("duplicate storage name `{0}`")]
    DuplicateStorageName(String),
    #[error("storage `{name}` path must be absolute: `{path}`")]
    RelativeStoragePath { name: String, path: String },
    #[error("runtime limiter concurrency limit must be greater than zero")]
    InvalidLimiterConcurrencyLimit,
    #[error("runtime cgroup bucket count must be greater than zero when cgroups are enabled")]
    InvalidCgroupBucketCount,
}

impl Config {
    pub fn from_toml(input: &str) -> Result<Self, ConfigError> {
        let config: Self = toml::from_str(input)?;
        config.validate()?;

        Ok(config)
    }

    pub fn validate(&self) -> Result<(), ConfigError> {
        if self.listen_addr.trim().is_empty() {
            return Err(ConfigError::EmptyListenAddr);
        }

        if self.storages.is_empty() {
            return Err(ConfigError::EmptyStorages);
        }

        let mut names = HashSet::new();

        for storage in &self.storages {
            if storage.name.trim().is_empty() {
                return Err(ConfigError::EmptyStorageName);
            }

            if !names.insert(storage.name.clone()) {
                return Err(ConfigError::DuplicateStorageName(storage.name.clone()));
            }

            if !storage.path.starts_with('/') {
                return Err(ConfigError::RelativeStoragePath {
                    name: storage.name.clone(),
                    path: storage.path.clone(),
                });
            }
        }

        if self.runtime.limiter.concurrency_limit == 0 {
            return Err(ConfigError::InvalidLimiterConcurrencyLimit);
        }

        if self.runtime.cgroups.enabled && self.runtime.cgroups.bucket_count == 0 {
            return Err(ConfigError::InvalidCgroupBucketCount);
        }

        Ok(())
    }
}

const fn default_limiter_concurrency_limit() -> usize {
    1024
}

const fn default_limiter_queue_limit() -> usize {
    0
}

const fn default_cgroup_enabled() -> bool {
    true
}

const fn default_cgroup_bucket_count() -> usize {
    500
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_valid_config() {
        let config = Config::from_toml(
            r#"
                listen_addr = "127.0.0.1:2305"
                internal_addr = "127.0.0.1:9236"

                [[storages]]
                name = "default"
                path = "/var/opt/gitlab/git-data/repositories"
            "#,
        )
        .expect("valid config should parse");

        assert_eq!(config.listen_addr, "127.0.0.1:2305");
        assert_eq!(config.internal_addr, "127.0.0.1:9236");
        assert_eq!(
            config.storages,
            vec![Storage {
                name: "default".to_string(),
                path: "/var/opt/gitlab/git-data/repositories".to_string(),
            }],
        );
        assert_eq!(
            config.runtime,
            RuntimeConfig {
                limiter: LimiterConfig {
                    concurrency_limit: 1024,
                    queue_limit: 0,
                },
                cgroups: CgroupConfig {
                    enabled: true,
                    bucket_count: 500,
                },
            }
        );
    }

    #[test]
    fn rejects_duplicate_storage_names() {
        let err = Config::from_toml(
            r#"
                listen_addr = "127.0.0.1:2305"
                internal_addr = "127.0.0.1:9236"

                [[storages]]
                name = "default"
                path = "/var/opt/gitlab/git-data/repositories"

                [[storages]]
                name = "default"
                path = "/var/opt/gitlab/git-data/repositories-2"
            "#,
        )
        .expect_err("duplicate storage names should fail");

        assert!(matches!(err, ConfigError::DuplicateStorageName(name) if name == "default"));
    }

    #[test]
    fn rejects_empty_storages() {
        let err = Config::from_toml(
            r#"
                listen_addr = "127.0.0.1:2305"
                internal_addr = "127.0.0.1:9236"
                storages = []
            "#,
        )
        .expect_err("empty storages should fail");

        assert!(matches!(err, ConfigError::EmptyStorages));
    }

    #[test]
    fn rejects_relative_storage_path() {
        let err = Config::from_toml(
            r#"
                listen_addr = "127.0.0.1:2305"
                internal_addr = "127.0.0.1:9236"

                [[storages]]
                name = "default"
                path = "relative/path"
            "#,
        )
        .expect_err("relative storage path should fail");

        assert!(matches!(
            err,
            ConfigError::RelativeStoragePath { name, path }
                if name == "default" && path == "relative/path"
        ));
    }

    #[test]
    fn parses_runtime_overrides() {
        let config = Config::from_toml(
            r#"
                listen_addr = "127.0.0.1:2305"
                internal_addr = "127.0.0.1:9236"

                [runtime.limiter]
                concurrency_limit = 64
                queue_limit = 8

                [runtime.cgroups]
                enabled = false
                bucket_count = 32

                [[storages]]
                name = "default"
                path = "/var/opt/gitlab/git-data/repositories"
            "#,
        )
        .expect("runtime overrides should parse");

        assert_eq!(config.runtime.limiter.concurrency_limit, 64);
        assert_eq!(config.runtime.limiter.queue_limit, 8);
        assert!(!config.runtime.cgroups.enabled);
        assert_eq!(config.runtime.cgroups.bucket_count, 32);
    }

    #[test]
    fn rejects_zero_runtime_limiter_concurrency() {
        let err = Config::from_toml(
            r#"
                listen_addr = "127.0.0.1:2305"
                internal_addr = "127.0.0.1:9236"

                [runtime.limiter]
                concurrency_limit = 0

                [[storages]]
                name = "default"
                path = "/var/opt/gitlab/git-data/repositories"
            "#,
        )
        .expect_err("zero limiter concurrency should fail");

        assert!(matches!(err, ConfigError::InvalidLimiterConcurrencyLimit));
    }

    #[test]
    fn rejects_zero_runtime_cgroup_bucket_count_when_enabled() {
        let err = Config::from_toml(
            r#"
                listen_addr = "127.0.0.1:2305"
                internal_addr = "127.0.0.1:9236"

                [runtime.cgroups]
                enabled = true
                bucket_count = 0

                [[storages]]
                name = "default"
                path = "/var/opt/gitlab/git-data/repositories"
            "#,
        )
        .expect_err("zero cgroup bucket count should fail");

        assert!(matches!(err, ConfigError::InvalidCgroupBucketCount));
    }
}
