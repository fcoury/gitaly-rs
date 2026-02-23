use std::collections::HashMap;
use std::path::PathBuf;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StorageStatus {
    pub storage_name: String,
    pub readable: bool,
    pub writeable: bool,
    pub fs_type: String,
    pub filesystem_id: String,
    pub replication_factor: u32,
    pub available: i64,
    pub used: i64,
}

impl Default for StorageStatus {
    fn default() -> Self {
        Self {
            storage_name: "default".to_string(),
            readable: true,
            writeable: true,
            fs_type: "unknown".to_string(),
            filesystem_id: String::new(),
            replication_factor: 1,
            available: 0,
            used: 0,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Dependencies {
    pub server_version: String,
    pub git_version: String,
    pub storage_statuses: Vec<StorageStatus>,
    pub storage_paths: HashMap<String, PathBuf>,
    pub server_signature_public_key: Vec<u8>,
    pub ready: bool,
}

impl Dependencies {
    #[must_use]
    pub fn new(server_version: impl Into<String>, git_version: impl Into<String>) -> Self {
        Self::default()
            .with_server_version(server_version)
            .with_git_version(git_version)
    }

    #[must_use]
    pub fn with_server_version(mut self, server_version: impl Into<String>) -> Self {
        self.server_version = server_version.into();
        self
    }

    #[must_use]
    pub fn with_git_version(mut self, git_version: impl Into<String>) -> Self {
        self.git_version = git_version.into();
        self
    }

    #[must_use]
    pub fn with_storage_statuses(mut self, storage_statuses: Vec<StorageStatus>) -> Self {
        self.storage_statuses = storage_statuses;
        self
    }

    #[must_use]
    pub fn with_storage_paths(mut self, storage_paths: HashMap<String, PathBuf>) -> Self {
        self.storage_paths = storage_paths;
        self
    }

    #[must_use]
    pub fn with_server_signature_public_key(
        mut self,
        server_signature_public_key: Vec<u8>,
    ) -> Self {
        self.server_signature_public_key = server_signature_public_key;
        self
    }

    #[must_use]
    pub fn with_ready(mut self, ready: bool) -> Self {
        self.ready = ready;
        self
    }
}

impl Default for Dependencies {
    fn default() -> Self {
        Self {
            server_version: env!("CARGO_PKG_VERSION").to_string(),
            git_version: "unknown".to_string(),
            storage_statuses: vec![StorageStatus::default()],
            storage_paths: HashMap::new(),
            server_signature_public_key: Vec::new(),
            ready: true,
        }
    }
}
