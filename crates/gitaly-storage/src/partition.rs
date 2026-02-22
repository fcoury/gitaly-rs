use std::path::{Path, PathBuf};

use thiserror::Error;

use crate::counter::{Counter, CounterError};
use crate::keyvalue::KeyValueMutation;
use crate::locator::{Locator, LocatorError};
use crate::transaction::{TransactionError, TransactionManager};
use crate::wal::{Wal, WalError, WalRecord};

#[derive(Debug, Error)]
pub enum PartitionError {
    #[error("invalid partition locator: {0}")]
    Locator(#[from] LocatorError),
}

#[derive(Debug, Error)]
pub enum PartitionRuntimeError {
    #[error(transparent)]
    Partition(#[from] PartitionError),
    #[error(transparent)]
    Counter(#[from] CounterError),
    #[error(transparent)]
    Wal(#[from] WalError),
    #[error(transparent)]
    Transaction(#[from] TransactionError),
}

#[derive(Debug)]
pub struct Partition<TWalHandle, TTransactionManagerHandle> {
    locator: Locator,
    wal_path: PathBuf,
    wal_handle: TWalHandle,
    transaction_manager_handle: TTransactionManagerHandle,
}

impl<TWalHandle, TTransactionManagerHandle> Partition<TWalHandle, TTransactionManagerHandle> {
    pub fn new(
        storage_root: impl Into<PathBuf>,
        wal_handle: TWalHandle,
        transaction_manager_handle: TTransactionManagerHandle,
    ) -> Result<Self, PartitionError> {
        let locator = Locator::new(storage_root).map_err(PartitionError::Locator)?;
        let wal_path = locator.storage_root().join("wal");

        Ok(Self {
            locator,
            wal_path,
            wal_handle,
            transaction_manager_handle,
        })
    }

    pub fn storage_root(&self) -> &Path {
        self.locator.storage_root()
    }

    pub fn wal_path(&self) -> &Path {
        &self.wal_path
    }

    pub fn repository_counter_path(&self) -> PathBuf {
        self.storage_root().join("repositories.counter")
    }

    pub fn locate_repository(
        &self,
        repository_path: impl AsRef<Path>,
    ) -> Result<PathBuf, PartitionError> {
        Ok(self.locator.resolve(repository_path)?)
    }

    pub fn wal_handle(&self) -> &TWalHandle {
        &self.wal_handle
    }

    pub fn transaction_manager_handle(&self) -> &TTransactionManagerHandle {
        &self.transaction_manager_handle
    }
}

#[derive(Debug)]
pub struct PartitionRuntime {
    partition: Partition<Wal, TransactionManager>,
    counter: Counter,
    partition_id: String,
}

impl PartitionRuntime {
    pub fn bootstrap(
        storage_root: impl Into<PathBuf>,
        partition_id: impl Into<String>,
        initial_lsn: u64,
    ) -> Result<Self, PartitionRuntimeError> {
        let storage_root = storage_root.into();
        let partition_id = partition_id.into();
        let locator = Locator::new(storage_root).map_err(PartitionError::Locator)?;
        let wal_dir = locator.storage_root().join("wal");
        let wal = Wal::for_partition(&wal_dir, &partition_id)?;
        let replayed_wal = wal.replay()?;
        let tx_manager = TransactionManager::from_replayed_wal(initial_lsn, &replayed_wal);
        let partition = Partition {
            locator,
            wal_path: wal_dir,
            wal_handle: wal,
            transaction_manager_handle: tx_manager,
        };
        let counter = Counter::open(partition.repository_counter_path())?;

        Ok(Self {
            partition,
            counter,
            partition_id,
        })
    }

    pub fn partition_id(&self) -> &str {
        self.partition_id.as_str()
    }

    pub fn storage_root(&self) -> &Path {
        self.partition.storage_root()
    }

    pub fn wal_path(&self) -> &Path {
        self.partition.wal_handle().path()
    }

    pub fn next_repository_id(&self) -> Result<u64, PartitionRuntimeError> {
        self.counter.next().map_err(PartitionRuntimeError::Counter)
    }

    pub fn append_commit(
        &self,
        tx_id: u64,
        mutations: Vec<KeyValueMutation>,
    ) -> Result<WalRecord, PartitionRuntimeError> {
        self.partition
            .transaction_manager_handle()
            .append_commit(self.partition.wal_handle(), tx_id, mutations)
            .map_err(PartitionRuntimeError::Transaction)
    }

    pub fn replay_wal(&self) -> Result<Vec<WalRecord>, PartitionRuntimeError> {
        self.partition
            .wal_handle()
            .replay()
            .map_err(PartitionRuntimeError::Wal)
    }
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::path::{Path, PathBuf};
    use std::sync::atomic::{AtomicU64, Ordering};

    use super::{Partition, PartitionRuntime};
    use crate::keyvalue::KeyValueMutation;
    use crate::wal::WalPayload;

    #[test]
    fn derives_wal_and_counter_paths_from_storage_root() {
        let storage_root = TempDir::new("partition-paths");

        let partition = Partition::new(
            storage_root.path().to_path_buf(),
            "wal-handle",
            "transaction-manager-handle",
        )
        .expect("partition creates");

        let canonical_root = storage_root
            .path()
            .canonicalize()
            .expect("temp path should canonicalize");
        let expected_root = partition.storage_root().to_path_buf();

        assert_eq!(expected_root, canonical_root);
        assert_eq!(partition.wal_path(), expected_root.join("wal"));
        assert_eq!(
            partition.repository_counter_path(),
            expected_root.join("repositories.counter")
        );
        assert_eq!(partition.wal_handle(), &"wal-handle");
        assert_eq!(
            partition.transaction_manager_handle(),
            &"transaction-manager-handle"
        );
    }

    #[test]
    fn locate_repository_uses_safe_locator_resolution() {
        let storage_root = TempDir::new("partition-locate");

        let partition =
            Partition::new(storage_root.path().to_path_buf(), (), ()).expect("partition creates");
        let expected_root = partition.storage_root().to_path_buf();

        let path = partition
            .locate_repository("group/project.git")
            .expect("safe path resolves");
        assert_eq!(path, expected_root.join("group/project.git"));

        partition
            .locate_repository("../escape.git")
            .expect_err("unsafe path should fail");
    }

    #[test]
    fn partition_runtime_bootstrap_wires_paths_and_ids() {
        let storage_root = TempDir::new("partition-runtime-bootstrap");
        let runtime = PartitionRuntime::bootstrap(storage_root.path().to_path_buf(), "p0", 1)
            .expect("runtime should bootstrap");

        assert_eq!(runtime.partition_id(), "p0");
        assert!(runtime.wal_path().ends_with("wal/p0.wal"));
        assert_eq!(runtime.next_repository_id().expect("first id"), 1);
        assert_eq!(runtime.next_repository_id().expect("second id"), 2);
    }

    #[test]
    fn partition_runtime_appends_commit_to_wal() {
        let storage_root = TempDir::new("partition-runtime-commit");
        let runtime = PartitionRuntime::bootstrap(storage_root.path().to_path_buf(), "p1", 10)
            .expect("runtime should bootstrap");

        let record = runtime
            .append_commit(
                55,
                vec![KeyValueMutation::Put {
                    key: "a".to_string(),
                    value: "1".to_string(),
                }],
            )
            .expect("commit append should work");

        assert_eq!(record.lsn, 10);

        let replayed = runtime.replay_wal().expect("replay should succeed");
        assert_eq!(replayed.len(), 1);
        assert!(matches!(
            replayed[0].payload,
            WalPayload::Commit { tx_id: 55, .. }
        ));
    }

    #[test]
    fn partition_runtime_bootstrap_resumes_lsn_from_existing_wal() {
        let storage_root = TempDir::new("partition-runtime-restart");
        let partition_id = "p2";

        {
            let runtime =
                PartitionRuntime::bootstrap(storage_root.path().to_path_buf(), partition_id, 100)
                    .expect("first runtime should bootstrap");

            let first = runtime
                .append_commit(1, vec![])
                .expect("first commit should append");
            let second = runtime
                .append_commit(2, vec![])
                .expect("second commit should append");

            assert_eq!(first.lsn, 100);
            assert_eq!(second.lsn, 101);
        }

        let restarted =
            PartitionRuntime::bootstrap(storage_root.path().to_path_buf(), partition_id, 1)
                .expect("restarted runtime should bootstrap");
        let third = restarted
            .append_commit(3, vec![])
            .expect("third commit should append");

        assert_eq!(third.lsn, 102);

        let replayed = restarted.replay_wal().expect("replay should succeed");
        assert_eq!(replayed.len(), 3);
        assert_eq!(replayed[0].lsn, 100);
        assert_eq!(replayed[1].lsn, 101);
        assert_eq!(replayed[2].lsn, 102);
    }

    struct TempDir {
        path: PathBuf,
    }

    impl TempDir {
        fn new(prefix: &str) -> Self {
            static NEXT_ID: AtomicU64 = AtomicU64::new(0);
            let id = NEXT_ID.fetch_add(1, Ordering::Relaxed);
            let path = std::env::temp_dir().join(format!(
                "gitaly-storage-{prefix}-{}-{id}",
                std::process::id()
            ));
            fs::create_dir_all(&path).expect("temp directory should be creatable");
            Self { path }
        }

        fn path(&self) -> &Path {
            &self.path
        }
    }

    impl Drop for TempDir {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.path);
        }
    }
}
