use std::sync::atomic::{AtomicU64, Ordering};

use thiserror::Error;

use crate::keyvalue::KeyValueMutation;
use crate::wal::{Wal, WalError, WalRecord};

#[derive(Debug, Error)]
pub enum TransactionError {
    #[error("WAL operation failed: {0}")]
    Wal(#[from] WalError),
}

#[derive(Debug)]
pub struct TransactionManager {
    next_lsn: AtomicU64,
}

impl TransactionManager {
    pub fn new(initial_lsn: u64) -> Self {
        Self {
            next_lsn: AtomicU64::new(initial_lsn),
        }
    }

    pub fn from_replayed_wal(default_next_lsn: u64, replayed_records: &[WalRecord]) -> Self {
        let next_lsn = replayed_records
            .iter()
            .map(|record| record.lsn)
            .max()
            .map_or(default_next_lsn, |max_lsn| max_lsn + 1);

        Self::new(next_lsn)
    }

    pub fn allocate_lsn(&self) -> u64 {
        self.next_lsn.fetch_add(1, Ordering::SeqCst)
    }

    pub fn append_commit(
        &self,
        wal: &Wal,
        tx_id: u64,
        mutations: Vec<KeyValueMutation>,
    ) -> Result<WalRecord, TransactionError> {
        let lsn = self.allocate_lsn();
        let record = WalRecord::commit(lsn, tx_id, mutations);
        wal.append(&record)?;
        Ok(record)
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::{SystemTime, UNIX_EPOCH};

    use crate::keyvalue::KeyValueMutation;
    use crate::transaction::TransactionManager;
    use crate::wal::{Wal, WalRecord};

    #[test]
    fn allocate_lsn_is_monotonic() {
        let tx_manager = TransactionManager::new(7);
        assert_eq!(tx_manager.allocate_lsn(), 7);
        assert_eq!(tx_manager.allocate_lsn(), 8);
        assert_eq!(tx_manager.allocate_lsn(), 9);
    }

    #[test]
    fn append_commit_allocates_lsn_and_persists_record() {
        let dir = temp_test_dir("tx-append");
        let wal = Wal::for_partition(&dir, "part-tx").expect("create wal");
        let tx_manager = TransactionManager::new(100);

        let first = tx_manager
            .append_commit(
                &wal,
                55,
                vec![KeyValueMutation::Put {
                    key: "x".to_string(),
                    value: "1".to_string(),
                }],
            )
            .expect("append first");
        let second = tx_manager
            .append_commit(
                &wal,
                56,
                vec![KeyValueMutation::Delete {
                    key: "x".to_string(),
                }],
            )
            .expect("append second");

        assert_eq!(
            first,
            WalRecord::commit(
                100,
                55,
                vec![KeyValueMutation::Put {
                    key: "x".to_string(),
                    value: "1".to_string(),
                }]
            )
        );
        assert_eq!(
            second,
            WalRecord::commit(
                101,
                56,
                vec![KeyValueMutation::Delete {
                    key: "x".to_string(),
                }]
            )
        );

        let replayed = wal.replay().expect("replay");
        assert_eq!(replayed, vec![first, second]);

        std::fs::remove_dir_all(dir).expect("cleanup temp dir");
    }

    #[test]
    fn from_replayed_wal_uses_max_lsn_plus_one_or_default() {
        let empty = TransactionManager::from_replayed_wal(9, &[]);
        assert_eq!(empty.allocate_lsn(), 9);

        let replayed = vec![
            WalRecord::commit(
                14,
                1,
                vec![KeyValueMutation::Put {
                    key: "k".to_string(),
                    value: "v".to_string(),
                }],
            ),
            WalRecord::commit(
                11,
                2,
                vec![KeyValueMutation::Delete {
                    key: "k".to_string(),
                }],
            ),
            WalRecord::commit(27, 3, vec![]),
        ];
        let resumed = TransactionManager::from_replayed_wal(9, &replayed);
        assert_eq!(resumed.allocate_lsn(), 28);
        assert_eq!(resumed.allocate_lsn(), 29);
    }

    fn temp_test_dir(prefix: &str) -> PathBuf {
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let unique = COUNTER.fetch_add(1, Ordering::Relaxed);
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock should be after unix epoch")
            .as_nanos();
        let dir = std::env::temp_dir().join(format!("{prefix}-{timestamp}-{unique}"));
        std::fs::create_dir_all(&dir).expect("create temp dir");
        dir
    }
}
