use std::fs::OpenOptions;
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::keyvalue::KeyValueMutation;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum WalPayload {
    Commit {
        tx_id: u64,
        mutations: Vec<KeyValueMutation>,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WalRecord {
    pub lsn: u64,
    pub payload: WalPayload,
}

#[derive(Debug, Error)]
pub enum WalError {
    #[error("I/O error for WAL file '{path}': {source}")]
    Io {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("failed to serialize WAL record: {0}")]
    Serialize(#[from] serde_json::Error),
    #[error("failed to deserialize WAL record at line {line}: {source}")]
    DeserializeLine {
        line: usize,
        #[source]
        source: serde_json::Error,
    },
}

#[derive(Clone, Debug)]
pub struct Wal {
    path: PathBuf,
}

impl WalRecord {
    pub fn commit(lsn: u64, tx_id: u64, mutations: Vec<KeyValueMutation>) -> Self {
        Self {
            lsn,
            payload: WalPayload::Commit { tx_id, mutations },
        }
    }
}

impl Wal {
    pub fn for_partition(base_dir: impl AsRef<Path>, partition_id: &str) -> Result<Self, WalError> {
        let file_name = format!("{partition_id}.wal");
        let path = base_dir.as_ref().join(file_name);
        Self::open(path)
    }

    pub fn open(path: impl AsRef<Path>) -> Result<Self, WalError> {
        let path = path.as_ref().to_path_buf();
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).map_err(|source| WalError::Io {
                path: parent.to_path_buf(),
                source,
            })?;
        }

        OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)
            .map_err(|source| WalError::Io {
                path: path.clone(),
                source,
            })?;

        Ok(Self { path })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn append(&self, record: &WalRecord) -> Result<(), WalError> {
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)
            .map_err(|source| WalError::Io {
                path: self.path.clone(),
                source,
            })?;
        let mut writer = BufWriter::new(file);
        serde_json::to_writer(&mut writer, record)?;
        writer.write_all(b"\n").map_err(|source| WalError::Io {
            path: self.path.clone(),
            source,
        })?;
        writer.flush().map_err(|source| WalError::Io {
            path: self.path.clone(),
            source,
        })?;
        Ok(())
    }

    pub fn replay(&self) -> Result<Vec<WalRecord>, WalError> {
        let file = match OpenOptions::new().read(true).open(&self.path) {
            Ok(file) => file,
            Err(source) if source.kind() == std::io::ErrorKind::NotFound => return Ok(vec![]),
            Err(source) => {
                return Err(WalError::Io {
                    path: self.path.clone(),
                    source,
                });
            }
        };

        let mut records = Vec::new();
        for (index, line) in BufReader::new(file).lines().enumerate() {
            let line = line.map_err(|source| WalError::Io {
                path: self.path.clone(),
                source,
            })?;
            if line.trim().is_empty() {
                continue;
            }

            let record =
                serde_json::from_str(&line).map_err(|source| WalError::DeserializeLine {
                    line: index + 1,
                    source,
                })?;
            records.push(record);
        }

        Ok(records)
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::{SystemTime, UNIX_EPOCH};

    use super::{Wal, WalRecord};
    use crate::keyvalue::KeyValueMutation;

    #[test]
    fn replay_returns_records_in_append_order() {
        let dir = temp_test_dir("wal-ordering");
        let wal = Wal::for_partition(&dir, "part-a").expect("create wal");

        wal.append(&WalRecord::commit(
            1,
            10,
            vec![KeyValueMutation::Put {
                key: "alpha".to_string(),
                value: "1".to_string(),
            }],
        ))
        .expect("append first");
        wal.append(&WalRecord::commit(
            2,
            11,
            vec![KeyValueMutation::Delete {
                key: "alpha".to_string(),
            }],
        ))
        .expect("append second");

        let records = wal.replay().expect("replay");

        assert_eq!(
            records,
            vec![
                WalRecord::commit(
                    1,
                    10,
                    vec![KeyValueMutation::Put {
                        key: "alpha".to_string(),
                        value: "1".to_string(),
                    }]
                ),
                WalRecord::commit(
                    2,
                    11,
                    vec![KeyValueMutation::Delete {
                        key: "alpha".to_string(),
                    }]
                ),
            ]
        );

        std::fs::remove_dir_all(dir).expect("cleanup temp dir");
    }

    #[test]
    fn replay_is_empty_when_log_file_has_no_entries() {
        let dir = temp_test_dir("wal-empty-replay");
        let wal = Wal::for_partition(&dir, "part-empty").expect("create wal");

        let records = wal.replay().expect("replay");
        assert!(records.is_empty());

        std::fs::remove_dir_all(dir).expect("cleanup temp dir");
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
