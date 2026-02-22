use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use thiserror::Error;

pub type KeyValueState = BTreeMap<String, String>;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case")]
pub enum KeyValueMutation {
    Put { key: String, value: String },
    Delete { key: String },
}

impl KeyValueMutation {
    pub fn apply(&self, state: &mut KeyValueState) {
        match self {
            Self::Put { key, value } => {
                state.insert(key.clone(), value.clone());
            }
            Self::Delete { key } => {
                state.remove(key);
            }
        }
    }
}

pub fn apply_mutations(
    state: &mut KeyValueState,
    mutations: impl IntoIterator<Item = KeyValueMutation>,
) {
    for mutation in mutations {
        mutation.apply(state);
    }
}

const KEY_VALUE_TABLE: redb::TableDefinition<&str, &str> = redb::TableDefinition::new("keyvalue");

#[derive(Debug, Error)]
pub enum RedbStoreError {
    #[error("failed to create parent directory for key-value database '{path}': {source}")]
    CreateParentDirectory {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("redb database error: {0}")]
    Database(#[from] redb::DatabaseError),
    #[error("redb transaction error: {0}")]
    Transaction(#[from] redb::TransactionError),
    #[error("redb table error: {0}")]
    Table(#[from] redb::TableError),
    #[error("redb storage error: {0}")]
    Storage(#[from] redb::StorageError),
    #[error("redb commit error: {0}")]
    Commit(#[from] redb::CommitError),
}

#[derive(Debug)]
pub struct RedbStore {
    db: redb::Database,
    path: PathBuf,
}

impl RedbStore {
    pub fn create(path: impl AsRef<Path>) -> Result<Self, RedbStoreError> {
        let path = path.as_ref().to_path_buf();
        Self::create_parent_dir(&path)?;

        let db = redb::Database::create(&path)?;
        let store = Self { db, path };
        store.ensure_table_exists()?;
        Ok(store)
    }

    pub fn open(path: impl AsRef<Path>) -> Result<Self, RedbStoreError> {
        let path = path.as_ref().to_path_buf();
        let db = redb::Database::open(&path)?;
        let store = Self { db, path };
        store.ensure_table_exists()?;
        Ok(store)
    }

    pub fn open_or_create(path: impl AsRef<Path>) -> Result<Self, RedbStoreError> {
        Self::create(path)
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn put(&self, key: &str, value: &str) -> Result<(), RedbStoreError> {
        let write_txn = self.db.begin_write()?;
        {
            let mut table = write_txn.open_table(KEY_VALUE_TABLE)?;
            table.insert(key, value)?;
        }
        write_txn.commit()?;
        Ok(())
    }

    pub fn delete(&self, key: &str) -> Result<(), RedbStoreError> {
        let write_txn = self.db.begin_write()?;
        {
            let mut table = write_txn.open_table(KEY_VALUE_TABLE)?;
            table.remove(key)?;
        }
        write_txn.commit()?;
        Ok(())
    }

    pub fn get(&self, key: &str) -> Result<Option<String>, RedbStoreError> {
        let read_txn = self.db.begin_read()?;
        let table = match read_txn.open_table(KEY_VALUE_TABLE) {
            Ok(table) => table,
            Err(redb::TableError::TableDoesNotExist(_)) => return Ok(None),
            Err(err) => return Err(err.into()),
        };
        let value = table.get(key)?.map(|stored| stored.value().to_string());
        Ok(value)
    }

    pub fn apply_mutation(&self, mutation: &KeyValueMutation) -> Result<(), RedbStoreError> {
        let write_txn = self.db.begin_write()?;
        {
            let mut table = write_txn.open_table(KEY_VALUE_TABLE)?;
            Self::apply_mutation_to_table(&mut table, mutation)?;
        }
        write_txn.commit()?;
        Ok(())
    }

    pub fn apply_mutations(
        &self,
        mutations: impl IntoIterator<Item = KeyValueMutation>,
    ) -> Result<(), RedbStoreError> {
        let write_txn = self.db.begin_write()?;
        {
            let mut table = write_txn.open_table(KEY_VALUE_TABLE)?;
            for mutation in mutations {
                Self::apply_mutation_to_table(&mut table, &mutation)?;
            }
        }
        write_txn.commit()?;
        Ok(())
    }

    fn apply_mutation_to_table(
        table: &mut redb::Table<'_, &str, &str>,
        mutation: &KeyValueMutation,
    ) -> Result<(), RedbStoreError> {
        match mutation {
            KeyValueMutation::Put { key, value } => {
                table.insert(key.as_str(), value.as_str())?;
            }
            KeyValueMutation::Delete { key } => {
                table.remove(key.as_str())?;
            }
        }

        Ok(())
    }

    fn create_parent_dir(path: &Path) -> Result<(), RedbStoreError> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).map_err(|source| {
                RedbStoreError::CreateParentDirectory {
                    path: parent.to_path_buf(),
                    source,
                }
            })?;
        }

        Ok(())
    }

    fn ensure_table_exists(&self) -> Result<(), RedbStoreError> {
        let write_txn = self.db.begin_write()?;
        {
            let _ = write_txn.open_table(KEY_VALUE_TABLE)?;
        }
        write_txn.commit()?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::path::{Path, PathBuf};
    use std::sync::atomic::{AtomicU64, Ordering};

    use super::{apply_mutations, KeyValueMutation, KeyValueState, RedbStore};

    #[test]
    fn apply_mutations_replays_put_and_delete() {
        let mut state = KeyValueState::new();
        apply_mutations(
            &mut state,
            vec![
                KeyValueMutation::Put {
                    key: "a".to_string(),
                    value: "1".to_string(),
                },
                KeyValueMutation::Put {
                    key: "b".to_string(),
                    value: "2".to_string(),
                },
                KeyValueMutation::Delete {
                    key: "a".to_string(),
                },
            ],
        );

        assert_eq!(state.get("a"), None);
        assert_eq!(state.get("b"), Some(&"2".to_string()));
    }

    #[test]
    fn redb_store_applies_batch_mutations() {
        let tmp_dir = TempDir::new("keyvalue-redb-batch");
        let db_path = tmp_dir.path().join("kv.redb");
        let store = RedbStore::create(&db_path).expect("create redb store");

        store
            .apply_mutations(vec![
                KeyValueMutation::Put {
                    key: "alpha".to_string(),
                    value: "1".to_string(),
                },
                KeyValueMutation::Put {
                    key: "beta".to_string(),
                    value: "2".to_string(),
                },
                KeyValueMutation::Delete {
                    key: "alpha".to_string(),
                },
            ])
            .expect("apply mutations");

        assert_eq!(
            store.get("alpha").expect("read key"),
            None,
            "deleted key should not be present"
        );
        assert_eq!(
            store.get("beta").expect("read key"),
            Some("2".to_string()),
            "last value should be persisted"
        );
    }

    #[test]
    fn redb_store_persists_values_across_reopen() {
        let tmp_dir = TempDir::new("keyvalue-redb-reopen");
        let db_path = tmp_dir.path().join("kv.redb");

        {
            let store = RedbStore::create(&db_path).expect("create redb store");
            store.put("project", "gitaly").expect("put first value");
            store
                .apply_mutation(&KeyValueMutation::Put {
                    key: "language".to_string(),
                    value: "rust".to_string(),
                })
                .expect("apply single mutation");
            store.delete("project").expect("delete key");
        }

        let reopened = RedbStore::open(&db_path).expect("reopen redb store");
        assert_eq!(
            reopened.get("project").expect("read deleted key"),
            None,
            "delete should persist across reopen"
        );
        assert_eq!(
            reopened.get("language").expect("read persisted key"),
            Some("rust".to_string()),
            "put should persist across reopen"
        );
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
