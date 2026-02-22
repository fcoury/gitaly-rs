use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::time::{SystemTime, UNIX_EPOCH};

use thiserror::Error;

#[derive(Debug, Error)]
pub enum CounterError {
    #[error("failed to create counter parent directory `{path}`")]
    CreateParentDir {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("failed to read counter file `{path}`")]
    ReadCounter {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("counter value in `{path}` is not a valid u64: `{value}`")]
    InvalidCounterValue { path: PathBuf, value: String },
    #[error("failed to write counter file `{path}`")]
    WriteCounter {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("counter lock poisoned")]
    PoisonedLock,
    #[error("counter at `{0}` overflowed")]
    Overflow(PathBuf),
}

#[derive(Debug)]
pub struct Counter {
    path: PathBuf,
    value: Mutex<u64>,
}

impl Counter {
    pub fn open(path: impl Into<PathBuf>) -> Result<Self, CounterError> {
        let path = path.into();
        let value = read_initial_counter_value(&path)?;

        Ok(Self {
            path,
            value: Mutex::new(value),
        })
    }

    pub fn current(&self) -> Result<u64, CounterError> {
        let value = self.value.lock().map_err(|_| CounterError::PoisonedLock)?;
        Ok(*value)
    }

    pub fn next(&self) -> Result<u64, CounterError> {
        let mut value = self.value.lock().map_err(|_| CounterError::PoisonedLock)?;
        let next = value
            .checked_add(1)
            .ok_or_else(|| CounterError::Overflow(self.path.clone()))?;

        persist_counter_value(&self.path, next)?;
        *value = next;

        Ok(next)
    }
}

fn read_initial_counter_value(path: &Path) -> Result<u64, CounterError> {
    if !path.exists() {
        return Ok(0);
    }

    let value = fs::read_to_string(path).map_err(|source| CounterError::ReadCounter {
        path: path.to_path_buf(),
        source,
    })?;
    let value = value.trim();

    value
        .parse::<u64>()
        .map_err(|_| CounterError::InvalidCounterValue {
            path: path.to_path_buf(),
            value: value.to_string(),
        })
}

fn persist_counter_value(path: &Path, value: u64) -> Result<(), CounterError> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(|source| CounterError::CreateParentDir {
            path: parent.to_path_buf(),
            source,
        })?;
    }

    let tmp_path = temporary_counter_path(path);
    fs::write(&tmp_path, format!("{value}\n")).map_err(|source| CounterError::WriteCounter {
        path: tmp_path.clone(),
        source,
    })?;
    fs::rename(&tmp_path, path).map_err(|source| CounterError::WriteCounter {
        path: path.to_path_buf(),
        source,
    })?;

    Ok(())
}

fn temporary_counter_path(path: &Path) -> PathBuf {
    let now_nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| duration.as_nanos());
    let suffix = format!("tmp.{}.{}", std::process::id(), now_nanos);

    let mut tmp_path = path.to_path_buf();
    tmp_path.set_extension(suffix);
    tmp_path
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::path::{Path, PathBuf};
    use std::sync::atomic::{AtomicU64, Ordering};

    use super::{Counter, CounterError};

    #[test]
    fn counter_is_monotonic_and_persists_to_disk() {
        let temp_dir = TempDir::new("counter-monotonic");
        let counter_path = temp_dir.path().join("repositories.counter");

        let counter = Counter::open(counter_path.clone()).expect("counter opens");
        assert_eq!(counter.current().expect("counter has initial value"), 0);
        assert_eq!(counter.next().expect("first value"), 1);
        assert_eq!(counter.next().expect("second value"), 2);

        let reopened = Counter::open(counter_path).expect("counter reopens");
        assert_eq!(reopened.current().expect("counter value restored"), 2);
        assert_eq!(reopened.next().expect("counter continues monotonically"), 3);
    }

    #[test]
    fn counter_rejects_invalid_serialized_value() {
        let temp_dir = TempDir::new("counter-invalid");
        let counter_path = temp_dir.path().join("repositories.counter");
        fs::write(&counter_path, "not-a-number\n").expect("counter state should be writable");

        let err = Counter::open(counter_path).expect_err("invalid value must fail");
        assert!(matches!(
            err,
            CounterError::InvalidCounterValue { value, .. } if value == "not-a-number"
        ));
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
