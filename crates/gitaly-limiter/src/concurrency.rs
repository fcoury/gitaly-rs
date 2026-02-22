use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use thiserror::Error;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

#[derive(Debug, Clone)]
pub struct ConcurrencyLimiter {
    concurrency_limit: usize,
    queue_limit: usize,
    entries: Arc<Mutex<HashMap<String, Arc<KeyEntry>>>>,
}

impl ConcurrencyLimiter {
    pub fn new(concurrency_limit: usize, queue_limit: usize) -> Self {
        assert!(
            concurrency_limit > 0,
            "concurrency_limit must be greater than zero"
        );

        Self {
            concurrency_limit,
            queue_limit,
            entries: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub async fn acquire(&self, key: impl Into<String>) -> Result<ConcurrencyGuard, AcquireError> {
        let key = key.into();
        let entry = self.entry_for_key(&key);

        let queue_permit = entry
            .queue
            .clone()
            .try_acquire_owned()
            .map_err(|_| AcquireError::QueueFull { key: key.clone() })?;

        let concurrency_permit = entry
            .concurrency
            .clone()
            .acquire_owned()
            .await
            .expect("limiter semaphores are never closed");

        Ok(ConcurrencyGuard {
            _key: key,
            _queue_permit: queue_permit,
            _concurrency_permit: concurrency_permit,
        })
    }

    pub fn try_acquire(&self, key: impl Into<String>) -> Result<ConcurrencyGuard, AcquireError> {
        let key = key.into();
        let entry = self.entry_for_key(&key);

        let queue_permit = entry
            .queue
            .clone()
            .try_acquire_owned()
            .map_err(|_| AcquireError::QueueFull { key: key.clone() })?;

        let concurrency_permit = match entry.concurrency.clone().try_acquire_owned() {
            Ok(permit) => permit,
            Err(_) => return Err(AcquireError::ConcurrencyFull { key }),
        };

        Ok(ConcurrencyGuard {
            _key: key,
            _queue_permit: queue_permit,
            _concurrency_permit: concurrency_permit,
        })
    }

    pub async fn acquire_with_timeout(
        &self,
        key: impl Into<String>,
        timeout: Duration,
    ) -> Result<ConcurrencyGuard, AcquireError> {
        let key = key.into();
        let entry = self.entry_for_key(&key);

        let queue_permit = entry
            .queue
            .clone()
            .try_acquire_owned()
            .map_err(|_| AcquireError::QueueFull { key: key.clone() })?;

        let concurrency_permit =
            match tokio::time::timeout(timeout, entry.concurrency.clone().acquire_owned()).await {
                Ok(permit_result) => permit_result.expect("limiter semaphores are never closed"),
                Err(_) => return Err(AcquireError::QueueTimeout { key, timeout }),
            };

        Ok(ConcurrencyGuard {
            _key: key,
            _queue_permit: queue_permit,
            _concurrency_permit: concurrency_permit,
        })
    }

    fn entry_for_key(&self, key: &str) -> Arc<KeyEntry> {
        let mut entries = self
            .entries
            .lock()
            .expect("limiter key map lock should not be poisoned");

        entries
            .entry(key.to_string())
            .or_insert_with(|| Arc::new(KeyEntry::new(self.concurrency_limit, self.queue_limit)))
            .clone()
    }
}

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum AcquireError {
    /// Admission queue is saturated (running + waiting).
    /// Returned by all acquire APIs before waiting for a concurrency slot.
    #[error("queue is full for key `{key}`")]
    QueueFull { key: String },
    /// Admission succeeded, but no concurrency slot is currently available.
    /// Returned only by `try_acquire()`.
    #[error("concurrency slots are exhausted for key `{key}`")]
    ConcurrencyFull { key: String },
    /// Admission succeeded, but waiting for a concurrency slot exceeded timeout.
    /// Returned only by `acquire_with_timeout()`.
    #[error("timed out waiting for an available slot for key `{key}` after {timeout:?}")]
    QueueTimeout { key: String, timeout: Duration },
}

#[derive(Debug)]
struct KeyEntry {
    queue: Arc<Semaphore>,
    concurrency: Arc<Semaphore>,
}

impl KeyEntry {
    fn new(concurrency_limit: usize, queue_limit: usize) -> Self {
        let queue_capacity = concurrency_limit
            .checked_add(queue_limit)
            .expect("queue and concurrency limits overflowed");

        Self {
            queue: Arc::new(Semaphore::new(queue_capacity)),
            concurrency: Arc::new(Semaphore::new(concurrency_limit)),
        }
    }
}

#[derive(Debug)]
pub struct ConcurrencyGuard {
    _key: String,
    _queue_permit: OwnedSemaphorePermit,
    _concurrency_permit: OwnedSemaphorePermit,
}

#[cfg(test)]
mod tests {
    use super::{AcquireError, ConcurrencyLimiter};
    use std::time::Duration;

    #[tokio::test]
    async fn acquire_release_allows_next_waiter_to_run() {
        let limiter = ConcurrencyLimiter::new(1, 1);
        let first = limiter
            .acquire("repo-1")
            .await
            .expect("first acquire should succeed");

        let limiter_clone = limiter.clone();
        let waiter = tokio::spawn(async move {
            limiter_clone
                .acquire_with_timeout("repo-1", Duration::from_secs(1))
                .await
        });

        tokio::task::yield_now().await;
        drop(first);

        let second = tokio::time::timeout(Duration::from_millis(100), waiter)
            .await
            .expect("waiter task should complete after permit release")
            .expect("waiter task should not panic")
            .expect("queued acquire should succeed");

        drop(second);
    }

    #[tokio::test]
    async fn acquire_with_timeout_rejects_when_queue_is_full() {
        let limiter = ConcurrencyLimiter::new(1, 0);
        let first = limiter
            .acquire("repo-1")
            .await
            .expect("first acquire should succeed");

        let result = limiter
            .acquire_with_timeout("repo-1", Duration::from_millis(50))
            .await;
        assert_eq!(
            result.expect_err("second acquire should be rejected"),
            AcquireError::QueueFull {
                key: "repo-1".to_string()
            }
        );

        drop(first);
    }

    #[tokio::test(start_paused = true)]
    async fn queued_acquire_times_out() {
        let limiter = ConcurrencyLimiter::new(1, 1);
        let first = limiter
            .acquire("repo-1")
            .await
            .expect("first acquire should succeed");

        let limiter_clone = limiter.clone();
        let waiting = tokio::spawn(async move {
            limiter_clone
                .acquire_with_timeout("repo-1", Duration::from_secs(5))
                .await
        });

        tokio::task::yield_now().await;
        tokio::time::advance(Duration::from_secs(6)).await;

        let result = waiting.await.expect("waiting task should not panic");
        assert_eq!(
            result.expect_err("queued acquire should time out"),
            AcquireError::QueueTimeout {
                key: "repo-1".to_string(),
                timeout: Duration::from_secs(5),
            }
        );

        drop(first);
    }

    #[tokio::test]
    async fn different_keys_do_not_block_each_other() {
        let limiter = ConcurrencyLimiter::new(1, 0);
        let first_key_guard = limiter
            .acquire("repo-1")
            .await
            .expect("first key should acquire");

        let second_key_guard = limiter
            .acquire_with_timeout("repo-2", Duration::from_millis(50))
            .await
            .expect("independent key should not be blocked");

        drop(first_key_guard);
        drop(second_key_guard);
    }

    #[test]
    fn try_acquire_rejects_when_queue_is_full() {
        let limiter = ConcurrencyLimiter::new(1, 0);
        let first = limiter
            .try_acquire("repo-1")
            .expect("first acquire should succeed");

        let result = limiter.try_acquire("repo-1");
        assert_eq!(
            result.expect_err("second try_acquire should fail"),
            AcquireError::QueueFull {
                key: "repo-1".to_string()
            }
        );

        drop(first);
    }

    #[test]
    fn try_acquire_rejects_when_concurrency_is_full_with_spare_queue_capacity() {
        let limiter = ConcurrencyLimiter::new(1, 1);
        let first = limiter
            .try_acquire("repo-1")
            .expect("first acquire should succeed");

        let result = limiter.try_acquire("repo-1");
        assert_eq!(
            result.expect_err("second try_acquire should fail"),
            AcquireError::ConcurrencyFull {
                key: "repo-1".to_string()
            }
        );

        drop(first);
    }
}
