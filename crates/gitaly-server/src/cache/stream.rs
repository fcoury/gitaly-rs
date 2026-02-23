use std::{
    collections::HashMap,
    future::Future,
    sync::{
        atomic::{AtomicU64, AtomicUsize, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};

use thiserror::Error;
use tokio::sync::{Mutex, Notify};

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum StreamCacheError {
    #[error("stream build failed: {0}")]
    BuildFailed(String),
}

#[derive(Debug)]
pub struct StreamCache {
    ttl: Duration,
    max_entries: usize,
    state: Mutex<CacheState>,
    hits: AtomicU64,
    misses: AtomicU64,
    inflight: AtomicUsize,
    evictions: AtomicU64,
}

#[derive(Debug, Default)]
struct CacheState {
    entries: HashMap<String, CacheEntry>,
    access_tick: u64,
}

impl CacheState {
    fn next_access_tick(&mut self) -> u64 {
        self.access_tick = self.access_tick.wrapping_add(1);
        self.access_tick
    }
}

#[derive(Debug)]
enum CacheEntry {
    Ready(ReadyEntry),
    InFlight(Arc<Notify>),
}

#[derive(Debug)]
struct ReadyEntry {
    value: Arc<Vec<u8>>,
    expires_at: Instant,
    last_access_tick: u64,
}

enum NextStep {
    Wait(Arc<Notify>),
    Build(Arc<Notify>),
}

enum Lookup {
    Ready(Arc<Vec<u8>>),
    Expired,
    InFlight(Arc<Notify>),
    Missing,
}

impl StreamCache {
    #[must_use]
    pub fn new(ttl: Duration, max_entries: usize) -> Self {
        Self {
            ttl,
            max_entries,
            state: Mutex::new(CacheState::default()),
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
            inflight: AtomicUsize::new(0),
            evictions: AtomicU64::new(0),
        }
    }

    pub async fn get_or_insert_with<F, Fut>(
        &self,
        key: impl Into<String>,
        build: F,
    ) -> Result<Arc<Vec<u8>>, StreamCacheError>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<Arc<Vec<u8>>, StreamCacheError>>,
    {
        let key = key.into();
        let mut build = Some(build);

        loop {
            let next_step = {
                let mut state = self.state.lock().await;
                let now = Instant::now();

                let lookup = match state.entries.get(&key) {
                    Some(CacheEntry::Ready(ready)) if ready.expires_at > now => {
                        Lookup::Ready(Arc::clone(&ready.value))
                    }
                    Some(CacheEntry::Ready(_)) => Lookup::Expired,
                    Some(CacheEntry::InFlight(notify)) => Lookup::InFlight(Arc::clone(notify)),
                    None => Lookup::Missing,
                };

                match lookup {
                    Lookup::Ready(value) => {
                        let access_tick = state.next_access_tick();
                        if let Some(CacheEntry::Ready(ready)) = state.entries.get_mut(&key) {
                            ready.last_access_tick = access_tick;
                        }
                        self.hits.fetch_add(1, Ordering::Relaxed);
                        return Ok(value);
                    }
                    Lookup::InFlight(notify) => NextStep::Wait(notify),
                    Lookup::Expired | Lookup::Missing => {
                        if matches!(lookup, Lookup::Expired) {
                            state.entries.remove(&key);
                        }

                        self.misses.fetch_add(1, Ordering::Relaxed);
                        let notify = Arc::new(Notify::new());
                        state
                            .entries
                            .insert(key.clone(), CacheEntry::InFlight(Arc::clone(&notify)));
                        self.inflight.fetch_add(1, Ordering::Relaxed);
                        NextStep::Build(notify)
                    }
                }
            };

            match next_step {
                NextStep::Wait(notify) => {
                    let notified = notify.notified();
                    notified.await;
                }
                NextStep::Build(notify) => {
                    let result = build
                        .take()
                        .expect("builder must only run for the owner request")(
                    )
                    .await;

                    {
                        let mut state = self.state.lock().await;

                        if let Some(CacheEntry::InFlight(current)) = state.entries.get(&key) {
                            if Arc::ptr_eq(current, &notify) {
                                state.entries.remove(&key);
                                self.inflight.fetch_sub(1, Ordering::Relaxed);
                            }
                        }

                        if let Ok(value) = &result {
                            if self.max_entries > 0 {
                                let expires_at = Instant::now() + self.ttl;
                                let access_tick = state.next_access_tick();
                                state.entries.insert(
                                    key.clone(),
                                    CacheEntry::Ready(ReadyEntry {
                                        value: Arc::clone(value),
                                        expires_at,
                                        last_access_tick: access_tick,
                                    }),
                                );
                                self.evict_if_needed(&mut state);
                            }
                        }
                    }

                    notify.notify_waiters();
                    return result;
                }
            }
        }
    }

    fn evict_if_needed(&self, state: &mut CacheState) {
        let mut ready_entries = state
            .entries
            .iter()
            .filter_map(|(key, entry)| match entry {
                CacheEntry::Ready(ready) => Some((key.clone(), ready.last_access_tick)),
                CacheEntry::InFlight(_) => None,
            })
            .collect::<Vec<_>>();

        if ready_entries.len() <= self.max_entries {
            return;
        }

        ready_entries.sort_by(|(key_a, access_a), (key_b, access_b)| {
            access_a.cmp(access_b).then_with(|| key_a.cmp(key_b))
        });

        let to_evict = ready_entries.len() - self.max_entries;
        for (key, _) in ready_entries.into_iter().take(to_evict) {
            if matches!(state.entries.get(&key), Some(CacheEntry::Ready(_))) {
                state.entries.remove(&key);
                self.evictions.fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    #[must_use]
    pub fn hits(&self) -> u64 {
        self.hits.load(Ordering::Relaxed)
    }

    #[must_use]
    pub fn misses(&self) -> u64 {
        self.misses.load(Ordering::Relaxed)
    }

    #[must_use]
    pub fn inflight(&self) -> usize {
        self.inflight.load(Ordering::Relaxed)
    }

    #[must_use]
    pub fn evictions(&self) -> u64 {
        self.evictions.load(Ordering::Relaxed)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    };

    use tokio::sync::Barrier;
    use tokio::time::{sleep, Duration};

    use super::{StreamCache, StreamCacheError};

    #[tokio::test]
    async fn cache_hit_and_miss_behavior() {
        let cache = StreamCache::new(Duration::from_secs(60), 16);
        let builds = Arc::new(AtomicUsize::new(0));

        let first = cache
            .get_or_insert_with("pack:1", {
                let builds = Arc::clone(&builds);
                move || async move {
                    builds.fetch_add(1, Ordering::SeqCst);
                    Ok(Arc::new(vec![1, 2, 3]))
                }
            })
            .await
            .expect("first build succeeds");

        let second = cache
            .get_or_insert_with("pack:1", || async move {
                panic!("builder should not run on cache hit");
            })
            .await
            .expect("cache hit succeeds");

        assert_eq!(builds.load(Ordering::SeqCst), 1);
        assert!(Arc::ptr_eq(&first, &second));
        assert_eq!(cache.misses(), 1);
        assert_eq!(cache.hits(), 1);
        assert_eq!(cache.inflight(), 0);
        assert_eq!(cache.evictions(), 0);
    }

    #[tokio::test]
    async fn ttl_expiry_forces_rebuild() {
        let cache = StreamCache::new(Duration::from_millis(20), 16);
        let builds = Arc::new(AtomicUsize::new(0));

        let first = cache
            .get_or_insert_with("pack:ttl", {
                let builds = Arc::clone(&builds);
                move || async move {
                    builds.fetch_add(1, Ordering::SeqCst);
                    Ok(Arc::new(vec![1]))
                }
            })
            .await
            .expect("initial build succeeds");

        sleep(Duration::from_millis(35)).await;

        let second = cache
            .get_or_insert_with("pack:ttl", {
                let builds = Arc::clone(&builds);
                move || async move {
                    builds.fetch_add(1, Ordering::SeqCst);
                    Ok(Arc::new(vec![2]))
                }
            })
            .await
            .expect("rebuild succeeds after ttl");

        assert_eq!(builds.load(Ordering::SeqCst), 2);
        assert_eq!(&*first, &[1]);
        assert_eq!(&*second, &[2]);
        assert_eq!(cache.misses(), 2);
        assert_eq!(cache.hits(), 0);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn single_flight_dedupes_concurrent_builders() {
        let cache = Arc::new(StreamCache::new(Duration::from_secs(60), 16));
        let builds = Arc::new(AtomicUsize::new(0));
        let start = Arc::new(Barrier::new(8));

        let mut handles = Vec::new();
        for _ in 0..8 {
            let cache = Arc::clone(&cache);
            let builds = Arc::clone(&builds);
            let start = Arc::clone(&start);
            handles.push(tokio::spawn(async move {
                start.wait().await;
                cache
                    .get_or_insert_with("pack:shared", move || async move {
                        builds.fetch_add(1, Ordering::SeqCst);
                        sleep(Duration::from_millis(40)).await;
                        Ok(Arc::new(vec![9, 9, 9]))
                    })
                    .await
            }));
        }

        let mut payloads = Vec::new();
        for handle in handles {
            let payload = handle
                .await
                .expect("task join succeeds")
                .expect("cache call succeeds");
            payloads.push(payload);
        }

        let first = Arc::clone(&payloads[0]);
        for payload in payloads.iter().skip(1) {
            assert!(Arc::ptr_eq(&first, payload));
        }

        assert_eq!(builds.load(Ordering::SeqCst), 1);
        assert_eq!(cache.misses(), 1);
        assert_eq!(cache.hits(), 7);
        assert_eq!(cache.inflight(), 0);
    }

    #[tokio::test]
    async fn builder_error_is_not_cached() {
        let cache = StreamCache::new(Duration::from_secs(60), 16);
        let builds = Arc::new(AtomicUsize::new(0));

        let first = cache
            .get_or_insert_with("pack:error", {
                let builds = Arc::clone(&builds);
                move || async move {
                    builds.fetch_add(1, Ordering::SeqCst);
                    Err(StreamCacheError::BuildFailed("boom".to_string()))
                }
            })
            .await;
        assert!(matches!(first, Err(StreamCacheError::BuildFailed(_))));

        let second = cache
            .get_or_insert_with("pack:error", {
                let builds = Arc::clone(&builds);
                move || async move {
                    builds.fetch_add(1, Ordering::SeqCst);
                    Ok(Arc::new(vec![4, 5, 6]))
                }
            })
            .await
            .expect("second build succeeds");

        assert_eq!(&*second, &[4, 5, 6]);
        assert_eq!(builds.load(Ordering::SeqCst), 2);
        assert_eq!(cache.misses(), 2);
        assert_eq!(cache.hits(), 0);
    }

    #[tokio::test]
    async fn max_entries_evicts_oldest_accessed_entry() {
        let cache = StreamCache::new(Duration::from_secs(60), 2);

        cache
            .get_or_insert_with("k1", || async { Ok(Arc::new(vec![1])) })
            .await
            .expect("k1 insert succeeds");
        cache
            .get_or_insert_with("k2", || async { Ok(Arc::new(vec![2])) })
            .await
            .expect("k2 insert succeeds");

        cache
            .get_or_insert_with("k1", || async {
                panic!("k1 should still be cached");
            })
            .await
            .expect("k1 hit succeeds");

        cache
            .get_or_insert_with("k3", || async { Ok(Arc::new(vec![3])) })
            .await
            .expect("k3 insert succeeds");
        assert_eq!(cache.evictions(), 1);

        let rebuilt_k2 = cache
            .get_or_insert_with("k2", || async { Ok(Arc::new(vec![20])) })
            .await
            .expect("k2 rebuild succeeds");

        assert_eq!(&*rebuilt_k2, &[20]);
    }
}
