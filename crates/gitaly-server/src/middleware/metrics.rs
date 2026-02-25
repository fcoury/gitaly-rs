use std::collections::HashMap;
use std::sync::{Mutex, OnceLock};

use tonic::{Request, Status};

use super::request_info::RequestInfo;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RequestMetricSnapshot {
    pub(crate) full_method: String,
    pub(crate) total_seen: u64,
}

static REQUEST_COUNTS: OnceLock<Mutex<HashMap<String, u64>>> = OnceLock::new();
static REJECTION_COUNTS: OnceLock<Mutex<HashMap<(String, String), u64>>> = OnceLock::new();

pub(crate) fn apply(mut request: Request<()>) -> Result<Request<()>, Status> {
    let full_method = request
        .extensions()
        .get::<RequestInfo>()
        .map(|info| info.full_method.clone())
        .unwrap_or_else(|| "/unknown/unknown".to_string());

    let total_seen = increment_count(&full_method);
    request.extensions_mut().insert(RequestMetricSnapshot {
        full_method,
        total_seen,
    });

    super::mark_step(request, "metrics")
}

pub(crate) fn record_rejection(reason: &str, full_method: &str) -> u64 {
    let mut counts = rejection_counts()
        .lock()
        .expect("rejection counters mutex should not be poisoned");
    let entry = counts
        .entry((reason.to_string(), full_method.to_string()))
        .or_insert(0);
    *entry += 1;
    *entry
}

fn increment_count(full_method: &str) -> u64 {
    let mut counts = request_counts()
        .lock()
        .expect("request counters mutex should not be poisoned");
    let entry = counts.entry(full_method.to_string()).or_insert(0);
    *entry += 1;
    *entry
}

fn request_counts() -> &'static Mutex<HashMap<String, u64>> {
    REQUEST_COUNTS.get_or_init(|| Mutex::new(HashMap::new()))
}

fn rejection_counts() -> &'static Mutex<HashMap<(String, String), u64>> {
    REJECTION_COUNTS.get_or_init(|| Mutex::new(HashMap::new()))
}

#[cfg(test)]
pub(crate) fn count_for_method(full_method: &str) -> u64 {
    request_counts()
        .lock()
        .expect("request counters mutex should not be poisoned")
        .get(full_method)
        .copied()
        .unwrap_or(0)
}

#[cfg(test)]
pub(crate) fn reset_counts_for_test() {
    request_counts()
        .lock()
        .expect("request counters mutex should not be poisoned")
        .clear();
}

#[cfg(test)]
pub(crate) fn rejection_count_for_test(reason: &str, full_method: &str) -> u64 {
    rejection_counts()
        .lock()
        .expect("rejection counters mutex should not be poisoned")
        .get(&(reason.to_string(), full_method.to_string()))
        .copied()
        .unwrap_or(0)
}

#[cfg(test)]
pub(crate) fn reset_rejection_counts_for_test() {
    rejection_counts()
        .lock()
        .expect("rejection counters mutex should not be poisoned")
        .clear();
}

#[cfg(test)]
mod tests {
    use tonic::Request;

    use super::{
        apply, count_for_method, record_rejection, rejection_count_for_test, reset_counts_for_test,
        reset_rejection_counts_for_test, RequestMetricSnapshot,
    };
    use crate::middleware::request_info::RequestInfo;

    #[test]
    fn apply_increments_per_method_counter() {
        reset_counts_for_test();
        let method = "/gitaly.RepositoryService/RepositoryExists";

        let mut request = Request::new(());
        request.extensions_mut().insert(RequestInfo {
            service: "gitaly.RepositoryService".to_string(),
            method: "RepositoryExists".to_string(),
            full_method: method.to_string(),
        });

        let first = apply(request).expect("first request should succeed");
        assert_eq!(count_for_method(method), 1);
        assert_eq!(
            first.extensions().get::<RequestMetricSnapshot>(),
            Some(&RequestMetricSnapshot {
                full_method: method.to_string(),
                total_seen: 1,
            })
        );

        let mut request = Request::new(());
        request.extensions_mut().insert(RequestInfo {
            service: "gitaly.RepositoryService".to_string(),
            method: "RepositoryExists".to_string(),
            full_method: method.to_string(),
        });
        let second = apply(request).expect("second request should succeed");
        assert_eq!(count_for_method(method), 2);
        assert_eq!(
            second.extensions().get::<RequestMetricSnapshot>(),
            Some(&RequestMetricSnapshot {
                full_method: method.to_string(),
                total_seen: 2,
            })
        );
    }

    #[test]
    fn record_rejection_increments_per_reason_and_method() {
        reset_rejection_counts_for_test();
        let method = "/gitaly.RepositoryService/RepositoryExists";
        let reason = "auth.unauthenticated";

        assert_eq!(record_rejection(reason, method), 1);
        assert_eq!(record_rejection(reason, method), 2);
        assert_eq!(rejection_count_for_test(reason, method), 2);
    }
}
