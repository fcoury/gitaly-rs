use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use gitaly_cgroups::CommandIdentity;
use gitaly_limiter::concurrency::{AcquireError, ConcurrencyGuard};
use tonic::{Request, Status};

use super::MiddlewareContext;

const FALLBACK_LIMITER_KEY: &str = "global";
const REPOSITORY_METADATA_KEY: &str = "x-gitaly-repository";
const REQUEST_ID_METADATA_KEY: &str = "x-request-id";
const CORRELATION_ID_METADATA_KEY: &str = "x-correlation-id";
const FALLBACK_COMMAND_ID: &str = "unknown";

#[derive(Debug, Clone)]
struct RequestConcurrencyGuard {
    _guard: Arc<Mutex<ConcurrencyGuard>>,
}

impl RequestConcurrencyGuard {
    fn new(guard: ConcurrencyGuard) -> Self {
        Self {
            _guard: Arc::new(Mutex::new(guard)),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct AssignedCgroupPath(pub(crate) PathBuf);

pub(crate) fn apply(
    request: Request<()>,
    context: &MiddlewareContext,
) -> Result<Request<()>, Status> {
    let mut request = super::mark_step(request, "limiting")?;
    let limiter_key = limiter_key(&request);
    let guard = context
        .limiter()
        .try_acquire(limiter_key.clone())
        .map_err(acquire_error_to_status)?;

    request
        .extensions_mut()
        .insert(RequestConcurrencyGuard::new(guard));
    assign_bucket_path_if_configured(&mut request, context, limiter_key);

    Ok(request)
}

fn limiter_key(request: &Request<()>) -> String {
    request
        .metadata()
        .get(REPOSITORY_METADATA_KEY)
        .and_then(|value| value.to_str().ok())
        .filter(|value| !value.is_empty())
        .map(std::string::ToString::to_string)
        .unwrap_or_else(|| FALLBACK_LIMITER_KEY.to_string())
}

fn command_id(request: &Request<()>) -> String {
    request
        .metadata()
        .get(REQUEST_ID_METADATA_KEY)
        .or_else(|| request.metadata().get(CORRELATION_ID_METADATA_KEY))
        .and_then(|value| value.to_str().ok())
        .filter(|value| !value.is_empty())
        .map(std::string::ToString::to_string)
        .unwrap_or_else(|| FALLBACK_COMMAND_ID.to_string())
}

fn assign_bucket_path_if_configured(
    request: &mut Request<()>,
    context: &MiddlewareContext,
    repo_key: String,
) {
    let Some(cgroup_manager) = context.cgroup_manager() else {
        return;
    };

    let identity = CommandIdentity::new(repo_key, command_id(request));
    if let Ok(path) = cgroup_manager.assign_bucket_path(&identity) {
        request.extensions_mut().insert(AssignedCgroupPath(path));
    }
}

fn acquire_error_to_status(error: AcquireError) -> Status {
    Status::resource_exhausted(error.to_string())
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use gitaly_cgroups::{CgroupManager, CgroupManagerError};
    use gitaly_limiter::concurrency::ConcurrencyLimiter;
    use tonic::Code;

    use super::*;

    struct RecordingCgroupManager {
        assigned: Mutex<Vec<CommandIdentity>>,
        assigned_path: PathBuf,
    }

    impl RecordingCgroupManager {
        fn new(assigned_path: PathBuf) -> Self {
            Self {
                assigned: Mutex::new(Vec::new()),
                assigned_path,
            }
        }

        fn assigned(&self) -> Vec<CommandIdentity> {
            self.assigned
                .lock()
                .expect("recording lock should not be poisoned")
                .clone()
        }
    }

    impl CgroupManager for RecordingCgroupManager {
        fn assign_bucket_path(
            &self,
            command: &CommandIdentity,
        ) -> Result<PathBuf, CgroupManagerError> {
            self.assigned
                .lock()
                .expect("recording lock should not be poisoned")
                .push(command.clone());
            Ok(self.assigned_path.clone())
        }
    }

    struct FailingCgroupManager;

    impl CgroupManager for FailingCgroupManager {
        fn assign_bucket_path(
            &self,
            _command: &CommandIdentity,
        ) -> Result<PathBuf, CgroupManagerError> {
            Err(CgroupManagerError::InvalidBucketCount)
        }
    }

    #[test]
    fn apply_returns_resource_exhausted_when_limiter_is_exhausted() {
        let limiter = ConcurrencyLimiter::new(1, 0);
        let _held = limiter
            .try_acquire("repo-1")
            .expect("first acquire should succeed");
        let context = MiddlewareContext::new(limiter);
        let mut request = Request::new(());
        request.metadata_mut().insert(
            REPOSITORY_METADATA_KEY,
            "repo-1".parse().expect("metadata should parse"),
        );

        let status = apply(request, &context).expect_err("second acquire should be rejected");

        assert_eq!(status.code(), Code::ResourceExhausted);
    }

    #[test]
    fn apply_assigns_cgroup_path_when_manager_is_present() {
        let manager = std::sync::Arc::new(RecordingCgroupManager::new(PathBuf::from(
            "/tmp/gitaly-bucket-0001",
        )));
        let context = MiddlewareContext::default().with_cgroup_manager(manager.clone());
        let mut request = Request::new(());
        request.metadata_mut().insert(
            REPOSITORY_METADATA_KEY,
            "repo-42".parse().expect("metadata should parse"),
        );
        request.metadata_mut().insert(
            REQUEST_ID_METADATA_KEY,
            "req-1".parse().expect("metadata should parse"),
        );

        let request = apply(request, &context).expect("apply should succeed");
        let assigned = request
            .extensions()
            .get::<AssignedCgroupPath>()
            .expect("assigned cgroup path should be present");

        assert_eq!(assigned.0, PathBuf::from("/tmp/gitaly-bucket-0001"));
        assert_eq!(
            manager.assigned(),
            vec![CommandIdentity::new("repo-42", "req-1")]
        );
    }

    #[test]
    fn apply_ignores_cgroup_assignment_failures() {
        let context = MiddlewareContext::default()
            .with_cgroup_manager(std::sync::Arc::new(FailingCgroupManager));
        let mut request = Request::new(());
        request.metadata_mut().insert(
            REPOSITORY_METADATA_KEY,
            "repo-42".parse().expect("metadata should parse"),
        );

        let request = apply(request, &context).expect("apply should succeed");

        assert!(request.extensions().get::<AssignedCgroupPath>().is_none());
    }
}
