use std::sync::Arc;

use gitaly_cgroups::{
    build_platform_cgroup_manager, CgroupConfig, CgroupManager, CgroupManagerError,
};
use gitaly_limiter::concurrency::ConcurrencyLimiter;
use tonic::{Request, Status};

pub mod auth;
pub mod cache_invalidation;
pub mod correlation_id;
pub mod limiting;
pub mod log_fields;
pub mod logging;
pub mod metrics;
pub mod panic;
pub mod request_info;
pub mod sentry;
pub mod sidechannel;
pub mod status;

const DEFAULT_LIMITER_CONCURRENCY_LIMIT: usize = 1024;
const DEFAULT_LIMITER_QUEUE_LIMIT: usize = 0;

#[derive(Clone)]
pub struct MiddlewareContext {
    limiter: ConcurrencyLimiter,
    cgroup_manager: Option<Arc<dyn CgroupManager>>,
    auth_token: Option<String>,
    allow_unauthenticated: bool,
}

impl MiddlewareContext {
    #[must_use]
    pub fn new(limiter: ConcurrencyLimiter) -> Self {
        Self {
            limiter,
            cgroup_manager: None,
            auth_token: None,
            allow_unauthenticated: false,
        }
    }

    #[must_use]
    pub fn with_limiter(mut self, limiter: ConcurrencyLimiter) -> Self {
        self.limiter = limiter;
        self
    }

    #[must_use]
    pub fn with_cgroup_manager(mut self, cgroup_manager: Arc<dyn CgroupManager>) -> Self {
        self.cgroup_manager = Some(cgroup_manager);
        self
    }

    #[must_use]
    pub fn with_auth_token(
        mut self,
        auth_token: impl Into<String>,
        allow_unauthenticated: bool,
    ) -> Self {
        self.auth_token = Some(auth_token.into());
        self.allow_unauthenticated = allow_unauthenticated;
        self
    }

    #[must_use]
    pub fn with_allow_unauthenticated(mut self, allow_unauthenticated: bool) -> Self {
        self.allow_unauthenticated = allow_unauthenticated;
        self
    }

    pub fn with_platform_cgroup_manager(
        mut self,
        config: CgroupConfig,
    ) -> Result<Self, CgroupManagerError> {
        self.cgroup_manager = Some(build_platform_cgroup_manager(config)?);
        Ok(self)
    }

    pub(crate) fn limiter(&self) -> &ConcurrencyLimiter {
        &self.limiter
    }

    pub(crate) fn cgroup_manager(&self) -> Option<&Arc<dyn CgroupManager>> {
        self.cgroup_manager.as_ref()
    }

    pub(crate) fn auth_token(&self) -> Option<&str> {
        self.auth_token.as_deref()
    }

    pub(crate) fn allow_unauthenticated(&self) -> bool {
        self.allow_unauthenticated
    }
}

impl Default for MiddlewareContext {
    fn default() -> Self {
        Self {
            limiter: ConcurrencyLimiter::new(
                DEFAULT_LIMITER_CONCURRENCY_LIMIT,
                DEFAULT_LIMITER_QUEUE_LIMIT,
            ),
            cgroup_manager: None,
            auth_token: None,
            allow_unauthenticated: false,
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct MiddlewareTrace {
    pub steps: Vec<&'static str>,
}

#[must_use]
pub fn ordered_interceptor() -> impl FnMut(Request<()>) -> Result<Request<()>, Status> + Clone {
    ordered_interceptor_with_context(Arc::new(MiddlewareContext::default()))
}

#[must_use]
pub fn ordered_interceptor_with_context(
    context: Arc<MiddlewareContext>,
) -> impl FnMut(Request<()>) -> Result<Request<()>, Status> + Clone {
    move |request| run_chain(request, &context)
}

fn run_chain(request: Request<()>, context: &MiddlewareContext) -> Result<Request<()>, Status> {
    let request = correlation_id::apply(request)?;
    let request = request_info::apply(request)?;
    let request = metrics::apply(request)?;
    let request = log_fields::apply(request)?;
    let request = logging::apply(request)?;
    let request = sentry::apply(request)?;
    let request = status::apply(request)?;
    let request = auth::apply(request, context)?;
    let request = limiting::apply(request, context)?;
    let request = cache_invalidation::apply(request)?;
    let request = sidechannel::apply(request)?;
    panic::apply(request)
}

fn mark_step(mut request: Request<()>, step: &'static str) -> Result<Request<()>, Status> {
    if let Some(trace) = request.extensions_mut().get_mut::<MiddlewareTrace>() {
        trace.steps.push(step);
    } else {
        request
            .extensions_mut()
            .insert(MiddlewareTrace { steps: vec![step] });
    }

    Ok(request)
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;
    use std::sync::Arc;
    use std::time::{SystemTime, UNIX_EPOCH};

    use gitaly_limiter::concurrency::ConcurrencyLimiter;
    use tonic::Code;
    use tonic::Request;

    use gitaly_cgroups::CgroupConfig;

    use super::{limiting, ordered_interceptor, run_chain, MiddlewareContext, MiddlewareTrace};

    const ORDERED_STEPS: [&str; 12] = [
        "correlation_id",
        "request_info",
        "metrics",
        "log_fields",
        "logging",
        "sentry",
        "status",
        "auth",
        "limiting",
        "cache_invalidation",
        "sidechannel",
        "panic",
    ];

    #[test]
    fn run_chain_marks_steps_in_documented_order() {
        let context = MiddlewareContext::default();
        let request =
            run_chain(Request::new(()), &context).expect("middleware chain should succeed");

        assert_eq!(trace_steps(&request), ORDERED_STEPS.to_vec());
    }

    #[test]
    fn ordered_interceptor_applies_chain_in_documented_order() {
        let mut interceptor = ordered_interceptor();
        let request = interceptor(Request::new(())).expect("interceptor should succeed");

        assert_eq!(trace_steps(&request), ORDERED_STEPS.to_vec());
    }

    #[test]
    fn ordered_interceptor_with_context_applies_chain_in_documented_order() {
        let context = Arc::new(MiddlewareContext::default());
        let mut interceptor = super::ordered_interceptor_with_context(context);
        let request = interceptor(Request::new(())).expect("interceptor should succeed");

        assert_eq!(trace_steps(&request), ORDERED_STEPS.to_vec());
    }

    #[test]
    fn with_platform_cgroup_manager_enables_best_effort_assignment() {
        let temp_dir = unique_test_dir("platform");
        let config = CgroupConfig::new(&temp_dir, 4).expect("valid cgroup config");
        let context = MiddlewareContext::default()
            .with_platform_cgroup_manager(config)
            .expect("platform manager should configure");

        let request = run_chain(Request::new(()), &context).expect("middleware chain should work");

        let assigned = request.extensions().get::<limiting::AssignedCgroupPath>();
        #[cfg(target_os = "linux")]
        assert!(
            assigned.is_some(),
            "linux path should assign cgroup bucket when manager is configured"
        );
        #[cfg(not(target_os = "linux"))]
        assert!(
            assigned.is_some(),
            "non-linux should still compute deterministic noop bucket path"
        );

        remove_dir_if_exists(&temp_dir);
    }

    #[test]
    fn run_chain_enforces_auth_when_token_is_configured() {
        let context = MiddlewareContext::new(ConcurrencyLimiter::new(1, 0))
            .with_auth_token("secret-token", false);
        let error = run_chain(Request::new(()), &context).expect_err("request should be rejected");

        assert_eq!(error.code(), Code::Unauthenticated);
    }

    fn trace_steps(request: &Request<()>) -> Vec<&'static str> {
        request
            .extensions()
            .get::<MiddlewareTrace>()
            .map_or_else(Vec::new, |trace| trace.steps.clone())
    }

    fn unique_test_dir(suffix: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock drift")
            .as_nanos();
        std::env::temp_dir().join(format!("gitaly-server-middleware-{suffix}-{nanos}"))
    }

    fn remove_dir_if_exists(path: &PathBuf) {
        if path.exists() {
            std::fs::remove_dir_all(path).expect("test temp directory should be removable");
        }
    }
}
