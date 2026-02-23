use std::sync::Arc;

use tonic::transport::server::Router;

use gitaly_cgroups::{CgroupConfig, CgroupManagerError};
use gitaly_config::RuntimeConfig;
use gitaly_limiter::concurrency::ConcurrencyLimiter;
use gitaly_proto::gitaly::blob_service_server::BlobServiceServer;
use gitaly_proto::gitaly::commit_service_server::CommitServiceServer;
use gitaly_proto::gitaly::diff_service_server::DiffServiceServer;
use gitaly_proto::gitaly::hook_service_server::HookServiceServer;
use gitaly_proto::gitaly::operation_service_server::OperationServiceServer;
use gitaly_proto::gitaly::ref_service_server::RefServiceServer;
use gitaly_proto::gitaly::repository_service_server::RepositoryServiceServer;
use gitaly_proto::gitaly::server_service_server::ServerServiceServer;
use gitaly_proto::gitaly::smart_http_service_server::SmartHttpServiceServer;
use gitaly_proto::gitaly::ssh_service_server::SshServiceServer;
use thiserror::Error;

use crate::dependencies::Dependencies;
use crate::middleware;
use crate::middleware::MiddlewareContext;
use crate::runtime::RuntimePaths;
use crate::service::blob::BlobServiceImpl;
use crate::service::commit::CommitServiceImpl;
use crate::service::diff::DiffServiceImpl;
use crate::service::hook::HookServiceImpl;
use crate::service::operations::OperationServiceImpl;
use crate::service::ref_::RefServiceImpl;
use crate::service::repository::RepositoryServiceImpl;
use crate::service::server::ServerServiceImpl;
use crate::service::smarthttp::SmartHttpServiceImpl;
use crate::service::ssh::SshServiceImpl;

#[derive(Debug, Default, Clone, Copy)]
pub struct GitalyServer;

#[derive(Debug, Error)]
pub enum ServerBootstrapError {
    #[error("runtime limiter concurrency limit must be greater than zero")]
    InvalidLimiterConcurrencyLimit,
    #[error("runtime cgroup bucket count must be greater than zero when cgroups are enabled")]
    InvalidCgroupBucketCount,
    #[error(transparent)]
    CgroupManager(#[from] CgroupManagerError),
}

impl GitalyServer {
    #[must_use]
    pub fn build_router() -> Router {
        Self::build_router_with_dependencies_and_middleware_context(
            Arc::new(Dependencies::default()),
            Arc::new(MiddlewareContext::default()),
        )
    }

    #[must_use]
    pub fn build_router_with_dependencies(dependencies: Arc<Dependencies>) -> Router {
        Self::build_router_with_dependencies_and_middleware_context(
            dependencies,
            Arc::new(MiddlewareContext::default()),
        )
    }

    #[must_use]
    pub fn build_router_with_middleware_context(
        middleware_context: Arc<MiddlewareContext>,
    ) -> Router {
        Self::build_router_with_dependencies_and_middleware_context(
            Arc::new(Dependencies::default()),
            middleware_context,
        )
    }

    pub fn build_router_with_runtime_paths(
        runtime_paths: &RuntimePaths,
    ) -> Result<Router, ServerBootstrapError> {
        Self::build_router_with_dependencies_runtime_config(
            Arc::new(Dependencies::default()),
            runtime_paths,
            &RuntimeConfig::default(),
        )
    }

    pub fn build_router_with_runtime_config(
        runtime_paths: &RuntimePaths,
        runtime_config: &RuntimeConfig,
    ) -> Result<Router, ServerBootstrapError> {
        Self::build_router_with_dependencies_runtime_config(
            Arc::new(Dependencies::default()),
            runtime_paths,
            runtime_config,
        )
    }

    pub fn build_router_with_dependencies_runtime_config(
        dependencies: Arc<Dependencies>,
        runtime_paths: &RuntimePaths,
        runtime_config: &RuntimeConfig,
    ) -> Result<Router, ServerBootstrapError> {
        if runtime_config.limiter.concurrency_limit == 0 {
            return Err(ServerBootstrapError::InvalidLimiterConcurrencyLimit);
        }

        if runtime_config.cgroups.enabled && runtime_config.cgroups.bucket_count == 0 {
            return Err(ServerBootstrapError::InvalidCgroupBucketCount);
        }

        let mut middleware_context =
            MiddlewareContext::default().with_limiter(ConcurrencyLimiter::new(
                runtime_config.limiter.concurrency_limit,
                runtime_config.limiter.queue_limit,
            ));

        if runtime_config.cgroups.enabled {
            let cgroup_config = CgroupConfig::new(
                runtime_paths.cgroup_base_dir(),
                runtime_config.cgroups.bucket_count,
            )?;
            middleware_context = middleware_context.with_platform_cgroup_manager(cgroup_config)?;
        }

        Ok(Self::build_router_with_dependencies_and_middleware_context(
            dependencies,
            Arc::new(middleware_context),
        ))
    }

    pub fn build_router_with_dependencies_and_runtime_paths(
        dependencies: Arc<Dependencies>,
        runtime_paths: &RuntimePaths,
    ) -> Result<Router, ServerBootstrapError> {
        Self::build_router_with_dependencies_runtime_config(
            dependencies,
            runtime_paths,
            &RuntimeConfig::default(),
        )
    }

    #[must_use]
    pub fn build_router_with_dependencies_and_middleware_context(
        dependencies: Arc<Dependencies>,
        middleware_context: Arc<MiddlewareContext>,
    ) -> Router {
        let interceptor = middleware::ordered_interceptor_with_context(middleware_context);

        tonic::transport::Server::builder()
            .add_service(ServerServiceServer::with_interceptor(
                ServerServiceImpl::new(Arc::clone(&dependencies)),
                interceptor.clone(),
            ))
            .add_service(RepositoryServiceServer::with_interceptor(
                RepositoryServiceImpl::new(Arc::clone(&dependencies)),
                interceptor.clone(),
            ))
            .add_service(CommitServiceServer::with_interceptor(
                CommitServiceImpl::new(Arc::clone(&dependencies)),
                interceptor.clone(),
            ))
            .add_service(BlobServiceServer::with_interceptor(
                BlobServiceImpl::new(Arc::clone(&dependencies)),
                interceptor.clone(),
            ))
            .add_service(DiffServiceServer::with_interceptor(
                DiffServiceImpl::new(Arc::clone(&dependencies)),
                interceptor.clone(),
            ))
            .add_service(HookServiceServer::with_interceptor(
                HookServiceImpl::new(Arc::clone(&dependencies)),
                interceptor.clone(),
            ))
            .add_service(OperationServiceServer::with_interceptor(
                OperationServiceImpl::new(Arc::clone(&dependencies)),
                interceptor.clone(),
            ))
            .add_service(SmartHttpServiceServer::with_interceptor(
                SmartHttpServiceImpl::new(Arc::clone(&dependencies)),
                interceptor.clone(),
            ))
            .add_service(SshServiceServer::with_interceptor(
                SshServiceImpl::new(Arc::clone(&dependencies)),
                interceptor.clone(),
            ))
            .add_service(RefServiceServer::with_interceptor(
                RefServiceImpl::new(dependencies),
                interceptor,
            ))
    }
}

#[cfg(test)]
mod tests {
    use std::path::{Path, PathBuf};
    use std::sync::Arc;
    use std::time::Duration;

    use tokio::sync::oneshot;
    use tokio::task::JoinHandle;
    use tokio::time::sleep;

    use gitaly_config::{CgroupConfig as RuntimeCgroupConfig, LimiterConfig, RuntimeConfig};
    use gitaly_limiter::concurrency::ConcurrencyLimiter;
    use gitaly_proto::gitaly::server_service_client::ServerServiceClient;
    use gitaly_proto::gitaly::ServerInfoRequest;
    use tonic::Code;

    use crate::dependencies::Dependencies;
    use crate::middleware::MiddlewareContext;
    use crate::runtime::RuntimePaths;

    use super::{GitalyServer, ServerBootstrapError};

    async fn start_server(
        router: tonic::transport::server::Router,
    ) -> (String, oneshot::Sender<()>, JoinHandle<()>) {
        let listener =
            std::net::TcpListener::bind("127.0.0.1:0").expect("ephemeral listener should bind");
        let server_addr = listener
            .local_addr()
            .expect("ephemeral listener should provide local address");
        drop(listener);

        let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
        let server_task = tokio::spawn(async move {
            router
                .serve_with_shutdown(server_addr, async {
                    let _ = shutdown_rx.await;
                })
                .await
                .expect("router should run");
        });

        (format!("http://{server_addr}"), shutdown_tx, server_task)
    }

    async fn connect_client(endpoint: String) -> ServerServiceClient<tonic::transport::Channel> {
        for _ in 0..50 {
            match ServerServiceClient::connect(endpoint.clone()).await {
                Ok(client) => return client,
                Err(_) => sleep(Duration::from_millis(10)).await,
            }
        }

        panic!("server did not become reachable at {endpoint}");
    }

    async fn shutdown_server(shutdown_tx: oneshot::Sender<()>, server_task: JoinHandle<()>) {
        let _ = shutdown_tx.send(());
        let join_result = tokio::time::timeout(Duration::from_secs(2), server_task)
            .await
            .expect("server should stop within timeout");
        join_result.expect("server task should not panic");
    }

    #[tokio::test]
    async fn build_router_with_dependencies_uses_injected_dependencies() {
        let dependencies = Arc::new(Dependencies::new("gitaly-rs-custom", "git custom-version"));
        let (endpoint, shutdown_tx, server_task) = start_server(
            GitalyServer::build_router_with_dependencies(Arc::clone(&dependencies)),
        )
        .await;
        let mut client = connect_client(endpoint).await;

        let response = client
            .server_info(ServerInfoRequest {})
            .await
            .expect("server_info should succeed")
            .into_inner();

        assert_eq!(response.server_version, dependencies.server_version);
        assert_eq!(response.git_version, dependencies.git_version);

        shutdown_server(shutdown_tx, server_task).await;
    }

    #[tokio::test]
    async fn build_router_delegates_to_default_dependencies() {
        let (endpoint, shutdown_tx, server_task) = start_server(GitalyServer::build_router()).await;
        let mut client = connect_client(endpoint).await;

        let response = client
            .server_info(ServerInfoRequest {})
            .await
            .expect("server_info should succeed")
            .into_inner();

        assert_eq!(response.server_version, env!("CARGO_PKG_VERSION"));
        assert_eq!(response.git_version, "unknown");

        shutdown_server(shutdown_tx, server_task).await;
    }

    #[tokio::test]
    async fn build_router_with_injected_middleware_context_uses_limiter() {
        let limiter = ConcurrencyLimiter::new(1, 0);
        let _held = limiter
            .try_acquire("global")
            .expect("first acquire should succeed");
        let middleware_context = Arc::new(MiddlewareContext::new(limiter));
        let (endpoint, shutdown_tx, server_task) = start_server(
            GitalyServer::build_router_with_dependencies_and_middleware_context(
                Arc::new(Dependencies::default()),
                middleware_context,
            ),
        )
        .await;
        let mut client = connect_client(endpoint).await;

        let error = client
            .server_info(ServerInfoRequest {})
            .await
            .expect_err("request should be rejected by limiter");

        assert_eq!(error.code(), Code::ResourceExhausted);

        shutdown_server(shutdown_tx, server_task).await;
    }

    #[tokio::test]
    async fn build_router_with_runtime_paths_configures_platform_cgroup_defaults() {
        let temp = TempDir::new("router-runtime");
        let runtime_paths = RuntimePaths::bootstrap_for_pid(temp.path().to_path_buf(), 42)
            .expect("runtime bootstrap should succeed");
        let (endpoint, shutdown_tx, server_task) = start_server(
            GitalyServer::build_router_with_runtime_paths(&runtime_paths)
                .expect("router should build with runtime paths"),
        )
        .await;
        let mut client = connect_client(endpoint).await;

        let response = client
            .server_info(ServerInfoRequest {})
            .await
            .expect("server_info should succeed")
            .into_inner();
        assert_eq!(response.server_version, env!("CARGO_PKG_VERSION"));

        shutdown_server(shutdown_tx, server_task).await;
    }

    #[tokio::test]
    async fn build_router_with_runtime_config_supports_disabling_cgroups() {
        let temp = TempDir::new("router-runtime-disabled-cgroups");
        let runtime_paths = RuntimePaths::bootstrap_for_pid(temp.path().to_path_buf(), 43)
            .expect("runtime bootstrap should succeed");
        let runtime_config = RuntimeConfig {
            limiter: LimiterConfig {
                concurrency_limit: 64,
                queue_limit: 4,
            },
            cgroups: RuntimeCgroupConfig {
                enabled: false,
                bucket_count: 0,
            },
        };
        let (endpoint, shutdown_tx, server_task) = start_server(
            GitalyServer::build_router_with_runtime_config(&runtime_paths, &runtime_config)
                .expect("router should build with disabled cgroups"),
        )
        .await;
        let mut client = connect_client(endpoint).await;

        let response = client
            .server_info(ServerInfoRequest {})
            .await
            .expect("server_info should succeed")
            .into_inner();
        assert_eq!(response.server_version, env!("CARGO_PKG_VERSION"));

        shutdown_server(shutdown_tx, server_task).await;
    }

    #[test]
    fn build_router_with_runtime_config_rejects_invalid_limiter_config() {
        let temp = TempDir::new("router-runtime-invalid-limiter");
        let runtime_paths = RuntimePaths::bootstrap_for_pid(temp.path().to_path_buf(), 44)
            .expect("runtime bootstrap should succeed");
        let runtime_config = RuntimeConfig {
            limiter: LimiterConfig {
                concurrency_limit: 0,
                queue_limit: 4,
            },
            cgroups: RuntimeCgroupConfig {
                enabled: true,
                bucket_count: 8,
            },
        };

        let error = GitalyServer::build_router_with_runtime_config(&runtime_paths, &runtime_config)
            .expect_err("invalid limiter config should fail");

        assert!(matches!(
            error,
            ServerBootstrapError::InvalidLimiterConcurrencyLimit
        ));
    }

    struct TempDir {
        path: PathBuf,
    }

    impl TempDir {
        fn new(prefix: &str) -> Self {
            let path = std::env::temp_dir().join(format!(
                "gitaly-server-tests-{prefix}-{}-{}",
                std::process::id(),
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .expect("clock drift")
                    .as_nanos(),
            ));
            std::fs::create_dir_all(&path).expect("temp directory should be creatable");
            Self { path }
        }

        fn path(&self) -> &Path {
            &self.path
        }
    }

    impl Drop for TempDir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.path);
        }
    }
}
