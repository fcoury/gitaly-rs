use std::pin::Pin;
use std::sync::Arc;

use tokio_stream::Stream;
use tonic::{Request, Response, Status};

use gitaly_proto::gitaly::cleanup_service_server::CleanupService;
use gitaly_proto::gitaly::*;

use crate::dependencies::Dependencies;

type ServiceStream<T> = Pin<Box<dyn Stream<Item = Result<T, Status>> + Send + 'static>>;

#[derive(Debug, Clone)]
pub struct CleanupServiceImpl {
    dependencies: Arc<Dependencies>,
}

impl CleanupServiceImpl {
    #[must_use]
    pub fn new(dependencies: Arc<Dependencies>) -> Self {
        Self { dependencies }
    }

    fn unimplemented(&self, method: &'static str) -> Status {
        let _ = &self.dependencies;
        unimplemented_cleanup_rpc(method)
    }
}

fn unimplemented_cleanup_rpc(method: &'static str) -> Status {
    Status::unimplemented(format!("CleanupService::{method} is not implemented"))
}

#[tonic::async_trait]
impl CleanupService for CleanupServiceImpl {
    type ApplyBfgObjectMapStreamStream = ServiceStream<ApplyBfgObjectMapStreamResponse>;

    async fn apply_bfg_object_map_stream(
        &self,
        _request: Request<tonic::Streaming<ApplyBfgObjectMapStreamRequest>>,
    ) -> Result<Response<Self::ApplyBfgObjectMapStreamStream>, Status> {
        Err(self.unimplemented("apply_bfg_object_map_stream"))
    }

    async fn rewrite_history(
        &self,
        _request: Request<tonic::Streaming<RewriteHistoryRequest>>,
    ) -> Result<Response<RewriteHistoryResponse>, Status> {
        Err(self.unimplemented("rewrite_history"))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use tokio::sync::oneshot;
    use tokio::task::JoinHandle;
    use tokio::time::sleep;
    use tonic::Code;

    use gitaly_proto::gitaly::cleanup_service_client::CleanupServiceClient;
    use gitaly_proto::gitaly::cleanup_service_server::CleanupServiceServer;
    use gitaly_proto::gitaly::RewriteHistoryRequest;

    use crate::dependencies::Dependencies;

    use super::CleanupServiceImpl;

    async fn start_server(
        service: CleanupServiceImpl,
    ) -> (String, oneshot::Sender<()>, JoinHandle<()>) {
        let listener =
            std::net::TcpListener::bind("127.0.0.1:0").expect("ephemeral listener should bind");
        let server_addr = listener
            .local_addr()
            .expect("ephemeral listener should provide local address");
        drop(listener);

        let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
        let server_task = tokio::spawn(async move {
            tonic::transport::Server::builder()
                .add_service(CleanupServiceServer::new(service))
                .serve_with_shutdown(server_addr, async {
                    let _ = shutdown_rx.await;
                })
                .await
                .expect("server should run");
        });

        (format!("http://{server_addr}"), shutdown_tx, server_task)
    }

    async fn connect_client(endpoint: String) -> CleanupServiceClient<tonic::transport::Channel> {
        for _ in 0..50 {
            match CleanupServiceClient::connect(endpoint.clone()).await {
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

    fn test_dependencies() -> Arc<Dependencies> {
        Arc::new(Dependencies::default())
    }

    #[tokio::test]
    async fn representative_method_returns_unimplemented_status() {
        let (endpoint, shutdown_tx, server_task) =
            start_server(CleanupServiceImpl::new(test_dependencies())).await;
        let mut client = connect_client(endpoint).await;

        let rewrite_history_stream = tokio_stream::iter(vec![RewriteHistoryRequest::default()]);
        let error = client
            .rewrite_history(rewrite_history_stream)
            .await
            .expect_err("rewrite_history should be unimplemented");
        assert_eq!(error.code(), Code::Unimplemented);
        assert_eq!(
            error.message(),
            "CleanupService::rewrite_history is not implemented"
        );

        shutdown_server(shutdown_tx, server_task).await;
    }
}
