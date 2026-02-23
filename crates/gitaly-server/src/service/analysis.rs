use std::pin::Pin;
use std::sync::Arc;

use tokio_stream::Stream;
use tonic::{Request, Response, Status};

use gitaly_proto::gitaly::analysis_service_server::AnalysisService;
use gitaly_proto::gitaly::*;

use crate::dependencies::Dependencies;

type ServiceStream<T> = Pin<Box<dyn Stream<Item = Result<T, Status>> + Send + 'static>>;

#[derive(Debug, Clone)]
pub struct AnalysisServiceImpl {
    dependencies: Arc<Dependencies>,
}

impl AnalysisServiceImpl {
    #[must_use]
    pub fn new(dependencies: Arc<Dependencies>) -> Self {
        Self { dependencies }
    }

    fn unimplemented(&self, method: &'static str) -> Status {
        let _ = &self.dependencies;
        unimplemented_analysis_rpc(method)
    }
}

fn unimplemented_analysis_rpc(method: &'static str) -> Status {
    Status::unimplemented(format!("AnalysisService::{method} is not implemented"))
}

#[tonic::async_trait]
impl AnalysisService for AnalysisServiceImpl {
    type CheckBlobsGeneratedStream = ServiceStream<CheckBlobsGeneratedResponse>;

    async fn check_blobs_generated(
        &self,
        _request: Request<tonic::Streaming<CheckBlobsGeneratedRequest>>,
    ) -> Result<Response<Self::CheckBlobsGeneratedStream>, Status> {
        Err(self.unimplemented("check_blobs_generated"))
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

    use gitaly_proto::gitaly::analysis_service_client::AnalysisServiceClient;
    use gitaly_proto::gitaly::analysis_service_server::AnalysisServiceServer;
    use gitaly_proto::gitaly::CheckBlobsGeneratedRequest;

    use crate::dependencies::Dependencies;

    use super::AnalysisServiceImpl;

    async fn start_server(
        service: AnalysisServiceImpl,
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
                .add_service(AnalysisServiceServer::new(service))
                .serve_with_shutdown(server_addr, async {
                    let _ = shutdown_rx.await;
                })
                .await
                .expect("server should run");
        });

        (format!("http://{server_addr}"), shutdown_tx, server_task)
    }

    async fn connect_client(endpoint: String) -> AnalysisServiceClient<tonic::transport::Channel> {
        for _ in 0..50 {
            match AnalysisServiceClient::connect(endpoint.clone()).await {
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
            start_server(AnalysisServiceImpl::new(test_dependencies())).await;
        let mut client = connect_client(endpoint).await;

        let request_stream = tokio_stream::iter(vec![CheckBlobsGeneratedRequest::default()]);
        let error = client
            .check_blobs_generated(request_stream)
            .await
            .expect_err("check_blobs_generated should be unimplemented");

        assert_eq!(error.code(), Code::Unimplemented);

        shutdown_server(shutdown_tx, server_task).await;
    }
}
