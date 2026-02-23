use std::pin::Pin;
use std::sync::Arc;

use tokio_stream::Stream;
use tonic::{Request, Response, Status};

use gitaly_proto::gitaly::raft_service_server::RaftService;
use gitaly_proto::gitaly::*;

use crate::dependencies::Dependencies;

type ServiceStream<T> = Pin<Box<dyn Stream<Item = Result<T, Status>> + Send + 'static>>;

#[derive(Debug, Clone)]
pub struct RaftServiceImpl {
    dependencies: Arc<Dependencies>,
}

impl RaftServiceImpl {
    #[must_use]
    pub fn new(dependencies: Arc<Dependencies>) -> Self {
        Self { dependencies }
    }

    fn unimplemented(&self, method: &'static str) -> Status {
        let _ = &self.dependencies;
        unimplemented_raft_rpc(method)
    }
}

fn unimplemented_raft_rpc(method: &'static str) -> Status {
    Status::unimplemented(format!("RaftService::{method} is not implemented"))
}

#[tonic::async_trait]
impl RaftService for RaftServiceImpl {
    type GetPartitionsStream = ServiceStream<GetPartitionsResponse>;

    async fn send_message(
        &self,
        _request: Request<tonic::Streaming<RaftMessageRequest>>,
    ) -> Result<Response<RaftMessageResponse>, Status> {
        Err(self.unimplemented("send_message"))
    }

    async fn send_snapshot(
        &self,
        _request: Request<tonic::Streaming<RaftSnapshotMessageRequest>>,
    ) -> Result<Response<RaftSnapshotMessageResponse>, Status> {
        Err(self.unimplemented("send_snapshot"))
    }

    async fn join_cluster(
        &self,
        _request: Request<JoinClusterRequest>,
    ) -> Result<Response<JoinClusterResponse>, Status> {
        Err(self.unimplemented("join_cluster"))
    }

    async fn get_partitions(
        &self,
        _request: Request<GetPartitionsRequest>,
    ) -> Result<Response<Self::GetPartitionsStream>, Status> {
        Err(self.unimplemented("get_partitions"))
    }

    async fn get_cluster_info(
        &self,
        _request: Request<RaftClusterInfoRequest>,
    ) -> Result<Response<RaftClusterInfoResponse>, Status> {
        Err(self.unimplemented("get_cluster_info"))
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

    use gitaly_proto::gitaly::raft_service_client::RaftServiceClient;
    use gitaly_proto::gitaly::raft_service_server::RaftServiceServer;
    use gitaly_proto::gitaly::{GetPartitionsRequest, RaftMessageRequest};

    use crate::dependencies::Dependencies;

    use super::RaftServiceImpl;

    async fn start_server(
        service: RaftServiceImpl,
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
                .add_service(RaftServiceServer::new(service))
                .serve_with_shutdown(server_addr, async {
                    let _ = shutdown_rx.await;
                })
                .await
                .expect("server should run");
        });

        (format!("http://{server_addr}"), shutdown_tx, server_task)
    }

    async fn connect_client(endpoint: String) -> RaftServiceClient<tonic::transport::Channel> {
        for _ in 0..50 {
            match RaftServiceClient::connect(endpoint.clone()).await {
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
    async fn representative_methods_return_unimplemented_status() {
        let (endpoint, shutdown_tx, server_task) =
            start_server(RaftServiceImpl::new(test_dependencies())).await;
        let mut client = connect_client(endpoint).await;

        let message_stream = tokio_stream::iter(vec![RaftMessageRequest::default()]);
        let send_message_error = client
            .send_message(message_stream)
            .await
            .expect_err("send_message should be unimplemented");
        assert_eq!(send_message_error.code(), Code::Unimplemented);

        let get_partitions_error = client
            .get_partitions(GetPartitionsRequest::default())
            .await
            .expect_err("get_partitions should be unimplemented");
        assert_eq!(get_partitions_error.code(), Code::Unimplemented);

        shutdown_server(shutdown_tx, server_task).await;
    }
}
