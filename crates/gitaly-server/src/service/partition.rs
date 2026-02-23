use std::sync::Arc;

use tonic::{Request, Response, Status};

use gitaly_proto::gitaly::partition_service_server::PartitionService;
use gitaly_proto::gitaly::*;

use crate::dependencies::Dependencies;

#[derive(Debug, Clone)]
pub struct PartitionServiceImpl {
    dependencies: Arc<Dependencies>,
}

impl PartitionServiceImpl {
    #[must_use]
    pub fn new(dependencies: Arc<Dependencies>) -> Self {
        Self { dependencies }
    }

    fn unimplemented(&self, method: &'static str) -> Status {
        let _ = &self.dependencies;
        unimplemented_partition_rpc(method)
    }
}

fn unimplemented_partition_rpc(method: &'static str) -> Status {
    Status::unimplemented(format!("PartitionService::{method} is not implemented"))
}

#[tonic::async_trait]
impl PartitionService for PartitionServiceImpl {
    async fn backup_partition(
        &self,
        _request: Request<BackupPartitionRequest>,
    ) -> Result<Response<BackupPartitionResponse>, Status> {
        Err(self.unimplemented("backup_partition"))
    }

    async fn list_partitions(
        &self,
        _request: Request<ListPartitionsRequest>,
    ) -> Result<Response<ListPartitionsResponse>, Status> {
        Err(self.unimplemented("list_partitions"))
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

    use gitaly_proto::gitaly::partition_service_client::PartitionServiceClient;
    use gitaly_proto::gitaly::partition_service_server::PartitionServiceServer;
    use gitaly_proto::gitaly::{BackupPartitionRequest, ListPartitionsRequest};

    use crate::dependencies::Dependencies;

    use super::PartitionServiceImpl;

    async fn start_server(
        service: PartitionServiceImpl,
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
                .add_service(PartitionServiceServer::new(service))
                .serve_with_shutdown(server_addr, async {
                    let _ = shutdown_rx.await;
                })
                .await
                .expect("server should run");
        });

        (format!("http://{server_addr}"), shutdown_tx, server_task)
    }

    async fn connect_client(endpoint: String) -> PartitionServiceClient<tonic::transport::Channel> {
        for _ in 0..50 {
            match PartitionServiceClient::connect(endpoint.clone()).await {
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
            start_server(PartitionServiceImpl::new(test_dependencies())).await;
        let mut client = connect_client(endpoint).await;

        let backup_error = client
            .backup_partition(BackupPartitionRequest::default())
            .await
            .expect_err("backup_partition should be unimplemented");
        assert_eq!(backup_error.code(), Code::Unimplemented);

        let list_error = client
            .list_partitions(ListPartitionsRequest::default())
            .await
            .expect_err("list_partitions should be unimplemented");
        assert_eq!(list_error.code(), Code::Unimplemented);

        shutdown_server(shutdown_tx, server_task).await;
    }
}
