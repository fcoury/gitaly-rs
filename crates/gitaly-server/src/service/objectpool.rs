use std::sync::Arc;

use tonic::{Request, Response, Status};

use gitaly_proto::gitaly::object_pool_service_server::ObjectPoolService;
use gitaly_proto::gitaly::*;

use crate::dependencies::Dependencies;

#[derive(Debug, Clone)]
pub struct ObjectPoolServiceImpl {
    dependencies: Arc<Dependencies>,
}

impl ObjectPoolServiceImpl {
    #[must_use]
    pub fn new(dependencies: Arc<Dependencies>) -> Self {
        Self { dependencies }
    }

    fn unimplemented(&self, method: &'static str) -> Status {
        let _ = &self.dependencies;
        unimplemented_object_pool_rpc(method)
    }
}

fn unimplemented_object_pool_rpc(method: &'static str) -> Status {
    Status::unimplemented(format!("ObjectPoolService::{method} is not implemented"))
}

#[tonic::async_trait]
impl ObjectPoolService for ObjectPoolServiceImpl {
    async fn create_object_pool(
        &self,
        _request: Request<CreateObjectPoolRequest>,
    ) -> Result<Response<CreateObjectPoolResponse>, Status> {
        Err(self.unimplemented("create_object_pool"))
    }

    async fn delete_object_pool(
        &self,
        _request: Request<DeleteObjectPoolRequest>,
    ) -> Result<Response<DeleteObjectPoolResponse>, Status> {
        Err(self.unimplemented("delete_object_pool"))
    }

    async fn link_repository_to_object_pool(
        &self,
        _request: Request<LinkRepositoryToObjectPoolRequest>,
    ) -> Result<Response<LinkRepositoryToObjectPoolResponse>, Status> {
        Err(self.unimplemented("link_repository_to_object_pool"))
    }

    async fn disconnect_git_alternates(
        &self,
        _request: Request<DisconnectGitAlternatesRequest>,
    ) -> Result<Response<DisconnectGitAlternatesResponse>, Status> {
        Err(self.unimplemented("disconnect_git_alternates"))
    }

    async fn fetch_into_object_pool(
        &self,
        _request: Request<FetchIntoObjectPoolRequest>,
    ) -> Result<Response<FetchIntoObjectPoolResponse>, Status> {
        Err(self.unimplemented("fetch_into_object_pool"))
    }

    async fn get_object_pool(
        &self,
        _request: Request<GetObjectPoolRequest>,
    ) -> Result<Response<GetObjectPoolResponse>, Status> {
        Err(self.unimplemented("get_object_pool"))
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

    use gitaly_proto::gitaly::object_pool_service_client::ObjectPoolServiceClient;
    use gitaly_proto::gitaly::object_pool_service_server::ObjectPoolServiceServer;
    use gitaly_proto::gitaly::GetObjectPoolRequest;

    use crate::dependencies::Dependencies;

    use super::ObjectPoolServiceImpl;

    async fn start_server(
        service: ObjectPoolServiceImpl,
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
                .add_service(ObjectPoolServiceServer::new(service))
                .serve_with_shutdown(server_addr, async {
                    let _ = shutdown_rx.await;
                })
                .await
                .expect("server should run");
        });

        (format!("http://{server_addr}"), shutdown_tx, server_task)
    }

    async fn connect_client(
        endpoint: String,
    ) -> ObjectPoolServiceClient<tonic::transport::Channel> {
        for _ in 0..50 {
            match ObjectPoolServiceClient::connect(endpoint.clone()).await {
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
            start_server(ObjectPoolServiceImpl::new(test_dependencies())).await;
        let mut client = connect_client(endpoint).await;

        let error = client
            .get_object_pool(GetObjectPoolRequest::default())
            .await
            .expect_err("get_object_pool should be unimplemented");
        assert_eq!(error.code(), Code::Unimplemented);
        assert_eq!(
            error.message(),
            "ObjectPoolService::get_object_pool is not implemented"
        );

        shutdown_server(shutdown_tx, server_task).await;
    }
}
