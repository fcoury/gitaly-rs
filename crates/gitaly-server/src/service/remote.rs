use std::sync::Arc;

use tonic::{Request, Response, Status};

use gitaly_proto::gitaly::remote_service_server::RemoteService;
use gitaly_proto::gitaly::*;

use crate::dependencies::Dependencies;

#[derive(Debug, Clone)]
pub struct RemoteServiceImpl {
    dependencies: Arc<Dependencies>,
}

impl RemoteServiceImpl {
    #[must_use]
    pub fn new(dependencies: Arc<Dependencies>) -> Self {
        Self { dependencies }
    }

    fn unimplemented(&self, method: &'static str) -> Status {
        let _ = &self.dependencies;
        unimplemented_remote_rpc(method)
    }
}

fn unimplemented_remote_rpc(method: &'static str) -> Status {
    Status::unimplemented(format!("RemoteService::{method} is not implemented"))
}

#[tonic::async_trait]
impl RemoteService for RemoteServiceImpl {
    async fn update_remote_mirror(
        &self,
        _request: Request<tonic::Streaming<UpdateRemoteMirrorRequest>>,
    ) -> Result<Response<UpdateRemoteMirrorResponse>, Status> {
        Err(self.unimplemented("update_remote_mirror"))
    }

    async fn find_remote_repository(
        &self,
        _request: Request<FindRemoteRepositoryRequest>,
    ) -> Result<Response<FindRemoteRepositoryResponse>, Status> {
        Err(self.unimplemented("find_remote_repository"))
    }

    async fn find_remote_root_ref(
        &self,
        _request: Request<FindRemoteRootRefRequest>,
    ) -> Result<Response<FindRemoteRootRefResponse>, Status> {
        Err(self.unimplemented("find_remote_root_ref"))
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

    use gitaly_proto::gitaly::remote_service_client::RemoteServiceClient;
    use gitaly_proto::gitaly::remote_service_server::RemoteServiceServer;
    use gitaly_proto::gitaly::{
        FindRemoteRepositoryRequest, FindRemoteRootRefRequest, UpdateRemoteMirrorRequest,
    };

    use crate::dependencies::Dependencies;

    use super::RemoteServiceImpl;

    async fn start_server(
        service: RemoteServiceImpl,
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
                .add_service(RemoteServiceServer::new(service))
                .serve_with_shutdown(server_addr, async {
                    let _ = shutdown_rx.await;
                })
                .await
                .expect("server should run");
        });

        (format!("http://{server_addr}"), shutdown_tx, server_task)
    }

    async fn connect_client(endpoint: String) -> RemoteServiceClient<tonic::transport::Channel> {
        for _ in 0..50 {
            match RemoteServiceClient::connect(endpoint.clone()).await {
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
            start_server(RemoteServiceImpl::new(test_dependencies())).await;
        let mut client = connect_client(endpoint).await;

        let update_mirror_stream = tokio_stream::iter(vec![UpdateRemoteMirrorRequest::default()]);
        let update_mirror_error = client
            .update_remote_mirror(update_mirror_stream)
            .await
            .expect_err("update_remote_mirror should be unimplemented");
        assert_eq!(update_mirror_error.code(), Code::Unimplemented);

        let find_repository_error = client
            .find_remote_repository(FindRemoteRepositoryRequest::default())
            .await
            .expect_err("find_remote_repository should be unimplemented");
        assert_eq!(find_repository_error.code(), Code::Unimplemented);

        let find_root_ref_error = client
            .find_remote_root_ref(FindRemoteRootRefRequest::default())
            .await
            .expect_err("find_remote_root_ref should be unimplemented");
        assert_eq!(find_root_ref_error.code(), Code::Unimplemented);

        shutdown_server(shutdown_tx, server_task).await;
    }
}
