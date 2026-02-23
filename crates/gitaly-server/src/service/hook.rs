use std::pin::Pin;
use std::sync::Arc;

use tokio_stream::Stream;
use tonic::{Request, Response, Status};

use gitaly_proto::gitaly::hook_service_server::HookService;
use gitaly_proto::gitaly::*;

use crate::dependencies::Dependencies;

type ServiceStream<T> = Pin<Box<dyn Stream<Item = Result<T, Status>> + Send + 'static>>;

#[derive(Debug, Clone)]
pub struct HookServiceImpl {
    dependencies: Arc<Dependencies>,
}

impl HookServiceImpl {
    #[must_use]
    pub fn new(dependencies: Arc<Dependencies>) -> Self {
        Self { dependencies }
    }

    fn unimplemented(&self, method: &'static str) -> Status {
        let _ = &self.dependencies;
        unimplemented_hook_rpc(method)
    }
}

fn unimplemented_hook_rpc(method: &'static str) -> Status {
    Status::unimplemented(format!("HookService::{method} is not implemented"))
}

#[tonic::async_trait]
impl HookService for HookServiceImpl {
    type PreReceiveHookStream = ServiceStream<PreReceiveHookResponse>;
    type PostReceiveHookStream = ServiceStream<PostReceiveHookResponse>;
    type UpdateHookStream = ServiceStream<UpdateHookResponse>;
    type ReferenceTransactionHookStream = ServiceStream<ReferenceTransactionHookResponse>;
    type ProcReceiveHookStream = ServiceStream<ProcReceiveHookResponse>;

    async fn pre_receive_hook(
        &self,
        _request: Request<tonic::Streaming<PreReceiveHookRequest>>,
    ) -> Result<Response<Self::PreReceiveHookStream>, Status> {
        Err(self.unimplemented("pre_receive_hook"))
    }

    async fn post_receive_hook(
        &self,
        _request: Request<tonic::Streaming<PostReceiveHookRequest>>,
    ) -> Result<Response<Self::PostReceiveHookStream>, Status> {
        Err(self.unimplemented("post_receive_hook"))
    }

    async fn update_hook(
        &self,
        _request: Request<UpdateHookRequest>,
    ) -> Result<Response<Self::UpdateHookStream>, Status> {
        Err(self.unimplemented("update_hook"))
    }

    async fn reference_transaction_hook(
        &self,
        _request: Request<tonic::Streaming<ReferenceTransactionHookRequest>>,
    ) -> Result<Response<Self::ReferenceTransactionHookStream>, Status> {
        Err(self.unimplemented("reference_transaction_hook"))
    }

    async fn pack_objects_hook_with_sidechannel(
        &self,
        _request: Request<PackObjectsHookWithSidechannelRequest>,
    ) -> Result<Response<PackObjectsHookWithSidechannelResponse>, Status> {
        Err(self.unimplemented("pack_objects_hook_with_sidechannel"))
    }

    async fn proc_receive_hook(
        &self,
        _request: Request<tonic::Streaming<ProcReceiveHookRequest>>,
    ) -> Result<Response<Self::ProcReceiveHookStream>, Status> {
        Err(self.unimplemented("proc_receive_hook"))
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

    use gitaly_proto::gitaly::hook_service_client::HookServiceClient;
    use gitaly_proto::gitaly::hook_service_server::HookServiceServer;
    use gitaly_proto::gitaly::{
        PackObjectsHookWithSidechannelRequest, PreReceiveHookRequest, UpdateHookRequest,
    };

    use crate::dependencies::Dependencies;

    use super::HookServiceImpl;

    async fn start_server(
        service: HookServiceImpl,
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
                .add_service(HookServiceServer::new(service))
                .serve_with_shutdown(server_addr, async {
                    let _ = shutdown_rx.await;
                })
                .await
                .expect("server should run");
        });

        (format!("http://{server_addr}"), shutdown_tx, server_task)
    }

    async fn connect_client(endpoint: String) -> HookServiceClient<tonic::transport::Channel> {
        for _ in 0..50 {
            match HookServiceClient::connect(endpoint.clone()).await {
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
            start_server(HookServiceImpl::new(test_dependencies())).await;
        let mut client = connect_client(endpoint).await;

        let pre_receive_stream = tokio_stream::iter(vec![PreReceiveHookRequest::default()]);
        let pre_receive_error = client
            .pre_receive_hook(pre_receive_stream)
            .await
            .expect_err("pre_receive_hook should be unimplemented");
        assert_eq!(pre_receive_error.code(), Code::Unimplemented);

        let update_hook_error = client
            .update_hook(UpdateHookRequest::default())
            .await
            .expect_err("update_hook should be unimplemented");
        assert_eq!(update_hook_error.code(), Code::Unimplemented);

        let pack_objects_error = client
            .pack_objects_hook_with_sidechannel(PackObjectsHookWithSidechannelRequest::default())
            .await
            .expect_err("pack_objects_hook_with_sidechannel should be unimplemented");
        assert_eq!(pack_objects_error.code(), Code::Unimplemented);

        shutdown_server(shutdown_tx, server_task).await;
    }
}
