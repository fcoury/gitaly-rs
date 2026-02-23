use std::pin::Pin;
use std::sync::Arc;

use tokio_stream::Stream;
use tonic::{Request, Response, Status};

use gitaly_proto::gitaly::operation_service_server::OperationService;
use gitaly_proto::gitaly::*;

use crate::dependencies::Dependencies;

type ServiceStream<T> = Pin<Box<dyn Stream<Item = Result<T, Status>> + Send + 'static>>;

#[derive(Debug, Clone)]
pub struct OperationServiceImpl {
    dependencies: Arc<Dependencies>,
}

impl OperationServiceImpl {
    #[must_use]
    pub fn new(dependencies: Arc<Dependencies>) -> Self {
        Self { dependencies }
    }

    fn unimplemented(&self, method: &'static str) -> Status {
        let _ = &self.dependencies;
        unimplemented_operation_rpc(method)
    }
}

fn unimplemented_operation_rpc(method: &'static str) -> Status {
    Status::unimplemented(format!("OperationService::{method} is not implemented"))
}

#[tonic::async_trait]
impl OperationService for OperationServiceImpl {
    type UserMergeBranchStream = ServiceStream<UserMergeBranchResponse>;
    type UserRebaseConfirmableStream = ServiceStream<UserRebaseConfirmableResponse>;

    async fn user_create_branch(
        &self,
        _request: Request<UserCreateBranchRequest>,
    ) -> Result<Response<UserCreateBranchResponse>, Status> {
        Err(self.unimplemented("user_create_branch"))
    }

    async fn user_update_branch(
        &self,
        _request: Request<UserUpdateBranchRequest>,
    ) -> Result<Response<UserUpdateBranchResponse>, Status> {
        Err(self.unimplemented("user_update_branch"))
    }

    async fn user_delete_branch(
        &self,
        _request: Request<UserDeleteBranchRequest>,
    ) -> Result<Response<UserDeleteBranchResponse>, Status> {
        Err(self.unimplemented("user_delete_branch"))
    }

    async fn user_create_tag(
        &self,
        _request: Request<UserCreateTagRequest>,
    ) -> Result<Response<UserCreateTagResponse>, Status> {
        Err(self.unimplemented("user_create_tag"))
    }

    async fn user_delete_tag(
        &self,
        _request: Request<UserDeleteTagRequest>,
    ) -> Result<Response<UserDeleteTagResponse>, Status> {
        Err(self.unimplemented("user_delete_tag"))
    }

    async fn user_merge_to_ref(
        &self,
        _request: Request<UserMergeToRefRequest>,
    ) -> Result<Response<UserMergeToRefResponse>, Status> {
        Err(self.unimplemented("user_merge_to_ref"))
    }

    async fn user_rebase_to_ref(
        &self,
        _request: Request<UserRebaseToRefRequest>,
    ) -> Result<Response<UserRebaseToRefResponse>, Status> {
        Err(self.unimplemented("user_rebase_to_ref"))
    }

    async fn user_merge_branch(
        &self,
        _request: Request<tonic::Streaming<UserMergeBranchRequest>>,
    ) -> Result<Response<Self::UserMergeBranchStream>, Status> {
        Err(self.unimplemented("user_merge_branch"))
    }

    async fn user_ff_branch(
        &self,
        _request: Request<UserFfBranchRequest>,
    ) -> Result<Response<UserFfBranchResponse>, Status> {
        Err(self.unimplemented("user_ff_branch"))
    }

    async fn user_cherry_pick(
        &self,
        _request: Request<UserCherryPickRequest>,
    ) -> Result<Response<UserCherryPickResponse>, Status> {
        Err(self.unimplemented("user_cherry_pick"))
    }

    async fn user_commit_files(
        &self,
        _request: Request<tonic::Streaming<UserCommitFilesRequest>>,
    ) -> Result<Response<UserCommitFilesResponse>, Status> {
        Err(self.unimplemented("user_commit_files"))
    }

    async fn user_rebase_confirmable(
        &self,
        _request: Request<tonic::Streaming<UserRebaseConfirmableRequest>>,
    ) -> Result<Response<Self::UserRebaseConfirmableStream>, Status> {
        Err(self.unimplemented("user_rebase_confirmable"))
    }

    async fn user_revert(
        &self,
        _request: Request<UserRevertRequest>,
    ) -> Result<Response<UserRevertResponse>, Status> {
        Err(self.unimplemented("user_revert"))
    }

    async fn user_squash(
        &self,
        _request: Request<UserSquashRequest>,
    ) -> Result<Response<UserSquashResponse>, Status> {
        Err(self.unimplemented("user_squash"))
    }

    async fn user_apply_patch(
        &self,
        _request: Request<tonic::Streaming<UserApplyPatchRequest>>,
    ) -> Result<Response<UserApplyPatchResponse>, Status> {
        Err(self.unimplemented("user_apply_patch"))
    }

    async fn user_update_submodule(
        &self,
        _request: Request<UserUpdateSubmoduleRequest>,
    ) -> Result<Response<UserUpdateSubmoduleResponse>, Status> {
        Err(self.unimplemented("user_update_submodule"))
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

    use gitaly_proto::gitaly::operation_service_client::OperationServiceClient;
    use gitaly_proto::gitaly::operation_service_server::OperationServiceServer;
    use gitaly_proto::gitaly::{UserCreateBranchRequest, UserMergeBranchRequest};

    use crate::dependencies::Dependencies;

    use super::OperationServiceImpl;

    async fn start_server(
        service: OperationServiceImpl,
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
                .add_service(OperationServiceServer::new(service))
                .serve_with_shutdown(server_addr, async {
                    let _ = shutdown_rx.await;
                })
                .await
                .expect("server should run");
        });

        (format!("http://{server_addr}"), shutdown_tx, server_task)
    }

    async fn connect_client(endpoint: String) -> OperationServiceClient<tonic::transport::Channel> {
        for _ in 0..50 {
            match OperationServiceClient::connect(endpoint.clone()).await {
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
            start_server(OperationServiceImpl::new(test_dependencies())).await;
        let mut client = connect_client(endpoint).await;

        let create_branch_error = client
            .user_create_branch(UserCreateBranchRequest::default())
            .await
            .expect_err("user_create_branch should be unimplemented");
        assert_eq!(create_branch_error.code(), Code::Unimplemented);

        let merge_branch_stream = tokio_stream::iter(vec![UserMergeBranchRequest::default()]);
        let merge_branch_error = client
            .user_merge_branch(merge_branch_stream)
            .await
            .expect_err("user_merge_branch should be unimplemented");
        assert_eq!(merge_branch_error.code(), Code::Unimplemented);

        shutdown_server(shutdown_tx, server_task).await;
    }
}
