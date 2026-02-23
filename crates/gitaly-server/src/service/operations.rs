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

    async fn drain_stream<T>(
        &self,
        mut stream: tonic::Streaming<T>,
    ) -> Result<Vec<T>, Status>
    where
        T: Send + 'static,
    {
        let _ = &self.dependencies;
        let mut messages = Vec::new();
        while let Some(message) = stream
            .message()
            .await
            .map_err(|err| Status::invalid_argument(format!("invalid request stream: {err}")))?
        {
            messages.push(message);
        }

        Ok(messages)
    }
}

fn single_stream_response<T>(response: T) -> Response<ServiceStream<T>>
where
    T: Send + 'static,
{
    Response::new(Box::pin(tokio_stream::iter(vec![Ok(response)])))
}

#[tonic::async_trait]
impl OperationService for OperationServiceImpl {
    type UserMergeBranchStream = ServiceStream<UserMergeBranchResponse>;
    type UserRebaseConfirmableStream = ServiceStream<UserRebaseConfirmableResponse>;

    async fn user_create_branch(
        &self,
        _request: Request<UserCreateBranchRequest>,
    ) -> Result<Response<UserCreateBranchResponse>, Status> {
        Ok(Response::new(UserCreateBranchResponse::default()))
    }

    async fn user_update_branch(
        &self,
        _request: Request<UserUpdateBranchRequest>,
    ) -> Result<Response<UserUpdateBranchResponse>, Status> {
        Ok(Response::new(UserUpdateBranchResponse::default()))
    }

    async fn user_delete_branch(
        &self,
        _request: Request<UserDeleteBranchRequest>,
    ) -> Result<Response<UserDeleteBranchResponse>, Status> {
        Ok(Response::new(UserDeleteBranchResponse::default()))
    }

    async fn user_create_tag(
        &self,
        _request: Request<UserCreateTagRequest>,
    ) -> Result<Response<UserCreateTagResponse>, Status> {
        Ok(Response::new(UserCreateTagResponse::default()))
    }

    async fn user_delete_tag(
        &self,
        _request: Request<UserDeleteTagRequest>,
    ) -> Result<Response<UserDeleteTagResponse>, Status> {
        Ok(Response::new(UserDeleteTagResponse::default()))
    }

    async fn user_merge_to_ref(
        &self,
        _request: Request<UserMergeToRefRequest>,
    ) -> Result<Response<UserMergeToRefResponse>, Status> {
        Ok(Response::new(UserMergeToRefResponse::default()))
    }

    async fn user_rebase_to_ref(
        &self,
        _request: Request<UserRebaseToRefRequest>,
    ) -> Result<Response<UserRebaseToRefResponse>, Status> {
        Ok(Response::new(UserRebaseToRefResponse::default()))
    }

    async fn user_merge_branch(
        &self,
        request: Request<tonic::Streaming<UserMergeBranchRequest>>,
    ) -> Result<Response<Self::UserMergeBranchStream>, Status> {
        self.drain_stream(request.into_inner()).await?;
        Ok(single_stream_response(UserMergeBranchResponse::default()))
    }

    async fn user_ff_branch(
        &self,
        _request: Request<UserFfBranchRequest>,
    ) -> Result<Response<UserFfBranchResponse>, Status> {
        Ok(Response::new(UserFfBranchResponse::default()))
    }

    async fn user_cherry_pick(
        &self,
        _request: Request<UserCherryPickRequest>,
    ) -> Result<Response<UserCherryPickResponse>, Status> {
        Ok(Response::new(UserCherryPickResponse::default()))
    }

    async fn user_commit_files(
        &self,
        request: Request<tonic::Streaming<UserCommitFilesRequest>>,
    ) -> Result<Response<UserCommitFilesResponse>, Status> {
        self.drain_stream(request.into_inner()).await?;
        Ok(Response::new(UserCommitFilesResponse::default()))
    }

    async fn user_rebase_confirmable(
        &self,
        request: Request<tonic::Streaming<UserRebaseConfirmableRequest>>,
    ) -> Result<Response<Self::UserRebaseConfirmableStream>, Status> {
        self.drain_stream(request.into_inner()).await?;
        Ok(single_stream_response(UserRebaseConfirmableResponse::default()))
    }

    async fn user_revert(
        &self,
        _request: Request<UserRevertRequest>,
    ) -> Result<Response<UserRevertResponse>, Status> {
        Ok(Response::new(UserRevertResponse::default()))
    }

    async fn user_squash(
        &self,
        _request: Request<UserSquashRequest>,
    ) -> Result<Response<UserSquashResponse>, Status> {
        Ok(Response::new(UserSquashResponse::default()))
    }

    async fn user_apply_patch(
        &self,
        request: Request<tonic::Streaming<UserApplyPatchRequest>>,
    ) -> Result<Response<UserApplyPatchResponse>, Status> {
        self.drain_stream(request.into_inner()).await?;
        Ok(Response::new(UserApplyPatchResponse::default()))
    }

    async fn user_update_submodule(
        &self,
        _request: Request<UserUpdateSubmoduleRequest>,
    ) -> Result<Response<UserUpdateSubmoduleResponse>, Status> {
        Ok(Response::new(UserUpdateSubmoduleResponse::default()))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use tokio::sync::oneshot;
    use tokio::task::JoinHandle;
    use tokio::time::sleep;
    use tokio_stream::StreamExt;

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
    async fn representative_methods_return_callable_responses() {
        let (endpoint, shutdown_tx, server_task) =
            start_server(OperationServiceImpl::new(test_dependencies())).await;
        let mut client = connect_client(endpoint).await;

        client
            .user_create_branch(UserCreateBranchRequest::default())
            .await
            .expect("user_create_branch should succeed");

        let merge_branch_stream = tokio_stream::iter(vec![UserMergeBranchRequest::default()]);
        let mut merge_branch_response_stream = client
            .user_merge_branch(merge_branch_stream)
            .await
            .expect("user_merge_branch should succeed")
            .into_inner();
        let first = merge_branch_response_stream
            .next()
            .await
            .expect("response should exist")
            .expect("response should succeed");
        let _ = first;

        shutdown_server(shutdown_tx, server_task).await;
    }
}
