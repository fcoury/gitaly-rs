use std::path::{Component, Path, PathBuf};
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

    fn resolve_repo_path(&self, repository: Option<Repository>) -> Result<PathBuf, Status> {
        let repository =
            repository.ok_or_else(|| Status::invalid_argument("repository is required"))?;

        if repository.storage_name.trim().is_empty() {
            return Err(Status::invalid_argument(
                "repository.storage_name is required",
            ));
        }

        if repository.relative_path.trim().is_empty() {
            return Err(Status::invalid_argument(
                "repository.relative_path is required",
            ));
        }

        validate_relative_path(&repository.relative_path)?;

        let storage_root = self
            .dependencies
            .storage_paths
            .get(&repository.storage_name)
            .ok_or_else(|| {
                Status::not_found(format!(
                    "storage `{}` is not configured",
                    repository.storage_name
                ))
            })?;

        Ok(storage_root.join(repository.relative_path))
    }
}

fn validate_relative_path(relative_path: &str) -> Result<(), Status> {
    let path = Path::new(relative_path);
    if path.is_absolute() {
        return Err(Status::invalid_argument(
            "repository.relative_path must be relative",
        ));
    }

    for component in path.components() {
        match component {
            Component::Normal(_) => {}
            _ => {
                return Err(Status::invalid_argument(
                    "repository.relative_path contains disallowed path components",
                ))
            }
        }
    }

    Ok(())
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
        request: Request<UserCreateBranchRequest>,
    ) -> Result<Response<UserCreateBranchResponse>, Status> {
        self.resolve_repo_path(request.into_inner().repository)?;
        Ok(Response::new(UserCreateBranchResponse::default()))
    }

    async fn user_update_branch(
        &self,
        request: Request<UserUpdateBranchRequest>,
    ) -> Result<Response<UserUpdateBranchResponse>, Status> {
        self.resolve_repo_path(request.into_inner().repository)?;
        Ok(Response::new(UserUpdateBranchResponse::default()))
    }

    async fn user_delete_branch(
        &self,
        request: Request<UserDeleteBranchRequest>,
    ) -> Result<Response<UserDeleteBranchResponse>, Status> {
        self.resolve_repo_path(request.into_inner().repository)?;
        Ok(Response::new(UserDeleteBranchResponse::default()))
    }

    async fn user_create_tag(
        &self,
        request: Request<UserCreateTagRequest>,
    ) -> Result<Response<UserCreateTagResponse>, Status> {
        self.resolve_repo_path(request.into_inner().repository)?;
        Ok(Response::new(UserCreateTagResponse::default()))
    }

    async fn user_delete_tag(
        &self,
        request: Request<UserDeleteTagRequest>,
    ) -> Result<Response<UserDeleteTagResponse>, Status> {
        self.resolve_repo_path(request.into_inner().repository)?;
        Ok(Response::new(UserDeleteTagResponse::default()))
    }

    async fn user_merge_to_ref(
        &self,
        request: Request<UserMergeToRefRequest>,
    ) -> Result<Response<UserMergeToRefResponse>, Status> {
        self.resolve_repo_path(request.into_inner().repository)?;
        Ok(Response::new(UserMergeToRefResponse::default()))
    }

    async fn user_rebase_to_ref(
        &self,
        request: Request<UserRebaseToRefRequest>,
    ) -> Result<Response<UserRebaseToRefResponse>, Status> {
        self.resolve_repo_path(request.into_inner().repository)?;
        Ok(Response::new(UserRebaseToRefResponse::default()))
    }

    async fn user_merge_branch(
        &self,
        request: Request<tonic::Streaming<UserMergeBranchRequest>>,
    ) -> Result<Response<Self::UserMergeBranchStream>, Status> {
        let mut stream = request.into_inner();
        let mut first_message = stream
            .message()
            .await
            .map_err(|err| Status::invalid_argument(format!("invalid request stream: {err}")))?
            .ok_or_else(|| {
                Status::invalid_argument("request stream must contain at least one message")
            })?;
        self.resolve_repo_path(first_message.repository.take())?;

        while stream
            .message()
            .await
            .map_err(|err| Status::invalid_argument(format!("invalid request stream: {err}")))?
            .is_some()
        {}

        Ok(single_stream_response(UserMergeBranchResponse::default()))
    }

    async fn user_ff_branch(
        &self,
        request: Request<UserFfBranchRequest>,
    ) -> Result<Response<UserFfBranchResponse>, Status> {
        self.resolve_repo_path(request.into_inner().repository)?;
        Ok(Response::new(UserFfBranchResponse::default()))
    }

    async fn user_cherry_pick(
        &self,
        request: Request<UserCherryPickRequest>,
    ) -> Result<Response<UserCherryPickResponse>, Status> {
        self.resolve_repo_path(request.into_inner().repository)?;
        Ok(Response::new(UserCherryPickResponse::default()))
    }

    async fn user_commit_files(
        &self,
        request: Request<tonic::Streaming<UserCommitFilesRequest>>,
    ) -> Result<Response<UserCommitFilesResponse>, Status> {
        let mut stream = request.into_inner();
        let first_message = stream
            .message()
            .await
            .map_err(|err| Status::invalid_argument(format!("invalid request stream: {err}")))?
            .ok_or_else(|| {
                Status::invalid_argument("request stream must contain at least one message")
            })?;

        let repository = match first_message.user_commit_files_request_payload {
            Some(user_commit_files_request::UserCommitFilesRequestPayload::Header(header)) => {
                header.repository
            }
            Some(_) => {
                return Err(Status::invalid_argument(
                    "first request must contain a commit files header payload",
                ))
            }
            None => {
                return Err(Status::invalid_argument(
                    "first request must contain a commit files header payload",
                ))
            }
        };
        self.resolve_repo_path(repository)?;

        while stream
            .message()
            .await
            .map_err(|err| Status::invalid_argument(format!("invalid request stream: {err}")))?
            .is_some()
        {}

        Ok(Response::new(UserCommitFilesResponse::default()))
    }

    async fn user_rebase_confirmable(
        &self,
        request: Request<tonic::Streaming<UserRebaseConfirmableRequest>>,
    ) -> Result<Response<Self::UserRebaseConfirmableStream>, Status> {
        let mut stream = request.into_inner();
        let first_message = stream
            .message()
            .await
            .map_err(|err| Status::invalid_argument(format!("invalid request stream: {err}")))?
            .ok_or_else(|| {
                Status::invalid_argument("request stream must contain at least one message")
            })?;

        let repository = match first_message.user_rebase_confirmable_request_payload {
            Some(user_rebase_confirmable_request::UserRebaseConfirmableRequestPayload::Header(
                header,
            )) => header.repository,
            Some(_) => {
                return Err(Status::invalid_argument(
                    "first request must contain a rebase confirmable header payload",
                ))
            }
            None => {
                return Err(Status::invalid_argument(
                    "first request must contain a rebase confirmable header payload",
                ))
            }
        };
        self.resolve_repo_path(repository)?;

        while stream
            .message()
            .await
            .map_err(|err| Status::invalid_argument(format!("invalid request stream: {err}")))?
            .is_some()
        {}

        Ok(single_stream_response(
            UserRebaseConfirmableResponse::default(),
        ))
    }

    async fn user_revert(
        &self,
        request: Request<UserRevertRequest>,
    ) -> Result<Response<UserRevertResponse>, Status> {
        self.resolve_repo_path(request.into_inner().repository)?;
        Ok(Response::new(UserRevertResponse::default()))
    }

    async fn user_squash(
        &self,
        request: Request<UserSquashRequest>,
    ) -> Result<Response<UserSquashResponse>, Status> {
        self.resolve_repo_path(request.into_inner().repository)?;
        Ok(Response::new(UserSquashResponse::default()))
    }

    async fn user_apply_patch(
        &self,
        request: Request<tonic::Streaming<UserApplyPatchRequest>>,
    ) -> Result<Response<UserApplyPatchResponse>, Status> {
        let mut stream = request.into_inner();
        let first_message = stream
            .message()
            .await
            .map_err(|err| Status::invalid_argument(format!("invalid request stream: {err}")))?
            .ok_or_else(|| {
                Status::invalid_argument("request stream must contain at least one message")
            })?;

        let repository = match first_message.user_apply_patch_request_payload {
            Some(user_apply_patch_request::UserApplyPatchRequestPayload::Header(header)) => {
                header.repository
            }
            Some(_) => {
                return Err(Status::invalid_argument(
                    "first request must contain an apply patch header payload",
                ))
            }
            None => {
                return Err(Status::invalid_argument(
                    "first request must contain an apply patch header payload",
                ))
            }
        };
        self.resolve_repo_path(repository)?;

        while stream
            .message()
            .await
            .map_err(|err| Status::invalid_argument(format!("invalid request stream: {err}")))?
            .is_some()
        {}

        Ok(Response::new(UserApplyPatchResponse::default()))
    }

    async fn user_update_submodule(
        &self,
        request: Request<UserUpdateSubmoduleRequest>,
    ) -> Result<Response<UserUpdateSubmoduleResponse>, Status> {
        self.resolve_repo_path(request.into_inner().repository)?;
        Ok(Response::new(UserUpdateSubmoduleResponse::default()))
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;
    use std::time::Duration;

    use tokio::sync::oneshot;
    use tokio::task::JoinHandle;
    use tokio::time::sleep;
    use tokio_stream::StreamExt;
    use tonic::Code;

    use gitaly_proto::gitaly::operation_service_client::OperationServiceClient;
    use gitaly_proto::gitaly::operation_service_server::OperationServiceServer;
    use gitaly_proto::gitaly::{
        Repository, UserApplyPatchRequest, UserCommitFilesRequest, UserCreateBranchRequest,
        UserMergeBranchRequest,
    };

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
        let mut storage_paths = HashMap::new();
        storage_paths.insert("default".to_string(), std::env::temp_dir());
        Arc::new(Dependencies::default().with_storage_paths(storage_paths))
    }

    #[tokio::test]
    async fn representative_methods_return_callable_responses() {
        let (endpoint, shutdown_tx, server_task) =
            start_server(OperationServiceImpl::new(test_dependencies())).await;
        let mut client = connect_client(endpoint).await;

        client
            .user_create_branch(UserCreateBranchRequest {
                repository: Some(Repository {
                    storage_name: "default".to_string(),
                    relative_path: "group/project.git".to_string(),
                    ..Repository::default()
                }),
                ..UserCreateBranchRequest::default()
            })
            .await
            .expect("user_create_branch should succeed");

        let merge_branch_stream = tokio_stream::iter(vec![UserMergeBranchRequest {
            repository: Some(Repository {
                storage_name: "default".to_string(),
                relative_path: "group/project.git".to_string(),
                ..Repository::default()
            }),
            ..UserMergeBranchRequest::default()
        }]);
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

    #[tokio::test]
    async fn unary_methods_require_repository() {
        let (endpoint, shutdown_tx, server_task) =
            start_server(OperationServiceImpl::new(test_dependencies())).await;
        let mut client = connect_client(endpoint).await;

        let status = client
            .user_create_branch(UserCreateBranchRequest::default())
            .await
            .expect_err("missing repository should fail");
        assert_eq!(status.code(), Code::InvalidArgument);

        shutdown_server(shutdown_tx, server_task).await;
    }

    #[tokio::test]
    async fn streaming_methods_require_header_payload_first() {
        let (endpoint, shutdown_tx, server_task) =
            start_server(OperationServiceImpl::new(test_dependencies())).await;
        let mut client = connect_client(endpoint).await;

        let commit_files_stream = tokio_stream::iter(vec![UserCommitFilesRequest::default()]);
        let commit_files_status = client
            .user_commit_files(commit_files_stream)
            .await
            .expect_err("missing commit_files header should fail");
        assert_eq!(commit_files_status.code(), Code::InvalidArgument);

        let apply_patch_stream = tokio_stream::iter(vec![UserApplyPatchRequest::default()]);
        let apply_patch_status = client
            .user_apply_patch(apply_patch_stream)
            .await
            .expect_err("missing apply_patch header should fail");
        assert_eq!(apply_patch_status.code(), Code::InvalidArgument);

        shutdown_server(shutdown_tx, server_task).await;
    }
}
