use std::path::{Component, Path, PathBuf};
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

fn success_exit_status() -> Option<ExitStatus> {
    Some(ExitStatus { value: 0 })
}

fn single_stream_response<T>(response: T) -> Response<ServiceStream<T>>
where
    T: Send + 'static,
{
    Response::new(Box::pin(tokio_stream::iter(vec![Ok(response)])))
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
        request: Request<tonic::Streaming<PreReceiveHookRequest>>,
    ) -> Result<Response<Self::PreReceiveHookStream>, Status> {
        let mut stream = request.into_inner();
        let mut repository_seen = false;
        while let Some(message) = stream
            .message()
            .await
            .map_err(|err| Status::invalid_argument(format!("invalid request stream: {err}")))?
        {
            if !repository_seen {
                self.resolve_repo_path(message.repository)?;
                repository_seen = true;
            }
        }

        if !repository_seen {
            return Err(Status::invalid_argument(
                "request stream must contain at least one message",
            ));
        }

        Ok(single_stream_response(PreReceiveHookResponse {
            stdout: Vec::new(),
            stderr: Vec::new(),
            exit_status: success_exit_status(),
        }))
    }

    async fn post_receive_hook(
        &self,
        request: Request<tonic::Streaming<PostReceiveHookRequest>>,
    ) -> Result<Response<Self::PostReceiveHookStream>, Status> {
        let mut stream = request.into_inner();
        let mut repository_seen = false;
        while let Some(message) = stream
            .message()
            .await
            .map_err(|err| Status::invalid_argument(format!("invalid request stream: {err}")))?
        {
            if !repository_seen {
                self.resolve_repo_path(message.repository)?;
                repository_seen = true;
            }
        }

        if !repository_seen {
            return Err(Status::invalid_argument(
                "request stream must contain at least one message",
            ));
        }

        Ok(single_stream_response(PostReceiveHookResponse {
            stdout: Vec::new(),
            stderr: Vec::new(),
            exit_status: success_exit_status(),
        }))
    }

    async fn update_hook(
        &self,
        request: Request<UpdateHookRequest>,
    ) -> Result<Response<Self::UpdateHookStream>, Status> {
        let request = request.into_inner();
        self.resolve_repo_path(request.repository)?;

        Ok(single_stream_response(UpdateHookResponse {
            stdout: Vec::new(),
            stderr: Vec::new(),
            exit_status: success_exit_status(),
        }))
    }

    async fn reference_transaction_hook(
        &self,
        request: Request<tonic::Streaming<ReferenceTransactionHookRequest>>,
    ) -> Result<Response<Self::ReferenceTransactionHookStream>, Status> {
        let mut stream = request.into_inner();
        let mut repository_seen = false;
        while let Some(message) = stream
            .message()
            .await
            .map_err(|err| Status::invalid_argument(format!("invalid request stream: {err}")))?
        {
            if !repository_seen {
                self.resolve_repo_path(message.repository)?;
                repository_seen = true;
            }
        }

        if !repository_seen {
            return Err(Status::invalid_argument(
                "request stream must contain at least one message",
            ));
        }

        Ok(single_stream_response(ReferenceTransactionHookResponse {
            stdout: Vec::new(),
            stderr: Vec::new(),
            exit_status: success_exit_status(),
        }))
    }

    async fn pack_objects_hook_with_sidechannel(
        &self,
        request: Request<PackObjectsHookWithSidechannelRequest>,
    ) -> Result<Response<PackObjectsHookWithSidechannelResponse>, Status> {
        let request = request.into_inner();
        self.resolve_repo_path(request.repository)?;

        Ok(Response::new(PackObjectsHookWithSidechannelResponse {}))
    }

    async fn proc_receive_hook(
        &self,
        request: Request<tonic::Streaming<ProcReceiveHookRequest>>,
    ) -> Result<Response<Self::ProcReceiveHookStream>, Status> {
        let mut stream = request.into_inner();
        let mut repository_seen = false;
        while let Some(message) = stream
            .message()
            .await
            .map_err(|err| Status::invalid_argument(format!("invalid request stream: {err}")))?
        {
            if !repository_seen {
                self.resolve_repo_path(message.repository)?;
                repository_seen = true;
            }
        }

        if !repository_seen {
            return Err(Status::invalid_argument(
                "request stream must contain at least one message",
            ));
        }

        Ok(single_stream_response(ProcReceiveHookResponse {
            stdout: Vec::new(),
            stderr: Vec::new(),
            exit_status: success_exit_status(),
        }))
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

    use gitaly_proto::gitaly::hook_service_client::HookServiceClient;
    use gitaly_proto::gitaly::hook_service_server::HookServiceServer;
    use gitaly_proto::gitaly::{
        PackObjectsHookWithSidechannelRequest, PreReceiveHookRequest, Repository, UpdateHookRequest,
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
        let mut storage_paths = HashMap::new();
        storage_paths.insert("default".to_string(), std::env::temp_dir());
        Arc::new(Dependencies::default().with_storage_paths(storage_paths))
    }

    #[tokio::test]
    async fn streaming_and_unary_hook_methods_return_success_exit_status() {
        let (endpoint, shutdown_tx, server_task) =
            start_server(HookServiceImpl::new(test_dependencies())).await;
        let mut client = connect_client(endpoint).await;

        let pre_receive_stream = tokio_stream::iter(vec![PreReceiveHookRequest {
            repository: Some(Repository {
                storage_name: "default".to_string(),
                relative_path: "group/project.git".to_string(),
                ..Repository::default()
            }),
            ..PreReceiveHookRequest::default()
        }]);
        let mut pre_receive_response_stream = client
            .pre_receive_hook(pre_receive_stream)
            .await
            .expect("pre_receive_hook should succeed")
            .into_inner();
        let pre_receive = pre_receive_response_stream
            .next()
            .await
            .expect("response should exist")
            .expect("response should succeed");
        assert_eq!(pre_receive.exit_status.expect("exit status should exist").value, 0);

        let mut update_hook_response_stream = client
            .update_hook(UpdateHookRequest {
                repository: Some(Repository {
                    storage_name: "default".to_string(),
                    relative_path: "group/project.git".to_string(),
                    ..Repository::default()
                }),
                ..UpdateHookRequest::default()
            })
            .await
            .expect("update_hook should succeed")
            .into_inner();
        let update_hook = update_hook_response_stream
            .next()
            .await
            .expect("response should exist")
            .expect("response should succeed");
        assert_eq!(update_hook.exit_status.expect("exit status should exist").value, 0);

        client
            .pack_objects_hook_with_sidechannel(PackObjectsHookWithSidechannelRequest {
                repository: Some(Repository {
                    storage_name: "default".to_string(),
                    relative_path: "group/project.git".to_string(),
                    ..Repository::default()
                }),
                ..PackObjectsHookWithSidechannelRequest::default()
            })
            .await
            .expect("pack_objects_hook_with_sidechannel should succeed");

        shutdown_server(shutdown_tx, server_task).await;
    }
}
