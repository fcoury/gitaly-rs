use std::path::{Component, Path, PathBuf};
use std::pin::Pin;
use std::process::Stdio;
use std::sync::Arc;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::process::Command;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::Stream;
use tonic::{Request, Response, Status};

use gitaly_proto::gitaly::ssh_service_server::SshService;
use gitaly_proto::gitaly::*;

use crate::dependencies::Dependencies;

type ServiceStream<T> = Pin<Box<dyn Stream<Item = Result<T, Status>> + Send + 'static>>;
const STREAM_CHUNK_SIZE: usize = 32 * 1024;

#[derive(Debug, Clone)]
pub struct SshServiceImpl {
    dependencies: Arc<Dependencies>,
}

impl SshServiceImpl {
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

fn build_stream_response<T>(response: T) -> Response<ServiceStream<T>>
where
    T: Send + 'static,
{
    Response::new(Box::pin(tokio_stream::iter(vec![Ok(response)])))
}

fn build_channel_stream_response<T>(
    receiver: mpsc::Receiver<Result<T, Status>>,
) -> Response<ServiceStream<T>>
where
    T: Send + 'static,
{
    Response::new(Box::pin(ReceiverStream::new(receiver)))
}

async fn git_output_with_input<I, S>(
    repo_path: &Path,
    args: I,
    stdin_payload: &[u8],
) -> Result<std::process::Output, Status>
where
    I: IntoIterator<Item = S>,
    S: AsRef<str>,
{
    let mut command = Command::new("git");
    command.arg("-C").arg(repo_path);
    for arg in args {
        command.arg(arg.as_ref());
    }
    command
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());

    let mut child = command
        .spawn()
        .map_err(|err| Status::internal(format!("failed to spawn git command: {err}")))?;

    if let Some(mut stdin) = child.stdin.take() {
        stdin
            .write_all(stdin_payload)
            .await
            .map_err(|err| Status::internal(format!("failed to write git stdin: {err}")))?;
    }

    child
        .wait_with_output()
        .await
        .map_err(|err| Status::internal(format!("failed to await git command: {err}")))
}

fn unimplemented_ssh_rpc(method: &'static str) -> Status {
    Status::unimplemented(format!("SshService::{method} is not implemented"))
}

#[tonic::async_trait]
impl SshService for SshServiceImpl {
    type SSHUploadPackStream = ServiceStream<SshUploadPackResponse>;
    type SSHReceivePackStream = ServiceStream<SshReceivePackResponse>;
    type SSHUploadArchiveStream = ServiceStream<SshUploadArchiveResponse>;

    async fn ssh_upload_pack_with_sidechannel(
        &self,
        _request: Request<SshUploadPackWithSidechannelRequest>,
    ) -> Result<Response<SshUploadPackWithSidechannelResponse>, Status> {
        Err(unimplemented_ssh_rpc("ssh_upload_pack_with_sidechannel"))
    }

    async fn ssh_upload_pack(
        &self,
        request: Request<tonic::Streaming<SshUploadPackRequest>>,
    ) -> Result<Response<Self::SSHUploadPackStream>, Status> {
        let mut stream = request.into_inner();
        let mut first_message = stream
            .message()
            .await
            .map_err(|err| Status::invalid_argument(format!("invalid upload-pack stream: {err}")))?
            .ok_or_else(|| {
                Status::invalid_argument("upload-pack stream must contain at least one message")
            })?;

        let repo_path = self.resolve_repo_path(first_message.repository.take())?;
        let (response_tx, response_rx) = mpsc::channel(16);

        tokio::spawn(async move {
            let task_result: Result<(), Status> = async {
                let mut command = Command::new("git");
                command
                    .arg("-C")
                    .arg(&repo_path)
                    .arg("upload-pack")
                    .arg(".")
                    .stdin(Stdio::piped())
                    .stdout(Stdio::piped())
                    .stderr(Stdio::piped());

                let mut child = command.spawn().map_err(|err| {
                    Status::internal(format!("failed to spawn git upload-pack: {err}"))
                })?;

                let mut child_stdin = child
                    .stdin
                    .take()
                    .ok_or_else(|| Status::internal("git upload-pack stdin is unavailable"))?;
                let mut child_stdout = child
                    .stdout
                    .take()
                    .ok_or_else(|| Status::internal("git upload-pack stdout is unavailable"))?;
                let mut child_stderr = child
                    .stderr
                    .take()
                    .ok_or_else(|| Status::internal("git upload-pack stderr is unavailable"))?;

                let initial_stdin = first_message.stdin;
                let stdin_task = tokio::spawn(async move {
                    if !initial_stdin.is_empty() {
                        child_stdin.write_all(&initial_stdin).await.map_err(|err| {
                            Status::internal(format!("failed writing upload-pack stdin: {err}"))
                        })?;
                    }

                    while let Some(message) = stream.message().await.map_err(|err| {
                        Status::invalid_argument(format!("invalid upload-pack stream: {err}"))
                    })? {
                        if !message.stdin.is_empty() {
                            child_stdin.write_all(&message.stdin).await.map_err(|err| {
                                Status::internal(format!(
                                    "failed writing upload-pack stdin chunk: {err}"
                                ))
                            })?;
                        }
                    }

                    Ok::<(), Status>(())
                });

                let stdout_tx = response_tx.clone();
                let stdout_task = tokio::spawn(async move {
                    let mut buffer = vec![0_u8; STREAM_CHUNK_SIZE];
                    loop {
                        let read = child_stdout.read(&mut buffer).await.map_err(|err| {
                            Status::internal(format!("failed reading upload-pack stdout: {err}"))
                        })?;
                        if read == 0 {
                            break;
                        }
                        if stdout_tx
                            .send(Ok(SshUploadPackResponse {
                                stdout: buffer[..read].to_vec(),
                                ..SshUploadPackResponse::default()
                            }))
                            .await
                            .is_err()
                        {
                            break;
                        }
                    }
                    Ok::<(), Status>(())
                });

                let stderr_tx = response_tx.clone();
                let stderr_task = tokio::spawn(async move {
                    let mut buffer = vec![0_u8; STREAM_CHUNK_SIZE];
                    loop {
                        let read = child_stderr.read(&mut buffer).await.map_err(|err| {
                            Status::internal(format!("failed reading upload-pack stderr: {err}"))
                        })?;
                        if read == 0 {
                            break;
                        }
                        if stderr_tx
                            .send(Ok(SshUploadPackResponse {
                                stderr: buffer[..read].to_vec(),
                                ..SshUploadPackResponse::default()
                            }))
                            .await
                            .is_err()
                        {
                            break;
                        }
                    }
                    Ok::<(), Status>(())
                });

                stdin_task.await.map_err(|err| {
                    Status::internal(format!("upload-pack stdin task failed: {err}"))
                })??;
                stdout_task.await.map_err(|err| {
                    Status::internal(format!("upload-pack stdout task failed: {err}"))
                })??;
                stderr_task.await.map_err(|err| {
                    Status::internal(format!("upload-pack stderr task failed: {err}"))
                })??;

                let status = child.wait().await.map_err(|err| {
                    Status::internal(format!("failed waiting for upload-pack: {err}"))
                })?;

                let _ = response_tx
                    .send(Ok(SshUploadPackResponse {
                        exit_status: Some(ExitStatus {
                            value: status.code().unwrap_or(1),
                        }),
                        ..SshUploadPackResponse::default()
                    }))
                    .await;

                Ok(())
            }
            .await;

            if let Err(status) = task_result {
                let _ = response_tx.send(Err(status)).await;
            }
        });

        Ok(build_channel_stream_response(response_rx))
    }

    async fn ssh_receive_pack(
        &self,
        request: Request<tonic::Streaming<SshReceivePackRequest>>,
    ) -> Result<Response<Self::SSHReceivePackStream>, Status> {
        let mut stream = request.into_inner();
        let mut first_message = stream
            .message()
            .await
            .map_err(|err| Status::invalid_argument(format!("invalid receive-pack stream: {err}")))?
            .ok_or_else(|| {
                Status::invalid_argument("receive-pack stream must contain at least one message")
            })?;

        let repo_path = self.resolve_repo_path(first_message.repository.take())?;
        let (response_tx, response_rx) = mpsc::channel(16);

        tokio::spawn(async move {
            let task_result: Result<(), Status> = async {
                let mut command = Command::new("git");
                command
                    .arg("-C")
                    .arg(&repo_path)
                    .arg("receive-pack")
                    .arg(".")
                    .stdin(Stdio::piped())
                    .stdout(Stdio::piped())
                    .stderr(Stdio::piped());

                let mut child = command.spawn().map_err(|err| {
                    Status::internal(format!("failed to spawn git receive-pack: {err}"))
                })?;

                let mut child_stdin = child
                    .stdin
                    .take()
                    .ok_or_else(|| Status::internal("git receive-pack stdin is unavailable"))?;
                let mut child_stdout = child
                    .stdout
                    .take()
                    .ok_or_else(|| Status::internal("git receive-pack stdout is unavailable"))?;
                let mut child_stderr = child
                    .stderr
                    .take()
                    .ok_or_else(|| Status::internal("git receive-pack stderr is unavailable"))?;

                let initial_stdin = first_message.stdin;
                let stdin_task = tokio::spawn(async move {
                    if !initial_stdin.is_empty() {
                        child_stdin.write_all(&initial_stdin).await.map_err(|err| {
                            Status::internal(format!("failed writing receive-pack stdin: {err}"))
                        })?;
                    }

                    while let Some(message) = stream.message().await.map_err(|err| {
                        Status::invalid_argument(format!("invalid receive-pack stream: {err}"))
                    })? {
                        if !message.stdin.is_empty() {
                            child_stdin.write_all(&message.stdin).await.map_err(|err| {
                                Status::internal(format!(
                                    "failed writing receive-pack stdin chunk: {err}"
                                ))
                            })?;
                        }
                    }

                    Ok::<(), Status>(())
                });

                let stdout_tx = response_tx.clone();
                let stdout_task = tokio::spawn(async move {
                    let mut buffer = vec![0_u8; STREAM_CHUNK_SIZE];
                    loop {
                        let read = child_stdout.read(&mut buffer).await.map_err(|err| {
                            Status::internal(format!("failed reading receive-pack stdout: {err}"))
                        })?;
                        if read == 0 {
                            break;
                        }
                        if stdout_tx
                            .send(Ok(SshReceivePackResponse {
                                stdout: buffer[..read].to_vec(),
                                ..SshReceivePackResponse::default()
                            }))
                            .await
                            .is_err()
                        {
                            break;
                        }
                    }
                    Ok::<(), Status>(())
                });

                let stderr_tx = response_tx.clone();
                let stderr_task = tokio::spawn(async move {
                    let mut buffer = vec![0_u8; STREAM_CHUNK_SIZE];
                    loop {
                        let read = child_stderr.read(&mut buffer).await.map_err(|err| {
                            Status::internal(format!("failed reading receive-pack stderr: {err}"))
                        })?;
                        if read == 0 {
                            break;
                        }
                        if stderr_tx
                            .send(Ok(SshReceivePackResponse {
                                stderr: buffer[..read].to_vec(),
                                ..SshReceivePackResponse::default()
                            }))
                            .await
                            .is_err()
                        {
                            break;
                        }
                    }
                    Ok::<(), Status>(())
                });

                stdin_task.await.map_err(|err| {
                    Status::internal(format!("receive-pack stdin task failed: {err}"))
                })??;
                stdout_task.await.map_err(|err| {
                    Status::internal(format!("receive-pack stdout task failed: {err}"))
                })??;
                stderr_task.await.map_err(|err| {
                    Status::internal(format!("receive-pack stderr task failed: {err}"))
                })??;

                let status = child.wait().await.map_err(|err| {
                    Status::internal(format!("failed waiting for receive-pack: {err}"))
                })?;

                let _ = response_tx
                    .send(Ok(SshReceivePackResponse {
                        exit_status: Some(ExitStatus {
                            value: status.code().unwrap_or(1),
                        }),
                        ..SshReceivePackResponse::default()
                    }))
                    .await;

                Ok(())
            }
            .await;

            if let Err(status) = task_result {
                let _ = response_tx.send(Err(status)).await;
            }
        });

        Ok(build_channel_stream_response(response_rx))
    }

    async fn ssh_upload_archive(
        &self,
        request: Request<tonic::Streaming<SshUploadArchiveRequest>>,
    ) -> Result<Response<Self::SSHUploadArchiveStream>, Status> {
        let mut stream = request.into_inner();
        let mut repository_descriptor: Option<Repository> = None;
        let mut stdin_payload = Vec::new();

        while let Some(message) = stream
            .message()
            .await
            .map_err(|err| Status::invalid_argument(format!("invalid upload stream: {err}")))?
        {
            if repository_descriptor.is_none() {
                repository_descriptor = message.repository;
            }
            if !message.stdin.is_empty() {
                stdin_payload.extend_from_slice(&message.stdin);
            }
        }

        let repo_path = self.resolve_repo_path(repository_descriptor)?;
        let output =
            git_output_with_input(&repo_path, ["upload-archive", "."], &stdin_payload).await?;

        Ok(build_stream_response(SshUploadArchiveResponse {
            stdout: output.stdout,
            stderr: output.stderr,
            exit_status: Some(ExitStatus {
                value: output.status.code().unwrap_or(1),
            }),
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
    use tonic::Code;

    use gitaly_proto::gitaly::ssh_service_client::SshServiceClient;
    use gitaly_proto::gitaly::ssh_service_server::SshServiceServer;
    use gitaly_proto::gitaly::{
        Repository, SshReceivePackRequest, SshUploadArchiveRequest,
        SshUploadPackWithSidechannelRequest,
    };

    use crate::dependencies::Dependencies;

    use super::SshServiceImpl;

    async fn start_server(
        service: SshServiceImpl,
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
                .add_service(SshServiceServer::new(service))
                .serve_with_shutdown(server_addr, async {
                    let _ = shutdown_rx.await;
                })
                .await
                .expect("server should run");
        });

        (format!("http://{server_addr}"), shutdown_tx, server_task)
    }

    async fn connect_client(endpoint: String) -> SshServiceClient<tonic::transport::Channel> {
        for _ in 0..50 {
            match SshServiceClient::connect(endpoint.clone()).await {
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
    async fn unimplemented_methods_return_unimplemented_status() {
        let (endpoint, shutdown_tx, server_task) =
            start_server(SshServiceImpl::new(test_dependencies())).await;
        let mut client = connect_client(endpoint).await;

        let upload_pack_error = client
            .ssh_upload_pack_with_sidechannel(SshUploadPackWithSidechannelRequest::default())
            .await
            .expect_err("ssh_upload_pack_with_sidechannel should be unimplemented");
        assert_eq!(upload_pack_error.code(), Code::Unimplemented);

        shutdown_server(shutdown_tx, server_task).await;
    }

    #[tokio::test]
    async fn ssh_receive_pack_returns_response_item_with_exit_status() {
        let (endpoint, shutdown_tx, server_task) =
            start_server(SshServiceImpl::new(test_dependencies())).await;
        let mut client = connect_client(endpoint).await;

        let request_stream = tokio_stream::iter(vec![
            SshReceivePackRequest {
                repository: Some(Repository {
                    storage_name: "default".to_string(),
                    relative_path: "group/project.git".to_string(),
                    ..Repository::default()
                }),
                stdin: Vec::new(),
                ..SshReceivePackRequest::default()
            },
            SshReceivePackRequest {
                repository: None,
                stdin: b"ignored".to_vec(),
                ..SshReceivePackRequest::default()
            },
        ]);

        let mut response_stream = client
            .ssh_receive_pack(request_stream)
            .await
            .expect("ssh_receive_pack should succeed")
            .into_inner();

        let mut saw_exit_status = false;
        while let Some(item) = response_stream
            .message()
            .await
            .expect("stream read should succeed")
        {
            if item.exit_status.is_some() {
                saw_exit_status = true;
            }
        }
        assert!(saw_exit_status, "expected at least one response with exit_status");

        shutdown_server(shutdown_tx, server_task).await;
    }

    #[tokio::test]
    async fn ssh_upload_archive_returns_response_item_with_exit_status() {
        let (endpoint, shutdown_tx, server_task) =
            start_server(SshServiceImpl::new(test_dependencies())).await;
        let mut client = connect_client(endpoint).await;

        let request_stream = tokio_stream::iter(vec![SshUploadArchiveRequest {
            repository: Some(Repository {
                storage_name: "default".to_string(),
                relative_path: "group/project.git".to_string(),
                ..Repository::default()
            }),
            stdin: b"ignored".to_vec(),
        }]);

        let mut response_stream = client
            .ssh_upload_archive(request_stream)
            .await
            .expect("ssh_upload_archive should succeed")
            .into_inner();

        let first_item = response_stream
            .message()
            .await
            .expect("stream read should succeed")
            .expect("expected first response item");
        assert!(first_item.exit_status.is_some());

        let second_item = response_stream
            .message()
            .await
            .expect("stream read should succeed");
        assert!(second_item.is_none());

        shutdown_server(shutdown_tx, server_task).await;
    }

    #[tokio::test]
    async fn ssh_receive_pack_requires_repository() {
        let (endpoint, shutdown_tx, server_task) =
            start_server(SshServiceImpl::new(test_dependencies())).await;
        let mut client = connect_client(endpoint).await;

        let receive_pack_stream = tokio_stream::iter(vec![SshReceivePackRequest::default()]);
        let status = client
            .ssh_receive_pack(receive_pack_stream)
            .await
            .expect_err("missing repository should fail");
        assert_eq!(status.code(), Code::InvalidArgument);

        shutdown_server(shutdown_tx, server_task).await;
    }
}
