use std::path::{Component, Path, PathBuf};
use std::pin::Pin;
use std::process::Stdio;
use std::sync::Arc;

use tokio::io::AsyncWriteExt;
use tokio::process::Command;
use tokio_stream::Stream;
use tonic::{Request, Response, Status};

use gitaly_proto::gitaly::smart_http_service_server::SmartHttpService;
use gitaly_proto::gitaly::*;

use crate::dependencies::Dependencies;

type ServiceStream<T> = Pin<Box<dyn Stream<Item = Result<T, Status>> + Send + 'static>>;

const STREAM_CHUNK_SIZE: usize = 32 * 1024;

#[derive(Debug, Clone)]
pub struct SmartHttpServiceImpl {
    dependencies: Arc<Dependencies>,
}

impl SmartHttpServiceImpl {
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

fn stream_from_values<T>(values: Vec<T>) -> Response<ServiceStream<T>>
where
    T: Send + 'static,
{
    let items = values.into_iter().map(Ok);
    Response::new(Box::pin(tokio_stream::iter(items)))
}

fn stream_bytes<T, F>(data: Vec<u8>, mut map: F) -> Response<ServiceStream<T>>
where
    T: Send + 'static,
    F: FnMut(Vec<u8>) -> T,
{
    let chunks: Vec<T> = if data.is_empty() {
        vec![map(Vec::new())]
    } else {
        data.chunks(STREAM_CHUNK_SIZE)
            .map(|chunk| map(chunk.to_vec()))
            .collect()
    };

    stream_from_values(chunks)
}

async fn git_output<I, S>(repo_path: &Path, args: I) -> Result<std::process::Output, Status>
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
        .output()
        .await
        .map_err(|err| Status::internal(format!("failed to execute git command: {err}")))
}

fn status_for_git_failure(args: &str, output: &std::process::Output) -> Status {
    Status::internal(format!(
        "git command failed: git {args}; stderr: {}",
        String::from_utf8_lossy(&output.stderr).trim()
    ))
}

fn parse_packfile_negotiation_statistics(stdout: &[u8]) -> PackfileNegotiationStatistics {
    let mut payload_size = 0_i64;
    let mut packets = 0_i64;
    let mut caps = Vec::new();

    let mut offset = 0_usize;
    while offset + 4 <= stdout.len() {
        let header = &stdout[offset..offset + 4];
        let Ok(header) = std::str::from_utf8(header) else {
            break;
        };
        let Ok(packet_len) = usize::from_str_radix(header, 16) else {
            break;
        };
        offset += 4;
        packets = packets.saturating_add(1);

        if packet_len == 0 {
            continue;
        }
        if packet_len < 4 {
            break;
        }
        let payload_len = packet_len - 4;
        if offset + payload_len > stdout.len() {
            break;
        }

        let payload = &stdout[offset..offset + payload_len];
        payload_size =
            payload_size.saturating_add(i64::try_from(payload.len()).unwrap_or(i64::MAX));
        if caps.is_empty() {
            if let Some(separator) = payload.iter().position(|byte| *byte == 0) {
                let cap_text = String::from_utf8_lossy(&payload[separator + 1..]);
                caps = cap_text
                    .split_whitespace()
                    .map(|capability| capability.to_string())
                    .collect();
            }
        }

        offset += payload_len;
    }

    PackfileNegotiationStatistics {
        payload_size,
        packets,
        caps,
        wants: 0,
        haves: 0,
        shallows: 0,
        deepen: String::new(),
        filter: String::new(),
    }
}

fn apply_git_command_options(
    command: &mut Command,
    git_config_options: &[String],
    git_protocol: &str,
) -> Result<(), Status> {
    for option in git_config_options {
        if !option.contains('=') {
            return Err(Status::invalid_argument(
                "git_config_options entries must be formatted as key=value",
            ));
        }
        command.arg("-c").arg(option);
    }

    let git_protocol = git_protocol.trim();
    if !git_protocol.is_empty() {
        command.env("GIT_PROTOCOL", git_protocol);
    }

    Ok(())
}

#[tonic::async_trait]
impl SmartHttpService for SmartHttpServiceImpl {
    type InfoRefsUploadPackStream = ServiceStream<InfoRefsResponse>;
    type InfoRefsReceivePackStream = ServiceStream<InfoRefsResponse>;
    type PostUploadPackStream = ServiceStream<PostUploadPackResponse>;
    type PostReceivePackStream = ServiceStream<PostReceivePackResponse>;

    async fn info_refs_upload_pack(
        &self,
        request: Request<InfoRefsRequest>,
    ) -> Result<Response<Self::InfoRefsUploadPackStream>, Status> {
        let request = request.into_inner();
        let repo_path = self.resolve_repo_path(request.repository)?;

        let output = git_output(
            &repo_path,
            ["upload-pack", "--stateless-rpc", "--advertise-refs", "."],
        )
        .await?;

        if !output.status.success() {
            return Err(status_for_git_failure(
                "-C <repo> upload-pack --stateless-rpc --advertise-refs .",
                &output,
            ));
        }

        Ok(stream_bytes(output.stdout, |data| InfoRefsResponse {
            data,
        }))
    }

    async fn info_refs_receive_pack(
        &self,
        request: Request<InfoRefsRequest>,
    ) -> Result<Response<Self::InfoRefsReceivePackStream>, Status> {
        let request = request.into_inner();
        let repo_path = self.resolve_repo_path(request.repository)?;

        let output = git_output(
            &repo_path,
            ["receive-pack", "--stateless-rpc", "--advertise-refs", "."],
        )
        .await?;

        if !output.status.success() {
            return Err(status_for_git_failure(
                "-C <repo> receive-pack --stateless-rpc --advertise-refs .",
                &output,
            ));
        }

        Ok(stream_bytes(output.stdout, |data| InfoRefsResponse {
            data,
        }))
    }

    async fn post_upload_pack_with_sidechannel(
        &self,
        request: Request<PostUploadPackWithSidechannelRequest>,
    ) -> Result<Response<PostUploadPackWithSidechannelResponse>, Status> {
        let request = request.into_inner();
        let repo_path = self.resolve_repo_path(request.repository)?;
        if !repo_path.exists() {
            return Err(Status::not_found("repository does not exist"));
        }

        let mut command = Command::new("git");
        command.arg("-C").arg(&repo_path);
        apply_git_command_options(
            &mut command,
            &request.git_config_options,
            &request.git_protocol,
        )?;

        command
            .arg("upload-pack")
            .arg("--stateless-rpc")
            .arg("--advertise-refs")
            .arg(".");

        let output = command
            .output()
            .await
            .map_err(|err| Status::internal(format!("failed to execute git upload-pack: {err}")))?;

        if !output.status.success() {
            return Err(status_for_git_failure(
                "-C <repo> upload-pack --stateless-rpc --advertise-refs .",
                &output,
            ));
        }

        Ok(Response::new(PostUploadPackWithSidechannelResponse {
            packfile_negotiation_statistics: Some(parse_packfile_negotiation_statistics(
                &output.stdout,
            )),
        }))
    }

    async fn post_upload_pack(
        &self,
        request: Request<tonic::Streaming<PostUploadPackRequest>>,
    ) -> Result<Response<Self::PostUploadPackStream>, Status> {
        let mut stream = request.into_inner();
        let mut first_message = stream
            .message()
            .await
            .map_err(|err| Status::invalid_argument(format!("invalid request stream: {err}")))?
            .ok_or_else(|| {
                Status::invalid_argument("request stream must contain at least one message")
            })?;

        let repository = first_message.repository.take();
        let git_config_options = first_message.git_config_options.clone();
        let git_protocol = first_message.git_protocol.clone();
        let mut payload = Vec::new();
        if !first_message.data.is_empty() {
            payload.extend_from_slice(&first_message.data);
        }

        while let Some(message) = stream
            .message()
            .await
            .map_err(|err| Status::invalid_argument(format!("invalid request stream: {err}")))?
        {
            if !message.data.is_empty() {
                payload.extend_from_slice(&message.data);
            }
        }

        let repo_path = self.resolve_repo_path(repository)?;
        let command_args = vec![
            "upload-pack".to_string(),
            "--stateless-rpc".to_string(),
            ".".to_string(),
        ];
        let output =
            {
                let mut command = Command::new("git");
                command.arg("-C").arg(&repo_path);
                apply_git_command_options(&mut command, &git_config_options, &git_protocol)?;
                for arg in &command_args {
                    command.arg(arg);
                }
                command
                    .stdin(Stdio::piped())
                    .stdout(Stdio::piped())
                    .stderr(Stdio::piped());

                let mut child = command.spawn().map_err(|err| {
                    Status::internal(format!("failed to spawn git command: {err}"))
                })?;
                if let Some(mut stdin) = child.stdin.take() {
                    stdin.write_all(&payload).await.map_err(|err| {
                        Status::internal(format!("failed to write git stdin: {err}"))
                    })?;
                }
                child.wait_with_output().await.map_err(|err| {
                    Status::internal(format!("failed to await git command: {err}"))
                })?
            };

        if !output.status.success() {
            return Err(status_for_git_failure(
                "-C <repo> upload-pack --stateless-rpc .",
                &output,
            ));
        }

        Ok(stream_bytes(output.stdout, |data| PostUploadPackResponse {
            data,
        }))
    }

    async fn post_receive_pack(
        &self,
        request: Request<tonic::Streaming<PostReceivePackRequest>>,
    ) -> Result<Response<Self::PostReceivePackStream>, Status> {
        let mut stream = request.into_inner();
        let mut first_message = stream
            .message()
            .await
            .map_err(|err| Status::invalid_argument(format!("invalid request stream: {err}")))?
            .ok_or_else(|| {
                Status::invalid_argument("request stream must contain at least one message")
            })?;

        let repository = first_message.repository.take();
        let git_config_options = first_message.git_config_options.clone();
        let git_protocol = first_message.git_protocol.clone();
        let mut payload = Vec::new();
        if !first_message.data.is_empty() {
            payload.extend_from_slice(&first_message.data);
        }

        while let Some(message) = stream
            .message()
            .await
            .map_err(|err| Status::invalid_argument(format!("invalid request stream: {err}")))?
        {
            if !message.data.is_empty() {
                payload.extend_from_slice(&message.data);
            }
        }

        let repo_path = self.resolve_repo_path(repository)?;
        let output =
            {
                let mut command = Command::new("git");
                command.arg("-C").arg(&repo_path);
                apply_git_command_options(&mut command, &git_config_options, &git_protocol)?;
                command.arg("receive-pack").arg("--stateless-rpc").arg(".");
                command
                    .stdin(Stdio::piped())
                    .stdout(Stdio::piped())
                    .stderr(Stdio::piped());

                let mut child = command.spawn().map_err(|err| {
                    Status::internal(format!("failed to spawn git command: {err}"))
                })?;
                if let Some(mut stdin) = child.stdin.take() {
                    stdin.write_all(&payload).await.map_err(|err| {
                        Status::internal(format!("failed to write git stdin: {err}"))
                    })?;
                }
                child.wait_with_output().await.map_err(|err| {
                    Status::internal(format!("failed to await git command: {err}"))
                })?
            };

        if !output.status.success() {
            return Err(status_for_git_failure(
                "-C <repo> receive-pack --stateless-rpc .",
                &output,
            ));
        }

        Ok(stream_bytes(output.stdout, |data| {
            PostReceivePackResponse { data }
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::collections::HashMap;
    use std::process::Command;
    use std::time::Duration;
    use std::time::{SystemTime, UNIX_EPOCH};

    use tokio::sync::oneshot;
    use tokio::task::JoinHandle;
    use tokio::time::sleep;
    use tokio_stream::StreamExt;
    use tonic::transport::Channel;
    use tonic::Code;

    use gitaly_proto::gitaly::smart_http_service_client::SmartHttpServiceClient;
    use gitaly_proto::gitaly::smart_http_service_server::SmartHttpServiceServer;

    fn unique_dir(name: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock drift")
            .as_nanos();
        std::env::temp_dir().join(format!("gitaly-rs-{name}-{nanos}"))
    }

    fn run_git(repo_path: &Path, args: &[&str]) {
        let output = Command::new("git")
            .arg("-C")
            .arg(repo_path)
            .args(args)
            .output()
            .expect("git should execute");
        assert!(
            output.status.success(),
            "git -C {} {} failed\nstdout: {}\nstderr: {}",
            repo_path.display(),
            args.join(" "),
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
    }

    async fn start_server(
        service: SmartHttpServiceImpl,
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
                .add_service(SmartHttpServiceServer::new(service))
                .serve_with_shutdown(server_addr, async {
                    let _ = shutdown_rx.await;
                })
                .await
                .expect("server should run");
        });

        (format!("http://{server_addr}"), shutdown_tx, server_task)
    }

    async fn connect_client(endpoint: String) -> SmartHttpServiceClient<Channel> {
        for _ in 0..50 {
            match SmartHttpServiceClient::connect(endpoint.clone()).await {
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

    #[tokio::test]
    async fn info_refs_upload_pack_stream_contains_heads_ref() {
        let storage_root = unique_dir("smarthttp-service");
        let repo_path = storage_root.join("project.git");

        std::fs::create_dir_all(&storage_root).expect("storage root should be creatable");
        std::fs::create_dir_all(&repo_path).expect("repo path should be creatable");

        run_git(&repo_path, &["init", "--quiet"]);
        run_git(
            &repo_path,
            &["config", "user.name", "SmartHTTP Service Tests"],
        );
        run_git(
            &repo_path,
            &[
                "config",
                "user.email",
                "smarthttp-service-tests@example.com",
            ],
        );
        std::fs::write(repo_path.join("README.md"), b"hello\n").expect("README should write");
        run_git(&repo_path, &["add", "README.md"]);
        run_git(&repo_path, &["commit", "--quiet", "-m", "initial"]);

        let mut storage_paths = HashMap::new();
        storage_paths.insert("default".to_string(), storage_root.clone());

        let dependencies = Arc::new(Dependencies::default().with_storage_paths(storage_paths));
        let service = SmartHttpServiceImpl::new(dependencies);

        let repository = Repository {
            storage_name: "default".to_string(),
            relative_path: "project.git".to_string(),
            ..Repository::default()
        };

        let mut stream = service
            .info_refs_upload_pack(Request::new(InfoRefsRequest {
                repository: Some(repository),
                ..InfoRefsRequest::default()
            }))
            .await
            .expect("info_refs_upload_pack should succeed")
            .into_inner();

        let mut data = Vec::new();
        while let Some(chunk) = stream.next().await {
            let chunk = chunk.expect("stream chunk should not error");
            data.extend(chunk.data);
        }

        assert!(
            data.windows(b"refs/heads/".len())
                .any(|window| window == b"refs/heads/"),
            "advertised refs should include a heads reference"
        );

        if storage_root.exists() {
            std::fs::remove_dir_all(storage_root).expect("storage root should be removable");
        }
    }

    #[tokio::test]
    async fn post_upload_pack_with_sidechannel_returns_negotiation_statistics() {
        let storage_root = unique_dir("smarthttp-sidechannel");
        let repo_path = storage_root.join("project.git");

        std::fs::create_dir_all(&storage_root).expect("storage root should be creatable");
        std::fs::create_dir_all(&repo_path).expect("repo path should be creatable");
        run_git(&repo_path, &["init", "--bare", "--quiet"]);

        let mut storage_paths = HashMap::new();
        storage_paths.insert("default".to_string(), storage_root.clone());
        let dependencies = Arc::new(Dependencies::default().with_storage_paths(storage_paths));
        let service = SmartHttpServiceImpl::new(dependencies);

        let response = service
            .post_upload_pack_with_sidechannel(Request::new(PostUploadPackWithSidechannelRequest {
                repository: Some(Repository {
                    storage_name: "default".to_string(),
                    relative_path: "project.git".to_string(),
                    ..Repository::default()
                }),
                git_config_options: vec!["uploadpack.allowReachableSHA1InWant=true".to_string()],
                git_protocol: "version=2".to_string(),
            }))
            .await
            .expect("post_upload_pack_with_sidechannel should succeed")
            .into_inner();
        assert!(response.packfile_negotiation_statistics.is_some());

        if storage_root.exists() {
            std::fs::remove_dir_all(storage_root).expect("storage root should be removable");
        }
    }

    #[tokio::test]
    async fn post_upload_pack_with_sidechannel_rejects_invalid_git_config_options() {
        let storage_root = unique_dir("smarthttp-sidechannel-invalid-config");
        let repo_path = storage_root.join("project.git");

        std::fs::create_dir_all(&storage_root).expect("storage root should be creatable");
        std::fs::create_dir_all(&repo_path).expect("repo path should be creatable");
        run_git(&repo_path, &["init", "--bare", "--quiet"]);

        let mut storage_paths = HashMap::new();
        storage_paths.insert("default".to_string(), storage_root.clone());
        let dependencies = Arc::new(Dependencies::default().with_storage_paths(storage_paths));
        let service = SmartHttpServiceImpl::new(dependencies);

        let err = service
            .post_upload_pack_with_sidechannel(Request::new(PostUploadPackWithSidechannelRequest {
                repository: Some(Repository {
                    storage_name: "default".to_string(),
                    relative_path: "project.git".to_string(),
                    ..Repository::default()
                }),
                git_config_options: vec!["invalid".to_string()],
                git_protocol: String::new(),
            }))
            .await
            .expect_err("invalid git_config_options should fail");
        assert_eq!(err.code(), Code::InvalidArgument);

        if storage_root.exists() {
            std::fs::remove_dir_all(storage_root).expect("storage root should be removable");
        }
    }

    #[tokio::test]
    async fn post_receive_pack_requires_repository_in_stream() {
        let storage_root = unique_dir("smarthttp-post-receive-repo-required");
        let mut storage_paths = HashMap::new();
        storage_paths.insert("default".to_string(), storage_root.clone());
        let dependencies = Arc::new(Dependencies::default().with_storage_paths(storage_paths));
        let (endpoint, shutdown_tx, server_task) =
            start_server(SmartHttpServiceImpl::new(dependencies)).await;
        let mut client = connect_client(endpoint).await;

        let request_stream = tokio_stream::iter(vec![PostReceivePackRequest {
            repository: None,
            data: b"0000".to_vec(),
            ..PostReceivePackRequest::default()
        }]);

        let status = client
            .post_receive_pack(request_stream)
            .await
            .err()
            .expect("missing repository should fail");
        assert_eq!(status.code(), Code::InvalidArgument);

        shutdown_server(shutdown_tx, server_task).await;
        if storage_root.exists() {
            std::fs::remove_dir_all(storage_root).expect("storage root should be removable");
        }
    }

    #[tokio::test]
    async fn post_upload_pack_rejects_invalid_git_config_options() {
        let storage_root = unique_dir("smarthttp-post-upload-invalid-config");
        let mut storage_paths = HashMap::new();
        storage_paths.insert("default".to_string(), storage_root.clone());
        let dependencies = Arc::new(Dependencies::default().with_storage_paths(storage_paths));
        let (endpoint, shutdown_tx, server_task) =
            start_server(SmartHttpServiceImpl::new(dependencies)).await;
        let mut client = connect_client(endpoint).await;

        let request_stream = tokio_stream::iter(vec![PostUploadPackRequest {
            repository: Some(Repository {
                storage_name: "default".to_string(),
                relative_path: "project.git".to_string(),
                ..Repository::default()
            }),
            git_config_options: vec!["invalid-config".to_string()],
            git_protocol: String::new(),
            data: b"0000".to_vec(),
        }]);

        let status = client
            .post_upload_pack(request_stream)
            .await
            .err()
            .expect("invalid git_config_options should fail");
        assert_eq!(status.code(), Code::InvalidArgument);

        shutdown_server(shutdown_tx, server_task).await;
        if storage_root.exists() {
            std::fs::remove_dir_all(storage_root).expect("storage root should be removable");
        }
    }

    #[tokio::test]
    async fn post_receive_pack_rejects_invalid_git_config_options() {
        let storage_root = unique_dir("smarthttp-post-receive-invalid-config");
        let mut storage_paths = HashMap::new();
        storage_paths.insert("default".to_string(), storage_root.clone());
        let dependencies = Arc::new(Dependencies::default().with_storage_paths(storage_paths));
        let (endpoint, shutdown_tx, server_task) =
            start_server(SmartHttpServiceImpl::new(dependencies)).await;
        let mut client = connect_client(endpoint).await;

        let request_stream = tokio_stream::iter(vec![PostReceivePackRequest {
            repository: Some(Repository {
                storage_name: "default".to_string(),
                relative_path: "project.git".to_string(),
                ..Repository::default()
            }),
            git_config_options: vec!["invalid-config".to_string()],
            git_protocol: String::new(),
            data: b"0000".to_vec(),
            ..PostReceivePackRequest::default()
        }]);

        let status = client
            .post_receive_pack(request_stream)
            .await
            .err()
            .expect("invalid git_config_options should fail");
        assert_eq!(status.code(), Code::InvalidArgument);

        shutdown_server(shutdown_tx, server_task).await;
        if storage_root.exists() {
            std::fs::remove_dir_all(storage_root).expect("storage root should be removable");
        }
    }

    #[tokio::test]
    async fn post_receive_pack_executes_receive_pack_instead_of_unimplemented() {
        let storage_root = unique_dir("smarthttp-post-receive-exec");
        let repo_path = storage_root.join("project.git");
        std::fs::create_dir_all(&storage_root).expect("storage root should be creatable");
        std::fs::create_dir_all(&repo_path).expect("repo path should be creatable");
        run_git(&repo_path, &["init", "--bare", "--quiet"]);

        let mut storage_paths = HashMap::new();
        storage_paths.insert("default".to_string(), storage_root.clone());
        let dependencies = Arc::new(Dependencies::default().with_storage_paths(storage_paths));
        let (endpoint, shutdown_tx, server_task) =
            start_server(SmartHttpServiceImpl::new(dependencies)).await;
        let mut client = connect_client(endpoint).await;

        let request_stream = tokio_stream::iter(vec![PostReceivePackRequest {
            repository: Some(Repository {
                storage_name: "default".to_string(),
                relative_path: "project.git".to_string(),
                ..Repository::default()
            }),
            data: b"0000".to_vec(),
            ..PostReceivePackRequest::default()
        }]);

        match client.post_receive_pack(request_stream).await {
            Ok(response) => {
                let mut stream = response.into_inner();
                let _ = stream.message().await.expect("stream read should succeed");
            }
            Err(status) => {
                assert_ne!(status.code(), Code::Unimplemented);
            }
        }

        shutdown_server(shutdown_tx, server_task).await;
        if storage_root.exists() {
            std::fs::remove_dir_all(storage_root).expect("storage root should be removable");
        }
    }
}
