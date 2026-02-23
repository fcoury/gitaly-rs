use std::sync::Arc;

use tokio::process::Command;
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

    fn ensure_storage_configured(&self, storage_name: &str) -> Result<(), Status> {
        if storage_name.trim().is_empty() {
            return Err(Status::invalid_argument("storage_name is required"));
        }

        if self.dependencies.storage_paths.contains_key(storage_name) {
            return Ok(());
        }

        Err(Status::not_found(format!(
            "storage `{storage_name}` is not configured"
        )))
    }
}

fn unimplemented_remote_rpc(method: &'static str) -> Status {
    Status::unimplemented(format!("RemoteService::{method} is not implemented"))
}

fn status_for_git_failure(args: &str, output: &std::process::Output) -> Status {
    Status::internal(format!(
        "git command failed: git {args}; stderr: {}",
        String::from_utf8_lossy(&output.stderr).trim()
    ))
}

fn parse_remote_root_ref(stdout: &[u8]) -> Option<String> {
    String::from_utf8_lossy(stdout).lines().find_map(|line| {
        let line = line.trim();
        let rest = line.strip_prefix("ref:")?;
        let mut parts = rest.trim().split('\t');
        let reference = parts.next()?.trim();
        let symref = parts.next()?.trim();
        (symref == "HEAD" && !reference.is_empty()).then(|| reference.to_string())
    })
}

fn remote_not_found(stderr: &[u8]) -> bool {
    let stderr = String::from_utf8_lossy(stderr).to_lowercase();
    stderr.contains("repository not found")
        || stderr.contains("could not read from remote repository")
        || stderr.contains("does not appear to be a git repository")
        || stderr.contains("not found")
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
        request: Request<FindRemoteRepositoryRequest>,
    ) -> Result<Response<FindRemoteRepositoryResponse>, Status> {
        let request = request.into_inner();
        if request.remote.trim().is_empty() {
            return Err(Status::invalid_argument("remote is required"));
        }
        self.ensure_storage_configured(&request.storage_name)?;

        let output = Command::new("git")
            .args(["ls-remote", "--exit-code", &request.remote, "HEAD"])
            .output()
            .await
            .map_err(|err| Status::internal(format!("failed to execute git command: {err}")))?;

        if output.status.success() {
            return Ok(Response::new(FindRemoteRepositoryResponse { exists: true }));
        }

        if remote_not_found(&output.stderr) {
            return Ok(Response::new(FindRemoteRepositoryResponse { exists: false }));
        }

        Err(Status::unavailable(format!(
            "failed to query remote repository: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        )))
    }

    async fn find_remote_root_ref(
        &self,
        request: Request<FindRemoteRootRefRequest>,
    ) -> Result<Response<FindRemoteRootRefResponse>, Status> {
        let request = request.into_inner();
        if let Some(repository) = &request.repository {
            self.ensure_storage_configured(&repository.storage_name)?;
        }
        if request.remote_url.trim().is_empty() {
            return Err(Status::invalid_argument("remote_url is required"));
        }

        let output = Command::new("git")
            .args(["ls-remote", "--symref", &request.remote_url, "HEAD"])
            .output()
            .await
            .map_err(|err| Status::internal(format!("failed to execute git command: {err}")))?;

        if !output.status.success() {
            return Err(status_for_git_failure(
                "ls-remote --symref <remote> HEAD",
                &output,
            ));
        }

        let reference = parse_remote_root_ref(&output.stdout)
            .ok_or_else(|| Status::not_found("remote root ref was not advertised"))?;

        Ok(Response::new(FindRemoteRootRefResponse { r#ref: reference }))
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::path::{Path, PathBuf};
    use std::process::Command;
    use std::sync::Arc;
    use std::time::Duration;
    use std::time::{SystemTime, UNIX_EPOCH};

    use tokio::sync::oneshot;
    use tokio::task::JoinHandle;
    use tokio::time::sleep;
    use tonic::Code;

    use gitaly_proto::gitaly::remote_service_client::RemoteServiceClient;
    use gitaly_proto::gitaly::remote_service_server::RemoteServiceServer;
    use gitaly_proto::gitaly::{
        FindRemoteRepositoryRequest, FindRemoteRootRefRequest, Repository, UpdateRemoteMirrorRequest,
    };

    use crate::dependencies::Dependencies;

    use super::RemoteServiceImpl;

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

    fn create_remote_fixture(root: &Path) -> PathBuf {
        let source_repo = root.join("source");
        let remote_repo = root.join("remote.git");
        std::fs::create_dir_all(&source_repo).expect("source repo should be creatable");
        std::fs::create_dir_all(&remote_repo).expect("remote repo should be creatable");

        run_git(&source_repo, &["init", "--quiet"]);
        run_git(&source_repo, &["config", "user.name", "Remote Service Tests"]);
        run_git(
            &source_repo,
            &["config", "user.email", "remote-service-tests@example.com"],
        );
        std::fs::write(source_repo.join("README.md"), b"hello\n").expect("README should write");
        run_git(&source_repo, &["add", "README.md"]);
        run_git(&source_repo, &["commit", "--quiet", "-m", "initial"]);
        run_git(&source_repo, &["branch", "-M", "main"]);

        run_git(&remote_repo, &["init", "--bare", "--quiet"]);
        run_git(&source_repo, &["remote", "add", "origin", remote_repo.to_string_lossy().as_ref()]);
        run_git(&source_repo, &["push", "--quiet", "origin", "main"]);
        run_git(&remote_repo, &["symbolic-ref", "HEAD", "refs/heads/main"]);

        remote_repo
    }

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
        let mut storage_paths = HashMap::new();
        storage_paths.insert("default".to_string(), std::env::temp_dir());
        Arc::new(Dependencies::default().with_storage_paths(storage_paths))
    }

    #[tokio::test]
    async fn update_remote_mirror_remains_unimplemented() {
        let (endpoint, shutdown_tx, server_task) =
            start_server(RemoteServiceImpl::new(test_dependencies())).await;
        let mut client = connect_client(endpoint).await;

        let update_mirror_stream = tokio_stream::iter(vec![UpdateRemoteMirrorRequest::default()]);
        let update_mirror_error = client
            .update_remote_mirror(update_mirror_stream)
            .await
            .expect_err("update_remote_mirror should be unimplemented");
        assert_eq!(update_mirror_error.code(), Code::Unimplemented);

        shutdown_server(shutdown_tx, server_task).await;
    }

    #[tokio::test]
    async fn find_remote_repository_reports_exists_and_missing() {
        let root = unique_dir("remote-service-find-repository");
        std::fs::create_dir_all(&root).expect("root should be creatable");
        let remote_repo = create_remote_fixture(&root);
        let missing_repo = root.join("missing.git");

        let (endpoint, shutdown_tx, server_task) =
            start_server(RemoteServiceImpl::new(test_dependencies())).await;
        let mut client = connect_client(endpoint).await;

        let existing = client
            .find_remote_repository(FindRemoteRepositoryRequest {
                remote: remote_repo.to_string_lossy().into_owned(),
                storage_name: "default".to_string(),
            })
            .await
            .expect("find_remote_repository should succeed for existing remote")
            .into_inner();
        assert!(existing.exists);

        let missing = client
            .find_remote_repository(FindRemoteRepositoryRequest {
                remote: missing_repo.to_string_lossy().into_owned(),
                storage_name: "default".to_string(),
            })
            .await
            .expect("find_remote_repository should succeed for missing remote")
            .into_inner();
        assert!(!missing.exists);

        shutdown_server(shutdown_tx, server_task).await;
        if root.exists() {
            std::fs::remove_dir_all(root).expect("root should be removable");
        }
    }

    #[tokio::test]
    async fn find_remote_root_ref_returns_advertised_head_reference() {
        let root = unique_dir("remote-service-find-root-ref");
        std::fs::create_dir_all(&root).expect("root should be creatable");
        let remote_repo = create_remote_fixture(&root);

        let (endpoint, shutdown_tx, server_task) =
            start_server(RemoteServiceImpl::new(test_dependencies())).await;
        let mut client = connect_client(endpoint).await;

        let response = client
            .find_remote_root_ref(FindRemoteRootRefRequest {
                repository: Some(Repository {
                    storage_name: "default".to_string(),
                    relative_path: "irrelevant.git".to_string(),
                    ..Repository::default()
                }),
                remote_url: remote_repo.to_string_lossy().into_owned(),
                ..FindRemoteRootRefRequest::default()
            })
            .await
            .expect("find_remote_root_ref should succeed")
            .into_inner();
        assert_eq!(response.r#ref, "refs/heads/main");

        shutdown_server(shutdown_tx, server_task).await;
        if root.exists() {
            std::fs::remove_dir_all(root).expect("root should be removable");
        }
    }
}
