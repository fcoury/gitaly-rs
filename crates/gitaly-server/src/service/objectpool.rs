use std::path::{Component, Path, PathBuf};
use std::sync::Arc;

use tokio::process::Command;
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

    fn resolve_object_pool_path(
        &self,
        object_pool: Option<ObjectPool>,
    ) -> Result<PathBuf, Status> {
        let object_pool =
            object_pool.ok_or_else(|| Status::invalid_argument("object_pool is required"))?;
        self.resolve_repo_path(object_pool.repository)
    }

    fn path_to_repository(&self, pool_path: &Path) -> Option<Repository> {
        for (storage_name, storage_root) in &self.dependencies.storage_paths {
            if let Ok(relative_path) = pool_path.strip_prefix(storage_root) {
                let relative_path = relative_path.to_string_lossy().replace('\\', "/");
                if relative_path.is_empty() {
                    continue;
                }

                return Some(Repository {
                    storage_name: storage_name.clone(),
                    relative_path,
                    ..Repository::default()
                });
            }
        }

        None
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

async fn git_output(args: &[String]) -> Result<std::process::Output, Status> {
    Command::new("git")
        .args(args)
        .output()
        .await
        .map_err(|err| Status::internal(format!("failed to execute git command: {err}")))
}

fn status_for_git_failure(args: &[String], output: &std::process::Output) -> Status {
    Status::internal(format!(
        "git command failed: git {}; stderr: {}",
        args.join(" "),
        String::from_utf8_lossy(&output.stderr).trim()
    ))
}

#[tonic::async_trait]
impl ObjectPoolService for ObjectPoolServiceImpl {
    async fn create_object_pool(
        &self,
        request: Request<CreateObjectPoolRequest>,
    ) -> Result<Response<CreateObjectPoolResponse>, Status> {
        let request = request.into_inner();
        let object_pool_path = self.resolve_object_pool_path(request.object_pool)?;
        let origin_path = self.resolve_repo_path(request.origin)?;

        if object_pool_path.exists() {
            return Err(Status::already_exists("object pool already exists"));
        }

        if let Some(parent) = object_pool_path.parent() {
            std::fs::create_dir_all(parent)
                .map_err(|err| Status::internal(format!("failed to create object pool parent: {err}")))?;
        }

        let args = vec![
            "clone".to_string(),
            "--bare".to_string(),
            "--quiet".to_string(),
            origin_path.to_string_lossy().into_owned(),
            object_pool_path.to_string_lossy().into_owned(),
        ];
        let output = git_output(&args).await?;
        if !output.status.success() {
            return Err(status_for_git_failure(&args, &output));
        }

        Ok(Response::new(CreateObjectPoolResponse {}))
    }

    async fn delete_object_pool(
        &self,
        request: Request<DeleteObjectPoolRequest>,
    ) -> Result<Response<DeleteObjectPoolResponse>, Status> {
        let object_pool_path = self.resolve_object_pool_path(request.into_inner().object_pool)?;
        if !object_pool_path.exists() {
            return Err(Status::not_found("object pool does not exist"));
        }

        std::fs::remove_dir_all(&object_pool_path)
            .map_err(|err| Status::internal(format!("failed to delete object pool: {err}")))?;
        Ok(Response::new(DeleteObjectPoolResponse {}))
    }

    async fn link_repository_to_object_pool(
        &self,
        request: Request<LinkRepositoryToObjectPoolRequest>,
    ) -> Result<Response<LinkRepositoryToObjectPoolResponse>, Status> {
        let request = request.into_inner();
        let object_pool_path = self.resolve_object_pool_path(request.object_pool)?;
        let repository_path = self.resolve_repo_path(request.repository)?;
        if !object_pool_path.exists() {
            return Err(Status::not_found("object pool does not exist"));
        }
        if !repository_path.exists() {
            return Err(Status::not_found("repository does not exist"));
        }

        let info_dir = repository_path.join("objects").join("info");
        std::fs::create_dir_all(&info_dir)
            .map_err(|err| Status::internal(format!("failed to create alternates directory: {err}")))?;
        let alternates_file = info_dir.join("alternates");
        std::fs::write(
            &alternates_file,
            format!("{}\n", object_pool_path.join("objects").to_string_lossy()),
        )
        .map_err(|err| Status::internal(format!("failed to write alternates file: {err}")))?;

        Ok(Response::new(LinkRepositoryToObjectPoolResponse {}))
    }

    async fn disconnect_git_alternates(
        &self,
        request: Request<DisconnectGitAlternatesRequest>,
    ) -> Result<Response<DisconnectGitAlternatesResponse>, Status> {
        let repository_path = self.resolve_repo_path(request.into_inner().repository)?;
        let alternates_file = repository_path.join("objects").join("info").join("alternates");
        if alternates_file.exists() {
            std::fs::remove_file(&alternates_file)
                .map_err(|err| Status::internal(format!("failed to remove alternates file: {err}")))?;
        }

        Ok(Response::new(DisconnectGitAlternatesResponse {}))
    }

    async fn fetch_into_object_pool(
        &self,
        request: Request<FetchIntoObjectPoolRequest>,
    ) -> Result<Response<FetchIntoObjectPoolResponse>, Status> {
        let request = request.into_inner();
        let origin_path = self.resolve_repo_path(request.origin)?;
        let object_pool_path = self.resolve_object_pool_path(request.object_pool)?;
        if !object_pool_path.exists() {
            return Err(Status::not_found("object pool does not exist"));
        }

        let args = vec![
            "-C".to_string(),
            object_pool_path.to_string_lossy().into_owned(),
            "fetch".to_string(),
            "--prune".to_string(),
            "--quiet".to_string(),
            origin_path.to_string_lossy().into_owned(),
            "+refs/*:refs/*".to_string(),
        ];
        let output = git_output(&args).await?;
        if !output.status.success() {
            return Err(status_for_git_failure(&args, &output));
        }

        Ok(Response::new(FetchIntoObjectPoolResponse {}))
    }

    async fn get_object_pool(
        &self,
        request: Request<GetObjectPoolRequest>,
    ) -> Result<Response<GetObjectPoolResponse>, Status> {
        let repository_path = self.resolve_repo_path(request.into_inner().repository)?;
        let alternates_file = repository_path.join("objects").join("info").join("alternates");
        if !alternates_file.exists() {
            return Ok(Response::new(GetObjectPoolResponse { object_pool: None }));
        }

        let alternates = std::fs::read_to_string(&alternates_file)
            .map_err(|err| Status::internal(format!("failed to read alternates file: {err}")))?;
        let first_entry = alternates
            .lines()
            .map(str::trim)
            .find(|line| !line.is_empty())
            .ok_or_else(|| Status::failed_precondition("alternates file is empty"))?;
        let objects_path = PathBuf::from(first_entry);
        let pool_path = objects_path.parent().ok_or_else(|| {
            Status::failed_precondition("alternates entry does not reference an object directory")
        })?;

        let repository = self.path_to_repository(pool_path).ok_or_else(|| {
            Status::failed_precondition(
                "object pool path does not map to a configured storage repository",
            )
        })?;

        Ok(Response::new(GetObjectPoolResponse {
            object_pool: Some(ObjectPool {
                repository: Some(repository),
            }),
        }))
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

    use gitaly_proto::gitaly::object_pool_service_client::ObjectPoolServiceClient;
    use gitaly_proto::gitaly::object_pool_service_server::ObjectPoolServiceServer;
    use gitaly_proto::gitaly::{
        CreateObjectPoolRequest, DeleteObjectPoolRequest, DisconnectGitAlternatesRequest,
        FetchIntoObjectPoolRequest, GetObjectPoolRequest, LinkRepositoryToObjectPoolRequest,
        ObjectPool, Repository,
    };

    use crate::dependencies::Dependencies;

    use super::ObjectPoolServiceImpl;

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

    fn create_origin_repository(storage_root: &Path) -> PathBuf {
        let source_repo = storage_root.join("source");
        let origin_repo = storage_root.join("origin.git");
        std::fs::create_dir_all(&source_repo).expect("source repo should be creatable");
        std::fs::create_dir_all(&origin_repo).expect("origin repo should be creatable");

        run_git(&source_repo, &["init", "--quiet"]);
        run_git(&source_repo, &["config", "user.name", "ObjectPool Service Tests"]);
        run_git(
            &source_repo,
            &["config", "user.email", "objectpool-service-tests@example.com"],
        );
        std::fs::write(source_repo.join("README.md"), b"hello\n").expect("README should write");
        run_git(&source_repo, &["add", "README.md"]);
        run_git(&source_repo, &["commit", "--quiet", "-m", "initial"]);
        run_git(&source_repo, &["branch", "-M", "main"]);

        run_git(&origin_repo, &["init", "--bare", "--quiet"]);
        run_git(
            &source_repo,
            &["remote", "add", "origin", origin_repo.to_string_lossy().as_ref()],
        );
        run_git(&source_repo, &["push", "--quiet", "origin", "main"]);
        run_git(&origin_repo, &["symbolic-ref", "HEAD", "refs/heads/main"]);

        origin_repo
    }

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

    fn test_dependencies(storage_root: &Path) -> Arc<Dependencies> {
        let mut storage_paths = HashMap::new();
        storage_paths.insert("default".to_string(), storage_root.to_path_buf());
        Arc::new(Dependencies::default().with_storage_paths(storage_paths))
    }

    #[tokio::test]
    async fn object_pool_lifecycle_operations_succeed() {
        let storage_root = unique_dir("objectpool-service-lifecycle");
        std::fs::create_dir_all(&storage_root).expect("storage root should be creatable");
        create_origin_repository(&storage_root);
        let member_repo = storage_root.join("member.git");
        std::fs::create_dir_all(&member_repo).expect("member repo should be creatable");
        run_git(&member_repo, &["init", "--bare", "--quiet"]);

        let (endpoint, shutdown_tx, server_task) =
            start_server(ObjectPoolServiceImpl::new(test_dependencies(&storage_root))).await;
        let mut client = connect_client(endpoint).await;

        client
            .create_object_pool(CreateObjectPoolRequest {
                object_pool: Some(ObjectPool {
                    repository: Some(Repository {
                        storage_name: "default".to_string(),
                        relative_path: "pool.git".to_string(),
                        ..Repository::default()
                    }),
                }),
                origin: Some(Repository {
                    storage_name: "default".to_string(),
                    relative_path: "origin.git".to_string(),
                    ..Repository::default()
                }),
            })
            .await
            .expect("create_object_pool should succeed");
        assert!(storage_root.join("pool.git").exists());

        client
            .link_repository_to_object_pool(LinkRepositoryToObjectPoolRequest {
                object_pool: Some(ObjectPool {
                    repository: Some(Repository {
                        storage_name: "default".to_string(),
                        relative_path: "pool.git".to_string(),
                        ..Repository::default()
                    }),
                }),
                repository: Some(Repository {
                    storage_name: "default".to_string(),
                    relative_path: "member.git".to_string(),
                    ..Repository::default()
                }),
            })
            .await
            .expect("link_repository_to_object_pool should succeed");
        assert!(member_repo.join("objects/info/alternates").exists());

        let get_response = client
            .get_object_pool(GetObjectPoolRequest {
                repository: Some(Repository {
                    storage_name: "default".to_string(),
                    relative_path: "member.git".to_string(),
                    ..Repository::default()
                }),
            })
            .await
            .expect("get_object_pool should succeed")
            .into_inner();
        let object_pool = get_response
            .object_pool
            .expect("object pool should be returned")
            .repository
            .expect("object pool repository should be returned");
        assert_eq!(object_pool.storage_name, "default");
        assert_eq!(object_pool.relative_path, "pool.git");

        client
            .fetch_into_object_pool(FetchIntoObjectPoolRequest {
                origin: Some(Repository {
                    storage_name: "default".to_string(),
                    relative_path: "origin.git".to_string(),
                    ..Repository::default()
                }),
                object_pool: Some(ObjectPool {
                    repository: Some(Repository {
                        storage_name: "default".to_string(),
                        relative_path: "pool.git".to_string(),
                        ..Repository::default()
                    }),
                }),
            })
            .await
            .expect("fetch_into_object_pool should succeed");

        client
            .disconnect_git_alternates(DisconnectGitAlternatesRequest {
                repository: Some(Repository {
                    storage_name: "default".to_string(),
                    relative_path: "member.git".to_string(),
                    ..Repository::default()
                }),
            })
            .await
            .expect("disconnect_git_alternates should succeed");
        assert!(!member_repo.join("objects/info/alternates").exists());

        client
            .delete_object_pool(DeleteObjectPoolRequest {
                object_pool: Some(ObjectPool {
                    repository: Some(Repository {
                        storage_name: "default".to_string(),
                        relative_path: "pool.git".to_string(),
                        ..Repository::default()
                    }),
                }),
            })
            .await
            .expect("delete_object_pool should succeed");
        assert!(!storage_root.join("pool.git").exists());

        shutdown_server(shutdown_tx, server_task).await;
        if storage_root.exists() {
            std::fs::remove_dir_all(storage_root).expect("storage root should be removable");
        }
    }

    #[tokio::test]
    async fn get_object_pool_returns_none_when_repository_is_not_linked() {
        let storage_root = unique_dir("objectpool-service-unlinked");
        let repository = storage_root.join("project.git");
        std::fs::create_dir_all(&repository).expect("repo should be creatable");
        run_git(&repository, &["init", "--bare", "--quiet"]);

        let (endpoint, shutdown_tx, server_task) =
            start_server(ObjectPoolServiceImpl::new(test_dependencies(&storage_root))).await;
        let mut client = connect_client(endpoint).await;

        let response = client
            .get_object_pool(GetObjectPoolRequest {
                repository: Some(Repository {
                    storage_name: "default".to_string(),
                    relative_path: "project.git".to_string(),
                    ..Repository::default()
                }),
            })
            .await
            .expect("get_object_pool should succeed for unlinked repo")
            .into_inner();
        assert!(response.object_pool.is_none());

        shutdown_server(shutdown_tx, server_task).await;
        if storage_root.exists() {
            std::fs::remove_dir_all(storage_root).expect("storage root should be removable");
        }
    }
}
