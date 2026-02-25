#![allow(dead_code)]

use std::collections::HashMap;
use std::future::Future;
use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use gitaly_proto::gitaly::repository_service_client::RepositoryServiceClient;
use gitaly_proto::gitaly::server_service_client::ServerServiceClient;
use gitaly_server::{Dependencies, GitalyServer, RuntimePaths, StorageStatus};
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tokio::time::{sleep, timeout};
use tonic::transport::Channel;

const DEFAULT_STORAGE_NAME: &str = "default";
const DEFAULT_CONNECT_ATTEMPTS: usize = 50;
const DEFAULT_CONNECT_RETRY_DELAY: Duration = Duration::from_millis(20);
const SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(2);

static TEMP_COUNTER: AtomicU64 = AtomicU64::new(0);

pub struct TestDirs {
    root_dir: PathBuf,
    pub runtime_dir: PathBuf,
    pub storage_dir: PathBuf,
}

impl TestDirs {
    fn new(prefix: &str) -> Self {
        let counter = TEMP_COUNTER.fetch_add(1, Ordering::Relaxed);
        let now_nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock should be monotonic")
            .as_nanos();
        let root_dir = std::env::temp_dir().join(format!(
            "gitaly-rs-tests-{prefix}-{}-{now_nanos}-{counter}",
            std::process::id()
        ));
        let runtime_dir = root_dir.join("runtime");
        let storage_dir = root_dir.join("storage");

        std::fs::create_dir_all(&runtime_dir).expect("runtime dir should be creatable");
        std::fs::create_dir_all(&storage_dir).expect("storage dir should be creatable");

        Self {
            root_dir,
            runtime_dir,
            storage_dir,
        }
    }
}

impl Drop for TestDirs {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.root_dir);
    }
}

pub fn create_temp_runtime_storage_dirs(prefix: &str) -> TestDirs {
    TestDirs::new(prefix)
}

pub fn build_dependencies(storage_dir: &Path) -> Arc<Dependencies> {
    let mut storage_paths = HashMap::new();
    storage_paths.insert(DEFAULT_STORAGE_NAME.to_string(), storage_dir.to_path_buf());

    let storage_statuses = vec![StorageStatus {
        storage_name: DEFAULT_STORAGE_NAME.to_string(),
        ..StorageStatus::default()
    }];

    Arc::new(
        Dependencies::default()
            .with_storage_paths(storage_paths)
            .with_storage_statuses(storage_statuses),
    )
}

pub fn bootstrap_runtime_paths(runtime_dir: &Path) -> RuntimePaths {
    RuntimePaths::bootstrap(runtime_dir.to_path_buf())
        .expect("runtime paths should bootstrap for integration tests")
}

pub struct TestServer {
    dirs: TestDirs,
    pub dependencies: Arc<Dependencies>,
    pub runtime_paths: RuntimePaths,
    pub endpoint: String,
    shutdown_tx: Option<oneshot::Sender<()>>,
    server_task: Option<JoinHandle<Result<(), tonic::transport::Error>>>,
}

impl TestServer {
    pub fn storage_dir(&self) -> &Path {
        &self.dirs.storage_dir
    }

    pub async fn shutdown(mut self) {
        self.shutdown_internal().await;
    }

    async fn shutdown_internal(&mut self) {
        if let Some(shutdown_tx) = self.shutdown_tx.take() {
            let _ = shutdown_tx.send(());
        }

        if let Some(server_task) = self.server_task.take() {
            let joined = timeout(SHUTDOWN_TIMEOUT, server_task)
                .await
                .expect("server should stop within timeout");
            let result = joined.expect("server task should not panic");
            if let Err(error) = result {
                panic!("server task should stop cleanly: {error}");
            }
        }
    }
}

impl Drop for TestServer {
    fn drop(&mut self) {
        if let Some(shutdown_tx) = self.shutdown_tx.take() {
            let _ = shutdown_tx.send(());
        }
        if let Some(server_task) = self.server_task.take() {
            server_task.abort();
        }
    }
}

pub async fn start_test_server(prefix: &str) -> TestServer {
    let dirs = create_temp_runtime_storage_dirs(prefix);
    let dependencies = build_dependencies(&dirs.storage_dir);
    let runtime_paths = bootstrap_runtime_paths(&dirs.runtime_dir);
    let router = GitalyServer::build_router_with_dependencies_and_runtime_paths(
        Arc::clone(&dependencies),
        &runtime_paths,
    )
    .expect("router should build for integration test");

    let listener =
        TcpListener::bind("127.0.0.1:0").expect("ephemeral listener should bind successfully");
    let listen_addr = listener
        .local_addr()
        .expect("ephemeral listener should have local address");
    drop(listener);

    let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
    let server_task = tokio::spawn(async move {
        router
            .serve_with_shutdown(listen_addr, async {
                let _ = shutdown_rx.await;
            })
            .await
    });

    TestServer {
        dirs,
        dependencies,
        runtime_paths,
        endpoint: format!("http://{listen_addr}"),
        shutdown_tx: Some(shutdown_tx),
        server_task: Some(server_task),
    }
}

async fn connect_with_retries<Client, Connector, Fut>(
    attempts: usize,
    retry_delay: Duration,
    mut connector: Connector,
) -> Result<Client, tonic::transport::Error>
where
    Connector: FnMut() -> Fut,
    Fut: Future<Output = Result<Client, tonic::transport::Error>>,
{
    let mut last_error = None;

    for _ in 0..attempts {
        match connector().await {
            Ok(client) => return Ok(client),
            Err(error) => {
                last_error = Some(error);
                sleep(retry_delay).await;
            }
        }
    }

    Err(last_error.expect("at least one connection attempt should run"))
}

pub async fn try_connect_server_client(
    endpoint: &str,
    attempts: usize,
    retry_delay: Duration,
) -> Result<ServerServiceClient<Channel>, tonic::transport::Error> {
    let endpoint = endpoint.to_string();
    connect_with_retries(attempts, retry_delay, move || {
        ServerServiceClient::connect(endpoint.clone())
    })
    .await
}

pub async fn connect_server_client(endpoint: &str) -> ServerServiceClient<Channel> {
    try_connect_server_client(
        endpoint,
        DEFAULT_CONNECT_ATTEMPTS,
        DEFAULT_CONNECT_RETRY_DELAY,
    )
    .await
    .unwrap_or_else(|error| panic!("server client should connect to {endpoint}: {error}"))
}

pub async fn try_connect_repository_client(
    endpoint: &str,
    attempts: usize,
    retry_delay: Duration,
) -> Result<RepositoryServiceClient<Channel>, tonic::transport::Error> {
    let endpoint = endpoint.to_string();
    connect_with_retries(attempts, retry_delay, move || {
        RepositoryServiceClient::connect(endpoint.clone())
    })
    .await
}

pub async fn connect_repository_client(endpoint: &str) -> RepositoryServiceClient<Channel> {
    try_connect_repository_client(
        endpoint,
        DEFAULT_CONNECT_ATTEMPTS,
        DEFAULT_CONNECT_RETRY_DELAY,
    )
    .await
    .unwrap_or_else(|error| panic!("repository client should connect to {endpoint}: {error}"))
}
