use std::collections::HashMap;
use std::net::{SocketAddr, TcpListener};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use gitaly_server::{Dependencies, GitalyServer};
use tokio::sync::oneshot;
use tokio::task::JoinHandle;

fn command_exists(name: &str) -> bool {
    Command::new("which")
        .arg(name)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .map(|status| status.success())
        .unwrap_or(false)
}

fn run_git(args: &[&str], cwd: Option<&Path>, extra_env: &[(&str, &str)]) -> String {
    let mut command = Command::new("git");
    command.args(args);
    if let Some(cwd) = cwd {
        command.current_dir(cwd);
    }
    for (key, value) in extra_env {
        command.env(key, value);
    }

    let output = command.output().expect("git should execute");
    assert!(
        output.status.success(),
        "git {:?} failed\nstdout: {}\nstderr: {}",
        args,
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8_lossy(&output.stdout).to_string()
}

fn run_command(program: &str, args: &[&str], cwd: Option<&Path>) {
    let mut command = Command::new(program);
    command.args(args);
    if let Some(cwd) = cwd {
        command.current_dir(cwd);
    }

    let output = command.output().expect("command should execute");
    assert!(
        output.status.success(),
        "{} {:?} failed\nstdout: {}\nstderr: {}",
        program,
        args,
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

fn unique_dir(name: &str) -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock drift")
        .as_nanos();
    std::env::temp_dir().join(format!("gitaly-gateway-{name}-{nanos}"))
}

fn free_addr() -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind should succeed");
    listener
        .local_addr()
        .expect("ephemeral listener should have local addr")
}

async fn wait_for_tcp(addr: SocketAddr) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    loop {
        if tokio::net::TcpStream::connect(addr).await.is_ok() {
            return;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "timed out waiting for {}",
            addr
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

struct RuntimeGuard {
    runtime_dir: PathBuf,
    gateway: Option<tokio::process::Child>,
    shutdown_tx: Option<oneshot::Sender<()>>,
    upstream_task: Option<JoinHandle<()>>,
}

impl RuntimeGuard {
    fn new(runtime_dir: PathBuf) -> Self {
        Self {
            runtime_dir,
            gateway: None,
            shutdown_tx: None,
            upstream_task: None,
        }
    }

    fn set_gateway(&mut self, gateway: tokio::process::Child) {
        self.gateway = Some(gateway);
    }

    fn set_upstream(&mut self, shutdown_tx: oneshot::Sender<()>, upstream_task: JoinHandle<()>) {
        self.shutdown_tx = Some(shutdown_tx);
        self.upstream_task = Some(upstream_task);
    }

    async fn cleanup(mut self) {
        if let Some(gateway) = &mut self.gateway {
            let _ = gateway.start_kill();
            let _ = tokio::time::timeout(Duration::from_secs(2), gateway.wait()).await;
        }

        if let Some(shutdown_tx) = self.shutdown_tx.take() {
            let _ = shutdown_tx.send(());
        }
        if let Some(upstream_task) = self.upstream_task.take() {
            let _ = tokio::time::timeout(Duration::from_secs(2), upstream_task).await;
        }

        let _ = std::fs::remove_dir_all(&self.runtime_dir);
    }
}

impl Drop for RuntimeGuard {
    fn drop(&mut self) {
        if let Some(gateway) = &mut self.gateway {
            let _ = gateway.start_kill();
        }
        if let Some(shutdown_tx) = self.shutdown_tx.take() {
            let _ = shutdown_tx.send(());
        }
        if let Some(upstream_task) = self.upstream_task.take() {
            upstream_task.abort();
        }
        let _ = std::fs::remove_dir_all(&self.runtime_dir);
    }
}

async fn start_upstream(
    storage_root: PathBuf,
) -> (SocketAddr, oneshot::Sender<()>, JoinHandle<()>) {
    let mut storage_paths = HashMap::new();
    storage_paths.insert("default".to_string(), storage_root);
    let dependencies = Arc::new(Dependencies::default().with_storage_paths(storage_paths));

    let listener = TcpListener::bind("127.0.0.1:0").expect("upstream bind should succeed");
    let addr = listener
        .local_addr()
        .expect("upstream local addr should exist");
    drop(listener);

    let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
    let router = GitalyServer::build_router_with_dependencies(dependencies);
    let task = tokio::spawn(async move {
        let _ = router
            .serve_with_shutdown(addr, async {
                let _ = shutdown_rx.await;
            })
            .await;
    });

    wait_for_tcp(addr).await;
    (addr, shutdown_tx, task)
}

async fn start_gateway(
    config_path: &Path,
    http_addr: SocketAddr,
    ssh_addr: Option<SocketAddr>,
) -> tokio::process::Child {
    let child = tokio::process::Command::new(env!("CARGO_BIN_EXE_gitaly-gateway"))
        .arg("--config")
        .arg(config_path)
        .stdout(Stdio::null())
        .stderr(Stdio::inherit())
        .kill_on_drop(true)
        .spawn()
        .expect("gateway should spawn");

    wait_for_tcp(http_addr).await;
    if let Some(ssh_addr) = ssh_addr {
        wait_for_tcp(ssh_addr).await;
    }

    child
}

fn init_repo_fixture(storage_root: &Path, source_repo: &Path) {
    let bare_repo = storage_root.join("project.git");
    std::fs::create_dir_all(storage_root).expect("storage dir should be creatable");
    run_command(
        "git",
        &["init", "--bare", bare_repo.to_str().expect("path utf8")],
        None,
    );

    std::fs::create_dir_all(source_repo).expect("source repo dir should be creatable");
    run_git(&["init"], Some(source_repo), &[]);
    run_git(
        &["config", "user.name", "Gateway Tests"],
        Some(source_repo),
        &[],
    );
    run_git(
        &["config", "user.email", "gateway-tests@example.com"],
        Some(source_repo),
        &[],
    );
    std::fs::write(source_repo.join("README.md"), b"hello\n").expect("README should write");
    run_git(&["add", "README.md"], Some(source_repo), &[]);
    run_git(&["commit", "-m", "initial"], Some(source_repo), &[]);
}

#[tokio::test]
async fn http_proxy_push_and_ls_remote() {
    let token = "user-token-1";
    let runtime_dir = unique_dir("http-e2e");
    let mut runtime_guard = RuntimeGuard::new(runtime_dir.clone());
    let storage_root = runtime_dir.join("storage");
    let source_repo = runtime_dir.join("source");
    std::fs::create_dir_all(&runtime_dir).expect("runtime dir should be creatable");
    init_repo_fixture(&storage_root, &source_repo);

    let (upstream_addr, shutdown_tx, upstream_task) = start_upstream(storage_root.clone()).await;
    runtime_guard.set_upstream(shutdown_tx, upstream_task);
    let http_addr = free_addr();

    let config_path = runtime_dir.join("gateway.toml");
    let config = format!(
        "http_listen_addr = \"{http_addr}\"\n\
         gitaly_addr = \"http://{upstream_addr}\"\n\n\
         [auth]\n\
         gitaly_token = \"secret\"\n\
         client_tokens = [\"{token}\"]\n\n\
         [repositories]\n\
         storage_name = \"default\"\n",
    );
    std::fs::write(&config_path, config).expect("gateway config should write");

    let gateway = start_gateway(&config_path, http_addr, None).await;
    runtime_guard.set_gateway(gateway);

    let remote = format!("http://oauth2:{token}@{http_addr}/project.git");
    run_git(
        &["push", &remote, "HEAD:refs/heads/main"],
        Some(&source_repo),
        &[],
    );
    let refs = run_git(&["ls-remote", &remote, "refs/heads/main"], None, &[]);
    assert!(
        refs.contains("refs/heads/main"),
        "expected refs/heads/main in ls-remote output, got: {refs}"
    );

    runtime_guard.cleanup().await;
}

#[tokio::test]
async fn ssh_proxy_push_and_ls_remote() {
    if !command_exists("ssh") || !command_exists("ssh-keygen") {
        eprintln!("skipping ssh e2e test because ssh or ssh-keygen is unavailable");
        return;
    }

    let token = "user-token-1";
    let runtime_dir = unique_dir("ssh-e2e");
    let mut runtime_guard = RuntimeGuard::new(runtime_dir.clone());
    let storage_root = runtime_dir.join("storage");
    let source_repo = runtime_dir.join("source");
    std::fs::create_dir_all(&runtime_dir).expect("runtime dir should be creatable");
    init_repo_fixture(&storage_root, &source_repo);

    let private_key_path = runtime_dir.join("gateway_test_key");
    run_command(
        "ssh-keygen",
        &[
            "-t",
            "ed25519",
            "-N",
            "",
            "-f",
            private_key_path
                .to_str()
                .expect("private key path should be utf8"),
        ],
        None,
    );
    let public_key = std::fs::read_to_string(private_key_path.with_extension("pub"))
        .expect("public key should read")
        .trim()
        .to_string();

    let (upstream_addr, shutdown_tx, upstream_task) = start_upstream(storage_root.clone()).await;
    runtime_guard.set_upstream(shutdown_tx, upstream_task);
    let http_addr = free_addr();
    let mut ssh_addr = free_addr();
    while ssh_addr == http_addr {
        ssh_addr = free_addr();
    }

    let config_path = runtime_dir.join("gateway.toml");
    let config = format!(
        "http_listen_addr = \"{http_addr}\"\n\
         ssh_listen_addr = \"{ssh_addr}\"\n\
         gitaly_addr = \"http://{upstream_addr}\"\n\n\
         [auth]\n\
         gitaly_token = \"secret\"\n\
         client_tokens = [\"{token}\"]\n\
         ssh_public_keys = [\"{public_key}\"]\n\n\
         [repositories]\n\
         storage_name = \"default\"\n",
    );
    std::fs::write(&config_path, config).expect("gateway config should write");

    let gateway = start_gateway(&config_path, http_addr, Some(ssh_addr)).await;
    runtime_guard.set_gateway(gateway);

    let ssh_cmd = format!(
        "ssh -i {} -o IdentitiesOnly=yes -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o PasswordAuthentication=no -o PubkeyAuthentication=yes -p {}",
        private_key_path.display(),
        ssh_addr.port()
    );
    let remote = format!("ssh://git@{ssh_addr}/project.git");

    run_git(
        &["push", &remote, "HEAD:refs/heads/main"],
        Some(&source_repo),
        &[("GIT_SSH_COMMAND", &ssh_cmd)],
    );
    let refs = run_git(
        &["ls-remote", &remote, "refs/heads/main"],
        None,
        &[("GIT_SSH_COMMAND", &ssh_cmd)],
    );
    assert!(
        refs.contains("refs/heads/main"),
        "expected refs/heads/main in ls-remote output, got: {refs}"
    );

    runtime_guard.cleanup().await;
}
