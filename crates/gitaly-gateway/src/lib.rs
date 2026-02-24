use std::collections::HashMap;
use std::env;
use std::fs;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, Context, Result};
use axum::body::{to_bytes, Body};
use axum::extract::{OriginalUri, Path, Request, State};
use axum::http::header::{AUTHORIZATION, CONTENT_TYPE};
use axum::http::{HeaderMap, HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::Router;
use base64::engine::general_purpose::STANDARD;
use base64::Engine;
use russh::server::{Auth, Handle as SshHandle, Handler, Msg, Server, Session};
use russh::{Channel, ChannelId, CryptoVec};
use serde::Deserialize;
use tokio::sync::mpsc;
use tonic::transport::Channel as GrpcChannel;
use tonic::Request as GrpcRequest;
use tonic::Status;
use tracing::{info, warn};

use gitaly_proto::gitaly::smart_http_service_client::SmartHttpServiceClient;
use gitaly_proto::gitaly::ssh_service_client::SshServiceClient;
use gitaly_proto::gitaly::*;

const ENV_CONFIG_PATH: &str = "GITALY_GATEWAY_CONFIG";

#[derive(Debug, Clone, Deserialize)]
struct GatewayConfig {
    http_listen_addr: String,
    ssh_listen_addr: Option<String>,
    gitaly_addr: String,
    auth: AuthConfig,
    repositories: RepositoriesConfig,
}

#[derive(Debug, Clone, Deserialize)]
struct AuthConfig {
    gitaly_token: String,
    client_tokens: Vec<String>,
    #[serde(default)]
    ssh_public_keys: Vec<String>,
}

#[derive(Debug, Clone, Deserialize)]
struct RepositoriesConfig {
    storage_name: String,
}

#[derive(Debug, Clone)]
struct AppState {
    config: Arc<GatewayConfig>,
    channel: GrpcChannel,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct CliArgs {
    config_path: String,
}

impl CliArgs {
    fn parse<I>(args: I) -> Result<Self>
    where
        I: IntoIterator<Item = String>,
    {
        let mut iter = args.into_iter();
        let _ = iter.next();
        let mut config_path = env::var(ENV_CONFIG_PATH).ok();

        while let Some(arg) = iter.next() {
            match arg.as_str() {
                "--config" => {
                    config_path = Some(
                        iter.next()
                            .ok_or_else(|| anyhow!("missing value for `--config`"))?,
                    );
                }
                "--help" | "-h" => {
                    eprintln!("usage: gitaly-gateway --config <path>");
                    std::process::exit(0);
                }
                _ => return Err(anyhow!("unknown argument `{arg}`")),
            }
        }

        let config_path = config_path.ok_or_else(|| {
            anyhow!("missing config path; pass `--config <path>` or set `{ENV_CONFIG_PATH}`")
        })?;

        Ok(Self { config_path })
    }
}

pub async fn run_from_args<I>(args: I) -> Result<()>
where
    I: IntoIterator<Item = String>,
{
    tracing_subscriber::fmt::init();

    let args = CliArgs::parse(args)?;
    let config = load_config(&args.config_path)?;
    let http_addr = config
        .http_listen_addr
        .parse::<SocketAddr>()
        .context("invalid http_listen_addr")?;
    let ssh_addr = config
        .ssh_listen_addr
        .as_ref()
        .map(|value| {
            value
                .parse::<SocketAddr>()
                .context("invalid ssh_listen_addr")
        })
        .transpose()?;

    let channel = GrpcChannel::from_shared(config.gitaly_addr.clone())
        .context("invalid gitaly_addr")?
        .connect()
        .await
        .context("failed connecting to gitaly-rs")?;

    let state = Arc::new(AppState {
        config: Arc::new(config),
        channel,
    });

    let app = Router::new()
        .route("/{*path}", get(handle_get).post(handle_post))
        .with_state(Arc::clone(&state));

    let http_listener = tokio::net::TcpListener::bind(http_addr)
        .await
        .context("failed binding HTTP listener")?;
    info!("gitaly-gateway HTTP listening on {http_addr}");
    let http_task = tokio::spawn(async move {
        axum::serve(http_listener, app)
            .await
            .map_err(|err| anyhow!("http server exited with error: {err}"))
    });

    let ssh_task = if let Some(ssh_addr) = ssh_addr {
        let state = Arc::clone(&state);
        Some(tokio::spawn(async move {
            run_ssh_server(state, ssh_addr).await
        }))
    } else {
        warn!("SSH listener disabled because `ssh_listen_addr` is not configured");
        None
    };

    tokio::select! {
        _ = tokio::signal::ctrl_c() => {
            info!("received Ctrl-C, shutting down");
        }
        result = http_task => {
            result.context("http task panicked")??;
            return Ok(());
        }
        result = async {
            if let Some(task) = ssh_task {
                task.await.context("ssh task panicked")??;
            } else {
                std::future::pending::<Result<()>>().await?;
            }
            Ok::<(), anyhow::Error>(())
        } => {
            result?;
            return Ok(());
        }
    }

    Ok(())
}

pub async fn run_from_env() -> Result<()> {
    run_from_args(env::args()).await
}

fn load_config(path: &str) -> Result<GatewayConfig> {
    let raw = fs::read_to_string(path).with_context(|| format!("failed to read `{path}`"))?;
    let config: GatewayConfig =
        toml::from_str(&raw).with_context(|| format!("failed to parse `{path}`"))?;

    if config.auth.client_tokens.is_empty() {
        return Err(anyhow!("auth.client_tokens must not be empty"));
    }
    if config.auth.gitaly_token.trim().is_empty() {
        return Err(anyhow!("auth.gitaly_token must not be empty"));
    }
    if config.repositories.storage_name.trim().is_empty() {
        return Err(anyhow!("repositories.storage_name must not be empty"));
    }

    Ok(config)
}

async fn handle_get(
    State(state): State<Arc<AppState>>,
    Path(path): Path<String>,
    OriginalUri(uri): OriginalUri,
    headers: HeaderMap,
) -> Response {
    match handle_info_refs(state, &path, &uri, &headers).await {
        Ok(response) => response,
        Err((code, msg)) => error_response(code, msg),
    }
}

async fn handle_post(
    State(state): State<Arc<AppState>>,
    Path(path): Path<String>,
    headers: HeaderMap,
    request: Request,
) -> Response {
    let body = request.into_body();
    match handle_post_rpc(state, &path, &headers, body).await {
        Ok(response) => response,
        Err((code, msg)) => error_response(code, msg),
    }
}

async fn handle_info_refs(
    state: Arc<AppState>,
    path: &str,
    uri: &axum::http::Uri,
    headers: &HeaderMap,
) -> Result<Response, (StatusCode, String)> {
    authenticate(headers, &state.config.auth.client_tokens)?;

    let repo = path
        .strip_suffix("/info/refs")
        .ok_or_else(|| (StatusCode::NOT_FOUND, "not found".to_string()))?;
    if repo.is_empty() {
        return Err((
            StatusCode::BAD_REQUEST,
            "missing repository path".to_string(),
        ));
    }

    let service = query_param(uri.query(), "service").ok_or_else(|| {
        (
            StatusCode::BAD_REQUEST,
            "missing `service` query param".to_string(),
        )
    })?;
    let repository = Repository {
        storage_name: state.config.repositories.storage_name.clone(),
        relative_path: repo.to_string(),
        ..Repository::default()
    };

    let mut client = SmartHttpServiceClient::new(state.channel.clone());
    let mut grpc_request = GrpcRequest::new(InfoRefsRequest {
        repository: Some(repository),
        ..InfoRefsRequest::default()
    });
    attach_gitaly_auth(&mut grpc_request, &state.config.auth.gitaly_token)?;

    let (content_type, stream) = match service {
        "git-upload-pack" => (
            "application/x-git-upload-pack-advertisement",
            client
                .info_refs_upload_pack(grpc_request)
                .await
                .map_err(map_grpc_error)?
                .into_inner(),
        ),
        "git-receive-pack" => (
            "application/x-git-receive-pack-advertisement",
            client
                .info_refs_receive_pack(grpc_request)
                .await
                .map_err(map_grpc_error)?
                .into_inner(),
        ),
        _ => {
            return Err((
                StatusCode::BAD_REQUEST,
                format!("unsupported service `{service}`"),
            ))
        }
    };

    let body = decorate_info_refs_advertisement(service, collect_info_refs(stream).await?);
    build_ok_response(content_type, body)
}

async fn handle_post_rpc(
    state: Arc<AppState>,
    path: &str,
    headers: &HeaderMap,
    body: Body,
) -> Result<Response, (StatusCode, String)> {
    authenticate(headers, &state.config.auth.client_tokens)?;

    let payload = to_bytes(body, usize::MAX).await.map_err(|err| {
        (
            StatusCode::BAD_REQUEST,
            format!("invalid request body: {err}"),
        )
    })?;

    if let Some(repo) = path.strip_suffix("/git-upload-pack") {
        if repo.is_empty() {
            return Err((
                StatusCode::BAD_REQUEST,
                "missing repository path".to_string(),
            ));
        }

        let repository = Repository {
            storage_name: state.config.repositories.storage_name.clone(),
            relative_path: repo.to_string(),
            ..Repository::default()
        };

        let mut client = SmartHttpServiceClient::new(state.channel.clone());
        let request_stream = tokio_stream::iter(vec![PostUploadPackRequest {
            repository: Some(repository),
            data: payload.to_vec(),
            ..PostUploadPackRequest::default()
        }]);
        let mut grpc_request = GrpcRequest::new(request_stream);
        attach_gitaly_auth(&mut grpc_request, &state.config.auth.gitaly_token)?;

        let stream = client
            .post_upload_pack(grpc_request)
            .await
            .map_err(map_grpc_error)?
            .into_inner();
        let body = collect_post_upload_pack(stream).await?;
        return build_ok_response("application/x-git-upload-pack-result", body);
    }

    if let Some(repo) = path.strip_suffix("/git-receive-pack") {
        if repo.is_empty() {
            return Err((
                StatusCode::BAD_REQUEST,
                "missing repository path".to_string(),
            ));
        }

        let repository = Repository {
            storage_name: state.config.repositories.storage_name.clone(),
            relative_path: repo.to_string(),
            ..Repository::default()
        };

        let mut client = SmartHttpServiceClient::new(state.channel.clone());
        let request_stream = tokio_stream::iter(vec![PostReceivePackRequest {
            repository: Some(repository),
            data: payload.to_vec(),
            gl_id: "gateway".to_string(),
            gl_repository: repo.to_string(),
            gl_username: "gateway".to_string(),
            ..PostReceivePackRequest::default()
        }]);
        let mut grpc_request = GrpcRequest::new(request_stream);
        attach_gitaly_auth(&mut grpc_request, &state.config.auth.gitaly_token)?;

        let stream = client
            .post_receive_pack(grpc_request)
            .await
            .map_err(map_grpc_error)?
            .into_inner();
        let body = collect_post_receive_pack(stream).await?;
        return build_ok_response("application/x-git-receive-pack-result", body);
    }

    Err((StatusCode::NOT_FOUND, "not found".to_string()))
}

#[derive(Debug, Clone)]
enum SshCommandKind {
    UploadPack,
    ReceivePack,
}

#[derive(Debug, Clone)]
struct SshCommand {
    kind: SshCommandKind,
    repo_relative_path: String,
}

#[derive(Debug, Default, Clone)]
struct SshChannelState {
    stdin_tx: Option<mpsc::UnboundedSender<Vec<u8>>>,
}

#[derive(Clone)]
struct GatewaySshServer {
    state: Arc<AppState>,
    next_connection_id: usize,
    connection_id: usize,
    channels: HashMap<ChannelId, SshChannelState>,
}

impl GatewaySshServer {
    fn new(state: Arc<AppState>) -> Self {
        Self {
            state,
            next_connection_id: 0,
            connection_id: 0,
            channels: HashMap::new(),
        }
    }

    fn parse_exec_command(raw_command: &[u8]) -> Option<SshCommand> {
        let command = std::str::from_utf8(raw_command).ok()?.trim();

        let (kind, raw_path) = if let Some(path) = command.strip_prefix("git-upload-pack ") {
            (SshCommandKind::UploadPack, path)
        } else if let Some(path) = command.strip_prefix("git-receive-pack ") {
            (SshCommandKind::ReceivePack, path)
        } else {
            return None;
        };

        let trimmed = raw_path.trim().trim_matches('\'').trim_matches('"').trim();
        let relative_path = trimmed.strip_prefix('/').unwrap_or(trimmed);
        if relative_path.is_empty() {
            return None;
        }

        Some(SshCommand {
            kind,
            repo_relative_path: relative_path.to_string(),
        })
    }
}

impl Server for GatewaySshServer {
    type Handler = Self;

    fn new_client(&mut self, _: Option<SocketAddr>) -> Self {
        self.next_connection_id = self.next_connection_id.saturating_add(1);
        let mut next = self.clone();
        next.connection_id = self.next_connection_id;
        next.channels = HashMap::new();
        next
    }
}

impl Handler for GatewaySshServer {
    type Error = anyhow::Error;

    fn auth_publickey(
        &mut self,
        _user: &str,
        public_key: &russh::keys::ssh_key::PublicKey,
    ) -> impl std::future::Future<Output = Result<Auth, Self::Error>> + Send {
        let configured_keys = self.state.config.auth.ssh_public_keys.clone();
        let offered = public_key
            .to_openssh()
            .map_err(|err| anyhow!("invalid offered public key: {err}"));

        async move {
            if configured_keys.is_empty() {
                return Ok(Auth::reject());
            }

            let offered = offered?;
            let offered = normalize_public_key_entry(&offered)
                .ok_or_else(|| anyhow!("invalid offered public key format"))?;
            let accepted = configured_keys
                .iter()
                .filter_map(|configured| normalize_public_key_entry(configured))
                .any(|configured| configured == offered);
            Ok(if accepted {
                Auth::Accept
            } else {
                Auth::reject()
            })
        }
    }

    fn auth_password(
        &mut self,
        _user: &str,
        password: &str,
    ) -> impl std::future::Future<Output = Result<Auth, Self::Error>> + Send {
        let configured_tokens = self.state.config.auth.client_tokens.clone();
        let password = password.to_string();

        async move {
            if configured_tokens
                .iter()
                .any(|configured| configured == &password)
            {
                Ok(Auth::Accept)
            } else {
                Ok(Auth::reject())
            }
        }
    }

    fn channel_open_session(
        &mut self,
        channel: Channel<Msg>,
        _session: &mut Session,
    ) -> impl std::future::Future<Output = Result<bool, Self::Error>> + Send {
        self.channels
            .insert(channel.id(), SshChannelState::default());
        std::future::ready(Ok(true))
    }

    fn exec_request(
        &mut self,
        channel: ChannelId,
        data: &[u8],
        session: &mut Session,
    ) -> impl std::future::Future<Output = Result<(), Self::Error>> + Send {
        let Some(command) = Self::parse_exec_command(data) else {
            return std::future::ready(session.channel_failure(channel).map_err(Into::into));
        };

        let state = self.channels.entry(channel).or_default();

        let (stdin_tx, stdin_rx) = mpsc::unbounded_channel::<Vec<u8>>();
        state.stdin_tx = Some(stdin_tx);
        let session_handle = session.handle();
        spawn_ssh_proxy_task(
            Arc::clone(&self.state),
            command,
            channel,
            session_handle,
            stdin_rx,
        );

        std::future::ready(session.channel_success(channel).map_err(Into::into))
    }

    fn data(
        &mut self,
        channel: ChannelId,
        data: &[u8],
        _session: &mut Session,
    ) -> impl std::future::Future<Output = Result<(), Self::Error>> + Send {
        if let Some(state) = self.channels.get_mut(&channel) {
            if let Some(stdin_tx) = &state.stdin_tx {
                let _ = stdin_tx.send(data.to_vec());
            }
        }
        std::future::ready(Ok(()))
    }

    fn channel_eof(
        &mut self,
        channel: ChannelId,
        _session: &mut Session,
    ) -> impl std::future::Future<Output = Result<(), Self::Error>> + Send {
        self.channels.remove(&channel);
        std::future::ready(Ok(()))
    }
}

fn spawn_ssh_proxy_task(
    app_state: Arc<AppState>,
    command: SshCommand,
    channel: ChannelId,
    session_handle: SshHandle,
    stdin_rx: mpsc::UnboundedReceiver<Vec<u8>>,
) {
    tokio::spawn(async move {
        let result = match command.kind {
            SshCommandKind::UploadPack => {
                stream_upload_pack(
                    app_state,
                    command.repo_relative_path,
                    session_handle.clone(),
                    channel,
                    stdin_rx,
                )
                .await
            }
            SshCommandKind::ReceivePack => {
                stream_receive_pack(
                    app_state,
                    command.repo_relative_path,
                    session_handle.clone(),
                    channel,
                    stdin_rx,
                )
                .await
            }
        };

        if let Err(status) = result {
            let _ = session_handle
                .extended_data(
                    channel,
                    1,
                    CryptoVec::from_slice(
                        format!("gateway proxy error: {}\n", status.message()).as_bytes(),
                    ),
                )
                .await;
            let _ = session_handle.exit_status_request(channel, 1).await;
            let _ = session_handle.eof(channel).await;
            let _ = session_handle.close(channel).await;
        }
    });
}

async fn stream_upload_pack(
    app_state: Arc<AppState>,
    repo_relative_path: String,
    session_handle: SshHandle,
    channel: ChannelId,
    mut stdin_rx: mpsc::UnboundedReceiver<Vec<u8>>,
) -> Result<(), Status> {
    let repository = Repository {
        storage_name: app_state.config.repositories.storage_name.clone(),
        relative_path: repo_relative_path,
        ..Repository::default()
    };

    let (request_tx, request_rx) = mpsc::channel::<SshUploadPackRequest>(16);
    request_tx
        .send(SshUploadPackRequest {
            repository: Some(repository),
            ..SshUploadPackRequest::default()
        })
        .await
        .map_err(|_| Status::internal("failed to initialize upload-pack stream"))?;

    let mut client = SshServiceClient::new(app_state.channel.clone());
    let mut grpc_request =
        GrpcRequest::new(tokio_stream::wrappers::ReceiverStream::new(request_rx));
    attach_gitaly_auth(&mut grpc_request, &app_state.config.auth.gitaly_token)
        .map_err(|(_, msg)| Status::internal(msg))?;
    let mut response_stream = client.ssh_upload_pack(grpc_request).await?.into_inner();

    let stdin_forward = tokio::spawn(async move {
        let request_tx = request_tx;
        while let Some(chunk) = stdin_rx.recv().await {
            if request_tx
                .send(SshUploadPackRequest {
                    stdin: chunk,
                    ..SshUploadPackRequest::default()
                })
                .await
                .is_err()
            {
                break;
            }
        }
    });

    let mut exit_status = 0_u32;
    while let Some(message) = response_stream.message().await? {
        if !message.stdout.is_empty() {
            session_handle
                .data(channel, CryptoVec::from_slice(&message.stdout))
                .await
                .map_err(|_| Status::internal("failed to forward upload-pack stdout"))?;
        }
        if !message.stderr.is_empty() {
            session_handle
                .extended_data(channel, 1, CryptoVec::from_slice(&message.stderr))
                .await
                .map_err(|_| Status::internal("failed to forward upload-pack stderr"))?;
        }
        if let Some(status) = message.exit_status {
            exit_status = status.value.max(0) as u32;
        }
    }

    let _ = stdin_forward.await;
    session_handle
        .exit_status_request(channel, exit_status)
        .await
        .map_err(|_| Status::internal("failed to send upload-pack exit status"))?;
    session_handle
        .eof(channel)
        .await
        .map_err(|_| Status::internal("failed to send upload-pack EOF"))?;
    session_handle
        .close(channel)
        .await
        .map_err(|_| Status::internal("failed to close upload-pack channel"))?;
    Ok(())
}

async fn stream_receive_pack(
    app_state: Arc<AppState>,
    repo_relative_path: String,
    session_handle: SshHandle,
    channel: ChannelId,
    mut stdin_rx: mpsc::UnboundedReceiver<Vec<u8>>,
) -> Result<(), Status> {
    let repository = Repository {
        storage_name: app_state.config.repositories.storage_name.clone(),
        relative_path: repo_relative_path.clone(),
        ..Repository::default()
    };

    let (request_tx, request_rx) = mpsc::channel::<SshReceivePackRequest>(16);
    request_tx
        .send(SshReceivePackRequest {
            repository: Some(repository),
            gl_id: "gateway".to_string(),
            gl_repository: repo_relative_path,
            gl_username: "gateway".to_string(),
            ..SshReceivePackRequest::default()
        })
        .await
        .map_err(|_| Status::internal("failed to initialize receive-pack stream"))?;

    let mut client = SshServiceClient::new(app_state.channel.clone());
    let mut grpc_request =
        GrpcRequest::new(tokio_stream::wrappers::ReceiverStream::new(request_rx));
    attach_gitaly_auth(&mut grpc_request, &app_state.config.auth.gitaly_token)
        .map_err(|(_, msg)| Status::internal(msg))?;
    let mut response_stream = client.ssh_receive_pack(grpc_request).await?.into_inner();

    let stdin_forward = tokio::spawn(async move {
        let request_tx = request_tx;
        while let Some(chunk) = stdin_rx.recv().await {
            if request_tx
                .send(SshReceivePackRequest {
                    stdin: chunk,
                    ..SshReceivePackRequest::default()
                })
                .await
                .is_err()
            {
                break;
            }
        }
    });

    let mut exit_status = 0_u32;
    while let Some(message) = response_stream.message().await? {
        if !message.stdout.is_empty() {
            session_handle
                .data(channel, CryptoVec::from_slice(&message.stdout))
                .await
                .map_err(|_| Status::internal("failed to forward receive-pack stdout"))?;
        }
        if !message.stderr.is_empty() {
            session_handle
                .extended_data(channel, 1, CryptoVec::from_slice(&message.stderr))
                .await
                .map_err(|_| Status::internal("failed to forward receive-pack stderr"))?;
        }
        if let Some(status) = message.exit_status {
            exit_status = status.value.max(0) as u32;
        }
    }

    let _ = stdin_forward.await;
    session_handle
        .exit_status_request(channel, exit_status)
        .await
        .map_err(|_| Status::internal("failed to send receive-pack exit status"))?;
    session_handle
        .eof(channel)
        .await
        .map_err(|_| Status::internal("failed to send receive-pack EOF"))?;
    session_handle
        .close(channel)
        .await
        .map_err(|_| Status::internal("failed to close receive-pack channel"))?;
    Ok(())
}

async fn run_ssh_server(state: Arc<AppState>, listen_addr: SocketAddr) -> Result<()> {
    let host_key = russh::keys::PrivateKey::random(
        &mut russh::keys::ssh_key::rand_core::OsRng,
        russh::keys::ssh_key::Algorithm::Ed25519,
    )
    .context("failed generating SSH host key")?;

    let ssh_config = russh::server::Config {
        auth_rejection_time: Duration::from_secs(1),
        auth_rejection_time_initial: Some(Duration::from_millis(0)),
        inactivity_timeout: Some(Duration::from_secs(1800)),
        keys: vec![host_key],
        ..russh::server::Config::default()
    };

    info!("gitaly-gateway SSH listening on {listen_addr}");
    let mut server = GatewaySshServer::new(state);
    server
        .run_on_address(Arc::new(ssh_config), listen_addr)
        .await
        .context("ssh server exited with error")
}

fn authenticate(
    headers: &HeaderMap,
    client_tokens: &[String],
) -> Result<String, (StatusCode, String)> {
    let Some(raw_auth) = headers.get(AUTHORIZATION) else {
        return Err((
            StatusCode::UNAUTHORIZED,
            "missing authorization".to_string(),
        ));
    };
    let auth = raw_auth.to_str().map_err(|_| {
        (
            StatusCode::UNAUTHORIZED,
            "invalid authorization header".to_string(),
        )
    })?;

    if let Some(token) = auth.strip_prefix("Bearer ") {
        if client_tokens.iter().any(|candidate| candidate == token) {
            return Ok(token.to_string());
        }
    }

    if let Some(encoded) = auth.strip_prefix("Basic ") {
        let decoded = STANDARD
            .decode(encoded)
            .map_err(|_| (StatusCode::UNAUTHORIZED, "invalid basic auth".to_string()))?;
        let decoded = String::from_utf8(decoded)
            .map_err(|_| (StatusCode::UNAUTHORIZED, "invalid basic auth".to_string()))?;
        if let Some((_, password)) = decoded.split_once(':') {
            if client_tokens.iter().any(|candidate| candidate == password) {
                return Ok(password.to_string());
            }
        }
    }

    Err((StatusCode::UNAUTHORIZED, "unauthorized".to_string()))
}

fn query_param<'a>(query: Option<&'a str>, name: &str) -> Option<&'a str> {
    let query = query?;
    for pair in query.split('&') {
        if let Some((k, v)) = pair.split_once('=') {
            if k == name {
                return Some(v);
            }
        }
    }
    None
}

fn normalize_public_key_entry(value: &str) -> Option<String> {
    let mut parts = value.split_whitespace();
    let key_type = parts.next()?;
    let key_data = parts.next()?;
    Some(format!("{key_type} {key_data}"))
}

fn decorate_info_refs_advertisement(service: &str, body: Vec<u8>) -> Vec<u8> {
    let header = format!("# service={service}\n");
    let mut decorated = Vec::with_capacity(4 + header.len() + 4 + body.len());
    append_pkt_line(&mut decorated, header.as_bytes());
    decorated.extend_from_slice(b"0000");
    decorated.extend_from_slice(&body);
    decorated
}

fn append_pkt_line(out: &mut Vec<u8>, payload: &[u8]) {
    let line_len = payload.len() + 4;
    out.extend_from_slice(format!("{line_len:04x}").as_bytes());
    out.extend_from_slice(payload);
}

fn attach_gitaly_auth<T>(
    request: &mut GrpcRequest<T>,
    gitaly_token: &str,
) -> Result<(), (StatusCode, String)> {
    let auth_value = format!("Bearer {gitaly_token}")
        .parse::<tonic::metadata::MetadataValue<_>>()
        .map_err(|err| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("invalid gitaly auth metadata: {err}"),
            )
        })?;
    request.metadata_mut().insert("authorization", auth_value);
    Ok(())
}

fn map_grpc_error(status: Status) -> (StatusCode, String) {
    (
        grpc_status_to_http(status.code()),
        status.message().to_string(),
    )
}

fn grpc_status_to_http(code: tonic::Code) -> StatusCode {
    match code {
        tonic::Code::InvalidArgument => StatusCode::BAD_REQUEST,
        tonic::Code::NotFound => StatusCode::NOT_FOUND,
        tonic::Code::PermissionDenied => StatusCode::FORBIDDEN,
        tonic::Code::Unauthenticated => StatusCode::UNAUTHORIZED,
        tonic::Code::Unavailable => StatusCode::SERVICE_UNAVAILABLE,
        _ => StatusCode::INTERNAL_SERVER_ERROR,
    }
}

async fn collect_info_refs(
    mut stream: tonic::Streaming<InfoRefsResponse>,
) -> Result<Vec<u8>, (StatusCode, String)> {
    let mut body = Vec::new();
    while let Some(message) = stream.message().await.map_err(map_grpc_error)? {
        body.extend_from_slice(&message.data);
    }
    Ok(body)
}

async fn collect_post_upload_pack(
    mut stream: tonic::Streaming<PostUploadPackResponse>,
) -> Result<Vec<u8>, (StatusCode, String)> {
    let mut body = Vec::new();
    while let Some(message) = stream.message().await.map_err(map_grpc_error)? {
        body.extend_from_slice(&message.data);
    }
    Ok(body)
}

async fn collect_post_receive_pack(
    mut stream: tonic::Streaming<PostReceivePackResponse>,
) -> Result<Vec<u8>, (StatusCode, String)> {
    let mut body = Vec::new();
    while let Some(message) = stream.message().await.map_err(map_grpc_error)? {
        body.extend_from_slice(&message.data);
    }
    Ok(body)
}

fn build_ok_response(content_type: &str, body: Vec<u8>) -> Result<Response, (StatusCode, String)> {
    let mut response = Response::new(Body::from(body));
    *response.status_mut() = StatusCode::OK;
    response.headers_mut().insert(
        CONTENT_TYPE,
        HeaderValue::from_str(content_type).map_err(|err| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("invalid content type header: {err}"),
            )
        })?,
    );
    Ok(response)
}

fn error_response(code: StatusCode, message: String) -> Response {
    let mut response = (code, message).into_response();
    if code == StatusCode::UNAUTHORIZED {
        response.headers_mut().insert(
            axum::http::header::WWW_AUTHENTICATE,
            HeaderValue::from_static("Basic realm=\"gitaly-gateway\""),
        );
    }

    response
}

#[cfg(test)]
mod tests {
    use super::{decorate_info_refs_advertisement, normalize_public_key_entry};

    #[test]
    fn decorate_receive_pack_info_refs_adds_service_preamble() {
        let decorated = decorate_info_refs_advertisement("git-receive-pack", b"payload".to_vec());
        let mut expected = b"001f# service=git-receive-pack\n0000".to_vec();
        expected.extend_from_slice(b"payload");
        assert_eq!(decorated, expected);
    }

    #[test]
    fn decorate_upload_pack_info_refs_adds_service_preamble() {
        let decorated = decorate_info_refs_advertisement("git-upload-pack", b"abc".to_vec());
        let mut expected = b"001e# service=git-upload-pack\n0000".to_vec();
        expected.extend_from_slice(b"abc");
        assert_eq!(decorated, expected);
    }

    #[test]
    fn normalize_public_key_entry_strips_comment() {
        let normalized = normalize_public_key_entry(
            "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIKsIFygFaTbKGqrHMFHM/7QqorpNsBeULda7aIbQgYVP user@example",
        );
        assert_eq!(
            normalized.as_deref(),
            Some(
                "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIKsIFygFaTbKGqrHMFHM/7QqorpNsBeULda7aIbQgYVP"
            )
        );
    }
}
