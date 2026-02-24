use std::collections::HashMap;
use std::env;
use std::fs;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{anyhow, Context, Result};
use gitaly_config::Config;
use gitaly_server::{
    Dependencies, GitalyServer, RuntimePaths, ServerBootstrapError, StorageStatus,
};
use tokio::sync::watch;

const ENV_CONFIG_PATH: &str = "GITALY_CONFIG";
const ENV_RUNTIME_DIR: &str = "GITALY_RUNTIME_DIR";

pub async fn run_from_args<I>(args: I) -> Result<()>
where
    I: IntoIterator<Item = String>,
{
    let args = CliArgs::parse(args)?;
    let config_path = absolute_path(&args.config_path);
    let config = load_config(&config_path)?;
    let runtime_dir = args.runtime_dir.unwrap_or_else(default_runtime_dir);
    let runtime_paths =
        RuntimePaths::bootstrap(runtime_dir).context("failed to bootstrap runtime paths")?;

    let listen_addr = parse_socket_addr("listen_addr", &config.listen_addr, &config_path)?;
    let internal_addr = parse_socket_addr("internal_addr", &config.internal_addr, &config_path)?;
    let dependencies = Arc::new(build_dependencies(&config));

    eprintln!("gitaly-rs external listener: {listen_addr}");
    if internal_addr != listen_addr {
        eprintln!("gitaly-rs internal listener: {internal_addr}");
    } else {
        eprintln!("gitaly-rs internal listener shares external address: {internal_addr}");
    }
    eprintln!(
        "gitaly-rs runtime dir: {}",
        runtime_paths.pid_dir().display()
    );

    let (shutdown_tx, shutdown_rx) = watch::channel(false);

    let external_router = GitalyServer::build_router_with_dependencies_runtime_config(
        Arc::clone(&dependencies),
        &runtime_paths,
        &config.runtime,
    )
    .map_err(map_bootstrap_error)?;
    let external_shutdown_rx = shutdown_rx.clone();
    let external_task = tokio::spawn(async move {
        external_router
            .serve_with_shutdown(listen_addr, shutdown_future(external_shutdown_rx))
            .await
            .map_err(|error| format_listener_error("external gRPC", listen_addr, error))
    });

    let internal_task = if internal_addr != listen_addr {
        let internal_router = GitalyServer::build_router_with_dependencies_runtime_config(
            Arc::clone(&dependencies),
            &runtime_paths,
            &config.runtime,
        )
        .map_err(map_bootstrap_error)?;
        let internal_shutdown_rx = shutdown_rx.clone();
        Some(tokio::spawn(async move {
            internal_router
                .serve_with_shutdown(internal_addr, shutdown_future(internal_shutdown_rx))
                .await
                .map_err(|error| format_listener_error("internal gRPC", internal_addr, error))
        }))
    } else {
        None
    };

    tokio::signal::ctrl_c()
        .await
        .context("failed to wait for Ctrl-C")?;
    let _ = shutdown_tx.send(true);

    external_task
        .await
        .context("external listener task panicked")??;
    if let Some(internal_task) = internal_task {
        internal_task
            .await
            .context("internal listener task panicked")??;
    }

    Ok(())
}

pub async fn run_from_env() -> Result<()> {
    run_from_args(env::args()).await
}

fn load_config(path: &PathBuf) -> Result<Config> {
    let raw = fs::read_to_string(path)
        .with_context(|| format!("failed to read config file `{}`", path.display()))?;
    Config::from_toml(&raw).map_err(|error| format_parse_error(path, &error.to_string()))
}

fn parse_socket_addr(
    field_name: &str,
    listen_addr: &str,
    config_path: &Path,
) -> Result<SocketAddr> {
    listen_addr.parse::<SocketAddr>().map_err(|error| {
        anyhow!(
            "invalid `{field_name}` value `{listen_addr}` in `{}`: {error}",
            config_path.display()
        )
    })
}

async fn shutdown_future(mut shutdown_rx: watch::Receiver<bool>) {
    loop {
        if *shutdown_rx.borrow() {
            break;
        }

        if shutdown_rx.changed().await.is_err() {
            break;
        }
    }
}

fn default_runtime_dir() -> PathBuf {
    if let Some(from_env) = env::var_os(ENV_RUNTIME_DIR) {
        return PathBuf::from(from_env);
    }

    env::temp_dir().join("gitaly-rs-runtime")
}

fn build_dependencies(config: &Config) -> Dependencies {
    let storage_statuses = config
        .storages
        .iter()
        .map(|storage| StorageStatus {
            storage_name: storage.name.clone(),
            ..StorageStatus::default()
        })
        .collect();
    let storage_paths = config
        .storages
        .iter()
        .map(|storage| (storage.name.clone(), PathBuf::from(storage.path.clone())))
        .collect::<HashMap<_, _>>();

    Dependencies::default()
        .with_git_version(detect_git_version())
        .with_storage_statuses(storage_statuses)
        .with_storage_paths(storage_paths)
        .with_auth_token(trimmed_token(&config.auth.token))
        .with_auth_transitioning(config.auth.transitioning)
}

fn trimmed_token(token: &str) -> Option<String> {
    let token = token.trim();
    (!token.is_empty()).then(|| token.to_string())
}

fn detect_git_version() -> String {
    let Ok(output) = std::process::Command::new("git").arg("--version").output() else {
        return "unknown".to_string();
    };
    if !output.status.success() {
        return "unknown".to_string();
    }

    let version = String::from_utf8_lossy(&output.stdout).trim().to_string();
    if version.is_empty() {
        "unknown".to_string()
    } else {
        version
    }
}

fn map_bootstrap_error(error: ServerBootstrapError) -> anyhow::Error {
    anyhow!("failed to build server router: {error}")
}

fn absolute_path(path: &Path) -> PathBuf {
    if path.is_absolute() {
        return path.to_path_buf();
    }

    env::current_dir()
        .map(|cwd| cwd.join(path))
        .unwrap_or_else(|_| path.to_path_buf())
}

fn format_parse_error(config_path: &Path, parse_error: &str) -> anyhow::Error {
    let mut message = format!(
        "failed to parse config `{}`: {parse_error}",
        config_path.display()
    );
    if let Some(hint) = parse_error_hint(parse_error) {
        message.push_str("\nhint: ");
        message.push_str(hint);
    }
    anyhow!(message)
}

fn parse_error_hint(parse_error: &str) -> Option<&'static str> {
    if parse_error.contains("duplicate key") {
        return Some(
            "remove duplicate TOML table headers or keys (for example duplicate `[auth]`).",
        );
    }

    if parse_error.contains("invalid type: string") && parse_error.contains("expected a sequence") {
        return Some(
            "array fields must use TOML arrays, for example `client_tokens = [\"token\"]`.",
        );
    }

    None
}

fn format_listener_error(
    listener_name: &str,
    listen_addr: SocketAddr,
    error: impl std::fmt::Display,
) -> anyhow::Error {
    let error_text = error.to_string();
    let mut message = format!("{listener_name} listener failed on `{listen_addr}`: {error_text}");
    if error_text
        .to_ascii_lowercase()
        .contains("address already in use")
    {
        message.push_str(
            "\nhint: another process is already bound to this address; stop it or change the listen address.",
        );
    }
    anyhow!(message)
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct CliArgs {
    config_path: PathBuf,
    runtime_dir: Option<PathBuf>,
}

impl CliArgs {
    fn parse<I>(args: I) -> Result<Self>
    where
        I: IntoIterator<Item = String>,
    {
        let mut iter = args.into_iter();
        let _program_name = iter.next();

        let mut config_path = env::var_os(ENV_CONFIG_PATH).map(PathBuf::from);
        let mut runtime_dir = None;

        while let Some(arg) = iter.next() {
            match arg.as_str() {
                "--help" | "-h" => {
                    print_usage();
                    std::process::exit(0);
                }
                "--config" => {
                    let value = iter
                        .next()
                        .ok_or_else(|| anyhow!("missing value for `--config`"))?;
                    config_path = Some(PathBuf::from(value));
                }
                "--runtime-dir" => {
                    let value = iter
                        .next()
                        .ok_or_else(|| anyhow!("missing value for `--runtime-dir`"))?;
                    runtime_dir = Some(PathBuf::from(value));
                }
                _ => return Err(anyhow!("unknown argument `{arg}`")),
            }
        }

        let config_path = config_path.ok_or_else(|| {
            anyhow!("missing config path; pass `--config <path>` or set `{ENV_CONFIG_PATH}`")
        })?;

        Ok(Self {
            config_path,
            runtime_dir,
        })
    }
}

fn print_usage() {
    eprintln!(
        "usage: gitaly-rs --config <path> [--runtime-dir <path>]\n\
         env:\n  {ENV_CONFIG_PATH}=<path>\n  {ENV_RUNTIME_DIR}=<path>"
    );
}

#[cfg(test)]
mod tests {
    use super::{parse_error_hint, trimmed_token, CliArgs};

    #[test]
    fn parses_required_config_argument() {
        let args = CliArgs::parse(vec![
            "gitaly-rs".to_string(),
            "--config".to_string(),
            "/tmp/gitaly.toml".to_string(),
        ])
        .expect("args should parse");

        assert_eq!(
            args.config_path,
            std::path::PathBuf::from("/tmp/gitaly.toml")
        );
        assert_eq!(args.runtime_dir, None);
    }

    #[test]
    fn parses_optional_runtime_dir_argument() {
        let args = CliArgs::parse(vec![
            "gitaly-rs".to_string(),
            "--config".to_string(),
            "/tmp/gitaly.toml".to_string(),
            "--runtime-dir".to_string(),
            "/tmp/gitaly-runtime".to_string(),
        ])
        .expect("args should parse");

        assert_eq!(
            args.runtime_dir,
            Some(std::path::PathBuf::from("/tmp/gitaly-runtime"))
        );
    }

    #[test]
    fn parse_error_hint_reports_duplicate_key_guidance() {
        let hint = parse_error_hint("invalid table header\nduplicate key `auth` in document root");
        assert_eq!(
            hint,
            Some("remove duplicate TOML table headers or keys (for example duplicate `[auth]`).")
        );
    }

    #[test]
    fn parse_error_hint_reports_array_type_guidance() {
        let hint = parse_error_hint("invalid type: string \"[token]\", expected a sequence");
        assert_eq!(
            hint,
            Some("array fields must use TOML arrays, for example `client_tokens = [\"token\"]`.")
        );
    }

    #[test]
    fn trimmed_token_discards_empty_values() {
        assert_eq!(trimmed_token(""), None);
        assert_eq!(trimmed_token("   "), None);
        assert_eq!(trimmed_token(" secret "), Some("secret".to_string()));
    }
}
