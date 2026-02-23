use std::collections::HashMap;
use std::env;
use std::fs;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{anyhow, Context, Result};
use gitaly_config::Config;
use gitaly_server::{
    Dependencies, GitalyServer, RuntimePaths, ServerBootstrapError, StorageStatus,
};

const ENV_CONFIG_PATH: &str = "GITALY_CONFIG";
const ENV_RUNTIME_DIR: &str = "GITALY_RUNTIME_DIR";

#[tokio::main]
async fn main() -> Result<()> {
    let args = CliArgs::parse(env::args())?;
    let config = load_config(&args.config_path)?;
    let runtime_dir = args.runtime_dir.unwrap_or_else(default_runtime_dir);
    let runtime_paths =
        RuntimePaths::bootstrap(runtime_dir).context("failed to bootstrap runtime paths")?;

    let listen_addr = parse_listen_addr(&config.listen_addr)?;
    let dependencies = Arc::new(build_dependencies(&config));
    let router = GitalyServer::build_router_with_dependencies_runtime_config(
        dependencies,
        &runtime_paths,
        &config.runtime,
    )
    .map_err(map_bootstrap_error)?;

    eprintln!(
        "gitaly-rs listening on {listen_addr} (runtime: {})",
        runtime_paths.pid_dir().display()
    );

    router
        .serve_with_shutdown(listen_addr, async {
            let _ = tokio::signal::ctrl_c().await;
        })
        .await
        .context("gRPC server exited with error")
}

fn load_config(path: &PathBuf) -> Result<Config> {
    let raw = fs::read_to_string(path)
        .with_context(|| format!("failed to read config file `{}`", path.display()))?;
    Config::from_toml(&raw).with_context(|| format!("failed to parse config `{}`", path.display()))
}

fn parse_listen_addr(listen_addr: &str) -> Result<SocketAddr> {
    listen_addr
        .parse::<SocketAddr>()
        .with_context(|| format!("invalid `listen_addr`: `{listen_addr}`"))
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
    use super::CliArgs;

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
}
