use std::env;
use std::process::ExitCode;

use anyhow::{anyhow, Error, Result};

#[derive(Debug, PartialEq, Eq)]
enum Dispatch {
    Help,
    Server(Vec<String>),
    Gateway(Vec<String>),
}

#[tokio::main]
async fn main() -> ExitCode {
    match dispatch_from_args(env::args()).await {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            render_error(&error);
            ExitCode::FAILURE
        }
    }
}

async fn dispatch_from_args<I>(args: I) -> Result<()>
where
    I: IntoIterator<Item = String>,
{
    match parse_dispatch(args)? {
        Dispatch::Help => {
            print_usage();
            Ok(())
        }
        Dispatch::Server(args) => gitaly_rs::run_from_args(args).await,
        Dispatch::Gateway(args) => gitaly_gateway::run_from_args(args).await,
    }
}

fn parse_dispatch<I>(args: I) -> Result<Dispatch>
where
    I: IntoIterator<Item = String>,
{
    let mut iter = args.into_iter();
    let _program = iter.next();
    let Some(subcommand) = iter.next() else {
        return Ok(Dispatch::Help);
    };

    match subcommand.as_str() {
        "help" | "--help" | "-h" => Ok(Dispatch::Help),
        "server" => {
            let mut forwarded = vec!["gitaly-rs".to_string()];
            forwarded.extend(iter);
            Ok(Dispatch::Server(forwarded))
        }
        "gateway" => {
            let mut forwarded = vec!["gitaly-gateway".to_string()];
            forwarded.extend(iter);
            Ok(Dispatch::Gateway(forwarded))
        }
        _ => Err(anyhow!(
            "unknown subcommand `{subcommand}`; expected `server` or `gateway`"
        )),
    }
}

fn print_usage() {
    eprintln!(
        "usage: gitaly <subcommand> [options]\n\nsubcommands:\n  server   run gitaly-rs\n  gateway  run gitaly-gateway\n\nexamples:\n  gitaly server --config /path/to/gitaly.toml\n  gitaly gateway --config /path/to/gateway.toml"
    );
}

fn render_error(error: &Error) {
    eprintln!("Error: {error}");

    let mut chain = error.chain();
    let _ = chain.next();
    if let Some(cause) = chain.next() {
        eprintln!("Cause: {cause}");
    }

    if debug_enabled() {
        for cause in chain {
            eprintln!("Cause: {cause}");
        }
    }

    if let Some(hint) = error_hint(error) {
        eprintln!("Hint: {hint}");
    }
}

fn debug_enabled() -> bool {
    matches!(
        env::var("GITALY_DEBUG")
            .ok()
            .as_deref()
            .map(str::to_ascii_lowercase)
            .as_deref(),
        Some("1" | "true" | "yes" | "on")
    )
}

fn error_hint(error: &Error) -> Option<&'static str> {
    let error_text = error
        .chain()
        .map(ToString::to_string)
        .collect::<Vec<_>>()
        .join(" | ")
        .to_ascii_lowercase();

    if error_text.contains("hint:") {
        return None;
    }

    if error_text.contains("duplicate key") {
        return Some(
            "remove duplicate TOML tables or keys from your config (for example duplicate `[auth]`).",
        );
    }

    if error_text.contains("invalid type: string") && error_text.contains("expected a sequence") {
        return Some(
            "array fields must use TOML arrays, for example `client_tokens = [\"token\"]`.",
        );
    }

    if error_text.contains("connection refused") {
        return Some(
            "start the server first and ensure `gitaly_addr` points to the active `listen_addr`.",
        );
    }

    if error_text.contains("address already in use") {
        return Some("another process is already bound to this address; free the port or change the listen address.");
    }

    None
}

#[cfg(test)]
mod tests {
    use super::{error_hint, parse_dispatch, Dispatch};
    use anyhow::anyhow;

    #[test]
    fn parse_help_when_missing_subcommand() {
        let args = vec!["gitaly".to_string()];
        let parsed = parse_dispatch(args).expect("parse should succeed");
        assert_eq!(parsed, Dispatch::Help);
    }

    #[test]
    fn parse_server_subcommand_forwards_remaining_args() {
        let args = vec![
            "gitaly".to_string(),
            "server".to_string(),
            "--config".to_string(),
            "/tmp/gitaly.toml".to_string(),
        ];

        let parsed = parse_dispatch(args).expect("parse should succeed");
        assert_eq!(
            parsed,
            Dispatch::Server(vec![
                "gitaly-rs".to_string(),
                "--config".to_string(),
                "/tmp/gitaly.toml".to_string(),
            ])
        );
    }

    #[test]
    fn parse_gateway_subcommand_forwards_remaining_args() {
        let args = vec![
            "gitaly".to_string(),
            "gateway".to_string(),
            "--config".to_string(),
            "/tmp/gateway.toml".to_string(),
        ];

        let parsed = parse_dispatch(args).expect("parse should succeed");
        assert_eq!(
            parsed,
            Dispatch::Gateway(vec![
                "gitaly-gateway".to_string(),
                "--config".to_string(),
                "/tmp/gateway.toml".to_string(),
            ])
        );
    }

    #[test]
    fn parse_rejects_unknown_subcommand() {
        let args = vec!["gitaly".to_string(), "unknown".to_string()];
        let error = parse_dispatch(args).expect_err("parse should fail");
        assert!(error
            .to_string()
            .contains("unknown subcommand `unknown`; expected `server` or `gateway`"));
    }

    #[test]
    fn duplicate_key_errors_provide_config_hint() {
        let error = anyhow!("duplicate key `auth` in document root");
        assert_eq!(
            error_hint(&error),
            Some(
                "remove duplicate TOML tables or keys from your config (for example duplicate `[auth]`).",
            )
        );
    }

    #[test]
    fn connection_refused_errors_provide_startup_hint() {
        let error = anyhow!("tcp connect error: Connection refused");
        assert_eq!(
            error_hint(&error),
            Some("start the server first and ensure `gitaly_addr` points to the active `listen_addr`.")
        );
    }
}
