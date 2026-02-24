use anyhow::{anyhow, Result};

#[derive(Debug, PartialEq, Eq)]
enum Dispatch {
    Help,
    Server(Vec<String>),
    Gateway(Vec<String>),
}

#[tokio::main]
async fn main() -> Result<()> {
    dispatch_from_args(std::env::args()).await
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

#[cfg(test)]
mod tests {
    use super::{parse_dispatch, Dispatch};

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
}
