use std::env;
use std::io;
use std::net::{TcpStream, ToSocketAddrs};
use std::process::ExitCode;
use std::time::Duration;

fn main() -> ExitCode {
    match parse_command(env::args().skip(1)) {
        Ok(Command::Help) => {
            print_help();
            ExitCode::SUCCESS
        }
        Ok(Command::TcpCheck { addr, timeout_ms }) => match tcp_check(&addr, timeout_ms) {
            Ok(()) => ExitCode::SUCCESS,
            Err(error) => {
                eprintln!("gitaly-blackbox: tcp-check failed for '{addr}': {error}");
                ExitCode::from(1)
            }
        },
        Err(error) => {
            eprintln!("gitaly-blackbox: {error}");
            eprintln!("gitaly-blackbox: try `--help` for usage");
            ExitCode::from(2)
        }
    }
}

enum Command {
    Help,
    TcpCheck { addr: String, timeout_ms: u64 },
}

fn parse_command(args: impl Iterator<Item = String>) -> Result<Command, String> {
    let mut args = args.peekable();
    match args.next().as_deref() {
        None | Some("--help") | Some("-h") | Some("help") => Ok(Command::Help),
        Some("tcp-check") => parse_tcp_check(args),
        Some(other) => Err(format!("unknown subcommand '{other}'")),
    }
}

fn parse_tcp_check(args: impl Iterator<Item = String>) -> Result<Command, String> {
    let mut addr: Option<String> = None;
    let mut timeout_ms: u64 = 1_000;
    let mut args = args.peekable();

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--addr" => {
                addr = Some(
                    args.next()
                        .ok_or_else(|| String::from("missing value for `--addr`"))?,
                );
            }
            "--timeout-ms" => {
                let value = args
                    .next()
                    .ok_or_else(|| String::from("missing value for `--timeout-ms`"))?;
                timeout_ms = value
                    .parse()
                    .map_err(|_| format!("invalid `--timeout-ms` value '{value}'"))?;
            }
            value if value.starts_with("--addr=") => {
                addr = Some(value.trim_start_matches("--addr=").to_string());
            }
            value if value.starts_with("--timeout-ms=") => {
                let value = value.trim_start_matches("--timeout-ms=");
                timeout_ms = value
                    .parse()
                    .map_err(|_| format!("invalid `--timeout-ms` value '{value}'"))?;
            }
            "--help" | "-h" => return Ok(Command::Help),
            other => return Err(format!("unknown argument for `tcp-check`: '{other}'")),
        }
    }

    let addr = addr.ok_or_else(|| String::from("`tcp-check` requires `--addr <host:port>`"))?;
    Ok(Command::TcpCheck { addr, timeout_ms })
}

fn tcp_check(addr: &str, timeout_ms: u64) -> io::Result<()> {
    let timeout = Duration::from_millis(timeout_ms);
    let mut last_error: Option<io::Error> = None;

    for socket_addr in addr.to_socket_addrs()? {
        match TcpStream::connect_timeout(&socket_addr, timeout) {
            Ok(_) => return Ok(()),
            Err(error) => last_error = Some(error),
        }
    }

    Err(last_error
        .unwrap_or_else(|| io::Error::new(io::ErrorKind::AddrNotAvailable, "no address found")))
}

fn print_help() {
    println!(
        "Usage:\n\
           gitaly-blackbox tcp-check --addr <host:port> [--timeout-ms <ms>]\n\
         \n\
         Probe TCP connectivity to a host:port endpoint."
    );
}
