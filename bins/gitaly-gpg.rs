use std::env;
use std::ffi::OsString;
use std::io::ErrorKind;
use std::process::{Command, ExitCode, ExitStatus};

fn main() -> ExitCode {
    let args: Vec<OsString> = env::args_os().skip(1).collect();
    if is_help(&args) {
        print_help();
        return ExitCode::SUCCESS;
    }

    let binary = env::var_os("GITALY_GPG_BIN").unwrap_or_else(|| OsString::from("gpg"));
    match Command::new(&binary).args(&args).status() {
        Ok(status) => exit_from_status(status),
        Err(source) => {
            eprintln!(
                "gitaly-gpg: failed to execute '{}': {source}",
                binary.to_string_lossy()
            );
            if source.kind() == ErrorKind::NotFound {
                ExitCode::from(127)
            } else {
                ExitCode::from(1)
            }
        }
    }
}

fn is_help(args: &[OsString]) -> bool {
    args.len() == 1 && matches!(args[0].to_str(), Some("--help" | "-h"))
}

fn print_help() {
    println!(
        "Usage: gitaly-gpg [gpg-args...]\n\
         \n\
         Execute `gpg` with passthrough arguments. Override binary with\n\
         `GITALY_GPG_BIN`."
    );
}

fn exit_from_status(status: ExitStatus) -> ExitCode {
    if let Some(code) = status.code() {
        return ExitCode::from(u8::try_from(code).unwrap_or(1));
    }
    #[cfg(unix)]
    {
        use std::os::unix::process::ExitStatusExt;
        if let Some(signal) = status.signal() {
            let code = (128_i32 + signal).clamp(0, 255) as u8;
            return ExitCode::from(code);
        }
    }
    ExitCode::from(1)
}
