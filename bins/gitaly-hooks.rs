use std::env;
use std::path::{Component, Path, PathBuf};
use std::process::{Command, ExitCode, ExitStatus};

fn main() -> ExitCode {
    match run() {
        Ok(RunOutcome::Status(status)) => exit_from_status(status),
        Ok(RunOutcome::Help) => ExitCode::SUCCESS,
        Err(RunError::Usage(error)) => {
            eprintln!("gitaly-hooks: {error}");
            eprintln!("gitaly-hooks: try `--help` for usage");
            ExitCode::from(2)
        }
        Err(RunError::Execution(error)) => {
            eprintln!("gitaly-hooks: {error}");
            ExitCode::from(1)
        }
    }
}

enum RunOutcome {
    Help,
    Status(ExitStatus),
}

enum RunError {
    Usage(String),
    Execution(String),
}

fn run() -> Result<RunOutcome, RunError> {
    let mut hooks_dir: Option<PathBuf> = None;
    let mut args = env::args().skip(1).peekable();

    while let Some(arg) = args.peek().cloned() {
        if arg == "--help" || arg == "-h" {
            print_help();
            return Ok(RunOutcome::Help);
        }
        if arg == "--hooks-dir" {
            let _ = args.next();
            let value = args
                .next()
                .ok_or_else(|| RunError::Usage(String::from("missing value for `--hooks-dir`")))?;
            hooks_dir = Some(PathBuf::from(value));
            continue;
        }
        if let Some(value) = arg.strip_prefix("--hooks-dir=") {
            let _ = args.next();
            hooks_dir = Some(PathBuf::from(value));
            continue;
        }
        break;
    }

    let hook_name = args
        .next()
        .ok_or_else(|| RunError::Usage(String::from("missing hook name")))?;
    validate_hook_name(&hook_name)?;

    let hook_args: Vec<String> = args.collect();
    let hooks_dir = hooks_dir
        .or_else(|| env::var_os("GITALY_HOOKS_DIR").map(PathBuf::from))
        .unwrap_or_else(|| PathBuf::from("./custom_hooks"));
    let hook_path = hooks_dir.join(hook_name);

    let status = Command::new(&hook_path)
        .args(&hook_args)
        .status()
        .map_err(|source| {
            RunError::Execution(format!("failed to run '{}': {source}", hook_path.display()))
        })?;
    Ok(RunOutcome::Status(status))
}

fn validate_hook_name(name: &str) -> Result<(), RunError> {
    let mut components = Path::new(name).components();
    let first = components.next();
    if components.next().is_some() {
        return Err(RunError::Usage(format!(
            "invalid hook name '{name}': must be a single path segment"
        )));
    }
    match first {
        Some(Component::Normal(_)) => Ok(()),
        _ => Err(RunError::Usage(format!("invalid hook name '{name}'"))),
    }
}

fn print_help() {
    println!(
        "Usage: gitaly-hooks [--hooks-dir <dir>] <hook-name> [hook-args...]\n\
         \n\
         Run a hook executable from `--hooks-dir`, `GITALY_HOOKS_DIR`, or\n\
         `./custom_hooks`."
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
