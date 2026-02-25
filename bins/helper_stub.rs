use std::env;
use std::process::ExitCode;

pub(crate) fn run(binary_name: &str) -> ExitCode {
    let mut args = env::args();
    let _program = args.next();
    let maybe_first = args.next();

    match maybe_first.as_deref() {
        Some("--help") | Some("-h") | Some("help") => {
            print_usage(binary_name);
            ExitCode::SUCCESS
        }
        _ => not_implemented(binary_name),
    }
}

fn print_usage(binary_name: &str) {
    println!(
        "usage: {binary_name} [--help]\n\n{binary_name} is a helper binary placeholder.\n"
    );
}

fn not_implemented(binary_name: &str) -> ExitCode {
    eprintln!(
        "{binary_name}: not implemented yet. Use `--help` for basic usage."
    );
    ExitCode::from(1)
}
