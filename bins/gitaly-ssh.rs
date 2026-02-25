#[path = "helper_stub.rs"]
mod helper_stub;

fn main() -> std::process::ExitCode {
    helper_stub::run("gitaly-ssh")
}
