//! command execution abstractions for Git subprocesses.

use std::process::{Output, Stdio};
use std::time::Duration;
use std::{future::poll_fn, pin::Pin};

use thiserror::Error;
use tokio::io::AsyncWrite;
use tokio::process::{Child, Command};

const MAX_STDERR_BYTES: usize = 10 * 1024;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommandSpec {
    pub program: String,
    pub args: Vec<String>,
    pub env: Vec<(String, String)>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommandOutput {
    pub status_code: Option<i32>,
    pub stdout: Vec<u8>,
    pub stderr: Vec<u8>,
}

#[derive(Debug, Error)]
pub enum CommandError {
    #[error("failed to spawn `{program}`: {source}")]
    Spawn {
        program: String,
        #[source]
        source: std::io::Error,
    },
    #[error("timed out after {timeout:?} while running `{program}`")]
    Timeout { program: String, timeout: Duration },
    #[error("failed waiting for `{program}`: {source}")]
    Wait {
        program: String,
        #[source]
        source: std::io::Error,
    },
}

#[derive(Debug, Default, Clone, Copy)]
pub struct CommandRunner;

impl CommandRunner {
    pub async fn run(&self, spec: &CommandSpec) -> Result<CommandOutput, CommandError> {
        self.run_inner(spec, None, None).await
    }

    pub async fn run_with_timeout(
        &self,
        spec: &CommandSpec,
        timeout: Duration,
    ) -> Result<CommandOutput, CommandError> {
        self.run_inner(spec, None, Some(timeout)).await
    }

    pub async fn run_with_input(
        &self,
        spec: &CommandSpec,
        stdin: &[u8],
    ) -> Result<CommandOutput, CommandError> {
        self.run_inner(spec, Some(stdin), None).await
    }

    pub async fn run_with_input_timeout(
        &self,
        spec: &CommandSpec,
        stdin: &[u8],
        timeout: Duration,
    ) -> Result<CommandOutput, CommandError> {
        self.run_inner(spec, Some(stdin), Some(timeout)).await
    }

    async fn run_inner(
        &self,
        spec: &CommandSpec,
        stdin: Option<&[u8]>,
        timeout: Option<Duration>,
    ) -> Result<CommandOutput, CommandError> {
        let mut command = Command::new(&spec.program);
        command
            .args(&spec.args)
            .envs(spec.env.iter().map(|(name, value)| (name, value)))
            .stdin(if stdin.is_some() {
                Stdio::piped()
            } else {
                Stdio::null()
            })
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true);

        let mut child = command.spawn().map_err(|source| CommandError::Spawn {
            program: spec.program.clone(),
            source,
        })?;

        if let Some(stdin_payload) = stdin {
            write_stdin_payload(spec, &mut child, stdin_payload).await?;
        }

        let mut output = if let Some(timeout) = timeout {
            wait_with_timeout(spec, child, timeout).await?
        } else {
            wait_for_output(spec, child).await?
        };

        truncate_stderr_tail(&mut output.stderr);

        Ok(CommandOutput {
            status_code: output.status.code(),
            stdout: output.stdout,
            stderr: output.stderr,
        })
    }
}

async fn write_stdin_payload(
    spec: &CommandSpec,
    child: &mut Child,
    payload: &[u8],
) -> Result<(), CommandError> {
    let mut child_stdin = child.stdin.take().ok_or_else(|| CommandError::Wait {
        program: spec.program.clone(),
        source: std::io::Error::new(
            std::io::ErrorKind::BrokenPipe,
            "stdin was not piped for child process",
        ),
    })?;

    write_all_and_shutdown(&mut child_stdin, payload)
        .await
        .map_err(|source| CommandError::Wait {
            program: spec.program.clone(),
            source,
        })
}

async fn write_all_and_shutdown(
    child_stdin: &mut tokio::process::ChildStdin,
    payload: &[u8],
) -> std::io::Result<()> {
    let mut offset = 0;
    while offset < payload.len() {
        let written =
            poll_fn(|cx| Pin::new(&mut *child_stdin).poll_write(cx, &payload[offset..])).await?;
        if written == 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::WriteZero,
                "failed to write full stdin payload",
            ));
        }
        offset += written;
    }

    poll_fn(|cx| Pin::new(&mut *child_stdin).poll_shutdown(cx)).await
}

async fn wait_for_output(spec: &CommandSpec, child: Child) -> Result<Output, CommandError> {
    match child.wait_with_output().await {
        Ok(output) => Ok(output),
        Err(source) => Err(CommandError::Wait {
            program: spec.program.clone(),
            source,
        }),
    }
}

async fn wait_with_timeout(
    spec: &CommandSpec,
    child: Child,
    deadline: Duration,
) -> Result<Output, CommandError> {
    match tokio::time::timeout(deadline, child.wait_with_output()).await {
        Ok(wait_result) => wait_result.map_err(|source| CommandError::Wait {
            program: spec.program.clone(),
            source,
        }),
        Err(_) => Err(CommandError::Timeout {
            program: spec.program.clone(),
            timeout: deadline,
        }),
    }
}

fn truncate_stderr_tail(stderr: &mut Vec<u8>) {
    if stderr.len() > MAX_STDERR_BYTES {
        stderr.truncate(MAX_STDERR_BYTES);
    }
}

#[cfg(test)]
mod tests {
    use super::{CommandError, CommandRunner, CommandSpec, MAX_STDERR_BYTES};
    use std::time::Duration;

    #[tokio::test]
    async fn run_returns_successful_output() {
        let runner = CommandRunner;
        let output = runner
            .run(&helper_spec("success"))
            .await
            .expect("success mode should run");

        assert_eq!(output.status_code, Some(0));
        assert!(bytes_contain(&output.stdout, "helper:success:stdout"));
        assert!(bytes_contain(&output.stderr, "helper:success:stderr"));
    }

    #[tokio::test]
    async fn run_returns_output_for_non_zero_exit() {
        let runner = CommandRunner;
        let output = runner
            .run(&helper_spec("nonzero"))
            .await
            .expect("non-zero exits should not be hard errors");

        assert_eq!(output.status_code, Some(23));
        assert!(bytes_contain(&output.stderr, "helper:nonzero:stderr"));
    }

    #[tokio::test]
    async fn run_with_timeout_returns_timeout_error() {
        let runner = CommandRunner;
        let err = runner
            .run_with_timeout(&helper_spec("sleep"), Duration::from_millis(100))
            .await
            .expect_err("sleep mode should time out");

        match err {
            CommandError::Timeout { timeout, .. } => {
                assert_eq!(timeout, Duration::from_millis(100));
            }
            other => panic!("expected timeout error, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn run_truncates_stderr_to_10kb() {
        let runner = CommandRunner;
        let output = runner
            .run(&helper_spec("stderr_big"))
            .await
            .expect("stderr_big mode should run");

        assert_eq!(output.status_code, Some(0));
        assert_eq!(output.stderr.len(), MAX_STDERR_BYTES);
        assert!(bytes_contain(&output.stderr, "helper:stderr_big:begin"));
        assert!(!bytes_contain(&output.stderr, "helper:stderr_big:end"));
    }

    #[tokio::test]
    async fn run_with_input_writes_stdin_payload() {
        let runner = CommandRunner;
        let output = runner
            .run_with_input(&helper_spec("cat"), b"hello from stdin")
            .await
            .expect("cat mode should echo stdin");

        assert_eq!(output.status_code, Some(0));
        assert_eq!(output.stdout, b"hello from stdin");
    }

    #[tokio::test]
    async fn run_with_input_timeout_writes_stdin_payload() {
        let runner = CommandRunner;
        let output = runner
            .run_with_input_timeout(
                &helper_spec("cat"),
                b"echo with timeout",
                Duration::from_millis(100),
            )
            .await
            .expect("cat mode should echo stdin with timeout");

        assert_eq!(output.status_code, Some(0));
        assert_eq!(output.stdout, b"echo with timeout");
    }

    #[tokio::test]
    async fn run_applies_spec_environment_variables() {
        let runner = CommandRunner;
        let mut spec = helper_spec("print_env");
        spec.env = vec![("TEST_ENV_VALUE".to_string(), "env-ok".to_string())];

        let output = runner.run(&spec).await.expect("print_env mode should run");

        assert_eq!(output.status_code, Some(0));
        assert_eq!(output.stdout, b"env-ok");
    }

    fn helper_spec(mode: &str) -> CommandSpec {
        let command = match mode {
            "success" => "printf 'helper:success:stdout'; printf 'helper:success:stderr' >&2",
            "nonzero" => "printf 'helper:nonzero:stderr' >&2; exit 23",
            "sleep" => "sleep 5",
            "cat" => "cat",
            "stderr_big" => {
                "python3 -c \"import sys; sys.stderr.write('helper:stderr_big:begin:' + 'x'*13000 + ':helper:stderr_big:end')\""
            }
            "print_env" => "printf '%s' \"$TEST_ENV_VALUE\"",
            other => panic!("unexpected helper mode: {other}"),
        };

        CommandSpec {
            program: "sh".to_string(),
            args: vec!["-c".to_string(), command.to_string()],
            env: Vec::new(),
        }
    }

    fn bytes_contain(bytes: &[u8], needle: &str) -> bool {
        String::from_utf8_lossy(bytes).contains(needle)
    }
}
