//! git command factory with allowlist policy and hardened defaults.

use std::collections::HashSet;
use std::path::Path;

use thiserror::Error;

use crate::command::CommandSpec;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommandWhitelistPolicy {
    allowed_subcommands: HashSet<String>,
}

impl CommandWhitelistPolicy {
    pub fn new<I, S>(allowed_subcommands: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        Self {
            allowed_subcommands: allowed_subcommands.into_iter().map(Into::into).collect(),
        }
    }

    pub fn is_allowed(&self, subcommand: &str) -> bool {
        self.allowed_subcommands.contains(subcommand)
    }
}

impl Default for CommandWhitelistPolicy {
    fn default() -> Self {
        Self::new(["rev-parse", "show-ref", "hash-object", "update-ref"])
    }
}

#[derive(Debug, Clone)]
pub struct GitCommandFactory {
    policy: CommandWhitelistPolicy,
    hardened_env: Vec<(String, String)>,
}

impl GitCommandFactory {
    pub fn new(policy: CommandWhitelistPolicy) -> Self {
        Self {
            policy,
            hardened_env: default_hardened_env(),
        }
    }

    pub fn build_for_repo<I, S>(
        &self,
        repo_path: &Path,
        args: I,
    ) -> Result<CommandSpec, CommandFactoryError>
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        let args: Vec<String> = args.into_iter().map(Into::into).collect();
        let subcommand = args
            .first()
            .ok_or(CommandFactoryError::MissingSubcommand)?
            .clone();

        if !self.policy.is_allowed(&subcommand) {
            return Err(CommandFactoryError::ForbiddenSubcommand { subcommand });
        }

        let mut command_args = Vec::with_capacity(args.len() + 2);
        command_args.push("-C".to_string());
        command_args.push(repo_path.to_string_lossy().into_owned());
        command_args.extend(args);

        Ok(CommandSpec {
            program: "git".to_string(),
            args: command_args,
            env: self.hardened_env.clone(),
        })
    }
}

impl Default for GitCommandFactory {
    fn default() -> Self {
        Self::new(CommandWhitelistPolicy::default())
    }
}

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum CommandFactoryError {
    #[error("git command requires a subcommand")]
    MissingSubcommand,
    #[error("git subcommand `{subcommand}` is not allowed")]
    ForbiddenSubcommand { subcommand: String },
}

fn default_hardened_env() -> Vec<(String, String)> {
    vec![
        ("LANG".to_string(), "en_US.UTF-8".to_string()),
        ("GIT_TERMINAL_PROMPT".to_string(), "0".to_string()),
        ("GIT_CONFIG_GLOBAL".to_string(), "/dev/null".to_string()),
        ("GIT_CONFIG_SYSTEM".to_string(), "/dev/null".to_string()),
        ("XDG_CONFIG_HOME".to_string(), "/dev/null".to_string()),
    ]
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use super::{CommandFactoryError, CommandWhitelistPolicy, GitCommandFactory};

    #[test]
    fn default_policy_allows_known_subcommands() {
        let policy = CommandWhitelistPolicy::default();

        assert!(policy.is_allowed("rev-parse"));
        assert!(policy.is_allowed("show-ref"));
        assert!(policy.is_allowed("hash-object"));
        assert!(policy.is_allowed("update-ref"));
    }

    #[test]
    fn default_policy_rejects_unknown_subcommands() {
        let policy = CommandWhitelistPolicy::default();

        assert!(!policy.is_allowed("clone"));
    }

    #[test]
    fn build_for_repo_includes_repo_flag_and_hardened_env() {
        let factory = GitCommandFactory::default();
        let spec = factory
            .build_for_repo(Path::new("/tmp/repo"), ["rev-parse", "HEAD"])
            .expect("rev-parse is allowlisted");

        assert_eq!(spec.program, "git");
        assert_eq!(
            spec.args,
            vec![
                "-C".to_string(),
                "/tmp/repo".to_string(),
                "rev-parse".to_string(),
                "HEAD".to_string()
            ]
        );
        assert_eq!(
            spec.env,
            vec![
                ("LANG".to_string(), "en_US.UTF-8".to_string()),
                ("GIT_TERMINAL_PROMPT".to_string(), "0".to_string()),
                ("GIT_CONFIG_GLOBAL".to_string(), "/dev/null".to_string()),
                ("GIT_CONFIG_SYSTEM".to_string(), "/dev/null".to_string()),
                ("XDG_CONFIG_HOME".to_string(), "/dev/null".to_string()),
            ]
        );
    }

    #[test]
    fn build_for_repo_rejects_missing_subcommand() {
        let factory = GitCommandFactory::default();
        let err = factory
            .build_for_repo(Path::new("/tmp/repo"), Vec::<&str>::new())
            .expect_err("empty args must be rejected");

        assert!(matches!(err, CommandFactoryError::MissingSubcommand));
    }

    #[test]
    fn build_for_repo_rejects_subcommands_not_in_policy() {
        let factory = GitCommandFactory::default();
        let err = factory
            .build_for_repo(Path::new("/tmp/repo"), ["fetch", "origin"])
            .expect_err("fetch should be rejected by default policy");

        assert_eq!(
            err,
            CommandFactoryError::ForbiddenSubcommand {
                subcommand: "fetch".to_string()
            }
        );
    }
}
