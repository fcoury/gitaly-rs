pub mod cpu;
pub mod memory;

use std::path::PathBuf;

use gitaly_cgroups::{CgroupManager, CgroupManagerError, CommandIdentity};

pub trait PressureWatcher {
    fn is_under_pressure(&self) -> bool;
}

pub fn enforce_pressure_policy<W, M>(
    watcher: &W,
    manager: &M,
    command_identity: &CommandIdentity,
) -> Result<Option<PathBuf>, CgroupManagerError>
where
    W: PressureWatcher,
    M: CgroupManager,
{
    if watcher.is_under_pressure() {
        let bucket_path = manager.assign_bucket_path(command_identity)?;
        return Ok(Some(bucket_path));
    }

    Ok(None)
}

#[must_use]
pub fn is_threshold_exceeded(value_percent: f64, threshold_percent: f64) -> bool {
    value_percent >= threshold_percent
}

#[cfg(test)]
mod tests {
    use super::{enforce_pressure_policy, is_threshold_exceeded, PressureWatcher};
    use gitaly_cgroups::{CgroupConfig, CommandIdentity, NoopCgroupManager};
    use std::path::PathBuf;

    struct FakeWatcher {
        under_pressure: bool,
    }

    impl PressureWatcher for FakeWatcher {
        fn is_under_pressure(&self) -> bool {
            self.under_pressure
        }
    }

    #[test]
    fn threshold_exceeded_is_inclusive() {
        assert!(!is_threshold_exceeded(49.9, 50.0));
        assert!(is_threshold_exceeded(50.0, 50.0));
        assert!(is_threshold_exceeded(80.0, 50.0));
    }

    #[test]
    fn enforce_pressure_policy_places_command_into_bucket_when_under_pressure() {
        let watcher = FakeWatcher {
            under_pressure: true,
        };
        let base = PathBuf::from("/tmp/gitaly-limiter-test-cgroups");
        let config = CgroupConfig::new(&base, 8).expect("config should be valid");
        let manager = NoopCgroupManager::new(config);
        let command = CommandIdentity::new("repo-42", "cmd-1");

        let assigned = enforce_pressure_policy(&watcher, &manager, &command)
            .expect("noop cgroup assignment should succeed")
            .expect("under pressure should assign bucket path");

        assert!(assigned.starts_with(base));
    }

    #[test]
    fn enforce_pressure_policy_is_noop_when_not_under_pressure() {
        let watcher = FakeWatcher {
            under_pressure: false,
        };
        let base = PathBuf::from("/tmp/gitaly-limiter-test-cgroups");
        let config = CgroupConfig::new(&base, 8).expect("config should be valid");
        let manager = NoopCgroupManager::new(config);
        let command = CommandIdentity::new("repo-42", "cmd-1");

        let assigned = enforce_pressure_policy(&watcher, &manager, &command)
            .expect("noop cgroup assignment should succeed");
        assert!(assigned.is_none());
    }
}
