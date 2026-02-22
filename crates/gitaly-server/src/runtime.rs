use std::env;
use std::fs;
use std::io;
#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
#[cfg(unix)]
use std::process::{Command, Stdio};

const PID_DIR_PREFIX: &str = "gitaly-";
const INTERNAL_SOCKET_DIR_NAME: &str = "sock.d";
const INTERNAL_SOCKET_NAME: &str = "intern";
const CGROUP_DIR_NAME: &str = "cgroup.d";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuntimePaths {
    process_id: u32,
    base_dir: PathBuf,
    pid_dir: PathBuf,
}

impl RuntimePaths {
    pub fn bootstrap(base_dir: impl Into<PathBuf>) -> io::Result<Self> {
        Self::bootstrap_for_pid(base_dir, std::process::id())
    }

    pub fn bootstrap_for_pid(base_dir: impl Into<PathBuf>, process_id: u32) -> io::Result<Self> {
        let base_dir = base_dir.into();
        fs::create_dir_all(&base_dir)?;

        prune_stale_pid_dirs(&base_dir, process_id);

        let pid_dir = pid_dir_for(&base_dir, process_id);
        if pid_dir.exists() {
            fs::remove_dir_all(&pid_dir)?;
        }
        fs::create_dir_all(pid_dir.join(INTERNAL_SOCKET_DIR_NAME))?;

        Ok(Self {
            process_id,
            base_dir,
            pid_dir,
        })
    }

    #[must_use]
    pub fn process_id(&self) -> u32 {
        self.process_id
    }

    #[must_use]
    pub fn base_dir(&self) -> &Path {
        &self.base_dir
    }

    #[must_use]
    pub fn pid_dir(&self) -> &Path {
        &self.pid_dir
    }

    #[must_use]
    pub fn internal_socket_dir(&self) -> PathBuf {
        self.pid_dir.join(INTERNAL_SOCKET_DIR_NAME)
    }

    #[must_use]
    pub fn internal_socket_path(&self) -> PathBuf {
        self.internal_socket_dir().join(INTERNAL_SOCKET_NAME)
    }

    #[must_use]
    pub fn cgroup_base_dir(&self) -> PathBuf {
        self.pid_dir.join(CGROUP_DIR_NAME)
    }
}

#[must_use]
pub fn pid_dir_for(base_dir: &Path, process_id: u32) -> PathBuf {
    base_dir.join(format!("{PID_DIR_PREFIX}{process_id}"))
}

pub fn prune_stale_pid_dirs(base_dir: &Path, current_process_id: u32) {
    let Ok(entries) = fs::read_dir(base_dir) else {
        return;
    };

    for entry in entries.filter_map(Result::ok) {
        let Ok(file_type) = entry.file_type() else {
            continue;
        };
        if !file_type.is_dir() {
            continue;
        }

        let Some(entry_name) = entry.file_name().to_str().map(ToOwned::to_owned) else {
            continue;
        };
        let Some(pid) = parse_pid_dir_name(&entry_name) else {
            continue;
        };
        if pid == current_process_id || is_pid_alive(pid) {
            continue;
        }

        let _ = fs::remove_dir_all(entry.path());
    }
}

#[must_use]
pub fn discover_auxiliary_binary(name: &str, search_paths: &[PathBuf]) -> Option<PathBuf> {
    for search_path in search_paths {
        let candidate = search_path.join(name);
        if is_executable_file(&candidate) {
            return Some(candidate);
        }
    }

    let path = env::var_os("PATH")?;
    for directory in env::split_paths(&path) {
        let candidate = directory.join(name);
        if is_executable_file(&candidate) {
            return Some(candidate);
        }
    }

    None
}

fn parse_pid_dir_name(name: &str) -> Option<u32> {
    name.strip_prefix(PID_DIR_PREFIX)?.parse::<u32>().ok()
}

fn is_pid_alive(process_id: u32) -> bool {
    if process_id == 0 {
        return false;
    }

    #[cfg(unix)]
    {
        let proc_dir = Path::new("/proc");
        if proc_dir.is_dir() {
            return proc_dir.join(process_id.to_string()).exists();
        }

        return Command::new("kill")
            .arg("-0")
            .arg(process_id.to_string())
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .is_ok_and(|status| status.success());
    }

    #[cfg(not(unix))]
    {
        let _ = process_id;
        false
    }
}

fn is_executable_file(path: &Path) -> bool {
    let Ok(metadata) = fs::metadata(path) else {
        return false;
    };
    if !metadata.is_file() {
        return false;
    }

    #[cfg(unix)]
    {
        metadata.permissions().mode() & 0o111 != 0
    }

    #[cfg(not(unix))]
    {
        true
    }
}

#[cfg(test)]
mod tests {
    use std::ffi::OsString;
    use std::io::Write;
    use std::path::{Path, PathBuf};
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::Mutex;

    use super::{discover_auxiliary_binary, RuntimePaths};

    static ENV_LOCK: Mutex<()> = Mutex::new(());

    #[test]
    fn bootstrap_creates_pid_scoped_runtime_paths() {
        let temp = TempDir::new("runtime-bootstrap");
        let runtime = RuntimePaths::bootstrap_for_pid(temp.path().to_path_buf(), 42)
            .expect("runtime bootstrap should succeed");

        assert_eq!(runtime.process_id(), 42);
        assert_eq!(runtime.base_dir(), temp.path());
        assert_eq!(runtime.pid_dir(), temp.path().join("gitaly-42"));
        assert!(runtime.pid_dir().is_dir());
        assert!(runtime.internal_socket_dir().is_dir());
        assert_eq!(
            runtime.internal_socket_path(),
            temp.path().join("gitaly-42/sock.d/intern")
        );
        assert_eq!(
            runtime.cgroup_base_dir(),
            temp.path().join("gitaly-42/cgroup.d")
        );
    }

    #[test]
    fn bootstrap_prunes_stale_pid_directories() {
        let temp = TempDir::new("runtime-prune");
        let stale_pid = u32::MAX;
        let live_pid = std::process::id();
        let current_pid = live_pid.saturating_add(1);
        let stale_dir = temp.path().join(format!("gitaly-{stale_pid}"));
        let live_pid_dir = temp.path().join(format!("gitaly-{live_pid}"));
        let current_dir = temp.path().join(format!("gitaly-{current_pid}"));
        let unrelated_dir = temp.path().join("keep-me");

        std::fs::create_dir_all(&stale_dir).expect("stale dir should be created");
        std::fs::create_dir_all(&live_pid_dir).expect("live pid dir should be created");
        std::fs::create_dir_all(&current_dir).expect("current dir should be created");
        std::fs::create_dir_all(&unrelated_dir).expect("unrelated dir should be created");
        std::fs::write(current_dir.join("old.txt"), b"old state").expect("old file should exist");

        let runtime = RuntimePaths::bootstrap_for_pid(temp.path().to_path_buf(), current_pid)
            .expect("runtime bootstrap should succeed");

        assert_eq!(runtime.pid_dir(), current_dir);
        assert!(!stale_dir.exists());
        assert!(live_pid_dir.exists());
        assert!(current_dir.exists());
        assert!(!current_dir.join("old.txt").exists());
        assert!(unrelated_dir.exists());
    }

    #[test]
    fn discover_auxiliary_binary_prefers_explicit_paths_then_falls_back_to_path() {
        let _env_guard = ENV_LOCK
            .lock()
            .expect("environment guard should lock for test isolation");

        let temp = TempDir::new("runtime-discovery");
        let explicit_dir = temp.path().join("explicit");
        let path_dir = temp.path().join("path");
        std::fs::create_dir_all(&explicit_dir).expect("explicit dir should be created");
        std::fs::create_dir_all(&path_dir).expect("path dir should be created");

        let hooks_in_explicit = write_executable(&explicit_dir, "gitaly-hooks");
        let hooks_in_path = write_executable(&path_dir, "gitaly-hooks");
        let ssh_in_path = write_executable(&path_dir, "gitaly-ssh");

        let previous_path = std::env::var_os("PATH");
        std::env::set_var("PATH", OsString::from(path_dir.as_os_str()));

        let hooks = discover_auxiliary_binary("gitaly-hooks", &[explicit_dir.clone()])
            .expect("explicit hooks binary should be found");
        assert_eq!(hooks, hooks_in_explicit);
        assert_ne!(hooks, hooks_in_path);

        let ssh = discover_auxiliary_binary("gitaly-ssh", &[explicit_dir])
            .expect("ssh binary should be found via PATH fallback");
        assert_eq!(ssh, ssh_in_path);

        match previous_path {
            Some(path) => std::env::set_var("PATH", path),
            None => std::env::remove_var("PATH"),
        }
    }

    fn write_executable(directory: &Path, name: &str) -> PathBuf {
        let path = directory.join(name);
        let mut file = std::fs::File::create(&path).expect("binary placeholder should be created");
        writeln!(file, "#!/bin/sh").expect("script should be writable");
        writeln!(file, "exit 0").expect("script should be writable");

        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mut permissions = file
                .metadata()
                .expect("metadata should be readable")
                .permissions();
            permissions.set_mode(0o755);
            std::fs::set_permissions(&path, permissions).expect("binary should be executable");
        }

        path
    }

    struct TempDir {
        path: PathBuf,
    }

    impl TempDir {
        fn new(prefix: &str) -> Self {
            static NEXT_ID: AtomicU64 = AtomicU64::new(0);
            let id = NEXT_ID.fetch_add(1, Ordering::Relaxed);
            let path = std::env::temp_dir().join(format!(
                "gitaly-server-{prefix}-{}-{id}",
                std::process::id()
            ));
            std::fs::create_dir_all(&path).expect("temp directory should be creatable");
            Self { path }
        }

        fn path(&self) -> &Path {
            &self.path
        }
    }

    impl Drop for TempDir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.path);
        }
    }
}
