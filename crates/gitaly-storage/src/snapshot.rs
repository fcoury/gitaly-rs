use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum SnapshotError {
    #[error("source directory '{path}' does not exist")]
    SourceMissing { path: PathBuf },
    #[error("source path '{path}' is not a directory")]
    SourceNotDirectory { path: PathBuf },
    #[error("destination path '{path}' exists and is not a directory")]
    DestinationNotDirectory { path: PathBuf },
    #[error("snapshot directory '{path}' does not exist")]
    SnapshotMissing { path: PathBuf },
    #[error("snapshot path '{path}' is not a directory")]
    SnapshotNotDirectory { path: PathBuf },
    #[error("target path '{path}' exists and is not a directory")]
    TargetNotDirectory { path: PathBuf },
    #[error("unsupported filesystem entry at '{path}'")]
    UnsupportedEntry { path: PathBuf },
    #[error("i/o error while {operation} at '{path}': {source}")]
    Io {
        operation: &'static str,
        path: PathBuf,
        #[source]
        source: io::Error,
    },
    #[error("failed to restore original target after copy error at '{path}': {source}")]
    Rollback {
        path: PathBuf,
        #[source]
        source: io::Error,
    },
}

pub fn create_snapshot(
    source_dir: impl AsRef<Path>,
    destination_dir: impl AsRef<Path>,
) -> Result<(), SnapshotError> {
    let source_dir = source_dir.as_ref();
    let destination_dir = destination_dir.as_ref();
    ensure_source_directory(source_dir)?;
    ensure_destination_directory_or_absent(destination_dir)?;

    replace_directory_safely(source_dir, destination_dir, "create snapshot")
}

pub fn restore_snapshot(
    snapshot_dir: impl AsRef<Path>,
    target_dir: impl AsRef<Path>,
) -> Result<(), SnapshotError> {
    let snapshot_dir = snapshot_dir.as_ref();
    let target_dir = target_dir.as_ref();
    ensure_snapshot_directory(snapshot_dir)?;
    ensure_target_directory_or_absent(target_dir)?;

    replace_directory_safely(snapshot_dir, target_dir, "restore snapshot")
}

fn replace_directory_safely(
    source_dir: &Path,
    destination_dir: &Path,
    copy_operation: &'static str,
) -> Result<(), SnapshotError> {
    let backup_dir = if destination_dir.exists() {
        Some(move_to_backup(destination_dir)?)
    } else {
        None
    };

    let copy_result = copy_directory_recursive(source_dir, destination_dir, copy_operation);
    match copy_result {
        Ok(()) => {
            if let Some(backup_dir) = backup_dir {
                fs::remove_dir_all(&backup_dir).map_err(|source| SnapshotError::Io {
                    operation: "remove backup directory",
                    path: backup_dir,
                    source,
                })?;
            }
            Ok(())
        }
        Err(copy_error) => {
            if destination_dir.exists() {
                let _ = fs::remove_dir_all(destination_dir);
            }

            if let Some(backup_dir) = backup_dir {
                fs::rename(&backup_dir, destination_dir).map_err(|source| {
                    SnapshotError::Rollback {
                        path: destination_dir.to_path_buf(),
                        source,
                    }
                })?;
            }

            Err(copy_error)
        }
    }
}

fn move_to_backup(directory: &Path) -> Result<PathBuf, SnapshotError> {
    let parent = directory.parent().unwrap_or_else(|| Path::new("."));
    let name = directory
        .file_name()
        .map_or_else(|| "snapshot-target".into(), |value| value.to_os_string());
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let process_id = std::process::id();

    for attempt in 0..100_u32 {
        let backup_name = format!(
            ".snapshot-backup-{}-{timestamp}-{process_id}-{attempt}",
            name.to_string_lossy()
        );
        let backup_dir = parent.join(backup_name);
        if backup_dir.exists() {
            continue;
        }

        fs::rename(directory, &backup_dir).map_err(|source| SnapshotError::Io {
            operation: "move directory to backup",
            path: directory.to_path_buf(),
            source,
        })?;

        return Ok(backup_dir);
    }

    Err(SnapshotError::Io {
        operation: "create unique backup path",
        path: directory.to_path_buf(),
        source: io::Error::new(io::ErrorKind::AlreadyExists, "backup path collision"),
    })
}

fn copy_directory_recursive(
    source_dir: &Path,
    destination_dir: &Path,
    copy_operation: &'static str,
) -> Result<(), SnapshotError> {
    fs::create_dir_all(destination_dir).map_err(|source| SnapshotError::Io {
        operation: "create destination directory",
        path: destination_dir.to_path_buf(),
        source,
    })?;

    for entry in fs::read_dir(source_dir).map_err(|source| SnapshotError::Io {
        operation: "read source directory",
        path: source_dir.to_path_buf(),
        source,
    })? {
        let entry = entry.map_err(|source| SnapshotError::Io {
            operation: "read directory entry",
            path: source_dir.to_path_buf(),
            source,
        })?;
        let source_path = entry.path();
        let destination_path = destination_dir.join(entry.file_name());
        let file_type = entry.file_type().map_err(|source| SnapshotError::Io {
            operation: "read entry type",
            path: source_path.clone(),
            source,
        })?;

        if file_type.is_dir() {
            copy_directory_recursive(&source_path, &destination_path, copy_operation)?;
            continue;
        }

        if file_type.is_file() {
            fs::copy(&source_path, &destination_path).map_err(|source| SnapshotError::Io {
                operation: copy_operation,
                path: source_path,
                source,
            })?;
            continue;
        }

        return Err(SnapshotError::UnsupportedEntry { path: source_path });
    }

    Ok(())
}

fn ensure_source_directory(source_dir: &Path) -> Result<(), SnapshotError> {
    if !source_dir.exists() {
        return Err(SnapshotError::SourceMissing {
            path: source_dir.to_path_buf(),
        });
    }
    if !source_dir.is_dir() {
        return Err(SnapshotError::SourceNotDirectory {
            path: source_dir.to_path_buf(),
        });
    }
    Ok(())
}

fn ensure_destination_directory_or_absent(destination_dir: &Path) -> Result<(), SnapshotError> {
    if destination_dir.exists() && !destination_dir.is_dir() {
        return Err(SnapshotError::DestinationNotDirectory {
            path: destination_dir.to_path_buf(),
        });
    }
    Ok(())
}

fn ensure_snapshot_directory(snapshot_dir: &Path) -> Result<(), SnapshotError> {
    if !snapshot_dir.exists() {
        return Err(SnapshotError::SnapshotMissing {
            path: snapshot_dir.to_path_buf(),
        });
    }
    if !snapshot_dir.is_dir() {
        return Err(SnapshotError::SnapshotNotDirectory {
            path: snapshot_dir.to_path_buf(),
        });
    }
    Ok(())
}

fn ensure_target_directory_or_absent(target_dir: &Path) -> Result<(), SnapshotError> {
    if target_dir.exists() && !target_dir.is_dir() {
        return Err(SnapshotError::TargetNotDirectory {
            path: target_dir.to_path_buf(),
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    #[cfg(unix)]
    use std::os::unix::fs as unix_fs;
    use std::path::{Path, PathBuf};
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn create_snapshot_copies_directory_recursively() {
        let source_root = TestDir::new("snapshot-source");
        let snapshot_root = TestDir::new("snapshot-destination");

        write_file(source_root.path().join("README.md"), "source");
        write_file(source_root.path().join("nested/deep/file.txt"), "content");

        create_snapshot(source_root.path(), snapshot_root.path()).expect("snapshot should succeed");

        assert_eq!(
            fs::read_to_string(snapshot_root.path().join("README.md")).expect("snapshot file"),
            "source"
        );
        assert_eq!(
            fs::read_to_string(snapshot_root.path().join("nested/deep/file.txt"))
                .expect("snapshot nested file"),
            "content"
        );
    }

    #[test]
    fn restore_snapshot_replaces_existing_target_contents() {
        let snapshot_root = TestDir::new("restore-source");
        let target_root = TestDir::new("restore-target");

        write_file(snapshot_root.path().join("fresh/data.txt"), "new");
        write_file(target_root.path().join("stale/old.txt"), "old");

        restore_snapshot(snapshot_root.path(), target_root.path()).expect("restore should succeed");

        assert!(
            !target_root.path().join("stale/old.txt").exists(),
            "existing target content should be replaced"
        );
        assert_eq!(
            fs::read_to_string(target_root.path().join("fresh/data.txt")).expect("restored file"),
            "new"
        );
    }

    #[test]
    fn create_snapshot_returns_source_missing_error() {
        let missing_source = TestDir::new("missing-source").into_path();
        let snapshot_root = TestDir::new("missing-snapshot");

        let error = create_snapshot(&missing_source, snapshot_root.path())
            .expect_err("missing source should fail");

        match error {
            SnapshotError::SourceMissing { path } => assert_eq!(path, missing_source),
            other => panic!("unexpected error variant: {other:?}"),
        }
    }

    #[test]
    fn restore_snapshot_returns_target_not_directory_error() {
        let snapshot_root = TestDir::new("restore-source");
        write_file(snapshot_root.path().join("fresh/data.txt"), "new");

        let target_parent = TestDir::new("restore-file-target");
        let target_file = target_parent.path().join("target-file");
        write_file(target_file.clone(), "not-a-directory");

        let error = restore_snapshot(snapshot_root.path(), &target_file)
            .expect_err("target file should be rejected");

        match error {
            SnapshotError::TargetNotDirectory { path } => assert_eq!(path, target_file),
            other => panic!("unexpected error variant: {other:?}"),
        }
    }

    #[cfg(unix)]
    #[test]
    fn restore_snapshot_rolls_back_target_when_copy_fails_midway() {
        let snapshot_root = TestDir::new("restore-source-rollback");
        let target_root = TestDir::new("restore-target-rollback");

        write_file(snapshot_root.path().join("new.txt"), "new");
        unix_fs::symlink(
            snapshot_root.path().join("new.txt"),
            snapshot_root.path().join("unsupported-link"),
        )
        .expect("symlink should be creatable");
        write_file(target_root.path().join("old.txt"), "old");

        let error = restore_snapshot(snapshot_root.path(), target_root.path())
            .expect_err("restore should fail on unsupported entry");
        match error {
            SnapshotError::UnsupportedEntry { path } => {
                assert_eq!(path, snapshot_root.path().join("unsupported-link"));
            }
            other => panic!("unexpected error variant: {other:?}"),
        }

        assert_eq!(
            fs::read_to_string(target_root.path().join("old.txt")).expect("old file should exist"),
            "old"
        );
        assert!(
            !target_root.path().join("new.txt").exists(),
            "partial copy should be removed after rollback"
        );
    }

    fn write_file(path: PathBuf, contents: &str) {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).expect("create parent directories");
        }
        fs::write(path, contents).expect("write file");
    }

    struct TestDir {
        path: PathBuf,
    }

    impl TestDir {
        fn new(prefix: &str) -> Self {
            let mut attempt = 0_u32;
            loop {
                let timestamp = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .expect("clock should be after epoch")
                    .as_nanos();
                let path = std::env::temp_dir().join(format!(
                    "gitaly-storage-{prefix}-{timestamp}-{}-{attempt}",
                    std::process::id()
                ));
                match fs::create_dir_all(&path) {
                    Ok(()) => return Self { path },
                    Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
                        attempt += 1;
                    }
                    Err(error) => panic!("failed to create test directory: {error}"),
                }
            }
        }

        fn into_path(self) -> PathBuf {
            self.path.clone()
        }

        fn path(&self) -> &Path {
            &self.path
        }
    }

    impl Drop for TestDir {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.path);
        }
    }
}
