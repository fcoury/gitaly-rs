//! Git object quarantine support.

use std::cmp::Ordering;
use std::ffi::{OsStr, OsString};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use thiserror::Error;

const DEFAULT_OBJECT_DIRECTORY: &str = "objects";
const QUARANTINE_PREFIX: &str = "incoming-";
const QUARANTINE_NAME_ATTEMPTS: usize = 32;

#[derive(Debug)]
pub struct Quarantine {
    object_directory: PathBuf,
    quarantine_object_directory: PathBuf,
    alternate_object_directories: Vec<PathBuf>,
}

impl Quarantine {
    pub fn create(repository_path: impl Into<PathBuf>) -> Result<Self, QuarantineError> {
        let object_directory = repository_path.into().join(DEFAULT_OBJECT_DIRECTORY);
        fs::create_dir_all(&object_directory).map_err(|source| QuarantineError::Io {
            operation: "creating object directory",
            path: object_directory.clone(),
            source,
        })?;

        let quarantine_object_directory = create_quarantine_object_directory(&object_directory)?;
        let alternate_object_directories = vec![object_directory.clone()];

        Ok(Self {
            object_directory,
            quarantine_object_directory,
            alternate_object_directories,
        })
    }

    pub fn object_directory(&self) -> &Path {
        &self.object_directory
    }

    pub fn quarantine_object_directory(&self) -> &Path {
        &self.quarantine_object_directory
    }

    pub fn git_environment(&self) -> Result<[(&'static str, OsString); 2], QuarantineError> {
        Ok([
            (
                "GIT_OBJECT_DIRECTORY",
                self.quarantine_object_directory.clone().into_os_string(),
            ),
            (
                "GIT_ALTERNATE_OBJECT_DIRECTORIES",
                std::env::join_paths(&self.alternate_object_directories)?,
            ),
        ])
    }

    pub fn migrate(&mut self) -> Result<(), QuarantineError> {
        if !self
            .quarantine_object_directory
            .try_exists()
            .map_err(|source| QuarantineError::Io {
                operation: "checking quarantine directory existence",
                path: self.quarantine_object_directory.clone(),
                source,
            })?
        {
            return Ok(());
        }

        migrate_directory(&self.quarantine_object_directory, &self.object_directory)
    }
}

impl Drop for Quarantine {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.quarantine_object_directory);
    }
}

#[derive(Debug, Error)]
pub enum QuarantineError {
    #[error("{operation} `{path}`: {source}")]
    Io {
        operation: &'static str,
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("joining alternate object directories: {0}")]
    JoinPaths(#[from] std::env::JoinPathsError),
    #[error("failed to create a unique quarantine directory under `{object_directory}`")]
    UniqueDirectoryExhausted { object_directory: PathBuf },
}

fn create_quarantine_object_directory(object_directory: &Path) -> Result<PathBuf, QuarantineError> {
    static NEXT_ID: AtomicU64 = AtomicU64::new(0);

    for _ in 0..QUARANTINE_NAME_ATTEMPTS {
        let unique_id = NEXT_ID.fetch_add(1, AtomicOrdering::Relaxed);
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or(Duration::ZERO)
            .as_nanos();
        let candidate = object_directory.join(format!(
            "{QUARANTINE_PREFIX}{timestamp:x}-{}-{unique_id}",
            std::process::id()
        ));

        match fs::create_dir(&candidate) {
            Ok(()) => return Ok(candidate),
            Err(source) if source.kind() == std::io::ErrorKind::AlreadyExists => continue,
            Err(source) => {
                return Err(QuarantineError::Io {
                    operation: "creating quarantine directory",
                    path: candidate,
                    source,
                });
            }
        }
    }

    Err(QuarantineError::UniqueDirectoryExhausted {
        object_directory: object_directory.to_path_buf(),
    })
}

#[derive(Debug)]
struct DirectoryEntry {
    name: OsString,
    is_dir: bool,
}

fn migrate_directory(source_path: &Path, target_path: &Path) -> Result<(), QuarantineError> {
    let mut entries = read_directory_entries(source_path)?;
    sort_entries_for_migration(&mut entries);

    for entry in &entries {
        let nested_source_path = source_path.join(&entry.name);
        let nested_target_path = target_path.join(&entry.name);

        if entry.is_dir {
            match fs::create_dir(&nested_target_path) {
                Ok(()) => {}
                Err(source) if source.kind() == std::io::ErrorKind::AlreadyExists => {}
                Err(source) => {
                    return Err(QuarantineError::Io {
                        operation: "creating migration target directory",
                        path: nested_target_path,
                        source,
                    });
                }
            }

            migrate_directory(&nested_source_path, &nested_target_path)?;
            continue;
        }

        finalize_object_file(&nested_source_path, &nested_target_path)?;
    }

    fs::remove_dir(source_path).map_err(|source| QuarantineError::Io {
        operation: "removing quarantine directory",
        path: source_path.to_path_buf(),
        source,
    })?;

    Ok(())
}

fn read_directory_entries(path: &Path) -> Result<Vec<DirectoryEntry>, QuarantineError> {
    let mut entries = Vec::new();
    let read_dir = fs::read_dir(path).map_err(|source| QuarantineError::Io {
        operation: "reading quarantine directory",
        path: path.to_path_buf(),
        source,
    })?;

    for read_dir_entry in read_dir {
        let read_dir_entry = read_dir_entry.map_err(|source| QuarantineError::Io {
            operation: "reading directory entry",
            path: path.to_path_buf(),
            source,
        })?;
        let file_type = read_dir_entry
            .file_type()
            .map_err(|source| QuarantineError::Io {
                operation: "reading directory entry file type",
                path: read_dir_entry.path(),
                source,
            })?;

        entries.push(DirectoryEntry {
            name: read_dir_entry.file_name(),
            is_dir: file_type.is_dir(),
        });
    }

    Ok(entries)
}

fn sort_entries_for_migration(entries: &mut [DirectoryEntry]) {
    entries.sort_by(|left, right| {
        compare_names_for_migration(left.name.as_os_str(), right.name.as_os_str())
    });
}

fn compare_names_for_migration(left_name: &OsStr, right_name: &OsStr) -> Ordering {
    let left_name = left_name.to_string_lossy();
    let right_name = right_name.to_string_lossy();

    pack_copy_priority(left_name.as_ref())
        .cmp(&pack_copy_priority(right_name.as_ref()))
        .then_with(|| left_name.cmp(&right_name))
}

fn pack_copy_priority(name: &str) -> u8 {
    if !name.starts_with("pack") {
        return 0;
    }

    if name.ends_with(".keep") {
        return 1;
    }
    if name.ends_with(".pack") {
        return 2;
    }
    if name.ends_with(".rev") {
        return 3;
    }
    if name.ends_with(".idx") {
        return 4;
    }

    5
}

fn finalize_object_file(source_path: &Path, target_path: &Path) -> Result<(), QuarantineError> {
    let hard_link_result = fs::hard_link(source_path, target_path);
    let renamed = match hard_link_result {
        Ok(()) => false,
        Err(source) if source.kind() == std::io::ErrorKind::AlreadyExists => false,
        Err(_) => match fs::rename(source_path, target_path) {
            Ok(()) => true,
            Err(source) if source.kind() == std::io::ErrorKind::AlreadyExists => false,
            Err(source) => {
                return Err(QuarantineError::Io {
                    operation: "finalizing object file",
                    path: source_path.to_path_buf(),
                    source,
                });
            }
        },
    };

    if !renamed {
        let _ = fs::remove_file(source_path);
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{compare_names_for_migration, Quarantine};
    use std::fs;
    use std::path::{Path, PathBuf};
    use std::sync::atomic::{AtomicU64, Ordering};

    #[test]
    fn create_places_quarantine_inside_object_directory() {
        let repo = TempRepo::new();
        let quarantine =
            Quarantine::create(repo.path.clone()).expect("quarantine should be creatable");

        assert!(quarantine
            .quarantine_object_directory()
            .starts_with(repo.object_directory()));
        assert!(quarantine.quarantine_object_directory().is_dir());

        let quarantine_name = quarantine
            .quarantine_object_directory()
            .file_name()
            .expect("quarantine directory should have a basename")
            .to_string_lossy()
            .into_owned();
        assert!(quarantine_name.starts_with("incoming-"));
    }

    #[test]
    fn git_environment_exposes_object_and_alternate_directories() {
        let repo = TempRepo::new();
        let quarantine =
            Quarantine::create(repo.path.clone()).expect("quarantine should be creatable");

        let environment = quarantine
            .git_environment()
            .expect("environment should be derivable");
        let expected_alternates = std::env::join_paths([repo.object_directory()])
            .expect("single alternate object directory should join");

        assert_eq!(environment[0].0, "GIT_OBJECT_DIRECTORY");
        assert_eq!(
            PathBuf::from(environment[0].1.clone()),
            quarantine.quarantine_object_directory()
        );
        assert_eq!(environment[1].0, "GIT_ALTERNATE_OBJECT_DIRECTORIES");
        assert_eq!(environment[1].1, expected_alternates);
    }

    #[test]
    fn migrate_moves_quarantined_files_and_keeps_existing_targets() {
        let repo = TempRepo::new();
        let mut quarantine =
            Quarantine::create(repo.path.clone()).expect("quarantine should be creatable");

        let loose_object = quarantine
            .quarantine_object_directory()
            .join("aa")
            .join("11111111111111111111111111111111111111");
        let existing_target = repo
            .object_directory()
            .join("aa")
            .join("22222222222222222222222222222222222222");
        let conflicting_source = quarantine
            .quarantine_object_directory()
            .join("aa")
            .join("22222222222222222222222222222222222222");
        let pack_file = quarantine
            .quarantine_object_directory()
            .join("pack")
            .join("pack-abc.pack");

        write_file(&loose_object, b"loose");
        write_file(&existing_target, b"existing");
        write_file(&conflicting_source, b"different");
        write_file(&pack_file, b"pack");

        quarantine
            .migrate()
            .expect("quarantine migration should succeed");

        assert_eq!(
            fs::read(
                repo.object_directory()
                    .join("aa/11111111111111111111111111111111111111")
            )
            .expect("migrated loose object should exist"),
            b"loose"
        );
        assert_eq!(
            fs::read(existing_target).expect("existing target should remain"),
            b"existing"
        );
        assert_eq!(
            fs::read(repo.object_directory().join("pack/pack-abc.pack"))
                .expect("migrated pack should exist"),
            b"pack"
        );
        assert!(!quarantine.quarantine_object_directory().exists());
    }

    #[test]
    fn migrate_is_noop_if_quarantine_is_already_gone() {
        let repo = TempRepo::new();
        let mut quarantine =
            Quarantine::create(repo.path.clone()).expect("quarantine should be creatable");

        fs::remove_dir_all(quarantine.quarantine_object_directory())
            .expect("test should be able to remove quarantine directory");

        quarantine
            .migrate()
            .expect("migrate should be a no-op for missing quarantine");
    }

    #[test]
    fn drop_removes_quarantine_directory_best_effort() {
        let repo = TempRepo::new();
        let quarantine_path = {
            let quarantine =
                Quarantine::create(repo.path.clone()).expect("quarantine should be creatable");
            quarantine.quarantine_object_directory().to_path_buf()
        };

        assert!(!quarantine_path.exists());
    }

    #[test]
    fn migration_sorting_prioritizes_pack_metadata_deterministically() {
        let mut names = vec![
            "pack-1.pack".to_string(),
            "pack-2.keep".to_string(),
            "pack-1.keep".to_string(),
            "pack-2.rev".to_string(),
            "pack-2.pack".to_string(),
            "pack-2.idx".to_string(),
            "pack-3.keep".to_string(),
            "pack-3.rev".to_string(),
            "pack-1.idx".to_string(),
            "pack-3.idx".to_string(),
            "pack-3.pack".to_string(),
            "pack-1.rev".to_string(),
            "07".to_string(),
            "aa".to_string(),
        ];

        names.sort_by(|left, right| compare_names_for_migration(left.as_ref(), right.as_ref()));

        assert_eq!(
            names,
            vec![
                "07",
                "aa",
                "pack-1.keep",
                "pack-2.keep",
                "pack-3.keep",
                "pack-1.pack",
                "pack-2.pack",
                "pack-3.pack",
                "pack-1.rev",
                "pack-2.rev",
                "pack-3.rev",
                "pack-1.idx",
                "pack-2.idx",
                "pack-3.idx",
            ]
        );
    }

    struct TempRepo {
        path: PathBuf,
    }

    impl TempRepo {
        fn new() -> Self {
            let path = unique_repo_path();
            fs::create_dir_all(path.join("objects")).expect("temp repository should be creatable");

            Self { path }
        }

        fn object_directory(&self) -> PathBuf {
            self.path.join("objects")
        }
    }

    impl Drop for TempRepo {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.path);
        }
    }

    fn unique_repo_path() -> PathBuf {
        static NEXT_ID: AtomicU64 = AtomicU64::new(0);
        let id = NEXT_ID.fetch_add(1, Ordering::Relaxed);

        std::env::temp_dir().join(format!(
            "gitaly-git-quarantine-tests-{}-{id}",
            std::process::id()
        ))
    }

    fn write_file(path: &Path, contents: &[u8]) {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).expect("file parent should be creatable");
        }

        fs::write(path, contents).expect("file should be writable");
    }
}
