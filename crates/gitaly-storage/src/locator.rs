use std::path::{Component, Path, PathBuf};

use thiserror::Error;

#[derive(Debug, Error)]
pub enum LocatorError {
    #[error("storage root must be absolute: `{0}`")]
    NonAbsoluteStorageRoot(PathBuf),
    #[error("failed to resolve storage root `{path}`")]
    ResolveStorageRoot {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("storage root is not a directory: `{0}`")]
    StorageRootNotDirectory(PathBuf),
    #[error("repository path must not be empty")]
    EmptyRepositoryPath,
    #[error("repository path must be relative: `{0}`")]
    NonRelativeRepositoryPath(PathBuf),
    #[error("repository path contains disallowed component `{component}`: `{path}`")]
    InvalidRepositoryPathComponent {
        path: PathBuf,
        component: &'static str,
    },
    #[error(
        "repository path escapes storage root: `{path}` resolved to `{resolved}` outside `{storage_root}`"
    )]
    RepositoryPathEscapesStorageRoot {
        path: PathBuf,
        resolved: PathBuf,
        storage_root: PathBuf,
    },
    #[error("failed to inspect repository path `{path}`")]
    InspectRepositoryPath {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
}

#[derive(Debug, Clone)]
pub struct Locator {
    storage_root: PathBuf,
}

impl Locator {
    pub fn new(storage_root: impl Into<PathBuf>) -> Result<Self, LocatorError> {
        let storage_root = storage_root.into();

        if !storage_root.is_absolute() {
            return Err(LocatorError::NonAbsoluteStorageRoot(storage_root));
        }

        let storage_root =
            storage_root
                .canonicalize()
                .map_err(|source| LocatorError::ResolveStorageRoot {
                    path: storage_root,
                    source,
                })?;

        if !storage_root.is_dir() {
            return Err(LocatorError::StorageRootNotDirectory(storage_root));
        }

        Ok(Self { storage_root })
    }

    pub fn storage_root(&self) -> &Path {
        &self.storage_root
    }

    pub fn resolve(&self, repository_path: impl AsRef<Path>) -> Result<PathBuf, LocatorError> {
        let normalized = normalize_repository_path(repository_path.as_ref())?;
        self.validate_existing_path_components(&normalized)?;
        Ok(self.storage_root.join(normalized))
    }

    fn validate_existing_path_components(
        &self,
        repository_path: &Path,
    ) -> Result<(), LocatorError> {
        let mut current = self.storage_root.clone();

        for component in repository_path.components() {
            let Component::Normal(segment) = component else {
                continue;
            };

            current.push(segment);

            if !current.exists() {
                continue;
            }

            let canonical =
                current
                    .canonicalize()
                    .map_err(|source| LocatorError::InspectRepositoryPath {
                        path: repository_path.to_path_buf(),
                        source,
                    })?;

            if !canonical.starts_with(&self.storage_root) {
                return Err(LocatorError::RepositoryPathEscapesStorageRoot {
                    path: repository_path.to_path_buf(),
                    resolved: canonical,
                    storage_root: self.storage_root.clone(),
                });
            }

            current = canonical;
        }

        Ok(())
    }
}

fn normalize_repository_path(repository_path: &Path) -> Result<PathBuf, LocatorError> {
    if repository_path.as_os_str().is_empty() {
        return Err(LocatorError::EmptyRepositoryPath);
    }

    let mut normalized = PathBuf::new();

    for component in repository_path.components() {
        match component {
            Component::Normal(segment) => normalized.push(segment),
            Component::CurDir => {
                return Err(LocatorError::InvalidRepositoryPathComponent {
                    path: repository_path.to_path_buf(),
                    component: "current directory",
                });
            }
            Component::ParentDir => {
                return Err(LocatorError::InvalidRepositoryPathComponent {
                    path: repository_path.to_path_buf(),
                    component: "parent directory",
                });
            }
            Component::RootDir | Component::Prefix(_) => {
                return Err(LocatorError::NonRelativeRepositoryPath(
                    repository_path.to_path_buf(),
                ));
            }
        }
    }

    if normalized.as_os_str().is_empty() {
        return Err(LocatorError::EmptyRepositoryPath);
    }

    Ok(normalized)
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::path::{Path, PathBuf};
    use std::sync::atomic::{AtomicU64, Ordering};

    use super::{Locator, LocatorError};

    #[cfg(unix)]
    use std::os::unix::fs::symlink;

    #[test]
    fn resolves_repository_path_under_storage_root() {
        let storage_root = TempDir::new("locator-storage");
        let locator = Locator::new(storage_root.path().to_path_buf()).expect("locator creates");

        let resolved = locator
            .resolve("group/project.git")
            .expect("path should resolve");

        assert_eq!(resolved, locator.storage_root().join("group/project.git"));
    }

    #[test]
    fn rejects_parent_traversal_components() {
        let storage_root = TempDir::new("locator-parent");
        let locator = Locator::new(storage_root.path().to_path_buf()).expect("locator creates");

        let err = locator
            .resolve("../escape.git")
            .expect_err("parent traversal must be rejected");

        assert!(matches!(
            err,
            LocatorError::InvalidRepositoryPathComponent {
                component: "parent directory",
                ..
            }
        ));
    }

    #[test]
    fn rejects_absolute_repository_paths() {
        let storage_root = TempDir::new("locator-absolute");
        let locator = Locator::new(storage_root.path().to_path_buf()).expect("locator creates");

        let err = locator
            .resolve("/tmp/repo.git")
            .expect_err("absolute paths must be rejected");

        assert!(matches!(err, LocatorError::NonRelativeRepositoryPath(_)));
    }

    #[cfg(unix)]
    #[test]
    fn rejects_symlink_escape_from_storage_root() {
        let storage_root = TempDir::new("locator-symlink-root");
        let outside = TempDir::new("locator-symlink-outside");
        fs::create_dir_all(outside.path().join("target"))
            .expect("outside target directory should exist");

        symlink(outside.path(), storage_root.path().join("link"))
            .expect("symlink should be created");

        let locator = Locator::new(storage_root.path().to_path_buf()).expect("locator creates");
        let err = locator
            .resolve("link/target/repo.git")
            .expect_err("symlink escape must be rejected");

        assert!(matches!(
            err,
            LocatorError::RepositoryPathEscapesStorageRoot { .. }
        ));
    }

    struct TempDir {
        path: PathBuf,
    }

    impl TempDir {
        fn new(prefix: &str) -> Self {
            static NEXT_ID: AtomicU64 = AtomicU64::new(0);
            let id = NEXT_ID.fetch_add(1, Ordering::Relaxed);
            let path = std::env::temp_dir().join(format!(
                "gitaly-storage-{prefix}-{}-{id}",
                std::process::id()
            ));
            fs::create_dir_all(&path).expect("temp directory should be creatable");
            Self { path }
        }

        fn path(&self) -> &Path {
            &self.path
        }
    }

    impl Drop for TempDir {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.path);
        }
    }
}
