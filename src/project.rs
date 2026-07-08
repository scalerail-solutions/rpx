use directories::ProjectDirs;
use miette::Diagnostic;
use std::{
    collections::hash_map::DefaultHasher,
    env, fs,
    hash::{Hash, Hasher},
    path::{Path, PathBuf},
};
use thiserror::Error;

pub const LOCKFILE_NAME: &str = "rpx.lock";
pub const DESCRIPTION_NAME: &str = "DESCRIPTION";

#[derive(Debug, Error, Diagnostic)]
pub enum ProjectPathError {
    #[error("failed to get current directory: {source}")]
    #[diagnostic(code(rpx::project::current_dir_failed))]
    CurrentDirFailed {
        #[source]
        source: std::io::Error,
    },

    #[error("{DESCRIPTION_NAME} not found in current directory or any parent directory")]
    #[diagnostic(code(rpx::project::description_not_found))]
    DescriptionNotFound,
}

pub fn new_project_description_path() -> Result<PathBuf, ProjectPathError> {
    Ok(current_dir()?.join(DESCRIPTION_NAME))
}

pub fn description_path() -> Result<PathBuf, ProjectPathError> {
    Ok(project_root_result()?.join(DESCRIPTION_NAME))
}

pub fn lockfile_path_result() -> Result<PathBuf, String> {
    Ok(project_root_result()
        .map_err(|error| error.to_string())?
        .join(LOCKFILE_NAME))
}

pub fn project_library_path() -> PathBuf {
    let library_path = project_library_root_path().join("library");

    fs::create_dir_all(&library_path).expect("failed to create project library");
    library_path
}

pub fn project_library_root_path() -> PathBuf {
    let project_key = hash_path(&project_root());
    project_dirs()
        .data_dir()
        .join("libraries")
        .join(project_key)
}

pub fn cache_dir_path() -> PathBuf {
    project_dirs().cache_dir().to_path_buf()
}

pub fn artifact_cache_path(package: &str, version: &str, file_name: &str) -> PathBuf {
    let path = project_dirs()
        .cache_dir()
        .join("artifacts")
        .join(package)
        .join(version)
        .join(file_name);
    ensure_parent_dir(&path);
    path
}

pub fn build_temp_library_path(package: &str, unique: &str) -> PathBuf {
    let path = project_dirs()
        .cache_dir()
        .join("build-temp")
        .join(format!("{package}-{unique}"))
        .join("library");
    fs::create_dir_all(&path).expect("failed to create temporary build library");
    path
}

pub fn project_root() -> PathBuf {
    project_root_result().unwrap_or_else(|error| panic!("{error}"))
}

pub fn project_root_result() -> Result<PathBuf, ProjectPathError> {
    let current_dir = current_dir()?;
    let current_dir = current_dir
        .canonicalize()
        .unwrap_or_else(|_| current_dir.clone());

    for candidate in current_dir.ancestors() {
        if candidate.join(DESCRIPTION_NAME).exists() {
            return Ok(candidate.to_path_buf());
        }
    }

    Err(ProjectPathError::DescriptionNotFound)
}

fn current_dir() -> Result<PathBuf, ProjectPathError> {
    env::current_dir().map_err(|source| ProjectPathError::CurrentDirFailed { source })
}

fn project_dirs() -> ProjectDirs {
    ProjectDirs::from("de", "scalerail", "rpx").expect("failed to resolve rpx data directory")
}

fn ensure_parent_dir(path: &Path) {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).expect("failed to create cache directory");
    }
}

fn hash_path(path: &Path) -> String {
    let mut hasher = DefaultHasher::new();
    path.hash(&mut hasher);
    format!("{:016x}", hasher.finish())
}
