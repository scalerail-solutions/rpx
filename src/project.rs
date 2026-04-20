use directories::ProjectDirs;
use std::{
    collections::hash_map::DefaultHasher,
    env, fs,
    hash::{Hash, Hasher},
    path::{Path, PathBuf},
};

pub const LOCKFILE_NAME: &str = "rpx.lock";
pub const DESCRIPTION_NAME: &str = "DESCRIPTION";

pub fn current_description_path() -> PathBuf {
    current_dir().join(DESCRIPTION_NAME)
}

pub fn description_path() -> PathBuf {
    project_root().join(DESCRIPTION_NAME)
}

pub fn lockfile_path() -> PathBuf {
    project_root().join(LOCKFILE_NAME)
}

pub fn project_library_path() -> PathBuf {
    let project_key = hash_path(&project_root());
    let library_path = project_dirs()
        .data_dir()
        .join("libraries")
        .join(project_key)
        .join("library");

    fs::create_dir_all(&library_path).expect("failed to create project library");
    library_path
}

pub fn artifact_cache_path(package: &str, version: &str) -> PathBuf {
    let path = project_dirs()
        .cache_dir()
        .join("artifacts")
        .join(package)
        .join(version)
        .join("source.tar.gz");
    ensure_parent_dir(&path);
    path
}

pub fn compiled_cache_package_path(cache_key: &str, package: &str) -> PathBuf {
    let path = project_dirs()
        .cache_dir()
        .join("builds")
        .join(cache_key)
        .join("library")
        .join(package);
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
    let current_dir = current_dir();
    let current_dir = current_dir
        .canonicalize()
        .unwrap_or_else(|_| current_dir.clone());

    for candidate in current_dir.ancestors() {
        if candidate.join(DESCRIPTION_NAME).exists() {
            return candidate.to_path_buf();
        }
    }

    panic!("{DESCRIPTION_NAME} not found in current directory or any parent directory");
}

fn current_dir() -> PathBuf {
    env::current_dir().expect("failed to get current directory")
}

fn project_dirs() -> ProjectDirs {
    ProjectDirs::from("dev", "blyedev", "rpx").expect("failed to resolve rpx data directory")
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
