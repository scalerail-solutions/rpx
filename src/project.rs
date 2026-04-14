use directories::ProjectDirs;
use std::{
    collections::hash_map::DefaultHasher,
    env, fs,
    hash::{Hash, Hasher},
    path::{Path, PathBuf},
};

pub const LOCKFILE_NAME: &str = "rpx.lock";
pub const DESCRIPTION_NAME: &str = "DESCRIPTION";

pub fn description_path() -> PathBuf {
    project_root().join(DESCRIPTION_NAME)
}

pub fn lockfile_path() -> PathBuf {
    project_root().join(LOCKFILE_NAME)
}

pub fn project_library_path() -> PathBuf {
    let project_key = hash_path(&project_root());
    let project_dirs =
        ProjectDirs::from("dev", "blyedev", "rpx").expect("failed to resolve rpx data directory");
    let library_path = project_dirs
        .data_dir()
        .join("libraries")
        .join(project_key)
        .join("library");

    fs::create_dir_all(&library_path).expect("failed to create project library");
    library_path
}

pub fn project_root() -> PathBuf {
    let current_dir = env::current_dir().expect("failed to get current directory");
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

fn hash_path(path: &Path) -> String {
    let mut hasher = DefaultHasher::new();
    path.hash(&mut hasher);
    format!("{:016x}", hasher.finish())
}
