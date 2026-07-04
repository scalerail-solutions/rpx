use crate::project::cache_dir_path;
use std::{
    collections::hash_map::DefaultHasher,
    fmt, fs,
    hash::{Hash, Hasher},
    path::{Path, PathBuf},
};

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct CompiledPackageCacheKey {
    package: String,
    version: String,
    r_version: String,
    platform: String,
}

impl CompiledPackageCacheKey {
    pub fn new(package: &str, version: &str, r_version: &str) -> Self {
        Self::with_platform(package, version, r_version, host_platform_key())
    }

    pub fn with_platform(
        package: &str,
        version: &str,
        r_version: &str,
        platform: impl Into<String>,
    ) -> Self {
        Self {
            package: package.to_string(),
            version: version.to_string(),
            r_version: r_version.to_string(),
            platform: platform.into(),
        }
    }

    pub fn package(&self) -> &str {
        &self.package
    }

    fn cache_dir_name(&self) -> String {
        format!(
            "{}-{}-{}-{}",
            self.package,
            self.version,
            self.platform,
            self.digest()
        )
    }

    fn digest(&self) -> String {
        let input = format!(
            "{}\n{}\n{}\n{}",
            self.package, self.version, self.r_version, self.platform
        );
        let mut hasher = DefaultHasher::new();
        input.hash(&mut hasher);

        format!("{:016x}", hasher.finish())
    }
}

impl fmt::Display for CompiledPackageCacheKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.cache_dir_name())
    }
}

pub async fn exists(key: &CompiledPackageCacheKey) -> bool {
    package_cache_path(key).exists()
}

pub async fn restore(key: &CompiledPackageCacheKey, target_library: &Path) -> Result<(), String> {
    let source = package_cache_path(key);
    let target = target_library.join(key.package());

    tokio::task::spawn_blocking(move || copy_package_dir(&source, &target))
        .await
        .map_err(|error| format!("failed to join cache restore task: {error}"))?
}

pub async fn store(key: &CompiledPackageCacheKey, package_dir: &Path) -> Result<(), String> {
    let target = package_cache_path(key);
    let package_dir = package_dir.to_path_buf();

    tokio::task::spawn_blocking(move || copy_package_dir(&package_dir, &target))
        .await
        .map_err(|error| format!("failed to join cache store task: {error}"))?
}

fn host_platform_key() -> String {
    format!("{}-{}", std::env::consts::OS, std::env::consts::ARCH)
}

fn package_cache_path(key: &CompiledPackageCacheKey) -> PathBuf {
    cache_dir_path()
        .join("builds")
        .join(key.cache_dir_name())
        .join("package")
}

fn copy_package_dir(source: &Path, destination: &Path) -> Result<(), String> {
    if !source.exists() {
        return Err(format!(
            "cached package directory is missing: {}",
            source.display()
        ));
    }

    if destination.exists() {
        fs::remove_dir_all(destination)
            .map_err(|error| format!("failed to replace package directory: {error}"))?;
    }
    fs::create_dir_all(destination)
        .map_err(|error| format!("failed to create package directory: {error}"))?;

    for entry in fs::read_dir(source)
        .map_err(|error| format!("failed to read package directory: {error}"))?
    {
        let entry = entry.map_err(|error| format!("failed to read package entry: {error}"))?;
        let source_path = entry.path();
        let destination_path = destination.join(entry.file_name());
        let file_type = entry
            .file_type()
            .map_err(|error| format!("failed to inspect package entry: {error}"))?;

        if file_type.is_dir() {
            copy_package_dir(&source_path, &destination_path)?;
        } else {
            fs::copy(&source_path, &destination_path)
                .map_err(|error| format!("failed to copy package file: {error}"))?;
        }
    }

    Ok(())
}
