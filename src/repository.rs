use crate::output::try_prompt;
use crate::registry::{
    PackageVersionsResponse, RegistryClient, RepositoryPackageSummary, RepositoryPackagesResponse,
    VersionSummary, is_not_found_error, is_unauthorized_error,
};
use flate2::read::GzDecoder;
use keyring::Entry;
use std::{
    collections::{BTreeMap, BTreeSet, hash_map::DefaultHasher},
    fs,
    hash::{Hash, Hasher},
    io::{self, IsTerminal, Read},
    path::{Path, PathBuf},
    sync::Arc,
};
use tar::Archive;

use crate::project::cache_dir_path;

const KEYRING_SERVICE: &str = "rpx";

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct RepositorySource {
    base_url: String,
    kind: RepositoryKind,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum RepositoryKind {
    Rrepo,
    CranLike,
}

#[derive(Debug, Clone)]
pub struct SourcedPackageVersions {
    pub source: RepositorySource,
    pub response: PackageVersionsResponse,
}

pub trait CredentialStore: Send + Sync {
    fn get(&self, source: &RepositorySource) -> Result<Option<String>, String>;
    fn set(&self, source: &RepositorySource, token: &str) -> Result<(), String>;
    fn delete(&self, source: &RepositorySource) -> Result<(), String>;
}

pub trait ApiKeyPrompter: Send + Sync {
    fn prompt(&self, source: &RepositorySource, had_stored_token: bool) -> Result<String, String>;
}

#[derive(Debug, Clone)]
pub struct KeyringCredentialStore;

#[derive(Debug, Clone)]
pub struct TerminalApiKeyPrompter;

#[derive(Clone)]
pub struct RepositorySet {
    sources: Vec<RepositorySource>,
    credentials: Arc<dyn CredentialStore>,
    prompter: Arc<dyn ApiKeyPrompter>,
    cran_current_indexes:
        Arc<std::sync::Mutex<BTreeMap<String, BTreeMap<String, Vec<VersionSummary>>>>>,
    cran_archive_unavailable: Arc<std::sync::Mutex<BTreeSet<String>>>,
}

impl std::fmt::Debug for RepositorySet {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RepositorySet")
            .field("sources", &self.sources)
            .finish_non_exhaustive()
    }
}

impl RepositorySource {
    pub fn new(base_url: impl AsRef<str>) -> Self {
        Self::with_kind(base_url, RepositoryKind::Rrepo)
    }

    pub fn cran_like(base_url: impl AsRef<str>) -> Self {
        Self::with_kind(base_url, RepositoryKind::CranLike)
    }

    pub fn with_kind(base_url: impl AsRef<str>, kind: RepositoryKind) -> Self {
        Self {
            base_url: normalize_repository_url(base_url.as_ref()),
            kind,
        }
    }

    pub fn base_url(&self) -> &str {
        &self.base_url
    }

    pub fn kind(&self) -> RepositoryKind {
        self.kind
    }

    pub fn matches_source_url(&self, url: &str) -> bool {
        if !url.starts_with(self.base_url()) {
            return false;
        }

        match self.kind {
            RepositoryKind::Rrepo => !url.contains("/src/contrib/"),
            RepositoryKind::CranLike => url.contains("/src/contrib/"),
        }
    }
}

impl RepositorySet {
    pub fn new(sources: Vec<RepositorySource>) -> Self {
        Self::with_support(
            sources,
            Arc::new(KeyringCredentialStore),
            Arc::new(TerminalApiKeyPrompter),
        )
    }

    pub fn with_support(
        sources: Vec<RepositorySource>,
        credentials: Arc<dyn CredentialStore>,
        prompter: Arc<dyn ApiKeyPrompter>,
    ) -> Self {
        let mut deduped = Vec::new();
        let mut seen = BTreeSet::new();

        for source in sources {
            if seen.insert((source.base_url.clone(), source.kind)) {
                deduped.push(source);
            }
        }

        Self {
            sources: deduped,
            credentials,
            prompter,
            cran_current_indexes: Arc::new(std::sync::Mutex::new(BTreeMap::new())),
            cran_archive_unavailable: Arc::new(std::sync::Mutex::new(BTreeSet::new())),
        }
    }

    pub fn sources(&self) -> &[RepositorySource] {
        &self.sources
    }

    pub fn fetch_repository_packages(
        &self,
        source: &RepositorySource,
    ) -> Result<RepositoryPackagesResponse, String> {
        match source.kind() {
            RepositoryKind::Rrepo => {
                self.with_authorized_client(source, |client| client.fetch_repository_packages())
            }
            RepositoryKind::CranLike => self.fetch_cran_like_repository_packages(source),
        }
    }

    pub fn fetch_package_versions_with_retry(
        &self,
        package: &str,
    ) -> Result<SourcedPackageVersions, String> {
        for source in &self.sources {
            let result = match source.kind() {
                RepositoryKind::Rrepo => self.with_authorized_client(source, |client| {
                    client.fetch_package_versions_with_retry(package)
                }),
                RepositoryKind::CranLike => self.fetch_cran_like_package_versions(source, package),
            };

            match result {
                Ok(response) => {
                    return Ok(SourcedPackageVersions {
                        source: source.clone(),
                        response,
                    });
                }
                Err(error) if is_not_found_error(&error) => continue,
                Err(error) => return Err(error),
            }
        }

        Err(format!(
            "unexpected registry response (404 Not Found): package {package} not found"
        ))
    }

    pub fn fetch_description_with_retry(
        &self,
        source: &RepositorySource,
        package: &str,
        version: &str,
    ) -> Result<String, String> {
        match source.kind() {
            RepositoryKind::Rrepo => self.with_authorized_client(source, |client| {
                client.fetch_description_with_retry(package, version)
            }),
            RepositoryKind::CranLike => self.fetch_cran_like_description(source, package, version),
        }
    }

    pub fn download_artifact_with_progress(
        &self,
        source: &RepositorySource,
        package: &str,
        version: &str,
        artifact: &crate::registry::ArtifactRequest,
        mut on_progress: impl FnMut(crate::registry::DownloadProgress),
    ) -> Result<crate::registry::DownloadedArtifact, String> {
        self.with_authorized_client(source, |client| {
            client.download_artifact_with_progress(package, version, artifact, &mut on_progress)
        })
    }

    pub fn has_stored_credential(&self, source: &RepositorySource) -> Result<bool, String> {
        Ok(self.credentials.get(source)?.is_some())
    }

    pub fn remove_api_key(&self, source: &RepositorySource) -> Result<(), String> {
        self.credentials.delete(source)
    }

    pub fn source_for_url(&self, url: &str) -> Option<RepositorySource> {
        self.sources
            .iter()
            .filter(|source| source.matches_source_url(url))
            .max_by_key(|source| source.base_url().len())
            .cloned()
            .or_else(|| repository_source_from_package_url(url).map(RepositorySource::new))
            .or_else(|| cran_like_source_from_package_url(url).map(RepositorySource::cran_like))
    }

    pub fn cran_archive_unavailable_repositories(&self) -> Vec<String> {
        self.cran_archive_unavailable
            .lock()
            .expect("CRAN-like archive availability should lock")
            .iter()
            .cloned()
            .collect()
    }

    fn fetch_cran_like_repository_packages(
        &self,
        source: &RepositorySource,
    ) -> Result<RepositoryPackagesResponse, String> {
        let index = self.cran_like_current_index(source)?;
        let packages = index
            .into_iter()
            .filter_map(|(name, versions)| {
                let latest_version = versions.last()?.version.clone();
                Some(RepositoryPackageSummary {
                    name,
                    latest_version,
                    latest_uploaded_at: String::new(),
                    versions: versions
                        .into_iter()
                        .map(|version| version.version)
                        .collect(),
                })
            })
            .collect();

        Ok(RepositoryPackagesResponse {
            repository_slug: source.base_url().to_string(),
            packages,
        })
    }

    fn fetch_cran_like_package_versions(
        &self,
        source: &RepositorySource,
        package: &str,
    ) -> Result<PackageVersionsResponse, String> {
        let mut by_version = BTreeMap::new();

        for version in self
            .cran_like_current_index(source)?
            .remove(package)
            .unwrap_or_default()
        {
            by_version.insert(version.version.clone(), version);
        }

        match fetch_cran_like_archive_versions(source, package) {
            Ok(versions) => {
                for version in versions {
                    by_version.entry(version.version.clone()).or_insert(version);
                }
            }
            Err(CranLikeArchiveError::Unavailable) => {
                self.cran_archive_unavailable
                    .lock()
                    .expect("CRAN-like archive availability should lock")
                    .insert(source.base_url().to_string());
            }
            Err(CranLikeArchiveError::Failed(error)) => return Err(error),
        }

        if by_version.is_empty() {
            return Err(missing_package_error(package));
        }

        Ok(PackageVersionsResponse {
            package: package.to_string(),
            versions: by_version.into_values().collect(),
        })
    }

    fn cran_like_current_index(
        &self,
        source: &RepositorySource,
    ) -> Result<BTreeMap<String, Vec<VersionSummary>>, String> {
        let mut indexes = self
            .cran_current_indexes
            .lock()
            .expect("CRAN-like current index cache should lock");

        if let Some(index) = indexes.get(source.base_url()) {
            return Ok(index.clone());
        }

        let index = fetch_cran_like_current_index(source)?;
        indexes.insert(source.base_url().to_string(), index.clone());
        Ok(index)
    }

    fn fetch_cran_like_description(
        &self,
        source: &RepositorySource,
        package: &str,
        version: &str,
    ) -> Result<String, String> {
        let path = cran_like_description_cache_path(source, package, version);
        if let Ok(description) = fs::read_to_string(&path) {
            if description_declares_package(&description, package) {
                return Ok(description);
            }
            let _ = fs::remove_file(&path);
        }

        let versions = self.fetch_cran_like_package_versions(source, package)?;
        let tarball_url = versions
            .versions
            .into_iter()
            .find(|candidate| candidate.version == version)
            .map(|candidate| candidate.source_url)
            .ok_or_else(|| missing_package_error(package))?;
        let description = fetch_description_from_tarball(&tarball_url, package)?;
        write_text_cache(&path, &description);
        Ok(description)
    }

    fn with_authorized_client<T>(
        &self,
        source: &RepositorySource,
        mut action: impl FnMut(&RegistryClient) -> Result<T, String>,
    ) -> Result<T, String> {
        let stored_token = self.credentials.get(source)?;
        let client = RegistryClient::with_token(source.base_url(), stored_token.clone());

        match action(&client) {
            Ok(result) => Ok(result),
            Err(error) if is_unauthorized_error(&error) => {
                let token = self.prompt_and_store_token(source, stored_token.is_some())?;
                let client = RegistryClient::with_token(source.base_url(), Some(token));
                action(&client)
            }
            Err(error) => Err(error),
        }
    }

    fn prompt_and_store_token(
        &self,
        source: &RepositorySource,
        had_stored_token: bool,
    ) -> Result<String, String> {
        let token = self.prompter.prompt(source, had_stored_token)?;
        self.credentials.set(source, &token)?;
        Ok(token)
    }
}

impl CredentialStore for KeyringCredentialStore {
    fn get(&self, source: &RepositorySource) -> Result<Option<String>, String> {
        let Ok(entry) = keyring_entry(source) else {
            return Ok(None);
        };

        match entry.get_password() {
            Ok(password) => Ok(Some(password)),
            Err(keyring::Error::NoEntry) | Err(_) => Ok(None),
        }
    }

    fn set(&self, source: &RepositorySource, token: &str) -> Result<(), String> {
        keyring_entry(source)?
            .set_password(token)
            .map_err(|error| format!("failed to store API key for {}: {error}", source.base_url()))
    }

    fn delete(&self, source: &RepositorySource) -> Result<(), String> {
        match keyring_entry(source)?.delete_credential() {
            Ok(()) | Err(keyring::Error::NoEntry) => Ok(()),
            Err(error) => Err(format!(
                "failed to remove stored API key for {}: {error}",
                source.base_url()
            )),
        }
    }
}

impl ApiKeyPrompter for TerminalApiKeyPrompter {
    fn prompt(&self, source: &RepositorySource, had_stored_token: bool) -> Result<String, String> {
        if !io::stdin().is_terminal() || !io::stderr().is_terminal() {
            return Err(format!(
                "{} requires an API key, but no interactive terminal is available",
                source.base_url()
            ));
        }

        let prompt = if had_stored_token {
            format!(
                "Stored API key rejected for {}. Enter a new API key: ",
                source.base_url()
            )
        } else {
            format!("API key required for {}: ", source.base_url())
        };

        try_prompt(prompt).map_err(|error| format!("failed to prompt for API key: {error}"))?;

        let token = rpassword::read_password()
            .map_err(|error| format!("failed to read API key: {error}"))?;
        let token = token.trim().to_string();

        if token.is_empty() {
            return Err("API key cannot be empty".to_string());
        }

        Ok(token)
    }
}

pub fn normalize_repository_url(value: &str) -> String {
    value.trim().trim_end_matches('/').to_string()
}

pub fn repository_source_from_package_url(url: &str) -> Option<String> {
    let marker = "/packages/";
    let index = url.find(marker)?;
    Some(normalize_repository_url(&url[..index]))
}

pub fn cran_like_source_from_package_url(url: &str) -> Option<String> {
    let marker = "/src/contrib/";
    let index = url.find(marker)?;
    Some(normalize_repository_url(&url[..index]))
}

fn fetch_cran_like_current_index(
    source: &RepositorySource,
) -> Result<BTreeMap<String, Vec<VersionSummary>>, String> {
    let url = format!("{}/src/contrib/PACKAGES", source.base_url());
    let body = reqwest::blocking::get(&url)
        .map_err(|error| format!("failed to contact CRAN-like repository: {error}"))?;
    let status = body.status();
    if !status.is_success() {
        return Err(unexpected_cran_like_response(body));
    }

    let body = body
        .text()
        .map_err(|error| format!("failed to read CRAN-like PACKAGES index: {error}"))?;
    let mut index: BTreeMap<String, Vec<VersionSummary>> = BTreeMap::new();

    for record in parse_dcf_records(&body) {
        let Some(package) = record.get("Package").filter(|value| !value.is_empty()) else {
            continue;
        };
        let Some(version) = record.get("Version").filter(|value| !value.is_empty()) else {
            continue;
        };

        index
            .entry(package.to_string())
            .or_default()
            .push(VersionSummary {
                version: version.to_string(),
                source_url: cran_like_current_tarball_url(source, package, version),
            });
    }

    for versions in index.values_mut() {
        versions.sort_by(|left, right| left.version.cmp(&right.version));
    }

    Ok(index)
}

#[derive(Debug)]
enum CranLikeArchiveError {
    Unavailable,
    Failed(String),
}

fn fetch_cran_like_archive_versions(
    source: &RepositorySource,
    package: &str,
) -> Result<Vec<VersionSummary>, CranLikeArchiveError> {
    let url = format!("{}/src/contrib/Archive/{package}/", source.base_url());
    let response = reqwest::blocking::get(&url).map_err(|_| CranLikeArchiveError::Unavailable)?;
    let status = response.status();
    if status == reqwest::StatusCode::NOT_FOUND || status == reqwest::StatusCode::FORBIDDEN {
        return Err(CranLikeArchiveError::Unavailable);
    }
    if !status.is_success() {
        return Err(CranLikeArchiveError::Failed(unexpected_cran_like_response(
            response,
        )));
    }

    let body = response.text().map_err(|error| {
        CranLikeArchiveError::Failed(format!("failed to read CRAN-like archive listing: {error}"))
    })?;
    let mut versions = tarball_file_names_from_listing(&body)
        .into_iter()
        .filter_map(|file_name| {
            parse_cran_tarball_file_name(&file_name).and_then(|(name, version)| {
                (name == package).then(|| VersionSummary {
                    version: version.to_string(),
                    source_url: cran_like_archive_tarball_url(source, package, version),
                })
            })
        })
        .collect::<Vec<_>>();
    versions.sort_by(|left, right| left.version.cmp(&right.version));
    Ok(versions)
}

fn cran_like_current_tarball_url(
    source: &RepositorySource,
    package: &str,
    version: &str,
) -> String {
    format!(
        "{}/src/contrib/{package}_{version}.tar.gz",
        source.base_url()
    )
}

fn cran_like_archive_tarball_url(
    source: &RepositorySource,
    package: &str,
    version: &str,
) -> String {
    format!(
        "{}/src/contrib/Archive/{package}/{package}_{version}.tar.gz",
        source.base_url()
    )
}

fn parse_dcf_records(input: &str) -> Vec<BTreeMap<String, String>> {
    let mut records = Vec::new();
    let mut record: BTreeMap<String, String> = BTreeMap::new();
    let mut current_key: Option<String> = None;

    for line in input.lines() {
        if line.trim().is_empty() {
            if !record.is_empty() {
                records.push(record);
                record = BTreeMap::new();
                current_key = None;
            }
            continue;
        }

        if line.starts_with(' ') || line.starts_with('\t') {
            if let Some(key) = &current_key
                && let Some(value) = record.get_mut(key)
            {
                value.push(' ');
                value.push_str(line.trim());
            }
            continue;
        }

        let Some((key, value)) = line.split_once(':') else {
            current_key = None;
            continue;
        };
        let key = key.trim().to_string();
        record.insert(key.clone(), value.trim().to_string());
        current_key = Some(key);
    }

    if !record.is_empty() {
        records.push(record);
    }

    records
}

fn tarball_file_names_from_listing(listing: &str) -> Vec<String> {
    listing
        .split(['"', '\'', '<', '>', ' ', '\n', '\r', '\t'])
        .filter_map(|part| part.rsplit('/').next())
        .filter(|part| part.ends_with(".tar.gz") && part.contains('_'))
        .map(html_unescape_minimal)
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect()
}

fn parse_cran_tarball_file_name(file_name: &str) -> Option<(&str, &str)> {
    let stem = file_name.strip_suffix(".tar.gz")?;
    let (package, version) = stem.rsplit_once('_')?;
    if package.is_empty() || version.is_empty() {
        return None;
    }
    Some((package, version))
}

fn html_unescape_minimal(value: &str) -> String {
    value
        .replace("&amp;", "&")
        .replace("&lt;", "<")
        .replace("&gt;", ">")
        .replace("&quot;", "\"")
}

fn fetch_description_from_tarball(url: &str, package: &str) -> Result<String, String> {
    let response = reqwest::blocking::get(url)
        .map_err(|error| format!("failed to download source package for DESCRIPTION: {error}"))?;
    let status = response.status();
    if !status.is_success() {
        return Err(unexpected_cran_like_response(response));
    }

    let bytes = response
        .bytes()
        .map_err(|error| format!("failed to read source package for DESCRIPTION: {error}"))?;
    let decoder = GzDecoder::new(bytes.as_ref());
    let mut archive = Archive::new(decoder);
    let entries = archive
        .entries()
        .map_err(|error| format!("failed to read source package archive: {error}"))?;

    for entry in entries {
        let mut entry = entry.map_err(|error| format!("failed to read archive entry: {error}"))?;
        let is_description = {
            let path = entry
                .path()
                .map_err(|error| format!("failed to read archive entry path: {error}"))?;
            path_is_top_level_description(&path, package)
        };
        if !is_description {
            continue;
        }

        let mut description = String::new();
        entry
            .read_to_string(&mut description)
            .map_err(|error| format!("failed to read DESCRIPTION from source package: {error}"))?;
        if description.trim().is_empty() {
            return Err(format!(
                "source package {url} contains an empty DESCRIPTION"
            ));
        }
        if !description_declares_package(&description, package) {
            return Err(format!(
                "source package {url} DESCRIPTION does not describe package {package}"
            ));
        }
        return Ok(description);
    }

    Err(format!(
        "source package {url} does not contain {package}/DESCRIPTION"
    ))
}

fn path_is_top_level_description(path: &Path, package: &str) -> bool {
    let mut components = path.components().filter_map(|component| {
        let component = component.as_os_str().to_str()?;
        (component != ".").then_some(component)
    });

    components.next() == Some(package)
        && components.next() == Some("DESCRIPTION")
        && components.next().is_none()
}

fn description_declares_package(description: &str, package: &str) -> bool {
    parse_dcf_records(description)
        .first()
        .and_then(|record| record.get("Package"))
        .is_some_and(|name| name == package)
}

fn cran_like_description_cache_path(
    source: &RepositorySource,
    package: &str,
    version: &str,
) -> PathBuf {
    cache_dir_path()
        .join("cran-like")
        .join(hash_string(source.base_url()))
        .join("descriptions")
        .join(package)
        .join(format!("{version}.dcf"))
}

fn write_text_cache(path: &Path, value: &str) {
    if let Some(parent) = path.parent() {
        let _ = fs::create_dir_all(parent);
    }
    let _ = fs::write(path, value);
}

fn unexpected_cran_like_response(response: reqwest::blocking::Response) -> String {
    let status = response.status();
    let body = response.text().unwrap_or_default();
    let body = body.trim();

    if body.is_empty() {
        return format!("unexpected registry response ({status})");
    }

    format!("unexpected registry response ({status}): {body}")
}

fn missing_package_error(package: &str) -> String {
    format!("unexpected registry response (404 Not Found): package {package} not found")
}

fn keyring_entry(source: &RepositorySource) -> Result<Entry, String> {
    Entry::new(KEYRING_SERVICE, &keyring_account_name(source))
        .map_err(|error| format!("failed to access local keyring: {error}"))
}

fn keyring_account_name(source: &RepositorySource) -> String {
    format!("repo:{}", hash_string(source.base_url()))
}

fn hash_string(value: &str) -> String {
    let mut hasher = DefaultHasher::new();
    value.hash(&mut hasher);
    format!("{:016x}", hasher.finish())
}

#[cfg(test)]
mod tests {
    use super::*;
    use flate2::{Compression, write::GzEncoder};
    use mockito::Server;
    use std::{collections::BTreeMap, io::Write as _, sync::Mutex};

    #[derive(Default)]
    struct MemoryCredentialStore {
        values: Mutex<BTreeMap<String, String>>,
    }

    impl CredentialStore for MemoryCredentialStore {
        fn get(&self, source: &RepositorySource) -> Result<Option<String>, String> {
            Ok(self
                .values
                .lock()
                .expect("memory store should lock")
                .get(source.base_url())
                .cloned())
        }

        fn set(&self, source: &RepositorySource, token: &str) -> Result<(), String> {
            self.values
                .lock()
                .expect("memory store should lock")
                .insert(source.base_url().to_string(), token.to_string());
            Ok(())
        }

        fn delete(&self, source: &RepositorySource) -> Result<(), String> {
            self.values
                .lock()
                .expect("memory store should lock")
                .remove(source.base_url());
            Ok(())
        }
    }

    struct StaticPrompter {
        token: String,
    }

    impl ApiKeyPrompter for StaticPrompter {
        fn prompt(
            &self,
            _source: &RepositorySource,
            _had_stored_token: bool,
        ) -> Result<String, String> {
            Ok(self.token.clone())
        }
    }

    #[test]
    fn derives_repository_source_from_package_url() {
        assert_eq!(
            repository_source_from_package_url(
                "https://scalerail.rrepo.dev/test/packages/rpxsmoke/versions/0.0.1/source"
            )
            .as_deref(),
            Some("https://scalerail.rrepo.dev/test")
        );
    }

    #[test]
    fn derives_cran_like_repository_source_from_package_url() {
        assert_eq!(
            cran_like_source_from_package_url(
                "https://cran.example/src/contrib/Archive/digest/digest_0.6.37.tar.gz"
            )
            .as_deref(),
            Some("https://cran.example")
        );
    }

    #[test]
    fn normalizes_repository_urls() {
        assert_eq!(
            normalize_repository_url(" https://scalerail.rrepo.dev/test/ "),
            "https://scalerail.rrepo.dev/test"
        );
    }

    #[test]
    fn prefers_explicit_source_match_for_package_url_lookup() {
        let repositories = RepositorySet::with_support(
            vec![
                RepositorySource::new("https://api.rrepo.org"),
                RepositorySource::new("https://scalerail.rrepo.dev/test"),
            ],
            Arc::new(MemoryCredentialStore::default()),
            Arc::new(StaticPrompter {
                token: "secret".to_string(),
            }),
        );

        let source = repositories
            .source_for_url(
                "https://scalerail.rrepo.dev/test/packages/rpxsmoke/versions/0.0.1/source",
            )
            .expect("source should be derived");

        assert_eq!(source.base_url(), "https://scalerail.rrepo.dev/test");
    }

    #[test]
    fn prefers_default_repository_before_additional_repositories() {
        let mut default_server = Server::new();
        let mut additional_server = Server::new();

        let default_mock = default_server
            .mock("GET", "/packages/digest/versions")
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(
                r#"{
  "package": "digest",
  "versions": [
    {
      "version": "0.6.37",
      "sourceUrl": "https://api.rrepo.org/packages/digest/versions/0.6.37/source"
    }
  ]
}"#,
            )
            .expect(1)
            .create();
        let additional_mock = additional_server
            .mock("GET", "/packages/digest/versions")
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(
                r#"{
  "package": "digest",
  "versions": [
    {
      "version": "9.9.9",
      "sourceUrl": "https://example.test/packages/digest/versions/9.9.9/source"
    }
  ]
}"#,
            )
            .expect(0)
            .create();

        let repositories = RepositorySet::with_support(
            vec![
                RepositorySource::new(default_server.url()),
                RepositorySource::new(additional_server.url()),
            ],
            Arc::new(MemoryCredentialStore::default()),
            Arc::new(StaticPrompter {
                token: "secret".to_string(),
            }),
        );

        let result = repositories
            .fetch_package_versions_with_retry("digest")
            .expect("package versions should resolve");

        default_mock.assert();
        additional_mock.assert();
        assert_eq!(result.source.base_url(), default_server.url());
        assert_eq!(result.response.versions[0].version, "0.6.37");
    }

    #[test]
    fn fetches_cran_like_versions_from_current_and_archive_listings() {
        let mut server = Server::new();
        let current_mock = server
            .mock("GET", "/src/contrib/PACKAGES")
            .with_status(200)
            .with_header("content-type", "text/plain")
            .with_body(
                r#"Package: digest
Version: 0.6.38

Package: rlang
Version: 1.1.6
"#,
            )
            .expect(1)
            .create();
        let archive_mock = server
            .mock("GET", "/src/contrib/Archive/digest/")
            .with_status(200)
            .with_header("content-type", "text/html")
            .with_body(
                r#"<a href="digest_0.6.37.tar.gz">digest_0.6.37.tar.gz</a>
<a href="digest_0.6.38.tar.gz">digest_0.6.38.tar.gz</a>"#,
            )
            .expect(1)
            .create();
        let source = RepositorySource::cran_like(server.url());
        let repositories = RepositorySet::with_support(
            vec![source.clone()],
            Arc::new(MemoryCredentialStore::default()),
            Arc::new(StaticPrompter {
                token: "secret".to_string(),
            }),
        );

        let result = repositories
            .fetch_package_versions_with_retry("digest")
            .expect("CRAN-like versions should resolve");

        current_mock.assert();
        archive_mock.assert();
        assert_eq!(result.source.kind(), RepositoryKind::CranLike);
        assert_eq!(
            result
                .response
                .versions
                .iter()
                .map(|version| version.version.as_str())
                .collect::<Vec<_>>(),
            vec!["0.6.37", "0.6.38"]
        );
        assert_eq!(
            result.response.versions[1].source_url,
            format!("{}/src/contrib/digest_0.6.38.tar.gz", server.url())
        );
    }

    #[test]
    fn resolves_cran_like_current_versions_when_archive_listing_is_unavailable() {
        let mut server = Server::new();
        let current_mock = server
            .mock("GET", "/src/contrib/PACKAGES")
            .with_status(200)
            .with_header("content-type", "text/plain")
            .with_body("Package: digest\nVersion: 0.6.38\n")
            .expect(1)
            .create();
        let archive_mock = server
            .mock("GET", "/src/contrib/Archive/digest/")
            .with_status(404)
            .expect(1)
            .create();
        let source = RepositorySource::cran_like(server.url());
        let repositories = RepositorySet::with_support(
            vec![source.clone()],
            Arc::new(MemoryCredentialStore::default()),
            Arc::new(StaticPrompter {
                token: "secret".to_string(),
            }),
        );

        let result = repositories
            .fetch_package_versions_with_retry("digest")
            .expect("CRAN-like versions should resolve");

        current_mock.assert();
        archive_mock.assert();
        assert_eq!(result.response.versions[0].version, "0.6.38");
        assert_eq!(
            repositories.cran_archive_unavailable_repositories(),
            vec![server.url()]
        );
    }

    #[test]
    fn fetches_cran_like_description_from_source_tarball() {
        let mut server = Server::new();
        let current_mock = server
            .mock("GET", "/src/contrib/PACKAGES")
            .with_status(200)
            .with_header("content-type", "text/plain")
            .with_body("Package: digest\nVersion: 0.6.38\n")
            .expect(1)
            .create();
        let archive_mock = server
            .mock("GET", "/src/contrib/Archive/digest/")
            .with_status(404)
            .expect(1)
            .create();
        let tarball_mock = server
            .mock("GET", "/src/contrib/digest_0.6.38.tar.gz")
            .with_status(200)
            .with_header("content-type", "application/gzip")
            .with_body(source_tarball(
                "digest",
                "Package: digest\nVersion: 0.6.38\nImports: utils\n",
            ))
            .expect(1)
            .create();
        let source = RepositorySource::cran_like(server.url());
        write_text_cache(
            &cran_like_description_cache_path(&source, "digest", "0.6.38"),
            "",
        );
        let repositories = RepositorySet::with_support(
            vec![source.clone()],
            Arc::new(MemoryCredentialStore::default()),
            Arc::new(StaticPrompter {
                token: "secret".to_string(),
            }),
        );

        let description = repositories
            .fetch_description_with_retry(&source, "digest", "0.6.38")
            .expect("DESCRIPTION should be extracted");

        current_mock.assert();
        archive_mock.assert();
        tarball_mock.assert();
        assert!(description.contains("Package: digest"));
        assert!(description.contains("Imports: utils"));
    }

    #[test]
    fn fetches_cran_like_description_from_top_level_source_tarball_entry() {
        let mut server = Server::new();
        let current_mock = server
            .mock("GET", "/src/contrib/rprojroot_2.1.1.tar.gz")
            .with_status(200)
            .with_header("content-type", "application/gzip")
            .with_body(source_tarball_entries(&[
                ("rprojroot/tests/testthat/package/DESCRIPTION", ""),
                (
                    "rprojroot/tests/testthat/hierarchy/DESCRIPTION",
                    "Package: hierarchy\nVersion: 0.0-0\n",
                ),
                (
                    "rprojroot/DESCRIPTION",
                    "Package: rprojroot\nVersion: 2.1.1\nDepends: R (>= 3.0.0)\n",
                ),
            ]))
            .expect(1)
            .create();

        let description = fetch_description_from_tarball(
            &format!("{}/src/contrib/rprojroot_2.1.1.tar.gz", server.url()),
            "rprojroot",
        )
        .expect("top-level DESCRIPTION should be extracted");

        current_mock.assert();
        assert!(description.contains("Package: rprojroot"));
        assert!(!description.contains("Package: hierarchy"));
    }

    fn source_tarball(package: &str, description: &str) -> Vec<u8> {
        source_tarball_entries(&[(&format!("{package}/DESCRIPTION"), description)])
    }

    fn source_tarball_entries(entries: &[(&str, &str)]) -> Vec<u8> {
        let mut tar = Vec::new();
        {
            let mut builder = tar::Builder::new(&mut tar);
            for (path, description) in entries {
                let mut header = tar::Header::new_gnu();
                header.set_size(description.len() as u64);
                header.set_mode(0o644);
                header.set_cksum();
                builder
                    .append_data(&mut header, path, description.as_bytes())
                    .expect("DESCRIPTION should be written to tarball");
            }
            builder.finish().expect("tarball should finish");
        }

        let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
        encoder.write_all(&tar).expect("tarball should gzip");
        encoder.finish().expect("gzip should finish")
    }
}
