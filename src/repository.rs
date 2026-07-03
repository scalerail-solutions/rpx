use crate::http::CranPackagesIndex;
use crate::output::try_prompt;
use crate::registry::{
    PackageVersionsResponse, RegistryClient, VersionSummary, is_not_found_error,
    is_unauthorized_error,
};
use flate2::read::GzDecoder;
use keyring::Entry;
use moka::future::Cache;
use std::{
    collections::{BTreeMap, BTreeSet, hash_map::DefaultHasher},
    fs,
    hash::{Hash, Hasher},
    io::{self, IsTerminal, Read},
    path::{Path, PathBuf},
    sync::{Arc, OnceLock},
};
use tar::Archive;
use tokio::task::JoinSet;

use crate::project::cache_dir_path;
use crate::r_version::compare_r_versions;

const KEYRING_SERVICE: &str = "rpx";
static REPOSITORY_HTTP_RUNTIME: OnceLock<tokio::runtime::Runtime> = OnceLock::new();

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct RepositorySource {
    base_url: String,
    kind: RepositoryKind,
    cran_archive_support: Option<CranArchiveSupport>,
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
    cran_current_indexes: Cache<String, Arc<CranPackagesIndex>>,
    cran_archive_unavailable: Arc<std::sync::Mutex<BTreeSet<String>>>,
    cran_archive_listing_support: Arc<std::sync::Mutex<BTreeMap<String, CranArchiveSupport>>>,
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
            cran_archive_support: None,
        }
    }

    pub(crate) fn cran_like_with_archive_support(
        base_url: impl AsRef<str>,
        support: CranArchiveSupport,
    ) -> Self {
        Self {
            base_url: normalize_repository_url(base_url.as_ref()),
            kind: RepositoryKind::CranLike,
            cran_archive_support: Some(support),
        }
    }

    pub fn base_url(&self) -> &str {
        &self.base_url
    }

    pub fn kind(&self) -> RepositoryKind {
        self.kind
    }

    pub(crate) fn cran_archive_support(&self) -> Option<CranArchiveSupport> {
        self.cran_archive_support
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
        let cran_archive_listing_support = deduped
            .iter()
            .filter_map(|source| {
                source
                    .cran_archive_support()
                    .map(|support| (source.base_url().to_string(), support))
            })
            .collect::<BTreeMap<_, _>>();
        let cran_archive_unavailable = cran_archive_listing_support
            .iter()
            .filter_map(|(url, support)| {
                (*support == CranArchiveSupport::Unavailable).then_some(url.clone())
            })
            .collect::<BTreeSet<_>>();

        Self {
            sources: deduped,
            credentials,
            prompter,
            cran_current_indexes: Cache::new(64),
            cran_archive_unavailable: Arc::new(std::sync::Mutex::new(cran_archive_unavailable)),
            cran_archive_listing_support: Arc::new(std::sync::Mutex::new(
                cran_archive_listing_support,
            )),
        }
    }

    pub fn sources(&self) -> &[RepositorySource] {
        &self.sources
    }

    #[cfg(test)]
    pub fn fetch_package_versions_with_retry(
        &self,
        package: &str,
    ) -> Result<SourcedPackageVersions, String> {
        for source in &self.sources {
            let result = match source.kind() {
                RepositoryKind::Rrepo => self.with_authorized_client(source, |client| {
                    client.fetch_package_versions_with_retry(package)
                }),
                RepositoryKind::CranLike => REPOSITORY_HTTP_RUNTIME
                    .get_or_init(|| {
                        tokio::runtime::Runtime::new()
                            .expect("repository HTTP runtime should start")
                    })
                    .block_on(self.fetch_cran_like_package_versions(source, package)),
            };

            match result {
                Ok(response) => {
                    return Ok(SourcedPackageVersions {
                        source: source.clone(),
                        response,
                    });
                }
                Err(error) if is_not_found_error(&error) => {}
                Err(error) => return Err(error),
            }
        }

        Err(format!(
            "unexpected registry response (404 Not Found): package {package} not found"
        ))
    }

    pub fn fetch_all_package_versions_with_retry(
        &self,
        package: &str,
    ) -> Result<Vec<SourcedPackageVersions>, String> {
        let mut responses = Vec::new();

        for source in &self.sources {
            let result = match source.kind() {
                RepositoryKind::Rrepo => self.with_authorized_client(source, |client| {
                    client.fetch_package_versions_with_retry(package)
                }),
                RepositoryKind::CranLike => REPOSITORY_HTTP_RUNTIME
                    .get_or_init(|| {
                        tokio::runtime::Runtime::new()
                            .expect("repository HTTP runtime should start")
                    })
                    .block_on(self.fetch_cran_like_package_versions(source, package)),
            };

            match result {
                Ok(response) => responses.push(SourcedPackageVersions {
                    source: source.clone(),
                    response,
                }),
                Err(error) if is_not_found_error(&error) => {}
                Err(error) => return Err(error),
            }
        }

        if responses.is_empty() {
            return Err(format!(
                "unexpected registry response (404 Not Found): package {package} not found"
            ));
        }

        Ok(responses)
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
            RepositoryKind::CranLike => Self::fetch_cran_like_description(source, package, version),
        }
    }

    pub fn has_stored_credential(&self, source: &RepositorySource) -> Result<bool, String> {
        Ok(self.credentials.get(source)?.is_some())
    }

    pub fn remove_api_key(&self, source: &RepositorySource) -> Result<(), String> {
        self.credentials.delete(source)
    }

    pub fn cran_archive_unavailable_repositories(&self) -> Vec<String> {
        self.cran_archive_unavailable
            .lock()
            .expect("CRAN-like archive availability should lock")
            .iter()
            .cloned()
            .collect()
    }

    async fn fetch_cran_like_package_versions(
        &self,
        source: &RepositorySource,
        package: &str,
    ) -> Result<PackageVersionsResponse, String> {
        let base_url = reqwest::Url::parse(source.base_url()).map_err(|error| {
            format!(
                "invalid CRAN-like repository URL {}: {error}",
                source.base_url()
            )
        })?;
        let client = crate::http::client();
        let mut by_version = BTreeMap::new();

        match self.cran_archive_support(source) {
            Some(CranArchiveSupport::Unavailable) => {
                let index = self.cran_like_current_index(source).await?;
                for entry in index
                    .packages
                    .iter()
                    .filter(|entry| entry.package == package)
                {
                    let version = VersionSummary {
                        version: entry.version.clone(),
                        source_url: cran_like_current_tarball_url(
                            source,
                            &entry.package,
                            &entry.version,
                        ),
                    };
                    by_version.insert(version.version.clone(), version);
                }
            }
            Some(CranArchiveSupport::Available) => {
                let index = self.cran_like_current_index(source).await?;
                let archive =
                    crate::http::cran_package_archive_listing(&client, &base_url, package).await;
                for entry in index
                    .packages
                    .iter()
                    .filter(|entry| entry.package == package)
                {
                    let version = VersionSummary {
                        version: entry.version.clone(),
                        source_url: cran_like_current_tarball_url(
                            source,
                            &entry.package,
                            &entry.version,
                        ),
                    };
                    by_version.insert(version.version.clone(), version);
                }

                match archive {
                    Ok(Some(archive)) => {
                        for version in archive.versions {
                            let version = version.to_string();
                            by_version.entry(version.clone()).or_insert(VersionSummary {
                                source_url: cran_like_archive_tarball_url(
                                    source, package, &version,
                                ),
                                version,
                            });
                        }
                    }
                    Ok(None) | Err(crate::http::HttpError::RequestFailed { .. }) => {}
                    Err(crate::http::HttpError::UnexpectedStatus { status, .. })
                        if status == reqwest::StatusCode::FORBIDDEN => {}
                    Err(error) => return Err(error.to_string()),
                }
            }
            None => {
                let index = self.cran_like_current_index(source).await?;
                let root_archive = crate::http::cran_archive_root(&client, &base_url).await;
                let package_archive =
                    crate::http::cran_package_archive_listing(&client, &base_url, package).await;
                for entry in index
                    .packages
                    .iter()
                    .filter(|entry| entry.package == package)
                {
                    let version = VersionSummary {
                        version: entry.version.clone(),
                        source_url: cran_like_current_tarball_url(
                            source,
                            &entry.package,
                            &entry.version,
                        ),
                    };
                    by_version.insert(version.version.clone(), version);
                }

                match (&root_archive, &package_archive) {
                    (_, Ok(Some(_))) => {
                        self.record_cran_archive_support(source, CranArchiveSupport::Available);
                    }
                    (Ok(_), _) => {
                        self.record_cran_archive_support(source, CranArchiveSupport::Available);
                    }
                    (Err(crate::http::HttpError::UnexpectedStatus { status, .. }), _)
                        if *status == reqwest::StatusCode::NOT_FOUND
                            || *status == reqwest::StatusCode::FORBIDDEN =>
                    {
                        self.record_cran_archive_support(source, CranArchiveSupport::Unavailable);
                    }
                    (Err(crate::http::HttpError::RequestFailed { .. }), _) => {}
                    (Err(error), _) => return Err(error.to_string()),
                }

                let root_archive_unavailable = matches!(
                    root_archive,
                    Err(crate::http::HttpError::UnexpectedStatus { status, .. })
                        if status == reqwest::StatusCode::NOT_FOUND
                            || status == reqwest::StatusCode::FORBIDDEN
                );

                match package_archive {
                    Ok(Some(archive)) => {
                        for version in archive.versions {
                            let version = version.to_string();
                            by_version.entry(version.clone()).or_insert(VersionSummary {
                                source_url: cran_like_archive_tarball_url(
                                    source, package, &version,
                                ),
                                version,
                            });
                        }
                    }
                    Ok(None) | Err(crate::http::HttpError::RequestFailed { .. }) => {}
                    Err(_) if root_archive_unavailable => {}
                    Err(crate::http::HttpError::UnexpectedStatus { status, .. })
                        if status == reqwest::StatusCode::FORBIDDEN => {}
                    Err(error) => return Err(error.to_string()),
                }
            }
        }

        package_versions_response(package, by_version)
    }

    fn cran_archive_support(&self, source: &RepositorySource) -> Option<CranArchiveSupport> {
        self.cran_archive_listing_support
            .lock()
            .expect("CRAN-like archive listing support should lock")
            .get(source.base_url())
            .copied()
    }

    fn record_cran_archive_support(&self, source: &RepositorySource, support: CranArchiveSupport) {
        self.cran_archive_listing_support
            .lock()
            .expect("CRAN-like archive listing support should lock")
            .insert(source.base_url().to_string(), support);
        if support == CranArchiveSupport::Unavailable {
            self.cran_archive_unavailable
                .lock()
                .expect("CRAN-like archive availability should lock")
                .insert(source.base_url().to_string());
        }
    }

    async fn cran_like_current_index(
        &self,
        source: &RepositorySource,
    ) -> Result<Arc<CranPackagesIndex>, String> {
        let key = source.base_url().to_string();

        self.cran_current_indexes
            .try_get_with(key.clone(), async move {
                let base_url = reqwest::Url::parse(&key)
                    .map_err(|error| format!("invalid CRAN-like repository URL {key}: {error}"))?;
                let client = crate::http::client();

                let index = crate::http::cran_packages(&client, &base_url)
                    .await
                    .map_err(|error| {
                        format!("failed to fetch CRAN-like PACKAGES index from {key}: {error}")
                    })?;

                Ok(Arc::new(index))
            })
            .await
            .map_err(|error: Arc<String>| error.as_ref().clone())
    }

    fn fetch_cran_like_description(
        source: &RepositorySource,
        package: &str,
        version: &str,
    ) -> Result<String, String> {
        let path = cran_like_description_cache_path(source, package, version);
        if let Ok(description) = fs::read_to_string(&path) {
            if description_declares_package_version(&description, package, version) {
                return Ok(description);
            }
            let _ = fs::remove_file(&path);
        }

        let description = fetch_description_from_cran_like_tarballs(source, package, version)?;
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
            Err(_) => Ok(None),
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) enum CranArchiveSupport {
    Available,
    Unavailable,
}

fn sort_version_summaries(versions: &mut [VersionSummary]) {
    versions.sort_by(|left, right| compare_r_versions(&left.version, &right.version));
}

fn package_versions_response(
    package: &str,
    by_version: BTreeMap<String, VersionSummary>,
) -> Result<PackageVersionsResponse, String> {
    if by_version.is_empty() {
        return Err(missing_package_error(package));
    }

    let mut versions = by_version.into_values().collect::<Vec<_>>();
    sort_version_summaries(&mut versions);

    Ok(PackageVersionsResponse {
        package: package.to_string(),
        versions,
    })
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

fn cran_like_current_description_url(source: &RepositorySource, package: &str) -> String {
    format!("{}/web/packages/{package}/DESCRIPTION", source.base_url())
}

fn fetch_description_from_cran_like_tarballs(
    source: &RepositorySource,
    package: &str,
    version: &str,
) -> Result<String, String> {
    let current_url = cran_like_current_tarball_url(source, package, version);
    let archive_url = cran_like_archive_tarball_url(source, package, version);
    let direct_url = cran_like_current_description_url(source, package);
    let candidates = [
        DescriptionCandidate::direct(direct_url, package, version),
        DescriptionCandidate::tarball(current_url, package, version),
        DescriptionCandidate::tarball(archive_url, package, version),
    ];
    REPOSITORY_HTTP_RUNTIME
        .get_or_init(|| {
            tokio::runtime::Runtime::new().expect("DESCRIPTION fetch runtime should start")
        })
        .block_on(fetch_first_description_candidate(candidates))
}

async fn fetch_first_description_candidate(
    candidates: [DescriptionCandidate; 3],
) -> Result<String, String> {
    let mut tasks = JoinSet::new();
    for candidate in candidates {
        tasks.spawn(async move {
            let url = candidate.url.clone();
            (url, candidate.fetch_async().await)
        });
    }

    let mut errors = Vec::new();
    while let Some(result) = tasks.join_next().await {
        match result {
            Ok((_, Ok(description))) => {
                tasks.abort_all();
                return Ok(description);
            }
            Ok((url, Err(error))) => errors.push((url, error)),
            Err(error) => errors.push((
                "DESCRIPTION worker".to_string(),
                format!("failed to join DESCRIPTION result: {error}"),
            )),
        }
    }

    Err(combine_description_errors(errors))
}

struct DescriptionCandidate {
    url: String,
    package: String,
    version: String,
    kind: DescriptionCandidateKind,
}

enum DescriptionCandidateKind {
    Direct,
    Tarball,
}

impl DescriptionCandidate {
    fn direct(url: String, package: &str, version: &str) -> Self {
        Self {
            url,
            package: package.to_string(),
            version: version.to_string(),
            kind: DescriptionCandidateKind::Direct,
        }
    }

    fn tarball(url: String, package: &str, version: &str) -> Self {
        Self {
            url,
            package: package.to_string(),
            version: version.to_string(),
            kind: DescriptionCandidateKind::Tarball,
        }
    }

    async fn fetch_async(self) -> Result<String, String> {
        match self.kind {
            DescriptionCandidateKind::Direct => {
                fetch_description_from_direct_url_async(&self.url, &self.package, &self.version)
                    .await
            }
            DescriptionCandidateKind::Tarball => {
                fetch_description_from_tarball_async(&self.url, &self.package, &self.version).await
            }
        }
    }
}

fn combine_description_errors(errors: Vec<(String, String)>) -> String {
    if let Some((_, error)) = errors.iter().find(|(_, error)| !is_not_found_error(error)) {
        return error.clone();
    }

    let details = errors
        .into_iter()
        .map(|(url, error)| format!("{url}: {error}"))
        .collect::<Vec<_>>()
        .join("; ");
    format!("failed to fetch DESCRIPTION from any candidate location ({details})")
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

async fn fetch_description_from_direct_url_async(
    url: &str,
    package: &str,
    version: &str,
) -> Result<String, String> {
    let response = reqwest::get(url)
        .await
        .map_err(|error| format!("failed to download DESCRIPTION: {error}"))?;
    let status = response.status();
    if !status.is_success() {
        return Err(unexpected_cran_like_response_async(response).await);
    }

    let description = response
        .text()
        .await
        .map_err(|error| format!("failed to read DESCRIPTION: {error}"))?;
    validate_description(&description, package, version, url)?;
    Ok(description)
}

async fn fetch_description_from_tarball_async(
    url: &str,
    package: &str,
    version: &str,
) -> Result<String, String> {
    let response = reqwest::get(url)
        .await
        .map_err(|error| format!("failed to download source package for DESCRIPTION: {error}"))?;
    let status = response.status();
    if !status.is_success() {
        return Err(unexpected_cran_like_response_async(response).await);
    }

    let bytes = response
        .bytes()
        .await
        .map_err(|error| format!("failed to read source package for DESCRIPTION: {error}"))?;
    description_from_tarball_bytes(bytes.as_ref(), url, package, version)
}

fn description_from_tarball_bytes(
    bytes: &[u8],
    url: &str,
    package: &str,
    version: &str,
) -> Result<String, String> {
    let decoder = GzDecoder::new(bytes);
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
        validate_description(&description, package, version, url)?;
        return Ok(description);
    }

    Err(format!(
        "source package {url} does not contain {package}/DESCRIPTION"
    ))
}

fn validate_description(
    description: &str,
    package: &str,
    version: &str,
    url: &str,
) -> Result<(), String> {
    if description.trim().is_empty() {
        return Err(format!("DESCRIPTION at {url} is empty"));
    }
    if !description_declares_package_version(description, package, version) {
        return Err(format!(
            "DESCRIPTION at {url} does not describe package {package} {version}"
        ));
    }
    Ok(())
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

fn description_declares_package_version(description: &str, package: &str, version: &str) -> bool {
    let Some(record) = parse_dcf_records(description).into_iter().next() else {
        return false;
    };
    record.get("Package").is_some_and(|name| name == package)
        && record.get("Version").is_some_and(|value| value == version)
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

async fn unexpected_cran_like_response_async(response: reqwest::Response) -> String {
    let status = response.status();
    let body = response.text().await.unwrap_or_default();
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
    fn normalizes_repository_urls() {
        assert_eq!(
            normalize_repository_url(" https://scalerail.rrepo.dev/test/ "),
            "https://scalerail.rrepo.dev/test"
        );
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
                r"Package: digest
Version: 0.6.38

Package: rlang
Version: 1.1.6
",
            )
            .expect(1)
            .create();
        let archive_mock = server
            .mock("GET", "/src/contrib/Archive/")
            .with_status(200)
            .expect(1)
            .create();
        let package_archive_mock = server
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
        package_archive_mock.assert();
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
    fn uses_preclassified_cran_archive_support_for_package_versions() {
        let mut server = Server::new();
        let current_mock = server
            .mock("GET", "/src/contrib/PACKAGES")
            .with_status(200)
            .with_header("content-type", "text/plain")
            .with_body("Package: digest\nVersion: 0.6.38\n")
            .expect(1)
            .create();
        let root_archive_mock = server
            .mock("GET", "/src/contrib/Archive/")
            .with_status(200)
            .expect(0)
            .create();
        let package_archive_mock = server
            .mock("GET", "/src/contrib/Archive/digest/")
            .with_status(200)
            .with_body(r#"<a href="digest_0.6.37.tar.gz">digest_0.6.37.tar.gz</a>"#)
            .expect(1)
            .create();
        let source = RepositorySource::cran_like_with_archive_support(
            server.url(),
            CranArchiveSupport::Available,
        );
        let repositories = RepositorySet::with_support(
            vec![source],
            Arc::new(MemoryCredentialStore::default()),
            Arc::new(StaticPrompter {
                token: "secret".to_string(),
            }),
        );

        let result = repositories
            .fetch_package_versions_with_retry("digest")
            .expect("digest should resolve");

        assert_eq!(result.response.versions.len(), 2);
        current_mock.assert();
        root_archive_mock.assert();
        package_archive_mock.assert();
    }

    #[test]
    fn keeps_querying_rrepo_after_package_not_found() {
        let mut server = Server::new();
        let digest_rrepo_mock = server
            .mock("GET", "/packages/digest/versions")
            .with_status(404)
            .expect(1)
            .create();
        let rlang_rrepo_mock = server
            .mock("GET", "/packages/rlang/versions")
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(
                r#"{
  "package": "rlang",
  "versions": [
    {
      "version": "1.1.6",
      "sourceUrl": "https://example.test/packages/rlang/versions/1.1.6/source"
    }
  ]
}"#,
            )
            .expect(1)
            .create();
        let repositories = RepositorySet::with_support(
            vec![RepositorySource::new(server.url())],
            Arc::new(MemoryCredentialStore::default()),
            Arc::new(StaticPrompter {
                token: "secret".to_string(),
            }),
        );

        let result = repositories
            .fetch_package_versions_with_retry("digest")
            .expect_err("digest should remain missing");
        assert!(is_not_found_error(&result));
        let result = repositories
            .fetch_package_versions_with_retry("rlang")
            .expect("rlang should resolve from rrepo source");
        assert_eq!(result.response.versions[0].version, "1.1.6");

        digest_rrepo_mock.assert();
        rlang_rrepo_mock.assert();
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
            .mock("GET", "/src/contrib/Archive/")
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
            .mock("GET", "/src/contrib/digest_0.6.38.tar.gz")
            .with_status(200)
            .with_header("content-type", "application/gzip")
            .with_body(source_tarball(
                "digest",
                "Package: digest\nVersion: 0.6.38\nImports: utils\n",
            ))
            .expect(1)
            .create();
        let _archive_mock = server
            .mock("GET", "/src/contrib/Archive/digest/digest_0.6.38.tar.gz")
            .with_status(404)
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
        assert!(description.contains("Package: digest"));
        assert!(description.contains("Imports: utils"));
    }

    #[test]
    fn fetches_cran_like_description_from_direct_endpoint() {
        let mut server = Server::new();
        let direct_mock = server
            .mock("GET", "/web/packages/digest/DESCRIPTION")
            .with_status(200)
            .with_header("content-type", "text/plain")
            .with_body("Package: digest\nVersion: 0.6.38\nImports: utils\n")
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

        let description = repositories
            .fetch_description_with_retry(&source, "digest", "0.6.38")
            .expect("direct DESCRIPTION should be used");

        direct_mock.assert();
        assert!(description.contains("Package: digest"));
        assert!(description.contains("Version: 0.6.38"));
    }

    #[test]
    fn fetches_cran_like_description_from_archive_tarball() {
        let mut server = Server::new();
        let _current_mock = server
            .mock("GET", "/src/contrib/digest_0.6.37.tar.gz")
            .with_status(404)
            .create();
        let archive_mock = server
            .mock("GET", "/src/contrib/Archive/digest/digest_0.6.37.tar.gz")
            .with_status(200)
            .with_header("content-type", "application/gzip")
            .with_body(source_tarball(
                "digest",
                "Package: digest\nVersion: 0.6.37\nImports: utils\n",
            ))
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

        let description = repositories
            .fetch_description_with_retry(&source, "digest", "0.6.37")
            .expect("archive DESCRIPTION should be extracted");

        archive_mock.assert();
        assert!(description.contains("Package: digest"));
        assert!(description.contains("Version: 0.6.37"));
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
