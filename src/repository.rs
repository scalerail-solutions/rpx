use crate::http::{self};
use crate::registry::{is_not_found_error};
use crate::resolver::PackageVersion;
use flate2::read::GzDecoder;
use keyring::Entry;
use r_description::{Version, lossy::RDescription};
use reqwest::Url;
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, BTreeSet, hash_map::DefaultHasher},
    hash::{Hash, Hasher},
    io::{Read},
    path::{Path },
    sync::{Arc, OnceLock},
};
use tar::Archive;
use tokio::task::JoinSet;

const KEYRING_SERVICE: &str = "rpx";
static REPOSITORY_HTTP_RUNTIME: OnceLock<tokio::runtime::Runtime> = OnceLock::new();

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct RepositorySource {
    base_url: String,
    kind: RepositoryKind,
    pub cran_archive_support: ArchiveSupport,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum RepositoryKind {
    Rrepo,
    CranLike,
}

pub trait CredentialStore: Send + Sync {
    fn get(&self, source: &RepositorySource) -> Result<Option<String>, String>;
    fn delete(&self, source: &RepositorySource) -> Result<(), String>;
}

#[derive(Debug, Clone)]
pub struct KeyringCredentialStore;


#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct PackageRepository {
    url: reqwest::Url,
    repo_type: RepositoryType,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum RepositoryType {
    Cran { archives: ArchiveSupport },
    Rrepo,
}

#[derive(Debug, Serialize, Deserialize, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[serde(rename_all = "kebab-case")]
pub enum ArchiveSupport {
    Available,
    Unavailable,
}

impl PackageRepository {
    pub fn new(url: reqwest::Url, repo_type: RepositoryType) -> Self {
        Self {
            url: url,
            repo_type: repo_type,
        }
    }

    pub async fn from_url(client: &http::HttpClient, url: &str) -> Result<Self, String> {
        let normalized_url = normalize_repository_url(url);
        let base_url = reqwest::Url::parse(&normalized_url)
            .map_err(|error| format!("invalid repository URL {normalized_url}: {error}"))?;

        let rrepo_probe = http::rrepo_repository_packages(client, &base_url);
        let cran_probe = http::cran_packages(client, &base_url);
        let archive_probe = http::cran_archive_root(client, &base_url);

        let (rrepo_result, cran_result, archive_result) =
            tokio::join!(rrepo_probe, cran_probe, archive_probe);

        let repo_type = if rrepo_result.is_ok() {
            RepositoryType::Rrepo
        } else if cran_result.is_ok() {
            let archives = match archive_result {
                Ok(_) => ArchiveSupport::Available,
                Err(http::HttpError::UnexpectedStatus { status, .. })
                    if status == reqwest::StatusCode::NOT_FOUND
                        || status == reqwest::StatusCode::FORBIDDEN =>
                {
                    ArchiveSupport::Unavailable
                }
                Err(error) => return Err(error.to_string()),
            };

            RepositoryType::Cran { archives }
        } else {
            let rrepo_error = rrepo_result.expect_err("rrepo probe should have failed");
            let cran_error = cran_result.expect_err("cran probe should have failed");

            return Err(format!(
                "not an rrepo API ({rrepo_error}) or CRAN-like repository ({cran_error})"
            ));
        };

        Ok(Self {
            url: base_url,
            repo_type,
        })
    }

    pub(crate) fn repo_type(&self) -> RepositoryType {
        self.repo_type
    }

    pub(crate) fn base_url(&self) -> Url {
        self.url.clone()
    }

    #[allow(dead_code)]
    pub async fn packages(&self, client: &http::HttpClient) -> Result<Vec<String>, String> {
        match self.repo_type {
            RepositoryType::Rrepo => Ok(http::rrepo_repository_packages(client, &self.url)
                .await
                .map_err(|error| error.to_string())?
                .packages
                .into_iter()
                .map(|package| package.name)
                .collect()),
            RepositoryType::Cran { .. } => Ok(http::cran_packages(client, &self.url)
                .await
                .map_err(|error| error.to_string())?
                .packages
                .into_iter()
                .map(|package| package.package)
                .collect()),
        }
    }

    pub async fn versions(
        &self,
        client: &http::HttpClient,
        package: &str,
    ) -> Result<Vec<PackageVersion>, String> {
        let repository = Arc::new(self.clone());

        match self.repo_type {
            RepositoryType::Rrepo => http::rrepo_package_versions(client, &self.url, package)
                .await
                .map_err(|error| error.to_string())?
                .versions
                .into_iter()
                .map(|summary| {
                    let version = summary.version.parse::<Version>().map_err(|error| {
                        format!("invalid version {} for {package}: {error}", summary.version)
                    })?;

                    Ok(PackageVersion::new(version, Arc::clone(&repository)))
                })
                .collect(),

            RepositoryType::Cran { archives } => {
                let source = RepositorySource::from_package_repository(self.clone());
                let mut by_version = BTreeMap::new();

                for entry in http::cran_packages(client, &self.url)
                    .await
                    .map_err(|error| error.to_string())?
                    .packages
                    .into_iter()
                    .filter(|entry| entry.package == package)
                {
                    by_version.insert(
                        entry.version.clone(),
                        cran_like_current_tarball_url(&source, &entry.package, &entry.version),
                    );
                }

                if archives == ArchiveSupport::Available {
                    match http::cran_package_archive_listing(client, &self.url, package).await {
                        Ok(Some(archive)) => {
                            for version in archive.versions {
                                let version = version.to_string();
                                by_version.entry(version.clone()).or_insert_with(|| {
                                    cran_like_archive_tarball_url(&source, package, &version)
                                });
                            }
                        }
                        Ok(None) | Err(http::HttpError::RequestFailed { .. }) => {}
                        Err(http::HttpError::UnexpectedStatus { status, .. })
                            if status == reqwest::StatusCode::FORBIDDEN => {}
                        Err(error) => return Err(error.to_string()),
                    }
                }

                if by_version.is_empty() {
                    return Err(missing_package_error(package));
                }

                let mut versions = by_version
                    .into_iter()
                    .map(|(version, _source_url)| {
                        let parsed = version.parse::<Version>().map_err(|error| {
                            format!("invalid version {version} for {package}: {error}")
                        })?;

                        Ok(PackageVersion::new(parsed, Arc::clone(&repository)))
                    })
                    .collect::<Result<Vec<_>, String>>()?;

                versions.sort();
                Ok(versions)
            }
        }
    }

    pub async fn description(
        &self,
        client: &http::HttpClient,
        package: &str,
        version: &Version,
    ) -> Result<RDescription, String> {
        match self.repo_type {
            RepositoryType::Rrepo => {
                http::rrepo_package_description(client, &self.url, package, &version.to_string())
                    .await
                    .map_err(|error| error.to_string())
            }
            RepositoryType::Cran { .. } => {
                let source = RepositorySource::from_package_repository(self.clone());
                let description = fetch_description_from_cran_like_tarballs(
                    &source,
                    package,
                    &version.to_string(),
                )?;
                description
                    .parse::<RDescription>()
                    .map_err(|details| format!("failed to parse DESCRIPTION: {details}"))
            }
        }
    }
}

#[derive(Clone)]
pub struct RepositorySet {
    sources: Vec<RepositorySource>,
    credentials: Arc<dyn CredentialStore>,
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

    pub fn with_kind(base_url: impl AsRef<str>, kind: RepositoryKind) -> Self {
        Self {
            base_url: normalize_repository_url(base_url.as_ref()),
            kind,
            cran_archive_support: ArchiveSupport::Unavailable,
        }
    }

    pub(crate) fn cran_like_with_archive_support(
        base_url: impl AsRef<str>,
        support: ArchiveSupport,
    ) -> Self {
        Self {
            base_url: normalize_repository_url(base_url.as_ref()),
            kind: RepositoryKind::CranLike,
            cran_archive_support: support,
        }
    }

    pub(crate) fn from_package_repository(repository: PackageRepository) -> Self {
        match repository.repo_type() {
            RepositoryType::Rrepo => Self::new(repository.base_url().as_str()),
            RepositoryType::Cran { archives } => {
                Self::cran_like_with_archive_support(repository.base_url().as_str(), archives)
            }
        }
    }

    pub fn base_url(&self) -> &str {
        &self.base_url
    }

    pub fn kind(&self) -> RepositoryKind {
        self.kind
    }
}

impl RepositorySet {
    pub fn new(sources: Vec<RepositorySource>) -> Self {
        Self::with_support(
            sources,
            Arc::new(KeyringCredentialStore),
        )
    }

    pub fn with_support(
        sources: Vec<RepositorySource>,
        credentials: Arc<dyn CredentialStore>,
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
        }
    }
    pub fn has_stored_credential(&self, source: &RepositorySource) -> Result<bool, String> {
        Ok(self.credentials.get(source)?.is_some())
    }

    pub fn remove_api_key(&self, source: &RepositorySource) -> Result<(), String> {
        self.credentials.delete(source)
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

pub fn normalize_repository_url(value: &str) -> String {
    value.trim().trim_end_matches('/').to_string()
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
