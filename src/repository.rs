use crate::http;
use crate::resolver::PackageVersion;
use keyring::Entry;
use miette::Diagnostic;
use moka::future::Cache;
use r_description::lossless::{RDescription, Version};
use reqwest::Url;
use serde::{Deserialize, Serialize};
use std::{
    cmp::Ordering,
    collections::{BTreeSet, hash_map::DefaultHasher},
    hash::{Hash, Hasher},
    sync::Arc,
    time::Duration,
};
use thiserror::Error;

const KEYRING_SERVICE: &str = "rpx";

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

const PACKAGE_VERSIONS_CACHE_TTL: Duration = Duration::from_secs(15 * 60);
const CRAN_PACKAGES_CACHE_TTL: Duration = Duration::from_secs(15 * 60);
const CRAN_ARCHIVE_CACHE_TTL: Duration = Duration::from_secs(24 * 60 * 60);
#[derive(Debug, Clone)]
pub struct PackageRepository {
    url: reqwest::Url,
    repo_type: RepositoryType,
    versions: Cache<String, BTreeSet<Version>>,
    descriptions: Cache<(String, Version), Arc<RDescription>>,
    rrepo_packages: Cache<(), Arc<http::RrepoPackagesResponse>>,
    cran_packages: Cache<(), Arc<http::CranPackagesIndex>>,
    cran_archives: Cache<String, BTreeSet<Version>>,
}

pub struct PackageDependencyMetadata {
    pub depends: Option<r_description::lossy::Relations>,
    pub imports: Option<r_description::lossy::Relations>,
    pub linking_to: Option<r_description::lossy::Relations>,
    pub system_requirements: Option<String>,
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

#[derive(Debug, Error, Diagnostic)]
pub enum RepositoryError {
    #[error("failed to send request to {url}: {source}")]
    #[diagnostic(code(rpx::repository::failure))]
    RepositoryFailed {
        url: reqwest::Url,
        #[source]
        source: reqwest_middleware::Error,
    },
}

impl PackageRepository {
    pub fn new(url: reqwest::Url, repo_type: RepositoryType) -> Self {
        Self {
            url,
            repo_type,
            versions: Cache::new(1024),
            descriptions: Cache::new(4096),
            rrepo_packages: Cache::new(1),
            cran_packages: Cache::new(1),
            cran_archives: Cache::new(1024),
        }
    }

    pub async fn from_url(client: &http::HttpClient, url: &str) -> Result<Self, String> {
        let normalized_url = normalize_repository_url(url);
        let base_url = reqwest::Url::parse(&normalized_url)
            .map_err(|error| format!("invalid repository URL {normalized_url}: {error}"))?;

        let rrepo_base_url = base_url.clone();
        let rrepo_probe = async move {
            http::rrepo_repository_packages(client, &rrepo_base_url)
                .await
                .map_err(|error| error.to_string())?
                .error_for_status()
                .map_err(|error| error.to_string())?;

            Ok::<RepositoryType, String>(RepositoryType::Rrepo)
        };

        let cran_base_url = base_url.clone();
        let cran_probe = async move {
            let packages_probe = async {
                http::cran_packages(client, &cran_base_url)
                    .await
                    .map_err(|error| error.to_string())?
                    .error_for_status()
                    .map_err(|error| error.to_string())
            };

            let archive_probe = async {
                http::cran_archive_root(client, &cran_base_url)
                    .await
                    .map_err(|error| error.to_string())?
                    .error_for_status()
                    .map_err(|error| error.to_string())
            };

            let (packages_result, archive_result) = tokio::join!(packages_probe, archive_probe);

            packages_result?;

            let archives = match archive_result {
                Ok(_) => ArchiveSupport::Available,
                Err(error)
                    if error.contains("404 Not Found") || error.contains("403 Forbidden") =>
                {
                    ArchiveSupport::Unavailable
                }
                Err(error) => return Err(error),
            };

            Ok::<RepositoryType, String>(RepositoryType::Cran { archives })
        };

        tokio::pin!(rrepo_probe);
        tokio::pin!(cran_probe);

        tokio::select! {
            rrepo_result = &mut rrepo_probe => {
                match rrepo_result {
                    Ok(repo_type) => Ok(Self::new(base_url, repo_type)),
                    Err(rrepo_error) => {
                        match cran_probe.await {
                            Ok(repo_type) => Ok(Self::new(base_url, repo_type)),
                            Err(cran_error) => Err(format!(
                                "not an rrepo API ({rrepo_error}) or CRAN-like repository ({cran_error})"
                            )),
                        }
                    }
                }
            }

            cran_result = &mut cran_probe => {
                match cran_result {
                    Ok(repo_type) => Ok(Self::new(base_url, repo_type)),
                    Err(cran_error) => {
                        match rrepo_probe.await {
                            Ok(repo_type) => Ok(Self::new(base_url, repo_type)),
                            Err(rrepo_error) => Err(format!(
                                "not an rrepo API ({rrepo_error}) or CRAN-like repository ({cran_error})"
                            )),
                        }
                    }
                }
            }
        }
    }

    pub(crate) fn repo_type(&self) -> RepositoryType {
        self.repo_type
    }

    pub(crate) fn base_url(&self) -> Url {
        self.url.clone()
    }

    #[allow(dead_code)]
    pub async fn packages(&self, client: &http::HttpClient) -> Result<BTreeSet<String>, String> {
        match self.repo_type {
            RepositoryType::Rrepo => {
                let response = self
                    .rrepo_packages
                    .try_get_with((), async {
                        let response = http::rrepo_repository_packages(client, &self.url)
                            .await
                            .map_err(|error| error.to_string())?
                            .error_for_status()
                            .map_err(|error| error.to_string())?
                            .json::<http::RrepoPackagesResponse>()
                            .await
                            .map_err(|error| error.to_string())?;

                        Ok::<Arc<http::RrepoPackagesResponse>, String>(Arc::new(response))
                    })
                    .await
                    .map_err(|error| error.as_ref().clone())?;

                Ok(response
                    .packages
                    .iter()
                    .map(|package| package.name.clone())
                    .collect())
            }

            RepositoryType::Cran { .. } => {
                let index = self
                    .cran_packages
                    .try_get_with((), async {
                        let text = http::cran_packages(client, &self.url)
                            .await
                            .map_err(|error| error.to_string())?
                            .error_for_status()
                            .map_err(|error| error.to_string())?
                            .text()
                            .await
                            .map_err(|error| error.to_string())?;

                        let index = text
                            .parse::<http::CranPackagesIndex>()
                            .map_err(|error| error.to_string())?;

                        Ok::<Arc<http::CranPackagesIndex>, String>(Arc::new(index))
                    })
                    .await
                    .map_err(|error| error.as_ref().clone())?;

                Ok(index
                    .packages
                    .iter()
                    .map(|package| package.package.clone())
                    .collect())
            }
        }
    }

    pub async fn versions(
        &self,
        client: &http::HttpClient,
        package: &str,
    ) -> Result<BTreeSet<PackageVersion>, String> {
        let repository = Arc::new(self.clone());

        let versions = match self.repo_type {
            RepositoryType::Rrepo => self
                .versions
                .try_get_with(package.to_string(), async {
                    let response = http::rrepo_package_versions(client, &self.url, package)
                        .await
                        .map_err(|error| error.to_string())?
                        .error_for_status()
                        .map_err(|error| error.to_string())?
                        .json::<http::RrepoPackageVersionsResponse>()
                        .await
                        .map_err(|error| error.to_string())?;

                    response
                        .versions
                        .into_iter()
                        .map(|summary| {
                            summary.version.parse::<Version>().map_err(|error| {
                                format!(
                                    "invalid version {} for {package}: {error}",
                                    summary.version
                                )
                            })
                        })
                        .collect::<Result<BTreeSet<_>, String>>()
                })
                .await
                .map_err(|error| error.as_ref().clone())?,

            RepositoryType::Cran { archives } => {
                let index = self
                    .cran_packages
                    .try_get_with((), async {
                        let text = http::cran_packages(client, &self.url)
                            .await
                            .map_err(|error| error.to_string())?
                            .error_for_status()
                            .map_err(|error| error.to_string())?
                            .text()
                            .await
                            .map_err(|error| error.to_string())?;

                        let index = text
                            .parse::<http::CranPackagesIndex>()
                            .map_err(|error| error.to_string())?;

                        Ok::<Arc<http::CranPackagesIndex>, String>(Arc::new(index))
                    })
                    .await
                    .map_err(|error| error.as_ref().clone())?;

                let mut versions = index
                    .packages
                    .iter()
                    .filter(|entry| entry.package == package)
                    .map(|entry| {
                        entry.version.parse::<Version>().map_err(|error| {
                            format!("invalid version {} for {package}: {error}", entry.version)
                        })
                    })
                    .collect::<Result<BTreeSet<_>, String>>()?;

                if archives == ArchiveSupport::Available {
                    let archived_versions = self
                        .cran_archives
                        .try_get_with(package.to_string(), async {
                            let listing =
                                http::cran_package_archive_listing(client, &self.url, package)
                                    .await
                                    .map_err(|error| error.to_string())?
                                    .error_for_status()
                                    .map_err(|error| error.to_string())?
                                    .text()
                                    .await
                                    .map_err(|error| error.to_string())?;

                            let listing = listing
                                .parse::<http::CranPackageArchiveListing>()
                                .map_err(|error| error.to_string())?;

                            Ok::<BTreeSet<Version>, String>(listing.versions.into_iter().collect())
                        })
                        .await
                        .map_err(|error| error.as_ref().clone())?;

                    versions.extend(archived_versions);
                }

                versions
            }
        };

        tracing::trace!(
            package,
            repository = %self.url,
            versions = versions.len(),
            "loaded package versions"
        );

        Ok(versions
            .into_iter()
            .map(|version| PackageVersion::new(version, Arc::clone(&repository)))
            .collect())
    }

    pub async fn description(
        &self,
        client: &http::HttpClient,
        package: &str,
        version: &Version,
    ) -> Result<Arc<RDescription>, String> {
        let key = (package.to_string(), version.clone());

        self.descriptions
            .try_get_with(key, async {
                let description = match self.repo_type {
                    RepositoryType::Rrepo => http::rrepo_package_description(
                        client,
                        &self.url,
                        package,
                        &version.to_string(),
                    )
                    .await
                    .map_err(|error| error.to_string())?
                    .error_for_status()
                    .map_err(|error| error.to_string())?
                    .text()
                    .await
                    .map_err(|error| error.to_string())?
                    .parse::<RDescription>()
                    .map_err(|error| {
                        format!("failed to parse DESCRIPTION for {package} {version}: {error}")
                    })?,

                    RepositoryType::Cran { .. } => {
                        let index = self
                            .cran_packages
                            .try_get_with((), async {
                                let text = http::cran_packages(client, &self.url)
                                    .await
                                    .map_err(|error| error.to_string())?
                                    .error_for_status()
                                    .map_err(|error| error.to_string())?
                                    .text()
                                    .await
                                    .map_err(|error| error.to_string())?;

                                let index = text
                                    .parse::<http::CranPackagesIndex>()
                                    .map_err(|error| error.to_string())?;

                                Ok::<Arc<http::CranPackagesIndex>, String>(Arc::new(index))
                            })
                            .await
                            .map_err(|error| error.as_ref().clone())?;

                        let version_string = version.to_string();

                        if let Some(entry) = index.packages.iter().find(|entry| {
                            entry.package == package && entry.version == version_string
                        }) {
                            cran_packages_entry_to_description(entry)?
                        } else {
                            let response = http::cran_archive_source_tarball(
                                client,
                                &self.url,
                                package,
                                &version_string,
                            )
                            .await
                            .map_err(|error| error.to_string())?
                            .error_for_status()
                            .map_err(|error| error.to_string())?;

                            description_from_source_tarball_response(response, package).await?
                        }
                    }
                };

                tracing::trace!(
                    package,
                    version = %version,
                    repository = %self.url,
                    "fetched package description"
                );

                Ok::<Arc<RDescription>, String>(Arc::new(description))
            })
            .await
            .map_err(|error| error.as_ref().clone())
    }
}

fn cran_packages_entry_to_description(
    entry: &http::CranPackageIndexEntry,
) -> Result<RDescription, String> {
    let mut description = RDescription::new();

    description.set_package(&entry.package);
    description.set_version(&entry.version);

    if !entry.depends.is_empty() {
        description.set_depends(entry.depends.clone());
    }

    if !entry.imports.is_empty() {
        description.set_imports(entry.imports.clone());
    }

    if !entry.suggests.is_empty() {
        description.set_suggests(entry.suggests.clone());
    }

    if !entry.linking_to.is_empty() {
        description.set_linking_to(entry.linking_to.clone());
    }

    if let Some(system_requirements) = &entry.system_requirements {
        description.set_system_requirements(&[system_requirements]);
    }

    Ok(description)
}

async fn description_from_source_tarball_response(
    response: reqwest::Response,
    package: &str,
) -> Result<RDescription, String> {
    use futures_util::TryStreamExt;
    use std::io::Read;

    let mut bytes = Vec::with_capacity(response.content_length().unwrap_or_default() as usize);
    let mut stream = response.bytes_stream();

    while let Some(chunk) = stream
        .try_next()
        .await
        .map_err(|error| format!("failed to read source package response body: {error}"))?
    {
        bytes.extend_from_slice(&chunk);
    }

    let decoder = flate2::read::GzDecoder::new(bytes.as_slice());
    let mut archive = tar::Archive::new(decoder);

    let entries = archive
        .entries()
        .map_err(|error| format!("failed to read source package archive: {error}"))?;

    for entry in entries {
        let mut entry = entry
            .map_err(|error| format!("failed to read source package archive entry: {error}"))?;

        let is_description = {
            let path = entry
                .path()
                .map_err(|error| format!("failed to read source package archive path: {error}"))?;

            path_is_top_level_description(&path, package)
        };

        if !is_description {
            continue;
        }

        let mut body = String::new();
        entry
            .read_to_string(&mut body)
            .map_err(|error| format!("failed to read DESCRIPTION from source package: {error}"))?;

        return body.parse::<RDescription>().map_err(|error| {
            format!("failed to parse DESCRIPTION from source package for {package}: {error}")
        });
    }

    Err(format!(
        "source package does not contain {package}/DESCRIPTION"
    ))
}

fn path_is_top_level_description(path: &std::path::Path, package: &str) -> bool {
    let mut components = path.components().filter_map(|component| {
        let component = component.as_os_str().to_str()?;
        (component != ".").then_some(component)
    });

    components.next() == Some(package)
        && components.next() == Some("DESCRIPTION")
        && components.next().is_none()
}

impl PartialEq for PackageRepository {
    fn eq(&self, other: &Self) -> bool {
        self.url == other.url && self.repo_type == other.repo_type
    }
}

impl Eq for PackageRepository {}

impl PartialOrd for PackageRepository {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for PackageRepository {
    fn cmp(&self, other: &Self) -> Ordering {
        self.url
            .as_str()
            .cmp(other.url.as_str())
            .then_with(|| self.repo_type.cmp(&other.repo_type))
    }
}

impl Hash for PackageRepository {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.url.as_str().hash(state);
        self.repo_type.hash(state);
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

    pub fn base_url(&self) -> &str {
        &self.base_url
    }

    pub fn kind(&self) -> RepositoryKind {
        self.kind
    }
}

impl RepositorySet {
    pub fn new(sources: Vec<RepositorySource>) -> Self {
        Self::with_support(sources, Arc::new(KeyringCredentialStore))
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
