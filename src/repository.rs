use crate::http::{self};
use crate::resolver::PackageVersion;
use keyring::Entry;
use r_description::{Version, lossy::RDescription};
use reqwest::Url;
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeSet, hash_map::DefaultHasher},
    hash::{Hash, Hasher},
    sync::Arc,
};

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
                let current_versions = http::cran_packages(client, &self.url)
                    .await
                    .map_err(|error| error.to_string())?
                    .packages
                    .into_iter()
                    .filter(|entry| entry.package == package)
                    .map(|entry| {
                        entry.version.parse::<Version>().map_err(|error| {
                            format!("invalid version {} for {package}: {error}", entry.version)
                        })
                    })
                    .collect::<Result<BTreeSet<_>, String>>()?;

                let archived_versions = if archives == ArchiveSupport::Available {
                    match http::cran_package_archive_listing(client, &self.url, package).await {
                        Ok(Some(archive)) => archive.versions.into_iter().collect::<BTreeSet<_>>(),
                        Ok(None) | Err(http::HttpError::RequestFailed { .. }) => BTreeSet::new(),
                        Err(http::HttpError::UnexpectedStatus { status, .. })
                            if status == reqwest::StatusCode::FORBIDDEN =>
                        {
                            BTreeSet::new()
                        }
                        Err(error) => return Err(error.to_string()),
                    }
                } else {
                    BTreeSet::new()
                };

                let versions = current_versions
                    .union(&archived_versions)
                    .cloned()
                    .map(|version| PackageVersion::new(version, Arc::clone(&repository)))
                    .collect::<Vec<_>>();

                if versions.is_empty() {
                    return Err(missing_package_error(package));
                }

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
                http::cran_package_description(client, &self.url, package, &version.to_string())
                    .await
                    .map_err(|error| error.to_string())
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
