use crate::registry::{
    LatestVersionResponse, PackageVersionsResponse, RegistryClient, RepositoryPackagesResponse,
    is_not_found_error, is_unauthorized_error,
};
use keyring::Entry;
use std::{
    collections::{BTreeSet, hash_map::DefaultHasher},
    hash::{Hash, Hasher},
    io::{self, IsTerminal, Write},
    sync::Arc,
};

const KEYRING_SERVICE: &str = "rpx";

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct RepositorySource {
    base_url: String,
}

#[derive(Debug, Clone)]
pub struct SourcedLatestVersion {
    pub source: RepositorySource,
    pub response: LatestVersionResponse,
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
        Self {
            base_url: normalize_repository_url(base_url.as_ref()),
        }
    }

    pub fn base_url(&self) -> &str {
        &self.base_url
    }

    pub fn matches_source_url(&self, url: &str) -> bool {
        url.starts_with(self.base_url())
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
            if seen.insert(source.base_url.clone()) {
                deduped.push(source);
            }
        }

        Self {
            sources: deduped,
            credentials,
            prompter,
        }
    }

    pub fn sources(&self) -> &[RepositorySource] {
        &self.sources
    }

    pub fn fetch_repository_packages(
        &self,
        source: &RepositorySource,
    ) -> Result<RepositoryPackagesResponse, String> {
        self.with_authorized_client(source, |client| client.fetch_repository_packages())
    }

    pub fn fetch_latest_version_with_retry(
        &self,
        package: &str,
    ) -> Result<SourcedLatestVersion, String> {
        for source in &self.sources {
            match self.with_authorized_client(source, |client| {
                client.fetch_latest_version_with_retry(package)
            }) {
                Ok(response) => {
                    return Ok(SourcedLatestVersion {
                        source: source.clone(),
                        response,
                    });
                }
                Err(error) if is_not_found_error(&error) => continue,
                Err(error) => return Err(error),
            }
        }

        Err(format!(
            "package {package} not found in configured repositories"
        ))
    }

    pub fn fetch_package_versions_with_retry(
        &self,
        package: &str,
    ) -> Result<SourcedPackageVersions, String> {
        for source in &self.sources {
            match self.with_authorized_client(source, |client| {
                client.fetch_package_versions_with_retry(package)
            }) {
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
        self.with_authorized_client(source, |client| {
            client.fetch_description_with_retry(package, version)
        })
    }

    pub fn download_artifact(
        &self,
        source: &RepositorySource,
        package: &str,
        version: &str,
        artifact: &crate::registry::ArtifactRequest,
    ) -> Result<crate::registry::DownloadedArtifact, String> {
        self.with_authorized_client(source, |client| {
            client.download_artifact(package, version, artifact)
        })
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

    pub fn store_api_key(&self, source: &RepositorySource, token: &str) -> Result<(), String> {
        self.credentials.set(source, token)
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

        io::stderr()
            .write_all(prompt.as_bytes())
            .map_err(|error| format!("failed to prompt for API key: {error}"))?;
        io::stderr()
            .flush()
            .map_err(|error| format!("failed to prompt for API key: {error}"))?;

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
    use mockito::Server;
    use std::{collections::BTreeMap, sync::Mutex};

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
}
