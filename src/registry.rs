use crate::project::cache_dir_path;
use reqwest::StatusCode;
use serde::{Deserialize, Serialize};
use std::{
    collections::hash_map::DefaultHasher,
    fs,
    hash::{Hash, Hasher},
    path::{Path, PathBuf},
    thread,
    time::Duration,
};

pub const DEFAULT_REGISTRY_BASE_URL: &str = "https://upstream.rrepo.dev/cran";
const VERSION_CACHE_TTL: Duration = Duration::from_secs(15 * 60);

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub struct ResolutionRoot {
    pub name: String,
    pub constraint: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct IngestingResponse {}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PackageVersionsResponse {
    pub package: String,
    pub versions: Vec<VersionSummary>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RepositoryPackagesResponse {
    #[serde(rename = "repositorySlug")]
    pub repository_slug: String,
    pub packages: Vec<RepositoryPackageSummary>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RepositoryPackageSummary {
    pub name: String,
    #[serde(rename = "latestVersion")]
    pub latest_version: String,
    #[serde(rename = "latestUploadedAt")]
    pub latest_uploaded_at: String,
    pub versions: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct VersionSummary {
    pub version: String,
    #[serde(rename = "sourceUrl")]
    pub source_url: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(untagged)]
enum PackageVersionsEnvelope {
    Complete(PackageVersionsResponse),
    Ingesting(IngestingResponse),
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum DescriptionEnvelope {
    Complete(String),
    Ingesting(IngestingResponse),
}

#[derive(Debug, Clone)]
pub struct PollConfig {
    delays: Vec<Duration>,
}

impl Default for PollConfig {
    fn default() -> Self {
        Self {
            delays: vec![
                Duration::from_secs(2),
                Duration::from_secs(4),
                Duration::from_secs(8),
                Duration::from_secs(15),
                Duration::from_secs(30),
            ],
        }
    }
}

impl PollConfig {}

#[derive(Debug)]
pub struct RegistryClient {
    base_url: String,
    token: Option<String>,
    client: reqwest::blocking::Client,
    poll_config: PollConfig,
    version_cache_ttl: Duration,
}

impl Default for RegistryClient {
    fn default() -> Self {
        Self::new(DEFAULT_REGISTRY_BASE_URL)
    }
}

impl RegistryClient {
    pub fn new(base_url: impl AsRef<str>) -> Self {
        Self::with_token_and_poll_config(base_url, None, PollConfig::default())
    }

    pub fn with_token(base_url: impl AsRef<str>, token: Option<String>) -> Self {
        Self::with_token_and_poll_config(base_url, token, PollConfig::default())
    }

    pub fn with_token_and_poll_config(
        base_url: impl AsRef<str>,
        token: Option<String>,
        poll_config: PollConfig,
    ) -> Self {
        Self::with_config(base_url, token, poll_config, VERSION_CACHE_TTL)
    }

    fn with_config(
        base_url: impl AsRef<str>,
        token: Option<String>,
        poll_config: PollConfig,
        version_cache_ttl: Duration,
    ) -> Self {
        Self {
            base_url: base_url.as_ref().trim_end_matches('/').to_string(),
            token,
            client: reqwest::blocking::Client::new(),
            poll_config,
            version_cache_ttl,
        }
    }

    pub fn fetch_package_versions_with_retry(
        &self,
        package: &str,
    ) -> Result<PackageVersionsResponse, String> {
        if let Some(response) = read_json_cache_fresh(
            &self.package_versions_cache_path(package),
            self.version_cache_ttl,
        ) {
            return Ok(response);
        }

        if self.missing_package_cache_path(package).exists() {
            return Err(missing_package_error(package));
        }

        for (attempt, delay) in self.poll_config.delays.iter().enumerate() {
            match self.fetch_package_versions_once(package) {
                Err(error) if is_not_found_error(&error) => {
                    write_missing_package_cache(&self.missing_package_cache_path(package));
                    return Err(missing_package_error(package));
                }
                Err(error) => return Err(error),
                Ok(PackageVersionsEnvelope::Complete(response)) => {
                    let _ = fs::remove_file(self.missing_package_cache_path(package));
                    write_json_cache(&self.package_versions_cache_path(package), &response);
                    return Ok(response);
                }
                Ok(PackageVersionsEnvelope::Ingesting(_)) => {
                    if attempt == self.poll_config.delays.len() - 1 {
                        break;
                    }
                    thread::sleep(*delay);
                }
            }
        }

        Err("registry is still hydrating dependencies; wait a bit and retry".to_string())
    }

    pub fn fetch_description_with_retry(
        &self,
        package: &str,
        version: &str,
    ) -> Result<String, String> {
        if let Some(description) = read_text_cache(&self.description_cache_path(package, version)) {
            return Ok(description);
        }

        for (attempt, delay) in self.poll_config.delays.iter().enumerate() {
            match self.fetch_description_once(package, version)? {
                DescriptionEnvelope::Complete(description) => {
                    write_text_cache(&self.description_cache_path(package, version), &description);
                    return Ok(description);
                }
                DescriptionEnvelope::Ingesting(_) => {
                    if attempt == self.poll_config.delays.len() - 1 {
                        break;
                    }
                    thread::sleep(*delay);
                }
            }
        }

        Err("registry is still hydrating dependencies; wait a bit and retry".to_string())
    }

    fn fetch_package_versions_once(
        &self,
        package: &str,
    ) -> Result<PackageVersionsEnvelope, String> {
        let response = self
            .request(
                reqwest::Method::GET,
                format!("{}/packages/{package}/versions", self.base_url),
            )
            .send()
            .map_err(|error| format!("failed to contact registry: {error}"))?;

        decode_json_response(response, "failed to decode package versions response")
    }

    fn fetch_description_once(
        &self,
        package: &str,
        version: &str,
    ) -> Result<DescriptionEnvelope, String> {
        let response = self
            .request(
                reqwest::Method::GET,
                format!(
                    "{}/packages/{package}/versions/{version}/description",
                    self.base_url
                ),
            )
            .send()
            .map_err(|error| format!("failed to contact registry: {error}"))?;

        decode_description_response(response)
    }

    fn registry_metadata_root(&self) -> PathBuf {
        cache_dir_path()
            .join("registry")
            .join(hash_string(&self.base_url))
    }

    fn package_versions_cache_path(&self, package: &str) -> PathBuf {
        self.registry_metadata_root()
            .join("versions")
            .join(format!("{package}.json"))
    }

    fn missing_package_cache_path(&self, package: &str) -> PathBuf {
        self.registry_metadata_root()
            .join("missing-packages")
            .join(format!("{package}.marker"))
    }

    fn description_cache_path(&self, package: &str, version: &str) -> PathBuf {
        self.registry_metadata_root()
            .join("descriptions")
            .join(package)
            .join(format!("{version}.dcf"))
    }

    fn request(
        &self,
        method: reqwest::Method,
        url: impl AsRef<str>,
    ) -> reqwest::blocking::RequestBuilder {
        let builder = self.client.request(method, url.as_ref());

        match &self.token {
            Some(token) => builder.bearer_auth(token),
            None => builder,
        }
    }
}

fn decode_json_response<T: serde::de::DeserializeOwned>(
    response: reqwest::blocking::Response,
    decode_error: &str,
) -> Result<T, String> {
    let status = response.status();

    if status.is_success() {
        return response
            .json::<T>()
            .map_err(|error| format!("{decode_error}: {error}"));
    }

    if status.is_server_error() {
        let body = response.text().unwrap_or_default();
        let body = body.trim();

        if body.is_empty() {
            return Err(format!("registry error ({status})"));
        }

        return Err(format!("registry error ({status}): {body}"));
    }

    Err(unexpected_response_message(response))
}

fn decode_description_response(
    response: reqwest::blocking::Response,
) -> Result<DescriptionEnvelope, String> {
    let status = response.status();

    if status == StatusCode::ACCEPTED {
        return response
            .json::<IngestingResponse>()
            .map(DescriptionEnvelope::Ingesting)
            .map_err(|error| format!("failed to decode DESCRIPTION response: {error}"));
    }

    if status.is_success() {
        return response
            .text()
            .map(DescriptionEnvelope::Complete)
            .map_err(|error| format!("failed to decode DESCRIPTION response: {error}"));
    }

    if status.is_server_error() {
        let body = response.text().unwrap_or_default();
        let body = body.trim();

        if body.is_empty() {
            return Err(format!("registry error ({status})"));
        }

        return Err(format!("registry error ({status}): {body}"));
    }

    Err(unexpected_response_message(response))
}

fn unexpected_response_message(response: reqwest::blocking::Response) -> String {
    let status = response.status();
    let body = response.text().unwrap_or_default();
    let body = body.trim();

    if body.is_empty() {
        return format!("unexpected registry response ({status})");
    }

    format!("unexpected registry response ({status}): {body}")
}

fn read_json_cache_fresh<T: serde::de::DeserializeOwned>(path: &Path, ttl: Duration) -> Option<T> {
    if !is_fresh(path, ttl) {
        return None;
    }

    let contents = fs::read(path).ok()?;
    serde_json::from_slice(&contents).ok()
}

fn is_fresh(path: &Path, ttl: Duration) -> bool {
    let Ok(metadata) = fs::metadata(path) else {
        return false;
    };
    let Ok(modified) = metadata.modified() else {
        return false;
    };
    let Ok(age) = modified.elapsed() else {
        return false;
    };

    age <= ttl
}

fn write_json_cache<T: Serialize>(path: &Path, value: &T) {
    ensure_parent_dir(path);
    let Ok(contents) = serde_json::to_vec(value) else {
        return;
    };
    let _ = fs::write(path, contents);
}

fn read_text_cache(path: &Path) -> Option<String> {
    fs::read_to_string(path).ok()
}

fn write_text_cache(path: &Path, value: &str) {
    ensure_parent_dir(path);
    let _ = fs::write(path, value);
}

fn write_missing_package_cache(path: &Path) {
    ensure_parent_dir(path);
    let _ = fs::write(path, b"missing");
}

fn ensure_parent_dir(path: &Path) {
    if let Some(parent) = path.parent() {
        let _ = fs::create_dir_all(parent);
    }
}

fn hash_string(value: &str) -> String {
    let mut hasher = DefaultHasher::new();
    value.hash(&mut hasher);
    format!("{:016x}", hasher.finish())
}

pub fn is_not_found_error(error: &str) -> bool {
    error.starts_with("unexpected registry response (404")
}

pub fn is_unauthorized_error(error: &str) -> bool {
    error.starts_with("unexpected registry response (401")
}

fn missing_package_error(package: &str) -> String {
    format!("unexpected registry response (404 Not Found): package {package} not found")
}

#[cfg(test)]
mod tests {
    use super::*;
    use mockito::Server;

    fn sample_ingesting_body() -> &'static str {
        r#"{
  "status": "ingesting"
}"#
    }

    fn sample_package_versions_body() -> &'static str {
        r#"{
  "package": "dplyr",
  "versions": [
    {
      "version": "1.1.4",
      "sourceUrl": "https://api.rrepo.org/packages/dplyr/versions/1.1.4/source"
    },
    {
      "version": "1.1.3",
      "sourceUrl": "https://api.rrepo.org/packages/dplyr/versions/1.1.3/source"
    }
  ]
}"#
    }

    fn sample_description_body() -> &'static str {
        "Package: dplyr\nVersion: 1.1.4\nTitle: Test Package\nDescription: Test package for unit tests.\nLicense: MIT\nImports: rlang\n"
    }

    fn clear_registry_metadata_cache(base_url: &str) {
        let path = cache_dir_path()
            .join("registry")
            .join(hash_string(base_url));
        if path.exists() {
            fs::remove_dir_all(path).expect("metadata cache should be removable");
        }
    }

    #[test]
    fn polls_until_package_versions_are_ready() {
        let mut server = Server::new();
        let _first = server
            .mock("GET", "/packages/dplyr/versions")
            .with_status(202)
            .with_header("content-type", "application/json")
            .with_body(sample_ingesting_body())
            .expect(1)
            .create();
        let second = server
            .mock("GET", "/packages/dplyr/versions")
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(sample_package_versions_body())
            .expect(1)
            .create();

        let client = RegistryClient::with_config(
            server.url(),
            None,
            PollConfig {
                delays: vec![Duration::ZERO, Duration::ZERO, Duration::ZERO],
            },
            VERSION_CACHE_TTL,
        );
        let response = client
            .fetch_package_versions_with_retry("dplyr")
            .expect("package versions fetch should succeed");

        second.assert();
        assert_eq!(response.versions.len(), 2);
    }

    #[test]
    fn caches_package_versions_on_disk() {
        let mut server = Server::new();
        clear_registry_metadata_cache(&server.url());
        let mock = server
            .mock("GET", "/packages/dplyr/versions")
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(sample_package_versions_body())
            .expect(1)
            .create();

        let client = RegistryClient::new(server.url());
        let first = client
            .fetch_package_versions_with_retry("dplyr")
            .expect("initial package versions fetch should succeed");
        let second = client
            .fetch_package_versions_with_retry("dplyr")
            .expect("cached package versions fetch should succeed");

        mock.assert();
        assert_eq!(first, second);
        clear_registry_metadata_cache(&client.base_url);
    }

    #[test]
    fn refreshes_stale_package_versions_cache() {
        let mut server = Server::new();
        clear_registry_metadata_cache(&server.url());
        let mock = server
            .mock("GET", "/packages/dplyr/versions")
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(sample_package_versions_body())
            .expect(2)
            .create();

        let client =
            RegistryClient::with_config(server.url(), None, PollConfig::default(), Duration::ZERO);
        let first = client
            .fetch_package_versions_with_retry("dplyr")
            .expect("initial package versions fetch should succeed");
        let second = client
            .fetch_package_versions_with_retry("dplyr")
            .expect("stale package versions fetch should refresh");

        mock.assert();
        assert_eq!(first, second);
        clear_registry_metadata_cache(&client.base_url);
    }

    #[test]
    fn caches_missing_package_lookups_on_disk() {
        let mut server = Server::new();
        clear_registry_metadata_cache(&server.url());
        let mock = server
            .mock("GET", "/packages/missingpkg/versions")
            .with_status(404)
            .expect(1)
            .create();

        let client = RegistryClient::new(server.url());
        let first = client
            .fetch_package_versions_with_retry("missingpkg")
            .expect_err("initial missing package fetch should fail");
        assert!(
            client.missing_package_cache_path("missingpkg").exists(),
            "missing package cache was not written for error: {first}"
        );
        let second = client
            .fetch_package_versions_with_retry("missingpkg")
            .expect_err("cached missing package fetch should fail");

        mock.assert();
        assert_eq!(first, second);
        assert!(first.contains("package missingpkg not found"));
        clear_registry_metadata_cache(&client.base_url);
    }

    #[test]
    fn fetches_remote_description_text() {
        let mut server = Server::new();
        let mock = server
            .mock("GET", "/packages/dplyr/versions/1.1.4/description")
            .with_status(200)
            .with_header("content-type", "text/plain")
            .with_body(sample_description_body())
            .create();

        let client = RegistryClient::new(server.url());
        let response = client
            .fetch_description_with_retry("dplyr", "1.1.4")
            .expect("description fetch should succeed");

        mock.assert();
        assert!(response.contains("Package: dplyr"));
        assert!(response.contains("Imports: rlang"));
    }

    #[test]
    fn caches_description_text_on_disk() {
        let mut server = Server::new();
        clear_registry_metadata_cache(&server.url());
        let mock = server
            .mock("GET", "/packages/dplyr/versions/1.1.4/description")
            .with_status(200)
            .with_header("content-type", "text/plain")
            .with_body(sample_description_body())
            .expect(1)
            .create();

        let client = RegistryClient::new(server.url());
        let first = client
            .fetch_description_with_retry("dplyr", "1.1.4")
            .expect("initial description fetch should succeed");
        let second = client
            .fetch_description_with_retry("dplyr", "1.1.4")
            .expect("cached description fetch should succeed");

        mock.assert();
        assert_eq!(first, second);
        clear_registry_metadata_cache(&client.base_url);
    }

    #[test]
    fn polls_until_description_is_ready() {
        let mut server = Server::new();
        let _first = server
            .mock("GET", "/packages/dplyr/versions/1.1.4/description")
            .with_status(202)
            .with_header("content-type", "application/json")
            .with_body(sample_ingesting_body())
            .expect(1)
            .create();
        let second = server
            .mock("GET", "/packages/dplyr/versions/1.1.4/description")
            .with_status(200)
            .with_header("content-type", "text/plain")
            .with_body(sample_description_body())
            .expect(1)
            .create();

        let client = RegistryClient::with_config(
            server.url(),
            None,
            PollConfig {
                delays: vec![Duration::ZERO, Duration::ZERO, Duration::ZERO],
            },
            VERSION_CACHE_TTL,
        );
        let response = client
            .fetch_description_with_retry("dplyr", "1.1.4")
            .expect("description fetch should succeed");

        second.assert();
        assert!(response.contains("Version: 1.1.4"));
    }
}
