use crate::project::{artifact_cache_path, cache_dir_path};
use reqwest::StatusCode;
use serde::{Deserialize, Serialize};
use std::{
    collections::hash_map::DefaultHasher,
    fs,
    hash::{Hash, Hasher},
    io::{Read, Write},
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ArtifactKind {
    Source,
    Binary,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ArtifactRequest {
    pub kind: ArtifactKind,
    pub url: String,
    pub cache_file_name: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DownloadProgress {
    ContentLength(u64),
    Advanced(u64),
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

    pub fn fetch_repository_packages(&self) -> Result<RepositoryPackagesResponse, String> {
        let response = self
            .request(reqwest::Method::GET, format!("{}/packages", self.base_url))
            .send()
            .map_err(|error| format!("failed to contact registry: {error}"))?;

        decode_json_response(response, "failed to decode repository packages response")
    }

    #[cfg(test)]
    fn fetch_package_versions(&self, package: &str) -> Result<PackageVersionsResponse, String> {
        if let Some(response) = read_json_cache_fresh(
            &self.package_versions_cache_path(package),
            self.version_cache_ttl,
        ) {
            return Ok(response);
        }

        if self.missing_package_cache_path(package).exists() {
            return Err(missing_package_error(package));
        }

        match self.fetch_package_versions_once(package) {
            Err(error) if is_not_found_error(&error) => {
                write_missing_package_cache(&self.missing_package_cache_path(package));
                Err(missing_package_error(package))
            }
            Err(error) => Err(error),
            Ok(PackageVersionsEnvelope::Complete(response)) => {
                let _ = fs::remove_file(self.missing_package_cache_path(package));
                write_json_cache(&self.package_versions_cache_path(package), &response);
                Ok(response)
            }
            Ok(PackageVersionsEnvelope::Ingesting(_)) => {
                Err("registry is still hydrating dependencies; wait a bit and retry".to_string())
            }
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

    pub fn download_artifact_with_progress(
        &self,
        package: &str,
        version: &str,
        artifact: &ArtifactRequest,
        mut on_progress: impl FnMut(DownloadProgress),
    ) -> Result<DownloadedArtifact, String> {
        let path = artifact_cache_path(package, version, &artifact.cache_file_name);
        if path.exists() {
            return Ok(DownloadedArtifact { path });
        }

        let artifact_label = match artifact.kind {
            ArtifactKind::Source => "source artifact",
            ArtifactKind::Binary => "binary artifact",
        };
        let response = self
            .request(reqwest::Method::GET, &artifact.url)
            .send()
            .map_err(|error| format!("failed to download {artifact_label}: {error}"))?;

        let status = response.status();
        if !status.is_success() {
            let body = response.text().unwrap_or_default();
            let body = body.trim();

            if body.is_empty() {
                return Err(format!("artifact download failed ({status})"));
            }

            return Err(format!("artifact download failed ({status}): {body}"));
        }

        if let Some(length) = response.content_length() {
            on_progress(DownloadProgress::ContentLength(length));
        }

        let mut file = fs::File::create(&path)
            .map_err(|error| format!("failed to write {artifact_label}: {error}"))?;
        let mut response = response;
        let mut buffer = [0_u8; 16 * 1024];

        loop {
            let read = response
                .read(&mut buffer)
                .map_err(|error| format!("failed to read {artifact_label}: {error}"))?;
            if read == 0 {
                break;
            }

            file.write_all(&buffer[..read])
                .map_err(|error| format!("failed to write {artifact_label}: {error}"))?;
            on_progress(DownloadProgress::Advanced(read as u64));
        }

        Ok(DownloadedArtifact { path })
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

#[derive(Debug)]
pub struct DownloadedArtifact {
    path: PathBuf,
}

impl DownloadedArtifact {
    pub fn path(&self) -> &Path {
        &self.path
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::project::artifact_cache_path;
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
    fn fetches_all_package_versions() {
        let mut server = Server::new();
        let mock = server
            .mock("GET", "/packages/dplyr/versions")
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(sample_package_versions_body())
            .create();

        let client = RegistryClient::new(server.url());
        let response = client
            .fetch_package_versions("dplyr")
            .expect("package versions fetch should succeed");

        mock.assert();
        assert_eq!(response.package, "dplyr");
        assert_eq!(response.versions.len(), 2);
        assert_eq!(response.versions[1].version, "1.1.3");
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

    #[test]
    fn downloads_source_artifact_to_a_local_file() {
        clear_cached_artifact("digest", "0.6.37", "digest_0.6.37_download.tar.gz");

        let mut server = Server::new();
        let mock = server
            .mock("GET", "/packages/digest/versions/0.6.37/source")
            .with_status(200)
            .with_header("content-type", "application/gzip")
            .with_body("fake-tarball")
            .create();

        let client = RegistryClient::new(server.url());
        let artifact = client
            .download_artifact_with_progress(
                "digest",
                "0.6.37",
                &ArtifactRequest {
                    kind: ArtifactKind::Source,
                    url: format!("{}/packages/digest/versions/0.6.37/source", server.url()),
                    cache_file_name: "digest_0.6.37_download.tar.gz".to_string(),
                },
                |_| {},
            )
            .expect("download should succeed");

        mock.assert();
        let contents = fs::read(artifact.path()).expect("artifact should exist");
        assert_eq!(contents, b"fake-tarball");
        clear_cached_artifact("digest", "0.6.37", "digest_0.6.37_download.tar.gz");
    }

    #[test]
    fn surfaces_source_artifact_download_errors() {
        clear_cached_artifact("digest", "0.6.37", "digest_0.6.37_error.tar.gz");

        let mut server = Server::new();
        let mock = server
            .mock("GET", "/packages/digest/versions/0.6.37/source")
            .with_status(500)
            .with_body("tarball missing")
            .create();

        let client = RegistryClient::new(server.url());
        let error = client
            .download_artifact_with_progress(
                "digest",
                "0.6.37",
                &ArtifactRequest {
                    kind: ArtifactKind::Source,
                    url: format!("{}/packages/digest/versions/0.6.37/source", server.url()),
                    cache_file_name: "digest_0.6.37_error.tar.gz".to_string(),
                },
                |_| {},
            )
            .expect_err("download should fail");

        mock.assert();
        assert!(
            error.contains("artifact download failed (500 Internal Server Error): tarball missing")
        );
    }

    #[test]
    fn downloads_binary_artifact_to_a_local_file() {
        clear_cached_artifact("digest", "0.6.37", "digest_0.6.37.zip");

        let mut server = Server::new();
        let mock = server
            .mock(
                "GET",
                "/packages/digest/versions/0.6.37/binaries/windows/4.5",
            )
            .with_status(200)
            .with_header("content-type", "application/zip")
            .with_body("fake-zip")
            .create();

        let client = RegistryClient::new(server.url());
        let artifact = client
            .download_artifact_with_progress(
                "digest",
                "0.6.37",
                &ArtifactRequest {
                    kind: ArtifactKind::Binary,
                    url: format!(
                        "{}/packages/digest/versions/0.6.37/binaries/windows/4.5",
                        server.url()
                    ),
                    cache_file_name: "digest_0.6.37.zip".to_string(),
                },
                |_| {},
            )
            .expect("download should succeed");

        mock.assert();
        let contents = fs::read(artifact.path()).expect("artifact should exist");
        assert_eq!(contents, b"fake-zip");
        clear_cached_artifact("digest", "0.6.37", "digest_0.6.37.zip");
    }

    fn clear_cached_artifact(package: &str, version: &str, file_name: &str) {
        let path = artifact_cache_path(package, version, file_name);
        let _ = fs::remove_file(&path);
    }

    #[test]
    fn sends_bearer_auth_when_token_is_configured() {
        let mut server = Server::new();
        let mock = server
            .mock("GET", "/packages")
            .match_header("authorization", "Bearer secret-token")
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(
                r#"{
  "repositorySlug": "test",
  "packages": []
}"#,
            )
            .create();

        let client = RegistryClient::with_token(server.url(), Some("secret-token".to_string()));
        let response = client
            .fetch_repository_packages()
            .expect("repository listing should succeed");

        mock.assert();
        assert_eq!(response.repository_slug, "test");
    }
}
