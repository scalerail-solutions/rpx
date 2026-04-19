use reqwest::StatusCode;
use serde::{Deserialize, Serialize};
use std::{
    fs,
    io::{Read, Write},
    path::{Path, PathBuf},
    thread,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

pub const DEFAULT_REGISTRY_BASE_URL: &str = "https://api.rrepo.org";

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ClosureRequest {
    pub roots: Vec<ClosureRoot>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub struct ClosureRoot {
    pub name: String,
    pub constraint: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "status", rename_all = "lowercase")]
pub enum ClosureResponse {
    Complete(CompleteClosureResponse),
    Ingesting(IngestingResponse),
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CompleteClosureResponse {
    pub roots: Vec<ClosureRoot>,
    #[serde(rename = "includeDependencyKinds")]
    pub include_dependency_kinds: Vec<String>,
    pub packages: Vec<ClosurePackage>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct IngestingResponse {}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ClosurePackage {
    pub name: String,
    pub versions: Vec<ClosureVersion>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ClosureVersion {
    pub version: String,
    #[serde(rename = "sourceUrl")]
    pub source_url: String,
    pub dependencies: Vec<ClosureDependency>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ClosureDependency {
    #[serde(rename = "dependencyName")]
    pub dependency_name: String,
    #[serde(rename = "dependencyKind")]
    pub dependency_kind: String,
    #[serde(rename = "minVersion")]
    pub min_version: Option<RegistryVersion>,
    #[serde(rename = "maxVersionExclusive")]
    pub max_version_exclusive: Option<RegistryVersion>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RegistryVersion {
    pub major: u64,
    pub minor: u64,
    pub patch: u64,
    pub build: u64,
}

impl std::fmt::Display for RegistryVersion {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.build != 0 {
            return write!(f, "{}.{}.{}.{}", self.major, self.minor, self.patch, self.build);
        }

        if self.patch != 0 {
            return write!(f, "{}.{}.{}", self.major, self.minor, self.patch);
        }

        write!(f, "{}.{}", self.major, self.minor)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct LatestVersionResponse {
    pub package: String,
    pub version: String,
    #[serde(rename = "sourceUrl")]
    pub source_url: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PackageVersionsResponse {
    pub package: String,
    pub versions: Vec<VersionSummary>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct VersionSummary {
    pub version: String,
    #[serde(rename = "sourceUrl")]
    pub source_url: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(untagged)]
enum LatestVersionEnvelope {
    Complete(LatestVersionResponse),
    Ingesting(ClosureResponse),
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(untagged)]
enum PackageVersionsEnvelope {
    Complete(PackageVersionsResponse),
    Ingesting(ClosureResponse),
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

impl PollConfig {
    #[cfg(test)]
    pub fn from_delays(delays: Vec<Duration>) -> Self {
        Self { delays }
    }
}

#[derive(Debug)]
pub struct RegistryClient {
    base_url: String,
    client: reqwest::blocking::Client,
    poll_config: PollConfig,
}

impl Default for RegistryClient {
    fn default() -> Self {
        Self::new(DEFAULT_REGISTRY_BASE_URL)
    }
}

impl RegistryClient {
    pub fn new(base_url: impl AsRef<str>) -> Self {
        Self::with_poll_config(base_url, PollConfig::default())
    }

    pub fn with_poll_config(base_url: impl AsRef<str>, poll_config: PollConfig) -> Self {
        Self {
            base_url: base_url.as_ref().trim_end_matches('/').to_string(),
            client: reqwest::blocking::Client::new(),
            poll_config,
        }
    }

    pub fn base_url(&self) -> &str {
        &self.base_url
    }

    pub fn fetch_closure_with_retry(
        &self,
        request: &ClosureRequest,
    ) -> Result<CompleteClosureResponse, String> {
        for (attempt, delay) in self.poll_config.delays.iter().enumerate() {
            match self.fetch_closure_once(request)? {
                ClosureResponse::Complete(response) => return Ok(response),
                ClosureResponse::Ingesting(_) => {
                    if attempt == self.poll_config.delays.len() - 1 {
                        break;
                    }
                    thread::sleep(*delay);
                }
            }
        }

        Err("registry is still hydrating dependencies; wait a bit and retry".to_string())
    }

    fn fetch_closure_once(&self, request: &ClosureRequest) -> Result<ClosureResponse, String> {
        let response = self
            .client
            .post(format!("{}/closure", self.base_url))
            .json(request)
            .send()
            .map_err(|error| format!("failed to contact registry: {error}"))?;

        let status = response.status();

        if status == StatusCode::OK || status == StatusCode::ACCEPTED {
            return response
                .json::<ClosureResponse>()
                .map_err(|error| format!("failed to decode registry response: {error}"));
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

    #[allow(dead_code)]
    pub fn fetch_latest_version(&self, package: &str) -> Result<LatestVersionResponse, String> {
        match self.fetch_latest_version_once(package)? {
            LatestVersionEnvelope::Complete(response) => Ok(response),
            LatestVersionEnvelope::Ingesting(_) => {
                Err("registry is still hydrating dependencies; wait a bit and retry".to_string())
            }
        }
    }

    pub fn fetch_latest_version_with_retry(
        &self,
        package: &str,
    ) -> Result<LatestVersionResponse, String> {
        for (attempt, delay) in self.poll_config.delays.iter().enumerate() {
            match self.fetch_latest_version_once(package)? {
                LatestVersionEnvelope::Complete(response) => return Ok(response),
                LatestVersionEnvelope::Ingesting(_) => {
                    if attempt == self.poll_config.delays.len() - 1 {
                        break;
                    }
                    thread::sleep(*delay);
                }
            }
        }

        Err("registry is still hydrating dependencies; wait a bit and retry".to_string())
    }

    fn fetch_latest_version_once(&self, package: &str) -> Result<LatestVersionEnvelope, String> {
        let response = self
            .client
            .get(format!("{}/packages/{package}/versions/latest", self.base_url))
            .send()
            .map_err(|error| format!("failed to contact registry: {error}"))?;

        decode_json_response(response, "failed to decode latest version response")
    }

    #[allow(dead_code)]
    pub fn fetch_package_versions(&self, package: &str) -> Result<PackageVersionsResponse, String> {
        match self.fetch_package_versions_once(package)? {
            PackageVersionsEnvelope::Complete(response) => Ok(response),
            PackageVersionsEnvelope::Ingesting(_) => {
                Err("registry is still hydrating dependencies; wait a bit and retry".to_string())
            }
        }
    }

    #[allow(dead_code)]
    pub fn fetch_package_versions_with_retry(
        &self,
        package: &str,
    ) -> Result<PackageVersionsResponse, String> {
        for (attempt, delay) in self.poll_config.delays.iter().enumerate() {
            match self.fetch_package_versions_once(package)? {
                PackageVersionsEnvelope::Complete(response) => return Ok(response),
                PackageVersionsEnvelope::Ingesting(_) => {
                    if attempt == self.poll_config.delays.len() - 1 {
                        break;
                    }
                    thread::sleep(*delay);
                }
            }
        }

        Err("registry is still hydrating dependencies; wait a bit and retry".to_string())
    }

    fn fetch_package_versions_once(&self, package: &str) -> Result<PackageVersionsEnvelope, String> {
        let response = self
            .client
            .get(format!("{}/packages/{package}/versions", self.base_url))
            .send()
            .map_err(|error| format!("failed to contact registry: {error}"))?;

        decode_json_response(response, "failed to decode package versions response")
    }

    #[allow(dead_code)]
    pub fn download_source_artifact(
        &self,
        package: &str,
        version: &str,
        source_url: &str,
    ) -> Result<DownloadedArtifact, String> {
        self.download_source_artifact_with_progress(package, version, source_url, |_| {})
    }

    pub fn download_source_artifact_with_progress<F>(
        &self,
        package: &str,
        version: &str,
        source_url: &str,
        mut on_progress: F,
    ) -> Result<DownloadedArtifact, String>
    where
        F: FnMut(DownloadProgress),
    {
        let response = self
            .client
            .get(source_url)
            .send()
            .map_err(|error| format!("failed to download source artifact: {error}"))?;

        let status = response.status();
        if !status.is_success() {
            let body = response.text().unwrap_or_default();
            let body = body.trim();

            if body.is_empty() {
                return Err(format!("artifact download failed ({status})"));
            }

            return Err(format!("artifact download failed ({status}): {body}"));
        }

        let total_bytes = response.content_length();
        let directory = artifact_directory();
        fs::create_dir_all(&directory)
            .map_err(|error| format!("failed to create artifact directory: {error}"))?;

        let path = directory.join(format!("{package}_{version}.tar.gz"));
        let mut file = fs::File::create(&path)
            .map_err(|error| format!("failed to write source artifact: {error}"))?;
        let mut response = response;
        let mut downloaded_bytes = 0_u64;
        let mut buffer = [0_u8; 16 * 1024];

        on_progress(DownloadProgress {
            downloaded_bytes,
            total_bytes,
        });

        loop {
            let read = response
                .read(&mut buffer)
                .map_err(|error| format!("failed to read source artifact: {error}"))?;
            if read == 0 {
                break;
            }

            file.write_all(&buffer[..read])
                .map_err(|error| format!("failed to write source artifact: {error}"))?;
            downloaded_bytes += read as u64;
            on_progress(DownloadProgress {
                downloaded_bytes,
                total_bytes,
            });
        }

        Ok(DownloadedArtifact { path })
    }
}

#[derive(Debug, Clone, Copy)]
pub struct DownloadProgress {
    pub downloaded_bytes: u64,
    pub total_bytes: Option<u64>,
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

fn unexpected_response_message(response: reqwest::blocking::Response) -> String {
    let status = response.status();
    let body = response.text().unwrap_or_default();
    let body = body.trim();

    if body.is_empty() {
        return format!("unexpected registry response ({status})");
    }

    format!("unexpected registry response ({status}): {body}")
}

#[derive(Debug)]
pub struct DownloadedArtifact {
    path: PathBuf,
}

impl DownloadedArtifact {
    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn cleanup(self) {
        let _ = fs::remove_file(&self.path);

        if let Some(parent) = self.path.parent() {
            let _ = fs::remove_dir(parent);
        }
    }
}

fn artifact_directory() -> PathBuf {
    let unique = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time should be after unix epoch")
        .as_nanos();
    std::env::temp_dir().join(format!("rpx-artifacts-{}-{unique}", std::process::id()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use mockito::{Matcher, Server};

    fn sample_request() -> ClosureRequest {
        ClosureRequest {
            roots: vec![ClosureRoot {
                name: "dplyr".to_string(),
                constraint: "*".to_string(),
            }],
        }
    }

    fn sample_complete_body() -> &'static str {
        r#"{
  "status": "complete",
  "roots": [
    { "name": "dplyr", "constraint": "*" }
  ],
  "includeDependencyKinds": ["Depends", "Imports", "LinkingTo"],
  "packages": [
    {
      "name": "dplyr",
      "versions": [
        {
          "version": "1.1.4",
          "sourceUrl": "https://api.rrepo.org/packages/dplyr/versions/1.1.4/source",
          "dependencies": [
            {
              "dependencyName": "rlang",
              "dependencyKind": "Imports",
              "minVersion": { "major": 1, "minor": 1, "patch": 0, "build": 0 },
              "maxVersionExclusive": null
            }
          ]
        }
      ]
    }
  ]
}"#
    }

    fn sample_ingesting_body() -> &'static str {
        r#"{
  "status": "ingesting"
}"#
    }

    fn sample_latest_version_body() -> &'static str {
        r#"{
  "package": "dplyr",
  "version": "1.1.4",
  "sourceUrl": "https://api.rrepo.org/packages/dplyr/versions/1.1.4/source"
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

    #[test]
    fn deserializes_complete_closure_response() {
        let response = serde_json::from_str::<ClosureResponse>(sample_complete_body())
            .expect("complete response should deserialize");

        let ClosureResponse::Complete(response) = response else {
            panic!("expected complete response");
        };

        assert_eq!(response.roots[0].name, "dplyr");
        assert_eq!(
            response.include_dependency_kinds,
            ["Depends", "Imports", "LinkingTo"]
        );
        assert_eq!(
            response.packages[0].versions[0].dependencies[0].dependency_name,
            "rlang"
        );
    }

    #[test]
    fn fetches_latest_package_version() {
        let mut server = Server::new();
        let mock = server
            .mock("GET", "/packages/dplyr/versions/latest")
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(sample_latest_version_body())
            .create();

        let client = RegistryClient::new(server.url());
        let response = client
            .fetch_latest_version("dplyr")
            .expect("latest version fetch should succeed");

        mock.assert();
        assert_eq!(response.version, "1.1.4");
        assert_eq!(
            response.source_url,
            "https://api.rrepo.org/packages/dplyr/versions/1.1.4/source"
        );
    }

    #[test]
    fn polls_until_latest_version_is_ready() {
        let mut server = Server::new();
        let _first = server
            .mock("GET", "/packages/dplyr/versions/latest")
            .with_status(202)
            .with_header("content-type", "application/json")
            .with_body(sample_ingesting_body())
            .expect(1)
            .create();
        let second = server
            .mock("GET", "/packages/dplyr/versions/latest")
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(sample_latest_version_body())
            .expect(1)
            .create();

        let client = RegistryClient::with_poll_config(
            server.url(),
            PollConfig::from_delays(vec![Duration::ZERO, Duration::ZERO, Duration::ZERO]),
        );
        let response = client
            .fetch_latest_version_with_retry("dplyr")
            .expect("latest version fetch should succeed");

        second.assert();
        assert_eq!(response.version, "1.1.4");
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

        let client = RegistryClient::with_poll_config(
            server.url(),
            PollConfig::from_delays(vec![Duration::ZERO, Duration::ZERO, Duration::ZERO]),
        );
        let response = client
            .fetch_package_versions_with_retry("dplyr")
            .expect("package versions fetch should succeed");

        second.assert();
        assert_eq!(response.versions.len(), 2);
    }

    #[test]
    fn fetches_complete_closure_without_retrying() {
        let mut server = Server::new();
        let mock = server
            .mock("POST", "/closure")
            .match_header(
                "content-type",
                Matcher::Regex("application/json.*".to_string()),
            )
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(sample_complete_body())
            .create();

        let client = RegistryClient::with_poll_config(
            server.url(),
            PollConfig::from_delays(vec![Duration::ZERO]),
        );
        let response = client
            .fetch_closure_with_retry(&sample_request())
            .expect("closure fetch should succeed");

        mock.assert();
        assert_eq!(response.packages[0].name, "dplyr");
    }

    #[test]
    fn polls_until_closure_is_complete() {
        let mut server = Server::new();
        let _first = server
            .mock("POST", "/closure")
            .match_header(
                "content-type",
                Matcher::Regex("application/json.*".to_string()),
            )
            .with_status(202)
            .with_header("content-type", "application/json")
            .with_body(sample_ingesting_body())
            .expect(1)
            .create();
        let second = server
            .mock("POST", "/closure")
            .match_header(
                "content-type",
                Matcher::Regex("application/json.*".to_string()),
            )
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(sample_complete_body())
            .expect(1)
            .create();

        let client = RegistryClient::with_poll_config(
            server.url(),
            PollConfig::from_delays(vec![Duration::ZERO, Duration::ZERO, Duration::ZERO]),
        );
        let response = client
            .fetch_closure_with_retry(&sample_request())
            .expect("closure fetch should succeed");

        second.assert();
        assert_eq!(response.packages[0].versions[0].version, "1.1.4");
    }

    #[test]
    fn returns_friendly_error_when_registry_keeps_ingesting() {
        let mut server = Server::new();
        let mock = server
            .mock("POST", "/closure")
            .match_header(
                "content-type",
                Matcher::Regex("application/json.*".to_string()),
            )
            .with_status(202)
            .with_header("content-type", "application/json")
            .with_body(sample_ingesting_body())
            .expect(3)
            .create();

        let client = RegistryClient::with_poll_config(
            server.url(),
            PollConfig::from_delays(vec![Duration::ZERO, Duration::ZERO, Duration::ZERO]),
        );
        let error = client
            .fetch_closure_with_retry(&sample_request())
            .expect_err("closure fetch should time out");

        mock.assert();
        assert_eq!(
            error,
            "registry is still hydrating dependencies; wait a bit and retry"
        );
    }

    #[test]
    fn returns_friendly_error_when_latest_version_keeps_ingesting() {
        let mut server = Server::new();
        let mock = server
            .mock("GET", "/packages/dplyr/versions/latest")
            .with_status(202)
            .with_header("content-type", "application/json")
            .with_body(sample_ingesting_body())
            .expect(3)
            .create();

        let client = RegistryClient::with_poll_config(
            server.url(),
            PollConfig::from_delays(vec![Duration::ZERO, Duration::ZERO, Duration::ZERO]),
        );
        let error = client
            .fetch_latest_version_with_retry("dplyr")
            .expect_err("latest version fetch should time out");

        mock.assert();
        assert_eq!(
            error,
            "registry is still hydrating dependencies; wait a bit and retry"
        );
    }

    #[test]
    fn surfaces_registry_server_errors() {
        let mut server = Server::new();
        let mock = server
            .mock("POST", "/closure")
            .match_header(
                "content-type",
                Matcher::Regex("application/json.*".to_string()),
            )
            .with_status(500)
            .with_body("registry exploded")
            .create();

        let client = RegistryClient::with_poll_config(
            server.url(),
            PollConfig::from_delays(vec![Duration::ZERO]),
        );
        let error = client
            .fetch_closure_with_retry(&sample_request())
            .expect_err("closure fetch should fail");

        mock.assert();
        assert!(error.contains("registry error (500 Internal Server Error): registry exploded"));
    }

    #[test]
    fn downloads_source_artifact_to_a_local_file() {
        let mut server = Server::new();
        let mock = server
            .mock("GET", "/packages/digest/versions/0.6.37/source")
            .with_status(200)
            .with_header("content-type", "application/gzip")
            .with_body("fake-tarball")
            .create();

        let client = RegistryClient::new(server.url());
        let artifact = client
            .download_source_artifact(
                "digest",
                "0.6.37",
                &format!("{}/packages/digest/versions/0.6.37/source", server.url()),
            )
            .expect("download should succeed");

        mock.assert();
        let contents = fs::read(artifact.path()).expect("artifact should exist");
        assert_eq!(contents, b"fake-tarball");
        artifact.cleanup();
    }

    #[test]
    fn surfaces_source_artifact_download_errors() {
        let mut server = Server::new();
        let mock = server
            .mock("GET", "/packages/digest/versions/0.6.37/source")
            .with_status(500)
            .with_body("tarball missing")
            .create();

        let client = RegistryClient::new(server.url());
        let error = client
            .download_source_artifact(
                "digest",
                "0.6.37",
                &format!("{}/packages/digest/versions/0.6.37/source", server.url()),
            )
            .expect_err("download should fail");

        mock.assert();
        assert!(
            error.contains("artifact download failed (500 Internal Server Error): tarball missing")
        );
    }
}
