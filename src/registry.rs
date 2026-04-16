use reqwest::StatusCode;
use serde::{Deserialize, Serialize};
use std::{thread, time::Duration};

pub const DEFAULT_REGISTRY_BASE_URL: &str = "https://api.rrepo.org";

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ClosureRequest {
    pub roots: Vec<ClosureRoot>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ClosureRoot {
    pub name: String,
    pub constraint: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "status", rename_all = "lowercase")]
pub enum ClosureResponse {
    Complete(CompleteClosureResponse),
    Ingesting(IngestingClosureResponse),
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CompleteClosureResponse {
    pub roots: Vec<ClosureRoot>,
    #[serde(rename = "includeDependencyKinds")]
    pub include_dependency_kinds: Vec<String>,
    pub packages: Vec<ClosurePackage>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct IngestingClosureResponse {
    pub roots: Vec<ClosureRoot>,
    pub statuses: Vec<PackageIngestionStatus>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PackageIngestionStatus {
    #[serde(rename = "packageName")]
    pub package_name: String,
    #[serde(rename = "workflowId")]
    pub workflow_id: String,
    pub status: WorkflowStatus,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct WorkflowStatus {
    pub status: String,
    pub error: Option<String>,
}

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
    #[serde(rename = "sourceTarballKey")]
    pub source_tarball_key: String,
    #[serde(rename = "descriptionKey")]
    pub description_key: String,
    pub dependencies: Vec<ClosureDependency>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ClosureDependency {
    #[serde(rename = "dependencyName")]
    pub dependency_name: String,
    #[serde(rename = "dependencyKind")]
    pub dependency_kind: String,
    #[serde(rename = "constraintRaw")]
    pub constraint_raw: String,
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

        let body = response.text().unwrap_or_default();
        let body = body.trim();

        if body.is_empty() {
            return Err(format!("unexpected registry response ({status})"));
        }

        Err(format!("unexpected registry response ({status}): {body}"))
    }
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
          "sourceTarballKey": "src/dplyr_1.1.4.tar.gz",
          "descriptionKey": "desc/dplyr_1.1.4",
          "dependencies": [
            {
              "dependencyName": "rlang",
              "dependencyKind": "Imports",
              "constraintRaw": ">= 1.1.0"
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
  "status": "ingesting",
  "roots": [
    { "name": "dplyr", "constraint": "*" }
  ],
  "statuses": [
    {
      "packageName": "dplyr",
      "workflowId": "pkg-dplyr",
      "status": {
        "status": "running",
        "error": null
      }
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
}
