#![allow(dead_code)]

use flate2::read::GzDecoder;
use miette::Diagnostic;
use reqwest_middleware::ClientBuilder;
use reqwest_tracing::TracingMiddleware;
use std::io::{Cursor, Read};
use std::pin::Pin;
use std::str::FromStr;
use thiserror::Error;

pub type HttpClient = reqwest_middleware::ClientWithMiddleware;

pub fn client() -> HttpClient {
    ClientBuilder::new(reqwest::Client::new()).build()
}

pub fn traced_client() -> HttpClient {
    ClientBuilder::new(reqwest::Client::new())
        .with(TracingMiddleware::default())
        .build()
}

pub struct ArtifactResponse {
    pub content_length: Option<u64>,
    pub stream:
        Pin<Box<dyn futures_core::Stream<Item = Result<bytes::Bytes, reqwest::Error>> + Send>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CranPackagesIndex {
    pub packages: Vec<CranPackageIndexEntry>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CranPackageIndexEntry {
    pub package: String,
    pub version: String,
    pub depends: r_description::lossy::Relations,
    pub imports: r_description::lossy::Relations,
    pub suggests: r_description::lossy::Relations,
    pub linking_to: r_description::lossy::Relations,
    pub system_requirements: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CranArchiveRootListing {
    pub packages: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CranPackageArchiveListing {
    pub versions: Vec<r_description::Version>,
}

impl FromStr for CranPackagesIndex {
    type Err = String;

    fn from_str(input: &str) -> Result<Self, Self::Err> {
        let reader = Cursor::new(input);
        let packages = deb822_fast::ParagraphReader::new(reader)
            .map(|paragraph| {
                paragraph
                    .map_err(|error| error.to_string())
                    .and_then(cran_package_index_entry_from_paragraph)
            })
            .collect::<Result<Vec<_>, String>>()?;

        Ok(Self { packages })
    }
}

impl FromStr for CranArchiveRootListing {
    type Err = String;

    fn from_str(input: &str) -> Result<Self, Self::Err> {
        let mut packages = Vec::new();
        for part in archive_listing_parts(input) {
            if part.starts_with('/') || !part.ends_with('/') || part.contains('?') {
                continue;
            }

            let package = part.trim_end_matches('/');
            if package.is_empty() || packages.iter().any(|seen| seen == package) {
                continue;
            }

            packages.push(html_unescape_minimal(package));
        }

        Ok(Self { packages })
    }
}

impl FromStr for CranPackageArchiveListing {
    type Err = String;

    fn from_str(input: &str) -> Result<Self, Self::Err> {
        let mut versions = Vec::new();
        for part in archive_listing_parts(input) {
            let file_name = part.rsplit('/').next().unwrap_or(part);
            if !file_name.ends_with(".tar.gz") || !file_name.contains('_') {
                continue;
            }

            let file_name = html_unescape_minimal(file_name);
            let stem = file_name
                .strip_suffix(".tar.gz")
                .expect("archive file name was checked for tar.gz suffix");
            let Some((package, version)) = stem.rsplit_once('_') else {
                continue;
            };
            if package.is_empty() || version.is_empty() {
                continue;
            }

            let version = version.parse::<r_description::Version>()?;
            if !versions.iter().any(|seen| seen == &version) {
                versions.push(version);
            }
        }

        Ok(Self { versions })
    }
}

#[derive(Debug, Clone, serde::Deserialize, PartialEq, Eq)]
pub struct RrepoPackagesResponse {
    #[serde(rename = "repositorySlug")]
    pub repository_slug: String,
    pub packages: Vec<RrepoPackageSummary>,
}

#[derive(Debug, Clone, serde::Deserialize, PartialEq, Eq)]
pub struct RrepoPackageSummary {
    pub name: String,
    #[serde(rename = "latestVersion")]
    pub latest_version: String,
    #[serde(rename = "latestUploadedAt")]
    pub latest_uploaded_at: Option<String>,
    #[serde(rename = "versionCount")]
    pub version_count: Option<usize>,
}

#[derive(Debug, Clone, serde::Deserialize, PartialEq, Eq)]
pub struct RrepoPackageVersionsResponse {
    pub package: String,
    pub versions: Vec<RrepoVersionSummary>,
}

#[derive(Debug, Clone, serde::Deserialize, PartialEq, Eq)]
pub struct RrepoVersionSummary {
    pub version: String,
    #[serde(rename = "sourceUrl")]
    pub source_url: String,
}

#[derive(Debug, Error, Diagnostic)]
pub enum HttpError {
    #[error("failed to send request to {url}: {source}")]
    #[diagnostic(code(rpx::http::request_failed))]
    RequestFailed {
        url: reqwest::Url,
        #[source]
        source: reqwest_middleware::Error,
    },

    #[error("unexpected response from {url}: {status}")]
    #[diagnostic(code(rpx::http::unexpected_status))]
    UnexpectedStatus {
        url: reqwest::Url,
        status: reqwest::StatusCode,
        body: String,
    },

    #[error("failed to decode JSON response from {url}: {source}")]
    #[diagnostic(code(rpx::http::json_decode_failed))]
    JsonDecodeFailed {
        url: reqwest::Url,
        #[source]
        source: reqwest::Error,
    },

    #[error("failed to extract source package from {url}: {details}")]
    #[diagnostic(code(rpx::http::artifact_extract_failed))]
    ArtifactExtractFailed { url: reqwest::Url, details: String },

    #[error("failed to read response body from {url}: {source}")]
    #[diagnostic(code(rpx::http::body_read_failed))]
    BodyReadFailed {
        url: reqwest::Url,
        #[source]
        source: reqwest::Error,
    },

    #[error("failed to parse DESCRIPTION response from {url}: {details}")]
    #[diagnostic(code(rpx::http::description_parse_failed))]
    DescriptionParseFailed { url: reqwest::Url, details: String },

    #[error("failed to decompress PACKAGES.gz response from {url}: {source}")]
    #[diagnostic(code(rpx::http::packages_decompress_failed))]
    PackagesDecompressFailed {
        url: reqwest::Url,
        #[source]
        source: std::io::Error,
    },

    #[error("failed to parse PACKAGES response from {url}: {details}")]
    #[diagnostic(code(rpx::http::packages_parse_failed))]
    PackagesParseFailed { url: reqwest::Url, details: String },

    #[error("failed to parse archive listing response from {url}: {details}")]
    #[diagnostic(code(rpx::http::archive_listing_parse_failed))]
    ArchiveListingParseFailed { url: reqwest::Url, details: String },
}

fn archive_listing_parts(listing: &str) -> impl Iterator<Item = &str> {
    listing.split(['"', '\'', '<', '>', ' ', '\n', '\r', '\t'])
}

fn html_unescape_minimal(value: &str) -> String {
    value
        .replace("&amp;", "&")
        .replace("&lt;", "<")
        .replace("&gt;", ">")
        .replace("&quot;", "\"")
}

fn cran_package_index_entry_from_paragraph(
    paragraph: deb822_fast::Paragraph,
) -> Result<CranPackageIndexEntry, String> {
    let package = required_packages_field(&paragraph, "Package")?;
    let version = required_packages_field(&paragraph, "Version")?;

    Ok(CranPackageIndexEntry {
        package,
        version,
        depends: parse_packages_relations_field(&paragraph, "Depends")?,
        imports: parse_packages_relations_field(&paragraph, "Imports")?,
        suggests: parse_packages_relations_field(&paragraph, "Suggests")?,
        linking_to: parse_packages_relations_field(&paragraph, "LinkingTo")?,
        system_requirements: paragraph
            .get("SystemRequirements")
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(ToString::to_string),
    })
}

fn required_packages_field(
    paragraph: &deb822_fast::Paragraph,
    field: &'static str,
) -> Result<String, String> {
    paragraph
        .get(field)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToString::to_string)
        .ok_or_else(|| format!("missing required field {field}"))
}

fn parse_packages_relations_field(
    paragraph: &deb822_fast::Paragraph,
    field: &'static str,
) -> Result<r_description::lossy::Relations, String> {
    paragraph
        .get(field)
        .map(str::parse)
        .transpose()
        .map_err(|details| format!("failed to parse {field}: {details}"))
        .map(Option::unwrap_or_default)
}

async fn artifact_response(
    client: &HttpClient,
    url: reqwest::Url,
) -> Result<ArtifactResponse, HttpError> {
    let response =
        client
            .get(url.clone())
            .send()
            .await
            .map_err(|source| HttpError::RequestFailed {
                url: url.clone(),
                source,
            })?;

    let status = response.status();
    if status != reqwest::StatusCode::OK {
        let body = response.text().await.unwrap_or_default();
        return Err(HttpError::UnexpectedStatus { url, status, body });
    }

    Ok(ArtifactResponse {
        content_length: response.content_length(),
        stream: Box::pin(response.bytes_stream()),
    })
}

pub async fn rrepo_repository_packages(
    client: &HttpClient,
    base_url: &reqwest::Url,
) -> Result<RrepoPackagesResponse, HttpError> {
    let mut url = base_url.clone();
    url.path_segments_mut()
        .expect("repository base URL should support path segments")
        .push("packages");

    let response =
        client
            .get(url.clone())
            .send()
            .await
            .map_err(|source| HttpError::RequestFailed {
                url: url.clone(),
                source,
            })?;

    let status = response.status();
    if !status.is_success() {
        let body = response.text().await.unwrap_or_default();
        return Err(HttpError::UnexpectedStatus { url, status, body });
    }

    response
        .json::<RrepoPackagesResponse>()
        .await
        .map_err(|source| HttpError::JsonDecodeFailed { url, source })
}

pub async fn rrepo_package_versions(
    client: &HttpClient,
    base_url: &reqwest::Url,
    package: &str,
) -> Result<RrepoPackageVersionsResponse, HttpError> {
    let mut url = base_url.clone();
    url.path_segments_mut()
        .expect("repository base URL should support path segments")
        .extend(["packages", package, "versions"]);

    let response =
        client
            .get(url.clone())
            .send()
            .await
            .map_err(|source| HttpError::RequestFailed {
                url: url.clone(),
                source,
            })?;

    let status = response.status();
    if !status.is_success() {
        let body = response.text().await.unwrap_or_default();
        return Err(HttpError::UnexpectedStatus { url, status, body });
    }

    response
        .json::<RrepoPackageVersionsResponse>()
        .await
        .map_err(|source| HttpError::JsonDecodeFailed { url, source })
}

pub async fn rrepo_package_description(
    client: &HttpClient,
    base_url: &reqwest::Url,
    package: &str,
    version: &str,
) -> Result<r_description::lossy::RDescription, HttpError> {
    let mut url = base_url.clone();
    url.path_segments_mut()
        .expect("repository base URL should support path segments")
        .extend(["packages", package, "versions", version, "description"]);

    let response =
        client
            .get(url.clone())
            .send()
            .await
            .map_err(|source| HttpError::RequestFailed {
                url: url.clone(),
                source,
            })?;

    let status = response.status();
    if status != reqwest::StatusCode::OK {
        let body = response.text().await.unwrap_or_default();
        return Err(HttpError::UnexpectedStatus { url, status, body });
    }

    let body = response
        .text()
        .await
        .map_err(|source| HttpError::BodyReadFailed {
            url: url.clone(),
            source,
        })?;

    r_description::lossy::RDescription::from_str(&body)
        .map_err(|details| HttpError::DescriptionParseFailed { url, details })
}

pub async fn rrepo_source_artifact(
    client: &HttpClient,
    base_url: &reqwest::Url,
    package: &str,
    version: &str,
) -> Result<ArtifactResponse, HttpError> {
    let mut url = base_url.clone();
    url.path_segments_mut()
        .expect("repository base URL should support path segments")
        .extend(["packages", package, "versions", version, "source"]);

    artifact_response(client, url).await
}

pub async fn rrepo_windows_binary(
    client: &HttpClient,
    base_url: &reqwest::Url,
    package: &str,
    version: &str,
    r_minor: &str,
) -> Result<ArtifactResponse, HttpError> {
    let mut url = base_url.clone();
    url.path_segments_mut()
        .expect("repository base URL should support path segments")
        .extend([
            "packages", package, "versions", version, "binaries", "windows", r_minor,
        ]);

    artifact_response(client, url).await
}

pub async fn rrepo_macos_binary(
    client: &HttpClient,
    base_url: &reqwest::Url,
    package: &str,
    version: &str,
    target: &str,
    r_minor: &str,
) -> Result<ArtifactResponse, HttpError> {
    let mut url = base_url.clone();
    url.path_segments_mut()
        .expect("repository base URL should support path segments")
        .extend([
            "packages", package, "versions", version, "binaries", "macos", target, r_minor,
        ]);

    artifact_response(client, url).await
}

async fn cran_packages_gz(
    client: &HttpClient,
    base_url: &reqwest::Url,
) -> Result<CranPackagesIndex, HttpError> {
    let mut url = base_url.clone();
    url.path_segments_mut()
        .expect("repository base URL should support path segments")
        .extend(["src", "contrib", "PACKAGES.gz"]);

    let response =
        client
            .get(url.clone())
            .send()
            .await
            .map_err(|source| HttpError::RequestFailed {
                url: url.clone(),
                source,
            })?;

    let status = response.status();
    if status != reqwest::StatusCode::OK {
        let body = response.text().await.unwrap_or_default();
        return Err(HttpError::UnexpectedStatus { url, status, body });
    }

    let bytes = response
        .bytes()
        .await
        .map_err(|source| HttpError::BodyReadFailed {
            url: url.clone(),
            source,
        })?;

    let mut decoder = GzDecoder::new(bytes.as_ref());
    let mut body = String::new();
    decoder
        .read_to_string(&mut body)
        .map_err(|source| HttpError::PackagesDecompressFailed {
            url: url.clone(),
            source,
        })?;

    CranPackagesIndex::from_str(&body)
        .map_err(|details| HttpError::PackagesParseFailed { url, details })
}

async fn cran_packages_uncompressed(
    client: &HttpClient,
    base_url: &reqwest::Url,
) -> Result<CranPackagesIndex, HttpError> {
    let mut url = base_url.clone();
    url.path_segments_mut()
        .expect("repository base URL should support path segments")
        .extend(["src", "contrib", "PACKAGES"]);

    let response =
        client
            .get(url.clone())
            .send()
            .await
            .map_err(|source| HttpError::RequestFailed {
                url: url.clone(),
                source,
            })?;

    let status = response.status();
    if status != reqwest::StatusCode::OK {
        let body = response.text().await.unwrap_or_default();
        return Err(HttpError::UnexpectedStatus { url, status, body });
    }

    let body = response
        .text()
        .await
        .map_err(|source| HttpError::BodyReadFailed {
            url: url.clone(),
            source,
        })?;

    CranPackagesIndex::from_str(&body)
        .map_err(|details| HttpError::PackagesParseFailed { url, details })
}

pub async fn cran_packages(
    client: &HttpClient,
    base_url: &reqwest::Url,
) -> Result<CranPackagesIndex, HttpError> {
    match cran_packages_gz(client, base_url).await {
        Ok(index) => Ok(index),
        Err(_) => cran_packages_uncompressed(client, base_url).await,
    }
}

pub async fn cran_archive_root(
    client: &HttpClient,
    base_url: &reqwest::Url,
) -> Result<CranArchiveRootListing, HttpError> {
    let mut url = base_url.clone();
    url.path_segments_mut()
        .expect("repository base URL should support path segments")
        .extend(["src", "contrib", "Archive", ""]);

    let response =
        client
            .get(url.clone())
            .send()
            .await
            .map_err(|source| HttpError::RequestFailed {
                url: url.clone(),
                source,
            })?;

    let status = response.status();
    if status != reqwest::StatusCode::OK {
        let body = response.text().await.unwrap_or_default();
        return Err(HttpError::UnexpectedStatus { url, status, body });
    }

    let body = response
        .text()
        .await
        .map_err(|source| HttpError::BodyReadFailed {
            url: url.clone(),
            source,
        })?;

    CranArchiveRootListing::from_str(&body)
        .map_err(|details| HttpError::ArchiveListingParseFailed { url, details })
}

pub async fn cran_package_archive_listing(
    client: &HttpClient,
    base_url: &reqwest::Url,
    package: &str,
) -> Result<Option<CranPackageArchiveListing>, HttpError> {
    let mut url = base_url.clone();
    url.path_segments_mut()
        .expect("repository base URL should support path segments")
        .extend(["src", "contrib", "Archive", package, ""]);

    let response =
        client
            .get(url.clone())
            .send()
            .await
            .map_err(|source| HttpError::RequestFailed {
                url: url.clone(),
                source,
            })?;

    let status = response.status();
    if status == reqwest::StatusCode::NOT_FOUND {
        return Ok(None);
    }
    if status != reqwest::StatusCode::OK {
        let body = response.text().await.unwrap_or_default();
        return Err(HttpError::UnexpectedStatus { url, status, body });
    }

    let body = response
        .text()
        .await
        .map_err(|source| HttpError::BodyReadFailed {
            url: url.clone(),
            source,
        })?;

    CranPackageArchiveListing::from_str(&body)
        .map(Some)
        .map_err(|details| HttpError::ArchiveListingParseFailed { url, details })
}

pub async fn cran_current_source_tarball(
    client: &HttpClient,
    base_url: &reqwest::Url,
    package: &str,
    version: &str,
) -> Result<ArtifactResponse, HttpError> {
    let file_name = format!("{package}_{version}.tar.gz");
    let mut url = base_url.clone();
    url.path_segments_mut()
        .expect("repository base URL should support path segments")
        .extend(["src", "contrib", &file_name]);

    artifact_response(client, url).await
}

pub async fn cran_archive_source_tarball(
    client: &HttpClient,
    base_url: &reqwest::Url,
    package: &str,
    version: &str,
) -> Result<ArtifactResponse, HttpError> {
    let file_name = format!("{package}_{version}.tar.gz");
    let mut url = base_url.clone();
    url.path_segments_mut()
        .expect("repository base URL should support path segments")
        .extend(["src", "contrib", "Archive", package, &file_name]);

    artifact_response(client, url).await
}

pub async fn cran_latest_package_description(
    client: &HttpClient,
    base_url: &reqwest::Url,
    package: &str,
) -> Result<r_description::lossy::RDescription, HttpError> {
    let mut url = base_url.clone();
    url.path_segments_mut()
        .expect("repository base URL should support path segments")
        .extend(["web", "packages", package, "DESCRIPTION"]);

    let response =
        client
            .get(url.clone())
            .send()
            .await
            .map_err(|source| HttpError::RequestFailed {
                url: url.clone(),
                source,
            })?;

    let status = response.status();
    if status != reqwest::StatusCode::OK {
        let body = response.text().await.unwrap_or_default();
        return Err(HttpError::UnexpectedStatus { url, status, body });
    }

    let body = response
        .text()
        .await
        .map_err(|source| HttpError::BodyReadFailed {
            url: url.clone(),
            source,
        })?;

    r_description::lossy::RDescription::from_str(&body)
        .map_err(|details| HttpError::DescriptionParseFailed { url, details })
}

pub async fn cran_windows_binary(
    client: &HttpClient,
    base_url: &reqwest::Url,
    r_minor: &str,
    package: &str,
    version: &str,
) -> Result<ArtifactResponse, HttpError> {
    let file_name = format!("{package}_{version}.zip");
    let mut url = base_url.clone();
    url.path_segments_mut()
        .expect("repository base URL should support path segments")
        .extend(["bin", "windows", "contrib", r_minor, &file_name]);

    artifact_response(client, url).await
}

pub async fn cran_macos_binary(
    client: &HttpClient,
    base_url: &reqwest::Url,
    target: &str,
    r_minor: &str,
    package: &str,
    version: &str,
) -> Result<ArtifactResponse, HttpError> {
    let file_name = format!("{package}_{version}.tgz");
    let mut url = base_url.clone();
    url.path_segments_mut()
        .expect("repository base URL should support path segments")
        .extend(["bin", "macosx", target, "contrib", r_minor, &file_name]);

    artifact_response(client, url).await
}

pub async fn cran_package_description(
    client: &HttpClient,
    base_url: &reqwest::Url,
    package: &str,
    version: &str,
) -> Result<r_description::lossy::RDescription, HttpError> {
    match cran_latest_package_description(client, base_url, package).await {
        Ok(description) if description.version.to_string() == version => {
            return Ok(description);
        }
        Ok(_) | Err(_) => {
            // The latest DESCRIPTION is either unavailable or not the exact
            // selected version. Fall back to the archived source tarball.
        }
    }

    let artifact = cran_archive_source_tarball(client, base_url, package, version).await?;
    let body = description_body_from_source_artifact(artifact, base_url, package, version).await?;

    parse_cran_description_body(&body, base_url, package, version)
}

async fn description_body_from_source_artifact(
    mut artifact: ArtifactResponse,
    base_url: &reqwest::Url,
    package: &str,
    version: &str,
) -> Result<String, HttpError> {
    use futures_util::TryStreamExt;

    let mut bytes = Vec::with_capacity(artifact.content_length.unwrap_or_default() as usize);

    while let Some(chunk) = artifact
        .stream
        .try_next()
        .await
        .map_err(|source| HttpError::BodyReadFailed {
            url: cran_archive_source_url_for_error(base_url, package, version),
            source,
        })?
    {
        bytes.extend_from_slice(&chunk);
    }

    description_body_from_tar_gz_bytes(&bytes, base_url, package, version)
}

fn description_body_from_tar_gz_bytes(
    bytes: &[u8],
    base_url: &reqwest::Url,
    package: &str,
    version: &str,
) -> Result<String, HttpError> {
    let url = cran_archive_source_url_for_error(base_url, package, version);
    let decoder = GzDecoder::new(bytes);
    let mut archive = tar::Archive::new(decoder);

    let entries = archive
        .entries()
        .map_err(|source| HttpError::ArtifactExtractFailed {
            url: url.clone(),
            details: format!("failed to read source package archive: {source}"),
        })?;

    for entry in entries {
        let mut entry = entry.map_err(|source| HttpError::ArtifactExtractFailed {
            url: url.clone(),
            details: format!("failed to read archive entry: {source}"),
        })?;

        let is_description = {
            let path = entry.path().map_err(|source| HttpError::ArtifactExtractFailed {
                url: url.clone(),
                details: format!("failed to read archive entry path: {source}"),
            })?;

            path_is_top_level_description(&path, package)
        };

        if !is_description {
            continue;
        }

        let mut body = String::new();
        entry
            .read_to_string(&mut body)
            .map_err(|source| HttpError::ArtifactExtractFailed {
                url: url.clone(),
                details: format!("failed to read DESCRIPTION from source package: {source}"),
            })?;

        return Ok(body);
    }

    Err(HttpError::ArtifactExtractFailed {
        url,
        details: format!("source package does not contain {package}/DESCRIPTION"),
    })
}

fn parse_cran_description_body(
    body: &str,
    base_url: &reqwest::Url,
    package: &str,
    version: &str,
) -> Result<r_description::lossy::RDescription, HttpError> {
    let url = cran_archive_source_url_for_error(base_url, package, version);

    let description = r_description::lossy::RDescription::from_str(body).map_err(|details| {
        HttpError::DescriptionParseFailed {
            url: url.clone(),
            details,
        }
    })?;

    Ok(description)
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

fn cran_archive_source_url_for_error(
    base_url: &reqwest::Url,
    package: &str,
    version: &str,
) -> reqwest::Url {
    let file_name = format!("{package}_{version}.tar.gz");
    let mut url = base_url.clone();

    url.path_segments_mut()
        .expect("repository base URL should support path segments")
        .extend(["src", "contrib", "Archive", package, &file_name]);

    url
}
