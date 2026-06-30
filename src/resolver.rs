use std::{
    cmp::{Ordering, Reverse},
    collections::{BTreeMap, BTreeSet},
    error::Error,
    fmt,
    io::Read,
    path::Path,
    rc::Rc,
    str::FromStr,
};

use flate2::read::GzDecoder;
use pubgrub::{
    Dependencies, DependencyConstraints, DependencyProvider, PackageResolutionStatistics, Ranges,
    VersionSet, resolve,
};
use r_description::{Version, VersionConstraint};
use tar::Archive;

use crate::{
    description::{DescriptionDependency, RDescription},
    registry::{RegistryClient, is_not_found_error as is_registry_not_found_error},
    repository::{CranArchiveSupport, normalize_repository_url},
    ui::ResolutionUi,
};

const ROOT_PACKAGE: &str = "__rpx_root__";
const BASE_PACKAGES: &[&str] = &[
    "base",
    "compiler",
    "datasets",
    "graphics",
    "grDevices",
    "grid",
    "methods",
    "parallel",
    "splines",
    "stats",
    "stats4",
    "tcltk",
    "tools",
    "utils",
];

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedPackage {
    pub name: String,
    pub version: String,
    pub source_url: String,
    pub dependencies: Vec<ResolvedDependency>,
    pub system_requirements: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedDependency {
    pub package: String,
    pub kind: String,
    pub min_version: Option<String>,
    pub max_version_exclusive: Option<String>,
}

#[derive(Debug, Clone)]
struct VersionCandidate {
    version: String,
    repository_url: String,
    source_url: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ResolverError(String);

impl fmt::Display for ResolverError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl Error for ResolverError {}

impl From<String> for ResolverError {
    fn from(value: String) -> Self {
        Self(value)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PackageRepositoryKind {
    Rrepo,
    CranLike,
}

#[derive(Debug, Clone)]
pub(crate) struct PackageDependency {
    package: String,
    range: Ranges<Version>,
}

impl PackageDependency {
    pub(crate) fn from_relation(relation: &DescriptionDependency) -> Self {
        Self {
            package: relation.name.clone(),
            range: r_description_range_from_relation(relation),
        }
    }

    pub(crate) fn package(&self) -> &str {
        &self.package
    }

    pub(crate) fn any(package: impl Into<String>) -> Self {
        Self {
            package: package.into(),
            range: Ranges::full(),
        }
    }
}

#[derive(Debug, Clone)]
struct RepositoryVersion {
    version: Version,
    repository: Rc<dyn PackageRepository>,
}

impl fmt::Display for RepositoryVersion {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}@{}", self.repository.base_url(), self.version)
    }
}

impl PartialEq for RepositoryVersion {
    fn eq(&self, other: &Self) -> bool {
        self.version == other.version && self.repository.base_url() == other.repository.base_url()
    }
}

impl Eq for RepositoryVersion {}

impl Ord for RepositoryVersion {
    fn cmp(&self, other: &Self) -> Ordering {
        self.version
            .cmp(&other.version)
            .then_with(|| self.repository.base_url().cmp(other.repository.base_url()))
    }
}

impl PartialOrd for RepositoryVersion {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct RepositoryVersionRange {
    range: Ranges<Version>,
}

impl fmt::Display for RepositoryVersionRange {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.range.fmt(f)
    }
}

impl VersionSet for RepositoryVersionRange {
    type V = RepositoryVersion;

    fn empty() -> Self {
        Self {
            range: Ranges::empty(),
        }
    }

    fn singleton(v: Self::V) -> Self {
        Self {
            range: Ranges::singleton(v.version),
        }
    }

    fn complement(&self) -> Self {
        Self {
            range: self.range.complement(),
        }
    }

    fn intersection(&self, other: &Self) -> Self {
        Self {
            range: self.range.intersection(&other.range),
        }
    }

    fn contains(&self, v: &Self::V) -> bool {
        self.range.contains(&v.version)
    }

    fn full() -> Self {
        Self {
            range: Ranges::full(),
        }
    }
}

pub(crate) trait PackageRepository: std::fmt::Debug {
    fn base_url(&self) -> &str;

    fn kind(&self) -> PackageRepositoryKind;

    fn cran_archive_support(&self) -> Option<CranArchiveSupport> {
        None
    }

    fn package_list(&self) -> Result<Vec<String>, ResolverError>;

    fn package_versions(&self, package: &str) -> Result<Vec<Version>, ResolverError>;

    fn package_description(
        &self,
        package: &str,
        version: &Version,
    ) -> Result<Option<Vec<PackageDependency>>, ResolverError>;

    fn resolved_package(
        &self,
        package: &str,
        version: &Version,
    ) -> Result<Option<ResolvedPackage>, ResolverError>;
}

#[derive(Debug, Default)]
struct NoopPackageRepository;

impl PackageRepository for NoopPackageRepository {
    fn base_url(&self) -> &str {
        ""
    }

    fn kind(&self) -> PackageRepositoryKind {
        PackageRepositoryKind::Rrepo
    }

    fn package_list(&self) -> Result<Vec<String>, ResolverError> {
        Ok(Vec::new())
    }

    fn package_versions(&self, _package: &str) -> Result<Vec<Version>, ResolverError> {
        Ok(Vec::new())
    }

    fn package_description(
        &self,
        _package: &str,
        _version: &Version,
    ) -> Result<Option<Vec<PackageDependency>>, ResolverError> {
        Ok(None)
    }

    fn resolved_package(
        &self,
        _package: &str,
        _version: &Version,
    ) -> Result<Option<ResolvedPackage>, ResolverError> {
        Ok(None)
    }
}

#[derive(Debug)]
pub(crate) struct RrepoPackageRepository {
    base_url: String,
}

impl RrepoPackageRepository {
    pub(crate) fn new(base_url: impl Into<String>) -> Self {
        Self {
            base_url: base_url.into(),
        }
    }
}

impl PackageRepository for RrepoPackageRepository {
    fn base_url(&self) -> &str {
        &self.base_url
    }

    fn kind(&self) -> PackageRepositoryKind {
        PackageRepositoryKind::Rrepo
    }

    fn package_list(&self) -> Result<Vec<String>, ResolverError> {
        let client = RegistryClient::new(&self.base_url);
        let response = client.fetch_repository_packages().map_err(ResolverError)?;
        Ok(response
            .packages
            .into_iter()
            .map(|package| package.name)
            .collect())
    }

    fn package_versions(&self, package: &str) -> Result<Vec<Version>, ResolverError> {
        let client = RegistryClient::new(&self.base_url);
        let response = match client.fetch_package_versions_with_retry(package) {
            Ok(response) => response,
            Err(error) if is_registry_not_found_error(&error) => return Ok(Vec::new()),
            Err(error) => return Err(ResolverError(error)),
        };

        response
            .versions
            .into_iter()
            .map(|summary| summary.version.parse().map_err(ResolverError))
            .collect::<Result<BTreeSet<_>, _>>()
            .map(|versions| versions.into_iter().collect())
    }

    fn package_description(
        &self,
        package: &str,
        version: &Version,
    ) -> Result<Option<Vec<PackageDependency>>, ResolverError> {
        let client = RegistryClient::new(&self.base_url);
        let version = version.to_string();
        let description = match client.fetch_description_with_retry(package, &version) {
            Ok(description) => description,
            Err(error) if is_registry_not_found_error(&error) => return Ok(None),
            Err(error) => return Err(ResolverError(error)),
        };
        let description = RDescription::from_str(&description).map_err(ResolverError)?;
        Ok(Some(description.dependencies()))
    }

    fn resolved_package(
        &self,
        package: &str,
        version: &Version,
    ) -> Result<Option<ResolvedPackage>, ResolverError> {
        let client = RegistryClient::new(&self.base_url);
        let version = version.to_string();
        let description = match client.fetch_description_with_retry(package, &version) {
            Ok(description) => description,
            Err(error) if is_registry_not_found_error(&error) => return Ok(None),
            Err(error) => return Err(ResolverError(error)),
        };
        let description = RDescription::from_str(&description).map_err(ResolverError)?;

        Ok(Some(ResolvedPackage {
            name: package.to_string(),
            version: version.clone(),
            source_url: format!(
                "{}/packages/{package}/versions/{version}/source",
                self.base_url
            ),
            dependencies: resolved_dependencies_from_r_description(&description),
            system_requirements: description.system_requirements,
        }))
    }
}

#[derive(Debug)]
pub(crate) struct CranLikePackageRepository {
    base_url: String,
    archive_support: Option<CranArchiveSupport>,
}

impl CranLikePackageRepository {
    pub(crate) fn new(
        base_url: impl AsRef<str>,
        archive_support: Option<CranArchiveSupport>,
    ) -> Self {
        Self {
            base_url: normalize_repository_url(base_url.as_ref()),
            archive_support,
        }
    }
}

impl PackageRepository for CranLikePackageRepository {
    fn base_url(&self) -> &str {
        &self.base_url
    }

    fn kind(&self) -> PackageRepositoryKind {
        PackageRepositoryKind::CranLike
    }

    fn cran_archive_support(&self) -> Option<CranArchiveSupport> {
        self.archive_support
    }

    fn package_list(&self) -> Result<Vec<String>, ResolverError> {
        Ok(cran_like_current_index(self)?
            .into_keys()
            .collect::<Vec<_>>())
    }

    fn package_versions(&self, package: &str) -> Result<Vec<Version>, ResolverError> {
        let mut versions = cran_like_current_index(self)?
            .remove(package)
            .unwrap_or_default()
            .into_iter()
            .collect::<BTreeSet<_>>();

        if self.archive_support != Some(CranArchiveSupport::Unavailable) {
            versions.extend(cran_like_archive_versions(self, package)?);
        }

        Ok(versions.into_iter().collect())
    }

    fn package_description(
        &self,
        package: &str,
        version: &Version,
    ) -> Result<Option<Vec<PackageDependency>>, ResolverError> {
        let version = version.to_string();
        let Some(description) = cran_like_description(self, package, &version)? else {
            return Ok(None);
        };
        let description = RDescription::from_str(&description).map_err(ResolverError)?;
        Ok(Some(description.dependencies()))
    }

    fn resolved_package(
        &self,
        package: &str,
        version: &Version,
    ) -> Result<Option<ResolvedPackage>, ResolverError> {
        let version = version.to_string();
        let Some(description) = cran_like_description(self, package, &version)? else {
            return Ok(None);
        };
        let description = RDescription::from_str(&description).map_err(ResolverError)?;

        Ok(Some(ResolvedPackage {
            name: package.to_string(),
            version: version.clone(),
            source_url: cran_like_source_url(self, package, &version)?,
            dependencies: resolved_dependencies_from_r_description(&description),
            system_requirements: description.system_requirements,
        }))
    }
}

fn cran_like_current_index(
    repository: &CranLikePackageRepository,
) -> Result<BTreeMap<String, Vec<Version>>, ResolverError> {
    let body = cran_like_packages_index(repository)?;
    let mut index = BTreeMap::<String, Vec<Version>>::new();

    for record in parse_dcf_records(&body) {
        let Some(package) = record.get("Package").filter(|value| !value.is_empty()) else {
            continue;
        };
        let Some(version) = record.get("Version").filter(|value| !value.is_empty()) else {
            continue;
        };
        let version = version
            .parse::<Version>()
            .map_err(|error| ResolverError(format!("invalid version {version}: {error}")))?;
        index.entry(package.to_string()).or_default().push(version);
    }

    for versions in index.values_mut() {
        versions.sort();
        versions.dedup();
    }

    Ok(index)
}

fn cran_like_packages_index(
    repository: &CranLikePackageRepository,
) -> Result<String, ResolverError> {
    cran_like_packages_gz_index(repository).or_else(|_| cran_like_packages_plain_index(repository))
}

fn cran_like_packages_gz_index(
    repository: &CranLikePackageRepository,
) -> Result<String, ResolverError> {
    let url = format!("{}/src/contrib/PACKAGES.gz", repository.base_url());
    let response = reqwest::blocking::get(&url).map_err(|error| {
        ResolverError(format!("failed to contact CRAN-like repository: {error}"))
    })?;
    if !response.status().is_success() {
        return Err(ResolverError(unexpected_cran_like_response(response)));
    }

    let bytes = response.bytes().map_err(|error| {
        ResolverError(format!(
            "failed to read CRAN-like PACKAGES.gz index: {error}"
        ))
    })?;
    let mut decoder = GzDecoder::new(bytes.as_ref());
    let mut body = String::new();
    decoder.read_to_string(&mut body).map_err(|error| {
        ResolverError(format!(
            "failed to decompress CRAN-like PACKAGES.gz index: {error}"
        ))
    })?;
    Ok(body)
}

fn cran_like_packages_plain_index(
    repository: &CranLikePackageRepository,
) -> Result<String, ResolverError> {
    let url = format!("{}/src/contrib/PACKAGES", repository.base_url());
    let response = reqwest::blocking::get(&url).map_err(|error| {
        ResolverError(format!("failed to contact CRAN-like repository: {error}"))
    })?;
    if !response.status().is_success() {
        return Err(ResolverError(unexpected_cran_like_response(response)));
    }

    response
        .text()
        .map_err(|error| ResolverError(format!("failed to read CRAN-like PACKAGES index: {error}")))
}

fn cran_like_archive_versions(
    repository: &CranLikePackageRepository,
    package: &str,
) -> Result<Vec<Version>, ResolverError> {
    let url = format!("{}/src/contrib/Archive/{package}/", repository.base_url());
    let response = reqwest::blocking::get(&url)
        .map_err(|error| ResolverError(format!("failed to contact CRAN-like archive: {error}")))?;
    let status = response.status();
    if status == reqwest::StatusCode::NOT_FOUND || status == reqwest::StatusCode::FORBIDDEN {
        return Ok(Vec::new());
    }
    if !status.is_success() {
        return Err(ResolverError(unexpected_cran_like_response(response)));
    }

    let body = response.text().map_err(|error| {
        ResolverError(format!("failed to read CRAN-like archive listing: {error}"))
    })?;
    let mut versions = tarball_file_names_from_listing(&body)
        .into_iter()
        .filter_map(|file_name| {
            parse_cran_tarball_file_name(&file_name).and_then(|(name, version)| {
                if name != package {
                    return None;
                }
                version.parse::<Version>().ok()
            })
        })
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    versions.sort();
    Ok(versions)
}

fn cran_like_description(
    repository: &CranLikePackageRepository,
    package: &str,
    version: &str,
) -> Result<Option<String>, ResolverError> {
    let direct_url = cran_like_current_description_url(repository, package);
    let current_url = cran_like_current_tarball_url(repository, package, version);
    let archive_url = cran_like_archive_tarball_url(repository, package, version);
    let candidates = [
        DescriptionCandidate::Direct(direct_url),
        DescriptionCandidate::Tarball(current_url),
        DescriptionCandidate::Tarball(archive_url),
    ];
    let mut errors = Vec::new();

    for candidate in candidates {
        let result = match candidate {
            DescriptionCandidate::Direct(url) => {
                let result = fetch_description_from_direct_url(&url, package, version);
                result.map_err(|error| (url, error))
            }
            DescriptionCandidate::Tarball(url) => {
                let result = fetch_description_from_tarball(&url, package, version);
                result.map_err(|error| (url, error))
            }
        };

        match result {
            Ok(description) => return Ok(Some(description)),
            Err(error) => errors.push(error),
        }
    }

    if errors
        .iter()
        .all(|(_, error)| is_not_found_or_wrong_version(error))
    {
        return Ok(None);
    }

    let error = errors
        .into_iter()
        .find(|(_, error)| !is_not_found_or_wrong_version(error))
        .map_or_else(|| "DESCRIPTION not found".to_string(), |(_, error)| error);
    Err(ResolverError(error))
}

fn cran_like_source_url(
    repository: &CranLikePackageRepository,
    package: &str,
    version: &str,
) -> Result<String, ResolverError> {
    let current_versions = cran_like_current_index(repository)?
        .remove(package)
        .unwrap_or_default();

    if current_versions
        .iter()
        .any(|current| current.to_string() == version)
    {
        return Ok(cran_like_current_tarball_url(repository, package, version));
    }

    Ok(cran_like_archive_tarball_url(repository, package, version))
}

enum DescriptionCandidate {
    Direct(String),
    Tarball(String),
}

fn fetch_description_from_direct_url(
    url: &str,
    package: &str,
    version: &str,
) -> Result<String, String> {
    let response = reqwest::blocking::get(url)
        .map_err(|error| format!("failed to download DESCRIPTION: {error}"))?;
    if !response.status().is_success() {
        return Err(unexpected_cran_like_response(response));
    }

    let description = response
        .text()
        .map_err(|error| format!("failed to read DESCRIPTION: {error}"))?;
    validate_description(&description, package, version, url)?;
    Ok(description)
}

fn fetch_description_from_tarball(
    url: &str,
    package: &str,
    version: &str,
) -> Result<String, String> {
    let response = reqwest::blocking::get(url)
        .map_err(|error| format!("failed to download source package for DESCRIPTION: {error}"))?;
    if !response.status().is_success() {
        return Err(unexpected_cran_like_response(response));
    }

    let bytes = response
        .bytes()
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

fn cran_like_current_tarball_url(
    repository: &CranLikePackageRepository,
    package: &str,
    version: &str,
) -> String {
    format!(
        "{}/src/contrib/{package}_{version}.tar.gz",
        repository.base_url()
    )
}

fn cran_like_archive_tarball_url(
    repository: &CranLikePackageRepository,
    package: &str,
    version: &str,
) -> String {
    format!(
        "{}/src/contrib/Archive/{package}/{package}_{version}.tar.gz",
        repository.base_url()
    )
}

fn cran_like_current_description_url(
    repository: &CranLikePackageRepository,
    package: &str,
) -> String {
    format!(
        "{}/web/packages/{package}/DESCRIPTION",
        repository.base_url()
    )
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

fn tarball_file_names_from_listing(listing: &str) -> Vec<String> {
    listing
        .split(['"', '\'', '<', '>', ' ', '\n', '\r', '\t'])
        .filter_map(|part| part.rsplit('/').next())
        .filter(|part| part.ends_with(".tar.gz") && part.contains('_'))
        .map(html_unescape_minimal)
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect()
}

fn parse_cran_tarball_file_name(file_name: &str) -> Option<(&str, &str)> {
    let stem = file_name.strip_suffix(".tar.gz")?;
    let (package, version) = stem.rsplit_once('_')?;
    if package.is_empty() || version.is_empty() {
        return None;
    }
    Some((package, version))
}

fn html_unescape_minimal(value: &str) -> String {
    value
        .replace("&amp;", "&")
        .replace("&lt;", "<")
        .replace("&gt;", ">")
        .replace("&quot;", "\"")
}

fn unexpected_cran_like_response(response: reqwest::blocking::Response) -> String {
    let status = response.status();
    let body = response.text().unwrap_or_default();
    let body = body.trim();

    if body.is_empty() {
        return format!("unexpected registry response ({status})");
    }

    format!("unexpected registry response ({status}): {body}")
}

fn is_not_found_or_wrong_version(error: &str) -> bool {
    is_registry_not_found_error(error) || error.contains("does not describe package")
}

impl RDescription {
    pub(crate) fn dependencies(&self) -> Vec<PackageDependency> {
        let mut dependencies = Vec::new();

        dependencies.extend(package_dependencies_from_relations(&self.depends));
        dependencies.extend(package_dependencies_from_relations(&self.imports));
        dependencies.extend(package_dependencies_from_relations(&self.linking_to));

        dependencies
    }
}

fn package_dependencies_from_relations(
    relations: &BTreeSet<DescriptionDependency>,
) -> Vec<PackageDependency> {
    relations
        .iter()
        .filter(|relation| relation.name != "R")
        .map(package_dependency_from_relation)
        .collect()
}

fn resolved_dependencies_from_r_description(description: &RDescription) -> Vec<ResolvedDependency> {
    let mut dependencies = Vec::new();

    dependencies.extend(resolved_dependencies_from_relations(
        "Depends",
        &description.depends,
    ));
    dependencies.extend(resolved_dependencies_from_relations(
        "Imports",
        &description.imports,
    ));
    dependencies.extend(resolved_dependencies_from_relations(
        "LinkingTo",
        &description.linking_to,
    ));

    dependencies
}

fn resolved_dependencies_from_relations(
    kind: &str,
    relations: &BTreeSet<DescriptionDependency>,
) -> Vec<ResolvedDependency> {
    relations
        .iter()
        .filter(|relation| relation.name != "R")
        .map(|relation| {
            let (min_version, max_version_exclusive) = relation_bounds(relation);
            ResolvedDependency {
                package: relation.name.clone(),
                kind: kind.to_string(),
                min_version,
                max_version_exclusive,
            }
        })
        .collect()
}

fn relation_bounds(relation: &DescriptionDependency) -> (Option<String>, Option<String>) {
    let Some((operator, version)) = relation.version.as_ref() else {
        return (None, None);
    };

    match operator {
        VersionConstraint::GreaterThan | VersionConstraint::GreaterThanEqual => {
            (Some(version.to_string()), None)
        }
        VersionConstraint::LessThan | VersionConstraint::LessThanEqual => {
            (None, Some(version.to_string()))
        }
        VersionConstraint::Equal => (Some(version.to_string()), None),
    }
}

fn package_dependency_from_relation(relation: &DescriptionDependency) -> PackageDependency {
    PackageDependency::from_relation(relation)
}

fn r_description_range_from_relation(relation: &DescriptionDependency) -> Ranges<Version> {
    let Some((operator, version)) = relation.version.as_ref() else {
        return Ranges::full();
    };

    match operator {
        VersionConstraint::Equal => Ranges::singleton(version.clone()),
        VersionConstraint::GreaterThan => Ranges::strictly_higher_than(version.clone()),
        VersionConstraint::GreaterThanEqual => Ranges::higher_than(version.clone()),
        VersionConstraint::LessThan => Ranges::strictly_lower_than(version.clone()),
        VersionConstraint::LessThanEqual => Ranges::lower_than(version.clone()),
    }
}

#[derive(Debug)]
struct RDependencyProvider {
    repositories: Vec<Rc<dyn PackageRepository>>,
    root_dependencies: Vec<PackageDependency>,
    preferred_versions_by_package: BTreeMap<String, Version>,
    ui: ResolutionUi,
}

impl RDependencyProvider {
    fn new(
        repositories: Vec<Rc<dyn PackageRepository>>,
        root_dependencies: Vec<PackageDependency>,
        preferred_versions_by_package: BTreeMap<String, Version>,
        ui: ResolutionUi,
    ) -> Self {
        Self {
            repositories,
            root_dependencies,
            preferred_versions_by_package,
            ui,
        }
    }

    fn repository_versions(&self, package: &str) -> Result<Vec<RepositoryVersion>, ResolverError> {
        self.ui.on_version_load(package);
        let mut versions =
            self.repositories
                .iter()
                .try_fold(Vec::new(), |mut versions, repository| {
                    versions.extend(repository.package_versions(package)?.into_iter().map(
                        |version| RepositoryVersion {
                            version,
                            repository: Rc::clone(repository),
                        },
                    ));
                    Ok::<_, ResolverError>(versions)
                })?;
        versions.sort();
        Ok(versions)
    }

    fn repository_dependencies(
        &self,
        package: &str,
        version: &RepositoryVersion,
    ) -> Result<Option<Vec<PackageDependency>>, ResolverError> {
        self.ui.on_description_load(package, &version.version);
        version
            .repository
            .package_description(package, &version.version)
    }

    fn resolved_package(
        &self,
        package: &str,
        version: &RepositoryVersion,
    ) -> Result<Option<ResolvedPackage>, ResolverError> {
        self.ui.on_description_load(package, &version.version);
        version
            .repository
            .resolved_package(package, &version.version)
    }

    fn finish(&self, resolved_packages: usize) {
        self.ui.finish(resolved_packages);
    }

    fn fail(&self) {
        self.ui.fail();
    }
}

impl DependencyProvider for RDependencyProvider {
    type P = String;
    type V = RepositoryVersion;
    type VS = RepositoryVersionRange;
    type Priority = (u32, Reverse<usize>);
    type M = String;
    type Err = ResolverError;

    fn prioritize(
        &self,
        package: &Self::P,
        range: &Self::VS,
        package_conflicts_counts: &PackageResolutionStatistics,
    ) -> Self::Priority {
        let matches = self
            .repository_versions(package)
            .ok()
            .map_or(usize::MAX, |versions| {
                versions
                    .iter()
                    .filter(|version| range.contains(version))
                    .count()
            });

        (package_conflicts_counts.conflict_count(), Reverse(matches))
    }

    fn choose_version(
        &self,
        package: &Self::P,
        range: &Self::VS,
    ) -> Result<Option<Self::V>, Self::Err> {
        if package == ROOT_PACKAGE {
            let version = r_dependency_root_version();
            return Ok(range.contains(&version).then_some(version));
        }

        let versions = self.repository_versions(package)?;

        if let Some(preferred_version) = self.preferred_versions_by_package.get(package)
            && let Some(version) = versions
                .iter()
                .find(|version| &version.version == preferred_version && range.contains(version))
        {
            return Ok(Some(version.clone()));
        }

        Ok(versions
            .into_iter()
            .rev()
            .find(|version| range.contains(version)))
    }

    fn get_dependencies(
        &self,
        package: &Self::P,
        version: &Self::V,
    ) -> Result<Dependencies<Self::P, Self::VS, Self::M>, Self::Err> {
        if package == ROOT_PACKAGE {
            if *version != r_dependency_root_version() {
                return Ok(Dependencies::Unavailable(format!(
                    "unsupported root version: {version}"
                )));
            }

            return Ok(Dependencies::Available(
                self.root_dependencies
                    .iter()
                    .map(|dependency| {
                        (
                            dependency.package.clone(),
                            RepositoryVersionRange {
                                range: dependency.range.clone(),
                            },
                        )
                    })
                    .collect::<DependencyConstraints<_, _>>(),
            ));
        }

        let Some(dependencies) = self.repository_dependencies(package, version)? else {
            return Ok(Dependencies::Unavailable(format!(
                "package DESCRIPTION not found for {package}@{version}"
            )));
        };

        Ok(Dependencies::Available(
            dependencies
                .into_iter()
                .filter(|dependency| !is_base_package(&dependency.package))
                .map(|dependency| {
                    (
                        dependency.package,
                        RepositoryVersionRange {
                            range: dependency.range,
                        },
                    )
                })
                .collect::<DependencyConstraints<_, _>>(),
        ))
    }
}

#[allow(dead_code)]
fn r_dependency_root_version() -> RepositoryVersion {
    RepositoryVersion {
        version: "0.0.0".parse().expect("root version should parse"),
        repository: Rc::new(NoopPackageRepository),
    }
}

pub fn is_base_package(package: &str) -> bool {
    BASE_PACKAGES.contains(&package)
}

pub fn resolve_from_registry(
    repositories: Vec<Rc<dyn PackageRepository>>,
    roots: Vec<PackageDependency>,
    preferred_versions_by_package: BTreeMap<String, Version>,
) -> Result<Vec<ResolvedPackage>, String> {
    if roots.is_empty() {
        return Ok(Vec::new());
    }

    let provider = RDependencyProvider::new(
        repositories,
        roots,
        preferred_versions_by_package,
        ResolutionUi::new(),
    );
    let mut selected = match resolve(
        &provider,
        ROOT_PACKAGE.to_string(),
        r_dependency_root_version(),
    ) {
        Ok(selected) => selected
            .into_iter()
            .filter(|(package, _)| package != ROOT_PACKAGE)
            .collect::<Vec<_>>(),
        Err(error) => {
            provider.fail();
            return Err(error.to_string());
        }
    };
    selected.sort_by(|left, right| left.0.cmp(&right.0));
    let mut resolved = selected
        .into_iter()
        .map(|(package, version)| {
            provider
                .resolved_package(&package, &version)?
                .ok_or_else(|| {
                    ResolverError(format!(
                        "package DESCRIPTION not found for {package}@{version}"
                    ))
                })
        })
        .collect::<Result<Vec<_>, ResolverError>>()
        .map_err(|error| {
            provider.fail();
            error.to_string()
        })?;
    resolved.sort_by(|left, right| left.name.cmp(&right.name));
    provider.finish(resolved.len());
    Ok(resolved)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::repository::RepositorySource;
    use mockito::Server;

    #[derive(Debug, Clone)]
    struct TestPackageVersion {
        version: RPackageVersion,
        dependencies: Vec<(String, VersionRange)>,
    }

    #[derive(Debug, Clone)]
    struct TestProvider {
        root_dependencies: Vec<(String, VersionRange)>,
        packages: BTreeMap<String, Vec<TestPackageVersion>>,
    }

    impl DependencyProvider for TestProvider {
        type P = String;
        type V = RPackageVersion;
        type VS = VersionRange;
        type Priority = (u32, Reverse<usize>);
        type M = String;
        type Err = ResolverError;

        fn prioritize(
            &self,
            package: &Self::P,
            range: &Self::VS,
            package_conflicts_counts: &PackageResolutionStatistics,
        ) -> Self::Priority {
            let matches = self.packages.get(package).map_or(usize::MAX, |versions| {
                versions
                    .iter()
                    .filter(|version| range.contains(&version.version))
                    .count()
            });
            (package_conflicts_counts.conflict_count(), Reverse(matches))
        }

        fn choose_version(
            &self,
            package: &Self::P,
            range: &Self::VS,
        ) -> Result<Option<Self::V>, Self::Err> {
            if package == ROOT_PACKAGE {
                let version = root_version();
                return Ok(range.contains(&version).then_some(version));
            }

            Ok(self
                .packages
                .get(package)
                .into_iter()
                .flat_map(|versions| versions.iter())
                .rev()
                .find(|version| range.contains(&version.version))
                .map(|version| version.version.clone()))
        }

        fn get_dependencies(
            &self,
            package: &Self::P,
            version: &Self::V,
        ) -> Result<Dependencies<Self::P, Self::VS, Self::M>, Self::Err> {
            if package == ROOT_PACKAGE {
                return Ok(Dependencies::Available(
                    self.root_dependencies.clone().into_iter().collect(),
                ));
            }

            let dependencies = self
                .packages
                .get(package)
                .and_then(|versions| {
                    versions
                        .iter()
                        .find(|candidate| &candidate.version == version)
                })
                .map(|version| version.dependencies.clone())
                .unwrap_or_default();
            Ok(Dependencies::Available(dependencies.into_iter().collect()))
        }
    }

    #[test]
    fn compares_cran_hyphen_versions_numerically() {
        assert!(parse_version("7.3-65").unwrap() > parse_version("7.3-9").unwrap());
        assert!(parse_version("7.3-60.2").unwrap() > parse_version("7.3-60").unwrap());
    }

    #[test]
    fn extracts_supported_description_dependency_kinds() {
        let description = RDescription::from_str(
            "Package: testpkg\nVersion: 0.1.0\nTitle: Test Package\nDescription: Test package for unit tests.\nLicense: MIT\nDepends: R (>= 4.3), cli\nImports: digest (>= 0.6.37)\nLinkingTo: cpp11\nSuggests: testthat\nEnhances: covr\n",
        )
        .expect("description should parse");

        let dependencies =
            description_dependencies(&description).expect("dependencies should parse");

        assert_eq!(
            dependencies
                .iter()
                .map(|dependency| {
                    (
                        dependency.resolved.package.clone(),
                        dependency.resolved.kind.clone(),
                    )
                })
                .collect::<Vec<_>>(),
            vec![
                ("cli".to_string(), "Depends".to_string()),
                ("digest".to_string(), "Imports".to_string()),
                ("cpp11".to_string(), "LinkingTo".to_string()),
            ]
        );
    }

    #[test]
    fn extracts_cran_style_strict_constraints_from_registry_description() {
        let description = RDescription::from_str(
            "Package: Rdpack\nVersion: 2.6.6\nTitle: Test Package\nDescription: Test package for unit tests.\nLicense: MIT\nDepends: R (>= 2.15.0), methods\nImports: tools, utils, rbibutils (> 2.4)\n",
        )
        .expect("description should parse");

        let dependencies =
            description_dependencies(&description).expect("dependencies should parse");

        assert_eq!(
            dependencies
                .iter()
                .map(|dependency| {
                    (
                        dependency.resolved.package.clone(),
                        dependency.resolved.kind.clone(),
                        dependency.resolved.min_version.clone(),
                        dependency.resolved.max_version_exclusive.clone(),
                    )
                })
                .collect::<Vec<_>>(),
            vec![
                ("methods".to_string(), "Depends".to_string(), None, None),
                (
                    "rbibutils".to_string(),
                    "Imports".to_string(),
                    Some("2.4".to_string()),
                    None,
                ),
                ("tools".to_string(), "Imports".to_string(), None, None),
                ("utils".to_string(), "Imports".to_string(), None, None),
            ]
        );
    }

    #[test]
    fn resolves_transitives_to_highest_compatible_versions() {
        let provider = TestProvider {
            root_dependencies: vec![("dplyr".to_string(), VersionRange::full())],
            packages: BTreeMap::from([
                (
                    "dplyr".to_string(),
                    vec![TestPackageVersion {
                        version: parse_version("1.1.4").expect("version should parse"),
                        dependencies: vec![(
                            "rlang".to_string(),
                            parse_constraint_range(">= 1.1.0").expect("constraint should parse"),
                        )],
                    }],
                ),
                (
                    "rlang".to_string(),
                    vec![
                        TestPackageVersion {
                            version: parse_version("1.0.6").expect("version should parse"),
                            dependencies: vec![],
                        },
                        TestPackageVersion {
                            version: parse_version("1.1.1").expect("version should parse"),
                            dependencies: vec![],
                        },
                    ],
                ),
            ]),
        };

        let mut resolved = resolve(&provider, ROOT_PACKAGE.to_string(), root_version())
            .expect("resolution should work")
            .into_iter()
            .filter(|(package, _)| package != ROOT_PACKAGE)
            .collect::<Vec<_>>();
        resolved.sort_by(|left, right| left.0.cmp(&right.0));

        assert_eq!(
            resolved
                .into_iter()
                .map(|(package, version)| format!("{package}@{version}"))
                .collect::<Vec<_>>(),
            vec!["dplyr@1.1.4".to_string(), "rlang@1.1.1".to_string()]
        );
    }

    #[test]
    fn backtracks_to_find_consistent_solution() {
        let provider = TestProvider {
            root_dependencies: vec![
                ("pkg".to_string(), VersionRange::full()),
                (
                    "dep".to_string(),
                    parse_constraint_range(">= 1.0.0").expect("constraint should parse"),
                ),
            ],
            packages: BTreeMap::from([
                (
                    "pkg".to_string(),
                    vec![
                        TestPackageVersion {
                            version: parse_version("1.0.0").expect("version should parse"),
                            dependencies: vec![(
                                "dep".to_string(),
                                parse_constraint_range("< 2.0.0").expect("constraint should parse"),
                            )],
                        },
                        TestPackageVersion {
                            version: parse_version("2.0.0").expect("version should parse"),
                            dependencies: vec![(
                                "dep".to_string(),
                                parse_constraint_range(">= 2.0.0")
                                    .expect("constraint should parse"),
                            )],
                        },
                    ],
                ),
                (
                    "dep".to_string(),
                    vec![
                        TestPackageVersion {
                            version: parse_version("1.5.0").expect("version should parse"),
                            dependencies: vec![],
                        },
                        TestPackageVersion {
                            version: parse_version("2.0.0").expect("version should parse"),
                            dependencies: vec![],
                        },
                    ],
                ),
            ]),
        };

        let mut resolved = resolve(&provider, ROOT_PACKAGE.to_string(), root_version())
            .expect("resolution should work")
            .into_iter()
            .filter(|(package, _)| package != ROOT_PACKAGE)
            .collect::<Vec<_>>();
        resolved.sort_by(|left, right| left.0.cmp(&right.0));

        assert_eq!(
            resolved
                .into_iter()
                .map(|(package, version)| format!("{package}@{version}"))
                .collect::<Vec<_>>(),
            vec!["dep@2.0.0".to_string(), "pkg@2.0.0".to_string()]
        );
    }

    #[test]
    fn prefers_locked_version_when_it_satisfies_requested_range() {
        let repositories = RepositorySet::new(vec![]);
        let provider = RegistryDependencyProvider::new(
            &repositories,
            &[],
            BTreeMap::from([(
                "cli".to_string(),
                parse_version("3.6.4").expect("version should parse"),
            )]),
        )
        .expect("provider should build");

        provider.versions_by_package.borrow_mut().insert(
            "cli".to_string(),
            vec![
                VersionCandidate {
                    version: "3.6.4".to_string(),
                    repository_url: "https://example.test".to_string(),
                    source_url: "https://example.test/cli/3.6.4".to_string(),
                },
                VersionCandidate {
                    version: "3.6.5".to_string(),
                    repository_url: "https://example.test".to_string(),
                    source_url: "https://example.test/cli/3.6.5".to_string(),
                },
            ],
        );

        let chosen = provider
            .choose_version(
                &"cli".to_string(),
                &parse_constraint_range(">= 3.6.0, < 4.0.0").expect("constraint should parse"),
            )
            .expect("selection should succeed")
            .expect("selection should exist");

        assert_eq!(chosen.to_string(), "3.6.4");
    }

    #[test]
    fn resolves_highest_compatible_version_across_repositories() {
        let mut first = Server::new();
        let first_versions = first
            .mock("GET", "/packages/pkg/versions")
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(format!(
                r#"{{"package":"pkg","versions":[{{"version":"1.0.0","sourceUrl":"{}/packages/pkg/versions/1.0.0/source"}}]}}"#,
                first.url()
            ))
            .expect(1)
            .create();
        let mut second = Server::new();
        let second_versions = second
            .mock("GET", "/packages/pkg/versions")
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(format!(
                r#"{{"package":"pkg","versions":[{{"version":"2.0.0","sourceUrl":"{}/packages/pkg/versions/2.0.0/source"}}]}}"#,
                second.url()
            ))
            .expect(1)
            .create();
        let second_description = second
            .mock("GET", "/packages/pkg/versions/2.0.0/description")
            .with_status(200)
            .with_body("Package: pkg\nVersion: 2.0.0\n")
            .expect(1)
            .create();
        let repositories = RepositorySet::new(vec![
            RepositorySource::new(first.url()),
            RepositorySource::new(second.url()),
        ]);

        let resolved = resolve_from_registry(
            &repositories,
            &[ResolutionRoot {
                name: "pkg".to_string(),
                constraint: "*".to_string(),
            }],
            &BTreeMap::new(),
        )
        .expect("resolution should succeed");

        assert_eq!(resolved[0].version, "2.0.0");
        assert_eq!(
            resolved[0].source_url,
            format!("{}/packages/pkg/versions/2.0.0/source", second.url())
        );
        first_versions.assert();
        second_versions.assert();
        second_description.assert();
    }

    #[test]
    fn keeps_preferred_locked_version_from_enabled_repositories() {
        let mut server = Server::new();
        let versions = server
            .mock("GET", "/packages/pkg/versions")
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(format!(
                r#"{{"package":"pkg","versions":[{{"version":"1.0.0","sourceUrl":"{0}/packages/pkg/versions/1.0.0/source"}},{{"version":"2.0.0","sourceUrl":"{0}/packages/pkg/versions/2.0.0/source"}}]}}"#,
                server.url()
            ))
            .expect(1)
            .create();
        let description = server
            .mock("GET", "/packages/pkg/versions/1.0.0/description")
            .with_status(200)
            .with_body("Package: pkg\nVersion: 1.0.0\n")
            .expect(1)
            .create();
        let repositories = RepositorySet::new(vec![RepositorySource::new(server.url())]);
        let preferred = BTreeMap::from([("pkg".to_string(), "1.0.0".to_string())]);

        let resolved = resolve_from_registry(
            &repositories,
            &[ResolutionRoot {
                name: "pkg".to_string(),
                constraint: "*".to_string(),
            }],
            &preferred,
        )
        .expect("resolution should succeed");

        assert_eq!(resolved[0].version, "1.0.0");
        assert_eq!(
            resolved[0].source_url,
            format!("{}/packages/pkg/versions/1.0.0/source", server.url())
        );
        versions.assert();
        description.assert();
    }
}
