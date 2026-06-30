use std::{
    cmp::{Ordering, Reverse},
    collections::{BTreeMap, BTreeSet},
    error::Error,
    fmt,
    rc::Rc,
    str::FromStr,
};

use pubgrub::{
    Dependencies, DependencyConstraints, DependencyProvider, PackageResolutionStatistics, Ranges,
    VersionSet, resolve,
};
use r_description::{Version, VersionConstraint, lossy};

use crate::{
    description::{DescriptionDependency, RDescription},
    repository::{
        CranArchiveSupport, fetch_cran_like_archive_listing, fetch_cran_like_description,
        fetch_cran_like_packages_index, fetch_rrepo_description, fetch_rrepo_package_list,
        fetch_rrepo_package_versions, normalize_repository_url, rrepo_source_url,
    },
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

    #[allow(dead_code)]
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
        fetch_rrepo_package_list(&self.base_url).map_err(ResolverError)
    }

    fn package_versions(&self, package: &str) -> Result<Vec<Version>, ResolverError> {
        fetch_rrepo_package_versions(&self.base_url, package).map_err(ResolverError)
    }

    fn package_description(
        &self,
        package: &str,
        version: &Version,
    ) -> Result<Option<Vec<PackageDependency>>, ResolverError> {
        let version = version.to_string();
        let Some(description) =
            fetch_rrepo_description(&self.base_url, package, &version).map_err(ResolverError)?
        else {
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
        let Some(description) =
            fetch_rrepo_description(&self.base_url, package, &version).map_err(ResolverError)?
        else {
            return Ok(None);
        };
        let description = RDescription::from_str(&description).map_err(ResolverError)?;

        Ok(Some(ResolvedPackage {
            name: package.to_string(),
            version: version.clone(),
            source_url: rrepo_source_url(&self.base_url, package, &version),
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
        Ok(fetch_cran_like_packages_index(&self.base_url)
            .map_err(ResolverError)?
            .records
            .into_iter()
            .map(|record| record.package)
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect())
    }

    fn package_versions(&self, package: &str) -> Result<Vec<Version>, ResolverError> {
        let mut versions = fetch_cran_like_packages_index(&self.base_url)
            .map_err(ResolverError)?
            .records
            .into_iter()
            .filter(|record| record.package == package)
            .map(|record| record.version)
            .collect::<BTreeSet<_>>();

        if self.archive_support != Some(CranArchiveSupport::Unavailable) {
            versions.extend(
                fetch_cran_like_archive_listing(&self.base_url, package)
                    .map_err(ResolverError)?
                    .records
                    .into_iter()
                    .filter(|record| record.package == package)
                    .map(|record| record.version.parse::<Version>().map_err(ResolverError))
                    .collect::<Result<BTreeSet<_>, _>>()?,
            );
        }

        Ok(versions.into_iter().collect())
    }

    fn package_description(
        &self,
        package: &str,
        version: &Version,
    ) -> Result<Option<Vec<PackageDependency>>, ResolverError> {
        let index = fetch_cran_like_packages_index(&self.base_url).map_err(ResolverError)?;
        if let Some(record) = index
            .records
            .into_iter()
            .find(|record| record.package == package && &record.version == version)
        {
            return Ok(Some(package_dependencies_from_cran_like_record(record)));
        }

        let version = version.to_string();
        let Some(description) = fetch_cran_like_description(&self.base_url, package, &version)
            .map_err(ResolverError)?
        else {
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
        let Some(description) = fetch_cran_like_description(&self.base_url, package, &version)
            .map_err(ResolverError)?
        else {
            return Ok(None);
        };
        let description = RDescription::from_str(&description).map_err(ResolverError)?;

        Ok(Some(ResolvedPackage {
            name: package.to_string(),
            version: version.clone(),
            source_url: self.source_url(package, &version)?,
            dependencies: resolved_dependencies_from_r_description(&description),
            system_requirements: description.system_requirements,
        }))
    }
}

impl CranLikePackageRepository {
    fn source_url(&self, package: &str, version: &str) -> Result<String, ResolverError> {
        let is_current = fetch_cran_like_packages_index(&self.base_url)
            .map_err(ResolverError)?
            .records
            .into_iter()
            .any(|record| record.package == package && record.version.to_string() == version);

        if is_current {
            return Ok(self.current_tarball_url(package, version));
        }

        Ok(self.archive_tarball_url(package, version))
    }

    fn current_tarball_url(&self, package: &str, version: &str) -> String {
        format!("{}/src/contrib/{package}_{version}.tar.gz", self.base_url)
    }

    fn archive_tarball_url(&self, package: &str, version: &str) -> String {
        format!(
            "{}/src/contrib/Archive/{package}/{package}_{version}.tar.gz",
            self.base_url
        )
    }
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

fn package_dependencies_from_cran_like_record(
    record: crate::repository::CranLikePackageRecord,
) -> Vec<PackageDependency> {
    let mut dependencies = Vec::new();

    dependencies.extend(package_dependencies_from_lossy_relations(&record.depends));
    dependencies.extend(package_dependencies_from_lossy_relations(&record.imports));
    dependencies.extend(package_dependencies_from_lossy_relations(
        &record.linking_to,
    ));

    dependencies
}

fn package_dependencies_from_lossy_relations(
    relations: &lossy::Relations,
) -> Vec<PackageDependency> {
    relations
        .iter()
        .filter(|relation| relation.name != "R")
        .map(package_dependency_from_lossy_relation)
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

fn package_dependency_from_lossy_relation(relation: &lossy::Relation) -> PackageDependency {
    PackageDependency {
        package: relation.name.clone(),
        range: r_description_range_from_lossy_relation(relation),
    }
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

fn r_description_range_from_lossy_relation(relation: &lossy::Relation) -> Ranges<Version> {
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
