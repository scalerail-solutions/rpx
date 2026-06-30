use std::{
    cell::RefCell,
    cmp::{Ordering, Reverse},
    collections::{BTreeMap, BTreeSet},
    error::Error,
    fmt,
    hash::{Hash, Hasher},
    str::FromStr,
};

use pubgrub::{
    Dependencies, DependencyConstraints, DependencyProvider, PackageResolutionStatistics, Ranges,
    resolve,
};
use r_description::{Version, VersionConstraint};

use crate::{
    description::{DescriptionDependency, RDescription},
    r_version::{compare_version_components, r_version_components},
    registry::{RegistryClient, ResolutionRoot, is_not_found_error as is_registry_not_found_error},
    repository::RepositorySet,
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
type VersionRange = Ranges<RPackageVersion>;

#[derive(Debug, Clone, Eq)]
struct RPackageVersion {
    raw: String,
    components: Vec<u32>,
}

impl PartialEq for RPackageVersion {
    fn eq(&self, other: &Self) -> bool {
        self.components == other.components
    }
}

impl Hash for RPackageVersion {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.components.hash(state);
    }
}

impl fmt::Display for RPackageVersion {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.raw)
    }
}

impl FromStr for RPackageVersion {
    type Err = String;

    fn from_str(version: &str) -> Result<Self, Self::Err> {
        let components = r_version_components(version)?;

        Ok(Self {
            raw: version.to_string(),
            components,
        })
    }
}

impl Ord for RPackageVersion {
    fn cmp(&self, other: &Self) -> Ordering {
        compare_version_components(&self.components, &other.components)
    }
}

impl PartialOrd for RPackageVersion {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

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
struct RegistryPackageDependency {
    range: VersionRange,
    resolved: ResolvedDependency,
}

#[derive(Debug, Clone)]
struct PackageMetadata {
    version: String,
    source_url: String,
    dependencies: Vec<RegistryPackageDependency>,
    system_requirements: Option<String>,
}

#[derive(Debug, Clone)]
struct VersionCandidate {
    version: String,
    repository_url: String,
    source_url: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ResolverError(String);

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

#[derive(Debug, Clone)]
struct PackageDependency {
    package: String,
    range: Ranges<Version>,
}

#[derive(Debug, Clone)]
struct PackageDescription {
    dependencies: Vec<PackageDependency>,
}

trait PackageRepository: std::fmt::Debug {
    fn package_list(&self) -> Result<Vec<String>, ResolverError>;

    fn package_versions(&self, package: &str) -> Result<Vec<Version>, ResolverError>;

    fn package_description(
        &self,
        package: &str,
        version: &Version,
    ) -> Result<Option<PackageDescription>, ResolverError>;
}

#[derive(Debug, Default)]
struct NoopPackageRepository;

impl PackageRepository for NoopPackageRepository {
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
    ) -> Result<Option<PackageDescription>, ResolverError> {
        Ok(None)
    }
}

#[derive(Debug)]
struct RrepoPackageRepository {
    base_url: String,
}

impl RrepoPackageRepository {
    fn new(base_url: impl Into<String>) -> Self {
        Self {
            base_url: base_url.into(),
        }
    }
}

impl PackageRepository for RrepoPackageRepository {
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
    ) -> Result<Option<PackageDescription>, ResolverError> {
        let client = RegistryClient::new(&self.base_url);
        let version = version.to_string();
        let description = match client.fetch_description_with_retry(package, &version) {
            Ok(description) => description,
            Err(error) if is_registry_not_found_error(&error) => return Ok(None),
            Err(error) => return Err(ResolverError(error)),
        };
        let description = RDescription::from_str(&description).map_err(ResolverError)?;
        Ok(Some(package_description_from_r_description(&description)?))
    }
}

fn package_description_from_r_description(
    description: &RDescription,
) -> Result<PackageDescription, ResolverError> {
    let mut dependencies = Vec::new();

    dependencies.extend(package_dependencies_from_relations(&description.depends)?);
    dependencies.extend(package_dependencies_from_relations(&description.imports)?);
    dependencies.extend(package_dependencies_from_relations(
        &description.linking_to,
    )?);

    Ok(PackageDescription { dependencies })
}

fn package_dependencies_from_relations(
    relations: &BTreeSet<DescriptionDependency>,
) -> Result<Vec<PackageDependency>, ResolverError> {
    relations
        .iter()
        .filter(|relation| relation.name != "R")
        .map(package_dependency_from_relation)
        .collect()
}

fn package_dependency_from_relation(
    relation: &DescriptionDependency,
) -> Result<PackageDependency, ResolverError> {
    Ok(PackageDependency {
        package: relation.name.clone(),
        range: r_description_range_from_relation(relation),
    })
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
    repositories: Vec<Box<dyn PackageRepository>>,
    root_dependencies: Vec<PackageDependency>,
    preferred_versions_by_package: BTreeMap<String, Version>,
}

impl RDependencyProvider {
    fn new(
        repositories: Vec<Box<dyn PackageRepository>>,
        root_dependencies: Vec<PackageDependency>,
        preferred_versions_by_package: BTreeMap<String, Version>,
    ) -> Self {
        Self {
            repositories,
            root_dependencies,
            preferred_versions_by_package,
        }
    }

    fn package_list(&self) -> Result<Vec<String>, ResolverError> {
        let mut packages = BTreeSet::new();

        for repository in &self.repositories {
            packages.extend(repository.package_list()?);
        }

        Ok(packages.into_iter().collect())
    }

    fn package_versions(&self, package: &str) -> Result<Vec<Version>, ResolverError> {
        let mut versions = BTreeSet::new();

        for repository in &self.repositories {
            versions.extend(repository.package_versions(package)?);
        }

        Ok(versions.into_iter().collect())
    }

    fn package_description(
        &self,
        package: &str,
        version: &Version,
    ) -> Result<Option<PackageDescription>, ResolverError> {
        for repository in &self.repositories {
            if let Some(description) = repository.package_description(package, version)? {
                return Ok(Some(description));
            }
        }

        Ok(None)
    }
}

impl DependencyProvider for RDependencyProvider {
    type P = String;
    type V = Version;
    type VS = Ranges<Version>;
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
            .package_versions(package)
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

        let versions = self.package_versions(package)?;
        if let Some(preferred_version) = self.preferred_versions_by_package.get(package)
            && range.contains(preferred_version)
            && versions.contains(preferred_version)
        {
            return Ok(Some(preferred_version.clone()));
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
                    .map(|dependency| (dependency.package.clone(), dependency.range.clone()))
                    .collect::<DependencyConstraints<_, _>>(),
            ));
        }

        let Some(description) = self.package_description(package, version)? else {
            return Ok(Dependencies::Unavailable(format!(
                "package DESCRIPTION not found for {package}@{version}"
            )));
        };

        Ok(Dependencies::Available(
            description
                .dependencies
                .into_iter()
                .filter(|dependency| !is_base_package(&dependency.package))
                .map(|dependency| (dependency.package, dependency.range))
                .collect::<DependencyConstraints<_, _>>(),
        ))
    }
}

#[allow(dead_code)]
fn r_dependency_root_version() -> Version {
    "0.0.0".parse().expect("root version should parse")
}

pub fn is_base_package(package: &str) -> bool {
    BASE_PACKAGES.contains(&package)
}

pub fn resolve_from_registry(
    repositories: &RepositorySet,
    roots: &[ResolutionRoot],
    preferred_versions_by_package: &BTreeMap<String, String>,
) -> Result<Vec<ResolvedPackage>, String> {
    if roots.is_empty() {
        return Ok(Vec::new());
    }

    let preferred_versions_by_package = preferred_versions_by_package
        .iter()
        .map(|(package, version)| Ok((package.clone(), parse_version(version)?)))
        .collect::<Result<BTreeMap<_, _>, ResolverError>>()
        .map_err(|error| error.to_string())?;
    let provider = RDependencyProvider::new(Vec::new(), Vec::new(), BTreeMap::new());
    let mut selected = match resolve(&provider, ROOT_PACKAGE.to_string(), r_dependency_root_version()) {
        Ok(selected) => selected
            .into_iter()
            .filter(|(package, _)| package != ROOT_PACKAGE)
            .collect::<Vec<_>>(),
        Err(error) => {
            return Err(error.to_string());
        }
    };
    selected.sort_by(|left, right| left.0.cmp(&right.0));
    let mut resolved = selected
        .into_iter()
        .map(|(package, version)| provider.resolved_package(&package, &version))
        .collect::<Result<Vec<_>, _>>()
        .map_err(|error: ResolverError| error.to_string())?;
    resolved.sort_by(|left, right| left.name.cmp(&right.name));
    Ok(resolved)
}

fn description_dependencies(
    description: &RDescription,
) -> Result<Vec<RegistryPackageDependency>, ResolverError> {
    let mut dependencies = Vec::new();

    dependencies.extend(relations_to_dependencies("Depends", &description.depends)?);
    dependencies.extend(relations_to_dependencies("Imports", &description.imports)?);
    dependencies.extend(relations_to_dependencies(
        "LinkingTo",
        &description.linking_to,
    )?);

    Ok(dependencies)
}

fn relations_to_dependencies(
    kind: &str,
    relations: &BTreeSet<DescriptionDependency>,
) -> Result<Vec<RegistryPackageDependency>, ResolverError> {
    relations
        .iter()
        .filter(|relation| relation.name != "R")
        .map(|relation| relation_dependency(kind, relation))
        .collect()
}

fn relation_dependency(
    kind: &str,
    relation: &DescriptionDependency,
) -> Result<RegistryPackageDependency, ResolverError> {
    let (min_version, max_version_exclusive) = relation_bounds(relation);
    Ok(RegistryPackageDependency {
        range: range_from_relation(relation)?,
        resolved: ResolvedDependency {
            package: relation.name.clone(),
            kind: kind.to_string(),
            min_version,
            max_version_exclusive,
        },
    })
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

fn range_from_relation(relation: &DescriptionDependency) -> Result<VersionRange, ResolverError> {
    let Some((operator, version)) = relation.version.as_ref() else {
        return Ok(VersionRange::full());
    };
    let version = parse_version(&version.to_string())?;

    Ok(match operator {
        VersionConstraint::Equal => VersionRange::singleton(version.clone()),
        VersionConstraint::GreaterThan => VersionRange::strictly_higher_than(version.clone()),
        VersionConstraint::GreaterThanEqual => VersionRange::higher_than(version.clone()),
        VersionConstraint::LessThan => VersionRange::strictly_lower_than(version.clone()),
        VersionConstraint::LessThanEqual => VersionRange::lower_than(version.clone()),
    })
}

fn parse_constraint_range(constraint: &str) -> Result<VersionRange, ResolverError> {
    let constraint = constraint.trim();
    if constraint.is_empty() || constraint == "*" {
        return Ok(VersionRange::full());
    }

    constraint
        .trim_start_matches('(')
        .trim_end_matches(')')
        .split(',')
        .map(str::trim)
        .filter(|part| !part.is_empty())
        .try_fold(VersionRange::full(), |range, part| {
            Ok(range.intersection(&range_from_constraint_part(part)?))
        })
}

fn range_from_constraint_part(constraint: &str) -> Result<VersionRange, ResolverError> {
    let (operator, version) = parse_constraint_part(constraint);
    let version = parse_version(version)?;

    Ok(match operator {
        ParsedConstraint::Eq => VersionRange::singleton(version),
        ParsedConstraint::Gt => VersionRange::strictly_higher_than(version),
        ParsedConstraint::Gte => VersionRange::higher_than(version),
        ParsedConstraint::Lt => VersionRange::strictly_lower_than(version),
        ParsedConstraint::Lte => VersionRange::lower_than(version),
    })
}

fn parse_constraint_part(constraint: &str) -> (ParsedConstraint, &str) {
    for (prefix, operator) in [
        (">=", ParsedConstraint::Gte),
        ("<=", ParsedConstraint::Lte),
        (">>", ParsedConstraint::Gt),
        ("<<", ParsedConstraint::Lt),
        ("==", ParsedConstraint::Eq),
        (">", ParsedConstraint::Gt),
        ("<", ParsedConstraint::Lt),
        ("=", ParsedConstraint::Eq),
    ] {
        if let Some(version) = constraint.strip_prefix(prefix) {
            return (operator, version.trim());
        }
    }

    (ParsedConstraint::Eq, constraint.trim())
}

fn parse_version(version: &str) -> Result<RPackageVersion, ResolverError> {
    version
        .parse::<RPackageVersion>()
        .map_err(|error| ResolverError(format!("invalid version {version}: {error}")))
}

fn root_version() -> RPackageVersion {
    RPackageVersion::from_str("0.0.0").expect("root version should parse")
}

fn is_not_found_error(error: &str) -> bool {
    error.starts_with("unexpected registry response (404")
}

#[derive(Debug, Clone, Copy)]
enum ParsedConstraint {
    Eq,
    Gt,
    Gte,
    Lt,
    Lte,
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
