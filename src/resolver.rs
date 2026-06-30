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
use r_description::{Version, VersionConstraint};

use crate::{
    description::{DescriptionDependency, RDescription},
    registry::{RegistryClient, ResolutionRoot, is_not_found_error as is_registry_not_found_error},
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
    fn base_url(&self) -> &str {
        ""
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
    ) -> Result<Option<PackageDescription>, ResolverError> {
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
    repositories: Vec<Rc<dyn PackageRepository>>,
    root_dependencies: Vec<PackageDependency>,
    preferred_versions_by_package: BTreeMap<String, Version>,
}

impl RDependencyProvider {
    fn new(
        repositories: Vec<Rc<dyn PackageRepository>>,
        root_dependencies: Vec<PackageDependency>,
        preferred_versions_by_package: BTreeMap<String, Version>,
    ) -> Self {
        Self {
            repositories,
            root_dependencies,
            preferred_versions_by_package,
        }
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
        let matches =
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
                })
                .ok()
                .map_or(usize::MAX, |mut versions| {
                    versions.sort();
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

        let Some(description) = version
            .repository
            .package_description(package, &version.version)?
        else {
            return Ok(Dependencies::Unavailable(format!(
                "package DESCRIPTION not found for {package}@{version}"
            )));
        };

        Ok(Dependencies::Available(
            description
                .dependencies
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
    roots: &[ResolutionRoot],
    preferred_versions_by_package: &BTreeMap<String, String>,
) -> Result<Vec<ResolvedPackage>, String> {
    if roots.is_empty() {
        return Ok(Vec::new());
    }

    let preferred_versions_by_package = preferred_versions_by_package
        .iter()
        .map(|(package, version)| {
            Ok((
                package.clone(),
                version.parse::<Version>().map_err(|error| {
                    ResolverError(format!("invalid version {version}: {error}"))
                })?,
            ))
        })
        .collect::<Result<BTreeMap<_, _>, ResolverError>>()
        .map_err(|error| error.to_string())?;
    let root_dependencies = roots
        .iter()
        .filter(|root| !is_base_package(&root.name))
        .map(|root| PackageDependency {
            package: root.name.clone(),
            range: Ranges::full(),
        })
        .collect();
    let provider = RDependencyProvider::new(
        repositories,
        root_dependencies,
        preferred_versions_by_package,
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
            return Err(error.to_string());
        }
    };
    selected.sort_by(|left, right| left.0.cmp(&right.0));
    let mut resolved = selected
        .into_iter()
        .map(|(package, version)| ResolvedPackage {
            name: package,
            version: version.version.to_string(),
            source_url: String::new(),
            dependencies: Vec::new(),
            system_requirements: None,
        })
        .collect::<Vec<_>>();
    resolved.sort_by(|left, right| left.name.cmp(&right.name));
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
