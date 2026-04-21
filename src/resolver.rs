use std::{
    cell::RefCell,
    cmp::Reverse,
    collections::BTreeMap,
    error::Error,
    fmt,
    str::FromStr,
};

use pubgrub::{
    Dependencies, DependencyConstraints, DependencyProvider, PackageResolutionStatistics, Ranges,
    resolve,
};
use r_description::{
    Version, VersionConstraint,
    lossy::{RDescription, Relation, Relations},
};

use crate::registry::{ClosureRoot, RegistryClient, VersionSummary};

const ROOT_PACKAGE: &str = "__rpx_root__";
type VersionRange = Ranges<Version>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedPackage {
    pub name: String,
    pub version: String,
    pub source_url: String,
    pub dependencies: Vec<ResolvedDependency>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedDependency {
    pub package: String,
    pub kind: String,
    pub min_version: Option<String>,
    pub max_version_exclusive: Option<String>,
}

#[derive(Debug, Clone)]
struct PackageDependency {
    range: VersionRange,
    resolved: ResolvedDependency,
}

#[derive(Debug, Clone)]
struct PackageMetadata {
    version: String,
    source_url: String,
    dependencies: Vec<PackageDependency>,
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
struct RegistryDependencyProvider<'a> {
    client: &'a RegistryClient,
    root_dependencies: Vec<PackageDependency>,
    versions_by_package: RefCell<BTreeMap<String, Vec<VersionSummary>>>,
    metadata_by_package_version: RefCell<BTreeMap<(String, String), PackageMetadata>>,
}

impl<'a> RegistryDependencyProvider<'a> {
    fn new(client: &'a RegistryClient, roots: &[ClosureRoot]) -> Result<Self, ResolverError> {
        let root_dependencies = roots
            .iter()
            .map(|root| {
                let range = parse_constraint_range(&root.constraint)?;
                Ok(PackageDependency {
                    range,
                    resolved: ResolvedDependency {
                        package: root.name.clone(),
                        kind: "Imports".to_string(),
                        min_version: None,
                        max_version_exclusive: None,
                    },
                })
            })
            .collect::<Result<Vec<_>, ResolverError>>()?;

        Ok(Self {
            client,
            root_dependencies,
            versions_by_package: RefCell::new(BTreeMap::new()),
            metadata_by_package_version: RefCell::new(BTreeMap::new()),
        })
    }

    fn package_versions(&self, package: &str) -> Result<Vec<VersionSummary>, ResolverError> {
        if let Some(versions) = self.versions_by_package.borrow().get(package) {
            return Ok(versions.clone());
        }

        let mut versions = self.client.fetch_package_versions_with_retry(package)?.versions;
        versions.sort_by(|left, right| {
            let left_version = parse_version(&left.version).expect("registry version should parse");
            let right_version =
                parse_version(&right.version).expect("registry version should parse");
            left_version.cmp(&right_version)
        });
        self.versions_by_package
            .borrow_mut()
            .insert(package.to_string(), versions.clone());
        Ok(versions)
    }

    fn registry_contains_package(&self, package: &str) -> Result<bool, ResolverError> {
        match self.package_versions(package) {
            Ok(versions) => Ok(!versions.is_empty()),
            Err(error) if is_not_found_error(&error.0) => Ok(false),
            Err(error) => Err(error),
        }
    }

    fn package_metadata(&self, package: &str, version: &Version) -> Result<PackageMetadata, ResolverError> {
        let key = (package.to_string(), version.to_string());
        if let Some(metadata) = self.metadata_by_package_version.borrow().get(&key) {
            return Ok(metadata.clone());
        }

        let version_entry = self
            .package_versions(package)?
            .into_iter()
            .find(|entry| parse_version(&entry.version).ok().as_ref() == Some(version))
            .ok_or_else(|| ResolverError(format!("version {version} missing from registry for {package}")))?;
        let description = self
            .client
            .fetch_description_with_retry(package, &version_entry.version)?;
        let description = RDescription::from_str(&description)
            .map_err(|error| ResolverError(format!("failed to parse DESCRIPTION for {package}@{}: {error}", version_entry.version)))?;
        let metadata = PackageMetadata {
            version: version_entry.version.clone(),
            source_url: version_entry.source_url,
            dependencies: description_dependencies(&description)?,
        };
        self.metadata_by_package_version
            .borrow_mut()
            .insert(key, metadata.clone());
        Ok(metadata)
    }

    fn resolved_package(&self, package: &str, version: &Version) -> Result<ResolvedPackage, ResolverError> {
        let metadata = self.package_metadata(package, version)?;
        Ok(ResolvedPackage {
            name: package.to_string(),
            version: metadata.version,
            source_url: metadata.source_url,
            dependencies: metadata
                .dependencies
                .into_iter()
                .map(|dependency| dependency.resolved)
                .collect(),
        })
    }
}

impl DependencyProvider for RegistryDependencyProvider<'_> {
    type P = String;
    type V = Version;
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
        let matches = self
            .package_versions(package)
            .ok()
            .map(|versions| {
                versions
                    .iter()
                    .filter_map(|version| parse_version(&version.version).ok())
                    .filter(|version| range.contains(version))
                    .count()
            })
            .unwrap_or(usize::MAX);

        (
            package_conflicts_counts.conflict_count(),
            Reverse(matches),
        )
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
            .package_versions(package)?
            .into_iter()
            .rev()
            .filter_map(|version| parse_version(&version.version).ok())
            .find(|version| range.contains(version)))
    }

    fn get_dependencies(
        &self,
        package: &Self::P,
        version: &Self::V,
    ) -> Result<Dependencies<Self::P, Self::VS, Self::M>, Self::Err> {
        if package == ROOT_PACKAGE {
            if *version != root_version() {
                return Ok(Dependencies::Unavailable(format!(
                    "unsupported root version: {version}"
                )));
            }

            return Ok(Dependencies::Available(
                self.root_dependencies
                    .iter()
                    .map(|dependency| {
                        (
                            dependency.resolved.package.clone(),
                            dependency.range.clone(),
                        )
                    })
                    .collect::<DependencyConstraints<_, _>>(),
            ));
        }

        let metadata = self.package_metadata(package, version)?;
        Ok(Dependencies::Available(
            metadata
                .dependencies
                .into_iter()
                .filter_map(|dependency| match self.registry_contains_package(&dependency.resolved.package) {
                    Ok(true) => Some(Ok((dependency.resolved.package.clone(), dependency.range))),
                    Ok(false) => None,
                    Err(error) => Some(Err(error)),
                })
                .collect::<Result<Vec<_>, _>>()?
                .into_iter()
                .collect::<DependencyConstraints<_, _>>(),
        ))
    }
}

pub fn resolve_from_registry(
    client: &RegistryClient,
    roots: &[ClosureRoot],
) -> Result<Vec<ResolvedPackage>, String> {
    let provider = RegistryDependencyProvider::new(client, roots).map_err(|error| error.to_string())?;
    let selected = solve_selected_versions(&provider)?;
    let mut resolved = selected
        .into_iter()
        .map(|(package, version)| provider.resolved_package(&package, &version))
        .collect::<Result<Vec<_>, _>>()
        .map_err(|error: ResolverError| error.to_string())?;
    resolved.sort_by(|left, right| left.name.cmp(&right.name));
    Ok(resolved)
}

fn solve_selected_versions<DP>(provider: &DP) -> Result<Vec<(String, Version)>, String>
where
    DP: DependencyProvider<P = String, V = Version, VS = VersionRange, M = String>,
    DP::Err: fmt::Display,
{
    let selected = resolve(provider, ROOT_PACKAGE.to_string(), root_version())
        .map_err(|error| error.to_string())?;
    let mut selected = selected
        .into_iter()
        .filter(|(package, _)| package != ROOT_PACKAGE)
        .collect::<Vec<_>>();
    selected.sort_by(|left, right| left.0.cmp(&right.0));
    Ok(selected)
}

fn description_dependencies(description: &RDescription) -> Result<Vec<PackageDependency>, ResolverError> {
    let mut dependencies = Vec::new();

    if let Some(depends) = &description.depends {
        dependencies.extend(relations_to_dependencies("Depends", depends)?);
    }

    if let Some(imports) = &description.imports {
        dependencies.extend(relations_to_dependencies("Imports", imports)?);
    }

    if let Some(linking_to) = &description.linking_to {
        dependencies.extend(relations_to_dependencies("LinkingTo", linking_to)?);
    }

    Ok(dependencies)
}

fn relations_to_dependencies(
    kind: &str,
    relations: &Relations,
) -> Result<Vec<PackageDependency>, ResolverError> {
    relations
        .iter()
        .filter(|relation| relation.name != "R")
        .map(|relation| relation_dependency(kind, relation))
        .collect()
}

fn relation_dependency(kind: &str, relation: &Relation) -> Result<PackageDependency, ResolverError> {
    let (min_version, max_version_exclusive) = relation_bounds(relation);
    Ok(PackageDependency {
        range: range_from_relation(relation),
        resolved: ResolvedDependency {
            package: relation.name.clone(),
            kind: kind.to_string(),
            min_version,
            max_version_exclusive,
        },
    })
}

fn relation_bounds(relation: &Relation) -> (Option<String>, Option<String>) {
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

fn range_from_relation(relation: &Relation) -> VersionRange {
    let Some((operator, version)) = relation.version.as_ref() else {
        return VersionRange::full();
    };

    match operator {
        VersionConstraint::Equal => VersionRange::singleton(version.clone()),
        VersionConstraint::GreaterThan => VersionRange::strictly_higher_than(version.clone()),
        VersionConstraint::GreaterThanEqual => VersionRange::higher_than(version.clone()),
        VersionConstraint::LessThan => VersionRange::strictly_lower_than(version.clone()),
        VersionConstraint::LessThanEqual => VersionRange::lower_than(version.clone()),
    }
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

fn parse_version(version: &str) -> Result<Version, ResolverError> {
    version
        .parse::<Version>()
        .map_err(|error| ResolverError(format!("invalid version {version}: {error}")))
}

fn root_version() -> Version {
    Version::from_str("0.0.0").expect("root version should parse")
}

fn is_not_found_error(error: &str) -> bool {
    error.starts_with("unexpected registry response (404)")
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

    #[derive(Debug, Clone)]
    struct TestPackageVersion {
        version: Version,
        dependencies: Vec<(String, VersionRange)>,
    }

    #[derive(Debug, Clone)]
    struct TestProvider {
        root_dependencies: Vec<(String, VersionRange)>,
        packages: BTreeMap<String, Vec<TestPackageVersion>>,
    }

    impl DependencyProvider for TestProvider {
        type P = String;
        type V = Version;
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
            let matches = self
                .packages
                .get(package)
                .map(|versions| versions.iter().filter(|version| range.contains(&version.version)).count())
                .unwrap_or(usize::MAX);
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
                .and_then(|versions| versions.iter().find(|candidate| &candidate.version == version))
                .map(|version| version.dependencies.clone())
                .unwrap_or_default();
            Ok(Dependencies::Available(dependencies.into_iter().collect()))
        }
    }

    #[test]
    fn extracts_supported_description_dependency_kinds() {
        let description = RDescription::from_str(
            "Package: testpkg\nVersion: 0.1.0\nTitle: Test Package\nDescription: Test package for unit tests.\nLicense: MIT\nDepends: R (>= 4.3), cli\nImports: digest (>= 0.6.37)\nLinkingTo: cpp11\nSuggests: testthat\nEnhances: covr\n",
        )
        .expect("description should parse");

        let dependencies = description_dependencies(&description).expect("dependencies should parse");

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

        let resolved = solve_selected_versions(&provider).expect("resolution should work");

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
                                parse_constraint_range(">= 2.0.0").expect("constraint should parse"),
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

        let resolved = solve_selected_versions(&provider).expect("resolution should work");

        assert_eq!(
            resolved
                .into_iter()
                .map(|(package, version)| format!("{package}@{version}"))
                .collect::<Vec<_>>(),
            vec!["dep@2.0.0".to_string(), "pkg@2.0.0".to_string()]
        );
    }
}
