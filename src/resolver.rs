use futures_util::{StreamExt, stream};
use pubgrub::{
    Dependencies, DependencyConstraints, DependencyProvider, PackageResolutionStatistics, Ranges,
    resolve,
};
use r_description::{
    VersionConstraint,
    lossless::{RDescription, Relation, Version},
};
use std::{cmp::Reverse, collections::BTreeMap, error::Error, fmt, str::FromStr, sync::Arc};
use tracing::Instrument;
use tracing_indicatif::span_ext::IndicatifSpanExt;

use crate::{
    default_repository, http,
    repository::{PackageRepository, RepositoryType},
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
pub struct PackageVersion {
    version: Version,
    repository: Arc<PackageRepository>,
}

impl PackageVersion {
    pub fn new(version: Version, repository: Arc<PackageRepository>) -> Self {
        Self {
            version,
            repository,
        }
    }

    pub fn version(&self) -> &Version {
        &self.version
    }

    pub fn repository(&self) -> &Arc<PackageRepository> {
        &self.repository
    }

    pub fn source_url(&self, package: &str) -> String {
        let base_url = self.repository.base_url().to_string();
        let base_url = base_url.trim_end_matches('/');

        match self.repository.repo_type() {
            RepositoryType::Rrepo => format!(
                "{}/packages/{package}/versions/{}/source",
                base_url, self.version
            ),
            RepositoryType::Cran { .. } => format!(
                "{}/src/contrib/Archive/{package}/{package}_{}.tar.gz",
                base_url, self.version
            ),
        }
    }
}

impl PartialEq for PackageVersion {
    fn eq(&self, other: &Self) -> bool {
        self.version == other.version
    }
}

impl Eq for PackageVersion {}

impl PartialOrd for PackageVersion {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for PackageVersion {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.version.cmp(&other.version)
    }
}

impl std::hash::Hash for PackageVersion {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.version.hash(state);
    }
}
impl std::fmt::Display for PackageVersion {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} from {}", self.version, self.repository.base_url())
    }
}

#[derive(Debug)]
struct RDependencyProvider {
    client: http::HttpClient,
    repositories: Vec<PackageRepository>,
    root_dependencies: BTreeMap<String, Ranges<PackageVersion>>,
    preferred_versions: BTreeMap<String, PackageVersion>,
}

impl RDependencyProvider {
    fn new(
        client: http::HttpClient,
        repositories: Vec<PackageRepository>,
        root_dependencies: BTreeMap<String, Ranges<PackageVersion>>,
        preferred_versions: BTreeMap<String, PackageVersion>,
    ) -> Result<Self, ResolverError> {
        Ok(Self {
            client,
            repositories,
            root_dependencies,
            preferred_versions,
        })
    }

    fn prefetch_versions<I>(&self, packages: I)
    where
        I: IntoIterator<Item = String>,
    {
        let packages = packages.into_iter().collect::<Vec<_>>();

        if packages.is_empty() {
            return;
        }

        let repositories = self.repositories.clone();
        let client = self.client.clone();

        let parent_span = tracing::Span::current();

        tokio::runtime::Handle::current().spawn(
            async move {
                stream::iter(packages)
                    .for_each_concurrent(8, |package| {
                        let repositories = repositories.clone();
                        let client = client.clone();

                        async move {
                            let _ = futures_util::future::join_all(
                                repositories
                                    .iter()
                                    .map(|repository| repository.versions(&client, &package)),
                            )
                            .await;
                        }
                    })
                    .await;
            }
            .instrument(parent_span),
        );
    }
}

impl DependencyProvider for RDependencyProvider {
    type P = String;
    type V = PackageVersion;
    type VS = Ranges<PackageVersion>;
    type Priority = (u32, Reverse<usize>);
    type M = String;
    type Err = ResolverError;

    fn prioritize(
        &self,
        package: &Self::P,
        range: &Self::VS,
        package_conflicts_counts: &PackageResolutionStatistics,
    ) -> Self::Priority {
        let conflicts = package_conflicts_counts.conflict_count();

        let matches = tokio::runtime::Handle::current()
            .block_on(async {
                let results = futures_util::future::join_all(
                    self.repositories
                        .iter()
                        .map(|repository| repository.versions(&self.client, package)),
                )
                .await;

                results.into_iter().collect::<Result<Vec<_>, String>>().map(
                    |versions_by_repository| {
                        versions_by_repository
                            .into_iter()
                            .flatten()
                            .filter(|version| range.contains(version))
                            .count()
                    },
                )
            })
            .unwrap_or(usize::MAX);

        (conflicts, Reverse(matches))
    }

    fn choose_version(
        &self,
        package: &Self::P,
        range: &Self::VS,
    ) -> Result<Option<Self::V>, Self::Err> {
        if package == ROOT_PACKAGE {
            let root = tokio::runtime::Handle::current()
                .block_on(root_package_version())
                .map_err(ResolverError::from)?;

            return Ok(range.contains(&root).then_some(root));
        }

        if let Some(preferred) = self.preferred_versions.get(package) {
            if range.contains(preferred) {
                return Ok(Some(preferred.clone()));
            }
        }

        let versions_by_repository = tokio::runtime::Handle::current()
            .block_on(async {
                futures_util::future::join_all(
                    self.repositories
                        .iter()
                        .map(|repository| repository.versions(&self.client, package)),
                )
                .await
                .into_iter()
                .collect::<Result<Vec<_>, String>>()
            })
            .map_err(ResolverError::from)?;

        Ok(versions_by_repository
            .into_iter()
            .enumerate()
            .flat_map(|(repository_index, versions)| {
                versions
                    .into_iter()
                    .filter(move |version| range.contains(version))
                    .map(move |version| (version, repository_index))
            })
            .max_by(|(left_version, left_repo), (right_version, right_repo)| {
                left_version
                    .cmp(right_version)
                    // For equal versions, prefer lower repository index.
                    // `max_by` wants the preferred item to compare greater,
                    // so reverse the repo-index comparison.
                    .then_with(|| right_repo.cmp(left_repo))
            })
            .map(|(version, _repository_index)| version))
    }

    fn get_dependencies(
        &self,
        package: &Self::P,
        version: &Self::V,
    ) -> Result<Dependencies<Self::P, Self::VS, Self::M>, Self::Err> {
        if package == ROOT_PACKAGE {
            let constraints = self
                .root_dependencies
                .iter()
                .map(|(package, range)| (package.clone(), range.clone()))
                .collect::<DependencyConstraints<_, _>>();

            self.prefetch_versions(constraints.keys().cloned());

            return Ok(Dependencies::Available(constraints));
        }

        if is_base_package(package) {
            return Ok(Dependencies::Available(DependencyConstraints::default()));
        }

        let description = tokio::runtime::Handle::current()
            .block_on(
                version
                    .repository
                    .description(&self.client, package, &version.version),
            )
            .map_err(|err| {
                ResolverError(format!(
                    "failed to parse dependency metadata for {package} {} from {}: {err}",
                    version.version,
                    version.repository.base_url(),
                ))
            })?;

        let constraints =
            dependency_constraints_from_description(Arc::clone(&version.repository), &description)?;

        self.prefetch_versions(constraints.keys().cloned());

        Ok(Dependencies::Available(constraints))
    }
}

fn dependency_constraints_from_description(
    repository: Arc<PackageRepository>,
    description: &RDescription,
) -> Result<DependencyConstraints<String, Ranges<PackageVersion>>, ResolverError> {
    description
        .depends()
        .into_iter()
        .chain(description.imports())
        .chain(description.linking_to())
        .flat_map(|relations| relations.iter())
        .filter(|relation| relation.name() != "R")
        .filter(|relation| !is_base_package(&relation.name()))
        .map(|relation| {
            Ok((
                relation.name().to_string(),
                relation_package_range(Arc::clone(&repository), &relation)?,
            ))
        })
        .collect()
}

async fn root_package_version() -> Result<PackageVersion, ResolverError> {
    Ok(PackageVersion {
        version: Version::from_str("0.0.0").expect("root version should parse"),
        repository: Arc::new(default_repository().await.map_err(ResolverError::from)?),
    })
}

fn relation_package_range(
    repository: Arc<PackageRepository>,
    relation: &Relation,
) -> Result<Ranges<PackageVersion>, ResolverError> {
    let relation_version = relation.version();

    let Some((operator, version)) = relation_version.as_ref() else {
        return Ok(Ranges::full());
    };

    let bound = PackageVersion {
        version: version.clone(),
        repository,
    };

    Ok(match operator {
        VersionConstraint::Equal => Ranges::singleton(bound),
        VersionConstraint::GreaterThan => Ranges::strictly_higher_than(bound),
        VersionConstraint::GreaterThanEqual => Ranges::higher_than(bound),
        VersionConstraint::LessThan => Ranges::strictly_lower_than(bound),
        VersionConstraint::LessThanEqual => Ranges::lower_than(bound),
        VersionConstraint::NotEqual => {
            return Err(ResolverError(
                "not-equal dependency constraints are not supported".to_string(),
            ));
        }
    })
}

pub fn is_base_package(package: &str) -> bool {
    BASE_PACKAGES.contains(&package)
}

pub async fn resolve_from_registry(
    client: http::HttpClient,
    repositories: Vec<PackageRepository>,
    root_dependencies: BTreeMap<String, Ranges<PackageVersion>>,
    preferred_versions: BTreeMap<String, PackageVersion>,
) -> Result<Vec<(String, PackageVersion)>, String> {
    if root_dependencies.is_empty() {
        return Ok(Vec::new());
    }

    let span = tracing::info_span!(
        "resolve_dependencies",
        roots = root_dependencies.len(),
        repositories = repositories.len(),
        preferred = preferred_versions.len(),
        selected = tracing::field::Empty,
        stage = tracing::field::Empty,
        indicatif.pb_show = true,
    );
    span.pb_set_message("resolve dependencies");
    span.pb_start();

    let root_version = root_package_version()
        .await
        .map_err(|error| error.to_string())?;

    let provider =
        RDependencyProvider::new(client, repositories, root_dependencies, preferred_versions)
            .map_err(|error| error.to_string())?;

    let resolve_span = span.clone();
    let selected = tokio::task::spawn_blocking(move || {
        let _enter = resolve_span.enter();
        resolve_span.record("stage", "solving");

        let selected = resolve(&provider, ROOT_PACKAGE.to_string(), root_version)
            .map_err(|error| error.to_string())?;

        let mut selected = selected
            .into_iter()
            .filter(|(package, _)| package != ROOT_PACKAGE)
            .collect::<Vec<_>>();

        selected.sort_by(|left, right| left.0.cmp(&right.0));
        resolve_span.record("selected", selected.len());

        Ok::<_, String>(selected)
    })
    .await
    .map_err(|error| format!("failed to join resolver task: {error}"))??;

    span.record("stage", "done");
    span.record("selected", selected.len());
    span.pb_set_finish_message(&format!("resolve dependencies {} packages", selected.len()));

    Ok(selected)
}

pub fn package_range(
    repository: Arc<PackageRepository>,
    constraint: &str,
) -> Result<Ranges<PackageVersion>, String> {
    parse_package_constraint_range(repository, constraint).map_err(|error| error.to_string())
}

fn parse_package_constraint_range(
    repository: Arc<PackageRepository>,
    constraint: &str,
) -> Result<Ranges<PackageVersion>, ResolverError> {
    let constraint = constraint.trim();
    if constraint.is_empty() || constraint == "*" {
        return Ok(Ranges::full());
    }

    constraint
        .trim_start_matches('(')
        .trim_end_matches(')')
        .split(',')
        .map(str::trim)
        .filter(|part| !part.is_empty())
        .try_fold(Ranges::full(), |range, part| {
            Ok(range.intersection(&package_range_from_constraint_part(
                Arc::clone(&repository),
                part,
            )?))
        })
}

fn package_range_from_constraint_part(
    repository: Arc<PackageRepository>,
    constraint: &str,
) -> Result<Ranges<PackageVersion>, ResolverError> {
    let (operator, version) = parse_constraint_part(constraint);
    let version = PackageVersion::new(parse_version(version)?, repository);

    Ok(match operator {
        ParsedConstraint::Eq => Ranges::singleton(version),
        ParsedConstraint::Gt => Ranges::strictly_higher_than(version),
        ParsedConstraint::Gte => Ranges::higher_than(version),
        ParsedConstraint::Lt => Ranges::strictly_lower_than(version),
        ParsedConstraint::Lte => Ranges::lower_than(version),
        ParsedConstraint::Ne => {
            return Err(ResolverError(
                "not-equal dependency constraints are not supported".to_string(),
            ));
        }
    })
}

fn parse_constraint_part(constraint: &str) -> (ParsedConstraint, &str) {
    for (prefix, operator) in [
        (">=", ParsedConstraint::Gte),
        ("<=", ParsedConstraint::Lte),
        ("==", ParsedConstraint::Eq),
        ("!=", ParsedConstraint::Ne),
        (">", ParsedConstraint::Gt),
        ("<", ParsedConstraint::Lt),
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

#[derive(Debug, Clone, Copy)]
enum ParsedConstraint {
    Eq,
    Gt,
    Gte,
    Lt,
    Lte,
    Ne,
}
