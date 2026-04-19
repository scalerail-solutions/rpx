use std::collections::{BTreeMap, BTreeSet};

use crate::registry::{ClosurePackage, ClosureRequest, ClosureResponse, ClosureVersion};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedPackage {
    pub name: String,
    pub version: String,
    pub source_url: String,
    pub source_tarball_key: String,
    pub description_key: String,
}

pub fn resolve_from_closure(
    request: &ClosureRequest,
    response: &ClosureResponse,
) -> Result<Vec<ResolvedPackage>, String> {
    let response = match response {
        ClosureResponse::Complete(response) => response,
        ClosureResponse::Ingesting(_) => {
            return Err("cannot resolve dependencies from an ingesting closure".to_string());
        }
    };

    let package_index = response
        .packages
        .iter()
        .map(|package| (package.name.as_str(), package))
        .collect::<BTreeMap<_, _>>();

    let mut constraints = BTreeMap::<String, Vec<String>>::new();
    let mut pending = BTreeSet::new();

    for root in &request.roots {
        constraints
            .entry(root.name.clone())
            .or_default()
            .push(root.constraint.clone());
        pending.insert(root.name.clone());
    }

    let assignments = solve(&package_index, constraints, pending, BTreeMap::new())?;

    Ok(assignments
        .into_values()
        .map(|resolved| ResolvedPackage {
            name: resolved.package_name,
            version: resolved.version.version.clone(),
            source_url: resolved.version.source_url.clone(),
            source_tarball_key: resolved.version.source_tarball_key.clone(),
            description_key: resolved.version.description_key.clone(),
        })
        .collect())
}

#[derive(Debug, Clone)]
struct AssignedVersion<'a> {
    package_name: String,
    version: &'a ClosureVersion,
}

fn solve<'a>(
    package_index: &BTreeMap<&'a str, &'a ClosurePackage>,
    constraints: BTreeMap<String, Vec<String>>,
    pending: BTreeSet<String>,
    assignments: BTreeMap<String, AssignedVersion<'a>>,
) -> Result<BTreeMap<String, AssignedVersion<'a>>, String> {
    if pending.is_empty() {
        return Ok(assignments);
    }

    let package_name = pending
        .iter()
        .next()
        .cloned()
        .expect("pending is not empty");
    let package = package_index
        .get(package_name.as_str())
        .copied()
        .ok_or_else(|| format!("package missing from closure: {package_name}"))?;
    let package_constraints = constraints
        .get(&package_name)
        .cloned()
        .unwrap_or_else(|| vec!["*".to_string()]);

    let mut candidates = package
        .versions
        .iter()
        .filter(|version| version_satisfies_all(&version.version, &package_constraints))
        .collect::<Vec<_>>();
    candidates.sort_by(|left, right| compare_versions(&right.version, &left.version));

    if candidates.is_empty() {
        return Err(format!(
            "no version of {package_name} satisfies constraints [{}]",
            package_constraints.join(", ")
        ));
    }

    for candidate in candidates {
        let mut next_pending = pending.clone();
        next_pending.remove(&package_name);

        let mut next_constraints = constraints.clone();
        let mut next_assignments = assignments.clone();
        next_assignments.insert(
            package_name.clone(),
            AssignedVersion {
                package_name: package_name.clone(),
                version: candidate,
            },
        );

        if add_dependency_constraints(
            package_index,
            candidate,
            &mut next_constraints,
            &mut next_pending,
            &next_assignments,
        ) {
            if let Ok(result) = solve(
                package_index,
                next_constraints,
                next_pending,
                next_assignments,
            ) {
                return Ok(result);
            }
        }
    }

    Err(format!(
        "could not resolve a consistent dependency set for {package_name}"
    ))
}

fn add_dependency_constraints(
    package_index: &BTreeMap<&str, &ClosurePackage>,
    version: &ClosureVersion,
    constraints: &mut BTreeMap<String, Vec<String>>,
    pending: &mut BTreeSet<String>,
    assignments: &BTreeMap<String, AssignedVersion<'_>>,
) -> bool {
    for dependency in &version.dependencies {
        if !package_index.contains_key(dependency.dependency_name.as_str()) {
            continue;
        }

        constraints
            .entry(dependency.dependency_name.clone())
            .or_default()
            .push(
                dependency
                    .constraint_raw
                    .clone()
                    .unwrap_or_else(|| "*".to_string()),
            );

        if let Some(assigned) = assignments.get(&dependency.dependency_name) {
            let dependency_constraints = constraints
                .get(&dependency.dependency_name)
                .expect("dependency constraints should exist");

            if !version_satisfies_all(&assigned.version.version, dependency_constraints) {
                return false;
            }
        } else {
            pending.insert(dependency.dependency_name.clone());
        }
    }

    true
}

fn version_satisfies_all(version: &str, constraints: &[String]) -> bool {
    constraints
        .iter()
        .all(|constraint| version_satisfies_constraint(version, constraint))
}

fn version_satisfies_constraint(version: &str, constraint: &str) -> bool {
    let constraint = constraint.trim();

    if constraint.is_empty() || constraint == "*" {
        return true;
    }

    let constraint = constraint
        .trim_start_matches('(')
        .trim_end_matches(')')
        .trim();

    constraint
        .split(',')
        .map(str::trim)
        .filter(|part| !part.is_empty())
        .all(|part| version_satisfies_part(version, part))
}

fn version_satisfies_part(version: &str, constraint: &str) -> bool {
    let (operator, expected) = match parse_constraint_part(constraint) {
        Some(parsed) => parsed,
        None => return false,
    };

    let ordering = compare_versions(version, expected);

    match operator {
        ConstraintOperator::Eq => ordering == std::cmp::Ordering::Equal,
        ConstraintOperator::Gt => ordering == std::cmp::Ordering::Greater,
        ConstraintOperator::Gte => matches!(
            ordering,
            std::cmp::Ordering::Equal | std::cmp::Ordering::Greater
        ),
        ConstraintOperator::Lt => ordering == std::cmp::Ordering::Less,
        ConstraintOperator::Lte => {
            matches!(
                ordering,
                std::cmp::Ordering::Equal | std::cmp::Ordering::Less
            )
        }
    }
}

fn compare_versions(left: &str, right: &str) -> std::cmp::Ordering {
    let left_parts = version_parts(left);
    let right_parts = version_parts(right);
    let len = left_parts.len().max(right_parts.len());

    for index in 0..len {
        let left_part = left_parts
            .get(index)
            .copied()
            .unwrap_or(VersionPart::Numeric(0));
        let right_part = right_parts
            .get(index)
            .copied()
            .unwrap_or(VersionPart::Numeric(0));

        let ordering = match (left_part, right_part) {
            (VersionPart::Numeric(left), VersionPart::Numeric(right)) => left.cmp(&right),
            (VersionPart::Text(left), VersionPart::Text(right)) => left.cmp(right),
            (VersionPart::Numeric(left), VersionPart::Text(right)) => {
                left.to_string().as_str().cmp(right)
            }
            (VersionPart::Text(left), VersionPart::Numeric(right)) => {
                left.cmp(right.to_string().as_str())
            }
        };

        if ordering != std::cmp::Ordering::Equal {
            return ordering;
        }
    }

    std::cmp::Ordering::Equal
}

fn version_parts(version: &str) -> Vec<VersionPart<'_>> {
    version
        .split(['.', '-'])
        .filter(|part| !part.is_empty())
        .map(|part| match part.parse::<u64>() {
            Ok(value) => VersionPart::Numeric(value),
            Err(_) => VersionPart::Text(part),
        })
        .collect()
}

fn parse_constraint_part(constraint: &str) -> Option<(ConstraintOperator, &str)> {
    for (prefix, operator) in [
        (">=", ConstraintOperator::Gte),
        ("<=", ConstraintOperator::Lte),
        (">>", ConstraintOperator::Gt),
        ("<<", ConstraintOperator::Lt),
        ("==", ConstraintOperator::Eq),
        (">", ConstraintOperator::Gt),
        ("<", ConstraintOperator::Lt),
        ("=", ConstraintOperator::Eq),
    ] {
        if let Some(value) = constraint.strip_prefix(prefix) {
            let value = value.trim();
            if value.is_empty() {
                return None;
            }
            return Some((operator, value));
        }
    }

    Some((ConstraintOperator::Eq, constraint.trim()))
}

#[derive(Debug, Clone, Copy)]
enum ConstraintOperator {
    Eq,
    Gt,
    Gte,
    Lt,
    Lte,
}

#[derive(Debug, Clone, Copy)]
enum VersionPart<'a> {
    Numeric(u64),
    Text(&'a str),
}

#[cfg(test)]
mod tests {
    use crate::registry::{
        ClosureDependency, ClosureRoot, CompleteClosureResponse, IngestingResponse,
    };

    use super::*;

    #[test]
    fn resolves_single_root_to_highest_matching_version() {
        let request = closure_request(vec![root("dplyr", "*")]);
        let response = complete_response(vec![package(
            "dplyr",
            vec![version("1.1.3", vec![]), version("1.1.4", vec![])],
        )]);

        let resolved = resolve_from_closure(&request, &response).expect("resolution should work");

        assert_eq!(resolved_names(&resolved), ["dplyr@1.1.4"]);
    }

    #[test]
    fn resolves_transitive_dependencies() {
        let request = closure_request(vec![root("dplyr", "*")]);
        let response = complete_response(vec![
            package(
                "dplyr",
                vec![version(
                    "1.1.4",
                    vec![dependency("rlang", "Imports", ">= 1.1.0")],
                )],
            ),
            package(
                "rlang",
                vec![version("1.0.6", vec![]), version("1.1.1", vec![])],
            ),
        ]);

        let resolved = resolve_from_closure(&request, &response).expect("resolution should work");

        assert_eq!(resolved_names(&resolved), ["dplyr@1.1.4", "rlang@1.1.1"]);
    }

    #[test]
    fn respects_version_constraints_when_choosing_transitives() {
        let request = closure_request(vec![root("pkg", "*")]);
        let response = complete_response(vec![
            package(
                "pkg",
                vec![version(
                    "1.0.0",
                    vec![dependency("dep", "Imports", ">= 1.1.0, < 2.0.0")],
                )],
            ),
            package(
                "dep",
                vec![
                    version("2.0.0", vec![]),
                    version("1.5.0", vec![]),
                    version("1.0.0", vec![]),
                ],
            ),
        ]);

        let resolved = resolve_from_closure(&request, &response).expect("resolution should work");

        assert_eq!(resolved_names(&resolved), ["dep@1.5.0", "pkg@1.0.0"]);
    }

    #[test]
    fn backtracks_to_find_a_consistent_solution() {
        let request = closure_request(vec![root("pkg", "*"), root("dep", ">= 1.0.0")]);
        let response = complete_response(vec![
            package(
                "pkg",
                vec![
                    version("2.0.0", vec![dependency("dep", "Imports", ">= 2.0.0")]),
                    version("1.0.0", vec![dependency("dep", "Imports", "< 2.0.0")]),
                ],
            ),
            package(
                "dep",
                vec![version("2.0.0", vec![]), version("1.5.0", vec![])],
            ),
        ]);

        let resolved = resolve_from_closure(&request, &response).expect("resolution should work");

        assert_eq!(resolved_names(&resolved), ["dep@2.0.0", "pkg@2.0.0"]);
    }

    #[test]
    fn returns_packages_in_deterministic_name_order() {
        let request = closure_request(vec![root("zoo", "*"), root("alpha", "*")]);
        let response = complete_response(vec![
            package("zoo", vec![version("1.0.0", vec![])]),
            package("alpha", vec![version("2.0.0", vec![])]),
        ]);

        let resolved = resolve_from_closure(&request, &response).expect("resolution should work");

        assert_eq!(resolved_names(&resolved), ["alpha@2.0.0", "zoo@1.0.0"]);
    }

    #[test]
    fn rejects_ingesting_closure_responses() {
        let request = closure_request(vec![root("dplyr", "*")]);
        let response = ClosureResponse::Ingesting(IngestingResponse {});

        let error = resolve_from_closure(&request, &response).expect_err("resolution should fail");

        assert_eq!(
            error,
            "cannot resolve dependencies from an ingesting closure"
        );
    }

    #[test]
    fn accepts_debian_style_strict_bounds() {
        let request = closure_request(vec![root("pkg", "*"), root("dep", "<< 2.0.0")]);
        let response = complete_response(vec![
            package(
                "pkg",
                vec![version("1.0.0", vec![dependency("dep", "Imports", "<< 2.0.0")])],
            ),
            package(
                "dep",
                vec![version("2.0.0", vec![]), version("1.5.0", vec![])],
            ),
        ]);

        let resolved = resolve_from_closure(&request, &response).expect("resolution should work");

        assert_eq!(resolved_names(&resolved), ["dep@1.5.0", "pkg@1.0.0"]);
    }

    fn closure_request(roots: Vec<ClosureRoot>) -> ClosureRequest {
        ClosureRequest { roots }
    }

    fn complete_response(packages: Vec<ClosurePackage>) -> ClosureResponse {
        ClosureResponse::Complete(CompleteClosureResponse {
            roots: vec![],
            include_dependency_kinds: vec![
                "Depends".to_string(),
                "Imports".to_string(),
                "LinkingTo".to_string(),
            ],
            packages,
        })
    }

    fn root(name: &str, constraint: &str) -> ClosureRoot {
        ClosureRoot {
            name: name.to_string(),
            constraint: constraint.to_string(),
        }
    }

    fn package(name: &str, versions: Vec<ClosureVersion>) -> ClosurePackage {
        ClosurePackage {
            name: name.to_string(),
            versions,
        }
    }

    fn version(version: &str, dependencies: Vec<ClosureDependency>) -> ClosureVersion {
        ClosureVersion {
            version: version.to_string(),
            source_url: format!("https://api.rrepo.org/packages/pkg/versions/{version}/source"),
            source_tarball_key: format!("src/pkg_{version}.tar.gz"),
            description_key: format!("desc/pkg_{version}"),
            dependencies,
        }
    }

    fn dependency(name: &str, kind: &str, constraint: &str) -> ClosureDependency {
        ClosureDependency {
            dependency_name: name.to_string(),
            dependency_kind: kind.to_string(),
            constraint_raw: Some(constraint.to_string()),
        }
    }

    fn resolved_names(resolved: &[ResolvedPackage]) -> Vec<String> {
        resolved
            .iter()
            .map(|package| format!("{}@{}", package.name, package.version))
            .collect()
    }
}
