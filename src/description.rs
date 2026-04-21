use r_description::{
    Version, VersionConstraint, lossy::RDescription, lossy::Relation, lossy::Relations,
};
use std::{collections::BTreeSet, fs, str::FromStr};

use crate::project::{current_description_path, description_path};
use crate::registry::ClosureRoot;

pub trait DescriptionExt {
    fn add_to_imports(&mut self, package: &str);
    fn add_to_imports_with_constraints(&mut self, package: &str, constraints: &[String]);
    fn has_dependency(&self, package: &str) -> bool;
    fn remove_from_field(&mut self, field_name: &str, package: &str);
    fn closure_roots(&self) -> Vec<ClosureRoot>;
    fn requirements(&self) -> Vec<String>;
}

#[derive(Debug)]
pub struct ProjectDescription {
    pub description: RDescription,
}

pub fn read_description() -> Result<ProjectDescription, String> {
    let path = description_path();
    let contents = fs::read_to_string(&path).map_err(|error| error.to_string())?;
    let description = RDescription::from_str(&contents).map_err(|error| error.to_string())?;

    if description.name.trim().is_empty() {
        return Err("DESCRIPTION is missing Package".to_string());
    }

    Ok(ProjectDescription { description })
}

pub fn write_description(project: &ProjectDescription) {
    let mut contents = format_description_for_write(&project.description);
    contents.push('\n');
    fs::write(description_path(), contents).expect("failed to write DESCRIPTION");
}

pub fn init_description() -> Result<String, String> {
    let path = current_description_path();
    if path.exists() {
        return Err("DESCRIPTION already exists".to_string());
    }

    let package_name = package_name_from_current_directory()?;
    let description = ProjectDescription {
        description: initial_description(&package_name),
    };

    let mut contents = format_description_for_write(&description.description);
    contents.push('\n');
    fs::write(&path, contents).map_err(|error| error.to_string())?;

    Ok(path.display().to_string())
}

impl DescriptionExt for RDescription {
    fn add_to_imports(&mut self, package: &str) {
        self.add_to_imports_with_constraints(package, &[]);
    }

    fn add_to_imports_with_constraints(&mut self, package: &str, constraints: &[String]) {
        if self.has_dependency(package) {
            return;
        }

        let mut imports = self.imports.clone().unwrap_or_default();

        if constraints.is_empty()
            || constraints
                .iter()
                .all(|constraint| constraint.trim() == "*")
        {
            imports.0.push(Relation {
                name: package.to_string(),
                version: None,
            });
        } else {
            imports.0.extend(
                constraints
                    .iter()
                    .map(|constraint| relation_with_constraint(package, constraint)),
            );
        }

        self.imports = Some(imports);
    }

    fn has_dependency(&self, package: &str) -> bool {
        let present_in_imports = self
            .imports
            .as_ref()
            .map(|imports| imports.iter().any(|entry| entry.name == package))
            .unwrap_or(false);

        if present_in_imports {
            return true;
        }

        self.depends
            .as_ref()
            .map(|depends| depends.iter().any(|entry| entry.name == package))
            .unwrap_or(false)
    }

    fn remove_from_field(&mut self, field_name: &str, package: &str) {
        match field_name {
            "Imports" => {
                let filtered = self
                    .imports
                    .clone()
                    .unwrap_or_default()
                    .0
                    .into_iter()
                    .filter(|entry| entry.name != package)
                    .collect::<Vec<_>>();

                self.imports = if filtered.is_empty() {
                    None
                } else {
                    Some(Relations(filtered))
                };
            }
            "Depends" => {
                let filtered = self
                    .depends
                    .clone()
                    .unwrap_or_default()
                    .iter()
                    .filter(|relation| relation.name != package)
                    .cloned()
                    .collect::<Vec<_>>();

                self.depends = if filtered.is_empty() {
                    None
                } else {
                    Some(Relations(filtered))
                };
            }
            _ => {}
        }
    }

    fn closure_roots(&self) -> Vec<ClosureRoot> {
        let mut roots = BTreeSet::new();

        if let Some(imports) = &self.imports {
            for relation in imports.iter() {
                roots.insert(closure_root_from_relation(relation));
            }
        }

        if let Some(depends) = &self.depends {
            for relation in depends.iter() {
                if relation.name != "R" {
                    roots.insert(closure_root_from_relation(relation));
                }
            }
        }

        roots.into_iter().collect()
    }

    fn requirements(&self) -> Vec<String> {
        let mut requirements = BTreeSet::new();

        if let Some(imports) = &self.imports {
            for relation in imports.iter() {
                requirements.insert(relation.name.clone());
            }
        }

        if let Some(depends) = &self.depends {
            for relation in depends.iter() {
                let name = relation.name.clone();
                if name != "R" {
                    requirements.insert(name);
                }
            }
        }

        requirements.into_iter().collect()
    }
}

fn format_description_for_write(description: &RDescription) -> String {
    format!("{description}")
        .trim_end()
        .lines()
        .map(format_description_line)
        .collect::<Vec<_>>()
        .join("\n")
}

fn format_description_line(line: &str) -> String {
    const MULTILINE_RELATION_FIELDS: &[&str] =
        &["Imports", "Depends", "Suggests", "Enhances", "LinkingTo"];

    let Some((field, value)) = line.split_once(':') else {
        return line.to_string();
    };

    if !MULTILINE_RELATION_FIELDS.contains(&field) {
        return line.to_string();
    }

    let value = value.trim();
    if value.is_empty() {
        return line.to_string();
    }

    let entries = value
        .split(", ")
        .filter(|entry| !entry.is_empty())
        .map(str::to_string)
        .collect::<Vec<_>>();

    if entries.is_empty() {
        return line.to_string();
    }

    let mut entries = entries;
    entries.sort_by(|left, right| compare_dependency_entries(left, right));

    format!("{field}:\n    {}", entries.join(",\n    "))
}

fn compare_dependency_entries(left: &str, right: &str) -> std::cmp::Ordering {
    dependency_entry_name(left)
        .to_ascii_lowercase()
        .cmp(&dependency_entry_name(right).to_ascii_lowercase())
        .then_with(|| dependency_entry_name(left).cmp(dependency_entry_name(right)))
        .then_with(|| {
            dependency_entry_operator_rank(left).cmp(&dependency_entry_operator_rank(right))
        })
        .then_with(|| left.cmp(right))
}

fn dependency_entry_name(entry: &str) -> &str {
    entry
        .split_once(" (")
        .map(|(name, _)| name)
        .unwrap_or(entry)
}

fn dependency_entry_operator_rank(entry: &str) -> u8 {
    let Some((_, rest)) = entry.split_once(" (") else {
        return 0;
    };
    let operator = rest
        .trim_end_matches(')')
        .split_whitespace()
        .next()
        .unwrap_or("");
    match operator {
        ">=" => 1,
        ">>" => 2,
        "=" => 3,
        "<=" => 4,
        "<<" => 5,
        _ => 6,
    }
}

fn relation_with_constraint(package: &str, constraint: &str) -> Relation {
    let constraint = constraint.trim();

    if constraint.is_empty() || constraint == "*" {
        return Relation {
            name: package.to_string(),
            version: None,
        };
    }

    let (operator, version) = parse_constraint(constraint);

    Relation {
        name: package.to_string(),
        version: Some((
            operator,
            version.parse().expect("constraint version should parse"),
        )),
    }
}

fn parse_constraint(constraint: &str) -> (VersionConstraint, &str) {
    for (prefix, operator) in [
        (">=", VersionConstraint::GreaterThanEqual),
        ("<=", VersionConstraint::LessThanEqual),
        ("<<", VersionConstraint::LessThan),
        (">>", VersionConstraint::GreaterThan),
        ("=", VersionConstraint::Equal),
        ("<", VersionConstraint::LessThan),
        (">", VersionConstraint::GreaterThan),
    ] {
        if let Some(version) = constraint.strip_prefix(prefix) {
            let version = version.trim();
            if !version.is_empty() {
                return (operator, version);
            }
        }
    }

    panic!("invalid dependency constraint: {constraint}");
}

fn closure_root_from_relation(relation: &Relation) -> ClosureRoot {
    let constraint = relation
        .version
        .as_ref()
        .map(|(operator, version)| format!("{} {version}", closure_operator(operator)))
        .unwrap_or_else(|| "*".to_string());

    ClosureRoot {
        name: relation.name.clone(),
        constraint,
    }
}

fn closure_operator(operator: &VersionConstraint) -> &'static str {
    match operator {
        VersionConstraint::LessThan => "<",
        VersionConstraint::GreaterThan => ">",
        VersionConstraint::LessThanEqual => "<=",
        VersionConstraint::GreaterThanEqual => ">=",
        VersionConstraint::Equal => "=",
    }
}

fn initial_description(package_name: &str) -> RDescription {
    RDescription {
        name: package_name.to_string(),
        description: "Add a package description.".to_string(),
        title: title_from_package_name(package_name),
        maintainer: Some("Your Name <you@example.com>".to_string()),
        author: Some("Your Name".to_string()),
        authors: None,
        version: "0.1.0".parse::<Version>().expect("version should parse"),
        encoding: None,
        license: "MIT".to_string(),
        url: None,
        bug_reports: None,
        imports: None,
        suggests: None,
        depends: None,
        linking_to: None,
        lazy_data: None,
        collate: None,
        vignette_builder: None,
        system_requirements: None,
        date: None,
        language: None,
        repository: None,
    }
}

fn package_name_from_current_directory() -> Result<String, String> {
    let current_dir = std::env::current_dir().map_err(|error| error.to_string())?;
    let directory_name = current_dir
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| "failed to derive package name from current directory".to_string())?;

    sanitize_package_name(directory_name)
}

fn sanitize_package_name(directory_name: &str) -> Result<String, String> {
    let mut package_name = String::new();

    for character in directory_name.chars() {
        match character {
            'a'..='z' | 'A'..='Z' | '0'..='9' => package_name.push(character),
            '-' | '_' | ' ' | '.' => {
                if !package_name.ends_with('.') {
                    package_name.push('.');
                }
            }
            _ => {}
        }
    }

    let package_name = package_name.trim_matches('.').to_string();
    let Some(first) = package_name.chars().next() else {
        return Err("current directory does not produce a valid package name".to_string());
    };

    if !first.is_ascii_alphabetic() {
        return Err("package name must start with a letter".to_string());
    }

    Ok(package_name)
}

fn title_from_package_name(package_name: &str) -> String {
    package_name
        .split('.')
        .filter(|part| !part.is_empty())
        .map(|part| {
            let mut characters = part.chars();
            let Some(first) = characters.next() else {
                return String::new();
            };

            format!("{}{}", first.to_ascii_uppercase(), characters.as_str())
        })
        .collect::<Vec<_>>()
        .join(" ")
}

#[cfg(test)]
mod tests {
    use super::{
        DescriptionExt, format_description_for_write, parse_constraint, relation_with_constraint,
        sanitize_package_name, title_from_package_name,
    };
    use crate::registry::ClosureRoot;
    use r_description::{VersionConstraint, lossy::RDescription};
    use std::str::FromStr;

    #[test]
    fn sanitizes_directory_name_to_package_name() {
        assert_eq!(
            sanitize_package_name("my-package_name").unwrap(),
            "my.package.name"
        );
    }

    #[test]
    fn rejects_package_name_without_leading_letter() {
        assert_eq!(
            sanitize_package_name("123pkg").unwrap_err(),
            "package name must start with a letter"
        );
    }

    #[test]
    fn derives_title_from_package_name() {
        assert_eq!(
            title_from_package_name("my.package.name"),
            "My Package Name"
        );
    }

    #[test]
    fn adds_multiple_import_entries_for_bounded_constraints() {
        let mut description = RDescription::from_str(
            "Package: testpkg\nVersion: 0.1.0\nTitle: Test Package\nDescription: Test package for unit tests.\nLicense: MIT\n",
        )
        .expect("description should parse");

        description.add_to_imports_with_constraints(
            "dplyr",
            &[">= 1.1.4".to_string(), "< 2.0.0".to_string()],
        );

        let imports = description.imports.expect("imports should exist");
        assert_eq!(imports.0.len(), 2);
        assert_eq!(imports.0[0].to_string(), "dplyr (>= 1.1.4)");
        assert_eq!(imports.0[1].to_string(), "dplyr (<< 2.0.0)");
    }

    #[test]
    fn detects_existing_dependency_in_imports_or_depends() {
        let description = RDescription::from_str(
            "Package: testpkg\nVersion: 0.1.0\nTitle: Test Package\nDescription: Test package for unit tests.\nLicense: MIT\nImports: digest\nDepends: R (>= 4.3), cli\n",
        )
        .expect("description should parse");

        assert!(description.has_dependency("digest"));
        assert!(description.has_dependency("cli"));
        assert!(!description.has_dependency("jsonlite"));
    }

    #[test]
    fn parses_strict_less_than_constraints() {
        assert_eq!(
            parse_constraint("< 2.0.0"),
            (VersionConstraint::LessThan, "2.0.0")
        );
        assert_eq!(
            parse_constraint("<< 2.0.0"),
            (VersionConstraint::LessThan, "2.0.0")
        );
        assert_eq!(
            relation_with_constraint("dplyr", "< 2.0.0").to_string(),
            "dplyr (<< 2.0.0)"
        );
    }

    #[test]
    fn formats_relationship_fields_as_multiline() {
        let description = RDescription::from_str(
            "Package: testpkg\nVersion: 0.1.0\nTitle: Test Package\nDescription: Test package for unit tests.\nLicense: MIT\nImports: cli (>= 3.6.0), digest\nDepends: R (>= 4.3), jsonlite\n",
        )
        .expect("description should parse");

        let formatted = format_description_for_write(&description);

        assert!(formatted.contains("Imports:\n    cli (>= 3.6.0),\n    digest"));
        assert!(formatted.contains("Depends:\n    jsonlite,\n    R (>= 4.3)"));
    }

    #[test]
    fn formats_bounded_dependencies_one_per_line() {
        let mut description = RDescription::from_str(
            "Package: testpkg\nVersion: 0.1.0\nTitle: Test Package\nDescription: Test package for unit tests.\nLicense: MIT\n",
        )
        .expect("description should parse");

        description.add_to_imports_with_constraints(
            "dplyr",
            &[">= 1.1.4".to_string(), "< 2.0.0".to_string()],
        );

        let formatted = format_description_for_write(&description);

        assert!(formatted.contains("Imports:\n    dplyr (>= 1.1.4),\n    dplyr (<< 2.0.0)"));
    }

    #[test]
    fn sorts_dependency_entries_within_each_field() {
        let description = RDescription::from_str(
            "Package: testpkg\nVersion: 0.1.0\nTitle: Test Package\nDescription: Test package for unit tests.\nLicense: MIT\nImports: jsonlite, AzureAuth, cli\nDepends: zlib, R (>= 4.3), base\n",
        )
        .expect("description should parse");

        let formatted = format_description_for_write(&description);

        assert!(formatted.contains("Imports:\n    AzureAuth,\n    cli,\n    jsonlite"));
        assert!(formatted.contains("Depends:\n    base,\n    R (>= 4.3),\n    zlib"));
    }

    #[test]
    fn normalizes_strict_relation_operators_for_closure_requests() {
        let description = RDescription::from_str(
            "Package: testpkg\nVersion: 0.1.0\nTitle: Test Package\nDescription: Test package for unit tests.\nLicense: MIT\nImports: AzureAuth (<< 2.0.0), httr2 (>> 1.0.0)\n",
        )
        .expect("description should parse");

        assert_eq!(
            description.closure_roots(),
            vec![
                ClosureRoot {
                    name: "AzureAuth".to_string(),
                    constraint: "< 2.0.0".to_string(),
                },
                ClosureRoot {
                    name: "httr2".to_string(),
                    constraint: "> 1.0.0".to_string(),
                },
            ]
        );
    }
}
