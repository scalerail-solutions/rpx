use deb822_fast::Paragraph;
use r_description::{Version, VersionConstraint};
use std::{collections::BTreeSet, fs, str::FromStr};

use crate::project::{current_description_path, description_path};
use crate::registry::ResolutionRoot;

const DESCRIPTION_FIELD_ORDER: &[&str] = &[
    "Package",
    "Version",
    "Title",
    "Authors@R",
    "Author",
    "Maintainer",
    "Description",
    "License",
    "Depends",
    "Imports",
    "Suggests",
    "Enhances",
    "LinkingTo",
    "Additional_repositories",
    "URL",
    "BugReports",
    "Encoding",
    "Repository",
    "Date",
    "VignetteBuilder",
    "SystemRequirements",
    "Language",
    "Collate",
    "LazyData",
];

pub trait DescriptionExt {
    fn add_to_imports(&mut self, package: &str);
    fn add_to_imports_with_constraints(&mut self, package: &str, constraints: &[String]);
    fn has_dependency(&self, package: &str) -> bool;
    fn remove_from_field(&mut self, field_name: &str, package: &str);
    fn resolution_roots(&self) -> Vec<ResolutionRoot>;
    fn requirements(&self) -> Vec<String>;
}

#[derive(Debug, Clone)]
pub struct RDescription {
    paragraph: Paragraph,
}

#[derive(Debug)]
pub struct ProjectDescription {
    pub description: RDescription,
    pub additional_repositories: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DescriptionDependency {
    pub name: String,
    pub version: Option<(VersionConstraint, Version)>,
}

impl std::fmt::Display for DescriptionDependency {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.version {
            Some((operator, version)) => write!(f, "{} ({} {version})", self.name, relation_operator(operator)),
            None => f.write_str(&self.name),
        }
    }
}

impl std::str::FromStr for RDescription {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let paragraph = Paragraph::from_str(s).map_err(|error| error.to_string())?;
        Ok(Self { paragraph })
    }
}

impl std::fmt::Display for RDescription {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.paragraph)
    }
}

impl RDescription {
    pub fn package_name(&self) -> Option<&str> {
        self.paragraph.get("Package")
    }

    pub fn dependency_field(&self, field_name: &str) -> Result<Vec<DescriptionDependency>, String> {
        match self.paragraph.get(field_name) {
            Some(contents) => parse_dependency_field(contents),
            None => Ok(Vec::new()),
        }
    }

    pub fn additional_repositories(&self) -> Vec<String> {
        self.paragraph
            .get("Additional_repositories")
            .map(split_repository_entries)
            .unwrap_or_default()
    }

    pub fn remove_field(&mut self, field_name: &str) {
        self.paragraph.remove(field_name);
    }

    fn set_field(&mut self, field_name: &str, value: &str) {
        self.paragraph
            .set_with_field_order(field_name, value, DESCRIPTION_FIELD_ORDER);
    }

    fn set_dependency_field(&mut self, field_name: &str, dependencies: &[DescriptionDependency]) {
        if dependencies.is_empty() {
            self.paragraph.remove(field_name);
            return;
        }

        let value = dependencies
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>()
            .join(", ");
        self.set_field(field_name, &value);
    }

    fn field_pairs(&self) -> impl Iterator<Item = (&str, &str)> {
        self.paragraph.iter()
    }
}

pub fn read_description() -> Result<ProjectDescription, String> {
    let path = description_path();
    let contents = fs::read_to_string(&path).map_err(|error| error.to_string())?;
    let mut description = RDescription::from_str(&contents)?;
    let additional_repositories = description.additional_repositories();
    description.remove_field("Additional_repositories");

    if description.package_name().unwrap_or_default().trim().is_empty() {
        return Err("DESCRIPTION is missing Package".to_string());
    }

    Ok(ProjectDescription {
        description,
        additional_repositories,
    })
}

pub fn write_description(project: &ProjectDescription) {
    let mut contents = format_description_for_write(&project.description);
    if !project.additional_repositories.is_empty() {
        contents.push('\n');
        contents.push_str(&format_additional_repositories(
            &project.additional_repositories,
        ));
    }
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
        additional_repositories: vec![],
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

        let mut imports = self
            .dependency_field("Imports")
            .expect("existing Imports should parse");

        if constraints.is_empty()
            || constraints
                .iter()
                .all(|constraint| constraint.trim() == "*")
        {
            imports.push(DescriptionDependency {
                name: package.to_string(),
                version: None,
            });
        } else {
            imports.extend(
                constraints
                    .iter()
                    .map(|constraint| relation_with_constraint(package, constraint))
                    .collect::<Result<Vec<_>, _>>()
                    .expect("constraints should parse"),
            );
        }

        self.set_dependency_field("Imports", &imports);
    }

    fn has_dependency(&self, package: &str) -> bool {
        ["Imports", "Depends"]
            .into_iter()
            .any(|field| {
                self.dependency_field(field)
                    .map(|dependencies| dependencies.into_iter().any(|entry| entry.name == package))
                    .unwrap_or(false)
            })
    }

    fn remove_from_field(&mut self, field_name: &str, package: &str) {
        let filtered = self
            .dependency_field(field_name)
            .expect("dependency field should parse")
            .into_iter()
            .filter(|entry| entry.name != package)
            .collect::<Vec<_>>();
        self.set_dependency_field(field_name, &filtered);
    }

    fn resolution_roots(&self) -> Vec<ResolutionRoot> {
        let mut roots = BTreeSet::new();

        for relation in self
            .dependency_field("Imports")
            .expect("Imports should parse")
        {
            roots.insert(resolution_root_from_relation(&relation));
        }

        for relation in self
            .dependency_field("Depends")
            .expect("Depends should parse")
        {
            if relation.name != "R" {
                roots.insert(resolution_root_from_relation(&relation));
            }
        }

        roots.into_iter().collect()
    }

    fn requirements(&self) -> Vec<String> {
        let mut requirements = BTreeSet::new();

        for relation in self
            .dependency_field("Imports")
            .expect("Imports should parse")
        {
            requirements.insert(relation.name);
        }

        for relation in self
            .dependency_field("Depends")
            .expect("Depends should parse")
        {
            if relation.name != "R" {
                requirements.insert(relation.name);
            }
        }

        requirements.into_iter().collect()
    }
}

fn format_description_for_write(description: &RDescription) -> String {
    description
        .field_pairs()
        .map(|(field, value)| format_description_field(field, value))
        .collect::<Vec<_>>()
        .join("\n")
}

fn format_description_field(field: &str, value: &str) -> String {
    const MULTILINE_RELATION_FIELDS: &[&str] =
        &["Imports", "Depends", "Suggests", "Enhances", "LinkingTo"];

    if MULTILINE_RELATION_FIELDS.contains(&field) {
        let mut entries = parse_dependency_field(value).expect("stored dependency field should parse");
        if entries.is_empty() {
            return format!("{field}:");
        }

        entries.sort_by(compare_dependencies);

        let mut lines = vec![format!("{field}:")];
        for (index, entry) in entries.iter().enumerate() {
            let suffix = if index + 1 < entries.len() { "," } else { "" };
            lines.push(format!("    {entry}{suffix}"));
        }
        return lines.join("\n");
    }

    let value_lines = value.lines().collect::<Vec<_>>();
    if value_lines.len() <= 1 {
        return format!("{field}: {value}");
    }

    let mut lines = vec![format!("{field}:")];
    lines.extend(value_lines.into_iter().map(|line| format!(" {line}")));
    lines.join("\n")
}

fn format_additional_repositories(repositories: &[String]) -> String {
    if repositories.is_empty() {
        return String::new();
    }

    if repositories.len() == 1 {
        return format!("Additional_repositories: {}", repositories[0]);
    }

    format!(
        "Additional_repositories:\n    {}",
        repositories.join(",\n    ")
    )
}

fn compare_dependencies(
    left: &DescriptionDependency,
    right: &DescriptionDependency,
) -> std::cmp::Ordering {
    left.name
        .to_ascii_lowercase()
        .cmp(&right.name.to_ascii_lowercase())
        .then_with(|| left.name.cmp(&right.name))
        .then_with(|| dependency_operator_rank(left).cmp(&dependency_operator_rank(right)))
        .then_with(|| left.to_string().cmp(&right.to_string()))
}

fn dependency_operator_rank(entry: &DescriptionDependency) -> u8 {
    match entry.version.as_ref().map(|(operator, _)| operator) {
        None => 0,
        Some(VersionConstraint::GreaterThanEqual) => 1,
        Some(VersionConstraint::GreaterThan) => 2,
        Some(VersionConstraint::Equal) => 3,
        Some(VersionConstraint::LessThanEqual) => 4,
        Some(VersionConstraint::LessThan) => 5,
    }
}

fn relation_with_constraint(package: &str, constraint: &str) -> Result<DescriptionDependency, String> {
    let constraint = constraint.trim();

    if constraint.is_empty() || constraint == "*" {
        return Ok(DescriptionDependency {
            name: package.to_string(),
            version: None,
        });
    }

    let (operator, version) = parse_constraint(constraint)?;

    Ok(DescriptionDependency {
        name: package.to_string(),
        version: Some((
            operator,
            version.parse().map_err(|error: String| error.to_string())?,
        )),
    })
}

fn parse_dependency_field(value: &str) -> Result<Vec<DescriptionDependency>, String> {
    value
        .split(',')
        .map(str::trim)
        .filter(|entry| !entry.is_empty())
        .map(parse_dependency_entry)
        .collect()
}

fn parse_dependency_entry(entry: &str) -> Result<DescriptionDependency, String> {
    let Some((name, rest)) = entry.split_once('(') else {
        return Ok(DescriptionDependency {
            name: entry.trim().to_string(),
            version: None,
        });
    };

    let name = name.trim();
    if name.is_empty() {
        return Err(format!("invalid dependency entry: {entry}"));
    }

    let rest = rest.trim();
    let Some(constraint) = rest.strip_suffix(')') else {
        return Err(format!("invalid dependency entry: {entry}"));
    };

    relation_with_constraint(name, constraint.trim())
}

fn split_repository_entries(value: &str) -> Vec<String> {
    value
        .split(',')
        .map(str::trim)
        .filter(|entry| !entry.is_empty())
        .map(ToString::to_string)
        .collect()
}

fn parse_constraint(constraint: &str) -> Result<(VersionConstraint, &str), String> {
    for (prefix, operator) in [
        (">=", VersionConstraint::GreaterThanEqual),
        ("<=", VersionConstraint::LessThanEqual),
        ("<<", VersionConstraint::LessThan),
        (">>", VersionConstraint::GreaterThan),
        ("==", VersionConstraint::Equal),
        ("=", VersionConstraint::Equal),
        ("<", VersionConstraint::LessThan),
        (">", VersionConstraint::GreaterThan),
    ] {
        if let Some(version) = constraint.strip_prefix(prefix) {
            let version = version.trim();
            if !version.is_empty() {
                return Ok((operator, version));
            }
        }
    }

    Err(format!("invalid dependency constraint: {constraint}"))
}

fn resolution_root_from_relation(relation: &DescriptionDependency) -> ResolutionRoot {
    let constraint = relation
        .version
        .as_ref()
        .map(|(operator, version)| format!("{} {version}", relation_operator(operator)))
        .unwrap_or_else(|| "*".to_string());

    ResolutionRoot {
        name: relation.name.clone(),
        constraint,
    }
}

fn relation_operator(operator: &VersionConstraint) -> &'static str {
    match operator {
        VersionConstraint::LessThan => "<",
        VersionConstraint::GreaterThan => ">",
        VersionConstraint::LessThanEqual => "<=",
        VersionConstraint::GreaterThanEqual => ">=",
        VersionConstraint::Equal => "=",
    }
}

fn initial_description(package_name: &str) -> RDescription {
    let mut paragraph = Paragraph::from(Vec::new());
    paragraph.insert("Package", package_name);
    paragraph.insert("Version", "0.1.0");
    paragraph.insert("Title", &title_from_package_name(package_name));
    paragraph.insert("Description", "Add a package description.");
    paragraph.insert("License", "MIT");
    paragraph.insert("Author", "Your Name");
    paragraph.insert("Maintainer", "Your Name <you@example.com>");
    RDescription { paragraph }
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
        DescriptionExt, RDescription, format_additional_repositories, format_description_for_write,
        parse_constraint, relation_with_constraint, sanitize_package_name,
        split_repository_entries, title_from_package_name,
    };
    use crate::registry::ResolutionRoot;
    use r_description::VersionConstraint;
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

        let imports = description
            .dependency_field("Imports")
            .expect("imports should parse");
        assert_eq!(imports.len(), 2);
        assert_eq!(imports[0].to_string(), "dplyr (>= 1.1.4)");
        assert_eq!(imports[1].to_string(), "dplyr (< 2.0.0)");
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
            parse_constraint("< 2.0.0").unwrap(),
            (VersionConstraint::LessThan, "2.0.0")
        );
        assert_eq!(
            parse_constraint("<< 2.0.0").unwrap(),
            (VersionConstraint::LessThan, "2.0.0")
        );
        assert_eq!(
            relation_with_constraint("dplyr", "< 2.0.0")
                .unwrap()
                .to_string(),
            "dplyr (< 2.0.0)"
        );
    }

    #[test]
    fn canonicalizes_cran_style_greater_than_constraints() {
        let description = RDescription::from_str(
            "Package: testpkg\nVersion: 0.1.0\nTitle: Test Package\nDescription: Test package for unit tests.\nLicense: MIT\nImports: rbibutils (> 2.4)\n",
        )
        .expect("description should parse");

        let formatted = format_description_for_write(&description);

        assert!(formatted.contains("Imports:\n    rbibutils (> 2.4)"));
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

        assert!(formatted.contains("Imports:\n    dplyr (>= 1.1.4),\n    dplyr (< 2.0.0)"));
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
    fn normalizes_strict_relation_operators_for_resolution_roots() {
        let description = RDescription::from_str(
            "Package: testpkg\nVersion: 0.1.0\nTitle: Test Package\nDescription: Test package for unit tests.\nLicense: MIT\nImports: AzureAuth (<< 2.0.0), httr2 (>> 1.0.0)\n",
        )
        .expect("description should parse");

        assert_eq!(
            description.resolution_roots(),
            vec![
                ResolutionRoot {
                    name: "AzureAuth".to_string(),
                    constraint: "< 2.0.0".to_string(),
                },
                ResolutionRoot {
                    name: "httr2".to_string(),
                    constraint: "> 1.0.0".to_string(),
                },
            ]
        );
    }

    #[test]
    fn parses_additional_repositories_from_description() {
        let description = RDescription::from_str(
            "Package: testpkg\nAdditional_repositories:\n    https://one.example/repo,\n    https://two.example/repo\n",
        )
        .expect("description should parse");
        let repositories = description.additional_repositories();

        assert_eq!(
            repositories,
            vec![
                "https://one.example/repo".to_string(),
                "https://two.example/repo".to_string()
            ]
        );
    }

    #[test]
    fn formats_additional_repositories_as_multiline_field() {
        assert_eq!(
            format_additional_repositories(&[
                "https://one.example/repo".to_string(),
                "https://two.example/repo".to_string(),
            ]),
            "Additional_repositories:\n    https://one.example/repo,\n    https://two.example/repo"
        );
    }

    #[test]
    fn splits_repository_entries_from_folded_field_value() {
        assert_eq!(
            split_repository_entries("https://one.example/repo,\nhttps://two.example/repo"),
            vec![
                "https://one.example/repo".to_string(),
                "https://two.example/repo".to_string(),
            ]
        );
    }
}
