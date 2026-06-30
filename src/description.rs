use deb822_fast::Paragraph;
use miette::Diagnostic;
use r_description::{Version, VersionConstraint};
use std::{
    collections::BTreeSet,
    fs,
    path::{Path, PathBuf},
    str::FromStr,
};
use thiserror::Error;

use crate::project::{ProjectPathError, description_path, new_project_description_path};
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

#[derive(Debug, Clone)]
pub struct RDescription {
    paragraph: Paragraph,
    pub depends: BTreeSet<DescriptionDependency>,
    pub imports: BTreeSet<DescriptionDependency>,
    pub suggests: BTreeSet<DescriptionDependency>,
    pub enhances: BTreeSet<DescriptionDependency>,
    pub linking_to: BTreeSet<DescriptionDependency>,
    pub additional_repositories: Vec<String>,
    pub system_requirements: Option<String>,
}

#[derive(Debug, Error, Diagnostic)]
pub enum DescriptionError {
    #[error(transparent)]
    #[diagnostic(transparent)]
    ProjectPath(#[from] ProjectPathError),

    #[error("failed to read DESCRIPTION at {}: {source}", path.display())]
    #[diagnostic(code(rpx::description::read_failed))]
    ReadFailed {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },

    #[error("failed to parse DESCRIPTION at {}: {details}", path.display())]
    #[diagnostic(code(rpx::description::parse_failed))]
    ParseFailed { path: PathBuf, details: String },

    #[error("DESCRIPTION already exists at {}", path.display())]
    #[diagnostic(
        code(rpx::description::already_exists),
        help(
            "Run rpx commands from this project, or remove DESCRIPTION before initializing a new project."
        )
    )]
    AlreadyExists { path: PathBuf },

    #[error("failed to derive package name for DESCRIPTION: {details}")]
    #[diagnostic(code(rpx::description::package_name_failed))]
    PackageNameFailed { details: String },

    #[error("failed to write DESCRIPTION at {}: {source}", path.display())]
    #[diagnostic(code(rpx::description::write_failed))]
    WriteFailed {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DescriptionDependency {
    pub name: String,
    pub version: Option<(VersionConstraint, Version)>,
}

impl std::fmt::Display for DescriptionDependency {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.version {
            Some((operator, version)) => write!(
                f,
                "{} ({} {version})",
                self.name,
                relation_operator(operator)
            ),
            None => f.write_str(&self.name),
        }
    }
}

impl Ord for DescriptionDependency {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        compare_dependencies(self, other)
    }
}

impl PartialOrd for DescriptionDependency {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl std::str::FromStr for RDescription {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let paragraph = Paragraph::from_str(s).map_err(|error| error.to_string())?;
        Self::from_paragraph(paragraph)
    }
}

impl std::fmt::Display for RDescription {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&format_description_for_write(self))
    }
}

impl RDescription {
    fn from_paragraph(paragraph: Paragraph) -> Result<Self, String> {
        Ok(Self {
            depends: parse_stored_dependency_field(&paragraph, "Depends")?,
            imports: parse_stored_dependency_field(&paragraph, "Imports")?,
            suggests: parse_stored_dependency_field(&paragraph, "Suggests")?,
            enhances: parse_stored_dependency_field(&paragraph, "Enhances")?,
            linking_to: parse_stored_dependency_field(&paragraph, "LinkingTo")?,
            additional_repositories: paragraph
                .get("Additional_repositories")
                .map(split_repository_entries)
                .unwrap_or_default(),
            system_requirements: paragraph
                .get("SystemRequirements")
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .map(ToString::to_string),
            paragraph,
        })
    }
}

pub fn read_description() -> Result<RDescription, DescriptionError> {
    let path = description_path()?;
    let contents = fs::read_to_string(&path).map_err(|source| DescriptionError::ReadFailed {
        path: path.clone(),
        source,
    })?;
    RDescription::from_str(&contents).map_err(|details| DescriptionError::ParseFailed {
        path: path.clone(),
        details,
    })
}

pub fn write_description(description: &RDescription) -> Result<(), DescriptionError> {
    let mut contents = format_description_for_write(description);
    contents.push('\n');
    let path = description_path()?;
    fs::write(&path, contents).map_err(|source| DescriptionError::WriteFailed { path, source })
}

pub fn init_description() -> Result<String, DescriptionError> {
    let path = new_project_description_path()?;
    if path.exists() {
        return Err(DescriptionError::AlreadyExists { path });
    }

    let package_name = package_name_from_description_path(&path)?;
    let description = initial_description(&package_name);

    let mut contents = format_description_for_write(&description);
    contents.push('\n');
    fs::write(&path, contents).map_err(|source| DescriptionError::WriteFailed {
        path: path.clone(),
        source,
    })?;

    Ok(path.display().to_string())
}

fn format_description_for_write(description: &RDescription) -> String {
    let paragraph = paragraph_for_write(description);
    paragraph
        .iter()
        .map(|(field, value)| format_description_field(field, value))
        .collect::<Vec<_>>()
        .join("\n")
}

fn paragraph_for_write(description: &RDescription) -> Paragraph {
    let mut paragraph = description.paragraph.clone();
    set_dependency_field(&mut paragraph, "Depends", &description.depends);
    set_dependency_field(&mut paragraph, "Imports", &description.imports);
    set_dependency_field(&mut paragraph, "Suggests", &description.suggests);
    set_dependency_field(&mut paragraph, "Enhances", &description.enhances);
    set_dependency_field(&mut paragraph, "LinkingTo", &description.linking_to);

    if description.additional_repositories.is_empty() {
        paragraph.remove("Additional_repositories");
    } else {
        paragraph.set_with_field_order(
            "Additional_repositories",
            &description.additional_repositories.join(",\n"),
            DESCRIPTION_FIELD_ORDER,
        );
    }

    paragraph
}

fn set_dependency_field(
    paragraph: &mut Paragraph,
    field_name: &'static str,
    dependencies: &BTreeSet<DescriptionDependency>,
) {
    if dependencies.is_empty() {
        paragraph.remove(field_name);
        return;
    }

    let value = dependencies
        .iter()
        .map(ToString::to_string)
        .collect::<Vec<_>>()
        .join(", ");
    paragraph.set_with_field_order(field_name, &value, DESCRIPTION_FIELD_ORDER);
}

fn parse_stored_dependency_field(
    paragraph: &Paragraph,
    field_name: &'static str,
) -> Result<BTreeSet<DescriptionDependency>, String> {
    match paragraph.get(field_name) {
        Some(contents) => parse_dependency_field(contents)
            .map(|dependencies| dependencies.into_iter().collect())
            .map_err(|details| format!("failed to parse {field_name}: {details}")),
        None => Ok(BTreeSet::new()),
    }
}

fn format_description_field(field: &str, value: &str) -> String {
    const MULTILINE_RELATION_FIELDS: &[&str] =
        &["Imports", "Depends", "Suggests", "Enhances", "LinkingTo"];

    if MULTILINE_RELATION_FIELDS.contains(&field) {
        let entries = parse_dependency_field(value)
            .expect("stored dependency field should parse")
            .into_iter()
            .collect::<BTreeSet<_>>();
        if entries.is_empty() {
            return format!("{field}:");
        }

        let mut lines = vec![format!("{field}:")];
        for (index, entry) in entries.iter().enumerate() {
            let suffix = if index + 1 < entries.len() { "," } else { "" };
            lines.push(format!("    {entry}{suffix}"));
        }
        return lines.join("\n");
    }

    if field == "Additional_repositories" {
        let entries = split_repository_entries(value);
        if entries.is_empty() {
            return format!("{field}:");
        }

        if entries.len() == 1 {
            return format!("{field}: {}", entries[0]);
        }

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

pub fn relation_with_constraint(
    package: &str,
    constraint: &str,
) -> Result<DescriptionDependency, String> {
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

pub fn resolution_root_from_relation(relation: &DescriptionDependency) -> ResolutionRoot {
    let constraint = relation.version.as_ref().map_or_else(
        || "*".to_string(),
        |(operator, version)| format!("{} {version}", relation_operator(operator)),
    );

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
    RDescription::from_paragraph(paragraph).expect("initial DESCRIPTION should parse")
}

fn package_name_from_description_path(path: &Path) -> Result<String, DescriptionError> {
    let directory_name = path
        .parent()
        .and_then(Path::file_name)
        .and_then(|name| name.to_str())
        .ok_or_else(|| DescriptionError::PackageNameFailed {
            details: "failed to derive package name from DESCRIPTION path".to_string(),
        })?;

    sanitize_package_name(directory_name)
}

fn sanitize_package_name(directory_name: &str) -> Result<String, DescriptionError> {
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
        return Err(DescriptionError::PackageNameFailed {
            details: "current directory does not produce a valid package name".to_string(),
        });
    };

    if !first.is_ascii_alphabetic() {
        return Err(DescriptionError::PackageNameFailed {
            details: "package name must start with a letter".to_string(),
        });
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
        DescriptionDependency, RDescription, format_description_for_write, parse_constraint,
        relation_with_constraint, resolution_root_from_relation, sanitize_package_name,
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
            sanitize_package_name("123pkg").unwrap_err().to_string(),
            "failed to derive package name for DESCRIPTION: package name must start with a letter"
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

        description.imports.extend([
            relation_with_constraint("dplyr", ">= 1.1.4").unwrap(),
            relation_with_constraint("dplyr", "< 2.0.0").unwrap(),
        ]);

        assert_eq!(
            description
                .imports
                .iter()
                .map(ToString::to_string)
                .collect::<Vec<_>>(),
            vec!["dplyr (>= 1.1.4)", "dplyr (< 2.0.0)"]
        );
    }

    #[test]
    fn detects_existing_dependency_in_imports_or_depends() {
        let description = RDescription::from_str(
            "Package: testpkg\nVersion: 0.1.0\nTitle: Test Package\nDescription: Test package for unit tests.\nLicense: MIT\nImports: digest\nDepends: R (>= 4.3), cli\n",
        )
        .expect("description should parse");

        let dependencies = description
            .imports
            .iter()
            .chain(&description.depends)
            .map(|dependency| dependency.name.as_str())
            .collect::<Vec<_>>();
        assert!(dependencies.contains(&"digest"));
        assert!(dependencies.contains(&"cli"));
        assert!(!dependencies.contains(&"jsonlite"));
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
        let mut description = RDescription::from_str(
            "Package: testpkg\nVersion: 0.1.0\nTitle: Test Package\nDescription: Test package for unit tests.\nLicense: MIT\nImports: rbibutils (> 2.4)\n",
        )
        .expect("description should parse");
        description.imports.insert(DescriptionDependency {
            name: "digest".to_string(),
            version: None,
        });

        let formatted = format_description_for_write(&description);

        assert!(formatted.contains("rbibutils (> 2.4)"));
    }

    #[test]
    fn formats_relationship_fields_as_multiline() {
        let mut description = RDescription::from_str(
            "Package: testpkg\nVersion: 0.1.0\nTitle: Test Package\nDescription: Test package for unit tests.\nLicense: MIT\nImports: cli (>= 3.6.0), digest\nDepends: R (>= 4.3), jsonlite\n",
        )
        .expect("description should parse");
        description.imports.insert(DescriptionDependency {
            name: "withr".to_string(),
            version: None,
        });
        description
            .depends
            .retain(|dependency| dependency.name != "jsonlite");

        let formatted = format_description_for_write(&description);

        assert!(formatted.contains("Imports:\n    cli (>= 3.6.0),\n    digest,\n    withr"));
        assert!(formatted.contains("Depends:\n    R (>= 4.3)"));
    }

    #[test]
    fn formats_bounded_dependencies_one_per_line() {
        let mut description = RDescription::from_str(
            "Package: testpkg\nVersion: 0.1.0\nTitle: Test Package\nDescription: Test package for unit tests.\nLicense: MIT\n",
        )
        .expect("description should parse");

        description.imports.extend([
            relation_with_constraint("dplyr", ">= 1.1.4").unwrap(),
            relation_with_constraint("dplyr", "< 2.0.0").unwrap(),
        ]);

        let formatted = format_description_for_write(&description);

        assert!(formatted.contains("Imports:\n    dplyr (>= 1.1.4),\n    dplyr (< 2.0.0)"));
    }

    #[test]
    fn sorts_dependency_entries_within_each_field() {
        let mut description = RDescription::from_str(
            "Package: testpkg\nVersion: 0.1.0\nTitle: Test Package\nDescription: Test package for unit tests.\nLicense: MIT\nImports: jsonlite, AzureAuth, cli\nDepends: zlib, R (>= 4.3), base\n",
        )
        .expect("description should parse");
        description.imports.insert(DescriptionDependency {
            name: "digest".to_string(),
            version: None,
        });
        description
            .depends
            .retain(|dependency| dependency.name != "zlib");

        let formatted = format_description_for_write(&description);

        assert!(
            formatted.contains("Imports:\n    AzureAuth,\n    cli,\n    digest,\n    jsonlite")
        );
        assert!(formatted.contains("Depends:\n    base,\n    R (>= 4.3)"));
    }

    #[test]
    fn normalizes_strict_relation_operators_for_resolution_roots() {
        let description = RDescription::from_str(
            "Package: testpkg\nVersion: 0.1.0\nTitle: Test Package\nDescription: Test package for unit tests.\nLicense: MIT\nImports: AzureAuth (<< 2.0.0), httr2 (>> 1.0.0)\n",
        )
        .expect("description should parse");

        assert_eq!(
            description
                .imports
                .iter()
                .map(resolution_root_from_relation)
                .collect::<Vec<_>>(),
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
        let repositories = &description.additional_repositories;

        assert_eq!(
            repositories,
            &vec![
                "https://one.example/repo".to_string(),
                "https://two.example/repo".to_string()
            ]
        );
    }

    #[test]
    fn formats_additional_repositories_as_multiline_field() {
        let mut description =
            RDescription::from_str("Package: testpkg\n").expect("description should parse");
        description.additional_repositories = vec![
            "https://one.example/repo".to_string(),
            "https://two.example/repo".to_string(),
        ];

        assert!(format_description_for_write(&description).contains(
            "Additional_repositories:\n    https://one.example/repo,\n    https://two.example/repo"
        ));
    }

    #[test]
    fn preserves_custom_fields_when_known_fields_are_edited() {
        let mut description = RDescription::from_str(
            "Package: testpkg\nX-Customer-Field: keep me\nImports: digest\n",
        )
        .expect("description should parse");

        description.imports.insert(DescriptionDependency {
            name: "cli".to_string(),
            version: None,
        });

        let formatted = format_description_for_write(&description);

        assert!(formatted.contains("X-Customer-Field: keep me"));
        assert!(formatted.contains("Imports:\n    cli,\n    digest"));
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
