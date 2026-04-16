use r_description::{Version, lossy::RDescription, lossy::Relation, lossy::Relations};
use std::{collections::BTreeSet, fs, str::FromStr};

use crate::project::{current_description_path, description_path};
use crate::registry::ClosureRoot;

pub trait DescriptionExt {
    fn add_to_imports(&mut self, package: &str);
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
    let mut contents = format!("{}", project.description).trim_end().to_string();
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

    let mut contents = format!("{}", description.description)
        .trim_end()
        .to_string();
    contents.push('\n');
    fs::write(&path, contents).map_err(|error| error.to_string())?;

    Ok(path.display().to_string())
}

impl DescriptionExt for RDescription {
    fn add_to_imports(&mut self, package: &str) {
        let mut imports = self.imports.clone().unwrap_or_default();

        let already_present_in_depends = self
            .depends
            .as_ref()
            .map(|depends| depends.iter().any(|entry| entry.name == package))
            .unwrap_or(false);

        if imports.iter().any(|entry| entry.name == package) || already_present_in_depends {
            return;
        }

        imports.0.push(Relation {
            name: package.to_string(),
            version: None,
        });
        self.imports = Some(imports);
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

fn closure_root_from_relation(relation: &Relation) -> ClosureRoot {
    let constraint = relation
        .version
        .as_ref()
        .map(|(operator, version)| format!("{operator} {version}"))
        .unwrap_or_else(|| "*".to_string());

    ClosureRoot {
        name: relation.name.clone(),
        constraint,
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
    use super::{sanitize_package_name, title_from_package_name};

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
}
