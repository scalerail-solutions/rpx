use r_description::lossy::{RDescription, Relation, Relations};
use std::{collections::BTreeSet, fs, str::FromStr};

use crate::project::description_path;

pub trait DescriptionExt {
    fn add_to_imports(&mut self, package: &str);
    fn remove_from_field(&mut self, field_name: &str, package: &str);
    fn requirements(&self) -> Vec<String>;
}

#[derive(Debug)]
pub struct ProjectDescription {
    pub description: RDescription,
    pub additional_repositories: Vec<String>,
}

pub fn read_description() -> Result<ProjectDescription, String> {
    let path = description_path();
    let contents = fs::read_to_string(&path).map_err(|error| error.to_string())?;
    let description = RDescription::from_str(&contents).map_err(|error| error.to_string())?;

    if description.name.trim().is_empty() {
        return Err("DESCRIPTION is missing Package".to_string());
    }

    Ok(ProjectDescription {
        description,
        additional_repositories: parse_additional_repositories(&contents),
    })
}

pub fn write_description(project: &ProjectDescription) {
    let mut contents = format!("{}", project.description).trim_end().to_string();

    if !project.additional_repositories.is_empty() {
        contents.push_str("\nAdditional_repositories: ");
        contents.push_str(&project.additional_repositories.join(",\n "));
    }

    contents.push('\n');
    fs::write(description_path(), contents).expect("failed to write DESCRIPTION");
}

impl ProjectDescription {
    pub fn add_repositories(&mut self, repositories: &[String]) {
        for repository in repositories {
            if !self.additional_repositories.contains(repository) {
                self.additional_repositories.push(repository.clone());
            }
        }
    }

    pub fn remove_repositories(&mut self, repositories: &[String]) {
        self.additional_repositories
            .retain(|repository| !repositories.contains(repository));
    }
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

fn parse_additional_repositories(contents: &str) -> Vec<String> {
    let mut value = None::<String>;
    let mut current_field = None::<String>;

    for line in contents.lines() {
        if line.starts_with(' ') || line.starts_with('\t') {
            if current_field.as_deref() == Some("Additional_repositories") {
                let current = value.get_or_insert_with(String::new);
                if !current.is_empty() {
                    current.push('\n');
                }
                current.push_str(line.trim());
            }
            continue;
        }

        if let Some((field, rest)) = line.split_once(':') {
            current_field = Some(field.trim().to_string());
            if field.trim() == "Additional_repositories" {
                value = Some(rest.trim().to_string());
            }
        }
    }

    value
        .unwrap_or_default()
        .split([',', '\n'])
        .map(str::trim)
        .filter(|repository| !repository.is_empty())
        .map(ToOwned::to_owned)
        .collect()
}
