use r_description::lossy::{RDescription, Relation, Relations};
use std::{collections::BTreeSet, fs, str::FromStr};

use crate::project::description_path;
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
