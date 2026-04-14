use r_description::lossy::{RDescription, Relation, Relations};
use std::{collections::BTreeSet, fs, str::FromStr};

use crate::project::description_path;

pub trait DescriptionExt {
    fn add_to_imports(&mut self, package: &str);
    fn remove_from_field(&mut self, field_name: &str, package: &str);
    fn requirements(&self) -> Vec<String>;
}

pub fn read_description() -> Result<RDescription, String> {
    let path = description_path();
    let contents = fs::read_to_string(&path).map_err(|error| error.to_string())?;
    let description = RDescription::from_str(&contents).map_err(|error| error.to_string())?;

    if description.name.trim().is_empty() {
        return Err("DESCRIPTION is missing Package".to_string());
    }

    Ok(description)
}

pub fn write_description(description: &RDescription) {
    fs::write(description_path(), format!("{description}")).expect("failed to write DESCRIPTION");
}

impl DescriptionExt for RDescription {
    fn add_to_imports(&mut self, package: &str) {
        let mut imports = self.imports.clone().unwrap_or_default();

        if imports.iter().any(|entry| entry.name == package) {
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
