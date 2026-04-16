use crate::project::{LOCKFILE_NAME, lockfile_path};
use serde::{Deserialize, Serialize};
use std::{collections::BTreeMap, fs};

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct Lockfile {
    pub version: u32,
    pub requirements: Vec<String>,
    pub registry: String,
    pub packages: BTreeMap<String, LockedPackage>,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct LockedPackage {
    pub package: String,
    pub version: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source_url: Option<String>,
}

pub fn read_lockfile() -> Result<Lockfile, String> {
    read_lockfile_optional()?.ok_or_else(|| format!("{LOCKFILE_NAME} not found in project root"))
}

pub fn read_lockfile_optional() -> Result<Option<Lockfile>, String> {
    let path = lockfile_path();

    if !path.exists() {
        return Ok(None);
    }

    let contents = fs::read_to_string(&path).map_err(|error| error.to_string())?;
    let lockfile = serde_json::from_str(&contents).map_err(|error| error.to_string())?;
    Ok(Some(lockfile))
}

pub fn write_lockfile(lockfile: Lockfile) {
    let contents = serde_json::to_string_pretty(&lockfile).expect("failed to serialize lockfile");
    fs::write(lockfile_path(), format!("{contents}\n")).expect("failed to write lockfile");
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::{LockedPackage, Lockfile};

    #[test]
    fn serializes_new_registry_lockfile_shape() {
        let lockfile = Lockfile {
            version: 2,
            requirements: vec!["digest".to_string()],
            registry: "https://api.rrepo.org".to_string(),
            packages: BTreeMap::from([(
                "digest".to_string(),
                LockedPackage {
                    package: "digest".to_string(),
                    version: "0.6.37".to_string(),
                    source: Some("registry".to_string()),
                    source_url: Some(
                        "https://api.rrepo.org/packages/digest/versions/0.6.37/source".to_string(),
                    ),
                },
            )]),
        };

        let json = serde_json::to_string_pretty(&lockfile).expect("lockfile should serialize");

        assert!(json.contains("\"registry\": \"https://api.rrepo.org\""));
        assert!(json.contains("\"source_url\""));
        assert!(!json.contains("\"repositories\""));
        assert!(!json.contains("\"repository\""));
    }

    #[test]
    fn omits_optional_source_url_when_missing() {
        let package = LockedPackage {
            package: "digest".to_string(),
            version: "0.6.37".to_string(),
            source: None,
            source_url: None,
        };

        let json = serde_json::to_string(&package).expect("package should serialize");

        assert!(!json.contains("source_url"));
        assert!(!json.contains("source"));
    }
}
