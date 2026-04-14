use crate::project::{lockfile_path, LOCKFILE_NAME};
use serde::{Deserialize, Serialize};
use std::{collections::BTreeMap, fs};

#[derive(Debug, Serialize, Deserialize)]
pub struct Lockfile {
    pub version: u32,
    pub requirements: Vec<String>,
    #[serde(default)]
    pub repositories: Vec<String>,
    pub packages: BTreeMap<String, LockedPackage>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct LockedPackage {
    pub package: String,
    pub version: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub repository: Option<String>,
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
