pub const DEFAULT_REPOSITORY_URL: &str = "https://cloud.r-project.org";
pub const POSIT_REPOSITORY_URL: &str = "https://packagemanager.posit.co/cran/latest";

const BIOCONDUCTOR_REPOSITORIES: [&str; 3] = [
    "https://bioconductor.org/packages/release/bioc",
    "https://bioconductor.org/packages/release/data/annotation",
    "https://bioconductor.org/packages/release/data/experiment",
];

pub fn expand_repo_spec(spec: &str) -> Result<Vec<String>, String> {
    match spec.to_ascii_lowercase().as_str() {
        "bioconductor" => Ok(BIOCONDUCTOR_REPOSITORIES
            .iter()
            .map(|url| url.to_string())
            .collect()),
        "posit" => Ok(vec![POSIT_REPOSITORY_URL.to_string()]),
        "r-forge" => Ok(vec!["https://R-Forge.R-project.org".to_string()]),
        _ if spec.contains("://") => Ok(vec![spec.to_string()]),
        _ => Err(format!("unknown repository alias: {spec}")),
    }
}

pub fn effective_repositories(additional_repositories: &[String]) -> Vec<String> {
    let mut repositories = vec![DEFAULT_REPOSITORY_URL.to_string()];

    for repository in additional_repositories {
        if !repositories.contains(repository) {
            repositories.push(repository.clone());
        }
    }

    repositories
}

pub fn alias_for_repository(repository: &str) -> Option<&'static str> {
    if BIOCONDUCTOR_REPOSITORIES.contains(&repository) {
        return Some("bioconductor");
    }

    match repository {
        POSIT_REPOSITORY_URL => Some("posit"),
        "https://R-Forge.R-project.org" => Some("r-forge"),
        _ => None,
    }
}
