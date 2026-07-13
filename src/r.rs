use std::{
    fs,
    path::{Path, PathBuf},
    time::{SystemTime, UNIX_EPOCH},
};

use miette::Diagnostic;
use thiserror::Error;
use tokio::{process::Command, sync::OnceCell};

use crate::project::project_library_path;

#[derive(Debug)]
pub struct InstalledPackage {
    pub package: String,
    pub version: String,
}

#[derive(Debug, Error, Diagnostic)]
pub enum RscriptError {
    #[error("failed to run Rscript while {operation}: {source}")]
    #[diagnostic(code(rpx::r::launch_failed))]
    LaunchFailed {
        operation: String,
        #[source]
        source: std::io::Error,
    },

    #[error("Rscript failed while {operation}: {summary}")]
    #[diagnostic(code(rpx::r::failed))]
    Failed { operation: String, summary: String },

    #[error("Rscript failed while {operation}: {summary}")]
    #[diagnostic(
        code(rpx::r::failed),
        help("Inspect the full R output at {log_path:?}.")
    )]
    FailedWithLog {
        operation: String,
        summary: String,
        log_path: PathBuf,
    },

    #[error("Rscript returned invalid output while {operation}: {details}")]
    #[diagnostic(code(rpx::r::invalid_output))]
    InvalidOutput { operation: String, details: String },

    #[error("failed to write Rscript output log at {}: {source}", path.display())]
    #[diagnostic(code(rpx::r::install_log_write_failed))]
    InstallLogWriteFailed {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },

    #[error("{label} path is not valid UTF-8: {}", path.display())]
    #[diagnostic(code(rpx::r::non_utf8_path))]
    NonUtf8Path { label: &'static str, path: PathBuf },
}

#[derive(Debug, Error, Diagnostic)]
pub enum RLibraryError {
    #[error("failed to remove package directory {}: {source}", path.display())]
    #[diagnostic(code(rpx::r::library_remove_failed))]
    PackageRemoveFailed {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
}

pub(crate) trait RVirtualEnv {
    fn with_venv(program: impl AsRef<str>) -> Self;
}

impl RVirtualEnv for tokio::process::Command {
    fn with_venv(program: impl AsRef<str>) -> Self {
        let mut command = tokio::process::Command::new(program.as_ref());
        command.env("R_LIBS_USER", project_library_path());
        command
    }
}

static BASE_PACKAGES: OnceCell<Vec<String>> = OnceCell::const_new();

pub async fn install_local_package(
    artifact_path: &Path,
    package: &str,
    version: &str,
    pkg_type: &str,
    target_library: &Path,
) -> Result<(), RscriptError> {
    let artifact_path = artifact_path
        .to_str()
        .ok_or_else(|| RscriptError::NonUtf8Path {
            label: "artifact",
            path: artifact_path.to_path_buf(),
        })?;
    let target_library = target_library
        .to_str()
        .ok_or_else(|| RscriptError::NonUtf8Path {
            label: "target library",
            path: target_library.to_path_buf(),
        })?;

    let expression = concat!(
        "install.packages('%ARTIFACT%', repos = NULL, type = '%TYPE%', lib = '%LIB%');",
        "packages <- installed.packages(lib.loc = '%LIB%');",
        "if (!('%PACKAGE%' %in% rownames(packages))) stop('Expected package %PACKAGE% to be installed');",
        "installed_version <- packages['%PACKAGE%', 'Version'];",
        "if (installed_version != '%VERSION%') warning(sprintf('Installed %s version %s, expected %s', '%PACKAGE%', installed_version, '%VERSION%'))"
    )
    .replace("%ARTIFACT%", &escape_r_string(artifact_path))
    .replace("%TYPE%", &escape_r_string(pkg_type))
    .replace("%LIB%", &escape_r_string(target_library))
    .replace("%PACKAGE%", &escape_r_string(package))
    .replace("%VERSION%", &escape_r_string(version));

    let operation = format!("install package {package}@{version}");
    let output = rscript_output(&operation, expression, true).await?;

    if output.status.success() {
        return Ok(());
    }

    let log_path = install_log_path();
    let mut contents = String::new();

    contents.push_str("# stdout\n");
    contents.push_str(&String::from_utf8_lossy(&output.stdout));
    if !contents.ends_with('\n') {
        contents.push('\n');
    }

    contents.push_str("# stderr\n");
    contents.push_str(&String::from_utf8_lossy(&output.stderr));

    fs::write(&log_path, contents).map_err(|source| RscriptError::InstallLogWriteFailed {
        path: log_path.clone(),
        source,
    })?;

    let summary = summarize_rscript_output(&output.stdout, &output.stderr);

    Err(RscriptError::FailedWithLog {
        operation,
        log_path,
        summary,
    })
}

pub async fn base_packages() -> Result<Vec<String>, RscriptError> {
    Ok(BASE_PACKAGES
        .get_or_try_init(fetch_base_packages)
        .await?
        .clone())
}

pub async fn installed_packages() -> Result<Vec<InstalledPackage>, RscriptError> {
    let expression = concat!(
        "packages <- installed.packages(lib.loc = .libPaths()[1]);",
        "if (nrow(packages) == 0) quit(save = 'no', status = 0);",
        "write.table(packages[, c('Package', 'Version'), drop = FALSE], ",
        "sep = '\t', row.names = FALSE, col.names = TRUE, quote = FALSE)"
    );

    let operation = "list installed packages";
    let output = rscript_output(operation, expression, true).await?;
    rscript_success(operation, &output)?;
    parse_installed_packages(&String::from_utf8_lossy(&output.stdout), operation)
}

pub fn remove_packages_from_venv(packages: &[String]) -> Result<(), RLibraryError> {
    packages
        .iter()
        .try_for_each(|package| remove_package_from_venv(package))
}

pub fn remove_package_from_venv(package: &str) -> Result<(), RLibraryError> {
    let package_dir = project_library_path().join(package);

    if !package_dir.exists() {
        return Ok(());
    }

    std::fs::remove_dir_all(&package_dir).map_err(|source| RLibraryError::PackageRemoveFailed {
        path: package_dir,
        source,
    })
}

fn parse_installed_packages(
    output: &str,
    operation: &str,
) -> Result<Vec<InstalledPackage>, RscriptError> {
    output
        .lines()
        .skip(1)
        .filter(|line| !line.trim().is_empty())
        .map(|line| {
            let mut parts = line.split('\t');
            let package = parts.next().map(str::trim).filter(|part| !part.is_empty());
            let version = parts.next().map(str::trim).filter(|part| !part.is_empty());

            match (package, version, parts.next()) {
                (Some(package), Some(version), None) => Ok(InstalledPackage {
                    package: package.to_string(),
                    version: version.to_string(),
                }),
                _ => Err(RscriptError::InvalidOutput {
                    operation: operation.to_string(),
                    details: format!("expected tab-separated package and version, got {line:?}"),
                }),
            }
        })
        .collect()
}

fn escape_r_string(value: &str) -> String {
    value.replace('\\', "\\\\").replace('\'', "\\'")
}

pub async fn r_version() -> Result<String, RscriptError> {
    let operation = "query R version";
    let output = rscript_output(operation, "cat(as.character(getRversion()))", false).await?;
    rscript_success(operation, &output)?;

    let version = String::from_utf8_lossy(&output.stdout).trim().to_string();

    if version.is_empty() {
        return Err(RscriptError::InvalidOutput {
            operation: operation.to_string(),
            details: "Rscript returned empty output".to_string(),
        });
    }

    Ok(version)
}

async fn fetch_base_packages() -> Result<Vec<String>, RscriptError> {
    let operation = "list R base packages";
    let output = rscript_output(
        operation,
        "writeLines(rownames(installed.packages(priority = 'base')))",
        true,
    )
    .await?;
    rscript_success(operation, &output)?;

    Ok(String::from_utf8_lossy(&output.stdout)
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .map(ToString::to_string)
        .collect())
}

async fn rscript_output(
    operation: &str,
    expression: impl AsRef<str>,
    with_venv: bool,
) -> Result<std::process::Output, RscriptError> {
    let mut command = if with_venv {
        Command::with_venv("Rscript")
    } else {
        Command::new("Rscript")
    };
    command.arg("-e").arg(expression.as_ref());
    command
        .output()
        .await
        .map_err(|source| RscriptError::LaunchFailed {
            operation: operation.to_string(),
            source,
        })
}

fn rscript_success(operation: &str, output: &std::process::Output) -> Result<(), RscriptError> {
    if output.status.success() {
        return Ok(());
    }

    Err(RscriptError::Failed {
        operation: operation.to_string(),
        summary: summarize_rscript_output(&output.stdout, &output.stderr),
    })
}

fn summarize_rscript_output(stdout: &[u8], stderr: &[u8]) -> String {
    let combined = [
        String::from_utf8_lossy(stderr),
        String::from_utf8_lossy(stdout),
    ]
    .join("\n");
    let lines = combined
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .collect::<Vec<_>>();

    lines
        .iter()
        .rev()
        .find(|line| {
            line.contains("ERROR")
                || line.contains("error:")
                || line.contains("installation of package")
                || line.contains("failed")
        })
        .copied()
        .or_else(|| lines.last().copied())
        .unwrap_or("package installation failed")
        .to_string()
}

fn install_log_path() -> PathBuf {
    let unique = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time should be after unix epoch")
        .as_nanos();
    std::env::temp_dir().join(format!("rpx-install-{}-{unique}.log", std::process::id()))
}
