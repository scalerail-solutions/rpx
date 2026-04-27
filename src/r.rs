use std::{
    collections::BTreeMap,
    fs,
    path::{Path, PathBuf},
    process::Command,
    sync::OnceLock,
    time::{SystemTime, UNIX_EPOCH},
};

use crate::project::project_library_path;

#[derive(Debug)]
pub struct InstalledPackage {
    pub package: String,
    pub version: String,
}

#[derive(Debug)]
pub struct InstallFailure {
    pub exit_code: Option<i32>,
    pub log_path: PathBuf,
    pub summary: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuntimeInfo {
    pub version: String,
    pub platform: String,
    pub pkg_type: String,
}

static RUNTIME_INFO: OnceLock<RuntimeInfo> = OnceLock::new();
static BASE_PACKAGES: OnceLock<Vec<String>> = OnceLock::new();

pub fn project_command(program: impl AsRef<str>) -> Command {
    library_command(program, &project_library_path(), &[])
}

pub fn library_command(
    program: impl AsRef<str>,
    primary_library: &Path,
    additional_libraries: &[PathBuf],
) -> Command {
    let mut command = Command::new(program.as_ref());
    let primary_library = primary_library
        .to_str()
        .expect("library path should be valid utf-8");
    command.env("R_LIBS_USER", primary_library);

    if !additional_libraries.is_empty() {
        let joined = additional_libraries
            .iter()
            .map(|path| path.to_str().expect("library path should be valid utf-8"))
            .collect::<Vec<_>>()
            .join(":");
        command.env("R_LIBS", joined);
    }

    command
}

pub fn install_local_package(
    artifact_path: &Path,
    package: &str,
    version: &str,
    pkg_type: &str,
    target_library: &Path,
    dependency_libraries: &[PathBuf],
) -> Result<(), InstallFailure> {
    let target_library_path = target_library.to_path_buf();
    let artifact_path = artifact_path
        .to_str()
        .expect("artifact path should be valid utf-8");
    let target_library = target_library
        .to_str()
        .expect("target library path should be valid utf-8");
    let expression = concat!(
        "install.packages('%ARTIFACT%', repos = NULL, type = '%TYPE%', lib = '%LIB%');",
        "packages <- installed.packages(lib.loc = '%LIB%');",
        "if (!('%PACKAGE%' %in% rownames(packages))) stop('Expected package %PACKAGE% to be installed');",
        "installed_version <- packages['%PACKAGE%', 'Version'];",
        "if (installed_version != '%VERSION%') stop(sprintf('Installed %s version %s, expected %s', '%PACKAGE%', installed_version, '%VERSION%'))"
    )
    .replace("%ARTIFACT%", &escape_r_string(artifact_path))
    .replace("%TYPE%", &escape_r_string(pkg_type))
    .replace("%LIB%", &escape_r_string(target_library))
    .replace("%PACKAGE%", &escape_r_string(package))
    .replace("%VERSION%", &escape_r_string(version));

    let output = library_command("Rscript", &target_library_path, dependency_libraries)
        .arg("-e")
        .arg(expression)
        .output()
        .expect("failed to run Rscript");

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
    fs::write(&log_path, contents).expect("failed to write install log");

    let summary = summarize_install_output(&output.stdout, &output.stderr);

    Err(InstallFailure {
        exit_code: output.status.code(),
        log_path,
        summary,
    })
}

pub fn runtime_info() -> RuntimeInfo {
    RUNTIME_INFO.get_or_init(fetch_runtime_info).clone()
}

pub fn base_packages() -> Vec<String> {
    BASE_PACKAGES.get_or_init(fetch_base_packages).clone()
}

pub fn installed_packages() -> Vec<InstalledPackage> {
    let expression = concat!(
        "packages <- installed.packages(lib.loc = .libPaths()[1]);",
        "if (nrow(packages) == 0) quit(save = 'no', status = 0);",
        "write.table(packages[, c('Package', 'Version'), drop = FALSE], ",
        "sep = '\t', row.names = FALSE, col.names = TRUE, quote = FALSE)"
    );

    let output = project_command("Rscript")
        .arg("-e")
        .arg(expression)
        .output()
        .expect("failed to run Rscript");

    crate::exit_with_status(output.status.code());

    parse_installed_packages(&String::from_utf8_lossy(&output.stdout))
}

pub fn installed_packages_by_name() -> BTreeMap<String, InstalledPackage> {
    installed_packages()
        .into_iter()
        .map(|package| (package.package.clone(), package))
        .collect()
}

pub fn remove_installed_packages(packages: &[String]) {
    if packages.is_empty() {
        return;
    }

    let package_expression = packages
        .iter()
        .map(|package| format!("'{package}'"))
        .collect::<Vec<_>>()
        .join(", ");

    let output = project_command("Rscript")
        .arg("-e")
        .arg(format!("remove.packages(c({package_expression}))"))
        .output()
        .expect("failed to run Rscript");

    if !output.status.success() {
        let stdout = String::from_utf8_lossy(&output.stdout);
        let stderr = String::from_utf8_lossy(&output.stderr);
        if !stdout.trim().is_empty() {
            eprintln!("{stdout}");
        }
        if !stderr.trim().is_empty() {
            eprintln!("{stderr}");
        }
        crate::exit_with_status(output.status.code());
    }

    for package in packages {
        remove_installed_package_dir(package);
    }
}

pub fn remove_installed_package_dir(package: &str) {
    let package_dir = project_library_path().join(package);

    if package_dir.exists() {
        fs::remove_dir_all(package_dir).expect("failed to remove package directory");
    }
}

fn parse_installed_packages(output: &str) -> Vec<InstalledPackage> {
    output
        .lines()
        .skip(1)
        .filter(|line| !line.trim().is_empty())
        .filter_map(|line| {
            let mut parts = line.split('\t');
            let package = parts.next()?.trim().to_string();
            let version = parts.next()?.trim().to_string();

            Some(InstalledPackage { package, version })
        })
        .collect()
}

fn escape_r_string(value: &str) -> String {
    value.replace('\\', "\\\\").replace('\'', "\\'")
}

fn fetch_runtime_info() -> RuntimeInfo {
    let output = Command::new("Rscript")
        .arg("-e")
        .arg("cat(as.character(getRversion()), '\t', R.version$platform, '\t', .Platform$pkgType, sep = '')")
        .output()
        .expect("failed to run Rscript");

    crate::exit_with_status(output.status.code());

    let stdout = String::from_utf8_lossy(&output.stdout);
    let mut parts = stdout.trim().splitn(3, '\t');
    RuntimeInfo {
        version: parts
            .next()
            .expect("R version should be present")
            .to_string(),
        platform: parts
            .next()
            .expect("R platform should be present")
            .to_string(),
        pkg_type: parts
            .next()
            .expect("R package type should be present")
            .to_string(),
    }
}

fn fetch_base_packages() -> Vec<String> {
    let output = Command::new("Rscript")
        .arg("-e")
        .arg("writeLines(rownames(installed.packages(priority = 'base')))")
        .output()
        .expect("failed to run Rscript");

    crate::exit_with_status(output.status.code());

    String::from_utf8_lossy(&output.stdout)
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .map(ToString::to_string)
        .collect()
}

fn summarize_install_output(stdout: &[u8], stderr: &[u8]) -> String {
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
