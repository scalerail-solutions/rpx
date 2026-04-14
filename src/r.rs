use std::{fs, process::Command};

use crate::{lockfile::LockedPackage, project::project_library_path};

#[derive(Debug)]
pub struct InstalledPackage {
    pub package: String,
    pub version: String,
    pub repository: Option<String>,
}

pub fn project_command(program: impl AsRef<str>) -> Command {
    let mut command = Command::new(program.as_ref());
    command.env("R_LIBS_USER", project_library_path());
    command
}

pub fn install_requirements(requirements: &[String]) {
    if requirements.is_empty() {
        return;
    }

    let requirements = requirements
        .iter()
        .map(|package| format!("'{package}'"))
        .collect::<Vec<_>>()
        .join(", ");
    let expression = format!("install.packages(c({requirements}))");

    let status = project_command("Rscript")
        .arg("-e")
        .arg(expression)
        .status()
        .expect("failed to run Rscript");

    crate::exit_with_status(status.code());
}

pub fn installed_packages() -> Vec<InstalledPackage> {
    let expression = concat!(
        "packages <- installed.packages(lib.loc = .libPaths()[1], fields = 'Repository');",
        "if (nrow(packages) == 0) quit(save = 'no', status = 0);",
        "write.table(packages[, c('Package', 'Version', 'Repository'), drop = FALSE], ",
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

pub fn remove_installed_package_dir(package: &str) {
    let package_dir = project_library_path().join(package);

    if package_dir.exists() {
        fs::remove_dir_all(package_dir).expect("failed to remove package directory");
    }
}

pub fn to_locked_package(package: InstalledPackage) -> LockedPackage {
    let source = package
        .repository
        .as_ref()
        .map(|_| "repository".to_string());

    LockedPackage {
        package: package.package,
        version: package.version,
        source,
        repository: package.repository,
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
            let repository = parts
                .next()
                .map(str::trim)
                .filter(|value| !value.is_empty() && *value != "NA")
                .map(ToOwned::to_owned);

            Some(InstalledPackage {
                package,
                version,
                repository,
            })
        })
        .collect()
}
