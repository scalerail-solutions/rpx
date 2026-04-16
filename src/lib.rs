use clap::Parser;
use std::{collections::BTreeSet, env};

mod cli;
mod description;
mod lockfile;
mod project;
mod r;
mod registry;
mod resolver;

use cli::{Cli, Commands};
use description::{DescriptionExt, init_description, read_description, write_description};
use lockfile::{Lockfile, read_lockfile, write_lockfile};
use project::lockfile_path;
use r::{
    install_source_package, installed_packages, installed_packages_by_name, project_command,
    remove_installed_package_dir, remove_installed_packages,
};
use registry::{ClosureRequest, DEFAULT_REGISTRY_BASE_URL, DownloadedArtifact, RegistryClient};
use resolver::{ResolvedPackage, resolve_from_closure};

pub fn run() {
    let cli = Cli::parse();

    match cli.command {
        Commands::Init => cmd_init(),
        Commands::Add { package } => cmd_add(&package),
        Commands::Remove { package } => cmd_remove(&package),
        Commands::Run { command } => cmd_run(&command),
        Commands::Lock => cmd_lock(),
        Commands::Status => cmd_status(),
        Commands::Sync => cmd_sync(),
    }
}

fn cmd_init() {
    let path =
        init_description().unwrap_or_else(|error| panic!("failed to initialize project: {error}"));
    println!("Initialized project at {path}");
    println!("Next: run `rpx add <package>` or `rpx lock`");
}

fn cmd_add(package: &str) {
    if lockfile_path().exists() {
        sync_from_lockfile();
    }

    let mut project = read_description().expect("failed to read DESCRIPTION");
    project.description.add_to_imports(package);
    write_description(&project);
    lock_from_description();
    sync_from_lockfile();
}

fn cmd_remove(package: &str) {
    if lockfile_path().exists() {
        sync_from_lockfile();
    }

    let mut project = read_description().expect("failed to read DESCRIPTION");
    project.description.remove_from_field("Imports", package);
    project.description.remove_from_field("Depends", package);
    write_description(&project);

    let status = project_command("Rscript")
        .arg("-e")
        .arg(format!("remove.packages('{package}')"))
        .status()
        .expect("failed to run Rscript");

    exit_with_status(status.code());
    remove_installed_package_dir(package);
    lock_from_description();
}

fn cmd_run(command: &[String]) {
    let (program, args) = command
        .split_first()
        .expect("run command requires at least one argument");

    let status = project_command(program)
        .args(args)
        .status()
        .unwrap_or_else(|_| panic!("failed to run {program}"));

    exit_with_status(status.code());
}

fn cmd_lock() {
    lock_from_description();
}

fn cmd_sync() {
    sync_from_lockfile();
}

fn cmd_status() {
    let project = match read_description() {
        Ok(description) => description,
        Err(error) => {
            eprintln!("Status: drift");
            eprintln!("Description: {error}");
            std::process::exit(1);
        }
    };

    let lockfile = match read_lockfile() {
        Ok(lockfile) => lockfile,
        Err(error) => {
            eprintln!("Status: drift");
            eprintln!("Lockfile: {error}");
            std::process::exit(1);
        }
    };

    let manifest_requirements = project
        .description
        .requirements()
        .into_iter()
        .collect::<BTreeSet<_>>();
    let lock_requirements = lockfile
        .requirements
        .iter()
        .cloned()
        .collect::<BTreeSet<_>>();
    let installed = installed_packages();
    let installed_names = installed
        .iter()
        .map(|package| package.package.clone())
        .collect::<BTreeSet<_>>();
    let installed_versions = installed
        .iter()
        .map(|package| (package.package.clone(), package.version.clone()))
        .collect::<std::collections::BTreeMap<_, _>>();
    let locked_names = lockfile.packages.keys().cloned().collect::<BTreeSet<_>>();

    let missing_from_lockfile = manifest_requirements
        .difference(&lock_requirements)
        .cloned()
        .collect::<Vec<_>>();
    let extra_in_lockfile = lock_requirements
        .difference(&manifest_requirements)
        .cloned()
        .collect::<Vec<_>>();
    let missing_from_library = locked_names
        .difference(&installed_names)
        .cloned()
        .collect::<Vec<_>>();
    let extra_in_library = installed_names
        .difference(&locked_names)
        .cloned()
        .collect::<Vec<_>>();
    let version_mismatches = lockfile
        .packages
        .iter()
        .filter_map(|(name, package)| {
            installed_versions
                .get(name)
                .filter(|installed_version| *installed_version != &package.version)
                .map(|installed_version| {
                    format!(
                        "{name} ({installed_version} installed, {} locked)",
                        package.version
                    )
                })
        })
        .collect::<Vec<_>>();

    println!("Manifest requirements: {}", manifest_requirements.len());
    println!("Locked requirements: {}", lockfile.requirements.len());
    println!("Locked registry: {}", lockfile.registry);
    println!("Locked packages: {}", lockfile.packages.len());
    println!("Installed packages: {}", installed.len());

    if missing_from_lockfile.is_empty()
        && extra_in_lockfile.is_empty()
        && missing_from_library.is_empty()
        && extra_in_library.is_empty()
        && version_mismatches.is_empty()
    {
        println!("Status: ok");
        return;
    }

    println!("Status: drift");

    if !missing_from_lockfile.is_empty() {
        println!(
            "Missing from lockfile: {}",
            missing_from_lockfile.join(", ")
        );
    }

    if !extra_in_lockfile.is_empty() {
        println!("Extra in lockfile: {}", extra_in_lockfile.join(", "));
    }

    if !missing_from_library.is_empty() {
        println!("Missing from library: {}", missing_from_library.join(", "));
    }

    if !extra_in_library.is_empty() {
        println!("Extra in library: {}", extra_in_library.join(", "));
    }

    if !version_mismatches.is_empty() {
        println!("Version mismatch: {}", version_mismatches.join(", "));
    }

    std::process::exit(1);
}

fn lock_from_description() {
    let project = read_description().expect("failed to read DESCRIPTION");
    let requirements = project.description.requirements();
    let registry = registry_base_url();

    if requirements.is_empty() {
        write_lockfile(lockfile_from_resolution(vec![], &registry, &[]));
        return;
    }

    let request = ClosureRequest {
        roots: project.description.closure_roots(),
    };
    let client = RegistryClient::new(&registry);
    let closure = client
        .fetch_closure_with_retry(&request)
        .unwrap_or_else(|error| panic!("failed to resolve lockfile from registry: {error}"));
    let resolved = resolve_from_closure(&request, &registry::ClosureResponse::Complete(closure))
        .unwrap_or_else(|error| panic!("failed to resolve package set from closure: {error}"));

    write_lockfile(lockfile_from_resolution(
        requirements,
        client.base_url(),
        &resolved,
    ));
}

fn sync_from_lockfile() {
    let project = read_description().expect("failed to read DESCRIPTION");
    let manifest_requirements = project.description.requirements();
    let lockfile = read_lockfile().expect("failed to read lockfile");

    if manifest_requirements != lockfile.requirements {
        eprintln!("lockfile out of date; run rpx lock");
        std::process::exit(1);
    }

    let installed = installed_packages_by_name();
    let exact_reinstalls = lockfile
        .packages
        .iter()
        .filter_map(|(name, package)| match installed.get(name) {
            Some(installed_package) if installed_package.version == package.version => None,
            _ => Some((
                name.clone(),
                package.version.clone(),
                package.source_url.clone(),
            )),
        })
        .collect::<Vec<_>>();

    let client = RegistryClient::new(&lockfile.registry);

    for (name, version, source_url) in &exact_reinstalls {
        let source_url = source_url
            .as_ref()
            .unwrap_or_else(|| panic!("lockfile package {name}@{version} is missing source_url"));
        let artifact = client
            .download_source_artifact(name, version, source_url)
            .unwrap_or_else(|error| panic!("failed to download source artifact: {error}"));
        install_downloaded_artifact(artifact);
    }

    let extras = installed_packages_by_name()
        .into_keys()
        .filter(|name| !lockfile.packages.contains_key(name))
        .collect::<Vec<_>>();
    remove_installed_packages(&extras);

    let final_state = installed_packages_by_name();
    let missing = lockfile
        .packages
        .keys()
        .filter(|name| !final_state.contains_key(*name))
        .cloned()
        .collect::<Vec<_>>();
    let extras = final_state
        .keys()
        .filter(|name| !lockfile.packages.contains_key(*name))
        .cloned()
        .collect::<Vec<_>>();
    let version_mismatches = lockfile
        .packages
        .iter()
        .filter_map(|(name, package)| {
            final_state
                .get(name)
                .filter(|installed_package| installed_package.version != package.version)
                .map(|installed_package| {
                    format!(
                        "{name} ({}, expected {})",
                        installed_package.version, package.version
                    )
                })
        })
        .collect::<Vec<_>>();

    if missing.is_empty() && extras.is_empty() && version_mismatches.is_empty() {
        return;
    }

    if !missing.is_empty() {
        eprintln!("missing from library after sync: {}", missing.join(", "));
    }

    if !extras.is_empty() {
        eprintln!("extra in library after sync: {}", extras.join(", "));
    }

    if !version_mismatches.is_empty() {
        eprintln!(
            "version mismatch after sync: {}",
            version_mismatches.join(", ")
        );
    }

    std::process::exit(1);
}

pub(crate) fn exit_with_status(code: Option<i32>) {
    if code != Some(0) {
        std::process::exit(code.unwrap_or(1));
    }
}

fn registry_base_url() -> String {
    env::var("RPX_REGISTRY_BASE_URL")
        .unwrap_or_else(|_| DEFAULT_REGISTRY_BASE_URL.to_string())
        .trim_end_matches('/')
        .to_string()
}

fn lockfile_from_resolution(
    requirements: Vec<String>,
    registry: &str,
    resolved: &[ResolvedPackage],
) -> Lockfile {
    Lockfile {
        version: 2,
        requirements,
        registry: registry.to_string(),
        packages: resolved
            .iter()
            .map(|package| {
                (
                    package.name.clone(),
                    lockfile::LockedPackage {
                        package: package.name.clone(),
                        version: package.version.clone(),
                        source: Some("registry".to_string()),
                        source_url: Some(package.source_url.clone()),
                    },
                )
            })
            .collect(),
    }
}

fn install_downloaded_artifact(artifact: DownloadedArtifact) {
    install_source_package(artifact.path());
    artifact.cleanup();
}

#[cfg(test)]
mod tests {
    use super::{lockfile_from_resolution, registry_base_url};
    use crate::{description::DescriptionExt, registry::ClosureRoot, resolver::ResolvedPackage};
    use r_description::lossy::RDescription;
    use std::{
        env,
        str::FromStr,
        sync::{Mutex, OnceLock},
    };

    #[test]
    fn builds_closure_roots_from_description_constraints() {
        let description = RDescription::from_str(
            "Package: testpkg\nVersion: 0.1.0\nTitle: Test Package\nDescription: Test package for unit tests.\nLicense: MIT\nImports: cli (>= 3.6.0), digest\nDepends: R (>= 4.2), jsonlite (= 1.8.9)\n",
        )
        .expect("description should parse");

        assert_eq!(
            description.closure_roots(),
            vec![
                ClosureRoot {
                    name: "cli".to_string(),
                    constraint: ">= 3.6.0".to_string(),
                },
                ClosureRoot {
                    name: "digest".to_string(),
                    constraint: "*".to_string(),
                },
                ClosureRoot {
                    name: "jsonlite".to_string(),
                    constraint: "= 1.8.9".to_string(),
                },
            ]
        );
    }

    #[test]
    fn builds_lockfile_from_registry_resolution() {
        let lockfile = lockfile_from_resolution(
            vec!["cli".to_string(), "digest".to_string()],
            "https://api.rrepo.org",
            &[
                ResolvedPackage {
                    name: "cli".to_string(),
                    version: "3.6.5".to_string(),
                    source_url: "https://api.rrepo.org/packages/cli/versions/3.6.5/source"
                        .to_string(),
                    source_tarball_key: "src/cli_3.6.5.tar.gz".to_string(),
                    description_key: "desc/cli_3.6.5".to_string(),
                },
                ResolvedPackage {
                    name: "digest".to_string(),
                    version: "0.6.37".to_string(),
                    source_url: "https://api.rrepo.org/packages/digest/versions/0.6.37/source"
                        .to_string(),
                    source_tarball_key: "src/digest_0.6.37.tar.gz".to_string(),
                    description_key: "desc/digest_0.6.37".to_string(),
                },
            ],
        );

        assert_eq!(lockfile.registry, "https://api.rrepo.org");
        assert_eq!(lockfile.packages["cli"].source.as_deref(), Some("registry"));
        assert_eq!(
            lockfile.packages["digest"].source_url.as_deref(),
            Some("https://api.rrepo.org/packages/digest/versions/0.6.37/source")
        );
    }

    #[test]
    fn reads_registry_base_url_from_environment() {
        static ENV_LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        let _guard = ENV_LOCK
            .get_or_init(|| Mutex::new(()))
            .lock()
            .expect("environment mutex should lock");

        unsafe {
            env::set_var("RPX_REGISTRY_BASE_URL", "https://example.test/");
        }

        assert_eq!(registry_base_url(), "https://example.test");

        unsafe {
            env::remove_var("RPX_REGISTRY_BASE_URL");
        }
    }
}
