use clap::Parser;
use std::collections::BTreeSet;

mod cli;
mod description;
mod lockfile;
mod project;
mod r;
mod registry;
mod repo;
mod resolver;

use cli::{Cli, Commands, RepoCommands};
use description::{read_description, write_description, DescriptionExt};
use lockfile::{read_lockfile, write_lockfile, Lockfile};
use project::lockfile_path;
use r::{
    install_exact_repository_package, install_package, install_requirements, installed_packages,
    installed_packages_by_name, project_command, remove_installed_package_dir,
    remove_installed_packages, to_locked_package,
};
use repo::{
    alias_for_repository, effective_repositories, expand_repo_spec, DEFAULT_REPOSITORY_URL,
};

pub fn run() {
    let cli = Cli::parse();

    match cli.command {
        Commands::Add { package } => cmd_add(&package),
        Commands::Remove { package } => cmd_remove(&package),
        Commands::Repo { command } => cmd_repo(command),
        Commands::Run { command } => cmd_run(&command),
        Commands::Lock => cmd_lock(),
        Commands::Status => cmd_status(),
        Commands::Sync => cmd_sync(),
    }
}

fn cmd_add(package: &str) {
    if lockfile_path().exists() {
        sync_from_lockfile();
    }

    let mut project = read_description().expect("failed to read DESCRIPTION");
    project.description.add_to_imports(package);
    write_description(&project);

    let repositories = effective_repositories(&project.additional_repositories);
    install_package(package, &repositories);
    lock_from_description();
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

fn cmd_repo(command: RepoCommands) {
    match command {
        RepoCommands::Add { repo } => cmd_repo_add(&repo),
        RepoCommands::Remove { repo } => cmd_repo_remove(&repo),
        RepoCommands::List => cmd_repo_list(),
    }
}

fn cmd_repo_add(repo: &str) {
    let repositories = expand_repo_spec(repo).expect("failed to expand repository alias");
    let should_relock = lockfile_path().exists();
    let mut project = read_description().expect("failed to read DESCRIPTION");
    project.add_repositories(&repositories);
    write_description(&project);

    if should_relock {
        lock_from_description();
    }
}

fn cmd_repo_remove(repo: &str) {
    let repositories = expand_repo_spec(repo).unwrap_or_else(|_| vec![repo.to_string()]);
    let should_relock = lockfile_path().exists();
    let mut project = read_description().expect("failed to read DESCRIPTION");
    project.remove_repositories(&repositories);
    write_description(&project);

    if should_relock {
        lock_from_description();
    }
}

fn cmd_repo_list() {
    let project = read_description().expect("failed to read DESCRIPTION");

    println!("CRAN: {DEFAULT_REPOSITORY_URL}");
    for repository in &project.additional_repositories {
        match alias_for_repository(repository) {
            Some(alias) => println!("{alias}: {repository}"),
            None => println!("repo: {repository}"),
        }
    }
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
    let manifest_repositories = effective_repositories(&project.additional_repositories);
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
    let repository_mismatch = manifest_repositories != lockfile.repositories;
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
    println!("Locked repositories: {}", lockfile.repositories.len());
    println!("Locked packages: {}", lockfile.packages.len());
    println!("Installed packages: {}", installed.len());

    if missing_from_lockfile.is_empty()
        && extra_in_lockfile.is_empty()
        && !repository_mismatch
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

    if repository_mismatch {
        println!(
            "Repository mismatch: current [{}], locked [{}]",
            manifest_repositories.join(", "),
            lockfile.repositories.join(", ")
        );
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
    let repositories = effective_repositories(&project.additional_repositories);

    install_requirements(&requirements, &repositories);
    write_lockfile(Lockfile {
        version: 2,
        requirements,
        repositories,
        packages: installed_packages()
            .into_iter()
            .map(|package| {
                let name = package.package.clone();
                (name, to_locked_package(package))
            })
            .collect(),
    });
}

fn sync_from_lockfile() {
    let project = read_description().expect("failed to read DESCRIPTION");
    let manifest_requirements = project.description.requirements();
    let manifest_repositories = effective_repositories(&project.additional_repositories);
    let lockfile = read_lockfile().expect("failed to read lockfile");

    if manifest_requirements != lockfile.requirements {
        eprintln!("lockfile out of date; run rpx lock");
        std::process::exit(1);
    }

    if manifest_repositories != lockfile.repositories {
        eprintln!("lockfile out of date; run rpx lock");
        eprintln!(
            "repositories changed: current [{}], locked [{}]",
            manifest_repositories.join(", "),
            lockfile.repositories.join(", ")
        );
        std::process::exit(1);
    }

    install_requirements(&lockfile.requirements, &lockfile.repositories);

    let installed = installed_packages_by_name();
    let exact_reinstalls = lockfile
        .packages
        .iter()
        .filter_map(|(name, package)| match installed.get(name) {
            Some(installed_package) if installed_package.version == package.version => None,
            _ => package
                .repository
                .clone()
                .map(|repository| (name.clone(), package.version.clone(), repository)),
        })
        .collect::<Vec<_>>();

    for (name, version, repository) in &exact_reinstalls {
        install_exact_repository_package(name, version, repository, &lockfile.repositories);
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
