use clap::Parser;
use std::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    env, fs,
    io::{IsTerminal, Write},
    path::{Path, PathBuf},
    sync::{Arc, Mutex, mpsc},
    thread,
    time::{SystemTime, UNIX_EPOCH},
};

mod cli;
mod description;
mod lockfile;
mod project;
mod r;
mod registry;
mod repository;
mod resolver;
mod sysreqs;
mod ui;

use cli::{Cli, Commands, RepoCommands};
use description::{DescriptionExt, init_description, read_description, write_description};
use lockfile::{
    LOCKFILE_VERSION, LockedR, LockedSystemRequirements, Lockfile, read_lockfile,
    read_lockfile_optional, write_lockfile,
};
use project::{
    build_temp_library_path, cache_dir_path, compiled_cache_package_path, project_library_path,
    project_library_root_path,
};
use r::{
    InstallFailure, RuntimeInfo, base_packages, install_local_package, installed_packages,
    installed_packages_by_name, project_command, remove_installed_package_dir,
    remove_installed_packages, runtime_info,
};
use registry::{
    ArtifactKind, ArtifactRequest, DEFAULT_REGISTRY_BASE_URL, DownloadProgress, DownloadedArtifact,
    ResolutionRoot,
};
use repository::{
    RepositorySet, RepositorySource, normalize_repository_url, repository_source_from_package_url,
};
use resolver::{ResolvedPackage, is_base_package, resolve_from_registry};
use sysreqs::{
    SystemDependencyPlan, cached_latest_snapshot, current_host_platform,
    empty_snapshot as empty_sysreq_snapshot, install as install_system_dependencies,
    latest_snapshot as latest_sysreq_snapshot, preview_commands as sysreq_preview_commands,
    recheck_missing_packages as recheck_system_missing_packages,
    refresh_metadata as refresh_system_metadata,
    refresh_preview_command as system_metadata_refresh_preview,
    resolve_plan as resolve_system_plan,
};
use ui::{InstallKind, SyncUi, SystemDepsUi};

const DOWNLOAD_WORKERS: usize = 8;
const INSTALL_WORKERS: usize = 8;
const MAX_SOURCE_BUILDS: usize = 4;

pub fn run() {
    let cli = Cli::parse();

    match cli.command {
        Commands::Init => cmd_init(),
        Commands::Add { packages } => cmd_add(&packages),
        Commands::Remove { packages } => cmd_remove(&packages),
        Commands::Run { command } => cmd_run(&command),
        Commands::Lock => cmd_lock(),
        Commands::Status => cmd_status(),
        Commands::Sync {
            install_system,
            install_only_system,
        } => cmd_sync(install_system, install_only_system),
        Commands::Clean => cmd_clean(),
        Commands::Repo { command } => cmd_repo(command),
    }
}

fn cmd_init() {
    let path =
        init_description().unwrap_or_else(|error| panic!("failed to initialize project: {error}"));
    println!("Initialized project at {path}");
    println!("Next: run `rpx add <package>` or `rpx lock`");
}

fn cmd_add(packages: &[String]) {
    let mut project = read_description().expect("failed to read DESCRIPTION");
    let mut lockfile = read_lockfile_optional().expect("failed to read lockfile");
    let repositories = configured_repositories(&project);
    let mut new_packages = Vec::new();

    for package in packages {
        if project.description.has_dependency(package) {
            project.description.add_to_imports(package);
            continue;
        }

        new_packages.push(package.clone());
    }

    if !new_packages.is_empty() {
        let sysreq_db = load_sysreq_snapshot_for_lock(lockfile.as_ref());
        let resolved_addition = resolve_additions_from_latest(
            &project.description,
            lockfile.as_ref(),
            &new_packages,
            &repositories,
        )
        .unwrap_or_else(|error| panic!("failed to add package from registry: {error}"));

        for package in &new_packages {
            let constraints = resolved_addition
                .constraints
                .get(package)
                .expect("resolved addition should include constraints for each new package");
            project
                .description
                .add_to_imports_with_constraints(package, constraints);
        }

        lockfile = Some(lockfile_from_resolution(
            project.description.resolution_roots(),
            &default_registry_base_url(),
            &resolved_addition.resolved,
            &sysreq_db,
        ));
    }

    write_description(&project);
    if let Some(lockfile) = lockfile {
        write_lockfile(lockfile);
    } else {
        let _ = lock_from_description();
    }
    let _ = sync_from_lockfile(false, false);
    println!("Added {}", packages.join(", "));
}

fn cmd_repo(command: RepoCommands) {
    match command {
        RepoCommands::Add { url } => cmd_repo_add(&url),
        RepoCommands::Remove {
            url,
            remove_credential,
        } => cmd_repo_remove(&url, remove_credential),
        RepoCommands::List => cmd_repo_list(),
    }
}

fn cmd_repo_add(url: &str) {
    let mut project = read_description().expect("failed to read DESCRIPTION");
    let source = RepositorySource::new(url);

    if project
        .additional_repositories
        .iter()
        .any(|existing| normalize_repository_url(existing) == source.base_url())
    {
        println!("Repository already configured: {}", source.base_url());
        return;
    }

    let repositories = RepositorySet::new(vec![source.clone()]);
    repositories
        .fetch_repository_packages(&source)
        .unwrap_or_else(|error| panic!("failed to add repository {}: {error}", source.base_url()));

    project
        .additional_repositories
        .push(source.base_url().to_string());
    write_description(&project);
    println!("Added repository {}", source.base_url());
}

fn cmd_repo_remove(url: &str, remove_credential: bool) {
    let mut project = read_description().expect("failed to read DESCRIPTION");
    let source = RepositorySource::new(url);
    let original_len = project.additional_repositories.len();
    project
        .additional_repositories
        .retain(|repository| normalize_repository_url(repository) != source.base_url());

    if project.additional_repositories.len() == original_len {
        println!("Repository not configured: {}", source.base_url());
        return;
    }

    write_description(&project);

    if remove_credential {
        RepositorySet::new(vec![source.clone()])
            .remove_api_key(&source)
            .unwrap_or_else(|error| panic!("failed to remove repository credential: {error}"));
    }

    println!("Removed repository {}", source.base_url());
}

fn cmd_repo_list() {
    let project = read_description().expect("failed to read DESCRIPTION");

    if project.additional_repositories.is_empty() {
        println!("No additional repositories configured");
        return;
    }

    let repositories = RepositorySet::new(
        project
            .additional_repositories
            .iter()
            .cloned()
            .map(RepositorySource::new)
            .collect(),
    );

    for source in repositories.sources() {
        let credential = repositories
            .has_stored_credential(source)
            .unwrap_or_else(|error| panic!("failed to inspect repository credential: {error}"));
        println!(
            "{} [{}]",
            source.base_url(),
            if credential {
                "credential stored"
            } else {
                "no credential"
            }
        );
    }
}

fn cmd_remove(packages: &[String]) {
    let mut project = read_description().expect("failed to read DESCRIPTION");
    for package in packages {
        project.description.remove_from_field("Imports", package);
        project.description.remove_from_field("Depends", package);
    }
    write_description(&project);

    let installed = installed_packages_by_name();
    let mut removed = Vec::new();
    let mut missing = Vec::new();
    for package in packages {
        if installed.contains_key(package) {
            removed.push(package.clone());
        } else {
            missing.push(package.clone());
            remove_installed_package_dir(package);
        }
    }

    if !removed.is_empty() {
        remove_installed_packages(&removed);
    }

    let _ = lock_from_description();
    let _ = sync_from_lockfile(false, false);

    if !removed.is_empty() {
        println!("Removed {}", removed.join(", "));
    }
    if !missing.is_empty() {
        println!(
            "{} {} already missing from the project library",
            missing.join(", "),
            if missing.len() == 1 { "is" } else { "are" }
        );
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
    let outcome = lock_from_description();
    if outcome.changed {
        println!("Updated rpx.lock");
    } else {
        println!("rpx.lock is already up to date");
    }
}

fn cmd_sync(install_system: bool, install_only_system: bool) {
    if (install_system || install_only_system) && !host_supports_system_sync() {
        eprintln!(
            "System dependency installation is currently supported only on supported Linux distributions/package managers."
        );
        std::process::exit(1);
    }

    let outcome = sync_from_lockfile(install_system, install_only_system);
    if install_only_system {
        return;
    }
    if outcome.installed == 0 && outcome.removed == 0 {
        println!("Project library is already in sync");
    } else {
        println!("Synchronized project library");
    }
}

fn cmd_status() {
    let project = match read_description() {
        Ok(description) => description,
        Err(error) => {
            eprintln!("Could not read DESCRIPTION");
            eprintln!("{error}");
            std::process::exit(1);
        }
    };

    let lockfile = match read_lockfile() {
        Ok(lockfile) => lockfile,
        Err(error) => {
            eprintln!("Lockfile is missing or unreadable");
            eprintln!("Run: rpx lock");
            eprintln!("{error}");
            std::process::exit(1);
        }
    };

    if lockfile.version < LOCKFILE_VERSION {
        println!("Lockfile is out of date");
        println!();
        print_relock_message();
        std::process::exit(1);
    }

    let manifest_requirements = project
        .description
        .requirements()
        .into_iter()
        .collect::<BTreeSet<_>>();
    let lock_requirements = lockfile
        .roots
        .iter()
        .map(|root| root.package.clone())
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
    let locked_names = locked_package_names(&lockfile);
    let runtime_status = runtime_status(&lockfile);
    let system_plan = if host_supports_system_sync() {
        system_plan_from_lockfile(&lockfile).ok()
    } else {
        None
    };

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
        .filter(|(name, _)| !is_base_package(name))
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

    if missing_from_lockfile.is_empty()
        && extra_in_lockfile.is_empty()
        && missing_from_library.is_empty()
        && extra_in_library.is_empty()
        && version_mismatches.is_empty()
        && runtime_status.missing_base_packages.is_empty()
        && system_plan
            .as_ref()
            .map(|plan| plan.missing_packages.is_empty() && plan.unsupported_rules.is_empty())
            .unwrap_or(true)
    {
        print_runtime_version_warning(&runtime_status);
        println!("Project is in sync");
        return;
    }

    let lockfile_out_of_date = !missing_from_lockfile.is_empty() || !extra_in_lockfile.is_empty();
    let library_out_of_date = !missing_from_library.is_empty()
        || !extra_in_library.is_empty()
        || !version_mismatches.is_empty();
    let runtime_out_of_date = !runtime_status.missing_base_packages.is_empty();
    let system_out_of_date = system_plan
        .as_ref()
        .map(|plan| !plan.missing_packages.is_empty() || !plan.unsupported_rules.is_empty())
        .unwrap_or(false);

    if lockfile_out_of_date && library_out_of_date {
        println!("Project is out of sync");
        println!();
        println!("Run: rpx lock && rpx sync");
    } else if lockfile_out_of_date {
        println!("Lockfile is out of date");
        println!();
        println!("Run: rpx lock");
    } else if runtime_out_of_date {
        println!("R runtime is out of sync");
    } else if system_out_of_date {
        println!("System dependencies are out of sync");
    } else {
        println!("Project library is out of sync");
        println!();
        println!("Run: rpx sync");
    }

    print_status_group(
        "Packages in DESCRIPTION but not locked:",
        &missing_from_lockfile,
    );
    print_status_group(
        "Packages locked but no longer in DESCRIPTION:",
        &extra_in_lockfile,
    );
    print_status_group("Packages locked but not installed:", &missing_from_library);
    print_status_group("Packages installed but not locked:", &extra_in_library);
    print_status_group(
        "Installed versions that differ from rpx.lock:",
        &version_mismatches,
    );
    print_runtime_version_warning(&runtime_status);
    print_status_group(
        "R runtime is missing locked base packages:",
        &runtime_status.missing_base_packages,
    );
    if let Some(plan) = system_plan {
        print_status_group(
            "Missing system packages for this host:",
            &plan.missing_packages,
        );
        print_status_group(
            "System requirement rules without a host mapping:",
            &plan.unsupported_rules,
        );
    }

    std::process::exit(1);
}

fn cmd_clean() {
    let mut removed_any = false;

    removed_any |= remove_dir_if_exists(&project_library_root_path(), "project library");
    removed_any |= remove_dir_if_exists(&cache_dir_path(), "cache directory");

    if removed_any {
        println!("Removed project library and cache directories");
    } else {
        println!("Project library and cache directories are already clean");
    }
}

fn remove_dir_if_exists(path: &Path, label: &str) -> bool {
    if !path.exists() {
        return false;
    }

    fs::remove_dir_all(path)
        .unwrap_or_else(|error| panic!("failed to remove {label} at {}: {error}", path.display()));
    true
}

fn print_status_group(title: &str, items: &[String]) {
    if items.is_empty() {
        return;
    }

    println!();
    println!("{title}");
    for item in items {
        println!("- {item}");
    }
}

#[derive(Debug, PartialEq, Eq)]
struct AddResolution {
    constraints: BTreeMap<String, Vec<String>>,
    resolved: Vec<ResolvedPackage>,
}

#[derive(Debug, Default, PartialEq, Eq)]
struct LockOutcome {
    changed: bool,
}

#[derive(Debug, Default, PartialEq, Eq)]
struct SyncOutcome {
    installed: usize,
    removed: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct PendingInstall {
    name: String,
    version: String,
    artifact: ArtifactRequest,
    fallback_artifact: Option<ArtifactRequest>,
    install_kind: InstallKind,
    install_type: String,
    dependencies: BTreeSet<String>,
    cache_key: String,
    cache_path: PathBuf,
}

#[derive(Debug)]
struct CompletedBuild {
    package: PendingInstall,
    temp_library: PathBuf,
}

#[derive(Debug)]
struct DownloadedInstall {
    artifact: DownloadedArtifact,
    install_kind: InstallKind,
    install_type: String,
}

#[derive(Debug)]
enum InstallEvent {
    Finished {
        package: PendingInstall,
        result: Result<CompletedBuild, InstallFailure>,
    },
}

#[derive(Debug)]
enum DownloadEvent {
    Started {
        name: String,
        version: String,
        kind: ArtifactKind,
    },
    ContentLength {
        name: String,
        length: u64,
    },
    Advanced {
        name: String,
        bytes: u64,
    },
    FallbackToSource {
        name: String,
        version: String,
    },
    Finished {
        package: PendingInstall,
        result: Result<DownloadedInstall, String>,
    },
}

fn resolve_additions_from_latest(
    description: &description::RDescription,
    lockfile: Option<&Lockfile>,
    packages: &[String],
    repositories: &RepositorySet,
) -> Result<AddResolution, String> {
    let new_packages = packages
        .iter()
        .cloned()
        .map(|package| (package, "*".to_string()))
        .collect::<BTreeMap<_, _>>();
    let preferred_versions = preferred_locked_versions(description, lockfile, &new_packages)?;
    let roots = add_resolution_roots(description, &new_packages);
    let resolved =
        resolve_from_registry(repositories, &roots, &preferred_versions).map_err(|error| {
            format!(
                "could not resolve a compatible dependency set for {}: {error}",
                packages.join(", ")
            )
        })?;

    Ok(AddResolution {
        constraints: constraints_from_resolved_roots(packages, &resolved)?,
        resolved,
    })
}

fn add_resolution_roots(
    description: &description::RDescription,
    new_packages: &BTreeMap<String, String>,
) -> Vec<ResolutionRoot> {
    let mut roots = BTreeSet::new();

    for root in description.resolution_roots() {
        if new_packages.contains_key(&root.name) {
            continue;
        }

        roots.insert(root);
    }

    for (name, constraint) in new_packages {
        roots.insert(ResolutionRoot {
            name: name.clone(),
            constraint: constraint.clone(),
        });
    }

    roots.into_iter().collect()
}

fn preferred_locked_versions(
    description: &description::RDescription,
    lockfile: Option<&Lockfile>,
    excluded_packages: &BTreeMap<String, String>,
) -> Result<BTreeMap<String, String>, String> {
    let Some(lockfile) = lockfile else {
        return Ok(BTreeMap::new());
    };

    let mut preferred_versions: BTreeMap<String, String> = description
        .requirements()
        .into_iter()
        .filter(|name| !excluded_packages.contains_key(name))
        .filter_map(|name| {
            lockfile
                .packages
                .get(&name)
                .map(|package| (name, package.version.clone()))
        })
        .collect();

    for package in excluded_packages.keys() {
        if let Some(locked_package) = lockfile.packages.get(package) {
            preferred_versions.insert(package.clone(), locked_package.version.clone());
        }
    }

    Ok(preferred_versions)
}

fn semver_add_constraint(version: &str) -> Result<String, String> {
    let parts = semver_prefixes(version)?;
    let major = *parts
        .first()
        .ok_or_else(|| format!("latest version is not semver-like: {version}"))?;
    let upper_bound = format!("< {}.0.0", major + 1);
    Ok(format!(">= {version}, {upper_bound}"))
}

fn semver_prefixes(version: &str) -> Result<Vec<u64>, String> {
    version
        .split(['.', '-'])
        .filter(|part| !part.is_empty())
        .map(|part| {
            part.parse::<u64>()
                .map_err(|_| format!("latest version is not semver-like: {version}"))
        })
        .collect()
}

fn persisted_constraints(constraint: &str) -> Vec<String> {
    let constraint = constraint.trim();
    if constraint.is_empty() || constraint == "*" {
        return vec![];
    }

    constraint
        .split(',')
        .map(str::trim)
        .filter(|part| !part.is_empty())
        .map(ToString::to_string)
        .collect()
}

fn constraints_from_resolved_roots(
    packages: &[String],
    resolved: &[ResolvedPackage],
) -> Result<BTreeMap<String, Vec<String>>, String> {
    let resolved_by_package = resolved
        .iter()
        .map(|package| (package.name.as_str(), package.version.as_str()))
        .collect::<BTreeMap<_, _>>();

    packages
        .iter()
        .map(|package| {
            if is_base_package(package) {
                return Ok((package.clone(), vec![]));
            }

            let version = resolved_by_package
                .get(package.as_str())
                .ok_or_else(|| format!("missing resolved version for {package}"))?;
            Ok((
                package.clone(),
                persisted_constraints(&semver_add_constraint(version)?),
            ))
        })
        .collect()
}

fn lock_from_description() -> LockOutcome {
    let project = read_description().expect("failed to read DESCRIPTION");
    let roots = project.description.resolution_roots();
    let registry = default_registry_base_url();
    let repositories = configured_repositories(&project);
    let existing_lockfile = read_lockfile_optional().expect("failed to read lockfile");
    let sysreq_db = load_sysreq_snapshot_for_lock(existing_lockfile.as_ref());

    if roots.is_empty() {
        let lockfile = lockfile_from_resolution(vec![], &registry, &[], &sysreq_db);
        let changed = existing_lockfile.as_ref() != Some(&lockfile);
        write_lockfile(lockfile);
        return LockOutcome { changed };
    }

    let resolved = resolve_from_registry(&repositories, &roots, &BTreeMap::new())
        .unwrap_or_else(|error| panic!("failed to resolve package set from registry: {error}"));

    let lockfile = lockfile_from_resolution(roots, &registry, &resolved, &sysreq_db);
    let changed = existing_lockfile.as_ref() != Some(&lockfile);
    write_lockfile(lockfile);
    LockOutcome { changed }
}

fn load_sysreq_snapshot_for_lock(
    existing_lockfile: Option<&Lockfile>,
) -> sysreqs::SysreqDbSnapshot {
    if let Ok(snapshot) = latest_sysreq_snapshot() {
        return snapshot;
    }

    if let Ok(Some(snapshot)) = cached_latest_snapshot() {
        eprintln!("warning: using cached system requirements database snapshot");
        return snapshot;
    }

    if let Some(commit) = existing_lockfile
        .map(|lockfile| lockfile.sysreqs.db_commit.as_str())
        .filter(|commit| !commit.is_empty())
    {
        if let Ok(snapshot) = sysreqs::snapshot_for_commit(commit) {
            eprintln!(
                "warning: using system requirements database pinned by the existing lockfile ({commit})"
            );
            return snapshot;
        }
    }

    eprintln!(
        "warning: system requirements database unavailable; continuing without updating locked system dependency rules"
    );
    empty_sysreq_snapshot()
}

fn sync_from_lockfile(install_system: bool, install_only_system: bool) -> SyncOutcome {
    let project = read_description().expect("failed to read DESCRIPTION");
    let manifest_requirements = project
        .description
        .requirements()
        .into_iter()
        .collect::<BTreeSet<_>>();
    let lockfile = read_lockfile().expect("failed to read lockfile");
    validate_lockfile_version_for_sync(&lockfile);
    validate_runtime_for_sync(&lockfile);
    if host_supports_system_sync() {
        let system_plan = system_plan_from_lockfile(&lockfile).unwrap_or_else(|error| {
            eprintln!("warning: failed to prepare system dependency plan: {error}");
            system_plan_without_db(&lockfile)
        });
        let proceed_with_r =
            handle_system_requirements(&system_plan, install_system, install_only_system);
        if install_only_system || !proceed_with_r {
            return SyncOutcome::default();
        }
    }
    let lock_requirements = lockfile
        .roots
        .iter()
        .map(|root| root.package.clone())
        .collect::<BTreeSet<_>>();

    if manifest_requirements != lock_requirements {
        eprintln!("lockfile out of date; run rpx lock");
        std::process::exit(1);
    }

    let installed = installed_packages_by_name();
    let runtime = runtime_info();
    let repositories = repositories_for_sync(&project, &lockfile);
    let mut outcome = SyncOutcome::default();
    let ui = SyncUi::new();
    let mut satisfied = installed
        .iter()
        .filter_map(|(name, installed_package)| {
            lockfile
                .packages
                .get(name)
                .filter(|locked_package| locked_package.version == installed_package.version)
                .map(|_| name.clone())
        })
        .collect::<BTreeSet<_>>();

    let mut pending = collect_pending_installs(&lockfile, &installed, &runtime, &repositories);

    let cached_names = pending
        .values()
        .filter(|package| package.cache_path.exists())
        .map(|package| package.name.clone())
        .collect::<Vec<_>>();

    ui.start_restores(cached_names.len());
    for name in cached_names {
        let package = pending
            .remove(&name)
            .expect("cached package should still be pending");
        restore_cached_package(&package, &project_library_path()).unwrap_or_else(|error| {
            panic!(
                "failed to restore cached package {}@{}: {error}",
                package.name, package.version
            )
        });
        satisfied.insert(package.name.clone());
        outcome.installed += 1;
        ui.finish_restore(&package.name, &package.version);
    }
    ui.finish_restores();

    let download_order = pending.values().cloned().collect::<Vec<_>>();
    ui.start_downloads(download_order.len());
    let downloaded = download_artifacts_in_parallel(&repositories, &download_order, &ui)
        .unwrap_or_else(|error| panic!("failed to prepare source artifacts: {error}"));
    ui.finish_downloads();

    let project_library = project_library_path();
    let binary_installs = downloaded
        .values()
        .filter(|download| download.install_kind == InstallKind::Binary)
        .count();
    let source_builds = downloaded.len().saturating_sub(binary_installs);
    ui.start_installs(binary_installs, source_builds);
    install_downloaded_packages_in_parallel(
        &mut pending,
        &mut satisfied,
        &downloaded,
        &project_library,
        &ui,
        &mut outcome,
    );
    ui.finish_installs();

    let locked_names = locked_package_names(&lockfile);
    let extras = installed_packages_by_name()
        .into_keys()
        .filter(|name| !locked_names.contains(name))
        .collect::<Vec<_>>();
    outcome.removed = extras.len();
    ui.start_removals(extras.len());
    remove_installed_packages(&extras);
    ui.finish_removals();

    let final_state = installed_packages_by_name();
    let missing = locked_names
        .iter()
        .filter(|name| !final_state.contains_key(*name))
        .cloned()
        .collect::<Vec<_>>();
    let extras = final_state
        .keys()
        .filter(|name| !locked_names.contains(*name))
        .cloned()
        .collect::<Vec<_>>();
    let version_mismatches = lockfile
        .packages
        .iter()
        .filter(|(name, _)| !is_base_package(name))
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
        ui.finish();
        return outcome;
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

    ui.finish();
    std::process::exit(1);
}

pub(crate) fn exit_with_status(code: Option<i32>) {
    if code != Some(0) {
        std::process::exit(code.unwrap_or(1));
    }
}

fn default_registry_base_url() -> String {
    env::var("RPX_REGISTRY_BASE_URL")
        .unwrap_or_else(|_| DEFAULT_REGISTRY_BASE_URL.to_string())
        .trim_end_matches('/')
        .to_string()
}

fn configured_repositories(project: &description::ProjectDescription) -> RepositorySet {
    RepositorySet::new(repository_sources_from_project(project))
}

fn repository_sources_from_project(
    project: &description::ProjectDescription,
) -> Vec<RepositorySource> {
    let mut sources = vec![RepositorySource::new(default_registry_base_url())];
    sources.extend(
        project
            .additional_repositories
            .iter()
            .cloned()
            .map(RepositorySource::new),
    );
    sources
}

fn repositories_for_sync(
    project: &description::ProjectDescription,
    lockfile: &Lockfile,
) -> RepositorySet {
    let mut sources = repository_sources_from_project(project);
    sources.extend(lockfile.packages.values().filter_map(|package| {
        package
            .source_url
            .as_deref()
            .and_then(repository_source_from_package_url)
            .map(RepositorySource::new)
    }));
    RepositorySet::new(sources)
}

fn lockfile_from_resolution(
    roots: Vec<ResolutionRoot>,
    registry: &str,
    resolved: &[ResolvedPackage],
    sysreq_db: &sysreqs::SysreqDbSnapshot,
) -> Lockfile {
    let required_base_packages = locked_base_packages(&roots, resolved);
    let sysreqs = locked_system_requirements(resolved, sysreq_db);
    Lockfile {
        version: LOCKFILE_VERSION,
        registry: registry.to_string(),
        r: LockedR {
            version: runtime_info().version,
            base_packages: required_base_packages,
        },
        sysreqs,
        roots: roots
            .into_iter()
            .map(|root| lockfile::LockedRoot {
                package: root.name,
                constraint: root.constraint,
            })
            .collect(),
        packages: resolved
            .iter()
            .map(|package| {
                (
                    package.name.clone(),
                    lockfile::LockedPackage {
                        package: package.name.clone(),
                        version: package.version.clone(),
                        source: Some("repository".to_string()),
                        source_url: Some(package.source_url.clone()),
                        dependencies: package
                            .dependencies
                            .iter()
                            .map(|dependency| lockfile::LockedDependency {
                                package: dependency.package.clone(),
                                kind: dependency.kind.clone(),
                                min_version: dependency.min_version.clone(),
                                max_version_exclusive: dependency.max_version_exclusive.clone(),
                            })
                            .collect(),
                    },
                )
            })
            .collect(),
    }
}

fn print_relock_message() {
    println!("Your lockfile was created by an older rpx version and needs to be updated.");
    println!("Run: rpx lock");
}

fn validate_lockfile_version_for_sync(lockfile: &Lockfile) {
    if lockfile.version >= LOCKFILE_VERSION {
        return;
    }

    eprintln!("Your lockfile was created by an older rpx version and needs to be updated.");
    eprintln!("Run: rpx lock");
    std::process::exit(1);
}

fn locked_base_packages(roots: &[ResolutionRoot], resolved: &[ResolvedPackage]) -> Vec<String> {
    let mut packages = BTreeSet::new();

    packages.extend(
        roots
            .iter()
            .filter(|root| is_base_package(&root.name))
            .map(|root| root.name.clone()),
    );
    packages.extend(
        resolved
            .iter()
            .flat_map(|package| &package.dependencies)
            .filter(|dependency| is_base_package(&dependency.package))
            .map(|dependency| dependency.package.clone()),
    );

    packages.into_iter().collect()
}

fn locked_package_names(lockfile: &Lockfile) -> BTreeSet<String> {
    lockfile
        .packages
        .keys()
        .filter(|name| !is_base_package(name))
        .cloned()
        .collect()
}

#[derive(Debug, Default, PartialEq, Eq)]
struct RuntimeStatus {
    version_mismatch: Option<String>,
    missing_base_packages: Vec<String>,
}

fn runtime_status(lockfile: &Lockfile) -> RuntimeStatus {
    let runtime = runtime_info();
    let version_mismatch =
        (!lockfile.r.version.is_empty() && lockfile.r.version != runtime.version).then(|| {
            format!(
                "R {} installed, R {} locked",
                runtime.version, lockfile.r.version
            )
        });
    let available_base_packages = base_packages().into_iter().collect::<BTreeSet<_>>();
    let locked_base_packages = lockfile
        .r
        .base_packages
        .iter()
        .chain(lockfile.roots.iter().map(|root| &root.package))
        .chain(lockfile.packages.keys())
        .filter(|package| is_base_package(package))
        .cloned()
        .collect::<BTreeSet<_>>();
    let missing_base_packages = lockfile
        .r
        .base_packages
        .iter()
        .chain(locked_base_packages.iter())
        .filter(|package| !available_base_packages.contains(*package))
        .cloned()
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect();

    RuntimeStatus {
        version_mismatch,
        missing_base_packages,
    }
}

fn print_runtime_version_warning(status: &RuntimeStatus) {
    let Some(version_mismatch) = &status.version_mismatch else {
        return;
    };

    println!();
    println!("R runtime differs from lockfile:");
    println!("- {version_mismatch}");
}

fn system_plan_from_lockfile(lockfile: &Lockfile) -> Result<SystemDependencyPlan, String> {
    if lockfile.sysreqs.db_commit.is_empty() {
        return Ok(system_plan_without_db(lockfile));
    }

    let snapshot = sysreqs::snapshot_for_commit(&lockfile.sysreqs.db_commit)?;
    Ok(resolve_system_plan(&snapshot, &lockfile.sysreqs.packages))
}

fn system_plan_without_db(lockfile: &Lockfile) -> SystemDependencyPlan {
    SystemDependencyPlan {
        host: current_host_platform(),
        missing_packages: vec![],
        install_packages: vec![],
        pre_install_commands: vec![],
        post_install_commands: vec![],
        unsupported_rules: lockfile.sysreqs.rules.clone(),
        package_rules: lockfile.sysreqs.packages.clone(),
        install_supported: false,
        can_auto_install: false,
        installed_query_error: None,
        needs_metadata_refresh: false,
    }
}

fn host_supports_system_sync() -> bool {
    matches!(current_host_platform(), sysreqs::HostPlatform::Linux { .. })
}

fn handle_system_requirements(
    plan: &SystemDependencyPlan,
    install_system: bool,
    install_only_system: bool,
) -> bool {
    let explicit_install = install_system || install_only_system;
    let interactive = std::io::stdin().is_terminal() && std::io::stderr().is_terminal();
    let mut plan = plan.clone();

    if !plan.unsupported_rules.is_empty() {
        eprintln!(
            "warning: some system requirement rules do not have an install mapping for {}: {}",
            plan.host.label(),
            plan.unsupported_rules.join(", ")
        );
    }

    if plan.needs_metadata_refresh && explicit_install {
        if interactive {
            prompt_for_metadata_refresh(&plan);
        }

        eprintln!("Refreshing system package information...");
        refresh_system_metadata(&plan)
            .unwrap_or_else(|error| panic!("failed to refresh package metadata: {error}"));
        match recheck_system_missing_packages(&plan) {
            Ok(missing_packages) => {
                plan.missing_packages = missing_packages;
                plan.installed_query_error = None;
                plan.needs_metadata_refresh = false;
            }
            Err(error) => {
                plan.installed_query_error = Some(error);
                plan.needs_metadata_refresh = false;
            }
        }
    }

    if plan.missing_packages.is_empty() {
        if install_only_system {
            println!("System dependencies are already installed");
        }
        return !install_only_system;
    }

    if plan.installed_query_error.is_none() {
        print_system_package_summary(
            &format!("Missing system packages for {}:", plan.host.label()),
            &plan.missing_packages,
        );
    }
    let preview = sysreq_preview_commands(&plan);
    if !preview.is_empty() {
        eprintln!("rpx will run:");
        for command in &preview {
            eprintln!("- {command}");
        }
    }

    if explicit_install && interactive && !prompt_for_install_confirmation() {
        println!("Canceled");
        std::process::exit(1);
    }

    if explicit_install {
        let ui = SystemDepsUi::start();
        if let Err(error) = install_system_dependencies(&plan) {
            ui.fail();
            panic!("failed to install system dependencies: {error}");
        }
        ui.finish();
        if install_only_system {
            println!("System dependency sync complete.");
            return false;
        }
        return true;
    }

    if !interactive {
        eprintln!("warning: continuing with R package sync without installing system dependencies");
        return !install_only_system;
    }

    match prompt_for_system_dependency_action() {
        SyncSystemChoice::InstallAndContinue => {
            let ui = SystemDepsUi::start();
            if let Err(error) = install_system_dependencies(&plan) {
                ui.fail();
                panic!("failed to install system dependencies: {error}");
            }
            ui.finish();
            true
        }
        SyncSystemChoice::TryROnly => true,
        SyncSystemChoice::Cancel => {
            println!("Canceled");
            std::process::exit(1);
        }
    }
}

fn prompt_for_install_confirmation() -> bool {
    eprintln!("Proceed with system package installation? [y/N]");
    eprint!("> ");
    std::io::stderr().flush().expect("failed to flush prompt");

    let mut input = String::new();
    if std::io::stdin().read_line(&mut input).is_err() {
        return false;
    }

    matches!(input.trim(), "y" | "Y" | "yes" | "YES" | "Yes")
}

fn print_system_package_summary(title: &str, packages: &[String]) {
    eprintln!("{title}");
    let shown = packages.iter().take(8).collect::<Vec<_>>();
    for package in shown {
        eprintln!("- {package}");
    }
    if packages.len() > 8 {
        eprintln!("- ... and {} more", packages.len() - 8);
    }
}

fn prompt_for_metadata_refresh(plan: &SystemDependencyPlan) {
    eprintln!("rpx could not verify which system packages are missing yet.");
    eprintln!();
    eprintln!("rpx can run:");
    if let Some(command) = system_metadata_refresh_preview(plan) {
        eprintln!("- {command}");
    }
    eprintln!("to refresh apt package information and check what is missing.");
    eprintln!();
    eprintln!("Run package metadata refresh now? [y/N]");
    eprint!("> ");
    std::io::stderr().flush().expect("failed to flush prompt");

    let mut input = String::new();
    if std::io::stdin().read_line(&mut input).is_err() {
        println!("Canceled");
        std::process::exit(1);
    }

    if !matches!(input.trim(), "y" | "Y" | "yes" | "YES" | "Yes") {
        println!("Canceled");
        std::process::exit(1);
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SyncSystemChoice {
    InstallAndContinue,
    TryROnly,
    Cancel,
}

fn prompt_for_system_dependency_action() -> SyncSystemChoice {
    eprintln!("Choose an action:");
    eprintln!("1. Install system deps and continue");
    eprintln!("2. Try to install R packages only");
    eprintln!("3. Cancel");
    eprint!("> ");
    std::io::stderr().flush().expect("failed to flush prompt");

    let mut input = String::new();
    if std::io::stdin().read_line(&mut input).is_err() {
        return SyncSystemChoice::TryROnly;
    }

    match input.trim() {
        "1" | "y" | "Y" => SyncSystemChoice::InstallAndContinue,
        "2" | "r" | "R" => SyncSystemChoice::TryROnly,
        _ => SyncSystemChoice::Cancel,
    }
}

fn locked_system_requirements(
    resolved: &[ResolvedPackage],
    sysreq_db: &sysreqs::SysreqDbSnapshot,
) -> LockedSystemRequirements {
    let packages = resolved
        .iter()
        .filter_map(|package| {
            let rules = sysreqs::match_rules(package.system_requirements.as_deref(), sysreq_db);
            (!rules.is_empty()).then(|| (package.name.clone(), rules))
        })
        .collect::<BTreeMap<_, _>>();
    let rules = packages
        .values()
        .flatten()
        .cloned()
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect();

    LockedSystemRequirements {
        db_commit: sysreq_db.commit.clone(),
        rules,
        packages,
    }
}

fn validate_runtime_for_sync(lockfile: &Lockfile) {
    let status = runtime_status(lockfile);

    if let Some(version_mismatch) = status.version_mismatch {
        eprintln!("warning: {version_mismatch}");
    }

    if status.missing_base_packages.is_empty() {
        return;
    }

    for package in &status.missing_base_packages {
        eprintln!("R runtime is missing required base package: {package}");
    }
    eprintln!("These packages are part of R itself and cannot be installed by rpx.");
    eprintln!("Use a complete R installation compatible with this project.");
    std::process::exit(1);
}

fn registry_source_url(registry: &str, package: &str, version: &str) -> String {
    format!(
        "{}/packages/{package}/versions/{version}/source",
        registry.trim_end_matches('/')
    )
}

fn preferred_artifact(
    source: &RepositorySource,
    package: &str,
    version: &str,
    source_url: &str,
    runtime: &RuntimeInfo,
) -> (
    ArtifactRequest,
    Option<ArtifactRequest>,
    InstallKind,
    String,
) {
    let source_artifact = ArtifactRequest {
        kind: ArtifactKind::Source,
        url: source_url.to_string(),
        cache_file_name: source_cache_file_name(package, version),
    };

    let Some(binary) = binary_artifact_request(source.base_url(), package, version, runtime) else {
        return (
            source_artifact,
            None,
            InstallKind::Source,
            "source".to_string(),
        );
    };

    (
        binary,
        Some(source_artifact),
        InstallKind::Binary,
        runtime.pkg_type.clone(),
    )
}

fn binary_artifact_request(
    registry: &str,
    package: &str,
    version: &str,
    runtime: &RuntimeInfo,
) -> Option<ArtifactRequest> {
    if runtime.pkg_type == "win.binary" {
        return Some(ArtifactRequest {
            kind: ArtifactKind::Binary,
            url: format!(
                "{}/packages/{package}/versions/{version}/binaries/windows/{}",
                registry.trim_end_matches('/'),
                r_minor_version(&runtime.version)?
            ),
            cache_file_name: windows_binary_cache_file_name(package, version),
        });
    }

    let target = runtime.pkg_type.strip_prefix("mac.binary.")?;
    Some(ArtifactRequest {
        kind: ArtifactKind::Binary,
        url: format!(
            "{}/packages/{package}/versions/{version}/binaries/macos/{target}/{}",
            registry.trim_end_matches('/'),
            r_minor_version(&runtime.version)?
        ),
        cache_file_name: macos_binary_cache_file_name(package, version),
    })
}

fn source_cache_file_name(package: &str, version: &str) -> String {
    format!("{package}_{version}.tar.gz")
}

fn windows_binary_cache_file_name(package: &str, version: &str) -> String {
    format!("{package}_{version}.zip")
}

fn macos_binary_cache_file_name(package: &str, version: &str) -> String {
    format!("{package}_{version}.tgz")
}

fn r_minor_version(version: &str) -> Option<String> {
    let mut parts = version.split('.');
    Some(format!("{}.{}", parts.next()?, parts.next()?))
}

fn should_fallback_to_source(error: &str) -> bool {
    error.contains("artifact download failed (404 ")
        || error.contains("artifact download failed (502 ")
}

fn collect_pending_installs(
    lockfile: &Lockfile,
    installed: &BTreeMap<String, r::InstalledPackage>,
    runtime: &RuntimeInfo,
    repositories: &RepositorySet,
) -> BTreeMap<String, PendingInstall> {
    lockfile
        .packages
        .iter()
        .filter(|(name, _)| !is_base_package(name))
        .filter_map(|(name, package)| match installed.get(name) {
            Some(installed_package) if installed_package.version == package.version => None,
            _ => {
                let source_url = package.source_url.clone().unwrap_or_else(|| {
                    registry_source_url(&lockfile.registry, name, &package.version)
                });
                let source = repositories
                    .source_for_url(&source_url)
                    .unwrap_or_else(|| RepositorySource::new(lockfile.registry.clone()));
                let (artifact, fallback_artifact, install_kind, install_type) =
                    preferred_artifact(&source, name, &package.version, &source_url, runtime);
                let dependencies = package
                    .dependencies
                    .iter()
                    .filter(|dependency| lockfile.packages.contains_key(&dependency.package))
                    .map(|dependency| dependency.package.clone())
                    .collect::<BTreeSet<_>>();
                let cache_key = compiled_cache_key(name, &package.version, runtime);
                let cache_path = compiled_cache_package_path(&cache_key, name);

                Some((
                    name.clone(),
                    PendingInstall {
                        name: name.clone(),
                        version: package.version.clone(),
                        artifact,
                        fallback_artifact,
                        install_kind,
                        install_type,
                        dependencies,
                        cache_key,
                        cache_path,
                    },
                ))
            }
        })
        .collect()
}

fn compiled_cache_key(package: &str, version: &str, runtime: &RuntimeInfo) -> String {
    let input = format!(
        "{package}\n{version}\n{}\n{}\n{}",
        runtime.version, runtime.platform, runtime.pkg_type
    );
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    use std::hash::{Hash, Hasher};
    input.hash(&mut hasher);
    format!("{}-{}-{:016x}", package, version, hasher.finish())
}

fn restore_cached_package(package: &PendingInstall, target_library: &Path) -> Result<(), String> {
    copy_package_into_library(&package.cache_path, target_library)
}

fn download_artifacts_in_parallel(
    repositories: &RepositorySet,
    packages: &[PendingInstall],
    ui: &SyncUi,
) -> Result<BTreeMap<String, DownloadedInstall>, String> {
    if packages.is_empty() {
        return Ok(BTreeMap::new());
    }

    let queue = Arc::new(Mutex::new(VecDeque::from(packages.to_vec())));
    let (sender, receiver) = mpsc::channel();

    for _ in 0..DOWNLOAD_WORKERS.min(packages.len()) {
        let queue = Arc::clone(&queue);
        let sender = sender.clone();
        let repositories = repositories.clone();
        thread::spawn(move || {
            loop {
                let Some(package) = queue
                    .lock()
                    .expect("download queue should lock")
                    .pop_front()
                else {
                    break;
                };

                let _ = sender.send(DownloadEvent::Started {
                    name: package.name.clone(),
                    version: package.version.clone(),
                    kind: package.artifact.kind,
                });

                let progress_sender = sender.clone();
                let progress_name = package.name.clone();
                let result = repositories
                    .source_for_url(&package.artifact.url)
                    .ok_or_else(|| {
                        format!(
                            "could not determine repository source for {}@{}",
                            package.name, package.version
                        )
                    })
                    .and_then(|source| {
                        repositories.download_artifact_with_progress(
                            &source,
                            &package.name,
                            &package.version,
                            &package.artifact,
                            move |progress| match progress {
                                DownloadProgress::ContentLength(length) => {
                                    let _ = progress_sender.send(DownloadEvent::ContentLength {
                                        name: progress_name.clone(),
                                        length,
                                    });
                                }
                                DownloadProgress::Advanced(bytes) => {
                                    let _ = progress_sender.send(DownloadEvent::Advanced {
                                        name: progress_name.clone(),
                                        bytes,
                                    });
                                }
                            },
                        )
                    })
                    .map(|artifact| DownloadedInstall {
                        artifact,
                        install_kind: package.install_kind,
                        install_type: package.install_type.clone(),
                    })
                    .or_else(|error| {
                        let Some(fallback) = &package.fallback_artifact else {
                            return Err(error);
                        };
                        if !should_fallback_to_source(&error) {
                            return Err(error);
                        }
                        let _ = sender.send(DownloadEvent::FallbackToSource {
                            name: package.name.clone(),
                            version: package.version.clone(),
                        });
                        let _ = sender.send(DownloadEvent::Started {
                            name: package.name.clone(),
                            version: package.version.clone(),
                            kind: fallback.kind,
                        });
                        let source =
                            repositories.source_for_url(&fallback.url).ok_or_else(|| {
                                format!(
                                    "could not determine repository source for {}@{}",
                                    package.name, package.version
                                )
                            })?;
                        let progress_sender = sender.clone();
                        let progress_name = package.name.clone();
                        repositories
                            .download_artifact_with_progress(
                                &source,
                                &package.name,
                                &package.version,
                                fallback,
                                move |progress| match progress {
                                    DownloadProgress::ContentLength(length) => {
                                        let _ =
                                            progress_sender.send(DownloadEvent::ContentLength {
                                                name: progress_name.clone(),
                                                length,
                                            });
                                    }
                                    DownloadProgress::Advanced(bytes) => {
                                        let _ = progress_sender.send(DownloadEvent::Advanced {
                                            name: progress_name.clone(),
                                            bytes,
                                        });
                                    }
                                },
                            )
                            .map(|artifact| DownloadedInstall {
                                artifact,
                                install_kind: InstallKind::Source,
                                install_type: "source".to_string(),
                            })
                    });
                let _ = sender.send(DownloadEvent::Finished { package, result });
            }
        });
    }
    drop(sender);

    let mut downloaded = BTreeMap::new();
    let mut finished = 0;
    while finished < packages.len() {
        match receiver
            .recv()
            .expect("download worker should return a result")
        {
            DownloadEvent::Started {
                name,
                version,
                kind,
            } => ui.start_download(&name, &version, kind),
            DownloadEvent::ContentLength { name, length } => ui.set_download_length(&name, length),
            DownloadEvent::Advanced { name, bytes } => ui.advance_download(&name, bytes),
            DownloadEvent::FallbackToSource { name, version } => {
                ui.fallback_to_source(&name, &version)
            }
            DownloadEvent::Finished { package, result } => {
                finished += 1;
                let artifact = result
                    .map_err(|error| format!("{}@{}: {error}", package.name, package.version))?;
                ui.finish_download(&package.name, &package.version, artifact.install_kind);
                downloaded.insert(package.name.clone(), artifact);
            }
        }
    }

    Ok(downloaded)
}

fn ready_install_names(
    pending: &BTreeMap<String, PendingInstall>,
    satisfied: &BTreeSet<String>,
) -> Vec<String> {
    pending
        .values()
        .filter(|package| {
            package
                .dependencies
                .iter()
                .all(|dependency| satisfied.contains(dependency))
        })
        .map(|package| package.name.clone())
        .collect()
}

fn install_downloaded_packages_in_parallel(
    pending: &mut BTreeMap<String, PendingInstall>,
    satisfied: &mut BTreeSet<String>,
    artifacts: &BTreeMap<String, DownloadedInstall>,
    project_library: &Path,
    ui: &SyncUi,
    outcome: &mut SyncOutcome,
) {
    let (sender, receiver) = mpsc::channel();
    let mut active_installs = 0;
    let mut active_source_builds = 0;

    while !pending.is_empty() || active_installs > 0 {
        let ready_names = ready_install_names(pending, satisfied);
        let mut dispatched = Vec::new();

        for name in ready_names {
            if active_installs >= INSTALL_WORKERS {
                break;
            }

            let install_kind = effective_install_kind(artifacts, &name);
            if install_kind == InstallKind::Source && active_source_builds >= MAX_SOURCE_BUILDS {
                continue;
            }

            let package = pending
                .remove(&name)
                .expect("ready package should still be pending");
            spawn_install_worker(package.clone(), artifacts, project_library, &sender);
            active_installs += 1;
            if install_kind == InstallKind::Source {
                active_source_builds += 1;
            }
            dispatched.push((package.name, package.version, install_kind));
        }

        if !dispatched.is_empty() {
            ui.start_install_batch(dispatched);
        }

        if active_installs == 0 {
            let blocked = pending.keys().cloned().collect::<Vec<_>>();
            panic!(
                "no installable packages remain after dependency resolution: {}",
                blocked.join(", ")
            );
        }

        match receiver
            .recv()
            .expect("install worker should return a result")
        {
            InstallEvent::Finished { package, result } => {
                active_installs -= 1;
                let install_kind = effective_install_kind(artifacts, &package.name);
                if install_kind == InstallKind::Source {
                    active_source_builds -= 1;
                }

                match result {
                    Ok(completed) => {
                        finalize_built_package(&completed, project_library).unwrap_or_else(
                            |error| {
                                panic!(
                                    "failed to cache built package {}@{}: {error}",
                                    completed.package.name, completed.package.version
                                )
                            },
                        );
                        satisfied.insert(completed.package.name.clone());
                        outcome.installed += 1;
                        ui.finish_install(
                            &completed.package.name,
                            &completed.package.version,
                            install_kind,
                        );
                    }
                    Err(error) => {
                        ui.fail_install(&package.name, &package.version);
                        report_install_failure(&package.name, &package.version, &error);
                        std::process::exit(error.exit_code.unwrap_or(1));
                    }
                }
            }
        }
    }
}

fn spawn_install_worker(
    package: PendingInstall,
    artifacts: &BTreeMap<String, DownloadedInstall>,
    dependency_library: &Path,
    sender: &mpsc::Sender<InstallEvent>,
) {
    let sender = sender.clone();
    let downloaded = artifacts
        .get(&package.name)
        .expect("artifact should exist for pending package");
    let artifact_path = downloaded.artifact.path().to_path_buf();
    let install_type = downloaded.install_type.clone();
    let dependency_library = dependency_library.to_path_buf();

    thread::spawn(move || {
        let temp_library = build_temp_library_path(&package.name, &unique_build_token());
        let package_for_success = package.clone();
        let result = install_local_package(
            &artifact_path,
            &package.name,
            &package.version,
            &install_type,
            &temp_library,
            &[dependency_library],
        )
        .map(|_| CompletedBuild {
            package: package_for_success,
            temp_library,
        });
        let _ = sender.send(InstallEvent::Finished { package, result });
    });
}

fn effective_install_kind(
    artifacts: &BTreeMap<String, DownloadedInstall>,
    package_name: &str,
) -> InstallKind {
    artifacts
        .get(package_name)
        .expect("artifact should exist for pending package")
        .install_kind
}

fn finalize_built_package(completed: &CompletedBuild, target_library: &Path) -> Result<(), String> {
    let built_package_path = completed.temp_library.join(&completed.package.name);
    if !built_package_path.exists() {
        return Err(format!(
            "built package directory is missing: {}",
            built_package_path.display()
        ));
    }

    copy_package_dir(&built_package_path, &completed.package.cache_path)?;
    copy_package_into_library(&completed.package.cache_path, target_library)?;
    fs::remove_dir_all(
        completed
            .temp_library
            .parent()
            .expect("temporary build library should have a parent"),
    )
    .map_err(|error| format!("failed to clean temporary build directory: {error}"))?;
    Ok(())
}

fn copy_package_into_library(package_path: &Path, target_library: &Path) -> Result<(), String> {
    let package_name = package_path
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| format!("invalid package path: {}", package_path.display()))?;
    copy_package_dir(package_path, &target_library.join(package_name))
}

fn copy_package_dir(source: &Path, destination: &Path) -> Result<(), String> {
    if destination.exists() {
        fs::remove_dir_all(destination)
            .map_err(|error| format!("failed to replace package directory: {error}"))?;
    }
    fs::create_dir_all(destination)
        .map_err(|error| format!("failed to create package directory: {error}"))?;

    for entry in fs::read_dir(source)
        .map_err(|error| format!("failed to read package directory: {error}"))?
    {
        let entry = entry.map_err(|error| format!("failed to read package entry: {error}"))?;
        let source_path = entry.path();
        let destination_path = destination.join(entry.file_name());
        let file_type = entry
            .file_type()
            .map_err(|error| format!("failed to inspect package entry: {error}"))?;
        if file_type.is_dir() {
            copy_package_dir(&source_path, &destination_path)?;
        } else {
            fs::copy(&source_path, &destination_path)
                .map_err(|error| format!("failed to copy package file: {error}"))?;
        }
    }

    Ok(())
}

fn unique_build_token() -> String {
    let unique = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time should be after unix epoch")
        .as_nanos();
    format!("{}-{unique}", std::process::id())
}

#[cfg(test)]
fn locked_install_order(lockfile: &Lockfile) -> Result<Vec<String>, String> {
    let mut indegree = lockfile
        .packages
        .keys()
        .map(|name| (name.clone(), 0_usize))
        .collect::<BTreeMap<_, _>>();
    let mut dependents = lockfile
        .packages
        .keys()
        .map(|name| (name.clone(), BTreeSet::new()))
        .collect::<BTreeMap<_, _>>();

    for (name, package) in &lockfile.packages {
        let internal_dependencies = package
            .dependencies
            .iter()
            .filter(|dependency| lockfile.packages.contains_key(&dependency.package))
            .map(|dependency| dependency.package.clone())
            .collect::<BTreeSet<_>>();

        *indegree
            .get_mut(name)
            .expect("lockfile package should have indegree") += internal_dependencies.len();

        for dependency in internal_dependencies {
            dependents
                .get_mut(&dependency)
                .expect("lockfile dependency should exist")
                .insert(name.clone());
        }
    }

    let mut ready = indegree
        .iter()
        .filter(|(_, count)| **count == 0)
        .map(|(name, _)| name.clone())
        .collect::<BTreeSet<_>>();
    let mut ordered = Vec::with_capacity(lockfile.packages.len());

    while let Some(name) = ready.pop_first() {
        ordered.push(name.clone());

        for dependent in dependents.get(&name).cloned().unwrap_or_default() {
            let count = indegree
                .get_mut(&dependent)
                .expect("dependent should have indegree entry");
            *count -= 1;
            if *count == 0 {
                ready.insert(dependent);
            }
        }
    }

    if ordered.len() != lockfile.packages.len() {
        let unresolved = indegree
            .into_iter()
            .filter(|(_, count)| *count > 0)
            .map(|(name, _)| name)
            .collect::<Vec<_>>();
        return Err(format!(
            "cyclic or unresolved lockfile dependencies: {}",
            unresolved.join(", ")
        ));
    }

    Ok(ordered)
}

fn report_install_failure(name: &str, version: &str, failure: &InstallFailure) {
    eprintln!("failed to install {name}@{version}");
    eprintln!("summary: {}", failure.summary);
    eprintln!("log: {}", failure.log_path.display());

    let log_tail = read_log_tail(&failure.log_path, 80);
    if !log_tail.is_empty() {
        eprintln!("recent build output:");
        eprintln!("{log_tail}");
    }
}

fn read_log_tail(path: &Path, max_lines: usize) -> String {
    let Ok(contents) = std::fs::read_to_string(path) else {
        return String::new();
    };

    let lines = contents.lines().collect::<Vec<_>>();
    let start = lines.len().saturating_sub(max_lines);
    lines[start..].join("\n")
}

#[cfg(test)]
mod tests {
    use super::{
        add_resolution_roots, binary_artifact_request, compiled_cache_key,
        constraints_from_resolved_roots, default_registry_base_url, locked_install_order,
        lockfile_from_resolution, persisted_constraints, r_minor_version, semver_add_constraint,
        should_fallback_to_source,
    };
    use crate::description::RDescription;
    use crate::{
        description::DescriptionExt,
        lockfile::{
            LOCKFILE_VERSION, LockedDependency, LockedPackage, LockedR, LockedSystemRequirements,
            Lockfile,
        },
        r::RuntimeInfo,
        registry::ResolutionRoot,
        resolver::{ResolvedDependency, ResolvedPackage},
        sysreqs::SysreqDbSnapshot,
    };
    use std::collections::BTreeMap;
    use std::{
        env,
        str::FromStr,
        sync::{Mutex, OnceLock},
    };

    #[test]
    fn builds_resolution_roots_from_description_constraints() {
        let description = RDescription::from_str(
            "Package: testpkg\nVersion: 0.1.0\nTitle: Test Package\nDescription: Test package for unit tests.\nLicense: MIT\nImports: cli (>= 3.6.0), digest\nDepends: R (>= 4.2), jsonlite (= 1.8.9)\n",
        )
        .expect("description should parse");

        assert_eq!(
            description.resolution_roots(),
            vec![
                ResolutionRoot {
                    name: "cli".to_string(),
                    constraint: ">= 3.6.0".to_string(),
                },
                ResolutionRoot {
                    name: "digest".to_string(),
                    constraint: "*".to_string(),
                },
                ResolutionRoot {
                    name: "jsonlite".to_string(),
                    constraint: "= 1.8.9".to_string(),
                },
            ]
        );
    }

    #[test]
    fn builds_lockfile_from_registry_resolution() {
        let lockfile = lockfile_from_resolution(
            vec![
                ResolutionRoot {
                    name: "digest".to_string(),
                    constraint: "*".to_string(),
                },
                ResolutionRoot {
                    name: "cli".to_string(),
                    constraint: "= 3.6.5".to_string(),
                },
            ],
            "https://api.rrepo.org",
            &[
                ResolvedPackage {
                    name: "cli".to_string(),
                    version: "3.6.5".to_string(),
                    source_url: "https://api.rrepo.org/packages/cli/versions/3.6.5/source"
                        .to_string(),
                    dependencies: vec![
                        ResolvedDependency {
                            package: "R".to_string(),
                            kind: "Depends".to_string(),
                            min_version: Some("4.3".to_string()),
                            max_version_exclusive: None,
                        },
                        ResolvedDependency {
                            package: "utils".to_string(),
                            kind: "Imports".to_string(),
                            min_version: None,
                            max_version_exclusive: None,
                        },
                        ResolvedDependency {
                            package: "base".to_string(),
                            kind: "Depends".to_string(),
                            min_version: None,
                            max_version_exclusive: None,
                        },
                    ],
                    system_requirements: None,
                },
                ResolvedPackage {
                    name: "digest".to_string(),
                    version: "0.6.37".to_string(),
                    source_url: "https://api.rrepo.org/packages/digest/versions/0.6.37/source"
                        .to_string(),
                    dependencies: vec![],
                    system_requirements: None,
                },
            ],
            &empty_sysreq_db(),
        );

        assert_eq!(lockfile.registry, "https://api.rrepo.org");
        assert_eq!(lockfile.version, LOCKFILE_VERSION);
        assert_eq!(lockfile.r.base_packages, vec!["base", "utils"]);
        assert_eq!(lockfile.roots[0].package, "digest");
        assert_eq!(lockfile.roots[1].package, "cli");
        assert_eq!(
            lockfile.packages["cli"].source.as_deref(),
            Some("repository")
        );
        assert_eq!(
            lockfile.packages["cli"].dependencies,
            vec![
                LockedDependency {
                    package: "R".to_string(),
                    kind: "Depends".to_string(),
                    min_version: Some("4.3".to_string()),
                    max_version_exclusive: None,
                },
                LockedDependency {
                    package: "utils".to_string(),
                    kind: "Imports".to_string(),
                    min_version: None,
                    max_version_exclusive: None,
                },
                LockedDependency {
                    package: "base".to_string(),
                    kind: "Depends".to_string(),
                    min_version: None,
                    max_version_exclusive: None,
                },
            ]
        );
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

        assert_eq!(default_registry_base_url(), "https://example.test");

        unsafe {
            env::remove_var("RPX_REGISTRY_BASE_URL");
        }
    }

    #[test]
    fn builds_semver_constraint_from_resolved_version() {
        assert_eq!(
            semver_add_constraint("1.1.4").unwrap(),
            ">= 1.1.4, < 2.0.0".to_string()
        );
    }

    #[test]
    fn builds_semver_constraint_for_short_version() {
        assert_eq!(
            semver_add_constraint("1").unwrap(),
            ">= 1, < 2.0.0".to_string()
        );
    }

    #[test]
    fn splits_persisted_constraints_for_description_entries() {
        assert_eq!(
            persisted_constraints(">= 1.1.4, < 2.0.0"),
            vec![">= 1.1.4".to_string(), "< 2.0.0".to_string()]
        );
        assert!(persisted_constraints("*").is_empty());
    }

    #[test]
    fn derives_constraints_from_resolved_root_versions() {
        let constraints = constraints_from_resolved_roots(
            &["digest".to_string()],
            &[ResolvedPackage {
                name: "digest".to_string(),
                version: "0.6.37".to_string(),
                source_url: "https://api.rrepo.org/packages/digest/versions/0.6.37/source"
                    .to_string(),
                dependencies: vec![],
                system_requirements: None,
            }],
        )
        .expect("constraints should derive");

        assert_eq!(
            constraints,
            BTreeMap::from([(
                "digest".to_string(),
                vec![">= 0.6.37".to_string(), "< 1.0.0".to_string()],
            )])
        );
    }

    #[test]
    fn derives_empty_constraints_for_base_package_roots() {
        let constraints = constraints_from_resolved_roots(&["grid".to_string()], &[])
            .expect("base package constraints should derive");

        assert_eq!(constraints, BTreeMap::from([("grid".to_string(), vec![])]));
    }

    #[test]
    fn records_direct_base_roots_as_runtime_requirements() {
        let lockfile = lockfile_from_resolution(
            vec![ResolutionRoot {
                name: "grid".to_string(),
                constraint: "*".to_string(),
            }],
            "https://api.rrepo.org",
            &[],
            &empty_sysreq_db(),
        );

        assert_eq!(lockfile.r.base_packages, vec!["grid"]);
        assert!(!lockfile.packages.contains_key("grid"));
    }

    #[test]
    fn keeps_existing_roots_when_adding_new_package() {
        let description = RDescription::from_str(
            "Package: testpkg\nVersion: 0.1.0\nTitle: Test Package\nDescription: Test package for unit tests.\nLicense: MIT\nImports: cli\n",
        )
        .expect("description should parse");

        let roots = add_resolution_roots(
            &description,
            &BTreeMap::from([("digest".to_string(), ">= 0.6.37, < 1.0.0".to_string())]),
        );

        assert_eq!(
            roots,
            vec![
                ResolutionRoot {
                    name: "cli".to_string(),
                    constraint: "*".to_string(),
                },
                ResolutionRoot {
                    name: "digest".to_string(),
                    constraint: ">= 0.6.37, < 1.0.0".to_string(),
                },
            ]
        );
    }

    #[test]
    fn installs_locked_packages_in_dependency_order() {
        let lockfile = Lockfile {
            version: LOCKFILE_VERSION,
            registry: "https://api.rrepo.org".to_string(),
            r: LockedR::default(),
            sysreqs: LockedSystemRequirements::default(),
            roots: vec![],
            packages: BTreeMap::from([
                (
                    "AzureKeyVault".to_string(),
                    LockedPackage {
                        package: "AzureKeyVault".to_string(),
                        version: "1.0.0".to_string(),
                        source: Some("registry".to_string()),
                        source_url: Some(
                            "https://api.rrepo.org/packages/AzureKeyVault/versions/1.0.0/source"
                                .to_string(),
                        ),
                        dependencies: vec![LockedDependency {
                            package: "AzureRMR".to_string(),
                            kind: "Imports".to_string(),
                            min_version: None,
                            max_version_exclusive: None,
                        }],
                    },
                ),
                (
                    "AzureRMR".to_string(),
                    LockedPackage {
                        package: "AzureRMR".to_string(),
                        version: "1.0.0".to_string(),
                        source: Some("registry".to_string()),
                        source_url: Some(
                            "https://api.rrepo.org/packages/AzureRMR/versions/1.0.0/source"
                                .to_string(),
                        ),
                        dependencies: vec![LockedDependency {
                            package: "httr2".to_string(),
                            kind: "Imports".to_string(),
                            min_version: None,
                            max_version_exclusive: None,
                        }],
                    },
                ),
                (
                    "httr2".to_string(),
                    LockedPackage {
                        package: "httr2".to_string(),
                        version: "1.0.0".to_string(),
                        source: Some("registry".to_string()),
                        source_url: Some(
                            "https://api.rrepo.org/packages/httr2/versions/1.0.0/source"
                                .to_string(),
                        ),
                        dependencies: vec![],
                    },
                ),
            ]),
        };

        assert_eq!(
            locked_install_order(&lockfile).unwrap(),
            vec![
                "httr2".to_string(),
                "AzureRMR".to_string(),
                "AzureKeyVault".to_string()
            ]
        );
    }

    #[test]
    fn rejects_cyclic_locked_dependencies() {
        let lockfile = Lockfile {
            version: LOCKFILE_VERSION,
            registry: "https://api.rrepo.org".to_string(),
            r: LockedR::default(),
            sysreqs: LockedSystemRequirements::default(),
            roots: vec![],
            packages: BTreeMap::from([
                (
                    "a".to_string(),
                    LockedPackage {
                        package: "a".to_string(),
                        version: "1.0.0".to_string(),
                        source: Some("registry".to_string()),
                        source_url: None,
                        dependencies: vec![LockedDependency {
                            package: "b".to_string(),
                            kind: "Imports".to_string(),
                            min_version: None,
                            max_version_exclusive: None,
                        }],
                    },
                ),
                (
                    "b".to_string(),
                    LockedPackage {
                        package: "b".to_string(),
                        version: "1.0.0".to_string(),
                        source: Some("registry".to_string()),
                        source_url: None,
                        dependencies: vec![LockedDependency {
                            package: "a".to_string(),
                            kind: "Imports".to_string(),
                            min_version: None,
                            max_version_exclusive: None,
                        }],
                    },
                ),
            ]),
        };

        let error = locked_install_order(&lockfile).expect_err("cycle should fail");
        assert!(error.contains("cyclic or unresolved lockfile dependencies"));
    }

    #[test]
    fn derives_windows_binary_artifact_url_from_runtime() {
        let runtime = RuntimeInfo {
            version: "4.5.2".to_string(),
            platform: "x86_64-w64-mingw32".to_string(),
            pkg_type: "win.binary".to_string(),
        };

        let artifact =
            binary_artifact_request("https://api.rrepo.org", "digest", "0.6.37", &runtime)
                .expect("windows binary should be supported");

        assert_eq!(
            artifact.url,
            "https://api.rrepo.org/packages/digest/versions/0.6.37/binaries/windows/4.5"
        );
        assert_eq!(artifact.cache_file_name, "digest_0.6.37.zip");
    }

    #[test]
    fn derives_binary_artifact_url_from_path_based_repository() {
        let runtime = RuntimeInfo {
            version: "4.5.2".to_string(),
            platform: "x86_64-w64-mingw32".to_string(),
            pkg_type: "win.binary".to_string(),
        };

        let artifact = binary_artifact_request(
            "https://upstream.rrepo.dev/cran",
            "digest",
            "0.6.37",
            &runtime,
        )
        .expect("path-based repository binaries should be attempted");

        assert_eq!(
            artifact.url,
            "https://upstream.rrepo.dev/cran/packages/digest/versions/0.6.37/binaries/windows/4.5"
        );
    }

    #[test]
    fn derives_macos_binary_artifact_url_from_runtime() {
        let runtime = RuntimeInfo {
            version: "4.5.2".to_string(),
            platform: "aarch64-apple-darwin20".to_string(),
            pkg_type: "mac.binary.big-sur-arm64".to_string(),
        };

        let artifact =
            binary_artifact_request("https://api.rrepo.org", "jsonlite", "2.0.0", &runtime)
                .expect("macOS binary should be supported");

        assert_eq!(
            artifact.url,
            "https://api.rrepo.org/packages/jsonlite/versions/2.0.0/binaries/macos/big-sur-arm64/4.5"
        );
        assert_eq!(artifact.cache_file_name, "jsonlite_2.0.0.tgz");
    }

    #[test]
    fn skips_binary_artifacts_when_runtime_pkg_type_is_not_binary() {
        let runtime = RuntimeInfo {
            version: "4.5.2".to_string(),
            platform: "aarch64-apple-darwin20".to_string(),
            pkg_type: "source".to_string(),
        };

        assert!(
            binary_artifact_request("https://api.rrepo.org", "digest", "0.6.37", &runtime)
                .is_none()
        );
    }

    #[test]
    fn extracts_r_minor_version_for_binary_urls() {
        assert_eq!(r_minor_version("4.5.2"), Some("4.5".to_string()));
        assert_eq!(r_minor_version("4.4"), Some("4.4".to_string()));
        assert_eq!(r_minor_version("4"), None);
    }

    #[test]
    fn fallback_statuses_are_limited_to_missing_or_upstream_binary_errors() {
        assert!(should_fallback_to_source(
            "artifact download failed (404 Not Found): missing"
        ));
        assert!(should_fallback_to_source(
            "artifact download failed (502 Bad Gateway): upstream failed"
        ));
        assert!(!should_fallback_to_source(
            "artifact download failed (500 Internal Server Error): nope"
        ));
    }

    #[test]
    fn compiled_cache_key_changes_with_package_install_type() {
        let source_runtime = RuntimeInfo {
            version: "4.5.2".to_string(),
            platform: "aarch64-apple-darwin20".to_string(),
            pkg_type: "source".to_string(),
        };
        let binary_runtime = RuntimeInfo {
            version: "4.5.2".to_string(),
            platform: "aarch64-apple-darwin20".to_string(),
            pkg_type: "mac.binary.big-sur-arm64".to_string(),
        };

        assert_ne!(
            compiled_cache_key("jsonlite", "2.0.0", &source_runtime),
            compiled_cache_key("jsonlite", "2.0.0", &binary_runtime)
        );
    }

    fn empty_sysreq_db() -> SysreqDbSnapshot {
        SysreqDbSnapshot {
            commit: "test-commit".to_string(),
            rules: vec![],
            scripts: BTreeMap::new(),
        }
    }
}
