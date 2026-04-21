use clap::Parser;
use indicatif::{MultiProgress, ProgressBar, ProgressDrawTarget, ProgressStyle};
use std::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    env, fs,
    io::IsTerminal,
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
mod resolver;

use cli::{Cli, Commands};
use description::{DescriptionExt, init_description, read_description, write_description};
use lockfile::{Lockfile, read_lockfile, read_lockfile_optional, write_lockfile};
use project::{build_temp_library_path, compiled_cache_package_path, project_library_path};
use r::{
    InstallFailure, RuntimeInfo, install_local_package, installed_packages,
    installed_packages_by_name, project_command, remove_installed_package_dir,
    remove_installed_packages, runtime_info,
};
use registry::{
    ArtifactKind, ArtifactRequest, ClosureRequest, ClosureRoot, DEFAULT_REGISTRY_BASE_URL,
    DownloadedArtifact, RegistryClient,
};
use resolver::{ResolvedPackage, resolve_from_closure};

const SYNC_WORKERS: usize = 4;

pub fn run() {
    let cli = Cli::parse();

    match cli.command {
        Commands::Init => cmd_init(),
        Commands::Add { packages } => cmd_add(&packages),
        Commands::Remove { packages } => cmd_remove(&packages),
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

fn cmd_add(packages: &[String]) {
    let mut project = read_description().expect("failed to read DESCRIPTION");
    let mut lockfile = read_lockfile_optional().expect("failed to read lockfile");
    let registry = registry_base_url();
    let client = RegistryClient::new(&registry);
    let mut new_packages = Vec::new();

    for package in packages {
        if project.description.has_dependency(package) {
            project.description.add_to_imports(package);
            continue;
        }

        new_packages.push(package.clone());
    }

    if !new_packages.is_empty() {
        let resolved_addition = resolve_additions_from_latest(
            &project.description,
            lockfile.as_ref(),
            &new_packages,
            &client,
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
            project.description.closure_roots(),
            client.base_url(),
            &resolved_addition.resolved,
        ));
    }

    write_description(&project);
    if let Some(lockfile) = lockfile {
        write_lockfile(lockfile);
    } else {
        let _ = lock_from_description();
    }
    let _ = sync_from_lockfile();
    println!("Added {}", packages.join(", "));
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
    let _ = sync_from_lockfile();

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

fn cmd_sync() {
    let outcome = sync_from_lockfile();
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

    if missing_from_lockfile.is_empty()
        && extra_in_lockfile.is_empty()
        && missing_from_library.is_empty()
        && extra_in_library.is_empty()
        && version_mismatches.is_empty()
    {
        println!("Project is in sync");
        return;
    }

    let lockfile_out_of_date = !missing_from_lockfile.is_empty() || !extra_in_lockfile.is_empty();
    let library_out_of_date = !missing_from_library.is_empty()
        || !extra_in_library.is_empty()
        || !version_mismatches.is_empty();

    if lockfile_out_of_date && library_out_of_date {
        println!("Project is out of sync");
        println!();
        println!("Run: rpx lock && rpx sync");
    } else if lockfile_out_of_date {
        println!("Lockfile is out of date");
        println!();
        println!("Run: rpx lock");
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

    std::process::exit(1);
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
    install_type: String,
}

fn resolve_additions_from_latest(
    description: &r_description::lossy::RDescription,
    lockfile: Option<&Lockfile>,
    packages: &[String],
    client: &RegistryClient,
) -> Result<AddResolution, String> {
    let constraints_by_package = latest_constraints_by_package(packages, client)?;
    let attempts = max_constraint_attempts(&constraints_by_package);

    for index in 0..attempts {
        let package_constraints = constraints_for_attempt(&constraints_by_package, index);
        let request = ClosureRequest {
            roots: add_closure_roots(description, lockfile, &package_constraints),
        };

        let closure = client.fetch_closure_with_retry(&request)?;
        if let Ok(resolved) =
            resolve_from_closure(&request, &registry::ClosureResponse::Complete(closure))
        {
            return Ok(AddResolution {
                constraints: package_constraints
                    .into_iter()
                    .map(|(package, constraint)| (package, persisted_constraints(&constraint)))
                    .collect(),
                resolved,
            });
        }
    }

    Err(format!(
        "could not resolve a compatible dependency set for {}",
        packages.join(", ")
    ))
}

fn add_closure_roots(
    description: &r_description::lossy::RDescription,
    lockfile: Option<&Lockfile>,
    new_packages: &BTreeMap<String, String>,
) -> Vec<ClosureRoot> {
    let mut roots = BTreeSet::new();
    let locked_packages = pinned_existing_roots(description, lockfile, new_packages);

    for root in description.closure_roots() {
        if new_packages.contains_key(&root.name) || locked_packages.contains_key(&root.name) {
            continue;
        }

        roots.insert(root);
    }

    for (name, version) in locked_packages {
        roots.insert(ClosureRoot {
            name,
            constraint: format!("= {version}"),
        });
    }

    for (name, constraint) in new_packages {
        roots.insert(ClosureRoot {
            name: name.clone(),
            constraint: constraint.clone(),
        });
    }

    roots.into_iter().collect()
}

fn pinned_existing_roots(
    description: &r_description::lossy::RDescription,
    lockfile: Option<&Lockfile>,
    excluded_packages: &BTreeMap<String, String>,
) -> BTreeMap<String, String> {
    let Some(lockfile) = lockfile else {
        return BTreeMap::new();
    };

    description
        .requirements()
        .into_iter()
        .filter(|name| !excluded_packages.contains_key(name))
        .filter_map(|name| {
            lockfile
                .packages
                .get(&name)
                .map(|package| (name, package.version.clone()))
        })
        .collect()
}

fn latest_constraints_by_package(
    packages: &[String],
    client: &RegistryClient,
) -> Result<BTreeMap<String, Vec<String>>, String> {
    if packages.is_empty() {
        return Ok(BTreeMap::new());
    }

    let queue = Arc::new(Mutex::new(VecDeque::from(packages.to_vec())));
    let (sender, receiver) = mpsc::channel();

    for _ in 0..SYNC_WORKERS.min(packages.len()) {
        let queue = Arc::clone(&queue);
        let sender = sender.clone();
        let base_url = client.base_url().to_string();
        thread::spawn(move || {
            let client = RegistryClient::new(&base_url);
            loop {
                let Some(package) = queue.lock().expect("latest queue should lock").pop_front()
                else {
                    break;
                };
                let result = client
                    .fetch_latest_version_with_retry(&package)
                    .and_then(|latest| semver_add_constraints(&latest.version));
                let _ = sender.send((package, result));
            }
        });
    }
    drop(sender);

    let mut constraints = BTreeMap::new();
    for _ in 0..packages.len() {
        let (package, result) = receiver
            .recv()
            .expect("latest worker should return a result");
        constraints.insert(
            package.clone(),
            result.map_err(|error| format!("{package}: {error}"))?,
        );
    }
    Ok(constraints)
}

fn max_constraint_attempts(constraints_by_package: &BTreeMap<String, Vec<String>>) -> usize {
    constraints_by_package
        .values()
        .map(Vec::len)
        .max()
        .unwrap_or(0)
}

fn constraints_for_attempt(
    constraints_by_package: &BTreeMap<String, Vec<String>>,
    index: usize,
) -> BTreeMap<String, String> {
    constraints_by_package
        .iter()
        .map(|(package, constraints)| {
            let constraint = constraints
                .get(index)
                .or_else(|| constraints.last())
                .expect("each package should have at least one constraint")
                .clone();
            (package.clone(), constraint)
        })
        .collect()
}

fn semver_add_constraints(version: &str) -> Result<Vec<String>, String> {
    let parts = semver_prefixes(version)?;
    let major = *parts
        .first()
        .ok_or_else(|| format!("latest version is not semver-like: {version}"))?;
    let upper_bound = format!("< {}.0.0", major + 1);
    let mut constraints = Vec::new();

    constraints.push(format!(">= {version}, {upper_bound}"));

    if parts.len() >= 2 {
        constraints.push(format!(">= {}.{}, {upper_bound}", parts[0], parts[1]));
    }

    constraints.push(format!(">= {major}, {upper_bound}"));
    constraints.push("*".to_string());

    let mut deduped = Vec::new();
    for constraint in constraints {
        if !deduped.contains(&constraint) {
            deduped.push(constraint);
        }
    }

    Ok(deduped)
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

fn lock_from_description() -> LockOutcome {
    let project = read_description().expect("failed to read DESCRIPTION");
    let roots = project.description.closure_roots();
    let registry = registry_base_url();
    let existing_lockfile = read_lockfile_optional().expect("failed to read lockfile");

    if roots.is_empty() {
        let lockfile = lockfile_from_resolution(vec![], &registry, &[]);
        let changed = existing_lockfile.as_ref() != Some(&lockfile);
        write_lockfile(lockfile);
        return LockOutcome { changed };
    }

    let request = ClosureRequest {
        roots: roots.clone(),
    };
    let client = RegistryClient::new(&registry);
    let closure = client
        .fetch_closure_with_retry(&request)
        .unwrap_or_else(|error| panic!("failed to resolve lockfile from registry: {error}"));
    let resolved = resolve_from_closure(&request, &registry::ClosureResponse::Complete(closure))
        .unwrap_or_else(|error| panic!("failed to resolve package set from closure: {error}"));

    let lockfile = lockfile_from_resolution(roots, client.base_url(), &resolved);
    let changed = existing_lockfile.as_ref() != Some(&lockfile);
    write_lockfile(lockfile);
    LockOutcome { changed }
}

fn sync_from_lockfile() -> SyncOutcome {
    let project = read_description().expect("failed to read DESCRIPTION");
    let manifest_requirements = project
        .description
        .requirements()
        .into_iter()
        .collect::<BTreeSet<_>>();
    let lockfile = read_lockfile().expect("failed to read lockfile");
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
    let client = RegistryClient::new(&lockfile.registry);
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

    let mut pending = collect_pending_installs(&lockfile, &installed, &runtime);

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
    let downloaded = download_artifacts_in_parallel(&client, &download_order, &ui)
        .unwrap_or_else(|error| panic!("failed to prepare source artifacts: {error}"));
    ui.finish_downloads();

    let project_library = project_library_path();
    ui.start_builds(downloaded.len());
    while !pending.is_empty() {
        let ready_names = ready_install_batch(&pending, &satisfied, SYNC_WORKERS);
        if ready_names.is_empty() {
            let blocked = pending.keys().cloned().collect::<Vec<_>>();
            panic!(
                "no installable packages remain after dependency resolution: {}",
                blocked.join(", ")
            );
        }

        let batch = ready_names
            .into_iter()
            .map(|name| {
                pending
                    .remove(&name)
                    .expect("ready package should be pending")
            })
            .collect::<Vec<_>>();
        ui.start_build_batch(&batch);
        let results = build_batch(&batch, &downloaded, &project_library);
        for result in results {
            match result {
                Ok(completed) => {
                    finalize_built_package(&completed, &project_library).unwrap_or_else(|error| {
                        panic!(
                            "failed to cache built package {}@{}: {error}",
                            completed.package.name, completed.package.version
                        )
                    });
                    satisfied.insert(completed.package.name.clone());
                    outcome.installed += 1;
                    ui.finish_build(&completed.package.name, &completed.package.version);
                }
                Err((package, error)) => {
                    ui.fail_build(&package.name, &package.version);
                    report_install_failure(&package.name, &package.version, &error);
                    std::process::exit(error.exit_code.unwrap_or(1));
                }
            }
        }
    }
    ui.finish_builds();

    let extras = installed_packages_by_name()
        .into_keys()
        .filter(|name| !lockfile.packages.contains_key(name))
        .collect::<Vec<_>>();
    outcome.removed = extras.len();
    ui.start_removals(extras.len());
    remove_installed_packages(&extras);
    ui.finish_removals();

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

fn registry_base_url() -> String {
    env::var("RPX_REGISTRY_BASE_URL")
        .unwrap_or_else(|_| DEFAULT_REGISTRY_BASE_URL.to_string())
        .trim_end_matches('/')
        .to_string()
}

fn lockfile_from_resolution(
    roots: Vec<ClosureRoot>,
    registry: &str,
    resolved: &[ResolvedPackage],
) -> Lockfile {
    Lockfile {
        version: 1,
        registry: registry.to_string(),
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
                        source: Some("registry".to_string()),
                        source_url: Some(registry_source_url(
                            registry,
                            &package.name,
                            &package.version,
                        )),
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

fn registry_source_url(registry: &str, package: &str, version: &str) -> String {
    format!(
        "{}/packages/{package}/versions/{version}/source",
        registry.trim_end_matches('/')
    )
}

fn preferred_artifact(
    registry: &str,
    package: &str,
    version: &str,
    source_url: &str,
    runtime: &RuntimeInfo,
) -> (ArtifactRequest, Option<ArtifactRequest>, String) {
    let source = ArtifactRequest {
        kind: ArtifactKind::Source,
        url: source_url.to_string(),
        cache_file_name: source_cache_file_name(package, version),
    };

    let Some(binary) = binary_artifact_request(registry, package, version, runtime) else {
        return (source, None, "source".to_string());
    };

    (binary, Some(source), runtime.pkg_type.clone())
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
) -> BTreeMap<String, PendingInstall> {
    lockfile
        .packages
        .iter()
        .filter_map(|(name, package)| match installed.get(name) {
            Some(installed_package) if installed_package.version == package.version => None,
            _ => {
                let source_url = package.source_url.clone().unwrap_or_else(|| {
                    registry_source_url(&lockfile.registry, name, &package.version)
                });
                let (artifact, fallback_artifact, install_type) = preferred_artifact(
                    &lockfile.registry,
                    name,
                    &package.version,
                    &source_url,
                    runtime,
                );
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
    client: &RegistryClient,
    packages: &[PendingInstall],
    ui: &SyncUi,
) -> Result<BTreeMap<String, DownloadedInstall>, String> {
    if packages.is_empty() {
        return Ok(BTreeMap::new());
    }

    let queue = Arc::new(Mutex::new(VecDeque::from(packages.to_vec())));
    let (sender, receiver) = mpsc::channel();

    for _ in 0..SYNC_WORKERS.min(packages.len()) {
        let queue = Arc::clone(&queue);
        let sender = sender.clone();
        let base_url = client.base_url().to_string();
        thread::spawn(move || {
            let client = RegistryClient::new(&base_url);
            loop {
                let Some(package) = queue
                    .lock()
                    .expect("download queue should lock")
                    .pop_front()
                else {
                    break;
                };

                let result = client
                    .download_artifact(&package.name, &package.version, &package.artifact)
                    .map(|artifact| DownloadedInstall {
                        artifact,
                        install_type: package.install_type.clone(),
                    })
                    .or_else(|error| {
                        let Some(fallback) = &package.fallback_artifact else {
                            return Err(error);
                        };
                        if !should_fallback_to_source(&error) {
                            return Err(error);
                        }
                        client
                            .download_artifact(&package.name, &package.version, fallback)
                            .map(|artifact| DownloadedInstall {
                                artifact,
                                install_type: "source".to_string(),
                            })
                    });
                let _ = sender.send((package, result));
            }
        });
    }
    drop(sender);

    let mut downloaded = BTreeMap::new();
    for _ in 0..packages.len() {
        let (package, result) = receiver
            .recv()
            .expect("download worker should return a result");
        let artifact =
            result.map_err(|error| format!("{}@{}: {error}", package.name, package.version))?;
        ui.finish_download(&package.name, &package.version);
        downloaded.insert(package.name.clone(), artifact);
    }

    Ok(downloaded)
}

fn ready_install_batch(
    pending: &BTreeMap<String, PendingInstall>,
    satisfied: &BTreeSet<String>,
    concurrency: usize,
) -> Vec<String> {
    pending
        .values()
        .filter(|package| {
            package
                .dependencies
                .iter()
                .all(|dependency| satisfied.contains(dependency))
        })
        .take(concurrency)
        .map(|package| package.name.clone())
        .collect()
}

fn build_batch(
    batch: &[PendingInstall],
    artifacts: &BTreeMap<String, DownloadedInstall>,
    dependency_library: &Path,
) -> Vec<Result<CompletedBuild, (PendingInstall, InstallFailure)>> {
    let (sender, receiver) = mpsc::channel();

    for package in batch.iter().cloned() {
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
            let package_for_error = package.clone();
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
            })
            .map_err(|error| (package_for_error, error));
            let _ = sender.send(result);
        });
    }
    drop(sender);

    (0..batch.len())
        .map(|_| {
            receiver
                .recv()
                .expect("build worker should return a result")
        })
        .collect()
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

struct SyncUi {
    interactive: bool,
    downloads: ProgressBar,
    builds: ProgressBar,
    status: ProgressBar,
}

impl SyncUi {
    fn new() -> Self {
        let interactive = std::io::stderr().is_terminal();

        if interactive {
            let multi = MultiProgress::with_draw_target(ProgressDrawTarget::stderr());
            let downloads = multi.add(ProgressBar::new(0));
            downloads.set_style(
                ProgressStyle::with_template("downloads [{bar:30.cyan/blue}] {pos}/{len}")
                    .expect("progress template should parse")
                    .progress_chars("##-"),
            );
            let builds = multi.add(ProgressBar::new(0));
            builds.set_style(
                ProgressStyle::with_template("builds    [{bar:30.green/blue}] {pos}/{len}")
                    .expect("progress template should parse")
                    .progress_chars("##-"),
            );
            let status = multi.add(ProgressBar::new_spinner());
            status.set_style(
                ProgressStyle::with_template("{spinner} {msg}")
                    .expect("progress template should parse"),
            );
            status.enable_steady_tick(std::time::Duration::from_millis(100));

            Self {
                interactive,
                downloads,
                builds,
                status,
            }
        } else {
            Self {
                interactive,
                downloads: ProgressBar::hidden(),
                builds: ProgressBar::hidden(),
                status: ProgressBar::hidden(),
            }
        }
    }

    fn start_restores(&self, total: usize) {
        if total == 0 {
            return;
        }

        if self.interactive {
            self.downloads.set_length(total as u64);
            self.downloads.set_position(0);
            self.status
                .set_message("restoring cached packages".to_string());
        } else {
            eprintln!("Restoring {total} cached packages");
        }
    }

    fn finish_restore(&self, name: &str, version: &str) {
        self.downloads.inc(1);
        if self.interactive {
            self.status
                .set_message(format!("restored {name}@{version} from cache"));
        } else {
            eprintln!("Restored {name}@{version} from cache");
        }
    }

    fn finish_restores(&self) {
        if self.interactive && self.downloads.length().unwrap_or(0) > 0 {
            self.downloads.finish_and_clear();
        }
    }

    fn start_downloads(&self, total: usize) {
        if self.interactive {
            self.downloads.set_length(total as u64);
            self.downloads.set_position(0);
            self.status
                .set_message("downloading package artifacts".to_string());
        } else {
            eprintln!("Downloading {total} packages");
        }
    }

    fn finish_download(&self, name: &str, version: &str) {
        self.downloads.inc(1);
        if self.interactive {
            self.status
                .set_message(format!("downloaded {name}@{version}"));
        } else {
            eprintln!("Downloaded {name}@{version}");
        }
    }

    fn finish_downloads(&self) {
        if self.interactive {
            self.downloads.finish_and_clear();
        }
    }

    fn start_builds(&self, total: usize) {
        if self.interactive {
            self.builds.set_length(total as u64);
            self.builds.set_position(0);
            self.status.set_message("building packages".to_string());
        } else {
            eprintln!("Building {total} packages");
        }
    }

    fn start_build_batch(&self, batch: &[PendingInstall]) {
        if self.interactive {
            let names = batch
                .iter()
                .map(|package| package.name.as_str())
                .collect::<Vec<_>>()
                .join(", ");
            self.status.set_message(format!("building {names}"));
        } else {
            let names = batch
                .iter()
                .map(|package| format!("{}@{}", package.name, package.version))
                .collect::<Vec<_>>()
                .join(", ");
            eprintln!("Building {names}");
        }
    }

    fn finish_build(&self, name: &str, version: &str) {
        self.builds.inc(1);
        if self.interactive {
            self.status.set_message(format!("built {name}@{version}"));
        } else {
            eprintln!("Built {name}@{version}");
        }
    }

    fn fail_build(&self, name: &str, version: &str) {
        if self.interactive {
            self.status.set_message(format!("failed {name}@{version}"));
        }
    }

    fn finish_builds(&self) {
        if self.interactive {
            self.builds.finish_and_clear();
        }
    }

    fn start_removals(&self, total: usize) {
        if total == 0 {
            return;
        }

        if self.interactive {
            self.status
                .set_message("removing extra packages".to_string());
        } else {
            eprintln!("Removing {total} extra packages");
        }
    }

    fn finish_removals(&self) {
        if self.interactive {
            self.status
                .set_message("removed extra packages".to_string());
        }
    }

    fn finish(&self) {
        if self.interactive {
            self.status.finish_and_clear();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{
        add_closure_roots, binary_artifact_request, compiled_cache_key, locked_install_order,
        lockfile_from_resolution, persisted_constraints, r_minor_version, registry_base_url,
        semver_add_constraints, should_fallback_to_source,
    };
    use crate::{
        description::DescriptionExt,
        lockfile::{LockedDependency, LockedPackage, LockedRoot, Lockfile},
        r::RuntimeInfo,
        registry::ClosureRoot,
        resolver::{ResolvedDependency, ResolvedPackage},
    };
    use r_description::lossy::RDescription;
    use std::collections::BTreeMap;
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
            vec![
                ClosureRoot {
                    name: "digest".to_string(),
                    constraint: "*".to_string(),
                },
                ClosureRoot {
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
                },
                ResolvedPackage {
                    name: "digest".to_string(),
                    version: "0.6.37".to_string(),
                    source_url: "https://api.rrepo.org/packages/digest/versions/0.6.37/source"
                        .to_string(),
                    dependencies: vec![],
                },
            ],
        );

        assert_eq!(lockfile.registry, "https://api.rrepo.org");
        assert_eq!(lockfile.version, 1);
        assert_eq!(lockfile.roots[0].package, "digest");
        assert_eq!(lockfile.roots[1].package, "cli");
        assert_eq!(lockfile.packages["cli"].source.as_deref(), Some("registry"));
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

        assert_eq!(registry_base_url(), "https://example.test");

        unsafe {
            env::remove_var("RPX_REGISTRY_BASE_URL");
        }
    }

    #[test]
    fn builds_semver_retry_constraints_from_latest_version() {
        assert_eq!(
            semver_add_constraints("1.1.4").unwrap(),
            vec![
                ">= 1.1.4, < 2.0.0".to_string(),
                ">= 1.1, < 2.0.0".to_string(),
                ">= 1, < 2.0.0".to_string(),
                "*".to_string(),
            ]
        );
    }

    #[test]
    fn deduplicates_short_semver_retry_constraints() {
        assert_eq!(
            semver_add_constraints("1").unwrap(),
            vec![">= 1, < 2.0.0".to_string(), "*".to_string(),]
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
    fn pins_existing_roots_from_lockfile_when_adding_new_package() {
        let description = RDescription::from_str(
            "Package: testpkg\nVersion: 0.1.0\nTitle: Test Package\nDescription: Test package for unit tests.\nLicense: MIT\nImports: cli\n",
        )
        .expect("description should parse");
        let lockfile = Lockfile {
            version: 1,
            registry: "https://api.rrepo.org".to_string(),
            roots: vec![LockedRoot {
                package: "cli".to_string(),
                constraint: "*".to_string(),
            }],
            packages: BTreeMap::from([(
                "cli".to_string(),
                LockedPackage {
                    package: "cli".to_string(),
                    version: "3.6.5".to_string(),
                    source: Some("registry".to_string()),
                    source_url: Some(
                        "https://api.rrepo.org/packages/cli/versions/3.6.5/source".to_string(),
                    ),
                    dependencies: vec![LockedDependency {
                        package: "R".to_string(),
                        kind: "Depends".to_string(),
                        min_version: Some("4.3".to_string()),
                        max_version_exclusive: None,
                    }],
                },
            )]),
        };

        let roots = add_closure_roots(
            &description,
            Some(&lockfile),
            &BTreeMap::from([("digest".to_string(), ">= 0.6.37, < 1.0.0".to_string())]),
        );

        assert_eq!(
            roots,
            vec![
                ClosureRoot {
                    name: "cli".to_string(),
                    constraint: "= 3.6.5".to_string(),
                },
                ClosureRoot {
                    name: "digest".to_string(),
                    constraint: ">= 0.6.37, < 1.0.0".to_string(),
                },
            ]
        );
    }

    #[test]
    fn installs_locked_packages_in_dependency_order() {
        let lockfile = Lockfile {
            version: 1,
            registry: "https://api.rrepo.org".to_string(),
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
            version: 1,
            registry: "https://api.rrepo.org".to_string(),
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
}
