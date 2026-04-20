use clap::Parser;
use indicatif::{MultiProgress, ProgressBar, ProgressDrawTarget, ProgressStyle};
use std::{collections::{BTreeMap, BTreeSet}, env, io::IsTerminal, path::Path};

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
use r::{
    InstallFailure, install_source_package, installed_packages, installed_packages_by_name, project_command,
    remove_installed_package_dir, remove_installed_packages,
};
use registry::{ClosureRequest, ClosureRoot, DEFAULT_REGISTRY_BASE_URL, DownloadProgress, DownloadedArtifact, RegistryClient};
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
    let mut project = read_description().expect("failed to read DESCRIPTION");
    let lockfile = read_lockfile_optional().expect("failed to read lockfile");

    if project.description.has_dependency(package) {
        project.description.add_to_imports(package);
        write_description(&project);
        lock_from_description();
        let _ = sync_from_lockfile();
        println!("Added {package}");
        return;
    }

    let registry = registry_base_url();
    let client = RegistryClient::new(&registry);
    let resolved_addition = resolve_addition_from_latest(&project.description, lockfile.as_ref(), package, &client)
        .unwrap_or_else(|error| panic!("failed to add package from registry: {error}"));

    project
        .description
        .add_to_imports_with_constraints(package, &resolved_addition.constraints);
    write_description(&project);
    write_lockfile(lockfile_from_resolution(
        project.description.closure_roots(),
        client.base_url(),
        &resolved_addition.resolved,
    ));
    let _ = sync_from_lockfile();
    println!("Added {package}");
}

fn cmd_remove(package: &str) {
    let mut project = read_description().expect("failed to read DESCRIPTION");
    project.description.remove_from_field("Imports", package);
    project.description.remove_from_field("Depends", package);
    write_description(&project);

    let was_installed = installed_packages_by_name().contains_key(package);
    if was_installed {
        remove_installed_packages(&[package.to_string()]);
    } else {
        remove_installed_package_dir(package);
    }

    lock_from_description();
    let _ = sync_from_lockfile();

    if was_installed {
        println!("Removed {package}");
    } else {
        println!("{package} is already missing from the project library");
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
    constraints: Vec<String>,
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

fn resolve_addition_from_latest(
    description: &r_description::lossy::RDescription,
    lockfile: Option<&Lockfile>,
    package: &str,
    client: &RegistryClient,
) -> Result<AddResolution, String> {
    let latest = client.fetch_latest_version_with_retry(package)?;
    let constraints = semver_add_constraints(&latest.version)?;

    for constraint in constraints {
        let request = ClosureRequest {
            roots: add_closure_roots(description, lockfile, package, &constraint),
        };

        let closure = client.fetch_closure_with_retry(&request)?;
        if let Ok(resolved) = resolve_from_closure(&request, &registry::ClosureResponse::Complete(closure)) {
            return Ok(AddResolution {
                constraints: persisted_constraints(&constraint),
                resolved,
            });
        }
    }

    Err(format!("could not resolve a compatible dependency set for {package}"))
}

fn add_closure_roots(
    description: &r_description::lossy::RDescription,
    lockfile: Option<&Lockfile>,
    new_package: &str,
    new_constraint: &str,
) -> Vec<ClosureRoot> {
    let mut roots = BTreeSet::new();
    let locked_packages = pinned_existing_roots(description, lockfile, new_package);

    for root in description.closure_roots() {
        if root.name == new_package || locked_packages.contains_key(&root.name) {
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

    roots.insert(ClosureRoot {
        name: new_package.to_string(),
        constraint: new_constraint.to_string(),
    });

    roots.into_iter().collect()
}

fn pinned_existing_roots(
    description: &r_description::lossy::RDescription,
    lockfile: Option<&Lockfile>,
    excluded_package: &str,
) -> BTreeMap<String, String> {
    let Some(lockfile) = lockfile else {
        return BTreeMap::new();
    };

    description
        .requirements()
        .into_iter()
        .filter(|name| name != excluded_package)
        .filter_map(|name| {
            lockfile
                .packages
                .get(&name)
                .map(|package| (name, package.version.clone()))
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

    let request = ClosureRequest { roots: roots.clone() };
    let client = RegistryClient::new(&registry);
    let closure = client
        .fetch_closure_with_retry(&request)
        .unwrap_or_else(|error| panic!("failed to resolve lockfile from registry: {error}"));
    let resolved = resolve_from_closure(&request, &registry::ClosureResponse::Complete(closure))
        .unwrap_or_else(|error| panic!("failed to resolve package set from closure: {error}"));

    let lockfile = lockfile_from_resolution(
        roots,
        client.base_url(),
        &resolved,
    );
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
    let install_order = locked_install_order(&lockfile)
        .unwrap_or_else(|error| panic!("failed to compute install order from lockfile: {error}"));
    let exact_reinstalls = install_order
        .into_iter()
        .filter_map(|name| {
            let package = lockfile
                .packages
                .get(&name)
                .expect("ordered package should exist in lockfile");
            match installed.get(&name) {
                Some(installed_package) if installed_package.version == package.version => None,
                _ => Some((name, package.version.clone(), package.source_url.clone())),
            }
        })
        .collect::<Vec<_>>();

    let client = RegistryClient::new(&lockfile.registry);
    let mut ui = SyncUi::new(exact_reinstalls.len());
    let mut outcome = SyncOutcome::default();

    for (index, (name, version, source_url)) in exact_reinstalls.iter().enumerate() {
        let source_url = source_url
            .as_ref()
            .unwrap_or_else(|| panic!("lockfile package {name}@{version} is missing source_url"));
        ui.start_download(index + 1, name, version, None);
        let artifact = client
            .download_source_artifact_with_progress(name, version, source_url, |progress| {
                ui.update_download(progress)
            })
            .unwrap_or_else(|error| panic!("failed to download source artifact: {error}"));
        ui.finish_download(name, version);
        ui.start_install(index + 1, name, version);
        if let Err(error) = install_downloaded_artifact(name, version, artifact) {
            ui.fail_install(name, version);
            report_install_failure(name, version, &error);
            std::process::exit(error.exit_code.unwrap_or(1));
        }
        ui.finish_install(name, version);
        outcome.installed += 1;
    }

    ui.finish();

    let extras = installed_packages_by_name()
        .into_keys()
        .filter(|name| !lockfile.packages.contains_key(name))
        .collect::<Vec<_>>();
    outcome.removed = extras.len();
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
                        source_url: Some(registry_source_url(registry, &package.name, &package.version)),
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

fn install_downloaded_artifact(
    package: &str,
    version: &str,
    artifact: DownloadedArtifact,
) -> Result<(), InstallFailure> {
    let result = install_source_package(artifact.path(), package, version);
    artifact.cleanup();
    result
}

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

        for dependent in dependents
            .get(&name)
            .cloned()
            .unwrap_or_default()
        {
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
    overall: ProgressBar,
    current: ProgressBar,
    total: usize,
}

impl SyncUi {
    fn new(total: usize) -> Self {
        let interactive = std::io::stderr().is_terminal();

        if interactive {
            let multi = MultiProgress::with_draw_target(ProgressDrawTarget::stderr());
            let overall = multi.add(ProgressBar::new(total as u64));
            overall.set_style(
                ProgressStyle::with_template("{bar:40.cyan/blue} {pos}/{len} packages")
                    .expect("progress template should parse")
                    .progress_chars("##-"),
            );
            let current = multi.add(ProgressBar::new_spinner());
            current.set_style(
                ProgressStyle::with_template("{spinner} {msg}")
                    .expect("progress template should parse"),
            );

            Self {
                interactive,
                overall,
                current,
                total,
            }
        } else {
            Self {
                interactive,
                overall: ProgressBar::hidden(),
                current: ProgressBar::hidden(),
                total,
            }
        }
    }

    fn start_download(&mut self, index: usize, name: &str, version: &str, total_bytes: Option<u64>) {
        if self.interactive {
            self.current.set_length(total_bytes.unwrap_or(0));
            if total_bytes.is_some() {
                self.current.set_style(
                    ProgressStyle::with_template(
                        "{spinner} downloading {msg} [{bar:30.cyan/blue}] {bytes}/{total_bytes}",
                    )
                    .expect("progress template should parse")
                    .progress_chars("##-"),
                );
            } else {
                self.current.set_style(
                    ProgressStyle::with_template("{spinner} downloading {msg} {bytes}")
                        .expect("progress template should parse"),
                );
            }
            self.current.set_position(0);
            self.current.enable_steady_tick(std::time::Duration::from_millis(100));
            self.current
                .set_message(format!("{index}/{} {name}@{version}", self.total));
        } else {
            eprintln!("Downloading {index}/{}: {name}@{version}", self.total);
        }
    }

    fn update_download(&self, progress: DownloadProgress) {
        if !self.interactive {
            return;
        }

        if let Some(total_bytes) = progress.total_bytes {
            if self.current.length() != Some(total_bytes) {
                self.current.set_length(total_bytes);
            }
            self.current.set_position(progress.downloaded_bytes.min(total_bytes));
        } else {
            self.current.set_position(progress.downloaded_bytes);
        }
    }

    fn finish_download(&self, name: &str, version: &str) {
        if self.interactive {
            self.current.finish_with_message(format!("downloaded {name}@{version}"));
            self.current.reset();
        }
    }

    fn start_install(&self, index: usize, name: &str, version: &str) {
        if self.interactive {
            self.current.set_style(
                ProgressStyle::with_template("{spinner} installing {msg}")
                    .expect("progress template should parse"),
            );
            self.current.enable_steady_tick(std::time::Duration::from_millis(100));
            self.current
                .set_message(format!("{index}/{} {name}@{version}", self.total));
        } else {
            eprintln!("Installing {index}/{}: {name}@{version}", self.total);
        }
    }

    fn finish_install(&self, name: &str, version: &str) {
        self.overall.inc(1);
        if self.interactive {
            self.current.finish_with_message(format!("installed {name}@{version}"));
            self.current.reset();
        } else {
            eprintln!("Installed {name}@{version}");
        }
    }

    fn fail_install(&self, name: &str, version: &str) {
        if self.interactive {
            self.current.abandon_with_message(format!("failed {name}@{version}"));
        }
    }

    fn finish(&self) {
        if self.interactive {
            self.overall.finish_and_clear();
            self.current.finish_and_clear();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{
        add_closure_roots, locked_install_order, lockfile_from_resolution,
        persisted_constraints, registry_base_url, semver_add_constraints,
    };
    use crate::{
        description::DescriptionExt,
        lockfile::{LockedDependency, LockedPackage, LockedRoot, Lockfile},
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
                    dependencies: vec![ResolvedDependency {
                        package: "R".to_string(),
                        kind: "Depends".to_string(),
                        min_version: Some("4.3".to_string()),
                        max_version_exclusive: None,
                    }, ResolvedDependency {
                        package: "utils".to_string(),
                        kind: "Imports".to_string(),
                        min_version: None,
                        max_version_exclusive: None,
                    }, ResolvedDependency {
                        package: "base".to_string(),
                        kind: "Depends".to_string(),
                        min_version: None,
                        max_version_exclusive: None,
                    }],
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

        let roots = add_closure_roots(&description, Some(&lockfile), "digest", ">= 0.6.37, < 1.0.0");

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
                        source_url: Some("https://api.rrepo.org/packages/AzureKeyVault/versions/1.0.0/source".to_string()),
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
                        source_url: Some("https://api.rrepo.org/packages/AzureRMR/versions/1.0.0/source".to_string()),
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
                        source_url: Some("https://api.rrepo.org/packages/httr2/versions/1.0.0/source".to_string()),
                        dependencies: vec![],
                    },
                ),
            ]),
        };

        assert_eq!(
            locked_install_order(&lockfile).unwrap(),
            vec!["httr2".to_string(), "AzureRMR".to_string(), "AzureKeyVault".to_string()]
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
}
