use clap::Parser;
use futures_util::StreamExt;
use miette::Diagnostic;
use std::{
    collections::{BTreeMap, BTreeSet},
    env, fs,
    io::IsTerminal,
    path::{Path, PathBuf},
    sync::{Arc, OnceLock},
    time::{SystemTime, UNIX_EPOCH},
};
use thiserror::Error;
use tokio::process::Command;
use tracing::Instrument;
use tracing_indicatif::{
    filter::{IndicatifFilter, hide_indicatif_span_fields},
    span_ext::IndicatifSpanExt,
    style::ProgressStyle,
};
use tracing_subscriber::{
    EnvFilter,
    fmt::format::DefaultFields,
    layer::{Layer, SubscriberExt},
    util::SubscriberInitExt,
};

mod cache;
mod cli;
mod description;
mod http;
mod lockfile;
mod output;
mod project;
mod r;
mod registry;
mod repository;
mod resolver;
mod sysreqs;
mod ui;

use cli::{Cli, Commands, RepoCommands};
use description::{
    DescriptionDependency, init_description, read_description, relation_with_constraint,
    resolution_root_from_relation, write_description,
};
use lockfile::{
    LOCKFILE_REVISION, LOCKFILE_VERSION, LockedR, LockedRepository, LockedRepositoryKind,
    LockedSystemRequirements, Lockfile, read_lockfile, read_lockfile_optional, write_lockfile,
};
use output::{blank_note_line, blank_status_line, note, prompt, status, warning};
use project::{
    artifact_cache_path, build_temp_library_path, cache_dir_path, project_library_path,
    project_library_root_path,
};
use pubgrub::Ranges;
use r::{InstallFailure, base_packages, install_local_package, installed_packages, runtime_info};
use registry::{DEFAULT_REGISTRY_BASE_URL, ResolutionRoot};
use repository::{RepositoryKind, RepositorySet, RepositorySource, normalize_repository_url};
use resolver::{is_base_package, resolve_from_registry};
use sysreqs::{
    SystemDependencyPlan, cached_latest_snapshot, current_host_platform,
    empty_snapshot as empty_sysreq_snapshot, install as install_system_dependencies,
    latest_snapshot as latest_sysreq_snapshot, preview_commands as sysreq_preview_commands,
    recheck_missing_packages as recheck_system_missing_packages,
    refresh_metadata as refresh_system_metadata,
    refresh_preview_command as system_metadata_refresh_preview,
    resolve_plan as resolve_system_plan,
};
use ui::SystemDepsUi;

use crate::{
    cache::CompiledPackageCacheKey,
    lockfile::LockedPackage,
    r::{RVirtualEnv, installed_packages_async, r_version_async, remove_packages_from_venv},
    repository::{ArchiveSupport, PackageRepository, RepositoryType},
    resolver::{PackageVersion, package_range},
};
use tokio::io::AsyncWriteExt;

static REPOSITORY_CLASSIFIER_RUNTIME: OnceLock<tokio::runtime::Runtime> = OnceLock::new();

type RpxResult<T> = Result<T, RpxError>;

#[derive(Debug, Error, Diagnostic)]
enum RpxError {
    #[error(transparent)]
    #[diagnostic(transparent)]
    Description(#[from] description::DescriptionError),

    #[error(transparent)]
    #[diagnostic(transparent)]
    Project(#[from] ProjectError),

    #[error(transparent)]
    #[diagnostic(transparent)]
    Repo(#[from] RepoError),

    #[error(transparent)]
    #[diagnostic(transparent)]
    Run(#[from] RunError),

    #[error(transparent)]
    #[diagnostic(transparent)]
    Lock(#[from] LockError),

    #[error(transparent)]
    #[diagnostic(transparent)]
    Status(#[from] StatusError),

    #[error(transparent)]
    #[diagnostic(transparent)]
    Sync(Box<SyncError>),

    #[error(transparent)]
    #[diagnostic(transparent)]
    Clean(#[from] CleanError),
}

#[derive(Debug, Error, Diagnostic)]
enum ProjectError {
    #[error("failed to read rpx.lock: {details}")]
    #[diagnostic(
        code(rpx::project::lockfile_read),
        help("Run `rpx lock` to regenerate rpx.lock.")
    )]
    LockfileRead { details: String },

    #[error("failed to write rpx.lock: {details}")]
    #[diagnostic(code(rpx::project::lockfile_write))]
    LockfileWrite { details: String },
}

#[derive(Debug, Error, Diagnostic)]
enum RepoError {
    #[error("failed to add repository {url}: {details}")]
    #[diagnostic(code(rpx::repo::add_failed))]
    Add { url: String, details: String },

    #[error("failed to remove repository credential: {details}")]
    #[diagnostic(code(rpx::repo::credential_remove_failed))]
    CredentialRemove { details: String },

    #[error("failed to inspect repository credential: {details}")]
    #[diagnostic(code(rpx::repo::credential_inspect_failed))]
    CredentialInspect { details: String },
}

impl From<SyncError> for RpxError {
    fn from(error: SyncError) -> Self {
        Self::Sync(Box::new(error))
    }
}

#[derive(Debug, Error, Diagnostic)]
enum RunError {
    #[error("failed to run {program}")]
    #[diagnostic(code(rpx::run::command_failed))]
    CommandFailed {
        program: String,
        #[source]
        source: std::io::Error,
    },
}

#[derive(Debug, Error, Diagnostic)]
enum LockError {
    #[error("lockfile is incompatible")]
    #[diagnostic(
        code(rpx::lockfile::newer),
        help("Upgrade rpx or regenerate the lockfile with this version.")
    )]
    LockfileNewer,

    #[error("failed to resolve package set from registry: {details}")]
    #[diagnostic(
        code(rpx::lock::resolve_failed),
        help("Check package names and version constraints in DESCRIPTION.")
    )]
    ResolveFailed { details: String },
}

#[derive(Debug, Error, Diagnostic)]
enum StatusError {
    #[error("lockfile is out of date")]
    #[diagnostic(code(rpx::lockfile::older), help("Run `rpx lock` to update rpx.lock."))]
    LockfileOlder,

    #[error("lockfile is incompatible")]
    #[diagnostic(
        code(rpx::lockfile::newer),
        help("Upgrade rpx or regenerate the lockfile with this version.")
    )]
    LockfileNewer,
}

#[derive(Debug, Error, Diagnostic)]
enum SyncError {
    #[error("lockfile is out of date")]
    #[diagnostic(code(rpx::lockfile::older), help("Run `rpx lock` to update rpx.lock."))]
    LockfileOlder,

    #[error("lockfile is incompatible")]
    #[diagnostic(
        code(rpx::lockfile::newer),
        help("Upgrade rpx or regenerate the lockfile with this version.")
    )]
    LockfileNewer,

    #[error(
        "system dependency installation is currently supported only on supported Linux distributions/package managers"
    )]
    #[diagnostic(code(rpx::sync::unsupported_system_install))]
    UnsupportedSystemInstall,

    #[error("R runtime is missing required base packages: {packages}")]
    #[diagnostic(
        code(rpx::sync::runtime_missing_base_packages),
        help(
            "These packages are part of R itself. Use a complete R installation compatible with this project."
        )
    )]
    RuntimeMissingBasePackages { packages: String },

    #[error("failed to refresh package metadata: {details}")]
    #[diagnostic(code(rpx::sync::metadata_refresh_failed))]
    MetadataRefreshFailed { details: String },

    #[error("failed to install system dependencies: {details}")]
    #[diagnostic(code(rpx::sync::system_dependencies_failed))]
    SystemDependenciesFailed { details: String },

    #[error("failed to prepare source artifacts: {details}")]
    #[diagnostic(code(rpx::sync::download_failed))]
    DownloadArtifactsFailed { details: String },
}

#[derive(Debug, Error, Diagnostic)]
enum CleanError {
    #[error("failed to remove {label} at {path}")]
    #[diagnostic(code(rpx::clean::remove_failed))]
    RemoveFailed {
        label: String,
        path: String,
        #[source]
        source: std::io::Error,
    },
}

#[derive(Debug, Error, Diagnostic)]
enum RpxWarning {
    #[error("using cached system requirements database snapshot")]
    #[diagnostic(
        severity(Warning),
        code(rpx::sysreqs::cached_snapshot),
        help("Run `rpx lock` later to refresh locked system dependency metadata.")
    )]
    CachedSysreqSnapshot,

    #[error("using system requirements database pinned by the existing lockfile ({commit})")]
    #[diagnostic(
        severity(Warning),
        code(rpx::sysreqs::pinned_snapshot),
        help("Run `rpx lock` later to refresh locked system dependency metadata.")
    )]
    PinnedSysreqSnapshot { commit: String },

    #[error(
        "system requirements database unavailable; continuing without updating locked system dependency rules"
    )]
    #[diagnostic(
        severity(Warning),
        code(rpx::sysreqs::unavailable),
        help("Check network access and run `rpx lock` again when the database is reachable.")
    )]
    SysreqUnavailable,

    #[error("failed to prepare system dependency plan: {details}")]
    #[diagnostic(
        severity(Warning),
        code(rpx::sysreqs::plan_failed),
        help("rpx will continue with the system requirement rules recorded in rpx.lock.")
    )]
    SystemPlanFailed { details: String },

    #[error("some system requirement rules do not have an install mapping for {host}: {rules}")]
    #[diagnostic(severity(Warning), code(rpx::sysreqs::unsupported_rules))]
    UnsupportedSystemRequirementRules { host: String, rules: String },

    #[error("{details}")]
    #[diagnostic(
        severity(Warning),
        code(rpx::runtime::version_mismatch),
        help("Use the R version recorded in rpx.lock for the most reproducible install.")
    )]
    RuntimeVersionMismatch { details: String },

    #[error("continuing with R package sync without installing system dependencies")]
    #[diagnostic(
        severity(Warning),
        code(rpx::sync::system_dependencies_skipped),
        help("Run `rpx sync --install-system` to install missing system dependencies first.")
    )]
    ContinuingWithoutSystemDependencies,
}

fn read_project_lockfile() -> Result<Lockfile, ProjectError> {
    read_lockfile().map_err(|details| ProjectError::LockfileRead { details })
}

fn read_project_lockfile_optional() -> Result<Option<Lockfile>, ProjectError> {
    read_lockfile_optional().map_err(|details| ProjectError::LockfileRead { details })
}

fn write_project_lockfile(lockfile: &Lockfile) -> Result<(), ProjectError> {
    write_lockfile(lockfile).map_err(|details| ProjectError::LockfileWrite { details })
}

/// Runs the CLI application.
///
/// # Errors
///
/// Returns an error when command execution or diagnostic rendering fails.
pub async fn run() -> miette::Result<()> {
    run_inner().await?;
    Ok(())
}

fn init_tracing() {
    let indicatif_layer = tracing_indicatif::IndicatifLayer::new()
        .with_span_field_formatter(hide_indicatif_span_fields(DefaultFields::new()))
        .with_progress_style(progress_spinner_style())
        .with_max_progress_bars(
            10,
            Some(
                ProgressStyle::with_template("...and {pending_progress_bars} more packages")
                    .expect("progress footer style should be valid"),
            ),
        );
    let filter = EnvFilter::new("warn,reqwest_tracing=info,rpx=info");
    let fmt_layer =
        tracing_subscriber::fmt::layer().with_writer(indicatif_layer.get_stderr_writer());

    let _ = tracing_subscriber::registry()
        .with(filter)
        .with(fmt_layer)
        .with(indicatif_layer.with_filter(IndicatifFilter::new(false)))
        .try_init();
}

fn progress_spinner_style() -> ProgressStyle {
    ProgressStyle::with_template("{span_child_prefix}{spinner} {msg}")
        .expect("progress spinner style should be valid")
}

fn progress_bar_style() -> ProgressStyle {
    ProgressStyle::with_template(
        "{span_child_prefix}{spinner} {msg} [{bar:24.cyan/blue}] {bytes}/{total_bytes}",
    )
    .expect("progress bar style should be valid")
}

async fn run_inner() -> RpxResult<()> {
    let cli = Cli::parse();

    match cli.command {
        Commands::Init => cmd_init(),
        Commands::Add {
            default_repo,
            no_default_repo,
            packages,
        } => {
            cmd_add(
                &packages,
                DefaultRepositoryPreference::from_flags(default_repo, no_default_repo),
            )
            .await
        }
        Commands::Remove {
            default_repo,
            no_default_repo,
            packages,
        } => {
            cmd_remove(
                &packages,
                DefaultRepositoryPreference::from_flags(default_repo, no_default_repo),
            )
            .await
        }
        Commands::Run { command } => cmd_run(&command).await,
        Commands::Lock {
            default_repo,
            no_default_repo,
        } => {
            cmd_lock(DefaultRepositoryPreference::from_flags(
                default_repo,
                no_default_repo,
            ))
            .await
        }
        Commands::Status => cmd_status(),
        Commands::Sync {
            install_system,
            install_only_system,
        } => cmd_sync(install_system, install_only_system),
        Commands::Clean => cmd_clean(),
        Commands::Repo { command } => cmd_repo(command).await,
    }
}

fn cmd_init() -> RpxResult<()> {
    let path = init_description()?;
    status(format_args!("Initialized project at {path}"));
    status("Next: run `rpx add <package>` or `rpx lock`");
    Ok(())
}

async fn cmd_add(
    packages: &[String],
    repository_preference: DefaultRepositoryPreference,
) -> RpxResult<()> {
    let mut description = read_description()?;
    let existing_lockfile = read_project_lockfile_optional()?;
    validate_optional_lockfile_for_lock(existing_lockfile.as_ref())?;
    let matching_lockfile =
        matching_lockfile_for_description(existing_lockfile.as_ref(), &description);
    let repositories = repository_preference
        .package_repositories(&description, matching_lockfile)
        .await
        .map_err(|details| LockError::ResolveFailed { details })?;

    let mut desired_roots = roots_from_lockfile_or_description(matching_lockfile, &description);
    let new_packages = packages
        .iter()
        .filter(|package| !desired_roots.contains_key(package.as_str()))
        .cloned()
        .collect::<Vec<_>>();
    for package in &new_packages {
        desired_roots.insert(package.clone(), "*".to_string());
    }

    let preferred_versions = preferred_versions_from_lockfile(
        matching_lockfile,
        &repositories,
        &new_packages.iter().cloned().collect::<BTreeSet<_>>(),
    )?;
    let lockfile = lockfile_from_roots(
        repositories,
        desired_roots,
        preferred_versions,
        existing_lockfile.as_ref(),
        None,
    )
    .await?;

    apply_added_packages_to_description(&mut description, &new_packages, &lockfile)?;

    write_description(&description)?;
    write_project_lockfile(&lockfile)?;
    let _ = sync_from_lockfile(false, false)?;
    status(format_args!("Added {}", packages.join(", ")));
    Ok(())
}

async fn cmd_repo(command: RepoCommands) -> RpxResult<()> {
    match command {
        RepoCommands::Add { url } => cmd_repo_add(&url).await,
        RepoCommands::Remove {
            url,
            remove_credential,
        } => cmd_repo_remove(&url, remove_credential),
        RepoCommands::List => cmd_repo_list(),
    }
}

async fn cmd_repo_add(url: &str) -> RpxResult<()> {
    let mut description = read_description()?;
    let new_repo = PackageRepository::from_url(&http::client(), url)
        .await
        .map_err(|details| RepoError::Add {
            url: normalize_repository_url(url),
            details,
        })?;

    let mut additional_repositories = description.additional_repositories.clone();
    if additional_repositories
        .iter()
        .any(|existing| normalize_repository_url(existing) == new_repo.base_url().as_str())
    {
        status(format_args!(
            "Repository already configured: {}",
            new_repo.base_url().as_str()
        ));
        return Ok(());
    }

    additional_repositories.push(new_repo.base_url().to_string());
    description.additional_repositories = additional_repositories;
    write_description(&description)?;
    status(format_args!(
        "Added repository {}",
        new_repo.base_url().to_string()
    ));
    Ok(())
}

fn repository_source_from_lockfile(lockfile: &Lockfile, url: &str) -> Option<RepositorySource> {
    let normalized = normalize_repository_url(url);
    lockfile
        .repositories
        .iter()
        .find(|repository| repository.url == normalized)
        .map(repository_source_from_locked)
}

fn repository_source_from_locked(repository: &LockedRepository) -> RepositorySource {
    match repository.kind {
        LockedRepositoryKind::Rrepo => RepositorySource::new(&repository.url),
        LockedRepositoryKind::CranLike => RepositorySource::cran_like_with_archive_support(
            &repository.url,
            match repository.cran_archive_support {
                Some(sup) => sup,
                None => ArchiveSupport::Unavailable,
            },
        ),
    }
}

fn repository_kind_label(lockfile: Option<&Lockfile>, url: &str) -> &'static str {
    match lockfile.and_then(|lockfile| repository_source_from_lockfile(lockfile, url)) {
        Some(source) => match source.kind() {
            RepositoryKind::Rrepo => "rrepo",
            RepositoryKind::CranLike => "CRAN-like",
        },
        None => "unknown",
    }
}

fn cmd_repo_remove(url: &str, remove_credential: bool) -> RpxResult<()> {
    let mut description = read_description()?;
    let source = RepositorySource::new(url);
    let mut additional_repositories = description.additional_repositories.clone();
    let original_len = additional_repositories.len();
    additional_repositories
        .retain(|repository| normalize_repository_url(repository) != source.base_url());

    if additional_repositories.len() == original_len {
        status(format_args!(
            "Repository not configured: {}",
            source.base_url()
        ));
        return Ok(());
    }

    description.additional_repositories = additional_repositories;
    write_description(&description)?;

    if remove_credential {
        RepositorySet::new(vec![source.clone()])
            .remove_api_key(&source)
            .map_err(|details| RepoError::CredentialRemove { details })?;
    }

    status(format_args!("Removed repository {}", source.base_url()));
    Ok(())
}

fn cmd_repo_list() -> RpxResult<()> {
    let description = read_description()?;
    let lockfile = read_project_lockfile_optional()?;
    let additional_repositories = &description.additional_repositories;

    if additional_repositories.is_empty() {
        status("No additional repositories configured");
        return Ok(());
    }

    for url in additional_repositories {
        let source = lockfile
            .as_ref()
            .and_then(|lockfile| repository_source_from_lockfile(lockfile, url))
            .unwrap_or_else(|| RepositorySource::new(url));
        let repositories = RepositorySet::new(vec![source.clone()]);
        let credential = repositories
            .has_stored_credential(&source)
            .map_err(|details| RepoError::CredentialInspect { details })?;
        status(format_args!(
            "{} [{}; {}]",
            normalize_repository_url(url),
            repository_kind_label(lockfile.as_ref(), url),
            if credential {
                "credential stored"
            } else {
                "no credential"
            }
        ));
    }
    Ok(())
}

async fn cmd_remove(
    packages: &[String],
    repository_preference: DefaultRepositoryPreference,
) -> RpxResult<()> {
    let mut description = read_description()?;
    let existing_lockfile = read_project_lockfile_optional()?;
    validate_optional_lockfile_for_lock(existing_lockfile.as_ref())?;
    let matching_lockfile =
        matching_lockfile_for_description(existing_lockfile.as_ref(), &description);
    let repositories = repository_preference
        .package_repositories(&description, matching_lockfile)
        .await
        .map_err(|details| LockError::ResolveFailed { details })?;

    let mut desired_roots = roots_from_lockfile_or_description(matching_lockfile, &description);
    for package in packages {
        desired_roots.remove(package);
        description
            .imports
            .retain(|dependency| dependency.name != *package);
        description
            .depends
            .retain(|dependency| dependency.name != *package);
    }

    let preferred_versions = preferred_versions_from_lockfile(
        matching_lockfile,
        &repositories,
        &packages.iter().cloned().collect::<BTreeSet<_>>(),
    )?;
    let lockfile = lockfile_from_roots(
        repositories,
        desired_roots,
        preferred_versions,
        existing_lockfile.as_ref(),
        None,
    )
    .await?;

    write_description(&description)?;
    write_project_lockfile(&lockfile)?;
    let _ = sync_from_lockfile(false, false)?;

    Ok(())
}

async fn cmd_run(command: &[String]) -> RpxResult<()> {
    let (program, args) = command
        .split_first()
        .expect("run command requires at least one argument");

    let status = Command::with_venv(program)
        .args(args)
        .status()
        .await
        .map_err(|source| RunError::CommandFailed {
            program: program.clone(),
            source,
        })?;

    exit_with_status(status.code());
    Ok(())
}

async fn cmd_lock(repository_preference: DefaultRepositoryPreference) -> RpxResult<()> {
    let outcome = lock_from_description(repository_preference).await?;
    if outcome.changed {
        status("Updated rpx.lock");
    } else {
        status("rpx.lock is already up to date");
    }
    Ok(())
}

fn cmd_sync(install_system: bool, install_only_system: bool) -> RpxResult<()> {
    if (install_system || install_only_system) && !host_supports_system_sync() {
        return Err(SyncError::UnsupportedSystemInstall.into());
    }

    let outcome = sync_from_lockfile(install_system, install_only_system)?;
    if install_only_system {
        return Ok(());
    }
    if outcome.installed == 0 && outcome.removed == 0 {
        status("Project library is already in sync");
    } else {
        status("Synchronized project library");
    }
    Ok(())
}

fn cmd_status() -> RpxResult<()> {
    let description = read_description()?;
    let lockfile = read_project_lockfile()?;

    match validate_lockfile_compatibility(&lockfile) {
        Ok(()) => {}
        Err(LockfileCompatibilityError::Older) => return Err(StatusError::LockfileOlder.into()),
        Err(LockfileCompatibilityError::Newer) => return Err(StatusError::LockfileNewer.into()),
    }

    let manifest_requirements = description
        .imports
        .iter()
        .chain(
            description
                .depends
                .iter()
                .filter(|relation| relation.name != "R"),
        )
        .map(|relation| relation.name.clone())
        .collect::<BTreeSet<_>>();
    let lock_requirements = lockfile
        .roots
        .iter()
        .map(|root| root.package.clone())
        .collect::<BTreeSet<_>>();
    let installed = installed_packages();
    let installed_names = installed
        .iter()
        .map(|package| &package.package)
        .collect::<BTreeSet<&String>>();
    let installed_versions = installed
        .iter()
        .map(|package| (package.package.clone(), package.version.clone()))
        .collect::<std::collections::BTreeMap<_, _>>();
    let locked_names = lockfile.packages.keys().collect::<BTreeSet<&String>>();
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
        && system_plan.as_ref().is_none_or(|plan| {
            plan.missing_packages.is_empty() && plan.unsupported_rules.is_empty()
        })
    {
        print_runtime_version_warning(&runtime_status);
        status("Project is in sync");
        return Ok(());
    }

    let lockfile_out_of_date = !missing_from_lockfile.is_empty() || !extra_in_lockfile.is_empty();
    let library_out_of_date = !missing_from_library.is_empty()
        || !extra_in_library.is_empty()
        || !version_mismatches.is_empty();
    let runtime_out_of_date = !runtime_status.missing_base_packages.is_empty();
    let system_out_of_date = system_plan.as_ref().is_some_and(|plan| {
        !plan.missing_packages.is_empty() || !plan.unsupported_rules.is_empty()
    });

    if lockfile_out_of_date && library_out_of_date {
        status("Project is out of sync");
        blank_status_line();
        status("Run: rpx lock && rpx sync");
    } else if lockfile_out_of_date {
        status("Lockfile is out of date");
        blank_status_line();
        status("Run: rpx lock");
    } else if runtime_out_of_date {
        status("R runtime is out of sync");
    } else if system_out_of_date {
        status("System dependencies are out of sync");
    } else {
        status("Project library is out of sync");
        blank_status_line();
        status("Run: rpx sync");
    }

    print_status_group(
        "Packages in DESCRIPTION but not locked:",
        &missing_from_lockfile,
    );
    print_status_group(
        "Packages locked but no longer in DESCRIPTION:",
        &extra_in_lockfile,
    );
    // TODO: get it back when infrerence is back lol
    // print_status_group("Packages locked but not installed:", &missing_from_library);
    // print_status_group("Packages installed but not locked:", &extra_in_library);
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

fn cmd_clean() -> RpxResult<()> {
    let mut removed_any = false;

    removed_any |= remove_dir_if_exists(&project_library_root_path(), "project library")?;
    removed_any |= remove_dir_if_exists(&cache_dir_path(), "cache directory")?;

    if removed_any {
        status("Removed project library and cache directories");
    } else {
        status("Project library and cache directories are already clean");
    }
    Ok(())
}

fn remove_dir_if_exists(path: &Path, label: &str) -> RpxResult<bool> {
    if !path.exists() {
        return Ok(false);
    }

    fs::remove_dir_all(path).map_err(|source| CleanError::RemoveFailed {
        label: label.to_string(),
        path: path.display().to_string(),
        source,
    })?;
    Ok(true)
}

fn print_status_group(title: &str, items: &[String]) {
    if items.is_empty() {
        return;
    }

    blank_status_line();
    status(title);
    for item in items {
        status(format_args!("- {item}"));
    }
}

#[derive(Debug, Default, PartialEq, Eq)]
struct LockOutcome {
    changed: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DefaultRepositoryPreference {
    FromLockfileOrDefault,
    Enabled,
    Disabled,
}

impl DefaultRepositoryPreference {
    fn from_flags(default_repo: bool, no_default_repo: bool) -> Self {
        if default_repo {
            Self::Enabled
        } else if no_default_repo {
            Self::Disabled
        } else {
            Self::FromLockfileOrDefault
        }
    }

    async fn package_repositories(
        self,
        description: &description::RDescription,
        lockfile: Option<&Lockfile>,
    ) -> Result<Vec<PackageRepository>, String> {
        let mut repos = match lockfile {
            Some(l) => l
                .repositories
                .iter()
                .map(|lr| {
                    let url = reqwest::Url::parse(&lr.url).map_err(|error| error.to_string())?;

                    let repo_type = locked_repository_type(
                        lr.kind,
                        lr.cran_archive_support
                            .unwrap_or(ArchiveSupport::Unavailable),
                    );

                    Ok(PackageRepository::new(url, repo_type))
                })
                .collect::<Result<Vec<_>, String>>()?,

            None => {
                let client = http::client();

                futures_util::future::join_all(
                    description
                        .additional_repositories
                        .iter()
                        .map(|url| PackageRepository::from_url(&client, url)),
                )
                .await
                .into_iter()
                .collect::<Result<Vec<_>, String>>()?
            }
        };

        if self == Self::Enabled || (self == Self::FromLockfileOrDefault && lockfile.is_none()) {
            let default = default_repository().await?;
            if !repos.contains(&default) {
                repos.insert(0, default);
            }
        }

        Ok(repos)
    }
}

#[derive(Debug, Default, PartialEq, Eq)]
struct SyncOutcome {
    installed: usize,
    removed: usize,
}

#[derive(Debug, PartialEq, Eq)]
enum LockfileCompatibilityError {
    Older,
    Newer,
}

fn matching_lockfile_for_description<'a>(
    lockfile: Option<&'a Lockfile>,
    description: &description::RDescription,
) -> Option<&'a Lockfile> {
    lockfile.filter(|lockfile| roots_from_lockfile(lockfile) == roots_from_description(description))
}

fn roots_from_lockfile_or_description(
    lockfile: Option<&Lockfile>,
    description: &description::RDescription,
) -> BTreeMap<String, String> {
    match lockfile {
        Some(lockfile) => roots_from_lockfile(lockfile),
        None => roots_from_description(description),
    }
}

fn roots_from_lockfile(lockfile: &Lockfile) -> BTreeMap<String, String> {
    lockfile
        .roots
        .iter()
        .map(|root| (root.package.clone(), root.constraint.clone()))
        .collect()
}

fn roots_from_description(description: &description::RDescription) -> BTreeMap<String, String> {
    description
        .imports
        .iter()
        .chain(
            description
                .depends
                .iter()
                .filter(|relation| relation.name != "R"),
        )
        .map(resolution_root_from_relation)
        .map(|root| (root.name, root.constraint))
        .collect()
}

fn resolution_roots_from_map(roots: &BTreeMap<String, String>) -> Vec<ResolutionRoot> {
    roots
        .iter()
        .map(|(name, constraint)| ResolutionRoot {
            name: name.clone(),
            constraint: constraint.clone(),
        })
        .collect()
}

fn root_dependency_ranges(
    repositories: &[PackageRepository],
    roots: &BTreeMap<String, String>,
) -> Result<BTreeMap<String, Ranges<PackageVersion>>, String> {
    let repository = repositories
        .first()
        .cloned()
        .ok_or_else(|| "no repositories configured".to_string())?;
    let repository = Arc::new(repository);

    roots
        .iter()
        .map(|(package, constraint)| {
            Ok((
                package.clone(),
                package_range(Arc::clone(&repository), constraint)?,
            ))
        })
        .collect()
}

fn preferred_versions_from_lockfile(
    lockfile: Option<&Lockfile>,
    repositories: &[PackageRepository],
    excluded_packages: &BTreeSet<String>,
) -> RpxResult<BTreeMap<String, PackageVersion>> {
    let Some(lockfile) = lockfile else {
        return Ok(BTreeMap::new());
    };

    lockfile
        .packages
        .iter()
        .filter(|(name, _)| !excluded_packages.contains(name.as_str()))
        .map(|(name, package)| {
            let repository = repository_for_locked_package(repositories, package)?;
            let version = package
                .version
                .parse::<r_description::Version>()
                .map_err(|error| LockError::ResolveFailed {
                    details: format!(
                        "invalid locked version {} for {name}: {error}",
                        package.version
                    ),
                })?;

            let version = PackageVersion::new(version, repository);

            Ok((name.clone(), version))
        })
        .collect()
}

fn repository_for_locked_package(
    repositories: &[PackageRepository],
    package: &LockedPackage,
) -> RpxResult<Arc<PackageRepository>> {
    if let Some(source_url) = package.source_url.as_deref()
        && let Some(repository) = repositories
            .iter()
            .find(|repository| source_url.starts_with(repository.base_url().as_str()))
    {
        return Ok(Arc::new(repository.clone()));
    }

    repositories.first().cloned().map(Arc::new).ok_or_else(|| {
        LockError::ResolveFailed {
            details: format!(
                "no repository available for locked package {}",
                package.package
            ),
        }
        .into()
    })
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

fn apply_added_packages_to_description(
    description: &mut description::RDescription,
    packages: &[String],
    lockfile: &Lockfile,
) -> RpxResult<()> {
    for package in packages {
        if is_base_package(package) {
            continue;
        }

        let Some(locked) = lockfile.packages.get(package) else {
            return Err(LockError::ResolveFailed {
                details: format!("missing resolved version for {package}"),
            }
            .into());
        };

        let constraints = persisted_constraints(
            &semver_add_constraint(&locked.version)
                .map_err(|details| LockError::ResolveFailed { details })?,
        );

        if constraints.is_empty()
            || constraints
                .iter()
                .all(|constraint| constraint.trim() == "*")
        {
            description.imports.insert(DescriptionDependency {
                name: package.clone(),
                version: None,
            });
        } else {
            description.imports.extend(
                constraints
                    .iter()
                    .map(|constraint| relation_with_constraint(package, constraint))
                    .collect::<Result<Vec<_>, _>>()
                    .expect("constraints should parse"),
            );
        }
    }

    Ok(())
}

async fn lock_from_description(
    repository_preference: DefaultRepositoryPreference,
) -> RpxResult<LockOutcome> {
    let description = read_description()?;
    let existing_lockfile = read_project_lockfile_optional()?;
    validate_optional_lockfile_for_lock(existing_lockfile.as_ref())?;
    let matching_lockfile =
        matching_lockfile_for_description(existing_lockfile.as_ref(), &description);
    let repositories = repository_preference
        .package_repositories(&description, matching_lockfile)
        .await
        .map_err(|details| LockError::ResolveFailed { details })?;
    let roots = roots_from_lockfile_or_description(matching_lockfile, &description);
    let preferred_versions =
        preferred_versions_from_lockfile(matching_lockfile, &repositories, &BTreeSet::new())?;

    let lockfile = lockfile_from_roots(
        repositories,
        roots,
        preferred_versions,
        existing_lockfile.as_ref(),
        None,
    )
    .await?;
    let changed = existing_lockfile.as_ref() != Some(&lockfile);
    write_project_lockfile(&lockfile)?;
    Ok(LockOutcome { changed })
}

fn validate_optional_lockfile_for_lock(lockfile: Option<&Lockfile>) -> RpxResult<()> {
    if let Some(lockfile) = lockfile
        && lockfile.version > LOCKFILE_VERSION
    {
        return Err(LockError::LockfileNewer.into());
    }

    Ok(())
}

fn load_sysreq_snapshot_for_lock(
    existing_lockfile: Option<&Lockfile>,
) -> sysreqs::SysreqDbSnapshot {
    if let Ok(snapshot) = latest_sysreq_snapshot() {
        return snapshot;
    }

    if let Ok(Some(snapshot)) = cached_latest_snapshot() {
        warning(RpxWarning::CachedSysreqSnapshot);
        return snapshot;
    }

    if let Some(commit) = existing_lockfile
        .map(|lockfile| lockfile.sysreqs.db_commit.as_str())
        .filter(|commit| !commit.is_empty())
        && let Ok(snapshot) = sysreqs::snapshot_for_commit(commit)
    {
        warning(RpxWarning::PinnedSysreqSnapshot {
            commit: commit.to_string(),
        });
        return snapshot;
    }

    warning(RpxWarning::SysreqUnavailable);
    empty_sysreq_snapshot()
}

fn sync_from_lockfile(install_system: bool, install_only_system: bool) -> RpxResult<SyncOutcome> {
    let description = read_description()?;
    let manifest_requirements = description
        .imports
        .iter()
        .chain(
            description
                .depends
                .iter()
                .filter(|relation| relation.name != "R"),
        )
        .map(|relation| relation.name.clone())
        .collect::<BTreeSet<_>>();
    let lockfile = read_project_lockfile()?;
    validate_lockfile_compatibility_for_sync(&lockfile)?;
    validate_runtime_for_sync(&lockfile)?;
    if host_supports_system_sync() {
        let system_plan = system_plan_from_lockfile(&lockfile).unwrap_or_else(|error| {
            warning(RpxWarning::SystemPlanFailed { details: error });
            system_plan_without_db(&lockfile)
        });
        let proceed_with_r =
            handle_system_requirements(&system_plan, install_system, install_only_system)?;
        if install_only_system || !proceed_with_r {
            return Ok(SyncOutcome::default());
        }
    }
    let lock_requirements = lockfile
        .roots
        .iter()
        .map(|root| root.package.clone())
        .collect::<BTreeSet<_>>();

    if manifest_requirements != lock_requirements {
        return Err(SyncError::LockfileOlder.into());
    }

    let mut outcome = SyncOutcome::default();

    let extra_packages = installed_packages()
        .into_iter()
        .filter_map(|p| match lockfile.packages.get(&p.package) {
            Some(locked) if locked.version == p.version => None,
            _ => Some(p.package),
        })
        .collect::<Vec<_>>();

    outcome.removed = extra_packages.len();
    let _ = remove_packages_from_venv(&extra_packages);

    init_tracing();

    REPOSITORY_CLASSIFIER_RUNTIME
        .get_or_init(|| tokio::runtime::Runtime::new().expect("install runtime should start"))
        .block_on(install_locked_packages(
            lockfile.packages.values().cloned().collect::<Vec<_>>(),
            lockfile.repositories.iter().cloned().collect::<Vec<_>>(),
        ))
        .map_err(|details| SyncError::DownloadArtifactsFailed { details })?;

    return Ok(outcome);
}

pub(crate) fn exit_with_status(code: Option<i32>) {
    if code != Some(0) {
        std::process::exit(code.unwrap_or(1));
    }
}

async fn default_repository() -> Result<PackageRepository, String> {
    match env::var("RPX_REGISTRY_BASE_URL") {
        Ok(url) => {
            let normalized_url = normalize_repository_url(&url);

            PackageRepository::from_url(&http::client(), &normalized_url)
                .await
                .map_err(|details| {
                    format!(
                        "failed to classify RPX_REGISTRY_BASE_URL repository {}: {details}",
                        normalized_url
                    )
                })
        }

        Err(_) => {
            let url = reqwest::Url::parse(DEFAULT_REGISTRY_BASE_URL).map_err(|error| {
                format!(
                    "invalid default registry URL {}: {error}",
                    DEFAULT_REGISTRY_BASE_URL
                )
            })?;

            Ok(PackageRepository::new(url, RepositoryType::Rrepo))
        }
    }
}

async fn lockfile_from_roots(
    repositories: Vec<PackageRepository>,
    roots: BTreeMap<String, String>,
    preferred_versions: BTreeMap<String, PackageVersion>,
    existing_lockfile: Option<&Lockfile>,
    r_version: Option<&str>,
) -> RpxResult<Lockfile> {
    let root_dependencies = root_dependency_ranges(&repositories, &roots)
        .map_err(|details| LockError::ResolveFailed { details })?;
    let selected = resolve_from_registry(
        http::client(),
        repositories.clone(),
        root_dependencies,
        preferred_versions,
    )
    .await
    .map_err(|details| LockError::ResolveFailed { details })?;

    let sysreq_db = load_sysreq_snapshot_for_lock(existing_lockfile);
    lockfile_from_selected_versions(
        resolution_roots_from_map(&roots),
        selected,
        &sysreq_db,
        &repositories,
        r_version,
    )
    .await
    .map_err(|details| LockError::ResolveFailed { details }.into())
}

async fn lockfile_from_selected_versions(
    roots: Vec<ResolutionRoot>,
    selected: Vec<(String, PackageVersion)>,
    sysreq_db: &sysreqs::SysreqDbSnapshot,
    repositories: &[PackageRepository],
    r_version: Option<&str>,
) -> Result<Lockfile, String> {
    let client = http::client();
    let mut packages = BTreeMap::new();
    let mut sysreq_packages = BTreeMap::new();

    for (name, version) in selected {
        let description = version
            .repository()
            .description(&client, &name, version.version())
            .await?;
        let dependencies = locked_dependencies_from_description(&description)?;

        let rules = sysreqs::match_rules(description.system_requirements.as_deref(), sysreq_db);
        if !rules.is_empty() {
            sysreq_packages.insert(name.clone(), rules);
        }

        packages.insert(
            name.clone(),
            LockedPackage {
                package: name.clone(),
                version: version.version().to_string(),
                source: Some("repository".to_string()),
                source_url: Some(version.source_url(&name)),
                dependencies,
            },
        );
    }

    let sysreq_rules = sysreq_packages
        .values()
        .flatten()
        .cloned()
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect();

    let required_base_packages = locked_base_packages_from_locked(&roots, packages.values());

    Ok(Lockfile {
        version: LOCKFILE_VERSION,
        revision: LOCKFILE_REVISION,
        repositories: locked_package_repositories(repositories),
        r: LockedR {
            version: r_version.map_or_else(|| runtime_info().version, ToString::to_string),
            base_packages: required_base_packages,
        },
        sysreqs: LockedSystemRequirements {
            db_commit: sysreq_db.commit.clone(),
            rules: sysreq_rules,
            packages: sysreq_packages,
        },
        roots: roots
            .into_iter()
            .map(|root| lockfile::LockedRoot {
                package: root.name,
                constraint: root.constraint,
            })
            .collect(),
        packages,
    })
}

fn locked_dependencies_from_description(
    description: &r_description::lossy::RDescription,
) -> Result<Vec<lockfile::LockedDependency>, String> {
    let mut dependencies = Vec::new();

    dependencies.extend(locked_dependencies_from_relations(
        "Depends",
        description.depends.as_ref(),
    )?);
    dependencies.extend(locked_dependencies_from_relations(
        "Imports",
        description.imports.as_ref(),
    )?);
    dependencies.extend(locked_dependencies_from_relations(
        "LinkingTo",
        description.linking_to.as_ref(),
    )?);

    Ok(dependencies)
}

fn locked_dependencies_from_relations(
    kind: &str,
    relations: Option<&r_description::lossy::Relations>,
) -> Result<Vec<lockfile::LockedDependency>, String> {
    relations
        .into_iter()
        .flat_map(|relations| relations.iter())
        .filter(|relation| relation.name != "R")
        .map(|relation| {
            let (min_version, max_version_exclusive) = lossy_relation_bounds(relation);
            Ok(lockfile::LockedDependency {
                package: relation.name.clone(),
                kind: kind.to_string(),
                min_version,
                max_version_exclusive,
            })
        })
        .collect()
}

fn lossy_relation_bounds(
    relation: &r_description::lossy::Relation,
) -> (Option<String>, Option<String>) {
    let Some((operator, version)) = relation.version.as_ref() else {
        return (None, None);
    };

    match operator {
        r_description::VersionConstraint::GreaterThan
        | r_description::VersionConstraint::GreaterThanEqual => (Some(version.to_string()), None),
        r_description::VersionConstraint::LessThan
        | r_description::VersionConstraint::LessThanEqual => (None, Some(version.to_string())),
        r_description::VersionConstraint::Equal => (Some(version.to_string()), None),
        r_description::VersionConstraint::NotEqual => (None, None),
    }
}

fn locked_package_repositories(repositories: &[PackageRepository]) -> Vec<LockedRepository> {
    repositories
        .iter()
        .map(|repository| {
            let (kind, cran_archive_support) = match repository.repo_type() {
                RepositoryType::Rrepo => (LockedRepositoryKind::Rrepo, None),
                RepositoryType::Cran { archives } => {
                    (LockedRepositoryKind::CranLike, Some(archives))
                }
            };

            LockedRepository {
                url: repository.base_url().to_string(),
                kind,
                cran_archive_support,
            }
        })
        .collect()
}

fn locked_base_packages_from_locked<'a>(
    roots: &[ResolutionRoot],
    packages: impl Iterator<Item = &'a LockedPackage>,
) -> Vec<String> {
    let mut base_packages = roots
        .iter()
        .filter(|root| is_base_package(&root.name))
        .map(|root| root.name.clone())
        .collect::<BTreeSet<_>>();

    base_packages.extend(
        packages
            .flat_map(|package| &package.dependencies)
            .filter(|dependency| is_base_package(&dependency.package))
            .map(|dependency| dependency.package.clone()),
    );

    base_packages.into_iter().collect()
}

fn locked_repository_type(kind: LockedRepositoryKind, archives: ArchiveSupport) -> RepositoryType {
    match kind {
        LockedRepositoryKind::Rrepo => RepositoryType::Rrepo,
        LockedRepositoryKind::CranLike => RepositoryType::Cran { archives: archives },
    }
}

fn validate_lockfile_compatibility(lockfile: &Lockfile) -> Result<(), LockfileCompatibilityError> {
    if lockfile.version < LOCKFILE_VERSION {
        return Err(LockfileCompatibilityError::Older);
    }
    if lockfile.version > LOCKFILE_VERSION {
        return Err(LockfileCompatibilityError::Newer);
    }
    Ok(())
}

fn validate_lockfile_compatibility_for_sync(lockfile: &Lockfile) -> RpxResult<()> {
    match validate_lockfile_compatibility(lockfile) {
        Ok(()) => Ok(()),
        Err(LockfileCompatibilityError::Older) => Err(SyncError::LockfileOlder.into()),
        Err(LockfileCompatibilityError::Newer) => Err(SyncError::LockfileNewer.into()),
    }
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

fn print_runtime_version_warning(runtime_status: &RuntimeStatus) {
    let Some(version_mismatch) = &runtime_status.version_mismatch else {
        return;
    };

    blank_status_line();
    status("R runtime differs from lockfile:");
    status(format_args!("- {version_mismatch}"));
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
) -> RpxResult<bool> {
    let explicit_install = install_system || install_only_system;
    let interactive = std::io::stdin().is_terminal() && std::io::stderr().is_terminal();
    let mut plan = plan.clone();

    if !plan.unsupported_rules.is_empty() {
        warning(RpxWarning::UnsupportedSystemRequirementRules {
            host: plan.host.label(),
            rules: plan.unsupported_rules.join(", "),
        });
    }

    if plan.needs_metadata_refresh && explicit_install {
        if interactive {
            prompt_for_metadata_refresh(&plan);
        }

        note("Refreshing system package information...");
        refresh_system_metadata(&plan)
            .map_err(|details| SyncError::MetadataRefreshFailed { details })?;
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
            status("System dependencies are already installed");
        }
        return Ok(!install_only_system);
    }

    if plan.installed_query_error.is_none() {
        print_system_package_summary(
            &format!("Missing system packages for {}:", plan.host.label()),
            &plan.missing_packages,
        );
    }
    let preview = sysreq_preview_commands(&plan);
    if !preview.is_empty() {
        note("rpx will run:");
        for command in &preview {
            note(format_args!("- {command}"));
        }
    }

    if explicit_install && interactive && !prompt_for_install_confirmation() {
        status("Canceled");
        std::process::exit(1);
    }

    if explicit_install {
        let ui = SystemDepsUi::start();
        if let Err(error) = install_system_dependencies(&plan) {
            ui.fail();
            return Err(SyncError::SystemDependenciesFailed { details: error }.into());
        }
        ui.finish();
        if install_only_system {
            status("System dependency sync complete.");
            return Ok(false);
        }
        return Ok(true);
    }

    if !interactive {
        warning(RpxWarning::ContinuingWithoutSystemDependencies);
        return Ok(!install_only_system);
    }

    match prompt_for_system_dependency_action() {
        SyncSystemChoice::InstallAndContinue => {
            let ui = SystemDepsUi::start();
            if let Err(error) = install_system_dependencies(&plan) {
                ui.fail();
                return Err(SyncError::SystemDependenciesFailed { details: error }.into());
            }
            ui.finish();
            Ok(true)
        }
        SyncSystemChoice::TryROnly => Ok(true),
        SyncSystemChoice::Cancel => {
            status("Canceled");
            std::process::exit(1);
        }
    }
}

fn prompt_for_install_confirmation() -> bool {
    note("Proceed with system package installation? [y/N]");
    prompt("> ");

    let mut input = String::new();
    if std::io::stdin().read_line(&mut input).is_err() {
        return false;
    }

    matches!(input.trim(), "y" | "Y" | "yes" | "YES" | "Yes")
}

fn print_system_package_summary(title: &str, packages: &[String]) {
    note(title);
    let shown = packages.iter().take(8).collect::<Vec<_>>();
    for package in shown {
        note(format_args!("- {package}"));
    }
    if packages.len() > 8 {
        note(format_args!("- ... and {} more", packages.len() - 8));
    }
}

fn prompt_for_metadata_refresh(plan: &SystemDependencyPlan) {
    note("rpx could not verify which system packages are missing yet.");
    blank_note_line();
    note("rpx can run:");
    if let Some(command) = system_metadata_refresh_preview(plan) {
        note(format_args!("- {command}"));
    }
    note("to refresh apt package information and check what is missing.");
    blank_note_line();
    note("Run package metadata refresh now? [y/N]");
    prompt("> ");

    let mut input = String::new();
    if std::io::stdin().read_line(&mut input).is_err() {
        status("Canceled");
        std::process::exit(1);
    }

    if !matches!(input.trim(), "y" | "Y" | "yes" | "YES" | "Yes") {
        status("Canceled");
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
    note("Choose an action:");
    note("1. Install system deps and continue");
    note("2. Try to install R packages only");
    note("3. Cancel");
    prompt("> ");

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

fn validate_runtime_for_sync(lockfile: &Lockfile) -> RpxResult<()> {
    let status = runtime_status(lockfile);

    if let Some(version_mismatch) = status.version_mismatch {
        warning(RpxWarning::RuntimeVersionMismatch {
            details: version_mismatch,
        });
    }

    if status.missing_base_packages.is_empty() {
        return Ok(());
    }

    Err(SyncError::RuntimeMissingBasePackages {
        packages: status.missing_base_packages.join(", "),
    }
    .into())
}

fn r_minor_version(version: &str) -> Option<String> {
    let mut parts = version.split('.');
    Some(format!("{}.{}", parts.next()?, parts.next()?))
}

async fn install_locked_packages(
    packages: Vec<LockedPackage>,
    repositories: Vec<LockedRepository>,
) -> Result<(), String> {
    let total_packages = packages.len() as u64;
    let sync_span = tracing::info_span!(
        "sync_packages",
        total = total_packages,
        completed = 0_u64,
        running = 0_u64,
        pending = total_packages,
        stage = tracing::field::Empty,
        indicatif.pb_show = true,
    );
    sync_span.pb_set_style(&progress_spinner_style());
    sync_span.pb_set_message(&format!("sync packages 0/{total_packages}"));
    sync_span.pb_set_length(total_packages);
    sync_span.pb_start();

    let locked_names = packages
        .iter()
        .map(|p| p.package.clone())
        .collect::<BTreeSet<_>>();
    let mut installed_packages = installed_packages_async()
        .await
        .into_iter()
        .map(|p| p.package)
        .collect::<BTreeSet<_>>();

    let mut pending_packages = packages
        .into_iter()
        .filter(|p| !installed_packages.contains(&p.package))
        .collect::<Vec<_>>();
    sync_span.record("pending", pending_packages.len() as u64);

    let mut running = tokio::task::JoinSet::new();
    let mut completed = total_packages.saturating_sub(pending_packages.len() as u64);
    sync_span.record("completed", completed);
    sync_span.pb_set_position(completed);
    sync_span.pb_set_message(&format!("sync packages {completed}/{total_packages}"));

    loop {
        while running.len() < 8 {
            let Some(new) = pending_packages.iter().position(|p| {
                p.dependencies
                    .iter()
                    .filter(|dep| locked_names.contains(&dep.package))
                    .all(|dep| installed_packages.contains(&dep.package))
            }) else {
                break;
            };

            let package = pending_packages.remove(new);

            let source_url = package
                .source_url
                .as_deref()
                .ok_or_else(|| format!("{} is missing source_url", package.package))?;

            let repository = repositories
                .iter()
                .find(|repo| source_url.starts_with(&repo.url))
                .cloned()
                .ok_or_else(|| {
                    format!(
                        "package {} source URL does not match any locked repository: {}",
                        package.package, source_url
                    )
                })?;

            running
                .spawn(install_locked_package(package, repository).instrument(sync_span.clone()));
            sync_span.record("running", running.len() as u64);
            sync_span.record("pending", pending_packages.len() as u64);
        }

        if pending_packages.is_empty() && running.is_empty() {
            sync_span.record("stage", "done");
            sync_span.pb_set_finish_message(&format!("sync packages {completed}/{total_packages}"));
            return Ok(());
        }

        if running.is_empty() {
            let blocked = pending_packages
                .iter()
                .map(|p| p.package.clone())
                .collect::<Vec<_>>()
                .join(", ");

            return Err(format!(
                "no installable packages remain; blocked packages: {blocked}"
            ));
        }

        let result = running
            .join_next()
            .await
            .expect("running install task should exist")
            .map_err(|error| format!("install task failed to join: {error}"))?;

        installed_packages.insert(result?);
        completed += 1;
        sync_span.record("completed", completed);
        sync_span.record("running", running.len() as u64);
        sync_span.record("pending", pending_packages.len() as u64);
        sync_span.pb_set_position(completed);
        sync_span.pb_set_message(&format!("sync packages {completed}/{total_packages}"));
    }
}

async fn install_locked_package(
    package: LockedPackage,
    repository: LockedRepository,
) -> Result<String, String> {
    let span = tracing::info_span!(
        "install_package",
        package = %package.package,
        version = %package.version,
        repository = %repository.url,
        stage = tracing::field::Empty,
        artifact_kind = tracing::field::Empty,
        bytes = tracing::field::Empty,
        total_bytes = tracing::field::Empty,
        indicatif.pb_show = true,
    );
    span.pb_set_style(&progress_spinner_style());
    span.pb_set_message(&package_stage_message(
        &package.package,
        &package.version,
        "queued",
    ));
    span.pb_start();

    install_locked_package_inner(package, repository, span.clone())
        .instrument(span)
        .await
}

async fn install_locked_package_inner(
    package: LockedPackage,
    repository: LockedRepository,
    span: tracing::Span,
) -> Result<String, String> {
    let project_library = project_library_path();
    record_package_stage(&span, &package, "checking R version");
    let r_version = r_version_async().await?;
    let r_minor = r_minor_version(&r_version)
        .ok_or_else(|| format!("failed to parse R minor version from {r_version}"))?;
    let key = CompiledPackageCacheKey::new(&package.package, &package.version, &r_version);
    record_package_stage(&span, &package, "checking cache");
    if cache::exists(&key).await {
        record_package_stage(&span, &package, "restoring from cache");
        cache::restore(&key, &project_library).await?;
        record_package_stage(&span, &package, "restored from cache");
        return Ok(package.package);
    }

    let base_url = reqwest::Url::parse(&repository.url)
        .map_err(|error| format!("invalid repository URL {}: {error}", repository.url))?;
    let client = http::traced_client();

    record_package_stage(&span, &package, "downloading binary");
    let binary = match (std::env::consts::OS, repository.kind) {
        ("windows", LockedRepositoryKind::Rrepo) => http::rrepo_windows_binary(
            &client,
            &base_url,
            &package.package,
            &package.version,
            &r_minor,
        )
        .await
        .map(|response| (response, "zip", "win.binary".to_string()))
        .map_err(|error| error.to_string()),
        ("windows", LockedRepositoryKind::CranLike) => http::cran_windows_binary(
            &client,
            &base_url,
            &r_minor,
            &package.package,
            &package.version,
        )
        .await
        .map(|response| (response, "zip", "win.binary".to_string()))
        .map_err(|error| error.to_string()),
        ("macos", LockedRepositoryKind::Rrepo) => {
            let target = macos_binary_target()?;
            http::rrepo_macos_binary(
                &client,
                &base_url,
                &package.package,
                &package.version,
                &target,
                &r_minor,
            )
            .await
            .map(|response| (response, "tgz", format!("mac.binary.{target}")))
            .map_err(|error| error.to_string())
        }
        ("macos", LockedRepositoryKind::CranLike) => {
            let target = macos_binary_target()?;
            http::cran_macos_binary(
                &client,
                &base_url,
                &target,
                &r_minor,
                &package.package,
                &package.version,
            )
            .await
            .map(|response| (response, "tgz", format!("mac.binary.{target}")))
            .map_err(|error| error.to_string())
        }
        _ => Err(format!(
            "binary artifacts are not supported on {}-{}",
            std::env::consts::OS,
            std::env::consts::ARCH
        )),
    };

    let (response, extension, install_type) = match binary {
        Ok(binary) => {
            span.record("artifact_kind", binary.2.as_str());
            binary
        }
        Err(error) => {
            tracing::debug!(package = %package.package, version = %package.version, error = %error, "binary artifact unavailable; falling back to source");
            record_package_stage(&span, &package, "falling back to source");
            record_package_stage(&span, &package, "downloading source");
            let response = match repository.kind {
                LockedRepositoryKind::Rrepo => {
                    http::rrepo_source_artifact(
                        &client,
                        &base_url,
                        &package.package,
                        &package.version,
                    )
                    .await
                }
                LockedRepositoryKind::CranLike => {
                    let source_url = package
                        .source_url
                        .as_deref()
                        .ok_or_else(|| format!("{} is missing source_url", package.package))?;

                    if source_url.contains("/src/contrib/Archive/") {
                        http::cran_archive_source_tarball(
                            &client,
                            &base_url,
                            &package.package,
                            &package.version,
                        )
                        .await
                    } else {
                        http::cran_current_source_tarball(
                            &client,
                            &base_url,
                            &package.package,
                            &package.version,
                        )
                        .await
                    }
                }
            }
            .map_err(|error| error.to_string())?;

            span.record("artifact_kind", "source");
            (response, "tar.gz", "source".to_string())
        }
    };

    let artifact_path = write_artifact_response(&package, extension, response, &span).await?;
    record_package_stage(&span, &package, "installing");
    let temp_library = build_temp_library_path(&package.package, &unique_build_token());
    let install_package = package.package.clone();
    let install_version = package.version.clone();
    let install_type_for_task = install_type.clone();
    let artifact_path_for_task = artifact_path.clone();
    let temp_library_for_task = temp_library.clone();

    tokio::task::spawn_blocking(move || {
        install_local_package(
            &artifact_path_for_task,
            &install_package,
            &install_version,
            &install_type_for_task,
            &temp_library_for_task,
        )
    })
    .await
    .map_err(|error| format!("failed to join install task: {error}"))?
    .map_err(|failure| install_failure_message(&package.package, &package.version, &failure))?;

    let built_package_path = temp_library.join(&package.package);
    record_package_stage(&span, &package, "storing cache");
    cache::store(&key, &built_package_path).await?;
    record_package_stage(&span, &package, "restoring project library");
    cache::restore(&key, &project_library).await?;

    record_package_stage(&span, &package, "cleaning up");
    if let Some(temp_root) = temp_library.parent() {
        tokio::fs::remove_dir_all(temp_root)
            .await
            .map_err(|error| format!("failed to clean temporary build directory: {error}"))?;
    }

    record_package_stage(&span, &package, "done");
    Ok(package.package)
}

fn record_package_stage(span: &tracing::Span, package: &LockedPackage, stage: &'static str) {
    span.record("stage", stage);
    span.pb_set_style(&progress_spinner_style());
    span.pb_set_message(&package_stage_message(
        &package.package,
        &package.version,
        stage,
    ));
    span.pb_tick();
}

fn package_stage_message(package: &str, version: &str, stage: &str) -> String {
    format!("{package} {version} {stage}")
}

async fn write_artifact_response(
    package: &LockedPackage,
    extension: &str,
    response: http::ArtifactResponse,
    span: &tracing::Span,
) -> Result<PathBuf, String> {
    let file_name = format!("{}_{}.{}", package.package, package.version, extension);
    let path = artifact_cache_path(&package.package, &package.version, &file_name);

    if path.exists() {
        if let Ok(metadata) = path.metadata() {
            span.record("bytes", metadata.len());
            span.record("total_bytes", metadata.len());
            span.pb_set_style(&progress_bar_style());
            span.pb_set_length(metadata.len());
            span.pb_set_position(metadata.len());
            span.pb_set_message(&package_stage_message(
                &package.package,
                &package.version,
                "using cached artifact",
            ));
        }
        return Ok(path);
    }

    let content_length = response.content_length;
    if let Some(total) = content_length {
        span.record("total_bytes", total);
        span.pb_set_style(&progress_bar_style());
        span.pb_set_length(total);
        span.pb_set_position(0);
    }

    let mut file = tokio::fs::File::create(&path)
        .await
        .map_err(|error| format!("failed to create artifact file {}: {error}", path.display()))?;
    let mut stream = response.stream;
    let mut written = 0_u64;

    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(|error| format!("failed to read artifact response: {error}"))?;
        let chunk_len = chunk.len() as u64;
        file.write_all(&chunk).await.map_err(|error| {
            format!("failed to write artifact file {}: {error}", path.display())
        })?;
        written += chunk_len;
        span.record("bytes", written);
        if content_length.is_some() {
            span.pb_inc(chunk_len);
        } else {
            span.pb_tick();
        }
    }

    Ok(path)
}

fn macos_binary_target() -> Result<String, String> {
    match std::env::consts::ARCH {
        "aarch64" => Ok("big-sur-arm64".to_string()),
        "x86_64" => Ok("big-sur-x86_64".to_string()),
        arch => Err(format!(
            "unsupported macOS architecture for binary packages: {arch}"
        )),
    }
}

fn install_failure_message(package: &str, version: &str, failure: &InstallFailure) -> String {
    format!(
        "failed to install {package}@{version}: {} (log: {})",
        failure.summary,
        failure.log_path.display()
    )
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

#[cfg(test)]
mod tests {
    use super::{
        LockfileCompatibilityError, locked_install_order, persisted_constraints,
        semver_add_constraint, validate_lockfile_compatibility,
    };
    use crate::description::{RDescription, resolution_root_from_relation};
    use crate::{
        lockfile::{
            LOCKFILE_REVISION, LOCKFILE_VERSION, LockedDependency, LockedPackage, LockedR,
            LockedSystemRequirements, Lockfile,
        },
        registry::ResolutionRoot,
    };
    use std::collections::BTreeMap;
    use std::str::FromStr;

    #[test]
    fn builds_resolution_roots_from_description_constraints() {
        let description = RDescription::from_str(
            "Package: testpkg\nVersion: 0.1.0\nTitle: Test Package\nDescription: Test package for unit tests.\nLicense: MIT\nImports: cli (>= 3.6.0), digest\nDepends: R (>= 4.2), jsonlite (== 1.8.9)\n",
        )
        .expect("description should parse");

        assert_eq!(
            description
                .imports
                .iter()
                .chain(
                    description
                        .depends
                        .iter()
                        .filter(|relation| relation.name != "R")
                )
                .map(resolution_root_from_relation)
                .collect::<Vec<_>>(),
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
                    constraint: "== 1.8.9".to_string(),
                },
            ]
        );
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
    fn accepts_newer_revision_but_rejects_newer_lockfile_version() {
        let mut lockfile = Lockfile {
            version: LOCKFILE_VERSION,
            revision: LOCKFILE_REVISION + 1,
            repositories: vec![],
            r: LockedR::default(),
            sysreqs: LockedSystemRequirements::default(),
            roots: vec![],
            packages: BTreeMap::new(),
        };

        assert_eq!(validate_lockfile_compatibility(&lockfile), Ok(()));

        lockfile.version = LOCKFILE_VERSION + 1;

        assert_eq!(
            validate_lockfile_compatibility(&lockfile),
            Err(LockfileCompatibilityError::Newer)
        );
    }

    #[test]
    fn installs_locked_packages_in_dependency_order() {
        let lockfile = Lockfile {
            version: LOCKFILE_VERSION,
            revision: LOCKFILE_REVISION,
            repositories: vec![],
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
            revision: LOCKFILE_REVISION,
            repositories: vec![],
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
}
