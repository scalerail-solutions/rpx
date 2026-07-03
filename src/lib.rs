use clap::Parser;
use futures_util::StreamExt;
use miette::Diagnostic;
use std::{
    collections::{BTreeMap, BTreeSet},
    env, fs,
    io::IsTerminal,
    path::{Path, PathBuf},
    sync::OnceLock,
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
mod r_version;
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
    LOCKFILE_REVISION, LOCKFILE_VERSION, LockedCranArchiveSupport, LockedR, LockedRepository,
    LockedRepositoryKind, LockedSystemRequirements, Lockfile, read_lockfile,
    read_lockfile_optional, write_lockfile,
};
use output::{blank_note_line, blank_status_line, note, prompt, status, warning};
use project::{
    artifact_cache_path, build_temp_library_path, cache_dir_path, project_library_path,
    project_library_root_path,
};
use r::{InstallFailure, base_packages, install_local_package, installed_packages, runtime_info};
use registry::{DEFAULT_REGISTRY_BASE_URL, ResolutionRoot};
use repository::{
    CranArchiveSupport, RepositoryKind, RepositorySet, RepositorySource, normalize_repository_url,
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
use ui::SystemDepsUi;

use crate::{
    cache::CompiledPackageCacheKey,
    lockfile::LockedPackage,
    r::{RVirtualEnv, installed_packages_async, r_version_async, remove_packages_from_venv},
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

    #[error("archive listing unavailable for CRAN-like repository {url}")]
    #[diagnostic(
        severity(Warning),
        code(rpx::repository::cran_archive_unavailable),
        help(
            "rpx can restore locked archived package URLs, but new resolution for this repository is limited to versions listed in PACKAGES."
        )
    )]
    CranArchiveUnavailable { url: String },
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
pub fn run() -> miette::Result<()> {
    run_inner()?;
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

fn run_inner() -> RpxResult<()> {
    let cli = Cli::parse();

    match cli.command {
        Commands::Init => cmd_init(),
        Commands::Add {
            default_repo,
            no_default_repo,
            packages,
        } => cmd_add(
            &packages,
            DefaultRepositoryPreference::from_flags(default_repo, no_default_repo),
        ),
        Commands::Remove {
            default_repo,
            no_default_repo,
            packages,
        } => cmd_remove(
            &packages,
            DefaultRepositoryPreference::from_flags(default_repo, no_default_repo),
        ),
        Commands::Run { command } => REPOSITORY_CLASSIFIER_RUNTIME
            .get_or_init(|| {
                tokio::runtime::Runtime::new().expect("repository classifier runtime should start")
            })
            .block_on(cmd_run(&command)),
        Commands::Lock {
            default_repo,
            no_default_repo,
        } => cmd_lock(DefaultRepositoryPreference::from_flags(
            default_repo,
            no_default_repo,
        )),
        Commands::Status => cmd_status(),
        Commands::Sync {
            install_system,
            install_only_system,
        } => cmd_sync(install_system, install_only_system),
        Commands::Clean => cmd_clean(),
        Commands::Repo { command } => cmd_repo(command),
    }
}

fn cmd_init() -> RpxResult<()> {
    let path = init_description()?;
    status(format_args!("Initialized project at {path}"));
    status("Next: run `rpx add <package>` or `rpx lock`");
    Ok(())
}

fn cmd_add(
    packages: &[String],
    repository_preference: DefaultRepositoryPreference,
) -> RpxResult<()> {
    let mut description = read_description()?;
    let mut lockfile = read_project_lockfile_optional()?;
    let repositories = repository_preference.repositories(&description, lockfile.as_ref())?;
    let mut new_packages = Vec::new();

    for package in packages {
        if description
            .imports
            .iter()
            .chain(&description.depends)
            .any(|dependency| dependency.name == *package)
        {
            continue;
        }

        new_packages.push(package.clone());
    }

    if !new_packages.is_empty() {
        let sysreq_db = load_sysreq_snapshot_for_lock(lockfile.as_ref());
        let requested_packages = new_packages
            .iter()
            .cloned()
            .map(|package| (package, "*".to_string()))
            .collect::<BTreeMap<_, _>>();
        // Newly added packages should resolve from the latest compatible version so
        // DESCRIPTION reflects the addition, while unrelated locked packages stay stable.
        let preferred_versions = match &lockfile {
            Some(lockfile) => lockfile
                .packages
                .iter()
                .filter(|(name, _)| !requested_packages.contains_key(name.as_str()))
                .map(|(name, package)| (name.clone(), package.version.clone()))
                .collect(),
            None => BTreeMap::new(),
        };
        let roots = add_resolution_roots(&description, &requested_packages);
        let resolved =
            resolve_from_registry(&repositories, &roots, &preferred_versions).map_err(|error| {
                LockError::ResolveFailed {
                    details: format!(
                        "could not resolve a compatible dependency set for {}: {error}",
                        new_packages.join(", ")
                    ),
                }
            })?;
        warn_cran_archive_unavailable(&repositories);
        let constraints = constraints_from_resolved_roots(&new_packages, &resolved)
            .map_err(|details| LockError::ResolveFailed { details })?;

        for package in &new_packages {
            let package_constraints = constraints
                .get(package)
                .expect("resolved addition should include constraints for each new package");
            if package_constraints.is_empty()
                || package_constraints
                    .iter()
                    .all(|constraint| constraint.trim() == "*")
            {
                description.imports.insert(DescriptionDependency {
                    name: package.clone(),
                    version: None,
                });
            } else {
                description.imports.extend(
                    package_constraints
                        .iter()
                        .map(|constraint| relation_with_constraint(package, constraint))
                        .collect::<Result<Vec<_>, _>>()
                        .expect("constraints should parse"),
                );
            }
        }

        lockfile = Some(lockfile_from_resolution(
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
                .collect::<BTreeSet<_>>()
                .into_iter()
                .collect(),
            &resolved,
            &sysreq_db,
            repositories.sources(),
            None,
        ));
    }

    write_description(&description)?;
    if let Some(lockfile) = lockfile {
        write_project_lockfile(&lockfile)?;
    } else {
        let _ = lock_from_description(repository_preference)?;
    }
    let _ = sync_from_lockfile(false, false)?;
    status(format_args!("Added {}", packages.join(", ")));
    Ok(())
}

fn cmd_repo(command: RepoCommands) -> RpxResult<()> {
    match command {
        RepoCommands::Add { url } => cmd_repo_add(&url),
        RepoCommands::Remove {
            url,
            remove_credential,
        } => cmd_repo_remove(&url, remove_credential),
        RepoCommands::List => cmd_repo_list(),
    }
}

fn cmd_repo_add(url: &str) -> RpxResult<()> {
    let mut description = read_description()?;
    let repository_type = classify_repository_type(url).map_err(|details| RepoError::Add {
        url: normalize_repository_url(url),
        details,
    })?;
    let source = repository_source_from_type(url, repository_type);

    let mut additional_repositories = description.additional_repositories.clone();
    if additional_repositories
        .iter()
        .any(|existing| normalize_repository_url(existing) == source.base_url())
    {
        status(format_args!(
            "Repository already configured: {}",
            source.base_url()
        ));
        return Ok(());
    }

    additional_repositories.push(source.base_url().to_string());
    description.additional_repositories = additional_repositories;
    write_description(&description)?;
    status(format_args!("Added repository {}", source.base_url()));
    Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RepositoryType {
    Rrepo,
    CranLike {
        archive_support: Option<CranArchiveSupport>,
    },
}

fn classify_repository_type(url: &str) -> Result<RepositoryType, String> {
    REPOSITORY_CLASSIFIER_RUNTIME
        .get_or_init(|| {
            tokio::runtime::Runtime::new().expect("repository classifier runtime should start")
        })
        .block_on(classify_repository_type_async(url))
}

async fn classify_repository_type_async(url: &str) -> Result<RepositoryType, String> {
    let normalized_url = normalize_repository_url(url);
    let base_url = reqwest::Url::parse(&normalized_url)
        .map_err(|error| format!("invalid repository URL {normalized_url}: {error}"))?;
    let client = http::client();

    match http::rrepo_repository_packages(&client, &base_url).await {
        Ok(_) => Ok(RepositoryType::Rrepo),
        Err(rrepo_error) => match http::cran_packages(&client, &base_url).await {
            Ok(_) => Ok(RepositoryType::CranLike {
                archive_support: classify_cran_like_archive_support(&client, &base_url).await?,
            }),
            Err(cran_error) => Err(format!(
                "not an rrepo API ({rrepo_error}) or CRAN-like repository ({cran_error})"
            )),
        },
    }
}

async fn classify_cran_like_archive_support(
    client: &http::HttpClient,
    base_url: &reqwest::Url,
) -> Result<Option<CranArchiveSupport>, String> {
    match http::cran_archive_root(client, base_url).await {
        Ok(_) => Ok(Some(CranArchiveSupport::Available)),
        Err(http::HttpError::UnexpectedStatus { status, .. })
            if status == reqwest::StatusCode::NOT_FOUND
                || status == reqwest::StatusCode::FORBIDDEN =>
        {
            Ok(Some(CranArchiveSupport::Unavailable))
        }
        Err(http::HttpError::RequestFailed { .. }) => Ok(None),
        Err(error) => Err(error.to_string()),
    }
}

fn repository_source_from_type(url: &str, repository_type: RepositoryType) -> RepositorySource {
    match repository_type {
        RepositoryType::Rrepo => RepositorySource::new(url),
        RepositoryType::CranLike { archive_support } => match archive_support {
            Some(archive_support) => {
                RepositorySource::cran_like_with_archive_support(url, archive_support)
            }
            None => RepositorySource::cran_like(url),
        },
    }
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
        LockedRepositoryKind::CranLike => match repository.cran_archive_support {
            Some(support) => RepositorySource::cran_like_with_archive_support(
                &repository.url,
                cran_archive_support_from_locked(support),
            ),
            None => RepositorySource::cran_like(&repository.url),
        },
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

fn cmd_remove(
    packages: &[String],
    repository_preference: DefaultRepositoryPreference,
) -> RpxResult<()> {
    let mut description = read_description()?;
    for package in packages {
        description
            .imports
            .retain(|dependency| dependency.name != *package);
        description
            .depends
            .retain(|dependency| dependency.name != *package);
    }
    write_description(&description)?;

    let _ = lock_from_description(repository_preference)?;
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

fn cmd_lock(repository_preference: DefaultRepositoryPreference) -> RpxResult<()> {
    let outcome = lock_from_description(repository_preference)?;
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

    fn repositories(
        self,
        description: &description::RDescription,
        lockfile: Option<&Lockfile>,
    ) -> Result<RepositorySet, LockError> {
        let default_url = default_registry_base_url();
        let use_default_repository = match self {
            Self::Enabled => true,
            Self::Disabled => false,
            Self::FromLockfileOrDefault => lockfile.is_none_or(|lockfile| {
                let default_url = normalize_repository_url(&default_url);
                lockfile
                    .repositories
                    .iter()
                    .any(|repository| repository.url == default_url)
            }),
        };

        let mut sources = Vec::new();
        if use_default_repository {
            sources.push(RepositorySource::new(default_url));
        }
        for url in &description.additional_repositories {
            if let Some(source) =
                lockfile.and_then(|lockfile| repository_source_from_lockfile(lockfile, url))
            {
                sources.push(source);
                continue;
            }

            let repository_type =
                classify_repository_type(url).map_err(|details| LockError::ResolveFailed {
                    details: format!(
                        "failed to classify configured repository {}: {details}",
                        normalize_repository_url(url)
                    ),
                })?;
            sources.push(repository_source_from_type(url, repository_type));
        }
        Ok(RepositorySet::new(sources))
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

fn add_resolution_roots(
    description: &description::RDescription,
    new_packages: &BTreeMap<String, String>,
) -> Vec<ResolutionRoot> {
    let mut roots = BTreeSet::new();

    for root in description
        .imports
        .iter()
        .chain(
            description
                .depends
                .iter()
                .filter(|relation| relation.name != "R"),
        )
        .map(resolution_root_from_relation)
    {
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

fn lock_from_description(
    repository_preference: DefaultRepositoryPreference,
) -> RpxResult<LockOutcome> {
    let description = read_description()?;
    let roots = description
        .imports
        .iter()
        .chain(
            description
                .depends
                .iter()
                .filter(|relation| relation.name != "R"),
        )
        .map(resolution_root_from_relation)
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    let existing_lockfile = read_project_lockfile_optional()?;
    let repositories =
        repository_preference.repositories(&description, existing_lockfile.as_ref())?;
    if let Some(lockfile) = &existing_lockfile
        && lockfile.version > LOCKFILE_VERSION
    {
        return Err(LockError::LockfileNewer.into());
    }
    let sysreq_db = load_sysreq_snapshot_for_lock(existing_lockfile.as_ref());

    let preferred_versions = match &existing_lockfile {
        Some(lockfile) => lockfile
            .packages
            .iter()
            .map(|(name, package)| (name.clone(), package.version.clone()))
            .collect(),
        None => BTreeMap::new(),
    };
    let resolved =
        resolve_from_registry(&repositories, &roots, &preferred_versions).map_err(|details| {
            LockError::ResolveFailed {
                details: details.to_string(),
            }
        })?;
    warn_cran_archive_unavailable(&repositories);

    let lockfile =
        lockfile_from_resolution(roots, &resolved, &sysreq_db, repositories.sources(), None);
    let changed = existing_lockfile.as_ref() != Some(&lockfile);
    write_project_lockfile(&lockfile)?;
    Ok(LockOutcome { changed })
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

fn warn_cran_archive_unavailable(repositories: &RepositorySet) {
    for url in repositories.cran_archive_unavailable_repositories() {
        warning(RpxWarning::CranArchiveUnavailable { url });
    }
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

fn default_registry_base_url() -> String {
    env::var("RPX_REGISTRY_BASE_URL")
        .unwrap_or_else(|_| DEFAULT_REGISTRY_BASE_URL.to_string())
        .trim_end_matches('/')
        .to_string()
}

fn lockfile_from_resolution(
    roots: Vec<ResolutionRoot>,
    resolved: &[ResolvedPackage],
    sysreq_db: &sysreqs::SysreqDbSnapshot,
    repositories: &[RepositorySource],
    r_version: Option<&str>,
) -> Lockfile {
    let required_base_packages = locked_base_packages(&roots, resolved);
    let sysreqs = locked_system_requirements(resolved, sysreq_db);
    Lockfile {
        version: LOCKFILE_VERSION,
        revision: LOCKFILE_REVISION,
        repositories: locked_repositories(repositories),
        r: LockedR {
            version: r_version.map_or_else(|| runtime_info().version, ToString::to_string),
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

fn locked_repositories(repositories: &[RepositorySource]) -> Vec<LockedRepository> {
    repositories
        .iter()
        .map(|source| LockedRepository {
            url: source.base_url().to_string(),
            kind: locked_repository_kind(source.kind()),
            cran_archive_support: source
                .cran_archive_support()
                .map(locked_cran_archive_support),
        })
        .collect()
}

fn locked_repository_kind(kind: RepositoryKind) -> LockedRepositoryKind {
    match kind {
        RepositoryKind::Rrepo => LockedRepositoryKind::Rrepo,
        RepositoryKind::CranLike => LockedRepositoryKind::CranLike,
    }
}

fn locked_cran_archive_support(support: CranArchiveSupport) -> LockedCranArchiveSupport {
    match support {
        CranArchiveSupport::Available => LockedCranArchiveSupport::Available,
        CranArchiveSupport::Unavailable => LockedCranArchiveSupport::Unavailable,
    }
}

fn cran_archive_support_from_locked(support: LockedCranArchiveSupport) -> CranArchiveSupport {
    match support {
        LockedCranArchiveSupport::Available => CranArchiveSupport::Available,
        LockedCranArchiveSupport::Unavailable => CranArchiveSupport::Unavailable,
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
        DefaultRepositoryPreference, LockfileCompatibilityError, RepositoryType,
        add_resolution_roots, classify_repository_type, constraints_from_resolved_roots,
        default_registry_base_url, locked_install_order, lockfile_from_resolution,
        persisted_constraints, semver_add_constraint, validate_lockfile_compatibility,
    };
    use crate::description::{RDescription, resolution_root_from_relation};
    use crate::{
        lockfile::{
            LOCKFILE_REVISION, LOCKFILE_VERSION, LockedDependency, LockedPackage, LockedR,
            LockedRepository, LockedRepositoryKind, LockedSystemRequirements, Lockfile,
        },
        registry::ResolutionRoot,
        repository::{CranArchiveSupport, RepositoryKind, RepositorySource},
        resolver::{ResolvedDependency, ResolvedPackage},
        sysreqs::SysreqDbSnapshot,
    };
    use mockito::Server;
    use std::collections::BTreeMap;
    use std::{
        env,
        str::FromStr,
        sync::{Mutex, OnceLock},
    };

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
            &[RepositorySource::new("https://api.rrepo.org")],
            Some("4.5.2"),
        );

        assert_eq!(lockfile.version, LOCKFILE_VERSION);
        assert_eq!(lockfile.repositories[0].url, "https://api.rrepo.org");
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
    fn resolves_default_repository_preference_from_flags_then_lockfile() {
        let description = RDescription::from_str("Package: test\nVersion: 0.1.0\n")
            .expect("description should parse");
        let lockfile = Lockfile {
            version: LOCKFILE_VERSION,
            revision: LOCKFILE_REVISION,
            repositories: vec![],
            r: LockedR::default(),
            sysreqs: LockedSystemRequirements::default(),
            roots: vec![],
            packages: BTreeMap::new(),
        };
        let lockfile_with_default = Lockfile {
            version: LOCKFILE_VERSION,
            revision: LOCKFILE_REVISION,
            repositories: vec![LockedRepository {
                url: default_registry_base_url(),
                kind: LockedRepositoryKind::Rrepo,
                cran_archive_support: None,
            }],
            r: LockedR::default(),
            sysreqs: LockedSystemRequirements::default(),
            roots: vec![],
            packages: BTreeMap::new(),
        };

        assert_eq!(
            DefaultRepositoryPreference::from_flags(false, false)
                .repositories(&description, None)
                .expect("repositories should build")
                .sources()
                .len(),
            1
        );
        assert!(
            DefaultRepositoryPreference::from_flags(false, false)
                .repositories(&description, Some(&lockfile))
                .expect("repositories should build")
                .sources()
                .is_empty()
        );
        assert_eq!(
            DefaultRepositoryPreference::from_flags(true, false)
                .repositories(&description, Some(&lockfile))
                .expect("repositories should build")
                .sources()
                .len(),
            1
        );
        assert_eq!(
            DefaultRepositoryPreference::from_flags(false, false)
                .repositories(&description, Some(&lockfile_with_default))
                .expect("repositories should build")
                .sources()
                .len(),
            1
        );
        assert!(
            DefaultRepositoryPreference::from_flags(false, true)
                .repositories(&description, Some(&lockfile))
                .expect("repositories should build")
                .sources()
                .is_empty()
        );
    }

    #[test]
    fn omits_default_repository_from_project_sources_when_disabled() {
        let description = RDescription::from_str("Package: test\nVersion: 0.1.0\n")
            .expect("description should parse");

        let repositories = DefaultRepositoryPreference::Disabled
            .repositories(&description, None)
            .expect("repositories should build");
        let sources = repositories.sources();

        assert!(sources.is_empty());
    }

    #[test]
    fn classifies_rrepo_repository_sources_up_front() {
        let mut server = Server::new();
        let packages_mock = server
            .mock("GET", "/packages")
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(r#"{"repositorySlug":"test","packages":[]}"#)
            .expect(1)
            .create();

        let repository_type =
            classify_repository_type(&server.url()).expect("rrepo should classify");

        assert_eq!(repository_type, RepositoryType::Rrepo);
        packages_mock.assert();
    }

    #[test]
    fn classifies_cran_like_repository_sources_up_front() {
        let mut server = Server::new();
        let rrepo_mock = server
            .mock("GET", "/packages")
            .with_status(404)
            .expect(1)
            .create();
        let gz_mock = server
            .mock("GET", "/src/contrib/PACKAGES.gz")
            .with_status(404)
            .expect(1)
            .create();
        let packages_mock = server
            .mock("GET", "/src/contrib/PACKAGES")
            .with_status(200)
            .with_header("content-type", "text/plain")
            .with_body("Package: digest\nVersion: 0.6.38\n")
            .expect(1)
            .create();
        let archive_mock = server
            .mock("GET", "/src/contrib/Archive/")
            .with_status(404)
            .expect(1)
            .create();

        let repository_type =
            classify_repository_type(&server.url()).expect("CRAN-like should classify");

        assert_eq!(
            repository_type,
            RepositoryType::CranLike {
                archive_support: Some(CranArchiveSupport::Unavailable)
            }
        );
        rrepo_mock.assert();
        gz_mock.assert();
        packages_mock.assert();
        archive_mock.assert();
    }

    #[test]
    fn project_sources_include_classified_additional_repositories_once() {
        let mut server = Server::new();
        server
            .mock("GET", "/packages")
            .with_status(404)
            .expect(1)
            .create();
        server
            .mock("GET", "/src/contrib/PACKAGES.gz")
            .with_status(404)
            .expect(1)
            .create();
        server
            .mock("GET", "/src/contrib/PACKAGES")
            .with_status(200)
            .with_header("content-type", "text/plain")
            .with_body("Package: digest\nVersion: 0.6.38\n")
            .expect(1)
            .create();
        server
            .mock("GET", "/src/contrib/Archive/")
            .with_status(200)
            .expect(1)
            .create();
        let mut description = RDescription::from_str("Package: test\nVersion: 0.1.0\n")
            .expect("description should parse");
        description.additional_repositories = vec![server.url()];

        let repositories = DefaultRepositoryPreference::Disabled
            .repositories(&description, None)
            .expect("repositories should build");
        let sources = repositories.sources();

        assert_eq!(sources.len(), 1);
        assert_eq!(sources[0].kind(), RepositoryKind::CranLike);
        assert_eq!(
            sources[0].cran_archive_support(),
            Some(CranArchiveSupport::Available)
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
            &[],
            &empty_sysreq_db(),
            &[],
            Some("4.5.2"),
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

    fn empty_sysreq_db() -> SysreqDbSnapshot {
        SysreqDbSnapshot {
            commit: "test-commit".to_string(),
            rules: vec![],
            scripts: BTreeMap::new(),
        }
    }
}
