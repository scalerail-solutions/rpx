use clap::{Parser, Subcommand};
use directories::ProjectDirs;
use serde::{Deserialize, Serialize};
use std::{
    collections::{hash_map::DefaultHasher, BTreeMap, BTreeSet},
    env, fs,
    hash::{Hash, Hasher},
    path::{Path, PathBuf},
    process::Command,
};

const LOCKFILE_NAME: &str = "rpx.lock";

#[derive(Parser, Debug)]
#[command(name = "rpx")]
#[command(about = "A package manager CLI", long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand, Debug)]
enum Commands {
    Add {
        package: String,
    },
    Remove {
        package: String,
    },
    Run {
        #[arg(trailing_var_arg = true, allow_hyphen_values = true, required = true)]
        command: Vec<String>,
    },
    Lock,
    Status,
    Sync,
}

#[derive(Debug, Serialize, Deserialize)]
struct Lockfile {
    version: u32,
    requirements: Vec<String>,
    packages: BTreeMap<String, LockedPackage>,
}

#[derive(Debug, Serialize, Deserialize)]
struct LockedPackage {
    package: String,
    version: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    source: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    repository: Option<String>,
}

#[derive(Debug)]
struct InstalledPackage {
    package: String,
    version: String,
    repository: Option<String>,
}

fn main() {
    let cli = Cli::parse();

    match cli.command {
        Commands::Add { package } => cmd_add(&package),
        Commands::Remove { package } => cmd_remove(&package),
        Commands::Run { command } => cmd_run(&command),
        Commands::Lock => cmd_lock(),
        Commands::Status => cmd_status(),
        Commands::Sync => cmd_sync(),
    }
}

fn cmd_add(package: &str) {
    if lockfile_path().exists() {
        sync_from_lockfile();
        write_lockfile(current_lockfile_with_existing_requirements());
    }

    let status = project_command("Rscript")
        .arg("-e")
        .arg(format!("install.packages('{package}')"))
        .status()
        .expect("failed to run Rscript");

    exit_with_status(status.code());

    let mut lockfile = current_lockfile_with_existing_requirements();
    ensure_requirement(&mut lockfile.requirements, package);
    write_lockfile(lockfile);
}

fn cmd_remove(package: &str) {
    if lockfile_path().exists() {
        sync_from_lockfile();
        write_lockfile(current_lockfile_with_existing_requirements());
    }

    let status = project_command("Rscript")
        .arg("-e")
        .arg(format!("remove.packages('{package}')"))
        .status()
        .expect("failed to run Rscript");

    exit_with_status(status.code());

    let mut lockfile = current_lockfile_with_existing_requirements();
    remove_requirement(&mut lockfile.requirements, package);
    write_lockfile(lockfile);
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
    write_lockfile(current_lockfile_with_existing_requirements());
}

fn cmd_sync() {
    sync_from_lockfile();
}

fn cmd_status() {
    let lockfile = match read_lockfile() {
        Ok(lockfile) => lockfile,
        Err(error) => {
            eprintln!("Status: drift");
            eprintln!("Lockfile: {error}");
            std::process::exit(1);
        }
    };

    let installed = installed_packages();
    let installed_names = installed
        .iter()
        .map(|package| package.package.clone())
        .collect::<BTreeSet<_>>();
    let locked_names = lockfile.packages.keys().cloned().collect::<BTreeSet<_>>();
    let requirement_names = lockfile
        .requirements
        .iter()
        .cloned()
        .collect::<BTreeSet<_>>();

    let missing_from_lockfile = requirement_names
        .difference(&locked_names)
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

    println!("Requirements: {}", lockfile.requirements.len());
    println!("Locked packages: {}", lockfile.packages.len());
    println!("Installed packages: {}", installed.len());

    if missing_from_lockfile.is_empty()
        && missing_from_library.is_empty()
        && extra_in_library.is_empty()
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

    if !missing_from_library.is_empty() {
        println!("Missing from library: {}", missing_from_library.join(", "));
    }

    if !extra_in_library.is_empty() {
        println!("Extra in library: {}", extra_in_library.join(", "));
    }

    std::process::exit(1);
}

fn sync_from_lockfile() {
    let lockfile = read_lockfile().expect("failed to read lockfile");

    if lockfile.requirements.is_empty() {
        return;
    }

    let requirements = lockfile
        .requirements
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

    exit_with_status(status.code());
}

fn current_lockfile_with_existing_requirements() -> Lockfile {
    let requirements = read_lockfile_optional()
        .expect("failed to read lockfile")
        .map(|lockfile| lockfile.requirements)
        .unwrap_or_default();

    Lockfile {
        version: 1,
        requirements,
        packages: installed_packages()
            .into_iter()
            .map(|package| {
                let name = package.package.clone();
                (name, to_locked_package(package))
            })
            .collect(),
    }
}

fn installed_packages() -> Vec<InstalledPackage> {
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

    exit_with_status(output.status.code());

    parse_installed_packages(&String::from_utf8_lossy(&output.stdout))
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

fn to_locked_package(package: InstalledPackage) -> LockedPackage {
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

fn ensure_requirement(requirements: &mut Vec<String>, package: &str) {
    let mut unique = requirements.iter().cloned().collect::<BTreeSet<_>>();
    unique.insert(package.to_string());
    *requirements = unique.into_iter().collect();
}

fn remove_requirement(requirements: &mut Vec<String>, package: &str) {
    requirements.retain(|requirement| requirement != package);
}

fn read_lockfile() -> Result<Lockfile, String> {
    read_lockfile_optional()?
        .ok_or_else(|| format!("{LOCKFILE_NAME} not found in current directory"))
}

fn read_lockfile_optional() -> Result<Option<Lockfile>, String> {
    let path = lockfile_path();

    if !path.exists() {
        return Ok(None);
    }

    let contents = fs::read_to_string(&path).map_err(|error| error.to_string())?;
    let lockfile = serde_json::from_str(&contents).map_err(|error| error.to_string())?;
    Ok(Some(lockfile))
}

fn write_lockfile(lockfile: Lockfile) {
    let contents = serde_json::to_string_pretty(&lockfile).expect("failed to serialize lockfile");
    fs::write(lockfile_path(), format!("{contents}\n")).expect("failed to write lockfile");
}

fn lockfile_path() -> PathBuf {
    env::current_dir()
        .expect("failed to get current directory")
        .join(LOCKFILE_NAME)
}

fn project_command(program: impl AsRef<str>) -> Command {
    let mut command = Command::new(program.as_ref());
    command.env("R_LIBS_USER", project_library_path());
    command
}

fn project_library_path() -> PathBuf {
    let current_dir = env::current_dir().expect("failed to get current directory");
    let project_key = hash_path(&current_dir);
    let project_dirs =
        ProjectDirs::from("dev", "blyedev", "rpx").expect("failed to resolve rpx data directory");
    let library_path = project_dirs
        .data_dir()
        .join("libraries")
        .join(project_key)
        .join("library");

    fs::create_dir_all(&library_path).expect("failed to create project library");

    library_path
}

fn hash_path(path: &Path) -> String {
    let mut hasher = DefaultHasher::new();
    path.hash(&mut hasher);
    format!("{:016x}", hasher.finish())
}

fn exit_with_status(code: Option<i32>) {
    if code != Some(0) {
        std::process::exit(code.unwrap_or(1));
    }
}
