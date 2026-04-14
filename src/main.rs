use clap::{Parser, Subcommand};
use directories::ProjectDirs;
use r_description::lossy::{RDescription, Relation, Relations};
use serde::{Deserialize, Serialize};
use std::{
    collections::{hash_map::DefaultHasher, BTreeMap, BTreeSet},
    env, fs,
    hash::{Hash, Hasher},
    path::{Path, PathBuf},
    process::Command,
    str::FromStr,
};

const LOCKFILE_NAME: &str = "rpx.lock";
const DESCRIPTION_NAME: &str = "DESCRIPTION";

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
    }

    let mut description = read_description().expect("failed to read DESCRIPTION");
    description.add_to_imports(package);
    write_description(&description);

    let status = project_command("Rscript")
        .arg("-e")
        .arg(format!("install.packages('{package}')"))
        .status()
        .expect("failed to run Rscript");

    exit_with_status(status.code());
    lock_from_description();
}

fn cmd_remove(package: &str) {
    if lockfile_path().exists() {
        sync_from_lockfile();
    }

    let mut description = read_description().expect("failed to read DESCRIPTION");
    description.remove_from_field("Imports", package);
    description.remove_from_field("Depends", package);
    write_description(&description);

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
    let description = match read_description() {
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

    let manifest_requirements = description
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

    println!("Manifest requirements: {}", manifest_requirements.len());
    println!("Locked requirements: {}", lockfile.requirements.len());
    println!("Locked packages: {}", lockfile.packages.len());
    println!("Installed packages: {}", installed.len());

    if missing_from_lockfile.is_empty()
        && extra_in_lockfile.is_empty()
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

    if !extra_in_lockfile.is_empty() {
        println!("Extra in lockfile: {}", extra_in_lockfile.join(", "));
    }

    if !missing_from_library.is_empty() {
        println!("Missing from library: {}", missing_from_library.join(", "));
    }

    if !extra_in_library.is_empty() {
        println!("Extra in library: {}", extra_in_library.join(", "));
    }

    std::process::exit(1);
}

fn lock_from_description() {
    let requirements = read_description()
        .expect("failed to read DESCRIPTION")
        .requirements();

    install_requirements(&requirements);
    write_lockfile(Lockfile {
        version: 1,
        requirements,
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
    let manifest_requirements = read_description()
        .expect("failed to read DESCRIPTION")
        .requirements();
    let lockfile = read_lockfile().expect("failed to read lockfile");

    if manifest_requirements != lockfile.requirements {
        eprintln!("lockfile out of date; run rpx lock");
        std::process::exit(1);
    }

    install_requirements(&lockfile.requirements);
}

fn install_requirements(requirements: &[String]) {
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

    exit_with_status(status.code());
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

fn remove_installed_package_dir(package: &str) {
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

fn read_description() -> Result<RDescription, String> {
    let path = description_path();
    let contents = fs::read_to_string(&path).map_err(|error| error.to_string())?;
    let description = RDescription::from_str(&contents).map_err(|error| error.to_string())?;

    if description.name.trim().is_empty() {
        return Err("DESCRIPTION is missing Package".to_string());
    }

    Ok(description)
}
fn write_description(description: &RDescription) {
    fs::write(description_path(), format!("{description}")).expect("failed to write DESCRIPTION");
}

trait DescriptionExt {
    fn add_to_imports(&mut self, package: &str);
    fn remove_from_field(&mut self, field_name: &str, package: &str);
    fn requirements(&self) -> Vec<String>;
}

impl DescriptionExt for RDescription {
    fn add_to_imports(&mut self, package: &str) {
        let mut imports = self.imports.clone().unwrap_or_default();

        if imports.iter().any(|entry| entry.name == package) {
            return;
        }

        imports.0.push(Relation {
            name: package.to_string(),
            version: None,
        });
        self.imports = Some(imports);
    }

    fn remove_from_field(&mut self, field_name: &str, package: &str) {
        match field_name {
            "Imports" => {
                let filtered = self
                    .imports
                    .clone()
                    .unwrap_or_default()
                    .0
                    .into_iter()
                    .filter(|entry| entry.name != package)
                    .collect::<Vec<_>>();

                self.imports = if filtered.is_empty() {
                    None
                } else {
                    Some(Relations(filtered))
                };
            }
            "Depends" => {
                let filtered = self
                    .depends
                    .clone()
                    .unwrap_or_default()
                    .iter()
                    .filter(|relation| relation.name != package)
                    .cloned()
                    .collect::<Vec<_>>();

                self.depends = if filtered.is_empty() {
                    None
                } else {
                    Some(Relations(filtered))
                };
            }
            _ => {}
        }
    }

    fn requirements(&self) -> Vec<String> {
        let mut requirements = BTreeSet::new();

        if let Some(imports) = &self.imports {
            for relation in imports.iter() {
                requirements.insert(relation.name.clone());
            }
        }

        if let Some(depends) = &self.depends {
            for relation in depends.iter() {
                let name = relation.name.clone();
                if name != "R" {
                    requirements.insert(name);
                }
            }
        }

        requirements.into_iter().collect()
    }
}

fn read_lockfile() -> Result<Lockfile, String> {
    read_lockfile_optional()?.ok_or_else(|| format!("{LOCKFILE_NAME} not found in project root"))
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

fn description_path() -> PathBuf {
    project_root().join(DESCRIPTION_NAME)
}

fn lockfile_path() -> PathBuf {
    project_root().join(LOCKFILE_NAME)
}

fn project_command(program: impl AsRef<str>) -> Command {
    let mut command = Command::new(program.as_ref());
    command.env("R_LIBS_USER", project_library_path());
    command
}

fn project_library_path() -> PathBuf {
    let project_key = hash_path(&project_root());
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

fn project_root() -> PathBuf {
    let current_dir = env::current_dir().expect("failed to get current directory");
    let current_dir = current_dir
        .canonicalize()
        .unwrap_or_else(|_| current_dir.clone());

    for candidate in current_dir.ancestors() {
        if candidate.join(DESCRIPTION_NAME).exists() {
            return candidate.to_path_buf();
        }
    }

    panic!("{DESCRIPTION_NAME} not found in current directory or any parent directory");
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
