use clap::{Parser, Subcommand};
use directories::ProjectDirs;
use std::{
    collections::hash_map::DefaultHasher,
    env, fs,
    hash::{Hash, Hasher},
    path::PathBuf,
    process::Command,
};

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
}

fn main() {
    let cli = Cli::parse();

    match cli.command {
        Commands::Add { package } => cmd_add(&package),
        Commands::Remove { package } => cmd_remove(&package),
        Commands::Run { command } => cmd_run(&command),
    }
}

fn cmd_add(package: &str) {
    let status = project_command("Rscript")
        .arg("-e")
        .arg(format!("install.packages('{package}')"))
        .status()
        .expect("failed to run Rscript");

    exit_with_status(status.code());
}

fn cmd_remove(package: &str) {
    let status = project_command("Rscript")
        .arg("-e")
        .arg(format!("remove.packages('{package}')"))
        .status()
        .expect("failed to run Rscript");

    exit_with_status(status.code());
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

fn hash_path(path: &std::path::Path) -> String {
    let mut hasher = DefaultHasher::new();
    path.hash(&mut hasher);
    format!("{:016x}", hasher.finish())
}

fn exit_with_status(code: Option<i32>) {
    if code != Some(0) {
        std::process::exit(code.unwrap_or(1));
    }
}
