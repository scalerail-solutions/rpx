use clap::{Parser, Subcommand};
use std::process::Command;

#[derive(Parser, Debug)]
#[command(name = "rpx")]
#[command(about = "A package manager CLI", long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand, Debug)]
enum Commands {
    Add { package: String },
    Remove { package: String },
}

fn main() {
    let cli = Cli::parse();

    match cli.command {
        Commands::Add { package } => cmd_add(&package),
        Commands::Remove { package } => cmd_remove(&package),
    }
}

fn cmd_add(package: &str) {
    let status = Command::new("Rscript")
        .arg("-e")
        .arg(format!("install.packages('{package}')"))
        .status()
        .expect("failed to run Rscript");

    if !status.success() {
        std::process::exit(status.code().unwrap_or(1));
    }
}

fn cmd_remove(package: &str) {
    let status = Command::new("Rscript")
        .arg("-e")
        .arg(format!("remove.packages('{package}')"))
        .status()
        .expect("failed to run Rscript");

    if !status.success() {
        std::process::exit(status.code().unwrap_or(1));
    }
}
