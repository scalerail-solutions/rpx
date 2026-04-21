use clap::{Parser, Subcommand};

#[derive(Parser, Debug)]
#[command(name = "rpx")]
#[command(
    about = "Manage R project dependencies with DESCRIPTION and rpx.lock",
    long_about = None
)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Commands,
}

#[derive(Subcommand, Debug)]
pub enum Commands {
    #[command(
        about = "Initialize an R project",
        long_about = "Initialize an R project in the current directory by creating a DESCRIPTION file."
    )]
    Init,

    #[command(
        about = "Install one or more packages",
        long_about = "Install one or more packages for this project. Each package is recorded in DESCRIPTION, then rpx regenerates rpx.lock and syncs the project library."
    )]
    Add {
        #[arg(
            help = "Package names to add to the project's dependencies",
            value_name = "PACKAGE",
            required = true
        )]
        packages: Vec<String>,
    },

    #[command(
        about = "Remove one or more packages",
        long_about = "Remove one or more packages from this project. The packages are removed from DESCRIPTION, the project library is synced, and rpx regenerates rpx.lock."
    )]
    Remove {
        #[arg(
            help = "Package names to remove from the project's dependencies",
            value_name = "PACKAGE",
            required = true
        )]
        packages: Vec<String>,
    },

    #[command(
        about = "Run a command in the project environment",
        long_about = "Run a command with this project's isolated R package library activated."
    )]
    Run {
        #[arg(
            help = "Command and arguments to run inside the project environment",
            value_name = "COMMAND",
            trailing_var_arg = true,
            allow_hyphen_values = true,
            required = true
        )]
        command: Vec<String>,
    },

    #[command(
        about = "Resolve project dependencies",
        long_about = "Resolve project dependencies from DESCRIPTION and write the resolved package set to rpx.lock without installing packages."
    )]
    Lock,

    #[command(
        about = "Check project dependency state",
        long_about = "Check whether DESCRIPTION, rpx.lock, and the project library are in sync."
    )]
    Status,

    #[command(
        about = "Install the locked package set",
        long_about = "Install the exact package set recorded in rpx.lock into the project library."
    )]
    Sync,
}
