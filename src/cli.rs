use clap::{Parser, Subcommand};

#[derive(Parser, Debug)]
#[command(name = "rpx")]
#[command(about = "A package manager CLI", long_about = None)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Commands,
}

#[derive(Subcommand, Debug)]
pub enum Commands {
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
