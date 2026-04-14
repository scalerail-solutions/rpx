use clap::{Parser, Subcommand};

#[derive(Parser, Debug)]
#[command(name = "rpx")]
#[command(about = "A package manager CLI", long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand, Debug)]
enum Commands {
    Add,
    Remove,
}

fn main() {
    let cli = Cli::parse();

    match cli.command {
        Commands::Add => println!("add"),
        Commands::Remove => println!("remove"),
    }
}
