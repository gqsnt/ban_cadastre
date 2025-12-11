mod analysis;
mod cli;
mod indexer;
mod link_mode;
mod loader;
mod matcher;
mod pipeline;
mod structures;
mod writer;

use anyhow::Result;
use clap::Parser;
use cli::{Cli, Commands};

fn main() -> Result<()> {
    let cli = Cli::parse();

    match cli.command {
        Commands::Link(args) => {
            link_mode::run_link(args)?;
        }
        Commands::Pipeline(args) => {
            pipeline::run_pipeline(args)?;
        }
        Commands::Analyze(args) => {
            analysis::run_analyze(args)?;
        }
    }

    Ok(())
}
