mod analysis;
mod cli;
mod indexer;
mod link_mode;
mod loader;
mod matcher;
mod pipeline;
mod structures;
mod writer;

use clap::Parser;
use cli::{Cli, Commands};

fn main() -> std::process::ExitCode {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::from_default_env()
                .add_directive(tracing::Level::INFO.into()),
        )
        .init();

    let cli = Cli::parse();

    match cli.command {
        Commands::Link(args) => {
            if let Err(e) = link_mode::run_link(args) {
                eprintln!("{:#}", e);
                return std::process::ExitCode::from(1);
            }
            std::process::ExitCode::from(0)
        }
        Commands::Pipeline(args) => {
            let strict = args.strict;
            match pipeline::run_pipeline(args) {
                Ok(outcome) => {
                    tracing::info!(
                        total_departments = outcome.total_departments,
                        completed_departments = outcome.completed_departments,
                        failed_departments = outcome.failed_departments,
                        partial = outcome.partial,
                        aggregate_partial = outcome.aggregate_partial,
                        state_path = ?outcome.state_path,
                        output_dir = ?outcome.output_dir,
                        "pipeline outcome"
                    );
                    if outcome.partial && strict {
                        std::process::ExitCode::from(2)
                    } else {
                        std::process::ExitCode::from(0)
                    }
                }
                Err(e) => {
                    eprintln!("{:#}", e);
                    std::process::ExitCode::from(1)
                }
            }
        }
        Commands::Analyze(args) => {
            let strict = args.strict;
            match analysis::run_analyze(args) {
                Ok(outcome) => {
                    tracing::info!(
                        output_dir = ?outcome.output_dir,
                        expected_departments = outcome.expected_departments,
                        invalid_manifest_rows = outcome.invalid_manifest_rows,
                        analyzed_departments = outcome.analyzed_departments,
                        skipped_missing_matches = outcome.skipped_missing_matches,
                        skipped_missing_parcels = outcome.skipped_missing_parcels,
                        partial = outcome.partial,
                        "analysis outcome"
                    );
                    if outcome.partial && strict {
                        std::process::ExitCode::from(2)
                    } else {
                        std::process::ExitCode::from(0)
                    }
                }
                Err(e) => {
                    eprintln!("{:#}", e);
                    std::process::ExitCode::from(1)
                }
            }
        }
        Commands::Status(args) => {
            if let Err(e) = pipeline::status::run_status(args) {
                eprintln!("{:#}", e);
                return std::process::ExitCode::from(1);
            }
            std::process::ExitCode::from(0)
        }
    }
}
