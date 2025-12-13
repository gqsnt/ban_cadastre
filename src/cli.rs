use clap::{Args, Parser, Subcommand};
use std::path::PathBuf;

#[derive(Parser)]
#[command(name = "ban-cadastre")]
#[command(about = "BAN-Cadastre matching tool", long_about = None)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Commands,
}

#[derive(Subcommand)]
pub enum Commands {
    /// Match a single scope (one-off / debugging)
    Link(LinkArgs),
    /// Run the full pipeline for multiple departments
    Pipeline(PipelineArgs),
    /// Analyze results and generate reports
    Analyze(AnalyzeArgs),
    /// Show pipeline status from batch_state.json
    Status(StatusArgs),
}

#[derive(Args, Debug)]
pub struct LinkArgs {
    /// Path to prepared addresses Parquet (columns: id, code_insee, geom(WKB EPSG:2154), existing_link)
    #[arg(long, alias = "addresses")]
    pub input_adresses: PathBuf,

    /// Path to prepared parcels Parquet (columns: id, code_insee, geom(WKB EPSG:2154))
    #[arg(long, alias = "parcels")]
    pub input_parcelles: PathBuf,

    /// Output path for matches Parquet
    #[arg(long)]
    pub output: PathBuf,

    #[arg(long, default_value_t = 50.0)]
    pub distance_threshold: f64,

    #[arg(long, default_value_t = 5)]
    pub num_neighbors: usize,

    #[arg(long, default_value_t = 10000)]
    pub batch_size: usize,

    #[arg(long)]
    pub limit_addresses: Option<usize>,

    #[arg(long)]
    pub filter_commune: Option<String>,
}

#[derive(Args, Debug)]
pub struct PipelineArgs {
    /// Departments manifest CSV path (expects a first column containing department code; header allowed)
    #[arg(long, alias = "manifest")]
    pub departments_file: PathBuf,

    #[arg(long)]
    pub departments: Option<String>,

    #[arg(long)]
    pub data_dir: PathBuf,

    #[arg(long, default_value_t = false)]
    pub resume: bool,

    #[arg(long, default_value_t = false)]
    pub force: bool,

    /// Skip matching step if matches_XX.parquet already exists
    #[arg(long, default_value_t = false, alias = "quick-qa")]
    pub quick_qa: bool,

    #[arg(long)]
    pub limit_addresses: Option<usize>,

    #[arg(long)]
    pub filter_commune: Option<String>,

    /// Return exit code 2 if any department failed (partial run)
    #[arg(long, default_value_t = false)]
    pub strict: bool,
}

#[derive(Args, Debug)]
pub struct AnalyzeArgs {
    #[arg(long)]
    pub results_dir: PathBuf,

    #[arg(long)]
    pub departments_file: PathBuf,

    #[arg(long)]
    pub output_dir: Option<PathBuf>,
    /// Return exit code 2 if inputs are incomplete (missing matches/parcels)
    #[arg(long, default_value_t = false)]
    pub strict: bool,
}

#[derive(Args, Debug)]
pub struct StatusArgs {
    /// Data directory containing batch_state.json
    #[arg(long)]
    pub data_dir: PathBuf,
}
