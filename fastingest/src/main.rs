//! CLI entry point for the fastingest sidecar. All the work lives in the
//! library (`lib.rs`); this file only parses arguments, prints the one-line
//! JSON stats blob to stdout, and maps fatal errors to a nonzero exit code.
//!
//! Exit code is 0 even when individual files fail validation (they are
//! counted, skipped, and retried next run since they never enter the
//! manifest); nonzero only for fatal problems (bad args, unreadable dir,
//! output write failure).

use std::path::PathBuf;
use std::process::ExitCode;

use clap::Parser;

use fastingest::{run, Config};

#[derive(Parser)]
#[command(
    about = "Parallel gunzip + serde parse/validate + flatten of *.json.gz job listings",
    long_about = None,
)]
struct Cli {
    /// Directory of *.json.gz job listings to ingest.
    json_dir: PathBuf,
    /// Directory for jobs.arrow, jobs.sqlite, and ingest_manifest.json.
    out_dir: PathBuf,
    /// Process only the first N files (sorted by name).
    #[arg(long, value_name = "N")]
    limit: Option<usize>,
    /// Force a full rebuild, ignoring the manifest.
    #[arg(long)]
    full: bool,
    /// Also write jobs.parquet (zstd).
    #[arg(long)]
    parquet: bool,
}

fn main() -> ExitCode {
    let cli = Cli::parse();
    let stats = match run(Config {
        json_dir: cli.json_dir,
        out_dir: cli.out_dir,
        limit: cli.limit,
        full: cli.full,
        parquet: cli.parquet,
    }) {
        Ok(stats) => stats,
        Err(e) => {
            eprintln!("fastingest: {e}");
            return ExitCode::FAILURE;
        }
    };

    match serde_json::to_string(&stats) {
        Ok(line) => println!("{line}"),
        Err(e) => {
            eprintln!("fastingest: failed to serialize stats: {e}");
            return ExitCode::FAILURE;
        }
    }
    ExitCode::SUCCESS
}
