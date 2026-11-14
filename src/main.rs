//! `gatewayd` — standalone gateway process entry.
//!
//! CLI arguments are parsed with `clap` (derive): `--config <path>` and `--check`, plus
//! the built-in `--help` / `--version`. Everything else (listeners, limits, catalog)
//! lives in the config file. The config is read before the runtime is built because its
//! `workers` key sizes the tokio worker pool.

use std::path::PathBuf;
use std::process::ExitCode;

use anyhow::Context as _;
use clap::Parser;
use gateway_daemon::{DaemonConfig, run};

fn main() -> ExitCode {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let args = Args::parse();
    match real_main(&args) {
        Ok(()) => ExitCode::SUCCESS,
        Err(err) => {
            eprintln!("gatewayd: {err:#}");
            ExitCode::FAILURE
        }
    }
}

fn real_main(args: &Args) -> anyhow::Result<()> {
    let config = DaemonConfig::load(&args.config)?;
    let catalog_path = config.catalog_path(&args.config);
    let runtime = build_runtime(config.workers)?;
    runtime.block_on(run(config, catalog_path, args.check))
}

/// Tokio runtime sized by the config's `workers` (`0` = available parallelism).
///
/// The knob keeps its operator-facing meaning: it sizes the multi-thread runtime's
/// worker threads instead of a framework-specific worker pool.
fn build_runtime(workers: usize) -> anyhow::Result<tokio::runtime::Runtime> {
    let mut builder = tokio::runtime::Builder::new_multi_thread();
    builder.enable_all();
    if workers > 0 {
        builder.worker_threads(workers);
    }
    builder.build().context("build tokio runtime")
}

/// Command line interface, as shown by `--help`.
#[derive(Debug, Parser)]
#[command(
    name = "gatewayd",
    version,
    about = "Standalone gateway process: axum transport over the gateway data plane"
)]
struct Args {
    /// Path to the TOML process config (listeners, limits, catalog, proxy trust).
    ///
    /// Relative `catalog` paths inside resolve against this file's directory.
    #[arg(long, value_name = "PATH", default_value = "gateway.toml")]
    config: PathBuf,

    /// Load the config and catalog, log the summary, and exit without binding.
    #[arg(long)]
    check: bool,
}
