//! `gatewayd` — standalone gateway process entry.
//!
//! CLI arguments are parsed with `clap` (derive): `--config <path>` and `--check`, plus
//! the built-in `--help` / `--version`. Everything else (listeners, limits, catalog)
//! lives in the config file.

use std::path::PathBuf;
use std::process::ExitCode;

use clap::Parser;
use gateway_daemon::{RunArgs, run};

fn main() -> ExitCode {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let args = Args::parse();

    let system = actix_web::rt::System::new();
    match system.block_on(run(RunArgs {
        config_path: args.config,
        check: args.check,
    })) {
        Ok(()) => ExitCode::SUCCESS,
        Err(err) => {
            eprintln!("gatewayd: {err:#}");
            ExitCode::FAILURE
        }
    }
}

/// Command line interface, as shown by `--help`.
#[derive(Debug, Parser)]
#[command(
    name = "gatewayd",
    version,
    about = "Standalone gateway process: actix-web transport over the gateway data plane"
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
