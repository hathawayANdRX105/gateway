//! `gatewayd` — standalone gateway process entry.
//!
//! Argument parsing stays dependency-free: one `--config` path and one `--check` switch.
//! Everything else (listeners, limits, catalog) is in the config file.

use std::path::PathBuf;
use std::process::ExitCode;

use gateway_daemon::{RunArgs, run};

fn main() -> ExitCode {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let args = match parse_args(std::env::args().skip(1)) {
        Ok(args) => args,
        Err(message) => {
            eprintln!("gatewayd: {message}");
            eprintln!("usage: gatewayd --config <path> [--check]");
            return ExitCode::from(2);
        }
    };

    let system = actix_web::rt::System::new();
    match system.block_on(run(args)) {
        Ok(()) => ExitCode::SUCCESS,
        Err(err) => {
            eprintln!("gatewayd: {err:#}");
            ExitCode::FAILURE
        }
    }
}

fn parse_args(mut args: impl Iterator<Item = String>) -> Result<RunArgs, String> {
    let mut config_path = PathBuf::from("gateway.toml");
    let mut check = false;
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--config" => {
                let value = args.next().ok_or("--config requires a path")?;
                config_path = PathBuf::from(value);
            }
            "--check" => check = true,
            other => return Err(format!("unknown argument {other}")),
        }
    }
    Ok(RunArgs { config_path, check })
}
