//! Standalone gateway process.
//!
//! The library crates stay transport-neutral (the facade exposes an axum router for
//! embedding); this package is the process shape: a TOML config, a file catalog that feeds
//! the data-plane snapshots, and an axum listener that runs the pipeline per request.
//!
//! Entry points:
//! - [`run`] — load the catalog and serve (the binary's whole job);
//! - [`catalog::Catalog`] — the file → snapshot loader, usable on its own (`--check`).

pub mod catalog;
pub mod config;
pub mod server;

/// Settlement delivery for the standalone shape (log-only sink).
mod settle_log;

pub use config::DaemonConfig;
pub use server::run;
