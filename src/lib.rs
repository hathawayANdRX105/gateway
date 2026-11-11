//! Standalone gateway process.
//!
//! The library crates stay transport-neutral (the facade exposes an axum router for
//! embedding); this package is the process shape: a TOML config, a file catalog that feeds
//! the data-plane snapshots, and an actix-web listener that runs the pipeline per request.
//!
//! Entry points:
//! - [`run`] — load config + catalog and serve (the binary's whole job);
//! - [`catalog::Catalog`] — the file → snapshot loader, usable on its own (`--check`).

pub mod catalog;
pub mod config;
pub mod server;

pub use config::DaemonConfig;
pub use server::{RunArgs, run};
