//! Process configuration for the standalone gateway.
//!
//! One TOML file describes the transport (listeners, limits, proxy trust) and points at
//! the catalog file that feeds the data plane. Everything a request can be judged on at
//! runtime lives in the catalog; nothing here is re-read while serving.

use std::net::IpAddr;
use std::path::{Path, PathBuf};

use serde::Deserialize;

/// Largest request body accepted by the daemon, in bytes.
///
/// The library collects the body into memory before the pipeline runs, so this bound is
/// the process's memory guard: without it a single client can exhaust the heap.
pub const DEFAULT_MAX_REQUEST_BYTES: usize = 32 * 1024 * 1024;

/// Listener and process settings.
///
/// `catalog` is resolved relative to the config file's directory, so a deployment can
/// move both files together.
#[derive(Debug, Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct DaemonConfig {
    /// TCP listener, e.g. `127.0.0.1:8080`.
    pub listen: String,
    /// Optional Unix-domain listener for co-located clients.
    pub uds_path: Option<PathBuf>,
    /// Catalog file: channels, groups, users, tokens, prices.
    pub catalog: PathBuf,
    /// Request body ceiling in bytes; larger requests get `413`.
    pub max_request_bytes: usize,
    /// Peers whose `x-forwarded-for` header is trusted. Empty = never trust the header
    /// (the socket peer address is used). Add the reverse proxy's address, not the
    /// client's.
    pub trusted_proxies: Vec<IpAddr>,
    /// Actix worker count; `0` uses the actix default (available parallelism).
    pub workers: usize,
    /// Graceful-shutdown drain budget in seconds.
    pub shutdown_timeout_secs: u64,
}

impl Default for DaemonConfig {
    fn default() -> Self {
        Self {
            listen: "127.0.0.1:8080".to_string(),
            uds_path: None,
            catalog: PathBuf::from("gateway.toml"),
            max_request_bytes: DEFAULT_MAX_REQUEST_BYTES,
            trusted_proxies: Vec::new(),
            workers: 0,
            shutdown_timeout_secs: 30,
        }
    }
}

impl DaemonConfig {
    /// Read and validate the config file.
    ///
    /// # Errors
    ///
    /// Returns the I/O error when the file cannot be read and the parse error when the
    /// TOML is malformed or an unknown field appears (a typo must fail, not silently
    /// fall back to a default).
    pub fn load(path: &Path) -> anyhow::Result<Self> {
        let text = std::fs::read_to_string(path)
            .map_err(|err| anyhow::anyhow!("read config {}: {err}", path.display()))?;
        let config: Self = toml::from_str(&text)
            .map_err(|err| anyhow::anyhow!("parse config {}: {err}", path.display()))?;
        config.validate()?;
        Ok(config)
    }

    /// Catalog path resolved against the config file's directory.
    #[must_use]
    pub fn catalog_path(&self, config_path: &Path) -> PathBuf {
        if self.catalog.is_absolute() {
            return self.catalog.clone();
        }
        config_path
            .parent()
            .unwrap_or_else(|| Path::new("."))
            .join(&self.catalog)
    }

    fn validate(&self) -> anyhow::Result<()> {
        if self.listen.trim().is_empty() {
            anyhow::bail!("config: listen must not be empty (uds_path is additional)");
        }
        if self.max_request_bytes == 0 {
            anyhow::bail!("config: max_request_bytes must be greater than zero");
        }
        Ok(())
    }
}
