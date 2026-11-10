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
/// `catalog` is required — a process with no catalog has no routes and no keys — and is
/// resolved relative to the config file's directory. There is intentionally no
/// `Default`: the per-field serde defaults below carry the documented values, and
/// `catalog` has none.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DaemonConfig {
    /// TCP listener, e.g. `127.0.0.1:8080`.
    #[serde(default = "default_listen")]
    pub listen: String,
    /// Optional Unix-domain listener for co-located clients.
    #[serde(default)]
    pub uds_path: Option<PathBuf>,
    /// Catalog file: channels, groups, users, tokens, prices. Required: a process with
    /// no catalog has no route and no key, so a missing path must fail at load.
    pub catalog: PathBuf,
    /// Request body ceiling in bytes; larger requests get `413`.
    #[serde(default = "default_max_request_bytes")]
    pub max_request_bytes: usize,
    /// Peers whose `x-forwarded-for` header is trusted. Empty = never trust the header
    /// (the socket peer address is used). Add the reverse proxy's address, not the
    /// client's.
    #[serde(default)]
    pub trusted_proxies: Vec<IpAddr>,
    /// Actix worker count; `0` uses the actix default (available parallelism).
    #[serde(default)]
    pub workers: usize,
    /// Graceful-shutdown drain budget in seconds.
    #[serde(default = "default_shutdown_timeout_secs")]
    pub shutdown_timeout_secs: u64,
}

fn default_listen() -> String {
    "127.0.0.1:8080".to_string()
}

const fn default_max_request_bytes() -> usize {
    DEFAULT_MAX_REQUEST_BYTES
}

const fn default_shutdown_timeout_secs() -> u64 {
    30
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
        if self.catalog.as_os_str().is_empty() {
            anyhow::bail!("config: catalog path must not be empty");
        }
        if self.max_request_bytes == 0 {
            anyhow::bail!("config: max_request_bytes must be greater than zero");
        }
        if self.shutdown_timeout_secs == 0 {
            anyhow::bail!("config: shutdown_timeout_secs must be greater than zero");
        }
        Ok(())
    }
}
