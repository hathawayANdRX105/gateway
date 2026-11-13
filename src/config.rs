//! Process configuration for the standalone gateway.
//!
//! One TOML file describes the transport (listeners, limits, proxy trust) and points at
//! the catalog file that feeds the data plane. Everything a request can be judged on at
//! runtime lives in the catalog; nothing here is re-read while serving.

use std::net::IpAddr;
use std::path::{Path, PathBuf};

use anyhow::{Context, ensure};
use serde::Deserialize;

/// Largest request body accepted by the daemon, in bytes.
///
/// The library collects the body into memory before the pipeline runs, so this bound is
/// the process's memory guard: without it a single client can exhaust the heap.
pub const DEFAULT_MAX_REQUEST_BYTES: usize = 32 * 1024 * 1024;

/// Default TCP bind host. Loopback only: the standalone shape is fronted by a reverse
/// proxy that terminates TLS, and binding a public interface is a deliberate act.
pub const DEFAULT_LISTEN_HOST: &str = "127.0.0.1";

/// Default TCP bind port.
pub const DEFAULT_LISTEN_PORT: u16 = 8080;

/// Default graceful-shutdown drain budget, in seconds.
pub const DEFAULT_SHUTDOWN_TIMEOUT_SECS: u64 = 30;

/// Listener and process settings.
///
/// Defaults live in the `Default` impl below (serde reads it via container-level
/// `#[serde(default)]`). `catalog` is still required in practice — an omitted path
/// loads as empty and the catalog file open fails at startup.
/// `pool_shard_by_alias` defaults to `true` when the key is absent from the TOML
/// (container-level `default` falls back to `Default::default()`); the helper is
/// explicit so the field-level attribute reads plainly.
#[allow(dead_code)]
fn default_true() -> bool {
    true
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct DaemonConfig {
    /// TCP listener, e.g. `127.0.0.1:8080`.
    pub listen: String,
    /// Optional Unix-domain listener for co-located clients.
    pub uds_path: Option<PathBuf>,
    /// Catalog file: channels, groups, users, tokens, prices. A process with no catalog
    /// has no route and no key: an omitted path loads as empty and the catalog file open
    /// fails at startup.
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
    /// Shard the egress connection pool by the requested model alias (public model
    /// name) instead of sharing one pool across all aliases. Many upstream channels
    /// serve the same public alias, so requests aggregate per alias: a per-alias
    /// pool isolates a hot alias from other aliases while reqwest still pools per
    /// host inside each client. When `false`, every alias uses the single shared
    /// pooled client (the old behavior). `true` by default; the key is documented
    /// here because `DaemonConfig` uses `deny_unknown_fields`, so an unknown or
    /// misspelled key fails config parsing instead of silently falling back.
    #[serde(default = "default_true")]
    pub pool_shard_by_alias: bool,

    /// Dispatch-stage configuration. Omitted = every default (P2C selection).
    pub dispatch: DispatchSection,
}

impl Default for DaemonConfig {
    fn default() -> Self {
        Self {
            listen: format!("{DEFAULT_LISTEN_HOST}:{DEFAULT_LISTEN_PORT}"),
            uds_path: None,
            catalog: PathBuf::new(),
            max_request_bytes: DEFAULT_MAX_REQUEST_BYTES,
            trusted_proxies: Vec::new(),
            workers: 0,
            shutdown_timeout_secs: DEFAULT_SHUTDOWN_TIMEOUT_SECS,
            pool_shard_by_alias: true,

            dispatch: DispatchSection::default(),
        }
    }
}

/// The `[dispatch]` config table: strategy knobs for the route-selection layer.
#[derive(Debug, Clone, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct DispatchSection {
    /// Selection strategy for the route pool. `p2c` (default) = power-of-two-choices
    /// with health/latency/concurrency/token posteriors; `weighted_random` = new-api
    /// same-tier semantics (static weight only; decay/recovery lives in the
    /// cooldown/health layer). The `[dispatch]` table is **boot-fixed**: changing the
    /// strategy rebuilds the data plane.
    #[serde(default)]
    pub selector: gateway_facade::SelectorKind,
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
            .with_context(|| format!("read config {}", path.display()))?;
        let config: Self =
            toml::from_str(&text).with_context(|| format!("parse config {}", path.display()))?;
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
        ensure!(
            !self.listen.trim().is_empty(),
            "config: listen must not be empty (uds_path is additional)"
        );
        ensure!(
            !self.catalog.as_os_str().is_empty(),
            "config: catalog path must not be empty"
        );
        ensure!(
            self.max_request_bytes > 0,
            "config: max_request_bytes must be greater than zero"
        );
        ensure!(
            self.shutdown_timeout_secs > 0,
            "config: shutdown_timeout_secs must be greater than zero"
        );
        Ok(())
    }
}
