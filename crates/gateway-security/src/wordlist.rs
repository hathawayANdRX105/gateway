//! `wordlist` — filter-word config (the facade `Config::security` value)

use serde::Deserialize;

/// Filter-word config.
///
/// Defaults live in one place — the `Default` impl below — which serde reads via
/// container-level `#[serde(default)]`: `FilterConfig::default()` and a partial config
/// object with missing fields structurally cannot fork.
#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct FilterConfig {
    /// Filter-word list; empty = no filtering.
    pub words: Vec<String>,
    /// Replacement text used on a hit.
    pub replacement: String,
    /// Whether to filter requests (input).
    pub filter_request: bool,
    /// Whether to filter responses (output, including streaming).
    pub filter_response: bool,
}

impl Default for FilterConfig {
    fn default() -> Self {
        Self {
            words: Vec::new(),
            replacement: "***".to_string(),
            filter_request: true,
            filter_response: true,
        }
    }
}
