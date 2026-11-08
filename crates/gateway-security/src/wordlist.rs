//! `wordlist` — filter-word config (`config.api.security`, env `FERRITE_SECURITY_JSON`)

use serde::Deserialize;

/// Filter-word config.
///
/// `Default` and the serde defaults go through the same functions: giving the two paths
/// different defaults would fork the behavior of `FilterConfig::default()` and of a
/// `config.api.security` object with missing fields.
#[derive(Debug, Clone, Deserialize)]
pub struct FilterConfig {
    /// Filter-word list; empty = no filtering.
    #[serde(default)]
    pub words: Vec<String>,
    /// Replacement text used on a hit.
    #[serde(default = "default_replacement")]
    pub replacement: String,
    /// Whether to filter requests (input).
    #[serde(default = "default_true")]
    pub filter_request: bool,
    /// Whether to filter responses (output, including streaming).
    #[serde(default = "default_true")]
    pub filter_response: bool,
}

impl Default for FilterConfig {
    fn default() -> Self {
        Self {
            words: Vec::new(),
            replacement: default_replacement(),
            filter_request: default_true(),
            filter_response: default_true(),
        }
    }
}

fn default_replacement() -> String {
    "***".to_string()
}

fn default_true() -> bool {
    true
}
