use super::SyncMeta;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

/// Upper bound on `ChannelSettings.context_window` (tokens). A channel whose window
/// exceeds it is refused at catalog load — it is never silently clamped.
pub const MAX_CONTEXT_WINDOW: u64 = 10_000_000;

/// Channel-level overrides, promoted from the raw settings bag to typed fields.
///
/// Known keys deserialize into typed fields with documented defaults; every other
/// key lands in `extra` and round-trips verbatim, so old settings JSON with
/// arbitrary keys still deserializes and re-serializes unchanged
/// (the `gateway_protocol_bridge::ir` `extra`/`flatten` escape-hatch precedent).
#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
pub struct ChannelSettings {
    #[serde(default)]
    pub priority: i32,
    #[serde(default)]
    pub context_window: u64,
    #[serde(default)]
    pub fallback: bool,
    #[serde(default)]
    pub tpm_limit: u32,
    /// Header overrides: header name → raw JSON value; non-string values are skipped
    /// by the consumer (`extra_headers_from_settings`), matching the old bag-read
    /// leniency; absent means no overrides.
    #[serde(default, skip_serializing_if = "Map::is_empty")]
    pub headers: Map<String, Value>,
    /// Index of the key a multi-key fallback channel uses; `None` = not yet
    /// settings-driven (kept for the #11(a) consumer, no behavior change yet).
    #[serde(default)]
    pub fallback_key_index: Option<u32>,
    /// Unknown settings keys preserved verbatim for lossless round-trip.
    #[serde(default, flatten, skip_serializing_if = "Map::is_empty")]
    pub extra: Map<String, Value>,
}

impl ChannelSettings {
    /// Parse a raw settings bag into typed settings. Wrongly typed known keys are
    /// rejected with the offending key named; unknown keys are preserved in `extra`.
    ///
    /// # Errors
    /// Returns the offending key and serde's reason when the value is not a JSON
    /// object or a known key holds a wrongly typed value.
    pub fn from_value(value: Value) -> Result<Self, String> {
        let mut bag = serde_json::from_value::<Map<String, Value>>(value)
            .map_err(|e| format!("invalid channel settings: {e}"))?;
        Ok(Self {
            priority: take_field(&mut bag, "priority")?,
            context_window: take_field(&mut bag, "context_window")?,
            fallback: take_field(&mut bag, "fallback")?,
            tpm_limit: take_field(&mut bag, "tpm_limit")?,
            headers: take_field(&mut bag, "headers")?,
            fallback_key_index: take_field(&mut bag, "fallback_key_index")?,
            extra: bag,
        })
    }

    /// Load-time invariants: `priority` non-negative, `context_window` within the
    /// documented bound. Rejects the record instead of silently clamping.
    ///
    /// # Errors
    /// Returns the key, the rejected value, and the bound when an invariant is violated.
    pub fn validate(&self) -> Result<(), String> {
        if self.priority < 0 {
            return Err(format!(
                "priority must be non-negative (got {})",
                self.priority
            ));
        }
        if self.context_window > MAX_CONTEXT_WINDOW {
            return Err(format!(
                "context_window {} exceeds the maximum of {} tokens",
                self.context_window, MAX_CONTEXT_WINDOW
            ));
        }
        Ok(())
    }
}

/// Deserialize one known settings key, falling back to `T::default()` when absent.
/// The key is named in the error so catalog-load messages point at the offender.
fn take_field<T: DeserializeOwned + Default>(
    bag: &mut Map<String, Value>,
    key: &str,
) -> Result<T, String> {
    match bag.remove(key) {
        Some(v) => serde_json::from_value(v).map_err(|e| format!("field `{key}`: {e}")),
        None => Ok(T::default()),
    }
}

/// Upstream channel: an external provider access point that dispatch can select.
///
/// Deliberate difference: multiple keys are split into `keys: Vec<ChannelKey>`, and
/// dispatch granularity is the key rather than the channel (new-api's big channel table
/// keeps the keys in one column; here they are explicit).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ChannelRecord {
    pub meta: SyncMeta,
    pub name: String,
    /// Provider protocol family: "openai" | "claude" | "gemini" | "passthrough"
    /// (lowercase). Corresponds to the `gateway_protocol_bridge::Protocol` whitelist.
    pub provider_type: String,
    pub base_url: String,
    /// One set of upstream credentials usable by this channel (`key_index` is the index
    /// into this Vec; dispatch selects by index).
    pub keys: Vec<ChannelKey>,
    /// Channel-wide maximum concurrency (capacity source for the edge-local Semaphore).
    pub max_concurrency: u32,
    /// 1 enabled / 2 manually disabled / 3 auto circuit-broken (driven by health
    /// observations and decided by the center's aggregation)
    pub status: u8,
    /// Groups under which this channel is visible/usable; corresponds to `GroupRecord::id`.
    pub groups: Vec<String>,
    /// Channel-level overrides: priority / context window / tpm limit / fallback flag /
    /// header overrides, typed ([`ChannelSettings`]). Missing settings deserialize to
    /// the defaults; unknown keys are preserved in `settings.extra` so old JSON with
    /// arbitrary keys round-trips losslessly.
    #[serde(default)]
    pub settings: ChannelSettings,
}

/// One upstream credential. Encryption is the store layer's responsibility; the contract
/// only cares about the logical structure.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ChannelKey {
    /// Key index within the channel (stable; deleting a middle key must not renumber,
    /// otherwise route snapshots go stale).
    pub index: u32,
    /// Ciphertext or plaintext, decided by the store layer; the contract layer does not
    /// parse it.
    pub secret: String,
    /// Per-key rate limit (RPM), 0 = unlimited. TODO(ferrite-#211, re-anchored: #13): confirm whether per-key RPM
    /// is needed.
    pub rpm_limit: u32,
}
