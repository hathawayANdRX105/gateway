//! The response cache: identical non-streaming requests replayed from the
//! previous upstream answer.
//!
//! One cached response is the client-facing body plus everything a replay
//! needs to settle identically: the recorded usage counts and the recorded
//! channel attribution. The cache key is derived from the requester's identity
//! scope + the public model + the inbound protocol + the raw body — the body
//! is deliberately included (two semantically different requests can serialize
//! to the same bytes only if they *are* the same request), and the scope is
//! deliberately included so a cached answer is never replayed across users,
//! groups, or model price classes.
//!
//! Storage layout: each string component is hashed into its own length-delimited
//! boundary (no concatenation ambiguity from client-controlled model values);
//! the raw body is appended after a length prefix.

use std::sync::Arc;
use std::time::Duration;

use sha2::{Digest, Sha256};

use crate::ttl::TtlCache;

/// Isolation scope of a cached response. Both values come from the requester's
/// identity: the user owns the cached answer, and group membership decides the
/// model allowlist and the price multiplier, so it is part of the key.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CacheScope {
    /// Owning user's UUID (`TokenInfo.user_key`).
    pub user_key: String,
    /// Effective group (`TokenInfo.group`).
    pub group: String,
}

/// Opaque response-cache key — the sha256 digest hex of the isolation-scoped
/// request identity. Construction goes through [`response_key`] only, so the
/// component layout has one authority.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct CacheKey(pub(crate) String);

/// Derive the isolation-scoped cache key for one request.
///
/// Components (in order): user key, group, public model, inbound protocol, raw
/// request body. Each is length-prefixed, so client-controlled values (model
/// names, body bytes) can never collide across boundaries by concatenation.
///
/// The entry records the *original* channel for attribution: two requests with
/// the same key are byte-identical, so whichever upstream served first defines
/// what a replay bills and attributes.
pub fn response_key(scope: &CacheScope, model: &str, protocol: &str, body: &[u8]) -> CacheKey {
    let mut hasher = Sha256::new();
    for part in [
        scope.user_key.as_bytes(),
        scope.group.as_bytes(),
        model.as_bytes(),
        protocol.as_bytes(),
    ] {
        hasher.update((part.len() as u64).to_be_bytes());
        hasher.update(part);
    }
    hasher.update((body.len() as u64).to_be_bytes());
    hasher.update(body);
    CacheKey(hex::encode(hasher.finalize()))
}

/// Token counts recorded with an entry: the settlement basis when the entry is
/// replayed, exactly as the original call settled.
#[derive(Debug, Clone, Copy, Default)]
pub struct CachedUsage {
    pub prompt_tokens: u64,
    pub completion_tokens: u64,
    pub cached_tokens: u64,
    /// Cache-write tokens of the original call (quantity only; not priced).
    pub cache_creation_tokens: u64,
    /// Reasoning/thinking tokens of the original call (quantity only; not priced).
    pub reasoning_tokens: u64,
    /// Whether `prompt_tokens` already includes `cached_tokens` (vendor shape), so a
    /// replay prices identically to the original call.
    pub prompt_includes_cache: bool,
}

/// One cached response: the client-facing body (already in the client's
/// inbound protocol shape) plus everything a replay needs to settle identically.
#[derive(Debug, Clone)]
pub struct CachedResponse {
    pub status: u16,
    pub body: bytes::Bytes,
    pub usage: CachedUsage,
    /// Channel that originally produced this response (attribution to settle
    /// identically on replay).
    pub channel_key: String,
    /// Routing unit that produced this response.
    pub unit_key: String,
    pub public_model: String,
    pub upstream_model: String,
    /// Group the original request settled under (routing-semantic authority:
    /// the replay prices with the recorded group, not a re-read of the
    /// requester's current membership).
    pub group: String,
}

/// Default profile: TTL 300s, 64 MiB total / 1 MiB per entry.
pub const DEFAULT_TTL_SECS: u64 = 300;
pub const DEFAULT_MAX_TOTAL_BYTES: u64 = 64 * 1024 * 1024;
pub const DEFAULT_MAX_ENTRY_BYTES: usize = 1024 * 1024;

/// Byte-bounded response cache: byte-identical request → previous upstream
/// answer. Storage is bounded by *total body bytes* (moka weigher, not entry
/// count — an LLM response is KBs out but can spike to MBs, and the memory
/// bound is what protects the process). Entries above [`ResponseCache::max_entry_bytes`]
/// are refused at store time (they would dominate the budget for one slot).
pub struct ResponseCache {
    entries: TtlCache<CacheKey, Arc<CachedResponse>>,
    max_entry_bytes: usize,
}

impl ResponseCache {
    /// Mounted only when the response-cache switch is on (see app-assembly);
    /// `None` otherwise. Capacity is bounded by total body bytes, per-entry
    /// refusal by `max_entry_bytes`.
    pub fn with_limits(ttl: Duration, max_total_bytes: u64, max_entry_bytes: usize) -> Arc<Self> {
        let entries = TtlCache::bounded_weighted_with_ttl(
            max_total_bytes,
            ttl,
            |_: &CacheKey, v: &Arc<CachedResponse>| u32::try_from(v.body.len()).unwrap_or(u32::MAX),
        );
        Arc::new(Self {
            entries,
            max_entry_bytes,
        })
    }

    /// Lookup one entry. Miss / absent or expired → `None`.
    pub fn lookup(&self, key: &CacheKey) -> Option<Arc<CachedResponse>> {
        self.entries.get(key)
    }

    /// Entry count, forced through the store's maintenance (observability only,
    /// never the hot path — maintenance takes the policy lock).
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Store one entry. Bodies above the per-entry limit are refused with
    /// a debug log (they would dominate the byte budget for one slot).
    pub fn store(&self, key: CacheKey, response: CachedResponse) {
        if response.body.len() > self.max_entry_bytes {
            tracing::debug!(bytes = response.body.len(), "response too large to cache");
            return;
        }
        self.entries.insert(key, Arc::new(response));
    }

    /// Cumulative hit/miss counters (observability only).
    pub fn stats(&self) -> crate::ttl::CacheStats {
        self.entries.stats()
    }
}
