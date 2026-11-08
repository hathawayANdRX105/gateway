//! `sticky` — prefix-cache sticky binding table (design
//! `docs/design/prompt-cache-sticky-routing.md` §3.1)
//!
//! Once the same (session × public model) pair has hit the upstream prefix cache, the channel
//! is bound into this table; later requests receive a boost in the **prior layer** (the boost
//! consumer is select, see the wiring ticket), while health / cooldown / rate-limit /
//! concurrency-gate vetoes stay fully intact — stickiness is a hint, not ground truth.
//!
//! ## Key composition
//!
//! [`StickyKey`] = `(public_model, prefix)`, where `prefix` is one of:
//!
//! - `"sid:" + explicit session id` (**preferred**): the `x-session-id` header (or a
//!   [`RequestCtx::session_hint`](https://docs.rs/gateway-pipeline) pre-filled at assembly
//!   time), truncated to [`SESSION_PREFIX_LIMIT`] bytes so an oversized header cannot blow up
//!   table memory;
//! - `"fp:" + SipHash-1-3 zero-key fingerprint of the first 4KB of the raw request body`
//!   ([FINGERPRINT_WINDOW] bytes): weak stickiness for shared prefixes. **Only the hash is
//!   stored, never the prefix text** (privacy + bounded memory).
//!
//! ## Fingerprint source: raw request body, not the adapted body
//!
//! Design §3.1 originally said "use `adapted_body` (the unified upstream view after
//! protocol-bridge adaptation)", but the routing side **cannot physically obtain it**: protocol
//! adaptation (`rewrite_upstream_model` + `translate_request`) happens inside `forward_once`,
//! i.e. **after** `dispatch.select` (`forward_once` in `forward/src/pipeline.rs`). The
//! fingerprint source is therefore the `InMemory` raw bytes of
//! [`RequestMeta::body`](gateway_pipeline::RequestMeta), and **the write side (settle writing
//! the table) and the read side (routing lookup) must use the same source** — both take the raw
//! bytes of `ctx.request.body`.
//!
//! **Known limitation (not pretended away)**: when the same session enters through different
//! client protocols (OpenAI / Anthropic / Gemini entry points), the raw bytes differ ⇒ the
//! fingerprint keys differ ⇒ the lookup misses ⇒ behaviour degrades to the status quo (P2C
//! only, no stickiness). That is a **distribution shift, not a correctness loss**: a miss costs
//! only one boost. The main path for strong cross-protocol stickiness is the explicit
//! `x-session-id` header.
//!
//! ## Lifecycle
//!
//! - Storage sits on the shared cache substrate
//!   [`TtlCache`](gateway_cache::TtlCache) (moka-backed): **capacity eviction is the store's
//!   job** — a full table makes room by evicting the least recently used binding, so a fresh
//!   session is never starved by a stable hot set;
//! - Default TTL is 300s (within Claude's 5m cache lifetime); a repeated `bind` call **slides
//!   the expiry**. Two layers enforce it: the per-entry `expire_at_ms` checked against this
//!   table's own (injectable) clock — the read-time authority, `expire_at <= now` means
//!   expired — and the store-level TTL, which reclaims expired entries opportunistically
//!   (maintenance + expired-on-read);
//! - An expired lookup deletes the entry on the spot, so it cannot occupy a slot;
//! - Table full / expired / no key source / table not mounted ⇒ always degrade to "not
//!   sticky", never panic. Capacity 0 is honored literally: nothing is ever written.

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::sync::Arc;
use std::time::Duration;

use gateway_cache::TtlCache;

/// Number of raw body bytes that feed the fingerprint (first 4KB). A long session's key is
/// determined by its opening segment, so tail rewrites (the normal shape of continuing a
/// conversation) do not split the key.
pub const FINGERPRINT_WINDOW: usize = 4096;

/// Byte cap for the `sid:` prefix (including `"sid:"` itself). Overlong values are truncated on
/// a UTF-8 character boundary — the header value is client-controlled, and without truncation
/// it would be a memory-amplification surface.
pub const SESSION_PREFIX_LIMIT: usize = 128;

/// Explicit session header name (the fallback source the routing side extracts from
/// `RequestMeta.headers`).
pub const SESSION_HEADER: &str = "x-session-id";

/// Default TTL (ms): 300s.
pub const DEFAULT_TTL_MS: u64 = 300_000;

/// Default capacity limit (entry count).
pub const DEFAULT_CAPACITY: usize = 10_000;

/// Sticky key: (public model name, prefix identity). `Eq + Hash` feeds the store directly.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct StickyKey {
    pub public_model: String,
    /// `"sid:<session>"` or `"fp:<016x hash>"` (see the module docs).
    pub prefix: String,
}

impl StickyKey {
    /// Construct a sticky key. `None` = "this request is not sticky" (the caller takes the
    /// status-quo path directly).
    ///
    /// Priority:
    /// 1. `session_hint` non-empty after trimming ⇒ `prefix = "sid:" + hint` (truncated to
    ///    [`SESSION_PREFIX_LIMIT`] bytes). Whitespace / empty string counts as **no** session
    ///    header — otherwise every session would crowd into the same `"sid:"` key and pollute
    ///    each other.
    /// 2. Otherwise fall back to the raw-body fingerprint ([`FINGERPRINT_WINDOW`] bytes);
    ///    `raw_body` is `None` (not InMemory) or the body is empty ⇒ `None`: no shared prefix,
    ///    no reason to be sticky.
    pub fn from_request(
        public_model: &str,
        session_hint: Option<&str>,
        raw_body: Option<&[u8]>,
    ) -> Option<Self> {
        let prefix = match session_hint.map(str::trim).filter(|h| !h.is_empty()) {
            Some(hint) => truncate_utf8(&format!("sid:{hint}"), SESSION_PREFIX_LIMIT),
            None => {
                let body = raw_body?;
                if body.is_empty() {
                    return None;
                }
                fingerprint(body)
            }
        };
        Some(Self {
            public_model: public_model.to_string(),
            prefix,
        })
    }
}

/// One binding: channel key + absolute expiry instant (ms, in the table's own clock frame).
///
/// The expiry travels with the value because the table's clock is injectable (fake clock in
/// tests): the store-level TTL runs on the real clock, the read-time boundary runs on this one.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StickyEntry {
    pub channel_key: String,
    pub expire_at_ms: u64,
}

/// Sticky binding table: `(public model, prefix)` → channel.
///
/// Pure in-memory, single instance (bindings are not shared across instances). The clock is
/// injectable (fake clock in tests); defaults to `chrono::Utc` milliseconds. Capacity eviction
/// and reclamation are delegated to the shared [`TtlCache`] substrate; this table owns the
/// binding semantics (sliding TTL on the table's own clock).
pub struct StickyTable {
    entries: TtlCache<StickyKey, StickyEntry>,
    clock: Arc<dyn Fn() -> u64 + Send + Sync + 'static>,
    ttl_ms: u64,
    capacity: usize,
}

impl StickyTable {
    /// Default profile: TTL 300s / capacity 10_000 / `chrono::Utc` millisecond clock.
    pub fn new() -> Arc<Self> {
        Self::with_clock(
            || u64::try_from(chrono::Utc::now().timestamp_millis()).unwrap_or(u64::MAX),
            DEFAULT_TTL_MS,
            DEFAULT_CAPACITY,
        )
    }

    /// Inject a clock (returning milliseconds) + custom TTL / capacity (fake clock and small
    /// capacity in tests).
    pub fn with_clock(
        now_ms: impl Fn() -> u64 + Send + Sync + 'static,
        ttl_ms: u64,
        capacity: usize,
    ) -> Arc<Self> {
        Arc::new(Self {
            entries: TtlCache::bounded_with_ttl(capacity as u64, Duration::from_millis(ttl_ms)),
            clock: Arc::new(now_ms),
            ttl_ms,
            capacity,
        })
    }

    /// This table's clock reading (ms). The `now_ms` of [`StickyTable::lookup`] must come from
    /// here — callers do not hold the table's clock, and mixing in an external clock would
    /// distort the TTL decision.
    pub fn now_ms(&self) -> u64 {
        (self.clock)()
    }

    /// Current entry count (live bindings; expired entries not yet reclaimed by the store's
    /// maintenance are counted too). Used for observability and for tests asserting
    /// reclamation behaviour.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Write / extend a binding: `expire_at = now + ttl` (sliding TTL; an active session keeps
    /// renewing itself). Capacity pressure is the store's business: a full table evicts its
    /// least recently used binding to admit the new one. Capacity 0 degrades to rejection
    /// before any write (moka would surface a just-inserted entry until its maintenance
    /// evicts it, which would violate the "admit nothing" contract).
    pub fn bind(&self, key: StickyKey, channel_key: &str) {
        if self.capacity == 0 {
            return;
        }
        let now = (self.clock)();
        self.entries.insert(
            key,
            StickyEntry {
                channel_key: channel_key.to_string(),
                expire_at_ms: now.saturating_add(self.ttl_ms),
            },
        );
    }

    /// Look up the sticky channel: a hit returns the channel key; expired / absent returns
    /// `None` (an expired entry is deleted on the spot).
    ///
    /// `now_ms` must be a reading of this table's clock (see [`StickyTable::now_ms`]).
    pub fn lookup(&self, key: &StickyKey, now_ms: u64) -> Option<String> {
        match self.entries.get(key) {
            Some(entry) if entry.expire_at_ms > now_ms => Some(entry.channel_key),
            // Expired (read-time boundary: `expire_at <= now` means expired) → delete on the
            // spot; absent needs no action.
            Some(_) => {
                self.entries.invalidate(key);
                None
            }
            None => None,
        }
    }
}

/// Raw-body fingerprint: `"fp:" + SipHash-1-3 (zero key) of the first 4KB`.
///
/// `DefaultHasher` = SipHash-1-3 with a fixed zero key; std guarantees the same input always
/// yields the same output. The hash source is a fixed-length slice ⇒ tail rewrites after 4KB do
/// not change the result. Both sides (routing lookup / settle write) go through this function
/// only, ruling out "write and read from different sources".
fn fingerprint(body: &[u8]) -> String {
    let window = &body[..body.len().min(FINGERPRINT_WINDOW)];
    let mut hasher = DefaultHasher::new();
    window.hash(&mut hasher);
    format!("fp:{:016x}", hasher.finish())
}

/// Truncate to at most `limit` bytes, landing on a UTF-8 character boundary (never produce an
/// invalid string).
///
/// Cost: the boundary lookup is O(1) (`floor_char_boundary` backwalks at most 3 bytes), so the
/// total cost is O(min(limit, s.len())) for the copy.
fn truncate_utf8(s: &str, limit: usize) -> String {
    s[..s.floor_char_boundary(limit)].to_string()
}
