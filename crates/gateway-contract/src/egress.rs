//! Egress timeout vocabulary shared by the admin plane and the forwarding engine.
//!
//! The forwarding engine owns the executor (pooled client, proxy leasing, stream
//! handling). The admin plane owns the *parameters*: they are read from the
//! options table, written into [`Timeouts`], and handed to the executor. Keeping
//! the type here is what lets `api-gateway-state` configure egress timeouts
//! without depending on `gateway-forward`.

/// Timeout configuration (ms).
///
/// Runtime source: `first_byte_ms` reads `gateway.timeout.first_byte_ms` from the
/// PG `options` table (a missing row falls back to the registry default 10s; this
/// type's field default of 30s only applies when a caller constructs
/// `Timeouts::default()` directly); `stream_idle_ms` / `stream_max_ms` read
/// `gateway.timeout.stream_idle_ms` / `gateway.timeout.stream_max_ms` (registry
/// defaults 300s / 1800s; applied when the key is absent); `connect_ms` / `total_ms`
/// have no config keys and use this type's defaults of 5s / 300s.
#[derive(Debug, Clone, Copy)]
pub struct Timeouts {
    pub connect_ms: u64,
    pub first_byte_ms: u64,
    pub total_ms: u64,
    /// Maximum silence (ms) between two chunks of an upstream stream after the first
    /// byte. Default 300s: reasoning models (o-series / Claude extended thinking)
    /// can legitimately stay silent for over two minutes, so a lower default would
    /// kill valid streams. Revisit after #158 (live heartbeat) lands.
    pub stream_idle_ms: u64,
    /// Hard ceiling (ms) on the total duration of a streamed response since it was
    /// submitted, so an upstream trickle cannot hold a permit/token forever.
    /// Default 30 minutes.
    pub stream_max_ms: u64,
}

impl Default for Timeouts {
    fn default() -> Self {
        Self {
            connect_ms: 5_000,
            first_byte_ms: 30_000,
            total_ms: 300_000,
            stream_idle_ms: 300_000,
            stream_max_ms: 1_800_000,
        }
    }
}
