//! Egress connection-pool sharding by requested model alias (issue #14).
//!
//! Purpose: the same public alias is served by many upstream channels, so
//! requests aggregate per alias; a per-alias pool isolates a hot alias from
//! other aliases while reqwest still pools per host inside each client.
//!
//! Scenario: `ReqwestEgress` holds an alias-keyed map of `reqwest::Client`s
//! (bound [`MAX_ALIAS_SHARDS`], FIFO eviction) gated by the
//! `pool_shard_by_alias` knob passed to [`ReqwestEgress::new`].
//!
//! Acceptance:
//! 1. same alias twice → identical `Arc` (pool reuse across the channels
//!    serving that alias);
//! 2. two aliases → different `Arc`s (pool isolation);
//! 3. knob `false` → alias-independent single client (the old behavior);
//! 4. bound + 1 aliases → map stays at the bound (FIFO eviction drops the
//!    oldest shard; its `Client` is dropped, closing idle connections).

use std::sync::Arc;

use gateway_forward::egress::{MAX_ALIAS_SHARDS, ReqwestEgress};

/// Same alias resolves to the same pooled client across calls: `client_for`
/// is the testable seam for pool reuse.
#[test]
fn same_alias_returns_identical_client() {
    let egress = ReqwestEgress::new(true);
    let first = egress.client_for("gpt-4o");
    let second = egress.client_for("gpt-4o");
    assert!(
        Arc::ptr_eq(&first, &second),
        "the same alias must reuse one pooled client"
    );
}

/// Different aliases get isolated pools: different clients, so a hot alias
/// cannot starve the connections of another alias.
#[test]
fn different_aliases_get_isolated_clients() {
    let egress = ReqwestEgress::new(true);
    let a = egress.client_for("gpt-4o");
    let b = egress.client_for("claude-3-opus");
    assert!(
        !Arc::ptr_eq(&a, &b),
        "different aliases must not share a pooled client"
    );
}

/// Knob off → one shared client regardless of alias: the old behavior exactly.
#[test]
fn sharding_disabled_returns_single_client() {
    let egress = ReqwestEgress::new(false);
    assert!(
        Arc::ptr_eq(&egress.client_for("a"), &egress.client_for("b")),
        "with sharding off all aliases share the single pooled client"
    );
}

/// The shard map is bounded: inserting `MAX_ALIAS_SHARDS + 1` aliases keeps
/// the map at the bound. The first-inserted alias is evicted (FIFO), so
/// re-resolving it yields a new `Client` rather than the evicted one.
#[test]
fn shard_map_stays_within_bound_after_eviction() {
    let egress = ReqwestEgress::new(true);
    let alias = |i: usize| format!("alias-{i}");
    let mut clients = Vec::new();
    for i in 0..=MAX_ALIAS_SHARDS {
        clients.push(egress.client_for(&alias(i)));
    }
    assert_eq!(
        egress.shard_count(),
        MAX_ALIAS_SHARDS,
        "map must not grow beyond the bound"
    );
    // The evicted alias's original client is gone from the map: re-resolution
    // creates a new one (the eviction dropped the map's reference, closing
    // that client's idle connections once the last external reference drops).
    assert!(
        !Arc::ptr_eq(&clients[0], &egress.client_for(&alias(0))),
        "evicted alias must not reuse the evicted client"
    );
    // The map stayed at the bound through the eviction/re-insertion cycle.
    assert_eq!(egress.shard_count(), MAX_ALIAS_SHARDS);
}
