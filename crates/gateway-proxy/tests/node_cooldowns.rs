//! `node_cooldowns` / `channel_node_ids` accessor behavior tests (P1-C dual-ledger
//! bridge data source).
//!
//! These cover the public semantics the forward-side downgrade decision relies on, all
//! through the public API:
//! - The cooldown table only holds entries that are "still cooling": feedback(502)
//!   inserts, feedback(200) evicts. forward decides whether a direct fallback is a
//!   downgrade via "all bound nodes ∈ cooldown table"; stale entries would misjudge a
//!   healthy channel as dead;
//! - The binding table distinguishes "nodes bound" from "no binding at all" — for the
//!   latter, a direct connection is the expected path, not a downgrade.

use gateway_proxy::manager::ProxyManager;
use gateway_proxy::node::{ProxyNode, ProxyScheme};
use gateway_proxy::pool::ProxySnapshot;
use std::time::Instant;

fn node(id: i64, channels: &[&str]) -> ProxyNode {
    ProxyNode {
        id,
        scheme: ProxyScheme::Http,
        host: format!("n{id}.example"),
        port: 8080,
        auth: None,
        opts: None,
        channel_keys: channels
            .iter()
            .map(std::string::ToString::to_string)
            .collect(),
        priority: 0,
    }
}

fn manager_with(nodes: Vec<ProxyNode>) -> ProxyManager {
    let mgr = ProxyManager::new();
    mgr.install(ProxySnapshot { nodes });
    mgr
}

/// Test: with no failure the cooldown table is empty; after a transport failure the
/// node enters the table with a deadline in the future.
/// Why: this is the only signal source for forward to judge "the node really is
/// cooling".
#[test]
fn feedback_cools_node_into_cooldown_table() {
    let mgr = manager_with(vec![node(1, &["ch"])]);
    assert!(
        mgr.node_cooldowns().is_empty(),
        "no nodes in cooldown initially"
    );

    mgr.feedback(1, 502, true);
    let cooled = mgr.node_cooldowns();
    assert_eq!(
        cooled.len(),
        1,
        "after a 502 transport failure there should be exactly one cooling node"
    );
    assert_eq!(cooled[0].0, 1);
    assert!(
        cooled[0].1 > Instant::now(),
        "deadline must be in the future (still cooling)"
    );
}

/// Test: success feedback evicts the cooldown entry (expired/lifted nodes are not
/// returned).
/// Why: if a recovered node lingered in the table, forward would permanently misjudge
/// the channel as downgraded.
#[test]
fn success_evicts_node_from_cooldown_table() {
    let mgr = manager_with(vec![node(1, &["ch"])]);
    mgr.feedback(1, 502, true);
    mgr.feedback(1, 200, false);
    assert!(
        mgr.node_cooldowns().is_empty(),
        "cooldown lifted after success; no stale table entries allowed"
    );
}

/// Test: direct-connection feedback (`node_id` == 0) never enters the cooldown table.
/// Why: 0 is not a real node; including it would make every unbound channel look like
/// "all bound nodes are cooling".
#[test]
fn direct_feedback_is_ignored() {
    let mgr = manager_with(vec![]);
    mgr.feedback(0, 502, true);
    assert_eq!(mgr.node_cooldowns(), [] as [(i64, std::time::Instant); 0]);
}

/// Test: `channel_node_ids` returns node ids by binding, and an unbound channel returns
/// empty.
/// Why: an empty table means "no binding to begin with", so forward treats a direct
/// connection as the expected path rather than a downgrade.
#[test]
fn channel_node_ids_reflects_binding() {
    let mgr = manager_with(vec![node(1, &["ch-a"]), node(2, &["ch-a", "ch-b"])]);
    assert_eq!(mgr.channel_node_ids("ch-a"), vec![1, 2]);
    assert_eq!(mgr.channel_node_ids("ch-b"), vec![2]);
    assert_eq!(mgr.channel_node_ids("ch-none"), [] as [i64; 0]);
}
