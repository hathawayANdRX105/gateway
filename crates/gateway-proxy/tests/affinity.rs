//! `ProxyManager::acquire` soft-affinity node-selection behavior tests
//!
//! Each test verifies one explicit behavior contract; the comment states what is tested
//! and why. Tie-break randomization relies on `rand::thread_rng` (no injectable seed), so
//! assertions depend only on the affinity semantics themselves: a valid pointer must
//! stick; when not sticky, assertions about concrete ids only cover set membership.

use gateway_proxy::manager::ProxyManager;
use gateway_proxy::node::{ProxyNode, ProxyScheme};
use gateway_proxy::pool::ProxySnapshot;

/// Build an Http test node (reqwest native path; does not trigger protocol-adapter
/// dialing).
fn node(id: i64, channels: &[&str], priority: i32) -> ProxyNode {
    ProxyNode {
        id,
        scheme: ProxyScheme::Http,
        host: format!("n{id}.example"),
        port: 8080,
        auth: None,
        // After the NodeOpts rename (#137) the generic transport-layer options field is
        // opts; Http nodes have no transport-layer options.
        opts: None,
        channel_keys: channels
            .iter()
            .map(std::string::ToString::to_string)
            .collect(),
        priority,
    }
}

/// Two equal-priority nodes on the same channel "ch": at any moment (no cooldown) the
/// least-inflight tie group = {1, 2}.
fn two_nodes_channel() -> ProxyManager {
    let mgr = ProxyManager::new();
    mgr.install(ProxySnapshot {
        nodes: vec![node(1, &["ch"], 0), node(2, &["ch"], 0)],
    });
    mgr
}

/// Tests: consecutive acquires stick to the same node when tied for least-inflight,
/// instead of randomizing per request. Why: ws/grpc/TLS handshake cost is paid per
/// connection, and randomizing pays it repeatedly — sticking is exactly M4's purpose.
#[test]
fn consecutive_acquires_stick_to_one_node() {
    let mgr = two_nodes_channel();
    let first = mgr.acquire("ch");
    let id = first.node_id;
    assert_ne!(id, 0, "available nodes must not fall back to direct");
    drop(first);
    for _ in 0..10 {
        let lease = mgr.acquire("ch");
        assert_eq!(
            lease.node_id, id,
            "tie intact within TTL must stick to affinity node"
        );
        drop(lease);
    }
}

/// Tests: after the affinity node is cooled by feedback(502), it leaves the candidate set
/// and acquire falls through to the other node. Why: a cooling node is not in the tie
/// group → affinity lapses → the single remaining node takes over (cooldown triggers
/// rebalance).
#[test]
fn cooled_affinity_node_rebalances_to_other() {
    let mgr = two_nodes_channel();
    let first = mgr.acquire("ch");
    let cooled = first.node_id;
    let other = if cooled == 1 { 2 } else { 1 };
    mgr.feedback(cooled, 502, false);
    drop(first);
    for _ in 0..5 {
        let lease = mgr.acquire("ch");
        assert_eq!(
            lease.node_id, other,
            "cooled affinity node must be succeeded by sole uncooled node"
        );
        drop(lease);
    }
}

/// Tests: once the affinity node's load diverges (holding a Lease raises inflight above
/// the tier minimum), it yields to the idle node automatically. Why: the contract's
/// "rebalance semantics come for free" — a busy node drops out of the tie group and is
/// replaced, with no need to wait for cooldown or TTL expiry.
#[test]
fn busy_affinity_node_yields_to_idle_node() {
    let mgr = two_nodes_channel();
    let busy_lease = mgr.acquire("ch");
    let busy = busy_lease.node_id;
    let idle = if busy == 1 { 2 } else { 1 };
    for _ in 0..5 {
        let lease = mgr.acquire("ch");
        assert_eq!(
            lease.node_id, idle,
            "affinity node inflight above tier minimum must select idle node",
        );
        drop(lease);
    }
    drop(busy_lease);
}

/// Tests: after cooldown recovery the affinity node neither grabs traffic back, deadlocks,
/// nor drops out of selection. Why: after recovery (feedback(200) clears cooldown) the tie
/// group contains both nodes again, and the contract explicitly says "no particular id is
/// required" — assert only that the result ∈ {1,2} (not skipped back to direct) and that
/// subsequent selections stay stably stuck (the affinity pointer does not flip-flop).
#[test]
fn recovered_node_does_not_grab_back_traffic() {
    let mgr = two_nodes_channel();
    let first = mgr.acquire("ch");
    let cooled = first.node_id;
    mgr.feedback(cooled, 502, false);
    drop(first);

    let second = mgr.acquire("ch");
    let taken = second.node_id;
    assert_ne!(
        taken, cooled,
        "during cooldown must defer to the other node"
    );
    drop(second);

    // Clear cooldown: the old node returns to the tie group.
    mgr.feedback(cooled, 200, false);
    let after = mgr.acquire("ch");
    let picked = after.node_id;
    assert!(
        picked == 1 || picked == 2,
        "after recovery must pick from tied nodes, not fall back to direct",
    );
    drop(after);
    for _ in 0..5 {
        let lease = mgr.acquire("ch");
        assert_eq!(
            lease.node_id, picked,
            "first post-recovery choice must stick to affinity, not flip-flop",
        );
        drop(lease);
    }
}

/// Tests: affinity is isolated per `channel_key`; channel B's selection does not rewrite
/// channel A's affinity pointer. Why: if the affinity table were shared (e.g. keyed by node
/// rather than channel), a high-frequency channel would pin a low-frequency channel to the
/// same node, and A's load could not spread.
#[test]
fn affinity_is_isolated_per_channel() {
    let mgr = ProxyManager::new();
    mgr.install(ProxySnapshot {
        nodes: vec![node(1, &["a", "b"], 0), node(2, &["a", "b"], 0)],
    });
    let a_first = mgr.acquire("a").node_id;
    let b_first = mgr.acquire("b").node_id;
    assert!(a_first == 1 || a_first == 2);
    assert!(b_first == 1 || b_first == 2);

    // Hammer b; a's affinity pointer must not be shaken.
    for _ in 0..10 {
        let lease = mgr.acquire("b");
        assert_eq!(
            lease.node_id, b_first,
            "channel b must stick to its own node"
        );
        drop(lease);
    }
    assert_eq!(
        mgr.acquire("a").node_id,
        a_first,
        "channel B's selection must not overwrite channel A's affinity",
    );
}

/// Tests: a channel with no nodes still returns direct `node_id=0` (regression guard), and
/// the affinity table does not "leak" nodes from another channel.
#[test]
fn empty_channel_falls_back_to_direct() {
    let mgr = ProxyManager::new();
    for _ in 0..3 {
        assert_eq!(
            mgr.acquire("missing").node_id,
            0,
            "node-less channel must fall back to direct"
        );
    }
    // After the other channel has an affinity record, the node-less channel still goes
    // direct.
    mgr.install(ProxySnapshot {
        nodes: vec![node(1, &["ch"], 0)],
    });
    assert_eq!(mgr.acquire("ch").node_id, 1);
    assert_eq!(mgr.acquire("missing").node_id, 0);
}
