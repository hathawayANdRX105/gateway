//! `pool` — the installed node set and the admin-plane port over the running pool.
//!
//! [`ProxySnapshot`] is what an admin write installs; [`NodeStats`] is the runtime report
//! row the panel renders. Both are plain data, so they live here rather than with the
//! engine that produces them.
//!
//! [`ProxyPoolHandle`] is the port: it carries only the three operations the admin plane
//! performs on a live pool. Selection, leases, cooldowns, and probing stay in
//! `gateway-proxy` — the request path is deliberately unreachable from here.

use crate::proxy::node::ProxyNode;

/// Full proxy-node snapshot (from `service::sync`).
#[derive(Default)]
pub struct ProxySnapshot {
    pub nodes: Vec<ProxyNode>,
}

/// Node runtime status view: exports the engine's in-process state to the admin plane.
///
/// The engine's inflight / health maps are private, so without this row the admin console
/// has no runtime information at all. Data source for `GET /api/proxy_nodes/report`.
pub struct NodeStats {
    pub node_id: i64,
    pub inflight: u32,
    pub failure_count: u32,
    /// Remaining cooldown seconds; 0 when not cooling down
    pub cooldown_remaining_secs: u64,
    /// Latest probe delay (from the adapter's `ProxyHealth`); None when unrecorded
    pub last_delay_ms: Option<u16>,
}

/// Admin-plane port over the running egress pool.
///
/// Only the operations the admin plane performs: install a rebuilt snapshot after a CRUD
/// write, read the runtime report rows, and ask which egress implementation serves a node
/// (the probe endpoint's answer). The engine keeps its richer surface — `acquire`,
/// `probe_channel`, lease release — which the admin plane must never reach: those are the
/// request path.
pub trait ProxyPoolHandle: Send + Sync {
    /// Replace the node snapshot wholesale. Runtime health / inflight state is not reset
    /// with it: a node id keeps its cooldown history across a reload.
    fn install(&self, snap: ProxySnapshot);

    /// Runtime report rows, one per node in the installed snapshot (deduplicated by id).
    fn node_stats(&self) -> Vec<NodeStats>;

    /// Which egress implementation serves `node`, as the adapter type name, or `None` when
    /// the node needs no adapter (http / socks5 go through reqwest; direct connects
    /// directly) *or* its configuration cannot be assembled at all.
    ///
    /// Collapsing those two cases is deliberate: the probe endpoint already
    /// distinguishes them by looking at the scheme, and it is the only caller.
    fn adapter_type(&self, node: &ProxyNode) -> Option<String>;
}
