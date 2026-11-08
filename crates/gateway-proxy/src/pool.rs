//! `pool` — proxy node pool (single ArcSwap source, indexed by channel_key)
//!
//! Single state `ArcSwap<HashMap<channel_key, Vec<Arc<ProxyNode>>>>`: `install` builds the
//! index once and swaps it wholesale; reads go through `load()` lock-free.
//!
//! Consistency is **eventual**: `install` swaps the pointer atomically, but readers that
//! already hold the old `Arc` keep seeing the old snapshot until their next `load()`. Proxy
//! selection tolerates that lag (picking a just-removed node means at worst this one request
//! leaves through the old egress), which buys a lock-free read path. Strong consistency
//! would require a read-write lock and per-request contention — not worth it.
//!
//! The snapshot type itself is vocabulary and lives in `gateway-contract::proxy::pool`; it is
//! re-exported here so existing `gateway_proxy::pool::*` paths keep resolving.

use arc_swap::ArcSwap;
pub use gateway_contract::proxy::node::ProxyNode;
pub use gateway_contract::proxy::pool::ProxySnapshot;
use rand::Rng;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

/// Proxy node pool.
pub struct ProxyPool {
    // Single state: channel_key -> ProxyNode list sorted by descending priority.
    by_channel: ArcSwap<HashMap<String, Vec<Arc<ProxyNode>>>>,
}

impl ProxyPool {
    pub fn new() -> Self {
        Self {
            by_channel: ArcSwap::from(Arc::new(HashMap::new())),
        }
    }

    /// Replace the whole snapshot: rebuild the channel index.
    pub fn install(&self, snap: ProxySnapshot) {
        let mut channel_map: HashMap<String, Vec<Arc<ProxyNode>>> = HashMap::new();
        for node in snap.nodes {
            let node_arc = Arc::new(node);
            node_arc.channel_keys.iter().for_each(|channel_key| {
                channel_map
                    .entry(channel_key.clone())
                    .or_default()
                    .push(node_arc.clone());
            });
        }
        // Sort each channel list by descending priority (higher priority first).
        channel_map.values_mut().for_each(|nodes| {
            nodes.sort_by(|a, b| b.priority.cmp(&a.priority).then_with(|| a.id.cmp(&b.id)));
        });
        self.by_channel.store(Arc::new(channel_map));
    }

    /// Pick a proxy node for a channel (tiered by priority, random within the tier).
    ///
    /// Injecting `rng` makes tests of the random choice fully deterministic and follows the
    /// same convention as `gateway_dispatch::selector::Selector::pick` (no dependency on a
    /// global random source).
    ///
    /// Returns `None` when the channel has no proxy nodes (callers treat it as direct).
    pub fn pick(&self, channel_key: &str, rng: &mut dyn rand::RngCore) -> Option<Arc<ProxyNode>> {
        let channel_map = self.by_channel.load();
        let nodes = channel_map.get(channel_key)?;
        // The list is sorted by descending priority: the top tier is a prefix, so its length
        // is the tier size.
        let max_priority = nodes.first()?.priority;
        let tier_len = nodes
            .iter()
            .take_while(|n| n.priority == max_priority)
            .count();
        let idx = rng.gen_range(0..tier_len);
        Some(nodes[idx].clone())
    }

    /// All nodes under this channel (already sorted by descending priority). Returns an empty
    /// Vec when there are none.
    pub fn candidates(&self, channel_key: &str) -> Vec<Arc<ProxyNode>> {
        self.by_channel
            .load()
            .get(channel_key)
            .cloned()
            .unwrap_or_default()
    }

    /// All nodes in the current snapshot, deduplicated by id and sorted ascending by id.
    ///
    /// The index is `channel_key → node list`, so a node bound to several channels appears as
    /// the same `Arc` in several buckets. Probing and status export need the *set* of nodes,
    /// hence deduplication by `id` (`HashSet<i64>`). A single `Arc` bound to several channels
    /// is kept only once (install clones from one source), which matches `HashSet` semantics
    /// naturally.
    pub fn all_nodes(&self) -> Vec<Arc<ProxyNode>> {
        let mut seen = HashSet::new();
        let mut out = Vec::new();
        for nodes in self.by_channel.load().values() {
            for node in nodes {
                if seen.insert(node.id) {
                    out.push(Arc::clone(node));
                }
            }
        }
        out.sort_by_key(|n| n.id);
        out
    }
}

impl Default for ProxyPool {
    fn default() -> Self {
        Self::new()
    }
}
