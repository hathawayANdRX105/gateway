//! `manager` — lease an egress Client per channel + in-process health
//!
//! Aligned with grok2api-sing's `Acquire` / `Feedback` / per-node client cache.
//! HTTP CONNECT / SOCKS5 handshakes are still delegated to reqwest (the `socks`
//! feature); this module does not implement vless/vmess/ss/trojan. With no nodes or
//! all nodes cooling down, it falls back to the direct Client (`node_id = 0`).
//!
//! Protocol nodes (SS/Trojan/VLESS/VMess/Hysteria2/AnyTLS/Snell) are mapped to meow
//! `ProxyAdapter`s by `crate::adapter::adapter_for` (cached in connector_cache); a
//! failed mapping falls back to direct.
//! ponytail: the health table and soft affinity live only in-process; DB persistence
//! can be added when needed.
use super::pool::ProxyPool;
use super::probe::ProbeResult;
use gateway_cache::TtlCache;
use gateway_core::proxy::node::{ProxyNode, ProxyScheme};
/// Per-node runtime counters — defined on the injection surface; re-exported so the
/// historical `gateway_proxy::manager::NodeStats` path keeps resolving.
pub use gateway_core::proxy::pool::NodeStats;
use gateway_core::proxy::pool::{ProxyPoolHandle, ProxySnapshot};
use meow_common::ProxyAdapter;
use rand::seq::SliceRandom;
use reqwest::redirect::Policy as RedirectPolicy;
use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, Instant};

/// Soft-affinity TTL: a channel's affinity record older than this since last use is
/// considered stale (the node-switch window), and the next tie is randomly
/// rebalanced. Under sustained traffic the timestamp refreshes on every selection,
/// so an active node never expires by TTL; only a stall >10s (the connection is
/// probably idle and closed) allows switching.
/// ponytail: fixed value; add an options entry if it needs to be configurable.
const AFFINITY_TTL: Duration = Duration::from_secs(10);

/// Client / adapter cache bound (entry count) and time-to-idle. The key is
/// `(node id, config fingerprint)`: live entries ≤ node count, and stale
/// fingerprints (config edits) plus retired nodes are the only growth source,
/// so the cap is a leak guard, not a working-set size. Time-to-idle fits the
/// usage: `acquire` touches the entry on every egress, so actively used
/// entries never expire, while a config not leased for 30 min is reclaimed and
/// rebuilt on demand.
const CLIENT_CACHE_CAPACITY: u64 = 1024;
const CLIENT_CACHE_IDLE: Duration = Duration::from_secs(30 * 60);

/// Take an in-process state lock and treat poisoning as recoverable: these locks
/// only guard in-memory health / inflight / cache tables, a panic while holding one
/// leaves no half-updated table, and later requests can keep reading and writing.
fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(|e| e.into_inner())
}

/// Egress form: reqwest native (direct/http/socks5) or a protocol adapter (meow).
///
/// The `Reqwest` variant is used by forward directly as a `reqwest::Client`; the
/// `Adapter` variant is bridged into a hyper connector by
/// `gateway_forward::adapter_egress` — the protocol handshake happens in `dial_tcp`
/// (meow), and the bridge layers TLS on top and carries HTTP.
#[derive(Clone)]
pub enum ProxyClient {
    /// reqwest native path (Direct/Http/Socks5).
    Reqwest(Arc<reqwest::Client>),
    /// Protocol adapter path (SS/Trojan/VLESS/VMess, implemented by meow).
    Adapter(Arc<dyn ProxyAdapter>),
}

/// Egress Client held by one upstream attempt.
///
/// `Drop` decrements the matching node's inflight. `node_id == 0` means direct and
/// has no release action.
pub struct Lease {
    /// Selected node id; `0` = direct (no proxy nodes or all cooling down).
    pub node_id: i64,
    /// Egress form.
    pub client: Arc<ProxyClient>,
    release: Option<Arc<dyn Fn() + Send + Sync>>,
}

impl Lease {
    /// Return the reqwest Client (only for the [`ProxyClient::Reqwest`] variant).
    ///
    /// The `Adapter` variant returns `None` — protocol egress goes through
    /// [`Self::adapter`].
    pub fn reqwest_client(&self) -> Option<Arc<reqwest::Client>> {
        match self.client.as_ref() {
            ProxyClient::Reqwest(c) => Some(Arc::clone(c)),
            ProxyClient::Adapter(_) => None,
        }
    }

    /// Return the protocol adapter (only for the [`ProxyClient::Adapter`] variant).
    pub fn adapter(&self) -> Option<Arc<dyn ProxyAdapter>> {
        match self.client.as_ref() {
            ProxyClient::Adapter(a) => Some(Arc::clone(a)),
            ProxyClient::Reqwest(_) => None,
        }
    }
}

impl Drop for Lease {
    fn drop(&mut self) {
        if let Some(release) = self.release.take() {
            release();
        }
    }
}

#[derive(Default)]
struct NodeHealth {
    failure_count: u32,
    cooldown_until: Option<Instant>,
}

/// Channel egress manager: selects nodes, caches Clients, cools down by feedback.
pub struct ProxyManager {
    pool: ProxyPool,
    direct_client: Arc<reqwest::Client>,
    /// Egress clients / protocol adapters by `(node id, config fingerprint)`.
    /// Bounded + idle-evicting (`TtlCache`): a churning config (fingerprint
    /// changes on every edit) used to grow these maps forever — each stale
    /// fingerprint kept a full connection pool alive; idle reclamation drops
    /// configs not leased for [`CLIENT_CACHE_IDLE`].
    connector_cache: TtlCache<(i64, u64), Arc<dyn ProxyAdapter>>,
    inflight: Arc<Mutex<HashMap<i64, u32>>>,
    client_cache: TtlCache<(i64, u64), Arc<reqwest::Client>>,
    health: Mutex<HashMap<i64, NodeHealth>>,
    /// Soft affinity: channel_key → (last selected node id, last selection time).
    /// Only takes part in the decision when the least-inflight tie group has >1
    /// member; the pointer always names the node that last actually served this
    /// channel's traffic.
    affinity: Mutex<HashMap<String, (i64, Instant)>>,
}

/// Admin-plane port: the three operations the control plane performs on a live pool.
///
/// `adapter_type` answers the probe endpoint's question without handing out an adapter (the
/// probe endpoint must not be able to dial). `None` covers both "no adapter needed"
/// (http / socks5 go through reqwest, direct connects directly) and "configuration cannot be
/// assembled" — the caller has the node and distinguishes the two by scheme, which is also
/// why it owns that rule.
impl ProxyPoolHandle for ProxyManager {
    fn install(&self, snap: ProxySnapshot) {
        ProxyManager::install(self, snap);
    }

    fn node_stats(&self) -> Vec<NodeStats> {
        ProxyManager::node_stats(self)
    }

    fn adapter_type(&self, node: &ProxyNode) -> Option<String> {
        crate::adapter::adapter_for(node).map(|a| a.adapter_type().to_string())
    }
}

impl Default for ProxyManager {
    fn default() -> Self {
        Self::new()
    }
}

impl ProxyManager {
    /// Empty pool + shared direct Client (redirects disabled, connect 5s, 8 idle per
    /// host).
    pub fn new() -> Self {
        Self {
            pool: ProxyPool::new(),
            direct_client: Arc::new(build_client(None)),
            inflight: Arc::new(Mutex::new(HashMap::new())),
            client_cache: TtlCache::bounded_with_idle(CLIENT_CACHE_CAPACITY, CLIENT_CACHE_IDLE),
            connector_cache: TtlCache::bounded_with_idle(CLIENT_CACHE_CAPACITY, CLIENT_CACHE_IDLE),
            health: Mutex::new(HashMap::new()),
            affinity: Mutex::new(HashMap::new()),
        }
    }

    /// Replace the node snapshot wholesale. Health / inflight / client caches are not
    /// cleared with the snapshot.
    pub fn install(&self, snap: ProxySnapshot) {
        self.pool.install(snap);
    }

    /// Lease an egress for `channel_key`.
    ///
    /// No nodes or all cooling down → the direct Client with `node_id = 0`.
    /// Otherwise: skip cooling nodes → highest priority tier → least-inflight within
    /// the tier → soft affinity on ties (stick to the previous node within TTL;
    /// expired or no longer eligible means a random rebalance) → a single selection
    /// goes straight through.
    pub fn acquire(&self, channel_key: &str) -> Lease {
        let now = Instant::now();
        let candidates = self.pool.candidates(channel_key);
        let health = lock(&self.health);
        let inflight = lock(&self.inflight);

        let mut eligible: Vec<Arc<ProxyNode>> = candidates
            .into_iter()
            .filter(|node| {
                health
                    .get(&node.id)
                    .and_then(|h| h.cooldown_until)
                    .is_none_or(|until| until <= now)
            })
            .collect();
        drop(health);

        if eligible.is_empty() {
            // No nodes / all cooling down → direct.
            return self.direct_lease();
        }

        let max_priority = eligible.iter().map(|n| n.priority).max().unwrap_or(0);
        eligible.retain(|n| n.priority == max_priority);

        let min_inflight = eligible
            .iter()
            .map(|n| inflight.get(&n.id).copied().unwrap_or(0))
            .min()
            .unwrap_or(0);
        eligible.retain(|n| inflight.get(&n.id).copied().unwrap_or(0) == min_inflight);
        drop(inflight);

        let selected = self.select_candidate(channel_key, eligible, now);
        let node_id = selected.id;
        let release = self.track_inflight(node_id);
        let client = self.client_for(&selected);
        Lease {
            node_id,
            client,
            release: Some(release),
        }
    }

    /// Pick a node within the least-inflight tie group: stick to the affinity node
    /// within TTL, otherwise pick randomly in the group and write the pointer back.
    ///
    /// Once the affinity node's load diverges and it drops out of the tie group, the
    /// rebalance happens automatically with no extra code. Lock order: health /
    /// inflight were already released by the caller; the affinity lock covers only
    /// this decision block and is neither held across candidate construction nor
    /// nested with the inflight increment.
    // The two expects assert internal invariants (sticky id is filtered by
    // eligibility; eligible is non-empty when reached); this helper has no external
    // failure mode, so converting it to Result would only ripple through callers.
    #[allow(unknown_lints)]
    #[allow(expect_in_non_result)]
    fn select_candidate(
        &self,
        channel_key: &str,
        eligible: Vec<Arc<ProxyNode>>,
        now: Instant,
    ) -> Arc<ProxyNode> {
        let mut affinity = lock(&self.affinity);
        if eligible.len() == 1 {
            return eligible[0].clone();
        }
        let sticky = affinity
            .get(channel_key)
            .filter(|(_, last_used)| last_used.elapsed() <= AFFINITY_TTL)
            .map(|(id, _)| *id)
            .filter(|id| eligible.iter().any(|n| n.id == *id));
        // Affinity not expired and still in the least-inflight tie group → stick to
        // it without randomizing; first selection / expired / affinity node no longer
        // eligible → random within the tie group (rebalance).
        let chosen = sticky.map_or_else(
            || {
                eligible
                    .choose(&mut rand::thread_rng())
                    .cloned()
                    .expect("eligible non-empty")
            },
            |id| {
                eligible
                    .iter()
                    .find(|n| n.id == id)
                    .cloned()
                    .expect("sticky id present in eligible")
            },
        );
        affinity.insert(channel_key.to_string(), (chosen.id, now));
        chosen
    }

    /// inflight +1, returning a release closure that decrements on `Lease::drop`.
    fn track_inflight(&self, node_id: i64) -> Arc<dyn Fn() + Send + Sync> {
        let inflight = Arc::clone(&self.inflight);
        {
            let mut map = lock(&inflight);
            *map.entry(node_id).or_insert(0) += 1;
        }
        Arc::new(move || {
            let mut map = lock(&inflight);
            if let Some(count) = map.get_mut(&node_id) {
                *count = count.saturating_sub(1);
                if *count == 0 {
                    map.remove(&node_id);
                }
            }
        })
    }

    /// Update node health from this attempt's HTTP status / transport failure.
    ///
    /// - `node_id == 0`: direct connection, ignored.
    /// - `401` / `429`: account problems, do not cool down the proxy.
    /// - success (no transport error and 2xx/3xx): clear the cooldown, reset the
    ///   failure count.
    /// - otherwise: failure count +1, cooldown `min(10min, 30s << min(n-1, 4))`.
    pub fn feedback(&self, node_id: i64, status: u16, transport_err: bool) {
        if node_id == 0 || status == 401 || status == 429 {
            return;
        }
        let mut health = lock(&self.health);
        let node = health.entry(node_id).or_default();
        if !transport_err && (200..400).contains(&status) {
            node.failure_count = 0;
            node.cooldown_until = None;
            return;
        }
        node.failure_count = node.failure_count.saturating_add(1);
        let shift = (node.failure_count - 1).min(4);
        let secs = (30u64 << shift).min(10 * 60);
        node.cooldown_until = Some(Instant::now() + Duration::from_secs(secs));
    }

    fn direct_lease(&self) -> Lease {
        Lease {
            node_id: 0,
            client: Arc::new(ProxyClient::Reqwest(Arc::clone(&self.direct_client))),
            release: None,
        }
    }

    fn client_for(&self, node: &ProxyNode) -> Arc<ProxyClient> {
        match node.scheme {
            // Direct builds no Client: reuse the process-wide direct_client (same
            // redirect-disabled / connect-timeout settings).
            ProxyScheme::Direct => Arc::new(ProxyClient::Reqwest(Arc::clone(&self.direct_client))),
            // reqwest native path: http / socks5 Clients are cached by (node id, config
            // fingerprint)
            ProxyScheme::Http | ProxyScheme::Socks5 => {
                let fingerprint = fingerprint_of(node);
                if let Some(client) = self.client_cache.get(&(node.id, fingerprint)) {
                    return Arc::new(ProxyClient::Reqwest(client));
                }
                let proxy = match crate::node::to_reqwest_proxy(node) {
                    Ok(Some(proxy)) => Some(proxy),
                    Ok(None) => None,
                    Err(_) => {
                        tracing::warn!(
                            node_id = %node.id,
                            "proxy config parse failed, falling back to direct connection"
                        );
                        None
                    }
                };
                let client = Arc::new(build_client(proxy));
                self.client_cache
                    .insert((node.id, fingerprint), Arc::clone(&client));
                Arc::new(ProxyClient::Reqwest(client))
            }
            // Protocol adapter path (SS/Trojan/VLESS/VMess/Hysteria2/AnyTLS/Snell): meow
            // adapter + cache
            ProxyScheme::Shadowsocks
            | ProxyScheme::Trojan
            | ProxyScheme::Vless
            | ProxyScheme::Vmess
            | ProxyScheme::Hysteria2
            | ProxyScheme::AnyTls
            | ProxyScheme::Snell => {
                let fingerprint = fingerprint_of(node);
                if let Some(adapter) = self.connector_cache.get(&(node.id, fingerprint)) {
                    return Arc::new(ProxyClient::Adapter(adapter));
                }
                // When mapping fails (missing credentials / invalid UUID / unknown
                // cipher), adapter_for returns None and has already warned. Fall back to
                // direct instead of 502: a misconfigured node is not worth retrying, so
                // let traffic take the direct path.
                let Some(adapter) = crate::adapter::adapter_for(node) else {
                    return Arc::new(ProxyClient::Reqwest(Arc::clone(&self.direct_client)));
                };
                let adapter: Arc<dyn ProxyAdapter> = adapter;
                self.connector_cache
                    .insert((node.id, fingerprint), Arc::clone(&adapter));
                Arc::new(ProxyClient::Adapter(adapter))
            }
        }
    }

    /// Probe every node in the current snapshot concurrently (deduplicated by id, so a
    /// node bound to several channels is probed once).
    ///
    /// **The caller decides whether to call this**: active probing sends real traffic
    /// to the provider, so it is not triggered automatically from `install` or
    /// `acquire`, and there is no built-in timer.
    ///
    /// Concurrency is fixed at 4: probing is an operational action rather than a
    /// request path. Four concurrent probes are enough to cover dozens of nodes within
    /// seconds without opening dozens of connections to one provider at once (which
    /// itself looks like attack traffic).
    ///
    /// Probing does **not** change node cooldown state: `feedback` cooldowns reflect
    /// real request outcomes, and a failed probe must not take a node that is serving
    /// out of rotation (the probe target and the real upstream can differ). Successful
    /// delays are written into the adapter's own `ProxyHealth` and read out by
    /// [`Self::node_stats`].
    pub async fn probe_all(&self, target: &str, timeout: Duration) -> Vec<ProbeResult> {
        Self::probe_nodes(&self.pool.all_nodes(), target, timeout).await
    }

    /// Probe the nodes bound to `channel_key` (per-channel-target variant).
    ///
    /// Difference from [`Self::probe_all`]: the candidate set is
    /// `pool.candidates(channel_key)` (a single channel bucket has no duplicate ids by
    /// construction, so no id deduplication is needed).
    ///
    /// **Note**: when a node is bound to several channels it is probed once per
    /// channel, and `ProxyHealth` has a single last_delay slot — later probes overwrite
    /// earlier ones (channels are polled in name order, so the result is stable).
    /// Per-channel delays must wait for NodeStats to key by (node, channel), which is
    /// M3-D; do not do it casually. Concurrency and error classification match
    /// `probe_all` (they share the chunked implementation).
    pub async fn probe_channel(
        &self,
        channel_key: &str,
        target: &str,
        timeout: Duration,
    ) -> Vec<ProbeResult> {
        Self::probe_nodes(&self.pool.candidates(channel_key), target, timeout).await
    }

    /// Shared chunked concurrent probe implementation (concurrency 4, see the notes on
    /// [`Self::probe_all`]).
    async fn probe_nodes(
        nodes: &[Arc<ProxyNode>],
        target: &str,
        timeout: Duration,
    ) -> Vec<ProbeResult> {
        const CONCURRENCY: usize = 4;
        let mut results = Vec::with_capacity(nodes.len());
        for chunk in nodes.chunks(CONCURRENCY) {
            let probes = chunk
                .iter()
                .map(|node| crate::probe::probe_node(node, target, timeout));
            results.extend(futures_util::future::join_all(probes).await);
        }
        results
    }

    /// Node runtime status view: exports in-process private state to the admin plane.
    /// `inflight` / `health` are private fields, so the admin console previously had no
    /// runtime information at all. This is the data source for
    /// `GET /api/proxy_nodes/report`.
    ///
    /// `last_delay_ms` comes from the adapter's `ProxyHealth`, so it only has a value
    /// for protocol nodes that have been **assembled before**: nodes never probed, and
    /// http/socks5 nodes going through reqwest, are all `None`. That is accurate — no
    /// measurement means no data, never fake it with 0.
    pub fn node_stats(&self) -> Vec<NodeStats> {
        let now = Instant::now();
        let inflight = lock(&self.inflight);
        let health = lock(&self.health);
        let inflight_map: HashMap<i64, u32> = inflight.clone();
        let health_data: HashMap<i64, (u32, Option<Instant>)> = health
            .iter()
            .map(|(&id, h)| (id, (h.failure_count, h.cooldown_until)))
            .collect();
        drop(inflight);
        drop(health);

        let delay_by_node: HashMap<i64, u16> = self
            .connector_cache
            .iter()
            .filter_map(|((node_id, _fp), adapter)| {
                let delay = adapter.health().last_delay();
                (delay > 0).then_some((node_id, delay))
            })
            .collect();

        self.pool
            .all_nodes()
            .iter()
            .map(|node| {
                let inflight = inflight_map.get(&node.id).copied().unwrap_or(0);
                let (failure_count, cooldown_until) =
                    health_data.get(&node.id).copied().unwrap_or((0, None));
                NodeStats {
                    node_id: node.id,
                    inflight,
                    failure_count,
                    cooldown_remaining_secs: cooldown_until
                        .map_or(0, |until| until.saturating_duration_since(now).as_secs()),
                    last_delay_ms: delay_by_node.get(&node.id).copied(),
                }
            })
            .collect()
    }

    /// Proxy nodes currently cooling down: `(node_id, cooldown deadline)`, containing
    /// only entries with `cooldown_until > now` (expired ones count as healthy and are
    /// not returned); no cooling nodes → empty Vec.
    ///
    /// Purpose: the sole data source for the P1-C dual health-ledger bridge. When a
    /// forward retry-attempt closure falls back to direct (`Lease.node_id == 0`), it
    /// uses this table plus [`Self::channel_node_ids`] to decide that "all proxy nodes
    /// bound to this channel are cooling down after failures" — i.e. direct is a
    /// degraded fallback rather than the expected path — and records the failure as
    /// Retryable on the route unit health (`gateway_forward::stage`; the bridge point
    /// is in forward rather than in dispatch polling, see todo/gateway-resilience.md
    /// P1-C).
    ///
    /// Call timing: read only on the failure path (after an attempt errors), never on
    /// the success hot path. Lock discipline matches [`Self::node_stats`]: hold only
    /// the health lock, read one snapshot, drop it immediately.
    pub fn node_cooldowns(&self) -> Vec<(i64, Instant)> {
        let now = Instant::now();
        let health = lock(&self.health);
        health
            .iter()
            .filter_map(|(&id, h)| {
                h.cooldown_until
                    .filter(|until| *until > now)
                    .map(|until| (id, until))
            })
            .collect()
    }

    /// Proxy node ids bound to `channel_key` (snapshot order, priority descending). No
    /// binding → empty Vec.
    ///
    /// Combined with [`Self::node_cooldowns`], decides "the channel really has nodes
    /// bound and all of them are cooling down" (as opposed to "there was no binding,
    /// so direct is the expected path"). Lock discipline: `pool.candidates` is a
    /// lock-free ArcSwap read and touches no Mutex in this struct.
    pub fn channel_node_ids(&self, channel_key: &str) -> Vec<i64> {
        self.pool
            .candidates(channel_key)
            .iter()
            .map(|n| n.id)
            .collect()
    }
}

// reqwest client build only fails on unrecoverable environment errors;
// callers (`new`/`client_for`) have infallible signatures, so panic is fail-fast.
#[allow(unknown_lints)]
#[allow(expect_in_non_result)]
fn build_client(proxy: Option<reqwest::Proxy>) -> reqwest::Client {
    let mut builder = reqwest::Client::builder()
        .redirect(RedirectPolicy::none())
        .connect_timeout(Duration::from_secs(5))
        .pool_max_idle_per_host(8);
    if let Some(proxy) = proxy {
        builder = builder.proxy(proxy);
    }
    builder.build().expect("reqwest client build")
}

fn fingerprint_of(node: &ProxyNode) -> u64 {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    node.scheme.hash(&mut hasher);
    node.host.hash(&mut hasher);
    node.port.hash(&mut hasher);
    if let Some(auth) = &node.auth {
        auth.user.hash(&mut hasher);
        auth.pass.hash(&mut hasher);
    }
    hasher.finish()
}
