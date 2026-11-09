//! `probe` — active probing of node liveness and latency
//!
//! Latency history reuses meow's built-in [`ProxyHealth`] (`ProxyAdapter::health()`): it
//! already provides `record_delay(u16)` / `last_delay()` / `delay_history()` (rolling 10
//! entries) / `alive()`, and `record_delay` also sets `alive` to `delay > 0`. Keeping a
//! second copy would mean two sources of truth.
//!
//! **TCP dial only, no HTTP request**: a completed TCP handshake is enough to prove
//! liveness, whereas an HTTP request would hit the upstream channel, burn quota, and
//! pollute usage statistics.
//!
//! Only protocol nodes (the seven kinds that go through the meow adapter) can be probed.
//! `Direct` / `Http` / `Socks5` use reqwest, have no `ProxyAdapter`, and therefore no
//! `health()` to write back to — those nodes return `error = Some(...)` instead of
//! pretending to succeed.

use std::time::{Duration, Instant};

use gateway_core::proxy::node::ProxyNode;

/// Result of probing a single node.
#[derive(Debug, Clone)]
pub struct ProbeResult {
    /// Id of the probed node (matches `ProxyNode::id`).
    pub node_id: i64,
    /// Dial round-trip in milliseconds; `None` when the probe failed.
    ///
    /// The upper bound is `u16::MAX` (about 65 seconds) — meow's
    /// `ProxyHealth::record_delay` takes a `u16`. Longer durations are clamped: for a node
    /// that connects but is as slow as 65s, recording 65535 instead of the true value makes
    /// no difference to the decision — both mean "time to switch nodes".
    pub delay_ms: Option<u16>,
    /// Failure reason (unsupported probe / adapter construction failure / dial failure /
    /// timeout); `None` on success.
    pub error: Option<String>,
}

impl ProbeResult {
    /// Whether the probe succeeded (dialed and obtained a latency).
    pub fn is_alive(&self) -> bool {
        self.delay_ms.is_some()
    }
}

/// Probe a single node: build the adapter → time `dial_tcp` → write back through
/// `health().record_delay()`.
///
/// # Parameters
/// - `target`: probe target in `host:port` form (e.g. `api.openai.com:443`). Using the
///   channel's own upstream host best reflects real availability — the same node can have
///   completely different connectivity to different upstreams.
/// - `timeout`: dial timeout ceiling. A timeout counts as a failure and does **not** write
///   `record_delay` (writing 0 would make `ProxyHealth` mark the node dead, and "this dial
///   timed out" is not "the node is dead"; liveness judgement is left to the caller based
///   on consecutive failures).
///
/// # Returns
/// Never panics: adapter construction failure, unsupported protocol, dial failure, and
/// timeout all land in the `error` field. Probing is an ops action, so one bad node must not
/// abort the whole round.
pub async fn probe_node(node: &ProxyNode, target: &str, timeout: Duration) -> ProbeResult {
    let fail = |msg: String| ProbeResult {
        node_id: node.id,
        delay_ms: None,
        error: Some(msg),
    };

    let Some((host, port)) = split_host_port(target) else {
        return fail(format!("probe target `{target}` is not in host:port form"));
    };

    // Direct/Http/Socks5 have no ProxyAdapter (they use reqwest), so there is no health()
    // to write back to.
    let Some(adapter) = crate::adapter::adapter_for(node) else {
        return fail(format!(
            "{:?} node does not use meow adapter, cannot probe with dial_tcp",
            node.scheme
        ));
    };

    let metadata = crate::adapter::tcp_metadata(host, port);
    let started = Instant::now();
    let dialed = tokio::time::timeout(timeout, adapter.dial_tcp(&metadata)).await;

    match dialed {
        Err(_elapsed) => fail(format!("dial timed out ({} ms)", timeout.as_millis())),
        Ok(Err(e)) => fail(format!("dial failed: {e}")),
        Ok(Ok(_conn)) => {
            // Drop the connection immediately: only the handshake matters, no bytes are sent.
            let elapsed = started.elapsed().as_millis();
            // 0 means dead in ProxyHealth; a sub-millisecond but successful dial is 1.
            let delay = elapsed.clamp(1, u128::from(u16::MAX)) as u16;
            adapter.health().record_delay(delay);
            ProbeResult {
                node_id: node.id,
                delay_ms: Some(delay),
                error: None,
            }
        }
    }
}

/// Split `host:port`, tolerating IPv6 literals (`[::1]:443`).
///
/// Split on the last `:` from the right: an IPv6 address contains colons itself, so
/// splitting from the left would cut the address apart.
fn split_host_port(target: &str) -> Option<(&str, u16)> {
    let (host, port) = target.rsplit_once(':')?;
    if host.starts_with('[') != host.ends_with(']') {
        return None;
    }
    let host = host.strip_prefix('[').unwrap_or(host);
    let host = host.strip_suffix(']').unwrap_or(host);
    if host.is_empty() {
        return None;
    }
    Some((host, port.parse().ok()?))
}
