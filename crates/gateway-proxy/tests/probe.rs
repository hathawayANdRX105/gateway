//! Behavior tests for `probe` and `node_stats`.
//!
//! A successful dial needs a real proxy server and cannot be produced locally (the
//! protocol handshake cannot be simulated with a `TcpListener`), so following the
//! convention in `forward/tests/protocol_live.rs` these tests are env-gated: a real dial
//! runs only when `FERRITE_PROXY_PROBE_URL` is set, otherwise it is skipped. Everything
//! else exercises real paths that can be verified deterministically.

use std::time::Duration;

use gateway_proxy::node::{BasicAuth, ProxyNode, ProxyScheme};
use gateway_proxy::pool::ProxySnapshot;
use gateway_proxy::probe::probe_node;
use gateway_proxy::{ProxyManager, node};

/// Build an ss node that meow-config can assemble successfully, pointing at the given
/// `host:port`.
///
/// For ss, `auth.user` is the cipher and `auth.pass` is the password (see
/// `adapter.rs::clash_config`).
fn ss_node(id: i64, host: &str, port: u16) -> ProxyNode {
    ProxyNode {
        id,
        scheme: ProxyScheme::Shadowsocks,
        host: host.into(),
        port,
        auth: Some(BasicAuth {
            user: "aes-128-gcm".into(),
            pass: "probe-test-pass".into(),
        }),
        opts: None,
        channel_keys: vec!["ch".into()],
        priority: 0,
    }
}

/// Schemes routed through reqwest have no `ProxyAdapter`, so there is no `health()` to
/// write a delay back to.
///
/// The expected result is `error` rather than a pretended success: returning
/// `delay_ms: Some(0)` here would make the admin UI show a never-probed node as the
/// "fastest node at 0ms latency".
#[tokio::test]
async fn probe_rejects_non_adapter_scheme() {
    let mut n = ss_node(1, "127.0.0.1", 1080);
    n.scheme = ProxyScheme::Socks5;
    let r = probe_node(&n, "127.0.0.1:9", Duration::from_millis(50)).await;
    assert_eq!(r.delay_ms, None);
    assert!(!r.is_alive());
    assert!(
        r.error.as_deref().unwrap_or_default().contains("adapter"),
        "error should explain that this scheme does not use an adapter, got: {:?}",
        r.error
    );
}

/// A node that fails to assemble (cipher unknown to meow-config) returns error without
/// panicking.
///
/// `adapter_for` warns about the invalid config and then returns None; probing must
/// report that as a failure instead of crashing the whole probe round.
#[tokio::test]
async fn probe_reports_assembly_failure() {
    let mut n = ss_node(2, "127.0.0.1", 8388);
    n.auth = Some(BasicAuth {
        user: "not-a-real-cipher".into(),
        pass: "x".into(),
    });
    let r = probe_node(&n, "127.0.0.1:9", Duration::from_millis(50)).await;
    assert_eq!(r.delay_ms, None);
    assert!(r.error.is_some());
}

/// An invalid probe target (missing port) is rejected before dialing and consumes no
/// timeout.
#[tokio::test]
async fn probe_rejects_malformed_target() {
    let n = ss_node(3, "127.0.0.1", 8388);
    let r = probe_node(&n, "no-port-here", Duration::from_secs(30)).await;
    assert_eq!(r.delay_ms, None);
    assert!(
        r.error.as_deref().unwrap_or_default().contains("host:port"),
        "should indicate a target format problem, got: {:?}",
        r.error
    );
}

/// Targets with unbalanced brackets are rejected before dialing: `[::1:443` (missing
/// closing bracket) and `]:443` (missing opening bracket).
///
/// `split_host_port` finds the last `:` from the right and then strips brackets; when
/// they are unbalanced it would shred the address or strip an empty host. The pairing
/// check routes such targets into the "host:port form" error path instead of producing a
/// bad connection.
#[tokio::test]
async fn probe_rejects_unbalanced_brackets() {
    let node = ss_node(4, "example.com", 80);
    for target in ["[::1:443", "]:443"] {
        let r = probe_node(&node, target, Duration::from_millis(50)).await;
        assert!(
            r.error.as_deref().unwrap_or_default().contains("host:port"),
            "`{target}` should report a target format error, got: {:?}",
            r.error
        );
    }
}

/// An unreachable address is classified as a failure within the timeout.
///
/// The target uses TEST-NET-1 (192.0.2.0/24, reserved for documentation by RFC 5737 and
/// not routable on the public internet) so the connection cannot accidentally succeed; a
/// 1ms timeout keeps the case fast.
#[tokio::test]
async fn probe_unreachable_node_fails_within_timeout() {
    let n = ss_node(5, "192.0.2.1", 8388);
    let r = probe_node(&n, "192.0.2.2:443", Duration::from_millis(1)).await;
    assert_eq!(r.delay_ms, None);
    assert!(r.error.is_some());
}

/// `node_stats` must reflect the inflight created by `acquire` and the cooldown created
/// by `feedback`.
///
/// Both fields used to be completely unobservable (`inflight` / `health` are private), so
/// the admin UI could not see any runtime state. The case goes through the public API:
/// install → acquire (hold the Lease to keep inflight) → feedback(502) → read stats.
#[tokio::test]
async fn node_stats_reports_inflight_and_cooldown() {
    let mgr = ProxyManager::new();
    mgr.install(ProxySnapshot {
        nodes: vec![ss_node(7, "192.0.2.1", 8388)],
    });

    // Idle: no inflight, no cooldown, no recorded delay.
    let idle = mgr.node_stats();
    assert_eq!(idle.len(), 1);
    assert_eq!(idle[0].node_id, 7);
    assert_eq!(idle[0].inflight, 0);
    assert_eq!(idle[0].cooldown_remaining_secs, 0);
    assert_eq!(idle[0].last_delay_ms, None);

    // While the Lease is held inflight must be 1; it drops back to zero afterwards.
    {
        let lease = mgr.acquire("ch");
        assert_eq!(
            lease.node_id, 7,
            "the only available node should be selected"
        );
        let busy = mgr.node_stats();
        assert_eq!(busy[0].inflight, 1);
    }
    assert_eq!(
        mgr.node_stats()[0].inflight,
        0,
        "inflight should be released after Lease drop"
    );

    // A single 502 triggers the first cooldown round (exponential backoff starting at 30s).
    mgr.feedback(7, 502, false);
    let cooled = mgr.node_stats();
    assert_eq!(cooled[0].failure_count, 1);
    assert!(
        cooled[0].cooldown_remaining_secs > 0,
        "should enter cooldown after 502, remaining {}s",
        cooled[0].cooldown_remaining_secs
    );

    // A cooling node must no longer be selected; acquire falls back to direct (node_id = 0).
    assert_eq!(mgr.acquire("ch").node_id, 0);
}

/// When a node is bound to several channels, `node_stats` lists it once (deduplicated by
/// id).
///
/// The index is `channel_key` → node list, so binding three channels puts the node in three
/// buckets; `all_nodes` must deduplicate them to 1, otherwise the admin UI shows three
/// rows for the same node and probing dials it three times. Note that two installs are a
/// "whole-snapshot replacement", so they cannot test multi-channel bucket deduplication —
/// this needs a single install.
#[tokio::test]
async fn node_stats_dedupes_multi_channel_node() {
    let mgr = ProxyManager::new();
    let mut node = ss_node(9, "192.0.2.1", 8388);
    node.channel_keys = vec!["a".into(), "b".into(), "c".into()];
    mgr.install(ProxySnapshot { nodes: vec![node] });
    let stats = mgr.node_stats();
    assert_eq!(stats.len(), 1);
    assert_eq!(stats[0].node_id, 9);
}

/// `probe_channel` probes only the nodes bound to the given channel, and the probe target
/// is the supplied target.
///
/// Two nodes are bound to channels a / b: probing a returns only node a. The target
/// `[::1:1]` (unbalanced brackets) is classified as malformed before dialing, and the
/// error carries the supplied target verbatim — proving the target is the `probe_channel`
/// argument rather than the node's own host. The case has zero external-network
/// dependency and finishes in milliseconds.
#[tokio::test]
async fn probe_channel_scopes_candidates_and_uses_given_target() {
    let mgr = ProxyManager::new();
    let mut a = ss_node(11, "192.0.2.1", 8388);
    a.channel_keys = vec!["a".into()];
    let mut b = ss_node(12, "192.0.2.2", 8388);
    b.channel_keys = vec!["b".into()];
    mgr.install(ProxySnapshot { nodes: vec![a, b] });

    let results = mgr
        .probe_channel("a", "[::1:1]", Duration::from_secs(5))
        .await;
    assert_eq!(
        results.len(),
        1,
        "should include only the node bound to channel a"
    );
    assert_eq!(results[0].node_id, 11);
    assert!(!results[0].is_alive());
    assert!(
        results[0]
            .error
            .as_deref()
            .unwrap_or_default()
            .contains("`[::1:1]`"),
        "error should point to the supplied probe target rather than the node host, got: {:?}",
        results[0].error
    );

    // A channel with no nodes returns empty without an error.
    assert!(
        mgr.probe_channel("c", "[::1:1]", Duration::from_secs(5))
            .await
            .is_empty()
    );
}

/// Live-node dial verification: runs only when `FERRITE_PROXY_PROBE_URL` is set.
///
/// The success path must reach a real proxy server (the protocol handshake cannot be
/// simulated locally), hence the env gate. Same convention as
/// `forward/tests/protocol_live.rs`.
#[tokio::test]
async fn probe_live_node_records_delay() {
    let Ok(url) = std::env::var("FERRITE_PROXY_PROBE_URL") else {
        eprintln!("skip: FERRITE_PROXY_PROBE_URL not set");
        return;
    };
    let target =
        std::env::var("FERRITE_PROXY_PROBE_TARGET").unwrap_or_else(|_| "example.com:443".into());
    // Env content is untrusted: malformed values are skipped gracefully (same convention
    // as protocol_live.rs), without panicking.
    let mut n = match node::ProxyNode::parse_url(&url) {
        Ok(n) => n,
        Err(e) => {
            eprintln!("FERRITE_PROXY_PROBE_URL invalid format: {e}. Skipping");
            return;
        }
    };
    n.id = 1;
    n.channel_keys = vec!["ch".into()];
    let r = probe_node(&n, &target, Duration::from_secs(10)).await;
    assert!(
        r.is_alive(),
        "real node should dial {target}, failure reason: {:?}",
        r.error
    );
    assert!(
        r.delay_ms.unwrap() > 0,
        "delay should be greater than 0 (0 means dead in ProxyHealth)"
    );
}
