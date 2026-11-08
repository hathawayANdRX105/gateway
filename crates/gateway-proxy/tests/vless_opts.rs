//! VLESS transport-layer options: URL query → [`NodeOpts`] → meow adapter.
//!
//! Vision flow and REALITY parameters are read from the `vless://` query. A bad
//! configuration must warn + fall back to direct (return `None`); it must never panic
//! the gateway — node URLs come from user configuration.

use gateway_proxy::adapter::adapter_for;
use gateway_proxy::node::{ProxyNode, ProxyScheme};

/// A valid REALITY public key: 64 hex characters (any content, as long as the length is
/// right).
const PBK: &str = "AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8";
const UUID: &str = "11111111-2222-3333-4444-555555555555";

fn vless_node(query: &str) -> ProxyNode {
    let url = format!("vless://{UUID}@example.com:443{query}");
    let mut node = ProxyNode::parse_url(&url).expect("vless URL must parse");
    node.channel_keys = vec!["openai".to_string()];
    node
}

/// The query's flow / sni / pbk / sid must land in `NodeOpts`.
#[test]
fn query_fills_vless_opts() {
    let node = vless_node(&format!(
        "?flow=xtls-rprx-vision&sni=cdn.example.com&pbk={PBK}&sid=01ab"
    ));
    let opts = node.opts.expect("vless node must carry NodeOpts");
    assert_eq!(opts.flow.as_deref(), Some("xtls-rprx-vision"));
    assert_eq!(opts.sni.as_deref(), Some("cdn.example.com"));
    assert_eq!(opts.pbk.as_deref(), Some(PBK));
    assert_eq!(opts.sid.as_deref(), Some("01ab"));
}

/// Non-VLESS nodes must not carry `NodeOpts` — that keeps other schemes from misreading
/// the query.
#[test]
fn non_vless_has_no_opts() {
    let node = ProxyNode::parse_url("socks5://127.0.0.1:7890?flow=xtls-rprx-vision").unwrap();
    assert_eq!(node.scheme, ProxyScheme::Socks5);
    assert!(node.opts.is_none());
}

/// A full Vision + REALITY node must build an adapter (both the TLS layer and flow
/// wired up).
#[test]
fn vision_reality_builds_adapter() {
    let node = vless_node(&format!(
        "?flow=xtls-rprx-vision&sni=cdn.example.com&pbk={PBK}&sid=01"
    ));
    assert!(
        adapter_for(&node).is_some(),
        "Vision + REALITY node must be able to construct meow adapter"
    );
}

/// Plain VLESS (no sni/pbk) must build too: no TLS layer attached, plaintext TCP egress.
#[test]
fn plain_vless_builds_adapter() {
    assert!(adapter_for(&vless_node("")).is_some());
}

/// A wrong pbk length → warn + fall back to direct, never panic.
#[test]
fn short_pbk_falls_back_to_direct() {
    let node = vless_node("?pbk=dead&sni=cdn.example.com");
    assert!(
        adapter_for(&node).is_none(),
        "illegal pbk must fall back to direct instead of building a wrong REALITY layer"
    );
}

/// sid over 8 bytes (16 hex) → likewise falls back to direct.
#[test]
fn oversized_sid_falls_back_to_direct() {
    let node = vless_node(&format!("?pbk={PBK}&sid=00112233445566778899"));
    assert!(
        adapter_for(&node).is_none(),
        "sid too long must fall back to direct"
    );
}

/// A deprecated/unknown flow must fall back to direct and **not** silently degrade to
/// non-Vision.
///
/// The old implementation warned and then used the node as plain VLESS — users believed
/// Vision protection was in place when it was not. meow-config hard-rejects these per
/// ADR-0002 Class A (`xtls-rprx-direct` / `xtls-rprx-splice` are unsafe flows that xray
/// has deprecated; unknown values may skip intended security handling).
#[test]
fn deprecated_or_unknown_flow_falls_back_to_direct() {
    for flow in ["xtls-rprx-direct", "xtls-rprx-splice", "not-a-flow"] {
        let node = vless_node(&format!("?flow={flow}"));
        assert!(
            adapter_for(&node).is_none(),
            "flow={flow} must fall back to direct, not silently treated as non-Vision"
        );
    }
}
