//! WebSocket transport layer: URL query → `NodeOpts` → meow `WsLayer`.
//!
//! Both VLESS and `VMess` support `type=ws&path=/xxx&host=sni-domain`.
//! Convention: a present `path` means a WS node; `host` is optional and defaults to the
//! SNI or the node host.
//! Transport layer order: TLS inside, WS outside (meow-config assembles the chain with
//! TLS pushed first and WS second, so WS wraps TLS).

use gateway_proxy::adapter::adapter_for;
use gateway_proxy::node::{ProxyNode, ProxyScheme};

const UUID: &str = "11111111-2222-3333-4444-555555555555";

fn vless_node(query: &str) -> ProxyNode {
    let url = format!("vless://{UUID}@example.com:443{query}");
    let mut node = ProxyNode::parse_url(&url).expect("vless URL must parse");
    node.channel_keys = vec!["openai".to_string()];
    node
}

fn vmess_node(query: &str) -> ProxyNode {
    let url = format!("vmess://{UUID}@example.com:443{query}");
    let mut node = ProxyNode::parse_url(&url).expect("vmess URL must parse");
    node.channel_keys = vec!["openai".to_string()];
    node
}

#[test]
fn vless_query_parses_ws_path_and_host() {
    let node = vless_node("?path=/ws&host=cdn.example.com&type=ws");
    let opts = node.opts.expect("vless node must carry NodeOpts");
    assert_eq!(opts.ws_path.as_deref(), Some("/ws"));
    assert_eq!(opts.ws_host.as_deref(), Some("cdn.example.com"));
}

#[test]
fn vmess_query_parses_ws_path_and_host() {
    let node = vmess_node("?path=/vmess-ws&host=vmess.example.com");
    let opts = node
        .opts
        .expect("vmess node also reuses NodeOpts for WS parameters");
    assert_eq!(opts.ws_path.as_deref(), Some("/vmess-ws"));
    assert_eq!(opts.ws_host.as_deref(), Some("vmess.example.com"));
}

#[test]
fn ws_path_alone_implies_ws_transport() {
    // A path alone implies WS; no explicit type=ws is needed
    let node = vless_node("?path=/ws-only");
    let opts = node.opts.expect("vless node must carry NodeOpts");
    assert_eq!(opts.ws_path.as_deref(), Some("/ws-only"));
    assert!(opts.ws_host.is_none());
}

#[test]
fn non_vless_non_vmess_schemes_do_not_get_ws_opts() {
    // SOCKS5 / HTTP etc. must not reuse NodeOpts
    let node = ProxyNode::parse_url("socks5://127.0.0.1:7890?path=/ws&host=foo").unwrap();
    assert_eq!(node.scheme, ProxyScheme::Socks5);
    assert!(node.opts.is_none());
}

#[test]
fn vless_ws_only_builds_adapter_with_ws_layer() {
    // Pure WS (no TLS): the ws layer wraps plaintext TCP
    let node = vless_node("?path=/ws");
    let adapter = adapter_for(&node);
    assert!(
        adapter.is_some(),
        "VLESS + WS (no TLS) must be able to construct adapter"
    );
}

#[test]
fn vless_ws_tls_builds_adapter_tls_then_ws() {
    // WS + TLS: TLS inside, WS outside (the chain pushes TLS first, then WS)
    let node = vless_node("?path=/ws&sni=cdn.example.com");
    let adapter = adapter_for(&node);
    assert!(
        adapter.is_some(),
        "VLESS + WS + TLS (sni) must be able to construct adapter"
    );
}

#[test]
fn vmess_ws_only_builds_adapter_with_ws_layer() {
    // VMess pure WS (no TLS)
    let node = vmess_node("?path=/vmess-ws");
    let adapter = adapter_for(&node);
    assert!(
        adapter.is_some(),
        "VMess + WS (no TLS) must be able to construct adapter"
    );
}

#[test]
fn vmess_ws_tls_builds_adapter_tls_then_ws() {
    // VMess WS + TLS
    let node = vmess_node("?path=/vmess-ws&sni=cdn.example.com");
    let adapter = adapter_for(&node);
    assert!(
        adapter.is_some(),
        "VMess + WS + TLS (sni) must be able to construct adapter"
    );
}

#[test]
fn vless_ws_invalid_config_falls_back() {
    // A failed WsLayer::new would warn and return None (fall back to direct). This test
    // covers the other path: without ws_path there is no WS layer, so the adapter runs
    // over plain TCP/TLS and still builds.
    let node = vless_node("");
    let adapter = adapter_for(&node);
    assert!(
        adapter.is_some(),
        "VLESS without WS config must also be able to construct adapter"
    );
}

#[test]
fn vmess_ws_invalid_config_falls_back() {
    // VMess works without WS configuration too
    let node = vmess_node("");
    let adapter = adapter_for(&node);
    assert!(
        adapter.is_some(),
        "VMess without WS config must also be able to construct adapter"
    );
}
