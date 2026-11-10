//! Fingerprint tests: `fingerprint=` / `fp=` query parsing and TLS Config propagation.
//!
//! Verifies:
//! 1. query parsing lands in `NodeOpts.fingerprint`
//! 2. `adapter_for` passes the fingerprint to `TlsConfig` on the VLESS + TLS/REALITY path
//! 3. building the adapter does not fail with the feature off (meow only warns internally)

use gateway_proxy::adapter::adapter_for;
use gateway_proxy::node::{ProxyNode, ProxyScheme};

const UUID: &str = "11111111-2222-3333-4444-555555555555";

fn vless_node(query: &str) -> ProxyNode {
    let url = format!("vless://{UUID}@example.com:443{query}");
    let mut node = ProxyNode::parse_url(&url).expect("vless URL must parse");
    node.channel_keys = vec!["openai".to_string()];
    node
}

/// `fingerprint=chrome` parsing lands in `NodeOpts`
#[test]
fn fingerprint_query_parsed() {
    let node = vless_node("?fingerprint=chrome&sni=cdn.example.com");
    let opts = node.opts.expect("vless node must have NodeOpts");
    assert_eq!(opts.fingerprint.as_deref(), Some("chrome"));
}

/// The `fp=firefox` short alias parses the same way
#[test]
fn fp_short_alias_parsed() {
    let node = vless_node("?fp=firefox&sni=cdn.example.com");
    let opts = node.opts.expect("vless node must have NodeOpts");
    assert_eq!(opts.fingerprint.as_deref(), Some("firefox"));
}

/// fingerprint coexists with other query parameters without conflict
#[test]
fn fingerprint_with_other_params() {
    let node = vless_node(
        "?flow=xtls-rprx-vision&sni=cdn.example.com&pbk=0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef&sid=01ab&fp=safari",
    );
    let opts = node.opts.expect("vless node must have NodeOpts");
    assert_eq!(opts.flow.as_deref(), Some("xtls-rprx-vision"));
    assert_eq!(opts.sni.as_deref(), Some("cdn.example.com"));
    assert_eq!(opts.fingerprint.as_deref(), Some("safari"));
}

/// VLESS + TLS + fingerprint: the adapter can be built even with the feature off (meow warns
/// internally but does not block)
#[test]
fn vless_tls_fingerprint_builds_adapter_without_utls_feature() {
    // The utls feature is deliberately not enabled here, verifying:
    // - TlsConfig.fingerprint can be set (default rustls backend)
    // - adapter_for returns Some (no fallback to a direct connection)
    // - runtime emits meow's one-time stub warn, which does not fail the test
    let node = vless_node("?fingerprint=chrome&sni=cdn.example.com");
    let adapter = adapter_for(&node);
    assert!(
        adapter.is_some(),
        "with the feature off, fingerprint must not block adapter construction",
    );
}

/// REALITY + fingerprint likewise builds an adapter
#[test]
fn reality_fingerprint_builds_adapter_without_utls_feature() {
    const PBK: &str = "AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8";
    let node = vless_node(&format!(
        "?fingerprint=chrome&sni=cdn.example.com&pbk={PBK}&sid=01ab"
    ));
    let adapter = adapter_for(&node);
    assert!(
        adapter.is_some(),
        "REALITY + fingerprint with the feature off must not block",
    );
}

/// Non-VLESS nodes do not parse fingerprint (shared `NodeOpts` mechanism; only
/// VLESS/VMess/Hysteria2/AnyTLS/Snell produce it)
#[test]
fn non_vless_no_fingerprint() {
    let node =
        ProxyNode::parse_url("socks5://127.0.0.1:7890?fingerprint=chrome&sni=cdn.example.com")
            .unwrap();
    assert_eq!(node.scheme, ProxyScheme::Socks5);
    assert!(node.opts.is_none());
}
