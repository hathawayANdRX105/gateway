//! `extra_schemes.rs` —— Hysteria2, `AnyTLS`, Snell parse & `adapter_for` tests (no network).
//!
//! Tests `parse_url` for query params, default ports, and `adapter_for` happy/bad paths.
//! Config error = warn + None (fallback direct), no panic. Matches contract.
//! Uses env-like nodes but no real dial (`adapter_for` only).

use gateway_proxy::adapter::adapter_for;
use gateway_proxy::node::{NodeOpts, ProxyNode, ProxyScheme};

#[test]
fn test_parse_hysteria2() {
    let url = "hysteria2://password@example.com:443?sni=cdn.example.com&insecure=1";
    let node = ProxyNode::parse_url(url).expect("hysteria2 URL must parse");
    assert_eq!(node.scheme, ProxyScheme::Hysteria2);
    assert_eq!(node.host, "example.com");
    assert_eq!(node.port, 443);
    assert!(node.auth.is_some());
    let opts = node.opts.expect("hysteria2 must populate opts");
    assert_eq!(opts.sni.as_deref(), Some("cdn.example.com"));
    assert!(opts.insecure);
}

#[test]
fn test_parse_anytls() {
    let url = "anytls://password@1.2.3.4:8443?sni=example.com&insecure=0";
    let node = ProxyNode::parse_url(url).expect("anytls URL must parse");
    assert_eq!(node.scheme, ProxyScheme::AnyTls);
    assert_eq!(node.port, 8443);
    let opts = node.opts.expect("anytls must populate opts");
    assert_eq!(opts.sni.as_deref(), Some("example.com"));
    assert!(!opts.insecure);
}

#[test]
fn test_parse_snell() {
    let url = "snell://psk@server.com:443?version=v5&obfs=http&obfs-uri=/obfs";
    let node = ProxyNode::parse_url(url).expect("snell URL must parse");
    assert_eq!(node.scheme, ProxyScheme::Snell);
    assert_eq!(node.port, 443);
    let opts = node.opts.expect("snell must populate opts");
    assert_eq!(opts.version.as_deref(), Some("v5"));
    assert_eq!(opts.obfs.as_deref(), Some("http"));
    assert_eq!(opts.obfs_uri.as_deref(), Some("/obfs"));
}

#[test]
fn test_default_ports() {
    let cases = vec![
        ("hysteria2://p@ex.com", 443),
        ("anytls://p@ex.com", 443),
        ("snell://p@ex.com", 443),
    ];
    for (url, expected) in cases {
        let node = ProxyNode::parse_url(url).unwrap();
        assert_eq!(node.port, expected, "default port for {url}");
    }
}

/// Hysteria2 happy path: valid auth + default opts must build an adapter.
/// Hy2 runs over QUIC; meow owns the UDP socket inside `dial_tcp`, so the TCP egress path
/// stays usable.
#[test]
fn test_adapter_for_hysteria2_happy_path() {
    let node_h2 = ProxyNode {
        id: 10,
        scheme: ProxyScheme::Hysteria2,
        host: "example.com".to_string(),
        port: 443,
        auth: Some(gateway_proxy::node::BasicAuth {
            user: "unused".to_string(),
            pass: "validpass".to_string(),
        }),
        opts: Some(NodeOpts::default()),
        channel_keys: vec!["test".to_string()],
        priority: 0,
    };
    let adapter = adapter_for(&node_h2);
    assert!(
        adapter.is_some(),
        "valid hysteria2 node must build an adapter (dial guaranteed by meow)",
    );

    assert_eq!(
        adapter.unwrap().adapter_type(),
        meow_common::AdapterType::Hysteria2
    );
}

#[test]
fn test_adapter_for_bad_params() {
    let node_bad = ProxyNode {
        id: 11,
        scheme: ProxyScheme::Snell,
        host: "example.com".to_string(),
        port: 443,
        auth: None, // missing auth -> fallback
        opts: None,
        channel_keys: vec!["test".to_string()],
        priority: 0,
    };
    assert!(
        adapter_for(&node_bad).is_none(),
        "missing auth must fallback with warn"
    );
}

#[test]
fn test_parse_error_unsupported() {
    assert!(ProxyNode::parse_url("unknown://host:80").is_err());
}
