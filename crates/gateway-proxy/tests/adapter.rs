use gateway_proxy::adapter::{adapter_for, tcp_metadata};
use gateway_proxy::node::{BasicAuth, ProxyNode, ProxyScheme};
use meow_common::adapter_type::ConnType;
use meow_common::{AdapterType, Network};

#[test]
fn test_tcp_metadata_host_port() {
    let meta = tcp_metadata("example.com", 8080);
    assert_eq!(meta.host, "example.com");
    assert_eq!(meta.dst_port, 8080);
    assert_eq!(meta.conn_type, ConnType::Http);
    assert_eq!(meta.network, Network::Tcp);
}

/// http/socks5 are handled by reqwest (the workspace `socks` feature) and `manager`
/// produces only `ProxyClient::Reqwest` for them — so `adapter_for` must return `None`.
/// The old implementation built them a meow adapter that was never used (dead code).
#[test]
fn test_adapter_for_reqwest_schemes_have_no_adapter() {
    for scheme in [ProxyScheme::Http, ProxyScheme::Socks5] {
        let node = ProxyNode {
            id: 1,
            scheme,
            host: "proxy.example.com".to_string(),
            port: 1080,
            auth: Some(BasicAuth {
                user: "user".to_string(),
                pass: "pass".to_string(),
            }),
            channel_keys: vec!["test-channel".to_string()],
            opts: None,
            priority: 0,
        };
        assert!(
            adapter_for(&node).is_none(),
            "{scheme:?} must use reqwest, not the meow adapter",
        );
    }
}

#[test]
fn test_adapter_for_vless_invalid_uuid() {
    let node = ProxyNode {
        id: 2,
        scheme: ProxyScheme::Vless,
        host: "proxy.example.com".to_string(),
        port: 443,
        auth: Some(BasicAuth {
            user: "invalid-uuid".to_string(),
            pass: String::new(),
        }),
        channel_keys: vec!["test-channel".to_string()],
        opts: None,
        priority: 0,
    };
    let adapter_opt = adapter_for(&node);
    assert!(adapter_opt.is_none());
}

/// A VLESS node with a valid UUID reaches meow-config's `parse_proxy` and builds a
/// `VlessAdapter` — the happy path. Mirrors the invalid-UUID case: one proves the
/// rejection path, the other the acceptance path.
#[test]
fn test_adapter_for_vless_valid_uuid() {
    let node = ProxyNode {
        id: 4,
        scheme: ProxyScheme::Vless,
        host: "proxy.example.com".to_string(),
        port: 443,
        auth: Some(BasicAuth {
            user: "11111111-2222-3333-4444-555555555555".to_string(),
            pass: String::new(),
        }),
        opts: None,
        channel_keys: vec!["test-channel".to_string()],
        priority: 0,
    };
    let adapter = adapter_for(&node);
    assert!(adapter.is_some(), "valid UUID vless must build an adapter");
    assert_eq!(adapter.unwrap().adapter_type(), AdapterType::Vless);
}

#[test]
fn test_adapter_for_direct() {
    let node = ProxyNode {
        id: 3,
        scheme: ProxyScheme::Direct,
        host: String::new(),
        port: 0,
        auth: None,
        channel_keys: vec!["test-channel".to_string()],
        opts: None,
        priority: 0,
    };
    let adapter_opt = adapter_for(&node);
    assert!(adapter_opt.is_none());
}

/// The trojan password lives in the user field (`trojan://pass@host`), so the clash side
/// must map it to the password key. A regression caught in OCR review: the pass field was
/// once read by mistake, leaving every trojan node with an empty password.
#[test]
fn trojan_password_lands_in_clash_password() {
    let node = ProxyNode::parse_url("trojan://sekret@example.com:443").expect("parse");
    let adapter = adapter_for(&node).expect("trojan node must be able to build an adapter");
    // Successful adapter construction = meow-config received a non-empty password
    // (otherwise parse_proxy reports "missing password", returns Err → adapter_for is None)
    assert_eq!(
        adapter.name(),
        "node-0-trojan",
        "parse_url yields node id=0, name derived from id",
    );
}
