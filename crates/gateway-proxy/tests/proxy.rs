//! proxy test suite
//!
//! Design principle: each test verifies one explicit behavioral contract, and the
//! comments explain:
//! - which behavior is tested
//! - why this expectation holds
//! - which production stage it corresponds to

use gateway_proxy::manager::ProxyManager;
use gateway_proxy::node::{BasicAuth, ProxyNode, ProxyScheme, to_reqwest_proxy};
use gateway_proxy::pool::{ProxyPool, ProxySnapshot};
use gateway_proxy::ssrf::{SsrfError, check_ip, validate_resolved, validate_url};
use rand::SeedableRng;
use rand::rngs::StdRng;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};
use std::str::FromStr;
use url::Url;
/// `parse_url`: correct scheme mapping + default ports + auth extraction + error on an
/// illegal scheme
#[test]
fn parse_url_maps_schemes_and_defaults() {
    // http -> Http, default 8080
    let node = ProxyNode::parse_url("http://proxy.example.com").unwrap();
    assert_eq!(node.scheme, ProxyScheme::Http);
    assert_eq!(node.host, "proxy.example.com");
    assert_eq!(node.port, 8080);
    assert!(node.auth.is_none());
    // https -> Http (both are HTTP proxy tunnels), default 8080
    let node = ProxyNode::parse_url("https://proxy.example.com").unwrap();
    assert_eq!(node.scheme, ProxyScheme::Http);
    assert_eq!(node.port, 8080);
    // socks5 -> Socks5, default 1080
    let node = ProxyNode::parse_url("socks5://proxy.example.com").unwrap();
    assert_eq!(node.scheme, ProxyScheme::Socks5);
    assert_eq!(node.port, 1080);
    // socks5h -> Socks5 (same as socks5; h means remote DNS resolution)
    let node = ProxyNode::parse_url("socks5h://proxy.example.com").unwrap();
    assert_eq!(node.scheme, ProxyScheme::Socks5);
    assert_eq!(node.port, 1080);
    // explicit port takes precedence over the default
    let node = ProxyNode::parse_url("http://proxy.example.com:3128").unwrap();
    assert_eq!(node.port, 3128);
    // auth extraction: user:pass
    let node = ProxyNode::parse_url("http://user:pass@proxy.example.com:3128").unwrap();
    assert_eq!(node.auth.as_ref().unwrap().user, "user");
    assert_eq!(node.auth.as_ref().unwrap().pass, "pass");
    // auth: user only, no pass -> pass = ""
    let node = ProxyNode::parse_url("http://user@proxy.example.com:3128").unwrap();
    assert_eq!(node.auth.as_ref().unwrap().user, "user");
    assert_eq!(node.auth.as_ref().unwrap().pass, "");
    // Special characters: `Url::username()` returns the percent-encoded original, and
    // parse_url decodes it. Without decoding, a username containing `@` would become
    // `user%40domain` and proxy authentication would fail.
    let node = ProxyNode::parse_url("http://user%40domain:p%2Fss@proxy.example.com").unwrap();
    assert_eq!(node.auth.as_ref().unwrap().user, "user@domain");
    assert_eq!(node.auth.as_ref().unwrap().pass, "p/ss");
    // illegal scheme → unsupported proxy scheme (new protocol variants are valid schemes and
    // have their own tests)
    let err = ProxyNode::parse_url("ftp://proxy.example.com").unwrap_err();
    assert!(
        err.to_string().contains("unsupported proxy scheme"),
        "got: {err}"
    );
    // missing host: the url crate rejects an empty host at parse time; only assert the error,
    // not the wording.
    assert!(ProxyNode::parse_url("http://:3128").is_err());
    assert!(ProxyNode::parse_url("not-a-url").is_err());
}

/// `to_reqwest_proxy`: Direct returns None; the rest return Proxy + auth via basic_auth
#[test]
fn to_reqwest_proxy_returns_correct_variant() {
    // Direct -> None
    let node = ProxyNode {
        id: 1,
        scheme: ProxyScheme::Direct,
        host: "".into(),
        port: 0,
        auth: None,
        channel_keys: vec![],
        opts: None,
        priority: 0,
    };
    assert!(to_reqwest_proxy(&node).unwrap().is_none());

    // Http
    let node = ProxyNode {
        id: 1,
        scheme: ProxyScheme::Http,
        host: "proxy.example.com".into(),
        port: 3128,
        auth: Some(BasicAuth {
            user: "u".into(),
            pass: "p".into(),
        }),
        channel_keys: vec![],
        opts: None,
        priority: 0,
    };
    let proxy = to_reqwest_proxy(&node).unwrap().unwrap();
    // The internal URL cannot be inspected directly; we can only confirm construction
    // succeeded. Actual dialing is handled by forward
    drop(proxy);

    // Socks5
    let node = ProxyNode {
        id: 1,
        scheme: ProxyScheme::Socks5,
        host: "proxy.example.com".into(),
        port: 1080,
        auth: None,
        channel_keys: vec![],
        opts: None,
        priority: 0,
    };
    let proxy = to_reqwest_proxy(&node).unwrap().unwrap();
    drop(proxy);
}

/// `pick`: priority layering — only the highest tier is sampled; an empty channel returns
/// None; install replaces wholesale
#[test]
fn pick_respects_priority_layers_and_replaces_on_install() {
    let pool = ProxyPool::new();
    let mut rng = StdRng::seed_from_u64(7);
    // Initially empty
    assert!(pool.pick("1", &mut rng).is_none());

    // Install two nodes for channel 1: prio 10 and 5
    let snap = ProxySnapshot {
        nodes: vec![
            ProxyNode {
                id: 1,
                scheme: ProxyScheme::Direct,
                host: "a".into(),
                port: 1,
                auth: None,
                channel_keys: vec!["1".into()],
                opts: None,
                priority: 5,
            },
            ProxyNode {
                id: 2,
                scheme: ProxyScheme::Direct,
                host: "b".into(),
                port: 2,
                auth: None,
                channel_keys: vec!["1".into()],
                opts: None,
                priority: 10,
            },
        ],
    };
    pool.install(snap);
    // Lower-priority tiers never participate: whatever rng does, only id=2 at prio 10 can be picked
    for _ in 0..20 {
        let picked = pool.pick("1", &mut rng).unwrap();
        assert_eq!(
            picked.id, 2,
            "only highest priority layer (10) should be picked"
        );
    }

    // Channel with no proxy configured
    assert!(pool.pick("999", &mut rng).is_none());

    // Re-install: wholesale replacement, so the old index (including id=2) does not linger
    let snap = ProxySnapshot {
        nodes: vec![ProxyNode {
            id: 3,
            scheme: ProxyScheme::Direct,
            host: "c".into(),
            port: 3,
            auth: None,
            channel_keys: vec!["1".into()],
            opts: None,
            priority: 5,
        }],
    };
    pool.install(snap);
    for _ in 0..10 {
        assert_eq!(pool.pick("1", &mut rng).unwrap().id, 3);
    }
}

/// `pick`: random within the same priority layer — with a fixed seed the result is deterministic
/// and reproducible.
///
/// Same convention as `gateway_dispatch::selector`: the caller injects the rng and tests do
/// not depend on a global random source, so this can assert "both nodes appeared" rather than
/// a probabilistic expectation.
#[test]
fn pick_random_within_same_priority_layer() {
    let pool = ProxyPool::new();
    let snap = ProxySnapshot {
        nodes: vec![
            ProxyNode {
                id: 10,
                scheme: ProxyScheme::Direct,
                host: "a".into(),
                port: 1,
                auth: None,
                channel_keys: vec!["1".into()],
                opts: None,
                priority: 10,
            },
            ProxyNode {
                id: 20,
                scheme: ProxyScheme::Direct,
                host: "b".into(),
                port: 2,
                auth: None,
                channel_keys: vec!["1".into()],
                opts: None,
                priority: 10,
            },
        ],
    };
    pool.install(snap);

    let mut rng = StdRng::seed_from_u64(42);
    let mut ids: Vec<i64> = (0..50)
        .map(|_| pool.pick("1", &mut rng).unwrap().id)
        .collect();
    ids.sort_unstable();
    ids.dedup();
    assert_eq!(
        ids,
        vec![10, 20],
        "both same-priority nodes must be reachable"
    );
}

/// SSRF: `check_ip` blocks loopback / private / link-local / multicast / unspecified / CGNAT
#[test]
fn check_ip_blocks_reserved_ranges() {
    // loopback
    assert!(matches!(
        check_ip(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1))),
        Err(SsrfError::Loopback)
    ));
    assert!(matches!(
        check_ip(IpAddr::V6(Ipv6Addr::new(0, 0, 0, 0, 0, 0, 0, 1))),
        Err(SsrfError::Loopback)
    ));

    // private IPv4 (10/8, 172.16/12, 192.168/16)
    assert!(matches!(
        check_ip(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1))),
        Err(SsrfError::PrivateIp)
    ));
    assert!(matches!(
        check_ip(IpAddr::V4(Ipv4Addr::new(172, 16, 0, 1))),
        Err(SsrfError::PrivateIp)
    ));
    assert!(matches!(
        check_ip(IpAddr::V4(Ipv4Addr::new(192, 168, 0, 1))),
        Err(SsrfError::PrivateIp)
    ));

    // link-local IPv4 (169.254/16)
    assert!(matches!(
        check_ip(IpAddr::V4(Ipv4Addr::new(169, 254, 0, 1))),
        Err(SsrfError::LinkLocal)
    ));

    // multicast
    assert!(matches!(
        check_ip(IpAddr::V4(Ipv4Addr::new(224, 0, 0, 1))),
        Err(SsrfError::Multicast)
    ));
    assert!(matches!(
        check_ip(IpAddr::V6(Ipv6Addr::new(0xff00, 0, 0, 0, 0, 0, 0, 0))),
        Err(SsrfError::Multicast)
    ));

    // unspecified
    assert!(matches!(
        check_ip(IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0))),
        Err(SsrfError::Unspecified)
    ));
    assert!(matches!(
        check_ip(IpAddr::V6(Ipv6Addr::new(0, 0, 0, 0, 0, 0, 0, 0))),
        Err(SsrfError::Unspecified)
    ));

    // CGNAT 100.64.0.0/10
    assert!(matches!(
        check_ip(IpAddr::V4(Ipv4Addr::new(100, 64, 0, 1))),
        Err(SsrfError::PrivateIp)
    ));
    assert!(matches!(
        check_ip(IpAddr::V4(Ipv4Addr::new(100, 127, 255, 255))),
        Err(SsrfError::PrivateIp)
    ));
    // Outside the CGNAT boundary should pass
    assert!(check_ip(IpAddr::V4(Ipv4Addr::new(100, 63, 255, 255))).is_ok());
    assert!(check_ip(IpAddr::V4(Ipv4Addr::new(100, 128, 0, 0))).is_ok());

    // Public IP passes
    assert!(check_ip(IpAddr::V4(Ipv4Addr::new(1, 1, 1, 1))).is_ok());
    assert!(check_ip(IpAddr::V6(Ipv6Addr::from_str("2606:4700::1111").unwrap())).is_ok());
}

/// `::ffff:127.0.0.1` (IPv4-mapped IPv6) must be blocked — this is a regression test
#[test]
fn ipv4_mapped_ipv6_loopback_is_blocked() {
    // ::ffff:127.0.0.1
    let mapped = IpAddr::V6(Ipv6Addr::new(0, 0, 0, 0, 0, 0xffff, 0x7f00, 0x0001));
    assert!(matches!(check_ip(mapped), Err(SsrfError::Loopback)));
    // another mapped loopback
    let mapped2 = IpAddr::V6(Ipv6Addr::from_str("::ffff:127.0.0.1").unwrap());
    assert!(matches!(check_ip(mapped2), Err(SsrfError::Loopback)));
    // A non-loopback mapped address should pass (as long as it is not private/loopback)
    let mapped_public = IpAddr::V6(Ipv6Addr::from_str("::ffff:1.1.1.1").unwrap());
    assert!(check_ip(mapped_public).is_ok());
}

/// `validate_url`: only IP literals are checked; domain names are not DNS-resolved and pass
/// (the caller must call validate_resolved afterwards)
#[test]
fn validate_url_ip_literals_checked_domains_skipped() {
    // IP literals are checked
    let url = Url::parse("http://127.0.0.1/").unwrap();
    assert!(matches!(validate_url(&url), Err(SsrfError::Loopback)));
    let url = Url::parse("http://1.1.1.1/").unwrap();
    assert!(validate_url(&url).is_ok());

    // Domain names pass (no DNS resolution)
    let url = Url::parse("http://internal.example.com/").unwrap();
    assert!(validate_url(&url).is_ok());
    // The caller must resolve DNS and then call validate_resolved
}

/// `validate_resolved`: a mixed list with one internal IP fails as a whole
#[test]
fn validate_resolved_fails_if_any_bad_ip() {
    let addrs = vec![
        IpAddr::V4(Ipv4Addr::new(1, 1, 1, 1)),  // public
        IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1)), // private
    ];
    let err = validate_resolved(&addrs).unwrap_err();
    assert!(matches!(err, SsrfError::PrivateIp));

    // all public -> return the first
    let addrs = vec![
        IpAddr::V4(Ipv4Addr::new(1, 1, 1, 1)),
        IpAddr::V4(Ipv4Addr::new(8, 8, 8, 8)),
    ];
    let first = validate_resolved(&addrs).unwrap();
    assert_eq!(first, IpAddr::V4(Ipv4Addr::new(1, 1, 1, 1)));

    // empty list -> Dns error
    let err = validate_resolved(&[]).unwrap_err();
    assert!(matches!(err, SsrfError::Dns(_)));
}

fn test_node(
    id: i64,
    scheme: ProxyScheme,
    host: &str,
    port: u16,
    key: &str,
    priority: i32,
) -> ProxyNode {
    ProxyNode {
        id,
        scheme,
        host: host.into(),
        port,
        auth: None,
        opts: None,
        channel_keys: vec![key.into()],
        priority,
    }
}

/// With no nodes acquire falls back to direct (node_id = 0); in production this equals a
/// channel with no egress configured.
#[test]
fn acquire_without_nodes_returns_direct() {
    let manager = ProxyManager::new();
    let lease = manager.acquire("missing");
    assert_eq!(lease.node_id, 0);
}

/// After an HTTP node is installed, acquire hits that node instead of direct.
#[test]
fn acquire_http_node_returns_that_node() {
    let manager = ProxyManager::new();
    manager.install(ProxySnapshot {
        nodes: vec![test_node(
            7,
            ProxyScheme::Http,
            "proxy.example",
            8080,
            "ch",
            10,
        )],
    });
    let lease = manager.acquire("ch");
    assert_eq!(lease.node_id, 7);
}

/// 401 is an account problem and does not cool down the egress; subsequent acquires should
/// still hit the same high-priority node.
#[test]
fn feedback_401_does_not_cooldown() {
    let manager = ProxyManager::new();
    manager.install(ProxySnapshot {
        nodes: vec![
            test_node(1, ProxyScheme::Http, "a", 8080, "ch", 10),
            test_node(2, ProxyScheme::Http, "b", 8081, "ch", 5),
        ],
    });
    let lease = manager.acquire("ch");
    assert_eq!(lease.node_id, 1);
    manager.feedback(lease.node_id, 401, false);
    drop(lease);
    assert_eq!(manager.acquire("ch").node_id, 1);
}

/// A dial failure cools the highest tier down and falls to the next; if both tiers are
/// cooling, go direct.
#[test]
fn transport_error_cools_node_then_falls_back() {
    let manager = ProxyManager::new();
    manager.install(ProxySnapshot {
        nodes: vec![
            test_node(1, ProxyScheme::Http, "a", 8080, "ch", 10),
            test_node(2, ProxyScheme::Http, "b", 8081, "ch", 5),
        ],
    });
    let first = manager.acquire("ch");
    assert_eq!(first.node_id, 1);
    manager.feedback(first.node_id, 502, true);
    drop(first);
    let second = manager.acquire("ch");
    assert_eq!(second.node_id, 2);
    manager.feedback(second.node_id, 502, true);
    drop(second);
    assert_eq!(manager.acquire("ch").node_id, 0);
}
/// `parse_url`: new protocol scheme mapping + default ports + error for ss without an explicit port
#[test]
fn parse_url_new_schemes_mapping_and_defaults() {
    // vless -> Vless, default 443
    let node = ProxyNode::parse_url("vless://proxy.example.com").unwrap();
    assert_eq!(node.scheme, ProxyScheme::Vless);
    assert_eq!(node.host, "proxy.example.com");
    assert_eq!(node.port, 443);
    assert!(node.auth.is_none());

    // vmess -> Vmess, default 443 (vmess:// is a base64 JSON payload; this PR only recognizes
    // the scheme marker, payload parsing lands in PR3)
    let node = ProxyNode::parse_url("vmess://proxy.example.com").unwrap();
    assert_eq!(node.scheme, ProxyScheme::Vmess);
    assert_eq!(node.port, 443);

    // trojan -> Trojan, default 443
    let node = ProxyNode::parse_url("trojan://proxy.example.com").unwrap();
    assert_eq!(node.scheme, ProxyScheme::Trojan);
    assert_eq!(node.port, 443);

    // ss -> Shadowsocks, no standard default port; a missing port is an error
    let err = ProxyNode::parse_url("ss://proxy.example.com").unwrap_err();
    assert!(err.to_string().contains("ss scheme requires explicit port"));

    // ss with an explicit port
    let node = ProxyNode::parse_url("ss://proxy.example.com:8388").unwrap();
    assert_eq!(node.scheme, ProxyScheme::Shadowsocks);
    assert_eq!(node.port, 8388);

    // New protocols also support auth extraction (percent-decoded)
    let node = ProxyNode::parse_url("vless://user:pass@proxy.example.com:443").unwrap();
    assert_eq!(node.auth.as_ref().unwrap().user, "user");
    assert_eq!(node.auth.as_ref().unwrap().pass, "pass");

    // An explicit port wins over the default
    let node = ProxyNode::parse_url("vless://proxy.example.com:8443").unwrap();
    assert_eq!(node.port, 8443);
}

/// `to_reqwest_proxy`: new protocol variants return an error (these protocols do not go
/// through reqwest::Proxy; they are dialed by the meow `ProxyAdapter` in
/// `gateway_proxy::adapter`)
#[test]
fn to_reqwest_proxy_new_schemes_return_error() {
    let schemes = [
        ProxyScheme::Vless,
        ProxyScheme::Vmess,
        ProxyScheme::Shadowsocks,
        ProxyScheme::Trojan,
    ];
    for scheme in schemes {
        let node = ProxyNode {
            id: 1,
            scheme,
            host: "proxy.example.com".into(),
            port: 443,
            auth: None,
            channel_keys: vec![],
            opts: None,
            priority: 0,
        };
        let result = to_reqwest_proxy(&node);
        assert!(result.is_err(), "scheme {:?} should return error", scheme);
        let err = result.unwrap_err();
        assert!(
            err.to_string().contains("does not go through reqwest"),
            "got: {err}"
        );
    }
}

/// `manager::acquire`: with a vless node and an http node in the same channel, acquire always
/// picks the vless node — priority is compared before scheme, and the vless adapter is
/// implemented.
#[test]
fn acquire_prefers_implemented_high_priority_nodes() {
    let manager = ProxyManager::new();
    let mut vless_node = test_node(1, ProxyScheme::Vless, "vless.example", 443, "ch", 10);
    vless_node.auth = Some(BasicAuth {
        user: "b831381d-6324-4d53-ad4f-8cda48b30811".into(),
        pass: String::new(),
    });
    manager.install(ProxySnapshot {
        nodes: vec![
            vless_node,
            // http node: lower priority
            test_node(2, ProxyScheme::Http, "http.example", 8080, "ch", 5),
        ],
    });
    for _ in 0..20 {
        let lease = manager.acquire("ch");
        assert_eq!(
            lease.node_id, 1,
            "implemented Vless at higher priority should be picked"
        );
    }
}
