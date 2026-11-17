//! Egress-proxy microbenchmarks for the pure validation, mapping and selection helpers.
//!
//! - `validate_url` / `check_ip` — SSRF checks on public IPs and domains (the accept path);
//! - `tcp_metadata` — meow egress metadata construction, done once per adapter dial;
//! - `adapter_for` — node to adapter mapping: the cheap reqwest-scheme fallthrough and the
//!   full meow-config `parse_proxy` path for a VLESS node;
//! - `ProxyPool::pick` / `ProxyManager::acquire` — per-request lock-free node selection.
//!
//! A fixed in-memory snapshot is installed once per benchmark and no socket is ever opened:
//! none of these helpers dial a network. Inputs and results are black-boxed so the compiler
//! cannot elide the measured work. Local only, not run in CI.

use std::net::{IpAddr, Ipv4Addr};
use std::time::Duration;

use criterion::{Criterion, black_box, criterion_group, criterion_main};
use gateway_proxy::adapter::{adapter_for, tcp_metadata};
use gateway_proxy::manager::ProxyManager;
use gateway_proxy::node::ProxyNode;
use gateway_proxy::pool::{ProxyPool, ProxySnapshot};
use gateway_proxy::ssrf::{check_ip, validate_url};
use rand::SeedableRng;
use rand::rngs::StdRng;
use url::Url;

const CHANNEL: &str = "bench";

/// Build an in-memory snapshot with two tiers on the benchmark channel: the top tier has a
/// tie (priority 10) so pool pick and manager acquire exercise priority + least-inflight
/// selection, not just a single-node fast path.
fn snapshot() -> ProxySnapshot {
    let node = |id, priority| ProxyNode {
        id,
        scheme: gateway_proxy::node::ProxyScheme::Http,
        host: format!("n{id}.example"),
        port: 8080,
        auth: None,
        opts: None,
        channel_keys: vec![CHANNEL.to_string()],
        priority,
    };
    ProxySnapshot {
        nodes: vec![node(1, 10), node(2, 10), node(3, 5)],
    }
}

fn proxy_ops_bench(c: &mut Criterion) {
    let mut group = c.benchmark_group("proxy_ops");
    group.sample_size(50);
    group.measurement_time(Duration::from_secs(2));
    group.warm_up_time(Duration::from_secs(1));

    let public_ip = Url::parse("http://8.8.8.8:8080/v1").expect("public IP URL must parse");
    let public_domain =
        Url::parse("https://api.example.com/v1").expect("public domain URL must parse");
    group.bench_function("validate_url_ip_literal", |b| {
        b.iter(|| {
            let _ = black_box(validate_url(black_box(&public_ip)));
        });
    });
    group.bench_function("validate_url_domain", |b| {
        b.iter(|| {
            let _ = black_box(validate_url(black_box(&public_domain)));
        });
    });
    group.bench_function("check_ip_public_v4", |b| {
        let ip = IpAddr::V4(Ipv4Addr::new(8, 8, 8, 8));
        b.iter(|| {
            let _ = black_box(check_ip(black_box(ip)));
        });
    });

    group.bench_function("tcp_metadata", |b| {
        b.iter(|| {
            black_box(tcp_metadata(black_box("api.example.com"), black_box(443)));
        });
    });

    let socks5 = ProxyNode::parse_url("socks5://proxy.example.com:1080")
        .expect("socks5 node URL must parse");
    let vless = ProxyNode::parse_url(
        "vless://11111111-2222-3333-4444-555555555555@example.com:443?flow=xtls-rprx-vision&sni=cdn.example.com",
    )
    .expect("vless node URL must parse");
    group.bench_function("adapter_for_socks5", |b| {
        b.iter(|| {
            black_box(adapter_for(black_box(&socks5)));
        });
    });
    group.bench_function("adapter_for_vless", |b| {
        b.iter(|| {
            black_box(adapter_for(black_box(&vless)));
        });
    });

    let pool = ProxyPool::new();
    pool.install(snapshot());
    let mut rng = StdRng::seed_from_u64(42);
    group.bench_function("pool_pick", |b| {
        b.iter(|| {
            black_box(pool.pick(black_box(CHANNEL), &mut rng));
        });
    });

    let manager = ProxyManager::new();
    manager.install(snapshot());
    group.bench_function("manager_acquire", |b| {
        b.iter(|| {
            let lease = manager.acquire(black_box(CHANNEL));
            black_box(lease.node_id);
        });
    });

    group.finish();
}

criterion_group!(bench, proxy_ops_bench);
criterion_main!(bench);
