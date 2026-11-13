//! P1-C dual health-ledger bridge integration test (`proxy::manager` node ledger → dispatch route
//! unit ledger).
//!
//! Behavior under test: `ForwardStage` takes the production lease path (when proxies=Some the
//! injected egress is replaced, so a real local HTTP mock is started with `std::net` and reqwest
//! goes through the real protocol stack for both direct and via-node requests). The bridge
//! semantics are observed through the real `Dispatcher` + `MemoryHealthTable` (externally
//! observable state = the unit's `cooldown_until_ms` / mock hit count; private flags are not
//! tested):
//!
//! 1. When all proxy nodes bound to a channel are cooling → a direct-connection fallback failure
//!    is recorded as a Retryable failure on the route unit; cooldown has no threshold, so the
//!    **first** such failure evicts the unit and select stops repeatedly picking this
//!    "all proxies down" channel (positive).
//! 2. A channel with no bound nodes to begin with → direct connection is the expected path, a
//!    401 goes through P1-B Neutral, and the unit records no streak and never cools (reverse pin:
//!    distinguishes "degradation fallback" from "by design").
//! 3. Bound nodes healthy and selected (no direct fallback happens) → a 401 on the node does not
//!    trigger degradation accounting, and unit health and the node ledger do not pollute each
//!    other (reverse pin: unaffected when no fallback exists).

use bytes::Bytes;
use gateway_core::records::{
    ChannelKey, ChannelRecord, ChannelSettings, RouteUnitRecord, SyncMeta,
};
use gateway_dispatch::health::{AttemptReport, HealthTable, MemoryHealthTable, TpsSample};
use gateway_dispatch::{Candidate, Dispatch, DispatchError, Dispatcher, RetryPolicy, Snapshot};
use gateway_forward::ForwardStage;
use gateway_forward::egress::{Egress, ForwardedResponse, Timeouts};
use gateway_gate::concurrency::ConcurrencyState;
use gateway_pipeline::{
    BodySource, ProtocolKind, RequestCtx, RequestMeta, Stage, StageOutcome, StreamedAccum,
    TokenInfo,
};
use gateway_proxy::manager::ProxyManager;
use gateway_proxy::node::{ProxyNode, ProxyScheme};
use gateway_proxy::pool::ProxySnapshot;
use std::collections::HashMap;
use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpListener;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

// ---------- Local mock upstream ----------

/// Minimal HTTP/1.1 mock: every request gets a fixed `status` + JSON body, `connection: close`.
/// It first consumes the headers + the content-length-declared body before responding (avoiding a
/// premature close that triggers an RST and degrades a 401-semantic request into a 502 transport
/// error). Returns (port, hit counter).
fn spawn_mock(status: u16, body: &'static [u8]) -> (u16, Arc<AtomicUsize>) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    let hits = Arc::new(AtomicUsize::new(0));
    let counter = Arc::clone(&hits);
    std::thread::spawn(move || {
        let head = format!(
            "HTTP/1.1 {status} X\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n",
            body.len()
        );
        for stream in listener.incoming() {
            let Ok(mut stream) = stream else { continue };
            counter.fetch_add(1, Ordering::Relaxed);
            let mut reader = BufReader::new(stream.try_clone().unwrap());
            let mut content_length = 0usize;
            loop {
                let mut line = String::new();
                if reader.read_line(&mut line).unwrap_or(0) == 0 {
                    break;
                }
                let t = line.trim().to_ascii_lowercase();
                if t.is_empty() {
                    break;
                }
                if let Some(v) = t.strip_prefix("content-length:") {
                    content_length = v.trim().parse().unwrap_or(0);
                }
            }
            if content_length > 0 {
                let mut discard = vec![0u8; content_length];
                let _ = reader.read_exact(&mut discard);
            }
            let _ = stream.write_all(head.as_bytes());
            let _ = stream.write_all(body);
            let _ = stream.flush();
            let _ = stream.shutdown(std::net::Shutdown::Write);
        }
    });
    (port, hits)
}

/// With proxies=Some the lease Client fully replaces the injected egress; being called is a
/// wiring bug.
struct NeverEgress;

impl Egress for NeverEgress {
    fn execute<'a>(
        &'a self,
        _alias: &str,
        _url: &'a str,
        _headers: &'a [(String, String)],
        _body: Bytes,
        _timeouts: &'a Timeouts,
    ) -> std::pin::Pin<
        Box<
            dyn Future<Output = Result<ForwardedResponse, gateway_core::error::NormalizedError>>
                + Send
                + 'a,
        >,
    > {
        Box::pin(async move { panic!("proxies=Some: injected egress must not be used") })
    }
}

// ---------- Snapshot / ctx / stage assembly ----------

fn http_node(id: i64, channel_key: &str, host: &str, port: u16) -> ProxyNode {
    ProxyNode {
        id,
        scheme: ProxyScheme::Http,
        host: host.to_string(),
        port,
        auth: None,
        opts: None,
        channel_keys: vec![channel_key.to_string()],
        priority: 0,
    }
}

fn unit(key: &str, channel_key: &str) -> RouteUnitRecord {
    RouteUnitRecord {
        meta: SyncMeta {
            key: key.to_string(),
            schema_version: 1,
            logical_version: 1,
            origin: "test".to_string(),
            updated_at: chrono::Utc::now(),
        },
        group: "g".to_string(),
        public_model: "m".to_string(),
        channel_key: channel_key.to_string(),
        key_index: 0,
        upstream_model: "m".to_string(),
        weight: 10,
        status: 1,
    }
}

fn channel(key: &str, base_url: String) -> ChannelRecord {
    ChannelRecord {
        meta: SyncMeta {
            key: key.to_string(),
            schema_version: 1,
            logical_version: 1,
            origin: "test".to_string(),
            updated_at: chrono::Utc::now(),
        },
        name: key.to_string(),
        provider_type: "openai".to_string(),
        base_url,
        keys: vec![ChannelKey {
            index: 0,
            secret: "sk-test".to_string(),
            rpm_limit: 0,
        }],
        max_concurrency: 10,
        status: 1,
        groups: vec!["g".to_string()],
        settings: ChannelSettings::default(),
    }
}

/// u1 → ch-a (401 upstream, first hop pinned by `U1FirstAttempt`), u2 → ch-b (200 upstream, the
/// fallback winner).
fn snap(p401: u16, p200: u16) -> Snapshot {
    let mut channels = HashMap::new();
    channels.insert(
        "ch-a".to_string(),
        channel("ch-a", format!("http://127.0.0.1:{p401}")),
    );
    channels.insert(
        "ch-b".to_string(),
        channel("ch-b", format!("http://127.0.0.1:{p200}")),
    );
    Snapshot {
        units: vec![unit("u1", "ch-a"), unit("u2", "ch-b")],
        channels,
    }
}

fn mk_ctx() -> RequestCtx {
    RequestCtx {
        request: RequestMeta {
            method: "POST".to_string(),
            path: "/v1/chat/completions".to_string(),
            headers: http::HeaderMap::new(),
            body: BodySource::InMemory(Bytes::from_static(b"{\"model\":\"m\"}")),
            client_ip: "127.0.0.1".parse().unwrap(),
            request_id: uuid::Uuid::now_v7(),
            inbound_protocol: ProtocolKind::OpenAI,
        },
        token: Some(TokenInfo {
            id: "tok-1".into(),
            user_key: "user-test".to_string(),
            group: "g".to_string(),
            enabled: true,
            allowed_models: None,
            auth_version: 1,
        }),
        requested_model: Some("m".to_string()),
        requested_max_tokens: None,
        route: None,
        selected_channel_key: None,
        selected_channel_name: None,
        session_hint: None,
        upstream: None,
        streamed: StreamedAccum::default(),
        error: None,
        drop_guards: Vec::new(),
        hold: None,
    }
}

/// Test-only Dispatch wrapper: under global weighted P2C (PR #19) the first-hop candidate is
/// sampled randomly, while these three cases assert the **degradation accounting ledger** (u1's
/// 401 hit count, cooldown state), which requires u1 to always be tried first. So during tests the
/// first hop is pinned: on the first hop the fallback u2 is hidden in exclude, and all other
/// calls pass straight through to the real `Dispatcher` (health cooldown, failover, and selector
/// semantics unchanged). Once u1 is cooling, `is_selectable` is false and the wrapper stops
/// interfering → eviction/fallback assertions still take the real path.
struct U1FirstAttempt {
    inner: Dispatcher,
    health: Arc<MemoryHealthTable>,
}

impl Dispatch for U1FirstAttempt {
    fn select(
        &self,
        group: &str,
        public_model: &str,
        exclude: &[String],
        sticky_channel_key: Option<&str>,
        min_context: Option<u64>,
    ) -> Result<Candidate, DispatchError> {
        let now_ms = chrono::Utc::now().timestamp_millis().max(0).cast_unsigned();
        let exclude: Vec<String> = if exclude.is_empty() && self.health.is_selectable("u1", now_ms)
        {
            vec!["u2".to_string()]
        } else {
            exclude.to_vec()
        };
        self.inner.select(
            group,
            public_model,
            &exclude,
            sticky_channel_key,
            min_context,
        )
    }

    fn report(&self, unit_key: &str, report: AttemptReport) {
        self.inner.report(unit_key, report);
    }

    fn report_tps(&self, unit_key: &str, sample: TpsSample) {
        self.inner.report_tps(unit_key, sample);
    }

    fn channel_name(&self, channel_key: &str) -> Option<String> {
        self.inner.channel_name(channel_key)
    }
}

fn build_stage(mgr: Arc<ProxyManager>, dispatcher: Arc<dyn Dispatch>) -> ForwardStage {
    let adaptors = Arc::new(gateway_protocol_bridge::format_codec::FormatRegistry::new());
    ForwardStage::new(Arc::new(NeverEgress), adaptors)
        .with_proxies(mgr)
        .with_retry(
            dispatcher,
            RetryPolicy {
                max_attempts: 3,
                total_budget_ms: 0,
            },
        )
}

/// Run one request: u1 is selected first, fails, then u2 succeeds; assert the client-visible final
/// status 200.
async fn once(stage: &ForwardStage) -> u16 {
    let mut ctx = mk_ctx();
    let outcome = stage.handle(&mut ctx).await.expect("stage ok");
    assert!(
        matches!(outcome, StageOutcome::Continue),
        "non-streaming should return Continue",
    );
    ctx.upstream.expect("upstream settled").status
}

// ---------- Cases ----------

/// Positive: after both nodes A and B of a two-node channel are cooling, the request falls back
/// to a direct connection and hits the 401 upstream → the failure is recorded as Retryable on u1.
/// Cooldown no longer has a "N consecutive failures" threshold, so the **first** degradation
/// failure evicts u1; select then stops trying it (the 401 mock hit count stops growing) and u2
/// carries all traffic.
///
/// base and max are both set to 30s: the default 300ms window is too short on CI to span one mock
/// round-trip, which would turn the "u1 is no longer selected" assertion into a race. What this
/// case verifies is that eviction **stays effective**, not the duration.
#[tokio::test]
async fn cooled_channel_degrades_unit_health_until_eviction() {
    let (p401, hits401) = spawn_mock(401, b"{\"error\":\"bad_key\"}");
    let (p200, _) = spawn_mock(200, b"{\"ok\":true}");

    let mgr = Arc::new(ProxyManager::new());
    mgr.install(ProxySnapshot {
        nodes: vec![
            http_node(1, "ch-a", "n1.example", 8080),
            http_node(2, "ch-a", "n2.example", 8080),
        ],
    });
    // Mark both nodes dead in the node ledger: both bound nodes cool on transport failure (30s, so
    // they will not expire during the test).
    mgr.feedback(1, 502, true);
    mgr.feedback(2, 502, true);
    assert_eq!(
        mgr.node_cooldowns().len(),
        2,
        "precondition: both nodes are cooling"
    );

    let health = Arc::new(MemoryHealthTable::with_config(
        gateway_dispatch::health::HealthSetting {
            // Lengthen base and max together: raising only base would be clamped back to 8s by
            // start_cooldown's `min(max_ms)`, defeating the "long enough window" goal.
            cooldown_base_ms: 30_000,
            cooldown_max_ms: 30_000,
            ..gateway_dispatch::health::HealthSetting::default()
        },
    ));
    let dispatcher: Arc<dyn Dispatch> = Arc::new(U1FirstAttempt {
        inner: Dispatcher::new(
            Some(Arc::new(snap(p401, p200))),
            health.clone(),
            Arc::new(ConcurrencyState::default()),
        ),
        health: health.clone(),
    });
    let stage = build_stage(mgr, dispatcher);

    // Round 1: the direct-fallback failure is recorded as Retryable on u1 → no threshold,
    // immediate cooldown; within the same request u2 catches the traffic (invisible to the user).
    let status = once(&stage).await;
    assert_eq!(
        status, 200,
        "after u1 degradation failure, u2 must take over the request"
    );
    assert_eq!(
        hits401.load(Ordering::Relaxed),
        1,
        "first round should hit u1 once"
    );
    let now_ms = chrono::Utc::now().timestamp_millis().max(0).cast_unsigned();
    let st = health.get("u1");
    assert!(
        st.cooldown_until_ms > now_ms,
        "the first degradation failure should evict u1 immediately (cooling has no threshold)"
    );

    // Stop retrying dead nodes: u1 is excluded by the selector and the 401 mock gets zero new hits.
    for round in 2..=4 {
        let status = once(&stage).await;
        assert_eq!(status, 200, "round {round}: u2 alone carries traffic");
        assert_eq!(
            hits401.load(Ordering::Relaxed),
            1,
            "cooling u1 should not be selected again",
        );
    }
}

/// Reverse pin 1: the channel **has no bound nodes from the start** → direct connection is the
/// expected path, not a degradation fallback. The same 401 goes through P1-B Neutral (no channel
/// fault); even 6 times in a row does not cool it — degradation accounting must not misfire on
/// proxy-less channels.
#[tokio::test]
async fn unbound_direct_channel_is_not_degraded() {
    let (p401, hits401) = spawn_mock(401, b"{\"error\":\"bad_key\"}");
    let (p200, _) = spawn_mock(200, b"{\"ok\":true}");

    let mgr = Arc::new(ProxyManager::new()); // empty pool: ch-a has no binding
    let health = Arc::new(MemoryHealthTable::new());
    let dispatcher: Arc<dyn Dispatch> = Arc::new(U1FirstAttempt {
        inner: Dispatcher::new(
            Some(Arc::new(snap(p401, p200))),
            health.clone(),
            Arc::new(ConcurrencyState::default()),
        ),
        health: health.clone(),
    });
    let stage = build_stage(mgr, dispatcher);

    for round in 1..=6u32 {
        let status = once(&stage).await;
        assert_eq!(status, 200);
        let st = health.get("u1");
        assert_eq!(
            st.cooldown_until_ms, 0,
            "round {round}: an unbound direct-connection 401 is channel-related 4xx (Neutral), must not trigger cooling",
        );
        assert_eq!(
            u32::try_from(hits401.load(Ordering::Relaxed)).unwrap(),
            round
        );
    }
}

/// Reverse pin 2: bound nodes are healthy and selected (**no** direct fallback) → a 401 through
/// the node exit does not trigger degradation accounting: the unit never cools; the 401 also does
/// not cool the node (feedback semantics), leaving both ledgers clean.
#[tokio::test]
async fn healthy_bound_node_attempt_is_not_degraded() {
    let (p401, hits401) = spawn_mock(401, b"{\"error\":\"bad_key\"}");
    let (p200, _) = spawn_mock(200, b"{\"ok\":true}");

    // Node 1 = the local 401 mock moonlighting as an HTTP proxy (absolute-form requests also get a
    // 401), healthy and not cooling.
    let mgr = Arc::new(ProxyManager::new());
    mgr.install(ProxySnapshot {
        nodes: vec![http_node(1, "ch-a", "127.0.0.1", p401)],
    });

    let health = Arc::new(MemoryHealthTable::new());
    let mut snap = snap(p401, p200);
    // Swap ch-a's base_url for a non-directly-reachable domain: errors can only come from the
    // proxy node path, so attribution is unambiguous.
    snap.channels.get_mut("ch-a").unwrap().base_url = "http://via-node-only.invalid".to_string();
    let dispatcher: Arc<dyn Dispatch> = Arc::new(U1FirstAttempt {
        inner: Dispatcher::new(
            Some(Arc::new(snap)),
            health.clone(),
            Arc::new(ConcurrencyState::default()),
        ),
        health: health.clone(),
    });
    let stage = build_stage(Arc::clone(&mgr), dispatcher);

    for round in 1..=2u32 {
        let status = once(&stage).await;
        assert_eq!(
            status, 200,
            "round {round}: after u1 fails via the node, u2 falls back"
        );
        let st = health.get("u1");
        assert_eq!(
            st.cooldown_until_ms, 0,
            "node selected ≠ direct-connection fallback, must not book degradation"
        );
        assert_eq!(
            u32::try_from(hits401.load(Ordering::Relaxed)).unwrap(),
            round
        );
    }
    assert!(
        mgr.node_cooldowns().is_empty(),
        "401 must not cool proxy nodes"
    );
}
