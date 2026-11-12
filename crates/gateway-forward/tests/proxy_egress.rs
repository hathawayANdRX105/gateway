//! Wiring tests for `ForwardStage::with_proxies` — verify the leased Client
//! replaces the injected egress.
//!
//! Contract:
//! - proxies = None → the injected mock egress is used
//! - proxies = Some(manager with node) → the leased Client is used (the mock is not
//!   called)
//! - proxies = Some(manager no node) → the direct Client is used (the mock is not
//!   called)

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use bytes::Bytes;
use gateway_forward::egress::{Egress, ForwardedResponse, Timeouts};
use gateway_forward::stage::ForwardStage;
use gateway_pipeline::{
    BodySource, Pipeline, ProtocolKind, RequestMeta, SelectedRoute, StreamedAccum, TokenInfo,
};
use gateway_proxy::manager::ProxyManager;
use gateway_proxy::node::ProxyScheme;
use gateway_proxy::pool::{ProxyNode, ProxySnapshot};
use gateway_core::records::ChannelSettings;
const STREAM: StreamedAccum = StreamedAccum {
    prompt_tokens: 0,
    completion_tokens: 0,
    first_token_at: None,
};

// ---------- spy egress: records whether it is called ----------

struct SpyEgress {
    call_count: Arc<AtomicUsize>,
}

impl Egress for SpyEgress {
    fn execute<'a>(
        &'a self,
        _url: &'a str,
        _headers: &'a [(String, String)],
        _body: Bytes,
        _timeouts: &'a Timeouts,
    ) -> Pin<
        Box<
            dyn Future<Output = Result<ForwardedResponse, gateway_core::error::NormalizedError>>
                + Send
                + 'a,
        >,
    > {
        self.call_count.fetch_add(1, Ordering::SeqCst);
        Box::pin(async move {
            let stream = futures_util::stream::iter(vec![Ok::<Bytes, std::io::Error>(
                Bytes::from_static(b"{\"choices\":[]}"),
            )]);
            Ok(ForwardedResponse::from_stream(
                200,
                "application/json",
                None,
                stream,
            ))
        })
    }
}

/// Fixture envelope version: record identity is assigned by the consumer, so gateway
/// tests only need a stable placeholder.
const FIXTURE_SCHEMA_VERSION: u32 = 1;

fn make_ctx(route: Option<SelectedRoute>) -> gateway_pipeline::RequestCtx {
    gateway_pipeline::RequestCtx {
        request: RequestMeta {
            method: "POST".to_string(),
            path: "/v1/chat/completions".to_string(),
            headers: http::HeaderMap::new(),
            body: BodySource::InMemory(Bytes::from_static(b"{\"model\":\"gpt-4o\"}")),
            client_ip: "127.0.0.1".parse().unwrap(),
            request_id: uuid::Uuid::now_v7(),
            inbound_protocol: ProtocolKind::OpenAI,
        },
        token: Some(TokenInfo {
            id: "tok-1".into(),
            user_key: "user-test".to_string(),
            group: "default".to_string(),
            enabled: true,
            allowed_models: None,
            auth_version: 1,
        }),
        requested_model: Some("gpt-4o".to_string()),
        requested_max_tokens: None,
        route,
        selected_channel_key: None,
        selected_channel_name: None,
        session_hint: None,
        upstream: None,
        streamed: STREAM,
        error: None,
        drop_guards: Vec::new(),
        hold: None,
    }
}

fn route_for(key: &str, base: &str) -> SelectedRoute {
    let unit = gateway_core::records::RouteUnitRecord {
        meta: gateway_core::records::SyncMeta {
            key: format!("unit-{key}"),
            schema_version: FIXTURE_SCHEMA_VERSION,
            logical_version: 1,
            origin: "test".into(),
            updated_at: chrono::Utc::now(),
        },
        group: "default".into(),
        public_model: "gpt-4o".into(),
        channel_key: key.into(),
        key_index: 0,
        upstream_model: "gpt-4o".into(),
        weight: 10,
        status: 1,
    };
    SelectedRoute {
        unit,
        secret: "sk-test".into(),
        base_url: base.into(),
        upstream_model: "gpt-4o".into(),
        provider_type: "openai".into(),
        settings: ChannelSettings::default(),
    }
}

fn make_pipeline_with(egress: Arc<SpyEgress>, proxies: Option<Arc<ProxyManager>>) -> Arc<Pipeline> {
    let adaptors = Arc::new(gateway_protocol_bridge::format_codec::FormatRegistry::with_defaults());
    let mut stage = ForwardStage::new(egress, adaptors.clone());
    if let Some(proxies) = proxies {
        stage = stage.with_proxies(proxies);
    }
    Arc::new(Pipeline::new().push(stage).push(
        gateway_protocol_bridge::stage::ProtocolBridgeStage::new(adaptors),
    ))
}

// ---------- tests ----------

/// proxies = None → the injected mock egress is called.
#[tokio::test]
async fn proxies_none_uses_injected_egress() {
    let spy = Arc::new(SpyEgress {
        call_count: Arc::new(AtomicUsize::new(0)),
    });
    let pipe = make_pipeline_with(spy.clone(), None);
    let ctx = make_ctx(Some(route_for("ch", "https://upstream.example")));
    let resp = pipe.run(ctx).await.expect("pipeline should succeed");
    assert_eq!(resp.status(), 200);
    assert_eq!(
        spy.call_count.load(Ordering::SeqCst),
        1,
        "mock egress should be called"
    );
}

/// proxies = Some(manager with node) → the leased Client replaces the mock; the
/// mock is not called.
/// The node points at an unreachable port, so the request fails — but the failure
/// comes from the leased Client, not the mock.
#[tokio::test]
async fn proxies_with_node_uses_leased_client() {
    let spy = Arc::new(SpyEgress {
        call_count: Arc::new(AtomicUsize::new(0)),
    });
    let manager = Arc::new(ProxyManager::new());
    manager.install(ProxySnapshot {
        nodes: vec![ProxyNode {
            id: 1,
            scheme: ProxyScheme::Http,
            host: "127.0.0.1".into(),
            port: 1,
            auth: None,
            channel_keys: vec!["ch".into()],
            opts: None,
            priority: 10,
        }],
    });
    let pipe = make_pipeline_with(spy.clone(), Some(manager));
    let ctx = make_ctx(Some(route_for("ch", "https://upstream.example")));
    let _ = pipe.run(ctx).await;
    assert_eq!(
        spy.call_count.load(Ordering::SeqCst),
        0,
        "leased client should replace mock egress"
    );
}

/// proxies = Some(manager no node) → the direct Client replaces the mock; the mock
/// is not called.
#[tokio::test]
async fn proxies_without_node_uses_direct_client() {
    let spy = Arc::new(SpyEgress {
        call_count: Arc::new(AtomicUsize::new(0)),
    });
    let manager = Arc::new(ProxyManager::new());
    let pipe = make_pipeline_with(spy.clone(), Some(manager));
    let ctx = make_ctx(Some(route_for("missing", "https://upstream.example")));
    let _ = pipe.run(ctx).await;
    assert_eq!(
        spy.call_count.load(Ordering::SeqCst),
        0,
        "direct client should replace mock egress"
    );
}
