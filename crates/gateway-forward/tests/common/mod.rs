//! Shared mocks and request builders for the forward integration tests.
//!
//! Each test file pulls this in with `mod common; use common::*;`. The module is a purely
//! mechanical dedup of the copies inlined in `retry_wiring` / `failure_telemetry` / settle:
//! mock logic and assertion semantics are unchanged, and `Plan` / `MockDispatch` take the
//! superset shape of those files (cases that do not read the recording fields never see
//! them). `common` is compiled separately into every test binary, so items not used by all
//! binaries are marked `#[allow(dead_code)]`.

use bytes::Bytes;
use gateway_core::error::NormalizedError;
use gateway_core::records::{ChannelSettings, RouteUnitRecord, SyncMeta, UsageEvent};
use gateway_dispatch::health::{AttemptReport, FailureClass};
use gateway_dispatch::{Candidate, Dispatch, DispatchError, TpsSample};
use gateway_forward::egress::{Egress, ForwardedResponse, Timeouts};
use gateway_metering::SettleSink;
use gateway_pipeline::{
    BodySource, ProtocolKind, RequestCtx, RequestMeta, SelectedRoute, StreamedAccum, TokenInfo,
};
use parking_lot::Mutex;

// ---------- Request builders ----------

/// Minimal non-streaming request body (the default body shared by the three test files).
pub const NON_STREAM_BODY: &[u8] = b"{\"model\":\"m\"}";

/// Streaming request body: it carries a top-level `"stream":true`, which
/// `body_wants_stream` uses to detect streaming intent.
#[allow(dead_code)] // not used by every test binary
pub const STREAM_BODY: &[u8] = b"{\"model\":\"m\",\"stream\":true}";

/// Build a test candidate: `base_url` carries the key marker so `ScriptedEgress` can tell
/// candidates apart.
pub fn candidate(key: &str) -> Candidate {
    SelectedRoute {
        unit: RouteUnitRecord {
            meta: SyncMeta {
                key: key.to_string(),
                schema_version: 1,
                logical_version: 1,
                origin: "test".to_string(),
                updated_at: chrono::Utc::now(),
            },
            group: "g".to_string(),
            public_model: "m".to_string(),
            channel_key: format!("ch-{key}"),
            key_index: 0,
            upstream_model: "m".to_string(),
            weight: 10,
            status: 1,
        },
        secret: "sk-test".to_string(),
        base_url: format!("http://upstream-{key}.invalid"),
        upstream_model: "m".to_string(),
        provider_type: "openai".to_string(),
        settings: ChannelSettings::default(),
    }
}

/// Build a request ctx: the body decides streaming intent (a `"stream":true` body is
/// streaming), and route is preset for single-shot mode (after `with_retry`, handle ignores
/// it). The token is fixed to tok-1/g; the channel-attribution fields stay empty (#140:
/// this group of cases does not rely on attribution).
pub fn ctx_with_body(body: &'static [u8], route: Candidate) -> RequestCtx {
    RequestCtx {
        request: RequestMeta {
            method: "POST".to_string(),
            path: "/v1/chat/completions".to_string(),
            headers: http::HeaderMap::new(),
            body: BodySource::InMemory(Bytes::from_static(body)),
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
        route: Some(route),
        hold: None,
        upstream: None,
        streamed: StreamedAccum::default(),
        selected_channel_key: None,
        selected_channel_name: None,
        session_hint: None,
        error: None,
        drop_guards: Vec::new(),
    }
}

/// Build a request ctx carrying only a route (no body parsing needed; used by
/// retry-attribution cases).
#[allow(dead_code)] // not used by every test binary
pub fn ctx_with_route(route: Candidate) -> RequestCtx {
    ctx_with_body(NON_STREAM_BODY, route)
}

// ---------- egress mock ----------

/// One plan item of the scripted egress mock: `Ok` returns a fixed success body, `Fail`
/// returns a classified error.
#[allow(dead_code)] // the settle binary does not use this mock
pub enum Plan {
    Ok { body: &'static [u8] },
    Fail { status: u16, retryable: bool },
}

/// Scripted egress mock: returns fixed results based on the candidate marker in the url.
/// The `Fail` `NormalizedError` is isomorphic to the real `egress::classify_status` output
/// (502 → retryable, 400 → not retryable, 401/403/404 → `channel_scoped`); forward only sees
/// the trait's return value.
#[allow(dead_code)] // the settle binary does not use this mock
pub struct ScriptedEgress {
    plans: Vec<(String, Plan)>,
    calls: Mutex<Vec<String>>,
}

#[allow(dead_code)] // same as above: not every binary uses the constructor and accessors
impl ScriptedEgress {
    pub fn new(plans: Vec<(&str, Plan)>) -> Self {
        Self {
            plans: plans.into_iter().map(|(k, p)| (k.to_string(), p)).collect(),
            calls: Mutex::new(Vec::new()),
        }
    }

    /// Upstream urls that were requested (for asserting attempt counts and hit
    /// candidates).
    pub fn calls(&self) -> Vec<String> {
        self.calls.lock().clone()
    }
}

impl Egress for ScriptedEgress {
    fn execute<'a>(
        &'a self,
        _alias: &str,
        url: &'a str,
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
        self.calls.lock().push(url.to_string());
        let (_, plan) = self
            .plans
            .iter()
            .find(|(marker, _)| url.contains(marker))
            .unwrap_or_else(|| panic!("ScriptedEgress: unexpected url {url}"));
        match plan {
            Plan::Ok { body } => {
                let body = Bytes::from_static(body);
                let stream = futures_util::stream::iter(vec![Ok::<Bytes, std::io::Error>(body)]);
                Box::pin(async move {
                    Ok(ForwardedResponse::from_stream(
                        200,
                        "application/json",
                        None,
                        stream,
                    ))
                })
            }
            Plan::Fail { status, retryable } => {
                let (status, retryable) = (*status, *retryable);
                let err = NormalizedError {
                    code: gateway_core::error::code::UPSTREAM_ERROR,
                    status,
                    retryable,
                    channel_scoped: matches!(status, 401 | 403 | 404) && !retryable,
                    message: format!("upstream {status}"),
                    retry_after_ms: None,
                };
                Box::pin(async move { Err(err) })
            }
        }
    }
}

/// Mock egress that always returns 200 plus a fixed body (non-streaming).
#[allow(dead_code)] // settle only
pub struct FixedEgress {
    pub body: &'static [u8],
}

impl Egress for FixedEgress {
    fn execute<'a>(
        &'a self,
        _alias: &str,
        _url: &'a str,
        _headers: &'a [(String, String)],
        _body: Bytes,
        _timeouts: &'a Timeouts,
    ) -> std::pin::Pin<
        Box<dyn Future<Output = Result<ForwardedResponse, NormalizedError>> + Send + 'a>,
    > {
        let stream = futures_util::stream::iter(vec![Ok::<Bytes, std::io::Error>(
            Bytes::from_static(self.body),
        )]);
        Box::pin(async move {
            Ok(ForwardedResponse::from_stream(
                200,
                "application/json",
                None,
                stream,
            ))
        })
    }
}

// ---------- dispatch mock ----------

/// Hand-written mock Dispatch: yields candidates in exclude order and records every
/// select's exclude set and every report verbatim so tests can assert the wiring (cases
/// that do not read the records simply ignore them; the select/report decision logic is
/// identical to the inline versions in the individual files).
#[allow(dead_code)] // settle binary unused; records are read only by some binaries
pub struct MockDispatch {
    candidates: Vec<Candidate>,
    /// `AllUnavailable.retry_after_ms` returned by `select` when everything is cooling or
    /// exhausted (default 0 = no cooldown).
    cooled_ra: u64,
    selects: Mutex<Vec<Vec<String>>>,
    reports: Mutex<Vec<(String, AttemptReport)>>,
}

#[allow(dead_code)] // same as above: not every binary uses the constructor and accessors
impl MockDispatch {
    pub fn new(candidates: Vec<Candidate>) -> Self {
        Self {
            candidates,
            cooled_ra: 0,
            selects: Mutex::new(Vec::new()),
            reports: Mutex::new(Vec::new()),
        }
    }

    /// `retry_after_ms` returned by `select` when everything is cooling or exhausted
    /// (0 = no cooldown; >0 = cooldown).
    pub fn with_cooling_ra(mut self, ra: u64) -> Self {
        self.cooled_ra = ra;
        self
    }

    /// Sequence of select calls (a snapshot of the exclude set per call).
    pub fn selects(&self) -> Vec<Vec<String>> {
        self.selects.lock().clone()
    }

    /// Sequence of report calls (unit key + health result).
    pub fn reports(&self) -> Vec<(String, Result<u16, FailureClass>)> {
        self.reports
            .lock()
            .iter()
            .map(|(key, report)| (key.clone(), report.outcome))
            .collect()
    }
}

impl Dispatch for MockDispatch {
    fn select(
        &self,
        group: &str,
        model: &str,
        exclude: &[String],
        _sticky_channel_key: Option<&str>,
        _min_context: Option<u64>,
    ) -> Result<Candidate, DispatchError> {
        self.selects.lock().push(exclude.to_vec());
        self.candidates
            .iter()
            .find(|c| !exclude.contains(&c.unit.meta.key))
            .cloned()
            .ok_or_else(|| DispatchError::AllUnavailable {
                group: group.to_string(),
                model: model.to_string(),
                retry_after_ms: self.cooled_ra,
            })
    }

    fn report(&self, unit_key: &str, report: AttemptReport) {
        self.reports.lock().push((unit_key.to_string(), report));
    }

    fn report_tps(&self, _unit_key: &str, _sample: TpsSample) {}

    fn channel_name(&self, channel_key: &str) -> Option<String> {
        // candidate() generates `ch-{key}`; tests assert `name-{key}`.
        let key = channel_key.strip_prefix("ch-")?;
        Some(format!("name-{key}"))
    }
}

// ---------- sink mock ----------

/// In-memory sink mock (the same shape the apps side implements): collects settlement /
/// observation events for assertions.
#[allow(dead_code)] // the retry_wiring binary does not use this mock
#[derive(Default)]
pub struct VecSink(Mutex<Vec<UsageEvent>>);

impl SettleSink for VecSink {
    fn submit(&self, event: UsageEvent) {
        self.0.lock().push(event);
    }
}

#[allow(dead_code)] // same as above: the events accessor is not used by every binary
impl VecSink {
    pub fn events(&self) -> Vec<UsageEvent> {
        self.0.lock().clone()
    }
}
