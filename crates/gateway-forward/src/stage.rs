//! `stage` — `ForwardStage`: wires forwarding into the pipeline
//!
//! Two drive modes:
//! 1. **Single forward** (default, `DispatchStage` → `ForwardStage` chain): assemble a
//!    `ForwardTask` from ctx.route (written by `DispatchStage`) and call `forward_once` to
//!    send one upstream request;
//! 2. **Retry loop** (after `with_retry` injects dispatch + RetryPolicy): handle
//!    ignores ctx.route and drives `gateway_dispatch::retry::run_retry_loop`
//!    itself (select candidate → attempt → health report → exclude tried → select
//!    again); the winning candidate's response goes through `commit_forwarded`
//!    into ctx.upstream (non-streaming) or straight back to the client (streaming).
//!
//! Retry orchestration and health reporting all live inside the
//! `gateway_dispatch::retry` loop; this stage only supplies the single-attempt
//! closure (pure IO) and writes the result into ctx.

use crate::ForwardTask;
use crate::egress::{ReqwestEgress, upstream_error};
use crate::stage_concurrency::try_acquire_concurrency;
use crate::stage_sticky::{StickyBind, sticky_key};
use crate::stage_telemetry::normalized_to_stage_error;
use crate::stream;
use crate::stream_resilience::StreamResilience;
use arc_swap::ArcSwap;
use async_trait::async_trait;
use bytes::Bytes;
use gateway_core::error::NormalizedError;
use gateway_dispatch::sticky::StickyTable;
use gateway_dispatch::{Dispatch, RetryPolicy};
use gateway_gate::concurrency::ConcurrencyState;
use gateway_metering::ledger::Ledger;
use gateway_pipeline::{BodySource, RequestCtx, SelectedRoute, Stage, StageError, StageOutcome};
use gateway_protocol_bridge::format_codec::FormatRegistry;
use gateway_proxy::ProxyManager;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Semaphore;

/// Forwarding stage — depends on egress (test mock / no proxy) or a
/// [`ProxyManager`]-leased Client.
pub struct ForwardStage {
    pub(crate) egress: Arc<dyn crate::egress::Egress>,
    pub(crate) timeouts: crate::egress::Timeouts,
    /// Single-format protocol registry (`FormatRegistry`): request/response
    /// conversion shares one codec.
    pub(crate) formats: Arc<FormatRegistry>,
    /// Injected on the production path; `None` falls back to `egress` (test mock).
    pub(crate) proxies: Option<Arc<ProxyManager>>,
    /// Dispatcher injected by `with_retry`: when `Some`, handle drives the full
    /// retry loop and ignores `ctx.route`; `None` keeps single-forward semantics
    /// (reads the route written by `DispatchStage`).
    pub(crate) dispatch: Option<Arc<dyn Dispatch>>,
    /// Retry budget, effective only while `dispatch` is `Some`.
    ///
    /// Hot-swappable handle: `ArcSwap` lets options polling replace
    /// the budget without rebuilding the stage; `handle_with_retry` reads the
    /// current value once on each loop entry (see `update_retry_policy` /
    /// `with_retry_handle`). `with_retry` keeps its value semantics and wraps it
    /// in a handle internally (existing callers need no change).
    pub(crate) retry_policy: Arc<ArcSwap<RetryPolicy>>,
    /// Pricing table injected by `with_price_table`: when `Some`, the commit
    /// point emits settlement events; `None` = no billing (behaviour as before
    /// billing was wired in).
    pub(crate) price_table: Option<Arc<dyn gateway_metering::pricing::PriceTable>>,
    /// Settlement output channel (implemented by apps: writes `usage_logs` /
    /// debits in-memory quota); injected as a pair with `price_table`.
    pub(crate) sink: Option<Arc<dyn gateway_metering::SettleSink>>,
    /// Prehold ledger from the admission track (`QuotaGate`); `None` in the coding
    /// face, where no preholds exist and the settlement commit points skip ledger
    /// operations entirely.
    pub(crate) ledger: Option<Arc<dyn Ledger>>,
    /// Global concurrency gate (v2 mount): the overall concurrency-limit
    /// semaphore injected by `with_concurrency`. `None` = unlimited (behaviour
    /// identical to not mounting it).
    pub(crate) concurrency: Option<Arc<Semaphore>>,
    /// Per-channel concurrency gate: shares one `ConcurrencyState` with the
    /// dispatcher so the P2C selector can read each channel's real available
    /// slots. `None` = unlimited (behaviour as before mounting).
    pub(crate) channel_concurrency: Option<Arc<ConcurrencyState>>,
    /// P2 streaming resilience parameters: `Some` makes streaming attempts go
    /// through the window phase (buffer before the first byte + switch candidate
    /// on stream break), consumed by the attempt closure of `handle_with_retry`;
    /// `None` = no-window passthrough (the existing streaming semantics, bytes
    /// sent out immediately).
    ///
    /// Defaults to `None`: the window phase changes streaming attempt timing (at
    /// most one window of waiting before the first byte), so it is a behaviour
    /// change and is enabled explicitly by
    /// [`ForwardStage::with_stream_resilience`]; pass
    /// [`StreamResilience::default`] for a 2s window / 15s heartbeat / 3
    /// candidate switches. The heartbeat (`heartbeat_ms`) belongs to the Live
    /// phase and is not consumed this round.
    pub(crate) stream_resilience: Option<StreamResilience>,
    /// Channel token budget (soft budget): shares one `TokenBudgetState` with the
    /// dispatcher (TPM cap + in-flight token count + completion EMA); the forward
    /// side only pre-claims at attempt level (`preclaim`) and refunds the
    /// difference at the settlement point (`settle_actual`) — both mounted by
    /// [`ForwardStage::with_token_budget`], with `preclaim_token_budget` called in
    /// both the attempt closure and the single-pass handle. `None` = not mounted,
    /// behaviour identical item by item to the status quo (regression invariant,
    /// design §4 invariant 4).
    pub(crate) token_budget: Option<Arc<gateway_dispatch::token_budget::TokenBudgetState>>,
    /// Prefix-cache sticky binding table (design prompt-cache-sticky-routing
    /// §3.3): when the settlement commit point sees a measured `counts.cached > 0`
    /// on a successful attempt, it binds (public model × request prefix) to the
    /// winning channel; later requests with the same prefix get a prior boost from
    /// the read side looking up the same table. `None` = not mounted, the write
    /// side is skipped entirely, behaviour byte-for-byte identical to the status
    /// quo (design §4 invariant 2: the `sticky_enabled=false` regression switch).
    pub(crate) sticky: Option<Arc<StickyTable>>,
    /// Response cache (on by default via `with_response_cache`): byte-identical
    /// non-streaming requests replay the previous upstream answer, settled
    /// exactly like the original call. `None` = not mounted, every request
    /// forwards upstream byte-for-byte as before.
    pub(crate) response_cache: Option<Arc<gateway_cache::ResponseCache>>,
    /// Filter word list (`config.api.security` `words`, env
    /// `FERRITE_SECURITY_JSON`; empty list dormant by default): `handle` filters
    /// the materialized request body once, against the client's original text.
    /// `None` or an empty list ⇒ the whole request-side filter is skipped at zero
    /// hot-path cost (byte-for-byte passthrough, identical to no filter).
    /// Response / streaming-side filtering (`StreamFilter` wiring) awaits a later
    /// enhancement PR and is not implemented this round.
    pub(crate) word_filter: Option<Arc<gateway_security::WordFilter>>,
    /// Request-side filter switch (`config.api.security` `filter_request`,
    /// defaults to true): forms a double gate with the word list — when `false`,
    /// request-side filtering is skipped entirely even with a mounted list
    /// (`filter_response` is response-side and not consumed this round).
    pub(crate) filter_request: bool,
}

/// Internal carrier for a failed single attempt: [`NormalizedError`] + the P1-C
/// degradation flag.
///
/// Not part of the contract: degraded is a local fact of the forward lease and
/// the node ledger; only the attempt closure of `handle_with_retry` consumes it,
/// and it is invisible to clients.
#[derive(Debug)]
pub(crate) struct AttemptError {
    pub(crate) error: NormalizedError,
    /// The attempt's lease fell back to a direct connection (`Lease.node_id == 0`)
    /// and every proxy node bound to this channel is in node cooldown — direct is
    /// a degraded fallback, not the expected path (P1-C dual-ledger bridge).
    pub(crate) degraded: bool,
}

impl ForwardStage {
    /// Uses the injected [`crate::egress::Egress`] (tests, or a deployment without
    /// a proxy pool).
    pub fn new(egress: Arc<dyn crate::egress::Egress>, formats: Arc<FormatRegistry>) -> Self {
        Self {
            egress,
            timeouts: crate::egress::Timeouts::default(),
            formats,
            proxies: None,
            dispatch: None,
            retry_policy: Arc::new(ArcSwap::from_pointee(RetryPolicy::default())),
            price_table: None,
            sink: None,
            ledger: None,
            concurrency: None,
            channel_concurrency: None,
            stream_resilience: None,
            token_budget: None,
            sticky: None,
            response_cache: None,
            word_filter: None,
            filter_request: true,
        }
    }

    /// Leases an egress Client for model requests by `SelectedRoute.channel_key`.
    #[must_use]
    pub fn with_proxies(mut self, proxies: Arc<ProxyManager>) -> Self {
        self.proxies = Some(proxies);
        self
    }

    /// Overrides the egress timeout configuration (default
    /// [`crate::egress::Timeouts::default`]).
    ///
    /// The assembly side calls this after reading values from the PG `options`
    /// table; `first_byte_ms` decides how long a hung upstream is tolerated before
    /// it is failed over to another candidate — the key knob for scheduling feel.
    #[must_use]
    pub fn with_timeouts(mut self, timeouts: crate::egress::Timeouts) -> Self {
        self.timeouts = timeouts;
        self
    }

    /// Mounts the P2 streaming resilience window phase (not mounted by default,
    /// i.e. no-window passthrough).
    ///
    /// `Some(cfg)` enables it: after response headers arrive, each streaming
    /// attempt of `handle_with_retry` first accumulates upstream bytes into a
    /// window of `cfg.window_ms`; a break inside the window (EOF without a
    /// completion marker / stream error) discards the whole attempt and retries
    /// another candidate, completing inside the window drains the buffer into one
    /// whole response, and window expiry splices the buffer with the remaining
    /// stream into a new body passed through. `cfg` set to
    /// [`StreamResilience::default`] gives a 2s window / 15s heartbeat / 3
    /// candidate switches; `None` returns to unbuffered passthrough semantics.
    ///
    /// Where the parameters come from (apps reading the config `[stream]`
    /// section) is left to later wiring; production currently enables this with
    /// [`StreamResilience::default`]. The heartbeat (`heartbeat_ms`) belongs to
    /// the Live phase and is not consumed this round.
    #[must_use]
    pub fn with_stream_resilience(mut self, cfg: Option<StreamResilience>) -> Self {
        self.stream_resilience = cfg;
        self
    }

    /// Mounts the library-side settlement channel: `pt` prices, `sink` lands
    /// [`gateway_core::records::UsageEvent`].
    ///
    /// The two fields are injected as a pair (consumed by the same
    /// `commit_forwarded` settlement point); with `Some`, streaming and
    /// non-streaming responses emit settlement events at the commit point, and
    /// not calling this builder keeps the no-billing behaviour. Authoritative
    /// charging still belongs to apps (usage.rs); this channel only produces
    /// events — whether the local balance is also debited is decided by the sink
    /// implementation, avoiding a double charge.
    #[must_use]
    pub fn with_price_table(
        mut self,
        pt: Arc<dyn gateway_metering::pricing::PriceTable>,
        sink: Arc<dyn gateway_metering::SettleSink>,
    ) -> Self {
        self.price_table = Some(pt);
        self.sink = Some(sink);
        self
    }
    /// Mounts the prehold ledger from the admission track (`QuotaGate`). `None` means
    /// no preholds exist (coding face) and the settlement commit points skip ledger
    /// operations entirely; behavior is unchanged.
    #[must_use]
    pub fn with_ledger(mut self, ledger: Option<Arc<dyn Ledger>>) -> Self {
        self.ledger = ledger;
        self
    }

    /// Mounts the channel token budget (soft budget): shares one
    /// [`gateway_dispatch::token_budget::TokenBudgetState`] with the dispatcher.
    ///
    /// Each attempt (retry-mode attempt closure / single-mode handle) pre-claims
    /// the estimated in-flight tokens before `forward_task` (`admit_window`
    /// window check + `preclaim` bookkeeping); the handle is attached to the
    /// winning attempt's [`Forwarded`] (same point as `channel_permit`) and
    /// released by RAII when the stream ends / is dropped for streaming, or with
    /// the commit scope for non-streaming; the settlement commit point
    /// `settle_actual` refunds the difference (idempotent, independent of Drop
    /// order). When this builder is not called (`None`) the whole block is
    /// skipped, behaviour identical item by item to the status quo.
    #[must_use]
    pub fn with_token_budget(
        mut self,
        state: Arc<gateway_dispatch::token_budget::TokenBudgetState>,
    ) -> Self {
        self.token_budget = Some(state);
        self
    }

    /// Mounts the prefix-cache sticky binding table: shares the same `Arc` as the
    /// read side — `DispatchStage::with_sticky_table` for single-pass routing and
    /// this stage's own `sticky_channel_for` under the retry loop. The write side
    /// (this stage's settlement commit point) writes entries and the read side
    /// (route selection) looks them up; only same-source sides can hit. Not
    /// calling this builder (`None`) ⇒ the write side is skipped entirely and
    /// settlement semantics are unchanged (design §4 invariant 2).
    #[must_use]
    pub fn with_sticky_table(mut self, table: Arc<StickyTable>) -> Self {
        self.sticky = Some(table);
        self
    }

    /// Mounts the response cache (on by default via assembly; non-streaming only):
    /// requests replay the previous upstream answer at the top of `handle`,
    /// settled exactly like the original call. Not calling this builder (`None`)
    /// ⇒ the read and write paths are skipped entirely and behaviour is
    /// byte-for-byte identical to the status quo. See
    /// `crate::stage_cache` for the read / write placement rules.
    #[must_use]
    pub fn with_response_cache(mut self, cache: Arc<gateway_cache::ResponseCache>) -> Self {
        self.response_cache = Some(cache);
        self
    }

    /// Mounts the filter word list (`config.api.security` `words`): `handle`
    /// filters the materialized request body once against the client's original
    /// text, with the same semantics in both drive modes (the mount point precedes
    /// `ForwardTask` construction, so protocol adaptation / estimation / sticky
    /// caching all operate on the filtered body).
    ///
    /// Not calling this builder (or an empty word list) ⇒ request-side filtering
    /// is skipped entirely at zero hot-path cost. Response / streaming-side
    /// filtering awaits a later enhancement PR.
    #[must_use]
    pub fn with_word_filter(mut self, filter: Arc<gateway_security::WordFilter>) -> Self {
        self.word_filter = Some(filter);
        self
    }

    /// Overrides the request-side filter switch (`config.api.security`
    /// `filter_request`, defaults to true): when `false`, request-side filtering
    /// is skipped entirely even with a mounted word list.
    #[must_use]
    pub fn with_word_filter_request(mut self, filter_request: bool) -> Self {
        self.filter_request = filter_request;
        self
    }

    /// Builds this request's sticky binding input (used by the settlement commit
    /// point's write side).
    ///
    /// Table not mounted (`None`, `with_sticky_table` not called) or no key source
    /// (no resolved public model / neither a session hint nor a raw body) ⇒
    /// `None`, and the write side is skipped entirely.
    pub(crate) fn sticky_bind(
        &self,
        ctx: &RequestCtx,
        candidate: &SelectedRoute,
        req_body: &Bytes,
    ) -> Option<Arc<StickyBind>> {
        let table = self.sticky.as_ref()?;
        Some(Arc::new(StickyBind {
            table: Arc::clone(table),
            key: sticky_key(ctx, req_body)?,
            channel_key: candidate.unit.channel_key.clone(),
        }))
    }

    /// Read-side sticky lookup (design §3.2, first step of route selection,
    /// symmetric to the write side [`Self::sticky_bind`]): returns the channel key
    /// this request should stick to. Table not mounted (`None`, `with_sticky_table`
    /// not called) / no key source / no hit ⇒ `None`, and the whole path degrades
    /// to no stickiness (behaviour byte-for-byte identical to the status quo,
    /// design §4 invariant 2).
    ///
    /// The key is **same-source** as the write side's settlement commit point
    /// ([`sticky_key`]: the public model is `ctx.requested_model`, the session
    /// hint is `ctx.session_hint` → the `x-session-id` header, and the fingerprint
    /// is the raw bytes of `raw_body`); different sources on the two sides would
    /// make stickiness never hit (design §3.1 ruling).
    ///
    /// **Computed once per request**: the production entry
    /// [`ForwardStage::handle_with_retry`] calls this before
    /// [`gateway_dispatch::retry::run_retry_loop`], and the resulting key is passed
    /// as the 4th argument through every loop round to `Dispatch::select`
    /// unchanged (not recomputed inside the loop or per attempt). The consumer is
    /// the selector's **prior** boost: on a hit, with the channel still in the
    /// candidate pool, it only raises the prior and does not override exclude /
    /// cooldown vetoes (design §4 invariant 1).
    pub(crate) fn sticky_channel_for(&self, ctx: &RequestCtx, raw_body: &Bytes) -> Option<String> {
        let table = self.sticky.as_ref()?;
        table.lookup(&sticky_key(ctx, raw_body)?, table.now_ms())
    }

    /// Pre-claims this attempt's estimated in-flight tokens (attempt level,
    /// mounted at the same point as the per-channel concurrency gate: after
    /// `try_acquire_concurrency` and before `forward_task`; shared by the retry
    /// and single-pass paths).
    ///
    /// - `token_budget` not mounted (`None`) ⇒ skip entirely, return `Ok(None)`
    ///   (behaviour = status quo);
    /// - minute window full (`admit_window` rejects) ⇒ return a rejection shaped
    ///   exactly like a full concurrency slot (429 retryable,
    ///   `channel_scoped: false`) — the caller does **not** call `submit_failed`
    ///   (a full window is not an upstream error and has no `status_code` to
    ///   report); the health side goes through the existing 429/Throttled channel
    ///   (retry mode reports Throttled, records no Fatal, enters no channel
    ///   failure cooldown — token-full ≠ channel-bad);
    /// - window admits ⇒ `preclaim` records the in-flight estimate and returns an
    ///   RAII handle: a failed attempt refunds it with the scope / drop (no
    ///   settle), a successful one attaches it to [`Forwarded`] to travel with
    ///   the response.
    pub(crate) fn preclaim_token_budget(
        &self,
        task: &ForwardTask,
    ) -> Result<Option<gateway_dispatch::token_budget::TokenHandle>, NormalizedError> {
        let Some(token_budget) = self.token_budget.as_ref() else {
            return Ok(None);
        };
        let channel_key = task.candidate.unit.channel_key.as_str();
        let ema = token_budget.completion_ema(channel_key);
        let est = self.estimate_forward_tokens(task, ema);
        if !token_budget.admit_window(channel_key, est) {
            return Err(crate::rate_limited("channel token budget window full"));
        }
        Ok(Some(token_budget.preclaim(channel_key, est)))
    }

    /// Estimates this attempt's in-flight tokens (`est = prompt_est + fwd_est`),
    /// computed on the forward side so dispatch does not depend on metering:
    /// - `prompt_est` = `gateway_metering::estimate::estimate_prompt_detail`
    ///   over the adapted body (the request body after protocol conversion, same
    ///   basis as `forward_once`); conversion failure ⇒ 0 (a soft budget only
    ///   under-estimates, never over-estimates, and `forward_once` rejects the
    ///   request anyway);
    /// - `fwd_est` = `min(max_tokens ?? max_completion_tokens, ema)`; body JSON
    ///   parse failure or both fields missing ⇒ `ema` (the channel-level
    ///   completion EMA of `TokenBudgetState`, calibrated at the settle commit
    ///   point with α=0.3);
    /// - `est = prompt_est + fwd_est`.
    fn estimate_forward_tokens(&self, task: &ForwardTask, ema: u64) -> u64 {
        let inbound = gateway_protocol_bridge::adaptor::Protocol::from_kind(task.inbound_format);
        let upstream_format =
            gateway_protocol_bridge::adaptor::Protocol::from_provider_type(&task.provider_type);
        let adapted = self.formats.translate_request(
            inbound,
            upstream_format,
            crate::pipeline::rewrite_upstream_model(&task.body, &task.candidate.upstream_model),
        );
        let (prompt_est, fwd_cap) = adapted
            .as_ref()
            .map_or_else(|_| (0, None), Self::estimate_body_budget);
        let fwd_est = fwd_cap.map_or(ema, |cap| cap.min(ema));
        prompt_est + fwd_est
    }

    /// Parses the adapted body once and derives both budget inputs from the same
    /// tree: the prompt estimate (character-class weights, raw-length fallback for
    /// shapes with no billable text) and the explicit output bound. Same lenient
    /// parsing discipline as `body_wants_stream`: non-UTF-8 / not JSON ⇒
    /// `(0, None)` (a soft budget only under-estimates); field missing /
    /// non-integer ⇒ no explicit bound, the caller falls back to the EMA.
    ///
    /// Cost: one JSON parse, O(n) in body bytes. The previous form ran three full
    /// passes over the same body (a validity parse, a hand-written character scan
    /// and a re-parse for `max_tokens`); this is the only place both values are
    /// needed, so the tree is parsed and walked exactly once here.
    fn estimate_body_budget(body: &bytes::Bytes) -> (u64, Option<u64>) {
        let Ok(s) = std::str::from_utf8(body) else {
            return (0, None);
        };
        let Ok(value) = serde_json::from_str::<serde_json::Value>(s) else {
            return (0, None);
        };
        let prompt = gateway_metering::estimate::estimate_prompt_detail(&value)
            .map_or(gateway_metering::estimate::raw_length_tokens(s), |e| {
                e.tokens
            });
        // Output-bound dialects are not wired yet: Responses `max_output_tokens`
        // and Gemini `generationConfig.maxOutputTokens` fall back to the EMA, as
        // before this rewrite — tracked with the prompt-side carriers as issue #19.
        let bound = value
            .get("max_tokens")
            .and_then(serde_json::Value::as_u64)
            .or_else(|| {
                value
                    .get("max_completion_tokens")
                    .and_then(serde_json::Value::as_u64)
            });
        (prompt, bound)
    }

    /// Request-side filter words (double gate): acts only when both the
    /// `filter_request` switch and a non-empty `word_filter` (`is_empty()`) hold;
    /// otherwise the original text is passed through (zero hot-path cost).
    ///
    /// - Either gate fails ⇒ return the original `Bytes` (no allocation);
    /// - a non-UTF-8 body (binary) cannot be filtered word by word ⇒ pass through
    ///   unchanged, never rewritten;
    /// - allocate anew only on a hit (`filter` returns `Cow::Owned`); no hit
    ///   reuses the original `Bytes` (`Cow::Borrowed` zero-allocation hot path).
    fn apply_word_filter(&self, body: Bytes) -> Bytes {
        if !self.filter_request {
            return body;
        }
        let Some(filter) = self.word_filter.as_ref() else {
            return body;
        };
        if filter.is_empty() {
            return body;
        }
        let Ok(text) = std::str::from_utf8(&body) else {
            return body;
        };
        match filter.filter(text) {
            std::borrow::Cow::Borrowed(_) => body,
            std::borrow::Cow::Owned(replaced) => {
                tracing::debug!("word filter applied to request body");
                Bytes::from(replaced.into_bytes())
            }
        }
    }

    /// Assembles one attempt's [`ForwardTask`] from the candidate and the request
    /// body already read.
    ///
    /// Field assembly matches the original single-pass path: headers stay empty
    /// (the gate already sanitized them; passthrough headers are assembled by
    /// bin/api), and `provider_type/extra_headers` come from the candidate's full
    /// parse.
    pub(crate) fn build_task(
        candidate: &SelectedRoute,
        path: String,
        body: Bytes,
        stream: bool,
        inbound_format: gateway_pipeline::ProtocolKind,
    ) -> ForwardTask {
        ForwardTask {
            candidate: candidate.clone(),
            path,
            headers: vec![],
            body,
            stream,
            provider_type: candidate.provider_type.clone(),
            extra_headers: crate::adapter::extra_headers_from_settings(&candidate.settings),
            // Client-declared protocol: decode the request direction, encode the
            // response direction.
            inbound_format,
        }
    }

    pub(crate) async fn forward_task(
        &self,
        task: &ForwardTask,
        timeouts: &crate::egress::Timeouts,
    ) -> Result<crate::Forwarded, AttemptError> {
        let Some(proxies) = self.proxies.as_ref() else {
            return crate::pipeline::forward_once(task, &*self.egress, &self.formats, timeouts)
                .await
                .map_err(|error| AttemptError {
                    error,
                    degraded: false,
                });
        };

        let lease = proxies.acquire(&task.candidate.unit.channel_key);
        let node_id = lease.node_id;
        // Two-state egress: a reqwest Client (direct/http/socks5) or a protocol
        // adapter (SS/Trojan/VLESS/VMess — meow dial_tcp bridged to HTTP via
        // adapter_egress).
        let result = if let Some(adapter) = lease.adapter() {
            let egress = crate::adapter_egress::AdapterEgress::new(
                Arc::new(crate::adapter_egress::AdapterDialer::new(adapter)),
                Duration::from_secs(5),
            );
            crate::pipeline::forward_once(task, &egress, &self.formats, timeouts).await
        } else if let Some(client) = lease.reqwest_client() {
            let egress = ReqwestEgress::with_client((*client).clone(), Duration::from_secs(5));
            crate::pipeline::forward_once(task, &egress, &self.formats, timeouts).await
        } else {
            proxies.feedback(node_id, 502, false);
            return Err(AttemptError {
                error: upstream_error(
                    502,
                    true,
                    "lease produced neither reqwest client nor adapter",
                ),
                // A node was selected yet produced no egress = the node ledger is
                // not at fault (config/assembly error); do not record degradation.
                degraded: false,
            });
        };
        match &result {
            Ok(forwarded) => proxies.feedback(node_id, forwarded.status, false),
            Err(err) => {
                let transport_err = err.status == 502 || err.status == 504;
                proxies.feedback(node_id, err.status, transport_err);
            }
        }
        // P1-C: read both ledger snapshots on the failure path only (a successful
        // direct connection records nothing — see the mount points in spec P1-C).
        result.map_err(|error| AttemptError {
            degraded: node_id == 0 && self.channel_proxy_degraded(&task.candidate.unit.channel_key),
            error,
        })
    }

    /// Whether every proxy node bound to the channel is in node cooldown (and the
    /// channel is not "unbound to begin with").
    ///
    /// All cooled ⇒ [`ProxyManager::acquire`] necessarily falls back to a direct
    /// connection (`node_id == 0`); then the channel's "direct success/failure"
    /// would hide the fact that the proxy ledger has judged it dead. The caller
    /// uses this to record the failure on route-unit health (see the attempt
    /// closure of `handle_with_retry`).
    fn channel_proxy_degraded(&self, channel_key: &str) -> bool {
        let Some(proxies) = self.proxies.as_ref() else {
            return false;
        };
        let cooled: std::collections::HashSet<i64> = proxies
            .node_cooldowns()
            .into_iter()
            .map(|(id, _)| id)
            .collect();
        let bound = proxies.channel_node_ids(channel_key);
        !bound.is_empty() && bound.iter().all(|id| cooled.contains(id))
    }
}

#[async_trait]
impl Stage for ForwardStage {
    fn name(&self) -> &'static str {
        "forward"
    }

    async fn handle(&self, ctx: &mut RequestCtx) -> Result<StageOutcome, StageError> {
        // Read the body (replayable: both InMemory/OnDisk yield a fresh reader).
        let body = read_request_body(ctx).await?;
        // Filter words (`config.api.security` `words`, dormant by default): after the
        // body is materialized, and before any build_task / estimation / sticky caching,
        // filter once against the client's original text. Double gate = the `filter_request`
        // switch + a non-empty word list; a non-UTF-8 body (binary) passes through unchanged.
        // Both drive modes share this filtered body (same semantics). Response / streaming-side
        // filtering awaits a later enhancement PR.
        let body = self.apply_word_filter(body);
        // Streaming is decided by the request body's `stream` field — a "stream"
        // substring in the path is not a protocol signal.
        let stream = stream::body_wants_stream(&body);

        // Response-cache short-circuit (opt-in, non-streaming only, both drive
        // modes): after the gates (identity / model / quota decided) and before
        // any upstream work. A hit replays the stored answer and settles it
        // exactly like the original call; no health report, no sticky write, no
        // concurrency / token accounting — none of those signals exist for a
        // replay. `cache_key` also feeds the write side (a miss stores its
        // response at the settlement commit point).
        let cache_key = self.response_cache_key(ctx, &body);
        if !stream
            && let Some(key) = cache_key.as_ref()
            && let Some(hit) = self.lookup_response_cache(key)
        {
            return Ok(self.commit_cache_hit(ctx, &hit));
        }

        // Once with_retry is wired in, drive the whole retry loop here and
        // ignore ctx.route entirely.
        if let Some(dispatch) = self.dispatch.clone() {
            return self.handle_with_retry(ctx, dispatch, body, stream).await;
        }

        // Single mode: the candidate was written by DispatchStage and is already fully
        // resolved (secret / upstream_model / provider_type / settings); forward neither
        // re-queries the snapshot nor fabricates one.
        let Some(candidate) = ctx.route.clone() else {
            return Err(StageError::Internal(anyhow::anyhow!(
                "dispatch stage did not run before forward"
            )));
        };
        let task = Self::build_task(
            &candidate,
            ctx.request.path.clone(),
            body,
            stream,
            ctx.request.inbound_protocol,
        );

        // Timing start for failure observation events (single mode): covers the
        // concurrency-gate wait and the upstream request; the success path's duration_ms
        // is measured on the response side, so only the failure path is filled in here.
        let started = std::time::Instant::now();
        // Concurrency gate (v2 mount): try_acquire before forward_task; the permit lives
        // for this function's scope — the success path holds it until commit_forwarded
        // completes (covering response body reading / the settlement window), and an early
        // failure return drops it at once. Slot full: no submit_failed (a concurrency
        // rejection is not an upstream error and has no status_code to report); short-circuit
        // with 429 semantics.
        let _permit = match try_acquire_concurrency(self.concurrency.as_ref()) {
            Ok(permit) => permit,
            Err(e) => return Err(normalized_to_stage_error(e)),
        };
        // Channel token budget pre-claim (same point as the per-channel concurrency gate):
        // not mounted = skip entirely; window full = a 429 rejection shaped like a full slot
        // (no submit_failed, no Fatal); the handle is attached to the winning response (see
        // below) and fully refunded on upstream failure when this scope drops.
        let token_handle = match self.preclaim_token_budget(&task) {
            Ok(handle) => handle,
            Err(e) => return Err(normalized_to_stage_error(e)),
        };
        let forwarded = match self.forward_task(&task, &self.timeouts).await {
            Ok(mut f) => {
                // Assigned at the same point as `channel_permit`: the handle's lifetime
                // follows the response (unfold stream state for streaming, the commit scope
                // for non-streaming; inside commit_forwarded the settlement commit point runs
                // settle_actual for the refund before RAII release).
                f.token_handle = token_handle;
                f
            }
            // Upstream failure: the pre-claimed handle is fully refunded when the scope
            // below drops before returning (no settle).
            Err(a) => {
                // Upstream answered/transport failed and a candidate exists: record one
                // zero-cost observation event, then short-circuit.
                // (P1-C: the AttemptError carrier holds the degraded flag; single mode does
                // not enter the retry closure and only consumes its inner NormalizedError.)
                self.submit_failed(ctx, &candidate, &a.error, stream, started.elapsed());
                return Err(normalized_to_stage_error(a.error));
            }
        };

        self.commit_forwarded(
            ctx,
            forwarded,
            &task.candidate,
            task.stream,
            &task.body,
            cache_key.as_ref(),
        )
        .await
    }
}

/// Reads this request's body (`InMemory` is cloned, `OnDisk` is read back by length).
///
/// Replayable: `handle` reads it once and every attempt clones the returned
/// `Bytes`.
async fn read_request_body(ctx: &RequestCtx) -> Result<Bytes, StageError> {
    match &ctx.request.body {
        BodySource::InMemory(body) => Ok(body.clone()),
        BodySource::OnDisk { path, len } => {
            use tokio::io::AsyncReadExt;
            let mut file = tokio::fs::File::open(path)
                .await
                .map_err(|e| StageError::Internal(anyhow::anyhow!("open body: {e}")))?;
            let Ok(len) = usize::try_from(*len) else {
                return Err(StageError::Internal(anyhow::anyhow!(
                    "on-disk body length exceeds usize"
                )));
            };
            let mut buf = vec![0u8; len];
            file.read_exact(&mut buf)
                .await
                .map_err(|e| StageError::Internal(anyhow::anyhow!("read body: {e}")))?;
            Ok(Bytes::from(buf))
        }
    }
}
