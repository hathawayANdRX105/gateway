pub mod candidate;
pub mod failure_scope;
pub mod health;
pub mod latency_digest;
pub mod ratelimit;
pub mod retry;
pub mod selector;
pub mod stage;
pub mod sticky;
pub mod tdigest;
pub mod token_budget;

pub use candidate::{Candidate, STATUS_ENABLED, resolve_candidate};
pub use failure_scope::{FailureScope, classify_channel_scope};
/// Channel / route-unit catalog snapshot — defined on the injection surface so the
/// admin plane can build and read it without depending on this engine.
pub use gateway_core::dispatch::Snapshot;
pub use health::{
    AttemptReport, FailureClass, HealthState, HealthTable, MemoryHealthTable, TpsSample,
};
pub use latency_digest::LatencyDigestTable;
pub use ratelimit::{RateLimitSpec, SlidingWindow};
pub use retry::{
    Attempt, AttemptOutcome, Failover, FakeTimeControl, ModelRetryPolicies, RealTimeControl,
    RetryLoop, RetryPolicy, TimeControl, run_retry_loop,
};
pub use selector::{DEFAULT_EXPLORE_RATE, DEFAULT_STICKY_BOOST, P2CSelector, Selector};
pub use stage::DispatchStage;
pub use sticky::{StickyEntry, StickyKey, StickyTable};
pub use token_budget::{COMPLETION_EMA_INIT, TokenBudgetState, TokenHandle};

use arc_swap::ArcSwap;
use gateway_core::records::RouteUnitRecord;
use gateway_gate::concurrency::ConcurrencyState;
use std::collections::HashMap;
use std::sync::Arc;

/// Default millisecond clock for this crate's tables/schedulers (`chrono::Utc`; negative values
/// clamped to 0).
///
/// The assembly points (`Dispatcher` / `MemoryHealthTable` / `SlidingWindow` /
/// `TokenBudgetState` / `LatencyDigestTable`) share one convention, avoiding a copy of the same
/// expression in several places. `sticky::StickyTable` has its own convention (negative →
/// `u64::MAX`) and does not share this function.
pub(crate) fn utc_now_ms() -> u64 {
    chrono::Utc::now().timestamp_millis().max(0) as u64
}

pub trait Dispatch: Send + Sync {
    /// `sticky_channel_key`: the sticky channel key for this request (`None` = not sticky).
    /// When it hits and is inside the candidate pool, only its **prior** is raised (P2C boost);
    /// it does not bypass `exclude` / cooldown vetoes.
    fn select(
        &self,
        group: &str,
        public_model: &str,
        exclude: &[String],
        sticky_channel_key: Option<&str>,
    ) -> Result<Candidate, DispatchError>;
    /// Report one attempt result to the health table. The payload is bundled in the
    /// [`AttemptReport`] struct (constructor `AttemptReport::new` + `with_tps` builder):
    /// `outcome` is `Ok(status code)` (classified by status code: 2xx→Success / 429→Throttled /
    /// 5xx→Fatal / 401/403→escalation counter (3 consecutive→Fatal) / other→Neutral) or
    /// `Err(FailureClass)` (transport-layer / client failure); `latency_ms` is this attempt's
    /// upstream round-trip time (unobserved = `None`); `retry_after_ms` is the upstream 429
    /// `Retry-After` hint (ms, meaningful only under Throttled semantics; other cases may pass
    /// `None` and the value is dropped); `tps` is an optional generation-throughput sample
    /// (tok/s; non-streaming / interrupted = `None`), merged into `HealthState.tps_ema`.
    fn report(&self, unit_key: &str, report: AttemptReport);

    /// Report a **late** generation-throughput sample (at stream end, see the `tps` docs of
    /// [`Dispatch::report`]): only merges into `HealthState.tps_ema`, leaving
    /// cooldown/streak/EWMA untouched. Since ticket 2 the forward side calls this at stream
    /// end; before that, mocks may no-op.
    fn report_tps(&self, unit_key: &str, sample: TpsSample);

    /// Look up a channel display name (snapshot channels map, key = channel UUID).
    ///
    /// After DispatchStage picks a route it uses this to fill in the channel attribution name
    /// on ctx; returns None when the snapshot is not ready or the channel has been removed
    /// (callers degrade to carrying the key only).
    /// Default None: `Candidate`/`SelectedRoute` do not carry a channel name (see ctx.rs);
    /// only an implementation holding the snapshot can look it up; non-`Dispatcher`
    /// implementations do not take on that responsibility.
    fn channel_name(&self, _channel_key: &str) -> Option<String> {
        None
    }
}

#[derive(Debug, thiserror::Error)]
pub enum DispatchError {
    /// The snapshot has no enabled route unit for this model (and no fallback) — missing
    /// configuration, terminal 404. The client should not retry (the model really is not
    /// configured); semantics stay NOT_FOUND.
    #[error("model not configured for {group}/{model}")]
    ModelUnknown { group: String, model: String },
    /// The model has route units, but they are all cooling / disabled / already tried by this
    /// request — transient, not a 404.
    /// `retry_after_ms` = the shortest cooldown recovery among the model's enabled channels
    /// (0 = no natural recovery, not caused by cooling, e.g. all disabled / missing key —
    /// waiting is useless → fail directly).
    #[error("all candidates unavailable for {group}/{model}, retry after {retry_after_ms}ms")]
    AllUnavailable {
        group: String,
        model: String,
        retry_after_ms: u64,
    },
    #[error("all candidates rate-limited for {group}/{model}")]
    RateLimited { group: String, model: String },
    #[error("catalog snapshot not ready")]
    SnapshotNotReady,
    /// Request-level time budget (45s) exhausted with no successful candidate — retryable 504
    /// with Retry-After (503 without Retry-After when `retry_after_ms` is 0).
    /// `retry_after_ms` = the current shortest cooldown recovery (fed to Retry-After).
    #[error("budget exhausted for {group}/{model}, retry after {retry_after_ms}ms")]
    BudgetExhausted {
        group: String,
        model: String,
        retry_after_ms: u64,
    },
}

pub fn candidates_from_snapshot<'a>(
    units: &'a [RouteUnitRecord],
    group: &str,
    model: &str,
) -> Vec<&'a RouteUnitRecord> {
    units
        .iter()
        .filter(|u| u.status == STATUS_ENABLED && u.group == group && u.public_model == model)
        .collect()
}

/// Fallback candidates: when the group has **no** enabled route unit for the model, fall back to
/// enabled channels with at least one key whose settings are marked `fallback: true` (the PG
/// version of api-hub's `[aliases] default`).
///
/// The model name passes through unchanged (no rename) — the semantics of a fallback channel
/// is "accept everything and dispatch it yourself".
/// key_index is fixed at 0: which key a multi-key fallback channel uses is out of scope here
/// (TODO(#158)); no settings-driven key selection exists yet.
/// Channels not marked fallback do not participate, preserving the old behaviour.
fn fallback_units(snap: &Snapshot, group: &str, public_model: &str) -> Vec<RouteUnitRecord> {
    snap.channels
        .values()
        .filter(|c| {
            c.status == STATUS_ENABLED
                && !c.keys.is_empty()
                && c.groups.iter().any(|g| g == group)
                && c.settings.get("fallback").and_then(|v| v.as_bool()) == Some(true)
        })
        .map(|c| RouteUnitRecord {
            meta: gateway_core::records::SyncMeta {
                key: format!("{}#fallback", c.meta.key),
                schema_version: c.meta.schema_version,
                logical_version: c.meta.logical_version,
                origin: c.meta.origin.clone(),
                updated_at: c.meta.updated_at,
            },
            group: group.to_string(),
            public_model: public_model.to_string(),
            channel_key: c.meta.key.clone(),
            key_index: 0,
            upstream_model: public_model.to_string(),
            weight: 1,
            status: STATUS_ENABLED,
        })
        .collect()
}

pub struct Dispatcher {
    snapshot: ArcSwap<Option<Arc<Snapshot>>>,
    health: Arc<MemoryHealthTable>,
    selector: P2CSelector,
    concurrency: Arc<ConcurrencyState>,
    now_ms: Box<dyn Fn() -> u64 + Send + Sync>,
    limits: ArcSwap<HashMap<String, RateLimitSpec>>,
    rl: Arc<SlidingWindow>,
    token: Option<Arc<TokenBudgetState>>,
}

/// Failure surface of rate-limit-aware extraction, used by
/// [`Dispatcher::pick_within_limits`].
enum LimitedPickError {
    /// At least one candidate in the pool was rejected by rate limiting → the upper layer
    /// reports 429.
    RateLimited,
    /// The candidate pool itself cannot produce a pick (cooling/disabled/excluded), the same
    /// error semantics as when rate limiting is not mounted.
    NoCandidate,
}

impl Dispatcher {
    /// Construct a dispatcher with the default rate limiter and the dual-choice P2C selector.
    pub fn new(
        snapshot: Option<Arc<Snapshot>>,
        health: Arc<MemoryHealthTable>,
        concurrency: Arc<ConcurrencyState>,
    ) -> Self {
        Self::with_limits(
            snapshot,
            health,
            concurrency,
            Arc::new(HashMap::new()),
            Arc::new(SlidingWindow::new()),
        )
    }

    /// Construct a dispatcher with the specified rate-limit state and the dual-choice P2C
    /// selector.
    pub fn with_limits(
        snapshot: Option<Arc<Snapshot>>,
        health: Arc<MemoryHealthTable>,
        concurrency: Arc<ConcurrencyState>,
        limits: Arc<HashMap<String, RateLimitSpec>>,
        rl: Arc<SlidingWindow>,
    ) -> Self {
        Self::with_limits_and_clock(snapshot, health, concurrency, limits, rl, utc_now_ms)
    }

    /// Construct a dispatcher with the specified clock, for deterministic tests to inject time.
    pub fn with_limits_and_clock(
        snapshot: Option<Arc<Snapshot>>,
        health: Arc<MemoryHealthTable>,
        concurrency: Arc<ConcurrencyState>,
        limits: Arc<HashMap<String, RateLimitSpec>>,
        rl: Arc<SlidingWindow>,
        now_ms: impl Fn() -> u64 + Send + Sync + 'static,
    ) -> Self {
        let dispatcher = Self {
            snapshot: ArcSwap::new(Arc::new(snapshot.clone())),
            health,
            selector: P2CSelector::new(concurrency.clone(), 2, DEFAULT_EXPLORE_RATE),
            concurrency,
            now_ms: Box::new(now_ms),
            limits: ArcSwap::new(limits),
            rl,
            token: None,
        };
        if let Some(snapshot) = snapshot.as_deref() {
            register_channel_limits(&dispatcher.concurrency, snapshot);
        }
        dispatcher
    }

    /// Mount a token budget (builder; must be called after `new` / `with_limits`): inject
    /// [`TokenBudgetState`] into the P2C selector's decision layer and, when a snapshot already
    /// exists, register the token caps from it immediately. A `Dispatcher` that never calls this
    /// stays token-neutral (regression guard: behaviour byte-for-byte identical to not mounting).
    pub fn with_token_budget(mut self, token: Arc<TokenBudgetState>) -> Self {
        let loaded = self.snapshot.load();
        if let Some(snap) = loaded.as_ref() {
            register_token_budgets(&token, snap);
        }
        self.selector = self.selector.with_token(Arc::clone(&token));
        self.token = Some(token);
        self
    }

    /// Attach the windowed t-digest latency digest table (builder): inject
    /// [`LatencyDigestTable`] into the P2C selector so the decision layer's `latency_factor`
    /// reads t-digest quantiles (design §3.2). Write-side accounting still belongs to
    /// `MemoryHealthTable` (`with_digest`, mounted during app assembly); this `Dispatcher` adds
    /// no field — it only passes the table through to the selector. When not called,
    /// `latency_factor` follows the existing EWMA path byte-for-byte (regression invariant).
    pub fn with_latency_digest(mut self, t: Arc<LatencyDigestTable>) -> Self {
        self.selector = self.selector.with_latency_digest(t);
        self
    }

    /// Assemble the sticky prior boost multiplier (builder, design
    /// prompt-cache-sticky-routing §3.2/§3.4): inject the `gateway.dispatch.sticky_boost` knob
    /// into the P2C selector.
    ///
    /// Consumed only when `Dispatch::select` receives `sticky_channel_key = Some` and the bound
    /// channel is still in the eligible pool — table not mounted / no hit ⇒ permanently dormant.
    /// Out-of-range values (non-finite / ≤0 / above the upper bound) are partly rejected by the
    /// options registry validator; the rest (e.g. huge values with no upper bound) are clamped
    /// by the selector to [`selector::MAX_STICKY_BOOST`] (this builder passes the value straight
    /// through and does not bypass that clamp). The boost enters only the prior/sampling layer;
    /// posterior likelihood and the cooldown/exclude/rate-limit/concurrency vetoes are untouched
    /// (stickiness never hard-pins a channel, design §4 invariant 1). Without this builder the
    /// selector keeps [`DEFAULT_STICKY_BOOST`].
    pub fn with_sticky_boost(mut self, boost: f64) -> Self {
        self.selector = self.selector.with_sticky_boost(boost);
        self
    }

    /// Swap the snapshot in place and refresh the concurrency slots from the new snapshot's
    /// per-channel concurrency limits.
    ///
    /// `ConcurrencyState::register_channel` only inserts missing keys and keeps the existing
    /// semaphore for an already-registered channel (so already-issued permits are never lost),
    /// which means a reload neither raises nor shrinks live limits — the existing convention.
    pub fn set_snapshot(&self, snapshot: Arc<Snapshot>) {
        register_channel_limits(&self.concurrency, &snapshot);
        // Register token caps at the same point when a token budget is mounted (symmetric with
        // the concurrency-slot refresh).
        if let Some(token) = &self.token {
            register_token_budgets(token, &snapshot);
        }
        self.snapshot.store(Arc::new(Some(snapshot)));
    }

    /// Read-only port to the current channel snapshot — returns the shared `Arc` handle (zero
    /// copy, not a data clone).
    ///
    /// Semantics: `None` before any snapshot has ever been loaded at boot (the
    /// [`DispatchError::SnapshotNotReady`] case); after loading, the old handle stays valid even
    /// if a reload overwrites it (ArcSwap atomic replacement). Aimed at the admin query surface
    /// (e.g. the channel-attribution join of `/api/gateway/health`), not used for hot-path
    /// selection (the hot path goes through [`Dispatch::select`]).
    pub fn snapshot(&self) -> Option<Arc<Snapshot>> {
        self.snapshot.load_full().as_ref().clone()
    }

    /// Route units exist but none is available right now (cooling/disabled/excluded) — not
    /// ModelUnknown. `retry_after_ms` = the shortest cooldown recovery among the model's enabled
    /// channels (0 = no natural recovery, waiting is useless → the caller decides whether to
    /// wait or fail fast). Logs a warn as the "why zero available" diagnostic surface.
    fn all_unavailable(
        &self,
        cands: &[&RouteUnitRecord],
        group: &str,
        model: &str,
        now_ms: u64,
    ) -> DispatchError {
        let retry_after_ms = self.min_cooldown_remaining(cands, now_ms);
        let cooling = cands
            .iter()
            .filter(|u| {
                self.health
                    .cooling_remaining_ms(&u.meta.key, now_ms)
                    .is_some()
            })
            .count();
        tracing::warn!(
            group,
            model,
            candidates = cands.len(),
            cooling,
            retry_after_ms,
            "all route units for this model currently unavailable (disabled/cooling/excluded)"
        );
        DispatchError::AllUnavailable {
            group: group.to_string(),
            model: model.to_string(),
            retry_after_ms,
        }
    }

    /// Shortest cooldown recovery (ms) among the model's enabled channels; no cooling channel →
    /// 0. Only cooling channels count (`cooling_remaining_ms` returns Some only while cooling) —
    /// non-cooling reasons such as disabled / missing key never self-heal, so waiting is useless
    /// and they are excluded (returning 0 lets the caller fail fast).
    fn min_cooldown_remaining(&self, cands: &[&RouteUnitRecord], now_ms: u64) -> u64 {
        cands
            .iter()
            .filter_map(|u| self.health.cooling_remaining_ms(&u.meta.key, now_ms))
            .min()
            .unwrap_or(0)
    }

    /// Extraction when rate limiting is enabled: sample P2C repeatedly, skipping candidates
    /// rejected by rate limiting (the refusal set is accounted by unit key, starting from the
    /// caller's `exclude`).
    ///
    /// Returns [`LimitedPickError::RateLimited`] if and only if this extraction rejected at
    /// least one candidate and ultimately cannot pick — distinguished from `NoCandidate` when
    /// rate limiting is not mounted, so the upper layer can map 429 / `AllUnavailable`
    /// separately.
    fn pick_within_limits<'a>(
        &self,
        cands: &[&'a RouteUnitRecord],
        exclude: &[String],
        sticky_channel_key: Option<&str>,
        now: u64,
        limits: &HashMap<String, RateLimitSpec>,
        rng: &mut dyn rand::RngCore,
    ) -> Result<&'a RouteUnitRecord, LimitedPickError> {
        let mut refused: Vec<String> = Vec::with_capacity(cands.len() + exclude.len());
        refused.extend_from_slice(exclude);
        let mut refused_by_limit = false;
        loop {
            let Some(picked) =
                self.selector
                    .pick(cands, &*self.health, &refused, sticky_channel_key, now, rng)
            else {
                return Err(if refused_by_limit {
                    LimitedPickError::RateLimited
                } else {
                    LimitedPickError::NoCandidate
                });
            };
            let admitted = limits
                .get(&picked.meta.key)
                .is_none_or(|spec| self.rl.admits(&picked.meta.key, Some(spec), now));
            if admitted {
                return Ok(picked);
            }
            refused_by_limit = true;
            refused.push(picked.meta.key.clone());
        }
    }

    pub fn set_limits(&self, limits: HashMap<String, RateLimitSpec>) {
        self.limits.store(Arc::new(limits));
    }
}

/// Register each channel's concurrency slot from `ChannelRecord.max_concurrency` in the
/// snapshot.
///
/// `max_concurrency == 0` means unlimited concurrency and is not registered — unregistered
/// channels are treated neutrally by both the concurrency gate and the P2C likelihood,
/// preserving the existing assembly convention "0 = unlimited".
fn register_channel_limits(concurrency: &ConcurrencyState, snapshot: &Snapshot) {
    for channel in snapshot.channels.values() {
        if channel.max_concurrency > 0 {
            concurrency.register_channel(&channel.meta.key, channel.max_concurrency);
        }
    }
}

/// Register token budget caps from each snapshot channel's `settings.tpm_limit` (missing key /
/// non-numeric falls back to `global_default`).
///
/// Symmetric to [`register_channel_limits`], called at the same snapshot assembly points; `0`
/// = unlimited, not registered. `settings.tpm_limit` is a channel-level override;
/// `global_default` is the default installed by `set_global_default`. Repeated registration is
/// idempotent (`TokenBudgetState::register_channel` overwrites the same key).
fn register_token_budgets(token: &TokenBudgetState, snapshot: &Snapshot) {
    for channel in snapshot.channels.values() {
        let eff = channel
            .settings
            .get("tpm_limit")
            .and_then(serde_json::Value::as_u64)
            .unwrap_or_else(|| token.global_default());
        if eff > 0 {
            token.register_channel(&channel.meta.key, eff);
        }
    }
}

fn model_unknown(group: &str, model: &str) -> DispatchError {
    DispatchError::ModelUnknown {
        group: group.to_string(),
        model: model.to_string(),
    }
}

impl Dispatch for Dispatcher {
    fn select(
        &self,
        group: &str,
        public_model: &str,
        exclude: &[String],
        sticky_channel_key: Option<&str>,
    ) -> Result<Candidate, DispatchError> {
        let loaded = self.snapshot.load();
        let Some(snap) = loaded.as_ref() else {
            return Err(DispatchError::SnapshotNotReady);
        };
        let cands = candidates_from_snapshot(&snap.units, group, public_model);
        // No enabled route unit for this model → fallback channels (settings.fallback, see
        // fallback_units).
        // If route units exist but their channels are disabled → no fallback: that is an
        // operator taking them offline deliberately, and silently rerouting would hide a
        // configuration error.
        // Neither route units nor fallback → ModelUnknown (the model really does not exist,
        // terminal 404, the client does not retry).
        let fallback_owned: Vec<RouteUnitRecord>;
        let cands: Vec<&RouteUnitRecord> = if cands.is_empty() {
            fallback_owned = fallback_units(snap, group, public_model);
            if fallback_owned.is_empty() {
                return Err(model_unknown(group, public_model));
            }
            fallback_owned.iter().collect()
        } else {
            cands
        };
        let cands: Vec<&RouteUnitRecord> = cands
            .into_iter()
            .filter(|u| {
                snap.channels
                    .get(&u.channel_key)
                    .is_some_and(|c| c.status == STATUS_ENABLED)
            })
            .collect();
        let now = (self.now_ms)();
        let limits = self.limits.load();
        // Thread-local RNG: initialized only on the first call per thread; later selects do not
        // hit system entropy (the old implementation called `StdRng::from_entropy()` on every
        // hot-path request, i.e. one system entropy read each time). The production path only
        // needs an independent random stream; test determinism comes from the explicit rng
        // injected through the Selector trait.
        let mut rng = rand::thread_rng();
        let unit = if limits.is_empty() {
            self.selector
                .pick(
                    &cands,
                    &*self.health,
                    exclude,
                    sticky_channel_key,
                    now,
                    &mut rng,
                )
                .ok_or_else(|| self.all_unavailable(&cands, group, public_model, now))?
        } else {
            match self.pick_within_limits(
                &cands,
                exclude,
                sticky_channel_key,
                now,
                &limits,
                &mut rng,
            ) {
                Ok(unit) => unit,
                Err(LimitedPickError::RateLimited) => {
                    return Err(DispatchError::RateLimited {
                        group: group.to_string(),
                        model: public_model.to_string(),
                    });
                }
                Err(LimitedPickError::NoCandidate) => {
                    return Err(self.all_unavailable(&cands, group, public_model, now));
                }
            }
        };
        let channel = snap
            .channels
            .get(&unit.channel_key)
            .ok_or_else(|| self.all_unavailable(&cands, group, public_model, now))?;
        resolve_candidate(unit, channel)
            .ok_or_else(|| self.all_unavailable(&cands, group, public_model, now))
    }

    fn report(&self, unit_key: &str, report: AttemptReport) {
        self.health.record(unit_key, report);
    }

    fn report_tps(&self, unit_key: &str, sample: TpsSample) {
        self.health.record_tps(unit_key, sample);
    }

    fn channel_name(&self, channel_key: &str) -> Option<String> {
        let loaded = self.snapshot.load();
        let snap = loaded.as_ref().as_ref()?;
        snap.channels.get(channel_key).map(|c| c.name.clone())
    }
}

/// Admin-plane port over the running dispatcher: read the current catalog snapshot
/// and install a freshly loaded one. Selection stays on the engine's [`Dispatch`]
/// trait, so the admin plane cannot reach into the request path.
impl gateway_core::dispatch::DispatcherHandle for Dispatcher {
    fn snapshot(&self) -> Option<Arc<Snapshot>> {
        Dispatcher::snapshot(self)
    }

    fn set_snapshot(&self, snapshot: Arc<Snapshot>) {
        Dispatcher::set_snapshot(self, snapshot);
    }
}
