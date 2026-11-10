//! `config` — startup configuration, injected components, and the runtime knobs.
//!
//! Two disjoint value sets, one rule: **everything a consumer can set lives in
//! [`Config`]; everything the running data plane can change lives in
//! [`RuntimeKnobs`]; there is no third place.**
//!
//! - [`Config`] — boot values. Assembled once by the consumer; changing any of
//!   them means rebuilding the data plane. Where a field is also runtime-
//!   mutable, the boot value seeds the knob.
//! - [`Components`] — the runtime implementations the consumer injects (egress,
//!   proxy pool, snapshots, billing ports). Values are configuration; these are
//!   objects.
//! - [`Context`] — the single assembly input: `config` + `components`.
//! - [`RuntimeKnobs`] — the runtime-mutable subset, with an `epoch` bumped on
//!   every accepted change. Fields that are **not** knobs are explicitly listed
//!   in [`BootConfig`] so nobody has to guess which values are hot.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use arc_swap::ArcSwap;

use gateway_core::dispatch::RetryPolicy;
use gateway_core::egress::Timeouts;
use gateway_core::gate::{SharedGroupSnapshot, SharedTokenSnapshot, SharedUserSnapshot};
use gateway_core::health::{HealthHandle, HealthSetting};
use gateway_core::metering::{Ledger, PriceTable, SettleSink};
use gateway_core::proxy::ProxySnapshot;
use gateway_forward::egress::Egress;
use gateway_gate::quota::QuotaGate;
use gateway_gate::ratelimit::RateLimiter;
use gateway_proxy::ProxyManager;
use gateway_security::FilterConfig;

/// Response-cache limits, all fixed at construction.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ResponseCacheTuning {
    /// Entry TTL in seconds.
    pub ttl_secs: u32,
    /// Total body-byte budget.
    pub max_total_bytes: u32,
    /// Per-entry refusal limit in bytes.
    pub max_entry_bytes: u32,
}

/// Shared handles of the directory snapshots (loaded by the consumer at boot;
/// a reload stores new values into these same instances).
pub struct State {
    /// Channel / route-unit dispatch snapshot.
    pub dispatch: gateway_core::Snapshot,
    /// Token snapshot (the `AuthGate` allow-list decision).
    pub token_snapshot: SharedTokenSnapshot,
    /// User snapshot (the `StateGate` ban / version decision).
    pub user_snapshot: SharedUserSnapshot,
    /// Group snapshot (the `GroupModelGate` group allow-list decision).
    pub group_snapshot: SharedGroupSnapshot,
}

/// Billing settlement injection point: price table + settlement sink installed as
/// a pair at the `ForwardStage` settlement commit point.
///
/// The fields are trait objects — the implementations belong to the consumer
/// (e.g. the billing domain's price table / sink); this crate does not know
/// billing domain types.
pub struct Settle {
    /// Price table (resolves the unit price per model).
    pub price_table: Arc<dyn PriceTable>,
    /// Settlement sink (settlement event / charging write point).
    pub sink: Arc<dyn SettleSink>,
}

/// Startup configuration: every value the data plane is built from.
///
/// Split from [`Components`] so the "what we run with" question has one answer.
/// `Default` is a working local shape; a product overrides what it cares about.
#[derive(Debug, Clone)]
pub struct Config {
    /// Egress timeouts (`first_byte_ms` decides how long a stalled upstream stays
    /// on the wire before failover).
    pub timeouts: Timeouts,
    /// Boot value of the dispatch retry budget (installed into the hot handle;
    /// hot-mutable afterwards via [`RuntimeKnobs`]).
    pub retry_policy: RetryPolicy,
    /// Channel health configuration (cooldown / slow start); hot-mutable.
    pub health: HealthSetting,
    /// Windowed t-digest latency signal gray switch. **Boot-fixed**: mounting the
    /// digest table is structural, so toggling it rebuilds the data plane.
    pub digest_enabled: bool,
    /// Prefix-cache sticky routing switch. **Boot-fixed** (same reason).
    pub sticky_enabled: bool,
    /// Sticky routing boost. **Boot-fixed**: the selector takes it at construction.
    pub sticky_boost: f64,
    /// Global default of the per-channel token budget (TPM); hot-mutable via the
    /// knobs. It applies to channels **without** a `tpm_limit` override and is
    /// re-read on catalog reload; channels with an explicit override never
    /// follow it.
    pub token_tpm_default: u64,
    /// Per-token rate limit: maximum requests per window; hot-mutable.
    pub ratelimit_max_requests: u32,
    /// Per-token rate limit: window length in seconds; hot-mutable.
    pub ratelimit_window_seconds: u32,
    /// Global forwarding concurrency gate capacity. **Boot-fixed**: a live
    /// semaphore cannot be resized safely; changing it rebuilds the data plane.
    pub global_max_concurrency: u32,
    /// Response-cache configuration (`None` = not mounted). **Boot-fixed**.
    pub response_cache: Option<ResponseCacheTuning>,
    /// Word filter configuration; an empty word list means dormant. **Boot-fixed**.
    pub security: FilterConfig,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            timeouts: Timeouts::default(),
            retry_policy: RetryPolicy::default(),
            health: HealthSetting::default(),
            digest_enabled: false,
            sticky_enabled: true,
            sticky_boost: 1.0,
            token_tpm_default: 0,
            ratelimit_max_requests: 1_000,
            ratelimit_window_seconds: 60,
            global_max_concurrency: 64,
            response_cache: None,
            security: FilterConfig::default(),
        }
    }
}

/// Runtime implementations injected by the consumer. Not configuration: a data
/// plane is assembled from one `Config` plus one `Components`.
pub struct Components {
    /// Upstream egress. Production = `ReqwestEgress`; tests inject a mock.
    /// Required: a data plane that cannot dial upstream cannot be assembled, so
    /// this is a compile-time obligation, not a runtime panic.
    pub egress: Arc<dyn Egress>,
    /// Whether model requests borrow an egress Client (direct / proxied) from
    /// the [`ProxyManager`]. Tests pass false — the injected egress is called.
    pub wire_proxy_pool: bool,
    /// Egress proxy pool manager: admin-plane CRUD and data-plane leases share
    /// the same instance.
    pub proxies: Arc<ProxyManager>,
    /// Boot snapshot of the consumer's proxy-node rows.
    pub proxy_snapshot: ProxySnapshot,
    /// Directory snapshots (see [`State`]).
    pub state: State,
    /// Quota gate injection point; `None` = not installed into the chain.
    pub quota_gate: Option<QuotaGate>,
    /// Settlement injection point; `None` = no charge, no usage writes.
    pub settle: Option<Settle>,
    /// Prehold ledger; `None` in shapes without preholds.
    pub ledger: Option<Arc<dyn Ledger>>,
}

/// Assembly input: one config plus one set of components.
pub struct Context {
    pub config: Config,
    pub components: Components,
}

/// The boot-fixed **value** subset, exposed read-only so an operator can see
/// what a change would need a rebuild for. Injected implementations in
/// `Components` (egress, proxy pool, ledger, settle, …) are boot-fixed too, but
/// they are objects, not values, so they do not appear here.
#[derive(Debug, Clone)]
pub struct BootConfig {
    pub timeouts: Timeouts,
    pub digest_enabled: bool,
    pub sticky_enabled: bool,
    pub sticky_boost: f64,
    pub global_max_concurrency: u32,
    pub response_cache: Option<ResponseCacheTuning>,
    pub security: FilterConfig,
}

/// Effective configuration of a running data plane: the immutable boot view
/// plus every live knob value.
///
/// The reported rate-limit pair is only enforced when the assembled admission
/// chain actually contains the rate-limit gate (`Shape::Open` and hand-built
/// chains may not); the list of gates the chain runs is visible through the
/// chain, not through this view.
#[derive(Debug, Clone)]
pub struct EffectiveConfig {
    pub boot: BootConfig,
    pub retry_policy: RetryPolicy,
    pub health: HealthSetting,
    pub ratelimit_max_requests: u32,
    pub ratelimit_window_seconds: u32,
    pub token_tpm_default: u64,
}

/// The runtime-mutable subset, as a value a consumer can build, validate and
/// hand over. Boot-fixed fields are deliberately absent: they cannot be
/// expressed here, so they cannot be changed by accident.
#[derive(Debug, Clone, Copy, PartialEq, Default)]
pub struct ConfigDraft {
    pub retry_policy: Option<RetryPolicy>,
    pub health: Option<HealthSetting>,
    pub ratelimit_max_requests: Option<u32>,
    pub ratelimit_window_seconds: Option<u32>,
    pub token_tpm_default: Option<u64>,
}

impl ConfigDraft {
    /// Whether this draft changes anything at all.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.retry_policy.is_none()
            && self.health.is_none()
            && self.ratelimit_max_requests.is_none()
            && self.ratelimit_window_seconds.is_none()
            && self.token_tpm_default.is_none()
    }
}

/// Why a draft was refused. Nothing is applied on failure.
#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    /// The health setting is structurally invalid; nothing was applied.
    #[error("invalid health setting: {0}")]
    Health(&'static str),

    /// A retry budget that can never run an attempt (`max_attempts` == 0) would
    /// answer every request with 503; refused.
    #[error("invalid retry policy: {0}")]
    Retry(&'static str),

    /// A rate limit of 0 (or a 0-second window) denies every request outright
    /// instead of limiting them; refused.
    #[error("invalid rate limit: {0}")]
    RateLimit(&'static str),
}

/// Runtime-mutable knobs of an assembled data plane.
///
/// Held by the consumer (see [`crate::Gateway::knobs`]). Every mutation is
/// published as one atomic store per field and takes effect on the **next
/// request** — a request in flight keeps the value it read when it started.
/// `epoch` counts accepted changes so a control plane can tell "which
/// generation am I looking at".
///
/// Scope is exactly this set: `retry_policy`, `health`, the rate-limit pair,
/// and `token_tpm_default`. Boot-fixed *values* (timeouts, global concurrency,
/// digest / sticky / response-cache / word-filter mounts) are not knobs — they
/// are structural, and the migration path is an `ArcSwap<Option<Arc<..>>>`
/// slot per knob once the read side moves to load-per-request. Injected
/// *implementations* (`Components`: egress, proxies, ledger, settle, …) are
/// likewise assembly-time. Gray-list state and the state-gate IP policy are
/// consumer-tunable through [`crate::Gateway::graylist`] /
/// [`crate::Gateway::ip_policy`], not through this surface.
pub struct RuntimeKnobs {
    epoch: AtomicU64,
    retry: Arc<ArcSwap<RetryPolicy>>,
    health: Arc<dyn HealthHandle>,
    rate_limit: Arc<RateLimiter>,
    token_budget: Arc<gateway_dispatch::token_budget::TokenBudgetState>,
    boot: BootConfig,
}

impl RuntimeKnobs {
    pub(crate) fn new(
        retry: Arc<ArcSwap<RetryPolicy>>,
        health: Arc<dyn HealthHandle>,
        rate_limit: Arc<RateLimiter>,
        token_budget: Arc<gateway_dispatch::token_budget::TokenBudgetState>,
        boot: BootConfig,
    ) -> Self {
        Self {
            epoch: AtomicU64::new(0),
            retry,
            health,
            rate_limit,
            token_budget,
            boot,
        }
    }

    /// Number of accepted changes since assembly. An unchanged (all-`None`)
    /// draft is not a change and does not move the counter.
    pub fn epoch(&self) -> u64 {
        self.epoch.load(Ordering::Relaxed)
    }

    /// Current effective configuration (boot view + live knobs).
    pub fn snapshot(&self) -> EffectiveConfig {
        let (max_requests, window_seconds) = self.rate_limit.limits();
        EffectiveConfig {
            boot: self.boot.clone(),
            retry_policy: **self.retry.load(),
            health: self.health.config(),
            ratelimit_max_requests: max_requests,
            ratelimit_window_seconds: window_seconds,
            token_tpm_default: self.token_budget.global_default(),
        }
    }

    /// Apply a draft.
    ///
    /// Validation happens **first** and covers every mutable field: a draft that
    /// would deny all traffic (a zero retry attempt budget, a zero rate limit) is
    /// refused whole — nothing is applied, `epoch` does not move. After
    /// validation each field is published with a single atomic store (the rate
    /// limit pair as one value), so no request observes a torn configuration;
    /// a multi-field draft applies its fields in sequence, so a concurrent
    /// request may briefly see a mixture of the two drafts around it — per-field
    /// atomicity is the guarantee, not cross-field snapshots.
    ///
    /// `token_tpm_default` is the one knob whose effect is not per-request: it is
    /// the default for channels **without** a `tpm_limit` override, and it is
    /// re-read when the catalog snapshot reloads. Channels with an explicit
    /// override never follow this knob.
    ///
    /// # Errors
    ///
    /// Refuses the draft whole (`ConfigError`) when a field would deny all
    /// traffic: a zero `max_attempts`, a zero rate-limit count or window, or a
    /// structurally invalid health setting. Nothing is applied on refusal.
    pub fn update(&self, draft: ConfigDraft) -> Result<EffectiveConfig, ConfigError> {
        if let Some(retry) = draft.retry_policy
            && retry.max_attempts == 0
        {
            return Err(ConfigError::Retry("max_attempts must be >= 1"));
        }
        if let Some(max) = draft.ratelimit_max_requests
            && max == 0
        {
            return Err(ConfigError::RateLimit("max_requests must be >= 1"));
        }
        if let Some(window) = draft.ratelimit_window_seconds
            && window == 0
        {
            return Err(ConfigError::RateLimit(
                "window_seconds must be >= 1 (0 collapses the window)",
            ));
        }
        if let Some(health) = draft.health {
            health.validate().map_err(ConfigError::Health)?;
        }
        if draft.is_empty() {
            return Ok(self.snapshot());
        }
        if let Some(retry) = draft.retry_policy {
            self.retry.store(Arc::new(retry));
        }
        if let Some(health) = draft.health {
            self.health.set_config(health);
        }
        // The pair is read once and re-published as one value: two concurrent
        // updates cannot drop each other's field, and no reader sees a torn pair.
        if draft.ratelimit_max_requests.is_some() || draft.ratelimit_window_seconds.is_some() {
            let (max, window) = self.rate_limit.limits();
            self.rate_limit.set_limits(
                draft.ratelimit_max_requests.unwrap_or(max),
                draft.ratelimit_window_seconds.unwrap_or(window),
            );
        }
        if let Some(tpm) = draft.token_tpm_default {
            self.token_budget.set_global_default(tpm);
        }
        self.epoch.fetch_add(1, Ordering::Relaxed);
        Ok(self.snapshot())
    }
}
