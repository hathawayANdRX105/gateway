//! P2C routing selection: sample by effective prior (static weight × slow-start
//! ramp) and compare real-state posteriors.
//!
//! Algorithm:
//! 1. Filter out disabled, excluded, and cooling candidates;
//! 2. Put every candidate into the same comparison pool; the expected baseline
//!    share is expressed by the effective prior:
//!    `share_i = (w_i × f_i) / Σ(w_j × f_j)`, where `f` is the slow-start ramp
//!    discount factor (`HealthState::slow_start_factor`, healthy and fully
//!    warmed up = 1.0);
//! 3. Draw `choices` independent samples with replacement according to the prior;
//! 4. Compare `likelihood` across the samples and pick the highest posterior;
//! 5. likelihood is the relative product of latency, health score, available
//!    concurrency slots, token in-flight occupancy, and generation throughput,
//!    floored at 0.05;
//! 6. Candidates at the floor or with zero prior (weight = 0) keep a uniform
//!    share of observed traffic via `explore_rate`.
//! 7. With a token budget attached, a channel's token in-flight occupancy
//!    (`load = min(1.0, inflight / cap)`) is appended to likelihood as the
//!    decision-layer multiplier `× (1 − load)` — a channel about to exhaust its
//!    TPM is down-weighted by real-time selection. **The prior (weight × ramp)
//!    and the sampling probabilities never move**: token enters the decision
//!    only, never the sampling; when a channel has `cap == 0` or no token budget
//!    is attached, the term is always 1.0, byte-identical to a build without the
//!    the wiring.
//! 8. With a windowed latency digest table attached (`LatencyDigestTable`, gray
//!    flag `digest_enabled` off by default), the **data source** of
//!    `latency_factor` is swapped wholesale to t-digest quantiles
//!    (`speed = pool_min_p50 / p50` × `tail = clamp(p50/p95×1.2, 0.5, 1.0)`,
//!    replacing rather than multiplying — see design §3.2); the warm predicate is
//!    `weight >= min_requests`, and no warm candidate or insufficient samples
//!    means a neutral 1.0. **Prior/sampling/explore/ramp/token never move**;
//!    without a digest attached (`digest == None`, the gray default),
//!    `latency_factor` takes the EWMA branch above byte for byte, behaving
//!    exactly as today.
//! 9. Generation throughput (tps) is the fourth decision factor:
//!    `tps_factor = clamp(candidate tps_ema / pool max tps_ema, 0.2, 1.0)` is
//!    multiplied into the last position of likelihood (design §3.3). `tps_ema`
//!    is a late signal written back only when the stream finishes (the
//!    `record_tps` channel); an unobserved candidate and a pool with no
//!    observations stay neutral at 1.0; with the health system disabled
//!    (`enabled=false`) every factor is neutral, matching current behavior.
//!    **Prior/sampling/explore/ramp/token/digest never move**.
//!
//! 10. Sticky prior boost (design §4/§5): when the sticky channel bound to this
//!     request (`sticky_channel_key`) is still in the `eligible` pool, its prior
//!     weight is multiplied by an additional `boost` (default 4.0, tunable via
//!     `P2CSelector::with_sticky_boost`), then is explicitly renormalized over
//!     the **post-boost total** (Σprior == 1 — diagnostics and sampling share one
//!     and the same prior).
//!     **The boost only enters the prior/sampling layer: posterior likelihood and
//!     veto power are unchanged** (design §4 invariant 1, "sticky never hard-pins
//!     a channel") — once the bound channel cools down / is excluded / is
//!     disabled, it is filtered out of `eligible`, naturally gets no boost, and
//!     falls back to plain P2C.
//!
//! **The slow-start ramp is a sampling-layer discount and never enters
//! likelihood**: weight governs the long-run distribution, ramp governs the
//! temporary recovery discount of a channel whose cooldown just ended —
//! `ramp_pending` starts at the `1/min_requests` floor, then recovers linearly to
//! 1.0 as the success count accumulates via `count/min_requests` (the math and
//! the health table share the single `HealthState::slow_start_factor`, no second
//! copy). The share is discounted during recovery but the sampling probability
//! stays strictly positive: a channel need not wait for `explore_rate` to rescue
//! it; accumulating counts is enough to heal back to a full share.
//! With the health system disabled (`enabled=false`) the state stays at the
//! full-health default and the discount factor is 1.0.
//!
//! When no candidate in the pool is selectable, `recall_fainted` stays in place
//! to force-recall the channel with the shortest remaining cooldown.

use crate::candidate::STATUS_ENABLED;
use crate::health::{HealthSetting, HealthState, HealthTable};
use crate::latency_digest::LatencyDigestTable;
use crate::token_budget::TokenBudgetState;
use gateway_core::records::RouteUnitRecord;
use gateway_gate::concurrency::ConcurrencyState;
use rand::Rng;
use std::sync::Arc;

/// Selector trait — candidates plus a health table in, one choice out.
/// Picks one from the candidate set. When the whole pool is unselectable
/// (typically: everything is cooling down), it urgently recalls the channel
/// with the shortest remaining cooldown; it returns None only when no enabled
/// channel outside the exclude set is cooling (the caller reports NoCandidate).
///
/// Returns a borrow rather than ownership (ocr #8): candidates are already held
/// by Arc in the snapshot, so the hot path clones nothing per request; only the
/// caller's final resolve_candidate produces an owned value.
///
/// The injected `rng` makes weighted-random tests fully deterministic
/// (statistical assertions no longer depend on a global random source).
/// `sticky_channel_key` is the sticky channel bound to this request (`None` =
/// not sticky): when it hits and is still in the eligible pool, only its
/// **prior** (`prior`) is raised — it does not enter the decision and does not
/// bypass exclude.
pub trait Selector: Send + Sync {
    fn pick<'a>(
        &self,
        units: &[&'a RouteUnitRecord],
        health: &dyn HealthTable,
        exclude: &[String],
        sticky_channel_key: Option<&str>,
        now_ms: u64,
        rng: &mut dyn rand::RngCore,
    ) -> Option<&'a RouteUnitRecord>;
}

/// P2C selector that samples by effective prior (static weight × slow-start ramp
/// discount) and compares real-state posteriors.
pub struct P2CSelector {
    concurrency: Arc<ConcurrencyState>,
    choices: usize,
    explore_rate: f64,
    token: Option<Arc<TokenBudgetState>>,
    /// Windowed t-digest latency signal table (gray wiring). `None` = not
    /// attached, so `latency_factor` takes the EWMA branch (byte-for-byte
    /// current behavior). Assembled inside `Dispatcher::with_latency_digest`;
    /// only the latency signal source changes — prior/sampling/explore stay put.
    digest: Option<Arc<LatencyDigestTable>>,
    /// Sticky prior boost (design §4/§5): when the bound channel hits and is in
    /// the eligible pool, its prior weight is multiplied by `boost` (default
    /// [`DEFAULT_STICKY_BOOST`]) and then renormalized over the new total.
    /// Prior/sampling layer only — posterior likelihood and veto power are
    /// untouched (stickiness never hard-pins a channel).
    boost: f64,
}

impl P2CSelector {
    /// Create a P2C selector.
    ///
    /// `choices` is treated as at least 1; the default production configuration
    /// uses 2. `explore_rate` is the floor-exploration probability — candidates
    /// whose likelihood is already clamped to the floor (`LIKELIHOOD_FLOOR`) keep
    /// a small stream of traffic through it; otherwise a single jitter could
    /// starve an already recovered channel forever. The concurrency state must
    /// share the same `Arc` as the `ConcurrencyGate`, or the available-slot
    /// signal is distorted.
    pub fn new(concurrency: Arc<ConcurrencyState>, choices: usize, explore_rate: f64) -> Self {
        Self {
            concurrency,
            choices: choices.max(1),
            explore_rate: explore_rate.clamp(0.0, 1.0),
            token: None,
            digest: None,
            boost: DEFAULT_STICKY_BOOST,
        }
    }

    /// Attach a token budget: the P2C decision layer reads channel in-flight
    /// occupancy from it for discounting. Assembled inside
    /// `Dispatcher::with_token_budget`; when not attached (the default `None`)
    /// the decision simply omits the token factor, matching current behavior.
    /// Exposed as `pub` so `tests/` can inject statistical cases directly.
    pub fn with_token(mut self, token: Arc<TokenBudgetState>) -> Self {
        self.token = Some(token);
        self
    }

    /// Attach the windowed latency digest table: the decision layer's
    /// `latency_factor` switches its data source to t-digest p50/p95 (design
    /// §3.2). When not attached (the default `None`) it still uses the EWMA
    /// scalar, byte-for-byte identical to current behavior. Exposed as `pub` so
    /// `tests/` can inject statistical cases directly (same as `with_token`).
    pub fn with_latency_digest(mut self, t: Arc<LatencyDigestTable>) -> Self {
        self.digest = Some(t);
        self
    }

    /// Configure the sticky prior boost multiplier: when a sticky channel hits
    /// (and is still in the eligible pool), its prior weight is multiplied by
    /// `boost` and then explicitly renormalized over the post-boost total
    /// (Σprior == 1). **Prior/sampling layer only** — posterior likelihood and
    /// veto power (cooldown/exclude/disabled) are untouched; stickiness never
    /// hard-pins a channel. Defaults to [`DEFAULT_STICKY_BOOST`] (4.0), clamped
    /// to [`MAX_STICKY_BOOST`]: illegal input (non-finite / ≤0 / out of range)
    /// is clamped to the ceiling so `weight × ramp × boost` always stays finite
    /// and the normalized prior can never become `inf/inf = NaN`.
    pub fn with_sticky_boost(mut self, boost: f64) -> Self {
        self.boost = clamp_sticky_boost(boost);
        self
    }
}

/// Clamp the sticky boost to [`MAX_STICKY_BOOST`]: legal values
/// (`0 < boost ≤ ceiling`) pass through unchanged; non-finite / ≤0 / out of
/// range are all clamped to the ceiling. The selector layer provides this
/// backstop so prior normalization is immune to inf inputs from any source
/// (including future assembly paths).
fn clamp_sticky_boost(boost: f64) -> f64 {
    if boost.is_finite() && boost > 0.0 && boost <= MAX_STICKY_BOOST {
        boost
    } else {
        MAX_STICKY_BOOST
    }
}

/// Clamp floor of likelihood. Candidates degraded to this value keep traffic
/// through `explore_rate`.
pub const LIKELIHOOD_FLOOR: f64 = 0.05;
/// Floor-exploration probability: each comparison has a small chance to switch
/// to a candidate that has hit the floor.
pub const DEFAULT_EXPLORE_RATE: f64 = 0.005;
/// Default sticky prior boost multiplier: the bound channel's prior is ×4 and
/// then renormalized over the new total (two equally weighted channels go from
/// 1:1 to 4:1, i.e. 0.8:0.2). Prior/sampling layer only (design §4).
pub const DEFAULT_STICKY_BOOST: f64 = 4.0;
/// **Upper bound** of the sticky prior boost (the clamp target of
/// `with_sticky_boost`). Value rationale: the weight field is `u32`
/// (`RouteUnitRecord::weight`, max `u32::MAX ≈ 4.3e9`) and ramp is
/// `HealthState::slow_start_factor` (always ∈ (0, 1]), so the worst product is
/// `weight × ramp × boost ≤ 4.3e9 × 1.0 × ceiling`. At `1e6` that product is
/// ≤ `4.3e15`, leaving ~292 orders of magnitude of headroom below the f64
/// overflow threshold `f64::MAX ≈ 1.8e308`; the post-boost Σw is always finite.
/// Semantically `1e6` is also far above any meaningful operational setting
/// (default 4.0, realistically on the order of hundreds).
/// Illegal input (non-finite / ≤0 / out of range) is clamped to this ceiling
/// (see `clamp_sticky_boost`).
pub const MAX_STICKY_BOOST: f64 = 1e6;

impl Selector for P2CSelector {
    fn pick<'a>(
        &self,
        units: &[&'a RouteUnitRecord],
        health: &dyn HealthTable,
        exclude: &[String],
        sticky_channel_key: Option<&str>,
        now_ms: u64,
        rng: &mut dyn rand::RngCore,
    ) -> Option<&'a RouteUnitRecord> {
        let scored = self.scored(units, health, exclude, sticky_channel_key, now_ms);
        if scored.is_empty() {
            return recall_fainted(units, health, exclude, now_ms);
        }
        Some(compare_p2c(&scored, self.choices, self.explore_rate, rng).0)
    }
}

impl P2CSelector {
    /// Prior/posterior scoring — the comparison input of [`Selector::pick`]:
    /// per-candidate `(unit, prior, likelihood)` inside the eligible pool.
    ///
    /// - `eligible`: enabled, not in `exclude`, not cooling (a sticky channel
    ///   must also be in the pool to receive the boost).
    /// - `prior`: **normalized effective prior** = `weight × ramp × boost`
    ///   divided by the post-boost total (Σprior == 1; the sampling call
    ///   `pick_weighted_index` and the upper-layer diagnostics read the same
    ///   convention).
    /// - `likelihood`: posterior product (latency × health × concurrency × token
    ///   discount × throughput) — the prior is never multiplied in again.
    ///
    /// An empty pool returns an empty `Vec`, and `pick` decides to fall back to
    /// the emergency recall. `pub` only so tests can assert the prior convention
    /// directly (Σprior, whether the boost really entered the prior); the only
    /// production call site is still `pick`.
    pub fn scored<'a>(
        &self,
        units: &[&'a RouteUnitRecord],
        health: &dyn HealthTable,
        exclude: &[String],
        sticky_channel_key: Option<&str>,
        now_ms: u64,
    ) -> Vec<(&'a RouteUnitRecord, f64, f64)> {
        let eligible = eligible_units(units, health, exclude, now_ms);
        if eligible.is_empty() {
            return Vec::new();
        }

        // Effective prior = base weight × slow-start ramp discount × sticky boost
        // (bound channel only): the expected baseline share is
        // w_i·f_i·b_i / Σ(w_j·f_j·b_j). The ramp factor is read from the live
        // health table config (set_config follows automatically); with the health
        // system disabled the state stays at the full-health default and the
        // factor is 1.0 (same convention as the health table's disabled mode).
        let cfg = health.setting();
        let (weighted, total_weight) =
            effective_priors(&eligible, health, &cfg, sticky_channel_key, self.boost);
        let signals = self.pool_signals(&eligible, health, &cfg);

        // Digest path: pre-scan `eligible` once, one `quantiles_with_weight` per
        // candidate gathering p50/p95/weight together (both the warm predicate
        // and the `pool_min_p50` baseline are computed in this single scan; one
        // lock acquisition avoids two mutex trips per candidate), so the
        // likelihood loop below only reads the results, not the table. No digest
        // attached (gray default) ⇒ `None` ⇒ latency_factor takes the EWMA
        // branch below.
        let digest_latency: Option<Vec<f64>> = self
            .digest
            .as_ref()
            .map(|table| digest_latency_factors(table, &eligible, cfg.min_requests));

        let mut scored: Vec<(&RouteUnitRecord, f64, f64)> = Vec::with_capacity(eligible.len());
        for (index, unit) in eligible.iter().enumerate() {
            let unit = *unit;
            let state = health.get(&unit.meta.key);
            let (boosted_weight, ramp) = weighted[index];
            // Explicit renormalization: divide by the post-boost Σw so
            // Σprior == 1 (sampling and diagnostics use the same convention).
            // **The prior enters sampling only**, never the decision (see the
            // option-C notes on compare_p2c).
            let prior = normalized_prior(boosted_weight, total_weight, ramp, eligible.len());
            // The decision looks only at likelihood: weight has already been
            // expressed once through the sampling probability, so multiplying it
            // into the score would count the same configuration twice — the
            // sticky boost likewise enters the prior only.
            let likelihood =
                self.unit_likelihood(unit, &state, index, &signals, digest_latency.as_deref());
            scored.push((unit, prior, likelihood));
        }
        scored
    }

    /// Pool-level baselines (order matches the historical implementation:
    /// latency → concurrency → throughput).
    fn pool_signals(
        &self,
        eligible: &[&RouteUnitRecord],
        health: &dyn HealthTable,
        cfg: &HealthSetting,
    ) -> PoolSignals {
        let best_latency = eligible
            .iter()
            .map(|unit| health.get(&unit.meta.key).latency_ewma_ms)
            .filter(|latency| *latency > 0.0)
            .reduce(f64::min);
        let max_permits = eligible
            .iter()
            .filter_map(|unit| self.concurrency.available_permits(&unit.channel_key))
            .max();
        // Pool throughput baseline (for the fourth factor): the largest
        // `tps_ema` among observed candidates. No observation in the pool or the
        // health system disabled ⇒ `None`, keeping the factor neutral at 1.0
        // (design §3.3/§3.4 gray rules).
        let pool_max_tps = cfg.enabled.then(|| {
            eligible
                .iter()
                .map(|unit| health.get(&unit.meta.key).tps_ema)
                .fold(0.0, f64::max)
        });
        PoolSignals {
            best_latency,
            max_permits,
            max_tps: pool_max_tps.filter(|max| *max > 0.0),
        }
    }

    /// Per-candidate likelihood: latency × health × concurrency × token ×
    /// throughput, clamped to [`LIKELIHOOD_FLOOR`]..=1.0. Factor read order
    /// matches the historical implementation.
    fn unit_likelihood(
        &self,
        unit: &RouteUnitRecord,
        state: &HealthState,
        index: usize,
        signals: &PoolSignals,
        digest_latency: Option<&[f64]>,
    ) -> f64 {
        // latency_factor has one of two data sources: with a digest attached it
        // uses t-digest quantiles (speed × tail, replacing rather than
        // multiplying — see design §3.2); otherwise it takes the EWMA scalar
        // branch byte for byte (regression invariant: no digest attached =
        // current behavior).
        let latency_factor = digest_latency.map_or_else(
            || match (signals.best_latency, state.latency_ewma_ms) {
                (Some(best), latency) if latency > 0.0 => best / latency,
                _ => 1.0,
            },
            |factors| factors[index],
        );
        let concurrency_factor = match (
            self.concurrency.available_permits(&unit.channel_key),
            signals.max_permits,
        ) {
            (Some(permits), Some(maximum)) => (permits as f64 + 1.0) / (maximum as f64 + 1.0),
            _ => 1.0,
        };
        let token_discount = self
            .token
            .as_ref()
            .and_then(|t| t.load(&unit.channel_key))
            .map(|l| 1.0 - l)
            .unwrap_or(1.0);
        // Fourth factor: generation throughput (design §3.3). tps_ema is a late
        // signal written back only when the stream finishes; like the three
        // preceding factors (observable by response-header time) it enters the
        // decision only — **prior/sampling/explore/ramp never move**. Placed
        // last: order is irrelevant to a product, but reading it last matches
        // "apply one more throughput squeeze after the three quality factors".
        let tps_factor = match (state.tps_ema, signals.max_tps) {
            (tps, Some(max)) if tps > 0.0 => (tps / max).clamp(0.2, 1.0),
            _ => 1.0,
        };
        (latency_factor * state.ewma_score * concurrency_factor * token_discount * tps_factor)
            .clamp(LIKELIHOOD_FLOOR, 1.0)
    }
}

/// Eligible pool: enabled, not in `exclude`, not cooling (a sticky channel must
/// also be in the pool to receive the boost).
fn eligible_units<'a>(
    units: &[&'a RouteUnitRecord],
    health: &dyn HealthTable,
    exclude: &[String],
    now_ms: u64,
) -> Vec<&'a RouteUnitRecord> {
    units
        .iter()
        .copied()
        .filter(|u| u.status == STATUS_ENABLED)
        .filter(|u| !is_excluded(u, exclude))
        .filter(|u| health.is_selectable(&u.meta.key, now_ms))
        .collect()
}

/// Whether the candidate is in this request's tried-and-excluded set (`exclude`
/// is keyed by unit key).
fn is_excluded(unit: &RouteUnitRecord, exclude: &[String]) -> bool {
    exclude.iter().any(|key| key == &unit.meta.key)
}

/// First pass computes only the "post-boost weight": returns per-candidate
/// `(boosted_weight, ramp)` plus the post-boost total. Σ serves both the second
/// pass's explicit prior renormalization and the "all-zero weight degradation"
/// check (tested on the post-boost total; boost and ramp are strictly positive,
/// so the verdict is unchanged).
fn effective_priors(
    eligible: &[&RouteUnitRecord],
    health: &dyn HealthTable,
    cfg: &HealthSetting,
    sticky_channel_key: Option<&str>,
    boost: f64,
) -> (Vec<(f64, f64)>, f64) {
    let weighted: Vec<(f64, f64)> = eligible
        .iter()
        .map(|unit| {
            let ramp = if cfg.enabled {
                health
                    .get(&unit.meta.key)
                    .slow_start_factor(cfg.min_requests)
            } else {
                1.0
            };
            // Hit test = the bound channel is still in the eligible pool
            // (cooling/exclude/disabled were already filtered out): not in the
            // pool → boost factor 1.0, and stickiness never hard-pins a channel
            // (design §4 invariant 1).
            let boost = if sticky_channel_key.is_some_and(|key| unit.channel_key == key) {
                boost
            } else {
                1.0
            };
            (f64::from(unit.weight) * ramp * boost, ramp)
        })
        .collect();
    let total_weight: f64 = weighted.iter().map(|(weight, _)| *weight).sum();
    (weighted, total_weight)
}

/// Pool-level baseline signals — the shared denominators/upper bounds of
/// per-candidate likelihood.
struct PoolSignals {
    /// Smallest positive latency EWMA in the pool (the `speed` denominator of
    /// the EWMA branch).
    best_latency: Option<f64>,
    /// Largest available concurrency slots in the pool (normalization ceiling of
    /// the concurrency factor).
    max_permits: Option<usize>,
    /// Largest `tps_ema` among observed candidates in the pool (no observations
    /// or health disabled ⇒ `None`).
    max_tps: Option<f64>,
}

/// Explicit renormalization: `boosted_weight / Σw` (Σprior == 1, one convention
/// for sampling and diagnostics).
///
/// The fallback condition is "Σw is not a finite positive number", not merely
/// `> 0.0`: an overflow of `weight × ramp × boost` turns Σw into `inf`, and then
/// `boosted_weight / total_weight = inf/inf = NaN`; a NaN prior travels all the
/// way into `gen_range(0.0..NaN)` inside `pick_weighted_index` and panics (rand
/// panics on empty/invalid ranges). Defense in depth: even if a future assembly
/// path bypasses the clamping in `with_sticky_boost` and produces a non-finite
/// value, this degrades to the existing uniform prior (×ramp), keeping the
/// sampling pool alive and never panicking.
fn normalized_prior(boosted_weight: f64, total_weight: f64, ramp: f64, count: usize) -> f64 {
    if total_weight.is_finite() && total_weight > 0.0 {
        boosted_weight / total_weight
    } else {
        1.0 / count as f64 * ramp
    }
}

/// Digest latency factors (design §3.2): one `quantiles_with_weight` per
/// candidate collects p50/p95/weight and computes `clamp(speed × tail)`. A
/// single scan gathers the warm predicate, the `pool_min_p50` baseline, and the
/// per-candidate formula inputs, so the likelihood loop does not read the table
/// again.
///
/// The warm predicate is `weight >= min_requests` (same convention as
/// `HealthSetting::min_requests`). No warm candidate (or every p50 non-positive)
/// ⇒ everyone neutral at 1.0; cold (quantiles missing or too few samples) is
/// likewise 1.0. Dirty data (non-positive p50/p95) takes `tail = 1.0` with a
/// speed denominator fallback, producing neither NaN nor a panic.
fn digest_latency_factors(
    table: &LatencyDigestTable,
    eligible: &[&RouteUnitRecord],
    min_requests: u32,
) -> Vec<f64> {
    let min_reqs = f64::from(min_requests);
    // One locked read per candidate collects p50/p95/weight, aligned with the
    // eligible order so the baseline and the formula can reuse it.
    let samples: Vec<Option<(f64, f64, f64)>> = eligible
        .iter()
        .map(|unit| table.quantiles_with_weight(&unit.meta.key))
        .collect();
    // Baseline = the smallest p50 among warm candidates (weight >= min_requests
    // and p50 > 0).
    let pool_min_p50 = samples
        .iter()
        .filter_map(|q| *q)
        .filter(|(_, _, w)| *w >= min_reqs)
        .map(|(p50, _, _)| p50)
        .filter(|p50| *p50 > 0.0)
        .reduce(f64::min);
    let Some(pool_min_p50) = pool_min_p50 else {
        // No warm candidate: everything is neutral; prior and sampling are
        // unaffected (cold channels are not starved).
        return vec![1.0; eligible.len()];
    };
    samples
        .iter()
        .map(|q| {
            // cold (quantiles missing or too few samples) ⇒ neutral 1.0.
            let Some((p50, p95, _)) = (*q).filter(|(_, _, w)| *w >= min_reqs) else {
                return 1.0;
            };
            // Tail-ratio defense: dirty data with non-positive p50/p95 is kept
            // out of the ratio and degrades to neutral.
            let speed = if p50 > 0.0 { pool_min_p50 / p50 } else { 1.0 };
            let tail = if p50 > 0.0 && p95 > 0.0 {
                (p50 / p95 * 1.2).clamp(0.5, 1.0)
            } else {
                1.0
            };
            (speed * tail).clamp(LIKELIHOOD_FLOOR, 1.0)
        })
        .collect()
}

/// Emergency recall — when the whole pool is unselectable, find the enabled
/// channel with the shortest remaining cooldown (skipping the exclude set
/// already tried in this request), force its cooldown to end, and return it.
///
/// Returns `None` only when no enabled channel outside the exclude set is
/// cooling down (nothing left to recall). Equal remainders keep the first one
/// (stable `units` order, deterministic result).
fn recall_fainted<'a>(
    units: &[&'a RouteUnitRecord],
    health: &dyn HealthTable,
    exclude: &[String],
    now_ms: u64,
) -> Option<&'a RouteUnitRecord> {
    let mut best: Option<(&RouteUnitRecord, u64)> = None;
    for u in units {
        if u.status != STATUS_ENABLED || is_excluded(u, exclude) {
            continue;
        }
        let Some(remaining) = health.cooling_remaining_ms(&u.meta.key, now_ms) else {
            continue;
        };
        match best {
            Some((_, prev)) if prev <= remaining => {}
            _ => best = Some((u, remaining)),
        }
    }
    let (unit, _) = best?;
    health.force_recall(&unit.meta.key);
    Some(unit)
}

/// Perform cumulative weighted sampling by effective prior (static weight ×
/// slow-start ramp) and return the hit index.
///
/// Returns an index rather than a reference: the comparison stage switches
/// between several indices, which lets the steps "sampled set → add opponent →
/// take the max posterior" avoid repeatedly resolving element identities.
/// Callers guarantee non-negative weights with at least one positive: an
/// all-zero channel group degrades to uniform weights when the prior is
/// assembled (also multiplied by the ramp discount, which is always positive and
/// therefore keeps at least one weight positive); duplicate draws only add
/// opponents from unselected positive-prior candidates.
// Callers guarantee at least one positive weight (see doc comment), so the
// fallback rposition cannot fail; converting to Option would push an unreachable
// error onto `compare_p2c`, which returns a reference, not Result.
#[allow(unknown_lints)]
#[allow(expect_in_non_result)]
fn pick_weighted_index(weights: &[f64], rng: &mut dyn rand::RngCore) -> usize {
    let total: f64 = weights.iter().sum();
    let mut target = rng.gen_range(0.0..total);
    for (index, weight) in weights.iter().enumerate() {
        target -= weight;
        if target < 0.0 {
            return index;
        }
    }
    // Floating-point accumulation fallback: pick only the last positive-weight
    // candidate so zero weights never slip into a normal comparison.
    weights
        .iter()
        .rposition(|weight| *weight > 0.0)
        .expect("weighted sampling requires a positive weight")
}

/// Build the comparison pool: sort and deduplicate sampled indices; on duplicate
/// draws, add one more opponent from the **unselected candidates with a positive
/// prior** (zero weights must not bypass the explore rate by filling a slot).
fn build_pool(sampled: &[usize], weights: &[f64], rng: &mut dyn rand::RngCore) -> Vec<usize> {
    let mut pool = sampled.to_vec();
    pool.sort_unstable();
    pool.dedup();

    if pool.len() < sampled.len() {
        // On duplicate draws, only positive-prior candidates serve as opponents:
        // a zero weight must not bypass the explore rate by filling a slot.
        let mut rest: Vec<usize> = Vec::with_capacity(weights.len() - pool.len());
        rest.extend((0..weights.len()).filter(|i| weights[*i] > 0.0 && !pool.contains(i)));
        if !rest.is_empty() {
            let mut rest_weights: Vec<f64> = Vec::with_capacity(rest.len());
            rest_weights.extend(rest.iter().map(|i| weights[*i]));
            let offset = pick_weighted_index(&rest_weights, rng);
            pool.push(rest.swap_remove(offset));
        }
    }
    pool
}

/// P2C comparison: **draw `choices` samples by effective prior, then take the
/// highest likelihood among those candidates**.
///
/// ## Separation of concerns (option C)
///
/// | Layer | Looks only at | Expresses |
/// |---|---|---|
/// | Sampling | effective prior = weight × slow_start_factor (plus sticky boost) | **long-run mix × temporary recovery discount**: expected share = w_i·f_i/Σ(w_j·f_j), at full health f=1.0 |
/// | Decision | `likelihood` (latency × health × concurrency × token × throughput) | **real-time preference**: who is faster and healthier right now |
/// | Decision tie | sampling order | fallback: with no quality signal, the first drawn wins |
///
/// **The slow-start ramp enters only the sampling prior, never the decision**:
/// a channel that just left cooldown starts at the floor `1/min_requests` per
/// `HealthState::slow_start_factor` (the configured `min_requests` comes from
/// the health table and follows live updates) and recovers the discount factor
/// linearly as its success count accumulates; the share is discounted during
/// recovery but the sampling probability stays strictly positive — the channel
/// need not wait for `explore_rate` to rescue it, filling the count alone heals
/// it back to a full share. Real-time preference in likelihood and the recovery
/// discount do not mix.
///
/// Weight **enters sampling only** and is not multiplied into the decision
/// score. An earlier implementation also multiplied the prior into the posterior
/// (`posterior = prior × likelihood`), counting the same configuration twice:
/// a high-weight channel was both more likely to be drawn and more likely to win
/// the comparison, so its share inflated superlinearly with weight. After the
/// separation the mix is stable, while a "low weight but currently fastest"
/// channel can keep earning excess traffic through the decision.
///
/// **Tie-break by sampling order**: `Iterator::max_by` returns the last maximum
/// on ties, and the comparison-pool order depends on "which indices were drawn +
/// where the added opponent was appended", unrelated to weight. A two-candidate
/// pool contains both every round and likelihood often ties (both healthy,
/// neither with latency data), so the result degraded into traffic decided by
/// accidental order — measured, a heavy channel with weight 10:30 got only 0.455,
/// less than the low-weight light one. Tie-breaking to prior would eliminate that
/// but amounts to stuffing weight back into the decision (measured: 10:30 →
/// 1.000, 0:5 → 0.000), completely hollowing out the sampling probability.
/// Deciding by sampling order is the closed semantics of "weight governs sampling
/// only": with no quality signal the first drawn wins, and traffic strictly
/// equals the prior.
///
/// Sampling is **with replacement**. That brings a defect that must be fixed:
/// when the same candidate is drawn twice, no comparison happens at all and that
/// candidate wins without a fight — a slow channel with 3× the weight would
/// therefore steadily take $(3/4)^2 \approx 55\%$ of traffic, making the latency
/// signal useless.
///
/// Fix one: upon detecting a duplicate draw, draw one more opponent from the
/// **unselected candidates with a positive prior** and add it to the comparison.
/// The duplicate candidate still has to beat a real opponent; zero-prior
/// candidates earn traffic only through exploration.
///
/// Fix two (floor/zero-prior exploration): candidates that cannot get baseline
/// traffic — degraded channels whose likelihood hits `LIKELIHOOD_FLOOR`, and
/// channels with `weight = 0` (prior = 0) — can never win a normal comparison,
/// so their state never refreshes → one jitter permanently stops the channel even
/// after it recovers. With a small `explore_rate` probability, **uniformly**
/// switch to one such candidate (uniform rather than prior-weighted: a
/// zero-prior candidate has prior 0, so prior-weighted sampling would never reach
/// it), keeping observed traffic flowing so they can re-accumulate latency and
/// health data and heal back to a normal weight.
fn compare_p2c<'a, 'b>(
    scored: &'b [(&'a RouteUnitRecord, f64, f64)],
    choices: usize,
    explore_rate: f64,
    rng: &mut dyn rand::RngCore,
) -> &'b (&'a RouteUnitRecord, f64, f64) {
    let weights: Vec<f64> = scored.iter().map(|(_, prior, _)| *prior).collect();
    let mut sampled: Vec<usize> = Vec::with_capacity(choices);
    (0..choices).for_each(|_| sampled.push(pick_weighted_index(&weights, rng)));
    let pool = build_pool(&sampled, &weights, rng);

    // Slot 3 is now likelihood itself (no longer multiplied by prior), so the
    // floor test compares it directly. The clamped floor means likelihood is
    // exactly LIKELIHOOD_FLOOR; the tolerance covers floating-point error.
    let is_floored =
        |(_, _, likelihood): &(&RouteUnitRecord, f64, f64)| *likelihood <= LIKELIHOOD_FLOOR * 1.01;

    // Decision: the higher likelihood wins; **on a tie, sampling order decides
    // and the first drawn wins**.
    //
    // Ties must be handled explicitly rather than left to `max_by`'s pool order.
    // `pool` is sorted+deduplicated and then gets an opponent `push`ed, so its
    // order depends on "which indices were drawn + where the opponent landed",
    // unrelated to weight; a two-candidate pool contains both every round and
    // likelihood often ties (both healthy, neither with latency data), so it
    // degraded into traffic decided by accidental order — measured, a heavy
    // channel with weight 10:30 got only 0.455, **less than the low-weight
    // light**, exactly the wrong direction.
    //
    // tie-breaking to prior would eliminate that, but amounts to stuffing weight
    // back into the decision: measured it becomes 10:30 → 1.000, 0:5 → 0.000,
    // the sampling probability is completely hollowed out — precisely the "same
    // configuration counted twice" that option C eliminates.
    //
    // Deciding by sampling order is the closed semantics of "weight governs
    // sampling only": with no quality signal the comparison cannot tell better
    // from worse anyway, so the first drawn wins → traffic strictly equals the
    // prior ratio (10:30 → 0.75).
    let first_seen = |index: &usize| {
        sampled
            .iter()
            .position(|sampled_index| sampled_index == index)
            .unwrap_or(usize::MAX)
    };
    let better = |a_index: &usize, b_index: &usize| match scored[*a_index]
        .2
        .total_cmp(&scored[*b_index].2)
    {
        std::cmp::Ordering::Equal => first_seen(b_index).cmp(&first_seen(a_index)),
        other => other,
    };
    let best_of_pool = || {
        let index = *pool
            .iter()
            .max_by(|a, b| better(a, b))
            .expect("comparison pool is non-empty");
        &scored[index]
    };

    // Candidates that cannot get baseline traffic = at the floor (likelihood ≤
    // floor) or zero prior (weight = 0): they can never win a normal comparison,
    // so a small uniform `explore_rate` chance switches to them and keeps
    // observed traffic flowing. Uniform rather than prior-weighted: a zero-prior
    // candidate has prior 0, which weighted sampling can never pick.
    let starving: Vec<&(&RouteUnitRecord, f64, f64)> = scored
        .iter()
        .filter(|c| is_floored(c) || c.1 <= 0.0)
        .collect();
    if !starving.is_empty() && explore_rate > 0.0 && rng.gen_bool(explore_rate) {
        let index = rng.gen_range(0..starving.len());
        return starving[index];
    }

    best_of_pool()
}
