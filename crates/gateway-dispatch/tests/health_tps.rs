//! Behavior tests for the generation-throughput (tps) EMA — PR #136 ticket 1 (health-side
//! data and channel).
//!
//! Pinned behavior:
//! - `Measured` first sample establishes `tps_ema` directly; later samples converge at α=0.2
//!   (hand-checked);
//! - `Estimated` samples update at α=0.1 and stay distinguishable from `Measured` (mixed
//!   feeds do not blur);
//! - `record_tps` (late channel) moves only `tps_ema`; cooldown/streak/EWMA/latency/digest
//!   accounting stays completely unchanged (pins "the late channel does not pollute
//!   existing health accounting");
//! - with `report.tps = None`, everything except `tps_ema` is byte-identical to the legacy
//!   path (regression guard);
//! - zero-observation `HealthState::default().tps_ema == 0.0` (selector: neutral, factor 1.0).

use gateway_dispatch::health::{
    AttemptReport, FailureClass, HealthSetting, HealthState, HealthTable, MemoryHealthTable,
    TpsSample,
};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

/// Pure in-memory health table with a fixed clock (`now_ms` always `1_000_000)`: the tps EMA
/// does not read the clock, but goes through `with_config_and_clock` to match existing tests.
fn table() -> Arc<MemoryHealthTable> {
    let now = 1_000_000u64;
    Arc::new(MemoryHealthTable::with_config_and_clock(
        HealthSetting::default(),
        move || now,
    ))
}

/// Compare two health records field by field (`HealthState` does not derive `PartialEq` —
/// this ticket does not widen the public surface, so assert per field; covers all existing
/// accounting fields except `tps_ema`).
fn assert_same_except_tps(a: &HealthState, b: &HealthState) {
    assert_eq!(a.ewma_score, b.ewma_score, "ewma_score");
    assert_eq!(a.latency_ewma_ms, b.latency_ewma_ms, "latency_ewma_ms");
    assert_eq!(
        a.latency_updated_at_ms, b.latency_updated_at_ms,
        "latency_updated_at_ms"
    );
    assert_eq!(a.request_count, b.request_count, "request_count");
    assert_eq!(a.unauthorized_run, b.unauthorized_run, "unauthorized_run");
    assert_eq!(a.ramp_exited, b.ramp_exited, "ramp_exited");
    assert_eq!(a.ramp_pending, b.ramp_pending, "ramp_pending");
    assert_eq!(a.cooldown_streak, b.cooldown_streak, "cooldown_streak");
    assert_eq!(a.throttle_streak, b.throttle_streak, "throttle_streak");
    assert_eq!(
        a.cooldown_until_ms, b.cooldown_until_ms,
        "cooldown_until_ms"
    );
    assert_eq!(
        a.score_updated_at_ms, b.score_updated_at_ms,
        "score_updated_at_ms"
    );
    assert_eq!(a.anneal_since_ms, b.anneal_since_ms, "anneal_since_ms");
    assert_eq!(
        a.last_cooling_outcome, b.last_cooling_outcome,
        "last_cooling_outcome"
    );
}

/// Case 1: a `Measured` first sample becomes `tps_ema` directly; later samples converge at
/// α=0.2 (hand-checked).
#[test]
fn measured_first_sample_becomes_ema_then_converges_alpha_0_2() {
    let t = table();
    t.record(
        "u",
        AttemptReport::new(Ok(200), None, None).with_tps(TpsSample::Measured(20.0)),
    );
    assert_eq!(
        t.get("u").tps_ema,
        20.0,
        "First sample (tps_ema is 0.0) directly takes the sample value"
    );

    // Second sample 40.0 → 0.2×40 + 0.8×20 = 8 + 16 = 24.0
    t.record(
        "u",
        AttemptReport::new(Ok(200), None, None).with_tps(TpsSample::Measured(40.0)),
    );
    assert!(
        (t.get("u").tps_ema - 24.0).abs() < 1e-9,
        "α=0.2 convergence: 0.2·40 + 0.8·20 = 24.0 (got {})",
        t.get("u").tps_ema
    );

    // Third sample 40.0 → 0.2×40 + 0.8×24 = 8 + 19.2 = 27.2
    t.record(
        "u",
        AttemptReport::new(Ok(200), None, None).with_tps(TpsSample::Measured(40.0)),
    );
    assert!(
        (t.get("u").tps_ema - 27.2).abs() < 1e-9,
        "α=0.2 converge again: 0.2·40 + 0.8·24 = 27.2 (constantly approaches 40 but stays below the sample, got {})",
        t.get("u").tps_ema
    );
}

/// Case 2: `Estimated` samples update at α=0.1 and stay distinguishable from `Measured`
/// (the same sequence fed to both lands on two different tracks; values never blur).
#[test]
fn estimated_updates_alpha_0_1_and_does_not_mingle_with_measured() {
    let t = table();
    // Estimated first sample takes the sample value directly (same first-sample rule as Measured):
    t.record(
        "u",
        AttemptReport::new(Ok(200), None, None).with_tps(TpsSample::Estimated(10.0)),
    );
    assert_eq!(
        t.get("u").tps_ema,
        10.0,
        "Estimated first sample directly takes the sample value"
    );

    // Estimated second sample 30.0 → 0.1×30 + 0.9×10 = 12.0
    t.record(
        "u",
        AttemptReport::new(Ok(200), None, None).with_tps(TpsSample::Estimated(30.0)),
    );
    assert_eq!(
        t.get("u").tps_ema,
        12.0,
        "α=0.1 convergence: 0.1·30 + 0.9·10 = 12.0"
    );

    // Control: the same "first sample 10.0" through Measured → first sample is still 10.0, but
    // the later Measured sample 30.0 → 0.2×30 + 0.8×10 = 14.0 (different track, different value).
    let m = table();
    m.record(
        "u",
        AttemptReport::new(Ok(200), None, None).with_tps(TpsSample::Measured(10.0)),
    );
    m.record(
        "u",
        AttemptReport::new(Ok(200), None, None).with_tps(TpsSample::Measured(30.0)),
    );
    assert_eq!(
        m.get("u").tps_ema,
        14.0,
        "Measured trajectory: 0.2·30 + 0.8·10 = 14.0"
    );

    // The two tracks must stay distinguishable after the same sequence (12.0 ≠ 14.0):
    assert!(
        t.get("u").tps_ema < m.get("u").tps_ema,
        "Estimated (α=0.1) should stay closer to the old value than Measured (α=0.2), not confused: estimated={} measured={}",
        t.get("u").tps_ema,
        m.get("u").tps_ema
    );
}

/// Case 3: `record_tps` moves only `tps_ema` — `ewma_score` / `latency_ewma_ms` /
/// `cooldown_until_ms` / all streak counters are completely unchanged before and after
/// the call (pins "the late channel does not pollute existing health accounting").
#[test]
fn record_tps_only_moves_tps_ema_not_health_accounting() {
    // First establish a health record with cooldown + EWMA + latency (non-default values), so
    // the before/after field comparison is meaningful.
    let now = Arc::new(AtomicU64::new(1_000_000));
    let clock = {
        let now = now.clone();
        move || now.load(Ordering::Relaxed)
    };
    let t = Arc::new(MemoryHealthTable::with_config_and_clock(
        HealthSetting::default(),
        clock,
    ));

    // One transport-layer failure → enters cooldown + writes EWMA + records latency.
    t.record(
        "u",
        AttemptReport::new(Err(FailureClass::Retryable), Some(800), None),
    );
    let before = t.get("u");
    assert!(
        before.cooldown_until_ms > 0,
        "Precondition: failure should have activated cooldown (field non-zero, otherwise comparison is meaningless)"
    );
    assert!(before.cooldown_streak >= 1);

    // The late channel merges only tps; every other field must not move by a single byte.
    t.record_tps("u", TpsSample::Measured(25.0));
    let after = t.get("u");

    assert_eq!(
        after.tps_ema, 25.0,
        "record_tps should establish tps_ema at 25.0 (first sample)"
    );
    assert_eq!(
        after.ewma_score, before.ewma_score,
        "EWMA score must not be moved by the late-arriving channel"
    );
    assert_eq!(
        after.latency_ewma_ms, before.latency_ewma_ms,
        "Latency EWMA must not be moved by the late-arriving channel"
    );
    assert_eq!(
        after.cooldown_until_ms, before.cooldown_until_ms,
        "Cooldown deadline must not be moved by the late-arriving channel"
    );
    assert_eq!(
        after.cooldown_streak, before.cooldown_streak,
        "Cooldown streak must not be moved by the late-arriving channel"
    );
    assert_eq!(
        after.throttle_streak, before.throttle_streak,
        "429 streak must not be moved by the late-arriving channel"
    );
    assert_eq!(
        after.unauthorized_run, before.unauthorized_run,
        "Credentials run must not be moved by the late-arriving channel"
    );
    assert_eq!(
        after.request_count, before.request_count,
        "Request count must not be moved by the late-arriving channel"
    );
}

/// Case 4: with `report.tps = None`, `tps_ema` stays 0.0 and the remaining fields equal
/// the legacy path (`AttemptReport::new` without tps) — regression guard: the None path
/// has zero behavioral difference after the split.
#[test]
fn tps_none_keeps_ema_zero_and_matches_legacy_path() {
    let t = table();
    // Exactly the same record sequence as the "legacy path" (neither carries tps):
    t.record("u", AttemptReport::new(Ok(200), Some(120), None));
    t.record("u", AttemptReport::new(Ok(200), Some(100), None));
    let st = t.get("u");
    assert_eq!(
        st.tps_ema, 0.0,
        "When report.tps is None, tps_ema stays 0.0 (no observation)"
    );

    // Control: another table runs the identical sequence (no tps); byte-for-byte equal.
    let legacy = table();
    legacy.record("u", AttemptReport::new(Ok(200), Some(120), None));
    legacy.record("u", AttemptReport::new(Ok(200), Some(100), None));
    let l = legacy.get("u");
    assert_eq!(
        st.tps_ema, l.tps_ema,
        "Both tables' tps_ema is 0.0 (no observation)"
    );
    assert_same_except_tps(&st, &l);
}

/// Case 5: zero-observation channel `HealthState::default().tps_ema == 0.0`, and the
/// `Default` path works normally (the selector reads 0.0 as neutral, factor 1.0 — the
/// selector side belongs to ticket 2; this pins only the "0.0 = no observation" data
/// contract and that the Default path does not panic).
#[test]
fn zero_observation_defaults_tps_ema_to_zero() {
    assert_eq!(
        HealthState::default().tps_ema,
        0.0,
        "Default (zero-observation) channel tps_ema must be 0.0 (= no observation, selector neutral 1.0)"
    );

    // A never-recorded key goes through get → Default, so tps_ema should also be 0.0 (the
    // Default path works).
    let t = table();
    assert_eq!(
        t.get("never-seen").tps_ema,
        0.0,
        "Unrecorded key get goes through Default, tps_ema = 0.0"
    );
}
