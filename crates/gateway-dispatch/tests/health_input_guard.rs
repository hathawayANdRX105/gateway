//! Behavior tests for the health-table ingest boundary guards (`record` / `record_tps` /
//! `set_config`).
//!
//! Pins four boundary semantics (spec `api_endpoint_security`: ingest validation plus
//! read contract):
//! 1. An empty `unit_key` is silently dropped at the `record` / `record_tps` entry (debug
//!    log) and **creates no table entry** — the admin read side never sees an empty-key
//!    entry;
//! 2. Non-physical throughput samples (NaN / Inf / ≤0) are dropped **before entry
//!    creation**, so a bad sample neither creates an entry nor contaminates an existing
//!    entry's EMA;
//! 3. An invalid config is rejected wholesale by `set_config` (warn plus keeping the old
//!    config), while a valid new config takes effect as usual;
//! 4. Existing semantics for valid input are unchanged (the guards do not hit the hot
//!    path).
//!
//! Synchronous, no tokio: all `MemoryHealthTable` state lives in an in-memory Mutex, and
//! the injected clock from `with_config_and_clock` is fixed — same setup as the existing
//! health_* tests.

use gateway_dispatch::health::{
    AttemptReport, FailureClass, HealthSetting, HealthTable, MemoryHealthTable, TpsSample,
};

/// In-memory health table with a fixed clock (`now_ms` is always `1_000_000`).
fn table() -> MemoryHealthTable {
    let now = 1_000_000u64;
    MemoryHealthTable::with_config_and_clock(HealthSetting::default(), move || now)
}

/// `record` / `record_tps` always drop an empty `unit_key`: the table stays empty.
///
/// Which bug would make this red: a missing guard, or one placed after entry creation —
/// `""` becomes a real entry, `entries()` is non-empty (the admin read side gains a
/// phantom empty-key channel), or `record("")` panics inside the lock/constructor.
#[test]
fn empty_unit_key_is_dropped() {
    let t = table();

    // Full payload with latency and tps: the guard must run before any accounting or
    // digest write.
    t.record(
        "",
        AttemptReport::new(Ok(200), Some(42), None).with_tps(TpsSample::Measured(100.0)),
    );
    t.record(
        "",
        AttemptReport::new(Err(FailureClass::Retryable), None, None),
    );
    t.record_tps("", TpsSample::Measured(100.0));
    t.record_tps("", TpsSample::Estimated(50.0));

    assert!(
        t.entries().is_empty(),
        "empty unit_key must not create a table entry; entries must be empty"
    );
    // Read-side fallback: get("") still returns the default full-health state (there is
    // no such record in the table at all).
    assert_eq!(
        t.get("").request_count,
        0,
        "empty key must not be accounted"
    );
}

/// Non-physical throughput samples (NaN / Inf / ≤0) are dropped: no entry is created
/// first, and an existing EMA is not contaminated.
///
/// Which bug would make this red: missing sample validation, or validation placed after
/// `states.insert` — a bad sample creates an empty-key entry, or merges NaN / negative
/// values into `tps_ema` (the first-sample direct path yields ema=NaN, every later
/// comparison is ruined, and the selector reads a NaN weight).
#[test]
fn non_physical_tps_samples_are_dropped() {
    let t = table();
    let key = "ch-1/gpt-4o";

    t.record_tps(key, TpsSample::Measured(f64::NAN));
    t.record_tps(key, TpsSample::Estimated(f64::INFINITY));
    t.record_tps(key, TpsSample::Measured(-1.0));
    t.record_tps(key, TpsSample::Estimated(0.0));
    assert!(
        t.entries().is_empty(),
        "bad sample must not create a table entry (validation must precede entry creation)"
    );

    // A valid sample establishes the baseline normally: the first sample is taken
    // directly → 100.0.
    t.record_tps(key, TpsSample::Measured(100.0));
    assert_eq!(
        t.get(key).tps_ema,
        100.0,
        "valid sample should become EMA baseline"
    );

    // After the baseline exists, feed a bad sample: the EMA must stay untouched.
    t.record_tps(key, TpsSample::Measured(f64::NAN));
    assert_eq!(
        t.get(key).tps_ema,
        100.0,
        "NaN sample must not contaminate existing EMA"
    );

    // A tps sample carried along by record goes through the same guard.
    t.record(
        key,
        AttemptReport::new(Ok(200), None, None).with_tps(TpsSample::Measured(f64::INFINITY)),
    );
    assert_eq!(
        t.get(key).tps_ema,
        100.0,
        "non-physical samples in report.tps are dropped at the entry boundary too"
    );
}

/// `set_config` rejects an invalid config wholesale and keeps the old one; a valid new
/// config takes effect as usual.
///
/// Which bug would make this red: missing validation, or "partial application" — a bad
/// value enters the config slot directly (α=NaN turns the whole EWMA table into NaN;
/// base > max degenerates the cooldown duration formula), or the old config is cleared or
/// half-written so that `get_config()` no longer equals the pre-rejection value.
#[test]
fn invalid_setting_keeps_old_config() {
    let t = table();
    let before = t.get_config();

    // α = NaN: a non-finite value poisons the entire EWMA.
    t.set_config(HealthSetting {
        alpha: f64::NAN,
        ..before
    });
    assert_eq!(
        t.get_config(),
        before,
        "alpha=NaN must be rejected and old config kept field by field"
    );

    // base exceeds the ceiling: the ladder starts above the cap, so the duration formula
    // degenerates.
    t.set_config(HealthSetting {
        cooldown_base_ms: 9_000,
        cooldown_max_ms: 3_000,
        ..before
    });
    assert_eq!(
        t.get_config(),
        before,
        "cooldown_base_ms > cooldown_max_ms must be rejected and old config kept field by field"
    );

    // A valid new config takes effect as usual (validation does not break hot updates).
    t.set_config(HealthSetting {
        cooldown_max_ms: 16_000,
        ..t.get_config()
    });
    assert_eq!(
        t.get_config().cooldown_max_ms,
        16_000,
        "valid new config should take effect"
    );
}

/// Existing semantics for valid input are unchanged: a success stays selectable and a
/// single transport-layer failure enters cooldown.
///
/// Which bug would make this red: an over-broad guard (for example dropping reports that
/// "carry tps" or whose "key contains /") swallows valid reports — the channel stays at
/// full health forever and failures never enter cooldown.
#[test]
fn valid_behavior_unchanged() {
    let t = table();
    let now_ms = 1_000_000u64;
    let key = "ch-2/gpt-4o";

    t.record(key, AttemptReport::new(Ok(200), Some(40), None));
    assert!(
        t.is_selectable(key, now_ms),
        "channel should be selectable after successful report"
    );

    t.record(
        key,
        AttemptReport::new(Err(FailureClass::Retryable), None, None),
    );
    assert!(
        !t.is_selectable(key, now_ms),
        "single transport-layer failure enters cooldown (existing semantics; guards must not misfire)"
    );
}

/// `validate()` rule contract: the default config is valid, and each violating field
/// yields its own stable reason string.
///
/// Which bug would make this red: rule drift (a non-finite α accepted, the base/ceiling
/// comparison direction reversed, out-of-range endpoints relaxed), or a changed error
/// string — the string is shared by the `set_config` rejection log and the boot-path
/// fallback log, so drift makes the two warnings attribute the failure differently.
#[test]
fn validate_rules() {
    let base = HealthSetting::default();
    assert!(
        base.validate().is_ok(),
        "default config must be valid (rules must not misfire on defaults)"
    );

    let alpha_nan = HealthSetting {
        alpha: f64::NAN,
        ..base
    };
    assert_eq!(
        alpha_nan.validate(),
        Err("alpha must be finite and in [0, 1]"),
        "alpha=NaN must be rejected with a stable reason string"
    );

    let alpha_oob = HealthSetting { alpha: 1.5, ..base };
    assert_eq!(
        alpha_oob.validate(),
        Err("alpha must be finite and in [0, 1]"),
        "alpha above upper bound must be rejected with a stable reason string"
    );

    let min_score_oob = HealthSetting {
        min_score: -0.1,
        ..base
    };
    assert_eq!(
        min_score_oob.validate(),
        Err("min_score must be finite and in [0, 1]"),
        "min_score below lower bound must be rejected with a stable reason string"
    );

    let base_over_max = HealthSetting {
        cooldown_base_ms: 9_000,
        cooldown_max_ms: 3_000,
        ..base
    };
    assert_eq!(
        base_over_max.validate(),
        Err("cooldown_base_ms must not exceed cooldown_max_ms"),
        "base above fatal ceiling must be rejected with a stable reason string"
    );

    let base_over_throttle = HealthSetting {
        cooldown_base_ms: 5_000,
        cooldown_max_ms: 8_000,
        throttle_max_ms: 1_000,
        ..base
    };
    assert_eq!(
        base_over_throttle.validate(),
        Err("cooldown_base_ms must not exceed throttle_max_ms"),
        "base above 429 ceiling must be rejected with a stable reason string"
    );
}
