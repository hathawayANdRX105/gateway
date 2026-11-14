//! Token budget core — preclaim/settle RAII, no leak under concurrency, window decisions,
//! EMA calibration.
//!
//! A single test file that finishes locally within 3 seconds; the concurrency stress shape
//! imitates the 8-thread form of `tests/concurrency.rs`. Every case states "which bug turns
//! it red".

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use gateway_dispatch::token_budget::{COMPLETION_EMA_INIT, TokenBudgetState};

// ---------- preclaim / inflight / Drop full refund ----------

#[test]
fn preclaim_inflight_and_drop_returns_full() {
    // bug: drop does not refund the estimate (RAII leak) → inflight stays at 500, the
    // assertion of 0 goes red.
    let state = TokenBudgetState::new();
    assert_eq!(state.inflight("chA"), 0);

    let handle = state.preclaim("chA", 500);
    assert_eq!(
        state.inflight("chA"),
        500,
        "after preclaim, inflight should equal estimate"
    );

    drop(handle);
    assert_eq!(
        state.inflight("chA"),
        0,
        "after handle drop, inflight should be fully refunded"
    );
}

// ---------- settle: over-estimate refund / under-estimate top-up / repeated settle ----------

#[test]
fn settle_over_estimate_refunds_delta() {
    // bug: over-estimation does not refund the delta (only increases) → inflight is still
    // 500 after settle, the assertion of 100 goes red.
    let state = TokenBudgetState::new();
    let handle = state.preclaim("chA", 500);
    assert_eq!(state.inflight("chA"), 500);

    handle.settle_actual(100, 80); // measured 100 < estimate 500 → refund the 400 delta
    assert_eq!(
        state.inflight("chA"),
        100,
        "after settle, inflight should return to measured"
    );

    // Second settle is idempotent: no repeated refund.
    handle.settle_actual(999, 999);
    assert_eq!(
        state.inflight("chA"),
        100,
        "repeat settle should not change inflight"
    );

    drop(handle);
    assert_eq!(
        state.inflight("chA"),
        0,
        "drop refunds current hold (measured 100)"
    );
}

#[test]
fn settle_under_estimate_fills_delta() {
    // bug: under-estimation does not top up the delta (floored at the estimate) → inflight
    // is still 100 after settle, the assertion of 500 goes red.
    let state = TokenBudgetState::new();
    let handle = state.preclaim("chB", 100);
    assert_eq!(state.inflight("chB"), 100);

    handle.settle_actual(500, 300); // measured 500 > estimate 100 → top up 400
    assert_eq!(
        state.inflight("chB"),
        500,
        "after settle, inflight should top up to measured"
    );

    drop(handle);
    assert_eq!(state.inflight("chB"), 0);
}

// ---------- EMA: initial value + convergence toward the sample ----------

#[test]
fn ema_init_and_converges_toward_sample() {
    // bug: settle does not feed the EMA (frozen at the initial value) → the first settle
    // ≠ 388, assertion red; a wrong α convention (e.g. 0.7) is red too.
    let state = TokenBudgetState::new();
    assert_eq!(
        state.completion_ema("chC"),
        COMPLETION_EMA_INIT,
        "without observations, should be initial value 512"
    );

    // First settle (completion=100): α=0.3 ⇒ (512·7 + 100·3)/10 = 388, pinning the α
    // convention.
    let h1 = state.preclaim("chC", 100);
    h1.settle_actual(100, 100);
    assert_eq!(
        state.completion_ema("chC"),
        388,
        "α=0.3 first settle EMA should be 388"
    );
    drop(h1);

    // Feed the same sample twice more → converge monotonically toward 100
    // (512 → 388 → 301 → 240).
    for _ in 0..2 {
        let h = state.preclaim("chC", 100);
        h.settle_actual(100, 100);
        drop(h);
    }
    let after = state.completion_ema("chC");
    assert!(
        after < 300,
        "after 3 feeds of 100, EMA should be < 300 (converging toward sample), actual {after}"
    );
}

// ---------- Window decision: unregistered passes / full rejects / 60s rolling recovery ----------

#[test]
fn admit_window_unregistered_allows_full_rejects_and_recovers() {
    // bug: cap==0 is not short-circuited (unregistered channel wrongly rejected) or the
    // window does not roll (still rejected after 61s) → the corresponding assertion goes
    // red.
    let now = Arc::new(AtomicU64::new(0));
    let clock = {
        let n = Arc::clone(&now);
        move || n.load(Ordering::Relaxed)
    };
    let state = TokenBudgetState::with_clock(clock);

    assert!(
        state.admit_window("free", 1_000_000),
        "unregistered channel cap=0 should always pass"
    );

    state.register_channel("cap1000", 1000);
    assert!(
        state.admit_window("cap1000", 600),
        "cumulative 600 < 1000 should pass"
    );
    assert!(
        !state.admit_window("cap1000", 500),
        "600 + 500 = 1100 > 1000 strictly rejected"
    );
    assert!(
        state.admit_window("cap1000", 400),
        "rejected 500 not counted; 600 + 400 = 1000 exactly full, allowed"
    );
    assert!(
        !state.admit_window("cap1000", 1),
        "full at 1000, +1 over limit, rejected"
    );

    // 60s window rolls: advance the clock by 61s, the old second buckets leave the
    // window → recovery.
    now.store(61_000, Ordering::Relaxed);
    assert!(
        state.admit_window("cap1000", 400),
        "after 60s window slide, old occupancy leaves window, should resume passing"
    );
}

// ---------- Global default: channels without an override follow it ----------

#[test]
fn unregistered_channel_follows_global_default() {
    // Purpose: the global default installed by `set_global_default` is the cap for every
    // channel without a `tpm_limit` override — the documented contract of the knob
    // (`Config::token_tpm_default`).
    // Scenarios: no default installed → unlimited; default installed → unregistered
    // channel follows it (cap / load / window); per-channel override still wins.
    // Acceptance: cap and the window decision track the default, and the override wins.
    // bug: `cap` ignores `global_default` (knob stored but never enforced) → the channel
    // stays unlimited after `set_global_default` and these assertions go red.
    let now = Arc::new(AtomicU64::new(0));
    let clock = {
        let n = Arc::clone(&now);
        move || n.load(Ordering::Relaxed)
    };
    let state = TokenBudgetState::with_clock(clock);

    assert_eq!(state.cap("ch"), 0, "no override and no default = unlimited");
    assert!(
        state.admit_window("ch", 1_000_000),
        "no cap in effect should always pass"
    );

    state.set_global_default(1_000);
    assert_eq!(state.cap("ch"), 1_000, "no override → global default");
    assert_eq!(state.load("ch"), Some(0.0), "no in-flight → load 0");
    assert!(state.admit_window("ch", 600), "600 < 1000 passes");
    assert!(!state.admit_window("ch", 500), "600 + 500 > 1000 rejected");

    state.register_channel("ch", 2_000);
    assert_eq!(state.cap("ch"), 2_000, "per-channel override wins");
    assert!(
        !state.admit_window("ch", 1_500),
        "600 already in window + 1500 > 2000 rejected"
    );
    assert!(
        state.admit_window("ch", 1_000),
        "600 + 1000 = 1600 < 2000 passes"
    );
}

// ---------- Concurrency without leaks (imitating the 8-thread shape of concurrency.rs) ----------

#[test]
fn concurrent_preclaim_settle_drop_no_leak() {
    // bug: a drop/settle race causes a double refund or a missed refund → final
    // inflight ≠ 0, assertion red.
    // 8 threads × 200, one shared channel: each preclaims(7); half settle
    // (measured=estimate), half drop directly.
    let state = Arc::new(TokenBudgetState::new());

    let mut handles = Vec::new();
    for _ in 0..8 {
        let state = Arc::clone(&state);
        handles.push(std::thread::spawn(move || {
            for i in 0..200 {
                let h = state.preclaim("hot", 7);
                if i % 2 == 0 {
                    h.settle_actual(7, 5); // measured total tokens=7 (= estimate), delta refunded 0
                }
                drop(h); // return at the end of every round; the final state must be zero
            }
        }));
    }
    for h in handles {
        h.join().unwrap();
    }
    assert_eq!(
        state.inflight("hot"),
        0,
        "after concurrent preclaim/settle/drop, inflight must return to zero"
    );
}
