//! Behavior tests for S2 (the failover time accumulator) `run_retry_loop`.
//!
//! S2 semantics: the 45s budget accumulates only [failed-attempt latency] + [cooldown
//! waits]; **successful connection time is not counted** (P1: success neither spends
//! budget nor clamps the egress timeout). Not wall-clock: scheduling gaps while a busy
//! server handles the request are not counted.
//!
//! Clock/sleep injection: `run_retry_loop`'s last parameter `time: &dyn TimeControl`. All
//! tests use [`FakeTimeControl`] (cooldown waits take 0ms real time, never blocking CI) and
//! assert through its `cooled_ms` whether a wait happened — **no real sleep**, 0ms wall-clock.
//!
//! Assertions land only on **deterministic end states** (error variant / attempt count /
//! cooled_ms), never on jitter-prone quantities.

use app_contract::records::{RouteUnitRecord, SyncMeta};
use gateway_dispatch::DispatchError;
use gateway_dispatch::candidate::Candidate;
use gateway_dispatch::health::FailureClass;
use gateway_dispatch::retry::{AttemptOutcome, FakeTimeControl, RetryPolicy, run_retry_loop};
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};

fn cand(key: &str) -> Candidate {
    let unit = RouteUnitRecord {
        meta: SyncMeta {
            key: key.to_string(),
            schema_version: 1,
            logical_version: 1,
            origin: "test".to_string(),
            updated_at: chrono::Utc::now(),
        },
        group: "default".to_string(),
        public_model: "m".to_string(),
        channel_key: key.to_string(),
        key_index: 0,
        upstream_model: "m".to_string(),
        weight: 10,
        status: 1,
    };
    Candidate {
        unit,
        secret: "s".into(),
        base_url: "https://u".into(),
        upstream_model: "m".into(),
        provider_type: "openai".into(),
        settings: serde_json::Value::Null,
    }
}

fn rt() -> tokio::runtime::Runtime {
    tokio::runtime::Runtime::new().unwrap()
}

/// P1 + S2: a single cooling channel recovers within budget → success; the cooldown wait
/// **does not burn an attempt** (`max_attempts = 1` is still left for the real upstream
/// attempt), and the fake clock's `cooled_ms` records the wait (really waited, 0ms
/// wall-clock).
#[test]
fn all_cooling_recovers_within_budget_and_wait_does_not_burn_attempt() {
    let select_calls = Arc::new(AtomicU32::new(0));
    let attempt_calls = Arc::new(AtomicU32::new(0));
    let acalls = Arc::clone(&attempt_calls);
    let fake = FakeTimeControl::default();

    let result = rt().block_on(run_retry_loop(
        "default",
        "m",
        &RetryPolicy {
            max_attempts: 1,
            total_budget_ms: 500,
        },
        None,
        |_, _, _, _| {
            let n = select_calls.fetch_add(1, Ordering::SeqCst);
            // First select: all cooling → AllUnavailable (recovers after 10ms); after the
            // loop sleeps, the next select recovers → Ok.
            if n == 0 {
                Err(DispatchError::AllUnavailable {
                    group: "default".to_string(),
                    model: "m".to_string(),
                    retry_after_ms: 10,
                })
            } else {
                Ok(cand("c1"))
            }
        },
        |_| {
            let acalls = Arc::clone(&acalls);
            async move {
                acalls.fetch_add(1, Ordering::SeqCst);
                AttemptOutcome::Done {
                    status: 200,
                    latency_ms: 1,
                }
            }
        },
        |_k, _report| {},
        &fake,
    ));

    assert!(
        matches!(result, Ok((_, AttemptOutcome::Done { status: 200, .. }))),
        "after cooldown recovery should succeed, got {result:?}"
    );
    assert_eq!(
        attempt_calls.load(Ordering::SeqCst),
        1,
        "wait does not burn attempt: max_attempts=1 still left for real upstream attempt"
    );
    assert_eq!(
        fake.cooled_ms.load(Ordering::SeqCst),
        10,
        "fake clock should record this 10ms cooldown wait (0ms real time)"
    );
}

/// All cooling but recovery time (10s) far exceeds the budget (50ms) → **no wait**
/// (consumed+ra would exceed it); return `AllUnavailable` directly. Deterministic: the fake
/// clock's `cooled_ms` is 0 (it really did not wait).
#[test]
fn all_cooling_recovery_over_budget_returns_all_unavailable() {
    let attempt_calls = Arc::new(AtomicU32::new(0));
    let acalls = Arc::clone(&attempt_calls);
    let fake = FakeTimeControl::default();
    let result = rt().block_on(run_retry_loop(
        "default",
        "m",
        &RetryPolicy {
            max_attempts: 3,
            total_budget_ms: 50,
        },
        None,
        |g, m, _, _| {
            Err(DispatchError::AllUnavailable {
                group: g.to_string(),
                model: m.to_string(),
                retry_after_ms: 10_000,
            })
        },
        |_| {
            let acalls = Arc::clone(&acalls);
            async move {
                acalls.fetch_add(1, Ordering::SeqCst);
                AttemptOutcome::Done {
                    status: 200,
                    latency_ms: 1,
                }
            }
        },
        |_k, _report| {},
        &fake,
    ));
    assert!(
        matches!(
            result,
            Err(DispatchError::AllUnavailable {
                retry_after_ms: 10_000,
                ..
            })
        ),
        "recovery over budget should directly return AllUnavailable(with 10s Retry-After), got {result:?}"
    );
    assert_eq!(
        attempt_calls.load(Ordering::SeqCst),
        0,
        "recovery over budget must never hit upstream"
    );
    assert_eq!(
        fake.cooled_ms.load(Ordering::SeqCst),
        0,
        "over-budget cooldown wait must never sleep (fake clock cooled_ms should be 0)"
    );
}

/// The snapshot has no such model at all (missing configuration) → immediate
/// `ModelUnknown` (404), no upstream call, no wait.
#[test]
fn model_unknown_returns_immediately_without_attempt() {
    let attempt_calls = Arc::new(AtomicU32::new(0));
    let acalls = Arc::clone(&attempt_calls);
    let fake = FakeTimeControl::default();
    let result = rt().block_on(run_retry_loop(
        "default",
        "m",
        &RetryPolicy {
            max_attempts: 3,
            total_budget_ms: 500,
        },
        None,
        |g, m, _, _| {
            Err(DispatchError::ModelUnknown {
                group: g.to_string(),
                model: m.to_string(),
            })
        },
        |_| {
            let acalls = Arc::clone(&acalls);
            async move {
                acalls.fetch_add(1, Ordering::SeqCst);
                AttemptOutcome::Done {
                    status: 200,
                    latency_ms: 1,
                }
            }
        },
        |_k, _report| {},
        &fake,
    ));
    assert!(
        matches!(result, Err(DispatchError::ModelUnknown { .. })),
        "no such model should be ModelUnknown(404), got {result:?}"
    );
    assert_eq!(
        attempt_calls.load(Ordering::SeqCst),
        0,
        "ModelUnknown must not hit any upstream"
    );
}

/// S2 time budget: failed-attempt latency accumulates (`consumed += latency_ms`); once it
/// exceeds the budget (50ms) the result is `BudgetExhausted` (**time** exhausted, unrelated
/// to `max_attempts`). Each failed attempt has `latency_ms = 30`: after the 1st consumed=30
/// (≤50, continue), after the 2nd consumed=60 (>50, stop) → exactly 2 attempts.
/// Deterministic (fake clock, 0ms real time).
#[test]
fn failed_attempts_accumulate_until_budget_exhausted() {
    let attempt_calls = Arc::new(AtomicU32::new(0));
    let acalls = Arc::clone(&attempt_calls);
    let fake = FakeTimeControl::default();
    let result = rt().block_on(run_retry_loop(
        "default",
        "m",
        &RetryPolicy {
            max_attempts: 100, // high enough that the time budget exhausts first (not count)
            total_budget_ms: 50,
        },
        None,
        |_, _, _, _| Ok(cand("c1")),
        |_| {
            let acalls = Arc::clone(&acalls);
            async move {
                acalls.fetch_add(1, Ordering::SeqCst);
                // Each failure costs 30ms (latency evidence), accumulated 30+30=60 > 50.
                AttemptOutcome::Retryable(FailureClass::Retryable, Some(30))
            }
        },
        |_k, _report| {},
        &fake,
    ));
    assert!(
        matches!(result, Err(DispatchError::BudgetExhausted { .. })),
        "failed-attempt latency accumulating over budget should be BudgetExhausted, got {result:?}"
    );
    assert_eq!(
        attempt_calls.load(Ordering::SeqCst),
        2,
        "30+30=60>50: exactly 2 failed attempts then time budget exhausted"
    );
}

/// P1: a healthy channel succeeds on the **first attempt** — the 45s budget is not
/// "burned up front". Successful connection time is not counted (S2): the first select
/// is immediately Ok and the first attempt is Done.
#[test]
fn healthy_channel_succeeds_first_attempt_without_burning_budget() {
    let attempts = Arc::new(AtomicU32::new(0));
    let ac = Arc::clone(&attempts);
    let fake = FakeTimeControl::default();
    let result = rt().block_on(run_retry_loop(
        "default",
        "m",
        &RetryPolicy {
            max_attempts: 3,
            total_budget_ms: 45_000,
        },
        None,
        |_, _, _, _| Ok(cand("c1")),
        |_| {
            let ac = Arc::clone(&ac);
            async move {
                ac.fetch_add(1, Ordering::SeqCst);
                AttemptOutcome::Done {
                    status: 200,
                    latency_ms: 1,
                }
            }
        },
        |_k, _report| {},
        &fake,
    ));
    assert!(
        matches!(result, Ok((_, AttemptOutcome::Done { status: 200, .. }))),
        "healthy channel should succeed immediately (not 503 / not budget exhausted), got {result:?}"
    );
    assert_eq!(
        attempts.load(Ordering::SeqCst),
        1,
        "only 1 real attempt used, budget not wasted"
    );
}

/// P1 core invariant: **successful connection time does not count against the failover
/// budget**. Even a slow success (latency=400ms far above the 50ms budget) is still `Ok`,
/// not `BudgetExhausted` — S2 times only [failure/cooldown], success never enters the
/// accounting. (The old wall-clock model clipped a 400ms slow success to the 50ms
/// remaining → false exhaustion; S2 neither clips nor subtracts.)
#[test]
fn success_does_not_count_against_budget_even_when_slow() {
    let fake = FakeTimeControl::default();
    let result = rt().block_on(run_retry_loop(
        "default",
        "m",
        &RetryPolicy {
            max_attempts: 3,
            total_budget_ms: 50,
        },
        None,
        |_, _, _, _| Ok(cand("c1")),
        |_| async {
            // One slow success (400ms) — success, no budget charged.
            AttemptOutcome::Done {
                status: 200,
                latency_ms: 400,
            }
        },
        |_k, _report| {},
        &fake,
    ));
    assert!(
        matches!(result, Ok((_, AttemptOutcome::Done { status: 200, .. }))),
        "slow success (latency > budget) still Ok, not judged budget exhausted for success time, got {result:?}"
    );
}

/// All unavailable but **no channel is cooling** (e.g. all disabled, or the last failure
/// was a neutral 4xx that does not trigger cooldown) → `AllUnavailable.retry_after_ms == 0`
/// (no natural recovery). With retry_after_ms=0 the loop does not sleep (fake clock
/// `cooled_ms` should be 0) and returns the variant directly — deterministic.
#[test]
fn all_unavailable_without_cooling_reports_zero_retry_after() {
    let fake = FakeTimeControl::default();
    let result = rt().block_on(run_retry_loop(
        "default",
        "m",
        &RetryPolicy {
            max_attempts: 1,
            total_budget_ms: 0,
        },
        None,
        |_, _, _, _| {
            Err(DispatchError::AllUnavailable {
                group: "default".to_string(),
                model: "m".to_string(),
                retry_after_ms: 0,
            })
        },
        |_| async {
            AttemptOutcome::Done {
                status: 200,
                latency_ms: 1,
            }
        },
        |_k, _report| {},
        &fake,
    ));
    assert!(
        matches!(
            result,
            Err(DispatchError::AllUnavailable {
                retry_after_ms: 0,
                ..
            })
        ),
        "no cooling channel: AllUnavailable.retry_after_ms should be 0, got {result:?}"
    );
    assert_eq!(
        fake.cooled_ms.load(Ordering::SeqCst),
        0,
        "no cooling: should not sleep (fake clock cooled_ms should be 0)"
    );
}

/// Termination (bug regression): with the budget **off** (total_budget_ms=0) + cooling
/// (ra>0), do **not sleep on the gateway side waiting for cooldown** (no budget cap =
/// permanent cooling would spin forever). Return AllUnavailable{ra} directly; the client
/// waits via 504+Retry-After. The fake clock's cooled_ms must be 0 (proof of no wait), and
/// the loop terminates immediately (deterministic).
#[test]
fn cooling_with_budget_off_does_not_wait_or_loop() {
    let fake = FakeTimeControl::default();
    let result = rt().block_on(run_retry_loop(
        "default",
        "m",
        &RetryPolicy {
            max_attempts: 3,
            total_budget_ms: 0, // off
        },
        None,
        |g, m, _, _| {
            // Channel permanently cooling (ra>0 forever): with the budget off it must never
            // sleep → AllUnavailable immediately.
            Err(DispatchError::AllUnavailable {
                group: g.to_string(),
                model: m.to_string(),
                retry_after_ms: 8_000,
            })
        },
        |_| async {
            AttemptOutcome::Done {
                status: 200,
                latency_ms: 1,
            }
        },
        |_k, _report| {},
        &fake,
    ));
    assert!(
        matches!(
            result,
            Err(DispatchError::AllUnavailable {
                retry_after_ms: 8_000,
                ..
            })
        ),
        "budget off + cooling: immediately AllUnavailable(with ra), not blocking nor looping, got {result:?}"
    );
    assert_eq!(
        fake.cooled_ms.load(Ordering::SeqCst),
        0,
        "budget off: must never sleep waiting for cooldown (cooled_ms should be 0)"
    );
}
