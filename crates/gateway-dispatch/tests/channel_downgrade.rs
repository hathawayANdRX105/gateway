//! P1-B channel-scoped 4xx demotion — the loop-decision contract of `run_retry_loop`.
//!
//! Behaviours under test (all observable outcomes of "switch candidate or not / what the
//! client finally sees", not wiring):
//! 1. A channel-scoped 4xx (401/403/404 shape → `FatalButSwitchable`) on the first
//!    candidate → that candidate enters the exclude set, the second is selected and wins;
//!    the health report payload is `Err(Fatal)`, i.e. `health::classify`'s Neutral semantics
//!    (no EWMA change, no streak);
//! 2. A request-scoped 4xx (400 shape → `Fatal`) with a single candidate → pass through
//!    directly: one attempt terminates the loop, no second select is issued;
//! 3. `FatalButSwitchable` on the last candidate → the loop carries the result out as Ok
//!    for the upper layer to pass through, rather than disguising it as `Unavailable`
//!    (503 without cooldown / 504 with one);
//! 4. The converse of "only the last candidate passes through": after a switchable failure
//!    a Retryable candidate is tried and no candidate is left → still Err(AllUnavailable);
//!    the stashed 4xx must not come back.
//!
//! Pure classification assertions for `classify_channel_scope` live in `failure_scope.rs`;
//! the egress status-code → `channel_scoped` mapping is consumed by the stage closure, and
//! the end-to-end shape is covered by `bin/api/tests/route_resolution.rs` P0-2.

use gateway_core::records::{ChannelSettings, RouteUnitRecord, SyncMeta};
use gateway_dispatch::health::FailureClass;
use gateway_dispatch::{
    AttemptOutcome, Candidate, DispatchError, FakeTimeControl, RetryPolicy, run_retry_loop,
};
use gateway_pipeline::SelectedRoute;

fn candidate(key: &str) -> Candidate {
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

/// Planned result of a single attempt. `AttemptOutcome` is not Clone, so it is built per key
/// on demand.
#[derive(Clone, Copy, PartialEq)]
enum Plan {
    /// Upstream 200.
    Ok,
    /// Channel-scoped 4xx (the loop payload for egress 401/403/404 → `channel_scoped`).
    Switch,
    /// Retryable failure (5xx / transport-layer shape).
    Retry,
    /// Request-scoped 4xx (400 shape, no channel switch).
    Fatal,
}

fn outcome(plan: Plan) -> AttemptOutcome {
    match plan {
        Plan::Ok => AttemptOutcome::Done {
            status: 200,
            latency_ms: 1,
        },
        Plan::Switch => AttemptOutcome::FatalButSwitchable(FailureClass::Fatal, None),
        Plan::Retry => AttemptOutcome::Retryable(FailureClass::Retryable, None),
        Plan::Fatal => AttemptOutcome::Fatal(FailureClass::Fatal, None),
    }
}

struct Harness {
    pool: Vec<Candidate>,
    plans: Vec<(&'static str, Plan)>,
    /// Exclude set received by each select.
    selects: Vec<Vec<String>>,
    /// (`unit_key`, outcome) received by each report.
    reports: Vec<(String, Result<u16, FailureClass>)>,
}

impl Harness {
    async fn run(
        mut self,
    ) -> (
        Result<(gateway_dispatch::Attempt, AttemptOutcome), DispatchError>,
        Vec<Vec<String>>,
        Vec<(String, Result<u16, FailureClass>)>,
    ) {
        let result = run_retry_loop(
            "g",
            "m",
            &RetryPolicy::default(),
            // Sticky key (None = not sticky): byte-for-byte equivalent to the status quo.
            None,
            // Context-window floor (None = unconstrained; this harness pins routing only).
            None,
            |g, m, exclude, _sticky, _floor| {
                self.selects.push(exclude.to_vec());
                self.pool
                    .iter()
                    .find(|c| !exclude.contains(&c.unit.meta.key))
                    .cloned()
                    .ok_or(DispatchError::AllUnavailable {
                        group: g.to_string(),
                        model: m.to_string(),
                        retry_after_ms: 0,
                    })
            },
            |c| {
                let plan = self
                    .plans
                    .iter()
                    .find(|(k, _)| *k == c.unit.meta.key)
                    .map(|(_, p)| *p)
                    .expect("plan for candidate");
                std::future::ready(outcome(plan))
            },
            |key, report: gateway_dispatch::AttemptReport| {
                self.reports.push((key.to_string(), report.outcome));
            },
            &FakeTimeControl::default(),
        )
        .await;
        (result, self.selects, self.reports)
    }
}

#[tokio::test]
async fn channel_scoped_4xx_first_candidate_switches_to_second() {
    let (result, selects, reports) = Harness {
        pool: vec![candidate("A"), candidate("B")],
        plans: vec![("A", Plan::Switch), ("B", Plan::Ok)],
        selects: Vec::new(),
        reports: Vec::new(),
    }
    .run()
    .await;

    let (win, outcome) = result.expect("A channel-scoped 4xx should be demoted to B and succeed");
    assert_eq!(
        win.candidate.unit.meta.key, "B",
        "the second candidate must be selected"
    );
    assert_eq!(win.attempt_no, 2);
    assert!(matches!(
        outcome,
        AttemptOutcome::Done {
            status: 200,
            latency_ms: 1
        }
    ));
    // Candidate switching relies on the exclude set: the second select must exclude A.
    assert_eq!(selects, vec![Vec::<String>::new(), vec!["A".to_string()]]);
    // Health reports: A goes through Err(Fatal) = Neutral semantics (does not pollute the
    // EWMA), B records 200.
    assert_eq!(
        reports,
        vec![
            ("A".to_string(), Err(FailureClass::Fatal)),
            ("B".to_string(), Ok(200)),
        ]
    );
}

#[tokio::test]
async fn request_scoped_400_single_candidate_passes_through_immediately() {
    let (result, selects, reports) = Harness {
        pool: vec![candidate("A")],
        plans: vec![("A", Plan::Fatal)],
        selects: Vec::new(),
        reports: Vec::new(),
    }
    .run()
    .await;

    let (win, outcome) = result.expect("request-scoped 4xx returned as Ok(Fatal) terminal state");
    assert_eq!(win.candidate.unit.meta.key, "A");
    assert_eq!(win.attempt_no, 1);
    assert!(matches!(outcome, AttemptOutcome::Fatal(_, _)));
    // Key behaviour: no channel switch = exactly one select.
    assert_eq!(
        selects.len(),
        1,
        "400 shape must not trigger candidate switch"
    );
    assert_eq!(reports, vec![("A".to_string(), Err(FailureClass::Fatal))]);
}

#[tokio::test]
async fn last_candidate_channel_scoped_passes_through_not_no_route() {
    let (result, _selects, _reports) = Harness {
        pool: vec![candidate("A")],
        plans: vec![("A", Plan::Switch)],
        selects: Vec::new(),
        reports: Vec::new(),
    }
    .run()
    .await;

    // The only candidate returns 404: the client should get the upstream's real 4xx (the
    // loop carries the switchable terminal outcome out), not a masking 503
    // `all_candidates_unavailable`.
    let (last, outcome) = result.expect("last candidate's channel-scoped 4xx must pass through");
    assert_eq!(last.candidate.unit.meta.key, "A");
    assert!(matches!(outcome, AttemptOutcome::FatalButSwitchable(_, _)));
}

#[tokio::test]
async fn stale_switchable_does_not_resurrect_after_retryable_last_failure() {
    let (result, _selects, _reports) = Harness {
        pool: vec![candidate("A"), candidate("B")],
        plans: vec![("A", Plan::Switch), ("B", Plan::Retry)],
        selects: Vec::new(),
        reports: Vec::new(),
    }
    .run()
    .await;

    // "Only the last candidate passes through": the final failure is Retryable (not
    // switchable), the stash is cleared, and with no candidate left the loop returns
    // AllUnavailable (no cooldown → retry_after_ms=0); A's stashed 4xx must not come back.
    assert!(
        matches!(result, Err(DispatchError::AllUnavailable { .. })),
        "got {result:?}"
    );
}
