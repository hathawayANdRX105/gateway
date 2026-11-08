//! Full semantics of `ChannelOutcome::Neutral`: the channel is not at fault — it
//! does not change the EWMA, does not count a request, and does not enter cooldown.
//!
//! Historically this file also held a regression test: "alternating Fatal / Neutral
//! reports must accumulate `failure_streak` to the cooldown threshold" — because the
//! `Neutral` branch at the time cleared the streak, so an alternating 500/404 sequence
//! never reached the threshold. After cooldown changed to "any failure triggers
//! immediately", that regression can no longer occur (the `failure_streak` field
//! itself was removed with it), so the test was removed too.

use gateway_dispatch::health::{AttemptReport, HealthSetting, HealthTable, MemoryHealthTable};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

const T0: u64 = 1_000_000;

fn clock_table(now: &Arc<AtomicU64>) -> MemoryHealthTable {
    let clock = {
        let now = now.clone();
        move || now.load(Ordering::Relaxed)
    };
    MemoryHealthTable::with_config_and_clock(HealthSetting::default(), clock)
}

#[test]
fn neutral_still_skips_score_and_request_count() {
    let now = Arc::new(AtomicU64::new(T0));
    let table = clock_table(&now);

    // Build a trustworthy EWMA first (6 successes, above min_requests=5).
    for _ in 0..6 {
        table.record("ch", AttemptReport::new(Ok(200), None, None));
    }
    let score_before = table.get("ch").ewma_score;
    let count_before = table.get("ch").request_count;

    table.record("ch", AttemptReport::new(Ok(422), None, None));
    let st = table.get("ch");
    assert_eq!(
        st.request_count, count_before,
        "Neutral does not count requests"
    );
    assert_eq!(
        st.ewma_score, score_before,
        "Neutral does not change EWMA score"
    );
}

/// The boundary between `Neutral` and `Fatal`: the former blames no channel, the
/// latter enters cooldown immediately. Cooldown no longer has an "N consecutive
/// failures" threshold, so a **single** 5xx is enough to eject the channel.
#[test]
fn neutral_never_cools_but_single_fatal_does() {
    let now = Arc::new(AtomicU64::new(T0));
    let table = clock_table(&now);

    for _ in 0..20 {
        table.record("neutral_ch", AttemptReport::new(Ok(404), None, None));
    }
    assert_eq!(
        table.get("neutral_ch").cooldown_until_ms,
        0,
        "20 404s should not enter cooldown"
    );
    assert!(table.is_selectable("neutral_ch", T0));

    table.record(
        "fatal_ch",
        AttemptReport::new(
            Err(gateway_dispatch::health::FailureClass::Retryable),
            None,
            None,
        ),
    );
    let st = table.get("fatal_ch");
    assert!(
        st.is_cooling(T0),
        "A single transport-layer failure immediately enters cooldown (cooldown has no threshold)"
    );
    assert_eq!(
        st.cooldown_until_ms - T0,
        300,
        "First hit lands on base tier = cooldown_base_ms"
    );
}
