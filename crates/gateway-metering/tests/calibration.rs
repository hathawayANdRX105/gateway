//! Calibration log — estimate-vs-reported prompt token pairs.
//!
//! Purpose: the ring-bounded calibration log that feeds issue #20 (weight-table
//! calibration) and the issue #9 (estimation-error anchor): deployment accumulates
//! samples on real traffic, then weights are adjusted from the report.
//!
//! Coverage:
//! 1. `report()` computes exact mean / p95 / max absolute percent error.
//! 2. The ring is bounded at 512 samples: 600 records keep 512, oldest dropped.
//! 3. A reported prompt of 0 is ignored (no usage was reported; avoids division by
//!    zero and carries no calibration signal).

use gateway_metering::calibration::{CalibrationLog, CalibrationReport};

/// Purpose: `report()` computes exact mean / p95 / max over a small sample set.
/// Scenario: two pairs with absolute percent errors 100% and 0%.
/// Acceptance: `n == 2`, `mean == 50`, `p95 == 100`, `max == 100`.
#[test]
fn report_computes_mean_p95_and_max() {
    let log = CalibrationLog::new();
    assert!(log.record(200, 100)); // |200 - 100| / 100 * 100 = 100
    assert!(log.record(100, 100)); // |100 - 100| / 100 * 100 = 0
    let report = log.report();
    assert_eq!(report.n, 2);
    assert_eq!(report.mean_abs_err_pct, 50.0);
    assert_eq!(report.p95_abs_err_pct, 100.0);
    assert_eq!(report.max_abs_err_pct, 100.0);
}

/// Purpose: the ring is bounded at 512 samples.
/// Scenario: record `(i, 100)` for `i` in `1..=600`; error is `|i - 100|`.
/// Acceptance: `n == 512` and `max == 500` (from `i == 600`), proving the newest
/// samples are kept and the oldest dropped — a ring that kept the first 512 would
/// cap the max at 412 (from `i == 512`).
#[test]
fn ring_keeps_at_most_512_samples() {
    let log = CalibrationLog::new();
    for i in 1..=600 {
        log.record(i, 100);
    }
    let report = log.report();
    assert_eq!(report.n, 512);
    assert_eq!(report.max_abs_err_pct, 500.0);
}

/// Purpose: `reported == 0` means the upstream did not report usage and is ignored.
/// Scenario: record pairs with `reported == 0` plus one valid pair.
/// Acceptance: `record(_, 0)` returns `false`; only the valid pair is in the report.
#[test]
fn reported_zero_is_ignored() {
    let log = CalibrationLog::new();
    assert!(!log.record(100, 0));
    assert!(!log.record(0, 0));
    assert!(log.record(200, 100));
    let report = log.report();
    assert_eq!(report.n, 1);
    assert_eq!(report.mean_abs_err_pct, 100.0);
    assert_eq!(report.max_abs_err_pct, 100.0);
}

/// Purpose: an empty log reports zeros and serializes as an empty report.
#[test]
fn empty_report_is_zero() {
    let log = CalibrationLog::new();
    assert_eq!(log.report(), CalibrationReport::default());
}
