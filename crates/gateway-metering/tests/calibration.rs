//! Calibration log — estimate-vs-reported prompt samples.
//!
//! Purpose: the ring-bounded calibration log that feeds issue #20 (weight-table
//! calibration): deployment accumulates samples on real traffic, then the weight
//! table is adjusted from the report's fitted suggestion.
//!
//! Coverage:
//! 1. `report()` computes exact mean / p95 / max absolute percent error.
//! 2. The ring is bounded at 512 samples: 600 records keep 512, oldest dropped.
//! 3. A reported prompt of 0 is ignored (no usage was reported; avoids division by
//!    zero and carries no calibration signal).
//! 4. The weight fit: exact recovery on observed classes, the ridge prior keeps
//!    unobserved classes at the current table, raw-fallback samples are excluded,
//!    image budgets are subtracted from the fit target, and no suggestion is
//!    reported without usable samples.

use gateway_metering::calibration::{CalibrationLog, CalibrationReport};
use gateway_metering::estimate::{PromptEstimate, TokenClassCounts};

/// Builds a non-fallback estimate from a billed total and per-class counts.
fn est(tokens: u64, classes: TokenClassCounts) -> PromptEstimate {
    PromptEstimate {
        tokens,
        classes,
        ..PromptEstimate::default()
    }
}

/// Builds a raw-fallback estimate (priced by the chars/4 path, no decomposition).
fn fallback(tokens: u64) -> PromptEstimate {
    PromptEstimate {
        tokens,
        raw_fallback: true,
        ..PromptEstimate::default()
    }
}

/// Purpose: `report()` computes exact mean / p95 / max over a small sample set.
/// Scenario: two pairs with absolute percent errors 100% and 0%.
/// Acceptance: `n == 2`, `mean == 50`, `p95 == 100`, `max == 100`.
#[test]
fn report_computes_mean_p95_and_max() {
    let log = CalibrationLog::new();
    assert!(log.record(est(200, TokenClassCounts::default()), 100)); // |200 - 100| / 100 * 100 = 100
    assert!(log.record(est(100, TokenClassCounts::default()), 100)); // |100 - 100| / 100 * 100 = 0
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
        log.record(est(i, TokenClassCounts::default()), 100);
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
    assert!(!log.record(est(100, TokenClassCounts::default()), 0));
    assert!(!log.record(est(0, TokenClassCounts::default()), 0));
    assert!(log.record(est(200, TokenClassCounts::default()), 100));
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

/// Purpose: the fit recovers the generating weights when the observed classes vary
/// independently — the core promise of carrying per-class counts.
/// Scenario: 10 usable samples over two classes (latin, cjk), deliberately
/// non-collinear, with `reported = 0.3·latin + 0.7·cjk` exactly (both sides
/// integral: counts are multiples of 10). digit/other are never observed.
/// Acceptance: latin ≈ 0.3 and cjk ≈ 0.7 (tight tolerance — the system is exactly
/// consistent, λ=1 is negligible against count-scale normal equations); digit/other
/// stay at the current table (0.3 / 1.0, the ridge prior); `samples_used == 10`;
/// the fitted error is ~0.
#[test]
fn fit_recovers_true_weights_on_observed_classes() {
    let log = CalibrationLog::new();
    let pairs = [
        (400u64, 100u64),
        (600, 200),
        (800, 150),
        (1000, 300),
        (1200, 250),
        (1400, 400),
        (1600, 350),
        (1800, 500),
        (2000, 450),
        (2200, 600),
    ];
    for (latin, cjk) in pairs {
        // 0.3·latin and 0.7·cjk are exact integers for multiples of 10.
        let reported = 3 * latin / 10 + 7 * cjk / 10;
        assert!(log.record(
            est(
                reported,
                TokenClassCounts {
                    latin,
                    cjk,
                    ..Default::default()
                }
            ),
            reported
        ));
    }
    let report = log.report();
    let fit = report.suggested_weights.expect("usable samples exist");
    assert_eq!(fit.samples_used, 10);
    assert!((fit.latin - 0.3).abs() < 1e-4, "latin {}", fit.latin);
    assert!((fit.cjk - 0.7).abs() < 1e-4, "cjk {}", fit.cjk);
    assert!((fit.digit - 0.3).abs() < 1e-9, "digit {}", fit.digit);
    assert!((fit.other - 1.0).abs() < 1e-9, "other {}", fit.other);
    // The system is exactly consistent, so the residual is purely the λ=1 ridge
    // perturbation against count-scale normal equations (~1e-5 percent here) —
    // anything below 1e-3 percent is a perfect fit at the ±10 percent target.
    assert!(
        fit.fit_mean_abs_err_pct < 1e-3,
        "fit err {}",
        fit.fit_mean_abs_err_pct
    );
}

/// Purpose: classes with no observed signal keep the current calibration (the ridge
/// prior anchors at the current table), not 0 — otherwise a single-class traffic mix
/// would collapse the unobserved weights toward zero.
/// Scenario: 5 usable samples carrying only latin counts (multiples of 4 so
/// `reported = 0.25·latin` is integral).
/// Acceptance: latin recovers 0.25; cjk / digit / other equal the current table
/// values 0.6 / 0.3 / 1.0 exactly.
#[test]
fn fit_keeps_unobserved_classes_at_current_table() {
    let log = CalibrationLog::new();
    for latin in [400u64, 800, 1200, 1600, 2000] {
        let reported = latin / 4; // 0.25 tok/char, exact
        assert!(log.record(
            est(
                reported,
                TokenClassCounts {
                    latin,
                    ..Default::default()
                }
            ),
            reported
        ));
    }
    let report = log.report();
    let fit = report.suggested_weights.expect("usable samples exist");
    assert_eq!(fit.samples_used, 5);
    assert!((fit.latin - 0.25).abs() < 1e-4, "latin {}", fit.latin);
    assert!((fit.cjk - 0.6).abs() < 1e-9, "cjk {}", fit.cjk);
    assert!((fit.digit - 0.3).abs() < 1e-9, "digit {}", fit.digit);
    assert!((fit.other - 1.0).abs() < 1e-9, "other {}", fit.other);
}

/// Purpose: raw-fallback samples do not enter the fit (the chars/4 path does not
/// follow the class model) and image budgets are subtracted from the fit target
/// (they are fixed provider constants, not weight-driven), so a multimodal sample
/// does not bias the text weights upward.
/// Scenario: three usable latin-only samples whose reported prompt includes an 85
/// image budget exactly, plus one raw-fallback sample.
/// Acceptance: `samples_used == 3`; latin recovers 0.25 despite the image budget;
/// the fitted error is ~0.
#[test]
fn fit_excludes_fallback_and_subtracts_image_budget() {
    let log = CalibrationLog::new();
    for latin in [400u64, 800, 1200] {
        let text_tokens = latin / 4; // 0.25 tok/char, exact
        let sample = PromptEstimate {
            tokens: text_tokens + 85,
            classes: TokenClassCounts {
                latin,
                ..Default::default()
            },
            image_tokens: 85,
            raw_fallback: false,
        };
        assert!(log.record(sample, text_tokens + 85));
    }
    assert!(log.record(fallback(3), 100)); // chars/4 path: excluded from the fit
    let report = log.report();
    let fit = report.suggested_weights.expect("usable samples exist");
    assert_eq!(fit.samples_used, 3);
    assert!((fit.latin - 0.25).abs() < 1e-4, "latin {}", fit.latin);
    // Same λ-perturbation scale as the exact-consistency recovery test.
    assert!(
        fit.fit_mean_abs_err_pct < 1e-3,
        "fit err {}",
        fit.fit_mean_abs_err_pct
    );
}

/// Purpose: a ring holding only raw-fallback samples yields no suggestion (nothing
/// follows the class model) — the /readyz surface must not present a fit that has
/// no basis.
/// Scenario: only a raw-fallback sample recorded.
/// Acceptance: `suggested_weights` is `None` while the error statistics still report.
#[test]
fn report_without_usable_samples_has_no_suggestion() {
    let log = CalibrationLog::new();
    assert!(log.record(fallback(3), 100));
    let report = log.report();
    assert_eq!(report.n, 1);
    assert!(report.suggested_weights.is_none());
}
