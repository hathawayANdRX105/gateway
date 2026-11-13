//! Calibration hook integration — estimate-vs-reported pairs land in the log on the
//! settle path.
//!
//! Purpose: prove that a non-streaming settle with upstream usage records an
//! (estimate, reported) prompt pair into the calibration log.
//! Scenario: mock egress returns 200 with `{"usage":{"prompt_tokens":100,...}}`;
//! request body is `{"model":"m"}` (estimate 3 via raw-char fallback: 13 chars / 4).
//! No price table or sink is mounted, so the only observable effect of settlement is
//! the calibration side-record.
//! Acceptance: the global calibration log holds exactly one sample; mean / p95 / max
//! absolute percent error are all 97.0 (`|3 - 100| / 100 * 100`).

mod common;

use gateway_forward::ForwardStage;
use gateway_metering::calibration::{CalibrationLog, global, install};
use gateway_pipeline::{Stage, StageOutcome};
use gateway_protocol_bridge::format_codec::FormatRegistry;
use std::sync::Arc;

use common::*;

/// Purpose: a non-streaming settle with upstream usage records a calibration pair.
/// Scenario: mock egress returns 200 with `{"usage":{"prompt_tokens":100,...}}`;
/// request body is `{"model":"m"}` (estimate 3).
/// Acceptance: the calibration log has `n == 1` and all error statistics are 97.0.
#[tokio::test]
async fn settle_non_stream_records_calibration_pair() {
    // Fresh log for this test binary's global handle.
    assert!(install(Arc::new(CalibrationLog::new())));
    let egress = Arc::new(FixedEgress {
        body: b"{\"choices\":[{\"finish_reason\":\"stop\"}],\"usage\":{\"prompt_tokens\":100,\"completion_tokens\":50}}",
    });
    let adaptors = Arc::new(FormatRegistry::new());
    let stage = ForwardStage::new(egress, adaptors);

    let mut ctx = ctx_with_body(NON_STREAM_BODY, candidate("c1"));
    let outcome = stage.handle(&mut ctx).await.expect("forward must succeed");
    assert!(matches!(outcome, StageOutcome::Continue));

    let report = global().expect("calibration log installed").report();
    assert_eq!(report.n, 1);
    assert_eq!(report.mean_abs_err_pct, 97.0);
    assert_eq!(report.p95_abs_err_pct, 97.0);
    assert_eq!(report.max_abs_err_pct, 97.0);
}
