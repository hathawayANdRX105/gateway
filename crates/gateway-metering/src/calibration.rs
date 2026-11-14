//! Calibration log — estimate-vs-reported prompt token samples.
//!
//! Records the prompt estimate (with its full [`PromptEstimate`] decomposition)
//! against the upstream-reported prompt usage at the settlement commit point, and
//! exposes calibration statistics (mean / p95 / max absolute percent error) plus a
//! weight-table suggestion fitted over the ring.
//!
//! This is the data source for issue #20 (weight-table calibration): deployment
//! accumulates samples on real traffic, then the weight table is adjusted from the
//! report's suggested weights instead of only knowing aggregate totals — per-class
//! counts make the per-class deviations separable.
//!
//! The log is a bounded ring of 512 samples: once full, the oldest sample is
//! dropped, so memory stays bounded regardless of traffic volume. Every method
//! takes `&self` through an internal `parking_lot::Mutex`, so the log is
//! thread-safe with no poisoning path (no tokio).

use crate::estimate::{PromptEstimate, WEIGHT_CJK, WEIGHT_DIGIT, WEIGHT_LATIN, WEIGHT_OTHER};
use parking_lot::Mutex;
use std::sync::{Arc, OnceLock};
use std::time::{SystemTime, UNIX_EPOCH};

/// Capacity of the ring buffer: once full, the oldest sample is dropped on
/// each new write.
const CALIBRATION_CAPACITY: usize = 512;

/// Ridge prior weight of the weight fit: small enough to be negligible against
/// real character counts (hundreds to thousands per sample), large enough to pin
/// an unobserved class to its prior (the current weight table) instead of letting
/// the least-squares solve drift it toward 0.
const FIT_PRIOR_WEIGHT: f64 = 1.0;

/// One observed estimate-vs-reported prompt-token sample.
#[derive(Debug, Clone, Copy, Default)]
pub struct CalibrationSample {
    /// The gateway's prompt estimate with its decomposition, taken before
    /// forwarding (`gateway_metering::estimate::estimate_prompt_tokens`).
    pub estimate: PromptEstimate,
    /// Prompt tokens reported by the upstream in its usage response.
    pub reported_prompt: u64,
    /// Epoch milliseconds when the sample was recorded.
    pub recorded_ms: u64,
}

/// Weight-table suggestion fitted over the ring's usable samples (issue #20).
///
/// Prior-centered ridge least squares: minimize `Σ (w·n - y)² + λ Σ (w_c - prior_c)²`
/// with the prior anchored at the current weight table and a small λ. Classes the
/// samples observe move toward their least-squares value; classes with no observed
/// signal stay at the current calibration instead of collapsing toward 0. `y` is the
/// reported prompt minus the image budget (image tokens are fixed provider constants,
/// outside the weight model); raw-fallback samples are excluded because the chars/4
/// path does not follow the class model.
#[derive(Debug, Clone, Copy, Default, PartialEq, serde::Serialize)]
pub struct SuggestedWeights {
    /// Suggested tok/char weight for Latin characters.
    pub latin: f64,
    /// Suggested tok/char weight for CJK ideographs.
    pub cjk: f64,
    /// Suggested tok/char weight for ASCII digits.
    pub digit: f64,
    /// Suggested tok/char weight for other characters.
    pub other: f64,
    /// Usable (non-fallback) samples the fit ran on — judge the suggestion's
    /// trustworthiness against this, not against the ring's full `n`.
    pub samples_used: u64,
    /// Mean absolute percent error of the fitted model over those samples —
    /// compare against [`CalibrationReport::mean_abs_err_pct`] (the current table
    /// on the same samples) to see whether refitting is worth it.
    pub fit_mean_abs_err_pct: f64,
}

/// Aggregated calibration statistics over the samples currently in the ring.
#[derive(Debug, Clone, Copy, Default, PartialEq, serde::Serialize)]
pub struct CalibrationReport {
    /// Number of samples in the ring.
    pub n: u64,
    /// Mean absolute percent error, `|est - rep| / rep * 100`.
    pub mean_abs_err_pct: f64,
    /// p95 absolute percent error (nearest-rank).
    pub p95_abs_err_pct: f64,
    /// Max absolute percent error.
    pub max_abs_err_pct: f64,
    /// Weight-table suggestion fitted over the ring's usable samples; `None` when
    /// no usable (non-fallback) sample is present.
    pub suggested_weights: Option<SuggestedWeights>,
}

/// Bounded ring log of estimate-vs-reported prompt token samples.
pub struct CalibrationLog {
    inner: Mutex<Ring>,
}

/// Bounded ring buffer: writes append until `CAPACITY`, then overwrite the
/// oldest slot, so the oldest live sample is always at `head` when full.
struct Ring {
    buf: Vec<CalibrationSample>,
    /// Next slot to write (fill phase) or the oldest slot (full phase).
    head: usize,
    /// Number of valid samples in the ring.
    len: usize,
}

/// Process-global calibration log handle, installed once at daemon startup
/// (the standalone daemon does it in its `src/server.rs`). The forward data plane records through this handle
/// so no field on `ForwardStage` and no facade-builder plumbing is required;
/// the readiness surface reads the same handle for the report.
static CALIBRATION: OnceLock<Arc<CalibrationLog>> = OnceLock::new();

/// Install the process-global calibration log. Returns `true` on first
/// installation, `false` if one is already installed.
pub fn install(log: Arc<CalibrationLog>) -> bool {
    CALIBRATION.set(log).is_ok()
}

/// Read the process-global calibration log, if one has been installed.
pub fn global() -> Option<Arc<CalibrationLog>> {
    CALIBRATION.get().cloned()
}

impl CalibrationLog {
    /// Creates an empty calibration log.
    #[must_use]
    pub fn new() -> Self {
        Self {
            inner: Mutex::new(Ring {
                buf: Vec::with_capacity(CALIBRATION_CAPACITY),
                head: 0,
                len: 0,
            }),
        }
    }

    /// Records an estimate-vs-reported prompt sample.
    ///
    /// A reported prompt of `0` means the upstream did not report usage
    /// (absence, not a genuine zero-count report), so such samples are
    /// ignored: they cannot calibrate (the percent error divides by
    /// `reported_prompt`) and carry no information. Returns `true` when the
    /// sample was recorded.
    pub fn record(&self, estimate: PromptEstimate, reported_prompt: u64) -> bool {
        if reported_prompt == 0 {
            return false;
        }
        let sample = CalibrationSample {
            estimate,
            reported_prompt,
            recorded_ms: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map_or(0, |d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX)),
        };
        self.inner.lock().push(sample);
        true
    }

    /// Returns aggregated calibration statistics over the samples currently in
    /// the ring, plus the fitted weight suggestion. `n == 0` (and all-zero stats,
    /// no suggestion) when nothing has been recorded.
    #[must_use]
    pub fn report(&self) -> CalibrationReport {
        let inner = self.inner.lock();
        if inner.len == 0 {
            return CalibrationReport::default();
        }
        #[allow(clippy::cast_precision_loss)] // token counts stay far below 2^53
        let mut errs: Vec<f64> = inner
            .iter()
            .map(|s| {
                ((s.estimate.tokens as f64) - (s.reported_prompt as f64)).abs()
                    / s.reported_prompt as f64
                    * 100.0
            })
            .collect();
        errs.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
        let n = inner.len;
        #[allow(clippy::cast_precision_loss)] // sample count far below 2^53
        let mean = errs.iter().sum::<f64>() / n as f64;
        let max = errs[n - 1];
        // Nearest-rank p95: rank = ceil(0.95 * n) (1-based) => index = rank - 1.
        let rank = (n * 95).div_ceil(100);
        let p95_idx = rank.saturating_sub(1).min(n - 1);
        let p95 = errs[p95_idx];
        let suggested_weights = suggest_weights(&inner);
        CalibrationReport {
            n: u64::try_from(n).unwrap_or(u64::MAX),
            mean_abs_err_pct: mean,
            p95_abs_err_pct: p95,
            max_abs_err_pct: max,
            suggested_weights,
        }
    }
}

impl Default for CalibrationLog {
    fn default() -> Self {
        Self::new()
    }
}

impl Ring {
    fn push(&mut self, sample: CalibrationSample) {
        if self.len < CALIBRATION_CAPACITY {
            self.buf.push(sample);
            self.len += 1;
        } else {
            self.buf[self.head] = sample;
            self.head = (self.head + 1) % CALIBRATION_CAPACITY;
        }
    }

    /// Iterates the valid samples oldest-first (fill order; once full, `head`
    /// is the oldest slot).
    fn iter(&self) -> impl Iterator<Item = &CalibrationSample> {
        let cap = CALIBRATION_CAPACITY;
        (0..self.len).map(move |i| {
            let idx = if self.len < cap {
                i
            } else {
                (self.head + i) % cap
            };
            &self.buf[idx]
        })
    }
}

/// Fits [`SuggestedWeights`] over the ring's usable (non-fallback) samples.
///
/// Builds the normal equations `(AᵀA + λI) w = Aᵀy + λ·prior` in one pass, solves
/// the 4×4 system, then evaluates the fitted model's mean percent error in a second
/// pass over the same samples.
#[allow(clippy::cast_precision_loss, clippy::cast_possible_truncation)] // counts and
// token counts stay far below 2^53, so the f64 casts are exact
fn suggest_weights(ring: &Ring) -> Option<SuggestedWeights> {
    let prior = [WEIGHT_LATIN, WEIGHT_CJK, WEIGHT_DIGIT, WEIGHT_OTHER];
    let mut ata = [[0.0f64; 4]; 4];
    let mut aty = [0.0f64; 4];
    let mut used = 0u64;
    for s in ring.iter() {
        if s.estimate.raw_fallback {
            continue;
        }
        used += 1;
        let n = [
            s.estimate.classes.latin as f64,
            s.estimate.classes.cjk as f64,
            s.estimate.classes.digit as f64,
            s.estimate.classes.other as f64,
        ];
        // Image budgets are fixed provider constants, outside the weight model:
        // fit against the reported prompt minus the image contribution.
        let y = s.reported_prompt.saturating_sub(s.estimate.image_tokens) as f64;
        for r in 0..4 {
            aty[r] += n[r] * y;
            for c in 0..4 {
                ata[r][c] += n[r] * n[c];
            }
        }
    }
    if used == 0 {
        return None;
    }
    // Prior-centered ridge: shrink toward the current table, not toward zero.
    for d in 0..4 {
        ata[d][d] += FIT_PRIOR_WEIGHT;
        aty[d] += FIT_PRIOR_WEIGHT * prior[d];
    }
    let w = solve4(ata, aty)?;
    // Fitted-model error over the same usable samples, image budgets included in
    // the prediction (they are part of the billed estimate).
    let err_sum: f64 = ring
        .iter()
        .filter(|s| !s.estimate.raw_fallback)
        .map(|s| {
            let fitted = w[0] * s.estimate.classes.latin as f64
                + w[1] * s.estimate.classes.cjk as f64
                + w[2] * s.estimate.classes.digit as f64
                + w[3] * s.estimate.classes.other as f64
                + s.estimate.image_tokens as f64;
            (fitted - s.reported_prompt as f64).abs() / s.reported_prompt as f64 * 100.0
        })
        .sum();
    Some(SuggestedWeights {
        latin: w[0],
        cjk: w[1],
        digit: w[2],
        other: w[3],
        samples_used: used,
        fit_mean_abs_err_pct: err_sum / used as f64,
    })
}

/// Solves a 4×4 linear system by Gaussian elimination with partial pivoting.
/// Returns `None` on a numerically singular system — cannot happen for the ridge
/// normal equations (`AᵀA` is positive semidefinite, `+λI` makes it positive
/// definite), so this is purely defensive against NaN inputs.
fn solve4(mut a: [[f64; 4]; 4], mut b: [f64; 4]) -> Option<[f64; 4]> {
    for col in 0..4 {
        // Partial pivot: the largest magnitude in this column at or below the diagonal.
        let pivot = (col..4).max_by(|&r1, &r2| a[r1][col].abs().total_cmp(&a[r2][col].abs()))?;
        if a[pivot][col].abs() < f64::EPSILON {
            return None;
        }
        a.swap(col, pivot);
        b.swap(col, pivot);
        // Copy the pivot row so the mutable iteration over lower rows can zip
        // against it (two overlapping borrows through `a` would not split).
        let pivot_vals = a[col];
        for row in (col + 1)..4 {
            let factor = a[row][col] / a[col][col];
            for (cell, pv) in a[row][col..4].iter_mut().zip(pivot_vals[col..4].iter()) {
                *cell -= factor * *pv;
            }
            b[row] -= factor * b[col];
        }
    }
    let mut x = [0.0f64; 4];
    for row in (0..4).rev() {
        let mut acc = b[row];
        for c in (row + 1)..4 {
            acc -= a[row][c] * x[c];
        }
        x[row] = acc / a[row][row];
    }
    Some(x)
}
