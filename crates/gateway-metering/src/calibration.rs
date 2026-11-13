//! Calibration log — estimate-vs-reported prompt token pairs.
//!
//! Records `(estimated_prompt, reported_prompt)` pairs observed at the forward
//! settlement commit point whenever the upstream reports prompt usage, and
//! exposes calibration statistics (mean / p95 / max absolute percent error).
//!
//! This is the data source for issue #20 (weight-table calibration) and the
//! issue #9 estimation-error anchor: deployment accumulates samples on real
//! traffic, then weights are adjusted from the report.
//!
//! The log is a bounded ring of 512 samples: once full, the oldest sample is
//! dropped, so memory stays bounded regardless of traffic volume. Every method
//! takes `&self` through an internal `parking_lot::Mutex`, so the log is
//! thread-safe with no poisoning path (no tokio).

use parking_lot::Mutex;
use std::sync::{Arc, OnceLock};
use std::time::{SystemTime, UNIX_EPOCH};

/// Capacity of the ring buffer: once full, the oldest sample is dropped on
/// each new write.
const CALIBRATION_CAPACITY: usize = 512;

/// One observed (estimate, reported) prompt-token pair.
#[derive(Debug, Clone, Copy, Default)]
pub struct CalibrationSample {
    /// Prompt tokens estimated by the gateway before forwarding
    /// (`gateway_metering::estimate::estimate_prompt_tokens`).
    pub estimated_prompt: u64,
    /// Prompt tokens reported by the upstream in its usage response.
    pub reported_prompt: u64,
    /// Epoch milliseconds when the sample was recorded.
    pub recorded_ms: u64,
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
}

/// Bounded ring log of estimate-vs-reported prompt token pairs.
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
/// (see `src/server.rs`). The forward data plane records through this handle
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

    /// Records an `(estimated, reported)` prompt-token pair.
    ///
    /// A reported prompt of `0` means the upstream did not report usage
    /// (absence, not a genuine zero-count report), so such samples are
    /// ignored: they cannot calibrate (the percent error divides by
    /// `reported_prompt`) and carry no information. Returns `true` when the
    /// sample was recorded.
    pub fn record(&self, estimated_prompt: u64, reported_prompt: u64) -> bool {
        if reported_prompt == 0 {
            return false;
        }
        let sample = CalibrationSample {
            estimated_prompt,
            reported_prompt,
            recorded_ms: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map_or(0, |d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX)),
        };
        self.inner.lock().push(sample);
        true
    }

    /// Returns aggregated calibration statistics over the samples currently in
    /// the ring. `n == 0` (and all-zero stats) when nothing has been recorded.
    #[must_use]
    pub fn report(&self) -> CalibrationReport {
        let inner = self.inner.lock();
        if inner.len == 0 {
            return CalibrationReport::default();
        }
        let cap = CALIBRATION_CAPACITY;
        #[allow(clippy::cast_precision_loss)] // token counts stay far below 2^53
        let mut errs: Vec<f64> = (0..inner.len)
            .map(|i| {
                let idx = if inner.len < cap {
                    i
                } else {
                    (inner.head + i) % cap
                };
                let s = &inner.buf[idx];
                ((s.estimated_prompt as f64) - (s.reported_prompt as f64)).abs()
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
        CalibrationReport {
            n: u64::try_from(n).unwrap_or(u64::MAX),
            mean_abs_err_pct: mean,
            p95_abs_err_pct: p95,
            max_abs_err_pct: max,
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
}
