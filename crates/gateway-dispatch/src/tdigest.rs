//! t-digest: streaming quantile estimation (Dunning & Ertl, *Computing Extremely
//! Accurate Quantiles Using t-Digests*).
//!
//! Purpose: data keeps arriving and only p50/p95/p99 are needed without retaining the
//! full sample — e.g. silverq scores proxy nodes by latency quantiles, keeping one
//! digest resident per node.
//!
//! ## Why not keep the full array
//!
//! For streaming quantiles over n f64 values, keeping everything costs O(n) memory
//! (never reclaimed) + O(n log n) sorting per query; t-digest keeps only a small number
//! of centroids (mean + weight), O(δ·log n) memory and amortized O(δ) per `add`,
//! independent of sample count — the only feasible choice for "one digest per node,
//! resident for months".
//!
//! ## Scale variant and delta
//!
//! The paper's compression invariant: centroids sorted by mean ascending, measured by the
//! k-size function, each centroid's span `k_i − k_{i−1} ≤ f(1, n)`; the k1 variant takes
//! `k(q) = δ·asin(2q−1)/(2π)`, tightening the bound on **quantiles** rather than the value
//! domain. This implementation uses an equivalent simplified form — **q(1−q) quadratic
//! scaling**: a centroid whose mean midpoint falls at quantile q has weight limit
//! `w ≤ 4·n·q·(1−q)/δ`, which like k1 is "tight at both tails, loose in the middle" (the cap
//! is largest at q=0.5, n/δ), drops the asin, and has a monotonic bound that is easier to
//! check. δ (delta) is the compression parameter: smaller means more centroids and higher
//! accuracy, typically 20~300.
//!
//! ## Accuracy characteristics
//!
//! - Middle quantiles are very accurate, and the **tails (p99.9+) are actually more
//!   accurate than a uniform histogram** — the core advantage of t-digest over HDR
//!   histogram; the extreme tail still has boundary error, bounded by the width of the
//!   first/last centroid.
//! - Centroids store only mean/weight, **no internal distribution information**.
//!   `quantile` interpolates: when q falls inside centroid i, linearly interpolate between
//!   the means of adjacent centroids by cumulative weight. This is t-digest's accuracy
//!   boundary: the error is on the order of the weight gap between adjacent centroids and
//!   cannot be overcome — so q=0/q=1 return the first/last centroid mean, an approximation
//!   of min/max rather than an exact extremum.
//! - [`Tdigest::merge`] loses accuracy: both sides are already compressed and their
//!   internal information is gone; the merged result is worse than a single digest built
//!   streaming over the same data.
//!
//! ## Trade-offs vs fixed histograms / HDR
//!
//! t-digest needs no known value range and zero tuning; the cost is that rank queries
//! such as "what fraction is below x" are not supported, and merging (recompression after
//! merge) is more expensive than HDR's direct addition.
//!
//! Optimizations:
//! - Incremental compression: every δ accumulated `add` calls run one linear full-scan
//!   greedy merge, instead of compressing on every add (most adds do not change the
//!   structure, so the O(δ) scan would be wasted), and instead of accumulating a large
//!   batch before compressing (optimal amortized, but the centroid count grows without
//!   bound with the batch, making memory peaks and compression latency spikes
//!   uncontrollable). This scheme has a hard upper bound of roughly compressed size + δ
//!   centroids.
//! - `add` first binary-searches for an exactly equal mean: a hit only adds weight O(log δ)
//!   and leaves the mean bit-for-bit unchanged — the median of all-equal data is exact;
//!   otherwise it inserts into the sorted array, memmove O(δ).
//!
//! ---
//!
//! ## vendor provenance
//!
//! Vendored from the `algorchemy` repo's `src/sketch/tdigest.rs` (upstream commit
//! `2caccf43ab30f683b6985f991f99c59898e11367`, synced 2026-10-11).
//! Apart from this vendor note, the code is **byte-identical** to upstream — any change
//! must first pass the `tests/tdigest.rs` cross-check (run the same test when syncing an
//! upstream upgrade).

/// Centroid: an approximate representation of a cluster of points. `mean` is the weighted
/// mean, `weight` the total weight.
#[derive(Clone, Copy, Debug)]
pub struct Centroid {
    pub mean: f64,
    pub weight: f64,
}

/// Streaming quantile estimator (accuracy characteristics and trade-offs: [module docs](self)).
pub struct Tdigest {
    centroids: Vec<Centroid>, // ascending by mean
    total: f64,               // sum of weights
    delta: f64,               // compression parameter
    dirty: usize,             // number of adds since the last compression
}

impl Tdigest {
    /// Create a new digest. `delta ≥ 1` (typically 20~300); smaller is more accurate with
    /// more centroids.
    pub fn new(delta: f64) -> Self {
        assert!(
            delta.is_finite() && delta >= 1.0,
            "Tdigest requires δ ≥ 1, got {delta}"
        );
        Self {
            centroids: Vec::new(),
            total: 0.0,
            delta,
            dirty: 0,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.centroids.is_empty()
    }

    /// Total weight (equals the sample count when all weights are 1).
    pub fn count(&self) -> f64 {
        self.total
    }

    /// Add a point (weight 1).
    pub fn add(&mut self, value: f64) {
        self.add_weighted(value, 1.0);
    }

    /// Add a weighted point. `weight == 0` is a no-op; negative/NaN weight or a non-finite
    /// `value` panics.
    pub fn add_weighted(&mut self, value: f64, weight: f64) {
        assert!(
            value.is_finite(),
            "Tdigest value must be finite, got {value}"
        );
        assert!(
            weight.is_finite() && weight >= 0.0,
            "Tdigest weight must be finite and non-negative, got {weight}"
        );
        if weight == 0.0 {
            return;
        }
        match self
            .centroids
            .binary_search_by(|c| c.mean.total_cmp(&value))
        {
            // Exact mean hit: add weight only, leave the mean untouched.
            Ok(i) => self.centroids[i].weight += weight,
            Err(i) => self.centroids.insert(
                i,
                Centroid {
                    mean: value,
                    weight,
                },
            ),
        }
        self.total += weight;
        self.dirty += 1;
        if self.dirty >= self.delta as usize {
            self.compress();
            self.dirty = 0;
        }
    }

    /// Quantile estimate, `q ∈ [0, 1]`; an empty digest returns `None`, out-of-range q
    /// panics.
    ///
    /// Centroid i's "center quantile" is `(cumulative weight before it + w_i/2) / n`; when q
    /// falls between adjacent center quantiles, interpolate linearly between their means (a
    /// centroid has no internal distribution information — this is t-digest's accuracy
    /// boundary). q=0/q=1 fall outside the first/last center quantile and return that
    /// endpoint's mean.
    pub fn quantile(&self, q: f64) -> Option<f64> {
        assert!(
            q.is_finite() && (0.0..=1.0).contains(&q),
            "quantile requires q in [0,1], got {q}"
        );
        let cs = &self.centroids;
        if cs.is_empty() {
            return None;
        }
        if cs.len() == 1 {
            return Some(cs[0].mean);
        }
        let g = q * self.total; // target cumulative weight
        if g <= cs[0].weight / 2.0 {
            return Some(cs[0].mean);
        }
        let mut cum = cs[0].weight; // cumulative weight before cs[i]
        for i in 0..cs.len() - 1 {
            let t_hi = cum + cs[i + 1].weight / 2.0; // center-quantile position of cs[i+1]
            if g <= t_hi {
                let t_lo = cum - cs[i].weight / 2.0; // center-quantile position of cs[i]
                let frac = (g - t_lo) / (t_hi - t_lo);
                // The frac=0/1 endpoints must equal the two means exactly: m_lo + 1.0*(m_hi -
                // m_lo) can overshoot m_hi by one ulp in floating point (counterexample pinned
                // by the monotonicity test in tests/tdigest.rs), so clamp back into the interval.
                let v = cs[i].mean + frac * (cs[i + 1].mean - cs[i].mean);
                return Some(v.clamp(cs[i].mean, cs[i + 1].mean));
            }
            cum += cs[i + 1].weight;
        }
        Some(cs[cs.len() - 1].mean)
    }

    /// Same as `quantile(0.5)`.
    pub fn median(&self) -> Option<f64> {
        self.quantile(0.5)
    }

    /// Merge another digest (recompress after merging). Both sides are already compressed, so
    /// accuracy is lost; see module docs.
    pub fn merge(&mut self, other: &Self) {
        self.centroids.extend(other.centroids.iter().copied());
        self.total += other.total;
        self.centroids
            .sort_unstable_by(|a, b| a.mean.total_cmp(&b.mean));
        self.compress();
        self.dirty = 0;
    }

    /// Linear full-scan greedy merge: left to right, merge adjacent centroids into the
    /// current open centroid while staying within the `4·n·q·(1−q)/δ` weight limit
    /// (simplified version of the paper's Compress). Merging preserves ascending mean; the
    /// limit is a soft constraint — it can be exceeded temporarily within an `add` batch
    /// window (≤ δ adds).
    fn compress(&mut self) {
        let n = self.total;
        if n <= 0.0 {
            self.centroids.clear();
            return;
        }
        let delta = self.delta;
        let old = std::mem::take(&mut self.centroids); // already sorted
        let mut out: Vec<Centroid> = Vec::with_capacity(old.len());
        let mut cum = 0.0f64; // sum of finalized weights (excluding the current open group)
        for c in old {
            // If the current open group can merge with c (within the weight limit), merge in
            // place and skip finalization.
            if let Some(last) = out.last_mut() {
                let w = last.weight + c.weight;
                let q = (cum + w / 2.0) / n; // midpoint quantile of the merged group
                if w * delta <= 4.0 * n * q * (1.0 - q) {
                    last.mean = (last.mean * last.weight + c.mean * c.weight) / w;
                    last.weight = w;
                    continue;
                }
            }
            if let Some(last) = out.last() {
                cum += last.weight; // finalize the previous group
            }
            out.push(c);
        }
        self.centroids = out;
    }
}
