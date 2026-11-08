//! Purely in-memory sliding-window rate limiting — request-rate protection per unit (RouteUnit).
//!
//! Reference implementation:
//! - wildtoken `internal/ratelimit/{limiter.go,parser.go}`:
//!   keeps per-key per-second bucket counts (`BTreeMap<unix_sec, count>`); the window start is
//!   rounded down to whole seconds, and a bucket count >= the limit rejects; the fallback always
//!   errs on the strict side (reject one too many rather than let through a request that should
//!   be rejected, matching the comment in limiter.go).
//!
//! Design tradeoffs:
//! - No background cleanup thread; the admits path prunes buckets older than `now - 2h` along
//!   the way (avoiding residual memory for long-tail keys and keeping any timer failure out of
//!   the hot path).
//! - Clock handling matches health.rs: `chrono::Utc::now()` by default; `with_clock` injects
//!   deterministic milliseconds in tests.
//! - An empty spec / `None` means unrestricted; omitting it in config costs nothing (the select
//!   path checks whether limits are empty before running rate-limit checks).

use std::collections::{BTreeMap, HashMap};
use std::sync::Mutex;

/// Parsed rate-limit spec — `requests` requests per `window_ms` milliseconds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RateLimitSpec {
    pub requests: u64,
    pub window_ms: u64,
}

impl RateLimitSpec {
    /// Syntax `^(\d+)([kM])?/(\d+)?([smhd])$`:
    ///   - `100/s`  → 100 req / 1s
    ///   - `200/5m` → 200 req / 5 min
    ///   - `10000/d`→ 10000 req / 24h
    ///   - `200k/1m`→ 200000 tokens / 60s (k = 1e3, M = 1e6)
    ///
    /// Empty and malformed strings both return `None` (ocr #3): the caller cannot distinguish "not
    /// rate-limited" from "config typo". ponytail: parse does not distinguish either; the config
    /// layer rejects bad values at write time (same as wildtoken's write-time NormalizeRateLimit
    /// validation); the hot path only cares whether a limit exists. `requests == 0` or `mult == 0`
    /// are rejected too; the magnitude suffix only scales the request count, so `0k` / `0M` still
    /// yield `None`.
    pub fn parse(s: &str) -> Option<Self> {
        if s.is_empty() {
            return None;
        }
        // ponytail: hand-written scan instead of a regex — the pattern is simple, and it avoids
        // dragging in a regex dependency.
        let bytes = s.as_bytes();
        // Request count: \d+
        let req_end = digits_end(bytes, 0);
        if req_end == 0 {
            return None;
        }
        let requests: u64 = s[..req_end].parse().ok()?;
        let mut i = req_end;
        // Optional magnitude suffix k/M: 200k = 200000, 1M = 1000000 (k = 1e3, M = 1e6).
        let suffix_factor: u64 = match bytes.get(i).copied() {
            Some(b'k') => 1_000,
            Some(b'M') => 1_000_000,
            _ => 1,
        };
        if suffix_factor != 1 {
            i += 1;
        }
        let requests = requests.checked_mul(suffix_factor)?;
        if requests == 0 {
            return None;
        }
        // '/'
        if i >= bytes.len() || bytes[i] != b'/' {
            return None;
        }
        i += 1;
        // Optional multiplier: \d*
        let mult_end = digits_end(bytes, i);
        let mult: u64 = if mult_end > i {
            s[i..mult_end].parse().ok()?
        } else {
            1
        };
        if mult == 0 {
            return None;
        }
        i = mult_end;
        // Unit: a single [smhd] character
        if i >= bytes.len() {
            return None;
        }
        let unit = bytes[i];
        if i + 1 != bytes.len() {
            return None; // must end exactly at the unit
        }
        let base_ms: u64 = match unit {
            b's' => 1_000,
            b'm' => 60_000,
            b'h' => 3_600_000,
            b'd' => 86_400_000,
            _ => return None,
        };
        Some(RateLimitSpec {
            requests,
            window_ms: base_ms.checked_mul(mult)?,
        })
    }
}

/// Scan consecutive ASCII digits from `start` and return the end index of the digit run
/// (`== start` when there are no digits).
fn digits_end(bytes: &[u8], start: usize) -> usize {
    let mut end = start;
    while end < bytes.len() && bytes[end].is_ascii_digit() {
        end += 1;
    }
    end
}

/// Per-unit buckets: unix second → accumulated count within that second. `BTreeMap` makes it easy
/// to trim old buckets from a start point (`split_off`/`range`) and keeps iteration time-ordered
/// for summation.
type Buckets = BTreeMap<i64, u64>;

type LimitMap = HashMap<String, Buckets>;

/// Purely in-memory sliding-window limiter — per Pod, no cross-node synchronization.
///
/// `admits` takes an explicit `now_ms` so tests can inject it; production can call
/// `SlidingWindow::new()` (system clock by default) or `with_clock` for a deterministic clock.
pub struct SlidingWindow {
    limits: Mutex<LimitMap>,
    now_ms: Box<dyn Fn() -> u64 + Send + Sync>,
}

impl Default for SlidingWindow {
    fn default() -> Self {
        Self::new()
    }
}

impl SlidingWindow {
    pub fn new() -> Self {
        Self::with_clock(crate::utc_now_ms)
    }

    /// Clock injection (deterministic tests).
    pub fn with_clock(now_ms: impl Fn() -> u64 + Send + Sync + 'static) -> Self {
        Self {
            limits: Mutex::new(LimitMap::new()),
            now_ms: Box::new(now_ms),
        }
    }

    /// Take the lock without panicking on poisoning (ocr #2: one thread panicking on the hot path
    /// must not take down every request).
    fn lock(&self) -> std::sync::MutexGuard<'_, LimitMap> {
        self.limits.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// Decide + account (weighted): admit within the limit and add `weight`; over the limit
    /// returns false without accounting.
    ///
    /// A `None` spec always admits (short-circuited in the config layer, zero cost). `weight == 0`
    /// is a cost-free request — admitted and never accounted. Strict-side fallback: reject when
    /// `current + weight > requests` (reject one too many rather than let through a request that
    /// should be rejected); for `weight == 1` this is byte-for-byte identical to [`Self::admits`].
    pub fn admits_weighted(
        &self,
        key: &str,
        spec: Option<&RateLimitSpec>,
        weight: u64,
        now_ms: u64,
    ) -> bool {
        let spec = match spec {
            Some(s) if s.requests > 0 && s.window_ms > 0 => s,
            _ => return true,
        };
        if weight == 0 {
            return true;
        }

        let now_s = (now_ms / 1000) as i64;
        let window_s = (spec.window_ms / 1000).max(1) as i64;
        // Window start: includes the current second and covers window_s full second-buckets
        // backwards. start = now_s - window_s + 1; strict-side rule: admit when the buckets in that
        // range plus weight are <= requests.
        let start_s = now_s - window_s + 1;

        let mut limits = self.lock();
        let buckets = limits.entry(key.to_string()).or_default();

        // Prune buckets older than now_s - 7200 (2h) along the way — prevents memory growth for
        // long-tail keys. ponytail: every admits call checks, but retain only allocates when an old
        // bucket exists; skipping the background cleanup thread trades this small hot-path cost for
        // no timer coupling at all.
        let cutoff = now_s - 7200;
        if buckets
            .keys()
            .next()
            .copied()
            .is_some_and(|first| first < cutoff)
        {
            buckets.retain(|bucket, _| *bucket >= cutoff);
        }

        let current_count: u64 = buckets.range(start_s..=now_s).map(|(_, c)| *c).sum();

        if current_count.saturating_add(weight) > spec.requests {
            return false;
        }

        *buckets.entry(now_s).or_insert(0) += weight;
        true
    }

    /// Decide + account: admit within the limit and +1; over the limit returns false without
    /// incrementing.
    ///
    /// A `None` spec always admits (short-circuited in the config layer, zero cost). Delegates to
    /// the weighted version with `weight = 1`.
    pub fn admits(&self, key: &str, spec: Option<&RateLimitSpec>, now_ms: u64) -> bool {
        self.admits_weighted(key, spec, 1, now_ms)
    }

    /// Test helper: read now_ms from the internal clock, then call admits. Production normally
    /// passes `chrono::Utc::now()` directly to avoid a closure call; the injected clock is mainly
    /// for tests.
    pub fn admits_now(&self, key: &str, spec: Option<&RateLimitSpec>) -> bool {
        self.admits(key, spec, (self.now_ms)())
    }

    /// Number of second-buckets currently retained for a key (observation/tests; production does
    /// not need it).
    #[doc(hidden)]
    pub fn bucket_count(&self, key: &str) -> usize {
        self.lock().get(key).map_or(0, |b| b.len())
    }
}
