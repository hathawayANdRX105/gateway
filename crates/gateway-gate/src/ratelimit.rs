//! `ratelimit` — gate 4: rate limiting (per-key / per-group RPM sliding window)
//!
//! Algorithm: one sliding window per `(scope, key)`, bucketed by second; a request is
//! rejected once the cumulative count inside the window reaches the limit.
//! ponytail: in-memory single-process version, not sharded; switch to centralized rate
//! limiting once multiple instances arrive.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use parking_lot::Mutex;

use super::TokenInfo;
use super::chain::{Gate, GateCtx};
use super::error::Rejection;

/// Rate-limit dimensions
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum LimitScope {
    PerKey,     // by token id
    PerChannel, // by channel id (for use post-dispatch)
    PerGroup,   // by user group
}

/// Rate limiter (sliding window by second buckets).
///
/// Maintains one [`SecondWindow`] per `(scope, key)` internally. The limit pair is
/// interior-mutable so an operator can retune it at runtime (see [`Self::set_limits`]);
/// windows in flight keep their history and the new pair applies from the next acquire.
pub struct RateLimiter {
    inner: Mutex<HashMap<(LimitScope, String), SecondWindow>>,
    /// Default limit: N requests per window_secs seconds. For simplicity all scopes currently
    /// share this pair of values.
    max_requests: AtomicU32,
    window_secs: AtomicU32,
}

impl RateLimiter {
    pub fn new(max_requests: u32, window_secs: u32) -> Self {
        Self {
            inner: Mutex::new(HashMap::new()),
            max_requests: AtomicU32::new(max_requests),
            window_secs: AtomicU32::new(window_secs),
        }
    }

    /// Limit pair currently in effect.
    pub fn limits(&self) -> (u32, u32) {
        (
            self.max_requests.load(Ordering::Relaxed),
            self.window_secs.load(Ordering::Relaxed),
        )
    }

    /// Replace the limit pair at runtime; existing per-key windows keep their history.
    pub fn set_limits(&self, max_requests: u32, window_secs: u32) {
        self.max_requests.store(max_requests, Ordering::Relaxed);
        self.window_secs.store(window_secs, Ordering::Relaxed);
    }

    /// Try to acquire one permit. true = allowed, false = over limit.
    pub fn try_acquire(&self, scope: LimitScope, key: &str) -> bool {
        let now = now_secs();
        let mut map = self.inner.lock();
        let bucket = map.entry((scope, key.to_string())).or_default();
        let (max_requests, window_secs) = self.limits();
        bucket.try_acquire(now, max_requests, window_secs)
    }
}

/// Sliding window bucketed by second for one (scope, key) (not a token bucket: it counts
/// requests only, with no token refill rate).
///
/// Algorithm: treat the "current second" as the newest bucket and look back over the last
/// `window_secs` seconds of buckets; reject once the cumulative count has reached
/// `max_requests`.
/// ponytail: HashMap<sec, count>; each acquire opportunistically purges expired buckets,
/// O(window_secs).
#[derive(Default)]
struct SecondWindow {
    /// second → request count in that second
    buckets: HashMap<u64, u32>,
}

impl SecondWindow {
    fn try_acquire(&mut self, now: u64, max: u32, window: u32) -> bool {
        // Drop buckets older than `now - window` (the cutoff second is kept).
        let cutoff = now.saturating_sub(window as u64);
        self.buckets.retain(|&sec, _| sec >= cutoff);

        let count: u32 = self.buckets.values().copied().sum();
        if count >= max {
            return false;
        }
        *self.buckets.entry(now).or_insert(0) += 1;
        true
    }
}

fn now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

pub struct RateLimitGate {
    limiter: Arc<RateLimiter>,
}

impl RateLimitGate {
    pub fn new(limiter: Arc<RateLimiter>) -> Self {
        Self { limiter }
    }
}

#[async_trait]
impl Gate for RateLimitGate {
    fn name(&self) -> &'static str {
        "ratelimit"
    }

    async fn check(&self, ctx: &mut GateCtx) -> Result<(), Rejection> {
        let token: &TokenInfo = ctx.token.as_ref().ok_or(Rejection::AuthSkipped)?;

        // per-key rate limiting
        if !self.limiter.try_acquire(LimitScope::PerKey, &token.id) {
            return Err(Rejection::RateLimited);
        }

        // Optional: per-group (the `user.group` string is used directly as the key)
        if let Some(user) = &ctx.user
            && !self.limiter.try_acquire(LimitScope::PerGroup, &user.group)
        {
            return Err(Rejection::RateLimited);
        }

        Ok(())
    }
}
