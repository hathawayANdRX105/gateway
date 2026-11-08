//! `graylist` — gate 6: consecutive-failure banning (brute-force protection)

use std::sync::Arc;
use std::time::{Duration, Instant};

use arc_swap::ArcSwap;
use async_trait::async_trait;
use dashmap::DashMap;

use super::auth::sha256;
use super::chain::{Gate, GateCtx};
use super::error::Rejection;

/// Graylist state: one failure counter plus one temporary-ban deadline per key.
pub struct GrayListState {
    /// Failure count + first-failure time
    pub fail_count: DashMap<[u8; 32], FailEntry>,
    /// Whether the key is currently banned
    pub blocked_until: DashMap<[u8; 32], Instant>,
}

impl Default for GrayListState {
    fn default() -> Self {
        Self {
            fail_count: DashMap::new(),
            blocked_until: DashMap::new(),
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct FailEntry {
    pub streak: u32,
    /// Start of the streak; it resets when no failure occurs within this window
    pub window_start: Instant,
}

/// Consecutive-failure threshold that triggers a ban.
pub const FAIL_STREAK_THRESHOLD: u32 = 5;
/// Length of a single ban.
pub const BLOCK_DURATION: Duration = Duration::from_secs(60);
/// Failure-counting window (the streak resets when no failure occurs within it).
pub const STREAK_WINDOW: Duration = Duration::from_secs(300);

pub struct GrayListGate {
    state: Arc<ArcSwap<GrayListState>>,
}

impl GrayListGate {
    pub fn new(state: Arc<ArcSwap<GrayListState>>) -> Self {
        Self { state }
    }

    /// Records one result (called by forward after completion).
    ///
    /// Success: resets the streak.
    /// Failure: streak +1; reaching the threshold bans the key for `BLOCK_DURATION`.
    pub fn record(&self, hash: [u8; 32], success: bool) {
        let state = self.state.load();
        if success {
            state.fail_count.remove(&hash);
            state.blocked_until.remove(&hash);
            return;
        }
        let now = Instant::now();
        let mut entry = state.fail_count.entry(hash).or_insert(FailEntry {
            streak: 0,
            window_start: now,
        });
        if now.duration_since(entry.window_start) > STREAK_WINDOW {
            entry.streak = 0;
            entry.window_start = now;
        }
        entry.streak = entry.streak.saturating_add(1);
        if entry.streak >= FAIL_STREAK_THRESHOLD {
            tracing::warn!(
                "graylist: key banned after {} failures",
                FAIL_STREAK_THRESHOLD
            );
            state.blocked_until.insert(hash, now + BLOCK_DURATION);
            entry.streak = 0;
        }
    }
}

#[async_trait]
impl Gate for GrayListGate {
    fn name(&self) -> &'static str {
        "graylist"
    }

    async fn check(&self, ctx: &mut GateCtx) -> Result<(), Rejection> {
        // Use the token hash when the token already passed the auth gate; otherwise hash
        // the raw key on the fly; neither present = allow.
        let Some(hash) = ctx
            .token
            .as_ref()
            .map(|t| t.id_hash)
            .or_else(|| ctx.raw_key.as_deref().map(sha256))
        else {
            return Ok(());
        };

        let state = self.state.load();
        if let Some(until) = state.blocked_until.get(&hash)
            && Instant::now() < *until.value()
        {
            return Err(Rejection::Graylisted);
        }
        Ok(())
    }
}
