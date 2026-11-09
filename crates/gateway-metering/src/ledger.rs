//! In-memory ledger — the atomic operation surface for prehold/settle/release.
//!
//! Reference: new-api open_billing_session.go (request-level billing session) +
//! resolve_funding_source.go (wallet/subscription dual funding sources).
//! V1: single wallet balance slot; V2: subscription windows (issued by the billing domain).

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};

/// Settlement delivery port — the trait lives on the injection surface; re-exported here
/// so the historical `gateway_metering::ledger::SettleSink` path keeps resolving.
pub use gateway_core::metering::SettleSink;

/// Admission ledger port: re-exported from `gateway_core::metering` so the admin plane
/// (api-gateway-state) can name it without depending on the metering engine.
pub use gateway_core::metering::{Hold, Insufficient, Ledger};

/// Balance query side — read-only access to the ledger's available amount.
pub trait BalanceLedger: Ledger {
    /// Admission read: available = quota - used - held.
    fn available(&self, user_key: &str, token_key: &str) -> i64;
}

/// Value shape of a prehold record: (user_key, token_key, amount).
type HoldRecord = (String, String, i64);

pub struct MemoryLedger {
    /// token_key → balance (internal units). A token belongs to exactly one user, so the
    /// balance map is keyed by the token; `user_key` travels on the [`Hold`] record for
    /// attribution only.
    balances: Arc<Mutex<HashMap<String, i64>>>,
    /// Prehold records: hold_id → (user_key, token_key, amount).
    holds: Arc<Mutex<HashMap<u64, HoldRecord>>>,
    /// Global hold_id counter.
    next_id: AtomicU64,
}

impl MemoryLedger {
    pub fn new() -> Self {
        Self {
            balances: Arc::new(Mutex::new(HashMap::new())),
            holds: Arc::new(Mutex::new(HashMap::new())),
            next_id: AtomicU64::new(1),
        }
    }

    /// Take the balance-map lock; poison is treated as unrecoverable (a half-updated ledger
    /// would compute money wrong), so panicking is the only fail-fast choice.
    // Mutex poison is unrecoverable process corruption; this helper cannot fail.
    #[allow(unknown_lints)]
    #[allow(expect_in_non_result)]
    fn lock_balances(&self) -> MutexGuard<'_, HashMap<String, i64>> {
        self.balances.lock().expect("balances map mutex poisoned")
    }

    /// Take the prehold-record lock; poison handling as in [`Self::lock_balances`].
    // Mutex poison is unrecoverable process corruption; this helper cannot fail.
    #[allow(unknown_lints)]
    #[allow(expect_in_non_result)]
    fn lock_holds(&self) -> MutexGuard<'_, HashMap<u64, HoldRecord>> {
        self.holds.lock().expect("holds map mutex poisoned")
    }

    /// Adjust the balance and remove the prehold record (shared by settle / release).
    ///
    /// Takes the balance lock first, then the prehold-record lock, matching `prehold`'s lock
    /// order; a positive `delta` refunds the difference, a negative one charges extra.
    fn apply_hold(&self, hold: &Hold, delta: i64) {
        let mut balances = self.lock_balances();
        let balance = balances.entry(hold.token_key.clone()).or_insert(0);
        *balance += delta;

        self.lock_holds().remove(&hold.id);
    }

    /// Set a balance (for tests/initialization).
    ///
    /// `user_key` is part of the call-site contract (attribution travels on the hold)
    /// but balances are token-scoped: a token belongs to exactly one user.
    pub fn set_balance(&self, _user_key: &str, token_key: &str, amount: i64) {
        let mut balances = self.lock_balances();
        balances.insert(token_key.to_string(), amount);
    }
}

impl Default for MemoryLedger {
    fn default() -> Self {
        Self::new()
    }
}

impl Ledger for MemoryLedger {
    fn prehold(
        &self,
        user_key: &str,
        token_key: &str,
        estimated: i64,
    ) -> Result<Hold, Insufficient> {
        let mut balances = self.lock_balances();
        let balance = balances.entry(token_key.to_string()).or_insert(0);

        if *balance < estimated {
            return Err(Insufficient {
                need: estimated,
                have: *balance,
            });
        }

        *balance -= estimated;
        let id = self.next_id.fetch_add(1, Ordering::SeqCst);

        let mut holds = self.lock_holds();
        holds.insert(id, (user_key.to_string(), token_key.to_string(), estimated));

        Ok(Hold {
            id,
            amount: estimated,
            user_key: user_key.to_string(),
            token_key: token_key.to_string(),
        })
    }

    fn settle(&self, hold: &Hold, actual: i64) -> i64 {
        let diff = hold.amount - actual;
        self.apply_hold(hold, diff); // refund the difference (or charge extra)
        diff
    }

    fn release(&self, hold: &Hold) {
        // A hold is settled or released exactly once: refund only when the prehold
        // record is still in flight, so a repeated release cannot double-refund.
        //
        // The holds guard is scoped to a block and dropped *before* the balance lock
        // is taken: `release` never nests `holds` inside `balances`, so the lock order
        // stays balances → holds (same as `prehold` / `apply_hold`). Rewriting the
        // condition to `if let` would keep the guard alive across the then-block and
        // deadlock — the explicit scope is intentional.
        let still_in_flight = {
            let mut holds = self.lock_holds();
            holds.remove(&hold.id).is_some()
        };
        if still_in_flight {
            let mut balances = self.lock_balances();
            let balance = balances.entry(hold.token_key.clone()).or_insert(0);
            *balance += hold.amount;
        }
    }
    fn reseed(&self, quotas: &gateway_core::gate::QuotaSnapshot) {
        let mut balances = self.lock_balances();
        quotas.iter().for_each(|(token_key, remaining)| {
            balances.insert(token_key, remaining);
        });
        drop(balances);
        // The snapshot carries only durable quotas: in-flight holds were deducted from
        // the old in-memory balance and are not reflected in it, so a plain overwrite
        // would resurrect money that is still reserved. Subtract each token's total
        // in-flight hold amount so a prehold taken before the reseed stays honored and
        // `available` cannot sell the same money twice.
        let held_per_token: HashMap<String, i64> = {
            let holds = self.lock_holds();
            holds
                .values()
                .fold(HashMap::new(), |mut acc, (_, token_key, amount)| {
                    *acc.entry(token_key.clone()).or_default() += amount;
                    acc
                })
        };
        if !held_per_token.is_empty() {
            let mut balances = self.lock_balances();
            for (token_key, held) in held_per_token {
                let balance = balances.entry(token_key).or_insert(0);
                *balance -= held;
            }
        }
    }
}

impl BalanceLedger for MemoryLedger {
    fn available(&self, _user_key: &str, token_key: &str) -> i64 {
        let balances = self.lock_balances();
        balances.get(token_key).copied().unwrap_or(0)
    }
}
