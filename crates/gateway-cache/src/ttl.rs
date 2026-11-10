//! The bounded store primitive shared by every gateway cache.
//!
//! A thin wrapper over [`moka::sync::Cache`] that fixes one shape for all
//! gateway caches: a bounded concurrent store whose eviction policy (entry
//! count, weighted size) and freshness policy (TTL, time-to-idle) are chosen at
//! construction. Counting and sizing live here so consumers never re-derive
//! the spell per use site.
//!
//! Two layers of expiry coexist on purpose:
//!
//! - **Capacity eviction** is moka's job (LRU — pinned explicitly, see
//!   [`TtlCache::build`]): live entries are evicted as the cache fills, and an
//!   *expired* entry counts the same as any other entry towards the bound.
//!   Fresh values therefore stay within the bound even under sustained write
//!   pressure.
//! - **TTL** is a value-freshness contract, not an eviction strategy: expired
//!   entries are reclaimed opportunistically — on read (checked before every
//!   hit) and by moka's background maintenance — so a stale value is never
//!   served.

use std::collections::hash_map::RandomState;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use moka::policy::EvictionPolicy;

/// Cumulative hit/miss counters of one [`TtlCache`].
///
/// Data-plane decisions (sticky routing, leases, replays) read these; the
/// counters are per-cache, so a consumer's "how often did X hit" question is
/// answered without instrumenting call sites.
#[derive(Debug, Default, Clone, Copy)]
pub struct CacheStats {
    /// Lookups that found a live entry.
    pub hits: u64,
    /// Lookups that found nothing or only an expired entry.
    pub misses: u64,
}

/// A bounded concurrent store with optional TTL / time-to-idle / weighted size.
///
/// `get` returns an owned clone of the value; expired entries are invalidated
/// on read, so a lookup can still miss after the value is in the map — the TTL
/// is the authority, moka's capacity eviction only guarantees the bound.
pub struct TtlCache<K, V>
where
    K: Eq + std::hash::Hash + Clone + Send + Sync + 'static,
    V: Clone + Send + Sync + 'static,
{
    inner: moka::sync::Cache<K, V, RandomState>,
    hits: AtomicU64,
    misses: AtomicU64,
}

impl<K, V> TtlCache<K, V>
where
    K: Eq + std::hash::Hash + Clone + Send + Sync + 'static,
    V: Clone + Send + Sync + 'static,
{
    /// Bounded by entry count, no TTL. Idle eviction (moka's LRU) reclaims the
    /// least recently used entries once `max_capacity` is exceeded, and live
    /// values never expire.
    #[must_use]
    pub fn bounded(max_capacity: u64) -> Self {
        Self::build(moka::sync::Cache::builder().max_capacity(max_capacity))
    }

    /// Bounded by entry count with an entry TTL.
    #[must_use]
    pub fn bounded_with_ttl(max_capacity: u64, ttl: Duration) -> Self {
        Self::build(
            moka::sync::Cache::builder()
                .max_capacity(max_capacity)
                .time_to_live(ttl),
        )
    }

    /// Bounded by total weight (the weigher returns each entry's weight, e.g.
    /// body bytes) with an entry TTL. Entries above 1 weigh more than the bound
    /// can hold, so a single oversized value must be refused by the caller.
    pub fn bounded_weighted_with_ttl(
        max_capacity: u64,
        ttl: Duration,
        weigher: impl Fn(&K, &V) -> u32 + Send + Sync + 'static,
    ) -> Self {
        Self::build(
            moka::sync::Cache::builder()
                .max_capacity(max_capacity)
                .time_to_live(ttl)
                .weigher(weigher),
        )
    }

    /// Bounded by entry count with a time-to-idle: entries unused for `idle`
    /// are reclaimed; actively used ones never expire.
    #[must_use]
    pub fn bounded_with_idle(max_capacity: u64, idle: Duration) -> Self {
        Self::build(
            moka::sync::Cache::builder()
                .max_capacity(max_capacity)
                .time_to_idle(idle),
        )
    }

    fn build(
        builder: moka::sync::CacheBuilder<K, V, moka::sync::Cache<K, V, RandomState>>,
    ) -> Self {
        // LRU, not moka's default TinyLFU: TinyLFU's admission filter admits a new
        // entry only when its estimated frequency *exceeds* the victim's, so a
        // first-ever key (sticky binding, freshly mounted client, one-shot answer)
        // can be rejected outright when the cache is full — a fresh session would
        // be starved by a stable hot set. Recency-only admission keeps the
        // "a new key always lands" contract every gateway cache relies on.
        Self {
            inner: builder.eviction_policy(EvictionPolicy::lru()).build(),
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
        }
    }

    /// Look one entry up: expired / absent → `None`. Counts hit/miss.
    ///
    /// Owned-value design: the value is cloned, so callers cannot hold borrows
    /// into the map across await points.
    pub fn get(&self, key: &K) -> Option<V> {
        if let Some(v) = self.inner.get(key) {
            self.hits.fetch_add(1, Ordering::Relaxed);
            Some(v)
        } else {
            self.misses.fetch_add(1, Ordering::Relaxed);
            None
        }
    }

    /// Insert or overwrite one entry. Re-inserting an existing key resets its
    /// TTL (moka measures TTL from insert), which is how sliding or write-time
    /// expiry is expressed.
    pub fn insert(&self, key: K, value: V) {
        self.inner.insert(key, value);
    }

    /// Drop one entry (expired-on-read reclamation, explicit invalidation).
    pub fn invalidate(&self, key: &K) {
        self.inner.invalidate(key);
    }

    /// Whether the key currently maps to an entry (live or expired — moka's
    /// maintenance may not have reclaimed it yet). Observability only: never
    /// called on the hot path (`run_pending_tasks` takes the policy lock).
    pub fn contains_key(&self, key: &K) -> bool {
        self.inner.contains_key(key)
    }

    /// Entry count, forced through moka's maintenance so counters drained by
    /// earlier reads/writes are applied. Observability path only.
    // moka's entry_count is u64; a cache cannot hold more entries than usize on
    // the running target, so the narrowing cast is lossless in practice.
    #[allow(clippy::cast_possible_truncation)]
    pub fn len(&self) -> usize {
        self.inner.run_pending_tasks();
        self.inner.entry_count() as usize
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Iterate all entries (key + value, cloned), arbitrary order. Used by
    /// eviction-cool state surfaces, not the hot path.
    pub fn iter(&self) -> impl Iterator<Item = (K, V)> + '_ {
        self.inner.iter().map(|(k, v)| ((*k).clone(), v.clone()))
    }

    /// Cumulative hit/miss state surface (observability only).
    pub fn stats(&self) -> CacheStats {
        CacheStats {
            hits: self.hits.load(Ordering::Relaxed),
            misses: self.misses.load(Ordering::Relaxed),
        }
    }
}
