//! Sticky table unit tests: key construction / fingerprint same-source / sliding TTL /
//! capacity limit / opportunistic expiry cleanup / degradation when the table isn't mounted.
//!
//! A plain unit test file, no DB / network involved; CI runs it directly.
//! Key semantics: `StickyKey = (public_model, prefix)`, and prefix is either
//! `"sid:"+session id` or `"fp:"+first-4KB-of-raw-body SipHash-1-3`, see dispatch/src/sticky.rs.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use bytes::Bytes;
use gateway_dispatch::health::{AttemptReport, TpsSample};
use gateway_dispatch::sticky::SESSION_HEADER;
use gateway_dispatch::{Candidate, Dispatch, DispatchError, DispatchStage, StickyKey, StickyTable};
use gateway_pipeline::{BodySource, ProtocolKind, RequestCtx, RequestMeta};
use http::{HeaderMap, HeaderValue};
use uuid::Uuid;

const T0: u64 = 1_000_000;

const BODY: &[u8] = br#"{"model":"gpt-4o","messages":[{"role":"user","content":"hi"}]}"#;

// ---------- construction helpers ----------

fn fp_key(model: &str, body: &[u8]) -> StickyKey {
    StickyKey::from_request(model, None, Some(body))
        .expect("in-memory raw body should yield a fingerprint key")
}

fn sid_key(model: &str, sid: &str) -> StickyKey {
    StickyKey::from_request(model, Some(sid), None).expect("session hint should yield a sid key")
}

fn fake_table(now: &Arc<AtomicU64>, ttl_ms: u64, capacity: usize) -> Arc<StickyTable> {
    let clock = {
        let now = now.clone();
        move || now.load(Ordering::Relaxed)
    };
    StickyTable::with_clock(clock, ttl_ms, capacity)
}

fn body_with_head(head_len: usize, fill: u8) -> Vec<u8> {
    // `i % 251 < 256`, so `u8::try_from` never fails here.
    let mut v: Vec<u8> = (0..head_len)
        .map(|i| u8::try_from(i % 251).expect("i % 251 < 256"))
        .collect();
    v.extend(std::iter::repeat_n(fill, 256));
    v
}

fn in_memory(body: &'static [u8]) -> BodySource {
    BodySource::InMemory(Bytes::from_static(body))
}

fn ctx(model: &str, body: BodySource, header: Option<&str>, hint: Option<&str>) -> RequestCtx {
    let mut headers = HeaderMap::new();
    if let Some(v) = header {
        headers.insert(SESSION_HEADER, HeaderValue::from_str(v).unwrap());
    }
    let mut ctx = RequestCtx::new(RequestMeta {
        method: "POST".to_string(),
        path: "/v1/chat/completions".to_string(),
        headers,
        body,
        client_ip: "127.0.0.1".parse().unwrap(),
        request_id: Uuid::now_v7(),
        inbound_protocol: ProtocolKind::OpenAI,
    });
    ctx.requested_model = Some(model.to_string());
    ctx.session_hint = hint.map(str::to_string);
    ctx
}

/// Test stub for sticky-table lookups: `select` always fails (this file asserts the lookup
/// surface, not routing results).
struct StubDispatch;

impl Dispatch for StubDispatch {
    fn select(
        &self,
        _: &str,
        _: &str,
        _: &[String],
        _: Option<&str>,
        _: Option<u64>,
    ) -> Result<Candidate, DispatchError> {
        Err(DispatchError::SnapshotNotReady)
    }

    fn report(&self, _: &str, _: AttemptReport) {}

    fn report_tps(&self, _: &str, _: TpsSample) {}
}

fn bare_stage() -> DispatchStage {
    DispatchStage::new(Arc::new(StubDispatch))
}

fn stage_with(table: Arc<StickyTable>) -> DispatchStage {
    bare_stage().with_sticky_table(table)
}

// ---------- key construction ----------

/// Tails differing only after the first 4KB (the tail-rewriting shape of a multi-turn
/// continuation) land in the same key; a diff within the first 4KB splits the key.
/// Bug it catches: an off-by-one window length (e.g. 2048/8192, or hashing the whole
/// body) would make a long session's tail rewrite break the hit → sticky routing silently stops working.
#[test]
fn key_ignores_differences_beyond_the_fingerprint_window() {
    let a = body_with_head(4096, 0xAA);
    let b = body_with_head(4096, 0xBB);
    assert_eq!(
        fp_key("gpt-4o", &a),
        fp_key("gpt-4o", &b),
        "body tails beyond the 4KB window should not split the key"
    );

    let mut c = a.clone();
    c[10] ^= 0xFF;
    assert_ne!(
        fp_key("gpt-4o", &a),
        fp_key("gpt-4o", &c),
        "a difference within the first 4KB must split the key"
    );
}

/// Bug it catches: dropping `public_model` from the key, so different models' sessions collide into one key → cached sessions get bound to a channel that doesn't even serve the model.
#[test]
fn key_changes_with_model() {
    assert_ne!(fp_key("gpt-4o", BODY), fp_key("claude-3", BODY));
    assert_ne!(sid_key("gpt-4o", "s-1"), sid_key("claude-3", "s-1"));
}

/// Bug it catches: using only the session id and forgetting the model → the same session's different models share one key.
#[test]
fn key_changes_with_session_hint() {
    assert_ne!(sid_key("gpt-4o", "s-1"), sid_key("gpt-4o", "s-2"));
}

/// sid takes priority: same sid + completely different raw bodies ⇒ same key.
/// Bug it catches: the fingerprint taking precedence over the explicit session id → an explicit session id is useless (a multi-turn conversation keeps getting a different body → never hits).
#[test]
fn session_hint_takes_priority_over_body_fingerprint() {
    let k1 = StickyKey::from_request("gpt-4o", Some("s-1"), Some(BODY)).unwrap();
    let k2 = StickyKey::from_request(
        "gpt-4o",
        Some("s-1"),
        Some(b"a completely different request body"),
    )
    .unwrap();
    assert_eq!(
        k1, k2,
        "with an explicit session id the body change must not affect the key"
    );
    assert!(k1.prefix.starts_with("sid:"));
}

/// Two calls on the same raw body (the write side and read side of settle/routing) yield the same key.
/// Bug it catches: introducing a random salt / nonce / time-dependent input into the key, or hashing an "adapted body" computed differently on each side → sticky never hits at all.
#[test]
fn fingerprint_is_stable_for_the_same_raw_body() {
    assert_eq!(fp_key("gpt-4o", BODY), fp_key("gpt-4o", BODY));
    let k = fp_key("gpt-4o", BODY);
    assert!(k.prefix.starts_with("fp:"));
    assert_eq!(
        k.prefix.len(),
        "fp:".len() + 16,
        "fingerprint should be a fixed-width 16-hex digest"
    );
}

/// Known limitation (documented, not papered over): the same session going through two client
/// protocols arrives as two different byte sequences ⇒ two fingerprint keys ⇒ sticky table miss
/// ⇒ degrades to the current state (no boost), with no correctness loss.
/// Bug it catches: "fixing" the limitation by loosening the fingerprint into a coarse key
/// (only the first N bytes / model field / normalization missing a protocol tag) → unrelated sessions collide and bind to the same channel.
#[test]
fn protocol_wrapped_same_session_splits_the_key_known_limitation() {
    let openai = br#"{"model":"gpt-4o","messages":[{"role":"user","content":"hi"}]}"#;
    let claude = br#"{"model":"gpt-4o","anthropic_version":"vertex-2023-10-16","max_tokens":64,"messages":[{"role":"user","content":"hi"}]}"#;
    assert_ne!(
        fp_key("gpt-4o", openai),
        fp_key("gpt-4o", claude),
        "fingerprint is raw-body based: different client-protocol wrapping = different key (miss → degrades to no stickiness)"
    );
}

/// Missing raw body / empty body ⇒ no key at all (degrades to no stickiness).
/// Bug it catches: hashing an empty body anyway → all bodyless requests of the same model (one-shot API calls) pile onto a single key and stick to one channel, creating a false hotspot.
#[test]
fn no_session_hint_and_no_usable_body_yields_no_key() {
    assert!(StickyKey::from_request("gpt-4o", None, None).is_none());
    assert!(StickyKey::from_request("gpt-4o", None, Some(&[])).is_none());
}

/// A blank session id is treated as no session id, falling back to the fingerprint.
/// Bug it catches: stripping only `""` but not whitespace, or not stripping at all ⇒ every session (including all-blank ones) gets collapsed into the same `"sid:"` key and mutually contaminates stickiness.
#[test]
fn blank_session_hint_falls_back_to_fingerprint() {
    let expected = fp_key("gpt-4o", BODY);
    assert_eq!(
        StickyKey::from_request("gpt-4o", Some("   "), Some(BODY)),
        Some(expected.clone())
    );
    assert_eq!(
        StickyKey::from_request("gpt-4o", Some(""), Some(BODY)),
        Some(expected),
    );
    assert!(StickyKey::from_request("gpt-4o", Some("  "), None).is_none());
}

/// The sid prefix is truncated to 128 bytes (header values are client-controlled;
/// without truncation a single 10KB header would explode per-request memory).
/// Bug it catches: removing the length cap or capping after allocation → a malicious/accidental oversized header becomes a table memory amplification vector.
#[test]
fn session_hint_is_truncated_to_128_bytes() {
    let short = "s".repeat(300);
    let long = "s".repeat(5_000);
    let k1 = StickyKey::from_request("gpt-4o", Some(&short), None).unwrap();
    let k2 = StickyKey::from_request("gpt-4o", Some(&long), None).unwrap();
    assert_eq!(
        k1.prefix.len(),
        128,
        "prefix should be truncated to 128 bytes"
    );
    assert_eq!(
        k1, k2,
        "sid values sharing the first 128 bytes collapse into the same key (truncation is byte-wise)"
    );
}

/// The 128-byte cap must land on a UTF-8 char boundary.
/// Bug it catches: cutting through a multibyte character (a CJK session hint puts
/// the raw byte 128 inside a 3-byte character) → panic on slicing a str, or an
/// invalid UTF-8 key. Acceptance: the truncated prefix stays valid UTF-8 at the
/// largest boundary ≤ 128 bytes, and hints sharing that boundary prefix collapse.
#[test]
fn session_hint_truncation_lands_on_char_boundary() {
    // "sid:" is 4 ASCII bytes; "あ" is 3 bytes each → byte 128 would cut the
    // 42nd character (4 + 41*3 = 127), so a correct truncation ends at 127.
    let cjk_hint = "あ".repeat(300);
    let key = StickyKey::from_request("gpt-4o", Some(&cjk_hint), None).unwrap();
    assert_eq!(key.prefix.len(), 127, "must back off to the char boundary");
    assert!(key.prefix.is_char_boundary(key.prefix.len()));
    // 42 identical chars keep byte 128 inside the same 3-byte character, so the
    // same 127-byte prefix is all that survives — tails diverging after it collapse.
    let same_prefix = format!("{}tail-differs", "あ".repeat(42));
    let other = StickyKey::from_request("gpt-4o", Some(&same_prefix), None).unwrap();
    assert_eq!(
        key, other,
        "hints collapsing on the same boundary prefix share a key"
    );
}

// ---------- TTL / capacity / expiry cleanup ----------

/// Within TTL ⇒ hit. Bug it catches: an inverted comparison or `expire_at` treated as already-expired (e.g. comparing against a default-zero timestamp) ⇒ everything misses immediately.
#[test]
fn bind_then_lookup_hits_within_ttl() {
    let now = Arc::new(AtomicU64::new(T0));
    let table = fake_table(&now, 300_000, 8);
    let k = sid_key("gpt-4o", "s-1");
    table.bind(k.clone(), "ch-a");
    now.store(T0 + 299_999, Ordering::Relaxed);
    assert_eq!(table.lookup(&k, table.now_ms()), Some("ch-a".to_string()));
}

/// A second bind slides the expiry (not absolute TTL): hitting after the original
/// expiry moment proves the expiry was refreshed.
/// Bug it catches: computing `expire_at` from the "first bind time" or refusing to
/// refresh on overwrite ⇒ a long-running session goes stale and sticky loses effect halfway through.
#[test]
fn rebind_slides_the_expiry() {
    let now = Arc::new(AtomicU64::new(T0));
    let table = fake_table(&now, 300_000, 8);
    let k = sid_key("gpt-4o", "s-1");
    table.bind(k.clone(), "ch-a");
    now.store(T0 + 290_000, Ordering::Relaxed);
    table.bind(k.clone(), "ch-b");

    // T0+350s exceeds the original T0+300s but is within the slid T0+590s.
    now.store(T0 + 350_000, Ordering::Relaxed);
    assert_eq!(
        table.lookup(&k, table.now_ms()),
        Some("ch-b".to_string()),
        "sliding TTL: still hitting well after the original expiry moment"
    );

    // Beyond the slid expiry ⇒ miss.
    now.store(T0 + 590_001, Ordering::Relaxed);
    assert_eq!(table.lookup(&k, table.now_ms()), None);
}

/// An expired entry gets deleted on read, not just missed: with capacity 1, after k1
/// expires binding k2 would be dropped if k1 were still occupying the table.
/// Bug it catches: a read path that only returns None without removing ⇒ expired entries pile up forever and a table that once filled never accepts anything again.
#[test]
fn expired_entry_is_deleted_not_just_missed() {
    let now = Arc::new(AtomicU64::new(T0));
    let table = fake_table(&now, 1_000, 1);
    let k1 = sid_key("gpt-4o", "s-1");
    table.bind(k1.clone(), "ch-a");
    assert_eq!(table.lookup(&k1, table.now_ms()), Some("ch-a".to_string()));

    // Reaching expire_at exactly counts as expired.
    now.store(T0 + 1_000, Ordering::Relaxed);
    assert_eq!(table.lookup(&k1, table.now_ms()), None);

    let k2 = sid_key("gpt-4o", "s-2");
    table.bind(k2.clone(), "ch-b");
    assert_eq!(
        table.lookup(&k2, table.now_ms()),
        Some("ch-b".to_string()),
        "k1 was deleted after expiring, so capacity 1 still admits k2 (otherwise the write is silently dropped)"
    );
}

/// Full table with every entry still live: the new binding **lands** — the store
/// (LRU, see `gateway_cache::TtlCache`) evicts a victim to make room — and the
/// count still respects capacity. Here the LRU victim is s-0: first inserted,
/// never read since.
/// Bug it catches: dropping the new key behind a `debug!` line ⇒ once a stable
/// hot set fills the table, every *new* session is starved of stickiness forever
/// with no signal (TinyLFU-style frequency admission would do exactly that —
/// this test is the regression tripwire for "a fresh key always lands").
#[test]
fn full_active_table_admits_new_key() {
    let now = Arc::new(AtomicU64::new(T0));
    let table = fake_table(&now, 1_000, 3);
    // Distinct write times from a sliding fake clock: s-0 written first, never
    // read afterwards — the LRU victim.
    table.bind(sid_key("gpt-4o", "s-0"), "ch-a");
    now.store(T0 + 500, Ordering::Relaxed);
    table.bind(sid_key("gpt-4o", "s-1"), "ch-a");
    now.store(T0 + 900, Ordering::Relaxed);
    table.bind(sid_key("gpt-4o", "s-2"), "ch-a");
    assert_eq!(
        table.len(),
        3,
        "table is full with three still-live entries"
    );

    let fresh = sid_key("gpt-4o", "s-fresh");
    table.bind(fresh.clone(), "ch-new");

    assert_eq!(
        table.lookup(&fresh, table.now_ms()),
        Some("ch-new".to_string()),
        "a new session must not be permanently starved by a stable hot set"
    );
    assert_eq!(
        table.len(),
        3,
        "capacity must still hold after evict-one + insert-one"
    );
    assert_eq!(
        table.lookup(&sid_key("gpt-4o", "s-0"), table.now_ms()),
        None,
        "s-0 is the least recently used entry (first written, never read) so it is the victim"
    );
    for k in ["s-1", "s-2"] {
        assert_eq!(
            table.lookup(&sid_key("gpt-4o", k), table.now_ms()),
            Some("ch-a".to_string()),
            "only the LRU entry may be evicted; {k} is more recent"
        );
    }
}

/// Hammering a full table from many threads neither panics nor deadlocks.
/// Bug it catches: mutating the table from inside its own iteration (the old
/// hand-rolled `DashMap` eviction could hold two shard write locks at once ⇒
/// deadlock — the test hangs, CI times out), or an unchecked removal on the
/// data plane's hot path panicking instead of degrading. With the store-backed
/// table this is the regression tripwire for "reintroducing a self-mutating
/// sweep" and for moka's concurrency handling.
#[test]
fn concurrent_binds_against_a_full_table_neither_panic_nor_deadlock() {
    let now = Arc::new(AtomicU64::new(T0));
    let table = fake_table(&now, 300_000, 64);
    for i in 0..64 {
        table.bind(sid_key("gpt-4o", &format!("seed-{i}")), "ch-a");
    }
    assert_eq!(table.len(), 64);

    // Clock pinned at T0: nothing ever expires, so every new-key bind is forced
    // through the store's full-table eviction path. A later bind may evict a
    // just-bound key (all keys tie on recency inside one maintenance batch);
    // the point here is *no deadlock / no panic / capacity never exceeded*,
    // not that every key survives.
    let handles: Vec<_> = (0..8)
        .map(|t| {
            let table = table.clone();
            std::thread::spawn(move || {
                for i in 0..2_000 {
                    let k = sid_key("gpt-4o", &format!("t{t}-{i}"));
                    table.bind(k.clone(), "ch-new");
                    let _ = table.lookup(&k, table.now_ms());
                }
            })
        })
        .collect();
    for h in handles {
        h.join().expect("bind threads must not panic");
    }
    // The store enforces the capacity bound during maintenance; what matters is
    // it stays bounded rather than growing without limit. Without any cap this
    // would hold ~16_000 keys; with the cap it stays near 64.
    assert!(
        table.len() < 1_000,
        "capacity must bound growth under concurrency, got {}",
        table.len()
    );
}

/// Default capacity is 10k: after filling it, the 10001st write is admitted by
/// evicting one live entry, and the table still stops at 10k.
/// Bug it catches: an incorrectly configured capacity (off by 10x, or the cap not
/// taking effect and the table growing without bound).
#[test]
fn default_capacity_is_ten_thousand() {
    let table = StickyTable::new();
    for i in 0..10_000 {
        table.bind(sid_key("gpt-4o", &format!("s-{i}")), "ch-a");
    }
    assert_eq!(table.len(), 10_000);

    table.bind(sid_key("gpt-4o", "s-overflow"), "ch-new");
    assert_eq!(
        table.len(),
        10_000,
        "capacity must hold exactly: evict one + insert one keeps the count"
    );
    assert_eq!(
        table.lookup(&sid_key("gpt-4o", "s-overflow"), table.now_ms()),
        Some("ch-new".to_string()),
        "the 10001st distinct session is admitted by evicting the soonest-expiring entry, not starved"
    );
}

/// Capacity 0 ⇒ every write is dropped, every read misses, nothing panics.
/// Bug it catches: writing under `len < capacity - 1` / underflowing capacity arithmetic ⇒ panic (capacity misconfigured to 0 crashes the data plane).
#[test]
fn zero_capacity_degrades_silently() {
    let now = Arc::new(AtomicU64::new(T0));
    let table = fake_table(&now, 300_000, 0);
    let k = sid_key("gpt-4o", "s-1");
    table.bind(k.clone(), "ch-a");
    assert_eq!(table.lookup(&k, table.now_ms()), None);
    assert!(table.is_empty());
}

/// Expired entries must not starve a fresh binding: once full with expired
/// entries, a new bind still lands and the count stays bounded. Bug it
/// catches: dropping the new key once full ⇒ sticky routing goes permanently
/// dead after a traffic spike (reclamation is the store's job — moka's real
/// clock TTL plus LRU eviction — so the fake-clock test asserts the
/// admission contract, not the sweep).
#[test]
fn full_table_admits_fresh_binding_after_expiry() {
    let now = Arc::new(AtomicU64::new(T0));
    let table = fake_table(&now, 1_000, 4);
    for i in 0..4 {
        table.bind(sid_key("gpt-4o", &format!("s-{i}")), "ch-a");
    }
    assert_eq!(table.len(), 4);

    now.store(T0 + 1_001, Ordering::Relaxed);
    let fresh = sid_key("gpt-4o", "s-fresh");
    table.bind(fresh.clone(), "ch-new");

    assert_eq!(
        table.lookup(&fresh, table.now_ms()),
        Some("ch-new".to_string()),
        "expired entries must never block a fresh binding"
    );
    assert!(
        table.len() <= 4,
        "capacity stays the bound, got {}",
        table.len()
    );
}

/// An expired lookup deletes the looked-up entry on the spot (so it cannot
/// occupy a slot); other expired entries are left to their own lookups and to
/// the store's real-clock reclamation. Bug it catches: a read path that only
/// returns None without reclaiming ⇒ expired entries occupy capacity
/// indefinitely (a write-heavy scenario that hits the cap with only reads can
/// never free space).
#[test]
fn expired_lookup_reclaims_the_looked_up_entry() {
    let now = Arc::new(AtomicU64::new(T0));
    let table = fake_table(&now, 1_000, 4);
    for i in 0..4 {
        table.bind(sid_key("gpt-4o", &format!("s-{i}")), "ch-a");
    }
    now.store(T0 + 1_001, Ordering::Relaxed);
    assert_eq!(
        table.len(),
        4,
        "nothing is reclaimed before the first lookup after expiry"
    );

    // Expired entry: miss + delete on the spot.
    assert_eq!(
        table.lookup(&sid_key("gpt-4o", "s-0"), table.now_ms()),
        None
    );
    assert_eq!(table.len(), 3, "the expired looked-up entry is reclaimed");
    assert_eq!(
        table.lookup(&sid_key("gpt-4o", "s-1"), table.now_ms()),
        None,
        "every expired lookup misses"
    );
    assert_eq!(table.len(), 2, "each expired lookup reclaims its own entry");

    // A ghost key (never bound) misses without touching the table.
    assert_eq!(
        table.lookup(&sid_key("gpt-4o", "ghost"), table.now_ms()),
        None
    );
    assert_eq!(table.len(), 2, "a ghost miss reclaims nothing");
}

// ---------- DispatchStage wiring ----------

/// Not mounting a table ⇒ never sticky. Even if the table's key matches exactly, the stage returns None (and vice versa, staging it hits).
/// Bug it catches: an accidentally mounted default table / an unhandled Option ⇒ sticky applies when the feature was never enabled (behavior differs from "not mounted" ⇒ violates the "behavior identical to the current state" invariant).
#[test]
fn stage_without_sticky_table_never_sticks() {
    let table = StickyTable::new();
    table.bind(sid_key("gpt-4o", "s-1"), "ch-a");
    // Deliberately not calling with_sticky_table.
    let stage = bare_stage();
    let ctx = ctx("gpt-4o", in_memory(BODY), None, Some("s-1"));
    assert_eq!(stage.sticky_channel_for(&ctx), None);
}

/// `ctx.session_hint` hit ⇒ returns the bound channel key.
/// Bug it catches: `ctx.session_hint` never actually being read (an added field left at None) ⇒ explicit session ids carry no effect at all.
#[test]
fn stage_hits_by_session_hint_field() {
    let table = StickyTable::new();
    table.bind(sid_key("gpt-4o", "s-1"), "ch-a");
    let stage = stage_with(table);
    let ctx = ctx("gpt-4o", in_memory(BODY), None, Some("s-1"));
    assert_eq!(stage.sticky_channel_for(&ctx), Some("ch-a".to_string()));
}

/// `session_hint` missing ⇒ falls back to the x-session-id request header.
/// Bug it catches: the header path not being wired (`session_hint=None` is always None in production) ⇒ sessions only stick when something else sets the field ⇒ sticky never takes effect.
#[test]
fn stage_falls_back_to_session_header() {
    let table = StickyTable::new();
    table.bind(sid_key("gpt-4o", "h-1"), "ch-h");
    let stage = stage_with(table);
    let ctx = ctx("gpt-4o", in_memory(BODY), Some("h-1"), None);
    assert_eq!(stage.sticky_channel_for(&ctx), Some("ch-h".to_string()));
}

/// `session_hint` takes priority over the request header: the two bindings point to different channels, and the result follows the field.
/// Bug it catches: the header taking precedence (or the field being ignored) ⇒ an explicitly corrected session id still uses the header's binding ⇒ sticky errors cannot be corrected in time.
#[test]
fn stage_session_hint_takes_priority_over_header() {
    let table = StickyTable::new();
    table.bind(sid_key("gpt-4o", "s-1"), "ch-from-field");
    table.bind(sid_key("gpt-4o", "h-1"), "ch-from-header");
    let stage = stage_with(table);

    let with_field = ctx("gpt-4o", in_memory(BODY), Some("h-1"), Some("s-1"));
    assert_eq!(
        stage.sticky_channel_for(&with_field),
        Some("ch-from-field".to_string())
    );

    let header_only = ctx("gpt-4o", in_memory(BODY), Some("h-1"), None);
    assert_eq!(
        stage.sticky_channel_for(&header_only),
        Some("ch-from-header".to_string())
    );
}

/// No session id ⇒ falls back to the raw body fingerprint (weak stickiness).
/// Bug it catches: writing the body-fingerprint key but reading via an undefined source / hashing the adapted body (unavailable on the routing side) ⇒ write and read use different key sources ⇒ never hits.
#[test]
fn stage_falls_back_to_body_fingerprint() {
    let table = StickyTable::new();
    table.bind(fp_key("gpt-4o", BODY), "ch-fp");
    let stage = stage_with(table);
    let ctx = ctx("gpt-4o", in_memory(BODY), None, None);
    assert_eq!(stage.sticky_channel_for(&ctx), Some("ch-fp".to_string()));
}

/// When the raw body isn't in memory (spooled to disk), it degrades to no stickiness (and never panics).
/// Bug it catches: reading disk bodies directly / calling `bytes()` on a disk body ⇒ disk IO or a panic on the data plane's hot path.
#[test]
fn stage_degrades_when_body_not_in_memory() {
    let table = StickyTable::new();
    table.bind(fp_key("gpt-4o", BODY), "ch-fp");
    let stage = stage_with(table);
    let disk = BodySource::OnDisk {
        path: "/dev/null".into(),
        len: BODY.len() as u64,
    };
    let ctx = ctx("gpt-4o", disk, None, None);
    assert_eq!(stage.sticky_channel_for(&ctx), None);
}

/// No model name (gate hasn't parsed yet / request has no model) ⇒ no key, degrades to no stickiness.
/// Bug it catches: using an empty string as the model component of the key ⇒ all request types without a model collapse onto the same key ⇒ meaningless stickiness between unrelated requests.
#[test]
fn stage_degrades_without_requested_model() {
    let table = StickyTable::new();
    table.bind(sid_key("", "s-1"), "ch-a");
    let stage = stage_with(table);
    let mut ctx = ctx("gpt-4o", in_memory(BODY), None, Some("s-1"));
    ctx.requested_model = None;
    assert_eq!(stage.sticky_channel_for(&ctx), None);
}
