//! Measurement-only model of the proposed cheap prefilter in front of the response
//! cache key.
//!
//! The proposal under evaluation is a two-level key: a 16-byte `quick_fp` that is
//! cheap to compute, plus a second-level index mapping `quick_fp` to the full key.
//! The claim being tested is that a prefilter lookup lets a request skip the full
//! SHA-256 digest.
//!
//! That claim only holds on a hit. The response cache is write-through, so a
//! request whose fingerprint is absent from the index still has to derive the full
//! key: the new response is filed under that key. The miss path therefore pays the
//! fingerprint plus the index probe on top of the digest it needed anyway, and the
//! hit path is the only one that saves anything. This module models exactly those
//! three costs and nothing else; no production code is wired to it.
//!
//! The full key is derived from the production authority's identity stream
//! (`gateway_cache::request_identity_bytes`) plus the raw body, so a layout or
//! version change upstream breaks the compile here instead of drifting. The corpus
//! comes from the shared `hash_candidates` module, so the numbers line up with
//! `benches/hash_candidates.rs`. Expected to be included as a sibling of that
//! module under the name `support`, which is how the corpus is reached (`super::support`).

use std::collections::HashMap;

use sha2::{Digest, Sha256};
use xxhash_rust::xxh3::xxh3_64;

use super::support::RequestCase;

/// Leading body bytes the fingerprint hashes: touching the whole body is exactly
/// the cost the prefilter exists to avoid.
pub const PREFILTER_PREFIX: usize = 4096;

/// Entries the index is prefilled with, so a probe walks a realistically populated
/// table instead of an empty one.
pub const INDEX_FILL: usize = 4096;

/// Byte width of the full key: scope digest followed by body digest.
const FULL_KEY_LEN: usize = 64;

/// The full key as the cache stores it.
pub type FullKey = [u8; FULL_KEY_LEN];

/// Length of a synthetic filler body: long enough to carry a distinct 8-byte tag in
/// its prefix, short enough that building `INDEX_FILL` of them costs nothing.
const FILLER_BODY_LEN: usize = 64;

/// The cheap fingerprint: body length plus XXH3-64 over the body prefix.
///
/// The length is part of the value so a longer body can never reuse a shorter
/// body's prefix digest.
#[must_use]
pub fn quick_fp(body: &[u8]) -> [u8; 16] {
    let prefix = &body[..body.len().min(PREFILTER_PREFIX)];
    let mut fp = [0u8; 16];
    fp[..8].copy_from_slice(&(body.len() as u64).to_be_bytes());
    fp[8..].copy_from_slice(&xxh3_64(prefix).to_le_bytes());
    fp
}

/// Derive the full key for `case` from the production authority's streams.
///
/// `response_key` owns the layout; this wrapper only concatenates its two
/// digests, so a layout or version change upstream surfaces as a compile-time
/// error here instead of a silently wrong measurement.
#[must_use]
pub fn full_key(case: &RequestCase) -> FullKey {
    let (identity, body) = case.streams();
    let scope_hash = Sha256::digest(&identity);
    let body_hash = Sha256::digest(body);
    let mut key = [0u8; FULL_KEY_LEN];
    key[..32].copy_from_slice(&scope_hash);
    key[32..].copy_from_slice(&body_hash);
    key
}

/// The proposed second-level index: `quick_fp` to full key.
///
/// The value is a `Vec<u8>` standing in for the stored full key, so the measured
/// shape (map probe plus key hand-back) matches the real structure without
/// importing it.
#[derive(Debug)]
pub struct QuickIndex {
    entries: HashMap<[u8; 16], Vec<u8>>,
}

impl QuickIndex {
    /// An index holding `fill` entries, none of them the measured case: the table a
    /// first request probes, where the lookup misses.
    #[must_use]
    pub fn cold(fill: usize) -> Self {
        let entries = (0..fill).map(filler_entry).collect();
        Self { entries }
    }

    /// Record `case` as already cached, which is the state a repeat request — the
    /// only request that can hit — probes.
    pub fn store(&mut self, case: &RequestCase) {
        let fp = quick_fp(&case.body);
        let key = full_key(case).to_vec();
        self.entries.insert(fp, key);
    }

    /// Probe for the stored key of `fp`; `None` is the miss branch.
    #[must_use]
    pub fn probe(&self, fp: &[u8; 16]) -> Option<&[u8]> {
        self.entries.get(fp).map(Vec::as_slice)
    }
}

/// The proposed lookup path: fingerprint, probe, and fall back to the full key when
/// the probe misses — the write-through store still needs that key to file the new
/// response.
///
/// One function models both outcomes; the bench drives it against a cold index
/// (miss) and a warmed index (hit).
#[must_use]
pub fn prefilter_lookup(index: &QuickIndex, case: &RequestCase) -> FullKey {
    let fp = quick_fp(&case.body);
    index
        .probe(&fp)
        .and_then(|key| key.try_into().ok())
        .unwrap_or_else(|| full_key(case))
}

/// Synthetic filler body: distinct from every corpus case (their lengths differ at
/// minimum) and cheap to rebuild.
fn filler_body(i: usize) -> Vec<u8> {
    let mut body = vec![0u8; FILLER_BODY_LEN];
    body[..8].copy_from_slice(&(i as u64).to_be_bytes());
    body
}

/// One synthetic index entry: filler fingerprint paired with a stand-in key.
fn filler_entry(i: usize) -> ([u8; 16], Vec<u8>) {
    (quick_fp(&filler_body(i)), filler_key(i))
}

/// Filler value standing in for a stored full key.
fn filler_key(i: usize) -> Vec<u8> {
    let mut key = vec![0u8; FULL_KEY_LEN];
    key[..8].copy_from_slice(&(i as u64).to_be_bytes());
    key
}
