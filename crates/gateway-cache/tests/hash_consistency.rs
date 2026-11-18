//! Correctness and consistency tests for the hash-candidate comparison.
//!
//! Purpose: decide with evidence whether the response-cache key must stay SHA-256.
//! The benchmark measures speed; this file checks the properties a cache key needs:
//!
//! - **Correctness**: each candidate returns a digest of its declared width, and the
//!   production SHA-256 matches published NIST vectors (so the harness feeds the
//!   bytes we think it does).
//! - **Determinism**: the same input yields the same digest on repeated calls and on
//!   independently constructed hashers — a key that varies within one process would
//!   never hit.
//! - **Collision resistance at sample scale**: 2048 distinct requests produce 2048
//!   distinct digests per candidate. This is a smoke check, not a collision-resistance
//!   proof: 2048 samples cannot distinguish a 64-bit from a 256-bit birthday bound.
//! - **Isolation**: inputs that differ in exactly one component (including the
//!   concatenation trap) never share a digest.
//!
//! Acceptance: every candidate in `Algorithm::ALL` passes correctness, determinism,
//! collision and isolation checks. Cross-process stability is a declared policy
//! (`is_cross_process_stable`), asserted separately because it cannot be observed
//! inside one process.

mod support;

use std::collections::HashSet;

use gateway_cache::{CacheScope, response_key};
use support::hash_candidates::{Algorithm, boundary_pairs, corpus};

/// Sample count for the collision/isolation sweep. Above 2000 as requested, and large
/// enough that an accidental collision or a broken encoding shows up immediately.
const SAMPLE_COUNT: usize = 2048;

fn encoded_corpus() -> Vec<(Vec<u8>, Vec<u8>)> {
    corpus(SAMPLE_COUNT)
        .iter()
        .map(|case| {
            let (identity, body) = case.streams();
            (identity, body.clone())
        })
        .collect()
}

/// Every candidate returns its declared digest width, and repeats exactly.
#[test]
fn digests_have_declared_width_and_are_deterministic() {
    let samples = encoded_corpus();
    for algorithm in Algorithm::ALL {
        for (index, (identity, body)) in samples.iter().enumerate() {
            let first = algorithm.key_bytes(identity, body);
            let second = algorithm.key_bytes(identity, body);
            assert_eq!(
                first.len(),
                algorithm.digest_len(),
                "{} produced {} bytes at sample {index}",
                algorithm.label(),
                first.len()
            );
            assert_eq!(
                first,
                second,
                "{} is not deterministic at sample {index}",
                algorithm.label()
            );
        }
    }
}

/// 2048 distinct requests never collide, for every candidate.
#[test]
fn no_collisions_across_2048_distinct_requests() {
    let samples = encoded_corpus();
    for algorithm in Algorithm::ALL {
        let mut seen: HashSet<Vec<u8>> = HashSet::with_capacity(samples.len());
        for (index, (identity, body)) in samples.iter().enumerate() {
            let digest = algorithm.key_bytes(identity, body);
            assert!(
                seen.insert(digest),
                "{} collided at sample {index}",
                algorithm.label()
            );
        }
        assert_eq!(
            seen.len(),
            SAMPLE_COUNT,
            "{} produced {} unique digests for {SAMPLE_COUNT} requests",
            algorithm.label(),
            seen.len()
        );
    }
}

/// Inputs differing in one component — including the concatenation trap — stay distinct.
#[test]
fn isolation_boundaries_never_collide() {
    for algorithm in Algorithm::ALL {
        for (description, left, right) in boundary_pairs() {
            let (left_id, left_body) = left.streams();
            let (right_id, right_body) = right.streams();
            assert_ne!(
                (left_id.clone(), left_body.clone()),
                (right_id.clone(), right_body.clone()),
                "{}: request bytes are identical for case {description}",
                algorithm.label()
            );
            assert_ne!(
                algorithm.key_bytes(&left_id, &left_body),
                algorithm.key_bytes(&right_id, &right_body),
                "{} collided on case {description}",
                algorithm.label()
            );
        }
    }
}

/// SHA-256 matches published NIST vectors, proving the harness hashes the intended bytes.
#[test]
fn sha256_matches_published_vectors() {
    use sha2::{Digest, Sha256};

    let empty = format!("{:x}", Sha256::digest(b""));
    assert_eq!(
        empty, "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
        "SHA-256 of the empty input must match the NIST vector"
    );

    let abc = format!("{:x}", Sha256::digest(b"abc"));
    assert_eq!(
        abc, "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad",
        "SHA-256 of \"abc\" must match the NIST vector"
    );
}

/// Independently constructed hashers agree, so no hidden per-instance seeding exists.
#[test]
fn stable_candidates_are_reproducible_across_independent_hashers() {
    let samples = encoded_corpus();
    for algorithm in Algorithm::ALL {
        if !algorithm.is_cross_process_stable() {
            continue;
        }
        for (index, (identity, body)) in samples.iter().take(64).enumerate() {
            let a = algorithm.key_bytes(identity, body);
            let b = algorithm.key_bytes(identity, body);
            assert_eq!(
                a,
                b,
                "{} differed between hashers at sample {index}",
                algorithm.label()
            );
        }
    }
}

/// The harness must measure the production layout byte-for-byte: the baseline
/// candidate's output equals what `response_key` stores.
/// Bug it catches: `request_identity_bytes` changing (layout / version / framing)
/// while the harness keeps hashing stale bytes — measurements would silently
/// describe a key the production no longer produces.
#[test]
fn harness_baseline_matches_production_key_bytes() {
    for case in &corpus(64) {
        let scope = CacheScope {
            user_key: case.user_key.clone(),
            group: case.group.clone(),
        };
        let (identity, body) = case.streams();
        // response_key takes the RAW body and adds its own length prefix; the
        // harness stream already carries that prefix, so hand over the raw bytes.
        let expected = response_key(&scope, &case.model, &case.protocol, &case.body).as_bytes();
        let produced = Algorithm::Sha256.key_bytes(&identity, &body);
        assert_eq!(
            produced, expected,
            "harness layout drifted from production for user {}",
            case.user_key
        );
    }
}

/// The stability policy is explicit: only non-randomized digests may key a shared cache.
#[test]
fn stability_policy_matches_declared_flags() {
    assert!(
        Algorithm::Sha256.is_cross_process_stable(),
        "the production baseline must stay cross-process stable"
    );
    assert!(
        !Algorithm::AHashFixed.is_cross_process_stable(),
        "ahash output depends on build/platform; it must not key a shared cache"
    );
    assert!(
        !Algorithm::FoldHashFixed.is_cross_process_stable(),
        "foldhash output is not guaranteed stable; it must not key a shared cache"
    );
    assert!(
        Algorithm::Blake3.is_cross_process_stable()
            && Algorithm::Xxh3_128.is_cross_process_stable(),
        "the deterministic alternatives must report stable output"
    );
}
