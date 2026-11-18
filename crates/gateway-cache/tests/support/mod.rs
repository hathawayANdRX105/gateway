//! Test-side glue for the hash-candidate comparison.
//!
//! The candidate algorithms, byte layout and corpus live in the benchmark support
//! module (`benches/support/hash_candidates.rs`), so `hash_consistency.rs` and
//! `hash_candidates.rs` measure and assert over the exact same inputs. This file only
//! re-exports it under a path that `#[path]` can reach from a test binary.
//!
//! Kept as a directory module (`tests/support/mod.rs`) so Cargo does not compile it
//! as a standalone integration test.

#[path = "../../../../benches/support/hash_candidates.rs"]
pub mod hash_candidates;
