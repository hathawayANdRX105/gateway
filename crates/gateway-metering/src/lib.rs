//! # metering — metering (off the hot path)
//!
//! Three sub-responsibilities, all built around one principle: **the forwarding
//! path performs only local in-memory operations**.
//!
//! ```text
//! ① prehold  pre-deduct (ledger)    after admission, reserve the estimated cost in local balance
//! ② scan     stream scan (scanner)  beside the SSE pipe, capturing usage/counts
//! ③ settle   settlement (settle)    request end → refund delta (memory) + UsageEvent → WAL
//! ```
//!
//! Pricing lives in [`pricing`]; estimation when upstream usage is absent lives in
//! [`estimate`].
//!
//! The two things the apps side exchanges with this engine — the [`SettleSink`] delivery
//! channel and the [`pricing::PriceTable`] port — are declared in `gateway-core::metering`
//! and re-exported here, so the billing crate can implement them without depending on this
//! crate.
//! The WAL push to center is driven by service/sync; this module only "produces
//! correct events".
//!
//! Crash semantics (explicit trade-off): preholds live only in memory and are lost
//! on a crash — acceptable, because the `UsageEvent` (WAL) is the authoritative
//! billing source; the balance of record is the center's converged `used_quota`.

pub mod estimate;
pub mod ledger;
pub mod pricing;
pub mod scanner;
pub mod settle;

pub use gateway_core::metering::SettleSink;
pub use ledger::{Hold, Ledger};
pub use scanner::StreamScanner;
pub use settle::{extract_finish_reason, extract_usage, settle_event};
