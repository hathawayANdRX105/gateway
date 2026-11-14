//! `gateway-security` — filter-word scanning
//!
//! Pure filtering logic with no pipeline stage: given a word list in configuration,
//! scan and replace the model request context (input) and the response (output,
//! including streaming).
//!
//! ## Core types
//!
//! - [`WordFilter`]: one-shot text filtering over an `aho-corasick` automaton, zero
//!   allocation on no match (`Cow`).
//! - [`StreamFilter`]: streaming filter across chunks; holds back a tail to catch words
//!   spanning chunk boundaries.
//!
//! ## Wiring
//!
//! Wiring belongs to the `forward` domain; this crate only delivers the logic:
//! - Request side: `gateway_forward::stage::ForwardStage::handle` passes the body
//!   through [`WordFilter::filter`] after reading it.
//! - Response side (not wired yet; awaits a later enhancement PR):
//!   `gateway_forward::stream::pipe_chunk` will pass each chunk through
//!   [`StreamFilter::push`] and call [`StreamFilter::flush`] when the stream ends.
//!
//! ## Configuration
//!
//! [`FilterConfig`] is a boot value on the facade `Config` (`Config::security`): the
//! consumer builds it from its own configuration source and hands it over at assembly
//! time. An empty word list leaves the filter dormant.

pub mod scan;
pub mod wordlist;

pub use scan::{StreamFilter, WordFilter};
pub use wordlist::FilterConfig;
