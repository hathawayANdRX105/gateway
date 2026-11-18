pub mod candidate;
pub mod dispatcher;
pub mod failure_scope;
pub mod health;
pub mod latency_digest;
pub mod ratelimit;
pub mod retry;
pub mod selector;
pub mod stage;
pub mod sticky;
pub mod tdigest;
pub mod token_budget;

pub use candidate::{Candidate, STATUS_ENABLED, resolve_candidate};
pub(crate) use dispatcher::utc_now_ms;
pub use dispatcher::{Dispatch, DispatchError, Dispatcher, candidates_from_snapshot};
pub use failure_scope::{FailureScope, classify_channel_scope};
/// Channel / route-unit catalog snapshot — defined on the injection surface so the
/// admin plane can build and read it without depending on this engine.
pub use gateway_core::dispatch::Snapshot;
pub use health::{
    AttemptReport, FailureClass, HealthState, HealthTable, MemoryHealthTable, TpsSample,
};
pub use latency_digest::LatencyDigestTable;
pub use ratelimit::{RateLimitSpec, SlidingWindow};
pub use retry::{
    Attempt, AttemptOutcome, Failover, FakeTimeControl, ModelRetryPolicies, RealTimeControl,
    RetryLoop, RetryPolicy, TimeControl, run_retry_loop,
};
pub use selector::{
    DEFAULT_EXPLORE_RATE, DEFAULT_STICKY_BOOST, P2CSelector, Selector, WeightedRandom,
    clamp_sticky_boost,
};
pub use stage::DispatchStage;
pub use sticky::{StickyEntry, StickyKey, StickyTable};
pub use token_budget::{COMPLETION_EMA_INIT, TokenBudgetState, TokenHandle};
