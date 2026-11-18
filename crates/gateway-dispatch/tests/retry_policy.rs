//! Model-specific retry policy lookup: exact per-model entry wins, unknown models
//! fall back to the default policy.

use gateway_dispatch::retry::{ModelRetryPolicies, RetryPolicy};
use std::collections::HashMap;

#[test]
fn policy_for_hits_per_model_then_default() {
    let default = RetryPolicy {
        max_attempts: 5,
        total_budget_ms: 45_000,
    };
    let per_model = RetryPolicy {
        max_attempts: 2,
        total_budget_ms: 1_000,
    };
    let mut map = HashMap::new();
    map.insert("m".to_string(), per_model);
    let policies = ModelRetryPolicies::new(default, map);

    assert_eq!(
        policies.policy_for("m"),
        &RetryPolicy {
            max_attempts: 2,
            total_budget_ms: 1_000,
        },
        "exact per_model hit returns exactly that entry, not the default",
    );
    assert_eq!(
        policies.policy_for("unknown"),
        &default,
        "unknown model falls back to the default policy",
    );
}
