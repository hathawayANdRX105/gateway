//! pricing tests — verify cost computation.

use gateway_metering::pricing::{ModelPrice, price_of};
use gateway_metering::scanner::TokenCounts;

#[test]
fn price_of_zero_tokens() {
    let counts = TokenCounts::default();
    let price = ModelPrice {
        input: 15.0,
        output: 60.0,
        cache: 0.0,
        group_multiplier: 1.0,
    };
    assert_eq!(price_of(counts, &price, 1.0), 0);
}

#[test]
fn price_of_input_only() {
    let counts = TokenCounts {
        prompt: 1_000_000,
        completion: 0,
        cached: 0,
        cache_creation: 0,
        reasoning: 0,
        prompt_includes_cache: false,
    };
    let price = ModelPrice {
        input: 15.0,
        output: 60.0,
        cache: 0.0,
        group_multiplier: 1.0,
    };
    // 1M * $15/M = $15 → 15 * 500_000 = 7_500_000
    assert_eq!(price_of(counts, &price, 1.0), 7_500_000);
}

#[test]
fn price_of_output_only() {
    let counts = TokenCounts {
        prompt: 0,
        completion: 500_000,
        cached: 0,
        cache_creation: 0,
        reasoning: 0,
        prompt_includes_cache: false,
    };
    let price = ModelPrice {
        input: 15.0,
        output: 60.0,
        cache: 0.0,
        group_multiplier: 1.0,
    };
    // 500K * $60/M = $30 → 30 * 500_000 = 15_000_000
    assert_eq!(price_of(counts, &price, 1.0), 15_000_000);
}

#[test]
fn price_of_with_group_multiplier() {
    let counts = TokenCounts {
        prompt: 1_000_000,
        completion: 1_000_000,
        cached: 0,
        cache_creation: 0,
        reasoning: 0,
        prompt_includes_cache: false,
    };
    let price = ModelPrice {
        input: 15.0,
        output: 60.0,
        cache: 0.0,
        group_multiplier: 2.0,
    };
    // (1M * 15/M + 1M * 60/M) * 2 = ($15 + $60) * 2 = $150 → 150 * 500_000 = 75_000_000
    assert_eq!(price_of(counts, &price, 1.0), 75_000_000);
}

#[test]
fn price_of_free_model() {
    let counts = TokenCounts {
        prompt: 1000,
        completion: 500,
        cached: 0,
        cache_creation: 0,
        reasoning: 0,
        prompt_includes_cache: false,
    };
    let price = ModelPrice {
        input: 0.0,
        output: 0.0,
        cache: 0.0,
        group_multiplier: 1.0,
    };
    assert_eq!(price_of(counts, &price, 1.0), 0);
}

// ---------- group_ratio (request group multiplier) ----------

/// `group_ratio=2` multiplies once more on top of the table's `group_multiplier`: cost
/// doubles.
#[test]
fn price_of_group_ratio_doubles_cost() {
    let counts = TokenCounts {
        prompt: 1_000_000,
        completion: 1_000_000,
        cached: 0,
        cache_creation: 0,
        reasoning: 0,
        prompt_includes_cache: false,
    };
    let price = ModelPrice {
        input: 15.0,
        output: 60.0,
        cache: 0.0,
        group_multiplier: 1.0,
    };
    // $75 × gm 1.0 × ratio 2 = $150 → 150 * 500_000 = 75_000_000
    assert_eq!(price_of(counts, &price, 2.0), 75_000_000);
}

/// The two multiplier levels multiply: the table's gm and the request-side
/// `group_ratio` stack independently.
#[test]
fn price_of_group_multiplier_times_group_ratio() {
    let counts = TokenCounts {
        prompt: 1_000_000,
        completion: 0,
        cached: 0,
        cache_creation: 0,
        reasoning: 0,
        prompt_includes_cache: false,
    };
    let price = ModelPrice {
        input: 15.0,
        output: 60.0,
        cache: 0.0,
        group_multiplier: 3.0,
    };
    // $15 × 3 (gm) × 4 (ratio) = $180 → 90_000_000
    assert_eq!(price_of(counts, &price, 4.0), 90_000_000);
}

/// Lower-bound semantics: non-zero usage rounds up to at least 1 unit (7.5 → 8),
/// while zero usage stays 0.
#[test]
fn price_of_ceil_lower_bound() {
    let counts = TokenCounts {
        prompt: 1,
        completion: 0,
        cached: 0,
        cache_creation: 0,
        reasoning: 0,
        prompt_includes_cache: false,
    };
    let price = ModelPrice {
        input: 15.0,
        output: 60.0,
        cache: 0.0,
        group_multiplier: 1.0,
    };
    // 1 tok × $15/M = $1.5e-5 → ×500_000 = 7.5 → ceil 8
    assert_eq!(price_of(counts, &price, 1.0), 8);
    // 0 tokens produce no charge (ceil does not lift zero to non-zero)
    assert_eq!(price_of(TokenCounts::default(), &price, 2.0), 0);
}

/// The cache column is priced independently and also takes both multiplier levels.
#[test]
fn price_of_cache_column_with_ratio() {
    let counts = TokenCounts {
        prompt: 0,
        completion: 0,
        cached: 1_000_000,
        cache_creation: 0,
        reasoning: 0,
        prompt_includes_cache: false,
    };
    let price = ModelPrice {
        input: 15.0,
        output: 60.0,
        cache: 3.75,
        group_multiplier: 2.0,
    };
    // $3.75 × 2 (gm) × 1 (ratio) = $7.5 → 3_750_000
    assert_eq!(price_of(counts, &price, 1.0), 3_750_000);
}

// ---------- ConfigPriceTable::lookup(model, group) ----------

/// The config table matches by model and is independent of group (the real group
/// multiplier goes through settle's `group_ratio`, not the table); unknown model → None
/// (settle treats it as free).
#[test]
fn config_table_lookup_hits_by_model_regardless_of_group() {
    use gateway_metering::pricing::{ConfigPriceTable, PriceTable};
    use std::collections::HashMap;

    let mut prices = HashMap::new();
    prices.insert(
        "gpt-4o".to_string(),
        ModelPrice {
            input: 15.0,
            output: 60.0,
            cache: 0.0,
            group_multiplier: 1.0,
        },
    );
    let table = ConfigPriceTable::new(prices);

    assert!(table.lookup("gpt-4o", "default").is_some());
    assert!(
        table.lookup("gpt-4o", "vip").is_some(),
        "model has a price once matched, independent of group"
    );
    assert!(table.lookup("unknown-model", "vip").is_none());
}

/// OpenAI/Gemini report cached tokens **inside** `prompt_tokens`; Claude reports them
/// outside `input_tokens`. Both shapes describe the same request and must price
/// identically: 200 input tokens at the input rate + 800 cached at the cache rate.
#[test]
fn price_of_does_not_double_charge_cached_tokens() {
    let price = ModelPrice {
        input: 15.0,
        output: 60.0,
        cache: 3.0,
        group_multiplier: 1.0,
    };
    let openai_shape = TokenCounts {
        prompt: 1000,
        cached: 800,
        prompt_includes_cache: true,
        ..Default::default()
    };
    let claude_shape = TokenCounts {
        prompt: 200,
        cached: 800,
        prompt_includes_cache: false,
        ..Default::default()
    };
    // 200 * $15/M + 800 * $3/M = $0.0054 → 2_700 units.
    assert_eq!(price_of(openai_shape, &price, 1.0), 2_700);
    assert_eq!(price_of(claude_shape, &price, 1.0), 2_700);
}

/// Malformed vendor payload (cached > prompt) must clamp billable input at zero instead
/// of wrapping around.
#[test]
fn price_of_saturates_when_cached_exceeds_prompt() {
    let price = ModelPrice {
        input: 15.0,
        output: 60.0,
        cache: 3.0,
        group_multiplier: 1.0,
    };
    let counts = TokenCounts {
        prompt: 10,
        cached: 800,
        prompt_includes_cache: true,
        ..Default::default()
    };
    // Input side clamps to 0; only the cache charge remains: 800 * $3/M = $0.0024 →
    // 1_200 units.
    assert_eq!(price_of(counts, &price, 1.0), 1_200);
}
