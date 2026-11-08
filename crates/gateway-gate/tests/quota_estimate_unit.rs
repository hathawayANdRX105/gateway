//! Unit-contract regression test: `estimate_cost` must keep the same books as
//! `gateway_metering::pricing::price_of` (1 internal unit = $1/500_000). Bug
//! reproduction: the old code's `as i64` truncation and `* 1_000_000` scaling left the
//! estimate at 0, so "balance < estimate → 402" never engaged in production.

use gateway_gate::quota::estimate_cost;
use gateway_gate::snapshot::PriceRow;

#[test]
fn estimate_cost_unit_contract_gpt4o_4096() {
    // gpt-4o output $15/M -> 15 * 500_000 = 7_500_000 internal units/M
    // max_tokens=4096 -> 7_500_000 * 4096 / 1_000_000 = 30_720 internal units
    let price = PriceRow {
        input_per_m: 0.0,
        output_per_m: 7_500_000.0,
        cache_per_m: 0.0,
    };
    let cost = estimate_cost(&price, 4096);
    assert_eq!(
        cost, 30_720,
        "gpt-4o 4096 output tokens must be 30_720 internal units"
    );
}

#[test]
fn estimate_cost_small_max_tokens_not_zero() {
    // A small max_tokens must not truncate to 0 (the old `as i64` turned 1 token into 0)
    let price = PriceRow {
        input_per_m: 0.0,
        output_per_m: 7_500_000.0,
        cache_per_m: 0.0,
    };
    // 1 token -> 7_500_000 * 1 / 1_000_000 = 7.5 -> ceil = 8
    let cost = estimate_cost(&price, 1);
    assert!(cost >= 1, "small token count must not round to zero");
}
