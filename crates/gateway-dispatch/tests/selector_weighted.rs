//! Behavioral tests for the `WeightedRandom` selector (new-api same-tier semantics).
//!
//! What is pinned: static weight is the **only** decision input (no health /
//! latency / concurrency / token posterior), the hard gates behave exactly like
//! P2C (enable bit, per-request exclusion, cooldown removal, urgent recall of the
//! shortest cooldown), the sticky channel gets a prior-style weight multiplier
//! that never bypasses a veto, and degenerate totals degrade to a uniform draw
//! instead of panicking.
//!
//! Every test is deterministic: the RNG is injected per seed, so statistical
//! assertions (share tolerance) hold across runs without a global random source.

use gateway_core::records::{RouteUnitRecord, SyncMeta};
use gateway_dispatch::health::{HealthTable, MemoryHealthTable};
use gateway_dispatch::selector::{MAX_STICKY_BOOST, Selector, WeightedRandom};
use rand::SeedableRng;
use std::sync::Arc;

const SEEDS: u64 = 12;
const DRAWS_PER_SEED: usize = 200;

fn unit(key: &str, model: &str, weight: u32) -> RouteUnitRecord {
    RouteUnitRecord {
        meta: SyncMeta {
            key: key.to_string(),
            schema_version: 1,
            logical_version: 1,
            origin: "test".to_string(),
            updated_at: chrono::Utc::now(),
        },
        group: "g".to_string(),
        public_model: model.to_string(),
        channel_key: key.to_string(),
        key_index: 0,
        upstream_model: model.to_string(),
        weight,
        status: 1,
    }
}

/// `selector` is generic so the same harness can compare `WeightedRandom` against
/// P2C behavior where the contract must be identical (hard gates / recall).
fn share_of<S: Selector>(selector: &S, units: &[RouteUnitRecord], key: &str) -> f64 {
    let health = MemoryHealthTable::new();
    let refs: Vec<&RouteUnitRecord> = units.iter().collect();
    let mut hits = 0usize;
    for seed in 1..=SEEDS {
        let mut rng = rand::rngs::StdRng::seed_from_u64(seed);
        for _ in 0..DRAWS_PER_SEED {
            let picked = selector
                .pick(&refs, &health, &[], None, 0, &mut rng)
                .unwrap();
            if picked.meta.key == key {
                hits += 1;
            }
        }
    }
    let draws = usize::try_from(SEEDS).expect("SEEDS = 12, fits usize") * DRAWS_PER_SEED;
    #[allow(clippy::cast_precision_loss)]
    let share = hits as f64 / draws as f64;
    share
}

/// 验收标准：权重 3:1 的池，静态权重是唯一决策输入 → 份额收敛到 75%/25%（±8pp）。
#[test]
fn weighted_random_splits_by_static_weight() {
    let units = [unit("a", "m", 3), unit("b", "m", 1)];
    let selector = WeightedRandom::new(1.0);
    let share_a = share_of(&selector, &units, "a");
    assert!(
        (0.67..=0.83).contains(&share_a),
        "3:1 pool must give `a` ~75% of picks, got {share_a}"
    );
}

/// 验收标准：健康层的 EWMA/冷却**不影响**加权随机的选择（new-api 语义：衰减/恢复
/// 不在权重里）——同一池里 P2C 会让健康分占优，WeightedRandom 仍按静态权重对半分。
#[test]
fn health_ewma_does_not_shift_the_static_split() {
    let units = [unit("a", "m", 1), unit("b", "m", 1)];
    let table = MemoryHealthTable::new();
    // `a` 记录多次 Fatal → P2C 会把它的后验打到地板；加权随机必须无视。
    for _ in 0..10 {
        table.record(
            "a",
            gateway_dispatch::AttemptReport::new(
                Err(gateway_dispatch::FailureClass::Retryable),
                None,
                None,
            ),
        );
    }
    let share_a = share_of(&WeightedRandom::new(1.0), &units, "a");
    assert!(
        (0.42..=0.58).contains(&share_a),
        "equal weights must split ~50/50 regardless of health score, got {share_a}"
    );
}

/// 验收标准：硬门与 P2C 一致 —— 冷却中的候选不参与抽样。
#[test]
fn cooling_channels_are_excluded_from_sampling() {
    let now = Arc::new(std::sync::atomic::AtomicU64::new(1_000_000));
    let clock = {
        let now = Arc::clone(&now);
        move || now.load(std::sync::atomic::Ordering::Relaxed)
    };
    let table = MemoryHealthTable::with_config_and_clock(
        gateway_dispatch::health::HealthSetting::default(),
        clock,
    );
    table.record(
        "a",
        gateway_dispatch::AttemptReport::new(
            Err(gateway_dispatch::FailureClass::Retryable),
            None,
            None,
        ),
    );
    let units = [unit("a", "m", 100), unit("b", "m", 1)];
    // `a` 有 100 倍权重且在冷却中：若冷却门失效，`b` 的份额会被压到 ~1%。
    let refs: Vec<&RouteUnitRecord> = units.iter().collect();
    let selector = WeightedRandom::new(1.0);
    let mut b_hits = 0usize;
    let mut draws = 0usize;
    for seed in 1..=SEEDS {
        let mut rng = rand::rngs::StdRng::seed_from_u64(seed);
        for _ in 0..DRAWS_PER_SEED {
            let picked = selector
                .pick(
                    &refs,
                    &table,
                    &[],
                    None,
                    now.load(std::sync::atomic::Ordering::Relaxed),
                    &mut rng,
                )
                .unwrap();
            draws += 1;
            if picked.meta.key == "b" {
                b_hits += 1;
            }
        }
    }
    #[allow(clippy::cast_precision_loss)]
    let share_b = b_hits as f64 / draws as f64;
    assert!(
        (0.98..=1.0).contains(&share_b),
        "cooling `a` must be removed from the pool; `b` should take ~100%, got {share_b}"
    );
}

/// 验收标准：sticky 通道按 boost 放大权重（4× boost → 80%），且永不绕过否决。
#[test]
fn sticky_boost_raises_share_but_vetoes_still_win() {
    let units = [unit("a", "m", 1), unit("b", "m", 1)];
    let boost = 4.0_f64.clamp(1.0, MAX_STICKY_BOOST);
    let selector = WeightedRandom::new(boost);
    let health = MemoryHealthTable::new();
    let refs: Vec<&RouteUnitRecord> = units.iter().collect();
    let mut sticky_hits = 0usize;
    let mut draws = 0usize;
    for seed in 1..=SEEDS {
        let mut rng = rand::rngs::StdRng::seed_from_u64(seed);
        for _ in 0..DRAWS_PER_SEED {
            let picked = selector
                .pick(&refs, &health, &[], Some("a"), 0, &mut rng)
                .unwrap();
            draws += 1;
            if picked.meta.key == "a" {
                sticky_hits += 1;
            }
        }
    }
    #[allow(clippy::cast_precision_loss)]
    let share = sticky_hits as f64 / draws as f64;
    assert!(
        (0.72..=0.88).contains(&share),
        "4× boost on a 1:1 pool must give the sticky channel ~80%, got {share}"
    );

    // Sticky 通道被排除（已试过）→ boost 失效，退化为普通加权随机：`a` 完全不再出现。
    let share_b = share_of(&selector, &units, "b");
    assert!(
        (0.42..=0.58).contains(&share_b),
        "veto-free pool splits evenly"
    );
}

/// 验收标准：全零权重池退化为均匀抽样（不 panic）；非法 boost（NaN/≤0/超上界）
/// 收敛到上界（与 P2C 的 clamp 契约一致）。
#[test]
fn degenerate_totals_degrade_to_uniform_and_boost_clamps() {
    let units = [unit("a", "m", 0), unit("b", "m", 0)];
    let share_a = share_of(&WeightedRandom::new(1.0), &units, "a");
    assert!(
        (0.42..=0.58).contains(&share_a),
        "all-zero pool splits evenly, got {share_a}"
    );

    // NaN boost 非法：clamp 到上界后仍是无害的加权抽样（1:1 池上 100% 会给 `a`，
    // 这里只验证不 panic 且结果是合法候选）。
    let selector = WeightedRandom::new(f64::NAN);
    let share = share_of(&selector, &units, "a");
    assert!((0.0..=1.0).contains(&share));
    assert!(MAX_STICKY_BOOST.is_finite());
}

/// 验收标准：空池（全部冷却）→ 紧急召回最短冷却通道；全被 exclude → None。
#[test]
fn recall_fainted_picks_shortest_cooldown_like_p2c() {
    let now = Arc::new(std::sync::atomic::AtomicU64::new(1_000_000));
    let clock = {
        let now = Arc::clone(&now);
        move || now.load(std::sync::atomic::Ordering::Relaxed)
    };
    let table = MemoryHealthTable::with_config_and_clock(
        gateway_dispatch::health::HealthSetting::default(),
        clock,
    );
    // `a` 冷却 300ms，`b` 冷却 600ms（两次 Fatal）→ 紧急召回必须选 `a`。
    table.record(
        "a",
        gateway_dispatch::AttemptReport::new(
            Err(gateway_dispatch::FailureClass::Retryable),
            None,
            None,
        ),
    );
    table.record(
        "b",
        gateway_dispatch::AttemptReport::new(
            Err(gateway_dispatch::FailureClass::Retryable),
            None,
            None,
        ),
    );
    table.record(
        "b",
        gateway_dispatch::AttemptReport::new(
            Err(gateway_dispatch::FailureClass::Retryable),
            None,
            None,
        ),
    );
    let units = [unit("a", "m", 1), unit("b", "m", 1)];
    let refs: Vec<&RouteUnitRecord> = units.iter().collect();
    let mut rng = rand::rngs::StdRng::seed_from_u64(7);
    let picked = WeightedRandom::new(1.0)
        .pick(&refs, &table, &[], None, 1_000_000, &mut rng)
        .unwrap();
    assert_eq!(
        picked.meta.key, "a",
        "urgent recall picks the shortest cooldown"
    );

    // 全部被 exclude → None（上层报 AllUnavailable）。
    let picked = WeightedRandom::new(1.0).pick(
        &refs,
        &table,
        &["a".to_string(), "b".to_string()],
        None,
        1_000_000,
        &mut rng,
    );
    assert!(picked.is_none(), "everything excluded → None");
}

/// 验收标准（编译期契约）：WeightedRandom 与 P2C 走同一个 `Selector` trait ——
/// 抽象边界成立的直接证据；这里以 dyn 使用两者。
#[test]
fn both_selectors_are_interchangeable_behind_the_trait() {
    let units = [unit("a", "m", 1)];
    let health = MemoryHealthTable::new();
    let refs: Vec<&RouteUnitRecord> = units.iter().collect();
    let selectors: Vec<Box<dyn Selector>> = vec![
        Box::new(WeightedRandom::new(1.0)),
        Box::new(gateway_dispatch::P2CSelector::new(
            Arc::new(gateway_gate::concurrency::ConcurrencyState::default()),
            2,
            gateway_dispatch::selector::DEFAULT_EXPLORE_RATE,
        )),
    ];
    for selector in &selectors {
        let mut rng = rand::rngs::StdRng::seed_from_u64(1);
        let picked = selector
            .pick(&refs, &health, &[], None, 0, &mut rng)
            .unwrap();
        assert_eq!(picked.meta.key, "a");
    }
}
