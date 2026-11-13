//! Behavioral tests for the data-driven dispatch filters: priority tiers
//! (`settings.priority`) and context fit (`settings.context_window`), on top of
//! the pluggable `Selector` slot.
//!
//! Pinned semantics:
//! - Priority tiers (new-api model): only the highest-priority tier with an
//!   **eligible** candidate competes; lower tiers become reachable automatically
//!   when every higher-tier candidate is excluded or cooling — tier descent is
//!   the filter re-running, not a dedicated code path.
//! - Sticky does not cross tiers: a bound channel in a lower tier is dropped
//!   like any other lower-tier candidate (priority dominates stickiness).
//! - Context fit is feasibility and runs before preference: a window that cannot
//!   hold prompt + output budget is dropped; channels without a window are
//!   unlimited (neutral). Overflow escalation raises the floor, so the next
//!   selection can only land on strictly larger windows.

use gateway_core::dispatch::SelectorKind;
use gateway_core::records::{
    ChannelKey, ChannelRecord, ChannelSettings, RouteUnitRecord, SyncMeta,
};
use gateway_dispatch::Dispatch;
use std::collections::HashMap;
use std::sync::Arc;

fn meta(key: &str) -> SyncMeta {
    SyncMeta {
        key: key.to_string(),
        schema_version: 1,
        logical_version: 1,
        origin: "test".to_string(),
        updated_at: chrono::Utc::now(),
    }
}

fn unit(group: &str, model: &str, key: &str, weight: u32) -> RouteUnitRecord {
    RouteUnitRecord {
        meta: meta(key),
        group: group.to_string(),
        public_model: model.to_string(),
        channel_key: key.to_string(),
        key_index: 0,
        upstream_model: model.to_string(),
        weight,
        status: 1,
    }
}

fn channel(key: &str, settings: ChannelSettings) -> ChannelRecord {
    ChannelRecord {
        meta: meta(key),
        name: key.to_string(),
        provider_type: "openai".to_string(),
        base_url: format!("https://{key}.invalid"),
        keys: vec![ChannelKey {
            index: 0,
            secret: "sk-test".to_string(),
            rpm_limit: 0,
        }],
        max_concurrency: 10,
        status: 1,
        groups: vec!["default".to_string()],
        settings,
    }
}

/// Assemble a dispatcher from units + channels with the given strategy.
fn dispatcher(
    units: Vec<RouteUnitRecord>,
    channels: &[ChannelRecord],
    kind: SelectorKind,
) -> gateway_dispatch::Dispatcher {
    let mut channel_map = HashMap::with_capacity(channels.len());
    for c in channels {
        channel_map.insert(c.meta.key.clone(), c.clone());
    }
    let snapshot = gateway_core::dispatch::Snapshot {
        units,
        channels: channel_map,
    };
    gateway_dispatch::Dispatcher::new(
        Some(Arc::new(snapshot)),
        Arc::new(gateway_dispatch::health::MemoryHealthTable::new()),
        Arc::new(gateway_gate::concurrency::ConcurrencyState::default()),
    )
    .with_selector(match kind {
        SelectorKind::WeightedRandom => Box::new(gateway_dispatch::WeightedRandom::new(1.0)),
        SelectorKind::P2c => Box::new(gateway_dispatch::P2CSelector::new(
            Arc::new(gateway_gate::concurrency::ConcurrencyState::default()),
            2,
            gateway_dispatch::selector::DEFAULT_EXPLORE_RATE,
        )),
    })
}

#[allow(clippy::cast_precision_loss)]
fn count_picks<F: FnMut() -> Option<String>>(picks: &mut F, key: &str, rounds: usize) -> f64 {
    let mut hits = 0usize;
    let mut total = 0usize;
    for _ in 0..rounds {
        if let Some(k) = picks() {
            total += 1;
            if k == key {
                hits += 1;
            }
        }
    }
    assert_eq!(total, rounds, "every round must produce a pick");
    hits as f64 / rounds as f64
}

/// 验收标准：高优先级通道在场时低优先级通道永不入选（priority 数据决定偏好）。
#[test]
fn priority_tiers_prefer_the_higher_tier() {
    // ch-hi: priority 10, weight 1；ch-lo: priority 0, weight 100 —— 权重不能跨层。
    let units = vec![unit("g", "m", "ch-hi", 1), unit("g", "m", "ch-lo", 100)];
    let channels = vec![
        channel(
            "ch-hi",
            ChannelSettings {
                priority: 10,
                ..Default::default()
            },
        ),
        channel("ch-lo", ChannelSettings::default()),
    ];
    for kind in [SelectorKind::P2c, SelectorKind::WeightedRandom] {
        let d = dispatcher(units.clone(), &channels, kind);
        let share = count_picks(
            &mut || {
                d.select("g", "m", &[], None, None)
                    .ok()
                    .map(|c| c.unit.channel_key)
            },
            "ch-hi",
            50,
        );
        assert_eq!(
            share, 1.0,
            "{kind:?}: the higher tier must take every pick while eligible"
        );
    }
}

/// 验收标准：高层候选失败被 exclude 后自动降级到低层（层间回退无需专用代码，
/// 过滤器随 exclude 重算）；低层失败再回高层不受影响。
#[test]
fn tier_descends_when_the_higher_tier_is_excluded() {
    let units = vec![unit("g", "m", "ch-hi", 1), unit("g", "m", "ch-lo", 1)];
    let channels = [
        channel(
            "ch-hi",
            ChannelSettings {
                priority: 10,
                ..Default::default()
            },
        ),
        channel("ch-lo", ChannelSettings::default()),
    ];
    let d = dispatcher(units, &channels, SelectorKind::WeightedRandom);
    let exclude = vec!["ch-hi".to_string()];
    let picked = d.select("g", "m", &exclude, None, None).unwrap();
    assert_eq!(
        picked.unit.channel_key, "ch-lo",
        "excluding the top tier must expose the lower tier"
    );
    // 无 exclude 时回到高层。
    let picked = d.select("g", "m", &[], None, None).unwrap();
    assert_eq!(picked.unit.channel_key, "ch-hi");
}

/// 验收标准：高层全冷却同样降级（与 P2C 冷却语义组合），冷却恢复后回高层。
#[test]
fn tier_descends_when_the_higher_tier_cools_down() {
    let units = [unit("g", "m", "ch-hi", 1), unit("g", "m", "ch-lo", 1)];
    let now = Arc::new(std::sync::atomic::AtomicU64::new(1_000_000));
    let channels = [
        channel(
            "ch-hi",
            ChannelSettings {
                priority: 10,
                ..Default::default()
            },
        ),
        channel("ch-lo", ChannelSettings::default()),
    ];
    let clock = {
        let now = Arc::clone(&now);
        move || now.load(std::sync::atomic::Ordering::Relaxed)
    };
    let clock_for_constructor = clock.clone();
    // 用注入时钟的 dispatcher：health 表的冷却判定与 now 对齐。
    let d = gateway_dispatch::Dispatcher::with_limits_and_clock(
        Some(Arc::new(gateway_core::dispatch::Snapshot {
            units: units.to_vec(),
            channels: channels
                .iter()
                .map(|c| (c.meta.key.clone(), c.clone()))
                .collect(),
        })),
        Arc::new(
            gateway_dispatch::health::MemoryHealthTable::with_config_and_clock(
                gateway_dispatch::health::HealthSetting::default(),
                clock_for_constructor,
            ),
        ),
        Arc::new(gateway_gate::concurrency::ConcurrencyState::default()),
        Arc::new(HashMap::new()),
        Arc::new(gateway_dispatch::ratelimit::SlidingWindow::new()),
        clock,
    )
    .with_selector(Box::new(gateway_dispatch::WeightedRandom::new(1.0)));
    // 高层 Fatal → 冷却 300ms。
    d.report(
        "ch-hi",
        gateway_dispatch::AttemptReport::new(
            Err(gateway_dispatch::FailureClass::Retryable),
            None,
            None,
        ),
    );
    let picked = d.select("g", "m", &[], None, None).unwrap();
    assert_eq!(
        picked.unit.channel_key, "ch-lo",
        "cooling top tier must expose the lower tier"
    );
    // 冷却结束（now 前进 300ms）→ 回高层。
    now.store(1_000_301, std::sync::atomic::Ordering::Relaxed);
    let picked = d.select("g", "m", &[], None, None).unwrap();
    assert_eq!(picked.unit.channel_key, "ch-hi");
}

/// 验收标准：`context fit` 是可行性前置过滤 —— 窗口装不下 `prompt+max_tokens` 的
/// 渠道被丢弃；无窗口（`unlimited`）渠道中性。两种策略行为一致。
#[test]
fn context_fit_drops_channels_that_cannot_hold_the_request() {
    // ch-tiny: 窗口 1000；ch-big: 窗口 100_000。请求需要 50_000。
    let units = vec![unit("g", "m", "ch-tiny", 100), unit("g", "m", "ch-big", 1)];
    let channels = vec![
        channel(
            "ch-tiny",
            ChannelSettings {
                context_window: 1000,
                ..Default::default()
            },
        ),
        channel(
            "ch-big",
            ChannelSettings {
                context_window: 100_000,
                ..Default::default()
            },
        ),
    ];
    let required = Some(50_000_u64);
    for kind in [SelectorKind::P2c, SelectorKind::WeightedRandom] {
        let d = dispatcher(units.clone(), &channels, kind);
        let share = count_picks(
            &mut || {
                d.select("g", "m", &[], None, required)
                    .ok()
                    .map(|c| c.unit.channel_key)
            },
            "ch-big",
            30,
        );
        assert_eq!(
            share, 1.0,
            "{kind:?}: the unfit tiny window must be dropped before preference"
        );
    }
}

/// 验收标准：无窗口（unlimited）渠道中性——始终视为可容纳。
#[test]
fn channels_without_a_window_stay_neutral() {
    let units = vec![unit("g", "m", "ch-unlimited", 1)];
    let channels = vec![channel("ch-unlimited", ChannelSettings::default())];
    let d = dispatcher(units, &channels, SelectorKind::WeightedRandom);
    let picked = d.select("g", "m", &[], None, Some(999_999)).unwrap();
    assert_eq!(picked.unit.channel_key, "ch-unlimited");
}

/// 验收标准（review finding 5b）：priority 分层 × 限流循环交互 —— 高层被限流
/// 拒绝后，循环内重算分层自动暴露低层（同一轮 select 内不报 429）。
#[test]
fn rate_limit_refusals_expose_the_lower_tier() {
    let units = [unit("g", "m", "ch-hi", 1), unit("g", "m", "ch-lo", 1)];
    let channels = [
        channel(
            "ch-hi",
            ChannelSettings {
                priority: 10,
                ..Default::default()
            },
        ),
        channel("ch-lo", ChannelSettings::default()),
    ];
    let now = Arc::new(std::sync::atomic::AtomicU64::new(1_000_000));
    let clock = {
        let now = Arc::clone(&now);
        move || now.load(std::sync::atomic::Ordering::Relaxed)
    };
    let clock2 = clock.clone();
    let d = gateway_dispatch::Dispatcher::with_limits_and_clock(
        Some(Arc::new(gateway_core::dispatch::Snapshot {
            units: units.to_vec(),
            channels: channels
                .iter()
                .map(|c| (c.meta.key.clone(), c.clone()))
                .collect(),
        })),
        Arc::new(gateway_dispatch::health::MemoryHealthTable::new()),
        Arc::new(gateway_gate::concurrency::ConcurrencyState::default()),
        Arc::new(HashMap::from([(
            "ch-hi".to_string(),
            gateway_dispatch::ratelimit::RateLimitSpec {
                requests: 1,
                window_ms: 60_000,
            },
        )])),
        Arc::new(gateway_dispatch::ratelimit::SlidingWindow::with_clock(
            clock2,
        )),
        clock,
    )
    .with_selector(Box::new(gateway_dispatch::WeightedRandom::new(1.0)));
    // 第一次 select：抽中高层 ch-hi 并消耗其唯一速率名额。
    let first = d.select("g", "m", &[], None, None).unwrap();
    assert_eq!(first.unit.channel_key, "ch-hi");
    // 第二次 select：ch-hi 被限流拒绝 → pick_within_limits 循环内重算分层 →
    // 落到低层 ch-lo（不报 429：低层仍在预算内）。
    let second = d.select("g", "m", &[], None, None).unwrap();
    assert_eq!(
        second.unit.channel_key, "ch-lo",
        "a rate-limited top tier must expose the lower tier inside the same select"
    );
}

/// 验收标准（review finding 1 分类器）：`is_context_overflow` 识别 context-window
/// 溢出族（400/413 + provider 措辞），非 400/413 或无关措辞不误判。
#[test]
fn context_overflow_classifier_covers_provider_phrasings() {
    use gateway_dispatch::failure_scope::is_context_overflow;
    assert!(is_context_overflow(
        400,
        "{\"error\":{\"code\":\"context_length_exceeded\"}}"
    ));
    assert!(is_context_overflow(
        400,
        "This model's maximum context length is 8192 tokens"
    ));
    assert!(is_context_overflow(
        400,
        "messages: prompt is too long: 200000 tokens > 128000"
    ));
    assert!(is_context_overflow(
        413,
        "exceeds the maximum number of tokens"
    ));
    assert!(!is_context_overflow(500, "internal server error"));
    assert!(!is_context_overflow(400, "invalid api key format"));
}

/// 验收标准：溢出升级方向 —— floor 抬到失败窗口之上后，只能落在严格更大的
/// 窗口；全部放不下时 AllUnavailable（客户端不透传 context 错误）。
#[test]
fn raising_the_floor_escalates_to_strictly_larger_windows() {
    // 三层：1k / 10k / 100k。请求需要 50k：1k 在 dispatch 即被过滤；10k 装得下
    // （预过滤只看估算），但真实溢出后 floor 抬到 10_001 → 只剩 100k。
    let units = vec![
        unit("g", "m", "ch-1k", 100),
        unit("g", "m", "ch-10k", 50),
        unit("g", "m", "ch-100k", 1),
    ];
    let channels = vec![
        channel(
            "ch-1k",
            ChannelSettings {
                context_window: 1_000,
                ..Default::default()
            },
        ),
        channel(
            "ch-10k",
            ChannelSettings {
                context_window: 10_000,
                ..Default::default()
            },
        ),
        channel(
            "ch-100k",
            ChannelSettings {
                context_window: 100_000,
                ..Default::default()
            },
        ),
    ];
    // 预过滤：需要 5k → 1k 被丢，10k 与 100k 按 priority（默认 0，同层）竞争。
    let d = dispatcher(units, &channels, SelectorKind::WeightedRandom);
    for _ in 0..10 {
        let picked = d.select("g", "m", &[], None, Some(5_000)).unwrap();
        assert_ne!(
            picked.unit.channel_key, "ch-1k",
            "the 1k window must never serve a 5k request"
        );
    }
    // floor 抬到 10_001（10k 渠道溢出）→ 只剩 100k。
    let picked = d.select("g", "m", &[], None, Some(10_001)).unwrap();
    assert_eq!(picked.unit.channel_key, "ch-100k");
    // floor 再抬过 100k → 无候选可用 → AllUnavailable（不透传 context 错误）。
    let err = d.select("g", "m", &[], None, Some(100_001)).unwrap_err();
    assert!(
        matches!(err, gateway_dispatch::DispatchError::AllUnavailable { .. }),
        "every tier overflowed → AllUnavailable, got {err:?}"
    );
}
