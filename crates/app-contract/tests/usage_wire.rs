//! Verifies that the usage-domain contract DTOs stay wire-compatible with the backend observe
//! shapes.
//!
//! The backend is authoritative: `LogView` / `LogQuery` / `UsageStat` all carry
//! `#[serde(rename_all = "camelCase")]`, and timestamps appear on the wire as RFC3339 strings.
//! The contract [`app_contract::api::usage`] defines the same shape (timestamps use `String`,
//! avoiding a chrono dependency). The test pins this compatibility with camelCase wire-sample
//! JSON: decoding asserts field types and semantics; serialization asserts query parameter names.

use app_contract::api::usage::{
    DashboardSummaryDto, UsageDailyStatDto, UsageErrorStatPage, UsageLogDto, UsageLogPage,
    UsageLogQuery, UsageStatDto, UsageTopRowDto,
};

/// Wire sample for the `GET /api/log` response body `{"items": [LogView...], "total": n}`.
/// camelCase; `userKey` is a UUID string and `createdAt` is RFC3339.
fn real_log_page_body() -> &'static str {
    r#"{"items":[{"id":42,"logType":2,"userKey":"d9334bb6-1652-4e00-ade8-75fe5d5fbc45","username":"probe_user","tokenName":"work","channelName":"chan-a","modelName":"gpt-4o","promptTokens":128,"completionTokens":64,"cachedTokens":16,"quota":95000,"useTimeMs":812,"firstTokenMs":137,"isStream":true,"ip":"127.0.0.1","requestId":"req-0001","finishReason":"stop","createdAt":"2026-09-08T17:55:28.579140Z"}],"total":1}"#
}

#[test]
fn usage_log_dto_parses_backend_wire_shape() {
    let page: UsageLogPage = serde_json::from_str(real_log_page_body())
        .expect("后端真实 log 页响应必须能被 UsageLogPage 解析");

    assert_eq!(page.total, 1, "total 为 i64 命中总数");
    // Element type is spelled out: items must be exactly UsageLogDto.
    let row: &UsageLogDto = &page.items[0];
    assert_eq!(row.id, 42, "id 为数据库 BIGSERIAL 主键 (i64)");
    assert_eq!(row.log_type, 2, "logType=2 即 consume (i16)");
    assert_eq!(row.user_key, "d9334bb6-1652-4e00-ade8-75fe5d5fbc45");
    assert_eq!(row.username, "probe_user");
    assert_eq!(row.token_name, "work");
    assert_eq!(row.channel_name, "chan-a");
    assert_eq!(row.model_name, "gpt-4o");
    assert_eq!(row.prompt_tokens, 128, "promptTokens 为 i32");
    assert_eq!(row.completion_tokens, 64, "completionTokens 为 i32");
    assert_eq!(row.cached_tokens, 16, "cachedTokens 为 i64（0024 质量列）");
    assert_eq!(
        row.finish_reason.as_deref(),
        Some("stop"),
        "finishReason 为 TEXT NULL 列的可选字符串"
    );
    assert_eq!(row.quota, 95_000, "quota 为 i64 内部额度单位 (500_000≈$1)");
    assert_eq!(row.use_time_ms, 812, "useTimeMs 为 i32 毫秒");
    assert_eq!(
        row.first_token_ms, 137,
        "firstTokenMs 为迁移 0026 的首 token 延迟 (i32)"
    );
    assert!(row.is_stream, "isStream 布尔位");
    assert_eq!(row.ip, "127.0.0.1");
    assert_eq!(row.request_id, "req-0001");
    assert_eq!(
        row.created_at, "2026-09-08T17:55:28.579140Z",
        "createdAt 为 RFC3339 字符串"
    );
}

#[test]
fn usage_stat_dto_parses_wire_and_defaults_to_zero() {
    // Wire shape of the backend UsageStat: four i64 values, all single-word keys under camelCase.
    let stat: UsageStatDto =
        serde_json::from_str(r#"{"quota":5000000,"requests":10,"rpm":3,"tpm":2048}"#)
            .expect("后端 stat 响应必须能被 UsageStatDto 解析");
    assert_eq!(stat.quota, 5_000_000);
    assert_eq!(stat.requests, 10);
    assert_eq!(stat.rpm, 3, "rpm 为近 60s 请求数");
    assert_eq!(stat.tpm, 2048, "tpm 为近 60s token 数");

    let empty = UsageStatDto::default();
    assert_eq!(
        empty,
        UsageStatDto {
            quota: 0,
            requests: 0,
            rpm: 0,
            tpm: 0
        },
        "Default 全零"
    );
}

#[test]
fn usage_daily_stat_dto_parses_wire_shape() {
    let row: UsageDailyStatDto = serde_json::from_str(
        r#"{"date":"2026-09-08","requests":12,"tokens":3456,"quota":7000000}"#,
    )
    .expect("按天聚合 wire 行必须能被 UsageDailyStatDto 解析");
    assert_eq!(row.date, "2026-09-08", "date 为 YYYY-MM-DD 分组键");
    assert_eq!(row.requests, 12);
    assert_eq!(row.tokens, 3456, "tokens = prompt + completion");
    assert_eq!(row.quota, 7_000_000, "quota 为内部额度单位");
}

#[test]
fn usage_log_query_serializes_camel_case_param_names() {
    // Serialize with every field populated; the key names must map one-to-one to the backend
    // LogQuery camelCase query parameters.
    let q = UsageLogQuery {
        log_type: Some(2),
        username: Some("probe_user".into()),
        token_name: Some("work".into()),
        model_name: Some("gpt-4o".into()),
        start: Some("2026-09-08T00:00:00Z".into()),
        end: Some("2026-09-09T00:00:00Z".into()),
        page: Some(1),
        size: Some(20),
    };
    let json: serde_json::Value = serde_json::to_value(&q).expect("query 必须可序列化");
    assert_eq!(
        json,
        serde_json::json!({
            "logType": 2,
            "username": "probe_user",
            "tokenName": "work",
            "modelName": "gpt-4o",
            "start": "2026-09-08T00:00:00Z",
            "end": "2026-09-09T00:00:00Z",
            "page": 1,
            "size": 20
        }),
        "序列化键名须为后端认识的 camelCase 参数名"
    );
}

#[test]
fn usage_log_query_omits_unset_filters() {
    // Unset filters produce no key — equivalent to omitting the parameter from the URL query,
    // where the backend parses Option as None and applies the default (page=1, size=20).
    let q = UsageLogQuery {
        page: Some(2),
        size: Some(50),
        ..Default::default()
    };
    let json: serde_json::Value = serde_json::to_value(&q).expect("query 必须可序列化");
    assert_eq!(
        json,
        serde_json::json!({ "page": 2, "size": 50 }),
        "None 字段被省略, 不发出 null 参数"
    );
}

#[test]
fn usage_log_query_deserializes_camel_case_subset() {
    // A subset of backend query parameters (keys and values as they appear after URL decoding)
    // must deserialize back into UsageLogQuery, with absent keys staying None — equivalent to
    // omitting the parameter from the URL.
    let json = serde_json::json!({
        "logType": 2, "modelName": "gpt-4o", "start": "2026-09-08T00:00:00Z", "page": 1, "size": 20
    });
    let q: UsageLogQuery =
        serde_json::from_value(json).expect("camelCase 键须能解回 UsageLogQuery");
    assert_eq!(
        q,
        UsageLogQuery {
            log_type: Some(2),
            model_name: Some("gpt-4o".into()),
            start: Some("2026-09-08T00:00:00Z".into()),
            page: Some(1),
            size: Some(20),
            ..Default::default()
        }
    );
    assert!(q.username.is_none() && q.token_name.is_none() && q.end.is_none());
}

#[test]
fn dashboard_summary_dto_defaults_for_new_fields() {
    // What it tests: the new DashboardSummaryDto fields quotaRemaining/asOf must carry serde
    // default.
    // Why: the backend is authoritative on the wire and rolls out before the frontend/contract —
    // when an old backend response lacks these two keys, the new contract must not fail to parse
    // (missing field → 0/empty-string fallback).
    let old: DashboardSummaryDto = serde_json::from_str(
        r#"{"users":3,"tokens":7,"channels":5,"channelsEnabled":4,"groups":2,
            "quotaToday":10,"requestsToday":5,"rpm":1,"tpm":9}"#,
    )
    .expect("老 wire（无新字段）必须能被 DashboardSummaryDto 解析");
    assert_eq!(old.quota_remaining, 0, "缺 quotaRemaining 按默认 0 兜底");
    assert_eq!(old.as_of, "", "缺 asOf 按默认空串兜底");
    assert_eq!(old.users, 3, "既有字段不受新字段影响");

    // New wire: all fields present, matched by camelCase.
    let new: DashboardSummaryDto = serde_json::from_str(
        r#"{"users":3,"tokens":7,"channels":5,"channelsEnabled":4,"groups":2,
            "quotaToday":10,"requestsToday":5,"rpm":1,"tpm":9,
            "quotaRemaining":123456,"asOf":"2026-09-16T17:00:00Z"}"#,
    )
    .expect("新 wire（含 quotaRemaining/asOf）必须能被解析");
    assert_eq!(new.quota_remaining, 123_456);
    assert_eq!(new.as_of, "2026-09-16T17:00:00Z");
}

#[test]
fn usage_top_row_dto_defaults_previous_tokens() {
    // What it tests: previousTokens on the /api/log/top row DTO must carry serde default.
    // Why: old backend top-response rows have no previousTokens key, and the upgraded frontend
    // must still parse them.
    let old: UsageTopRowDto =
        serde_json::from_str(r#"{"name":"gpt-4o","tokens":150,"quota":300,"calls":2}"#)
            .expect("老 wire（无 previousTokens）必须能被 UsageTopRowDto 解析");
    assert_eq!(old.name, "gpt-4o");
    assert_eq!(old.tokens, 150);
    assert_eq!(old.quota, 300);
    assert_eq!(old.calls, 2);
    assert_eq!(old.previous_tokens, 0, "缺 previousTokens 按默认 0 兜底");

    let new: UsageTopRowDto = serde_json::from_str(
        r#"{"name":"gpt-4o","tokens":150,"quota":300,"calls":2,"previousTokens":80}"#,
    )
    .expect("新 wire（含 previousTokens）必须能被解析");
    assert_eq!(new.previous_tokens, 80);
}

#[test]
fn usage_error_stat_page_parses_wire_shape() {
    // What it tests: the camelCase shape of the /api/log/errors envelope
    // {"items":[...],"asOf":"..."}.
    // Why: the backend emits camelCase directly from json! (modelName/lastSeenAt/asOf), so the
    // contract must align key by key; row semantics are log_type=5 aggregated per model
    // (count descending).
    let page: UsageErrorStatPage = serde_json::from_str(
        r#"{"items":[
             {"modelName":"gpt-4o","count":12,"lastSeenAt":"2026-09-16T16:59:00Z"},
             {"modelName":"claude-x","count":3,"lastSeenAt":"2026-09-16T16:40:00Z"}
           ],
           "asOf":"2026-09-16T17:00:00Z"}"#,
    )
    .expect("errors 信封 wire 形状必须能被 UsageErrorStatPage 解析");
    assert_eq!(page.items.len(), 2, "两行聚合结果");
    assert_eq!(page.items[0].model_name, "gpt-4o");
    assert_eq!(page.items[0].count, 12);
    assert_eq!(page.items[0].last_seen_at, "2026-09-16T16:59:00Z");
    assert_eq!(page.items[1].model_name, "claude-x");
    assert_eq!(page.as_of, "2026-09-16T17:00:00Z");

    // Empty window (no errors): an empty items array must also be valid.
    let empty: UsageErrorStatPage =
        serde_json::from_str(r#"{"items":[],"asOf":"2026-09-16T17:00:00Z"}"#)
            .expect("空 items 信封必须能解析");
    assert!(empty.items.is_empty());
}
