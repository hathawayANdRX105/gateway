//! DTOs for the usage query endpoints.
//!
//! Mirrors the backend `crates/api-observe` wire shapes (the backend is authoritative):
//!
//! - [`UsageLogDto`]     ← the `LogView` subset carried by this contract (`items`
//!   element of GET /api/log and /api/log/self, camelCase)
//! - [`UsageLogPage`]    ← the endpoints' response body `{"items": [...], "total": n}`
//! - [`UsageLogQuery`]   ← `LogQuery` (query-string parameters, camelCase)
//! - [`UsageStatDto`]    ← `UsageStat` (GET /api/log/stat, /api/log/self/stat)
//! - [`UsageDailyStatDto`] per-day aggregation shape (contract placeholder for a
//!   future /api/log/daily endpoint)
//! - [`UsageTopRowDto`]  ← `UsageTopRow` (`items` element of GET /api/log/top)
//! - [`UsageTrendRowDto`] ← `UsageTrendRow` (`items` element of GET /api/log/trend;
//!   carried bucket by bucket as-is, also the raw material for the six metrics of
//!   that hour when the card is hovered)
//! - [`UsageErrorStatDto`] / [`UsageErrorStatPage`] ← GET /api/log/errors (error
//!   ledger aggregation)
//! - [`DashboardSummaryDto`] ← `json!` summary of GET /api/dashboard
//!
//! Dependency discipline (crate level): the contract layer allows only pure
//! data/serialization dependencies. Times are always carried as `String` in
//! RFC3339 (e.g. `"2026-09-08T17:55:28.579140Z"`), avoiding chrono's parse cost
//! and feature surface on the wasm side.

use serde::{Deserialize, Serialize};

/// Query parameters for GET /api/log and GET /api/log/self.
///
/// Aligns one for one with the backend `LogQuery` fields (camelCase). Every field is
/// optional: absent from the wire means no filtering. Serialization omits `None`
/// fields via [`serde::skip_serializing_if`], equivalent to "not carrying that
/// parameter" in the URL query; the backend parses it as `Option::None` and falls
/// back to defaults (`page` defaults to 1, `size` to 20 and is clamped to 1..=100).
///
/// Errors: `start`/`end` must be RFC3339 strings (e.g. `2026-09-08T00:00:00Z`); a
/// backend parse failure returns a 4xx error envelope.
#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UsageLogQuery {
    /// Log-type filter. `None` = no filtering. Values match new-api:
    /// 1=topup, 2=consume, 3=manage, 4=system.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub log_type: Option<i16>,
    /// Exact username match. `None` or an empty string = no filtering (the backend
    /// trims, then filters out empty strings).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub username: Option<String>,
    /// Exact API key name match. `None` or an empty string = no filtering.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub token_name: Option<String>,
    /// Exact model name match. `None` or an empty string = no filtering.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub model_name: Option<String>,
    /// Start of the time window, RFC3339 string, inclusive (`created_at >= start`).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub start: Option<String>,
    /// End of the time window, RFC3339 string, exclusive (`created_at < end`).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub end: Option<String>,
    /// 1-based page number. Backend defaults to 1 when `None`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub page: Option<i64>,
    /// Page size. Backend defaults to 20 when `None`; the backend clamps to 1..=100.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub size: Option<i64>,
}

/// `{"items": UsageLogDto[], "total": i64}` — response body of /api/log and
/// /api/log/self.
#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UsageLogPage {
    /// Log rows on the current page, ordered by `id DESC` (newest → oldest).
    pub items: Vec<UsageLogDto>,
    /// Total number of matches (not the current page length); i64 matches the backend
    /// `count(*)`.
    pub total: i64,
}

/// One usage-log wire row — the backend `LogView` fields this DTO carries (camelCase;
/// `group_id` / `status_code` are not included).
///
/// Source: the observe `usage_logs` flat table (single-node build). `id` comes from
/// the database BIGSERIAL sequence and is part of the composite `(id, created_at)`
/// primary key; the frontend may display it as a string or use it as a row key.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UsageLogDto {
    /// Record identifier from the PostgreSQL BIGSERIAL sequence (part of the composite
    /// `(id, created_at)` primary key).
    pub id: i64,
    /// Log type: 1=topup, 2=consume, 3=manage, 4=system (matches new-api).
    pub log_type: i16,
    /// UUID of the user that produced the record. Serialized as a string on the wire
    /// (a Uuid serialization product).
    pub user_key: String,
    /// Username. The backend defaults to an empty string (`''`); the field is never
    /// missing.
    pub username: String,
    /// API key name. Empty string for requests that did not use a key.
    pub token_name: String,
    /// Upstream channel name. Empty string when not attributed to a channel.
    pub channel_name: String,
    /// Model name.
    pub model_name: String,
    /// Input token count.
    pub prompt_tokens: i32,
    pub completion_tokens: i32,
    /// Cached read token count (migration 0024 `usage_logs.cached_tokens`, BIGINT;
    /// 0 when the upstream did not echo usage).
    pub cached_tokens: i64,
    /// Consumption in internal quota units. Conversion: 500_000 ≈ $1.
    pub quota: i64,
    /// Request duration (milliseconds).
    pub use_time_ms: i32,
    /// Time to first token (milliseconds). Migration 0026 `usage_logs.first_token_ms`,
    /// INT; non-streaming / failed observation rows / no upstream SSE event seen = 0.
    /// For the trend aggregation average, see
    /// `api_observe::logs::UsageTrendRow::ft_calls` (counts only rows > 0).
    /// `#[serde(default)]`: before 0026 the backend did not send this field; a missing
    /// field folds to 0 (= unknown; read-side averages use only rows > 0), so the whole
    /// log page response does not fail to parse.
    #[serde(default)]
    pub first_token_ms: i32,
    /// Whether the request was streaming.
    pub is_stream: bool,
    /// Client IP. Empty string when unknown.
    pub ip: String,
    /// Gateway-side request correlation ID.
    pub request_id: String,
    /// Upstream finish reason (migration 0024 `usage_logs.finish_reason`, TEXT NULL;
    /// the raw `choices[0].finish_reason` / `stop_reason` value. null = missing /
    /// unparseable / failed observation row, not a status enum).
    pub finish_reason: Option<String>,
    /// Record creation time, RFC3339 string (e.g.
    /// `"2026-09-08T17:55:28.579140Z"`). Carried as String in the contract layer to
    /// avoid pulling chrono into the wasm side.
    pub created_at: String,
}

/// Usage statistics — response body of GET /api/log/stat (all users) and
/// /api/log/self/stat (self).
///
/// Scope: `quota` / `requests` are **today's** (`date_trunc('day', now())` onward)
/// accumulation; `rpm` / `tpm` are a **last 60 seconds** sliding window.
#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UsageStatDto {
    /// Today's total consumption (internal quota units, 500_000 ≈ $1).
    pub quota: i64,
    /// Today's total request count.
    pub requests: i64,
    /// Request count over the last 60 seconds.
    pub rpm: i64,
    /// Token count over the last 60 seconds (prompt + completion).
    pub tpm: i64,
}

/// Usage statistics aggregated per day.
///
/// Contract placeholder: the backend observe aggregation query has not landed yet;
/// this DTO defines the wire shape ahead of time for the frontend's per-day charts.
/// Fields:
///
/// - `date`: per-day grouping key, format `YYYY-MM-DD` (backend local day; the
///   frontend can compare/sort as strings);
/// - `requests`: total requests that day;
/// - `tokens`: total tokens that day, defined as
///   `prompt_tokens + completion_tokens`;
/// - `quota`: that day's consumption (internal quota units, 500_000 ≈ $1).
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UsageDailyStatDto {
    /// Per-day grouping key, `YYYY-MM-DD`.
    pub date: String,
    /// Total requests that day.
    pub requests: i64,
    /// Total tokens that day (prompt + completion).
    pub tokens: i64,
    /// Consumption that day (internal quota units, 500_000 ≈ $1).
    pub quota: i64,
}

/// Planned response envelope for `GET /api/log/self/stat/daily` (the backend is to
/// emit `json!({ "items": [...] })` directly). One row = one day; intermediate dates
/// without data are absent (the backend does not zero-fill).
#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UsageDailyStatPage {
    pub items: Vec<UsageDailyStatDto>,
}

/// Dashboard summary — response body of GET /api/dashboard (the backend emits `json!`
/// directly, camelCase).
///
/// Same scope as [`UsageStatDto`]: `quota_today` / `requests_today` are today's
/// accumulation, `rpm` / `tpm` are the last-60-seconds window.
#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DashboardSummaryDto {
    /// Total user count.
    pub users: i64,
    /// Total API key count.
    pub tokens: i64,
    /// Total channel count.
    pub channels: i64,
    /// Number of enabled channels.
    pub channels_enabled: i64,
    /// Total group count.
    pub groups: i64,
    /// Today's consumption (internal quota units, 500_000 ≈ $1).
    pub quota_today: i64,
    /// Today's total request count.
    pub requests_today: i64,
    /// Request count over the last 60 seconds.
    pub rpm: i64,
    /// Token count over the last 60 seconds (prompt + completion).
    pub tpm: i64,
    /// Platform's remaining available quota (internal quota units, 500_000 ≈ $1).
    ///
    /// Scope: `SUM(quota) FROM auth_users WHERE status = 1` (enabled users only).
    /// Rationale: the login-side availability test is `status == 1` (the auth service
    /// only admits enabled users), so leftover balances of disabled/banned users can no
    /// longer be consumed; counting them would overstate the platform's spendable
    /// remainder. This matches the spirit of `channelsEnabled`, which counts only
    /// `status = 1` channels.
    /// `#[serde(default)]`: when an older backend omits the field, fold it to 0 without
    /// breaking the old wire.
    #[serde(default)]
    pub quota_remaining: i64,
    /// Data as-of time, ISO8601/RFC3339 UTC string (e.g. `2026-09-16T17:00:00Z`).
    /// Principle 7: query responses carry freshness instead of faking real time.
    /// `#[serde(default)]` as above.
    #[serde(default)]
    pub as_of: String,
}

/// One `GET /api/log/top` row — consumption aggregated by user or model
/// (backend observe `UsageTopRow`, camelCase).
///
/// `previous_tokens` is the same entity's aggregate over the **previous equal-length
/// window**: under the same `by`/`start`/`limit`, the total tokens inside
/// `[start-(end-start), start)` (`end` defaults to now). It is 0 when the entity is
/// absent from the previous window or when the request omits `start` (window not
/// determinable).
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UsageTopRowDto {
    /// Grouping key: username when `by=user`, model_name when `by=model`.
    pub name: String,
    /// Total tokens in the window (prompt + completion).
    pub tokens: i64,
    /// Consumption in the window (internal quota units, 500_000 ≈ $1).
    pub quota: i64,
    /// Number of calls in the window.
    pub calls: i64,
    /// Same entity's tokens in the previous equal-length window; 0 when there is no
    /// data or the window is not determinable. `#[serde(default)]`: fold to 0 when an
    /// older backend omits the field.
    #[serde(default)]
    pub previous_tokens: i64,
}

/// One `GET /api/log/trend` row — usage aggregated by time bucket × model
/// (backend observe `UsageTrendRow`, camelCase).
///
/// Bucket axis: the backend buckets by `date_trunc(granularity)` (the hourly tier is
/// whole-hour buckets); `bucket` is the bucket start time in RFC3339. The frontend
/// folds the buckets inside the request window into a fixed 24 slots (one hour per
/// slot, last slot = the whole hour at the window end) for line/heat axes.
///
/// Each bucket is also the raw material for the "six metrics of that hour"
/// (2026-10-13 maintainer note: when hovering a line/heat point, the card's six
/// metrics show that hour's values):
/// `calls` → req, `tokens` → tok, `latencyMs ÷ calls` → lat,
/// `tokens ÷ (latencyMs ÷ 1000)` → tps, `firstTokenMs ÷ ftCalls` → ft.
/// **Success rate has no per-bucket material**: `/api/log/errors` only gives a
/// window-level `count` (see [`UsageErrorStatDto`]); per-bucket sr needs the backend
/// to aggregate error counts by bucket too, at which point a field is added here or
/// on the error aggregation side — until then the frontend draws per-bucket sr as
/// "—", never passing off a window value as the bucket value.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UsageTrendRowDto {
    /// Bucket start time, RFC3339 UTC string (e.g. `"2026-09-08T17:00:00Z"`).
    pub bucket: String,
    /// Model name.
    pub model_name: String,
    /// Total tokens in the bucket (prompt + completion).
    pub tokens: i64,
    /// Consumption in the bucket (internal quota units, 500_000 ≈ $1).
    pub quota: i64,
    /// Number of calls in the bucket.
    pub calls: i64,
    /// **Sum** of `use_time_ms` in the bucket (consume rows, `log_type = 2`, only).
    ///
    /// The sum is stored rather than the mean: the mean can only be reconstructed as
    /// "total milliseconds ÷ total calls"; averaging pre-averaged values would be
    /// skewed by small buckets. `#[serde(default)]` backstops older backends (a
    /// missing field folds to 0 = unknown, degraded).
    #[serde(default)]
    pub latency_ms: i64,
    /// Sum of `first_token_ms` in the bucket (migration 0026 column; only rows > 0
    /// count).
    #[serde(default)]
    pub first_token_ms: i64,
    /// Number of rows in the bucket **with a first-token timing** (denominator for the
    /// average first-token latency: 0 from non-streaming / failed rows must not enter
    /// the denominator, or the mean would be diluted to a fake low).
    #[serde(default)]
    pub ft_calls: i64,
}

/// One error-ledger aggregation row — an `items` element of `GET /api/log/errors`
/// (backend observe `UsageErrorRow`, camelCase).
///
/// Scope: `usage_logs.log_type = 5` (error ledger) grouped by `model_name` inside the
/// window, ordered by `count` descending.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UsageErrorStatDto {
    /// Model name (error rows with an empty model name are excluded from aggregation).
    pub model_name: String,
    /// Number of errors for this model in the window.
    pub count: i64,
    /// Time of the last error in the window, RFC3339 UTC string.
    pub last_seen_at: String,
}

/// Response envelope of `GET /api/log/errors` (the backend emits `json!` directly):
/// `{"items": [...], "asOf": "..."}`.
#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UsageErrorStatPage {
    /// Aggregation rows, ordered by `count` descending.
    pub items: Vec<UsageErrorStatDto>,
    /// Data as-of time, ISO8601/RFC3339 UTC string. `#[serde(default)]` backstops the
    /// old wire.
    #[serde(default)]
    pub as_of: String,
}
