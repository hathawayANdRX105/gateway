//! Billing endpoint DTOs — console APIs for subscriptions, redemption, and model aliases.
//!
//! Reference: new-api /api/alias | /api/subscription | /api/redemption
//! plus the corresponding record definitions in app_contract::records::billing.

use crate::records::billing::{RedeemCodeRecord, SubscriptionPlanRecord};
use serde::{Deserialize, Serialize};

/// Alias DTO — mirrors the admin-api /api/alias response.
#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AliasDto {
    pub key: String,
    pub name: String,
    pub display_name: Option<String>,
    pub input_per_1k: Option<f64>,
    pub output_per_1k: Option<f64>,
    pub multiplier: Option<f64>,
    pub status: u8,
    pub created_at: Option<String>,
}

/// Group reference matched by a model — the element type of `ModelView.groups` in
/// `/api/models` responses.
///
/// The canonical rules live in `crates/api-control-plane/src/models.rs`
/// (`whitelist_matches` / `match_groups`): an empty group whitelist array means unrestricted
/// (matches all models); otherwise any matching `*` wildcard pattern counts as a match.
/// Only enabled groups (`status=1`) participate. The backend emits them sorted by `name`
/// ascending, and consumers do not re-sort; no matches yields an empty array, not null.
#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ModelGroupDto {
    /// Group name (`api_groups.name`, the same name used by the settlement group reference).
    pub name: String,
    /// Group-level billing multiplier (`api_groups.ratio`): settlement cost × ratio, not a
    /// "model × group" dimension.
    pub ratio: f64,
}

/// Subscription plan DTO — mirrors the admin-api /api/subscription response.
#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SubscriptionDto {
    pub id: Option<u32>,
    pub name: String,
    pub description: Option<String>,
    pub price: Option<f64>,
    pub quota: Option<f64>,
    pub currency_price: Option<f64>,
    pub payment_method: Option<String>,
    pub group: Option<String>,
    pub downgrade_group: Option<String>,
    pub period_val: Option<u32>,
    pub period_unit: Option<String>,
    pub reset_cycle: Option<String>,
    pub priority: Option<u32>,
    pub enabled: Option<bool>,
    pub allow_redeem: Option<bool>,
    pub allow_wallet: Option<bool>,
    pub max_per_user: Option<u32>,
    pub sort_order: Option<u32>,
    pub stripe_price_id: Option<String>,
    pub creem_product_id: Option<String>,
    pub waffo_product_id: Option<String>,
}

/// Redemption code DTO — mirrors the admin-api /api/redemption response.
#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RedemptionDto {
    pub name: String,
    pub key: String,
    pub quota: Option<f64>,
    pub status: Option<i16>,
    pub created_at: Option<String>,
    pub expires_at: Option<String>,
}

impl From<&SubscriptionPlanRecord> for SubscriptionDto {
    fn from(r: &SubscriptionPlanRecord) -> Self {
        Self {
            id: None,
            name: r.name.clone(),
            description: None,
            price: r.price.parse().ok(),
            // Record-layer quota is in i64 internal units (500_000 = $1); the DTO uses
            // display-layer units, so convert at the boundary (see the
            // SubscriptionUpsertRequest docs). Same formula as billing's
            // From<SubscriptionRow>; both paths produce identical output.
            quota: Some(r.quota as f64 / 500_000.0),
            currency_price: None,
            payment_method: None,
            group: r.upgrade_group.clone(),
            downgrade_group: None,
            period_val: Some(r.duration_days),
            period_unit: Some("days".into()),
            reset_cycle: None,
            priority: None,
            enabled: Some(r.enabled),
            allow_redeem: None,
            allow_wallet: None,
            max_per_user: r.max_purchases,
            sort_order: None,
            stripe_price_id: None,
            creem_product_id: None,
            waffo_product_id: None,
        }
    }
}

impl From<&RedeemCodeRecord> for RedemptionDto {
    fn from(r: &RedeemCodeRecord) -> Self {
        Self {
            name: r.batch.clone(),
            key: r.code_hash.clone(), // This is a hash; frontend display may need separate handling
            quota: Some(r.quota as f64),
            status: Some(r.redeemed_by.is_some() as i16),
            // TODO(#211): SyncMeta lacks created_at; updated_at stands in.
            created_at: Some(r.meta.updated_at.format("%Y-%m-%d").to_string()),
            expires_at: r.expires_at.map(|dt| dt.format("%Y-%m-%d").to_string()),
        }
    }
}

/// Create/update model alias request — mirrors admin-api /api/alias.
#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AliasUpsertRequest {
    pub name: String,
    pub display_name: Option<String>,
    pub input_per_1k: Option<f64>,
    pub output_per_1k: Option<f64>,
    pub multiplier: Option<f64>,
    pub status: Option<u8>,
}

/// Create/update subscription plan request — mirrors admin-api /api/subscription.
///
/// quota uses f64 to match the display-layer units of SubscriptionDto/RedemptionDto; the
/// record-layer SubscriptionPlanRecord.quota uses i64 internal units and converts at the
/// boundary.
#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SubscriptionUpsertRequest {
    pub name: String,
    /// NUMERIC semantics: JSON carries it as a string to avoid floating-point error (same as
    /// SubscriptionPlanRecord.price).
    pub price: String,
    pub currency: String,
    pub duration_days: u32,
    pub quota: f64,
    pub upgrade_group: Option<String>,
    pub max_purchases: Option<u32>,
    pub enabled: Option<bool>,
}

/// Create/update redemption code request — mirrors admin-api /api/redemption.
#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RedemptionUpsertRequest {
    pub name: String,
    pub quota: f64,
    pub count: u32,
    pub expires_at: Option<String>,
}

/// Currency DTO — mirrors the admin-api /api/currency response.
#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CurrencyView {
    pub code: String,
    pub name: String,
    pub internal_rate: f64,
    pub enabled: bool,
    pub remark: String,
    /// Display symbol (¥ / $ / P); required for fiat, optional for points.
    #[serde(default)]
    pub symbol: String,
    /// `points` = balance currency that can be charged; `fiat` = pricing/display only (does
    /// not enter the balance).
    #[serde(default)]
    pub kind: String,
    /// Display decimal places: 2 for fiat, 0 for points.
    #[serde(default)]
    pub precision: i16,
}

/// User balance entry DTO.
#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UserBalanceDto {
    pub currency_code: String,
    pub amount: i64,
    /// Display symbol for this currency (from currency_defs.symbol; empty string when an
    /// older backend lacks the field).
    #[serde(default)]
    pub symbol: String,
}

/// User wallet DTO — aggregate view.
#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct WalletView {
    pub user_key: String,
    /// Invitation short code (`auth_users.aff_code`, 0013); None = not generated, and the
    /// frontend falls back to a UUID link.
    #[serde(default)]
    pub aff_code: Option<String>,
    pub balances: Vec<UserBalanceDto>,
    pub available_i64: i64,
}

/// Top-up request — mirrors admin-api /api/user/topup (order creation is
/// `POST /api/user/topup/orders`).
///
/// The `provider` amount semantics vary by channel (normalized by
/// `TopupService::open_topup` before persistence):
/// - manual (default): `amount` = points credited directly (in `currency` units);
/// - epay: `amount` = amount to pay in CNY **yuan**, `currency` = target points currency to
///   credit; points = `convert(amount, "CNY", currency)` — fiat does not enter the balance,
///   and the order row stores points.
#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TopUpRequest {
    pub user_key: String,
    pub currency: String,
    pub amount: i64,
    /// Payment channel: missing/empty string = "manual" (existing callers unchanged); real
    /// channels such as "epay".
    #[serde(default = "default_topup_provider")]
    pub provider: String,
}

/// Deserialization default for `TopUpRequest.provider` (manual = default channel with no
/// real payment).
fn default_topup_provider() -> String {
    "manual".into()
}

/// Reward request — mirrors admin-api /api/affiliate/reward.
#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RewardRequest {
    pub kind: String,
    pub user_key: String,
    pub amount: i64,
}

/// Top-up order DTO — an item in the `GET /api/user/topup/orders` list (the "top-up
/// records" view in the frontend rewards panel).
#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TopupOrderView {
    /// Order key (UUID string, `billing_topups.key`).
    pub key: String,
    /// Top-up currency code (`currency_defs.code`, e.g. "FREE").
    pub currency: String,
    /// Top-up amount (in that currency's unit, not internal units).
    pub amount: i64,
    /// Order state: pending | settling | paid | failed | refunded.
    pub state: String,
    /// Payment channel ("" = manual/no real payment, e.g. "epay" | "stripe").
    pub provider: String,
    /// Creation time (RFC3339/ISO8601 string, `billing_topups.created_at`).
    pub created_at: String,
}

/// Invitee DTO — an item in the `GET /api/affiliate/invitees` list (the "invitee list" view
/// in the frontend rewards panel).
#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct InviteeView {
    /// Invitee key (UUID string, `auth_users.key`).
    pub user_key: String,
    /// Display name: falls back to `username` when `display_name` is empty.
    pub name: String,
    /// Time the invitation relationship was established (RFC3339/ISO8601 string,
    /// `affiliate_links.created_at`).
    pub joined_at: String,
    /// Cumulative reward this invitee has brought the inviter (FREE internal units; 0 when
    /// there is no reward).
    pub reward: i64,
}
