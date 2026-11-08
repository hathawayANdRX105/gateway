//! Commercial records — subscription/order/redemption (v2 domain).
//!
//! Fields correspond directly to domain four of 07-database-schema.md; enabled when the
//! console payment page is wired up.
//! References: new-api internal/billing (store_subscription/topup_api/store_redemption),
//! sub2api ent/schema/{subscription_plan,user_subscription,payment_order,redeem_code}.

use super::SyncMeta;
use serde::{Deserialize, Serialize};

/// Purchasable subscription product (price/duration/quota/upgrade group).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SubscriptionPlanRecord {
    pub meta: SyncMeta,
    pub name: String,
    pub price: String, // NUMERIC semantics; JSON carries a string to avoid float error
    pub currency: String, // "CNY" | "USD"
    pub duration_days: u32,
    pub quota: i64,
    /// Group the user is upgraded to after purchase; None = unchanged.
    pub upgrade_group: Option<String>,
    pub max_purchases: Option<u32>,
    pub enabled: bool,
}

/// A subscription instance held by a user (time window + consumption cap).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UserSubscriptionRecord {
    pub meta: SyncMeta,
    pub user_key: String,
    pub plan_key: String,
    pub starts_at: chrono::DateTime<chrono::Utc>,
    pub expires_at: chrono::DateTime<chrono::Utc>,
    /// Consumption window: "daily" | "weekly" | "monthly" | "custom".
    pub window_kind: String,
    /// Consumption cap within the window (0 = unlimited).
    pub window_limit: i64,
    pub status: u8,
}

/// Payment order (state machine: pending → paid|failed|refunded; terminal states are
/// immutable).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PaymentOrderRecord {
    pub meta: SyncMeta,
    pub user_key: String,
    pub provider: String, // "epay" | "stripe" | "creem"
    pub amount: String,
    pub quota: i64,
    /// None = pure top-up order; Some = subscription purchase order.
    pub plan_key: Option<String>,
    pub state: String,
    pub provider_txn_id: Option<String>,
    pub created_at: chrono::DateTime<chrono::Utc>,
    pub paid_at: Option<chrono::DateTime<chrono::Utc>>,
}

/// Redeem code (single redemption, guaranteed by row lock/CAS).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RedeemCodeRecord {
    pub meta: SyncMeta,
    /// sha256(plaintext code); the plaintext appears only when a batch is generated and
    /// exported.
    pub code_hash: String,
    pub quota: i64,
    pub batch: String,
    pub redeemed_by: Option<String>,
    pub redeemed_at: Option<chrono::DateTime<chrono::Utc>>,
    pub expires_at: Option<chrono::DateTime<chrono::Utc>>,
}

/// Currency definition (currency_defs table).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CurrencyDefRecord {
    pub meta: SyncMeta,
    pub code: String,
    pub name: String,
    pub internal_rate: f64,
    pub enabled: bool,
    pub remark: String,
    /// Display symbol (¥ / $ / P); required for fiat, nullable for points.
    pub symbol: String,
    /// `points` = balance currency that can be charged; `fiat` = pricing/display only
    /// (never enters user_balances).
    pub kind: String,
    /// Display decimal places: 2 for fiat, 0 for points.
    pub precision: i16,
}

/// User currency balance (user_balances table).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UserBalanceRecord {
    pub meta: SyncMeta,
    pub user_key: String,
    pub currency_code: String,
    pub amount: i64,
}
