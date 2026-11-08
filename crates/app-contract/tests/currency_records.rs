use app_contract::api::billing::{
    CurrencyView, RewardRequest, TopUpRequest, UserBalanceDto, WalletView,
};
use app_contract::records::SyncMeta;
use app_contract::records::billing::{CurrencyDefRecord, UserBalanceRecord};

#[test]
fn currency_def_record_roundtrip() {
    let record = CurrencyDefRecord {
        meta: SyncMeta {
            key: "currency_defs:FREE".to_string(),
            schema_version: 7,
            logical_version: 1,
            origin: "center".to_string(),
            updated_at: chrono::Utc::now(),
        },
        code: "FREE".to_string(),
        name: "Free Points".to_string(),
        internal_rate: 1.0,
        enabled: true,
        remark: "".to_string(),
        symbol: "P".to_string(),
        kind: "points".to_string(),
        precision: 0,
    };
    let json = serde_json::to_string(&record).unwrap();
    let decoded: CurrencyDefRecord = serde_json::from_str(&json).unwrap();
    assert_eq!(record, decoded);
}

#[test]
fn user_balance_record_roundtrip() {
    let record = UserBalanceRecord {
        meta: SyncMeta {
            key: "user_balances:user1:FREE".to_string(),
            schema_version: 7,
            logical_version: 1,
            origin: "center".to_string(),
            updated_at: chrono::Utc::now(),
        },
        user_key: "user1".to_string(),
        currency_code: "FREE".to_string(),
        amount: 500000, // 500_000 = $1 in internal units
    };
    let json = serde_json::to_string(&record).unwrap();
    let decoded: UserBalanceRecord = serde_json::from_str(&json).unwrap();
    assert_eq!(record, decoded);
}

#[test]
fn currency_view_roundtrip() {
    let view = CurrencyView {
        code: "FREE".to_string(),
        name: "Free Points".to_string(),
        internal_rate: 1.0,
        enabled: true,
        remark: "".to_string(),
        symbol: "P".to_string(),
        kind: "points".to_string(),
        precision: 0,
    };
    let json = serde_json::to_string(&view).unwrap();
    let decoded: CurrencyView = serde_json::from_str(&json).unwrap();
    assert_eq!(view, decoded);
}

#[test]
fn user_balance_dto_roundtrip() {
    let dto = UserBalanceDto {
        currency_code: "FREE".to_string(),
        amount: 500000,
        symbol: "P".to_string(),
    };
    let json = serde_json::to_string(&dto).unwrap();
    let decoded: UserBalanceDto = serde_json::from_str(&json).unwrap();
    assert_eq!(dto, decoded);
}

#[test]
fn wallet_view_roundtrip() {
    let view = WalletView {
        aff_code: Some("aB3x9Q".to_string()),
        user_key: "user1".to_string(),
        balances: vec![
            UserBalanceDto {
                currency_code: "FREE".to_string(),
                amount: 500000,
                symbol: "P".to_string(),
            },
            UserBalanceDto {
                currency_code: "PAID".to_string(),
                amount: 300000,
                symbol: "P".to_string(),
            },
        ],
        available_i64: 800000,
    };
    let json = serde_json::to_string(&view).unwrap();
    let decoded: WalletView = serde_json::from_str(&json).unwrap();
    // camelCase pins the wire key: the frontend wire.rs decodes affCode; a typo drops the short
    // code and the invite link falls back to user_key.
    assert!(
        json.contains("\"affCode\""),
        "WalletView must serialize affCode as camelCase: {json}"
    );
    assert_eq!(view, decoded);
}

#[test]
fn top_up_request_roundtrip() {
    let request = TopUpRequest {
        user_key: "user1".to_string(),
        currency: "FREE".to_string(),
        amount: 100000,
        provider: "manual".into(),
    };
    let json = serde_json::to_string(&request).unwrap();
    let decoded: TopUpRequest = serde_json::from_str(&json).unwrap();
    assert_eq!(request, decoded);
}

#[test]
fn reward_request_roundtrip() {
    let request = RewardRequest {
        kind: "referral".to_string(),
        user_key: "user1".to_string(),
        amount: 50000,
    };
    let json = serde_json::to_string(&request).unwrap();
    let decoded: RewardRequest = serde_json::from_str(&json).unwrap();
    assert_eq!(request, decoded);
}
