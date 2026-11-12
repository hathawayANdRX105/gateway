//! Catalog file → data-plane snapshots.
//!
//! The standalone process has no database: channels, groups, users, tokens and prices come
//! from one TOML file. This module is the whole "loading" half of the facade contract — it
//! builds the owned snapshot values the builder consumes and nothing here touches a
//! request.
//!
//! Expansion mirrors the admin plane's loader: a route unit is the
//! `(channel, key, group, public model)` quadruple, so one channel with three keys and two
//! models visible in two groups produces twelve units. Keeping the shape identical means a
//! file catalog and a database catalog feed the dispatcher the same way.

use std::collections::{HashMap, HashSet};
use std::path::Path;

use anyhow::{Context, ensure};
use chrono::Utc;
use gateway_facade::{
    ChannelKey, ChannelRecord, ChannelSettings, GroupEntry, GroupSnapshot, ModelPrice, PriceRow,
    PriceTable, PricingSnapshot, QuotaSnapshot, RouteUnitRecord, STATUS_ENABLED, Snapshot, SyncMeta,
    TokenEntry, TokenRecord, TokenSnapshot, UserRecord, UserSnapshot, sha256,
};
use serde::Deserialize;
use serde_json::Value;

/// Internal units per dollar (new-api convention): catalog prices are `$ / M tokens`, and
/// both the quota estimate and the settlement arithmetic work in internal units.
const UNITS_PER_DOLLAR: f64 = 500_000.0;

/// Balance used for a token declared `unlimited`. Large enough never to bind, small enough
/// that settlement arithmetic cannot overflow.
const UNLIMITED_BALANCE: i64 = i64::MAX / 2;

/// Documented default values.
///
/// `#[serde(default = "…")]` takes a function path, not a constant, so each value lives
/// here and the serde hooks at the bottom of the file are thin wrappers over them
/// (`const fn` where the return type allows it — `String` cannot be const-constructed).
/// The hooks are complete, not placeholders: they change only when the documented
/// default itself changes.
const DEFAULT_GROUP_NAME: &str = "default";
const DEFAULT_PROVIDER_TYPE: &str = "openai";
const DEFAULT_CHANNEL_MAX_CONCURRENCY: u32 = 32;
const DEFAULT_USER_ROLE: u16 = 1;
const DEFAULT_MODEL_WEIGHT: u32 = 1;
const DEFAULT_GROUP_MULTIPLIER: f64 = 1.0;

/// One catalog document.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CatalogFile {
    #[serde(default)]
    pub groups: Vec<GroupSpec>,
    #[serde(default)]
    pub users: Vec<UserSpec>,
    #[serde(default)]
    pub tokens: Vec<TokenSpec>,
    #[serde(default)]
    pub channels: Vec<ChannelSpec>,
    #[serde(default)]
    pub prices: Vec<PriceSpec>,
}

/// Per-group policy: model allowlist, rate multiplier and the enable bit.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GroupSpec {
    pub name: String,
    #[serde(default)]
    pub allowed_models: Vec<String>,
    #[serde(default = "default_group_multiplier")]
    pub multiplier: f64,
    #[serde(default = "enabled")]
    pub enabled: bool,
}

/// User account. A token must point at one of these keys.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct UserSpec {
    pub key: String,
    pub username: String,
    #[serde(default)]
    pub display_name: String,
    #[serde(default)]
    pub email: String,
    #[serde(default = "default_group")]
    pub group: String,
    #[serde(default = "enabled_status")]
    pub status: u8,
    #[serde(default = "user_role")]
    pub role: u16,
}

/// Client token. The plaintext key is accepted here because this file *is* the credential
/// store for the standalone shape; only its SHA-256 reaches the snapshot.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TokenSpec {
    pub key: String,
    pub user: String,
    #[serde(default)]
    pub name: String,
    /// Fixed group; absent = follow the user's group.
    #[serde(default)]
    pub group: Option<String>,
    /// `None` = all models allowed.
    #[serde(default)]
    pub allowed_models: Option<Vec<String>>,
    /// Remaining balance in internal units. Absent = unlimited.
    #[serde(default)]
    pub quota: Option<i64>,
    #[serde(default = "enabled_status")]
    pub status: u8,
}

/// Upstream channel.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ChannelSpec {
    pub key: String,
    pub name: String,
    #[serde(default = "default_provider")]
    pub provider_type: String,
    pub base_url: String,
    #[serde(default = "default_channel_concurrency")]
    pub max_concurrency: u32,
    #[serde(default = "enabled_status")]
    pub status: u8,
    #[serde(default)]
    pub groups: Vec<String>,
    pub keys: Vec<KeySpec>,
    #[serde(default)]
    pub models: Vec<ModelSpec>,
    /// Channel-level overrides (raw until load); unknown keys are preserved on the
    /// record via [`ChannelSettings::extra`].
    #[serde(default)]
    pub settings: Option<Value>,
}

/// One upstream credential. `index` is optional; omitted entries are numbered by position.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct KeySpec {
    #[serde(default)]
    pub index: Option<u32>,
    pub secret: String,
    #[serde(default)]
    pub rpm_limit: u32,
}

/// One public model served by a channel.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ModelSpec {
    /// Public alias the client asks for.
    pub name: String,
    /// Upstream name; absent = the public alias.
    #[serde(default)]
    pub upstream: Option<String>,
    #[serde(default = "model_weight")]
    pub weight: u32,
    /// Group restriction; empty = every group the channel is visible in.
    #[serde(default)]
    pub groups: Vec<String>,
}

/// Price of one model (optionally per group), in dollars per million tokens.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PriceSpec {
    pub model: String,
    #[serde(default = "default_group")]
    pub group: String,
    pub input: f64,
    pub output: f64,
    #[serde(default)]
    pub cache: f64,
}

/// Built snapshots plus the counts `/readyz` reports.
#[derive(Debug)]
pub struct Catalog {
    pub dispatch: Snapshot,
    pub token_snapshot: TokenSnapshot,
    pub user_snapshot: UserSnapshot,
    pub group_snapshot: GroupSnapshot,
    pub quota_snapshot: QuotaSnapshot,
    pub pricing_snapshot: PricingSnapshot,
    pub price_table: CatalogPriceTable,
    pub counts: CatalogCounts,
}

/// What the catalog contains, reported by the readiness endpoint.
#[derive(Debug, Clone, Copy, Default)]
pub struct CatalogCounts {
    pub channels: usize,
    pub route_units: usize,
    pub groups: usize,
    pub users: usize,
    pub tokens: usize,
    pub prices: usize,
}

/// Settlement price table built from the catalog.
///
/// Lookup order is exact `(model, group)` then the model's `default` group, matching
/// [`PricingSnapshot`]'s fallback so admission estimation and settlement agree.
#[derive(Debug)]
pub struct CatalogPriceTable {
    rows: HashMap<String, HashMap<String, ModelPrice>>,
}

impl PriceTable for CatalogPriceTable {
    fn lookup(&self, model: &str, group: &str) -> Option<ModelPrice> {
        self.rows
            .get(model)
            .and_then(|groups| groups.get(group).or_else(|| groups.get("default")).copied())
    }
}

impl Catalog {
    /// Read and build a catalog file.
    ///
    /// # Errors
    ///
    /// Returns an error for unreadable/malformed files and for semantically invalid
    /// catalogs (duplicate keys, a token pointing at a missing user, an empty channel
    /// group set, a negative price).
    pub fn load(path: &Path) -> anyhow::Result<Self> {
        let text = std::fs::read_to_string(path)
            .with_context(|| format!("read catalog {}", path.display()))?;
        Self::from_toml(&text).with_context(|| format!("catalog {}", path.display()))
    }

    /// Build from a TOML document (also the test entry point).
    ///
    /// # Errors
    ///
    /// See [`Self::load`].
    pub fn from_toml(text: &str) -> anyhow::Result<Self> {
        let file: CatalogFile = toml::from_str(text)?;
        Self::build(&file)
    }

    fn build(file: &CatalogFile) -> anyhow::Result<Self> {
        validate_unique(file)?;

        let group_snapshot = build_groups(&file.groups);
        let user_snapshot = build_users(&file.users);
        let (dispatch, route_units) = build_dispatch(&file.channels)?;
        let (token_snapshot, quota_snapshot) = build_tokens(&file.tokens, &file.users)?;
        let (pricing_snapshot, price_table) = build_prices(&file.prices)?;

        let counts = CatalogCounts {
            channels: dispatch.channels.len(),
            route_units,
            groups: file.groups.len(),
            users: file.users.len(),
            tokens: file.tokens.len(),
            prices: file.prices.len(),
        };
        Ok(Self {
            dispatch,
            token_snapshot,
            user_snapshot,
            group_snapshot,
            quota_snapshot,
            pricing_snapshot,
            price_table,
            counts,
        })
    }
}

fn build_groups(specs: &[GroupSpec]) -> GroupSnapshot {
    let mut snapshot = GroupSnapshot::default();
    for spec in specs {
        let entry = GroupEntry {
            allowed_models: spec.allowed_models.clone(),
            multiplier: spec.multiplier,
            enabled: spec.enabled,
        };
        snapshot.upsert(spec.name.clone(), entry);
    }
    snapshot
}

fn build_users(specs: &[UserSpec]) -> UserSnapshot {
    let snapshot = UserSnapshot::default();
    for spec in specs {
        snapshot.upsert(UserRecord {
            meta: meta(&spec.key),
            username: spec.username.clone(),
            display_name: spec.display_name.clone(),
            email: spec.email.clone(),
            quota: 0,
            used_quota: 0,
            request_count: 0,
            group: spec.group.clone(),
            status: spec.status,
            role: spec.role,
            created_at: Utc::now(),
        });
    }
    snapshot
}

fn build_dispatch(channels: &[ChannelSpec]) -> anyhow::Result<(Snapshot, usize)> {
    let mut snapshot = Snapshot::default();
    let mut seen_units = HashSet::new();

    for channel in channels {
        if !channel.models.is_empty() {
            ensure!(
                !channel.keys.is_empty(),
                "channel {}: models require at least one key",
                channel.key
            );
            ensure!(
                !channel.groups.is_empty(),
                "channel {}: models require at least one group",
                channel.key
            );
        }

        let mut keys = Vec::with_capacity(channel.keys.len());
        let mut seen_indexes = HashSet::new();
        let mut seen_secrets = HashSet::new();
        for (position, key) in channel.keys.iter().enumerate() {
            let index = match key.index {
                Some(index) => index,
                None => u32::try_from(position).with_context(|| {
                    format!("channel {}: more keys than the index space", channel.key)
                })?,
            };
            ensure!(
                seen_indexes.insert(index),
                "channel {}: duplicate key index {index}",
                channel.key
            );
            ensure!(
                seen_secrets.insert(key.secret.as_str()),
                "channel {}: duplicate key secret",
                channel.key
            );
            keys.push(ChannelKey {
                index,
                secret: key.secret.clone(),
                rpm_limit: key.rpm_limit,
            });
        }

        snapshot.channels.insert(
            channel.key.clone(),
            ChannelRecord {
                meta: meta(&channel.key),
                name: channel.name.clone(),
                provider_type: channel.provider_type.clone(),
                base_url: channel.base_url.clone(),
                keys: keys.clone(),
                max_concurrency: channel.max_concurrency,
                status: channel.status,
                groups: channel.groups.clone(),
                settings: {
                    let raw = channel.settings.clone().unwrap_or(serde_json::json!({}));
                    let parsed = ChannelSettings::from_value(raw).map_err(|e| {
                        anyhow::anyhow!(
                            "channel {}: invalid settings: {e}",
                            channel.key
                        )
                    })?;
                    parsed.validate().map_err(|e| {
                        anyhow::anyhow!(
                            "channel {}: invalid settings: {e}",
                            channel.key
                        )
                    })?;
                    parsed
                },
            },
        );

        for model in &channel.models {
            let groups = effective_groups(channel, model)?;
            push_model_units(
                &mut snapshot.units,
                channel,
                model,
                &groups,
                &keys,
                &mut seen_units,
            )?;
        }
    }

    let unit_count = snapshot.units.len();
    Ok((snapshot, unit_count))
}

/// Expand one model across its visible groups and the channel's keys.
fn push_model_units(
    units: &mut Vec<RouteUnitRecord>,
    channel: &ChannelSpec,
    model: &ModelSpec,
    groups: &[String],
    keys: &[ChannelKey],
    seen: &mut HashSet<String>,
) -> anyhow::Result<()> {
    for group in groups {
        for key in keys {
            let unit_key = format!("{}:{group}:{}:{}", channel.key, model.name, key.index);
            ensure!(
                seen.insert(unit_key.clone()),
                "duplicate route unit {unit_key}"
            );
            units.push(RouteUnitRecord {
                meta: meta(&unit_key),
                group: group.clone(),
                public_model: model.name.clone(),
                channel_key: channel.key.clone(),
                key_index: key.index,
                upstream_model: model.upstream.clone().unwrap_or_else(|| model.name.clone()),
                weight: model.weight,
                status: channel.status,
            });
        }
    }
    Ok(())
}

fn effective_groups(channel: &ChannelSpec, model: &ModelSpec) -> anyhow::Result<Vec<String>> {
    if model.groups.is_empty() {
        return Ok(channel.groups.clone());
    }
    let groups = channel
        .groups
        .iter()
        .filter(|group| model.groups.contains(group))
        .cloned()
        .collect::<Vec<_>>();
    ensure!(
        !groups.is_empty(),
        "channel {}: model {} is restricted to groups it is not visible in",
        channel.key,
        model.name
    );
    Ok(groups)
}

fn build_tokens(
    specs: &[TokenSpec],
    users: &[UserSpec],
) -> anyhow::Result<(TokenSnapshot, QuotaSnapshot)> {
    let known_users = users
        .iter()
        .map(|user| user.key.as_str())
        .collect::<HashSet<_>>();
    let snapshot = TokenSnapshot::default();
    let quotas = QuotaSnapshot::default();

    for spec in specs {
        ensure!(
            known_users.contains(spec.user.as_str()),
            "token {}: unknown user {}",
            spec.name,
            spec.user
        );
        ensure!(
            !spec.key.trim().is_empty(),
            "token {}: empty key",
            spec.name
        );

        let hash = sha256(&spec.key);
        let token_key = format!("tok_{}", hex::encode(hash));
        let remaining = match spec.quota {
            Some(quota) => {
                ensure!(quota >= 0, "token {}: negative quota", spec.name);
                quota
            }
            None => UNLIMITED_BALANCE,
        };

        let record = TokenRecord {
            meta: meta(&token_key),
            user_key: spec.user.clone(),
            name: spec.name.clone(),
            key_hash: hex::encode(hash),
            key_preview: preview(&spec.key),
            group: spec.group.clone(),
            quota: remaining,
            unlimited_quota: spec.quota.is_none(),
            used_quota: 0,
            expires_at: None,
            status: spec.status,
        };
        snapshot.upsert(hash, TokenEntry::new(record, spec.allowed_models.clone()));
        quotas.upsert(token_key, remaining);
    }
    Ok((snapshot, quotas))
}

fn build_prices(specs: &[PriceSpec]) -> anyhow::Result<(PricingSnapshot, CatalogPriceTable)> {
    let snapshot = PricingSnapshot::default();
    let mut rows: HashMap<String, HashMap<String, ModelPrice>> = HashMap::new();
    let mut seen = HashSet::new();

    for spec in specs {
        ensure!(
            seen.insert((spec.model.clone(), spec.group.clone())),
            "duplicate price for {}/{}",
            spec.model,
            spec.group
        );
        ensure!(
            spec.input.is_finite() && spec.output.is_finite() && spec.cache.is_finite(),
            "price {}: non-finite value",
            spec.model
        );
        ensure!(
            spec.input >= 0.0 && spec.output >= 0.0 && spec.cache >= 0.0,
            "price {}: negative value",
            spec.model
        );

        snapshot.upsert(
            spec.model.clone(),
            spec.group.clone(),
            PriceRow {
                input_per_m: spec.input * UNITS_PER_DOLLAR,
                output_per_m: spec.output * UNITS_PER_DOLLAR,
                cache_per_m: spec.cache * UNITS_PER_DOLLAR,
            },
        );
        rows.entry(spec.model.clone()).or_default().insert(
            spec.group.clone(),
            ModelPrice {
                input: spec.input,
                output: spec.output,
                cache: spec.cache,
                group_multiplier: 1.0,
            },
        );
    }
    Ok((snapshot, CatalogPriceTable { rows }))
}

fn validate_unique(file: &CatalogFile) -> anyhow::Result<()> {
    unique_by(file.groups.iter().map(|g| g.name.as_str()), "group")?;
    unique_by(file.users.iter().map(|u| u.key.as_str()), "user")?;
    unique_by(file.channels.iter().map(|c| c.key.as_str()), "channel")?;
    unique_by(file.tokens.iter().map(|t| t.key.as_str()), "token")?;
    Ok(())
}

fn unique_by<'a>(items: impl Iterator<Item = &'a str>, what: &str) -> anyhow::Result<()> {
    let mut seen = HashSet::new();
    for item in items {
        ensure!(seen.insert(item), "duplicate {what} {item}");
    }
    Ok(())
}

fn meta(key: &str) -> SyncMeta {
    SyncMeta {
        key: key.to_string(),
        schema_version: 1,
        logical_version: 1,
        origin: "gatewayd".to_string(),
        updated_at: Utc::now(),
    }
}

fn preview(key: &str) -> String {
    let head = key.chars().take(4).collect::<String>();
    let tail = key
        .chars()
        .rev()
        .take(4)
        .collect::<Vec<_>>()
        .into_iter()
        .rev()
        .collect::<String>();
    format!("{head}****{tail}")
}

const fn enabled() -> bool {
    true
}

const fn enabled_status() -> u8 {
    STATUS_ENABLED
}

const fn user_role() -> u16 {
    DEFAULT_USER_ROLE
}

const fn model_weight() -> u32 {
    DEFAULT_MODEL_WEIGHT
}

const fn default_channel_concurrency() -> u32 {
    DEFAULT_CHANNEL_MAX_CONCURRENCY
}

const fn default_group_multiplier() -> f64 {
    DEFAULT_GROUP_MULTIPLIER
}

fn default_group() -> String {
    DEFAULT_GROUP_NAME.to_string()
}

fn default_provider() -> String {
    DEFAULT_PROVIDER_TYPE.to_string()
}
