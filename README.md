# gateway — LLM gateway data plane

The public home of the gateway: contract vocabulary, engine stages, the
assembly facade a consumer wires up, and the standalone process entry. The
crates are libraries — a product embeds the facade; the root package is
`gatewayd`, an actix-web process that serves the same pipeline from a TOML
config plus a file catalog. No database, no admin plane.

## Crate manifest

| crate                            | responsibility                                                                                                                                                                                                                                                                                                                   |
| -------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `gateway-daemon` (root package, bin `gatewayd`) | **The standalone process.** TOML config + file catalog → the same snapshots the facade consumes, served by actix-web: body ceiling, trusted-proxy policy, `/healthz` + `/readyz`, streaming pass-through. Depends on `gateway-facade` plus transport crates only; contains no admission logic and names no engine crate.                                                                                                                            |
| `crates/gateway-facade`          | **The one crate a consumer depends on.** Assembly surface in four modules: `config` (boot values + runtime knobs), `preset` (named chains), `builder` (composition + wiring), `gateway` (the assembled plane and its exits). Re-exports the vocabulary and the engine types a consumer may name, so no other crate appears in a consumer's `Cargo.toml`.                                                                      |
| `crates/gateway-core`            | Vocabulary + ports shared by the two planes: records (channel / routing / identity / usage), snapshots, retry policy, timeouts, health, metering and proxy vocabulary, the error-code table, and the admin-plane ports (`Ledger`, `SettleSink`, `PriceTable`, `DispatcherHandle`, `HealthHandle`). Internal dependency: nothing. |
| `crates/gateway-pipeline`        | Orchestration core: `Stage` trait, `RequestCtx`, the chained `Pipeline` runner, axum router integration.                                                                                                                                                                                                                         |
| `crates/gateway-gate`            | Admission gates: token auth, user state, model allow-lists, quota (with prehold), rate limits, gray list, plus the gate primitives a consumer composes with.                                                                                                                                                                     |
| `crates/gateway-dispatch`        | Catalog selection: P2C over route-unit weights with health accounting, cooldowns, token budgets, sticky routing and latency digests.                                                                                                                                                                                             |
| `crates/gateway-forward`         | Upstream forwarding and IO orchestration: build the request, send it, stream the response back; retry and per-channel concurrency live on this stage.                                                                                                                                                                            |
| `crates/gateway-protocol-bridge` | Inbound protocol dialects (OpenAI / Gemini / Claude message) and streaming codecs.                                                                                                                                                                                                                                               |
| `crates/gateway-proxy`           | Egress proxy pool: node parsing, selection, leased clients; protocol assembly via `meow-rs`.                                                                                                                                                                                                                                     |
| `crates/gateway-cache`           | Bounded concurrent store primitive (moka: LRU + TTL / idle / weighted size) + response cache. The only `moka` owner.                                                                                                                                                                                                             |
| `crates/gateway-security`        | Sensitive-word filter (Aho-corasick). Leaf crate, no gateway dependencies.                                                                                                                                                                                                                                                       |
| `crates/gateway-metering`        | Metering engine: prehold / settle / release ledger ports and the price arithmetic (`price_of`).                                                                                                                                                                                                                                  |

## What `gateway-facade` owns (and does not)

- **owns**: assembly policy — chain order, which instances must be shared, the
  input/output contracts, the insertion points, and the runtime knob surface.
- **does not own**: any request-path logic (that is the engines), any data
  loading (that is the consumer), any process shape (embedded server or daemon,
  the facade does not know). The standalone shape lives in the root
  `gateway-daemon` package; embedding consumers keep `Gateway::router()`.
- **does not leak**: consumers name nothing but `gateway_facade::…` — the
  vocabulary of every other crate is re-exported from it.

## Quick start

```toml
[dependencies]
gateway-facade = { git = "https://github.com/hathawayANdRX105/gateway" }
```

```rust
use gateway_facade::*; // the whole surface; nothing else is named

// 1. load your values (catalog snapshot, options, egress, proxies, ...) — this
//    crate reads nothing itself; you hand it plain values and shared handles.
let ctx = Context { config: Config::default(), components: /* … */ };

// 2. pick a preset (Relay / NoBilling / Open) and compose at named points.
let gateway = GatewayBuilder::relay(ctx)
    .with_gate(MyGate::new())
    .with_stage_before_forward(MyStage::new())
    .with_stage_after_chain(MyTelemetryStage::new())
    .build();

// 3. run it as a router, or drive the pipeline directly.
let router = gateway.router();

// 4. retune the running plane through the one knob surface.
gateway.knobs().update(ConfigDraft { ratelimit_max_requests: Some(500), ..Default::default() })?;
```

Named insertion points:

| method                                          | position                                | typical use                      |
| ----------------------------------------------- | --------------------------------------- | -------------------------------- |
| `GatewayBuilder::relay` / `no_billing` / `open` | which admission chain                   | preset choice                    |
| `with_gate` / `with_gates` / `without_gate`     | gate composition                        | product-specific admission rules |
| `with_stage_before_forward`                     | after the gate chain, before forwarding | auth, logging, metrics           |
| `with_stage_after_chain` / `with_stage`         | after the whole default chain           | telemetry, audit                 |

Route selection lives inside the forwarding stage (the stage drives re-selection
through its retry loop), so no separate dispatch stage sits in the default chain.

## Standalone process (`gatewayd`)

The root package is a thin process shell over the same facade; it contains no
admission logic. Build and run:

```bash
cargo run --bin gatewayd -- --config gateway.toml
# load the config + catalog, print the summary, exit without binding
cargo run --bin gatewayd -- --config gateway.toml --check
```

`gateway.toml` describes the transport; the catalog it points at describes the
data plane. Paths are resolved relative to the config file.

```toml
# gateway.toml
listen = "127.0.0.1:8080"
uds_path = "/run/gatewayd.sock"   # optional; same pipeline on a Unix socket
catalog = "gateway.catalog.toml"
max_request_bytes = 33554432      # 413 above this; the body is read into memory
trusted_proxies = ["127.0.0.1"]   # only these peers may set x-forwarded-for
workers = 0                       # 0 = actix default
shutdown_timeout_secs = 30
```

The catalog mirrors the admin plane's loader: channels × keys × groups × models
become route units, tokens are indexed by SHA-256, prices are `$ / M tokens` and
are converted to the internal unit (`500_000` = $1). A token without `quota` is
unlimited; a token without `group` follows its user's group.

```toml
# gateway.catalog.toml (abridged)
[[groups]]
name = "default"
allowed_models = ["gpt-4o*"]
multiplier = 1.0

[[users]]
key = "user-1"
username = "alice"
group = "default"

[[tokens]]
key = "sk-..."            # plaintext here: this file is the credential store
user = "user-1"
quota = 2_000_000         # internal units; omit = unlimited

[[channels]]
key = "ch-1"
name = "primary"
provider_type = "openai"
base_url = "https://api.example.com"
groups = ["default"]
[[channels.keys]]
secret = "sk-upstream"
[[channels.models]]
name = "gpt-4o"

[[prices]]
model = "gpt-4o"
input = 2.5
output = 10.0
cache = 1.25
```

Transport notes:

- Served scope is the data plane only (`/v1*`); `/healthz` and `/readyz` are
  separate routes. Unknown paths are 404 before the pipeline runs.
- The pipeline is the library's: admission chain → `ForwardStage` →
  protocol bridge. Response bodies stream frame by frame (SSE included); no
  full-response buffering.
- The axum `Gateway::router()` remains the embedding exit for products that
  merge the data plane into their own axum app; the daemon uses actix-web.
- The catalog is read once at boot; a change needs a restart.

## Config and runtime knobs

Two disjoint value sets, one rule: **everything a consumer can set lives in
`Config`; everything the running plane can change lives in `RuntimeKnobs`.**

- `Config` — boot values. Changing one means rebuilding the data plane. The
  boot-fixed subset (`timeouts`, `global_max_concurrency`, the digest / sticky /
  response-cache / word-filter mounts, `sticky_boost`) is also exposed read-only
  via `RuntimeKnobs::snapshot().boot`, so "what needs a rebuild" is visible
  instead of implied.
- `RuntimeKnobs` — the runtime-mutable subset (`retry_policy`, `health`,
  `ratelimit_*`, `token_tpm_default`) with an `epoch` counter. Changes are atomic
  and apply to the **next request**; an invalid draft is rejected whole
  (`HealthSetting::validate`).
- Structural mounts move to `ArcSwap` slots per knob once the read side of each
  stage loads per request; that migration is a follow-up, not a new interface.

## Dependency direction (hard rule)

```
gateway-facade ──> all engines ──> gateway-core ──> (nothing)
       └──────────────────────────┘
gateway-daemon ──> gateway-facade (+ actix-web / toml; no engine crate)
```

- Engines depend only on `gateway-core` plus other engines, never the reverse;
  the vocabulary surface must not grow back into implementations.
- `gateway-core` is pure vocabulary + minimal ports (serde / serde_json / chrono /
  arc-swap / dashmap / tracing / thiserror / base64 / url / percent-encoding).
  tokio / axum / reqwest / any engine crate are forbidden — that is the rule
  that keeps the vocabulary wasm-*compatible in principle*; the wasm32 target
  itself is not yet green (the inherited `dashmap` / `url` dependency chains do
  not build there — tracked as a follow-up, not a claim).
- The consumer (your product) depends on `gateway-facade`; it names no other
  crate.

## Conventions

- Comments and doc comments in English; test modules and tests live in
  `tests/`, never in `src/`.
- `[lints] workspace = true` enforces the idiom set (hand-written combinators,
  if/else discipline, etc.).
- Conventional commits; one commit does one thing.

## License

MIT.
