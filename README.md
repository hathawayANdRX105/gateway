# gateway — LLM gateway data plane

The public home of the gateway: contract vocabulary, engine stages, and the
assembly facade a consumer wires up. Everything here is a library crate — no
process entry, no database, no admin plane. A product assembles these parts.

## Crate manifest

| crate                            | responsibility                                                                                                                                                                                                                                                                                                                   |
| -------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `crates/gateway-facade`          | **The one crate a consumer depends on.** Assembly surface in three parts: `config` (boot values + runtime knobs), `builder` (presets, gate/stage composition), `preset` (named chains). Re-exports the whole vocabulary so a consumer names no other crate.                                                                      |
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
  the facade does not know).
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
```

- Engines depend only on `gateway-core` plus other engines, never the reverse;
  the vocabulary surface must not grow back into implementations.
- `gateway-core` is pure vocabulary + minimal ports (serde / chrono / arc-swap /
  dashmap / thiserror / base64 / url / percent-encoding). tokio / axum /
  reqwest / any engine crate are forbidden — that is what keeps it wasm-safe.
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
