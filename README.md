# gateway — LLM gateway data plane

The public home of the gateway: contract vocabulary, engine stages, and the
assembly facade a consumer wires up. Everything here is a library crate — no
process entry, no database, no admin plane. A product assembles these parts.

## Crate manifest

| crate | responsibility |
|---|---|
| `crates/app-contract` | Cross-boundary DTOs: records (channel / token / user / group / route unit) and protocol errors. Pure data, wasm-safe. |
| `crates/gateway-contract` | Injection surface shared by the two planes: vocabulary (snapshots, retry policy, timeouts, ports) the admin side and the engines pass around. Internal dependency: only `app-contract`. |
| `crates/gateway-cache` | Bounded concurrent store primitive (moka: LRU + TTL / idle / weighted size) + response cache. The only `moka` owner. |
| `crates/gateway-dispatch` | Catalog selection: P2C over route-unit weights with health accounting, cooldowns, token budgets, sticky routing and latency digests. |
| `crates/gateway-forward` | Upstream forwarding and IO orchestration: build the request, send it, stream the response back; retry and per-channel concurrency live on this stage. |
| `crates/gateway-gate` | Admission gates: token auth, user state, model allow-lists, quota (with prehold), rate limits, gray list. |
| `crates/gateway-metering` | Metering engine: prehold / settle / release ledger ports and the price arithmetic (`price_of`). |
| `crates/gateway-pipeline` | Orchestration core: `Stage` trait, `RequestCtx`, the chained `Pipeline` runner, axum router integration. |
| `crates/gateway-protocol-bridge` | Inbound protocol dialects (OpenAI / Gemini / Claude message) and streaming codecs. |
| `crates/gateway-proxy` | Egress proxy pool: node parsing, selection, leased clients; protocol assembly via `meow-rs`. |
| `crates/gateway-security` | Sensitive-word filter (Aho-corasick). Leaf crate, no gateway dependencies. |
| `crates/gateway-facade` | **Public assembly facade.** `GatewayBuilder` assembles the default relay chain and exposes the `Stage` insertion points. Start here. |

## Quick start

```toml
[dependencies]
gateway-facade = { git = "https://github.com/hathawayANdRX105/gateway" }
```

```rust
use gateway_facade::{Context, GatewayBuilder, GatewayConfig, State};

// 1. load your values (catalog snapshot, options, egress, ...) — this crate reads
//    nothing itself; you hand it plain values and shared handles.
let ctx = Context { /* ... */ tuning: GatewayConfig { /* ... */ ..Default::default() }, /* ... */ };

// 2. assemble the default chain: gate chain -> forwarding stage -> protocol bridge.
let gateway = GatewayBuilder::relay(ctx).build();

// 3. add stages of your own at named points without naming the engines.
let gateway = GatewayBuilder::relay(ctx)
    .with_stage_before_forward(MyAuthStage::new())
    .with_stage_after_chain(MyTelemetryStage::new())
    .build();

// 4. run it as a router, or drive the pipeline directly.
let router = gateway.router();
```

Named insertion points:

| method | position | typical use |
|---|---|---|
| `with_stage_before_forward` | after the gate chain, before forwarding | auth, logging, metrics |
| `with_stage_after_chain` | after the whole default chain | telemetry, audit |
| `with_stage` | appended (alias of after-chain) | same |

Route selection lives inside the forwarding stage (the stage drives re-selection
through its retry loop), so no separate dispatch stage sits in the default chain.

## Dependency direction (hard rule)

```
gateway-facade ──> all engines ──> gateway-contract ──> app-contract
                        └──────────> gateway-contract ──> app-contract
```

- Engines depend **only** on the contract crates plus other engines, never the
  reverse (the contract surface must not grow back into implementations).
- `app-contract` / `gateway-contract` are pure vocabulary + minimal ports.
- The consumer (your product) depends on `gateway-facade`; it need not name any
  engine crate.

## Conventions

- Comments and doc comments in English; test modules and tests live in
  `tests/`, never in `src/`.
- `[lints] workspace = true` enforces the idiom set (hand-written combinators,
  if/else discipline, etc.).
- Conventional commits; one commit does one thing.

## License

MIT.
