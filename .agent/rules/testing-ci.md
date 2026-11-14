# Testing & CI

**When to read this**: when writing a test, deciding what to run locally, reading a CI result, or suspecting that "CI is green but did not actually verify anything".

**What this solves**: how tests are written and run here, what the local gates check, and how CI can pass without asserting anything.

Read order: §1–§2 before writing a test, §3 when wiring a new dependency for tests, §4 when you watch CI, §5 when CI surprises you.

## 1. Writing tests

- Tests live in each crate's standard `tests/` directory (`crates/<crate>/tests/`); `#[cfg(test)]` modules inside `src/` are not the convention here. Unit, integration and contract tests all go under `tests/`. Assert **real behavior** — never decorate with `assert!(true)`.
- Every test carries an English comment stating its **purpose**, the **scenarios it covers**, and the **acceptance criterion** it asserts.
- **A test that cannot reach its dependency fails; it does not return early.** A test that prints "skipping" and `return`s is recorded as passed — that is a false green, and it is a defect in the test, not a graceful fallback. Gateway tests should run against in-memory snapshots and local mock listeners; if a test genuinely needs an optional environment, say so in the file header and make the case explicit.
- The engines are IO orchestrators: prefer spinning a local `TcpListener` / in-process axum router over reaching the network. Tests that bind fixed ports collide under nextest's process-per-test model — bind port 0 and read back the assigned address.
- Tests that share a mutable resource must be serialized: nextest runs **one process per test**, so a `static Mutex` cannot serialize two test binaries against the same resource. Add them to a `test-groups` entry in `.config/nextest.toml` (the config file path is fixed by nextest: `workspace-root/.config/nextest.toml`) and validate with `cargo nextest show-config test-groups`.

## 2. What to run locally

| Scenario                          | Command                                              | Notes                                                                                                                                     |
| --------------------------------- | ---------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------- |
| Affected tests only (daily entry) | `just test-fast [base]` (default base `origin/main`) | `testless` selects affected cases, `nextest` runs them. Every fallback is loud — a degrade always runs the full workspace, never nothing. |
| Verify compilation                | `cargo check -p <crate>`                             | Wrap in `.githooks/heavy` on multi-session machines.                                                                                      |
| Debug a single failing case       | `cargo test -p <crate> -- <test name>`               | Debug only, not acceptance; must finish in ~3 seconds.                                                                                    |
| Full check suite                  | `just verify`                                        | fmt + clippy + check + spec-check (no tests — run `just test-fast` / `just test` for those). The local "everything" entry.                     |
| Full test run                     | `just test`                                          | `cargo nextest run --workspace`. The suite is small enough to run locally; CI runs it under `--cargo-profile ci` regardless.              |

The local dev loop compiles with **Cranelift** (dev profile) for speed; CI and the shipped-quality gate compile with **LLVM** (`--profile ci`). A Cranelift-only pass locally is normal — CI is the LLVM verdict.

## 3. Normative gates (rust-specs)

- The idiom forms this repo cares about are already **workspace lints** (`[workspace.lints.clippy]` in the root `Cargo.toml`) and enforced by `just clippy` under `-D warnings`.
- The rust-specs **dylint syntax gate** is wired via `just spec-check` (`scripts/spec-check.sh`): it loads the **pre-built lint libraries** from the rust-specs checkout and runs each against the workspace. First time on a machine: `just spec-check build` (builds the libs from rust-specs; needs `RUST_SPECS_DIR` or the default `$HOME/projects/rust-specs`). It is wired into `.githooks/pre-push` and the CI `spec-check` job.
- The dylint lints are **generic Rust syntax rules** (ban-unwrap, test-placement, nesting depth, let-if-else, …) — they run on any Rust repo, so gateway adopts them directly. (The rust-specs `checks` _structure_ rules — dep-edge / path-forbidden — are still ferrite-layout-specific and are **not** wired here; the dependency-direction red line is enforced by README + AGENTS.md + PR review.)
- Running dylint locally needs the pinned nightly + `cargo-dylint`; the pinned nightly already ships `rustc-dev` + `rust-src` for it. On machines without the tooling the gate **skips with a notice** — run it on desktop or CI before treating a change as clean.

## 4. CI

- CI is the repo's own `.github/workflows/ci.yml`, three jobs on the pinned nightly (LLVM `--profile ci` throughout):
  - `lint`: `fmt --check` + clippy `-D warnings` + check.
  - `spec-check`: the rust-specs dylint syntax gate (builds the pre-compiled lint libs from rust-specs, then loads each).
  - `test`: **PR lane runs `test-fast`** (testless narrows to affected tests); **the main-push (merge) lane runs the full workspace** (`cargo nextest run --workspace --cargo-profile ci`). The codegen backend in CI is always LLVM.
- A PR merges only when CI is green. When CI fails, pull the logs (`gh run view <id> --log-failed`) and fix on the same PR.
- `gh run view --log-failed` returns empty while the run is not `completed` — check run state before concluding there are no logs.

## 5. Ways CI can be falsely green

1. **Feature-gated tests**: a test file with `#![cfg(feature = "xxx")]` is compiled and executed but outputs `running 0 tests`, because CI never enables that feature. Fix: make the test run without the feature, or enable the feature for that package in the workflow; always note the feature requirement in the test file header.
2. **Resource-gated tests that return early**: a test that cannot reach a required service prints a skip message and returns — recorded as passed. Confirm the resource is actually up rather than trusting the skip message.
3. **`#[ignore]` tests**: CI never passes `--ignored`, so ignored tests never run at all.
4. **Zero-hit selection**: `just test-fast` treats "selected N, ran 0" as suspicious and falls back to a full run; never "fix" a zero-hit by deleting the guard.

## 6. Hard constraints

- Run `just verify` (fmt + clippy + check + spec-check) and the relevant test entry before committing; never bypass `.githooks/pre-push` with `--no-verify`.
- When CI fails, fix on the same PR — do not open another PR.
- Never hand-edit `.config/nextest.toml`'s path or move it into `.cargo/`: nextest discovers it only at `workspace-root/.config/nextest.toml`.
