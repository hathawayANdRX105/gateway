# gateway conventions

gateway is the public LLM-gateway data-plane workspace in Rust: contract
vocabulary, engine stages (gate / dispatch / forward / metering / protocol /
proxy / security) and the assembly facade (`gateway-facade`) a consumer wires
up. Everything here is a library crate — no process entry, no database, no
admin plane. A product assembles these parts; this repo must stay product-neutral.

This file carries hard constraints and the order of operations; expanded
procedures live under `.agent/rules/` — look them up when needed instead of
reading them end to end.

| Scenario | Read |
|---|---|
| Rust public API / code style / investigation and review | `.agent/rules/rust-code.md` |
| Writing / running tests, CI policy | `.agent/rules/testing-ci.md` |
| Local dev environment, build optimizations, worktrees | `.agent/rules/dev-env.md` |

## Common entry points

| Purpose | Command | Notes |
|---|---|---|
| One-time setup after clone (hooks path + testless index) | `just setup` | |
| Affected tests (testless → nextest) | `just test-fast [base]` | default base `origin/main`; loud fallbacks, never a silent skip |
| Full check suite (fmt + clippy + check + affected tests) | `just verify` | run before committing |
| Full workspace tests | `just test` | `cargo nextest run --workspace`; the suite is small, CI also runs it |
| Format / lint / compile | `just fmt` / `just clippy` / `just check` | clippy/check use `--profile ci` (LLVM), same backend as CI |

## 1. Understand the project first

Read these first (five minutes to a global picture):

1. `README.md` crate manifest — the **single source of truth** for crate
   responsibilities and the quick-start facade contract.
2. Root `Cargo.toml` `[workspace.lints.clippy]` — the idiom lints this repo
   **denies** (hand-written combinators, if/else disguise, …). Skim before
   writing code so you do not run into them.
3. `.agent/rules/` — the rule bodies; follow the navigation table above
   instead of reading everything.

### Dependency direction (hard rule)

```
gateway-facade ──> all engines ──> gateway-contract ──> app-contract
```

- Engines depend **only** on the contract crates plus other engines, never the
  reverse; the contract surface must not grow back into implementations.
- `app-contract` / `gateway-contract` are pure vocabulary + minimal ports.
  `app-contract` stays **wasm-safe**: only pure data / serialization deps
  (serde / uuid / chrono / thiserror); tokio / axum / reqwest are forbidden in it.
- The consumer depends on `gateway-facade` and need not name any engine crate;
  keep `GatewayBuilder` insertion points the only extension surface.
- A new crate or a new dependency edge means updating the `README.md` manifest
  **and** the root `Cargo.toml` layout comment in the same change.

## 2. Prepare the development environment

### One task, one worktree

- Never run `git worktree add` from inside a worktree — it nests.
- Push the branch immediately after creating it (`git push -u origin <type>/<slug>`).
- Give subagents **global absolute paths** (`/home/<user>/projects/gateway/.wt/<name>/`);
  never let them guess relative paths.
- `rm -rf .wt/`, `rm -rf .wt/*`, and `git clean` are strictly forbidden.

### Toolchain: pinned nightly, two codegen backends

`rust-toolchain.toml` pins `nightly-2026-08-20` and `.cargo/config.toml` selects
the **Cranelift** backend for the `dev` profile (~2x faster codegen in the local
loop); the `ci` profile stays on **LLVM** — the backend CI validates and that
ships. sccache caches Cranelift artifacts like LLVM's.

- Never run anything that ships on the `dev` profile: release builds, CI lanes
  and pre-push lint all go through `--profile ci` (or the default `release`).
- Never put `codegen-backend` into `Cargo.toml [profile.*]`: the manifest form
  needs `cargo-features = ["codegen-backend"]`, which breaks any stable cargo
  parsing the manifest. The `.cargo/config.toml` form is the only supported one.
- The dev profile ships no debuginfo (`debug = 0`): local backtraces carry no
  file:line; the `ci` profile restores `line-tables-only` for triage.

### Cargo configuration: shared baseline + probe-based optimizations

`.cargo/config.toml` holds only the **shared baseline** (jobs ceiling +
profile-scoped Cranelift). sccache and mold are **never hardcoded** there:
they are injected per machine by sourcing `scripts/dev-env.sh` (env > project
config in cargo's precedence). Check what this machine enables with
`GATEWAY_DEV_ENV_VERBOSE=1 . scripts/dev-env.sh`; details in
`.agent/rules/dev-env.md`.

- Do not `export RUSTC_WRAPPER` or change `jobs` in a session — the repo config
  is the single source.
- With `incremental = false`, same-tree small edits recompile at crate level —
  expected behavior, not a regression; sccache carries cold builds and worktrees.
- Wrap CPU-heavy commands (compile / test / index) in `.githooks/heavy <cmd>` —
  the quota is computed from the core count, never hard-code a CPUQuota number.

## 3. Write Rust code

Follow `.agent/rules/rust-code.md`. Highlights:

- Comments — including doc comments — are written in **English**; explain why, not what.
- Investigate before writing: LSP (`hover` / `goToDefinition` / `findReferences`)
  before touching a public symbol; list every caller, migrate them in the same
  change — no dual-path compatibility.
- Preferred syntax forms: plain `if` / `else` for two-way choices, `match` for
  three-or-more dispatch, one iter chain for collections, Option/Result
  combinators over hand-written match, named functions over closures, nesting
  within 3 levels. The workspace lints deny the hand-written forms — a denied
  lint is a hard build error under `-D warnings`.
- One function does one thing: past roughly 80 lines, split by responsibility.
- Reuse first: find a same-shape implementation in the workspace before writing
  a new one; push cross-layer logic down into the owning crate.

## 4. Test

Follow `.agent/rules/testing-ci.md`. Highlights:

- All tests live in each crate's `tests/` directory — no `#[cfg(test)]` modules
  inside `src/`. Assert **real behavior** — never decorate with `assert!(true)`.
- Every test carries an English comment stating purpose, scenarios and the
  acceptance criterion.
- **GitHub PR CI is the primary test path — never "finish locally".** CI
  runs the committed tree (the repo's `.github/workflows/ci.yml`: lint +
  nextest, `--profile ci`); a PR merges only when it is green. Fix failures on
  the same PR (pull logs with `gh run view <id> --log-failed`). Local
  loops exist for fast feedback on small scopes only — they never stand in
  for CI and never authorize a push.
- **When you do run locally, use `just test-fast`, not bare `cargo test`.**
  testless (or the changed-packages fallback) keeps the run to what the diff
  can actually break; `cargo test` re-runs whole crates. Every fallback is
  loud — a declared degrade to a full-workspace `just test`, never a silent
  skip and never "0 hits" reported as clean.

## 5. Wrap up

### Processes and resources

- Stop the long-lived processes you started and confirm ports are released;
  wait for background job results before closing the session.
- Run long-lived jobs as session-persistent background jobs, never `nohup … &`.

### Worktrees and branches

- Clean up only what **this session** created. Check three things before
  removing anything: merged/pushed, no uncommitted changes, directory belongs
  to the branch being cleaned. If any fails, state the blocker instead of forcing it.
- Order: `git worktree remove <dir>` → `git branch -d <branch>` → delete remote branch.

### Commits and identity

- Conventional commits: title in English, body may be Chinese; one commit does
  one thing — no unrelated changes, formatting noise, or generated files.
- The commit identity is the maintainer account; never override it with
  `git -c user.name=…`, and never add a `Co-authored-by:` bot trailer.
- Run `just verify` before committing — do not wait for a push failure.

### Pull requests

- All work goes through a PR — never commit or push directly to main.
- Title in plain English (conventional-commit style) with a type label; the
  body follows `.github/PULL_REQUEST_TEMPLATE.md` (English headings, Chinese content).
- This is a **published library**: breaking changes to a public API
  (`gateway-facade` surface, contract types) must be called out in the PR body
  (`## Changelog` block) with the migration line for consumers.
- When CI fails, pull the logs (`gh run view <id> --log-failed`) and fix on the
  same PR.

### Merging

- Squash only: `gh pr merge <N> --squash --delete-branch`. `--merge` and
  `--rebase` are forbidden, and so is a local `git merge <branch>` pushed to main.

## 6. Special cautions

### Deleting things

- Confirm a file is really waste before deleting — not merely "looks unused".
- Delete recoverably (`gio trash`); for destructive writes, explain the impact
  and wait for confirmation.

### Secrets and irreversible changes

- Credentials, tokens, private keys: never printed, never committed; examples
  use placeholders (`<API_KEY>`).
- Do not dump whole config files or environments; read only the fields you need.

### Gate findings are work items, not verdicts

1. Read the rule text first; record pending adjudications — do not guess and do not ignore.
2. Fix the root cause: make the constraint hold, not the check stop firing.
3. Gaming fixes are violations even when the gate turns green: editing rule
   files, `--no-verify`, `#[allow(...)]` suppression, `assert!(true)` tests.
4. Leave one written disposition per finding: `rule ID → root cause → fix (file:line)`.

### Limits of your authority

- `AGENTS.md` and `.agent/rules/` are the rules of this repo; when a rule
  conflicts with reality, escalate to the maintainer — never rewrite a rule yourself.
- Do not create issues or restructure epics unless the maintainer explicitly asks.
