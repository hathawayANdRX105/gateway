# gateway — development constraints and workflow

This file carries **development constraints and workflow rules only**. Project
facts — the crate manifest, crate responsibilities, the transport, the current
dependency edges, the config surface — are the README's job and are updated
there in the same change that touches them. Expanded procedures live under
`.agent/rules/`; look them up when needed instead of reading them end to end.

| Scenario                                               | Read                         |
| ------------------------------------------------------- | ---------------------------- |
| Project facts (crates, transport, dependency edges, config) | `README.md` — single source of truth |
| Rust public API / code style / investigation and review | `.agent/rules/rust-code.md`  |
| Writing / running tests, CI policy                      | `.agent/rules/testing-ci.md` |
| Local dev environment, build optimizations, worktrees   | `.agent/rules/dev-env.md`    |

## Common entry points

| Purpose                                                  | Command                                   | Notes                                                                                         |
| -------------------------------------------------------- | ----------------------------------------- | --------------------------------------------------------------------------------------------- |
| One-time setup after clone (hooks path + testless index) | `just setup`                              |                                                                                               |
| Affected tests (testless → nextest)                      | `just test-fast [base]`                   | default base `origin/main`; loud fallbacks, never a silent skip                               |
| Rust syntax lint (rust-specs dylint)                     | `just spec-check`                         | first time: `just spec-check build`; skips with a notice when nightly/dylint/libs are missing |
| Full check suite (fmt + clippy + check + spec-check)     | `just verify`                             | run before committing                                                                         |
| Full workspace tests                                     | `just test`                               | `cargo nextest run --workspace`; the suite is small, CI also runs it                          |
| Format / lint / compile                                  | `just fmt` / `just clippy` / `just check` | clippy/check use `--profile ci` (LLVM), same backend as CI                                    |

## 1. Read before working

1. `README.md` — the **single source of truth** for what the crates are, how
   they depend on each other, and the quick-start facade contract. If README
   and your mental model disagree, README wins; update your model, not the doc.
2. Root `Cargo.toml` `[workspace.lints.clippy]` — the idiom lints this repo
   **denies** (hand-written combinators, if/else disguise, …). Skim before
   writing code so you do not run into them.
3. `.agent/rules/` — the rule bodies; follow the navigation table above
   instead of reading everything.

### Dependency direction (hard rule)

The README states the current edges; the constraint itself does not move:

- A consumer depends on the facade crate and names nothing else; the facade's
  insertion points stay the only extension surface.
- Engine crates depend on the shared vocabulary crate plus other engines,
  never the reverse. The vocabulary surface must not grow back into
  implementations, and it stays free of server-framework dependencies.
- The standalone process depends on the facade plus its transport crates; it
  must never name an engine crate directly. If a process-level need seems to
  require an engine type, re-export it from the facade first.
- A new crate or a new dependency edge updates the `README.md` manifest
  **and** the root `Cargo.toml` layout comment in the same change.

### Transport (hard rule)

One HTTP stack for every surface. The library's pipeline and every consumer
that embeds it, and the standalone process, speak the same framework and the
same `http` major. Introducing a second server framework or a second `http`
major into the tree is a design decision that needs the maintainer's
sign-off — not a convenience.

## 2. Prepare the development environment

### One task, one worktree

- Never run `git worktree add` from inside a worktree — it nests.
- Push the branch immediately after creating it (`git push -u origin <type>/<slug>`).
- Give subagents **global absolute paths** (`/home/<user>/projects/gateway/.wt/<name>/`);
  never let them guess relative paths.
- `rm -rf .wt/`, `rm -rf .wt/*`, and `git clean` are strictly forbidden.

### Toolchain: pinned toolchain, two codegen backends

`rust-toolchain.toml` pins the toolchain; `.cargo/config.toml` selects the
**Cranelift** backend for the `dev` profile (~2x faster codegen in the local
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
  runs the committed tree (the repo's `.github/workflows/ci.yml`: `lint` +
  `spec-check` (rust-specs dylint) + `test`; PR lane runs `test-fast`, the
  main-push lane runs the full workspace, all `--profile ci`); a PR merges
  only when it is green. Fix failures on the same PR (pull logs with
  `gh run view <id> --log-failed`). Local loops exist for fast feedback on
  small scopes only — they never stand in for CI and never authorize a push.
- **When you do run locally, use `just test-fast`, not bare `cargo test`.**
  testless (or the changed-packages fallback) keeps the run to what the diff
  can actually break; `cargo test` re-runs whole crates. Every fallback is
  loud — a declared degrade to a full-workspace `just test`, never a silent
  skip and never "0 hits" reported as clean.
- **Run `just spec-check` (rust-specs dylint) before pushing** — it is wired
  into `.githooks/pre-push` and the CI `spec-check` job. First time on a
  machine: `just spec-check build`.

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
  (the facade surface, vocabulary types) must be called out in the PR body
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
