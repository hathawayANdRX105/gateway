# Local Development Environment

**When to read this**: when starting work in this repo, when a build feels slow or always cold, when configuring a new machine, or when processes/worktrees pile up.

**What this solves**: gateway's dev optimizations are optional and probe-based. This rule tells you where they live, and the multi-session process hygiene that applies regardless of machine.

## 1. Toolchain optimizations (read the script header first)

The probe script is `scripts/dev-env.sh` — read its header comment for why each probe is written the way it is. Summary of the model:

- `.cargo/config.toml` holds the shared baseline only: the `jobs` ceiling and the profile-scoped Cranelift backend (a workspace build contract, not a machine detail). Never hardcode `sccache` / `mold` into it — machines lacking the tool then fail to start cargo.
- Per-machine optimizations (sccache, mold) are injected by sourcing `scripts/dev-env.sh`, which probes and exports env vars (env > project config in cargo's precedence). A machine with the tools enables them; a machine without them probes nothing and silently runs plain cargo.
- Command-path choices (nextest vs `cargo test`, testless vs full run) happen at run time via `command -v` in justfile/scripts.

Before building or testing, verify what this machine enables:

```sh
GATEWAY_DEV_ENV_VERBOSE=1 . scripts/dev-env.sh
```

## 2. Toolchain notes

- `rust-toolchain.toml` pins `nightly-2026-08-20` — required by Cranelift (`[profile.dev] codegen-backend` is unstable cargo). The split: **dev profile = Cranelift** (~2x faster host codegen in the local loop, no debuginfo), **ci profile = LLVM** (`--profile ci`, the backend CI validates and that ships). sccache caches Cranelift artifacts like LLVM's.
- Never put `codegen-backend` into `Cargo.toml [profile.*]`: the manifest form needs `cargo-features`, which breaks stable cargo parsing the manifest (this repo's manifests are parsed by consumers on stable toolchains).
- With `incremental = false`, same-tree small edits recompile at crate level — expected behavior, not a regression; sccache carries cold builds, branch switches and worktrees.
- nextest config, if any test-group serialization is needed, lives at `.config/nextest.toml` — that path is fixed by nextest, not movable into `.cargo/`.

## 3. Process hygiene (multi-session)

- Run long-lived processes (file watchers, mock upstreams) as **session-persistent background jobs** — never `nohup ... &` (the process group is reaped when the tool call ends, so the job silently dies).
- `pkill -f cargo|rustc` is forbidden — those may be other sessions' builds. Identify processes via `readlink /proc/<pid>/cwd` first; resume T-state builds with `kill -CONT` only after establishing ownership.
- Stop builds inside a worktree **before** `git worktree remove`; orphaned builds (cwd `(deleted)`) slow down every later session.
- Wrap CPU-heavy commands (compile / test / index) in `.githooks/heavy <cmd>`. On machines with systemd it applies a cgroup CPU quota derived from the core count — never hard-code a CPUQuota number; on machines without systemd it runs the command directly.

## 4. Worktrees

- One task, one worktree: `git worktree add .wt/<name> -b <type>/<slug>`; never run `git worktree add` from inside a worktree (it nests).
- Push the branch immediately after creating it (`git push -u origin <type>/<slug>`).
- Never `rm -rf .wt/` or `git clean` — they wipe other sessions' worktrees too.

## 5. Setup checklist (new clone)

```sh
just setup                       # git config core.hooksPath .githooks + testless index
. scripts/dev-env.sh             # sccache / mold probes (add to your shell rc)
just test-fast                   # the daily validation entry
```
