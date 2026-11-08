# gateway — justfile
#
# 本机级优化（sccache/mold）经 scripts/dev-env.sh 探测注入，不在这里硬编码；
# CPU 密集命令在有多会话压力的机器上套 .githooks/heavy（见 AGENTS.md）。

default: check

# Build check
check:
    cargo check --workspace --all-targets

# Format
fmt:
    cargo fmt --all

# Format check (no file changes)
fmt-check:
    cargo fmt --all -- --check

# Clippy (same backend as CI: LLVM via --profile ci)
clippy:
    cargo clippy --workspace --all-targets --profile ci -- -D warnings

# Build binary/libraries
build:
    cargo build --workspace

# Full test run (workspace). Daily entry is `just test-fast`; full runs belong
# to CI, local is fine too — this library suite is small.
test:
    cargo nextest run --workspace

# Testless test selection (function-level impact analysis). Usage: just test-fast [base]
test-fast base="origin/main":
    bash scripts/test-fast.sh "{{base}}"

# Full check suite: fmt + clippy + check + affected tests.
verify: fmt-check clippy check test-fast

# One-time setup after cloning: hooks path + testless index.
setup:
    git config core.hooksPath .githooks
    command -v testless >/dev/null && .githooks/heavy testless index || echo "(testless not installed; test-fast falls back to full runs)"
