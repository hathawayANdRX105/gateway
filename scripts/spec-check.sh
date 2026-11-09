#!/usr/bin/env bash
# spec-check — gateway 的 rust-specs dylint 语法门（预编译库加载）。
#
# 为什么不是 cargo dylint --git/--path 直跑：dylint 用 --target-dir 构建库，
# 但本机 cargo fork（huacnlee/cargo）会重定向 target 目录，库落在 dylint
# 不查找的位置。预编译 + DYLINT_LIBRARY_PATH 加载是实测跑通的形式
#（exit=0），见 .agent/rules/testing-ci.md §3。
#
# gateway 是纯 Rust 库 workspace（无 dioxus），dioxus 专属 lint 在无 dioxus
# 代码的仓里是 no-op，但仍会加载执行——保留它们与 rust-specs 规则库一致，
# 不做硬编码排除（规则库是唯一正本）。
#
# Usage:
#   just spec-check          # 加载预编译库跑 lint 门（缺 nightly/dylint/库 → skip 提示）
#   just spec-check build    # 从 rust-specs (重新) 构建预编译库
#
# 环境变量：
#   RUST_SPECS_DIR   rust-specs 检出目录（默认探测 $HOME/projects/rust-specs）
#   DYLINT_LIB_DIR   预编译库目录（默认 <repo>/target/dylint/libraries/<toolchain>-<triple>/release）
set -uo pipefail

REPO=$(git rev-parse --show-toplevel 2>/dev/null) || REPO="$PWD"
cd "$REPO" || exit 1

NIGHTLY="nightly-2026-08-20"

# ── 工具探测：缺件即 skip（不阻塞流程）─────────────────────────────────────
if ! rustup toolchain list 2>/dev/null | grep -q "$NIGHTLY"; then
    echo "spec-check: 缺 $NIGHTLY（dylint 需要 nightly），门 skip —— 在桌面/CI 补跑"
    exit 0
fi
if ! cargo "+$NIGHTLY" dylint --version >/dev/null 2>&1; then
    echo "spec-check: 缺 cargo-dylint（cargo +$NIGHTLY install cargo-dylint），门 skip"
    exit 0
fi

TC_TRIPLE=$(rustup run "$NIGHTLY" rustc -vV 2>/dev/null | awk '/^host/ {print $2}')
if [ -z "$TC_TRIPLE" ]; then
    echo "spec-check: 无法解析 host triple，门 skip"
    exit 0
fi
LIB_DIR="${DYLINT_LIB_DIR:-$REPO/target/dylint/libraries/$NIGHTLY-$TC_TRIPLE/release}"
SUFFIX="@$NIGHTLY-$TC_TRIPLE.so"

# ── build：从 rust-specs 构建预编译库 ────────────────────────────────────────
if [ "${1:-}" = "build" ]; then
    RS="${RUST_SPECS_DIR:-}"
    [ -d "$RS" ] || RS="$HOME/projects/rust-specs"
    if [ ! -d "$RS" ]; then
        echo "spec-check: rust-specs 未找到（设 RUST_SPECS_DIR 或 clone 到 $HOME/projects/rust-specs）"
        exit 1
    fi
    echo "==> 在 $RS 构建预编译 lints（$NIGHTLY）..."
    # shellcheck disable=SC2164
    (cd "$RS" && cargo "+$NIGHTLY" build --release -q) || { echo "✗ rust-specs build 失败"; exit 1; }
    mkdir -p "$LIB_DIR"
    # 遍历 lints/*/Cargo.toml 的 package name → lib<name_with_underscores>.so，
    # 拷贝并改名为 dylint 加载所需的 lib<name>@<toolchain>.so 形式。
    count=0
    for m in "$RS"/lints/*/Cargo.toml; do
        name=$(awk '/^name *=/ { gsub(/[" ]/, "", $0); split($0, a, "="); print a[2]; exit }' "$m" 2>/dev/null)
        [ -n "$name" ] || continue
        us=$(printf '%s' "$name" | tr '-' '_')
        src="$RS/target/release/lib${us}.so"
        if [ -f "$src" ]; then
            cp "$src" "$LIB_DIR/lib${us}${SUFFIX}"
            count=$((count + 1))
        fi
    done
    echo "✓ 已放置 $count 个预编译 lint 库到 $LIB_DIR"
    exit 0
fi

# ── 默认：加载预编译库逐个跑 ───────────────────────────────────────────────
if [ ! -d "$LIB_DIR" ] || ! ls "$LIB_DIR"/lib*"$SUFFIX" >/dev/null 2>&1; then
    echo "spec-check: 预编译库未就绪（just spec-check build），门 skip —— 桌面/CI 补跑"
    exit 0
fi

names=()
for lib in "$LIB_DIR"/lib*"$SUFFIX"; do
    base=$(basename "$lib")
    n="${base#lib}"
    n="${n%"$SUFFIX"}"
    names+=("$n")
done
echo "==> dylint: ${#names[@]} 个 lint 库（$LIB_DIR）"

rc=0
for n in "${names[@]}"; do
    echo "--- lint: $n ---"
    if ! DYLINT_LIBRARY_PATH="$LIB_DIR" cargo "+$NIGHTLY" dylint --manifest-path Cargo.toml --lib "$n" -q; then
        rc=1
    fi
done
if [ "$rc" -eq 0 ]; then
    echo "✓ spec-check 通过"
else
    echo "✗ spec-check 有 findings（见 .agent/rules/testing-ci.md「gate findings are work items」）"
fi
exit "$rc"
