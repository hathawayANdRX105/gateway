#!/usr/bin/env bash
# spec-check — gateway 本地 dylint 语法门（metadata entry，git-tag 加载）。
#
# CI 直接 `cargo dylint --all`（标准 cargo 没有本机问题）。本地因 huacnlee
# cargo fork 重定向 target 目录，dylint 自建库会落到查找路径之外——本脚本
# 用 PATH=/usr/bin 前缀的系统 cargo 跑同一命令作为本地入口；缺环境时 skip
# 并 exit 0（CI 才是主门）。规则库经 root Cargo.toml 的
# [workspace.metadata.dylint] 接入，tag 固定 lints-v2026.11.21；升级规则
# = 改 tag（rust-specs-projects.md §6）。
# gateway 是纯 Rust 库 workspace（无 dioxus），dioxus 专属 lint 在无 dioxus
# 代码的仓里是 no-op，但仍会加载执行——保留它们与 rust-specs 规则库一致，
# 不做硬编码排除（规则库是唯一正本）。
#
# Usage:
#   just spec-check          # 本地跑 dylint 门（缺 nightly/dylint → skip 提示）
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

# ── 本地跑：metadata entry 加载，系统 cargo 规避 fork 重定向 ────────────────
# 本机默认 cargo 是 huacnlee fork（~/.local/bin/cargo），会把 target 目录
# 重定向到 dylint 查找路径之外。PATH=/usr/bin 前缀强制用系统标准 cargo 跑，
# 与 CI 同一入口（cargo dylint --all，规则库从 root Cargo.toml 的
# [workspace.metadata.dylint] tag 拉取）。
rc=0
if PATH="/usr/bin:$PATH" cargo "+$NIGHTLY" dylint --all --workspace; then
    echo "✓ spec-check 通过"
else
    echo "✗ spec-check 有 findings（见 .agent/rules/testing-ci.md「gate findings are work items」）"
    rc=1
fi
exit "$rc"


