#!/usr/bin/env sh
# dev-env.sh — 按本机能力探测式注入 cargo 优化（source 进 shell 用）。
#
# 为什么要探测而不是写死在 .cargo/config.toml：
#   config.toml 是仓库共享文件，写死 sccache/mold 会让没装这些工具的机器
#   cargo 启动即报错。本脚本只做「有就启用」的 export，没探到就什么也不
#   注入——同一份文件，桌面启用优化，裸机自动降级裸跑，无需 fork。
#   （cranelift 不在这里：它是 workspace 级构建约定，走 .cargo/config.toml
#   + rust-toolchain.toml 的 pinned nightly，与机器能力无关。）
#
# 用法（加到 ~/.bashrc 或 ~/.zshrc）:
#   . "$(git rev-parse --show-toplevel 2>/dev/null)/scripts/dev-env.sh"
# 或绝对路径 / 直接 source 本文件。

# 供 GATEWAY_DEV_ENV_VERBOSE=1 打印探测结果；默认静默，不污染每次开终端的输出。
_gateway_env_note() {
    if [ "${GATEWAY_DEV_ENV_VERBOSE:-}" = "1" ]; then
        printf 'dev-env: %s\n' "$1" >&2
    fi
}

# ── sccache: 编译缓存（跨会话/跨 worktree 命中）──────────────────────────────
if command -v sccache >/dev/null 2>&1; then
    export CARGO_BUILD_RUSTC_WRAPPER=sccache
    # 本机 cgroup 隔离: 让编译跑在调用方会话的 cgroup 而不是共享 sccache.service
    # (那台机器的 systemd 给 service 设了机器级 CPUQuota，多会话会互相抢)。
    # 无 systemd 的环境导出也无害，sccache 自己不会因此失败。
    export SCCACHE_CLIENT_SIDE=1
    _gateway_env_note "sccache: on (CARGO_BUILD_RUSTC_WRAPPER)"
else
    _gateway_env_note "sccache: off (未安装, 裸 rustc)"
fi

# ── mold 链接器: 仅 x86_64 Linux 且 mold/clang 都在才启用 ────────────────────
if [ "$(uname -m)" = "x86_64" ] && command -v mold >/dev/null 2>&1 && command -v clang >/dev/null 2>&1; then
    export CARGO_TARGET_X86_64_UNKNOWN_LINUX_GNU_LINKER=clang
    export CARGO_TARGET_X86_64_UNKNOWN_LINUX_GNU_RUSTFLAGS="-C link-arg=--ld-path=/usr/bin/mold"
    _gateway_env_note "mold linker: on (clang + --ld-path=mold)"
else
    _gateway_env_note "mold linker: off (非 x86_64 或缺 mold/clang, 系统默认链接器)"
fi

# 幂等: 重复 source 只会重新探测一遍，export 覆盖自身，无需 unset 守卫。
unset -f _gateway_env_note
