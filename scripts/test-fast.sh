#!/usr/bin/env bash
# 快关：跑 testless 判定受影响的测试（testless 选区 → nextest 收窄跑）。
#
# 用法: just test-fast [base]（默认 origin/main）
#
# 判定契约（与 ferrite 的 scripts/test-fast.sh 同族，但 gateway 是纯库工作区、
# 全量跑负担小，所以「本地禁止全量」收紧为「本地允许全量但必须显式出声」）：
#
#   ① testless 不可用 / 输出异常      → 出声降级：全仓 nextest
#   ② exit=2 = testless 判 run-all    → 全仓 nextest
#   ③ 选出名单但 0 通过（零命中可疑） → 出声降级：全仓 nextest
#   ④ 名单为空且 git diff 非空        → 按「改动包 ∪ 反向依赖」的闭包跑
#      （编译期可证的界：改包 X 只可能影响传递依赖 X 的包）
#
# 判定基准一律取 **merge-base**（三点 base...HEAD），不是 base 的当前 tip：
# testless 的 `--from <rev>` 是「<rev> 对工作树」的两方 diff，base 的 tip 会在
# 开发期间往前走，两方 diff 可能为空 → 快关假绿（ferrite 实测教训，PR #399）。
#
# 失败一律不静默：任何降级都先打印原因，且降级跑的是全量——宁可多跑，
# 不许「什么都没跑却报绿」。
set -uo pipefail
cd "$(git rev-parse --show-toplevel)" || exit 1

base="${1:-origin/main}"

# nextest 的 cargo profile：本地默认空（dev = Cranelift，快）；CI 设 TEST_FAST_PROFILE=ci
#（LLVM，与合并门同一后端）。nextest 用 --cargo-profile 透传给 cargo。
nextest_profile="${TEST_FAST_PROFILE:-}"
if [ -n "$nextest_profile" ]; then
  profile_arg=(--cargo-profile "$nextest_profile")
else
  profile_arg=()
fi

# ── 判定基准：merge-base ─────────────────────────────────────────────────────
base_rev="$(git merge-base "$base" HEAD 2>/dev/null || true)"
if [ -z "$base_rev" ]; then
  echo "⚠️  git merge-base ${base} HEAD 取不到 → 保留原引用，交由 testless 判定"
  base_rev="$base"
fi

# 全量跑（所有降级路径的落点）。nextest 而非 cargo test：process-per-test、
# 并行 test binary，且 filterset 与收窄跑同一口径。
full_run() {
  echo "↩︎ 降级全量：$1"
  echo ">>> cargo nextest run --workspace ${profile_arg[*]}"
  out="$(cargo nextest run --workspace "${profile_arg[@]}" 2>&1)" && rc=0 || rc=$?
  printf '%s\n' "$out"
  # 空 workspace：nextest 报 "no tests to run" 是合法空态，不是失败。
  # 只转这个明确信号（带提示），真实测试失败仍传播——不制造假绿。
  if [ "$rc" -ne 0 ] && printf '%s' "$out" | grep -q 'no tests to run'; then
    echo "✓ workspace 无测试 —— 无可跑用例"
    exit 0
  fi
  exit "$rc"
}

# 解析 nextest Summary 的通过数。**先去 ANSI 色**再匹配：CI 设
# CARGO_TERM_COLOR=always 时 `grep '[0-9]+ passed'` 永不命中，会把每条 PR
# 误判成零命中（ferrite 壳仓实测教训，run 41297721679）。
passed_count() {
  printf '%s\n' "$1" | sed -E 's/\x1b\[[0-9;]*m//g' \
    | grep -oE '[0-9]+ passed' | tail -1 | grep -oE '[0-9]+' || true
}

# ── ① testless 缺席 ──────────────────────────────────────────────────────────
if ! command -v testless >/dev/null 2>&1; then
  full_run "testless 不可用（安装 testless 并 `testless index` 可启用收窄跑）"
  exit $?
fi

err="$(mktemp)"
trap 'rm -f "$err"' EXIT
json="$(testless select --from "$base_rev" 2>"$err")" && rc=0 || rc=$?

# ── ② run-all（影响面 = 全仓）───────────────────────────────────────────────
if [ "$rc" -eq 2 ]; then
  reason="$(printf '%s' "$json" | jq -r '.reason // empty' 2>/dev/null || true)"
  full_run "testless 判定 run-all（vs ${base_rev}）：${reason:-（JSON 未给 reason）}"
  exit $?
fi

# ── ① testless 异常输出 ──────────────────────────────────────────────────────
if [ "$rc" -ne 0 ] || ! printf '%s' "$json" | jq -e . >/dev/null 2>&1; then
  if [ -s "$err" ]; then echo "testless stderr:"; sed 's/^/    /' "$err"; fi
  full_run "testless 输出异常（exit=${rc}），建议 \`testless index\` 重建索引后重试"
  exit $?
fi

names="$(printf '%s\n' "$json" | jq -r '.tests[]? | .name | last' | sort -u | tr '\n' ' ')"

# ── ④ 空选区：按包闭包兜底 ───────────────────────────────────────────────────
# 改动文件集 = 「base..HEAD 的提交」∪「工作区未提交 ∪ 未跟踪」，与 testless
# 的 --from 比较面一致。
if [ -z "${names// }" ]; then
  files="$(
    { git diff --name-only "${base_rev}"...HEAD 2>/dev/null
      git diff --name-only HEAD 2>/dev/null
      git ls-files --others --exclude-standard 2>/dev/null
    } | sed '/^$/d' | sort -u
  )"
  # 非 Rust 构建面（文档/注释/CI yml）不动编译产物 → 无需测试。
  rust_files="$(printf '%s\n' "$files" | grep -E '\.rs$' || true)"
  if [ "$(printf '%s\n' "$files" | grep -cE '^Cargo\.(toml|lock)$|^\.cargo/' || true)" -gt 0 ]; then
    full_run "根级构建清单/.cargo 变动，闭包不可靠"
    exit $?
  fi
  if [ -z "$rust_files" ]; then
    echo "✓ 无 Rust 构建面改动（vs ${base_rev}），无需跑测试"
    exit 0
  fi
  # 闭包 = 改动包 ∪（传递地）依赖它们的包。目录名与包名一致（crates/<pkg>）。
  meta="$(cargo metadata --no-deps --format-version 1)" || {
    full_run "cargo metadata 失败，闭包推不出来"; exit $?; }
  seed="$(printf '%s\n' "$rust_files" | sed -nE 's#^crates/([^/]+)/.*$#\1#p' | sort -u | tr '\n' ' ')"
  if [ -z "${seed// }" ]; then
    full_run "改动的 .rs 不在 crates/ 工作区内，闭包推不出来"
    exit $?
  fi
  current="$seed"
  while :; do
    nxt="$(printf '%s' "$meta" | jq -r --arg cur "$current" \
      '.packages[] | select(.dependencies | map(.name) | any(. as $d | $cur | split(" ") | index($d) != null)) | .name' \
      | sort -u | tr '\n' ' ')"
    merged="$(printf '%s %s\n' "$current" "$nxt" | tr ' ' '\n' | sed '/^$/d' | sort -u | tr '\n' ' ')"
    [ "$merged" = "$current" ] && break
    current="$merged"
  done
  echo "无 testless 选区 → 按包闭包跑（可证明的界）：$(printf '%s' "$current" | tr ' ' ',')"
  pargs=(); for p in $current; do pargs+=(-p "$p"); done
  echo ">>> cargo nextest run ${pargs[*]}"
  cargo nextest run "${pargs[@]}"
  exit $?
fi

# ── 收窄跑：filterset 精确匹配 ───────────────────────────────────────────────
expr=""
for n in $names; do
  [ -n "$expr" ] && expr="$expr | "
  expr="${expr}test(=${n})"
done
echo "testless 选出 $(printf '%s' "$names" | wc -w) 条用例（vs ${base_rev}）"
# --no-tests=pass：命中 0 条时 nextest 默认非零退出；这里要靠解析出的 passed
# 数自行判「零命中可疑」（③），由本脚本决定是否降级。
out="$(cargo nextest run --no-tests=pass -E "$expr" "${profile_arg[@]}" 2>&1)" && rc=0 || rc=$?
printf '%s\n' "$out"
if [ "$rc" -ne 0 ]; then
  echo "✗ 收窄跑失败（filterset 精确匹配用例红）"
  exit 1
fi
passed="$(passed_count "$out")"; passed="${passed:-0}"
if [ "$passed" -eq 0 ]; then
  # ③ 选出了名单但一条没跑 → 可疑（索引或口径问题），不许静默通过。
  full_run "零命中可疑：选了 $(printf '%s' "$names" | wc -w) 条但一条没跑，建议 \`testless index\` 重建索引"
  exit $?
fi
echo "✓ 收窄跑通过（passed=${passed}）"
