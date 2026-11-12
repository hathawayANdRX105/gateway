#!/usr/bin/env bash
# bench/scripts/bench_gateway.sh — Gateway 压测脚本
# 用法：./scripts/bench_gateway.sh [scenario_name]
# 依赖：hey (https://github.com/rakyll/hey), jq, python3

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
BENCH_DIR="$(dirname "$SCRIPT_DIR")"
DATA_DIR="$BENCH_DIR/data"
RESULTS_DIR="$BENCH_DIR/results"
SCENARIOS="$DATA_DIR/scenarios.yaml"
# 语料在 config/prompt-templates.jsonl（渠道探活的同一份文件，运行时资产，
# 不进编译）。压测与探活共用，改一处两处生效。
PROMPTS_FILE="$BENCH_DIR/../config/prompt-templates.jsonl"

# 颜色
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

log() { echo -e "${GREEN}[$(date +%H:%M:%S)]${NC} $*"; }
warn() { echo -e "${YELLOW}[$(date +%H:%M:%S)] WARNING:${NC} $*"; }
error() { echo -e "${RED}[$(date +%H:%M:%S)] ERROR:${NC} $*" >&2; }

# 检查依赖
check_deps() {
    local missing=()
    command -v hey >/dev/null 2>&1 || missing+=("hey")
    command -v jq >/dev/null 2>&1 || missing+=("jq")
    command -v python3 >/dev/null 2>&1 || missing+=("python3")
    if [ ${#missing[@]} -gt 0 ]; then
        error "缺少依赖: ${missing[*]}"
        echo "安装 hey: go install github.com/rakyll/hey@latest"
        exit 1
    fi
}

# 从语料里随机抽一条组装请求体。
# 场景的 model / stream 覆盖语料里的同名字段：语料只提供问题，不绑定部署的模型别名。
random_prompt() {
    local prompt_file="$1"
    local model_override="${2:-}"
    local stream_override="${3:-}"
    python3 - "$prompt_file" "$model_override" "$stream_override" << 'PYEOF'
import json
import random
import sys

prompt_file, model_override, stream_override = sys.argv[1], sys.argv[2], sys.argv[3]
prompts = []
with open(prompt_file) as f:
    for line in f:
        line = line.strip()
        if not line:
            continue
        try:
            prompts.append(json.loads(line))
        except json.JSONDecodeError:
            continue

if not prompts:
    prompts = [{"messages": [{"role": "user", "content": "hi"}]}]

p = random.choice(prompts)
msg = p.get("messages", [{"role": "user", "content": "hi"}])
model = model_override or p.get("model", "gpt-4o")
stream = (stream_override == "true") if stream_override else bool(p.get("stream", False))

result = {"model": model, "messages": msg}
if stream:
    result["stream"] = True

print(json.dumps(result, ensure_ascii=False))
PYEOF
}

# 解析 yaml（用 python3，避免依赖 yq）
parse_yaml() {
    python3 -c "
import yaml, sys, json
with open('$SCENARIOS') as f:
    data = yaml.safe_load(f)
print(json.dumps(data, ensure_ascii=False))
"
}

# 获取场景配置
get_scenario() {
    local name="$1"
    parse_yaml | python3 -c "
import json, sys
data = json.load(sys.stdin)
if '$name' not in data.get('scenarios', {}):
    print('SCENARIO_NOT_FOUND', file=sys.stderr)
    sys.exit(1)
print(json.dumps(data['scenarios']['$name'], ensure_ascii=False))
"
}

# 获取全局配置
get_global() {
    parse_yaml | python3 -c "
import json, sys
data = json.load(sys.stdin)
print(json.dumps(data.get('global', {}), ensure_ascii=False))
"
}

# 运行单个场景
run_scenario() {
    local scenario_name="$1"
    local scenario_json
    scenario_json=$(get_scenario "$scenario_name") || { error "场景不存在: $scenario_name"; return 1; }

    local global_json
    global_json=$(get_global)

    # 解析配置
    local gateway_url model duration rps max_concurrent timeout stream
    gateway_url=$(echo "$global_json" | jq -r '.gateway_url // "http://127.0.0.1:8080"')
    local api_key
    api_key=$(echo "$global_json" | jq -r '.api_key // "sk-replace-me"')

    model=$(echo "$scenario_json" | jq -r '.model // .models[0] // "gpt-4o"')
    duration=$(echo "$scenario_json" | jq -r '.duration_seconds // 60')
    rps=$(echo "$scenario_json" | jq -r '.rate.requests_per_second // 2')
    max_concurrent=$(echo "$scenario_json" | jq -r '.rate.max_concurrent // 5')
    timeout=$(echo "$scenario_json" | jq -r '.timeout_ms // 30000')
    stream=$(echo "$scenario_json" | jq -r '.stream // false')
    local abort_on_429
    abort_on_429=$(echo "$scenario_json" | jq -r '.abort_on_429 // true')

    local desc
    desc=$(echo "$scenario_json" | jq -r '.description // ""')

    log "========================================"
    log "场景: $scenario_name"
    log "描述: $desc"
    log "模型: $model"
    log "时长: ${duration}s | RPS: $rps | 并发: $max_concurrent"
    log "流式: $stream | 429 退避: $abort_on_429"
    log "========================================"

    # 输出文件
    local timestamp
    timestamp=$(date +%Y%m%d_%H%M%S)
    local result_dir="$RESULTS_DIR/${scenario_name}_${timestamp}"
    mkdir -p "$result_dir"

    local hey_out="$result_dir/hey_output.csv"
    local gateway_log_before="$result_dir/gateway_log_before.log"
    local gateway_log_after="$result_dir/gateway_log_after.log"
    local analysis_out="$result_dir/analysis.json"

    # 压测期间的 gateway 日志位置（可选）。gatewayd 默认写 stdout，需要重定向到文件
    # 并在 scenarios.yaml 的 global.gateway_log 指向它；为空时跳过日志分析。
    local gw_log_file
    gw_log_file=$(echo "$global_json" | jq -r '.gateway_log // empty')

    # 清空调度前的日志（如果存在）
    if [ -n "$gw_log_file" ] && [ -f "$gw_log_file" ]; then
        cp "$gw_log_file" "$gateway_log_before"
    fi

    # 构建请求体（随机选 prompt，场景的 model/stream 覆盖语料字段）
    local body_file="$result_dir/request_body.json"
    random_prompt "$PROMPTS_FILE" "$model" "$stream" > "$body_file"
    log "使用 prompt: $(head -c 100 < "$body_file")..."

    log "开始压测..."

    # 用 hey 跑压测
    local total_requests=$((rps * duration))

    # 运行 hey
    hey \
        -m POST \
        -H "content-type: application/json" \
        -H "authorization: Bearer $api_key" \
        -d @"$body_file" \
        -n "$total_requests" \
        -c "$max_concurrent" \
        -q "$rps" \
        -o csv \
        -t "$((timeout / 1000))" \
        "$gateway_url/v1/chat/completions" > "$hey_out" 2>&1 || true

    log "压测完成，结果保存到: $result_dir"

    # 收集压测后的日志
    if [ -n "$gw_log_file" ] && [ -f "$gw_log_file" ]; then
        cp "$gw_log_file" "$gateway_log_after"
    fi

    # 分析结果
    analyze_results "$result_dir" "$scenario_name" "$model" "$duration" "$rps" "$gw_log_file"
}

# 分析结果
analyze_results() {
    local result_dir="$1"
    local scenario="$2"
    local model="$3"
    local duration="$4"
    local rps="$5"
    local gateway_log="$6"

    local hey_out="$result_dir/hey_output.csv"
    local analysis_out="$result_dir/analysis.json"

    log "分析结果..."

    # 用 python3 分析 hey 输出 + gateway 日志
    python3 - "$result_dir" "$scenario" "$model" "$duration" "$rps" "$hey_out" "$analysis_out" "$gateway_log" << 'PYEOF'
import json
import sys
import os
import re
from datetime import datetime

result_dir = sys.argv[1]
scenario = sys.argv[2]
model = sys.argv[3]
duration = int(sys.argv[4])
rps = int(sys.argv[5])
hey_out = sys.argv[6]
analysis_out = sys.argv[7]

# 解析 hey CSV 输出
latencies = []
status_codes = {}
errors = []
total_requests = 0

with open(hey_out, 'r') as f:
    lines = f.readlines()

# hey CSV 格式: "1",0.123,200,1234 (序号, 延迟秒, 状态码, 响应大小)
for line in lines:
    line = line.strip().strip('"')
    if not line:
        continue
    parts = line.split(',')
    if len(parts) >= 3:
        try:
            latency_ms = float(parts[1]) * 1000
            status = int(parts[2])
            latencies.append(latency_ms)
            status_codes[status] = status_codes.get(status, 0) + 1
            total_requests += 1
        except (ValueError, IndexError):
            pass

# 计算统计
latencies.sort()
n = len(latencies)

if n > 0:
    avg_latency = sum(latencies) / n
    p50 = latencies[int(n * 0.5)]
    p95 = latencies[int(n * 0.95)]
    p99 = latencies[int(n * 0.99)]
    min_latency = latencies[0]
    max_latency = latencies[-1]
else:
    avg_latency = p50 = p95 = p99 = min_latency = max_latency = 0

# 解析 gateway 日志（未配置时为跳过）
gateway_log = sys.argv[8] if len(sys.argv) > 8 else ""
log_entries = []
if gateway_log and os.path.exists(gateway_log):
    with open(gateway_log, 'r') as f:
        for line in f:
            if "request completed" in line:
                try:
                    entry = {}
                    for match in re.finditer(r'(\w+)=("([^"]*)"|(\S+))', line):
                        key = match.group(1)
                        val = match.group(3) if match.group(3) is not None else match.group(4)
                        entry[key] = val
                    if entry:
                        log_entries.append(entry)
                except Exception:
                    pass

# 输出分析
analysis = {
    "scenario": scenario,
    "model": model,
    "timestamp": datetime.now().isoformat(),
    "config": {
        "duration_seconds": duration,
        "target_rps": rps,
    },
    "results": {
        "total_requests": total_requests,
        "successful_requests": status_codes.get(200, 0),
        "failed_requests": total_requests - status_codes.get(200, 0),
        "status_codes": {str(k): v for k, v in status_codes.items()},
        "latency_ms": {
            "avg": round(avg_latency, 2),
            "p50": round(p50, 2),
            "p95": round(p95, 2),
            "p99": round(p99, 2),
            "min": round(min_latency, 2),
            "max": round(max_latency, 2),
        },
        "actual_rps": round(total_requests / duration, 2) if duration > 0 else 0,
    },
    "gateway_log_entries": len(log_entries),
}

with open(analysis_out, 'w') as f:
    json.dump(analysis, f, indent=2, ensure_ascii=False)

print(f"分析完成: {analysis_out}")
print(f"总请求: {total_requests}, 成功: {status_codes.get(200, 0)}, 失败: {total_requests - status_codes.get(200, 0)}")
print(f"延迟: avg={avg_latency:.1f}ms p50={p50:.1f}ms p95={p95:.1f}ms p99={p99:.1f}ms")
PYEOF

    log "分析完成: $analysis_out"
}

# 列出所有场景
list_scenarios() {
    log "可用场景:"
    parse_yaml | python3 -c "
import json, sys
data = json.load(sys.stdin)
for name, cfg in data.get('scenarios', {}).items():
    enabled = cfg.get('enabled', False)
    desc = cfg.get('description', '')
    model = cfg.get('model', cfg.get('models', ['-']))
    status = '✅' if enabled else '❌'
    print(f'  {status} {name}: {desc} (model={model})')
"
}

# 主函数
main() {
    check_deps

    local scenario="${1:-}"

    if [ -z "$scenario" ]; then
        echo "用法: $0 <scenario_name>"
        echo ""
        list_scenarios
        echo ""
        echo "示例: $0 baseline"
        exit 0
    fi

    if [ "$scenario" = "all" ]; then
        # 运行所有启用的场景
        local scenarios
        scenarios=$(parse_yaml | python3 -c "
import json, sys
data = json.load(sys.stdin)
enabled = [name for name, cfg in data.get('scenarios', {}).items() if cfg.get('enabled', False)]
print(' '.join(enabled))
")
        for s in $scenarios; do
            run_scenario "$s"
            log "场景 $s 完成，冷却 10s..."
            sleep 10
        done
    else
        run_scenario "$scenario"
    fi

    log "全部完成！结果在: $RESULTS_DIR"
}

main "$@"
