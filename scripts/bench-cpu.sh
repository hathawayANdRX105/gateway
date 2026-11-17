#!/usr/bin/env bash
# scripts/bench-cpu.sh — best-effort CPU sampling for the benchmark suite.
#
# Profiles one Criterion bench process with `perf record`, converts the samples to
# folded stacks, and writes benches/perf-folded.txt for benches/bench_report.py.
# This answers "which function inside the hot bench is expensive", which the
# latency table alone cannot. Sampling needs perf_event access: GitHub runners
# allow lowering perf_event_paranoid with sudo; desktop hosts may not. The script
# degrades with a clear reason instead of failing the benchmark run.
#
# Usage: bash scripts/bench-cpu.sh [bench-name] [sample-seconds]
set -uo pipefail
cd "$(git rev-parse --show-toplevel)" || exit 1

bench="${1:-cache_ops}"
crate="${2:-gateway-cache}"
seconds="${3:-15}"
out="benches/perf-folded.txt"
status="benches/perf-status.txt"
: > "$out"
status_note() { printf '%s\n' "$1" > "$status"; }
status_note "pending"

log() { echo "[bench-cpu] $*"; }

if ! command -v perf >/dev/null 2>&1; then
    msg="perf is not installed on this host; CPU sample table left empty."
    log "$msg"
    status_note "$msg"
    exit 0
fi

paranoid=$(cat /proc/sys/kernel/perf_event_paranoid 2>/dev/null || echo "unknown")
log "perf_event_paranoid=$paranoid"
if [ "$paranoid" != "unknown" ] && [ "$paranoid" -gt 1 ] 2>/dev/null; then
    if command -v sudo >/dev/null 2>&1; then
        sudo sysctl -w kernel.perf_event_paranoid=1 >/dev/null 2>&1 || true
    fi
    paranoid=$(cat /proc/sys/kernel/perf_event_paranoid 2>/dev/null || echo "unknown")
fi
if [ "$paranoid" != "unknown" ] && [ "$paranoid" -gt 1 ] 2>/dev/null; then
    msg="perf_event_paranoid=$paranoid denies unprivileged sampling and could not be lowered; CPU sample table left empty"
    log "$msg"
    status_note "$msg"
    exit 0
fi

log "compiling bench $crate/$bench and resolving its release binary"
bin=$(cargo bench -p "$crate" --bench "$bench" --no-run --message-format=json 2>/dev/null \
    | python3 -c 'import json,sys
for line in sys.stdin:
    try: obj=json.loads(line)
    except ValueError: continue
    exe=obj.get("executable")
    if exe and obj.get("profile", {}).get("test") and "release" in exe:
        print(exe); break' || true)
if [ -z "$bin" ] || [ ! -x "$bin" ]; then
    msg="cannot locate release bench binary for $crate/$bench; CPU sample table left empty."
    log "$msg"
    status_note "$msg"
    exit 0
fi
log "bench binary: $bin"

# --profile-time keeps Criterion in its measurement loop so perf has a live target.
log "starting $bench with --profile-time ${seconds}s"
"$bin" --profile-time "$seconds" >/tmp/bench-cpu-run.log 2>&1 &
bench_pid=$!
sleep 3

if ! kill -0 "$bench_pid" 2>/dev/null; then
    msg="bench process exited before sampling; see /tmp/bench-cpu-run.log"
    log "$msg"
    status_note "$msg"
    exit 0
fi

perf_data=/tmp/bench-cpu-$bench.data
log "recording ${seconds}s of call-graph samples"
if ! perf record -F 99 -g -o "$perf_data" -p "$bench_pid" -- sleep "$seconds" >/tmp/bench-cpu-perf.log 2>&1; then
    msg="perf record failed (perf_event_paranoid=$paranoid): $(tr '\n' ' ' < /tmp/bench-cpu-perf.log | cut -c1-200)"
    log "perf record failed; reason: $msg"
    status_note "$msg"
    wait "$bench_pid" 2>/dev/null || true
    exit 0
fi
wait "$bench_pid" 2>/dev/null || true

# Prefer stackcollapse-perf.pl (FlameGraph), else use perf report stdio as folded-ish input.
if command -v stackcollapse-perf.pl >/dev/null 2>&1; then
    perf script -i "$perf_data" 2>/dev/null | stackcollapse-perf.pl > "$out"
elif command -v inferno-collapse-perf >/dev/null 2>&1; then
    perf script -i "$perf_data" 2>/dev/null | inferno-collapse-perf > "$out"
else
    msg="no stack collapsing tool found (need stackcollapse-perf.pl or inferno); table left empty"
    log "$msg"
    status_note "$msg"
    exit 0
fi

if [ -s "$out" ]; then
    log "wrote $(wc -l < "$out") folded stacks to $out"
    status_note "CPU samples from perf on $crate/$bench ($(wc -l < "$out") folded stacks)."
else
    msg="perf produced no samples (perf_event_paranoid=$paranoid or kernel restriction); table left empty"
    log "$msg"
    status_note "$msg"
fi
