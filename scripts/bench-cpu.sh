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
seconds="${2:-15}"
out="benches/perf-folded.txt"
: > "$out"

log() { echo "[bench-cpu] $*"; }

if ! command -v perf >/dev/null 2>&1; then
    log "perf not installed; CPU sample table stays empty (java/Linux perf tools)."
    log "On a runner: sudo apt-get install -y linux-tools-common linux-tools-generic"
    exit 0
fi

paranoid=$(cat /proc/sys/kernel/perf_event_paranoid 2>/dev/null || echo "unknown")
log "perf_event_paranoid=$paranoid"
if [ "$paranoid" != "unknown" ] && [ "$paranoid" -gt 1 ] 2>/dev/null; then
    if command -v sudo >/dev/null 2>&1; then
        sudo sysctl -w kernel.perf_event_paranoid=1 >/dev/null 2>&1 || \
            log "could not lower perf_event_paranoid; samples may be empty"
    fi
fi

log "compiling bench target: $bench"
cargo bench --bench "$bench" --no-run >/dev/null 2>&1 || {
    log "cannot compile bench $bench; skipping CPU sampling"
    exit 0
}

# --profile-time keeps Criterion in its measurement loop so perf has a live target.
log "starting $bench with --profile-time ${seconds}s"
cargo bench --bench "$bench" -- --profile-time "$seconds" >/tmp/bench-cpu-run.log 2>&1 &
bench_pid=$!
sleep 3

if ! kill -0 "$bench_pid" 2>/dev/null; then
    log "bench process exited before sampling; see /tmp/bench-cpu-run.log"
    exit 0
fi

perf_data=/tmp/bench-cpu-$bench.data
log "recording ${seconds}s of call-graph samples"
if ! perf record -F 99 -g -o "$perf_data" -p "$bench_pid" -- sleep "$seconds" >/tmp/bench-cpu-perf.log 2>&1; then
    log "perf record failed; reason:"
    cat /tmp/bench-cpu-perf.log || true
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
    log "no stack collapsing tool found; install FlameGraph or inferno to fill the table"
    exit 0
fi

if [ -s "$out" ]; then
    log "wrote $(wc -l < "$out") folded stacks to $out"
else
    log "perf produced no stacks (permission or kernel restriction); table stays empty"
fi
