#!/usr/bin/env bash
# scripts/bench-cpu.sh — best-effort function-level CPU hotspots for the benchmarks.
#
# Primary profiler: valgrind/callgrind. It is pure user-mode instrumentation, so it
# works on GitHub-hosted runners where perf_event_paranoid=4 forbids perf. It needs
# symbol names, which [profile.bench] in Cargo.toml keeps via strip=false/debug=1.
# Perf is kept as a secondary option for hosts that allow unprivileged sampling.
#
# Output: benches/perf-folded.txt with "<leaf function> <Ir cost>" rows.
# benches/bench_report.py reads it and renders the CPU sample table. Every failure
# path writes the reason into benches/perf-status.txt and exits 0 — CPU sampling is
# best effort and never fails the benchmark run.
#
# Usage: bash scripts/bench-cpu.sh [bench-name] [crate] [iterations]
set -uo pipefail
cd "$(git rev-parse --show-toplevel)" || exit 1

bench="${1:-cache_ops}"
crate="${2:-gateway-cache}"
iters="${3:-40}"
out="benches/perf-folded.txt"
status="benches/perf-status.txt"
: > "$out"
status_note() { printf '%s\n' "$1" > "$status"; }
status_note "pending"

log() { echo "[bench-cpu] $*"; }

# Locate the release bench binary Cargo built (symbols come from [profile.bench]).
log "compiling bench $crate/$bench and resolving its binary"
bin=$(cargo bench -p "$crate" --bench "$bench" --no-run --message-format=json 2>/dev/null \
    | python3 -c 'import json,sys
for line in sys.stdin:
    try: obj=json.loads(line)
    except ValueError: continue
    exe=obj.get("executable")
    if exe and obj.get("profile", {}).get("test") and "release" in exe:
        print(exe); break' || true)
if [ -z "$bin" ] || [ ! -x "$bin" ]; then
    msg="cannot locate bench binary for $crate/$bench; CPU table left empty."
    log "$msg"
    status_note "$msg"
    exit 0
fi
log "bench binary: $bin"

if ! command -v valgrind >/dev/null 2>&1 || ! command -v callgrind_annotate >/dev/null 2>&1; then
    msg="valgrind/callgrind_annotate not installed; CPU table left empty (CI: apt install valgrind)."
    log "$msg"
    status_note "$msg"
    exit 0
fi

# Run one short measurement under callgrind. IR counts (instructions read) are the
# cost unit; callgrind is ~20x slower, so keep the sample set small.
cg_out="/tmp/bench-cpu-${bench}.out"
log "profiling $bench under callgrind (sample-size $iters)"
if ! valgrind --tool=callgrind --callgrind-out-file="$cg_out" --quiet \
        "$bin" --sample-size "$iters" --warm-up-time 0.1 --measurement-time 0.2 \
        >/tmp/bench-cpu-run.log 2>&1; then
    msg="callgrind run failed; see /tmp/bench-cpu-run.log"
    log "$msg"
    status_note "$msg"
    exit 0
fi
if [ ! -s "$cg_out" ]; then
    msg="callgrind produced no profile data; CPU table left empty."
    log "$msg"
    status_note "$msg"
    exit 0
fi

# callgrind_annotate prints a sorted table: "Ir  file:function".
# Convert it into "<leaf> <Ir>" rows for the report's folded-stack reader.
annotate="/tmp/bench-cpu-${bench}.annotate.txt"
callgrind_annotate --inclusive=yes --auto=no --threshold=100 "$cg_out" > "$annotate" 2>/dev/null
python3 - "$annotate" "$out" <<'PY'
import re
import sys

src, dst = sys.argv[1], sys.argv[2]
rows = []
for line in open(src, errors="replace"):
    # e.g. "  12,345,678 ( 30.00%) /path/crate.rs:fn::name (40x)"
    match = re.match(r"\s*([\d,]+)\s+\([\d.]+%\)\s+(.+)", line)
    if not match:
        continue
    cost = int(match.group(1).replace(",", ""))
    symbol = re.sub(r"\s*\(\d+x\)\s*$", "", match.group(2).strip())
    leaf = symbol.rsplit(":", 1)[-1].strip()
    if leaf and cost > 0:
        rows.append((leaf, cost))
with open(dst, "w") as fh:
    for leaf, cost in rows[:25]:
        fh.write(f"{leaf} {cost}\n")
PY

if [ -s "$out" ]; then
    log "wrote $(wc -l < "$out") function rows from callgrind"
    status_note "callgrind Ir instructions on $crate/$bench ($(wc -l < "$out") functions)."
else
    msg="callgrind produced no annotated functions; CPU table left empty."
    log "$msg"
    status_note "$msg"
fi
