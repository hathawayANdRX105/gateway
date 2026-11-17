#!/usr/bin/env bash
# scripts/bench.sh — shared local/CI entry for the Criterion benchmark suite.
#
# Runs the whole workspace benchmark suite on the pinned toolchain's bench profile
# (inherits release, LLVM) and rebuilds benches/REPORT.md from Criterion's JSON.
# Both `just bench` and the manual CI workflow call this script, so a local run and
# a runner run produce the same table.
#
# Usage: bash scripts/bench.sh [report-output-path]
set -euo pipefail
cd "$(git rev-parse --show-toplevel)" || exit 1

out="${1:-benches/REPORT.md}"

# Drop stale measurement dirs from prior runs so renamed/removed benchmarks do not
# linger in the report. Keep `base/` — that is the cross-run comparison baseline.
echo "==> clearing stale criterion new/ and change/ dirs"
if [ -d target/criterion ]; then
    find target/criterion -mindepth 3 -maxdepth 3 -type d \
        \( -name new -o -name change \) -prune -exec rm -rf {} +
fi

echo "==> cargo bench --workspace"
cargo bench --workspace

echo "==> python3 benches/bench_report.py --out $out"
python3 benches/bench_report.py --out "$out"

echo "==> benchmark report: $out"
