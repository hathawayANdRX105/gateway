#!/usr/bin/env python3
"""Build a Markdown performance report from tool-produced data.

Values come from Criterion.rs JSON under target/criterion/ and, when supplied,
folded stacks produced by `perf script | stackcollapse-perf.pl`. No value in the
report is estimated or invented by this script.

Usage:
    python3 benches/bench_report.py [--criterion-dir target/criterion]
                                    [--perf-folded <path>] [--out benches/REPORT.md]
"""

from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

GROUP_RE = re.compile(r'benchmark_group\(\s*"([^"]+)"\s*\)')


def repo_root() -> Path:
    return Path(
        subprocess.check_output(
            ["git", "rev-parse", "--show-toplevel"], text=True
        ).strip()
    )


def group_to_package(root: Path) -> dict[str, str]:
    """Map Criterion group names to the crate that defines them."""
    meta = json.loads(
        subprocess.check_output(
            ["cargo", "metadata", "--no-deps", "--format-version", "1"],
            cwd=root,
            text=True,
        )
    )
    result: dict[str, str] = {}
    for package in meta["packages"]:
        for target in package["targets"]:
            if "bench" not in target["kind"]:
                continue
            source = Path(target["src_path"])
            if not source.is_file():
                continue
            for group in GROUP_RE.findall(source.read_text()):
                result[group] = package["name"]
    return result


def criterion_rows(
    criterion_dir: Path, groups: dict[str, str]
) -> list[tuple[str, str, float, float, str]]:
    """Read (crate, bench id, mean ns, median ns, vs-base %) from Criterion output.

    The vs-base column comes from Criterion's own `change/estimates.json`, written
    only when a persisted `base/` baseline exists for that benchmark.
    """
    rows: list[tuple[str, str, float, float, str]] = []
    for estimates in sorted(criterion_dir.glob("*/*/new/estimates.json")):
        group, name, _, _ = estimates.relative_to(criterion_dir).parts
        data = json.loads(estimates.read_text())
        mean = data.get("mean", {}).get("point_estimate")
        median = data.get("median", {}).get("point_estimate")
        if mean is None:
            continue
        change = criterion_dir / group / name / "change" / "estimates.json"
        change_text = "-"
        if change.is_file():
            delta = json.loads(change.read_text()).get("mean", {}).get("point_estimate")
            if delta is not None:
                # Criterion stores the relative change as a ratio (0.1 = +10%).
                change_text = f"{float(delta) * 100:+.1f}%"
        rows.append(
            (
                groups.get(group, "unknown"),
                f"{group}/{name}",
                float(mean),
                float(median) if median is not None else 0.0,
                change_text,
            )
        )
    return sorted(rows, key=lambda row: row[2], reverse=True)


def folded_stack_rows(path: Path) -> list[tuple[str, int, float]]:
    """Rank leaf functions from a folded-stack file.

    Accepts perf's `A;B;leaf 123` and callgrind's `leaf 123456` (written by
    scripts/bench-cpu.sh). The count unit differs by profiler; the header carries
    the source note from perf-status.txt.
    """
    counts: dict[str, int] = {}
    for line in path.read_text().splitlines():
        name, separator, count_text = line.strip().rpartition(" ")
        if not separator or not count_text.isdigit():
            continue
        leaf = name.rsplit(";", 1)[-1]
        if not leaf:
            continue
        counts[leaf] = counts.get(leaf, 0) + int(count_text)
    total = sum(counts.values())
    return [
        (frame, count, 100.0 * count / total)
        for frame, count in sorted(
            counts.items(), key=lambda item: item[1], reverse=True
        )
    ]


def environment(root: Path) -> list[str]:
    cpu_model = "unknown"
    cpuinfo = Path("/proc/cpuinfo")
    if cpuinfo.is_file():
        for line in cpuinfo.read_text().splitlines():
            if line.startswith("model name"):
                cpu_model = line.split(":", 1)[1].strip()
                break
    rustc = subprocess.check_output(["rustc", "-vV"], cwd=root, text=True).splitlines()[
        0
    ]
    timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    loadavg = Path("/proc/loadavg")
    load = loadavg.read_text().split()[0] if loadavg.is_file() else "unknown"
    scaling = "unknown"
    try:
        for line in subprocess.check_output(
            ["lscpu"], text=True, stderr=subprocess.DEVNULL
        ).splitlines():
            if line.startswith("CPU(s) scaling MHz:"):
                scaling = line.split(":", 1)[1].strip()
                break
    except (OSError, subprocess.CalledProcessError):
        pass
    commit = subprocess.check_output(
        ["git", "rev-parse", "--short", "HEAD"], cwd=root, text=True
    ).strip()
    runner = os.environ.get("RUNNER_NAME") or os.environ.get("HOSTNAME") or "unknown"
    return [
        f"- Generated: {timestamp}",
        f"- Commit: {commit}",
        f"- Runner: {runner}",
        f"- CPU: {cpu_model}",
        f"- CPU scaling MHz: {scaling}",
        f"- Load average (1m): {load}",
        f"- Toolchain: {rustc}",
        "- Benchmark profile: Cargo default bench profile (inherits release, LLVM).",
        "- Source: `target/criterion/*/*/new/estimates.json`.",
    ]


def render(
    root: Path,
    rows: list[tuple[str, str, float, float, str]],
    perf: list[tuple[str, int, float]],
    perf_path: Path | None,
) -> str:
    lines = [
        "# Gateway benchmark report",
        "",
        "Generated by `python3 benches/bench_report.py`. Every number below was read",
        "from benchmark tool output; this script does not estimate or invent values.",
        "",
        "## Environment",
        "",
        *environment(root),
        "",
        "## Measured functions (slowest first)",
        "",
        "Per-call latency from Criterion. `mean` and `median` are nanoseconds per iteration.",
        "Stream benchmarks run a full chunk sequence in one iteration, so their numbers",
        "are per sequence, not per chunk.",
        "",
        "| Crate | Benchmark | Mean (ns) | Median (ns) | vs base |",
        "| --- | --- | ---: | ---: | ---: |",
    ]
    for crate, name, mean, median, change in rows:
        lines.append(
            f"| {crate} | `{name}` | {mean:.1f} | {median:.1f} | {change} |"
        )
    lines.extend(
        [
            "",
            "## CPU sample breakdown",
            "",
        ]
    )
    if perf_path is None:
        note = Path("benches/perf-status.txt")
        reason = (
            note.read_text().strip()
            if note.is_file() and note.read_text().strip()
            else "No CPU sampling was run for this report."
        )
        lines.extend(
            [
                "Not collected. Reason from the sampling step:",
                "",
                f"> {reason}",
                "",
                "On a host with perf_event permission, `bash scripts/bench-cpu.sh` fills this",
                "table with leaf functions ranked by sample count. CI runners commonly set",
                "perf_event_paranoid=4 (denied), so this table stays empty there by design.",
            ]
        )
    else:
        note = Path("benches/perf-status.txt")
        source = note.read_text().strip() if note.is_file() else "CPU profiler output"
        lines.extend(
            [
                f"Source: `{perf_path}` ({source}).",
                "`share` is the fraction of the profiler's total cost.",
                "",
                "| Function | Cost | Share (%) |",
                "| --- | ---: | ---: |",
            ]
        )
        for frame, count, share in perf[:25]:
            lines.append(f"| `{frame}` | {count} | {share:.2f} |")
    lines.append("")
    return "\n".join(lines)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--criterion-dir",
        default=None,
        help="Criterion output directory (default: <root>/target/criterion)",
    )
    parser.add_argument(
        "--perf-folded",
        type=Path,
        default=None,
        help="Folded stacks from stackcollapse-perf.pl",
    )
    parser.add_argument(
        "--out",
        type=Path,
        default=None,
        help="Markdown output path (default: <root>/benches/REPORT.md)",
    )
    args = parser.parse_args()

    root = repo_root()
    criterion_dir = (
        Path(args.criterion_dir).resolve()
        if args.criterion_dir
        else root / "target/criterion"
    )
    out = args.out.resolve() if args.out else root / "benches/REPORT.md"
    if not criterion_dir.is_dir():
        print(
            f"no Criterion data at {criterion_dir}; run `cargo bench --workspace` first",
            file=sys.stderr,
        )
        return 1

    rows = criterion_rows(criterion_dir, group_to_package(root))
    if not rows:
        print(f"no estimates.json under {criterion_dir}", file=sys.stderr)
        return 1
    perf = folded_stack_rows(args.perf_folded) if args.perf_folded else []
    out.write_text(render(root, rows, perf, args.perf_folded))
    print(f"wrote {out} ({len(rows)} benchmark rows, {len(perf)} CPU sample rows)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
