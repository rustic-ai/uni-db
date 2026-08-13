#!/usr/bin/env python3
"""Compute per-target coefficient of variation across iai-callgrind pilot runs.

Consumes the JSON files written by ``iai_collect.py`` and reports, per
benchmark, the mean instruction count, standard deviation, CV, and the
qualification verdict.

**CV alone does not qualify a target.** A benchmark can be perfectly repeatable
and still be a bad gate — an IO-dominant path whose instruction count is flat
while its wall-clock regresses is stable *and* useless. The correlation leg
(instruction delta vs wall-clock delta under an injected regression) is the
second half, and is run separately. This script reports the stability half and
says so.

Usage::

    iai_cv.py target/iai-pilot/run-*.json
"""

from __future__ import annotations

import argparse
import json
import statistics
import sys
from pathlib import Path

# Below this, run-to-run noise is small enough that a 5% regression threshold is
# meaningful rather than a coin flip.
CV_THRESHOLD_PCT = 1.0


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("runs", nargs="+", type=Path)
    ap.add_argument(
        "--markdown", action="store_true", help="emit a markdown table for docs/perf/"
    )
    args = ap.parse_args()

    series: dict[str, list[int]] = {}
    escaped: dict[str, list[float]] = {}
    for path in args.runs:
        data = json.loads(path.read_text())
        for name, entry in data.items():
            series.setdefault(name, []).append(entry["instructions"])
            total = entry["instructions"]
            frac = (entry["other_threads"] / total * 100.0) if total else 0.0
            escaped.setdefault(name, []).append(frac)

    if not series:
        print("error: no data", file=sys.stderr)
        return 1

    n_runs = len(args.runs)
    rows = []
    for name in sorted(series):
        vals = series[name]
        mean = statistics.fmean(vals)
        sd = statistics.stdev(vals) if len(vals) > 1 else 0.0
        cv = (sd / mean * 100.0) if mean else float("inf")
        off = statistics.fmean(escaped[name]) if escaped[name] else 0.0
        stable = mean > 0 and cv < CV_THRESHOLD_PCT
        rows.append((name, mean, sd, cv, off, stable, min(vals), max(vals)))

    if args.markdown:
        print("| target | samples | mean Ir | sd | CV % | off-main-thread % | stable? |")
        print("|---|---|---|---|---|---|---|")
        for name, mean, sd, cv, off, stable, _, _ in rows:
            verdict = "yes" if stable else ("**ZERO**" if mean == 0 else "**no**")
            n = len(series[name])
            samples = f"{n}" if n == n_runs else f"**{n} of {n_runs}**"
            print(
                f"| `{name}` | {samples} | {mean:,.0f} | {sd:,.0f} | "
                f"{cv:.4f} | {off:.1f} | {verdict} |"
            )
    else:
        print(f"{'target':<60} {'n':>3} {'mean Ir':>14} {'CV %':>9} {'off-thr %':>10}  stable")
        for name, mean, sd, cv, off, stable, lo, hi in rows:
            print(
                f"{name:<60} {len(series[name]):>3} {mean:>14,.0f} {cv:>9.4f} "
                f"{off:>10.1f}  {'yes' if stable else 'NO'}   [{lo:,} .. {hi:,}]"
            )

    # A target present in only some runs is a collection gap, not a clean sample.
    partial = [n for n in series if len(series[n]) != n_runs]
    if partial:
        print(
            f"\nWARNING: {len(partial)} target(s) missing from some runs "
            f"(collection gap, not noise): " + ", ".join(sorted(partial)),
            file=sys.stderr,
        )

    zero = [r[0] for r in rows if r[1] == 0]
    if zero:
        print(
            f"\nWARNING: {len(zero)} target(s) measured ZERO instructions — "
            "collection is broken, not stable.",
            file=sys.stderr,
        )
    print(
        f"\nStability is half the gate. CV < {CV_THRESHOLD_PCT}% is necessary, "
        "not sufficient: run the injected-regression correlation leg before "
        "gating any target.",
        file=sys.stderr,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
