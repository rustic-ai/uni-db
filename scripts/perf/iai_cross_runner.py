#!/usr/bin/env python3
"""Separate cross-runner from within-runner variance in iai-callgrind results.

``iai_cv.py`` answers a different question and must not be bent into this one.
It pools every run into a single CV, which **conflates** two sources of spread:

* **within-runner** — repeatability on fixed hardware. This is what the Phase-0B
  pilot measured (0.21-0.96% over 20 runs on one machine).
* **between-runner** — how much the same benchmark moves when it lands on a
  different CI VM. This has never been measured, and it is the one a PR gate
  actually experiences.

A pooled CV cannot tell them apart: a target with perfect repeatability and wild
machine-to-machine spread produces the same pooled number as the reverse, and
only the second is a threat to a gate.

So this script takes one directory per runner and reports both, per target. The
figure Phase 7's threshold comes from is **CV across the per-runner means**.

Usage::

    iai_cross_runner.py shard-1/ shard-2/ shard-3/ [--markdown]

Each directory holds that runner's ``run-NN.json`` files as written by
``iai_collect.py``. Exits non-zero on a collection gap or a zero-instruction
target — see ``check_usable`` for why that is fatal here rather than a warning.
"""

from __future__ import annotations

import argparse
import json
import statistics
import sys
from pathlib import Path

# The pilot's within-runner threshold, reused so the two halves are comparable.
# Whether the cross-runner figure should share it is a Phase-7 decision informed
# by what this script measures — it is deliberately not asserted here.
CV_REFERENCE_PCT = 1.0


def load_runner(directory: Path) -> dict[str, list[int]]:
    """All instruction series from one runner's run JSONs, keyed by target."""
    series: dict[str, list[int]] = {}
    runs = sorted(directory.glob("run-*.json"))
    if not runs:
        raise SystemExit(f"error: no run-*.json under {directory}")
    for path in runs:
        for name, entry in json.loads(path.read_text()).items():
            series.setdefault(name, []).append(entry["instructions"])
    return series


def cv_pct(values: list[float]) -> float:
    """Coefficient of variation as a percentage; 0.0 for a single sample."""
    if len(values) < 2:
        return 0.0
    mean = statistics.fmean(values)
    return (statistics.stdev(values) / mean * 100.0) if mean else 0.0


def check_usable(per_runner: dict[str, dict[str, list[int]]]) -> list[str]:
    """Reasons the collected data cannot support a qualification verdict.

    Both checks are **fatal**, not warnings, and that is deliberate.
    ``iai_collect.py`` warns about zero-instruction benchmarks on stderr but
    exits 0 — which is exactly how ``baseline_noop`` was dropped from the
    original pilot without anyone noticing. A CI job that inherited that
    behaviour would report a clean cross-runner qualification computed over no
    data at all, which is worse than reporting nothing.
    """
    problems: list[str] = []
    all_targets = {t for s in per_runner.values() for t in s}

    for runner, series in sorted(per_runner.items()):
        missing = all_targets - set(series)
        if missing:
            problems.append(
                f"{runner}: missing {len(missing)} target(s) other runners have "
                f"({', '.join(sorted(missing))}) — a collection gap, not noise"
            )
        for target, values in sorted(series.items()):
            if any(v == 0 for v in values):
                problems.append(
                    f"{runner}: {target} collected 0 instructions in "
                    f"{sum(1 for v in values if v == 0)} of {len(values)} run(s) — "
                    "callgrind's name-based toggle did not match, usually a "
                    "stripped bench binary (see [profile.bench] in .cargo/config.toml)"
                )
    return problems


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("runners", nargs="+", type=Path, help="one directory per runner")
    ap.add_argument("--markdown", action="store_true", help="emit a markdown table")
    args = ap.parse_args()

    per_runner = {d.name: load_runner(d) for d in args.runners}
    if len(per_runner) < 2:
        print(
            "error: cross-runner variance needs at least 2 runners; "
            f"got {len(per_runner)}",
            file=sys.stderr,
        )
        return 1

    problems = check_usable(per_runner)
    if problems:
        print("error: collected data cannot support a verdict:", file=sys.stderr)
        for p in problems:
            print(f"  - {p}", file=sys.stderr)
        return 1

    targets = sorted({t for s in per_runner.values() for t in s})
    rows = []
    for target in targets:
        runner_means = [
            statistics.fmean(series[target]) for series in per_runner.values()
        ]
        within = [cv_pct([float(v) for v in s[target]]) for s in per_runner.values()]
        rows.append(
            {
                "target": target,
                "grand_mean": statistics.fmean(runner_means),
                "spread_pct": (
                    (max(runner_means) - min(runner_means))
                    / statistics.fmean(runner_means)
                    * 100.0
                    if statistics.fmean(runner_means)
                    else 0.0
                ),
                "cross_cv": cv_pct(runner_means),
                "within_cv": statistics.fmean(within),
            }
        )

    n_runners = len(per_runner)
    n_runs = sum(len(next(iter(s.values()))) for s in per_runner.values())
    header = (
        f"{n_runners} runners, {n_runs} runs total "
        f"({', '.join(sorted(per_runner))})"
    )

    if args.markdown:
        print(f"| target | mean Ir | cross-runner CV % | within-runner CV % | spread % |")
        print("|---|---|---|---|---|")
        for r in rows:
            print(
                f"| `{r['target']}` | {r['grand_mean']:,.0f} | **{r['cross_cv']:.2f}** "
                f"| {r['within_cv']:.2f} | {r['spread_pct']:.2f} |"
            )
        print()
        print(f"_{header}._")
    else:
        print(f"== {header} ==\n")
        print(f"{'target':<44}{'mean Ir':>14}{'cross CV%':>11}{'within CV%':>12}{'spread%':>10}")
        print("-" * 91)
        for r in rows:
            print(
                f"{r['target']:<44}{r['grand_mean']:>14,.0f}"
                f"{r['cross_cv']:>11.2f}{r['within_cv']:>12.2f}{r['spread_pct']:>10.2f}"
            )

    # The `baselines::` group exists to make the other numbers readable — the
    # qualification report calls them out as "not gate candidates" — and
    # `baseline_noop` is single-digit Ir, where one instruction of difference is
    # a double-digit percentage. Letting them set the headline figure would
    # report a threat to a gate they are not part of.
    gated = [r for r in rows if not r["target"].startswith("baselines::")] or rows
    worst = max(gated, key=lambda r: r["cross_cv"])
    print(
        f"\nWorst cross-runner CV among gate candidates: {worst['cross_cv']:.2f}% "
        f"on `{worst['target']}` (the pilot's within-runner threshold was "
        f"{CV_REFERENCE_PCT}%).",
        file=sys.stderr,
    )
    print(
        "A PR gate's threshold must clear the cross-runner column, not the "
        "within-runner one. This script reports; it does not qualify.",
        file=sys.stderr,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
