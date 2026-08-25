#!/usr/bin/env python3
"""Generate docs/perf/iai-baseline.json from iai pilot runs.

Consumes the same ``run-NN.json`` files ``iai_collect.py`` writes, one
directory per runner, and records a per-target reference instruction count
for ``iai_gate.py`` to compare against.

Two things this deliberately does NOT do:

* It does not invent a gating threshold. Which targets are gated is recorded
  here as data (``--gate``); by how much they may drift is the gate's
  argument, and both come from measurement rather than from a default.
* It does not drop the rejected targets. ``write_paths::*`` failed the
  qualification pilot's wall-clock-correlation leg, so they must not fail a
  build -- but recording their numbers is what makes a later re-qualification
  possible. They are written with ``gated: false`` and a reason.

Usage:
    iai_baseline.py RUNNER_DIR [RUNNER_DIR ...] --out docs/perf/iai-baseline.json \
        --gate read_paths::parse_and_plan_cold --gate read_paths::vertex_lookup_by_id ...
"""

from __future__ import annotations

import argparse
import json
import statistics
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from iai_cross_runner import check_usable, cv_pct, load_runner  # noqa: E402

SCHEMA = 1

# Callgrind counts one instruction of harness overhead as a double-digit
# percentage of a 4-Ir no-op, so the calibration group is never gate material.
EXCLUDED_PREFIX = "baselines::"


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("runners", nargs="+", type=Path, help="one directory of run-*.json per runner")
    ap.add_argument("--out", type=Path, required=True, help="baseline JSON to write")
    ap.add_argument(
        "--gate",
        action="append",
        default=[],
        metavar="TARGET",
        help="fully-qualified target name to mark gated; repeatable. "
        "Targets not named here are recorded but never fail a build.",
    )
    ap.add_argument(
        "--reason",
        default="not qualified by the instruction-count pilot",
        help="reason recorded against every non-gated target",
    )
    ap.add_argument(
        "--allow-unusable",
        action="store_true",
        help="write the baseline even if collection gaps or zero-instruction "
        "samples are present. Only for debugging -- a zero here would become "
        "a permanently-passing gate.",
    )
    args = ap.parse_args()

    per_runner = {d.name: load_runner(d) for d in args.runners}
    per_runner = {k: v for k, v in per_runner.items() if v}
    if not per_runner:
        print("no run-*.json found in any runner directory", file=sys.stderr)
        return 1

    problems = check_usable(per_runner)
    if problems:
        for p in problems:
            print(f"unusable: {p}", file=sys.stderr)
        if not args.allow_unusable:
            print(
                "refusing to write a baseline from unusable samples; "
                "a zero-instruction target would gate on nothing forever",
                file=sys.stderr,
            )
            return 1

    # Pool every sample from every runner. The reference is the median, not the
    # mean: a single outlying run on one runner should not move the number a
    # gate compares against.
    pooled: dict[str, list[int]] = {}
    for samples in per_runner.values():
        for target, values in samples.items():
            pooled.setdefault(target, []).extend(values)

    gated = set(args.gate)
    unknown = gated - set(pooled)
    if unknown:
        print(f"--gate names targets absent from the samples: {sorted(unknown)}", file=sys.stderr)
        return 1

    targets: dict[str, dict] = {}
    for target in sorted(pooled):
        values = pooled[target]
        entry: dict = {
            "instructions": int(statistics.median(values)),
            "samples": len(values),
            "cv_pct": round(cv_pct([float(v) for v in values]), 3),
            "gated": target in gated,
        }
        if target.startswith(EXCLUDED_PREFIX):
            entry["reason"] = "calibration baseline; too small to gate"
        elif target not in gated:
            entry["reason"] = args.reason
        targets[target] = entry

    doc = {
        "schema": SCHEMA,
        "generated_from": {
            "runners": sorted(per_runner),
            "samples_per_target": {t: targets[t]["samples"] for t in targets},
        },
        "targets": targets,
    }

    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(json.dumps(doc, indent=2, sort_keys=True) + "\n")

    n_gated = sum(1 for e in targets.values() if e["gated"])
    print(f"wrote {args.out}: {len(targets)} targets, {n_gated} gated")
    if n_gated == 0:
        print("warning: no target is gated -- this baseline can never fail a build", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
