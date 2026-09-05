#!/usr/bin/env python3
"""Compare a current iai run against docs/perf/iai-baseline.json.

Exit codes:
    0  every gated target within tolerance (warnings may still be printed)
    1  a gated target regressed past --fail-pct, or the comparison is unusable
    2  usage error

``--fail-pct``, ``--warn-pct`` and ``--fail-improve-pct`` are REQUIRED and have
no defaults. The qualification pilot measured run-to-run variance on a single
machine (CV 0.21-0.96%); a PR gate experiences machine-to-machine spread, which
is a different and unmeasured number. Baking in a default here would be the
exact mistake the pilot exists to prevent, so the thresholds must be passed by
whoever can cite the measurement they came from.

A regression is reported per target; the run fails on the worst one.

**An implausible improvement fails too.** Every comparison used to be one-sided,
so a target whose instruction count collapsed to 3% of baseline was reported
``ok`` and the summary still printed ``worst +0.00%`` -- the gate could not fail
on "we measured nothing", which is the failure mode the surrounding machinery
exists to catch. ``--fail-improve-pct`` is deliberately looser than
``--fail-pct``: a genuine optimization also reduces instruction counts, so the
threshold marks "large enough that a human should confirm it", not "wrong".

**The improvement check is armed by default and disarmed only on request.**
Measured against a machine other than the baseline's, unchanged code has read
25-56% low, so a local run needs an escape -- but inferring one from machine
identity would disarm the check silently, and `generated_from.runners` cannot
supply it anyway: those are the qualify workflow's shard *artifact* names, all
five produced on one runner class. So the escape is explicit
(``--allow-foreign-machine``), CI passes no flag and is always armed, and a
green summary states which checks ran. Regressions fail in both modes.

Usage:
    iai_gate.py --baseline docs/perf/iai-baseline.json --current target/iai-pilot \
        --fail-pct 5 --warn-pct 2 --fail-improve-pct 50
"""

from __future__ import annotations

import argparse
import json
import os
import socket
import statistics
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from iai_cross_runner import load_runner  # noqa: E402


def current_runner() -> str:
    """Identity of the machine this run is measured on, for the record only.

    Printed alongside the baseline's provenance so a surprising comparison can
    be attributed later. It deliberately does **not** decide whether the
    improvement check runs: `generated_from.runners` holds the qualify
    workflow's shard artifact names (`iai-shard-1`..`5`), all produced on one
    runner class, so no hostname could ever match them and a check gated on that
    match would be permanently and silently off.
    """
    return os.environ.get("IAI_RUNNER") or socket.gethostname()


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--baseline", type=Path, required=True)
    ap.add_argument("--current", type=Path, required=True, help="directory of run-*.json from this build")
    ap.add_argument("--fail-pct", type=float, required=True, help="regression that fails the build")
    ap.add_argument("--warn-pct", type=float, required=True, help="regression that warns only")
    ap.add_argument(
        "--fail-improve-pct",
        type=float,
        required=True,
        help="improvement beyond this fails: an implausible drop is a collection failure, "
        "not a win. Looser than --fail-pct on purpose -- a real optimization lands here too",
    )
    ap.add_argument("--markdown", action="store_true", help="emit a markdown table for a PR comment")
    ap.add_argument(
        "--allow-foreign-machine",
        action="store_true",
        help="downgrade improvement failures to warnings: for a local run against a "
        "CI-generated baseline, where unchanged code has measured 25-56%% low. Never "
        "pass this in CI -- it is the one switch that disarms the collapse check",
    )
    args = ap.parse_args()

    if args.warn_pct > args.fail_pct:
        print("--warn-pct must not exceed --fail-pct", file=sys.stderr)
        return 2
    if args.fail_improve_pct <= args.fail_pct:
        print(
            "--fail-improve-pct must exceed --fail-pct: a threshold at or below the "
            "regression bound would fail ordinary optimizations",
            file=sys.stderr,
        )
        return 2

    doc = json.loads(args.baseline.read_text())
    if doc.get("schema") != 1:
        print(f"unsupported baseline schema: {doc.get('schema')!r}", file=sys.stderr)
        return 2
    targets = doc["targets"]

    current = load_runner(args.current)
    if not current:
        print(f"no run-*.json under {args.current}", file=sys.stderr)
        return 1

    gated = {name for name, entry in targets.items() if entry.get("gated")}
    if not gated:
        print("baseline gates no target; nothing to check", file=sys.stderr)
        return 1

    # A gated target that vanished from the run is a collection failure, not a
    # pass. This is the trap the whole track keeps finding: absent measurement
    # reading as success.
    missing = sorted(gated - set(current))
    if missing:
        for name in missing:
            print(f"FAIL {name}: gated target absent from the current run", file=sys.stderr)
        return 1

    runner = current_runner()
    # Armed unless explicitly told otherwise. Inferring this from machine
    # identity would turn the check off without anyone deciding to.
    improve_armed = not args.allow_foreign_machine

    rows = []
    worst = 0.0
    best = 0.0
    failed = []
    warned = []
    collapsed = []
    for name in sorted(gated):
        base = targets[name]["instructions"]
        values = current[name]
        if any(v == 0 for v in values):
            print(f"FAIL {name}: zero-instruction sample -- collection is broken", file=sys.stderr)
            return 1
        now = int(statistics.median(values))
        delta = (now - base) / base * 100 if base else 0.0
        # Tracked in both directions. A single `max` reported `worst +0.00%` for
        # a run in which every gated target sat 88-97% below baseline.
        worst = max(worst, delta)
        best = min(best, delta)
        status = "ok"
        if delta > args.fail_pct:
            status = "FAIL"
            failed.append((name, delta))
        elif delta > args.warn_pct:
            status = "warn"
            warned.append((name, delta))
        elif delta < -args.fail_improve_pct:
            status = "FAIL" if improve_armed else "warn"
            collapsed.append((name, delta))
        rows.append((name, base, now, delta, status))

    if args.markdown:
        print("| target | baseline Ir | current Ir | delta | |")
        print("|---|---:|---:|---:|---|")
        for name, base, now, delta, status in rows:
            print(f"| `{name}` | {base:,} | {now:,} | {delta:+.2f}% | {status} |")
    else:
        for name, base, now, delta, status in rows:
            print(f"{status:4}  {name:50} {base:>12,} -> {now:>12,}  {delta:+.2f}%")

    if not improve_armed:
        print(
            f"note: --allow-foreign-machine given, so improvement checking is OFF "
            f"(machine {runner!r}); regressions still fail",
            file=sys.stderr,
        )

    for name, delta in warned:
        print(f"warn: {name} +{delta:.2f}% (over --warn-pct {args.warn_pct})", file=sys.stderr)
    for name, delta in collapsed:
        label = "FAIL" if improve_armed else "warn"
        print(
            f"{label}: {name} {delta:+.2f}% -- an improvement past "
            f"--fail-improve-pct {args.fail_improve_pct} is a collection failure until "
            f"someone shows otherwise",
            file=sys.stderr,
        )
    if failed:
        for name, delta in failed:
            print(f"FAIL: {name} +{delta:.2f}% (over --fail-pct {args.fail_pct})", file=sys.stderr)
        return 1
    if collapsed and improve_armed:
        return 1

    # The summary states which checks ran. A green line that does not say the
    # improvement check was off would claim more than was measured -- the exact
    # shape this gate was found to have.
    scope = "regressions and improvements" if improve_armed else "regressions only"
    print(
        f"\nall {len(gated)} gated targets within {args.fail_pct}% "
        f"(worst {worst:+.2f}%, best {best:+.2f}%; checked: {scope})"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
