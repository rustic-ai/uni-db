#!/usr/bin/env python3
"""Compare a current iai run against docs/perf/iai-baseline.json.

Exit codes:
    0  every gated target within tolerance (warnings may still be printed)
    1  a gated target regressed past --fail-pct, or the comparison is unusable
    2  usage error

``--fail-pct`` and ``--warn-pct`` are REQUIRED and have no defaults. The
qualification pilot measured run-to-run variance on a single machine
(CV 0.21-0.96%); a PR gate experiences machine-to-machine spread, which is a
different and unmeasured number. Baking in a default here would be the exact
mistake the pilot exists to prevent, so the thresholds must be passed by
whoever can cite the measurement they came from.

A regression is reported per target; the run fails on the worst one.

Usage:
    iai_gate.py --baseline docs/perf/iai-baseline.json --current target/iai-pilot \
        --fail-pct 5 --warn-pct 2
"""

from __future__ import annotations

import argparse
import json
import statistics
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from iai_cross_runner import load_runner  # noqa: E402


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--baseline", type=Path, required=True)
    ap.add_argument("--current", type=Path, required=True, help="directory of run-*.json from this build")
    ap.add_argument("--fail-pct", type=float, required=True, help="regression that fails the build")
    ap.add_argument("--warn-pct", type=float, required=True, help="regression that warns only")
    ap.add_argument("--markdown", action="store_true", help="emit a markdown table for a PR comment")
    args = ap.parse_args()

    if args.warn_pct > args.fail_pct:
        print("--warn-pct must not exceed --fail-pct", file=sys.stderr)
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

    rows = []
    worst = 0.0
    failed = []
    warned = []
    for name in sorted(gated):
        base = targets[name]["instructions"]
        values = current[name]
        if any(v == 0 for v in values):
            print(f"FAIL {name}: zero-instruction sample -- collection is broken", file=sys.stderr)
            return 1
        now = int(statistics.median(values))
        delta = (now - base) / base * 100 if base else 0.0
        worst = max(worst, delta)
        status = "ok"
        if delta > args.fail_pct:
            status = "FAIL"
            failed.append((name, delta))
        elif delta > args.warn_pct:
            status = "warn"
            warned.append((name, delta))
        rows.append((name, base, now, delta, status))

    if args.markdown:
        print("| target | baseline Ir | current Ir | delta | |")
        print("|---|---:|---:|---:|---|")
        for name, base, now, delta, status in rows:
            print(f"| `{name}` | {base:,} | {now:,} | {delta:+.2f}% | {status} |")
    else:
        for name, base, now, delta, status in rows:
            print(f"{status:4}  {name:50} {base:>12,} -> {now:>12,}  {delta:+.2f}%")

    for name, delta in warned:
        print(f"warn: {name} +{delta:.2f}% (over --warn-pct {args.warn_pct})", file=sys.stderr)
    if failed:
        for name, delta in failed:
            print(f"FAIL: {name} +{delta:.2f}% (over --fail-pct {args.fail_pct})", file=sys.stderr)
        return 1

    print(f"\nall {len(gated)} gated targets within {args.fail_pct}% (worst {worst:+.2f}%)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
