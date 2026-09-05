#!/usr/bin/env python3
"""Self-checks for iai_gate.py, run as a step of the job that uses it.

There is no lane that runs tests under `scripts/`, so a test file here would
never execute and would be exactly the "check that cannot fail" this gate was
found to be. The perf workflow runs this immediately before the gate instead.

Dependency-free on purpose: the perf runner has python3 and nothing installed.

    python3 scripts/perf/test_iai_gate.py
"""

from __future__ import annotations

import json
import subprocess
import sys
import tempfile
from pathlib import Path

GATE = Path(__file__).resolve().parent / "iai_gate.py"
RUNNER = "test-runner-1"


def write_case(tmp: Path, base_ir: int, current_ir: int, runners: list[str] | None) -> tuple[Path, Path]:
    """A baseline gating one target, and a run directory reporting `current_ir`."""
    generated_from: dict = {"samples_per_target": {"t.a": 1}}
    if runners is not None:
        generated_from["runners"] = runners
    baseline = tmp / "baseline.json"
    baseline.write_text(
        json.dumps(
            {
                "schema": 1,
                "generated_from": generated_from,
                "targets": {"t.a": {"instructions": base_ir, "gated": True}},
            }
        )
    )
    rundir = tmp / "run"
    rundir.mkdir(exist_ok=True)
    # `load_runner` reads `{target: {"instructions": N}}` at the top level, not
    # nested under "targets". An earlier version of this file nested it, the gate
    # crashed with exit 1, and the first two checks read that as the gate
    # correctly failing the run -- passing for the wrong reason until a later
    # case that expects exit 0 exposed it.
    (rundir / "run-1.json").write_text(json.dumps({"t.a": {"instructions": current_ir}}))
    return baseline, rundir


def run_gate(
    baseline: Path, rundir: Path, foreign: bool = False
) -> subprocess.CompletedProcess:
    argv = [
        sys.executable, str(GATE),
        "--baseline", str(baseline),
        "--current", str(rundir),
        "--fail-pct", "2",
        "--warn-pct", "1",
        "--fail-improve-pct", "50",
    ]
    if foreign:
        argv.append("--allow-foreign-machine")
    return subprocess.run(
        argv,
        capture_output=True,
        text=True,
        env={"PATH": "/usr/bin:/bin", "IAI_RUNNER": RUNNER},
    )


def check(label: str, cond: bool, detail: str = "") -> None:
    if not cond:
        raise AssertionError(f"{label} FAILED {detail}")
    print(f"ok  {label}")


def main() -> int:
    with tempfile.TemporaryDirectory() as td:
        tmp = Path(td)

        # The defect this file exists for: a collapse to 3% of baseline used to
        # exit 0 with "worst +0.00%".
        b, r = write_case(tmp, 52_708_611, 1_553_543, [RUNNER])
        got = run_gate(b, r)
        check("a collapsed measurement fails", got.returncode == 1, got.stdout + got.stderr)
        check(
            "and the summary does not claim +0.00%",
            "worst +0.00%" not in got.stdout,
            got.stdout,
        )

        # A regression still fails, unchanged.
        b, r = write_case(tmp, 1_000_000, 1_100_000, [RUNNER])
        check("a regression fails", run_gate(b, r).returncode == 1)

        # An ordinary run passes, and says which checks ran.
        b, r = write_case(tmp, 1_000_000, 1_005_000, [RUNNER])
        got = run_gate(b, r)
        check("an unchanged run passes", got.returncode == 0, got.stdout + got.stderr)
        check(
            "and reports both directions and its scope",
            "best" in got.stdout and "regressions and improvements" in got.stdout,
            got.stdout,
        )

        # A modest improvement is not a failure: real optimizations land here.
        b, r = write_case(tmp, 1_000_000, 700_000, [RUNNER])
        check("a 30% improvement passes under a 50% bound", run_gate(b, r).returncode == 0)

        # --allow-foreign-machine is the only way to disarm the collapse check,
        # and the summary must admit it rather than printing a green line that
        # implies a check which never ran.
        b, r = write_case(tmp, 52_708_611, 1_553_543, [RUNNER])
        got = run_gate(b, r, foreign=True)
        check("--allow-foreign-machine downgrades the collapse", got.returncode == 0)
        check(
            "and the summary says improvement checking was off",
            "regressions only" in got.stdout,
            got.stdout,
        )
        check(
            "and a regression still fails with it",
            run_gate(*write_case(tmp, 1_000_000, 1_100_000, [RUNNER]), foreign=True).returncode == 1,
        )

        # The check must not depend on the baseline naming a machine: those are
        # shard artifact names, so a match could never happen and the check would
        # be silently off forever.
        b, r = write_case(tmp, 52_708_611, 1_553_543, None)
        check("a baseline naming no runners is still checked", run_gate(b, r).returncode == 1)
        b, r = write_case(tmp, 52_708_611, 1_553_543, ["iai-shard-1", "iai-shard-2"])
        check(
            "a baseline naming unmatchable shards is still checked",
            run_gate(b, r).returncode == 1,
        )

        # Threshold sanity: an improve bound at or below the regression bound
        # would fail ordinary optimizations, so it is refused.
        b, r = write_case(tmp, 1_000_000, 1_000_000, [RUNNER])
        bad = subprocess.run(
            [
                sys.executable, str(GATE),
                "--baseline", str(b), "--current", str(r),
                "--fail-pct", "2", "--warn-pct", "1", "--fail-improve-pct", "2",
            ],
            capture_output=True, text=True, env={"PATH": "/usr/bin:/bin"},
        )
        check("--fail-improve-pct must exceed --fail-pct", bad.returncode == 2, bad.stderr)

    print("\nall iai_gate self-checks passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
