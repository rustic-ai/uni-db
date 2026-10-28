#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2024-2026 Dragonscale Team
"""Run every static guard of pr.yml's `static-guards` job, in one command.

This file is the job's single list: pr.yml runs it, and so does the local
quick path in docs/local_ci_runbook.md. Before it existed the list lived in the
workflow and was copied into the runbook, and a change to the Python bindings
reached CI without the generated API reference having been regenerated (PR
#318).

Every guard runs even when an earlier one fails, so one run reports them all.

    python3 scripts/ci/static_guards.py                 # the pr.yml job
    python3 scripts/ci/static_guards.py --with-fuzz-lock

`--with-fuzz-lock` also asserts fuzz/Cargo.lock is current, as pr.yml's
`fuzz-smoke` job does before fuzzing. fuzz/ is a separate workspace with its
own tracked lock that no ordinary build touches, so a dependency added to any
crate the fuzz targets reach (uni-common, uni-store, uni-cypher, uni-btic)
leaves it stale. That check needs cargo; the guards above need only Python.

Exit status: 0 when every check passed, 1 otherwise.
"""

from __future__ import annotations

import argparse
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]

# (name, argv, what to do when it fails). Order matches the former pr.yml steps.
GUARDS: list[tuple[str, list[str], str]] = [
    # Static check (no compile): every wheel variant must enable the plugin
    # loader features its shared binding source calls unconditionally.
    (
        "wheel-variant plugin features",
        [sys.executable, "scripts/ci/check_wheel_variant_features.py"],
        "enable the missing loader features in the wheel variant's Cargo.toml",
    ),
    # Every Python-visible version must track the workspace Cargo version.
    # release.yml checks pyproject.toml, but only at release time; this runs
    # per-PR and also forbids re-introducing a hardcoded `__version__`.
    (
        "Python version consistency",
        [sys.executable, "scripts/ci/check_version_consistency.py"],
        "derive the version from the workspace Cargo version; never hardcode it",
    ),
    (
        "generated Locy notebooks",
        [sys.executable, "website/scripts/generate_locy_notebooks.py", "--check"],
        "run `python3 website/scripts/generate_locy_notebooks.py`",
    ),
    # Documented counts must match what source or the filesystem already
    # asserts: plugin surfaces, graph algorithms, both TCK suites' feature
    # files and scenario totals, and the skill reference pages.
    (
        "documented counts",
        [sys.executable, "scripts/ci/check_documented_counts.py"],
        "update the documented count to what the source asserts",
    ),
    # The Python symbol reference is generated from `__init__.pyi`; a stale
    # checked-in copy is the drift this replaced, so fail rather than diverge.
    (
        "generated Python API reference",
        [sys.executable, "scripts/gen_python_api_reference.py", "--check"],
        "run `python3 scripts/gen_python_api_reference.py` (never hand-edit the page)",
    ),
    # Documented Python methods must exist. Kills the invented-method class
    # (`xervo.rerank`, `session.profile_with`) that two hand-maintained
    # references produced independently.
    (
        "documented Python symbols",
        [sys.executable, "scripts/ci/check_doc_symbols.py"],
        "fix the doc, or add the method to bindings/uni-db/uni_db/__init__.pyi",
    ),
    # The guards above all compare Python to Python, so a Rust field or error
    # variant that never got bound is invisible to them -- which is how
    # `config({"ssi_enabled": False})` came to be accepted and ignored. This
    # one compares Rust to Python and requires every gap to be allowlisted
    # with a reason, so a new one is a decision rather than an omission.
    (
        "Rust <-> Python parity",
        [sys.executable, "scripts/ci/check_rust_python_parity.py"],
        "bind the item, or allowlist it in check_rust_python_parity.py with a reason",
    ),
    # Every #[ignore]d test runs somewhere: the nightly sweep runs all of them
    # except the dedicated lanes' and the on-demand ones it registers. This
    # keeps that registry honest without a build.
    (
        "ignored-test registry",
        [sys.executable, "scripts/ci/ignored_tests.py", "check"],
        "update scripts/ci/ignored_tests.py (a lane filter changed, or an "
        "on-demand entry names a test that is gone)",
    ),
    (
        "Rust/Python drift guard negative controls",
        [sys.executable, "scripts/ci/test_rust_python_parity.py"],
        "keep facade methods, config fields and readback gaps detectable",
    ),
    (
        "documentation contract negative controls",
        [sys.executable, "scripts/ci/test_doc_symbols.py"],
        "preserve receiver, factory, config and notebook drift detection",
    ),
    (
        "ORT cache preparation checks",
        [sys.executable, "scripts/ci/test_prepare_ort.py"],
        "preserve pinned-version verification, resume, checksum and atomic cache publication",
    ),
    (
        "TCK report exit status",
        [sys.executable, "scripts/ci/test_tck_report_exit_status.py"],
        "preserve nextest failures while generating TCK reports for both modes",
    ),
]

FUZZ_LOCK: tuple[str, list[str], str] = (
    "fuzz/Cargo.lock is current",
    ["cargo", "metadata", "--locked", "--format-version", "1", "--manifest-path",
     "fuzz/Cargo.toml"],
    "run `cargo update --workspace --manifest-path fuzz/Cargo.toml` (adds only the "
    "new entries; a bare `cargo update` there re-resolves every dependency) and "
    "commit fuzz/Cargo.lock",
)


def run(name: str, argv: list[str], fix: str) -> bool:
    """Run one check; print its outcome and, on failure, its output and fix."""
    started = time.monotonic()
    try:
        done = subprocess.run(argv, cwd=ROOT, capture_output=True, text=True)
    except FileNotFoundError as e:
        print(f"FAIL  {name}: {e}")
        return False
    elapsed = time.monotonic() - started
    if done.returncode == 0:
        print(f"ok    {name} ({elapsed:.1f}s)")
        return True
    print(f"FAIL  {name} ({elapsed:.1f}s)")
    output = (done.stdout + done.stderr).rstrip()
    if output:
        print("\n".join("      " + line for line in output.splitlines()))
    print(f"      fix: {fix}")
    return False


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument(
        "--with-fuzz-lock",
        action="store_true",
        help="also check fuzz/Cargo.lock is current (needs cargo)",
    )
    args = parser.parse_args()

    checks = list(GUARDS)
    if args.with_fuzz_lock:
        checks.append(FUZZ_LOCK)
    failed = [name for name, argv, fix in checks if not run(name, argv, fix)]
    if failed:
        print(f"\n{len(failed)} of {len(checks)} static checks failed: {', '.join(failed)}")
        return 1
    print(f"\nall {len(checks)} static checks passed")
    return 0


if __name__ == "__main__":
    sys.exit(main())
