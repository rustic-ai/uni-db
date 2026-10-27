#!/usr/bin/env python3
"""Exercise both report wrappers with failing tests and aggregation failures."""

from __future__ import annotations

import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[2]
WRAPPERS = ("run_tck_with_report.sh", "run_locy_tck_with_report.sh")

CARGO_STUB = r"""
import json, os, sys
from pathlib import Path
if sys.argv[1:3] == ['nextest', 'list']:
    sys.exit(0)
mode = os.environ.get('UNI_LOCY_TCK_SCHEMA_MODE', os.environ.get('UNI_TCK_SCHEMA_MODE'))
with open(os.environ['TRACE_PATH'], 'a') as out:
    out.write('run ' + mode + '\n')
sys.exit(json.loads(os.environ['STUB_FAILURES']).get(mode, 0))
"""
PYTHON_STUB = r"""
import os, sys
from pathlib import Path
if sys.argv[1] == 'scripts/aggregate_nextest_results.py':
    with open(os.environ['TRACE_PATH'], 'a') as out:
        out.write('aggregate\n')
    status = int(os.environ.get('STUB_AGGREGATION_STATUS', '0'))
    if status:
        sys.exit(status)
    output = Path(sys.argv[sys.argv.index('--output-dir') + 1])
    output.mkdir(parents=True, exist_ok=True)
    path = output / 'results_20261027_000000.json'
    path.write_text('{}')
    print(path)
elif sys.argv[1] == 'scripts/analyze_tck_json.py':
    Path(sys.argv[2]).with_name('report.md').write_text('Test report\n')
else:
    raise RuntimeError(sys.argv)
"""


class ReportExitStatus(unittest.TestCase):
    def invoke(self, wrapper, failures, *, both=False, aggregate_status=0):
        with tempfile.TemporaryDirectory(prefix="uni-tck-wrapper-") as temporary:
            root = Path(temporary)
            (root / "scripts").mkdir()
            binary = root / "bin"
            binary.mkdir()
            shutil.copyfile(ROOT / "scripts" / wrapper, root / "scripts" / wrapper)
            for name, body in (("cargo", CARGO_STUB), ("python3", PYTHON_STUB)):
                executable = binary / name
                executable.write_text(f"#!{sys.executable}\n" + body)
                executable.chmod(0o755)
            trace = root / "trace"
            command = ["bash", str(root / "scripts" / wrapper)]
            command += ["--both"] if both else ["~example"]
            env = {
                key: value
                for key, value in os.environ.items()
                if key not in {"UNI_TCK_SCHEMA_MODE", "UNI_LOCY_TCK_SCHEMA_MODE"}
            } | {
                "PATH": str(binary) + os.pathsep + os.environ["PATH"],
                "TRACE_PATH": str(trace),
                "STUB_FAILURES": json.dumps(failures),
                "STUB_AGGREGATION_STATUS": str(aggregate_status),
            }
            result = subprocess.run(command, env=env, capture_output=True, text=True)
            return result, trace.read_text().splitlines() if trace.exists() else []

    def test_filtered_failure_is_reported_after_aggregation(self):
        for wrapper in WRAPPERS:
            with self.subTest(wrapper=wrapper):
                result, trace = self.invoke(wrapper, {"schemaless": 100})
                self.assertEqual(result.returncode, 100, result.stdout + result.stderr)
                self.assertIn("aggregate", trace)

    def test_first_mode_failure_does_not_skip_second_mode(self):
        for wrapper in WRAPPERS:
            with self.subTest(wrapper=wrapper):
                result, trace = self.invoke(wrapper, {"schemaless": 100}, both=True)
                self.assertEqual(result.returncode, 100, result.stdout + result.stderr)
                self.assertEqual(
                    [s for s in trace if s.startswith("run ")],
                    ["run schemaless", "run sidecar"],
                )
                self.assertEqual(trace.count("aggregate"), 2)

    def test_second_mode_failure_is_reported(self):
        for wrapper in WRAPPERS:
            with self.subTest(wrapper=wrapper):
                result, _ = self.invoke(wrapper, {"sidecar": 100}, both=True)
                self.assertEqual(result.returncode, 100, result.stdout + result.stderr)

    def test_success_stays_successful(self):
        for wrapper in WRAPPERS:
            with self.subTest(wrapper=wrapper):
                result, _ = self.invoke(wrapper, {}, both=True)
                self.assertEqual(result.returncode, 0, result.stdout + result.stderr)

    def test_aggregation_failure_is_not_hidden(self):
        for wrapper in WRAPPERS:
            with self.subTest(wrapper=wrapper):
                result, _ = self.invoke(wrapper, {}, aggregate_status=7)
                self.assertEqual(result.returncode, 7, result.stdout + result.stderr)


if __name__ == "__main__":
    unittest.main()
