"""Run the performance review probes through Uni's existing integration test binary.

The temporary module registration is removed after nextest exits, including on
test failure. No Git commands are used. Timings are printed; each probe also checks its result.
"""

import json
import os
from pathlib import Path
import subprocess


def main() -> int:
    evidence_dir = Path(__file__).resolve().parent
    root = evidence_dir.parents[2]
    integration = root / "crates/uni/tests/integration.rs"
    test_path = os.path.relpath(evidence_dir / "probes.rs", integration.parent)
    registration = (
        "\n// Temporary performance review probes.\n"
        f"#[path = {json.dumps(test_path)}]\n"
        "mod perf_review_20261026;\n"
    )
    source = integration.read_text()
    if "mod perf_review_20261026;" in source:
        raise SystemExit("Review probes are already registered; refusing to duplicate them.")

    integration.write_text(source + registration)
    try:
        result = subprocess.run(
            [
                "cargo", "nextest", "run", "-p", "uni-db", "--test", "integration",
                "-E", "test(perf_review_20261026)", "--test-threads", "1", "--success-output", "immediate", "--no-fail-fast",
            ],
            cwd=root,
            env={**os.environ, "RUSTC_WRAPPER": ""},
            check=False,
        )
        return result.returncode
    finally:
        current = integration.read_text()
        if current.count(registration) != 1:
            raise RuntimeError("Temporary registration changed; inspect integration.rs manually.")
        integration.write_text(current.replace(registration, ""))


if __name__ == "__main__":
    raise SystemExit(main())
