#!/usr/bin/env python3
"""
Aggregate per-scenario nextest result JSONs into cucumber-compatible JSON.

Reads individual result files from a nextest output directory and produces
a timestamped results file in the same format as cucumber's JSON writer,
so analyze_tck_json.py can consume it.

If a manifest.json exists (written by `cargo nextest list`), scenarios
present in the manifest but missing from results are reported as "crashed"
(the test process died before writing a result).

Usage:
    python scripts/aggregate_nextest_results.py

Reads from: target/cucumber/nextest (default)
Writes to: target/cucumber/results_YYYYMMDD_HHMMSS.json (default)
Prints the output path to stdout (last line) for use by calling scripts.
"""

import argparse
import json
import sys
from collections import defaultdict
from datetime import datetime
from pathlib import Path


def load_manifest(results_dir):
    """Load manifest.json and return a list of (feature_path, scenario_name, line) tuples."""
    manifest_path = results_dir / "manifest.json"
    if not manifest_path.exists():
        return None

    try:
        with open(manifest_path) as f:
            data = json.load(f)
        return [
            (s["feature_path"], s["scenario_name"], s["line"])
            for s in data.get("scenarios", [])
        ]
    except (json.JSONDecodeError, KeyError) as e:
        print(f"  ⚠ Failed to load manifest.json: {e}", file=sys.stderr)
        return None


def main():
    parser = argparse.ArgumentParser(description="Aggregate nextest results into cucumber JSON")
    parser.add_argument(
        "--results-dir",
        help="Override the input directory containing per-scenario JSON files "
             "(default: target/cucumber/nextest)",
    )
    parser.add_argument(
        "--output-dir",
        help="Override the output directory for the results JSON (default: target/cucumber)",
    )
    args = parser.parse_args()

    repo_root = Path(__file__).parent.parent
    if args.results_dir:
        results_dir = repo_root / args.results_dir
    else:
        results_dir = repo_root / "target" / "cucumber" / "nextest"

    if args.output_dir:
        output_dir = repo_root / args.output_dir
    else:
        output_dir = repo_root / "target" / "cucumber"

    if not results_dir.exists():
        print(f"❌ No results directory found at {results_dir}", file=sys.stderr)
        sys.exit(1)

    # Exclude manifest.json from result files
    result_files = sorted(
        p for p in results_dir.glob("*.json")
        if p.name != "manifest.json"
    )
    if not result_files:
        print(f"❌ No result files found in {results_dir}", file=sys.stderr)
        sys.exit(1)

    print(f"📊 Aggregating {len(result_files)} scenario results...", file=sys.stderr)

    # Group scenarios by feature file
    features = defaultdict(list)
    # Track which (feature_path, line) combos we have results for
    seen_results = set()
    for path in result_files:
        try:
            with open(path) as f:
                data = json.load(f)
            features[data["feature_path"]].append(data)
            seen_results.add((data["feature_path"], data["line"]))
        except (json.JSONDecodeError, KeyError) as e:
            print(f"  ⚠ Skipping {path.name}: {e}", file=sys.stderr)

    # Cross-reference against manifest to detect crashed scenarios
    manifest_scenarios = load_manifest(results_dir)
    crashed_count = 0
    if manifest_scenarios is not None:
        for feature_path, scenario_name, line in manifest_scenarios:
            if (feature_path, line) not in seen_results:
                # This scenario is in the manifest but has no result file — it crashed
                crashed_count += 1
                features[feature_path].append({
                    "feature_path": feature_path,
                    "scenario_name": scenario_name,
                    "line": line,
                    "status": "crashed",
                    "error_message": (
                        f"Test process crashed (SIGABRT/segfault/OOM) before writing result. "
                        f"Scenario: {scenario_name} (line {line})"
                    ),
                })
                print(
                    f"  ⚠ CRASHED: {Path(feature_path).stem}:{line} — {scenario_name}",
                    file=sys.stderr,
                )

    # Build cucumber JSON format
    cucumber_json = []
    for feature_path, scenarios in sorted(features.items()):
        fp = Path(feature_path)
        feature_name = fp.stem

        elements = []
        for sc in sorted(scenarios, key=lambda s: s["line"]):
            status = sc["status"]
            step_result = {"status": status}
            if status in {"failed", "skipped", "crashed"}:
                # Preserve detailed failure/skip/crash reason from the test runner when available.
                step_result["error_message"] = sc.get(
                    "error_message",
                    f"Scenario failed: {sc['scenario_name']}",
                )

            elements.append({
                "type": "scenario",
                "name": sc["scenario_name"],
                "line": sc["line"],
                "steps": [
                    {
                        "keyword": "Scenario ",
                        "name": sc["scenario_name"],
                        "result": step_result,
                    }
                ],
            })

        cucumber_json.append({
            "name": feature_name,
            "uri": feature_path,
            "elements": elements,
        })

    # Write timestamped JSON
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_path = output_dir / f"results_{timestamp}.json"
    output_dir.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w") as f:
        json.dump(cucumber_json, f, indent=2)

    total = sum(len(f["elements"]) for f in cucumber_json)
    passed = sum(
        1
        for f in cucumber_json
        for el in f["elements"]
        if el["steps"][0]["result"]["status"] == "passed"
    )
    failed = sum(
        1
        for f in cucumber_json
        for el in f["elements"]
        if el["steps"][0]["result"]["status"] == "failed"
    )
    skipped = sum(
        1
        for f in cucumber_json
        for el in f["elements"]
        if el["steps"][0]["result"]["status"] == "skipped"
    )

    summary_parts = [f"Passed: {passed}", f"Failed: {failed}", f"Skipped: {skipped}"]
    if crashed_count > 0:
        summary_parts.append(f"Crashed: {crashed_count}")

    print(f"✅ Wrote {output_path} ({len(cucumber_json)} features, {total} scenarios)", file=sys.stderr)
    print(f"   {', '.join(summary_parts)}", file=sys.stderr)

    if crashed_count > 0:
        print(
            f"   ⚠ {crashed_count} scenario(s) crashed — test process died before writing results",
            file=sys.stderr,
        )

    # Print path to stdout for scripts to capture
    print(output_path)


if __name__ == "__main__":
    main()
