#!/usr/bin/env python3
"""
Analyze TCK test failures and categorize them by type.

Categories:
- Core Database: Fundamental graph operations (MATCH, CREATE, traversal, properties)
- Addon Features: Extensions like vector search, full-text search, procedures
- Higher Layer: Query planning, optimization, result formatting
- TCK Issues: Test framework or specification issues
"""

import json
import re
from pathlib import Path
from collections import defaultdict
from datetime import datetime


# Categorization patterns
PATTERNS = {
    "core_database": [
        r"property.*not found",
        r"node.*not found",
        r"edge.*not found",
        r"relationship.*not found",
        r"traversal",
        r"adjacency",
        r"vid|eid",
        r"storage layer",
        r"lance",
        r"create.*node",
        r"create.*relationship",
        r"delete.*node",
        r"delete.*relationship",
        r"set.*property",
        r"remove.*property",
        r"label.*not found",
        r"type.*not found",
    ],
    "addon_features": [
        r"vector.*search",
        r"db\.idx\.vector",
        r"full.*text",
        r"spatial",
        r"apoc\.",
        r"procedure",
        r"function.*not.*found",
        r"call.*db\.",
    ],
    "higher_layer": [
        r"planner",
        r"optimizer",
        r"datafusion",
        r"logical.*plan",
        r"physical.*plan",
        r"projection",
        r"aggregation",
        r"order.*by",
        r"limit",
        r"skip",
        r"distinct",
        r"union",
        r"join",
    ],
    "tck_issues": [
        r"cucumber",
        r"step.*definition",
        r"scenario.*not.*found",
        r"feature.*file",
        r"parsing.*scenario",
        r"undefined.*step",
    ],
}


def find_latest_results():
    """Find the most recent TCK results JSON file."""
    results_dir = Path("target/cucumber")
    if not results_dir.exists():
        raise FileNotFoundError(f"Results directory not found: {results_dir}")

    json_files = list(results_dir.glob("results_*.json"))
    if not json_files:
        raise FileNotFoundError(f"No results files found in {results_dir}")

    # Sort by modification time
    latest = max(json_files, key=lambda p: p.stat().st_mtime)
    return latest


def categorize_failure(failure_info):
    """Categorize a failure based on its error message and context."""
    text = (failure_info.get("error_message", "") + " " +
            failure_info.get("stack_trace", "")).lower()

    scores = defaultdict(int)

    for category, patterns in PATTERNS.items():
        for pattern in patterns:
            if re.search(pattern, text, re.IGNORECASE):
                scores[category] += 1

    if scores:
        # Return category with highest score
        return max(scores.items(), key=lambda x: x[1])[0]

    # Default to core_database if no pattern matches
    return "core_database"


def extract_failures(results_data):
    """Extract all failures from the TCK results JSON."""
    failures = []

    # Handle different JSON structures
    if isinstance(results_data, list):
        features = results_data
    elif isinstance(results_data, dict) and "features" in results_data:
        features = results_data["features"]
    else:
        features = [results_data]

    for feature in features:
        feature_name = feature.get("name", "Unknown Feature")

        for element in feature.get("elements", []):
            scenario_name = element.get("name", "Unknown Scenario")

            for step in element.get("steps", []):
                if step.get("result", {}).get("status") == "failed":
                    error_msg = step.get("result", {}).get("error_message", "")

                    failures.append({
                        "feature": feature_name,
                        "scenario": scenario_name,
                        "step": step.get("name", ""),
                        "error_message": error_msg,
                        "stack_trace": step.get("result", {}).get("stack_trace", ""),
                    })

    return failures


def generate_report(categorized_failures):
    """Generate a categorized failure report."""
    category_names = {
        "core_database": "Core Database",
        "addon_features": "Addon Features",
        "higher_layer": "Higher Layer",
        "tck_issues": "TCK Issues",
    }

    print("=" * 80)
    print("TCK FAILURE ANALYSIS REPORT")
    print("=" * 80)
    print()

    total_failures = sum(len(failures) for failures in categorized_failures.values())
    print(f"Total Failures: {total_failures}")
    print()

    for category in ["core_database", "addon_features", "higher_layer", "tck_issues"]:
        failures = categorized_failures.get(category, [])
        if not failures:
            continue

        print(f"\n{category_names[category].upper()} ({len(failures)} failures)")
        print("-" * 80)

        for i, failure in enumerate(failures, 1):
            print(f"\n{i}. {failure['feature']}")
            print(f"   Scenario: {failure['scenario']}")
            print(f"   Step: {failure['step']}")
            if failure['error_message']:
                # Truncate long error messages
                error = failure['error_message'][:200]
                if len(failure['error_message']) > 200:
                    error += "..."
                print(f"   Error: {error}")

    # Summary table
    print("\n" + "=" * 80)
    print("SUMMARY BY CATEGORY")
    print("=" * 80)
    for category in ["core_database", "addon_features", "higher_layer", "tck_issues"]:
        count = len(categorized_failures.get(category, []))
        percentage = (count / total_failures * 100) if total_failures > 0 else 0
        print(f"{category_names[category]:20} {count:4} ({percentage:5.1f}%)")


def save_report(categorized_failures, output_file):
    """Save the categorized failures to a JSON file."""
    output = {
        "timestamp": datetime.now().isoformat(),
        "total_failures": sum(len(f) for f in categorized_failures.values()),
        "categories": {
            category: [
                {
                    "feature": f["feature"],
                    "scenario": f["scenario"],
                    "step": f["step"],
                    "error": f["error_message"][:500],  # Truncate for JSON
                }
                for f in failures
            ]
            for category, failures in categorized_failures.items()
        }
    }

    with open(output_file, "w") as f:
        json.dump(output, f, indent=2)

    print(f"\n\nDetailed report saved to: {output_file}")


def main():
    try:
        # Find latest results
        results_file = find_latest_results()
        print(f"Analyzing: {results_file}")
        print()

        # Load results
        with open(results_file) as f:
            results = json.load(f)

        # Extract failures
        failures = extract_failures(results)

        if not failures:
            print("No failures found! 🎉")
            return

        # Categorize failures
        categorized = defaultdict(list)
        for failure in failures:
            category = categorize_failure(failure)
            categorized[category].append(failure)

        # Generate report
        generate_report(categorized)

        # Save detailed report
        output_file = Path("target/cucumber/failure_analysis.json")
        output_file.parent.mkdir(parents=True, exist_ok=True)
        save_report(categorized, output_file)

    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()
        return 1

    return 0


if __name__ == "__main__":
    exit(main())
