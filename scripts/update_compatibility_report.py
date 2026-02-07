#!/usr/bin/env python3
"""
Update COMPATIBILITY_REPORT.md based on TCK JSON results.

Usage:
    python scripts/update_compatibility_report.py target/cucumber/results.json
"""

import json
import sys
from pathlib import Path
from datetime import datetime
from collections import defaultdict


def load_json_results(json_path):
    """Load cucumber JSON results."""
    with open(json_path, 'r') as f:
        return json.load(f)


def analyze_results(features):
    """Analyze all features and generate comprehensive statistics."""
    stats = {
        'total_features': len(features),
        'total_scenarios': 0,
        'passed_scenarios': 0,
        'failed_scenarios': 0,
        'skipped_scenarios': 0,
        'total_steps': 0,
        'passed_steps': 0,
        'failed_steps': 0,
        'skipped_steps': 0,
        'by_category': defaultdict(lambda: {'passed': 0, 'failed': 0, 'total': 0}),
        'by_feature': {},
        'failure_types': defaultdict(int),
    }

    for feature in features:
        feature_name = feature.get('name', 'Unknown')
        feature_passed = 0
        feature_failed = 0
        feature_total = 0

        for element in feature.get('elements', []):
            if element.get('type') != 'scenario':
                continue

            feature_total += 1
            stats['total_scenarios'] += 1

            # Analyze scenario steps
            scenario_passed = True
            scenario_failed = False

            for step in element.get('steps', []):
                stats['total_steps'] += 1
                result = step.get('result', {})
                status = result.get('status', 'undefined')

                if status == 'passed':
                    stats['passed_steps'] += 1
                elif status == 'failed':
                    stats['failed_steps'] += 1
                    scenario_passed = False
                    scenario_failed = True

                    # Categorize failure type
                    error_msg = result.get('error_message', '')
                    if 'Result mismatch' in error_msg:
                        stats['failure_types']['Result mismatch'] += 1
                    elif 'No result found' in error_msg:
                        stats['failure_types']['No result found'] += 1
                    elif 'No error found' in error_msg:
                        stats['failure_types']['No error found'] += 1
                    elif 'Error detail mismatch' in error_msg or 'Error mismatch' in error_msg:
                        stats['failure_types']['Error detail mismatch'] += 1
                    else:
                        stats['failure_types']['Other errors'] += 1
                elif status == 'skipped':
                    stats['skipped_steps'] += 1

            if scenario_failed:
                feature_failed += 1
                stats['failed_scenarios'] += 1
            elif scenario_passed:
                feature_passed += 1
                stats['passed_scenarios'] += 1
            else:
                stats['skipped_scenarios'] += 1

        # Store per-feature stats
        stats['by_feature'][feature_name] = {
            'passed': feature_passed,
            'failed': feature_failed,
            'total': feature_total
        }

        # Categorize by feature category
        category = extract_category(feature_name)
        stats['by_category'][category]['passed'] += feature_passed
        stats['by_category'][category]['failed'] += feature_failed
        stats['by_category'][category]['total'] += feature_total

    return stats


def extract_category(feature_name):
    """Extract category from feature name."""
    # Remove numbers and special chars from end
    parts = feature_name.split(' - ')
    if parts:
        base = parts[0]
        # Remove trailing numbers
        import re
        category = re.sub(r'\d+$', '', base).strip()
        return category
    return 'Other'


def generate_report_summary(stats):
    """Generate the executive summary section."""
    pass_rate = (stats['passed_scenarios'] / stats['total_scenarios'] * 100) if stats['total_scenarios'] > 0 else 0
    step_pass_rate = (stats['passed_steps'] / stats['total_steps'] * 100) if stats['total_steps'] > 0 else 0

    return f"""## Executive Summary

| Metric | Count | Pass Rate |
|--------|-------|-----------|
| **Features** | {stats['total_features']} | - |
| **Scenarios** | {stats['total_scenarios']:,} | **{pass_rate:.1f}%** ({stats['passed_scenarios']:,} passed, {stats['failed_scenarios']:,} failed) |
| **Steps** | {stats['total_steps']:,} | **{step_pass_rate:.1f}%** ({stats['passed_steps']:,} passed, {stats['failed_steps']:,} failed) |

The high step pass rate ({step_pass_rate:.1f}%) vs lower scenario pass rate ({pass_rate:.1f}%) indicates that most basic operations work, but many scenarios fail at specific assertion points.
"""


def generate_category_breakdown(stats):
    """Generate category pass rate table."""
    lines = []
    lines.append("## Category Pass Rates\n")
    lines.append("| Category | Passed | Failed | Total | Rate |")
    lines.append("|----------|--------|--------|-------|------|")

    # Sort by pass rate descending
    categories = []
    for cat, data in stats['by_category'].items():
        if data['total'] > 0:
            rate = data['passed'] / data['total'] * 100
            categories.append((cat, data['passed'], data['failed'], data['total'], rate))

    categories.sort(key=lambda x: x[4], reverse=True)

    for cat, passed, failed, total, rate in categories:
        bold = "**" if rate >= 75 or rate == 0 else ""
        lines.append(f"| {bold}{cat}{bold} | {bold}{passed}{bold} | {bold}{failed}{bold} | {bold}{total}{bold} | {bold}{rate:.1f}%{bold} |")

    return '\n'.join(lines)


def main():
    if len(sys.argv) < 2:
        print("Usage: python update_compatibility_report.py <json-file>")
        sys.exit(1)

    json_path = Path(sys.argv[1])
    if not json_path.exists():
        print(f"Error: File not found: {json_path}")
        sys.exit(1)

    print(f"📊 Analyzing {json_path}...")

    # Load and analyze results
    features = load_json_results(json_path)
    stats = analyze_results(features)

    print(f"✅ Analysis complete!")
    print(f"   Features: {stats['total_features']}")
    print(f"   Scenarios: {stats['total_scenarios']} ({stats['passed_scenarios']} passed, {stats['failed_scenarios']} failed)")
    print(f"   Pass rate: {stats['passed_scenarios']/stats['total_scenarios']*100:.1f}%")

    # Generate report sections
    summary = generate_report_summary(stats)
    category_breakdown = generate_category_breakdown(stats)

    # TODO: Generate full report and update COMPATIBILITY_REPORT.md
    # For now, just print the summary
    print("\n" + "="*80)
    print(summary)
    print("\n" + "="*80)
    print(category_breakdown)


if __name__ == '__main__':
    main()
