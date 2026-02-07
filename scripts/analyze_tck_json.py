#!/usr/bin/env python3
"""
Analyze Cucumber JSON test results and generate markdown reports.

Usage:
    python scripts/analyze_tck_json.py target/cucumber/results.json

Output:
    - target/cucumber/report.md (main report)
    - target/cucumber/match-report.md (Match features)
    - target/cucumber/where-report.md (Where features)
    - target/cucumber/return-report.md (Return features)
"""

import json
import sys
from pathlib import Path
from collections import defaultdict
from datetime import datetime


def load_json_results(json_path):
    """Load cucumber JSON results."""
    with open(json_path, 'r') as f:
        return json.load(f)


def analyze_feature(feature):
    """Analyze a single feature and return statistics."""
    stats = {
        'name': feature.get('name', 'Unknown'),
        'uri': feature.get('uri', ''),
        'total_scenarios': 0,
        'passed_scenarios': 0,
        'failed_scenarios': 0,
        'skipped_scenarios': 0,
        'scenarios': []
    }

    for element in feature.get('elements', []):
        if element.get('type') == 'scenario':
            stats['total_scenarios'] += 1

            scenario_info = {
                'name': element.get('name', 'Unknown'),
                'line': element.get('line', 0),
                'steps': []
            }

            # Check scenario status based on steps
            scenario_passed = True
            scenario_failed = False
            scenario_skipped = False

            for step in element.get('steps', []):
                step_result = step.get('result', {})
                status = step_result.get('status', 'undefined')

                step_info = {
                    'keyword': step.get('keyword', ''),
                    'name': step.get('name', ''),
                    'status': status,
                    'error': step_result.get('error_message', '')
                }
                scenario_info['steps'].append(step_info)

                if status == 'failed':
                    scenario_passed = False
                    scenario_failed = True
                elif status == 'skipped':
                    scenario_skipped = True
                elif status != 'passed':
                    scenario_passed = False

            scenario_info['status'] = 'failed' if scenario_failed else ('skipped' if scenario_skipped else ('passed' if scenario_passed else 'undefined'))

            if scenario_info['status'] == 'passed':
                stats['passed_scenarios'] += 1
            elif scenario_info['status'] == 'failed':
                stats['failed_scenarios'] += 1
            else:
                stats['skipped_scenarios'] += 1

            stats['scenarios'].append(scenario_info)

    return stats


def filter_features_by_name(features, prefixes):
    """Filter features by name prefix."""
    return [f for f in features if any(f.get('name', '').startswith(prefix) for prefix in prefixes)]


def generate_summary_stats(all_stats):
    """Generate summary statistics."""
    summary = {
        'total_features': len(all_stats),
        'total_scenarios': sum(s['total_scenarios'] for s in all_stats),
        'passed_scenarios': sum(s['passed_scenarios'] for s in all_stats),
        'failed_scenarios': sum(s['failed_scenarios'] for s in all_stats),
        'skipped_scenarios': sum(s['skipped_scenarios'] for s in all_stats),
    }

    if summary['total_scenarios'] > 0:
        summary['pass_rate'] = (summary['passed_scenarios'] / summary['total_scenarios']) * 100
    else:
        summary['pass_rate'] = 0.0

    return summary


def generate_markdown_report(stats_list, title, output_path):
    """Generate a markdown report from statistics."""
    summary = generate_summary_stats(stats_list)

    md = []
    md.append(f"# {title}")
    md.append(f"\nGenerated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")

    # Summary section
    md.append("## Summary")
    md.append(f"- **Total Features**: {summary['total_features']}")
    md.append(f"- **Total Scenarios**: {summary['total_scenarios']}")
    md.append(f"- **Passed**: {summary['passed_scenarios']} ({summary['pass_rate']:.1f}%)")
    md.append(f"- **Failed**: {summary['failed_scenarios']}")
    md.append(f"- **Skipped**: {summary['skipped_scenarios']}")
    md.append("")

    # Feature breakdown
    md.append("## Feature Breakdown")
    md.append("")
    md.append("| Feature | Scenarios | Passed | Failed | Pass Rate |")
    md.append("|---------|-----------|--------|--------|-----------|")

    for stats in sorted(stats_list, key=lambda x: x['name']):
        total = stats['total_scenarios']
        passed = stats['passed_scenarios']
        failed = stats['failed_scenarios']
        pass_rate = (passed / total * 100) if total > 0 else 0

        status_icon = "✅" if pass_rate >= 80 else ("⚠️" if pass_rate >= 50 else "❌")

        md.append(f"| {status_icon} {stats['name']} | {total} | {passed} | {failed} | {pass_rate:.0f}% |")

    md.append("")

    # Failed scenarios detail
    md.append("## Failed Scenarios")
    md.append("")

    has_failures = False
    for stats in sorted(stats_list, key=lambda x: x['name']):
        failed_scenarios = [s for s in stats['scenarios'] if s['status'] == 'failed']

        if failed_scenarios:
            has_failures = True
            md.append(f"### {stats['name']}")
            md.append("")

            for scenario in failed_scenarios:
                md.append(f"#### ❌ {scenario['name']}")
                md.append("")

                # Show failed steps
                for step in scenario['steps']:
                    if step['status'] == 'failed':
                        md.append(f"- **{step['keyword']}{step['name']}**")
                        if step['error']:
                            # Truncate long error messages
                            error = step['error'][:500]
                            md.append(f"  ```")
                            md.append(f"  {error}")
                            if len(step['error']) > 500:
                                md.append(f"  ... (truncated)")
                            md.append(f"  ```")
                md.append("")

    if not has_failures:
        md.append("🎉 No failed scenarios!")
        md.append("")

    # Write to file
    with open(output_path, 'w') as f:
        f.write('\n'.join(md))

    print(f"✅ Generated: {output_path}")


def main():
    if len(sys.argv) < 2:
        print("Usage: python analyze_tck_json.py <json-file>")
        sys.exit(1)

    json_path = Path(sys.argv[1])
    if not json_path.exists():
        print(f"Error: File not found: {json_path}")
        sys.exit(1)

    print(f"📊 Analyzing {json_path}...")

    # Load JSON results
    results = load_json_results(json_path)

    # Analyze all features
    all_stats = [analyze_feature(feature) for feature in results]

    # Generate overall report
    output_dir = json_path.parent
    generate_markdown_report(
        all_stats,
        "TCK Test Results - All Features",
        output_dir / "report.md"
    )

    # Filter and generate Match report
    match_stats = [s for s in all_stats if s['name'].startswith('Match') and not s['name'].startswith('MatchWhere')]
    if match_stats:
        generate_markdown_report(
            match_stats,
            "TCK Test Results - Match Features",
            output_dir / "match-report.md"
        )

    # Filter and generate MatchWhere report
    matchwhere_stats = [s for s in all_stats if s['name'].startswith('MatchWhere')]
    if matchwhere_stats:
        generate_markdown_report(
            matchwhere_stats,
            "TCK Test Results - Match-Where Features",
            output_dir / "where-report.md"
        )

    # Filter and generate Return report
    return_stats = [s for s in all_stats if s['name'].startswith('Return')]
    if return_stats:
        generate_markdown_report(
            return_stats,
            "TCK Test Results - Return Features",
            output_dir / "return-report.md"
        )

    print("")
    print("📈 Summary:")
    summary = generate_summary_stats(all_stats)
    print(f"  Total Features: {summary['total_features']}")
    print(f"  Total Scenarios: {summary['total_scenarios']}")
    print(f"  Passed: {summary['passed_scenarios']} ({summary['pass_rate']:.1f}%)")
    print(f"  Failed: {summary['failed_scenarios']}")
    print("")
    print("✨ Done!")


if __name__ == '__main__':
    main()
