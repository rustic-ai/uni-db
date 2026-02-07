#!/usr/bin/env python3
"""
Analyze Match feature failures from TCK JSON results.

Usage:
    python scripts/analyze_match_failures.py target/cucumber/results.json
"""

import json
import sys
from pathlib import Path
from collections import defaultdict
import re


def load_json_results(json_path):
    """Load cucumber JSON results."""
    with open(json_path, 'r') as f:
        return json.load(f)


def extract_failure_info(error_message):
    """Extract structured failure information from error message."""
    info = {
        'type': 'Unknown',
        'details': error_message[:200] if error_message else 'No error message',
        'raw': error_message
    }

    if not error_message:
        return info

    # Categorize by failure type
    if 'Result mismatch' in error_message:
        info['type'] = 'Result mismatch'
        if 'No match found for actual row' in error_message:
            info['subtype'] = 'Wrong data format'
            # Extract what was expected vs actual
            if 'Actual values:' in error_message and 'Expected:' in error_message:
                actual_start = error_message.find('Actual values:')
                expected_start = error_message.find('Expected:')
                info['details'] = error_message[actual_start:expected_start].strip()[:150]
        elif 'Row count mismatch' in error_message:
            info['subtype'] = 'Wrong row count'
            # Extract expected vs got
            match = re.search(r'expected (\d+), got (\d+)', error_message)
            if match:
                info['details'] = f"Expected {match.group(1)} rows, got {match.group(2)}"
        elif 'Expected column .* not found' in error_message or 'not found in result' in error_message:
            info['subtype'] = 'Missing column'
            match = re.search(r"Expected column '([^']+)'", error_message)
            if match:
                info['details'] = f"Missing column: {match.group(1)}"
        else:
            info['subtype'] = 'Other mismatch'

    elif 'No result found' in error_message:
        info['type'] = 'Empty result'
        info['details'] = 'Query returned no results when data was expected'

    elif 'No error found' in error_message:
        info['type'] = 'Missing error'
        info['details'] = 'Query succeeded but should have raised an error'

    elif 'Error mismatch' in error_message or 'Error detail mismatch' in error_message:
        info['type'] = 'Wrong error type'
        # Extract expected error
        match = re.search(r"expected message to contain '([^']+)'", error_message)
        if match:
            info['details'] = f"Expected error: {match.group(1)}"
        else:
            info['details'] = error_message[:150]

    elif 'Failed to parse expected table' in error_message:
        info['type'] = 'Test parsing error'
        info['details'] = 'TCK test harness failed to parse expected results'

    elif 'Could not parse feature file' in error_message:
        info['type'] = 'Feature file error'
        info['details'] = 'Feature file has syntax errors'

    else:
        info['type'] = 'Other error'
        info['details'] = error_message[:150]

    return info


def analyze_match_features(features):
    """Analyze all Match-related features."""
    match_features = [f for f in features if f.get('name', '').startswith('Match')]

    stats = {
        'total_features': len(match_features),
        'total_scenarios': 0,
        'passed_scenarios': 0,
        'failed_scenarios': 0,
        'by_feature': {},
        'failure_types': defaultdict(lambda: defaultdict(int)),
        'failure_details': defaultdict(list),
        'error_examples': defaultdict(list),
    }

    for feature in match_features:
        feature_name = feature.get('name', 'Unknown')
        feature_stats = {
            'passed': 0,
            'failed': 0,
            'total': 0,
            'failures': []
        }

        for element in feature.get('elements', []):
            if element.get('type') != 'scenario':
                continue

            scenario_name = element.get('name', 'Unknown')
            feature_stats['total'] += 1
            stats['total_scenarios'] += 1

            # Check if scenario failed
            failed = False
            failure_info = None

            for step in element.get('steps', []):
                result = step.get('result', {})
                if result.get('status') == 'failed':
                    failed = True
                    error_msg = result.get('error_message', '')
                    failure_info = extract_failure_info(error_msg)
                    break

            if failed:
                feature_stats['failed'] += 1
                stats['failed_scenarios'] += 1
                feature_stats['failures'].append({
                    'scenario': scenario_name,
                    'info': failure_info
                })

                # Track failure types
                stats['failure_types'][failure_info['type']][failure_info.get('subtype', 'general')] += 1
                stats['failure_details'][failure_info['type']].append({
                    'feature': feature_name,
                    'scenario': scenario_name,
                    'details': failure_info['details']
                })

                # Collect examples (limit to 3 per type)
                if len(stats['error_examples'][failure_info['type']]) < 3:
                    stats['error_examples'][failure_info['type']].append({
                        'feature': feature_name,
                        'scenario': scenario_name,
                        'error': failure_info['raw']
                    })
            else:
                feature_stats['passed'] += 1
                stats['passed_scenarios'] += 1

        stats['by_feature'][feature_name] = feature_stats

    return stats


def print_analysis(stats):
    """Print comprehensive analysis."""
    print("=" * 80)
    print("MATCH FEATURES FAILURE ANALYSIS")
    print("=" * 80)
    print()

    # Overall stats
    print("## Overall Statistics")
    print(f"Total Match features: {stats['total_features']}")
    print(f"Total scenarios: {stats['total_scenarios']}")
    print(f"Passed: {stats['passed_scenarios']} ({stats['passed_scenarios']/stats['total_scenarios']*100:.1f}%)")
    print(f"Failed: {stats['failed_scenarios']} ({stats['failed_scenarios']/stats['total_scenarios']*100:.1f}%)")
    print()

    # Per-feature breakdown
    print("## Per-Feature Breakdown")
    print()
    for feature_name, feature_stats in sorted(stats['by_feature'].items()):
        total = feature_stats['total']
        passed = feature_stats['passed']
        failed = feature_stats['failed']
        if total > 0:
            rate = passed / total * 100
            status = "✅" if rate >= 75 else ("⚠️" if rate >= 50 else "❌")
            print(f"{status} {feature_name}")
            print(f"   Passed: {passed}/{total} ({rate:.0f}%)")
            if failed > 0:
                print(f"   Failed: {failed}")
        print()

    # Failure type breakdown
    print("=" * 80)
    print("## Failure Types Breakdown")
    print("=" * 80)
    print()

    total_failures = sum(sum(subtypes.values()) for subtypes in stats['failure_types'].values())

    for failure_type in sorted(stats['failure_types'].keys(),
                               key=lambda x: sum(stats['failure_types'][x].values()),
                               reverse=True):
        subtypes = stats['failure_types'][failure_type]
        type_total = sum(subtypes.values())
        percentage = (type_total / total_failures * 100) if total_failures > 0 else 0

        print(f"### {failure_type}: {type_total} occurrences ({percentage:.1f}%)")
        print()

        # Show subtypes if present
        if len(subtypes) > 1 or 'general' not in subtypes:
            for subtype, count in sorted(subtypes.items(), key=lambda x: x[1], reverse=True):
                if subtype != 'general':
                    print(f"  - {subtype}: {count}")
            print()

        # Show examples
        examples = stats['error_examples'].get(failure_type, [])
        if examples:
            print("  Examples:")
            for i, ex in enumerate(examples[:2], 1):
                print(f"  {i}. {ex['feature']} - {ex['scenario'][:60]}")
                # Show first line of error
                first_line = ex['error'].split('\n')[0] if ex['error'] else 'No error message'
                print(f"     {first_line[:100]}")
            print()

    # Common patterns
    print("=" * 80)
    print("## Common Failure Patterns")
    print("=" * 80)
    print()

    # Result mismatch details
    if 'Result mismatch' in stats['failure_details']:
        print("### Result Mismatch Patterns")
        print()
        mismatches = stats['failure_details']['Result mismatch']

        # Group by pattern
        patterns = defaultdict(list)
        for detail in mismatches:
            if '_vid' in detail['details'] or '_label' in detail['details']:
                patterns['Internal fields exposed'].append(detail)
            elif 'Map({' in detail['details']:
                patterns['Returned Map instead of Node/Relationship'].append(detail)
            elif 'String(' in detail['details'] and 'Int(' in detail['scenario']:
                patterns['Wrong type conversion'].append(detail)
            elif 'Row count mismatch' in detail['details']:
                patterns['Wrong number of results'].append(detail)
            else:
                patterns['Other'].append(detail)

        for pattern, details in sorted(patterns.items(), key=lambda x: len(x[1]), reverse=True):
            print(f"**{pattern}**: {len(details)} cases")
            if details:
                for detail in details[:2]:
                    print(f"  - {detail['feature']}: {detail['scenario'][:50]}")
            print()

    # Empty result details
    if 'Empty result' in stats['failure_details']:
        print("### Empty Result Patterns")
        print()
        empty = stats['failure_details']['Empty result']
        print(f"Total: {len(empty)} cases")
        print()
        feature_counts = defaultdict(int)
        for detail in empty:
            feature_counts[detail['feature']] += 1

        print("By feature:")
        for feature, count in sorted(feature_counts.items(), key=lambda x: x[1], reverse=True):
            print(f"  - {feature}: {count}")
        print()


def main():
    if len(sys.argv) < 2:
        print("Usage: python analyze_match_failures.py <json-file>")
        sys.exit(1)

    json_path = Path(sys.argv[1])
    if not json_path.exists():
        print(f"Error: File not found: {json_path}")
        sys.exit(1)

    print(f"📊 Analyzing Match features from {json_path}...")
    print()

    features = load_json_results(json_path)
    stats = analyze_match_features(features)
    print_analysis(stats)

    print()
    print("✨ Analysis complete!")


if __name__ == '__main__':
    main()
