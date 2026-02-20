#!/usr/bin/env python3
"""
Analyze TCK results and generate a comparative markdown report.

Automatically finds the previous results file in the same directory to
show progress, regressions, and deltas between runs.

Usage:
    python scripts/analyze_tck_json.py target/cucumber/results_20260211_180000.json

Output:
    - target/cucumber/report.md
"""

import json
import re
import sys
from pathlib import Path
from datetime import datetime


def load_json_results(json_path):
    """Load cucumber JSON results."""
    with open(json_path, 'r') as f:
        return json.load(f)


def build_scenario_index(results):
    """Build a dict of (feature_name, scenario_name, line) -> status."""
    index = {}
    for feature in results:
        fname = feature.get('name', 'Unknown')
        for el in feature.get('elements', []):
            if el.get('type') != 'scenario':
                continue
            sname = el.get('name', 'Unknown')
            line = el.get('line', 0)
            # Determine status from steps
            crashed = any(
                s.get('result', {}).get('status') == 'crashed'
                for s in el.get('steps', [])
            )
            failed = any(
                s.get('result', {}).get('status') == 'failed'
                for s in el.get('steps', [])
            )
            skipped = any(
                s.get('result', {}).get('status') == 'skipped'
                for s in el.get('steps', [])
            )
            if crashed:
                status = 'crashed'
            elif failed:
                status = 'failed'
            elif skipped:
                status = 'skipped'
            else:
                status = 'passed'
            index[(fname, sname, line)] = status
    return index


def analyze_features(results):
    """Analyze all features and return per-feature stats."""
    stats_list = []
    for feature in results:
        stats = {
            'name': feature.get('name', 'Unknown'),
            'uri': feature.get('uri', ''),
            'total': 0,
            'passed': 0,
            'failed': 0,
            'skipped': 0,
            'crashed': 0,
            'scenarios': [],
        }
        for el in feature.get('elements', []):
            if el.get('type') != 'scenario':
                continue
            stats['total'] += 1
            sname = el.get('name', 'Unknown')
            line = el.get('line', 0)

            crashed = any(
                s.get('result', {}).get('status') == 'crashed'
                for s in el.get('steps', [])
            )
            failed = any(
                s.get('result', {}).get('status') == 'failed'
                for s in el.get('steps', [])
            )
            skipped = any(
                s.get('result', {}).get('status') == 'skipped'
                for s in el.get('steps', [])
            )
            if crashed:
                status = 'crashed'
                stats['crashed'] += 1
            elif failed:
                status = 'failed'
                stats['failed'] += 1
            elif skipped:
                status = 'skipped'
                stats['skipped'] += 1
            else:
                status = 'passed'
                stats['passed'] += 1

            error = ''
            for s in el.get('steps', []):
                if s.get('result', {}).get('status') in ('failed', 'crashed'):
                    error = s.get('result', {}).get('error_message', '')
                    break

            stats['scenarios'].append({
                'name': sname,
                'line': line,
                'status': status,
                'error': error,
            })
        stats_list.append(stats)
    return stats_list


def find_previous_results(current_path):
    """Find the most recent results_*.json before the current one."""
    directory = current_path.parent
    current_name = current_path.name

    # Match results_YYYYMMDD_HHMMSS.json
    pattern = re.compile(r'^results_\d{8}_\d{6}\.json$')
    candidates = sorted(
        p for p in directory.glob("results_*.json")
        if pattern.match(p.name) and p.name < current_name
    )
    return candidates[-1] if candidates else None


def delta_str(current, previous):
    """Format a delta like +5 or -3 or (unchanged)."""
    d = current - previous
    if d > 0:
        return f"+{d}"
    elif d < 0:
        return str(d)
    return ""


def generate_report(current_stats, prev_index, current_path, prev_path, output_path):
    """Generate the comparative markdown report."""
    md = []
    md.append("# TCK Compliance Report")
    md.append("")
    md.append(f"**Generated:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    md.append(f"**Results:** `{current_path.name}`")
    if prev_path:
        md.append(f"**Compared to:** `{prev_path.name}`")
    md.append("")

    # --- Summary ---
    total = sum(s['total'] for s in current_stats)
    passed = sum(s['passed'] for s in current_stats)
    failed = sum(s['failed'] for s in current_stats)
    skipped = sum(s['skipped'] for s in current_stats)
    crashed = sum(s['crashed'] for s in current_stats)
    rate = (passed / total * 100) if total > 0 else 0.0

    md.append("## Summary")
    md.append("")

    if prev_index is not None:
        prev_total = len(prev_index)
        prev_passed = sum(1 for v in prev_index.values() if v == 'passed')
        prev_failed = sum(1 for v in prev_index.values() if v == 'failed')
        prev_crashed = sum(1 for v in prev_index.values() if v == 'crashed')
        prev_rate = (prev_passed / prev_total * 100) if prev_total > 0 else 0.0

        # Compute regressions / fixes
        current_index = {}
        for s in current_stats:
            for sc in s['scenarios']:
                current_index[(s['name'], sc['name'], sc['line'])] = sc['status']

        regressions = []  # were passing, now failing/crashed
        fixes = []        # were failing/crashed, now passing
        for key in current_index:
            cur = current_index[key]
            prev = prev_index.get(key)
            if prev == 'passed' and cur in ('failed', 'crashed'):
                regressions.append(key)
            elif prev in ('failed', 'crashed') and cur == 'passed':
                fixes.append(key)

        new_scenarios = [k for k in current_index if k not in prev_index]

        rate_delta = rate - prev_rate
        rate_arrow = "📈" if rate_delta > 0 else ("📉" if rate_delta < 0 else "➡️")

        md.append(f"| Metric | Current | Previous | Delta |")
        md.append(f"|--------|---------|----------|-------|")
        md.append(f"| Scenarios | {total} | {prev_total} | {delta_str(total, prev_total)} |")
        md.append(f"| Passed | {passed} | {prev_passed} | {delta_str(passed, prev_passed)} |")
        md.append(f"| Failed | {failed} | {prev_failed} | {delta_str(failed, prev_failed)} |")
        if crashed > 0 or prev_crashed > 0:
            md.append(f"| Crashed | {crashed} | {prev_crashed} | {delta_str(crashed, prev_crashed)} |")
        md.append(f"| Pass Rate | {rate:.1f}% | {prev_rate:.1f}% | {rate_arrow} {rate_delta:+.1f}pp |")
        md.append("")

        if fixes:
            md.append(f"**🟢 Fixed:** {len(fixes)} scenarios now passing")
            md.append("")
        if regressions:
            md.append(f"**🔴 Regressions:** {len(regressions)} scenarios now failing")
            md.append("")
        if new_scenarios:
            md.append(f"**🆕 New:** {len(new_scenarios)} scenarios added")
            md.append("")
    else:
        md.append(f"| Metric | Value |")
        md.append(f"|--------|-------|")
        md.append(f"| Total Scenarios | {total} |")
        md.append(f"| Passed | {passed} ({rate:.1f}%) |")
        md.append(f"| Failed | {failed} |")
        if crashed > 0:
            md.append(f"| Crashed | {crashed} |")
        md.append(f"| Skipped | {skipped} |")
        md.append("")
        md.append("*No previous run found for comparison.*")
        md.append("")

    # --- Feature Breakdown ---
    md.append("## Feature Breakdown")
    md.append("")

    if prev_index is not None:
        # Build previous per-feature stats
        prev_by_feature = {}
        for (fname, _, _), status in prev_index.items():
            if fname not in prev_by_feature:
                prev_by_feature[fname] = {'passed': 0, 'failed': 0, 'total': 0}
            prev_by_feature[fname]['total'] += 1
            if status == 'passed':
                prev_by_feature[fname]['passed'] += 1
            elif status == 'failed':
                prev_by_feature[fname]['failed'] += 1

        md.append("| Feature | Scenarios | Passed | Failed | Rate | Delta |")
        md.append("|---------|-----------|--------|--------|------|-------|")

        for stats in sorted(current_stats, key=lambda x: x['name']):
            t, p, f = stats['total'], stats['passed'], stats['failed']
            r = (p / t * 100) if t > 0 else 0
            icon = "✅" if r >= 80 else ("⚠️" if r >= 50 else "❌")

            prev_f = prev_by_feature.get(stats['name'])
            if prev_f:
                prev_r = (prev_f['passed'] / prev_f['total'] * 100) if prev_f['total'] > 0 else 0
                d = r - prev_r
                delta_col = f"{d:+.0f}pp" if d != 0 else ""
            else:
                delta_col = "🆕"

            md.append(f"| {icon} {stats['name']} | {t} | {p} | {f} | {r:.0f}% | {delta_col} |")
    else:
        md.append("| Feature | Scenarios | Passed | Failed | Rate |")
        md.append("|---------|-----------|--------|--------|------|")

        for stats in sorted(current_stats, key=lambda x: x['name']):
            t, p, f = stats['total'], stats['passed'], stats['failed']
            r = (p / t * 100) if t > 0 else 0
            icon = "✅" if r >= 80 else ("⚠️" if r >= 50 else "❌")
            md.append(f"| {icon} {stats['name']} | {t} | {p} | {f} | {r:.0f}% |")

    md.append("")

    # --- Regressions (if comparative) ---
    if prev_index is not None and regressions:
        md.append("## 🔴 Regressions")
        md.append("")
        md.append("Scenarios that were passing but are now failing:")
        md.append("")
        for fname, sname, line in sorted(regressions):
            md.append(f"- **{fname}** — {sname} (line {line})")
        md.append("")

    # --- Fixes (if comparative) ---
    if prev_index is not None and fixes:
        md.append("## 🟢 Newly Passing")
        md.append("")
        md.append("Scenarios that were failing but are now passing:")
        md.append("")
        for fname, sname, line in sorted(fixes):
            md.append(f"- **{fname}** — {sname} (line {line})")
        md.append("")

    # --- Crashed Scenarios ---
    if crashed > 0:
        md.append("## Crashed Scenarios")
        md.append("")
        md.append("These scenarios crashed (SIGABRT/segfault/OOM) before writing a result:")
        md.append("")

        for stats in sorted(current_stats, key=lambda x: x['name']):
            crashed_scenarios = [s for s in stats['scenarios'] if s['status'] == 'crashed']
            if not crashed_scenarios:
                continue
            md.append(f"### {stats['name']}")
            md.append("")
            for sc in crashed_scenarios:
                md.append(f"- **{sc['name']}** (line {sc['line']})")
                if sc['error']:
                    error = sc['error'][:300]
                    md.append(f"  ```")
                    md.append(f"  {error}")
                    if len(sc['error']) > 300:
                        md.append(f"  ... (truncated)")
                    md.append(f"  ```")
            md.append("")

    # --- Failed Scenarios ---
    md.append("## Failed Scenarios")
    md.append("")

    has_failures = False
    for stats in sorted(current_stats, key=lambda x: x['name']):
        failed_scenarios = [s for s in stats['scenarios'] if s['status'] == 'failed']
        if not failed_scenarios:
            continue
        has_failures = True
        md.append(f"### {stats['name']}")
        md.append("")
        for sc in failed_scenarios:
            md.append(f"- **{sc['name']}** (line {sc['line']})")
            if sc['error']:
                error = sc['error'][:300]
                md.append(f"  ```")
                md.append(f"  {error}")
                if len(sc['error']) > 300:
                    md.append(f"  ... (truncated)")
                md.append(f"  ```")
        md.append("")

    if not has_failures:
        md.append("🎉 No failed scenarios!")
        md.append("")

    with open(output_path, 'w') as f_out:
        f_out.write('\n'.join(md))

    print(f"✅ Generated: {output_path}")


def main():
    if len(sys.argv) < 2:
        print("Usage: python analyze_tck_json.py <results-json>")
        sys.exit(1)

    current_path = Path(sys.argv[1])
    if not current_path.exists():
        print(f"Error: File not found: {current_path}")
        sys.exit(1)

    print(f"📊 Analyzing {current_path.name}...")

    current_results = load_json_results(current_path)
    current_stats = analyze_features(current_results)

    # Find and load previous results
    prev_path = find_previous_results(current_path)
    prev_index = None
    if prev_path:
        print(f"📊 Comparing against {prev_path.name}...")
        prev_results = load_json_results(prev_path)
        prev_index = build_scenario_index(prev_results)
    else:
        print("   No previous results found — generating baseline report.")

    output_path = current_path.parent / "report.md"
    generate_report(current_stats, prev_index, current_path, prev_path, output_path)

    # Print summary
    total = sum(s['total'] for s in current_stats)
    passed = sum(s['passed'] for s in current_stats)
    failed = sum(s['failed'] for s in current_stats)
    crashed = sum(s['crashed'] for s in current_stats)
    rate = (passed / total * 100) if total > 0 else 0.0

    print("")
    summary = f"📈 Summary: {passed}/{total} passed ({rate:.1f}%), {failed} failed"
    if crashed > 0:
        summary += f", {crashed} crashed"
    print(summary)
    if prev_index is not None:
        prev_passed = sum(1 for v in prev_index.values() if v == 'passed')
        d = passed - prev_passed
        if d > 0:
            print(f"   📈 +{d} scenarios passing vs previous run")
        elif d < 0:
            print(f"   📉 {d} scenarios passing vs previous run")
        else:
            print(f"   ➡️  No change vs previous run")
    print("")


if __name__ == '__main__':
    main()
