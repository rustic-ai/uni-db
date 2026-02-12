#!/bin/bash
# Run TCK tests via nextest (parallel, filterable) and generate markdown reports
#
# Usage:
#   scripts/run_tck_with_report.sh              # Run all scenarios
#   scripts/run_tck_with_report.sh "~Match1"    # Filter by pattern

set -e

cd "$(dirname "$0")/.."

# Clean previous per-scenario results
rm -rf target/cucumber/nextest

FILTER_EXPR=""
if [ -n "$1" ]; then
    FILTER_EXPR="-E test($1)"
    echo "🚀 Running TCK tests (filter: $1)..."
else
    echo "🚀 Running TCK tests..."
fi

echo ""

# Run tests via nextest (--no-fail-fast to collect all results)
# shellcheck disable=SC2086
cargo nextest run -p uni-tck --test tck --no-fail-fast $FILTER_EXPR || true

# Aggregate per-scenario results into timestamped cucumber JSON
echo ""
echo "📊 Aggregating results..."

if [ -n "$1" ]; then
    # Filtered run — write results to a separate directory so they don't
    # get picked up as the "previous" baseline for full-run comparisons.
    RESULTS_JSON=$(python3 scripts/aggregate_nextest_results.py --output-dir target/cucumber/filtered)

    echo ""
    echo "ℹ️  Filtered run — results saved to target/cucumber/filtered/"
    echo "ℹ️  Skipping report generation (only full runs update the report)"
    echo ""
else
    RESULTS_JSON=$(python3 scripts/aggregate_nextest_results.py)

    echo ""
    echo "📊 Generating report..."
    echo ""

    # Generate comparative report (auto-finds previous results)
    python3 scripts/analyze_tck_json.py "$RESULTS_JSON"

    echo ""
    echo "📁 Report available at: target/cucumber/report.md"
    echo ""
fi
