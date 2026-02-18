#!/bin/bash
# Regenerate compliance reports from existing results JSON without re-running tests.
#
# Usage:
#   scripts/regenerate_reports.sh              # Regenerate for all modes with results
#   scripts/regenerate_reports.sh schemaless   # Regenerate for schemaless only
#   scripts/regenerate_reports.sh schema       # Regenerate for schema only

set -e

cd "$(dirname "$0")/.."

regenerate_for_dir() {
    local compliance_dir="$1"
    local mode_label
    mode_label=$(basename "$compliance_dir")

    # Find the latest timestamped results file.
    local latest_json
    latest_json=$(find "$compliance_dir" -maxdepth 1 -type f -regextype posix-extended \
        -regex '.*/results_[0-9]{8}_[0-9]{6}\.json' | sort | tail -n 1)

    if [ -z "$latest_json" ]; then
        echo "⏭️  Skipping $mode_label — no timestamped results found in $compliance_dir"
        return
    fi

    echo "📊 Regenerating report for $mode_label from $(basename "$latest_json")..."
    python3 scripts/analyze_tck_json.py "$latest_json"

    cp -f "$latest_json" "$compliance_dir/last_run_results.json"
    cp -f "$compliance_dir/report.md" "$compliance_dir/last_run_report.md"

    echo "📁 Updated: $compliance_dir/report.md"
    echo "📁 Updated: $compliance_dir/last_run_report.md"
    echo ""
}

if [ -n "${1:-}" ]; then
    dir="compliance_reports/$1"
    if [ ! -d "$dir" ]; then
        echo "❌ Directory not found: $dir" >&2
        exit 1
    fi
    regenerate_for_dir "$dir"
else
    for dir in compliance_reports/*/; do
        [ -d "$dir" ] && regenerate_for_dir "$dir"
    done
fi
