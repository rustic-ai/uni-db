#!/bin/bash
# Run TCK tests via nextest (parallel, filterable) and maintain compliance artifacts.
#
# Agent runbook:
#   - This script is expensive (large TCK suite). Re-run only after code changes
#     that can affect query/runtime behavior.
#   - Use filtered runs during investigation; use full runs for final artifact updates.
#
# Usage:
#   scripts/run_tck_with_report.sh
#     Full run in default mode (schemaless).
#
#   UNI_TCK_SCHEMA_MODE=sidecar scripts/run_tck_with_report.sh
#     Full run in schema mode (mapped from sidecar -> compliance_reports/schema).
#
#   scripts/run_tck_with_report.sh --both
#     Full runs for both modes (schemaless, then sidecar).
#
#   scripts/run_tck_with_report.sh "~Match1"
#   scripts/run_tck_with_report.sh --both "~Match1"
#     Filtered runs for quick checks (no checked-in artifact updates).
#
# Output locations:
#   - Raw per-scenario nextest JSON:
#       target/cucumber/nextest/<mode>/
#   - Aggregated run JSON (ephemeral):
#       target/cucumber/<mode>/results_YYYYMMDD_HHMMSS.json
#       target/cucumber/<mode>/filtered/results_YYYYMMDD_HHMMSS.json
#   - Checked-in compliance artifacts (source of truth for latest committed state):
#       compliance_reports/schemaless/
#       compliance_reports/schema/
#         - latest 2 results_*.json
#         - report.md (latest full-run report)
#         - last_run_results.json (stable pointer to latest full run)
#         - last_run_report.md   (stable pointer to latest full-run report)
#
# Important behavior:
#   - Filtered runs are exploratory and intentionally do NOT update
#     compliance_reports/*, report.md, or last_run_* snapshots.
#   - Only full runs refresh checked-in compliance artifacts.

set -e

cd "$(dirname "$0")/.."

compliance_mode_dir() {
    local mode="$1"
    case "$mode" in
        sidecar)
            echo "schema"
            ;;
        schemaless)
            echo "schemaless"
            ;;
        *)
            echo "❌ Invalid mode for compliance reports: '$mode'" >&2
            exit 1
            ;;
    esac
}

sync_compliance_reports() {
    local mode="$1"
    local results_json="$2"
    local compliance_mode
    local compliance_dir
    local latest_json
    local last_run_json
    local last_run_report
    local -a json_files=()
    local remove_count
    local i

    compliance_mode=$(compliance_mode_dir "$mode") || exit 1
    compliance_dir="compliance_reports/$compliance_mode"
    mkdir -p "$compliance_dir"

    latest_json="$compliance_dir/$(basename "$results_json")"
    cp -f "$results_json" "$latest_json"

    # Prune old timestamped results, keeping only the latest 2.
    # Use a strict regex to match only YYYYMMDD_HHMMSS filenames (avoids
    # non-timestamped files like results_baseline.json poisoning the sort).
    mapfile -t json_files < <(find "$compliance_dir" -maxdepth 1 -type f -regextype posix-extended -regex '.*/results_[0-9]{8}_[0-9]{6}\.json' | sort)
    if [ "${#json_files[@]}" -gt 2 ]; then
        remove_count=$(( ${#json_files[@]} - 2 ))
        for ((i = 0; i < remove_count; i++)); do
            rm -f "${json_files[$i]}"
        done
    fi

    python3 scripts/analyze_tck_json.py "$latest_json"

    last_run_json="$compliance_dir/last_run_results.json"
    last_run_report="$compliance_dir/last_run_report.md"
    cp -f "$latest_json" "$last_run_json"
    cp -f "$compliance_dir/report.md" "$last_run_report"
}

normalize_mode() {
    local raw="${1:-schemaless}"
    raw="$(echo "$raw" | tr '[:upper:]' '[:lower:]' | xargs)"
    case "$raw" in
        ""|schemaless|off|none)
            echo "schemaless"
            ;;
        schema|sidecar|predefined|predefined-schema)
            echo "sidecar"
            ;;
        *)
            echo "❌ Invalid UNI_TCK_SCHEMA_MODE: '$1'" >&2
            echo "   Expected one of: schemaless, sidecar" >&2
            exit 1
            ;;
    esac
}

run_for_mode() {
    local mode="$1"
    local filter="$2"

    local raw_results_dir="target/cucumber/nextest/$mode"
    local output_dir="target/cucumber/$mode"
    local -a filter_args=()
    local results_json

    if [ -n "$filter" ]; then
        filter_args=(-E "test($filter)")
        output_dir="$output_dir/filtered"
        echo "🚀 Running TCK tests in '$mode' mode (filter: $filter)..."
    else
        echo "🚀 Running TCK tests in '$mode' mode..."
    fi

    # Clean previous per-scenario results only for this mode.
    rm -rf "$raw_results_dir"

    # For full (unfiltered) runs, write a manifest of all discovered scenarios
    # so the aggregator can detect crashed tests that never wrote a result.
    if [ -z "$filter" ]; then
        UNI_TCK_SCHEMA_MODE="$mode" \
        UNI_TCK_NEXTEST_RESULTS_DIR="$raw_results_dir" \
        UNI_TCK_WRITE_MANIFEST=1 \
        cargo nextest list -p uni-tck --test tck >/dev/null 2>&1
    fi

    echo ""
    # Run tests via nextest (--no-fail-fast to collect all results)
    UNI_TCK_SCHEMA_MODE="$mode" \
    UNI_TCK_NEXTEST_RESULTS_DIR="$raw_results_dir" \
    cargo nextest run -p uni-tck --test tck --no-fail-fast "${filter_args[@]}" || true

    echo ""
    echo "📊 Aggregating results..."
    results_json=$(python3 scripts/aggregate_nextest_results.py \
        --results-dir "$raw_results_dir" \
        --output-dir "$output_dir")

    if [ -n "$filter" ]; then
        echo ""
        echo "ℹ️  Filtered run — results saved to $output_dir/"
        echo "ℹ️  Skipping report generation (only full runs update the report)"
        echo "ℹ️  Intended: filtered runs do not update compliance_reports or last_run snapshots"
        echo ""
        return
    fi

    echo ""
    echo "📊 Generating report..."
    echo ""

    # Keep checked-in compliance reports by mode.
    sync_compliance_reports "$mode" "$results_json"

    echo ""
    echo "📁 Report available at: compliance_reports/$(compliance_mode_dir "$mode")/report.md"
    echo "📁 Last run snapshot: compliance_reports/$(compliance_mode_dir "$mode")/last_run_report.md"
    echo ""
}

RUN_BOTH=0
FILTER_ARG=""

if [ "${1:-}" = "--both" ]; then
    RUN_BOTH=1
    FILTER_ARG="${2:-}"
else
    FILTER_ARG="${1:-}"
fi

if [ "$RUN_BOTH" -eq 1 ]; then
    run_for_mode "schemaless" "$FILTER_ARG"
    run_for_mode "sidecar" "$FILTER_ARG"
else
    MODE=$(normalize_mode "${UNI_TCK_SCHEMA_MODE:-schemaless}") || exit 1
    run_for_mode "$MODE" "$FILTER_ARG"
fi
