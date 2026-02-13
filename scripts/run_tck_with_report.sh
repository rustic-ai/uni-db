#!/bin/bash
# Run TCK tests via nextest (parallel, filterable) and generate markdown reports
#
# Usage:
#   scripts/run_tck_with_report.sh              # Run all scenarios
#   scripts/run_tck_with_report.sh "~Match1"    # Filter by pattern
#   UNI_TCK_SCHEMA_MODE=sidecar scripts/run_tck_with_report.sh
#   scripts/run_tck_with_report.sh --both       # Run schemaless + sidecar
#   scripts/run_tck_with_report.sh --both "~Match1"

set -e

cd "$(dirname "$0")/.."

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
    local filter_expr=""

    if [ -n "$filter" ]; then
        filter_expr="-E test($filter)"
        output_dir="$output_dir/filtered"
        echo "🚀 Running TCK tests in '$mode' mode (filter: $filter)..."
    else
        echo "🚀 Running TCK tests in '$mode' mode..."
    fi

    # Clean previous per-scenario results only for this mode.
    rm -rf "$raw_results_dir"

    echo ""
    # Run tests via nextest (--no-fail-fast to collect all results)
    # shellcheck disable=SC2086
    UNI_TCK_SCHEMA_MODE="$mode" \
    UNI_TCK_NEXTEST_RESULTS_DIR="$raw_results_dir" \
    cargo nextest run -p uni-tck --test tck --no-fail-fast $filter_expr || true

    echo ""
    echo "📊 Aggregating results..."
    RESULTS_JSON=$(python3 scripts/aggregate_nextest_results.py \
        --results-dir "$raw_results_dir" \
        --output-dir "$output_dir")

    if [ -n "$filter" ]; then
        echo ""
        echo "ℹ️  Filtered run — results saved to $output_dir/"
        echo "ℹ️  Skipping report generation (only full runs update the report)"
        echo ""
        return
    fi

    echo ""
    echo "📊 Generating report..."
    echo ""

    # Generate comparative report (auto-finds previous results)
    python3 scripts/analyze_tck_json.py "$RESULTS_JSON"

    echo ""
    echo "📁 Report available at: $output_dir/report.md"
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
    MODE=$(normalize_mode "${UNI_TCK_SCHEMA_MODE:-schemaless}")
    run_for_mode "$MODE" "$FILTER_ARG"
fi
