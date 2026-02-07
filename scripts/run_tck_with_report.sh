#!/bin/bash
# Run TCK tests and generate markdown reports

set -e

cd "$(dirname "$0")/.."

echo "🚀 Running TCK tests..."
echo ""

# Run the tests
cargo test --package uni-tck --test cucumber

# Check if JSON was generated
if [ ! -f "target/cucumber/results.json" ]; then
    echo "❌ Error: JSON results not found at target/cucumber/results.json"
    exit 1
fi

echo ""
echo "📊 Generating markdown reports..."
echo ""

# Generate reports
python3 scripts/analyze_tck_json.py target/cucumber/results.json

echo ""
echo "📁 Reports available at:"
echo "  - target/cucumber/report.md (all features)"
echo "  - target/cucumber/match-report.md (if Match features were run)"
echo "  - target/cucumber/where-report.md (if Where features were run)"
echo "  - target/cucumber/return-report.md (if Return features were run)"
echo ""
