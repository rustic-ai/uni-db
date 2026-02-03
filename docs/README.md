# Uni Cypher Parser Documentation

## TCK Test Results

### Current (v2)
- **[tck-parser-test-results-v2.md](tck-parser-test-results-v2.md)** - ⭐ **START HERE**
  - **92.5% pass rate** (3,856 queries)
  - Complete test results with properly expanded Cucumber Scenario Outlines
  - 14 categories at 100%, 25 categories above 90%

- **[tck-extraction-summary.md](tck-extraction-summary.md)** - How we got here
  - Explains the Cucumber placeholder substitution process
  - Before/after comparison
  - 60% increase in test coverage

### Deprecated (v1)
- **[tck-parser-test-results.md](tck-parser-test-results.md)** - ⚠️ DEPRECATED
  - 81.8% pass rate (2,410 templated queries)
  - Incorrectly included Cucumber template syntax as test cases
  - Kept for historical reference

### Analysis
- **[tck-failure-categorization.md](tck-failure-categorization.md)** - ⭐ **Grammar vs Walker**
  - Complete categorization of all 288 failures
  - Identifies which are grammar issues vs walker issues
  - ~230 grammar, ~15 walker, ~43 mixed/unknown
  - Detailed fix recommendations for each category

- **[tck-fix-priority.md](tck-fix-priority.md)** - ⭐ **Quick Reference**
  - Top 10 fixes ranked by impact
  - Time estimates and projected pass rates
  - Week-by-week implementation plan
  - **96% achievable in ~8 hours, 99% in 2-3 weeks**

- **[tck-grammar-gaps.md](tck-grammar-gaps.md)** - ⚠️ PARTIALLY OUTDATED
  - Detailed grammar gap analysis from v1
  - Still valid but impact numbers are lower after v2
  - 288 real failures vs 439 assumed

- **[tck-failure-breakdown.md](tck-failure-breakdown.md)** - ⚠️ OUTDATED
  - Categorized failure analysis from v1
  - Many "failures" were actually template placeholders
  - Kept for historical reference

## Quick Stats

| Version | Queries | Passed | Failed | Pass Rate |
|---------|---------|--------|--------|-----------|
| v1 (Templates) | 2,410 | 1,971 | 439 | 81.8% |
| v2 (Expanded) | 3,856 | 3,568 | 288 | **92.5%** |

## Key Improvements (v1 → v2)

| Category | v1 | v2 | Queries |
|----------|-----|-----|---------|
| expressions/quantifier | 35.2% | **100%** | 604 |
| expressions/temporal | 30.1% | **100%** | 1,004 |
| expressions/typeConversion | 89.2% | **100%** | 47 |
| expressions/map | 73.7% | **100%** | 44 |

## Running Tests

```bash
# Generate fresh TCK queries from Cucumber features
cd crates/uni-cypher
cargo run --example tck_extractor

# Run all TCK tests
cargo test --package uni-cypher --test tck_test_suite

# Get statistics
cargo test --package uni-cypher --test tck_test_suite test_tck_statistics -- --nocapture
```

## Document Guide

1. **New to TCK results?** → Read [tck-parser-test-results-v2.md](tck-parser-test-results-v2.md)
2. **Want to understand what changed?** → Read [tck-extraction-summary.md](tck-extraction-summary.md)
3. **Need to fix grammar gaps?** → Reference [tck-grammar-gaps.md](tck-grammar-gaps.md) (but note lower impact)
4. **Historical context?** → See v1 docs (deprecated but kept for reference)


