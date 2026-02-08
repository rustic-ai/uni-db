# Implementation Plan - TCK WHERE Compatibility

## Phase 1: Basic Comparisons & Ranges (Category A)
- [x] Identification: Run TCK comparison tests and catalog specific failures. 147f973
- [x] Implementation: Update `df_planner.rs` and `expr_eval.rs` to support all comparison operators.
- [x] Edge Cases: Handle heterogeneous type comparisons (e.g., String vs Int) as per Cypher spec.
- [x] Verification: 100% pass rate for `expressions/comparison` and relevant `match-where` scenarios. f752ebc

## Phase 2: Boolean Logic & Precedence (Category B)
- [ ] Identification: Run TCK boolean logic tests.
- [ ] Implementation: Fix `XOR` implementation and verify `AND`/`OR` precedence in `planner.rs`.
- [ ] Refactoring: Ensure `NOT` correctly negates complex predicates.
- [ ] Verification: 100% pass rate for `expressions/boolean` and `expressions/precedence`.

## Phase 3: Null Handling (Category C)
- [ ] Identification: Run TCK null handling tests.
- [ ] Implementation: Improve `IS NULL` / `IS NOT NULL` support in both legacy and vectorized executors.
- [ ] Logic Fix: Ensure 3-valued logic (True, False, Unknown) is correctly handled in `WHERE` filters.
- [ ] Verification: 100% pass rate for `expressions/null`.

## Phase 4: String Matching (Category D)
- [ ] Identification: Run TCK string matching tests.
- [ ] Implementation: Implement or fix `STARTS WITH`, `ENDS WITH`, and `CONTAINS` in `df_planner.rs`.
- [ ] Edge Cases: Correctly handle `null` inputs and non-string inputs in string operators.
- [ ] Verification: 100% pass rate for `expressions/string`.

## Phase 5: Final Integration & Cleanup
- [ ] Run full TCK suite to ensure no regressions in other areas.
- [ ] Clean up any temporary debug logs or workarounds.
- [ ] Update `COMPATIBILITY_REPORT.md` in `crates/uni-tck`.
