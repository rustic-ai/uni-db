# Implementation Plan - TCK Core Compliance

## Phase 1: String Functions
- [ ] Task: Implement `STARTS WITH`, `ENDS WITH`, `CONTAINS` in `expr_compiler.rs`.
    - [ ] Map `BinaryOp` to DataFusion `Like` or custom UDFs.
- [ ] Task: Implement `substring`, `toLower`, `toUpper` UDFs.
    - [ ] Register in `df_udfs.rs`.
    - [ ] Add support in `expr_compiler.rs` (should be automatic if UDF).

## Phase 2: Ordering Logic
- [ ] Task: Implement Cypher-compliant Sort Comparator.
    - [ ] DataFusion's default sort is SQL-style. Cypher has specific type ordering (`Map > Node > ... > Null`).
    - [ ] Create `CypherSortExpr` or wrap values to enforce order?
    - [ ] Or rely on `Arrow` custom comparator if possible.
    - [ ] Fix `NULL` handling (Nulls are generally largest in Cypher, check TCK).
- [ ] Task: Fix `ORDER BY` expression evaluation in `df_planner.rs`.
    - [ ] Ensure expressions in `ORDER BY` are properly compiled and aliased.

## Phase 3: Temporal Foundations
- [ ] Task: Fix Temporal Formatting.
    - [ ] Update `arrow_convert.rs` to strip seconds if zero? Or format according to TCK expectation.
    - [ ] Fix Timezone display (`Z` vs `+00:00`).
- [ ] Task: Fix Temporal Parsing.
    - [ ] Improve `parse_datetime` logic.

## Phase 4: Verification
- [ ] Task: Conductor - User Manual Verification 'Core Compliance' (Protocol in workflow.md)
