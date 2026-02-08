# Implementation Plan - TCK Core Compliance

## Phase 1: String Functions
- [x] Task: Implement `STARTS WITH`, `ENDS WITH`, `CONTAINS` in `expr_compiler.rs`.
    - [x] Map `BinaryOp` to DataFusion `Like` or custom UDFs.
- [x] Task: Implement `substring`, `toLower`, `toUpper` UDFs.
    - [x] Register in `df_udfs.rs` (or map to built-ins in `df_expr.rs`).
    - [x] Add support in `expr_compiler.rs` (should be automatic if UDF/mapped).

## Phase 2: Ordering Logic
- [x] Task: Implement Cypher-compliant Sort Comparator.
    - [x] DataFusion's default sort is SQL-style. Cypher has specific type ordering (`Map > Node > ... > Null`).
    - [x] Fix `NULL` handling (Nulls are generally largest in Cypher, check TCK).
- [x] Task: Fix `ORDER BY` expression evaluation in `df_planner.rs`.
    - [x] Ensure expressions in `ORDER BY` are properly compiled and aliased.

## Phase 3: Temporal Foundations
- [x] Task: Fix Temporal Formatting.
    - [x] Update `arrow_convert.rs` to strip seconds if zero? Or format according to TCK expectation.
    - [x] Fix Timezone display (`Z` vs `+00:00`).
- [ ] Task: Fix Temporal Parsing.
    - [ ] Improve `parse_datetime` logic. (Deferred to future optimization)

## Phase 4: Verification
- [x] Task: Conductor - User Manual Verification 'Core Compliance' (Protocol in workflow.md)