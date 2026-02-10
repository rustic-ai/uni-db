# Current Test Failures Analysis

**Date:** 2026-02-10
**Branch:** `debug/tck001`
**Results:** 1228 tests run: 1100 passed, 128 failed, 19 skipped (89.6% pass rate)

All 128 failures are in the `uni-db` crate (integration/e2e tests). Unit tests and all other crates pass clean.

---

## 1. Schema Field Resolution — Schemaless/Overflow Properties (~35 failures)

**Error pattern:**
```
No field named "p.name". Valid fields are "p._vid", "p._labels", "p.*", p.
```

When a label has no declared schema properties (schemaless mode), the DataFusion scan only produces `_vid`, `_labels`, and `*` (the JSONB overflow column). Property access like `p.name` isn't being resolved through the `*`/overflow_json column — DataFusion doesn't know how to extract individual fields from the LargeBinary JSONB blob.

**Affected tests:**
- `schemaless_labels_test::*`
- `test_overflow_fix::*`
- `overflow_json_e2e::*`
- `where_debug_test::*` (schemaless variants)
- `notebook_examples::*`
- `use_case_*`
- Several `e2e_comprehensive_test` tests

---

## 2. Edge/Relationship Property Resolution (~15 failures)

**Error pattern:**
```
No field named "r.since"
No field named "r.name"
No field named "r._vid" (should be r._eid)
```

Edge properties are not being registered in the DataFusion schema. After traversal, edge variables only expose `_eid` but not their typed properties. Also, some tests reference `r._vid` on edges when the correct field is `r._eid`.

**Affected tests:**
- `where_debug_test::test_where_edge_property_predicate`
- `pushdown_hydration_e2e::*`
- `e2e_comprehensive_test::path_tests::*`
- `cypher_shortest_path::*`

---

## 3. Not-Yet-Implemented Features in DataFusion Engine (~30 failures)

Several Cypher features explicitly return "not yet supported" errors:

| Feature | Error | Count |
|---------|-------|-------|
| Quantifiers (ALL/ANY/SINGLE/NONE) | `Quantifier expressions not supported` | ~12 |
| Map projections | `Map projection cannot be pushed down to DataFusion` | ~10 |
| CALL subqueries | `CALL subqueries not yet supported` | ~3 |
| Procedure calls (vector search, etc.) | `Procedure calls not yet supported` | ~3 |
| EXISTS subqueries | `EXISTS subqueries not yet supported` | 1 |
| Recursive CTEs | `Recursive CTEs not yet supported` | 1 |

**Affected tests:**
- `quantifier_e2e_test::*`
- `map_projection_test::*`
- `cypher_call::*`
- `hybrid_query::*`
- `cypher_exists::*`
- `recursive_cte_execution_test::*`
- `session_test::*`

---

## 4. UDF Scalar Type Handling (~15 failures)

**Error pattern:**
```
Unsupported scalar type for UDF: Date32("2024-01-15")
Unsupported scalar type for UDF: UInt64(0)
Unsupported scalar type for UDF: TimestampMicrosecond(...)
Unsupported scalar type for UDF: Time64Microsecond(...)
Unsupported scalar type for UDF: DurationMicrosecond(...)
```

Custom UDFs (likely `to_cypher_value` or similar) don't handle temporal types, unsigned integers, or duration types — only basic String/Int64/Float64/Boolean.

**Affected tests:**
- `e2e_comprehensive_test::data_type_tests::*` (date, datetime, time, timestamp, duration)
- `inverted_index_test::*`
- `norn_gap_coverage::*`

---

## 5. Type Coercion / Cross-Type Operations (~10 failures)

Multiple related errors:

| Error | Cause |
|-------|-------|
| `Invalid comparison operation: Int64 == Utf8` | Cypher allows `1 = '1'` but DF doesn't auto-coerce |
| `Cannot coerce arithmetic expression Utf8 + Utf8` | String concatenation via `+` not handled |
| `Invalid comparison operation: Utf8 < LargeBinary` | Comparing strings against overflow JSONB |
| `Invalid comparison operation: Timestamp <= Utf8` | Temporal comparisons |
| `arguments need to have the same data type` | Mixed-type CASE/COALESCE expressions |
| `CHARACTER_LENGTH function can only accept strings, got List(...)` | `size()` mapped to wrong DF function for lists |
| `tointeger UDF is not registered` | Missing Cypher function registration |
| `array_length only accepts List` | Type mismatch in list operations |

**Affected tests:**
- `comparison_test::*`
- `cypher_reduce::*`
- `null_handling_test::*`
- `type_conversion_test::*`
- `dynamic_access_test::*`
- `valid_at_test::*`

---

## 6. Arrow Schema Mismatch (~5 failures)

**Error pattern:**
```
column types must match schema types, expected LargeBinary but found UInt64 at column index 3
number of columns(5) must match number of fields(6) in schema
```

The schema declared for scan results doesn't match the actual Arrow record batches being produced. Likely a mismatch between the columnar-first scan path and what the storage layer actually returns.

**Affected tests:**
- `query_integration::*`
- `collection_types_test::*`
- `normalization_test::*`

---

## 7. Behavioral/Logic Bugs (~5 failures)

Assertion failures where queries return wrong results (not errors):

| Bug | Detail |
|-----|--------|
| DELETE not working | `test_delete_node` / `test_detach_delete` — nodes still found after deletion (count 1 instead of 0) |
| Reader isolation | `test_reader_isolation_lifecycle` — deleted nodes still visible through reader |
| Traversal filtering | `test_traversal_label_filtering_bug` — variable-length traversal ignoring label filters |

**Affected tests:**
- `e2e_comprehensive_test::clause_tests::test_delete_node`
- `e2e_comprehensive_test::clause_tests::test_detach_delete`
- `reader_isolation::*`
- `bug_traversal_filtering::*`

---

## Priority Assessment

| Priority | Category | Impact | Root Cause |
|----------|----------|--------|------------|
| **P0** | Schema field resolution (#1) | ~35 tests | Schemaless property extraction from JSONB not wired into DF |
| **P0** | Edge property resolution (#2) | ~15 tests | Edge props not registered in DF schema |
| **P1** | Arrow schema mismatch (#6) | ~5 tests | Columnar scan path schema vs actual data |
| **P1** | Behavioral bugs (#7) | ~5 tests | DELETE/isolation logic errors |
| **P1** | UDF type handling (#4) | ~15 tests | UDFs need temporal/uint type support |
| **P2** | Type coercion (#5) | ~10 tests | Cypher-to-DF type coercion layer |
| **P2** | Unimplemented features (#3) | ~30 tests | Incremental feature work |

Categories #1 and #2 together account for ~50 of the 128 failures and likely share a common root cause in the DataFusion planning/scan layer — specifically how the columnar-first scan path registers fields for the query plan.
