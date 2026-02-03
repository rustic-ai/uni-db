# DataFusion Read-Query Fallbacks to Legacy Executor

**Test run**: 1060 passed, 12 skipped. **132 total fallbacks** from read queries.

DDL/admin operations are routed directly to the legacy executor via `is_ddl_or_admin()` (with recursive wrapper-node detection). Write/mutation operations (`CREATE`, `MERGE`, `DELETE`, `SET`, `REMOVE`, `FOREACH`) are routed via `contains_write_operations()` (also recursive). Neither produces fallback noise.

---

## Previously Fixed

### ~~Arrow Type Mismatches — typed schema columns reported as Utf8 (~25 tests)~~ **FIXED**

**Status**: Resolved. `build_property_column_static()` in `scan.rs` now handles all Arrow types produced by `DataType::to_arrow()`: Binary (CRDT), FixedSizeList (Vector), Timestamp, Date32, Time64, Duration, List (Int64/Utf8/Float64/Boolean/Struct), and Struct (Point types).

**23 of 25 tests now execute via DataFusion** without fallback. The 2 remaining failures (`test_map_schema_property`, `test_collection_types_storage_and_query`) are **not fallback issues** — they execute via DataFusion but fail at result deserialization: `List(Struct(key,value))` Arrow arrays are not reconstructed back into JSON objects by the result conversion layer.

### ~~Write/DDL Operations Reaching DataFusion~~ **FIXED**

**Status**: Resolved with two functions in `read.rs`:
- `is_ddl_or_admin()` — catches DDL, index, admin, and utility operations (recurses through `Project`, `Sort`, `Limit`, `Distinct`, `Aggregate`, `Window`, `Unwind` wrappers)
- `contains_write_operations()` — catches `Create`, `Merge`, `Delete`, `Set`, `Remove`, `Foreach` (same recursive wrapper detection)

Previously, a query like `CREATE (n:Person) RETURN n` produced a plan `Project { input: Create { ... } }` — the top-level `Project` wasn't caught, so it went to DataFusion, which failed on the nested `Create`. Eliminated **148+ write fallbacks** from notebook execution and **21 DDL/index fallbacks** from the test suite.

---

## Category 1: Schema Field Resolution (38 fallbacks)

Bare variable names (`Expr::Variable("n")`) translate to `Column("n")` but the DataFusion schema only has qualified names like `n._vid`, `n.name`. Also affects path variables and edge variables after traversals.

| Count | Error Pattern | Subcategory |
|-------|--------------|-------------|
| 6 | No field named `p` | Path/node variable |
| 4 | No field named `b.id` | Qualified property name |
| 3 | No field named `n` (valid: `n._vid`) | Bare node variable |
| 2 | No field named `x` (valid: `p._vid`, `x._vid`) | Multi-hop variable |
| 2 | No field named `x` (valid: `p._vid`, `_target_vid`, `_hop_count`) | Multi-hop variable |
| 2 | No field named `r` (valid: `u1._vid`, ..., `r._eid`) | Edge variable |
| 2 | No field named `e._vid` | Wrong column qualifier |
| 2 | No field named `b.name` | Qualified property name |
| 1 | No field named `r._vid` | Edge accessed as node |
| 1 | No field named `r` (4 variants) | Edge variable |
| 1 | No field named `p` (4 path variants) | Path variable |
| 1 | No field named `part.cost` | Multi-hop property |
| 1 | No field named `n.name` (valid: `collect(n.name)`) | Post-aggregation alias |
| 1 | No field named `i.id` | Qualified property name |
| 1 | No field named `e._vid` (did you mean `e._eid`?) | Edge accessed as node |
| 1 | No field named `d.content` (valid: `d._vid`, `d`, `d._score`) | Vector search result |
| 1 | Cannot extract variable from `Parameter("session")` | Session parameter |
| 1 | Cannot extract variable from `ArrayIndex` (path indexing) | Path element access |

**Root cause**: The expression-to-DataFusion translation in `df_expr.rs` converts `Expr::Variable("n")` to `DfExpr::Column("n")`, but scan/traverse operators expose flattened columns (`n._vid`, `n.name`), not a struct column `n`.

---

## Category 2: Unsupported Expressions (52 fallbacks)

Expressions that require features DataFusion doesn't support (lambda functions, subqueries, etc.).

| Count | Expression Type | Notes |
|-------|----------------|-------|
| 21 | Quantifier expressions (ALL/ANY/SINGLE/NONE) | Requires DataFusion lambda functions (Issue #14205) |
| 10 | Map projections | `n { .name, .age }` syntax not pushed down |
| 8 | List comprehensions | Requires lambda functions |
| 6 | Reduce expressions | Requires lambda functions |
| 3 | CALL subqueries | `CALL { ... }` not supported |
| 2 | EXISTS subqueries | `EXISTS { MATCH ... }` not supported |
| 1 | Recursive CTEs | Not supported |
| 1 | Map literals | `{key: value}` not translated |

---

## Category 3: Function/Type Coercion Gaps (35 fallbacks)

Type mismatches and unsupported function signatures in the DataFusion execution engine.

| Count | Error | Notes |
|-------|-------|-------|
| 10 | Filter predicate must return BOOLEAN, got Null | Null-valued WHERE predicates |
| 5 | Invalid comparison: `Utf8 <= Timestamp` | String-to-timestamp coercion missing |
| 5 | Invalid comparison: `Boolean == Int64` | Boolean-to-integer coercion missing |
| 3 | UInt64 vs Int64 interval mismatch | DataFusion planning bug on LIMIT/SKIP |
| 3 | `array_slice` coercion failure | List(Int64), Int64, UInt64 signature mismatch |
| 2 | CHARACTER_LENGTH on `List(Utf8)` | size() should map to array_length |
| 2 | CHARACTER_LENGTH on `List(Int64)` | size() should map to array_length |
| 1 | CHARACTER_LENGTH on `List(Struct)` | size() should map to array_length |
| 1 | Unsupported data type Int64 for `signum` | DataFusion only supports Float |
| 1 | `array_length` on non-list type | Type mismatch after overflow column |
| 1 | Column types must match schema: expected Utf8, found UInt64 | Schema type propagation bug |
| 1 | Arguments need same data type | Heterogeneous argument coercion |

---

## Category 4: Unregistered UDFs (5 fallbacks)

| Count | UDF | Notes |
|-------|-----|-------|
| 3 | `ProcedureCall` | Procedure calls nested inside wrappers (e.g. `CALL proc() YIELD x RETURN x`) |
| 2 | `labels()` | Not registered in `df_udfs.rs`; only `id`, `type`, `keys`, `range`, bitwise ops exist |

**Notebook-only**: `properties()` UDF (6 fallbacks from pydantic notebooks) — not triggered by the test suite but observed during mkdocs notebook execution.

---

## Category 5: Overflow / Schemaless Properties (2 fallbacks)

| Count | Error | Details |
|-------|-------|---------|
| 1 | `LargeBinary == Utf8` | Overflow properties stored as LargeBinary (JSONB), comparison fails |
| 1 | `array_element` on LargeBinary | Array indexing on overflow column type |

---

## Summary

| # | Category | Fallbacks | Priority |
|---|----------|-----------|----------|
| 1 | **Unsupported expressions** (quantifiers, map projections, list comprehensions, reduce, subqueries) | 52 | Low — requires DataFusion lambda support |
| 2 | **Schema field resolution** (bare variable names, path variables, edge variables) | 38 | High — most impactful single fix |
| 3 | **Function/type coercion** (null predicates, type mismatches, array coercion) | 35 | Medium — incremental fixes |
| 4 | **Unregistered UDFs** (`labels`, `properties`, procedure calls) | 5 (+6 notebooks) | Medium — straightforward to add |
| 5 | **Overflow LargeBinary** | 2 | Low |
| 6 | **Map result deserialization** (not a fallback — DF succeeds, result conversion fails) | 2 | Low |
| | **Total** | **132** (+2 deser) | |

### Notebook-Specific Fallback Summary

After the write/DDL fix, **16 fallbacks remain** across notebook execution:

| Error | Count | Notebooks | Category |
|-------|-------|-----------|----------|
| `properties()` UDF not registered | 6 | All 5 pydantic notebooks | #4 |
| Schema field `p.name` not found | 4 | pydantic/recommendation, python/recommendation | #2 |
| UInt64/Int64 interval mismatch | 2 | pydantic/rag, python/rag | #3 |
| Schema field `part.cost` not found | 2 | pydantic/supply_chain, python/supply_chain | #2 |
| Schema field `c.text` not found | 1 | pydantic/rag | #2 |
| Schema field `part.sku` not found | 1 | pydantic/supply_chain | #2 |
