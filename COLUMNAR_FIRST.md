# Columnar-First DataFusion Scan Path

## Problem Statement

The DataFusion execution path in uni-query performs a **columnar → row → columnar** round-trip that defeats the purpose of using a columnar execution engine.

### Current Flow

```
Lance Table (Arrow RecordBatch — columnar)
  │
  ├── Phase 1: scan_vertex_vids_static()
  │     SELECT _vid only → Vec<Vid>           ← throws away everything else
  │
  ├── Phase 2: materialize_vertex_batch_static()
  │     PropertyManager re-queries Lance with _vid IN (...)
  │     → RecordBatch from Lance (columnar)
  │     → row-by-row decode into HashMap<Vid, Properties>    ← columnar → row
  │     → re-encode into new RecordBatch                     ← row → columnar
  │
  └── DataFusion consumes RecordBatch
```

There are **two stacked wastes**:

1. **Two Lance queries for the same data.** Phase 1 queries Lance for just `_vid`, then Phase 2 queries Lance *again* with those same VIDs to get properties. One query with a wider projection would suffice.

2. **Columnar → row → columnar conversion.** Phase 2 decodes the Lance `RecordBatch` row-by-row into `HashMap<Vid, Properties>`, then rebuilds a `RecordBatch` column-by-column. The `RecordBatch` from Lance already *is* columnar with the correct Arrow types.

### The `overflow_json` Reconstruction Problem

A concrete example of this waste is the `overflow_json` column:

- **Write path** (`vertex.rs`): Non-schema properties are serialized as JSONB and stored in `overflow_json` (LargeBinary column) in the per-label Lance table.

- **Read path** (`property_manager.rs`): Lance returns `overflow_json` as a `LargeBinaryArray`. The PropertyManager decodes it row-by-row:
  ```
  JSONB bytes → jsonb::RawJsonb::to_string() → serde_json::from_str() → HashMap<String, Value>
  ```
  These decoded properties are flattened into the `Properties` map alongside schema properties.

- **Rebuild path** (`scan.rs:build_overflow_json_column`): The scan needs `overflow_json` as a `LargeBinaryArray` for DataFusion filter evaluation (`json_get_string(overflow_json, 'key')`). Since the raw bytes were lost, it reconstructs the blob:
  ```
  HashMap<String, Value> → serde_json::Value::Object → jsonb::to_owned_jsonb() → JSONB bytes
  ```

The full round-trip is:
```
JSONB bytes → decode → HashMap → re-encode → JSONB bytes
```

Worse, the reconstruction doesn't even know which properties were originally in `overflow_json` vs. schema columns, so it re-encodes **all** non-system properties — producing a subtly different blob than what Lance stored.

## Root Cause

The `PropertyManager` is designed for the **row-based executor** (`executor/read.rs`) where `HashMap<Vid, Properties>` is the natural format. The DataFusion scan path was bolted onto this row-oriented API, forcing a columnar→row→columnar detour.

## Proposed Design: Columnar-First Scan

### Principle

1. **Do all columnar work first** — single Lance query, Arrow compute for filtering/dedup/version resolution, L0 as a small RecordBatch concatenated in, `overflow_json` passes through untouched.
2. **Row-level processing only at the end** — if CRDT columns exist, handle just those columns row-by-row after everything else is already resolved.

### Target Flow

```
Lance Table
  │
  ├── Single query with wide projection
  │   SELECT _vid, _deleted, _version, prop1, prop2, ..., overflow_json
  │   WHERE _version <= {hwm}
  │
  ├── Arrow compute: filter(_deleted = false)
  │
  ├── Arrow compute: sort by (_vid, _version DESC)
  │   then dedup-first per _vid (pick latest version)
  │
  ├── Build small RecordBatch from L0 buffer
  │   concat_batches with Lance result
  │   re-run dedup (L0 versions are higher, so they win)
  │
  ├── overflow_json column flows through untouched as LargeBinaryArray
  │
  ├── [Only if CRDT columns exist] Row-level merge on just those columns
  │
  └── Hand RecordBatch to DataFusion
```

### Step-by-Step

#### Step 1: Single Lance Query with Wide Projection

Instead of:
```rust
// Phase 1: VIDs only
table.query().select(Select::columns(&["_vid"]))
// Phase 2: Re-query with properties
table.query().only_if("_vid IN (...)").select(Select::Columns(all_columns))
```

Do:
```rust
// One query, all needed columns
let mut cols = vec!["_vid", "_deleted", "_version"];
cols.extend(schema_property_names);
cols.push("overflow_json");

table.query()
    .only_if("_version <= {hwm}")  // MVCC filter pushed to Lance
    .select(Select::columns(&cols))
    .execute()
```

This eliminates the second Lance query entirely.

#### Step 2: Columnar Deletion Filtering

Currently `_deleted` is checked row-by-row in PropertyManager. Instead, use Arrow compute:

```rust
let deleted_col = batch.column_by_name("_deleted").downcast_ref::<BooleanArray>();
let not_deleted = arrow::compute::not(deleted_col);
let filtered = arrow::compute::filter_record_batch(&batch, &not_deleted)?;
```

Or even better, push `_deleted = false` into the Lance `only_if`:
```rust
table.query()
    .only_if("_deleted = false AND _version <= {hwm}")
```

#### Step 3: Columnar MVCC Resolution

Multiple rows may exist for the same VID at different versions. Pick the latest:

```rust
// Sort by (_vid ASC, _version DESC)
let indices = arrow::compute::lexsort_to_indices(&[
    SortColumn { values: vid_col, options: Some(SortOptions { descending: false, .. }) },
    SortColumn { values: ver_col, options: Some(SortOptions { descending: true, .. }) },
]);
let sorted = arrow::compute::take_record_batch(&batch, &indices)?;

// Dedup: keep first occurrence of each _vid (which is the highest version)
let vid_arr = sorted.column_by_name("_vid").downcast_ref::<UInt64Array>();
let mut keep = BooleanBuilder::new();
let mut prev_vid = None;
for i in 0..vid_arr.len() {
    let vid = vid_arr.value(i);
    keep.append_value(prev_vid != Some(vid));
    prev_vid = Some(vid);
}
let deduped = arrow::compute::filter_record_batch(&sorted, &keep.finish())?;
```

Every column — including `overflow_json` — is carried along by the sort/filter operations without any per-cell decoding.

#### Step 4: L0 Overlay via RecordBatch Concatenation

Build a small RecordBatch from L0's `HashMap<Vid, Properties>`:

```rust
// For each VID in L0 that has the target label:
//   - Build typed Arrow columns from L0 properties
//   - Assign _version = L0 version (higher than any Lance version)
//   - Set _deleted = false (tombstoned VIDs excluded)

let l0_batch = build_l0_record_batch(l0_buffers, label, &arrow_schema)?;
let combined = arrow::compute::concat_batches(&arrow_schema, &[deduped, l0_batch])?;

// Re-run the same sort+dedup — L0 rows have higher versions, so they win
let final_batch = mvcc_dedup(&combined)?;
```

L0 is typically small (in-memory buffer), so building a RecordBatch from it is cheap. The key insight: by giving L0 rows higher `_version` values and re-running the same dedup, the merge logic is identical for both paths.

For L0 tombstones, either:
- Insert rows with `_deleted = true` into the L0 batch and let Step 2's filter handle them, or
- Filter out tombstoned VIDs after the dedup.

#### Step 5: Columnar `overflow_json` Passthrough

The `overflow_json` column is a `LargeBinaryArray` in Lance. After Steps 2-4, it's still a `LargeBinaryArray` — just with fewer rows. No decoding, no re-encoding. DataFusion's `json_get_string(overflow_json, 'key')` UDF operates directly on the JSONB bytes.

#### Step 6: Row-Level CRDT Merge (Only When Needed)

CRDT properties require seeing multiple versions of the same VID to merge them. This is the only case that genuinely needs row-level processing. If the scan touches CRDT columns:

1. **Before** the dedup in Step 3, extract just the CRDT columns and run per-VID merge.
2. **After** dedup, replace the CRDT column values with the merged results.

If no CRDT columns are in the projection (the common case), this step is skipped entirely. The planner already knows the schema — it can flag at plan time whether CRDT handling is needed.

### Schema Mapping

The Lance per-label table schema and the DataFusion scan output schema are almost identical:

| Lance Column | Arrow Type | DataFusion Column | Transform |
|---|---|---|---|
| `_vid` | UInt64 | `n._vid` | Rename only |
| `_deleted` | Boolean | (dropped) | Filtered out in Step 2 |
| `_version` | UInt64 | (dropped) | Used for MVCC, then dropped |
| `name` | Utf8 | `n.name` | Rename only |
| `age` | Int64 | `n.age` | Rename only |
| `overflow_json` | LargeBinary | `n.overflow_json` | Passthrough |

The transform is mostly column renaming (`name` → `n.name`) and dropping metadata columns (`_deleted`, `_version`). Arrow supports zero-copy column renaming.

The `_labels` column needs to be added (not in per-label Lance table). This can be built from the label name (constant for per-label scans) or fetched from the main vertex table / L0 label maps.

## What This Eliminates

| Waste | Current | Columnar-First |
|---|---|---|
| Lance queries per scan | 2 (VIDs, then properties) | 1 |
| `overflow_json` decode+re-encode | Every row | Zero |
| `HashMap<Vid, Properties>` allocation | Every scan | Only for L0 (small) |
| Per-row type conversion | Every property × every row | Zero (Arrow types preserved) |
| `serde_json` round-trips | 2 per overflow row (decode + encode) | Zero |
| `jsonb` round-trips | 1 decode + 1 encode per overflow row | Zero |

## Files Involved

| File | Current Role | Change |
|---|---|---|
| `crates/uni-query/src/query/df_graph/scan.rs` | VID scanning + RecordBatch rebuild | New columnar scan path |
| `crates/uni-store/src/runtime/property_manager.rs` | Row-by-row property fetching | Bypassed for DataFusion path |
| `crates/uni-query/src/query/df_planner.rs` | Builds GraphScanExec with property list | Pass schema info for columnar path |
| `crates/uni-store/src/storage/vertex.rs` | VertexDataset Lance table access | Reuse `open_lancedb` directly |
| `crates/uni-store/src/runtime/l0.rs` | L0Buffer definition | Add method to build RecordBatch from L0 data |
| `crates/uni-query/src/query/df_expr.rs` | Overflow property rewriting | No change (UDF rewriting still works) |

## Fallback Audit: What the Legacy Executor Was Hiding

### Background

The `execute()` method in `executor/read.rs` had a silent fallback: if `execute_datafusion()` returned an error, it caught it and re-ran the query through `execute_subplan()` (the row-based legacy executor). Only timeout and OOM errors were propagated. Everything else was swallowed.

```rust
// DISABLED — was at executor/read.rs:748 and :767
match self.execute_datafusion(plan.clone(), ...).await {
    Ok(batches) => self.record_batches_to_rows(batches),
    Err(e) => {
        log::debug!("DataFusion execution failed (falling back): {}", e);
        if e.to_string().contains("Query timed out")
            || e.to_string().contains("Query exceeded memory limit")
        { return Err(e); }
        self.execute_subplan(plan, ...).await  // silent fallback
    }
}
```

Both fallback sites were disabled (commented out) and replaced with direct error propagation (`?`). This covers all read query paths — the schema/schemaless distinction happens inside `execute_datafusion` → `HybridPhysicalPlanner` → `GraphScanExec`, not at the routing level.

### How the Fallback Mechanism Worked

The planner (`df_planner.rs`) used intentional `Err(anyhow!(...))` returns as a signaling mechanism to trigger fallback. For example:

```rust
// df_planner.rs:1428
return Err(anyhow!(
    "Bare variable '{}' requires fallback to materialize full node/edge object",
    var_name
));
```

These errors were designed to be caught by the outer `match` and routed to `execute_subplan`. With the fallback disabled, they propagate to the user as query errors.

### Test Results: 481 tests, 127 failures (354 passed)

Disabling the fallback surfaced three categories of issues:

#### Category 1: Intentionally Unimplemented Features (~28 failures)

Features deliberately punted to the legacy executor via error-based signaling:

| Count | Feature | Error Message |
|-------|---------|---------------|
| 16 | Bare variable projection (`RETURN n`) | `Bare variable 'n' requires fallback to materialize full node/edge object` |
| 10 | Map projection (`n { .name, .age }`) | `Map projection cannot be pushed down to DataFusion` |
| 3 | CALL subqueries | `CALL subqueries not yet supported in DataFusion engine` |
| 2 | Procedure calls | `Procedure calls not yet supported in DataFusion engine` |
| 1 | Recursive CTEs | `Recursive CTEs not yet supported in DataFusion engine` |

These need DataFusion-native implementations or explicit routing to the legacy executor (without the silent catch-all fallback).

#### Category 2: Real Bugs (~14 failures)

Bugs masked by the fallback that should have been caught:

| Count | Bug | Error Message |
|-------|-----|---------------|
| 7 | Overflow property type mismatch | `Arrow error: Invalid argument error: Invalid comparison operation: LargeBinary == Utf8` |
| 4 | Null predicate handling | `Filter predicate must return BOOLEAN values, got Null` |
| 2 | Schema nullability violation | `Column 'y._vid' is declared as non-nullable but contains null values` |
| 1 | Schema field resolution | `No field named "r._vid". Did you mean 'n._vid'?` |

#### Category 3: UDF/Type Coercion Gaps (~15 failures)

Missing type support in DataFusion UDFs and expression handling:

| Count | Gap | Error Message |
|-------|-----|---------------|
| 12 | Quantifier expressions (ALL/ANY/SINGLE/NONE) | `Quantifier expressions not supported - requires DataFusion lambda functions` |
| 3 | UInt64 in UDFs | `Unsupported scalar type for UDF: UInt64(0)` |
| 5 | Temporal types in UDFs | `Unsupported scalar type for UDF: TimestampMicrosecond(...)` / `Date32(...)` / `Time64(...)` / `Duration(...)` |
| 1 | String concat | `Cannot coerce arithmetic expression Utf8 + Utf8 to valid types` |
| 1 | Missing UDF registration | `UDF 'tointeger' is not registered` |
| 1 | size() on wrong type | `array_length can only accept List/LargeList/FixedSizeList` |
| 1 | CHARACTER_LENGTH on list | `CHARACTER_LENGTH function can only accept strings, but got List(...)` |

### Implications for Columnar-First

The fallback audit confirms:

1. **The legacy executor is still needed** for a defined set of features (bare variables, map projections, CALL subqueries, procedures, recursive CTEs). These should be routed explicitly, not via a catch-all error handler.

2. **Real bugs were hidden.** The `LargeBinary == Utf8` comparison errors (7 failures) are directly related to the `overflow_json` handling discussed in this document. The columnar-first approach eliminates this class of bug by keeping `overflow_json` as `LargeBinary` end-to-end.

3. **The catch-all fallback must stay disabled.** It masks bugs and makes it impossible to know whether DataFusion is actually executing a query or silently delegating to the legacy path. Explicit routing (check features at plan time, route to the correct executor) replaces implicit fallback (try DataFusion, catch errors, retry with legacy).

## Design Constraints

- **PropertyManager stays for row-based executor.** The row-based executor (`executor/read.rs`) still needs `HashMap<Vid, Properties>`. PropertyManager is not removed — the columnar path simply bypasses it.
- **CRDT columns are the exception, not the rule.** The fast path handles non-CRDT columns entirely in columnar mode. CRDT merging falls back to row-level only for those specific columns.
- **L0 overlay must respect visibility order.** Pending flush (oldest) → Current → Transaction (newest). The version-based dedup handles this as long as L0 versions are monotonically increasing.
- **Snapshot isolation preserved.** The `_version <= hwm` filter is pushed to Lance. L0 overlay respects `version_high_water_mark` as well.
