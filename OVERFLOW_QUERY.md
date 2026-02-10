# Overflow Property Query Failure Analysis

## Summary

Commit `d840fc4` ("feat: use LargeBinary/JSONB encoding for schemaless vertex properties") changed the Arrow type used for schemaless overflow properties from `Utf8` to `LargeBinary` in `scan.rs`. This broke 4 tests that query overflow properties after flush. The immediate cause is a type mismatch in the DataFusion filter path, but the failure exposes a deeper latent bug: the row-based fallback executor blindly pushes overflow property names to Lance as physical column references.

## The 4 Failing Tests

| Test | Failing Filter | Missing Column |
|------|---------------|----------------|
| `overflow_json_e2e::test_mixed_schema_and_overflow_properties` | `p.city = 'NYC'` | `city` |
| `overflow_json_e2e::test_overflow_properties_across_multiple_flushes` | `e.type = 'click'` | `type` |
| `overflow_json_e2e::test_bulk_overflow_properties` | `l.level = 'info'` | `level` |
| `test_overflow_fix::test_where_clause_on_overflow_property` | `p.category = 'books'` | `category` |

All produce the same class of error from Lance:

```
LanceError(Schema): Schema error: No field named city.
Valid fields are _vid, _uid, _deleted, _version, ext_id, _created_at, _updated_at, name, overflow_json
```

## Architecture: Two Execution Paths

The query executor has two paths for read queries (`executor/read.rs:757-774`):

```
execute()
├── Try: execute_datafusion(plan)     ← DataFusion engine (preferred)
│   └── On success: return results
└── Fallback: execute_subplan(plan)   ← Row-based executor
    └── Only if DataFusion fails (and error is not timeout/OOM)
```

These paths handle scan filters completely differently.

### Path A: DataFusion Engine (`df_planner.rs`)

```
LogicalPlan::Scan { filter: Some(p.city = 'NYC'), ... }
    │
    ├── plan_scan() creates GraphScanExec with filter: None
    │   └── GraphScanExec reads ALL vertices from Lance (no filter pushdown)
    │       └── Materializes overflow properties as Arrow columns in RecordBatch
    │           └── p.city becomes a column with type Utf8 (before) or LargeBinary (after)
    │
    └── apply_scan_filter() wraps with FilterExec
        └── DataFusion evaluates p.city = 'NYC' in-memory against the RecordBatch
```

In this path, Lance never sees the column name `city`. The scan fetches all data, the properties are extracted from `overflow_json` into individual Arrow columns, and DataFusion's `FilterExec` evaluates the predicate in-memory.

### Path B: Row-Based Executor (`executor/read.rs`)

```
LogicalPlan::Scan { filter: Some(p.city = 'NYC'), ... }
    │
    └── scan_label_with_filter()
        ├── scan_storage_candidates()
        │   └── LanceFilterGenerator::generate(filter, variable)
        │       └── extract_column() returns bare property name: "city"
        │       └── Generates Lance SQL: city = 'NYC'
        │       └── query.only_if("city = 'NYC'")  ← PUSHED TO LANCE
        │           └── Lance rejects: "No field named city"
        │
        └── verify_and_filter_candidates()  ← never reached
```

In this path, `LanceFilterGenerator` (`pushdown.rs:545-804`) converts the Cypher predicate to a Lance SQL string and pushes it directly to the per-label LanceDB table. It has no awareness of whether a property is a physical column or lives in `overflow_json`.

## Per-Label VertexDataset Schema

When a label like `Person` is registered with `schema().label("Person").property("name", String)`, the per-label Lance table has this physical schema:

```
_vid            UInt64
_uid            FixedSizeBinary(32)
_deleted        Boolean
_version        UInt64
ext_id          Utf8
_created_at     Timestamp
_updated_at     Timestamp
name            Utf8              ← schema-defined property (physical column)
overflow_json   LargeBinary       ← JSONB blob containing all non-schema properties
```

Properties like `city` and `age` are **not** physical columns; they are serialized inside `overflow_json`. Pushing `city = 'NYC'` as a Lance filter is invalid.

## The Cascade: Why Utf8 Worked and LargeBinary Doesn't

### Before the commit (Utf8)

1. `build_schemaless_vertex_schema` declares `p.city` as `Utf8`
2. `build_schemaless_vertex_record_batch` encodes values as JSON strings in a `StringBuilder`
3. DataFusion's `FilterExec` evaluates `p.city = 'NYC'`:
   - Left side: `Utf8` column containing `"NYC"`
   - Right side: `Utf8` literal `'NYC'`
   - Comparison succeeds (both are Utf8)
4. **DataFusion path succeeds** → results returned → fallback never triggered

### After the commit (LargeBinary)

1. `build_schemaless_vertex_schema` declares `p.city` as `LargeBinary`
2. `build_schemaless_vertex_record_batch` encodes values as JSONB bytes in a `LargeBinaryBuilder`
3. DataFusion's `FilterExec` evaluates `p.city = 'NYC'`:
   - Left side: `LargeBinary` column containing JSONB-encoded bytes
   - Right side: `Utf8` literal `'NYC'`
   - **Type mismatch** → DataFusion cannot compare `LargeBinary` to `Utf8`
4. **DataFusion path fails** → falls back to `execute_subplan`
5. Row executor calls `scan_storage_candidates` → pushes `city = 'NYC'` to Lance
6. Lance rejects: **"No field named city"**

The DataFusion type mismatch is the trigger, but the Lance schema error in the fallback path is the error the user sees.

## The Two Bugs

### Bug 1 (Immediate): DataFusion filter type mismatch

**Location:** `scan.rs:250-268` (`build_schemaless_vertex_schema`)
and `scan.rs:780-801` (`build_schemaless_vertex_record_batch`)

The schema declares overflow property columns as `LargeBinary` and stores JSONB bytes, but filter expressions contain `Utf8` string literals. DataFusion cannot compare `LargeBinary` to `Utf8`.

**Fix options:**
- A) Decode JSONB back to native Arrow types when building the RecordBatch (preferred for correctness)
- B) Register a UDF that decodes JSONB for comparisons
- C) Rewrite filter expressions to decode JSONB before comparison

### Bug 2 (Latent): Blind filter pushdown to Lance

**Location:** `executor/read.rs:155-184` (`scan_storage_candidates`)
and `pushdown.rs:748-759` (`LanceFilterGenerator::extract_column`)

`LanceFilterGenerator` converts Cypher property references to bare column names and pushes them to Lance without checking whether the property exists as a physical column. For overflow properties, this is always invalid.

**Fix options:**
- A) Check property against label schema in `LanceFilterGenerator` — only push if the property is a physical column, otherwise leave as residual for `verify_and_filter_candidates`
- B) Rewrite overflow property references to JSONB extraction expressions (e.g., `json_extract_string(overflow_json, '$.city') = 'NYC'`)
- C) Skip filter pushdown entirely for labels with `overflow_json`

## Code Path Reference

### Planner: WHERE clause → Scan filter

```
planner.rs:3042  plan_where_clause()
planner.rs:3130    find_scan_label_id() — is variable produced by a Scan?
planner.rs:3132    extract_variable_predicates() — split pushable vs residual
planner.rs:3136    push_predicate_to_scan() — pushes predicate into Scan.filter
```

The `PredicateAnalyzer` (`pushdown.rs:240-375`) decides pushability based purely on AST structure (is it `Property == Literal`?), with no schema awareness. So `p.city = 'NYC'` is always classified as pushable.

### Planner: Label routing

```
planner.rs:2982  schema.get_label_case_insensitive(label_name)
                 ├── Found → LogicalPlan::Scan { label_id, filter }
                 │           Uses per-label VertexDataset (has overflow_json)
                 └── Not found → LogicalPlan::ScanMainByLabels { labels, filter }
                                 Uses MainVertexDataset (has props_json)
```

All failing tests register labels in the schema (e.g., `db.schema().label("Person")`), so they take the `LogicalPlan::Scan` path.

### Executor: DataFusion → Fallback routing

```
read.rs:757   else (standard read query):
read.rs:760     execute_datafusion(plan)
read.rs:763       Ok → record_batches_to_rows()
read.rs:764       Err →
read.rs:771         execute_subplan(plan)    ← fallback
```

### DataFusion path: filter applied in-memory

```
df_planner.rs:818  plan_scan()
df_planner.rs:834    GraphScanExec::new_vertex_scan(filter: None)
df_planner.rs:850    apply_scan_filter() → FilterExec wraps the scan
                     Filter evaluated in-memory against RecordBatch columns
```

### Fallback path: filter pushed to Lance

```
read.rs:2378   scan_label_with_filter(label_id, variable, filter)
read.rs:232      scan_storage_candidates(label_id, variable, filter)
read.rs:166        vertex_dataset(label_name) → opens per-label LanceDB table
read.rs:179        LanceFilterGenerator::generate(filter, variable)
pushdown.rs:748      extract_column() → returns bare "city"
pushdown.rs:725      generates: city = 'NYC'
read.rs:183        query.only_if("city = 'NYC'")  → Lance schema error
```

## Relationship Between the Two Bugs

```
                    ┌──────────────────────────┐
                    │  Commit d840fc4           │
                    │  Utf8 → LargeBinary       │
                    └──────────┬───────────────┘
                               │
                               ▼
                    ┌──────────────────────────┐
                    │  Bug 1: Type mismatch     │
                    │  LargeBinary vs Utf8      │
                    │  in DataFusion FilterExec  │
                    └──────────┬───────────────┘
                               │
                     DataFusion path fails
                               │
                               ▼
                    ┌──────────────────────────┐
                    │  Fallback to row executor │
                    │  execute_subplan()        │
                    └──────────┬───────────────┘
                               │
                               ▼
                    ┌──────────────────────────┐
                    │  Bug 2: Blind pushdown    │
                    │  Pushes "city = 'NYC'"    │
                    │  to Lance (no such column)│
                    └──────────────────────────┘
                               │
                               ▼
                    ┌──────────────────────────┐
                    │  LanceError(Schema)       │
                    │  "No field named city"    │
                    └──────────────────────────┘
```

Bug 2 has always existed but was masked because the DataFusion path succeeded with `Utf8`. The commit exposed it by breaking the DataFusion path, causing queries to fall through to the buggy fallback.
