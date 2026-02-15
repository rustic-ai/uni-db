# Read/Query Path Analysis (Current)

This document describes the current Cypher read path in Uni, from parsing to
execution and storage access. The primary execution engine is DataFusion with
custom graph operators; unsupported operations fall back to the legacy row-based
executor.

---

## 1. Parsing and AST

**Parser:** `crates/uni-query/src/query/parser.rs`

- `CypherParser` tokenizes input using `sqlparser`'s tokenizer and builds a
  Cypher AST.
- AST nodes are defined in `crates/uni-query/src/query/ast.rs`.

---

## 2. Logical Planning

**Planner:** `crates/uni-query/src/query/planner.rs`

The planner converts AST to a `LogicalPlan` tree and performs targeted
optimizations:

- Extracts `vector_similarity(...)` into `LogicalPlan::VectorKnn`.
- Extracts `ANY IN [...]` patterns into `LogicalPlan::InvertedIndexLookup` if an
  inverted index exists.
- Pushes variable-scoped predicates into `Scan` or `Traverse` nodes where
  possible.

---

## 3. Execution Engine Selection

**Executor:** `crates/uni-query/src/query/executor/read.rs`

`Executor::execute` chooses the execution path:

- **Primary**: DataFusion hybrid engine (`execute_datafusion`) for read queries.
- **Bypass**: DDL/admin/write plans go directly to the legacy executor.
- **Fallback**: If DataFusion fails (unsupported plan, planning errors), it
  falls back to the legacy executor unless the error is a timeout/memory limit.

Unsupported in DataFusion today (falls back):
- Writes (CREATE/MERGE/SET/DELETE/REMOVE)
- Window functions
- Joins (CrossJoin/Apply)
- Inverted index lookup
- allShortestPaths, quantified patterns, recursive CTEs
- Procedure calls, LOAD CSV

---

## 4. DataFusion Hybrid Execution (Primary Path)

**Planner:** `crates/uni-query/src/query/df_planner.rs`

The hybrid planner produces a DataFusion `ExecutionPlan` with custom graph
operators:

- `GraphScanExec` (vertex scans)
- `GraphTraverseExec` / `GraphVariableLengthTraverseExec`
- `GraphShortestPathExec`
- `GraphVectorKnnExec`
- `GraphExtIdLookupExec`
- `GraphUnwindExec`

These operators use `GraphExecutionContext` (`df_graph/mod.rs`) to access:

- `AdjacencyCache` for traversal
- L0 buffers for MVCC visibility
- `PropertyManager` for property materialization

### 4.1 GraphScanExec

**Location:** `crates/uni-query/src/query/df_graph/scan.rs`

- Collects VIDs from `vertices_{Label}` tables and overlays L0 buffers.
- Uses `PropertyManager::get_batch_vertex_props` to materialize property columns.
- Filters are applied via a DataFusion `FilterExec` on top of the scan
  (no storage pushdown in the DataFusion path today).

### 4.2 GraphTraverseExec

**Location:** `crates/uni-query/src/query/df_graph/traverse.rs`

- Uses `AdjacencyCache` + L0 overlays to expand neighbors.
- Emits target `_vid` and optional edge IDs.
- Target label filtering is not enforced in the DataFusion path today.

### 4.3 GraphExtIdLookupExec

**Location:** `crates/uni-query/src/query/df_graph/ext_id_lookup.rs`

- Looks up VID in the main `vertices` table via `MainVertexDataset::find_by_ext_id`.
- Loads properties with `PropertyManager`.
- Resolves labels from the main table and emits `_labels`.

### 4.4 GraphVectorKnnExec

**Location:** `crates/uni-query/src/query/df_graph/vector_knn.rs`

- Calls `StorageManager::vector_search` (LanceDB vector index).
- Emits `_vid`, variable name, and `_score` (distance).

---

## 5. Legacy Execution (Fallback / DDL / Writes)

**Location:** `crates/uni-query/src/query/executor/read.rs`

The legacy executor is row-based and also handles DDL and write operations.
Key read behavior:

- `scan_storage_candidates` performs a LanceDB scan on `vertices_{Label}` and
  applies `LanceFilterGenerator` for pushdown when possible.
- L0 buffers (current, transaction, pending flush) are overlaid for MVCC.
- Filters not pushed down are evaluated in Rust.
- Traversals use `WorkingGraph` (CSR + L0) and BFS for variable-length paths.
- `InvertedIndexLookup` uses the inverted index and then validates visibility
  via `PropertyManager`.

---

## 6. Property Loading and MVCC

**Location:** `crates/uni-store/src/runtime/property_manager.rs`

- `get_batch_vertex_props` scans **all label tables** because VIDs do not
  embed label information, then overlays L0 buffers.
- `get_all_vertex_props_with_ctx` merges L0 + L1 values using `_version`,
  with CRDT merge semantics where applicable.
- Edge property lookup scans delta runs for all edge types and merges by version.
- LRU cache is used for single-property fetches.

---

## 7. Index Usage and Pushdown

- Scalar/vector/fulltext/json-fts/inverted indexes are built by
  `IndexManager` (`crates/uni-store/src/storage/index_manager.rs`).
- The legacy path uses `LanceFilterGenerator` for basic filter pushdown.
- Inverted index lookups are executed only in the legacy path today.
- UID and JsonPath indexes exist, but are not wired into query planning.

---

## 8. Summary

- **Primary engine**: DataFusion hybrid planner + custom graph operators.
- **Fallback**: Legacy row executor for unsupported plans and DDL/writes.
- **Storage access**: Per-label vertex tables, edge delta tables, L0 overlays,
  adjacency cache, and property manager merging.

