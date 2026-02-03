# CRUD Query Codeflow

This document ties together the Cypher query pipeline (uni-query) and the
storage/runtime pipeline (uni-store) to explain end-to-end CRUD behavior.

---

## 1. Core Components

- **Parser**: `CypherParser` (`crates/uni-query/src/query/parser.rs`)
- **Planner**: `QueryPlanner` -> `LogicalPlan` (`crates/uni-query/src/query/planner.rs`)
- **Executor**:
  - DataFusion hybrid engine (default for reads)
  - Legacy row-based executor (writes + fallback)
- **Writer**: `runtime/writer.rs` (L0 + WAL + flush)
- **PropertyManager**: L0/L1 merge + caching
- **Storage**:
  - `vertices_{Label}` and `deltas_{Type}_{dir}`
  - main `vertices` and `edges`
  - adjacency CSR (`adjacency_{Type}_{dir}`) built by compaction

---

## 2. Create (CREATE / MERGE)

### Cypher -> Plan

1. Parse Cypher to AST.
2. Planner builds `LogicalPlan::Create` or `LogicalPlan::Merge`.
3. Executor routes writes to the **legacy executor** (DataFusion does not
   support write plans).

### Legacy Write Execution

- `execute_create_pattern` builds property maps from expressions.
- `execute_merge` may use a composite unique index to detect existing nodes.
- Generated properties are applied via `enrich_properties_with_generated_columns`.

### Storage Writes

- `Writer::insert_vertex_with_labels`:
  - validates constraints (NOT NULL, UNIQUE, EXISTS, CHECK)
  - generates embeddings (FastEmbed only)
  - merges CRDT values
  - writes to L0 (SimpleGraph + properties + labels + version)
- `Writer::insert_edge` writes edge mutations to L0.

### Durability and Flush

- WAL is flushed before L0 rotation.
- Flush writes:
  - per-label vertex tables (`vertices_{Label}`)
  - per-type delta tables (`deltas_{Type}_{dir}`)
  - main tables (`vertices`, `edges`)
  - snapshot manifest update

---

## 3. Read (MATCH / RETURN)

### Plan -> Execution Engine

1. Planner may rewrite:
   - `vector_similarity` -> `VectorKnn`
   - `ANY IN [...]` -> `InvertedIndexLookup`
2. Executor uses **DataFusion hybrid engine** by default.
3. Unsupported plans fall back to the legacy executor.

### DataFusion Hybrid Execution

- `GraphScanExec` collects VIDs from `vertices_{Label}` + L0 overlays, then
  materializes properties via `PropertyManager::get_batch_vertex_props`.
- `GraphTraverseExec` expands neighbors using `AdjacencyCache` + L0 overlays.
- `GraphExtIdLookupExec` queries the main `vertices` table for `ext_id`.
- `GraphVectorKnnExec` uses `StorageManager::vector_search`.

### Legacy Execution (Fallback)

- `scan_storage_candidates` uses LanceDB filters when possible.
- L0 overlays are merged with storage data.
- Traversals use `WorkingGraph` (CSR + L0) with BFS.
- Inverted index lookups are executed here.

### Property Visibility

- `PropertyManager` merges L0 + L1 by `_version`.
- CRDT properties are merged; non-CRDT use LWW semantics.
- Deleted rows are filtered using tombstones.

---

## 4. Update (SET / REMOVE)

### SET

- `execute_set_items_locked` reads current properties via `PropertyManager`.
- New values are applied in memory, then written back via
  `Writer::insert_vertex_with_labels` or `Writer::insert_edge` (upsert).
- Generated columns are recomputed on update.

### REMOVE

- Property removal is implemented by setting the property to `null` and
  re-inserting the node/edge in L0.
- Label add/remove is **not supported** in the current write executor.

---

## 5. Delete (DELETE / DETACH DELETE)

- `execute_delete_vertex` checks edge existence for non-detach deletes.
- `detach` mode deletes incident edges first.
- Deletes write tombstones into L0 and are persisted on flush.

---

## 6. Consistency and Visibility

- Reads see the union of:
  - L0 (current + transaction + pending flush)
  - L1 (LanceDB tables)
- WAL provides durability between L0 and L1.
- Snapshot manifests track table versions and WAL high-water marks.

---

## 7. Storage Touch Points (Quick Map)

| Operation | Primary Tables |
|-----------|----------------|
| Create/Update vertex | `vertices_{Label}` + main `vertices` |
| Create/Update edge | `deltas_{Type}_{dir}` + main `edges` |
| Read by label | `vertices_{Label}` + L0 overlays |
| Read by ext_id | main `vertices` + property manager |
| Traverse | adjacency CSR + delta runs + L0 |
| Vector search | `vertices_{Label}` vector index |

