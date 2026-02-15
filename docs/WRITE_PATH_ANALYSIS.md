# Write Path Analysis (Current)

This document describes the write pipeline from Cypher/Writer to persistent
storage, including L0 buffering, WAL, and L1/L2 persistence.

---

## 1. Regular Writer Path

**Location:** `crates/uni-store/src/runtime/writer.rs`

### 1.1 Vertex Insert

`insert_vertex_with_labels(vid, properties, labels)`:

- `check_write_pressure()` throttles if too many L1 runs exist.
- `process_embeddings_for_labels()` generates vector properties based on
  embedding index config (FastEmbed only).
- `validate_vertex_constraints()` enforces NOT NULL, UNIQUE, EXISTS, CHECK,
  and global `ext_id` uniqueness.
- `prepare_vertex_upsert()` merges CRDT values.
- Writes the mutation to the active L0 buffer (transaction L0 if present).

### 1.2 Vertex Delete

`delete_vertex(vid)`:

- Ensures labels are known (L0, pending L0s, or storage) so tombstones flush
  to the correct label tables.
- Writes a vertex tombstone into L0.

### 1.3 Edge Insert/Delete

`insert_edge(...)` and `delete_edge(...)`:

- CRDT merge for properties on upsert.
- Mutations recorded in L0 (properties + topology + tombstones).

---

## 2. L0 -> L1 Flush

**Trigger:**

- `auto_flush_threshold` (default 10k mutations)
- `auto_flush_interval` when `auto_flush_min_mutations` is met

**Flow (`flush_to_l1`)**:

1. Flush WAL and capture the WAL LSN.
2. Rotate L0 (old L0 becomes `pending_flush`, new L0 becomes active).
3. Collect edge entries + tombstones into per-type delta runs.
4. Collect vertex entries + tombstones grouped by label.
5. Write edge delta tables:
   - `deltas_{Type}_fwd` and `deltas_{Type}_bwd`
   - Ensure `eid` index
6. Write per-label vertex tables:
   - `vertices_{Label}`
   - Ensure `_vid`, `_uid`, `ext_id` indexes
   - Apply incremental inverted index updates if configured
7. Dual-write main tables:
   - `edges` (props_json + type + tombstones)
   - `vertices` (props_json + labels + tombstones)
8. Save snapshot manifest + update latest snapshot.
9. Complete flush, truncate WAL safely (respecting pending L0s).

**Note:** Adjacency CSR tables are **not** updated here; they are built by
compaction.

---

## 3. BulkWriter Path

**Location:** `crates/uni/src/api/bulk.rs`

### 3.1 Insert Vertices

`insert_vertices(label, vertices)`:

- Optional constraint validation (NOT NULL, UNIQUE, CHECK).
- Allocates VIDs via Writer's allocator.
- Buffers by label in memory.
- Flushes when:
  - Per-label buffer reaches `batch_size` (default 10k), or
  - Total buffer exceeds `max_buffer_size_bytes` (default 1GB, triggers checkpoint).

### 3.2 Insert Edges

`insert_edges(edge_type, edges)`:

- Allocates EIDs.
- Buffers in memory and flushes at batch/size thresholds.

### 3.3 Flush Buffers

- Vertices -> `vertices_{Label}` + main `vertices` table.
- Edges -> `deltas_{Type}_{dir}` + main `edges` table.
- No WAL; rollback uses LanceDB table versioning.

### 3.4 Commit

- Flushes remaining buffers.
- Rebuilds deferred indexes (sync or async).
- Updates snapshot manifest.

### 3.5 Abort

- Clears buffers.
- Rolls back tables to recorded versions (or drops newly created tables).

---

## 4. Storage Table Schemas (Write Targets)

**Per-label vertex table (`vertices_{Label}`):**

- `_vid`, `_uid`, `_deleted`, `_version`, `ext_id`, `_labels`, `_created_at`,
  `_updated_at`, plus schema-defined property columns.
- `_labels` is a `List<Utf8>` column carrying the vertex's complete label set
  (e.g., `["Person", "Employee"]`), ensuring multi-label information survives
  flush and compaction.

**Edge delta tables (`deltas_{Type}_fwd` / `deltas_{Type}_bwd`):**

- `src_vid`, `dst_vid`, `eid`, `op`, `_version`, `_created_at`, `_updated_at`,
  plus schema-defined edge property columns.

**Main tables:**

- `vertices`: `_vid`, `_uid`, `ext_id`, `labels`, `props_json`, `_deleted`,
  `_version`, `_created_at`, `_updated_at`
- `edges`: `_eid`, `src_vid`, `dst_vid`, `type`, `props_json`, `_deleted`,
  `_version`, `_created_at`, `_updated_at`

---

## 5. Regular Writer vs BulkWriter

| Aspect | Regular Writer | BulkWriter |
|--------|----------------|------------|
| Buffer | L0 (SimpleGraph) | In-memory HashMaps |
| WAL | Yes | No |
| Constraints | Per-write | Per-batch (optional) |
| CRDT merge | Yes | No |
| Embeddings | Yes | No |
| Flush trigger | Mutation count / interval | Batch size / buffer size |
| Rollback | WAL | LanceDB version rollback |
| Index build | On flush | Deferred to commit (sync/async) |
| Main tables | Dual-write on flush | Dual-write on buffer flush |

