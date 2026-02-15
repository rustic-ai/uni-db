# Uni Storage Design (Current)

**Date**: 2026-01-22 (updated)
**Status**: Current

---

## Executive Summary

Uni uses a layered, LSM-style storage model:

- **L0 (in-memory)**: `L0Buffer` stores recent mutations (SimpleGraph topology,
  properties, tombstones, labels, versions) with optional WAL durability.
- **L1 (LanceDB)**: append-only tables for vertices and edge deltas.
- **L2 (Adjacency CSR)**: adjacency tables materialized by compaction for fast
  traversal.

Vertices are stored in **per-label typed tables** and also **dual-written** to a
main `vertices` table for label/ext_id lookups. Edges are stored as **delta runs**
per edge type/direction and dual-written to a main `edges` table. VIDs/EIDs are
pure auto-increment IDs (no label/type bits).

---

## 1. Storage Layers

### 1.1 L0 Buffer (In-Memory)

- `L0Buffer` holds:
  - SimpleGraph topology (adjacency lists)
  - vertex/edge properties
  - tombstones
  - versions, timestamps
  - vertex labels and edge types
- Optional WAL (`runtime/wal.rs`) provides durability before L0 rotation.
- L0 data is visible to reads via `QueryContext` (current, transaction, and
  pending-flush L0s).

### 1.2 L1 Tables (LanceDB)

- Per-label vertex tables: `vertices_{Label}`
- Per-edge-type delta tables: `deltas_{Type}_fwd` and `deltas_{Type}_bwd`
- Main unified tables: `vertices`, `edges`

### 1.3 L2 Adjacency (CSR)

- Adjacency CSR tables: `adjacency_{Type}_fwd` and `adjacency_{Type}_bwd`
- Built by compaction from delta runs (not updated on every write).

---

## 2. Vertex Storage

### 2.1 Per-Label Vertex Tables (`vertices_{Label}`)

Per-label tables store typed columns for schema-defined properties.

**Schema** (vertex tables):

| Column | Type | Notes |
|--------|------|-------|
| `_vid` | UInt64 | Internal vertex ID |
| `_uid` | FixedSizeBinary(32) | Content hash per label |
| `_deleted` | Boolean | Tombstone flag |
| `_version` | UInt64 | MVCC version |
| `ext_id` | Utf8 | Extracted from properties |
| `_labels` | List\<Utf8\> | Complete label set for multi-label support |
| `_created_at` | Timestamp(utc, us) | Creation time |
| `_updated_at` | Timestamp(utc, us) | Update time |
| `{props}` | Typed | Schema-defined properties only |
| `overflow_json` | LargeBinary | Non-schema properties as JSONB binary |

**Notes**:

- Properties declared in the label schema are stored as typed columns.
- Properties NOT declared in the label schema are stored in the `overflow_json` column as a JSONB binary blob.
- PropertyManager reads from typed columns first, then decodes `overflow_json` for non-schema properties.
- Storage layout:
  - Schema-defined properties → Typed Arrow columns (indexed, compressed, optimized for filtering)
  - Non-schema properties → `overflow_json` (JSONB binary, queryable via automatic query rewriting)
  - Main table `props_json` → Still contains all properties (redundant for overflow)
- Query rewriting: WHERE clauses on overflow properties are automatically rewritten to use Lance's built-in JSONB functions (`json_get_string`, `json_get_int`, etc.) for efficient querying
- JSONB binary format provides better performance than JSON strings and is compatible with PostgreSQL's JSONB format
- `_uid` is computed from **label + ext_id + sorted properties**.
- `_labels` stores the complete label set (e.g., `["Person", "Employee"]`) for
  each vertex, enabling multi-label support. This column is preserved through
  flush, compaction, and scan, so labels are never lost even after L0 is cleared.
- Multi-label vertices are written to **each label table** listed in L0.
  Each table stores only its schema-defined columns; other properties are
  ignored for that table. Every copy carries the full `_labels` list.

### 2.2 Main Vertex Table (`vertices`)

The main table stores all vertices, used for ext_id/label lookups and
cross-label discovery.

**Schema**:

| Column | Type | Notes |
|--------|------|-------|
| `_vid` | UInt64 | Internal vertex ID |
| `_uid` | FixedSizeBinary(32) | Content hash across labels |
| `ext_id` | Utf8 | Global external ID (optional) |
| `labels` | List<Utf8> | Multi-label support |
| `props_json` | Utf8 | JSON blob of all properties |
| `_deleted` | Boolean | Tombstone flag |
| `_version` | UInt64 | MVCC version |
| `_created_at` | Timestamp(utc, us) | Creation time |
| `_updated_at` | Timestamp(utc, us) | Update time |

**Notes**:

- `props_json` is not used by `PropertyManager` for reads; typed tables remain
  the source of truth for query evaluation.
- `labels` are used for ext_id lookups and label resolution.

---

## 3. Edge Storage

### 3.1 Delta Tables (`deltas_{Type}_fwd`, `deltas_{Type}_bwd`)

Edges are stored as delta runs with insert/delete ops.

**Schema**:

| Column | Type | Notes |
|--------|------|-------|
| `src_vid` | UInt64 | Source vertex |
| `dst_vid` | UInt64 | Destination vertex |
| `eid` | UInt64 | Edge ID |
| `op` | UInt8 | 0=Insert, 1=Delete |
| `_version` | UInt64 | MVCC version |
| `_created_at` | Timestamp(utc, us) | Creation time |
| `_updated_at` | Timestamp(utc, us) | Update time |
| `{props}` | Typed | Schema-defined properties |
| `overflow_json` | LargeBinary | Non-schema properties as JSONB binary |

**Notes**:

- Edge properties declared in the edge-type schema are stored as typed columns.
- Edge properties NOT declared in the schema are stored in the `overflow_json` column as JSONB binary.
- Reads merge delta rows by version; deletes override older inserts.
- Like vertex properties, overflow edge properties are queryable via automatic query rewriting to JSONB functions.

### 3.2 Main Edge Table (`edges`)

**Schema**:

| Column | Type | Notes |
|--------|------|-------|
| `_eid` | UInt64 | Edge ID |
| `src_vid` | UInt64 | Source vertex |
| `dst_vid` | UInt64 | Destination vertex |
| `type` | Utf8 | Edge type name |
| `props_json` | Utf8 | JSON blob of all properties |
| `_deleted` | Boolean | Tombstone flag |
| `_version` | UInt64 | MVCC version |
| `_created_at` | Timestamp(utc, us) | Creation time |
| `_updated_at` | Timestamp(utc, us) | Update time |

---

## 4. Adjacency and Traversal

- `AdjacencyCache` maintains CSR structures keyed by `(edge_type, direction)`.
- On cache miss, CSR is built from L2 adjacency tables plus L1 delta runs.
- L0 buffers overlay the CSR for MVCC visibility.
- Adjacency tables are materialized by compaction, not on every write.

---

## 5. Identity Model

| ID | Type | Usage |
|----|------|-------|
| `_vid` | UInt64 | Internal vertex ID (auto-increment) |
| `_eid` | UInt64 | Internal edge ID (auto-increment) |
| `_uid` | FixedSizeBinary(32) | Vertex content hash (no edge _uid today) |
| `ext_id` | Utf8 | Optional external ID for application references |

Notes:

- VIDs/EIDs do not embed label/type bits.
- `ext_id` uniqueness is enforced at write time via L0 + main table lookup.

---

## 6. Indexing

**Default Indexes** (created on flush):

- `vertices_{Label}`: `_vid`, `_uid`, `ext_id` (BTree)
- `deltas_{Type}_*`: `eid` (BTree)
- `vertices`: `_vid`, `_uid`, `ext_id`
- `edges`: `_eid`, `src_vid`, `dst_vid`, `type`

**User-Defined Indexes** (via schema/index manager):

- Scalar (BTree) indexes on one or more columns
- Vector indexes (IVF-PQ, HNSW, Flat)
- Full-text indexes (inverted index over columns)
- JSON full-text indexes (BM25 over JSON document columns)
- Inverted indexes (term -> vid list)
- JsonPath indexes (separate dataset `indexes/idx_{label}_{path}`)

---

## 7. Schemaless Properties and Query Rewriting

### 7.1 Overflow Properties

Uni supports **schemaless properties** - properties that are not defined in the schema but can be stored and queried dynamically. This enables flexible data modeling without predefined schemas.

**Storage Mechanism**:
- Properties declared in the label/edge-type schema → Stored as typed Arrow columns (fast, indexed, compressed)
- Properties NOT in schema → Stored in `overflow_json` column as JSONB binary

**Example**:
```cypher
-- Label has no properties defined in schema
CREATE LABEL Document;

-- Create with arbitrary properties
CREATE (:Document {title: 'Article', author: 'Alice', tags: ['tech', 'ai'], year: 2024});

-- All properties accessible normally
MATCH (d:Document) WHERE d.author = 'Alice' RETURN d.title, d.year;
```

### 7.2 JSONB Binary Format

The `overflow_json` column uses JSONB binary format (PostgreSQL-compatible):
- **Type**: `LargeBinary` Arrow column
- **Encoding**: JSONB binary (not JSON string)
- **Library**: `jsonb` crate for encoding/decoding
- **Benefits**:
  - More efficient than JSON strings (binary representation)
  - Compatible with PostgreSQL JSONB tools
  - Enables use of Lance's optimized JSONB UDFs

### 7.3 Automatic Query Rewriting

When a query accesses an overflow property, the query planner automatically rewrites it to use Lance's built-in JSONB functions:

**Original Cypher**:
```cypher
MATCH (p:Person) WHERE p.city = 'NYC' RETURN p.name, p.age
```

**Rewritten DataFusion Plan** (if `city` and `age` are overflow properties):
```sql
-- Property access rewritten to JSONB functions
WHERE json_get_string(p.overflow_json, 'city') = 'NYC'
RETURN p.name, json_get_int(p.overflow_json, 'age')
```

**Supported JSONB Functions**:
- `json_get_string(overflow_json, key)` - Extract string value
- `json_get_int(overflow_json, key)` - Extract integer value
- `json_get_float(overflow_json, key)` - Extract float value
- `json_get_bool(overflow_json, key)` - Extract boolean value

### 7.4 Performance Characteristics

**Schema Properties** (Typed Columns):
- ✅ Fast filtering and sorting (native Arrow operations)
- ✅ Efficient compression (type-specific compression)
- ✅ Column pruning (read only needed columns)
- ✅ Indexed access (when indexes exist)
- ⚠️ Requires schema migration to add new properties

**Overflow Properties** (JSONB):
- ✅ Flexible schema (no migration needed)
- ✅ Queryable via automatic rewriting
- ✅ Binary format (faster than JSON strings)
- ⚠️ Slower than typed columns (requires JSONB parsing)
- ⚠️ No column-level compression or indexing

**Recommendation**: Use typed schema properties for frequently-queried fields and core data model. Use overflow properties for:
- Optional/rare properties
- User-defined metadata
- Rapidly evolving schemas
- Prototyping/exploratory work

### 7.5 Mixed Schema + Overflow Queries

Queries can seamlessly mix schema properties and overflow properties:

```cypher
-- Schema: Person has 'name' property defined
-- Overflow: 'city' and 'age' are not in schema

CREATE LABEL Person PROPERTIES (name STRING);
CREATE (:Person {name: 'Alice', city: 'NYC', age: 30});

-- Query mixing both types (transparent to user)
MATCH (p:Person)
WHERE p.name = 'Alice' AND p.city = 'NYC'  -- name: typed column, city: overflow_json
RETURN p.name, p.age;  -- age from overflow_json
```

The query planner handles the rewriting automatically - users don't need to know which properties are in schema vs overflow.

---

## 8. MVCC and Versioning

- Every mutation increments an L0 version counter.
- `_version` is persisted into L1 tables and used to merge L0/L1 rows.
- CRDT properties merge values across versions; non-CRDT properties use LWW.

---

## 9. LanceDB Table Naming

| Logical Table | LanceDB Table Name |
|--------------|--------------------|
| Per-label vertices | `vertices_{Label}` |
| Edge deltas (fwd/bwd) | `deltas_{Type}_fwd`, `deltas_{Type}_bwd` |
| Adjacency (CSR) | `adjacency_{Type}_fwd`, `adjacency_{Type}_bwd` |
| Main vertices | `vertices` |
| Main edges | `edges` |

