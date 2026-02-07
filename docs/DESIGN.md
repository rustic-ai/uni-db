# Uni: Graph Storage and Query Design

A comprehensive design for an embedded, object-store-backed graph database with Lance storage, OpenCypher queries, hybrid vector search, and custom graph runtime.

## Overview

### Vision

Build an embedded, object-store-backed database that supports property graph workloads with OpenCypher queries (declared subset), columnar analytics (via Cypher aggregations), and vector search in a single engine.

### Goals

- Provide property-graph storage for OpenCypher declared subset queries
- Read-heavy performance at scale (GBs to PBs) with single-writer, multi-reader
- Object-store friendly layout (S3/GCS) using Lance datasets and snapshots
- Support hybrid queries (graph + vector + document/JSON filters)
- Leverage custom SimpleGraph for in-memory graph operations and algorithms
- **Columnar analytics via Cypher aggregations** (COUNT, SUM, AVG, COLLECT, etc.) - no separate SQL interface
- **Content-addressed UIDs**: Support for deterministic, content-based element identification

### Non-Goals (Initial)

- Distributed multi-writer transactions
- Online graph algorithms beyond traversal + pattern matching
- Strong guarantees for cross-process concurrent writers
- Real-time streaming updates without compaction
- Multi-label vertices (single label per vertex in v1)

### Design Principles

1. **Object-store-first**: Minimize round trips, maximize sequential reads, assume 100ms latency
2. **Simplicity over generality**: Single-label vertices, explicit constraints, fewer options
3. **LSM-style writes**: Memory buffer → sorted runs → compacted base
4. **Self-contained chunks**: One read gets everything needed, no joins across files
5. **Custom SimpleGraph**: Lightweight in-memory graph structure; focus custom work on storage
6. **Explicit trade-offs**: Document what's slow and why

---

## Part 1: Architecture Overview

### Layered Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                         Query Layer                              │
│  ┌──────────┐  ┌──────────┐  ┌───────────┐  ┌───────────────┐   │
│  │  Parser  │→ │  Binder  │→ │  Planner  │→ │   Optimizer   │   │
│  │ (Cypher) │  │          │  │           │  │               │   │
│  └──────────┘  └──────────┘  └───────────┘  └───────────────┘   │
└─────────────────────────────────────────────────────────────────┘
                                ↓
┌─────────────────────────────────────────────────────────────────┐
│                       Graph Runtime Layer                        │
│  ┌─────────────────────────────────────────────────────────┐    │
│  │  WorkingGraph (SimpleGraph)                              │    │
│  │  - Custom lightweight adjacency-list graph               │    │
│  │  - Materialized subgraph for query execution             │    │
│  │  - Algorithms: BFS, DFS, ShortestPaths, PageRank, etc.  │    │
│  └─────────────────────────────────────────────────────────┘    │
│  ┌─────────────────────────────────────────────────────────┐    │
│  │  L0Buffer                                                │    │
│  │  - SimpleGraph for uncommitted mutations                 │    │
│  │  - Merged with storage reads for read-your-writes        │    │
│  │  - Separate property storage (topology-only graph)       │    │
│  └─────────────────────────────────────────────────────────┘    │
└─────────────────────────────────────────────────────────────────┘
                                ↓
┌─────────────────────────────────────────────────────────────────┐
│                      Storage Layer (Custom)                      │
│  ┌────────────┐  ┌────────────┐  ┌────────────┐                 │
│  │   Lance    │  │  Chunked   │  │    LSM     │                 │
│  │  Datasets  │  │    CSR     │  │   Deltas   │                 │
│  └────────────┘  └────────────┘  └────────────┘                 │
│  ┌────────────┐  ┌────────────┐  ┌────────────┐                 │
│  │  Snapshot  │  │   Cache    │  │   Object   │                 │
│  │  Manifest  │  │  Manager   │  │   Store    │                 │
│  └────────────┘  └────────────┘  └────────────┘                 │
└─────────────────────────────────────────────────────────────────┘
```

### Workspace Organization

Uni is organized as a Rust workspace with specialized crates:

| Crate | Purpose |
|-------|---------|
| `uni-common` | Core types (Vid, Eid, UniId, Schema, Snapshots) |
| `uni-store` | Storage layer + runtime (L0Buffer, Writer, StorageManager, WAL) |
| `uni-query` | Query layer (Parser, Planner, Executor) |
| `uni-algo` | Graph algorithms (PageRank, WCC, etc.) |
| `uni` | Main API facade |
| `uni-crdt` | CRDT data types for conflict-free updates |
| `uni-cli` | CLI tools |
| `bindings/uni-db` | PyO3 Python bindings |

### Key External Dependencies

| Crate | Purpose |
|-------|---------|
| `lance` | Columnar storage with versioning |
| `arrow` | Columnar data processing |
| `datafusion` | Relational query engine (WHERE, ORDER BY, aggregations) |
| `object_store` | S3/GCS/local filesystem abstraction |
| `sqlparser` | Base for Cypher parser |
| `roaring` | Bitmap indexes for vertex/edge sets |
| `serde_json` | Document storage (dynamic JSON properties) |
| `pyo3` | Python bindings |
| `candle` | Native Rust embedding generation (HuggingFace Candle) |

---

## Part 2: Data Model

### Property Graph Model

- **Vertices**: Labeled nodes with properties
- **Edges**: Typed, directed relationships between vertices with properties
- **Labels**: Each vertex has exactly one label (single-label model)
- **Types**: Each edge has exactly one type

#### Parallel Edge Policy

**Parallel edges are allowed**: Multiple edges of the same type can exist between the same pair of vertices. Each edge has a unique `eid` and can have different properties.

Example: Two `:KNOWS` edges between Alice and Bob (met at work, met at school).

This matches Cypher semantics where `MATCH (a)-[r:KNOWS]->(b)` returns all matching relationships, not just one.

### Identity Model

#### Dual Identity: VID (Internal) + UID (Content-Addressed)

Uni maintains **two parallel identity systems** to support both efficient storage (VID) and content-addressed lookups (UID):

**Internal VID (64 bits)** - Dense, numeric ID for storage efficiency:

```
vid (64 bits):
┌─────────────────┬────────────────────────────────────────────┐
│ label_id (16)   │ local_offset (48)                          │
└─────────────────┴────────────────────────────────────────────┘
```

- **16 bits for label**: Supports 65,535 vertex labels
- **48 bits for offset**: Supports 281 trillion vertices per label
- **Contiguous per label**: local_offset is dense within each label (0, 1, 2, ...)

**VID Benefits**:
- Edge src_vid/dst_vid implicitly encode source/destination labels
- Label-based filtering extracts from vid bits (no lookup needed)
- Per-label datasets can use local_offset as direct array index
- Adjacency can be partitioned by source label

**External UID** - Content-addressed ID for deduplication and cross-system references:

```rust
/// UniId: 44-character base32 multibase string (SHA3-256)
/// Format: "b" + base32(sha3_256(canonical_content))
/// Example: "bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi"
#[derive(Clone, Copy, Hash, Eq, PartialEq)]
pub struct UniId([u8; 32]); // SHA3-256 raw bytes (32 bytes)

impl UniId {
    pub fn from_multibase(s: &str) -> Result<Self> {
        let (_, bytes) = multibase::decode(s)?;
        Ok(Self(bytes.try_into()?))
    }

    pub fn to_multibase(&self) -> String {
        multibase::encode(Base::Base32Lower, &self.0)
    }
}

/// Extended vertex identity with content-addressed UID
pub struct VertexIdentity {
    /// Internal dense ID (O(1) array indexing, CSR offsets)
    pub vid: Vid,

    /// Content-addressed UID (deterministic, cross-system)
    pub uid: UniId,
}
```

**UID Benefits**:
- Deterministic: Same content always produces same UID
- Cross-system references: Elements can be matched across databases
- Deduplication: Content-addressed IDs prevent duplicates
- CRDT-friendly: Content addressing enables conflict-free merging

**Index Strategy**:

```
vertices/{label}/data/chunk_*.lance:
  ├── _vid (uint64, primary - array index)
  ├── _uid (fixed_binary[32], indexed) ← NEW
  └── ...properties...

indexes/uid_to_vid/{label}/index.lance:
  ├── _uid (fixed_binary[32], sorted)
  └── _vid (uint64)
```

**Lookup Performance**:

| Operation | Performance | Use Case |
|-----------|-------------|----------|
| Lookup by VID | O(1) | Internal storage, adjacency |
| Lookup by ext_id | O(1) hash | User-facing queries |
| Lookup by UID | O(log n) B-tree | Content-addressed lookups, deduplication |

**API Extensions**:

```rust
impl Graph {
    /// Existing: lookup by internal VID
    pub fn get_vertex(&self, vid: Vid) -> Option<Vertex>;

    /// Lookup by content-addressed UID
    pub fn get_vertex_by_uid(&self, uid: &UniId) -> Option<Vertex>;

    /// Batch UID resolution
    pub fn resolve_uids(&self, uids: &[UniId]) -> HashMap<UniId, Vid>;

    /// Insert with content-addressed UID
    pub fn insert_vertex_with_uid(
        &mut self,
        uid: UniId,
        label: &str,
        properties: Properties,
    ) -> Result<Vid>;
}
```

#### Edge ID (eid)

Uses the same encoding scheme:

```
eid (64 bits):
┌─────────────────┬────────────────────────────────────────────┐
│ type_id (16)    │ local_offset (48)                          │
└─────────────────┴────────────────────────────────────────────┘
```

#### External IDs

- Users provide `ext_id` (string) as their primary key
- System maintains `ext_id → vid` index per label (unique per label)
- Queries resolve ext_id to vid at bind time

### Supported Data Types

#### Primitive Types

| Type | Description | Arrow Type |
|------|-------------|------------|
| `string` | UTF-8 text | Utf8 |
| `int32` | 32-bit signed integer | Int32 |
| `int64` | 64-bit signed integer | Int64 |
| `float32` | 32-bit float | Float32 |
| `float64` | 64-bit float | Float64 |
| `bool` | Boolean | Boolean |
| `timestamp` | UTC timestamp | Timestamp(Microsecond, UTC) |
| `date` | Calendar date | Date32 |
| `json` | Arbitrary JSON | Utf8 (serialized) |

#### Vector Types

| Type | Description | Arrow Type |
|------|-------------|------------|
| `vector{N}` | N-dimensional embedding | FixedSizeList(Float32, N) |

#### CRDT Types

Uni supports CRDT data types for conflict-free distributed updates:

| Type | Description | Use Case |
|------|-------------|----------|
| `gcounter` | Grow-only counter | View counts, likes |
| `gset` | Grow-only set | Tag collections |
| `orset` | Observed-remove set | Mutable collections |
| `lww_register` | Last-writer-wins register | Simple values |
| `lww_map` | Last-writer-wins map | Key-value properties |
| `rga` | Replicated Growable Array | Ordered lists, text |
| `vector_clock` | Vector clock | Causal ordering |
| `vc_register` | Vector-clock register | Causally-ordered values |

**CRDT Merge Semantics**:
```rust
// CRDT properties are automatically merged on write conflicts
let schema = SchemaBuilder::new()
    .label("Counter")
    .property("views", DataType::Crdt(CrdtType::GCounter))
    .build();

// Concurrent increments merge correctly
writer.update_vertex("counter1", json!({"views": {"increment": 1}}));
```

### Schema Evolution

The schema is stored in `catalog/schema.json` and versioned with each snapshot.

#### Supported Operations

| Operation | Complexity | Notes |
|-----------|------------|-------|
| Add property (nullable) | Easy | Lance handles nulls, no rewrite needed |
| Add property (with default) | Medium | Backfill on read or async migration |
| Drop property | Easy | Stop reading column, compaction removes |
| Add new label | Easy | Assign next label_id, create new dataset |
| Add new edge type | Easy | Assign next type_id, create new dataset |
| Rename property | Medium | Schema alias, compaction renames column |
| Change property type | Hard | Requires migration, not supported in v1 |
| Drop label/edge type | Hard | Cascading deletes, not supported in v1 |

#### Label/Type ID Registry

**The Problem**: `label_id` and `type_id` are encoded in VIDs/EIDs. They must be globally unique and never reused.

**Solution**: Append-only registry in `schema.json`.

```json
{
  "schema_version": 3,
  "labels": {
    "Person": { "id": 1, "created_at": "2025-01-01T00:00:00Z" },
    "Company": { "id": 2, "created_at": "2025-01-15T00:00:00Z" },
    "Product": { "id": 3, "created_at": "2025-02-01T00:00:00Z" }
  },
  "edge_types": {
    "KNOWS": { "id": 1, "src_labels": ["Person"], "dst_labels": ["Person"] },
    "WORKS_AT": { "id": 2, "src_labels": ["Person"], "dst_labels": ["Company"] }
  },
  "properties": {
    "Person": {
      "name": { "type": "string", "nullable": false, "added_in": 1 },
      "age": { "type": "int32", "nullable": true, "added_in": 1 },
      "email": { "type": "string", "nullable": true, "added_in": 2 }
    }
  }
}
```

**Rules**:
1. IDs are never reused, even after "deletion" (soft-delete only)
2. Schema changes require writer lock (single-writer enforces this)
3. Schema version is included in snapshot manifest
4. Readers use the schema version from their pinned snapshot

#### Adding a New Label

```rust
impl SchemaManager {
    pub async fn add_label(&mut self, name: &str, properties: &[PropertyDef]) -> Result<LabelId> {
        // 1. Acquire writer lock (already held if in transaction)

        // 2. Assign next label_id
        let label_id = self.next_label_id();

        // 3. Update schema.json
        self.schema.labels.insert(name.to_string(), LabelMeta {
            id: label_id,
            created_at: Utc::now(),
        });

        for prop in properties {
            self.schema.properties
                .entry(name.to_string())
                .or_default()
                .insert(prop.name.clone(), prop.clone());
        }

        // 4. Create empty vertex dataset
        self.storage.create_vertex_dataset(name, label_id, properties).await?;

        // 5. Persist schema
        self.persist_schema().await?;

        Ok(label_id)
    }
}
```

#### Property Migration (Async)

For adding properties with defaults or type changes (future):

```rust
pub struct Migration {
    pub id: String,
    pub status: MigrationStatus, // Pending, Running, Completed, Failed
    pub operation: MigrationOp,
    pub progress: f64,           // 0.0 to 1.0
}

pub enum MigrationOp {
    BackfillDefault { label: String, property: String, default: Value },
    RenameProperty { label: String, old_name: String, new_name: String },
}

// Migrations run in background, don't block reads/writes
// Old snapshots continue using old schema
// New snapshots see migrated data
```

#### Soft-Drop for Schema Evolution

**Problem**: Hard-deleting labels, edge types, or properties causes issues:
- Old snapshots become unreadable (column doesn't exist)
- Concurrent readers see schema/data mismatch
- Rollback impossible once data deleted
- ID reuse creates phantom data bugs

**Solution**: Soft-drop with three phases: deprecated → hidden → tombstone.

```rust
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum SchemaElementState {
    /// Active - fully usable
    Active,

    /// Deprecated - readable but writes warn/fail
    /// Gives users time to migrate
    Deprecated {
        since: DateTime<Utc>,
        reason: String,
        migration_hint: Option<String>,
    },

    /// Hidden - invisible to new queries, still stored
    /// Old snapshots can still read
    Hidden {
        since: DateTime<Utc>,
        last_active_snapshot: SnapshotId,
    },

    /// Tombstone - no data remains, ID reserved forever
    /// Compaction has removed all data
    Tombstone {
        since: DateTime<Utc>,
    },
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct LabelMeta {
    pub id: LabelId,
    pub created_at: DateTime<Utc>,
    pub state: SchemaElementState,  // NEW
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PropertyMeta {
    pub r#type: DataType,
    pub nullable: bool,
    pub added_in: SchemaVersion,
    pub state: SchemaElementState,  // NEW
}
```

**Schema JSON with soft-drop states**:

```json
{
  "schema_version": 5,
  "labels": {
    "Person": { "id": 1, "state": "Active" },
    "LegacyUser": {
      "id": 2,
      "state": {
        "Deprecated": {
          "since": "2025-03-01T00:00:00Z",
          "reason": "Use Person instead",
          "migration_hint": "MATCH (u:LegacyUser) CREATE (p:Person) SET p = u"
        }
      }
    },
    "TempTable": {
      "id": 3,
      "state": {
        "Hidden": {
          "since": "2025-02-15T00:00:00Z",
          "last_active_snapshot": "snap_00042"
        }
      }
    },
    "DeletedType": {
      "id": 4,
      "state": { "Tombstone": { "since": "2025-01-01T00:00:00Z" } }
    }
  },
  "properties": {
    "Person": {
      "name": { "type": "string", "nullable": false, "added_in": 1, "state": "Active" },
      "ssn": {
        "type": "string",
        "nullable": true,
        "added_in": 1,
        "state": {
          "Hidden": {
            "since": "2025-03-01T00:00:00Z",
            "last_active_snapshot": "snap_00050"
          }
        }
      }
    }
  }
}
```

**Soft-drop API**:

```rust
impl SchemaManager {
    /// Mark a property as deprecated (phase 1)
    /// Writers will receive warnings, readers unaffected
    pub async fn deprecate_property(
        &mut self,
        label: &str,
        property: &str,
        reason: &str,
        migration_hint: Option<&str>,
    ) -> Result<()> {
        let prop = self.schema.properties
            .get_mut(label)
            .and_then(|p| p.get_mut(property))
            .ok_or(Error::PropertyNotFound)?;

        prop.state = SchemaElementState::Deprecated {
            since: Utc::now(),
            reason: reason.to_string(),
            migration_hint: migration_hint.map(String::from),
        };

        self.persist_schema().await
    }

    /// Hide a property (phase 2)
    /// New queries won't see it, old snapshots still can
    pub async fn hide_property(
        &mut self,
        label: &str,
        property: &str,
    ) -> Result<()> {
        let prop = self.schema.properties
            .get_mut(label)
            .and_then(|p| p.get_mut(property))
            .ok_or(Error::PropertyNotFound)?;

        // Must be deprecated first
        if !matches!(prop.state, SchemaElementState::Deprecated { .. }) {
            return Err(Error::MustDeprecateFirst);
        }

        let current_snapshot = self.current_snapshot_id();
        prop.state = SchemaElementState::Hidden {
            since: Utc::now(),
            last_active_snapshot: current_snapshot,
        };

        self.persist_schema().await
    }

    /// Soft-drop a label (marks for future removal)
    pub async fn soft_drop_label(&mut self, label: &str) -> Result<()> {
        let label_meta = self.schema.labels
            .get_mut(label)
            .ok_or(Error::LabelNotFound)?;

        match &label_meta.state {
            SchemaElementState::Active => {
                // Go directly to deprecated
                label_meta.state = SchemaElementState::Deprecated {
                    since: Utc::now(),
                    reason: "Marked for removal".to_string(),
                    migration_hint: None,
                };
            }
            SchemaElementState::Deprecated { .. } => {
                // Advance to hidden
                label_meta.state = SchemaElementState::Hidden {
                    since: Utc::now(),
                    last_active_snapshot: self.current_snapshot_id(),
                };
            }
            SchemaElementState::Hidden { .. } => {
                // Already hidden, compaction will tombstone when data gone
                return Ok(());
            }
            SchemaElementState::Tombstone { .. } => {
                // Already gone
                return Ok(());
            }
        }

        self.persist_schema().await
    }
}
```

**Query behavior by state**:

| State | Read (current) | Read (old snapshot) | Write | Schema introspection |
|-------|----------------|---------------------|-------|---------------------|
| Active | ✅ Normal | ✅ Normal | ✅ Normal | ✅ Visible |
| Deprecated | ✅ Normal | ✅ Normal | ⚠️ Warning | ⚠️ Marked deprecated |
| Hidden | ❌ Not found | ✅ If in range | ❌ Error | ❌ Not visible |
| Tombstone | ❌ Not found | ❌ Not found | ❌ Error | ❌ Not visible |

**Compaction and garbage collection**:

```rust
impl Compactor {
    pub async fn compact_with_soft_drops(&mut self) -> Result<()> {
        // 1. Get hidden elements
        let hidden_props = self.schema.get_hidden_properties();
        let hidden_labels = self.schema.get_hidden_labels();

        // 2. Find snapshots that still need hidden data
        let retained_snapshots = self.get_retained_snapshots();
        let oldest_retained = retained_snapshots.iter().min();

        // 3. For each hidden element, check if still needed
        for (label, prop, meta) in hidden_props {
            if let SchemaElementState::Hidden { last_active_snapshot, .. } = &meta.state {
                // If oldest retained snapshot is newer than last_active, safe to delete
                if oldest_retained > Some(last_active_snapshot) {
                    // Drop column from Lance dataset
                    self.drop_column(label, prop).await?;

                    // Mark as tombstone in schema
                    self.schema_manager.tombstone_property(label, prop).await?;
                }
            }
        }

        // 4. Same for labels - drop entire dataset when no retained snapshot needs it
        for (label, meta) in hidden_labels {
            if let SchemaElementState::Hidden { last_active_snapshot, .. } = &meta.state {
                if oldest_retained > Some(last_active_snapshot) {
                    self.drop_label_dataset(label).await?;
                    self.schema_manager.tombstone_label(label).await?;
                }
            }
        }

        Ok(())
    }
}
```

**Backwards compatibility guarantees**:

1. **Old binaries**: Can read snapshots created with newer schema (unknown fields ignored)
2. **Rollback**: Soft-dropped data persists until retention policy clears it
3. **ID safety**: Tombstoned IDs never reused, preventing phantom data
4. **Gradual migration**: Deprecated phase gives time to update queries

**Configuration**:

```rust
pub struct SoftDropConfig {
    /// Minimum time in Deprecated state before hiding
    pub min_deprecation_period: Duration,  // default: 7 days

    /// Snapshot retention (hidden data survives this long)
    pub snapshot_retention: Duration,  // default: 30 days

    /// Auto-advance from Deprecated to Hidden
    pub auto_hide_after: Option<Duration>,  // default: None (manual)
}
```
---

## Part 3: Graph Runtime (SimpleGraph)

### Core Types

**Note**: The design originally planned gryf integration, but the implementation uses a custom `SimpleGraph` structure for simplicity, fewer dependencies, and faster compilation.

**CRITICAL: Topology-only graph structures** to maximize subgraph size in memory.

```rust
/// Custom lightweight adjacency-list graph structure
/// Replaces gryf for simpler, faster implementation
pub struct SimpleGraph {
    /// Vertex existence tracking
    vertices: FxHashMap<Vid, ()>,
    /// Outgoing adjacency lists: vid -> [(dst_vid, eid, edge_type)]
    outgoing: FxHashMap<Vid, Vec<EdgeEntry>>,
    /// Incoming adjacency lists: vid -> [(src_vid, eid, edge_type)]
    incoming: FxHashMap<Vid, Vec<EdgeEntry>>,
    /// Edge lookup by eid
    edge_map: FxHashMap<Eid, EdgeEntry>,
}

/// Edge entry in adjacency list
#[derive(Clone, Debug)]
pub struct EdgeEntry {
    pub eid: Eid,
    pub src_vid: Vid,
    pub dst_vid: Vid,
    pub edge_type: u16,
}

/// WorkingGraph and L0Buffer both use SimpleGraph
pub type WorkingGraph = SimpleGraph;
```

**Key operations**:
- `O(1)` vertex lookup via HashMap
- `O(degree)` neighbor iteration via adjacency lists
- `O(1)` edge lookup via edge_map
- Safe deletion without panics on missing edges

**Memory footprint comparison** (100K vertices, 1M edges):

| Design | Vertex Size | Edge Size | Total |
|--------|-------------|-----------|-------|
| With properties | ~500 bytes | ~200 bytes | ~250 MB |
| **Topology-only** | ~32 bytes | ~40 bytes | **~45 MB** |

This significant reduction allows processing much larger subgraphs in memory.

**Why directed?** Edges in the property graph model are directed (src→dst). SimpleGraph maintains separate outgoing/incoming lists:
- `graph.get_neighbors(vid, Direction::Outgoing)` returns only outgoing edges
- `graph.get_neighbors(vid, Direction::Incoming)` returns only incoming edges
- Pattern matching respects edge direction (`(a)-[:KNOWS]->(b)` vs `(a)<-[:KNOWS]-(b)`)

### PropertyManager (Lazy Property Access)

Properties are NOT stored in the graph. They're fetched on-demand via a separate manager:

```rust
/// Manages lazy property access with LRU caching
pub struct PropertyManager {
    /// Page cache for vertex properties
    vertex_cache: LruCache<(Vid, String), Value>,

    /// Page cache for edge properties
    edge_cache: LruCache<(Eid, String), Value>,

    /// Storage backend
    storage: Arc<StorageManager>,

    /// Current snapshot for reads
    snapshot: Snapshot,
}

impl PropertyManager {
    /// Get a single vertex property (cached)
    pub async fn get_vertex_prop(&mut self, vid: Vid, prop: &str) -> Result<Value> {
        let key = (vid, prop.to_string());
        if let Some(val) = self.vertex_cache.get(&key) {
            return Ok(val.clone());
        }

        // Cache miss - fetch from Lance
        let val = self.storage.fetch_vertex_property(vid, prop, &self.snapshot).await?;
        self.vertex_cache.put(key, val.clone());
        Ok(val)
    }

    /// Batch fetch properties (for result materialization)
    pub async fn batch_get_vertex_props(
        &mut self,
        vids: &[Vid],
        props: &[&str],
    ) -> Result<Vec<HashMap<String, Value>>> {
        // Batch read from Lance - single I/O for all
        self.storage.batch_fetch_vertex_properties(vids, props, &self.snapshot).await
    }

    /// Get edge property
    pub async fn get_edge_prop(&mut self, eid: Eid, prop: &str) -> Result<Value> {
        // Similar caching logic
        // ...
    }
}
```

**Usage in query execution**:

```rust
impl QueryExecutor {
    pub async fn execute(&self, plan: &Plan) -> Result<Vec<Row>> {
        // 1. Load topology-only subgraph (fast, small)
        let graph = self.storage.load_subgraph(...).await?;

        // 2. Run algorithm on topology (BFS, pattern match, etc.)
        let matched_vids = self.match_pattern(&graph, plan)?;

        // 3. ONLY NOW fetch properties for results
        let props = self.property_manager
            .batch_get_vertex_props(&matched_vids, plan.return_props())
            .await?;

        // 4. Build result rows
        Ok(self.build_rows(matched_vids, props))
    }
}
```

**Key insight**: Graph algorithms (BFS, ShortestPath, PageRank) only need topology. Properties are only needed for:
- Filter predicates (can use `get_neighbors_with_properties` for edge filters)
- Result projection (fetch at the end, not during traversal)

### Graph Loading Pattern

```rust
impl StorageManager {
    /// Load k-hop neighborhood into a WorkingGraph
    ///
    /// direction: Outgoing, Incoming, or Both - controls which adjacency to fetch
    pub async fn load_subgraph(
        &self,
        start_vids: &[Vid],
        edge_types: &[EdgeType],
        max_hops: usize,
        direction: Direction,
    ) -> Result<WorkingGraph> {
        let mut graph = WorkingGraph::new_directed();
        let mut vid_to_handle: HashMap<Vid, VertexId> = HashMap::new();
        let mut frontier: HashSet<Vid> = start_vids.iter().copied().collect();

        for _hop in 0..max_hops {
            // Fetch adjacency chunks for frontier vertices
            let chunks = self.fetch_adjacency_chunks(&frontier, edge_types, direction).await?;

            let mut next_frontier = HashSet::new();

            for (src_vid, dst_vid, eid, edge_type) in chunks.iter_edges() {
                // Add source vertex if not present
                let src_handle = *vid_to_handle
                    .entry(src_vid)
                    .or_insert_with(|| graph.add_vertex(VertexData::new(src_vid)));

                // Add destination vertex
                let dst_handle = *vid_to_handle
                    .entry(dst_vid)
                    .or_insert_with(|| {
                        next_frontier.insert(dst_vid);
                        graph.add_vertex(VertexData::new(dst_vid))
                    });

                // Add edge. IMPORTANT: WorkingGraph is directed.
                // If we fetched INCOMING edges (bwd adjacency), the chunks return (src, dst)
                // such that the edge is src->dst. The 'src_vid' in the loop is the neighbor,
                // and 'dst_vid' is the frontier node.
                // We always add the edge in its canonical direction: src -> dst.
                graph.add_edge(src_handle, dst_handle, EdgeData::new(eid, edge_type));
            }

            frontier = next_frontier;
            if frontier.is_empty() {
                break;
            }
        }

        Ok(graph)
    }
}
```

### Algorithm Execution

Once a subgraph is loaded into SimpleGraph, execute algorithms using the uni-algo crate:

```rust
use uni_algo::{bfs, dfs, shortest_path};

impl QueryExecutor {
    /// Find shortest path between two vertices
    pub fn shortest_path(
        &self,
        graph: &WorkingGraph,
        start: Vid,
        goal: Vid,
    ) -> Option<Vec<Vid>> {
        shortest_path::dijkstra(graph, start, goal)
    }

    /// BFS traversal with visitor pattern
    pub fn bfs_traverse<F>(
        &self,
        graph: &WorkingGraph,
        start: Vid,
        mut visitor: F,
    ) where
        F: FnMut(Vid),
    {
        for vid in bfs::traverse(graph, start) {
            visitor(vid);
        }
    }

    /// Check for cycles (useful for validation)
    pub fn has_cycle(&self, graph: &WorkingGraph) -> bool {
        dfs::has_cycle(graph)
    }
}
```

### L0 Delta Buffer

The L0 buffer uses SimpleGraph for in-memory mutation tracking:

```rust
/// Tombstone entry with full edge endpoints for L1 searchability
/// CRITICAL: Without src_vid/dst_vid, tombstones can't be found by find_by_key(vid)
#[derive(Clone, Debug)]
pub struct TombstoneEntry {
    pub eid: Eid,
    pub src_vid: Vid,
    pub dst_vid: Vid,
    pub edge_type: EdgeType,
}

pub struct L0Buffer {
    /// In-memory graph of uncommitted changes (TOPOLOGY ONLY)
    pub graph: SimpleGraph,

    /// Tombstones with full endpoints for L1 searchability
    /// MUST include (src_vid, dst_vid, edge_type) so tombstones appear in
    /// both FWD runs (keyed by src_vid) and BWD runs (keyed by dst_vid)
    tombstones: HashMap<Eid, TombstoneEntry>,

    /// Version tracking per edge for conflict resolution
    edge_versions: HashMap<Eid, Version>,

    /// Edge properties stored SEPARATELY from graph (topology-only contract)
    /// Properties are written to L1 during flush, not held in EdgeEntry
    edge_properties: HashMap<Eid, Properties>,

    /// Vertex properties stored separately (topology-only contract)
    vertex_properties: HashMap<Vid, Properties>,

    /// Edge endpoints cache for delete operations
    /// Populated on insert, used when delete needs to write tombstone
    edge_endpoints: HashMap<Eid, (Vid, Vid, EdgeType)>,

    /// Global version counter (monotonically increasing, persisted in snapshot)
    /// CRITICAL: Must be loaded from snapshot on writer open, never reset to 0
    current_version: Version,

    /// WAL for durability
    wal: WriteAheadLog,

    /// Current mutation count
    mutation_count: usize,

    /// Flush threshold
    flush_threshold: usize, // default: 100_000
}

impl L0Buffer {
    pub fn insert_edge(
        &mut self,
        src_vid: Vid,
        dst_vid: Vid,
        edge_type: EdgeType,
        eid: Eid,
        properties: Properties,
    ) -> Result<()> {
        self.current_version += 1;
        let version = self.current_version;

        // Write to WAL first (includes properties for durability)
        self.wal.append(Mutation::InsertEdge {
            src_vid, dst_vid, edge_type, eid, version, properties: properties.clone()
        })?;

        // Add to SimpleGraph (TOPOLOGY ONLY - no properties in EdgeEntry)
        let src = self.get_or_create_vertex(src_vid);
        let dst = self.get_or_create_vertex(dst_vid);

        // EdgeData is topology-only: just eid + edge_type (16 bytes)
        self.delta.add_edge(src, dst, EdgeData { eid, edge_type });

        // Store properties separately (not in graph)
        self.edge_properties.insert(eid, properties);
        self.edge_versions.insert(eid, version);

        // Cache endpoints for delete operations
        self.edge_endpoints.insert(eid, (src_vid, dst_vid, edge_type));

        self.tombstones.remove(&eid); // Un-delete if previously tombstoned
        self.mutation_count += 1;

        if self.mutation_count >= self.flush_threshold {
            self.flush_to_l1()?;
        }

        Ok(())
    }

    /// Delete an edge by eid
    /// REQUIRES: Either edge was inserted in this session (endpoints cached),
    /// or caller provides endpoints via delete_edge_with_endpoints()
    pub fn delete_edge(&mut self, eid: Eid) -> Result<()> {
        // Try to get endpoints from cache (edge was inserted in this session)
        let (src_vid, dst_vid, edge_type) = self.edge_endpoints.get(&eid)
            .copied()
            .or_else(|| self.lookup_endpoints_from_graph(eid))
            .ok_or_else(|| Error::DeleteRequiresEndpoints { eid })?;

        self.delete_edge_with_endpoints(eid, src_vid, dst_vid, edge_type)
    }

    /// Delete an edge with explicit endpoints
    /// Use when deleting an edge that wasn't inserted in this session
    pub fn delete_edge_with_endpoints(
        &mut self,
        eid: Eid,
        src_vid: Vid,
        dst_vid: Vid,
        edge_type: EdgeType,
    ) -> Result<()> {
        self.current_version += 1;
        let version = self.current_version;

        self.wal.append(Mutation::DeleteEdge {
            eid, src_vid, dst_vid, edge_type, version
        })?;

        self.tombstones.insert(eid, TombstoneEntry {
            eid, src_vid, dst_vid, edge_type
        });
        self.edge_versions.insert(eid, version);
        Ok(())
    }

    /// Query L0 for neighbors with version info
    pub fn get_neighbors(
        &self,
        vid: Vid,
        edge_type: EdgeType,
        direction: Direction,
    ) -> Vec<(Vid, Eid, Version)> {
        let Some(&handle) = self.vid_index.get(&vid) else {
            return vec![];
        };

        let mut neighbors = Vec::new();

        // Helper to process edges from iterator
        // Implementation uses SimpleGraph's get_neighbors method which handles
        // both outgoing and incoming directions uniformly

        if matches!(direction, Direction::Outgoing | Direction::Both) {
             self.delta.neighbors_outgoing(handle)
                .filter(|(_, data)| data.edge_type == edge_type)
                .for_each(|(h, data)| {
                     let neighbor = self.delta.vertex(h).vid;
                     let ver = self.edge_versions.get(&data.eid).copied().unwrap_or(0);
                     neighbors.push((neighbor, data.eid, ver));
                });
        }

        if matches!(direction, Direction::Incoming | Direction::Both) {
             self.delta.neighbors_incoming(handle)
                .filter(|(_, data)| data.edge_type == edge_type)
                .for_each(|(h, data)| {
                     let neighbor = self.delta.vertex(h).vid;
                     let ver = self.edge_versions.get(&data.eid).copied().unwrap_or(0);
                     neighbors.push((neighbor, data.eid, ver));
                });
        }

        neighbors
    }

    pub fn is_tombstoned(&self, eid: Eid) -> bool {
        self.tombstones.contains(&eid)
    }

    /// Get edge properties from L0 (stored separately from graph)
    pub fn get_edge_properties(&self, eid: Eid) -> Option<&Properties> {
        self.edge_properties.get(&eid)
    }

    /// Get vertex properties from L0 (stored separately from graph)
    pub fn get_vertex_properties(&self, vid: Vid) -> Option<&Properties> {
        self.vertex_properties.get(&vid)
    }

    /// Flush L0 to L1 - writes both topology and properties to Lance
    pub async fn flush_to_l1(&mut self) -> Result<()> {
        // Collect all edges with their properties for writing
        let mut edges_to_flush = Vec::new();
        for edge_handle in self.delta.edges() {
            let edge_data = self.delta.edge(edge_handle);
            let src_vid = self.delta.vertex(self.delta.endpoints(edge_handle).0).vid;
            let dst_vid = self.delta.vertex(self.delta.endpoints(edge_handle).1).vid;
            let properties = self.edge_properties.get(&edge_data.eid).cloned()
                .unwrap_or_default();
            let version = self.edge_versions.get(&edge_data.eid).copied().unwrap_or(0);

            edges_to_flush.push(L1Entry {
                src_vid,
                dst_vid,
                eid: edge_data.eid,
                edge_type: edge_data.edge_type,
                op: Op::INSERT,
                version,
                properties,
            });
        }

        // Add tombstones WITH FULL ENDPOINTS
        // CRITICAL: Tombstones must have src_vid/dst_vid so they appear in
        // both FWD runs (searchable by src_vid) and BWD runs (searchable by dst_vid)
        for (eid, tombstone) in &self.tombstones {
            let version = self.edge_versions.get(eid).copied().unwrap_or(0);
            edges_to_flush.push(L1Entry {
                src_vid: tombstone.src_vid,
                dst_vid: tombstone.dst_vid,
                eid: tombstone.eid,
                edge_type: tombstone.edge_type,
                op: Op::DELETE,
                version,
                properties: Properties::default(),
            });
        }

        // Write dual runs (FWD sorted by src_vid, BWD sorted by dst_vid)
        // Tombstones now appear in correct position for both directions
        self.write_fwd_run(&edges_to_flush).await?;
        self.write_bwd_run(&edges_to_flush).await?;

        // Clear L0 state
        self.delta = DeltaGraph::new();
        self.vid_index.clear();
        self.tombstones.clear();
        self.edge_versions.clear();
        self.edge_properties.clear();
        self.vertex_properties.clear();
        self.edge_endpoints.clear();
        self.mutation_count = 0;

        Ok(())
    }
}
```

### Global Version Counter (Persisted)

**Problem**: The `current_version` counter determines merge conflict resolution. If it resets to 0 after restart, two edges can have the same version, causing incorrect merges.

**Solution**: Persist `current_version` in the snapshot manifest and load it on writer open.

```rust
/// Snapshot manifest includes version high-water mark
pub struct SnapshotManifest {
    // ... other fields ...

    /// Highest version number used in this snapshot
    /// Writer MUST start from version_high_water_mark + 1
    pub version_high_water_mark: Version,
}

impl Writer {
    pub async fn open(db: &Database) -> Result<Self> {
        // 1. Load latest snapshot
        let snapshot = db.load_latest_snapshot().await?;

        // 2. Initialize L0 buffer with version from snapshot
        let l0_buffer = L0Buffer {
            // CRITICAL: Continue from persisted version, never reset to 0
            current_version: snapshot.manifest.version_high_water_mark,
            // ... other fields ...
        };

        Ok(Self { l0_buffer, snapshot, .. })
    }
}

impl L0Buffer {
    pub fn flush_to_l1(&mut self) -> Result<FlushResult> {
        // ... write runs ...

        // Return high-water mark for snapshot manifest
        Ok(FlushResult {
            fwd_run: fwd_run_path,
            bwd_run: bwd_run_path,
            version_high_water_mark: self.current_version, // Persist this!
        })
    }
}

impl Writer {
    pub async fn commit(&mut self) -> Result<SnapshotId> {
        let flush_result = self.l0_buffer.flush_to_l1()?;

        // Include version in manifest
        let manifest = SnapshotManifest {
            version_high_water_mark: flush_result.version_high_water_mark,
            wal_high_water_mark: self.wal.current_lsn(),
            // ... other fields ...
        };

        self.cas_manifest(&manifest).await
    }
}
```

**Merge Correctness**:

| Scenario | Behavior |
|----------|----------|
| Version A > Version B | A wins, B discarded |
| Version A == Version B | Impossible if versions are globally unique |
| Restart without commit | Version continues from last committed snapshot |
| Crash during flush | WAL replay, versions preserved |

**Invariant**: `current_version` is strictly monotonically increasing across all writer sessions for a given graph. This is enforced by loading from snapshot and never resetting.

---

## Part 4: Storage Layout

### Directory Structure

```
{graph_name}/
├── catalog/
│   ├── manifests/
│   │   ├── {snapshot_id}.json
│   │   └── ...
│   ├── latest                    # Pointer to current snapshot
│   └── schema.json               # Label/type definitions
├── vertices_{label}/
│   ├── _versions/
│   ├── data/
│   │   └── *.lance
│   └── _indices/
│       └── ext_id_hash/
├── edges_{type}_{src_label}/     # e.g., edges_knows_person/ - clustered by src_vid
│   ├── chunk_{offset}.lance      # Same chunking as adjacency for merge join
│   └── ...
├── adj_fwd_{type}_{src_label}/   # e.g., adj_fwd_knows_person/
│   ├── chunk_{offset}.lance
│   └── ...
├── adj_bwd_{type}_{dst_label}/   # e.g., adj_bwd_knows_person/
│   └── ...
└── delta_{type}_{label}/         # e.g., delta_knows_person/
    ├── run_{timestamp}.lance     # Sorted runs for this (type, label) partition
    └── ...
```

### Vertex Datasets

Per-label Lance datasets with local_offset as implicit row index.

**Schema** (`vertices_person`):

| Column | Type | Notes |
|--------|------|-------|
| **Core Identity** |  |  |
| local_offset | uint64 | Stable per-label ID, dense, used to derive vid |
| ext_id | string | User-provided key, unique per label, indexed |
| _deleted | bool | Soft delete tombstone |
| _version | uint64 | Write sequence number for conflict resolution |
| **System Columns** |  |  |
| _uid | fixed_binary[32] | Content-addressed UID (SHA3-256), indexed |
| _valid_from | timestamp[us, tz=UTC] | Validity interval start (bi-temporal) |
| _valid_to | timestamp[us, tz=UTC] | Validity interval end (nullable, NULL = current) |
| _vector_clock | binary | Serialized vector clock for CRDT conflict resolution |
| **User Properties** |  |  |
| name | string | Property |
| age | int32 | Property |
| city | string | Property |
| **Vector Properties** |  |  |
| _vectors_embedding | list\<float32\> | Primary element embedding |
| _vectors_encoding_space | string | Encoding model identifier |

**Key decisions**:
- No `vid` column stored - derived from `(label_id, local_offset)`
- `local_offset` is stored explicitly and preserved across compaction
- `_deleted` flag enables soft deletes without rewriting
- `_version` enables conflict resolution during compaction
- System columns are nullable for backward compatibility

### Edge Datasets

Per-type Lance datasets storing edge properties. **CRITICAL: Clustered by src_vid** to enable merge joins with adjacency and avoid random I/O on object storage.

**File organization** (mirrors adjacency partitioning):
```
edges_{type}_{src_label}/
  ├── chunk_0000000000.lance   # src local_offset 0..262143
  ├── chunk_0000262144.lance   # src local_offset 262144..524287
  └── ...
```

**Schema** (`edges_knows_person/chunk_*.lance`):

| Column | Type | Notes |
|--------|------|-------|
| eid | uint64 | Global edge ID (type_id << 48 \| offset) |
| src_vid | uint64 | Source vertex - **SORT KEY** |
| dst_vid | uint64 | Destination vertex (label encoded) |
| _deleted | bool | Soft delete tombstone |
| _version | uint64 | Write sequence number |
| since | date | Property |
| weight | float32 | Property |

**Why cluster by src_vid?** (Avoiding the Pointer Chasing Performance Cliff)

The naive design stores edges sorted by `eid`. This causes **random I/O disaster**:
```
Query: MATCH (a)-[e:KNOWS]->(b) WHERE e.weight > 0.5

Naive approach:
1. Fetch adjacency chunk → get 100 eids
2. For each eid, lookup in edges dataset → 100 RANDOM reads
3. On S3: 100 × 100ms = 10 SECONDS (!!!)
```

With src_vid clustering, edges are **colocated with adjacency**:
```
Optimized approach:
1. Fetch adjacency chunk (src range 0..262143) → get 100 eids
2. Fetch edge property chunk (SAME src range) → 1 SEQUENTIAL read
3. Merge join in memory (both sorted by src_vid)
4. On S3: 2 × 100ms = 200ms ✓
```

**Merge Join** (see Read Path for implementation):
- Adjacency chunk and edge property chunk cover the same src_vid range
- Both are sorted by src_vid within the chunk
- Single-pass merge join to attach properties to edges

### Adjacency Datasets (Chunked CSR)

Self-contained chunks for fast neighbor access. Partitioned by **(edge_type, source_label)** to avoid local_offset collisions across labels.

**File organization**:
```
adj_fwd_{type}_{src_label}/
  ├── chunk_0000000000.lance   # src local_offset 0..262143
  ├── chunk_0000262144.lance   # src local_offset 262144..524287
  └── ...
```

Example: `adj_fwd_knows_person/` contains forward adjacency for KNOWS edges originating from Person vertices.

**Schema** (row-per-vertex format for Lance compatibility):

| Column | Type | Notes |
|--------|------|-------|
| src_vid | uint64 | Full source vertex ID (includes label) |
| neighbors | list\<uint64\> | Destination vids (full VIDs) |
| edge_ids | list\<uint64\> | Corresponding eids |
| **edge_weights** | list\<float32\> | **Denormalized** - hot property |
| **edge_timestamps** | list\<int64\> | **Denormalized** - hot property |

#### Denormalized Edge Properties (Critical for PB Scale)

**The Problem**: Separating topology (adjacency) from edge properties causes random I/O disaster at scale.

```
Scenario: MATCH (a)-[e:FOLLOWS]->(b) WHERE e.weight > 0.5

Without denormalization:
1. Fetch adjacency chunk → 1000 edges, 1000 eids
2. For each eid, fetch property from edges_follows dataset
   → 1000 random reads across S3 → ~100 seconds (!!)

With denormalization:
1. Fetch adjacency chunk → 1000 edges with weights inline
2. Filter in memory → 0.1ms
```

**Strategy**: Duplicate frequently-filtered properties ("hot properties") directly in adjacency chunks.

**Hot Property Selection**:

| Property Type | Denormalize? | Rationale |
|--------------|--------------|-----------|
| weight/score | ✅ Yes | Most common filter predicate |
| timestamp/created_at | ✅ Yes | Range queries, temporal filters |
| type/category (enum) | ✅ Yes | Small cardinality, common filter |
| description (text) | ❌ No | Large, rarely filtered |
| embedding (vector) | ❌ No | Use vector index instead |

**Schema Configuration**:

```rust
/// Edge type schema with denormalization hints
pub struct EdgeTypeSchema {
    pub name: String,
    pub properties: Vec<PropertyDef>,
    /// Properties to copy into adjacency chunks for fast filtering
    pub denormalized_properties: Vec<String>,
}

// Example: FOLLOWS edges denormalize weight and created_at
EdgeTypeSchema {
    name: "FOLLOWS",
    properties: vec![
        PropertyDef::new("weight", Type::Float32),
        PropertyDef::new("created_at", Type::Timestamp),
        PropertyDef::new("note", Type::String),  // Not denormalized
    ],
    denormalized_properties: vec!["weight", "created_at"],
}
```

**Trade-offs**:

| Aspect | Without Denorm | With Denorm |
|--------|----------------|-------------|
| Adjacency chunk size | ~24 bytes/edge | ~36 bytes/edge (+50%) |
| Filter query (1000 edges) | 100s (S3 random) | 0.1ms (in-memory) |
| Storage cost | 1x | ~1.5x for hot properties |
| Write amplification | 1x | 2x (adjacency + edge dataset) |

**When NOT to denormalize**:
- Property rarely used in WHERE clauses
- Property is large (>100 bytes)
- Property changes frequently (high write amplification)

**Denormalized Property Updates (L0/L1 Override)**:

When an edge's denormalized property is updated, L2 adjacency chunks become stale until major compaction. The read path must apply L0/L1 deltas to get current values:

```rust
impl StorageManager {
    /// Get neighbors with denormalized properties, applying L0/L1 overrides
    /// - l0_buffer: Some(&L0Buffer) for writer context, None for reader context
    /// - ReadSession always passes None to ensure snapshot isolation
    pub async fn get_neighbors_with_denorm_props(
        &self,
        vid: Vid,
        edge_type: EdgeType,
        direction: Direction,
        snapshot: &Snapshot,
        l0_buffer: Option<&L0Buffer>,  // None for readers, Some for writer
    ) -> Result<Vec<NeighborWithProps>> {
        // 1. Fetch L2 adjacency chunk (includes denormalized weight, timestamp)
        let l2_neighbors = self.fetch_adjacency_chunk(vid, edge_type, direction).await?;

        // 2. Build map from eid -> (dst_vid, weight, timestamp)
        let mut result: HashMap<Eid, NeighborWithProps> = l2_neighbors
            .into_iter()
            .map(|n| (n.eid, n))
            .collect();

        // 3. Apply L1 delta runs (may have updated properties)
        for run in self.get_l1_runs(edge_type, snapshot, direction) {
            for entry in run.find_by_key(vid) {
                if entry.op == Op::DELETE {
                    result.remove(&entry.eid);
                } else {
                    // L1 INSERT overrides L2 denormalized values
                    result.insert(entry.eid, NeighborWithProps {
                        dst_vid: entry.dst_vid,
                        eid: entry.eid,
                        weight: entry.properties.get("weight").cloned(),
                        timestamp: entry.properties.get("created_at").cloned(),
                    });
                }
            }
        }

        // 4. Apply L0 buffer (writer context ONLY)
        // NOTE: ReadSession passes None here to ensure snapshot isolation
        if let Some(l0) = l0_buffer {
            for (dst_vid, eid, _version) in l0.get_neighbors(vid, edge_type, direction) {
                if l0.is_tombstoned(eid) {
                    result.remove(&eid);
                } else if let Some(props) = l0.get_edge_properties(eid) {
                    result.insert(eid, NeighborWithProps {
                        dst_vid,
                        eid,
                        weight: props.get("weight").cloned(),
                        timestamp: props.get("created_at").cloned(),
                    });
                }
            }
        }

        Ok(result.into_values().collect())
    }
}
```

**Key Insight**: Denormalized properties in L2 are a **cache**, not the source of truth. L0/L1 always have the current values. Major compaction rebuilds L2 with latest values.

**Filter Optimization**: If a filter uses a denormalized property (e.g., `WHERE e.weight > 0.5`):
1. Check L0 first (in-memory, always current)
2. Check L1 runs (apply to L2 results)
3. L2 values are valid only for edges NOT in L0/L1

**Why partition by (type, src_label)?**
- Avoids local_offset collisions: Person:5 and Company:5 are in different files
- Chunk lookup: `chunk_id = local_offset / CHUNK_SIZE` is unambiguous within partition
- Enables label-based pruning at storage level

**Why store full src_vid?**
- Self-describing rows: no need to derive VID from file path + row position
- Simplifies merge logic: L0/L1/L2 all use same VID format

**Backward adjacency** (`adj_bwd_{type}_{dst_label}/`) uses the same structure, partitioned by destination label.

### Delta Datasets (LSM-Style)

Three-level hierarchy for efficient writes:

```
Level 0 (L0): SimpleGraph (Memory)
  - In-memory SimpleGraph for mutations
  - Backed by WAL for crash recovery
  - Capacity: ~100K edges
  - Flushed to L1 on threshold or commit

Level 1 (L1): Sorted runs on disk (DUAL: FWD + BWD)
  - Small Lance files with **two copies** per flush:
    - FWD run: sorted by src_vid (for outgoing lookups)
    - BWD run: sorted by dst_vid (for incoming lookups)
  - Each run pair: ~100K-1M edges
  - Multiple run pairs exist concurrently
  - 2x storage overhead, but O(log n) lookups in both directions

Level 2 (L2): Base adjacency (Chunked CSR)
  - Per-vertex rows as described above
  - Rebuilt during major compaction
```

**L1 Schema (Dual Runs)**:

FWD run (`delta_{type}_fwd/run_{timestamp}.lance`) - sorted by `src_vid`:

| Column | Type | Notes |
|--------|------|-------|
| src_vid | uint64 | **Sort key** - Source vertex |
| dst_vid | uint64 | Destination vertex |
| eid | uint64 | Edge ID |
| op | uint8 | 0=INSERT, 1=DELETE |
| _version | uint64 | Sequence number |
| props | struct | Edge properties (flattened) |

BWD run (`delta_{type}_bwd/run_{timestamp}.lance`) - sorted by `dst_vid`:

| Column | Type | Notes |
|--------|------|-------|
| dst_vid | uint64 | **Sort key** - Destination vertex |
| src_vid | uint64 | Source vertex |
| eid | uint64 | Edge ID |
| op | uint8 | 0=INSERT, 1=DELETE |
| _version | uint64 | Sequence number |
| props | struct | Edge properties (flattened) |

**Why dual runs?** Incoming edge lookups at scale cannot afford O(n) scans. Binary search requires sorted data, so we maintain both orderings.

### Chunking Parameters

| Parameter | Default | Notes |
|-----------|---------|-------|
| vertex_chunk_size | 2^18 (262,144) | Vertices per adjacency chunk |
| edge_chunk_size | 2^22 (4,194,304) | Max edges per edge dataset partition |
| l1_run_target | 1,000,000 | Target edges per L1 run |
| l0_buffer_size | 100,000 | Max edges in memory buffer |

---

## Part 5: Snapshots and Transactions

### Snapshot Manifest

Each snapshot captures a consistent view of the graph at a point in time.

```json
{
  "snapshot_id": "20250221T120000Z-000042",
  "created_at": "2025-02-21T12:00:00Z",
  "parent_snapshot": "20250221T110000Z-000041",
  "schema_version": "v1",

  "chunking": {
    "vertex_chunk_size": 262144,
    "edge_chunk_size": 4194304
  },

  "vertices": {
    "person": { "version": 42, "count": 1000000 },
    "company": { "version": 17, "count": 50000 }
  },

  "edges": {
    "knows": { "version": 23, "count": 50000000 },
    "works_at": { "version": 8, "count": 100000 }
  },

  "adjacency": {
    "knows": {
      "person": {
        "fwd": { "version": 15, "chunk_count": 128 },
        "bwd": { "version": 15, "chunk_count": 128 }
      },
      "company": {
        "fwd": { "version": 3, "chunk_count": 4 },
        "bwd": { "version": 3, "chunk_count": 4 }
      }
    }
  },

  "delta": {
    "knows": {
      "person": {
        "fwd_runs": [
          "delta_knows_fwd/person/run_1708516800.lance",
          "delta_knows_fwd/person/run_1708520400.lance"
        ],
        "bwd_runs": [
          "delta_knows_bwd/person/run_1708516800.lance",
          "delta_knows_bwd/person/run_1708520400.lance"
        ]
      },
      "company": {
        "fwd_runs": [],
        "bwd_runs": []
      }
    }
  },

  "stats": {
    "degree": {
      "knows_fwd": { "min": 0, "max": 150000, "avg": 50, "p99": 500 }
    }
  },

  "json_path_indexes": {
    "article": {
      "$.title": {
        "version": 12,
        "path": "idx_article_title/",
        "type": "string"
      },
      "$.author.name": {
        "version": 12,
        "path": "idx_article_author_name/",
        "type": "string"
      },
      "$.tags[*]": {
        "version": 10,
        "path": "idx_article_tags/",
        "type": "string",
        "array": true
      }
    }
  },

  "vector_indexes": {
    "article": {
      "$.embedding": {
        "version": 8,
        "path": "vec_article_embedding/",
        "dimensions": 384,
        "metric": "cosine",
        "index_type": "hnsw"
      }
    },
    "person": {
      "face_embedding": {
        "version": 15,
        "path": "vec_person_face/",
        "dimensions": 512,
        "metric": "l2",
        "index_type": "ivf_pq"
      }
    }
  },

  "scalar_indexes": {
    "person": {
      "age": {
        "version": 5,
        "lance_dataset_version": 42,
        "type": "btree"
      },
      "created_at": {
        "version": 3,
        "lance_dataset_version": 40,
        "type": "btree"
      }
    },
    "order": {
      "total": {
        "version": 7,
        "lance_dataset_version": 55,
        "type": "btree"
      }
    }
  }
}
```

**CRITICAL: Index Snapshot Isolation**

All indexes (JSON path, vector, and scalar) are **snapshot-versioned artifacts**. Each snapshot references specific index versions to ensure readers see consistent state.

```rust
/// Index versioning ensures snapshot isolation
impl Snapshot {
    /// Get JSON path index for a label/path, matching this snapshot's version
    pub fn get_json_index(&self, label: &str, path: &str) -> Option<&IndexRef> {
        self.manifest.json_path_indexes
            .get(label)?
            .get(path)
    }

    /// Get vector index matching this snapshot's version
    pub fn get_vector_index(&self, label: &str, prop: &str) -> Option<&VectorIndexRef> {
        self.manifest.vector_indexes
            .get(label)?
            .get(prop)
    }

    /// Get scalar property index matching this snapshot's version
    /// Returns the Lance dataset version to use for index lookups
    pub fn get_scalar_index(&self, label: &str, prop: &str) -> Option<&ScalarIndexRef> {
        self.manifest.scalar_indexes
            .get(label)?
            .get(prop)
    }
}

// Index files are immutable; new versions create new files
// Old index files are GC'd when no snapshot references them

/// Scalar index reference
pub struct ScalarIndexRef {
    /// Index version (for tracking changes)
    pub version: u64,
    /// Lance dataset version containing this index
    /// Readers MUST use this version for index lookups
    pub lance_dataset_version: u64,
    /// Index type (btree, hash, etc.)
    pub index_type: String,
}
```

**Scalar Index Snapshot Isolation**:

Lance manages scalar indexes internally. However, Lance dataset versions can advance independently of our snapshots. To ensure isolation:

1. When a snapshot is committed, record the current Lance dataset version for each indexed label
2. Readers use `Snapshot::get_scalar_index()` to get the correct Lance version
3. Query the Lance dataset at that specific version for index lookups

```rust
impl StorageReader {
    pub async fn lookup_by_scalar_index(
        &self,
        label: &str,
        property: &str,
        value: &Value,
        snapshot: &Snapshot,
    ) -> Result<Vec<Vid>> {
        // Get the scalar index version for this snapshot
        let index_ref = snapshot.get_scalar_index(label, property)
            .ok_or(Error::IndexNotFound { label, property })?;

        // Open Lance dataset at the pinned version
        let dataset = self.open_lance_at_version(
            &format!("vertices_{}", label),
            index_ref.lance_dataset_version,
        ).await?;

        // Use Lance's index for the lookup
        dataset.scan()
            .filter(col(property).eq(value))
            .project(&["vid"])
            .execute()
            .await
    }
}
```

**Why Index Versioning Matters**:
- Without versioning, a query at snapshot S1 might use an index built for S2
- This can return vertices that don't exist in S1, or miss vertices that do
- Vector search is especially vulnerable: embeddings change, index must match

### Commit Protocol

Optimistic concurrency control for object store:

```
1. Writer prepares:
   a. Flush L0 (SimpleGraph) to L1 runs
   b. Write all data files (vertices, edges, deltas)
   c. Generate new snapshot manifest with unique ID

2. Commit attempt:
   a. Read current "latest" pointer
   b. Verify parent_snapshot matches expected
   c. Write new manifest file (immutable)
   d. Conditional PUT to update "latest" pointer
      - S3: If-None-Match on version marker
      - GCS: Generation match
      - Local: Atomic rename

3. On conflict (CAS failure):
   a. FAIL FAST - another writer exists (misconfiguration)
   b. Schedule GC for orphaned files from failed commit
   c. Return fatal error - do NOT retry

4. On success:
   a. New snapshot is visible to readers
   b. Clear L0 buffer (reinitialize SimpleGraph)
   c. Schedule garbage collection of orphaned files
```

**Atomic Snapshot Publication Guarantees**:

We rely on object store durability guarantees without additional verification:

| Guarantee | Mechanism | Notes |
|-----------|-----------|-------|
| Manifest atomicity | Single PUT operation | Readers see complete manifest or nothing |
| Data file durability | Object store replication | S3/GCS provide 99.999999999% durability |
| Visibility ordering | "latest" pointer update is final step | Data files exist before manifest references them |

**Why NOT checksums/existence proofs?**

1. **Object stores guarantee write atomicity**: A successful PUT means the object is durably stored
2. **Adding checksums creates overhead**: Must read all files to verify on every snapshot load
3. **False negatives rare**: S3/GCS don't return success if write fails
4. **Recovery handles edge cases**: If manifest references non-existent file (extremely rare), reader fails fast with clear error

**Trust model**: Once object store confirms PUT success, we trust the object exists and is intact. Object store's internal replication and checksums handle bit-rot and corruption.

### Durability and WAL

#### Standard Mode (Persistent Storage)

- All mutations appended to WAL before updating L0 SimpleGraph
- On crash, replay WAL to reconstruct L0 SimpleGraph
- WAL location: local disk (SSD recommended)
- Snapshot commit only after WAL + L1 files are durable

#### Serverless Mode (Ephemeral Storage)

**The Problem**: In Lambda/K8s, local disk is ephemeral. Process restart = data loss.

**Solution**: Object-store WAL with batched commits.

```rust
pub enum WalMode {
    /// Fast local writes, crash recovery from local WAL
    Local { path: PathBuf },

    /// Durable remote writes, higher latency
    ObjectStore {
        bucket: String,
        /// Batch mutations before flushing to reduce round trips
        batch_size: usize,        // default: 100
        /// Max time before forced flush
        flush_interval_ms: u64,   // default: 1000
    },

    /// Hybrid: local WAL + async replication to object store
    Hybrid {
        local_path: PathBuf,
        remote_bucket: String,
        /// Sync to remote every N local writes
        sync_interval: usize,     // default: 1000
    },
}

impl ObjectStoreWal {
    pub async fn append(&mut self, mutation: Mutation) -> Result<()> {
        self.batch.push(mutation);

        if self.batch.len() >= self.batch_size || self.should_flush() {
            self.flush().await?;
        }

        Ok(())
    }

    async fn flush(&mut self) -> Result<()> {
        if self.batch.is_empty() {
            return Ok(());
        }

        // Write batch as single object (amortizes latency)
        let batch_id = Uuid::new_v4();
        let path = format!("wal/{}/{}.batch", self.session_id, batch_id);

        let data = self.serialize_batch(&self.batch)?;
        self.store.put(&path, data).await?;

        self.batch.clear();
        Ok(())
    }
}
```

**Trade-offs by mode**:

| Mode | Write Latency | Durability | Use Case |
|------|---------------|------------|----------|
| Local | <1ms | Process crash loses uncommitted | Long-running server |
| ObjectStore | 50-100ms (batched) | Fully durable | Serverless, Lambda |
| Hybrid | <1ms + async | Durable with lag | Best of both |

**⚠️ CRITICAL: Serverless Write Throughput Limitation**

Using S3/GCS as WAL imposes a **physics limitation** on write throughput:

| Mode | Commit Latency | Max Transactions/sec | Notes |
|------|----------------|---------------------|-------|
| Local WAL | ~1ms | 1,000+ | Limited by SSD |
| ObjectStore WAL | 50-100ms | **10-20** | S3 PUT latency |
| Hybrid | ~1ms (local) | 1,000+ | Async durability |

**This is not a bug—it's fundamental to object store semantics.** If your workload requires >20 writes/second with immediate durability, you MUST use:
- Local mode (accept crash risk), OR
- Hybrid mode (accept async durability lag), OR
- An external durable queue (Kafka, SQS) feeding batch imports

**Serverless is optimized for**:
- Read-heavy analytics workloads
- Batch/bulk imports (many edges per transaction)
- Infrequent writes with immediate reads

**WAL Sequencing and Recovery**:

**Critical**: WAL recovery must use monotonic sequence numbers (LSN), NOT timestamps. Clock skew or delayed flushes can cause timestamp-based filtering to skip valid mutations.

```rust
/// WAL entry with monotonic sequence number
pub struct WalEntry {
    /// Monotonic Log Sequence Number - strictly increasing per writer session
    pub lsn: u64,
    /// The mutation payload
    pub mutation: Mutation,
}

/// WAL batch (for object store mode)
pub struct WalBatch {
    /// Session ID that created this batch
    pub session_id: Uuid,
    /// First LSN in this batch (inclusive)
    pub start_lsn: u64,
    /// Last LSN in this batch (inclusive)
    pub end_lsn: u64,
    /// Entries in this batch
    pub entries: Vec<WalEntry>,
}

/// Snapshot manifest includes WAL high-water mark
pub struct SnapshotManifest {
    // ... other fields ...

    /// Highest LSN included in this snapshot
    /// All WAL entries with lsn <= wal_high_water_mark are already applied
    pub wal_high_water_mark: u64,

    /// Session ID of the writer that created this snapshot
    pub writer_session_id: Uuid,
}
```

**Recovery in Serverless**:

```rust
impl Database {
    pub async fn open_serverless(config: &Config) -> Result<Self> {
        // 1. Read latest snapshot manifest
        let snapshot = self.load_latest_snapshot().await?;
        let high_water_mark = snapshot.wal_high_water_mark;

        // 2. Scan for orphaned WAL batches from crashed sessions
        let orphaned_wals = self.scan_wal_batches().await?;

        // 3. Replay WAL entries with LSN > high_water_mark
        // This is clock-independent and handles out-of-order flushes
        for wal_batch in orphaned_wals {
            // Skip batches fully included in snapshot
            if wal_batch.end_lsn <= high_water_mark {
                self.delete_wal_batch(&wal_batch).await?; // GC
                continue;
            }

            // Replay entries not yet in snapshot
            for entry in wal_batch.entries {
                if entry.lsn > high_water_mark {
                    self.replay_mutation(entry.mutation).await?;
                }
            }
        }

        // 4. Flush recovered mutations to L1
        if self.l0_buffer.mutation_count > 0 {
            self.flush_l0_to_l1().await?;
            self.commit_snapshot().await?;
        }

        Ok(self)
    }
}
```

**Why LSN, not timestamp?**

| Issue | Timestamp-based | LSN-based |
|-------|-----------------|-----------|
| Clock skew | Can skip valid mutations | Immune - monotonic |
| Delayed flush | Race between wall clock and flush | Immune - ordered by LSN |
| Multi-session (future) | Timestamps can interleave incorrectly | Each session has own LSN space |
| Crash recovery | Ambiguous ordering | Deterministic replay |

### Concurrency Model

**Single-Writer, Multi-Reader Architecture**:

- **Single-writer constraint**: Exactly one writer process per graph (enforced or user-managed)
- **Multi-reader**: Unlimited concurrent readers on any committed snapshot
- **Isolation**: Readers see immutable snapshots; writer's uncommitted changes (L0) are invisible
- **Durability**: Committed snapshots survive crashes
- **CAS fence**: If-Match/ETag on commit detects misconfiguration (multiple writers), does NOT enable conflict resolution

#### Read Sessions with Snapshot Pinning

**Read-Only Snapshot Isolation**: A reader can pin a snapshot and run multiple queries against it. All queries within a session see the same consistent, immutable state. This provides serializable semantics for read-only workloads (no write skew possible since readers cannot write).

```rust
/// A read session pinned to a specific snapshot
pub struct ReadSession {
    /// The pinned snapshot - immutable for lifetime of session
    snapshot: Arc<Snapshot>,
    /// Session creation time (for timeout enforcement)
    created_at: Instant,
    /// Maximum session duration (prevents snapshot retention bloat)
    max_duration: Duration,
}

impl Database {
    /// Start a read session pinned to the current snapshot
    pub async fn begin_read(&self) -> Result<ReadSession> {
        let snapshot = self.load_latest_snapshot().await?;
        Ok(ReadSession {
            snapshot: Arc::new(snapshot),
            created_at: Instant::now(),
            max_duration: self.config.max_read_session_duration, // default: 1 hour
        })
    }

    /// Start a read session pinned to a specific historical snapshot
    pub async fn begin_read_at(&self, snapshot_id: &SnapshotId) -> Result<ReadSession> {
        let snapshot = self.load_snapshot(snapshot_id).await?;
        Ok(ReadSession {
            snapshot: Arc::new(snapshot),
            created_at: Instant::now(),
            max_duration: self.config.max_read_session_duration,
        })
    }
}

impl ReadSession {
    /// Execute a query against the pinned snapshot
    pub async fn query(&self, cypher: &str) -> Result<QueryResult> {
        // Check session timeout
        if self.created_at.elapsed() > self.max_duration {
            return Err(Error::SessionExpired);
        }

        // All queries use the same snapshot - read-only snapshot isolation
        self.execute_query(cypher, &self.snapshot).await
    }

    /// Get the pinned snapshot ID
    pub fn snapshot_id(&self) -> &SnapshotId {
        &self.snapshot.id
    }

    /// Refresh to latest snapshot (starts new logical transaction)
    pub async fn refresh(&mut self, db: &Database) -> Result<()> {
        let new_snapshot = db.load_latest_snapshot().await?;
        self.snapshot = Arc::new(new_snapshot);
        self.created_at = Instant::now();
        Ok(())
    }
}
```

**Session Semantics**:

| Property | Behavior |
|----------|----------|
| Isolation level | Read-only snapshot isolation |
| Visibility | Only sees data committed before session start |
| Concurrent writes | Invisible until `refresh()` called |
| Write operations | Not supported (read-only session) |
| Session duration | Configurable, default 1 hour max |
| Resource impact | Prevents GC of pinned snapshot's files |

**Usage Example**:

```rust
// Start a read session
let session = db.begin_read().await?;

// Multiple queries see the same consistent state
let users = session.query("MATCH (u:User) RETURN count(u)").await?;
let orders = session.query("MATCH (o:Order) RETURN count(o)").await?;
// These counts are consistent - from the same snapshot

// Meanwhile, a writer commits new data...
writer.create_vertex("User", ...).await?;
writer.commit().await?;

// Session still sees old data
let users_again = session.query("MATCH (u:User) RETURN count(u)").await?;
assert_eq!(users, users_again); // Same count!

// Explicitly refresh to see new data
session.refresh(&db).await?;
let users_updated = session.query("MATCH (u:User) RETURN count(u)").await?;
// Now sees the new user
```

**Snapshot Retention for Active Sessions**:

```rust
impl GarbageCollector {
    pub async fn collect(&self) -> Result<()> {
        // Get all active read sessions
        let active_sessions = self.session_registry.get_active_sessions();

        // Find oldest pinned snapshot
        let oldest_pinned = active_sessions
            .iter()
            .map(|s| s.snapshot_id())
            .min();

        // Only GC snapshots older than oldest_pinned AND beyond retention
        for snapshot in self.list_snapshots().await? {
            if Some(&snapshot.id) < oldest_pinned {
                continue; // Still needed by active session
            }
            if snapshot.age() < self.retention_period {
                continue; // Within retention window
            }
            self.delete_snapshot_files(&snapshot).await?;
        }

        Ok(())
    }
}
```

#### Single-Writer Exclusivity

**V1 Constraint**: Exactly one writer process per graph. This is enforced at the process boundary:

```rust
pub enum WriterExclusivity {
    /// Local filesystem: use flock/lockfile
    LocalLock {
        lock_file: PathBuf,  // e.g., /data/graph.lock
    },

    /// Object store: single-writer across processes is UNSUPPORTED
    /// User must ensure only one writer process exists (e.g., single Lambda, K8s singleton)
    ObjectStoreUnsupported,
}

impl Database {
    pub fn open_writer(&self) -> Result<Writer> {
        match &self.config.storage {
            StorageConfig::Local { path } => {
                // Acquire exclusive file lock
                let lock_path = path.join(".uni.lock");
                let lock_file = OpenOptions::new()
                    .create(true)
                    .write(true)
                    .open(&lock_path)?;

                // Try non-blocking exclusive lock
                if !lock_file.try_lock_exclusive()? {
                    return Err(Error::WriterAlreadyExists {
                        message: "Another process holds the writer lock".to_string(),
                        lock_path,
                    });
                }

                Ok(Writer::new(self, Some(lock_file)))
            }
            StorageConfig::ObjectStore { .. } => {
                // No cross-process locking available
                // User MUST ensure single writer externally
                log::warn!(
                    "Object store mode: single-writer across processes is NOT enforced. \
                     Ensure only one writer process exists (singleton deployment)."
                );
                Ok(Writer::new(self, None))
            }
        }
    }
}
```

**Deployment Models**:

| Environment | Exclusivity Mechanism | Notes |
|-------------|----------------------|-------|
| Local disk | File lock (`flock`) | Automatic, process-level |
| Single Lambda/Container | Implicit | One instance = one writer |
| K8s | StatefulSet with replicas=1 | Orchestrator enforces |
| Multiple processes on S3 | **UNSUPPORTED** | Will cause data corruption |

#### CAS as Misconfiguration Fence

The If-Match/ETag check on commit is a **misconfiguration detector**, not a coordination mechanism. If CAS fails, another writer modified the graph—which violates the single-writer constraint and indicates a deployment error.

```rust
impl Writer {
    pub async fn commit(&mut self) -> Result<SnapshotId> {
        // 1. Read current manifest (remember ETag)
        let current = self.read_manifest().await?;
        let expected_parent = current.snapshot_id.clone();

        // 2. Prepare commit (flush L0, upload data files)
        let new_manifest = self.prepare_commit().await?;

        // 3. Attempt CAS on manifest
        //    S3: PUT with If-Match on ETag
        //    GCS: PUT with generation match
        match self.cas_manifest(&new_manifest, &expected_parent).await {
            Ok(()) => {
                self.schedule_gc().await;
                Ok(new_manifest.snapshot_id)
            }
            Err(ConflictError) => {
                // CAS failure = ANOTHER WRITER EXISTS
                // This is a MISCONFIGURATION, not a normal condition
                self.schedule_gc_for_failed_commit(&new_manifest).await;

                // FAIL FAST - do NOT retry, do NOT attempt merge
                Err(Error::MultipleWritersDetected {
                    message: "CAS failed: another writer modified the graph. \
                              This violates the single-writer constraint. \
                              Ensure only one writer process exists.".to_string(),
                    expected_parent,
                    actual_parent: self.read_manifest().await?.snapshot_id,
                })
            }
        }
    }
}
```

**Key Design Decision**: No rebase/merge logic. CAS failure is always fatal.

| CAS Result | Meaning | Action |
|------------|---------|--------|
| Success | Commit accepted | Normal operation |
| Conflict | **Multiple writers exist** | Fail with clear error, user must fix deployment |

**Garbage Collection for orphaned files**:

```rust
impl GarbageCollector {
    pub async fn cleanup_orphaned_files(&self) -> Result<()> {
        // 1. List all data files in storage
        let all_files = self.list_all_files().await?;

        // 2. List files referenced by valid snapshots (current + retained)
        let referenced = self.get_referenced_files().await?;

        // 3. Delete orphaned files older than retention period
        let orphaned = all_files.difference(&referenced);
        for file in orphaned.filter(|f| f.age() > ORPHAN_RETENTION) {
            self.delete(file).await?;
        }

        Ok(())
    }
}
```

---

## Part 6: Read Path

### L0 Scope and Snapshot Isolation

**Critical**: L0 (the in-memory SimpleGraph) is **writer-scoped**. It contains uncommitted mutations that belong to the single writer process. Readers never see L0.

| Accessor | Sees L0? | Sees L1/L2? | Notes |
|----------|----------|-------------|-------|
| Writer (own uncommitted) | ✅ Yes | ✅ Yes | Sees full state including pending changes |
| ReadSession (snapshot) | ❌ No | ✅ Yes | Only sees committed data at snapshot time |
| Compactor | ❌ No | ✅ Yes | Operates on committed runs only |

**Why this matters for isolation**:
- A `ReadSession` pins a snapshot which references specific L1 runs and L2 chunks
- The snapshot does NOT include L0 because L0 is not yet committed
- Even if the writer modifies L0 while a reader is querying, the reader is unaffected
- This ensures read-only snapshot isolation without locking

### Unified Neighbor Lookup

To find neighbors of vertex V for edge type T, merge the levels visible to the caller. **Key**: Use `eid` as the unique key (not `dst_vid`) to support parallel edges.

```rust
/// Represents an edge with its version for merge conflict resolution
#[derive(Clone)]
struct EdgeEntry {
    dst_vid: Vid,
    eid: Eid,
    version: Version,
    deleted: bool,
}

impl StorageManager {
    /// Get neighbors for a vertex
    /// - l0_buffer: Some(&L0Buffer) for writer context, None for reader context
    /// - ReadSession always passes None to ensure snapshot isolation
    pub async fn get_neighbors(
        &self,
        vid: Vid,
        edge_type: EdgeType,
        direction: Direction,
        snapshot: &Snapshot,
        l0_buffer: Option<&L0Buffer>,  // None for readers, Some for writer
    ) -> Result<Vec<(Vid, Eid)>> {
        let label_id = vid.label_id();
        let local_offset = vid.local_offset();
        // Lookup label name for partition path (cached from schema)
        let label_name = self.schema.get_label_name(label_id)?;

        // Use eid as key to preserve parallel edges
        let mut edges: HashMap<Eid, EdgeEntry> = HashMap::new();

        // 1. Check L2 (base CSR chunks)
        // Handle BOTH directions if needed
        let directions = match direction {
            Direction::Both => vec![Direction::Outgoing, Direction::Incoming],
            d => vec![d],
        };

        for dir in directions {
            let partition_prefix = if dir == Direction::Outgoing { "fwd" } else { "bwd" };
            let adj_partition = format!("adj_{}_{}_{}", partition_prefix, edge_type, label_name);
            
            let chunk_id = local_offset / VERTEX_CHUNK_SIZE;
            // Note: In real impl, check if partition exists in manifest
            if let Some(chunk) = self.cache.get_chunk_if_exists(&adj_partition, chunk_id).await? {
                for (dst_vid, eid) in chunk.get_neighbors(vid) {
                    edges.insert(eid, EdgeEntry {
                        dst_vid, eid, version: 0, deleted: false
                    });
                }
            }
        }

        // 2. Check L1 (dual sorted runs) - keyed by (edge_type, label_name)
        // Note: manifest uses label names (strings), not IDs
        let type_name = self.schema.type_name(edge_type);
        if let Some(type_delta) = snapshot.delta.get(&type_name) {
            if let Some(label_delta) = type_delta.get(&label_name) {
                // Select appropriate run set based on direction
                // FWD runs are sorted by src_vid (for outgoing)
                // BWD runs are sorted by dst_vid (for incoming)
                for dir in &directions {
                    let runs = match dir {
                        Direction::Outgoing => &label_delta.fwd_runs,
                        Direction::Incoming => &label_delta.bwd_runs,
                        Direction::Both => unreachable!(), // Already expanded above
                    };

                    for run_path in runs {
                        let run = self.cache.get_or_load(run_path).await?;
                        // Binary search on sorted key (src_vid for FWD, dst_vid for BWD)
                        for (other_vid, eid, op, version) in run.find_by_key(vid) {
                            match edges.get(&eid) {
                                Some(existing) if existing.version >= version => continue,
                                _ => {}
                            }
                            edges.insert(eid, EdgeEntry {
                                dst_vid: other_vid, eid, version,
                                deleted: op == Op::DELETE,
                            });
                        }
                    }
                }
            }
        }

        // 3. Check L0 (SimpleGraph) - ONLY for writer context
        // NOTE: ReadSession does NOT pass l0_buffer; it uses None
        // This ensures snapshot isolation for readers
        if let Some(l0) = l0_buffer {
            for (dst_vid, eid, version) in l0.get_neighbors(vid, edge_type, direction) {
                edges.insert(eid, EdgeEntry { dst_vid, eid, version, deleted: false });
            }

            // Apply L0 tombstones (writer context only)
            Ok(edges.into_values()
                .filter(|e| !e.deleted && !l0.is_tombstoned(e.eid))
                .map(|e| (e.dst_vid, e.eid))
                .collect())
        } else {
            // Reader context: no L0, just filter deleted from L1/L2
            Ok(edges.into_values()
                .filter(|e| !e.deleted)
                .map(|e| (e.dst_vid, e.eid))
                .collect())
        }
    }
}
```

**Key design decisions:**
- **eid as key**: Parallel edges between same (src, dst) have different eids, so all are preserved
- **Version-based conflict resolution**: Higher version wins, regardless of which level it came from
- **Manifest run ordering**: L1 runs are listed oldest-to-newest in manifest, so later runs override earlier ones
- **Tombstone by eid**: DELETE operations target specific edges, not all edges to a destination

### Edge Property Fetch (Merge Join)

When a query needs edge properties (e.g., `WHERE e.weight > 0.5`), we use **merge join** to avoid random I/O:

```rust
impl StorageManager {
    /// Fetch neighbors WITH edge properties using merge join
    /// Returns: Vec<(dst_vid, eid, properties)>
    pub async fn get_neighbors_with_properties(
        &self,
        vid: Vid,
        edge_type: EdgeType,
        direction: Direction,
        needed_props: &[&str],  // Which properties to fetch
        snapshot: &Snapshot,
    ) -> Result<Vec<(Vid, Eid, Properties)>> {
        let label_name = self.schema.get_label_name(vid.label_id())?;
        let type_name = self.schema.type_name(edge_type);
        let local_offset = vid.local_offset();
        let chunk_id = local_offset / VERTEX_CHUNK_SIZE;

        // 1. Fetch adjacency chunk (topology)
        let adj_partition = format!("adj_fwd_{}_{}", type_name, label_name);
        let adj_chunk = self.cache.get_or_load_chunk(&adj_partition, chunk_id).await?;

        // 2. Fetch edge property chunk (SAME partition, SAME chunk_id!)
        // This is the key optimization: colocated data, sequential read
        let edge_partition = format!("edges_{}_{}", type_name, label_name);
        let edge_chunk = self.cache.get_or_load_chunk(&edge_partition, chunk_id).await?;

        // 3. Merge join: both chunks sorted by src_vid
        let neighbors = adj_chunk.get_neighbors(vid);  // (dst_vid, eid) pairs
        let edge_props = edge_chunk.get_edges_for_src(vid, needed_props);  // (eid, props) pairs

        // 4. Single-pass merge (both sorted by eid within src_vid group)
        let mut result = Vec::with_capacity(neighbors.len());
        let mut prop_iter = edge_props.into_iter().peekable();

        for (dst_vid, eid) in neighbors {
            // Advance property iterator to match eid
            while prop_iter.peek().map(|(e, _)| *e < eid).unwrap_or(false) {
                prop_iter.next();
            }

            let props = if prop_iter.peek().map(|(e, _)| *e == eid).unwrap_or(false) {
                prop_iter.next().unwrap().1
            } else {
                Properties::default()  // Edge exists in adjacency but not in edge dataset (L0/L1 edge)
            };

            result.push((dst_vid, eid, props));
        }

        // 5. Apply L0/L1 deltas and tombstones (similar to get_neighbors)
        self.apply_deltas_with_props(&mut result, vid, edge_type, direction, snapshot).await?;

        Ok(result)
    }
}
```

**Performance comparison**:

| Approach | 100 neighbors on S3 | 100 neighbors on SSD |
|----------|---------------------|----------------------|
| Random eid lookups | 10s (100 × 100ms) | 500ms (100 × 5ms) |
| **Merge join** | 200ms (2 × 100ms) | 10ms (2 × 5ms) |

**When to use which method**:
- `get_neighbors`: Topology-only queries, algorithms (BFS, shortest path)
- `get_neighbors_with_properties`: Queries with edge predicates or projections

### Loading into SimpleGraph for Query Execution

For multi-hop queries, load subgraph into SimpleGraph:

```rust
impl QueryExecutor {
    pub async fn execute_pattern_match(
        &self,
        start_vid: Vid,
        pattern: &Pattern,
    ) -> Result<Vec<Row>> {
        // Determine required hops from pattern
        let max_hops = pattern.max_path_length();
        let edge_types = pattern.required_edge_types();

        // Load subgraph into SimpleGraph
        // Pattern defines direction (e.g. (a)-[]->(b) is Outgoing)
        let direction = pattern.required_direction(); 
        let graph = self.storage
            .load_subgraph(&[start_vid], &edge_types, max_hops, direction)
            .await?;

        // Execute pattern matching on SimpleGraph
        let start_handle = graph.vertices()
            .find(|v| graph.vertex(*v).vid == start_vid)
            .ok_or(Error::VertexNotFound)?;

        // Use SimpleGraph's traversal for pattern matching
        let matches = self.match_pattern(&graph, start_handle, pattern)?;

        // Fetch properties for matched vertices (lazy load from Lance)
        self.fetch_properties(&graph, &matches).await
    }
}
```

---

## Part 7: Write Path

### Single Edge Insert

```rust
impl Writer {
    pub async fn insert_edge(
        &mut self,
        src_vid: Vid,
        dst_vid: Vid,
        edge_type: EdgeType,
        properties: Properties,
    ) -> Result<Eid> {
        // 1. Validate endpoints exist
        self.validate_vertex_exists(src_vid).await?;
        self.validate_vertex_exists(dst_vid).await?;

        // 2. Assign edge ID
        let eid = self.next_eid(edge_type);

        // 3. Write to L0 (SimpleGraph + WAL)
        self.l0_buffer.insert_edge(src_vid, dst_vid, edge_type, eid, properties.clone())?;

        // 4. Append to edge dataset
        self.edge_datasets[&edge_type].append(eid, src_vid, dst_vid, &properties).await?;

        Ok(eid)
    }
}
```

### Batch Insert

```rust
impl Writer {
    pub async fn batch_insert_edges(
        &mut self,
        edges: Vec<EdgeInsert>,
    ) -> Result<Vec<Eid>> {
        // Sort by (type, src_vid) for locality
        let mut sorted = edges;
        sorted.sort_by_key(|e| (e.edge_type, e.src_vid));

        // Group by type
        for (edge_type, group) in sorted.group_by(|e| e.edge_type) {
            // Assign eids
            let eids: Vec<_> = group.iter()
                .map(|_| self.next_eid(edge_type))
                .collect();

            // Build L1 run directly (bypass L0 for bulk)
            self.build_l1_run(edge_type, &group, &eids).await?;

            // Batch append to edge dataset
            self.edge_datasets[&edge_type].batch_append(&group, &eids).await?;
        }

        // Trigger compaction if needed
        if self.should_compact() {
            self.minor_compaction().await?;
        }

        Ok(eids)
    }
}
```

### Compaction

#### Compaction Strategy (Tiered, Write-Amplification Aware)

**The Problem**: Full L2 rebuild for every compaction is expensive. A 100GB adjacency dataset shouldn't be rewritten to merge 1MB of deltas.

**Solution**: Tiered compaction with selective chunk rewriting.

```
┌─────────────────────────────────────────────────────────────────────┐
│  L0 (Memory): DeltaGraph                                            │
│  - Flush when: size > 100K edges OR commit requested                │
│  → Writes: 1 L1 run file                                            │
├─────────────────────────────────────────────────────────────────────┤
│  L1 (Sorted Runs): Multiple small files                             │
│  - Minor compaction when: run_count > 10 OR total_size > 100MB      │
│  → Merges N runs → 1 larger run (no L2 touch)                       │
├─────────────────────────────────────────────────────────────────────┤
│  L2 (Base CSR): Chunked adjacency                                   │
│  - Major compaction when: L1 size > 10% of L2 OR tombstone_ratio > 20%│
│  → Selective: Only rewrite chunks with deltas                       │
└─────────────────────────────────────────────────────────────────────┘
```

#### Compaction Triggers

```rust
pub struct CompactionPolicy {
    // L0 → L1
    l0_flush_edge_count: usize,     // default: 100_000
    l0_flush_interval_secs: u64,    // default: 60

    // L1 minor compaction (merge runs)
    l1_max_run_count: usize,        // default: 10
    l1_max_total_size_mb: usize,    // default: 100

    // L1 → L2 major compaction
    l1_to_l2_size_ratio: f64,       // default: 0.1 (10%)
    tombstone_ratio_trigger: f64,   // default: 0.2 (20% tombstones)

    // Resource limits
    max_concurrent_compactions: usize, // default: 2
    compaction_memory_budget_mb: usize, // default: 512
}

impl Compactor {
    pub fn should_minor_compact(&self, edge_type: EdgeType) -> bool {
        let runs = &self.snapshot.delta[&edge_type].l1_runs;
        runs.len() > self.policy.l1_max_run_count ||
        self.l1_total_size(edge_type) > self.policy.l1_max_total_size_mb * MB
    }

    pub fn should_major_compact(&self, edge_type: EdgeType) -> bool {
        let l1_size = self.l1_total_size(edge_type);
        let l2_size = self.l2_total_size(edge_type);
        let tombstone_ratio = self.tombstone_ratio(edge_type);

        (l1_size as f64 / l2_size as f64) > self.policy.l1_to_l2_size_ratio ||
        tombstone_ratio > self.policy.tombstone_ratio_trigger
    }
}
```

#### Selective Chunk Rewriting

Only rewrite L2 chunks that have deltas:

```rust
impl Compactor {
    pub async fn major_compaction(&mut self, edge_type: EdgeType) -> Result<()> {
        // 1. Build delta index: which chunks have modifications?
        let mut affected_chunks: HashSet<ChunkId> = HashSet::new();
        let mut merge_graph = DeltaGraph::new_directed();

        for run_path in &self.snapshot.delta[&edge_type].l1_runs {
            let run = self.load_run(run_path).await?;
            for row in run.iter() {
                let chunk_id = row.src_vid.local_offset() / VERTEX_CHUNK_SIZE;
                affected_chunks.insert(chunk_id);
                self.apply_delta(&mut merge_graph, row);
            }
        }

        // 2. Only rewrite affected chunks (huge savings!)
        let total_chunks = self.snapshot.adjacency[&edge_type].chunk_count;
        let rewrite_ratio = affected_chunks.len() as f64 / total_chunks as f64;
        log::info!(
            "Major compaction: rewriting {}/{} chunks ({:.1}%)",
            affected_chunks.len(), total_chunks, rewrite_ratio * 100.0
        );

        for chunk_id in affected_chunks {
            let chunk = self.load_chunk(edge_type, chunk_id).await?;
            let new_chunk = self.merge_chunk_with_deltas(chunk, &merge_graph);
            self.write_chunk(edge_type, chunk_id, new_chunk).await?;
        }

        // 3. Update manifest (clear L1 runs, bump affected chunk versions)
        self.update_manifest(edge_type, &affected_chunks).await?;

        Ok(())
    }
}
```

#### Write Amplification Mitigation (Dense Graphs)

**The Concern**: In dense graphs with random updates, selective chunk rewriting may still touch 50%+ of chunks.

**Reality Check**: This is inherent to any mutable storage on immutable object stores. However, we can minimize impact:

**Strategy 1: Amortized Compaction**

Don't compact too frequently. Let L1 grow larger before major compaction:

```rust
// For update-heavy workloads, increase thresholds:
CompactionPolicy {
    l1_to_l2_size_ratio: 0.25,     // 25% instead of 10%
    tombstone_ratio_trigger: 0.40, // 40% instead of 20%
    // Accept higher read amplification for lower write amplification
}
```

**Strategy 2: Update Locality Hints**

When updates have locality (common in real workloads), exploit it:

```rust
/// Batch updates by chunk to minimize compaction spread
impl BatchWriter {
    /// Sort updates by chunk before writing
    /// Reduces L1 run fragmentation
    pub fn batch_insert_edges(&mut self, edges: Vec<EdgeInsert>) -> Result<()> {
        // Sort by (edge_type, src_vid / CHUNK_SIZE)
        let mut sorted = edges;
        sorted.sort_by_key(|e| (e.edge_type, e.src_vid / VERTEX_CHUNK_SIZE));

        // Write as single L1 run with locality
        self.write_sorted_run(&sorted).await
    }
}
```

**Strategy 3: Read-Optimized vs Write-Optimized Mode**

```rust
pub enum StorageMode {
    /// Aggressive compaction, fast reads, high write cost
    /// Use for: Read-heavy analytics, batch-loaded data
    ReadOptimized {
        l1_to_l2_ratio: 0.05,  // Compact early
    },

    /// Lazy compaction, slower reads, low write cost
    /// Use for: Write-heavy workloads, streaming ingestion
    WriteOptimized {
        l1_to_l2_ratio: 0.50,  // Delay compaction
        max_l1_runs: 50,       // Accept more runs
    },
}
```

**Cost Analysis** (100GB adjacency, 10% random updates):

| Strategy | Chunks Affected | S3 PUTs | Cost/Compaction |
|----------|----------------|---------|-----------------|
| Naive (full rewrite) | 100% | ~1000 | $5.00 |
| Selective (10% affected) | 10% | ~100 | $0.50 |
| Amortized (delay 5x) | 10% / 5 | ~20 | $0.10 |

**Recommendation**: For PB-scale deployments, use WriteOptimized mode with background compaction during low-traffic periods.

#### Write Amplification Analysis

| Scenario | Full Rewrite | Selective |
|----------|--------------|-----------|
| 1% of vertices modified | Rewrite 100% L2 | Rewrite ~1% L2 |
| 10% of vertices modified | Rewrite 100% L2 | Rewrite ~10% L2 |
| Deletes only (tombstones) | Rewrite 100% L2 | Rewrite affected chunks |

**Worst case**: Uniformly distributed updates touch all chunks → same as full rewrite.
**Best case**: Localized updates (e.g., recent vertices) → minimal rewrite.

#### Background Compaction

```rust
impl CompactionScheduler {
    pub async fn run(&mut self) {
        loop {
            tokio::select! {
                _ = self.check_interval.tick() => {
                    for edge_type in self.schema.edge_types() {
                        if self.compactor.should_minor_compact(edge_type) {
                            self.spawn_compaction(CompactionJob::Minor(edge_type));
                        }
                        if self.compactor.should_major_compact(edge_type) {
                            self.spawn_compaction(CompactionJob::Major(edge_type));
                        }
                    }
                }
                _ = self.shutdown.recv() => break,
            }
        }
    }
}
```

---

## Part 8: Query Execution

### Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                        Query Text                           │
└─────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────┐
│  Parser                    → Abstract Syntax Tree (AST)     │
└─────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────┐
│  Binder                    → Bound AST (types resolved)     │
│  - Resolve labels/types                                     │
│  - Resolve ext_id → vid                                     │
│  - Validate schema                                          │
└─────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────┐
│  Planner                   → Logical Plan                   │
│  - Convert patterns to operators                            │
│  - Determine subgraph load bounds                           │
└─────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────┐
│  Optimizer                 → Optimized Logical Plan         │
│  - Cost-based join reordering                               │
│  - Predicate pushdown                                       │
│  - Subgraph size estimation                                 │
└─────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────┐
│  Graph Loader              → WorkingGraph (SimpleGraph)     │
│  - Load required subgraph from storage                      │
│  - Merge L0 + L1 + L2                                       │
└─────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────┐
│  Executor                  → Results                        │
│  - Pattern matching on SimpleGraph                          │
│  - Use uni-algo (BFS, ShortestPaths, etc.)                 │
│  - Lazy property fetch from Lance                           │
└─────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────┐
│  DataFusion                → Final Results                  │
│  - WHERE/RETURN/ORDER BY/GROUP BY evaluation                │
│  - Aggregations (COUNT, SUM, AVG, etc.)                     │
│  - Columnar batch processing                                │
└─────────────────────────────────────────────────────────────┘
```

### Query Engine Architecture: SimpleGraph + DataFusion

**Critical Insight**: SimpleGraph is a graph data structure, not a query engine. It solves pattern matching but does not handle:
- Predicate evaluation (WHERE clauses)
- Projections (RETURN with expressions)
- Sorting (ORDER BY)
- Aggregations (GROUP BY, COUNT, SUM)
- Joins beyond graph traversal

**Solution**: Hybrid execution with DataFusion for relational operations.

**⚠️ IMPORTANT: DataFusion is for BOUNDED queries only**

| Query Type | Execution Path | DataFusion? |
|------------|----------------|-------------|
| Bounded (subgraph fits in memory) | Materialize → SimpleGraph → DataFusion | ✅ Yes |
| Unbounded (PB-scale scan) | Streaming iterator | ❌ No |
| Aggregations over bounded | Materialize → aggregate | ✅ Yes |
| Aggregations over unbounded | Streaming aggregate | ❌ No (custom) |

**Why?** `collect()` materializes all results. For PB-scale scans, this OOMs. Unbounded queries use iterator-based processing that never materializes more than one "morsel" at a time.

```rust
impl QueryExecutor {
    pub async fn execute(&self, plan: &LogicalPlan) -> Result<Box<dyn RecordBatchStream>> {
        let estimate = self.estimate_result_size(plan)?;

        if estimate.is_bounded(&self.config) {
            // Bounded: use DataFusion (materializes results)
            self.execute_bounded(plan).await
        } else {
            // Unbounded: streaming execution (bypasses DataFusion)
            self.execute_streaming(plan).await
        }
    }

    /// Streaming execution for unbounded queries
    /// Returns an iterator, never materializes full result set
    async fn execute_streaming(&self, plan: &LogicalPlan) -> Result<Box<dyn RecordBatchStream>> {
        // Process one morsel at a time
        // Apply filters during iteration, not after
        // Aggregations use streaming accumulators (e.g., HyperLogLog for COUNT DISTINCT)
        Ok(Box::new(StreamingGraphIterator::new(plan, self.storage.clone())))
    }
}
```

**Bounded Execution (with DataFusion)**:

```rust
/// Query execution splits work between SimpleGraph and DataFusion
pub struct QueryExecutor {
    /// Graph algorithms and pattern matching
    graph_engine: GraphEngine,
    /// Relational operations (filter, project, aggregate, sort)
    datafusion_ctx: SessionContext,
}

impl QueryExecutor {
    pub async fn execute(&self, plan: &LogicalPlan) -> Result<RecordBatch> {
        // Phase 1: Graph Operations (SimpleGraph)
        // - Load subgraph into WorkingGraph
        // - Execute pattern matching (MATCH clause)
        // - Produce intermediate tuples: (matched vertices, edges)
        let graph_results = self.graph_engine.execute_patterns(plan).await?;

        // Phase 2: Convert to Arrow RecordBatch
        // - Each matched pattern becomes a row
        // - Columns: bound variables (a, b, r, etc.) as VID/EID references
        let arrow_batch = self.to_arrow_batch(&graph_results)?;

        // Phase 3: Relational Operations (DataFusion)
        // - Register batch as table "matched"
        // - Execute SQL-like operations for WHERE, RETURN, ORDER BY
        let df = self.datafusion_ctx.read_batch(arrow_batch)?;

        // WHERE clause → DataFusion filter
        let filtered = df.filter(plan.where_clause())?;

        // Property access → lazy fetch, then join
        let with_props = self.join_properties(filtered, plan.needed_properties()).await?;

        // RETURN clause → DataFusion projection
        let projected = with_props.select(plan.return_expressions())?;

        // ORDER BY, LIMIT, SKIP
        let sorted = projected
            .sort(plan.order_by())?
            .limit(plan.skip(), plan.limit())?;

        // Aggregations (GROUP BY)
        let aggregated = if plan.has_aggregations() {
            sorted.aggregate(plan.group_keys(), plan.aggregates())?
        } else {
            sorted
        };

        aggregated.collect().await
    }
}
```

**Why DataFusion?**

| Capability | SimpleGraph | DataFusion | Used For |
|------------|------|------------|----------|
| Graph traversal | ✅ | ❌ | MATCH patterns |
| BFS/DFS/ShortestPath | ✅ | ❌ | Path algorithms |
| Predicate evaluation | ❌ | ✅ | WHERE clauses |
| Expression computation | ❌ | ✅ | RETURN with math |
| Sorting | ❌ | ✅ | ORDER BY |
| Aggregations | ❌ | ✅ | COUNT, SUM, GROUP BY |
| Vectorized execution | ❌ | ✅ | SIMD-optimized ops |
| Lance integration | ❌ | ✅ | Direct columnar reads |

**Execution Example**:

```cypher
MATCH (p:Person)-[r:FOLLOWS]->(q:Person)
WHERE r.weight > 0.5 AND q.age > 30
RETURN p.name, q.name, r.weight
ORDER BY r.weight DESC
LIMIT 10
```

```
Phase 1 (SimpleGraph):
  - Load FOLLOWS subgraph into WorkingGraph
  - Pattern match: find all (p)-[r]->(q) tuples
  - Output: RecordBatch with columns [p_vid, q_vid, r_eid]

Phase 2 (DataFusion):
  - Join with denormalized weight from adjacency (already loaded)
  - Filter: r.weight > 0.5
  - Join with Person properties for q.age
  - Filter: q.age > 30
  - Project: fetch p.name, q.name
  - Sort: ORDER BY r.weight DESC
  - Limit: 10 rows
  - Return Arrow RecordBatch
```

**Property Fetch Strategy**:

```rust
impl QueryExecutor {
    /// Batch fetch properties and join with matched results
    async fn join_properties(
        &self,
        matched: DataFrame,
        needed: &[(Variable, Vec<String>)],  // e.g., [(p, ["name", "age"]), ...]
    ) -> Result<DataFrame> {
        let mut result = matched;

        for (var, props) in needed {
            // 1. Extract VIDs for this variable
            let vids: Vec<u64> = result.column(&var.name())?.collect();

            // 2. Batch read from Lance (single scan, not per-row!)
            let prop_batch = self.storage
                .read_vertex_properties(&vids, props)
                .await?;

            // 3. Join on VID using DataFusion
            result = result.join(
                prop_batch,
                JoinType::Inner,
                &[var.name()],
                &["vid"],
            )?;
        }

        Ok(result)
    }
}
```

### OpenCypher Declared Subset

**Compatibility Statement**: Uni implements a **declared subset** of OpenCypher. Unsupported syntax returns a clear error with the specific unsupported feature identified.

#### Supported Clauses

| Clause | Status | Notes |
|--------|--------|-------|
| `MATCH` | ✅ Supported | Single and multi-pattern |
| `WHERE` | ✅ Supported | All operators below |
| `RETURN` | ✅ Supported | Including aliases, `DISTINCT` |
| `ORDER BY` | ✅ Supported | ASC/DESC |
| `LIMIT` / `SKIP` | ✅ Supported | |
| `CREATE` | ✅ Supported | Vertices and edges |
| `DELETE` / `DETACH DELETE` | ✅ Supported | |
| `SET` | ✅ Supported | Property updates |
| `WITH` | ✅ Supported | Chained queries |
| `UNWIND` | ✅ Supported | List expansion |
| `OPTIONAL MATCH` | ✅ Supported | Left outer join semantics |
| `MERGE` | ⚠️ Partial | `ON CREATE SET` only, no `ON MATCH` |
| `UNION` / `UNION ALL` | ❌ Not supported | Use multiple queries |
| `CALL` (procedures) | ❌ Not supported | |
| `FOREACH` | ❌ Not supported | |
| `LOAD CSV` | ❌ Not supported | Use bulk import API |

#### Supported Operators

| Category | Operators |
|----------|-----------|
| Comparison | `=`, `<>`, `<`, `>`, `<=`, `>=` |
| Boolean | `AND`, `OR`, `NOT`, `XOR` |
| String | `STARTS WITH`, `ENDS WITH`, `CONTAINS`, `=~` (regex) |
| List | `IN`, list indexing `[0]`, slicing `[1..3]` |
| Null | `IS NULL`, `IS NOT NULL` |
| Math | `+`, `-`, `*`, `/`, `%`, `^` |
| Property | `.property`, `[]` dynamic access |

#### Supported Aggregations (Columnar Analytics)

**Note**: Columnar analytics are provided through Cypher aggregation functions, not a separate SQL interface.

| Function | Description |
|----------|-------------|
| `count(*)` | Count rows |
| `count(expr)` | Count non-null values |
| `sum(expr)` | Sum numeric values |
| `avg(expr)` | Average numeric values |
| `min(expr)` / `max(expr)` | Min/max values |
| `collect(expr)` | Aggregate into list |
| `stDev(expr)` / `stDevP(expr)` | Standard deviation |
| `percentileCont(expr, p)` | Percentile (continuous) |
| `percentileDisc(expr, p)` | Percentile (discrete) |

```cypher
// Analytics examples
MATCH (p:Person)-[:PURCHASED]->(prod:Product)
RETURN prod.category, count(*) AS purchases, sum(p.amount) AS revenue
ORDER BY revenue DESC

MATCH (u:User)-[:RATED]->(m:Movie)
WHERE m.year >= 2020
RETURN m.genre, avg(u.rating) AS avg_rating, collect(m.title) AS titles
```

#### Path Patterns

| Pattern | Status | Notes |
|---------|--------|-------|
| `(a)-[r]->(b)` | ✅ | Directed edge |
| `(a)<-[r]-(b)` | ✅ | Reverse directed |
| `(a)-[r]-(b)` | ✅ | Undirected (both directions) |
| `(a)-[r:TYPE]->(b)` | ✅ | Typed edge |
| `(a)-[r:T1\|T2]->(b)` | ✅ | Multiple types |
| `(a)-[*1..3]->(b)` | ✅ | Variable-length path |
| `shortestPath(...)` | ✅ | Single shortest path |
| `allShortestPaths(...)` | ✅ | All shortest paths |
| `(a)-[*]->+(b)` | ❌ | Quantified path patterns (GQL) |

#### Functions

| Category | Functions |
|----------|-----------|
| String | `toString()`, `toUpper()`, `toLower()`, `trim()`, `substring()`, `split()`, `replace()` |
| Numeric | `abs()`, `ceil()`, `floor()`, `round()`, `sign()`, `rand()` |
| List | `size()`, `head()`, `tail()`, `last()`, `range()`, `reverse()` |
| Type | `type()` (edge type), `labels()` (vertex labels), `id()` (internal id) |
| Null | `coalesce()` |
| **Vector (Extension)** | `vector_search()`, `vector_distance()`, `cosine_similarity()` |
| **JSON (Extension)** | `json_path()`, `json_extract()` |

#### Parser Error Messages

Unsupported syntax produces actionable errors:

```
Error: Unsupported Cypher feature: UNION clause
  |
  | MATCH (a:Person) RETURN a.name
  | UNION
  | ^^^^^ UNION is not supported in Uni
  |
  = help: Execute multiple queries separately and combine results in your application

Error: Unsupported Cypher feature: Quantified path pattern
  |
  | MATCH (a)-[*]->+(b)
  |              ^^ QPP syntax is not supported
  |
  = help: Use variable-length pattern: (a)-[*1..10]->(b)
```

### Logical Operators

| Operator | Description |
|----------|-------------|
| `NodeScan(label)` | Scan all vertices of a label |
| `EdgeScan(type)` | Scan all edges of a type |
| `Expand(src, type, dir)` | Expand from src via edge type |
| `ExpandInto(src, dst, type, dir)` | Check if edge exists between bound endpoints |
| `Filter(expr)` | Apply predicate |
| `Project(exprs)` | Select/compute columns |
| `ShortestPath(src, dst, type)` | Find shortest path (delegates to uni-algo) |
| `AllPaths(src, dst, type, max_len)` | Find all paths up to length |
| `VectorSearch(label, embedding, k)` | k-NN vector search |

### Execution Strategy

**Bounded queries** (most common):
1. Estimate subgraph size from statistics
2. Load subgraph into SimpleGraph WorkingGraph
3. Execute pattern matching / algorithms on SimpleGraph
4. Lazy-load properties from Lance as needed

**Unbounded queries** (full scans):
1. Stream through Lance datasets
2. Use iterator-based processing
3. No full materialization into SimpleGraph

### Memory Budgeting & Subgraph Explosion Protection

**The Problem**: A query like `MATCH (a)-[:FOLLOWS*1..6]->(b)` on a social graph can explode to millions of vertices, exceeding RAM.

**Solution**: Memory-aware execution with adaptive fallback.

```rust
pub struct ExecutionConfig {
    /// Max memory for WorkingGraph (default: 256MB)
    max_subgraph_memory: usize,

    /// Max vertices before switching to streaming (default: 100_000)
    max_subgraph_vertices: usize,

    /// Max edges before switching to streaming (default: 1_000_000)
    max_subgraph_edges: usize,
}

impl QueryExecutor {
    pub async fn execute(&self, plan: &Plan) -> Result<ResultSet> {
        // 1. Estimate subgraph size from statistics
        let estimate = self.estimate_subgraph_size(plan)?;

        if estimate.exceeds_budget(&self.config) {
            // 2a. Switch to streaming execution
            self.execute_streaming(plan).await
        } else {
            // 2b. Materialize and execute on SimpleGraph
            self.execute_materialized(plan).await
        }
    }

    /// Streaming execution for large result sets
    async fn execute_streaming(&self, plan: &Plan) -> Result<ResultSet> {
        // Process one frontier "morsel" at a time
        // Never hold more than `morsel_size` vertices in memory
        let mut results = Vec::new();
        let mut frontier = plan.start_vertices();

        while !frontier.is_empty() && results.len() < plan.limit() {
            // Fetch one chunk of neighbors
            let chunk = self.storage
                .get_neighbors_batch(&frontier, plan.edge_type())
                .await?;

            // Apply filters immediately, don't accumulate
            for (src, dst, eid) in chunk {
                if plan.matches(src, dst, eid) {
                    results.push(self.build_result_row(src, dst, eid));
                }
            }

            // Advance frontier (with limit to prevent explosion)
            frontier = self.next_frontier(&chunk, self.config.max_subgraph_vertices);
        }

        Ok(ResultSet::from(results))
    }
}
```

**Supernode Detection** (query planning time):

```rust
impl Planner {
    fn check_supernode_risk(&self, vid: Vid, edge_type: EdgeType) -> SupernodeRisk {
        let degree = self.stats.get_degree(vid, edge_type);

        match degree {
            d if d > 100_000 => SupernodeRisk::Critical,  // Reject or require LIMIT
            d if d > 10_000 => SupernodeRisk::High,       // Warn, use streaming
            d if d > 1_000 => SupernodeRisk::Medium,      // Proceed with caution
            _ => SupernodeRisk::Low,
        }
    }
}
```

**Configuration options**:
- `QUERY_MEMORY_LIMIT`: Hard cap on working memory
- `SUPERNODE_THRESHOLD`: Degree above which to warn/reject
- `REQUIRE_LIMIT_FOR_UNBOUNDED`: Force `LIMIT` clause on `*` paths

### Hybrid Vector + Graph Queries

**The Problem**: Vector indexes (HNSW/IVF) are approximate and pre-built. How do we combine with graph traversal while respecting MVCC?

**Two Execution Strategies**:

```
Strategy 1: Vector-First (when vector is selective)
┌─────────────────────────────────────────────────────────────┐
│ 1. VectorSearch(embedding, k=100)  → candidate vids         │
│ 2. Filter candidates by snapshot visibility                 │
│ 3. For each candidate: check graph connectivity             │
│ 4. Return connected candidates with vector scores           │
└─────────────────────────────────────────────────────────────┘

Strategy 2: Graph-First (when graph is selective)
┌─────────────────────────────────────────────────────────────┐
│ 1. Traverse graph pattern → collect candidate vids          │
│ 2. Batch fetch embeddings for candidates from Lance         │
│ 3. Compute vector distance in-memory (skip index)           │
│ 4. Sort by distance, return top-k                           │
└─────────────────────────────────────────────────────────────┘
```

**MVCC for Vector Results (Version-Aware)**:

Vector indexes are built on L2 (compacted) data. Delta entries in L0/L1 may:
- Add new vertices (not in index)
- Delete vertices (in index but should be hidden)
- **Modify embeddings (stale in index - GHOST RESULTS!)**

**The Ghost Results Problem**:
```
Timeline:
  t=100: Vertex V has embedding E1, index built with (V, E1)
  t=200: V's embedding updated to E2 (delta in L0)
  t=300: Query for "cats" → index returns V with high score (based on E1!)
         But E2 might be about "dogs" - WRONG RESULT
```

The naive fix (just checking visibility) misses this case because V is still visible - only its embedding changed.

**Solution: Version Tracking in Vector Index**:

Store `_version` (Lance row version or update timestamp) alongside vid in the index:

```rust
// Vector index entry includes version
struct VectorIndexEntry {
    vid: u64,
    version: u64,    // Lance row version when embedding was indexed
    embedding: Vec<f32>,
}

impl VectorSearch {
    pub async fn search_with_mvcc(
        &self,
        query: &[f32],
        k: usize,
        snapshot: &Snapshot,
    ) -> Result<Vec<(Vid, f32)>> {
        // 1. Search index (may return stale/deleted results)
        //    Index returns (vid, version, score)
        let candidates = self.index.search(query, k * 3)?; // Over-fetch more

        // 2. Filter by snapshot visibility AND version freshness
        let mut visible = Vec::new();
        let mut stale_vids = Vec::new();

        for (vid, indexed_version, score) in candidates {
            match self.check_visibility_and_version(vid, indexed_version, snapshot).await? {
                VersionCheck::Fresh => {
                    // Indexed version matches current visible version
                    visible.push((vid, score));
                }
                VersionCheck::Stale(current_version) => {
                    // Embedding was updated - need to re-score
                    stale_vids.push(vid);
                }
                VersionCheck::Deleted => {
                    // Skip deleted vertices
                    continue;
                }
            }

            if visible.len() >= k && stale_vids.is_empty() {
                break;
            }
        }

        // 3. Re-score stale vertices with current embeddings
        if !stale_vids.is_empty() {
            let current_embeddings = self.fetch_current_embeddings(&stale_vids, snapshot).await?;
            for (vid, embedding) in current_embeddings {
                let score = self.compute_distance(query, &embedding);
                visible.push((vid, score));
            }
        }

        // 4. Check L0/L1 for new vertices with embeddings (not in index)
        let delta_results = self.search_deltas(query, k, snapshot).await?;

        // 5. Merge and return top-k
        let mut all_results = visible;
        all_results.extend(delta_results);
        all_results.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap());
        all_results.truncate(k);

        Ok(all_results)
    }

    async fn check_visibility_and_version(
        &self,
        vid: Vid,
        indexed_version: u64,
        snapshot: &Snapshot,
    ) -> Result<VersionCheck> {
        // Check L0 first (most recent)
        if let Some(delta) = snapshot.l0.get_vertex(vid) {
            if delta.is_tombstone {
                return Ok(VersionCheck::Deleted);
            }
            // L0 has newer version - index is stale
            return Ok(VersionCheck::Stale(delta.version));
        }

        // Check L1 sorted runs
        for run in &snapshot.l1_runs {
            if let Some(delta) = run.get_vertex(vid).await? {
                if delta.is_tombstone {
                    return Ok(VersionCheck::Deleted);
                }
                if delta.version != indexed_version {
                    return Ok(VersionCheck::Stale(delta.version));
                }
            }
        }

        // L2: check if vertex exists and version matches
        if let Some(l2_version) = snapshot.l2.get_vertex_version(vid).await? {
            if l2_version == indexed_version {
                return Ok(VersionCheck::Fresh);
            } else {
                return Ok(VersionCheck::Stale(l2_version));
            }
        }

        Ok(VersionCheck::Deleted)
    }
}

enum VersionCheck {
    Fresh,           // Indexed version matches current - score is valid
    Stale(u64),      // Embedding updated - need to re-score
    Deleted,         // Vertex deleted - skip
}
```

**Performance Implications**:
- **Best case**: Most results are fresh, minimal overhead
- **Moderate churn**: 10-20% stale results, re-scoring adds ~10ms
- **High churn**: Consider rebuilding index on compaction

**Index Rebuild Strategy**:
```rust
// During L2 compaction, optionally rebuild vector index
impl Compaction {
    pub async fn compact_with_vector_rebuild(&mut self) -> Result<()> {
        // 1. Compact L1 → L2 as normal
        self.compact_to_l2().await?;

        // 2. If stale_ratio > threshold, rebuild vector index
        let stale_ratio = self.estimate_stale_ratio();
        if stale_ratio > 0.3 {  // 30% stale
            self.rebuild_vector_index().await?;
        }

        Ok(())
    }
}
```

**Planner Heuristics**:

| Condition | Strategy |
|-----------|----------|
| Vector k < 100, graph pattern complex | Vector-first |
| Graph pattern simple (1-hop), many candidates | Graph-first |
| User hint `/*+ VECTOR_FIRST */` | Vector-first |
| No vector index exists | Graph-first (compute distances) |

### Example: Shortest Path Query

```cypher
MATCH path = shortestPath((a:Person {name: "Alice"})-[:KNOWS*..5]-(b:Person {name: "Bob"}))
RETURN path
```

Execution:

```rust
// 1. Resolve start/end vids
let alice_vid = self.resolve_ext_id("Person", "Alice")?;
let bob_vid = self.resolve_ext_id("Person", "Bob")?;

// 2. Load 5-hop neighborhood (or use bidirectional meet-in-middle)
let graph = self.storage.load_subgraph(
    &[alice_vid, bob_vid],
    &[EdgeType::KNOWS],
    5,
).await?;

// 3. Find vertices in SimpleGraph
let alice = self.find_vertex(&graph, alice_vid)?;
let bob = self.find_vertex(&graph, bob_vid)?;

// 4. Use uni-algo's ShortestPaths
let result = ShortestPaths::on(&graph)
    .goal(bob)
    .run(alice)?;

// 5. Reconstruct path
let path = result.reconstruct(bob)?;

// 6. Fetch properties for path vertices
self.fetch_path_properties(&graph, &path).await
```

---

## Part 9: Caching

### Three-Tier Cache Architecture

```
┌─────────────────────────────────────────────────────────────┐
│ L1 Cache: Hot (Memory)                                      │
│ - Adjacency chunks (decoded, ready for SimpleGraph)         │
│ - L0 SimpleGraph buffer                                     │
│ - Property pages (hot vertices)                             │
│ - Recent WorkingGraphs (LRU, for repeated queries)          │
│ - Capacity: ~1GB (configurable)                             │
└─────────────────────────────────────────────────────────────┘
                         ↓ miss
┌─────────────────────────────────────────────────────────────┐
│ L2 Cache: Warm (Local SSD)                                  │
│ - Adjacency chunks (Lance files)                            │
│ - L1 delta runs                                             │
│ - Vertex/edge data files                                    │
│ - Capacity: ~100GB (configurable)                           │
│ - Eviction: LRU with admission policy                       │
└─────────────────────────────────────────────────────────────┘
                         ↓ miss
┌─────────────────────────────────────────────────────────────┐
│ L3: Cold (Object Store)                                     │
│ - Source of truth                                           │
│ - All data files                                            │
│ - Read-through to upper tiers                               │
└─────────────────────────────────────────────────────────────┘
```

### WorkingGraph Caching

For repeated queries on similar subgraphs:

```rust
pub struct WorkingGraphCache {
    /// LRU cache of recently materialized subgraphs
    cache: LruCache<SubgraphKey, Arc<WorkingGraph>>,
}

#[derive(Hash, Eq, PartialEq)]
pub struct SubgraphKey {
    start_vids: Vec<Vid>,
    edge_types: Vec<EdgeType>,
    max_hops: usize,
    snapshot_id: SnapshotId,
}

impl WorkingGraphCache {
    pub fn get_or_load(
        &mut self,
        key: SubgraphKey,
        loader: impl FnOnce() -> WorkingGraph,
    ) -> Arc<WorkingGraph> {
        self.cache.get_or_insert(key, || Arc::new(loader()))
    }
}
```

### Disk-Based Fallback for Large Graphs

**Problem**: DeltaGraph and WorkingGraph are memory-only. When graphs exceed available RAM:
- OOM kills the process
- Users must pre-filter data or scale vertically
- No graceful degradation path

**Solution**: Memory-mapped fallback for "slightly-too-big" graphs—data that exceeds RAM but fits on fast local SSD.

```rust
/// Strategy for graph data storage
pub enum GraphStorageStrategy {
    /// Pure in-memory (default, fastest)
    InMemory,

    /// Memory-mapped file (for slightly-too-big graphs)
    MappedFile {
        path: PathBuf,
        prefetch: bool,
    },

    /// Spill to disk when memory pressure detected
    AdaptiveSpill {
        memory_threshold: usize,
        spill_path: PathBuf,
    },
}

/// Unified trait for graph storage backends
pub trait GraphStore: Send + Sync {
    fn add_vertex(&mut self, vid: Vid, data: VertexData) -> VertexId;
    fn add_edge(&mut self, src: VertexId, dst: VertexId, data: EdgeData) -> EdgeId;
    fn get_neighbors(&self, vid: VertexId, direction: Direction) -> impl Iterator<Item = VertexId>;
    fn vertex_count(&self) -> usize;
    fn edge_count(&self) -> usize;
}
```

**MappedDeltaGraph implementation**:

```rust
use memmap2::{MmapMut, MmapOptions};
use std::fs::OpenOptions;

/// Memory-mapped graph for larger-than-RAM workloads
pub struct MappedDeltaGraph {
    /// Memory-mapped vertex array
    vertices: MmapMut,
    vertex_count: usize,

    /// Memory-mapped edge array
    edges: MmapMut,
    edge_count: usize,

    /// Memory-mapped adjacency lists (CSR format)
    adj_offsets: MmapMut,
    adj_targets: MmapMut,

    /// Small in-memory index for hot data
    hot_vertices: HashSet<Vid>,

    /// File handles
    _files: MappedFiles,
}

struct MappedFiles {
    vertices_file: std::fs::File,
    edges_file: std::fs::File,
    adj_file: std::fs::File,
}

impl MappedDeltaGraph {
    pub fn create(path: &Path, estimated_vertices: usize, estimated_edges: usize) -> Result<Self> {
        // Pre-allocate files based on estimates
        let vertex_size = estimated_vertices * std::mem::size_of::<MappedVertex>();
        let edge_size = estimated_edges * std::mem::size_of::<MappedEdge>();
        let adj_size = (estimated_vertices + 1) * 8 + estimated_edges * 8;

        let vertices_file = OpenOptions::new()
            .read(true).write(true).create(true)
            .open(path.join("vertices.mmap"))?;
        vertices_file.set_len(vertex_size as u64)?;

        let edges_file = OpenOptions::new()
            .read(true).write(true).create(true)
            .open(path.join("edges.mmap"))?;
        edges_file.set_len(edge_size as u64)?;

        let adj_file = OpenOptions::new()
            .read(true).write(true).create(true)
            .open(path.join("adj.mmap"))?;
        adj_file.set_len(adj_size as u64)?;

        // Memory-map the files
        let vertices = unsafe { MmapOptions::new().map_mut(&vertices_file)? };
        let edges = unsafe { MmapOptions::new().map_mut(&edges_file)? };
        let adj = unsafe { MmapOptions::new().map_mut(&adj_file)? };

        Ok(Self {
            vertices,
            vertex_count: 0,
            edges,
            edge_count: 0,
            adj_offsets: adj,  // First portion
            adj_targets: adj,  // Second portion (split internally)
            hot_vertices: HashSet::new(),
            _files: MappedFiles { vertices_file, edges_file, adj_file },
        })
    }

    /// Hint that certain vertices will be accessed frequently
    pub fn mark_hot(&mut self, vids: impl Iterator<Item = Vid>) {
        for vid in vids {
            self.hot_vertices.insert(vid);
            // Prefetch pages containing hot vertices
            self.prefetch_vertex(vid);
        }
    }

    fn prefetch_vertex(&self, vid: Vid) {
        let offset = vid.local_offset() * std::mem::size_of::<MappedVertex>();
        // Use madvise to prefetch
        #[cfg(unix)]
        unsafe {
            let ptr = self.vertices.as_ptr().add(offset);
            libc::madvise(
                ptr as *mut libc::c_void,
                4096, // Page size
                libc::MADV_WILLNEED,
            );
        }
    }
}

/// Compact vertex representation for mmap
#[repr(C)]
struct MappedVertex {
    vid: u64,
    label_id: u32,
    flags: u32,  // deleted, etc.
}

/// Compact edge representation for mmap
#[repr(C)]
struct MappedEdge {
    eid: u64,
    src_vid: u64,
    dst_vid: u64,
    edge_type: u32,
    flags: u32,
}

impl GraphStore for MappedDeltaGraph {
    fn add_vertex(&mut self, vid: Vid, _data: VertexData) -> VertexId {
        let idx = self.vertex_count;
        let vertex = MappedVertex {
            vid: vid.0,
            label_id: vid.label_id() as u32,
            flags: 0,
        };

        // Write directly to mmap
        let offset = idx * std::mem::size_of::<MappedVertex>();
        unsafe {
            let ptr = self.vertices.as_mut_ptr().add(offset) as *mut MappedVertex;
            std::ptr::write(ptr, vertex);
        }

        self.vertex_count += 1;
        VertexId::from(idx)
    }

    fn get_neighbors(&self, vid: VertexId, direction: Direction) -> impl Iterator<Item = VertexId> {
        // Read from mmap - OS handles paging
        let idx = vid.index();
        let (start, end) = self.get_adj_range(idx, direction);

        (start..end).map(move |i| {
            let offset = i * 8;
            let target = unsafe {
                let ptr = self.adj_targets.as_ptr().add(offset) as *const u64;
                std::ptr::read(ptr)
            };
            VertexId::from(target as usize)
        })
    }

    // ... other methods
}
```

**Adaptive spill strategy**:

```rust
/// Automatically spills to disk when memory pressure detected
pub struct AdaptiveGraph {
    /// Hot data in memory
    memory_graph: DeltaGraph,

    /// Cold data on disk
    disk_graph: Option<MappedDeltaGraph>,

    /// Memory threshold (bytes)
    threshold: usize,

    /// Track memory usage
    current_memory: AtomicUsize,
}

impl AdaptiveGraph {
    pub fn new(threshold: usize, spill_path: PathBuf) -> Self {
        Self {
            memory_graph: DeltaGraph::new_directed(),
            disk_graph: None,
            threshold,
            current_memory: AtomicUsize::new(0),
        }
    }

    pub fn add_vertex(&mut self, vid: Vid, data: VertexData) -> VertexId {
        let size = std::mem::size_of_val(&data);
        let new_memory = self.current_memory.fetch_add(size, Ordering::Relaxed) + size;

        if new_memory > self.threshold && self.disk_graph.is_none() {
            // Spill to disk
            self.spill_to_disk();
        }

        if self.disk_graph.is_some() {
            // Add to disk graph
            self.disk_graph.as_mut().unwrap().add_vertex(vid, data)
        } else {
            // Add to memory graph
            self.memory_graph.add_vertex(vid, data)
        }
    }

    fn spill_to_disk(&mut self) {
        // Create mmap file
        let estimated_size = self.memory_graph.vertex_count() * 2; // 2x headroom
        let disk = MappedDeltaGraph::create(
            &self.spill_path,
            estimated_size,
            estimated_size * 10,
        ).expect("Failed to create spill file");

        // Move existing data to disk
        // Keep hot vertices in memory
        self.disk_graph = Some(disk);

        log::warn!(
            "Graph exceeded memory threshold ({}MB), spilling to disk",
            self.threshold / 1_000_000
        );
    }
}
```

**WorkingGraph with mmap fallback**:

```rust
/// WorkingGraph that can use mmap for large subgraphs
pub enum FlexibleWorkingGraph {
    /// Standard in-memory SimpleGraph
    InMemory(WorkingGraph),

    /// Memory-mapped for large subgraphs
    Mapped(MappedDeltaGraph),
}

impl FlexibleWorkingGraph {
    pub fn load_subgraph(
        loader: &StorageManager,
        start_vids: &[Vid],
        max_hops: usize,
        memory_limit: usize,
    ) -> Result<Self> {
        // Estimate subgraph size
        let estimate = loader.estimate_subgraph_size(start_vids, max_hops)?;

        if estimate.memory_bytes <= memory_limit {
            // Fits in memory - use fast path
            let graph = loader.load_subgraph_into_memory(start_vids, max_hops)?;
            Ok(FlexibleWorkingGraph::InMemory(graph))
        } else if estimate.memory_bytes <= memory_limit * 10 {
            // "Slightly too big" - use mmap
            let temp_dir = tempfile::tempdir()?;
            let mut mapped = MappedDeltaGraph::create(
                temp_dir.path(),
                estimate.vertex_count,
                estimate.edge_count,
            )?;

            // Stream data into mmap
            loader.stream_subgraph_to_store(start_vids, max_hops, &mut mapped)?;

            // Prefetch start vertices
            mapped.mark_hot(start_vids.iter().copied());

            Ok(FlexibleWorkingGraph::Mapped(mapped))
        } else {
            // Way too big - must use streaming execution
            Err(Error::SubgraphTooLarge {
                estimated_bytes: estimate.memory_bytes,
                limit_bytes: memory_limit * 10,
                suggestion: "Use LIMIT clause or filter by edge type".to_string(),
            })
        }
    }
}
```

**Performance characteristics**:

| Strategy | Read Latency | Write Latency | Memory Usage | Use Case |
|----------|--------------|---------------|--------------|----------|
| InMemory | ~10ns | ~50ns | 100% of data | Small graphs (<1GB) |
| MappedFile (hot) | ~10ns | ~100ns | Hot set only | Medium graphs (1-50GB) |
| MappedFile (cold) | ~1μs (page fault) | ~10μs | Minimal | Large graphs, random access |
| AdaptiveSpill | Varies | Varies | Up to threshold | Unpredictable workloads |

**Configuration**:

```rust
pub struct GraphMemoryConfig {
    /// Maximum memory for in-memory graphs (default: 1GB)
    pub memory_limit: usize,

    /// Path for mmap files (default: system temp)
    pub spill_path: PathBuf,

    /// Enable adaptive spilling (default: true)
    pub adaptive_spill: bool,

    /// Mmap prefetch strategy
    pub prefetch_strategy: PrefetchStrategy,
}

pub enum PrefetchStrategy {
    /// No prefetching - rely on OS
    None,

    /// Prefetch N-hop neighbors of start vertices
    NHop { hops: usize },

    /// Prefetch based on access pattern prediction
    Adaptive,
}
```

**When to use each strategy**:

| Scenario | Recommended Strategy |
|----------|---------------------|
| Graph fits in RAM | `InMemory` (default) |
| Graph 1-10x RAM, fast SSD | `MappedFile` with prefetch |
| Unpredictable graph sizes | `AdaptiveSpill` |
| Graph >10x RAM | Streaming execution, not WorkingGraph |
| Cloud/serverless (no local SSD) | `InMemory` only, rely on query limits |

---

## Part 10: Indexing

### Primary Index

- **ext_id**: Hash index per label for O(1) lookup
- Maintained automatically, updated with vertex writes

### Secondary Indexes

- Property indexes via Lance scalar indexes
- Created on demand: `CREATE INDEX ON :Person(age)`
- Used for selective predicates

### Vector Indexes

- HNSW or IVF indexes on embedding columns
- Per-label, on vertices with embedding property
- Used for hybrid vector + graph queries

---

## Part 11: Operational Considerations

### Object Store Latency

| Operation | Cold (Object Store) | Warm (SSD) | Hot (Memory) |
|-----------|---------------------|------------|--------------|
| 1-hop (10 neighbors) | 100-200ms | 1-5ms | <1ms |
| 2-hop (100 neighbors) | 500-1000ms | 10-50ms | 1-5ms |
| 3-hop (1000 neighbors) | 2-5s | 50-200ms | 5-20ms |
| Point lookup (ext_id) | 100-200ms | 1-5ms | <1ms |
| Load subgraph → SimpleGraph | Dominated by fetch | 10-100ms | 1-10ms |
| SimpleGraph algorithm (in-memory) | N/A | N/A | <1ms typical |

**Key insight**: Once subgraph is loaded into SimpleGraph, algorithm execution is fast. The bottleneck is I/O.

### Storage Estimates

For a graph with 100M vertices and 1B edges:

| Component | Raw Size | Compressed (~4x) |
|-----------|----------|------------------|
| Vertices (avg 200 bytes) | 20 GB | 5 GB |
| Edges (avg 50 bytes) | 50 GB | 12 GB |
| Adjacency fwd + bwd | 16 GB | 4 GB |
| Indexes | 5 GB | 2 GB |
| **Total** | **~91 GB** | **~23 GB** |

### Memory Usage

| Component | Typical Size |
|-----------|--------------|
| L0 DeltaGraph (100K edges) | ~50 MB |
| WorkingGraph (10K vertices, 100K edges) | ~20 MB |
| L1 cache (adjacency chunks) | ~500 MB |
| Property cache | ~200 MB |

---

## Part 12: Design Trade-offs

| Decision | Trade-off |
|----------|-----------|
| **SimpleGraph for in-memory ops** | Custom implementation, fewer dependencies |
| **DataFusion for relational ops** | Additional dependency, but vectorized execution + Lance integration |
| **Load subgraph → execute** | Memory overhead, but algorithm flexibility |
| **Per-label/type datasets** | Better pruning + indexing, more datasets to manage |
| **Row-per-vertex CSR** | More rows than original design, but better Lance compatibility |
| **Denormalized edge properties** | +50% adjacency storage, but 1000x faster edge filters |
| **Dual L1 runs (FWD + BWD)** | 2x L1 storage, but O(log n) incoming lookups |
| **LSM-style deltas with SimpleGraph L0** | In-memory structure with property separation |
| **Single-label vertices** | Simplicity over flexibility (multi-label deferred) |
| **Global vid with label encoding** | Fast label extraction, limited to 65K labels |

---

## Appendix A: Example Query Execution

### Query

```cypher
MATCH (p:Person {name: "Alice"})-[:KNOWS]->(q:Person)
WHERE q.age > 30
RETURN q.name, q.city
```

### Execution Steps

```
1. Parse → AST

2. Bind:
   - Resolve "Person" → label_id = 1
   - Resolve "KNOWS" → type_id = 1
   - Resolve "Alice" → ext_id lookup → vid = 0x0001000000000042

3. Plan:
   - IndexLookup(Person.name = "Alice") → p.vid
   - Expand(p, KNOWS, OUT) → q.vid
   - Filter(q.age > 30)
   - Project(q.name, q.city)

4. Load subgraph:
   - Fetch 1-hop neighborhood of alice_vid for KNOWS
   - Merge L0 + L1 + L2
   - Build WorkingGraph with ~50 vertices

5. Execute on SimpleGraph:
   let neighbors = graph.get_neighbors(alice_vid, Direction::Outgoing);
   // Filter by edge type and collect results

6. Property fetch:
   - Batch read from vertices_person
   - Filter age > 30: 23 remain
   - Extract name, city

7. Return 23 rows
```

---

## Part 10: Python Bindings and Embedding Services

### Python Bindings (pyuni)

Uni provides comprehensive Python bindings via PyO3, offering the same functionality as the Rust API.

**Installation**:
```bash
pip install pyuni
```

**Key Features**:
- Full async/await support using Python asyncio
- Type hints for IDE autocomplete
- DataFrame interoperability via PyArrow

**Example Usage**:
```python
import asyncio
from pyuni import Uni, SchemaBuilder

async def main():
    # Create/open a database
    db = await Uni.open("my_graph")

    # Define schema
    schema = SchemaBuilder() \
        .label("Person") \
        .property("name", "string", nullable=False) \
        .property("age", "int32") \
        .edge_type("KNOWS", "Person", "Person") \
        .build()
    await db.apply_schema(schema)

    # Write data
    writer = await db.writer()
    await writer.create_vertex("Person", "alice", {"name": "Alice", "age": 30})
    await writer.create_vertex("Person", "bob", {"name": "Bob", "age": 25})
    await writer.create_edge("KNOWS", "alice", "bob", {"since": "2024-01-01"})
    await writer.commit()

    # Query
    results = await db.query("""
        MATCH (p:Person)-[:KNOWS]->(q:Person)
        RETURN p.name, q.name
    """)
    print(results.to_pandas())

asyncio.run(main())
```

### Embedding Services

Uni supports automatic embedding generation for vector search via multiple backends.

**Supported Backends**:

| Backend | Type | Notes |
|---------|------|-------|
| Candle | Local | Default, native Rust (HuggingFace Candle) |
| FastEmbed | Local | Optional, ONNX-based (requires `fastembed` feature) |
| OpenAI | Cloud | Planned, requires `OPENAI_API_KEY` |
| Ollama | Local | Planned, self-hosted |

**Configuration**:
```rust
// Rust API - Candle (default)
let config = UniConfig::builder()
    .embedding_service(EmbeddingService::Candle {
        model: "all-MiniLM-L6-v2".to_string(),
        revision: None,
    })
    .build();

// Python API
config = UniConfig(
    embedding_service="candle",
    embedding_model="all-MiniLM-L6-v2"
)
```

**Automatic Embedding on Insert**:
```python
# Configure auto-embed for a property
schema = SchemaBuilder() \
    .label("Document") \
    .property("content", "string") \
    .auto_embed("content", dimensions=384) \
    .build()

# Embeddings generated automatically on insert
await writer.create_vertex("Document", "doc1", {
    "content": "Graph databases are powerful..."
})  # embedding computed and stored

# Vector search (with auto-embedding from text)
results = await db.query("""
    CALL uni.vector.query('Document', 'embedding', $query, 10)
    YIELD node, score
    RETURN node.title, score
""", params={"query": "database performance"})

# Full-text search with BM25 scoring
fts_results = await db.query("""
    CALL uni.fts.query('Document', 'content', 'graph algorithms', 20)
    YIELD node, score
    RETURN node.title, score
""")

# Hybrid search combining vector + FTS with RRF fusion
hybrid_results = await db.query("""
    CALL uni.search(
        'Document',
        {vector: 'embedding', fts: 'content'},
        'machine learning optimization',
        null,
        10
    )
    YIELD node, score, vector_score, fts_score
    RETURN node.title, score
""")
```

---

## Appendix A: Trade-offs Summary

### Feature Interactions

| Feature | Status | Notes |
|---------|--------|-------|
| VID-based storage | ✅ | Core identity, O(1) lookup |
| UID content-addressing | ✅ | SHA3-256, indexed for lookup |
| CRDT data types | ✅ | Conflict-free concurrent updates |
| Vector search | ✅ | IVF-PQ indexes via LanceDB |
| Bi-temporal queries | ⏳ | Validity intervals with VALID_AT |
| Schema extensibility | ✅ | Per-label Lance datasets |

### Architecture Guarantees

1. **Single-writer maintained**: One Writer per graph instance
2. **Snapshot isolation preserved**: Readers never see uncommitted L0
3. **VID-based storage**: UID is additive, not a replacement for VID
4. **Cypher compatible**: Extensions follow OpenCypher patterns

---

## Appendix B: Implementation Phases

### Phase 1: Foundation ✅ COMPLETE

- [x] VID/EID encoding utilities (uni-common)
- [x] SimpleGraph implementation: custom lightweight graph
- [x] Basic L0Buffer with SimpleGraph
- [x] Lance vertex/edge dataset read/write

**Testing milestone:**
- [x] Unit tests for VID/EID encode/decode roundtrip
- [x] Unit tests for SimpleGraph construction and traversal
- [x] Integration test: write vertices/edges to Lance, read back

### Phase 2: Storage Layer ✅ COMPLETE

- [x] Chunked adjacency datasets (CSR format)
- [x] Delta datasets with L1 sorted runs
- [x] Snapshot manifest read/write
- [x] Storage manager with L0+L1 merge
- [x] L0Buffer with separate property storage (topology-only SimpleGraph)

**Testing milestone:**
- [ ] Unit tests for adjacency chunk lookup (verify no cross-label collisions)
- [ ] Unit tests for L0+L1+L2 merge with version conflict resolution
- [ ] Test parallel edges: multiple edges between same vertices preserved
- [ ] Test tombstones: deleted edges not returned after merge
- [ ] Test incoming edge lookup uses BWD runs (O(log n), not O(n) scan)
- [ ] Integration test: snapshot create → read → verify consistency

**Property-Based Testing (CRITICAL for LSM correctness):**

The LSM merge logic is notoriously hard to get right. Use `proptest` for exhaustive testing:

```rust
use proptest::prelude::*;

/// Generate random mutation sequences
fn mutation_strategy() -> impl Strategy<Value = Vec<Mutation>> {
    prop::collection::vec(
        prop_oneof![
            (any::<Vid>(), any::<Vid>(), any::<EdgeType>()).prop_map(|(s, d, t)|
                Mutation::InsertEdge { src: s, dst: d, edge_type: t }),
            any::<Eid>().prop_map(|e| Mutation::DeleteEdge { eid: e }),
            any::<Eid>().prop_map(|e| Mutation::UpdateEdge { eid: e }),
        ],
        0..1000
    )
}

proptest! {
    #[test]
    fn lsm_merge_correctness(mutations in mutation_strategy()) {
        // 1. Apply mutations to a simple HashMap (ground truth)
        let mut expected = HashMap::new();
        for m in &mutations {
            apply_to_hashmap(&mut expected, m);
        }

        // 2. Apply mutations to LSM (L0 → L1 → L2)
        let mut db = TestDatabase::new();
        for m in &mutations {
            db.apply(m)?;
            if db.should_flush() {
                db.flush_l0_to_l1()?;
            }
            if db.should_compact() {
                db.major_compact()?;
            }
        }

        // 3. Verify: read(db) == expected
        for (eid, expected_edge) in &expected {
            let actual = db.get_edge(*eid)?;
            prop_assert_eq!(actual, Some(expected_edge.clone()));
        }

        // 4. Verify: no zombie edges
        for eid in db.all_eids() {
            prop_assert!(expected.contains_key(&eid), "Zombie edge: {}", eid);
        }
    }

    #[test]
    fn tombstone_ordering(mutations in mutation_strategy()) {
        // Specific test: delete in L0, insert in L1 with older version
        // The delete should win (higher version)
    }

    #[test]
    fn denormalized_property_consistency(mutations in mutation_strategy()) {
        // Verify: L2 denormalized values match L0/L1 truth after read
    }
}
```

**Why Property-Based Testing?**
- Unit tests cover expected cases; property tests find unexpected edge cases
- Example bug found by proptest: "Edge deleted in L0, re-inserted in L1 with same eid but lower version" → delete should win, but naive version comparison failed
- Run with `PROPTEST_CASES=100000` in CI

### Phase 3: Query Execution ✅ COMPLETE

- [x] Cypher parser (declared subset: MATCH, WHERE, RETURN, CREATE, SET, DELETE)
- [x] Parser error messages with unsupported feature identification
- [x] Subgraph loader (storage → SimpleGraph WorkingGraph)
- [x] Pattern matching on SimpleGraph (directed edge semantics)
- [x] DataFusion integration for relational operations:
  - [x] WHERE clause predicate evaluation
  - [x] RETURN clause projection and expressions
  - [x] ORDER BY, LIMIT, SKIP
  - [x] Aggregation operators (COUNT, SUM, AVG, COLLECT, etc.)
- [x] Lazy property fetch via PropertyManager with caching
- [ ] Denormalized property filter pushdown (partial)

**Testing milestone:**
- [x] Unit tests for parser on supported/unsupported Cypher
- [x] Test directed vs undirected pattern matching
- [x] Test aggregation queries
- [x] Integration test: end-to-end query against known graph

### Phase 4: Algorithms ✅ COMPLETE

- [x] Implement shortest paths (uni-algo)
- [x] Implement BFS, DFS (uni-algo)
- [x] PageRank, WCC algorithms
- [ ] Variable-length path support (partial)

**Testing milestone:**
- [ ] Test shortest path on known graphs with expected results
- [ ] Test BFS/DFS traversal order
- [ ] Test variable-length paths respect min/max bounds

### Phase 5: Writes and Compaction ✅ COMPLETE

- [x] WAL for L0 durability (LSN-based with idempotent recovery)
- [x] L0 → L1 flush
- [x] Compaction support
- [x] Tombstone handling

**Testing milestone:**
- [x] Crash recovery test: verify WAL replay reconstructs L0
- [x] Compaction correctness: verify no data loss

### Phase 6: Caching and Performance ✅ COMPLETE

- [x] Adjacency cache with configurable size
- [x] Property cache (LRU with configurable capacity)
- [x] Dataset cache (DashMap for concurrent access)
- [ ] Prefetching during traversal (partial)

**Testing milestone:**
- [x] Cache hit/miss behavior verified
- [x] Cache invalidation on flush

### Phase 7: Vector Integration ✅ COMPLETE

- [x] Vector indexes (IvfPq, Hnsw, Flat)
- [x] Multiple distance metrics (Cosine, L2, Dot)
- [x] VectorSearch via CALL uni.vector.query()
- [x] Auto-embed text queries using index embedding config
- [x] Embedding services (FastEmbed, OpenAI, Ollama)

**Testing milestone:**
- [x] Test k-NN returns correct neighbors
- [x] Test hybrid query: vector search + graph filter

### Phase 8: Document Storage & Hybrid Search ✅ COMPLETE

- [x] JSON property storage
- [x] Full-text search indexes (Lance FTS)
- [x] Inverted indexes
- [x] FTS query procedure: CALL uni.fts.query() with BM25 scoring
- [x] Hybrid search: CALL uni.search() with RRF/weighted fusion
- [ ] Document collection API (planned)

**Testing milestone:**
- [x] Test JSON property read/write
- [x] Test full-text search queries
- [x] Test hybrid search with rank fusion

---

## Future Extensions

These features are explicitly **out of scope for V1** but documented here for future consideration.

### Distributed Writer Exclusivity (Object Store Deployments)

**Motivation**: V1 does not enforce single-writer across processes for object store deployments. Users must ensure singleton deployment externally. A future `LockManager` trait could provide automatic cross-process exclusivity.

**Status**: Deferred. V1 requires user-managed singleton deployment for object stores.

**V1 Constraint Recap**:
- Local disk: File lock enforced automatically
- Object store: **User responsibility** to ensure single writer (Lambda singleton, K8s StatefulSet replicas=1, etc.)
- CAS is a misconfiguration fence, not a coordination mechanism

**Future Option**: External lease service integration

```rust
/// Future: Cross-process writer exclusivity for object stores
#[async_trait]
pub trait WriterLease: Send + Sync {
    /// Acquire exclusive writer lease for a graph
    /// Only one process can hold the lease at a time
    async fn acquire(&self, graph_id: &str, timeout: Duration) -> Result<LeaseGuard>;

    /// Renew lease (called periodically while writer is active)
    async fn renew(&self, graph_id: &str) -> Result<()>;
}

/// Potential implementations:
/// - DynamoDbLease: AWS DynamoDB conditional writes with TTL
/// - RedisLease: Redis SET NX with TTL + renewal
/// - EtcdLease: etcd lease with keep-alive
```

**Trade-offs** (for future reference):

| Strategy | Latency Overhead | Dependencies | Failure Mode |
|----------|-----------------|--------------|--------------|
| User-managed (V1) | None | None | User error → data corruption |
| DynamoDB | +10-50ms per commit | DynamoDB | Lease expires, new writer can acquire |
| Redis | +1-5ms per commit | Redis | Lease expires, new writer can acquire |
| etcd | +5-20ms per commit | etcd cluster | Lease expires, new writer can acquire |

**When to revisit**: If users need automatic cross-process exclusivity for object store deployments without external orchestration.

---

## References

- Kuzu/Ladybug: Columnar storage + CSR adjacency, factorized query processing
- GraphAr: Chunked vertex/edge format with CSR/CSC offsets
- Lance: Columnar format with versioning and vector indexes
- LSM-trees: Write-optimized storage with leveled compaction
- FastEmbed: Local embedding model inference
- Arrow/DataFusion: Columnar data processing and query execution
