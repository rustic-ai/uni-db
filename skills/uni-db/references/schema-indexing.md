# Schema & Indexing Reference

## 1. Identity System

Every entity has two identifiers plus an optional user-facing key:

| Identity | Type | Bits | Purpose | Scope |
|----------|------|------|---------|-------|
| **VID** | `u64` auto-increment | 64 | O(1) array indexing, primary key in Lance tables | Local to database |
| **EID** | `u64` auto-increment | 64 | Uniquely identifies edges (parallel edges allowed) | Local to database |
| **UniId** | SHA3-256 hash | 256 | Content-addressed lookup, cross-system sync, provenance | Global / distributed |
| **ext_id** | `String` | Variable | Optional user-provided external key | Globally unique across labels |

### ext_id

Optional user-provided string key, globally unique across labels. MERGE can also match ordinary pattern properties without an `ext_id`.

```cypher
CREATE (n:Person {ext_id: 'user-123', name: 'Alice'})
MATCH (n:Person {ext_id: 'user-123'}) RETURN n
```

### VID

Dense, sequential, never reused. Serves as `_vid` column in all Lance tables and as array offsets in CSR adjacency structures. Sentinel: `Vid::INVALID = u64::MAX`.

```rust
use uni_db::core::Vid;
let vid = Vid::new(42);  // dense id; labels are resolved via VidLabelsIndex
```

### UniId

Content-addressed identifier. Lookup index, **not** a uniqueness constraint -- multiple vertices may share a UID.

- **Computation**: `SHA3-256(label || ext_id || sorted_properties)`
- **Encoding**: 53-character Base32Lower multibase string (b-prefix)
- **Lookup**: `MATCH (n:Person) WHERE n._uid = $uid RETURN n`

### EID

Same dense `u64` auto-increment shape as VID; neither encodes a label or edge-type id. Sentinel: `Eid::INVALID = u64::MAX`.

### When to Use Each

| Use Case | Recommended ID |
|----------|----------------|
| Internal operations, array indexing | VID |
| Cross-system sync, provenance tracking | UniId |
| User-facing lookups, MERGE operations | ext_id |

---

## 2. Data Types

### Primitive Types

| Uni Type | Arrow Type | Cypher DDL | Python Factory | Rust Enum |
|----------|-----------|------------|----------------|-----------|
| `String` | `Utf8` | `STRING` | `DataType.STRING()` | `DataType::String` |
| `Int32` | `Int32` | `INTEGER` | `DataType.INT32()` | `DataType::Int32` |
| `Int64` (alias: `Int`) | `Int64` | `INTEGER` | `DataType.INT64()` | `DataType::Int64` |
| `Float32` | `Float32` | `FLOAT` | `DataType.FLOAT32()` | `DataType::Float32` |
| `Float64` (alias: `Float`) | `Float64` | `FLOAT` | `DataType.FLOAT64()` | `DataType::Float64` |
| `Bool` | `Boolean` | `BOOLEAN` | `DataType.BOOL()` | `DataType::Bool` |

### Temporal Types

| Uni Type | Arrow Type | Cypher DDL | Rust Enum |
|----------|-----------|------------|-----------|
| `Timestamp` | `Timestamp(Nanosecond, UTC)` | `TIMESTAMP` | `DataType::Timestamp` |
| `Date` | `Date32` | `DATE` | `DataType::Date` |
| `Time` | Struct(nanos_since_midnight, offset_seconds) | `TIME` | `DataType::Time` |
| `DateTime` | Struct(nanos_since_epoch, offset_seconds, timezone_name) | `DATETIME` | `DataType::DateTime` |
| `Duration` | `LargeBinary` (CypherValue codec) | `DURATION` | `DataType::Duration` |
| `Btic` | `FixedSizeBinary(24)` | `BTIC` | `DataType::Btic` |

**BTIC (Binary Temporal Interval Codec):** Encodes half-open temporal intervals `[lo, hi)` as 24 bytes with per-bound granularity and certainty. Construct with `btic('literal')`: `btic('1985')` (year), `btic('1985-03/2024-06')` (range), `btic('~1985')` (approximate), `btic('2020-03/')` (ongoing). Supports comparison operators (`<`, `>`, `<=`, `>=`, `=`, `<>`) and 30+ dedicated functions (`btic_lo`, `btic_overlaps`, `btic_contains_point`, `btic_span`, etc.).

### Complex Types

| Uni Type | Arrow Type | Cypher DDL | Rust Enum |
|----------|-----------|------------|-----------|
| `Vector { dimensions }` | `FixedSizeList(Float32, N)` | `VECTOR(N)` | `DataType::Vector { dimensions: N }` |
| `List(T)` | `List(T)` | `LIST(T)` | `DataType::List(Box<DataType>)` |
| `Map(K, V)` | `List(Struct(key, value))` | `MAP(K, V)` | `DataType::Map(Box<DataType>, Box<DataType>)` |
| `CypherValue` | `LargeBinary` | -- | `DataType::CypherValue` |
| `Bytes` | `LargeBinary` | `BYTES` (also `BLOB`, `BINARY`) | `DataType::Bytes` |

### Spatial Types

| Uni Type | Arrow Type | Rust Enum |
|----------|-----------|-----------|
| `Point(Geographic)` | Struct(latitude, longitude, crs: Float64) | `DataType::Point(Geographic)` |
| `Point(Cartesian2D)` | Struct(x, y, crs: Float64) | `DataType::Point(Cartesian2D)` |
| `Point(Cartesian3D)` | Struct(x, y, z, crs: Float64) | `DataType::Point(Cartesian3D)` |

### CRDT Types

| Uni Type | Arrow Storage | Rust Enum |
|----------|--------------|-----------|
| `Crdt(GCounter)` | `Binary` (MessagePack) | `DataType::Crdt(CrdtType::GCounter)` |
| `Crdt(GSet)` | `Binary` (MessagePack) | `DataType::Crdt(CrdtType::GSet)` |
| `Crdt(ORSet)` | `Binary` (MessagePack) | `DataType::Crdt(CrdtType::ORSet)` |
| `Crdt(LWWRegister)` | `Binary` (MessagePack) | `DataType::Crdt(CrdtType::LWWRegister)` |
| `Crdt(LWWMap)` | `Binary` (MessagePack) | `DataType::Crdt(CrdtType::LWWMap)` |
| `Crdt(Rga)` | `Binary` (MessagePack) | `DataType::Crdt(CrdtType::Rga)` |
| `Crdt(VectorClock)` | `Binary` (MessagePack) | `DataType::Crdt(CrdtType::VectorClock)` |
| `Crdt(VCRegister)` | `Binary` (MessagePack) | `DataType::Crdt(CrdtType::VCRegister)` |

---

## 3. Schema Definition

### Cypher DDL

```cypher
// Create labels with typed properties
CREATE LABEL Person (
    name STRING,
    age INTEGER,
    email STRING UNIQUE
)

CREATE LABEL Document (
    title STRING,
    content STRING,
    embedding VECTOR(384)
)

// Create edge types with source/destination constraints
CREATE EDGE TYPE KNOWS (since DATE,
    weight FLOAT) FROM Person TO Person

CREATE EDGE TYPE AUTHORED FROM Person TO Document
```

### Rust API (SchemaBuilder)

```rust
use uni_db::{Uni, DataType, IndexType, ScalarType};

db.schema()
    .label("Person")
        .property("name", DataType::String)
        .property("age", DataType::Int64)
        .property_nullable("email", DataType::String)
        .index("name", IndexType::Scalar(ScalarType::BTree))
    .label("Document")
        .property("title", DataType::String)
        .property("content", DataType::String)
        .vector("embedding", 384)
    .edge_type("KNOWS", &["Person"], &["Person"])
        .property("since", DataType::Date)
        .property("weight", DataType::Float64)
    .apply().await?;
```

### Python API (SchemaBuilder)

```python
from uni_db import Uni, DataType

db = Uni.open("./my-graph")

db.schema() \
    .label("Person") \
        .property("name", DataType.STRING()) \
        .property("age", DataType.INT64()) \
        .property_nullable("email", DataType.STRING()) \
        .vector("embedding", 384) \
    .done() \
    .edge_type("KNOWS", ["Person"], ["Person"]) \
        .property("since", DataType.DATE()) \
        .property("weight", DataType.FLOAT64()) \
    .apply()
```

### Schemaless Properties (Overflow)

Properties not in the schema are stored in an `overflow_json` JSONB column. Queries are automatically rewritten to use JSONB extraction functions.

| Operation | Schema Properties | Overflow Properties |
|-----------|------------------|---------------------|
| WHERE filtering | ~2-3ms (columnar) | ~5-10ms (JSONB parse) |
| ORDER BY | Fast (native sort) | Slower (extract + sort) |
| Indexing | Supported | Not supported |
| Schema changes | Migration required | No migration |

---

## 4. CRDT Types

All CRDTs implement `CrdtMerge` (commutative, associative, idempotent). Serialized as MessagePack. On upsert, Uni auto-merges CRDT properties.

| CRDT | Structure | Merge Semantics | Use Case |
|------|-----------|-----------------|----------|
| **GCounter** | `HashMap<ActorId, u64>` | max per actor; value = sum | Page views, event counts |
| **GSet** | `HashSet<String>` | Set union | Append-only tags, immutable labels |
| **ORSet** | Elements with UUID tags | Add-wins (concurrent add+remove = present) | Shopping carts, mutable selections |
| **LWWRegister** | `(value, timestamp)` | Highest timestamp wins | Profile fields, config values |
| **LWWMap** | `HashMap<K, (V, timestamp)>` | Per-key highest timestamp wins | Property maps, key-value preferences |
| **Rga** | Ordered sequence with position IDs | Interleave by position ID | Collaborative text, ordered lists |
| **VectorClock** | `HashMap<ActorId, u64>` | Pointwise maximum | Causal ordering, happened-before tracking |
| **VCRegister** | `(value, VectorClock)` | Causally dominant wins; concurrent = keep self | Causal consistency without clock sync |

### VCRegister MergeResult

| Result | Meaning |
|--------|---------|
| `KeptSelf` | Self was causally newer or equal |
| `TookOther` | Other was causally newer |
| `Concurrent` | Concurrent updates; kept self's value, merged clocks |

---

## 5. Index Types

### Scalar Indexes

| Type | Best For | Query Pattern | Creation |
|------|----------|---------------|----------|
| **BTree** | Range queries, ordering, prefix | `WHERE x > 5`, `STARTS WITH` | Default. `CREATE INDEX idx ON Label (prop)` |
| **Hash** | Exact match lookups | `WHERE x = 123` | Rust: `ScalarType::Hash` |
| **Bitmap** | Low-cardinality columns (< 1000 distinct values) | `WHERE status = 'active'` | DDL: `"BITMAP"`, Rust: `ScalarType::Bitmap` |
| **LabelList** | List columns with containment queries | `array_contains_any`, `array_contains_all` | DDL: `"LABEL_LIST"`, Rust: `ScalarType::LabelList` |

```cypher
CREATE INDEX idx_name FOR (p:Person) ON (p.name)          // Default: BTree

// Via procedure API
CALL uni.schema.createIndex('Event', 'status', {"type": "BITMAP"})
CALL uni.schema.createIndex('Doc', 'tags', {"type": "LABEL_LIST"})
```

```rust
db.schema().label("User")
    .property("email", DataType::String)
    .index("email", IndexType::Scalar(ScalarType::BTree))
    .apply().await?;
```

### Vector Indexes

8 algorithm variants with different quantization strategies:

| Type | Quantization | Best For | Key Parameters |
|------|-------------|----------|----------------|
| **Flat** | None | < 10k vectors, exact search | — |
| **IVF-Flat** | None | Medium datasets, exact within partitions | `partitions` |
| **IVF-SQ** | Scalar (int8) | Large datasets, good recall/memory tradeoff | `partitions` |
| **IVF-PQ** | Product | Very large datasets, minimum memory | `partitions`, `sub_vectors`, `num_bits` |
| **IVF-RQ** | RaBitQ | Better accuracy than PQ at similar compression | `partitions`, `num_bits` |
| **HNSW-Flat** | None | Exact graph search, no compression loss | `m`, `ef_construction`, `partitions` |
| **HNSW-SQ** | Scalar (int8) | Default. Best recall-latency tradeoff | `m`, `ef_construction`, `partitions` |
| **HNSW-PQ** | Product | Large datasets with graph speed + compression | `m`, `ef_construction`, `sub_vectors`, `partitions` |

```cypher
// HNSW-SQ (default if no type specified)
CREATE VECTOR INDEX idx_embed FOR (d:Document) ON (d.embedding)
  WITH { metric: 'cosine', type: 'hnsw_sq' }

// HNSW-Flat (no quantization, exact graph search)
CREATE VECTOR INDEX idx_embed FOR (d:Document) ON (d.embedding)
  WITH { metric: 'cosine', type: 'hnsw_flat' }

// HNSW-SQ with IVF partitions for very large datasets
CREATE VECTOR INDEX idx_embed FOR (d:Document) ON (d.embedding)
  WITH { metric: 'cosine', type: 'hnsw_sq', partitions: 32 }

// IVF-RQ (RaBitQ, 1-bit per dimension by default)
CREATE VECTOR INDEX idx_embed FOR (d:Document) ON (d.embedding)
  WITH { metric: 'cosine', type: 'ivf_rq', partitions: 256 }

// IVF-PQ for memory-constrained large datasets
CREATE VECTOR INDEX idx_embed FOR (d:Document) ON (d.embedding)
  WITH { metric: 'l2', type: 'ivf_pq', partitions: 256, sub_vectors: 16 }
```

**Distance Metrics:**

| Metric | Raw Distance | Score Conversion | Range | Best For |
|--------|-------------|------------------|-------|----------|
| `Cosine` | `1.0 - cos(a,b)` [0,2] | `(2.0 - d) / 2.0` | [0,1] | Normalized embeddings (most models) |
| `L2` | Squared Euclidean | `1.0 / (1.0 + d)` | (0,1] | Raw embeddings, spatial data |
| `Dot` | Negative dot product | Pass-through | Unbounded | Maximum inner product search |

### Full-Text Index (BM25)

```cypher
CREATE FULLTEXT INDEX idx_content FOR (a:Article) ON EACH [a.content]

CALL uni.fts.query('Article', 'content', 'graph database', 10)
YIELD node, score
```

### JSON FTS Index

Full-text search on nested JSON/JSONB properties:

```cypher
CREATE JSON FULLTEXT INDEX idx_meta FOR (d:Data) ON metadata
```

### Inverted Index

Term-to-VID mapping for list containment queries (`ANY(x IN list WHERE x IN allowed)`). Memory guard: 256 MB default.

---

## 6. Predicate Pushdown Priority

The query planner pushes predicates in this priority order:

| Priority | Index | Lookup | Complexity |
|----------|-------|--------|------------|
| 1 | **UID Index** | `n._uid = '...'` | O(1) hash |
| 2 | **Scalar Index** | `n.prop = value`, `n.prop > value` | O(log N) |
| 3 | **Full-Text Index** | `CONTAINS 'term'` | BM25 scored |
| 4 | **Lance Columnar** | Arrow predicate pushdown | Columnar scan |
| 5 | **Residual Filter** | Post-scan evaluation | O(N) |

Resolution performance:

| Lookup Type | Typical Latency |
|-------------|-----------------|
| VID direct | ~10us |
| UniId / ext_id (BTree) | ~100us |
| Scalar index | ~100us |
| Full scan | Varies with N |

---

## 7. Schema Design Principles

### One Label Per Entity Type

Each label maps to a Lance table. Keep entities separate.

<!-- doctest: skip -->
```cypher
// GOOD
CREATE LABEL Person ( name STRING, age INTEGER )
CREATE LABEL Company ( name STRING, founded DATE )

// BAD: mega-label mixing entity types
CREATE LABEL Entity ( type STRING, name STRING, age INTEGER, founded DATE )
```

### Directional Edge Types

Use verb phrases in UPPER_SNAKE_CASE that read naturally.

```cypher
CREATE EDGE TYPE WORKS_AT (since DATE) FROM Person TO Company
CREATE EDGE TYPE PURCHASED (quantity INTEGER) FROM Customer TO Product
```

### Property Type Selection Guide

| Use Case | Recommended Type | Why |
|----------|-----------------|-----|
| Short text (names, codes) | `String` | Indexable, searchable |
| Counts, IDs | `Int64` | Range queries, aggregations |
| Measurements, scores | `Float64` | IEEE 754, aggregation-friendly |
| Timestamps | `Timestamp` | Nanosecond precision, UTC |
| Embeddings | `Vector{N}` | Fixed-size, vector-indexable |
| Distributed counters | `Crdt(GCounter)` | Merge-friendly increments |
| Tag collections | `Crdt(GSet)` | Add-only, merge-friendly |
| Mutable sets | `Crdt(ORSet)` | Add/remove with add-wins |
| Feature maps | `Map(String, Float64)` | Structured key-value |
| Multi-value fields | `List(String)` | Variable-length lists |

### Multi-Label Vertices

Vertices can carry multiple labels. Each additional label stores the vertex in one more Lance table.

```cypher
CREATE (n:Person:Employee {name: 'Alice', employee_id: 'E001'})
MATCH (n:Employee) RETURN n.name  // finds it
MATCH (n:Person) RETURN n.name    // also finds it
```

Limit to 2-3 labels per vertex to avoid excessive storage duplication.

### Nullable Property Strategy

- Mark required properties `nullable: false`
- Adding a nullable property requires **no data rewrite** -- existing rows return NULL
- Use `.property_nullable()` in builders or omit `NOT NULL` in DDL

### Naming Conventions

| Element | Convention | Example |
|---------|-----------|---------|
| Labels | Singular PascalCase | `:Person`, `:ResearchPaper` |
| Edge types | UPPER_SNAKE_CASE verbs | `:AUTHORED_BY`, `:WORKS_AT` |
| Properties | snake_case | `created_at`, `citation_count` |

---

## 8. Schema & Index Anti-Patterns

| Anti-Pattern | Problem | Solution |
|-------------|---------|----------|
| **Over-labeling** | Vertex duplicated across too many tables | Limit to 2-3 labels per vertex |
| **Mega-nodes** | Vertices with millions of edges | Introduce intermediate nodes or edge bucketing |
| **Missing indexes** | Full scans on filtered properties | Index every property used in WHERE |
| **Strings for numbers** | No range queries or aggregations | Use Int64/Float64 for numeric data |
| **Large blobs as properties** | Bloats Lance tables, slows scans | Store blobs externally, keep references |
| **Schemaless everything** | Overflow JSONB loses columnar benefits | Define schema for frequently-queried properties |
| **Over-indexing** | Index maintenance cost on every write | Only index properties used in queries |
| **Wrong distance metric** | Cosine on unnormalized vectors = poor results | Check embedding model docs |
| **Vector index without enough data** | HNSW/IVF need minimum data | Use Flat for < 10k rows |
| **Relationships as properties** | Loses graph traversal power | Use edges: `(a)-[:KNOWS]->(b)` not `a.friend_ids` |
| **Labels for states** | Unstable label categories | Use a property: `status: 'draft'` not `:DraftPaper` |
| **Frequently-queried overflow** | Slow JSONB parsing on every row | Promote to schema property |

---

## 9. ALTER Schema

```cypher
// Add property (nullable, no data rewrite)
ALTER LABEL Person ADD PROPERTY phone STRING

// Drop property (soft-delete)
ALTER LABEL Person DROP PROPERTY age

// Rename property
ALTER LABEL Person RENAME PROPERTY name TO full_name

// Add edge property
ALTER EDGE TYPE KNOWS ADD PROPERTY since DATE

// Drop label / edge type (soft-delete)
DROP LABEL IF EXISTS TempData
DROP EDGE TYPE IF EXISTS OLD_RELATION

// Add / drop indexes
CREATE INDEX idx_name FOR (p:Person) ON (p.name)
DROP INDEX idx_name
```

**Breaking changes (avoid -- require data migration):**
- Changing property types
- Changing vector dimensions
- Renaming labels (ID is fixed)

---

## 10. Schema Introspection

### Cypher Procedures

```cypher
CALL uni.schema.labels()
YIELD label, propertyCount, nodeCount, indexCount

CALL uni.schema.edgeTypes()
YIELD type, propertyCount, sourceLabels, targetLabels

CALL uni.schema.labelInfo('Person')
YIELD property, dataType, nullable, indexed, unique

CALL uni.schema.indexes()
YIELD name, type, label, state, properties

CALL uni.schema.constraints()
YIELD name, type, enabled, properties, target
```

### SHOW Commands

```cypher
SHOW INDEXES        // All indexes with status
SHOW CONSTRAINTS    // All constraints
SHOW DATABASE       // Database metadata
```

### Python API

```python
labels = db.list_labels()                    # ["Person"]
info = db.get_label_info("Person")           # LabelInfo
edge_info = db.get_edge_type_info("KNOWS")   # EdgeTypeInfo

db.indexes().list()
db.indexes().rebuild("Person", background=True)
```

**LabelInfo fields:** `name`, `count`, `properties: list[PropertyInfo]`, `indexes: list[IndexInfo]`, `constraints: list[ConstraintInfo]`

`count` on both `LabelInfo` and `EdgeTypeInfo` is computed with a Cypher `count()`, so it includes rows still in the L0 buffers (no flush needed) and excludes tombstoned/superseded versions. It is **not** the same number as `nodeCount` from `CALL uni.schema.labels()`, which is still a raw storage row count: flushed rows only, tombstones and superseded versions included. The name is backtick-quoted in the generated query, so punctuation (`.` included), leading digits and non-ASCII names count correctly; a name containing a backtick is refused with an error, since Cypher's quoted-identifier syntax has no escape for it.

**PropertyInfo fields:** `name`, `data_type`, `nullable`, `is_indexed`

**IndexInfo fields:** `name`, `index_type`, `properties: list[str]`, `status`

---

## 11. Examples

### Social Network Schema

```cypher
CREATE LABEL Person (
    name STRING,
    email STRING UNIQUE,
    age INTEGER,
    embedding VECTOR(384)
)

CREATE LABEL Company ( name STRING, founded DATE )

CREATE EDGE TYPE KNOWS (since DATE, weight FLOAT) FROM Person TO Person
CREATE EDGE TYPE WORKS_AT (since DATE) FROM Person TO Company

CREATE INDEX idx_person_name FOR (p:Person) ON (p.name)
CREATE VECTOR INDEX idx_person_embed FOR (p:Person) ON (p.embedding) OPTIONS { metric: 'cosine', type: 'hnsw' }
```

### Document / RAG Schema

```cypher
CREATE LABEL Document (
    title STRING,
    content STRING,
    embedding VECTOR(768)
)

CREATE LABEL Chunk (
    text STRING,
    embedding VECTOR(768),
    position INTEGER
)

CREATE EDGE TYPE CONTAINS FROM Document TO Chunk
CREATE EDGE TYPE SIMILAR_TO (score FLOAT) FROM Chunk TO Chunk

CREATE VECTOR INDEX idx_chunk_embed FOR (c:Chunk) ON (c.embedding) OPTIONS { metric: 'cosine', type: 'hnsw' }
CREATE FULLTEXT INDEX idx_chunk_text FOR (c:Chunk) ON EACH [c.text]
```

### IoT Sensor Schema

```cypher
CREATE LABEL Device (
    ext_id STRING,
    name STRING,
    type STRING
)

CREATE LABEL Reading (
    timestamp TIMESTAMP,
    value FLOAT,
    unit STRING
)

CREATE LABEL Location ( name STRING, coordinates STRING )

CREATE EDGE TYPE RECORDED FROM Device TO Reading
CREATE EDGE TYPE LOCATED_AT FROM Device TO Location

CREATE INDEX idx_reading_ts FOR (r:Reading) ON (r.timestamp)
CREATE INDEX idx_device_type FOR (d:Device) ON (d.type)
```
