# Glossary

A comprehensive glossary of terms used throughout the Uni documentation.

---

## A

### Adjacency Cache
An in-memory cache storing graph topology in Compressed Sparse Row (CSR) format. Enables O(1) neighbor lookups for fast graph traversal. See [Vectorized Execution](../internals/vectorized-execution.md).

### Aggregation
A Cypher operation that combines multiple rows into summary values. Supported functions include `COUNT`, `SUM`, `AVG`, `MIN`, `MAX`, and `COLLECT`. See [Cypher Querying](../guides/cypher-querying.md).

### ANN (Approximate Nearest Neighbor)
A class of algorithms that find vectors similar to a query vector without exhaustive comparison. Uni supports HNSW and IVF_PQ for ANN search. See [Vector Search](../guides/vector-search.md).

### Apache Arrow
A columnar memory format for flat and hierarchical data. Uni uses Arrow internally for zero-copy data processing and SIMD-accelerated operations. See [Architecture](../concepts/architecture.md).

---

## B

### Batch
A group of rows processed together in vectorized execution. Typical batch sizes are 1024-8192 rows, chosen to fit in CPU cache. See [Vectorized Execution](../internals/vectorized-execution.md).

### B-Tree Index
A balanced tree data structure for ordered data. Used for range queries (`<`, `>`, `BETWEEN`). See [Indexing](../concepts/indexing.md).

---

## C

### CommitResult
Returned by `Transaction.commit()`. Contains the mutation count, database version, WAL LSN, and commit duration.

### CommitStream
Async stream of commit notifications. Created via `session.watch()`. Allows reactive patterns where consumers are notified whenever a new version is committed.

### Compaction
The process of merging multiple data files into fewer, larger files. In Uni, L1 runs are compacted into L2. See [Storage Engine](../internals/storage-engine.md).

### CSR (Compressed Sparse Row)
A compact representation of sparse matrices (graphs). Stores vertex neighbors in contiguous arrays with offset pointers. Used in the adjacency cache for efficient traversal.

### Cypher
A declarative graph query language using ASCII-art patterns. Originally developed for Neo4j, now standardized as OpenCypher. Uni implements a substantial subset. See [Cypher Querying](../guides/cypher-querying.md).

---

## D

### DataFusion
An Apache Arrow-native query engine. Uni uses DataFusion for columnar processing, aggregations, and some query operations.

### Direction
The orientation of an edge traversal:
- **Outgoing** (`-[r]->`)**: From source to target
- **Incoming** (`<-[r]-`): From target to source
- **Both** (`-[r]-`): Either direction

---

## E

### Edge
A connection between two vertices in a graph. In Uni, edges have:
- **Type**: Category of relationship (e.g., `CITES`, `AUTHORED_BY`)
- **Direction**: From source to destination
- **Properties**: Optional key-value attributes

### EID (Edge ID)
A 64-bit identifier for edges, allocated from one database-wide auto-increment counter. It does not encode the edge type. See [Identity Model](../concepts/identity.md).

### Embedding
A dense vector representation of data (text, images, etc.) in a high-dimensional space. Similar items have embeddings close together. See [Vector Search](../guides/vector-search.md).

### EXPLAIN
A Cypher command prefix that shows the query plan without executing the query. Useful for understanding how queries will be processed.

---

## F

### FastEmbed
A Rust library for generating text embeddings locally via ONNX Runtime. Uni integrated `fastembed-rs` directly through `uni-xervo 0.7.x`'s `provider-fastembed`. As of `uni-xervo 0.8.0`, the FastEmbed wrapper was retired and replaced with a native ONNX `Embed` task on the unified `local/onnx` provider; all 25 FastEmbed alias strings (e.g. `BGESmallENV15`, `AllMiniLML6V2`, `NomicEmbedTextV15`) still work unchanged. See [Vector Search](../guides/vector-search.md).

### Flush
The process of writing in-memory data (L0) to persistent storage (L1). Triggered by size thresholds or explicit calls.

---

## G

### Graph Database
A database optimized for storing and querying connected data. Models data as vertices (nodes) and edges (relationships) rather than tables and rows.

---

## H

### Hash Index
A scalar index type for O(1) equality lookups on high-cardinality columns.

### HNSW (Hierarchical Navigable Small World)
A graph-based algorithm for approximate nearest neighbor search. Uni supports three HNSW variants: **HNSW-Flat** (no quantization), **HNSW-SQ** (scalar quantization, default), and **HNSW-PQ** (product quantization). All support optional IVF partitioning for very large datasets. See [Indexing](../concepts/indexing.md).

---

## I

### IVF (Inverted File)
A partition-based vector index family. Uni supports **IVF-Flat** (no compression), **IVF-SQ** (scalar quantization), **IVF-PQ** (product quantization), and **IVF-RQ** (RaBitQ quantization).

---

## L

### LabelList Index
A scalar index type for `List<T>` columns that enables efficient `array_contains_any` and `array_contains_all` queries. Created via `"LABEL_LIST"` in the procedure API.

---

## J

### JSONL (JSON Lines)
A text format with one JSON object per line. Uni's primary format for bulk data import.

---

## K

### KNN (K-Nearest Neighbors)
Finding the K vectors most similar to a query vector. Uni's vector search returns KNN results ordered by distance.

---

## L

### L0 Buffer
The in-memory write buffer that accepts all incoming mutations. Contains an adjacency-list graph (`SimpleGraph`) for topology and the pending property values. See [Storage Engine](../internals/storage-engine.md).

### L1 Layer
Immutable Lance datasets created by flushing L0. Contains sorted runs of data not yet compacted into L2.

### L2 Layer
The base storage layer containing fully compacted, indexed data. Most data resides here after compaction.

### Label
A type classifier for vertices (e.g., `Paper`, `Author`). Similar to a table name in relational databases. A vertex can carry several labels.

### Lance
A columnar data format optimized for ML workloads. Features native vector indexing, versioning, and cloud storage support. Uni uses Lance as its primary storage format. See [Storage Engine](../internals/storage-engine.md).

### Late Materialization
An optimization that delays loading heavy properties until after filtering. Reduces I/O by only loading data for rows that survive filters. See [Vectorized Execution](../internals/vectorized-execution.md).

### LSM Tree (Log-Structured Merge Tree)
A storage architecture with tiered levels (L0, L1, L2). Writes go to memory first, then flush to immutable files that are periodically compacted.

---

## M

### Manifest
A JSON file describing the state of storage at a point in time. Contains dataset versions, index metadata, and L1 run information. Enables snapshot isolation.

### MATCH
The primary Cypher clause for specifying graph patterns to find. Uses ASCII-art syntax like `(a)-[r]->(b)`.

### Morsel
A work unit in parallel execution. Source data is divided into morsels that workers process independently. See [Vectorized Execution](../internals/vectorized-execution.md).

---

## N

### Node
See [Vertex](#vertex).

---

## O

### OpenCypher
An open standard for the Cypher query language. Uni implements a substantial subset of OpenCypher.

---

## P

### Prepared Statement
A pre-parsed and pre-planned query for repeated execution with different parameters. Created via `session.prepare()` (Cypher) or `session.prepare_locy()` (Locy). Avoids re-parsing and re-planning overhead on each invocation.

### Predicate Pushdown
An optimization that pushes filter conditions down to the storage layer. Reduces I/O by filtering at scan time rather than after loading. See [Query Planning](../internals/query-planning.md).

### Profile
Running a query with `query_with(...).profile()`, which executes it and returns the result with a `ProfileOutput`: total time, planning time, peak memory and per-operator timing and row counts. More informative than EXPLAIN. There is no `PROFILE` keyword.

### Property
A key-value attribute on a vertex or edge. Properties have defined types (String, Int32, Vector, etc.) in the schema.

### Property Graph
A data model where vertices and edges can have arbitrary properties. More flexible than simple labeled graphs.

---

## Q

### Query Plan
The sequence of operations that will execute a query. Includes logical plan (what to do) and physical plan (how to do it).

---

## R

### RuleRegistry
Session-scoped or transaction-scoped store of pre-compiled Locy rules. Accessed via `session.rules()`. Rules registered in the registry are available to all Locy evaluations within that session without re-compilation.

### RecordBatch
An Apache Arrow data structure containing a batch of columnar data with a shared schema. The fundamental data unit in vectorized execution.

### Relationship
See [Edge](#edge).

---

## S

### Session
The primary read scope for all database access. Created via `db.session()`. Cheap, sync, and `Arc`-based — sessions can be cloned and shared across threads. All queries, Locy evaluations, and transaction creation go through a session.

### SessionHook
A trait for intercepting query and commit events. Registered with `Uni::add_plugin(BuiltinHookPlugin::new(name, hook))`; `session.add_hook()` is deprecated. Hooks receive callbacks before/after queries and commits, enabling logging, metrics, and custom middleware patterns.

### SessionTemplate
A pre-configured session factory for repeated session creation with identical parameters, rules, and hooks. Useful when many sessions share the same configuration.

### Schema
The structure definition for a graph, including:
- Labels and their properties
- Edge types and constraints
- Vector dimensions
- Indexes

### Selection Vector
A bitmap or index array marking which rows in a batch are "active" after filtering. Avoids copying data when filtering.

### SIMD (Single Instruction, Multiple Data)
CPU instructions that operate on multiple data elements simultaneously. Arrow compute kernels use SIMD for fast filtering and arithmetic.

### Snapshot
A consistent point-in-time view of the database. Readers see a stable snapshot even as writes occur.

### Snapshot Isolation
A concurrency model where each reader sees a consistent snapshot. Readers don't block writers and vice versa.

---

## T

### Transaction
An explicit write scope with ACID guarantees. Created via `session.tx()`. Each transaction owns a private L0 buffer and uses commit-time serialization — writes accumulate without holding a global lock, and conflicts are detected at commit time.

### Tombstone
A marker indicating deleted data. Soft deletes mark rows as deleted; compaction removes tombstoned data.

### Traversal
Following edges from vertices to discover connected vertices. A fundamental graph operation.

---

## U

### UNWIND
A Cypher clause that expands a list into multiple rows. Useful for batch operations and working with array properties.

---

## V

### Vector Index
A data structure enabling fast similarity search on high-dimensional vectors. Uni supports HNSW and IVF_PQ vector indexes.

### Vectorized Execution
A query processing model that operates on batches of rows rather than one row at a time. Improves performance through better cache utilization and SIMD operations. See [Vectorized Execution](../internals/vectorized-execution.md).

### Vertex
A node in the graph representing an entity. In Uni, vertices have:
- **VID**: Unique identifier
- **Label**: Type classification
- **Properties**: Key-value attributes

### VID (Vertex ID)
A 64-bit identifier for vertices, allocated from one database-wide auto-increment counter. It does not encode the label. See [Identity Model](../concepts/identity.md).

---

## W

### WAL (Write-Ahead Log)
A durability mechanism that logs mutations before applying them. Enables recovery after crashes.

### WriteLease
How a store keeps to one writable handle at a time. `Local` (the default) holds a local store with an OS lock and an object store with a renewed lease object, `catalog/writer_lease.json`. `Custom` calls a user-provided `WriteLeaseProvider`. `Unenforced` takes nothing (one writer by convention). `DynamoDB` is not implemented and fails the open. A second writable open fails with `StoreLocked`; a handle that loses its lease fails writes with `WriterFenced`.

### UniId
A content-addressed identifier using SHA3-256 hash (32 bytes). Used for provenance tracking and distributed synchronization with CRDT systems. See [Identity Model](../concepts/identity.md).

### WITH
A Cypher clause that pipes results from one query part to another. Enables subquery-like behavior and intermediate aggregations.

### Working Graph
An in-memory graph materialized from storage for algorithm execution: a `GraphProjection`, a dense CSR (compressed sparse row) structure.

---

## Y

### YIELD
A Cypher clause used with CALL to specify which columns to return from a procedure. Used with vector search and other built-in procedures.

---

## Common Abbreviations

| Abbreviation | Meaning |
|--------------|---------|
| ANN | Approximate Nearest Neighbor |
| CSR | Compressed Sparse Row |
| EID | Edge ID |
| HNSW | Hierarchical Navigable Small World |
| IVF | Inverted File |
| KNN | K-Nearest Neighbors |
| L0/L1/L2 | Storage layer levels |
| LSM | Log-Structured Merge |
| PQ | Product Quantization |
| SIMD | Single Instruction Multiple Data |
| VID | Vertex ID |
| WAL | Write-Ahead Log |

---

## Next Steps

- [Architecture](../concepts/architecture.md) — System overview
- [Rust API Reference](rust-api.md) — Complete API documentation
- [Configuration Reference](configuration.md) — All configuration options
