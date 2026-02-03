# Current Limitations

This document describes the known limitations in the current version of Uni. These are areas where functionality is either partially implemented, not yet available, or has known constraints.

---

## Storage Limitations

### Cloud Storage (S3/GCS/Azure)

**Status:** Supported (Hybrid Mode)

Uni supports cloud storage backends using a hybrid architecture: local WAL/metadata for low latency, cloud storage for bulk data.

| Backend | Status |
|---------|--------|
| Local filesystem | ✅ Fully supported |
| In-memory | ✅ Fully supported (testing) |
| Amazon S3 | ✅ Supported (hybrid mode) |
| Google Cloud Storage | ✅ Supported (hybrid mode) |
| Azure Blob Storage | ✅ Supported (hybrid mode) |
| S3-compatible (MinIO) | ✅ Supported |

**Hybrid Mode:**

Hybrid mode keeps WAL and ID allocation local while storing bulk vertex/edge data in cloud object storage:

```rust
let db = Uni::open("./local_meta")
    .hybrid("./local_meta", "s3://my-bucket/graph-data")
    .build()
    .await?;
```

**Cloud URLs in Commands:**

BACKUP and COPY commands support cloud URLs:

```cypher
BACKUP TO 's3://backup-bucket/snapshot'
COPY Person FROM 'gs://data-bucket/people.parquet'
```

**Configuration:**

See [Cloud Storage Configuration](configuration.md#cloud-storage-configuration) for detailed setup instructions.

**Limitations:**

- Pure cloud mode (no local storage) is not recommended due to WAL latency
- Cloud operations may have higher latency than local storage
- Network failures can cause transient errors (automatic retry is configured)

---

## Query Language Limitations

### OpenCypher Gaps

**Status:** Partial Coverage

Known gaps (see `CYPHER_SPEC_GAPS.md` and `CYPHER_STATUS.md` for full details):

- Dynamic property access with expressions (`node[expr]`)
- Map projection (`person{.name, .age}`)
- Advanced label expressions (`:A&B`, `:A|B`, `:!A`)
- Window frames (`ROWS BETWEEN`, `RANGE BETWEEN`)

**Example (not supported):**

```cypher
MATCH (n)
RETURN n["dynamic_property"]
```

---

## Index Limitations

### Vector Index Limitations

**Status:** Functional with Constraints

- DDL supports `type: 'hnsw'` and `type: 'flat'` (default is IVF_PQ)
- Advanced index parameters (e.g., `m`, `ef_construction`) are not yet configurable via DDL
- Index must be created before inserting vectors for optimal performance
- Rebuilding vector indexes on large datasets can be time-consuming

---

## Concurrency Limitations

### Single-Writer Model

**Status:** By Design

Uni uses a single-writer, multi-reader concurrency model:

- Only one write transaction can be active at a time
- Multiple read transactions can run concurrently
- Readers see a consistent snapshot and are never blocked by writers

**Implications:**

- Write throughput is limited to sequential operations
- Long-running write transactions block other writes
- Suitable for embedded/single-process deployments

**Workaround:** Use batch operations (`BulkWriter`) for high-throughput ingestion. Structure applications to minimize write transaction duration.

### No Distributed Mode

**Status:** Not Available

Uni is an embedded database and does not support distributed deployments:

- No built-in replication
- No sharding across nodes
- No distributed transactions

**Workaround:** For high-availability needs, use application-level replication or deploy behind a load balancer with read replicas using snapshot-based synchronization.

---

## Algorithm Limitations

### Graph Algorithm Scope

**Status:** Functional with Constraints

The 35 built-in graph algorithms operate on in-memory subgraphs:

- Algorithms load relevant data into memory before execution
- Very large graphs may exceed available memory
- No streaming/incremental algorithm execution

**Memory Consideration:**

```rust
// For large graphs, filter to relevant subgraph
let results = db.query(r#"
    CALL uni.algo.pageRank(['Person'], ['KNOWS'])
    YIELD nodeId, score
    RETURN nodeId, score
    LIMIT 100
"#).await?;
```

**Workaround:** Use label and edge type filters to reduce the working set. For very large graphs, consider sampling or partitioning strategies.

---

## Schema Limitations

### No Schema Evolution for Properties

**Status:** Partial Support

While labels and edge types can be created/dropped, property schema changes have constraints:

- Adding new properties: ✅ Supported
- Removing properties: ⚠️ Marks as deprecated, data remains
- Changing property types: ❌ Not supported
- Renaming properties: ❌ Not supported

**Workaround:** Create a new property with the desired type and migrate data manually:

```cypher
// Add new property
MATCH (p:Person)
SET p.age_new = toInteger(p.age_string)

// Remove old property reference from schema
// (data remains but is no longer queryable by name)
```

---

## API Limitations

### Python API Synchronous Only

**Status:** By Design

The Python bindings are synchronous (blocking):

```python
# Python API is synchronous
db = uni_db.Database("./my-graph")
results = db.query("MATCH (n) RETURN n")  # Blocks until complete
```

**Rationale:** Simplifies Python integration. The underlying Rust runtime handles async internally.

**Workaround:** For async Python applications, run Uni operations in a thread pool:

```python
import asyncio
from concurrent.futures import ThreadPoolExecutor

executor = ThreadPoolExecutor(max_workers=4)

async def async_query(db, cypher):
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(executor, db.query, cypher)
```

### No Streaming Results in Python

**Status:** Not Available

Python API returns complete result sets:

```python
# Returns all results at once
results = db.query("MATCH (n) RETURN n")

# No cursor/streaming API available in Python
# (Rust has query_cursor() for streaming)
```

**Workaround:** Use `LIMIT` and `SKIP` for pagination:

```python
offset = 0
batch_size = 1000
while True:
    results = db.query(f"MATCH (n) RETURN n SKIP {offset} LIMIT {batch_size}")
    if not results:
        break
    process(results)
    offset += batch_size
```

---

## Summary Table

| Limitation | Category | Severity | Workaround Available |
|------------|----------|----------|---------------------|
| Cloud Storage (S3/GCS/Azure) | Storage | Low | Supported via hybrid mode |
| Single-writer model | Concurrency | Medium | Batch operations |
| No distributed mode | Architecture | High | Application-level replication |
| No streaming in Python | API | Low | Pagination with SKIP/LIMIT |
| Schema type changes | Schema | Medium | Manual migration |

---

## Recently Resolved

The following limitations have been resolved in recent releases:

| Feature | Status | Notes |
|---------|--------|-------|
| Regular Expression (`=~`) | ✅ Implemented | Full regex support with `=~` operator |
| shortestPath hop constraints | ✅ Implemented | Range specifiers like `*1..5` now supported |
| BTree STARTS WITH pushdown | ✅ Implemented | BTree indexes accelerate prefix searches |

---

## Reporting Issues

If you encounter limitations not documented here, please report them:

- GitHub Issues: [rustic-ai/uni/issues](https://github.com/rustic-ai/uni/issues)
- Include: Uni version, query/code that fails, expected vs actual behavior
