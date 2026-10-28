# Performance Tuning Guide

This guide covers strategies for optimizing Uni's performance across query execution, storage, indexing, and resource utilization.

## Performance Overview

Uni's performance characteristics (indicative numbers from internal benchmarks; see the Benchmarks doc for details):

| Operation | Typical Latency | Optimization Target |
|-----------|-----------------|---------------------|
| Point lookup | 2-5ms | Index usage |
| 1-hop traversal | 4-8ms | Adjacency cache |
| Vector KNN (k=10) | 1-3ms | Index tuning |
| Aggregation (1M rows) | 50-200ms | Predicate pushdown |
| Bulk insert (10K) | 5-10ms | Batch size |

---

## Query Optimization

### 1. Use Predicate Pushdown

Let a scalar index narrow the scan instead of filtering every row:

```cypher
// Good: with indexes on year and venue, both conjuncts narrow the scan
MATCH (p:Paper)
WHERE p.year > 2020 AND p.venue = 'NeurIPS'
RETURN p.title

// Bad: Filter applied after full scan
MATCH (p:Paper)
WHERE p.title CONTAINS 'Transformer'  // No index serves CONTAINS
RETURN p.title
```

**Predicates a scalar index serves** (on a property with an Online index;
each conjunct of an `AND` is considered on its own):

| Predicate | BTree | Hash |
|---|---|---|
| `n.p = v`, `n.p IN [...]` | yes | yes |
| `n.p < v`, `<=`, `>`, `>=`, and `AND`-ed pairs such as `n.p >= a AND n.p < b` | yes | no |
| `n.p STARTS WITH 'prefix'` (literal or parameter) | yes, as a range | no |
| `n.p IS NULL`, `n.p IS NOT NULL` | yes | yes |

The index narrows the scan; the full `WHERE` is still applied to what it
returns, so results are the same as without the index. An operand that cannot
be handed to the index exactly is simply evaluated after the scan instead:
`NULL`, `NaN`, infinite or zero bounds on a `FLOAT` property, a value of
another type (`n.age < 'ten'`), an integer beyond 2^53 against a `FLOAT`
property, and the empty prefix. A float bound on an `INT` property is fine
(`n.age > 30.5` is served as `n.age > 30`).

An index pays off when the predicate is selective. A range that matches most of
the label (`n.age > 0`, `IS NOT NULL` on a mostly-set property) is somewhat
slower through the index than a plain scan, since every row it returns is
re-checked against newer versions.

**Evaluated after the scan:**
- Predicates on unindexed properties
- `CONTAINS`, `ENDS WITH`
- Function calls: `lower(x) = 'value'`
- `NOT (...)` and `OR` with different properties
- Comparisons between two properties (`n.a < n.b`)

### 2. Limit Early

Apply LIMIT as early as possible:

```cypher
// Good: Limit applied early in pipeline
MATCH (p:Paper)
WHERE p.year > 2020
RETURN p.title
ORDER BY p.year DESC
LIMIT 10

// Bad: Process all then limit
MATCH (p:Paper)-[:CITES]->(cited)
WITH p, COUNT(cited) AS citation_count
ORDER BY citation_count DESC
RETURN p.title, citation_count
LIMIT 10  // All citations computed before limit
```

### 3. Project Only Needed Properties

Don't fetch unnecessary properties:

```cypher
// Good: Only fetch needed properties
MATCH (p:Paper)
RETURN p.title, p.year

// Bad: Fetch all properties
MATCH (p:Paper)
RETURN p  // Loads all properties including large ones

// Worse: Return unused properties
MATCH (p:Paper)
RETURN p.title, p.abstract, p.embedding  // embedding loaded but unused
```

### 4. Use Indexes

Ensure indexes exist for filter properties:

```cypher
// Check if index is used
EXPLAIN MATCH (p:Paper) WHERE p.year = 2023 RETURN p.title

// Create index if missing
CREATE INDEX paper_year FOR (p:Paper) ON (p.year)
```

### 5. Optimize Traversal Patterns

Structure patterns for efficient execution:

```cypher
// Good: Filter before traverse
MATCH (p:Paper)
WHERE p.year > 2020
MATCH (p)-[:CITES]->(cited)
RETURN p.title, cited.title

// Good: Traverse from smaller set
MATCH (seed:Paper {title: 'Attention Is All You Need'})
MATCH (seed)-[:CITES]->(cited)
RETURN cited.title

// Bad: Full cross-product
MATCH (p1:Paper), (p2:Paper)
WHERE p1.title = p2.title  // Cartesian join
RETURN p1, p2
```

### 6. Ask for Endpoints, Not Paths

This is the highest-leverage change available on a variable-length pattern.

A `-[:R*1..n]->` pattern runs a graph search and, **whenever the answer depends
on how many paths match**, expands that search into individual paths. The search
cost tracks the edges explored. The expansion cost tracks the number of distinct
paths, which on a graph with cycles grows combinatorially with the hop bound.

A `MATCH` produces one row per path even when the relationship is unnamed — so
`count(*)`, `sum(...)` or a plain `RETURN b` all pay for expansion. Asking only
*which* endpoints are reachable (`DISTINCT`, `count(DISTINCT ...)`, `min`/`max`,
`EXISTS { ... }`, a pattern predicate in `WHERE`) lets the search stop there:

```cypher
// Expensive: every distinct path is materialised
MATCH p = (a:Company {id: 'X'})-[:OWNS*1..6]->(b:Company)
RETURN p

// Also expands: one row per path, even without a path variable
MATCH (a:Company {id: 'X'})-[:OWNS*1..6]->(b:Company)
RETURN b

// Cheap: identical search, no expansion
MATCH (a:Company {id: 'X'})-[:OWNS*1..6]->(b:Company)
RETURN DISTINCT b
```

A correlated `EXISTS { ... }` or `COUNT { ... }` between two nodes the outer
query binds is evaluated once per source, not once per pair, when the body
names one of them only as a bare node (no label, property map or `WHERE` on
it). The other one is the source:

```cypher
// One search per a, shared by every b
MATCH (a:Person), (b:Person)
WHERE NOT EXISTS { MATCH (a)-[:KNOWS*1..]->(b) }
RETURN a, b

// One search per (a, b) pair: the body reads both endpoints
MATCH (a:Person), (b:Person)
WHERE NOT EXISTS { MATCH (a)-[:KNOWS*1..]->(b) WHERE a.active AND b.active }
RETURN a, b
```

Move such filters to the outer query (`WHERE a.active AND b.active AND NOT
EXISTS { ... }`) to keep the shared search. The body is planned each time it
runs, so `result.metrics().physical_plans` minus one is how many times it ran.

When you genuinely need paths:

- **`shortestPath`** stops at the first path it finds — use it whenever one path
  will do. An unbound target (`shortestPath((a {id: 0})-[:R*]->(b:Person))`)
  runs one search from each source, shared by all its targets, and a `WHERE`
  on the target alone (`WHERE b.age > 30`) narrows the targets before the
  search. When several shortest paths tie, which one is returned is not
  guaranteed.
- **A `LIMIT` stops the expansion**, rather than trimming its output, so a small
  limit costs little. Two caveats: the graph search still runs in full, and the
  limit applies one batch (8192 rows) at a time, so any limit up to 8192 costs
  the same as `LIMIT 1`.
- **An omitted upper bound is 100, not infinity.** If a traversal actually hits
  that ceiling the query warns that its results are incomplete, so check
  `result.warnings` when a `[*]` answer looks short. Write `[*1..6]` when you
  know the depth.

---

## Index Tuning

### Vector Index Configuration

Cypher DDL lets you choose the vector index algorithm but uses cosine distance and default parameters:

```cypher
CREATE VECTOR INDEX paper_embeddings
FOR (p:Paper) ON p.embedding
OPTIONS { type: "hnsw" }  // hnsw | ivf_pq | flat
```

For metric selection or tuning, use the Rust schema builder:

```rust
use uni_db::{DataType, IndexType, VectorAlgo, VectorIndexCfg, VectorMetric};

db.schema()
    .label("Paper")
        .property("embedding", DataType::Vector { dimensions: 768 })
        .index("embedding", IndexType::Vector(VectorIndexCfg {
            algorithm: VectorAlgo::HnswSq { m: 32, ef_construction: 200, partitions: None },
            metric: VectorMetric::Cosine,
            embedding: None,
        }))
    .apply()
    .await?;
```

#### HNSW Parameters (Rust)

| Parameter | Effect | Guidance |
|-----------|--------|----------|
| `m` | Graph degree / memory | Higher improves recall, increases memory |
| `ef_construction` | Build-time search | Higher improves recall, slows build |

`ef_search` is fixed internally and not user-configurable yet.

#### IVF_PQ Parameters (Rust)

| Parameter | Effect | Guidance |
|-----------|--------|----------|
| `partitions` | Coarse clusters | Higher improves recall, increases memory |
| `sub_vectors` | PQ code size | Higher improves recall, larger index |

`bits_per_subvector` is fixed to 8 in the current Rust API.

### Scalar Indexes

Cypher creates BTree scalar indexes:

```cypher
CREATE INDEX paper_year FOR (p:Paper) ON (p.year)
```

The storage layer currently builds BTree scalar indexes only.

### Composite Indexes

Create composite indexes for common filter combinations:

```cypher
// Composite index for common query pattern
CREATE INDEX paper_venue_year FOR (p:Paper) ON (p.venue, p.year)

// Query uses the composite index
MATCH (p:Paper)
WHERE p.venue = 'NeurIPS' AND p.year > 2020
RETURN p.title
```

---

## Storage Optimization

### Batch Size Tuning

Tune batch sizes for your workload:

```rust
// BulkWriter with larger batches (more memory, faster)
let session = db.session();
let tx = session.tx().await?;
let bulk = tx.bulk_writer().batch_size(50_000).build()?;

// BulkWriter with smaller batches (less memory)
let bulk = tx.bulk_writer().batch_size(5_000).build()?;
```

**Guidelines:**
- Increase batch size if memory allows (faster)
- Decrease if OOM errors occur
- Default (10,000) is good for most cases

### L0 Buffer Configuration

Tune the in-memory write buffer:

```rust
use std::time::Duration;

let config = UniConfig {
    // Mutation-based flush (high-transaction systems)
    auto_flush_threshold: 10_000,  // Flush at 10K mutations

    // Time-based flush (low-transaction systems)
    auto_flush_interval: Some(Duration::from_secs(5)),  // Flush every 5s
    auto_flush_min_mutations: 1,  // If at least 1 mutation pending

    ..Default::default()
};
```

**Trade-offs:**
- Larger threshold: Better write throughput, higher memory, longer recovery
- Smaller threshold: Lower memory, more frequent flushes, faster recovery
- Shorter interval: Lower data-at-risk, more I/O overhead
- Longer interval: Less I/O overhead, more data-at-risk on crash

### Auto-Flush Tuning

Choose flush strategy based on workload:

| Workload | Recommended Settings | Rationale |
|----------|---------------------|-----------|
| High-transaction OLTP | `threshold: 10_000`, `interval: None` | Mutation count drives flush |
| Low-transaction | `threshold: 10_000`, `interval: 5s` | Time ensures eventual flush |
| Critical data | `threshold: 1_000`, `interval: 1s` | Minimize data at risk |
| Cost-sensitive cloud | `threshold: 50_000`, `interval: 30s` | Reduce API calls |
| Batch import | `threshold: 100_000`, `interval: None` | Maximum throughput |

```rust
// High-transaction system (default)
let config = UniConfig {
    auto_flush_threshold: 10_000,
    auto_flush_interval: Some(Duration::from_secs(5)),
    ..Default::default()
};

// Cost-sensitive cloud workload
let config = UniConfig {
    auto_flush_threshold: 50_000,
    auto_flush_interval: Some(Duration::from_secs(30)),
    auto_flush_min_mutations: 100,  // Batch up small writes
    ..Default::default()
};

// Critical data, minimize loss
let config = UniConfig {
    auto_flush_threshold: 1_000,
    auto_flush_interval: Some(Duration::from_secs(1)),
    ..Default::default()
};
```

### Compaction

Compaction is fully automatic. A background loop runs every `check_interval` (default 30s) and triggers compaction when any threshold is exceeded:

| Trigger | Condition | Default |
|---------|-----------|---------|
| **ByRunCount** | L1 delta tables with data ≥ threshold | `max_l1_runs = 4` |
| **BySize** | Aggregate L1 size ≥ threshold | `max_l1_size_bytes = 256 MB` |
| **ByAge** | Oldest L1 run age ≥ threshold | `max_l1_age = 1 hour` |

**What runs during compaction:**

1. **Semantic compaction** (Tier 2) — vertex dedup, CRDT merge, L1→L2 delta consolidation, tombstone cleanup
2. **Lance optimize** (Tier 3) — fragment consolidation, index rebuild, space reclaim across all table types

#### Configuration

```rust
use std::time::Duration;
use uni_db::UniConfig;

let mut config = UniConfig::default();
config.compaction.enabled = true;                          // Enable background compaction (default: true)
config.compaction.max_l1_runs = 4;                         // Trigger after 4 L1 runs (default: 4)
config.compaction.max_l1_size_bytes = 256 * 1024 * 1024;   // Trigger at 256 MB (default: 256 MB)
config.compaction.max_l1_age = Duration::from_secs(3600);  // Trigger after 1 hour (default: 1 hour)
config.compaction.check_interval = Duration::from_secs(30);// Check every 30s (default: 30s)
```

| Field | Default | Description |
|-------|---------|-------------|
| `enabled` | `true` | Enable background compaction |
| `max_l1_runs` | `4` | Max L1 delta run count before triggering |
| `max_l1_size_bytes` | `256 MB` | Max aggregate L1 size before triggering |
| `max_l1_age` | `1 hour` | Max age of oldest L1 run before triggering |
| `check_interval` | `30s` | How often the background loop checks |

#### Manual Compaction (Optional)

For on-demand compaction after bulk loads, you can trigger it manually.
`CALL uni.admin.compact()` (run it with `session.run()` or the `uni query` CLI;
`session.query()` is read-only and refuses it; Rust `StorageManager::compact`) runs the same full
pass as background compaction -- folding L1 deltas, then optimizing every
table -- and never at the same time as one: it waits for a running pass. It
also lowers the flush-generation count that drives write throttling, so it
releases writers stalled at `throttle.hard_limit`. The label- and
edge-type-scoped calls only optimize those tables' files and leave the count
unchanged.

```bash
# Manual compaction (via Cypher)
uni query "CALL uni.admin.compact() YIELD tables_optimized, fragments_removed, duration_ms RETURN *" --path ./storage
```

```rust
// Label/edge-specific compaction via Rust API
db.compact_label("Paper").await?;
db.compact_edge_type("CITES").await?;
```

---

## Cache Configuration

### Adjacency Cache

The CSR adjacency cache is critical for traversal performance:

```rust
use uni_db::{Uni, UniConfig};

let mut config = UniConfig::default();
config.cache_size = 1_000_000_000; // bytes

let db = Uni::open("./graph")
    .config(config)
    .build()
    .await?;
```

**Sizing Guidelines:**
- Size for your "hot" working set
- Monitor cache hit ratio
- Increase if traversals are slow after warmup

### Storage Engine (Lance) Caches

Every table a database opens reads through one Lance session, shared by all
tables and by every fork of the database. It holds two caches:

| Cache | `UniConfig` field | Lance default | Holds |
|---|---|---|---|
| Metadata | `lance_metadata_cache_bytes` | 1 GiB | manifests, fragment and file metadata |
| Index | `lance_index_cache_bytes` | 6 GiB | opened scalar, vector and full-text indexes |

Both caches fill as tables are flushed and read and are trimmed only when
full. A long-running process that flushes often therefore keeps growing toward
the full 7 GiB: each flush writes a new table version whose metadata the next
read caches. Set a capacity to bound it:

```rust
use uni_db::{Uni, UniConfig};

let config = UniConfig {
    lance_metadata_cache_bytes: Some(64 * 1024 * 1024),
    lance_index_cache_bytes: Some(512 * 1024 * 1024),
    ..Default::default()
};
let db = Uni::open("./graph").config(config).build().await?;

let m = db.metrics();
println!("{} / {}", m.storage_metadata_cache_bytes, m.storage_index_cache_bytes);
```

```python
db = (uni_db.UniBuilder.open("./graph")
      .config({"lance_metadata_cache_bytes": 64 << 20,
               "lance_index_cache_bytes": 512 << 20})
      .build())
db.metrics().storage_metadata_cache_bytes
```

A miss is re-read from storage, so a smaller cache costs I/O, never
correctness. Keep the index cache above the size of any vector index you
search: an ANN index evicted from it is re-read and re-decoded on every query.
`Some(0)` disables a cache.

### Property Cache

Property cache sizing is currently fixed internally. If you need explicit control, use the low-level APIs and construct a `PropertyManager` directly.

---

## Query Analysis

### EXPLAIN

View the query plan without execution:

```bash
uni query "EXPLAIN MATCH (p:Paper) WHERE p.year > 2020 RETURN p.title" \
    --path ./storage
```

Output:
```
Query Plan:
├── Project [p.title]
│   └── Scan [:Paper]
│         ↳ Index: paper_year (year > 2020)
│         ↳ Pushdown: year > 2020

Estimated rows: 5,000
Index usage: BTree (paper_year)
```

### PROFILE

Execute with timing breakdown:

```bash
uni query "PROFILE MATCH (p:Paper)-[:CITES]->(c) RETURN COUNT(c)" \
    --path ./storage
```

Output:
```
┌───────────┐
│ COUNT(c)  │
├───────────┤
│ 45,231    │
└───────────┘

Execution Profile:
  Parse:      0.8ms
  Plan:       1.2ms
  Execute:    42.3ms
    ├── Scan:       12.1ms (28.6%)  [10,000 rows]
    ├── Traverse:   24.5ms (57.9%)  [45,231 edges]
    └── Aggregate:   5.7ms (13.5%)  [1 row]
  Total:      44.3ms
```

### Identifying Bottlenecks

| Profile Pattern | Likely Cause | Solution |
|-----------------|--------------|----------|
| High Scan time | No index, large result set | Add index, add filters |
| High Traverse time | Cold cache, many edges | Warm cache, bound the hops |
| High Traverse time on a `*` pattern returning `p` | Path expansion, not the search | Return endpoints instead of the path; or add a `LIMIT` |
| High Aggregate time | Large group count | Add LIMIT, pre-aggregate |
| High memory | Large intermediate results | Stream results, limit |

---

## Parallel Execution

### Morsel-Driven Parallelism

Uni uses morsel-driven parallelism for large queries:

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                         PARALLEL EXECUTION                                   │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│   Source Data: [────────────────────────────────────────────────]           │
│                         │                                                   │
│                         ▼                                                   │
│   Morsels:     [────] [────] [────] [────] [────] [────]                   │
│                  │       │       │       │       │       │                  │
│                  ▼       ▼       ▼       ▼       ▼       ▼                  │
│   Workers:     [W1]   [W2]   [W3]   [W4]   [W1]   [W2]                     │
│                  │       │       │       │       │       │                  │
│                  └───────┴───────┴───────┴───────┴───────┘                  │
│                                   │                                         │
│                                   ▼                                         │
│   Merge:                     [Results]                                      │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

### Concurrency Configuration

```rust
use uni_db::{Uni, UniConfig};

let mut config = UniConfig::default();
config.parallelism = 8;                   // Partitions per query plan
config.execution_batch_size = Some(4096); // Rows per batch inside the engine (default 8192)

let db = Uni::open("./graph")
    .config(config)
    .build()
    .await?;
```

**Guidelines:**
- Set workers to CPU core count
- Increase morsel size for simpler queries
- Decrease morsel size for complex operators

---

## Memory Management

### Use mimalloc as Global Allocator

For allocation-heavy workloads (many small mutations, concurrent Cypher `CREATE`/`MERGE`, per-statement workloads), the **default glibc allocator becomes the dominant bottleneck** long before any uni-db lock does. Profile data at 24-session concurrency showed:

- **~25%** of CPU time in `__memset_avx2_unaligned_erms` (zeroing fresh heap pages)
- **~15%** in kernel `clear_page_erms` (zeroing anonymous pages on first touch)
- **~4%** in kernel page-freelist bookkeeping
- **~2%** in `glibc malloc_consolidate` (arena lock)

Switching the global allocator to **mimalloc** sidesteps both — its thread-local arenas and heap recycling avoid most kernel-side page traffic.

**Rust library consumers:**

```toml
[dependencies]
uni-db = { version = "...", features = ["mimalloc"] }
```

```rust
// in your binary's main.rs:
#[global_allocator]
static GLOBAL: uni_db::MiMalloc = uni_db::MiMalloc;
```

**Python users:** every wheel already ships with mimalloc as the Rust-side global allocator. CPython's `PyMem_*` allocator is untouched; only Rust allocations route through mimalloc.

**CLI users:** the `uni` CLI binary uses mimalloc by default.

**Measured win** (`concurrent_mutations` benchmark, 24 sessions × 100 `CREATE` statements each):

| N | Default glibc | With mimalloc | Speedup |
|---|---|---|---|
| 1 | 139.30 ms | 45.19 ms | 3.08× |
| 4 | 186.18 ms | 63.06 ms | 2.95× |
| 12 | 440.80 ms | 160.49 ms | 2.74× |
| 24 | 984.21 ms | 393.78 ms | 2.50× |

The 3× win at sess=1 reveals that glibc was bloated even single-threaded for this workload — the per-statement parse + logical plan + DataFusion physical plan churn outpaces glibc's arena allocator. mimalloc costs ~10 MB of additional RSS (its bookkeeping arenas) — negligible at production scale.

### Memory Budget

Monitor and limit memory usage:

```bash
# Monitor memory during query
RUST_LOG=uni_db=debug uni query "..." 2>&1 | grep -i memory
```

```rust
use uni_db::UniConfig;

let mut config = UniConfig::default();
config.max_query_memory = 4 * 1024 * 1024 * 1024; // 4 GB
```

### Reducing Memory Usage

1. **Smaller batch sizes**: use `UniConfig.execution_batch_size` (query execution) or `BulkWriter.batch_size()` (bulk ingest)
2. **Smaller caches**: Reduce `UniConfig.cache_size`, and cap the storage engine's caches with `lance_metadata_cache_bytes` / `lance_index_cache_bytes` (see [Storage Engine (Lance) Caches](#storage-engine-lance-caches))
3. **Stream large results**: Use SKIP/LIMIT pagination
4. **Avoid large intermediates**: Filter early

### Memory Profile

Detailed per-component memory stats are not exposed yet. Use OS-level tools (e.g., `top`, `htop`, `ps`) alongside `PROFILE` output for coarse insights.

---

## I/O Optimization

### Cloud Storage Configuration

Uni supports multiple cloud storage backends with automatic credential resolution:

```rust
use uni_db::Uni;
use uni_common::CloudStorageConfig;

// Amazon S3
let cfg = CloudStorageConfig::S3 {
    bucket: "my-bucket".to_string(),
    region: Some("us-east-1".to_string()),
    endpoint: None,
    access_key_id: None,
    secret_access_key: None,
    session_token: None,
    virtual_hosted_style: true,
};

let db = Uni::open("./local-meta")
    .remote_storage("s3://my-bucket/graph-data", cfg)
    .build()
    .await?;

// Google Cloud Storage
let cfg = CloudStorageConfig::Gcs {
    bucket: "my-gcs-bucket".to_string(),
    service_account_path: None,
    service_account_key: None,
};

let db = Uni::open("./local-meta")
    .remote_storage("gs://my-gcs-bucket/graph-data", cfg)
    .build()
    .await?;

// S3-compatible (MinIO, LocalStack)
let cfg = CloudStorageConfig::S3 {
    bucket: "my-bucket".to_string(),
    region: Some("us-east-1".to_string()),
    endpoint: Some("http://localhost:9000".to_string()),
    access_key_id: Some("minioadmin".to_string()),
    secret_access_key: Some("minioadmin".to_string()),
    session_token: None,
    virtual_hosted_style: false,
};

let db = Uni::open("./local-meta")
    .remote_storage("s3://my-bucket/graph-data", cfg)
    .build()
    .await?;
```

### Hybrid Mode for Optimal Performance

Use hybrid mode (local + cloud) for best write latency with cloud durability:

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                    HYBRID MODE PERFORMANCE                                   │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│   Operation          Local-Only    Cloud-Only    Hybrid Mode               │
│   ─────────────────────────────────────────────────────────────────────────│
│   Single write       ~50µs         ~100ms        ~50µs (local L0)          │
│   Batch 1K writes    ~550µs        ~150ms        ~550µs (local L0)         │
│   Point read (cold)  ~3ms          ~100ms        ~100ms (first access)     │
│   Point read (warm)  ~3ms          ~3ms          ~3ms (cached)             │
│   Durability         Local disk    Cloud         Cloud (after flush)       │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

**Best Practice:** Use hybrid mode when:
- Write latency matters (< 1ms)
- Data must ultimately reside in cloud storage
- You have local SSD for the write cache

### Auto-Flush Tuning for Cloud

Tune L1 flush frequency for request cost and checkpoint latency. With WAL enabled, commits are durable before the L1 flush:

| Cloud Provider | Recommended Interval | Rationale |
|----------------|---------------------|-----------|
| S3 | 5-30s | Balance PUT request costs |
| GCS | 5-30s | Similar to S3 |
| Azure Blob | 5-30s | Similar to S3 |
| Local SSD | 1-5s | No request charge; reduce L0 backlog |

```rust
use uni_db::{Uni, UniConfig};

// Cost-optimized for cloud (fewer API calls)
let mut config = UniConfig::default();
config.auto_flush_threshold = 50_000;
config.auto_flush_interval = Some(Duration::from_secs(30));
config.auto_flush_min_mutations = 100;

let db = Uni::open("./local-meta")
    .remote_storage("s3://my-bucket/data", CloudStorageConfig::s3_from_env("my-bucket"))
    .config(config)
    .build()
    .await?;
```

### Read-Ahead

Read-ahead and prefetch settings are not exposed yet. For sequential scans, rely on OS/file-system caching and keep datasets on local SSDs when possible.

---

## Benchmarking

### Built-in Benchmarks

```bash
# Run all benchmarks
cargo bench

# Run specific benchmark
cargo bench -- vector_search

# Save baseline
cargo bench -- --save-baseline main

# Compare to baseline
cargo bench -- --baseline main
```

### Custom Benchmarks

```rust
use criterion::{criterion_group, criterion_main, Criterion};

fn benchmark_traversal(c: &mut Criterion) {
    let storage = setup_storage();

    c.bench_function("1-hop traversal", |b| {
        b.iter(|| {
            let query = "MATCH (p:Paper)-[:CITES]->(c) RETURN COUNT(c)";
            executor.execute(query).unwrap()
        })
    });
}

criterion_group!(benches, benchmark_traversal);
criterion_main!(benches);
```

---

## Performance Checklist

Before deploying to production:

- [ ] Indexes created for filter properties
- [ ] Vector indexes tuned for recall/latency trade-off
- [ ] Batch sizes tuned for workload
- [ ] Cache sizes appropriate for working set
- [ ] Queries use pushable predicates where possible
- [ ] LIMIT applied early in query patterns
- [ ] Variable-length patterns return DISTINCT endpoints unless paths (or their count) are genuinely needed
- [ ] Variable-length patterns carry an explicit upper hop bound
- [ ] Only needed properties projected
- [ ] Memory limits configured
- [ ] I/O timeouts set for remote storage
- [ ] Monitoring enabled for cache hit rates

---

## Next Steps

- [Architecture](../concepts/architecture.md) — Understand system internals
- [Vectorized Execution](../internals/vectorized-execution.md) — Batch processing details
- [Storage Engine](../internals/storage-engine.md) — Storage layer optimization
- [Benchmarks](../internals/benchmarks.md) — Performance metrics
