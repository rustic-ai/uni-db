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

Push filters to storage for massive I/O reduction:

```cypher
// Good: Filter pushed to Lance
MATCH (p:Paper)
WHERE p.year > 2020 AND p.venue = 'NeurIPS'
RETURN p.title

// Bad: Filter applied after full scan
MATCH (p:Paper)
WHERE p.title CONTAINS 'Transformer'  // Cannot push CONTAINS
RETURN p.title
```

**Pushable Predicates:**
- `=`, `<>`, `<`, `>`, `<=`, `>=`
- `IN [list]`
- `IS NULL`, `IS NOT NULL`
- `AND` combinations of above

**Non-Pushable Predicates:**
- `CONTAINS`, `STARTS WITH`, `ENDS WITH`
- Function calls: `lower(x) = 'value'`
- `OR` with different properties

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
-- Check if index is used
EXPLAIN MATCH (p:Paper) WHERE p.year = 2023 RETURN p.title

-- Create index if missing
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
            algorithm: VectorAlgo::Hnsw { m: 32, ef_construction: 200 },
            metric: VectorMetric::Cosine,
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
-- Composite index for common query pattern
CREATE INDEX paper_venue_year FOR (p:Paper) ON (p.venue, p.year)

-- Query uses the composite index
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
let bulk = db.bulk_writer().batch_size(50_000).build()?;

// BulkWriter with smaller batches (less memory)
let bulk = db.bulk_writer().batch_size(5_000).build()?;
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

Trigger compaction after bulk operations:

```bash
# Manual compaction (via Cypher)
uni query "CALL uni.admin.compact() YIELD files_compacted, duration_ms RETURN *" --path ./storage
```

For label/edge-specific compaction, use the Rust API:

```rust
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
| High Traverse time | Cold cache, many edges | Warm cache, limit hops |
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
config.parallelism = 8;   // Parallel workers
config.batch_size = 4096; // Rows per morsel

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

1. **Smaller batch sizes**: use `UniConfig.batch_size` or `BulkWriter.batch_size()`
2. **Smaller caches**: Reduce `UniConfig.cache_size`
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
    .hybrid("./local-meta", "s3://my-bucket/graph-data")
    .cloud_config(cfg)
    .build()
    .await?;

// Google Cloud Storage
let cfg = CloudStorageConfig::Gcs {
    bucket: "my-gcs-bucket".to_string(),
    service_account_path: None,
    service_account_key: None,
};

let db = Uni::open("./local-meta")
    .hybrid("./local-meta", "gs://my-gcs-bucket/graph-data")
    .cloud_config(cfg)
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
    .hybrid("./local-meta", "s3://my-bucket/graph-data")
    .cloud_config(cfg)
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

Optimize flush interval for cloud cost vs. durability:

| Cloud Provider | Recommended Interval | Rationale |
|----------------|---------------------|-----------|
| S3 | 5-30s | Balance PUT request costs |
| GCS | 5-30s | Similar to S3 |
| Azure Blob | 5-30s | Similar to S3 |
| Local SSD | 1-5s | No cost concern, minimize data at risk |

```rust
use uni_db::{Uni, UniConfig};

// Cost-optimized for cloud (fewer API calls)
let mut config = UniConfig::default();
config.auto_flush_threshold = 50_000;
config.auto_flush_interval = Some(Duration::from_secs(30));
config.auto_flush_min_mutations = 100;

let db = Uni::open("./local-meta")
    .hybrid("./local-meta", "s3://my-bucket/data")
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
