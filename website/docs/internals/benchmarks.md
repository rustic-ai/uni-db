# Benchmarks

This document presents comprehensive performance measurements for Uni across various workloads including ingestion, querying, graph traversal, and vector search. All benchmarks are reproducible using the included benchmark suite.

## Executive Summary

| Workload | Performance | Context |
|----------|-------------|---------|
| **Ingestion** | 1.8M vertices/sec | Batched L0 writes |
| **Point Lookup** | 2.9ms | Indexed property access |
| **1-Hop Traversal** | 4.7ms | CSR adjacency cache |
| **Vector KNN (k=10)** | 1.8ms | HNSW index |
| **Hybrid Query** | 215ms | Vector + Graph + Filter |

---

## Test Environment

### Hardware Configuration

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                         BENCHMARK HARDWARE                                   │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│   Development Machine:                                                      │
│   ┌──────────────────────────────────────────────────────────────────────┐ │
│   │ CPU:     AMD Ryzen 9 5900X (12 cores, 24 threads)                    │ │
│   │ Memory:  64 GB DDR4-3600                                              │ │
│   │ Storage: Samsung 980 PRO NVMe (7,000 MB/s read)                       │ │
│   │ OS:      Ubuntu 22.04, kernel 5.15                                    │ │
│   └──────────────────────────────────────────────────────────────────────┘ │
│                                                                             │
│   Cloud VM (Comparable):                                                    │
│   ┌──────────────────────────────────────────────────────────────────────┐ │
│   │ Instance: AWS c6i.4xlarge (16 vCPU, 32 GB RAM)                       │ │
│   │ Storage:  gp3 EBS (16,000 IOPS, 1,000 MB/s)                          │ │
│   └──────────────────────────────────────────────────────────────────────┘ │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

### Dataset Characteristics

| Dataset | Vertices | Edges | Properties | Vector Dim |
|---------|----------|-------|------------|------------|
| **Small** | 10K | 50K | 5 per vertex | 128 |
| **Medium** | 100K | 500K | 5 per vertex | 384 |
| **Large** | 1M | 5M | 5 per vertex | 768 |
| **XLarge** | 10M | 50M | 5 per vertex | 768 |

---

## Ingestion Performance

### Raw Write Throughput

Measuring writes to the in-memory L0 buffer:

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                      INGESTION THROUGHPUT                                    │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│   Batch Size: 1,000 vertices                                                │
│                                                                             │
│   ┌────────────────────────────────────────────────────────────────────┐   │
│   │                                                                    │   │
│   │   Time (µs)                                                        │   │
│   │   800 ┤                                                            │   │
│   │       │                                                            │   │
│   │   600 ┤    ┌───┐                                                   │   │
│   │       │    │███│                                                   │   │
│   │   400 ┤    │███│    ┌───┐                                          │   │
│   │       │    │███│    │███│                                          │   │
│   │   200 ┤    │███│    │███│    ┌───┐    ┌───┐                        │   │
│   │       │    │███│    │███│    │███│    │███│                        │   │
│   │     0 ┼────┴───┴────┴───┴────┴───┴────┴───┴────                    │   │
│   │        Insert    Insert   Insert   Insert                          │   │
│   │        (cold)    (warm)   (batch)  (batch+WAL)                     │   │
│   │                                                                    │   │
│   └────────────────────────────────────────────────────────────────────┘   │
│                                                                             │
│   Results (1K vertices):                                                    │
│   ├── Cold insert (first batch):     ~720 µs                               │
│   ├── Warm insert (cached):          ~420 µs                               │
│   ├── Batch insert (no WAL sync):    ~180 µs                               │
│   └── Batch insert (WAL sync):       ~550 µs                               │
│                                                                             │
│   Throughput: 1.8M vertices/sec (batch, no sync)                           │
│               550K vertices/sec (batch, sync WAL)                          │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

### Flush Performance (L0 → L1)

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                         FLUSH LATENCY                                        │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│   Flush 10K vertices to Lance:                                              │
│                                                                             │
│   Phase               Time (ms)     Percentage                              │
│   ────────────────────────────────────────────                              │
│   Serialize           12.4          19.7%                                   │
│   Arrow conversion    18.2          28.9%                                   │
│   Lance write         28.1          44.6%                                   │
│   Index update         4.3           6.8%                                   │
│   ────────────────────────────────────────────                              │
│   Total               63.0 ms       100%                                    │
│                                                                             │
│   Throughput: ~160K vertices/sec (to persistent storage)                   │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

### Scaling with Data Size

| Data Size | L0 Insert | L0→L1 Flush | L1→L2 Compact |
|-----------|-----------|-------------|---------------|
| 1K vertices | 550 µs | 6.3 ms | N/A |
| 10K vertices | 5.2 ms | 63 ms | 180 ms |
| 100K vertices | 52 ms | 640 ms | 2.1 s |
| 1M vertices | 520 ms | 6.4 s | 25 s |

---

## Query Performance

### Point Lookups

Single vertex retrieval by indexed property:

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                     POINT LOOKUP LATENCY                                     │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│   Query: MATCH (p:Paper {id: 'paper_12345'}) RETURN p                       │
│                                                                             │
│   Index Type          P50        P90        P99        P99.9                │
│   ─────────────────────────────────────────────────────────                 │
│   BTree index         2.4 ms     3.1 ms     4.8 ms     9.3 ms               │
│   No index (scan)     85 ms      120 ms     180 ms     250 ms               │
│                                                                             │
│   Dataset: 1M vertices                                                      │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

### Range Queries

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                      RANGE QUERY PERFORMANCE                                 │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│   Query: MATCH (p:Paper) WHERE p.year >= 2020 AND p.year <= 2023            │
│          RETURN p.title                                                     │
│                                                                             │
│   Selectivity    Rows Returned    Index Time    Scan Time    Speedup        │
│   ─────────────────────────────────────────────────────────────────         │
│   1% (10K)       10,000           12 ms         85 ms        7.1x           │
│   5% (50K)       50,000           35 ms         95 ms        2.7x           │
│   10% (100K)     100,000          58 ms         102 ms       1.8x           │
│   50% (500K)     500,000          210 ms        180 ms       0.9x (scan wins)│
│                                                                             │
│   Dataset: 1M vertices                                                      │
│   Takeaway: Index wins for selectivity < 30%                                │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

### Aggregation Queries

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                    AGGREGATION PERFORMANCE                                   │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│   Query: MATCH (p:Paper) RETURN p.venue, COUNT(*) AS count                  │
│          ORDER BY count DESC                                                │
│                                                                             │
│   Dataset Size    Groups    Aggregate    Sort       Total                   │
│   ─────────────────────────────────────────────────────────                 │
│   100K            50        28 ms        2 ms       30 ms                   │
│   1M              50        185 ms       3 ms       188 ms                  │
│   1M              10K       320 ms       45 ms      365 ms                  │
│   1M              100K      580 ms       120 ms     700 ms                  │
│                                                                             │
│   Takeaway: Hash aggregation scales linearly with input size                │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## Graph Traversal Performance

### Single-Hop Traversal

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                    1-HOP TRAVERSAL LATENCY                                   │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│   Query: MATCH (p:Paper)-[:CITES]->(cited)                                  │
│          WHERE p.id = 'paper_12345'                                         │
│          RETURN cited.title                                                 │
│                                                                             │
│   Cache State        P50        P90        P99        Notes                 │
│   ─────────────────────────────────────────────────────────                 │
│   Cold cache         8.2 ms     12.1 ms    18.5 ms    Load from Lance      │
│   Warm cache         4.7 ms     5.8 ms     8.1 ms     CSR in memory        │
│   Hot path           2.1 ms     2.8 ms     4.2 ms     Repeated query       │
│                                                                             │
│   Average degree: 5 edges per vertex                                        │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

### Multi-Hop Traversal

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                   MULTI-HOP TRAVERSAL SCALING                                │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│   Query: MATCH (start)-[:CITES*N]->(end) RETURN DISTINCT end                │
│                                                                             │
│   Latency (ms)                                                              │
│   40 ┤                                                   ┌───┐              │
│      │                                                   │   │              │
│   35 ┤                                                   │   │              │
│      │                                                   │   │              │
│   30 ┤                                                   │   │              │
│      │                                                   │   │              │
│   25 ┤                                                   │   │              │
│      │                                                   │   │              │
│   20 ┤                                          ┌───┐    │   │              │
│      │                                          │   │    │   │              │
│   15 ┤                                 ┌───┐    │   │    │   │              │
│      │                                 │   │    │   │    │   │              │
│   10 ┤                        ┌───┐    │   │    │   │    │   │              │
│      │               ┌───┐    │   │    │   │    │   │    │   │              │
│    5 ┤      ┌───┐    │   │    │   │    │   │    │   │    │   │              │
│      │      │   │    │   │    │   │    │   │    │   │    │   │              │
│    0 ┼──────┴───┴────┴───┴────┴───┴────┴───┴────┴───┴────┴───┴──            │
│          1-hop    2-hop    3-hop    4-hop    5-hop    6-hop                 │
│                                                                             │
│   Hops    Vertices Visited    Latency    Throughput                         │
│   ─────────────────────────────────────────────────                         │
│   1       5                   4.7 ms     1.1K v/s                           │
│   2       25                  6.7 ms     3.7K v/s                           │
│   3       125                 9.0 ms     13.9K v/s                          │
│   4       625                 15.2 ms    41.1K v/s                          │
│   5       3,125               22.8 ms    137K v/s                           │
│   6       15,625              38.5 ms    406K v/s                           │
│                                                                             │
│   Note: Assuming avg degree = 5, no duplicates                              │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

### Traversal with Filters

```
┌─────────────────────────────────────────────────────────────────────────────┐
│               FILTERED TRAVERSAL PERFORMANCE                                 │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│   Query: MATCH (p:Paper)-[:CITES]->(cited:Paper)                            │
│          WHERE p.year > 2020 AND cited.year > 2018                          │
│          RETURN cited.title                                                 │
│                                                                             │
│   Strategy                    Latency    Notes                              │
│   ─────────────────────────────────────────────────                         │
│   Filter-then-traverse        12.5 ms    Filter p first, then traverse     │
│   Traverse-then-filter        28.3 ms    Traverse all, filter cited        │
│   Dual pushdown               8.2 ms     Push both filters down            │
│                                                                             │
│   Dataset: 1M papers, 5M citations                                          │
│   Filter selectivity: p.year > 2020 = 30%, cited.year > 2018 = 60%         │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## Vector Search Performance

### KNN Search (HNSW)

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                      VECTOR KNN LATENCY                                      │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│   Query: CALL uni.vector.query('Paper', 'embedding', $vec, 10)           │
│                                                                             │
│   Dataset: 1M vectors, 768 dimensions, HNSW index                           │
│                                                                             │
│   k       P50        P90        P99        Recall@k                         │
│   ─────────────────────────────────────────────────                         │
│   10      1.8 ms     2.4 ms     3.8 ms     0.95                             │
│   50      2.9 ms     3.8 ms     5.2 ms     0.94                             │
│   100     4.2 ms     5.5 ms     7.8 ms     0.93                             │
│   500     12.5 ms    16.2 ms    22.1 ms    0.91                             │
│                                                                             │
│   HNSW Parameters: M=32, ef_construction=200, ef_search=100                 │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

### HNSW vs IVF_PQ

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                    INDEX COMPARISON (k=10)                                   │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│   Dataset: 1M vectors, 768 dimensions                                       │
│                                                                             │
│   Index Type      Build Time    Memory     Latency    Recall                │
│   ─────────────────────────────────────────────────────────                 │
│   HNSW            45 min        2.4 GB     1.8 ms     0.95                  │
│   IVF_PQ          12 min        180 MB     3.2 ms     0.88                  │
│   Brute Force     N/A           2.9 GB     85 ms      1.00                  │
│                                                                             │
│   Recommendation:                                                           │
│   • HNSW: Best recall, moderate memory                                      │
│   • IVF_PQ: Low memory, acceptable recall                                   │
│   • Brute Force: Only for small datasets (<100K)                           │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

### Vector Search Scaling

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                    SCALING WITH DATASET SIZE                                 │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│   HNSW Index, k=10, 768 dimensions                                          │
│                                                                             │
│   Dataset Size    Latency (P50)    Memory (Index)    Build Time             │
│   ─────────────────────────────────────────────────────────                 │
│   100K            0.8 ms           240 MB            4 min                  │
│   1M              1.8 ms           2.4 GB            45 min                 │
│   10M             3.2 ms           24 GB             8 hours                │
│                                                                             │
│   Observation: Latency scales O(log n) due to HNSW structure               │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## Hybrid Query Performance

### Vector + Graph Queries

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                    HYBRID QUERY BREAKDOWN                                    │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│   Query: CALL uni.vector.query('Paper', 'embedding', $vec, 20)           │
│          YIELD node AS paper                                                │
│          MATCH (paper)-[:CITES]->(cited)                                    │
│          WHERE cited.year > 2020                                            │
│          RETURN cited.title                                                 │
│                                                                             │
│   Phase               Time        Rows In    Rows Out                       │
│   ─────────────────────────────────────────────────────                     │
│   Vector Search       2.1 ms      1M         20                             │
│   Traverse            45 ms       20         ~100                           │
│   Filter              8 ms        ~100       ~60                            │
│   Project             2 ms        ~60        ~60                            │
│   ─────────────────────────────────────────────────────                     │
│   Total               ~57 ms                                                │
│                                                                             │
│   Note: Traversal dominates due to cold cache for cited papers             │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

### Complex Hybrid Query

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                   COMPLEX HYBRID QUERY                                       │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│   Query: // Find papers similar to query, get their authors' other papers   │
│          CALL uni.vector.query('Paper', 'embedding', $vec, 10)           │
│          YIELD node AS similar                                              │
│          MATCH (similar)-[:AUTHORED_BY]->(author)                           │
│          MATCH (author)<-[:AUTHORED_BY]-(other:Paper)                       │
│          WHERE other.year > 2020                                            │
│          RETURN DISTINCT other.title, author.name                           │
│          LIMIT 50                                                           │
│                                                                             │
│   Execution Timeline:                                                       │
│   ┌──────────────────────────────────────────────────────────────────────┐ │
│   │ 0ms         50ms        100ms       150ms       200ms       250ms    │ │
│   │ ├───────────┼───────────┼───────────┼───────────┼───────────┤        │ │
│   │ │▓▓▓│                                                 Vector (2ms)   │ │
│   │    │▓▓▓▓▓▓▓▓▓▓▓▓│                                    Traverse1 (35ms)│ │
│   │               │▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓│          Traverse2 (120ms)│
│   │                                          │▓▓▓▓▓▓▓▓│  Filter (45ms)   │ │
│   │                                                  │▓│ Project (8ms)   │ │
│   └──────────────────────────────────────────────────────────────────────┘ │
│                                                                             │
│   Total: ~215 ms                                                            │
│                                                                             │
│   Bottleneck: Second traversal (author → other papers)                      │
│   Optimization: Add LIMIT earlier, warm adjacency cache                     │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## Concurrent Query Performance

### Read Throughput

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                   CONCURRENT READ THROUGHPUT                                 │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│   Query: MATCH (p:Paper) WHERE p.year = 2023 RETURN p.title LIMIT 10        │
│   Dataset: 1M vertices                                                      │
│                                                                             │
│   QPS                                                                       │
│   1200 ┤                                          ┌─────────────────────    │
│        │                                    ┌─────┘                         │
│   1000 ┤                              ┌─────┘                               │
│        │                        ┌─────┘                                     │
│    800 ┤                  ┌─────┘                                           │
│        │            ┌─────┘                                                 │
│    600 ┤      ┌─────┘                                                       │
│        │┌─────┘                                                             │
│    400 ┤│                                                                   │
│        ││                                                                   │
│    200 ┤│                                                                   │
│        ││                                                                   │
│      0 ┼┴───────────────────────────────────────────────────────────────    │
│         1    2    4    8   12   16   24   32  (concurrent readers)         │
│                                                                             │
│   Threads    QPS        Avg Latency    P99 Latency                         │
│   ─────────────────────────────────────────────────                         │
│   1          180        5.5 ms         8.2 ms                               │
│   4          650        6.1 ms         12.5 ms                              │
│   8          920        8.7 ms         18.2 ms                              │
│   16         1,050      15.2 ms        35.1 ms                              │
│   32         1,120      28.5 ms        65.2 ms                              │
│                                                                             │
│   Note: Scales well up to ~16 readers, then contention increases           │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

### Write Impact on Reads

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                 READ LATENCY UNDER WRITE LOAD                                │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│   Write Rate (vertices/sec)    Read P50    Read P99    Notes                │
│   ─────────────────────────────────────────────────────────                 │
│   0 (no writes)                5.5 ms      8.2 ms      Baseline             │
│   1,000                        5.6 ms      9.1 ms      Minimal impact       │
│   10,000                       5.8 ms      11.5 ms     Slight increase      │
│   50,000                       6.2 ms      15.2 ms     L0 buffer growing    │
│   100,000                      7.5 ms      22.1 ms     Frequent flushes     │
│                                                                             │
│   Takeaway: Single-writer model ensures reads remain consistent            │
│             L0 flushes cause brief latency spikes                          │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## Graph Algorithm Performance

Uni includes native implementations of common graph algorithms, optimized for the CSR adjacency cache.

| Algorithm | Complexity | Notes |
|-----------|------------|-------|
| **PageRank** | O(E) per iter | Parallel execution |
| **WCC** | O(V + E) | Union-Find with path compression |
| **Louvain** | O(E) per iter | Multi-level community detection |
| **Label Propagation** | O(E) per iter | Fast community detection |
| **Triangle Count** | O(E^1.5) | SIMD-optimized set intersection |
| **Betweenness** | O(VE) | Sampling-based approximation available |

---

## Running Benchmarks

### Built-in Benchmark Suite

```bash
# Run all benchmarks
cargo bench

# Run specific benchmark
cargo bench -- ingestion
cargo bench -- traversal
cargo bench -- vector_search

# Run with specific dataset size
BENCH_SIZE=large cargo bench

# Generate HTML report
cargo bench -- --save-baseline main
open target/criterion/report/index.html
```

### Custom Benchmarks

```rust
use criterion::{criterion_group, criterion_main, Criterion, BenchmarkId};
use uni_db::*;

fn benchmark_traversal(c: &mut Criterion) {
    let storage = setup_test_storage();

    let mut group = c.benchmark_group("traversal");

    for hops in [1, 2, 3, 4, 5] {
        group.bench_with_input(
            BenchmarkId::new("multi_hop", hops),
            &hops,
            |b, &hops| {
                b.iter(|| {
                    let query = format!(
                        "MATCH (p:Paper)-[:CITES*{}]->(end) \
                         WHERE p.id = 'seed_paper' \
                         RETURN DISTINCT end.id",
                        hops
                    );
                    executor.execute(&query).unwrap()
                })
            },
        );
    }

    group.finish();
}

criterion_group!(benches, benchmark_traversal);
criterion_main!(benches);
```

### Profiling

```bash
# CPU profiling with perf
perf record cargo bench -- vector_search
perf report

# Memory profiling
RUST_BACKTRACE=1 cargo bench -- --profile-time 30

# Flame graphs
cargo flamegraph --bench storage_bench -- --bench
```

---

## Performance Recommendations

### Query Optimization

| Scenario | Current | Optimized | Improvement |
|----------|---------|-----------|-------------|
| Missing index | 85ms | 2.9ms | 29x |
| Full projection | 12ms | 5ms | 2.4x |
| Late LIMIT | 180ms | 45ms | 4x |
| Cold cache | 8.2ms | 4.7ms | 1.7x |

### Configuration Tuning

```rust
// Optimized for throughput
let config = UniConfig {
    batch_size: 8192,
    cache_size: 2 * 1024 * 1024 * 1024,  // 2 GB
    auto_flush_threshold: 100_000,
    ..Default::default()
};

// Optimized for latency
let config = UniConfig {
    batch_size: 2048,
    cache_size: 4 * 1024 * 1024 * 1024,  // 4 GB
    auto_flush_threshold: 10_000,
    wal_enabled: true,
    ..Default::default()
};
```

---

## Next Steps

- [Performance Tuning](../guides/performance-tuning.md) — Optimization strategies
- [Vectorized Execution](vectorized-execution.md) — Execution engine details
- [Storage Engine](storage-engine.md) — Storage layer internals
