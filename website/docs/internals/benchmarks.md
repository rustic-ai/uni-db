# Benchmarks

Measured performance data for Uni across ingestion, querying, graph traversal, vector search, and graph algorithms. All numbers come from the Criterion benchmark suite (`cargo bench`) and are reproducible.

**Last updated:** 2026-04-01

!!! info "Provenance"
    Numbers on this page are a point-in-time measurement, not a live contract.
    Any table of measurements should carry:
    `Run: <date> · uni <version> · <CPU>/<RAM> · <dataset>`.
    The figures below were last regenerated **2026-04-01** against the then-current
    `main`; hardware and dataset were not recorded at the time, so treat them as
    indicative ordering, not absolute throughput. Re-run `cargo bench` locally
    before quoting any of them.

## Executive Summary

| Workload | Performance | Dataset | Notes |
|----------|-------------|---------|-------|
| **Point Lookup** | 3.0 ms | 1K vertices | By name, after flush |
| **1-Hop Traversal** | 4.7 ms | 1K vertices | CSR adjacency, warm cache |
| **Vector KNN (k=10)** | 6.9 ms | 1K vectors, 128d | HNSW indexed |
| **Aggregation (AVG)** | 3.1 ms | 1K vertices | Single-column aggregate |
| **Full Scan COUNT** | 26 ms | 1K vertices | Scales linearly with data |
| **Hybrid Vector+Graph** | 13.7 ms | 1K n / 1K e | Vector filter + 1-hop |
| **Micro: Raw Vector Search** | 1.9 ms | 1K vectors, 128d | Direct storage layer |
| **Micro: Neighbor Access** | 2.0 ns | 1K vertices, 50-deg | CSR slice lookup |

---

## Test Environment

All benchmarks run on a single machine using Criterion 0.5 with `harness = false`. Each measurement uses 10 samples (comprehensive/mutation/algo suites) or 100 samples (micro suite). Datasets use in-memory or temp-dir storage with `auto_flush_threshold: 100_000`.

### Benchmark Configurations

Three dataset sizes used for scaling analysis:

| Config | Vertices | Edges | Edge Density | Properties per Vertex |
|--------|----------|-------|--------------|-----------------------|
| **Small** | 1,000 | 1,000 | 1.0 e/v | 3 (name, age, embedding[128]) |
| **Medium** | 5,000 | 2,500 | 0.5 e/v | 3 (name, age, embedding[128]) |
| **Large** | 8,000 | 12,000 | 1.5 e/v | 3 (name, age, embedding[128]) |

Schema: `Person` label with `name` (String), `age` (Int32), `embedding` (Vector[128]). Edge type: `KNOWS`.

---

## Ingestion Performance

### Cypher INSERT Throughput

Per-statement ingestion via `CREATE` inside a transaction. Each vertex carries a name, age, and 128-dimensional embedding vector.

| Method | 1K vertices | 5K vertices | 8K vertices |
|--------|-------------|-------------|-------------|
| **String interpolation** | 1.89 s | 10.47 s | 12.59 s |
| **Parameterized query** | 1.51 s | 8.61 s | 13.14 s |
| **Per-vertex rate (parameterized)** | 662 v/s | 581 v/s | 609 v/s |

Parameterized queries are **~20% faster** at 1K scale because the plan cache can reuse parsed/planned queries and only substitute parameter values. At 8K the gap narrows as L0 buffer pressure dominates.

### Flush Performance (L0 to L1)

Flush converts the in-memory L0 buffer to persistent Lance columnar storage:

| Dataset | Flush Time | Throughput |
|---------|------------|------------|
| 1K vertices + 500 edges | 748 ms | ~2.0K entities/sec |
| 5K vertices + 1.25K edges | 1.19 s | ~5.3K entities/sec |
| 8K vertices + 6K edges | 1.54 s | ~9.1K entities/sec |

Flush time scales linearly with entity count. Throughput improves with batch size due to amortized Arrow serialization and Lance write overhead.

---

## Mutation Performance

Benchmarked with in-memory Uni instances, 100-node operations per measurement:

| Operation | Time | Per-Op Rate |
|-----------|------|-------------|
| **CREATE 100 nodes** | 137.8 ms | 1.38 ms/node |
| **SET 100 properties** | 6.7 ms | 67 us/prop |
| **DELETE 100 nodes** | 3.5 ms | 35 us/node |
| **CREATE 50 + MATCH** | 74.1 ms | 1.48 ms/create + query |
| **MERGE 100 nodes** (50 create + 50 match) | 336.8 ms | 3.37 ms/merge |

Key observations:

- **MERGE is ~2.4x slower than CREATE** because each MERGE must first check existence (read + conditional write), while CREATE is write-only.
- **SET and DELETE are fast** because they operate on already-indexed vertices in the L0 buffer.
- **Transaction overhead** is included in all measurements (tx begin + commit).

---

## Query Performance

### Point Lookup

Single-vertex retrieval by string property after bulk insert + flush:

| Dataset | Latency | Notes |
|---------|---------|-------|
| 1K vertices | 3.01 ms | Scan-based (no BTree index) |
| 5K vertices | 2.92 ms | Nearly constant |
| 8K vertices | 3.44 ms | Nearly constant |

Point lookup scales approximately O(1) with dataset size — the query planner pushes the filter predicate into the Lance scan, enabling predicate pushdown at the storage layer.

### Aggregation

Full-scan aggregations over all vertices:

| Query | 1K n | 5K n | 8K n | Scaling |
|-------|------|------|------|---------|
| `COUNT(n)` | 25.9 ms | 108.1 ms | 186.1 ms | Linear |
| `GROUP BY n.age, COUNT(n)` | 26.4 ms | 108.7 ms | 181.8 ms | Linear |
| `AVG(n.age)` | 3.10 ms | 2.93 ms | 3.18 ms | Constant |

`COUNT` and `GROUP BY` require a full table scan — 8x more data yields ~7x more time. `AVG` uses columnar statistics and returns in constant time regardless of dataset size.

### Indexed Queries

Queries using BTree, HNSW, or fulltext indexes created via Cypher DDL:

| Query Type | 1K n | 5K n | 8K n | Scaling |
|------------|------|------|------|---------|
| **Scalar equality** (`age = 25`) | 2.82 ms | 3.08 ms | 3.36 ms | ~Constant |
| **Scalar range** (`age >= 20 AND age <= 30`) | 3.07 ms | 3.45 ms | 3.78 ms | ~Constant |
| **Vector KNN** (k=10, HNSW) | 6.89 ms | 9.85 ms | 11.29 ms | Sub-linear |
| **Fulltext match** (exact name) | 3.01 ms | 3.26 ms | 3.27 ms | ~Constant |

BTree and fulltext indexes deliver nearly constant-time lookups. Vector KNN scales sub-linearly (O(log n)) due to the HNSW graph structure.

### Index Creation

Time to build an index on an already-populated and flushed dataset:

| Index Type | 1K n | 5K n | 8K n |
|------------|------|------|------|
| **Scalar BTree** | 1.47 ms | 2.08 ms | 3.18 ms |
| **Vector HNSW** | 1.44 ms | 2.13 ms | 2.76 ms |
| **Fulltext** | 1.36 ms | 2.17 ms | 2.93 ms |

Index creation scales linearly with dataset size. All three index types have comparable build times at these scales.

### Order By + Limit

`MATCH (n:Person) RETURN n.name, n.age ORDER BY ... LIMIT 10`:

| Sort | 1K n | 5K n | 8K n |
|------|------|------|------|
| `ORDER BY n.age` | 2.93 ms | 3.45 ms | 3.97 ms |
| `ORDER BY n.name DESC` | 2.98 ms | 3.82 ms | 4.60 ms |

Sub-linear scaling thanks to the `LIMIT 10` clause — the engine uses a top-N heap instead of a full sort.

---

## Graph Traversal Performance

### Cypher Traversal (via Comprehensive Suite)

Starting from a single vertex (`Person_0`), following `KNOWS` edges:

| Hops | 1K n / 1K e | 5K n / 2.5K e | 8K n / 12K e | Notes |
|------|-------------|---------------|--------------|-------|
| 1-hop | 4.75 ms | 4.31 ms | 5.08 ms | ~Constant |
| 2-hop | 4.86 ms | 4.83 ms | 4.89 ms | ~Constant |
| 3-hop | 4.74 ms | 4.13 ms | 4.98 ms | ~Constant |

Traversal from a single start vertex stays flat regardless of total graph size — the working set (reachable neighbors) is determined by local topology, not total vertex count.

### Low-Level Traversal (via Micro Suite)

Direct storage-layer traversal benchmarks, bypassing the Cypher parser/planner:

| Benchmark | Time | Notes |
|-----------|------|-------|
| **1-hop, 100-node chain** | 750 us | `load_subgraph` from storage |
| **Neighbor iteration** (50-degree vertex) | 9.71 ns | CSR in-memory iteration |
| **Neighbor access** (50-degree vertex) | 2.00 ns | Slice pointer lookup |

The CSR adjacency structure provides near-zero-cost neighbor access — a single slice lookup followed by sequential memory reads.

### SimpleGraph Construction

In-memory graph building performance (10K vertices, 50K edges):

| Strategy | Time | Notes |
|----------|------|-------|
| Standard (`add_vertex` + `add_edge`) | 3.27 ms | Hash map lookups per insert |
| Optimized (`with_capacity` + `add_edge_unchecked`) | 2.53 ms | Pre-allocated, no validation |

Pre-allocation saves ~23% by avoiding hash map resizing.

---

## Vector Search Performance

### Raw Vector Search (via Micro Suite)

Direct storage-layer vector search, bypassing Cypher:

| Dataset | Dimensions | k | Latency |
|---------|-----------|---|---------|
| 1K vectors | 128 | 10 | 1.92 ms |

### Cypher Vector Search (via Comprehensive Suite)

Vector similarity filter via Cypher (`WHERE vector_similarity(...) > 0.8`):

These figures predate a correctness fix: the predicate was then rewritten into a
top-100 index scan that ignored the threshold's meaning and capped the answer at
100 rows. It is now an exact per-row filter over the label scan, so expect
linear growth; `~=` / `uni.vector.query` is the index-served form.

| Dataset | Latency | Notes |
|---------|---------|-------|
| 1K n | 6.12 ms | Includes Cypher parse + plan |
| 5K n | 6.91 ms | Sub-linear growth |
| 8K n | 7.80 ms | Sub-linear growth |

### Indexed KNN

`CALL uni.vector.query('Person', 'embedding', $vec, 10)` with HNSW index:

| Dataset | Latency | Notes |
|---------|---------|-------|
| 1K n | 6.89 ms | HNSW index + Cypher overhead |
| 5K n | 9.85 ms | O(log n) scaling |
| 8K n | 11.29 ms | O(log n) scaling |

---

## Hybrid Query Performance

Combined vector similarity + graph traversal query:

```cypher
MATCH (a:Person)-[:KNOWS]->(b:Person)
WHERE vector_similarity(a.embedding, $vec) > 0.8 AND b.age >= 1
RETURN b.name
```

| Dataset | Latency | Notes |
|---------|---------|-------|
| 1K n / 1K e | 13.73 ms | Low edge density (1.0 e/v) |
| 5K n / 2.5K e | 26.30 ms | Low edge density (0.5 e/v) |
| 8K n / 12K e | 92.45 ms | Higher density (1.5 e/v) |

Hybrid queries show **super-linear growth** with edge density. At 8K/12K, the vector filter matches more start vertices (8x more nodes) and each has more outgoing edges (1.5x density), multiplying the traversal fan-out. This is the most edge-density-sensitive workload.

---

## Graph Algorithm Performance

All algorithms benchmarked on random graphs with 1,000 nodes and 5 edges per node (avg degree = 5). Execution includes Cypher parsing, planning, graph projection, and algorithm execution.

| Algorithm | Time | Complexity | Notes |
|-----------|------|------------|-------|
| **PageRank** | 7.24 ms | O(E) per iteration | Iterative convergence |
| **WCC** | 5.83 ms | O(V + E) | Union-Find with path compression |
| **SCC** | 6.07 ms | O(V + E) | Tarjan's algorithm |
| **Louvain** | 10.09 ms | O(E) per iteration | Multi-level community detection |
| **Label Propagation** | 10.45 ms | O(E) per iteration | Fast community detection |
| **Betweenness** | 7.68 ms | O(VE) | Sampling (100 sources) |
| **Closeness** | 7.68 ms | O(VE) | BFS from all vertices |
| **Node Similarity** | 12.05 ms | O(V^2) | Jaccard on neighbor sets |
| **Triangle Count** | 6.41 ms | O(E^1.5) | Set intersection |
| **K-Core** | 6.43 ms | O(V + E) | Iterative degree pruning |
| **Random Walk** | 7.17 ms | O(V * walk_length) | 5 steps, 1 walk per node |

At 1K nodes / 5K edges, most algorithms complete in 6-12 ms including full Cypher overhead (parse → plan → project → execute → collect). The graph projection step (loading CSR from storage) is amortized across the algorithm execution.

---

## Scaling Analysis

### What Scales Linearly (Full-Scan Bound)

These operations must touch every row and scale proportionally with data size:

| Operation | 1K | 5K | 8K | Growth Factor (1K→8K) |
|-----------|-----|-----|-----|----------------------|
| COUNT aggregation | 25.9 ms | 108.1 ms | 186.1 ms | 7.2x (data: 8x) |
| GROUP BY aggregation | 26.4 ms | 108.7 ms | 181.8 ms | 6.9x (data: 8x) |
| Flush L0→L1 | 748 ms | 1.19 s | 1.54 s | 2.1x (data: 8x) |
| Cypher ingestion | 1.51 s | 8.61 s | 13.14 s | 8.7x (data: 8x) |

### What Stays Constant (Index/Cache Bound)

These operations use indexes or local graph structure and are independent of total data size:

| Operation | 1K | 5K | 8K | Growth Factor (1K→8K) |
|-----------|-----|-----|-----|----------------------|
| Point lookup | 3.01 ms | 2.92 ms | 3.44 ms | 1.1x |
| Scalar equality (indexed) | 2.82 ms | 3.08 ms | 3.36 ms | 1.2x |
| Fulltext match (indexed) | 3.01 ms | 3.26 ms | 3.27 ms | 1.1x |
| AVG aggregation | 3.10 ms | 2.93 ms | 3.18 ms | 1.0x |
| 1-hop traversal | 4.75 ms | 4.31 ms | 5.08 ms | 1.1x |
| 3-hop traversal | 4.74 ms | 4.13 ms | 4.98 ms | 1.1x |

### What Scales Sub-Linearly (O(log n))

| Operation | 1K | 5K | 8K | Growth Factor (1K→8K) |
|-----------|-----|-----|-----|----------------------|
| Vector KNN (HNSW) | 6.89 ms | 9.85 ms | 11.29 ms | 1.6x (data: 8x) |
| Vector similarity filter | 6.12 ms | 6.91 ms | 7.80 ms | 1.3x (data: 8x) |
| ORDER BY + LIMIT | 2.93 ms | 3.45 ms | 3.97 ms | 1.4x (data: 8x) |

### What Scales Super-Linearly (Edge-Density Sensitive)

| Operation | 1K n/1K e | 5K n/2.5K e | 8K n/12K e | Notes |
|-----------|-----------|-------------|------------|-------|
| Hybrid vector+graph | 13.73 ms | 26.30 ms | 92.45 ms | 6.7x for 8x nodes + 12x edges |

The hybrid query is sensitive to both node count (more vector matches) and edge density (more traversal fan-out per match). This is expected and can be mitigated with tighter vector thresholds or earlier `LIMIT` clauses.

---

## Running Benchmarks

### Benchmark Suites

| Suite | File | Focus |
|-------|------|-------|
| `comprehensive` | `crates/uni/benches/comprehensive.rs` | Public API: ingestion, queries, indexes, traversal |
| `mutation_benchmarks` | `crates/uni/benches/mutation_benchmarks.rs` | CREATE, SET, DELETE, MERGE |
| `algo_benchmarks` | `crates/uni/benches/algo_benchmarks.rs` | Graph algorithms (PageRank, WCC, etc.) |
| `micro_benchmarks` | `crates/uni/benches/micro_benchmarks.rs` | Low-level: storage, CSR, vector search |


### Running

```bash
# Run all benchmarks
cargo bench

# Run a specific suite
cargo bench --bench comprehensive
cargo bench --bench algo_benchmarks
cargo bench --bench mutation_benchmarks
cargo bench --bench micro_benchmarks

# Run with custom dataset size (comprehensive suite)
BENCH_NODES=5000 BENCH_EDGES=2500 cargo bench --bench comprehensive

# Run only matching benchmarks
BENCH_NODES=1000 cargo bench --bench comprehensive -- traversal

# Algorithm benchmarks with custom graph size
BENCH_NODES=2000 BENCH_EDGES_PER_NODE=10 cargo bench --bench algo_benchmarks

# Save baseline and compare
cargo bench -- --save-baseline main
# ... make changes ...
cargo bench -- --baseline main

# View HTML reports
open target/criterion/report/index.html
```

### Profiling

```bash
# CPU profiling with perf
perf record cargo bench --bench comprehensive -- traversal
perf report

# Flame graphs
cargo flamegraph --bench comprehensive -- --bench
```

---

## Next Steps

- [Vectorized Execution](vectorized-execution.md) — Execution engine details
- [Storage Engine](storage-engine.md) — Storage layer internals
