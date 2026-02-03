# Internals

Deep dive into Uni's implementation details.

<div class="feature-grid">

<div class="feature-card">

### [Vectorized Execution](vectorized-execution.md)
Batch processing, Arrow integration, and SIMD-accelerated operations.

</div>

<div class="feature-card">

### [Storage Engine](storage-engine.md)
Lance integration, LSM design, and the L0/L1/L2 layer architecture.

</div>

<div class="feature-card">

### [Query Planning](query-planning.md)
Planner internals, optimization passes, and physical plan generation.

</div>

<div class="feature-card">

### [Query Rewriting](query-rewriting.md)
Function-to-predicate transformations, rewrite rules, and predicate pushdown optimization.

</div>

<div class="feature-card">

### [Benchmarks](benchmarks.md)
Performance measurements, methodology, and comparison data.

</div>

</div>

## Implementation Overview

Uni's internals are organized into four major subsystems:

### Query Processing

1. **Parser** — Cypher syntax to AST (based on sqlparser)
2. **Rewriter** — Function-to-predicate transformations (compile-time)
3. **Planner** — Logical plan with optimization passes
4. **Executor** — Vectorized physical operators

### Runtime

1. **L0 Buffer** — In-memory SimpleGraph graph for mutations
2. **CSR Cache** — Compressed adjacency for O(1) traversal
3. **Property Manager** — Lazy loading from Lance

### Storage

1. **Lance Datasets** — Columnar storage with versioning
2. **WAL** — Write-ahead log for durability
3. **Indexes** — Vector (HNSW/IVF_PQ), scalar (BTree)

### Object Store

1. **object_store** crate for S3/GCS/Azure/local
2. **Local caching** for frequently accessed data
3. **Manifest files** for snapshot isolation

## Key Design Decisions

| Decision | Rationale |
|----------|-----------|
| Vectorized execution | 100-500x faster than row-at-a-time |
| Lance for storage | Native vector indexes + versioning |
| SimpleGraph for in-memory | Fast graph algorithms in Rust |
| Single-writer model | Simplicity over distributed complexity |
| Query rewriting | Compile-time optimization for predicate pushdown |

## Next Steps

Start with [Vectorized Execution](vectorized-execution.md) to understand how queries are processed.
