# Tech Stack

## Architecture
Uni employs a layered architecture optimized for read-heavy workloads with batched mutations:

1.  **Query Layer**:
    *   **OpenCypher**: Parses and plans queries using `uni-query`.
    *   **Vectorized Engine**: Executes plans using columnar batches (Arrow). Supports complex expressions, window functions, and subqueries.
    *   **Hybrid Planning**: Optimizes across graph traversals, vector/scalar indices, and performs predicate pushdown.

2.  **Runtime Layer**:
    *   **WorkingGraph**: A topology-only (SimpleGraph) in-memory graph for algorithms.
    *   **PropertyManager**: Lazily fetches properties from disk/cache with batched loading.
    *   **L0 Buffer**: An in-memory buffer handling uncommitted mutations, supporting transactions and CRDT merge-on-write.

3.  **Storage Layer (Lance + LSM)**:
    *   **Lance**: Uses LanceDB format for columnar storage of vertices and edges.
    *   **LSM Tree**: Multi-level storage with L0 (Memory) -> L1 (Sorted Runs) -> L2 (Base Lance Files).
    *   **Adjacency**: Stores chunked CSR (Compressed Sparse Row) adjacency lists for fast traversal.
    *   **Object Store**: Supports S3/GCS with resilience features (retries, circuit breakers).

## Languages & Tools
- **Language**: Rust
- **Build System**: Cargo (Workspace)
- **Data Format**: Arrow, LanceDB, Parquet
- **Graph Query Language**: OpenCypher
