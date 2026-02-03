# Uni

<div class="hero" markdown>

## The Embedded Multi-Model Graph Database

**Uni** is a high-performance, embedded database that unifies **graph**, **vector**, **document**, and **columnar** workloads in a single engine. Built in Rust for speed and safety, Uni delivers sub-millisecond graph traversals, semantic vector search, and analytical queries—all without the operational complexity of distributed systems.

<div class="quick-links" markdown>
<a href="getting-started/installation.html" class="quick-link">Installation</a>
<a href="getting-started/quickstart.html" class="quick-link">Quick Start</a>
<a href="reference/rust-api.html" class="quick-link">API Reference</a>
<a href="https://github.com/rustic-ai/uni" class="quick-link">GitHub</a>
</div>

</div>

---

## Why Uni?

Modern applications need more than one data model. Knowledge graphs require relationships. AI features need vector similarity. Analytics demand columnar scans. Traditional approaches force you to glue together multiple databases, managing synchronization, consistency, and operational overhead.

**Uni solves this by design:**

| Capability | Description |
|------------|-------------|
| **Graph Traversals** | Navigate billions of edges with O(1) adjacency lookups via CSR-cached topology |
| **Vector Search** | Sub-2ms approximate nearest neighbor queries powered by Lance's HNSW indexes |
| **Document Storage** | Store and query nested JSON with path-based indexing |
| **Columnar Analytics** | Vectorized aggregations with predicate pushdown to storage |
| **OpenCypher Queries** | Familiar graph query syntax with vectorized execution |

---

## Key Features

<div class="feature-grid" markdown>

<div class="feature-card" markdown>

### Embedded & Serverless
Uni runs as a library in your process—no separate server, no network hops, no operational burden. Import it as a crate and start querying.

</div>

<div class="feature-card" markdown>

### Cloud-Native Storage
Persist directly to S3, GCS, or Azure Blob Storage. Local caching ensures fast reads while object stores provide infinite scale.

</div>

<div class="feature-card" markdown>

### Vectorized Execution
Queries process data in columnar batches using Apache Arrow, achieving 100-500x speedup over row-at-a-time execution.

</div>

<div class="feature-card" markdown>

### Single-Writer Simplicity
No complex distributed consensus. One writer, multiple readers, snapshot isolation. Perfect for embedded scenarios.

</div>

</div>

### Quick Example

```rust
use uni_db::Uni;

#[tokio::main]
async fn main() -> Result<(), uni_db::UniError> {
    let db = Uni::open("./my-graph").build().await?;
    let results = db
        .query_with(r#"
            CALL uni.vector.query('User', 'embedding', $query, 100)
            YIELD node, distance
            WITH node AS user, distance
            MATCH (user)-[:PURCHASED]->(product:Product)
            WHERE distance < 0.15
            RETURN product.name, COUNT(*) as purchases
            ORDER BY purchases DESC
            LIMIT 10
        "#)
        .param("query", vec![0.1, 0.2, 0.3])
        .fetch_all()
        .await?;
    Ok(())
}
```

---

## Architecture Overview

Uni's layered architecture separates concerns for maximum performance and flexibility:

```
┌─────────────────────────────────────────────────────────────┐
│                      Your Application                        │
├─────────────────────────────────────────────────────────────┤
│                    Uni (Embedded Library)                    │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────────────┐  │
│  │ Query Layer │  │   Runtime   │  │   Storage Layer     │  │
│  │  (Cypher)   │→ │  (L0 + CSR) │→ │     (Lance)         │  │
│  └─────────────┘  └─────────────┘  └──────────┬──────────┘  │
└─────────────────────────────────────────────────┼────────────┘
                                                  │
                    ┌─────────────────────────────┴─────────────────────────────┐
                    │                    Object Store                            │
                    │     ┌──────────┐    ┌──────────┐    ┌──────────┐          │
                    │     │    S3    │    │   GCS    │    │  Local   │          │
                    │     └──────────┘    └──────────┘    └──────────┘          │
                    └───────────────────────────────────────────────────────────┘
```

**[Learn more about the architecture →](concepts/architecture.md)**

---

## Performance at a Glance

| Operation | Latency | Notes |
|-----------|---------|-------|
| 1-hop traversal | <span class="metric metric-good">~4.7ms</span> | CSR-cached adjacency |
| 3-hop traversal | <span class="metric metric-good">~9.0ms</span> | Linear scaling with depth |
| Vector KNN (k=10) | <span class="metric metric-good">~1.8ms</span> | Lance HNSW index |
| Point lookup | <span class="metric metric-good">~2.9ms</span> | Property by index |
| Batch ingest (1K vertices) | <span class="metric metric-good">~550µs</span> | L0 memory buffer |
| Flush to storage | <span class="metric metric-good">~6.3ms</span> | L0 → Lance persistence |

*Benchmarks on standard cloud VM. See [Benchmarks](internals/benchmarks.md) for methodology.*

---

## Use Cases

### Knowledge Graphs & RAG
Build retrieval-augmented generation systems with graph-structured knowledge and semantic search.

```cypher
// Find contextually relevant documents via graph + vector
MATCH (query:Query)-[:SIMILAR_TO]->(doc:Document)
WHERE vector_similarity(query.embedding, doc.embedding) > 0.8
MATCH (doc)-[:REFERENCES]->(source:Source)
RETURN doc.content, source.citation
LIMIT 5
```

### Recommendation Engines
Traverse user-item-user paths while filtering by embedding similarity for personalized recommendations.

### Fraud Detection
Walk transaction graphs to identify suspicious patterns with real-time property filtering.

### Scientific Graphs
Model citation networks, molecular structures, or biological pathways with vector-enhanced similarity search.

---

## Quick Start

Get up and running in under 5 minutes:

```bash
# Clone and build
git clone https://github.com/rustic-ai/uni.git
cd uni && cargo build --release

# Import sample data
./target/release/uni import semantic-scholar \
  --papers demos/demo01/data/papers.jsonl \
  --citations demos/demo01/data/citations.jsonl \
  --output ./storage

# Run your first query
./target/release/uni query \
  "MATCH (p:Paper)-[:CITES]->(cited:Paper)
   WHERE p.year > 2020
   RETURN cited.title, COUNT(*) as citations
   ORDER BY citations DESC
   LIMIT 10" \
  --path ./storage
```

**[Complete Quick Start Guide →](getting-started/quickstart.md)**

---

## Documentation

### Getting Started
- **[Installation](getting-started/installation.md)** — Build from source, prerequisites, verification
- **[Quick Start](getting-started/quickstart.md)** — Your first graph in 5 minutes
- **[CLI Reference](getting-started/cli-reference.md)** — Complete command documentation

### Core Concepts
- **[Architecture](concepts/architecture.md)** — Layered design and component overview
- **[Data Model](concepts/data-model.md)** — Vertices, edges, properties, and schema
- **[Identity Model](concepts/identity.md)** — VID, EID, and UniId explained
- **[Indexing](concepts/indexing.md)** — Vector, scalar, and full-text indexes
- **[Concurrency](concepts/concurrency.md)** — Single-writer model and snapshots

### Developer Guides
- **[Cypher Querying](guides/cypher-querying.md)** — Complete OpenCypher reference
- **[Vector Search](guides/vector-search.md)** — Semantic similarity at scale
- **[Data Ingestion](guides/data-ingestion.md)** — Bulk import and streaming writes
- **[Schema Design](guides/schema-design.md)** — Best practices and patterns
- **[Performance Tuning](guides/performance-tuning.md)** — Optimization strategies

### Use Cases
- **[RAG & Knowledge Graphs](use-cases/rag-knowledge-graph.md)**
- **[Real-Time Fraud Detection](use-cases/fraud-detection.md)**
- **[Recommendation Engines](use-cases/recommendation-engine.md)**
- **[Supply Chain & BOM](use-cases/supply-chain.md)**

### Internals
- **[Vectorized Execution](internals/vectorized-execution.md)** — Batch processing deep dive
- **[Storage Engine](internals/storage-engine.md)** — Lance integration and LSM design
- **[Query Planning](internals/query-planning.md)** — Planner and optimization
- **[Benchmarks](internals/benchmarks.md)** — Performance metrics and methodology

### Reference
- **[Rust API](reference/rust-api.md)** — Programmatic access documentation
- **[Configuration](reference/configuration.md)** — All configuration options
- **[Troubleshooting](reference/troubleshooting.md)** — Common issues and solutions
- **[Glossary](reference/glossary.md)** — Terminology reference

---

## Technology Stack

Uni leverages best-in-class Rust ecosystem crates:

| Component | Technology | Purpose |
|-----------|------------|---------|
| Storage | [Lance](https://lancedb.github.io/lance/) | Columnar format with native vector indexes |
| Columnar | [Apache Arrow](https://arrow.apache.org/) | Zero-copy data representation |
| Analytics | Custom vectorized engine | Morsel-driven batch execution |
| Graph Runtime | SimpleGraph (custom) | In-memory graph algorithms |
| Object Store | [object_store](https://docs.rs/object_store) | S3/GCS/Azure abstraction |
| Parsing | [sqlparser](https://github.com/sqlparser-rs/sqlparser-rs) | SQL/Cypher tokenization |
| Embeddings | [FastEmbed](https://github.com/qdrant/fastembed) | Local embedding models |

---

## Project Status

Uni is under active development. Current status:

| Feature | Status |
|---------|--------|
| Graph storage & traversal | <span class="badge badge-new">Stable</span> |
| Vector search (HNSW, IVF_PQ) | <span class="badge badge-new">Stable</span> |
| Graph Algorithms (36 algorithms) | <span class="badge badge-new">Stable</span> |
| OpenCypher (MATCH, WHERE, RETURN, CREATE) | <span class="badge badge-new">Stable</span> |
| Aggregations & Window Functions | <span class="badge badge-new">Stable</span> |
| Scalar Functions (40+ functions) | <span class="badge badge-new">Stable</span> |
| Predicate pushdown | <span class="badge badge-new">Stable</span> |
| Variable-length paths (`*1..3`) | <span class="badge badge-new">Stable</span> |
| MERGE / SET / DELETE | <span class="badge badge-new">Stable</span> |
| UNION / UNION ALL | <span class="badge badge-new">Stable</span> |
| EXPLAIN | <span class="badge badge-new">Stable</span> |
| BACKUP / COPY | <span class="badge badge-new">Stable</span> |
| CRDT properties | <span class="badge badge-new">Stable</span> |
| Bulk Loading (BulkWriter) | <span class="badge badge-new">Stable</span> |
| Schema DDL Procedures | <span class="badge badge-new">Stable</span> |
| Snapshot Readers | <span class="badge badge-new">Stable</span> |
| Inverted Index (ANY IN) | <span class="badge badge-new">Stable</span> |
| Temporal Queries (validAt) | <span class="badge badge-new">Stable</span> |
| Composite Key Constraints | <span class="badge badge-new">Stable</span> |
| Full-text search (CONTAINS, STARTS WITH, ENDS WITH) | <span class="badge badge-new">Stable</span> |
| Distributed mode | <span class="badge badge-deprecated">Future</span> |

See **[Cypher Querying Guide](guides/cypher-querying.md)** for detailed feature documentation.

---

## Contributing

We welcome contributions! See the [Contributing Guide](https://github.com/rustic-ai/uni/blob/main/CONTRIBUTING.md) for details.

```bash
# Run tests
cargo test

# Run benchmarks
cargo bench

# Check formatting and lints
cargo fmt --check && cargo clippy
```

---

## License

Uni is open source under the [Apache 2.0 License](https://github.com/rustic-ai/uni/blob/main/LICENSE).

---

<div class="footer-nav">

**Ready to dive in?** Start with the **[Installation Guide](getting-started/installation.md)** or jump straight to the **[Quick Start](getting-started/quickstart.md)**.

</div>
