# Uni: Embedded Graph & Vector Database

[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![Rust](https://img.shields.io/badge/rust-1.75+-orange.svg)](https://www.rust-lang.org)
[![Python](https://img.shields.io/badge/python-3.10+-blue.svg)](https://www.python.org)

**Uni** is a modern, embedded database that combines property graph (OpenCypher), vector search, and columnar storage (Lance) into a single, cohesive engine. It is designed for applications requiring local, fast, and multimodal data access, backed by object storage (S3/GCS) durability.

Part of [The Rustic Initiative](https://www.rustic.ai) by [Dragonscale Industries Inc.](https://www.dragonscale.ai)

## Key Features

- **Embedded & Serverless:** Runs as a single process or library within your application.
- **Multimodal:** Supports Graph, Vector, Document, and Columnar workloads.
- **Storage:** Persists data using LanceDB format on local disk or object storage.
- **Query Language:** OpenCypher (subset) with vector search extensions.
- **Search:** Vector similarity, full-text (BM25), and hybrid search with rank fusion.
- **Algorithms:** Built-in graph algorithms (PageRank, Louvain, ShortestPath, etc.).
- **Connectivity:** Rust crate and Python bindings.

## Getting Started

### Rust

Add `uni-db` to your `Cargo.toml`:

```toml
[dependencies]
uni-db = { git = "https://github.com/rustic-ai/uni" }
```

```rust
use uni_db::Uni;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let db = Uni::open("my_db").build().await?;
    
    // Create node
    db.execute("CREATE (n:Person {name: 'Alice', age: 30})").await?;
    
    // Query
    let results = db.query("MATCH (n:Person) RETURN n").await?;
    println!("{:?}", results);
    
    Ok(())
}
```

### Python

Build and install the python bindings:

```bash
cd bindings/uni-db
maturin develop
```

```python
import uni_db

db = uni_db.Database("my_db")

# Create
db.query("CREATE (n:Person {name: 'Bob', age: 25})")

# Query
results = db.query("MATCH (n:Person) RETURN n")
print(results)
```

## Search Capabilities

Uni provides three powerful search methods that can be combined with graph traversals:

### Vector Search

```cypher
-- Search by semantic similarity (auto-embeds text if embedding config exists)
CALL uni.vector.query('Document', 'embedding', 'machine learning tutorial', 10)
YIELD node, score
RETURN node.title, score
```

### Full-Text Search (BM25)

```cypher
-- Traditional keyword search with relevance scoring
CALL uni.fts.query('Article', 'content', 'database optimization', 20)
YIELD node, score
RETURN node.title, score
```

### Hybrid Search (Vector + FTS Fusion)

```cypher
-- Combine semantic and keyword search with Reciprocal Rank Fusion
CALL uni.search(
    'Document',
    {vector: 'embedding', fts: 'content'},
    'neural network architectures',
    null,  -- auto-embed the query
    10
)
YIELD node, score, vector_score, fts_score
RETURN node.title, score
```

See [Language Extensions](docs/LANGUAGE_EXTENSIONS.md) for full procedure documentation.

## Documentation

- [Full Documentation](https://rustic-ai.github.io/uni)
- [Architecture Design](docs/DESIGN.md)
- [Feature Comparison](docs/feature-comparison.md)
- [Algorithms](crates/uni-algo/src/algo/algorithms)

## Contributing

We welcome contributions! Please see our [Contributing Guide](CONTRIBUTING.md) for details.

## License

Apache 2.0 - see [LICENSE](LICENSE) for details.

---

**Uni** is developed by [Dragonscale Industries Inc.](https://www.dragonscale.ai) as part of [The Rustic Initiative](https://www.rustic.ai).
