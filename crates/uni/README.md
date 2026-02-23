# Uni - Embedded Graph Database

[![Crates.io](https://img.shields.io/crates/v/uni-db.svg)](https://crates.io/crates/uni-db)
[![Documentation](https://docs.rs/uni-db/badge.svg)](https://docs.rs/uni-db)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

**Uni** is an embedded, multimodal database that combines **Property Graph** (OpenCypher), **Vector Search**, and **Columnar Storage** (Lance) into a single engine. It is designed for high-performance, local-first applications with object storage durability (S3/GCS).

Part of [The Rustic Initiative](https://www.rustic.ai) by [Dragonscale Industries Inc.](https://www.dragonscale.ai)

## Features

- **Embedded**: Runs in-process with your application (no sidecar required).
- **Multimodal**: Graph + Vector + Columnar in one engine.
- **OpenCypher**: Execute complex graph pattern matching queries.
- **Vector Search**: Native support for vector embeddings and KNN search.
- **Hybrid Storage**: Fast local WAL/ID allocation with bulk data + catalog metadata in S3/GCS.
- **Graph Algorithms**: Built-in PageRank, WCC, ShortestPath, and more.

## Installation

Add `uni` to your `Cargo.toml`:

```toml
[dependencies]
uni = "0.1.0"
tokio = { version = "1", features = ["full"] }
```

## Quick Start

### 1. Open Database

```rust
use uni_db::Uni;

#[tokio::main]
async fn main() -> Result<(), uni_db::UniError> {
    // Open (or create) a local database
    let db = Uni::open("./my_graph_db")
        .build()
        .await?;
    
    // Define Schema
    db.schema()
        .label("Person")
            .property("name", uni_db::DataType::String)
            .property("age", uni_db::DataType::Integer)
            .vector("embedding", 384) // Vector index
        .apply()
        .await?;

    Ok(())
}
```

### 2. Insert Data

You can insert data using Cypher queries or the builder API.

```rust
// Using Cypher
db.query("CREATE (p:Person {name: 'Alice', age: 30})").await?;

// Using Builder (faster for bulk)
use uni_db::PropertiesBuilder;
// ... (Bulk API usage if available or via loops)
```

### 3. Query Data

```rust
let results = db.query("MATCH (p:Person) WHERE p.age > 25 RETURN p.name, p.age").await?;

for row in results {
    let name: String = row.get("p.name")?;
    let age: i64 = row.get("p.age")?;
    println!("Found: {} ({})", name, age);
}
```

### 4. Vector Search

```rust
// Find similar nodes
let query_vec = vec![0.1, 0.2, ...]; // 384 dims
let results = db.query_builder()
    .knn("Person", "embedding", query_vec)
    .k(5)
    .execute()
    .await?;
```

## Storage Backends

Uni supports local filesystem and object storage (S3, GCS, Azure).

### Hybrid Mode (Recommended for Cloud)

Keep WAL and ID allocation on fast local disk (SSD), while storing bulk data and catalog metadata in S3.

```rust
let db = Uni::open("./local_meta")
    .hybrid("./local_meta", "s3://my-bucket/graph-data")
    .build()
    .await?;
```

## Documentation

- [Full Documentation](https://rustic-ai.github.io/uni)
- [Rust API Reference](https://docs.rs/uni)
- [GitHub Repository](https://github.com/rustic-ai/uni)

## License

Apache 2.0 - see [LICENSE](../../LICENSE) for details.

---

Developed by [Dragonscale Industries Inc.](https://www.dragonscale.ai) as part of [The Rustic Initiative](https://www.rustic.ai).
