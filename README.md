# Uni: Embedded Graph & Vector Database

[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![Rust](https://img.shields.io/badge/rust-1.96+-orange.svg)](https://www.rust-lang.org)
[![Python](https://img.shields.io/badge/python-3.10+-blue.svg)](https://www.python.org)

**Uni** is an embedded database that combines a property graph (OpenCypher), vector search, and columnar storage (Lance) into a single engine. Designed for applications requiring local, fast, multimodal data access with object storage (S3/GCS/local) durability.

Part of [The Rustic Initiative](https://www.rustic.ai) by [Dragonscale Industries Inc.](https://www.dragonscale.ai)

## Key Features

- **Embedded & Serverless:** Runs as a library within your application — no server process.
- **Property Graph:** OpenCypher queries with MATCH, CREATE, WHERE, ORDER BY, LIMIT, and aggregations.
- **Serializable Transactions:** Snapshot isolation + optimistic concurrency control, on by default — conflicting concurrent commits abort with a retriable error instead of silently losing writes.
- **Locy Reasoning:** Datalog-style recursive rules over the graph, with probabilistic semantics and neural predicates.
- **Vector Search:** K-NN similarity search (L2, cosine) with pre-filter and threshold support, plus hybrid vector + BM25 fusion.
- **Columnar Storage:** Lance-backed persistence on local disk or object storage (S3/GCS).
- **Graph Algorithms:** PageRank, Louvain, shortest path, and more via the built-in algorithm library.
- **Forks:** Named, durable, writable graph branches with nesting, TTL, structural diff, and content-UID-keyed write-audit-publish promotion to primary. See [`website/docs/features/forks.md`](website/docs/features/forks.md).
- **Rust & Python:** Native Rust crate and Python bindings (PyO3).

## Getting Started

### Rust

Add to your `Cargo.toml`:

```toml
[dependencies]
uni-db = "3"
```

```rust
use uni_db::{DataType, Uni};

let db = Uni::open("./my_graph").build().await?;

// Define schema
db.schema()
    .label("Person")
    .property("name", DataType::String)
    .property("age", DataType::Int)
    .done()
    .apply()
    .await?;

// Write data in a transaction
let session = db.session();
let tx = session.tx().await?;
tx.execute("CREATE (:Person {name: 'Alice', age: 30})").await?;
tx.execute("CREATE (:Person {name: 'Bob', age: 25})").await?;
tx.commit().await?;

// Query through the session
let results = session
    .query_with("MATCH (p:Person) WHERE p.age > $min RETURN p.name AS name")
    .param("min", 28)
    .fetch_all()
    .await?;
```

### Python

```bash
pip install uni-db
```

```python
import uni_db

# Open or create a database
db = uni_db.Uni.open("./my_graph")

# Define schema
(
    db.schema()
    .label("Person")
    .property("name", "string")
    .property("age", "int")
    .index("name", "btree")
    .done()
    .apply()
)

# Write data in a transaction
session = db.session()
tx = session.tx()
tx.execute("CREATE (:Person {name: 'Alice', age: 30})")
tx.execute("CREATE (:Person {name: 'Bob', age: 25})")
tx.commit()

# Query through the session
results = session.query_with(
    "MATCH (p:Person) WHERE p.age > $min RETURN p.name AS name"
).param("min", 28).fetch_all()

for row in results:
    print(row["name"])  # Alice
```

## Vector Search

```python
# Create schema with an indexed vector property
(
    db.schema()
    .label("Document")
    .property("text", "string")
    .vector("embedding", 128)
    .index("embedding", {"type": "vector", "metric": "cosine"})
    .done()
    .apply()
)

# Insert data
tx = session.tx()
tx.execute_with(
    "CREATE (:Document {text: $text, embedding: $vec})"
).param("text", "hello world").param("vec", my_embedding).run()
tx.commit()

# K-NN search via Cypher procedure
results = session.query_with("""
    CALL uni.vector.query('Document', 'embedding', $vec, 10)
    YIELD node, distance
    RETURN node.text AS text, distance
    ORDER BY distance
""").param("vec", query_embedding).fetch_all()
```

## Async API

The async API mirrors the sync one (`AsyncUni`, sessions, transactions):

```python
import uni_db

async with uni_db.AsyncUni.open("./my_graph") as db:
    session = db.session()
    tx = await session.tx()
    await tx.execute("CREATE (:Person {name: 'Alice', age: 30})")
    await tx.commit()
    results = await session.query("MATCH (p:Person) RETURN p.name AS name")
```

## Python OGM (uni-pydantic)

```bash
pip install uni-pydantic
```

```python
from uni_pydantic import UniNode, UniSession, Field, Relationship, Vector

class Person(UniNode):
    name: str
    age: int | None = None
    email: str = Field(unique=True, index="btree")
    embedding: Vector[128] = Field(metric="cosine")
    friends: list["Person"] = Relationship("FRIEND_OF", direction="both")

session = UniSession(db)
session.register(Person)
session.sync_schema()

alice = Person(name="Alice", email="alice@example.com")
session.add(alice)
session.commit()

adults = session.query(Person).filter(Person.age >= 18).order_by(Person.name).all()
```

## Documentation

- [Full Documentation](https://rustic-ai.github.io/uni-db)
- [GitHub Repository](https://github.com/rustic-ai/uni-db)

## License

Apache 2.0 — see [LICENSE](LICENSE) for details.

---

**Uni** is developed by [Dragonscale Industries Inc.](https://www.dragonscale.ai) as part of [The Rustic Initiative](https://www.rustic.ai).
