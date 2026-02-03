# Interactive Examples

This section contains interactive Jupyter notebooks demonstrating Uni's capabilities across various use cases.

## Available Examples

We provide examples in **Python**, **Python with Pydantic OGM**, and **Rust** to match your preferred development style.

### Use Cases

| Use Case | Description | Python | Pydantic OGM | Rust |
|----------|-------------|--------|--------------|------|
| **Supply Chain** | BOM explosion, cost rollup, defect tracking | [Python](python/supply_chain.ipynb) | [Pydantic](pydantic/supply_chain.ipynb) | [Rust](rust/supply_chain.ipynb) |
| **Recommendation** | Collaborative filtering, vector similarity | [Python](python/recommendation.ipynb) | [Pydantic](pydantic/recommendation.ipynb) | [Rust](rust/recommendation.ipynb) |
| **RAG** | Knowledge graph + vector search for LLM context | [Python](python/rag.ipynb) | [Pydantic](pydantic/rag.ipynb) | [Rust](rust/rag.ipynb) |
| **Fraud Detection** | Cycle detection, shared device analysis | [Python](python/fraud_detection.ipynb) | [Pydantic](pydantic/fraud_detection.ipynb) | [Rust](rust/fraud_detection.ipynb) |
| **Sales Analytics** | Graph traversal with columnar aggregations | [Python](python/sales_analytics.ipynb) | [Pydantic](pydantic/sales_analytics.ipynb) | [Rust](rust/sales_analytics.ipynb) |

## Choosing an API

| API | Best For | Key Features |
|-----|----------|--------------|
| **Python (uni_db)** | Direct database access, max flexibility | Raw Cypher, bulk operations |
| **Pydantic OGM** | Type-safe models, IDE autocomplete | Pydantic validation, query builder, ORM patterns |
| **Rust** | Performance-critical applications | Zero-cost abstractions, compile-time safety |

## Running the Notebooks

### Python Notebooks

```bash
# Install dependencies
cd bindings/uni-db
pip install -e .

# Run Jupyter
jupyter notebook examples/
```

### Pydantic OGM Notebooks

```bash
# Install uni-pydantic
cd bindings/uni-pydantic
poetry install

# Run Jupyter
poetry run jupyter notebook examples/
```

### Rust Notebooks

Rust notebooks require the `evcxr_jupyter` kernel:

```bash
# Install the Rust Jupyter kernel
cargo install evcxr_jupyter
evcxr_jupyter --install

# Run Jupyter
jupyter notebook examples/rust/
```

## What You'll Learn

Each notebook demonstrates:

- **Schema Design** - Defining labels, edge types, and properties
- **Data Ingestion** - Bulk loading vertices and edges
- **Cypher Queries** - Pattern matching, filtering, aggregations
- **Graph Algorithms** - Traversals, path finding, cycle detection
- **Vector Search** - Semantic similarity with embeddings

The Pydantic OGM notebooks additionally show:

- **Type-Safe Models** - Defining nodes and edges as Pydantic classes
- **Automatic Schema Sync** - Generating database schema from models
- **Query Builder** - Fluent API for building queries
- **Relationships** - Declaring and traversing graph relationships

## Source Code

The notebook source files are also available in the repository:

- Python: [`bindings/uni-db/examples/`](https://github.com/rustic-ai/uni/tree/main/bindings/uni-db/examples)
- Pydantic OGM: [`bindings/uni-pydantic/examples/`](https://github.com/rustic-ai/uni/tree/main/bindings/uni-pydantic/examples)
- Rust: [`examples/rust/`](https://github.com/rustic-ai/uni/tree/main/examples/rust)
