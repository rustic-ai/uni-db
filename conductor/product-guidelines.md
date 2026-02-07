# Product Guidelines

## Brand Identity
**Uni** is positioned as a sophisticated, high-performance engine that is also accessible and forward-looking. Our identity balances deep technical rigor with developer ergonomics.

- **Primary Voice:** Technical, Precise, and Authoritative. We speak the language of database engineers and systems architects.
- **Secondary Voice:** Developer-Centric and Visionary. We empower application developers to build the future of AI and data-intensive apps without getting bogged down in infrastructure.

## Communication Principles

### 1. Precision First, Simplicity Second
- **Guideline:** Always explain *how* it works before explaining *why* it's easy.
- **Do:** "Uni utilizes Lance columnar storage for O(1) point lookups."
- **Don't:** "Uni is really fast because of its storage engine."

### 2. Code over Prose
- **Guideline:** Show, don't just tell. Documentation should be heavy on code snippets (Rust/Python) and Cypher query examples.
- **Do:** Provide a complete, copy-pasteable example for vector search.
- **Don't:** Write a long paragraph describing the vector search API without showing it.

### 3. Local-First, Cloud-Ready
- **Guideline:** Emphasize the "embedded" nature first. The ability to run without a server is our key differentiator. Object storage durability is the powerful "backend" feature.

## Design & Engineering Standards

### 1. Performance is a Feature
- **Rule:** Every architectural decision must weigh the impact on latency and throughput. Benchmarks are required for major changes.
- **Constraint:** Zero-copy data paths should be preferred where possible (e.g., Arrow integration).

### 2. Ergonomics & Safety
- **Rule:** APIs (Rust & Python) must be idiomatic.
- **Constraint:** In Rust, leverage the type system to prevent runtime errors. In Python, provide full type hints and Pydantic integration.

### 3. Standards Compliance
- **Rule:** Adhere to OpenCypher semantics strictly unless there is a compelling performance reason to deviate (which must be documented).

## User Experience (UX) Goals
- **"Five-Minute Magic":** A developer should be able to install Uni, ingest data, and run a vector+graph hybrid query within 5 minutes.
- **Transparent Internals:** While easy to use, the system should offer deep introspection (EXPLAIN/PROFILE) for power users to optimize their queries.
