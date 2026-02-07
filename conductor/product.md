# Product Definition

## Vision
**Uni** is a modern, embedded, multimodal database designed to bridge the gap between property graphs, vector search, and columnar analytics. By combining OpenCypher for graph traversal, native vector embeddings for semantic search, and Lance for high-performance columnar storage, Uni provides a cohesive engine for next-generation, data-intensive applications. It is built to be "local-first" but cloud-ready, offering object storage durability (S3/GCS) for serverless and edge deployments.

## Target Audience
- **Data Scientists & ML Engineers:** Who need a local, performant store for vector embeddings and graph structures without managing complex infrastructure.
- **Application Developers:** Building "local-first" apps, RAG (Retrieval-Augmented Generation) pipelines, or edge services requiring rich data querying capabilities.
- **Embedded Systems Engineers:** Who require a lightweight, zero-dependency database that can run in-process.

## Core Value Proposition
- **Unified Engine:** Query graph relationships and vector similarities in a single request.
- **Embedded & Serverless:** Runs in-process with no separate server management; persists to object storage for durability.
- **Performance:** Leverages columnar storage (Lance) and vectorized execution (Arrow) for analytical speed.
- **Standards Compliant:** Supports OpenCypher query language, making it accessible to existing graph developers.

## Key Features
- **Property Graph Model:** Full support for nodes, edges, and properties with OpenCypher querying.
- **Vector Search:** Native support for storing and searching vector embeddings (ANN index).
- **Columnar Storage:** Data is stored in Lance format, enabling fast analytical scans and interoperability.
- **Hybrid Search:** Combine semantic vector search with structured graph traversals and full-text search.
- **Multi-Language Support:** Core engine in Rust with high-performance Python bindings.

## Success Metrics
- **Performance:** Low-latency point lookups (<5ms) and high-throughput analytical scans.
- **Compatibility:** High compliance with OpenCypher TCK (Technology Compatibility Kit).
- **Adoption:** Ease of integration for Python and Rust developers.