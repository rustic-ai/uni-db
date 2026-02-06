# Features

Uni is an embedded, multi-model graph database that unifies graph, vector, document, and columnar analytics in a single engine. This section provides a user-focused view of what Uni does and when each feature is the right choice.

## Summary

| Feature | What it gives you | Best for |
|---|---|---|
| [Embedded & Serverless](embedded-serverless.md) | Run Uni inside your process, no external server | Local apps, edge deployments, simple ops |
| [Multi-Model Data Model](data-model-multimodal.md) | Graph + vector + document + columnar in one DB | Knowledge graphs, AI apps, mixed workloads |
| [OpenCypher Querying](cypher-querying.md) | Familiar graph query language with extensions | Graph traversal and pattern matching |
| [Columnar Analytics](columnar-analytics.md) | Vectorized scans, filters, aggregates | Fast analytics without a separate warehouse |
| [Window Functions](window-functions.md) | OVER clauses for ranking and running totals | Partitioned analytics in a single query |
| [Vector Search](vector-search.md) | ANN search with HNSW/IVF_PQ/Flat, auto-embedding | Semantic search, RAG, similarity |
| [Full-Text + JSON Search](full-text-json-search.md) | BM25 text search over properties and JSON paths | Documents, metadata search |
| [Hybrid Search](hybrid-search.md) | Combined vector + FTS with rank fusion | Best of both worlds |
| [Schema & Indexing](schema-indexing.md) | Typed properties, constraints, and indexes | Performance, governance, stability |
| [Graph Algorithms](graph-algorithms.md) | Built-in centrality, clustering, pathing | Insights, scoring, routing |
| [Transactions & Consistency](transactions-consistency.md) | Snapshot isolation, single writer | Predictable reads, safe writes |
| [Snapshots & Time Travel](snapshots-time-travel.md) | Point-in-time snapshots + `AS OF` queries | Auditing, reproducible analytics |
| [Bulk Ingest](bulk-ingest.md) | High-throughput loading with index rebuilds | Initial loads, large updates |
| [Storage & Cloud Durability](storage-cloud.md) | Local + object-store storage with WAL | Low-ops durability on S3/GCS/Azure |
| [APIs & Tooling](apis-tooling.md) | Rust + Python APIs, CLI, REPL | App integration and exploration |

## When Uni Is the Right Choice

Use Uni when you want:

- A single embedded database for graph, vector, and document workloads.
- OpenCypher for graph traversal, combined with vector search or analytics.
- Local or object-store backed storage without managing a distributed system.
- Consistent reads with a simple single-writer model.

## When Another Database Might Be Better

Consider alternatives when you need:

- Multi-writer distributed transactions across many machines.
- A fully managed, multi-tenant service with no embedded deployment.
- A pure analytics warehouse with SQL-first interfaces and federated query.

## Why Uni Is a Strong Default for Graph + AI Workloads

Uni is designed to remove the usual trade-offs between graph traversal, vector search, and analytics. If your application needs those capabilities together, Uni tends to be the most direct and maintainable solution because it:

- Unifies graph, vector, document, and columnar queries in one engine.
- Runs embedded, avoiding a separate database service and network hops.
- Uses object-store friendly storage, so durability and scale are simple.
- Keeps the query surface small and consistent with OpenCypher.

If the constraints above do not apply, Uni is usually the most practical path to shipping graph + AI features in a single stack.
