# Uni

## Reasoning and memory infrastructure for intelligent systems.

Uni gives AI agents **structured memory**, **formal reasoning**, **what-if simulation**, and **explainable decisions** — in one embedded engine backed by object storage. No servers. No infrastructure. One `pip install`.

<div class="quick-links" markdown>
<a href="getting-started/installation/" class="quick-link">Install</a>
<a href="getting-started/quickstart/" class="quick-link">Quick Start</a>
<a href="locy/" class="quick-link">Reasoning Guide</a>
<a href="use-cases/" class="quick-link">Use Cases</a>
<a href="https://github.com/rustic-ai/uni-db" class="quick-link">GitHub</a>
</div>

---

## The Agent Reasoning Gap

Today's AI agents can generate text, but they cannot reason over structured knowledge, remember across sessions, simulate consequences before acting, or explain why they reached a conclusion. These are cognitive capabilities, not database features — and without them, agents remain fluent but unreliable.

The workaround is stitching together four or five systems — a graph database, a vector store, a text index, a rules engine, and custom glue code — each with its own data model, consistency boundary, and operational overhead. Uni closes that gap with a single embedded library where graph traversals, vector search, full-text retrieval, logic programming, and hypothetical reasoning all execute against the same data, in-process, with no ETL pipelines or cross-system joins.

---

## Five Pillars of Machine Cognition

Uni is organized around five cognitive capabilities that intelligent systems need:

1. **Structured Memory** — a typed property graph for entities and relationships (OpenCypher + 42 graph algorithms)
2. **Associative Recall** — hybrid retrieval that fuses semantic and lexical search (8 ANN index algorithms with RaBitQ/SQ/PQ quantization + BM25 full-text + `uni.search` fusion)
3. **Domain Physics** — declarative rules that encode how a domain actually works (Locy recursive rules with stratified negation)
4. **Mental Simulation** — hypothetical reasoning that explores consequences before committing (ASSUME … THEN in a rollback boundary)
5. **Explainable Decisions** — proof traces and abductive reasoning that show *why* and *what would need to change* (EXPLAIN RULE + ABDUCE)

The following example puts four of these pillars to work in a single scenario.

---

## See It In Action

A network-ops team needs to answer three questions about their service dependency graph: *What breaks if the auth service goes down? Why is it reachable? What would need to change so it isn't?*

**Domain Physics — define the rules once:**

```locy
// Transitive reachability over service dependencies
CREATE RULE reachable AS
MATCH (a:Service)-[:DEPENDS_ON]->(b:Service)
YIELD KEY a, KEY b

CREATE RULE reachable AS
MATCH (a:Service)-[:DEPENDS_ON]->(mid:Service)
WHERE mid IS reachable TO b
YIELD KEY a, KEY b
```

**Mental Simulation — what breaks if auth goes down?**

```locy
ASSUME {
  MATCH (s:Service {name: 'auth-service'})
  SET s.status = 'DOWN'
} THEN {
  QUERY reachable
  WHERE a.name = 'auth-service'
  RETURN b.name AS affected_service
}
```

**Explainability — why is payment-service reachable from auth-service?**

```locy
EXPLAIN RULE reachable
WHERE a.name = 'auth-service' AND b.name = 'payment-service'
```

**Abductive Reasoning — what would need to change so auth-service can't reach a service?**

```locy
ABDUCE NOT reachable
WHERE a.name = 'auth-service'
RETURN b
```

[Locy Overview](locy/index.md) | [Language Guide](locy/language-guide.md) | [ASSUME / ABDUCE / DERIVE](locy/advanced/derive-assume-abduce.md) | [Use Cases](locy/use-cases.md)

---

## The Engine Underneath

The five pillars run on a unified substrate — one process, one data model, one consistency boundary.

**Structured Memory + Domain Physics:**

- Full OpenCypher with recursive CTEs, window functions, and time travel (`VERSION AS OF`)
- 36 built-in graph algorithms — PageRank, Louvain, Dijkstra, betweenness centrality, k-shortest paths, and more
- Locy logic layer — recursive rules, stratified negation, goal-directed evaluation (SLG resolution)

**Associative Recall:**

- Vector indexes — 8 algorithms (Flat, IVF-Flat/SQ/PQ/RQ, HNSW-Flat/SQ/PQ) with auto-embedding support
- Full-text search — BM25 ranking over text fields
- JSON-path search — query nested document properties
- Hybrid fusion — reciprocal-rank or weighted fusion across vector and text results via `uni.search`

**Operational:**

- In-process execution — link as a Rust crate or Python package, no network round-trips
- Object-store backed — S3, GCS, Azure, or local disk with automatic local caching
- Automatic compaction — semantic compaction in the background, no manual tuning
- Snapshot isolation — single-writer, multi-reader with no lock contention

[Architecture](concepts/architecture.md) | [Features](features/hybrid-search.md) | [Storage Engine](internals/storage-engine.md)

---

## Performance

Cognitive operations need to be fast enough for an agent's decision loop. Indicative numbers from internal benchmarks — see the [Benchmarks](internals/benchmarks.md) doc for methodology.

| Operation | Latency |
|---|---|
| Point lookup (indexed) | 2–5 ms |
| Structured memory traversal (1-hop, cached) | 4–8 ms |
| Associative recall (vector KNN, k=10) | 1–3 ms |
| Aggregation over 1M rows | 50–200 ms |
| Memory update (batch insert, 10K nodes) | 5–10 ms |

[Performance Tuning](guides/performance-tuning.md) | [Benchmarks](internals/benchmarks.md)

---

## Get Started

1. [Install Uni](getting-started/installation.md)
2. [Quick Start](getting-started/quickstart.md) — create a graph, define rules, and run your first simulation in five minutes
3. [Programming Guide](getting-started/programming-guide.md) — Rust and Python APIs in depth
4. [AI Agent Skill](guides/ai-skill.md) — give your agent structured reasoning capabilities

---

## Explore

### Reasoning

- [Locy Overview](locy/index.md)
- [Language Guide](locy/language-guide.md)
- [ASSUME / ABDUCE / DERIVE](locy/advanced/derive-assume-abduce.md)
- [Use Cases](locy/use-cases.md)

### Structured Memory & Recall

- [Cypher Querying](guides/cypher-querying.md)
- [Vector Search](guides/vector-search.md)
- [Schema Design](guides/schema-design.md)
- [Data Ingestion](guides/data-ingestion.md)
- [Pydantic OGM](guides/pydantic-ogm.md)

### Reference

- [Concepts](concepts/index.md)
- [Internals](internals/index.md)
- [API Reference](reference/index.md)
- [Examples (Rust / Python / Notebooks)](examples/index.md)
