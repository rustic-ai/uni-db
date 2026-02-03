# Developer Guides

Practical guides for building applications with Uni.

<div class="feature-grid">

<div class="feature-card">

### [Cypher Querying](cypher-querying.md)
Complete OpenCypher reference with pattern matching, filtering, and aggregations.

</div>

<div class="feature-card">

### [Vector Search](vector-search.md)
Semantic similarity search with HNSW and IVF_PQ indexes.

</div>

<div class="feature-card">

### [Data Ingestion](data-ingestion.md)
Bulk import, streaming writes, and ETL patterns.

</div>

<div class="feature-card">

### [Schema Design](schema-design.md)
Best practices for modeling your data as a property graph.

</div>

<div class="feature-card">

### [Pydantic OGM](pydantic-ogm.md)
Type-safe Python models with automatic schema generation.

</div>

<div class="feature-card">

### [Performance Tuning](performance-tuning.md)
Optimization strategies for queries, indexes, and storage.

</div>

</div>

## Common Tasks

### Query Your Graph

```cypher
MATCH (p:Paper)-[:CITES]->(cited:Paper)
WHERE p.year > 2020
RETURN cited.title, COUNT(*) as citations
ORDER BY citations DESC
LIMIT 10
```

### Vector Similarity Search

```cypher
CALL uni.vector.query('Paper', 'embedding', $query_vector, 10)
YIELD node, score
RETURN node.title, score
```

### Bulk Data Import

```bash
uni import semantic-scholar \
  --papers papers.jsonl \
  --citations edges.jsonl \
  --output ./storage
```

## Next Steps

- New to graph queries? Start with [Cypher Querying](cypher-querying.md)
- Building AI features? See [Vector Search](vector-search.md)
- Loading large datasets? Check [Data Ingestion](data-ingestion.md)
