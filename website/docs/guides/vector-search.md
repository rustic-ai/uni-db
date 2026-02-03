# Vector Search Guide

Uni treats vector search as a first-class citizen, deeply integrated with the graph traversal engine. This guide covers schema design, index configuration, query patterns, and performance optimization for semantic similarity search.

## Overview

Vector search enables finding similar items based on high-dimensional embeddings:

```
Query: "papers about attention mechanisms"
         │
         ▼
    ┌───────────────────┐
    │  Embed Query      │
    │  → [0.12, -0.34,  │
    │     0.56, ...]    │
    └─────────┬─────────┘
              │
              ▼
    ┌───────────────────┐
    │  Vector Index     │
    │  (HNSW / IVF_PQ)  │
    └─────────┬─────────┘
              │
              ▼
    ┌───────────────────┐
    │  Top-K Results    │
    │  - Attention...   │
    │  - Transformer... │
    │  - BERT...        │
    └───────────────────┘
```

---

## Setting Up Vector Search

### Step 1: Define Vector Schema

Add a `Vector` type property to your schema:

```json
{
  "properties": {
    "Paper": {
      "title": { "type": "String", "nullable": false },
      "abstract": { "type": "String", "nullable": true },
      "embedding": {
        "type": "Vector",
        "dimensions": 768
      }
    },
    "Product": {
      "name": { "type": "String", "nullable": false },
      "description_embedding": {
        "type": "Vector",
        "dimensions": 384
      },
      "image_embedding": {
        "type": "Vector",
        "dimensions": 512
      }
    }
  }
}
```

**Dimension Guidelines:**

| Model | Dimensions | Use Case |
|-------|------------|----------|
| all-MiniLM-L6-v2 | 384 | General text, fast |
| BGE-base-en-v1.5 | 768 | High quality text |
| OpenAI text-embedding-3-small | 1536 | Commercial, high quality |
| CLIP ViT-B/32 | 512 | Image + text |

### Step 2: Create Vector Index

Create an index for efficient similarity search:

**HNSW (Recommended for most cases):**

```cypher
CREATE VECTOR INDEX paper_embeddings
FOR (p:Paper)
ON p.embedding
OPTIONS {
  type: "hnsw"
}
```

**IVF_PQ (For memory-constrained environments):**

```cypher
CREATE VECTOR INDEX paper_embeddings
FOR (p:Paper)
ON p.embedding
OPTIONS {
  type: "ivf_pq"
}
```

### Step 3: Import Data with Embeddings

Your import data should include embedding vectors:

```json
{"id": "paper_001", "title": "Attention Is All You Need", "embedding": [0.12, -0.34, 0.56, ...]}
{"id": "paper_002", "title": "BERT: Pre-training of Deep Bidirectional Transformers", "embedding": [0.08, -0.21, 0.42, ...]}
```

---

## Querying Vectors

### Basic KNN Search

Find the K nearest neighbors to a query vector:

```cypher
CALL uni.vector.query('Paper', 'embedding', $query_vector, 10)
YIELD node, distance
RETURN node.title, distance
ORDER BY distance
```

**Parameters:**
- `'Paper'`: Label to search
- `'embedding'`: Vector property name
- `$query_vector`: Query vector (list of floats)
- `10`: Number of results (K)

**Yields:**
- `node`: Full node object with all properties
- `vid`: Vertex ID (for efficient joins)
- `distance`: Raw distance value (lower is better)
- `score`: Normalized similarity score (higher is better, range 0-1)

### Operator Form (`~=`) with Scores

You can also use the `~=` operator to run a vector search and get a similarity score:

```cypher
MATCH (p:Paper)
WHERE p.embedding ~= $query_vector
RETURN p.title, p._score AS score
ORDER BY score DESC
LIMIT 10
```

### With Distance Threshold

Filter results by maximum distance:

```cypher
CALL uni.vector.query('Paper', 'embedding', $query_vector, 100, NULL, 0.3)
YIELD node, distance
RETURN node.title, distance
ORDER BY distance
LIMIT 10
```

The threshold parameter (6th argument) filters results to only those with `distance <= 0.3`.

### Hybrid Search: Pre-Filtering

Pre-filter at the vector index level for efficient hybrid search:

```cypher
// Filter BEFORE vector search (efficient!)
CALL uni.vector.query(
  'Paper',
  'embedding',
  $query_vector,
  10,
  'year >= 2020 AND venue IN (''NeurIPS'', ''ICML'')'  // Lance/DataFusion filter
)
YIELD node, distance, score
RETURN node.title, node.year, distance, score
ORDER BY distance
```

**Pre-filtering** searches only within the filtered subset, unlike post-filtering which searches all nodes then filters.

### Post-Filtering (Alternative)

Combine vector search with property filtering after search:

```cypher
CALL uni.vector.query('Paper', 'embedding', $query_vector, 50)
YIELD node AS paper, distance
WHERE paper.year >= 2020 AND paper.venue IN ['NeurIPS', 'ICML']
RETURN paper.title, paper.year, distance
ORDER BY distance
LIMIT 10
```

**Note:** Pre-filtering (above) is more efficient when the filter is selective.

### Filter + Threshold Together

Combine both for maximum control:

```cypher
CALL uni.vector.query(
  'Product',
  'embedding',
  $query_vector,
  100,
  'category = ''electronics'' AND price < 1000',  // Pre-filter
  0.5  // Distance threshold
)
YIELD node, distance, score
RETURN node.name, node.price, distance, score
ORDER BY score DESC  // Use normalized score for ranking
LIMIT 10
```

---

## Hybrid Graph + Vector Queries

The real power comes from combining graph traversal with vector search.

### Pattern 1: Vector Search → Graph Expansion

Find similar papers, then explore their citations:

```cypher
// Find papers similar to query
CALL uni.vector.query('Paper', 'embedding', $query_vector, 10)
YIELD node AS seed, distance

// Expand to citations
MATCH (seed)-[:CITES]->(cited:Paper)
RETURN seed.title AS source, cited.title AS cited_paper, distance
ORDER BY distance, cited.year DESC
```

### Pattern 2: Graph Context → Vector Search

Start from a known node, find similar neighbors:

```cypher
// Start from a specific paper
MATCH (seed:Paper {title: 'Attention Is All You Need'})

// Get its embedding
WITH seed, seed.embedding AS seed_embedding

// Find papers cited by seed that are similar to seed
MATCH (seed)-[:CITES]->(cited:Paper)
WHERE vector_similarity(seed_embedding, cited.embedding) > 0.8
RETURN cited.title, cited.year
```

### Pattern 3: Multi-Hop with Similarity Filter

Find papers in citation chain with semantic similarity:

```cypher
MATCH (start:Paper {title: 'Attention Is All You Need'})
MATCH (start)-[:CITES]->(hop1:Paper)-[:CITES]->(hop2:Paper)
WHERE vector_similarity(start.embedding, hop2.embedding) > 0.7
RETURN DISTINCT hop2.title, hop2.year
ORDER BY hop2.year DESC
LIMIT 20
```

### Pattern 4: Author's Similar Papers

Find an author's papers similar to a query:

```cypher
// Vector search for similar papers
CALL uni.vector.query('Paper', 'embedding', $query_vector, 100)
YIELD node AS paper, distance

// Filter to specific author
MATCH (paper)-[:AUTHORED_BY]->(a:Author {name: 'Geoffrey Hinton'})
RETURN paper.title, paper.year, distance
ORDER BY distance
LIMIT 10
```

---

## Generating Embeddings

### Auto-Embedding via Index Options

Uni can auto-generate embeddings on insert when you configure an embedding provider in the index options:

```cypher
CREATE VECTOR INDEX doc_embed_idx
FOR (d:Document) ON d.embedding
OPTIONS {
  type: "hnsw",
  embedding: {
    provider: "fastembed",
    model: "AllMiniLML6V2",
    source: ["content"]
  }
}
```

Supported providers in schema include `fastembed`, `openai`, and `ollama`, but **only `fastembed` is implemented today**. `openai`/`ollama` configs will parse but return an error at runtime. There is no direct embedding API in `uni_db` yet; use auto-embedding or your own embedding pipeline.

### Using External APIs

For production, you might use external embedding APIs:

```python
import openai
import json

# Generate embeddings
def embed_text(text):
    response = openai.Embedding.create(
        input=text,
        model="text-embedding-3-small"
    )
    return response['data'][0]['embedding']

# Prepare JSONL with embeddings
papers = [
    {"id": "p1", "title": "Paper 1", "embedding": embed_text("Paper 1 abstract")},
    {"id": "p2", "title": "Paper 2", "embedding": embed_text("Paper 2 abstract")},
]

with open("papers.jsonl", "w") as f:
    for paper in papers:
        f.write(json.dumps(paper) + "\n")
```

### Understanding Yields

The `uni.vector.query` procedure returns multiple values:

```cypher
CALL uni.vector.query('Product', 'embedding', $vec, 10)
YIELD node, vid, distance, score
RETURN node.name, vid, distance, score
```

| Yield | Type | Description | Use When |
|-------|------|-------------|----------|
| `node` | Object | Full node with all properties | Need immediate property access |
| `vid` | Integer | Vertex ID for efficient joins | Joining with other queries |
| `distance` | Float | Raw distance (lower = better) | Need exact distance values |
| `score` | Float | Normalized similarity 0-1 (higher = better) | Ranking by similarity |

**Performance tip:** Use `YIELD vid` when you only need IDs - it's much faster than `YIELD node` for large result sets since it skips property loading.

```cypher
// Fast: Only loads IDs
CALL uni.vector.query('Product', 'embedding', $vec, 1000)
YIELD vid, distance
WHERE distance < 0.5
RETURN vid

// Slower: Loads all properties for 1000 nodes
CALL uni.vector.query('Product', 'embedding', $vec, 1000)
YIELD node, distance
WHERE distance < 0.5
RETURN node
```

---

## Distance Metrics

### Cosine Similarity

Best for normalized embeddings (most text models):

```
similarity = A · B / (||A|| × ||B||)
distance = 1 - similarity
```

- Range: 0 (identical) to 2 (opposite)
- Use when: Magnitude doesn't matter, only direction

### L2 (Euclidean) Distance

Best for embeddings where magnitude matters:

```
distance = √Σ(aᵢ - bᵢ)²
```

- Range: 0 (identical) to ∞
- Use when: Absolute position in space matters

### Dot Product

Best for unnormalized embeddings:

```
similarity = A · B
distance = -similarity (negated for ranking)
```

- Range: -∞ to +∞
- Use when: Embeddings have meaningful magnitudes

---

## Index Tuning

DDL currently supports selecting the index type (`hnsw`, `flat`, `ivf_pq`) but uses default parameters. To tune HNSW or IVF_PQ parameters, use the Rust schema builder:

```rust
use uni_db::{DataType, IndexType, VectorAlgo, VectorIndexCfg, VectorMetric};

db.schema()
    .label("Paper")
        .property("embedding", DataType::Vector { dimensions: 768 })
        .index("embedding", IndexType::Vector(VectorIndexCfg {
            algorithm: VectorAlgo::Hnsw { m: 32, ef_construction: 200 },
            metric: VectorMetric::Cosine,
        }))
    .apply()
    .await?;
```

---

## Performance Optimization

### Pre-filtering Strategy

For hybrid queries, choose the right filtering strategy:

```cypher
// ✅ BEST: Pre-filter at index level (most efficient)
CALL uni.vector.query(
  'Paper',
  'embedding',
  $query_vector,
  10,
  'year >= 2020 AND venue = "NeurIPS"'  // Filter pushed to LanceDB
)
YIELD node AS paper, distance
RETURN paper.title, distance
ORDER BY distance

// ✅ GOOD: Vector search first, then post-filter
CALL uni.vector.query('Paper', 'embedding', $query_vector, 100)
YIELD node AS paper, distance
WHERE paper.year >= 2020  // Filter after vector search
RETURN paper.title, distance
ORDER BY distance
LIMIT 10

// ⚠️ OK: Over-fetch for selective filters (less efficient)
CALL uni.vector.query('Paper', 'embedding', $query_vector, 500)
YIELD node AS paper, distance
WHERE paper.year >= 2020 AND paper.venue = 'NeurIPS'
RETURN paper.title, distance
ORDER BY distance
LIMIT 10
```

**When to use pre-filtering:**
- Filter is selective (reduces search space significantly)
- You need fewer results than the filtered set size
- The filter column is indexed in LanceDB

**When to use post-filtering:**
- Filter is not very selective
- You need many results
- Complex Cypher expressions not expressible in SQL

### Batch Queries

For multiple queries, batch them:

```rust
// Process multiple query vectors efficiently
let queries = vec![query1, query2, query3];
let results = storage.batch_vector_search(
    "Paper",
    "embedding",
    &queries,
    10  // k per query
).await?;
```

### Caching Query Vectors

Pre-compute and cache frequent query embeddings:

```cypher
// Store computed query embedding
CREATE (q:Query {
  text: 'transformer architectures',
  embedding: $precomputed_embedding,
  created_at: datetime()
})

// Reuse later
MATCH (q:Query {text: 'transformer architectures'})
CALL uni.vector.query('Paper', 'embedding', q.embedding, 10)
YIELD node, distance
RETURN node.title, distance
```

---

## Use Cases

### Semantic Document Search

```cypher
// Find documents similar to a natural language query
WITH $query_embedding AS query_vec
CALL uni.vector.query('Document', 'content_embedding', query_vec, 20)
YIELD node AS doc, distance
RETURN doc.title, doc.summary, distance
ORDER BY distance
LIMIT 10
```

### Recommendation System

```cypher
// Find products similar to what user viewed
MATCH (u:User {id: $user_id})-[:VIEWED]->(viewed:Product)
WITH COLLECT(viewed.embedding) AS viewed_embeddings

// Average the embeddings (simplified)
WITH reduce(sum = [0.0]*384, e IN viewed_embeddings |
  [i IN range(0, 383) | sum[i] + e[i]]) AS summed,
  size(viewed_embeddings) AS count
WITH [x IN summed | x / count] AS avg_embedding

CALL uni.vector.query('Product', 'embedding', avg_embedding, 20)
YIELD node AS product, distance
WHERE NOT EXISTS((u)-[:VIEWED]->(product))  // Exclude already viewed
RETURN product.name, product.price, distance
LIMIT 10
```

### Duplicate Detection

```cypher
// Find near-duplicate documents
MATCH (d:Document)
CALL uni.vector.query('Document', 'embedding', d.embedding, 5)
YIELD node AS similar, distance
WHERE similar.id <> d.id AND distance < 0.1  // Very similar
RETURN d.title, similar.title, distance
```

### Clustering via Vector Search

```cypher
// Find clusters of similar papers
MATCH (seed:Paper)
WHERE seed.citations > 100  // Start from influential papers
CALL uni.vector.query('Paper', 'embedding', seed.embedding, 20)
YIELD node AS similar, distance
WHERE distance < 0.3
RETURN seed.title AS cluster_center, COLLECT(similar.title) AS cluster_members
```

---

## Troubleshooting

### Low Recall

**Symptoms:** Missing expected results

**Solutions:**
1. Increase `k` and post-filter
2. Use HNSW (higher recall) instead of IVF_PQ
3. Check embedding model consistency (same model for indexing and querying)
4. Verify dimensions match the schema
5. (Rust) Increase HNSW `m` / `ef_construction` or IVF_PQ `partitions` / `sub_vectors`

### Slow Queries

**Symptoms:** High latency on vector search

**Solutions:**
1. Reduce `k` or add a distance `threshold`
2. Use IVF_PQ instead of HNSW for large datasets
3. Pre-filter with `uni.vector.query(..., filter)` when possible
4. Ensure a vector index exists (`SHOW INDEXES`)

### Memory Issues

**Symptoms:** OOM during indexing or queries

**Solutions:**
1. Switch to IVF_PQ (compressed vectors)
2. (Rust) Reduce HNSW `m` / `ef_construction`
3. (Rust) Reduce IVF_PQ `partitions` / `sub_vectors`
4. Consider smaller embeddings or fewer indexed labels

---

## Next Steps

- [Indexing](../concepts/indexing.md) — All index types and configuration
- [Performance Tuning](performance-tuning.md) — Optimization strategies
- [Data Ingestion](data-ingestion.md) — Import data with embeddings
