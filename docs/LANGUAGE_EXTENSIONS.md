# Uni Cypher Language Extensions

This document lists Uni-specific extensions beyond OpenCypher.

## Operators

- `~=` Approximate vector match. Example: `n.embedding ~= $query`.

## Functions

- `vector_similarity(a, b)` returns cosine similarity of two vectors.
- `vector_distance(a, b [, metric])` returns distance using `cosine`, `l2`, or `dot`.
- `uni.validAt(node, start_prop, end_prop, time)` checks temporal validity.

## Procedures

### Algorithms

- `CALL algo.*` runs graph algorithms like PageRank, WCC, Louvain, shortest path, etc.

### Vector Search

```cypher
CALL uni.vector.query(label, property, query_input, k [, filter] [, threshold])
YIELD vid, score, node
```

Performs KNN vector search on a vector-indexed property.

**Parameters:**
- `label` - Node label to search (e.g., `'Document'`)
- `property` - Vector property name (e.g., `'embedding'`)
- `query_input` - Either:
  - `Vec<f32>` - Pre-computed query vector
  - `String` - Text to auto-embed using the index's embedding config
- `k` - Number of results to return
- `filter` - (Optional) WHERE clause for pre-filtering (e.g., `'category = "tech"'`)
- `threshold` - (Optional) Minimum similarity score to include

**Yields:**
- `vid` - Vertex ID of matching node
- `score` - Similarity score (normalized 0-1)
- `node` - Full node with properties (lazy-loaded)

**Examples:**

```cypher
-- Basic vector search with pre-computed embedding
CALL uni.vector.query('Document', 'embedding', [0.1, 0.2, 0.3, ...], 10)
YIELD node, score
RETURN node.title, score

-- Auto-embed text query (requires embedding config on index)
CALL uni.vector.query('Document', 'embedding', 'machine learning tutorial', 10)
YIELD node, score
RETURN node.title, score

-- With pre-filter
CALL uni.vector.query('Document', 'embedding', $query_vec, 10, 'status = "published"')
YIELD node, score
RETURN node.title, score
```

**Note:** Auto-embedding requires the vector index to be created with an `embedding` configuration:
```cypher
CREATE VECTOR INDEX doc_embed FOR (d:Document) ON (d.embedding)
OPTIONS {
    metric: 'cosine',
    embedding: {
        provider: 'fastembed',
        model: 'AllMiniLML6V2',
        source: ['content']
    }
}
```

### Full-Text Search

```cypher
CALL uni.fts.query(label, property, search_term, k [, threshold])
YIELD vid, score, node
```

Performs BM25 full-text search on an inverted-indexed property.

**Parameters:**
- `label` - Node label to search
- `property` - Text property with inverted index
- `search_term` - Search query string
- `k` - Number of results to return
- `threshold` - (Optional) Minimum normalized BM25 score (0-1)

**Yields:**
- `vid` - Vertex ID of matching node
- `score` - Normalized BM25 score (0-1)
- `node` - Full node with properties (lazy-loaded)

**Example:**

```cypher
-- Basic full-text search
CALL uni.fts.query('Article', 'content', 'database optimization', 20)
YIELD node, score
RETURN node.title, score
ORDER BY score DESC

-- With threshold filtering
CALL uni.fts.query('Article', 'content', 'graph algorithms', 10, 0.5)
YIELD node, score
WHERE score > 0.5
RETURN node.title, score
```

### Hybrid Search (Vector + FTS Fusion)

```cypher
CALL uni.search(label, properties, query_text [, query_vector] [, k] [, filter] [, options])
YIELD vid, score, node [, vector_score] [, fts_score]
```

Combines vector similarity and full-text search using rank fusion.

**Parameters:**
- `label` - Node label to search
- `properties` - Map specifying which properties to search:
  - `{vector: 'embedding', fts: 'content'}` - Explicit property names
- `query_text` - Text query (used for FTS, auto-embedded for vector if no vector provided)
- `query_vector` - (Optional) Pre-computed query vector; if omitted, auto-embeds `query_text`
- `k` - (Optional, default 10) Number of results
- `filter` - (Optional) WHERE clause applied to both branches
- `options` - (Optional) Fusion options map:
  - `{method: 'rrf'}` - Reciprocal Rank Fusion (default)
  - `{method: 'weighted', alpha: 0.7}` - Weighted fusion (alpha = vector weight)
  - `{over_fetch: 3.0}` - Over-fetch factor for pagination

**Yields:**
- `vid` - Vertex ID
- `score` - Fused score
- `node` - Full node with properties
- `vector_score` - (Optional) Normalized vector similarity
- `fts_score` - (Optional) Normalized BM25 score

**Examples:**

```cypher
-- Basic hybrid search with auto-embedding
CALL uni.search(
    'Document',
    {vector: 'embedding', fts: 'content'},
    'machine learning optimization',
    null,  -- auto-embed the text
    20
)
YIELD node, score
RETURN node.title, score

-- Hybrid search with pre-computed vector
CALL uni.search(
    'Document',
    {vector: 'embedding', fts: 'content'},
    'machine learning',
    $precomputed_vector,
    10
)
YIELD node, score, vector_score, fts_score
RETURN node.title, score, vector_score, fts_score

-- With pre-filter and weighted fusion
CALL uni.search(
    'Document',
    {vector: 'embedding', fts: 'content'},
    'neural networks',
    null,
    10,
    'category = "research"',
    {method: 'weighted', alpha: 0.6}
)
YIELD node, score
RETURN node.title, score
```

**Fusion Methods:**

| Method | Description | When to Use |
|--------|-------------|-------------|
| `rrf` | Reciprocal Rank Fusion | Default; robust, no tuning needed |
| `weighted` | Linear combination with alpha | When you want to favor vector or FTS |

RRF formula: `score = Σ 1/(k + rank)` where k=60 (standard constant)

Weighted formula: `score = alpha * vector_score + (1 - alpha) * fts_score`

### Admin and Metadata

- `CALL db.compact()` and `CALL db.compactionStatus()`.
- `CALL db.snapshot.create([name])`, `CALL db.snapshot.list()`, `CALL db.snapshot.restore(id)`.
- `CALL db.labels()`, `CALL db.edgeTypes()` (alias: `db.relationshipTypes()`).
- `CALL db.indexes()`, `CALL db.constraints()`, `CALL db.schema.labelInfo(label)`.

### DDL via Procedures

- `CALL db.createLabel(name, config)`, `CALL db.createEdgeType(name, src, dst, config)`.
- `CALL db.createIndex(label, property, config)`, `CALL db.dropIndex(name)`.
- `CALL db.createConstraint(label, type, properties)`, `CALL db.dropConstraint(name)`.
- `CALL db.dropLabel(name)`, `CALL db.dropEdgeType(name)`.

## DDL and Admin Clauses

- Indexes: `CREATE VECTOR INDEX`, `CREATE FULLTEXT INDEX`, `CREATE JSON FULLTEXT INDEX`,
  `CREATE SCALAR INDEX`, `DROP INDEX`, `SHOW INDEXES`.
- Schema: `CREATE LABEL`, `CREATE EDGE TYPE`, `ALTER LABEL`, `ALTER EDGE TYPE`,
  `DROP LABEL`, `DROP EDGE TYPE`.
- Constraints: `CREATE CONSTRAINT`, `DROP CONSTRAINT`, `SHOW CONSTRAINTS`.
- Utilities: `COPY`, `BACKUP`, `SHOW DATABASE`, `SHOW CONFIG`, `SHOW STATISTICS`,
  `VACUUM`, `CHECKPOINT`.
- Transactions: `BEGIN`, `COMMIT`, `ROLLBACK`.
- Recursive CTE: `WITH RECURSIVE`.

## Document and JSON Extensions

- JSON full-text predicates such as `n._doc CONTAINS 'term'`, including path-specific
  form like `n._doc.title CONTAINS 'term'`.
