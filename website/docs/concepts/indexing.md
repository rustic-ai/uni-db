# Indexing

Indexes are critical for query performance in Uni. This guide covers all index types, their use cases, and configuration options.

## Index Types Overview

Uni supports five categories of indexes:

```
┌─────────────────────────────────────────────────────────────────────────────────────────────────────────────┐
│                                              INDEX TYPES                                                     │
├─────────────────────┬─────────────────────┬──────────────────────┬──────────────────────┬────────────────────┤
│    VECTOR INDEXES   │   SCALAR INDEXES    │   FULL-TEXT INDEXES  │   JSON FTS INDEXES   │  INVERTED INDEXES  │
├─────────────────────┼─────────────────────┼──────────────────────┼──────────────────────┼────────────────────┤
│ • HNSW              │ • BTree             │ • Inverted Index     │ • Lance Inverted     │ • Set Membership   │
│ • IVF_PQ            │                     │ • Tokenizers         │ • BM25 Ranking       │ • ANY IN patterns  │
│ • Flat (exact)      │                     │ • Scoring            │ • Path-Specific      │ • Tag filtering    │
├─────────────────────┼─────────────────────┼──────────────────────┼──────────────────────┼────────────────────┤
│ Similarity search   │ Exact/range queries │ Keyword search       │ JSON document search │ List membership    │
│ Nearest neighbors   │ Equality checks     │ Text matching        │ CONTAINS operator    │ Multi-value props  │
│ Embeddings          │ Sorting             │ Relevance ranking    │ Phrase search        │ Security filtering │
└─────────────────────┴─────────────────────┴──────────────────────┴──────────────────────┴────────────────────┘
```

---

## Vector Indexes

Vector indexes enable fast approximate nearest neighbor (ANN) search on embedding columns.

### Supported Algorithms

| Algorithm | Description | Trade-offs |
|-----------|-------------|------------|
| **HNSW** | Hierarchical Navigable Small World | Best recall, higher memory |
| **IVF_PQ** | Inverted File + Product Quantization | Lower memory, good recall |
| **Flat** | Exact brute-force search | Perfect recall, O(n) speed |

### Distance Metrics

| Metric | Formula | Use Case |
|--------|---------|----------|
| **Cosine** | 1 - (A·B)/(‖A‖‖B‖) | Normalized embeddings |
| **L2** | √Σ(aᵢ-bᵢ)² | Euclidean distance |
| **Dot** | -A·B | Inner product (unnormalized) |

### Creating Vector Indexes

**Via Cypher:**

```cypher
CREATE VECTOR INDEX paper_embeddings
FOR (p:Paper)
ON p.embedding
OPTIONS {
  type: "hnsw"
}
```
DDL uses cosine distance and default parameters. For metric choice or tuning, use the Rust schema builder.

### HNSW Configuration (Rust-only)

HNSW parameters are configurable only via the Rust schema builder:

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

`ef_search` is not configurable yet (uses an internal default).

### IVF_PQ Configuration (Rust-only)

DDL can select IVF_PQ, but uses default parameters:

```cypher
CREATE VECTOR INDEX product_embeddings
FOR (p:Product)
ON p.embedding
OPTIONS { type: "ivf_pq" }
```

For tuning, use Rust:

```rust
use uni_db::{DataType, IndexType, VectorAlgo, VectorIndexCfg, VectorMetric};

db.schema()
    .label("Product")
        .property("embedding", DataType::Vector { dimensions: 384 })
        .index("embedding", IndexType::Vector(VectorIndexCfg {
            algorithm: VectorAlgo::IvfPq { partitions: 256, sub_vectors: 16 },
            metric: VectorMetric::Cosine,
        }))
    .apply()
    .await?;
```

### Querying Vector Indexes

**Procedure Call:**

```cypher
CALL uni.vector.query('Paper', 'embedding', $query_vector, 10)
YIELD node, distance
RETURN node.title, distance
ORDER BY distance
```

**With Threshold:**

```cypher
CALL uni.vector.query('Paper', 'embedding', $query_vector, 100, NULL, 0.2)
YIELD node, distance
WHERE distance < 0.15
RETURN node.title, distance
```

**Hybrid (Vector + Graph):**

```cypher
CALL uni.vector.query('Paper', 'embedding', $query_vector, 10)
YIELD node as paper, distance
MATCH (paper)-[:AUTHORED_BY]->(author:Author)
RETURN paper.title, author.name, distance
```

---

## Scalar Indexes

Scalar indexes optimize exact match and range queries on primitive properties.

### Index Types

| Type | Operations | Best For |
|------|------------|----------|
| **BTree** | `=`, `<`, `>`, `<=`, `>=`, `BETWEEN` | General purpose, range queries |

### Creating Scalar Indexes

**BTree Index (default):**

```cypher
CREATE INDEX author_email FOR (a:Author) ON (a.email)
```

The storage layer currently builds BTree scalar indexes only.

### Composite Indexes

Index multiple properties together:

```cypher
CREATE INDEX paper_venue_year FOR (p:Paper) ON (p.venue, p.year)
```

**Query utilization:**

```cypher
// Uses index (prefix match)
MATCH (p:Paper) WHERE p.venue = 'NeurIPS' AND p.year > 2020

// Uses index (first column only)
MATCH (p:Paper) WHERE p.venue = 'NeurIPS'

// Does NOT use index (missing prefix)
MATCH (p:Paper) WHERE p.year > 2020
```

### Index Selection

Uni's query planner automatically selects indexes:

```
Query: MATCH (p:Paper) WHERE p.year > 2020 AND p.venue = 'NeurIPS'

Plan:
├── Project [p.title]
│   └── Scan [:Paper]
│         ↳ Index: paper_venue_year (venue='NeurIPS', year>2020)
│         ↳ Predicate Pushdown: venue = 'NeurIPS' AND year > 2020
```

---

## Full-Text Indexes

Full-text indexes enable keyword search within text properties.

### Creating Full-Text Indexes

```cypher
CREATE FULLTEXT INDEX paper_search
FOR (p:Paper)
ON EACH [p.title, p.abstract]
```

### Tokenizers

| Tokenizer | Description | Example |
|-----------|-------------|---------|
| `standard` | Unicode word boundaries | "Hello, World!" → ["hello", "world"] |
| `whitespace` | Split on whitespace only | "Hello, World!" → ["hello,", "world!"] |
| `ngram` | Character n-grams | "cat" → ["ca", "at"] (bigrams) |
| `keyword` | No tokenization | "Hello World" → ["hello world"] |

**Note:** Tokenizer configuration is not yet exposed via DDL; the default is `standard`.

### Querying Full-Text Indexes

```cypher
MATCH (p:Paper)
WHERE p.title CONTAINS 'transformer' OR p.abstract CONTAINS 'attention'
RETURN p.title
LIMIT 10
```

**Boolean Operators:**

```cypher
// AND (default)
'transformer attention'  // Both terms required

// OR
'transformer OR attention'

// NOT
'transformer NOT vision'

// Phrase
'"attention mechanism"'

// Wildcard
'transform*'
```

---

## JSON Full-Text Indexes

JSON Full-Text indexes enable BM25-based full-text search on JSON document columns, leveraging Lance's native inverted index.

### When to Use JSON FTS

| Use Case | Index Type |
|----------|------------|
| Search within JSON documents | JSON Full-Text Index |
| Keyword/phrase search in text fields | JSON Full-Text Index |
| Exact JSON path matching | JsonPath Index |
| Equality filters on scalar fields | Scalar Index |

### Creating JSON Full-Text Indexes

**Via Cypher:**

```cypher
CREATE JSON FULLTEXT INDEX article_fts
FOR (a:Article) ON _doc
```

**With Options:**

```cypher
CREATE JSON FULLTEXT INDEX article_fts
FOR (a:Article) ON _doc
OPTIONS { with_positions: true }
```

The `with_positions` option enables phrase search by storing term positions.

**If Not Exists:**

```cypher
CREATE JSON FULLTEXT INDEX article_fts IF NOT EXISTS
FOR (a:Article) ON _doc
```

### Querying with CONTAINS

Use the `CONTAINS` operator to perform full-text search on FTS-indexed columns:

```cypher
// Basic full-text search
MATCH (a:Article)
WHERE a._doc CONTAINS 'graph database'
RETURN a.title

// Path-specific search (searches within a JSON path)
MATCH (a:Article)
WHERE a._doc.title CONTAINS 'graph'
RETURN a.title

// Combined with exact matching
MATCH (a:Article)
WHERE a._doc.title CONTAINS 'graph' AND a.status = 'published'
RETURN a.title
```

### Query Routing Priority

The query planner routes predicates to the most efficient index:

```
1. _uid = 'xxx'           → UidIndex (O(1) lookup)
2. column CONTAINS 'term' → Lance FTS (BM25 ranking)
3. path = 'exact'         → JsonPathIndex (exact match)
4. Pushable predicates    → Lance scan filter
5. Else                   → Residual (post-load filter)
```

### JSON FTS Configuration

| Parameter | Default | Description |
|-----------|---------|-------------|
| `with_positions` | false | Enable phrase search (stores term positions) |

### How It Works

JSON Full-Text indexes use Lance's inverted index with triplet tokenization:

```
Document: { "title": "Graph Databases", "year": 2024 }
         ↓
Tokens:  (title, string, "graph"), (title, string, "databases"), (year, int, 2024)
         ↓
Query:   title:graph → Matches documents with "graph" in title path
```

---

## Inverted Indexes

Inverted indexes enable efficient filtering on `List<String>` properties, ideal for tag-based access control and multi-value attribute queries.

### Use Cases

| Use Case | Query Pattern | Benefit |
|----------|---------------|---------|
| **Tag filtering** | `ANY(tag IN d.tags WHERE tag IN $allowed)` | O(k) vs O(n) scan |
| **Security labels** | Filter by granted access tags | Multi-tenant filtering |
| **Categories** | Documents in multiple categories | Efficient set intersection |
| **Skills matching** | Users with any required skill | Fast membership checks |

### Creating Inverted Indexes

**Via Schema:**

```json
{
  "indexes": {
    "document_tags": {
      "type": "inverted",
      "label": "Document",
      "property": "tags",
      "config": {
        "normalize": true,
        "max_terms_per_doc": 10000
      }
    }
  }
}
```

**Via Cypher:**

```cypher
CREATE INVERTED INDEX document_tags
FOR (d:Document)
ON d.tags
OPTIONS { normalize: true, max_terms_per_doc: 10000 }
```

**Via Rust API:**

```rust
db.schema()
    .label("Document")
        .property("tags", DataType::List(Box::new(DataType::String)))
        .index("tags", IndexType::Inverted(InvertedIndexConfig {
            normalize: true,
            max_terms_per_doc: 10_000,
        }))
    .apply()
    .await?;
```

### Inverted Index Configuration

| Parameter | Default | Description |
|-----------|---------|-------------|
| `normalize` | `true` | Lowercase and trim whitespace on terms |
| `max_terms_per_doc` | `10_000` | Maximum terms per document (DoS protection) |

### Query Patterns

**ANY IN pattern (optimized):**

```cypher
// Finds documents with ANY of the specified tags
MATCH (d:Document)
WHERE ANY(tag IN d.tags WHERE tag IN ['public', 'team:eng'])
RETURN d.title
```

**With session variables (multi-tenant):**

```cypher
// Security filtering with session-based permissions
MATCH (d:Document)
WHERE d.tenant_id = $session.tenant_id
  AND ANY(tag IN d.tags WHERE tag IN $session.granted_tags)
RETURN d
```

### How Inverted Indexes Work

```
Document 1: tags = ['rust', 'database']
Document 2: tags = ['python', 'ml']
Document 3: tags = ['rust', 'ml']
         ↓
Inverted Index (term → VID list):
  'rust'     → [vid_1, vid_3]
  'database' → [vid_1]
  'python'   → [vid_2]
  'ml'       → [vid_2, vid_3]
         ↓
Query: ANY(tag IN d.tags WHERE tag IN ['rust', 'python'])
Result: Union of 'rust' and 'python' → [vid_1, vid_2, vid_3]
```

### Query Planner Integration

When an inverted index exists on a `List<String>` property, the query planner automatically rewrites `ANY IN` patterns to use index lookups:

```
Query: MATCH (d:Document) WHERE ANY(tag IN d.tags WHERE tag IN $allowed) RETURN d

Without Index:
├─ Full Scan: Document
└─ Filter: ANY(tag IN d.tags WHERE tag IN $allowed)  // O(n × m)

With Inverted Index:
├─ Inverted Index Lookup: tags IN $allowed           // O(k)
└─ Fetch: Document properties
```

### Performance Comparison

| Scenario | Without Index | With Index | Speedup |
|----------|---------------|------------|---------|
| 1M docs, 10 tags each, query 3 tags | ~5s scan | ~10ms | 500x |
| 100K docs, security filter | ~500ms | ~5ms | 100x |
| Multi-value category filter | ~1s | ~15ms | 67x |

---

## Index Management

### List Indexes

```cypher
SHOW INDEXES
```

For more detail:

```cypher
CALL uni.schema.indexes()
```

### Drop Indexes

```cypher
DROP INDEX paper_year
```

### Rebuild Indexes

```rust
// Rust API
db.rebuild_indexes("Paper", false).await?;
```

---

## Index Storage

Indexes are stored within the Lance dataset structure:

```
storage/
├── vertices_Paper/
│   ├── data/
│   │   └── *.lance
│   ├── _indices/                    # Lance native indexes
│   │   └── embedding_idx-uuid/      # Vector index
│   │       ├── index.idx
│   │       └── aux/
│   └── _versions/
└── indexes/
    ├── scalar_paper_year/           # Separate scalar index
    │   └── index.lance
    └── fulltext_paper_search/       # Full-text index
        └── index/
```

---

## Predicate Pushdown

Indexes integrate with Uni's predicate pushdown optimization:

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                        PREDICATE PUSHDOWN FLOW                              │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│   Query: MATCH (p:Paper) WHERE p.year > 2020 AND p.title CONTAINS 'AI'     │
│                                                                             │
│   1. Predicate Analysis                                                     │
│      ├── p.year > 2020      → Pushable (scalar index or Lance filter)      │
│      └── p.title CONTAINS   → Residual (post-load filter)                  │
│                                                                             │
│   2. Index Selection                                                        │
│      └── paper_year index available? Yes → Use index scan                  │
│                                                                             │
│   3. Execution                                                              │
│      ├── Index Scan: year > 2020 → VIDs [v1, v2, v3, ...]                  │
│      ├── Load Properties: title for filtered VIDs                          │
│      └── Residual Filter: title CONTAINS 'AI'                              │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

### Pushable Predicates

| Predicate | Index Type | Pushed? |
|-----------|------------|---------|
| `p.x = 5` | BTree | Yes |
| `p.x > 5` | BTree | Yes |
| `p.x IN [1,2,3]` | BTree | Yes |
| `p.x IS NULL` | BTree | Yes |
| `p._doc CONTAINS 'foo'` | JSON FTS | Yes (if FTS-indexed) |
| `p.x CONTAINS 'foo'` | None | No (residual, if not FTS-indexed) |
| `p.x STARTS WITH 'foo'` | BTree | Partial |
| `func(p.x) = 5` | None | No (residual) |

---

## Best Practices

### When to Create Indexes

```
✓ CREATE INDEX when:
  • Property appears in WHERE clauses frequently
  • Property is used for JOIN conditions
  • Property is used in ORDER BY
  • Range queries on numeric/date properties (BTree)

✗ AVOID INDEX when:
  • Property rarely queried
  • Very small dataset (<1000 rows)
  • Property updated frequently
  • Very low selectivity (e.g., boolean with 50/50 split)
```

### Index Sizing

| Index Type | Memory Formula | Example (1M vectors, 768d) |
|------------|----------------|---------------------------|
| HNSW | ~1.5x vectors × (4 + m×8) bytes | ~120 MB |
| IVF_PQ | vectors × (d/sub_vectors) bytes | ~24 MB |
| BTree | ~40 bytes per key | ~40 MB |

### Index Maintenance

```cypher
SHOW INDEXES
```

For rebuilds, use the Rust API (`db.rebuild_indexes("Label", async_)`) or drop/recreate the index in Cypher.

---

## Performance Comparison

| Query Type | Without Index | With Index | Speedup |
|------------|---------------|------------|---------|
| Point lookup | O(n) scan | O(log n) BTree | 1000x+ |
| Range query | O(n) scan | O(log n + k) | 100x+ |
| Vector KNN | O(n×d) brute | O(log n) HNSW | 1000x+ |
| Full-text | O(n×len) scan | O(log n) inverted | 100x+ |

---

## Next Steps

- [Vector Search Guide](../guides/vector-search.md) — Deep dive into similarity search
- [Performance Tuning](../guides/performance-tuning.md) — Optimization strategies
- [Query Planning](../internals/query-planning.md) — How indexes are selected
