# Vector, Hybrid & Full-Text Search Reference

Quick reference for Uni DB's vector similarity, full-text, and hybrid search capabilities.

---

## 1. Vector Search -- `uni.vector.query`

<!-- doctest: skip -->
```cypher
CALL uni.vector.query(label, property, query_vector, k [, filter] [, threshold] [, options])
YIELD node, score, distance, vector_score, rerank_score, vid
```

| Parameter | Type | Required | Description |
|---|---|---|---|
| `label` | String | Yes | Vertex label to search |
| `property` | String | Yes | Vector property name |
| `query_vector` | List\<Float\> or String | Yes | Embedding vector, or text string (auto-embedded when index has embedding config) |
| `k` | Integer | Yes | Number of results |
| `filter` | String | No | Lance/DataFusion WHERE predicate for **pre-filtering** |
| `threshold` | Float | No | Minimum similarity score (0-1); results below are excluded |
| `options` | Map | No | Reranker configuration (see [Section 12](#12-cross-encoder-reranking)) |

| YIELD Column | Type | Description |
|---|---|---|
| `node` | Object | Full node with all properties (slower for large k) |
| `vid` | Integer | Vertex ID for efficient joins (faster than `node`) |
| `score` | Float | Normalized similarity 0-1, or reranker score when reranker is active |
| `distance` | Float | Raw distance (lower = closer; metric-dependent) |
| `vector_score` | Float | Same as `score` (for parity with `uni.search`) |
| `rerank_score` | Float | Cross-encoder score (null when reranker is not configured) |

**Basic search:**
```cypher
CALL uni.vector.query('Document', 'embedding', $query_vector, 10)
YIELD node, score
RETURN node.title, score
ORDER BY score DESC
```

**With pre-filter and threshold:**
```cypher
CALL uni.vector.query('Document', 'embedding', $query_vector, 20,
    'category = "tech" AND year >= 2023', 0.5)
YIELD node, score
RETURN node.title, score
```

**Auto-embed text query (requires embedding config on index):**
```cypher
CALL uni.vector.query('Document', 'embedding', 'graph databases for beginners', 5)
YIELD node, score
RETURN node.title, score
```

---

## 2. Sparse Vector Search -- `uni.sparse.query`

Sparse vectors (SPLADE / learned-sparse, BM25-style term weights) are stored as a `{indices, values}` pair over a fixed term space. Scoring is **dot product** (higher = more similar), computed exactly and MVCC/L0-aware (sees unflushed writes, exact rescoring).

### Property Type

| Type | Cypher DDL | Description |
|---|---|---|
| `DataType::SparseVector { dimensions }` | `sparse_vector(N)` | `dimensions` = term-space cardinality (max term id + 1) |

### Sparse Index DDL

```cypher
// Sparse index (8-bit weight quantization by default)
CREATE VECTOR INDEX idx_sparse FOR (d:Document) ON (d.sparse_embedding)
OPTIONS { type: 'sparse' }

// Lossless f32 weights (quantize: false)
CREATE VECTOR INDEX idx_sparse FOR (d:Document) ON (d.sparse_embedding)
OPTIONS { type: 'sparse', quantize: false }
```

`quantize` defaults **TRUE** (8-bit weight quantization); set `false` for lossless f32 weights.

Procedure path:
```cypher
CALL uni.schema.createIndex('Document', 'sparse_embedding',
    {"type": "VECTOR", "algorithm": "SPARSE"})
```

### Query Procedure

<!-- doctest: skip -->
```cypher
CALL uni.sparse.query(label, property, query, k [, filter] [, threshold] [, options])
YIELD vid, score, rerank_score
```

| Parameter | Type | Required | Description |
|---|---|---|---|
| `label` | String | Yes | Vertex label to search |
| `property` | String | Yes | Sparse vector property name |
| `query` | Map or sparse vector | Yes | `{indices, values}` (equal-length lists) or a native sparse vector |
| `k` | Integer | Yes | Number of results |
| `filter` | String | No | Pre-filter WHERE predicate |
| `threshold` | Float | No | Minimum score |
| `options` | Map | No | Supports `over_fetch` |

| YIELD Column | Type | Description |
|---|---|---|
| `vid` | Integer | Vertex ID |
| `score` | Float | Dot product (higher = more similar) |
| `rerank_score` | Float | Cross-encoder score (null when reranker not configured) |

> **Note:** the YIELD columns are exactly `vid, score, rerank_score`. There is **no** `sparse_score` and **no** `distance` column on `uni.sparse.query` (`score` *is* the dot product). `sparse_score` only appears on `uni.search` (Section 5).

### Example

```cypher
// query is a {indices, values} map over the term space
CALL uni.sparse.query('Document', 'sparse_embedding',
    {indices: [3, 17, 482], values: [0.7, 0.4, 0.9]}, 10)
YIELD vid, score
RETURN vid, score
ORDER BY score DESC
```

---

## 3. `similar_to()` Expression Function

<!-- doctest: skip -->
```cypher
similar_to(source, query [, options]) -> Float
```

A per-row expression for `WHERE`, `RETURN`, `ORDER BY`, and Locy rule bodies. Scores one already-bound node (not a top-K scan like `CALL` procedures).

### `~=` Operator vs `similar_to()` Function

| Syntax | What It Does | Best For |
|---|---|---|
| `n.embedding ~= $query` | **Top-K index scan** — desugars to `uni.vector.query`, returns nearest neighbors from vector index | "Find top 10 from millions" |
| `similar_to(n.embedding, $q)` | **Per-row scoring** — evaluates inline, scores each already-bound node | "Score this matched node" |
| `similar_to([sources], [queries])` | **Hybrid fusion** — combines vector + FTS via RRF or weighted fusion | "Rank by semantic + keyword" |

`~=` is **vector-only** (no FTS, no hybrid). For hybrid, use `similar_to()` with multi-source arrays.

> **`=~` is regex** (`n.name =~ '(?i)john'`), **`~=` is vector similarity** — unrelated operators.

### Scoring Modes (auto-detected)

| Source Type | Query Type | Mode | Behavior |
|---|---|---|---|
| Vector property | Vector literal | **Vector** | Metric-aware similarity per row |
| Vector property (with embedding config) | String literal | **AutoEmbed** | Embeds query once, then vector similarity per row |
| String property (with FTS index) | String literal | **FTS** | BM25 score normalized via `score / (score + fts_k)` |

Metric is resolved from the vector index at compile time. Defaults to cosine if no index found.

### Single-Source Examples

```cypher
// Vector-to-vector
MATCH (d:Doc)
RETURN d.title, similar_to(d.embedding, $query_vector) AS score

// Auto-embed text
MATCH (d:Doc)
WHERE similar_to(d.embedding, 'attention mechanisms') > 0.6
RETURN d.title

// FTS (BM25)
MATCH (d:Doc)
RETURN d.title, similar_to(d.content, 'distributed systems') AS score
```

### Multi-Source Fusion

```cypher
// Broadcast: same query applied to vector + FTS sources
MATCH (d:Doc)
RETURN d.title,
  similar_to([d.embedding, d.content], 'machine learning') AS score

// Per-source queries with weighted fusion
MATCH (p:Product)
RETURN p.name, similar_to(
  [p.image_embedding, p.desc_embedding, p.description],
  [$photo_vec, 'red sneakers', 'affordable running shoes'],
  {method: 'weighted', weights: [0.4, 0.3, 0.3]}
) AS score
```

### Options Map

| Option | Type | Default | Description |
|---|---|---|---|
| `method` | String | `'rrf'` | `'rrf'` or `'weighted'` |
| `weights` | List\<Float\> | Equal | Per-source weights (must sum to 1.0); weighted mode only |
| `k` | Integer | `60` | RRF constant |
| `fts_k` | Float | `1.0` | BM25 saturation constant: `score / (score + fts_k)` |

**Gotcha -- RRF in point context:** `similar_to()` scores one node at a time (no ranked list), so RRF degenerates to equal-weight averaging. A `RrfPointContext` warning is emitted. Use `method: 'weighted'` for explicit control.

### Correct vs Incorrect Hybrid Search

**Correct — single `similar_to` with multi-source arrays:**
```cypher
// RRF fusion (default), proper BM25 normalization
MATCH (d:Doc)
RETURN d.title,
  similar_to([d.embedding, d.content], [$qvec, $qtxt]) AS score
ORDER BY score DESC
```

**Incorrect — naive addition of two separate calls:**
```cypher
// DON'T: mixes incompatible scales (cosine [0,1] vs unbounded BM25)
MATCH (d:Doc)
RETURN d.title,
  (similar_to(d.embedding, $qvec) + similar_to(d.content, $qtxt)) AS score
```
The multi-source form normalizes BM25 via `score / (score + fts_k)` before fusion. Raw addition skips this.

### Execution Path Capability

| Context | Vector | Auto-Embed | FTS | Multi-Source |
|---|---|---|---|---|
| Cypher `MATCH ... WHERE/RETURN` | Yes | Yes | Yes | Yes |
| Locy rule `WHERE / YIELD / ALONG / FOLD` | Yes | Yes | Yes | Yes |
| Locy command `DERIVE / ABDUCE / ASSUME WHERE` | Yes | No | No | No |

### Procedures vs `similar_to`

| | `CALL uni.vector.query / uni.search` | `similar_to()` |
|---|---|---|
| **Operation** | Scan index, return top-K | Score one bound node |
| **Use in WHERE** | No (standalone CALL) | Yes |
| **Use in Locy rules** | No | Yes |
| **Best for** | "Find top 10 from millions" | "Score this matched node" |

---

## 4. Full-Text Search -- `uni.fts.query`

<!-- doctest: skip -->
```cypher
CALL uni.fts.query(label, property, search_term, k [, filter] [, threshold] [, options])
YIELD node, score, fts_score, rerank_score, vid
```

| Parameter | Type | Required | Description |
|---|---|---|---|
| `label` | String | Yes | Node label |
| `property` | String | Yes | Text property with fulltext index |
| `search_term` | String | Yes | Search query |
| `k` | Integer | Yes | Number of results |
| `filter` | String | No | Pre-filter predicate |
| `threshold` | Float | No | Minimum score (0-1) |
| `options` | Map | No | Reranker configuration (see [Section 12](#12-cross-encoder-reranking)) |

Scores are BM25, normalized to 0-1 relative to top match.

```cypher
CALL uni.fts.query('Article', 'content', 'distributed graph database', 10)
YIELD node, score
RETURN node.title, score
ORDER BY score DESC
```

**Immediate write visibility:** FTS queries see unflushed writes from the in-memory L0 buffer. Data is searchable immediately after write, no flush needed.

---

## 5. Hybrid Search -- `uni.search`

```cypher
CALL uni.search(label, properties, query_text, query_vector, k, filter, options)
YIELD vid, score, rerank_score, vector_score, fts_score, sparse_score, distance
```

| Parameter | Type | Required | Description |
|---|---|---|---|
| `label` | String | Yes | Node label |
| `properties` | Map or String | Yes | `{vector, fts, sparse}` map. A bare string = same prop for vector + fts (sparse off) |
| `query_text` | String | Yes | Text for FTS and auto-embedding |
| `query_vector` | List or null | Yes | Pre-computed vector; `null` = auto-embed `query_text` |
| `k` | Integer | Yes | Number of results. No default — omitting it is an error |
| `filter` | String | No | WHERE clause for pre-filtering |
| `options` | Map | No | Fusion options (see below) |

The `properties` map keys are `{vector, fts, sparse}`.

### 3-Way (dense + FTS + sparse)

The sparse arm is **opt-in** and requires **both** halves — either alone is a silent no-op:
1. `sparse: 'prop'` in the `properties` map (the sparse vector property), AND
2. `options.sparse_query` — a `{indices, values}` map or a native sparse vector.

### Fusion Options

| Option | Values | Default | Description |
|---|---|---|---|
| `method` | `'rrf'`, `'weighted'` | `'rrf'` | Fusion algorithm |
| `alpha` | 0.0 - 1.0 | 0.5 | Vector/FTS blend weight (**2-way only**) |
| `weights` | List\<Float\> | — | 3-way weights `[vector, fts, sparse]` (weighted method) |
| `rrf_k` | Integer | 60 | RRF constant |
| `over_fetch` | Float | 2.0 | Over-fetch factor; each branch retrieves `k * over_fetch` |
| `sparse_query` | Map or sparse vector | `null` | `{indices, values}`; enables the sparse arm (with `sparse:` key) |
| `reranker` | String | `null` | Xervo alias for cross-encoder, OR the literal `'maxsim'` for ColBERT MaxSim rerank (see [Section 12](#12-cross-encoder-reranking)) |
| `reranker_property` | String | FTS property | Node text property for cross-encoder document input |
| `reranker_k` | Integer | `k * 3` | Over-fetch for reranking (clamped to [k, 1000]) |
| `reranker_query` | String | `query_text` | Override query text for cross-encoder |
| `maxsim_query` | List\<Vector\> | — | Multivector query (only when `reranker: 'maxsim'`) |
| `maxsim_metric` | String | — | MaxSim distance metric (only when `reranker: 'maxsim'`) |

> **Not plumbed through `uni.search`:** ANN-tuning knobs `nprobes` / `refine_factor` / `ef_search` are not exposed here.

### YIELD Columns

| Column | Type | Description |
|---|---|---|
| `vid` | Integer | Vertex ID |
| `score` | Float | Fused score (or reranker score when reranking is active) |
| `rerank_score` | Float | Cross-encoder / MaxSim score (null when no reranker) |
| `vector_score` | Float | Dense branch score |
| `fts_score` | Float | FTS (BM25) branch score |
| `sparse_score` | Float | Sparse branch score (only populated when the sparse arm is active) |
| `distance` | Float | Raw dense distance |

### Fusion Formulas

**RRF (default):** `score = sum(1 / (60 + rank))` per result across branches. Robust, no score normalization needed.

**Weighted:** `score = alpha * vector_score + (1 - alpha) * fts_score`

| `alpha` | Behavior |
|---|---|
| `0.7` | Favor semantic similarity |
| `0.5` | Equal weight |
| `0.3` | Favor keyword matching |

```cypher
// RRF (default)
CALL uni.search('Document', {vector: 'embedding', fts: 'content'},
    'graph databases', null, 10)
YIELD node, score
RETURN node.title, score

// Weighted fusion favoring semantics, with pre-filter
CALL uni.search('Document', {vector: 'embedding', fts: 'content'},
    'deep learning', null, 10,
    'category = "technology" AND year >= 2023',
    {method: 'weighted', alpha: 0.7})
YIELD node, score, vector_score, fts_score
RETURN node.title, score, vector_score, fts_score

// 3-way: dense + FTS + sparse (sparse needs BOTH the `sparse:` key
// AND options.sparse_query — either alone is a silent no-op)
CALL uni.search('Document',
    {vector: 'embedding', fts: 'content', sparse: 'sparse_embedding'},
    'graph databases', null, 10, null,
    {method: 'weighted',
     weights: [0.5, 0.2, 0.3],
     sparse_query: {indices: [3, 17, 482], values: [0.7, 0.4, 0.9]}})
YIELD vid, score, vector_score, fts_score, sparse_score
RETURN vid, score, vector_score, fts_score, sparse_score
ORDER BY score DESC
```

**Prerequisites:** Hybrid search requires both a vector index (with embedding config) and a fulltext index on the respective properties. The 3-way form additionally needs a sparse index on the `sparse:` property.

---

## 6. Vector Index Configuration

### Index Type Decision Tree

| Dataset Size | Recommended | Notes |
|---|---|---|
| < 10k vectors | **Flat** | Exact brute-force; no tuning needed |
| 10k - 1M vectors | **HNSW-SQ** (recommended) | Best recall-latency tradeoff with scalar quantization |
| > 1M vectors, high recall | **HNSW-PQ** | Graph-based with product quantization for memory savings |
| > 1M vectors, memory-constrained | **IVF-PQ** | Partition-based with product quantization, smallest footprint |
| > 1M vectors, quality priority | **IVF-SQ** | Partition-based with scalar quantization, better recall than PQ |

> **No-type default = IVF-PQ.** `CREATE VECTOR INDEX ... OPTIONS { metric: 'cosine' }` with no `type` (and the short form `... WITH { metric: 'cosine' }`) produces **IVF-PQ** (256 partitions / 16 sub_vectors / 8 bits), not HNSW-SQ. HNSW-SQ is a *recommended* choice for the 10k–1M band, but it is never the implicit default — request it explicitly with `type: 'hnsw_sq'`.

### Algorithm Variants

All 8 algorithms available, grouped by architecture:

**Flat (no index structure):**

| Type | Quantization | Parameters | Best For |
|---|---|---|---|
| **Flat** | None | — | < 10k vectors, exact results |

**IVF (Inverted File — partition-based):**

| Type | Quantization | Parameters | Best For |
|---|---|---|---|
| **IVF-Flat** | None | `partitions` | Medium datasets, exact within partitions |
| **IVF-SQ** | Scalar (int8) | `partitions` | Large datasets, good recall/memory tradeoff |
| **IVF-PQ** | Product | `partitions`, `sub_vectors`, `num_bits` | Very large datasets, minimum memory |
| **IVF-RQ** | RaBitQ (1-bit) | `partitions`, `num_bits` | Better accuracy than PQ at similar compression |

**HNSW (Hierarchical Navigable Small World — graph-based):**

| Type | Quantization | Parameters | Best For |
|---|---|---|---|
| **HNSW-Flat** | None | `m`, `ef_construction`, `partitions` | Exact graph search, no compression loss |
| **HNSW-SQ** | Scalar (int8) | `m`, `ef_construction`, `partitions` | Recommended for 10k–1M. Best recall-latency tradeoff |
| **HNSW-PQ** | Product | `m`, `ef_construction`, `sub_vectors`, `partitions` | Large datasets needing graph-speed with memory savings |

### Parameter Reference

| Parameter | Applies To | Default | Description |
|---|---|---|---|
| `partitions` | All IVF variants, all HNSW variants | 256 (IVF) / 1 (HNSW) | Number of Voronoi partitions. HNSW default 1 = single global graph. Increase for >1M vectors. |
| `sub_vectors` | IVF-PQ, HNSW-PQ | 16 | Number of PQ sub-quantizers. More = better recall, more memory |
| `num_bits` | IVF-PQ, IVF-RQ | 8 (PQ) / 1 (RQ) | Bits per subvector (PQ) or per dimension (RQ/RaBitQ). RQ: 1=classic RaBitQ, 2/4/8 for higher fidelity |
| `m` | All HNSW variants | 16 | Edges per node in HNSW graph. Higher = better recall, more memory |
| `ef_construction` | All HNSW variants | 200 | Build-time search width. Higher = better graph quality, slower build |

### Distance Metrics

| Metric | Raw Distance Range | Score Conversion | Similarity Range | Best For |
|---|---|---|---|---|
| `Cosine` | [0, 2] (`1 - cos(a,b)`) | `(2 - d) / 2` | [0, 1] | Normalized embeddings (most text models) |
| `L2` | [0, inf) (squared Euclidean) | `1 / (1 + d)` | (0, 1] | Raw embeddings, spatial data |
| `Dot` | (-inf, +inf) (negative dot) | Pass-through | Unbounded | Maximum inner product search |

Score conversion is **metric-aware** and shared across `uni.vector.query`, `uni.search`, and `similar_to()`.

---

## 7. Creating Vector Indexes

### Cypher DDL

```cypher
// HNSW-SQ (recommended for 10k–1M; must be requested explicitly)
CREATE VECTOR INDEX idx_embed FOR (d:Document) ON (d.embedding)
OPTIONS { type: 'hnsw_sq', metric: 'cosine' }

// HNSW-PQ for large datasets needing graph speed + compression
CREATE VECTOR INDEX idx_embed FOR (d:Document) ON (d.embedding)
OPTIONS { type: 'hnsw_pq', metric: 'cosine', m: 16, ef_construction: 200, sub_vectors: 8 }

// IVF-PQ for very large datasets, minimum memory
CREATE VECTOR INDEX idx_embed FOR (d:Document) ON (d.embedding)
OPTIONS { type: 'ivf_pq', metric: 'l2', partitions: 256, sub_vectors: 16 }

// IVF-SQ for large datasets, better recall than PQ
CREATE VECTOR INDEX idx_embed FOR (d:Document) ON (d.embedding)
OPTIONS { type: 'ivf_sq', metric: 'cosine', partitions: 256 }

// IVF-RQ (RaBitQ quantization — 1 bit per dimension by default)
CREATE VECTOR INDEX idx_embed FOR (d:Document) ON (d.embedding)
OPTIONS { type: 'ivf_rq', metric: 'cosine', partitions: 256 }

// IVF-RQ with higher fidelity (4 bits per dimension)
CREATE VECTOR INDEX idx_embed FOR (d:Document) ON (d.embedding)
OPTIONS { type: 'ivf_rq', metric: 'cosine', partitions: 256, num_bits: '4' }

// HNSW-Flat (graph search, no quantization — exact results)
CREATE VECTOR INDEX idx_embed FOR (d:Document) ON (d.embedding)
OPTIONS { type: 'hnsw_flat', metric: 'cosine', m: 16, ef_construction: 200 }

// HNSW-SQ with IVF partitions (for very large datasets >1M)
CREATE VECTOR INDEX idx_embed FOR (d:Document) ON (d.embedding)
OPTIONS { type: 'hnsw_sq', metric: 'cosine', m: 16, ef_construction: 200, partitions: '32' }

// IVF-Flat (no quantization, exact within partitions)
CREATE VECTOR INDEX idx_embed FOR (d:Document) ON (d.embedding)
OPTIONS { type: 'ivf_flat', metric: 'cosine', partitions: 128 }

// Flat (brute-force, exact)
CREATE VECTOR INDEX idx_embed FOR (d:Document) ON (d.embedding)
OPTIONS { type: 'flat', metric: 'cosine' }

// With auto-embedding config (works with any algorithm)
CREATE VECTOR INDEX doc_embed FOR (d:Document) ON (d.embedding)
OPTIONS {
    metric: 'cosine',
    embedding: {
        alias: 'embed/default',
        source: ['content'],
        batch_size: 32
    }
}

// No type given → IVF-PQ (256 partitions / 16 sub_vectors / 8 bits)
CREATE VECTOR INDEX idx_embed FOR (d:Document) ON (d.embedding)
OPTIONS { metric: 'cosine' }

// Short form also defaults to IVF-PQ
CREATE VECTOR INDEX idx_embed FOR (d:Document) ON (d.embedding) OPTIONS { metric: 'cosine' }
```

### Rust API

```rust
use uni_db::{DataType, IndexType, VectorAlgo, VectorIndexCfg, VectorMetric};

db.schema()
    .label("Document")
        .property("title", DataType::String)
        .property("embedding", DataType::Vector { dimensions: 384 })
        .index("embedding", IndexType::Vector(VectorIndexCfg {
            algorithm: VectorAlgo::HnswSq { m: 16, ef_construction: 200, partitions: None },
            metric: VectorMetric::Cosine,
            embedding: None,
        }))
    .apply()
    .await?;

// All algorithm variants:
// VectorAlgo::Flat
// VectorAlgo::IvfFlat { partitions: 256 }
// VectorAlgo::IvfPq { partitions: 256, sub_vectors: 16 }
// VectorAlgo::IvfSq { partitions: 256 }
// VectorAlgo::IvfRq { partitions: 256, num_bits: None }           // RaBitQ (default 1-bit)
// VectorAlgo::HnswFlat { m: 16, ef_construction: 200, partitions: None }  // no quantization
// VectorAlgo::Hnsw { m: 16, ef_construction: 200, partitions: None }      // alias for HnswSq
// VectorAlgo::HnswSq { m: 16, ef_construction: 200, partitions: None }
// VectorAlgo::HnswPq { m: 16, ef_construction: 200, sub_vectors: 16, partitions: None }
```

### Python API

```python
db.schema() \
    .label("Document") \
        .property("title", "string") \
        .vector("embedding", 384) \
        .index("embedding", "vector") \
        .done() \
    .apply()
```

---

## 8. Full-Text Index Configuration

### BM25 Fulltext Index

```cypher
CREATE FULLTEXT INDEX idx_content FOR (a:Article) ON EACH [a.content]

// Multi-property
CREATE FULLTEXT INDEX doc_fts FOR (d:Doc) ON EACH [d.title, d.body]
```

Index is built automatically on creation over existing data. No manual `rebuild_indexes()` needed.

### JSON FTS Index

```cypher
CREATE JSON FULLTEXT INDEX idx_meta FOR (d:Data) ON metadata
```

Enables full-text search on nested JSON/JSONB property values.

### CONTAINS Predicate

```cypher
MATCH (d:Doc) WHERE d.body CONTAINS 'vector' RETURN d.title
```

---

## 9. Auto-Embedding / Xervo

### Embedding Config on Vector Index

```cypher
CREATE VECTOR INDEX doc_embed FOR (d:Document) ON (d.embedding)
OPTIONS {
    metric: 'cosine',
    embedding: {
        alias: 'embed/default',
        source: ['title', 'content'],
        batch_size: 32
    }
}
```

- **On write:** The writer auto-embeds text from `source` properties using the `alias` model, stores result in the vector property.
- **On query:** `uni.vector.query` and `similar_to()` auto-embed string query arguments using the same alias.

### Supported Providers

| Provider | Type | Feature Flag |
|---|---|---|
| MistralRS | Local | `provider-mistralrs` |
| Candle | Local | `provider-candle` |
| ONNX (raw / rerank / embed) | Local | `provider-onnx` |
| OpenAI | Remote | `provider-openai` |
| Gemini | Remote | `provider-gemini` |
| Anthropic | Remote | `provider-anthropic` |
| Vertex AI | Remote | `provider-vertexai` |
| Mistral | Remote | `provider-mistral` |
| Cohere | Remote | `provider-cohere` |
| Voyage AI | Remote | `provider-voyageai` |
| Azure OpenAI | Remote | `provider-azure-openai` |

### Direct Xervo API

```rust
let xervo = db.xervo()?;

// Embed
let vectors = xervo.embed("embed/default", &["query text"]).await?;
// -> Vec<Vec<f32>>

// Generate (structured messages)
let result = xervo.generate("llm/default", &[
    Message::system("You are a helpful assistant."),
    Message::user("Summarize this document."),
], GenerationOptions::default()).await?;
```

```python
xervo = db.xervo()
vectors = xervo.embed("embed/default", ["graph databases", "neural search"])
# -> list[list[float]]
```

---

## 10. Best Practices

### Metric Matching
- Use **Cosine** for most text embedding models (they output normalized vectors).
- Use **L2** for raw/unnormalized embeddings or spatial data.
- Use **Dot** for maximum inner product search. Check your model's documentation.

### Index Type Selection
- **< 10k rows:** Flat (exact). Graph/partition-based indexes need minimum data to be effective.
- **10k - 1M rows:** HNSW-SQ (recommended, best recall-latency tradeoff with scalar quantization). Request explicitly with `type: 'hnsw_sq'` — the no-type default is IVF-PQ.
- **> 1M rows, quality priority:** HNSW-PQ or IVF-SQ (graph speed or good recall with moderate compression).
- **> 1M rows, memory-constrained:** IVF-PQ (most aggressive compression, smallest footprint).
- **Experimental:** IVF-RQ (residual quantization, potentially better accuracy than PQ at same compression).

### Hybrid Search Tuning
- Start with **RRF** (robust default, no tuning needed).
- Switch to **weighted** when you want explicit semantic vs keyword balance.
- `over_fetch: 2.0` is usually sufficient; increase for highly selective pre-filters.

### Pre-Filtering vs Post-Filtering
- **Pre-filter** (`filter` param in `uni.vector.query`): Pushed to Lance, searches only the filtered subset. Use when filter is selective.
- **Post-filter** (`WHERE` after `YIELD`): Searches all nodes, then filters. Use for complex Cypher expressions not expressible in SQL, or non-selective filters.
- Pre-filtering is more efficient when it significantly reduces the search space.

### `similar_to` vs CALL Procedures
- Use `CALL uni.vector.query` / `uni.search` to **find** top-K candidates from a full label.
- Use `similar_to()` to **score** nodes already bound by `MATCH` (e.g., after graph traversal).

### Performance Tips
- `YIELD vid` is much faster than `YIELD node` for large result sets (skips property loading).
- Ensure embedding dimensions match between model and schema.
- Ensure the same model is used for indexing and querying.

---

## 11. Examples

### RAG Pipeline End-to-End

```cypher
// 1. Setup: schema + indexes
CREATE VECTOR INDEX doc_embed FOR (d:Document) ON (d.embedding)
OPTIONS {
    metric: 'cosine',
    embedding: { alias: 'embed/default', source: ['content'], batch_size: 32 }
}

CREATE FULLTEXT INDEX doc_fts FOR (d:Document) ON EACH [d.content]

// 2. Ingest (auto-embeds on write)
CREATE (d:Document {title: 'Graph DBs 101', content: 'Graph databases store...'})

// 3. Retrieve (auto-embeds the query text)
CALL uni.vector.query('Document', 'embedding', 'how do graph databases work', 5)
YIELD node, score
RETURN node.title, node.content, score
ORDER BY score DESC
```

### Semantic Search with Graph Context

```cypher
// Find similar papers, then expand citations
CALL uni.vector.query('Paper', 'embedding', $query_vector, 10)
YIELD node AS seed, score

MATCH (seed)-[:CITES]->(cited:Paper)
RETURN seed.title AS source, cited.title AS cited_paper, score
ORDER BY score DESC, cited.year DESC
```

### Multi-Hop with Similarity Filter

```cypher
MATCH (start:Paper {title: 'Attention Is All You Need'})
MATCH (start)-[:CITES]->(hop1:Paper)-[:CITES]->(hop2:Paper)
WHERE similar_to(start.embedding, hop2.embedding) > 0.7
RETURN DISTINCT hop2.title, hop2.year
ORDER BY hop2.year DESC
LIMIT 20
```

### Hybrid Search Setup

```cypher
// Prerequisites: both index types
CREATE VECTOR INDEX doc_embed FOR (d:Document) ON (d.embedding)
OPTIONS {
    metric: 'cosine',
    embedding: { alias: 'embed/default', source: ['content'] }
}

CREATE FULLTEXT INDEX doc_fts FOR (d:Document) ON EACH [d.content]

// Hybrid query with score transparency
CALL uni.search('Document', {vector: 'embedding', fts: 'content'},
    'transformer attention mechanisms', null, 10)
YIELD node, score, vector_score, fts_score
RETURN node.title, score, vector_score, fts_score
ORDER BY score DESC
```

### Expression-Based Hybrid Scoring

```cypher
MATCH (d:Document)
RETURN d.title,
  similar_to([d.embedding, d.content], 'machine learning',
    {method: 'weighted', weights: [0.7, 0.3]}) AS relevance
ORDER BY relevance DESC
LIMIT 20
```

---

## 12. Cross-Encoder Reranking

All three search procedures (`uni.vector.query`, `uni.fts.query`, `uni.search`) support an optional **cross-encoder reranking** stage. A cross-encoder jointly attends to a (query, document) pair to produce a more accurate relevance score than bi-encoder similarity or BM25, but is too expensive to run on the full corpus. By running it on a small over-fetched candidate set, you get fast retrieval with high-precision final ranking.

### How It Works

```
Retrieval (vector/FTS/hybrid)  ->  Over-fetch reranker_k candidates  ->  Cross-encoder scores each (query, doc) pair  ->  Top k returned
```

Reranking is **opt-in** — enabled by adding `reranker` to the options map.

### Reranker Options

Added to the options map (last argument) of any search procedure:

| Option | Type | Default | Description |
|---|---|---|---|
| `reranker` | String | `null` (disabled) | Xervo model alias for the cross-encoder (e.g. `'rerank/minilm'`). Enables reranking when present. |
| `reranker_property` | String | FTS property (hybrid/FTS) or **required** (vector) | Node property whose text is fed as the "document" side of the cross-encoder. |
| `reranker_k` | Integer | `k * 3` | Number of candidates to over-fetch for reranking. Clamped to `[k, 1000]`. |
| `reranker_query` | String | Query arg | Override query text for the cross-encoder. **Required** when `uni.vector.query` receives a pre-computed vector (not text). |

### YIELD Columns

When reranking is enabled, a new column is available:

| Column | Type | Description |
|---|---|---|
| `rerank_score` | Float | Sigmoid-normalized cross-encoder relevance score (0-1). `null` when reranker is not configured. |

When reranking is active, the `score` column reflects the **reranker score** (not the retrieval/fusion score). This means `ORDER BY score DESC` always gives the best available relevance signal regardless of whether reranking is on.

### Examples

**Vector search with reranking (text query):**
```cypher
CALL uni.vector.query('Document', 'embedding', 'graph database architecture', 10,
    null, null,
    {reranker: 'rerank/minilm', reranker_property: 'content'})
YIELD node, score, rerank_score, distance
RETURN node.title, score, rerank_score
ORDER BY score DESC
```

**Vector search with reranking (pre-computed vector — must provide reranker_query):**
```cypher
CALL uni.vector.query('Document', 'embedding', $query_vector, 10,
    null, null,
    {reranker: 'rerank/minilm', reranker_property: 'content',
     reranker_query: 'graph database architecture'})
YIELD node, score
RETURN node.title, score
```

**FTS search with reranking (reranker_property defaults to FTS property):**
```cypher
CALL uni.fts.query('Article', 'body', 'machine learning transformers', 10,
    null, null,
    {reranker: 'rerank/minilm'})
YIELD node, score, rerank_score
RETURN node.title, score
```

**Hybrid search with reranking:**
```cypher
CALL uni.search('Document', {vector: 'embedding', fts: 'content'},
    'quantum computing applications', null, 10, null,
    {method: 'rrf', reranker: 'rerank/minilm', reranker_property: 'content'})
YIELD node, score, rerank_score, vector_score, fts_score
RETURN node.title, score, vector_score, fts_score
```

**Controlling over-fetch:**
```cypher
// Reranker sees 30 candidates, returns top 10
CALL uni.vector.query('Document', 'embedding', 'search query', 10,
    null, null,
    {reranker: 'rerank/minilm', reranker_property: 'content', reranker_k: 30})
YIELD node, score
RETURN node.title, score
```

### Available Reranker Providers

| Provider | Provider ID | Model Example | Type |
|---|---|---|---|
| ONNX (local) | `local/onnx` | `cross-encoder/ms-marco-MiniLM-L6-v2` | Local CPU/GPU inference |
| Cohere | `remote/cohere` | `rerank-english-v3.0` | Remote API |
| Voyage AI | `remote/voyageai` | `rerank-2` | Remote API |

**Catalog configuration example:**
```json
[
  {
    "alias": "rerank/minilm",
    "task": "Rerank",
    "provider_id": "local/onnx",
    "model_id": "cross-encoder/ms-marco-MiniLM-L6-v2"
  }
]
```

The local ONNX provider (`local/onnx`) requires the `provider-onnx` feature flag. It downloads the model and tokenizer from HuggingFace on first use and caches them locally.

### Reranking Does NOT Apply to `similar_to()`

`similar_to()` is a per-row scalar expression — it scores every row DataFusion hands it and has no bounded candidate set. Cross-encoders are only effective on small candidate sets (tens to hundreds), so reranking is limited to the three search procedures which control their own retrieval and top-k.

### Direct Reranker API

```rust
let scored = db.xervo().rerank(
    "rerank/minilm",
    "How does Rust handle memory safety?",
    &["Rust uses ownership and borrowing.", "Python uses garbage collection."],
).await?;
// scored: Vec<ScoredDoc> sorted by relevance descending
```

```python
scored = db.xervo().rerank(
    "rerank/minilm",
    "How does Rust handle memory safety?",
    ["Rust uses ownership and borrowing.", "Python uses garbage collection."],
)
```
