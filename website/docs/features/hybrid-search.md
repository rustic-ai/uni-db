# Hybrid Search

Uni provides hybrid search that combines vector similarity and full-text search using rank fusion algorithms. Get the best of both semantic understanding and keyword matching.

## What It Provides

- Combined vector + FTS search in a single procedure.
- Reciprocal Rank Fusion (RRF) for robust result merging.
- Weighted fusion for tunable semantic vs keyword balance.
- Pre-filtering applied to both search branches.
- Individual score transparency when needed.

## Example

=== "Cypher"
    ```cypher
    // Hybrid search with auto-embedding
    CALL uni.search(
        'Document',
        {vector: 'embedding', fts: 'content'},
        'machine learning optimization',
        null,  // auto-embed the query text
        10
    )
    YIELD node, score
    RETURN node.title, score
    ORDER BY score DESC
    ```

=== "Rust"
    ```rust
    use uni_db::Uni;

    # async fn demo() -> Result<(), uni_db::UniError> {
    let db = Uni::open("./my_db").build().await?;
    let session = db.session();

    let rows = session.query(r#"
        CALL uni.search(
            'Document',
            {vector: 'embedding', fts: 'content'},
            'neural network architectures',
            null,
            10
        )
        YIELD node, score, vector_score, fts_score
        RETURN node.title, score, vector_score, fts_score
    "#).await?;

    println!("{:?}", rows);
    # Ok(())
    # }
    ```

=== "Python"
    ```python
    import uni_db

    db = uni_db.Uni.open("./my_db")
    session = db.session()

    rows = session.query("""
        CALL uni.search(
            'Document',
            {vector: 'embedding', fts: 'content'},
            'neural network architectures',
            null,
            10
        )
        YIELD node, score
        RETURN node.title, score
    """)
    print(rows)
    ```

## Procedure Signature

<!-- doctest: skip -->
```cypher
CALL uni.search(label, properties, query_text, query_vector, k [, filter] [, options])
YIELD vid, score, node [, vector_score] [, fts_score] [, sparse_score] [, distance]
```

Arguments are positional. `k` is the fifth, so `query_vector` cannot be dropped to reach it —
pass `null` to auto-embed `query_text` instead.

**Parameters:**

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `label` | String | Yes | Node label to search |
| `properties` | Map | Yes | `{vector: 'prop', fts: 'prop', sparse: 'prop'}` — a bare string means same prop for vector + fts, sparse off |
| `query_text` | String | Yes | Text for FTS and auto-embedding |
| `query_vector` | List or null | Yes | Pre-computed dense vector; `null` auto-embeds `query_text` |
| `k` | Integer | Yes | Number of results. **No default** — omitting it is an error |
| `filter` | String | No | WHERE clause for pre-filtering |
| `options` | Map | No | Fusion options |

**Fusion Options:**

| Option | Values | Description |
|--------|--------|-------------|
| `method` | `'rrf'` (default), `'weighted'` | Fusion algorithm |
| `alpha` | 0.0 - 1.0 | Vector weight for **2-way** weighted fusion (default: 0.5) |
| `weights` | `[vector, fts, sparse]` | Per-arm weights for **3-way** weighted fusion |
| `rrf_k` | Integer | RRF constant (default: 60) |
| `over_fetch` | Float | Over-fetch factor (default: 2.0) |
| `sparse_query` | Map or sparse vector | `{indices, values}` query enabling the sparse arm (see below) |
| `reranker` | String | `'maxsim'` for in-process late-interaction, or a Xervo alias for a cross-encoder model (see [Reranking](#reranking-cross-encoder-maxsim)) |
| `reranker_property` | String | Node property: text for a cross-encoder, or the multi-vector property for MaxSim |
| `reranker_k` | Integer | Candidates for reranking (default: k×3, max: 1000) |
| `reranker_query` | String | Override query text for a cross-encoder (ignored for MaxSim) |
| `maxsim_query` | List of vectors | **MaxSim only**: per-token query embeddings, e.g. `[[...], [...]]` (required when `reranker: 'maxsim'`) |
| `maxsim_metric` | String | **MaxSim only**: `'cosine'` (default), `'dot'`, or `'l2'` |

## Fusion Methods

### RRF (Reciprocal Rank Fusion) - Default

Best for general use. Combines rankings without requiring score normalization:

```
score = Σ 1/(k + rank)  where k=60
```

### Weighted Fusion

Use when you want to tune the balance between semantic and keyword matching:

```
score = alpha × vector_score + (1 - alpha) × fts_score
```

- `alpha = 0.7` → Favor semantic similarity
- `alpha = 0.3` → Favor keyword matching
- `alpha = 0.5` → Equal weight (default)

## Examples

### With Pre-Filter

```cypher
CALL uni.search(
    'Document',
    {vector: 'embedding', fts: 'content'},
    'graph databases',
    null,
    10,
    'category = "technology" AND year >= 2023'
)
YIELD node, score
RETURN node.title, score
```

### Weighted Fusion (Favor Semantics)

```cypher
CALL uni.search(
    'Document',
    {vector: 'embedding', fts: 'content'},
    'deep learning',
    null,
    10,
    null,
    {method: 'weighted', alpha: 0.7}
)
YIELD node, score
RETURN node.title, score
```

### Score Transparency

```cypher
CALL uni.search(
    'Document',
    {vector: 'embedding', fts: 'content'},
    'transformer models',
    null,
    10
)
YIELD node, score, vector_score, fts_score
RETURN node.title, score, vector_score, fts_score
```

### 3-Way: Dense + FTS + Sparse

`uni.search` can fuse a third arm — **learned-sparse** (SPLADE / BGE-M3 sparse). The sparse arm is **opt-in**: it activates only when you supply **both** a `sparse:` key in the `properties` map **and** an `options.sparse_query`. Supplying just one of the two is a silent no-op.

```cypher
CALL uni.search(
    'Document',
    {vector: 'embedding', fts: 'content', sparse: 'emb'},   // all three arms
    'machine learning optimization',
    null,                                                    // auto-embed dense
    10,
    null,
    {
        method: 'weighted',
        weights: [0.4, 0.2, 0.4],                            // [vector, fts, sparse]
        sparse_query: {indices: [42, 9001], values: [1.0, 0.5]}
    }
)
YIELD node, score, vector_score, fts_score, sparse_score
RETURN node.title, score, vector_score, fts_score, sparse_score
ORDER BY score DESC
```

- `properties` map keys are `{vector, fts, sparse}`. A bare string in place of the map uses that one property for vector + FTS, with the sparse arm off.
- `options.sparse_query` is a `{indices, values}` map (equal-length lists) or a native sparse vector.
- `weights` is `[vector, fts, sparse]` for 3-way weighted fusion; for RRF (default) omit `weights` and use `method: 'rrf'`.
- The `sparse_score` YIELD column exposes the per-arm sparse dot-product score (higher = more similar).

!!! warning "Sparse arm needs both pieces"
    `sparse: 'emb'` in the `properties` map **and** `options.sparse_query` must both be present. Either alone silently disables the sparse arm. ANN-tuning knobs (`nprobes`/`refine_factor`/`ef_search`) are **not** plumbed through `uni.search`.

For the headline end-to-end pattern — one BGE-M3 pass filling a dense, sparse, and multi-vector column, then a 3-way search with MaxSim re-rank — see the [BGE-M3 Hybrid Retrieval guide](../guides/bge-m3-hybrid-retrieval.md) and the [Sparse Vector Search guide](../guides/sparse-vectors.md).

## Expression Form: `similar_to`

For scoring already-bound nodes (rather than top-K retrieval), use the `similar_to()` expression function. It works in `WHERE`, `RETURN`, `ORDER BY`, and Locy rule bodies:

```cypher
// Hybrid scoring as an expression (correct way)
MATCH (d:Document)
RETURN d.title,
  similar_to([d.embedding, d.content], 'machine learning') AS relevance
ORDER BY relevance DESC
LIMIT 20

// With weighted fusion
MATCH (d:Document)
WHERE similar_to([d.embedding, d.content], 'deep learning',
  {method: 'weighted', weights: [0.7, 0.3]}) > 0.5
RETURN d.title
```

`similar_to` uses the same fusion algorithms (RRF, weighted) as `uni.search`, but operates on one node at a time. Vector scoring is metric-aware — it automatically uses the index's configured distance metric (Cosine, L2, or Dot Product).

!!! note "RRF in point-computation context"
    Because `similar_to()` scores one node at a time (no ranked list), RRF fusion degenerates to equal-weight averaging. A `RrfPointContext` warning is emitted in this case. Use `method: 'weighted'` for explicit control over source weights.

### Correct vs Incorrect Hybrid

Always use a **single** `similar_to` call with multi-source arrays for hybrid search:

```cypher
// ✅ CORRECT: single call with fusion and BM25 normalization
MATCH (d:Document)
RETURN d.title,
  similar_to([d.embedding, d.content], [$qvec, $qtxt]) AS score
ORDER BY score DESC

// ❌ INCORRECT: naive addition mixes incompatible score scales
MATCH (d:Document)
RETURN d.title,
  (similar_to(d.embedding, $qvec) + similar_to(d.content, $qtxt)) AS score
ORDER BY score DESC
```

Adding two separate `similar_to` calls produces raw score addition without normalization — cosine similarity scores ([0, 1]) and BM25 scores (unbounded) live on different scales. The multi-source form normalizes BM25 via a saturation function (`score / (score + fts_k)`) before fusion.

See the [Vector Search guide](../guides/vector-search.md#similar_to-expression-function) for full documentation.

## Reranking (Cross-Encoder & MaxSim)

All three search procedures (`uni.search`, `uni.vector.query`, `uni.fts.query`) support an optional reranking stage that re-scores a small over-fetched candidate set for higher-precision final ranking. Two modes are available:

- **Cross-encoder** — a neural model that jointly attends to a (query, document) text pair.
- **MaxSim** — in-process, model-free late-interaction (ColBERT) scoring over a stored multi-vector property.

Both share the same over-fetch path: retrieval fetches `reranker_k` candidates (default `k×3`, capped at 1000), the reranker re-scores them, and the top `k` are returned. When reranking is active, `score` reflects the reranker score; original retrieval scores remain available via `vector_score` and `fts_score`, and `rerank_score` is `null` when no reranker is configured.

### Cross-Encoder

A cross-encoder is too expensive to run on the full corpus, so it runs only on the over-fetched candidate set.

```
Retrieval (vector/FTS/hybrid) → Over-fetch reranker_k candidates → Cross-encoder re-scores → Top k returned
```

### Example

```cypher
CALL uni.search(
    'Document',
    {vector: 'embedding', fts: 'content'},
    'transformer attention mechanisms',
    null,
    10,
    null,
    {reranker: 'rerank/minilm', reranker_property: 'content'}
)
YIELD node, score, rerank_score, vector_score, fts_score
RETURN node.title, score
ORDER BY score DESC
```

When reranking is active, `score` reflects the reranker score. Original retrieval scores remain available via `vector_score` and `fts_score`. The `rerank_score` column is `null` when no reranker is configured.

### Available Providers

| Provider | Provider ID | Model | Type |
|----------|-------------|-------|------|
| ONNX (local) | `local/onnx` | `cross-encoder/ms-marco-MiniLM-L6-v2` | Local CPU, `provider-onnx` feature |
| Cohere | `remote/cohere` | `rerank-english-v3.0` | Remote API |
| Voyage AI | `remote/voyageai` | `rerank-2` | Remote API |

### MaxSim (Late-Interaction / ColBERT)

MaxSim is an in-process, model-free reranker. Instead of a neural model it scores each candidate by the exact **MaxSim** of its stored per-token vectors against the query's per-token vectors:

```
MaxSim = Σ_i max_j  similarity(query_token_i, doc_token_j)
```

It is fast (pure CPU over pre-stored embeddings) and requires no model runtime — just a multi-vector (`LIST<VECTOR(dim)>`) property and a per-token query. Set `reranker: 'maxsim'` and pass the query tokens via `maxsim_query`:

```cypher
CALL uni.vector.query(
    'Document',
    'embedding',                                   // dense property for first-stage ANN
    $dense_query_vector,
    50,                                            // over-fetch candidates to re-rank
    null,
    null,
    {
        reranker: 'maxsim',
        reranker_property: 'tokens',               // the LIST<VECTOR> property to score
        maxsim_query: [[0.1, 0.2], [0.3, 0.4]],    // per-token query embeddings
        maxsim_metric: 'cosine'                    // optional; default 'cosine'
    }
)
YIELD node, score, rerank_score
RETURN node.title, rerank_score
ORDER BY rerank_score DESC
```

MaxSim works identically inside `uni.fts.query` and `uni.search` (re-rank FTS or hybrid candidates by late interaction). The query property storing the tokens must be declared as a multi-vector type — see [Multi-Vector Search](vector-search.md#multi-vector-search-colbert-late-interaction).

!!! note "Reranking does not apply to `similar_to()`"
    `similar_to()` is a per-row expression with no bounded candidate set. Reranking (cross-encoder or MaxSim) is only effective on small candidate sets, so it is limited to the three search procedures.

## Use Cases

- **RAG applications**: Combine semantic retrieval with keyword boosting.
- **E-commerce search**: Match product descriptions semantically while respecting exact brand/model queries.
- **Document search**: Find relevant documents even when terminology varies.
- **Knowledge bases**: Surface answers that are both semantically relevant and contain key terms.

## When To Use

Use hybrid search when:

- Users might use different terminology than your content
- Some queries are keyword-specific (product names, codes) while others are conceptual
- You want robust search without tuning
- You need to combine the precision of keywords with the recall of semantics

## Prerequisites

Hybrid search requires both:

1. A **vector index** with embedding configuration
2. A **fulltext index** on the text property

```cypher
// Vector index with auto-embed
CREATE VECTOR INDEX doc_embed FOR (d:Document) ON (d.embedding)
OPTIONS {
    metric: 'cosine',
    embedding: {provider: 'Candle', model: 'all-MiniLM-L6-v2', source: ['content']}
}

// Fulltext index
CREATE FULLTEXT INDEX doc_fts FOR (d:Document) ON EACH [d.content]
```

See also: [Vector Search](vector-search.md) | [Full-Text Search](full-text-json-search.md)
