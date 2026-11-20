# Sparse Vector Search (SPLADE / Learned-Sparse)

Uni supports **learned-sparse** retrieval — the family of models (SPLADE, BGE-M3's sparse head, and similar) that map text onto a high-dimensional but mostly-zero weight vector over the model's vocabulary. Each non-zero entry is a `(term_id, weight)` pair: the term id indexes into a term space (typically the tokenizer vocabulary) and the weight is the learned importance of that term for the document.

This guide covers when to reach for sparse vs dense vectors, declaring a `sparse_vector(N)` property, creating a sparse index, inserting `{indices, values}` sparse vectors, auto-embedding a sparse column, and querying with `uni.sparse.query`.

---

## Sparse vs Dense — When to Use Which

| | **Dense** (e.g. BGE-base, MiniLM) | **Sparse** (SPLADE, BGE-M3 sparse) |
|---|---|---|
| Vector shape | A few hundred to ~1.5k floats, all non-zero | One weight per vocabulary term, almost all zero |
| Strength | Semantic / conceptual similarity | Lexical / term-overlap matching with learned term weighting |
| Out-of-domain | Can degrade when terminology drifts | Robust on rare terms, exact identifiers, codes |
| Interpretability | Opaque | Each non-zero is a real vocabulary term |

Sparse retrieval sits between classic BM25 and dense vectors: it keeps the term-level precision of keyword search but lets the model **learn** which terms matter and **expand** documents with related terms. In practice the strongest pipelines combine both — see the [BGE-M3 Hybrid Retrieval guide](bge-m3-hybrid-retrieval.md) for running dense + sparse + multi-vector from a single model pass.

---

## Step 1: Declare a Sparse Vector Property

A sparse column has type `sparse_vector(N)`, where `N` is the **term-space cardinality** — the size of the index space the model emits, i.e. `max_term_id + 1`. For a vocabulary-based model this is the tokenizer vocabulary size (e.g. 30522 for BERT-style models, 250002 for BGE-M3's XLM-RoBERTa vocabulary).

=== "Cypher"
    ```cypher
    CREATE LABEL Doc (
        content STRING,
        emb     SPARSE_VECTOR(30522)
    )
    ```

=== "Rust"
    ```rust
    use uni_db::{DataType, Uni};

    db.schema()
        .label("Doc")
            .property("content", DataType::String)
            .property_nullable("emb", DataType::SparseVector { dimensions: 30522 })
        .apply()
        .await?;
    ```

`dimensions` does **not** mean "number of non-zero terms" — a sparse vector for a given document usually has only tens to a few hundred non-zeros. It is the term-space size, so any `term_id` the model can emit fits inside `[0, dimensions)`.

---

## Step 2: Create a Sparse Index

A sparse index uses the `sparse` index type. It stores an inverted layout (term id → posting list of `(vid, weight)`) and scores candidates by **dot product** of the query weights against the stored document weights.

```cypher
CREATE VECTOR INDEX doc_sparse
FOR (d:Doc)
ON (d.emb)
OPTIONS {
    type: 'sparse',
    quantize: false
}
```

```rust
use uni_db::IndexType;

db.schema()
    .label("Doc")
        .property_nullable("emb", DataType::SparseVector { dimensions: 30522 })
        .index("emb", IndexType::sparse(30522))
    .apply()
    .await?;
```

### Quantization

| `quantize` | Storage | Fidelity |
|---|---|---|
| `true` (**default**) | 8-bit quantized weights | Smaller index, near-lossless ranking for most corpora |
| `false` | Lossless `f32` weights | Exact dot products at the cost of larger postings |

`quantize` defaults to **true** (8-bit weight quantization). Set `quantize: false` for lossless `f32` weights when you need exact scores or your weights span a wide dynamic range. See [Indexing → Sparse Vectors](../concepts/indexing.md#sparse-vectors) for the term-space and quantization reference.

---

## Step 3: Insert Sparse Vectors

A sparse vector value is a `{indices, values}` map of two **equal-length** lists: `indices` are the term ids (each `< dimensions`) and `values` are their weights.

```cypher
CREATE (:Doc {
    content: 'graph databases for semantic retrieval',
    emb: {indices: [42, 1337, 9001], values: [0.81, 0.44, 0.62]}
})
```

The two lists are positional: term `42` has weight `0.81`, term `1337` has weight `0.44`, and so on. Term ids do not need to be sorted, but they must be unique and within `[0, dimensions)`.

---

## Step 4: Query with `uni.sparse.query`

Use the `uni.sparse.query` procedure to retrieve the top-K documents by sparse dot product.

```cypher
CALL uni.sparse.query(
    'Doc',                                              // label
    'emb',                                              // sparse property
    {indices: [42, 9001], values: [1.0, 0.5]},          // query sparse vector
    10                                                  // k
)
YIELD vid, score, rerank_score
RETURN vid, score
ORDER BY score DESC
```

### Signature

```
uni.sparse.query(label, property, query, k [, filter] [, threshold] [, options])
YIELD vid, score, rerank_score
```

| Parameter | Type | Description |
|---|---|---|
| `label` | String | Node label to search |
| `property` | String | Sparse vector property name |
| `query` | Map or sparse vector | `{indices, values}` (equal-length lists) **or** a native sparse vector |
| `k` | Integer | Number of results |
| `filter` | String | (optional) Pre-filter clause applied before scoring |
| `threshold` | Float | (optional) Minimum `score` to return |
| `options` | Map | (optional) e.g. `{over_fetch: 4.0}` |

### YIELD columns

| Column | Type | Description |
|---|---|---|
| `vid` | Integer | Vertex id of the matched document |
| `score` | Float | The sparse **dot product** of query against document weights — **higher = more similar** |
| `rerank_score` | Float | Reranker score when a reranker is configured; `null` otherwise |

!!! note "`score` is the dot product"
    Unlike cosine-based dense search, `uni.sparse.query` returns the raw **dot product** of the query weights against the stored document weights. Higher is more similar. There is no separate `distance` or `sparse_score` column — the relevance value is `score`.

### With a Pre-Filter and Threshold

```cypher
CALL uni.sparse.query(
    'Doc',
    'emb',
    {indices: [42, 9001], values: [1.0, 0.5]},
    50,
    'lang = ''en''',     // pre-filter
    0.25                 // minimum dot-product score
)
YIELD vid, score
RETURN vid, score
ORDER BY score DESC
LIMIT 10
```

The filter is applied **before** the top-`k` cut: documents the filter excludes are never ranked, so they cannot take the place of ones it allows, and documents written since the last flush are filtered the same way. A filter of the form `_vid IN (…)` scopes the search to a known set of vertices. The same holds for the `filter` argument of `uni.search` when it has a sparse arm.

The filter is resolved against the label's table before the index is consulted, so its cost grows with the number of rows that pass it. A selective filter is cheap; one that most of a large table passes is not.

### Joining Back to the Graph

`vid` is the vertex id, so you can join the results into a normal `MATCH`:

```cypher
CALL uni.sparse.query('Doc', 'emb',
    {indices: [42, 9001], values: [1.0, 0.5]}, 20)
YIELD vid, score
MATCH (d:Doc) WHERE id(d) = vid
RETURN d.content, score
ORDER BY score DESC
```

---

## Auto-Embedding a Sparse Column

Instead of supplying `{indices, values}` by hand, attach an embedding alias to the sparse index. On insert, Uni passes the source text through the aliased model's sparse head and stores the resulting sparse vector automatically.

```cypher
CREATE VECTOR INDEX doc_sparse
FOR (d:Doc) ON (d.emb)
OPTIONS {
    type: 'sparse',
    embedding: {
        alias: 'sparse/splade',
        source: ['content']
    }
}
```

```rust
use uni_db::{EmbeddingCfg, IndexType};

let emb = EmbeddingCfg {
    alias: "sparse/splade".to_string(),
    source_properties: vec!["content".to_string()],
    batch_size: 32,
    document_prefix: None,
    query_prefix: None,
};

db.schema()
    .label("Doc")
        .property_nullable("emb", DataType::SparseVector { dimensions: 30522 })
        .index("emb", IndexType::sparse_with_embedding(30522, emb))
    .apply()
    .await?;
```

The `alias` must point at a model in your Uni-Xervo catalog whose task emits a sparse head. With an embedding configuration on the index you can also pass **text** as the query — Uni embeds it through the same model before scoring:

```cypher
CALL uni.sparse.query('Doc', 'emb', 'semantic retrieval over graphs', 10)
YIELD vid, score
RETURN vid, score
ORDER BY score DESC
```

A single model can fill a sparse column **and** a dense column **and** a multi-vector column from one forward pass — see the [BGE-M3 Hybrid Retrieval guide](bge-m3-hybrid-retrieval.md).

---

## Next Steps

- [BGE-M3 Hybrid Retrieval](bge-m3-hybrid-retrieval.md) — one model pass → dense + sparse + multi-vector
- [Hybrid Search](../features/hybrid-search.md) — fuse sparse with dense and FTS via `uni.search`
- [Vector Search](vector-search.md) — dense similarity search end-to-end
- [Indexing → Sparse Vectors](../concepts/indexing.md#sparse-vectors) — index kind, quantization, term-space sizing
