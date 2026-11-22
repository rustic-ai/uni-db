# BGE-M3 Hybrid Retrieval (One Pass → Dense + Sparse + Multi-Vector)

[BGE-M3](https://huggingface.co/BAAI/bge-m3) is a single embedding model that emits **three** representations of the same text in one forward pass:

- a **dense** vector (semantic similarity),
- a **learned-sparse** vector (SPLADE-style lexical weighting), and
- a **multi-vector / ColBERT** set (one vector per token, scored by late-interaction MaxSim).

This guide shows the headline pattern: configure **one** `EmbedHybrid` alias, point three index columns at it, and let a single model pass fill all three. Then run a 3-way `uni.search` that fuses dense + full-text + sparse and (optionally) re-ranks with MaxSim.

The schema and DDL below mirror the working integration test at `crates/uni/tests/common/bge_m3_hybrid_3way.rs` (mock model) and `crates/uni/tests/common/bge_m3_real_onnx.rs` (the real `aapot/bge-m3-onnx` model).

---

## How One Pass Fills Three Columns

The key idea: **the same alias and the same source property are referenced by multiple index configs.** When several `embedding: {alias, source}` configs share an identical alias + source, the engine auto-detects them as one **hybrid group** and runs the model exactly once per document, distributing each head to its column.

There is **no `head:` sub-key**. The engine infers which head feeds which column from the **destination column's DataType**:

| Destination column DataType | Head used |
|---|---|
| `Vector` | dense |
| `SparseVector` | learned-sparse |
| `List<Vector>` | multi-vector / ColBERT |

So you declare the *shape* of each column, attach the *same* hybrid alias to each, and routing falls out of the types.

---

## Step 1: Configure the BGE-M3 `EmbedHybrid` Alias

Add a single catalog alias whose task is `EmbedHybrid`, provided by `local/onnx` and backed by `aapot/bge-m3-onnx`:

```rust
use uni_db::{ModelAliasSpec, ModelTask, WarmupPolicy};

let bge_m3 = ModelAliasSpec {
    alias: "hybrid/bge-m3".to_string(),
    task: ModelTask::EmbedHybrid,
    provider_id: "local/onnx".to_string(),
    model_id: "aapot/bge-m3-onnx".to_string(),
    revision: None,
    warmup: WarmupPolicy::Lazy,
    required: false,
    timeout: None,
    load_timeout: None,
    retry: None,
    options: serde_json::json!({}),
};

let db = Uni::temporary()
    .xervo_catalog(vec![bge_m3])
    .build()
    .await?;
```

`EmbedHybrid` is the task that produces dense + sparse + multi-vector heads together. (The underlying model handle and its head set live in the external `uni-xervo` crate; here we only describe how to wire it from `uni-db`.)

---

## Step 2: Declare the Schema — Three Columns, One Shared Alias

Declare a `Doc` label with the source text plus three typed embedding columns. The dense `Vector`, the `SparseVector`, and the multi-vector `List<Vector>` index configs all reference the **same** alias (`hybrid/bge-m3`) and the **same** source (`content`):

```rust
use uni_db::{
    DataType, EmbeddingCfg, IndexType, VectorAlgo, VectorIndexCfg, VectorMetric,
};

const DENSE_DIM: usize = 1024;   // BGE-M3 dense dimension
const TOKEN_DIM: usize = 1024;   // BGE-M3 ColBERT token dimension
const VOCAB: usize = 250002;     // BGE-M3 sparse term-space (XLM-RoBERTa vocab)

// One shared hybrid embedding config — same alias + same source everywhere.
let hybrid_emb = || EmbeddingCfg {
    alias: "hybrid/bge-m3".to_string(),
    source_properties: vec!["content".to_string()],
    batch_size: 16,
    document_prefix: None,
    query_prefix: None,
};

let dense_index = || IndexType::Vector(VectorIndexCfg {
    algorithm: VectorAlgo::Flat,             // or IvfPq for large corpora
    metric: VectorMetric::Cosine,
    embedding: Some(hybrid_emb()),
});

db.schema()
    .label("Doc")
        .property("content", DataType::String)
        // dense column  → Vector       → routed the dense head
        .property_nullable("embedding", DataType::Vector { dimensions: DENSE_DIM })
        // sparse column → SparseVector  → routed the sparse head
        .property_nullable("emb", DataType::SparseVector { dimensions: VOCAB })
        // multi column  → List<Vector>  → routed the multi-vector head
        .property_nullable(
            "tokens",
            DataType::List(Box::new(DataType::Vector { dimensions: TOKEN_DIM })),
        )
        .index("embedding", dense_index())
        .index("emb", IndexType::sparse_with_embedding(VOCAB, hybrid_emb()))
        .index("tokens", dense_index())
    .apply()
    .await?;
```

Because every index config carries the identical `hybrid/bge-m3` alias and `content` source, the engine treats the three as **one single-pass 3-way group** — exactly the shape `bge_m3_hybrid_3way.rs` exercises and asserts round-trips through every read path (post-flush, pre-flush L0 scan, and deferred-batch flush).

!!! note "Routing is by DataType, not by a `head:` key"
    You never name a head in the config. `Vector` → dense, `SparseVector` → sparse, `List<Vector>` → multi-vector. Declare the column type and the engine routes the matching head.

---

## Step 3: Ingest Documents (Auto-Embed)

Insert documents with just the `content` text. On insert (or at flush), Uni runs BGE-M3 once per document and fills all three columns:

```cypher
CREATE (:Doc {content: 'graph databases for semantic retrieval'})
CREATE (:Doc {content: 'late interaction reranking with ColBERT'})
CREATE (:Doc {content: 'learned sparse retrieval with SPLADE'})
```

For at-scale ingest, defer embedding to flush time (materialized in batches via the deferred-batch path):

```rust
use uni_common::UniConfig;

let cfg = UniConfig { defer_embeddings: true, ..UniConfig::default() };
let db = Uni::temporary().config(cfg).xervo_catalog(vec![bge_m3]).build().await?;
// ... CREATE many docs, then:
db.flush().await?;
```

---

## Step 4: 3-Way Hybrid Search with `uni.search`

`uni.search` fuses **dense + full-text + sparse** in one call. The sparse arm is **opt-in**: it activates only when you provide *both* a `sparse:` key in the `properties` map *and* an `options.sparse_query`. Supplying just one of the two is a silent no-op.

```cypher
CALL uni.search(
    'Doc',
    {vector: 'embedding', fts: 'content', sparse: 'emb'},   // all three arms
    'semantic retrieval over graphs',                       // text → FTS + auto-embed dense
    null,                                                   // query_vector (auto-embed)
    10,                                                     // k
    null,                                                   // filter
    {
        method: 'weighted',
        weights: [0.4, 0.2, 0.4],                           // [vector, fts, sparse]
        sparse_query: {indices: [42, 9001], values: [1.0, 0.5]}
    }
)
YIELD vid, score, vector_score, fts_score, sparse_score
RETURN vid, score, vector_score, fts_score, sparse_score
ORDER BY score DESC
```

- `properties` keys are `{vector, fts, sparse}`. A bare string (e.g. `'content'` instead of a map) means "use this property for both vector and FTS, sparse off".
- `options.sparse_query` is a `{indices, values}` map (equal-length lists) **or** a native sparse vector.
- `weights` is `[vector, fts, sparse]` for 3-way weighted fusion; for RRF (default) use `method: 'rrf'` and omit `weights`.

### YIELD columns

`uni.search` exposes per-arm scores so you can inspect the fusion:

| Column | Description |
|---|---|
| `vid` | Vertex id |
| `score` | Fused (or reranked) final score |
| `rerank_score` | Reranker score, `null` if no reranker |
| `vector_score` | Dense arm score |
| `fts_score` | Full-text (BM25) arm score |
| `sparse_score` | Sparse dot-product arm score |
| `distance` | Raw dense distance |

!!! warning "Sparse is opt-in — both pieces required"
    `sparse: 'emb'` in the map **and** `options.sparse_query` must both be present. Either alone silently disables the sparse arm. ANN-tuning knobs (`nprobes`/`refine_factor`/`ef_search`) are **not** plumbed through `uni.search`.

---

## Step 5: Add MaxSim Re-Ranking (ColBERT Late Interaction)

The multi-vector `tokens` column lets you finish with an exact MaxSim re-rank over a small over-fetched candidate set. Set `reranker: 'maxsim'`, point `reranker_property` at the multi-vector column, and pass per-token query embeddings via `maxsim_query`:

```cypher
CALL uni.search(
    'Doc',
    {vector: 'embedding', fts: 'content', sparse: 'emb'},
    'semantic retrieval over graphs',
    null,
    10,
    null,
    {
        weights: [0.4, 0.2, 0.4],
        sparse_query: {indices: [42, 9001], values: [1.0, 0.5]},
        reranker: 'maxsim',
        reranker_property: 'tokens',
        maxsim_query: [[0.10, 0.20, /* ... */], [0.30, 0.40, /* ... */]],
        maxsim_metric: 'cosine'
    }
)
YIELD vid, score, rerank_score, vector_score, fts_score, sparse_score
RETURN vid, score, rerank_score
ORDER BY score DESC
```

When a reranker is active, `score` reflects the reranker score; the per-arm retrieval scores remain available. MaxSim is in-process and model-free — it scores stored per-token vectors against your per-token query, so it adds precision without another model call. See [Hybrid Search → Reranking](../features/hybrid-search.md#reranking-cross-encoder-maxsim).

---

## Why This Pattern

One model, one forward pass per document, three complementary signals:

- **Dense** captures conceptual similarity even when wording differs.
- **Sparse** keeps lexical precision and learned term weighting (codes, rare terms).
- **Multi-vector** adds token-level late interaction for high-precision re-ranking.

Fusing them recovers more relevant documents than any single retriever, and the shared-alias design means you pay the model cost **once**, not three times.

---

## Next Steps

- [Sparse Vector Search](sparse-vectors.md) — the `sparse_vector(N)` type, `uni.sparse.query`, quantization
- [Hybrid Search](../features/hybrid-search.md) — full `uni.search` signature, fusion methods, reranking
- [Vector Search](vector-search.md) — dense and multi-vector (ColBERT) search
- [Indexing → Sparse Vectors](../concepts/indexing.md#sparse-vectors) — sparse index kind and term-space sizing
