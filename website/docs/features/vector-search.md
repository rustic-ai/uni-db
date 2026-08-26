# Vector Search

Uni provides native vector search over embedding properties with 8 single-vector ANN index algorithms (Flat, IVF-Flat/SQ/PQ/RQ, HNSW-Flat/SQ/PQ) featuring scalar, product, and RaBitQ quantization — plus MUVERA for multi-vector (ColBERT / late-interaction) columns. The default algorithm is IVF-PQ with cosine distance. Use it for semantic search, RAG, and similarity-based retrieval.

!!! warning "Set `refine_factor` when querying the default IVF-PQ index"

    The default algorithm is IVF-PQ, which stores lossy compressed codes. A
    query with no `refine_factor` ranks results on those approximate distances
    and caps recall at roughly **0.56** on a 1M-vector benchmark — with no error
    and nothing unusual in the result. Adding `refine_factor` re-scores
    candidates against the original vectors and lifts recall to **0.99** for
    about 3% throughput:

    ```cypher
    CALL uni.vector.query('Document', 'embedding', $q, 10, NULL, NULL,
      { nprobes: 64, refine_factor: 20 })
    YIELD node, score
    RETURN node.title, score
    ```

    Raising `nprobes` alone does not fix it. See
    [Indexing — Query-Time Tuning](../concepts/indexing.md#query-time-tuning)
    for the measured numbers and the equivalent `ef_search` guidance for HNSW.

## What It Provides

- Vector properties stored alongside graph data.
- ANN indexes with cosine, L2, or dot distance — scores are automatically converted to [0, 1] similarity regardless of metric.
- `CALL uni.vector.query(...)` for KNN retrieval.
- `similar_to()` expression function for point-scoring bound nodes in `WHERE`, `RETURN`, and Locy rules.
- **Auto-embedding**: Pass text directly and let Uni embed it using the index's configured embedding model.
- **Multi-vector (ColBERT / late-interaction)**: store many vectors per row and rank by exact MaxSim, optionally accelerated by a MUVERA first-stage index.

## Example

=== "Rust"
    ```rust
    use uni_db::{DataType, IndexType, Uni, VectorAlgo, VectorIndexCfg, VectorMetric};

    # async fn demo() -> Result<(), uni_db::UniError> {
    let db = Uni::open("./my_db").build().await?;

    db.schema()
        .label("Document")
            .property("title", DataType::String)
            .property("embedding", DataType::Vector { dimensions: 384 })
            .index("embedding", IndexType::Vector(VectorIndexCfg {
                algorithm: VectorAlgo::HnswSq { m: 16, ef_construction: 200, partitions: None },
                metric: VectorMetric::Cosine,
                embedding: None,  // Or configure auto-embed
            }))
        .apply()
        .await?;

    let session = db.session();
    let rows = session.query_with(
        "CALL uni.vector.query('Document', 'embedding', $q, 10) YIELD node, score RETURN node, score"
    )
    .param("q", vec![0.1_f32, 0.2, 0.3])
    .fetch_all()
    .await?;

    println!("{:?}", rows);
    # Ok(())
    # }
    ```

=== "Python"
    ```python
    import uni_db

    db = uni_db.Uni.open("./my_db")

    db.schema() \
        .label("Document") \
            .property("title", "string") \
            .vector("embedding", 384) \
            .index("embedding", "vector") \
            .done() \
        .apply()

    session = db.session()
    rows = session.query(
        "CALL uni.vector.query('Document', 'embedding', $q, 10) YIELD node, score RETURN node, score",
        {"q": [0.1, 0.2, 0.3]}
    )
    print(rows)
    ```

Declared `VECTOR(dim)` dimensions are enforced (since 2.5.0): wrong-length writes fail with a `TypeError` naming the declared and actual lengths (including auto-embed output, which errors naming the embedding alias), and a query vector of the wrong length errors with "vector dimension mismatch" instead of silently returning 0 rows.

## Auto-Embedding Queries

With an embedding configuration on your index, you can query with text directly:

```cypher
// Create index with embedding config
CREATE VECTOR INDEX doc_embed FOR (d:Document) ON (d.embedding)
OPTIONS {
    metric: 'cosine',
    embedding: {
        alias: 'embed/default',
        source: ['title'],
        batch_size: 32
    }
}

// Query with text - Uni auto-embeds it
CALL uni.vector.query('Document', 'embedding', 'machine learning tutorial', 10)
YIELD node, score
RETURN node.title, score
```

## The `~=` Operator

The `~=` (approximate equality) operator is shorthand for a **top-K vector index scan** — it desugars to `uni.vector.query` under the hood:

```cypher
// ~= operator: top-K scan against the vector index
MATCH (p:Paper) WHERE p.embedding ~= $query_vector
RETURN p.title, p._score AS score
ORDER BY score DESC LIMIT 10
```

`~=` is **vector-only** — it cannot do FTS or hybrid search. For hybrid search, use `similar_to()` with multi-source arrays.

!!! note "`=~` is regex, `~=` is vector similarity"
    These are unrelated operators that look similar. `n.name =~  '(?i)john'` is a regex match on strings. `n.embedding ~= $vec` is vector similarity search.

## Expression-Based Scoring: `similar_to`

For scoring already-bound nodes rather than top-K retrieval, use `similar_to()`:

```cypher
MATCH (a:Paper)-[:CITES]->(b:Paper)
WHERE similar_to(b.embedding, 'attention mechanisms') > 0.7
RETURN b.title, similar_to(b.embedding, 'attention mechanisms') AS score
```

For **hybrid search** (vector + FTS combined), use multi-source arrays with fusion:

```cypher
// Correct hybrid: single similar_to with multi-source arrays
MATCH (d:Doc)
RETURN d.title,
  similar_to([d.embedding, d.content], [$query_vector, $query_text]) AS score
ORDER BY score DESC
```

`similar_to` supports metric-aware vector scoring (Cosine, L2, Dot Product), FTS scoring, and multi-source hybrid fusion with RRF or weighted algorithms. It automatically uses the distance metric configured on the vector index. It works in `WHERE`, `RETURN`, `ORDER BY`, and Locy rule bodies. See the [Vector Search guide](../guides/vector-search.md#similar_to-expression-function) for full details.

## Uni-Xervo Runtime

Beyond auto-embedding, the `Uni::xervo()` facade gives direct access to embedding and generation models:

=== "Embedding"
    ```rust
    let xervo = db.xervo();
    let vectors = xervo.embed("embed/default", &["query text"]).await?;
    ```

=== "Text Generation"
    ```rust
    use uni_db::xervo::{Message, GenerationOptions};
    let xervo = db.xervo();

    // Structured messages with roles
    let result = xervo.generate("llm/default", &[
        Message::system("You are a helpful assistant."),
        Message::user("Summarize this document."),
    ], GenerationOptions::default()).await?;

    // Or plain strings (convenience)
    let result = xervo.generate_text("llm/default",
        &["Summarize this."],
        GenerationOptions::default(),
    ).await?;
    ```

=== "Prefetch (Best Practice)"
    ```rust
    let xervo = db.xervo();
    // Pre-load models at startup to avoid cold-start latency
    xervo.prefetch(&["embed/default", "llm/default"]).await?;
    // Or load everything in the catalog
    xervo.prefetch_all().await?;
    ```

Uni-Xervo supports local providers (Candle, mistral.rs, ONNX Runtime — embed/rerank/raw) and remote providers (OpenAI, Gemini, Anthropic, Cohere, Vertex AI, Mistral, Voyage AI, Azure OpenAI). See the [Vector Search Guide](../guides/vector-search.md) for the full provider table and configuration details.

## Use Cases

- Semantic search for documents or products.
- RAG retrieval over knowledge graphs.
- Similarity search over embeddings generated in-app.
- Scoring graph-traversed nodes with `similar_to()` in `WHERE` and Locy rules.
- LLM generation with context from graph queries.

## Cross-Encoder Reranking

For higher-precision results, add a cross-encoder reranking stage to `uni.vector.query`. The reranker re-scores over-fetched candidates using a (query, document) cross-encoder model:

```cypher
CALL uni.vector.query('Document', 'embedding', 'graph databases', 10,
    null, null,
    {reranker: 'rerank/minilm', reranker_property: 'content'})
YIELD node, score, rerank_score
RETURN node.title, score
```

Supports local ONNX models (`local/onnx`) and remote APIs (Cohere, Voyage AI). See [Hybrid Search — Reranking](hybrid-search.md#reranking-cross-encoder-maxsim) for full details.

## Multi-Vector Search (ColBERT / Late-Interaction)

Late-interaction models (ColBERT, ColQwen2) represent each document and query as a **set of per-token vectors** rather than a single dense vector, then score with **MaxSim** — for each query token, take its best match across the document's tokens, and sum:

```
MaxSim = Σ_i max_j  similarity(query_token_i, doc_token_j)
```

This gives token-level matching precision, the strongest known approach for visual/layout-rich and long-document retrieval.

### Declaring a multi-vector property

A multi-vector property is a `LIST<VECTOR(dim)>` — a variable-length list of fixed-size token vectors. It must be schema-declared (multi-vectors cannot be stored on a schemaless property; a `CypherValue`/JSON column is the flexible-dimension alternative).

=== "Cypher DDL"
    ```cypher
    CREATE LABEL Document (
        title  STRING,
        embedding VECTOR(384),     // dense vector for first-stage ANN
        tokens LIST<VECTOR(96)>    // per-token (ColBERT) vectors
    )
    ```

=== "Rust"
    ```rust
    use uni_db::DataType;

    db.schema()
        .label("Document")
            .property("title", DataType::String)
            .property("embedding", DataType::Vector { dimensions: 384 })
            .property("tokens", DataType::List(Box::new(DataType::Vector { dimensions: 96 })))
        .apply()
        .await?;
    ```

=== "Python"
    ```python
    # Imperative builder: declare the multi-vector via its type string
    db.schema() \
        .label("Document") \
            .property("title", "string") \
            .vector("embedding", 384) \
            .property("tokens", "list:vector:96") \
            .done() \
        .apply()
    ```

The Pydantic OGM maps a `list[Vector[96]]` field to the same `list:vector:96` type — see [Pydantic OGM reference](../reference/pydantic-ogm.md#multi-vector-colbert-fields).

### Querying with MaxSim

Retrieve candidates with a fast first stage (dense ANN, or a MUVERA index over the tokens), then re-rank them by exact MaxSim. Pass the per-token query via `maxsim_query`:

<!-- doctest: skip -->
```cypher
CALL uni.vector.query(
    'Document',
    'embedding',                                // dense property for first-stage ANN
    $dense_query_vector,
    50,                                         // over-fetch candidates to re-rank
    null,
    null,
    {
        reranker: 'maxsim',
        reranker_property: 'tokens',            // the LIST<VECTOR> property
        maxsim_query: [[0.1, 0.2], [0.3, 0.4]], -- per-token query embeddings
        maxsim_metric: 'cosine'                 // optional; default 'cosine'
    }
)
YIELD node, score, rerank_score
RETURN node.title, rerank_score
ORDER BY rerank_score DESC
```

You can also query a multi-vector property **directly** (no separate dense stage) by passing a list of vectors as the query — `score` is then the exact MaxSim similarity:

```cypher
CALL uni.vector.query('Document', 'tokens', [[0.1, 0.2], [0.3, 0.4]], 10)
YIELD node, score
RETURN node.title, score
ORDER BY score DESC
```

### MUVERA first-stage index

For large corpora, add a MUVERA index on the multi-vector column. It encodes each row's token set into a single fixed-dimensional vector (FDE), indexes that with a normal single-vector ANN for fast candidate generation, then re-ranks with exact MaxSim:

```cypher
CREATE VECTOR INDEX doc_tokens FOR (d:Document) ON d.tokens
OPTIONS { type: 'muvera', k_sim: 4, reps: 20, d_proj: 16 }
```

Because the final stage is always an exact MaxSim re-rank, a weak FDE only costs recall, never precision. See [Indexing — MUVERA Multi-Vector Indexes](../concepts/indexing.md#muvera-multi-vector-indexes) for parameters and tuning, and [Hybrid Search — Reranking](hybrid-search.md#reranking-cross-encoder-maxsim) for using MaxSim inside `uni.fts.query` / `uni.search`.

## When To Use

Choose vector search when you need semantic similarity rather than exact matching. Pair it with graph traversal for contextual results.

- Use `CALL uni.vector.query(...)` to **find** top-K candidates from a full label.
- Use `similar_to()` to **score** nodes already bound by `MATCH`.
- Add a `reranker` option for higher-precision results on the final candidate set.

See also: [Full-Text Search](full-text-json-search.md) | [Hybrid Search](hybrid-search.md)
