# Vector Search

Uni provides native vector search over embedding properties with ANN indexes (HNSW, IVF_PQ, Flat). Use it for semantic search, RAG, and similarity-based retrieval.

## What It Provides

- Vector properties stored alongside graph data.
- ANN indexes with cosine, L2, or dot distance.
- `CALL uni.vector.query(...)` for KNN retrieval.
- **Auto-embedding**: Pass text directly and let Uni embed it using the index's configured embedding model.

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
                algorithm: VectorAlgo::Hnsw { m: 16, ef_construction: 200 },
                metric: VectorMetric::Cosine,
                embedding: None,  // Or configure auto-embed
            }))
        .apply()
        .await?;

    let rows = db.query_with(
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

    db = uni_db.Database("./my_db")

    db.schema() \
        .label("Document") \
            .property("title", "string") \
            .vector("embedding", 384) \
            .index("embedding", "vector") \
            .done() \
        .apply()

    rows = db.query(
        "CALL uni.vector.query('Document', 'embedding', $q, 10) YIELD node, score RETURN node, score",
        {"q": [0.1, 0.2, 0.3]}
    )
    print(rows)
    ```

## Auto-Embedding Queries

With an embedding configuration on your index, you can query with text directly:

```cypher
-- Create index with embedding config
CREATE VECTOR INDEX doc_embed FOR (d:Document) ON (d.embedding)
OPTIONS {
    metric: 'cosine',
    embedding: {
        provider: 'Candle',
        model: 'all-MiniLM-L6-v2',
        source: ['content']
    }
}

-- Query with text - Uni auto-embeds it
CALL uni.vector.query('Document', 'embedding', 'machine learning tutorial', 10)
YIELD node, score
RETURN node.title, score
```

## Use Cases

- Semantic search for documents or products.
- RAG retrieval over knowledge graphs.
- Similarity search over embeddings generated in-app.

## When To Use

Choose vector search when you need semantic similarity rather than exact matching. Pair it with graph traversal for contextual results.

See also: [Full-Text Search](full-text-json-search.md) | [Hybrid Search](hybrid-search.md)
