# Vector Search

Uni provides native vector search over embedding properties with ANN indexes (HNSW, IVF_PQ, Flat). Use it for semantic search, RAG, and similarity-based retrieval.

## What It Provides

- Vector properties stored alongside graph data.
- ANN indexes with cosine, L2, or dot distance.
- `CALL uni.vector.query(...)` for KNN retrieval.

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

## Use Cases

- Semantic search for documents or products.
- RAG retrieval over knowledge graphs.
- Similarity search over embeddings generated in-app.

## When To Use

Choose vector search when you need semantic similarity rather than exact matching. Pair it with graph traversal for contextual results.
