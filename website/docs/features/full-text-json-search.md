# Full-Text + JSON Search

Uni supports full-text search over string properties and JSON documents, plus JSON path predicates for nested fields.

## What It Provides

- Full-text indexes for string properties.
- JSON full-text search with path targeting.
- `CONTAINS` predicates on JSON documents.
- `CALL uni.fts.query(...)` for BM25-scored search with relevance ranking.

## Example

=== "Rust"
    ```rust
    use uni_db::Uni;

    # async fn demo() -> Result<(), uni_db::UniError> {
    let db = Uni::open("./my_db").build().await?;

    db.execute("CREATE FULLTEXT INDEX doc_fts FOR (d:Doc) ON (d.title, d.body)")
        .await?;

    let rows = db.query(
        "MATCH (d:Doc) WHERE d.body CONTAINS 'vector' RETURN d.title"
    ).await?;

    println!("{:?}", rows);
    # Ok(())
    # }
    ```

=== "Python"
    ```python
    import uni_db

    db = uni_db.Database("./my_db")
    db.execute("CREATE FULLTEXT INDEX doc_fts FOR (d:Doc) ON (d.title, d.body)")

    rows = db.query(
        "MATCH (d:Doc) WHERE d.body CONTAINS 'vector' RETURN d.title"
    )
    print(rows)
    ```

## FTS Query Procedure

For BM25-scored search with normalized relevance scores:

```cypher
-- Create a fulltext index
CREATE FULLTEXT INDEX article_content FOR (a:Article) ON (a.content)

-- Query with relevance scoring
CALL uni.fts.query('Article', 'content', 'database optimization', 20)
YIELD node, score
RETURN node.title, score
ORDER BY score DESC
```

**Parameters:**

- `label` - Node label to search
- `property` - Text property with inverted index
- `search_term` - Search query string
- `k` - Number of results
- `threshold` (optional) - Minimum score (0-1)

**Yields:**

- `vid` - Vertex ID
- `score` - Normalized BM25 score (0-1)
- `node` - Full node with properties

## Use Cases

- Search across documents, notes, or product descriptions.
- JSON documents with nested fields.
- Hybrid filters that combine text search with graph structure.

## When To Use

Use full-text or JSON search when keyword matching and relevance ranking matter more than exact equality on fields.

See also: [Vector Search](vector-search.md) | [Hybrid Search](hybrid-search.md)
