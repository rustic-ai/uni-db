# Full-Text + JSON Search

Uni supports full-text search over string properties and JSON documents, plus JSON path predicates for nested fields.

## What It Provides

- Full-text indexes for string properties.
- JSON full-text search with path targeting.
- `CONTAINS` predicates on JSON documents.
- `CALL uni.fts.query(...)` for BM25-scored search with relevance ranking.

`uni.fts.query` also searches undeclared string properties. It computes BM25
over the documents visible to the current session, using the same tokenizer
as an unindexed declared property. Buffered replacements and deletes are
applied before scoring; flushing does not change the ranking or scores.
This path scans the visible corpus rather than using a persistent text index.
Schemaless search does not accept a pushed-down property filter.

## Example

=== "Rust"
    ```rust
    use uni_db::Uni;

    # async fn demo() -> Result<(), uni_db::UniError> {
    let db = Uni::open("./my_db").build().await?;
    let session = db.session();

    let tx = session.tx().await?;
    tx.execute("CREATE FULLTEXT INDEX doc_fts FOR (d:Doc) ON EACH [d.title, d.body]")
        .await?;
    tx.commit().await?;

    let rows = session.query(
        "MATCH (d:Doc) WHERE d.body CONTAINS 'vector' RETURN d.title"
    ).await?;

    println!("{:?}", rows);
    # Ok(())
    # }
    ```

=== "Python"
    ```python
    import uni_db

    db = uni_db.Uni.open("./my_db")
    session = db.session()

    tx = session.tx()
    tx.execute("CREATE FULLTEXT INDEX doc_fts FOR (d:Doc) ON EACH [d.title, d.body]")
    tx.commit()

    rows = session.query(
        "MATCH (d:Doc) WHERE d.body CONTAINS 'vector' RETURN d.title"
    )
    print(rows)
    ```

## FTS Query Procedure

For BM25-scored search with normalized relevance scores:

```cypher
// Create a fulltext index
CREATE FULLTEXT INDEX article_content FOR (a:Article) ON EACH [a.content]

// Query with relevance scoring
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

## How It Works

### Auto-Build on Index Creation

When you run `CREATE FULLTEXT INDEX`, Uni automatically builds the FTS index over existing data. There is no need to call `rebuild_indexes()` manually — the index is ready to query immediately after creation.

```cypher
// This both creates AND builds the index in one step
CREATE FULLTEXT INDEX article_content FOR (a:Article) ON EACH [a.content]
```

### Immediate Write Visibility (L0 Buffer)

FTS queries see unflushed writes from the in-memory L0 buffer. This means data is searchable immediately after a write, without waiting for a flush to persistent storage. The query engine merges L0 buffer results with on-disk index results transparently.

```cypher
// Write and search in the same session — no flush needed
CREATE (a:Article {title: 'New Article', content: 'graph database optimization'})

// This will find the article even before flush
CALL uni.fts.query('Article', 'content', 'optimization', 10)
YIELD node, score
RETURN node.title, score
```

## Use Cases

- Search across documents, notes, or product descriptions.
- JSON documents with nested fields.
- Hybrid filters that combine text search with graph structure.

## When To Use

Use full-text or JSON search when keyword matching and relevance ranking matter more than exact equality on fields.

See also: [Vector Search](vector-search.md) | [Hybrid Search](hybrid-search.md)
