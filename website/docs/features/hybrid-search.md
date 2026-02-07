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
    -- Hybrid search with auto-embedding
    CALL uni.search(
        'Document',
        {vector: 'embedding', fts: 'content'},
        'machine learning optimization',
        null,  -- auto-embed the query text
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

    let rows = db.query(r#"
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

    db = uni_db.Database("./my_db")

    rows = db.query("""
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

```cypher
CALL uni.search(label, properties, query_text [, query_vector] [, k] [, filter] [, options])
YIELD vid, score, node [, vector_score] [, fts_score]
```

**Parameters:**

| Parameter | Type | Description |
|-----------|------|-------------|
| `label` | String | Node label to search |
| `properties` | Map | `{vector: 'prop1', fts: 'prop2'}` |
| `query_text` | String | Text for FTS and auto-embedding |
| `query_vector` | List or null | Pre-computed vector (optional) |
| `k` | Integer | Number of results (default: 10) |
| `filter` | String | WHERE clause for pre-filtering |
| `options` | Map | Fusion options |

**Fusion Options:**

| Option | Values | Description |
|--------|--------|-------------|
| `method` | `'rrf'` (default), `'weighted'` | Fusion algorithm |
| `alpha` | 0.0 - 1.0 | Vector weight for weighted fusion |
| `over_fetch` | Float | Over-fetch factor (default: 2.0) |

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
-- Vector index with auto-embed
CREATE VECTOR INDEX doc_embed FOR (d:Document) ON (d.embedding)
OPTIONS {
    metric: 'cosine',
    embedding: {provider: 'Candle', model: 'all-MiniLM-L6-v2', source: ['content']}
}

-- Fulltext index
CREATE FULLTEXT INDEX doc_fts FOR (d:Document) ON (d.content)
```

See also: [Vector Search](vector-search.md) | [Full-Text Search](full-text-json-search.md)
