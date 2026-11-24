# Indexing

Indexes are critical for query performance in Uni. This guide covers all index types, their use cases, and configuration options.

## Index Types Overview

Uni supports five categories of indexes:

```
┌─────────────────────────────────────────────────────────────────────────────────────────────────────────────┐
│                                              INDEX TYPES                                                     │
├─────────────────────┬─────────────────────┬──────────────────────┬──────────────────────┬────────────────────┤
│    VECTOR INDEXES   │   SCALAR INDEXES    │   FULL-TEXT INDEXES  │   JSON FTS INDEXES   │  INVERTED INDEXES  │
├─────────────────────┼─────────────────────┼──────────────────────┼──────────────────────┼────────────────────┤
│ • HNSW              │ • BTree             │ • Inverted Index     │ • Lance Inverted     │ • Set Membership   │
│ • IVF_PQ            │ • Hash              │ • Tokenizers         │ • BM25 Ranking       │ • ANY IN patterns  │
│ • Flat (exact)      │ • Bitmap            │ • Scoring            │ • Path-Specific      │ • Tag filtering    │
├─────────────────────┼─────────────────────┼──────────────────────┼──────────────────────┼────────────────────┤
│ Similarity search   │ Exact/range queries │ Keyword search       │ JSON document search │ List membership    │
│ Nearest neighbors   │ Equality checks     │ Text matching        │ CONTAINS operator    │ Multi-value props  │
│ Embeddings          │ Sorting             │ Relevance ranking    │ Term positions       │ Security filtering │
└─────────────────────┴─────────────────────┴──────────────────────┴──────────────────────┴────────────────────┘
```

---

## Vector Indexes

Vector indexes enable fast approximate nearest neighbor (ANN) search on embedding columns.

### Supported Algorithms

| Algorithm | Quantization | Trade-offs |
|-----------|-------------|------------|
| **Flat** | None | Perfect recall, O(n) speed. Best for < 10k vectors. |
| **IVF-Flat** | None | Partition-based, exact within partitions |
| **IVF-SQ** | Scalar (int8) | Large datasets, good recall/memory tradeoff |
| **IVF-PQ** | Product | **Default.** Best latency/compression tradeoff; very large datasets |
| **IVF-RQ** | RaBitQ (1-bit) | Better accuracy than PQ at similar compression |
| **HNSW-Flat** | None | Graph search, no compression loss |
| **HNSW-SQ** | Scalar (int8) | Graph search, low latency for mid-size datasets |
| **HNSW-PQ** | Product | Large datasets needing graph speed + compression |
| **MUVERA** | FDE projection | Multi-vector (ColBERT / late-interaction) — see [MUVERA Multi-Vector Indexes](#muvera-multi-vector-indexes) |

### Distance Metrics

| Metric | Formula | Use Case |
|--------|---------|----------|
| **Cosine** | 1 - (A·B)/(‖A‖‖B‖) | Normalized embeddings |
| **L2** | Σ(aᵢ-bᵢ)² | Squared Euclidean distance |
| **Dot** | -A·B | Inner product (unnormalized) |

### Creating Vector Indexes

**Via Cypher:**

```cypher
CREATE VECTOR INDEX paper_embeddings
FOR (p:Paper)
ON p.embedding
OPTIONS {
  type: "hnsw_sq"
}
```

DDL supports all algorithm types via the `type` option: `flat`, `ivf_flat`, `ivf_sq`, `ivf_pq`, `ivf_rq`, `hnsw_flat`, `hnsw_sq` (or `hnsw`), `hnsw_pq`, and `muvera` (for multi-vector columns). Parameters like `m`, `ef_construction`, `partitions`, `sub_vectors`, and `num_bits` can also be passed in OPTIONS. **The default is IVF-PQ with cosine distance**, applied uniformly across every surface (Cypher DDL, the `uni.schema.createIndex` procedure, the Python config map, and the Rust `VectorAlgo` builder).

### HNSW Configuration

HNSW parameters are configurable via DDL or the Rust schema builder:

```cypher
// HNSW-SQ with custom parameters
CREATE VECTOR INDEX paper_embeddings FOR (p:Paper) ON p.embedding
OPTIONS { type: 'hnsw_sq', m: '32', ef_construction: '200' }

// HNSW-Flat (no quantization, exact graph search)
CREATE VECTOR INDEX paper_embeddings FOR (p:Paper) ON p.embedding
OPTIONS { type: 'hnsw_flat', m: '16', ef_construction: '200' }

// HNSW-SQ with IVF partitions for very large datasets (>1M vectors)
CREATE VECTOR INDEX paper_embeddings FOR (p:Paper) ON p.embedding
OPTIONS { type: 'hnsw_sq', partitions: '32' }
```

```rust
use uni_db::{DataType, IndexType, VectorAlgo, VectorIndexCfg, VectorMetric};

db.schema()
    .label("Paper")
        .property("embedding", DataType::Vector { dimensions: 768 })
        .index("embedding", IndexType::Vector(VectorIndexCfg {
            algorithm: VectorAlgo::HnswSq { m: 32, ef_construction: 200, partitions: None },
            metric: VectorMetric::Cosine,
            embedding: None,
        }))
    .apply()
    .await?;
```

### IVF Configuration

```cypher
// IVF-PQ with tuned parameters
CREATE VECTOR INDEX product_embeddings FOR (p:Product) ON p.embedding
OPTIONS { type: 'ivf_pq', partitions: '256', sub_vectors: '16' }

// IVF-RQ (RaBitQ — 1 bit per dimension, best accuracy/compression)
CREATE VECTOR INDEX product_embeddings FOR (p:Product) ON p.embedding
OPTIONS { type: 'ivf_rq', partitions: '256' }

// IVF-RQ with higher fidelity (4 bits per dimension)
CREATE VECTOR INDEX product_embeddings FOR (p:Product) ON p.embedding
OPTIONS { type: 'ivf_rq', partitions: '256', num_bits: '4' }
```

```rust
db.schema()
    .label("Product")
        .property("embedding", DataType::Vector { dimensions: 384 })
        .index("embedding", IndexType::Vector(VectorIndexCfg {
            algorithm: VectorAlgo::IvfRq { partitions: 256, num_bits: None },
            metric: VectorMetric::Cosine,
            embedding: None,
        }))
    .apply()
    .await?;
```

### MUVERA Multi-Vector Indexes

MUVERA indexes accelerate **multi-vector** (ColBERT / late-interaction) columns — a `List<Vector(dim)>` property storing one vector per token. MUVERA encodes each row's variable set of token vectors into a single fixed-dimensional **FDE** (Fixed-Dimensional Encoding) vector, stored in a derived internal column (`__fde_<index_name>`), and indexes that with a standard single-vector ANN. At query time the FDE drives fast first-stage retrieval, then candidates are re-scored with **exact MaxSim**.

```cypher
// Defaults: k_sim=4, reps=20, d_proj=16, inner=ivf_pq
CREATE VECTOR INDEX doc_tokens FOR (d:Document) ON d.tokens
OPTIONS { type: 'muvera' }

// Tuned FDE parameters + explicit inner ANN
CREATE VECTOR INDEX doc_tokens FOR (d:Document) ON d.tokens
OPTIONS { type: 'muvera', k_sim: 4, reps: 20, d_proj: 16, inner: 'ivf_pq' }
```

| Option | Default | Description |
|--------|---------|-------------|
| `k_sim` | 4 | SimHash hyperplanes per repetition (`2^k_sim` buckets) |
| `reps` | 20 | Independent repetitions concatenated into the FDE |
| `d_proj` | 16 | Inner projection dimension (`0` = no projection) |
| `inner` | `ivf_pq` | Single-vector ANN over the derived FDE column |

The derived `__fde_*` column is internal — it never appears in `RETURN`, `SHOW INDEXES`, or `uni.schema.labelInfo`. Because the pipeline always finishes with an **exact MaxSim re-rank**, a poorly-tuned FDE only costs recall, never precision.

!!! note "Tune FDE parameters per corpus"
    The defaults are reasonable starting points, not validated for any specific corpus. FDE recall is corpus-dependent — measure recall on your own data and adjust `reps`/`k_sim` to trade recall against FDE size. MUVERA is opt-in; the default vector index remains IVF-PQ.

See [Vector Search](../features/vector-search.md#multi-vector-search-colbert-late-interaction) for the full multi-vector storage + MaxSim query model.

### Query-Time Tuning

Vector ANN queries accept tuning options in the procedure OPTIONS map to trade recall against latency:

| Option | Type | Applies to | Description |
|--------|------|-----------|-------------|
| `nprobes` | Integer | IVF indexes | Number of IVF partitions to probe (higher = better recall, slower) |
| `refine_factor` | Integer | IVF-PQ/SQ | Re-rank `refine_factor × k` candidates with exact distances (recovers quantization error) |
| `over_fetch` | Float | multi-vector | Candidate over-fetch multiplier before MaxSim re-rank (default `4.0`) |

```cypher
CALL uni.vector.query('Paper', 'embedding', $query_vector, 10, NULL, NULL,
  { nprobes: 8, refine_factor: 10 })
YIELD node, score
RETURN node.title, score
ORDER BY score DESC
```

`refine_factor` is the load-bearing recall knob for quantized (PQ/SQ) indexes — even a small value (e.g. `10`) typically recovers most of the recall lost to quantization.

!!! tip "IVF-PQ applies a `refine_factor` by default (since 3.5)"

    A quantized index now carries a `refine_factor` default derived from its
    compression ratio, so a query that passes nothing still re-scores candidates
    against the original vectors. Pass `refine_factor` explicitly to override it,
    or `refine_factor: 1` to opt out entirely.

    This matters because the un-refined answer is much worse than the index is
    capable of, with no error and nothing unusual in the result. Measured on
    SIFT-1M (1M x 128-d vectors, L2, K=10, recall@10 against the
    dataset's own published ground truth):

    | `nprobes` | `refine_factor` | recall@10 | QPS |
    |---:|---:|---:|---:|
    | 64 | *(none)* | 0.562 | 50.8 |
    | 64 | 5 | 0.926 | 52.3 |
    | 64 | 20 | **0.992** | 49.1 |
    | 128 | *(none)* | 0.562 | 45.4 |

    Raising `nprobes` alone does **not** help — recall is flat from 64 to 128,
    because the ceiling is quantization precision rather than how much of the
    index was searched. `refine_factor` is what lifts it. With the default in
    place the same `nprobes=64` cell measures **0.978** rather than 0.562.

    For comparison, the same corpus with HNSW reaches 0.980 recall at 32.6 QPS
    (`ef_search=50`), and an exact brute-force scan reaches 1.000 at 6.3 QPS. So
    IVF-PQ **with** refine is both the most accurate approximate option and the
    fastest — but only with the knob set.

### Choosing `ef_search` for HNSW

`ef_search` widens the HNSW search beam. On the same SIFT-1M corpus recall
saturates early and throughput then declines, so there is a useful band rather
than "higher is better":

| `ef_search` | recall@10 | QPS |
|---:|---:|---:|
| 10 | 0.862 | 20.4 |
| 25 | 0.960 | 32.6 |
| 50 | **0.980** | 32.6 |
| 100 | 0.980 | 32.1 |
| 400 | 0.980 | 27.6 |

Recall stops improving at 50 and QPS falls monotonically after it. The
underlying default (`1.5 x k`, i.e. 15 for `k=10`) lands near 0.86 and is
usually too low for recall-sensitive work; 25–50 is the band worth setting.

Numbers from `crates/uni/benches/ann_pareto.rs`; full method and caveats in
`docs/perf/ann-2026-08-25.md`. They are one machine, one corpus, and a
sanity-check band rather than a guarantee — re-measure on your own data.

### Querying Vector Indexes

**Procedure Call:**

```cypher
CALL uni.vector.query('Paper', 'embedding', $query_vector, 10)
YIELD node, distance
RETURN node.title, distance
ORDER BY distance
```

**With Threshold:**

The sixth argument is a **minimum similarity**, on the same scale as the yielded `score` (for cosine, `(2 - distance) / 2`):

```cypher
CALL uni.vector.query('Paper', 'embedding', $query_vector, 100, NULL, 0.8)
YIELD node, score
RETURN node.title, score
```

**Hybrid (Vector + Graph):**

```cypher
CALL uni.vector.query('Paper', 'embedding', $query_vector, 10)
YIELD node as paper, distance
MATCH (paper)-[:AUTHORED_BY]->(author:Author)
RETURN paper.title, author.name, distance
```

---

## Sparse Vectors

Sparse vector indexes serve **learned-sparse** retrieval (SPLADE, BGE-M3's sparse head, and similar). A sparse vector is a high-dimensional, mostly-zero weight vector over a term space: each non-zero entry is a `(term_id, weight)` pair. The index stores an inverted layout (term id → posting list of `(vid, weight)`) and scores candidates by the **dot product** of query weights against stored document weights — higher is more similar.

A sparse column has type `SparseVector { dimensions }` (`sparse_vector(N)` in Cypher DDL). It is indexed with the `sparse` index kind:

```cypher
CREATE VECTOR INDEX doc_sparse
FOR (d:Doc) ON (d.emb)
OPTIONS { type: 'sparse', quantize: false }
```

```rust
use uni_db::{DataType, IndexType};

db.schema()
    .label("Doc")
        .property_nullable("emb", DataType::SparseVector { dimensions: 30522 })
        .index("emb", IndexType::sparse(30522))
    .apply()
    .await?;
```

### Term-Space Sizing (`dimensions`)

`dimensions` is the **term-space cardinality** — `max_term_id + 1`, i.e. the size of the index space the model emits, not the number of non-zeros per document. For vocabulary-based models this is the tokenizer vocabulary size (e.g. 30522 for BERT-style models, 250002 for BGE-M3's XLM-RoBERTa vocabulary). Any `term_id` the model can produce must fall inside `[0, dimensions)`. Individual document vectors remain sparse — typically tens to a few hundred non-zeros.

### Quantization

| `quantize` | Storage | Fidelity |
|---|---|---|
| `true` (**default**) | 8-bit quantized weights | Smaller index, near-lossless ranking for most corpora |
| `false` | Lossless `f32` weights | Exact dot products, larger postings |

`quantize` defaults to **true** (8-bit weight quantization). Set `quantize: false` for lossless `f32` weights when you need exact scores. Query sparse columns with `uni.sparse.query` (YIELD `vid, score, rerank_score`; `score` is the dot product). See the [Sparse Vector Search guide](../guides/sparse-vectors.md) and [BGE-M3 Hybrid Retrieval](../guides/bge-m3-hybrid-retrieval.md) for end-to-end usage.

---

## Scalar Indexes

Scalar indexes optimize exact match and range queries. They work directly on strings, numbers, booleans and dates, and on several richer types through hidden *key columns* (see [Property Types](#property-types)).

### Index Types

| Type | Operations | Best For |
|------|------------|----------|
| **BTree** | `=`, `<`, `>`, `<=`, `>=`, `BETWEEN` | General purpose, range queries |
| **Hash** | `=`, `IN` | High-cardinality equality lookups (Rust schema builder only; stored as a BTree) |
| **Bitmap** | `=`, `IN`, low-cardinality filters | Enum-like columns (< 1000 distinct values), boolean flags |
| **LabelList** | `array_contains_any`, `array_contains_all` | List columns with tag/category filtering |

### Creating Scalar Indexes

**BTree Index (default):**

```cypher
CREATE INDEX author_email FOR (a:Author) ON (a.email)
```

**Via procedure API (Bitmap and LabelList):**

```cypher
CALL uni.schema.createIndex('Event', 'status', {"type": "BITMAP"})
CALL uni.schema.createIndex('Doc', 'tags', {"type": "LABEL_LIST"})
```

`CREATE INDEX` always builds a BTree. `uni.schema.createIndex` accepts `BTREE`, `BITMAP` and `LABEL_LIST`. `Hash` is available only from the Rust schema builder (`ScalarType::Hash`); it has no separate on-disk form and is stored as a BTree.

### Property Types

A BTree, Hash or Bitmap index on a type Lance cannot index directly is built on hidden key columns derived from each value. You declare it as usual (`CREATE INDEX i FOR (e:Event) ON (e.ts)`); the key columns are internal and never appear in results or `labelInfo`.

| Property type | Served predicates (against constants or parameters) | Not served |
|---------------|------------------------------------------------------|------------|
| `DATETIME` | `=`, `<`, `<=`, `>`, `>=`, `IN`, pattern-map equality, compared by instant | `<>`, other types, `datetime.realtime()` |
| `TIME` | `=`, `<`, `<=`, `>`, `>=`, `IN`, pattern-map equality, compared in UTC (not wrapped at midnight) | `<>`, `localtime` |
| `BTIC` | `btic_overlaps`, `btic_contains`, `btic_during`, `btic_before`, `btic_after`, `btic_meets`, `btic_starts`, `btic_finishes`, `btic_equals`, `btic_disjoint`, `btic_adjacent`, `btic_contains_point`, `=`, `IN`, `btic_lo` / `btic_hi` vs a datetime | |
| `POINT`, `POINT3D`, `GEOPOINT` | `point.withinBBox(p, ll, ur)`, `distance(p, c) < r` / `<= r` (a 3-D point's z is not indexed) | point `=` and ordering, a constant of another CRS |
| `MAP<STRING, T>` key path, `ON (n.m.k)` | `n.m.k` `=`, `<`, `<=`, `>`, `>=`, `IN`, for `T` a string, integer or float | whole-map predicates, deeper paths |

A key containing spaces or punctuation is quoted: ``CREATE INDEX i FOR (n:Doc) ON (n.m.`content-type`)``. An expression such as `n.m.a + 1` is an expression index, not a map key.

Types no scalar index can serve are refused when declared, with the index that fits:

| Property type | Use instead |
|---------------|-------------|
| `LIST<STRING>` | `CREATE INVERTED INDEX` |
| `VECTOR`, `LIST<VECTOR>` | `CREATE VECTOR INDEX` |
| whole `JSON` column | `CREATE JSON FULLTEXT INDEX`, or a typed `MAP` and a key-path index |
| whole `MAP` | an index on `n.m.<key>` |
| other `LIST<T>`, `DURATION` (not ordered) | none; queries scan |

A `DURATION` or `JSON` BTree created by an earlier release keeps working.

**An index never changes the answer.** The predicate is still applied to every row the index reads, so a query returns the same rows with or without it. It can change whether a query *fails*: a predicate that raises on some row (for example `a.doc.n CONTAINS '7'` on a row whose `doc` is a string) raises without the index, but with it that row may never be read.

**An index name names one index.** `CREATE INDEX i …` is refused when `i` already names an index on a different label or property; drop it first or choose another name. Repeating the same declaration, or `IF NOT EXISTS`, is fine.

### Composite Indexes

Index multiple properties together:

```cypher
CREATE INDEX paper_venue_year FOR (p:Paper) ON (p.venue, p.year)
```

**Query utilization:**

```cypher
// Uses index (prefix match)
MATCH (p:Paper) WHERE p.venue = 'NeurIPS' AND p.year > 2020

// Uses index (first column only)
MATCH (p:Paper) WHERE p.venue = 'NeurIPS'

// Does NOT use index (missing prefix)
MATCH (p:Paper) WHERE p.year > 2020
```

### Index Selection

Uni's query planner automatically selects indexes:

```
Query: MATCH (p:Paper) WHERE p.year > 2020 AND p.venue = 'NeurIPS'

Plan:
├── Project [p.title]
│   └── Scan [:Paper]
│         ↳ Index: paper_venue_year (venue='NeurIPS', year>2020)
│         ↳ Predicate Pushdown: venue = 'NeurIPS' AND year > 2020
```

---

## Full-Text Indexes

Full-text indexes enable keyword search within text properties.

### Creating Full-Text Indexes

```cypher
CREATE FULLTEXT INDEX paper_search
FOR (p:Paper)
ON EACH [p.title, p.abstract]
```

### Tokenizers

| Tokenizer | Description | Example |
|-----------|-------------|---------|
| `standard` | Unicode word boundaries | "Hello, World!" → ["hello", "world"] |
| `whitespace` | Split on whitespace only | "Hello, World!" → ["hello,", "world!"] |
| `ngram` | Character n-grams (`ngram_min`/`ngram_max`, default 3) | "cats" → ["cat", "ats"] (trigrams) |
| `keyword` | No tokenization | "Hello World" → ["hello world"] |

The default is `standard`. Choose another tokenizer and analyzer through `OPTIONS`: `tokenizer` (or `analyzer`), `language`, `stemmer` (bool), `stopwords` (bool or list of words), `lower_case`, `ascii_folding`, `max_token_length`, `ngram_min`, `ngram_max`, and `with_positions` (bool, default `true`: store term positions, which phrase queries need; an `ngram` tokenizer never stores them). Any other key is refused with an error naming it; `normalize` and `max_terms_per_doc` belong to `CREATE INVERTED INDEX`.

```cypher
CREATE FULLTEXT INDEX paper_title_ngram
FOR (p:Paper)
ON EACH [p.title]
OPTIONS { tokenizer: 'ngram', ngram_min: 3, ngram_max: 4 }
```

### Querying Full-Text Indexes

```cypher
MATCH (p:Paper)
WHERE p.title CONTAINS 'transformer' OR p.abstract CONTAINS 'attention'
RETURN p.title
LIMIT 10
```

**BM25-ranked search:**

```cypher
CALL uni.fts.query('Paper', 'abstract', 'transformer attention', 10)
YIELD node, score
RETURN node.title, score
ORDER BY score DESC
```

### Full-Text Query Syntax

A plain list of words matches documents that contain **any** of them (OR) and ranks them by BM25. The query may also use:

| Syntax | Meaning |
|--------|---------|
| `graph database` | Either word (OR); adjacent terms are OR'd |
| `graph AND database` | Both words |
| `graph OR database` | Either word |
| `"graph database"` | The phrase: the words adjacent and in order |
| `-legacy`, `NOT legacy` | Exclude documents with the word |
| `(graph OR vector) AND search` | Grouping |

`AND` binds tighter than `OR` and adjacency, so `a b AND c` is `a OR (b AND c)`. The operators are recognised only in upper case; `and`, `or` and `not` are words. A `-` negates only at the start of a word, phrase or group (`e-mail` is one term). A negated term excludes documents but does not score.

The same syntax applies to unflushed rows, rows flushed since the index was built, indexed rows and undeclared (schemaless) properties, with the same answer before and after a flush.

Edge cases:

- A blank query matches nothing.
- A query, or a parenthesised group, whose every term is negated (`-legacy`) is an error: it has nothing to rank.
- An unbalanced quote or parenthesis, or `AND` / `OR` / `NOT` without an operand, is an error.
- A phrase on an index built without positions (an `ngram` index, or `with_positions: false`) is an error naming the index's missing positions.

```cypher
CALL uni.fts.query('Paper', 'abstract', '"neural network" AND (vision OR speech) -survey', 10)
YIELD node, score
RETURN node.title, score
```

---

## JSON Full-Text Indexes

JSON Full-Text indexes enable BM25-based full-text search on JSON document columns, leveraging Lance's native inverted index.

### When to Use JSON FTS

| Use Case | Index Type |
|----------|------------|
| Search within JSON documents | JSON Full-Text Index |
| Keyword search in text fields | JSON Full-Text Index |
| Exact JSON path matching | JsonPath Index |
| Equality filters on scalar fields | Scalar Index |

### Creating JSON Full-Text Indexes

**Via Cypher:**

```cypher
CREATE JSON FULLTEXT INDEX article_fts
FOR (a:Article) ON doc
```

**With Options:**

```cypher
CREATE JSON FULLTEXT INDEX article_fts
FOR (a:Article) ON doc
OPTIONS { with_positions: true }
```

The `with_positions` option stores term positions in the index, which phrase queries need. `paths` restricts the index to the string values under the listed key paths:

```cypher
CREATE JSON FULLTEXT INDEX article_title_fts
FOR (a:Article) ON doc
OPTIONS { paths: ['title', 'meta.author'] }
```

The indexed column is a declared `JSON` property (`doc` here); names starting with `_` are reserved.

**If Not Exists:**

```cypher
CREATE JSON FULLTEXT INDEX article_fts IF NOT EXISTS
FOR (a:Article) ON doc
```

### Querying with CONTAINS

Use `CONTAINS` on a JSON path to match string values inside the document:

```cypher
// Path-specific search (searches within a JSON path)
MATCH (a:Article)
WHERE a.doc.title CONTAINS 'Graph'
RETURN a.title

// Combined with exact matching
MATCH (a:Article)
WHERE a.doc.title CONTAINS 'Graph' AND a.status = 'published'
RETURN a.title
```

`CONTAINS` on a JSON path is a case-sensitive substring match on the value at that path. With a JSON full-text index on the column, the scan first narrows to the rows whose indexed text contains the term (flushed rows from the index's text column, unflushed rows from their documents), and the predicate is then applied to those rows, so the answer is the same as without the index. One difference: without the index, a path into a document that is not a map (`a.doc.title` where `doc` is a string) is a type error, while with the index such a row is not among the candidates, so it is skipped instead of raising the error. `EXPLAIN` reports the index as `JSON_FTS`. A path outside the index's `paths` is evaluated without the prefilter.

`uni.fts.query` on a `JSON` column searches the index's text, with the full query syntax:

```cypher
CALL uni.fts.query('Article', 'doc', 'graph -vector', 10)
YIELD node, score
RETURN node.title, score
```

### Query Routing Priority

The query planner routes predicates to the most efficient index:

```
1. _uid = 'xxx'                    → UidIndex (O(1) lookup)
2. path = 'exact'                  → JsonPathIndex (exact match)
3. doc[.path] CONTAINS 'text'      → JSON full-text candidates, then the predicate
4. ANY(x IN n.p WHERE x IN [...])  → Inverted-index candidates, then the predicate
5. Pushable predicates             → Lance scan filter
6. Else                            → Residual (post-load filter)
```

### JSON FTS Configuration

| Parameter | Default | Description |
|-----------|---------|-------------|
| `with_positions` | false | Store term positions (needed for phrase queries) |
| `paths` | all | List of dotted key paths (`'title'`, `'meta.author'`) whose string values are indexed |

Any other key is refused.

### How It Works

A JSON full-text index keeps a derived text column next to the JSON column: each row's string values (map values and list elements, recursively; keys and numbers are not text), one per line, restricted to `paths` when given. Every flush fills it, and creating the index fills it for rows already flushed. The index is a BM25 full-text index on that column, with the standard tokenizer.

```
Document: { "title": "Graph Databases", "tags": ["storage"], "year": 2024 }
         ↓
Text:     "storage\nGraph Databases"
         ↓
Query:    uni.fts.query('Article', 'doc', 'graph', 10) → matches
```

---

## Inverted Indexes

Inverted indexes store a term → vertex mapping for `List<String>` properties, for tag-based access control and multi-value attribute queries.

### Use Cases

| Use Case | Query Pattern |
|----------|---------------|
| **Tag filtering** | `ANY(tag IN d.tags WHERE tag IN $allowed)` |
| **Security labels** | Filter by granted access tags |
| **Categories** | Documents in multiple categories |
| **Skills matching** | Users with any required skill |

### Creating Inverted Indexes

**Via procedure:**

```cypher
CALL uni.schema.createIndex('Document', 'tags', {type: 'INVERTED'})
```

The procedure uses the default configuration below.

**Via Cypher DDL:**

```cypher
CREATE INVERTED INDEX document_tags FOR (d:Document) ON (d.tags)
OPTIONS { normalize: true, max_terms_per_doc: 10000 }
```

`IF NOT EXISTS` may follow the index name. `OPTIONS` takes `normalize` and `max_terms_per_doc`; any other key is refused.

**Via Rust API:**

```rust
use uni_db::core::schema::InvertedIndexConfig;

db.schema()
    .label("Document")
        .property("tags", DataType::List(Box::new(DataType::String)))
        .index("tags", IndexType::Inverted(InvertedIndexConfig {
            name: "document_tags".to_string(),
            label: "Document".to_string(),
            property: "tags".to_string(),
            normalize: true,
            max_terms_per_doc: 10_000,
            metadata: Default::default(),
        }))
    .apply()
    .await?;
```

### Inverted Index Configuration

| Parameter | Default | Description |
|-----------|---------|-------------|
| `normalize` | `true` | Lowercase and trim whitespace on terms |
| `max_terms_per_doc` | `10_000` | Distinct terms recorded per document (DoS protection); a document with more is still found by every term |

### Query Patterns

**ANY IN pattern:**

```cypher
// Finds documents with ANY of the specified tags
MATCH (d:Document)
WHERE ANY(tag IN d.tags WHERE tag IN ['public', 'team:eng'])
RETURN d.title
```

**With session variables (multi-tenant):**

```cypher
// Security filtering with session-based permissions
MATCH (d:Document)
WHERE d.tenant_id = $session.tenant_id
  AND ANY(tag IN d.tags WHERE tag IN $session.granted_tags)
RETURN d
```

### How Inverted Indexes Work

```
Document 1: tags = ['rust', 'database']
Document 2: tags = ['python', 'ml']
Document 3: tags = ['rust', 'ml']
         ↓
Inverted Index (term → VID list):
  'rust'     → [vid_1, vid_3]
  'database' → [vid_1]
  'python'   → [vid_2]
  'ml'       → [vid_2, vid_3]
         ↓
Query: ANY(tag IN d.tags WHERE tag IN ['rust', 'python'])
Result: Union of 'rust' and 'python' → [vid_1, vid_2, vid_3]
```

### Query Planner Integration

A predicate `ANY(x IN n.p WHERE x IN <list>)`, where the list is a literal of strings or a parameter, on a property with an `Online` inverted index is served as a candidate prefilter: the scan reads only the vertices the index lists for any of the terms, plus the unflushed vertices whose list holds one, and the predicate is then applied to them, so the answer is the same as without the index. `EXPLAIN` reports the index as `INVERTED`, and `PROFILE` counts the lookup in the scan's `index_hits`.

The prefilter is not used inside a transaction, on a pinned snapshot or on a fork (the postings follow primary's newest state), or when a term is not a string; those reads scan the label.

---

## Index Management

### List Indexes

```cypher
SHOW INDEXES
```

For more detail:

```cypher
CALL uni.schema.indexes()
```

### Drop Indexes

```cypher
DROP INDEX paper_year
```

### Rebuild Indexes

```rust
// Rust API: rebuild every index on a label (`true` runs it in the background)
db.indexes().rebuild("Paper", false).await?;
```

```python
# Python
db.indexes().rebuild("Paper", background=False)
```

---

## Index Lifecycle Management

Uni tracks the lifecycle of each index via an `IndexStatus` state machine. This ensures queries only use up-to-date indexes, while stale or rebuilding indexes transparently fall back to full scans.

### Index States

| Status | Description | Used by Query Planner? |
|--------|-------------|------------------------|
| **Online** | Index is up-to-date and queryable | Yes |
| **Building** | Rebuild is in progress | No (falls back to scan) |
| **Stale** | Outdated, scheduled for rebuild | No (falls back to scan) |
| **Failed** | Rebuild failed after retries exhausted | No (falls back to scan) |

**Status gating:** The query planner only uses `Online` indexes. When an index is in any other state, queries transparently fall back to a full scan — no errors, no user intervention required.

### State Transitions

```
Online ──(data changes exceed trigger)──► Stale
Stale  ──(rebuild starts)──────────────► Building
Building ──(success)───────────────────► Online
Building ──(failure, retries left)─────► Stale (retry after delay)
Building ──(failure, retries exhausted)► Failed
```

### Automatic Rebuild Triggers

When `auto_rebuild_enabled: true`, the background worker checks indexes after each flush and marks them `Stale` when either trigger fires:

| Trigger | Condition | Default |
|---------|-----------|---------|
| **Growth** | `current_rows > row_count_at_build × (1 + growth_trigger_ratio)` | 50% growth (`0.5`) |
| **Age** | `time_since_last_build > max_index_age` | Disabled (`None`) |

Set `growth_trigger_ratio: 0.0` to disable the growth trigger. Set `max_index_age: Some(Duration::from_secs(3600))` to enable time-based rebuilds.

### Configuration

Index lifecycle is configured via `IndexRebuildConfig`:

```rust
use std::time::Duration;
use uni_db::UniConfig;

let mut config = UniConfig::default();
config.index_rebuild.auto_rebuild_enabled = true;   // Enable automatic rebuilds (default: false)
config.index_rebuild.growth_trigger_ratio = 0.5;    // Rebuild after 50% row growth (default: 0.5)
config.index_rebuild.max_index_age = None;          // Time-based trigger (default: None/disabled)
config.index_rebuild.max_retries = 3;               // Retry failed rebuilds (default: 3)
config.index_rebuild.retry_delay = Duration::from_secs(60); // Delay between retries (default: 60s)
```

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `auto_rebuild_enabled` | `bool` | `false` | Enable automatic index rebuilds |
| `growth_trigger_ratio` | `f64` | `0.5` | Row growth ratio to trigger rebuild (0.0 disables) |
| `max_index_age` | `Option<Duration>` | `None` | Max time since last build before triggering rebuild |
| `max_retries` | `u32` | `3` | Maximum rebuild attempts before marking `Failed` |
| `retry_delay` | `Duration` | `60s` | Delay between retry attempts |

---

## Index Storage

Vector, scalar and full-text indexes are Lance-native indexes inside the label's dataset. Inverted and sparse indexes, and the UID index, are separate datasets under `indexes/`:

```
storage/
├── vertices_Paper.lance/
│   ├── data/
│   ├── _indices/                    # Lance native indexes
│   │   ├── <uuid>/                  # Vector index
│   │   │   ├── index.idx
│   │   │   └── auxiliary.idx
│   │   ├── <uuid>/                  # Scalar (BTree) index
│   │   └── <uuid>/                  # Full-text index
│   └── _versions/
└── indexes/
    ├── Paper/
    │   ├── tags_inverted/           # Inverted index (List<String>)
    │   └── emb_sparse/              # Sparse vector index
    └── uni_id_to_vid/
        └── Paper/index.lance        # UID → VID index
```

---

## Predicate Pushdown

Indexes integrate with Uni's predicate pushdown optimization:

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                        PREDICATE PUSHDOWN FLOW                              │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│   Query: MATCH (p:Paper) WHERE p.year > 2020 AND p.title CONTAINS 'AI'     │
│                                                                             │
│   1. Predicate Analysis                                                     │
│      ├── p.year > 2020      → Pushable (scalar index or Lance filter)      │
│      └── p.title CONTAINS   → Residual (post-load filter)                  │
│                                                                             │
│   2. Index Selection                                                        │
│      └── paper_year index available? Yes → Use index scan                  │
│                                                                             │
│   3. Execution                                                              │
│      ├── Index Scan: year > 2020 → VIDs [v1, v2, v3, ...]                  │
│      ├── Load Properties: title for filtered VIDs                          │
│      └── Residual Filter: title CONTAINS 'AI'                              │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

### Pushable Predicates

| Predicate | Index Type | Pushed? |
|-----------|------------|---------|
| `p.x = 5` | BTree | Yes |
| `p.x > 5` | BTree | Yes |
| `p.x IN [1,2,3]` | BTree | Yes |
| `p.x IS NULL` | BTree | Yes |
| `p.x CONTAINS 'foo'` | None | No (residual, if not FTS-indexed) |
| `p.x STARTS WITH 'foo'` | BTree | Partial |
| `func(p.x) = 5` | None | No (residual) |

---

## Best Practices

### When to Create Indexes

```
✓ CREATE INDEX when:
  • Property appears in WHERE clauses frequently
  • Property is used for JOIN conditions
  • Property is used in ORDER BY
  • Range queries on numeric/date properties (BTree)

✗ AVOID INDEX when:
  • Property rarely queried
  • Very small dataset (<1000 rows)
  • Property updated frequently
  • Very low selectivity (e.g., boolean with 50/50 split)
```

### Index Sizing

| Index Type | Memory Formula | Example (1M vectors, 768d) |
|------------|----------------|---------------------------|
| HNSW | ~1.5x vectors × (4 + m×8) bytes | ~120 MB |
| IVF_PQ | vectors × (d/sub_vectors) bytes | ~24 MB |
| BTree | ~40 bytes per key | ~40 MB |

### Index Maintenance

```cypher
SHOW INDEXES
```

For rebuilds, use `db.indexes().rebuild("Label", background)` (Rust and Python) or drop/recreate the index in Cypher.

---

## Performance Comparison

| Query Type | Without Index | With Index | Speedup |
|------------|---------------|------------|---------|
| Point lookup | O(n) scan | O(log n) BTree | 1000x+ |
| Range query | O(n) scan | O(log n + k) | 100x+ |
| Vector KNN | O(n×d) brute | O(log n) HNSW | 1000x+ |
| Full-text | O(n×len) scan | O(log n) inverted | 100x+ |

---

## Next Steps

- [Vector Search Guide](../guides/vector-search.md) — Deep dive into similarity search
- [Performance Tuning](../guides/performance-tuning.md) — Optimization strategies
- [Query Planning](../internals/query-planning.md) — How indexes are selected
