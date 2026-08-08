# Cypher Reference (Uni DB)

Condensed syntax reference for the OpenCypher dialect supported by Uni DB.

---

## 1. Clause Reference

### MATCH

<!-- doctest: skip -->
```cypher
MATCH (n:Label {prop: value})-[r:TYPE]->(m:Label)
WHERE <predicate>
RETURN <expr>
```

Finds subgraphs matching a pattern. Inline property filters `{prop: val}` are equivalent to WHERE.

```cypher
MATCH (a:Person)-[:KNOWS]->(b:Person)-[:WORKS_AT]->(c:Company)
RETURN a.name, c.name
```

### OPTIONAL MATCH

Left-outer-join semantics -- unmatched variables become `null` instead of eliminating the row.

```cypher
MATCH (p:Person)
OPTIONAL MATCH (p)-[:KNOWS]->(friend:Person)
RETURN p.name, friend.name
```

### CREATE

```cypher
CREATE (n:Label {prop1: val1, prop2: val2})
CREATE (a)-[:TYPE {prop: val}]->(b)
```

Each expression creates a NEW node/edge. Use variable binding to reference the same node across patterns.

```cypher
MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'})
CREATE (a)-[:KNOWS {since: 2023}]->(b)
```

### MERGE

Upsert -- create if not exists, update if exists.

```cypher
MERGE (n:Label {key_prop: value})
ON CREATE SET n.created = datetime()
ON MATCH SET n.last_seen = datetime()
RETURN n
```

### WITH

Intermediate result projection -- acts as a pipeline stage. Non-aggregated columns become implicit GROUP BY keys.

```cypher
MATCH (p:Paper)-[:AUTHORED_BY]->(a:Author)
WITH a, COUNT(p) AS paper_count
WHERE paper_count > 5
RETURN a.name, paper_count
```

### WITH RECURSIVE

<!-- doctest: skip -->
```cypher
WITH RECURSIVE reachable(vid, depth) AS (
    MATCH (n:Person {name: 'Alice'}) RETURN id(n) AS vid, 0 AS depth
    UNION ALL
    MATCH (a)-[:KNOWS]->(b)
    WHERE id(a) IN reachable.vid AND reachable.depth < 5
    RETURN id(b) AS vid, reachable.depth + 1 AS depth
)
RETURN DISTINCT vid, min(depth) AS min_depth
```

### RETURN

```cypher
RETURN n.name, n.age                    // Columns
RETURN n.name AS person_name            // Aliases
RETURN DISTINCT n.city                  // Deduplicate
RETURN n ORDER BY n.age DESC            // Ordering
RETURN n SKIP 20 LIMIT 10              // Pagination
RETURN count(*) AS total                // Aggregation
```

### WHERE

<!-- doctest: skip -->
```cypher
WHERE n.age > 25 AND n.active = true
WHERE n.name STARTS WITH 'A'           // also: CONTAINS, ENDS WITH
WHERE n.name =~ '.*lice.*'             // Regex (Rust regex engine)
WHERE n.name IN ['Alice', 'Bob']
WHERE n.email IS NOT NULL
WHERE EXISTS { MATCH (n)-[:KNOWS]->() }
```

### UNWIND

Expand a list into rows.

```cypher
UNWIND $names AS name
MATCH (n:Person {name: name}) RETURN n
```

### DELETE / DETACH DELETE

```cypher
MATCH (n:Person {name: 'Alice'}) DELETE n              // Must have no edges
MATCH (n:Person {name: 'Alice'}) DETACH DELETE n       // Removes all edges first
```

### SET / REMOVE

```cypher
SET n.age = 31, n.updated = datetime()   // Set properties
SET n = {name: 'Alice', age: 30}          // Replace ALL properties
SET n += {city: 'NY', verified: true}     // Merge properties (upsert keys)
SET n:Employee                             // Add label
REMOVE n.temporary_field                   // Remove property (sets to null)
REMOVE n:Employee                          // Remove label
```

### CALL ... YIELD

```cypher
CALL uni.vector.query('Document', 'embedding', $query_vector, 10)
YIELD node, score
RETURN node.title, score
```

### UNION / UNION ALL

```cypher
MATCH (n:Person) RETURN n.name AS name
UNION                                   // Removes duplicates
MATCH (n:Company) RETURN n.name AS name

MATCH (n:Person) RETURN n.name
UNION ALL                               // Keeps duplicates
MATCH (n:Company) RETURN n.name
```

---

## 2. Pattern Syntax

### Node Patterns

<!-- doctest: skip -->
```cypher
(n)                    // Any node, bound to variable n
(n:Person)             // Node with label
(n:Person:Employee)    // Multiple labels
(n {name: 'Alice'})    // Property filter
()                     // Anonymous node
```

### Edge Patterns

<!-- doctest: skip -->
```cypher
-[r]->                 // Outgoing
<-[r]-                 // Incoming
-[r]-                  // Undirected
-[r:KNOWS]->           // Typed
-[r:KNOWS|FRIEND_OF]-> -- Multiple types (OR)
-[r:KNOWS {since: 2020}]-> -- With properties
```

### Variable-Length Paths

| Syntax | Meaning |
|---|---|
| `[*1..3]` | 1 to 3 hops |
| `[*2]` | Exactly 2 hops |
| `[*]` or `[*1..]` | 1 to infinity (CAUTION: unbounded) |
| `[*..5]` | 1 to 5 hops |
| `[*3..]` | At least 3 hops |
| `[*0..]` | Zero or more (source may equal target) |

### Path Patterns

```cypher
// Named path
MATCH p = (a)-[:KNOWS*]->(b)
RETURN nodes(p), relationships(p), length(p)

// Shortest path
MATCH p = shortestPath((a:Person)-[:KNOWS*]-(b:Person))
WHERE a.name = 'Alice' AND b.name = 'Bob'
RETURN p

// All shortest paths
MATCH p = allShortestPaths((a:Person)-[:KNOWS*]-(b:Person))
RETURN p
```

### Comprehensions & Projections

```cypher
// Pattern comprehension
RETURN [(a)-[:KNOWS]->(b) WHERE b.age > 25 | b.name] AS friends_over_25
// List comprehension
RETURN [x IN range(1, 10) WHERE x % 2 = 0 | x * x] AS even_squares
// Map projection
RETURN n{.name, .age, city: n.address.city} AS person_data
```

---

## 3. Operators & Expressions

| Category | Operators |
|---|---|
| **Arithmetic** | `+`, `-`, `*`, `/`, `%`, `^` |
| **Comparison** | `=`, `<>`, `!=`, `<`, `<=`, `>`, `>=` |
| **Logical** | `AND`, `OR`, `XOR`, `NOT` |
| **String** | `CONTAINS`, `STARTS WITH`, `ENDS WITH`, `=~` (regex) |
| **List** | `IN`, `NOT IN` |
| **Null** | `IS NULL`, `IS NOT NULL` |

### CASE Expression

<!-- doctest: skip -->
```cypher
// Simple form
CASE n.status WHEN 'active' THEN 'Active' WHEN 'inactive' THEN 'Inactive' ELSE 'Unknown' END

// Searched form
CASE WHEN n.age < 18 THEN 'Minor' WHEN n.age < 65 THEN 'Adult' ELSE 'Senior' END
```

### Quantifiers

`ALL(x IN list WHERE pred)`, `ANY(...)`, `SINGLE(...)`, `NONE(...)`

### REDUCE

<!-- doctest: skip -->
```cypher
REDUCE(total = 0, x IN n.scores | total + x) AS sum
```

### Parameters

`$name` syntax prevents injection and enables plan caching. Session variables: `$session.tenant_id` (set via `db.session().set("tenant_id", "val").build()`).

---

## 4. Built-in Functions

### Aggregation Functions

| Function | Description |
|---|---|
| `count(expr)` / `count(*)` | Count rows; supports `count(DISTINCT x)` |
| `sum(expr)` | Sum numeric values |
| `avg(expr)` | Average |
| `min(expr)` / `max(expr)` | Minimum / Maximum |
| `collect(expr)` | Collect into list; supports `collect(DISTINCT x)` |
| `percentileDisc(expr, p)` | Discrete percentile |
| `percentileCont(expr, p)` | Continuous percentile |

Non-aggregated columns in RETURN become implicit GROUP BY keys.

### Graph Introspection

| Function | Returns | Description |
|---|---|---|
| `id(node_or_rel)` | UInt64 | Internal VID or EID |
| `created_at(node_or_rel)` | DateTime (UTC, ns) | First-insert timestamp. Read-only, system-managed; never changes after creation. |
| `updated_at(node_or_rel)` | DateTime (UTC, ns) | Most-recent write-touch timestamp. Bumps on any CREATE/SET/MERGE that targets the row, including same-value writes. |
| `type(rel)` | String | Edge type name |
| `labels(node)` | List | Node labels |
| `keys(map)` | List | Map keys / property names |
| `properties(node)` | Map | All properties as a map |
| `nodes(path)` | List | Vertices in a path |
| `relationships(path)` | List | Edges in a path |
| `startNode(rel)` | Node | Source vertex |
| `endNode(rel)` | Node | Destination vertex |
| `length(path)` | Integer | Number of relationships in path |

### String Functions

| Function | Description |
|---|---|
| `toString(x)` | Convert to string |
| `toLower(s)` / `lower(s)` | Lowercase |
| `toUpper(s)` / `upper(s)` | Uppercase |
| `trim(s)` / `ltrim(s)` / `rtrim(s)` | Trim whitespace |
| `left(s, n)` / `right(s, n)` | First/last n characters |
| `substring(s, start, [len])` | Substring |
| `replace(s, from, to)` | Replace occurrences |
| `split(s, delim)` | Split into list |
| `lpad(s, len, [pad])` / `rpad(s, len, [pad])` | Pad to length |
| `reverse(s)` | Reverse string |
| `size(s)` | String length |

### Math Functions

| Function | Description |
|---|---|
| `abs(x)`, `ceil(x)`, `floor(x)`, `round(x)` | Rounding / absolute |
| `sqrt(x)`, `exp(x)`, `log(x)`, `log10(x)` | Roots and logarithms |
| `pow(x, y)` / `power(x, y)` | Exponentiation |
| `sign(x)` | -1, 0, or 1 |
| `rand()` | Random float in [0, 1) |
| `sin`, `cos`, `tan`, `asin`, `acos`, `atan`, `atan2` | Trigonometric |

### Collection Functions

| Function | Description |
|---|---|
| `size(list)` | List length |
| `head(list)` / `last(list)` | First / last element |
| `tail(list)` | All except first |
| `reverse(list)` | Reverse order |
| `range(start, end, [step])` | Integer range |
| `coalesce(a, b, ...)` | First non-null value |
| `nullif(a, b)` | Return null if a = b |

### Type Conversion

| Function | Description |
|---|---|
| `toInteger(x)` | Convert to integer |
| `toFloat(x)` | Convert to float |
| `toBoolean(x)` | Convert to boolean |
| `toString(x)` | Convert to string |

### Temporal Functions

**Constructors:** `date()`, `time()`, `localtime()`, `datetime()`, `localdatetime()`, `duration()`, `btic()`

**Clock functions:** `datetime.transaction()`, `datetime.statement()`, `datetime.realtime()`

**Epoch:** `datetime.fromepoch(seconds)`, `datetime.fromepochmillis(millis)`

**Duration:** `duration.between(start, end)`, `duration.inMonths()`, `duration.inDays()`, `duration.inSeconds()`

**Temporal property accessors:** `.year`, `.month`, `.day`, `.hour`, `.minute`, `.second`, `.timezone`

### Temporal Validity

<!-- doctest: skip -->
```cypher
WHERE uni.temporal.validAt(e, 'valid_from', 'valid_to', datetime($time))  // half-open interval
WHERE e VALID_AT datetime('2024-06-15T12:00:00Z')                          // macro shorthand
WHERE c VALID_AT(datetime($t), 'start_date', 'end_date')                   // custom props
```

### BTIC Temporal Intervals

BTIC encodes half-open time intervals `[lo, hi)` as a single 24-byte property with per-bound granularity and certainty.

**Literal formats:** `btic('1985')` (year), `btic('1985-03')` (month), `btic('1939/1945')` (range), `btic('~1985')` (approximate), `btic('2020-03/')` (ongoing), `btic('/')` (unbounded).

**Accessors:** `btic_lo(b)`, `btic_hi(b)` (DateTime), `btic_duration(b)` (Int64 ms), `btic_granularity(b)`, `btic_certainty(b)` (String), `btic_is_finite(b)`, `btic_is_unbounded(b)`, `btic_is_instant(b)` (Boolean).

**Predicates (2-arg → Boolean):** `btic_contains_point(b, point)`, `btic_overlaps(a, b)`, `btic_contains(a, b)`, `btic_before(a, b)`, `btic_after(a, b)`, `btic_meets(a, b)`, `btic_adjacent(a, b)`, `btic_disjoint(a, b)`, `btic_equals(a, b)`, `btic_starts(a, b)`, `btic_during(a, b)`, `btic_finishes(a, b)`.

**Set operations:** `btic_intersection(a, b)`, `btic_span(a, b)`, `btic_gap(a, b)` — return Btic or NULL.

**Aggregation:** `btic_min(col)`, `btic_max(col)`, `btic_span_agg(col)`, `btic_count_at(col, point)`.

**Comparison operators:** `<`, `>`, `<=`, `>=`, `=`, `<>` work on BTIC values (lexicographic on `lo`, `hi`, `meta`).

```cypher
// Store and query fuzzy historical dates
CREATE (e:Event {name: 'WW2', period: btic('1939/1945')})
MATCH (e:Event) WHERE btic_overlaps(e.period, btic('1940')) RETURN e.name
MATCH (e:Event) WHERE e.period < btic('1950') RETURN e.name             // comparison operators
MATCH (e:Event) RETURN btic_span_agg(e.period) AS total                 // aggregation
```

**When to use BTIC vs `uni.temporal.validAt`:** Use `validAt` for exact date ranges stored as two columns (`start_date`/`end_date`). Use BTIC for single-column fuzzy intervals with granularity metadata (historical dates, uncertain periods).

### Similarity Functions

| Function | Description |
|---|---|
| `similar_to(source, query [, options])` | Unified similarity: vector cosine, auto-embed, BM25 FTS, or multi-source fusion |
| `vector_similarity(v1, v2)` | Cosine similarity between two vectors |
| `vector_distance(v1, v2 [, metric])` | Distance between two vectors |

```cypher
MATCH (d:Doc) RETURN d.title, similar_to(d.embedding, 'graph databases') AS score ORDER BY score DESC
MATCH (d:Doc) RETURN d.title, similar_to([d.embedding, d.content], [$vec, 'term'], {method: 'weighted', weights: [0.7, 0.3]}) AS score
```

Options: `method` (`'rrf'`|`'weighted'`), `weights` (list), `k` (RRF constant, default 60), `fts_k` (BM25 saturation, default 1.0).

### Bitwise Functions

`bitwise_and`, `bitwise_or`, `bitwise_xor`, `bitwise_not`, `shift_left`, `shift_right`

---

## 5. Window Functions

```cypher
function(args) OVER ([PARTITION BY expr, ...] [ORDER BY expr [ASC|DESC], ...])
```

| Function | Description |
|---|---|
| `ROW_NUMBER()` | Sequential row number |
| `RANK()` | Rank with gaps for ties |
| `DENSE_RANK()` | Rank without gaps |
| `LAG(expr [, offset])` | Previous row value |
| `LEAD(expr [, offset])` | Next row value |
| `FIRST_VALUE(expr)` | First value in window |
| `LAST_VALUE(expr)` | Last value in window |
| `NTH_VALUE(expr, n)` | Nth value in window |
| `NTILE(n)` | Distribute into n buckets |
| `SUM(expr) OVER (...)` | Aggregate window |
| `AVG(expr) OVER (...)` | Aggregate window |
| `COUNT(expr) OVER (...)` | Aggregate window |
| `MIN(expr) OVER (...)` | Aggregate window |
| `MAX(expr) OVER (...)` | Aggregate window |

```cypher
MATCH (n:Employee)
RETURN n.name, n.salary, n.department,
    ROW_NUMBER() OVER (PARTITION BY n.department ORDER BY n.salary DESC) AS rank,
    SUM(n.salary) OVER (PARTITION BY n.department) AS dept_total
```

**Gotcha:** Queries that mix manual window functions (e.g. `ROW_NUMBER`) and aggregate window functions (e.g. `SUM OVER`) in the same RETURN clause are not yet supported.

---

## 6. DDL Commands

### Labels

```cypher
CREATE LABEL Person ( name STRING, age INTEGER, email STRING UNIQUE )
ALTER LABEL Person ADD PROPERTY phone STRING
ALTER LABEL Person DROP PROPERTY age
ALTER LABEL Person RENAME PROPERTY name TO full_name
DROP LABEL IF EXISTS Person
```

### Edge Types

```cypher
CREATE EDGE TYPE KNOWS (weight FLOAT) FROM Person TO Person
ALTER EDGE TYPE KNOWS ADD PROPERTY since DATE
DROP EDGE TYPE IF EXISTS KNOWS
```

### Indexes

```cypher
CREATE INDEX idx_name FOR (p:Person) ON (p.name)
CREATE VECTOR INDEX idx_embed FOR (d:Document) ON (d.embedding) OPTIONS { metric: 'cosine' }
CREATE FULLTEXT INDEX idx_content FOR (a:Article) ON EACH [a.content]
CREATE JSON FULLTEXT INDEX idx_meta FOR (d:Data) ON metadata
DROP INDEX idx_name
```

### Constraints

```cypher
CREATE CONSTRAINT uniq_email ON (p:Person) ASSERT p.email IS UNIQUE
CREATE CONSTRAINT pk_sku ON (p:Product) ASSERT p.sku IS KEY
CREATE CONSTRAINT has_since ON ()-[r:KNOWS]->() ASSERT EXISTS(r.since)
CREATE CONSTRAINT score_chk ON (p:Person) ASSERT p.score >= 0    // CHECK
DROP CONSTRAINT constraint_name
```

The name and `IF NOT EXISTS` are optional; `ON` takes a node pattern `(v:Label)` or a relationship pattern `()-[v:TYPE]->()`. An assertion that is not `IS UNIQUE`, `IS KEY` or `EXISTS(...)` becomes a CHECK constraint.

CHECK is enforced by the bulk loader and the transactional path, which share one evaluator. It handles the shape `prop op literal` with `op` in `= == != <> > < >= <=`; comparisons coerce `Int`/`Float`, so `ASSERT p.score = 5` passes against a stored `5.0`. A missing property passes -- absence is `EXISTS`'s concern. An expression outside that shape is **allowed with a warning**, not enforced.

### SHOW Commands

`SHOW DATABASE`, `SHOW INDEXES`, `SHOW CONSTRAINTS`, `SHOW CONFIG`, `SHOW STATISTICS`

### Admin Commands

`VACUUM` (reclaim space), `CHECKPOINT` (force flush), `BACKUP '/path'`, `COPY (Label) TO/FROM '/path' FORMAT csv|parquet`

---

## 7. Built-in Procedures

### Schema Introspection

```cypher
CALL uni.schema.labels() YIELD label, propertyCount, nodeCount, indexCount
CALL uni.schema.edgeTypes() YIELD type, propertyCount, sourceLabels, targetLabels
CALL uni.schema.labelInfo('Person') YIELD property, dataType, nullable, indexed, unique
CALL uni.schema.indexes() YIELD name, type, label, state, properties
CALL uni.schema.constraints() YIELD name, type, enabled, properties, target
```

### Vector Search

<!-- doctest: skip -->
```cypher
CALL uni.vector.query(label, property, query_vector, k [, filter] [, threshold])
YIELD node, score, distance, vector_score, vid
```

`query_vector` can be a `List<Float>` or a `String` (auto-embedded when embedding config exists). Score is normalized to 0-1.

### Full-Text Search

<!-- doctest: skip -->
```cypher
CALL uni.fts.query(label, property, search_term, k [, threshold])
YIELD node, score, fts_score, vid
```

BM25-based. Scores normalized to 0-1 relative to top match.

### Hybrid Search

<!-- doctest: skip -->
```cypher
CALL uni.search(label, properties, query_text, query_vector, k [, filter] [, options])
YIELD node, score, vector_score, fts_score, vid
```

Options: `method` (`'rrf'`|`'weighted'`), `alpha` (vector vs FTS weight), `over_fetch` (default 2.0).

### Snapshots

```cypher
CALL uni.admin.snapshot.create('release-v1.0') YIELD snapshot_id
CALL uni.admin.snapshot.list() YIELD snapshot_id, name, created_at, version_hwm
CALL uni.admin.snapshot.restore($snapshot_id) YIELD status
```

### Compaction

```cypher
CALL uni.admin.compact() YIELD success, files_compacted, bytes_before, bytes_after, duration_ms
CALL uni.admin.compactionStatus() YIELD l1_runs, l1_size_bytes, in_progress, pending
```

---

## 8. Time Travel

Query historical data without restoring a snapshot.

```cypher
// By snapshot ID
MATCH (n:Person) VERSION AS OF 'snapshot-abc123'
RETURN n.name, n.age

// By timestamp
MATCH (n:Person) TIMESTAMP AS OF '2025-01-15T12:00:00Z'
RETURN n.name, n.age
```

The time-travel clause goes between the pattern and RETURN/WHERE.

---

## 9. EXPLAIN and PROFILE

### EXPLAIN

Shows the query plan without executing.

```cypher
EXPLAIN MATCH (p:Paper)-[:CITES]->(cited:Paper)
WHERE p.year > 2020
RETURN cited.title, COUNT(*) AS citation_count
ORDER BY citation_count DESC LIMIT 10
```

Output: scan operations, filter/join/aggregation steps, index usage, estimated rows, cost.

### PROFILE

Executes the query and returns per-operator runtime statistics.

```cypher
EXPLAIN MATCH (n:Person)-[:KNOWS]->(m:Person) RETURN n, m
```

Output: `total_time_ms`, `rows_scanned`, `peak_memory_bytes`, per-operator `time_ms` and `rows_produced`.

---

## 10. Best Practices & Anti-Patterns

### Best Practices

| Practice | Details |
|---|---|
| **Use `$param` parameters** | Prevents injection, enables plan caching |
| **Filter early** | Put WHERE close to MATCH for predicate pushdown |
| **Use LIMIT with ORDER BY** | Enables top-K optimization |
| **Prefer MERGE over CREATE + existence check** | Single atomic upsert |
| **Named paths** | Use `p = (a)-->(b)` when you need path functions |
| **Index WHERE-clause properties** | Avoids full table scans |
| **Index MERGE key properties** | `MERGE (n:L {key: ...})` and `UNWIND ... MERGE` take a batched fast path (no per-row planning); a scalar index on the key makes the match an index point-lookup instead of a label scan |
| **Batch upserts with `UNWIND ... MERGE`** | One statement per batch of rows beats one MERGE statement per row; the single-node MERGE fast path handles intra-batch dedup |
| **Max 2-3 labels per vertex** | Prevents data duplication across tables |

### Anti-Patterns

| Anti-Pattern | Problem | Fix |
|---|---|---|
| **Cartesian products** | Unconnected patterns multiply results | Connect patterns or use WITH |
| **Unbounded VLP `[*]`** | Exponential expansion on large graphs | Always set upper bound: `[*..5]` |
| **`collect()` without DISTINCT** | Duplicate elements in collected list | Use `collect(DISTINCT x)` |
| **`WITH *`** | Materializes everything in pipeline | Explicitly name needed variables |
| **String concatenation for filters** | Injection risk, no plan caching | Use `$param` parameters |
| **Schemaless everything** | Loses columnar benefits | Define frequent properties in schema |
| **CREATE without variable binding** | Creates duplicate nodes per expression | Bind variable: `(a:Node)...(a)-[:REL]->...` |
| **MERGE on an un-indexed key** | Per-row match degrades to a full label scan (O(N x label_size) for large on-disk labels) | Add a scalar index on the MERGE key |
| **Multi-node/edge MERGE in a hot loop** | `MERGE (a)-[:R]->(b)` uses the per-row general path (builds a plan per row) | MERGE the node single-node, then batched `CREATE`/`MATCH` for edges |
| **Over-indexing** | Write performance cost | Only index queried properties |

---

## 11. Examples

### Example 1: Multi-Hop Traversal with Aggregation

Find the top 5 authors whose papers are most cited by papers from 2023+, returning each author's total received citations.

```cypher
MATCH (recent:Paper)-[:CITES]->(cited:Paper)-[:AUTHORED_BY]->(a:Author)
WHERE recent.year >= 2023
WITH a, COUNT(DISTINCT cited) AS papers_cited, SUM(cited.citations) AS total_citations
ORDER BY total_citations DESC
LIMIT 5
RETURN a.name, papers_cited, total_citations
```

### Example 2: Top-N per Group via Window Function

Find the top 3 most-cited papers in each venue.

```cypher
MATCH (p:Paper)
WITH p, ROW_NUMBER() OVER (PARTITION BY p.venue ORDER BY p.citations DESC) AS rn
WHERE rn <= 3
RETURN p.venue, p.title, p.citations
ORDER BY p.venue, p.citations DESC
```

### Example 3: Upsert with Conditional Logic and Vector Search

Upsert a document node, then find its 10 nearest neighbors by embedding similarity, enriching results with author information.

```cypher
MERGE (d:Document {ext_id: $doc_id})
ON CREATE SET d.title = $title, d.created = datetime()
ON MATCH SET d.updated = datetime()
SET d.embedding = $embedding
```

```cypher
CALL uni.vector.query('Document', 'embedding', $query_vector, 10)
YIELD node, score
MATCH (node)-[:AUTHORED_BY]->(a:Author)
RETURN node.title, score, COLLECT(a.name) AS authors
ORDER BY score DESC
```
