# Cypher Query Language

Uni supports a substantial subset of the OpenCypher query language, optimized for vectorized execution. This guide covers all supported syntax with practical examples.

## Overview

Cypher is a declarative graph query language that uses ASCII-art patterns to describe graph structures:

```cypher
// Pattern: (startNode)-[relationship]->(endNode)
MATCH (person:Person)-[:KNOWS]->(friend:Person)
WHERE person.age > 30
RETURN friend.name
```

If you need recursive rule-based reasoning, hypothetical scenarios, or abductive remediation, see the [Locy documentation](../locy/index.md), which builds on top of Cypher.

---

## Query Structure

A complete Cypher query follows this structure:

```
[MATCH clause]      -- Pattern matching
[WHERE clause]      -- Filtering
[WITH clause]       -- Intermediate processing
[RETURN clause]     -- Output projection
[ORDER BY clause]   -- Sorting
[SKIP/LIMIT clause] -- Pagination
```

### Example Query

```cypher
MATCH (p:Paper)-[:AUTHORED_BY]->(a:Author)
WHERE p.year >= 2020 AND a.affiliation = 'MIT'
RETURN p.title, a.name, p.year
ORDER BY p.year DESC
LIMIT 10
```

---

## MATCH Clause

The `MATCH` clause specifies graph patterns to find.

### Node Patterns

```cypher
// Any node
MATCH (n)

// Node with label
MATCH (p:Paper)

// Node with multiple labels
MATCH (n:Paper:Preprint)

// Node with variable binding
MATCH (paper:Paper)
RETURN paper.title

// Anonymous node (no variable)
MATCH (:Paper)-[:CITES]->(cited)
RETURN cited.title
```

### Node Properties in Pattern

```cypher
// Filter in pattern (equivalent to WHERE)
MATCH (p:Paper {year: 2023})
RETURN p.title

// Multiple properties
MATCH (p:Paper {year: 2023, venue: 'NeurIPS'})
RETURN p.title
```

### Relationship Patterns

```cypher
// Outgoing relationship
MATCH (a)-[:KNOWS]->(b)

// Incoming relationship
MATCH (a)<-[:KNOWS]-(b)

// Either direction
MATCH (a)-[:KNOWS]-(b)

// Any relationship type
MATCH (a)-[r]->(b)

// Relationship with variable
MATCH (p:Paper)-[c:CITES]->(cited:Paper)
RETURN c  // Access edge properties
```

### Multi-Hop Patterns

```cypher
// 2-hop path
MATCH (a:Paper)-[:CITES]->(b:Paper)-[:CITES]->(c:Paper)
RETURN a.title, b.title, c.title

// Chain multiple relationships
MATCH (p:Paper)-[:AUTHORED_BY]->(a:Author)-[:AFFILIATED_WITH]->(u:University)
RETURN p.title, a.name, u.name
```

### Variable-Length Paths

Variable-length path patterns allow traversing multiple hops in a single pattern.

```cypher
// 1 to 3 hops
MATCH (a:Paper)-[:CITES*1..3]->(b:Paper)
RETURN a.title, b.title

// Exactly 2 hops
MATCH (a)-[:KNOWS*2]->(b)

// Any length (use with caution on large graphs)
MATCH (a)-[:KNOWS*]->(b)

// Zero or more hops
MATCH (a)-[:KNOWS*0..]->(b)

// With path variable
MATCH path = (a:Paper)-[:CITES*1..5]->(b:Paper)
WHERE a.title = 'Attention Is All You Need'
RETURN path, length(path) AS hops
```

The relationship variable of a variable-length pattern holds a **list** of
relationships, so list functions and comprehensions apply to it:

<!-- doctest: skip -->
```cypher
MATCH (a:Paper)-[r:CITES*1..3]->(b:Paper)
RETURN size(r) AS hops, [e IN r | e.year] AS years
```

The same is true of `shortestPath` and `allShortestPaths`:

<!-- doctest: skip -->
```cypher
MATCH p = shortestPath((a:Author {name: 'Alice'})-[r:COAUTHOR*]-(b:Author {name: 'Bob'}))
RETURN size(r) AS hops, [e IN r | e.paper] AS papers
```

### Quantified Path Patterns

A quantified path pattern repeats a whole sub-pattern, rather than a single
relationship:

<!-- doctest: skip -->
```cypher
// Alice, then two rounds of "co-authored a paper that cites another paper"
MATCH (a:Author {name: 'Alice'})((x)-[:AUTHORED]->(p:Paper)-[:CITES]->(y)){2}(b)
RETURN b.title
```

#### Endpoints

The pattern's start and end are the **outer** nodes on either side — `(a)` and
`(b)` above. Variables declared *inside* the quantifier are group variables (see
below), not the pattern's endpoints, so name the endpoints explicitly when you
need them.

#### Group variables

Every variable declared inside a quantified pattern binds a **list** with one
element per iteration of the quantifier — a *group variable*, following GQL:

<!-- doctest: skip -->
```cypher
MATCH (a:Author {name: 'Alice'})((x)-[r:AUTHORED]->(p:Paper)){3}(b)
RETURN [n IN p | n.title] AS papers, size(r) AS steps
```

Because consecutive iterations share a boundary node, the sub-pattern's first
and last node variables overlap by one element: in
`((x)-[:E]->(y)-[:L]->(z)){2}`, `z[0]` and `x[1]` are the same node.

A group variable is a list, not a node or relationship. Use it as one:

| Use | Example |
|---|---|
| Whole list | `RETURN p` |
| Length | `size(p)` |
| Per-element property | `[n IN p \| n.title]` |
| Indexing | `p[0]`, `head(p)`, `last(p)` |
| Predicate | `WHERE all(n IN p WHERE n.year > 2020)` |
| Flatten to rows | `UNWIND p AS n RETURN n.title` |

Writing `p.title` is an error, because `p` holds many nodes. The message names
the replacement: `[n IN p | n.title]` for all of them, or `last(p).title` for
the final one.

**Restrictions.** A quantified pattern is compiled against declared
relationship types, so an undeclared type is refused — either declare it, or
write the sub-pattern without inner variable names so it is planned as an
ordinary variable-length pattern. Nested quantifiers are not supported.

---

## WHERE Clause

The `WHERE` clause filters matched patterns.

### Comparison Operators

<!-- doctest: skip -->
```cypher
// Equality
WHERE p.year = 2023

// Inequality
WHERE p.year <> 2020
WHERE p.year != 2020

// Comparison
WHERE p.year > 2020
WHERE p.year >= 2020
WHERE p.year < 2025
WHERE p.year <= 2025
```

### Boolean Logic

<!-- doctest: skip -->
```cypher
// AND
WHERE p.year > 2020 AND p.venue = 'NeurIPS'

// OR
WHERE p.venue = 'NeurIPS' OR p.venue = 'ICML'

// NOT
WHERE NOT p.is_retracted

// Parentheses for grouping
WHERE (p.year > 2020 AND p.venue = 'NeurIPS') OR p.citations > 1000
```

### NULL Handling

<!-- doctest: skip -->
```cypher
// Check for NULL
WHERE p.doi IS NULL
WHERE p.doi IS NOT NULL

// COALESCE (use default if NULL)
RETURN COALESCE(p.nickname, p.name) AS display_name
```

### String Operations

<!-- doctest: skip -->
```cypher
// Starts with
WHERE p.title STARTS WITH 'Attention'

// Ends with
WHERE p.title ENDS WITH 'Networks'

// Contains (uses JSON FTS index if available)
WHERE p.title CONTAINS 'Transformer'

// Full-text search on JSON document column (requires JSON FTS index)
WHERE a._doc CONTAINS 'graph database'

// Path-specific JSON FTS search
WHERE a._doc.title CONTAINS 'graph'

// Regular expression matching
WHERE p.title =~ '.*Transform.*'

// Case-insensitive regex
WHERE p.email =~ '(?i)john@.*'

// Email pattern matching
WHERE p.email =~ '^[\\w.-]+@[\\w.-]+\\.\\w+$'
```

**Note:** The `CONTAINS` operator is automatically routed to JSON FTS indexes when the column has an FTS index, enabling BM25-based full-text search with relevance ranking.

**Regex Notes:**
- The `=~` operator uses Rust's regex engine (similar to PCRE)
- NULL operands return NULL (Cypher semantics)
- Invalid regex patterns return an error

### List Operations

<!-- doctest: skip -->
```cypher
// IN list
WHERE p.venue IN ['NeurIPS', 'ICML', 'ICLR']

// NOT IN
WHERE p.venue NOT IN ['Workshop', 'Demo']
```

### Property Existence

<!-- doctest: skip -->
```cypher
// Check if property exists
WHERE p.abstract IS NOT NULL

// In pattern
WHERE EXISTS(p.doi)
```

---

## RETURN Clause

The `RETURN` clause specifies output columns.

### Basic Projections

<!-- doctest: skip -->
```cypher
// Single property
RETURN p.title

// Multiple properties
RETURN p.title, p.year, p.venue

// All properties (*)
RETURN p.*

// Entire node
RETURN p
```

### Aliases

```cypher
// AS keyword
RETURN p.title AS paper_title, a.name AS author_name

// Expression aliases
RETURN p.year + 1 AS next_year
```

### Expressions

```cypher
// Arithmetic
RETURN p.citations * 2 AS double_citations

// String concatenation
RETURN p.title + ' (' + p.venue + ')' AS formatted

// Conditional (CASE)
RETURN CASE
  WHEN p.citations > 1000 THEN 'High Impact'
  WHEN p.citations > 100 THEN 'Medium Impact'
  ELSE 'Low Impact'
END AS impact_level
```

### DISTINCT

```cypher
// Remove duplicates
RETURN DISTINCT p.venue

// Distinct combinations
RETURN DISTINCT p.venue, p.year
```

---

## Aggregations

Uni supports standard aggregation functions.

### Aggregation Functions

| Function | Description | Example |
|----------|-------------|---------|
| `COUNT(*)` | Count all rows | `RETURN COUNT(*)` |
| `COUNT(x)` | Count non-null values | `RETURN COUNT(p.doi)` |
| `COUNT(DISTINCT x)` | Count distinct values | `RETURN COUNT(DISTINCT p.venue)` |
| `SUM(x)` | Sum numeric values | `RETURN SUM(p.citations)` |
| `AVG(x)` | Average | `RETURN AVG(p.citations)` |
| `MIN(x)` | Minimum | `RETURN MIN(p.year)` |
| `MAX(x)` | Maximum | `RETURN MAX(p.year)` |
| `COLLECT(x)` | Collect into list | `RETURN COLLECT(p.title)` |

### Implicit Grouping

Non-aggregated columns become GROUP BY keys:

```cypher
// Group by venue, count papers per venue
MATCH (p:Paper)
RETURN p.venue, COUNT(p) AS paper_count
ORDER BY paper_count DESC
```

### Aggregation Examples

```cypher
// Count papers per author
MATCH (p:Paper)-[:AUTHORED_BY]->(a:Author)
RETURN a.name, COUNT(p) AS papers, AVG(p.citations) AS avg_citations
ORDER BY papers DESC

// Most cited papers per venue
MATCH (p:Paper)
RETURN p.venue, MAX(p.citations) AS top_citations, COUNT(p) AS total
ORDER BY top_citations DESC
```

---

## Window Functions

Window functions perform calculations across a set of rows related to the current row, without collapsing results like aggregations.

### Basic Syntax

```cypher
function_name(args) OVER (
  [PARTITION BY partition_expr, ...]
  [ORDER BY order_expr [ASC|DESC], ...]
)
```

### Supported Window Functions

| Function | Description | Example |
|----------|-------------|---------|
| `row_number()` | Sequential row number | `row_number() OVER (ORDER BY p.year)` |
| `rank()` | Rank with gaps for ties | `rank() OVER (ORDER BY p.citations DESC)` |
| `dense_rank()` | Rank without gaps | `dense_rank() OVER (ORDER BY p.citations DESC)` |
| `sum()` | Running sum | `sum(p.citations) OVER (ORDER BY p.year)` |
| `avg()` | Running average | `avg(p.citations) OVER (PARTITION BY p.venue)` |
| `count()` | Running count | `count(*) OVER (PARTITION BY p.venue)` |
| `min()` / `max()` | Running min/max | `max(p.citations) OVER (PARTITION BY p.author)` |

### Examples

**Ranking within partitions:**
```cypher
MATCH (p:Paper)
RETURN p.title, p.venue, p.citations,
       rank() OVER (PARTITION BY p.venue ORDER BY p.citations DESC) AS venue_rank
ORDER BY p.venue, venue_rank
```

**Running totals:**
```cypher
MATCH (p:Paper)
WHERE p.author = 'Alice'
RETURN p.title, p.year, p.citations,
       sum(p.citations) OVER (ORDER BY p.year) AS cumulative_citations
ORDER BY p.year
```

**Top N per group:**
```cypher
MATCH (p:Paper)
WITH p, row_number() OVER (PARTITION BY p.venue ORDER BY p.citations DESC) AS rn
WHERE rn <= 3
RETURN p.venue, p.title, p.citations
ORDER BY p.venue, p.citations DESC
```

---

## Scalar Functions

Uni provides a comprehensive set of scalar functions for data transformation.

### String Functions

| Function | Description | Example |
|----------|-------------|---------|
| `toUpper(s)` / `upper(s)` | Convert to uppercase | `RETURN toUpper('hello')` → `'HELLO'` |
| `toLower(s)` / `lower(s)` | Convert to lowercase | `RETURN toLower('HELLO')` → `'hello'` |
| `trim(s)` | Remove leading/trailing whitespace | `RETURN trim('  hi  ')` → `'hi'` |
| `ltrim(s)` | Remove leading whitespace | `RETURN ltrim('  hi')` → `'hi'` |
| `rtrim(s)` | Remove trailing whitespace | `RETURN rtrim('hi  ')` → `'hi'` |
| `substring(s, start, [len])` | Extract substring | `RETURN substring('hello', 1, 3)` → `'ell'` |
| `left(s, n)` | First n characters | `RETURN left('hello', 2)` → `'he'` |
| `right(s, n)` | Last n characters | `RETURN right('hello', 2)` → `'lo'` |
| `reverse(s)` | Reverse string | `RETURN reverse('hello')` → `'olleh'` |
| `replace(s, old, new)` | Replace occurrences | `RETURN replace('hello', 'l', 'x')` → `'hexxo'` |
| `split(s, delim)` | Split into list | `RETURN split('a,b,c', ',')` → `['a','b','c']` |
| `lpad(s, len, [pad])` | Pad left to length | `RETURN lpad('5', 3, '0')` → `'005'` |
| `rpad(s, len, [pad])` | Pad right to length | `RETURN rpad('5', 3, '0')` → `'500'` |

### Math Functions

| Function | Description | Example |
|----------|-------------|---------|
| `abs(n)` | Absolute value | `RETURN abs(-5)` → `5` |
| `ceil(n)` | Round up | `RETURN ceil(4.2)` → `5` |
| `floor(n)` | Round down | `RETURN floor(4.8)` → `4` |
| `round(n)` | Round to nearest | `RETURN round(4.5)` → `5` |
| `sqrt(n)` | Square root | `RETURN sqrt(16)` → `4` |
| `sign(n)` | Sign (-1, 0, 1) | `RETURN sign(-5)` → `-1` |
| `log(n)` | Natural logarithm | `RETURN log(2.718)` → `~1` |
| `log10(n)` | Base-10 logarithm | `RETURN log10(100)` → `2` |
| `exp(n)` | e^n | `RETURN exp(1)` → `~2.718` |
| `power(base, exp)` / `pow(base, exp)` | Exponentiation | `RETURN power(2, 3)` → `8` |
| `sin(n)`, `cos(n)`, `tan(n)` | Trigonometric | `RETURN sin(0)` → `0` |

### List Functions

| Function | Description | Example |
|----------|-------------|---------|
| `size(list)` | Length of list/string/map | `RETURN size([1,2,3])` → `3` |
| `head(list)` | First element | `RETURN head([1,2,3])` → `1` |
| `tail(list)` | All but first element | `RETURN tail([1,2,3])` → `[2,3]` |
| `last(list)` | Last element | `RETURN last([1,2,3])` → `3` |
| `keys(map)` | Keys of a map | `RETURN keys({a:1, b:2})` → `['a','b']` |
| `range(start, end, [step])` | Generate number sequence | `RETURN range(1, 5)` → `[1,2,3,4,5]` |

### Path Functions

| Function | Description | Example |
|----------|-------------|---------|
| `length(path)` | Number of relationships in path | `RETURN length(path)` |
| `nodes(path)` | List of nodes in path | `RETURN nodes(path)` |
| `relationships(path)` | List of relationships in path | `RETURN relationships(path)` |

### System-Managed Timestamps

Every vertex and edge automatically carries creation and modification timestamps maintained by the storage layer. They are exposed as Cypher functions and return `DateTime` (UTC, nanosecond precision). Read-only, no schema declaration required.

| Function | Description | Example |
|----------|-------------|---------|
| `created_at(n)` / `created_at(r)` | Wall-clock time when the row was first inserted. Never changes after creation. | `WHERE created_at(n) > datetime("2026-05-01")` |
| `updated_at(n)` / `updated_at(r)` | Most-recent write-touch timestamp. Bumps on any CREATE/SET/MERGE that targets the row, including same-value writes. | `RETURN n ORDER BY updated_at(n) DESC` |

```cypher
// Find recently created people
MATCH (n:Person) WHERE created_at(n) > datetime("2026-05-01") RETURN n

// Edges work the same way
MATCH (a)-[r:KNOWS]->(b) RETURN r, created_at(r), updated_at(r)
```

The bulk loader can supply explicit per-row values; otherwise the engine assigns timestamps at insertion time.

### Type Conversion Functions

| Function | Description | Example |
|----------|-------------|---------|
| `toInteger(x)` | Convert to integer | `RETURN toInteger('42')` → `42` |
| `toFloat(x)` | Convert to float | `RETURN toFloat('3.14')` → `3.14` |
| `toString(x)` | Convert to string | `RETURN toString(42)` → `'42'` |
| `toBoolean(x)` | Convert to boolean | `RETURN toBoolean('true')` → `true` |

### Similarity Functions

| Function | Description | Example |
|----------|-------------|---------|
| `similar_to(sources, queries [, options])` | Unified similarity scoring (vector similarity, BM25, or hybrid fusion). Metric-aware: uses the index's configured distance metric (Cosine, L2, or Dot). | `RETURN similar_to(n.embedding, $vec)` |
| `vector_similarity(v1, v2)` | Cosine similarity between two vectors. Alias for `similar_to` with a single vector source. | `WHERE vector_similarity(a.embed, b.embed) > 0.8` |
| `vector_distance(v1, v2 [, metric])` | Distance between two vectors. | `RETURN vector_distance(a.embed, b.embed, 'cosine')` |

`similar_to` is an expression function — it works in `WHERE`, `RETURN`, `WITH`, `ORDER BY`, and Locy rule bodies. Unlike `CALL uni.search(...)`, it scores one already-bound node (point computation), not a top-K scan.

```cypher
// Single vector source with auto-embedding
MATCH (d:Document) WHERE similar_to(d.embedding, 'search query') > 0.6
RETURN d.title, similar_to(d.embedding, 'search query') AS score

// Multi-source hybrid (vector + FTS)
MATCH (d:Document)
RETURN d.title, similar_to([d.embedding, d.content], 'query') AS relevance
ORDER BY relevance DESC

// Weighted fusion
MATCH (d:Document)
RETURN d.title, similar_to([d.embedding, d.content], 'query',
  {method: 'weighted', weights: [0.7, 0.3]}) AS score
```

See the [Vector Search guide](vector-search.md#similar_to-expression-function) for full details.

### Null Handling Functions

| Function | Description | Example |
|----------|-------------|---------|
| `coalesce(x, y, ...)` | First non-null value | `RETURN coalesce(null, 'default')` → `'default'` |
| `nullif(a, b)` | Return null if a = b | `RETURN nullif(5, 5)` → `null` |

### Example Usage

```cypher
// String manipulation
MATCH (p:Paper)
RETURN toUpper(p.title) AS upper_title,
       substring(p.abstract, 0, 100) AS abstract_preview

// Math operations
MATCH (p:Paper)
RETURN p.title, round(p.citations / 10.0) * 10 AS rounded_citations

// List operations
MATCH (a:Author)
WHERE size(a.affiliations) > 1
RETURN a.name, head(a.affiliations) AS primary_affiliation

// Type conversion
MATCH (p:Paper)
WHERE toInteger(p.year_str) > 2020
RETURN p.title
```

---

## ORDER BY, SKIP, LIMIT

### Sorting

<!-- doctest: skip -->
```cypher
// Ascending (default)
ORDER BY p.year

// Descending
ORDER BY p.year DESC

// Multiple columns
ORDER BY p.year DESC, p.title ASC

// By alias
RETURN p.title, p.citations AS cites
ORDER BY cites DESC
```

### Pagination

<!-- doctest: skip -->
```cypher
// First 10 results
LIMIT 10

// Skip first 20, take next 10
SKIP 20
LIMIT 10

// Combined
RETURN p.title
ORDER BY p.year DESC
SKIP 100
LIMIT 25
```

---

## CREATE Clause

Create new nodes and relationships.

### Creating Nodes

```cypher
// Simple node
CREATE (p:Paper {title: 'My Paper'})

// With properties
CREATE (p:Paper {
  title: 'New Research',
  year: 2024,
  venue: 'ArXiv',
  citations: 0
})
RETURN p

// Multiple nodes
CREATE (a:Author {name: 'Alice'}), (b:Author {name: 'Bob'})
```

### Creating Relationships

```cypher
// Between existing nodes
MATCH (p:Paper {title: 'Paper A'}), (a:Author {name: 'Alice'})
CREATE (p)-[:AUTHORED_BY {position: 1}]->(a)

// Create node and relationship together
CREATE (p:Paper {title: 'New Paper'})-[:AUTHORED_BY]->(a:Author {name: 'New Author'})
```

### Returning Created Elements

```cypher
CREATE (p:Paper {title: 'My Paper', year: 2024})
RETURN p.title, p.year
```

---

## WITH Clause

The `WITH` clause chains query parts together.

### Intermediate Processing

```cypher
// Filter after aggregation
MATCH (p:Paper)-[:AUTHORED_BY]->(a:Author)
WITH a, COUNT(p) AS paper_count
WHERE paper_count > 5
RETURN a.name, paper_count
```

### Subquery-like Behavior

```cypher
// First find top authors, then their papers
MATCH (a:Author)<-[:AUTHORED_BY]-(p:Paper)
WITH a, COUNT(p) AS papers
ORDER BY papers DESC
LIMIT 10
MATCH (a)<-[:AUTHORED_BY]-(recent:Paper)
WHERE recent.year >= 2022
RETURN a.name, recent.title
```

---

## UNWIND Clause

Expand a list into individual rows.

```cypher
// Expand literal list
UNWIND [1, 2, 3] AS num
RETURN num

// Expand list property
MATCH (p:Paper)
UNWIND p.keywords AS keyword
RETURN keyword, COUNT(*) AS usage
ORDER BY usage DESC

// Create multiple from list
UNWIND ['Alice', 'Bob', 'Charlie'] AS name
CREATE (a:Author {name: name})
```

---

## CALL Clause (Procedures)

Invoke built-in procedures.

### Vector Search

```cypher
// Basic vector search
CALL uni.vector.query('Paper', 'embedding', $query_vector, 10)
YIELD node, distance
RETURN node.title, distance

// With threshold
CALL uni.vector.query('Paper', 'embedding', $query_vector, 100, NULL, 0.3)
YIELD node, distance
WHERE distance < 0.2
RETURN node.title, distance
```

### Schema Introspection

```cypher
// List labels
CALL uni.schema.labels()
YIELD label
RETURN label

// List relationship types
CALL uni.schema.relationshipTypes()
YIELD relationshipType
RETURN relationshipType

// List indexes
CALL uni.schema.indexes()
YIELD name, type, label, properties, state
RETURN *
```

---

## Index Management

### Create Indexes

```cypher
// Scalar index
CREATE INDEX paper_year_idx FOR (p:Paper) ON (p.year)

// Named index
CREATE INDEX paper_year FOR (p:Paper) ON (p.year)

// Vector index
CREATE VECTOR INDEX paper_embeddings
FOR (p:Paper) ON p.embedding
OPTIONS {type: 'hnsw'}

// JSON Full-Text index
CREATE JSON FULLTEXT INDEX article_fts
FOR (a:Article) ON _doc
OPTIONS {with_positions: true}
```

### Drop Indexes

```cypher
DROP INDEX paper_year
```

### Show Indexes

```cypher
SHOW INDEXES
```

---

## Query Parameters

Use parameters to avoid injection and improve plan caching.

### Parameter Syntax

```cypher
// In query
MATCH (p:Paper)
WHERE p.year = $year AND p.venue = $venue
RETURN p.title
```

### CLI Usage

```bash
uni query "MATCH (p:Paper) WHERE p.year = \$year RETURN p" \
    --params '{"year": 2023}' \
    --path ./storage
```

---

## Session Variables

Session variables (`$session.*`) provide scoped context for multi-tenant queries. They're set when creating a session and accessible in all queries within that session.

=== "Rust"

    ```rust
    // Create session with tenant context
    let session = db.session()
        .set("tenant_id", "acme-corp")
        .set("user_id", "user-123")
        .build();

    // All queries have access to $session.* variables
    let results = session.query(r#"
        MATCH (d:Document)
        WHERE d.tenant_id = $session.tenant_id
        RETURN d.title AS title
    "#).await?;
    ```

=== "Python"

    ```python
    # Create session with tenant context
    session = (
        db.session()
        .set("tenant_id", "acme-corp")
        .set("user_id", "user-123")
        .build()
    )

    # All queries have access to $session.* variables
    results = session.query("""
        MATCH (d:Document)
        WHERE d.tenant_id = $session.tenant_id
        RETURN d.title AS title
    """)
    ```

Session variables use the `$session.property_name` syntax and are resolved at query execution time.

---

## Temporal Queries

Uni provides functions for querying temporal (time-based) data with validity ranges.

### validAt Function

Check if a node or edge was valid at a specific point in time:

```cypher
// Basic validAt usage
MATCH (e:Event)
WHERE uni.temporal.validAt(e, 'valid_from', 'valid_to', datetime($time))
RETURN e.name, e.valid_from, e.valid_to
```

**Parameters:**
- `e`: Node or edge to check
- `'valid_from'`: Property name for start time
- `'valid_to'`: Property name for end time
- `datetime($time)`: Point in time to check

**Semantics:** `valid_from <= time < valid_to` (half-open interval)

### VALID_AT Macro

For convenience, use the `VALID_AT` macro with default property names:

```cypher
// Macro form (uses 'valid_from' and 'valid_to' by default)
MATCH (e:Event)
WHERE e VALID_AT datetime('2024-06-15T12:00:00Z')
RETURN e.name
```

### Custom Property Names

Specify custom property names if your schema differs:

```cypher
// With custom property names
MATCH (c:Contract)
WHERE c VALID_AT(datetime($check_time), 'start_date', 'end_date')
RETURN c.name, c.value
```

### Temporal Query Patterns

**Find currently valid records:**

```cypher
MATCH (e:Employee)
WHERE uni.temporal.validAt(e, 'hire_date', 'termination_date', datetime())
RETURN e.name, e.department
```

**Find records valid at a historical point:**

```cypher
MATCH (p:Price)
WHERE p.product_id = $product_id
  AND uni.temporal.validAt(p, 'effective_from', 'effective_to', datetime('2024-01-01'))
RETURN p.amount
```

**Temporal edges:**

```cypher
MATCH (e:Employee)-[r:WORKS_IN]->(d:Department)
WHERE uni.temporal.validAt(r, 'start_date', 'end_date', datetime($as_of_date))
RETURN e.name, d.name
```

**Open-ended validity (NULL end date):**

```cypher
// Records with NULL valid_to are considered currently valid
MATCH (m:Membership)
WHERE m.valid_to IS NULL
  OR m.valid_to > datetime()
RETURN m
```

### BTIC Temporal Intervals

For fuzzy historical dates, variable-precision intervals, or Allen's interval algebra, use **BTIC** — a single-column temporal interval type with per-bound granularity and certainty metadata.

```cypher
// Create an event with a year-precision interval
CREATE (e:Event {name: 'WW2', period: btic('1939/1945')})

// Query: find events overlapping with the 1940s
MATCH (e:Event) WHERE btic_overlaps(e.period, btic('1940')) RETURN e.name

// Comparison operators work on BTIC
MATCH (e:Event) WHERE e.period < btic('1950') RETURN e.name
```

BTIC includes 30+ functions: accessors (`btic_lo`, `btic_hi`, `btic_duration`), 12 Allen algebra predicates (`btic_overlaps`, `btic_before`, `btic_meets`, ...), set operations (`btic_intersection`, `btic_span`, `btic_gap`), and aggregation (`btic_min`, `btic_max`, `btic_span_agg`).

**See the full guide: [Temporal Intervals (BTIC)](temporal-intervals.md).**

---

## SET / REMOVE Clause

Update properties and labels on existing nodes and relationships.

### Setting Properties

```cypher
// Set a single property
MATCH (n:Person {name: 'Alice'})
SET n.age = 31

// Set multiple properties at once
MATCH (n:Person {name: 'Alice'})
SET n.age = 25, n.email = 'alice@example.com'

// Set a property on a relationship
MATCH (a:Person)-[r:KNOWS]->(b:Person)
WHERE a.name = 'Alice' AND b.name = 'Bob'
SET r.since = 2020
```

### Setting Properties with Parameters

```cypher
MATCH (p:Person {name: $name})
SET p.age = $new_age
```

### Replace and Merge Property Maps

```cypher
// Replace ALL properties on a node (keeps internal fields like _vid)
MATCH (n:Person {name: 'Alice'})
SET n = {name: 'Alice', age: 30, city: 'Boston'}

// Merge properties — existing keys are overwritten, others are preserved
MATCH (n:Person {name: 'Alice'})
SET n += {city: 'New York', verified: true}
```

### Adding Labels

```cypher
// Add a label to an existing node
MATCH (p:Person {name: 'Alice'})
SET p:Employee
```

### Removing Properties

Use `REMOVE` to delete a property from a node or relationship, setting it to `null`.

```cypher
// Remove a property from a node
MATCH (u:User {name: 'Alice'})
REMOVE u.age
RETURN u.age  // returns null

// Remove a property from a relationship
MATCH (:User {name: 'Alice'})-[r:FOLLOWS]->(:User {name: 'Bob'})
REMOVE r.since
RETURN r.since  // returns null
```

### Removing Labels

```cypher
// Remove a label from a node
MATCH (p:Person)
REMOVE p:Employee
```

---

## MERGE Clause

`MERGE` is the upsert operation — it matches an existing pattern or creates it if it doesn't exist. Combine with `ON CREATE SET` and `ON MATCH SET` to control which properties are set in each case.

### Basic MERGE (Nodes)

```cypher
// Create node if it doesn't exist, otherwise match it
MERGE (n:Person {name: 'Alice'})

// MERGE with property defaults
MERGE (n:Person {name: 'Alice'})
ON CREATE SET n.created_at = datetime()
ON MATCH SET n.last_seen = datetime()
```

### MERGE with ON CREATE / ON MATCH

```cypher
// Different actions depending on create vs match
MERGE (p:Person {name: 'Alice'})
ON CREATE SET p.created = 1
ON MATCH SET p.matched = 1
RETURN p.created, p.matched
```

When the node doesn't exist, `ON CREATE SET` runs. When it already exists, `ON MATCH SET` runs.

### MERGE Relationships

```cypher
// Merge a relationship between existing nodes
MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'})
MERGE (a)-[r:KNOWS]->(b)
RETURN r
```

If the `KNOWS` relationship already exists between Alice and Bob, it is matched. Otherwise, it is created.

---

## OPTIONAL MATCH

`OPTIONAL MATCH` works like `MATCH` but uses left-outer-join semantics — if no match is found, bound variables from the pattern become `null` instead of eliminating the row.

### Basic Usage

```cypher
// Find all people and their friends (if any)
MATCH (p:Person)
OPTIONAL MATCH (p)-[:KNOWS]->(friend:Person)
RETURN p.name, friend.name
```

People with no friends still appear in the results — `friend.name` is `null` for them.

### Filtering Unmatched Rows

```cypher
// Find people who DON'T know anyone
MATCH (n:Person)
OPTIONAL MATCH (n)-[r:KNOWS]->()
WITH n, r
WHERE r IS NULL
RETURN n.name
```

### With Variable-Length Paths

```cypher
// Optional traversal with variable-length paths
MATCH (a:Person {name: 'Alice'})
OPTIONAL MATCH (a)-[*1..3]->(b:Person)
RETURN a.name, b.name
```

---

## DELETE Clause

Remove nodes and relationships from the graph.

### Deleting Relationships

```cypher
// Delete a specific relationship
MATCH (a:Author)-[r:AUTHORED]->(p:Paper)
WHERE a.name = 'Alice' AND p.title = 'Old Paper'
DELETE r

// Delete all relationships of a type
MATCH ()-[r:DEPRECATED]->()
DELETE r
```

### Deleting Nodes

```cypher
// Delete a node (must have no relationships)
MATCH (p:Paper {title: 'Deleted Paper'})
DELETE p

// Delete node and all its relationships (DETACH)
MATCH (p:Paper {title: 'Deleted Paper'})
DETACH DELETE p

// Delete multiple nodes
MATCH (p:Paper)
WHERE p.year < 1990
DETACH DELETE p
```

### Conditional Deletion

```cypher
// Delete only if condition is met
MATCH (p:Paper)
WHERE p.citations = 0 AND p.year < 2000
DETACH DELETE p
RETURN COUNT(*) AS deleted_count
```

---

## UNION Clause

Combine results from multiple queries.

### UNION (Distinct)

```cypher
// Combine results, removing duplicates
MATCH (a:Author {affiliation: 'MIT'})
RETURN a.name AS name
UNION
MATCH (a:Author {affiliation: 'Stanford'})
RETURN a.name AS name
```

### UNION ALL (Keep Duplicates)

```cypher
// Combine results, keeping all rows
MATCH (p:Paper)-[:CITES]->(:Paper {title: 'Paper A'})
RETURN p.title AS title
UNION ALL
MATCH (p:Paper)-[:CITES]->(:Paper {title: 'Paper B'})
RETURN p.title AS title
```

### Multi-way UNION

```cypher
// Combine multiple queries
MATCH (a:Author) WHERE a.h_index > 50
RETURN a.name AS name, 'high_impact' AS category
UNION
MATCH (a:Author) WHERE a.papers > 100
RETURN a.name AS name, 'prolific' AS category
UNION
MATCH (a:Author) WHERE a.citations > 10000
RETURN a.name AS name, 'highly_cited' AS category
```

---

## WITH RECURSIVE Clause

WITH RECURSIVE enables Common Table Expressions (CTEs) for complex recursive queries.

```cypher
// Find all transitive citations (papers cited by papers cited by...)
WITH RECURSIVE citation_chain AS (
    // Base case: direct citations
    MATCH (start:Paper {title: 'Original Paper'})-[:CITES]->(cited:Paper)
    RETURN cited, 1 AS depth

    UNION

    // Recursive case: citations of citations
    MATCH (prev)-[:CITES]->(cited:Paper)
    WHERE prev IN citation_chain AND depth < 5
    RETURN cited, depth + 1
)
SELECT cited.title, depth FROM citation_chain
```

!!! tip "Alternative: Variable-Length Paths"
    For simpler recursive traversals, variable-length path patterns (`*1..N`) are often more concise:
    ```cypher
    MATCH (start:Paper {title: 'Original Paper'})-[:CITES*1..5]->(cited:Paper)
    RETURN DISTINCT cited.title
    ```

---

## EXPLAIN Clause

Inspect the query execution plan without running the query.

### Basic EXPLAIN

```cypher
// Show query plan
EXPLAIN MATCH (p:Paper)-[:CITES]->(cited:Paper)
WHERE p.year > 2020
RETURN cited.title, COUNT(*) AS citation_count
ORDER BY citation_count DESC
LIMIT 10
```

### Understanding the Plan

The EXPLAIN output shows:
- **Scan operations**: How data is accessed (index scan, full scan)
- **Filter operations**: Where predicates are applied
- **Join operations**: How patterns are matched
- **Aggregation**: Grouping and aggregation steps
- **Sort/Limit**: Final ordering and pagination
- **Index usage**: Which indexes are used and why
- **Cost estimates**: Estimated rows and query cost

### EXPLAIN Output Example

```
Plan:
├─ Scan: Paper
│  ├─ Filter: year > 2020
│  │  └─ Index: paper_year_btree (BTREE) ✓
│  ├─ Estimated rows: 500
│  └─ Cost: 12.5
├─ Expand: CITES (OUTGOING)
│  ├─ Estimated paths: 2500
│  └─ Cost: 125.0
└─ Target: Paper
   └─ Estimated rows: 2500

Index Usage:
  ✓ Paper.year → paper_year_btree (BTREE)
  ✗ Paper.venue → paper_venue_idx (not in filter)

Total estimated cost: 137.5
```

---

## PROFILE Clause

PROFILE executes the query and returns runtime statistics alongside results.

=== "Rust"

    ```rust
    // Execute with profiling
    let (results, profile) = db.profile(
        "MATCH (p:Person) WHERE p.age > 25 RETURN p.name"
    ).await?;

    println!("Total time: {}ms", profile.total_time_ms);
    println!("Rows scanned: {}", profile.rows_scanned);
    println!("Peak memory: {} bytes", profile.peak_memory_bytes);

    // Per-operator statistics
    for op in &profile.operators {
        println!("{}: {}ms, {} rows", op.name, op.time_ms, op.rows_produced);
    }
    ```

=== "Python"

    ```python
    # Execute with profiling
    results, profile = db.profile(
        "MATCH (p:Person) WHERE p.age > 25 RETURN p.name AS name"
    )

    print(f"Total time: {profile['total_time_ms']}ms")
    print(f"Peak memory: {profile['peak_memory_bytes']} bytes")
    ```

### PROFILE Output

The profile includes:
- **total_time_ms**: Total query execution time
- **rows_scanned**: Number of rows read from storage
- **peak_memory_bytes**: Maximum memory used during execution
- **operators**: Per-operator breakdown with timing and row counts

---

## Common Query Patterns

### Find Connected Nodes

```cypher
MATCH (start:Paper {title: 'Attention Is All You Need'})
MATCH (start)-[:CITES]->(cited)
RETURN cited.title, cited.year
ORDER BY cited.year DESC
```

### Bidirectional Relationships

```cypher
// Papers that cite each other
MATCH (a:Paper)-[:CITES]->(b:Paper)-[:CITES]->(a)
RETURN a.title, b.title
```

### Shortest Path

The `shortestPath` function finds the shortest path between two nodes.

```cypher
// Basic shortest path
MATCH path = shortestPath((a:Author {name: 'Alice'})-[:COAUTHOR*]-(b:Author {name: 'Bob'}))
RETURN path

// With hop constraints (minimum and maximum hops)
MATCH path = shortestPath((a:Person)-[:KNOWS*2..6]-(b:Person))
WHERE a.name = 'Alice' AND b.name = 'Bob'
RETURN path, length(path) AS hops

// Minimum hops only (at least 2 hops)
MATCH path = shortestPath((a)-[:FOLLOWS*2..]-(b))
RETURN path

// Maximum hops only (at most 5 hops)
MATCH path = shortestPath((a)-[:KNOWS*..5]-(b))
RETURN path

// Zero-length path (source equals target with min_hops=0)
MATCH path = shortestPath((a)-[:KNOWS*0..3]-(a))
RETURN path
```

**Hop Constraint Semantics:**
- `*` or `*1..` — Unlimited hops (default: 1 to ∞)
- `*2..6` — Between 2 and 6 hops
- `*..5` — At most 5 hops (1 to 5)
- `*3..` — At least 3 hops (3 to ∞)
- `*0..` — Zero or more hops (allows source == target)

### Degree Counting

```cypher
// Count outgoing relationships
MATCH (p:Paper)-[c:CITES]->()
RETURN p.title, COUNT(c) AS out_degree
ORDER BY out_degree DESC
```

### Top-K with Ties

```cypher
MATCH (p:Paper)
WITH p, p.citations AS cites
ORDER BY cites DESC
LIMIT 10
RETURN p.title, cites
```

---

## Graph Algorithms

Uni provides 36 high-performance graph algorithms available via the `algo` namespace.

### Centrality Algorithms

| Algorithm | Procedure | Description |
|-----------|-----------|-------------|
| PageRank | `algo.pageRank` | Importance based on incoming links |
| Betweenness | `algo.betweenness` | Nodes on shortest paths |
| Closeness | `algo.closeness` | Average distance to all nodes |
| Degree Centrality | `algo.degreeCentrality` | Connection count |
| Harmonic Centrality | `algo.harmonicCentrality` | Harmonic mean distance |
| Eigenvector Centrality | `algo.eigenvectorCentrality` | Influence via neighbors |
| Katz Centrality | `algo.katzCentrality` | Weighted path influence |

```cypher
// PageRank
CALL algo.pageRank(['Paper'], ['CITES'])
YIELD nodeId, score
RETURN nodeId, score
ORDER BY score DESC

// Betweenness Centrality
CALL algo.betweenness(['Author'], ['COAUTHOR'], true, 100)
YIELD nodeId, score

// Closeness Centrality
CALL algo.closeness(['Station'], ['CONNECTS'])
YIELD nodeId, score
```

### Community Detection Algorithms

| Algorithm | Procedure | Description |
|-----------|-----------|-------------|
| Louvain | `algo.louvain` | Modularity-based community detection |
| Label Propagation | `algo.labelPropagation` | Fast community detection |
| WCC | `algo.wcc` | Weakly connected components |
| SCC | `algo.scc` | Strongly connected components |
| K-Core | `algo.kCore` | K-core decomposition |
| Triangle Count | `algo.triangleCount` | Count triangles per node |

```cypher
// Louvain
CALL algo.louvain(['User'], ['INTERACTS'])
YIELD nodeId, communityId

// Label Propagation
CALL algo.labelPropagation(['User'], ['INTERACTS'])
YIELD nodeId, communityId

// Weakly Connected Components
CALL algo.wcc(['User'], ['INTERACTS'])
YIELD nodeId, componentId

// Strongly Connected Components
CALL algo.scc(['Task'], ['DEPENDS_ON'])
YIELD nodeId, componentId

// K-Core Decomposition
CALL algo.kCore(['User'], ['FRIEND'], 3)
YIELD nodeId, coreNumber

// Triangle Count
CALL algo.triangleCount(['User'], ['FRIEND'])
YIELD nodeId, triangleCount
```

### Path Finding Algorithms

| Algorithm | Procedure | Description |
|-----------|-----------|-------------|
| Dijkstra | `algo.dijkstra` | Shortest weighted path |
| Bellman-Ford | `algo.bellmanFord` | Shortest path with negative weights |
| A* | `algo.astar` | Heuristic shortest path |
| Bidirectional Dijkstra | `algo.bidirectionalDijkstra` | Two-way shortest path |
| K-Shortest Paths | `algo.kShortestPaths` | Multiple shortest paths |
| All Simple Paths | `algo.allSimplePaths` | All paths between nodes |
| APSP | `algo.allPairsShortestPath` | All pairs shortest path |

```cypher
// Dijkstra shortest path
CALL algo.dijkstra(['Station'], ['CONNECTS'], $startId, $endId, 'distance')
YIELD path, cost

// K-Shortest Paths
CALL algo.kShortestPaths(['Station'], ['CONNECTS'], $startId, $endId, 5)
YIELD path, cost

// All Pairs Shortest Path
CALL algo.allPairsShortestPath(['Station'], ['CONNECTS'])
YIELD sourceNodeId, targetNodeId, distance
```

### Similarity & Traversal Algorithms

| Algorithm | Procedure | Description |
|-----------|-----------|-------------|
| Node Similarity | `algo.nodeSimilarity` | Jaccard similarity |
| Random Walk | `algo.randomWalk` | Random graph traversal |
| Maximal Cliques | `algo.maximalCliques` | Find all maximal cliques |
| Graph Coloring | `algo.graphColoring` | Vertex coloring |

```cypher
// Node Similarity (Jaccard)
CALL algo.nodeSimilarity(['Product'], ['PURCHASED'])
YIELD node1, node2, similarity

// Random Walk
CALL algo.randomWalk(['Page'], ['LINKS'], 10, 5)
YIELD path
```

### Structural Analysis Algorithms

| Algorithm | Procedure | Description |
|-----------|-----------|-------------|
| Topological Sort | `algo.topologicalSort` | DAG ordering |
| Cycle Detection | `algo.cycleDetection` | Find cycles |
| Bipartite Check | `algo.bipartiteCheck` | Test bipartiteness |
| Bridges | `algo.bridges` | Find bridge edges |
| Articulation Points | `algo.articulationPoints` | Find cut vertices |
| Elementary Circuits | `algo.elementaryCircuits` | Find all simple cycles |

```cypher
// Topological sort (for DAGs)
CALL algo.topologicalSort(['Task'], ['DEPENDS_ON'])
YIELD nodeId, order

// Find articulation points
CALL algo.articulationPoints(['Router'], ['CONNECTS'])
YIELD nodeId
```

### Flow & Matching Algorithms

| Algorithm | Procedure | Description |
|-----------|-----------|-------------|
| Maximum Matching | `algo.maxMatching` | Maximum cardinality matching |
| Minimum Spanning Tree | `algo.mst` | MST using Prim/Kruskal |
| Dinic's Algorithm | `algo.dinic` | Maximum flow |
| Ford-Fulkerson | `algo.fordFulkerson` | Maximum flow |

```cypher
// Minimum Spanning Tree
CALL algo.mst(['City'], ['ROAD'], 'distance')
YIELD edge, cost

// Maximum Matching
CALL algo.maxMatching(['Worker', 'Job'], ['CAN_DO'])
YIELD node1, node2
```

---

## Schema DDL

Uni supports Data Definition Language (DDL) statements for managing schema.

### Create Label (Vertex Type)

```cypher
// Create a label with properties
CREATE LABEL Paper (
  title STRING NOT NULL,
  year INT32,
  abstract STRING,
  embedding VECTOR(768)
)

// Create label with default values
CREATE LABEL Author (
  name STRING NOT NULL,
  h_index INT32 DEFAULT 0,
  active BOOLEAN DEFAULT true
)
```

#### Parameterized container types

Property types can be parameterized, including nested forms:

```cypher
CREATE LABEL Document (
  title    STRING,
  embedding VECTOR(384),            // fixed-dimension dense vector
  tags     LIST<STRING>,            // typed list
  tokens   LIST<VECTOR(96)>,        // multi-vector (ColBERT / late-interaction)
  scores   MAP<STRING, FLOAT>,      // typed map (string keys)
  sections MAP<STRING, LIST<INT>>   // nested: map of lists
)
```

`LIST<T>` accepts any element type (scalars, `VECTOR(N)`, nested `LIST`/`MAP`). `MAP<K, V>` requires string keys; `V` may be any scalar or nested container. A `LIST<VECTOR(N)>` property is the storage type for multi-vector search — see [Vector Search — Multi-Vector](../features/vector-search.md#multi-vector-search-colbert-late-interaction).

### Create Edge Type

```cypher
// Create edge type with source/destination constraints
CREATE EDGE TYPE AUTHORED (
  position INT32,
  corresponding BOOLEAN DEFAULT false
) FROM Author TO Paper

// Edge type between multiple label types
CREATE EDGE TYPE COLLABORATES (
  papers_count INT32
) FROM Author TO Author
```

### Alter Schema

```cypher
// Add a property to an existing label
ALTER LABEL Paper ADD PROPERTY abstract STRING

// Drop a property
ALTER LABEL Paper DROP PROPERTY deprecated_field

// Rename a property
ALTER LABEL Paper RENAME PROPERTY old_name TO new_name
```

### Drop Schema Elements

```cypher
// Drop a label (must have no vertices)
DROP LABEL TempLabel

// Drop an edge type
DROP EDGE TYPE OLD_RELATIONSHIP
```

---

## Constraints

Define and manage data integrity constraints.

### Create Constraints

```cypher
// Unique constraint
CREATE CONSTRAINT paper_doi_unique
ON (p:Paper) ASSERT p.doi IS UNIQUE

// Existence constraint (property must be present)
CREATE CONSTRAINT author_name_exists
ON (a:Author) ASSERT EXISTS(a.name)

// Check constraint (custom validation)
CREATE CONSTRAINT paper_year_valid
ON (p:Paper) ASSERT p.year >= 1900
```

CHECK enforcement covers a single `property <op> literal` comparison, where `<op>` is one of `=`, `!=`/`<>`, `>`, `<`, `>=`, `<=` and the literal is a single whitespace-free token. An expression that does not reduce to those three tokens — one joined with `AND`/`OR`, for instance — is still accepted and listed by `SHOW CONSTRAINTS`, but it logs a warning and permits the write rather than rejecting it, so do not rely on it to enforce a compound rule. Split it into one constraint per comparison instead. Do not point the right-hand side at a second property either: `REQUIRE p.year >= p.min_year` reduces to three tokens but compares against the *string* `p.min_year`, and the operands cannot be ordered, so the write fails with a comparison error rather than either passing or reporting a constraint violation. Numeric comparisons, including `=` and `!=`, coerce across `Int` and `Float`, so `REQUIRE p.score = 5` holds for a stored `5.0`. A missing property passes; use `IS NOT NULL` to require presence.

### Show Constraints

```cypher
// List all constraints
SHOW CONSTRAINTS

// Filter by label
SHOW CONSTRAINTS ON (Paper)
```

### Drop Constraints

```cypher
DROP CONSTRAINT paper_doi_unique
```

---

## Database Management

Administrative commands and procedures for database maintenance.

### BACKUP

Create a backup of the database to a specified destination.

```cypher
// Backup to local path
BACKUP TO './backups/backup_2026_01_17'

// Backup to S3
BACKUP TO 's3://my-bucket/backups/backup_2026_01_17'
```

### CHECKPOINT

Flush L0 to L1 so recent writes become part of a durable snapshot boundary.

```cypher
CHECKPOINT
```

### VACUUM

Flush and compact all L1 runs into L2 for storage hygiene.

```cypher
VACUUM
```

### Compaction

```cypher
// Trigger storage compaction
CALL uni.admin.compact()
YIELD tables_optimized, fragments_removed, fragments_added, bytes_reclaimed, duration_ms

// Check compaction status
CALL uni.admin.compactionStatus()
YIELD l1_runs, l1_size_bytes, in_progress, pending
```

### Snapshots

```cypher
// Create a named snapshot
CALL uni.admin.snapshot.create('before_migration')
YIELD snapshot_id

// List available snapshots
CALL uni.admin.snapshot.list()
YIELD snapshot_id, name, created_at, version_hwm

// Restore to a snapshot
CALL uni.admin.snapshot.restore('before_migration')
YIELD status
```

### Schema Introspection

```cypher
// List all labels
CALL uni.schema.labels()
YIELD label, propertyCount, nodeCount, indexCount
RETURN label, nodeCount

// List all relationship types
CALL uni.schema.relationshipTypes()
YIELD type, sourceLabels, targetLabels, propertyCount
RETURN type

// List all edge types (alias)
CALL uni.schema.edgeTypes()
YIELD type, sourceLabels, targetLabels, propertyCount

// List all indexes
CALL uni.schema.indexes()
YIELD name, type, label, properties, state
WHERE type = 'VECTOR'
RETURN name, label

// List all constraints
CALL uni.schema.constraints()
YIELD name, type, label, properties, enabled
RETURN name, type

// Get detailed label info
CALL uni.schema.labelInfo('Paper')
YIELD property, dataType, nullable, indexed, unique
RETURN property, dataType
```

### Schema DDL Procedures

Create and modify schema at runtime via procedures:

```cypher
// Create a new label
CALL uni.schema.createLabel('Product', {
  properties: {
    name: { type: 'STRING', nullable: false },
    price: { type: 'FLOAT64' },
    embedding: { type: 'VECTOR' }
  }
})

// Create an edge type
CALL uni.schema.createEdgeType('PURCHASED', ['Customer'], ['Product'], {
  properties: {
    quantity: { type: 'INT32' },
    timestamp: { type: 'TIMESTAMP' }
  }
})

// Create an index
CALL uni.schema.createIndex('Product', 'name', { type: 'BTREE' })

// Create a vector index
CALL uni.schema.createIndex('Product', 'embedding', {
  type: 'VECTOR'
})

// Create a constraint
CALL uni.schema.createConstraint('Product', 'UNIQUE', ['sku'])

// Drop operations
CALL uni.schema.dropLabel('TempLabel')
CALL uni.schema.dropEdgeType('OLD_RELATIONSHIP')
CALL uni.schema.dropIndex('Product', 'old_index')
CALL uni.schema.dropConstraint('Product', 'constraint_name')
```

---

## CRDT Properties

Uni supports CRDT (Conflict-free Replicated Data Type) properties for distributed, eventually-consistent data. CRDT properties can be defined in the schema and manipulated via Cypher.

### Schema Definition

Define CRDT properties in your schema:

```json
{
  "properties": {
    "Paper": {
      "title": { "type": "String", "nullable": false },
      "view_count": { "type": "Crdt", "crdt_type": "GCounter" },
      "tags": { "type": "Crdt", "crdt_type": "ORSet" },
      "metadata": { "type": "Crdt", "crdt_type": "LWWMap" }
    }
  }
}
```

### Supported CRDT Types

| Type | Description | Cypher Operations |
|------|-------------|-------------------|
| `GCounter` | Grow-only counter | Increment only |
| `GSet` | Grow-only set | Add only |
| `ORSet` | Add/remove set (add-wins) | Add, remove |
| `LWWRegister` | Single value (last-writer-wins) | Set value |
| `LWWMap` | Key-value map (per-key LWW) | Put, remove keys |
| `Rga` | Ordered sequence | Insert, delete at position |

### Querying CRDT Properties

```cypher
// Read CRDT values (returns materialized value)
MATCH (p:Paper {id: 'paper_001'})
RETURN p.view_count, p.tags

// GCounter returns total count
// ORSet returns list of visible elements
// LWWMap returns map of non-tombstoned entries
```

### Updating CRDT Properties

!!! warning "No `crdt.*` Cypher functions exist"
    Earlier revisions of this page documented a `crdt.increment()` /
    `crdt.orset_add()` / `crdt.map_put()` family plus an 18-function table.
    **None of those functions is registered anywhere in the codebase** and none
    ever was. They are removed here rather than reworded.

CRDTs are a **plugin surface**, not a set of query-language functions. A provider
implements `CrdtKindProvider` and registers a named kind on the registrar; merge
semantics then apply automatically when concurrent writes to that property are
reconciled.

The built-in kinds (`crates/uni-plugin-builtin/src/crdts.rs`) are:

| Kind id | Semantics |
|---|---|
| `lww-register` | Last-writer-wins single value |
| `or-set` | Observed-remove set (add wins over concurrent remove) |
| `g-counter` | Grow-only counter |
| `mv-register` | Multi-value register (keeps concurrent siblings) |
| `rga` | Replicated growable array (ordered sequence) |

```rust
// Registration is the extension point; there is no Cypher-level CRDT call.
r.crdt_kind(CrdtKind::new("g-counter"), Arc::new(GCounterProvider))?;
```

Declare a CRDT-typed property through the schema (`DataType::Crdt(..)`) and write
it with ordinary `SET`; the registered provider merges concurrent updates.

### Distributed Sync

CRDT properties enable conflict-free synchronization across distributed nodes:

```
┌─────────────┐         ┌─────────────┐
│   Node A    │◄───────►│   Node B    │
│             │         │             │
│ increment   │         │ increment   │
│ view_count  │         │ view_count  │
│    +5       │         │    +3       │
└──────┬──────┘         └──────┬──────┘
       │                       │
       └───────────┬───────────┘
                   │
                   ▼
            ┌─────────────┐
            │   Merged    │
            │ view_count  │
            │     = 8     │
            └─────────────┘
```

For detailed CRDT semantics and merge behavior, see [CRDT Types](../concepts/crdt-types.md).

---

## Cypher Feature Status

| Feature | Status |
|---------|--------|
| MATCH (basic patterns) | Stable |
| WHERE (comparisons, boolean) | Stable |
| RETURN, ORDER BY, LIMIT, SKIP | Stable |
| Aggregations (COUNT, SUM, AVG, MIN, MAX, COLLECT) | Stable |
| Window Functions (OVER clause) | Stable |
| Scalar Functions (40+ functions) | Stable |
| CREATE (nodes and relationships) | Stable |
| WITH clause | Stable |
| UNWIND | Stable |
| CALL procedures | Stable |
| Index management | Stable |
| JSON Full-Text Search (CONTAINS) | Stable |
| Variable-length paths (`*1..3`) | Stable |
| MERGE | Stable |
| SET / REMOVE | Stable |
| DELETE / DETACH DELETE | Stable |
| UNION / UNION ALL | Stable |
| EXPLAIN (with index usage) | Stable |
| Temporal Queries (`uni.temporal.validAt`, `VALID_AT`) | Stable |
| Schema DDL (CREATE/ALTER/DROP LABEL) | Stable |
| Schema DDL Procedures (`uni.schema.createLabel`, etc.) | Stable |
| Constraints (UNIQUE, EXISTS, CHECK) | Stable |
| Composite Key Constraints | Stable |
| Graph Algorithms (`uni.algo.*`) | Stable |
| CRDT Properties | Stable |
| Database Management (BACKUP, COPY, compaction) | Stable |
| Snapshots (create, list, restore) | Stable |
| Schema Introspection (`uni.schema.*`) | Stable |
| Inverted Index (ANY IN pattern) | Stable |
| OPTIONAL MATCH | Stable |
| Subqueries (EXISTS, CALL) | Stable |
| WITH RECURSIVE (CTEs) | Stable |
| PROFILE (runtime statistics) | Stable |
| Session Variables (`$session.*`) | Stable |
| Multi-label Nodes | Stable |
| Regular Expression Matching (`=~`) | Stable |
| shortestPath with Hop Constraints (`*1..5`) | Stable |
| Quantified Path Patterns (`(( … )){n,m}`) | Stable |
| Group Variables in Quantified Patterns | Stable |
| BTree Index STARTS WITH Optimization | Stable |

---

## Next Steps

- [Vector Search](vector-search.md) — Semantic similarity queries
- [Data Ingestion](data-ingestion.md) — Bulk import and streaming writes
- [Performance Tuning](performance-tuning.md) — Query optimization strategies
- [Troubleshooting](../reference/troubleshooting.md) — Common issues and solutions
