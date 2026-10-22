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

### Quoted Identifiers

Any name (variable, label, relationship type, property key, map key, alias,
parameter, procedure, index or constraint name) can be written in backticks,
which allows spaces, punctuation and keywords. A backtick inside the name is
written doubled:

```cypher
MATCH (`the paper`:Paper)
WHERE `the paper`.`order` = $`min order`
RETURN `the paper`.`has``tick` AS `a``b`   // property has`tick, column a`b
```

The quoted and the bare spelling of a name are the same name: `` `p` `` and
`p` are one variable.

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

// No written upper bound — the planner caps this at 100 hops
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

#### What a variable-length pattern costs

Two different things happen when a variable-length pattern runs, and knowing
which one is expensive is usually enough to fix a slow query.

First a breadth-first search explores the graph and records, compactly, which
vertices reach which. Its cost tracks the **edges explored** — roughly the size
of the reachable neighbourhood — and no hop bound or `LIMIT` avoids it.

Then, *only if the query asks for paths*, those records are expanded into actual
paths. This is where cost can explode: on a graph with cycles, the number of
distinct paths grows combinatorially with the hop bound, even though the search
that found them was cheap.

So the single most effective change is often to stop asking for paths:

```cypher
// Expensive: builds every distinct path
MATCH p = (a:Company {id: 'X'})-[:OWNS*]->(b:Company)
RETURN p

// Cheap: the search runs, the expansion does not
MATCH (a:Company {id: 'X'})-[:OWNS*]->(b:Company)
RETURN DISTINCT b
```

If you need one path rather than all of them, `shortestPath` stops at the first
one it finds and is dramatically cheaper than enumerating and discarding.

#### Using LIMIT with an unbounded pattern

A `LIMIT` stops the path expansion rather than trimming its output, so asking
for a handful of paths costs a handful of paths:

```cypher
// Returns promptly even though the full path set is combinatorial
MATCH p = (a:Company {id: 'X'})-[:OWNS*]->(b:Company)
RETURN length(p) AS hops
LIMIT 5
```

Two things to expect:

- **The search still runs in full.** `LIMIT` cannot avoid the breadth-first
  stage, so there is a floor under the query time set by the size of the
  reachable neighbourhood, not by the limit.
- **The limit takes effect a batch at a time.** Anything up to one batch
  (8192 rows by default) costs the same as `LIMIT 1`; a limit above that costs
  proportionally more batches.

Measured on a 852-vertex, 1013-edge graph with cycles, where the same query
without a limit does not complete within a 30-second timeout:

| Query | Result |
|---|---|
| `... RETURN p LIMIT 5` | 5 rows, ~4 s |
| `... RETURN p LIMIT 8192` | 8192 rows, ~4 s |
| `... RETURN p LIMIT 20000` | 20000 rows, ~16 s |
| `... RETURN count(p)` (no limit) | times out |

The last row is not a defect: counting *every* path of an unbounded pattern over
a cyclic graph is unbounded work. Bound the hops, limit the rows, or ask for
endpoints instead.

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

#### Inner node labels

A label on an inner node holds on every iteration. A label on the sub-pattern's
first node holds for the node that starts each iteration: the outer start node,
and each node where one iteration ends and the next begins. A label on a later
inner node holds for that node on every iteration. Each inner node may carry at
most one label; filter extra labels in `WHERE`.

<!-- doctest: skip -->
```cypher
// Every node a hop starts from is a Person; the final node may be anything.
MATCH (a {name: 'Alice'})((x:Person)-[:KNOWS]->(y)){1,3}(b)
RETURN b.name
```

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

Floats compare numerically, as openCypher specifies: `-0.0 = 0.0` is true and
neither is less than the other; `NaN = x` is false and `NaN <> x` is true for
every number `x` (including `NaN`), and `<`, `<=`, `>`, `>=` with a `NaN`
operand are false. `DISTINCT` and grouping treat `-0.0` and `0.0` as one value
and every `NaN` as one value; `ORDER BY` puts `NaN` after positive infinity.

An integer and a float compare by exact value; the integer is never rounded to
a float. `9007199254740993 = 9007199254740992.0` is false and
`9007199254740993 > 9007199254740992.0` is true, in `WHERE`, `IN`, joins,
`ORDER BY`, `min` and `max`, on literals, parameters and stored properties
alike. An integer equal to a float is the same value: `1` and `1.0` are one row
under `DISTINCT` and `UNION`, one group, and one element of
`collect(DISTINCT ...)`. The value kept is the first one seen, so
`UNWIND [1.0, 1] AS x RETURN DISTINCT x` returns `1.0`. An aggregate over
`DISTINCT` sees those same first values: `sum(DISTINCT x)` over `[1.0, 1]` is
`1.0`, and `collect(DISTINCT x)` is `[1.0]`. `UNION` of an integer column with
a float column keeps each row's own type.

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

Operators follow openCypher's NULL rules, whether an operand is a literal, a
declared property or a schemaless one:

- Arithmetic, comparison, string and regex operators over NULL return NULL,
  whatever the other operand is: `'ab' + null`, `p.missing ^ 2` and
  `p.title =~ null` are all NULL.
- `AND`, `OR` and `XOR` use three-valued (Kleene) logic. An operand that decides
  the answer wins whatever the other operand is, in either position:
  `false AND x` is false and `true OR x` is true. Otherwise a non-boolean
  operand is a type error, beside NULL too: `null AND p.count` is an error, not
  NULL. A number, string, list or map *literal* under `AND`/`OR`/`XOR`/`NOT` is
  rejected when the query is compiled.
- `STARTS WITH`, `ENDS WITH` and `CONTAINS` with a non-string operand return
  NULL.

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

// Regular expression matching: the pattern must match the WHOLE title
WHERE p.title =~ '.*Transform.*'

// Case-insensitive regex
WHERE p.email =~ '(?i)john@.*'

// Email pattern matching (anchors are implied)
WHERE p.email =~ '[\\w.-]+@[\\w.-]+\\.\\w+'
```

**Note:** The `CONTAINS` operator is automatically routed to JSON FTS indexes when the column has an FTS index, enabling BM25-based full-text search with relevance ranking.

**Regex Notes:**
- `=~` matches the **whole** string, as in Neo4j (Java's `Pattern.matches`):
  `'abc' =~ 'b'` is `false` and `'abc' =~ '.*b.*'` is `true`. Write `.*` on
  either side to search inside a string.
- The end of the pattern is the absolute end of the string: `'abc\n' =~ 'abc'`
  and `'abc\n' =~ 'abc$'` are `false`, also under `(?m)`. Alternation is
  decided over the whole string, so `'ab' =~ 'a|ab'` is `true`.
- Patterns use Java's `java.util.regex` syntax and defaults, as Neo4j does
  (`Pattern.compile(p)` with no flags):
  - `\w`, `\d`, `\s`, `\b` and the POSIX classes (`\p{Lower}`, `\p{Alpha}`, …)
    are ASCII: `'é' =~ '\\w'` is `false`. `(?U)` makes them Unicode.
  - `(?i)` folds ASCII letters only: `'É' =~ '(?i)é'` is `false`. Add `(?u)`
    (or `(?U)`) for Unicode case folding: `'É' =~ '(?iu)é'` is `true`.
  - `.` matches any character except a line terminator (`\n`, `\r`, U+0085,
    U+2028, U+2029); `(?s)` lets it match those too.
  - `$` also matches before a final line terminator (`'a\n' =~ 'a$\\n'` is
    `true`); `(?m)` makes `^`/`$` match at every line break.
  - `\v` is vertical whitespace, `\<`/`\>` are the literal characters, and
    `\p{..}` names follow Java (`\p{IsGreek}`, `\p{sc=Greek}`, `\p{Lu}`).
- Java syntax is accepted exactly as Java accepts it, and a pattern Java
  rejects is an invalid-pattern error. That includes `\Q…\E`, `\h`/`\H`,
  `\v`/`\V`, `\R`, `\Z`, `\z`, `\G` (the start, under `matches()`), `\e`,
  `\cX`, octal (`\0101`), `\k<name>`, `(?d)` (UNIX_LINES: only `\n` ends a
  line), flag groups such as `(?)` and `(?i-i)`, `(?x)` comments, class
  intersections (`[a-z&&[^k]]`), and stacked quantifiers with Java's meaning
  (`x{1}{2}` is `x` followed by an empty string repeated twice).
- Every pattern Java accepts is accepted, with Java's meaning; no valid
  pattern is refused. That includes look-ahead and look-behind (also with
  `\b`, look-arounds or backreferences in the body, and over emoji and other
  supplementary characters), backreferences (case-insensitive ones under
  `(?i)` and `(?iu)` too, and backreferences inside the group they name),
  atomic groups (`(?>…)`), possessive quantifiers (`a*+`), Unicode blocks
  (`\p{InGreek}`, `\p{block=Greek}`), the `Character` predicates
  (`\p{javaLowerCase}`, `\p{javaJavaIdentifierStart}`, …), named characters
  (`\N{LATIN SMALL LETTER A}`), grapheme clusters (`\X`) and boundaries
  (`\b{g}`), and `(?c)` (canonical equivalence for classes and properties:
  `'e\u0301' =~ '(?c)[é]'` is `true`).
  - Patterns with those constructs (and with `\b`, a multi-line or inner
    `^`/`$`, or a quantified `\R`) run on a backtracking matcher over the
    string's UTF-16 units, which follows Java's corner cases too: a capture
    made in a look-ahead survives backtracking past it, a loop ends at its
    first empty iteration (`'a' =~ '(?:a|\G){2}'` is `false`), a
    look-behind can start inside a surrogate pair, and `(?i)` backreferences
    fold case as Java does (`'kK' =~ '(?i)(k)\1'` is `true`, `'k\u212A' =~
    '(?i)(k)\1'` is `false`, `true` under `(?iu)`). Every other pattern runs
    on a linear-time automaton. The choice is made once per pattern.
  - Where Java's matcher itself throws on an input (it can read past the end
    of the string, as `\b{g}` does after a look-ahead that reached the end:
    `'ab' =~ '(?=ab)a\b{g}b'`), the query fails with an error naming the
    exception, as it does in Neo4j.
- **Backtrack limit.** The backtracking matcher stops after
  `regex_backtrack_limit` backtracking steps per match (default 10,000,000,
  about 0.1–0.3 s for a refused match) and the query fails with `UniError::RegexBacktrackLimit`
  (`UniRegexBacktrackLimitError` in Python, with `pattern` and `limit`).
  Patterns like `(a+)+b` that backtrack exponentially in Java reach it on
  long inputs; Java has no limit and would keep searching. Raise it with
  `UniConfig { regex_backtrack_limit, .. }` or `config({"regex_backtrack_limit":
  …})` in Python. A long scan of `=~` rows also honours the query timeout:
  the deadline is checked between rows.
- **Recursion limit.** Java's matcher runs out of stack on a backtracking
  loop over a long string (`(?:a|b)*` past about 1,500 characters on a
  default thread stack) and throws `StackOverflowError`. uni-db keeps its
  backtracking state on an explicit stack and answers such inputs: a match
  fails with a "Regex recursion limit exceeded" error only when its
  backtrack stack outgrows a 64 MB budget (a 200,000-character
  `(?:a|b)*(?=)` match is answered, a 2,400,000-character one is not). Loops Java runs without
  recursion (`a*`, `(?:ab)*`) and patterns the automaton answers have no
  such limit. A pattern nested more than 2,000 groups deep is rejected with
  Java's "Stack overflow during pattern compilation".
- **Reference JVM and Unicode version.** The semantics are pinned to Java 21,
  the minimum JVM the current Neo4j release (2026.09) supports ("Java SE 21
  and Java SE 25 Platform Specification", [Neo4j Operations Manual, system
  requirements](https://neo4j.com/docs/operations-manual/current/installation/requirements/)).
  Character classes, properties, scripts, blocks, character names, case
  folding, grapheme clusters and canonical equivalence use Java 21's
  Unicode 15.0 data, generated from that JDK, whatever the Rust toolchain:
  a character assigned in Unicode 16.0 or later is unassigned (`\p{Cn}`),
  and scripts added since (`\p{IsGaray}`, …) are unknown names, as on
  Java 21.
- Agreement with Java 21 is checked against answers recorded from the JDK
  for generated and hand-written cases (constructs, anchors, Unicode
  properties, blocks and `\p{java…}`, character names, grapheme clusters,
  `(?c)`, case-insensitive backreferences, empty loop iterations,
  look-arounds). Where Java 25 answers differently (Unicode 16.0 data and
  Java 25's newer grapheme rules), uni-db answers as Java 21. Look-behinds
  whose length arithmetic nears 2^31 (`{2147483647,}`, zero-width loops of
  about 10^9 iterations) can still differ from Java.
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

// A number, boolean or temporal concatenates its toString()
RETURN 'v' + 1.0, 'due ' + duration('P1D')   // 'v1.0', 'due P1D'

// `^` is always a float
RETURN 2 ^ 3   // 8.0

// Temporal arithmetic takes durations; subtracting two values of one
// temporal type gives a duration, and a number is not a duration
RETURN date('2020-01-02') - date('2020-01-01')   // P1D
RETURN duration('P1D') * 2                        // P2D
// date('2020-01-02') + 1 is a type error

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
| `SUM(x)` | Sum of numbers or of durations; `0` when there are none (no rows, or all NULL). Any other value, or numbers mixed with durations, is a type error | `RETURN SUM(p.citations)` |
| `AVG(x)` | Average of numbers (a float) or of durations (a duration); NULL when there are none. Other values are a type error | `RETURN AVG(p.citations)` |
| `MIN(x)` | Minimum, in `ORDER BY` order (durations by length, date-times by instant, offset kept) | `RETURN MIN(p.year)` |
| `MAX(x)` | Maximum, in `ORDER BY` order | `RETURN MAX(p.year)` |
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
| `sum()` | Running sum of numbers or durations; `0` over a frame with no non-null value | `sum(p.citations) OVER (ORDER BY p.year)` |
| `avg()` | Running average of numbers or durations | `avg(p.citations) OVER (PARTITION BY p.venue)` |
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

Math functions take numbers: `NULL` gives `NULL`, and any other value (a string, a duration) is a type error. So are `stDev`, `variance` and the percentiles over a value that is not a number.

`percentileDisc(x, p)` returns one of its inputs, unchanged: over integers it returns an integer, exactly. `percentileCont(x, p)` returns a float interpolated between the inputs around the percentile. The percentile `p` must be a number between `0.0` and `1.0`.

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
| `range(start, end, [step])` | Integer sequence from `start` to `end` inclusive; `null` if any argument is `null`. A non-integer argument raises `InvalidArgumentType`, a zero step `NumberOutOfRange` | `RETURN range(1, 5)` → `[1,2,3,4,5]`; `RETURN range(0, null)` → `null` |

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
| `vector_similarity(v1, v2)` | Cosine similarity between two vectors (NULL if either is NULL); MaxSim with `v2` as the query for two multi-vectors. Evaluated per row: a `WHERE` threshold returns every matching node and never uses a vector index (use `~=` for top-K). | `WHERE vector_similarity(a.embed, b.embed) > 0.8` |
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

### Write Counters

`tx.execute(...)` returns an `ExecuteResult` whose counters describe what the
statement changed. openCypher does not specify execution counters; these
follow Neo4j's query statistics, with the TCK's (entity, key, value) rule for
properties:

| Counter | Counts |
|---|---|
| `nodes_created` / `nodes_deleted` | Nodes created or deleted. A `SET`, `REMOVE` or `MERGE ... ON MATCH` on an existing node creates nothing. |
| `relationships_created` / `relationships_deleted` | The same for relationships, including those `DETACH DELETE` removes. |
| `properties_set` | Properties added, or overwritten with a different value. Assigning the value a property already has counts nothing. |
| `properties_removed` | Properties removed by an explicit `REMOVE` or `SET ... = null`, or overwritten with a different value. The properties of a deleted node or relationship are not counted, as in Neo4j. |
| `labels_added` / `labels_removed` | Per node whose label set changes, like Neo4j's `labelsAdded` / `labelsRemoved`. Deleting a node does not count its labels. |

An entity created and changed in one statement reports only its final state,
and an entity named twice in one `DELETE` is counted once.

The openCypher TCK's "side effects" (`+labels`, `-properties`, ...) are not
these counters: the TCK harness diffs the graph before and after the
statement. By that diff `CREATE (:Label), (:Label)` is `+labels 1` (one label
newly in use) and deleting a node with two properties is `-properties 2`;
here they report `labels_added = 2` and `properties_removed = 0`.

```cypher
CREATE (:Person {name: 'Alice', age: 30})-[:KNOWS {since: 2020}]->(:Person {name: 'Bob'})

// nodes_deleted = 1, relationships_deleted = 1, properties_removed = 0
MATCH (n:Person {name: 'Alice'}) DETACH DELETE n
```

---|---|---|
| `nodes_created` / `nodes_deleted` | `+nodes` / `-nodes` | Nodes created or deleted. A `SET`, `REMOVE` or `MERGE ... ON MATCH` on an existing node creates nothing. |
| `relationships_created` / `relationships_deleted` | `+relationships` / `-relationships` | The same for relationships, including those `DETACH DELETE` removes. |
| `properties_set` | `+properties` | Properties added, or overwritten with a different value. Assigning the value a property already has counts nothing. |
| `properties_removed` | `-properties` | Properties removed, set to `null`, or overwritten with a different value, and every property of a deleted node or relationship. |
| `labels_added` / `labels_removed` | `+labels` / `-labels` | Per node whose label set changes. |

An entity created and changed in one statement reports only its final state;
one created and deleted in the same statement reports no property changes.

The label counters follow Neo4j's `labelsAdded` / `labelsRemoved`, not the
TCK. The TCK compares the set of labels in use across the whole graph before
and after the statement: `CREATE (:Label), (:Label)` is `+labels 1`, and
`CREATE (:Label)` when other `:Label` nodes already exist is no label change.
Here the first reports `labels_added = 2` and the second `labels_added = 1`,
and deleting a node does not count its labels as removed.

```cypher
CREATE (:Person {name: 'Alice', age: 30})-[:KNOWS {since: 2020}]->(:Person {name: 'Bob'})

// nodes_deleted = 1, relationships_deleted = 1,
// properties_removed = 3 (two on Alice, one on the relationship)
MATCH (n:Person {name: 'Alice'}) DETACH DELETE n
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

### WHERE After ORDER BY / SKIP / LIMIT

A `WHERE` that follows `WITH … ORDER BY … SKIP … LIMIT` filters the rows the
page kept; it does not run before the page:

```cypher
UNWIND range(1, 10) AS x
WITH x ORDER BY x LIMIT 3
WHERE x > 2
RETURN collect(x)   // [3]
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

A `CALL` can stand alone or follow other clauses. Standalone, `YIELD` is
optional and every result column is returned. Inside a query, the procedure
runs once per input row, and a procedure that returns results must name the
ones it uses with `YIELD`; only a procedure with no results may be called
without it. Each yielded column has the type the procedure declares.

<!-- doctest: skip -->
```cypher
UNWIND ['1', '2'] AS s
CALL uni.number.parseInt(s) YIELD result
RETURN s, result + 1 AS next
```

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
    let (result, profile) = db
        .session()
        .query_with("MATCH (p:Person) WHERE p.age > 25 RETURN p.name")
        .profile()
        .await?;

    println!("Total time: {}ms", profile.total_time_ms);
    println!("Rows scanned: {}", result.metrics().rows_scanned);
    println!(
        "Planning: logical {:.3}ms, physical {:.3}ms over {} plan(s)",
        profile.logical_planning_ms, profile.physical_planning_ms, profile.physical_plans
    );

    // Per-operator statistics
    for op in &profile.runtime_stats {
        println!("{}: {:.3}ms, {} rows", op.operator, op.time_ms, op.actual_rows);
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
- **runtime_stats** (Python: `operators`): Per-operator breakdown with timing and row counts
- **logical_planning_ms** / **physical_planning_ms** / **physical_plans** (Rust): planning cost.
  `physical_plans` is one for the query plus one per sub-plan built while it runs, so a
  correlated `CALL { ... }` body planned once per outer row shows up as a count that grows
  with the rows
- **peak_memory_bytes**: Maximum memory used during execution

The result's `metrics()` carries the query-level counters, including **rows_scanned**
(rows examined by scans, before filtering). A variable-length pattern with an inline
property map, such as `-[:R*1..3 {w: 1}]->` or `->(b:N {x: 2})`, scans every edge of the
type before walking from the anchor, and that scan is included in `rows_scanned`.

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

// With an upper hop bound
MATCH path = shortestPath((a:Person)-[:KNOWS*1..6]-(b:Person))
WHERE a.name = 'Alice' AND b.name = 'Bob'
RETURN path, length(path) AS hops

// Maximum hops only (at most 5 hops)
MATCH path = shortestPath((a)-[:KNOWS*..5]-(b))
RETURN path

// Zero-length path (source equals target with min_hops=0)
MATCH path = shortestPath((a)-[:KNOWS*0..3]-(a))
RETURN path
```

**Hop Constraint Semantics:**
- `*` or `*1..` — No *written* upper bound. The planner supplies a default of **100 hops**
- `*1..6` — Between 1 and 6 hops
- `*..5` — At most 5 hops (1 to 5)
- `*0..` — Zero or more hops (allows source == target)
- A lower bound above 1 (`*2..6`, `*3..`) is rejected with an error: the search
  returns the first, shortest match, so honouring a higher minimum would need a
  different search.

**When there is no path.** A mandatory `MATCH` drops a pair that has no path
between its endpoints, as in openCypher and Neo4j; `OPTIONAL MATCH` keeps the
row with the path `NULL`. This applies to `shortestPath`, `allShortestPaths`
and the unnamed form `MATCH shortestPath(...)`. A node is its own target only
through the zero-length path, so `(a)-[*]->(a)` finds nothing and `*0..` finds
the path of length 0.

```cypher
// Only the people Alice can reach
MATCH (a:Person {name: 'Alice'}), (b:Person)
MATCH p = shortestPath((a)-[:KNOWS*]->(b))
RETURN b.name, length(p)

// Everyone, with hops NULL for those she cannot reach
MATCH (a:Person {name: 'Alice'}), (b:Person)
OPTIONAL MATCH p = shortestPath((a)-[:KNOWS*]->(b))
RETURN b.name, length(p)

// With an unbound target, each person gets the people they reach,
// or one row with NULLs if they reach nobody
MATCH (a:Person)
OPTIONAL MATCH p = shortestPath((a)-[:KNOWS*]->(b:Person))
RETURN a.name, b.name, length(p)
```

A `WHERE` on the path (`WHERE length(p) > 2`) filters the shortest path that was
found; it does not search for a longer path that would pass.

**A property map on the relationship restricts the search.** The result is the
shortest path whose every relationship matches the map, not the shortest path
filtered afterwards. A map value may be computed from the row — the endpoints
or variables from earlier clauses — and each row is searched under its own
values; a `NULL` value matches no relationship.

```cypher
// Shortest route over roads of the traveller's own class
MATCH (a:City {name: 'Oslo'}), (b:City {name: 'Rome'}), (t:Traveller {id: $id})
MATCH p = shortestPath((a)-[:ROAD* {class: t.class}]-(b))
RETURN length(p)
```

A value that names the path or its relationship variable is refused.

!!! warning "Changed in the next release"
    Up to 4.2.0 a mandatory `MATCH` kept every no-path pair as a row with a
    `NULL` path, and an `OPTIONAL MATCH` with an unbound target returned a
    `NULL`-path row for every unreachable target. Queries that relied on that —
    for example counting rows to count candidate pairs — should use `OPTIONAL
    MATCH`.

!!! warning "An omitted upper bound means 100, not infinity"
    A pattern with no upper bound is planned with a maximum of 100 hops. If
    the traversal actually reaches that ceiling with more graph left, the query
    returns a **warning** saying the results are incomplete — check
    `result.warnings` when a `[*]` answer looks short. On a chain 150 vertices
    long, `-[:R*]->` finds paths up to length
    100; writing `-[:R*1..140]->` finds paths up to length 140. An explicit
    bound above 100 is honoured, so write one whenever the graph might be
    deeper than that.

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

CHECK enforcement covers a single `property <op> literal` comparison, where `<op>` is one of `=`, `!=`/`<>`, `>`, `<`, `>=`, `<=` and the literal is a single whitespace-free token. Spacing around the operator does not matter — `REQUIRE p.age>=18` and `REQUIRE p.age >= 18` are the same constraint. (Before, the expression was split on whitespace and had to be exactly three tokens, so the unspaced form was silently never enforced.) An expression that does not reduce to those three tokens — one joined with `AND`/`OR`, for instance — is still accepted and listed by `SHOW CONSTRAINTS`, but it logs a warning and permits the write rather than rejecting it, so do not rely on it to enforce a compound rule. Split it into one constraint per comparison instead. Do not point the right-hand side at a second property either: `REQUIRE p.year >= p.min_year` reduces to three tokens but compares against the *string* `p.min_year`, and the operands cannot be ordered, so the write fails with a comparison error rather than either passing or reporting a constraint violation. Numeric comparisons, including `=` and `!=`, coerce across `Int` and `Float`, so `REQUIRE p.score = 5` holds for a stored `5.0`. A missing property passes; use `IS NOT NULL` to require presence.

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

### Running administrative procedures

`uni.admin.compact`, `uni.admin.snapshot.create`, `uni.admin.snapshot.restore`,
`uni.admin.snapshot.delete` and `uni.admin.verifyEdgeEndpoints` change the database as a whole, so they are
not reads: `session.query()`, a session prepared query, a pinned session, a
read-only handle and a `VERSION AS OF` statement refuse them with the read-only
error. Run them with `session.run()` (Rust `Session::run`, Python
`session.run()`, the `uni query` CLI), which executes them outside any
transaction — `snapshot.restore`, `snapshot.delete` and `verifyEdgeEndpoints`
refuse to run inside one. An `AuthzPolicy` sees the action `dbms` for them. `compactionStatus()` and
`snapshot.list()` are reads and run anywhere.

### Compaction

```cypher
// Trigger storage compaction
CALL uni.admin.compact()
YIELD tables_optimized, fragments_removed, fragments_added, bytes_reclaimed, duration_ms

// Check compaction status
CALL uni.admin.compactionStatus()
YIELD l1_runs, l1_size_bytes, in_progress, pending
```

### Edge endpoint verification

A store written by an earlier release may hold edges whose endpoints violate
their type's declared labels, so the planner does not narrow an unlabelled
endpoint of those types until they are verified:

```cypher
// Every unverified declared edge type, or one by name
CALL uni.admin.verifyEdgeEndpoints('WORK_AT')
YIELD edge_type, edges_checked, source_violations, target_violations, conformance
```

A type with no violation is recorded `verified` and saved at once; one with
violations stays `unverified` and its edges stay readable (delete them, then
verify again). The whole LDBC SF1 store (15 types, 13M edges) verifies in about
12 s. Run it outside a transaction: it waits up to 10 s for transactions opened
before it to finish, then fails. A fork created, or a snapshot taken, before
the verification does not use it, and restoring such a snapshot withdraws it.

Verification is no longer what keeps `MATCH ()-[r:R]->()` fast. When the
source cannot be narrowed to a label (an unverified type, a type with several
declared source labels, `[:R|S]`, or no type), the query reads its sources
from the relationship type's adjacency rather than scanning every vertex, with
the same results. At LDBC SF1, `MATCH ()-[w:WORK_AT]->() RETURN count(w)` on an
unverified store went from about 6 s to about 0.12 s. Inside a read-write
transaction every vertex is still read, so that concurrent changes to them
conflict as before.

Opening a store that an earlier release wrote to after a type was `enforced`
or `verified` (an unflushed WAL segment or a flush from that release) sets
every such type back to `unverified`, logs a warning naming them, and saves
that: the earlier release did not check endpoints. Verify again to restore
narrowing.

A `SET` or `REMOVE` cannot change a relationship's endpoints or type: a map
carrying `_eid`, `_src`, `_dst` and `_type` that names other ones than the
stored edge is refused with `EdgeIdentityChange` (Python
`UniEdgeIdentityError`).

### Snapshots

```cypher
// Create a named snapshot
CALL uni.admin.snapshot.create('before_migration')
YIELD snapshot_id

// List available snapshots
CALL uni.admin.snapshot.list()
YIELD snapshot_id, name, created_at, version_hwm

// Restore to a snapshot (data and schema), by id
CALL uni.admin.snapshot.restore($snapshot_id)
YIELD status

// Delete a snapshot, releasing the storage versions it pins
CALL uni.admin.snapshot.delete($snapshot_id)
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

Create and modify schema at runtime via procedures. They are schema writes,
not reads: run them with `session.run()` (an auto-commit transaction) or in a
transaction; `session.query()` refuses them. An `AuthzPolicy` sees the action
`schema`.

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
