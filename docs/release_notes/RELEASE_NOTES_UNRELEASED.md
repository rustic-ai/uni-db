# uni-db — unreleased

Changes on `main` since 4.2.0 that are not yet in a release. Rename this file to
the version it ships in.

---

## ⚠️ Behaviour and API changes

### Results that change

**A `shortestPath` pair with no path is dropped by a mandatory `MATCH`, as in
openCypher.** `MATCH p = shortestPath((a)-[:R*]->(b))` (and `allShortestPaths`)
used to return a pair whose endpoints are not connected as a row with `p` NULL.
It now returns no row for that pair, as openCypher and Neo4j do; `OPTIONAL MATCH`
keeps the row with `p` NULL. The same applies to a NULL endpoint and to a source
equal to its target under `*1..` (a node reaches itself only through the
zero-length path of `*0..`). A query that counted rows to count candidate pairs,
or tested `p IS NULL` after a mandatory `MATCH`, should use `OPTIONAL MATCH`.

**`OPTIONAL MATCH` with an unbound `shortestPath` target is null-extended per
entering row.** `MATCH (a) OPTIONAL MATCH p = shortestPath((a)-[:R*]->(b:N))`
returned one NULL-path row for every `N` node `a` does not reach, beside the ones
it does. It now returns only the reachable targets, or a single row with `b` and
`p` NULL when `a` reaches none of them. An entering row with no candidate target
at all (an empty label, or a property map nothing satisfies) used to be dropped;
it now gets its NULL row.

**`n.prop ~= q` returns as many neighbours as the `LIMIT` it feeds.** The
search was capped at a hardcoded 100, so `RETURN n LIMIT 500` silently returned
100 rows and `SKIP 100 LIMIT 20` returned none. `K` is now `SKIP + LIMIT` when the
search feeds that `LIMIT` directly (projections, or `ORDER BY n._score DESC`);
elsewhere — no `LIMIT`, a residual filter such as `~= q AND n.x > 3`, a traversal,
another `ORDER BY` — it is the new `UniConfig::vector_knn_default_k` (default 100,
also a Python `config()` key), and a search that fills it adds a result warning.
`~=` on a variable it cannot replace the label scan of (bound behind a `LIMIT`, a
relationship endpoint, a row-dependent query vector, under `OR`) is now a planning
error naming `vector_similarity`; `MATCH (n) WITH n LIMIT 5 WHERE n.emb ~= q` used
to return the 5 nearest neighbours of the whole label. Under `OPTIONAL MATCH`, `~=`
now uses the index and yields a NULL row when nothing qualifies (it was
previously left unrewritten, reaching an expression compiler that does not
support `~=`).

**Equality on an indexed property no longer loses or rejects edge-case
operands.** On a property with a scalar index, `n.s = '2021-06-01T00:00Z'` on a
`STRING` property returned no rows (the literal was rewritten as a timestamp
before reaching storage), `n.f = -0.0` on a `FLOAT` property missed the `-0.0`
row, `n.f = $p` with `$p` NaN or infinite failed, and `n.x = $p` with `$p` the
smallest 64-bit integer failed with `could not convert to literal of type
'Int64'`. All four now return what the unindexed query returns.

**`-null` is null in `SET` expressions.** `SET n.f = -n.f` failed with
`Cannot negate non-numeric value: Null` on a row where `f` is NULL; it now sets
NULL. Negating the smallest 64-bit integer is an overflow error instead of a
wrapped value.

**Float comparisons follow openCypher, not IEEE total order.** `-0.0 = 0.0`
was false and `-0.0 < 0.0`, `0 > -0.0` true; `n.f < 0.0` returned a row holding
`0.0 / 0.0` (a NaN with its sign bit set sorted below every number), `n.f =
n.f` held for a NaN row, the join `n.f = m.f` paired a NaN row with itself and
missed `-0.0` with `0.0`, and `-0.0` and `0.0` were two groups under
`DISTINCT`, grouping, `count(DISTINCT …)` and `UNION`. Now `-0.0` equals `0.0`,
every comparison involving `NaN` is false except `<>`, and `-0.0`/`0.0` (and all
NaNs) are one group; a group of zeros shows `0.0`.

**A list mixing integers and floats keeps each element's type.** `RETURN $l`
with `$l = [1, 1.5]` returned `[1.0, 1.5]` (so did the literal `[1, 1.5]`), and
with `$l = [9007199254740993, 1.5]` both `9007199254740992 IN $l` and
`n.x IN $l` for `x = 9007199254740992` matched, because the integer was rounded
to a float. Such lists now keep their integers; numeric lists passed as query
vectors (`~=`, `vector_similarity`, `uni.vector.query`, VECTOR writes) answer
as before.

**`WITH … ORDER BY … SKIP … LIMIT … WHERE` filters after the page.**
`UNWIND range(1, 10) AS x WITH x ORDER BY x LIMIT 3 WHERE x > 2` returned
`[3, 4, 5]` (the filter ran before `LIMIT`); it now returns `[3]`, as the
openCypher grammar and spec define. A `WITH DISTINCT` whose `WHERE` follows a
`SKIP`/`LIMIT` and reads a variable the projection dropped is now a
`VariableNotDefined` error.

**A list expression over a long variable-length path result no longer fails.**
`[r IN rs | r.k]`, `all(r IN rs WHERE …)` and other comprehensions over a
relationship list failed with "all columns in a record batch must have the same
length" once the traversal produced more than one batch of rows for its input.

### New

**Range, prefix and null predicates use scalar indexes.** On a property with an
Online BTree index, `<`, `<=`, `>`, `>=` (and `AND`-ed pairs), `STARTS WITH`
(literal or parameter) and `IS [NOT] NULL` narrow the scan through the index
instead of reading the whole label; a Hash index serves `IS [NOT] NULL`. A
selective range over 32,000 rows went from 37 ms to 7-8 ms. A range that matches
most of the label is about 1.5x slower than before (measured 60 ms against 38 ms
at 32,000 rows), the cost of re-checking each returned row against newer
versions. See *Performance Tuning → Use Predicate Pushdown*.

**The unnamed form `MATCH shortestPath(...)` plans.** It failed with
`shortestPath must be assigned to a variable`; it now matches like the named form.
