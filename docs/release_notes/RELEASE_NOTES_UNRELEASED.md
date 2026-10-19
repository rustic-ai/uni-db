# uni-db — unreleased

Changes on `main` since 4.2.0 that are not yet in a release. Rename this file to
the version it ships in.

---

## ⚠️ Behaviour and API changes

### Results that change

**Integers and floats compare exactly; `1` and `1.0` are one value.** An
integer compared with a float was widened to `f64`, which rounds above 2^53:
`9007199254740993 = 9007199254740992.0` was true, and orderings across the
boundary were wrong. This held on every path: literals, parameters, typed and
schemaless properties, the row evaluator, joins, `IN`, `ORDER BY` and
`min`/`max`. Comparisons are now exact, and the integer form of
a float bound keeps index pushdown (`n.i < 2.5` is `n.i <= 2`).

- `DISTINCT`, grouping, `UNION`, `count(DISTINCT)` and `collect(DISTINCT)` over a
  column mixing integers and floats now treat `1` and `1.0` (and `0`, `0.0`,
  `-0.0`) as one value, and keep the first one seen. They were separate rows.
- `RETURN 1 AS x UNION RETURN 1.0 AS x` failed to plan ("mismatched schemas").
  An integer column against a float column now unions as Cypher values, each
  row keeping its type.
- An inline edge map on a variable-length path over a schemaless edge type
  (`[:R*1..2 {w: 0}]`) and a node map in a pattern predicate
  (`WHERE (a)-->(:B {w: 1})`) compared structurally: `{w: 0}` missed `w = 0.0`,
  and a `NaN` matched a `NaN`. In a pattern predicate, `{k: null}` matched a
  node with no `k`; it now matches nothing. Both use Cypher equality now.
- An aggregate over `DISTINCT` sees the first value of each class, as
  `RETURN DISTINCT` keeps it. `sum(DISTINCT x)` over `[1.0, 1]` was `2.0` (the
  two were not deduplicated) and is `1.0`; `collect(DISTINCT x)` over
  `[1.0, 1]` was `[1]` and is `[1.0]`; `min`/`max(DISTINCT)` likewise.
- `MERGE (:K {k: 9223372036854775808.0})` matched an existing node with
  `k = 9223372036854775807`: the float `2^63` was folded into `i64::MAX`.
  It now creates the node. Batched `UNWIND ... MERGE` deduplicated the two the
  same way.
- A statement run whole by the fallback executor (a standalone `CALL` of a
  registered procedure, followed by `RETURN`) compared numbers by widening
  too, and folded `2^63` into `i64::MAX`: `min`/`max`, `DISTINCT`, grouping
  and `UNION` were wrong at the boundary. On that path only `count` honored
  `DISTINCT`; `sum`, `avg`, `min`, `max`, `collect` and the percentiles
  folded every row. Both are fixed.
- Locy: a `QUERY`'s `ORDER BY`, `min`/`max` and `DISTINCT` (including
  `count(DISTINCT)` and `collect(DISTINCT)`) compare numbers exactly and
  treat `1` and `1.0` as one value, as Cypher does. `ORDER BY` and
  `min`/`max` also order values of different types as Cypher does; they tied
  and kept arrival order.
- Locy: `BEST BY` over a column of mixed or schemaless numbers chose by the
  values' encoded bytes, so every integer ranked below every float
  (`BEST BY v` over `5` and `1.5` chose `5`) and negative numbers ranked
  above positive ones. It now chooses by value, in recursive rules too,
  where a new candidate is weighed against the fact already held for its KEY.

**Cypher operators answer the same on literals and on properties, per
openCypher.** Literals are typed statically and evaluated by Arrow kernels;
schemaless properties are evaluated per row. The two paths disagreed, and one of
them was often wrong:

- `n.missing AND null` panicked. `null AND n.b`, `'ab' + null`, `true - null`,
  `duration('P1D') * null`, `1 =~ n.missing` and similar failed to plan, and
  `'ab' ^ n.missing` failed at run time. All are now NULL.
- `n.s =~ 'ab'` over a schemaless property failed to plan. It now matches.
- `true OR n.i` over a non-boolean `n.i` was true when every row of the batch
  had a true left operand, and a type error otherwise. `AND`/`OR` now follow
  Kleene logic per row, in either operand order. A dominating operand decides
  the answer whatever the other one is: `false AND x` is false and `true OR x`
  is true. Otherwise a non-boolean operand is a type error, beside NULL too:
  `NULL AND n.i` and `n.missing XOR n.i` are now errors where the row path
  answered NULL.
- `2 ^ 3` is `8.0` everywhere. The interpreted evaluator, which Locy also uses,
  answered `8`.
- `date - date` is a duration on the literal path too. It was an integer day
  count.
- `datetime + duration`, `datetime - datetime` and `duration * number` now plan
  over literals.
- `n.s + 1` concatenates (`'ab1'`) on the row path, as the literal path already
  did. A float or a duration concatenates its `toString`: `'a' + 1.0` is
  `'a1.0'` and `'a' + duration('P1D')` is `'aP1D'`, not `'a1 days'`.
- A number is no longer read as a duration in microseconds. `datetime(..) + 1`
  and `date(..) - 1` are type errors on every path. Before, the row path moved
  the value by 1 µs, so `date - 1` returned the previous day.
- `STARTS WITH` / `ENDS WITH` / `CONTAINS` with a non-string operand is NULL in
  the interpreted evaluator too. It was an error there.
- `localdatetime(..) = datetime(..)` was true for equal instants on literals.
  `'ab' = localdatetime(..)` failed parsing `'ab'` as a timestamp. Both are now
  incomparable types (`=` false, `<` NULL), as the row path answered. A zoned
  `TIMESTAMP` column still compares with a datetime and a string as an instant.
- `-localdatetime(..)` negated the epoch offset. It is now a type error.
- **A silent wrong answer.** `n.dur * n.k`, with a schemaless duration and a
  declared `INT` column, was NULL. A vectorized fast path treated every
  non-numeric value as NULL. It now hands such a batch to the evaluator, which
  answers the duration (or the type error, for `n.s - n.k`).

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

**`ABDUCE NOT` validates the edit it reports.** A `RemoveEdge` candidate was
validated by deleting every edge of its type, and a `ChangeProperty` by setting
the property on every node (for an edge variable, on every node instead of the
edge), so a removal that leaves the goal derived could come back `validated`.
Each candidate now carries the identity of the one entity it edits —
`RemoveEdge.eid` / `source_vid` / `target_vid` and `ChangeProperty.vid` / `eid`
(new fields; also in the Python dicts) — and validation applies only that edit.
A relationship with neither a bound variable nor bound endpoints is no longer
proposed. Property candidates are no longer repeated under helper names such as
`a._all_props`. `source_var` / `target_var` of a removal now follow the edge's
direction, so `(b)<-[:R]-(a)` reports `a` as the source.

**Positive `ABDUCE` proposes and validates one concrete edge.** An `AddEdge`
candidate named only the rule's variables and was validated by adding an edge
of its type between every pair of nodes, so an edit no single edge achieves
(a two-hop rule) came back `validated`, at a cost quadratic in the node count.
Each candidate is now one edge between two existing nodes, carried in the new
`AddEdge.source_vid` / `target_vid` fields (also in the Python dicts), and
validation adds exactly that edge. Endpoints are enumerated from the nodes
carrying the hop's endpoint labels in id order, filtered by the goal's `WHERE`
when it reads only the hop's endpoint variables, up to
`max_abduce_candidates`. `source_var` / `target_var` follow the edge's
direction, so `(b)<-[:R]-(a)` adds `a -> b`.

**Locy provenance attributes a fact to its clause by column name, and fails
instead of guessing.** EXPLAIN's clause attribution compared a fact with each
clause's candidates by column position and gave clause 0 to a fact it could
not match; the TopKProofs MNOR support map was keyed the same way and, on a
miss, folded the group as independent. Both now resolve columns by name and
fail with an error on a missing column.

**Locy exact probability honours a renamed FOLD output.** With
`exact_probability`, a probabilistic FOLD whose output was renamed in YIELD
(`FOLD prob = MNOR(e.p) YIELD ..., prob AS pr`) never received its exact (BDD)
probability for shared-proof groups: the group silently kept one derivation's
weight (0.7 on the reachability diamond, whose exact value is 0.745 — see
below). The exact path now resolves columns by name and fails with an error
instead of skipping.

**Locy: a positive `IS` reference to a PROB rule carries its probability
(breaking: probability values change).** The row holds only when the
referenced fact does, so the fact's probability is now multiplied into the
calling row's PROB value, as `IS NOT` multiplies `1 - p`; it was a Boolean
filter. On the reachability diamond `s -0.7-> t`, `s -0.3-> m -0.5-> t` with
`FOLD prob = MNOR(e.p)` in both clauses, `reach(s,t)` was
`1 - (1 - 0.7)(1 - 0.3) = 0.79`, which treated `reach(m,t)` as certain; it is
`1 - (1 - 0.7)(1 - 0.3 · 0.5) = 0.745`, the probability that `t` is reachable
when each edge is present independently. With MPROD it was 0.21 and is 0.105.
- **Count once.** A clause whose PROB expression already reads the referenced
  probability as one product factor (`FOLD p = MNOR(p)`,
  `MNOR(e.w * p)`) is unchanged. Any other read (`1 - p`, `p * p`) is not
  multiplied either and raises the new runtime warning
  `ProbabilityReadNotFactor` (`probability_read_not_factor`).
- **Exact mode** (`exact_probability`) evaluates each fact under
  possible-worlds semantics: the OR (MNOR) or AND (MPROD) of its rows, each row
  its own event AND the formula of every fact it references. Two rows over the
  same expression and the same nodes and relationships are one event. It used
  to treat a recursive row as an independent fact of its own weight, so it
  never corrected anything a positive reference shared.
- **`SharedProbabilisticDependency` fires only on shared evidence** — two
  proofs over one edge or one referenced fact, or a cycle — not on any
  recursive clause. The diamond above no longer warns (its proofs share no
  edge); `A -0.5-> B -0.4-> C -0.6-> D` plus `A -0.3-> C` does, and exact mode
  gives `reach(A,D) = 0.264` against the independence 0.2784. Without a
  warning the two modes agree.
- **A renamed MNOR/MPROD output is a PROB column.** `FOLD prob = MNOR(..) YIELD
  prob AS pr` left `pr` unmarked, so neither `IS` nor `IS NOT` read it as a
  probability.

**A property named after a storage column is refused at write.** `CREATE`,
`SET`, `MERGE`, `Transaction::bulk_insert_*`, `BulkWriter`, the streaming
appender and the Python bindings now fail with `UniError::InvalidIdentifier`
(Python `UniInvalidIdentifierError`) for a property named `overflow_json`,
`_vid`, `_uid`, `_eid`, `_deleted`, `_version`, `_labels`, `_created_at` or
`_updated_at`. Such a write used to be accepted and then misread: `overflow_json`
read NULL while `properties()` held it, and the `_` names were dropped on a
declared label, hidden from `keys()` on a schemaless one, or shadowed by the
system value (`n._vid` returned the vertex id). The other storage column names
(`ext_id`, `eid`, `src_vid`, `dst_vid`, `op`, `type`, `labels`, `props_json`)
round-trip and stay writable. Schema DDL already refused all of these.

**A property whose name starts with `_` reads back.** Only the engine's own
names are reserved now (`uni_common::core::schema::is_reserved_property_name`,
one rule for writes and reads). Any other `_`-prefixed property — `_source`,
`__tag` — was accepted on write and then hidden by every read path that
treated a leading `_` as internal: it was missing from `keys()`,
`properties()` and `n {.*}` on vertices and relationships, flushed or not,
and `n {._foo}` failed to plan on a declared label. It now appears in every
read form, as `n._foo` and `WHERE n._foo = …` already did. In exchange the
reserved set grew to the map-encoding keys and planner columns the engine gives
a meaning (`_type`, `_type_name`, `_src`, `_dst`, `_src_vid`,
`_dst_vid`, `_all_props`, `_label`, `_score`, `_fwd`, Lance's `_rowid`-family
row columns, `__set_struct__`,
`__with_passthrough__`, and names starting with `_gen_`, `__fde_` or
`__alias_of__`): a write of one is refused with `UniError::InvalidIdentifier`.
Such a property was already hidden on read, and each of these names is one
the engine reads with its own meaning (`_type`, or `_src` with `_dst`, is how
an entity map is recognized as an edge).

**`MERGE` keyed on an undeclared property of a declared label works after a
flush.** `MERGE (n:N {nick: 'q'})`, where `N` is declared and `nick` is not,
failed with `No field named nick` once `N` had been flushed: the batched key
lookup filtered the label's table by a column the property does not have. Such
a pattern now takes the general MERGE path, which reads undeclared properties.

**`keys()` and `properties()` of a path element list its properties.**
`keys(relationships(p)[0])` returned `["properties"]` and
`properties(relationships(p)[0])` a map nesting them under `properties`,
while `relationships(p)[0].w` read the value.

**A flushed relationship no longer reports its storage blob.** On an edge type
declared with no properties, every flushed edge carrying an undeclared property
listed `overflow_json` in `keys(r)`, `properties(r)` and the returned
relationship, as a list of byte values, and a `SET` on it staged that entry
back as a property.

**A trigger sees a flushed vertex's properties as it sees an unflushed one's.**
The `old_value` pre-image of a vertex read back from storage carried
`overflow_json`, `_deleted`, `_uid`, `_created_at` and `_updated_at`, and its
undeclared properties sat nested under `overflow_json`.

**An `ext_id` given up can be taken again.** Once its holder was flushed, an
`ext_id` the holder later changed, removed or lost by deletion was still
reported as taken, permanently (`ext_id 'x' already exists`), on CREATE, SET,
batched CREATE and bulk insert.

**A bulk load on a fork is staged, and an interrupted one no longer damages
primary.** On a fork, `BulkWriter::abort()` failed (`rollback_table on a forked
backend is not supported`) and left the aborted rows on the fork, and the rows
were visible to the fork before `commit()`. Worse, an interrupted fork load
(crash, or a writer dropped without commit or abort) made the next open roll
**primary** tables back to the versions they had when the load began, losing
primary writes made meanwhile, and drop primary tables the load had seen
absent. A fork now stages its load on its own branches, exactly as primary
does, and writes no database crash marker. A fork load that fails part-way
through `commit()` can leave its per-label tables published and its main
table not; nothing reconciles a fork at reopen.

**A slow background flush no longer fails a commit with `CommitTimeout`.** The
time-based auto-flush (`auto_flush_interval`) held the commit lock for the
whole Lance write, and a commit waits at most `commit_timeout` (5 s) for that
lock. With async flush on (the default) it now writes Lance outside the lock,
as a commit's own flush trigger already did.

**Background work started on a fork stops with the database.** Async trigger
fires and index-rebuild workers started through a fork session were tracked
by a shutdown handle nothing ever signalled: `shutdown()` and `drop` neither
stopped nor waited for them, and `background_task_count()` did not count
them. They are now counted, signalled and waited for like the primary's, and
the fork's own workers stop when its last session is dropped.

**A bulk load with `async_indexes(true)` no longer leaves a worker behind.**
Each such load started its own index-rebuild worker, which ran until shutdown,
so N loads left N live tasks; the load now queues on the database's one
rebuild worker, and `indexes().rebuild_status()` sees its tasks.

**Concurrent commits that defer a trigger no longer queue on each other's
disk writes.** With a local data path, each `Defer` outcome rewrote and fsynced
the deferral sidecar while holding the queue lock. Eight threads committing 25
deferrals each took 6.6-10.2 s at a median of 243-385 ms per commit, and the
queue was unreadable for up to 0.66 s at a time; they now take 2.2-2.7 s at
51-83 ms, with the queue readable within 3 ms. A commit still returns only
once its deferral is on disk.

**A procedure called inside a query runs, and its yields keep their types.**
- `MATCH … CALL p(x) RETURN …` without `YIELD` was dropped from the plan: the
  procedure never ran. A procedure without results now runs once per row and
  keeps each row once. A procedure with results must name them with `YIELD`
  inside a query, as openCypher and Neo4j require; this is now a
  `SyntaxError` (it used to be silently skipped). A standalone `CALL p()` is
  unchanged.
- A per-row `YIELD` of a plugin procedure was always a string, so
  `UNWIND ['1'] AS s CALL uni.number.parseInt(s) YIELD result RETURN result + 1`
  answered `'11'`. Yields now take the procedure's declared type, as the
  standalone call's do.
- A `WRITE` procedure called per row failed with `inner write requires a
  writer-enabled procedure host`. Two equal input rows shared one call, so a
  writing or non-deterministic procedure ran once for both. A unit
  `CALL { … }` repeated its input row once per row its body made.

**A label check works on a node that arrives as one value.** `(a:L)`, `a:L`
and `EXISTS { MATCH (a:L)-->() }` on an `UNWIND` element (or any list item)
failed with `No field named "a._labels"`.

**`r['w']` over relationship types that share `w`.** When the relationship
could be either of two types that both declare `w`, `coalesce(r['w'], 0)`
failed with an ambiguous-field error.

**Quantified path patterns check the first inner node's label on every
iteration.** In `(a) ((x:L)-[:R]->(y)){1,3} (b)` the label on `x` was checked
only on the first iteration, and only when no outer node preceded the group,
so paths through nodes without `L` were returned. Without a quantifier,
`(a) ((x:M)-->(y)) (b)` ignored `:M`. An inner node label the schema does not
declare was ignored too, so `(y:Undeclared)` matched every node. A first inner
node with two labels is now refused, as for the other inner nodes. Under
`OPTIONAL MATCH`, a property map on the first inner node dropped the rows it
did not match instead of returning them with NULLs.

**A relationship matched again in a later clause is the same relationship,
schemaless.** `MATCH ()-[r:R]->() WITH r MATCH (x)-[r:R]->(y)` with `R`
undeclared paired every `r` with every `R` edge, or ignored the type.

**A quoted name is the same name everywhere.** `` MATCH (`n`) RETURN n ``
failed to plan, because the pattern bound `` `n` `` with its backticks while
`n` read the bare name. The same held for `UNWIND … AS`, `FOREACH`, list
comprehensions, `all/any/none/single`, `reduce`, `SET`/`REMOVE` targets, map
projections, `CALL` procedure names, index and constraint names, Locy
generator names and outputs, and the property names of `CREATE LABEL`,
`CREATE EDGE TYPE` and `ALTER … PROPERTY`: `` CREATE LABEL L (`my prop` STRING) ``
declared a property literally named `` `my prop` `` that `` n.`my prop` `` never
read. Index DDL on `` p.`a.b` `` took the name `` b` ``.

**Declaring a property whose name contains `.` or a backtick is refused.** The
storage layer cannot hold such a column. A dotted declaration used to succeed
and accept writes, and then every flush of the label failed; a backtick
declaration flushed, and then every read of the label failed with
`No field named`. Both names still work as undeclared properties.

**A refused `CREATE LABEL` / `CREATE EDGE TYPE` or schema `apply()` leaves
nothing behind.** A property refused part-way left the label (and the
properties before it) in the schema, so a corrected retry failed with
"already exists".

**A `WRITE` procedure's writes belong to the calling transaction.** The body of
a procedure declared with `uni.plugin.declareProcedure(..., 'WRITE', ...)`
wrote straight into the shared store: its writes survived `rollback()`, other
sessions saw them before `commit()`, and the transaction's own later reads did
not. This held for a standalone `CALL` and for a per-row `CALL` inside a query.
The writes now go to the transaction, so rollback discards them, commit
publishes them, reads in the transaction see them, and two transactions that
update the same node through a procedure conflict as plain writes do.
`Session::run` still auto-commits.

**An algorithm's list, map and node yields have their real type inside the
query.** They arrived as text that was parsed back only when rows were
returned, so `RETURN nodeIds` looked right while `size(nodeIds)` counted
characters, `UNWIND nodeIds` gave one string, `nodeIds[0]` and `x IN nodeIds`
failed, and `id(n) = node` (`articulationPoints`, `bridges`, `mst`, ...) never
matched. List, map and path yields are now Cypher values, and node and
relationship yields are integer ids.

**A `vector_similarity` threshold is an exact per-row filter.**
`WHERE vector_similarity(n.emb, $q) > t` was rewritten to a top-100 KNN over the
vector index that compared `t` against the index's native score rather than
the cosine similarity the function computes, so it returned at most 100 rows
(plus ties) and could keep the wrong ones. It now returns every node above the
threshold, and only those, with or without an index — and therefore scans the
label. Use `~=` or `uni.vector.query` for indexed top-K search.

**`percentileDisc` returns one of its inputs, exactly; both executors read
the percentile.** On the DataFusion path every input went through `f64`, so
`percentileDisc` over integers returned a float (`3.0` for `3`) and
`2^53 + 1` came back `9007199254740992.0`. It now returns the input itself,
with its type: an integer column gives an integer, a column of mixed type a
Cypher value. `percentileCont` still returns a float, now interpolated
between the exact inputs (halfway between `2^53 + 1` and `2^53 + 5` is
`2^53 + 3`, rounded once, where it was `2^53 + 2`). The fallback executor (a
standalone `CALL` of a registered procedure followed by `RETURN`) never read
the percentile argument, so both percentiles answered the 0th; it now reads
it, rejects one outside `[0, 1]` with `NumberOutOfRange`, and no longer turns a
float result such as `10.0` into the integer `10`. A percentile that is not a
number (`percentileDisc(x, 'a')`) is now an error on both paths; it was read
as `0`.

**A property declared on one label no longer types the same-named property
of another label.** A scan of an undeclared label, a multi-label pattern, or
`MATCH (n)` typed each property from the first label anywhere in the schema
that declared it. With `F.v` declared `FLOAT`, `MATCH (s:S) RETURN s.v` on the
schemaless `S` read an integer `9007199254740993` as `9007199254740992.0`, a
string as `NULL`, and `WHERE s.v = 9007199254740993` matched nothing. The
stored values were right; only the read was wrong. A property now takes a
declared type only from the scanned labels' own declarations, and is
otherwise read as the stored value. Locy's rule planner mirrored the old rule
and now mirrors the new one, and a Locy column of stored values brought to an
integer column is read exactly rather than through `f64`.

**Reachability over a variable-length pattern no longer lists trails.** An
`EXISTS { MATCH (a)-[:R*]-(b) }`, a pattern predicate, or `RETURN DISTINCT` /
`count(DISTINCT ...)` over `[:R*min..max]` is now answered from breadth-first
distances wherever those decide it: always for a lower bound of 0 or 1, and for
a higher one when every endpoint's shortest trail reaches it. On a cyclic graph
(every undirected one with an edge) the schema'd traversal searched for a trail
of each length up to the hop ceiling, and the schemaless one listed every
trail: an `EXISTS` over a 6-clique took 7 s to over 150 s in a debug build, and
now takes milliseconds. Answers are unchanged. The remaining search (a lower
bound above the shortest trail) stops at the number of relationships it can
reach, and no longer warns that an unbounded pattern was truncated at the hop
ceiling when no trail could have been that long.

**`range()` with a null argument returns null.** `range(0, null)`,
`range(null, 3)` and `range(0, 3, null)` raised `InvalidArgumentType … must be
an integer`; they now return null, as other scalar functions do for a null
argument, and `UNWIND range(0, null) AS i` produces no rows. A null wins over
every other check, so `range(null, 3, 0)` is null too. A non-null argument that
is not an integer still raises `InvalidArgumentType`, and a zero step still
raises `NumberOutOfRange`. All four evaluators agree: the DataFusion function,
the `UNWIND` evaluator, the fallback executor (whose errors now carry those
two codes; they were untagged) and Locy's `QUERY … RETURN`.

### API changes

**Minimum supported Rust version is now 1.96 (was 1.91).** The WASM plugin
runtime moves to wasmtime/wasmtime-wasi 49 (and extism 1.41), the first
releases carrying the fixes for RUSTSEC-2026-0352, -0359, -0362 and -0363;
wasmtime 49 declares `rust-version = "1.96.0"`. rustls moves to 0.23.46
(RUSTSEC-2026-0346 to -0349). Run `rustup update stable` before building.

**`ABDUCE` and `EXPLAIN RULE` inside a Locy `ASSUME … THEN` body are compile
errors.** An ASSUME returns the rows of its body's `QUERY` and Cypher commands;
an `ABDUCE` or `EXPLAIN RULE` in the body (or in a nested ASSUME's body) was
evaluated and its result silently dropped, so the program succeeded with no
abduction or explanation to show. Such a program now fails to compile with
`LocyCompileError::AssumeBodyCommandNotAllowed`. Run the command at the top
level after the ASSUME, or apply the hypothetical change in a transaction and
roll it back (`tx()`, mutate, run the command, `rollback()`), or in a scratch
transaction, whose writes are always discarded. Rules, `QUERY`, Cypher,
`DERIVE` and nested `ASSUME` remain allowed in a body.

**The in-memory CDC provider is no longer registered by default.** Every
database registered `MemoryCdcOutputProvider`, which kept every committed batch
until shutdown and made every commit extract CDC events (236.6 MB of RSS over
20,000 small commits, against 55.8 MB without it). A database now has no CDC
stream unless a provider is added. To keep the old behavior, add it explicitly
and keep a handle to read the batches back:

```rust
use uni_plugin_builtin::extras::{MemoryCdcOutputProvider, MemoryCdcPlugin};
let captured = MemoryCdcOutputProvider::new();
db.add_plugin(MemoryCdcPlugin::with_provider(captured.clone()))?;
// ... commits ...
let batches = captured.delivered();
```

`MemoryCdcOutputProvider` is no longer a unit struct: construct it with `new()`
or `Default`; it is a cloneable handle with `delivered()` and `delivered_len()`.
`MemoryCdcStream.delivered` is now `Arc<Mutex<Vec<CdcBatch>>>` (shared with the
provider that started the stream) instead of `Vec<CdcBatch>`.

**Rust `LocyResult::rows()` returns the first row-producing result.** It
returned the first `QUERY` result only, so a program whose first row-producing
command was an `ASSUME … THEN { … }` or a Cypher statement returned that later
`QUERY`'s rows, or `None`. It now takes the first `QUERY`, `ASSUME` or Cypher
result in program order, as the Python binding's `rows` already did. Use
`command_results()` to reach a specific command.

**A writable open waits for a compaction still running on the same local
path.** Dropping a `Uni` handle did not stop an in-flight background compaction,
so reopening the path at once ran two compactors on one store and could return
wrong answers. Compaction now holds a file lock, `<storage>/compaction.lock`,
for each pass, and a dropped handle cancels its pass and joins its background
tasks for up to 5 s. A writable open of a local store waits up to 30 s for that
lock and then fails with "another handle is still compacting"; remote stores are
unaffected. A cancelled pass reports `CompactionCancelled`.

### New

**Python: `Session.scratch()` and `AsyncSession.scratch()`.** They expose
Rust's `Session::scratch()`: a transaction with read-your-writes over a pinned
read base whose `commit()` is refused, so its writes are always discarded on
`rollback()` or drop. It is the cheap way to run a command such as `ABDUCE`
against a hypothetical change.

**The storage engine's caches can be sized.** Every table a database opens
reads through one Lance session whose metadata and index caches default to
1 GiB and 6 GiB. They fill as tables are flushed and read and are trimmed only
when full, so a long-running process that flushes often kept growing toward
that size (about 100–170 KB per flush in the W2 commit-loop probe). Two new
`UniConfig` fields set the capacities: `lance_metadata_cache_bytes` and
`lance_index_cache_bytes` (`None`, the default, keeps Lance's sizes; `Some(0)`
disables a cache). A fork shares its primary's caches. Both are accepted by
the Python `config({...})` dict, and `DatabaseMetrics` (Rust and Python) now
reports occupancy as `storage_metadata_cache_bytes` and
`storage_index_cache_bytes`. `StorageBackend` gains a defaulted
`cache_usage()` method.

**A correlated `EXISTS` or `COUNT` between two bound nodes runs once per
source.** `MATCH (a), (b) WHERE NOT EXISTS { MATCH (a)-[:R*1..]->(b) }`
executed its body once per `(a, b)` pair. When the body names one outer node
only as a bare `(b)`, rows that share every other import are now answered by
one execution with `b` unbound. Over 52 x 52 pairs the body ran 52 times
instead of 2,704: 1,383 ms → 105 ms with half the edges in L0 and a two-row
batch, 504 ms → 26 ms flushed with the default batch (release build). Answers are unchanged for every direction and
range, including bounded ones such as `*2..3`. A body that reads
every endpoint it imports (a label, a property map or a `WHERE` on each) keeps
the per-row evaluation, as do `COLLECT` and `OPTIONAL MATCH` bodies.

**`shortestPath` / `allShortestPaths` accept a relationship map computed per
row.** `MATCH (a), (b) MATCH p = shortestPath((a)-[:R* {w: a.x}]-(b))` was
refused; it now returns, for each row, the shortest path whose every
relationship satisfies the map with that row's values (Neo4j accepts these
too). Rows are searched per distinct value set; a `NULL` value matches no
relationship. A value naming the path or its relationship variable is refused.

**Quantified path pattern maps read only what the walk can reach.** A per-hop
map, `((x)-[:R {w: 0}]->(y)-[:R]->(z)){1,2}`, read every edge of the type
before an anchored walk; it is now read over the region the walk reaches, as
variable-length maps already were. From one anchor: 401 → 11 rows read at 400
`:R` edges, 1,601 → 11 at 1,600. Answers are unchanged.

**`x IN list` with the list repeated across rows is hashed once per run.** A
list from `WITH` or `collect()` carried over the rows of an `UNWIND` (the LDBC
IC5 shape) was scanned once per row. Each run of identical rows now builds one
hash index and probes it: with lists of 4,096 items over 8,192 rows the test
went from 613 ms to 91 ms (debug build, over the cost of carrying the lists).

**The doubled-backtick escape in quoted names.** `` `a``b` `` is the name
``a`b``, as in openCypher, for every kind of name, and `` $`p q` `` is a
parameter. A name containing a backtick could not be written before.
`get_label_info` / `get_edge_type_info` now count elements whose name contains
a backtick instead of refusing them.

**Locy `plan_time` includes ASSUME and ABDUCE planning, and reused strata are
not planned again.** `LocyResult::metrics().plan_time` (Python `plan_time_ms`)
used to cover only the program's first plan; the plans each ASSUME body and
ABDUCE candidate build for their hypothetical state are now added. A stratum
such a state reuses is no longer planned: 8 ASSUMEs over a 60-stratum program
that reuse every stratum went from 26.7 ms to 3.4 ms of planning.

**A recursive `BEST BY` merge costs its delta, not the whole fact set.** Each
merge re-sorted every fact with the new candidates; it now keeps the held best
row per KEY in an index and touches only the KEYs the candidates name. On a
40-layer graph of 19,500 facts the merge took 0.73 s instead of 4.4 s over the
run, with the same facts and deltas.

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

**Equality lookups on unflushed rows of an undeclared label and by `ext_id`
read only matching rows.** `MATCH (n:M) WHERE n.p = $v` on a label the schema
does not declare read every unflushed `M` vertex (2,000 / 8,000 rows) where the
inline `(n:M {p: $v})` read 1; the `WHERE` form now reads 1 too, as do `IN` and
`AND` with other conjuncts. `MATCH (n:L {ext_id: $x})` and
`WHERE n.ext_id = $x` on a declared label read every unflushed `L` row; they now
read 1.

