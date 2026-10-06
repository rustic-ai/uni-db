# uni-db 4.2.0

**Release focus: the same question gets the same answer.** A result could change when nothing
about the question changed: naming a relationship or leaving it anonymous, renaming a variable with
`WITH`, asking in Locy rather than Cypher, running at a different batch size, or whether a
background flush had landed yet. Most of these were silent wrong answers at default settings. They
were found by comparing equivalent formulations of the same query (W2–W6 in
`docs/proposals/`), not by user reports.

53 commits since 4.1.1.

---

## ⚠️ Behaviour and API changes

This release changes **results** for some queries that were previously answered differently, and
has a small number of **source-level Rust API changes**. They are listed first so that upgrading
callers can check them.

### Results that change

**`sum` of nothing is 0, as in Cypher.** A `sum` over no rows, or over only NULLs, returned NULL
(SQL's rule) on the DataFusion and Cypher-value paths but 0 on the row executor, so the answer
depended on the plan. It is now `0` (`0.0` for a float column) everywhere: grouped `sum`, windowed
`sum(...) OVER`, and Locy `FOLD SUM` / `MSUM`. `min`, `max` and `avg` of nothing stay NULL. A query
that tested `sum(x) IS NULL` to detect an empty group needs `count(x) = 0` instead.

**An anonymous variable-length relationship yields one row per path.** `MATCH (x)-[:R*2..2]->(e)
RETURN count(*)` over a diamond returned 1, while the same query with `[r:R*2..2]` returned 2.
openCypher binds a row per matched path whether or not the relationship is named, and both forms
now agree. Under `DISTINCT`, `EXISTS`, `min`/`max`, or a set-semantic Locy rule, where multiplicity
cannot be observed, the cheaper reachability search is still used.

**Locy keeps value types.** `FOLD MIN`/`MAX`/`MMIN`/`MMAX`, `COLLECT`, `ALONG` columns and
properties yielded from an unlabelled node or a relationship were cast to `Float64`, so an integer
came back as `1.0`, and above 2^53 the value itself was wrong. They now keep their argument's type.

**A Locy condition that cannot be evaluated is an error.** `QUERY`, `DERIVE`, `EXPLAIN RULE` and
`ABDUCE` `WHERE` clauses treated an evaluation error or a non-boolean as "drop the row". They now
raise; `true` keeps a row, `false` and NULL drop it. `QUERY ... WHERE` also uses three-valued logic
now: `WHERE a.age <> 0` no longer keeps rows whose `age` is NULL.

**A dynamically typed non-boolean in a boolean context is a type error.** `NOT n.b` kept a row whose
`b` was `1`, and `CASE WHEN n.b` took its `ELSE`. Both now raise, as Cypher does. A NULL is still
NULL.

**A user map is not a node.** Any map with an `_id`, `_vid` or `vid` key was treated as a vertex, so
`{_id: 0, x: 1} = {_id: 0, x: 2}` was true and `collect(DISTINCT ...)` merged them. A map is now an
entity only if it has an entity's full shape (`_labels` for a vertex; a type and endpoints for an
edge). `_id` is a common key in imported JSON, so this is likely to affect real data.

**A non-`DETACH` `DELETE` refuses a node with relationships created in the same transaction.**
`CREATE (a)-[:R]->(b)` followed by `DELETE a` in one transaction passed the check and cascaded the
edge away, behaving as `DETACH DELETE`.

**Locy compiler rejects two programs it used to accept.** A `FOLD` clause's `YIELD` item that is
not a `KEY`, not a fold output and not an expression over one is `UngroupedFoldYield` (`KEY` marks a
single item, so `YIELD KEY o, e, pct` declares `e` as a plain column, which was silently dropped;
write `KEY o, KEY e`). A `QUERY` that reads a variable its rule does not yield is
`UnknownQueryVariable`. A literal seed into a `COUNT` fold now counts as one row and raises the
`count_fold_seed_counted` warning; seed with `NULL AS n` to declare a key without counting it.

**Smaller Locy evaluator changes.** `/` and `%` error only for integer zero (a float zero gives IEEE
infinity or NaN). `toInteger` / `toFloat` of an unparsable string is NULL. An ordering comparison
between types with no order is NULL, not false. `^` of a non-number is a type error, not `0.0`.
These match Cypher.

**`UniConfig::parallelism` takes effect.** It had no reader; every query ran with DataFusion's
default partition count. It now sets `target_partitions`. Its default is the CPU count, which is
also DataFusion's, so only callers who set it see a change.

### Rust API

- **`UniConfig` has a new public field, `execution_batch_size: Option<usize>`.** Code that
  constructs `UniConfig` with a struct literal and no `..Default::default()` must add it.
- **`LocyCompileError` has two new variants**, `UngroupedFoldYield` and `UnknownQueryVariable`. An
  exhaustive `match` must handle them.
- **`uni-store`: `StorageManager::materialized_row_count` is now `existing_row_count`** and takes an
  `&L0Context`, so it counts unflushed rows as well (see the `NOT NULL` fix below).
- **`uni-query`: `VlpOutputMode::EndpointsOnly` is split** into `Reachability` (the old search) and
  `EndpointsPerPath` (the new default).

---

## New

- **`execution_batch_size`** (`UniConfig` field and Python `config` key) sets the number of rows
  per batch inside the query engine (DataFusion's `batch_size`). `None`, the default, keeps 8,192.
  It never changes results; a smaller value bounds per-batch memory. `batch_size` is unchanged and
  sizes the pages a query cursor returns; it was previously documented as an execution setting,
  which it never was.
- **Label disjunction in expressions.** `WHERE n:A|B` and `n IS :A|B` were parse errors, though a
  pattern accepted `:A|B`. They now mean `n:A OR n:B`. Mixing disjunction with conjunction
  (`n:A:B|C`) is refused.
- **Locy `QUERY ... RETURN` aggregates** group as Cypher's `RETURN` does. `count(*)` failed with
  "unsupported expression: Wildcard" and `sum(x)` returned `x`. `SKIP` and `LIMIT` accept any
  expression; `LIMIT $n` was silently ignored.
- **A Locy rule body may join an expression and an `IS` reference with `AND`.** `WHERE x AND NOT a
  IS r TO b` failed to parse. An `OR` before the reference is still refused.
- **TCK harnesses read `UNI_TCK_PARALLELISM` and `UNI_TCK_EXECUTION_BATCH_SIZE`**, so the existing
  Cypher and Locy expectations double as a determinism check across engine settings.

---

## Silent wrong answers — Cypher

### OPTIONAL MATCH

- **A multi-step `OPTIONAL MATCH` emitted extra NULL rows** beside its real matches, one per dead
  end (`OPTIONAL MATCH (a)-[:R]->(b)-[:S*]->(c)`), or extended rows an earlier path had already
  failed, returning `[6, 7]` where `[6, NULL]` was due. Each traversal inside the clause decided
  "no match" for the rows in front of it, which is exact only for the first step. The clause now
  decides once, at its end, per entering row.
- **Rows entering an `OPTIONAL MATCH` that bound the same node were merged**, so after an `UNWIND`
  one of two NULL rows disappeared, and identical entering rows collapsed to one. Each entering row
  now carries its own id.
- **A chunked optional traversal** (one input batch expanding past the execution batch size,
  e.g. an `OPTIONAL MATCH` over a hub) emitted a NULL row per chunk for every input row whose
  matches were in another chunk: 90 rows for 70.
- **An entity an `OPTIONAL MATCH` did not bind was a struct of NULLs**, not NULL. `labels(b)`
  failed, `keys(b)` returned `[]`, and `[b, 1]` or `{k: b}` became NULL as a whole.

### Patterns

- **Inline element predicates were ignored.** `(m WHERE m.id = 3)` and `[r WHERE r.w = 1]` parsed
  but never became a filter, in Cypher and in Locy rule bodies. Inside `EXISTS { MATCH ... }` they
  failed with `UndefinedVariable`. They are now applied, and refused on a variable-length element,
  where they would apply per edge.
- **A relationship after a variable-length one could reuse its edges.** `(a)<-[:K*1..2]-(x)-[:K]->(b)`
  walked back out along the edge it came in by: 221 rows where brute force gives 104. Fixed for
  Cypher and for Locy rule bodies.
- **Reachability under `DISTINCT` / `EXISTS` was order-dependent.** An endpoint was dropped if the
  first predecessor found lay on a walk that reused an edge, even when a later one had a valid
  trail. The same query returned 95 to 103 of its 103 rows over 40 runs.
- **Pattern predicates and pattern comprehensions honoured only part of a pattern.** `WHERE
  (n:Person)-[:R]->()` also matched a `Robot`; relationship property maps were ignored; an edge
  could be traversed twice. A pattern now takes the vectorized path only when it uses features that
  path implements; the rest run as correlated subqueries.
- **A label disjunction on a traversal target** (`(n)-[:R]->(m:Robot|C)`) returned nothing: the
  labels were ANDed, then pinned to the first.

### Expressions and projection

- **`WITH b AS a` did not rebind `a`.** In a swap (`WITH b AS a, a AS b WHERE a.id > b.id`) the
  `a.*` columns still held the old `a`, returning 14 rows of 36. A property read through a chain of
  renames (`WITH n AS a WITH a AS x RETURN x.id`) returned NULL.
- **`reduce` truncated floats** to the accumulator's type: `reduce(s = 0, v IN [1.5, 2.5] | s + v)`
  returned 3. With a `null` start it could panic in Arrow.
- **Window `sum` truncated floats**: `sum(n.f) OVER (...)` over 0.5 and 1.25 returned 1.
- **`collect(DISTINCT ...)` keyed values by display string**, merging `1` with `'1'` and `[1]` with
  `['1']`. It now keys structurally, and in a hash set instead of a linear scan.

## Silent wrong answers — Locy

- **Recursive `FOLD` merged parallel edges and distinct paths** (#294). `MSUM` over edges of 30 and
  40 into the same node, plus 20 from another, returned 60 or 50 depending on batch order, never 90.
  The same collapse one level down — two parallel equal-weight edges extending the same fact — gave
  one `ALONG` fact instead of two and a downstream `SUM` of 4 for 7. A derivation is now identified
  by every relationship it used and by the row it extended.
- **An ungrouped `FOLD` `YIELD` column was dropped without a diagnostic** (#293), and `QUERY ...
  RETURN e.uid` then read NULL. Now a compile error (see above).
- **Mutually recursive rules returned each other's facts.** `QUERY even` over `even`/`odd`
  returned odd's rows too, and which symptom appeared depended on hash-set order.
- **A non-recursive rule's facts were not deduplicated.** Three parallel edges gave `(a, b)` three
  times in `QUERY` results and `derived_facts`. A `FOLD` still counts every derivation, since it
  aggregates the bag before this point; `ALONG` facts stay one per path.
- **Fixpoint float rounding was applied to stored values**, so any magnitude below 1e-12 became 0:
  an `ALONG` product of 1e-7 × 1e-7 was stored as `0.0`. Rounding now applies only to comparisons.
- **A seeded `FOLD` column** failed with `concatenate arrays of different data types` when the seed
  and the folded input had different types (`MCOUNT(r)` beside an integer seed, or `MSUM(r.pct)`
  with a seed of `0`).
- **`QUERY ... ORDER BY` a column not returned** did nothing: the key was evaluated after
  projection and was NULL for every row.
- **An SLG goal binding** failed to match `3.0` against an integer key, and an aliased key
  (`YIELD KEY n.k AS k`) failed with "Variable 'k' not defined".

## Storage, search and schema

- **A chunked label scan dropped unflushed rows past a vid gap.** With 9,000 flushed rows, 60,000 of
  another label and 7 new ones, `MATCH (p:P) RETURN count(p)` returned 9,000. The walk asked flushed
  storage alone where the label's next row was.
- **Declaring a `NOT NULL` property on a label whose rows were all unflushed** recorded it as
  `NOT NULL`, after which every flush failed and those rows could not be written at all. Unflushed
  rows now count as existing, so the property is recorded nullable with a warning, as for flushed
  rows.
- **A vertex `UNIQUE` key stayed taken after it was freed** by an unflushed `DELETE`, a flushed
  `SET`, or an unflushed `SET` (which also tripped the commit-time SSI guard with "unique key
  already committed by a concurrent transaction").
- **Full-text and vector search returned a node through a value it no longer has**, both after an
  unflushed update and through the older row versions an append-only table keeps after a flush.
  A search is repeated with a wider fetch only when stale hits were dropped and left it short, so
  `ef_search` keeps its meaning.
- **`(n {ext_id: 'x'})` without a label** missed unflushed vertices, could not project properties
  (`No field named "n.name"`), and returned every property as a string. It is now planned as the
  ordinary vertex scan, with the `ext_id` equality pushed down to the index.
- **Pinned sessions reported `snapshot_reads: 0`** for queries that touched only edges or the main
  vertex table.

## Errors that are now answers

- `elementId(n)` on a scan- or traversal-bound variable failed with "No field named n".
- `EXISTS { MATCH (n:Person)-[:R]->() }` with `n` bound outside failed with
  `No field named "n._labels"`.
- A comprehension, quantifier or `reduce` inside a `CASE` branch failed with "Column references
  column 'v' at index 6 but input schema only has 2 columns", or, for a pattern comprehension,
  silently returned empty. `CASE WHEN ... THEN null ELSE reduce(...) END` failed at run time.
- `size([x IN xs | x]) * 1.5` failed with "Invalid arithmetic operation: Int64 * Float64".
- A repeated property predicate on a variable-length target failed in Lance with "Duplicate column
  name".
- A bare `WHERE n.b` on a schemaless boolean property failed to plan.
- A map compared with a scalar (`1 < {k: 1}`) failed to plan or failed at run time; it now follows
  openCypher (ordering NULL, `=` false, `<>` true).

---

## Testing

Every fix above has a regression test that fails with the fix reverted. Most were found by new
harnesses, which now run on every PR and nightly:

- **Query-rewrite relations** (`metamorphic::dqp::topo`) hold data and engine fixed and compare
  equivalent formulations: named ↔ anonymous relationship, `OPTIONAL MATCH` ↔ `MATCH` ⊎ `NOT
  EXISTS`, `EXISTS` ↔ `COUNT > 0`, `*i..j` ↔ unrolled hops, `DISTINCT` ↔ grouping, aggregates ↔
  `reduce` over `collect`, `UNWIND` relations, and five Locy-vs-Cypher relations. The fixture has
  self-loops, parallel edges, cycles and a dense cluster, and runs flushed and half in L0 at a
  two-row batch.
- **A wide DQP tier** puts a label across more than one scan slice with vid gaps, which is what the
  chunked-scan data loss needed and no earlier tier produced.
- **The Locy naive oracle** now evaluates `FOLD`, recursive `FOLD`, `ALONG`, `BEST BY` and `PROB`
  over bags, against random multigraph programs.
- **The DQP fork, pinned and flush levers** now run Locy programs, one per rule family.
- Debug builds assert that every Locy rule's facts are distinct rows and that two fold candidates
  with the same derivation key have the same input.

Black Book Appendix B2 records the invariants these fixes established, each with the test that
pins it.

## Dependencies and CI

- **`async-trait` 0.1.89 → 0.1.92 and `pyo3` 0.29.0 → 0.29.3** for Rust 1.99 clippy.
- **Six `wasmtime` 43 advisories** (RUSTSEC-2026-0314, -0316, -0321, -0322, -0323, -0327) are
  ignored in `cargo deny` with reasons. They are patched only in wasmtime 36.x, 48.x and 49.x, and
  `extism` 1.30.0 (the newest) requires 43. They must be re-tested on every `extism` upgrade.
- `docs/local_ci_runbook.md` gained the six steps the workflows ran and it lacked.
