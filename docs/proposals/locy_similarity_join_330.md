# Proposal: scoring a similarity pairing without per-pair overhead (#330)

Status: **closed**. Step 1 is implemented and the gate measured (§10); the
pattern predicate the measurement found is fixed (§10.6); the costs around the
pairing are removed (`locy_resolution_cost_2026_11_14.md`). Step 2 is not
built: the gate did not justify it. Step 3, the top-k generator, is the
subject of #335, since the number of pairs read is unchanged.
Date: 2026-11-13 (rev. 2); step 1 and §10 added 2026-11-14.

Issue: <https://github.com/rustic-ai/uni-db/issues/330>. Follow-up:
<https://github.com/rustic-ai/uni-db/issues/335>.
Scope: `uni-query` and `uni-query-functions` (`similar_to` /
`vector_similarity` evaluation) for step 1. Step 2, if built, adds one
operator and a planner rule in `uni-query`. Step 3 is deferred and would reach
`uni-plugin`, `uni-locy` and `uni-store`.

Revision 2 follows a review of rev. 1 against the code (§9). It changes three
things. Step 2 is no longer committed to: the measurements say step 1 takes
almost all of the cost, so step 2 waits for a re-measurement (§5). Step 2's
exactness no longer rests on a second scorer and a rounding slack, which the
review showed unsound; it evaluates the original predicate (§6.2). And several
statements about the code were wrong or loose and are corrected in §3.

## 1. Summary

A rule that pairs two labels by vector similarity,

```locy
CREATE RULE close AS
MATCH (m:Mention), (e:Entity)
WHERE similar_to(m.embedding, e.embedding) >= 0.99
YIELD KEY m, KEY e
```

runs as `FilterExec` over `CrossJoinExec`. #330 proposes reaching the vector
index from the rule body. Measuring the rule shows the cost is elsewhere:

- producing the pairs is cheap, scoring them is 97% of the time;
- probing the index once per mention, which Cypher can already do, is slower
  than scoring every pair at 1,000 × 1,000.

The plan:

1. **Score vectors straight from the Arrow buffers** in `similar_to` and
   `vector_similarity`. Same functions, same arithmetic, no per-pair
   conversion. Results are identical by construction.
2. **Re-measure.** Build the similarity join of §6 only if what is left
   matters.
3. **Deferred: an opt-in top-k generator** over the vector index, the only
   route to sub-quadratic cost.

No rule, grammar or result changes in steps 1 and 2.

## 2. What was measured

Debug build, 8-dimensional vectors, `VectorAlgo::Flat` indexes, the schema and
data of `crates/uni/tests/common/bugs/locy_similarity_cross_join_scale.rs`.

At 1,000 mentions × 1,000 entities:

| Run | Time | Pairs scored |
|---|---|---|
| Locy rule, `similar_to(...) >= 0.99` | 1.58 s | 1,000,000 |
| Locy rule, cheap predicate `m.k + e.k < 0` | 29 ms | 1,000,000 |
| Locy rule, `m.k % 10 = e.k % 10 AND similar_to(...) >= 0.99` | 203 ms | 100,000 |
| Locy rule, `m.k = e.k` | 50 ms | 0 |
| Cypher, the same cartesian `similar_to` | 1.53 s | 1,000,000 |
| Cypher, the same with `vector_similarity` | 1.67 s | 1,000,000 |
| Cypher, `MATCH (m:Mention) CALL uni.vector.query('Entity', 'embedding', m.embedding, 1)` | 2.42 s | 1,000 probes |

Per operator, first row: `CrossJoinExec` 38 ms for 1,000,000 rows,
`FilterExec` 1,478 ms.

At 300 × 300 with three values of `type`, the similarity clause of the
resolution program (`NOT (m)-[:REFERS_TO]->() AND m.type = e.type AND
similar_to(...) >= 0.5`, yielding `similar_to(...) * 0.8`):

| Operator | Rows | Time |
|---|---|---|
| `HashJoinExec` (on `type`) | 30,000 | 7.9 ms |
| `FilterExec` (pattern predicate and `similar_to`) | 2,792 | 44.0 ms |
| `ProjectionExec` (the `YIELD`, scoring again) | 2,792 | 3.8 ms |

Without the `NOT` pattern predicate the filter takes 40.8 ms, so that
predicate costs about 3 ms over 30,000 pairs here. That holds only because this
run did not declare the `REFERS_TO` edge type; with it declared the predicate
costs about 63 ms (§10.3).

Behaviour observed, which any change must keep:

- a NULL vector gives a NULL score, and the pair is dropped;
- a zero vector gives 0.0, not NULL and not an error;
- with an L2 index on the first argument's property `similar_to` returns
  `1 / (1 + d²)`, with a dot index the raw dot product, with no index cosine;
- `similar_to` and `vector_similarity` differ in the low digits for the same
  pair under cosine (0.05613529682159424 against 0.05613529687452645);
- the rule sees unflushed rows, through the same plan.

Not measured: a release build, 64-dimensional or larger vectors, an ANN index
(HNSW, IVF), the full resolution program. The crossover between scoring and
probing is therefore indicative only.

## 3. Why the plan is what it is

- **The cross join.** `MATCH (m:Mention), (e:Entity)` is two disconnected
  paths. A rule-body `WHERE` goes through `QueryPlanner::plan_where_clause`
  (`locy_planner.rs`, `build_clause`, step 2b), which pushes only
  single-variable conjuncts a scan can take and wraps the rest in one
  `Filter`. The rule reaches the physical planner as
  `Filter(CrossJoin(Scan m, Scan e))`. `plan_filter` turns that into a hash
  join only when the predicate has an equality across the two sides
  (`try_plan_cross_join_as_hash_join` returns `None` on
  `cls.equi_pairs.is_empty()`). Nothing here is specific to Locy.
- **With an equality.** The resolution clause has `m.type = e.type`, so it is
  a hash join on `type`. The similarity and the `NOT` pattern predicate are
  its residual, a `FilterExec` above the join (§2).
- **The scoring.** `SimilarToExecExpr::evaluate` loops over rows. For each row
  it turns the source and the query from Arrow into an owned `Value`, then
  each `Value` into a `Vec<f32>`, then calls `score_vectors`: about seven
  allocations per pair. Each call to `evaluate` also spawns a scoped thread
  for the auto-embed and full-text precomputation, whether or not the batch
  needs it. `score_vectors` computes `metric.compute_distance` before its
  match and discards the result for cosine. A declared vector is a
  `FixedSizeList<Float32>` column, for flushed and unflushed rows, and
  `score_vectors` already takes slices.
- **Three scorers, not one.** They do not agree, by design or by history:
  - `similar_to` in a plan (`SimilarToExecExpr`): the metric of the vector
    index on its first argument's `(label, property)`; failing that, the first
    vector index in the schema on that property name under any label; failing
    that, cosine (`resolve_metric_for_property`). The cosine is computed in
    f64, narrowed to f32, and widened again.
  - `vector_similarity` (a UDF over `eval_similar_to_pure`): always cosine,
    returned in f64 without narrowing. Its comment gives the reason: an exact
    0.8 rounded to f32 would pass `> 0.8`.
  - Locy's in-memory evaluator (`locy_eval.rs`), which serves DERIVE, EXPLAIN
    RULE, ABDUCE and QUERY re-evaluation: both names map to
    `eval_similar_to_pure`, so always cosine, whatever the index metric.
- **The index.** Three things keep a rule body from it, and the first is a
  decision, not a gap:
  - A threshold on a similarity is deliberately never planned as an index
    scan. The comment on `extract_knn_predicate` (`planner.rs`) gives the
    reason: an ANN index misses rows, each index scores with its own metric,
    and any top-K bound truncates a threshold answer.
  - `~=` is the top-K index scan and needs a constant query vector.
    `e.embedding ~= m.embedding` is refused at planning.
  - Cypher's per-row route is a correlated `CALL uni.vector.query(...)`. The
    rule grammar (`locy.pest`, `rule_definition`) has no `CALL`, and a
    generator cannot stand in: `LocyGenerator::generate` is synchronous, over
    argument columns, with no storage handle. No generator ships with the
    engine.

A correlated probe costs about 2.4 ms here. `GraphApplyExec` re-plans the
call for each outer row, and `StorageManager::vector_search` recomputes the
set of buffered vertices that shadow disk rows, the visibility filter and the
row-version check on every call. A `Flat` index is also a scan of every
vector, so with the indexes the repro and `nlp_resolve.rs` declare, a probe
skips no entity.

## 4. Step 1: score from the Arrow buffers

### 4.1 `similar_to`

In `SimilarToExecExpr::evaluate`, take a fast path when all of these hold:

- one source and one query, and no options argument;
- each child evaluates to a `FixedSizeList<Float32>`, as an array or a scalar,
  and the two have the same width.

The path:

- reads each row's vector as a `&[f32]` slice of the values buffer, honouring
  the array's offset;
- yields NULL where either validity bit is unset (a NULL slot's values are
  zeros, which would otherwise score 0.0);
- calls the existing `score_vectors` on the two slices with the metric already
  resolved at compile time, and widens the `f32` result to `f64` as today;
- does not spawn the precomputation thread, which only the auto-embed and
  full-text modes need.

Because it calls the same function on the same numbers, every score, including
0.0 for a zero vector and whatever a non-finite component produces, is the one
today's path returns. A scalar child is scored against every row without being
copied.

Everything else keeps today's path: a list of sources, an options map, a text
query, a `LargeBinary` or `List` child.

Separately, reorder `score_vectors` so that the cosine arm does not first
compute the distance it discards. For cosine `compute_distance` has no effect
on the result. `Hamming` and `Jaccard` panic inside it today; that order must
be preserved for the metrics that reach it.

### 4.2 `vector_similarity`

Give the UDF the same shape of fast path: both arguments
`FixedSizeList<Float32>`, validity honoured, and the score from
`cosine_similarity_inner` on the two `f32` slices, returned as `f64`. That is
the branch `eval_similar_to_pure` takes for two stored vectors today. It must
not share the `similar_to` path: the two functions return different numbers
(§3).

### 4.3 What step 1 covers

- The rule in §1: the 1,478 ms `FilterExec`.
- The resolution clause: the residual filter above the hash join, and the
  second scoring in its `YIELD`.
- Cypher queries that score a stored vector against another stored vector.

It does not cover Locy's in-memory evaluator, which works on `Value`s, and it
does not cover a query vector given as a literal or a parameter. Those compile
to a `List<Float64>` scalar (`List<Int64>` for integers, a CypherValue
`LargeBinary` when the list mixes the two, and always `LargeBinary` for a
`Value::Vector` parameter), so the common `similar_to(d.embedding, $q)` keeps
the `Value` path. Widening the fast path to a scalar list is a follow-up: for
`similar_to` the list narrows to `f32` once and the scores stay identical; for
`vector_similarity` a list selects the f64 branch, which needs the stored
vector widened per row.

## 5. The gate before step 2

After step 1, rerun the timing probe and the 300 × 300 clause, in a release
build as well, and at a realistic embedding width (384 or 768).

What is left of the cross-join plan is `CrossJoinExec` itself (38 ms per
million pairs at 8 dimensions) and the arithmetic. The cross join copies every
column of both sides for every pair, vectors included, so its share grows with
the embedding width and with the number of properties the rule reads.

Build step 2 only if, at the size we need, the cross join's copying is a
material share of the rule's time or its output does not fit the memory
budget. Otherwise stop at step 1 and restate the repro (§7).

## 6. Step 2 (conditional): `SimilarityJoinExec`

### 6.1 Recognition

In `plan_filter`, after `try_plan_cross_join_as_hash_join` declines. It
applies to `Filter(CrossJoin(L, R), pred)` when one top-level `AND` conjunct
of `pred` is a comparison between a similarity call and a threshold, in either
order, where:

- the call is `similar_to` with exactly two arguments, or `vector_similarity`;
- one argument reads only variables of `L` and the other only variables of
  `R`;
- each argument, compiled against its own side's schema, has the type
  `FixedSizeList<Float32>`, with equal widths. (The compiled type, not the
  schema declaration: a variable read whole reaches the filter as a struct
  column, and its property is then a field access.)
- the threshold is a numeric literal or a parameter. Not any constant
  expression: `is_simple_evaluable` accepts `rand()`, which is per-row today.
- the filter has no optional variables.

Everything else keeps today's plan. No metric is special-cased, because §6.2
never computes a score itself.

When the conjunct reads only one side, as it does for
`MATCH (m), (e), (x) WHERE similar_to(m.v, e.v) >= t` at the outer cross join,
push it into that side as a `Filter`, so that the inner
`Filter(CrossJoin(m, e))` is recognised in turn. Today nothing pushes a
one-sided conjunct down unless a cross-side equality exists.

### 6.2 Execution

`L` is the buffered side, as it is for `CrossJoinExec` and for the hash join,
so the side held in memory does not change.

1. Collect `L` with `collect_accounted`, which charges the memory pool.
   Evaluate `L`'s argument over it once: one vector array for the build side.
2. Compile the conjunct once against a two-column schema holding the two
   arguments, using the same merged translation context `plan_filter` uses, so
   the metric resolves as it does today.
3. For each probe batch of `R`, evaluate `R`'s argument. For each probe row,
   evaluate the compiled conjunct over the build vector array with the probe
   vector as a scalar. The result is a boolean mask over the build rows.
4. Emit the pairs the mask keeps, with `take` on both sides, in batches of the
   session's batch size.

The conjunct that decides a pair is the one that decides it today, evaluated
by the same code. There is no second scorer, no rounding slack, and nothing to
keep in step. Step 1's scalar fast path is what makes step 3 cheap: no vector
is copied to score it.

The output schema is `L`'s columns then `R`'s, as `CrossJoinExec`'s. The other
conjuncts of `pred` stay in a `FilterExec` above the join, with the same
endpoint hydration `plan_filter` applies today. They are not pushed into the
scans: `ReadSetRecordingExec` records the rows that survive a scan's own
filter, so pushing a conjunct down would narrow the transaction's read set.
The hash-join path does push them; matching it is a separate decision
(§8, question 2).

### 6.3 Deadline

`DeadlineGuardExec` is inserted beneath pipeline breakers, and only in
subtrees made of stock DataFusion operators and an allow-list of this
project's (`subtree_is_insertable`). A new operator is not on the list, so
every breaker above it would lose its checkpoint. The operator must be added
to the list, on the same argument as `ReadSetRecordingExec`, and must check
the deadline itself while it collects `L` and once per probe row.

### 6.4 What it buys

Only what §5 measures: the columns of rejected pairs are never copied, and at
most one batch of survivors is in flight. The arithmetic is step 1's.

### 6.5 Later, if arithmetic dominates

A blocked kernel that scores a probe batch against the build matrix can
pre-select candidates before step 3 of §6.2. It is sound only if it never
drops a pair the conjunct would keep. That needs a per-metric error bound that
depends on the data (a dot product's rounding error grows with the magnitudes)
and it must pass every non-finite score through. `lance-linalg` has batch
distance functions and is already a workspace dependency of `uni-store`. This
is out of scope until a measurement asks for it.

## 7. Step 3 (deferred): an opt-in top-k generator

<!-- doctest: skip -->
```locy
MATCH (m:Mention)
WHERE nearest(m.embedding, 'Entity', 'embedding', 10) -> (e, score)
```

This is the direction the issue names, and the only one whose cost does not
grow with `|Mention| × |Entity|`. It changes the answer: a match outside the
top `k`, or one an ANN index misses, is not found. That has to be the rule
author's choice, so it is a construct they write, not a rewrite of the
threshold.

It needs four things that do not exist:

- **A host generator.** A generator kind that runs asynchronously with the
  `GraphExecutionContext`, beside the pure columnar `LocyGenerator`.
- **A node output.** The binding plan gives every generator output the scalar
  kind (`uni-locy/src/binding_plan.rs`). `e` must bind a node, materialised as
  a positive `IS` reference's `BindNode` position is.
- **A batched search.** One `StorageManager` call for a batch of query
  vectors, computing the shadow set, the visibility filter and the row-version
  check once.
- **A read-set entry.** The generator reads a label without a scan, so it must
  record that read itself for the commit-time conflict check.

`score` must be defined against one of the three scorers of §3, and say which.

Build it only when the earlier steps have been measured at the target scale
and found too slow, and measure it against an ANN index: with `Flat` it does
the join's arithmetic.

## 8. Tests, documentation and open questions

### Tests

- **Step 1 equivalence.** `similar_to` and `vector_similarity` through the
  fast path against the `Value` path, on random vectors, for cosine, dot, L2
  and L1: NULL on either side, a zero vector, a sliced array (non-zero
  offset), a scalar on either side, a non-finite component, and a threshold
  equal to a pair's exact score under `>=` and `>`. The existing
  `vector_search/vector_predicate_exactness.rs` must still pass.
- **The repro.**
  `similarity_rule_does_not_pair_every_mention_with_every_entity` asserts on
  `CrossJoinExec` rows, so it still fails after step 1 and passes after
  step 2. Its comment expects about `N × k` candidates, which only step 3
  gives. At the gate, restate it for whichever step is last: a plan-shape
  check if step 2 is built, otherwise retire it and keep the timing probe.
  Its `ON_DEMAND` entry in `scripts/ci/ignored_tests.py` follows.
- **The timing probe.** Rerun
  `similarity_pairing_time_grows_with_the_product_of_both_sides` after each
  step and record the numbers here.
- **Step 2, if built.** The join against today's plan for the Locy rule and
  the Cypher query: each shape of §6.1, each decline, three patterns, a fork's
  own rows, an uncommitted transaction's rows, and a deadline that expires
  mid-join. A registry entry and a proving test in
  `crates/uni/tests/common/plan_shape/`.

New tests go in the existing `crates/uni/tests/integration.rs` binary.

### Documentation

- `docs/UNI_BLACK_BOOK.md`: the fast path, and the operator if built.
- `website/docs/features/nlp-extraction.md`: the cost note on the resolution
  program.
- `docs/proposals/kniv_v7_nlp_extraction_2026_11_07.md` §6: it says fixing
  the 1,000 × 1,000 timeout "needs a nearest-neighbour generator over the
  vector index". Point it here.

### What this does not fix

- The new-entity command's `NOT EXISTS { MATCH (o:Mention) ... }` is a
  mention × mention self-join in Cypher; the issue measures it at 6.8 s for
  1,000 mentions.
- At the 0.5 threshold the issue's similarity clause keeps 209,257 of
  1,000,000 pairs, and each is a fact that `MNOR` and `BEST BY` process.
- The `NOT (m)-[:REFERS_TO]->()` pattern predicate was evaluated per joined
  pair in the residual filter, and with the edge type declared it was the
  largest cost of the similarity clause (§10.3). Fixed in §10.6.
- Locy's in-memory evaluator scores `similar_to` with cosine whatever the
  index metric (§3). With a non-cosine index, a rule's facts and its EXPLAIN
  RULE or DERIVE re-evaluation could then disagree. This was read from the
  code and not reproduced; it predates this proposal.

So the full resolution program may still exceed the 30 s Locy timeout at
1,000 × 1,000 after step 1.

### Open questions

1. **Literal and parameter query vectors.** Answered in §4.3: a `List<Float64>`
   scalar, not covered. Should a follow-up cover it?
2. **One-sided conjuncts.** Should the similarity join push them into the
   scans as the hash-join path does, accepting the narrower read set, or leave
   them above as §6.2 says?
3. **Blocking by an equality.** With `m.type = e.type` the plan stays a hash
   join with a scored residual. Is step 1 enough for that case, or should the
   similarity join take the equality keys as a partition?
4. **Non-finite scores.** Nothing rejects a non-finite vector component, and
   how a NaN score compares with the threshold was not run. Step 1 and §6.2
   inherit today's behaviour whatever it is; should it be pinned by a test or
   changed?

## 9. Review record (rev. 1 → rev. 2)

Rev. 1 was checked by a second reader against the code and by a probe.
Corrections made:

- **Exactness.** Rev. 1 confirmed candidate pairs with "the slice scorer" and
  a threshold loosened by a slack. `similar_to` and `vector_similarity` return
  different numbers, a fixed slack is not a bound for a dot product, and
  non-finite scores would have been dropped. Rev. 2 evaluates the original
  conjunct (§6.2).
- **Threshold.** Rev. 1 hoisted any `is_simple_evaluable` constant, which
  includes `rand()`. Rev. 2 takes a literal or a parameter.
- **Metric.** "Cosine when there is none" omitted the fallback to another
  label's index on the same property name.
- **"The two cannot drift."** Locy's in-memory evaluator is a third scorer.
- **Read set.** Pushing one-sided conjuncts into the scans narrows it; rev. 1
  said it was unchanged.
- **Build side.** Rev. 1 buffered `R`; today's operators buffer `L`.
- **Deadline.** Rev. 1 assumed an in-loop check was enough; the operator must
  also join the guard's allow-list.
- **Zero vectors.** They score 0.0 and must be kept; rev. 1 handled only NULL.
- **Three patterns.** Rev. 1's recognition declined them.
- **Recognition by declaration.** Replaced by the compiled type.
- **Allocation count.** About seven per pair, not four, plus a thread per
  batch.
- **Step 2's standing.** Rev. 1 committed to it. Its own measurement (38 ms of
  1,522 ms in the cross join) does not support that, hence the gate.

Confirmed unchanged: the plan shape, the hash join with a residual filter for
the resolution clause, the vector column type, and the statements about the
index, the grammar and generators.

## 10. Step 1 as built, and the gate

### 10.1 What was built

- `F32Rows` (`uni-query-functions/src/similar_to.rs`): the rows of a
  `FixedSizeList<Float32>` array or scalar as `&[f32]` slices of its values
  buffer.
- `score_fixed_size_lists` (`uni-query/src/query/df_graph/similar_to_expr.rs`),
  taken by `SimilarToExecExpr::evaluate` under the conditions of §4.1.
- `stored_vector_similarity` (`uni-query-functions/src/df_udfs.rs`), in
  `invoke_similarity_udf`, so it serves the `vector_similarity` UDF and the
  `similar_to` UDF, which are the same function there. It needs at least one
  array argument; two scalars keep the old path and its scalar result.
- `score_vectors` no longer computes a distance it discards for cosine.

Tests: unit tests beside each function compare the fast path with the `Value`
path bit for bit (cosine, dot, L2, L1; NULL, zero vector, sliced array, scalar
on either side, NaN and infinite components), and
`crates/uni/tests/common/vector_search/similarity_fast_path.rs` checks Cypher
and the Locy rule end to end under no index, cosine, L2 and dot, flushed and
unflushed, including a threshold equal to a pair's exact score.

### 10.2 The rule of §1, before and after

`similarity_pairing_time_grows_with_the_product_of_both_sides`, 1,000 × 1,000,
one run each on one machine. "Cross join" and "filter" are the operators' own
times.

| Build | Width | | Rule | Cross join | Filter |
|---|---|---|---|---|---|
| debug | 8 | before | 1.55 s | 31.7 ms | 1,483 ms |
| debug | 8 | after | 231 ms | 29.5 ms | 151.8 ms |
| debug | 384 | before | over the 30 s timeout | | |
| debug | 384 | after | 2.68 s | 220 ms | 2,180 ms |
| release | 8 | before | 285 ms | 23.2 ms | 244.9 ms |
| release | 8 | after | 50 ms | 22.1 ms | 12.7 ms |
| release | 384 | before | 1.96 s | 159.5 ms | 1,751 ms |
| release | 384 | after | 526 ms | 153.8 ms | 327 ms |

The release "after" runs shared the machine with the clause probe, so read
them to about ±10%.

### 10.3 The resolution clause, 300 × 300, three types

`resolution_similarity_clause_operator_times`. The residual `FilterExec` over
30,000 joined pairs:

| | `REFERS_TO` undeclared | `REFERS_TO` declared |
|---|---|---|
| debug, before (§2) | 44.0 ms | not measured |
| debug, after | 7.5 ms | 68.8 ms |
| debug, after, without the pattern predicate | 4.6 ms | 4.5 ms |
| debug, after, without the similarity | 2.3 ms | 62.9 ms |
| release, after | 1.5 ms | 8.1 ms |
| release, after, without the similarity | 0.4 ms | 6.5 ms |

The similarity's part of the filter fell from about 41 ms to about 4.5 ms, and
the `YIELD`'s second scoring from 3.8 ms to 0.5 ms. The pattern predicate
`NOT (m)-[:REFERS_TO]->()` costs about 2 µs per pair in a debug build once the
edge type is declared, against under 0.1 µs when it is not. §2 measured it
undeclared. There is no "before" for the declared case or for a release build
of this clause.

### 10.4 The gate (§5)

The cross join's share of the rule after step 1 is 44% at 8 dimensions and 29%
at 384 in a release build. As a share that is material. In absolute terms it
is 22 ms and 154 ms per million pairs, and the whole rule runs a million pairs
in 50 ms and 526 ms against a 30 s timeout. The cross join streams its output
in batches, so no memory limit was met.

Step 2 can save at most the cross join's time. What is larger:

- at a realistic width, the arithmetic: 327 ms of 526 ms. That is §6.5's
  kernel, not §6's join;
- in the resolution clause, the pattern predicate (§10.3), which step 2 does
  not touch.

Recommendation: do not build step 2 now. Decide it together with the repro
test: `similarity_rule_does_not_pair_every_mention_with_every_entity` still
fails after step 1, as §8 said it would, and its `ON_DEMAND` entry is
unchanged. If step 2 is not built, §8 says to retire that test and keep the
probes.

Not measured: an ANN index, and widths above 384.

### 10.5 The full resolution program

`nlp_resolve::resolution_program_stage_times`: the program of
`website/docs/features/nlp-extraction.md` over `n` entities and `n` mentions of
one type, half of them surnames of entities and half new names, 64-dimensional
auto-embedded vectors, after step 1. One run each. The steps are the program's
own, run one at a time in one transaction; "whole program" is one `locy` call
on a fresh database.

| | debug, 300 | debug, 1,000 | release, 300 | release, 1,000 |
|---|---|---|---|---|
| Whole program | 3.82 s | 29.33 s | 0.83 s | 7.17 s |
| First `DERIVE refers_to` | 1.49 s | 12.19 s | 0.39 s | 3.10 s |
| `MERGE` of new entities | 0.92 s | 10.40 s | 0.20 s | 1.73 s |
| Second `DERIVE refers_to` | 1.46 s | 13.30 s | 0.28 s | 2.51 s |
| `MERGE` of leftovers | 6 ms | 19 ms | 1 ms | 5 ms |

The steps sum to more than the whole program because running them apart
evaluates the rules twice from scratch.

Inside the two DERIVE steps at 1,000 × 1,000:

| | debug | release |
|---|---|---|
| First pass, `match_evidence` (three clauses, 1,000,000 pairs each) | 2.35 s | 0.65 s |
| First pass, `same_as` | 4.89 s | 1.11 s |
| First pass, outside the strata | 4.8 s | 1.3 s |
| Second pass, `match_evidence` (1,500,000 pairs each) | 10.09 s | 1.39 s |
| Second pass, its two slowest `FilterExec` | 4.87 s, 4.59 s | 0.55 s, 0.52 s |
| Second pass, `same_as` | 1.02 s | 0.22 s |
| Second pass, outside the strata | 2.1 s | 0.9 s |

What it says:

- In a debug build the program is at the 30 s timeout, not under it. In a
  release build it has a factor of four to spare. There is no measurement of
  the whole program before step 1.
- Scoring is no longer where the time goes. The first pass scores 1,000,000
  pairs in the filter and 209,257 again in the `YIELD`, and all three clauses
  together take 0.65 s in release.
- The second pass is slower per pair than the first, with fewer facts. The
  difference is the `REFERS_TO` edges the first pass wrote: the
  `NOT (m)-[:REFERS_TO]->()` predicate then costs about 3 µs per joined pair
  in debug and 0.35 µs in release, in every clause (§10.3). It depends on `m`
  alone, so it could be decided once per mention, before the join.
- The new-entity `MERGE`, a mention × mention `NOT EXISTS`, is the next
  largest step. Nothing in this proposal touches it.
- Two costs were not expected and not looked into: `same_as`, whose profiled
  operators account for under a quarter of the rule's time, and the time a
  DERIVE step spends outside the strata.

None of these is what step 2 or step 3 addresses.

### 10.6 The pattern predicate, fixed

`classify_join_predicate` sorts a join's conjuncts by the side whose variables
they read, and pushes one-sided ones beneath the hash join. It did not look
inside a pattern predicate, so `NOT (m)-[:REFERS_TO]->()` read no variable,
was taken for a constant and stayed in the residual filter. It is now
classified by the variables its pattern names (`conjunct_variables`,
`df_planner.rs`) and applied to the mention's side as a `Filter` beneath the
join (`filter_join_side`): once per mention. A pattern with a property map or
an inline `WHERE`, and one that names both sides, stay above the join. This is
a planner change for every Cypher query and rule of that shape, not only this
program. It applies only where an equality makes the plan a hash join; a
cross join without one still pushes nothing down (§6.1).

Same probes, same machine, one run each:

| 1,000 × 1,000 | debug, before | debug, after | release, before | release, after |
|---|---|---|---|---|
| Whole program | 29.33 s | 21.16 s | 7.17 s | 5.86 s |
| Second `DERIVE refers_to` | 13.30 s | 4.44 s | 2.51 s | 1.45 s |
| Second pass, `match_evidence` | 10.09 s | 1.03 s | 1.39 s | 0.30 s |

At 300 × 300 the whole program went from 3.82 s to 3.09 s in debug and from
0.83 s to 0.73 s in release. The clause of §10.3 with `REFERS_TO` declared:
its filters went from 68.8 ms to 2.1 ms on the mention side plus 4.6 ms above
the join in debug, and from 8.1 ms to 1.0 ms plus 0.5 ms in release.

What is left at 1,000 × 1,000 in a debug build, of 21.2 s: the first DERIVE
step (12.6 s, of which `same_as` is 5.1 s and about 5 s is outside the strata)
and the new-entity `MERGE` (11.5 s run on its own).

Where that goes, and a design for it:
`docs/proposals/locy_resolution_cost_2026_11_14.md`.

Tests: `crates/uni/tests/common/bugs/pattern_predicate_join_side.rs` pins the
answers of one-sided and two-sided pattern predicates in Cypher and Locy,
flushed and unflushed, an uncommitted edge, and that the join emits only the
pairs of mentions the predicate keeps.

