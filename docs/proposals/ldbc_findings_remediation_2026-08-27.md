# Remediation plan for the LDBC SNB findings — 2026-08-27

Running LDBC SNB Interactive against SF1 found more defects than percentiles.
This orders what remains.

## Ordering principle

1. **Wrong answers before crashes before gaps before speed.** A wrong answer
   poisons every measurement taken on top of it and is invisible by construction;
   a crash is loud and self-reporting.
2. **Anything that stops the system defending itself outranks optimization.** A
   query that cannot be bounded by a timeout or a memory limit turns a slow query
   into an outage. Fixing the bounds first also makes the remaining work safer to
   land.
3. Within a tier: cheap-and-unblocking first.

One track runs **in parallel from the start** rather than in a wave, because it
changes how everything else is found.

---

## Track 0 (parallel, start now) — the differential oracle

**Track E's E3 stages 3–4.** Every silent wrong answer in this session was found
by hand: the adjacency-direction bug, `IN` over entities, `ORDER BY`
non-determinism, the comprehension fail-open. A differential oracle against Neo4j
finds that class automatically, on every run, for all queries that execute.

It does not need all 14 to run — 9 do today, and comparing 9 is worth more than
comparing 0. Its value compounds with every later wave, which is why it should not
wait for them.

Deliverables: Neo4j oracle harness, per-query result comparison, SF1 percentiles
document.

---

## Wave 1 — silent wrong answers

### 1.1 `ORDER BY` over a traversal is non-deterministic — [#186]

The same binary returns `p1, p2` and `p2, p1` on alternating runs for a
tie-free sort key. Reproduced with this branch's other fixes reverted, so it is
independent of them. A plain scan-plus-sort is stable; the traversal is what
makes the difference.

**Severity is higher than "rows in the wrong order".** `ORDER BY … LIMIT n` over a
traversal can return the wrong *rows*, so the failure is not confined to
presentation. Highest-ranked item here.

Size: unknown until root-caused. Root-cause first, then estimate.

### 1.2 `extract_vid` is blind to `Value::Node` — verify, then fix

`crates/uni-query-functions/src/df_udfs.rs:1183` handles only the `Value::Map`
entity encoding, and it backs `startNode`/`endNode` (`:1140`, `:1174`). If a typed
`Value::Node` reaches it, that is a silent wrong answer of the same class as the
`IN` bug. **Unverified** — confirm with a targeted test before changing anything;
if it cannot be reached, leave it and say so.

Size: hours.

### 1.3 One canonical `Value → vid` extractor

Five implementations exist and disagree on `_id`, `_eid`, and `Value::Node`:
`df_graph/common.rs:257`, `executor/read.rs:349`, `recursive_cte.rs:264`,
`df_udfs.rs:1183`, `df_udfs.rs:4731`. Promote `common.rs:257` and delegate the
rest.

This is the recurrence fix for a defect class that has now produced three separate
bugs. Touches the write path, so it wants its own PR.

Size: 1–2 days.

---

## Wave 2 — the system cannot currently defend itself

These two are what turn #184 from "a bad query is slow" into "a bad query kills
the process". Landing them first makes Wave 3 safer.

### 2.1 Deadlines are cooperative only

IC12 returned after **571 s against a 120 s per-query budget**, with
`query_timeout` also set. IC3 and IC5 were cut off correctly in the same run, so
the mechanism works — but execution that never awaits cannot be preempted, and
IC12 spent 390+ s past its deadline in one such stretch at 40 GB RSS.

**No timeout in the system currently bounds a runaway query.** Recorded on #184;
deserves its own issue.

### 2.2 `max_query_memory` is a result-size cap — [#185]

`enforce_memory_limit` runs *after* `executor.execute(...)` and measures only the
finished result set, with a `size_of_val(v) + 64` estimate that ignores heap
bytes. Queries returning 10–20 rows passed it while peak RSS hit 39 GB and the OS
killed the process.

Fix: size a DataFusion `RuntimeEnv` memory pool from `max_query_memory` on
`df_session_template` (`query/executor/core.rs`). Honest caveat already recorded on
the issue: a pool only accounts operators that reserve through it, and #184's
abort comes from `MutableArrayData`, which does not — so this makes the limit real
and honestly named without being a fix for that crash.

Size: 2–3 days for both.

---

## Wave 3 — the crash

### 3.1 Column pruning — [#184]

A `collect()` list is not pruned once `UNWIND` consumes it; it rides through the
traversal and is replicated onto every fan-out row. The allocation is
`rows × list_size` — 14 TB on SF1, which aborts the process. Proven by arithmetic
(exact integer division against an independently measured row count, twice) and by
the discriminating fix: inserting a bare `WITH f` makes the identical query return
the correct answer.

Root cause: **there is no column-pruning pass in the logical planner.** This is
building one, not extending one.

Blocks LDBC IC6 and IC9.

Size: the largest item here. Estimate after a design spike.

---

## Wave 4 — conformance gaps: valid queries that error

### 4.1 `startNode`/`endNode` on a MATCH-bound relationship — [#187]

Works when the relationship is bound by `MERGE`/`CREATE`, fails when bound by a
`MATCH` traversal — and it is `startNode(r)` itself that fails, not the property
access. The relationship is bound (`e._eid`, `e._type` are in the schema) but the
planner reaches for a bare `e` column the traversal never produces. The endpoint
VIDs are already available on the traversal's edge columns
(`e._src_vid` / `e._dst_vid`), which suggests a resolution bug rather than missing
capability.

**Do this first in the wave**: it is likely the smallest, and it is the only
remaining blocker for IC14 that is not an optimization. Tests are already written
and `#[ignore]`d against it.

### 4.2 Projecting a whole entity from a pattern comprehension

`[(a)-[:R]->(x) | x]` fails with `No field named x` while `x._vid` is present in
the inner schema. Same entity-representation family as the `collect()`/`UNWIND`
work already landed.

### 4.3 `COLLECT { … }` subqueries

`Expected aggregate function, got: CollectSubquery(...)`. The AST variant exists
and the planner references it, but nothing executes it. Adjacent to the pattern
comprehension fallback already landed, which does almost exactly this work.

Size: 3–5 days for the wave.

---

## Wave 5 — performance

### 5.1 Is `index_scans` wired at all?

Every LDBC query reports `idx_scans=0` even after adding BTree indexes cut IC4
from 62.2 s to 3.1 s — so indexes demonstrably *are* being used. Either the
counter is under-wired or `scans_reported` (1–4 for queries with many scans) does
not cover the paths that matter. **Answer this before optimizing anything**, or
the numbers guiding the work are not trustworthy. Related: #175.

### 5.2 Hoist an uncorrelated pattern comprehension (approved plan, Phase B)

Rewrite in the logical planner to
`CrossJoin(Input, Aggregate[collect(m)] over Filter[w] over <pattern>)`. The
one-row right side makes it evaluate once. No new plan node or physical operator.
Today every uncorrelated comprehension pays a full per-row evaluation through the
fallback.

### 5.3 Predicate-driven anchoring (approved plan, Phase C)

Anchor by a correlated equality via a batched property probe. Note its headline
motivation has changed: **it will not unblock IC14 until 4.1 lands**, because
IC14's correlation expression does not plan at all.

### 5.4 IC3, IC5, IC4, IC12 latency

Two exceed a 120 s budget, IC12 takes 571 s at 40 GB. Do after 5.1, so
attribution is possible.

### 5.5 Tier 4 decorrelation (approved plan, Phase D) — gated

Only if the per-row fallback proves unacceptable in measurement. Needs a synthetic
row id and LEFT-join semantics. Most likely to be dropped.

---

## Wave 6 — evidence and hygiene

### 6.1 The TCK single-shape blind spot — `docs/testing/` entry

Three times this session a feature's entire TCK coverage shared one structural
shape, so a green suite bounded nothing:

| feature | TCK coverage | what it hid |
|---|---|---|
| pattern comprehensions | 11 scenarios, **all** anchored on an outer `MATCH` variable | every unanchored form errored |
| `startNode`/`endNode` | 1 scenario, relationship bound by `MERGE` | fails on MATCH-bound relationships |
| `IN` over entities | none | `n IN [n]` was false |

The rule worth writing down: **when a feature's whole TCK coverage shares one
structural shape, passing it bounds what you may claim.** Lands with the tests
that give it teeth, not before them.

### 6.2 Audit other TCK feature areas for the same shape

The mechanism is not unique to the three above. Unaudited, and its own piece of
work.

### 6.3 Flaky `sparse_recovered_update_overrides_stale_posting_without_rebuild`

Fails under parallel load with Lance's own `Retryable commit conflict … preempted
by concurrent transaction`; passes alone. The test does not retry what Lance
explicitly marks retryable.

### 6.4 Small debts

- `crates/uni/benches/ldbc/queries/README.md` still describes the country
  parameters as "the two most-populated countries"; the derivation changed.
- Three untracked probe examples (`ldbc_probe`, `unwind_traverse_probe`,
  `pc_probe`) are genuinely useful and currently survive only by not being
  committed — `pr.yml` lints `--all-targets`. Decide: make them lint-clean and
  commit, or delete.

---

## Sequencing at a glance

```
Track 0  differential oracle ──────────────────────────────────► (continuous)

Wave 1   #186 → extract_vid verify → canonical extractor
Wave 2   cooperative deadline + #185          (makes Wave 3 safe to land)
Wave 3   #184 column pruning                  (unblocks IC6, IC9)
Wave 4   #187 → whole-entity projection → COLLECT{}   (unblocks IC14)
Wave 5   counter audit → Phase B → Phase C → latency → [Phase D]
Wave 6   docs, TCK audit, flake, debts
```

Waves 1 and 2 are independent of 3–5 and can run concurrently if there is more
than one pair of hands. Wave 4.1 (#187) is small enough to pull forward at any
point and is the single highest ratio of unblocking to effort.

## Out of scope

Pre-existing open issues #174–#179 predate this work and are not ordered here.

## Status of the work already done

Nine product fixes, five of them silent wrong answers; LDBC went from 7 of 14
answering to 9 of 14. TCK held at 3980/3980 throughout.

## Status of this plan — 2026-08-27

Implemented on `feat/test-harness-track-e`, **not pushed**. Gates green on the
committed state: `fmt`, workspace `clippy --all-targets -D warnings`, `cargo doc
-D warnings`, openCypher TCK 3925/3925, workspace suite 6646/6646.

The TCK count above is 3925, against the 3980 recorded for the earlier pass.
Both runs were green, so this is a difference in scenarios *collected*, not in
failures — most likely a mode or filter difference between the two invocations
(`cargo nextest run -p uni-tck --test tck` here). Flagged rather than reconciled
silently, since neither number is a pass rate and quietly replacing one with the
other would hide the discrepancy.

| item | state |
|---|---|
| Track 0 — differential oracle | **not started** — needs a Neo4j instance |
| 1.1 `ORDER BY` (#186) | fixed |
| 1.2 `extract_vid` verify | verified **not reachable**; left as the plan directs, then folded into 1.3 |
| 1.3 canonical extractor | done — five implementations to two |
| 2.1 cooperative deadlines | done |
| 2.2 `max_query_memory` (#185) | done |
| 3 column pruning (#184) | the `UNWIND`-source case fixed; **not re-run at SF1** |
| 4.1 `startNode`/`endNode` (#187) | directed and undirected fixed (#188 closed); the post-`WITH` shape open (#187) |
| 4.2 whole entity from a comprehension | done; map values and map projections fixed (#189 closed) |
| 4.3 `COLLECT { }` | done — and `COUNT { }`, which this plan did not list as broken |
| 5.1 is `index_scans` wired? | **answered**: `docs/perf/index-scan-counter-2026-08-27.md` |
| 5.2–5.5 | not started — see the note below |
| 6.1 single-shape blind spot | `docs/testing/single-shape-coverage-2026-08-27.md` |
| 6.2 audit other TCK areas | done; `relationships(` is the next one-shape case |
| 6.3 flaky sparse test | fixed as a **product** defect, not a test retry |
| 6.4 small debts | README corrected; probes decided and committed |

### Where this plan was wrong

Three of the four root causes differ from what is written above, and in each
case one cheap discriminating test is what showed it. Recorded because the
diagnoses here were confident and wrong, not vague:

- **1.1 is not about traversals.** "A plain scan-plus-sort is stable; the
  traversal is what makes the difference" — both halves are false. A plain scan
  over `p3, p1, p0, p2` reproduces it. Any string starting with `P` was
  classified as an ISO-8601 duration and neither parser could reject anything,
  so they collapsed to one sort key. The control that made the traversal look
  guilty had input already in sorted order, so a sort doing nothing was
  indistinguishable from a sort that worked.
- **4.1's stated hypothesis is disproved by its own error message.** There is no
  `e._src_vid` on a traversal — the message quoted in #187 lists the whole
  schema. The endpoint *variables* are in scope, which is a different fix.
- **6.3 is not a test flake.** Four secondary-index writers committed to Lance
  with no retry while the vertex and delta paths had one all along. Retrying in
  the test would have hidden a defect users can hit.

### Found while implementing, not listed here

- `COUNT { }` was broken identically to `COLLECT { }` — both classified as
  aggregates by `Expr::is_aggregate()` when both are scalar subqueries.
- A silent wrong answer in multi-hop pattern comprehensions: the inner schema
  ordered property columns by kind and the batch builder by step, so
  `r.since + '/' + x.tag` returned `TAGGED/YEAR`. Every column is
  `LargeBinary`, so Arrow accepted the mismatch.
- `VectorIndexConfig`'s `default_refine_factor` was never propagated to the
  Python bindings, so the workspace clippy lane was already red on this branch.
- The `df_session_template` "hot path" is dead for every database with a plugin
  registry, i.e. all of them — which is why the memory pool had to be installed
  on both session-construction paths.

### What 5.1's answer means for the rest of Wave 5

`idx_scans` counts `ScanRequest`-based Lance scans that reported index activity,
and it was wired to one such path. The gap splits three ways and only one third
was an omission: the schemaless vertex scan and the L1 edge scan are now
counted; `vector_knn` and FTS go through Lance's `nearest()` and build no
`ScanRequest`, so they need their own callback and remain open; and the
traversal serves from the in-memory adjacency and issues no Lance scan at all,
so a zero there is correct rather than missing.

5.4 says "do after 5.1, so attribution is possible". Attribution is **not** yet
possible: until the `nearest()` path reports, an index lookup cannot say it
consulted an index. That is the gate on 5.2–5.4, not effort.

### Open, with issues

- #184 — the `UNWIND`-source allocation is gone; there is still no general
  column-pruning pass, and the unbounded `MutableArrayData` allocation is
  untouched. **Unverified at SF1.**
- #187 — `startNode` after a `WITH`: the endpoint variables leave scope and
  recovering a node from the relationship's `_src_vid` needs a vertex lookup.
  The undirected shape that had been split out as #188 is closed (below).
- #175 — the `nearest()` reporting gap above.

## Closed since — 2026-08-28

### #189 — the container, not the map

The report reads as a fact about maps: a map value fails where a list value
works. It is not. `translate_property_access` chooses between
`Column("x.name")` and `index(Column("x"), 'name')` on whether the translation
context calls `x` a graph entity, and the comprehension compiled its inner
expressions with the **outer** context, which has never heard of the pattern's
own variables. Every container that reaches that leaf was broken; the list
literal worked only because a separate pre-pass, `resolve_flat_column_properties`,
happens to have a `List` arm and no `Map` arm.

So the fix is at the leaf, not the container: the comprehension now compiles its
predicate and map expression with a context in which its own variables are
registered as nodes and edges. `CASE`, `IN`, map literals, map projections and
edge properties were all fixed by the one change, and each has a test.

`collect_inner_properties` needed the second half — it had no `MapProjection`
arm at all, so `x {.name}` never got its column built. Its `_ => {}` catch-all
is now gone: the match is exhaustive, and a new `Expr` variant is a compile
error rather than a column that silently fails to appear.

**The same catch-all still exists in `resolve_flat_column_properties`** (5 arms
of 27 variants, behind `other => other.clone()`). It is no longer the cause of
anything known, but it is the same latent defect in a second place — worth
closing on its own.

### #188 — the orientation was known and discarded

The issue argues that no static rewrite applies because which end of `-[e]-` is
the relationship's tail is a per-row fact. True — and the traversal *knew* that
fact and threw it away. `build_edge_adjacency_map` holds `(eid, src_vid,
dst_vid)` and files the edge under both endpoints without recording which side
it filed under; `AdjacencyManager::get_neighbors` loops `direction.expand()` and
drops `dir`.

The traversal now reports it on `{r}._fwd` and the planner rewrites the call to
a `CASE` over the hop's two variables, both already in scope with their
properties materialised — so there is no `{_vid}`-only stand-in anywhere in the
path, and the silent-NULL trade the issue refuses cannot arise. `_fwd` is
computed only when a query asks for it, so an undirected traversal that never
calls `startNode`/`endNode` costs exactly what it cost before.

Three things this turned up that the plan did not predict:

- **The `#[ignore]`d test passes without any of the work.** `_fwd` did not
  exist, so it resolved to NULL, so the `CASE` always took its `ELSE` branch —
  which happens to be right for a fixture anchored at `b`. Anchoring the same
  query at `a` returns the *other* endpoint. The discriminating test runs both
  anchors and asserts they agree; a single-anchor test passes with the
  orientation inverted and passes again with it missing entirely.
- **There are two single-hop operators, and a schema'd fixture only exercises
  one.** `GraphTraverseExec` serves a declared schema and
  `GraphTraverseMainByType` serves a schemaless one. Fixing the first left the
  second silently returning the wrong endpoint, caught only by adding a
  schemaless fixture. Every other test in that file declares a schema.
- **An aggregate over `startNode(r)` was already broken, directed included.**
  This pass runs after planning, so an `Aggregate`'s outputs are already named
  by their rendered expression and the projection above refers to them by that
  string; rewriting the aggregate renamed the column out from under its own
  consumer. It failed for the directed case that had already shipped — the
  directed fix's tests never put an endpoint call under an aggregate. Fixed by
  publishing the repr rename upward alongside the bindings.

The `CASE` is deliberately never lifted across an aggregate: `_fwd` varies per
row while an aggregate spans rows, so `CASE WHEN r._fwd THEN count(x) ELSE
count(y) END` is not `count(CASE WHEN r._fwd THEN x ELSE y END)` — the first
splits one group in two and undercounts.

**Still unsupported, loudly:** returning the *whole* endpoint of an undirected
relationship (`RETURN startNode(e)` under `-[e]-`) needs a `CASE` over two node
structs, which the expression compiler cannot unify. It raises an error, and a
test pins that it does — so if it ever starts returning a row, that has to be
the right endpoint rather than a null-filled stand-in.
