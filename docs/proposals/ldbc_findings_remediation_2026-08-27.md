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

- #192 — `resolve_flat_column_properties` handles 5 of 27 `Expr` variants behind
  a catch-all. Filed 2026-08-28 as hardening, **with no user-visible repro**.
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
anything known, but it is the same latent defect in a second place — filed as
**#192**, and folded into #184's catch-all sweep rather than given its own PR,
because sixteen probed shapes failed to produce a repro for it.

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

### A wider gap underneath: `CASE` over two entities

Returning the *whole* endpoint of an undirected relationship errored at first,
and it was tempting to record that as a narrow limit of the rewrite. It was not
one. `RETURN CASE WHEN true THEN x ELSE y END` over two node variables failed
identically with no endpoint call anywhere in the query — a shape a user can
write directly, broken before any of this work.

`find_common_result_type` had no rule for entity structs. Two nodes are the same
*Cypher* type without being the same *Arrow* type: the struct's fields are
whatever the plan materialised for that variable, so a scanned anchor carrying
`_all_props` and a traversal target without it differ by a field, and two
different labels differ by their property columns outright. The pair matched no
rule and fell through to the Utf8 fallback, dying on `Unsupported CAST from
Struct(..) to Utf8`.

Entity structs now coerce to CypherValue `LargeBinary`, which is already the
codebase's universal encoding and already the answer Rule 6 gives for every
other mixed pair. `RETURN startNode(e)` under `-[e]-` returns a node with its
properties, across two labels as well as one.

The general lesson is the one this document keeps re-learning: the first
explanation was "the rewrite produces a shape the compiler cannot handle," which
is true and useless. The discriminating test — the same `CASE` with no
`startNode` in it — took one minute and moved the defect somewhere else
entirely.

## Found while closing #189 and #188 — 2026-08-28

The `CASE`-over-entities fix above ended by noting that a scanned node and a
traversed node are not the same Arrow struct. That was recorded as latent — "not
observable in seven probed paths." The claim was accurate about those seven
paths and wrong as a conclusion: an eighth, `UNION`, observes it, and probing it
turned up an unrelated and more serious defect sitting next to it.

Both were confirmed with controls before filing. Neither came from LDBC; both
were found by hunting a repro for something already believed to be harmless.

### #190 — `UNION` over a whole entity returns the wrong value, silently

```cypher
MATCH (a:P)-[:KNOWS]->(b:P) RETURN b AS n
```

returns `Node{vid: 1, name: "b"}`. The same query `UNION ALL` with **itself** —
identical schemas on both branches, nothing to reconcile — returns
`[List(["P"]), Null]`. Over a relationship it returns `[Null, Null]`.

Two controls bound it. `UNION` over a *property* (`b.name`) is correct, and
`UNION` over a *scan*-bound entity is correct. So the defect is specific to
entities produced by a traversal. `List(["P"])` is the struct's `_labels` field,
which reads like positional column misalignment — recorded on the issue as a
hypothesis drawn from the observed value, not as a verified cause.

**This is a silent wrong answer in a shape users write directly.** Under this
document's own ordering principle it outranks everything else still open.

### #191 — the struct asymmetry itself, now with a repro

```cypher
MATCH (z:P) RETURN z AS n
UNION
MATCH (x:P)-[:KNOWS]->(y:P) RETURN y AS n
```

fails with `cannot UNION branches with mismatched schemas`, whose own text says
"This is a planner bug; please file an issue."

Verified *not* affected, and recorded on the issue so the next person does not
re-probe them: equality, `id()`, list literals, `IN`, `DISTINCT`, `collect`,
`coalesce`. All of those route through the CypherValue encoding, which erases
the difference. `UNION` compares Arrow schemas directly and does not.

The `CASE` coercion rule shipped above treats the symptom — it makes mixed
entity types agree on `LargeBinary`. It does not answer why a traversal target
lacks `_all_props` when a scan anchor carries it. That question is #191.

### #192 — the catch-all, filed as hardening rather than as a bug

`resolve_flat_column_properties` still matches 5 of `Expr`'s 27 variants behind
`other => other.clone()`. Sixteen shapes were probed for a user-visible
consequence — map, `CASE`, `IN`, `IS NOT NULL`, array index, nested map, each
with and without an intervening `WITH`, inside `CALL {}`, after `collect`/
`UNWIND`, and inside comprehensions. **All sixteen returned correct answers.**

Triggering it needs a flat `"v.p"` column to exist *while* `v` is absent from
`variable_kinds`, which appears unreachable outside pattern comprehensions — and
that path is now fixed at the context layer. So the issue is `enhancement`, not
`bug`, and says plainly that no repro exists. Filing it as a defect would have
dressed a hunch as a finding.

## Revised plan for what remains — 2026-08-28

The original sequencing (PR 1 #189 → PR 2 #188 → PR 3 #187 → PR 4 #175 → PR 5
#184) is discharged through PR 2. The remaining order changes, because two of
the items now open did not exist when it was written and one of them is a silent
wrong answer.

```
PR 3  #190 UNION over a traversal-bound entity      (silent wrong answer)
PR 4  #191 struct parity scan vs traversal          (likely the same root cause)
PR 5  #187 startNode after a WITH                   (projection widening)
PR 6  #175 vector/FTS reporting        P0 gate first
PR 7  #184 spike: catch-all removal + SF1 re-run + design doc
      #192 folded into PR 7's catch-all sweep
```

### PR 3 — #190, first

Ranked first on severity alone: it returns a wrong value with no error, in a
query shape with no exotic syntax in it. Everything else open is either a loud
failure, a missing observable, or a known gap.

Root-cause before estimating. The one hypothesis on record — positional
misalignment between the branch schema and the branch's actual columns — is
inferred from the returned value (`_labels`, the struct's second field) and is
**unverified**. The discriminating question is whether the wrong field is read
because the columns are ordered differently on the two sides, or because the
`UNION` node projects by position where it should project by name. A single
branch whose struct has its fields deliberately reordered answers it.

The self-`UNION` case is the useful reproducer, not the mixed one: with both
branches identical there is nothing for schema reconciliation to do, so anything
that goes wrong is downstream of reconciliation.

### PR 4 — #191, immediately after

Sequenced second because #190 may well subsume it. If the two branches' structs
were identical, the mismatched-schema error could not arise; whatever makes a
traversal target's struct differ from a scan anchor's is a plausible common
cause with the misalignment in #190. Root-cause #190 first and re-check whether
#191 survives.

If it does survive, the fix belongs where the struct is *built*, not where it is
compared: make the traversal path and the scan path agree on a node's field set,
rather than teaching `UNION` to reconcile them. The `CASE` coercion rule stays
regardless — it is what handles two *different labels*, which no parity fix can
make structurally identical.

### PR 5 — #187, unchanged

Projection widening in `planner.rs`, with the four refusal conditions the
original plan specified (`Distinct`, aggregates, `Union` branch, `CALL {}`
boundary) each pinned by a test asserting a clear error. Its `#[ignore]` at
`start_end_node_test.rs:124` is the last one in that file.

One thing the #188 work adds to it: the aggregate/repr-rename defect fixed there
was invisible until an endpoint call was placed under an aggregate. #187's
widening interacts with the same post-planning pass, so its test list must
include the aggregate shape from the start rather than discovering it later.

### PR 6 — #175, unchanged and still gated

The P0 probe stands as written: confirm `partitions_searched` fires on ANN and
FTS and *not* on a flat search with a scalar prefilter, **before** wiring
anything. If it cannot discriminate, the honest outcome is to record that on
#175 and stop — a counter that cannot tell an index from a brute-force scan is
worse than the gap it would paper over.

### PR 7 — #184 spike, absorbing #192

The spike's P0 was already "delete the catch-all arms in
`collect_properties_recursive` so a new `LogicalPlan` variant is a compile
error." #192 is the identical defect class one layer down, in `Expr` rather than
`LogicalPlan`, and in the same crate. Doing them together is one review of one
idea instead of two.

Both are exhaustiveness work with no known user-visible symptom, which is why
neither justifies a PR of its own and why neither should be sold as a bug fix.

### What this round should be read as evidence for

Two of the three newly-open items were found by trying to write a failing test
for something already recorded as harmless. Neither would have been found by
running the suite, the TCK, or LDBC — the suite was green and stayed green
throughout. That is the same mechanism `docs/testing/single-shape-coverage-2026-08-27.md`
describes, arriving from the other direction: a claim of "not observable" is
bounded by the paths actually probed, in exactly the way a green TCK is bounded
by the shapes it contains.

### #190 — the columns, not the union

Closed 2026-08-28. The report guessed at positional column misalignment inside
the union. The batches were never wrong: dumped at the read boundary, column
`n` held a correct struct on both branches, declared type matching actual. The
defect was one layer further out, in *naming* the result's columns.

`extract_projection_order` had no `Union` arm and no `Distinct` arm, so both
shapes fell through its catch-all into a fallback that "falls back to the first
row's keys, sorted". A traversal's rows carry `b._vid`, `b._labels`, `b.name`
beside the projected `n`, so sorted keys put `b._labels` at index 0 — the only
column the caller reads. The second row came from the other branch, keyed
`d.*`, and had no `b._labels` at all, which is where the `Null` came from.

A second copy of that logic already existed in the planner and *did* handle
both variants. Two implementations of one idea, disagreeing; the disagreement
was the bug. Both now delegate to one canonical `projection_columns` with an
exhaustive match over all 70 `LogicalPlan` variants — the same recurrence fix
this document asked for in 1.3 and again for #189, in a third place.

Three things this turned up that the issue did not say:

- **`UNION` was never the requirement.** `RETURN DISTINCT b AS n`, with no
  union anywhere, returned the same labels list. It reaches the same catch-all,
  and it is the more ordinary of the two queries.
- **The issue's own control passed by luck.** Its scan-scan union works because
  the helper prefix `z` sorts *after* `n`. Rebound to `a`, the identical query
  fails. A test written only against the `z` shape reports the feature working.
- **The fallback cannot simply be deleted.** Instrumented across the
  integration suite it takes 51 legitimate hits — all DDL and admin plans
  (`success`, `registered`, `plan`, `labels`), none carrying an internal
  column. That measurement is why the guard added alongside is narrow: it
  errors only when the order is unknown *and* the rows carry internal columns,
  which is exactly the combination that produced a wrong answer.

The guard was checked by neutralising the fix: the same query then raises
`cannot name the result columns … the internal column \`b._labels\`` instead
of returning a value. Had it existed before, #190 would have been loud.

### #191 — and the label pair the asymmetry was hiding

Closed 2026-08-28. The asymmetry was where the issue said it was.
`plan_scan_*` pushes `_all_props` whenever a whole entity is requested; the
schema'd traversal path required `target_properties.is_empty()` as well, a
condition a schema-defined label can never meet, because `resolve_properties`
expands `"*"` into the declared property names. Its comment claimed to mirror
`plan_scan_all` and did the opposite. The two schemaless traverse planners in
the same file already applied the scan's rule, so the fix had a precedent
beside it rather than being a new policy.

That alone did not make the union work, and the reason is worth recording.
The branches differed in **width** as well as in the struct: a scan of `:P`
emits six columns to a traversal's four, because each carries the internal
helper columns its own operators produced. So the union now narrows each
branch to the columns the query projects before comparing them. That is the
more valuable half — it also stops helper columns escaping above a union at
all, which is the leak that made #190 possible.

**The part that was nearly left undone.** With widths and the `_all_props`
field reconciled, `MATCH (p:P) RETURN p UNION ALL MATCH (q:Q) RETURN q` still
failed: two labels have different properties, so their structs differ by
construction. It was recorded as a limitation and pinned with a test — the
second time in one session that a valid openCypher query was written off as an
acceptable loud error. The tell, both times, was naming the mechanism in the
same sentence that declined to apply it: `find_common_result_type` already
coerces entity structs to the CypherValue encoding for `CASE`, its `schema`
parameter is unused, and `_cypher_scalar_to_cv` already performs the encoding.
"Separate work" was a claim about effort that the reading had already
contradicted.

The union path now asks the same question `CASE` does, and two labels union
correctly. The coercion is deliberately narrow: only positions where both
sides are entity-ish are encoded, so `RETURN p UNION ALL RETURN id(q)` — a
node against an integer — still fails loudly rather than inventing a
conversion. Both sides of that line have a test.

The property assertion matters more than it looks: an encoding round-trip that
dropped properties would still produce `Value::Node`, so the test asserts
label *and* property (`P:a`, `Q:q1`) rather than node-ness.

### Still open after this round

- #184 — no general column-pruning pass; unverified at SF1.
- #187 — `startNode` after a `WITH`.
- #175 — the `nearest()` reporting gap.
- #192 — the `resolve_flat_column_properties` catch-all, folded into #184.
