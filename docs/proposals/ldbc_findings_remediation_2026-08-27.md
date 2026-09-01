# Remediation plan for the LDBC SNB findings — 2026-08-27

Running LDBC SNB Interactive against SF1 found more defects than percentiles.
This ordered what remained.

> **Status is at the end — [Status — 2026-08-28](#status--2026-08-28) for the
> closed/open ledger, [Step 0 executed — 2026-08-29](#step-0-executed--2026-08-29)
> for the current SF1 measurements, and the current ordering in
> [Plan — revised after triage, 2026-08-29](#plan--revised-after-triage-2026-08-29),
> which supersedes the earlier [Plan ahead](#plan-ahead).**
> Everything before it is the plan as written, plus the record of what
> executing it turned up. Both are kept because in most cases the
> diagnosis here was confident and wrong, and the correction is the part
> that does not survive being summarised. Do not execute the wave or PR
> sections below without checking that table first.

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

### The original waves — discharged

Waves 1 to 6 below are the plan as first written. Every item in them is
closed or reassigned, several for a reason the wave text gets wrong —
which is why the text stays. The exceptions are Track 0, never started,
and Wave 5.2–5.5, unblocked only now.

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
| 4.1 `startNode`/`endNode` (#187) | done — directed, undirected (#188), and post-`WITH` (#187) all closed |
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

Superseded — **Status — 2026-08-28** at the end of this document is the single
current list. The narrative sections between here and there record how each item
was closed; they are kept for the reasoning, not for the status.

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
PR 3  #190 UNION over a traversal-bound entity      DONE
PR 4  #191 struct parity scan vs traversal          DONE
PR 5  #187 startNode after a WITH                   DONE  (not by widening)
PR 6  #175 vector/FTS reporting        P0 gate first
PR 7  #184 spike: catch-all removal + SF1 re-run + design doc
      #192 folded into PR 7's catch-all sweep
```

**All seven are now discharged.** PR 6 closed #175 — whose headline
claim was already stale — and PR 7's spike closed #184, splitting what
was left into #197 and #198.

Four of the five closed by a mechanism other than the one written
below. #191 turned out **not** to share a root cause with #190, so the
conditional scope written for a shared cause never applied. #187 was
closed by hydration rather than the projection widening proposed here,
which the measurement retired. And #184's spike argued against the
general pruning pass it was chartered to design. The notes are kept as
the reasoning they were, not as a description of what happened; the
sections after them record what.

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

### What the #189/#188 round was evidence for

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

### #187 — the injection, not the endpoint pass

Closed 2026-08-28, and the diagnosis in this document was wrong twice over.

**It did not return NULL.** The plan for this work argued that the `{_vid}`
stand-in in `startnode_endnode_impl` was live, making #187 a silent wrong
answer. Measured, every post-`WITH` shape raised `No field named _anon_1`. The
issue's own transcript was right; the reasoning that predicted otherwise was
not.

**The stale reference came from argument injection.** `startNode`/`endNode`
resolve by being handed every known node variable as an extra argument, and
`collect_variable_kinds` walks the whole plan subtree — so after `WITH e AS rel`
the context still named the traversal's `_anon_0`/`_anon_1` and injected columns
the schema no longer had. Nothing checked the column existed. Three scopes reach
that injection — projections, filters, and pattern-comprehension sub-plans — and
each needed the schema it actually compiles against. IC14 failed at each one in
turn, the error moving inward as the outer scope was fixed.

Filtering the injection alone would have been a regression from a loud error to
exactly the silent NULL the plan wrongly believed was already happening, so it
only ships alongside real hydration.

**Projection widening could not have worked.** It was the earlier
recommendation, and the measurement retired it: `WITH relationships(p)[0] AS r`
fails identically and has no traversal binding anywhere to widen. IC14's
relationship is a list-comprehension variable over `relationships(path)`, which
is further still from any endpoint variable.

`EndpointHydrateExec` materialises the endpoints from the `_src`/`_dst` the
relationship already carries, using the batched pre-fetch discipline
`bind_fixed_path` uses. For a comprehension it emits a parallel list flattened
with the element list's own offsets, so element *i* stays paired with endpoint
*i* — padded explicitly rather than trusting the two lists to have the same
shape. The hydrated columns reach the *existing* UDF as extra arguments, so
neither the UDF nor property-access compilation moved.

**LDBC IC14 now plans and executes**, which is what the issue said this was for.
The test runs the real query text, not a paraphrase.

The `{_vid}` stand-in is gone. Returning a map that answers `id()` correctly and
every property as NULL is the defect, not a fallback for it; 358 related tests
pass without it.

### #193 — found while fixing it: an undirected match reversed the relationship

`MATCH ()-[e:KNOWS]-() RETURN e` returned the *same* edge twice — once as
`src:0, dst:1` and once as `src:1, dst:0`. The second is fabricated. No error,
and nothing to do with `startNode`.

Filed as #193 and fixed in the same pass. Filed separately rather than folded
into #187's record because it predates that work, reproduces with no endpoint
call anywhere in the query, and is a silent wrong answer in its own right.

`add_edge_structural_projection` built `_src`/`_dst` from the traversal's own
source and target variables, which for an undirected hop are whichever end the
row matched from rather than the edge's stored tail. `_fwd` — which #188 already
computes for exactly this ambiguity — now orients them back to storage order.

It was found only because the hydrated `startNode` disagreed with
`undirected_endpoints_do_not_depend_on_the_anchor`, a test #188 had already
written. Fixing the struct rather than special-casing the hydration closed both.

Worth noting why the existing suite could not see it: the edge really is present
in both rows and its `eid` and type are right, so anything asserting identity
passes. Only an assertion on the endpoints of a relationship *value* from an
undirected match can catch it, and #188's endpoint tests go through the `_fwd`
rewrite rather than through the edge struct. Two paths to the same fact, one
tested.

## Status — 2026-08-28

The single current list. Everything above this point is narrative.

### Closed

| item | what it turned out to be |
|---|---|
| #186 | not traversals at all — every `P…` string parsed as a duration, collapsing the sort key |
| #185 | `max_query_memory` measured only the finished result set |
| #187 | argument injection naming columns a `WITH` had dropped — not the endpoint pass, and not a silent NULL |
| #188 | orientation the adjacency computed and discarded; surfaced as `_fwd` |
| #189 | the container was innocent; the leaf chose its column from the wrong translation context |
| #190 | no `Union` or `Distinct` arm in the projection-order extractor, so result columns were guessed from sorted row keys |
| #191 | `_all_props` pushed unconditionally on the scan path, conditionally on the schema'd traversal path — plus a branch-width mismatch |
| #193 | undirected hops built the edge struct from the traversal's source, reporting the same edge reversed on half the rows |
| #194 | a filtered FTS query was bounded by `k` *before* the filter, so excluded rows consumed the top-k slots — measured 0 rows of 10 with 50 matches |
| #196 | an aggregate group key was marked "*", so the whole entity — `_all_props` and `overflow_json` included — was scanned and hashed per group |
| #184 | the `UNWIND` source really was the crash — with it pruned, IC6 answers 10 rows at SF1 rather than aborting on a 14 TB allocation |
| #175 | not unobservable as filed — the scalar half already shipped; the gap was vector/FTS, where the scan path's predicate is *unsound* rather than merely absent |
| #195 | no benchmark exercises the new vector/FTS counters — filed as coverage, not a defect |
| #197 | **not a silent wrong answer** — the pruned column is caught downstream, so a *valid* query fails with `No field named …`. Also not the `_ => {}` arms it named: the reachable hole was inside a **handled** arm, `Clause::Match` never walking its pattern's inline property maps and `WHERE`s |
| #201 | IC14's endpoint lookup searched the *nodes in scope*, which only succeeds when the endpoint happens to be one of the pattern's own bindings — so every non-matching `(a,b)` pair errored instead of evaluating false. The hydrated endpoint is now passed into the correlated subquery. **Fix verified locally; not yet re-run at SF1** |
| #192 | all 27 `Expr` variants enumerated, scope-introducing ones listed individually rather than swept into a wildcard. Filed as hardening with no user-visible repro, and that still holds — but it is **not inert**: it changed the rewrite for `Case`, `In`, `Map`, `ArrayIndex`, `ArraySlice`, `IsNull`/`IsNotNull`, `LabelCheck`, `ValidAt`, `MapProjection`, and for the iterated list of a comprehension |

Gates on the current tip: `fmt` and workspace
`clippy --all-targets -D warnings` clean; `uni-db` 2770/2770;
`uni-query` + `uni-cypher` + `uni-query-functions` 1183/1183; openCypher TCK
3925/3925 ("no change vs previous run").

An earlier tip (before the #201 fix) ran the **full workspace** at 6726/6726,
up from a 6710 baseline. That full run has **not** been repeated since #201
landed — `uni-store` and the remaining crates are untouched by planner and
expression changes, but they were not re-measured, and this line should not be
read as though they were.

Every fix arm across both issues was **discriminated** — neutralised one at a
time, each breaking only its own tests, with #184's genuinely-dead-source
control still pruning throughout. That matters more than usual here: both
changes are largely "add match arms that agree with the old behaviour", which is
the exact shape in which a decorative test hides.

**LDBC IC14 — this document's claim was wrong, and the way it was wrong is the
lesson.** It read: *"IC14 plans and executes … the only one of these verified
against the real query text rather than a reduction of it."* True of the text,
false of the data. The test used IC14's real query but a fixture with no
`Comment`, `Post`, `HAS_CREATOR` or `REPLY_OF`, so the weight pattern matched
nothing, `reduce` never evaluated its body, and `startNode(r)` — the whole point
— was never called. It asserted `personIdsInPath` and never asserted
`pathWeight`. At SF1, where the pattern does match, IC14 failed.

Filed as #201 and fixed; the vacuous-fixture mechanism is #205. The distinction
worth keeping: **real query text over reduced data is not verification of the
query**, and nothing about such a test looks wrong on review.

**Wave 5 is unblocked.** 5.2–5.5 were gated on #175: until the `nearest()` path
reported, latency work could not attribute anything to an index. It reports now.

### Open

| item | state | gate |
|---|---|---|
| #198 | peak child RSS **37.11 GB** against a 1 GiB pool; IC10 killed by the **kernel OOM killer** (123 GB virtual, 19.5 GB resident at kill) | Split from #184. Re-measured 2026-08-28: the figure in the issue, 13.8 GB, understated it by ~2.7×. The guard is not under-counting this path, it is absent from it. Bound the unwind output into chunks; pruning cannot cover a list that is legitimately live. |
| #202 | IC2 and IC9 fail inside DataFusion's external sort, which has no spill path | **The opposite failure to #198, and deliberately not folded into it.** Here the sorter *does* reserve, the pool *does* refuse, and the query fails cleanly — the guard works. The gap is that a sort above the pool should spill rather than fail. Raising the pool is not a fix: IC9 asks for 5.1 GB and the figure is data-dependent. |
| #203 | IC4 still exceeds the pool in `GroupedHashAggregateStream` (1476.5 MB vs 1024 MB) **after** #196 | Same operator and same message as #196, which fixed it for the parameter-derivation query by narrowing a `"*"` group key. IC4 still fails on the tree carrying that fix — either a second instance the fix does not reach, or a genuinely large aggregate. Could not be distinguished earlier because the bench never reached IC4. |
| #204 | IC12 10.5 min, IC3 4.5 min, IC5 over the 300 s budget — **no index consulted by any of them** (`scalar_idx=0 vec_idx=0 fts_idx=0`) | Latency, filed apart from the memory items so it is not mistaken for them. IC3 and IC12 answer correctly but peak at 31.8 GB and 38 GB, so **#198 should land first** or the profile will mostly rediscover it. The zero index counters are only visible because #175 shipped. |
| #205 | audit for vacuously-passing tests | The mechanism that hid #201: a fixture that cannot match the query's pattern, so the feature under test never runs and every assertion passes honestly. Real query text over reduced data reads as verification on review. |
| #199 | `COUNT { UNWIND outer AS y … }` fails with `Column 'outer' not found for UNWIND` | Found while probing #197. Confirmed pre-existing: fails identically with pruning disabled, so it predates #184 and is untouched by #197. The planner now records the read correctly; the execution path does not support the shape. |
| #200 | one `flush_to_l1 barrier not established` in fixture setup, first full-suite run of 2026-08-28 | **Unexplained, not dismissed.** Did not recur across two later full runs (6720/6720, 6726/6726). Disk space and `TMPDIR` both ruled out — `/home` had 1.2 TB free and the run was confirmed writing there. Recorded rather than called a flake. |
| Track 0 | differential oracle | Not started; blocked on infrastructure, not code. |
| Wave 5.2–5.5 | comprehension hoisting, anchoring, latency, decorrelation | **Ungated** — #175 closed. Start with 5.4's latency attribution, which now has a witness. |

### SF1, measured 2026-08-28

The first full SF1 run this document has on record. **6 of 14 queries fail**,
and the run is the source of #198's corrected figures and of #202–#204.

| query | rows | ms | peak RSS | outcome |
|---|---:|---:|---:|---|
| IC1 | 20 | 2 277 | 2.9 GB | ok |
| IC2 | — | — | 3.1 GB | external sort out of memory (#202) |
| IC3 | 20 | 272 110 | 31.8 GB | ok, 4.5 min (#204) |
| IC4 | — | — | 31.8 GB | aggregate over pool (#203) |
| IC5 | — | — | 31.8 GB | over the 300 s budget (#204) |
| IC6 | 10 | 298 366 | 31.8 GB | ok — the query #184 was filed for |
| IC7 | 20 | 22 154 | 31.8 GB | ok |
| IC8 | 20 | 3 528 | 31.8 GB | ok |
| IC9 | — | — | 31.8 GB | external sort out of memory (#202) |
| IC10 | — | — | — | **SIGKILL, OS OOM** (#198) |
| IC11 | 10 | 32 083 | 2.9 GB | ok |
| IC12 | 20 | 628 624 | 38.0 GB | ok, 10.5 min (#204) |
| IC13 | 1 | 86 | 38.0 GB | ok |
| IC14 | — | — | 38.0 GB | endpoint not in scope (#201, now fixed) |

Peak `VmHWM` across every bench child: **37.11 GB**.

Two things this run establishes that no earlier one could. The bench **forks one
child per query**, so a `SIGKILL` line is not the end of the run — the parent
records it and continues, which is why IC11–IC14 have results after IC10 died.
And the prior run (2026-08-27) stopped around IC10, so it **never reached
IC14**; the absence of an `IC14.tsv` from that run is not evidence IC14 was
already broken. Reasoning from absence, in a document about an analysis that
reasons from absence.

### What this round is evidence for

Four of the fifteen closed above were **silent wrong answers** — #186, #190,
#193 and #194. The rest failed loudly, and in different ways worth keeping
distinct: #187, #189, #191, #197 and #201 raised planner errors; #185 was a limit that
did not limit; #196 and #184 aborted; #175, #192 and #195 are observability and
hardening, not behaviour; and #188 was a rewrite that resolved to the wrong
endpoint only once its `#[ignore]` came off.

**#197 is the one whose severity this document got wrong, and in the unsafe
direction.** It was ranked first of everything open specifically because an
under-report in `mark_dead_unwind_sources` was believed to prune a live column
and answer wrongly in silence. It does prune the column; what follows is a hard
`No field named …` on a query that is perfectly valid and worked before #184's
pruning shipped. Both are bugs and one change fixes both, but the ranking rested
on a prediction nobody had run. The prediction was written here, not measured
here — which is the same failure this section is otherwise about.

**#201 is the sharpest instance of the same failure.** It was not found by
probing a claim — it was found because the claim was *checked against SF1*. A
test asserting the wrong thing had certified it, and this document repeated the
certification. The chain is worth keeping intact: a fixture that could not match
the pattern → a test that never ran the code → a headline verification in this
document → a query that fails on real data. Every link passed review.

Its fix also produced a wrong turn worth recording. The first attempt rebuilt
the scan anchor's struct from flat columns; it made the local repro pass while
**SF1 still failed, with the vid moving 1028 → 847**. A symptom moving rather
than a cause going away — and the only reason that was visible is that the SF1
error names the vid it could not find.

Separately, and more usefully: **#190, #191, #193 and #194 were all found by
probing something this document had already recorded as harmless, absent, or
out of scope** — not by a failing test. The suite was green before and after
each. That is a fact about how they were discovered rather than how bad they
were, and it is the reason this section exists.

A fifth, #196, was found by running a verification step this document had
written off as unexecutable. The stated reason for skipping it was true and
was not what stopped it.

**Five** defects were written off and had to be reopened, and the tell is the
same every time: a sentence that identifies the mechanism precisely and then
declines to apply it. `CASE` over two entities; two labels through a `UNION`;
the FTS filter that could starve top-k, noted as "out of scope, but noticed"
in a plan and measured at 0 rows of 10 a day later; the bench step called
unexecutable; and IC14, certified by this document on the strength of a test
that never executed its own subject. Naming a fix that specifically is evidence
it is cheap, not evidence it is out of scope.

The lesson this document keeps re-learning, now from three directions: a claim
of "not observable" is bounded by the paths actually probed, in exactly the way
a green TCK is bounded by the shapes it contains
(`docs/testing/single-shape-coverage-2026-08-27.md`).

### Plan ahead

Superseded — [Plan — revised after triage, 2026-08-29](#plan--revised-after-triage-2026-08-29)
is the current ordering; step 0 below was executed and is measured in
[Step 0 executed — 2026-08-29](#step-0-executed--2026-08-29). Kept for
the reasoning, per this document's convention.

Ordered by what each unblocks, not by size.

**0. Re-run SF1 and the full workspace suite.** Both are outstanding and both
are cheap relative to what they gate. #201's fix is verified locally against a
reproduction of SF1's actual failure mode, but IC14 has not been observed
returning rows at SF1, and the issue should stay open until it is. The full
workspace suite has not run since #201 landed. Everything below is measured
against a run that predates the current tip.

**1. #198 — bound the unwind output.** The last thing between SF1
and a completing bench run, and the only remaining item that ends in
a killed process rather than a wrong or slow answer.

The site is `GraphUnwindStream::build_output_batch`
(`df_graph/unwind.rs:600`). `process_batch` (`:337`) accumulates
*every* expansion for an input batch into one `Vec<(usize, Value)>`,
then does one `take` per surviving column over the whole set
(`:621`). Peak is `rows × list_size` for the batch, allocated in one
shot and — because it builds an Arrow buffer directly rather than
reserving — invisible to the memory pool. That is why peak RSS
reached 13.8 GB against a 1 GiB pool: the guard is bounding the
operators that ask it and missing roughly an order of magnitude.

The fix is to emit the expansion in fixed-size slices — several
output batches per input batch, each with its own `take` — capping
peak at `chunk × columns` with no change to semantics or row order.
`GraphUnwindStream` holds the pending remainder across `poll_next`,
the same shape `EndpointHydrateStream` and `BindZeroLengthPathStream`
already use.

Do this **whether or not** any further pruning lands: #197 and #184
remove the *dead* case, but a list that is legitimately live still
replicates. Pruning removes one case; chunking bounds the other.

Success is the process surviving, not a latency number. It will not
by itself fix IC5/IC6 (a time budget, not a memory one) nor IC2/IC9
(DataFusion's external sort with no spill path) — those are the SF1
residuals above and should not be folded in.

**2. #203, then #202.** Both are memory, and they want opposite fixes, which
is why the order matters. #203 is plausibly a second instance of #196's
narrowing gap — cheap to check, and if it is, it is one arm again. #202 is a
configuration decision rather than a defect: either set up spilling so a large
sort completes, or say a sort above the pool is out of scope and make the error
say so.

**3. #205 — the vacuous-fixture audit.** Placed here rather than last because
it governs how much the rest of this list can be trusted. #201 was certified by
a test that never ran its own subject; nothing establishes that it is the only
one. The signature to grep for is a fixture omitting an entity type the query's
pattern requires.

**4. The marker audit.** #196 was found by measuring, not by reading,
and it cost one match arm. The same method applies to the remaining
sites that consume expressions without emitting a provenance marker.
For each, the question is: *does this context need the entity whole,
or only its identity?*

- `Distinct` — `LogicalPlan::Distinct` (`df_planner.rs:1061`) groups
  by **all** schema columns with no narrowing. This is #196's shape
  exactly; `RETURN DISTINCT n` over a wide label is the obvious next
  candidate and should be measured before it is assumed.
- `Union` dedup (`df_planner.rs:5822`) — likewise groups by every
  column.
- `Sort` keys, `Window` partition/order keys, `CrossJoin` / `Apply`
  correlation keys.

Only if this turns up a shape a marker *cannot* express is the
general top-down pruning pass worth building. The argument against
building it now is in
`docs/proposals/column_pruning_spike_2026-08-28.md` and is unchanged
by this round.

**5. #204 and Wave 5.2–5.5.** Latency, and ungated for the first time. 5.4 first: latency
attribution now has a witness in the vector/FTS counters, and the
other three (comprehension hoisting, anchoring, decorrelation) are
optimisations that should be measured against it rather than assumed
to help.

**6. Track 0 — the differential oracle.** Stays last, and the reason
is worth restating rather than inheriting: it is the one thing that
would have found most of the fifteen closed items without anyone
probing for them. Four of them were found only because someone went
back to something this document had already recorded as harmless,
absent, or out of scope. That is not a repeatable process. It is
still blocked on a Neo4j instance rather than on code, which is the
only reason it is not first.

## Step 0 executed — 2026-08-29

Both halves of step 0 ran. The workspace suite is green on the tip
carrying #201: **6726/6726** (102 skipped), so the caveat above about
`uni-store` and the rest being unmeasured since #201 is discharged.

### SF1, measured 2026-08-29, machine idle

Results in `~/uni-bench-tmp/ldbc-results-20260829`. A first attempt the
evening before ran concurrently with an unrelated 20 GB model-loading
job on the same machine; its numbers are discarded here except as the
control they turned out to be (see the contention note below).

| query | rows | ms | peak RSS | outcome |
|---|---:|---:|---:|---|
| IC1 | 20 | 2 043 | 3.0 GB | ok |
| IC2 | — | — | — | external sort out of memory (#202) |
| IC3 | 20 | 252 299 | 29.3 GB | ok — back **under** the 300 s budget |
| IC4 | — | — | — | aggregate over pool, 1530.5 MB vs 1024 MB (#203) |
| IC5 | — | — | — | over the 300 s budget (#204) |
| IC6 | 10 | 270 186 | 29.3 GB | ok |
| IC7 | 20 | 19 594 | 29.3 GB | ok |
| IC8 | 20 | 3 137 | 29.3 GB | ok |
| IC9 | — | — | — | external sort asks 5.1 GB, out of memory (#202) |
| IC10 | 10 | 297 261 | 43.8 GB | **ok — completes for the first time on record** |
| IC11 | 10 | 31 306 | 43.8 GB | ok |
| IC12 | 20 | 536 629 | 45.0 GB | ok, 8.9 min — over budget, not cut off (#204, 2.1) |
| IC13 | 1 | 62 | 45.0 GB | ok |
| IC14 | — | — | 19.2 GB and climbing | **executes; killed by hand after 111 min** (see below) |

**8 of 14 answer**, up from 6 in the 2026-08-28 run. The two changes
are IC10 and IC14, and they move in opposite directions worth keeping
apart.

**IC10 survives, which reframes #198 without closing it.** The OS kill
was never the defect; the ~44 GB allocation is, and it is fully intact —
IC10 completes only because an idle 62 GB machine happens to fit it.
Peak `VmHWM` this run: **45.0 GB** against a 1 GiB pool. The chunking
fix stands as specified.

**IC14: #201's error mode is gone at SF1; rows are still unobserved.**
It no longer errors with `endpoint not in scope` — it plans, executes,
and grinds real per-row work, which is what the fix was for. What it
executes *into* is the known-deferred per-row fallback. A stack sample
of the live query reads, trimmed:

```
SortExec → ProjectionStream::poll_next
  → Projector::project_batch
    → ListComprehensionExecExpr::evaluate
      → ReduceExecExpr::evaluate
        → PatternComprehensionSubqueryExpr::evaluate
          → std::thread::scoped::scope → pthread_join   ← blocked
```

IC14's `reduce` over `relationships(p)` evaluates its correlated
weight pattern through `PatternComprehensionSubqueryExpr` — a sub-plan
per element per row, each on a scoped thread — which is precisely the
cost Wave 5.2/5.3 exists to remove, and 5.3's own note named IC14 as
its motivation. Killed after 111 minutes at 100 % CPU with no output.
So IC14's residual is a #204-class latency item gated on 5.2/5.3, not
a correctness one. Filed 2026-08-29 with a verified isolated repro
(`crates/uni/examples/pc_perrow_probe.rs`) as **#206** — the per-row
fallback is 95× the anchored form at N=400 and super-linear in N —
and "IC14 returns rows at SF1" is now #206's acceptance criterion;
#201 can close against it.

**The same sample gives 2.1's cooperative-deadline gap a named worst
case.** The budget mechanism was live in this exact process
configuration — IC5 was cut at 300 s in the same run — yet IC14 ran
22× past it, because the per-row evaluation happens inside
`poll_next` and blocks on a thread join: the future never reaches an
await point where the deadline could fire. IC12 shows the same shape
milder, completing at 536 s against the 300 s budget. Filed as
**#207**, with a repro in the same probe whose overrun scales with
data: a 250 ms timeout returns `UniError::Timeout` after 355 ms at
N=400 and after 7.1 s (29× late) at N=2000. The enforcement gap was
already documented in `impl_query.rs`'s own comments — the outer
timeout plus post-hoc elapsed check, and `check_timeout` reached by
no scan/join/traverse plan — but had no tracking issue.

### Two evidence-hygiene notes from running it

- **Parameter derivation is nondeterministic across invocations.**
  `params::derive` drew `countryName=Finland/Jordan` in one run and
  `Tunisia/Tanzania` in the next, with `person2Id` differing too — and
  each supervisor child re-derives its own parameters, so a supervised
  run that crosses a restart boundary is not internally comparable
  either. Cross-run latency and row-count comparisons in this document
  are looser than the tables imply. Root-caused and filed as **#208**:
  no RNG anywhere — `LIMIT 1` with no `ORDER BY` (`person2Id`, `month`),
  positional `cs[0]`/`cs[1]` indexing into `collect(DISTINCT …)`
  (the country parameters), and tie-unstable `ORDER BY count DESC`
  rankings. `personId` is stable only because SF1's max-degree person
  is unique. The fix is total-order tiebreakers throughout.
- **SF1 latency figures are only comparable machine-idle.** Under a
  competing 20 GB job, IC3 read as over-budget (a wrong conclusion —
  it passes at 252 s alone). The contended run was discarded for this
  reason; treat any future over-budget reading taken on a busy machine
  as unmeasured.

## Plan — revised after triage, 2026-08-29

Supersedes [Plan ahead](#plan-ahead). Everything open was re-ranked
against the ordering principle at the top of this document, and two
facts changed the order.

**The wrong-answer tier is empty.** For the first time since this
document exists, no known silent wrong answer is open. That promotes
the self-defense tier to the top — and that tier now holds exactly two
items, one per resource axis: **#198** (memory: the largest allocator
in the system is invisible to the pool) and **#207** (time: the
deadline cannot preempt execution that never yields). Both have their
fixes already designed.

**Every SF1 non-answer maps to exactly one issue.** The path to 14/14
is enumerable now, which the sequencing below follows:

| query | blocked on |
|---|---|
| IC2, IC9 | #202 (sort spill decision) |
| IC4 | #203 (aggregate over pool) |
| IC5 | #204 latency, with #207 as why the budget doesn't even cut it |
| IC10 | answers today only by luck — #198 makes it lawful |
| IC14 | #206 (per-row comprehension fallback) |

### The order

**0. #208 — bench parameter determinism. First, alone, and before
anything is re-measured.** An hour of mechanical tiebreakers, and it
is this round's Track-0-shaped item: it changes the trust of every
measurement taken after it. It must precede #198's verification
re-run, or before/after runs compare different queries.

> **Done — 2026-08-29.** Every derivation in `benches/ldbc/params.rs`
> now imposes a total order: tiebreak keys on every `ORDER BY … DESC
> LIMIT 1`, `ORDER BY id` on the var-length `person2Id` pick, the
> country pair from a lowest-id friend with the collected list sorted
> in Rust (positional `cs[0]`/`cs[1]` removed), and `month` computed
> as the actual modal birthday month instead of an arbitrary row. A
> `LDBC_DERIVE_ONLY=1` escape in `ldbc_snb.rs` prints the parameter
> block and exits, so the check doesn't have to run IC14. Verified:
> three derive-only runs against the step-0 SF1 store produced
> byte-identical parameter blocks (previously `person2Id` and the
> country pair drifted across runs). The lawful values differ from
> earlier runs' arbitrary ones — `person2Id` 94, countries
> Belarus/Belgium, month 10 — so **post-#208 numbers are not
> comparable to the step-0 table**; IC13 smoke with the new set
> returns 1 row in 38.2 ms (non-vacuous). The next full SF1 run is
> the new baseline.

**1. #198 — chunk the unwind output.** Unchanged in content from the
previous plan; re-framed by measurement. IC10 completing on an idle
62 GB machine is not success — success is IC10 completing where the
allocation *doesn't* happen to fit, so the verification run should
constrain memory (smaller machine or artificial pressure), not just
re-run idle. Expect broad peak-RSS drops (IC3 through IC12 all ride
the 29–45 GB plateau).

> **Shipped, and it does not close #198 — 2026-08-29.**
> `GraphUnwindStream` now expands row by row and emits in
> `session_config().batch_size()` chunks, holding the cursor across
> `poll_next`; peak is `chunk × columns` plus the one list being
> expanded, instead of `input_rows × list_size` in a single
> allocation. Verified at the operator: `UNWIND range(1, 20000)`
> emits exactly 3 batches (8192 + 8192 + 3616) where it previously
> emitted 1.
>
> **The expectation quoted above was wrong, and measurement is what
> showed it.** Both runs below are SF1, post-#208 parameters, under
> `systemd-run --scope -p MemoryMax=16G -p MemorySwapMax=0`:
>
> | query | before | after |
> |---|---|---|
> | IC3 | **SIGKILL** | **SIGKILL** |
> | IC6 | 300 s budget exceeded, no rows | **10 rows, 293.5 s**, peak 15083 MiB |
> | IC7 | 20 rows, 23586 ms | 20 rows, 19532 ms |
> | IC8 | 20 rows, 3830 ms | 20 rows, 3174 ms |
> | IC10 | **SIGKILL** | **SIGKILL** |
> | IC11 | 10 rows, 37970 ms | 10 rows, 29381 ms |
> | IC12 | **SIGKILL** | **SIGKILL** |
> | IC14 | **SIGKILL** | **SIGKILL** |
>
> IC2/IC4/IC5/IC9 fail identically before and after, as intended —
> they are #202, #203 and #204.
>
> **IC3, IC10 and IC12 contain no `UNWIND`.** Only IC6 and IC14 do.
> So the 29–45 GB plateau is *not* attributable to this operator for
> three of the four killed queries, and the sentence above predicting
> "broad peak-RSS drops (IC3 through IC12)" could never have come
> true. #198's headline — that the peak lives in
> `GraphUnwindStream::build_output_batch` — holds for the query #184
> actually measured (IC6) and is unevidenced for the rest. **Where
> IC3/IC10/IC12's allocation lives is now an open question and wants
> its own issue**, rather than being folded into #198 retroactively.
>
> On the two queries that do reach the operator: IC6 goes from
> returning nothing to returning 10 rows, and IC14 is still killed —
> its cost is the per-row comprehension fallback (#206), upstream of
> the unwind. IC6's margin is thin (293.5 s of a 300 s budget), and
> its higher peak (15083 vs 11934 MiB) is not a regression: the
> before figure is the high-water mark at the moment the query was
> cut off, not the cost of a completed one. Note also that
> `peak_rss_mib` reads `VmHWM`, a *process* high-water mark, so
> within one child the column is monotonic and only the query that
> set a new high is telling you anything.
>
> **#198's own success criterion — "the process surviving with peak
> RSS bounded" — is therefore not met, and the issue stays open.**
> The change is worth keeping on its merits (IC6 answers; the
> unbounded allocation is gone from the operator), but the bench
> still takes four kills under a 16 GiB cap.
>
> Harness note for anyone repeating this: systemd's default
> `OOMPolicy=stop` tears down the whole scope when the kernel OOM
> killer fires, which kills the bench's supervisor — the very thing
> that records the abort and restarts after it. The first baseline
> attempt died silently at IC2 for this reason. Pass
> `-p OOMPolicy=continue`.

**1b. The collected-list carry — two of the four kills, and two
misattributions. 2026-08-29.**

The open question above was answered by probing it.
`crates/uni/examples/collected_list_carry_probe.rs` (allocator-
instrumented, so it can report a peak that goes back down, which RSS
cannot) showed the excess over a control growing as `rows × elements`,
~300 B per pair — and a third arm showed a list **in scope but never
read** costing *identically* to one that is read. The price is the
column copy, not the predicate: `collect()` yields one opaque
`LargeBinary` blob, and `CrossJoinExec::to_array_of_size` plus every
traversal `take` re-copies it per row.

The fix interns a collected list past 1 KiB behind a 9-byte handle
resolved inside `cypher_value_codec::decode`, so the physical type
stays `LargeBinary` and no consumer changes. Same SF1 protocol,
16 GiB cap:

| query | before | after |
|---|---|---|
| IC10 | **SIGKILL** | 10 rows, 26.5 s |
| IC12 | **SIGKILL** | 20 rows, 79.9 s |
| IC3 | **SIGKILL** | **SIGKILL** |
| IC14 | **SIGKILL** | **SIGKILL** (#206) |

Probe excess: 26.7 → 232.5 MiB across the scaling range, now 0.2 →
0.9 MiB, flat at 1.0×.

Two predictions in this document were wrong, and both are worth
keeping visible because the same reasoning error produced each:

- **IC10 was called out of scope** on the grounds that its
  `collect(post)` is grouped and therefore "not an invariant column".
  True and irrelevant — interning applies per group, and each
  friend's post list clears the threshold. It was fixed by a change
  argued not to cover it.
- **IC3 was attributed to this defect** on an extrapolation — "~10²
  cities × ~10⁶ rows ≈ 30 GB, against the 31.8 GB recorded". The list
  is **20 cities**. With a byte-based threshold that admits its ~6 KB
  list, IC3 is *still* OOM-killed, so the carry is not its cause.
  That is now twice IC3 has been attributed to a mechanism it does
  not use — first `UNWIND` (#198), then this.

**IC3 needs its own investigation**, and the standard set by the two
successes is a probe that can falsify a named hypothesis, not an
arithmetic coincidence. The candidates its query actually contains:
the `KNOWS*1..2` var-length expansion, and the `WITH DISTINCT` above
it.

**2. #207 — the minimal deadline checkpoint.** A deadline check
between per-row sub-plan evaluations in
`PatternComprehensionSubqueryExpr::evaluate` caps the overrun at one
evaluation instead of all of them. Deliberately **not** folded into
#206: the checkpoint outlives the hoist — it defends against the next
non-yielding site, whatever it is. The broader audit of
blocking-inside-`poll` sites is the follow-up, not the gate.

**3. #203 + the marker audit — one discriminating check, then
measure.** #203's first step is unchanged: determine whether IC4 is a
second instance of #196's `"*"` group key (one match arm) or a
genuinely large aggregate. The marker audit (`Distinct` at
`df_planner.rs:1061`, `Union` dedup at `:5822`, `Sort`/`Window` keys)
rides along — it is the same question asked systematically, and #196
proved it is answered by measuring, not reading.

**4. #202 — a decision, not a defect.** Configure DataFusion spill so
a large sort completes, or declare above-pool sorts out of scope and
make the error say so. Timebox it; either outcome closes IC2/IC9's
issue.

**5. #199 — the `COUNT { UNWIND outer … }` gap.** A valid shape that
errors loudly; conformance tier, after defense and memory.

**6. #206, with #204 and Wave 5.2–5.5 — the performance track.**
Hoist the uncorrelated comprehension (5.2), batched anchoring for the
correlated one (5.3), then 5.4's latency attribution — witnessed by
the #175 counters and finally comparable thanks to #208. 5.5 stays
gated on measurement. Acceptance is written into #206: the probe's
ratio column stops growing with N, and IC14 returns rows at SF1 —
at which point #201 closes against it.

**7. Hygiene, parallelizable anytime: #205 and #195.** The
vacuous-fixture audit and the counter bench are read-mostly work that
contends with nothing above; their position here is about focus, not
dependency.

**8. #200 — keep watching, keep it recorded.** Still unexplained,
still not recurring.

**9. Track 0 — unchanged.** Still the one thing that would have found
most of this document's closed items without anyone probing; still
blocked on a Neo4j instance, not on code.

---

## Status — 2026-08-31

Steps 1 and 2 of the post-triage order are discharged (#198, then #209
as an insertion ahead of #207). SF1 has **not** been re-run since, and
that is the one thing standing between this and the rest of the queue.

### #198 — chunked, and it fixed less than the plan claimed

`GraphUnwindStream` now fills a bounded chunk and carries the remainder
in a `pending` slot rather than materialising the whole fan-out. It
moved IC6 and nothing else: three of the four SF1 kills contain no
UNWIND at all, so the plan's framing of this as "the last thing between
SF1 and a clean run" was wrong. Recorded at the time rather than
discovered later.

What actually carried IC10 and IC12 was a separate defect the chunking
could not reach: a collected list is *copied per row* when carried
through a projection. Interning it behind a handle (`TAG_HANDLE` in
`cypher_value_codec`, with a `HandleScope` bounding the lifetime) makes
carrying one cost 9 bytes a row. The decode arm fails closed rather
than returning a stale value.

### #209 — columnar traversal hydration, and the lift underneath it

Reaching a property through a traversal cost 86x reading the same
column through a scan, and scaled with the target *table* rather than
with rows produced. Both are fixed, measured on
`hydration_path_probe`:

| arm (60 000 rows produced) | before | after |
|---|---:|---:|
| traverse, 1 property, 300k-row table | 1620.9 MiB | **226.5 MiB** |
| decoy sensitivity (60k -> 300k table) | 10.6x | **1.18x** |

Two changes, and they are **not independently applicable**: chunking the
vid list pays only where the result is consumed columnarly and released
per chunk. Applying the same chunking to the map path was measured as an
18% regression (1626.3 -> 1929.5 MiB) and is recorded as a negative
result in the hydration proposal, not quietly dropped.

The pipeline then moved from `uni-query` to `uni-store` across six
pure-relocation commits, which is what let `uni-algo`'s projection read
properties columnarly instead of hand-rolling the same loop.

**The convergence that motivated the lift was declined.** Converging
`property_manager`'s row-wise MVCC onto the Arrow one is not safe as
scoped: the row-wise side is four implementations, not one, and they
disagree with each other more than any disagrees with the columnar one;
and the columnar path cannot express presence-vs-null, CRDT merge
across versions, or edge op-replay. The investigation was kept for what
it found instead — see below.

### Found while doing the above — nine defects, none of them listed here

Five MVCC/L0 defects, each with a fail-before test:

- `mvcc_dedup_batch_by` picked an arbitrary row on a version tie
  (`lexsort_to_indices` is unstable), live on **every** columnar scan.
- A live row at the same version as a tombstone could resurrect a
  deleted id.
- The main-edges fallback resurrected a tombstoned edge — the vertex
  path's C2 hole, which the edge path had no guard for.
- `get_batch_labels` ignored `vertex_label_overwrites`, so `REMOVE
  n:Label` came back.
- A partial L0 row shadowed storage instead of merging with it.

Four more, from the divergence analysis that followed:

- **An unflushed CRDT written in its string form read back as NULL**
  through any columnar read. Cypher stores a CRDT literal verbatim, and
  the columnar builder could only parse the value form. Pre-existing on
  scans; the hydration work extended it to traversal targets.
- **`overflow_json` precedence disagreed three ways** (declared column
  wins / blob wins / blob wins *and persists* in semantic compaction).
  Unified on declared-column-wins. Latent — no writer produces a
  colliding row — but compaction bakes its pick into the table, so it
  was disarmed rather than left.
- **`is_lance_conflict` classified retryable Lance errors by message**
  and missed the exact case its own doc comment named. See below.
- **Variable-length paths dropped schemaless properties.** `RETURN n`
  over `(h)-[:R*1..2]->(n)` returned the declared columns and silently
  lost the rest; the same match at fixed length returned all of them.
  One stray `properties.is_empty()` in
  `sanitize_vlp_target_properties`. A unit test asserted the defect as
  correct behaviour.

### The `async_flush_repro` "known flake" was a real bug

`storage::async_flush_repro::{r2,r3}` had been carried as a
load-sensitive flake, including in this document's own guidance. It was
neither.

Two concurrent flush streams both created the same per-label UID index
dataset; Lance failed the loser with `Dataset already exists`, which
`is_lance_conflict` did not recognise. The losing flush's rotated L0
stayed stranded on `pending_flush` with nothing to re-flush it, and
`flush_to_l1` correctly refused the barrier. **6 of 12 runs failed
before, 0 of 12 after.** Classification is now by typed `lance::Error`
variant, which also caught `TooMuchWriteContention` — retryable, and
still misclassified after the first fix.

`plan_cache_smoke`, carried on the same list, shows no flakiness at all:
6/6, activation 100.0% against an 80% floor, rows varying 2.7% against
52% headroom.

**The tell was visible throughout and read past three times: both tests
failed *more in isolation than under parallel load*, and isolation is
the absence of load.** A red test annotated as expected-red stops being
a signal; the repro file's header recorded "~50% of runs" as a known
property, and has been rewritten to name the cause instead.

### What this round is evidence for

The same pattern as the #197/#201 round this document already recorded,
in a new place: **a check that resembles verification but cannot fail.**

- A "known flake" label repeated three times before anyone measured a
  failure rate.
- A probe whose output filter only detected two of the four properties
  it was meant to check, so it reported a run containing the VLP defect
  as clean.
- A unit test asserting the VLP defect as the contract, so the bug had
  coverage confirming it.
- `repro_03`/`repro_07` cannot flake red but *can* flake falsely green,
  since they rely on Lance preserving an adverse row order.

Every one of these passes for free. Track 0 remains the structural
answer; in the meantime, the cheap local rule is that a test which has
only ever been observed passing is not yet evidence — which is what
`docs/testing/reverts/` and `teeth_validate.sh` exist to enforce.
(Editing code inside a block a revert patch deletes silently
un-validates that tooth; `every_revert_patch_still_applies` caught
exactly that this round, and the patch was regenerated *and* re-run to
confirm the control still fails under it.)

### Outstanding — the gate before the rest of the queue

**SF1 has not been re-run since #198 landed**, so:

- **#209's acceptance criterion is unmeasured.** The hydration proposal
  asks that `ic3_stage_probe` stage 7 fall well below its current
  10.5 GB, and explicitly declines to predict whether IC3 completes.
  Only the micro-probe has been measured.
- **Nine correctness fixes have landed since**, and one cuts against the
  memory goal: the VLP fix adds `_all_props` — the whole overflow blob
  per target row — whenever a wildcard is requested on a VLP target, and
  ten of the fourteen queries use variable-length paths. Reading their
  RETURN clauses the trigger does not appear to fire (IC1 returns named
  properties, IC13 `length(path)`, IC14 a derived list), but that is a
  read, and this round is a catalogue of reads that were wrong.

One run under the established protocol — `systemd-run --user --scope -p
MemoryMax=16G -p MemorySwapMax=0 -p OOMPolicy=continue`, `OOMPolicy`
being required or systemd kills the bench's own supervisor — closes
both.

Suite state at the tip: `uni-db` 2783/2783, `uni-store` + `uni-query`
1482/1482, clippy and the workspace check clean.

### The order from here

Unchanged from 2026-08-29 apart from the discharged items: SF1 first,
then **#207**, **#203** + the marker audit, **#202**, **#199**,
**#206/#204/Wave 5.2–5.5**, hygiene (**#205**, **#195**), **#200**, and
**Track 0** last.

---

## Status — 2026-09-01

SF1 was re-run and **13 of 14 queries answer**, against 7 before. The four
that had never completed — IC2, IC3, IC9, IC14 — all do. Only IC5 remains,
over its 300 s budget, which is #204 and untouched.

Six defects were fixed to get there, three of them found on the way and
filed during the work. Two of the six are silent wrong answers, so the
wrong-answer tier — recorded as empty on 2026-08-29 — was not.

### SF1, measured 2026-09-01, idle machine

`systemd-run --user --scope -p MemoryMax=16G -p MemorySwapMax=0 -p
OOMPolicy=continue`, default 1 GiB `max_query_memory`, store reused.

| query | rows | ms | peak MiB | scans | against 2026-08-29 |
|---|---:|---:|---:|---:|---|
| IC1 | 20 | 2 102.8 | 3514 | 7 | |
| IC2 | 20 | 4 594.7 | 3514 | 53 | **was FAIL (#202)** |
| IC3 | 20 | 100 532.8 | 3514 | 4 | **was SIGKILL** |
| IC4 | 10 | 3 808.4 | 3514 | 35 | **was FAIL (#203)** |
| IC5 | — | — | 3514 | — | over budget, unchanged (#204) |
| IC6 | 10 | 8 817.7 | 3551 | 2 | 293 500 → 8 818 |
| IC7 | 20 | 18 721.1 | 3551 | 4 | flat (19 532) |
| IC8 | 20 | 3 262.8 | 3602 | 5 | flat (3 174) |
| IC9 | 20 | 36 416.1 | 3847 | 354 | **was FAIL 5.1 GB (#202)** |
| IC10 | 10 | 3 826.7 | 3977 | 3 | 26 500 → 3 827 |
| IC11 | 10 | 32 217.5 | 3977 | 8 | flat (29 381) |
| IC12 | 20 | 23 584.3 | 4090 | 86 | 79 900 → 23 584 |
| IC13 | 1 | 21.6 | 4090 | 2 | |
| IC14 | 1 | 53 014.2 | 7748 | 2 | **was SIGKILL** |

Peak RSS tops out at 7 748 MiB against a previous ceiling of 15 772 MiB
plus four OOM kills.

**Do not read the speedups as pure performance.** #211 below means every
earlier run returned `Comment`s as `Post`s on traversal targets — a set
roughly 3× too large — so IC6's 33× and IC10's 7× are part real
improvement and part no-longer-doing-wrong-work, and this data cannot
separate them. The honest claim is that these queries now return the right
answer quickly, not that they got 33× faster.

The flat rows are the cleaner evidence. IC7 and IC8 barely move, which
settles the 1.4–3× slowdown reported mid-round and withdrawn: that was
machine contention, not code. It also bounds the chunking cost measured
below — a 5.9% regression in isolation does not surface here.

### What was fixed

**#207 — `query_timeout` could not preempt execution.** Nine operator-level
`check_timeout` calls existed and none could ever fire:
`GraphExecutionContext::deadline` was never populated. `deadline` is a
private field whose only writers are `with_parts` (called with `None`),
`with_deadline` and `from_query_context` — the latter two with zero callers
anywhere. Enforcement was split across two engines with only the
row-oriented one wired. Fixed by cloning the context in `take_graph_ctx`
rather than rebuilding it from the base constructor and re-attaching six
fields by hand, which also restores `warnings` and the `handle_scope`
bounding the interned collected lists. A 250 ms budget went from 7 145 ms
to 255 ms at N=2000, flat across N.

**#210 — the cursor path lost an abort's typed variant.** Found by
activating #207: `into_stream_error` lacked the cancellation and timeout
arms its sibling has, so an operator-raised abort surfaced as a generic
`UniError::Query`. Unreachable before, because no operator could raise one.

**#211 — a traversal target's label was ignored above 100k vertices.**
`rebuild_vid_labels_index` capped its startup scan at `.with_limit(100_000)`,
and the traversal's label filter *keeps* rows whose vid does not resolve.
So `MATCH (p:Person)<-[:HAS_CREATOR]-(post:Post)` returned 3 055 774 rows —
`Comment`s included — against the 1 003 605 the scan-anchored form returns,
and the labelled and unlabelled forms returned identical counts. A silent
wrong answer on any graph over 100k vertices; every SF1 measurement before
this one is void for anything touching those labels.

**#203 — a labelled traversal target materialised its whole entity.**
`hasLabel` was missing from `FUNCTION_SPECS`, so the predicate the planner
synthesises for every labelled traversal target took the unknown-function
fallback and marked its entity `"*"`. Third instance of that fallback
(#62, #134). Guarded now at plan level rather than by name, so the next
synthesised predicate cannot repeat it.

**#212 — `NOT(a IN (x) AND a IN (y))` disagreed with Lance on NULL columns.**
Found by proptest during unrelated work. Second instance of a shape
`eval.rs` documents; the first covered NULLs in the values list, this one
arrives via the column.

**#202 — IC2 and IC9's sorts.** The issue attributed it to missing spill
configuration; a disk manager was available the whole time. A sort spills
*between* batches and every operator fed it exactly one — `batch_size` was
honoured in one place in all of `df_graph`. Scan and traversal now slice
their output, and the traversal hydrates a chunk at a time rather than
materialising the whole expansion set: IC9 produced one batch of 2 869 951
rows, and `RecordBatch::slice` is zero-copy, so slicing alone left the
parent pinned and the sorter unable to free anything.

### Two things measured and rejected

**Pushing `LIMIT` into `SortExec`.** Every `ORDER BY … LIMIT n` in this
engine is a full sort — the physical planner builds plans directly and
never runs DataFusion's limit-pushdown pass. Adding it reads like free
money and is a regression: `ExternalSorter` spills, `TopK` does not, so it
removes the spill the slicing had just enabled. IC2 went from 20 rows back
to failing at `TopK[0]` with 977.4 MB already allocated. The real fix for
that class is a TopK that can spill, not the one-line pushdown.

**Chunked hydration is not free.** Measured on a control chosen to isolate
the cost — a large fan-out traversal that hydrates a property with no sort
above it, two runs a side: 4.74/4.75 s and 1 213/1 204 MiB unchunked
against 5.07/4.96 s and 1 141/1 135 MiB chunked. About 6% slower for ~87
hydration round trips instead of one. Only traversals whose expansion set
exceeds `batch_size` chunk at all.

### What this round is evidence for

The same pattern as the two rounds before it, and worth stating in its
strongest form: **an instance fixed is not a class fixed.** Three of the six
defects here were second or third instances of mechanisms already found,
fixed, and documented with an accurate comment explaining them — the
registry fail-open, the `InList` rewrite, and the limit-path patch that
covered one of two arms. In each case the earlier author understood the
mechanism completely and scoped the fix to what was in front of them.

What found the follow-ups was not better analysis. It was constructing the
next variant by hand and running it: the multi-value `IN` case, the
intermediate join vertex, the second limit path. Each failed on first run.

A related note on instruments. `EXPLAIN` prints the logical plan;
`UNI_DUMP_PHYSICAL=1` prints the physical one, and every #202 finding was
visible there — the `SortExec` with no fetch, no coalescing between
traverse and sort, and a root `Person` scan projecting every column plus
`_all_props` *and* `overflow_json` for a query that reads `root.id`. That
last one is not yet filed.

Four non-discriminating checks were written and caught during this round —
a cancellation test satisfied by the guard rather than the operator, a
110k-vertex fixture that passed with the defect restored, a `count(*)`
benchmark control that never entered the code path it was measuring, and a
deadline assertion whose flakiness grew as the fix improved. Each looked
reasonable and each proved nothing. The cheap rule that caught all four:
before believing a result, check that the code under test actually ran —
an unchanged number usually means the change did not execute, and a
suspiciously identical one means the same.

### The order from here

**IC5 (#204)** is the only query left. Then **#199**, **#206**, Wave
5.2–5.5, hygiene (**#205**, **#195**), **#200**, and **Track 0** last.

Open and unfiled: a spillable TopK, incremental scan materialisation (the
scan still builds its whole result before slicing), and the root-entity
widening visible in IC9's physical plan.

Suite at the tip: 4549/4549 across `uni-db`, `uni-query`, `uni-store` and
`uni-query-functions`; fmt and clippy clean.
