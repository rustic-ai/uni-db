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

All committed on `feat/test-harness-track-e`, **not pushed**. Nine product fixes,
five of them silent wrong answers; LDBC went from 7 of 14 answering to 9 of 14.
TCK held at 3980/3980 throughout.
