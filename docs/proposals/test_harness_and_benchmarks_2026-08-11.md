# Test Harness & Benchmark Strategy

**Date:** 2026-08-11
**Status:** Proposal (rev 3 — incorporates second review of 2026-08-12)
**Scope:** Correctness oracles, fault-injection, perf-regression gating, external benchmarks

> **Rev 3 changes (C1 executability).** DQP fixture scale is split into three
> tiers with a measured row budget, because 2/3 of generated cases carry no
> `WHERE` at all and a 50k-vertex fixture × 50k cases is billions of rows
> (§3.5.1). "Tier 2 is stateless" was false — creating a fork flushes L0 and
> persists registry state, and pinning requires a snapshot that also flushes —
> so Tier 2 becomes *prepared-state* with its own driver (§3.2, §3.4.1). The
> pinned witness was backwards and is corrected. The plan-cache lever cannot
> activate through `run_bag`, which builds a fresh session per query while the
> plan cache is session-local, so the driver now carries a session context
> (§3.4.2). Bag equality gains an explicit admissibility contract for floats,
> `LIMIT` and nondeterministic constructs (§3.5.2). C2's failpoints move from
> `registry.rs` alone to the orchestration files that actually span the 2PC
> windows (§4.2), and compaction is split into two separate crash matrices
> because Lance's file merge is opaque behind `compact_files` (§4.3).

> **Rev 2 changes.** The universal "plans must differ" vacuity rule was invalid
> and is replaced by per-lever *activation witnesses* (§3.3). The stateful-lever
> driver problem is addressed with a batch-per-state driver (§3.4). The
> fault-injection item was written against a stale baseline — the repo already
> ships fail-rs failpoints and a substantial crash/reopen suite — and is
> rewritten and re-scoped to the three genuinely uncovered areas (§4). The perf
> gate now requires a repeatability pilot before any target is gated (§7).
> Inventory figures corrected against executed artifacts (§2.5). Item
> identifiers renamed to C*/B* so they no longer collide with P0/P1/P2
> priorities.

---

## 1. Summary

uni-db's *correctness* harness is strong: openCypher + Locy TCK conformance,
proptest, loom/shuttle model checking, TLP/NoREC metamorphic oracles, a
naive-Datalog reference oracle for Locy, libFuzzer targets, a ThreadSanitizer
lane, **and** a fail-rs failpoint framework with crash/reopen suites across
commit, flush, WAL, index and fork-recovery paths.

Its *performance* story is the inverse: 18 Criterion bench targets, three of
which run nightly, uploaded as artifacts for "manual trend comparison (no
external regression service)" (`nightly.yml:10-11`). There is no baseline, no
threshold, no gate, and no externally-comparable number in the repo.

| # | Work item | Class | Priority |
|---|---|---|---|
| **C1** | **DQP** — differential query plans oracle | correctness | **P0** |
| **B1** | **iai-callgrind perf gate** (pilot-qualified) | performance | **P0** |
| C2 | Fault injection: fork 2PC, compaction, hard-kill harness | correctness | P1 |
| C3 | Elle — serializability checking for SSI | correctness | P1 |
| C4 | Coverage map, miri on pure crates, `cargo-deny`, fuzz-on-PR | correctness | P1 |
| B2 | LDBC SNB Interactive SF0.1/SF1 | performance | P1 |
| B3 | ann-benchmarks recall/QPS + BEIR nDCG@10 | performance | P1 |
| B4 | Contention curves (throughput vs abort-rate) | performance | P2 |

**Exactly two items are P0: C1 and B1.** They are independent, parallelizable,
require no new external services, and address the two demonstrated weaknesses —
silent wrong answers from execution-path divergence, and unguarded performance.
Everything else is P1 or below.

---

## 2. Motivation

### 2.1 The bug class we keep shipping

The dominant defect in the issue history is not a crash and not a parse error.
It is a **silent wrong answer produced when one execution path disagrees with
another**:

| Issue / fix | Divergence |
|---|---|
| Lance `"col"` string-literal range fusion | Hash-index + range predicate fused into a data-independent constant → `[]` |
| #103 / #106 | Fork branch scan returned wrong rows until `use_scalar_index(false)` |
| #135 | `expand_batch` read properties L0-only → NULL after flush |
| #99 | `(a)-[:R]->(b:B)` returned 0 rows on a fork |
| #97 | Fork ignored unflushed parent L0 |
| #134 | `"*"` projection leak — correct results, 60× slower dense scan |
| #110 | Fork reverse-adjacency not warmed |
| #166 (systemic) | MERGE + shortestPath + QPP all fail-open on a missing map |

Every one is a case where **query Q under configuration A** and **query Q under
configuration B** return different results, and only one is right.

TLP and NoREC cannot catch these. Both are *self*-differential: they compare a
query against a semantically-equivalent rewrite of itself, executed under the
*same* configuration. If the storage path is wrong, both sides are wrong
identically and the oracle passes.

The missing oracle is the third from the SQLancer family: **DQP — Differential
Query Plans.**

### 2.2 SSI is claimed but not proven

`crates/uni/tests/common/ssi_invariants.rs` is a good hand-written invariant
suite, and `ssi_resilience.rs` covers crash-and-reopen atomicity. Hand-written
invariants find the anomalies the author enumerated. For a serializable
isolation claim, the industry expectation is a black-box history checker that
finds the ones nobody enumerated.

### 2.3 Three durability areas remain genuinely uncovered

This is *not* a general fault-injection gap — see §4.1 for what already ships.
The uncovered areas are narrow and specific:

1. **`fork/registry.rs` contains zero `fail_point!` sites.** Crash-between-2PC-
   phases is simulated indirectly, by hand-constructing a `Pending`/`Tombstoned`
   registry and calling `recover_forks`, or by faulting Lance
   `create_branch`/`delete_branch` via env var. Nothing crashes *between*
   `begin_create`→`finish_create` or `begin_drop`→`finish_drop`.
2. **The compaction path has no fault injection and no crash tests at all.**
   `background_compaction_test.rs` covers config, triggers and status only.
3. **Every "crash" in the suite is a panic-in-task followed by `drop(db)`,**
   which still runs the shutdown flush. `ssi_resilience.rs` itself flags the
   confound in a comment. There is no `abort()` / SIGKILL / child-process
   harness anywhere in the workspace.

### 2.4 Performance is entirely unguarded

- No perf job in `pr.yml`, `ci.yml`, or `release.yml`.
- `nightly.yml` `bench` runs 3 of 18 targets, uploads `target/criterion/`, stops.
- No baseline is committed; nothing consumes the artifact.
- No externally-comparable number exists: no LDBC, no ann-benchmarks, no BEIR.

Wall-clock benchmarking on shared GitHub runners has ±20–30% variance, which is
why a wall-clock gate would be unenforceable. The fix is to change the *metric*
to something deterministic — subject to the qualification pilot in §7.2.

### 2.5 Inventory (verified 2026-08-11)

Figures corrected from rev 1, which conflated several different counts:

| Quantity | Value | Note |
|---|---|---|
| Workspace members | **35** | `crates/` 29 + `bindings/` 6 |
| `default-members` | 27 | excludes `uni-tck`, `uni-locy-tck`, `uni-locy-oracle`, 5 wheel siblings |
| `[[bench]]` stanzas, `crates/uni` | **18** | +1 auto-discovered file, §7.5 |
| `[[bench]]` stanzas, workspace | 21 | +2 `uni-plugin-wasm`, +1 `uni-plugin-rhai` |
| Cypher TCK, **executed** | **3926 scenarios, 3925 passed** | `compliance_reports/schemaless/last_run_report.md`, 2026-06-12 |
| Locy TCK, **executed** | **501 scenarios, 501 passed** | `locy_compliance_reports/schemaless/last_run_report.md`, 2026-06-12 |

Static `.feature` scenario headers (1642 Cypher / 519 Locy) are **not** the
right metric: uni-tck has 277 `Scenario Outline`s with 277 `Examples:` tables
that expand at runtime to 3926, while Locy's executed count (501) is *lower*
than its static count (519) because 18 are filtered. Any future claim about
conformance breadth must cite an executed artifact, never a grep.

---

## 3. C1 — DQP: Differential Query Plans oracle  *(P0)*

### 3.1 Principle

For a query `Q` and two result-neutral execution configurations `A` and `B`:

```
bag(Q under A) == bag(Q under B)
```

Any inequality is a bug in exactly one of the two paths. The oracle does not
need to know which — the diff plus the rendered query localizes it.

### 3.2 Lever taxonomy

Levers split by whether they can be applied **within a single `Uni` instance**.
This matters because `diff/mod.rs:16-21` warns that bags from different
instances are incomparable (VIDs differ).

#### Tier 1 — intra-instance, sequential (identity-preserving, **stateful**)

| Lever | Mechanism | Targets |
|---|---|---|
| **L0 vs L1** | run `Q`; `Session::flush()` (`session.rs:917`); run `Q` | #135, #97, #121 — "correct before flush, wrong after" |
| **Index absent vs present** | run `Q`; `CREATE INDEX`; run `Q` | the Lance `"col"` fusion bug, `LanceFilterGenerator` pushdown |
| **Plan cache cold vs warm** | run `Q` twice | plan-cache key collisions (`session.rs:2174`) |
| **Pre- vs post-compaction** | force compaction between runs | tombstone reads, `max_l1_runs` merge paths |

These are **stateful and irreversible** — see the driver problem in §3.4.

#### Tier 2 — intra-instance, parallel (identity-preserving, **prepared-state**)

| Lever | Mechanism | Targets |
|---|---|---|
| **Primary vs pristine fork** | `Session::fork(name)` (`session.rs:332`) with zero fork-local writes | highest value — see below |
| **Pinned vs live** | `pin_to_version` (`session.rs:1012`) at the current version | snapshot read paths |

> **Correction (rev 3): these are not stateless.** Rev 2 claimed Tier 2 could
> reuse `drive()` unchanged. It cannot. `create_fork_2pc` flushes the parent's
> L0 before capturing the fork point (`api/fork.rs:300-324` — deliberately, as
> the fix for #97) and then persists a registry entry, an allocator and one
> Lance branch per dataset. Pinning likewise needs a snapshot, and
> `Uni::create_snapshot` calls `flush_to_l1(Some(name))` (`api/mod.rs:991-1002`).
> Both mutate the instance the shared read-only driver assumes is immutable, and
> both are far too expensive to pay per case.
>
> The resolution is a **prepared-state driver** (§3.4.1): perform the flush,
> fork creation and snapshot **once, before any case runs**, then hold both
> sides open read-only for the whole run. That preserves the property that
> mattered — no per-case state change — without the false claim that no state
> change occurs at all.

The primary-vs-pristine-fork lever is disproportionately valuable because the
fork read path differs from primary in four independent, documented-as-
result-neutral ways:

- `use_scalar_index(false)` is hardcoded on the branch scan (`lance.rs:193`,
  `lance_branch.rs:297,359`) — so this lever **is** a scalar-index differential.
- Multivector search on a fork takes the brute-force branch scan and skips both
  the MUVERA/FDE fast path and `MULTIVECTOR_MAX_CANDIDATES` truncation
  (`search_procedures.rs:409-471`).
- `rewrite_for_fork_fusion` (`planner.rs:10469`) emits `FusedIndexScan` when a
  fork-local index is registered.
- Fork sessions start with a fresh empty plan cache (`session.rs:298`).

A pristine fork differing from its parent is, per the fork contract,
unambiguously a bug. #99, #103, #97, #110, #135 are all in range.
**This lever ships first** — not because it is free of setup, but because its
setup is paid exactly once per run (§3.4.1) rather than once per case.

#### Tier 3 — cross-instance (identity-*breaking*)

Different `UniConfig` requires two instances, so VIDs may differ. Usable only
with an identity-free projection (§3.5). Candidates from
`uni-common/src/config.rs`: `batch_size` (:420), `parallelism` (:417),
`partial_lance_writes` (:502), `async_flush_enabled` (:543),
`auto_flush_threshold` (:426), `compaction.max_l1_runs` (:449),
`fork_index_build_threshold` (:603) / `disable_fork_index_builder` (:614),
`index_rebuild.auto_rebuild_enabled` (:486).

**Explicitly excluded as not result-neutral:** `ssi_enabled` (:639) changes
concurrent semantics; `defer_embeddings` (:519) changes in-transaction reads of
the embedding column; the ANN recall knobs `nprobes` / `refine_factor` /
`ef_search` (`uni-store/src/backend/types.rs:497-506`) change recall by design
and get a directional oracle instead (§3.7).

### 3.3 Non-vacuity: per-lever activation witnesses

**A differential oracle where both sides exercise the same machinery is
vacuously green.** The existing seed (`metamorphic/seed.rs:72`) is 12 `Person`
+ 4 `Company` + 10 `WORKS_AT`, `Uni::in_memory()`, no indexes, never flushed —
Lance will never engage an index at that size, so DQP against it would be ~100%
vacuous. The repo already contains a live instance of this failure mode:
`fork_index_recall_bench.rs` reports recall@10 = 1.000 because at n=1000 Lance
brute-forces and the index under test is never used.

**Rev 1 proposed a universal `ensure_plans_differ` check. That was wrong.**
Several valid levers produce *identical logical plans* and differ only in
execution state — plan-cache cold/warm is identical by construction, and
L0/L1 and pre/post-compaction can also plan identically while reading entirely
different machinery. A universal plan-difference rule would reject exactly the
levers most worth testing.

The correct formulation is a **per-lever activation witness**: a lever-specific,
observable predicate asserting that side A and side B genuinely exercised
different machinery. Each lever must supply one.

```rust
trait Lever {
    fn name(&self) -> &'static str;
    async fn side_a(&self, db: &Uni) -> Result<(RowBag, Witness)>;
    async fn side_b(&self, db: &Uni) -> Result<(RowBag, Witness)>;
    /// Did the two sides actually exercise different machinery?
    fn activated(&self, a: &Witness, b: &Witness) -> bool;
}
```

| Lever | Activation witness |
|---|---|
| L0 vs L1 | rows-served-from-L0 counter > 0 on side A **and** == 0 on side B |
| Index absent vs present | plan text contains a pushdown/index-scan node on B and not on A |
| Plan cache | `QueryResult.plan_cache_hit` (`uni-query/src/types.rs:41`) false on A, true on B — **plans are identical by design; the witness is the cache flag, not the plan** |
| Compaction | L1 run/fragment count strictly decreased between A and B |
| Pristine fork | a **runtime** counter showing the query actually executed the branch scan path on side B |
| Pinned vs live | pinned snapshot resolves to **the same** version the live side reads (see below) |

Two corrections from the rev-3 review:

**"A branch exists for the table" is a configuration check, not proof of
execution.** `BranchedBackend` decides per call whether to take the branch path
or fall back to primary; a branch can exist while the query that ran never
touched it. The witness must be a counter incremented on the branch scan path
itself, so it observes what executed rather than what was configured. This is
the strongest case in the observability sub-task below.

**The pinned witness was backwards.** Rev 2 asserted the pinned snapshot id
*differs* from the live version. That inverts the oracle: if the two sides read
different versions they see different data, so a bag inequality is expected
behaviour, not a bug — the test would be unsound, not merely unexercised. The
lever's premise is that a pinned read at version *V* and a live read at the same
*V* must agree, so the witness asserts **equality** of the reference version,
plus a counter showing the pinned side took the snapshot read path. Any run
where a write advanced the live version mid-batch must be discarded, not
compared.

Some witnesses need counters that are not exposed yet (L0-served rows, L1 run
count, branch-scan executions, snapshot-path executions). **Sub-task: audit
which witnesses are observable today and add the missing counters to
`QueryResult` / storage metrics before the dependent lever ships.** A lever
whose witness cannot be observed does not ship — it would be indistinguishable
from a vacuous pass. On present evidence this sub-task is larger than rev 2
assumed: three of the six levers need a counter that does not exist.

Run-level reporting: emit the **activation rate per lever**. A run whose
activation rate falls below 80% for any lever fails with a message naming the
lever, so generator drift surfaces loudly instead of degrading to silence.

### 3.4 Drivers

`drive()` (`metamorphic/mod.rs:53-92`) builds **one** `Uni`, shares it read-only
across every generated case, and runs each query through `run_bag`, which opens
a fresh session per call (`metamorphic/mod.rs:115-119`). Neither Tier-1 nor
Tier-2 levers fit it unchanged. DQP therefore needs two new drivers plus a
session context.

#### 3.4.1 `drive_prepared` — Tier 2

```
drive_prepared(lever, seed):
    db      ← build_seed(seed)          # once
    db.flush()                          # once — makes the fork point explicit
    side_a  ← db.session()              # primary, held open
    side_b  ← lever.prepare(&db)        # fork created / snapshot pinned — ONCE
    for each generated case:
        assert activated(...) and bag_eq(run(side_a, q), run(side_b, q))
```

The setup that rev 2 wrongly called "no setup" is hoisted out of the case loop
entirely. Calling `db.flush()` explicitly first is not redundant with the flush
inside `create_fork_2pc`: doing it in the harness makes the fork point a stated
precondition of the run rather than a side effect of the lever, so a future
change to the fork path cannot silently move it.

#### 3.4.2 Session context

The plan-cache lever **cannot activate under the current API.** `run_bag` calls
`db.session()` for every query (`metamorphic/mod.rs:115-119`), and each session
is constructed with a "fresh metrics / plan cache / write guard"
(`session.rs:220-229`). Every query therefore runs against a cold cache and the
warm side never exists.

Both drivers thread a context holding **persistent sessions** — primary, fork,
and pinned — created once and reused for every case, with per-case helpers that
take `&Session` instead of `&Uni`. This is a prerequisite for the plan-cache
lever and is also what makes the Tier-2 sides genuinely comparable, since a
per-query session would rebuild fork-side state each time.

#### 3.4.3 `drive_stateful` — Tier 1

Tier-1 levers are stateful and irreversible, and rev 1 missed the consequence:

- Once case 1 flushes or creates an index, that state is global. Case 2 no
  longer has a side A — it silently compares B against B and passes vacuously.
- Proptest shrinking replays the failing case against a database whose state has
  since advanced, so the shrunk repro may not reproduce, or may reproduce for
  the wrong reason.

Rebuilding a ≥50k-row instance per case is too slow to run at
`METAMORPHIC_CASES=50000`. The resolution is to **invert the loop**: batch the
queries inside the state transition rather than the transition inside the query
loop.

```
drive_stateful(lever, k_queries, seed):
    db     ← build_seed(seed)              # once per batch
    cases  ← generate k queries from seed  # fixed, recorded
    bags_a ← run every case, with witness  # state A
    lever.transition(&db)                  # flush / create index / compact — once
    bags_b ← replay the same cases         # state B
    for each i: assert activated(w_a[i], w_b[i]) and bag_eq(bags_a[i], bags_b[i])
```

Properties this buys:

- **One state transition per batch**, so cost is amortized over `k` queries
  (target k ≈ 500) rather than paid per query.
- **State is fixed for the whole batch**, so no ordering contamination between
  cases and no dependence on execution order.
- **Identity is preserved** — one instance throughout, so VIDs are stable and
  `bag_eq` is valid.

**Shrinking** cannot use proptest's in-place shrinker here, because side A no
longer exists once the batch has transitioned. Instead: on failure, record
`(seed, case_index)`, then rebuild both states from `seed` in a dedicated
shrink harness and shrink the query against the rebuilt pair. This is slow, but
it runs only on failure, and it is deterministic given the seed. A
`dqp_replay(seed, case_index)` entry point becomes the repro command printed in
the failure message.

Tier-3 (cross-instance `UniConfig`) levers use a variant that builds two
instances from the same seed. **Open question that must be resolved before
Tier 3 ships: is VID assignment deterministic given an identical insert
sequence?** If yes, Tier-3 bags are directly comparable and the identity-free
projection below is unnecessary. If no, Tier 3 is restricted to identity-free
projections. This is a small, self-contained experiment and must be run before
designing around either answer — rev 1 assumed the pessimistic case without
checking.

### 3.5 Seed and generator changes

**Seed.** DQP needs its own seed at `metamorphic/dqp/seed.rs`, modelled on
`dense.rs:70`'s `Uni::temporary().build()` + `db.flush()` rather than
`seed.rs`'s in-memory 26-row fixture, and deterministic from a recorded seed
per §3.4.

#### 3.5.1 Fixture tiers and row budget

Rev 2 specified a single ≥50k-vertex / ≥200k-edge fixture *and* 50 000 cases.
That is not executable, for two compounding reasons:

- **Most generated cases have no filter.** `arb_base_where`
  (`querygen/mod.rs:565-570`) is weighted `2 => None, 1 => Some(pred)`, so **two
  thirds of cases carry no base `WHERE`** and scan the fixture end to end. At
  50k vertices × 50k cases × two sides, that is on the order of 10⁹–10¹⁰ rows
  materialized into `RowBag` hash maps per lever.
- **Batching multiplies fixture builds.** `drive_stateful` at k = 500 turns
  50 000 cases into **100 full rebuilds** of the large fixture per lever.

Case count and fixture size are therefore decoupled, and the 50 000-case volume
is retained only where a rebuild is cheap:

| Tier | Fixture | Cases | Lane |
|---|---|---|---|
| tiny | ~1k vertices / ~4k edges | 50 000 | nightly soak — carries generator volume |
| smoke | ~10k vertices / ~40k edges | 500 (`METAMORPHIC_CASES` default) | PR |
| large | ≥50k vertices / ≥200k edges | ≤ 500, batched | nightly, own job |

The large tier exists to engage index paths, morsel batching (`batch_size`
1024) and multi-fragment scans — properties of the *data*, which a few hundred
cases exercise as well as fifty thousand do. Volume is the tiny tier's job.

Two enforcement mechanisms, both mandatory:

- **A measured row budget.** Each driver accumulates rows returned across the
  run and fails if it exceeds a per-tier ceiling, with the offending case
  printed. This is a real assertion with a number in it, not a comment — the
  ceilings are set from the Phase-0 measurement below.
- **A selectivity floor on the large tier.** For fixtures above ~10k vertices,
  override the `arb_base_where` weighting so a bounded-selectivity predicate is
  always present. `LIMIT` is *not* used for this: `LIMIT` without a total order
  makes the result set nondeterministic and breaks bag equality (§3.9).

**Phase 0 for C1 is a measurement, not a build:** populate each tier, run 100
cases through one lever, record wall-clock and rows-scanned per case, and set
the ceilings from the measured distribution. If the large tier cannot complete
500 cases within the nightly budget, it shrinks — the fixture tiers are
hypotheses about cost, and the measurement is what settles them.

**Generator.** `querygen::Case` fields are private (`querygen/mod.rs:341`), so
variants are added as methods there:

- Widen `Shape` (`querygen/mod.rs:120`) beyond today's two shapes —
  `(a:Person)` and `(a:Person)-[:WORKS_AT]->(b:Company)` (:558). DQP wants
  2-hop paths, variable-length `*1..3`, `OPTIONAL MATCH`, and
  aggregation-with-grouping: the shapes where pushdown and projection bugs live.
- `Case::identity_free_projection()` — only if the §3.4 VID-determinism
  experiment comes back negative.
- Every new variant must join the round-trip proptest at `querygen/mod.rs:623`,
  or `render()` will panic on unhandled AST (`render.rs:44`).
- `EXPLAIN` is handled by `normalize` (`render.rs:71`) but **not** by `render` —
  needs a small `render_statement` extension for the plan-text witnesses.

#### 3.5.2 Bag-equality admissibility contract

`bag_eq` compares **exact** `Value`s: `CanonRow` derives `Hash`/`Eq` over
`Vec<Value>`, and the only float leniency is `0.0 == -0.0` and `NaN == NaN`
(`diff/mod.rs:31-38`). Exact equality is right for the existing TLP/NoREC
oracles, which rewrite one query against one execution config. It is **not**
automatically right for DQP, whose whole premise is that the two sides execute
differently.

The generator already emits `sum` over a numeric property
(`querygen/mod.rs:545-555`), and Tier-3 levers vary `parallelism` and
`batch_size` — precisely the knobs that change floating-point accumulation
order. `sum` over `f64` is not associative, so two sides can legitimately
produce `1.0000000000000002` and `1.0`. Under the current comparator that is a
loud failure with no bug behind it.

Every lever therefore declares an admissibility contract, and DQP starts at the
strictest setting that is sound:

- **Integer-only aggregates by default.** Restrict DQP's aggregate projections
  to integer-valued properties, where `sum`/`count`/`min`/`max` are exact and
  reassociation-invariant. This costs little and removes the entire class.
- **Float aggregates only with a tolerant comparator.** If float aggregates are
  wanted later, add a `bag_eq_approx` with a relative-epsilon comparison for
  float columns, used *only* by levers that declare they may reassociate. Do not
  loosen `bag_eq` itself — the existing oracles depend on its exactness.
- **`LIMIT` requires a total order.** `LIMIT` without an `ORDER BY` over a
  unique key returns an arbitrary subset; two execution paths may legitimately
  choose different rows. Either exclude `LIMIT` from DQP generation or emit it
  only with a total order, and in that case compare **ordered sequences**, not
  bags — bag equality cannot detect a wrong ordering.
- **Excluded constructs.** Anything nondeterministic by contract —
  `rand`-family functions, wall-clock functions, ANN search whose recall knobs
  are excluded in §3.2, and any construct returning an unordered sample.

The contract is declared per lever in code and asserted at generation time, so
an excluded construct reaching a lever is a harness failure rather than a
mysterious diff.

### 3.6 Module shape and CI

Structural contract, mirroring `norec.rs`; no trait exists in the harness today
beyond the `Lever` abstraction this item introduces. Each lever gets a
non-ignored `*_smoke` test and an `#[ignore = "soak: ..."]` `*_soak`, plus a
`#[cfg(test)] mod targeted` of hand-written `#[tokio::test]` teeth cases
following `tlp.rs:124-167`.

**The teeth must include a regression case for each historical bug in §2.1**,
each verified once against a deliberately reverted fix. An oracle that cannot
re-catch the bugs that motivated it is not yet correct.

Wiring: `pub mod dqp;` in `metamorphic/mod.rs:31-37`. **No CI changes needed** —
`pr.yml:200-205` filters `test(/metamorphic::/) and not test(soak)` and
`nightly.yml:299-304` takes the complement, so the module joins both lanes.

### 3.7 Companion oracles (cheap, same infrastructure)

- **CERT (monotonicity):** `|Q WHERE p AND q| <= |Q WHERE p|`. Catches
  cardinality-estimation and filter-inversion bugs. Reuses DQP's generator,
  fixtures and drivers wholesale — it is an additional assertion, not an
  additional harness.
- **ANN directional oracle:** for the recall knobs excluded in §3.2, assert that
  increasing `nprobes` / `refine_factor` / `ef_search` is monotonically
  non-decreasing in recall@k against the brute-force oracle already present in
  `dense_retrieval.rs` / `vector_recall.rs`.

### 3.8 Acceptance criteria

- [ ] **Phase 0 measurement done**: per-tier wall-clock and rows-scanned
      recorded; row-budget ceilings set from the measurement (§3.5.1).
- [ ] Witness-observability audit complete; missing counters added — including
      the branch-scan and snapshot-path runtime counters, without which the
      Tier-2 levers cannot ship.
- [ ] Session context lands; plan-cache lever demonstrated to reach
      `plan_cache_hit == true` on side B.
- [ ] `drive_prepared` lands; Tier-2 levers (pristine fork, pinned) ship on it.
- [ ] `drive_stateful` batch driver lands; Tier-1 levers ship on it.
- [ ] Admissibility contract declared per lever and asserted at generation time
      (§3.5.2).
- [ ] VID-determinism experiment resolved and recorded; Tier 3 designed on the
      answer.
- [ ] Per-lever activation rate reported; run fails below 80%.
- [ ] Row budget enforced; a run exceeding its tier ceiling fails with the
      offending case printed.
- [ ] Teeth reproduce the Lance `"col"` bug, #103, #135, #99 against reverted fixes.
- [ ] `dqp_replay(seed, case_index)` repro entry point works.
- [ ] Green at the tier matrix of §3.5.1 — 50 000 cases on the tiny fixture,
      ≤ 500 on the large one. **Not** 50 000 cases against a 50k-vertex fixture.

---

## 4. C2 — Fault injection: the three uncovered areas  *(P1)*

### 4.1 What already ships (baseline correction)

Rev 1 described the fault-injection baseline as "two env vars" and proposed
adding failpoints from scratch. **That was wrong.** The repo already has:

- **fail-rs as a real dependency** with a `failpoints` cargo feature
  (`uni-store/Cargo.toml:23,77`; `uni/Cargo.toml:22,114,265`).
- **11 `fail_point!` sites**: `commit::after-flush-lock` (`writer.rs:1223`),
  `commit::after-validate` (:1410), `commit::mid-wal` (:1458),
  `commit::after-wal-flush` (:1512), `commit::after-merge` (:1590),
  `nontx::after-capture` (:3683), `flush::rotate-fail` (:4700),
  `flush::after-rotate-before-lance` (:4765), `flush::stream-async-stall`
  (:4777), `flush::after-complete-before-cache-clear` (:5828),
  `nested_fork_before_branch` (`uni/src/api/fork.rs:567`).
- **Crash/reopen suites**: `ssi_resilience.rs` (4 failpoint-gated crash tests +
  WAL-tail corruption), `flush_resilience.rs` (whole file
  `#![cfg(feature="failpoints")]`, 7+ tests), `wal_durability_test.rs` (12
  replay/corruption/checksum tests), four `fork_recovery/*` suites,
  `dense_resilience.rs` / `sparse_resilience.rs` / `multivector_resilience.rs`.
- Additional non-fail-rs injection: `UNI_FORK_INJECT_FAIL_AFTER` /
  `..._DELETE_AFTER` (`lance_branch.rs:184,209`), `#[cfg(test)] FAIL_NEXT_FSYNC`
  (`wal.rs:90,408`), an in-test `FailingStore` (`wal.rs:1613`).

Proposing a general fault-injection framework would duplicate all of this. The
item is re-scoped to the three areas that are genuinely uncovered.

### 4.2 Gap 1 — fork 2PC windows (orchestration, not just the registry)

`crates/uni-store/src/fork/registry.rs` has **no `fail_point!` sites**, and
existing fork-recovery tests reach the recovery path by hand-constructing a
`Pending` or `Tombstoned` registry and calling `recover_forks`, or by faulting
Lance `create_branch`/`delete_branch` from outside. Neither crashes *inside* a
2PC window.

**Correction (rev 3): instrumenting `registry.rs` alone would miss most of those
windows.** The registry supplies the `begin_*`/`finish_*` transitions, but the
multi-step work that can tear happens in the callers, between those calls:

- **Create** (`uni/src/api/fork.rs`): validate schema names → flush and capture
  the fork point (:300-324) → `registry.begin_create` (:399) → bootstrap the
  per-fork id allocator (:409, with its own `rollback_create` on failure) →
  `build_datasets_for_fork` / one Lance branch per dataset (:427) →
  `finish_create`. Today the only failpoint anywhere in this span is
  `nested_fork_before_branch` (:567).
- **Drop** (`uni/src/api/fork_admin.rs`): drain holders (:177) →
  `registry.begin_drop` (:178) → evict the cached `Weak<UniInner>` (:185) →
  force-delete each branch in a loop (:203-209) → `finish_drop`, which is
  deliberately skipped if any branch delete failed so the recovery tombstone
  survives (:186-190).

Failpoints go in **both orchestration files as well as `registry.rs`**, one per
inter-step window — in particular between the allocator bootstrap and branch
creation, and *inside* the branch-delete loop so a partially-deleted fork is
reachable. The assertion in each case is that recovery lands on Active or
Tombstoned and never a torn state, and specifically that a crash mid-loop leaves
the tombstone intact.

### 4.3 Gap 2 — compaction (two mechanisms, two matrices)

No `fail_point!` exists anywhere in the compaction path, and
`background_compaction_test.rs` covers only config, triggers and status.

**Correction (rev 3): "compaction" is two different mechanisms and rev 2
conflated them.** The `max_l1_runs` trigger drives uni's own semantic
compaction, and then Lance optimization runs underneath. The Lance side is
`optimize_table` (`uni-store/src/backend/lance.rs:600-624`), which calls
`lance::dataset::optimize::compact_files` followed by a cleanup pass — and
`compact_files` is a **single opaque upstream call**. File merge and manifest
commit both happen inside it, so the `compaction::post-merge-pre-manifest`
failpoint rev 2 proposed **cannot be implemented at the uni level at all**
without upstream Lance instrumentation.

Two separate crash matrices instead:

- **Semantic compaction** (uni-owned): failpoints between the steps uni
  controls, asserting no tombstone resurrection and no row loss across a
  `max_l1_runs` merge. This is where the fault-injection work actually lands.
- **Lance optimization** (upstream-owned): treat `optimize_table` as atomic and
  crash *around* it — before, and after `compact_files` but before the cleanup
  pass, which is a boundary uni does control. Finer granularity needs an
  upstream failpoint or feature request; that is out of scope here and should be
  recorded as a known limitation rather than silently attempted.

### 4.4 Gap 3 — the graceful-drop confound

Every "crash" in the suite is a panic inside a spawned task followed by
`drop(db)`, which still runs `Uni::Drop` and its shutdown flush.
`ssi_resilience.rs` acknowledges this directly in a comment: "Under a real,
`Drop`-less crash the WAL was already durable, so only the graceful-close path
lost data." So the existing tests validate *graceful-close* atomicity, not
crash atomicity — a strictly weaker property, and the two have already diverged
in practice (#167's shutdown-triggered final flush *recreating* the directory
was exactly a `Drop`-path behaviour).

Add a child-process harness: spawn the test body in a subprocess, arm a
failpoint, `std::process::abort()` at it, then reopen the directory from the
parent and assert durability/atomicity/no-resurrection. This removes the
confound and is a prerequisite for trusting any of the existing crash results as
crash results.

### 4.5 Determinism: `madsim` (spike only)

loom and shuttle cover `uni-store`'s sync primitives. Nothing makes **tokio**
scheduling reproducible, which is the documented source of flakiness across the
repo (the `issue-55-get-edges` `max-threads = 1` group in `.config/nextest.toml`,
fork TTL sweeper races, the flagship-notebook contention note).

**Risk, stated plainly:** madsim requires all time/IO to route through its
shims, and Lance and DataFusion sit underneath. Full adoption is likely
infeasible. Time-box a spike against the uni-owned WAL + L0 + flush-coordinator
path only, and decide from the spike. Do not commit to workspace-wide adoption.

### 4.6 Acceptance criteria

- [ ] Failpoints between every 2PC phase pair in `registry.rs`; recovery matrix
      green from each.
- [ ] Compaction failpoints + mid-compaction crash tests.
- [ ] Child-process `abort()` harness; the four existing `ssi_resilience.rs`
      crash tests re-run under it and pass (or their failures are triaged).
- [ ] madsim spike report: adopt / partial / reject, with evidence.

---

## 5. C3 — Elle: serializability checking  *(P1)*

### 5.1 What it buys

Elle records a history of transactions over a list-append or read-write-register
datatype and detects consistency violations by **cycle detection in the
dependency graph**, classifying them G0 / G1a / G1b / G1c / G2-item / G2, and
returns a *minimal counterexample cycle*. That is a categorically stronger claim
than an enumerated-anomaly suite can make.

### 5.2 Why this does not require Jepsen

uni-db is embedded — no cluster, no nemesis, no Clojure harness:

1. A Rust driver spawns N concurrent tasks doing `append(key, value)` / `read(key)`
   against graph properties, recording an EDN/JSON history.
2. Pipe through `elle-cli` with `--consistency-model serializable`.
3. Optionally re-run under §4's fault injection as a poor-man's nemesis.

### 5.3 Friction

`elle-cli` is a JVM artifact, so this is a **nightly-only lane with a
`setup-java` step**, not a PR gate. Escalation if the JVM dependency is
blocked: implement G0/G1c/G2 cycle detection natively in Rust over the same
history format, accepting that it reimplements a subtle, well-tested checker.

### 5.4 Negative control — corrected

Rev 1 proposed "with `ssi_enabled = false`, Elle must report G2." **That is
unsound**: a lucky schedule under LWW can still be serializable, so the control
would flake. The negative control must be *deterministic*:

- **Preferred:** a synthetic anomalous history — a hand-constructed write-skew
  or G2 cycle, fed to the checker directly, asserting it is rejected. This tests
  the checker wiring with no dependence on scheduling.
- **Additionally:** a deliberately faulty adapter that drops a specific
  anti-dependency edge, asserting the pipeline reports the injected anomaly.

An LWW run may be kept as a *non-gating* observation, reported but never
asserted.

### 5.5 Acceptance criteria

- [ ] Rust history-generating driver with configurable concurrency and key skew.
- [ ] Nightly job runs ≥1000 transactions, asserts no cycle.
- [ ] Deterministic negative control (synthetic anomalous history) rejected.
- [ ] Faulty-adapter control reports the injected anomaly.

---

## 6. C4 — Cheap correctness wins  *(P1)*

- **`cargo-llvm-cov`, once, as a map — not a gate.** Percentage gates breed test
  theater. Deliverable: which modules have zero coverage, across 35 members.
  Publish to `docs/perf/`; re-run quarterly.
- **miri on pure-logic crates only:** `uni-btic`, `uni-crdt`, `uni-common`,
  `uni-sparse-vector`. The BTIC codec has unsafe surface and a committed
  `btic_decode` crash artifact in `fuzz/artifacts/`. miri cannot run the
  Lance/DataFusion crates — do not attempt workspace-wide.
- **`cargo-deny`** in `pr.yml`: advisories, licenses, bans, sources. No
  supply-chain gate exists today.
- **Fuzz on PR.** Currently nightly-only at 5 min/target, so a parser or codec
  regression lands and surfaces a day later. Add a 30 s/target corpus-seeded run
  (~2 min total); keep the long nightly run.
- **Hypothesis for the Python bindings.** All 1,073 pytest functions are
  example-based. The sync/async API-symmetry contract in the contributor guide
  is exactly what a `RuleBasedStateMachine` checks well.

---

## 7. B1 — Perf regression gate via instruction counts  *(P0)*

### 7.1 Why not wall-clock

GitHub-hosted runners vary ±20–30% run to run. A wall-clock gate at any useful
threshold either fires constantly (and gets disabled) or is set so loose it
catches nothing. Instruction counts under Callgrind are deterministic to ~0.1%
on noisy shared hardware.

*(Rev 1 cited the absence of `--release` in the nightly bench job as a defect.
That was wrong — `cargo bench` uses the `bench` profile, which inherits release
optimization. Withdrawn.)*

### 7.2 Metric qualification pilot — required before any gating

Instruction counts miss I/O, cache effects, and parallelism. Rev 1 nonetheless
proposed gating WAL commit, L0→L1 flush and HNSW search, which are precisely the
targets where that blind spot bites — a flush that does the same work with
worse locality, or a commit dominated by fsync, can regress badly with a flat
instruction count, and conversely can show instruction noise from IO-path
branching that has no wall-clock meaning.

**Phase 0 of this item is a repeatability pilot**, not a gate:

1. Instrument all 7 candidate targets under iai-callgrind.
2. Run each ≥20 times across ≥3 CI runner instantiations.
3. Compute per-target coefficient of variation and, separately, the correlation
   between instruction delta and wall-clock delta on a set of deliberately
   injected regressions.
4. **Gate only targets that are both stable (CV < 1%) and CPU-dominant
   (instruction delta tracks wall-clock delta).** Publish the qualification
   table in `docs/perf/iai-qualification.md`.

Candidate targets, with a prior on how they will qualify:

| Target | Expectation |
|---|---|
| Cypher parse + plan (cold cache) | CPU-dominant — expect qualifies |
| Single-vertex lookup by id | CPU-dominant — expect qualifies |
| `expand_batch` 1-hop, warm adjacency | CPU-dominant — expect qualifies |
| Property read across L0/L1 boundary | mixed — pilot decides |
| Transaction commit (WAL on) | IO-dominant — expect **does not** qualify |
| L0 → L1 flush | IO-dominant — expect **does not** qualify |
| HNSW top-10 search | cache-dominant — pilot decides |

Non-qualifying targets stay in the nightly wall-clock Criterion suite, where
their variance is tolerable because nothing gates on it. **A target that fails
the pilot is not gated, however much we would like coverage there** — a gate on
a noisy metric is worse than no gate, because it trains everyone to ignore it.

### 7.3 Tooling

`iai-callgrind` — self-hosted, no vendor, free. **Alternative: CodSpeed**
(`cargo-codspeed`) — same principle as a service with PR comments and a trend
UI, free for open source. Recommend iai-callgrind to keep third parties out of
the release path; CodSpeed is a reasonable substitution if the trend UI is
wanted.

### 7.4 CI shape and cost — stated honestly

Valgrind carries a 10–50× runtime multiplier. The Swatinem cache uses a single
`shared-key: "ci"` whose only saver is `ci.yml:56` (`tck-full`), which never
builds `--benches` — so bench artifacts are not cached and a new job pays a cold
bench-binary compile. This is not free.

- **One** new bench target, `crates/uni/benches/hot_paths_iai.rs`, containing
  all qualified benches. `docs/test_layout.md:15`'s 3-binary cap covers only
  `tests/` files, not benches — which is how 18 bench binaries came to coexist
  with a 3-test-binary cap. This proposal does not worsen that.
- Dedicated `perf-gate` job in `pr.yml`, `save-if: false`.
- Baseline committed as `docs/perf/iai-baseline.json`, regenerated on main and
  reviewed in-PR when it moves — regressions visible in the diff, not in a
  dashboard nobody opens.
- **Gate: fail at >5% instruction-count regression** on a qualified target;
  warn at 2%.
- Fallback if cold-compile cost proves unacceptable: run the gate post-merge on
  `ci.yml`. Weaker than pre-merge, still far better than today.

### 7.5 Cleanup

`crates/uni/benches/pushdown_performance.rs` has no `[[bench]]` stanza, but
`autobenches` is not disabled in `crates/uni/Cargo.toml`, so Cargo
auto-discovers it with the **default libtest harness** while the file calls
`criterion_main!` — `cargo bench --bench pushdown_performance` fails, as
recorded in `documentation_remediation_2026-08-06.md:271`. Its three functions
have empty TODO bodies and reference no uni-db APIs. **Delete it.**

### 7.6 Acceptance criteria

- [ ] Qualification pilot run; `docs/perf/iai-qualification.md` published with
      per-target CV and instruction-vs-wall-clock correlation.
- [ ] One iai target containing only qualified benches.
- [ ] `docs/perf/iai-baseline.json` committed; regeneration script in `scripts/`.
- [ ] `perf-gate` fails on >5% regression, verified by a deliberate-regression PR.
- [ ] Non-qualifying targets documented as nightly-only, with the reason.
- [ ] `pushdown_performance.rs` resolved.

---

## 8. B2 — LDBC SNB  *(P1)*

**SNB Interactive** ships a validated dataset generator (SF0.1/1/10/100), 14
complex reads, 7 short reads, 8 update operations, and an official driver that
**validates result correctness against reference answers** — making it a
correctness benchmark as much as a performance one. **SNB BI** adds 20
analytical queries hitting DataFusion aggregation paths Interactive does not
reach. **Graphalytics** (BFS, PageRank, WCC, CDLP, LCC, SSSP) maps 1:1 onto
`uni-algo` / `graph_compute`.

**Path, avoiding the Java/Spark toolchain:** generate SF0.1 and SF1 once
offline; commit SF0.1 CSVs, store SF1 in S3 with a checked-in manifest and
checksum; write a Rust loader in `crates/uni/benches/ldbc/` and implement the 14
complex reads as Cypher; validate against LDBC reference answers at SF0.1;
report SF1 latency percentiles to `docs/perf/ldbc_snb_<date>.md`. The queries are
the deliverable; the official driver is optional and only needed for formally
audited results.

- [ ] SF0.1 loads; all 14 complex reads match LDBC reference answers.
- [ ] SF1 latency percentiles published.
- [ ] Graphalytics BFS/PR/WCC/SSSP results for `uni-algo`.
- [ ] Nightly SF0.1 lane as a correctness regression guard.

---

## 9. B3 — Vector & retrieval quality benchmarks  *(P1)*

**ann-benchmarks protocol.** `dense_retrieval.rs` measures latency and recall
separately; the industry currency is the **recall@10 vs QPS Pareto curve** at
swept `ef_search` / `nprobes` on SIFT-1M, GloVe-100, GIST-960 — a direct
extension of the existing query-time `ef_search` work. Real curves need ≥1M
vectors; see §3.3 for why the current n=1000 recall test is vacuous.

**BEIR.** uni-db ships dense + SPLADE + ColBERT + RRF fusion with LoCoMo as the
only quality evidence. BEIR gives nDCG@10 — the number that says whether RRF
fusion actually beats dense alone. Start with SciFact / NFCorpus / FiQA; add
NQ / HotpotQA for scale. This is deliberately the benchmark that can tell us a
feature is not earning its complexity; publish regardless of outcome.

- [ ] recall@10-vs-QPS curves for HNSW / IVF_PQ / RaBitQ on SIFT-1M.
- [ ] nDCG@10 on ≥3 BEIR subsets for dense, sparse, multivec, RRF — with the
      fusion-vs-best-single-head delta stated explicitly.
- [ ] Both in `docs/perf/`, regenerable via script.

---

## 10. B4 — Contention curves  *(P2)*

`ssi_contention.rs` has the right idea; formalize as **throughput and abort-rate
vs contention**, sweeping Zipf θ and thread count. A single number hides the
shape change from "aborts rise gracefully" to "aborts collapse throughput".
Table into `docs/perf/`, nightly.

---

## 11. Sequencing

Ordering and dependencies, not a schedule. Each stage is gated on the one above
it, so the sequence is a dependency chain rather than a calendar.

```
Stage 1  C1 Phase 0: fixture-tier measurement + witness audit    ── P0
         B1 qualification pilot (no gate yet)                     ── P0
Stage 2  C1 session context + missing runtime counters;
            drive_prepared + Tier-2 levers
         B1 gate on qualified targets only
Stage 3  C1 drive_stateful + Tier-1 levers; VID-determinism expt
Stage 4  C1 teeth tests + EXPLAIN rendering; CERT + ANN oracles
         C4 coverage map, cargo-deny, fuzz-on-PR, miri
Stage 5  C2 fork 2PC failpoints (orchestration + registry)
Stage 6  C2 semantic-compaction failpoints; child-process abort harness
Stage 7  B2 LDBC SNB SF0.1 → SF1
Stage 8  C3 Elle lane; B3 ann-benchmarks + BEIR
Stage 9  B4 contention curves; consolidate docs/perf/
```

C1 and B1 are independent and run in parallel from stage 1. **Both P0s open with
a measurement rather than a build** — C1's fixture-tier cost measurement and
B1's metric-qualification pilot — because in both cases rev 2 committed to
numbers it had not measured, and both measurements can invalidate the design
that follows them. C1 is staged Tier-2-first so value lands before the
`drive_stateful` work, but Tier 2 is gated on the runtime counters from stage 2:
without them its activation witnesses are unobservable and the levers cannot
ship. C3 is sequenced late because it *demonstrates* a property we have reason
to believe holds, whereas C1 and C2 hunt bugs we have reason to believe exist.

---

## 12. Risks

| Risk | Mitigation |
|---|---|
| **DQP is vacuous** | Per-lever activation witnesses + ≥80% activation rate enforced per lever (§3.3). A lever whose witness is unobservable does not ship. |
| **DQP does not finish** | Fixture tiers decouple case count from fixture size; enforced row budget; Phase-0 measurement sets the ceilings (§3.5.1) |
| **Stateful levers contaminate each other** | Batch-per-state driver: one transition per batch of ~500 queries, state fixed within a batch (§3.4.3) |
| Tier-2 setup mistaken for free | `drive_prepared` hoists the fork/snapshot creation — both of which flush — out of the case loop (§3.4.1) |
| False diffs from float reassociation | Integer-only aggregates by default; tolerant comparator only for levers that declare reassociation (§3.5.2) |
| Shrinking broken for stateful levers | Seed-based `dqp_replay` rebuild-and-shrink harness; failure message prints the repro command |
| Tier-3 blocked by VID nondeterminism | A small experiment resolves it before design; identity-free projection is the fallback, not the assumption |
| **Perf gate on a metric that does not qualify** | Mandatory pilot; only stable, CPU-dominant targets gated; the rest stay nightly and ungated (§7.2) |
| iai job cost (cold bench compile + Valgrind) | Single bench target; `save-if: false`; documented fallback to post-merge gating |
| madsim incompatible with Lance/DataFusion | Time-boxed spike on the uni-owned path only; explicit adopt/reject |
| Elle's JVM dependency rejected | Nightly-only lane; native Rust cycle detection as documented escalation |
| Elle negative control flakes | Deterministic synthetic anomalous history, not an LWW run (§5.4) |
| LDBC scope creep into the Java toolchain | Offline one-time datagen; Rust loader + Cypher only; official driver out of scope |
| BEIR shows fusion does not beat dense | That is a *result*, not a failure. Publish it and let it drive the roadmap. |

---

## 13. What this does not propose

- **A coverage percentage gate.** Coverage is used as a map (§6), not a target.
- **Workspace-wide miri.** Cannot run the Lance/DataFusion crates.
- **Full Jepsen.** uni-db is embedded; the cluster harness is irrelevant.
- **A wall-clock perf gate.** Runner variance makes it unenforceable (§7.1).
- **A general fault-injection framework.** One already ships (§4.1); this
  proposal adds only the three uncovered areas.
- **Retiring any existing harness.** TCK, TLP/NoREC, loom/shuttle, the Locy
  oracle, the failpoint suites and the fuzz targets all stay as they are.
  Everything here is additive.
