# Test Harness & Benchmarks — Phased Implementation Plan

**Date:** 2026-08-12
**Status:** Plan
**Implements:** `docs/proposals/test_harness_and_benchmarks_2026-08-11.md` (rev 3)

This is the execution plan for the eight work items in the proposal. The
proposal argues *what* to build and *why*; this document specifies *in what
order*, *touching which files*, and *what has to be true before the next phase
starts*.

Two rules govern the whole plan:

1. **Every phase has a falsifiable exit criterion with a verification command.**
   A phase is not done because the code compiles; it is done because a named
   command passes and a named artifact exists.
2. **A phase that invalidates a later phase's premise stops the chain.** Phases 0A
   and 0B are measurements whose results can force a redesign of everything
   downstream. That is their purpose, not a failure mode.

---

## 0. Corrections to the proposal found while planning

Three details in the proposal are wrong or under-specified against live source.
The plan below uses the corrected values.

| Proposal says | Source says | Consequence |
|---|---|---|
| smoke tier = "500 (`METAMORPHIC_CASES` default)" (§3.5.1) | `SMOKE_CASES = 64`, `SOAK_CASES = 256` (`metamorphic/mod.rs:89-91`) | The smoke tier is **64**, not 500. Nightly overrides the env var; PR does not. Tier table corrected in phase 3. |
| `drive()` shape reused for new drivers (§3.4) | `drive()` builds a **current-thread** runtime (`metamorphic/mod.rs:70-73`) | Fork creation, the flush coordinator and the fork sweeper involve background tasks. A current-thread runtime only advances them inside `block_on`. The DQP drivers must be built on `new_multi_thread`, and phase 0A must confirm a fork can even be created under the current-thread flavor before we assume the difference is cosmetic. |
| `LIMIT` "needs a total order" — treated as new work (§3.5.2) | `Case::ordered_query()` and `Case::limited_query(n)` already exist and are round-trip-tested (`querygen/mod.rs:623` variant list) | The ordered-`LIMIT` machinery is already there. Phase 3 wires it, rather than building it. |

Also worth carrying forward: `bag_is_subset` already exists (`diff/mod.rs:166`)
and is exactly the comparator CERT needs (§3.7), so the CERT oracle in phase 6
adds an assertion and no comparator.

---

## 1. Phase map

```
        ┌─────────────────────────────────────────────┐
Track A │ 0A  DQP feasibility measurement             │  gate: can DQP run at all?
  (C1)  │  1  Runtime counters / witness observability │  gate: can DQP be non-vacuous?
        │  2  Lever trait + session ctx + drive_prepared + fork lever
        │  3  Pinned lever + admissibility contract + row budget
        │  4  drive_stateful + Tier-1 levers
        │  5  Generator widening + EXPLAIN + teeth
        │  6  CERT + ANN directional + Tier-3 decision
        └─────────────────────────────────────────────┘
        ┌─────────────────────────────────────────────┐
Track B │ 0B  iai qualification pilot                  │  gate: does the metric qualify?
  (B1)  │  7  Perf gate rollout + bench cleanup
        └─────────────────────────────────────────────┘
Track C   8  C4 cheap wins (independent, any time)
Track D   9  C2 fork 2PC failpoints
         10  C2 compaction matrices + abort harness
Track E  11  B2 LDBC SNB
         12  C3 Elle + B3 vector/BEIR
         13  B4 contention curves + docs/perf consolidation
```

Tracks A and B are independent and start together. Track C is independent of
everything and can be slotted wherever there is slack. Track D depends on
nothing in A or B but is sequenced after them by priority. Track E is last.

---

## Phase 0A — DQP feasibility measurement

**Goal:** produce the numbers the whole of C1 is designed around. No oracle is
written in this phase.

The proposal asserts fixture tiers and a row budget without having measured
either. This phase measures, and the tier table in phase 3 is set from the
result — not from the proposal's guesses.

### Tasks

1. **New file `crates/uni/tests/common/metamorphic/dqp/seed.rs`.** Parameterized
   fixture builder `build_dqp_seed(tier: Tier) -> Result<(Uni, TempDir)>`,
   modelled on `dense.rs:70`'s `Uni::temporary().build()` + explicit
   `db.flush()`, **not** on `seed.rs`'s 26-row in-memory fixture. Three tier
   sizes as hypotheses to be tested: tiny ~1k vertices / ~4k edges, smoke ~10k /
   ~40k, large ~50k / ~200k.
2. **Throwaway measurement harness** (a `#[ignore]`d test, deleted or demoted at
   the end of the phase): for each tier, build the fixture, run 100
   `arb_case()`-generated queries, record per case — wall-clock, rows returned,
   and fixture build time.
3. **Runtime-flavor probe.** Under a `new_current_thread` runtime, attempt
   `db.flush()`, `session.fork("x").build()`, and `pin_to_version`. Record
   whether each completes or hangs. This settles the correction in §0 above.
4. **Publish `docs/perf/dqp-feasibility.md`** with the measured distribution
   (p50/p95/max per tier), the extrapolated cost of `case_count × 2 sides`, and
   the runtime-flavor result.

### Results — **phase complete, 2026-08-12**

Published: **`docs/perf/dqp-feasibility-2026-08-12.md`**. Headlines, each of which
changes work below:

- **The proposed tier table is refuted on all three rows.** Measured p50 per case:
  tiny 31.66 ms, smoke 248.45 ms, large 1.247 s. Revised: tiny **500** cases (PR)
  / **20 000** (soak — not 50 000, which is 53 min at p50 and 4.2 h at p95 against
  a 60-min job); smoke **2 000**; large **300**, in its **own** nightly job.
- **The large fixture takes 621.73 s to build** — 789× smoke's for 5× the data.
  This forbids the large tier to `drive_stateful` entirely (see phase 4).
- **`rows p50` equals the fixture's vertex count exactly** at every tier, and
  `rows p95` equals its edge count exactly — the predicted full-scan consequence
  of `arb_base_where`'s weighting, now measured rather than argued.
- **Five of seven `QueryMetrics` counters always read zero**, and `plan_cache_hit`
  is write-path-only. Phase 1's scope is now a concrete five-item list.
- **The runtime-flavor concern is refuted** — a current-thread runtime performs
  flush, fork and snapshot+pin without trouble, so `drive()`'s existing shape is
  reusable and no lever is blocked by the flavor.

### Exit criteria

- [x] `docs/perf/dqp-feasibility-2026-08-12.md` exists with measured per-tier numbers.
- [x] Row-budget ceilings chosen **from the measurement**, as concrete integers
      (3 M / 120 M / 120 M / 90 M rows by lane), enforced over rows *returned*
      because `rows_scanned` always reads zero.
- [x] Case counts per tier chosen from the measurement; the large tier was
      reduced from ≤ 500 to 300 **and moved to its own job**, and the tiny tier's
      soak count was cut from 50 000 to 20 000. Both reductions recorded.
- [x] Runtime flavor decided and justified — either works; concern refuted.

### Verification

```
cargo nextest run --profile soak -p uni-db --test integration \
  --run-ignored ignored-only -E 'test(/metamorphic::dqp::feasibility/)' --no-capture
```

The `soak` profile is required: the large tier's build alone exceeds the default
profile's 180 s per-test kill.

### Stops the chain if

The large tier cannot be built and queried within nightly budget even at 100
cases. In that case the large tier is dropped and Tier-2 levers ship on the
smoke fixture only — a materially weaker C1, and a decision to surface rather
than absorb.

---

## Phase 1 — Runtime counters (witness observability)

**Goal:** make every activation witness observable. **This is a hard gate on
phases 2–4**: a lever whose witness cannot be observed is indistinguishable from
a vacuous pass and must not ship.

Three of the six witnesses in the proposal's §3.3 table currently need a counter
that does not exist.

### Tasks

> **Scope settled by Phase 0A.** The audit below has been run; its result is
> §4.2 of `docs/perf/dqp-feasibility-2026-08-12.md`. Work required:
> `l0_reads`, `storage_reads`, `rows_scanned`, a **branch-scan** execution
> counter and a **snapshot-path** execution counter (the last two do not exist in
> any form). Already usable, no work needed: `rows_returned`,
> `DatabaseMetrics::l1_run_count`, `SessionMetrics::plan_cache_hits`.
> `bytes_read` and `cache_hits` back no planned witness and stay unpopulated.

1. ~~**Audit.**~~ **Done in Phase 0A.** Corrections to what this step assumed:
   - `plan_cache_hit` was marked observable. It is **not**, on the read path —
     one assignment site, `execute_internal_with_tx_l0` (`impl_query.rs:808`),
     which is the write path. Use the `SessionMetrics::plan_cache_hits` delta.
   - L1 run count was marked non-existent. It **exists**, on
     `DatabaseMetrics::l1_run_count` (`api/mod.rs:321`), and needs no work.
   - `rows_scanned`, `bytes_read`, `l0_reads`, `storage_reads`, `cache_hits`
     exist as **fields that always read zero** — a strictly worse state than
     absent, since a witness written against them compiles and never fires.
   - plan-text pushdown node — reachable via `EXPLAIN`, but `render` does not
     emit `EXPLAIN` (only `normalize` handles it, `render.rs:71`). Needs the
     phase-5 renderer extension, **or** an interim non-Cypher accessor.
2. **Populate the placeholder counters and add the two that are missing**, as a
   per-query counter set surfaced on
   `QueryResult` (preferred — same lifetime as the bag it accompanies) rather
   than a process-global metric, which cannot be attributed to a single case
   under any concurrency.
   - L0-served rows: increment where L0 buffers serve a scan.
   - Branch-scan executions: increment on `BranchedBackend`'s branch path, at
     the point where it commits to the branch rather than falling back —
     the counter must observe *what executed*, never *what was configured*.
   - Snapshot-path executions: same shape, on the pinned-read path.
   - L1 run/fragment count: a storage-level accessor, read between the two
     sides of the compaction lever rather than per query.
3. **Counter unit tests** proving each counter is zero on the path that should
   not increment it and non-zero on the path that should. A counter that is
   always non-zero is as useless as one that is always zero.

### Results — **phase complete, 2026-08-12**

Counters ride three existing carriers, each chosen by the layer it must reach:
`QueryContext` into storage, `ScanRequest` into the backend (which cannot see
`QueryContext`), and `GraphExecutionContext` into the DataFusion operators. All
three share one `Arc<QueryCounters>` owned by the `Executor`, fresh per clone for
the same reason `warnings` is.

Measured before/after on the same audit that discovered the problem:

| field | before | after (flushed) | after (64 unflushed rows) |
|---|---|---|---|
| `rows_scanned` | 0 | 1000 | 1064 |
| `storage_reads` | 0 | 1000 | 1000 |
| `l0_reads` | 0 | **0** | **64** |
| `bytes_read`, `cache_hits` | 0 | 0 | 0 (asserted, by decision) |

`l0_reads` moving 0 → exactly 64 is the L0-vs-L1 witness working.

Findings from building it:

- **A pristine fork *does* execute branch scans.** `create_fork_2pc` materializes
  one Lance branch per dataset at fork time and `ForkScope::branch_for`
  (`fork/scope.rs:292`) resolves from that map, so fork scope alone takes the
  branch path — no fork-local write needed. This is what the Tier-2
  "primary vs pristine fork" lever depends on, and it holds.
- **The genuine fallback case is a dataset created *after* the fork**, not one
  the fork hasn't written. That is the negative test that separates an execution
  witness from a config read, and it passes.
- **`plan_time = 0` on a cache hit is correct**, not a defect — neither phase
  ran. Only the miss path was discarding real measurements; that is fixed.
- **`snapshot_reads` counts manifest pins only.** `version_high_water_mark()` is
  also `Some` for an ordinary transaction's version pin, so counting off it would
  have fired on every transactional read and made the pinned-vs-live witness
  meaningless.

### Exit criteria

- [x] Every witness in §3.3 is observable, **except** the index-present lever's
      plan-text witness, explicitly deferred to Phase 5's `EXPLAIN` rendering.
- [x] Each new counter has a positive and a negative test — 10 in
      `metamorphic::dqp::counters`, plus the flipped audit.
- [x] No counter is a config read; `branch_scans_zero_for_a_table_the_fork_has_no_branch_for`
      is the assertion that proves it.
- [x] The Phase 0A audit is now a **non-ignored regression test** that asserts
      rather than reports, including that `bytes_read`/`cache_hits` remain zero
      by decision.

### Verification

```
cargo nextest run -E 'test(/dqp_counter/)'
```

---

## Phase 2 — Harness skeleton and the first lever

**Goal:** land the abstraction plus the highest-value lever (primary vs pristine
fork), so the oracle is finding bugs before the rest of the levers exist.

### Tasks

1. **`crates/uni/tests/common/metamorphic/dqp/mod.rs`** — new module, wired as
   `pub mod dqp;` into `metamorphic/mod.rs:31-37`. **No CI changes needed**:
   `pr.yml:205` filters `test(/metamorphic::/) and not test(soak)` and
   `nightly.yml:304` takes the complement, so the module joins both lanes by
   name alone.
2. **`Lever` trait + `Witness` type** per the proposal's §3.3 signature, with
   `activated(&self, a, b) -> bool` mandatory — no default implementation, so a
   new lever cannot forget it.
3. **Session context.** A `DqpContext` holding persistent `Session` handles
   (primary, fork, pinned), created once. Per-case helpers take `&Session`.
   This is what makes the plan-cache lever possible at all: `run_bag` calls
   `db.session()` per query (`metamorphic/mod.rs:115-119`) and every session
   gets a fresh plan cache (`session.rs:220-229`), so a per-query session can
   never observe a warm cache. Add `run_bag_in(session: &Session, q: &Query)`
   alongside the existing `run_bag`; do **not** change `run_bag`, which TLP and
   NoREC depend on.
4. **`drive_prepared`** per §3.4.1, on the runtime flavor decided in phase 0A.
   Explicit `db.flush()` before `lever.prepare()` even though `create_fork_2pc`
   flushes internally (`api/fork.rs:300-324`) — stating the fork point as a
   harness precondition means a future change to the fork path cannot silently
   move it.
5. **Lever 1: primary vs pristine fork.** Zero fork-local writes. Witness = the
   branch-scan counter from phase 1.
6. **Activation-rate reporting.** Accumulate per-lever activation across the
   run; fail below 80% with a message naming the lever.

### Results — **phase complete, 2026-08-12**

The oracle runs and is non-vacuous. **Activation is 100%** — every one of 500
generated cases had side B execute a branch scan and side A not — and no bag
divergence was found, which is the expected outcome for a fork contract the
codebase has been repairing since #97.

Settled by measurement rather than assumption:

- **Tier 2 really is identity-preserving.** The design called it that on the
  reasoning that a fork inherits VIDs through the branch's `base_paths` chain,
  but nothing had checked. `dqp::identity` now pins it three ways: matching
  `id(n)` per row, a bare `RETURN p` comparing equal, and the edge shape
  comparing equal. Had this failed, every generated case projecting a bare
  variable would have diffed for no reason and the lever would have been
  unusable as designed — `querygen` emits `Expr::Variable` routinely.
- **Measured PR cost is 48 s for 500 cases**, against the ~32 s projected from
  the Phase-0A single-side figure. The fork side is slower than primary, which
  is unsurprising given `use_scalar_index(false)` on the branch scan.
- **The soak was run at full volume rather than extrapolated**: 20 000 cases,
  **100% activation**, zero divergences, 80 010 286 rows against a 117 540 000
  ceiling, in **31.9 minutes**. That is 80% of the `soak` profile's 40-minute
  per-test kill, which is why `.config/nextest.toml` gained a `test(fork_soak)`
  override raising it to 54 minutes — the job's own `timeout-minutes: 60` stays
  the real backstop.
- **The run row budget must not be enforced inside the case loop.** The first
  version was, and it double-counted: proptest's shrink replays kept adding
  rows, so a long shrink sequence could trip the budget and replace a genuine
  divergence report with a budget message — the harness hiding the bug it had
  just found. Split into a per-case ceiling (in-loop, immune to replay
  inflation) and a run total (post-run, checked only on an otherwise-clean run).
- **`disable_fork_index_builder` is load-bearing, not hygiene.** The background
  builder can register a fork-local index partway through a run, so the first N
  cases and the remainder would exercise different machinery with nothing in the
  output saying so — which breaks the one property `drive_prepared` exists to
  provide.

### Exit criteria

- [x] `dqp::fork_lever::fork_smoke` passes — on the **Tiny** fixture at 500
      cases, per the Phase-0A revision, not the Smoke fixture this line
      originally named. `fork_soak` is `#[ignore]`d and **verified at the full
      20 000 cases**, not extrapolated.
- [x] Activation rate printed every run; measured **100%** against the 80% floor.
- [x] The floor has teeth, proven three ways in `driver::tests`: a stub lever
      whose two sides are the *same session* and whose `activated` returns false
      is rejected **even though every one of its comparisons passes**; and both
      row budgets fire against deliberately impossible ceilings.
- [x] Soak runs in its own nightly job, with the shared metamorphic soak filter
      excluding `dqp::` so it does not run twice.

### Verification

```
cargo nextest run -p uni-db --test integration -E 'test(/metamorphic::dqp/)'

DQP_CASES=20000 cargo nextest run --profile soak -p uni-db --test integration \
  --run-ignored ignored-only -E 'test(/metamorphic::dqp::/) and test(soak)'
```

---

## Phase 3 — Pinned lever, admissibility contract, row budget

### Tasks

1. **Lever 2: pinned vs live.** Witness asserts the pinned snapshot resolves to
   **the same** version the live side reads, plus the snapshot-path counter.
   Asserting the versions *differ* would make the oracle unsound, not merely
   unexercised — the two sides would legitimately see different data. Any batch
   where a write advances the live version mid-run is **discarded, not
   compared**.
2. **Admissibility contract** (§3.5.2) as a per-lever declaration, asserted at
   *generation* time so a violation is a harness failure and not a mysterious
   diff:
   - Integer-only aggregates by default. The generator emits `sum`
     (`querygen/mod.rs:545-555`) and `bag_eq` is exact (`diff/mod.rs:37-38`,
     leniency limited to `0.0 == -0.0` / `NaN == NaN`), so float reassociation
     under differing `parallelism`/`batch_size` produces false diffs.
   - `LIMIT` only via the existing `ordered_query()` + `limited_query(n)` pair,
     compared as an **ordered sequence**, not a bag — bag equality cannot detect
     a wrong ordering.
   - Excluded constructs: `rand`-family, wall-clock, ANN recall knobs.
   - **`bag_eq` itself is not loosened.** TLP and NoREC depend on its exactness.
     A tolerant `bag_eq_approx` is added only if float aggregates are wanted
     later, and used only by levers declaring reassociation.
3. **Row budget enforcement**, with the ceilings measured in Phase 0A:

   | lane | tier | cases | ceiling (rows returned, both sides) |
   |---|---|---|---|
   | PR | tiny | 500 | 3 000 000 |
   | nightly soak | tiny | 20 000 | 120 000 000 |
   | nightly soak | smoke | 2 000 | 120 000 000 |
   | nightly (own job) | large | 300 | 90 000 000 |

   Secondary per-case guard that localizes the offending case: the measured max,
   which is the fixture's edge count — 4 000 / 40 000 / 200 000.
4. **Selectivity floor** for fixtures above ~10k vertices: override
   `arb_base_where`'s `2 => None, 1 => Some` weighting (`querygen/mod.rs:565-570`)
   so a bounded-selectivity predicate is always present. **Confirmed necessary by
   measurement, not merely suspected**: `rows p50` came back equal to the
   fixture's vertex count *exactly* at all three tiers, and `rows p95` equal to
   its edge count exactly — every unfiltered case scans the whole fixture.

### Results — **phase complete, 2026-08-12**

Both Tier-2 levers now run, at **100% activation** with zero divergences. The PR
lane is 52.6 s for both (they run concurrently under nextest, so it is the slower
of the two rather than the sum), and the full suite is 3932/3932.

All four soaks were run at full volume rather than extrapolated. 20 000 cases
each, 100% activation, zero divergences:

| soak | rows | time |
|---|---|---|
| `fork_agg_soak` | 40 000 | 10.3 min |
| `fork_soak` | 79 689 216 | 30.8 min |
| `pinned_agg_soak` | 40 000 | 10.6 min |
| `pinned_soak` | 80 257 298 | 33.7 min |

**That measurement changed the nightly volume.** 85.4 minutes serial on a
22-core box; nextest runs the four concurrently in CI, so wall-clock is the
slowest rather than the sum, but four heavy tests contending would push the
33.7-minute one toward both the job's 60-minute timeout and the 54-minute
per-test ceiling. The nightly job now runs **10 000** cases, halving all four and
restoring the margin — 80 000 queries a night across the four soaks. The response
to approaching a ceiling is to cut the volume, not raise the ceiling.

Two of this phase's own specifications did not survive contact with the source:

- **The version-equality witness is not directly assertable.** There is no public
  way to read the live version — `DatabaseMetrics` has no such field, `Session`
  exposes no `pinned_version()`, and on an unpinned `StorageManager`
  `version_high_water_mark()` returns `None` *by construction*, since it is
  `Some` only when a pin is in force. The claim is still checkable by a detour:
  `create_snapshot` flushes before recording its manifest, so the manifest's
  `version_high_water_mark` **is** the live version at that instant, and
  `list_snapshots()` is public. Taking one snapshot at prepare and another after
  the run turns "did a write move the live version?" into a comparison of two
  public numbers. Implemented as a new `Lever::check_invariants` hook, and its
  rejection path is tested by writing to the database mid-run.
- **`LIMIT` is excluded rather than supported.** The plan said to admit it "via
  the existing `ordered_query()` + `limited_query(n)` pair" — but those cannot
  combine: one emits `ORDER BY` with no `LIMIT`, the other `LIMIT` with no
  `ORDER BY`, and no method on `Case` produces a deterministic ordered-`LIMIT`.
  Adding one would not help either: the only sort key is `a.name`, which the
  `Edge` shape repeats once per `WORKS_AT` edge, so ties break arbitrarily. DQP
  takes the proposal's other branch and excludes `LIMIT` — which is the status
  quo, since it renders `base_query()` only, now made deliberate by assertion.

Also worth recording: **the row budget is inert for the aggregate kind.** An
aggregate returns one row per case regardless of how much it scanned, so a
ceiling over rows *returned* cannot catch a runaway aggregate. Phase 1 populated
`rows_scanned`, which would cover both kinds; recalibrating against it is a
worthwhile follow-up, but the current ceilings were measured against rows
returned and are not transferable.

### Exit criteria

- [x] Both Tier-2 levers green — `fork_smoke` and `pinned_smoke` on the PR lane,
      four soaks at nightly volume.
- [x] Row budget fires on a deliberately over-budget configuration — already
      delivered in Phase 2 (`driver::tests`), for both the per-case and run
      ceilings.
- [x] Admissibility violation fails at generation time. Seven tests in
      `dqp::admissibility`, including that the classifier **discriminates** (a
      constant would silently disable the contract) and that the two kinds
      reject each other's output.

---

## Phase 4 — `drive_stateful` and Tier-1 levers

### Tasks

1. **`drive_stateful`** per §3.4.3 — the inverted loop: generate `k` cases, run
   all of them against state A, apply the transition **once**, replay the same
   cases against state B. Without inversion, case 2 onward compares B against B
   and passes vacuously.
2. **`dqp_replay(seed, case_index)`** repro entry point. Proptest's in-place
   shrinker cannot work once side A is gone, so shrinking rebuilds both states
   from the seed. Printed as the repro command in every failure message.
3. **Levers 3–6:** L0-vs-L1 (flush), index absent-vs-present, plan-cache
   cold-vs-warm (needs the phase-2 session context), pre-vs-post-compaction.
4. **The large tier is forbidden to `drive_stateful`** — a constraint Phase 0A
   discovered, not a preference. `drive_stateful` rebuilds the fixture once per
   batch, and the large fixture measured **621.73 s** to build. At `k = 500` over
   50 000 cases that is 100 rebuilds — **17.3 hours of fixture construction**
   before a single comparison runs. Tier-1 levers therefore run on **tiny and
   smoke only**; the large tier is Tier-2 (`drive_prepared`, built exactly once
   per run) and nothing else.
5. **`k` from the 0A measurement.** With the tiny tier at 20 000 soak cases and a
   0.163 s rebuild, `k = 500` costs 40 rebuilds ≈ 6.5 s total — negligible. On
   smoke at 2 000 cases and 0.788 s, `k = 500` costs 4 rebuilds ≈ 3 s. Both fine;
   the constraint binds only on large, which is excluded above.

### Exit criteria

- [ ] All four Tier-1 levers green, each with ≥80% activation.
- [ ] `dqp_replay` reproduces a deliberately injected failure from its printed
      seed alone.
- [ ] Plan-cache lever demonstrably registers a hit on side B — via a
      **`SessionMetrics::plan_cache_hits` delta**, not `QueryResult.plan_cache_hit`,
      which Phase 0A showed is a write-path-only field and permanently `false` on
      the read path.

---

## Phase 5 — Generator widening, `EXPLAIN`, teeth

### Tasks

1. **Widen `Shape`** (`querygen/mod.rs:120`) beyond today's two variants
   (`Shape::Node(PERSON_A)` and `Shape::Edge(PERSON_A, COMPANY_B)`,
   `arb_shape()` at :558): 2-hop paths, variable-length `*1..3`,
   `OPTIONAL MATCH`, aggregation-with-grouping. These are where pushdown and
   projection bugs live. Fields on `Case` are private (:341), so variants are
   added as methods there.
2. **Every new variant joins the round-trip proptest** at `querygen/mod.rs:623`,
   or `render()` panics on unhandled AST (`render.rs:44`).
3. **`render_statement` extension for `EXPLAIN`** — handled by `normalize`
   (`render.rs:71`) but not by `render`. Required for plan-text witnesses.
4. **Teeth.** Hand-written `#[tokio::test]` cases following `tlp.rs:124-167`,
   one per historical bug in the proposal's §2.1 table: the Lance `"col"`
   string-literal fusion bug, #103, #135, #99, #97, #110. **Each verified once
   against a deliberately reverted fix** — an oracle that cannot re-catch the
   bugs that motivated it is not yet correct, and this is the only phase that
   proves it can.

### Exit criteria

- [ ] Round-trip proptest green over the widened generator.
- [ ] Each teeth case documented with the revert it was validated against.
- [ ] At least one historical bug re-caught by a *generated* case, not only by
      its hand-written tooth.

---

## Phase 6 — CERT, ANN directional oracle, Tier-3 decision

### Tasks

1. **CERT monotonicity:** `|Q WHERE p AND q| <= |Q WHERE p|`, using the existing
   `bag_is_subset` (`diff/mod.rs:166`). Reuses DQP's generator, fixtures and
   drivers wholesale — an additional assertion, not an additional harness.
2. **ANN directional oracle:** increasing `nprobes` / `refine_factor` /
   `ef_search` is monotonically non-decreasing in recall@k against the
   brute-force oracle already in `dense_retrieval.rs` / `vector_recall.rs`.
   These knobs are excluded from bag equality precisely because they change
   recall by design, so they get a directional law instead.
3. **VID-determinism experiment** — the plan's one genuinely open question: is
   VID assignment deterministic given an identical insert sequence? Build two
   instances from the same seed and compare VIDs directly. A small,
   self-contained test.
   - **Deterministic** → Tier-3 `UniConfig` levers compare bags directly; the
     identity-free projection is unnecessary and is not built.
   - **Nondeterministic** → Tier 3 is restricted to
     `Case::identity_free_projection()`, added in this phase.
   - Do not design around either answer before running it.
4. **Tier-3 levers** on the answer: `batch_size` (`config.rs:420`), `parallelism`
   (:417), `partial_lance_writes` (:502), `async_flush_enabled` (:543),
   `auto_flush_threshold` (:426), `compaction.max_l1_runs` (:449),
   `fork_index_build_threshold` (:603) / `disable_fork_index_builder` (:614),
   `index_rebuild.auto_rebuild_enabled` (:486). **Not** `ssi_enabled` (:639) or
   `defer_embeddings` (:519) — neither is result-neutral.

### Exit criteria

- [ ] VID-determinism answer recorded in `docs/perf/dqp-feasibility.md`.
- [ ] CERT and ANN oracles green.
- [ ] Tier-3 levers ship, or are explicitly deferred with the experiment's
      answer as the reason.

**C1 is complete at the end of phase 6.** Every box in the proposal's §3.8
acceptance list maps to a phase above.

---

## Phase 0B — iai qualification pilot *(runs in parallel with 0A)*

**Goal:** find out which targets can be gated on instruction counts at all. No
gate is added in this phase.

Instruction counts miss I/O, cache effects and parallelism, so a flush with
worse locality can regress badly at a flat instruction count. Gating a target
that does not qualify is worse than not gating it, because it trains everyone to
ignore the gate.

### Tasks

1. Instrument all 7 candidate targets under `iai-callgrind`.
2. Run each ≥20 times across ≥3 CI runner instantiations.
3. Compute per-target coefficient of variation, **and separately** the
   correlation between instruction delta and wall-clock delta over a set of
   deliberately injected regressions. CV alone is not sufficient — a target can
   be perfectly repeatable and still not track real performance.
4. Publish `docs/perf/iai-qualification.md`.

Priors, to be confirmed or refuted:

| Target | Prior |
|---|---|
| Cypher parse + plan (cold cache) | CPU-dominant — expect qualifies |
| Single-vertex lookup by id | CPU-dominant — expect qualifies |
| `expand_batch` 1-hop, warm adjacency | CPU-dominant — expect qualifies |
| Property read across L0/L1 boundary | mixed — pilot decides |
| Transaction commit (WAL on) | IO-dominant — expect **does not** qualify |
| L0 → L1 flush | IO-dominant — expect **does not** qualify |
| HNSW top-10 search | cache-dominant — pilot decides |

### Exit criteria

- [ ] `docs/perf/iai-qualification.md` published with per-target CV and
      instruction-vs-wall-clock correlation.
- [ ] Qualified set = targets with CV < 1% **and** demonstrated correlation.
- [ ] Non-qualifying targets documented as nightly-only with the reason.

### Stops the chain if

Fewer than three targets qualify. A gate over one or two narrow targets is not
worth a Valgrind-cost CI job; in that case B1 is re-scoped to a post-merge trend
report and the decision recorded.

---

## Phase 7 — Perf gate rollout

### Tasks

1. **One** new bench target, `crates/uni/benches/hot_paths_iai.rs`, containing
   only qualified benches. `docs/test_layout.md:15`'s 3-binary cap covers
   `tests/` only, not benches — which is how 18 bench binaries coexist with a
   3-test-binary cap. This plan does not worsen that ratio.
2. `docs/perf/iai-baseline.json` committed; regeneration script in `scripts/`.
   Baseline moves are reviewed **in the PR diff**, not in a dashboard nobody
   opens.
3. Dedicated `perf-gate` job in `pr.yml` with `save-if: false`. Cost is real and
   should be stated in the PR that adds it: Valgrind carries a 10–50× multiplier,
   and the Swatinem cache's only saver is `ci.yml:56` (`tck-full`), which never
   builds `--benches` — so this job pays a cold bench compile every time.
4. Gate: **fail above 5%** instruction-count regression on a qualified target,
   **warn at 2%**.
5. **Cleanup: delete `crates/uni/benches/pushdown_performance.rs`.** It has no
   `[[bench]]` stanza but `autobenches` is not disabled, so Cargo auto-discovers
   it with the default libtest harness while the file calls `criterion_main!` —
   `cargo bench --bench pushdown_performance` fails today. Its three functions
   have empty TODO bodies and reference no uni-db API.

### Exit criteria

- [ ] `perf-gate` fails on a **deliberate-regression PR** — verified, not assumed.
- [ ] `perf-gate` passes on an unrelated PR.
- [ ] `pushdown_performance.rs` removed and `cargo bench --benches` builds clean.

---

## Phase 8 — C4 cheap correctness wins *(independent)*

Four unrelated items, any order, no dependency on tracks A/B:

1. **`cargo-llvm-cov` once, as a map — not a gate.** Deliverable: which modules
   across the 35 workspace members have zero coverage. Publish to `docs/perf/`;
   re-run quarterly. Percentage gates breed test theater and are explicitly not
   proposed.
2. **miri on pure-logic crates only:** `uni-btic`, `uni-crdt`, `uni-common`,
   `uni-sparse-vector`. The BTIC codec has unsafe surface and a committed
   `btic_decode` crash artifact in `fuzz/artifacts/`. miri cannot run the
   Lance/DataFusion crates — do not attempt workspace-wide.
3. **`cargo-deny`** in `pr.yml`: advisories, licenses, bans, sources. No
   supply-chain gate exists today.
4. **Fuzz on PR** at 30 s/target corpus-seeded (~2 min total), keeping the
   existing 5 min/target nightly run. Today a parser or codec regression lands
   and surfaces a day later.
5. **Hypothesis `RuleBasedStateMachine`** for the Python bindings. All 1,073
   pytest functions are example-based, and the sync/async API-symmetry contract
   in the contributor guide is exactly what a state machine checks well.

---

## Phase 9 — C2 fork 2PC failpoints

**Baseline, stated so it is not re-litigated:** the repo already ships fail-rs
with a `failpoints` feature and 11 `fail_point!` sites, plus crash/reopen suites
across commit, flush, WAL, index and fork recovery. This phase adds failpoints to
uncovered windows; it does not build a framework.

### Tasks

Failpoints go in the **orchestration files as well as `registry.rs`** — the
registry supplies the `begin_*`/`finish_*` transitions, but the multi-step work
that can tear happens in the callers, between those calls.

- **Create** (`uni/src/api/fork.rs`): flush and capture the fork point (:300-324)
  → `begin_create` (:399) → allocator bootstrap (:409, with `rollback_create` at
  :417) → `build_datasets_for_fork` (:427) → `finish_create`. The only existing
  failpoint in this entire span is `nested_fork_before_branch` (:567).
- **Drop** (`uni/src/api/fork_admin.rs`): drain holders (:177) → `begin_drop`
  (:178) → evict the cached `Weak<UniInner>` (:185) → force-delete each branch in
  a **loop** (:203-209) → `finish_drop`, deliberately skipped if any delete
  failed so the recovery tombstone survives (:186-190).

One failpoint per inter-step window, and specifically one **inside** the
branch-delete loop so a partially-deleted fork is reachable.

### Exit criteria

- [ ] Recovery matrix green from every failpoint: recovery lands on Active or
      Tombstoned, never a torn state.
- [ ] A crash mid-delete-loop provably leaves the tombstone intact.

---

## Phase 10 — C2 compaction matrices and the abort harness

### Tasks

1. **Two separate compaction matrices — they are two mechanisms, not one.**
   - *Semantic compaction* (uni-owned): failpoints between the steps uni
     controls, asserting no tombstone resurrection and no row loss across a
     `max_l1_runs` merge. This is where the work lands.
   - *Lance optimization* (upstream-owned): `optimize_table`
     (`uni-store/src/backend/lance.rs:600-624`) calls
     `lance::dataset::optimize::compact_files` as **one opaque upstream call** —
     file merge and manifest commit both happen inside it. A
     `compaction::post-merge-pre-manifest` failpoint therefore **cannot be
     implemented at the uni level**. Treat `optimize_table` as atomic and crash
     *around* it: before, and after `compact_files` but before the cleanup pass,
     which is a boundary uni does control. Record the finer-granularity gap as a
     known limitation rather than silently attempting it.
2. **Child-process `abort()` harness.** Every "crash" in the suite today is a
   panic-in-task followed by `drop(db)`, which still runs the shutdown flush —
   `ssi_resilience.rs` flags the confound in its own comment. So the existing
   tests validate *graceful-close* atomicity, a strictly weaker property, and the
   two have already diverged in practice (#167's shutdown-triggered final flush
   *recreating* the directory was a `Drop`-path behaviour). Spawn the test body
   in a subprocess, arm a failpoint, `std::process::abort()` at it, reopen from
   the parent, assert durability and no resurrection.
3. **`madsim` spike, time-boxed, uni-owned path only** (WAL + L0 + flush
   coordinator). madsim requires all time/IO to route through its shims and
   Lance/DataFusion sit underneath, so full adoption is likely infeasible.
   Deliverable is an adopt/partial/reject decision with evidence — **not** an
   adoption.

### Exit criteria

- [ ] The four existing `ssi_resilience.rs` crash tests re-run under the abort
      harness and pass, **or** their failures are triaged as real findings.
- [ ] Both compaction matrices green; the upstream limitation documented.
- [ ] madsim spike report filed.

---

## Phase 11 — B2 LDBC SNB

Path deliberately avoids the Java/Spark toolchain: generate SF0.1 and SF1 once
offline; commit the SF0.1 CSVs; store SF1 in S3 with a checked-in manifest and
checksum; write a Rust loader in `crates/uni/benches/ldbc/`; implement the 14
complex reads as Cypher; validate against LDBC reference answers at SF0.1;
publish SF1 latency percentiles to `docs/perf/ldbc_snb_<date>.md`.

The queries are the deliverable. The official driver is optional and needed only
for formally audited results.

- [ ] SF0.1 loads; all 14 complex reads match LDBC reference answers.
- [ ] SF1 latency percentiles published.
- [ ] Graphalytics BFS/PR/WCC/SSSP results for `uni-algo`.
- [ ] Nightly SF0.1 lane as a correctness regression guard.

---

## Phase 12 — C3 Elle and B3 vector/retrieval quality

### C3 Elle

1. Rust driver spawning N concurrent tasks doing `append(key, value)` /
   `read(key)` over graph properties, recording an EDN/JSON history.
2. Pipe through `elle-cli` with `--consistency-model serializable`. `elle-cli` is
   a JVM artifact, so this is a **nightly lane with `setup-java`**, never a PR
   gate.
3. **Negative control must be deterministic.** "Run with `ssi_enabled = false`,
   expect G2" is unsound — a lucky schedule under LWW is still serializable, so
   the control would flake. Use a hand-constructed anomalous history (write-skew
   / G2 cycle) fed to the checker directly, plus a deliberately faulty adapter
   that drops an anti-dependency edge. An LWW run may be kept as a *non-gating*
   observation.
4. Escalation if the JVM dependency is refused: native Rust G0/G1c/G2 cycle
   detection over the same history format, accepting that it reimplements a
   subtle, well-tested checker.

### B3 vector/retrieval

- recall@10-vs-QPS Pareto curves at swept `ef_search`/`nprobes` for HNSW /
  IVF_PQ / RaBitQ on SIFT-1M. Real curves need ≥1M vectors — the existing
  `fork_index_recall_bench.rs` reports recall@10 = 1.000 only because at n=1000
  Lance brute-forces and the index under test is never used.
- BEIR nDCG@10 on ≥3 subsets (SciFact / NFCorpus / FiQA) for dense, sparse,
  multivec and RRF, **with the fusion-vs-best-single-head delta stated
  explicitly**. This is deliberately the benchmark that can show a feature is not
  earning its complexity. Publish regardless of outcome.

---

## Phase 13 — B4 contention curves and consolidation

Formalize `ssi_contention.rs` as throughput and abort-rate vs contention,
sweeping Zipf θ and thread count — a single number hides the shape change from
"aborts rise gracefully" to "aborts collapse throughput". Nightly, table into
`docs/perf/`.

Close out by consolidating `docs/perf/` into a single index: feasibility,
qualification, baseline, LDBC, ANN/BEIR, contention.

---

## 2. Cross-cutting rules

- **No new top-level test binary.** `docs/test_layout.md`'s hard cap of 3
  integration-test binaries per crate holds. Everything in track A goes under
  `crates/uni/tests/common/metamorphic/dqp/` and reaches CI through the existing
  `metamorphic::` name filter.
- **`cargo nextest run`**, never `cargo test`.
- **Nothing existing is retired.** TCK, TLP/NoREC, loom/shuttle, the Locy oracle,
  the failpoint suites and the fuzz targets all stay. Every item is additive.
- **A measurement that refutes the design is the phase succeeding.** Phases 0A
  and 0B exist because rev 2 of the proposal committed to numbers it had not
  measured. If they come back badly, the correct response is to re-scope in the
  open, not to proceed on the original numbers.

## 3. Ordering rationale

Both P0 tracks open with a measurement rather than a build, because in both
cases the design downstream depends on a number nobody has yet observed.

C1 is staged **Tier-2 first** (phase 2) so bug-finding value lands before the
`drive_stateful` work — but Tier 2 is gated on phase 1's runtime counters, since
without them its activation witnesses are unobservable and the levers cannot
honestly ship.

C3 is sequenced late because it *demonstrates* a property there is reason to
believe holds, whereas C1 and C2 hunt bugs there is reason to believe exist.
