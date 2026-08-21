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

## Status — verified against source, 2026-08-21

Track A (C1, the DQP oracle) is **complete through Phase 6, plus the half of
Phase 4B that became buildable**. Track B (B1, the perf gate) has its pilot
published but is **blocked on one unrun measurement**. Tracks C–E are untouched
except for the four Phase-8 items already recorded below.

| phase | item | status | evidence in-tree |
|---|---|---|---|
| 0A | DQP feasibility | **done** 2026-08-12 | `docs/perf/dqp-feasibility-2026-08-12.md` |
| 1 | Runtime counters | **done** 2026-08-12 | `dqp/counters.rs`, `QueryCounters` |
| 2 | Harness + fork lever | **done** 2026-08-12 | `dqp/{lever,driver,fork_lever}.rs` |
| 3 | Pinned lever, admissibility, row budget | **done** 2026-08-12 | `dqp/{pinned_lever,admissibility}.rs` |
| 4A | `drive_stateful`, flush + plan-cache levers | **done** 2026-08-12 | `dqp/{stateful,flush_lever,plan_cache_lever}.rs` |
| **4B** | index lever | **done** 2026-08-20 — unblocked by #175 | `dqp/index_lever.rs` |
| **4B** | compaction lever | **still deferred** — but on a new reason: #172 is fixed, the witness gap is not | `dqp/transition_probe.rs` |
| 5 | Generator widening, `EXPLAIN`, teeth | **done** 2026-08-13 | `docs/testing/teeth-2026-08-13.md`, 6 revert patches |
| 6 | CERT, ANN law, Tier-3 answer | **done** 2026-08-13 | `dqp/{cert,tier3_probe,vid_determinism}.rs` |
| 0B | iai qualification pilot | **done, one leg open** | `docs/perf/iai-qualification-2026-08-12.md` §4 |
| 7 | Perf gate rollout | **not started** — cleanup only | no `perf-gate` job, no `iai-baseline.json` |
| 8 | C4 cheap wins | **4 of 5 done** | see the phase's own progress list |
| 9–13 | C2 / B2 / C3 / B3 / B4 | **not started** | 11 `fail_point!` sites, unchanged since Phase 0 |

**Five levers now ship** — fork, pinned, flush, plan-cache, index — plus CERT as
an additional law over the same drivers, across **8 smoke tests** in the PR lane
and **10 soaks** in the nightly `dqp` job.

### What the last ten days changed, that the phases below do not yet say

1. **Phase 4B is half-closed.** The index lever landed on 2026-08-20 at 100%
   activation. It did not become buildable by trying harder; it became buildable
   because `QueryMetrics::index_scans` was added (#175, `4ed90d906`) from Lance
   7.0.0's `Scanner::scan_stats_callback`, which reports what the executed plan
   consulted rather than what the planner predicted. The tripwire test that
   justified the deferral is what signalled the unblock.
2. **The oracle's observability work found three product defects**, none of them
   in test code: a failed physical index build reported itself `Online`
   (`1ead21741`), declared scalar indexes were not built at the flush that
   creates the table (`a2876e37a`), and `OperatorStats::index_hits` shipped in
   `PROFILE` output hardcoded to `None` (`61bbc78a0`).
3. **Phase 8's coverage map produced follow-through the plan never scheduled**:
   `VidLookupJoinExec` was made reachable and three defects it had been hiding
   were fixed (`76789a0ee`), with an operator-reachability test to keep it that
   way (`ebbc2c114`), and the 29 silent-downgrade sites in the planner are now
   catalogued (`docs/testing/silent-downgrades-2026-08-15.md`).
4. **Phase 7 is gated on a workflow that exists and has never been run.**
   `perf-qualify.yml` is dispatch-only; until it produces cross-runner CV
   figures there is no defensible threshold, so the 5%/2% numbers below remain
   the pilot's recommendation rather than a measured gate.

### Open, in the order they unblock things

| # | item | blocks |
|---|---|---|
| 1 | Run `Perf Qualify (cross-runner iai)` and replace §4 of the qualification doc with its table | all of Phase 7 |
| 2 | ~~Give `CompactionStats` real numbers~~ **DONE 2026-08-21** (#172) — did *not* unblock the lever; see below | Phase 10's compaction matrix |
| 3 | Re-measure the nightly `dqp` job and add `index_pushdown_soak` | the index lever is smoke-only today |
| 4 | Wire the miri lane measured in Phase 8 into CI | nothing; it is measured and unshipped |
| 5 | Phase 8 item 5 — Hypothesis `RuleBasedStateMachine` for the bindings | closes C4 |

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

### Results — **phase complete (4A), 2026-08-12**

The phase opened by measuring whether the four proposed transitions can be
witnessed at all, in `dqp/transition_probe.rs`. The answer split them in half and
that split is the phase's main finding.

| transition | witness | verdict |
|---|---|---|
| flush (L0→L1) | `l0_reads` / `storage_reads` | **built** — Tier 1 |
| plan cache cold→warm | `SessionMetrics::plan_cache_hits` delta | **built** — but Tier **2**, not Tier 1 |
| index absent→present | none exists | **deferred** |
| pre→post compaction | none exists | **deferred** |

**The plan-cache lever is Tier-2, and that is a correction rather than a
shortcut.** The plan grouped it with flush and compaction as a transition on the
database. It is not: the plan cache belongs to the `Session` (`session.rs:202`,
constructed at `:257`), and a fresh `db.session()` measures `hits=0 misses=0
size=0`. Cold and warm are two sessions held open at once, so the lever runs
under `drive_prepared` with no new driver. Side B runs each query twice — the
first execution of a generated text is necessarily a miss — and observes the
second.

**Two levers are deferred, with the measurement as the reason.**

- **Index.** No counter in `QueryMetrics` moves when a scalar index is created,
  and the logical plan is *byte-identical* before `apply()`, after it, and after
  an explicit `indexes().rebuild()` — which also returned `None` with an empty
  `rebuild_status()`. The one place index usage is modelled,
  `OperatorStats::index_hits`, is hardcoded `None` at all three construction
  sites (`executor/core.rs:1068,1102`), so it is a dead field behind the PROFILE
  path rather than a witness. Lance may well be using the index below the plan
  layer — the fork lever's premise is that `use_scalar_index(false)` on branch
  scans *matters* — but the narrow claim is all the deferral needs: at the
  observability this repo offers, index-absent and index-present are
  indistinguishable, so a lever between them cannot prove it activated.
- **Compaction.** No per-query counter moves, and the run-level observable that
  should have rescued it does not exist either: `compact_label` returns a
  **hardcoded literal** (`uni-store/src/storage/manager.rs:876`) with
  `files_compacted: 1` regardless of what was merged and `bytes_before` /
  `bytes_after` at `0` — as do all six construction sites in that file. Only
  `duration` is real. Five deliberately separate fragments and an immediate
  no-op re-compact all reported identically. **This is a user-facing defect worth
  its own ticket**, independent of the oracle: `db.compaction().compact(...)`
  returns a struct whose every field but `duration` is a constant.

Both probes stay in the suite as **tripwires** rather than comments: they assert
the unobservability that justified the deferral, and their failure messages open
with "good news" and point at 4B. A deferral backed by an executing test does not
quietly become false.

**A background timer nearly made the flush lever vacuous, and the activation
floor caught it.** The first `flush_smoke` run reported **25.8% activation** for a
witness that scored 60/60 when checked directly against generated cases. Root
cause: `auto_flush_interval` defaults to `Some(5s)` with
`auto_flush_min_mutations: 1` (`uni-common/src/config.rs:431,443`), so the
background writer drained L0 five seconds into a ~34-second pass 1 — every case
after that compared a flushed state against a flushed state. `dqp_config()` now
pins `auto_flush_interval: None`, with a test asserting it, because the failure
mode is a `..Default::default()` away from returning and would read as witness
drift rather than as a background task. Same species as
`disable_fork_index_builder`. With the timer off: **100% activation.**

**Reproduction by seed works and discriminates.** `drive_stateful` gives up
proptest's shrinker — it would re-run every shrink candidate against side B's
state and conclude no failure reproduces — in exchange for `replay_stateful` and
a printed `DQP_SEED` / `DQP_BATCH` / `DQP_CASE`. That trade is tested rather than
asserted: a fault injected at a known case fails the driver, the coordinates it
prints reproduce the identical query and the identical bag difference, **and
replay one case over reports "bags AGREE"**. Without that third check, a replay
that panicked wherever it was pointed would have passed the first two.

The cost is real and stated: a failure reports the generated query, not a
minimized one.

**The nightly volume was re-measured rather than assumed to still fit.** Phase 4
takes the nightly job from four soaks to eight, so the whole configuration was
run as CI runs it — all eight concurrently at `DQP_CASES=10000` on a 22-core box:

| soak | time | | soak | time |
|---|---|---|---|---|
| `fork_agg_soak` | 8.8 min | | `fork_soak` | 21.7 min |
| `pinned_agg_soak` | 9.7 min | | `pinned_soak` | 22.0 min |
| `flush_agg_soak` | 10.2 min | | `flush_soak` | 23.9 min |
| `plan_cache_agg_soak` | 12.3 min | | `plan_cache_soak` | 29.9 min |

**29.9 minutes wall-clock**, all eight passing, against a 60-minute job timeout
and a 54-minute per-test ceiling. So `DQP_CASES: 10000` stands unchanged — and
the measurement is pessimistic for CI, since eight heavy tests contending over 22
cores is more contention than the 64-core runner sees. 160 000 queries a night
across the eight.

One limit on what that run proves: it did not use `--no-capture`, which forces
serial execution, so nextest swallowed the per-run activation lines. The eight
passes do establish that **each cleared the 80% floor** — that assertion lives
inside the test — but the exact rates are not quotable from this run. The 100%
figures above are from the captured smoke runs.

**A pre-existing CI defect surfaced while wiring this up.** The nightly `soak`
job filters on `test(/soak/) | test(/stress/)`, which matches every DQP soak by
name — so since Phase 2 they have been running a *second* time each night under
that job, at the default 20 000 cases rather than the 10 000 the `dqp` job sets,
inside a timeout sized for a different workload. The `metamorphic` job already
excluded `dqp::`; the `soak` job did not. Fixed, and verified by count: the old
filter matched 8 DQP tests, the new one matches 0.

### Exit criteria

- [x] ~~All four~~ **Both witnessable** Tier-1 levers green, each with ≥80%
      activation — `flush_smoke` and `plan_cache_smoke` at **100%**. The index and
      compaction levers were deferred to 4B with the measurement above as the
      reason; the original criterion was not achievable, and the probes that show
      why now run in the suite. **Update 2026-08-20: the index lever has since
      shipped** at 100% activation, once #175 supplied its witness — see the
      rewritten 4B section. Compaction is still deferred.
- [x] `dqp_replay` reproduces a deliberately injected failure from its printed
      seed alone — `a_failure_reproduces_from_its_printed_coordinates`, which also
      asserts it does *not* fire one case over.
- [x] Plan-cache lever demonstrably registers a hit on side B — via a
      **`SessionMetrics::plan_cache_hits` delta**, not `QueryResult.plan_cache_hit`,
      which Phase 0A showed is a write-path-only field and permanently `false` on
      the read path.

### Phase 4B — **half shipped, 2026-08-20**; half still deferred

Both halves were deferred on the same reason — no witness existed — and the
tripwire tests were written to fail the moment one appeared. One did.

#### Index absent-vs-present — **shipped** (`dqp/index_lever.rs`)

The unblock was not persistence, it was a new observable. `4ed90d906` (#175)
added `QueryMetrics::index_scans` from Lance 7.0.0's
`Scanner::scan_stats_callback`, which harvests the metrics of the plan that
**executed** — so, unlike `ExplainOutput::index_usage` (whose `used: true` is
hardcoded) or the `assert!(rows <= 1)` in the #57 tests (satisfied by the
in-process `extra_runtime_filter` regardless of what Lance did), it reports
execution rather than prediction. `Witness::index_scans` was added in that same
change for this lever.

Measured: **500/500 cases activated (100.0%)**, 28.0 s, zero bag divergences —
which is the expected result, since an index changes how rows are found and
never which. The activation rate is the number that matters here.

Three things about it are worth carrying forward:

- **`scans_reported` is the load-bearing counter, not `index_scans`.** Without a
  denominator, `index_scans == 0` is ambiguous between "no scan ran", "a scan
  consulted nothing", and "the callback was never wired" — so every negative
  assertion would be satisfied by the exact regression it exists to catch.
- **The lever is restricted to `CaseKind::Pushdown`, and the restriction is
  load-bearing rather than tidy.** A scalar index is consulted only for an
  `=`/`IN` on a Hash-indexed column. Measured over the generator's kinds, the
  fraction of cases that can consult one is ~7% for `Plain` and ~22% for the
  selective variants — both below the 80% floor. A `Plain` variant would fail
  its floor for a reason having nothing to do with indexing, so it is
  deliberately not registered.
- **The spike ran before any lever code was written**, so a 0% activation result
  could not be mistaken for a broken lever, witness, or fixture — four
  explanations, one symptom. It also produced the first evidence that
  `Fixture::TINY_INDEXED` had ever had a physical index: 1 000 rows scanned down
  to 103. It had been named "indexed" without being one, which is what
  `a2876e37a` fixed.

**Outstanding on this lever: there is no soak.** The nightly `dqp` job runs ten
concurrent soaks in a 60-minute budget, last measured at 29.9 minutes; an
eleventh needs that re-measured rather than assumed, per the workflow's own
comment. Tracked as open item 3 in the status table above.

#### Pre-vs-post compaction — **still deferred, on a corrected reason** (2026-08-21)

`CompactionStats` is now honest (#172): `tables_optimized`, `fragments_removed`,
`fragments_added`, `files_removed`, `files_added` and `bytes_reclaimed` are all
measured, `bytes_before`/`bytes_after` are gone, and both tripwires below have
been flipped into positive assertions.

**That did not unblock the lever, and the reason is structural rather than
temporary.** `activated` takes two `Witness`es, which are per-*query*;
`CompactionStats` is per-*run*. A run-level number cannot feed a per-query
predicate, and `check_invariants` — the only run-level hook — returns `Err`
("reject the run"), not "this case activated". So the correct place for
`CompactionStats` in a future lever is as a run *precondition* (did the
transition actually merge anything?), never as the activation signal.

What would unblock it is unchanged and unmet: a per-query counter that moves
across a compaction. The candidate is `iops`/`requests` from Lance's
`ExecutionSummaryCounts`, which the scan-stats callback already receives and
discards. Measuring that is Phase 4B's remaining work.

**Two findings from doing it, both worth carrying:**

1. **Compaction runs one flush behind.** The pass immediately after a flush
   reports no work; a second pass immediately after — with nothing happening in
   between, so it is not time-based — merges. Reproduced on every round:
   `compact#1: removed=0` then `compact#2: removed=2 added=1`. Pre-existing, and
   invisible for as long as the counts were literals. Tests now assert a
   fixpoint (two consecutive quiet passes) rather than single-pass completeness.
   Not chased further; it is a separate defect from the reporting one.
2. **A test that had never tested anything.** `compaction_granular_test.rs`
   asserted `files_compacted == 1` under a comment reading "we expect compaction
   to run because we have 2 fragments". The hardcoded literal satisfied it
   whether or not anything merged — which is precisely how finding 1 stayed
   hidden.

#### The original deferral, for the record

Re-verified 2026-08-21 and unchanged: `CompactionStats` still reports
`bytes_before: 0`, `bytes_after: 0` and `crdt_merges: 0` at every construction
site in `uni-store/src/storage/manager.rs`, and `compact_label` still returns a
literal `files_compacted: 1` (`:877`). Where `files_compacted` is computed at all
(`:856`, `:901`, `:1144`) it counts **tables optimized**, not files merged — so
even the one non-constant field does not mean what its name says. Only `duration`
is real.

This remains a user-facing defect worth its own ticket independent of the oracle:
`db.compaction().compact(...)` returns a struct whose every field but `duration`
is a constant or a mislabelled count. The tripwire in `dqp/transition_probe.rs`
still asserts the unobservability, so the deferral cannot quietly become false.

---

## Phase 5 — Generator widening, `EXPLAIN`, teeth

### Tasks as originally written — **superseded, kept for the record**

> 1. **Widen `Shape`** (`querygen/mod.rs:120`): 2-hop paths, variable-length
>    `*1..3`, `OPTIONAL MATCH`, aggregation-with-grouping. These are where
>    pushdown and projection bugs live.
> 2. **Every new variant joins the round-trip proptest.**
> 3. **`render_statement` extension for `EXPLAIN`** — required for plan-text
>    witnesses.
> 4. **Teeth.** Hand-written `#[tokio::test]` cases, one per historical bug,
>    each verified against a deliberately reverted fix.

**Tasks 1, 3 and 4 were wrong, and the phase was re-scoped before
implementation.** What follows is what was built and why, because the reasoning
is more reusable than the task list was.

#### Correction 1 — the four shapes score 0/6 on bug reachability

Measured against the six historical bugs the phase exists to re-catch, none of
the four proposed shapes reaches any of them. Worse, three are harmful or inert:

- **`*1..3` is provably vacuous.** Both fixtures are bipartite `Person→Company`
  with one `WORKS_AT` edge type, so length > 1 is unreachable and every 2- and
  3-hop expansion returns zero rows. `bag_eq(∅, ∅)` is green forever.
- **`OPTIONAL MATCH` and grouped aggregation break the existing oracles
  semantically** — false positives, not compile errors. `partition_query`
  conjoins the partition predicate into the MATCH clause's `WHERE`; under
  `OPTIONAL` a failing row is null-extended rather than dropped, so
  `bag(base) ≠ t ⊎ f ⊎ n`. NoREC breaks identically. Grouped aggregation breaks
  `run_scalar`'s 1-row×1-col contract and structural's ungrouped `count(*)` law.
- **2-hop is law-safe but reaches no new code** on a typed fixture.

#### Correction 2 — the binding constraint is the *fixture*, not the grammar

The task list has no fixture dimension at all, and that is where the teeth
actually live. Two examples, both verified by reading rather than assumed:

- `build_edge_adjacency_and_target_props` (the **#135** fix site) has exactly one
  caller, under `GraphTraverseMainStream`, which is planned only when every
  requested relationship type is **absent from the schema**
  (`planner.rs:5405`). Both fixtures declare `WORKS_AT`, so no generated query
  can reach it — at any shape. The #135 regression test declares its label but
  never its `PARENT` edge type, and that omission is exactly why it reproduces.
- The **`"col"` fusion** needs a Hash scalar index, and the DQP fixture declared
  no indexes whatsoever.

A widened generator over a fixture that routes around the defect yields a wider,
greener, still-toothless oracle.

#### Correction 3 — task 3's rationale for `EXPLAIN` is invalid

"Required for plan-text witnesses" contradicts this codebase's own Phase-3
reasoning: `dqp/lever.rs:26-34` explicitly rejects plan-difference as an
activation rule ("a universal plan-difference check would reject precisely the
levers most worth testing"). `Witness` is `Copy + Eq` and all-integer, so a
`String` field breaks it; and `format!("{:#?}", plan)` is not ordering-stable
across processes. The change is worth making for a different reason —
**routing assertions**, which are what Correction 2 needed.

#### Correction 4 — task 4 duplicates six existing regression suites

Five of the six bugs already have regression tests (twelve for #97 alone). A
seventh near-copy detects nothing new. What was missing was never another
assertion but *evidence that the existing ones bite*, so the deliverable became a
revert ledger plus a validation harness.

### Tasks as built

1. **`SchemaMode` / `Fixture`** in `dqp/seed.rs` — `Typed` and `TypedIndexed`
   (Hash scalar index on `Person.age`), threaded through both drivers as
   `impl Into<Fixture>` so every pre-Phase-5 call site is unchanged and all
   Phase-4 measured budgets stay valid.
2. **Pushdown predicates** in `querygen` — `arb_in_list`, `arb_same_column_conj`
   (`=`/`IN` plus a two-sided inclusive range on one column), `arb_case_pushdown`,
   and `CaseKind::Pushdown`. `arb_pred` previously topped out at two comparisons
   with independently-drawn targets, so three conditions on one column had
   probability zero.
3. **`Query::Explain` in `render`** plus routing assertions that pin which
   operator a fixture plans through.
4. **Round-trip widened** to draw from every case strategy and to include the
   `EXPLAIN` wrapper — closing a pre-existing gap where aggregate cases were
   round-tripped only by a hand-written spike.
5. **Revert ledger + harness** — `docs/testing/reverts/*.patch`,
   `scripts/testing/teeth_validate.sh` (throwaway worktree; asserts the
   pre-existing regression test *fails* before trusting any oracle result), and
   `docs/testing/teeth-2026-08-13.md`.

### Results — **phase complete, 2026-08-13**

- [x] Round-trip proptest green over the widened generator — 512 cases × 10
      variants × 3 strategies.
- [x] Each tooth documented with the revert it was validated against —
      `docs/testing/teeth-2026-08-13.md`, six patches.
- [x] **At least one historical bug re-caught by a generated case** — the Lance
      `"col"` fusion, by `dqp::flush_lever::flush_pushdown_smoke`:
      `bag mismatch: left_total=28, right_total=0`, on case 0 of the batch, with
      replay coordinates printed. The same reconstruction left the pre-Phase-5
      `flush_smoke` **green**, which is the measured before/after.

Two findings worth carrying forward:

- **A latent defect in the flush lever, found by the activation floor.** The
  first pushdown run failed at **63.6% activation**. `FlushLever`'s delta wrote
  five of the fixture's seven `AGE_DOMAIN` values, so a generated `age = 22`
  predicate selected fixture rows but no *unflushed* row — the lever's whole
  premise. Unfiltered `Plain` cases could never expose it, since two thirds carry
  no base `WHERE` and scan the delta wholesale. Fixed at source; **63.6% → 86.6%**.
  Lowering the floor would have turned it green while making the oracle
  permanently weaker.
- **A differential oracle is blind to any defect that damages both sides
  equally.** #135's naive revert (`HashMap::new()`) nulls target properties on
  both sides and is therefore DQP-invisible; a DQP-visible revert must be
  tier-selective. Same shape as the TLP/NoREC blindness recorded in
  `dqp/mod.rs:14`.

### Deferred, with reasons

| item | reason |
|---|---|
| `*1..3` | Vacuous on both bipartite fixtures. Needs a `Person→Person` edge type, which perturbs the exact-cardinality assertions at `metamorphic/seed.rs:107-159`, for zero of the six bugs. |
| `OPTIONAL MATCH` | Breaks TLP and NoREC semantically. A *leading* `OPTIONAL` — all today's single-clause `Case` can build — is degenerate anyway: nothing precedes it to null-extend. |
| Grouped aggregation | Breaks `run_scalar`'s 1×1 contract and structural's count law; needs a new `CaseKind` plus admissibility rule. |
| 2-hop | Reaches no new code on a typed fixture, and on `Tier::Tiny` it is a self-join through a 40-company hub — ~400k rows/case against a 12k per-case ceiling. |
| Schemaless fixture (#135, #99) | `bulk_insert_edges` rejects an undeclared type (`transaction.rs:857`), so edges must go through Cypher `CREATE` — ~2 orders of magnitude slower, forcing a smaller tier. Guarded meanwhile by `typed_fixture_plans_a_typed_traverse`. |
| Plan-text witness | No consumer, unsound across processes, and rejected by `dqp/lever.rs:26-34`. |
| Generated catches for #97/#103/#110 | Each needs a distinct lever capability; see the ledger. #97's blocker — `drive_prepared_with` flushing unconditionally before `prepare` — is worth fixing regardless, since it makes the fork lever structurally blind to a whole bug class. |

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

### Results — **phase complete, 2026-08-13**

- [x] VID-determinism answer recorded — `docs/perf/dqp-feasibility-2026-08-12.md`
      §8. **Deterministic** across identical builds, a flush, repeats, *and a
      config change*.
- [x] CERT and ANN oracles green.
- [x] Tier-3 decision made and recorded: **deferred**, but not for the reason the
      plan anticipated. See below.

#### The VID experiment answered a narrower question than Tier 3 rests on

The plan asks whether VIDs are deterministic "given an identical insert
sequence". But a Tier-3 lever never compares two identical builds — it compares
two builds that differ by a **config knob**. A fixture could be build-to-build
deterministic while allocating different VIDs at a different `batch_size`, so
that stronger property is the one that licenses direct bag comparison. It was
measured separately and holds.

**Consequence:** `Case::identity_free_projection` is not needed and has not been
built.

#### Tier-3 levers are deferred on observability, not identity

The VID answer said go; a different constraint says stop, and it is the same one
that deferred Phase 4B. Measured over the six candidate knobs
(`dqp::tier3_probe`): **observable = 0, inert = 6** — no knob moves any witness
counter, while all bags stay equal.

A lever whose two sides move no counter cannot state what it exercised: its
`activated` predicate would have nothing true to say and the 80% floor would fail
every run, or — written without a witness — it would pass forever while comparing
two identical execution paths.

So Tier 3 ships one assertion rather than a lever set —
`tier3_knobs_are_result_neutral`, which checks the premise directly — plus a
tripwire that **fails the moment any knob becomes observable**, which is the
signal to promote it.

**What would unblock the full set:** a counter describing *how* rows were
produced rather than *what* (morsel count, partition count, a flush-path
discriminator). That is the same gap Phase 4B needs closed, so closing it once
unblocks both.

#### Also found

`diff::bag_is_subset` — CERT's comparator — **had no test**, while its sibling
`bag_eq` did. An oracle is only as good as its comparator, and CERT's
narrowing-rate floor proves the *inputs* differ, not that the *check* works.
`bag_is_subset_has_teeth` now covers it, including the multiset case (`{1,1}`
must not fit inside `{1}`), which a set-based implementation would wrongly
accept.

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

### Results — **published 2026-08-12; one leg open**

`docs/perf/iai-qualification-2026-08-12.md`. **5 of 7 targets qualify**;
`transaction_commit_wal_on` and `l0_to_l1_flush` are rejected as IO-sensitive —
the priors held on all five predicted targets, and both "pilot decides" targets
(`property_read_across_l0_l1`, `hnsw_top10_search`) qualified. Five clears the
stop-the-chain threshold of three.

Both halves of the qualification were necessary and the rejection proves it:
every target has CV < 1% (0.21–0.96%), so **stability alone would have qualified
all seven** — including the two whose measured 2.9× and 2.0× IO-driven slowdowns
are invisible to an instruction count.

Three defects were found before any number was trusted, the sharpest being that
`strip = "symbols"` makes iai-callgrind collect **zero instructions while
reporting success** — a silent-zero of exactly the kind this plan's §0 rules
exist to catch.

### Exit criteria

- [x] `docs/perf/iai-qualification-2026-08-12.md` published with per-target CV and
      instruction-vs-wall-clock correlation.
- [x] Qualified set chosen on CV < 1% **and** demonstrated correlation — five
      targets.
- [x] Non-qualifying targets documented as nightly-only with the reason.
- [ ] **Cross-runner variance — UNRESOLVED, and it gates Phase 7.** All 20 runs
      came from **one machine**, so the CV figures characterize run-to-run
      variance on fixed hardware and say nothing about the machine-to-machine
      spread a PR gate actually experiences. The mechanism to close it exists —
      `.github/workflows/perf-qualify.yml`, 5 runners × 5 runs, aggregated by
      `scripts/perf/iai_cross_runner.py` — and is **dispatch-only and has never
      been run**. The threshold in Phase 7 must be set from that figure, not
      from the 0.21–0.96% measured here.

### Stops the chain if

Fewer than three targets qualify. A gate over one or two narrow targets is not
worth a Valgrind-cost CI job; in that case B1 is re-scoped to a post-merge trend
report and the decision recorded.

---

## Phase 7 — Perf gate rollout

> **Status 2026-08-21: not started, and correctly so.** Only task 5 (the bench
> cleanup) has landed. `crates/uni/benches/hot_paths_iai.rs` exists from the 0B
> pilot, but there is no `perf-gate` job in `pr.yml`, no
> `docs/perf/iai-baseline.json`, and no regeneration script. **The blocker is
> Phase 0B's open leg**, not effort: until `perf-qualify.yml` has been dispatched
> and its cross-runner CV computed, the 5%/2% thresholds below are the pilot's
> recommendation rather than a measured gate, and setting a gate from
> single-machine variance is the mistake §7.1 of the proposal exists to prevent.

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
   it with the default libtest harness while the file calls `criterion_main!`.
   Its three functions have empty TODO bodies and reference no uni-db API.

   **Correction (2026-08-15): this item said "`cargo bench --bench
   pushdown_performance` fails today". Measured, it does not — and the truth is
   worse.** Under the libtest harness the `criterion_main!` main is dead code, so
   there is no symbol conflict; libtest finds zero `#[bench]` functions and
   reports `running 0 tests ... 0 measured; ok` at **RC=0**. It passes while
   measuring nothing. Only a criterion-specific flag fails
   (`-- --save-baseline main` → RC=101, `Unrecognized option`), which is what the
   original narrow observation at `documentation_remediation_2026-08-06.md:271`
   ("*the command it prints* fails") actually referred to. The claim widened as it
   was transcribed between documents.

   **Consequence for the fix:** a build check cannot catch this class, because the
   file compiles. Only comparing `benches/*.rs` against the declared `[[bench]]`
   names does. Both steps are now in `nightly.yml`'s `bench` job.

### Exit criteria

- [ ] `perf-gate` fails on a **deliberate-regression PR** — verified, not assumed.
- [ ] `perf-gate` passes on an unrelated PR.
- [x] `pushdown_performance.rs` removed (2026-08-15). Two `nightly.yml` steps now
      guard the class: a **stanza-coverage check**, verified against a deliberate
      violation rather than assumed (RC=1 naming a temporarily-added stanza-less
      file, RC=0 once removed), and `cargo check -p uni-db --benches` for a
      declared bench that stops compiling.

      **The build variant was measured and rejected.** `cargo bench -p uni-db
      --benches --no-run` takes **76m 08s** on a warm 22-core box — 20 executables
      (19 benches + the lib target), 0 errors — because each target statically
      links the whole datafusion/lance/candle tree, the same cost that caps
      integration-test binaries at 3 per crate in `docs/test_layout.md`. It does
      not fit this job's 90-minute budget. `cargo check --benches` is **50s** and
      covers compile errors across all 19; the three benches the job runs remain
      the standing check on linking. Useful baseline from that run: **all 19 bench
      targets currently link clean.**

---

## Phase 8 — C4 cheap correctness wins *(independent)*

**Five** unrelated items (the count below has always been five), any order, no
dependency on tracks A/B.

### Progress — 2026-08-13

- [x] **3. `cargo-deny`.** Shipped: `deny.toml` + a `supply-chain` job in
      `pr.yml`. `advisories ok, bans ok, licenses ok, sources ok`.
- [x] **4. Fuzz on PR.** Shipped: `fuzz-smoke` job, 30 s/target, seed-corpus
      first. Verified locally at 3.4M runs in 31 s.
- [x] **1. `cargo-llvm-cov` coverage map.** Published:
      `docs/perf/coverage-map-2026-08-14.md`. 78.9% line coverage, 22
      zero-coverage files. **The finding is not the percentage** — it is that
      two *planner-reachable* operators have never executed under any test:
      `vid_lookup_join.rs` (441 lines, reached from `df_planner.rs:4218`) and
      `mutation_foreach.rs` (154 lines, `:1342`). No feature gate; the
      `HashJoinExec` fallback is simply what tests always take. DQP cannot reach
      either — it varies *storage* state, and `querygen` emits no cross-MATCH
      joins — so this is the complement to Phase 5, not a duplicate of it.
- [x] **2. miri on the pure-logic crates.** Run; **zero UB, zero unsupported
      operations** across all four. One real finding, now fixed: a
      `std::mem::forget(tempfile::TempDir)` in
      `uni-common/tests/repro_rename_property_bypass.rs` that leaked the
      directory *on disk*, once per run, forever. The guard is now returned to
      the caller. See the correction below — the item's stated motivation was
      false, and the measured cost re-shapes the recommendation.

  **Measured cost, which should decide how this is wired up:**

  | crate | tests | miri wall-clock |
  |---|---|---|
  | `uni-btic` | 103 | 6.8 s |
  | `uni-sparse-vector` | 25 | 1.5 s |
  | `uni-common` (excl. `muvera`) | 121 | 23 s |
  | `uni-crdt` | 31 | **2,426 s (40 min)** |
  | `uni-common::muvera` | 8 | **killed at 132 min** |

  A ~350× spread, and `muvera`'s float-heavy FDE tests do not finish at all
  under the interpreter. So a single "miri over the four crates" lane is the
  wrong shape. Viable: `uni-btic` + `uni-sparse-vector` +
  `uni-common --skip muvera` ≈ **31 s total**, affordable in the PR lane;
  `uni-crdt` nightly; `muvera` excluded outright with this measurement as the
  reason.
- [ ] 5. Hypothesis `RuleBasedStateMachine` — **still not started** (re-verified
      2026-08-21: no `hypothesis` import anywhere under `bindings/`). This is the
      only one of the five outstanding.

**The miri lane is measured but unshipped.** The costs below decided its shape,
but no miri step exists in any workflow. Wiring `uni-btic` +
`uni-sparse-vector` + `uni-common --skip muvera` (~31 s) into `pr.yml` and
`uni-crdt` into `nightly.yml` is a small, already-designed piece of work.

### Follow-through the coverage map produced — 2026-08-14 → 08-16

The map was specified as a deliverable and nothing more, but its two
zero-coverage planner-reachable operators turned into real work that belongs in
this record:

- **`VidLookupJoinExec` was made reachable, and three defects it had been hiding
  were fixed** (`76789a0ee`). It had been inert since April 2026 because its own
  guard demanded `UInt64` while Cypher properties arrive as `Int64` — so the
  query written to exercise it never fired it. Once fixed: **2.5× / 2.1×**, and
  one of the three defects produced silently wrong rows. "Never ran" is not the
  same as "not worth running".
- **An operator-reachability test** now proves which physical operators actually
  execute, rather than which ones the planner can name (`ebbc2c114`).
- **The 29 silent-downgrade sites in the planner are catalogued** —
  `docs/testing/silent-downgrades-2026-08-15.md` (`a1b57f366`). This is the
  same defect class as the `PROFILE` field hardcoded to `None` and the bench
  that reported "ok" while measuring nothing: code that succeeds while doing
  less than it claims.

**What the first-ever supply-chain run found.** There was no gate at all, and
the default check reported **24 advisories, 10 of them vulnerabilities or
unsoundness**. Three were fixed outright rather than ignored, by an in-semver
`cargo update`:

| advisory | crate | |
|---|---|---|
| RUSTSEC-2026-0190 *unsound* | anyhow 1.0.102 → 1.0.104 | fixed |
| RUSTSEC-2026-0204 *vuln* | crossbeam-epoch 0.9.18 → 0.9.20 | fixed |
| RUSTSEC-2026-0253 *unsound* | lru 0.18.0 → 0.18.2 | fixed |

Both unsoundness advisories in the tree are now gone. The remaining seven are
upstream-pinned — `extism 1.30` holds `wasmtime 43`, and `lance-io → opendal`
holds `quick-xml` and `rsa` — and each carries a dated exception naming its
blocker so it can be re-tested on the next upgrade rather than decaying into
permanent silence.

Three further findings were **ours**, not upstream:

- **`fxhash` is a direct workspace dependency** (`Cargo.toml:158`, used by
  `uni-query` and `uni-crdt`), not transitive. Unmaintained, not vulnerable.
  The fix is migrating to `rustc-hash`; deliberately deferred to its own change
  because swapping a hasher changes `HashMap` iteration order.
- **`uni-tck` and `uni-locy-tck` were unlicensed** — missing
  `license.workspace = true` that every other crate carries. Fixed.
- **…and implicitly publishable.** Their sibling `uni-locy-oracle` already had
  `publish = false`, which is exactly why it did not trip the same check. Fixed.

**Correction to item 2's motivation:** see the note under it. The "committed
`btic_decode` crash artifact" does not exist as described, and the input it
refers to no longer crashes.

Original item list:

1. **`cargo-llvm-cov` once, as a map — not a gate.** Deliverable: which modules
   across the 35 workspace members have zero coverage. Publish to `docs/perf/`;
   re-run quarterly. Percentage gates breed test theater and are explicitly not
   proposed.
2. **miri on pure-logic crates only:** `uni-btic`, `uni-crdt`, `uni-common`,
   `uni-sparse-vector`. miri cannot run the Lance/DataFusion crates — do not
   attempt workspace-wide.

   **Correction (2026-08-13): both stated motivations are false.**

   *"A committed `btic_decode` crash artifact in `fuzz/artifacts/`"* — wrong
   twice over. `fuzz/artifacts/` is gitignored (`fuzz/.gitignore:2`), so nothing
   there is committed; and the input *is* already tracked, as
   `fuzz/seeds/btic_decode/utf8-boundary-bce-suffix`, whose README records it
   fixed on 2026-06-10. Verified: `cargo +nightly fuzz run btic_decode` on that
   input exits 0 against current code.

   *"The BTIC codec has unsafe surface"* — it does not. All four nominated
   crates contain **zero** `unsafe`:

   | crate | `unsafe` occurrences |
   |---|---|
   | `uni-btic` | 0 |
   | `uni-crdt` | 0 |
   | `uni-common` | 1 — a string literal in an error message (`schema.rs:1419`) |
   | `uni-sparse-vector` | 0 |

   That does not make miri worthless: it still detects UB reached *through
   dependencies*, and the crates' own tests exercise those. But the expected
   value is far lower than "this code has unsafe blocks we should check", and
   the item should be re-priced on measurement rather than on the premise above.
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

> **Status 2026-08-21: not started.** Verified rather than assumed — the
> workspace still has exactly **11 `fail_point!` sites**, the same count this
> phase's baseline records, and the only one anywhere in the fork create/drop
> span is still `nested_fork_before_branch` (`uni/src/api/fork.rs:567`).
> `crates/uni-store/src/fork/registry.rs` and
> `crates/uni/src/api/fork_admin.rs` contain none.

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

> **Status 2026-08-21: not started.** No `fail_point!` exists in
> `storage/manager.rs` or `backend/lance.rs`; no `std::process::abort()` appears
> anywhere in `crates/`; `madsim` is in no manifest. Note the dependency the
> plan does not state: the semantic-compaction matrix asserts "no row loss across
> a `max_l1_runs` merge", and the observable it would naturally assert against —
> `CompactionStats` — is the same set of hardcoded zeros that keeps the Phase-4B
> compaction lever deferred. Fixing that once unblocks both.

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

> **Status 2026-08-21: not started.** No `crates/uni/benches/ldbc/`.

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

> **Status 2026-08-21: not started.** No history-generating driver, no
> `setup-java` step in any workflow, no SIFT-1M or BEIR harness.

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

> **Status 2026-08-21: not started.** `crates/uni/benches/ssi_contention.rs`
> remains in its pre-plan shape. `docs/perf/` now holds four documents
> (build baseline, DQP feasibility, iai qualification, coverage map) with no
> index over them.

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
