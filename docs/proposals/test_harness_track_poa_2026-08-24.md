# Test Harness Track — Plan of Action

**Date:** 2026-08-24
**Status:** Plan of action
**Supersedes the status blocks in:**
`docs/proposals/test_harness_and_benchmarks_2026-08-11.md` (rev 3) and
`docs/proposals/test_harness_implementation_plan_2026-08-12.md`

Both documents' status tables were last refreshed 2026-08-21/22 and are now
stale by roughly two phases. This document re-establishes the state against the
tree at `b92b50df9`, then sequences what is left.

Verification rule carried forward from the implementation plan: **a claim about
status cites an artifact in the tree, never a memory of having done it.** Every
row below was re-checked against source on 2026-08-24.

---

## 1. Verified state at `b92b50df9`

`HEAD == md/main`, working tree clean.

> **Update 2026-08-25.** PR #180 ("test-harness and benchmark - PR1") merged to
> `rustic-ai/main` as `67235ea1a`. §2 below was written against stale local
> refs that had not been fetched; it is kept because its *finding* — a
> repo-guarded job reads as green when it skips — remains live, and because the
> nightly consequence in §2.1 is real. **T0 is closed and T1 is complete.**

| phase | item | doc says | **actually** |
|---|---|---|---|
| 0A–6 | C1 DQP oracle | complete, 6 levers | complete, **7 levers** — a `delete_lever` landed 2026-08-22 |
| 0B | iai qualification | done, one leg open | unchanged — cross-runner leg still unmeasured |
| 7 | perf gate | not started | unchanged — no `perf-gate` job, no baseline JSON |
| 8 | C4 cheap wins | 4 of 5 | **5 of 5**, plus the miri lane wired into CI |
| 9 | C2 fork 2PC failpoints | **not started** | **done** — 17 production seams, up from 11 |
| 10 | C2 compaction + abort | **not started** | **half done** — abort harness shipped; compaction untouched |
| 11–13 | B2 / C3 / B3 / B4 | not started | unchanged, confirmed absent |

### What landed since the documents were last updated

- **Phase 9 complete.** 7 new `fail_point!` seams across the fork 2PC windows —
  3 in `api/fork.rs` (create), 4 in `api/fork_admin.rs` (drop, including one
  *inside* the branch-delete loop as the plan required), 1 in
  `fork/registry.rs`. It found a product defect: fork artifacts were being
  deleted **before** the tombstone that anchors recovery (`99791f4a8`).
- **The abort harness shipped** (`crates/uni/tests/common/crash_harness.rs`,
  `#![cfg(all(unix, feature = "failpoints"))]`). 18 parent abort tests across 6
  suites, and **all four `ssi_resilience.rs` crash tests run under it** — the
  graceful-drop confound in §4.4 of the proposal is closed. The panic+`Drop`
  tests are deliberately kept alongside, since graceful-close is its own path.
- **The failpoint suite had no CI job at all** (`6dc168236`). It compiled and
  never ran, gated behind a `failpoints` feature no workflow passed. A second
  step now asserts the suite is *inert* without the feature, so the same
  silent-zero cannot recur.
- **C4 closed.** `test_stateful_crud.py` — `GraphMachine(RuleBasedStateMachine)`,
  10 rules, 3 invariants — found **two product bugs**: #182 (a delete before the
  first flush resurrected by the next flush) and #181 (flush resurrecting a
  detached edge), both fixed with Rust twins.
- **A seventh DQP lever**, and the first that changes *data* rather than
  physical state. Its law weakens from `bag_eq` to `bag_is_subset`, and its
  witness reads the bag rather than a counter, because a probe measured zero
  counter movement across a delete. It drove three storage fixes around
  tombstone-vs-version ranking (`7c2f1286a`, `474bac74a`, `a22bed1ba`).

---

## 2. The finding that reorders the plan: none of these gates have run

**Every job in all four workflows is guarded `if: github.repository ==
'rustic-ai/uni-db'`** — 10 of 10 in `pr.yml`, 9 of 9 in `ci.yml`, 9 of 9 in
`nightly.yml`, 2 of 2 in `perf-qualify.yml`. On `milliondreams/uni-db`, **zero
jobs execute.**

The merge landed on `md/main`. So 51 commits of test-harness work — the miri
lane, the failpoint crash suite, the supply-chain gate, the DQP smoke lane, the
Hypothesis state machine — are in the tree with **their CI having skipped
rather than passed**. The local runbook (`docs/local_ci_runbook.md`) is what
has validated them so far, which is real evidence but not the same evidence.

This is the same defect class the track has been finding all along — a gate that
reports success while doing nothing — one level up, in the delivery path rather
than in a test. It is listed first because it is cheap and because every B1 item
depends on it.

**Consequence for B1 specifically:** dispatching `perf-qualify.yml` from the
fork today would skip its `measure` job and produce an empty aggregate. The
cross-runner measurement cannot be taken until the work is on rustic-ai.

---

## 3. Track POA

Five tracks. T0 gates T1; T2–T4 are independent of each other and of T1.

### T0 — Land upstream and let the gates actually run — **DONE 2026-08-25**

Verified rather than assumed: all **19 PR + CI jobs ran `success`, none
skipped**. For a `pull_request` event `github.repository` is the *base* repo, so
the guards pass on a fork PR.

**One gap remains, and it is not cosmetic.** The nightly run that night started
~03:32, before the 04:01 merge, so it executed pre-merge `main` and contains
only 7 jobs. **`dqp` and `miri (uni-crdt)` have still never run in CI.** The
next scheduled nightly is their first execution — and also the first test of
whether 10 concurrent DQP soaks fit the 60-minute box, which so far rests on a
local 22-core measurement covering only 8 of them. **Watch that run; it feeds
T2 directly.**

#### Original task list, for the record

1. Push the 51 commits to `rustic-ai/uni-db` (PR or fast-forward, per the
   repo's convention — **needs explicit approval; not done unilaterally**).
2. Confirm each guarded job executes rather than skips. The failure mode to
   watch for is a job reporting green *because it skipped*, which is
   indistinguishable from success in the checks UI.
3. Triage anything the first real run reddens. The local runbook's 16 green
   jobs make this unlikely but not impossible — CI's stable Rust floats, which
   has bitten this repo before (PR #146).

**Exit:** a run on rustic-ai with every job non-skipped, and its result recorded.

### T1 — Close B1, the last open P0 — **DONE 2026-08-25**

All six steps complete. Measured, then built from the measurement:

- Run [32810181500](https://github.com/rustic-ai/uni-db/actions/runs/32810181500),
  5 runners x 5 runs. **Cross-runner CV 0.07-0.20%**, *lower* than within-runner
  CV (0.13-0.46%) on every target — the machine-to-machine spread this leg
  existed to worry about is smaller than the fixed-machine spread.
- **Threshold derived, not adopted: 5 runs, fail 2%, warn 1%.** Computed
  exhaustively over every N-subset of the 25 samples, the worst drift of a
  median with no code change is 0.997% at 3 runs and 0.599% at 5. A 1% gate at
  3 runs would false-fire; the proposal's 5% would pass a real 4% regression.
- `docs/perf/iai-baseline.json` committed — 9 targets recorded, 5 gated.
- `perf-gate` job added to `pr.yml`; §4 and §5 of the qualification doc rewritten.
- Validated: all 5 real shards pass (worst +0.46%), +2.5% fails, +1.4% warns.

**Deviation, stated:** `hot_paths_iai.rs` keeps all seven pilot targets rather
than being trimmed to five. Trimming would delete the rejected targets' only
baseline and remove the `baselines::` calibration group, whose fixed 4 Ir is
what separates "no regression" from "collection is broken". Non-gated targets
cost measurement time and nothing else.

#### Original task list, for the record

1. **Dispatch `Perf Qualify (cross-runner iai)`** — 5 runners × 5 runs, ~27 min.
   `scripts/perf/iai_cross_runner.py` already emits the per-target cross-runner
   CV table.
2. **Replace §4 of `docs/perf/iai-qualification-2026-08-12.md`** with the
   measured table and a threshold derived from it. The current 5%/2% figures are
   the pilot's recommendation from *single-machine* variance (CV 0.21–0.96%);
   setting a gate from those is precisely the mistake §7.1 of the proposal
   exists to prevent.
3. **Trim `hot_paths_iai.rs` to the qualified set.** It still carries all 7
   pilot targets including the two rejected as IO-sensitive. Exclude the
   `baselines::` group from the headline figure.
4. **Commit `docs/perf/iai-baseline.json`** — the regeneration script and the
   gate are **done** (2026-08-24): `scripts/perf/iai_baseline.py` and
   `scripts/perf/iai_gate.py`, both reusing `iai_cross_runner.py`'s
   `load_runner` / `cv_pct` / `check_usable` rather than re-parsing callgrind.
   The JSON itself cannot be written until the measurement exists. Baseline
   moves are then reviewed in the PR diff.

   Two deliberate properties, both verified against synthetic fixtures:
   `--fail-pct` and `--warn-pct` are **required arguments with no defaults**,
   so no unmeasured threshold can be baked in; and every way the pair could
   pass while measuring nothing is a hard failure — all-zero counters (the
   `strip = "symbols"` trap), a gated target absent from the run, and a
   baseline generated from unusable samples, which refuses to write at all.
5. **Add the `perf-gate` job** to `pr.yml` with `save-if: false`. State the cost
   in that PR: ~15 min cold bench compile every run (the Swatinem cache's only
   saver is `ci.yml`'s `tck-full`, which never builds `--benches`), plus ~140 s
   per Valgrind pass.
6. **Verify with a deliberate-regression PR** — the criterion is that the gate
   *fires*, not that it exists.

**Exit:** `perf-gate` fails on a deliberate regression and passes on an
unrelated PR. Both P0s then closed.

**Stops the chain if:** cross-runner CV is high enough that no defensible
threshold exists. In that case B1 re-scopes to a post-merge trend report and the
decision is recorded — the pilot's own stop condition, applied one level later.

### T2 — Protect C1 from decaying *(independent, small)*

C1 is complete but three of its seven levers ship **smoke-only**: index,
compaction, and delete. The nightly `dqp` job runs 10 soaks in a 60-minute box,
and its 29.9-minute measurement covers only 8 of them — `flush_pushdown_soak` is
an extrapolation.

1. **Re-measure the nightly job as CI runs it**, before adding anything. The
   plan's own rule is that the response to approaching a ceiling is to cut
   volume, not raise it.
2. **Add soaks for the three smoke-only levers**, if and only if the
   re-measurement leaves room. If it does not, the honest outcome is to say so
   and cut `DQP_CASES` rather than to skip the soaks silently.
3. **Fix three stale claims in-tree**: `dqp/mod.rs:78` says "all six levers
   ship" (there are seven), the nightly step name says "four levers", and the
   delete lever's smoke-only status is undocumented.

**Exit:** every lever has a soak or a written reason it does not, and the
nightly job's runtime is a measurement rather than an extrapolation.

### T3 — Close C2 — **Phases 1-3 DONE 2026-08-25**; Lance tier + madsim deferred

The compaction path had zero fault injection. It now has three seams, and both
an in-process ordering suite and a real `SIGABRT` matrix over them.

**Phase 1 found and fixed a live data-loss defect** — semantic vertex
compaction erased `ext_id`, every schemaless property and both timestamps, and
recomputed `_uid` from the truncated map. Reachable from `VACUUM`. Tests were
observed RED first, and the revert patch is registered in the teeth ledger and
verified to bite.

**Phase 2 seams**, all inert without the `failpoints` feature:

| seam | window |
|---|---|
| `compaction::after-adj-replace-before-delta-clear` | L2 merged, the deltas that produced it not yet cleared |
| `compaction::between-fwd-and-bwd` | one direction merged and cleared, the other untouched |
| `compaction::after-vertex-replace` | per-label table replaced, `main_vertices` still holds the tombstones |

**What the matrix established** — all three properties were previously assumed:

- The adjacency redo **is** idempotent, including when the same endpoints are
  re-connected between the crash and the redo. Per-op idempotence of
  `apply_deltas_to_edges` turns out to be sufficient; that is now asserted
  rather than argued.
- A mid-`compact_all` crash leaves **both directions agreeing**, because the
  uncompacted direction still resolves through its intact L1 overlay, and the
  next pass converges them.
- A crash after the vertex replace does **not** resurrect the deleted vertex
  from the surviving `main_vertices` row, and survivors keep their schemaless
  properties.

**Phase 3**: the `:265` pin and the multi-src-label probe both green — an edge
type with two `src_labels` compacts the same tables twice, and the second pass
does not clear what the first merged.

Every crash assertion is guarded by a denominator that was proven to
discriminate: the in-process helper fails if the seam is never reached, and the
abort matrix fails on both sides (`did not abort: status Some(101), signal
None` in the parent, `the seam was never reached` in the child). Both were
verified by deliberately pointing a test at a non-existent seam.

**S4 and S5 landed 2026-08-25**, taking the path to **five seams**:
`compaction::between-labels` (per-label skew) and
`compaction::after-compact-files-before-cleanup` (Lance committed a fragment
rewrite, indices not yet re-optimized — the only seam reachable from the public
`compact()` API). Both recover clean: index-backed lookups agree with a full
scan for all 60 rows and compaction reaches a fixpoint; both label anchors agree
and converge.

**S4 found a defect, and the crash turned out to be irrelevant to it.** A
label-anchored `DETACH DELETE` of a multi-label vertex is undone by the next
flush for the vertex's *other* labels — measured before any compaction runs. The
same delete is correct with no flush, and correct when unanchored or fully
anchored, which localizes it to the flush writing the tombstone into only the
matched label's per-label table. Pinned by
`storage::multi_label_delete`, whose correct-behaviour case is `#[ignore]`d with
the reason so it neither reddens CI nor encodes the bug as intended. **Not fixed
here** — it is a write-path defect, unrelated to compaction, and wants its own
change.

**Still open:** the madsim spike, split out per the scope decision and
re-scoped to the background compaction loop; and the multi-label delete defect
above.

#### Original task list, for the record

Phase 10's remaining half. The dependency the plan flagged is now **resolved**:
`CompactionStats` reports real numbers since #172, so the semantic-compaction
matrix has something true to assert against.

1. **Semantic-compaction failpoints** (uni-owned steps in
   `uni-store/src/storage/manager.rs`): assert no tombstone resurrection and no
   row loss across a `max_l1_runs` merge. Currently **zero** failpoints exist in
   any compaction path.
2. **Lance-optimization matrix**: treat `optimize_table` as atomic and crash
   *around* it — before, and after `compact_files` but before the cleanup pass.
   Finer granularity needs upstream instrumentation; record that as a known
   limitation rather than attempt it.
3. Run both under the existing abort harness, which is already built.
4. **madsim spike**, time-boxed, on the uni-owned WAL + L0 + flush-coordinator
   path only. Deliverable is an adopt/partial/reject decision **with evidence**,
   not an adoption.

Note a live lead: compaction runs one flush behind (`compact#1: removed=0` then
`compact#2: removed=2 added=1`, reproduced every round). Recorded in Phase 4B as
a separate defect and never chased.

**Exit:** both matrices green, upstream limitation documented, spike report filed.

### T4 — Honesty sweep *(independent, cheap, ~half a day total)*

Four instances of the track's own theme, found while verifying this document:

1. **The Hypothesis `nightly` profile is dead code.** `test_stateful_crud.py`
   registers `pr` (25 × 12) and `nightly` (500 × 50), selected by
   `UNI_HYPOTHESIS_PROFILE` — **which is set in no workflow**. Only the `pr`
   profile ever runs. Either wire the nightly profile into a job or delete it.
2. **`deny.toml`'s `fxhash` ignore has no expiry.** It is a bare
   `TODO: migrate to rustc-hash` and will never self-expire, unlike the
   blocker-named upstream ignores. `fxhash` is a *direct* workspace dependency.
3. **`docs/perf/` and `docs/testing/` have no index** — 4 and 2 documents plus a
   revert directory, with nothing tying them together. Phase 13's consolidation,
   worth doing now that there is something to consolidate.
4. **Refresh the two proposal documents' status blocks** so they stop
   understating the tree by two phases.

### T5 — Track E, in value order *(not yet scheduled)*

Sequenced last and deliberately unstarted. In ascending cost:

- **B4 contention curves** — cheapest. `ssi_contention.rs` is still in its
  pre-plan shape (106 lines, fixed sweep, wall-time only, no abort-rate
  instrumentation). Formalizing it as throughput-and-abort-rate vs Zipf θ is a
  contained change to an existing bench.
- **B3 ann-benchmarks + BEIR** — the benchmark that can show RRF fusion is not
  earning its complexity. Publish regardless of outcome.
- **B2 LDBC SNB** — largest, but doubles as a correctness benchmark via LDBC's
  reference answers.
- **C3 Elle** — sequenced last on the proposal's own reasoning: it
  *demonstrates* a property there is reason to believe holds, whereas C1 and C2
  hunt bugs there is reason to believe exist.

---

## 4. Recommended order

```
T0  land upstream, gates run          ── prerequisite, hours
T1  close B1 (last P0)                ── depends on T0
T2  DQP soak parity + re-measure      ── independent, small
T4  honesty sweep                     ── independent, cheap
T3  close C2 (compaction + madsim)    ── independent, medium
T5  Track E: B4 → B3 → B2 → C3        ── unscheduled
```

T0 first because it is hours of work that converts 51 commits of unexercised CI
into exercised CI, and because T1 cannot start without it. T1 next because it is
the only remaining P0 and the work after the measurement is mechanical. T2 and
T4 can be slotted against any slack — both are small and both protect claims
that are currently drifting toward false.

---

## 5. What this does not propose

- **Reopening C1.** It is complete; T2 protects it, it does not extend it.
  Tier-3 levers stay deferred on observability (measured: observable = 0,
  inert = 6) until a counter describing *how* rows were produced exists.
- **Relaxing the repo guards.** They are the standard fork-protection pattern
  and are correct; the fix is landing the work upstream, not weakening the
  guard.
- **Gating on a metric the cross-runner measurement rejects.** T1 has an
  explicit stop condition for exactly that.
