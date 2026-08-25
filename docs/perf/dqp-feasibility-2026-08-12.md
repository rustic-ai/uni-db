# DQP Feasibility Measurement — Phase 0A

**Date:** 2026-08-12
**Phase:** 0A of `docs/proposals/test_harness_implementation_plan_2026-08-12.md`
**Status:** complete — **the proposal's tier table is refuted and replaced below**

Regenerate with:

```bash
cargo nextest run --profile soak -p uni-db --test integration \
  --run-ignored ignored-only -E 'test(/metamorphic::dqp::feasibility/)' --no-capture
```

Raw output is written to `target/dqp-feasibility/{tiers,runtime-flavor,witness-audit}.md`.

## Measurement conditions

| | |
|---|---|
| Machine | Intel Core Ultra 9 185H, 22 logical cores, Linux 7.1.8 |
| Profile | `test` (debug, unoptimized) — same profile CI uses for `cargo nextest run` |
| Cases per tier | 100, identical across tiers (deterministic `TestRunner`) |
| Query | `Case::base_query()` from the existing `querygen::arb_case()` |
| **Caveat** | the box carried a concurrent unrelated `cargo` build during the large tier; load average reached 29. **Large-tier numbers are pessimistic and should be re-measured on an idle box before they are treated as tight.** |

---

## 1. Tier cost

| tier | persons | edges | build | p50 | p95 | max | rows p50 | rows p95 | rows max | total rows / 100 cases |
|---|---|---|---|---|---|---|---|---|---|---|
| tiny | 1 000 | 4 000 | **0.163 s** | 31.66 ms | 152.40 ms | 218.05 ms | 1 000 | 4 000 | 4 000 | 195 906 |
| smoke | 10 000 | 40 000 | **0.788 s** | 248.45 ms | 2.012 s | 2.344 s | 10 000 | 40 000 | 40 000 | 1 951 651 |
| large | 50 000 | 200 000 | **621.73 s** | 1.247 s | 7.113 s | 10.093 s | 50 000 | 200 000 | 200 000 | 9 755 939 |

Zero query failures at every tier, so the generator and the fixture schema agree.

### 1.1 The large tier's build cost is the headline

**621.73 s — 10.4 minutes — against 0.788 s for smoke.** That is a **789× increase
in build time for a 5× increase in data**, and it is the single most consequential
number in this phase.

> **Candidate finding, unverified:** growth that far above linear suggests
> quadratic behaviour somewhere in the bulk-insert or flush path — plausibly
> per-batch endpoint validation scanning the accumulated vertex set, since the
> large tier runs 10 vertex batches and 20 edge batches against smoke's 2 and 4.
> This is a *hypothesis*, not a finding: it has not been isolated, and the
> concurrent build load on the box is a confound. It is recorded here because it
> is worth a dedicated investigation on the performance track, independently of
> DQP.

### 1.2 Rows returned confirm the full-scan analysis

`rows p50` equals the **fixture's vertex count exactly** at every tier, and
`rows p95` equals the **edge count exactly**. That is the predicted consequence of
`arb_base_where`'s `2 => None, 1 => Some(pred)` weighting
(`querygen/mod.rs:565-570`): two thirds of cases carry no `WHERE` and scan the
fixture end to end, and the ones that do carry a filter are dominated by the edge
shape.

So cost is essentially linear in *rows materialized into a `RowBag`*, and case
count and fixture size trade off directly against one another. **The selectivity
floor for the large tier (implementation plan §3.5.1) is mandatory, not
optional.**

---

## 2. Revised tier table — replaces the proposal's

Sized from p50 (the mean proxy) × 2 sides. The proposal's row is shown for
contrast.

| tier | proposed cases | **measured-viable cases** | lane | expected wall-clock |
|---|---|---|---|---|
| tiny | 50 000 | **500** | PR | ~32 s |
| tiny | 50 000 | **20 000** | nightly soak | ~21 min |
| smoke | 500 | **2 000** | nightly soak | ~17 min |
| large | ≤ 500 | **300** | nightly, **own job** | ~12.5 min + 10.4 min build ≈ 23 min |

Why each changed:

- **tiny @ 50 000 is not viable.** 50 000 × 2 × 31.66 ms = **53 min** at p50, and
  **4.2 hours** at p95. The nightly `metamorphic` job has a 60-minute timeout
  (`nightly.yml:264`) already carrying four concurrent soaks whose heaviest takes
  ~25 min. 20 000 cases fits; 50 000 does not.
- **smoke moves up, not down.** At 248 ms/case it comfortably carries 2 000 cases
  in a nightly lane — more than the proposal's 500. The PR gate stays on tiny.
- **large moves down to 300 and needs its own job.** Its 10.4-minute build cannot
  share a lane with soaks that must also finish inside 60 minutes.

### 2.1 New constraint: `drive_stateful` must never use the large tier

This was not anticipated by the proposal and follows directly from §1.1.

`drive_stateful` (implementation plan §3.4.3) rebuilds the fixture **once per
batch** of `k` cases. At `k = 500` over 50 000 cases that is 100 rebuilds; at
621.73 s each, **17.3 hours of pure fixture construction** before a single
comparison runs.

Therefore:

- **Tier-1 levers** (`drive_stateful`: flush, index create, plan cache,
  compaction) run on **tiny and smoke only**.
- **The large tier is Tier-2 only** (`drive_prepared`), where the fixture is built
  exactly once per run and both sides read it concurrently.

---

## 3. Row budget ceilings

Measured rows per case: tiny **1 959**, smoke **19 517**, large **97 559**.

Ceilings below are measured-total × ~1.5, so ordinary variation passes and a
generator drift toward unfiltered scans trips the budget loudly.

| lane | tier | cases | measured rows (2 sides) | **ceiling** |
|---|---|---|---|---|
| PR | tiny | 500 | 1 959 000 | **3 000 000** |
| nightly soak | tiny | 20 000 | 78 360 000 | **120 000 000** |
| nightly soak | smoke | 2 000 | 78 068 000 | **120 000 000** |
| nightly large | large | 300 | 58 535 400 | **90 000 000** |

Per-case ceiling, as a secondary guard that localizes the offending case: the
measured max, which is the fixture's edge count — **4 000 / 40 000 / 200 000**.

The budget is enforced over **rows returned**, not rows scanned. The proposal
specified it over `rows_scanned`, which §4 shows is a field that always reads
zero — a budget over it could never fire.

---

## 4. Witness observability audit

| field | L1 only | repeat | L0 over L1 | verdict |
|---|---|---|---|---|
| `rows_returned` | 371 | 371 | 435 | **observable** |
| `rows_scanned` | 0 | 0 | 0 | **always zero** |
| `bytes_read` | 0 | 0 | 0 | **always zero** |
| `l0_reads` | 0 | 0 | 0 | **always zero** |
| `storage_reads` | 0 | 0 | 0 | **always zero** |
| `cache_hits` | 0 | 0 | 0 | **always zero** |
| `plan_cache_hit` | false | false | false | **never warm on the read path** |
| `DatabaseMetrics::l1_run_count` | 1 | — | — | **observable** |

Five of `QueryMetrics`' seven counters are declared-but-unpopulated
(`uni-query/src/types.rs:36-47`, each documented "0 until … instrumentation").

**A field that exists and always returns zero is more dangerous than a missing
one.** A witness written `l0_reads > 0` compiles, runs, and silently never
activates; written `l0_reads == 0` on side B it passes **vacuously on both
sides** — the exact failure DQP exists to catch, reproduced inside DQP's own
non-vacuity check.

### 4.1 `plan_cache_hit` is a write-path field

`plan_cache_hit` has one assignment site: `execute_internal_with_tx_l0`
(`api/impl_query.rs:808`, surfaced at :920) — the **transaction/write** path.
`Session::query` never sets it, so on the read path it is permanently `false`.
The existing coverage (`cypher_write/tx_plan_cache_test.rs`) is a write-path
test, which is why the gap was invisible.

The read-path plan cache is real; its hits are counted on
`SessionMetrics::plan_cache_hits` (`session.rs:1276`), a per-session cumulative
counter. **The plan-cache lever's witness must therefore be a delta on
`Session::metrics()` across the two sides, not a per-query flag.** This is pinned
by the non-ignored test
`plan_cache_hit_is_observable_only_via_session_metrics_on_the_read_path`.

### 4.2 Phase 1 work list

Instrumentation required before any dependent lever ships:

1. `l0_reads` — L0-vs-L1 lever witness.
2. `storage_reads` — L0-vs-L1 lever witness (the complementary side).
3. `rows_scanned` — useful for the index-present lever; not strictly required.
4. Branch-scan execution counter — **does not exist in any form**; pristine-fork
   lever witness.
5. Snapshot-path execution counter — **does not exist in any form**; pinned lever
   witness.

`bytes_read` and `cache_hits` are not required by any planned witness and can
stay unpopulated.

Already usable, no work needed: `rows_returned`, `DatabaseMetrics::l1_run_count`
(compaction witness), `SessionMetrics::plan_cache_hits` (plan-cache witness).

---

## 5. Runtime flavor — the concern is refuted

| flavor | flush | fork | snapshot + pin |
|---|---|---|---|
| `current_thread` | ok | ok | ok |
| `multi_thread` | ok | ok | ok |

The implementation plan hypothesized that `metamorphic::drive`'s current-thread
runtime (`metamorphic/mod.rs:70-73`) might not be able to create a fork or a
snapshot, since both involve background tasks and the fork TTL suites require the
multi-thread flavor for the sweeper to progress.

**Not so.** All three operations complete on a current-thread runtime, each well
inside a 60-second timeout. A current-thread `block_on` drives spawned tasks on
the same runtime, which is sufficient here; the TTL suites' requirement is
specific to the *sweeper's* periodic timer, not to fork creation.

**Decision: the DQP drivers may use either flavor.** The measurement tests use
`multi_thread` because they are also timing fixture builds, but no lever is
blocked by this and `drive()`'s existing shape is reusable.

---

## 6. Incidental observation

During fixture teardown, a tokio worker panicked:

```
thread 'tokio-rt-worker' panicked at
lance-datafusion-7.0.0/src/utils.rs:59:10:
called `Result::unwrap()` on an `Err` value: JoinError::Cancelled(Id(1232))
```

The test still passed — it is a background task unwrapping a `JoinError` on
cancellation during drop. Not blocking for Phase 0A and **not investigated
here**, but recorded because an `unwrap` on a cancelled join in a dependency's
teardown path is the kind of thing that becomes a flaky-shutdown report later.

---

## 7. Stop-the-chain check

The Phase-0A gate asked: *if the large tier cannot be built and queried at 100
cases inside the nightly budget, say so and re-scope Tier-2 levers to the smoke
fixture.*

**It can** — 621.73 s build + ~125 s for 100 cases ≈ 12.5 min, inside a 60-minute
job. The large tier survives, at 300 cases in a dedicated job, and Tier-2 levers
proceed against it.

The chain continues to Phase 1, whose scope is now the concrete five-item list in
§4.2 rather than an open question.

---

## 8. VID determinism — measured 2026-08-13 (Phase 6)

**The question.** Is VID assignment deterministic given an identical insert
sequence? The plan makes Tier-3's design conditional on the answer and is
explicit that neither outcome should be designed around before measuring:
deterministic means a Tier-3 lever compares bags directly, nondeterministic
means every comparison must first strip identity via a
`Case::identity_free_projection`.

**The answer: deterministic**, on every axis tested.

| property | result |
|---|---|
| Two builds, one seed → identical `name → vid` mapping | ✅ |
| Mapping survives a `flush()` | ✅ |
| Mapping stable over 3 further repeats | ✅ |
| **Mapping survives a config change** (`batch_size` 64/4096, `parallelism=1`) | ✅ |
| VIDs genuinely distinct per row (1000/1000) | ✅ |

Tests: `metamorphic::dqp::vid_determinism`.

The comparison is over `name → vid` **pairs**, not the VID multiset: a fixture
whose VIDs were merely a permutation would still break a direct bag comparison,
because `id(p)` would pair with a different `p.name` on each side.

**The plan asked a narrower question than Tier 3 depends on.** A Tier-3 lever
never compares two identical builds — it compares two builds that differ by a
config knob. A fixture could be build-to-build deterministic while allocating
different VIDs at a different `batch_size`, so the fourth row above is the one
that actually licenses direct bag comparison. It was added after noticing the
gap, and it passes.

**Consequence:** `identity_free_projection` is **not needed and has not been
built**.

### 8.1 Tier-3 levers are deferred anyway — for a different reason

The VID answer said go. A separate constraint says stop, and it is the same one
that deferred the Phase 4B index and compaction levers: **no activation
witness**.

Measured across the six candidate knobs (`metamorphic::dqp::tier3_probe`):

| knob | witness | bags |
|---|---|---|
| `batch_size=64` | no counter moved | equal |
| `batch_size=8192` | no counter moved | equal |
| `parallelism=1` | no counter moved | equal |
| `partial_lance_writes` (flipped) | no counter moved | equal |
| `async_flush_enabled` (flipped) | no counter moved | equal |
| `auto_flush_threshold=1` | no counter moved | equal |

**observable = 0, inert = 6.**

A lever whose two sides move no counter cannot state what it exercised. Its
`activated` predicate would have nothing true to say, and the drivers' 80%
activation floor would fail every run — or, if written without a witness, it
would pass forever while comparing two identical execution paths. That is
precisely the vacuous test this oracle exists to prevent.

So Tier 3 ships **one** thing rather than a lever set:
`tier3_knobs_are_result_neutral`, which asserts the premise directly over three
fixed queries. Narrow, but a real check — a knob that changed results would be a
defect whether or not a counter noticed.

`probe_which_tier3_knobs_are_observable` is a tripwire: it **fails the moment any
knob becomes observable**, which is the signal to promote it to a lever with
`activated` written against the counter it moved.

**What would unblock the full Tier-3 set:** a counter that distinguishes *how*
rows were produced rather than *what* was produced — morsel count, partition
count, or a flush-path discriminator. That is the same missing observability
Phase 4B needs, and closing it once would unblock both.
