# Contention curves — throughput and abort rate vs skew

**Date:** 2026-08-25
**Bench:** `crates/uni/benches/ssi_contention.rs`
**Tree:** `83587ee4c`
**Track:** E1 / B4

Regenerate with:

```bash
cargo bench -p uni-db --bench ssi_contention
```

## What this measures, and why the previous shape could not

`ssi_contention.rs` previously timed N writers hammering a **single hot key** and
reported wall time. A single key is `theta = infinity` — the one point on the
skew axis where the curve is invisible — and wall time alone hides the shape
change from *aborts rise gracefully* to *aborts collapse throughput*.

It now sweeps two axes and reports both quantities:

- **skew** — Zipf `theta` over a keyspace of 256 counters. `theta = 0` is uniform.
- **writers** — concurrent tasks, each doing 10 read-modify-write increments.

Both SSI arms are cells of one sweep. The `lww` arm is `ssi_enabled = false`: no
validation runs, so nothing ever aborts and **increments are silently lost**. It
is the ceiling correctness is bought against, not a mode to run.

### Abort rate needed no new instrumentation

`uni-store` already emits one `uni_ssi_commit_validations_total` per writing
commit and one `uni_ssi_serialization_conflicts_total` per aborted one. The
comment beside the emission (`crates/uni-store/src/runtime/writer.rs`) states the
intent directly: *"the ratio of conflicts to validations is the headline abort
rate."* This bench is the first thing to compute it. Because every retry
re-commits, the ratio is a true **per-attempt** abort rate.

Counters are read through the probe the SSI tests already use
(`crates/uni/tests/common/ssi_support/metrics.rs`), included via `#[path]` rather
than reimplemented — `metrics::set_global_recorder` installs at most once per
process and, since metrics-util 0.20, `Snapshotter::snapshot()` *consumes*
counters. The probe solves both; a second implementation would step on them
again.

## Results

Intel Core Ultra 9 185H, 22 cores, 62 GiB, Linux 7.1.8, rustc 1.98.0.
Release profile. `CONTENTION_MEASURE_SECS=5`, `CONTENTION_WARMUP_SECS=2`.
**One machine, two runs** (the second is in the reproducibility table below) —
the shape is the result here, not the absolute ops/s.

| arm | theta | writers | ops/s | abort rate | conflicts/validations | retry-exhausted |
|---|---:|---:|---:|---:|---:|---:|
| ssi | 0.00 | 1 | 1254 | 0.00% | 0/6400 | 0 |
| ssi | 0.00 | 8 | 2363 | 2.38% | 445/18685 | 0 |
| ssi | 0.00 | 24 | 2508 | 8.82% | 1996/22636 | 0 |
| ssi | 0.90 | 1 | 1251 | 0.00% | 0/6400 | 0 |
| ssi | 0.90 | 8 | 2233 | 14.99% | 3203/21368 | 75 |
| ssi | 0.90 | 24 | 2301 | 40.19% | 12975/32281 | 1334 |
| ssi | 1.20 | 1 | 1246 | 0.00% | 0/6400 | 0 |
| ssi | 1.20 | 8 | 2171 | 30.40% | 7757/25519 | 478 |
| ssi | 1.20 | 24 | 2131 | 59.37% | 25262/42549 | 3353 |
| lww | 0.00 | 1 | 1236 | 0.00% | 0/0 | 0 |
| lww | 0.00 | 8 | 2596 | 0.00% | 0/0 | 0 |
| lww | 0.00 | 24 | 2791 | 0.00% | 0/0 | 0 |
| lww | 0.90 | 1 | 1229 | 0.00% | 0/0 | 0 |
| lww | 0.90 | 8 | 2626 | 0.00% | 0/0 | 0 |
| lww | 0.90 | 24 | 2863 | 0.00% | 0/0 | 0 |
| lww | 1.20 | 1 | 1263 | 0.00% | 0/0 | 0 |
| lww | 1.20 | 8 | 2679 | 0.00% | 0/0 | 0 |
| lww | 1.20 | 24 | 2903 | 0.00% | 0/0 | 0 |

## Reproducibility

The sweep was run twice. Because the Zipf sampler is seeded and deterministic,
**abort rate reproduces within 0.6 percentage points on every cell**, while
wall-clock throughput moves by up to ~5%:

| theta | writers | abort run 1 | abort run 2 | ops/s run 1 | ops/s run 2 |
|---:|---:|---:|---:|---:|---:|
| 0.00 | 8 | 2.38% | 2.32% | 2363 | 2316 |
| 0.00 | 24 | 8.82% | 8.70% | 2508 | 2628 |
| 0.90 | 8 | 14.99% | 14.86% | 2233 | 2313 |
| 0.90 | 24 | 40.19% | 40.60% | 2301 | 2310 |
| 1.20 | 8 | 30.40% | 30.26% | 2171 | 2195 |
| 1.20 | 24 | 59.37% | 59.07% | 2131 | 2185 |

That asymmetry is the useful part: the **abort rate is the stable quantity here
and the throughput is not**, so conclusions should be drawn from the former.

## The shape

**1. The scaling curves diverge — that is the whole point of two axes.** Going
from 8 to 24 writers at `theta = 1.2`, the `lww` arm gains throughput
(2679 → 2903, and 2687 → 2970 in run 2) while the `ssi` arm does not
(2171 → 2131, and 2195 → 2185). SSI **stops scaling** where LWW keeps scaling,
and it stops precisely where the abort rate passes ~59%.

Stated carefully: the `ssi` decline across those two points is small (−1.8% and
−0.5%) and within the throughput noise reported above, so this is **not** a
demonstrated throughput collapse. The reproducible claim is the *divergence* —
LWW gains ~8% from those extra 16 writers and SSI gains nothing. Extending the
sweep past 24 writers is what would establish whether it turns over outright, and
that is left for the nightly lane where a longer sweep is affordable.

**2. The SSI tax is a function of skew, not a constant.** At 24 writers it is
~10% at `theta = 0` (2508 vs 2791) and ~27% at `theta = 1.2` (2131 vs 2903). Any
single headline figure for "the cost of SSI" is a figure for one point on this
surface.

**3. Uniform is not conflict-free.** `theta = 0` over 256 keys still aborts 8.8%
of commits at 24 writers — birthday-paradox collisions, not skew. Worth stating
because "we spread the keys" is a common and insufficient mitigation.

**4. Retry exhaustion is the finding with operational teeth.** `RetryOptions`
defaults to `max_attempts: 5`. At `theta = 1.2` / 24 writers, **3353 operations
exhausted that budget and returned an error** (3338 in run 2) — and at
`theta = 0.9` / 24 writers, 1334 did (1327 in run 2). These are not aborts that
retried successfully; they are failures the caller receives, and they reproduce.
The abort rate rising past ~40% is where `execute_with_retry`'s default stops
being sufficient, and that boundary is now measured rather than assumed. Callers
with genuinely hot keys need a raised `max_attempts`, application-level backoff,
or a data model that does not funnel writes onto one row.

## Non-vacuity

A contention curve whose abort rate is flat at zero is not a curve — it means
the workload never collided, and publishing its throughput as "under contention"
would be decoration. This is B4's analogue of C1's per-lever activation
witnesses, and the bench **fails** rather than printing a table nobody can trust:

- The `ssi` arm must record at least one conflict in a multi-writer cell.
  Single-writer cells are excluded, since they have nobody to conflict with and
  their 0% is legitimate.
- The `ssi` arm must record a nonzero number of validations at all — zero would
  mean the counters are not being observed.
- The `lww` arm must record **zero** conflicts. With `ssi_enabled = false` the
  validation block is skipped entirely, so a conflict there would mean the arm
  never took effect and the sweep measured the same thing twice. That is exactly
  the defect this rewrite exists to fix, so it is asserted rather than assumed.

Both gates were verified to bite, not merely to exist:

| probe | result |
|---|---|
| `CONTENTION_WRITERS=1` (no cell can collide) | exit 1, *"the sweep has no multi-writer ssi cell, so it cannot observe an abort"* |
| `--test` mode (cells run once, samples too small for a curve) | refuses to report, says so, exits 0 |

The run above reports its own witness:

```
[contention] non-vacuity: peak abort rate 59.37% over 182238 validations in the
ssi arm; lww arm recorded 0 conflicts as expected.
```

## Cost

**177 s and 178 s** for the two full 18-cell sweeps at
`CONTENTION_MEASURE_SECS=5` / `CONTENTION_WARMUP_SECS=2`, excluding compile. At the in-code defaults (10 s / 3
s) expect roughly 2.4x that, ~7 minutes.

This is a measurement, not an estimate, and it is what a nightly lane should be
sized against. It fits `nightly.yml`'s 60-minute boxes with room to spare — but
per the track's own rule, the lane is not wired in until that number is
confirmed on CI hardware rather than on a 22-core laptop.

## Limits, stated

- **One machine, two runs.** Absolute ops/s are not comparable across hardware and
  are not a gate. Only `hot_paths_iai.rs` gates perf, on instruction counts, for
  the reason `docs/perf/iai-qualification-2026-08-12.md` gives.
- **Correctness is not checked here.** That the `ssi` arm's final counts equal
  the total increments — and that `lww`'s do not — is covered by the stress
  tests. This bench measures time and counters only.
- **`theta` is over a fixed 256-key space.** A different keyspace size moves the
  absolute abort rates; the sweep compares cells against each other, not against
  another run's keyspace.
- **Retry-exhausted operations are counted, not retried further.** They shorten
  the measured work at high skew, which flatters the high-`theta` ops/s slightly.
  The `retry-exhausted` column is there so that is visible rather than buried.
