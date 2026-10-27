# `docs/perf/` — what has been measured

Every document here records a measurement. This index exists so that a number
can be traced to **when it was taken, on what hardware, and whether anything
gates on it** — because a figure without those three is not evidence, and
because the machine it came from turns out to matter more than it looks.

## The index

| document | measures | date | machine | gates? |
|---|---|---|---|---|
| [iai-baseline.json](iai-baseline.json) | reference instruction counts, 9 targets, **3** gated | 2026-09-17 | **CI** — 5 `ubuntu-xlarge` shards, 25 samples/target | **yes** — `perf-gate` in `pr.yml` |
| [iai-qualification-2026-08-12.md](iai-qualification-2026-08-12.md) | which hot paths *can* honestly be gated on instruction counts; 5 of 7 qualify | 2026-08-12 | Intel Core Ultra 9 185H, 22 cores, Linux 7.1.8 | no — a pilot, gates nothing |
| [ann-2026-08-25.md](ann-2026-08-25.md) | ANN recall@10 vs QPS on SIFT-1M, against SIFT's own ground truth | 2026-08-25 | Intel Core Ultra 9 185H, 22 cores, Linux 7.1.8 | no |
| [contention-2026-08-25.md](contention-2026-08-25.md) | SSI throughput **and abort rate** vs Zipf skew × writer count | 2026-08-25 | Intel Core Ultra 9 185H, 22 cores, Linux 7.1.8 | no |
| [dqp-feasibility-2026-08-12.md](dqp-feasibility-2026-08-12.md) | DQP fixture-tier cost; **refutes and replaces** the proposal's tier table | 2026-08-12 | Intel Core Ultra 9 185H, 22 cores, Linux 7.1.8 | no — sizes the nightly `dqp` lane |
| [coverage-map-2026-08-14.md](coverage-map-2026-08-14.md) | which live code has never executed under any test | 2026-08-14 | n/a — coverage, not timing | no, **deliberately** |
| [index-scan-counter-2026-08-27.md](index-scan-counter-2026-08-27.md) | what `idx_scans` counts — one scan path of ~40 operators, and why two thirds of the gap is not a wiring omission at all | 2026-08-27 | n/a — a wiring audit | no |
| [build-baseline-2026-05-19.md](build-baseline-2026-05-19.md) | post-consolidation build wall-time reference | 2026-05-19 | under-recorded — "see `git log`" | no |

## Reading these safely

**Regenerated 2026-09-17, and two targets were demoted.** The metric changed
definition: `hot_paths_iai.rs` bounded its measured region with Callgrind's
`--toggle-collect` entry point, whose collection state is per-thread, so work
Lance dispatches to tokio's blocking pool via `spawn_cpu` was never counted.
`vertex_lookup_by_id` attributed 7,895 Ir to a query costing 7.4M, and
`l0_to_l1_flush` was 57.6% invisible. The bench now gates **instrumentation**,
which is process-global, so every thread is counted.

That also retired most of #230: the local-vs-CI gap was largely this bug, and
the same box that measured 88-97% below the old baseline now lands within
1.0-2.3x.

The honest metric is **noisier** on two targets, because the old one was stable
by virtue of being blind — it was largely measuring fixture teardown, which is
near-identical across targets. `parse_and_plan_cold` (cross-runner CV 1.60%) and
`property_read_across_l0_l1` (2.25%, and bimodal across runner populations with
a ~6.5% split) both fail the pilot's CV < 1.0% rule and are recorded
`gated: false` with their own reasons. Gating a target the runner fleet cannot
measure to better than the threshold is how a gate gets ignored. Three targets
remain gated, and every one of the five shards passes against the pooled
baseline with worst drift 0.92%.

Re-qualifying either demoted target means finding the variance source first —
their off-thread share is ~0.2%, so it is main-thread work, and per-process
`HashMap` iteration order is an untested hypothesis, not a finding.

**Only one number in this directory gates anything.** `iai-baseline.json` backs
the `perf-gate` job; everything else is a characterization. That is deliberate,
and `docs/proposals/test_harness_and_benchmarks_2026-08-11.md` §7.1 has the
argument: wall-clock in CI cannot carry a threshold, so instruction counts are
the only thing gated, and only on targets that passed a qualification pilot.

The vertex-ID lookup benchmark also checks that it returns exactly one row with
the selected fixture vertex's name. These assertions run outside the Callgrind
measurement window: an empty or incorrect result must fail the benchmark rather
than masquerade as cheaper query execution.

**An implausible improvement now fails the gate (#230).** A local run of
*unchanged* code measured 25–56% below the CI-generated `iai-baseline.json` and
`iai_gate.py` reported "all 5 gated targets within 2.0%", because every
comparison was one-sided. A later run reached 88–97% below baseline and still
printed `worst +0.00%`. Both directions are checked now:

- `--fail-improve-pct` is required and must exceed `--fail-pct`. CI passes 50,
  which is far above the measured same-runner drift (0.599%) and far below the
  observed collapse, so it cannot fire on noise. A genuine optimization past 50%
  on a gated hot path stops for a human on purpose.
- The summary reports `worst` **and** `best`, and states which checks ran, so a
  green line cannot imply a check that did not happen.
- **Running locally against the committed baseline?** Pass
  `--allow-foreign-machine`, which downgrades the collapse check to a warning.
  CI never passes it. The arming is deliberately *not* inferred from machine
  identity: `generated_from.runners` holds the qualify workflow's shard artifact
  names, which no machine can match, so a check gated on that would have been
  permanently and silently off.
- `scripts/perf/test_iai_gate.py` pins all of the above and runs as a step of
  the perf job itself, because no lane runs tests under `scripts/`.

- **Never regenerate `iai-baseline.json` locally.** Dispatch
  `Perf Qualify (cross-runner iai)` and rebuild from its artifacts. Writing local
  numbers into the committed baseline would poison the gate far worse than the
  regression it exists to catch.

**A measurement that refuted its own design is the document succeeding.**
`dqp-feasibility` exists because the proposal committed to a tier table nobody
had measured; the document replaces it. `iai-qualification` rejected 2 of its 7
candidate targets as IO-sensitive. Neither is a failure report.

**The oldest entry is the weakest.** `build-baseline-2026-05-19.md` records no
usable machine identity and states outright that its pre-change baseline was
never captured, so it carries no before/after delta. Treat it as a historical
reference point, not a comparison.

## Related

- `docs/testing/` — how the suite is known to catch things, rather than how fast
  it runs.
- `docs/proposals/test_harness_and_benchmarks_2026-08-11.md` — the strategy these
  measurements serve.
- `docs/proposals/test_harness_track_e_poa_2026-08-25.md` — the current plan of
  action and what remains.
