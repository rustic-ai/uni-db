# `docs/perf/` — what has been measured

Every document here records a measurement. This index exists so that a number
can be traced to **when it was taken, on what hardware, and whether anything
gates on it** — because a figure without those three is not evidence, and
because the machine it came from turns out to matter more than it looks.

## The index

| document | measures | date | machine | gates? |
|---|---|---|---|---|
| [iai-baseline.json](iai-baseline.json) | reference instruction counts, 9 targets, 5 gated | 2026-08-25 | **CI** — 5 `ubuntu-xlarge` shards, 25 samples/target | **yes** — `perf-gate` in `pr.yml` |
| [iai-qualification-2026-08-12.md](iai-qualification-2026-08-12.md) | which hot paths *can* honestly be gated on instruction counts; 5 of 7 qualify | 2026-08-12 | Intel Core Ultra 9 185H, 22 cores, Linux 7.1.8 | no — a pilot, gates nothing |
| [ann-2026-08-25.md](ann-2026-08-25.md) | ANN recall@10 vs QPS on SIFT-1M, against SIFT's own ground truth | 2026-08-25 | Intel Core Ultra 9 185H, 22 cores, Linux 7.1.8 | no |
| [contention-2026-08-25.md](contention-2026-08-25.md) | SSI throughput **and abort rate** vs Zipf skew × writer count | 2026-08-25 | Intel Core Ultra 9 185H, 22 cores, Linux 7.1.8 | no |
| [dqp-feasibility-2026-08-12.md](dqp-feasibility-2026-08-12.md) | DQP fixture-tier cost; **refutes and replaces** the proposal's tier table | 2026-08-12 | Intel Core Ultra 9 185H, 22 cores, Linux 7.1.8 | no — sizes the nightly `dqp` lane |
| [coverage-map-2026-08-14.md](coverage-map-2026-08-14.md) | which live code has never executed under any test | 2026-08-14 | n/a — coverage, not timing | no, **deliberately** |
| [index-scan-counter-2026-08-27.md](index-scan-counter-2026-08-27.md) | what `idx_scans` counts — one scan path of ~40 operators, and why two thirds of the gap is not a wiring omission at all | 2026-08-27 | n/a — a wiring audit | no |
| [build-baseline-2026-05-19.md](build-baseline-2026-05-19.md) | post-consolidation build wall-time reference | 2026-05-19 | under-recorded — "see `git log`" | no |

## Reading these safely

**Only one number in this directory gates anything.** `iai-baseline.json` backs
the `perf-gate` job; everything else is a characterization. That is deliberate,
and `docs/proposals/test_harness_and_benchmarks_2026-08-11.md` §7.1 has the
argument: wall-clock in CI cannot carry a threshold, so instruction counts are
the only thing gated, and only on targets that passed a qualification pilot.

**Machine identity is load-bearing, and one tool is blind to it.** A local run of
*unchanged* code measured 25–56% below the CI-generated `iai-baseline.json`, and
`iai_gate.py` reported "all 5 gated targets within 2.0%" — because it only fails
on regressions, so an implausible improvement passes silently. Two consequences:

- **Never regenerate `iai-baseline.json` locally.** Dispatch
  `Perf Qualify (cross-runner iai)` and rebuild from its artifacts. Writing local
  numbers into the committed baseline would poison the gate far worse than the
  regression it exists to catch.
- Recording the machine in the baseline and warning on a mismatch is an open
  follow-up, along with treating a large negative delta as suspicious rather than
  free.

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
