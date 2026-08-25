# iai-callgrind Qualification Pilot — Phase 0B

**Date:** 2026-08-12
**Phase:** 0B of `docs/proposals/test_harness_implementation_plan_2026-08-12.md`
**Status:** complete — **5 of 7 targets qualify; 2 rejected as IO-sensitive**

This is a pilot, not a gate. Its job is to decide which hot paths *can* honestly
be gated on instruction counts. Nothing here gates anything.

Regenerate:

```bash
# instruction counts, 20 runs
bash scripts/perf/iai_pilot.sh 20

# wall-clock companion (release, so both halves measure the same code)
cargo nextest run --release --profile soak -p uni-db --test integration \
  --run-ignored ignored-only -E 'test(iai_wallclock)' --no-capture
```

## Conditions

| | |
|---|---|
| Machine | Intel Core Ultra 9 185H, 22 logical cores, Linux 7.1.8 |
| valgrind | 3.27.1 |
| iai-callgrind | 0.16.1 (library and runner pinned to the same version) |
| Instruction counts | `bench` profile (release opt + `debug = 1`), 20 runs |
| Wall-clock | `--release`, median of 20 reps per target |
| Runtime | tokio **current-thread** on both halves |

---

## 1. Verdict

| target | mean Ir | CV % | wall tmpfs (ms) | wall disk (ms) | ΔIr % | **Δwall %** | Gi/s | verdict |
|---|---|---|---|---|---|---|---|---|
| `parse_and_plan_cold` | 8 321 222 | 0.21 | 2.98 | 2.90 | −0.33 | **−2.7** | 2.86 | **qualify** |
| `expand_batch_one_hop_warm` | 38 703 115 | 0.23 | 6.86 | 6.97 | −0.17 | **+1.5** | 5.55 | **qualify** |
| `property_read_across_l0_l1` | 11 534 810 | 0.21 | 3.05 | 3.32 | +0.18 | **+9.1** | 3.48 | **qualify** |
| `hnsw_top10_search` | 40 707 780 | 0.26 | 8.35 | 9.37 | −0.21 | **+12.3** | 4.34 | **qualify** |
| `vertex_lookup_by_id` | 9 195 005 | 0.29 | 3.04 | 3.58 | −0.50 | **+17.6** | 2.56 | **qualify** |
| `l0_to_l1_flush` | 22 726 147 | 0.21 | 6.54 | 13.06 | +0.27 | **+99.7** | 1.74 | **reject — IO-sensitive** |
| `transaction_commit_wal_on` | 7 739 220 | 0.96 | 1.12 | 3.26 | +0.44 | **+192.3** | 2.38 | **reject — IO-sensitive** |

Baselines (not gate candidates; they make the numbers above readable):

| baseline | Ir | what it establishes |
|---|---|---|
| `baseline_noop` | **4** | harness + toggle overhead is effectively free |
| `baseline_session_only` | **954 493** | one `db.session()` + `block_on` |

**Every prior in the implementation plan held.** Commit and flush were predicted
not to qualify, and do not. Parse+plan, id-lookup and 1-hop expansion were
predicted to qualify, and do. The two "pilot decides" targets — the L0/L1
boundary read and HNSW search — both qualify.

Five qualifying targets clears the phase's stop-the-chain threshold of three, so
**B1 proceeds to Phase 7**.

---

## 2. How the rejection was established

Both halves of the qualification are necessary, and stability alone would have
passed all seven targets.

**Stability.** Over 20 runs every target has CV < 1%, the tightest at 0.21% and
the loosest — `transaction_commit_wal_on` — at 0.96%. On a shared, loaded
developer box. Instruction counting delivers exactly the determinism it
promises.

**CPU-dominance.** The discriminator is a natural experiment rather than a
synthetic regression. `Uni::temporary()` builds its fixture under
`std::env::temp_dir()`, and on this machine **`/tmp` is `tmpfs`** — RAM. Every
WAL write, Lance file and flush in the first pilot went to memory. Re-pointing
`TMPDIR` at the btrfs home filesystem makes the identical code do real durable
IO, and that is the injected regression:

- **Instruction counts do not move.** All seven stay within ±0.5%, inside the
  run-to-run noise band.
- **Wall-clock moves enormously, but only for two targets.** Commit slows
  **2.9×**, flush **2.0×**. The five read paths move ≤ 17.6%.

So for `transaction_commit_wal_on`, a change that made the operation nearly three
times slower left its instruction count flat to within 0.44%. That is precisely
the blind spot §7.2 of the proposal warned about — a gate on that target would
have sailed green through a 3× regression.

> **This is why the pilot was made mandatory.** Had B1 been built directly, the
> first pilot's tmpfs numbers would have "shown" commit and flush to be
> CPU-dominant (commit had the *highest* throughput of all seven at 6.94 Gi/s),
> and both would have been gated on a metric that cannot see their dominant cost.

### 2.1 Caveat on the read-path deltas

The disk-vs-tmpfs wall-clock comparison is one median-of-20 per configuration,
so differences below roughly 20% are not resolved. The `+17.6%` on
`vertex_lookup_by_id` and `+12.3%` on `hnsw_top10_search` are therefore *not*
established as real; they are well inside the gap to the rejected pair (+99.7%
and +192.3%), which is what the verdict rests on. If either target later proves
IO-sensitive under load, it should be dropped from the gate — the threshold used
here (Δwall < 25%) is deliberately generous to the rejects, not to the
qualifiers.

---

## 3. Three defects found before any number was trusted

Each produced a **green, plausible-looking run** that measured the wrong thing or
nothing at all.

### 3.1 Stripped bench binaries → silent zero

The first pilot reported `Iai-Callgrind result: Ok. 7 without regressions` with
**0 instructions on every target**. Callgrind's own log: `Collected: 0`, against
34.8 M basic blocks actually executed.

Cause: `.cargo/config.toml` sets `[profile.release] strip = "symbols"` (a wheel
size measure), and **`bench` inherits from `release`**. `nm` on the bench binary
returned zero symbols. iai-callgrind runs Callgrind with `--collect-atstart=no`
and toggles collection on entry to the benchmark function *by name*; with no
symbol table the toggle never matches, nothing is collected, and the run exits
successfully.

Fix: a `[profile.bench]` inheriting release with `strip = "none"` and
`debug = 1`. This also benefits the 18 existing Criterion benches, which
previously produced unsymbolicated `perf`/`valgrind` output.

### 3.2 Multi-threaded work escaping the collection toggle

Callgrind's toggle fires on the thread entering the benchmark function. With a
multi-thread tokio runtime the query work lands on workers, whose per-thread
files collect nothing.

Fix: the bench uses a **current-thread** runtime. Phase 0A had established that
current-thread performs flush, fork and snapshot+pin without trouble, so this
costs no coverage — and it removes scheduler nondeterminism from the counts,
which an instruction-count gate cannot tolerate anyway.

Verified rather than assumed: the collector reports per-thread totals, and every
target now shows **100% of instructions on the main thread and exactly 0 on the
other 10–37 threads**.

### 3.3 Session construction swamping every read target

With real counts flowing, five read targets sat within **1.7%** of each other
despite doing entirely different work — an HNSW top-10 search measuring *less*
than a cold parse+plan. Absolute numbers gave no hint; the tell was a ratio that
could not be true.

The two baselines diagnosed it. `baseline_noop` = 4 instructions, so harness
overhead is nil; `baseline_session_only` = **954 493**, which *exceeded the entire
previous "session + query" measurement*. Hoisting `db.session()` into setup moved
the targets by **12–59×**, after which they differentiate sensibly.

> **Worth carrying to the performance track:** `Session` construction costs about
> **0.95 M instructions**. `Uni::session()` is documented as "cheap, synchronous,
> and infallible" and reads that way at the API surface, but `Session::new_base`
> allocates a fresh session-local plugin registry, metrics, plan cache, write
> guard and cancellation token every time. That is ~11% of a full simple query
> (`parse_and_plan_cold`, 8.3 M) — and `metamorphic::run_bag` calls it **once per
> query**. Not investigated here; recorded as a candidate.

### 3.4 A silent drop in this pilot's own tooling

`scripts/perf/iai_collect.py` required a `.tNN.pN` infix on Callgrind output
filenames. Single-threaded benchmarks produce a bare `callgrind.<fn>.<id>.out`,
so `baseline_noop` was skipped entirely and never appeared in the report — with
no warning. Fixed; `iai_cv.py` now prints per-target sample counts so a
collection gap shows as `2 of 20` rather than passing as a clean mean.

---

## 4. Cross-runner variance — **UNRESOLVED**

The phase's exit criterion asked for ≥ 20 runs across ≥ 3 CI runner
instantiations. Only the first half is satisfied: all 20 runs are from **one
machine**. Per decision at planning time, the cross-runner leg is deferred.

What this means concretely:

- The CV figures above characterize **run-to-run** variance on fixed hardware.
  They say nothing about variance **across** GitHub runners, which is what a PR
  gate actually experiences.
- Instruction counts are expected to be far more portable than wall-clock, but
  microarchitecture, allocator behaviour and dependency versions can all shift
  them. This is an expectation, not a measurement.
- **Closing this is a prerequisite of Phase 7**, before any threshold is set.
  Collect ≥ 3 runner instantiations, recompute CV across them, and set the gate
  threshold from the cross-runner figure — not from the 0.21–0.96% measured here.

### How to close it

The mechanism now exists and is dispatch-only; it has **not been run**, so this
section describes the procedure, not a result.

```
Actions → "Perf Qualify (cross-runner iai)" → Run workflow
```

Defaults are **5 runners × 5 runs = 25 samples**. Each matrix shard is a fresh
VM, so five shards on one label is five runner instantiations; they run in
parallel, so wall-clock is one shard (~27 min: a ~15 min cold bench compile plus
5 × ~2.3 min of Valgrind). The runner label is hardcoded `ubuntu-xlarge` rather
than routed through `vars.CI_RUNNER_*`, because Phase 7 puts `perf-gate` in
`pr.yml` — whose heavy jobs use that label — and because the variable could
otherwise land every shard on one self-hosted box and collapse the measurement
without changing the workflow file.

The `aggregate` job runs `scripts/perf/iai_cross_runner.py`, which reports per
target:

| column | meaning |
|---|---|
| cross-runner CV % | spread of the per-runner means ← **the Phase-7 figure** |
| within-runner CV % | mean repeatability on fixed hardware — comparable to §1 |
| spread % | max-to-min across runner means, as a sanity read |

`iai_cv.py` cannot answer this and was not extended to: it pools every run into a
single CV, conflating the two columns above. A target with perfect repeatability
and wild machine-to-machine spread produces the same pooled number as the
reverse, and only one of those threatens a gate.

The script **exits non-zero** on a collection gap or any zero-instruction target,
rather than warning. That is deliberate — §3.4 above is the record of a
zero-instruction target being dropped silently, and a CI job that inherited
`iai_collect.py`'s warn-and-exit-0 behaviour would report a clean cross-runner
qualification computed over no data.

Then replace this section with the measured table and a recommended threshold.
**The `baselines::` group is excluded from the headline figure** — the report
already classes them as non-candidates, and `baseline_noop` is 4 Ir, where a
one-instruction difference is a double-digit percentage.

Also recorded for Phase 7 while the cost is fresh:

- The Swatinem cache's only saver is `ci.yml:56` (`tck-full`), which never builds
  `--benches`, so a perf job pays a cold bench compile every run. Observed here:
  **15 min** for a full bench-profile build, then ~140 s per Valgrind pass.
- `iai-callgrind` pulls `proc-macro-error2 v2.0.1`, which cargo already flags as
  containing code that a future Rust version will reject. Harmless today; worth
  watching before this lands in a gating job.

---

## 5. Recommendation for Phase 7

Gate these five, at **>5% fail / 2% warn** on instruction count, once the
cross-runner variance leg (§4) sets the real threshold:

- `parse_and_plan_cold`
- `expand_batch_one_hop_warm`
- `property_read_across_l0_l1`
- `hnsw_top10_search`
- `vertex_lookup_by_id`

Keep these two in the nightly wall-clock Criterion suite, **ungated**, with the
reason recorded so nobody re-proposes them:

- `transaction_commit_wal_on` — instruction count blind to a measured 2.9×
  IO-driven slowdown.
- `l0_to_l1_flush` — same, 2.0×.

Retain both baselines in the gating bench. They cost 4 and ~954 k instructions
and they are what makes a future anomalous reading diagnosable instead of
mysterious.
