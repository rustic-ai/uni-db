# Track E — Plan of Action

**Date:** 2026-08-25
**Status:** Plan of action, not yet started
**Extends:** `docs/proposals/test_harness_track_poa_2026-08-24.md` §T5
**Sources the items from:** `docs/proposals/test_harness_and_benchmarks_2026-08-11.md`
§§8–10 (B2/B3/B4), §5 (C3), and `test_harness_implementation_plan_2026-08-12.md`
phases 11–13.

T5 in the track POA is one paragraph and an ordering — `B4 → B3 → B2 → C3`. This
document turns it into a plan against the tree at `83587ee4c`.

The verification rule carries forward unchanged: **a claim about status cites an
artifact in the tree, never a memory of having done it.** Everything in §1 was
re-checked on 2026-08-25.

---

## 1. Verified state at `83587ee4c`

| item | proposal says | **actually, checked 2026-08-25** |
|---|---|---|
| B4 | `ssi_contention.rs` "has the right idea" | present, 106 lines, wall-time only — **and its documented second arm does not exist** (§2.1) |
| B3 | extend `dense_retrieval.rs` | present and better than the proposal assumes: already sweeps `ef_search` and measures recall@10 against a brute-force oracle. Missing: scale, IVF-family cells, QPS-vs-recall framing, any BEIR surface |
| B2 | write `crates/uni/benches/ldbc/` | absent. No LDBC anything. `csv` **is** already a `uni` dependency; `uni-bulk`'s `BulkWriter` is the loader path |
| C3 | Rust driver + `elle-cli` | absent. No history driver, no `setup-java` in any workflow |
| — | (unlisted) dataset fetch | **absent, and needed by both B2 and B3** (§2.3) |
| — | (unlisted) Graphalytics LCC | **`uni-algo` has no clustering coefficient** (§2.4) |

Supporting inventory, for the items the plan leans on:

- `crates/uni/benches/` holds 19 benches; all are Criterion `harness = false`
  except `hot_paths_iai`. Every one is registered in `crates/uni/Cargo.toml`.
- `docs/perf/` holds 4 documents plus `iai-baseline.json`, still with **no
  index** — carried as T4.3 and closed here as E5.
- `nightly.yml` runs 6 jobs, each boxed at 60 or 120 minutes.
- `uni-algo` ships BFS (`traversal.rs:41`), PageRank, WCC, label propagation
  (= Graphalytics CDLP) and Dijkstra (= SSSP). Five of the six Graphalytics
  kernels exist.

---

## 2. Findings that shape the plan

### 2.1 Three SSI benches document an arm that cannot be run

`ssi_contention.rs:14-17` instructs:

```bash
cargo bench --bench ssi_contention                  # baseline (LWW, no aborts)
cargo bench --bench ssi_contention --features ssi   # ssi on: retries under contention
```

**There is no `ssi` feature.** `crates/uni/Cargo.toml` says so in a comment on
the bench block itself: *"SSI/OCC is always compiled and toggled at runtime via
`UniConfig::ssi_enabled` (default `true`); it is no longer a cargo feature."*
Cargo rejects the flag outright, so nobody has silently measured the wrong
thing — but the bench's *stated* deliverable, the on/off gap that "is the
conflict-and-retry overhead that buys correctness", has never been measurable
from the bench as written.

`ssi_commit_overhead.rs` and `ssi_read_tax.rs` carry the same stale
instructions. `ssi_freeze.rs` and `commit_throughput.rs` do not name the dead
flag, but describe the off arm in prose all the same.

**Corrected 2026-08-25, and it is worse than the above.** An earlier revision of
this document said `ssi_freeze.rs` toggles `ssi_enabled` through the builder and
is therefore the template. That was a grep matching a doc comment, not code.
`grep -n ssi_enabled crates/uni/benches/*.rs` returns exactly one hit, and it is
line 18 of `ssi_freeze.rs`, inside a `//!` block. **No bench in the workspace
sets `ssi_enabled` in code.** Every one of the five constructs its database with
`Uni::in_memory().build()`, which takes the default — SSI on. So all five
document a comparison arm that no code path produces, and there is no in-tree
template to copy. The lever is `UniBuilder::config(UniConfig { ssi_enabled:
false, .. })` (`crates/uni/src/api/mod.rs`), which E1 is the first bench code to
use.

This is the same defect class the track has been finding all along, one more
level out: not a gate that passes while doing nothing, but a *benchmark whose
comparison arm does not exist*. It is fixed inside B4 and swept across its two
siblings, because a contention curve with no LWW reference line is half a
result.

### 2.2 Abort-rate needs no new instrumentation, only relocation

B4's headline metric is abort rate, and the proposal describes it as absent.
It is not. Production emits it:

- `crates/uni/src/api/session.rs:835` — `uni_ssi_retries_total{stage="commit"}`
- `crates/uni/src/api/session.rs:843` — `uni_ssi_retries_total{stage="body"}`
- `uni_ssi_serialization_conflicts_total` (per the `ssi_support` rustdoc)

The obstacle is reading them. `metrics::set_global_recorder` may be installed
**at most once per process**, and as of metrics-util 0.20 `Snapshotter::snapshot()`
*consumes* counters. `crates/uni/tests/common/ssi_support/metrics.rs` already
solves both — install-once via `OnceLock`, fold every snapshot into a
process-global accumulator so reads stay monotonic and multiple probes compose.

A Criterion bench binary is one process, so the constraint is satisfiable, but
the probe lives under `tests/` where a bench cannot reach it. **E1 lifts it into
a shared location** rather than reimplementing it — reimplementation is how the
consuming-snapshotter trap gets stepped on a second time.

### 2.3 B2 and B3 share a prerequisite neither of them lists

Both need multi-hundred-megabyte fixtures that cannot be committed: LDBC SF1,
SIFT-1M, GloVe-100, GIST-960, and 3+ BEIR corpora. The tree has **no
`.gitattributes`, no LFS, and nothing in `scripts/` that downloads and verifies
anything**. The proposal's phrasing ("store SF1 in S3 with a checked-in manifest
and checksum") describes infrastructure that does not exist.

Built twice, it will be built two different ways and one of them will skip the
checksum. **E0 builds it once**, before either consumer.

### 2.4 Graphalytics LCC has no implementation

`grep -rin "clustering_coefficient|local_clustering" crates/uni-algo/src
crates/uni-plugin-builtin/src` returns nothing. LCC is derivable from
`triangle_count` and degree, so this is a small addition rather than a new
algorithm — but it is product work inside a benchmark item, and B2's third
checkbox cannot be ticked without it. Called out so it is scheduled rather than
discovered.

### 2.5 The nightly box is already contended

`nightly.yml` runs 6 jobs. T2 (in the track POA) is *already* re-measuring the
`dqp` job against its 60-minute box because 10 soaks were sized from an 8-soak
extrapolation. Track E proposes to add three more nightly lanes — B4, B3 and C3.
Each must state its measured wall time before it is wired in, and the response
to approaching a ceiling is to cut volume, not to raise the box. That rule is
inherited, not new.

---

## 3. The plan

Six items. E0 gates E2 and E3. E1, E4 and E5 are independent of everything.

### E0 — Fixture fetch and verify — **DONE 2026-08-25**

Shipped as `scripts/fixtures/fetch.py` (stdlib only), pinned by
`scripts/fixtures/fixtures.toml`, documented at `docs/fixtures.md`. Proven
end-to-end on BEIR SciFact: 3 files, 2 repos, both digest algorithms, 4.4 MB.

**The design changed on one measured fact.** The Hub publishes a digest *out of
band* — the tree API's `lfs.oid` is the content sha256 for LFS files, the git
blob sha1 for small plain ones — so a pin can be cross-checked against a number
we did not choose. Verified before building: streaming the real bytes through
`sha256sum` reproduced the published oid exactly. Without that, a digest we
compute after downloading and write into our own manifest would verify nothing —
the self-certifying case `crates/uni-plugin/src/verify.rs` already warns about
in prose. `--check-upstream` is that third channel.

Two deviations from the plan as written, both corrections:

- **The cache is `~/.cache/uni-fixtures`, not `target/fixtures/`.** The
  rationale at `crates/uni/tests/common/bge_m3_real_onnx.rs:33-38` records that a
  relative default once stranded a 2.1 GB model inside the checkout, and
  `cargo clean` must not destroy a multi-GB download.
- **No "skip loudly" mode.** A skip switch on a layer whose entire purpose is
  anti-vacuity is a hazard; a missing fixture is a hard failure that prints the
  exact fetch command. The Rust consumer contract is *documented* rather than
  shipped, because no bench consumes a fixture yet and Cargo compiles only
  declared bench targets — an unreferenced `benches/common/fixture.rs` would sit
  in the tree uncompiled by anything, which is this track's own inert-code
  defect. It lands with its first caller in E2. Writing it down still caught a
  real bug: `cargo bench` runs with CWD at the *package* root, so a
  workspace-relative path misses; the documented form uses `CARGO_MANIFEST_DIR`
  and was compiled and run against warm and empty caches.

**Guards proven to bite** (`--self-test`, 12/12 offline, plus live checks): a
single flipped byte at identical size is caught and the file deleted; a tampered
pin is caught against the Hub; a missing `digest` key, a branch-name `revision`,
a typo'd selector and an empty manifest are all exit 2 rather than a quiet pass;
`--print-path` refuses to emit a path for a fixture that is not there.

Deferred to the first fixture large enough to need them: retry-with-resume,
`--gc`, an `actions/cache` layer, and the fvecs structural probe.


### E1 — B4 contention curves — **DONE 2026-08-25**

`ssi_contention.rs` rewritten as throughput **and abort rate** vs contention.
Published: `docs/perf/contention-2026-08-25.md`. 18 cells, 177 s measured.

**The curve separates the two regimes B4 existed to distinguish.** Going from 8
to 24 writers at `theta = 1.2`, the `lww` arm gains ~8% throughput while the
`ssi` arm gains nothing, as its abort rate reaches ~59%. Stated carefully: the
`ssi` change across those points is −1.8% and −0.5% over two runs, which is
inside the throughput noise, so this is a demonstrated **divergence in scaling**,
not a demonstrated collapse. Establishing an outright turn-over needs a sweep
past 24 writers, which is left for the nightly lane.

Run twice. Because the Zipf sampler is seeded, **abort rate reproduces within 0.6
percentage points on every cell** while throughput moves up to ~5% — so the abort
rate is the stable quantity and conclusions are drawn from it.

Three findings beyond the curve:

- **Retry exhaustion is the operationally sharp one.** `RetryOptions` defaults to
  `max_attempts: 5`. At `theta = 1.2` / 24 writers, **3353 operations exhausted
  that budget and returned an error to the caller** — not aborts that later
  succeeded. The boundary where `execute_with_retry`'s default stops sufficing is
  now measured rather than assumed.
- **Uniform is not conflict-free**: `theta = 0` over 256 keys still aborts 8.82%
  of commits at 24 writers. "Spread the keys" is an insufficient mitigation.
- **The SSI tax is a function of skew, not a constant** — ~10% at `theta = 0`,
  ~27% at `theta = 1.2`, both at 24 writers.

**Abort rate needed no new instrumentation**, contrary to the proposal's premise.
`uni_ssi_commit_validations_total` and
`uni_ssi_serialization_conflicts_total{kind}` are both already emitted, and the
comment beside them at `crates/uni-store/src/runtime/writer.rs` had already named
the metric: *"the ratio of conflicts to validations is the headline abort rate."*
E1 is the first thing to compute it.

**The probe was included, not moved.** Step 2 as written proposed relocating
`tests/common/ssi_support/metrics.rs` into the crate behind an internals feature
or into a new dev-crate. Neither was necessary: the module has no `crate::`/
`super::` coupling and `metrics-util` is already a dev-dependency, so the bench
pulls it in with `#[path = "../tests/common/ssi_support/metrics.rs"]`. One
implementation, no drift, no new crate, and the public surface does not grow.

**Non-vacuity, and both gates verified to bite.** The `ssi` arm must record a
conflict in a multi-writer cell and a nonzero validation count; the `lww` arm must
record **zero** conflicts, since `ssi_enabled = false` skips validation entirely —
a conflict there would mean the arm never took effect and the sweep measured the
same thing twice. `CONTENTION_WRITERS=1` exits 1 with *"cannot observe an
abort"*; `--test` mode refuses to report a curve. Neither was assumed to work.

**Two defects found in the new bench before it ever ran:** a fresh `Session` per
operation (discarding the shared plan cache, so re-planning would have buried the
conflict cost being measured), and counter probes bracketing the seeding `CREATE`
— itself a writing commit — which put setup in the abort-rate denominator while a
comment claimed it did not.

**Still open:** the nightly lane. Per the track's own rule the 177 s is a
laptop measurement (22 cores) and the lane is not wired in until it is confirmed
on CI hardware. Sibling benches `ssi_commit_overhead.rs` and `ssi_read_tax.rs`
had their dead `--features ssi` instructions replaced with the runtime-toggle
form, but still measure only one arm; converting them to two-arm sweeps was not
in E1's scope.

#### Original task list, for the record

Formalize `ssi_contention.rs` as **throughput and abort-rate vs contention**.

1. **Fix the arm first.** Replace the `--features ssi` doc block in
   `ssi_contention.rs`, `ssi_commit_overhead.rs` and `ssi_read_tax.rs` with the
   runtime toggle. There is no in-tree template — E1 writes the first one. In
   `ssi_contention` make both arms
   *cells of one sweep* (`ssi=on` / `ssi=off`) rather than two invocations, so
   the gap is in one report. Note in the bench that the off arm is LWW and
   **loses updates** — it is a reference line, not an alternative.
2. **Lift the metrics probe.** Move `tests/common/ssi_support/metrics.rs` to a
   location both tests and benches can use (`crates/uni/src/testing/metrics.rs`
   behind an existing internals feature, or a small `uni-test-support` dev
   crate — decided at implementation time by which keeps the public surface
   unchanged). Tests keep importing it from one place; the bench gains access.
3. **Two axes, not one.** Sweep thread count `{1,4,12,24}` × Zipf θ
   `{0.0, 0.5, 0.9, 0.99, 1.2}` over a keyspace large enough that θ actually
   changes the collision rate — today's bench has a **single** hot key, which is
   θ=∞ and hides the entire shape.
4. **Report abort rate per cell**: `conflicts / attempted-commits`, from the
   counters in §2.2, alongside throughput.
5. **Non-vacuity check, mandatory.** At θ=0 with a large keyspace the abort rate
   must be ≈0, and at θ=1.2 it must be materially above it. If both are zero the
   sweep is not contending and the numbers are decoration — fail the bench.
   This is the B4 analogue of C1's activation witnesses.
6. Publish `docs/perf/contention-<date>.md`: the two-axis table, the shape
   commentary (graceful rise vs throughput collapse), and the LWW reference.
7. Nightly lane, **only after** its wall time is measured (§2.5).

**Exit:** a published table where abort rate is a measured quantity, the LWW arm
runs, and the non-vacuity check fails when pointed at a uniform keyspace.

**Stops the chain if:** nothing. B4 has no dependents.

### E2 — B3 ann-benchmarks + BEIR *(depends on E0)*

The benchmark that can show RRF fusion is not earning its complexity.
**Publish regardless of outcome** — that is the point of it.

Split, because the two halves have different risk:

**E2a — recall-vs-QPS curves.** `dense_retrieval.rs` is a better starting point
than the proposal assumes: it already measures recall@10 against a brute-force
cosine oracle and already sweeps `ef_search`. What it lacks:

- **Scale.** Its default is 2k/10k synthetic vectors. `fork_index_recall_bench.rs`
  reports recall@10 = 1.000 at n=1000 *because Lance brute-forces below a
  threshold and the index under test never runs* — the same trap applies here
  until the corpus is ≥1M. SIFT-1M via E0.
- **Coverage.** `VectorAlgo` ships `Flat`, `IvfFlat`, `IvfPq`, `IvfSq`, `IvfRq`
  and `Hnsw` (`api/schema.rs:667`). The proposal names HNSW / IVF_PQ / RaBitQ;
  `IvfRq` is the RaBitQ-family cell. Sweep `nprobes` for the IVF family and
  `ef_search` for HNSW — both knobs are already plumbed to Lance
  (`backend/lance.rs:812`, `lance_branch.rs:301`).
- **The Pareto framing.** Report recall@10 against QPS, which is the industry
  currency; latency and recall reported separately is what the proposal is
  correcting.
- **Ground truth.** SIFT-1M ships its own; a brute-force oracle over 1M vectors
  per query is affordable once, cached, not per-run.

**E2b — BEIR nDCG@10.** SciFact / NFCorpus / FiQA, four retrieval arms: dense,
SPLADE, ColBERT, RRF. **State the fusion-vs-best-single-head delta explicitly**
as the headline number. Requires an embedding model in the loop; pin the model
identity and revision in the manifest, because nDCG moves with it and an
unpinned model makes the number unreproducible.

**Exit:** `docs/perf/ann-<date>.md` with per-index Pareto curves at ≥1M vectors,
and `docs/perf/beir-<date>.md` with the fusion delta stated. Both regenerable by
a script named in the document.

**Stops the chain if:** BEIR's embedding cost exceeds any nightly box. Then E2b
becomes a manually-dispatched lane with a committed result, and that is recorded
rather than quietly dropped.

### E3 — B2 LDBC SNB *(depends on E0; largest)*

The one item that is a correctness benchmark as much as a performance one,
because LDBC ships reference answers.

1. **Generate offline, commit small.** SF0.1 CSVs committed; SF1 through E0.
   Avoid the Java/Spark toolchain at runtime — generate once, record the
   generator invocation in the manifest's `produced-by`.
2. **Loader** in `crates/uni/benches/ldbc/`, on `uni-bulk`'s `BulkWriter`
   (`insert_vertices` / `insert_edges` / `commit`). `csv` is already a
   dependency of `uni`.
3. **The 14 complex reads as Cypher.** These are the deliverable. The official
   driver is optional and needed only for formally audited results.
4. **Validate at SF0.1 against LDBC reference answers** — a mismatch is a
   correctness bug, and this is the first thing to run, before any timing.
5. **SF1 latency percentiles** to `docs/perf/ldbc_snb_<date>.md`.
6. **Graphalytics**: BFS, PageRank, WCC, CDLP (label propagation), SSSP
   (Dijkstra) all map onto existing `uni-algo` entry points. **LCC must be
   implemented** (§2.4) — derive from `triangle_count` + degree, with its own
   unit test against a hand-computed graph, landed as a normal product change
   rather than inside the bench.
7. **Nightly SF0.1 lane** as a correctness regression guard — SF0.1 only; SF1 is
   dispatched.

**Exit:** all 14 complex reads match reference answers at SF0.1; SF1 percentiles
published; Graphalytics results for six kernels; SF0.1 nightly and green.

**Stops the chain if:** a reference-answer mismatch turns out to be a product
defect. That is B2 **succeeding** — it fixes the defect and reports it, exactly
as C1's levers and C2's seams did.

### E4 — C3 Elle *(independent; sequenced last)*

Last on the proposal's own reasoning: Elle *demonstrates* a property there is
reason to believe holds, whereas C1 and C2 hunt bugs there is reason to believe
exist.

1. **History driver** in Rust: N concurrent tasks doing `append(key, value)` /
   `read(key)` over graph properties, emitting an EDN/JSON history in Elle's
   list-append format. Configurable concurrency and key skew — reuse E1's Zipf
   generator.
2. **Checker**: `elle-cli` with `--consistency-model serializable`. JVM
   artifact, so **nightly lane with `setup-java`, never a PR gate**.
3. **Negative controls, both deterministic.** The proposal already corrected
   itself here and the correction is load-bearing: *"run with `ssi_enabled =
   false`, expect G2" is unsound* — a lucky schedule under LWW is still
   serializable, so the control would flake.
   - A **hand-constructed anomalous history** (write-skew / G2 cycle) fed
     straight to the checker, asserting rejection. Tests the wiring with zero
     dependence on scheduling.
   - A **deliberately faulty adapter** that drops one anti-dependency edge,
     asserting the pipeline reports the injected anomaly.
   - An LWW run may be kept as a **non-gating observation**, reported, never
     asserted.
4. **≥1000 transactions per nightly run, assert no cycle.**
5. Optionally re-run under C2's failpoints as a poor-man's nemesis — C2's abort
   harness already exists, so this is cheap. Not required for exit.

**Escalation if the JVM dependency is refused:** native Rust G0/G1c/G2 cycle
detection over the same history format, accepting that it reimplements a subtle,
well-tested checker. Decide this **before** writing the driver, since it changes
nothing about the history format but everything about the schedule.

**Exit:** nightly lane green over ≥1000 transactions; both negative controls
reject; the controls verified to *fail* when the checker is disconnected —
same denominator discipline C2 applied to its seams.

### E5 — `docs/perf/` and `docs/testing/` index — **DONE 2026-08-25**

`docs/perf/README.md` and `docs/testing/README.md`. Closes T4.3 of the track POA.

Each is a table of what the document measures, when, **on what hardware**, and —
for `docs/perf/` — whether anything gates on it. The machine column is the point:
`iai_gate.py` was found blind to machine identity, reporting *"all 5 gated
targets within 2.0%"* for local numbers sitting 25-56% below a CI-generated
baseline, because it only fails on regressions. The index states in one place
that exactly **one** number here gates anything, and that
`iai-baseline.json` must never be regenerated locally.

`docs/testing/README.md` names the theme its documents share — a check that
reports success while doing nothing — and documents `reverts/` as replayable
evidence rather than history: a patch that no longer makes its test fail is
itself a finding.

Two citations were wrong on the first draft and were caught by checking rather
than by review: §7.1 lives in the proposal, not in the qualification doc (the
grep matched "Linux 7.1.8"), and `iai-baseline.json` landed 2026-08-25, not
08-24. Every link in both indexes was verified to resolve.

#### Original task

Carried from the track POA's honesty sweep, and now genuinely worth doing
because Track E is what gives it something to consolidate: feasibility,
qualification, baseline, contention, ANN, BEIR, LDBC. One `docs/perf/README.md`
and one `docs/testing/README.md`, each a table of what the document measures,
when it was measured, and on what hardware — the machine identity being the
thing `iai_gate.py` was found to be blind to.

Do it **after** E1 so the index is written against real content rather than
speculatively.

---

## 4. Order and dependencies

```
E0  fixture fetch + verify        ── DONE 2026-08-25
E1  B4 contention curves          ── DONE 2026-08-25
E5  docs/perf index               ── DONE 2026-08-25
E2  B3 ann-benchmarks + BEIR      ── depends on E0, medium
E3  B2 LDBC SNB                   ── depends on E0, largest
E4  C3 Elle                       ── independent, sequenced last
```

E1 first among the substantive items because it is the cheapest, has no
dependencies, and closes a live honesty defect (§2.1) on its way. E0 before E2
and E3 because building the fetch layer twice guarantees two designs. E4 last on
the proposal's own reasoning.

E2 and E3 are independent of each other once E0 lands and may proceed in either
order or in parallel; E3 is listed second because it is larger, and because its
correctness half makes it the more valuable of the two if only one is done.

---

## 5. Cross-cutting rules

Inherited from the implementation plan, restated because Track E is the first
part of the track that is mostly *benchmarks*, where they are easiest to forget:

- **No new top-level test binary.** `docs/test_layout.md`'s cap of 3 per crate
  holds. Benches are exempt (each is its own target by construction) but every
  new bench must be registered in `crates/uni/Cargo.toml` and must justify its
  link cost.
- **`cargo nextest run`**, never `cargo test`.
- **Nothing existing is retired.** Every item is additive.
- **A measurement that refutes the design is the item succeeding.** B3 exists
  precisely so it *can* show RRF is not earning its complexity.
- **Every benchmark states its non-vacuity condition.** E1's is explicit
  (§E1.5); E2's is the ≥1M corpus that stops Lance brute-forcing; E3's is the
  reference-answer match; E4's is the negative controls. A benchmark that
  cannot fail is not evidence.
- **Every nightly lane states its measured wall time before it is wired in**
  (§2.5).
- **Publish regardless of outcome**, into `docs/perf/`, regenerable by a named
  script.

---

## 6. Open decisions

Four, all of which change the work rather than only its presentation. Recorded
here rather than assumed:

1. **Fixture hosting.** S3 (the proposal's assumption, and LocalStack is already
   in `nightly.yml`), a GitHub release asset, or Hugging Face Hub. Affects E0's
   auth story and whether a fork can run the lanes at all.
2. **`elle-cli` vs a native Rust checker** (E4). Decide before the driver is
   written; it does not affect the history format.
3. **Where the metrics probe lives** (E1.2) — an internals-feature module in
   `uni`, or a new `uni-test-support` dev-crate. The constraint is that the
   public surface must not grow.
4. **Whether E2 and E3 run in parallel.** Both are multi-day; both touch
   `docs/perf/` and E0 only.

---

## 7. What this does not propose

- **Adopting the official LDBC driver.** The 14 queries are the deliverable; the
  driver is needed only for formally audited results, and audited results are
  not a goal.
- **A PR gate on any Track E number.** All four are nightly or dispatched. B1's
  instruction-count gate remains the only perf gate, for the reason §7.1 of the
  proposal gives: wall-clock in CI cannot carry a threshold.
- **Reopening C1 or C2.** Both are complete. Track E does not extend them.
- **Raising any nightly timeout box.** The response to a ceiling is to cut
  volume and say so.
