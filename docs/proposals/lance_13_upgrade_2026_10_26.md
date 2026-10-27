# Lance 13 upgrade validation

Uni upgrades Lance, lance-index and lance-linalg from 11.0.0 to 13.0.0,
and Arrow/Parquet from 58.3.0 to 58.4.1. DataFusion remains at 54.1.0.
Lance 13 requires Arrow 58 and DataFusion 54; upgrading to newer major
versions of those dependencies separately would create incompatible Rust
types at the query/storage boundary.

Upstream references: [Lance 13 dependencies](https://docs.rs/crate/lance/13.0.0),
[Lance 13 release](https://github.com/lance-format/lance/releases/tag/v13.0.0),
and [Lance 12 release](https://github.com/lance-format/lance/releases/tag/v12.0.0).

## Compatibility decisions

- Lance now uses object_store 0.14. Uni and DataFusion retain 0.13.
  A private, optional `lance-object-store` alias supplies the Lance adapter's
  path type; public storage and WAL interfaces do not change.
- Lance 13 defaults to data format 2.2. Uni explicitly creates datasets in
  2.1, the previous stable default. Existing datasets keep their exact format
  across appends, overwrites and compaction. Production primary and
  auxiliary-index writes resolve the dataset once and pass that handle to
  `InsertBuilder`, preserving the shared cache and storage credentials.
- Keep the fork registry state machine, staged-file cleanup holds,
  scalar-index branch fallbacks, stable IO runtime and patched zstd facades.
  The dependency version alone does not establish that these can be removed.
- Lance 13 can report a decoder task-cancellation error where Lance 11
  panicked when polling a scan after its original runtime shut down. The
  regression probe now accepts that specific cancellation failure as well
  as the previous cancelled-task panic or post-cancellation stall. It still
  rejects a successful resume or an unrelated error. Uni's stable IO-runtime
  protection remains necessary.

## Validation

The initial workspace run passed 8,644 tests and failed three: two local
TCP listeners were denied by the sandbox, and the runtime-cancellation
probe expected a panic rather than the newly propagated cancellation
error. After adapting the probe, the host-enabled workspace run passed
**8,647 tests**, with 137 skipped by the existing test configuration.

Three new permanent storage tests verify new primary, append-to-create,
overwrite-to-create, fork-parent and auxiliary-index datasets use 2.1;
existing 2.0 and 2.2 datasets retain their format through writes and actual
compaction. They are registered in the existing uni-store integration binary.

Formatting, all eight repository static guards (including the fuzz lockfile),
and all four cargo-deny policy categories passed. Both root and fuzz dependency
graphs resolve the aligned Lance/Arrow/Parquet/DataFusion versions.

The rebuilt Python binding passed 1,148 uni-db tests (15 skipped) and 317
uni-pydantic tests. Python linting, formatting and stub validation passed.
The saved-database forward and rollback checks both passed, including the
original snapshot's pinned view. Lance 11 also read a fresh Lance 13-created
5,000-row database and its BTree index successfully.

All 35 Rust PyO3 loader tests passed with the installed PyArrow package.
Seventeen cloud-store tests and the hybrid LocalStack end-to-end test
passed against LocalStack 4.13.1; the task-owned container was removed.
These are S3 emulator checks, not live Azure/GCP credential tests.

Cypher conformance passed 3,925 scenarios with one skipped in each of
schemaless and schema modes. Locy passed all 558 scenarios in each mode.
Both suites ran through their repository reporting scripts with `--both`;
raw nextest summaries and the aggregated scenario statuses were checked,
not just the scripts' exit codes. Compliance reports were refreshed.

All 246 selected crash-recovery, resilience and durability tests passed
with failpoints enabled, including interrupted flushes, compaction,
index recovery, WAL replay and fork recovery.

The workspace Clippy gate passed with `--all-targets -- -D warnings`, using
the PR workflow's accelerator exclusions. Cargo emitted a dependency
future-incompatibility notice for proc-macro-error2 2.0.1; the gate itself
returned success. CUDA/Metal wheel variants and live cloud credentials
were not validated by this Linux CPU run.

Exact lane commands, results and source hashes are retained in the
[evidence directory](lance_13_upgrade_2026_10_26/README.md), including
[the validation manifest](lance_13_upgrade_2026_10_26/validation.json).

## Additional CI follow-up (2026-10-27, completed)

At the original additional-CI snapshot, all 66 command checks completed:
63 passed and three failed. The subsequent parser and soak repairs and final
validation are tracked in
[the parser repair report](parser_backtracking_2026_10_27.md). The failures below
record the original snapshot, not the latest disposition:

- Nightly `cypher_parse` fuzzing exceeded the unchanged 10-second per-input
  budget on a 1,633-byte malformed query. Replaying that exact input reproduced
  the timeout (exit 70). The input, original log and bounded replay are saved in
  the evidence directory. This parser-only reproduction does not establish when
  the defect was introduced; no parser repair or timeout increase was made.
- The strict instruction-count gate failed its implausible-improvement guard:
  indexed vertex lookup measured 9,575,537 instructions versus the committed
  baseline's 20,721,018 (-53.79%, beyond the 50% limit). All five samples contained
  all nine workloads; the lookup's CV was 0.18%. The other two gated workloads
  passed. The baseline and thresholds remain unchanged. The repository documents
  25–56% lower counts on other machines, but that observation does not clear
  this failure or prove the measured reduction is a valid improvement.

- The dedicated metamorphic lane at `METAMORPHIC_CASES=50000` passed seven
  suites, but `metamorphic::binding::binding_guard_soak` timed out at 2,400
  seconds. This is an incomplete workload, not a reported query-equivalence
  mismatch. Each case checks at least eight variants through a reference query,
  a conjunct permutation and a variable rename (at least 1.2 million queries
  at the configured volume). Neither case count nor timeout was changed.

The follow-up has passed Miri (including nightly CRDT), PR/nightly Loom,
Shuttle, recovery without failpoints, graph-compute tracing, metamorphic smoke,
slim ONNX compilation, bundled/dynamic reranker tests, warnings-as-errors
Rustdoc, strict website build, Python wheel/OGM/stub checks, all six notebook
verifiers, and the WASM/Extism Python lane. All four short fuzz runs passed;
nightly Locy, WAL and BTIC fuzzing passed their full five-minute budgets.
Benchmark registration and compilation also passed.

All three Criterion targets completed: SSI commit overhead, concurrent mutations
at 1/4/12/24 sessions, and commit throughput at those counts with WAL on and off.
ThreadSanitizer passed all 22 selected SSI tests with no race reports. Its nightly
compiler emitted a future-compatibility warning about recursion depth while
checking the `Send` bound in Locy's `materialize`; this did not fail the lane.

The soak/stress group passed all 22 tests, including the fork workload at
40 forks / 300 mutations / 4 restarts and the oracle tests at the workflow's
volumes (`ORACLE_SOAK_CASES=10000`; the random-program test uses its separate
5,000-program default). That last test took 2,405 seconds, leaving little margin
around the soak profile's documented 40-minute budget.

DQP passed all 13 selected suites at `DQP_CASES=10000` in 1,944 seconds,
including flush, fork, pinned-snapshot, plan-cache, aggregate, CERT and topology
comparisons. The ignored-test sweep passed all 86 selected tests, with the
repository's exact filter and package exclusions.
Exact completed commands, environments,
results and log hashes are in
[`remaining-ci-validation.json`](lance_13_upgrade_2026_10_26/remaining-ci-validation.json).
This follow-up retains normal process priority and serializes Cargo lanes.

## CI completion after parser repairs

The remaining validation run completed after the parser repairs. All five
binding shards passed **50,000 total cases and 1,315,911 query executions**.
Parser/compiler tests, both parser fuzz budgets, both schema modes of Cypher
and Locy TCK, Clippy, formatting, and static checks passed. The Locy probability
projection regression found during the log audit was repaired; report wrappers
now preserve test failure exit codes instead of returning a false green.

The current-source IAI benchmark completed five runs with explicit lookup result
checks. Its strict comparison still flags an unusually large improvement
against the CI baseline
(lookup median 9,564,478 instructions, -53.84%, CV 0.183%).
Baseline and thresholds were not changed. Local CI cannot be described as
entirely green until that comparison is qualified on the appropriate CI runners.
See the [repair report](parser_backtracking_2026_10_27.md) and
[final validation manifest](parser_backtracking_2026_10_27/ci-completion-validation.json).

## Scope of compatibility evidence

The saved Lance 11 fixture contains declared and schemaless vertices, edges,
scalar/vector/full-text indexes, a fork and nested fork, a tag and a snapshot.
The upgrade check compares query results before writing, then writes to
primary and both forks, compacts primary data and rebuilds native indexes.
A separate process using the saved Lance 11 extension checks rollback reads.

The Lance 11 fixture already fails inherited-fork full-text queries with a
missing branch-local tokens dataset. Those queries are excluded from this
cross-version fixture; primary full-text/vector results and fork row/edge
isolation are compared. New-database fork-index behavior is covered by the
workspace regression suite. This fixture is not evidence that the existing
inherited-fork full-text limitation has been repaired.

Performance checks use the saved Lance 11 and rebuilt Lance 13 Python
bindings in separate processes, with 5,000 rows, three repeats and 25 warmed
queries per workload. They measure insert/flush, indexed point/range reads
and scans. These development-build smoke timings do not establish release
performance or production-scale speedups.

| Workload | Lance 11 median | Lance 13 median | New / old |
| --- | ---: | ---: | ---: |
| Insert and flush 5,000 rows | 406.39 ms | 387.23 ms | 0.953 |
| Indexed point query | 4.04 ms | 4.06 ms | 1.005 |
| Indexed 200-row range query | 5.68 ms | 5.37 ms | 0.945 |
| Materialize a 5,000-row scan | 14.21 ms | 13.08 ms | 0.920 |

Separate profiles confirmed actual BTree index hits for both query shapes
with both extensions. No obvious slowdown appeared in this smoke check.
Fixed execution order and
three repeats do not establish statistical significance for the differences.
