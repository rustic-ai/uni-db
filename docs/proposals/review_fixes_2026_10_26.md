# Uni review corrections

Implementation date: October 26, 2026. Scope: the ten findings in the
[combined triage](review_triage_2026_10_26.md).

The seven correctness defects and three scaling defects have local corrections.
The original reproduction cases are now regular tests in the existing
integration binary, with additional coverage for edge and NodeKey constraints,
anonymous MERGE concurrency, fork first writes and nesting, database reopen,
multiple invalidated vector candidates, private overlays, vector metrics, and
recursive convergence boundaries.

## Corrections

| Finding | Implementation |
| --- | --- |
| C1 Key conflicts after flush | The bounded OCC commit history retains vertex/edge constraint keys, MERGE keys and external IDs alongside item IDs. Distinct-item key conflicts reject before durable WAL commit even after the owning buffer finishes flushing. Pruned history still causes a conservative abort. |
| C2 Fork reads of absent tables | The persisted branch map is authoritative: unmapped datasets are absent in the fork. Reads, counts and searches preserve that empty view. First writes create an empty private tip before publishing its branch mapping. Fork creation inherits only captured datasets; nested capture resolves parent branches after the parent flush. |
| C3 Anonymous MERGE | A temporary internal binding captures the created identity for statement deduplication and commit guards, then is removed from the result row. Existing named-node and path bindings are preserved. |
| C4 Vector candidate replenishment | Disk candidate count includes a bound on distinct buffered VIDs that can invalidate or replace disk winners. Retrieval of latest persistent versions widens until enough candidates remain or storage is exhausted. A fixed multiplier is not used as the stopping guarantee. |
| C5 Fork vector metric | The metric is threaded through `ForkBranching::vector_search_on_branch` and set explicitly on the native scanner, along with the existing query options. |
| C6 Vector label eligibility | The newest explicit label overwrite invalidates old candidates outside the requested label. Overlay precedence, tombstones, replacement scores and predicates are reconciled before final ranking. |
| C7 Recursive budget exhaustion | Convergence is tracked explicitly. An exhausted 1,000-round budget returns an iteration-limit error instead of a successful partial result. |
| F1 Repeated recursive scans | Frontier membership in leading MATCH clauses becomes a parameterized VID predicate. The existing scan performs VID-restricted reads; scope-changing clauses and shadowed CTE names retain the general execution path. |
| F2 Constraint retirement | Vertex, edge and MERGE keys are retired through reverse ownership maps with an ownership check. Cascade deletion retires edge constraint keys too. |
| F3 Hub-edge removal | The edge index stores positions in both adjacency vectors. Swap removal updates moved entries in constant time; vertex deletion uses the same removal primitive. Upserts retire old positions before inserting new ones. |

The writer's existing boxed row-write future now uses an explicit `BoxFuture`
boundary. This prevents compiler auto-trait recursion overflow in callers that
wrap the deep future in transaction timeout machinery, while retaining the
existing allocation and Send contract.

## Performance evidence

The same five probes and three repetitions per case were run after correction.
Fixtures, query text and input sizes match the original performance review.
Development-profile medians:

| Workload | Before | After | Interpretation |
| --- | ---: | ---: | --- |
| Recursive CTE, 900-node chain | 12,609.90 ms | 739.21 ms | About 17 times faster; row visits fall from 811,800 to 2,700 |
| Commit deleting 16,000 uniquely constrained vertices | 1,459.30 ms | 136.95 ms | About 10.7 times faster for commit |
| Commit deleting 16,000 star edges | 1,202.68 ms | 103.51 ms | About 11.6 times faster for commit |
| Direct L0 deletion, 16,000 unique keys | 1,357.00 ms | 34.05 ms | Removes repeated full constraint-map scans |
| Direct graph deletion, 16,000 star edges | 1,639.70 ms | 5.30 ms | Removes repeated adjacency-vector scans |

The recursive query now visits `3 × N` rows on the measured chain: the anchor
and outer query each scan the label once, and recursive rounds visit frontier
nodes. Planning still occurs per round, so this is not a claim that CTE traversal
is as cheap as the dedicated variable-length traversal operator.

For the public deletion cases, the after-fix unconstrained commit is 130.67 ms
and the disjoint-edge commit is 104.32 ms. The pathological cases are now close
to those controls. The reported speed ratios concern commit only; query
execution remains a substantial part of the complete transaction.

The graph position index adds two `usize` positions per indexed edge. Removal
can change neighbor iteration order, which is unspecified; callers needing an
ordered result must request ordering. The reverse key maps add ownership
metadata in exchange for eliminating global scans on each deletion.

These before/after timings use the development/test profile and retain data in
L0 with automatic flush disabled. They establish improvement for the reproduced
workloads, not release latency or L1 storage performance. Optimized scaling and
the final regression run are recorded in the validation evidence below.

### Optimized scaling

The probes also passed with `CARGO_PROFILE_DEV_OPT_LEVEL=2`, retaining the
development profile's debug information and assertions. These are separate
after-fix measurements; there is no optimized before-fix baseline.

| Items | Direct star-edge deletion | Direct constrained L0 deletion | Constrained vertex commit | Star-edge commit |
| ---: | ---: | ---: | ---: | ---: |
| 1,000 | 0.048 ms | 0.226 ms | 1.245 ms | 0.855 ms |
| 4,000 | 0.155 ms | 0.963 ms | 6.095 ms | 4.359 ms |
| 16,000 | 0.901 ms | 4.777 ms | 48.791 ms | 23.765 ms |

For four times the input from 4,000 to 16,000, the isolated operations take
about five to six times as long, versus approximately sixteen times before the
fixes. At 16,000, the optimized unconstrained commit takes 40.681 ms and the
disjoint-edge commit takes 25.076 ms. The public controls also have more than
fourfold growth over this size range; the direct probes isolate the corrected
key and adjacency operations from the rest of commit.

Optimized CTE medians are 45.162 ms for 100 nodes, 127.742 ms for 300, and
377.151 ms for 900, with 300, 900 and 2,700 row visits respectively. The remaining
per-round planning cost is visible, while the repeated full-label scans are gone
for the frontier pattern measured here.

## Validation evidence

- [Low-level tests](review_fixes_2026_10_26/unit_results.txt): 130 selected tests
  passed across graph, L0, OCC, key handling, and storage backend behavior.
- [Initial integration run](review_fixes_2026_10_26/initial_integration_results.txt):
  298 tests passed, including the original reproductions and expanded fork,
  constraint, overlay and recursion coverage.
- [Final integration run](review_fixes_2026_10_26/integration_results.txt): 821
  selected tests passed, including all 19 permanent review regressions. This
  expands the selection to SSI, MERGE, vectors, forks, recursion, deletion and
  snapshots. It overlaps the initial run; their totals should not be added.
- [Development performance run](review_fixes_2026_10_26/performance_debug_results.txt):
  all five probes passed their result checks.
- [Development medians](review_fixes_2026_10_26/performance_debug_medians.json)
  preserve all 42 cases with three repetitions each.
- [Optimized performance run](review_fixes_2026_10_26/performance_optimized_results.txt)
  and [medians](review_fixes_2026_10_26/performance_optimized_medians.json): the same
  five probes passed, with three samples for each of the 42 cases.
- [Clippy](review_fixes_2026_10_26/clippy_results.txt): `cargo clippy -p uni-db
  -p uni-query -p uni-store -p uni-common --lib -- -D warnings` passed. Formatting
  checks passed for the modified Rust sources.
- [Fixed source hashes](review_fixes_2026_10_26/fixed_source_sha256.json) identify
  the implementation and permanent regression source. The original review's
  hashes and failing logs remain the historical baseline.

The permanent regressions are in
[review_regressions.rs](../../crates/uni/tests/common/review_regressions.rs),
registered in the existing `crates/uni/tests/integration.rs` binary. The original
[performance runner](performance_review_2026_10_26/run_probes.py) remains usable
and removes its temporary module registration after each run.

The final integration selection is reproducible with:

```bash
RUSTC_WRAPPER= cargo nextest run -p uni-db --test integration \
  -E 'test(review_regressions) | test(ssi_) | test(merge) | test(vector) | test(fork_) | test(recursive) | test(delete) | test(snapshot)' \
  --test-threads 8 --no-fail-fast
```

Run the optimized measurements with:

```bash
CARGO_PROFILE_DEV_OPT_LEVEL=2 RUSTC_WRAPPER= \
  python3 docs/proposals/performance_review_2026_10_26/run_probes.py
```

The internals reference, user-facing Cypher and fork documentation, and
unreleased notes describe the changed guarantees and recursive-limit error.
All temporary probe registrations have been removed; the 19 permanent
regressions remain registered in the consolidated integration binary.


## Local CI follow-up (2026-10-26)

The complete workspace run passes **8,638 tests**, with 137 skipped by the configured default tier. The initial broad run exposed two storage follow-ups, plus a timing assertion distorted by overlapping builds:

- Preserve the logical key kind in OCC conflict diagnostics, including `ext_id`.
- Keep an unmapped fork table's flushed cardinality at zero, include its buffered rows when requested, and prevent its refresh from overwriting primary's shared cardinality cache. The existing fork-count fixture now checks all three properties.
- Serialize local Cargo lanes through test execution; nextest releases Cargo's build lock before its tests finish. The unchanged scaling assertion passes under that isolation. The runbook now records this requirement.

A subsequent full run passed 8,637 tests and timed out in an existing probe of Lance's cancelled-runtime behavior. The probe now runs each cancellation mode in a bounded child process, verifies that the child actually selected one test, and keeps fixtures owned by the parent. A timeout witnesses the upstream defect only after the cancellation boundary; fixture or initial-read stalls still fail. A normal result or unexpected error still fails the probe. Both modes reproduced the expected cancelled-task panic in the focused verification.

Evidence: [full passing workspace run](review_fixes_2026_10_26/local_ci/workspace-green.log), [26 focused regressions](review_fixes_2026_10_26/local_ci/focused-fixes.log), [bounded Lance probe](review_fixes_2026_10_26/local_ci/runtime-drop-probe-final.log), and [source snapshot](review_fixes_2026_10_26/local_ci/source_snapshot.json). The earlier performance measurements and their original source hash manifest remain historical evidence for the initial fix.

Completed additional gates on the current source:

| Gate | Result |
| --- | --- |
| Formatting, workspace Clippy, rustdoc, static guards | Pass; 8 static guards |
| Miri | 329 passed |
| Loom | 4 passed |
| Metamorphic, `METAMORPHIC_CASES=64` | 105 passed |
| GraphCompute tracing | 216 passed |
| Failpoint crash/recovery | 246 passed |
| Feature-disabled recovery | 26 passed |
| LocalStack storage / hybrid end-to-end | 17 / 1 passed |
| Slim ONNX wheel compile | Pass |
| Cypher TCK, schemaless / sidecar | 3,925 passed and 1 skipped in each mode |
| Locy TCK, schemaless / sidecar | 558 passed in each mode |
| Python uni-db / uni-pydantic | 1,148 passed, 15 skipped / 317 passed |
| Python stub / 3.14 typing checks | 6 modules / 12 source files pass |
| Rust PyO3 loader with PyArrow | 35 passed |
| Fuzz smoke: Cypher, Locy, WAL, BTIC | All 4 passed without crashes |
| CUDA wheel dependency graph | Pass; metadata only |

Local versions are Rust 1.99.1 and Python 3.14.8; see the [environment record](review_fixes_2026_10_26/local_ci/environment.json). Some orchestrator durations include queued or deliberately suspended time. The LocalStack container created for this run has been removed.

**The complete CI result is not green.** Every selected local functional/lint gate above passed, but the supply-chain gate remains red. The calibrated instruction-count gate, flagship notebook execution, real ONNX reranker feature matrix, Python WASM/Extism feature wheel, and dedicated built-wheel ABI job were not run locally. The supply-chain gate rejects RUSTSEC-2026-0391, RUSTSEC-2026-0398 and RUSTSEC-2026-0403. The compatible `zstd-sys` 2.1.2 update clears RUSTSEC-2026-0410. No advisory exceptions were added. Registry checks confirm that Arrow IPC 58.4.1 still requires zstd 0.13, whose latest compatible release is 0.13.3; the advertised fixes require zstd >=0.14.3 and zstd-safe >=8.2.0. See the [remaining advisory output](review_fixes_2026_10_26/local_ci/deny-final.log).

All 21 recorded source/configuration hashes matched the passing workspace run before the subsequent runbook edit. [Fuzz results](review_fixes_2026_10_26/local_ci/fuzz-result.json) and [final Clippy output](review_fixes_2026_10_26/local_ci/clippy-current.log) complete the selected verification. The source snapshot records the tested local tree, including pre-existing unrelated lockfile changes. The follow-up commit includes only the `zstd-sys` security update from that lockfile. The runbook was subsequently updated to require foreground execution at normal OS priority.

The [final local CI summary](review_fixes_2026_10_26/local_ci/results.json) records the completed selected checks, the three remaining advisory failures, and the unrun job scope. No CI process group remains deliberately suspended.


## zstd advisory remediation

The three remaining advisories required implementations newer than the release
lines accepted by Arrow/Lance, compression-codecs and Wasmtime. Two private
workspace crates now forward the older package requirements and features to
registry zstd 0.14.3 and zstd-safe 8.2.0. They contain no compression code;
the fixed upstream implementations and zstd-sys 2.1.2 retain their registry
checksums. Both the main and standalone fuzz workspaces apply the patches.
`deny.toml` is unchanged.

Five facade regressions cover abandoned encoder/decoder context isolation,
bulk/dictionary round trips, corrupt legacy-frame output positions, and
seekable offsets past EOF with Vec output. They run in the regular workspace
nextest lane. An integration regression also exercises zstd-compressed Arrow
IPC batches and Parquet row groups through the actual downstream callers.

The earlier red supply-chain results above are historical evidence from before
this remediation. Patch scope and removal criteria are recorded in
[compat/README.md](../../compat/README.md), including the fact that root patches
do not propagate to downstream published-crate consumers.

Validation on the remediation: **cargo-deny passes all four checks**, with no
new advisory ignores; **5/5 codec regressions** and **224/224 selected consumer
tests** pass. The consumer run includes the compressed Arrow/Parquet regression
and a WASM network round trip. Compatibility-crate Clippy with warnings denied,
the no-default-features `no_std` build, formatting, all eight static guards
(including fuzz-lock freshness), and the 25-crate release-list check pass.
All 15 zstd and 16 zstd-safe feature names and their defaults are preserved.
The optional separate full plugin-suite rerun was stopped during a redundant
rebuild and is not counted as a passing suite. The full workspace and Python
suites above were not rerun for this dependency change.

Current [security validation results](review_fixes_2026_10_26/security_fix/results.json),
[passing audit](review_fixes_2026_10_26/security_fix/deny.log),
[codec regressions](review_fixes_2026_10_26/security_fix/compat-tests.log), and
[consumer tests](review_fixes_2026_10_26/security_fix/consumer-tests.log)
are separate from the earlier failing historical evidence.

An isolated snapshot of the focused commit also passes locked offline dependency
resolution for the root and fuzz workspaces and all four cargo-deny checks.
Its [audit log](review_fixes_2026_10_26/security_fix/staged-deny.log) and lockfile
hash in the validation results distinguish it from the original tested local
lockfile, which contained unrelated changes excluded from this commit.
