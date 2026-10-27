# Collection candidate follow-up evidence

This directory holds results of the real-Value and L0 follow-up experiments.
The executable source is `crates/uni/benches/collection_candidates.rs`; its
experimental kernels live in `benches/support/collection_candidates.rs`.
Differential tests join the existing `uni-db` integration binary in
`tests/common/perf/collection_candidates.rs`.

No candidate index is installed in the storage engine. The hinted range query
uses a quiet, unflushed fixture without concurrent writes and is not a production
visibility mechanism. The immutable-open benchmark reads owned, warm bytes,
not a cold file mapping, and its trusted path relies on FileWriter provenance.

## Reproduce

```bash
RUSTC_WRAPPER= cargo nextest run --offline -p uni-db --test integration -E 'test(collection_)' -j 6 --no-capture
RUSTC_WRAPPER= cargo bench --offline -p uni-db --bench collection_candidates --profile dev --no-run -j 6 --config 'profile.dev.package.uni-db.opt-level=3' --config 'profile.dev.package.uni-store.opt-level=3' --config 'profile.dev.package.uni-query.opt-level=3' --config 'profile.dev.package.uni-common.opt-level=3' --config 'profile.dev.package.bbtree.opt-level=3' --config 'profile.dev.package.uni-db.debug-assertions=false' --config 'profile.dev.package.uni-db.overflow-checks=false' --config 'profile.dev.package.uni-store.debug-assertions=false' --config 'profile.dev.package.uni-store.overflow-checks=false' --config 'profile.dev.package.uni-query.debug-assertions=false' --config 'profile.dev.package.uni-query.overflow-checks=false' --config 'profile.dev.package.uni-common.debug-assertions=false' --config 'profile.dev.package.uni-common.overflow-checks=false' --config 'profile.dev.package.bbtree.debug-assertions=false' --config 'profile.dev.package.bbtree.overflow-checks=false'
```

Use the executable path printed by Cargo as `$COLLECTION_BIN`. This uses the dev
dependency cache with the benchmark, bbtree and the three relevant production crates
at opt-level 3, with debug assertions and overflow checks disabled for those
packages. Other dependencies retain the dev package overrides in
`.cargo/config.toml` (generally opt-level 2). Symbols and line tables are kept.
It is not the release profile, and does not set `target-cpu=native`.

After builds have finished, choose an otherwise idle CPU and run:

```bash
taskset -c 1 "$COLLECTION_BIN" --bench --noplot
COLLECTION_E2E=1 taskset -c 1 "$COLLECTION_BIN"
COLLECTION_PROFILE=l0_scan COLLECTION_SIZE=8000 COLLECTION_REPETITIONS=5000 perf stat -e cycles,instructions,cache-misses,branch-misses taskset -c 1 "$COLLECTION_BIN"
COLLECTION_PROFILE=l0_scan COLLECTION_SIZE=8000 COLLECTION_REPETITIONS=5000 perf record -e cpu-clock:u -F 499 --call-graph dwarf -o /tmp/collection-profile.data taskset -c 1 "$COLLECTION_BIN"
perf report --stdio --no-children --percent-limit 1 -i /tmp/collection-profile.data
```

Other profile selectors: `l0_index_query`, `l0_index_build`, `sparse_tree`,
`sparse_sort`, `canonical_tree`, `canonical_vec`, `interval_tree`, `interval_vec`.
Profile process counters include fixture setup and teardown; choose enough
repetitions for the selected loop to dominate. The loop prints its own elapsed
time separately. Criterion uses 20 samples, 200 ms warmup and a 1 second target
measurement per cell; expensive cells can take longer.

The `assertions_enabled_pilot/` directory retains the diagnostic first pass.
CPU profiling exposed debug-only precondition checks there, so the main results
use the corrected flags above. It is not a second independent hardware trial.

## Public-query controls and provenance

`e2e-control.log` and `e2e_samples.csv` contain the final four-variant experiment:
range, parameter VID hint, literal VID hint, and correlated query. The literal
variant includes candidate lookup, ID sorting, query-text reconstruction and
planning/execution inside its timer. Its one-time index build is separate.
`e2e.log` retains the earlier three-variant run; it is not pooled into the final
seven-sample medians. `e2e-control.report.txt` is the final whole-fixture CPU
profile. Neither CPU recording isolates one query variant.

The Criterion run preceded the addition of the literal control; its kernels and
build configuration were unchanged. Cargo reused the executable filename across
these builds. Use source hashes and the recorded build logs as provenance,
not that filename as a unique binary ID. The final smoke exercised all 61 cells.
The CSV PMU summary uses CPU-core events; CPU-atom rows with zero coverage are
excluded. Raw perf data remain at the `/tmp` paths in `profile_commands.json`;
portable text reports are retained here. Recreate raw data with the commands if
those temporary files have been removed.

See [the follow-up report](../../bbtree_followup_2026_10_26.md) for conclusions
and experimental limitations. `SHA256SUMS` covers this directory recursively;
`source_hashes.json` records the final harness/test and dependency-file hashes.
