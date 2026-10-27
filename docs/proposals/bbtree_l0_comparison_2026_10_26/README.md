# L0 ordered-index backend comparison

The decision and measurements are in [the comparison report](../bbtree_l0_comparison_2026_10_26.md).
This directory preserves raw runs, derived CSVs, environment, profiles and source snapshots.

## Reproduce

Run the command in `build-command.txt` from the repository root. It builds the
`collection_candidates` bench using the same optimized dev overrides as the
preceding evaluation. It is not a release build. No network is needed with the
recorded dependencies cached. Use the executable printed by Cargo; the recorded
path was `target/debug/deps/collection_candidates-e16a25ee9d65d605`.

```bash
RUSTC_WRAPPER= cargo nextest run --offline -p uni-db --test integration -E 'test(collection_)' -j 4 --no-capture
COLLECTION_COMPARE=1 taskset -c 1 "$COLLECTION_BIN"
COLLECTION_COMPARE=1 COLLECTION_COMPARE_PROFILE=std_tuple:point_hit perf stat -x, -e cpu_core/cycles/,cpu_core/instructions/,cpu_core/cache-misses/,cpu_core/branch-misses/ taskset -c 1 "$COLLECTION_BIN"
COLLECTION_COMPARE=1 COLLECTION_COMPARE_PROFILE=bb_packed:replace perf record -e cpu-clock:u -F 499 --call-graph dwarf -o /tmp/l0-profile.data taskset -c 1 "$COLLECTION_BIN"
perf report --stdio --no-children --percent-limit 1 -i /tmp/l0-profile.data
```

Valid backends: `std_tuple`, `bb_tuple`, `std_packed`, `bb_packed`.
Profile operations: `point_hit`, `range_half`, `replace` (32K rows).
The profile selector requires `COLLECTION_COMPARE=1` as well.
CPU-core PMU selectors match CPU 1 on this host; choose the appropriate PMU on
other machines. Profile process counters include setup and teardown.

`run1.log` and `run2.log` are the two final complete processes. Each has eight
rounds, rotating backend order, over four sizes and ten operations. Each TIME
line contains elapsed nanoseconds and repetitions; `samples.csv` divides by
repetitions, and `summary.csv` gives the median/min/max of 16 round averages per
cell. `MATCHES` lines give candidate counts. These are not 16 independent hosts
or individual-operation latency samples. `initial-run.log` preserves the pilot
before a guaranteed point-hit control was added; it is excluded from summaries.
`memory.csv` contains two independent allocation-accounting passes per backend
and size. Timers and memory accounting run separately.

`profile_commands.json` records all 12 stat commands and eight recordings.
Raw `perf.data` paths are temporary; portable text reports are retained here.
The final benchmark smoke covers the preceding 61 Criterion cells as well.
`source/` is a snapshot of the final experimental code and relevant manifests;
`source_hashes.json` identifies its original repository paths. The comparison
uses the unchanged production visibility oracle through the existing tests.
`SHA256SUMS` covers the directory recursively except itself.

## Sustained replacement control

`steady-churn.log` runs the unprofiled `COLLECTION_COMPARE_PROFILE=<backend>:replace`
selector once per backend in eight rotated-order rounds, using separate processes.
Each timer covers one million replacements on a 32K-row index, excluding initial
fixture/build time. `steady-churn-summary.csv` summarizes the eight per-process
averages. This is separate from the 4,096-replacement bursts in the main table.
The focused perf replacement runs also use the one-million-operation selector.
