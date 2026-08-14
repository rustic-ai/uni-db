# Coverage map — 2026-08-14

**A map, not a gate.** Phase 8 item 1 is explicit that percentage gates are not
proposed, because they breed test theater: the cheapest way to raise a number is
to write assertions over code that already works, which is exactly the test this
suite has spent five phases learning to distrust. What a map is good for is the
opposite question — *which live code has never executed under any test* — and
that turned up two answers worth acting on.

Regenerate:

```bash
cargo llvm-cov nextest --workspace \
  --exclude uni-tck --exclude uni-python --exclude uni-python-onnx \
  --exclude uni-python-cuda --exclude uni-python-metal \
  --exclude uni-python-onnx-cuda --exclude uni-python-onnx-metal \
  --json --output-path coverage.json
```

Re-run quarterly. Next due: **2026-11**.

## Run it under `nextest`, not `cargo test`

`cargo llvm-cov` defaults to `cargo test`, which runs a crate's tests in **one
process with threads**. Several `uni-db` integration tests fail that way — not on
assertions about behaviour, but on *preconditions*:

```
bugs::repro_time_travel_explain_profile_cursor::…
  assertion `left == right` failed: precondition: live version has two people
    left: 1, right: 2
bugs::repro_multi_label_endpoint_properties::single_label_endpoints_still_work
  Error: open table 'deltas_WROTE_fwd' at '/tmp/.tmpF3Ctbc/…'
```

Fixtures colliding, not logic breaking — which is why this repo standardizes on
nextest for process isolation. Under `cargo llvm-cov nextest` the whole run is
green. Anyone regenerating this map with the default invocation will hit those
failures and should not read them as regressions.

## Headline numbers

| | |
|---|---|
| line coverage | **78.9%** (141,967 / 179,909) |
| files reported | 520 |
| **zero-coverage files** | **22** (1,441 lines) |
| files under 25% | 7 |

78.9% across a workspace this size is healthy, and the number is not the point.

## The finding: two live operators that no test has ever run

Both are **planner-reachable** — this is not dead code — and both have **zero**
executed lines:

| file | lines | reached from |
|---|---|---|
| `uni-query/src/query/df_graph/vid_lookup_join.rs` | **441** | `df_planner.rs:4218` (`try_emit_vid_lookup_join`) |
| `uni-query/src/query/df_graph/mutation_foreach.rs` | **154** | `df_planner.rs:1342` (`ForeachExec::new`) |

`VidLookupJoinExec` replaces `HashJoinExec` when the join is INNER or LEFT, the
equi-pairs contain exactly one anchor pair on the probe-side `_vid`, and the
probe subtree is a fresh `GraphScanExec`; otherwise the planner falls through to
`HashJoinExec`. **No feature gate, no config flag** — the fallback is what has
kept it unexercised. So a query of the right shape gets a 441-line physical
operator that has never run in a test, and the differential oracles cannot see
it either: DQP compares one query across two *storage* states, and nothing in
`querygen` emits a cross-MATCH join.

That is the single highest-value item this map produced. It is exactly the shape
Phase 5 was built to catch and structurally cannot: a code path whose entry
condition no generated case satisfies.

`ForeachExec` (Cypher `FOREACH`) is the same story at 154 lines.

## The rest, and why most of it is uninteresting

The remaining 20 zero-coverage files are small and mostly explicable:

| crate | files | lines | what they are |
|---|---|---|---|
| `uni-plugin-wasm` | 4 | 445 | guest adapters — need a WASM fixture to execute |
| `uni-plugin-extism` | 3 | 190 | same, Extism side |
| `uni-plugin` | 5 | 88 | trait definitions; little executable code |
| `uni-locy-tck` | 2 | 51 | harness fixtures, excluded from the run |
| everything else | 6 | 72 | error enums, small builders |

The plugin adapters are worth a note: `pr.yml`'s default-feature pytest run skips
every loader test, and the loaders only execute in the `ci.yml` WASM/Extism lane.
That lane passes (39 tests), so the adapters are exercised *somewhere* — just not
in a run this map captures.

## Under 25%

| coverage | lines | file |
|---|---|---|
| 6.4% | 47 | `uni-store/src/backend/traits.rs` |
| 8.3% | 12 | `uni-plugin-wasm/src/bindings.rs` |
| 17.6% | 17 | `uni-plugin/src/traits/index.rs` |
| **17.8%** | **416** | **`uni/src/api/sync.rs`** |
| 23.1% | 26 | `uni-algo/src/algo/cypher/elementary_circuits.rs` |
| 23.1% | 26 | `uni-algo/src/algo/cypher/maximal_cliques.rs` |
| 24.4% | 41 | `uni-algo/src/algo/cypher/graph_metrics.rs` |

`api/sync.rs` at 17.8% of 416 lines is the notable one — the synchronous facade
over the async API. The Rust suite is `#[tokio::test]` throughout, so the sync
wrappers are largely exercised only through the Python bindings, which this run
excludes.

## What to do with this

Ranked by value, and deliberately short — a long list of "add tests here" is how
a map becomes theater:

1. **Cover `VidLookupJoinExec`.** Construct the query shape that triggers it and
   assert the plan actually contains it (`EXPLAIN` substring, per the Phase-5
   routing-assertion pattern — asserting the operator was reached, not merely
   that the query returned rows). Then compare its results against the
   `HashJoinExec` fallback: same query, two operators, one answer. That is a
   differential test the existing `diff::bag_eq` already supports.
2. **Cover `ForeachExec`** the same way.
3. Leave the rest. Trait definitions and error enums at 0% are not a risk, and
   the plugin adapters are covered by a lane this run excludes.
