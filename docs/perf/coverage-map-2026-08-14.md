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

## The finding: two operators that no test has ever run

> **Corrected 2026-08-15.** This section originally called both operators
> "planner-reachable". That is true of `VidLookupJoinExec` and **false** of
> `ForeachExec` — see the correction below the table. The two are different
> findings and were investigated separately.

Both have **zero** executed lines:

| file | lines | constructed at | actually reachable? |
|---|---|---|---|
| `uni-query/src/query/df_graph/vid_lookup_join.rs` | **441** | `df_planner.rs:4218` (`try_emit_vid_lookup_join`) | **yes** — guarded, now fixed and measured |
| `uni-query/src/query/df_graph/mutation_foreach.rs` | **154** | `df_planner.rs:1342` (`ForeachExec::new`) | **no** — see #176 |

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

`ForeachExec` (Cypher `FOREACH`) is **not** the same story, though this document
originally said it was. It is 154 lines that no query can reach at all:
`grep -rci foreach crates/uni-cypher/src/` returns **0** — the pest grammar has no
`FOREACH` rule and the AST has no `Foreach` variant. `LogicalPlan::Foreach` is
matched in sixteen places and constructed from a query in none, so the front end
cannot produce the node the operator executes. An existing comment at
`crates/uni/tests/common/cypher_write/set_projection_test.rs:1497` already said so
— "(unimplemented in uni) Cypher FOREACH clause" — and is correct.

That makes it a deliberate implement-or-delete decision rather than a coverage
gap, tracked as **#176**. Zero coverage was the symptom; an absent front end was
the cause.

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

## Follow-up: `VidLookupJoinExec` investigated — 2026-08-14

The headline item was chased down. It is **not a one-off**, and the answer was
not what reading the code predicted.

### It is reachable, and correct where it is reachable

`MATCH (a:A) MATCH (b:B) WHERE id(a) = id(b) RETURN a.x, b.y` **emits it**
(measured: `[GraphScanExec, VidLookupJoinExec, ProjectionExec]`). Over
multi-label nodes that query means "nodes that are both A and B" — degenerate,
but a legitimate schemaless question, not a shape nobody writes. It returns the
right answer, verified differentially against the same query forced onto
`HashJoinExec` (`vid_lookup_join_agrees_with_its_hash_join_fallback`).

Vids are globally unique, so this only matches when one node carries both
labels — an earlier version of the probe created `:A` and `:B` separately,
matched zero rows, and would have "proved" correctness over an empty bag.

### Its own documented use case could not fire — the core finding

`MATCH (a:Source) MATCH (b:Target) WHERE id(b) = a.linked_vid` — the query in
the module's own docs — measured as `[GraphScanExec, GraphScanExec,
HashJoinExec, ProjectionExec]`. The build anchor had to be Arrow `UInt64`
(`df_planner.rs:4387`) and **no `uni_common::DataType` maps to `UInt64`**,
because Cypher has no unsigned integer. So the operator could not serve the
purpose it was written for, and the only shape that *did* clear the guard was
`id(a) = id(b)` — where both keys are already vids and there is nothing to look
up. The one shape it could optimize was the one shape needing no optimization.

Now fixed and pinned by `documented_query_uses_the_vid_lookup_join`.

### The LEFT half was dead *and* broken — both now fixed

`VidJoinKind::Left` had **never executed**, and for a reason neither guard
analysis found at first: for LEFT the probe is *necessarily* the optional side,
and `wrap_optional` (`df_planner.rs:747`) always wraps an optional scan in
`NestedLoopJoinExec(PlaceholderRowExec, GraphScanExec)`, which the bare-scan
guard rejected. "Optional side" and "wrapped" are the same condition, so the arm
was dead **by construction** from the commit that introduced it (`deb9d907d`,
April 2026) — born dead, not orphaned by a later refactor.

Three defects had accumulated in that never-run code:

- **B1** — LEFT null-padding wrote NULLs into `b._vid`, declared non-nullable at
  `scan.rs:378,402`, so `RecordBatch::try_new` rejected the batch. Fixed by
  widening the null-extended side in `concat_schemas`, as DataFusion's own
  `build_join_schema` does for outer joins.
- **B2** — build columns were assembled `[m(b0), u(b0), m(b1), …]` against a
  probe side of `[all matches…, all NULLs…]`, so with ≥2 build batches and an
  unmatched row in a non-final batch the output paired the wrong build row with
  the wrong probe values — **silently**. Fixed by emitting all matches before all
  unmatched, so the two agree by construction.
- **B3** — a *second* `downcast_ref::<UInt64Array>().expect("validated above")`
  where "above" was a different loop, surfacing as a panic inside a stream future
  the moment the first assumption changed. Fixed by one shared accessor.

### It pays, measured

Same 20k-target / 50-source fixture, serial runs:

| path | before | after | |
|---|---|---|---|
| INNER | 18.12 ms (`HashJoinExec`) | **7.38 ms** | **2.5×** |
| LEFT outer | 19.03 ms (`HashJoinExec` + `NestedLoopJoinExec`) | **8.86 ms** | **2.1×** |

In both, the 20,000-row probe scan disappears from the plan entirely.

**An earlier revision of this document recommended deleting the LEFT path as
born-dead.** That was wrong, and the error is worth recording: "never ran" and
"not worth running" are different claims, and the first is far easier to
establish. Deleting it would have discarded a 2.1× optimization with no failing
test and no trace.

### What made it fire

- **Guard 5** now accepts `Int64` build anchors with a range-checked conversion.
  A vid is a `u64`, but a vid stored in a Cypher property is `Int64` — Cypher has
  no unsigned integer — so demanding `UInt64` excluded the operator's own
  motivating shape and left it able to fire only on `id(a) = id(b)`, where both
  keys are already vids and there is nothing to look up.
- **Guard 3** now peels the `wrap_optional` wrapper, one level and exact-match
  only. Sound because this operator already null-pads every unmatched build row,
  which is the sole thing that wrapper guaranteed; and schema-neutral because the
  placeholder carries `Schema::empty()`. The SSI `ReadSetRecordingExec` wrapper
  deliberately still blocks the rewrite, so read-set capture is unaffected.

### The systemic fix

`plan_shape::gate` now requires every one of the 31 `ExecutionPlan` impls (35
observable names — `MutationExec` reports five) to be classified `Proven`,
`Unreachable`, or `Unproven`, with `MAX_UNPROVEN` ratcheting down only. `Proven`
must be backed by an actual `assert_plan_uses` call naming the operator, because
a bare mention in a comment is not evidence — the gate rejected two of its
author's own claims on exactly that basis.

**Use `PROFILE`, not `EXPLAIN`.** `ExplainOutput.plan_text` is the *logical*
plan and can never name a physical operator; a positive assertion over it fails
for the wrong reason and a negative one passes vacuously.

## What to do with this

Ranked by value, and deliberately short — a long list of "add tests here" is how
a map becomes theater:

1. ~~**Cover `VidLookupJoinExec`.**~~ **Done** — see the follow-up section above.
   It is covered, fixed, measured at 2.5× / 2.1×, and `Proven` in
   `crates/uni/tests/common/plan_shape/registry.rs`.

   **This item originally said to assert the operator via an `EXPLAIN` substring.
   That was wrong**, and it contradicted this document's own follow-up section ten
   lines earlier. `ExplainOutput.plan_text` is the *logical* plan and can never
   name a physical operator, so a positive assertion over it fails for the wrong
   reason and a negative one passes vacuously. Use `PROFILE`. The retrofit recipe
   lives in `crates/uni/tests/common/plan_shape/mod.rs`.
2. ~~**Cover `ForeachExec`** the same way.~~ **Not possible** — the clause has no
   grammar rule or AST node, so there is no query shape to construct. Tracked as
   **#176** as an implement-or-delete decision.
3. **Work the silent-downgrade catalogue.** The `VidLookupJoinExec` investigation
   found that its failure mode is not rare: a planner survey turned up **29**
   sites where an optimization silently falls back to a result-identical path, so
   no result-based test can tell fired from skipped. See
   `docs/testing/silent-downgrades-2026-08-15.md` for the ranked list, and **#177**
   for the ratchet that works through it.
4. Leave the rest. Trait definitions and error enums at 0% are not a risk, and
   the plugin adapters are covered by a lane this run excludes.
