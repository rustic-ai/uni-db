# Silent-downgrade catalogue — 2026-08-15

Every place in the query planner where an optimization is *attempted*, and on
failing a guard falls back to a slower-but-correct path with **no error, no
warning, and no trace**.

## Why this document exists

A coverage run found `df_graph/vid_lookup_join.rs` at **0 of 441 executed lines**
— while a dedicated 15-test suite written for that operator was passing. Those
tests assert result *bags*, and the operator's whole contract is to be
bag-identical to the `HashJoinExec` it replaces. So no bag assertion could ever
distinguish *"the optimization fired and is correct"* from *"the optimization
silently fell back"*. It sat behind six `return Ok(None)` guards for four months
and nothing turned red.

Two of those guards excluded the operator's own use case. Once fixed it measured
**2.5× on INNER and 2.1× on LEFT**, with a 20,000-row probe scan disappearing from
the plan.

The obvious response is to test that operator. The right response is to ask **how
many others look like it** — because the hazard is structural, not incidental:

> An optimization with a correctness-preserving fallback is *by construction*
> invisible to result-only tests.

This catalogue is the answer. There are **29**.

**And the pattern is not confined to the planner.** While this document was being
written, `crates/uni/benches/pushdown_performance.rs` was found to have no
`[[bench]]` stanza, so Cargo auto-discovered it under the default libtest harness.
Its `criterion_main!` main became dead code, libtest found zero `#[bench]`
functions, and `cargo bench --bench pushdown_performance` exited **RC=0 reporting
`0 measured; ok`** — a green benchmark that measured nothing, for as long as the
file existed. Two documents had recorded it as *failing*, which is the louder and
more tractable defect it was not. Deleted 2026-08-15, with a stanza-coverage check
added to `nightly.yml` because a build check cannot catch it: the file compiles.

## Scope and method

Surveyed 2026-08-15 across `crates/uni-query/src/`. Line numbers are as of that
date; treat them as starting points, not anchors. Sites 1, 2, 20 and 21 were read
directly during the survey; the remainder come from the sweep and should be
re-confirmed when worked.

A site qualifies if **all** of these hold:

1. It attempts something faster or more specific.
2. On failure it produces a *correct* result by another path.
3. The bail emits nothing a test or an operator could observe.

Correctness-driven bails that change results are **not** silent downgrades and are
excluded — those are ordinary control flow.

---

## The structural finding: the activation gate covers only half of this

`crates/uni/tests/common/plan_shape/` asserts which **physical operator** ran, via
`ProfileOutput::runtime_stats`. That is the right instrument for the case that
motivated it — and it cannot see a large part of this catalogue.

| class | witnessable by | sites |
|---|---|---|
| **Physical-operator selection** | `plan_shape` / `PROFILE` — the #177 gate | 1, 6, 7, 9, 10, 16, 18, 19, 22 |
| **Logical-plan rewrite** | *nothing today* — except 2 and 5, which surface indirectly via `FusedIndexScanWrapped`'s runtime name | 2, 3, 4, 5, 14 |

Sites **3** (`rewrite_node`'s Scan arm) and **14** (`try_fuse_set_items`) have **no
observable of any kind**. A logical rewrite changes which `LogicalPlan` node is
emitted and never introduces a distinct physical operator name, so
`assert_plan_uses` has nothing to match on.

**The gate is necessary and not sufficient.** Closing #177 to zero would still
leave five sites unwitnessed.

`EXPLAIN` does not rescue them either: `ExplainOutput.plan_text` is
`format!("{:#?}")` over the `LogicalPlan`, so it is not ordering-stable across
processes and was explicitly rejected as an activation signal in
`dqp/lever.rs:26-34`.

---

## The catalogue

Ranked by hazard, where **hazard = the fallback is result-identical**, so no
result-based test can distinguish fired from skipped. Observability column:
**none** = completely silent; **partial** = some bails covered, others not;
**yes** = every bail observable.

### Tier 1 — result-identical, completely silent

| # | site | guards | falls back to | obs. |
|---|---|---|---|---|
| 1 | `try_emit_vid_lookup_join` — `df_planner.rs:4279` | 6 | `HashJoinExec` (`:4232`) | none |
| 2 | `procedure_call_fusion_kind` — `planner.rs:10710` | 7 | unwrapped `ProcedureCall`; the fused name never reaches EXPLAIN/PROFILE | none |
| 3 | `rewrite_node` Scan arm — `planner.rs:10473` (decision `10485-10521`) | 3 | plain `LogicalPlan::Scan` | none |
| 4 | `into_fusion_kind` — `planner.rs:10787` | 1 (`_ => None` over a `#[non_exhaustive]` enum) | plain `Scan` | none — the comment itself says "silently passed through" |
| 5 | `rewrite_node` Sort arm — `planner.rs:10648-10680` | 3 | plain `Scan` + `Sort` | none |
| 14 | `try_fuse_set_items` — `planner.rs:10352` | 6 | unfused `Set { Create }` | none |
| 15 | `merge_pattern_property` / `merge_into_elements` / `set_map_property` — `planner.rs:~10405/10420/10450` | 3 `false` returns | feeds 14 | none |
| 18 | `merge_single_node_fastpath` — `write.rs:1568` | 6 | general per-row MERGE, one `LogicalPlan` **per row** | none |
| 19 | `merge_relationship_fastpath_shape` — `write.rs:1621` | 5 | general per-row traversal `LogicalPlan`. Its doc comment at `:1611` puts the general path at **~19× the bulk CREATE of the same edges** — note that baseline is bulk CREATE, not the fastpath, so it bounds the stake rather than measuring the downgrade | none |
| 8 | `unify_join_key_types` — `df_planner.rs:8464` | 4 | propagates to 7 | none |
| 9 | `build_indexed_property_pushdown` — `df_planner.rs:1728` | ~6 | unindexed scan + residual `FilterExec` | none — two `.ok()?` calls discard their errors |
| 12 | `materialize_unwind_source` — `df_planner.rs:7809` | 3 | full scan + `FilterExec` | none |
| 22 | `try_extract_vid_eq` / `extract_vid_from_physical_filter` / `scalar_to_u64` — `scan.rs:1236/1207/1264` | ~5 | full label scan; no `_vid` Lance pushdown or L0 short-circuit | none |
| 23 | `extract_vid_from_cypher_filter` / `resolve_vid_value` / `detect_bound_target` — `df_planner.rs:1609/1688/2428` | ~9 | scan without VID pushdown | none |
| 25 | `equality_target_column` / `column_of_scan_variable` / `is_constant_or_param` — `planner.rs:~10800+` | ~4 | feeds 3 | none |
| 26 | predicate pushdown to Scan/Traverse — `planner.rs:6328-6345` | 2 + a reachability walk | predicate stays in the `Filter` node | none |
| 27 | `push_predicates_to_apply` — `planner.rs:8287` | conditional | `Apply.input_filter` stays `None`; the subquery runs per unfiltered row | none |
| 28 | `rewrite_predicates_using_indexes` — `planner.rs:6392` | 4 nested `if let` | predicate un-rewritten; generated-column index unused | none |
| 6 | `FusedIndexScan` → `Scan` decay — `df_planner.rs:794-805` | 0 — **unconditional, by design** | generic scan | none |

Site 6 is listed for completeness: the decay is deliberate and documented
(Phase 5a-impl), because Lance's per-branch `base_paths` reads already produce
correct fused results. It belongs here only because the *name* disappears from the
plan, so nothing downstream can tell fusion was engaged.

### Tier 2 — partially observable

| # | site | guards | falls back to | obs. |
|---|---|---|---|---|
| 7 | `try_plan_cross_join_as_hash_join` — `df_planner.rs:4067` | 4 | `FilterExec(CrossJoinExec)` | **partial** — one `tracing::debug!` at `:4133` that fires *after* three of the four bails, and describes classification rather than the bail |
| 11 | `materialize_unwind_source_field` — `df_planner.rs:7853` | 7 | full scan + `FilterExec` | **partial** — `warn_unpushable_unwind_once` (`:7709`, one-shot `AtomicBool`) covers 3 of 7 |
| 13 | `merge_unwind_in_filters` — `df_planner.rs:8167` | 2 | `rebuild_unmodified` | **partial** — `tracing::debug!` at `:8230` fires only on the equi-pair path |
| 16 | `compile_pattern_exists` → `compile_exists` — `expr_compiler.rs:453-461`, fn at `:836` | ~7 `Err` bails | per-row `ExistsExecExpr` (vectorized → row-at-a-time) | **partial** — a single `log::debug!` at `:456` covering all reasons in one message, and via `log` rather than `tracing` |
| 20 | batch property prefetch — `mutation_common.rs:223-231`, `237-245` | 2 | per-row property fetch | **none** — see candidates below |

### Tier 3 — deliberate and documented

| # | site | note |
|---|---|---|
| 17 | `RowDedupState::try_new` — `locy_fixpoint.rs:378` | **`tracing::warn!` at `:389`.** A model site. |
| 24 | `maybe_add_edge_structural_projection` — `df_planner.rs:2512` | 3 bails, all correctness-driven. Low hazard. |
| 29 | `is_monotonic_aggregate` — `locy_fold.rs:63` | falls back to `default_monotonicity_oracle`; deliberate and documented |

---

## The template — two sites already do this right

The convention does not need inventing. It exists in this codebase:

- **`build_in_pushdown` (`df_planner.rs:7933`) — the model.** All 7 bails emit a
  `tracing::debug!` carrying a `reason` field, plus a success debug at `:8034`.
  You can tell from a log whether the pushdown fired *and why not* when it did not.
- **`RowDedupState::try_new` (`locy_fixpoint.rs:378`)** — `tracing::warn!` at `:389`
  when it drops to the legacy dedup path.

## Precedent — three fallbacks already made loud

This direction is established, not newly proposed:

| site | what changed |
|---|---|
| `PatternExistsExecExpr::resolve_predicate_value` — `pattern_exists.rs:129` | was an `Option` with a silent drop; now fails closed. Explicit "loud rather than silent" comment at `:138` |
| `run_hybrid_search` dense arm — `search_procedures.rs:1649` | auto-embed failure silently degraded hybrid → FTS-only; now `?`-propagated |
| `locy_complement.rs:35-36`, `:491` | documents removing a "degrade to pass-through" fallback |

---

## Two candidates that may be defects

Both are **verified present**. Their *consequences* are **not** verified, so they
are recorded as candidates rather than asserted as bugs.

### Site 20 — a dropped error

`mutation_common.rs:223-245` wraps the batch property prefetch in `if let Ok(...)`,
discarding the error value entirely:

```rust
if let Ok(label_results) = pm
    .get_batch_vertex_props_for_label(&vids, &label, ctx)
    .await
{ ... }
// Batch errors fall through to per-row fallback (correctness preserved).
```

The comment is accurate and the behaviour is deliberate. But if that batch call
began failing on *every* invocation, the only symptom would be slowness — which is
precisely the `VidLookupJoinExec` shape. At minimum this wants a `tracing::debug!`
naming the error.

### Site 21 — a possible *semantic* downgrade

`plan_binary_udf` (`expr_compiler.rs:1892`) returns `Ok(None)` when the named UDF
is not registered:

```rust
let Some(udf) = self.state.scalar_functions().get(udf_name) else {
    return Ok(None);
};
```

The caller falls back to `compile_standard` — native Arrow comparison rather than
the Cypher-aware path. **If those two differ semantically, this is a correctness
bug rather than a performance one.** Whether they differ is unverified. That
question deserves its own investigation; it is not answered here, and this
document deliberately does not guess.

---

## Confirmed defects of this class, found later by LDBC SNB — 2026-08-26

Both were found by running LDBC SNB Interactive against SF1, and neither is a
planner fallback: they are a **different mechanism reaching the same outcome**.
No guard is involved, nothing is attempted-then-abandoned. A comparison simply
answers `false` where it should answer `true`, and every layer above it behaves
correctly given that answer. So the catalogue's own qualification test — "it
attempts something faster, falls back correctly, and says nothing" — does not
match either of them, while the *symptom* is identical: a well-formed wrong
answer with no error.

That is worth recording here, because the document's premise is that
result-only tests cannot see a silent fallback. These two show that result-only
tests also cannot see a silent *comparison* failure unless the fixture makes the
true and false answers differ — and against a graph where the affected filter
matched nothing, "0 rows" looked like a legitimate result.

### Backward adjacency after a bulk load

`MATCH (t:Tag)<-[:HAS_TAG]-()` returned **zero rows** in the process that had
just loaded the data, and the correct 24 311 after reopening the same graph. An
*undirected* aggregation in the same run was correct throughout. Fixed in
`uni-query`'s planner, which inferred traversal endpoint labels from the
destination side regardless of direction; the regression test is
`crates/uni/tests/common/storage/incoming_after_bulk_load.rs`.

**Still open next door:** `crates/uni-bulk/src/bulk.rs` warms adjacency with
`let _ = ... .warm_adjacency(...)`. A failure there is invisible and leaves
backward rows unreachable for the life of the process — Site 20's shape exactly.

### Graph entities were never members of a list

`n IN [n]` — the same node, the same row — was **false**, while `n = n` was true.
Every `IN` over a list of nodes or edges answered false.

The mechanism is a representation mismatch rather than a guard.
`translate_in_expression` (`crates/uni-query-functions/src/df_expr.rs`) rewrites a
node/edge variable on the *left* of `IN` down to its bare `_vid` / `_eid` `Int64`
column, because a list injected from a parameter holds ids. It does not rewrite
the right side, so a list built inside the query still holds whole entities, and
`_cypher_in` compared `Int` against an entity. `=` never took that path, which is
why equality stayed correct and only `IN` was wrong.

Fixed in `_cypher_in` via `entity_aware_eq`, the one place that can see both
sides. Two details cost a full cycle each and are worth carrying forward:

- **An entity has two live encodings.** A projected entity arrives as a
  `Value::Map` carrying `_vid`, not as a typed `Value::Node`. A first fix that
  handled only the typed variants was a complete no-op, and every test stayed
  red. `cypher_eq` splits the same way — it grew a `Map`/`_vid` identity arm and
  never grew the `Node` one, so entities there compared by vid **and** labels
  **and** the whole property map.
- **`EXPLAIN` did not settle this one.** Both the working and failing forms
  produced the same `In { expr, list }` node; the divergence was below the plan,
  in what the UDF received at runtime. What settled it was printing the actual
  `Value`s inside `_cypher_in` — after three hypotheses that were each plausible,
  each consistent with the plan, and each wrong.

Regression tests: `crates/uni/tests/common/cypher_read/entity_identity_in_list_test.rs`.
LDBC IC3 went from `0 rows` to matching, which is what the non-vacuity gate in
the runner is for.

---

## How to work this list

Per site, following the retrofit recipe in
`crates/uni/tests/common/plan_shape/mod.rs`:

1. **Physical-operator sites** — add an `assert_plan_uses` proof plus its
   **mandatory negative twin** (a query outside the guards, asserting the operator
   is absent *and* the fallback present), then flip the row in
   `plan_shape/registry.rs` and lower `MAX_UNPROVEN`. Tracked by **#177**.
2. **Logical-rewrite sites** — no instrument exists. Either add a `tracing::debug!`
   on the `build_in_pushdown` model, or give the rewrite a distinguishable name in
   the plan. This is unbuilt work, not just unwritten tests.
3. **Do not delete a site because it never fires.** `VidLookupJoinExec` was nearly
   deleted as born-dead and turned out to be worth 2.5×. *"Never ran"* and *"not
   worth running"* are different claims, and only the first is answerable by
   reading.

## Related

- `docs/perf/coverage-map-2026-08-14.md` — the run that started this
- `docs/testing/teeth-2026-08-13.md` — the sibling ledger, for oracle sensitivity
- #177 (ratchet `MAX_UNPROVEN`), #179 (the gate's own blind spot: `VidLookupJoinExec`
  hides its probe scan from `runtime_stats`)
