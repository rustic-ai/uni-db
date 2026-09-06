# Issue review: one-off or class? — 2026-09-01

Every open issue, plus defects found and not filed, reviewed against one
question: **is this a single site, or one face of a mechanism with others?**

The distinction decides the work. A one-off is fixed where it is found. A class
fixed at one site comes back — which this project has now watched happen enough
times to treat as the default expectation rather than bad luck.

Written after the LDBC IC5 round, in which three separate fixes each turned out
to leave siblings untouched: #218's batching missed its neighbour in the same
file, #219's anchoring does not reach MERGE or Locy, and #221's selectivity
switch does not reach the vertex path. All three were found by auditing *after*
the fix, not before.

A fourth case is sharper and is recorded in Class 8: #219's anchoring rewrite
**introduced a silent wrong answer** by turning `Outgoing` patterns into
`Incoming` ones, where a fourth derivation of edge orientation read traversal
order directly. Its soundness argument had verified one of the four derivations.
That is the review's own strongest evidence for doing this exercise before
shipping a fix rather than after.

---

## Method, and its limits

Each class below was established by reading the code, not by grouping issue
titles. Where a claim is a source read that has not been reproduced, it says so.
Where an agent's suspicion turned out to be wrong on inspection, the correction
is recorded rather than the suspicion.

Two things this review does **not** establish:

- **Reachability.** For most sites below I can show the code handles one case and
  not another; I usually cannot show a user query that reaches the gap. Those are
  marked *unverified*.
- **Impact ordering.** Ranking is by reasoning about table sizes and call
  frequency, not by measurement, except where a number is quoted.

---

## Status ledger: the tracker is misleading right now

**22 of the 57 open issues are already fixed in unpushed commits.** The branch
`feat/test-harness-track-e` is 139 commits ahead of `origin/main` with no
upstream, so no closing keyword has ever reached GitHub.

Fixed but reading OPEN: #175, #184, #185, #186, #187, #188, #189, #190, #191,
#192, #194, #196, #197, #201, #202, #203, #207, #208, #210, #211, #212, #218.

Partially fixed (carry `Refs`, no `Fixes`): #193, #198, #209, #219, #221.

Anyone reading the tracker today gets a materially wrong picture. This is the
single highest-value action in this document and it is not a code change.

---

## Class 1 — Fail-open: a failure is swallowed and a wrong answer continues

**27 sites. 17 produce silent wrong answers.** The largest and most severe class
found.

The shape: an error on a read, decode, index, or merge path is logged (or not)
and execution continues with a substituted default — `Null`, an empty result,
`0`, the older value, "not found". Nothing surfaces. The query returns.

### Why it is a class and not a style

The handling is **inconsistent within the same files**, which means these are
decisions nobody made rather than tradeoffs someone chose:

- `adjacency_manager.rs:462-471` fails open; `:795-810` and `:723-740` in the
  same file fail closed.
- `index.rs:206` (`get_vid`) and `:324` (`resolve_uids`) fail open;
  `resolve_all_vids` at `:355` deliberately does the opposite.

Several sites already document the defect in their own comments:

- `arrow_convert.rs:793` — *"This is a silent wrong answer if the column holds
  real data."*
- `adjacency_manager.rs:462` — the doc two lines above promises "returns `None`
  if a version read fails, which callers treat as 'assume stale'". The code
  contributes **0 to the cache-validity stamp** instead, so **a stale adjacency
  CSR is served as fresh**: missing or extra edges in traversals.

### The worst instances

| site | failure | silent consequence |
|---|---|---|
| `executor/read.rs:2222` / `:2248` | `id()` / `elementId()` on a `Value::Node` | returns `Null`; the `_id` arm returns the *string* `"Vid(7)"`, so `id(n) = 7` is false |
| `df_graph/scan.rs:1448` | `build_property_column_static` errors | **an entire all-NULL property column**, with no log at all |
| `writer.rs:2184`, `:2408` | I/O error on the ext_id uniqueness probe | indistinguishable from "not found" — the constraint **admits a duplicate** |
| `df_graph/common.rs:1883` | list decode fails | every row gets a distinct grouping key, so **DISTINCT / GROUP BY stop deduplicating** |
| `storage/index.rs:206`, `:324`, `json_index.rs:78` | index open fails | reads as "no such UID" — MERGE then **creates a duplicate** |
| `value_codec.rs:213` | CRDT deserialize fails | substitutes `GCounter::new()`, i.e. **counter value 0** |
| `storage/manager.rs:2545` | sparse index cannot be opened | sparse search returns **zero hits** |
| `storage/manager.rs:1284` | semantic compaction fails | returns `SemanticCompactionReport::default()` — **a fabricated report of zero work** |
| `property_manager.rs:343`, `:1629`, `:2114` | CRDT merge fails | silently keeps the **older** value, no log |
| `fork/registry.rs:511` | fork schema overlay fails to load | fork silently reads with the **parent's** schema |
| `writer.rs:806` | WAL replay meets a wrong-dimension vector | **nulls it**; destructive on recovery |
| `l0.rs:440`, `:462` | CRDT variant mismatch | last-writer-wins, **discarding merged CRDT state** |

Recall-loss variants, which are wrong answers rather than slowness:
`writer.rs:4174` (MUVERA FDE encode failure leaves a vertex permanently
invisible to accelerated vector search), `manager.rs:523`
(`REFINE_CANDIDATE_CAP`), `search_procedures.rs:369` (`MULTIVECTOR_OVER_FETCH`,
`MULTIVECTOR_MAX_CANDIDATES` truncate the set an exact re-rank sees).

### Silent-slowness variants

`ensure_default_indexes` (`main_edge.rs:270`) warns and continues when
`create_scalar_index` fails — so a store can silently have **no index**. Its
siblings do the same: `main_vertex.rs:326`, `vertex.rs:446`, `delta.rs:517`.
`index.rs:186-198` is worse: `.ok()` with no log, justified as "Non-fatal:
filter pushdown works without index".

Index *status* is also fail-open: `index_rebuild.rs:483`, `:501` and
`writer.rs:6074` all `let _ = update_index_metadata(...)`, so an index can read
`Active` while actually `Stale` or `Failed`. `writer.rs:6083` then warns-and-drops
a failed rebuild schedule, so indexes marked `Stale` are **never rebuilt** and
nothing records it. This is the same shape as the previously-fixed "index build
status Online lie".

### Suggested handling

File as a class with a tiered instance list. The tiering matters: a fix pass that
starts at the top of the table removes wrong answers, one that starts at the
bottom removes log noise.

Two sub-decisions worth making once, project-wide, rather than per site:
1. Does a failed *read* ever return a default? (Proposed: no. Return `Err`.)
2. Does a failed *index* or *status* write ever go unrecorded? (Proposed: no.
   Record `Failed`/`Stale` durably, which the index-honesty work already
   established as the contract.)

---

## Class 2 — An entity has two encodings, and identity is hand-rolled ~30 times

A vertex reaches an expression as either a native `Value::Node` / `Value::Edge`
or a `Value::Map` carrying `_vid` / `_eid` — or `_id`, which serde renders as the
**string** `"Vid(7)"`. Code that handles one encoding treats the other as "not an
entity": an empty result, not an error.

### The finding that reframes this

`Value::entity_vid` (`value.rs:504`) exists to be the single definition. Its
rustdoc says *"One definition, because five hand-rolled ones disagreed."*

**It has four call sites.** Roughly **20 extractors and 10 classifiers** still
hand-roll identity beside it. The consolidation was written and never applied —
so this is not "five bugs were fixed", it is "a sixth implementation was added".

Only `entity_vid` understands the `_id` = `"Vid(7)"` form. Every other site
misses it.

### Instances already filed

#189, #190, #191, #193, #215, #216, #217.

### Instances found and not filed

| site | gap |
|---|---|
| `executor/core.rs:88`, `:251` | `COUNT(DISTINCT n)` has **no entity arm at all** — dedups by structural `Value` equality, so the same node as `Node` and as `Map` counts twice |
| `locy_validate.rs:133` | a Map-encoded node falls to `format!("{other:?}")` on a `HashMap` — an **order-nondeterministic join key**, differing run to run |
| `df_udfs/sort_key.rs:276`, `:313` | `unwrap_or(0)` on a non-Int id, so **every such node ties in ORDER BY**; a `_id`-only map also sorts in a different type class than the same node as `Value::Node` |
| `uni-algo/.../random_walk.rs:82` | accepts string or int only — passing a matched node errors "Invalid Vid format" |
| `locy_fixpoint.rs:3757` | `Value::Map` with `_vid` returns `None`, so the row **silently drops out** of the fixpoint join |
| `locy_eval.rs:698` | `values_equal_for_join` handles Node/Node and Edge/Edge; a mixed Node-vs-Map pair **never joins** |
| `mutation_common.rs:260`, `:298` | requires `_vid` to be `Value::Int` specifically — any other form returns `None`, and the mutation **silently targets nothing** |
| `expr_eval.rs:253-262` | `eval_in_op`'s raw-id fallback is Map-only; `Value::Node` on the left never gets it, and there is no `_eid` arm at all |
| `df_udfs.rs:504` | `entity_identity`, backing the `id()` UDF, misses `_id` |
| `core.rs:819` / `sort_key.rs:148` | duplicated rank logic that must stay in sync **by hand**; the comment says so |

### Corrected, not propagated

An audit flagged `df_udfs.rs:6099` `distinct_key` as producing different keys for
the two encodings. **It does not.** `Vid`'s `Display` writes `self.0`
(`id.rs:99-103`) and `Value::Int`'s writes `{i}` (`value.rs:678`); both render
`7`, so both arms yield `\0n7`. Recorded because the suspicion was reasonable and
propagating it would have been a false finding.

### The structural gap

**No invariant enforces which encoding a path produces.** The comment at
`df_udfs.rs:1261` asserts "a structural projection always yields the map form",
but `pattern_comprehension.rs:677` and `:1040` build native `Value::Node` columns
directly. Two producers of the native form coexist with map-only consumers, and
nothing type-checks the pairing.

### Suggested handling

Not "fix the sites". Make the encoding unrepresentable-in-two-ways at the
boundary, or make `entity_vid` the only way to ask — then the remaining sites
fail to compile rather than failing to match. A class issue listing 30 sites
invites 30 patches and a 31st site next quarter.

---

## Class 3 — Unbounded materialisation, and a memory pool almost nothing reserves from

### Two corrections to what the existing issues assert

**1. A memory pool IS configured.** `GreedyMemoryPool::new(max_query_memory)` at
`executor/read.rs:663-676`, installed at `:486`, and again on the shared session
template at `uni/src/api/mod.rs:2089-2104`. #185/#198's framing that nothing
accounts is out of date.

But **no custom operator reserves from it**: `MemoryConsumer`, `try_grow` and
`MemoryReservation` return **zero hits** across `uni-query` and `uni-store`.
Every `df_graph` operator allocates outside the pool. Only stock DataFusion
`SortExec`, `AggregateExec`, `FilterExec`, limits and window aggregates account.

**2. A disk manager IS present, and two comments say otherwise.**
`read.rs:650-655` and `api/mod.rs:2074-2077` state *"no disk-spill path is
configured, so neither pool can spill"* — and that claim is **the stated
justification for choosing `GreedyMemoryPool` over `FairSpillPool`**.
`scan.rs:527` states the opposite and is correct: DataFusion 53.1's
`RuntimeEnvBuilder` defaults to `DiskManagerMode::OsTmpDirectory`.

A wrong comment is driving a configuration decision. That is worth its own issue
regardless of which pool is right.

**3. `VidLookupJoinExec` replaced the one join that accounted.** `plan_shape.rs:129`
asserts by design that the plan avoids `HashJoinExec` in favour of
`VidLookupJoinExec` — trading a pool-accounted, spillable operator for an
unaccounted, unspillable one that returns the whole join as a single
`RecordBatch` (`vid_lookup_join.rs:364-427`).

**4. #185 is half-fixed.** `estimate_result_bytes` (`impl_query.rs:307-327`) now
recurses into containers and strings, so the `size_of_val + 64` estimator the
issue describes is gone. The *post-hoc* half — it runs after `executor.execute`
— remains.

### Instances, ranked by OOM likelihood

| site | what is materialised | bound |
|---|---|---|
| `executor/read.rs:717-749`, `:905`, `:1462-1493` | whole result → `Vec<RecordBatch>` → one `HashMap<String, Value>` **per row**; `execute_stream` is **not a stream** — it drains fully and emits one item, so the cursor's incremental check at `impl_query.rs:781` fires after everything is resident | Cypher `LIMIT` only |
| `traverse.rs:4070-4092`, `:4304` | variable-length traversal has **no `Chunking` or `Slicing` state** (single-hop got both); accumulates full vid+eid paths for every path from every source | nothing |
| `backend/lance.rs:392`, `manager.rs:340`, `:1701` | storage scan collects the whole table, then `concat_batches`. **`ScanRequest::limit` exists (`types.rs:644,:691`) and no query path ever sets it** — verified: the only callers are compaction and a comment about a removed one. `MATCH (n:Person) RETURN n LIMIT 1` reads all of `Person` | nothing |
| `vid_lookup_join.rs:364-427` | build side fully collected, probe chunks re-concatenated into one batch, no `batch_size` slicing anywhere in the file | nothing |
| `adjacency_manager.rs:739` | CSR warm reads the **whole** adjacency table, lazily on first traversal of an edge type | edge count |
| `traverse.rs:2233-2258` | schemaless traversal buffers all input, then the whole edge type as `HashMap`s | `TRAVERSE_PUSHDOWN_MAX_SOURCES`, which *caps the pushdown* — above 1024 sources it loads everything |
| `scan.rs:1500-1533` | #214 partly closed: `Slicing` exists, but `RecordBatch::slice` is zero-copy so the whole result is still built first. Mitigated for downstream spill, not for scan peak | nothing |
| `apply.rs:654-884` | full input, then a fully-collected subplan result **per row** | nothing × nothing |
| `mutation_common.rs:697-710`, `mutation_foreach.rs:185` | whole input collected, then a `HashMap` per row | nothing |
| `recursive_cte.rs:299-341` | every iteration's rows as `Value`s, plus a `HashSet<Value>` of everything seen, plus the frontier as a `Value::List` parameter | `MAX_ITERATIONS` |
| `shortest_path.rs:481` | BFS depth + predecessor maps over the reachable subgraph | graph size |
| `optional_filter.rs:457` | all buffered NULL-recovery rows concatenated at end of stream | input size |
| `procedure_call.rs:912` | plugin stream drained and concatenated | what the procedure yields |

### #213, confirmed and refined

`SortExec::new` is called with no fetch at `df_planner.rs:5612` and `:6179`; the
limit is a separate `LocalLimitExec` above it. But this operator *does* reserve
and *can* spill, so it degrades to disk rather than dying — which is exactly why
swapping to TopK regressed. The issue's own diagnosis holds; the "nothing can
spill" belief around it does not.

---

## Class 4 — Fixed constants standing in for a cost decision

**12 sites.** One was fixed today (`find_props_by_eids`, #221) and the fix is
itself incomplete.

### The root: there is no cost model

`planner.rs:8908`:

```rust
fn estimate_costs(&self, _plan: &LogicalPlan) -> Result<CostEstimates> {
    Ok(CostEstimates { estimated_rows: 100.0, estimated_cost: 10.0 })
}
```

The plan is ignored. Constants are returned. It feeds `EXPLAIN` output and
nothing else.

**No graph operator overrides `ExecutionPlan::statistics()`** — `grep "fn statistics"`
across `uni-query` returns nothing — so DataFusion's own cost-based rewrites run
on unknown cardinality for every graph node.

What *does* exist is one cheap exact primitive: `StorageBackend::count_rows(table, None)`
(`traits.rs:123`), metadata-only on Lance, already called from six places.

And one in-repo precedent that does it right: `bitmap.rs:14-34` computes actual
density (`ids.len() / range`) rather than switching on a raw count.

### Instances

| rank | site | decision | signal available |
|---|---|---|---|
| 1 | `scan.rs:814` + `vid_lookup_join.rs:412` (`MAX_VIDS_PER_CHUNK = 10_000`) | **the vertex twin of the edge bug fixed today, unfixed.** Its own comment already says *"A selectivity-aware choice would beat a fixed constant"* | yes — `count_rows` + span, both in hand |
| 2 | `main_edge.rs:694` (`VID_CHUNK = 8192`) | chunks `src_vid/dst_vid IN (…)` over the **same 17.3M-row table just fixed**; a full-scan sibling arm exists at `:685` and the choice is made by which `match` arm you land in | yes — `prefers_full_scan` is 130 lines above it |
| 3 | `traverse.rs:2225` (`TRAVERSE_PUSHDOWN_MAX_SOURCES = 1_024`) | its comment derives the crossover as a **ratio** (10k vids / 100k edges) and codes it as an **absolute** | yes |
| 4 | `write.rs:1838` (`MERGE_SCAN_CHUNK = 1000`) | N chunked scans vs one pass over a per-label table | yes — per-label `count_rows` |
| 5 | `manager.rs:523` (`REFINE_CANDIDATE_CAP`) | vector refine budget | partly — **recall loss**, a wrong answer |
| 6 | `search_procedures.rs:369,:371` | ANN over-fetch and re-rank cap | partly — **recall loss** |
| 7 | `locy_fixpoint.rs:1428` (`DEDUP_ANTI_JOIN_THRESHOLD = 300`) | HashSet vs vectorized join; consults candidates only, though the crossover depends on the existing set too | yes |
| 8 | `df_planner.rs:8077` (`MAX_UNWIND_IN_PUSHDOWN_VALUES`) | IN-list pushdown vs HashJoin | partly; does emit a one-shot warn, so at least observable |
| 9 | `config.rs:768` (`fork_index_build_threshold`) | absolute row count with no reference to primary table size | yes |

Also noted: `DEFAULT_MAX_HOPS` is **128** in `nfa.rs:8` and **100** in three
separate places in `planner.rs` — an unexplained inconsistency, outside this
class but worth a look.

### The uncomfortable part

The fix landed today (`prefers_full_scan`, `main_edge.rs:528-560`) is an
instance fix in a class I did not audit until afterwards. Its own rustdoc already
concedes *"The constants are fitted to one dataset on one machine… a selectivity
estimate from real statistics would beat them."* Items 1 and 2 above are the same
defect within arm's reach of it.

---

## Class 5 — Plan shape is syntax-driven (#224)

Already filed as a class. Instances: #219 (end-bound case, fixed), #225 (MERGE's
separate hand-rolled copy, executed per input row), #226 (Locy plans every rule
body with an empty scope, so anchoring can never apply), #206 (unanchored
pattern comprehensions fall back to a sub-plan per outer row), QPP source
selection is positional, and comma-separated paths are never reordered.

Anchor selection exists at **two** physical sites (`pattern_exists.rs:548`,
`pattern_comprehension.rs:884`) and **no** logical one. The codebase knows how;
the planner does not ask.

This class and Class 4 share a root — no cardinality information — and should
probably be sequenced together.

---

## Class 6 — Per-item storage round-trips (#220)

Already filed as a class with 8 open sites. Two fixed today. Nothing to add
except that the audit which found it ran *after* the first fix shipped.

---

## Class 7 — Optimizations invisible to result-only tests

#177 states the mechanism better than I can:

> an optimization with a correctness-preserving fallback is by construction
> invisible to result-only tests

**32 of 35 operators are still `Unproven`.** `VidLookupJoinExec` sat at 0 of 441
executed lines for four months behind six silent `return Ok(None)` guards while a
dedicated 15-test suite passed.

Related faces: #179 (the operator hides its probe scan from `PROFILE`, so the
gate that depends on `runtime_stats` cannot see it either), #223 (main-edges
scans are invisible to `scans_reported`, which is why #218's shape test could not
be written), #195 (no benchmark exercises the index-consultation counters),
#205 (vacuously-passing tests), #176 (`ForeachExec`: 154 lines that no query can
reach, because the grammar has no `FOREACH` rule).

Four more non-discriminating checks were caught in the IC5 round and recorded on
#205. The rule that caught all of them: **before believing a result, confirm the
code under test actually ran.**

This class is the reason several of the others went undetected for so long, which
argues for sequencing it early rather than treating it as hygiene.

---

## Class 8 — Relationship endpoint orientation

**Verdict: one mechanism with four faces, and four more still live.** The
strongest class finding of the review, and the only one that turned up a
regression introduced during this round.

### The mechanism

**An edge's orientation is not carried with the edge value. It is re-derived at
each consumer from local context, by four different mechanisms, and nothing
checks them against each other.**

| # | mechanism | where | correct by construction? |
|---|---|---|---|
| A | plan-time variable rewrite — `endpoints_for_direction` | `planner.rs:11349-11380`, consumed `:11404`, `:11487` | yes, for single-hop MATCH-bound |
| B | per-row `_fwd` column | produced `traverse.rs:817`, `:2828`; consumed `df_planner.rs:6473` | yes, undirected single hops only |
| C | runtime probe from the eid — `resolve_stored_edge` | `df_graph/mod.rs:782-853` | **yes — it asks storage** |
| D | hand-rolled from whichever two vids are in scope | `df_planner.rs:6462`, `pattern_comprehension.rs:373-386`, `bind_fixed_path.rs:409` | **no** |

Only C is correct by construction. A, B and D are reconstructions.

The four filed issues are four faces of that one fact:

- **#187** — A absent after a projection dropped the endpoint variables, so the
  call fell to the UDF, which had neither the vid nor the node. Loud.
- **#188** — A had no answer for `Direction::Both`, so B was invented. Loud.
- **#193** — D used where B was needed. **Silent.**
- **#201** — the UDF's node-scan succeeds only when the endpoint coincides with a
  pattern binding. Loud.

The remediation document already stated the class without naming it: #193 was
found only because a hydrated `startNode` disagreed with a `_fwd`-based test —
*"two paths to the same fact, one tested."*

### A regression this round introduced, found and fixed

`add_edge_structural_projection` (mechanism D) read traversal order and ignored
`direction`, so `MATCH (b)<-[r]-(a) RETURN r` reported `r.src = b`. Pre-existing
for patterns *written* `Incoming`.

`reversed_for_bound_anchor` — landed earlier in this same round — rewrites a
pattern written from its unbound end into the opposite direction. An `Outgoing`
pattern with its far end bound therefore began planning as `Incoming`, and **a
query that was correct-but-slow became fast-and-wrong.**

That commit's soundness argument verified `endpoints_for_direction` and checked
that `startNode`/`endNode` were preserved. They were. It never checked the edge
struct, because nothing indicated a fourth derivation existed. Verifying one of
four mechanisms and concluding safety is precisely the hazard this class
describes.

Fixed, with a test that asserts the relationship **value** for both spellings.
Confirmed discriminating: with the fix reverted it is the only one of the file's
27 tests that fails — every other test asks `startNode(r)`, which takes path A
and was always right.

### Four faces still live, all silent, none tested

1. **Schemaless undirected `RETURN r`** — `plan_traverse_main_by_type`
   (`df_planner.rs:3840`) has no counterpart to the `_fwd` request at `:3341`,
   yet calls the structural projection. #193 unfixed on that path. The exec side
   already supports it; only the request is missing.
2. **Pattern-comprehension relationship values** — `pattern_comprehension.rs:373-386`
   takes `src = anchor / previous target` and ignores `step.direction`, including
   `Both`. Its own doc comment at `:646-649` claims "Endpoints are in the edge's
   stored orientation"; the caller does not provide that. Feeds
   `endpoint_hydrate`, so `startNode` inherits it.
3. **`OPTIONAL MATCH`** — `single_hop_binding` (`planner.rs:11545`) ignores the
   `optional` flag. On an unmatched row `r` is NULL but the source variable is
   bound, so `startNode(r)` returns a node where Cypher requires NULL. No test
   covers it.
4. **`read.rs:2338-2384`** still returns the `{_vid}`-only stand-in that the UDF
   deliberately deleted in favour of a loud error — `id(startNode(r))` right,
   every property NULL.

### The counter-example worth copying

`traverse.rs:806-813` models `_fwd` as `Option<bool>` and hard-errors at
`:1205-1208` when the column is requested but untracked. Absence cannot be
mistaken for `false`. That is the shape the other three mechanisms lack.

### Suggested handling

Carry orientation *with* the edge value rather than re-deriving it, or make
mechanism C (the storage probe) the only answer and delete the reconstructions.
A class issue listing four sites invites four patches and a fifth derivation.

## Genuine one-offs

Reviewed and found to be single sites, not classes:

- **#174** migrate off `fxhash` — dependency hygiene; changes `HashMap` iteration
  order, so it needs its own PR.
- **#199** UNWIND of an outer-scope variable inside a subquery body fails to
  resolve the column — a scope-resolution bug; no sibling shape found.
- **#200** `flush_to_l1` barrier failed once under load — a single unreproduced
  flake. Note the project's own precedent that a "known flake" was twice a real
  bug, so this should not be closed for being intermittent.
- **#222** edge-property reads scan every edge type when L0 is cold — one call
  site, one fix (thread the edge type the traversal already knows).
- **#227** LDBC bench parameters admit most of the corpus — a harness decision,
  not a code defect. Worth settling before more latency work, since it decides
  whether IC5's remaining gap to 300 s is real.
- **#118, #119, #120, #122, #123** sparse/vector feature work — planned features,
  not defects.
- **#178** Hypothesis state machine for the Python bindings — a testing gap;
  belongs with Class 7 thematically but is a single deliverable.

---

## Actions

### Not a code change, and the highest value

**Push the branch.** 22 issues are fixed and reading OPEN. Every issue comment
written today says "local, unpushed" because nothing can be honestly closed.

### New issues to file

1. **Class: fail-open** — 27 sites, tiered, wrong answers first.
2. **Class: entity identity is hand-rolled ~30 times** — with the finding that
   `entity_vid` has four call sites and consolidated nothing.
3. **`COUNT(DISTINCT n)` has no entity arm** — `executor/core.rs:88,:251`.
   Separable and severe enough to stand alone.
4. **Locy join keys are order-nondeterministic for Map-encoded entities** —
   `locy_validate.rs:133`.
5. **The vertex path has the selectivity blindness just fixed for edges** —
   `scan.rs:814`, `vid_lookup_join.rs:412`, plus `main_edge.rs:694` in the
   already-touched file.
6. **Two comments assert no disk spill is configured, and drive the pool choice** —
   `read.rs:650`, `api/mod.rs:2074`, contradicted by `scan.rs:527`.
7. **`ScanRequest::limit` is never set on any query path** — `LIMIT 1` reads the
   whole table.
8. **`execute_stream` is not a stream** — it drains fully and emits one item, so
   the cursor's incremental memory check is decorative.
9. **Variable-length traversal has no chunking** — `traverse.rs:4070`, where the
   single-hop sibling got both `Chunking` and `Slicing`.
10. **No operator reserves from the memory pool** — zero `try_grow` sites; and
    `VidLookupJoinExec` deliberately replaced the one join that accounted.
11. **Class: an edge's orientation is re-derived four ways** — #187/#188/#193/#201
    are four faces of it, with four more live and untested (schemaless undirected
    `RETURN r`, pattern-comprehension relationship values, `OPTIONAL MATCH`
    `startNode(NULL)`, and `read.rs:2338`'s stand-in).

### Corrections to existing issues

- **#185 / #198** — a pool *is* configured; the gap is that no custom operator
  reserves from it. #185's estimator half is already fixed.
- **#213** — the operator can spill; the "nothing can spill" belief around it is
  wrong and comes from the two incorrect comments.
- **#214** — partly closed: `Slicing` exists, but the whole result is still built
  before the first slice.
- **#204** — IC5 answers at 345 s; #229 is now 64% of it.

### Sequencing

Class 7 first, because it is why the others hid. Then Class 1, because wrong
answers outrank everything. Classes 4 and 5 share a root (no cardinality
information) and should be planned together. Class 3 is the one most likely to
need design rather than patches.

---

## What this review could not determine

- Reachability for most Class 1 and Class 2 sites. The code paths are asymmetric;
  whether a user query reaches the asymmetry is usually unproven.
- Whether `locy_fixpoint.rs`'s timeout/iteration-limit interruption surfaces to
  the client. If it does not, every over-budget Locy query silently returns
  partial results — which would move it from Class 1 Tier 3 to Tier 1.
- Whether `property_manager.rs:1557`'s claim that "per-row fallback handles
  correctness" is actually true; the caller chain was not traced.
- Whether `EID_SCAN_CROSSOVER_RATIO` / `EID_SPAN_RATIO` transfer numerically to
  the vertex tables. They almost certainly do not — vertex tables are per-label
  and much narrower than the unified 17.3M-row edge table — and there is no
  vertex-side benchmark in the repo to calibrate against.
- Impact ordering within every class. Ranked by reasoning, not measurement.
- Plugin, CLI, bulk and CRDT crates were not audited for Class 1; roughly 25
  further warn sites exist there, mostly scheduler and CDC paths.

---

## Status — 2026-09-02

**The branch is pushed.** `origin/main` carries every commit this review was
written against (PR #232). The 22 issues listed above as "fixed but reading
OPEN" are now closed against shipped code. The ledger section is history.

**All 11 proposed issues are filed**, and six corrections were posted to
existing ones.

| proposal | filed as |
|---|---|
| Class: fail-open | #233 |
| Class: entity identity hand-rolled ~30 times | #234 |
| `COUNT(DISTINCT n)` has no entity arm | #235 |
| Locy join keys order-nondeterministic | #236 |
| The vertex path's selectivity blindness | #237 |
| Two comments assert no disk spill | #238 |
| `ScanRequest::limit` never set | #239 |
| `execute_stream` is not a stream | #240 |
| Variable-length traversal has no chunking | #241 |
| No operator reserves from the memory pool | #242 |
| Class: orientation re-derived four ways | #243 |

Corrections posted to #198, #205, #213, #214, #217 and #220.

### What re-verification changed

Eleven claims were re-read against the tree before filing. All held. Four came
back **sharper than this document states**, and the direction is consistent —
each was understated, not overstated.

- **`Value::entity_vid` has two call sites, not four** (`df_graph/common.rs:312`,
  `df_udfs.rs:1268`, plus one internal self-call). Against ~30 hand-rolled
  identity checks, the consolidation is weaker than "written and never applied";
  it is essentially unused.
- **`common.rs:1883` fails in two directions, not one.** The catch-all yields
  `ScalarKey::Utf8(format!("opaque@{row_idx}"))`, so within a batch every row
  gets a distinct grouping key *and across batches rows at the same index
  falsely collide*. This document recorded only the first half. The swallowed
  error is also `ArrayFormatter::try_new` failing, not a per-row list decode.
- **`prefers_full_scan` has exactly one caller.** #221's selectivity helper
  (`main_edge.rs:528-560`) is consulted only from the eid path at `:476`. The
  endpoint-vid arm 160 lines below it chooses between `VID_CHUNK` chunking and a
  full scan by which `match` arm the caller lands in. The fix and the unfixed
  sibling are in the same file.
- **#202 corrected the spill premise in its commit message and new comments and
  left both `GreedyMemoryPool` justifications standing.** The tree now holds the
  correct fact (`scan.rs:526-528`) and the incorrect one (`read.rs:648-652`,
  `api/mod.rs:2074-2079`) about a hundred lines apart, with the incorrect one
  load-bearing on a configuration decision.

One structural finding is new and belongs to Class 8:

**The orientation fix's test is discriminating and still cannot reach the
sibling path.** `add_edge_structural_projection` now consults `direction`, but
is complete only where `_fwd` is available, and `plan_traverse_main_by_type`
(`df_planner.rs:3828`) never requests it. So the test that was confirmed
discriminating — 1 of 27 fails when reverted — covers the schema'd path by
construction and the schemaless one not at all.

Stated generally, because it sharpens Class 7's rule rather than merely adding
to it: **discrimination bounds a test from below, never from above.** Showing a
test fails when the bug is restored proves it exercises *that* path. It says
nothing about the sibling paths the same mechanism lives on, and those are
exactly what a class issue exists to enumerate.

### The state of the paths

The var-length traversal's absence of bounding was confirmed by enumerating the
state machines, which is worth keeping as a table:

| state enum | location | variants | bounded |
|---|---|---|---|
| `TraverseStreamState` | traverse.rs:677 | `Warming`, `Reading`, `Materializing`, `Chunking`, `MaterializingChunk`, `Slicing`, `Done` | chunks and slices |
| `GraphScanState` | scan.rs:515 | `Init`, `Executing`, `Slicing`, `Done` | slices only (#214) |
| `VarLengthStreamState` | traverse.rs:4072 | `Warming`, `Reading`, `PrefetchingProperties`, `Materializing`, `Done` | neither (#241) |
| `VarLengthMainStreamState` | traverse.rs:5158 | `Loading`, `Processing`, `PrefetchingProperties`, `Materializing`, `Done` | neither (#241) |

#202 added slicing to "scan and traversal"; its three commits reached the scan
and the **single-hop** traversal. Ten of the fourteen LDBC queries use
variable-length paths.

### A note on this document's own paths

Every source path in the sections above omits the `query/` segment —
`crates/uni-query/src/executor/core.rs` is really
`crates/uni-query/src/query/executor/core.rs`. Line numbers are accurate.

### Remaining

**IC5 is the only unanswered SF1 query**, and #229 is 64% of it with no fix
commit. #219's anchoring landed but does not reach MERGE (#225) or Locy (#226).

Class 7 continues to generate instances faster than it is closed: **#230 was
filed after this review** and is the same mechanism — `iai_gate.py` reporting
"worst +0.00%" while 5 of 5 gated targets sat 88-97% below baseline.

---

## Status — 2026-09-05

Every class in this document re-read against the source, four days after it was
written. Ledger is in `issue_triage_2026-09-03.md`'s *Status — 2026-09-05, third
pass*; this records what happened to the **classes**, which is what this document
is for.

| class | state |
|---|---|
| 1 — fail-open | **Tiers 2 and 3 closed; Tier 1 open.** The inverse of what the tracking ledger recorded |
| 2 — entity identity hand-rolled | **substantively closed**, and the remedy was the one this document argued for |
| 3 — unbounded materialisation | open; partially accounted, not bounded |
| 4 — fixed constants for a cost decision | open, unchanged |
| 5 — plan shape is syntax-driven (#224) | open, unchanged; #226's claim narrowed |
| 6 — per-item round-trips (#220) | open, unchanged |
| 7 — invisible to result-only tests | open; **still generating instances faster than it is closed** |
| 8 — orientation re-derived four ways | **substantively closed.** All four live faces fixed |

### Class 2 is the one that worked, and it worked the way this document said

The suggested handling was *"not 'fix the sites' — make `entity_vid` the only way
to ask, then the remaining sites fail to compile rather than failing to match."*
What shipped is that argument with one substitution: `Value`'s own `PartialEq`
now compares entities by identity (`value.rs:1421-1426`), so comparison, dedup
and hash sites cannot get it wrong without opting out. `entity_ref` is at 65
workspace hits against the two `entity_vid` had when this was written.

The boundary that could not be closed by types is held by a ratchet instead:
`crates/uni/tests/common/arch_entity_identity.rs`, wired at `integration.rs:62`,
greps `src/` against a per-file budget and **fails in both directions** — over
budget flags a new site, under budget demands the budget be tightened. Ten
hand-rolled id reads remain, each with an audited rationale, against the "~20
extractors and 10 classifiers" this document counted.

That is the first class here to be closed at the mechanism rather than the sites,
and it is worth reading as the template for Classes 1 and 8.

### Class 8 closed, and the residual is the right shape

All four faces listed as live are fixed. `wants_orientation_column`
(`df_planner.rs:174`) is now the single predicate for the schema'd and schemaless
paths both, and `add_edge_structural_projection` **hard-errors** on a missing
`_fwd` for `Direction::Both` rather than falling through to traversal order —
which is exactly the `Option<bool>` discipline this document nominated as "the
counter-example worth copying" from `traverse.rs:806-813`. Pattern comprehension
now normalises through `resolve_stored_edge_endpoints`, i.e. mechanism C, the
storage probe, which was the other suggested remedy.

Two residuals, both narrower than any filed face: `plan_traverse_virtual_edge`
has no `COL_FWD` request site, so a `Both` virtual-edge hop hits the new error —
**loud, not silent**, which is the point of the error. And `resolve_stored_edge`'s
inconclusive fallback still yields traversal order for an edge type with no CSR
adjacency.

### Class 1: the tiering worked and then chose for us

This document proposed tiering the 27 sites *because* "a fix pass that starts at
the top of the table removes wrong answers, one that starts at the bottom removes
log noise." Both bottom tiers are done and the top one is not.

The two project-wide sub-decisions this document asked to make once were in fact
made, and correctly — a failed *index or status write* is now recorded rather
than dropped (Tier 3: no `let _ = update_index_metadata` remains, all three sites
count `uni_index_status_write_failures_total`), and five default-index sites route
through one `record_default_index_failure` helper (Tier 2). The first
sub-decision — *does a failed read ever return a default?* — is the one still
unanswered, and it is the one that owns every remaining wrong answer:

- `common.rs:1949` — `ScalarKey::Utf8(format!("opaque@{row_idx}"))`, no log.
  DISTINCT and GROUP BY silently stop deduplicating.
- `writer.rs:2194`, `:2418` — the ext_id uniqueness probe still admits a
  duplicate on an I/O error.
- `value_codec.rs:214` — a CRDT counter silently reads 0.
- `writer.rs:5064` — a label lookup swallowing `Err`; **not in this document's
  table**, found on the re-read.

Two fixed and worth recording as evidence the approach works: `scan.rs:1470`
propagates instead of returning an all-NULL column, and **zero `new_null_array`
error-fallbacks remain repo-wide**; `storage/index.rs:230`/`:350` now discriminate
`is_dataset_not_found` from a real error, so a broken index no longer reads as
"no such UID". Two unaudited siblings of that same pattern remain at
`inverted_index.rs:78` and `sparse_index.rs:225`.

### Class 3, and a hazard this document's own framing created

Class 3's #242 was filed on a countable headline — "zero `try_grow` sites". Six
operators now reserve, so the headline is false and a status check by grep would
close the issue. **The mechanism is 55 of 58 intact**: every other `df_graph`
exec has no `MemoryConsumer`, including `shortest_path`, `recursive_cte`,
`vector_knn`, `pattern_comprehension`, the mutation execs and the whole Locy
runtime.

Worse for the class as stated: `scan.rs:1552-1560` concedes the reservation
happens **after** the batch is built. That converts an OOM into an error — a real
improvement in blast radius — but it is not the bound the class asks for, and it
cannot be until the scan is genuinely incremental. Which is why #214 with #240
correctly sits ahead of #242 rather than beside it.

**A class issue whose title states a countable fact gets closed by fixing the
count.** This document's own advice was that a class issue listing 30 sites
invites 30 patches and a 31st site next quarter; the sharper version is that a
class issue listing a *number* invites moving the number.

### Class 7, and the finding that extends it

Still open, still generating instances: `repro_commit_timeout_after_durable.rs`
asserts that a single-writer commit must not report `CommitTimeout`, and asserts
it **only** with `async_flush_enabled: false` — the path where the lock is
uncontended by construction. #177's ratchet is also pinned rather than
ratcheting: `MAX_UNPROVEN = 32` unchanged, 32 of ~37 rows `Unproven`, only **2**
`Proven`, and the gate asserts *equality*, so proving an operator fails the build
until someone edits the constant. And `registry.rs:155-158` classifies
`ForeachExec` as `Unproven` where no grammar path exists — it is `Unreachable`,
so the row is false and inflates the budget by one.

The extension this round earned, and it is about documents rather than tests:

> **A completion claim with no discriminating check is indistinguishable from no
> claim.**

Class 7's rule is applied here to test coverage throughout and was never turned
on the project's own status tables. Three entries in
`issue_triage_2026-09-03.md` were wrong on re-read — #233's tier assignment,
#216's second clause, #178 never checked at all — by exactly the mechanism this
class describes. The 2026-09-02 addendum above already sharpened the rule once
(*discrimination bounds a test from below, never from above*); this is the same
rule applied to a ledger.

### A class this review missed

**Infrastructure wired and never consumed** — five sites, filed as five unrelated
issues, none referencing another: `ScanRequest::with_limit` (#239, zero callers),
`index_consulted` (#195, a real metric with zero benchmark hits), `max_impact`
(#118, stored and unread for scoring), `get_batch_edge_props_for_type` (#222,
exists while the hot caller holds the ids it needs), `prefers_full_scan` (#237,
one caller from the eid path while the endpoint-vid arm 160 lines below chooses
by `match` arm).

It passes this document's own test for a class — one mechanism, sites that do not
know about each other — and it has a cheap detector the project already trusts: a
single-caller / dead-surface ratchet in the style of `arch_entity_identity.rs`.
Two of the five are inputs #224 would want to consume, so it is worth doing
before Class 4 rather than after.

---

## Status — 2026-09-06

Class 1 is closed for silent wrong answers across the whole workspace, not
just the crates this review audited. The ledger is in
`issue_triage_2026-09-03.md`'s *Status — 2026-09-06*; what belongs here is
what it says about **this document's method**.

### The scope note at the bottom of this review was the load-bearing error

Under *What this review could not determine*, this document records:

> Plugin, CLI, bulk and CRDT crates were not audited for Class 1; roughly 25
> further warn sites exist there, mostly scheduler and CDC paths.

Both halves are wrong in the same direction. Measured: **~40 Tier 1 sites**
in that scope — more than the 27 this review catalogued in total — and only 8
of the 29 in the plugin crates are in scheduler/CDC code.

The mechanism of the error is the part worth keeping. **The estimate counted
`warn!` sites.** 28 of 36 warns in those crates genuinely are in
scheduler/CDC/trigger files, so the characterization is a correct description
of the *logging*. But 98 `let _ =` swallows there log nothing at all, and most
of the worst findings emit no diagnostic whatsoever — including the two that
outrank anything in this document's own table (an ABI-incompatible plugin
loading, and a scan returning rows that violate its `WHERE` clause).

> **Counting the failures that announce themselves cannot measure the failures
> that do not** — and a class defined by silence is exactly where that bites.

This review's Class 7 says an optimization with a correctness-preserving
fallback is invisible to result-only tests. The scope note is the same
sentence about auditing: a swallowed error with no log is invisible to a
warn-site census. The census was the instrument, and it could not fail.

### The remedy this review argued for, applied to Class 1

Class 2 closed at the boundary and Class 8 closed by making absence an error.
Class 1 had no equivalent, because "a failed read" is not one call site — so
it closed twice at the sites and came back the second time in the crates
nobody had walked.

`crates/uni/tests/common/arch_fail_open.rs` is the boundary it can have: a
CI ratchet on the **decisions**, not on the class. Three rules, each with a
canonical remedy — an index dataset opened with `.ok()`, an index-status
write dropped with `let _ =`, not-found classified by matching error text.
Deliberately narrow: a general "no swallowed error" scan would budget
hundreds of ordinary `unwrap_or_default` uses and the real entries would
drown, which is the inflation that makes an audit worthless.

It earned itself immediately. Writing it turned up **a second
`let _ = update_index_metadata` in a file that same session had already
audited and fixed by hand**, plus a `DROP INDEX ... IF EXISTS` that decided
"no such index" by looking for "not found" in an error message.

### One correction to this document's own table

`df_graph/common.rs` is listed under *The worst instances* as making DISTINCT
and GROUP BY stop deduplicating. **Measured against arrow 58: unreachable.**
`ArrayFormatter::try_new` succeeds for all 26 constructible types, so the
`opaque@{row_idx}` arm cannot be reached by any array that can be built. It
is defense in depth, now pinned by a test that fails if an arrow upgrade ever
introduces an unformattable type. Both the original review and a later reader
reached "probably unreachable" by reading arrow's match arms; running it is
what made the difference between a guess and a licence to ship without a test.

### And one about audits generally

Three sites on the audit list were not defects: one unreachable, one a
documented contract (`Float64Column::get`), one a documented shorthand (the
Rhai `col{i}` yield name) whose "fix" **broke four passing tests**. Three real
defects were absent from the list entirely. An audit's tiering is a starting
hypothesis about extent — the same thing this project already knows about a
defect's filed description — and it is wrong in both directions.
