# Column pruning — what is left, and whether a general pass is the answer

*Spike for #184. Written before further code, per the remediation plan.*

## Why this document is not what #184 asks for

#184's body says:

> There is no column-pruning pass in the logical planner — `prune`,
> `required_columns` and `projection_pushdown` match nothing.

That was true when it was written and is not true now. Two passes exist,
one of them compiler-guarded, and the question has moved. Restating the
original framing would send the next reader looking for a pass that is
already there.

**What exists:**

- `collect_properties_from_plan` / `collect_properties_recursive`
  (`planner.rs:9424`, `:9733`) — a bottom-up union: *which properties does
  anyone mention*. Its match over `LogicalPlan` is **exhaustive**, 70 of 70
  variants, no `_ =>` arm (`planner.rs:10066`). The original plan's P0 is
  done, and a 71st variant is now a compile error.
- `reconcile_passthrough_properties` (`planner.rs:9444`) — resolves the
  provenance markers a `Project` (and, since #196, an `Aggregate`) emits,
  keeping `"*"` for variables returned whole and narrowing the rest to the
  properties actually accessed.
- `mark_dead_unwind_sources` (`planner.rs:9609`) — proves an `UNWIND`
  source dead and drops the column, which is #184's headline case.

So the honest question is **not** "how do we build a pruning pass". It is:
**which shapes are still uncovered, and is the remedy a general top-down
pass or a short list of arms that should each emit a marker?**

## The evidence that reframes it — #196

#196 is a worked example of finding an uncovered shape, and it argues
for the cheap answer.

`MATCH (p:Person)-[:KNOWS]-() WITH p, count(*) RETURN p.id` requested
1.76 GB against a 1 GiB pool. The group key needs the entity's identity;
the query reads one property. But the `Aggregate` arm routed group-by
expressions into the bare-`Variable` arm, which marks the source `"*"`,
so the scan projected the full schema — `_all_props` and `overflow_json`
included — and `plan_aggregate` made the whole node a group key twice
over (the entity struct, plus every `{v}.`-prefixed column beside it).

Measured on 20 001 groups, varying a property the query never reads:

| `pad` length | before | after |
|---|---|---|
| 4 chars | 9.8 MB | 4.4 MB |
| 256 chars | 65.5 MB | 4.4 MB |

**The fix was one arm.** It emits the marker the `Project` arm has emitted
since the #134 family, and the existing reconciliation did the rest. No
new pass, no new IR, no dataflow analysis — and the machinery it reuses
already had tests.

That is one data point, not a proof. But it is the *only* uncovered shape
anyone has found since the walker became exhaustive, and it cost one arm.

## Recommendation

**Do not build a general top-down pass yet.** Instead:

1. Close the correctness hole below. It is not a memory question at all
   and it outranks everything else here.
2. Bound the allocation below. It is what actually kills the process, and
   pruning cannot fix it.
3. Audit the remaining marker-emitting sites the way #196 was found —
   listed at the end — and only if that audit turns up a shape a marker
   *cannot* express, revisit the general pass.

The reasoning: a top-down required-set pass is a real piece of
infrastructure, and the two things currently costing us are neither
solved by it nor blocked on it.

## (a) The unsound catch-all — correctness, not memory

**This is the highest-ranked item in this document**, by the ordering the
remediation plan sets out: a wrong answer outranks a crash.

`collect_properties_from_subquery` (`planner.rs:10457`) still has
`_ => {}` arms on `Clause` (`:10487`) and on `Query` (`:10495`). These are
the only remaining catch-alls in the property-collection path. A read
inside an `EXISTS` / `COUNT` / `COLLECT` body whose clause is not
`With`/`Return` is therefore **not recorded**.

For most consumers an under-report costs performance. For
`mark_dead_unwind_sources` it fails in the **unsafe direction**:

- the analysis proves a list dead by *absence* — `is_read_anywhere`
  (`planner.rs:9644`) asks whether any reader survives the blanking pass;
- an unrecorded read looks exactly like no read;
- so a list that a subquery body genuinely consumes can be proven dead and
  **dropped**, and the query returns a wrong answer silently.

This is live in shipped code. It is bounded work — make both matches
exhaustive, classify each clause, and the compiler enforces the rest —
and it needs a test that constructs a read reachable only through a
non-`With`/`Return` clause inside a subquery body.

Nothing here depends on the pruning question being settled.

## (b) The unbounded allocation — what actually kills the process

Now measurable, because #196 unblocked the SF1 bench.

Run at SF1 with the memory pool at its 1 GiB default:

```
IC1  20 rows   2616.5 ms   peak_rss=12460MiB
IC2  ERROR  external sort out of memory
IC3   0 rows     71.8 ms
IC4  ERROR  1098.5 MB for GroupedHashAggregateStream, 1024.0 MB available
IC5  ERROR  exceeded the 300 s budget
IC6  ERROR  exceeded the 300 s budget
IC7  20 rows  20107.8 ms   peak_rss=13812MiB
IC8  20 rows    514.4 ms
IC9  ERROR  external sort out of memory
…
process didn't exit successfully (signal: 9, SIGKILL)
```

**Peak RSS 13.8 GB against a 1 GiB pool, ending in SIGKILL.** The bench's
own comment (`ldbc_snb.rs:319`) anticipated exactly this: *"if the process
is killed while well above that the OS did it, not the engine's own guard
— which is a finding about the guard, not just about the query."*

The guard is bounding the operators that reserve through it and missing
roughly an order of magnitude of real memory. Three separate comments
already say why (`executor/read.rs:646`, `api/mod.rs:2082`,
`query_limits_test.rs:578`): an operator that builds an Arrow buffer
directly never asks the pool.

The specific site is `GraphUnwindStream::build_output_batch`
(`df_graph/unwind.rs:600`). `process_batch` (`:337`) accumulates **every**
expansion for an input batch into one `Vec<(usize, Value)>`, then does one
`take` per surviving column over the whole set (`:621`). Peak is
`rows × list_size` for the batch, allocated in one shot, unaccounted.

**Proposal: bound the output rather than prune more.** `process_batch`
already produces the expansion list; emitting it in fixed-size slices —
several output batches per input batch, each with its own `take` — caps
peak at `chunk × columns` without changing semantics or row order.
`GraphUnwindStream` would hold the pending remainder across `poll_next`
calls, the same shape `EndpointHydrateStream` and `BindZeroLengthPathStream`
already use for their pending prefetches.

This is worth doing **whether or not** any more pruning lands, because a
list that is legitimately live still replicates. Pruning removes the dead
case; chunking bounds the live one.

Note it will not by itself make IC5/IC6 finish — those exceed a time
budget, not a memory one — nor IC2/IC9, which fail in DataFusion's own
external sort with no spill path configured. Those are separate findings
and should not be folded in.

## What a general pass would and would not buy

For the record, so the option is rejected on its merits rather than by
omission.

The two analyses differ exactly at fan-out points.
`collect_properties_from_plan` is a bottom-up union ("what does anyone
mention"); pruning wants top-down required-sets ("what does my consumer
need"). `mark_dead_unwind_sources` sidesteps this with the blanking
trick — re-run the union analyser on a plan copy with the candidate
erased, and read liveness off absence — which generalises to any
single-source drop candidate at a fraction of the cost.

Shapes where the two genuinely diverge: `Traverse` (the multiplier),
`CrossJoin` / `Apply`, `Unwind` (done), `RecursiveCTE` and variable-length
paths, and `Aggregate` with `collect()`.

A true top-down pass would cover all of them uniformly. What argues
against building it now:

- the one uncovered shape found since the walker became exhaustive (#196)
  needed one arm, not a pass;
- the crash is an allocation-shape problem, and a perfectly pruned plan
  still allocates `rows × list_size` when the list is live;
- the blanking trick already covers the drop-candidate case and is
  cheaper to reason about than a dataflow lattice.

Revisit if the audit below finds a shape no marker can express.

## The audit to run instead

#196 was found by measuring, not by reading. The same method applies to
the remaining sites that consume expressions without emitting a
provenance marker. For each, ask: *does this context need the entity
whole, or only its identity?*

- `Sort` keys — ordering by a bare entity.
- `Distinct` — `plan_aggregate`'s sibling; `LogicalPlan::Distinct`
  (`df_planner.rs:1061`) groups by **all** schema columns with no
  narrowing, the same shape #196 had.
- `Union` dedup (`df_planner.rs:5822`) — likewise groups by every column.
- `Window` partition and order keys.
- `CrossJoin` / `Apply` correlation keys.

`Distinct` is the obvious next candidate: `RETURN DISTINCT n` over a wide
label has exactly #196's shape, and `SELECT DISTINCT` over `_all_props`
is the same waste. It should be measured before it is assumed.

## Verification

The SF1 bench is the verification for anything in (b), and it now runs:

```bash
LDBC_DB=$HOME/uni-bench-tmp/sf1 TMPDIR=$HOME/uni-bench-tmp \
  cargo bench -p uni-db --bench ldbc_snb
```

`LDBC_DB` persists the loaded graph, so the ~4-minute SF1 load happens
once. Record peak RSS per query; success for (b) is the process
surviving, not a latency number.

For (a), a unit test in the `dead_unwind_source_tests` module constructing
a read reachable only through a non-`With`/`Return` clause inside a
subquery body, asserting the source is **not** proven dead.

Do not run the bench concurrently with the test suite. It peaks above
13 GB, and doing so once already timed out an unrelated test
(`instrumented_get_edges_scaling`, 153 s in isolation against a 360 s
limit) — which reads as a flake and is not one.
