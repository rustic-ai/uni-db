# Open-issue triage and reprioritization — 2026-09-03

44 open issues, ordered against the principle the LDBC remediation document
already established: **wrong answers before crashes before gaps before speed**,
and anything that stops the system defending itself outranks optimization.

Two things have changed since the last ordering was written, and both bear on it.

**SF1 is 14 of 14.** The latency emergency that shaped the last two orderings is
over. Nothing here is queued because a benchmark query fails.

**The IC5 round is evidence for putting observability first.** It took four
wrong attributions to find one cause, and every one of them was an inference
that resembled a measurement. What ended it was an instrument that reported its
own total beside its parts. That is the same argument the class review made for
Class 7 — "it is why the others hid" — now with a worked example.

---

## The count overstates the work

Fourteen of the 44 are instances of a class also filed. Counting both makes the
backlog look larger than it is and invites fixing the instance while the class
survives, which this project has watched happen repeatedly.

| class | instances also open |
|---|---|
| #234 entity identity hand-rolled | #217, #235, #236, #216 |
| #243 edge orientation re-derived four ways | #193 |
| #224 no cost model / syntax-driven plan shape | #219, #225, #226, #237, #247 |
| #220 per-item storage round-trips | #221, #222, #228 |

Recommendation: keep the instances open only where they carry a repro the class
issue does not, and say on each which class owns it. The classes are the work
items; the instances are symptoms.

---

## Tier 1 — silent wrong answers

Invisible by construction, so no amount of running the system surfaces them.

| # | why it is first |
|---|---|
| **#233** | fail-open class: 27 sites, **17 silent**. Six verified while filing. Two project-wide decisions (does a failed read ever return a default; does a failed index/status write ever go unrecorded) settle most of it |
| **#234** | identity hand-rolled ~30 times; `Value::entity_vid`, written to end that, has **two** call sites. Fix the boundary, not the sites |
| **#243** | orientation re-derived four ways, four faces still live and untested. This class already produced a regression *during* a fix whose soundness argument checked one of the four |
| **#236** | Locy join keys are `Debug` over a `HashMap` — **inconsistently** wrong, so a rerun does not confirm anything |
| **#235** | `COUNT(DISTINCT n)` has no entity arm at all — a wrong aggregate nothing downstream catches |

#193, #216, #217 fold into #234/#243.

## Tier 2 — the system cannot bound itself

Not wrong answers, but the difference between a slow query and an outage.

| # | note |
|---|---|
| **#242** | zero first-party operators reserve from the pool; a pool that sees a minority of the allocation is a sampler, not a bound. Peak RSS is 14.5 GiB against a 1 GiB pool |
| **#240** | `execute_stream` emits one item, so the incremental memory check fires after everything is resident |
| **#239** | `ScanRequest::limit` has zero callers — `LIMIT 1` reads the whole table |
| **#241** | variable-length traversal has neither chunking nor slicing; ten of fourteen LDBC queries use VLP |
| **#214** | the scan slices but still builds the whole result first |
| **#213** | `ORDER BY … LIMIT` is always a full sort; needs a spillable TopK, not the one-line pushdown |
| **#238** | two comments assert no disk spill and drive the pool choice. Cheap, and it is the premise Tier 2 reasoning rests on — do it first within this tier |

## Tier 3 — instruments that cannot fail

| # | note |
|---|---|
| **#230** | **live and lying now**: 5 of 5 gated targets 88–97% below baseline, reported "worst +0.00%". A gate that passes on a collapsed measurement corrupts everything measured after it |
| **#247** | indexes are built and never consulted, verified by a point lookup no faster than a scan written to defeat pushdown |
| **#179**, **#223** | operators and scans invisible to `PROFILE` / `scans_reported`, so the shape tests that would pin them cannot be written |
| **#205**, **#177** | the vacuous-fixture audit and the unproven-operator ratchet |
| **#195** | no benchmark exercises the index counters |

## Tier 4 — performance, once there is something to plan with

**#224 is the root** — `estimate_costs` ignores its plan and returns constants,
and no graph operator overrides `statistics()`. #237, #247, #219, #225 and #226
are all decisions made without cardinality. Sequencing anything else in this
tier before #224 means fitting more constants to one dataset.

Then #220 with #221/#222/#228, and #206 and #215.

## Tier 5 — hygiene and features

#174, #176, #178, #200, #227, #231; and #118–#123, which are planned features,
not defects.

---

## Recommended order

1. **#230**, then **#231**. Hours, not days, and everything measured afterwards
   is only as good as the gate that reports it.
2. **#233 Tier 1 only** — the wrong-answer rows, with the two project-wide
   decisions made once rather than per site.
3. **#234**, taking #217/#235/#236/#216 with it. Make the encoding
   unrepresentable-in-two-ways, or make `entity_vid` the only way to ask, so the
   remaining sites fail to compile rather than failing to match.
4. **#243**, taking #193. Carry orientation with the edge value, or make the
   storage probe the only answer and delete the reconstructions.
5. **Tier 2 as one track**, starting with #238, because these share a root — no
   operator accounts for what it allocates — and fixing them one at a time
   re-litigates the same design each time.
6. **#224**, then the rest of Tier 4.
7. Tier 5, and Track 0 (the differential oracle) whenever a Neo4j instance
   exists — it remains the one thing that would have found most of the Tier 1
   items without anyone probing.

## What this ordering deliberately does not do

It does not put IC5-adjacent performance work near the top. #206, #219, #225 and
#226 were queued there when a benchmark query could not answer; it now can, and
their remaining justification is throughput, which is Tier 4's business.

It also drops Wave 5.2–5.5 from the front of the queue. Its gating argument —
that latency work needed the index counters as a witness — did not survive: the
gate is still shut (#247) and IC5 was attributed with `PROFILE` instead.

---

## Status — 2026-09-04

46 open. Worked through the recommended order; four items closed.

| # | state |
|---|---|
| #230 | fixed — one-sided comparison, `--fail-improve-pct` added |
| #231 | not started |
| #233 | **Tier 1 and Tier 3 fixed; Tier 2 (silent slowness) is what remains** |
| #199 | fixed (not in this document's order; it was the remediation doc's step 5) |
| #247 | fixed — root cause below |
| #234, #243 | not started — the two remaining Tier 1 classes |

### One correction to this document

**#247 is listed under Tier 4 as a `#224` instance ("no cost model").** That is
wrong and should not be planned from. It was not a planning decision at all:
semantic compaction overwrites the per-label table, Lance drops the dataset's
indexes on an overwrite, and nothing rebuilt them. A pushdown gate
(Hash-only, so a BTree could never be collected) was a real second blocker and
is also fixed, but it was not the cause of the reported symptom.

The general lesson for the class/instance table above: **#247 was filed against
a symptom and grouped by the mechanism its title guessed at.** Two of the four
class groupings in this document rest on the same kind of guess and have not
been verified against the code the way #233's and #243's were.

### New since this document

- **#249** — adding a property to a label with flushed data makes every
  subsequent write to it fail. Reproducible, loud, and unrelated to #247 despite
  being found while bisecting it. Belongs in Tier 1 by severity: not a wrong
  answer, but it leaves a label unwritable.

### Recommended next

Unchanged in shape: **#234**, taking #216/#217/#235/#236 with it, then **#243**
taking #193. #233's Tier 2 and #249 are both small and self-contained.

**17 commits are unpushed**, and three closing keywords (`Fixes #199`,
`Fixes #230`, `Fixes #247`) cannot fire until they merge.

---

## Status — 2026-09-05

44 open. PR #250 merged, which closed **#193, #230, #235, #247 and #251** — the
keywords the previous status recorded as blocked. **#231 did not close**: its
commit was the one commit not pushed to the fork, so it sat outside the PR while
every other commit in the branch merged. A closing keyword only fires from a
commit that is actually in the merge.

A further **14 commits are unpushed**, carrying `Fixes #231`, `Fixes #236`,
`Fixes #238` and `Fixes #252`.

### Worked since

| # | state |
|---|---|
| #243 | **all four faces closed.** Faces 1 and 2 merged in #250; face 4 was already closed by the entity-encoding work; face 3 and the class remedy are on the unpushed branch |
| #236, #252 | fixed — one canonical rendering on `Value`, `Debug` fallback deleted from five sites |
| #238 | fixed — both comments corrected, pool decision re-made on the true premise |
| #233 | **Tier 2 done.** All three tiers now closed; the issue stays open for the ~25 unaudited plugin/CLI/bulk/CRDT sites it scopes out — **this row is wrong, corrected in [Status — 2026-09-05, third pass](#status--2026-09-05-third-pass): Tier 2 and Tier 3 are closed and Tier 1 is the tier still open** |
| #242 | six operators reserve; the pool is no longer blind to the largest allocations. Scope item 1 done, item 2 now unblocked |
| #234 | not started as filed, but see below |
| D7 (`docs/correctness-deferred.md`) | was already fixed; only the guard was missing |
| *(unfiled)* | index status reported `ONLINE` unconditionally on two surfaces — the `Schema` introspection projection and `uni.schema.indexes`. Same class as the index-build-status honesty fix, which landed and visited neither. Found while reading for #233, not from a report |

### What this round says about the document above

**Three of #243's four faces were already fixed when this document listed it as
"not started".** Two had merged in #250 and one closed as a side effect of the
entity-encoding work. The same is true of **#234**, listed here as a remaining
Tier 1 class: the query layer was unified days earlier, and what is left is a
boundary that should stay open — `COPY FROM` input and user map properties can
legitimately carry `_id`, so the CI ratchet is the guard, not a compiler check.

This is the *opposite* failure from the one the "#247 correction" section
records. That was a filing grouped by a guessed mechanism; these are filings that
were simply **overtaken**. Both produce the same planning error — work sequenced
against a description of the tree rather than the tree — and the fix for both is
the same: re-verify each site before planning from a class issue, not after.

The count is still overstated for the reason the top of this document gives, and
now also because closed work reads as open until someone checks.

### Two findings worth carrying

**The vacuous-fixture class (#205) is not incidental — it guards real defects.**
Three separate repros this round protected nothing: D7's printed its result
instead of asserting, and #236's and #252's shipped `#[ignore]`d because they
failed. In every case the *fix* was cheap and the *guard* was the missing half.
A repro left non-gating after its fix lands is indistinguishable from no test.

**`cargo nextest` cancels remaining tests after a failure.** A full run reported
`2271/2821` and a second failure sat hidden behind the first; the shortfall is
easy to read as a filter. Use `--no-fail-fast` before concluding a suite is
green.

### One consequence to record against #238

The reasoning committed for #238 — keep `GreedyMemoryPool`, because the operators
dominating peak memory do not reserve — was true when written and is now half
untrue by our own hand: six of them reserve. #238 recorded the condition for
revisiting rather than a verdict, which is what makes this checkable instead of
stale. **#242's scope item 2 is now due**, and it is the first thing that should
happen after the scan becomes incremental.

### Recommended next

1. **Push, and fetch first** — `origin/main` moved under this branch when #250
   merged, and four closing keywords are waiting on commits that only exist
   locally.
2. **#214 with #240.** The scan's new reservation fires *after* the whole batch
   is built, so today it bounds how long an over-budget result survives rather
   than whether it is constructed. Making the scan incremental is what converts
   it into a real bound, and it is the single fact behind #214, #240 and #202's
   unspillable sort. It also unblocks the #238 revisit above.
3. **#239** if a small self-contained item is wanted first: `ScanRequest::limit`
   has zero callers, so `LIMIT 1` reads the whole table.
4. **#224**, then the rest of Tier 4. Unchanged, and still nothing below it
   should be sequenced ahead of it.

Tier 2's remaining memory items (#241, #214, #213) still share one root and
should still move as one track rather than singly.

---

## Status — 2026-09-05, later

A second pass the same day, prompted by re-running this document's own order
against the tree. Narrative and measurements are in the 2026-09-05 status of
`ldbc_findings_remediation_2026-08-27.md`; this is the ledger.

### Verified against the source, not the filing

The previous status recorded that #243 and #234 had been *overtaken* — fixed
while still listed here as work. Checking the rest of the Tier 1 instances the
same way finds two more:

| # | state | evidence |
|---|---|---|
| #216 | **closed in fact** | `locy_eval.rs` matches on `(a.entity_ref(), b.entity_ref())`, with the mixed pairings documented beside it |
| #217 | **closed in fact** | `expr_eval.rs` carries a comment saying the raw `_vid` arms were deleted for `entity_ref`; `entity_aware_eq` routes through it too |
| #234 | **closed in substance** | `entity_vid` / `entity_ref` is at 28 call sites, against the two it was filed on; `arch_entity_identity.rs` is the ratchet. The remaining boundary should stay open — `COPY FROM` input and user map properties can legitimately carry `_id` |
| #239 | still holds | `ScanRequest::with_limit` has zero callers; the `scan_all_backend_with_limit` hits are a differently-named store method |
| #176, #174 | still hold | as filed |
| #254 | spam | auto-submitted promotion for an unrelated project; its title is a truncated copy of this repo's tagline |

So four of this document's Tier 1 rows are done and one is spam. **The count of
open issues is now a poor proxy for the work twice over** — once for the
class/instance double-count at the top of this document, and once because closed
work reads as open until someone opens the file.

### Worked since

| # | state |
|---|---|
| #253 | **fixed.** The schemaless edge-type registry was never persisted. The issue reports the pattern comprehension; the untyped `MATCH (a)-[e]-(x)` was also returning nothing, which is the wider and louder case |
| *(unfiled)* | **fixed.** A path's relationship type and its nodes' labels were read from L0-only sources, so both were lost once the data was flushed. Five live call sites for the label half. Needs an issue — its commit closes nothing |
| CI | **green.** `repro_get_edges_scales_with_graph_size` failed on a `CommitTimeout`; three tests shared the exposure and now configure the guard. `--no-fail-fast` added to the PR workflow, which had been hiding 2171 of 6869 tests behind the first failure |

### Recommended next

Step 1 of the previous list is discharged in intent — the branch is now **20
commits ahead of `origin/main`**, and five closing keywords are waiting on the
merge: `Fixes #231`, `#236`, `#238`, `#252`, `#253`.

The rest is unchanged, and nothing this round displaces it:

1. **#214 with #240**, still one change and still the single fact behind #202's
   unspillable sort; it unblocks the #238 revisit that #242's scope item 2 waits
   on.
2. **#239** if a small self-contained item is wanted first.
3. **#224**, then the rest of Tier 4.

Two additions to the tail of the queue, both from this round:

- **File the path/label hydration defect.** It is fixed with gating tests, but
  nothing tracks it.
- **A vacuous-coverage instance of #205, on an invariant already written down.**
  `repro_commit_timeout_after_durable.rs` asserts that a single-writer commit
  must not report `CommitTimeout`, and asserts it only with
  `async_flush_enabled: false` — the path where the lock is uncontended by
  construction. The default path is not covered. Whether a single-writer commit
  should fail because a *background* flush holds the lock is the product
  question the CI fix sidesteps rather than answers.

---

## Status — 2026-09-05, third pass

A third pass the same day, and the first one that verified **every** open issue
against the source rather than sampling. Six parallel readers, one per cluster;
44 issues, no edits. The result changes this document's own ledger in six
places, and one of those changes is the reason to keep doing this.

### The correction that matters: #233's tiers are recorded backwards

The status above says *"Tier 2 done. All three tiers now closed."* The source
says Tier 2 and Tier 3 are closed and **Tier 1 — the silent wrong answers, the
tier that put this class first in the whole backlog — is the one still open.**

| site | still fail-open | consequence |
|---|---|---|
| `df_graph/common.rs:1949` | `ScalarKey::Utf8(format!("opaque@{row_idx}"))`, no log | DISTINCT and GROUP BY silently stop deduplicating |
| `writer.rs:2194`, `:2418` | `if let Ok(Some(found_vid))` | the ext_id uniqueness constraint **admits a duplicate** on an I/O error |
| `value_codec.rs:214` | substitutes `GCounter::new()` in Lenient mode | a CRDT counter silently reads **0** |
| `writer.rs:5064` | a label lookup swallowing `Err` | same shape; **not previously listed anywhere** |

Two unaudited instances of the "open failure read as absent" pattern that Tier 1
just fixed in `storage/index.rs` also remain, at `inverted_index.rs:78` and
`sparse_index.rs:225`.

What was genuinely fixed: Tier 3 has no `let _ = update_index_metadata` left —
all three sites check, count `uni_index_status_write_failures_total`, and log.
Tier 2 routes five default-index sites through `record_default_index_failure`
(`baffbf498`), leaving ~2 benign sites. Two of the five sampled Tier 1 sites are
also fixed: `scan.rs:1470` propagates instead of returning `new_null_array` (zero
such fallbacks remain repo-wide), and `storage/index.rs:230`/`:350` now
discriminate `is_dataset_not_found` from a real error.

**Why it drifted is the part worth keeping.** Tier 2 and Tier 3 have countable,
greppable completion criteria — "no `let _ =` remains", "all five sites route
through one helper". Tier 1 has no such criterion; each site is a judgement about
whether a default is a lie. The work moved toward the tiers that could be
*declared* finished, and this ledger recorded the drift as if it were the plan.

That is Class 7's mechanism — an optimization with a correctness-preserving
fallback is invisible to result-only tests — operating on the project's own
tracking instead of its tests. **A completion claim with no discriminating check
is indistinguishable from no claim.** These documents apply that rule rigorously
to test coverage and have never turned it on their own status tables.

### Five further corrections

| # | recorded | actually |
|---|---|---|
| **#216** | "closed in fact" (2026-09-05, later) | **partial.** The structural half is fixed at `locy_eval.rs:697-701`. The three-valued-logic half holds: `Expr::In` at `locy_eval.rs:116-121` is still `Ok(Value::Bool(items.iter().any(...)))` — no `has_null`, so it can never return NULL, unlike `eval_in_op`. The title names two defects and the re-verification checked the first clause only |
| **#249** | tail hygiene, "loud, not silent" | **worse than filed.** `add_columns`/`alter_columns`/`NewColumnTransform` return **zero hits** across `crates/uni-store/src`. This is not a broken add-column path; there is no add-column path. The label is permanently unwritable — reopen does not clear it, a `SET` on pre-existing columns is accepted but its *flush* is rejected, and a `CREATE` that never mentions the property also fails |
| **#178** | Tier 5, open | **fixed.** `bindings/uni-db/tests/test_stateful_crud.py:135` already defines `GraphMachine(RuleBasedStateMachine)` with ~10 rules. Satisfied by the #181/#182 work; nothing connected the two |
| **#226** | as filed | **overstated.** `plan_pattern(&clause.match_pattern, &[])` is real, but anchoring is impossible for the **leading path only**; second-and-later comma-separated paths accumulate vars and can anchor |
| **#176** | as filed | **wider, and the registry row is wrong.** Dead surface is 263 lines plus planner/executor arms, not 154. `registry.rs:155-158` classifies `ForeachExec` as `Unproven` where no grammar path exists — it is `Unreachable`. Closing #176 therefore tightens #177's ratchet by one |

#216 is the first re-verification in this project to come back **weaker** than
the filing rather than sharper. The previous two passes both noted that
re-verification kept finding claims understated, never overstated. That is not a
rule to lean on.

### The count, a fourth time

Open is 44. Roughly **33** is the real figure, and the eleven split four ways —
the fourth is new:

| | issues | disposition |
|---|---|---|
| spam | #254 | close |
| fixed in fact, closeable now | #178, #217, #234, #243 | #243 substantively; its residual is loud, not silent |
| fixed, waiting on the merge | #231, #236, #238, #252, #253 | keyword fires when PR #255 and the five commits above it land |
| instance of an open class | #216, #219, #221, #222, #228, #235, #237 | keep only where the instance carries a repro the class does not |

The new category is **#178: discharged by a fix that never mentioned it.** The
three already recorded are class/instance double-count, fixed-but-unchecked, and
waiting-on-merge. This is a fourth and it is the hardest to detect, because
nothing in either the issue or the commit points at the other.

### Verified as still holding, no change

Every issue in the cost-model and plan-shape cluster holds as filed: **#224,
#237, #225, #206, #223, #222, #228, #213**, with #226 as corrected above. Two
readings are sharper than the filings:

- **#224** — zero `fn statistics` across all **39** `impl ExecutionPlan` blocks in
  `uni-query`, and no `OptimizerRule` is defined in the crate at all. Pinned by
  `pattern_anchor_test.rs:115-129`, which asserts a middle-bound pattern still
  cross-joins.
- **#213** — `QueryPlanner::plan` returns the hand-built plan with **no
  physical-optimizer pass**, so DataFusion's `LimitPushdown` cannot rescue the
  fetch-less `SortExec` even in principle.
- **#222** — the targeted `get_batch_edge_props_for_type` exists and the hot
  caller at `traverse.rs:1195` does not use it **while already holding
  `edge_type_ids`**.

Tier 3 holds throughout: **#177** (`MAX_UNPROVEN = 32` unchanged, 32 of ~37 rows
`Unproven`, only **2** `Proven`, and the gate asserts *equality* — so it is
pinned, not ratcheting, and makes no progress as a side effect of other work),
**#179**, **#205**, **#195**, **#200**, **#174**.

Tier 2 holds except where noted: **#240**, **#239**, **#214**, **#241** (both VLP
state machines now *account*, so an oversized expansion errors instead of OOMing,
but neither gained a `Chunking`/`Slicing` variant). **#238** is fixed — both
comments retract the premise and name `DiskManagerMode::OsTmpDirectory`, with a
test pinning it; the pool stays `GreedyMemoryPool` deliberately.

**#242 is partial in a way its own title hides.** The filed headline — "zero
`try_grow` sites" — is now false, so a status check by grep would close it. The
mechanism is 55 of 58 intact: every other `df_graph` exec still has no
`MemoryConsumer`, including `shortest_path`, `recursive_cte`, `vector_knn`,
`pattern_comprehension`, the mutation execs and the whole Locy runtime. And
`scan.rs:1552-1560` concedes the reservation happens *after* the batch is built,
so it bounds survival, not construction. **A class issue whose title states a
countable fact gets closed by fixing the count.**

### A class this document has been filing as five unrelated issues

**Infrastructure wired and never consumed.** Same mechanism, sites that do not
know about each other, which is this project's own test for a class:

| site | state |
|---|---|
| `ScanRequest::with_limit` (#239) | defined, zero callers; the field is still read by `lance.rs`, so the pushdown is wired and dead |
| `index_consulted` (#195) | a real metric read at `executor/core.rs:1055`; zero hits across all 23 files in `crates/uni/benches/` |
| `max_impact` (#118) | stored at `sparse_index.rs:351,375,436`; unread for scoring |
| `get_batch_edge_props_for_type` (#222) | exists; the hot caller holds the ids it needs and calls the untargeted sibling |
| `prefers_full_scan` (#237) | exactly one caller, from the eid path; the endpoint-vid arm 160 lines below chooses by `match` arm |

It has a cheap detector the project already trusts: a single-caller / dead-surface
ratchet in the style of `arch_entity_identity.rs`. Worth doing before #224, since
two of the five are inputs #224 would want to consume anyway.

### Recommended next

Unchanged in shape, with two insertions ahead of it:

1. **Merge PR #255 and the five commits above it.** Five keywords are held, and
   #231's has already failed to fire once. This is the second recurrence of what
   the class review called *"the single highest-value action in this document and
   it is not a code change."*
2. **Finish #233 Tier 1** — four silent wrong-answer sites, currently recorded as
   done. By this document's own ordering principle that outranks everything in
   Tier 2.
3. **Scope #249 separately.** It is a design gap sized like #224, not a defect
   sized like #253, and it is ranked as tail hygiene only because it is loud.
   Loudness is detectability, not severity, and this tiering has no row for
   *permanently destroys a label's writability with no recovery path*.
4. Then the existing order: **#214 with #240**, **#239**, **#224** and Tier 4.

### What this round is evidence for

**Re-verification has to reach a document's own status tables, not just its
filings.** The remedy already written here — "re-verify a class issue's sites
*before* planning from it" — was applied to the tracker and never to the ledger
that summarises it. Three of the six corrections above are entries this project
wrote, in this file, and did not re-check: #233's tier assignment, #216's second
clause, and #178, which was never checked at all.

**A two-clause title needs two verdicts.** #216 was closed on its first clause.
That is the same shape as the #219 orientation regression the class review calls
its strongest evidence — a soundness argument that verified one of four
derivations — reappearing at the granularity of an issue title.

---

## Status — 2026-09-06, the unaudited-crate audit

The third pass above closed #233's Tier 1 in `uni-store` and `uni-query` and
left the issue open for what it called *"the ~25 unaudited plugin/CLI/bulk/CRDT
sites it scopes out"*. That scope has now been audited and worked. It was not
25 sites, and it was not the tier the estimate implied.

### The estimate could not have been right, and the reason generalises

#233 projected **"roughly 25 further warn sites, mostly scheduler and CDC
paths"** across plugin + CLI + bulk + CRDT. Measured:

| scope | Tier 1 | Tier 2 | Tier 3 | examined |
|---|---:|---:|---:|---:|
| plugin crates | 29 | 1 | 17 | 295 non-test sites |
| scheduler / CDC (overlaps plugin-host) | +6 | 5 | 7 | — |
| `uni-bulk` | 2 | 1 | 4 | — |
| `uni-crdt` | 3 (latent) | 0 | 1 | — |
| `uni-cli` | **0** | 0 | 1 | — |

Deduped, **~40 silent wrong answers** — more than the 27 the entire original
issue catalogued, and the tier #233 ranks first in the whole backlog.

The characterization is half right in a way worth keeping: **28 of 36 `warn!`
sites in the plugin crates really are in scheduler/CDC/trigger files, but only
8 of 29 Tier 1 sites are.** The estimate counted warn sites, and 98 `let _ =`
swallows in those crates log nothing at all — most of the worst findings emit
no diagnostic whatsoever.

> **Counting the failures that announce themselves cannot measure the failures
> that do not.**

That is the LDBC round's "an instrument that cannot fail" applied to scoping
rather than to profiling, and it is why this scope sat untouched while four
sites in `uni-store` were fixed: the phrase *"warn sites"* pre-classified the
work as low-severity before anyone looked.

### What shipped

Eight commits, `Refs #233` throughout — the issue stays open, since Tier 2
(silent slowness) across these crates is still unaudited.

| wave | what |
|---|---|
| A | `AbiRange` validated on deserialize (an ABI-incompatible plugin loaded against any host); `StorageScanExec` honours its own predicate |
| B | CDC feed holes: `mutations_failed` halts instead of checkpointing past; checkpoint `lookup` returns `Result`; trigger predicate failures no longer read as "no rows match" |
| C | Scheduler durability: `periodic_cancel` no longer bypasses the persisting path; `add_scheduled_job`/`cancel` return `Result`; a failed startup load is recorded |
| D | Locy aggregates: an unread cell no longer yields `SUM 0.0`, `MNOR 0.0` ("impossible") or `MPROD 1.0` ("certain") |
| E | Eleven plugin sites substituting a default for an unreadable value |
| F | `uni-bulk` index-status and undecodable columns; `uni-crdt` register tie-breaks and a half-decoded ORSet |
| G | A cross-crate ratchet on the three settled decisions |

**Three findings were not in the audit at all**, and each came from following a
mechanism rather than a list:

- `Uni::periodic_cancel` called the bare in-memory scheduler, bypassing the
  persisting `SchedulerControl` impl. `periodic_schedule`, immediately above
  it, routes through the host with a comment saying it does so for exactly
  that reason. The procedure path was never affected, which is why the
  existing host-level test did not catch it.
- A second `let _ = update_index_metadata` in `uni-bulk`, found by the
  ratchet, in the file the same round had just fixed.
- `DROP INDEX ... IF EXISTS` in `uni-query` classifying not-found by matching
  the error's rendered text.

### Three sites audited and deliberately not "fixed"

Recorded because reclassifying is the same work as fixing, and because two of
them were on the list:

- **`Float64Column::get`** returns `()` for an out-of-range index. A
  documented contract that explicitly matches its sibling column type and is
  idiomatic for a scripting surface — not a swallowed failure.
- **The Rhai `col{i}` yield name.** The audit called it a fabrication that
  "would never match a natural-key row map". It is a documented shorthand:
  a manifest declares `yields: ["int"]`, the script returns `#{ col0: ... }`,
  the query says `YIELD col0`. **Rejecting it broke four passing tests**, which
  is how the mistake surfaced.
- **`triggers.rs` pending-vertex labels** is fixed only to the extent of being
  made loud. A vertex with no label in the tx L0 is skipped from the L1
  pre-existence probe and can be reported as CREATE rather than UPDATE;
  resolving it needs a label source that layer does not have.

### What this round is evidence for

**An audit's tiering is a hypothesis, and it is wrong in both directions.**
One listed site was unreachable, one was a documented contract, one was
three defects rather than one, and three real defects were absent from the
list entirely. The `col{i}` case is the sharpest: a fix shipped on the audit's
say-so broke working behaviour, and only the test suite disputed it.

**A ratchet finds what an audit misses, because it does not depend on someone
having looked.** `arch_fail_open.rs` found a site in a file that had just been
audited and fixed by hand, in the same session.

**Scope a ratchet to decisions, not to a class.** A general "no swallowed
error" scan would carry a budget of hundreds of ordinary `unwrap_or_default`
uses and the real entries would drown. Three narrow rules, each with a
canonical remedy, catch the recurrences without the noise.

### Recommended next

Unchanged, and now unblocked: **#214 with #240**, then **#239**, then **#224**
and the rest of Tier 4. #233 stays open for Tier 2 across the newly-audited
crates — ~7 sites, none of them wrong answers.
