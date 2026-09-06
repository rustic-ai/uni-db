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
| #233 | **Tier 2 done.** All three tiers now closed; the issue stays open for the ~25 unaudited plugin/CLI/bulk/CRDT sites it scopes out |
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
