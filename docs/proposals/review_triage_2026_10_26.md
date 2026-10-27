# Uni correctness and performance triage

Assessment date: October 26, 2026. This preserves the triage before implementation.
See the [corrections and validation](review_fixes_2026_10_26.md) for current local
implementation status. Scope: the seven findings in the
[correctness review](codebase_review_2026_10_26.md) and three findings in the
[performance review](performance_review_2026_10_26.md).

Prioritize transaction uniqueness across flush and fork snapshot isolation.
Both violate storage guarantees, and their corrections need careful design.
Anonymous MERGE is also a P1 integrity defect, with a narrower trigger and a
more focused correction. Make recursive budget exhaustion explicit before
optimizing recursion. Correct vector eligibility and candidate replenishment
together, retaining separate tests for each failure.

The performance findings are confirmed algorithmic problems. Their production
urgency depends on workload exposure: recursive CTEs suffer repeated label scans;
large buffered deletion batches perform repeated map or adjacency scans under
the commit lock. Development-build timings establish the observed cost and
scaling, but cannot establish release latency or a production throughput loss.

## Evidence and confidence

At triage, all 16 distinct files covered by the two source manifests matched
their reviewed hashes. The relevant source paths were inspected again for that
assessment.
The baseline evidence comprised: 11 failing correctness reproductions,
66 passing neighboring tests, and five passing performance probes with 42 cases
of three timing samples each. Tests were not rerun for this assessment because
the reviewed source was unchanged at triage. This is local source validation, not an
assessment of an upstream release or deployment.

Use `C1` through `C7` for the original correctness finding numbers. `F1` through
`F3` identify the performance findings in their original order. Severity **P1** means an integrity or isolation
defect to address first. Severity **P2** means a confirmed query-result or scaling
defect with a more bounded trigger. Difficulty and risk are qualitative source
assessments, not implementation estimates.

## Findings and disposition

| ID | Finding | Severity | Exposure and consequence | Fix scope and risk | Disposition |
| --- | --- | --- | --- | --- | --- |
| C1 | Key conflicts disappear after flush | P1 | Two overlapping transactions insert the same key; the winner flushes before the loser commits. Duplicate UNIQUE, ext_id, and named-MERGE records survive commit. | High: protect keys across L0, pending flush, and L1 without moving validation after the durable commit point. | Highest priority. Cover all three reproduced key classes; expand to edge and NodeKey constraints. |
| C2 | Fork reads a table absent at creation from live primary | P1 | Primary populates and flushes a previously absent table. An existing fork sees post-creation rows. | High: preserve absence across reads, first fork write, reopen, and nested branch creation. | Highest priority alongside C1. Correct the snapshot model rather than only the observed scan. |
| C3 | Anonymous MERGE skips intra-statement deduplication | P1 | Repeated inputs to `MERGE (:Account {key:'same'})` create three nodes instead of one. Concurrency is not required. | Low to medium: obtain created identity independently of a user variable; preserve binding and ON CREATE behavior. | Focused integrity fix. Keep separate from C1. |
| C4 | Vector changes invalidate the selected top-k without refill | P2 | A committed, unflushed update or deletion makes the disk winner unsuitable; the next valid disk neighbor is missing. | High: preserve exact-search behavior across overlays and filters without assuming fixed over-fetch guarantees completeness. | Implement with C6 reconciliation; retain update and delete regressions. |
| C5 | Fork vector search drops the distance metric | P2 | The reproduced flat cosine path selects an L2-nearest candidate on the fork. Primary selects the correct cosine neighbor. | Low to medium: thread the metric through the branching API and scanner, checking implementations and callers. | Focused correction independent of C4/C6. Test non-unit vectors and multiple metrics. |
| C6 | Vector search retains a hit after label removal | P2 | An unflushed `REMOVE n:Doc` leaves its persisted vector eligible for a Doc search. | Medium: invalidate old hits using current label membership, respecting overlay precedence. | Implement with C4. Removing an invalid hit alone does not restore the next valid answer. |
| C7 | Recursive CTE returns successful partial results at its cap | P2 | A 1,002-node chain succeeds with only 1,001 nodes. | Low for explicit budget failure; higher for changing recursion semantics or limits. | Move the focused error correction ahead of the larger recursion optimization. |
| F1 | Recursive CTE rescans the label every iteration | P2 | A 900-node chain visits 811,800 rows; the bounded traversal control visits 900. | High: make the frontier drive physical execution and preserve query semantics. | Optimize after C7. Elevate within the performance queue when recursive CTEs are a major workload. |
| F2 | Vertex deletion repeatedly scans constraint keys | P2 | Large L0 deletion batches repeatedly scan keys belonging to unrelated vertices. Measured 16,000-node commit: 1,459 ms versus 115 ms without a constraint. | Medium: the reverse vertex-key map exists; ownership, key reuse, replay, and related maps need care. | First general performance fix after correctness work. Keep the initial scope focused. |
| F3 | Edge deletion repeatedly scans a hub adjacency list | P2 | Deleting many edges sharing an endpoint repeatedly filters a shrinking vector. Measured 16,000-edge commit: 1,203 ms versus 110 ms for disjoint edges. | Medium to high: batched retirement or position tracking affects traversal and cascade invariants. | Follow key retirement; elevate for hub-heavy graph mutation workloads. |

There is no basis in this evidence for a P0 outage classification, an estimated
production incidence rate, or treating every query as affected. None of the ten
findings is dismissed as speculative. Broader cases mentioned in acceptance
criteria are investigation targets, not additional reproduced defects.

## Recommended order of work

1. **C1 and C2: resolve storage guarantees.** Treat these as the first design and
   validation priorities. Their broad contracts justify deeper review than a
   one-case patch.
2. **C3: restore anonymous MERGE bookkeeping.** Add the created identity to the
   statement lookup and conflict registration without depending on a named
   variable. This can be corrected independently of the larger storage work.
3. **C7, then C5: take the focused query corrections.** Reject an exhausted
   recursive budget when convergence has not been established. Pass the metric
   through fork search. Their small scope justifies doing them before a larger
   vector or recursion rewrite, despite their P2 severity.
4. **C4 and C6: reconcile vector eligibility and replenish candidates.** Treat
   membership, deletion, replacement, and ranking as one retrieval contract.
5. **F2, then F3: reduce work under the commit lock.** Begin with the existing
   reverse key map; then address repeated adjacency retirement. Measure the
   entire transaction as well as its commit phase.
6. **F1: execute recursion from its frontier.** Preserve C7's explicit failure
   semantics. Optimize physical input and scans before spending effort on
   smaller cloning or planning costs.

This is a default queue, not a dependency forcing every focused correction to
wait for a larger redesign. Production workload evidence can reorder F1–F3;
the largest debug timing ratio alone is not a sound prioritization rule.

## Design decisions and dependencies

### Protect keys independently of buffer residence

C1's validation runs with SSI enabled, which is the current default. It checks
only current and pending L0 buffers. Disabling SSI or postponing flush is not a
correction. A solution must validate against current committed ownership,
including persisted keys, while holding the commit serialization needed to
prevent another race. Validation must complete before durable WAL commit.

Choose between a current-state ownership lookup and a conflict-history mechanism
only after defining deletion, key reuse, and history retention. An in-memory
journal can protect overlapping writers if it retains every relevant commit
since the oldest active writer's read point, with the existing persistent-key
checks still correct. Active transactions do not survive restart, so such a
journal does not inherently require durable history. Test recovery and reopening
against persisted owners either way. Reuse an ownership index only after
verifying its uniqueness namespace and branch isolation.

C3 still needs its own fix: a key that anonymous MERGE never registers cannot be
protected by improving C1's commit checks. Conversely, naming the MERGE variable
does not fix C1. F2 touches related key bookkeeping, so its optimization must
preserve the ownership rules established by the correctness tests.

### Preserve absent datasets as part of a fork snapshot

C2 is more than a missing branch selection on one read. An unmapped table may be
absent at creation, newly created on the fork, or inherited through a parent
fork. Returning empty for every unmapped table would conflate those states.
Branching primary's current table on the first fork write would inherit the same
later data the snapshot must exclude.

Represent these states explicitly and persist required metadata through
`ForkRegistryHandle`, preserving the registry single-writer and 2PC invariants.
Validate read and write routing, not just counts from the original query. The
observed failure is snapshot isolation; the evidence does not establish an
authentication or tenant-authorization bypass.

### Separate vector eligibility from candidate completeness

C6 decides whether a persisted hit belongs in the current label. C4 decides
whether enough valid candidates remain to answer top-k after overlays change
those hits. Both are necessary. Final rescoring cannot recover a neighbor that
the candidate generator excluded, which is also why C5 matters independently.

For the reproduced exact path, define a refill stopping rule that establishes
the requested top-k result or exhausts valid candidates. A fixed multiplier is
not a correctness guarantee when arbitrarily many initial hits can be invalid.
For ANN paths, distinguish approximate recall from invalid membership or stale
overlay handling; do not promise exact ANN recall as part of this fix.

### Keep recursion termination separate from optimization

C7 can be corrected without redesigning the working table: track whether
execution established convergence. If the budget ends before that is known,
return an explicit iteration-limit error. Do not turn the cap into a much larger
constant as a substitute for this distinction. A boundary case may have collected
all rows without yet having executed the round proving convergence; tests should
permit a budget error there rather than demand an extra unbudgeted iteration.

F1 then replaces repeated label scans with execution driven by the frontier.
The current measurements establish repeated visits, but do not isolate planning
cost from execution cost. Removing replanning alone is not evidence that the
quadratic scanning problem has been resolved.

## Verification gates

| Work | Required evidence before considering it complete |
| --- | --- |
| C1 | Preserve the no-flush rejection control; reject conflicting UNIQUE/ext_id/MERGE inserts across pending and completed flushes. Add edge/NodeKey cases, delete-and-reuse, updates to an existing key, and persistent ownership after reopen. Verify rejection precedes durable commit. |
| C2 | Keep both original fork cases empty. Cover first fork write after primary materialization, reopen, nested forks, and edge tables; ensure primary and sibling views remain correct. |
| C3 | Named and anonymous MERGE agree for repeated input keys; assert one final row and correct ON CREATE effects. Add concurrent anonymous MERGE coverage and combine it with C1's flush scenario. |
| C4/C6 | Return the expected next eligible neighbor after update, deletion, or label removal, before and after flush. Cover k greater than one, many invalidated hits, multi-label nodes, private transaction overlays, pending buffers, and predicates. |
| C5 | Primary and fork agree on exact cosine/dot/L2 examples designed to distinguish metrics; check inherited and fork-local data and flat/indexed paths. |
| C7 | Test convergence below and at the budget boundary, exhaustion with remaining work, and cycles. Successful output must be complete; budget failure must be explicit. |
| F2/F3 | Assert identical results and preserved mutation semantics; measure increasing sizes in an optimized build. Include small deletions from large maps, star and disjoint graphs, explicit and cascade deletes, and unaffected concurrent commit latency. |
| F1 | Preserve exact results and limit behavior. Vary chain depth and unrelated same-label population independently; demonstrate removal of the repeated full scans using counters and optimized timings. |

Move accepted regression cases into the existing consolidated integration suite,
using `cargo nextest`. Performance regressions should primarily assert operation
counts or robust scaling properties; hardware-specific debug millisecond limits
would be fragile. Public performance runs should retain execution, commit, and
total-transaction measurements, since a large commit ratio can overstate the
end-to-end ratio.

## Operational scope of the measurements

The correctness controls deliberately use flush to expose storage-boundary
behavior. The defaults already permit automatic flush, so C1 is not limited to
applications that explicitly call `flush()`. The schedule was made deterministic
in the reproduction; its real-world frequency has not been measured.

The performance probes disable automatic flush and keep fixtures in L0. The
16,000-item batches exceed the current default 10,000-mutation threshold and
are not representative of every default-configured deletion. The repeated
scans are real, but their practical exposure depends on resident buffer size,
transaction size, constraints, and degree distribution. Concurrent commit delay
is implied by the shared lock scope; no concurrent throughput measurement has
yet quantified it. Optimized and workload-representative measurements are the
next performance gate, not grounds to discard the algorithmic findings.

No engine fixes or new runtime tests were added during this triage. The review
documents and preserved reproductions remain the detailed evidence for each
finding; this document supplies the combined priorities and completion criteria.
