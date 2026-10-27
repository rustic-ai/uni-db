# Uni codebase correctness review

Review date: October 26, 2026. The seven findings below describe the original
failures. Local corrections and validation are recorded in the
[implementation report](review_fixes_2026_10_26.md).

See the [combined correctness and performance triage](review_triage_2026_10_26.md)
for the current disposition, implementation order, and verification gates.

Seven reproducible correctness issues affect transaction uniqueness, fork
isolation, MERGE, vector retrieval, and recursive queries. The three P1 findings
can introduce duplicate records or expose data outside a fork's snapshot. They
should be addressed before performance optimization. The four P2 findings return
incorrect or incomplete query results without reporting an error.

The findings were reproduced against the local Rust source using `cargo nextest`.
Eleven focused reproduction cases cover the seven issues, and 66 existing tests
in the surrounding areas passed. This was a targeted review of execution and
storage boundaries, not an exhaustive audit of every crate or a full workspace
validation. No separate performance defect is claimed from benchmark evidence.

## Findings and priorities

| Finding | Priority | Observed failure |
| --- | --- | --- |
| 1. Uniqueness checks after flush | P1 | Two conflicting transactions both commit after an intervening flush |
| 2. Fork reads of previously absent tables | P1 | A fork sees rows written later on primary |
| 3. Anonymous MERGE deduplication | P1 | Three identical MERGE inputs create three nodes |
| 4. Vector candidates after buffered mutations | P2 | Updating or deleting the nearest vector hides the next valid neighbor |
| 5. Vector distance metric on forks | P2 | A cosine query chooses an L2-nearest candidate |
| 6. Vector search after label removal | P2 | Search returns a node that no longer carries the requested label |
| 7. Recursive query iteration limit | P2 | A successful query omits reachable nodes after 1,000 iterations |

P1 means an integrity or isolation failure to prioritize for correction. P2 means
a confirmed query correctness defect requiring a focused fix and regression
coverage. These priorities are local review assessments.

## Relevant execution and storage paths

The public `Session` and `Transaction` APIs in `crates/uni` coordinate query
execution and transaction lifetime. `uni-cypher` parses queries; `uni-query`
plans and executes them, including specialized mutation and search paths.
`uni-store` manages transaction-private L0 buffers, committed buffers, the WAL,
and persistent Lance datasets. A flush moves committed data from L0 into L1.

Forked sessions use `BranchedBackend` to select a dataset branch. Vector search
selects candidates from persistent storage, reconciles buffered state, and ranks
the result. Recursive CTE execution repeatedly evaluates a subplan until no new
rows remain or its iteration cap is reached.

These boundaries explain why ordinary cases can pass while the failures below
remain: a flush changes where conflict checks find data; a missing branch changes
where reads are routed; and a buffered mutation changes whether a previously
selected search candidate is still eligible.

## 1 Uniqueness checks lose protection after flush

**Priority: P1.** Concurrent transactions can commit duplicate declared UNIQUE
keys, duplicate `ext_id` values, and duplicate named MERGE creations.

**Reproduction sequence:**

1. Declare `Account.key` as UNIQUE and open transactions A and B on separate sessions.
2. Both execute `CREATE (:Account {key:'same'})` before either commits.
3. Commit A, then call `db.flush()`.
4. Commit B and query the accounts.

Both commits succeed and two nodes contain the same key. Removing step 3 causes
B to abort with `ConstraintConflict`, as expected. The same control and failure
were reproduced with `ext_id:'same'` and with
`MERGE (n:Account {key:'same'})` without a declared UNIQUE constraint.

**Cause:** [`Writer::commit_transaction_l0_sequenced`](../../crates/uni-store/src/runtime/writer.rs#L1960)
checks constraint, MERGE, and external-ID keys against only the current and
pending L0 buffers. Once the conflicting commit has finished flushing,
[`L0Manager::complete_flush`](../../crates/uni-store/src/runtime/l0_manager.rs#L344)
removes its buffer from the pending list. Commit validation does not recheck the
persisted conflicting keys. Item-level write conflicts do not catch these
inserts because they allocate different vertex IDs.

**Fix direction:** Preserve key-conflict protection independently of L0 residency,
or revalidate keys against current persistent state under commit serialization.
The rule must cover declared uniqueness, external IDs, and implicit MERGE keys.

**Acceptance:** The second conflicting commit must reject consistently with or
without a completed intervening flush. Keep the existing no-flush cases, and
extend coverage to edge uniqueness and NodeKey constraints before considering
the shared validation path fully corrected; those extensions were not separately
reproduced in this review.

## 2 Forks read primary tables that did not exist at creation

**Priority: P1.** A fork can observe primary writes made after its creation.

**Reproduction sequence:**

1. Declare an `Item` label but leave it empty.
2. Create a fork.
3. On primary, create `(:Item {name:'primary-later'})`, commit, and flush.
4. Query `MATCH (n:Item) RETURN n.name AS name` through the fork.

The fork returns `primary-later`; it should return no rows. This fails both when
the entire database was empty at fork creation and when another label already
contained data.

**Cause:** [`build_datasets_for_fork`](../../crates/uni/src/api/fork.rs#L650)
skips datasets that do not exist. Later,
[`BranchedBackend::apply_branch`](../../crates/uni-store/src/backend/branched.rs#L87)
leaves the request unbranched when the table has no recorded branch, so the
wrapped backend reads primary's current table. The fork does not preserve the
table's absence as part of its snapshot.

**Fix direction:** Represent absent-at-creation datasets explicitly and route
their reads to an empty inherited view. Dynamic branch creation must also respect
that view rather than inheriting primary's later contents.

**Acceptance:** Both reproduced cases remain empty on the fork after primary
flush. Extend coverage to fork writes into such a table, reopen, nested forks,
and edge tables so the correction preserves branch lineage throughout its
lifetime.

## 3 Anonymous MERGE skips deduplication

**Priority: P1.** A valid MERGE statement creates duplicate nodes when the pattern
does not name its node.

With a declared `Account` label and string `key` property, execute:

```cypher
UNWIND range(1,3) AS i
MERGE (:Account {key:'same'})
```

After commit, `MATCH (n:Account) RETURN count(n)` returns **3**, rather than **1**.
The control using `MERGE (n:Account {key:'same'})` creates one node. Initial Python
probes also showed that two concurrent anonymous MERGE transactions both commit;
the Rust regression isolates the simpler single-statement failure.

**Cause:** [`execute_merge_row_indexed`](../../crates/uni-query/src/query/executor/write.rs#L2244)
adds the created vertex to the statement's `existing` lookup and registers its
MERGE conflict key only inside `if let Some(var) = &node.variable`. Anonymous
patterns therefore skip both operations, and subsequent input rows do not find
the node just created by that statement.

**Fix direction:** Track the created vertex independently of a user-provided
variable, using an internal binding or an explicit creation result.

**Acceptance:** Named and anonymous patterns each create one node for repeated
keys in the same statement, and anonymous patterns receive the same concurrent
MERGE conflict protection as named patterns. Keep this separate from finding 1:
the flush failure also occurs with a named node and a correctly registered guard.

## 4 Buffered vector changes hide the next valid neighbor

**Priority: P2.** Vector top-k results depend on whether a committed mutation has
been flushed.

Create and flush two `Doc` nodes with a declared two-dimensional vector property:
`a.v = [0,0]` and `b.v = [1,0]`. Then commit one of the following mutations without
flushing and query the nearest node to `[0,0]` with `k=1`:

| Mutation to node a | Expected | Observed before flush | Observed after flush |
| --- | --- | --- | --- |
| `SET a.v = [100,0]` | Node b | Node a, now much farther away | Node b |
| `DELETE a` | Node b | No rows | Node b |

**Cause:** [`StorageManager::vector_search`](../../crates/uni-store/src/storage/manager.rs#L4664)
retrieves persistent candidates before merging L0 changes. Its widening loop
handles superseded persistent row versions, but does not replenish candidates
after L0 changes invalidate or worsen the selected hits. For `k=1`, disk returns
only a; the merge has no candidate b to return.

**Fix direction:** Account for buffered replacements and deletions during
candidate retrieval, or refill candidates using a stopping condition that
preserves the requested retrieval semantics. A fixed over-fetch factor alone
does not guarantee correctness for the exact search path.

**Acceptance:** Both mutations return b before and after flush. Cover larger k,
multiple invalidated hits, transaction-private mutations, and pending flush
buffers. The two Rust probes retain the after-flush control.

## 5 Fork vector search drops the distance metric

**Priority: P2.** A fork can return a different nearest neighbor from primary
despite having the same data and cosine configuration.

Declare a flat cosine vector index on `Doc.v`, insert `[100,0]` and `[1,0.1]`, and
create a fork. Query both sessions with:

```cypher
CALL uni.vector.query('Doc','v',[1.0,0.0],1)
YIELD node, distance
RETURN node.name AS name, distance
```

Primary chooses `[100,0]` with cosine distance 0. The fork chooses `[1,0.1]` with
reported cosine distance approximately 0.00496286. The latter is closer in L2
distance, but is not the nearest cosine neighbor.

**Cause:** [`vector_search_on_branch`](../../crates/uni-store/src/backend/lance_branch.rs#L279)
does not accept the metric and calls `nearest()` without setting it on the Lance
scanner. Primary's
[`vector_search`](../../crates/uni-store/src/backend/lance.rs#L1440)
sets the metric explicitly. Exact rescoring at the storage-manager layer happens
after candidate selection and cannot recover a candidate already excluded.

**Fix direction:** Thread the requested metric through the branching capability
and apply it when configuring the scanner.

**Acceptance:** Primary and fork agree on the exact cosine case above. Extend
coverage to dot product, indexed paths, and fork-local data with non-unit vector
magnitudes so the tests distinguish the distance metrics.

## 6 Vector search retains hits after label removal

**Priority: P2.** Search can return a node outside the requested label.

Create and flush nodes a and b from finding 4. Commit `REMOVE a:Doc` without a
flush, then query `uni.vector.query('Doc','v',[0,0],1)`. The search still returns a.
After flush, it correctly returns b.

**Cause:** [`merge_l0_into_vector_results`](../../crates/uni-store/src/storage/manager.rs#L5882)
skips buffered vertices whose current labels do not include `Doc`, but does not
use that label removal to invalidate their older disk candidates. A skipped
candidate is not equivalent to a tombstoned or explicitly excluded one.

**Fix direction:** Reconcile current label membership for stored hits as well as
their properties and deletion state. Compose this with candidate replenishment
from finding 4 so excluding a does not merely turn the answer into an empty set.

**Acceptance:** The search excludes a and returns b before flush. Extend coverage
to multi-label nodes and label changes in transaction and pending-flush overlays.

## 7 Recursive CTEs silently truncate at the iteration limit

**Priority: P2.** A recursive query can report success while omitting reachable
nodes.

Build a linear chain of 1,002 `Item` nodes, numbered 0 through 1,001, connected by
`NEXT` edges. Evaluate:

```cypher
WITH RECURSIVE reach AS (
    MATCH (root:Item {i:0}) RETURN root
    UNION
    MATCH (a:Item)-[:NEXT]->(b:Item)
    WHERE a IN reach
    RETURN b
)
MATCH (n:Item) WHERE n IN reach
RETURN count(n) AS c, max(n.i) AS last
```

The query succeeds with `c = 1001` and `last = 1000`. The complete result is
`c = 1002` and `last = 1001`.

**Cause:** [`run_cte_loop`](../../crates/uni-query/src/query/df_graph/recursive_cte.rs#L316)
runs at most 1,000 recursive iterations, then builds a normal result regardless
of whether it reached a fixed point. An anchor row plus 1,000 newly discovered
nodes produces the observed partial answer.

**Fix direction:** Distinguish convergence from exhaustion of the iteration
budget and surface an explicit limit error when work remains. Any future
configurable limit must preserve this distinction.

**Acceptance:** The chain query either returns all 1,002 nodes or reports an
iteration-limit error. Test chains around the boundary and cyclic graphs that
converge through deduplication. The preserved probe accepts an explicit
iteration error and rejects successful partial output.

## Reproductions and validation

The [evidence directory](codebase_review_2026_10_26/) preserves the
[11 Rust probes](codebase_review_2026_10_26/regressions.rs), raw test logs, a
portable runner, and hashes of the source files associated with the findings.
The probes are outside the normal test module tree because they intentionally
assert behavior that the reviewed implementation fails.

From the repository root, run:

```bash
python3 docs/proposals/codebase_review_2026_10_26/run_reproductions.py
```

The runner temporarily registers the probes in the existing
`crates/uni/tests/integration.rs` binary, invokes `cargo nextest`, and removes
that registration on exit, including test failure. It creates no extra test
binary. It clears `RUSTC_WRAPPER` because the review environment denied access to
the configured sccache process. On the original reviewed source, the outcome was 11 failing correctness
assertions. The corrected implementation is expected to pass these cases; they
also run as regular regressions in the consolidated integration suite.

The preserved logs record separate runs rather than one combined 11-test run:

| Evidence | Result and interpretation |
| --- | --- |
| [Initial results](codebase_review_2026_10_26/initial_results.txt) | Four failing probes: two fork-isolation cases, fork cosine search, and recursive truncation. The broad filter also selected one existing passing test. |
| [Vector and constraint results](codebase_review_2026_10_26/vector_and_constraint_results.txt) | Nine failing probes, including the three vector mutation cases and UNIQUE/ext_id flush cases. The MERGE helper initially used an anonymous node and failed its no-flush control, revealing finding 3. |
| [Merge and constraint results](codebase_review_2026_10_26/merge_and_constraint_results.txt) | The corrected named-MERGE flush probe, UNIQUE flush probe, ext_id flush probe, and dedicated anonymous-MERGE batch probe all fail their intended assertions. Three existing MERGE/atomic-SET tests pass. |
| [Existing test results](codebase_review_2026_10_26/existing_test_results.txt) | 63 existing tests pass across fork vector search, fork schema evolution, SSI/OCC, vector predicate exactness, and recursive CTE execution. |

The 66 existing neighboring passes are the 63-test run plus the three existing
tests in the final MERGE run; the extra pass selected by the initial broad
filter is not included in that total. Controls inside the new probes verify
the no-flush conflict behavior, named MERGE deduplication, primary cosine search,
and correct vector answers after flush.

The preserved probe file contains the final named-MERGE correction. Historical
logs retain their original `/tmp` source paths and line numbers; use test names
to match them to the formatted preserved source. Source hashes in
[`reviewed_source_sha256.json`](codebase_review_2026_10_26/reviewed_source_sha256.json)
identify the relevant file contents without relying on a Git revision. No Git
commands were used during this review.

## Suggested implementation order

Fix transaction key protection and absent-table fork isolation first, followed
by anonymous MERGE deduplication. Take the focused recursive iteration-limit
error and fork vector metric corrections before the larger vector reconciliation
work. Correct vector membership and candidate replenishment together, while
retaining separate regression cases for each failure. The
[combined triage](review_triage_2026_10_26.md) explains the dependencies and
performance priorities.

For each fix, promote the corresponding probes into the existing consolidated
integration suite and expand the boundary coverage described above. A passing
probe is necessary evidence for its finding; the broader acceptance cases are
needed to establish that the same mechanism is correct across storage states.
