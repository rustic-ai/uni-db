# Remaining follow-up triage — 2026-10-25

Scope: the October 23–24 P1/P2 handoff, its leftover working files, and the
remaining items in `issue_triage_2026_10_23.md`. This is a local triage, not a
verification of upstream issue/release status. The findings below preserve the
initial triage; implementation and validation updates follow.

## Implementation update

The three confirmed Uni defects are fixed locally:

- ID expressions normalize identity bits to signed Int64 before evaluation,
  including encoded/struct entities and `elementId`. Stored-ID predicate
  pushdown remains available for scans and traversal targets.
- Schemaless FTS scores one visible corpus with Lance's unindexed BM25
  tokenizer/scorer, after applying buffered replacements and tombstones.
  The former ignored #310 specification is now a regular regression test.
- Procedure inner queries share statement counters. Simple-executor and guest
  projection hosts also inherit the statement memory pool; nested declared
  calls inherit the procedure registry.

The focused Rust run passed all 8 tests, including the procedure memory-budget
test. Disabling signed ID normalization, shared inner-executor counters and
BM25 ranking made all 3 core regression tests fail with their expected
assertions; implementation sources were restored afterwards. Full validation
is recorded below; the focused run alone is not a claim of full CI completion.

The untracked #298 exploratory file was removed after checking its cases
against existing asserting coverage in `locy_is_ref_binding.rs`,
`locy_command_noninterference.rs`, `locy_scope_check.rs` and
`locy_clause_contract.rs`. No missing case required another probe binary.
The stale SLG wording and P2 handoff statuses were corrected. Existing
compliance artifacts and the website lockfile were left alone.

## Subsequent investigation of #310, #311 and #317

See [the follow-up evidence](issues_310_311_317_followup.md). Expanded #311
coverage reproduced a separate multi-label keyed-MERGE label-loss defect,
with a local binding fix; the original intermittent ordinary-SET report
remains unconfirmed. The revised [#317 hypothetical-query design](locy_business_actions_317.md)
withdraws the catalog/Action abstraction; native correlation is proposed, not implemented. The initial findings below preserve their
historical assessment.

## Findings and order of work

| Order | Item | Assessment | Next action and completion criterion |
|---|---|---|---|
| 1 | Virtual-node ID comparisons | **P2 correctness; reproduced.** The same row returns `id = -9223372036854775808` and `negative = false` for `id(node) < 0`. Reading the value alone is covered by an existing test; expression semantics are not. | Normalize identity representation before comparisons. Audit node/edge IDs through projection, WITH, equality, ordering, arithmetic and procedure round trips. Require agreement between projected IDs and expressions over them, while ordinary UInt64 values retain their separate overflow behavior. |
| 2 | #310 schemaless FTS | **P2 ranking correctness; reproduced.** Three documents with different lengths/frequencies all score `0.5`, both before and after flush. Previously labelled P3; elevate because ranking and score thresholds can silently mislead applications. | Prefer real BM25 over the visible L0+L1 corpus, reusing the declared-property scoring path's statistics/tokenization. If unsupported, explicitly refuse the operation rather than silently supplying a different score. Verify ranking, updates/deletes, flush/reopen and visibility boundaries. |
| 3 | Procedure inner-query counters | **P2 observability; reproduced.** Direct scan returns 12 rows and reports 12 scans/L0 reads; the declared procedure returns the same 12 rows but reports zero for both. | Pass the statement's shared counters into the inner Executor. Test direct versus wrapped work and nested/per-row calls without double counting. Audit other statement-scoped context at this constructor boundary. |
| 4 | #302 unconditional Candle dependency | **P3 dependency footprint; source-confirmed.** `uni-locy/Cargo.toml` unconditionally depends on candle-core/candle-nn; `neural.rs` uses them for safetensors loading and the linear classifier forward pass. | Preserve the public classifier/model-file contract while replacing this limited use, or feature-gate it with a documented compatibility decision. Validate shape/error handling and prediction parity, including extreme logits. Measure dependency/build savings rather than assuming them. |
| 5 | #311 BTree reads after SET | **Needs evidence, not a confirmed open defect.** Prior triage's deterministic matrix and stress runs did not reproduce it; no fresh reproduction attempted here. | Obtain the exact transaction, schema/index, data and read mode. Then target concurrent writers, multi-label nodes, MERGE/SET += and streaming reads. Do not claim fixed or implement a speculative patch. |
| 6 | #317 business-action schemas for ABDUCE | **P3 additive feature.** Existing ABDUCE graph-edit search is not the requested domain-action model. | Write a bounded design for action parameters, permitted edits, validation, cost and candidate search before implementation. Keep ASSUME-per-action as the current workaround; do not mix this feature into correctness fixes. |
| 7 | uni-xervo cold-cache download lock race | **P2 availability, sibling-repository follow-up.** Prior handoff reproduced 16/23 failures; not rerun here. Uni serializes model downloads in nextest. | Fix bounded lock-contention waiting/retry in uni-xervo; prove two processes sharing an empty cache both load successfully, and test timeout/cancellation. CI serialization is a mitigation only. |

Priorities above are local recommendations, not changes to GitHub labels.
P2 here means a real defect worth fixing ahead of dependency reduction and new
features; the evidence reviewed does not establish a remaining P0/P1.

## Evidence and useful entry points

- FTS: `crates/uni-store/src/storage/manager.rs::fts_search` explicitly uses
  word overlap for schemaless properties. `compute_text_relevance` reduces
  text to token sets, losing document length and term frequency. Existing
  ignored specification: `issue_310_schemaless_fts_scores_are_constant` in
  `crates/uni/tests/common/bugs/issue_310_fts_score_constant_before_index.rs`;
  registered as ON_DEMAND in `scripts/ci/ignored_tests.py`.
- Counters: `crates/uni-query/src/query/executor/procedure_host.rs::execute_inner_query`
  constructs a new Executor and inherits deadline, cancellation, memory and
  transaction read context, but not `self.counters`. Executor constructors
  allocate fresh counters; `Executor::set_counters` already exists.
- IDs: `crates/uni/tests/common/bugs/uint64_values_do_not_wrap.rs` distinguishes
  ordinary unsigned values from signed identity bits and tests a negative
  projected virtual ID plus a vEdge round trip. It does not assert `< 0`.
  Unsigned evaluation/coercion is a candidate mechanism, not a proven root cause.

The following probe was run successfully with
`bindings/uni-db/.venv/bin/python`. The installed extension identifies as
4.2.0 and was built October 23; it was not rebuilt for this triage. The
observations establish behavior in that build; source checks above establish
that the FTS and counter mechanisms remain present in the checkout.

```python
from uni_db import Uni

db = Uni.in_memory()
s = db.session()
q = """CALL uni.create.vNode(['L'], '{}') YIELD node
       RETURN id(node) AS id, id(node) < 0 AS negative"""
print([r.to_dict() for r in s.query(q)])
# [{'id': -9223372036854775808, 'negative': False}]

tx = s.tx()
for i, text in enumerate([
    'supplier', 'supplier supplier', 'supplier site code', 'customer order'
]):
    tx.execute('CREATE (:ElementVersion {name: $name, terms: $terms})',
               {'name': str(i), 'terms': text})
tx.commit()
q = """CALL uni.fts.query('ElementVersion','terms','supplier',50)
       YIELD node, score RETURN node.name AS name, score
       ORDER BY score DESC, name"""
print([r.to_dict() for r in s.query(q)])
db.flush()
print([r.to_dict() for r in s.query(q)])
# Both: names 0, 1, 2 all score 0.5.

tx = s.tx()
tx.execute('UNWIND range(1, 12) AS i CREATE (:Probe {i:i})')
tx.execute("""CALL uni.plugin.declareProcedure('t.scan',
    'MATCH (n:Probe) RETURN n.i AS i', 'READ', '[]', '[]')""")
tx.commit()
for q in ['MATCH (n:Probe) RETURN n.i AS i',
          'CALL t.scan() YIELD i RETURN i']:
    r = s.query(q)
    print(len(r), r.metrics.rows_scanned, r.metrics.l0_reads)
# Direct: 12 12 12. Procedure: 12 0 0.
```

## Leftover files and handoff cleanup

- `locy_issue_298_is_ref_value_column.rs` is an untracked exploratory probe,
  not an unfinished fix. It is not included in the integration module tree
  and catches errors only to print them. Compare its cases with the landed
  #298 regression coverage; migrate only missing cases into asserting tests
  in the existing integration binary. Do not simply wire it in as a passing
  test, since it currently accepts failure.
- Compliance changes are generated evidence/history rotation, not engine
  changes. Markdown summaries point to the earlier October 23 run while
  additional later JSON files exist. Reconcile each summary with the intended
  latest run before saving the artifacts; do not equate old markdown totals
  with the final P2 run. Keep meaningful old baselines deliberately.
- `website/uv.lock` raises the Python floor from 3.13 to 3.14 without a
  corresponding declared requirement change. Treat as unexplained lockfile
  drift until a docs-environment requirement justifies it; it is not evidence
  that the product now requires Python 3.14.
- Release readiness is a separate verification task: confirm actual published
  versions, API compatibility and the unreleased notes before choosing a
  version. The lazy async derived-facts API is a breaking change. Local
  version 4.2.0 alone does not establish what has shipped.

## Items that are not pending fixes

- The latest P1/P2 implementation batch is complete according to the handoff;
  the triage document's earlier “P2 work in progress” status is stale.
- REQUIRE is implemented in the current compiler; the older memory saying
  “decided, not built” is stale.
- The grammar's QUERY/SLG comment and the separate stale “SLG resolver”
  mention in `crates/uni-cypher/src/locy_ast.rs` have been corrected.
- WAL announcement recovery, CDC mid-commit registration, scan-runtime guarding
  and ignored-test lane coverage were completed in the recent handoff. Do not
  restart them from older “uncommitted” labels.
- Open-time event consumer registration, Python synchronous before-trigger
  support and unnecessary fork CDC extraction remain documented limits in
  `open_items_2026_10_22.md`, not newly reproduced regressions. Prioritize the
  builder-time registration gap if recovery-event delivery to custom consumers
  is required; otherwise schedule these separately from the three confirmed
  defects above.

The initial triage performed no implementation changes or full Rust suite
runs. The implementation update above records the subsequent authorized work.
No commits, GitHub writes or network release checks were performed.

## Validation

- Workspace nextest: **8,605 tests exercised**, 8,603 passed in the sandbox;
  the two failures were localhost `TcpListener::bind` calls denied by the
  sandbox. Both passed in a host-access rerun with the same workspace feature
  set: `without_otel_layer_no_traceparent_is_sent` and
  `http_egress::tests::redirect_is_not_followed`. No test was weakened or skipped
  to obtain those passes. The suite has 137 configured skips.
- The workspace run includes all 558 Locy TCK scenarios, the Locy differential
  oracle, the existing asserting coverage replacing the #298 probe, and the
  new signed-ID/null and BM25-cancellation unit tests.
- Formatting, Python Ruff checks and all 8 static guards passed.
- Python extension build: ordinary `maturin develop` could not write the
  shared uv cache; a temporary cache then exposed unavailable PyPI DNS.
  `maturin develop --skip-install` builds in place using the existing test
  environment, without changing or downloading its dependencies.
- Python pytest: **1,148 passed, 15 skipped**, using the rebuilt extension.
  The sandbox run stalled at async database open; a minimal probe reproduced
  that stall inside the sandbox and completed immediately with host access.
  The full host-access run completed in 86.49 seconds without code changes.
- Cypher TCK: **3,925 passed, 1 configured skip, 0 failed in each mode**
  (schemaless and sidecar), matching the baseline. Used
  `scripts/run_tck_with_report.sh --both '/.*/'` to execute every active
  scenario while retaining reports under `target/cucumber/*/filtered/`.
- Runner caveat observed during validation: the filtered TCK wrapper masks
  nextest's nonzero exit with `|| true`. An initial `~.*` filter selected no
  tests while the wrapper exited successfully. The corrected nextest regex
  `/.*/` was checked against discovery (3,926 scenarios), and the actual run
  summaries and aggregate counts were inspected. The wrapper itself was not
  changed in this implementation round.
- Workspace clippy with the runbook's platform excludes and `--all-targets
  -- -D warnings`: **passed**.
- Additional configuration check: `cargo check -p uni-store
  --no-default-features` **failed with 22 compiler errors**. With no backend
  enabled, Lance-specific signatures/imports remain unguarded (for example
  `storage/manager.rs::write_dataset_with_lance_conflict_retry`), index-manager
  vector types disappear, and the writer still calls `ensure_declared_indexes`.
  This no-backend configuration needs a separate feature/API audit or an
  explicit unsupported-configuration diagnostic. It is not a green validation
  lane, and was not expanded into the three agreed defect fixes. The default
  backend configuration passed the workspace, Python, TCK and clippy checks.
