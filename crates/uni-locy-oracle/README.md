# uni-locy-oracle

A **naive-Datalog reference oracle** for differential-testing the Locy engine.

Locy is a Datalog dialect, and Datalog's *naive fixpoint* — re-evaluate every rule
over every fact until nothing changes — is trivially correct and *semantically
identical* to the optimized semi-naive + `LeftAnti` algorithm the production engine
runs. This crate implements that naive evaluator as a reference oracle: generated
monotone-core programs are run through **both** the engine and the oracle, and their
derived fact sets must match exactly. A mismatch is, by construction, an engine bug
(this is how rustic-ai/uni-db#94 was found).

It exists to close gaps **G13** (silent under/over-derivation) and **G14**
(completeness failures masked as timeouts) from the test-expansion plan, and would
have caught the historical `IS NOT` complement bug — which derived 0 rows instead of
30 above the 300-fact dedup threshold — at *any* scale.

## The independence invariant (do not break)

The oracle's entire value is sharing **zero** evaluation code with the engine: if a
bug lived in code shared by both sides, the differential would stay green and the
oracle would be lying.

That invariant is enforced by the dependency graph, and **must stay that way**:

- The **library** (`[dependencies]`) depends only on `uni-common` (for the shared
  `Value` type) and `proptest`. It must **never** depend on `uni-db`, `uni-locy`, or
  `uni-query`.
- The engine (`uni-db`) is a **dev-dependency**, reachable only from
  `tests/differential.rs`, where it runs real Locy programs to diff against the oracle.

If you find yourself adding the engine to `[dependencies]`, stop — that defeats the
crate's reason to exist.

## Layout

| File | Role |
|------|------|
| `src/ir.rs` | The oracle's own minimal IR (`OracleProgram`/`OracleRule`/`OracleClause`/`IsRef`). |
| `src/eval.rs` | `evaluate()` — the naive fixpoint reference (join → anti-join → project → dedup to fixpoint). |
| `src/surface.rs` | Program text as structured data (`SurfaceProgram`), rendered under a `Renaming` and conjunct `Orders`. |
| `src/generator.rs` | Builders (`build_layered_dag`, `build_complement`, `build_union`, `random_program_strategy` with the #300 binding families and the P5 recursion families — non-linear and mutual strata with `BEST BY`, `FOLD`, `ALONG`, `IS NOT`) emitting the single-source-of-truth triple: base-graph Cypher, Locy program text (from the surface), and oracle IR. |
| `tests/differential.rs` | Engine↔oracle agreement: fixed cases, threshold proptest, completeness guard, complement/union, and the nightly soak. |
| `tests/differential/random.rs` | Random programs against the oracle (strict decoding by column kind, per-relation attribution, feature floors) and the `#[ignore]`d `teeth_refind_issue_300` teeth harness (passes only against a pre-fix engine). |
| `tests/differential/relations.rs` | Text-only metamorphic relations: conjunct permutation, alpha renaming, renaming into a collision, interleaved `YIELD`, and `EXPLAIN` agreement. |

## Scope

The oracle covers Locy's **monotone core** — plain rules, `IS` references, stratified
`IS NOT`, and `YIELD`. Non-core constructs (`FOLD` non-`M` aggregates, `ALONG`,
`BEST BY`, `DERIVE`, `ASSUME`, `PROB`, `HAVING`) are out of scope; the IR cannot
represent them and the evaluator panics rather than silently mis-handling them.

## Running

```sh
# Fast tests (also run on every PR via the workspace test job):
cargo nextest run -p uni-locy-oracle

# High-volume soak (nightly): thousands of programs, engine vs oracle.
ORACLE_SOAK_CASES=10000 cargo nextest run -p uni-locy-oracle \
  --run-ignored ignored-only -E 'test(/soak/)'
```

The soak volume (`ORACLE_SOAK_CASES`, default 10_000) is bounded by the engine,
not the oracle: each case pays a full `Uni::in_memory()` build, so the per-case
cost (~tens of ms) caps how many engine-diffs fit inside the nightly soak
profile's per-test kill window. The oracle's own correctness is validated against
closed-form cardinalities at much higher volume, cheaply, by the `eval.rs` unit
tests (no engine in the loop). Raising the engine-diff volume toward the plan's
aspirational ≥100k would require amortizing the DB build across cases or running
them in parallel — a deliberate follow-up.

Reproducing a failure: proptest prints the minimal failing `(stages, width)`; pass
it to the matching `build_*` builder and inspect `program_text` / `base_graph_cypher`
for a standalone repro.
