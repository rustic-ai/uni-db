# Locy Errors and Limits

## Common Error Classes

- Parse errors: invalid Locy syntax.
- Compile errors: invalid dependencies, type/schema mismatches, stratification violations.
- Runtime errors: timeout, iteration limit, memory constraints, mutation constraints, cancellation.

A program refused for cost gets a dedicated error, never the generic query error a broken program raises, so a caller can tell "too expensive" from "wrong" without reading the message:

| Refusal | Rust | Python |
|---|---|---|
| Timeout (the evaluation's `timeout`, or the database `query_timeout` when unset) | `UniError::Timeout { timeout_ms }` | `UniTimeoutError` |
| Memory pool (`max_memory`) or a relation over `max_derived_bytes` | `UniError::MemoryLimitExceeded { limit_bytes, message }` | `UniMemoryLimitExceededError` |
| Stopped at a stratum / iteration boundary | `UniError::LocyIncomplete` | `UniLocyIncompleteError` |

Cypher raises the same variants for the same conditions.

Cancelling the token attached to an evaluation aborts it with `UniError::Cancelled` (`UniCancelledError` in Python). Enforcement races the whole evaluation, so a long-running fixpoint is interruptible rather than only checked at statement boundaries.

## Operational Limits (via `LocyConfig`)

- `max_iterations`: recursion cap per recursive stratum.
- `timeout`: overall evaluation budget. When set, it also replaces the database `query_timeout` for the program's operators, whether above or below it. It covers the whole program: the evaluations it runs again after a write, ASSUME bodies and ABDUCE candidates share what remains of it, and every command runs within it.
- `allow_partial`: `false` (default) makes an evaluation that hits `timeout` or `max_iterations` fail with `LocyIncomplete`; `true` returns the partial result instead, with its `incomplete` diagnostics filled in for the caller to check.
- `max_derived_bytes`: derived fact memory bound.
- `max_explain_depth`: derivation tree depth bound.
- `max_slg_depth`: deprecated, no effect (QUERY reads the fixpoint's facts); setting it warns, and it will be removed.
- `goal_directed`: `Auto` (default) answers a `QUERY` whose WHERE pins a node KEY column by deriving only what it reaches, and derives a rule only such goals needed on the first read of `.derived`, against the evaluation's snapshot, so `.derived` is unchanged (programs that write, `EXPLAIN RULE` and transactions are evaluated in full); `Off` derives every rule in full; `On` does the rewrite and leaves rules only such goals needed out of `.derived`; `Shadow` does the rewrite but evaluates every rule. From Python, pass the lowercase string `"auto"` / `"off"` / `"on"` / `"shadow"`.
- `returned_rules`: `All` (default) returns every rule's facts in `.derived`; `Nothing` returns none; `Only(names)` returns the named rules. Each returned fact is converted to a row and its node and edge ids replaced by the full entities, so a program that derives many facts and reads few of them back is faster with fewer returned. Commands (`QUERY`, `DERIVE`, ...) are answered the same, and `stats.derived_nodes` still counts every fact. From Python, pass `"all"`, `"none"` or a list of rule names.
- `max_abduce_candidates` / `max_abduce_results`: abduction search bounds.
- `deterministic_best_by`: enforce deterministic tie-breaking in `BEST BY` clauses.
- `strict_probability_domain`: reject probability inputs outside `[0, 1]` instead of clamping.
- `probability_epsilon`: MPROD threshold for switching to log-space accumulation.
- `exact_probability`: enable BDD-based exact evaluation for shared-proof aggregate groups.
- `max_bdd_variables`: cap per-group BDD complexity before fallback.
- `semiring`: `AddMultProb` (default; noisy-OR and product), `MaxMinProb` (max / min, fuzzy), `BddExact`, or `TopKProofs { k }`. From Python: `"add_mult_prob"`, `"max_min_prob"`, `"bdd_exact"` or `{"top_k_proofs": k}`. See [Probabilistic Logic](../advanced/probabilistic-logic.md#topkproofs-semiring).
- `top_k_proofs`: keep at most `k` recorded derivations per derived fact for `EXPLAIN RULE`, the heaviest first (0 keeps all). A derivation's weight, its `proof_probability`, is its own PROB value: its local weight times every probability it referenced.

## Warning Codes

Locy has two warning channels. **Runtime warnings** (`RuntimeWarningCode`) surface in `result.warnings` at evaluation time. **Compile-time warnings** (`WarningCode`) surface in `compile_warnings` when the program is compiled. Both are informational; the program still runs.

### Runtime warnings (`result.warnings`)

- `SharedProbabilisticDependency`: multiple proof paths inside one MNOR/MPROD group reuse shared evidence, and the value was computed as if they did not — exact mode was off, or a group exceeded `max_bdd_variables` and fell back. The result may overestimate. `RuntimeWarning::probability` carries the structured diagnostic: `inference_mode` (`approximate`, `exact`, `top_k_proofs`), `resolution` (`not_attempted`, `fallback`), the `reason`, the groups detected and fallen back, and the configured and effective variable limits.
- `SharedDependencyResolved` (informational, `RuntimeWarningCode::is_informational`): proof paths share evidence, and exact or top-k proof inference evaluated every shared group without the independence assumption (`resolution: completed`). Reported instead of `SharedProbabilisticDependency`, whose overestimation premise does not hold.
- `BddLimitExceeded`: exact mode was enabled, but the group exceeded `max_bdd_variables` and fell back to independence mode.
- `CrossGroupCorrelationNotExact`: shared evidence spans multiple aggregate groups; each group is exact internally, but correlation across groups is still approximate.
- `FuzzyNotProbabilistic`: `LocyConfig.semiring = MaxMinProb` is active and a rule emits `PROB`. Fuzzy-truth math is being applied to a column declared as a probability; either pick the right semiring or drop the `PROB` annotation.
- `TopKPruningCrossedDependency`: under `TopKProofs(k)`, pruning dropped a proof that shared a base fact with a kept proof. The kept set is an approximation; increase `k` for exactness.
- `EntityHydrationIncomplete`: KEY columns of the named rule carry raw integer node ids instead of full nodes, because the id lookup could not finish; reading node properties from those rows finds nothing.
- `ProbabilityReadNotFactor`: a clause's `PROB` expression reads the probability of a positive `IS` reference other than as one product factor (`1 - p`, `p * p`). The referenced probability is not multiplied in again, and exact mode treats each such row as one independent event. See [Probabilistic Logic](../advanced/probabilistic-logic.md#positive-is-references-to-a-prob-rule).

### Compile-time warnings (`compile_warnings`)

- `SharedNeuralInputArgument`: two or more model invocations in the same rule receive the same INPUT variable argument, so their outputs are correlated. Downstream `MNOR` under-estimates joint risk. Mark the models `@independent` to suppress.
- `SharedNeuralFeatureValue`: two or more model invocations in the same rule share an equivalent feature value expression — the same correlation concern even when binding variables differ. `@independent` suppresses.
- `SharedRetrievalContext`: multiple `similar_to`/`semantic_match` features in the same rule share the same query embedding. The features are not independent of each other; the rule's joint composition may be biased.
- `UncalibratedNeuralPredicate`: a rule invokes a PROB model declaring no `CALIBRATION` (or `CALIBRATION none`); the uncalibrated probability compounds miscalibration downstream.
- `UncalibratedLLMLogprobs`: an uncalibrated `CREATE MODEL` whose `xervo_alias` looks like an LLM provider — raw logprobs are not calibrated probabilities.
- `MsumNonNegativity`: an `MSUM` in a recursive rule takes an argument that is not a literal, so it may be negative; `MSUM` converges only over non-negative values.
- `ProbabilityDomainViolation`: an `MNOR` or `MPROD` in a recursive rule takes a non-literal argument, which may fall outside `[0, 1]`. A static hint only: values are not inspected. At run time an out-of-range input is clamped, or rejected under `strict_probability_domain`.
- `FoldInRecursivePath`: a clause has a recursive IS-ref and a FOLD aggregate but no ALONG. The rule rolls up per KEY, composing level by level; use `ALONG` if you meant to accumulate along each path.
- `HavingInRecursivePath`: a recursive rule filters its FOLD result with a post-FOLD `WHERE`, which filters the converged answer and does not constrain the recursion. Use `REQUIRE` if the threshold is part of the definition. See [ALONG, FOLD, BEST BY](../advanced/along-fold-bestby.md#caveat-in-a-recursive-rule-it-does-not-constrain-the-recursion).
- `CountFoldSeedCounted`: a clause without a FOLD seeds a `COUNT` / `MCOUNT` column with a non-NULL literal (`0 AS n`); the seed counts as one row, so the key comes out as 1. Seed with `NULL AS n` to make the key present without counting it.
- `PositiveComplementCorrelation`: a rule has a positive `IS` reference and an `IS NOT` to different rules on the same subject variable; if the two rules share base facts, the independence assumption behind the probabilistic complement does not hold. Raised on the pattern, whether or not facts are actually shared.
- `CrossPredicateCorrelation`: a rule has two or more positive `IS` references to different PROB rules on the same subject variable; the implied conjunction assumes they are independent. Raised on the pattern, whether or not facts are actually shared.
- `EceBinningBias`: `VALIDATE METRICS ece` was requested; equal-width-binned ECE is biased in the small-sample regime. Prefer `debiased_ece`.

## Recommended Profiles

### Development

- Lower iteration and timeout values.
- Keep deterministic tie-break enabled.

### Production

- Set explicit timeout and memory budgets.
- Monitor command result sizes.
- Restrict unconstrained `QUERY` and `ABDUCE` patterns.

## Escalation Playbook

1. Reduce goal scope.
2. Add stronger filters.
3. Split large programs into smaller ones.
4. Profile expensive rule strata.
