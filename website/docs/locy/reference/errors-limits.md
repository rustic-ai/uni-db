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
- `max_derived_bytes`: derived fact memory bound.
- `max_explain_depth`: derivation tree depth bound.
- `max_slg_depth`: deprecated, no effect (QUERY reads the fixpoint's facts); setting it warns, and it will be removed.
- `goal_directed`: `Off` (default) derives every rule in full; `On` answers a `QUERY` whose WHERE pins a node KEY column by deriving only what it reaches, and leaves rules only such goals needed out of `.derived`; `Shadow` does the rewrite but keeps `.derived` complete. From Python, pass the lowercase string `"off"` / `"on"` / `"shadow"`.
- `max_abduce_candidates` / `max_abduce_results`: abduction search bounds.
- `deterministic_best_by`: enforce deterministic tie-breaking in `BEST BY` clauses.
- `strict_probability_domain`: reject probability inputs outside `[0, 1]` instead of clamping.
- `probability_epsilon`: MPROD threshold for switching to log-space accumulation.
- `exact_probability`: enable BDD-based exact evaluation for shared-proof aggregate groups.
- `max_bdd_variables`: cap per-group BDD complexity before fallback.
- `top_k_proofs`: keep at most `k` recorded derivations per derived fact for `EXPLAIN RULE`, the heaviest first (0 keeps all). A derivation's weight, its `proof_probability`, is its own PROB value: its local weight times every probability it referenced.

## Warning Codes

Locy has two warning channels. **Runtime warnings** (`RuntimeWarningCode`) surface in `result.warnings` at evaluation time. **Compile-time warnings** (`WarningCode`) surface in `compile_warnings` when the program is compiled. Both are informational; the program still runs.

### Runtime warnings (`result.warnings`)

- `SharedProbabilisticDependency`: multiple proof paths inside one MNOR/MPROD group reuse shared evidence.
- `BddLimitExceeded`: exact mode was enabled, but the group exceeded `max_bdd_variables` and fell back to independence mode.
- `CrossGroupCorrelationNotExact`: shared evidence spans multiple aggregate groups; each group is exact internally, but correlation across groups is still approximate.
- `FuzzyNotProbabilistic`: `LocyConfig.semiring = MaxMinProb` is active and a rule emits `PROB`. Fuzzy-truth math is being applied to a column declared as a probability; either pick the right semiring or drop the `PROB` annotation.
- `TopKPruningCrossedDependency`: under `TopKProofs(k)`, pruning dropped a proof that shared a base fact with a kept proof. The kept set is an approximation; increase `k` for exactness.
- `ProbabilityReadNotFactor`: a clause's `PROB` expression reads the probability of a positive `IS` reference other than as one product factor (`1 - p`, `p * p`). The referenced probability is not multiplied in again, and exact mode treats each such row as one independent event. See [Probabilistic Logic](../advanced/probabilistic-logic.md#positive-is-references-to-a-prob-rule).

### Compile-time warnings (`compile_warnings`)

- `SharedNeuralInputArgument`: two or more model invocations in the same rule receive the same INPUT variable argument, so their outputs are correlated. Downstream `MNOR` under-estimates joint risk. Mark the models `@independent` to suppress.
- `SharedNeuralFeatureValue`: two or more model invocations in the same rule share an equivalent feature value expression — the same correlation concern even when binding variables differ. `@independent` suppresses.
- `SharedRetrievalContext`: multiple `similar_to`/`semantic_match` features in the same rule share the same query embedding. The features are not independent of each other; the rule's joint composition may be biased.
- `UncalibratedNeuralPredicate`: a rule invokes a PROB model declaring no `CALIBRATION` (or `CALIBRATION none`); the uncalibrated probability compounds miscalibration downstream.
- `UncalibratedLLMLogprobs`: an uncalibrated `CREATE MODEL` whose `xervo_alias` looks like an LLM provider — raw logprobs are not calibrated probabilities.
- `ProbabilityDomainViolation`: a probability input fell outside `[0, 1]` (clamped, or rejected under `strict_probability_domain`).
- `FoldInRecursivePath`: a clause has a recursive IS-ref and a FOLD aggregate but no ALONG. The rule rolls up per KEY, composing level by level; use `ALONG` if you meant to accumulate along each path.
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
3. Split large programs by module.
4. Profile expensive rule strata.
