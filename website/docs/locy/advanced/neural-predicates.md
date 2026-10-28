# Neural Predicates (CREATE MODEL / FEATURES / CALIBRATE / VALIDATE)

Locy can invoke a neural classifier inline as part of a rule. You declare the model with `CREATE MODEL`, hand it features pulled from the graph (properties, embedding similarities, prior-rule outputs, graph-structural functions), and Locy dispatches through your classifier batch-by-batch. The classifier's output composes with the rest of the rule's probabilistic, recursive, or path-carried logic. After running, you calibrate the score against held-out labels with `CALIBRATE`, validate the calibration with `VALIDATE`, and trace any derivation back through the classifier invocations with `EXPLAIN`.

This page is the reference for the full surface. For a one-screen capability tour, see [Neural Predicates (features)](../../features/neural-predicates.md). For end-to-end worked examples, see the [Predictive Maintenance](../../examples/python/locy_predictive_maintenance_overview.md), [Adverse Drug Reaction](../../examples/python/locy_adverse_drug_reaction_overview.md), and [Polypharmacy DDI](../../examples/python/locy_drug_drug_interaction_overview.md) notebooks.

## Why Neural Predicates

Most production scoring pipelines split into two layers: a model that scores rows, and a separate orchestration layer that composes scores with business logic. The orchestration layer ends up reimplementing graph traversal, calibration math, joint-probability composition, and audit logging — every team builds it again. Neural predicates put that orchestration inside the same declarative rule that runs the classifier. You write one Locy program; the rule walks the graph, calls the model with graph-aware features, calibrates the output, composes it through `MNOR`/`MPROD`/`ALONG`, and emits a derivation tree that EXPLAIN can show end-to-end.

## CREATE MODEL Syntax

```text
CREATE MODEL model_name AS
  INPUT (binding [: Label])
  [FEATURES feature_expr (, feature_expr)*]
  [FEATURES (subject, column) FROM source_rule]
  OUTPUT output_type result_name
  USING xervo('provider_alias' [, embedder = 'embed_alias'])
  [CALIBRATION calibration_method]
  [VERSION 'version_string']
```

- **`INPUT (binding [: Label])`** — declares the variable name the rest of the rule will use to invoke the model. Optional label hint constrains where invocations are legal. The classifier's input dict is keyed by this binding name.
- **`FEATURES feature_expr (, feature_expr)*`** — zero or more feature expressions (see "Feature Sources" below). One model can also use **`FEATURES (subject, column) FROM source_rule`** to pull a path-context feature from a prior rule's derivation.
- **`OUTPUT output_type result_name`** — `output_type` is one of `PROB` (probability in `[0, 1]`), `SCORE` (real-valued), `LABEL` (categorical), `VECTOR` (embedding). `result_name` is the column name the rule's `YIELD` will see.
- **`USING xervo('provider_alias' [, embedder = 'embed_alias'])`** — provider hint. The string is informational at runtime — the classifier-registry lookup happens by the `CREATE MODEL` name, not the alias. The optional `embedder=` selects which Xervo embedder embeds `semantic_match` query literals; defaults to `default`.
- **`CALIBRATION method`** — the calibrator the classifier is wrapped in at load time. Optional; without it, the classifier ships raw probabilities and you can fit a calibrator later via the `CALIBRATE` command.
- **`VERSION 'string'`** — informational; surfaces in `NeuralProvenance` for audit.

The model name is the registry key. The `xervo('alias')` string is a provider hint surfaced to telemetry and EXPLAIN; the runtime registry lookup goes by model name.

## Invoking a Model in a Rule

A `CREATE MODEL` only registers a callable name. Rules invoke it by name with the same arity as `INPUT`:

```text
CREATE RULE at_risk AS
  MATCH (a:Asset)
  YIELD KEY a, failure_likelihood(a.score) AS risk
```

The invocation can appear:

- In `YIELD` position — produces an output column.
- In `ALONG` position — produces a path-carried scalar.
- Inside `MNOR(...)` or `MPROD(...)` — feeds the classifier output through a probabilistic aggregate.

At invocation time the runtime:

1. Builds one `ClassifyInput` per matching row, where the input dict is keyed by the model's `INPUT` binding name and the value is the evaluated argument expression at the call site (so `failure_likelihood(a.score)` produces `{"a": <a.score value>}`).
2. Batches all input rows and calls `classifier.classify(&batch).await` once.
3. Wires the returned probability into the rule's `YIELD` (or `ALONG`, or `FOLD`).
4. Records a `NeuralProvenance` entry per row keyed by `(model_name, input_hash)` for `EXPLAIN` to look up later.

## Feature Sources

`FEATURES` accepts any Cypher expression; the categories below summarise what's useful and how each kind reaches the classifier.

### Property features

Read a property off a graph entity. The compiler materialises the property through the standard property-access path, and the value lands in the per-row feature dict.

```text
CREATE MODEL supplier_risk AS
  INPUT (s)
  FEATURES s.country, s.revenue
  OUTPUT PROB risk
  USING xervo('classify/supplier-risk')
```

Invocation site picks which property is actually passed: `supplier_risk(s.country, s.revenue)` passes both; `supplier_risk(s.country)` passes one. The model's `FEATURES` declares the shape the model expects; the rule's invocation is what actually populates the feature dict.

### Embedding similarity — `similar_to` and `semantic_match`

`similar_to(left, right)` evaluates to a cosine-similarity score in `[-1, 1]`. Either side can be a property holding a vector, a literal vector, or a parameter.

```text
CREATE MODEL anomalous AS
  INPUT (s)
  FEATURES similar_to(s.profile_embedding, $watchlist_centroid)
  OUTPUT PROB risk
  USING xervo('classify/anomaly')
```

`semantic_match(prop, 'text')` is sugar over `similar_to` for the common case of embedding a literal query string. The literal is embedded once at compile time via the Xervo embedder named in `USING xervo(..., embedder='alias')` (default: `default`). The resulting score is what the classifier sees.

### Path-context features — `FEATURES (subject, column) FROM source_rule`

Pulls a column from a prior rule's derivation, keyed by `subject`. Lets a downstream model use an upstream rule's per-entity output as a feature without round-tripping through a property.

```text
CREATE RULE supply_path AS
  MATCH (s:Supplier)
  YIELD KEY s, 0.42 AS path_risk

CREATE MODEL risk_model AS
  INPUT (s)
  FEATURES (s, path_risk) FROM supply_path
  OUTPUT PROB risk
  USING xervo('classify/risk_model')

CREATE RULE risky AS
  MATCH (s:Supplier)
  YIELD KEY s, risk_model(s) AS risk
```

The compiler ensures the model's stratum follows `source_rule` so the source facts are fully materialised before invocation; the runtime joins each candidate row against the source rule's derived facts via a pre-built `subject → value` lookup.

### Graph-structural features

Ten built-in functions wrap the underlying graph algorithm library so it can be used inside `FEATURES`:

| Function | Returns |
|---|---|
| `degree_centrality(n)` | Float64 |
| `pagerank_score(n)` | Float64 |
| `closeness_centrality(n)` | Float64 |
| `betweenness_centrality(n)` | Float64 |
| `eigenvector_centrality(n)` | Float64 |
| `harmonic_centrality(n)` | Float64 |
| `katz_centrality(n)` | Float64 |
| `avg_neighbor(n, 'prop')` | Float64 — mean of `prop` over `n`'s neighbors |
| `max_neighbor(n, 'prop')` | Float64 — max of `prop` over `n`'s neighbors |
| `sum_neighbor(n, 'prop')` | Float64 — sum of `prop` over `n`'s neighbors |

These let a classifier consume topology without a separate feature pipeline. They are computed against the live graph at invocation time, so the model picks up structural updates immediately.

## Registering a Classifier

A `CREATE MODEL` declaration registers the **name** with the compiler, but the runtime needs an implementation to dispatch to. You register a classifier under the same name on `LocyConfig` before running the program.

=== "Python"
    ```python
    import uni_db

    def my_scorer(inputs: list[dict[str, Any]]) -> list[float]:
        return [0.7 for _ in inputs]

    config = uni_db.LocyConfig()
    config.register_classifier("failure_likelihood", my_scorer)
    # or, dict form via with_config:
    # session.locy_with(...).with_config({"classifier_registry": {"failure_likelihood": my_scorer}}).run()

    result = session.locy_with(program).with_config(config).run()
    ```

=== "Rust"
    ```rust
    use std::sync::Arc;
    use uni_locy::{LocyConfig, MockClassifier};

    let mut config = LocyConfig::default();
    let classifier = MockClassifier::constant("failure_likelihood", 0.7);
    config.classifier_registry.insert("failure_likelihood".to_string(), Arc::new(classifier));

    let result = session.locy_with(program).with_config(config).run().await?;
    ```

The Python callable must accept `list[dict[str, Any]]` and return `list[float]` of the same length, with values in `[0, 1]`. Out-of-range, NaN, length-mismatched, or exception-raising callables surface as a runtime error at the first invocation (see "Errors" below). The Rust trait is `uni_locy::NeuralClassifier`; any type implementing it works.

Without a registered classifier, the program fails at the first invocation with `neural classifier '<name>' not registered; add it to LocyConfig::classifier_registry`.

### Built-in Rust linear classifier

`uni_locy::CandleLinearClassifier::load(name, feature_order, path)` loads a
CPU logistic-regression model from a safetensors file. The file contains F32
`weight` values in feature order and one F32 `bias` value. The historical class
name remains available for compatibility; this classifier no longer requires
Candle or a model-provider feature. It retains f32 inference and returns f64
probabilities through `NeuralClassifier`. Optional embedding providers remain
controlled by their existing features.

## CALIBRATE

Calibration rescales a classifier's raw outputs so they line up with empirical frequencies. Useful when raw scores are over-confident in the tails (typical of boosted trees and many softmax heads).

```text
CALIBRATE failure_likelihood
  ON MATCH (a:Asset)
  TARGET a.actually_failed
  METHOD platt_scaling
  HOLDOUT 0.2
```

`CALIBRATE <model> ON MATCH <pattern> [WHERE ...] TARGET <expr> METHOD <method> [HOLDOUT n]` — the MATCH pattern collects the rows the model is invoked over, `TARGET` is the ground-truth label expression, `METHOD` picks the calibrator, and the optional `HOLDOUT` reserves a fraction for fitting.

Built-in methods:

| Method | Parameters | When to use |
|---|---|---|
| `platt_scaling` | 2 (slope, intercept) | Binary classifiers; robust default for most tabular and neural binary heads. |
| `isotonic_regression` | Non-parametric | Non-monotonic miscalibration. Requires more held-out data than Platt. |
| `temperature_scaling` | 1 (temperature) | Multi-class softmax models trained with cross-entropy. |
| `beta_calibration` | 3 | Binary heads when Platt produces a misshapen calibration curve. |
| `dirichlet` | Multi-class | True multi-class outputs (e.g. severity-tier predictions). |
| `conformal` / `conformal(alpha)` | Split-conformal | Distribution-free confidence bands; bare `conformal` defaults to `alpha = 0.1`. |

The `CALIBRATE` command runs against the same data the rule already binds (the rule's own derived facts plus the `params` you pass), then fits the calibrator and returns a `CalibrateCommandResult`:

| Field | Meaning |
|---|---|
| `model_name` | The `CREATE MODEL` name |
| `method` | The calibration method used |
| `n_samples` | Held-out sample count fitted |
| `holdout_size` | Reserved fraction (0.0–1.0) used for fitting |
| `raw_brier` | Brier score on raw outputs |
| `calibrated_brier` | Brier score after calibration |
| `raw_ece` | Expected Calibration Error on raw outputs |
| `calibrated_ece` | Expected Calibration Error after calibration |
| `confidence_band_quantile` | If a conformal predictor was fit alongside, the quantile |

Calibrated probabilities are what the rule emits on subsequent invocations. The raw output remains available in `NeuralProvenance` for audit.

## VALIDATE

`VALIDATE` scores a rule's predictions against ground-truth labels without modifying the classifier:

```text
VALIDATE failure_likelihood
  ON MATCH (a:Asset)
  TARGET a.actually_failed
  METRICS brier_score, ece
```

`VALIDATE <model> ON MATCH <pattern> [WHERE ...] TARGET <expr> METRICS <metric_list>` — the rule's `PROB` output is joined against the `TARGET` ground-truth expression over the MATCH rows, and the requested metrics are computed on the resulting `(prediction, label)` pairs.

Returns a `ValidateCommandResult`:

| Field | Meaning |
|---|---|
| `rule_name` | The rule whose `PROB` column is being validated |
| `prob_column` | The column name |
| `n_samples` | Number of label-prediction pairs scored |
| `metrics` | Dict mapping metric name to value |

Supported metrics: `brier_score`, `log_loss`, `ece`, `debiased_ece`, `accuracy`, `auc`. Multi-metric validation in one call.

ECE is more informative than `auc` for safety-critical applications: `auc` measures ranking quality only; ECE measures whether the probabilities themselves are honest. Prefer `debiased_ece` in the small-sample regime — equal-width-binned `ece` is biased there.

## EXPLAIN with NeuralProvenance

`EXPLAIN RULE rule_name [WHERE filter]` returns a derivation tree where every node carries the rule + clause + bound variables that produced it. Nodes that crossed a classifier additionally carry a `NeuralProvenance` record:

| Field | Meaning |
|---|---|
| `model_name` | Which model produced this score |
| `model_version` | The `VERSION 'string'` from `CREATE MODEL`, if set |
| `xervo_alias` | The `USING xervo('alias')` provider hint |
| `raw_probability` | Pre-calibration classifier output |
| `calibrated_probability` | Post-calibration probability, or `None` if no calibrator is active |
| `confidence_band` | A `ConfidenceBand` if conformal/Dirichlet calibration is active |
| `confidence_source` | `ConfidenceSource::Frequentist`, `::Conformal`, `::Dirichlet`, … |
| `feature_inputs` | The feature dict the classifier was called with (for reproduction) |

EXPLAIN re-runs the classifier on the recorded feature dict to produce the same probability — the derivation is reproducible from the trace alone.

## Semiring Choice

The semiring controls how probabilities compose through `MNOR`, `MPROD`, and shared-proof scenarios:

- **`AddMultProb` (default)** — independence-mode noisy-OR for `MNOR` and product for `MPROD`. Byte-identical to the pre-neural Locy. Use unless you have a reason to opt out.
- **`MaxMinProb` (Viterbi / fuzzy-truth)** — `MNOR` becomes max, `MPROD` becomes min. Useful when probabilities aren't actually probabilities (e.g. fuzzy-set memberships) and you want monotone composition without the independence assumption. Emits a `FuzzyNotProbabilistic` warning on any rule that also declares `PROB`, since you opted out of probability semantics.
- **`TopKProofs(k)`** — evaluates each `MNOR` fact over its `k` most probable rows (proofs), exactly: each proof is its own event AND the facts it references, so proofs sharing an edge count it once. With `k` at least a fact's number of rows the value is the possible-worlds probability. Pick this when shared proofs are a real concern and you want to bound the work per fact.

Set via `LocyConfig.semiring` (Rust) or by string name in the Python `with_config({"semiring": "TopKProofs(8)"})`.

## Warnings

Locy distinguishes two warning channels. **Compile-time warnings** (`WarningCode`) are raised when the program is compiled and surface in `compile_warnings`. **Runtime warnings** (`RuntimeWarningCode`) surface at evaluation time in `result.warnings`. Both are informational; the program continues regardless.

### Runtime warnings (`result.warnings`)

| Code | When it fires | What it means |
|---|---|---|
| `FuzzyNotProbabilistic` | `MaxMinProb` semiring is active and a rule emits `PROB` | You're using fuzzy-truth math on a column declared as a probability. Pick one. (Unsuppressible.) |
| `SharedProbabilisticDependency` | Two or more proof paths inside one `MNOR`/`MPROD` group reuse shared evidence | The independence assumption is violated; the aggregate over-/under-states joint probability. |
| `BddLimitExceeded` | Exact mode was on but the group exceeded `max_bdd_variables` | The BDD fell back to the independence-mode result. |
| `CrossGroupCorrelationNotExact` | `MNOR`/`MPROD` composes rule outputs sharing base facts across KEY groups | Each group is exact internally, but cross-group correlation is still approximate, under `TopKProofs(k)` too. |
| `TopKPruningCrossedDependency` | `TopKProofs(k)` pruning dropped a proof that shared a base fact with a kept proof | The kept set is an approximation; bump `k` for exactness. |

### Compile-time warnings (`compile_warnings`)

| Code | When it fires | What it means |
|---|---|---|
| `SharedNeuralInputArgument` | Two or more model invocations in the same rule share the same INPUT variable argument | Their outputs may be correlated; downstream `MNOR` will under-estimate joint risk. Marking the models `@independent` suppresses. |
| `SharedNeuralFeatureValue` | Two or more model invocations in the same rule share an equivalent feature value expression | Same correlation concern even when binding variables differ. `@independent` suppresses. |
| `SharedRetrievalContext` | Multiple `similar_to`/`semantic_match` features in the same rule share the same query embedding | The features are not independent of each other; the rule's joint composition over them may be biased. `@independent` suppresses. |
| `UncalibratedNeuralPredicate` | A rule invokes a PROB model that declares no `CALIBRATION` (or `CALIBRATION none`) | The uncalibrated probability flows into the probabilistic stack, compounding miscalibration. Run a `CALIBRATE` statement or acknowledge with `CALIBRATION none`. |
| `UncalibratedLLMLogprobs` | An uncalibrated `CREATE MODEL` whose `xervo_alias` looks like an LLM provider | Raw LLM logprobs are not calibrated probabilities. |
| `ProbabilityDomainViolation` | A probability input falls outside `[0, 1]` | The value was clamped (or rejected under `strict_probability_domain`). |
| `FoldInRecursivePath` | A clause has a recursive IS-ref and a FOLD aggregate but no ALONG | The rule rolls up per KEY, composing level by level. Use `ALONG` if you meant per-path accumulation. |
| `EceBinningBias` | `VALIDATE METRICS ece` was requested | Equal-width-binning ECE is biased in the small-sample regime; prefer `debiased_ece`. |

## Errors

| Error | When it fires |
|---|---|
| `neural classifier 'X' not registered; add it to LocyConfig::classifier_registry` | The rule invoked a model that has no registered classifier. The lookup key is the `CREATE MODEL` name. |
| `ArityMismatch { expected, actual }` | The classifier returned a different number of probabilities than inputs in the batch. |
| `DomainViolation { value: v }` | A returned probability was NaN, negative, or greater than 1.0. |
| `Provider(...)` | The classifier callable raised an exception. The wrapped Python exception text is included. |
| `NeuralPreviewDisabled` | `CREATE MODEL` appeared in a program but `LocyConfig.neural_predicates_preview = false`. Defaults to `true` since GA; setting `false` re-imposes the original compile-time rejection. |

## Configuration Summary

Fields on `LocyConfig` that govern neural predicates:

| Field | Default | Effect |
|---|---|---|
| `classifier_registry` | `{}` | Map from `CREATE MODEL` name to `Arc<dyn NeuralClassifier>`. Populate before running. |
| `classifier_cache` | `None` | Shared memoization cache across queries. `None` builds a fresh cache per query. |
| `classifier_cache_max` | `100_000` | Max entries in the per-query cache before eviction. |
| `neural_provenance_store` | `None` | Where `NeuralProvenance` records land for EXPLAIN. `None` falls back to a yield-alias lookup. |
| `semiring` | `AddMultProb` | Active semiring; see "Semiring Choice" above. |
| `top_k_proofs` | `0` | When `semiring = TopKProofs`, the K. |
| `neural_predicates_preview` | `true` | Compile-time toggle for `CREATE MODEL` acceptance. |

## Related

- [Neural Predicates (capability tour)](../../features/neural-predicates.md) — one-screen overview.
- [Probabilistic Logic](probabilistic-logic.md) — MNOR / MPROD / PROB / shared-proof detection, the prob-only baseline neural predicates compose on top of.
- [Graph Algorithms](../../features/graph-algorithms.md) — the algorithms exposed via `degree_centrality`, `pagerank_score`, and the rest of the graph-structural `FEATURES` functions.
- [Predictive Maintenance notebook](../../examples/python/locy_predictive_maintenance_overview.md), [ADR notebook](../../examples/python/locy_adverse_drug_reaction_overview.md), [DDI notebook](../../examples/python/locy_drug_drug_interaction_overview.md) — three end-to-end worked examples.
