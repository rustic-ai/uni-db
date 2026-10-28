# Locy Neural Predicates Reference

Agent-targeted reference for `CREATE MODEL` / `FEATURES` / `CALIBRATE` / `VALIDATE` / `NeuralProvenance` in Locy. For prose and use-case context see `website/docs/locy/advanced/neural-predicates.md`.

## 1. When to Use

Use neural predicates when:

- A learned scoring function needs to **participate in the same rule** that walks the graph (no separate orchestration layer).
- The scored output should **compose through MNOR/MPROD/ALONG/FOLD** with other rule output.
- You need **calibrated probabilities** plus an **explainable trace** of which classifier produced which derivation.

Don't use them when:

- Scoring is fully external and the rule only reads a precomputed property — plain Cypher + MNOR is enough.
- You just want vector retrieval — use `uni.search` / `similar_to` directly without `CREATE MODEL`.

## 2. CREATE MODEL Syntax

```text
CREATE MODEL model_name AS
  INPUT (binding [: Label])
  [FEATURES feature_expr (, feature_expr)*]
  [FEATURES (subject, column) FROM source_rule]
  OUTPUT output_type result_name
  USING xervo('provider_alias' [, embedder = 'embed_alias'])
  [CALIBRATION method]
  [VERSION 'string']
```

`output_type` ∈ `PROB | SCORE | LABEL | VECTOR`. `method` ∈ `platt_scaling | isotonic_regression | temperature_scaling | beta_calibration | dirichlet_calibration`.

**Important** — the **classifier-registry key is `model_name`** (the `CREATE MODEL <name>`), NOT the `USING xervo('alias')` string. The xervo alias is a provider hint surfaced to EXPLAIN; registry lookup happens by model name.

## 3. Invocation Syntax

```text
CREATE RULE risky AS
  MATCH (a:Asset)
  YIELD KEY a, model_name(a.score) AS risk
```

The invocation:

- can appear in **YIELD**, **ALONG**, or inside **MNOR(...)/MPROD(...)**.
- evaluates each argument **at the call site**; the value at index `i` lands in the per-row feature dict under the model's `INPUT` binding name at index `i`.

So `model_name(a.score)` produces feature dict `{"a": <value of a.score>}` per row.

## 4. Feature Sources

| Source | Syntax in FEATURES | Example |
|---|---|---|
| Property | bare property access | `s.country`, `e.weight` |
| Embedding similarity | `similar_to(left, right)` | `similar_to(s.profile, $watchlist)` |
| Semantic match (sugar) | `semantic_match(prop, 'text')` | `semantic_match(s.bio, 'sanctions')` |
| Path-context | `FEATURES (subject, col) FROM rule` | `FEATURES (s, path_risk) FROM upstream` |
| Graph-structural | one of 10 built-ins | see table below |

### Graph-Structural Functions

| Function | Returns |
|---|---|
| `degree_centrality(n)` | Float64 |
| `pagerank_score(n)` | Float64 |
| `closeness_centrality(n)` | Float64 |
| `betweenness_centrality(n)` | Float64 |
| `eigenvector_centrality(n)` | Float64 |
| `harmonic_centrality(n)` | Float64 |
| `katz_centrality(n)` | Float64 |
| `avg_neighbor(n, 'prop')` | Float64 (mean of `prop` over n's neighbors) |
| `max_neighbor(n, 'prop')` | Float64 |
| `sum_neighbor(n, 'prop')` | Float64 |

Computed against the live graph at invocation time.

## 5. Registering a Classifier (Python)

```python
import uni_db

def my_scorer(inputs: list[dict[str, Any]]) -> list[float]:
    # inputs[i] is the feature dict for row i, keyed by the INPUT binding name.
    return [...]

# Class form:
config = uni_db.LocyConfig()
config.register_classifier("model_name", my_scorer)

# Dict form (passed to with_config):
session.locy_with(program).with_config(
    {"classifier_registry": {"model_name": my_scorer}}
).run()
```

Callable contract:

- Receives `list[dict[str, Any]]`. Dict keys are the model's `INPUT` binding names.
- Returns `list[float]` of the **same length** as inputs, values in `[0, 1]`.
- Any exception, length mismatch, NaN, or out-of-range value raises a Locy runtime error at the first invocation.

## 6. Registering a Classifier (Rust)

```rust
use std::sync::Arc;
use uni_locy::{LocyConfig, MockClassifier, NeuralClassifier};

let mut config = LocyConfig::default();
let c: Arc<dyn NeuralClassifier> = Arc::new(
    MockClassifier::new("model_name", |inp| 0.7)
);
config.classifier_registry.insert("model_name".to_string(), c);
```

Or implement `NeuralClassifier` directly:

```rust
#[async_trait::async_trait]
impl NeuralClassifier for MyClassifier {
    fn name(&self) -> &str { "model_name" }
    async fn classify(&self, inputs: &[ClassifyInput]) -> ClassifierResult<Vec<f64>> {
        // ...
    }
}
```

## 7. CALIBRATE

```text
CALIBRATE model_name USING method
```

Returns `CalibrateCommandResult`:

| Field | Type | Notes |
|---|---|---|
| `model_name` | str | |
| `method` | str | `platt_scaling`, `isotonic_regression`, etc. |
| `n_samples` | int | Held-out samples used |
| `holdout_size` | float | Fraction reserved (0.0–1.0) |
| `raw_brier` | float | Pre-calibration Brier |
| `calibrated_brier` | float | Post-calibration Brier |
| `raw_ece` | float | Pre-calibration ECE |
| `calibrated_ece` | float | Post-calibration ECE |
| `confidence_band_quantile` | Optional[float] | If conformal calibration fitted |

## 8. VALIDATE

```text
VALIDATE model_name USING brier, ece
```

Returns `ValidateCommandResult`:

| Field | Type |
|---|---|
| `rule_name` | str |
| `prob_column` | str |
| `n_samples` | int |
| `metrics` | dict[str, float] |

Supported metrics: `brier`, `ece`, `accuracy`, `log_loss`, `auroc`.

## 9. EXPLAIN with NeuralProvenance

```text
EXPLAIN RULE rule_name [WHERE filter]
```

Derivation nodes that crossed a classifier carry a `NeuralProvenance` record:

| Field | Notes |
|---|---|
| `model_name` | |
| `model_version` | From `VERSION 'string'` if set |
| `xervo_alias` | From `USING xervo('alias')` |
| `raw_probability` | Pre-calibration |
| `calibrated_probability` | Post-calibration; `None` if no calibrator |
| `confidence_band` | Optional `ConfidenceBand` |
| `confidence_source` | `Frequentist | Conformal | Dirichlet | …` |
| `feature_inputs` | Reproducible feature dict |

The classifier is re-run on `feature_inputs` to verify the score — derivations are reproducible from the trace.

## 10. Semiring Choice

| Semiring | MNOR | MPROD | When to use |
|---|---|---|---|
| `AddMultProb` (default) | noisy-OR `1 − ∏(1 − pᵢ)` | product `∏ pᵢ` | Probabilities; independence assumed. |
| `MaxMinProb` (Viterbi) | `max(pᵢ)` | `min(pᵢ)` | Fuzzy truth, not probabilities. Emits `FuzzyNotProbabilistic` warning if rule also declares `PROB`. |
| `TopKProofs(k)` | DNF inclusion-exclusion over top-k proofs | as above | Shared base facts where independence is wrong. |

Set via `LocyConfig.semiring`.

## 11. Warnings

| Code | When |
|---|---|
| `FuzzyNotProbabilistic` | `MaxMinProb` + rule emits `PROB` |
| `SharedNeuralInput` | Two models in same rule see identical input |
| `SharedRetrievalContext` | Multiple `similar_to`/`semantic_match` features share a query embedding |
| `CrossGroupCorrelationNotExact` | MNOR/MPROD composing across shared base facts in different key_groups |
| `TopKPruningCrossedDependency` | `TopKProofs(k)` pruning lost a proof sharing a base fact with a kept proof |

## 12. Errors

| Error | When |
|---|---|
| `neural classifier '<name>' not registered; add it to LocyConfig::classifier_registry` | First invocation; the name was missing |
| `ArityMismatch { expected, actual }` | Classifier returned a wrong-length list |
| `DomainViolation { value }` | NaN, < 0.0, or > 1.0 |
| `Provider("...")` | Python exception (or other provider error). Includes formatted exception text |
| `NeuralPreviewDisabled` | `CREATE MODEL` in program with `neural_predicates_preview = false` |

## 13. LocyConfig Fields

| Field | Default | Notes |
|---|---|---|
| `classifier_registry` | `{}` | `{model_name: Arc<dyn NeuralClassifier>}` |
| `classifier_cache` | `None` | Shared `ModelInvocationCache` across queries |
| `classifier_cache_max` | `100_000` | Per-query cache size |
| `neural_provenance_store` | `None` | Where EXPLAIN finds `NeuralProvenance` records |
| `semiring` | `AddMultProb` | One of three above |
| `top_k_proofs` | `0` | For `TopKProofs(k)` |
| `neural_predicates_preview` | `true` | GA since Phase D |

## 14. Worked Examples

End-to-end notebooks under `website/docs/examples/python/`:

- `locy_predictive_maintenance.ipynb` — calibrated tabular classifier + topology features (AI4I 2020).
- `locy_adverse_drug_reaction.ipynb` — `similar_to` over narratives + audit-grade `NeuralProvenance` (Hetionet).
- `locy_drug_drug_interaction.ipynb` — heterogeneous-graph GNN embeddings + MLP head + ASSUME/ABDUCE (Hetionet).

## 15. TCK Coverage

| Feature file | Covers |
|---|---|
| `crates/uni-locy-tck/tck/features/neural/CreateModel.feature` | `CREATE MODEL` syntax + gating |
| `…/InvokeModel.feature` | Invocation in YIELD position, missing-classifier error |
| `…/AlongInvocation.feature` | Invocation in ALONG position |
| `…/NeuralFollowups.feature` | Property features, missing-property → Null |
| `…/RetrievalFeatures.feature` | `similar_to` / `semantic_match` in FEATURES |
| `…/PathContextFeatures.feature` | `FEATURES (subj, col) FROM rule` |
| `…/GraphStructuralFeatures.feature` | The 10 graph-structural functions |
| `…/SharedNeuralInput.feature` | `SharedNeuralInput` warning |
| `…/NoisyOrComposition.feature` | MNOR over neural-classified paths |
| `…/Calibrate.feature` | CALIBRATE command, calibration methods |
| `…/ConformalCalibration.feature` | Conformal predictor + `ConfidenceBand` |
| `…/Validate.feature` | VALIDATE command, all metrics |
| `…/NeuralProvenanceExplain.feature` | EXPLAIN with `NeuralProvenance` |
| `…/IntegratedExample.feature` | Property features + ALONG + MNOR + MPROD end-to-end |
| `…/CandleClassifier.feature` | Safetensors-backed CPU `CandleLinearClassifier` (historical name; no Candle dependency) |
