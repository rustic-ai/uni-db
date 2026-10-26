# Neural Predicates

Locy can invoke neural classifiers inline as part of a rule body. Declare a model with `CREATE MODEL`, hand it features pulled from the graph (properties, embedding similarities, prior-rule outputs, graph-structural functions), and Locy will dispatch through your classifier batch-by-batch — composing the output with the rest of the rule's probabilistic, recursive, or path-carried logic.

## What It Provides

- **`CREATE MODEL`** statement that registers a model alias inside a Locy program. Models declare their `INPUT` binding, their `FEATURES` expressions, their `OUTPUT` type (e.g. `PROB risk`), and the provider hint (`USING xervo('alias')`).
- **Four feature sources** that can be mixed inside one `CREATE MODEL`:
    - **Property features** — `s.country`, `e.weight`, any Cypher property access.
    - **Embedding similarity** — `similar_to(s.profile, $query)` and `semantic_match(s, $query)`.
    - **Path-context features** — `FEATURES (subject, column) FROM source_rule` pulls a column from a prior rule's derivation, keyed by `subject`.
    - **Graph-structural features** — `degree_centrality(n)`, `pagerank_score(n)`, `closeness_centrality(n)`, `betweenness_centrality(n)`, `eigenvector_centrality(n)`, `harmonic_centrality(n)`, `katz_centrality(n)`, `avg_neighbor(n, 'prop')`, `max_neighbor(n, 'prop')`, `sum_neighbor(n, 'prop')`.
- **`CALIBRATE rule USING method`** — fit a calibrator (Platt scaling, isotonic regression, temperature scaling, beta, Dirichlet) against held-out labels. Returns `CalibrationResult` with `raw_brier` / `raw_ece` / `calibrated_brier` / `calibrated_ece` so you can show the calibration delta.
- **`VALIDATE rule USING brier, ece`** — score a rule's predictions against ground-truth labels. Returns `ValidationResult` with per-metric values.
- **`EXPLAIN` with neural provenance** — every derivation that crossed a classifier carries a `NeuralProvenance` record with `model_name`, `raw_probability`, `calibrated_probability`, `confidence_band`, and `ConfidenceSource` so an auditor can reproduce the score.
- **Three semirings** — `AddMultProb` (the default; noisy-OR / product), `MaxMinProb` (Viterbi-style fuzzy truth), `TopKProofs(k)` (each fact's `k` most probable proofs, evaluated exactly). Pick per-call via `LocyConfig`.

## Minimal Example

A model that scores assets for failure likelihood, invoked inside a rule:

=== "Python"
    ```python
    import uni_db

    db = uni_db.Uni.temporary()
    (db.schema()
        .label("Asset").property("score", "float")
        .apply())

    session = db.session()
    tx = session.tx()
    tx.execute("CREATE (:Asset {score: 0.1})")
    tx.execute("CREATE (:Asset {score: 0.7})")
    tx.commit()

    def failure_likelihood(rows):
        # rows is a list[dict[str, Any]] keyed by the INPUT binding name.
        return [min(1.0, max(0.0, r["a"])) for r in rows]

    config = uni_db.LocyConfig()
    config.register_classifier("failure_likelihood", failure_likelihood)

    program = """
    CREATE MODEL failure_likelihood AS
      INPUT (a)
      FEATURES a.score
      OUTPUT PROB risk
      USING xervo('classify/failure-likelihood-v1')

    CREATE RULE at_risk AS
      MATCH (a:Asset)
      YIELD KEY a, failure_likelihood(a.score) AS risk
    """

    result = session.locy_with(program).with_config(config).run()
    for row in result.derived.get("at_risk", []):
        print(row)
    ```

=== "Rust"
    ```rust
    use std::sync::Arc;
    use uni_db::Uni;
    use uni_locy::{LocyConfig, MockClassifier};

    # async fn demo() -> anyhow::Result<()> {
    let db = Uni::temporary().build().await?;
    db.schema()
        .label("Asset").property("score", uni_db::DataType::Float64)
        .apply().await?;
    let session = db.session();
    let tx = session.tx().await?;
    tx.execute("CREATE (:Asset {score: 0.1})").await?;
    tx.execute("CREATE (:Asset {score: 0.7})").await?;
    tx.commit().await?;

    // Any NeuralClassifier impl works — MockClassifier here for brevity.
    let classifier = MockClassifier::new("failure_likelihood", |inp| {
        match inp.features.get("a") {
            Some(uni_locy::FeatureValue::Float(v)) => v.clamp(0.0, 1.0),
            _ => 0.0,
        }
    });

    let mut config = LocyConfig::default();
    config.classifier_registry.insert(
        "failure_likelihood".to_string(),
        Arc::new(classifier),
    );

    let program = r#"
    CREATE MODEL failure_likelihood AS
      INPUT (a)
      FEATURES a.score
      OUTPUT PROB risk
      USING xervo('classify/failure-likelihood-v1')

    CREATE RULE at_risk AS
      MATCH (a:Asset)
      YIELD KEY a, failure_likelihood(a.score) AS risk
    "#;
    let result = session.locy_with(program).with_config(config).run().await?;
    println!("{:?}", result.derived_rule("at_risk").await?);
    # Ok(())
    # }
    ```

Two things worth knowing up front:

1. **The classifier-registry key is the `CREATE MODEL <name>`** (here, `failure_likelihood`), not the `USING xervo('classify/foo')` provider hint. The xervo alias is informational; lookup happens by model name.
2. **The feature dict your callable receives is keyed by the INPUT binding** (here, `a`); the value is the evaluated argument expression at the call site (here, `a.score`).

## Calibrate, Validate, Explain

After registering and running a model, calibrate it against held-out labels, validate the calibration, and inspect the audit trail.

```text
CALIBRATE failure_likelihood USING platt_scaling
VALIDATE  failure_likelihood USING brier, ece
EXPLAIN RULE at_risk
```

`CalibrationResult` returns both raw and calibrated metrics so the improvement is concrete. `EXPLAIN` traces every derivation back through the classifier invocations, surfacing raw + calibrated probabilities and a confidence band per call — the artifact regulators ask for when a learned score drove a decision.

## Worked Examples

Three end-to-end notebooks show the same machinery driving different problem shapes:

- **[Predictive Maintenance](../examples/python/locy_predictive_maintenance_overview.md)** — a calibrated tabular classifier scoring industrial equipment, composed through `MNOR` per asset and `MPROD` across line dependencies. Smallest dataset (AI4I 2020), broadest audience.
- **[Adverse Drug Reaction Signal Detection](../examples/python/locy_adverse_drug_reaction_overview.md)** — `similar_to` over reported narrative text plus graph-structural features against Hetionet's biomedical graph, with audit-grade `NeuralProvenance` traces.
- **[Polypharmacy Drug-Drug Interaction Risk](../examples/python/locy_drug_drug_interaction_overview.md)** — heterogeneous graph + offline GNN-derived drug embeddings + an MLP head at query time, composed across pairwise predictions into joint regimen safety, with `ASSUME` for substitution and `ABDUCE` for minimum-change recommendations.

## Use Cases

- Calibrated risk scoring on assets, accounts, or entities where the score needs to compose with other graph-derived signals before driving a decision.
- Pharmacovigilance, fraud detection, content moderation, predictive maintenance — anywhere "score a row" and "reason over a graph" both matter.
- Clinical decision support and other regulated domains where the audit trail behind a probability must be reproducible.
- RAG-style pipelines that need a calibrated relevance head over graph-structured context.

## When To Use

Use neural predicates when you want a learned scoring function to participate in the same declarative rule that walks the graph, applies probabilistic semantics, and produces an explainable derivation. If your scoring is fully external and the rule just consumes a precomputed property, plain Cypher + `MNOR`/`MPROD` is enough; if the model is meant to score *as part of the rule body*, with full provenance through `EXPLAIN`, this is the path.

For the full reference — every `FEATURES` source, every calibration method, every warning code, and the choice of semiring — see [Locy: Neural Predicates](../locy/advanced/neural-predicates.md).
