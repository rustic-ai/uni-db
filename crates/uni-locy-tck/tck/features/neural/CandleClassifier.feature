Feature: Phase B A3 — safetensors linear classifier

  Validates that a real `CandleLinearClassifier` (single-layer
  logistic regression, weights loaded from safetensors at scenario
  setup) drives a Locy rule's PROB output end-to-end. The public
  historical name is retained; inference no longer requires Candle.

  Each scenario uses a deterministic fixture with weight=4.394 and
  bias=-2.197, chosen so that:
    * sigmoid(weight * 1.0 + bias) = sigmoid(2.197) ≈ 0.9
    * sigmoid(weight * 0.0 + bias) = sigmoid(-2.197) ≈ 0.1

  Background:
    Given an empty graph

  Scenario: Candle classifier returns sigmoid for high-score input
    Given having executed:
      """
      CREATE (a:Customer {name: 'top', score: 1.0})
      """
    And a Candle classifier "risk_scorer" over Float input binding "s"
    When evaluating the following Locy program with neural_predicates_preview:
      """
      CREATE MODEL risk_scorer AS
        INPUT (s)
        FEATURES s.score
        OUTPUT PROB risk
        USING xervo('classify/risk_scorer')
        CALIBRATION platt_scaling

      CREATE RULE risky AS
        MATCH (s:Customer)
        YIELD KEY s, risk_scorer(s.score) AS risk
      """
    Then evaluation should succeed
    And the derived relation 'risky' should have 1 facts
    And the derived relation 'risky' should contain a fact where risk is approximately 0.9 within 0.01

  Scenario: Candle classifier returns sigmoid for low-score input
    Given having executed:
      """
      CREATE (a:Customer {name: 'bottom', score: 0.0})
      """
    And a Candle classifier "risk_scorer" over Float input binding "s"
    When evaluating the following Locy program with neural_predicates_preview:
      """
      CREATE MODEL risk_scorer AS
        INPUT (s)
        FEATURES s.score
        OUTPUT PROB risk
        USING xervo('classify/risk_scorer')
        CALIBRATION platt_scaling

      CREATE RULE risky AS
        MATCH (s:Customer)
        YIELD KEY s, risk_scorer(s.score) AS risk
      """
    Then evaluation should succeed
    And the derived relation 'risky' should contain a fact where risk is approximately 0.1 within 0.01

  Scenario: Candle classifier with missing feature uses bias-only sigmoid
    # The customer has no `score` property — `extract_feature_value`
    # returns Null, the classifier encodes it as 0.0, and the output
    # is sigmoid(bias) ≈ 0.1. Documents the missing-feature contract.
    Given having executed:
      """
      CREATE (a:Customer {name: 'unscored'})
      """
    And a Candle classifier "risk_scorer" over Float input binding "s"
    When evaluating the following Locy program with neural_predicates_preview:
      """
      CREATE MODEL risk_scorer AS
        INPUT (s)
        FEATURES s.score
        OUTPUT PROB risk
        USING xervo('classify/risk_scorer')
        CALIBRATION platt_scaling

      CREATE RULE risky AS
        MATCH (s:Customer)
        YIELD KEY s, risk_scorer(s.score) AS risk
      """
    Then evaluation should succeed
    And the derived relation 'risky' should contain a fact where risk is approximately 0.1 within 0.01

  Scenario: Candle classifier scales over multiple rows in one batch
    # Mixed-population batch — exercises the [batch, n_features]
    # matmul path. Each row gets its own probability without
    # cross-contamination.
    Given having executed:
      """
      CREATE (:Customer {name: 'a', score: 1.0}),
             (:Customer {name: 'b', score: 0.0}),
             (:Customer {name: 'c', score: 1.0})
      """
    And a Candle classifier "risk_scorer" over Float input binding "s"
    When evaluating the following Locy program with neural_predicates_preview:
      """
      CREATE MODEL risk_scorer AS
        INPUT (s)
        FEATURES s.score
        OUTPUT PROB risk
        USING xervo('classify/risk_scorer')
        CALIBRATION platt_scaling

      CREATE RULE risky AS
        MATCH (s:Customer)
        YIELD KEY s, risk_scorer(s.score) AS risk
      """
    Then evaluation should succeed
    And the derived relation 'risky' should have 3 facts
