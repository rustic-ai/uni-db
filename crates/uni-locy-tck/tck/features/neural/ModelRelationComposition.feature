Feature: A classifier-backed relation composes like any other relation

  A rule whose YIELD scores rows with a model produces an ordinary relation:
  another rule reads it through IS exactly as it reads a constant-valued
  relation, and gets the same score (issue #314). The model's property
  features are classifier inputs, not columns of the relation, and its
  output is a Float64 probability.

  Background:
    Given an empty graph

  Scenario: A model-scored relation consumed through IS
    Given having executed:
      """
      CREATE (r:Requirement {id: 'R'})-[:USES]->(n:Supply {id: 'S', route_days: 8})
      """
    And a registered mock classifier "delivery_model" returning 0.7
    When evaluating the following Locy program with neural_predicates_preview:
      """
      CREATE MODEL delivery_model AS
        INPUT (n) FEATURES n.route_days OUTPUT PROB risk
        USING xervo('local/constant-v1') VERSION 'constant-v1'

      CREATE RULE predicted AS
        MATCH (n:Supply)
        YIELD KEY n, delivery_model(n.route_days) AS probability PROB

      CREATE RULE reviewed AS
        MATCH (r:Requirement)-[:USES]->(n:Supply)
        WHERE n IS predicted, probability >= 0.5
        YIELD KEY r, probability
      """
    Then evaluation should succeed
    And the derived relation 'reviewed' should have 1 facts
    And the derived relation 'reviewed' should contain a fact where probability is approximately 0.7 within 0.000001

  Scenario: Two rules consume one model-scored relation
    Given having executed:
      """
      CREATE (r:Requirement {id: 'R'})-[:USES]->(n:Supply {id: 'S', route_days: 8}),
             (c:Customer {id: 'C'})-[:ORDERS]->(n)
      """
    And a registered mock classifier "delivery_model" returning 0.7
    When evaluating the following Locy program with neural_predicates_preview:
      """
      CREATE MODEL delivery_model AS
        INPUT (n) FEATURES n.route_days OUTPUT PROB risk
        USING xervo('local/constant-v1') VERSION 'constant-v1'

      CREATE RULE predicted AS
        MATCH (n:Supply)
        YIELD KEY n, delivery_model(n.route_days) AS probability PROB

      CREATE RULE reviewed AS
        MATCH (r:Requirement)-[:USES]->(n:Supply)
        WHERE n IS predicted, probability >= 0.5
        YIELD KEY r, probability

      CREATE RULE committed AS
        MATCH (c:Customer)-[:ORDERS]->(n:Supply)
        WHERE n IS predicted
        YIELD KEY c, probability * 2.0 AS exposure
      """
    Then evaluation should succeed
    And the derived relation 'reviewed' should contain a fact where probability is approximately 0.7 within 0.000001
    And the derived relation 'committed' should contain a fact where exposure is approximately 1.4 within 0.000001
