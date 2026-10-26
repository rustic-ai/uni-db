Feature: Shared-event diagnostics report what inference did

  Two suppliers depend on one facility event with probability 0.2, so the
  portfolio's two proofs share that event (issue #315). Independence
  evaluation combines them as unrelated, 1 - 0.8^2 = 0.36, and warns that
  the result may overestimate. Exact inference counts the event once, 0.2,
  and reports the dependency as resolved rather than as a possible
  overestimate. A group exact inference cannot evaluate within the variable
  limit keeps its independence value and still warns, saying why.

  Background:
    Given an empty graph
    And having executed:
      """
      CREATE (f:Facility {id: 'F1'}),
             (:Supplier {id: 'S1'})-[:DEPENDS_ON]->(f),
             (:Supplier {id: 'S2'})-[:DEPENDS_ON]->(f)
      """

  Scenario: Ordinary mode warns of an overestimate
    When evaluating the following Locy program:
      """
      CREATE RULE factory_event AS
        MATCH (f:Facility {id: 'F1'})
        YIELD KEY f, 0.2 AS probability PROB
      CREATE RULE exposed AS
        MATCH (s:Supplier)-[:DEPENDS_ON]->(f:Facility)
        WHERE f IS factory_event
        YIELD KEY s, probability AS risk PROB
      CREATE RULE portfolio AS
        MATCH (s:Supplier)-[:DEPENDS_ON]->(f:Facility {id: 'F1'})
        WHERE s IS exposed
        FOLD p = MNOR(risk)
        YIELD KEY f, p AS probability PROB
      """
    Then evaluation should succeed
    And the derived relation 'portfolio' should contain a fact where probability is approximately 0.36 within 0.000001
    And the result should contain a SharedProbabilisticDependency warning for rule 'portfolio'
    And the shared dependency of rule 'portfolio' should have inference mode 'approximate' and resolution 'not_attempted'
    And the result should not report a SharedDependencyResolved status

  Scenario: Exact mode reports the dependency as resolved
    When evaluating the following Locy program with exact_probability:
      """
      CREATE RULE factory_event AS
        MATCH (f:Facility {id: 'F1'})
        YIELD KEY f, 0.2 AS probability PROB
      CREATE RULE exposed AS
        MATCH (s:Supplier)-[:DEPENDS_ON]->(f:Facility)
        WHERE f IS factory_event
        YIELD KEY s, probability AS risk PROB
      CREATE RULE portfolio AS
        MATCH (s:Supplier)-[:DEPENDS_ON]->(f:Facility {id: 'F1'})
        WHERE s IS exposed
        FOLD p = MNOR(risk)
        YIELD KEY f, p AS probability PROB
      """
    Then evaluation should succeed
    And the derived relation 'portfolio' should contain a fact where probability is approximately 0.2 within 0.000001
    And the result should not contain a SharedProbabilisticDependency warning
    And the result should report a SharedDependencyResolved status for rule 'portfolio'
    And the shared dependency of rule 'portfolio' should have inference mode 'exact' and resolution 'completed'

  Scenario: Exact mode over the variable limit falls back and warns
    When evaluating the following Locy program with exact_probability and max_bdd_variables 1:
      """
      CREATE RULE factory_event AS
        MATCH (f:Facility {id: 'F1'})
        YIELD KEY f, 0.2 AS probability PROB
      CREATE RULE exposed AS
        MATCH (s:Supplier)-[:DEPENDS_ON]->(f:Facility)
        WHERE f IS factory_event
        YIELD KEY s, probability AS risk PROB
      CREATE RULE portfolio AS
        MATCH (s:Supplier)-[:DEPENDS_ON]->(f:Facility {id: 'F1'})
        WHERE s IS exposed
        FOLD p = MNOR(risk)
        YIELD KEY f, p AS probability PROB
      """
    Then evaluation should succeed
    And the derived relation 'portfolio' should contain a fact where probability is approximately 0.36 within 0.000001
    And the result should contain a BddLimitExceeded warning for rule 'portfolio'
    And the result should contain a SharedProbabilisticDependency warning for rule 'portfolio'
    And the shared dependency of rule 'portfolio' should have inference mode 'exact' and resolution 'fallback'
