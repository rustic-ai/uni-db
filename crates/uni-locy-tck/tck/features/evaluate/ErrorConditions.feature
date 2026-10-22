Feature: Evaluation Error Conditions

  Tests runtime and compile-time error detection.

  Background:
    Given an empty graph

  # ── Compile errors ────────────────────────────────────────────────────

  Scenario: Undefined rule in IS reference
    When compiling the following Locy program:
      """
      CREATE RULE test AS MATCH (n) WHERE n IS nonexistent YIELD KEY n
      """
    Then the program should fail to compile
    And the compile error should mention 'undefined rule'

  Scenario: prev in base case clause rejected
    When compiling the following Locy program:
      """
      CREATE RULE bad AS
        MATCH (a)-[e:EDGE]->(b)
        ALONG cost = prev.cost + e.weight
        YIELD KEY a, KEY b, cost
      """
    Then the program should fail to compile
    And the compile error should mention 'prev'

  Scenario: YIELD schema mismatch rejected
    When compiling the following Locy program:
      """
      CREATE RULE mismatched AS MATCH (a)-[:EDGE]->(b) YIELD KEY a, KEY b
      CREATE RULE mismatched AS MATCH (x)-[:EDGE]->(y) YIELD KEY x, KEY y, x.name AS extra
      """
    Then the program should fail to compile
    And the compile error should mention 'schema mismatch'

  Scenario: Cyclic negation rejected
    When compiling the following Locy program:
      """
      CREATE RULE a AS MATCH (n) WHERE n IS NOT b YIELD KEY n
      CREATE RULE b AS MATCH (n) WHERE n IS NOT a YIELD KEY n
      """
    Then the program should fail to compile
    And the compile error should mention 'cyclic negation'

  # ── Runtime errors ────────────────────────────────────────────────────

  Scenario: Max iterations exceeded errors by default
    Given having executed:
      """
      CREATE (a:Node {name: 'A'})-[:EDGE]->(b:Node {name: 'B'}), (b)-[:EDGE]->(a)
      """
    When evaluating the following Locy program with max_iterations 1:
      """
      CREATE RULE reachable AS MATCH (a:Node)-[:EDGE]->(b:Node) YIELD KEY a, KEY b
      CREATE RULE reachable AS MATCH (a:Node)-[:EDGE]->(mid:Node) WHERE mid IS reachable TO b YIELD KEY a, KEY b
      """
    Then evaluation should fail
    And evaluation should be incomplete with reason 'iteration_limit'

  Scenario: Max iterations with allow_partial returns partial results
    Given having executed:
      """
      CREATE (a:Node {name: 'A'})-[:EDGE]->(b:Node {name: 'B'}), (b)-[:EDGE]->(a)
      """
    When evaluating the following Locy program with max_iterations 1 allowing partial:
      """
      CREATE RULE reachable AS MATCH (a:Node)-[:EDGE]->(b:Node) YIELD KEY a, KEY b
      CREATE RULE reachable AS MATCH (a:Node)-[:EDGE]->(mid:Node) WHERE mid IS reachable TO b YIELD KEY a, KEY b
      """
    Then evaluation should succeed with timed_out true
    And evaluation should be incomplete with reason 'iteration_limit'

  # A chain n0 -> ... -> n59 missing n29 -> n30: reachable needs about 30
  # iterations before the write and about 60 after it, so the re-derivation
  # the QUERY reads is cut by max_iterations 45 while the program's own run
  # is not. The cut is reported like the program's own.
  Scenario: A re-derivation cut by max iterations errors by default
    Given having executed:
      """
      UNWIND range(0, 59) AS i CREATE (:Node {name: 'n' + toString(i)})
      """
    And having executed:
      """
      MATCH (a:Node), (b:Node)
      WHERE toInteger(substring(b.name, 1)) = toInteger(substring(a.name, 1)) + 1
        AND a.name <> 'n29'
      CREATE (a)-[:EDGE]->(b)
      """
    When evaluating the following Locy program with max_iterations 45:
      """
      CREATE RULE reachable AS MATCH (a:Node)-[:EDGE]->(b:Node) YIELD KEY a, KEY b
      CREATE RULE reachable AS MATCH (a:Node)-[:EDGE]->(mid:Node) WHERE mid IS reachable TO b YIELD KEY a, KEY b
      MATCH (a:Node {name: 'n29'}), (b:Node {name: 'n30'}) CREATE (a)-[:EDGE]->(b)
      QUERY reachable WHERE a.name = 'n0' RETURN b.name AS b
      """
    Then evaluation should fail
    And evaluation should be incomplete with reason 'iteration_limit'
    And the incomplete evaluation should name rule 'reachable' as incomplete

  Scenario: A re-derivation cut by max iterations is reported with allow_partial
    Given having executed:
      """
      UNWIND range(0, 59) AS i CREATE (:Node {name: 'n' + toString(i)})
      """
    And having executed:
      """
      MATCH (a:Node), (b:Node)
      WHERE toInteger(substring(b.name, 1)) = toInteger(substring(a.name, 1)) + 1
        AND a.name <> 'n29'
      CREATE (a)-[:EDGE]->(b)
      """
    When evaluating the following Locy program with max_iterations 45 allowing partial:
      """
      CREATE RULE reachable AS MATCH (a:Node)-[:EDGE]->(b:Node) YIELD KEY a, KEY b
      CREATE RULE reachable AS MATCH (a:Node)-[:EDGE]->(mid:Node) WHERE mid IS reachable TO b YIELD KEY a, KEY b
      MATCH (a:Node {name: 'n29'}), (b:Node {name: 'n30'}) CREATE (a)-[:EDGE]->(b)
      QUERY reachable WHERE a.name = 'n0' RETURN b.name AS b
      """
    Then evaluation should be incomplete with reason 'iteration_limit'
    And the incomplete evaluation should name rule 'reachable' as incomplete
