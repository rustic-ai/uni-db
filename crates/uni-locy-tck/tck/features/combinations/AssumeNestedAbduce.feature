Feature: ABDUCE and EXPLAIN RULE inside an ASSUME body are rejected

  An ASSUME command returns the rows of the QUERY and Cypher commands in its
  THEN body. An ABDUCE or EXPLAIN RULE result is not rows, so a body that
  contained one used to evaluate it and silently drop the result. The grammar
  still accepts the program; the compiler rejects it, at any nesting depth,
  and names the alternatives (top level, tx() + rollback(), Session::scratch()).

  Background:
    Given an empty graph

  # ── Parse-level tests: the grammar is unchanged ──────────────────────────

  Scenario: ABDUCE NOT inside ASSUME THEN parses
    When parsing the following Locy program:
      """
      CREATE RULE reachable AS
        MATCH (a:Node)-[:EDGE]->(b:Node)
        YIELD KEY a, KEY b
      ASSUME { CREATE (:Node {name: 'C'})-[:EDGE]->(:Node {name: 'D'}) }
      THEN { ABDUCE NOT reachable WHERE a.name = 'C' }
      """
    Then the program should parse successfully

  # ── Compile-level tests ──────────────────────────────────────────────────

  Scenario: ABDUCE NOT inside ASSUME THEN fails to compile
    When compiling the following Locy program:
      """
      CREATE RULE reachable AS
        MATCH (a:Node)-[:EDGE]->(b:Node)
        YIELD KEY a, KEY b
      ASSUME { CREATE (:Node {name: 'C'})-[:EDGE]->(:Node {name: 'D'}) }
      THEN { ABDUCE NOT reachable WHERE a.name = 'C' }
      """
    Then the program should fail to compile
    And the compile error should mention 'ABDUCE is not allowed inside an ASSUME'

  Scenario: ABDUCE NOT with MNOR inside ASSUME THEN fails to compile
    When compiling the following Locy program:
      """
      CREATE RULE risk AS
        MATCH (a:Node)-[e:CAUSE {prob: 0.5}]->(b:Node)
        FOLD p = MNOR(e.prob)
        YIELD KEY b, p
      ASSUME { MATCH (:Node {name: 'Weak'})-[e:CAUSE]->() DELETE e }
      THEN { ABDUCE NOT risk WHERE b.name = 'B' }
      """
    Then the program should fail to compile
    And the compile error should mention 'ABDUCE is not allowed inside an ASSUME'

  Scenario: EXPLAIN RULE inside ASSUME THEN fails to compile
    When compiling the following Locy program:
      """
      CREATE RULE reachable AS
        MATCH (a:Node)-[:EDGE]->(b:Node)
        YIELD KEY a, KEY b
      ASSUME { CREATE (:Node {name: 'X'})-[:EDGE]->(:Node {name: 'Y'}) }
      THEN { EXPLAIN RULE reachable WHERE a.name = 'X' }
      """
    Then the program should fail to compile
    And the compile error should mention 'EXPLAIN RULE is not allowed inside an ASSUME'

  Scenario: ABDUCE inside a nested ASSUME body fails to compile
    When compiling the following Locy program:
      """
      CREATE RULE reachable AS
        MATCH (a:Node)-[:EDGE]->(b:Node)
        YIELD KEY a, KEY b
      ASSUME { CREATE (:Node {name: 'X'}) }
      THEN {
        QUERY reachable RETURN a
        ASSUME { CREATE (:Node {name: 'Y'}) }
        THEN { ABDUCE NOT reachable WHERE a.name = 'A' }
      }
      """
    Then the program should fail to compile
    And the compile error should mention 'ABDUCE is not allowed inside an ASSUME'

  # ── Evaluation tests ──────────────────────────────────────────────────────

  Scenario: ABDUCE NOT inside ASSUME THEN fails evaluation with the compile error
    Given having executed:
      """
      CREATE (:Node {name: 'A'})-[:EDGE]->(:Node {name: 'B'}),
             (:Node {name: 'A'})-[:EDGE]->(:Node {name: 'C'})
      """
    When evaluating the following Locy program:
      """
      CREATE RULE reachable AS
        MATCH (a:Node)-[:EDGE]->(b:Node)
        YIELD KEY a, KEY b
      ASSUME { CREATE (:Node {name: 'X'})-[:EDGE]->(:Node {name: 'Y'}) }
      THEN { ABDUCE NOT reachable WHERE a.name = 'A' }
      """
    Then evaluation should fail
    And the evaluation error should mention 'Session::scratch()'

  Scenario: QUERY inside ASSUME THEN with ABDUCE NOT at the top level
    Given having executed:
      """
      CREATE (b:Node {name: 'B'}),
             (:Node {name: 'Strong'})-[:CAUSE {prob: 0.6}]->(b),
             (:Node {name: 'Weak'})-[:CAUSE {prob: 0.4}]->(b)
      """
    When evaluating the following Locy program:
      """
      CREATE RULE risk AS
        MATCH (a:Node)-[e:CAUSE]->(b:Node)
        FOLD p = MNOR(e.prob)
        YIELD KEY b, p
      ASSUME { MATCH (:Node {name: 'Weak'})-[e:CAUSE]->() DELETE e }
      THEN { QUERY risk WHERE b.name = 'B' RETURN p }
      ABDUCE NOT risk WHERE b.name = 'B'
      """
    Then evaluation should succeed
    And the command result 0 should be an Assume containing row where p = 0.6
    And the command result 1 should be an Abduce with at least 1 modifications
