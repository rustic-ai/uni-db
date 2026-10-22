Feature: ABDUCE and EXPLAIN RULE inside an ASSUME body

  An ASSUME returns one result per command of its THEN body. An ABDUCE or
  EXPLAIN RULE there runs in the hypothetical state: EXPLAIN shows the
  derivations of the hypothetical facts, recorded while deriving them, and
  ABDUCE validates each candidate on top of the hypothetical state, against
  the body's own rules too. Nested bodies work the same way. The same
  re-evaluation records provenance whenever the program needs it, so a body
  QUERY under exact_probability reads the exact value a top-level QUERY reads.

  Background:
    Given an empty graph

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

  # reachable(B, X) exists only in the hypothetical state; its one candidate,
  # removing B -> X, breaks it.
  Scenario: ABDUCE NOT inside ASSUME THEN reads the hypothetical state
    Given having executed:
      """
      CREATE (:Node {name: 'A'})-[:EDGE]->(:Node {name: 'B'})
      """
    When evaluating the following Locy program:
      """
      CREATE RULE reachable AS
        MATCH (a:Node)-[:EDGE]->(b:Node)
        YIELD KEY a, KEY b
      ASSUME { MATCH (b:Node {name: 'B'}) CREATE (b)-[:EDGE]->(:Node {name: 'X'}) }
      THEN { ABDUCE NOT reachable WHERE a.name = 'B' }
      """
    Then evaluation should succeed
    And the result at path 0 should be an Assume with 1 results
    And the result at path 0.0 should be an Abduce with 1 modifications of which 1 validated

  Scenario: ABDUCE inside ASSUME THEN validates against the body's own rule
    Given having executed:
      """
      CREATE (:Node {name: 'A'})-[:EDGE]->(:Node {name: 'B'})
      """
    When evaluating the following Locy program:
      """
      CREATE RULE reachable AS
        MATCH (a:Node)-[:EDGE]->(b:Node)
        YIELD KEY a, KEY b
      ASSUME { MATCH (b:Node {name: 'B'}) CREATE (b)-[:EDGE]->(:Node {name: 'X'}) }
      THEN {
        CREATE RULE into_x AS
          MATCH (a:Node) WHERE a IS reachable TO b, b.name = 'X'
          YIELD KEY a
        QUERY into_x RETURN a.name AS name
        ABDUCE NOT into_x WHERE a.name = 'B'
      }
      """
    Then evaluation should succeed
    And the result at path 0.0 should be a Query with 1 rows
    And the result at path 0.0 should be a Query containing row where name = 'B'
    And the result at path 0.1 should be an Abduce with 1 modifications of which 1 validated

  Scenario: EXPLAIN RULE inside ASSUME THEN explains the hypothetical facts
    Given having executed:
      """
      CREATE (:Node {name: 'A'})-[:EDGE]->(:Node {name: 'B'})
      """
    When evaluating the following Locy program:
      """
      CREATE RULE reach AS
        MATCH (a:Node)-[:EDGE]->(b:Node)
        YIELD KEY a, KEY b
      CREATE RULE reach AS
        MATCH (a:Node)-[:EDGE]->(m:Node) WHERE m IS reach TO b
        YIELD KEY a, KEY b
      ASSUME { MATCH (b:Node {name: 'B'}) CREATE (b)-[:EDGE]->(:Node {name: 'X'}) }
      THEN { EXPLAIN RULE reach WHERE a.name = 'A' AND b.name = 'X' }
      """
    Then evaluation should succeed
    And the result at path 0.0 should be an Explain with rule 'reach' and 1 children
    And the result at path 0.0 should be an Explain whose tree mentions 'X'

  Scenario: ABDUCE inside a nested ASSUME body reads both mutations
    Given having executed:
      """
      CREATE (:Node {name: 'A'})-[:EDGE]->(:Node {name: 'B'})
      """
    When evaluating the following Locy program:
      """
      CREATE RULE reachable AS
        MATCH (a:Node)-[:EDGE]->(b:Node)
        YIELD KEY a, KEY b
      ASSUME { MATCH (b:Node {name: 'B'}) CREATE (b)-[:EDGE]->(:Node {name: 'X'}) }
      THEN {
        QUERY reachable RETURN a.name AS a
        ASSUME { MATCH (x:Node {name: 'X'}) CREATE (x)-[:EDGE]->(:Node {name: 'Y'}) }
        THEN { ABDUCE NOT reachable WHERE a.name = 'X' }
      }
      """
    Then evaluation should succeed
    And the result at path 0.0 should be a Query with 2 rows
    And the result at path 0.1.0 should be an Abduce with 1 modifications of which 1 validated

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

  # preach(A, D) over A -> B, A -> C, B -> C, B -> D, C -> D, every edge 0.5:
  # the probability D is reachable from A is 0.46875 (condition on C -> D:
  # 0.5 x 0.6875 + 0.5 x 0.25). The independence approximation is 0.484375.
  # A mutation no rule reads leaves both values unchanged in the body.
  Scenario: A QUERY inside ASSUME THEN reads the exact probability
    Given having executed:
      """
      CREATE (a:Node {name: 'A'}), (b:Node {name: 'B'}), (c:Node {name: 'C'}), (d:Node {name: 'D'}),
             (a)-[:CAUSE {prob: 0.5}]->(b), (a)-[:CAUSE {prob: 0.5}]->(c),
             (b)-[:CAUSE {prob: 0.5}]->(c), (b)-[:CAUSE {prob: 0.5}]->(d),
             (c)-[:CAUSE {prob: 0.5}]->(d)
      """
    When evaluating the following Locy program with exact_probability:
      """
      CREATE RULE preach AS
        MATCH (x:Node)-[e:CAUSE]->(y:Node)
        FOLD p = MNOR(e.prob)
        YIELD KEY x, KEY y, p
      CREATE RULE preach AS
        MATCH (x:Node)-[e:CAUSE]->(m:Node) WHERE m IS preach TO y
        FOLD p = MNOR(e.prob)
        YIELD KEY x, KEY y, p
      QUERY preach WHERE x.name = 'A' AND y.name = 'D' RETURN p
      ASSUME { CREATE (:Node {name: 'Isolated'}) }
      THEN { QUERY preach WHERE x.name = 'A' AND y.name = 'D' RETURN p }
      """
    Then evaluation should succeed
    And the result at path 0 should be a Query containing row where p = 0.46875
    And the result at path 1.0 should be a Query containing row where p = 0.46875

  Scenario: A QUERY inside ASSUME THEN reads the independence value without exact_probability
    Given having executed:
      """
      CREATE (a:Node {name: 'A'}), (b:Node {name: 'B'}), (c:Node {name: 'C'}), (d:Node {name: 'D'}),
             (a)-[:CAUSE {prob: 0.5}]->(b), (a)-[:CAUSE {prob: 0.5}]->(c),
             (b)-[:CAUSE {prob: 0.5}]->(c), (b)-[:CAUSE {prob: 0.5}]->(d),
             (c)-[:CAUSE {prob: 0.5}]->(d)
      """
    When evaluating the following Locy program:
      """
      CREATE RULE preach AS
        MATCH (x:Node)-[e:CAUSE]->(y:Node)
        FOLD p = MNOR(e.prob)
        YIELD KEY x, KEY y, p
      CREATE RULE preach AS
        MATCH (x:Node)-[e:CAUSE]->(m:Node) WHERE m IS preach TO y
        FOLD p = MNOR(e.prob)
        YIELD KEY x, KEY y, p
      QUERY preach WHERE x.name = 'A' AND y.name = 'D' RETURN p
      ASSUME { CREATE (:Node {name: 'Isolated'}) }
      THEN { QUERY preach WHERE x.name = 'A' AND y.name = 'D' RETURN p }
      """
    Then evaluation should succeed
    And the result at path 0 should be a Query containing row where p = 0.484375
    And the result at path 1.0 should be a Query containing row where p = 0.484375
