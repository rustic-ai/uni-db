Feature: Top-K Proof Filtering (Scallop, Huang et al. 2021)

  Tests that top_k_proofs bounds the number of proof annotations retained
  per derived fact, keeping only the highest-probability proof paths.

  Background:
    Given an empty graph

  Scenario: Default unlimited retains all proofs — backward compatible
    Given having executed:
      """
      CREATE (a:Node {name: 'A'})-[:LINK {p: 0.9}]->(b:Node {name: 'B'}),
             (a)-[:LINK {p: 0.5}]->(c:Node {name: 'C'}),
             (c)-[:LINK {p: 0.8}]->(b)
      """
    When evaluating the following Locy program with exact_probability:
      """
      CREATE RULE reach AS
        MATCH (x:Node)-[e:LINK]->(y:Node)
        FOLD prob = MNOR(e.p)
        YIELD KEY x, KEY y, prob
      """
    Then evaluation should succeed
    And the derived relation 'reach' should have 3 facts

  Scenario: top_k=1 retains only highest-probability proof
    Given having executed:
      """
      CREATE (a:Node {name: 'A'})-[:LINK {p: 0.9}]->(b:Node {name: 'B'}),
             (a)-[:LINK {p: 0.5}]->(c:Node {name: 'C'}),
             (c)-[:LINK {p: 0.8}]->(b)
      """
    When evaluating the following Locy program with exact_probability and top_k_proofs 1:
      """
      CREATE RULE reach AS
        MATCH (x:Node)-[e:LINK]->(y:Node)
        FOLD prob = MNOR(e.p)
        YIELD KEY x, KEY y, prob
      """
    Then evaluation should succeed
    And the derived relation 'reach' should have 3 facts

  Scenario: top_k=5 handles diamond graph identically to unlimited
    Given having executed:
      """
      CREATE (a:Node {name: 'A'})-[:LINK {p: 0.7}]->(b:Node {name: 'B'}),
             (a)-[:LINK {p: 0.6}]->(b)
      """
    When evaluating the following Locy program with exact_probability and top_k_proofs 5:
      """
      CREATE RULE reach AS
        MATCH (x:Node)-[e:LINK]->(y:Node)
        FOLD prob = MNOR(e.p)
        YIELD KEY x, KEY y, prob
      """
    Then evaluation should succeed
    And the derived relation 'reach' should have 1 facts
    And the derived relation 'reach' should contain a fact where x.name = 'A' and y.name = 'B'

  Scenario: EXPLAIN output includes proof_probability when top_k active
    Given having executed:
      """
      CREATE (a:Node {name: 'A'})-[:LINK {p: 0.9}]->(b:Node {name: 'B'})
      """
    When evaluating the following Locy program with exact_probability and top_k_proofs 3:
      """
      CREATE RULE reach AS
        MATCH (x:Node)-[e:LINK]->(y:Node)
        FOLD prob = MNOR(e.p)
        YIELD KEY x, KEY y, prob
      EXPLAIN RULE reach WHERE x.name = 'A'
      """
    Then evaluation should succeed
    And the command result 0 should be an Explain with rule 'reach'
    And the command result 0 should be an Explain with 1 children
    And the command result 0 should be an Explain where child 0 has proof_probability approximately 0.9

  # reach(A, C) has two derivations: the edge A -> C (0.3), recorded first,
  # and A -> B (0.5) times reach(B, C) (0.8), 0.4. A derivation weighs its own
  # PROB value, so top_k_proofs 1 keeps the 0.4 one.
  Scenario: top_k=1 keeps the heaviest derivation of a fact
    Given having executed:
      """
      CREATE (a:Node {name: 'A'})-[:LINK {p: 0.5}]->(b:Node {name: 'B'}),
             (b)-[:LINK {p: 0.8}]->(c:Node {name: 'C'}),
             (a)-[:LINK {p: 0.3}]->(c)
      """
    When evaluating the following Locy program with exact_probability and top_k_proofs 1:
      """
      CREATE RULE reach AS
        MATCH (x:Node)-[e:LINK]->(y:Node)
        FOLD prob = MNOR(e.p)
        YIELD KEY x, KEY y, prob
      CREATE RULE reach AS
        MATCH (x:Node)-[e:LINK]->(m:Node) WHERE m IS reach TO y
        FOLD prob = MNOR(e.p)
        YIELD KEY x, KEY y, prob
      EXPLAIN RULE reach WHERE x.name = 'A' AND y.name = 'C'
      """
    Then evaluation should succeed
    And the command result 0 should be an Explain with 1 children
    And the command result 0 should be an Explain where child 0 has proof_probability approximately 0.4

  Scenario: top_k=2 keeps both derivations of a fact
    Given having executed:
      """
      CREATE (a:Node {name: 'A'})-[:LINK {p: 0.5}]->(b:Node {name: 'B'}),
             (b)-[:LINK {p: 0.8}]->(c:Node {name: 'C'}),
             (a)-[:LINK {p: 0.3}]->(c)
      """
    When evaluating the following Locy program with exact_probability and top_k_proofs 2:
      """
      CREATE RULE reach AS
        MATCH (x:Node)-[e:LINK]->(y:Node)
        FOLD prob = MNOR(e.p)
        YIELD KEY x, KEY y, prob
      CREATE RULE reach AS
        MATCH (x:Node)-[e:LINK]->(m:Node) WHERE m IS reach TO y
        FOLD prob = MNOR(e.p)
        YIELD KEY x, KEY y, prob
      EXPLAIN RULE reach WHERE x.name = 'A' AND y.name = 'C'
      """
    Then evaluation should succeed
    And the command result 0 should be an Explain with 2 children
    And the command result 0 should be an Explain where all children have proof_probability
