Feature: Top-level commands run in sequence

  A program's commands run one after another: a command reads the state
  every earlier command of the same program left. A DERIVE or a Cypher
  write changes the graph, and a later QUERY, DERIVE, EXPLAIN RULE,
  ABDUCE or ASSUME reads the facts derived again in the new state.

  Background:
    Given an empty graph
    And having executed:
      """
      CREATE (a:Person {name: 'Alice'})-[:KNOWS]->(b:Person {name: 'Bob'})
      """

  Scenario: QUERY after DERIVE reads the derived edges
    When evaluating the following Locy program:
      """
      CREATE RULE link AS
        MATCH (a:Person)-[:KNOWS]->(b:Person)
        DERIVE (a)-[:FRIEND]->(b)
      CREATE RULE friends AS
        MATCH (a:Person)-[:FRIEND]->(b:Person)
        YIELD KEY a, KEY b
      DERIVE link
      QUERY friends RETURN a.name AS src, b.name AS dst
      """
    Then evaluation should succeed
    And the command result 0 should be a Derive with at least 1 affected
    And the command result 1 should be a Query with 1 rows
    And the command result 1 should be a Query containing row where src = 'Alice'

  Scenario: QUERY before and after a DERIVE reads each state
    When evaluating the following Locy program:
      """
      CREATE RULE link AS
        MATCH (a:Person)-[:KNOWS]->(b:Person)
        DERIVE (a)-[:FRIEND]->(b)
      CREATE RULE friends AS
        MATCH (a:Person)-[:FRIEND]->(b:Person)
        YIELD KEY a, KEY b
      QUERY friends RETURN a.name AS src
      DERIVE link
      QUERY friends RETURN a.name AS src
      """
    Then evaluation should succeed
    And the command result 0 should be a Query with 0 rows
    And the command result 2 should be a Query with 1 rows

  Scenario: A second DERIVE reads the first DERIVE's edges
    When evaluating the following Locy program:
      """
      CREATE RULE link AS
        MATCH (a:Person)-[:KNOWS]->(b:Person)
        DERIVE (a)-[:FRIEND]->(b)
      CREATE RULE close AS
        MATCH (a:Person)-[:FRIEND]->(b:Person)
        DERIVE (a)-[:CLOSE]->(b)
      DERIVE link
      DERIVE close
      """
    Then evaluation should succeed
    And the command result 0 should be a Derive affecting 1 elements
    And the command result 1 should be a Derive affecting 1 elements
    And the graph should contain an edge from 'Alice' to 'Bob' with type 'CLOSE'

  Scenario: A Cypher write is a command, and a later QUERY reads it
    When evaluating the following Locy program:
      """
      CREATE RULE friends AS
        MATCH (a:Person)-[:FRIEND]->(b:Person)
        YIELD KEY a, KEY b
      MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'})
      CREATE (a)-[:FRIEND]->(b)
      QUERY friends RETURN a.name AS src
      """
    Then evaluation should succeed
    And the command result 1 should be a Query with 1 rows
    And the graph should contain an edge from 'Alice' to 'Bob' with type 'FRIEND'

  Scenario: A recursive rule reads the derived edges
    Given having executed:
      """
      MATCH (b:Person {name: 'Bob'})
      CREATE (b)-[:KNOWS]->(:Person {name: 'Carol'})
      """
    When evaluating the following Locy program:
      """
      CREATE RULE link AS
        MATCH (a:Person)-[:KNOWS]->(b:Person)
        DERIVE (a)-[:FRIEND]->(b)
      CREATE RULE reach AS
        MATCH (a:Person)-[:FRIEND]->(b:Person)
        YIELD KEY a, KEY b
      CREATE RULE reach AS
        MATCH (a:Person)-[:FRIEND]->(m:Person) WHERE m IS reach TO b
        YIELD KEY a, KEY b
      DERIVE link
      QUERY reach RETURN a.name AS src, b.name AS dst
      """
    Then evaluation should succeed
    And the command result 1 should be a Query with 3 rows
    And the command result 1 should be a Query containing row where dst = 'Carol'
