Feature: DERIVE Clause Parsing

  Tests parsing of DERIVE patterns for edge materialization.

  Background:
    Given an empty graph

  Scenario: Forward edge derive
    When parsing the following Locy program:
      """
      CREATE RULE link AS MATCH (a)-[:KNOWS]->(b) DERIVE (a)-[:FRIEND]->(b)
      """
    Then the program should parse successfully

  Scenario: Backward edge derive
    When parsing the following Locy program:
      """
      CREATE RULE back AS MATCH (a)-[:KNOWS]->(b) DERIVE (b)<-[:KNOWN_BY]-(a)
      """
    Then the program should parse successfully

  Scenario: DERIVE MERGE two nodes
    When parsing the following Locy program:
      """
      CREATE RULE same AS MATCH (a)-[:SAME_AS]->(b) DERIVE MERGE a, b
      """
    Then the program should parse successfully

  Scenario: DERIVE NEW node
    When parsing the following Locy program:
      """
      CREATE RULE categorize AS MATCH (c) DERIVE (NEW x:Country)<-[:IN]-(c)
      """
    Then the program should parse successfully

  # ── Compile-level scenarios ────────────────────────────────────────────

  Scenario: DERIVE edge rule compiles successfully
    When compiling the following Locy program:
      """
      CREATE RULE link AS
        MATCH (a:Person)-[:KNOWS]->(b:Person)
        DERIVE (a)-[:FRIEND]->(b)
      """
    Then the program should compile successfully

  Scenario: DERIVE NEW rule compiles successfully
    When compiling the following Locy program:
      """
      CREATE RULE categorize AS
        MATCH (c:City)
        DERIVE (NEW x:Country)<-[:IN]-(c)
      """
    Then the program should compile successfully

  # ── Evaluate-level scenarios ──────────────────────────────────────────

  Scenario: DERIVE creates edges in the graph
    Given having executed:
      """
      CREATE (a:Person {name: 'Alice'})-[:KNOWS]->(b:Person {name: 'Bob'})
      """
    When evaluating the following Locy program:
      """
      CREATE RULE link AS
        MATCH (a:Person)-[:KNOWS]->(b:Person)
        DERIVE (a)-[:FRIEND]->(b)
      DERIVE link
      """
    Then evaluation should succeed
    And the graph should contain an edge from 'Alice' to 'Bob' with type 'FRIEND'

  Scenario: DERIVE NEW creates a Skolem node
    Given having executed:
      """
      CREATE (:City {name: 'Paris'})
      """
    When evaluating the following Locy program:
      """
      CREATE RULE categorize AS
        MATCH (c:City)
        DERIVE (NEW x:Country)<-[:IN]-(c)
      DERIVE categorize
      """
    Then evaluation should succeed
    And the graph should contain 1 nodes with label 'Country'

  Scenario: DERIVE NEW stores the new node's properties
    Given having executed:
      """
      CREATE (:City {name: 'Paris', country: 'France'})
      CREATE (:City {name: 'Rome', country: 'Italy'})
      """
    When evaluating the following Locy program:
      """
      CREATE RULE categorize AS
        MATCH (c:City)
        DERIVE (NEW x:Country {name: c.country, capital: c.name})<-[:IN]-(c)
      DERIVE categorize
      MATCH (x:Country)<-[:IN]-(c:City) RETURN x.name AS country, x.capital AS capital
      """
    Then evaluation should succeed
    And the graph should contain 2 nodes with label 'Country'
    And the command result 1 should be a Cypher with at least 2 rows
    And the command result 1 should be a Cypher containing row where country = 'France'
    And the command result 1 should be a Cypher containing row where capital = 'Rome'

  Scenario: DERIVE idempotency — re-derive does not duplicate
    Given having executed:
      """
      CREATE (a:Person {name: 'Alice'})-[:KNOWS]->(b:Person {name: 'Bob'})
      """
    When evaluating the following Locy program:
      """
      CREATE RULE link AS
        MATCH (a:Person)-[:KNOWS]->(b:Person)
        DERIVE (a)-[:FRIEND]->(b)
      DERIVE link
      """
    Then evaluation should succeed
    And the evaluation stats should show 1 mutations executed
