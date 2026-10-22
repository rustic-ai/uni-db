Feature: ASSUME returns one result per body command

  An ASSUME ... THEN body's commands run in sequence in the hypothetical
  state, and the ASSUME's result holds one result per body command, in
  order: a QUERY's rows, a DERIVE's affected count, a Cypher command's
  rows, a nested ASSUME's own results. Its derived facts are those of the
  hypothetical state the first body command reads: after the ASSUME's
  mutations, before any body command. A result is addressed by its path:
  `0.2` is body command 2 of top-level command 0.

  Background:
    Given an empty graph
    And having executed:
      """
      CREATE (a:Person {name: 'A'})-[:KNOWS]->(b:Person {name: 'B'})
             -[:KNOWS]->(c:Person {name: 'C'})-[:KNOWS]->(d:Person {name: 'D'})
      """

  Scenario: QUERY, DERIVE, QUERY and Cypher each return their own result
    When evaluating the following Locy program:
      """
      CREATE RULE knows AS MATCH (a:Person)-[:KNOWS]->(b:Person) YIELD KEY a, KEY b
      CREATE RULE link AS MATCH (a:Person)-[:KNOWS]->(b:Person) DERIVE (a)-[:LINK]->(b)
      CREATE RULE linked AS MATCH (a:Person)-[:LINK]->(b:Person) YIELD KEY a, KEY b
      ASSUME {
        MATCH (d:Person {name: 'D'}) CREATE (d)-[:KNOWS]->(:Person {name: 'E'})
      } THEN {
        QUERY linked RETURN a.name AS a, b.name AS b
        DERIVE link
        QUERY linked RETURN a.name AS a, b.name AS b
        MATCH (:Person)-[r:LINK]->(:Person) RETURN count(r) AS n
      }
      """
    Then evaluation should succeed
    And the command result 0 should be an Assume with 4 results
    And the result at path 0.0 should be a Query with 0 rows
    And the result at path 0.1 should be a Derive affecting 4 elements
    And the result at path 0.2 should be a Query with 4 rows
    And the result at path 0.2 should be a Query containing row where a = 'D'
    And the result at path 0.2 should be a Query containing row where b = 'E'
    And the result at path 0.3 should be a Cypher with 1 rows
    And the result at path 0.3 should be a Cypher containing row where n = 4
    And the result at path 0 should be an Assume whose derived relation 'knows' has 4 facts
    And the result at path 0 should be an Assume whose derived relation 'linked' has 0 facts
    And the derived relation 'knows' should have 3 facts
    And the graph should NOT contain an edge with type 'LINK'

  Scenario: A nested ASSUME returns its own results inside the outer one
    When evaluating the following Locy program:
      """
      CREATE RULE link AS MATCH (a:Person)-[:KNOWS]->(b:Person) DERIVE (a)-[:LINK]->(b)
      CREATE RULE linked AS MATCH (a:Person)-[:LINK]->(b:Person) YIELD KEY a, KEY b
      ASSUME { CREATE (:Marker {name: 'm'}) } THEN {
        DERIVE link
        ASSUME {
          MATCH (a:Person {name: 'A'}), (d:Person {name: 'D'}) CREATE (a)-[:LINK]->(d)
        } THEN {
          QUERY linked RETURN a.name AS a, b.name AS b
        }
        QUERY linked RETURN a.name AS a, b.name AS b
      }
      """
    Then evaluation should succeed
    And the command result 0 should be an Assume with 3 results
    And the result at path 0.0 should be a Derive affecting 3 elements
    And the result at path 0.1 should be an Assume with 1 results
    And the result at path 0.1.0 should be a Query with 4 rows
    And the result at path 0.1.0 should be a Query containing row where b = 'D'
    And the result at path 0.1 should be an Assume whose derived relation 'linked' has 4 facts
    And the result at path 0.2 should be a Query with 3 rows
    And the result at path 0 should be an Assume whose derived relation 'linked' has 0 facts
    And the graph should NOT contain an edge with type 'LINK'

  Scenario: A body of rules only returns no results, and its rules' facts as derived
    When evaluating the following Locy program:
      """
      CREATE RULE knows AS MATCH (a:Person)-[:KNOWS]->(b:Person) YIELD KEY a, KEY b
      ASSUME {
        MATCH (d:Person {name: 'D'}) CREATE (d)-[:KNOWS]->(:Person {name: 'E'})
      } THEN {
        CREATE RULE knows_e AS MATCH (a:Person)-[:KNOWS]->(:Person {name: 'E'}) YIELD KEY a
      }
      """
    Then evaluation should succeed
    And the command result 0 should be an Assume with 0 results
    And the result at path 0 should be an Assume whose derived relation 'knows' has 4 facts
    And the result at path 0 should be an Assume whose derived relation 'knows_e' has 1 facts
    And the derived relation 'knows' should have 3 facts
