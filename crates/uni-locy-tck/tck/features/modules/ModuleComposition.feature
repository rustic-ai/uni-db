Feature: Module Composition (MODULE / USE)

  Tests MODULE declaration and USE import syntax, and USE resolution against
  modules registered with `rules().register`. A `MODULE a.b` program's rules
  are registered as `a.b.<rule>`; `USE a.b` imports all of them under their
  short names and `USE a.b { r }` only the listed ones.

  Background:
    Given an empty graph

  # ── Parse level ───────────────────────────────────────────────────────

  Scenario: MODULE declaration parses
    When parsing the following Locy program:
      """
      MODULE acme.compliance
      CREATE RULE control AS MATCH (a)-[:OWNS]->(b) YIELD KEY a, KEY b
      """
    Then the program should parse successfully

  Scenario: USE import parses
    When parsing the following Locy program:
      """
      USE acme.compliance
      CREATE RULE flagged AS MATCH (n) WHERE n IS control YIELD KEY n
      """
    Then the program should parse successfully

  Scenario: MODULE with multiple USE imports parses
    When parsing the following Locy program:
      """
      MODULE acme.risk
      USE acme.compliance
      USE acme.sanctions
      CREATE RULE combined AS MATCH (n) WHERE n IS control, n IS NOT sanctioned YIELD KEY n
      """
    Then the program should parse successfully

  Scenario: Selective imports with multiple identifiers
    When parsing the following Locy program:
      """
      USE acme.compliance { control, reachable }
      CREATE RULE flagged AS MATCH (n) WHERE n IS control YIELD KEY n
      """
    Then the program should parse successfully

  Scenario: Single selective import
    When parsing the following Locy program:
      """
      USE acme.compliance { control }
      CREATE RULE flagged AS MATCH (n) WHERE n IS control YIELD KEY n
      """
    Then the program should parse successfully

  Scenario: Backtick-quoted reserved keyword in import list
    When parsing the following Locy program:
      """
      USE acme.compliance { `rule`, reachable }
      CREATE RULE flagged AS MATCH (n) WHERE n IS reachable YIELD KEY n
      """
    Then the program should parse successfully

  Scenario: Empty selective import list fails to parse
    When parsing the following Locy program:
      """
      USE acme.compliance { }
      CREATE RULE flagged AS MATCH (n) YIELD KEY n
      """
    Then the program should fail to parse

  Scenario: Glob import backward compatibility
    When parsing the following Locy program:
      """
      USE acme.compliance
      CREATE RULE flagged AS MATCH (n) WHERE n IS control YIELD KEY n
      """
    Then the program should parse successfully

  # ── Resolution against registered modules ───────────────────────────────

  Scenario: Glob USE resolves a registered module
    Given having executed:
      """
      CREATE (:Company {name: 'Parent'})-[:OWNS]->(:Company {name: 'Child'})
      """
    And the following Locy rules are registered:
      """
      MODULE acme.compliance
      CREATE RULE control AS MATCH (a:Company)-[:OWNS]->(b:Company) YIELD KEY a, KEY b
      """
    When evaluating the following Locy program:
      """
      USE acme.compliance
      CREATE RULE controller AS MATCH (a:Company)-[:OWNS]->(b:Company) WHERE (a, b) IS control YIELD KEY a
      QUERY controller RETURN a.name AS name
      """
    Then evaluation should succeed
    And the command result 0 should be a Query with 1 rows
    And the command result 0 should be a Query containing row where name = 'Parent'

  Scenario: Selective USE resolves the listed rule
    Given having executed:
      """
      CREATE (:Company {name: 'Parent'})-[:OWNS]->(:Company {name: 'Child'})
      """
    And the following Locy rules are registered:
      """
      MODULE acme.compliance
      CREATE RULE control AS MATCH (a:Company)-[:OWNS]->(b:Company) YIELD KEY a, KEY b
      CREATE RULE owned AS MATCH (a:Company)-[:OWNS]->(b:Company) YIELD KEY b
      """
    When evaluating the following Locy program:
      """
      USE acme.compliance { owned }
      QUERY owned RETURN b.name AS name
      """
    Then evaluation should succeed
    And the command result 0 should be a Query with 1 rows
    And the command result 0 should be a Query containing row where name = 'Child'

  Scenario: Selective USE does not import unlisted rules
    Given the following Locy rules are registered:
      """
      MODULE acme.compliance
      CREATE RULE control AS MATCH (a:Company)-[:OWNS]->(b:Company) YIELD KEY a, KEY b
      CREATE RULE owned AS MATCH (a:Company)-[:OWNS]->(b:Company) YIELD KEY b
      """
    When evaluating the following Locy program:
      """
      USE acme.compliance { owned }
      QUERY control RETURN a.name AS name
      """
    Then evaluation should fail
    And the evaluation error should mention "control"

  Scenario: USE of an unknown rule is an error
    Given the following Locy rules are registered:
      """
      MODULE acme.compliance
      CREATE RULE control AS MATCH (a:Company)-[:OWNS]->(b:Company) YIELD KEY a, KEY b
      """
    When evaluating the following Locy program:
      """
      USE acme.compliance { sanctioned }
      QUERY control RETURN a.name AS name
      """
    Then evaluation should fail
    And the evaluation error should mention "sanctioned"

  Scenario: USE of an unknown module is an error
    When evaluating the following Locy program:
      """
      USE acme.sanctions
      CREATE RULE flagged AS MATCH (n) YIELD KEY n
      """
    Then evaluation should fail
    And the evaluation error should mention "acme.sanctions"
