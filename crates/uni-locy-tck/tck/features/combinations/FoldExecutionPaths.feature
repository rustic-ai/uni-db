Feature: FOLD Execution Paths — All 6 FOLD Operators Across Execution Paths

  Tests all 6 monotonic FOLD operators (MNOR, MPROD, MSUM, MMAX, MMIN, MCOUNT)
  across the four execution paths: derived relations, QUERY, ASSUME, and ABDUCE.
  Validates that aggregation results are consistent regardless of how they are
  accessed or mutated.

  Background:
    Given an empty graph

  # Known limitations resolved:
  # - MCOUNT ABDUCE: fixed via FoldExec schema reconciliation
  # - Multi-FOLD same program: fixed via name-based column resolution

  # ═══════════════════════════════════════════════════════════════════════════
  # MNOR — noisy-OR: 1 - (1-0.3)*(1-0.5) = 0.65
  # ═══════════════════════════════════════════════════════════════════════════

  Scenario: MNOR derived relation computes correct noisy-OR
    Given having executed:
      """
      CREATE (:Node {name: 'A'}), (:Node {name: 'B'})
      """
    And having executed:
      """
      MATCH (a:Node {name: 'A'}), (b:Node {name: 'B'})
      CREATE (a)-[:CAUSE {prob: 0.3}]->(b)
      """
    And having executed:
      """
      MATCH (a:Node {name: 'A'}), (b:Node {name: 'B'})
      CREATE (a)-[:CAUSE {prob: 0.5}]->(b)
      """
    When evaluating the following Locy program:
      """
      CREATE RULE risk AS
        MATCH (a:Node)-[e:CAUSE]->(b:Node)
        FOLD p = MNOR(e.prob)
        YIELD KEY a, KEY b, p
      """
    Then evaluation should succeed
    And the derived relation 'risk' should have 1 facts
    And the derived relation 'risk' should contain a fact where p = 0.65

  Scenario: MNOR via QUERY returns correct value
    Given having executed:
      """
      CREATE (:Node {name: 'A'}), (:Node {name: 'B'})
      """
    And having executed:
      """
      MATCH (a:Node {name: 'A'}), (b:Node {name: 'B'})
      CREATE (a)-[:CAUSE {prob: 0.3}]->(b)
      """
    And having executed:
      """
      MATCH (a:Node {name: 'A'}), (b:Node {name: 'B'})
      CREATE (a)-[:CAUSE {prob: 0.5}]->(b)
      """
    When evaluating the following Locy program:
      """
      CREATE RULE risk AS
        MATCH (a:Node)-[e:CAUSE]->(b:Node)
        FOLD p = MNOR(e.prob)
        YIELD KEY a, KEY b, p
      QUERY risk WHERE a = a RETURN a.name AS src, b.name AS dst, p
      """
    Then evaluation should succeed
    And the derived relation 'risk' should have 1 facts
    And the derived relation 'risk' should contain a fact where p = 0.65
    And the command result 0 should be a Query with 1 rows
    And the command result 0 should be a Query containing row where p = 0.65

  Scenario: MNOR via ASSUME reflects hypothetical edge removal
    Given having executed:
      """
      CREATE (:Node {name: 'A'}), (:Node {name: 'B'})
      """
    And having executed:
      """
      MATCH (a:Node {name: 'A'}), (b:Node {name: 'B'})
      CREATE (a)-[:CAUSE {prob: 0.3}]->(b)
      """
    And having executed:
      """
      MATCH (a:Node {name: 'A'}), (b:Node {name: 'B'})
      CREATE (a)-[:CAUSE {prob: 0.5}]->(b)
      """
    When evaluating the following Locy program:
      """
      CREATE RULE risk AS
        MATCH (a:Node)-[e:CAUSE]->(b:Node)
        FOLD p = MNOR(e.prob)
        YIELD KEY a, KEY b, p
      ASSUME { MATCH (:Node {name: 'A'})-[e:CAUSE {prob: 0.5}]->(:Node {name: 'B'}) DELETE e }
      THEN { QUERY risk WHERE a.name = 'A' RETURN p }
      """
    Then evaluation should succeed
    And the derived relation 'risk' should contain a fact where p = 0.65
    And the command result 0 should be an Assume containing row where p = 0.3

  Scenario: MNOR via ABDUCE finds edge removal candidate
    Given having executed:
      """
      CREATE (:Node {name: 'A'}), (:Node {name: 'B'})
      """
    And having executed:
      """
      MATCH (a:Node {name: 'A'}), (b:Node {name: 'B'})
      CREATE (a)-[:CAUSE {prob: 0.3}]->(b)
      """
    And having executed:
      """
      MATCH (a:Node {name: 'A'}), (b:Node {name: 'B'})
      CREATE (a)-[:CAUSE {prob: 0.5}]->(b)
      """
    When evaluating the following Locy program:
      """
      CREATE RULE risk AS
        MATCH (a:Node)-[e:CAUSE]->(b:Node)
        FOLD p = MNOR(e.prob)
        YIELD KEY a, KEY b, p
      ABDUCE NOT risk WHERE a.name = 'A'
      """
    Then evaluation should succeed
    And the derived relation 'risk' should contain a fact where p = 0.65
    And the command result 0 should be an Abduce with at least 1 modifications

  # ═══════════════════════════════════════════════════════════════════════════
  # MPROD — product: 0.8 * 0.9 = 0.72
  # ═══════════════════════════════════════════════════════════════════════════

  Scenario: MPROD derived relation computes correct product
    Given having executed:
      """
      CREATE (:Node {name: 'A'}), (:Node {name: 'B'})
      """
    And having executed:
      """
      MATCH (a:Node {name: 'A'}), (b:Node {name: 'B'})
      CREATE (a)-[:CHECK {conf: 0.8}]->(b)
      """
    And having executed:
      """
      MATCH (a:Node {name: 'A'}), (b:Node {name: 'B'})
      CREATE (a)-[:CHECK {conf: 0.9}]->(b)
      """
    When evaluating the following Locy program:
      """
      CREATE RULE coverage AS
        MATCH (a:Node)-[e:CHECK]->(b:Node)
        FOLD p = MPROD(e.conf)
        YIELD KEY a, KEY b, p
      """
    Then evaluation should succeed
    And the derived relation 'coverage' should have 1 facts
    And the derived relation 'coverage' should contain a fact where p = 0.72

  Scenario: MPROD via QUERY returns correct value
    Given having executed:
      """
      CREATE (:Node {name: 'A'}), (:Node {name: 'B'})
      """
    And having executed:
      """
      MATCH (a:Node {name: 'A'}), (b:Node {name: 'B'})
      CREATE (a)-[:CHECK {conf: 0.8}]->(b)
      """
    And having executed:
      """
      MATCH (a:Node {name: 'A'}), (b:Node {name: 'B'})
      CREATE (a)-[:CHECK {conf: 0.9}]->(b)
      """
    When evaluating the following Locy program:
      """
      CREATE RULE coverage AS
        MATCH (a:Node)-[e:CHECK]->(b:Node)
        FOLD p = MPROD(e.conf)
        YIELD KEY a, KEY b, p
      QUERY coverage WHERE a = a RETURN a.name AS src, b.name AS dst, p
      """
    Then evaluation should succeed
    And the derived relation 'coverage' should have 1 facts
    And the derived relation 'coverage' should contain a fact where p = 0.72
    And the command result 0 should be a Query with 1 rows
    And the command result 0 should be a Query containing row where p = 0.72

  Scenario: MPROD via ASSUME reflects hypothetical edge removal
    Given having executed:
      """
      CREATE (:Node {name: 'A'}), (:Node {name: 'B'})
      """
    And having executed:
      """
      MATCH (a:Node {name: 'A'}), (b:Node {name: 'B'})
      CREATE (a)-[:CHECK {conf: 0.8}]->(b)
      """
    And having executed:
      """
      MATCH (a:Node {name: 'A'}), (b:Node {name: 'B'})
      CREATE (a)-[:CHECK {conf: 0.9}]->(b)
      """
    When evaluating the following Locy program:
      """
      CREATE RULE coverage AS
        MATCH (a:Node)-[e:CHECK]->(b:Node)
        FOLD p = MPROD(e.conf)
        YIELD KEY a, KEY b, p
      ASSUME { MATCH (:Node {name: 'A'})-[e:CHECK {conf: 0.9}]->(:Node {name: 'B'}) DELETE e }
      THEN { QUERY coverage WHERE a.name = 'A' RETURN p }
      """
    Then evaluation should succeed
    And the derived relation 'coverage' should contain a fact where p = 0.72
    And the command result 0 should be an Assume containing row where p = 0.8

  Scenario: MPROD via ABDUCE finds edge removal candidate
    Given having executed:
      """
      CREATE (:Node {name: 'A'}), (:Node {name: 'B'})
      """
    And having executed:
      """
      MATCH (a:Node {name: 'A'}), (b:Node {name: 'B'})
      CREATE (a)-[:CHECK {conf: 0.8}]->(b)
      """
    And having executed:
      """
      MATCH (a:Node {name: 'A'}), (b:Node {name: 'B'})
      CREATE (a)-[:CHECK {conf: 0.9}]->(b)
      """
    When evaluating the following Locy program:
      """
      CREATE RULE coverage AS
        MATCH (a:Node)-[e:CHECK]->(b:Node)
        FOLD p = MPROD(e.conf)
        YIELD KEY a, KEY b, p
      ABDUCE NOT coverage WHERE a.name = 'A'
      """
    Then evaluation should succeed
    And the derived relation 'coverage' should contain a fact where p = 0.72
    And the command result 0 should be an Abduce with at least 1 modifications

  # ═══════════════════════════════════════════════════════════════════════════
  # MSUM — sum: 10 + 20 + 30 = 60
  # ═══════════════════════════════════════════════════════════════════════════

  Scenario: MSUM derived relation computes correct sum
    Given having executed:
      """
      CREATE (:Dept {name: 'Eng'}), (:Person {name: 'Alice'}), (:Person {name: 'Bob'}), (:Person {name: 'Carol'})
      """
    And having executed:
      """
      MATCH (p:Person {name: 'Alice'}), (d:Dept {name: 'Eng'})
      CREATE (p)-[:WORKS_IN {hours: 10}]->(d)
      """
    And having executed:
      """
      MATCH (p:Person {name: 'Bob'}), (d:Dept {name: 'Eng'})
      CREATE (p)-[:WORKS_IN {hours: 20}]->(d)
      """
    And having executed:
      """
      MATCH (p:Person {name: 'Carol'}), (d:Dept {name: 'Eng'})
      CREATE (p)-[:WORKS_IN {hours: 30}]->(d)
      """
    When evaluating the following Locy program:
      """
      CREATE RULE dept_hours AS
        MATCH (p:Person)-[w:WORKS_IN]->(d:Dept)
        FOLD total = MSUM(w.hours)
        YIELD KEY d, total
      """
    Then evaluation should succeed
    And the derived relation 'dept_hours' should have 1 facts
    And the derived relation 'dept_hours' should contain a fact where total = 60.0

  Scenario: MSUM via QUERY returns correct value
    Given having executed:
      """
      CREATE (:Dept {name: 'Eng'}), (:Person {name: 'Alice'}), (:Person {name: 'Bob'}), (:Person {name: 'Carol'})
      """
    And having executed:
      """
      MATCH (p:Person {name: 'Alice'}), (d:Dept {name: 'Eng'})
      CREATE (p)-[:WORKS_IN {hours: 10}]->(d)
      """
    And having executed:
      """
      MATCH (p:Person {name: 'Bob'}), (d:Dept {name: 'Eng'})
      CREATE (p)-[:WORKS_IN {hours: 20}]->(d)
      """
    And having executed:
      """
      MATCH (p:Person {name: 'Carol'}), (d:Dept {name: 'Eng'})
      CREATE (p)-[:WORKS_IN {hours: 30}]->(d)
      """
    When evaluating the following Locy program:
      """
      CREATE RULE dept_hours AS
        MATCH (p:Person)-[w:WORKS_IN]->(d:Dept)
        FOLD total = MSUM(w.hours)
        YIELD KEY d, total
      QUERY dept_hours WHERE d = d RETURN d.name AS dept, total
      """
    Then evaluation should succeed
    And the derived relation 'dept_hours' should have 1 facts
    And the derived relation 'dept_hours' should contain a fact where total = 60.0
    And the command result 0 should be a Query with 1 rows
    And the command result 0 should be a Query containing row where total = 60.0

  Scenario: MSUM via ASSUME reflects hypothetical edge addition
    Given having executed:
      """
      CREATE (:Dept {name: 'Eng'}), (:Person {name: 'Alice'}), (:Person {name: 'Bob'}), (:Person {name: 'Carol'})
      """
    And having executed:
      """
      MATCH (p:Person {name: 'Alice'}), (d:Dept {name: 'Eng'})
      CREATE (p)-[:WORKS_IN {hours: 10}]->(d)
      """
    And having executed:
      """
      MATCH (p:Person {name: 'Bob'}), (d:Dept {name: 'Eng'})
      CREATE (p)-[:WORKS_IN {hours: 20}]->(d)
      """
    When evaluating the following Locy program:
      """
      CREATE RULE dept_hours AS
        MATCH (p:Person)-[w:WORKS_IN]->(d:Dept)
        FOLD total = MSUM(w.hours)
        YIELD KEY d, total
      ASSUME { MATCH (p:Person {name: 'Carol'}), (d:Dept {name: 'Eng'}) CREATE (p)-[:WORKS_IN {hours: 30}]->(d) }
      THEN { QUERY dept_hours WHERE d.name = 'Eng' RETURN total }
      """
    Then evaluation should succeed
    And the derived relation 'dept_hours' should contain a fact where total = 30.0
    And the command result 0 should be an Assume containing row where total = 60.0

  Scenario: MSUM via ABDUCE finds modification candidate
    Given having executed:
      """
      CREATE (:Dept {name: 'Eng'}), (:Person {name: 'Alice'}), (:Person {name: 'Bob'})
      """
    And having executed:
      """
      MATCH (p:Person {name: 'Alice'}), (d:Dept {name: 'Eng'})
      CREATE (p)-[:WORKS_IN {hours: 10}]->(d)
      """
    And having executed:
      """
      MATCH (p:Person {name: 'Bob'}), (d:Dept {name: 'Eng'})
      CREATE (p)-[:WORKS_IN {hours: 20}]->(d)
      """
    When evaluating the following Locy program:
      """
      CREATE RULE dept_hours AS
        MATCH (p:Person)-[w:WORKS_IN]->(d:Dept)
        FOLD total = MSUM(w.hours)
        YIELD KEY d, total
      ABDUCE NOT dept_hours WHERE d.name = 'Eng'
      """
    Then evaluation should succeed
    And the derived relation 'dept_hours' should contain a fact where total = 30.0
    And the command result 0 should be an Abduce with at least 1 modifications

  # ═══════════════════════════════════════════════════════════════════════════
  # MMAX — max: max(3, 7, 10) = 10
  # ═══════════════════════════════════════════════════════════════════════════

  Scenario: MMAX derived relation computes correct maximum
    Given having executed:
      """
      CREATE (:Sensor {name: 'S1'}), (:Reading {ts: 1}), (:Reading {ts: 2}), (:Reading {ts: 3})
      """
    And having executed:
      """
      MATCH (r:Reading {ts: 1}), (s:Sensor {name: 'S1'})
      CREATE (r)-[:FROM {value: 3}]->(s)
      """
    And having executed:
      """
      MATCH (r:Reading {ts: 2}), (s:Sensor {name: 'S1'})
      CREATE (r)-[:FROM {value: 7}]->(s)
      """
    And having executed:
      """
      MATCH (r:Reading {ts: 3}), (s:Sensor {name: 'S1'})
      CREATE (r)-[:FROM {value: 10}]->(s)
      """
    When evaluating the following Locy program:
      """
      CREATE RULE peak AS
        MATCH (r:Reading)-[f:FROM]->(s:Sensor)
        FOLD mx = MMAX(f.value)
        YIELD KEY s, mx
      """
    Then evaluation should succeed
    And the derived relation 'peak' should have 1 facts
    And the derived relation 'peak' should contain a fact where mx = 10.0

  Scenario: MMAX via QUERY returns correct value
    Given having executed:
      """
      CREATE (:Sensor {name: 'S1'}), (:Reading {ts: 1}), (:Reading {ts: 2}), (:Reading {ts: 3})
      """
    And having executed:
      """
      MATCH (r:Reading {ts: 1}), (s:Sensor {name: 'S1'})
      CREATE (r)-[:FROM {value: 3}]->(s)
      """
    And having executed:
      """
      MATCH (r:Reading {ts: 2}), (s:Sensor {name: 'S1'})
      CREATE (r)-[:FROM {value: 7}]->(s)
      """
    And having executed:
      """
      MATCH (r:Reading {ts: 3}), (s:Sensor {name: 'S1'})
      CREATE (r)-[:FROM {value: 10}]->(s)
      """
    When evaluating the following Locy program:
      """
      CREATE RULE peak AS
        MATCH (r:Reading)-[f:FROM]->(s:Sensor)
        FOLD mx = MMAX(f.value)
        YIELD KEY s, mx
      QUERY peak WHERE s = s RETURN s.name AS sensor, mx
      """
    Then evaluation should succeed
    And the derived relation 'peak' should have 1 facts
    And the derived relation 'peak' should contain a fact where mx = 10.0
    And the command result 0 should be a Query with 1 rows
    And the command result 0 should be a Query containing row where mx = 10.0

  Scenario: MMAX via ASSUME reflects hypothetical new reading
    Given having executed:
      """
      CREATE (:Sensor {name: 'S1'}), (:Reading {ts: 1}), (:Reading {ts: 2}), (:Reading {ts: 3})
      """
    And having executed:
      """
      MATCH (r:Reading {ts: 1}), (s:Sensor {name: 'S1'})
      CREATE (r)-[:FROM {value: 3}]->(s)
      """
    And having executed:
      """
      MATCH (r:Reading {ts: 2}), (s:Sensor {name: 'S1'})
      CREATE (r)-[:FROM {value: 7}]->(s)
      """
    When evaluating the following Locy program:
      """
      CREATE RULE peak AS
        MATCH (r:Reading)-[f:FROM]->(s:Sensor)
        FOLD mx = MMAX(f.value)
        YIELD KEY s, mx
      ASSUME { MATCH (r:Reading {ts: 3}), (s:Sensor {name: 'S1'}) CREATE (r)-[:FROM {value: 15}]->(s) }
      THEN { QUERY peak WHERE s.name = 'S1' RETURN mx }
      """
    Then evaluation should succeed
    And the derived relation 'peak' should contain a fact where mx = 7.0
    And the command result 0 should be an Assume containing row where mx = 15.0

  Scenario: MMAX via ABDUCE finds modification candidate
    Given having executed:
      """
      CREATE (:Sensor {name: 'S1'}), (:Reading {ts: 1}), (:Reading {ts: 2})
      """
    And having executed:
      """
      MATCH (r:Reading {ts: 1}), (s:Sensor {name: 'S1'})
      CREATE (r)-[:FROM {value: 3}]->(s)
      """
    And having executed:
      """
      MATCH (r:Reading {ts: 2}), (s:Sensor {name: 'S1'})
      CREATE (r)-[:FROM {value: 7}]->(s)
      """
    When evaluating the following Locy program:
      """
      CREATE RULE peak AS
        MATCH (r:Reading)-[f:FROM]->(s:Sensor)
        FOLD mx = MMAX(f.value)
        YIELD KEY s, mx
      ABDUCE NOT peak WHERE s.name = 'S1'
      """
    Then evaluation should succeed
    And the derived relation 'peak' should contain a fact where mx = 7.0
    And the command result 0 should be an Abduce with at least 1 modifications

  # ═══════════════════════════════════════════════════════════════════════════
  # MMIN — min: min(5, 3, 7) = 3
  # ═══════════════════════════════════════════════════════════════════════════

  Scenario: MMIN derived relation computes correct minimum
    Given having executed:
      """
      CREATE (:Server {name: 'Prod'}), (:Metric {id: 1}), (:Metric {id: 2}), (:Metric {id: 3})
      """
    And having executed:
      """
      MATCH (m:Metric {id: 1}), (s:Server {name: 'Prod'})
      CREATE (m)-[:MEASURED {latency: 5}]->(s)
      """
    And having executed:
      """
      MATCH (m:Metric {id: 2}), (s:Server {name: 'Prod'})
      CREATE (m)-[:MEASURED {latency: 3}]->(s)
      """
    And having executed:
      """
      MATCH (m:Metric {id: 3}), (s:Server {name: 'Prod'})
      CREATE (m)-[:MEASURED {latency: 7}]->(s)
      """
    When evaluating the following Locy program:
      """
      CREATE RULE fastest AS
        MATCH (m:Metric)-[r:MEASURED]->(s:Server)
        FOLD mn = MMIN(r.latency)
        YIELD KEY s, mn
      """
    Then evaluation should succeed
    And the derived relation 'fastest' should have 1 facts
    And the derived relation 'fastest' should contain a fact where mn = 3.0

  Scenario: MMIN via QUERY returns correct value
    Given having executed:
      """
      CREATE (:Server {name: 'Prod'}), (:Metric {id: 1}), (:Metric {id: 2}), (:Metric {id: 3})
      """
    And having executed:
      """
      MATCH (m:Metric {id: 1}), (s:Server {name: 'Prod'})
      CREATE (m)-[:MEASURED {latency: 5}]->(s)
      """
    And having executed:
      """
      MATCH (m:Metric {id: 2}), (s:Server {name: 'Prod'})
      CREATE (m)-[:MEASURED {latency: 3}]->(s)
      """
    And having executed:
      """
      MATCH (m:Metric {id: 3}), (s:Server {name: 'Prod'})
      CREATE (m)-[:MEASURED {latency: 7}]->(s)
      """
    When evaluating the following Locy program:
      """
      CREATE RULE fastest AS
        MATCH (m:Metric)-[r:MEASURED]->(s:Server)
        FOLD mn = MMIN(r.latency)
        YIELD KEY s, mn
      QUERY fastest WHERE s = s RETURN s.name AS server, mn
      """
    Then evaluation should succeed
    And the derived relation 'fastest' should have 1 facts
    And the derived relation 'fastest' should contain a fact where mn = 3.0
    And the command result 0 should be a Query with 1 rows
    And the command result 0 should be a Query containing row where mn = 3.0

  Scenario: MMIN via ASSUME reflects hypothetical faster measurement
    Given having executed:
      """
      CREATE (:Server {name: 'Prod'}), (:Metric {id: 1}), (:Metric {id: 2}), (:Metric {id: 3})
      """
    And having executed:
      """
      MATCH (m:Metric {id: 1}), (s:Server {name: 'Prod'})
      CREATE (m)-[:MEASURED {latency: 5}]->(s)
      """
    And having executed:
      """
      MATCH (m:Metric {id: 2}), (s:Server {name: 'Prod'})
      CREATE (m)-[:MEASURED {latency: 3}]->(s)
      """
    When evaluating the following Locy program:
      """
      CREATE RULE fastest AS
        MATCH (m:Metric)-[r:MEASURED]->(s:Server)
        FOLD mn = MMIN(r.latency)
        YIELD KEY s, mn
      ASSUME { MATCH (m:Metric {id: 3}), (s:Server {name: 'Prod'}) CREATE (m)-[:MEASURED {latency: 1}]->(s) }
      THEN { QUERY fastest WHERE s.name = 'Prod' RETURN mn }
      """
    Then evaluation should succeed
    And the derived relation 'fastest' should contain a fact where mn = 3.0
    And the command result 0 should be an Assume containing row where mn = 1.0

  Scenario: MMIN via ABDUCE finds modification candidate
    Given having executed:
      """
      CREATE (:Server {name: 'Prod'}), (:Metric {id: 1}), (:Metric {id: 2})
      """
    And having executed:
      """
      MATCH (m:Metric {id: 1}), (s:Server {name: 'Prod'})
      CREATE (m)-[:MEASURED {latency: 5}]->(s)
      """
    And having executed:
      """
      MATCH (m:Metric {id: 2}), (s:Server {name: 'Prod'})
      CREATE (m)-[:MEASURED {latency: 3}]->(s)
      """
    When evaluating the following Locy program:
      """
      CREATE RULE fastest AS
        MATCH (m:Metric)-[r:MEASURED]->(s:Server)
        FOLD mn = MMIN(r.latency)
        YIELD KEY s, mn
      ABDUCE NOT fastest WHERE s.name = 'Prod'
      """
    Then evaluation should succeed
    And the derived relation 'fastest' should contain a fact where mn = 3.0
    And the command result 0 should be an Abduce with at least 1 modifications

  # ═══════════════════════════════════════════════════════════════════════════
  # MCOUNT — count: 3 edges per group
  # ═══════════════════════════════════════════════════════════════════════════

  Scenario: MCOUNT derived relation computes correct count
    Given having executed:
      """
      CREATE (:Team {name: 'Alpha'}), (:Member {name: 'Alice'}), (:Member {name: 'Bob'}), (:Member {name: 'Carol'})
      """
    And having executed:
      """
      MATCH (m:Member {name: 'Alice'}), (t:Team {name: 'Alpha'})
      CREATE (m)-[:BELONGS_TO]->(t)
      """
    And having executed:
      """
      MATCH (m:Member {name: 'Bob'}), (t:Team {name: 'Alpha'})
      CREATE (m)-[:BELONGS_TO]->(t)
      """
    And having executed:
      """
      MATCH (m:Member {name: 'Carol'}), (t:Team {name: 'Alpha'})
      CREATE (m)-[:BELONGS_TO]->(t)
      """
    When evaluating the following Locy program:
      """
      CREATE RULE team_size AS
        MATCH (m:Member)-[:BELONGS_TO]->(t:Team)
        FOLD cnt = MCOUNT()
        YIELD KEY t, cnt
      """
    Then evaluation should succeed
    And the derived relation 'team_size' should have 1 facts
    And the derived relation 'team_size' should contain a fact where cnt = 3.0

  Scenario: MCOUNT(*) counts rows as MCOUNT() does
    Given having executed:
      """
      CREATE (:Team {name: 'Alpha'}), (:Member {name: 'Alice'}), (:Member {name: 'Bob'}), (:Member {name: 'Carol'})
      """
    And having executed:
      """
      MATCH (m:Member), (t:Team {name: 'Alpha'})
      CREATE (m)-[:BELONGS_TO]->(t)
      """
    When evaluating the following Locy program:
      """
      CREATE RULE team_size AS
        MATCH (m:Member)-[:BELONGS_TO]->(t:Team)
        FOLD cnt = MCOUNT(*)
        YIELD KEY t, cnt
      """
    Then evaluation should succeed
    And the derived relation 'team_size' should have 1 facts
    And the derived relation 'team_size' should contain a fact where cnt = 3.0

  Scenario: COUNT(*) counts rows as MCOUNT() does
    Given having executed:
      """
      CREATE (:Team {name: 'Alpha'}), (:Member {name: 'Alice'}), (:Member {name: 'Bob'}), (:Member {name: 'Carol'})
      """
    And having executed:
      """
      MATCH (m:Member), (t:Team {name: 'Alpha'})
      CREATE (m)-[:BELONGS_TO]->(t)
      """
    When evaluating the following Locy program:
      """
      CREATE RULE team_size AS
        MATCH (m:Member)-[:BELONGS_TO]->(t:Team)
        FOLD cnt = COUNT(*)
        YIELD KEY t, cnt
      """
    Then evaluation should succeed
    And the derived relation 'team_size' should have 1 facts
    And the derived relation 'team_size' should contain a fact where cnt = 3.0

  Scenario: a row count beside another fold survives EXPLAIN
    Given having executed:
      """
      CREATE (:Team {name: 'Alpha'}), (:Member {name: 'Alice'}), (:Member {name: 'Bob'}), (:Member {name: 'Carol'})
      """
    And having executed:
      """
      MATCH (m:Member), (t:Team {name: 'Alpha'})
      CREATE (m)-[:BELONGS_TO]->(t)
      """
    When evaluating the following Locy program:
      """
      CREATE RULE team_size AS
        MATCH (m:Member)-[:BELONGS_TO]->(t:Team)
        FOLD cnt = COUNT(*), names = COLLECT(m.name)
        YIELD KEY t, cnt, names
      EXPLAIN RULE team_size WHERE t.name = 'Alpha'
      QUERY team_size RETURN t.name AS team, cnt
      """
    Then evaluation should succeed
    And the command result 1 should be a Query containing row where cnt = 3

  Scenario: MCOUNT via QUERY returns correct value
    Given having executed:
      """
      CREATE (:Team {name: 'Alpha'}), (:Member {name: 'Alice'}), (:Member {name: 'Bob'}), (:Member {name: 'Carol'})
      """
    And having executed:
      """
      MATCH (m:Member {name: 'Alice'}), (t:Team {name: 'Alpha'})
      CREATE (m)-[:BELONGS_TO]->(t)
      """
    And having executed:
      """
      MATCH (m:Member {name: 'Bob'}), (t:Team {name: 'Alpha'})
      CREATE (m)-[:BELONGS_TO]->(t)
      """
    And having executed:
      """
      MATCH (m:Member {name: 'Carol'}), (t:Team {name: 'Alpha'})
      CREATE (m)-[:BELONGS_TO]->(t)
      """
    When evaluating the following Locy program:
      """
      CREATE RULE team_size AS
        MATCH (m:Member)-[:BELONGS_TO]->(t:Team)
        FOLD cnt = MCOUNT()
        YIELD KEY t, cnt
      QUERY team_size WHERE t = t RETURN t.name AS team, cnt
      """
    Then evaluation should succeed
    And the derived relation 'team_size' should have 1 facts
    And the command result 0 should be a Query with 1 rows
    And the command result 0 should be a Query containing row where cnt = 3.0

  Scenario: MCOUNT via ASSUME reflects hypothetical member addition
    Given having executed:
      """
      CREATE (:Team {name: 'Alpha'}), (:Member {name: 'Alice'}), (:Member {name: 'Bob'}), (:Member {name: 'Carol'})
      """
    And having executed:
      """
      MATCH (m:Member {name: 'Alice'}), (t:Team {name: 'Alpha'})
      CREATE (m)-[:BELONGS_TO]->(t)
      """
    And having executed:
      """
      MATCH (m:Member {name: 'Bob'}), (t:Team {name: 'Alpha'})
      CREATE (m)-[:BELONGS_TO]->(t)
      """
    When evaluating the following Locy program:
      """
      CREATE RULE team_size AS
        MATCH (m:Member)-[:BELONGS_TO]->(t:Team)
        FOLD cnt = MCOUNT()
        YIELD KEY t, cnt
      ASSUME { MATCH (m:Member {name: 'Carol'}), (t:Team {name: 'Alpha'}) CREATE (m)-[:BELONGS_TO]->(t) }
      THEN { QUERY team_size WHERE t.name = 'Alpha' RETURN cnt }
      """
    Then evaluation should succeed
    And the derived relation 'team_size' should contain a fact where cnt = 2.0
    And the command result 0 should be an Assume containing row where cnt = 3.0

  # ═══════════════════════════════════════════════════════════════════════════
  # Cross-operator: multi-group and multi-operator combinations
  # ═══════════════════════════════════════════════════════════════════════════

  Scenario: MSUM groups independently across two departments
    Given having executed:
      """
      CREATE (:Dept {name: 'Eng'}), (:Dept {name: 'Sales'}), (:Person {name: 'Alice'}), (:Person {name: 'Bob'}), (:Person {name: 'Carol'})
      """
    And having executed:
      """
      MATCH (p:Person {name: 'Alice'}), (d:Dept {name: 'Eng'})
      CREATE (p)-[:WORKS_IN {hours: 10}]->(d)
      """
    And having executed:
      """
      MATCH (p:Person {name: 'Bob'}), (d:Dept {name: 'Eng'})
      CREATE (p)-[:WORKS_IN {hours: 20}]->(d)
      """
    And having executed:
      """
      MATCH (p:Person {name: 'Carol'}), (d:Dept {name: 'Sales'})
      CREATE (p)-[:WORKS_IN {hours: 40}]->(d)
      """
    When evaluating the following Locy program:
      """
      CREATE RULE dept_hours AS
        MATCH (p:Person)-[w:WORKS_IN]->(d:Dept)
        FOLD total = MSUM(w.hours)
        YIELD KEY d, total
      QUERY dept_hours WHERE d = d RETURN d.name AS dept, total
      """
    Then evaluation should succeed
    And the derived relation 'dept_hours' should have 2 facts
    And the derived relation 'dept_hours' should contain a fact where total = 30.0
    And the derived relation 'dept_hours' should contain a fact where total = 40.0
    And the command result 0 should be a Query with 2 rows

  Scenario: MMAX groups independently across two sensors
    Given having executed:
      """
      CREATE (:Sensor {name: 'S1'}), (:Sensor {name: 'S2'}), (:Reading {ts: 1}), (:Reading {ts: 2}), (:Reading {ts: 3})
      """
    And having executed:
      """
      MATCH (r:Reading {ts: 1}), (s:Sensor {name: 'S1'})
      CREATE (r)-[:FROM {value: 3}]->(s)
      """
    And having executed:
      """
      MATCH (r:Reading {ts: 2}), (s:Sensor {name: 'S1'})
      CREATE (r)-[:FROM {value: 7}]->(s)
      """
    And having executed:
      """
      MATCH (r:Reading {ts: 3}), (s:Sensor {name: 'S2'})
      CREATE (r)-[:FROM {value: 12}]->(s)
      """
    When evaluating the following Locy program:
      """
      CREATE RULE peak AS
        MATCH (r:Reading)-[f:FROM]->(s:Sensor)
        FOLD mx = MMAX(f.value)
        YIELD KEY s, mx
      QUERY peak WHERE s = s RETURN s.name AS sensor, mx
      """
    Then evaluation should succeed
    And the derived relation 'peak' should have 2 facts
    And the derived relation 'peak' should contain a fact where mx = 7.0
    And the derived relation 'peak' should contain a fact where mx = 12.0
    And the command result 0 should be a Query with 2 rows

  Scenario: MMIN groups independently across two servers
    Given having executed:
      """
      CREATE (:Server {name: 'Prod'}), (:Server {name: 'Dev'}), (:Metric {id: 1}), (:Metric {id: 2}), (:Metric {id: 3})
      """
    And having executed:
      """
      MATCH (m:Metric {id: 1}), (s:Server {name: 'Prod'})
      CREATE (m)-[:MEASURED {latency: 5}]->(s)
      """
    And having executed:
      """
      MATCH (m:Metric {id: 2}), (s:Server {name: 'Prod'})
      CREATE (m)-[:MEASURED {latency: 3}]->(s)
      """
    And having executed:
      """
      MATCH (m:Metric {id: 3}), (s:Server {name: 'Dev'})
      CREATE (m)-[:MEASURED {latency: 8}]->(s)
      """
    When evaluating the following Locy program:
      """
      CREATE RULE fastest AS
        MATCH (m:Metric)-[r:MEASURED]->(s:Server)
        FOLD mn = MMIN(r.latency)
        YIELD KEY s, mn
      QUERY fastest WHERE s = s RETURN s.name AS server, mn
      """
    Then evaluation should succeed
    And the derived relation 'fastest' should have 2 facts
    And the derived relation 'fastest' should contain a fact where mn = 3.0
    And the derived relation 'fastest' should contain a fact where mn = 8.0
    And the command result 0 should be a Query with 2 rows

  Scenario: MCOUNT groups independently across two teams
    Given having executed:
      """
      CREATE (:Team {name: 'Alpha'}), (:Team {name: 'Beta'}), (:Member {name: 'Alice'}), (:Member {name: 'Bob'}), (:Member {name: 'Carol'}), (:Member {name: 'Dave'})
      """
    And having executed:
      """
      MATCH (m:Member {name: 'Alice'}), (t:Team {name: 'Alpha'})
      CREATE (m)-[:BELONGS_TO]->(t)
      """
    And having executed:
      """
      MATCH (m:Member {name: 'Bob'}), (t:Team {name: 'Alpha'})
      CREATE (m)-[:BELONGS_TO]->(t)
      """
    And having executed:
      """
      MATCH (m:Member {name: 'Carol'}), (t:Team {name: 'Alpha'})
      CREATE (m)-[:BELONGS_TO]->(t)
      """
    And having executed:
      """
      MATCH (m:Member {name: 'Dave'}), (t:Team {name: 'Beta'})
      CREATE (m)-[:BELONGS_TO]->(t)
      """
    When evaluating the following Locy program:
      """
      CREATE RULE team_size AS
        MATCH (m:Member)-[:BELONGS_TO]->(t:Team)
        FOLD cnt = MCOUNT()
        YIELD KEY t, cnt
      QUERY team_size WHERE t = t RETURN t.name AS team, cnt
      """
    Then evaluation should succeed
    And the derived relation 'team_size' should have 2 facts
    And the derived relation 'team_size' should contain a fact where cnt = 3.0
    And the derived relation 'team_size' should contain a fact where cnt = 1.0
    And the command result 0 should be a Query with 2 rows

  Scenario: MNOR and MPROD coexist as separate rules
    Given having executed:
      """
      CREATE (:Node {name: 'A'}), (:Node {name: 'B'})
      """
    And having executed:
      """
      MATCH (a:Node {name: 'A'}), (b:Node {name: 'B'})
      CREATE (a)-[:CAUSE {prob: 0.3}]->(b)
      """
    And having executed:
      """
      MATCH (a:Node {name: 'A'}), (b:Node {name: 'B'})
      CREATE (a)-[:CAUSE {prob: 0.5}]->(b)
      """
    And having executed:
      """
      MATCH (a:Node {name: 'A'}), (b:Node {name: 'B'})
      CREATE (a)-[:CHECK {conf: 0.8}]->(b)
      """
    And having executed:
      """
      MATCH (a:Node {name: 'A'}), (b:Node {name: 'B'})
      CREATE (a)-[:CHECK {conf: 0.9}]->(b)
      """
    When evaluating the following Locy program:
      """
      CREATE RULE risk AS
        MATCH (a:Node)-[e:CAUSE]->(b:Node)
        FOLD p = MNOR(e.prob)
        YIELD KEY a, KEY b, p

      CREATE RULE coverage AS
        MATCH (a:Node)-[e:CHECK]->(b:Node)
        FOLD p = MPROD(e.conf)
        YIELD KEY a, KEY b, p
      """
    Then evaluation should succeed
    And the derived relation 'risk' should have 1 facts
    And the derived relation 'risk' should contain a fact where p = 0.65
    And the derived relation 'coverage' should have 1 facts
    And the derived relation 'coverage' should contain a fact where p = 0.72

  Scenario: MSUM and MCOUNT on same graph with different rules
    Given having executed:
      """
      CREATE (:Dept {name: 'Eng'}), (:Person {name: 'Alice'}), (:Person {name: 'Bob'}), (:Person {name: 'Carol'})
      """
    And having executed:
      """
      MATCH (p:Person {name: 'Alice'}), (d:Dept {name: 'Eng'})
      CREATE (p)-[:WORKS_IN {hours: 10}]->(d)
      """
    And having executed:
      """
      MATCH (p:Person {name: 'Bob'}), (d:Dept {name: 'Eng'})
      CREATE (p)-[:WORKS_IN {hours: 20}]->(d)
      """
    And having executed:
      """
      MATCH (p:Person {name: 'Carol'}), (d:Dept {name: 'Eng'})
      CREATE (p)-[:WORKS_IN {hours: 30}]->(d)
      """
    When evaluating the following Locy program:
      """
      CREATE RULE dept_hours AS
        MATCH (p:Person)-[w:WORKS_IN]->(d:Dept)
        FOLD total = MSUM(w.hours)
        YIELD KEY d, total

      CREATE RULE dept_headcount AS
        MATCH (p:Person)-[:WORKS_IN]->(d:Dept)
        FOLD cnt = MCOUNT()
        YIELD KEY d, cnt
      """
    Then evaluation should succeed
    And the derived relation 'dept_hours' should have 1 facts
    And the derived relation 'dept_hours' should contain a fact where total = 60.0
    And the derived relation 'dept_headcount' should have 1 facts
    And the derived relation 'dept_headcount' should contain a fact where cnt = 3.0

  Scenario: MMAX and MMIN on same graph yield complementary extremes
    Given having executed:
      """
      CREATE (:Sensor {name: 'S1'}), (:Reading {ts: 1}), (:Reading {ts: 2}), (:Reading {ts: 3})
      """
    And having executed:
      """
      MATCH (r:Reading {ts: 1}), (s:Sensor {name: 'S1'})
      CREATE (r)-[:FROM {value: 5}]->(s)
      """
    And having executed:
      """
      MATCH (r:Reading {ts: 2}), (s:Sensor {name: 'S1'})
      CREATE (r)-[:FROM {value: 3}]->(s)
      """
    And having executed:
      """
      MATCH (r:Reading {ts: 3}), (s:Sensor {name: 'S1'})
      CREATE (r)-[:FROM {value: 7}]->(s)
      """
    When evaluating the following Locy program:
      """
      CREATE RULE peak AS
        MATCH (r:Reading)-[f:FROM]->(s:Sensor)
        FOLD mx = MMAX(f.value)
        YIELD KEY s, mx

      CREATE RULE trough AS
        MATCH (r:Reading)-[f:FROM]->(s:Sensor)
        FOLD mn = MMIN(f.value)
        YIELD KEY s, mn
      """
    Then evaluation should succeed
    And the derived relation 'peak' should have 1 facts
    And the derived relation 'peak' should contain a fact where mx = 7.0
    And the derived relation 'trough' should have 1 facts
    And the derived relation 'trough' should contain a fact where mn = 3.0

  # ═══════════════════════════════════════════════════════════════════════════
  # Re-added scenarios (previously removed, now fixed)
  # ═══════════════════════════════════════════════════════════════════════════

  Scenario: MCOUNT via ABDUCE finds modification candidate
    Given having executed:
      """
      CREATE (:Team {name: 'Eng'}), (:Person {name: 'Alice'}), (:Person {name: 'Bob'})
      """
    And having executed:
      """
      MATCH (t:Team {name: 'Eng'}), (p:Person {name: 'Alice'})
      CREATE (p)-[:MEMBER_OF]->(t)
      """
    And having executed:
      """
      MATCH (t:Team {name: 'Eng'}), (p:Person {name: 'Bob'})
      CREATE (p)-[:MEMBER_OF]->(t)
      """
    When evaluating the following Locy program:
      """
      CREATE RULE team_size AS
        MATCH (p:Person)-[:MEMBER_OF]->(t:Team)
        FOLD cnt = MCOUNT()
        YIELD KEY t, cnt

      ABDUCE NOT team_size WHERE t.name = 'Eng'
      """
    Then evaluation should succeed
    And the command result 0 should be an Abduce with at least 1 modifications

  Scenario: Two independent FOLD rules produce correct values in same program
    Given having executed:
      """
      CREATE (:Node {name: 'A'}), (:Node {name: 'B'})
      """
    And having executed:
      """
      MATCH (a:Node {name: 'A'}), (b:Node {name: 'B'})
      CREATE (a)-[:CAUSE {prob: 0.3}]->(b)
      """
    And having executed:
      """
      MATCH (a:Node {name: 'A'}), (b:Node {name: 'B'})
      CREATE (a)-[:CAUSE {prob: 0.5}]->(b)
      """
    And having executed:
      """
      MATCH (a:Node {name: 'A'}), (b:Node {name: 'B'})
      CREATE (a)-[:CHECK {amt: 10}]->(b)
      """
    And having executed:
      """
      MATCH (a:Node {name: 'A'}), (b:Node {name: 'B'})
      CREATE (a)-[:CHECK {amt: 20}]->(b)
      """
    When evaluating the following Locy program:
      """
      CREATE RULE risk AS
        MATCH (a:Node)-[e:CAUSE]->(b:Node)
        FOLD p = MNOR(e.prob)
        YIELD KEY a, KEY b, p

      CREATE RULE total AS
        MATCH (a:Node)-[e:CHECK]->(b:Node)
        FOLD s = MSUM(e.amt)
        YIELD KEY a, KEY b, s
      """
    Then evaluation should succeed
    And the derived relation 'risk' should have 1 facts
    And the derived relation 'risk' should contain a fact where p = 0.65
    And the derived relation 'total' should have 1 facts
    And the derived relation 'total' should contain a fact where s = 30.0

