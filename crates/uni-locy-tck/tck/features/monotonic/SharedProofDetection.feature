Feature: Shared-Proof Detection
  Detect when MNOR/MPROD proofs share an event (an edge, a referenced fact),
  violating the independence assumption. In default mode results are still
  computed with independence and a RuntimeWarning is emitted; with
  exact_probability the shared groups are recomputed.

  Background:
    Given an empty graph

  # ── Positive: two proofs sharing an edge trigger the warning ────────────
  # Both proofs of reach(A,D) need the edge C->D:
  #   A -0.5-> B -0.4-> C -0.6-> D  and  A -0.3-> C -0.6-> D.
  # MNOR treats the two proofs as independent, which they are not.
  # (This scenario used the Source/Mid/Target diamond, whose proofs S->T and
  # S->M->T share a node but no edge — no event — so they are independent;
  # see the negative scenario below.)

  Scenario: Diamond graph triggers SharedProbabilisticDependency warning
    Given having executed:
      """
      CREATE (a:Node {name: 'A'}),
             (b:Node {name: 'B'}),
             (c:Node {name: 'C'}),
             (d:Node {name: 'D'}),
             (a)-[:LINK {p: 0.5}]->(b),
             (a)-[:LINK {p: 0.3}]->(c),
             (b)-[:LINK {p: 0.4}]->(c),
             (c)-[:LINK {p: 0.6}]->(d)
      """
    When evaluating the following Locy program:
      """
      CREATE RULE reach AS
        MATCH (a:Node)-[e:LINK]->(b:Node)
        FOLD prob = MNOR(e.p)
        YIELD KEY a, KEY b, prob
      CREATE RULE reach AS
        MATCH (a:Node)-[e:LINK]->(mid:Node) WHERE mid IS reach TO b
        FOLD prob = MNOR(e.p)
        YIELD KEY a, KEY b, prob
      """
    Then evaluation should succeed
    And the result should contain a SharedProbabilisticDependency warning for rule 'reach'

  # ── Negative: edge-disjoint proofs — no warning ───────────────────────────
  #   Source --0.3--> Mid --0.5--> Target
  #   Source --0.7--> Target
  # The proofs of reach(Source, Target) rest on {S->T} and {S->M, M->T}: no
  # edge in common, so independence is exact (0.745) and nothing is reported.

  Scenario: Edge-disjoint diamond produces no warning
    Given having executed:
      """
      CREATE (s:Node {name: 'Source'}),
             (m:Node {name: 'Mid'}),
             (t:Node {name: 'Target'}),
             (s)-[:LINK {p: 0.3}]->(m),
             (m)-[:LINK {p: 0.5}]->(t),
             (s)-[:LINK {p: 0.7}]->(t)
      """
    When evaluating the following Locy program:
      """
      CREATE RULE reach AS
        MATCH (a:Node)-[e:LINK]->(b:Node)
        FOLD prob = MNOR(e.p)
        YIELD KEY a, KEY b, prob
      CREATE RULE reach AS
        MATCH (a:Node)-[e:LINK]->(mid:Node) WHERE mid IS reach TO b
        FOLD prob = MNOR(e.p)
        YIELD KEY a, KEY b, prob
      """
    Then evaluation should succeed
    And the derived relation 'reach' should contain a fact where a.name = 'Source' and b.name = 'Target' and prob = 0.745
    And the result should not contain a SharedProbabilisticDependency warning

  # ── Negative: independent parallel paths — no warning ─────────────────────
  # Two direct edges with no shared intermediate.
  #   A --0.4--> B
  #   A --0.6--> B

  Scenario: Independent parallel paths produce no warning
    Given having executed:
      """
      CREATE (a:Node {name: 'A'}),
             (b:Node {name: 'B'}),
             (a)-[:LINK {p: 0.4}]->(b),
             (a)-[:LINK {p: 0.6}]->(b)
      """
    When evaluating the following Locy program:
      """
      CREATE RULE risk AS
        MATCH (a:Node)-[e:LINK]->(b:Node)
        FOLD prob = MNOR(e.p)
        YIELD KEY a, KEY b, prob
      """
    Then evaluation should succeed
    And the result should not contain a SharedProbabilisticDependency warning

  # ── Negative: non-probabilistic FOLD — no warning ─────────────────────────
  # MSUM rule should not trigger shared-proof detection.

  Scenario: Non-probabilistic FOLD produces no warning
    Given having executed:
      """
      CREATE (a:Node {name: 'A'}),
             (b:Node {name: 'B'}),
             (a)-[:LINK {w: 3}]->(b),
             (a)-[:LINK {w: 5}]->(b)
      """
    When evaluating the following Locy program:
      """
      CREATE RULE total AS
        MATCH (a:Node)-[e:LINK]->(b:Node)
        FOLD s = MSUM(e.w)
        YIELD KEY a, KEY b, s
      """
    Then evaluation should succeed
    And the result should not contain a SharedProbabilisticDependency warning

  # ── Positive: non-recursive cross-stratum shared proofs trigger warning ──
  # Two strata, both non-recursive:
  #   direct   (stratum 1): base edges
  #   combined (stratum 2): one hop, then IS direct
  # Two parallel edges Source->Mid (0.3, 0.4) each extend direct(Mid,Target)
  # (0.5): both proofs of combined(Source,Target) need Mid->Target.
  # Independence: 1 - (1 - 0.15)(1 - 0.2) = 0.32.
  # Possible worlds: (0.3 OR 0.4) AND 0.5 = 0.58 * 0.5 = 0.29.
  # (This scenario used the edge-disjoint diamond, which shares nothing.)

  Scenario: Non-recursive cross-stratum shared proofs trigger warning
    Given having executed:
      """
      CREATE (s:Node {name: 'Source'}),
             (m:Node {name: 'Mid'}),
             (t:Node {name: 'Target'}),
             (s)-[:LINK {p: 0.3}]->(m),
             (s)-[:LINK {p: 0.4}]->(m),
             (m)-[:LINK {p: 0.5}]->(t)
      """
    When evaluating the following Locy program:
      """
      CREATE RULE direct AS
        MATCH (a:Node)-[e:LINK]->(b:Node)
        FOLD prob = MNOR(e.p)
        YIELD KEY a, KEY b, prob

      CREATE RULE combined AS
        MATCH (a:Node)-[e:LINK]->(mid:Node) WHERE mid IS direct TO b
        FOLD p = MNOR(e.p)
        YIELD KEY a, KEY b, p
      """
    Then evaluation should succeed
    And the derived relation 'combined' should contain a fact where a.name = 'Source' and b.name = 'Target' and p = 0.32
    And the result should contain a SharedProbabilisticDependency warning for rule 'combined'

  Scenario: Non-recursive cross-stratum shared proofs are exact with exact_probability
    Given having executed:
      """
      CREATE (s:Node {name: 'Source'}),
             (m:Node {name: 'Mid'}),
             (t:Node {name: 'Target'}),
             (s)-[:LINK {p: 0.3}]->(m),
             (s)-[:LINK {p: 0.4}]->(m),
             (m)-[:LINK {p: 0.5}]->(t)
      """
    When evaluating the following Locy program with exact_probability:
      """
      CREATE RULE direct AS
        MATCH (a:Node)-[e:LINK]->(b:Node)
        FOLD prob = MNOR(e.p)
        YIELD KEY a, KEY b, prob

      CREATE RULE combined AS
        MATCH (a:Node)-[e:LINK]->(mid:Node) WHERE mid IS direct TO b
        FOLD p = MNOR(e.p)
        YIELD KEY a, KEY b, p
      """
    Then evaluation should succeed
    And the derived relation 'combined' should contain a fact where a.name = 'Source' and b.name = 'Target' and p = 0.29
    And the result should contain a SharedProbabilisticDependency warning for rule 'combined'
