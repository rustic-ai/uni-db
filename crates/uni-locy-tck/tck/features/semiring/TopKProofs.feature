Feature: TopKProofs semiring

  `SemiringKind::TopKProofs { k }` evaluates each fact of an MNOR/MPROD
  rule over its proofs, as exact mode does: each derivation row is its own
  event AND every fact it references (AND NOT every fact it negates),
  evaluated under possible-worlds semantics, with each MNOR fact restricted
  to its k most probable rows. With k at least the number of a fact's rows
  the value is the possible-worlds probability; without shared events it is
  the default (independence) value. `TopKPruningCrossedDependency` is raised
  when proof retention (`top_k_proofs`) drops a proof that shares a base fact
  with a retained one.

  Background:
    Given an empty graph

  # ── TopKProofs(4) accepted; falls back to AddMultProb byte-identically ──

  Scenario: MNOR under TopKProofs(4) Stage 1 matches AddMultProb output
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
      CREATE (a)-[:CAUSE {prob: 0.7}]->(b)
      """
    When evaluating the following Locy program with semiring "TopKProofs(4)":
      """
      CREATE RULE risk AS
        MATCH (a:Node)-[e:CAUSE]->(b:Node)
        FOLD p = MNOR(e.prob)
        YIELD KEY a, KEY b, p
      """
    Then evaluation should succeed
    # 1 - (0.7)(0.5)(0.3) = 0.895 — Stage 1 row math = AddMultProb.
    And the derived relation 'risk' should contain a fact where p = 0.895
    # No Fuzzy warning (TopKProofs is not a fuzzy semiring).
    And the result should not contain a FuzzyNotProbabilistic warning

  # ── MPROD under TopKProofs(4): same fallback ──────────────────────────

  Scenario: MPROD under TopKProofs(4) matches AddMultProb product
    Given having executed:
      """
      CREATE (:Node {name: 'A'}), (:Node {name: 'B'})
      """
    And having executed:
      """
      MATCH (a:Node {name: 'A'}), (b:Node {name: 'B'})
      CREATE (a)-[:CAUSE {prob: 0.5}]->(b)
      """
    And having executed:
      """
      MATCH (a:Node {name: 'A'}), (b:Node {name: 'B'})
      CREATE (a)-[:CAUSE {prob: 0.4}]->(b)
      """
    When evaluating the following Locy program with semiring "TopKProofs(4)":
      """
      CREATE RULE joint AS
        MATCH (a:Node)-[e:CAUSE]->(b:Node)
        FOLD p = MPROD(e.prob)
        YIELD KEY a, KEY b, p
      """
    Then evaluation should succeed
    And the derived relation 'joint' should contain a fact where p = 0.2

  # ── Incoherent: TopKProofs(0) rejected at config-resolve time ─────────

  # ── Stage 2: tag-flow does not regress non-sharing programs ──────────

  Scenario: TopKProofs(2) on independent proofs emits no CrossedDependency warning
    Given having executed:
      """
      CREATE (:Node {name: 'A'}), (:Node {name: 'B'})
      """
    And having executed:
      """
      MATCH (a:Node {name: 'A'}), (b:Node {name: 'B'})
      CREATE (a)-[:CAUSE {prob: 0.4}]->(b)
      """
    And having executed:
      """
      MATCH (a:Node {name: 'A'}), (b:Node {name: 'B'})
      CREATE (a)-[:CAUSE {prob: 0.6}]->(b)
      """
    When evaluating the following Locy program with semiring "TopKProofs(2)":
      """
      CREATE RULE risk AS
        MATCH (a:Node)-[e:CAUSE]->(b:Node)
        FOLD p = MNOR(e.prob)
        YIELD KEY a, KEY b, p
      """
    Then evaluation should succeed
    # 1 - (0.4)(0.6) = 0.76
    And the derived relation 'risk' should contain a fact where p = 0.76
    And the result should not contain a TopKPruningCrossedDependency warning

  # ── Proofs over a certain referenced fact ─────────────────────────────
  # Two rows of `risk` for KEY h_a, one per CAUSE edge, both referencing
  # the same `hub_score(h_a)` fact. `hub_score` has no PROB column, so the
  # fact is certain: it is no event, and the two rows are the two edges,
  # independent events. Possible worlds: 1 - (1 - 0.7)(1 - 0.6) = 0.88.
  # TopKProofs(8) keeps both proofs and evaluates them exactly.
  # (This scenario used to expect 0.7: the shared referenced fact was
  # treated as the proofs' only event, weighted by the larger row, which
  # dropped the second edge.)

  Scenario: TopKProofs(8) over a certain shared referenced fact keeps the edges independent
    Given having executed:
      """
      CREATE (:Hub {name: 'h_a'}),
             (:Item {name: 'i1'}),
             (:Item {name: 'i2'})
      """
    And having executed:
      """
      MATCH (h:Hub {name: 'h_a'}), (i:Item {name: 'i1'})
      CREATE (h)-[:CAUSE {prob: 0.7}]->(i)
      """
    And having executed:
      """
      MATCH (h:Hub {name: 'h_a'}), (i:Item {name: 'i2'})
      CREATE (h)-[:CAUSE {prob: 0.6}]->(i)
      """
    # hub_score is per-Hub (one fact per Hub), so risk's IS-ref join
    # gives exactly 1 hub_score fact per Hub × N CAUSE edges. The two
    # `risk` pre-fold rows for KEY h_a both reference the SAME
    # `hub_score(h_a)` fact, which, without a PROB column, is certain.
    When evaluating the following Locy program with semiring "TopKProofs(8)":
      """
      CREATE RULE hub_score AS
        MATCH (h:Hub)
        YIELD KEY h

      CREATE RULE risk AS
        MATCH (h:Hub)-[e:CAUSE]->(i:Item)
        WHERE h IS hub_score
        FOLD risk_p = MNOR(e.prob)
        YIELD KEY h, risk_p
      """
    Then evaluation should succeed
    And the derived relation 'risk' should contain a fact where risk_p = 0.88

  # ── Incoherent: TopKProofs(0) rejected at config-resolve time ─────────

  Scenario: TopKProofs(0) rejected as incoherent
    Given having executed:
      """
      CREATE (:Node {name: 'A'})
      """
    When evaluating the following Locy program with semiring "TopKProofs(0)":
      """
      CREATE RULE noop AS
        MATCH (a:Node)
        YIELD KEY a
      """
    Then evaluation should fail
    And the evaluation error should mention "TopKProofs"

  # ── Proofs sharing an edge below a referenced fact ────────────────────
  # A -0.5-> B -0.4-> C -0.6-> D and A -0.3-> C. reach(A, D) has two proofs,
  # A->C then reach(C, D) (0.18) and A->B then reach(B, D) (0.12), and both
  # rest on C->D. TopKProofs evaluates its proofs over their lineage, so
  # with both kept it is the possible-worlds probability
  #   0.6 * (1 - 0.7 * 0.8) = 0.264,
  # not the independent 1 - (1 - 0.18)(1 - 0.12) = 0.2784. With k = 1 only
  # the more probable proof is kept: 0.3 * 0.6 = 0.18.

  Scenario: TopKProofs(8) counts an edge shared below referenced facts once
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
    When evaluating the following Locy program with semiring "TopKProofs(8)":
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
    And the derived relation 'reach' should contain a fact where a.name = 'A' and b.name = 'D' and prob = 0.264

  Scenario: TopKProofs(1) keeps the most probable proof of each fact
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
    When evaluating the following Locy program with semiring "TopKProofs(1)":
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
    And the derived relation 'reach' should contain a fact where a.name = 'A' and b.name = 'D' and prob = 0.18
