Feature: Exact BDD-based Probability Computation
  When exact_probability is enabled, each fact of an MNOR/MPROD rule is
  computed under possible-worlds semantics: OP over its rows of (the row's own
  event AND every positively referenced fact), OP being OR for MNOR and AND for
  MPROD, every row event independent. Proofs that share an event are counted
  once, which the independence evaluation cannot do.

  A positive IS reference to a PROB rule is a probabilistic condition in both
  modes: the referenced fact's probability is multiplied into the row, unless
  the row's PROB expression already reads it as a product factor.

  An IS NOT against a PROB rule multiplies 1 - p into the row in default mode.
  In exact mode the row instead conjoins the negation of the negated facts'
  formulas, which differs when they share events with the rest of the row.

  Background:
    Given an empty graph

  # ── BDD corrects shared proofs ─────────────────────────────────────────
  # Two proofs of reach(A,D) both need the edge C->D:
  #   A -0.5-> B -0.4-> C -0.6-> D  and  A -0.3-> C -0.6-> D.
  # Possible worlds: CD AND (AC OR (AB AND BC)) = 0.6 * (1 - 0.7 * 0.8) = 0.264.
  # Independence multiplies the two proofs as if unrelated:
  #   1 - (1 - 0.5 * 0.24)(1 - 0.3 * 0.6) = 0.2784.
  # (This scenario used the Source/Mid/Target diamond, whose two proofs share
  # no edge; see "BDD mode produces numeric probability for MNOR diamond".)

  Scenario: Shared-proof diamond with exact_probability produces correct result
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
    When evaluating the following Locy program with exact_probability:
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
    # Exact inference resolves the sharing, so the result is no
    # overestimate: reported as a status, not the warning (issue #315).
    And the result should report a SharedDependencyResolved status for rule 'reach'
    And the result should not contain a SharedProbabilisticDependency warning
    And the shared dependency of rule 'reach' should have inference mode 'exact' and resolution 'completed'
    And the result should not contain a BddLimitExceeded warning

  # ── BDD limit exceeded ─────────────────────────────────────────────────
  # Set max_bdd_variables to 1 so the shared group exceeds the limit and falls
  # back. Uses the shared-edge graph: the diamond's proofs share nothing, so
  # it needs no BDD at all.

  Scenario: BDD limit exceeded emits BddLimitExceeded warning
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
    When evaluating the following Locy program with exact_probability and max_bdd_variables 1:
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
    And the result should contain a BddLimitExceeded warning for rule 'reach'
    And the result should contain a SharedProbabilisticDependency warning for rule 'reach'
    And the shared dependency of rule 'reach' should have inference mode 'exact' and resolution 'fallback'
    And the result should not report a SharedDependencyResolved status

  # ── Default mode (exact_probability=false) ─────────────────────────────
  # Only the SharedProbabilisticDependency warning, no BDD (shared-edge graph).

  Scenario: Default mode emits only SharedProbabilisticDependency warning
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
    And the derived relation 'reach' should contain a fact where a.name = 'A' and b.name = 'D' and prob = 0.2784
    And the result should contain a SharedProbabilisticDependency warning for rule 'reach'
    And the shared dependency of rule 'reach' should have inference mode 'approximate' and resolution 'not_attempted'
    And the result should not report a SharedDependencyResolved status
    And the result should not contain a BddLimitExceeded warning

  # ── Independent groups unaffected ──────────────────────────────────────
  # No shared base facts → no warnings at all.

  Scenario: Independent groups produce no warnings with exact_probability
    Given having executed:
      """
      CREATE (a:Node {name: 'A'}),
             (b:Node {name: 'B'}),
             (a)-[:LINK {p: 0.4}]->(b),
             (a)-[:LINK {p: 0.6}]->(b)
      """
    When evaluating the following Locy program with exact_probability:
      """
      CREATE RULE risk AS
        MATCH (a:Node)-[e:LINK]->(b:Node)
        FOLD prob = MNOR(e.p)
        YIELD KEY a, KEY b, prob
      """
    Then evaluation should succeed
    And the result should not contain a SharedProbabilisticDependency warning
    And the result should not contain a BddLimitExceeded warning

  # ── MPROD with shared base facts ───────────────────────────────────────
  # Exact AND over the rows of reach(A,D) on the shared-edge graph: every edge
  # of both proofs, C->D once: 0.5 * 0.4 * 0.3 * 0.6 = 0.036. Independence
  # multiplies C->D twice: (0.5 * 0.4 * 0.6) * (0.3 * 0.6) = 0.0216.

  Scenario: MPROD with shared base facts uses BDD
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
    When evaluating the following Locy program with exact_probability:
      """
      CREATE RULE reach AS
        MATCH (a:Node)-[e:LINK]->(b:Node)
        FOLD prob = MPROD(e.p)
        YIELD KEY a, KEY b, prob
      CREATE RULE reach AS
        MATCH (a:Node)-[e:LINK]->(mid:Node) WHERE mid IS reach TO b
        FOLD prob = MPROD(e.p)
        YIELD KEY a, KEY b, prob
      """
    Then evaluation should succeed
    And the derived relation 'reach' should contain a fact where a.name = 'A' and b.name = 'D' and prob = 0.036
    # Exact inference resolves the sharing, so the result is no
    # overestimate: reported as a status, not the warning (issue #315).
    And the result should report a SharedDependencyResolved status for rule 'reach'
    And the result should not contain a SharedProbabilisticDependency warning
    And the shared dependency of rule 'reach' should have inference mode 'exact' and resolution 'completed'
    And the result should not contain a BddLimitExceeded warning

  # ── Numeric probability assertions: exact_probability mode ─────────────
  # Scenario 2A: the Source/Mid/Target diamond. Its edges are independent
  # events X = S->T (0.7), Y = S->M (0.3), Z = M->T (0.5), and reach(S,T)
  # holds in a world iff X OR (Y AND Z):
  #   P = 1 - (1 - 0.7)(1 - 0.3 * 0.5) = 1 - 0.3 * 0.85 = 0.745.
  # The recursive row holds only when reach(M,T) does, so its probability 0.5
  # is part of the row. (This scenario asserted 0.79 = 1 - 0.3 * 0.7 =
  # P(X OR Y), which treated `mid IS reach TO b` as certain: worlds where Y
  # holds and Z fails still derived reach(S,T).) The two proofs share no edge,
  # so there is no shared-lineage warning.

  Scenario: BDD mode produces numeric probability for MNOR diamond
    Given having executed:
      """
      CREATE (s:Node {name: 'Source'}),
             (m:Node {name: 'Mid'}),
             (t:Node {name: 'Target'}),
             (s)-[:LINK {p: 0.3}]->(m),
             (m)-[:LINK {p: 0.5}]->(t),
             (s)-[:LINK {p: 0.7}]->(t)
      """
    When evaluating the following Locy program with exact_probability:
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
    And the result should not contain a BddLimitExceeded warning

  # ── Scenario 2B: Default mode (independence assumption) ────────────────
  # Same graph, same program, default mode. The proofs share no edge, so the
  # independence evaluation is exact too: 0.745 (it was 0.79, see 2A).

  Scenario: Default mode produces numeric probability for MNOR diamond
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

  # ── Scenario 2C: Independent groups with exact_probability ─────────────
  # Two parallel edges A→B with p=0.4 and p=0.6.
  # MNOR(0.4, 0.6) = 1 - (1-0.4)(1-0.6) = 1 - 0.24 = 0.76.
  # No IS-refs → no shared proofs → no warnings.

  Scenario: Independent groups with exact_probability produce correct probability
    Given having executed:
      """
      CREATE (a:Node {name: 'A'}),
             (b:Node {name: 'B'}),
             (a)-[:LINK {p: 0.4}]->(b),
             (a)-[:LINK {p: 0.6}]->(b)
      """
    When evaluating the following Locy program with exact_probability:
      """
      CREATE RULE risk AS
        MATCH (a:Node)-[e:LINK]->(b:Node)
        FOLD prob = MNOR(e.p)
        YIELD KEY a, KEY b, prob
      """
    Then evaluation should succeed
    And the derived relation 'risk' should contain a fact where a.name = 'A' and prob = 0.76
    And the result should not contain a SharedProbabilisticDependency warning
    And the result should not contain a BddLimitExceeded warning

  # ── Scenario 2D: BDD limit exceeded falls back to independence value ────
  # max_bdd_variables=1 forces fallback on the shared-edge graph. reach(A,D)
  # stays at the independence value 1 - (1 - 0.12)(1 - 0.18) = 0.2784.

  Scenario: BDD limit exceeded retains independence value
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
    When evaluating the following Locy program with exact_probability and max_bdd_variables 1:
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
    And the derived relation 'reach' should contain a fact where a.name = 'A' and b.name = 'D' and prob = 0.2784
    And the result should contain a BddLimitExceeded warning for rule 'reach'
    And the result should contain a SharedProbabilisticDependency warning for rule 'reach'
    And the shared dependency of rule 'reach' should have inference mode 'exact' and resolution 'fallback'
    And the result should not report a SharedDependencyResolved status

  # ── Scenario 2E: MPROD diamond with exact_probability ──────────────────
  # MPROD is the conjunction of the rows: X AND (Y AND Z)
  #   = 0.7 * 0.3 * 0.5 = 0.105.
  # (It asserted 0.21 = 0.7 * 0.3, dropping reach(M,T)'s 0.5 as 2A did.) The
  # rows share no edge, so there is no shared-lineage warning.

  Scenario: MPROD diamond with exact_probability produces correct probability
    Given having executed:
      """
      CREATE (s:Node {name: 'Source'}),
             (m:Node {name: 'Mid'}),
             (t:Node {name: 'Target'}),
             (s)-[:LINK {p: 0.3}]->(m),
             (m)-[:LINK {p: 0.5}]->(t),
             (s)-[:LINK {p: 0.7}]->(t)
      """
    When evaluating the following Locy program with exact_probability:
      """
      CREATE RULE reach AS
        MATCH (a:Node)-[e:LINK]->(b:Node)
        FOLD prob = MPROD(e.p)
        YIELD KEY a, KEY b, prob
      CREATE RULE reach AS
        MATCH (a:Node)-[e:LINK]->(mid:Node) WHERE mid IS reach TO b
        FOLD prob = MPROD(e.p)
        YIELD KEY a, KEY b, prob
      """
    Then evaluation should succeed
    And the derived relation 'reach' should contain a fact where a.name = 'Source' and b.name = 'Target' and prob = 0.105
    And the result should not contain a SharedProbabilisticDependency warning
    And the result should not contain a BddLimitExceeded warning

  # ── Scenario 2F: Multi-key disjoint groups ─────────────────────────────
  # Non-recursive rule: two independent KEY groups (A,B) and (C,B).
  # No shared proofs → no CrossGroupCorrelationNotExact warning.

  Scenario: Multi-key disjoint groups do not emit CrossGroupCorrelationNotExact
    Given having executed:
      """
      CREATE (a:Node {name: 'A'}),
             (b:Node {name: 'B'}),
             (c:Node {name: 'C'}),
             (a)-[:LINK {p: 0.4}]->(b),
             (a)-[:LINK {p: 0.6}]->(b),
             (c)-[:LINK {p: 0.3}]->(b),
             (c)-[:LINK {p: 0.5}]->(b)
      """
    When evaluating the following Locy program with exact_probability:
      """
      CREATE RULE risk AS
        MATCH (x:Node)-[e:LINK]->(y:Node)
        FOLD prob = MNOR(e.p)
        YIELD KEY x, KEY y, prob
      """
    Then evaluation should succeed
    And the derived relation 'risk' should contain a fact where x.name = 'A' and prob = 0.76
    And the derived relation 'risk' should contain a fact where x.name = 'C' and prob = 0.65
    And the result should not contain a CrossGroupCorrelationNotExact warning

  # ── Scenario 2G: Shared hub triggers CrossGroupCorrelationNotExact ──────
  # Hub H → two target groups (H,X1) and (H,X2) via parallel LINK edges.
  # Rule hub_score yields KEY h (the hub); rule derived uses WHERE h IS hub_score.
  # IS-ref subject 'h' is a YIELD KEY so it appears in the pre-fold yield schema,
  # enabling Tier-1 base-fact tracking. Both groups consume the same hub_score(H)
  # base fact, triggering CrossGroupCorrelationNotExact.

  Scenario: Shared hub emits CrossGroupCorrelationNotExact warning
    Given having executed:
      """
      CREATE (h:HubNode {name: 'Hub'}),
             (b:BaseNode {name: 'Base'}),
             (x1:TargetNode {name: 'X1'}),
             (x2:TargetNode {name: 'X2'}),
             (h)-[:BASE {p: 0.8}]->(b),
             (h)-[:LINK {p: 0.3}]->(x1),
             (h)-[:LINK {p: 0.5}]->(x1),
             (h)-[:LINK {p: 0.4}]->(x2),
             (h)-[:LINK {p: 0.6}]->(x2)
      """
    When evaluating the following Locy program with exact_probability:
      """
      CREATE RULE hub_score AS
        MATCH (h:HubNode)-[e:BASE]->(b:BaseNode)
        FOLD prob = MNOR(e.p)
        YIELD KEY h, prob
      CREATE RULE derived AS
        MATCH (h:HubNode)-[e:LINK]->(x:TargetNode) WHERE h IS hub_score
        FOLD prob = MNOR(e.p)
        YIELD KEY h, KEY x, prob
      """
    Then evaluation should succeed
    And the result should contain a CrossGroupCorrelationNotExact warning for rule 'derived'
    # Phase D F3 case 1 BDD-time deepening: the cross-group warning carries
    # the count of shared base facts (= BDD variables that overlap across
    # KEY groups) as structured metadata, mirroring BddLimitExceeded.
    And the CrossGroupCorrelationNotExact warning for rule 'derived' should have variable_count >= 1

  # ── Scenario 5A: BddLimitExceeded warning has structured metadata ────────
  # Shared-edge graph + max_bdd_variables=1. The shared group requires >= 2
  # variables, so BDD falls back. The warning should carry variable_count and
  # key_group as typed fields (not just in the message string).

  Scenario: BddLimitExceeded warning carries variable_count and key_group metadata
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
    When evaluating the following Locy program with exact_probability and max_bdd_variables 1:
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
    And the result should contain a BddLimitExceeded warning for rule 'reach'
    And the BddLimitExceeded warning for rule 'reach' should have variable_count >= 2
    And the BddLimitExceeded warning for rule 'reach' should have a key_group

  # ── Scenario 5B: BDD fallback stamps _approximate on derived facts ────────
  # Shared-edge graph + max_bdd_variables=1. When a group falls back to
  # independence mode, all facts in the rule are stamped with _approximate=true.

  Scenario: BDD fallback marks derived facts as approximate
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
    When evaluating the following Locy program with exact_probability and max_bdd_variables 1:
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
    And the result should contain a BddLimitExceeded warning for rule 'reach'
    And the derived relation 'reach' should contain a fact where a.name = 'A' and _approximate = true

  # ── Scenario 5C: Non-fallback facts have no _approximate flag ────────────
  # Independent groups (no shared proofs) — BDD succeeds or is not needed.
  # No fact should be stamped with _approximate=true.

  Scenario: Non-fallback exact_probability facts are not marked approximate
    Given having executed:
      """
      CREATE (a:Node {name: 'A'}),
             (b:Node {name: 'B'}),
             (a)-[:LINK {p: 0.4}]->(b),
             (a)-[:LINK {p: 0.6}]->(b)
      """
    When evaluating the following Locy program with exact_probability:
      """
      CREATE RULE risk AS
        MATCH (a:Node)-[e:LINK]->(b:Node)
        FOLD prob = MNOR(e.p)
        YIELD KEY a, KEY b, prob
      """
    Then evaluation should succeed
    And the result should not contain a BddLimitExceeded warning
    And the derived relation 'risk' should not contain any approximate facts

  # ── IS NOT inside an exact formula ─────────────────────────────────────
  # The diamond s -0.7-> t, s -0.3-> m -0.5-> t. odd(s, m, t) holds when t is
  # reachable from s but there is no m -> t edge. Both rest on m -> t:
  #   (X OR (Y AND Z)) AND NOT Z = X AND NOT Z = 0.7 * 0.5 = 0.35,
  # with X = s->t, Y = s->m, Z = m->t. Default mode multiplies the
  # independent factors, 0.745 * (1 - 0.5) = 0.3725, and reports the sharing.
  # (The only other odd(s, m, _) fact is odd(s, m, m) = s->m = 0.3.)

  Scenario: A negated fact sharing an edge with the positive part is a negated literal in exact mode
    Given having executed:
      """
      CREATE (s:Node {name: 's'}),
             (m:Node {name: 'm'}),
             (t:Node {name: 't'}),
             (s)-[:LINK {p: 0.7}]->(t),
             (s)-[:LINK {p: 0.3}]->(m),
             (m)-[:LINK {p: 0.5}]->(t)
      """
    When evaluating the following Locy program with exact_probability:
      """
      CREATE RULE reach AS
        MATCH (a:Node)-[e:LINK]->(b:Node)
        FOLD prob = MNOR(e.p)
        YIELD KEY a, KEY b, prob
      CREATE RULE reach AS
        MATCH (a:Node)-[e:LINK]->(mid:Node) WHERE mid IS reach TO b
        FOLD prob = MNOR(e.p)
        YIELD KEY a, KEY b, prob
      CREATE RULE link AS
        MATCH (a:Node)-[e:LINK]->(b:Node)
        FOLD p = MNOR(e.p)
        YIELD KEY a, KEY b, p
      CREATE RULE odd AS
        MATCH (a:Node), (m:Node) WHERE a IS reach TO b, m IS NOT link TO b
        FOLD prob = MNOR(1.0)
        YIELD KEY a, KEY m, KEY b, prob
      """
    Then evaluation should succeed
    And the derived relation 'odd' should contain a fact where a.name = 's' and m.name = 'm' and prob = 0.35
    # Exact inference resolves the sharing, so the result is no
    # overestimate: reported as a status, not the warning (issue #315).
    And the result should report a SharedDependencyResolved status for rule 'odd'
    And the result should not contain a SharedProbabilisticDependency warning
    And the shared dependency of rule 'odd' should have inference mode 'exact' and resolution 'completed'

  Scenario: A negated fact sharing an edge with the positive part is an independent factor in default mode
    Given having executed:
      """
      CREATE (s:Node {name: 's'}),
             (m:Node {name: 'm'}),
             (t:Node {name: 't'}),
             (s)-[:LINK {p: 0.7}]->(t),
             (s)-[:LINK {p: 0.3}]->(m),
             (m)-[:LINK {p: 0.5}]->(t)
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
      CREATE RULE link AS
        MATCH (a:Node)-[e:LINK]->(b:Node)
        FOLD p = MNOR(e.p)
        YIELD KEY a, KEY b, p
      CREATE RULE odd AS
        MATCH (a:Node), (m:Node) WHERE a IS reach TO b, m IS NOT link TO b
        FOLD prob = MNOR(1.0)
        YIELD KEY a, KEY m, KEY b, prob
      """
    Then evaluation should succeed
    And the derived relation 'odd' should contain a fact where a.name = 's' and m.name = 'm' and prob = 0.3725
    And the result should contain a SharedProbabilisticDependency warning for rule 'odd'
