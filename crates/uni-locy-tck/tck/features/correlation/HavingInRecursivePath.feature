Feature: HavingInRecursivePath compile warning (issue #265)

  When a rule references itself AND carries a post-FOLD WHERE (HAVING),
  the filter is applied once to the CONVERGED answer rather than per
  iteration. The self-reference therefore reads the rule's *unfiltered*
  folded value, and rows the threshold excludes can still derive further
  rows — while being correctly absent from the output, which is what
  makes the wrong answer look plausible.

  That post-fixpoint reading is intended when the filter selects what you
  see: a child the threshold removes from the answer must still have been
  visible to its parent while the fixpoint ran, which is what
  probabilistic rules need (issue #162). It is also the only available
  reading, and "filter the answer" and "the threshold is part of the
  definition" are spelled identically — so the compiler warns rather than
  rejecting.

  Background:
    Given an empty graph

  # ── Self-reference + FOLD + post-FOLD WHERE → warning ────────────────

  Scenario: A self-referencing rule with a post-FOLD WHERE warns
    Given having executed:
      """
      CREATE (:Entity {name: 'A', designated: true})
      CREATE (:Entity {name: 'B', designated: false})
      """
    When evaluating the following Locy program:
      """
      CREATE RULE blocked AS
        MATCH (e:Entity)
        WHERE e.designated = true
        YIELD KEY e, 100.0 AS agg

      CREATE RULE blocked AS
        MATCH (o:Entity)-[s:OWNS]->(e:Entity)
        WHERE o IS blocked
        FOLD agg = MSUM(s.pct)
        WHERE agg >= 50.0
        YIELD KEY e, agg
      """
    Then evaluation should succeed
    And the result should contain a HavingInRecursivePath warning

  # ── The behaviour the warning is about ───────────────────────────────
  #
  # The scenario the warning exists for, spelled out so the semantics is
  # pinned rather than merely described. A(designated) owns 11% of B, and
  # B owns 60% of C. 11 < 50, so B does not qualify and nothing downstream
  # of it should. B is indeed absent from the answer — and C is present
  # anyway, derived through the very row the threshold removed.
  #
  # If this ever starts returning A only, the per-iteration reading has
  # landed and both this scenario and #162's W2 case have to be revisited
  # together: they want opposite answers from the same syntax.

  Scenario: The excluded group still drives the recursion
    Given having executed:
      """
      CREATE (a:Entity {name: 'A', designated: true})
      CREATE (b:Entity {name: 'B', designated: false})
      CREATE (c:Entity {name: 'C', designated: false})
      CREATE (a)-[:OWNS {pct: 11.0}]->(b)
      CREATE (b)-[:OWNS {pct: 60.0}]->(c)
      """
    When evaluating the following Locy program:
      """
      CREATE RULE blocked AS
        MATCH (e:Entity)
        WHERE e.designated = true
        YIELD KEY e, 100.0 AS agg

      CREATE RULE blocked AS
        MATCH (o:Entity)-[s:OWNS]->(e:Entity)
        WHERE o IS blocked
        FOLD agg = MSUM(s.pct)
        WHERE agg >= 50.0
        YIELD KEY e, agg

      QUERY blocked RETURN e.name AS name
      """
    Then evaluation should succeed
    And the result should contain a HavingInRecursivePath warning
    And the command result 0 should be a Query with 2 rows
    And the command result 0 should be a Query containing row where name = 'A'
    And the command result 0 should be a Query containing row where name = 'C'

  # ── Same filter, no recursion → no warning ───────────────────────────
  #
  # The control. Replacing the self-reference with a base-fact test leaves
  # the post-FOLD WHERE behaving exactly as SQL HAVING does, so a check
  # that warned on every post-FOLD WHERE would be useless noise.

  Scenario: A post-FOLD WHERE without recursion does not warn
    Given having executed:
      """
      CREATE (:Entity {name: 'A', designated: true})
      """
    When evaluating the following Locy program:
      """
      CREATE RULE blocked AS
        MATCH (o:Entity)-[s:OWNS]->(e:Entity)
        WHERE o.designated = true
        FOLD agg = MSUM(s.pct)
        WHERE agg >= 50.0
        YIELD KEY e, agg
      """
    Then evaluation should succeed
    And the result should not contain a HavingInRecursivePath warning

  # ── Recursion without a post-FOLD WHERE → no warning ─────────────────
  #
  # Separates this warning from its neighbour FoldInRecursivePath, which
  # fires on this same shape. Firing both wherever one fires would make
  # this one noise.

  Scenario: A recursive FOLD with no post-FOLD WHERE does not warn
    Given having executed:
      """
      CREATE (:Node {name: 'A'})-[:EDGE]->(:Node {name: 'B'})
      """
    When evaluating the following Locy program:
      """
      CREATE RULE reachable AS
        MATCH (a:Node)-[:EDGE]->(b:Node)
        FOLD risk = MNOR(0.9)
        YIELD KEY a, KEY b, risk

      CREATE RULE reachable AS
        MATCH (a:Node)-[:EDGE]->(mid:Node)
        WHERE mid IS reachable TO b
        FOLD risk = MNOR(0.9)
        YIELD KEY a, KEY b, risk
      """
    Then evaluation should succeed
    And the result should not contain a HavingInRecursivePath warning
