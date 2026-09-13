Feature: REQUIRE — a definitional threshold inside recursion (issue #265)

  A post-FOLD WHERE filters the converged answer, so in a recursive rule a
  group the threshold excludes is absent from the output while still having
  derived rows into it. REQUIRE is the other reading: it applies to every
  iteration's folded snapshot — the view a same-stratum self-reference reads —
  so the threshold constrains what the recursion can derive.

  The graph throughout is the OFAC 50 Percent Rule in miniature. A is
  designated. A holds 11% of B, and B holds 60% of C. 11 < 50, so B does not
  qualify, and nothing downstream of B should either. The correct determination
  is {A}.

  Background:
    Given an empty graph

  # ── The pair that decides it ─────────────────────────────────────────
  #
  # These two scenarios differ by one word. If they ever agree, REQUIRE has
  # stopped doing anything and the feature is gone.

  Scenario: REQUIRE constrains the recursion, so the chain stops at B
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
        REQUIRE agg >= 50.0
        YIELD KEY e, agg

      QUERY blocked RETURN e.name AS name
      """
    Then evaluation should succeed
    And the command result 0 should be a Query with 1 rows
    And the command result 0 should be a Query containing row where name = 'A'

  Scenario: the post-FOLD WHERE filters the answer only, so C is still derived
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
    And the command result 0 should be a Query with 2 rows
    And the command result 0 should be a Query containing row where name = 'A'
    And the command result 0 should be a Query containing row where name = 'C'

  # ── REQUIRE silences the warning the old spelling earns ──────────────

  Scenario: a rule using REQUIRE does not warn about HavingInRecursivePath
    Given having executed:
      """
      CREATE (:Entity {name: 'A', designated: true})
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
        REQUIRE agg >= 50.0
        YIELD KEY e, agg
      """
    Then evaluation should succeed
    And the result should not contain a HavingInRecursivePath warning

  # ── Outside recursion the two spellings agree ────────────────────────
  #
  # One pass means nothing can be derived after the filter runs, so REQUIRE and
  # the post-FOLD WHERE are the same statement. This is what makes a rule
  # refactor-safe: an author writes REQUIRE for the meaning they want and keeps
  # that meaning if a self-reference arrives later.

  Scenario: REQUIRE and the post-FOLD WHERE agree without recursion
    Given having executed:
      """
      CREATE (a:Entity {name: 'A', designated: true})
      CREATE (b:Entity {name: 'B', designated: false})
      CREATE (a)-[:OWNS {pct: 60.0}]->(b)
      """
    When evaluating the following Locy program:
      """
      CREATE RULE stake AS
        MATCH (o:Entity)-[s:OWNS]->(e:Entity)
        WHERE o.designated = true
        FOLD agg = MSUM(s.pct)
        REQUIRE agg >= 50.0
        YIELD KEY e, agg

      QUERY stake RETURN e.name AS name
      """
    Then evaluation should succeed
    And the command result 0 should be a Query with 1 rows
    And the command result 0 should be a Query containing row where name = 'B'

  # ── REQUIRE resolves against an aliased fold output ──────────────────
  #
  # `substitute_fold_aliases` is applied to REQUIRE exactly as to the post-FOLD
  # WHERE, so a predicate naming the fold by its own name still resolves after
  # YIELD renames it. Without that substitution this matches nothing and the
  # rule returns empty.
  #
  # Deliberately non-recursive: a RECURSIVE rule that renames a fold output in
  # YIELD fails before any of this is reached, with "FOLD aggregate 'SUM' input
  # column 'agg' not found in body batch". That reproduces identically with the
  # post-FOLD WHERE in place of REQUIRE, so it is a pre-existing limitation of
  # recursive FOLD plus aliasing rather than anything REQUIRE introduced, and
  # pinning it here would test someone else's defect.

  Scenario: REQUIRE names a fold output that YIELD renames
    Given having executed:
      """
      CREATE (a:Entity {name: 'A', designated: true})
      CREATE (b:Entity {name: 'B', designated: false})
      CREATE (c:Entity {name: 'C', designated: false})
      CREATE (a)-[:OWNS {pct: 60.0}]->(b)
      CREATE (a)-[:OWNS {pct: 11.0}]->(c)
      """
    When evaluating the following Locy program:
      """
      CREATE RULE stake AS
        MATCH (o:Entity)-[s:OWNS]->(e:Entity)
        WHERE o.designated = true
        FOLD agg = MSUM(s.pct)
        REQUIRE agg >= 50.0
        YIELD KEY e, agg AS total

      QUERY stake RETURN e.name AS name
      """
    Then evaluation should succeed
    And the command result 0 should be a Query with 1 rows
    And the command result 0 should be a Query containing row where name = 'B'
