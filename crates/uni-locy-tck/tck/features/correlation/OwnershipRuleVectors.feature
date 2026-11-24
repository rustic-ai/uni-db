Feature: REQUIRE against published ownership-rule examples

  RequireInRecursion pins REQUIRE on one hand-built graph. This feature checks
  the same program against worked examples that the regulators published, so
  the expected answers come from outside this repository.

  The cases are ported from the conformance vectors of ownership-rules 0.2.0
  (https://github.com/Mattbusel/ownership-rules, Apache-2.0, vectors/ofac,
  vectors/bis and vectors/engine). Each scenario is named after its vector id
  and quotes the agency text the vector encodes.

  Mapping: a listed party is an Entity with listed = true and seeds the rule
  with 100.0. An expected verdict of "definitely" is a row in the answer and
  "not" is its absence. The query leaves listed parties out, so the row count
  is the number of entities the agency says are covered.

  Ported: every vector whose answer depends only on point ownership shares,
  aggregation across listed owners, the >= 50 threshold, cascade through
  covered intermediaries, and cycles. 15 of the 31 vectors. Left out, because
  they need facts this program does not model: control without ownership,
  unknown or interval shares ("possibly"), dates (divestment, delisting),
  country (BIS reaches foreign entities only), BIS program scope, and Entity
  List address entries.

  Background:
    Given an empty graph

  # ── OFAC 50 Percent Rule: FAQ 399, FAQ 401, 2014 guidance ──────────────────

  # OFAC FAQ 399: "if Blocked Person X owns 25 percent of Entity A, and
  # Blocked Person Y owns another 25 percent of Entity A, Entity A is
  # considered to be blocked. ... the ownership interests of persons blocked
  # under different OFAC sanctions programs are aggregated."
  Scenario: ofac-faq399-aggregate-across-programs
    Given having executed:
      """
      CREATE (x:Entity {name: 'X', listed: true})
      CREATE (y:Entity {name: 'Y', listed: true})
      CREATE (a:Entity {name: 'A', listed: false})
      CREATE (x)-[:OWNS {pct: 25.0}]->(a)
      CREATE (y)-[:OWNS {pct: 25.0}]->(a)
      """
    When evaluating the following Locy program:
      """
      CREATE RULE covered AS
        MATCH (e:Entity)
        WHERE e.listed = true
        YIELD KEY e, 100.0 AS agg

      CREATE RULE covered AS
        MATCH (o:Entity)-[s:OWNS]->(e:Entity)
        WHERE o IS covered
        FOLD agg = MSUM(s.pct)
        REQUIRE agg >= 50.0
        YIELD KEY e, agg

      QUERY covered WHERE e.listed = false RETURN e.name AS name
      """
    Then evaluation should succeed
    And the command result 0 should be a Query with 1 rows
    And the command result 0 should be a Query containing row where name = 'A'

  # OFAC FAQ 401, Example 1: "Blocked Person X owns 50 percent of Entity A,
  # and Entity A owns 50 percent of Entity B. Entity B is considered to be
  # blocked."
  Scenario: ofac-faq401-example1
    Given having executed:
      """
      CREATE (x:Entity {name: 'X', listed: true})
      CREATE (a:Entity {name: 'A', listed: false})
      CREATE (b:Entity {name: 'B', listed: false})
      CREATE (x)-[:OWNS {pct: 50.0}]->(a)
      CREATE (a)-[:OWNS {pct: 50.0}]->(b)
      """
    When evaluating the following Locy program:
      """
      CREATE RULE covered AS
        MATCH (e:Entity)
        WHERE e.listed = true
        YIELD KEY e, 100.0 AS agg

      CREATE RULE covered AS
        MATCH (o:Entity)-[s:OWNS]->(e:Entity)
        WHERE o IS covered
        FOLD agg = MSUM(s.pct)
        REQUIRE agg >= 50.0
        YIELD KEY e, agg

      QUERY covered WHERE e.listed = false RETURN e.name AS name
      """
    Then evaluation should succeed
    And the command result 0 should be a Query with 2 rows
    And the command result 0 should be a Query containing row where name = 'A'
    And the command result 0 should be a Query containing row where name = 'B'

  # OFAC FAQ 401, Example 2: "Blocked Person X owns 50 percent of Entity A and
  # 50 percent of Entity B. Entities A and B each own 25 percent of Entity C.
  # Entity C is considered to be blocked."
  Scenario: ofac-faq401-example2
    Given having executed:
      """
      CREATE (x:Entity {name: 'X', listed: true})
      CREATE (a:Entity {name: 'A', listed: false})
      CREATE (b:Entity {name: 'B', listed: false})
      CREATE (c:Entity {name: 'C', listed: false})
      CREATE (x)-[:OWNS {pct: 50.0}]->(a)
      CREATE (x)-[:OWNS {pct: 50.0}]->(b)
      CREATE (a)-[:OWNS {pct: 25.0}]->(c)
      CREATE (b)-[:OWNS {pct: 25.0}]->(c)
      """
    When evaluating the following Locy program:
      """
      CREATE RULE covered AS
        MATCH (e:Entity)
        WHERE e.listed = true
        YIELD KEY e, 100.0 AS agg

      CREATE RULE covered AS
        MATCH (o:Entity)-[s:OWNS]->(e:Entity)
        WHERE o IS covered
        FOLD agg = MSUM(s.pct)
        REQUIRE agg >= 50.0
        YIELD KEY e, agg

      QUERY covered WHERE e.listed = false RETURN e.name AS name
      """
    Then evaluation should succeed
    And the command result 0 should be a Query with 3 rows
    And the command result 0 should be a Query containing row where name = 'A'
    And the command result 0 should be a Query containing row where name = 'B'
    And the command result 0 should be a Query containing row where name = 'C'

  # OFAC FAQ 401, Example 3: "Blocked Person X owns 50 percent of Entity A and
  # 10 percent of Entity B. Entity A also owns 40 percent of Entity B. Entity
  # B is considered to be blocked."
  Scenario: ofac-faq401-example3
    Given having executed:
      """
      CREATE (x:Entity {name: 'X', listed: true})
      CREATE (a:Entity {name: 'A', listed: false})
      CREATE (b:Entity {name: 'B', listed: false})
      CREATE (x)-[:OWNS {pct: 50.0}]->(a)
      CREATE (x)-[:OWNS {pct: 10.0}]->(b)
      CREATE (a)-[:OWNS {pct: 40.0}]->(b)
      """
    When evaluating the following Locy program:
      """
      CREATE RULE covered AS
        MATCH (e:Entity)
        WHERE e.listed = true
        YIELD KEY e, 100.0 AS agg

      CREATE RULE covered AS
        MATCH (o:Entity)-[s:OWNS]->(e:Entity)
        WHERE o IS covered
        FOLD agg = MSUM(s.pct)
        REQUIRE agg >= 50.0
        YIELD KEY e, agg

      QUERY covered WHERE e.listed = false RETURN e.name AS name
      """
    Then evaluation should succeed
    And the command result 0 should be a Query with 2 rows
    And the command result 0 should be a Query containing row where name = 'A'
    And the command result 0 should be a Query containing row where name = 'B'

  # OFAC FAQ 401, Example 4: "Blocked Person X owns 50 percent of Entity A and
  # 25 percent of Entity B. Entities A and B each own 25 percent of Entity C.
  # Entity C is not considered to be blocked."
  Scenario: ofac-faq401-example4
    Given having executed:
      """
      CREATE (x:Entity {name: 'X', listed: true})
      CREATE (a:Entity {name: 'A', listed: false})
      CREATE (b:Entity {name: 'B', listed: false})
      CREATE (c:Entity {name: 'C', listed: false})
      CREATE (x)-[:OWNS {pct: 50.0}]->(a)
      CREATE (x)-[:OWNS {pct: 25.0}]->(b)
      CREATE (a)-[:OWNS {pct: 25.0}]->(c)
      CREATE (b)-[:OWNS {pct: 25.0}]->(c)
      """
    When evaluating the following Locy program:
      """
      CREATE RULE covered AS
        MATCH (e:Entity)
        WHERE e.listed = true
        YIELD KEY e, 100.0 AS agg

      CREATE RULE covered AS
        MATCH (o:Entity)-[s:OWNS]->(e:Entity)
        WHERE o IS covered
        FOLD agg = MSUM(s.pct)
        REQUIRE agg >= 50.0
        YIELD KEY e, agg

      QUERY covered WHERE e.listed = false RETURN e.name AS name
      """
    Then evaluation should succeed
    And the command result 0 should be a Query with 1 rows
    And the command result 0 should be a Query containing row where name = 'A'

  # OFAC FAQ 401, Example 5: "Blocked Person X owns 25 percent of Entity A and
  # 25 percent of Entity B. Entities A and B each own 50 percent of Entity C.
  # Entity C is not considered to be blocked."
  Scenario: ofac-faq401-example5
    Given having executed:
      """
      CREATE (x:Entity {name: 'X', listed: true})
      CREATE (a:Entity {name: 'A', listed: false})
      CREATE (b:Entity {name: 'B', listed: false})
      CREATE (c:Entity {name: 'C', listed: false})
      CREATE (x)-[:OWNS {pct: 25.0}]->(a)
      CREATE (x)-[:OWNS {pct: 25.0}]->(b)
      CREATE (a)-[:OWNS {pct: 50.0}]->(c)
      CREATE (b)-[:OWNS {pct: 50.0}]->(c)
      """
    When evaluating the following Locy program:
      """
      CREATE RULE covered AS
        MATCH (e:Entity)
        WHERE e.listed = true
        YIELD KEY e, 100.0 AS agg

      CREATE RULE covered AS
        MATCH (o:Entity)-[s:OWNS]->(e:Entity)
        WHERE o IS covered
        FOLD agg = MSUM(s.pct)
        REQUIRE agg >= 50.0
        YIELD KEY e, agg

      QUERY covered WHERE e.listed = false RETURN e.name AS name
      """
    Then evaluation should succeed
    And the command result 0 should be a Query with 0 rows

  # The same graph with WHERE in place of REQUIRE. A and B are filtered from
  # the answer but still feed C 50% each, so C comes out covered. This pins
  # the wrong answer the WHERE reading gives on an agency example, so the pair
  # above stops passing together if either keyword changes meaning.
  Scenario: ofac-faq401-example5 with the post-FOLD WHERE derives C, which OFAC says is not blocked
    Given having executed:
      """
      CREATE (x:Entity {name: 'X', listed: true})
      CREATE (a:Entity {name: 'A', listed: false})
      CREATE (b:Entity {name: 'B', listed: false})
      CREATE (c:Entity {name: 'C', listed: false})
      CREATE (x)-[:OWNS {pct: 25.0}]->(a)
      CREATE (x)-[:OWNS {pct: 25.0}]->(b)
      CREATE (a)-[:OWNS {pct: 50.0}]->(c)
      CREATE (b)-[:OWNS {pct: 50.0}]->(c)
      """
    When evaluating the following Locy program:
      """
      CREATE RULE covered AS
        MATCH (e:Entity)
        WHERE e.listed = true
        YIELD KEY e, 100.0 AS agg

      CREATE RULE covered AS
        MATCH (o:Entity)-[s:OWNS]->(e:Entity)
        WHERE o IS covered
        FOLD agg = MSUM(s.pct)
        WHERE agg >= 50.0
        YIELD KEY e, agg

      QUERY covered WHERE e.listed = false RETURN e.name AS name
      """
    Then evaluation should succeed
    And the command result 0 should be a Query with 1 rows
    And the command result 0 should be a Query containing row where name = 'C'

  # OFAC Revised Guidance, Aug. 13, 2014: "any entity owned in the aggregate,
  # directly or indirectly, 50 percent or more by one or more blocked persons
  # is itself considered to be a blocked person ... U.S. persons are advised
  # to act with caution when considering a transaction with a non-blocked
  # entity in which one or more blocked persons has a significant ownership
  # interest that is less than 50 percent"
  Scenario: ofac-2014-guidance-threshold
    Given having executed:
      """
      CREATE (x:Entity {name: 'X', listed: true})
      CREATE (a:Entity {name: 'A', listed: false})
      CREATE (b:Entity {name: 'B', listed: false})
      CREATE (x)-[:OWNS {pct: 49.99}]->(a)
      CREATE (x)-[:OWNS {pct: 50.0}]->(b)
      """
    When evaluating the following Locy program:
      """
      CREATE RULE covered AS
        MATCH (e:Entity)
        WHERE e.listed = true
        YIELD KEY e, 100.0 AS agg

      CREATE RULE covered AS
        MATCH (o:Entity)-[s:OWNS]->(e:Entity)
        WHERE o IS covered
        FOLD agg = MSUM(s.pct)
        REQUIRE agg >= 50.0
        YIELD KEY e, agg

      QUERY covered WHERE e.listed = false RETURN e.name AS name
      """
    Then evaluation should succeed
    And the command result 0 should be a Query with 1 rows
    And the command result 0 should be a Query containing row where name = 'B'

  # ── BIS Affiliates Rule: 90 FR 47201, Entity List FAQs ─────────────────────

  # 90 FR 47201, Application example 1: "Company A is listed on the Entity
  # List and requires a license for all items subject to the EAR and has a
  # presumption of denial license review policy. Company A owns 35 percent of
  # Company C. ... Company B is listed on the Entity List and requires a
  # license for any item on the CCL and has a case-by-case license review
  # policy. Company B owns 15 percent of Company C. ... the Entity List
  # license requirements and license review policy for Company A would be
  # followed ... Company D is owned 50 percent by Company C."
  Scenario: bis-ifr-application-example1
    Given having executed:
      """
      CREATE (a:Entity {name: 'A', listed: true})
      CREATE (b:Entity {name: 'B', listed: true})
      CREATE (c:Entity {name: 'C', listed: false})
      CREATE (d:Entity {name: 'D', listed: false})
      CREATE (a)-[:OWNS {pct: 35.0}]->(c)
      CREATE (b)-[:OWNS {pct: 15.0}]->(c)
      CREATE (c)-[:OWNS {pct: 50.0}]->(d)
      """
    When evaluating the following Locy program:
      """
      CREATE RULE covered AS
        MATCH (e:Entity)
        WHERE e.listed = true
        YIELD KEY e, 100.0 AS agg

      CREATE RULE covered AS
        MATCH (o:Entity)-[s:OWNS]->(e:Entity)
        WHERE o IS covered
        FOLD agg = MSUM(s.pct)
        REQUIRE agg >= 50.0
        YIELD KEY e, agg

      QUERY covered WHERE e.listed = false RETURN e.name AS name
      """
    Then evaluation should succeed
    And the command result 0 should be a Query with 2 rows
    And the command result 0 should be a Query containing row where name = 'C'
    And the command result 0 should be a Query containing row where name = 'D'

  # 90 FR 47201, Application example 1: "the breakdown of the percentages
  # adding up to 50 percent or more does not matter (e.g., in this
  # hypothetical, it would not matter if Company B held 35 percent and Company
  # A held 15 percent--Company A's requirements would still apply)."
  Scenario: bis-ifr-application-example1-swapped
    Given having executed:
      """
      CREATE (a:Entity {name: 'A', listed: true})
      CREATE (b:Entity {name: 'B', listed: true})
      CREATE (c:Entity {name: 'C', listed: false})
      CREATE (a)-[:OWNS {pct: 15.0}]->(c)
      CREATE (b)-[:OWNS {pct: 35.0}]->(c)
      """
    When evaluating the following Locy program:
      """
      CREATE RULE covered AS
        MATCH (e:Entity)
        WHERE e.listed = true
        YIELD KEY e, 100.0 AS agg

      CREATE RULE covered AS
        MATCH (o:Entity)-[s:OWNS]->(e:Entity)
        WHERE o IS covered
        FOLD agg = MSUM(s.pct)
        REQUIRE agg >= 50.0
        YIELD KEY e, agg

      QUERY covered WHERE e.listed = false RETURN e.name AS name
      """
    Then evaluation should succeed
    And the command result 0 should be a Query with 1 rows
    And the command result 0 should be a Query containing row where name = 'C'

  # 90 FR 47201, Application example 2 for FDP application: "Company E is
  # listed on the Entity List with a footnote 3 designation ... Company E owns
  # 35 percent of Company G. ... Company F is listed on the Entity List with a
  # footnote 1 designation ... Company F owns 15 percent of Company G. ...
  # both the criteria under 734.9(e)(1) and (g) (i.e., the requirements for
  # both Company E and Company F) would be used"
  Scenario: bis-ifr-application-example2-fdp
    Given having executed:
      """
      CREATE (e:Entity {name: 'E', listed: true})
      CREATE (f:Entity {name: 'F', listed: true})
      CREATE (g:Entity {name: 'G', listed: false})
      CREATE (e)-[:OWNS {pct: 35.0}]->(g)
      CREATE (f)-[:OWNS {pct: 15.0}]->(g)
      """
    When evaluating the following Locy program:
      """
      CREATE RULE covered AS
        MATCH (e:Entity)
        WHERE e.listed = true
        YIELD KEY e, 100.0 AS agg

      CREATE RULE covered AS
        MATCH (o:Entity)-[s:OWNS]->(e:Entity)
        WHERE o IS covered
        FOLD agg = MSUM(s.pct)
        REQUIRE agg >= 50.0
        YIELD KEY e, agg

      QUERY covered WHERE e.listed = false RETURN e.name AS name
      """
    Then evaluation should succeed
    And the command result 0 should be a Query with 1 rows
    And the command result 0 should be a Query containing row where name = 'G'

  # BIS Entity List FAQ Q.52: "Company A (an Entity List party with a license
  # requirement for all items subject to the EAR), owns 50% of Company B (an
  # unlisted party), which owns 50% of Company C (an unlisted party). May I
  # export items subject to the EAR to Company C? A.52: No, not unless you
  # first obtain a license from BIS."
  Scenario: bis-faq52-chain
    Given having executed:
      """
      CREATE (a:Entity {name: 'A', listed: true})
      CREATE (b:Entity {name: 'B', listed: false})
      CREATE (c:Entity {name: 'C', listed: false})
      CREATE (a)-[:OWNS {pct: 50.0}]->(b)
      CREATE (b)-[:OWNS {pct: 50.0}]->(c)
      """
    When evaluating the following Locy program:
      """
      CREATE RULE covered AS
        MATCH (e:Entity)
        WHERE e.listed = true
        YIELD KEY e, 100.0 AS agg

      CREATE RULE covered AS
        MATCH (o:Entity)-[s:OWNS]->(e:Entity)
        WHERE o IS covered
        FOLD agg = MSUM(s.pct)
        REQUIRE agg >= 50.0
        YIELD KEY e, agg

      QUERY covered WHERE e.listed = false RETURN e.name AS name
      """
    Then evaluation should succeed
    And the command result 0 should be a Query with 2 rows
    And the command result 0 should be a Query containing row where name = 'B'
    And the command result 0 should be a Query containing row where name = 'C'

  # BIS Entity List FAQ Q.44: "the Affiliates Rule applies if the total
  # ownership by parties listed under all three programs (Entity List, MEU
  # List, and SDNs designated under programs listed in 744.8(a)(1)) or parties
  # that meet the Affiliates Rule criteria is greater than 50 percent, even if
  # the ownership by parties listed on one of these lists alone is less than
  # 50 percent."
  Scenario: bis-faq44-aggregate-across-lists
    Given having executed:
      """
      CREATE (a:Entity {name: 'A', listed: true})
      CREATE (m:Entity {name: 'M', listed: true})
      CREATE (s:Entity {name: 'S', listed: true})
      CREATE (c:Entity {name: 'C', listed: false})
      CREATE (a)-[:OWNS {pct: 30.0}]->(c)
      CREATE (m)-[:OWNS {pct: 15.0}]->(c)
      CREATE (s)-[:OWNS {pct: 10.0}]->(c)
      """
    When evaluating the following Locy program:
      """
      CREATE RULE covered AS
        MATCH (e:Entity)
        WHERE e.listed = true
        YIELD KEY e, 100.0 AS agg

      CREATE RULE covered AS
        MATCH (o:Entity)-[s:OWNS]->(e:Entity)
        WHERE o IS covered
        FOLD agg = MSUM(s.pct)
        REQUIRE agg >= 50.0
        YIELD KEY e, agg

      QUERY covered WHERE e.listed = false RETURN e.name AS name
      """
    Then evaluation should succeed
    And the command result 0 should be a Query with 1 rows
    And the command result 0 should be a Query containing row where name = 'C'

  # BIS Entity List FAQ Q.25: "The Affiliates Rule applies downward to
  # entities owned 50 percent or more, directly or indirectly, by listed
  # entities or by non-listed entities that meet the Affiliates Rule criteria,
  # not upward to owners of listed entities."
  Scenario: bis-faq25-downward-not-upward
    Given having executed:
      """
      CREATE (a:Entity {name: 'A', listed: true})
      CREATE (p:Entity {name: 'P', listed: false})
      CREATE (p)-[:OWNS {pct: 100.0}]->(a)
      """
    When evaluating the following Locy program:
      """
      CREATE RULE covered AS
        MATCH (e:Entity)
        WHERE e.listed = true
        YIELD KEY e, 100.0 AS agg

      CREATE RULE covered AS
        MATCH (o:Entity)-[s:OWNS]->(e:Entity)
        WHERE o IS covered
        FOLD agg = MSUM(s.pct)
        REQUIRE agg >= 50.0
        YIELD KEY e, agg

      QUERY covered WHERE e.listed = false RETURN e.name AS name
      """
    Then evaluation should succeed
    And the command result 0 should be a Query with 0 rows

  # ── Ownership cycles (engine suite, not agency text) ───────────────────────

  # X holds 10% of A; A and B hold 60% of each other. Neither is reached: the
  # cycle has no 50% entry from a listed owner, so it must not bootstrap
  # itself.
  Scenario: engine-cycle-without-listed-owner
    Given having executed:
      """
      CREATE (x:Entity {name: 'X', listed: true})
      CREATE (a:Entity {name: 'A', listed: false})
      CREATE (b:Entity {name: 'B', listed: false})
      CREATE (x)-[:OWNS {pct: 10.0}]->(a)
      CREATE (a)-[:OWNS {pct: 60.0}]->(b)
      CREATE (b)-[:OWNS {pct: 60.0}]->(a)
      """
    When evaluating the following Locy program:
      """
      CREATE RULE covered AS
        MATCH (e:Entity)
        WHERE e.listed = true
        YIELD KEY e, 100.0 AS agg

      CREATE RULE covered AS
        MATCH (o:Entity)-[s:OWNS]->(e:Entity)
        WHERE o IS covered
        FOLD agg = MSUM(s.pct)
        REQUIRE agg >= 50.0
        YIELD KEY e, agg

      QUERY covered WHERE e.listed = false RETURN e.name AS name
      """
    Then evaluation should succeed
    And the command result 0 should be a Query with 0 rows

  # X holds 50% of A; A and B hold 50% of each other. Both are reached, and
  # the fixpoint has to terminate even though A's stake in B feeds back into
  # A.
  Scenario: engine-cycle-with-listed-owner
    Given having executed:
      """
      CREATE (x:Entity {name: 'X', listed: true})
      CREATE (a:Entity {name: 'A', listed: false})
      CREATE (b:Entity {name: 'B', listed: false})
      CREATE (x)-[:OWNS {pct: 50.0}]->(a)
      CREATE (a)-[:OWNS {pct: 50.0}]->(b)
      CREATE (b)-[:OWNS {pct: 50.0}]->(a)
      """
    When evaluating the following Locy program:
      """
      CREATE RULE covered AS
        MATCH (e:Entity)
        WHERE e.listed = true
        YIELD KEY e, 100.0 AS agg

      CREATE RULE covered AS
        MATCH (o:Entity)-[s:OWNS]->(e:Entity)
        WHERE o IS covered
        FOLD agg = MSUM(s.pct)
        REQUIRE agg >= 50.0
        YIELD KEY e, agg

      QUERY covered WHERE e.listed = false RETURN e.name AS name
      """
    Then evaluation should succeed
    And the command result 0 should be a Query with 2 rows
    And the command result 0 should be a Query containing row where name = 'A'
    And the command result 0 should be a Query containing row where name = 'B'
