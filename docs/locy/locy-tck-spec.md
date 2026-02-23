# Locy TCK Specification

**Technology Compliance Kit for the Locy Graph Reasoning Language**

Version 0.2 — February 2026

---

## Table of Contents

1. [Overview](#1-overview)
2. [Architecture](#2-architecture)
3. [Relationship to OpenCypher TCK](#3-relationship-to-opencypher-tck)
4. [Testing Levels](#4-testing-levels)
5. [Feature Suite: Lexical & Keywords](#5-feature-suite-lexical--keywords)
6. [Feature Suite: Rule Definitions (CREATE RULE)](#6-feature-suite-rule-definitions-create-rule)
7. [Feature Suite: Rule References (IS / IS NOT)](#7-feature-suite-rule-references-is--is-not)
8. [Feature Suite: Path-Carried Values (ALONG / prev)](#8-feature-suite-path-carried-values-along--prev)
9. [Feature Suite: Aggregation (FOLD)](#9-feature-suite-aggregation-fold)
10. [Feature Suite: Monotonic Aggregation (MSUM / MMAX / MMIN / MCOUNT)](#10-feature-suite-monotonic-aggregation)
11. [Feature Suite: Optimized Selection (BEST BY)](#11-feature-suite-optimized-selection-best-by)
12. [Feature Suite: Graph Derivation (DERIVE)](#12-feature-suite-graph-derivation-derive)
13. [Feature Suite: Existential Quantification (NEW)](#13-feature-suite-existential-quantification-new)
14. [Feature Suite: Entity Resolution (DERIVE MERGE)](#14-feature-suite-entity-resolution-derive-merge)
15. [Feature Suite: Prioritized Rules (PRIORITY)](#15-feature-suite-prioritized-rules-priority)
16. [Feature Suite: Goal-Directed Evaluation (QUERY)](#16-feature-suite-goal-directed-evaluation-query)
17. [Feature Suite: Hypothetical Reasoning (ASSUME)](#17-feature-suite-hypothetical-reasoning-assume)
18. [Feature Suite: Abductive Reasoning (ABDUCE)](#18-feature-suite-abductive-reasoning-abduce)
19. [Feature Suite: Proof Traces (EXPLAIN RULE)](#19-feature-suite-proof-traces-explain-rule)
20. [Feature Suite: Modules and Composition (USE)](#20-feature-suite-modules-and-composition-use)
21. [Feature Suite: Evaluation Model & Semantics](#21-feature-suite-evaluation-model--semantics)
22. [Feature Suite: Type System](#22-feature-suite-type-system)
23. [Feature Suite: OpenCypher Superset Guarantee](#23-feature-suite-opencypher-superset-guarantee)
24. [Feature Suite: Integration Scenarios](#24-feature-suite-integration-scenarios)
25. [Error & Rejection Scenarios](#25-error--rejection-scenarios)
26. [Directory Structure](#26-directory-structure)
27. [Running the TCK](#27-running-the-tck)
28. [Compliance Reporting](#28-compliance-reporting)
29. [Scenario Summary](#29-scenario-summary)

---

## 1 Overview

### 1.1 Purpose

The Locy TCK (Technology Compliance Kit) is a comprehensive, executable test suite that validates conformance to the Locy Language Specification v0.2. It provides a standardized mechanism for implementors to verify that their Locy compiler, runtime, or engine correctly handles all syntactic constructs, semantic rules, compile-time checks, and runtime behaviors defined by the specification.

### 1.2 Scope

The TCK covers:

- **Parsing compliance** — every syntactic construct in the Locy BNF grammar parses correctly.
- **Semantic validation** — compile-time checks (stratification, wardedness, schema consistency, type constraints) accept valid programs and reject invalid ones.
- **Evaluation correctness** — rule evaluation, fixpoint computation, aggregation, goal-directed resolution, hypothetical reasoning, abductive search, and proof traces produce the results prescribed by the specification.
- **OpenCypher backward compatibility** — the entire OpenCypher TCK continues to pass through the Locy parser and runtime.

### 1.3 Non-Goals

The TCK does **not** cover:

- Performance benchmarks or scalability characteristics.
- Implementation-specific features (storage format, concurrency model, API bindings).
- Features explicitly marked as "implementation-defined" in the specification (e.g., proof trace output format, `ABDUCE` time limits).
- Future work items listed in Appendix B of the specification.

### 1.4 Conventions

All test scenarios are expressed as Gherkin `.feature` files compatible with Cucumber. Each scenario follows the pattern:

```gherkin
Feature: <Feature Name>

  Background:
    Given an empty graph
    And having executed:
      """
      <setup cypher>
      """

  Scenario: <Scenario Name>
    When <action>
    Then <expected outcome>
```

Spec references are given as `§<section>` linking to the Locy Specification v0.2.

---

## 2 Architecture

### 2.1 Layered Design

The Locy TCK is structured as an independent, additive layer alongside the OpenCypher TCK:

```
tck/
├── openCypher/              # Upstream OpenCypher TCK (untouched)
│   └── features/
│       ├── clauses/
│       ├── expressions/
│       └── ...
├── locy/                    # Locy extension TCK
│   ├── features/
│   │   ├── lexical/
│   │   ├── rules/
│   │   ├── path_values/
│   │   ├── aggregation/
│   │   ├── derivation/
│   │   ├── reasoning/
│   │   ├── modules/
│   │   ├── evaluation/
│   │   ├── type_system/
│   │   ├── superset/
│   │   ├── integration/
│   │   └── errors/
│   └── support/
│       ├── step_definitions/
│       └── test_helpers/
└── README.md
```

### 2.2 Independence Principle

The OpenCypher TCK is never modified. Both suites are run independently and report separate pass/fail rates. This allows implementors to track two compliance metrics:

- **OpenCypher compliance**: X% of N scenarios
- **Locy compliance**: Y% of M scenarios

### 2.3 Test Runner Integration

Implementations should extend their existing TCK runner (e.g., `tck_extractor` + `tck_test_suite` in Rust) with support for Locy-specific step definitions. All Locy evaluation-level tests submit programs through `db.locy().evaluate()`, which returns a `LocyResult` containing query rows, materialization stats, and compile metadata. See the **Locy API Design** document for type definitions.

The Locy TCK introduces the following new Gherkin steps in addition to standard OpenCypher steps:

```gherkin
# Parse-level steps
When parsing the following Locy program:
Then the program should parse successfully
Then the program should fail to parse
Then the parse tree should contain a <production> node

# Compile-level steps
When compiling the following Locy program:
Then the program should compile successfully
Then a compile error should be raised with code <error_code>
Then a compile warning should be raised for <warning_type>
Then the stratification should produce <n> strata
Then rule <name> should be in stratum <n>

# Evaluation-level steps (all via db.locy().evaluate())
When evaluating the following Locy program:
Then the result should be:
Then the result should be empty
Then the result should not be empty
Then the result should satisfy:
Then the result should contain:
Then the result should NOT contain:
Then the derived graph should contain edge (<a>)-[:<TYPE>]->(<b>)
Then the derivation tree should not be empty
Then the derivation tree should contain a leaf referencing base fact <fact>
Then the ABDUCE result should contain at least one modification
Then each modification should be a valid Cypher mutation statement
Then the ASSUME block should not modify persistent state
Then the derive stats should show <n> iterations
Then the derive stats should show convergence
```

---

## 3 Relationship to OpenCypher TCK

### 3.1 Superset Guarantee (§1.3)

The Locy specification guarantees that every valid OpenCypher query is a valid Locy program. The TCK enforces this by:

1. Running the complete OpenCypher TCK through the Locy parser (not just the Cypher parser).
2. Running the complete OpenCypher TCK through the Locy runtime (ensuring no behavioral divergence).

Any OpenCypher TCK scenario that fails when processed by the Locy parser/runtime is a Locy compliance failure, not an OpenCypher issue.

### 3.2 Extension Points (§21.2)

The only two modifications Locy makes to OpenCypher productions are:

1. **`<boolean primary>` extension** — adds `<is rule reference>` and `<is not rule reference>`.
2. **`<non-parenthesized value expression primary>` extension** — adds `<prev reference>`.

The TCK includes disambiguation tests to verify these extensions do not interfere with existing OpenCypher syntax (see §5 and §23).

---

## 4 Testing Levels

Each feature area is tested at three levels, progressing from surface syntax to deep semantics:

| Level | Name | What It Tests | Step Keywords |
|-------|------|---------------|---------------|
| **A** | Parse | Does the syntax parse correctly? Does invalid syntax fail? | `parsing`, `parse successfully`, `fail to parse` |
| **B** | Compile | Does the compiler accept valid programs and reject invalid ones? Are static checks correct? | `compiling`, `compile successfully`, `compile error`, `compile warning` |
| **C** | Evaluate | Does the runtime produce correct results? | `executing`, `deriving`, `querying`, result assertions |

Not all feature areas require all three levels. For example, `MODULE` declarations only require Level A and B (no runtime behavior beyond namespace resolution). Conversely, `ASSUME` requires all three (correct syntax, transaction semantics, rollback behavior).

---

## 5 Feature Suite: Lexical & Keywords

**Spec Reference:** §3
**Feature Files:** `locy/features/lexical/`

### 5.1 ReservedKeywords.feature

Tests that all Locy fully-reserved keywords (`RULE`, `ALONG`, `prev`, `FOLD`, `BEST`, `DERIVE`, `ASSUME`, `ABDUCE`, `QUERY`) are rejected as bare identifiers and accepted as backtick-quoted identifiers.

```gherkin
Feature: Locy Reserved Keywords

  Scenario Outline: Reject Locy reserved words as bare identifiers
    When parsing the following Locy program:
      """
      MATCH (n) WHERE n.<keyword> = 1 RETURN n
      """
    Then the program should fail to parse

    Examples:
      | keyword |
      | RULE    |
      | ALONG   |
      | prev    |
      | FOLD    |
      | BEST    |
      | DERIVE  |
      | ASSUME  |
      | ABDUCE  |
      | QUERY   |

  Scenario Outline: Accept Locy reserved words as backtick-quoted identifiers
    When parsing the following Locy program:
      """
      MATCH (n) WHERE n.`<keyword>` = 1 RETURN n
      """
    Then the program should parse successfully

    Examples:
      | keyword |
      | RULE    |
      | ALONG   |
      | prev    |
      | FOLD    |
      | DERIVE  |
      | ASSUME  |
      | ABDUCE  |
      | QUERY   |

  Scenario Outline: Contextual keywords remain usable as identifiers
    When parsing the following Locy program:
      """
      MATCH (n) WHERE n.<keyword> = 1 RETURN n
      """
    Then the program should parse successfully

    Examples:
      | keyword  |
      | MODULE   |
      | USE      |
      | PRIORITY |
      | KEY      |
      | NEW      |
      | EXPORT   |
```

### 5.2 CaseSensitivity.feature

```gherkin
Feature: Keyword Case Sensitivity

  Scenario: Keywords are case-insensitive
    When parsing the following Locy program:
      """
      create rule my_rule as
        match (a)-[:KNOWS]->(b)
        yield a, b
      """
    Then the program should parse successfully

  Scenario: Rule names are case-sensitive
    When compiling the following Locy program:
      """
      CREATE RULE myRule AS
        MATCH (a)-[:KNOWS]->(b)
        YIELD a, b

      MATCH (a), (b)
      WHERE a IS myrule TO b
      RETURN a, b
      """
    Then a compile error should be raised with code UNDEFINED_RULE
```

### 5.3 Identifiers.feature

```gherkin
Feature: Locy Identifiers

  Scenario: Rule names follow identifier rules
    When parsing the following Locy program:
      """
      CREATE RULE _private_rule AS
        MATCH (a)-[:E]->(b)
        YIELD a, b
      """
    Then the program should parse successfully

  Scenario: Rule names may contain digits (not leading)
    When parsing the following Locy program:
      """
      CREATE RULE rule2 AS
        MATCH (a)-[:E]->(b)
        YIELD a, b
      """
    Then the program should parse successfully

  Scenario: Reject rule names starting with digit
    When parsing the following Locy program:
      """
      CREATE RULE 2rule AS
        MATCH (a)-[:E]->(b)
        YIELD a, b
      """
    Then the program should fail to parse

  Scenario: Qualified rule names use dot notation
    When parsing the following Locy program:
      """
      MATCH (a), (b)
      WHERE a IS compliance.eu.control TO b
      RETURN a, b
      """
    Then the program should parse successfully
```

---

## 6 Feature Suite: Rule Definitions (CREATE RULE)

**Spec Reference:** §4
**Feature Files:** `locy/features/rules/`

### 6.1 RuleDefinition.feature — Parse Level

```gherkin
Feature: Rule Definition Parsing

  Scenario: Minimal rule definition
    When parsing the following Locy program:
      """
      CREATE RULE direct_reports AS
        MATCH (mgr:Person)-[:MANAGES]->(rep:Person)
        YIELD mgr, rep
      """
    Then the program should parse successfully

  Scenario: Rule with all optional clauses
    When parsing the following Locy program:
      """
      CREATE RULE shortest PRIORITY 2 AS
        MATCH (a:City)-[e:ROAD]->(b:City)
        WHERE b IS shortest TO a, e.distance > 0
        ALONG cost = prev.cost + e.distance INIT 0
        FOLD min_cost = MIN(cost)
        BEST BY MIN(min_cost)
        YIELD a, b KEY, min_cost
      """
    Then the program should parse successfully

  Scenario: Rule with DERIVE terminal instead of YIELD
    When parsing the following Locy program:
      """
      CREATE RULE controls AS
        MATCH (a:Company), (b:Company)
        WHERE (a, b, total) IS control, total > 0.5
        DERIVE (a)-[:CONTROLS { stake: total }]->(b)
      """
    Then the program should parse successfully

  Scenario: Multiple clauses for the same rule name
    When parsing the following Locy program:
      """
      CREATE RULE reachable AS
        MATCH (a)-[:KNOWS]->(b)
        YIELD a, b

      CREATE RULE reachable AS
        MATCH (a)-[:KNOWS]->(b)
        WHERE b IS reachable TO c
        YIELD a, c
      """
    Then the program should parse successfully

  Scenario: Reject rule definition without AS keyword
    When parsing the following Locy program:
      """
      CREATE RULE my_rule
        MATCH (a)-[:E]->(b)
        YIELD a, b
      """
    Then the program should fail to parse

  Scenario: Reject rule definition without MATCH
    When parsing the following Locy program:
      """
      CREATE RULE my_rule AS
        YIELD a, b
      """
    Then the program should fail to parse

  Scenario: Reject rule definition without terminal clause
    When parsing the following Locy program:
      """
      CREATE RULE my_rule AS
        MATCH (a)-[:E]->(b)
      """
    Then the program should fail to parse
```

### 6.2 RuleDefinition.feature — Compile Level

```gherkin
Feature: Rule Definition Compilation

  Scenario: Rule is registered but not immediately executed
    When compiling the following Locy program:
      """
      CREATE RULE my_rule AS
        MATCH (a:Nonexistent)-[:NOTHING]->(b)
        YIELD a, b
      """
    Then the program should compile successfully

  Scenario: Reject duplicate YIELD schema mismatch
    When compiling the following Locy program:
      """
      CREATE RULE r AS
        MATCH (a)-[:KNOWS]->(b)
        YIELD a, b

      CREATE RULE r AS
        MATCH (a)-[:WORKS_AT]->(b)
        YIELD a, b, b.name AS company
      """
    Then a compile error should be raised with code SCHEMA_MISMATCH

  Scenario: Accept compatible YIELD schemas (Integer and Float)
    When compiling the following Locy program:
      """
      CREATE RULE r AS
        MATCH (a)-[e:EDGE]->(b)
        YIELD a, b, 42 AS val

      CREATE RULE r AS
        MATCH (a)-[e:EDGE]->(b)
        YIELD a, b, 3.14 AS val
      """
    Then the program should compile successfully
```

### 6.3 RuleDefinition.feature — Evaluate Level

```gherkin
Feature: Rule Definition Evaluation

  Background:
    Given an empty graph
    And having executed:
      """
      CREATE (a:Person {name: 'Alice'})
      CREATE (b:Person {name: 'Bob'})
      CREATE (c:Person {name: 'Carol'})
      CREATE (a)-[:MANAGES]->(b)
      CREATE (a)-[:MANAGES]->(c)
      """

  Scenario: Simple non-recursive rule produces correct results
    When evaluating the following Locy program:
      """
      CREATE RULE direct_reports AS
        MATCH (mgr:Person)-[:MANAGES]->(rep:Person)
        YIELD mgr, rep

      DERIVE direct_reports
      MATCH (mgr), (rep) WHERE (mgr, rep) IS direct_reports
      RETURN mgr.name AS manager, rep.name AS report
      ORDER BY report
      """
    Then the result should be:
      | manager | report |
      | 'Alice' | 'Bob'  |
      | 'Alice' | 'Carol'|

  Scenario: Rule with no matching data returns empty
    When evaluating the following Locy program:
      """
      CREATE RULE empty_rule AS
        MATCH (a:Nonexistent)-[:NOTHING]->(b)
        YIELD a, b

      DERIVE empty_rule
      MATCH (a), (b) WHERE (a, b) IS empty_rule
      RETURN a, b
      """
    Then the result should be empty
```

### 6.4 YieldClause.feature

```gherkin
Feature: YIELD Clause

  Scenario: YIELD with alias
    When parsing the following Locy program:
      """
      CREATE RULE r AS
        MATCH (a)-[e:EDGE]->(b)
        YIELD a, b, e.weight AS w
      """
    Then the program should parse successfully

  Scenario: YIELD with KEY annotation
    When parsing the following Locy program:
      """
      CREATE RULE r AS
        MATCH (a)-[e:EDGE]->(b)
        YIELD a KEY, b KEY, e.weight AS w
      """
    Then the program should parse successfully

  Scenario: YIELD with expression
    When parsing the following Locy program:
      """
      CREATE RULE r AS
        MATCH (a)-[e:EDGE]->(b)
        YIELD a, b, e.weight * 2 AS double_weight
      """
    Then the program should parse successfully
```

---

## 7 Feature Suite: Rule References (IS / IS NOT)

**Spec Reference:** §5
**Feature Files:** `locy/features/rules/`

### 7.1 RuleReference_IS.feature — Parse Level

```gherkin
Feature: IS Rule Reference Parsing

  Scenario: Simple IS reference (node membership)
    When parsing the following Locy program:
      """
      CREATE RULE r AS
        MATCH (a)-[:E]->(b)
        WHERE b IS other_rule
        YIELD a, b
      """
    Then the program should parse successfully

  Scenario: IS with TO (binary relation)
    When parsing the following Locy program:
      """
      CREATE RULE r AS
        MATCH (a)-[:E]->(b)
        WHERE a IS reachable TO b
        YIELD a, b
      """
    Then the program should parse successfully

  Scenario: IS with value binding (tuple form)
    When parsing the following Locy program:
      """
      CREATE RULE r AS
        MATCH (a), (b)
        WHERE (a, b, cost) IS weighted_path
        YIELD a, b, cost
      """
    Then the program should parse successfully

  Scenario: IS with qualified rule name
    When parsing the following Locy program:
      """
      CREATE RULE r AS
        MATCH (a), (b)
        WHERE a IS compliance.eu.control TO b
        YIELD a, b
      """
    Then the program should parse successfully
```

### 7.2 RuleReference_IS_NOT.feature — Parse Level

```gherkin
Feature: IS NOT Rule Reference Parsing

  Scenario: IS NOT form
    When parsing the following Locy program:
      """
      CREATE RULE r AS
        MATCH (a)-[:E]->(b)
        WHERE b IS NOT blocked
        YIELD a, b
      """
    Then the program should parse successfully

  Scenario: NOT ... IS form
    When parsing the following Locy program:
      """
      CREATE RULE r AS
        MATCH (a)-[:E]->(b)
        WHERE NOT b IS blocked
        YIELD a, b
      """
    Then the program should parse successfully

  Scenario: IS NOT with TO
    When parsing the following Locy program:
      """
      CREATE RULE r AS
        MATCH (a)-[:E]->(b)
        WHERE a IS NOT blocked TO b
        YIELD a, b
      """
    Then the program should parse successfully

  Scenario: NOT ... IS with TO
    When parsing the following Locy program:
      """
      CREATE RULE r AS
        MATCH (a)-[:E]->(b)
        WHERE NOT a IS blocked TO b
        YIELD a, b
      """
    Then the program should parse successfully

  Scenario: IS NOT with tuple binding
    When parsing the following Locy program:
      """
      CREATE RULE r AS
        MATCH (a), (b)
        WHERE (a, b) IS NOT excluded
        YIELD a, b
      """
    Then the program should parse successfully
```

### 7.3 ISDisambiguation.feature

Tests that `IS` rule references do not collide with existing OpenCypher `IS` forms.

```gherkin
Feature: IS Disambiguation with OpenCypher

  Scenario: IS NULL is NOT an IS rule reference
    When parsing the following Locy program:
      """
      MATCH (n) WHERE n.name IS NULL RETURN n
      """
    Then the program should parse successfully
    And the parse tree should NOT contain an is_rule_reference node

  Scenario: IS NOT NULL is NOT an IS NOT rule reference
    When parsing the following Locy program:
      """
      MATCH (n) WHERE n.name IS NOT NULL RETURN n
      """
    Then the program should parse successfully
    And the parse tree should NOT contain an is_not_rule_reference node

  Scenario: IS :Label is NOT an IS rule reference
    When parsing the following Locy program:
      """
      MATCH (n) WHERE n IS :Person RETURN n
      """
    Then the program should parse successfully
    And the parse tree should NOT contain an is_rule_reference node

  Scenario: IS followed by identifier IS a rule reference
    When parsing the following Locy program:
      """
      CREATE RULE r AS
        MATCH (a)-[:E]->(b)
        WHERE b IS reachable
        YIELD a, b
      """
    Then the program should parse successfully
    And the parse tree should contain an is_rule_reference node
```

### 7.4 RuleReference.feature — Compile Level

```gherkin
Feature: IS Rule Reference Compilation

  Scenario: Reject reference to undefined rule
    When compiling the following Locy program:
      """
      CREATE RULE r AS
        MATCH (a)-[:E]->(b)
        WHERE b IS nonexistent_rule
        YIELD a, b
      """
    Then a compile error should be raised with code UNDEFINED_RULE

  Scenario: Reject cyclic negation (stratification violation)
    When compiling the following Locy program:
      """
      CREATE RULE a_rule AS
        MATCH (x)-[:E]->(y)
        WHERE y IS NOT b_rule
        YIELD x, y

      CREATE RULE b_rule AS
        MATCH (x)-[:E]->(y)
        WHERE y IS NOT a_rule
        YIELD x, y
      """
    Then a compile error should be raised with code CYCLIC_NEGATION

  Scenario: Accept positive mutual recursion
    When compiling the following Locy program:
      """
      CREATE RULE a_rule AS
        MATCH (x)-[:E]->(y)
        WHERE y IS b_rule
        YIELD x, y

      CREATE RULE b_rule AS
        MATCH (x)-[:F]->(y)
        WHERE y IS a_rule
        YIELD x, y
      """
    Then the program should compile successfully
```

### 7.5 RuleReference.feature — Evaluate Level

```gherkin
Feature: IS Rule Reference Evaluation

  Background:
    Given an empty graph
    And having executed:
      """
      CREATE (a:Person {name: 'Alice'})
      CREATE (b:Person {name: 'Bob'})
      CREATE (c:Person {name: 'Carol'})
      CREATE (d:Person {name: 'Dave'})
      CREATE (a)-[:KNOWS]->(b)
      CREATE (b)-[:KNOWS]->(c)
      CREATE (c)-[:KNOWS]->(d)
      """

  Scenario: Transitive closure via recursive IS
    When evaluating the following Locy program:
      """
      CREATE RULE reachable AS
        MATCH (a:Person)-[:KNOWS]->(b:Person)
        YIELD a, b

      CREATE RULE reachable AS
        MATCH (a:Person)-[:KNOWS]->(mid:Person)
        WHERE mid IS reachable TO b
        YIELD a, b

      DERIVE reachable
      MATCH (a:Person {name: 'Alice'}), (b:Person)
      WHERE a IS reachable TO b
      RETURN b.name AS name ORDER BY name
      """
    Then the result should be:
      | name    |
      | 'Bob'   |
      | 'Carol' |
      | 'Dave'  |

  Scenario: IS NOT excludes derived members
    When evaluating the following Locy program:
      """
      CREATE RULE connected AS
        MATCH (a:Person)-[:KNOWS]->(b:Person)
        YIELD a, b

      DERIVE connected
      MATCH (a:Person {name: 'Alice'}), (b:Person)
      WHERE b IS NOT connected TO a
      RETURN b.name AS name ORDER BY name
      """
    Then the result should contain:
      | name    |
      | 'Carol' |
      | 'Dave'  |

  Scenario: Self-referencing rule handles cycles
    Given an empty graph
    And having executed:
      """
      CREATE (a:Node {id: 1}), (b:Node {id: 2}), (c:Node {id: 3})
      CREATE (a)-[:LINK]->(b), (b)-[:LINK]->(c), (c)-[:LINK]->(a)
      """
    When evaluating the following Locy program:
      """
      CREATE RULE reachable AS
        MATCH (a:Node)-[:LINK]->(b:Node)
        YIELD a, b

      CREATE RULE reachable AS
        MATCH (a:Node)-[:LINK]->(mid:Node)
        WHERE mid IS reachable TO b
        YIELD a, b

      DERIVE reachable
      MATCH (a:Node {id: 1}), (b:Node)
      WHERE a IS reachable TO b
      RETURN b.id AS id ORDER BY id
      """
    Then the result should be:
      | id |
      | 1  |
      | 2  |
      | 3  |
```

---

## 8 Feature Suite: Path-Carried Values (ALONG / prev)

**Spec Reference:** §6
**Feature Files:** `locy/features/path_values/`

### 8.1 AlongClause.feature — Parse Level

```gherkin
Feature: ALONG Clause Parsing

  Scenario: Single ALONG variable
    When parsing the following Locy program:
      """
      CREATE RULE r AS
        MATCH (a)-[e:ROAD]->(b)
        ALONG cost = e.distance
        YIELD a, b, cost
      """
    Then the program should parse successfully

  Scenario: Multiple ALONG variables (comma-separated)
    When parsing the following Locy program:
      """
      CREATE RULE r AS
        MATCH (a)-[e:ROAD]->(b)
        ALONG cost = prev.cost + e.weight, hops = prev.hops + 1
        YIELD a, b, cost, hops
      """
    Then the program should parse successfully

  Scenario: ALONG with prev reference
    When parsing the following Locy program:
      """
      CREATE RULE r AS
        MATCH (a)-[e:EDGE]->(b)
        WHERE b IS r TO c
        ALONG cost = prev.cost + e.weight
        YIELD a, c, cost
      """
    Then the program should parse successfully

  Scenario: ALONG with complex expression
    When parsing the following Locy program:
      """
      CREATE RULE r AS
        MATCH (a)-[e:EDGE]->(b)
        ALONG path = prev.path + [b.name]
        YIELD a, b, path
      """
    Then the program should parse successfully
```

### 8.2 PrevReference.feature

```gherkin
Feature: prev Reference

  Scenario: prev disambiguates from user variable named prev
    When parsing the following Locy program:
      """
      MATCH (n {name: 'prev'}) RETURN n
      """
    Then the program should parse successfully
    And the parse tree should NOT contain a prev_reference node

  Scenario: prev inside ALONG is a prev_reference
    When parsing the following Locy program:
      """
      CREATE RULE r AS
        MATCH (a)-[e:E]->(b)
        WHERE b IS r TO c
        ALONG cost = prev.cost + e.weight
        YIELD a, c, cost
      """
    Then the parse tree should contain a prev_reference node

  Scenario: Reject prev in base case clause (no recursive IS)
    When compiling the following Locy program:
      """
      CREATE RULE r AS
        MATCH (a)-[e:E]->(b)
        ALONG cost = prev.cost + e.weight
        YIELD a, b, cost
      """
    Then a compile error should be raised with code PREV_IN_BASE_CASE
```

### 8.3 AlongClause.feature — Evaluate Level

```gherkin
Feature: ALONG Evaluation

  Background:
    Given an empty graph
    And having executed:
      """
      CREATE (a:City {name: 'A'})
      CREATE (b:City {name: 'B'})
      CREATE (c:City {name: 'C'})
      CREATE (a)-[:ROAD {distance: 10}]->(b)
      CREATE (b)-[:ROAD {distance: 20}]->(c)
      CREATE (a)-[:ROAD {distance: 50}]->(c)
      """

  Scenario: ALONG accumulates cost through recursive hops
    When evaluating the following Locy program:
      """
      CREATE RULE paths AS
        MATCH (a:City)-[e:ROAD]->(b:City)
        ALONG cost = e.distance
        YIELD a, b, cost

      CREATE RULE paths AS
        MATCH (a:City)-[e:ROAD]->(mid:City)
        WHERE mid IS paths TO b
        ALONG cost = prev.cost + e.distance
        YIELD a, b, cost

      DERIVE paths
      MATCH (a:City {name: 'A'}), (b:City {name: 'C'})
      WHERE (a, b, cost) IS paths
      RETURN cost ORDER BY cost
      """
    Then the result should be:
      | cost |
      | 30   |
      | 50   |

  Scenario: Per-hop filtering via prev (increasing weights)
    Given an empty graph
    And having executed:
      """
      CREATE (a:Node {id: 1}), (b:Node {id: 2}), (c:Node {id: 3}), (d:Node {id: 4})
      CREATE (a)-[:E {w: 1}]->(b)
      CREATE (b)-[:E {w: 3}]->(c)
      CREATE (c)-[:E {w: 2}]->(d)
      CREATE (b)-[:E {w: 5}]->(d)
      """
    When evaluating the following Locy program:
      """
      CREATE RULE increasing AS
        MATCH (a:Node)-[e:E]->(b:Node)
        ALONG weight = e.w
        YIELD a, b, weight

      CREATE RULE increasing AS
        MATCH (a:Node)-[e:E]->(b:Node)
        WHERE b IS increasing TO c,
              e.w > prev.weight
        ALONG weight = e.w
        YIELD a, c, weight

      DERIVE increasing
      MATCH (a:Node {id: 1}), (d:Node {id: 4})
      WHERE (a, d, w) IS increasing
      RETURN w
      """
    Then the result should not be empty

  Scenario: Bounded recursion via hop counter
    Given an empty graph
    And having executed:
      """
      CREATE (a:Node {id: 1})-[:LINK]->(b:Node {id: 2})-[:LINK]->(c:Node {id: 3})-[:LINK]->(d:Node {id: 4})-[:LINK]->(e:Node {id: 5})
      """
    When evaluating the following Locy program:
      """
      CREATE RULE bounded AS
        MATCH (a:Node)-[:LINK]->(b:Node)
        ALONG hops = 1
        YIELD a, b, hops

      CREATE RULE bounded AS
        MATCH (a:Node)-[:LINK]->(mid:Node)
        WHERE mid IS bounded TO b, prev.hops < 2
        ALONG hops = prev.hops + 1
        YIELD a, b, hops

      DERIVE bounded
      MATCH (start:Node {id: 1}), (end:Node)
      WHERE (start, end, hops) IS bounded
      RETURN end.id AS id, hops ORDER BY id
      """
    Then the result should be:
      | id | hops |
      | 2  | 1    |
      | 3  | 2    |
    And the result should NOT contain:
      | id | hops |
      | 4  | 3    |
```

---

## 9 Feature Suite: Aggregation (FOLD)

**Spec Reference:** §7
**Feature Files:** `locy/features/aggregation/`

### 9.1 FoldClause.feature — Parse Level

```gherkin
Feature: FOLD Clause Parsing

  Scenario: FOLD with COUNT
    When parsing the following Locy program:
      """
      CREATE RULE dept_size AS
        MATCH (d:Department)<-[:BELONGS_TO]-(e:Employee)
        FOLD count = COUNT(e)
        YIELD d, count
      """
    Then the program should parse successfully

  Scenario: FOLD with standard aggregation functions
    When parsing the following Locy program:
      """
      CREATE RULE stats AS
        MATCH (d:Dept)<-[:IN]-(e:Emp)
        FOLD total = SUM(e.salary), avg_sal = AVG(e.salary), top = MAX(e.salary)
        YIELD d, total, avg_sal, top
      """
    Then the program should parse successfully

  Scenario: FOLD over ALONG-carried values
    When parsing the following Locy program:
      """
      CREATE RULE total_cost AS
        MATCH (a), (b)
        WHERE (a, b, cost) IS weighted_path
        FOLD min_cost = MIN(cost)
        YIELD a, b, min_cost
      """
    Then the program should parse successfully
```

### 9.2 FoldClause.feature — Evaluate Level

```gherkin
Feature: FOLD Evaluation

  Background:
    Given an empty graph
    And having executed:
      """
      CREATE (d:Department {name: 'Engineering'})
      CREATE (e1:Employee {name: 'Alice', salary: 100000})
      CREATE (e2:Employee {name: 'Bob', salary: 90000})
      CREATE (e3:Employee {name: 'Carol', salary: 110000})
      CREATE (e1)-[:BELONGS_TO]->(d)
      CREATE (e2)-[:BELONGS_TO]->(d)
      CREATE (e3)-[:BELONGS_TO]->(d)
      """

  Scenario: FOLD COUNT aggregates correctly
    When evaluating the following Locy program:
      """
      CREATE RULE dept_size AS
        MATCH (d:Department)<-[:BELONGS_TO]-(e:Employee)
        FOLD count = COUNT(e)
        YIELD d, count

      DERIVE dept_size
      MATCH (d:Department) WHERE (d, count) IS dept_size
      RETURN d.name AS dept, count
      """
    Then the result should be:
      | dept          | count |
      | 'Engineering' | 3     |

  Scenario: FOLD MIN selects minimum value
    When evaluating the following Locy program:
      """
      CREATE RULE min_salary AS
        MATCH (d:Department)<-[:BELONGS_TO]-(e:Employee)
        FOLD lowest = MIN(e.salary)
        YIELD d, lowest

      DERIVE min_salary
      MATCH (d:Department) WHERE (d, lowest) IS min_salary
      RETURN lowest
      """
    Then the result should be:
      | lowest |
      | 90000  |

  Scenario: Grouping semantics — non-aggregated YIELD vars are the key
    Given an empty graph
    And having executed:
      """
      CREATE (d1:Dept {name: 'A'}), (d2:Dept {name: 'B'})
      CREATE (e1:Emp {sal: 10})-[:IN]->(d1)
      CREATE (e2:Emp {sal: 20})-[:IN]->(d1)
      CREATE (e3:Emp {sal: 30})-[:IN]->(d2)
      """
    When evaluating the following Locy program:
      """
      CREATE RULE dept_total AS
        MATCH (d:Dept)<-[:IN]-(e:Emp)
        FOLD total = SUM(e.sal)
        YIELD d, total

      DERIVE dept_total
      MATCH (d:Dept) WHERE (d, total) IS dept_total
      RETURN d.name AS dept, total ORDER BY dept
      """
    Then the result should be:
      | dept | total |
      | 'A'  | 30    |
      | 'B'  | 30    |
```

---

## 10 Feature Suite: Monotonic Aggregation

**Spec Reference:** §8
**Feature Files:** `locy/features/aggregation/`

### 10.1 MonotonicAggregation.feature — Parse Level

```gherkin
Feature: Monotonic Aggregation Parsing

  Scenario Outline: Monotonic operators parse correctly
    When parsing the following Locy program:
      """
      CREATE RULE r AS
        MATCH (a)-[e:E]->(b)
        WHERE b IS r TO c
        ALONG val = e.weight
        FOLD agg = <operator>(val)
        YIELD a, c, agg
      """
    Then the program should parse successfully

    Examples:
      | operator |
      | MSUM     |
      | MMAX     |
      | MMIN     |
      | MCOUNT   |
```

### 10.2 MonotonicAggregation.feature — Compile Level

```gherkin
Feature: Monotonic Aggregation Compilation

  Scenario: Reject non-monotonic operators in recursive FOLD
    When compiling the following Locy program:
      """
      CREATE RULE r AS
        MATCH (a)-[e:E]->(b)
        WHERE b IS r TO c
        FOLD avg_val = AVG(e.weight)
        YIELD a, c, avg_val
      """
    Then a compile error should be raised with code NON_MONOTONIC_IN_RECURSION

  Scenario: MSUM warns on potentially negative values
    When compiling the following Locy program:
      """
      CREATE RULE r AS
        MATCH (a)-[e:E]->(b)
        WHERE b IS r TO c
        ALONG val = e.weight - 10
        FOLD total = MSUM(val)
        YIELD a, c, total
      """
    Then a compile warning should be raised for MSUM_NON_NEGATIVITY

  Scenario: Reject multiple monotonic FOLDs for same key group
    When compiling the following Locy program:
      """
      CREATE RULE r AS
        MATCH (a)-[e:E]->(b)
        WHERE b IS r TO c
        FOLD total = MSUM(e.weight), mx = MMAX(e.weight)
        YIELD a, c, total, mx
      """
    Then a compile error should be raised with code MULTIPLE_MONOTONIC_FOLD
```

### 10.3 MonotonicAggregation.feature — Evaluate Level

```gherkin
Feature: Monotonic Aggregation Evaluation

  Scenario: MSUM accumulates across multi-path ownership
    Given an empty graph
    And having executed:
      """
      CREATE (a:Company {name: 'A'})
      CREATE (b:Company {name: 'B'})
      CREATE (c:Company {name: 'C'})
      CREATE (a)-[:OWNS {stake: 0.3}]->(b)
      CREATE (a)-[:OWNS {stake: 0.4}]->(c)
      CREATE (b)-[:OWNS {stake: 0.5}]->(c)
      """
    When evaluating the following Locy program:
      """
      CREATE RULE control AS
        MATCH (a:Company)-[r:OWNS]->(b:Company)
        ALONG stake = r.stake
        FOLD total = MSUM(stake)
        YIELD a, b, total

      CREATE RULE control AS
        MATCH (a:Company)-[r:OWNS]->(mid:Company)
        WHERE mid IS control TO c
        ALONG stake = r.stake * prev.stake
        FOLD total = MSUM(stake)
        YIELD a, c, total

      DERIVE control
      MATCH (a:Company {name: 'A'}), (c:Company {name: 'C'})
      WHERE (a, c, total) IS control
      RETURN total
      """
    Then the result should satisfy:
      | total |
      | 0.55  |
    # 0.4 (direct A->C) + 0.3*0.5 (A->B->C) = 0.55

  Scenario: MMAX converges to maximum across paths
    Given an empty graph
    And having executed:
      """
      CREATE (a:Node {id: 1}), (b:Node {id: 2}), (c:Node {id: 3})
      CREATE (a)-[:E {w: 5}]->(b)-[:E {w: 3}]->(c)
      CREATE (a)-[:E {w: 2}]->(c)
      """
    When evaluating the following Locy program:
      """
      CREATE RULE max_path AS
        MATCH (a:Node)-[e:E]->(b:Node)
        ALONG val = e.w
        FOLD mx = MMAX(val)
        YIELD a, b, mx

      CREATE RULE max_path AS
        MATCH (a:Node)-[e:E]->(mid:Node)
        WHERE mid IS max_path TO b
        ALONG val = e.w + prev.val
        FOLD mx = MMAX(val)
        YIELD a, b, mx

      DERIVE max_path
      MATCH (a:Node {id: 1}), (c:Node {id: 3})
      WHERE (a, c, mx) IS max_path
      RETURN mx
      """
    Then the result should satisfy:
      | mx |
      | 8  |
    # max(2, 5+3) = 8
```

---

## 11 Feature Suite: Optimized Selection (BEST BY)

**Spec Reference:** §9
**Feature Files:** `locy/features/aggregation/`

### 11.1 BestByClause.feature — Parse Level

```gherkin
Feature: BEST BY Parsing

  Scenario: BEST BY MIN
    When parsing the following Locy program:
      """
      CREATE RULE cheapest AS
        MATCH (a), (b)
        WHERE (a, b, cost, path) IS all_paths
        BEST BY MIN(cost)
        YIELD a, b, cost, path
      """
    Then the program should parse successfully

  Scenario: BEST BY MAX
    When parsing the following Locy program:
      """
      CREATE RULE strongest AS
        MATCH (a), (b)
        WHERE (a, b, stake) IS chains
        BEST BY MAX(stake)
        YIELD a, b, stake
      """
    Then the program should parse successfully

  Scenario: BEST BY with LIMIT
    When parsing the following Locy program:
      """
      CREATE RULE top3 AS
        MATCH (a), (b)
        WHERE (a, b, score) IS scored
        BEST BY MAX(score) LIMIT 3
        YIELD a, b, score
      """
    Then the program should parse successfully
```

### 11.2 BestByClause.feature — Compile Level

```gherkin
Feature: BEST BY Compilation

  Scenario: Reject BEST BY combined with monotonic FOLD
    When compiling the following Locy program:
      """
      CREATE RULE r AS
        MATCH (a)-[e:E]->(b)
        WHERE b IS r TO c
        FOLD total = MSUM(e.weight)
        BEST BY MIN(total)
        YIELD a, c, total
      """
    Then a compile error should be raised with code BEST_BY_WITH_MONOTONIC_FOLD

  Scenario: Reject BEST BY on non-ordered type
    When compiling the following Locy program:
      """
      CREATE RULE r AS
        MATCH (a)-[:E]->(b)
        BEST BY MIN(a)
        YIELD a, b
      """
    Then a compile error should be raised with code BEST_BY_NON_ORDERED_TYPE
```

### 11.3 BestByClause.feature — Evaluate Level

```gherkin
Feature: BEST BY Evaluation

  Background:
    Given an empty graph
    And having executed:
      """
      CREATE (a:City {name: 'A'}), (b:City {name: 'B'}), (c:City {name: 'C'})
      CREATE (a)-[:ROAD {dist: 10}]->(b)-[:ROAD {dist: 5}]->(c)
      CREATE (a)-[:ROAD {dist: 100}]->(c)
      """

  Scenario: BEST BY MIN selects cheapest path preserving witness
    When evaluating the following Locy program:
      """
      CREATE RULE all_paths AS
        MATCH (a:City)-[e:ROAD]->(b:City)
        ALONG cost = e.dist, via = [a.name]
        YIELD a, b, cost, via

      CREATE RULE all_paths AS
        MATCH (a:City)-[e:ROAD]->(mid:City)
        WHERE mid IS all_paths TO b
        ALONG cost = prev.cost + e.dist, via = prev.via + [mid.name]
        YIELD a, b, cost, via

      CREATE RULE cheapest AS
        MATCH (a), (b)
        WHERE (a, b, cost, via) IS all_paths
        BEST BY MIN(cost)
        YIELD a, b, cost, via

      DERIVE cheapest
      MATCH (a:City {name: 'A'}), (c:City {name: 'C'})
      WHERE (a, c, cost, via) IS cheapest
      RETURN cost, via
      """
    Then the result should be:
      | cost | via        |
      | 15   | ['A', 'B'] |
```

---

## 12 Feature Suite: Graph Derivation (DERIVE)

**Spec Reference:** §10
**Feature Files:** `locy/features/derivation/`

### 12.1 DeriveClause.feature — Parse Level

```gherkin
Feature: DERIVE Clause Parsing

  Scenario: Derive edge between existing nodes
    When parsing the following Locy program:
      """
      CREATE RULE r AS
        MATCH (a:Company), (b:Company)
        WHERE (a, b, total) IS control, total > 0.5
        DERIVE (a)-[:CONTROLS { stake: total }]->(b)
      """
    Then the program should parse successfully

  Scenario: Derive with backward edge direction
    When parsing the following Locy program:
      """
      CREATE RULE r AS
        MATCH (a), (b)
        DERIVE (a)<-[:FOLLOWS]-(b)
      """
    Then the program should parse successfully

  Scenario: Derive with NEW node
    When parsing the following Locy program:
      """
      CREATE RULE r AS
        MATCH (p:Person)
        WHERE NOT EXISTS { MATCH (p)-[:BORN_IN]->(:Country) }
        DERIVE (NEW c:Country:Inferred { _inferred: true })<-[:BORN_IN]-(p)
      """
    Then the program should parse successfully

  Scenario: DERIVE MERGE
    When parsing the following Locy program:
      """
      CREATE RULE same_entity AS
        MATCH (a:Person), (b:Person)
        WHERE a.ssn = b.ssn AND a <> b
        DERIVE MERGE a, b
      """
    Then the program should parse successfully
```

### 12.2 DeriveCommand.feature — Parse Level

```gherkin
Feature: DERIVE Command Parsing

  Scenario: Simple DERIVE command
    When parsing the following Locy program:
      """
      DERIVE control
      """
    Then the program should parse successfully

  Scenario: DERIVE command with WHERE filter
    When parsing the following Locy program:
      """
      DERIVE control WHERE a.name = 'Acme'
      """
    Then the program should parse successfully
```

### 12.3 DeriveClause.feature — Evaluate Level

```gherkin
Feature: DERIVE Evaluation

  Background:
    Given an empty graph
    And having executed:
      """
      CREATE (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'})
      CREATE (a)-[:KNOWS]->(b)
      """

  Scenario: Derived edges are queryable by standard patterns
    When evaluating the following Locy program:
      """
      CREATE RULE connected AS
        MATCH (a:Person)-[:KNOWS]->(b:Person)
        DERIVE (a)-[:CONNECTED_TO { _derived: true }]->(b)

      DERIVE connected
      MATCH (a:Person)-[:CONNECTED_TO]->(b:Person)
      RETURN a.name AS from, b.name AS to
      """
    Then the result should be:
      | from    | to    |
      | 'Alice' | 'Bob' |

  Scenario: Derived edges carry computed properties
    When evaluating the following Locy program:
      """
      CREATE RULE scored AS
        MATCH (a:Person)-[e:KNOWS]->(b:Person)
        DERIVE (a)-[:SCORED { score: 42, _derived: true }]->(b)

      DERIVE scored
      MATCH ()-[e:SCORED]->()
      RETURN e.score AS score
      """
    Then the result should be:
      | score |
      | 42    |

  Scenario: Derived edges are idempotent (no duplicates on re-derive)
    When evaluating the following Locy program:
      """
      CREATE RULE conn AS
        MATCH (a:Person)-[:KNOWS]->(b:Person)
        DERIVE (a)-[:CONN]->(b)

      DERIVE conn
      DERIVE conn

      MATCH ()-[e:CONN]->()
      RETURN count(e) AS cnt
      """
    Then the result should be:
      | cnt |
      | 1   |
```

---

## 13 Feature Suite: Existential Quantification (NEW)

**Spec Reference:** §11
**Feature Files:** `locy/features/derivation/`

### 13.1 ExistentialNew.feature — Compile Level

```gherkin
Feature: NEW Compilation

  Scenario: Wardedness violation detected
    When compiling a Locy program where a NEW-generated node is spread across
    multiple disjoint MATCH patterns in a downstream rule:
    Then a compile error should be raised with code WARDEDNESS_VIOLATION

  Scenario: Valid warded program compiles
    When compiling the following Locy program:
      """
      CREATE RULE has_owner AS
        MATCH (c:Company)
        WHERE NOT EXISTS { MATCH (c)<-[:OWNED_BY]-(:Person) }
        DERIVE (c)<-[:OWNED_BY]-(NEW p:Person:Inferred { _inferred: true })

      CREATE RULE owner_info AS
        MATCH (c:Company)<-[:OWNED_BY]-(p:Person)
        YIELD c, p
      """
    Then the program should compile successfully
```

### 13.2 ExistentialNew.feature — Evaluate Level

```gherkin
Feature: NEW Evaluation

  Background:
    Given an empty graph
    And having executed:
      """
      CREATE (c1:Company {name: 'Acme'})
      CREATE (c2:Company {name: 'Beta'})
      CREATE (p:Person {name: 'Alice'})-[:OWNS]->(c2)
      """

  Scenario: NEW creates inferred node with Skolem identity
    When evaluating the following Locy program:
      """
      CREATE RULE has_owner AS
        MATCH (c:Company)
        WHERE NOT EXISTS { MATCH (c)<-[:OWNS]-(:Person) }
        DERIVE (NEW p:Person:Inferred { _inferred: true })-[:OWNS]->(c)

      DERIVE has_owner
      MATCH (p:Inferred)-[:OWNS]->(c:Company)
      RETURN c.name AS company, p._inferred AS inferred
      """
    Then the result should be:
      | company | inferred |
      | 'Acme'  | true     |

  Scenario: Skolem idempotency — same rule + same input = same node
    When evaluating the following Locy program:
      """
      CREATE RULE has_owner AS
        MATCH (c:Company)
        WHERE NOT EXISTS { MATCH (c)<-[:OWNS]-(:Person) }
        DERIVE (NEW p:Person:Inferred { _inferred: true })-[:OWNS]->(c)

      DERIVE has_owner
      DERIVE has_owner

      MATCH (p:Inferred)
      RETURN count(p) AS cnt
      """
    Then the result should be:
      | cnt |
      | 1   |
```

---

## 14 Feature Suite: Entity Resolution (DERIVE MERGE)

**Spec Reference:** §12
**Feature Files:** `locy/features/derivation/`

### 14.1 DeriveMerge.feature — Evaluate Level

```gherkin
Feature: DERIVE MERGE Evaluation

  Background:
    Given an empty graph
    And having executed:
      """
      CREATE (a:Person {name: 'Alice', ssn: '123', email: 'alice@a.com'})
      CREATE (b:Person {name: 'Alice Smith', ssn: '123', phone: '555-0100'})
      CREATE (c:Person {name: 'Carol'})
      CREATE (a)-[:KNOWS]->(c)
      CREATE (b)-[:WORKS_WITH]->(c)
      """

  Scenario: DERIVE MERGE unifies nodes and transfers edges
    When evaluating the following Locy program:
      """
      CREATE RULE same_person AS
        MATCH (a:Person), (b:Person)
        WHERE a.ssn = b.ssn AND id(a) < id(b)
        DERIVE MERGE a, b

      DERIVE same_person

      MATCH (p:Person {ssn: '123'})
      RETURN count(p) AS cnt
      """
    Then the result should be:
      | cnt |
      | 1   |

  Scenario: Merged node retains edges from both original nodes
    When evaluating the following Locy program:
      """
      CREATE RULE same_person AS
        MATCH (a:Person), (b:Person)
        WHERE a.ssn = b.ssn AND id(a) < id(b)
        DERIVE MERGE a, b

      DERIVE same_person

      MATCH (p:Person {ssn: '123'})-[r]->(c:Person {name: 'Carol'})
      RETURN type(r) AS rel_type ORDER BY rel_type
      """
    Then the result should contain:
      | rel_type    |
      | 'KNOWS'     |
      | 'WORKS_WITH'|
```

---

## 15 Feature Suite: Prioritized Rules (PRIORITY)

**Spec Reference:** §13
**Feature Files:** `locy/features/rules/`

### 15.1 PrioritizedRules.feature — Parse Level

```gherkin
Feature: Prioritized Rules Parsing

  Scenario: Rule with PRIORITY clause
    When parsing the following Locy program:
      """
      CREATE RULE risk PRIORITY 1 AS
        MATCH (s:Supplier)
        YIELD s, 'low' AS level
      """
    Then the program should parse successfully

  Scenario: Multiple priority levels for same rule
    When parsing the following Locy program:
      """
      CREATE RULE risk PRIORITY 1 AS
        MATCH (s:Supplier)
        YIELD s, 'low' AS level

      CREATE RULE risk PRIORITY 2 AS
        MATCH (s:Supplier:Sanctioned)
        YIELD s, 'high' AS level
      """
    Then the program should parse successfully
```

### 15.2 PrioritizedRules.feature — Compile Level

```gherkin
Feature: Prioritized Rules Compilation

  Scenario: Reject mixing prioritized and non-prioritized clauses
    When compiling the following Locy program:
      """
      CREATE RULE r PRIORITY 1 AS
        MATCH (a)-[:E]->(b)
        YIELD a, b

      CREATE RULE r AS
        MATCH (a)-[:F]->(b)
        YIELD a, b
      """
    Then a compile error should be raised with code MIXED_PRIORITY

  Scenario: Reject non-positive priority values
    When parsing the following Locy program:
      """
      CREATE RULE r PRIORITY 0 AS
        MATCH (a)-[:E]->(b)
        YIELD a, b
      """
    Then the program should fail to parse

  Scenario: All priority clauses must share YIELD schema
    When compiling the following Locy program:
      """
      CREATE RULE r PRIORITY 1 AS
        MATCH (a)-[:E]->(b)
        YIELD a, b

      CREATE RULE r PRIORITY 2 AS
        MATCH (a)-[:E]->(b)
        YIELD a, b, 42 AS extra
      """
    Then a compile error should be raised with code SCHEMA_MISMATCH
```

### 15.3 PrioritizedRules.feature — Evaluate Level

```gherkin
Feature: Prioritized Rules Evaluation

  Background:
    Given an empty graph
    And having executed:
      """
      CREATE (s1:Supplier {name: 'NormalCo'})
      CREATE (s2:Supplier:Sanctioned {name: 'BadCo'})
      CREATE (s3:Supplier {name: 'RiskyCo'})
      CREATE (c:Country {risk: 'high'})
      CREATE (s3)-[:SOURCES_FROM]->(c)
      """

  Scenario: Higher priority overrides lower priority for same key
    When evaluating the following Locy program:
      """
      CREATE RULE risk_level PRIORITY 1 AS
        MATCH (s:Supplier)
        YIELD s, 'low' AS level

      CREATE RULE risk_level PRIORITY 2 AS
        MATCH (s:Supplier)-[:SOURCES_FROM]->(c:Country { risk: 'high' })
        YIELD s, 'medium' AS level

      CREATE RULE risk_level PRIORITY 3 AS
        MATCH (s:Supplier:Sanctioned)
        YIELD s, 'high' AS level

      DERIVE risk_level
      MATCH (s:Supplier)
      WHERE (s, level) IS risk_level
      RETURN s.name AS name, level ORDER BY name
      """
    Then the result should be:
      | name      | level    |
      | 'BadCo'   | 'high'   |
      | 'NormalCo'| 'low'    |
      | 'RiskyCo' | 'medium' |

  Scenario: Same priority level treats clauses as disjunctive (union)
    Given an empty graph
    And having executed:
      """
      CREATE (a:Node {id: 1, typeA: true})
      CREATE (b:Node {id: 2, typeB: true})
      """
    When evaluating the following Locy program:
      """
      CREATE RULE flagged PRIORITY 1 AS
        MATCH (n:Node) WHERE n.typeA = true
        YIELD n, 'A' AS reason

      CREATE RULE flagged PRIORITY 1 AS
        MATCH (n:Node) WHERE n.typeB = true
        YIELD n, 'B' AS reason

      DERIVE flagged
      MATCH (n:Node) WHERE (n, reason) IS flagged
      RETURN n.id AS id, reason ORDER BY id
      """
    Then the result should be:
      | id | reason |
      | 1  | 'A'    |
      | 2  | 'B'    |
```

---

## 16 Feature Suite: Goal-Directed Evaluation (QUERY)

**Spec Reference:** §14
**Feature Files:** `locy/features/reasoning/`

### 16.1 GoalDirectedQuery.feature — Parse Level

```gherkin
Feature: QUERY Parsing

  Scenario: Simple QUERY
    When parsing the following Locy program:
      """
      QUERY control
        WHERE a.name = 'Acme', c.name = 'TargetCo'
      """
    Then the program should parse successfully

  Scenario: QUERY with RETURN
    When parsing the following Locy program:
      """
      QUERY control
        WHERE a.name = 'Acme', c.name = 'TargetCo'
        RETURN a, c, total
      """
    Then the program should parse successfully

  Scenario: QUERY with qualified rule name
    When parsing the following Locy program:
      """
      QUERY compliance.eu.control
        WHERE a.name = 'Acme'
      """
    Then the program should parse successfully
```

### 16.2 GoalDirectedQuery.feature — Evaluate Level

```gherkin
Feature: QUERY Evaluation

  Background:
    Given an empty graph
    And having executed:
      """
      CREATE (a:Person {name: 'Alice'})
      CREATE (b:Person {name: 'Bob'})
      CREATE (c:Person {name: 'Carol'})
      CREATE (a)-[:KNOWS]->(b)-[:KNOWS]->(c)
      """

  Scenario: QUERY produces same results as DERIVE for point queries
    When evaluating the following Locy program:
      """
      CREATE RULE reachable AS
        MATCH (a:Person)-[:KNOWS]->(b:Person)
        YIELD a, b

      CREATE RULE reachable AS
        MATCH (a:Person)-[:KNOWS]->(mid:Person)
        WHERE mid IS reachable TO b
        YIELD a, b

      QUERY reachable
        WHERE a.name = 'Alice', b.name = 'Carol'
        RETURN a.name AS from, b.name AS to
      """
    Then the result should be:
      | from    | to      |
      | 'Alice' | 'Carol' |

  Scenario: QUERY with non-existent goal returns empty
    When evaluating the following Locy program:
      """
      CREATE RULE reachable AS
        MATCH (a:Person)-[:KNOWS]->(b:Person)
        YIELD a, b

      QUERY reachable
        WHERE a.name = 'Carol', b.name = 'Alice'
        RETURN a.name, b.name
      """
    Then the result should be empty

  Scenario: QUERY handles recursion with tabling (DAG subgoals)
    Given an empty graph
    And having executed:
      """
      CREATE (a:Node {id: 1}), (b:Node {id: 2}), (c:Node {id: 3}), (d:Node {id: 4})
      CREATE (a)-[:E]->(b), (a)-[:E]->(c), (b)-[:E]->(d), (c)-[:E]->(d)
      """
    When evaluating the following Locy program:
      """
      CREATE RULE reach AS
        MATCH (x:Node)-[:E]->(y:Node)
        YIELD x, y

      CREATE RULE reach AS
        MATCH (x:Node)-[:E]->(z:Node)
        WHERE z IS reach TO y
        YIELD x, y

      QUERY reach
        WHERE x.id = 1, y.id = 4
        RETURN x.id AS from, y.id AS to
      """
    Then the result should be:
      | from | to |
      | 1    | 4  |
```

---

## 17 Feature Suite: Hypothetical Reasoning (ASSUME)

**Spec Reference:** §15
**Feature Files:** `locy/features/reasoning/`

### 17.1 HypotheticalAssume.feature — Parse Level

```gherkin
Feature: ASSUME Parsing

  Scenario: ASSUME with SET mutation
    When parsing the following Locy program:
      """
      ASSUME {
        MATCH (s:Server { name: 'db-primary' })
        SET s.status = 'DOWN'
      }
      THEN
        MATCH (s:Server { status: 'DOWN' })
        RETURN s.name
      """
    Then the program should parse successfully

  Scenario: ASSUME with CREATE mutation
    When parsing the following Locy program:
      """
      ASSUME {
        CREATE (n:Temp {val: 1})
      }
      THEN
        MATCH (n:Temp) RETURN n.val
      """
    Then the program should parse successfully

  Scenario: ASSUME with DELETE mutation
    When parsing the following Locy program:
      """
      ASSUME {
        MATCH (n:Node {id: 1})
        DELETE n
      }
      THEN
        MATCH (n:Node) RETURN count(n) AS cnt
      """
    Then the program should parse successfully

  Scenario: ASSUME with multiple mutations
    When parsing the following Locy program:
      """
      ASSUME {
        MATCH (a:Server {name: 'db-primary'}) SET a.status = 'DOWN'
        CREATE (b:Server {name: 'db-failover', status: 'UP'})
      }
      THEN
        MATCH (s:Server {status: 'UP'}) RETURN s.name
      """
    Then the program should parse successfully
```

### 17.2 HypotheticalAssume.feature — Evaluate Level

```gherkin
Feature: ASSUME Evaluation

  Background:
    Given an empty graph
    And having executed:
      """
      CREATE (s:Server {name: 'db-primary', status: 'UP'})
      CREATE (svc:Service {name: 'api', tier: 'critical'})
      CREATE (svc)-[:DEPENDS_ON]->(s)
      """

  Scenario: ASSUME mutations are visible in THEN clause
    When evaluating the following Locy program:
      """
      ASSUME {
        MATCH (s:Server {name: 'db-primary'})
        SET s.status = 'DOWN'
      }
      THEN
        MATCH (s:Server {name: 'db-primary'})
        RETURN s.status AS status
      """
    Then the result should be:
      | status |
      | 'DOWN' |

  Scenario: ASSUME does NOT modify persistent state
    When evaluating the following Locy program:
      """
      ASSUME {
        MATCH (s:Server {name: 'db-primary'})
        SET s.status = 'DOWN'
      }
      THEN
        RETURN 1 AS dummy
      """
    Then having executed after the ASSUME block:
      """
      MATCH (s:Server {name: 'db-primary'})
      RETURN s.status AS status
      """
    Then the result should be:
      | status |
      | 'UP'   |

  Scenario: ASSUME with rule evaluation in THEN
    When evaluating the following Locy program:
      """
      CREATE RULE impacted AS
        MATCH (svc:Service)-[:DEPENDS_ON]->(dep)
        WHERE dep.status = 'DOWN'
        YIELD svc

      ASSUME {
        MATCH (s:Server {name: 'db-primary'})
        SET s.status = 'DOWN'
      }
      THEN
        MATCH (svc:Service)
        WHERE svc IS impacted
        RETURN svc.name AS affected
      """
    Then the result should be:
      | affected |
      | 'api'    |

  Scenario: Nested ASSUME blocks
    When evaluating the following Locy program:
      """
      ASSUME {
        MATCH (s:Server {name: 'db-primary'})
        SET s.status = 'DEGRADED'
      }
      THEN
        ASSUME {
          MATCH (s:Server {name: 'db-primary'})
          SET s.status = 'DOWN'
        }
        THEN
          MATCH (s:Server {name: 'db-primary'})
          RETURN s.status AS status
      """
    Then the result should be:
      | status |
      | 'DOWN' |
```

---

## 18 Feature Suite: Abductive Reasoning (ABDUCE)

**Spec Reference:** §16
**Feature Files:** `locy/features/reasoning/`

### 18.1 AbductiveReasoning.feature — Parse Level

```gherkin
Feature: ABDUCE Parsing

  Scenario: ABDUCE NOT (find changes to make conclusion false)
    When parsing the following Locy program:
      """
      ABDUCE NOT sanctioned_exposure
        WHERE target.name = 'TargetCo'
        MINIMIZE changes
        BUDGET 3
        LIMIT 5
      """
    Then the program should parse successfully

  Scenario: ABDUCE positive (find changes to make conclusion true)
    When parsing the following Locy program:
      """
      ABDUCE redundant_path
        WHERE source.name = 'api', target.name = 'db'
        BUDGET 2
      """
    Then the program should parse successfully

  Scenario: ABDUCE without optional clauses
    When parsing the following Locy program:
      """
      ABDUCE NOT can_access
        WHERE u.name = 'Z', res.name = 'prod-db'
      """
    Then the program should parse successfully
```

### 18.2 AbductiveReasoning.feature — Evaluate Level

```gherkin
Feature: ABDUCE Evaluation

  Background:
    Given an empty graph
    And having executed:
      """
      CREATE (u:User {name: 'Z'})
      CREATE (g:Group {name: 'DevOps'})
      CREATE (r:Role {name: 'admin'})
      CREATE (res:Resource {name: 'prod-db'})
      CREATE (u)-[:MEMBER_OF]->(g)
      CREATE (g)-[:HAS_ROLE]->(r)
      CREATE (r)-[:PERMITS]->(res)
      """

  Scenario: ABDUCE NOT finds single-edge removal to revoke access
    When evaluating the following Locy program:
      """
      CREATE RULE can_access AS
        MATCH (u:User)-[:MEMBER_OF]->(g:Group)-[:HAS_ROLE]->(r:Role)-[:PERMITS]->(res:Resource)
        YIELD u, res

      ABDUCE NOT can_access
        WHERE u.name = 'Z', res.name = 'prod-db'
        BUDGET 1
        LIMIT 3
      """
    Then the ABDUCE result should contain at least one modification
    And each modification should be a valid Cypher mutation statement

  Scenario: ABDUCE results are advisory (not executed)
    When evaluating the following Locy program:
      """
      CREATE RULE can_access AS
        MATCH (u:User)-[:MEMBER_OF]->(g:Group)-[:HAS_ROLE]->(r:Role)-[:PERMITS]->(res:Resource)
        YIELD u, res

      ABDUCE NOT can_access
        WHERE u.name = 'Z', res.name = 'prod-db'
        BUDGET 1
      """
    Then having executed after the ABDUCE:
      """
      MATCH (u:User {name: 'Z'})-[:MEMBER_OF]->(g:Group)
      RETURN count(g) AS cnt
      """
    Then the result should be:
      | cnt |
      | 1   |
```

---

## 19 Feature Suite: Proof Traces (EXPLAIN RULE)

**Spec Reference:** §17
**Feature Files:** `locy/features/reasoning/`

### 19.1 ExplainRule.feature — Parse Level

```gherkin
Feature: EXPLAIN RULE Parsing

  Scenario: Simple EXPLAIN RULE
    When parsing the following Locy program:
      """
      EXPLAIN RULE control
        WHERE a.name = 'Acme', c.name = 'TargetCo'
      """
    Then the program should parse successfully

  Scenario: EXPLAIN RULE with RETURN
    When parsing the following Locy program:
      """
      EXPLAIN RULE sanctioned_exposure
        WHERE target.name = 'TargetCo'
        RETURN *
      """
    Then the program should parse successfully
```

### 19.2 ExplainRule.feature — Evaluate Level

```gherkin
Feature: EXPLAIN RULE Evaluation

  Background:
    Given an empty graph
    And having executed:
      """
      CREATE (a:Company {name: 'Acme'})
      CREATE (b:Company {name: 'MidCo'})
      CREATE (c:Company {name: 'TargetCo'})
      CREATE (a)-[:OWNS {stake: 0.6}]->(b)
      CREATE (b)-[:OWNS {stake: 0.5}]->(c)
      """

  Scenario: EXPLAIN produces a derivation tree
    When evaluating the following Locy program:
      """
      CREATE RULE control AS
        MATCH (a:Company)-[r:OWNS]->(b:Company)
        ALONG stake = r.stake
        YIELD a, b, stake

      CREATE RULE control AS
        MATCH (a:Company)-[r:OWNS]->(mid:Company)
        WHERE mid IS control TO b
        ALONG stake = r.stake * prev.stake
        YIELD a, b, stake

      EXPLAIN RULE control
        WHERE a.name = 'Acme', b.name = 'TargetCo'
      """
    Then the derivation tree should not be empty
    And the derivation tree should contain a leaf referencing base fact OWNS(Acme, MidCo)
    And the derivation tree should contain a leaf referencing base fact OWNS(MidCo, TargetCo)
```

---

## 20 Feature Suite: Modules and Composition (USE)

**Spec Reference:** §18
**Feature Files:** `locy/features/modules/`

### 20.1 ModuleDeclaration.feature — Parse Level

```gherkin
Feature: Module Declaration Parsing

  Scenario: Simple MODULE declaration
    When parsing the following Locy program:
      """
      MODULE compliance.eu.ownership
      """
    Then the program should parse successfully

  Scenario: MODULE with EXPORT
    When parsing the following Locy program:
      """
      MODULE compliance.eu.ownership
      EXPORT control, sanctioned_exposure

      CREATE RULE control AS
        MATCH (a)-[:OWNS]->(b)
        YIELD a, b
      """
    Then the program should parse successfully

  Scenario: MODULE must appear before any statements
    When parsing the following Locy program:
      """
      CREATE RULE r AS MATCH (a)-[:E]->(b) YIELD a, b
      MODULE my.module
      """
    Then the program should fail to parse
```

### 20.2 UseImport.feature — Parse Level

```gherkin
Feature: USE Import Parsing

  Scenario: Simple USE
    When parsing the following Locy program:
      """
      USE compliance.eu.ownership
      """
    Then the program should parse successfully

  Scenario: USE with version constraint
    When parsing the following Locy program:
      """
      USE compliance.eu.ownership@v2.3
      """
    Then the program should parse successfully

  Scenario: Multiple USE declarations
    When parsing the following Locy program:
      """
      USE compliance.eu.ownership
      USE compliance.us.sec_reporting
      USE infra.dependency_analysis

      MATCH (n) RETURN n
      """
    Then the program should parse successfully

  Scenario: USE must appear before statements (but after MODULE)
    When parsing the following Locy program:
      """
      MODULE my.module
      USE other.module

      CREATE RULE r AS MATCH (a)-[:E]->(b) YIELD a, b
      """
    Then the program should parse successfully
```

### 20.3 QualifiedNames.feature — Compile Level

```gherkin
Feature: Qualified Name Resolution

  Scenario: Ambiguous unqualified name requires qualification
    When compiling a Locy program that imports two modules both exporting
    a rule named 'control' and references 'control' without qualification:
    Then a compile error should be raised with code AMBIGUOUS_RULE_NAME

  Scenario: Qualified names resolve correctly
    When compiling a Locy program that imports two modules both exporting
    'control' and references 'module_a.control':
    Then the program should compile successfully

  Scenario: Reject reference to non-exported rule
    When compiling a Locy program that references 'module.internal_helper'
    where 'internal_helper' is not in the EXPORT list:
    Then a compile error should be raised with code RULE_NOT_EXPORTED

  Scenario: Stratification spans across modules
    When compiling a Locy program where module A negates a rule from module B,
    and module B positively references a rule from module A:
    Then a compile error should be raised with code CYCLIC_NEGATION
```

---

## 21 Feature Suite: Evaluation Model & Semantics

**Spec Reference:** §19
**Feature Files:** `locy/features/evaluation/`

### 21.1 Stratification.feature

```gherkin
Feature: Stratification

  Scenario: Positive dependencies in same stratum
    When compiling the following Locy program:
      """
      CREATE RULE a AS MATCH (x)-[:E]->(y) WHERE y IS b YIELD x, y
      CREATE RULE b AS MATCH (x)-[:F]->(y) WHERE y IS a YIELD x, y
      """
    Then rule a should be in the same stratum as rule b

  Scenario: IS NOT forces higher stratum
    When compiling the following Locy program:
      """
      CREATE RULE base AS MATCH (x)-[:E]->(y) YIELD x, y

      CREATE RULE filtered AS
        MATCH (x)-[:F]->(y)
        WHERE y IS NOT base
        YIELD x, y
      """
    Then rule filtered should be in a strictly higher stratum than rule base

  Scenario: Non-monotonic aggregation forces higher stratum
    When compiling the following Locy program:
      """
      CREATE RULE paths AS
        MATCH (a)-[:E]->(b)
        YIELD a, b

      CREATE RULE path_count AS
        MATCH (a), (b) WHERE (a, b) IS paths
        FOLD cnt = COUNT(*)
        YIELD a, b, cnt
      """
    Then rule path_count should be in a strictly higher stratum than rule paths
```

### 21.2 SemiNaiveEvaluation.feature

```gherkin
Feature: Semi-Naive Evaluation Correctness

  Scenario: Fixpoint converges and produces complete transitive closure
    Given an empty graph
    And having executed:
      """
      CREATE (a:N {id:1})-[:E]->(b:N {id:2})-[:E]->(c:N {id:3})-[:E]->(d:N {id:4})
      """
    When evaluating the following Locy program:
      """
      CREATE RULE reach AS
        MATCH (x:N)-[:E]->(y:N) YIELD x, y

      CREATE RULE reach AS
        MATCH (x:N)-[:E]->(z:N)
        WHERE z IS reach TO y
        YIELD x, y

      DERIVE reach
      MATCH (a:N), (b:N) WHERE a IS reach TO b
      RETURN a.id AS from, b.id AS to ORDER BY from, to
      """
    Then the result should be:
      | from | to |
      | 1    | 2  |
      | 1    | 3  |
      | 1    | 4  |
      | 2    | 3  |
      | 2    | 4  |
      | 3    | 4  |

  Scenario: Post-fixpoint operations applied in correct order
    # Priority filtering -> BEST BY selection -> DERIVE execution
    # Detailed scenario testing that priority filtering happens before
    # BEST BY selection, which happens before DERIVE materialization
    Given an empty graph
    And having executed:
      """
      CREATE (a:Node {id: 1}), (b:Node {id: 2})
      CREATE (a)-[:E {w: 10}]->(b)
      CREATE (a)-[:F {w: 5}]->(b)
      """
    When evaluating the following Locy program:
      """
      CREATE RULE scored PRIORITY 1 AS
        MATCH (a:Node)-[e:E]->(b:Node)
        YIELD a, b, e.w AS score

      CREATE RULE scored PRIORITY 2 AS
        MATCH (a:Node)-[e:F]->(b:Node)
        YIELD a, b, e.w AS score

      DERIVE scored
      MATCH (a:Node {id: 1}), (b:Node {id: 2})
      WHERE (a, b, score) IS scored
      RETURN score
      """
    Then the result should be:
      | score |
      | 5     |
```

---

## 22 Feature Suite: Type System

**Spec Reference:** §20
**Feature Files:** `locy/features/type_system/`

### 22.1 TypeConstraints.feature

```gherkin
Feature: Type System Constraints

  Scenario: ALONG arithmetic requires numeric types
    When compiling the following Locy program:
      """
      CREATE RULE r AS
        MATCH (a)-[e:E]->(b)
        ALONG val = e.name + 1
        YIELD a, b, val
      """
    Then a compile warning should be raised for TYPE_MISMATCH
    # (if e.name is string, runtime error; compiler may warn)

  Scenario: BEST BY rejects non-ordered types
    When compiling the following Locy program:
      """
      CREATE RULE r AS
        MATCH (a)-[:E]->(b)
        BEST BY MIN([1,2,3])
        YIELD a, b
      """
    Then a compile error should be raised with code BEST_BY_NON_ORDERED_TYPE

  Scenario: Integer and Float are compatible in YIELD schema
    When compiling the following Locy program:
      """
      CREATE RULE r AS
        MATCH (a)-[e:E]->(b)
        YIELD a, b, 42 AS val

      CREATE RULE r AS
        MATCH (a)-[e:F]->(b)
        YIELD a, b, 3.14 AS val
      """
    Then the program should compile successfully
```

---

## 23 Feature Suite: OpenCypher Superset Guarantee

**Spec Reference:** §1.3, §21.2
**Feature Files:** `locy/features/superset/`

### 23.1 SupersetGuarantee.feature

```gherkin
Feature: OpenCypher Superset Guarantee

  Scenario: Pure Cypher program parses as valid Locy program
    When parsing the following Locy program:
      """
      MATCH (n:Person)-[:KNOWS]->(m:Person)
      WHERE n.age > 30
      RETURN n.name, m.name, n.age
      ORDER BY n.age DESC
      LIMIT 10
      """
    Then the program should parse successfully

  Scenario: Cypher IS NULL is not affected by Locy IS extension
    When parsing the following Locy program:
      """
      MATCH (n) WHERE n.email IS NULL RETURN n
      """
    Then the program should parse successfully
    And the program should produce identical results to OpenCypher

  Scenario: Cypher IS NOT NULL is not affected by Locy IS NOT extension
    When parsing the following Locy program:
      """
      MATCH (n) WHERE n.email IS NOT NULL RETURN n.email
      """
    Then the program should parse successfully
    And the program should produce identical results to OpenCypher

  Scenario: Cypher IS :Label is not affected by Locy IS extension
    When parsing the following Locy program:
      """
      MATCH (n) WHERE n IS :Person RETURN n
      """
    Then the program should parse successfully

  Scenario: Cypher CREATE statement still works (not confused with CREATE RULE)
    When parsing the following Locy program:
      """
      CREATE (n:Person {name: 'Alice'})
      """
    Then the program should parse successfully
    And the parse tree should contain a create_statement node
    And the parse tree should NOT contain a rule_definition node

  Scenario: Cypher EXPLAIN still works (not confused with EXPLAIN RULE)
    When parsing the following Locy program:
      """
      EXPLAIN MATCH (n) RETURN n
      """
    Then the program should parse successfully
```

### 23.2 OpenCypherTCKRegression.feature

```gherkin
Feature: OpenCypher TCK Regression Through Locy Parser

  # This is a meta-feature: the test runner should execute the ENTIRE
  # OpenCypher TCK (all 3,856+ scenarios from the upstream feature files)
  # through the Locy parser and runtime, verifying zero regressions.

  Scenario: All OpenCypher TCK parse scenarios pass through Locy parser
    Given the complete OpenCypher TCK feature suite
    When all parse scenarios are run through the Locy parser
    Then the Locy parser pass rate should equal the Cypher parser pass rate

  Scenario: All OpenCypher TCK evaluation scenarios pass through Locy runtime
    Given the complete OpenCypher TCK feature suite
    When all evaluation scenarios are run through the Locy runtime
    Then the Locy runtime pass rate should equal the Cypher runtime pass rate
```

---

## 24 Feature Suite: Integration Scenarios

**Spec Reference:** §23
**Feature Files:** `locy/features/integration/`

These scenarios test multi-feature interaction using the complete examples from the specification.

### 24.1 CorporateOwnership.feature

Tests the full pipeline from §23.1: recursive ownership with MSUM, DERIVE, QUERY, EXPLAIN, and ABDUCE.

```gherkin
Feature: Corporate Ownership Control Pipeline

  Background:
    Given an empty graph
    And having executed:
      """
      CREATE (acme:Company:Sanctioned {name: 'Acme'})
      CREATE (mid:Company {name: 'MidCo'})
      CREATE (target:Company {name: 'TargetCo'})
      CREATE (acme)-[:OWNS {stake: 0.6}]->(mid)
      CREATE (mid)-[:OWNS {stake: 0.5}]->(target)
      CREATE (acme)-[:OWNS {stake: 0.1}]->(target)
      """

  Scenario: Full ownership pipeline — MSUM, DERIVE, QUERY
    When evaluating the following Locy program:
      """
      CREATE RULE control AS
        MATCH (a:Company)-[r:OWNS]->(b:Company)
        ALONG stake = r.stake
        FOLD total = MSUM(stake)
        YIELD a, b, total

      CREATE RULE control AS
        MATCH (a:Company)-[r:OWNS]->(mid:Company)
        WHERE mid IS control TO c
        ALONG stake = r.stake * prev.stake
        FOLD total = MSUM(stake)
        YIELD a, c, total

      QUERY control
        WHERE a.name = 'Acme', c.name = 'TargetCo'
        RETURN total
      """
    Then the result should satisfy:
      | total |
      | 0.4   |
    # 0.1 (direct) + 0.6*0.5 (via MidCo) = 0.4
```

### 24.2 InfrastructureCascade.feature

Tests the cascade analysis from §23.2: ASSUME + rule evaluation + ABDUCE.

```gherkin
Feature: Infrastructure Cascade Analysis

  Background:
    Given an empty graph
    And having executed:
      """
      CREATE (db:Service {name: 'postgres-primary', status: 'UP', tier: 'critical'})
      CREATE (api:Service {name: 'api', status: 'UP', tier: 'critical'})
      CREATE (web:Service {name: 'web', status: 'UP', tier: 'standard'})
      CREATE (api)-[:DEPENDS_ON]->(db)
      CREATE (web)-[:DEPENDS_ON]->(api)
      """

  Scenario: ASSUME simulates outage and evaluates cascading impact
    When evaluating the following Locy program:
      """
      CREATE RULE cascade_impact AS
        MATCH (a:Service)-[:DEPENDS_ON]->(b:Service)
        WHERE b.status = 'DOWN'
        YIELD a, b

      CREATE RULE cascade_impact AS
        MATCH (a:Service)-[:DEPENDS_ON]->(b:Service)
        WHERE b IS cascade_impact
        YIELD a, b

      ASSUME {
        MATCH (db:Service {name: 'postgres-primary'})
        SET db.status = 'DOWN'
      }
      THEN
        MATCH (affected:Service)
        WHERE affected IS cascade_impact
        RETURN affected.name AS name ORDER BY name
      """
    Then the result should be:
      | name  |
      | 'api' |
      | 'web' |
```

### 24.3 RBACAuthorization.feature

Tests the RBAC pipeline from §23.3: recursive group membership + PRIORITY + EXPLAIN.

```gherkin
Feature: RBAC Authorization with Priority

  Background:
    Given an empty graph
    And having executed:
      """
      CREATE (alice:User {name: 'Alice'})
      CREATE (bob:User:Admin {name: 'Bob'})
      CREATE (dev:Group {name: 'Dev'})
      CREATE (viewer:Role {name: 'viewer'})
      CREATE (prod:Resource {name: 'prod-db'})
      CREATE (alice)-[:MEMBER_OF]->(dev)
      CREATE (dev)-[:HAS_ROLE]->(viewer)
      CREATE (viewer)-[:PERMITS]->(prod)
      CREATE (alice)-[:DENIED]->(prod)
      CREATE (bob)-[:FORCE_ACCESS]->(prod)
      """

  Scenario: DENY overrides ALLOW via priority
    When evaluating the following Locy program:
      """
      CREATE RULE can_access PRIORITY 1 AS
        MATCH (u:User)-[:MEMBER_OF]->(g:Group)-[:HAS_ROLE]->(r:Role)-[:PERMITS]->(res:Resource)
        YIELD u, res, 'ALLOW' AS decision

      CREATE RULE can_access PRIORITY 2 AS
        MATCH (u:User)-[:DENIED]->(res:Resource)
        YIELD u, res, 'DENY' AS decision

      CREATE RULE can_access PRIORITY 3 AS
        MATCH (u:User:Admin)-[:FORCE_ACCESS]->(res:Resource)
        YIELD u, res, 'ALLOW' AS decision

      DERIVE can_access
      MATCH (u:User), (res:Resource {name: 'prod-db'})
      WHERE (u, res, decision) IS can_access
      RETURN u.name AS user, decision ORDER BY user
      """
    Then the result should be:
      | user    | decision |
      | 'Alice' | 'DENY'   |
      | 'Bob'   | 'ALLOW'  |
```

---

## 25 Error & Rejection Scenarios

**Feature Files:** `locy/features/errors/`

This section consolidates all expected compile-time and parse-time error scenarios, providing a reference for error code coverage.

### 25.1 Error Code Reference

| Error Code | Trigger | Spec Section |
|---|---|---|
| `SCHEMA_MISMATCH` | Multiple clauses for same rule with different YIELD schemas | §4.6, §20.2 |
| `UNDEFINED_RULE` | IS reference to a rule name that has not been defined | §5 |
| `CYCLIC_NEGATION` | IS NOT creates a dependency cycle between rules | §5.3, §19.1 |
| `PREV_IN_BASE_CASE` | `prev` reference in a non-recursive clause | §6.3 |
| `NON_MONOTONIC_IN_RECURSION` | Non-monotonic aggregation (AVG, MEDIAN) in recursive FOLD | §8.6 |
| `MSUM_NON_NEGATIVITY` | MSUM on expression that may be negative (warning) | §8.6, §20.4 |
| `MULTIPLE_MONOTONIC_FOLD` | Two monotonic FOLDs for the same key group in one rule | §8.6 |
| `BEST_BY_WITH_MONOTONIC_FOLD` | BEST BY and monotonic FOLD in same clause | §9.7 |
| `BEST_BY_NON_ORDERED_TYPE` | BEST BY on a type without natural ordering | §20.5 |
| `WARDEDNESS_VIOLATION` | NEW-generated nodes violate the wardedness condition | §11.5 |
| `MIXED_PRIORITY` | Same rule name has both prioritized and non-prioritized clauses | §13.8 |
| `AMBIGUOUS_RULE_NAME` | Unqualified name matches exports from multiple imported modules | §18.4 |
| `RULE_NOT_EXPORTED` | Reference to a rule not in the module's EXPORT list | §18.2 |

### 25.2 CompileErrors.feature

```gherkin
Feature: Compile Error Coverage

  Scenario Outline: Expected compile errors are raised
    When compiling a Locy program that triggers <error_code>
    Then a compile error should be raised with code <error_code>

    Examples:
      | error_code                   |
      | SCHEMA_MISMATCH              |
      | UNDEFINED_RULE               |
      | CYCLIC_NEGATION              |
      | PREV_IN_BASE_CASE            |
      | NON_MONOTONIC_IN_RECURSION   |
      | MULTIPLE_MONOTONIC_FOLD      |
      | BEST_BY_WITH_MONOTONIC_FOLD  |
      | BEST_BY_NON_ORDERED_TYPE     |
      | WARDEDNESS_VIOLATION         |
      | MIXED_PRIORITY               |
      | AMBIGUOUS_RULE_NAME          |
      | RULE_NOT_EXPORTED            |
```

---

## 26 Directory Structure

Complete directory layout for the Locy TCK:

```
tck/locy/
├── features/
│   ├── lexical/
│   │   ├── ReservedKeywords.feature
│   │   ├── CaseSensitivity.feature
│   │   └── Identifiers.feature
│   ├── rules/
│   │   ├── RuleDefinition.feature
│   │   ├── YieldClause.feature
│   │   ├── RuleReference_IS.feature
│   │   ├── RuleReference_IS_NOT.feature
│   │   ├── ISDisambiguation.feature
│   │   └── PrioritizedRules.feature
│   ├── path_values/
│   │   ├── AlongClause.feature
│   │   └── PrevReference.feature
│   ├── aggregation/
│   │   ├── FoldClause.feature
│   │   ├── MonotonicAggregation.feature
│   │   └── BestByClause.feature
│   ├── derivation/
│   │   ├── DeriveClause.feature
│   │   ├── DeriveCommand.feature
│   │   ├── ExistentialNew.feature
│   │   └── DeriveMerge.feature
│   ├── reasoning/
│   │   ├── GoalDirectedQuery.feature
│   │   ├── HypotheticalAssume.feature
│   │   ├── AbductiveReasoning.feature
│   │   └── ExplainRule.feature
│   ├── modules/
│   │   ├── ModuleDeclaration.feature
│   │   ├── UseImport.feature
│   │   └── QualifiedNames.feature
│   ├── evaluation/
│   │   ├── Stratification.feature
│   │   └── SemiNaiveEvaluation.feature
│   ├── type_system/
│   │   └── TypeConstraints.feature
│   ├── superset/
│   │   ├── SupersetGuarantee.feature
│   │   └── OpenCypherTCKRegression.feature
│   ├── integration/
│   │   ├── CorporateOwnership.feature
│   │   ├── InfrastructureCascade.feature
│   │   └── RBACAuthorization.feature
│   └── errors/
│       └── CompileErrors.feature
└── support/
    ├── step_definitions/
    │   ├── parse_steps.rs
    │   ├── compile_steps.rs
    │   └── evaluate_steps.rs
    └── test_helpers/
        ├── graph_builder.rs
        └── result_matchers.rs
```

---

## 27 Running the TCK

### 27.1 Rust Integration

```bash
# Run the OpenCypher TCK through the Locy parser (superset guarantee)
cargo test --package locy-parser --test tck_superset_suite

# Run the Locy TCK parse-level tests
cargo test --package locy-parser --test locy_tck_parse

# Run the Locy TCK compile-level tests
cargo test --package locy-compiler --test locy_tck_compile

# Run the Locy TCK evaluate-level tests
cargo test --package uni --test locy_tck_evaluate

# Run all Locy TCK tests with statistics
cargo test --package uni --test locy_tck_all -- --nocapture

# Run a specific feature file
cargo test --package uni --test locy_tck_all -- "PrioritizedRules"
```

### 27.2 Extracting Scenarios from Feature Files

Extend the existing `tck_extractor` to process Locy feature files:

```rust
// In tck_extractor
fn extract_locy_scenarios() {
    let locy_features_dir = "tck/locy/features";
    let output_dir = "tck/locy/extracted";

    // Same Cucumber placeholder expansion logic as OpenCypher TCK
    for feature_file in walk_dir(locy_features_dir) {
        let scenarios = parse_gherkin(feature_file);
        let expanded = expand_scenario_outlines(scenarios);
        write_extracted(output_dir, expanded);
    }
}
```

### 27.3 Test Harness Skeleton

All Locy programs flow through `db.locy().evaluate()`, which returns a `LocyResult` containing query rows, materialization stats, and compile warnings. See the **Locy API Design** document for the full type definitions.

```rust
#[cfg(test)]
mod locy_tck {
    use crate::test_support::*;

    // Level A: Parse tests
    #[test_case_from_feature("tck/locy/features/rules/RuleDefinition.feature")]
    fn test_parse(input: &str, should_pass: bool) {
        let result = LocyParser::parse(input);
        if should_pass {
            assert!(result.is_ok(), "Expected parse success: {}", input);
        } else {
            assert!(result.is_err(), "Expected parse failure: {}", input);
        }
    }

    // Level B: Compile tests
    #[test_case_from_feature("tck/locy/features/rules/RuleDefinition.feature")]
    fn test_compile(program: &str, expected_error: Option<&str>) {
        let ast = LocyParser::parse(program).unwrap();
        let result = LocyCompiler::compile(ast);
        match expected_error {
            Some(code) => assert_compile_error(result, code),
            None => assert!(result.is_ok()),
        }
    }

    // Level C: Evaluate tests — via db.locy().evaluate()
    #[test_case_from_feature("tck/locy/features/rules/RuleDefinition.feature")]
    async fn test_evaluate(setup: &str, program: &str, expected: &ResultTable) {
        let db = Uni::in_memory().build().await.unwrap();
        db.execute(setup).await.unwrap();

        let result = db.locy().evaluate(program).await.unwrap();

        // Assert query rows from the terminal RETURN/QUERY
        assert_result_matches(result.rows(), expected);

        // Additional assertions available via LocyResult:
        // result.derive_stats      — iterations, facts derived, convergence
        // result.rules_created     — which rules were registered
        // result.warnings          — compile warnings
        // result.derivation_tree   — from EXPLAIN RULE
        // result.abduce_modifications — from ABDUCE
    }
}
```

---

## 28 Compliance Reporting

### 28.1 Report Format

The TCK produces a compliance report with the following structure:

```
═══════════════════════════════════════════════════════════
LOCY TCK COMPLIANCE REPORT — v0.2
═══════════════════════════════════════════════════════════

OpenCypher Baseline (Superset Guarantee)
  Scenarios: 3,856    Passed: 3,856    Failed: 0    Rate: 100.0%

Locy Extension Suite
  Total Scenarios: 187    Passed: 180    Failed: 7    Rate: 96.3%

  By Feature Area:
  ┌─────────────────────────┬──────┬────────┬────────┬───────┐
  │ Feature                 │ Total│ Passed │ Failed │ Rate  │
  ├─────────────────────────┼──────┼────────┼────────┼───────┤
  │ Lexical & Keywords      │   18 │     18 │      0 │ 100%  │
  │ Rule Definitions        │   15 │     15 │      0 │ 100%  │
  │ IS / IS NOT References  │   18 │     18 │      0 │ 100%  │
  │ ALONG / prev            │   12 │     12 │      0 │ 100%  │
  │ FOLD (Stratified)       │    8 │      8 │      0 │ 100%  │
  │ Monotonic Aggregation   │   10 │      9 │      1 │  90%  │
  │ BEST BY                 │    8 │      7 │      1 │  87%  │
  │ DERIVE                  │   12 │     12 │      0 │ 100%  │
  │ NEW (Existential)       │    6 │      5 │      1 │  83%  │
  │ DERIVE MERGE            │    5 │      5 │      0 │ 100%  │
  │ PRIORITY                │   10 │     10 │      0 │ 100%  │
  │ QUERY                   │    9 │      8 │      1 │  89%  │
  │ ASSUME                  │    8 │      7 │      1 │  87%  │
  │ ABDUCE                  │    7 │      5 │      2 │  71%  │
  │ EXPLAIN RULE            │    5 │      5 │      0 │ 100%  │
  │ Modules (USE)           │   10 │     10 │      0 │ 100%  │
  │ Evaluation Model        │    6 │      6 │      0 │ 100%  │
  │ Type System             │    5 │      5 │      0 │ 100%  │
  │ Superset Guarantee      │    6 │      6 │      0 │ 100%  │
  │ Integration             │    5 │      5 │      0 │ 100%  │
  │ Error Coverage          │   12 │     12 │      0 │ 100%  │
  └─────────────────────────┴──────┴────────┴────────┴───────┘

  By Testing Level:
  ┌─────────┬──────┬────────┬────────┬───────┐
  │ Level   │ Total│ Passed │ Failed │ Rate  │
  ├─────────┼──────┼────────┼────────┼───────┤
  │ A Parse │   65 │     65 │      0 │ 100%  │
  │ B Compile│  42 │     42 │      0 │ 100%  │
  │ C Evaluate│ 80 │     73 │      7 │  91%  │
  └─────────┴──────┴────────┴────────┴───────┘
```

### 28.2 Compliance Tiers

| Tier | OpenCypher TCK | Locy Parse (A) | Locy Compile (B) | Locy Evaluate (C) |
|---|---|---|---|---|
| **Locy Core** | 100% | 100% | 100% | ≥ 90% on: Rules, IS, ALONG, FOLD, DERIVE, PRIORITY |
| **Locy Full** | 100% | 100% | 100% | ≥ 90% on all feature areas |
| **Locy Extended** | 100% | 100% | 100% | ≥ 95% on all feature areas + integration |

### 28.3 Implementation Phase Alignment

The TCK features map to the implementation phases defined in §24.5 of the specification:

| Impl Phase | TCK Feature Areas | Minimum for Phase Compliance |
|---|---|---|
| Phase 1: Core | Rules, IS, YIELD, Stratification | 100% A+B, ≥90% C |
| Phase 2: Paths | ALONG, prev | 100% A+B, ≥90% C |
| Phase 3: Derivation | DERIVE (edges), DeriveCommand | 100% A+B, ≥90% C |
| Phase 4: Aggregation | FOLD, Monotonic | 100% A+B, ≥90% C |
| Phase 5: Priority | PRIORITY | 100% A+B, ≥90% C |
| Phase 6: Selection | BEST BY | 100% A+B, ≥90% C |
| Phase 7: What-If | ASSUME | 100% A+B, ≥90% C |
| Phase 8: Goal-Directed | QUERY | 100% A+B, ≥90% C |
| Phase 9: Modules | MODULE, USE, EXPORT | 100% A+B, ≥90% C |
| Phase 10: Proofs | EXPLAIN RULE | 100% A+B, ≥90% C |
| Phase 11: Abduction | ABDUCE | 100% A+B, ≥80% C |
| Phase 12: Existential | NEW, Wardedness | 100% A+B, ≥80% C |
| Phase 13: EGDs | DERIVE MERGE | 100% A+B, ≥90% C |

---

## 29 Scenario Summary

### Estimated Scenario Counts by Feature Area

| # | Feature Area | Parse (A) | Compile (B) | Evaluate (C) | Total |
|---|---|---|---|---|---|
| 1 | Lexical & Keywords | 12 | 2 | — | 14 |
| 2 | Rule Definitions | 6 | 3 | 3 | 12 |
| 3 | YIELD Clause | 3 | — | — | 3 |
| 4 | IS References | 4 | 3 | 4 | 11 |
| 5 | IS NOT References | 5 | 1 | 1 | 7 |
| 6 | IS Disambiguation | 4 | — | — | 4 |
| 7 | ALONG Clause | 4 | — | 3 | 7 |
| 8 | prev Reference | 2 | 1 | — | 3 |
| 9 | FOLD (Stratified) | 3 | — | 3 | 6 |
| 10 | Monotonic Aggregation | 4 | 3 | 2 | 9 |
| 11 | BEST BY | 3 | 2 | 1 | 6 |
| 12 | DERIVE Clause | 4 | — | 3 | 7 |
| 13 | DERIVE Command | 2 | — | — | 2 |
| 14 | NEW (Existential) | — | 2 | 2 | 4 |
| 15 | DERIVE MERGE | — | — | 2 | 2 |
| 16 | PRIORITY | 2 | 3 | 2 | 7 |
| 17 | QUERY | 3 | — | 3 | 6 |
| 18 | ASSUME | 4 | — | 4 | 8 |
| 19 | ABDUCE | 3 | — | 2 | 5 |
| 20 | EXPLAIN RULE | 2 | — | 1 | 3 |
| 21 | Modules (USE) | 4 | 4 | — | 8 |
| 22 | Evaluation Model | — | 3 | 2 | 5 |
| 23 | Type System | — | 3 | — | 3 |
| 24 | Superset Guarantee | 5 | — | 1 | 6 |
| 25 | Integration | — | — | 3 | 3 |
| 26 | Error Coverage | — | 12 | — | 12 |
| | **TOTAL** | **~73** | **~42** | **~42** | **~157** |

These counts represent the initial TCK. The suite is expected to grow as implementations reveal edge cases and the specification evolves.

---

*End of Locy TCK Specification.*
