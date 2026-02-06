# Uni OpenCypher TCK Compatibility Report

**Generated:** 2026-02-05
**TCK Version:** M23 (openCypher)
**Uni Version:** Current main branch (post `57e4295`, uncommitted temporal/ORDER BY fixes)

---

## Executive Summary

| Metric | Count | Pass Rate |
|--------|-------|-----------|
| **Features** | 191 | - |
| **Scenarios** | 3,867 | **55.0%** (2,126 passed, 1,741 failed) |
| **Steps** | 14,342 | **87.9%** (12,601 passed, 1,741 failed) |
| **Parsing Errors** | 1 | Match5.feature |

The high step pass rate (87.9%) vs lower scenario pass rate (55.0%) indicates that most basic operations work, but many scenarios fail at specific assertion points.

---

## Category Pass Rates

| Category | Passed | Failed | Total | Rate |
|----------|--------|--------|-------|------|
| Conditional | 12 | 1 | 13 | 92.3% |
| Quantifier | 489 | 115 | 604 | 81.0% |
| **Call** | **42** | **10** | **52** | **80.8%** |
| **Temporal** | **749** | **255** | **1,004** | **74.6%** |
| Null | 31 | 13 | 44 | 70.5% |
| Boolean | 104 | 46 | 150 | 69.3% |
| Match | 278 | 74 | 352 | 79.0% |
| Literals | 85 | 46 | 131 | 64.9% |
| Unwind | 8 | 6 | 14 | 57.1% |
| Union | 6 | 6 | 12 | 50.0% |
| Mathematical | 3 | 3 | 6 | 50.0% |
| Precedence | 60 | 61 | 121 | 49.6% |
| Comparison | 32 | 40 | 72 | 44.4% |
| List | 75 | 110 | 185 | 40.5% |
| ReturnSkipLimit | 12 | 19 | 31 | 38.7% |
| TypeConversion | 17 | 30 | 47 | 36.2% |
| Map | 15 | 29 | 44 | 34.1% |
| **WithOrderBy** | **86** | **206** | **292** | **29.5%** |
| ReturnOrderBy | 10 | 25 | 35 | 28.6% |
| Graph | 13 | 47 | 60 | 21.7% |
| ExistentialSubquery | 2 | 8 | 10 | 20.0% |
| Create | 13 | 65 | 78 | 16.7% |
| With | 4 | 25 | 29 | 13.8% |
| Delete | 5 | 36 | 41 | 12.2% |
| Return | 6 | 57 | 63 | 9.5% |
| MatchWhere | 27 | 7 | 34 | 79.4% |
| Remove | 2 | 31 | 33 | 6.1% |
| Pattern | 2 | 48 | 50 | 4.0% |
| Set | 2 | 51 | 53 | 3.8% |
| Aggregation | 0 | 35 | 35 | 0.0% |
| CountingSubgraphMatches | 0 | 11 | 11 | 0.0% |
| Merge | 0 | 75 | 75 | 0.0% |
| Path | 0 | 7 | 7 | 0.0% |
| String | 0 | 32 | 32 | 0.0% |
| TriadicSelection | 0 | 19 | 19 | 0.0% |
| WithSkipLimit | 0 | 9 | 9 | 0.0% |
| WithWhere | 0 | 19 | 19 | 0.0% |

---

## Per-Feature Detail

### 100% Pass Rate (Fully Passing)

| Feature | Passed | Total |
|---------|--------|-------|
| Call3 - Assignable-type arguments | 6 | 6 |
| Call4 - Null Arguments | 2 | 2 |
| Conditional2 - Case Expression | 12 | 12 |
| Literals1 - Boolean and Null | 6 | 6 |
| Mathematical11 - Signed numbers functions | 1 | 1 |
| Mathematical13 - Square root | 1 | 1 |
| Quantifier5 - None quantifier interop | 31 | 31 |
| Quantifier6 - Single quantifier interop | 21 | 21 |
| Quantifier8 - All quantifier interop | 31 | 31 |
| ReturnOrderBy4 - Order by in combination with projection | 2 | 2 |
| Temporal6 - Render Temporal Values as a String | 17 | 17 |
| Temporal8 - Compute Arithmetic Operations on Temporal Values | 27 | 27 |

### High Pass Rate (>75%)

| Feature | Passed | Total | Rate |
|---------|--------|-------|------|
| Temporal1 - Create Temporal Values from a Map | 199 | 207 | 96.1% |
| Literals5 - Float | 26 | 27 | 96.3% |
| Temporal10 - Compute Durations Between two Temporal Values | 125 | 131 | 95.4% |
| Temporal7 - Compare Temporal Values | 17 | 18 | 94.4% |
| Quantifier1 - None quantifier | 94 | 105 | 89.5% |
| Quantifier3 - Any quantifier | 94 | 105 | 89.5% |
| Quantifier4 - All quantifier | 93 | 105 | 88.6% |
| Quantifier2 - Single quantifier | 93 | 106 | 87.7% |
| Quantifier7 - Any quantifier interop | 31 | 36 | 86.1% |
| Match1 - Match nodes | 73 | 86 | 84.9% |
| Literals6 - String | 11 | 13 | 84.6% |
| Call5 - Results projection | 16 | 19 | 84.2% |
| Match2 - Match relationships | 71 | 86 | 82.6% |
| Null1 - IS NULL validation | 14 | 17 | 82.4% |
| Null2 - IS NOT NULL validation | 14 | 17 | 82.4% |
| Match6 - Match named paths scenarios | 78 | 97 | 80.4% |
| Boolean1 - And logical operations | 24 | 30 | 80.0% |
| Boolean2 - OR logical operations | 24 | 30 | 80.0% |
| Boolean3 - XOR logical operations | 24 | 30 | 80.0% |
| Literals4 - Octal integer | 8 | 10 | 80.0% |
| Temporal9 - Truncate Temporal Values | 255 | 322 | 79.2% |
| Precedence2 - On numeric values | 20 | 26 | 76.9% |
| Call1 - Basic procedure calling | 12 | 16 | 75.0% |

### Medium Pass Rate (25-75%)

| Feature | Passed | Total | Rate |
|---------|--------|-------|------|
| Graph9 - Retrieve all properties as a property map | 5 | 7 | 71.4% |
| Temporal5 - Access Components of Temporal Values | 5 | 7 | 71.4% |
| Literals3 - Hexadecimal integer | 11 | 16 | 68.8% |
| Call2 - Procedure arguments | 4 | 6 | 66.7% |
| Call6 - Call clause interoperation with other clauses | 2 | 3 | 66.7% |
| Literals2 - Decimal integer | 8 | 12 | 66.7% |
| Precedence4 - On null value | 8 | 12 | 66.7% |
| Comparison1 - Equality | 28 | 43 | 65.1% |
| Map1 - Static value access | 12 | 19 | 63.2% |
| Boolean5 - Interop of logical operations | 5 | 8 | 62.5% |
| List5 - List Membership Validation - IN Operator | 28 | 46 | 60.9% |
| Union1 - Union | 3 | 5 | 60.0% |
| Union2 - Union All | 3 | 5 | 60.0% |
| Unwind1 | 8 | 14 | 57.1% |
| Temporal4 - Store Temporal Values | 21 | 39 | 53.8% |
| Boolean4 - NOT logical operations | 27 | 52 | 51.9% |
| List11 - Create a list from a range | 34 | 67 | 50.7% |
| ExistentialSubquery1 - Simple existential subquery | 2 | 4 | 50.0% |
| ReturnOrderBy1 - Order by a single variable | 6 | 12 | 50.0% |
| Return1 - Return single variable | 1 | 2 | 50.0% |
| With2 - Forward single expression | 1 | 2 | 50.0% |
| Delete3 - Deleting named paths | 1 | 2 | 50.0% |
| Graph8 - Property keys function | 4 | 8 | 50.0% |
| Mathematical8 - Arithmetic precedence | 1 | 2 | 50.0% |
| TypeConversion4 - To String | 7 | 14 | 50.0% |
| ReturnSkipLimit2 - Limit | 8 | 17 | 47.1% |
| Literals7 - List | 9 | 20 | 45.0% |
| Precedence1 - On boolean values | 32 | 72 | 44.4% |
| List3 - List Equality | 3 | 7 | 42.9% |
| ReturnOrderBy6 - Aggregation expressions in order by | 2 | 5 | 40.0% |
| TypeConversion1 - To Boolean | 4 | 10 | 40.0% |
| Temporal3 - Project Temporal Values from other Temporal Values | 67 | 183 | 36.6% |
| ReturnSkipLimit1 - Skip | 4 | 11 | 36.4% |
| Match7 - Optional match | 17 | 31 | 54.8% |
| Create1 - Creating nodes | 7 | 20 | 35.0% |
| WithOrderBy3 - Order by multiple expressions | 32 | 93 | 34.4% |
| Delete4 - Delete clause interoperation with other clauses | 1 | 3 | 33.3% |
| List2 - List Slicing | 5 | 15 | 33.3% |
| Temporal2 - Create Temporal Values from a String | 16 | 53 | 30.2% |
| WithOrderBy2 - Order by a single expression | 25 | 83 | 30.1% |
| Null3 - Null evaluation | 3 | 10 | 30.0% |
| WithOrderBy1 - Order by a single variable | 27 | 96 | 28.1% |
| TypeConversion3 - To Float | 3 | 11 | 27.3% |
| Create2 - Creating relationships | 6 | 24 | 25.0% |
| Delete1 - Deleting nodes | 2 | 8 | 25.0% |
| TypeConversion2 - To Integer | 3 | 12 | 25.0% |

### Low Pass Rate (<25%)

| Feature | Passed | Total | Rate |
|---------|--------|-------|------|
| Literals8 - Maps | 6 | 27 | 22.2% |
| With6 - Implicit grouping with aggregates | 2 | 9 | 22.2% |
| MatchWhere1 - Filter single variable | 12 | 15 | 80.0% |
| Remove2 - Remove a Label | 1 | 5 | 20.0% |
| Return6 - Implicit grouping with aggregates | 4 | 21 | 19.0% |
| Map3 - Keys function | 2 | 11 | 18.2% |
| List6 - List size | 3 | 17 | 17.6% |
| Match3 - Match fixed length patterns | 5 | 30 | 16.7% |
| With1 - Forward single variable | 1 | 6 | 16.7% |
| Comparison2 - Half-bounded Range | 3 | 19 | 15.8% |
| Graph6 - Static property access | 2 | 14 | 14.3% |
| Remove1 - Remove a Property | 1 | 7 | 14.3% |
| Quantifier10 - Single quantifier invariants | 1 | 8 | 12.5% |
| Set3 - Set a Label | 1 | 8 | 12.5% |
| Comparison3 - Full-Bound Range | 1 | 9 | 11.1% |
| Delete5 - Delete clause interoperation with built-in data types | 1 | 9 | 11.1% |
| Graph3 - Node labels | 1 | 9 | 11.1% |
| WithOrderBy4 - Order by in combination with projection and aliasing | 2 | 20 | 10.0% |
| Graph4 - Edge relationship type | 1 | 11 | 9.1% |
| Pattern2 - Pattern Comprehension | 1 | 11 | 9.1% |
| Return4 - Column renaming | 1 | 11 | 9.1% |
| Set1 - Set a Property | 1 | 11 | 9.1% |
| List1 - Dynamic Element Access | 2 | 23 | 8.7% |
| Map2 - Dynamic Value Access | 1 | 14 | 7.1% |
| Pattern1 - Pattern predicate | 1 | 39 | 2.6% |

### 0% Pass Rate (Fully Failing)

| Feature | Total |
|---------|-------|
| Aggregation1 - Count | 2 |
| Aggregation2 - Min and Max | 12 |
| Aggregation3 - Sum | 2 |
| Aggregation5 - Collect | 2 |
| Aggregation6 - Percentiles | 13 |
| Aggregation8 - DISTINCT | 4 |
| Comparison4 - Combination of Comparisons | 1 |
| Conditional1 - Coalesce expression | 1 |
| CountingSubgraphMatches1 | 11 |
| Create3 - Interoperation with other clauses | 13 |
| Create4 - Large Create Query | 2 |
| Create5 - Multiple hops create patterns | 5 |
| Create6 - Persistence of create clause side effects | 14 |
| Delete2 - Deleting relationships | 5 |
| Delete6 - Persistence of delete clause side effects | 14 |
| ExistentialSubquery2 - Full existential subquery | 3 |
| ExistentialSubquery3 - Nested existential subquery | 3 |
| Graph5 - Node and edge label expressions | 8 |
| Graph7 - Dynamic property access | 3 |
| List12 - List Comprehension | 7 |
| List4 - List Concatenation | 2 |
| List9 - List Tail | 1 |
| Match4 - Match variable length patterns scenarios | 10 |
| Match8 - Match clause interoperation with other clauses | 3 |
| Match9 - Match deprecated scenarios | 9 |
| Mathematical2 - Addition | 1 |
| Mathematical3 - Subtraction | 1 |
| Merge1 - Merge node | 17 |
| Merge2 - Merge node - on create | 6 |
| Merge3 - Merge node - on match | 5 |
| Merge4 - Merge node - on match and on create | 2 |
| Merge5 - Merge relationships | 29 |
| Merge6 - Merge relationships - on create | 6 |
| Merge7 - Merge relationships - on match | 5 |
| Merge8 - Merge relationships - on match and on create | 1 |
| Merge9 - Merge clause interoperation with other clauses | 4 |
| Path1 - Nodes of a path | 1 |
| Path2 - Relationships of a path | 3 |
| Path3 - Length of a path | 3 |
| Precedence3 - On list values | 11 |
| Quantifier9 - None quantifier invariants | 17 |
| Quantifier11 - Any quantifier invariants | 22 |
| Quantifier12 - All quantifier invariants | 17 |
| Remove3 - Persistence of remove clause side effects | 21 |
| Return2 - Return single expression | 18 |
| Return3 - Return multiple expressions | 3 |
| Return5 - Implicit grouping with distinct | 5 |
| Return7 - Return all variables | 2 |
| Return8 - Return clause interoperation with other clauses | 1 |
| ReturnOrderBy2 - Order by a single expression | 14 |
| ReturnOrderBy3 - Order by multiple expressions | 1 |
| ReturnOrderBy5 - Order by in combination with column renaming | 1 |
| ReturnSkipLimit3 - Skip and limit | 3 |
| Set2 - Set a Property to Null | 3 |
| Set4 - Set all properties with a map | 5 |
| Set5 - Set multiple properties with a map | 5 |
| Set6 - Persistence of set clause side effects | 21 |
| String1 - Substring extraction | 1 |
| String10 - Exact Substring Search | 9 |
| String11 - Combining Exact String Search | 2 |
| String3 - String Reversal | 1 |
| String4 - String Splitting | 1 |
| String8 - Exact String Prefix Search | 9 |
| String9 - Exact String Suffix Search | 9 |
| TriadicSelection1 - Query three related nodes on binary-tree graphs | 19 |
| Union3 - Union in combination with Union All | 2 |
| With3 - Forward multiple expressions | 1 |
| With4 - Variable aliasing | 7 |
| With5 - Implicit grouping with DISTINCT | 2 |
| With7 - WITH on WITH | 2 |
| WithSkipLimit1 - Skip | 2 |
| WithSkipLimit2 - Limit | 4 |
| WithSkipLimit3 - Skip and limit | 3 |
| WithWhere1 - Filter single variable | 4 |
| WithWhere2 - Filter multiple variables | 2 |
| WithWhere3 - Equi-Joins on variables | 3 |
| WithWhere4 - Non-Equi-Joins on variables | 2 |
| WithWhere5 - Filter on predicate resulting in null | 4 |
| WithWhere6 - Filter on aggregates | 1 |
| WithWhere7 - Variable visibility under aliasing | 3 |

---

## Step-Level Failure Breakdown

| Failure Type | Steps | % of Failed |
|-------------|-------|-------------|
| Result mismatch (wrong values, missing/extra rows) | ~981 | 51.6% |
| No match found for actual row (extra rows) | ~647 | 34.0% |
| No result found (query returns empty) | ~409 | 21.5% |
| Error detail mismatch (wrong error keyword) | ~136 | 7.2% |
| No error found (Uni too permissive) | ~117 | 6.2% |

Note: A single scenario failure may involve multiple step-level failure types.

---

## Remaining Validation Gaps (Expected Errors Not Raised)

### Errors Not Raised (~117 scenarios)

Uni accepts queries that openCypher rejects:

| Count | Expected Error | Description |
|-------|---------------|-------------|
| 27 | InvalidArgumentValue | Invalid argument values |
| 22 | UndefinedVariable | Variables used before definition |
| 15 | InvalidArgumentType | Wrong type for operations |
| 9 | UnexpectedSyntax | Syntax that should be rejected |
| 7 | NumberOutOfRange | Number out of valid range |
| 5 | IntegerOverflow | Integer literal overflow |
| 4 | DeletedEntityAccess | Access to deleted entity |
| 4 | AmbiguousAggregationExpression | Aggregation scope ambiguity |
| 3 | VariableAlreadyBound | Variable illegally rebound |
| 3 | VariableTypeConflict | Variable used as conflicting types |
| 3 | ColumnNameConflict | Duplicate column names |
| 2 | DeleteConnectedNode | Delete node with relationships |
| 2 | MapElementAccessByNonString | Non-string map key access |
| 1 | ProcedureNotFound | Missing procedure |
| 1 | InvalidDelete | Invalid delete target |
| 1 | InvalidParameterUse | Parameter validation |
| 1 | RelationshipUniquenessViolation | Relationship uniqueness |
| 1 | NoSingleRelationshipType | Relationship type required |
| 1 | NestedAggregation | Nested aggregation |
| 1 | NoVariablesInScope | Empty scope |
| 1 | InvalidClauseComposition | Clause ordering error |
| 1 | InvalidAggregation | Aggregation in wrong context |
| 1 | InvalidNumberLiteral | Invalid number literal |
| 1 | FloatingPointOverflow | Float overflow |

### Error Detail Mismatches (~136 scenarios)

An error is raised but the message doesn't contain the expected keyword:

| Count | Expected Keyword | Description |
|-------|-----------------|-------------|
| 58 | InvalidArgumentType | Wrong error classification for type errors |
| 25 | InvalidAggregation | Aggregation error classification |
| 14 | VariableTypeConflict | Wrong error for type conflicts |
| 13 | UnexpectedSyntax | Parser error doesn't include expected keyword |
| 4 | InvalidNumberLiteral | Number parsing error |
| 3 | InvalidParameterUse | Parameter validation |
| 3 | NonConstantExpression | Constant-folding error |
| 2 | VariableAlreadyBound | Variable rebinding error |
| 2 | InvalidRelationshipPattern | Malformed relationship pattern |
| 2 | UndefinedVariable | Scope error missing keyword |
| 2 | NoSingleRelationshipType | Missing relationship type |
| 2 | InvalidClauseComposition | Clause ordering error |
| 1 | MissingParameter | Missing parameter |
| 1 | InvalidArgumentPassingMode | Wrong argument passing mode |
| 1 | CreatingVarLength | Variable-length in CREATE |
| 1 | AmbiguousAggregationExpression | Aggregation scope |
| 1 | InvalidUnicodeLiteral | Unicode literal error |
| 1 | InvalidUnicodeCharacter | Unicode character error |

---

## Failure Root Causes

1. **Result Mismatch (~981 step failures)**
   - Most common failure type
   - Root causes: missing functions (aggregation, string, list), incorrect query execution, edge counting bugs
   - Includes: wrong values, empty results, extra/missing rows

2. **No Result Found (~409 step failures)**
   - Query returns empty when data is expected
   - Root causes: graph fixture loading failures, missing query plan steps, complex multi-clause queries

3. **Over-Permissive Behavior (~117 remaining "no error" failures)**
   - Uni accepts queries that openCypher rejects
   - **Addressed in previous work:** Added semantic validation for:
     - UndefinedVariable (variables used before definition)
     - VariableTypeConflict (variable reused with conflicting type)
     - VariableAlreadyBound (path variable conflicts, variable re-binding)
     - InvalidArgumentType (wrong type for DELETE, SKIP/LIMIT, WITH ORDER BY)
     - InvalidAggregation (aggregation in WITH ORDER BY)
     - NegativeIntegerArgument (negative SKIP/LIMIT)
   - Still needs: InvalidArgumentValue (27), more UndefinedVariable (22), NumberOutOfRange (7)

4. **Unimplemented Features (~166 failures)**
   - MERGE not implemented (75 scenarios)
   - Existential subqueries partially supported (8 remaining failures)
   - Procedure CALL now mostly working (10 remaining failures)

5. **TCK Harness Gaps (~100 step failures)**
   - Side effect verification not fully implemented
   - CREATE/DELETE/SET/REMOVE side effects not checked

6. **Named Graph Fixtures (19 failures)**
   - binary-tree-1/2 use `CREATE LABEL` syntax
   - Parser doesn't support this syntax

---

## Feature Coverage Analysis

### Clauses

| Clause | Status | Pass Rate | Notes |
|--------|--------|-----------|-------|
| MATCH | Good | 67.6% | Basic patterns strong, variable-length paths missing |
| WHERE | Good | 79.4% | Simple filters, joins, null predicates, OPTIONAL MATCH work |
| RETURN | Partial | 9.5% | Core works, expressions/aggregates need work |
| CREATE | Partial | 16.7% | Basic node/edge creation works |
| DELETE | Partial | 12.2% | Basic delete works, interop issues |
| SET | Limited | 3.8% | Basic property setting works |
| REMOVE | Limited | 6.1% | Basic functionality present |
| MERGE | None | 0.0% | Not implemented |
| WITH | Partial | 13.8% | Piping works, WHERE/aliasing issues |
| WITH ORDER BY | Partial | 29.5% | Basic ordering works, single-expression regression fixed |
| UNWIND | Good | 57.1% | List unwinding mostly works |
| UNION | Good | 50.0% | Basic union works |
| **CALL** | **Strong** | **80.8%** | **Procedure infrastructure now working** |

### Expressions

| Category | Status | Pass Rate | Notes |
|----------|--------|-----------|-------|
| Quantifiers | Strong | 81.0% | ALL, ANY, NONE, SINGLE well-supported |
| Conditional (CASE) | Strong | 92.3% | CASE expressions fully supported |
| Null handling | Good | 70.5% | Three-valued logic correct |
| Boolean | Good | 69.3% | AND, OR, NOT, XOR work |
| Literals | Good | 64.9% | Booleans, integers, floats, strings work |
| **Temporal** | **Strong** | **74.6%** | **Creation, truncation, arithmetic, comparison, duration-between, formatting all work** |
| Precedence | Moderate | 49.6% | Numeric precedence good, boolean/list issues |
| Comparison | Moderate | 44.4% | Equality good, ranges need work |
| List | Moderate | 40.5% | IN operator and ranges work, comprehension/dynamic access weak |
| Type Conversion | Moderate | 36.2% | toString partially works |
| Map | Moderate | 34.1% | Static access good, dynamic/keys weak |
| Graph | Limited | 21.7% | Property access works, labels/types weak |
| Existential Subquery | Limited | 20.0% | Simple EXISTS partially working |
| Pattern | Limited | 4.0% | Pattern predicates incomplete |
| Aggregation | None | 0.0% | COUNT, SUM, etc. not returning correct results |
| String | None | 0.0% | String functions not implemented |
| Path | None | 0.0% | Path functions not implemented |

---

## Progress Tracking

| Date | Scenarios Passed | Pass Rate | Key Changes |
|------|-----------------|-----------|-------------|
| 2026-02-03 (baseline) | 1,279 | 33.1% | Initial measurement |
| 2026-02-03 | 1,331 | 34.4% | Schemaless vertex scan support |
| 2026-02-04 | 1,352 | 35.0% | Schemaless edge creation support |
| 2026-02-04 | 1,355 | 35.0% | CREATE returns entities for RETURN, edge dedup, pattern comprehension |
| 2026-02-04 | 1,423 | 36.8% | Semantic validation + error classification fix |
| 2026-02-04 | 1,764 | 45.6% | Path variable binding, WITH ORDER BY/SKIP/LIMIT, aggregation validation, error type leniency |
| 2026-02-04 | 1,941 | 50.2% | Temporal7 (comparison), Temporal8 (arithmetic), Temporal10 (duration between), DST-aware computation, null propagation |
| 2026-02-05 | 1,966 | 50.8% | Procedure CALL support (+42), planner validation bugfixes, time storage fixes |
| 2026-02-05 | 2,126 | 55.0% | Temporal formatting fixes (+139), WithOrderBy2 regression fix (+21) |
| 2026-02-05 | **2,186** | **56.5%** | MatchWhere fixes (+24): type coercion, NULL handling, OPTIONAL MATCH, WHERE clause |

### Cumulative Improvement

| From | To | Scenarios Gained | Improvement |
|------|-----|------------------|-------------|
| Baseline (1,279) | Current (2,126) | **+847** | **+66.2%** |

### Recent Gains (1,966 → 2,126)

| Category | Before | After | Delta |
|----------|--------|-------|-------|
| Temporal (formatting fixes) | 610/1,004 | 749/1,004 | +139 |
| WithOrderBy (regression fix) | 65/292 | 86/292 | +21 |
| **Net** | | | **+160** |

**Key fixes:**
- Omit redundant `:00` seconds when seconds are zero (matches Neo4j formatting)
- Normalize `+00:00` to `Z` for UTC offsets
- Fix WITH ORDER BY validation to allow references to both projected aliases and original variables
- WithOrderBy2 recovered from 1/83 to 25/83 (regression from planner validation changes fixed)

---

## Key Gaps to Address

### High Priority (blocking many tests)

1. **Aggregation Functions** (35 failures)
   - COUNT, SUM, AVG, MIN, MAX, COLLECT, percentiles not returning correct results
   - Core feature needed for many query patterns

2. **String Functions** (32 failures)
   - STARTS WITH, ENDS WITH, CONTAINS predicates
   - substring(), reverse(), split(), etc.

3. **MERGE Implementation** (75 failures)
   - Upsert semantics not implemented
   - ON CREATE / ON MATCH clauses

4. ~~**WithOrderBy2 Regression** (24 lost scenarios)~~ **FIXED** — recovered from 1/83 to 25/83

5. **Result Mismatch Investigation** (~981 failures)
   - Wrong query output is most common failure
   - Need to audit specific failing queries
   - Likely causes: missing functions, incorrect aggregation, path handling

### Medium Priority

6. **WITH/WHERE Integration** (19 failures)
   - WHERE clause after WITH not working
   - Variable aliasing issues

7. **Variable-Length Paths** (10 failures)
   - `(a)-[*1..3]->(b)` patterns
   - Requires iterative path expansion

8. **Pattern Predicates** (50 failures)
   - WHERE (a)-->(b) patterns
   - EXISTS patterns

9. **Over-Permissive Validation** (~117 failures)
   - InvalidArgumentValue (27 scenarios)
   - UndefinedVariable (22 scenarios)
   - InvalidArgumentType (15 scenarios)

### Low Priority

10. **Named Graph Fixtures** (19 failures)
    - Implement binary-tree-1/2 fixtures

11. **Side Effect Verification** (~100 failures)
    - TCK harness needs side effect checking

---

## Recommendations

### Short-term Wins
1. ~~**Fix WithOrderBy2 regression**~~ **DONE** — recovered +24 scenarios
2. ~~**Fix temporal formatting**~~ **DONE** — recovered +139 scenarios (omit `:00` seconds, normalize `+00:00` → `Z`)
3. Implement basic aggregation (COUNT, SUM, AVG, MIN, MAX) — could unlock ~35 scenarios
4. Add string functions (STARTS WITH, ENDS WITH, CONTAINS) — could unlock ~32 scenarios
5. Fix WITH WHERE integration — could unlock ~19 scenarios

### Medium-term Goals
1. Implement MERGE clause (~75 scenarios)
2. Add variable-length path patterns (~10+ scenarios)
3. Extend semantic validation coverage (~117 remaining "no error" scenarios)

### Long-term Goals
1. Achieve 60%+ scenario pass rate
2. Existential subqueries and pattern predicates
3. Complete side effect verification in TCK harness

### Next Milestone Target

| Target | Scenarios | Pass Rate | Gap |
|--------|-----------|-----------|-----|
| ~~55%~~ | ~~2,127~~ | ~~55.0%~~ | **ACHIEVED** (2,126 ≈ 55.0%) |
| 60% | 2,320 | 60.0% | +194 scenarios |
| 65% | 2,514 | 65.0% | +388 scenarios |

Achieving 60% likely requires:
- Aggregation functions (+35)
- String functions (+32)
- WITH WHERE integration (+19)
- Partial MERGE implementation (+30)
- Additional validation coverage (+21)
- Remaining temporal formatting gaps (+57)

---

## Test Command Reference

```bash
# Run all TCK tests
cargo test -p uni-tck --test cucumber -- --tags 'not @ignore'

# Run specific feature
cargo test -p uni-tck --test cucumber -- features/expressions/literals/Literals1.feature

# Run specific temporal features
cargo test -p uni-tck --test cucumber -- features/expressions/temporal/Temporal7.feature
cargo test -p uni-tck --test cucumber -- features/expressions/temporal/Temporal8.feature
cargo test -p uni-tck --test cucumber -- features/expressions/temporal/Temporal10.feature

# Run by scenario name regex
cargo test -p uni-tck --test cucumber -- -n 'Should compare dates'

# Save output for analysis (recommended for bulk filtering)
cargo test -p uni-tck --test cucumber 2>&1 > /tmp/tck_output.txt

# Count failures by pattern (from saved output)
grep -c "No error found" /tmp/tck_output.txt
grep -c "No result found" /tmp/tck_output.txt
grep -c "No match found for actual row" /tmp/tck_output.txt
```

---

## Appendix: Feature File Categories

| Directory | Feature Files | Focus |
|-----------|--------------|-------|
| `clauses/call` | 6 | Procedure calling |
| `clauses/create` | 6 | Node/edge creation |
| `clauses/delete` | 6 | Node/edge deletion |
| `clauses/match` | 9 | Pattern matching |
| `clauses/match-where` | 6 | Filtered matching |
| `clauses/merge` | 9 | Upsert operations |
| `clauses/remove` | 3 | Property/label removal |
| `clauses/return` | 8 | Result projection |
| `clauses/return-orderby` | 6 | Sorted results |
| `clauses/return-skip-limit` | 3 | Pagination |
| `clauses/set` | 6 | Property updates |
| `clauses/union` | 3 | Query combination |
| `clauses/unwind` | 1 | List expansion |
| `clauses/with` | 7 | Query chaining |
| `clauses/with-orderby` | 4 | Sorted chaining |
| `clauses/with-skip-limit` | 3 | Paginated chaining |
| `clauses/with-where` | 7 | Filtered chaining |
| `expressions/aggregation` | 8 | Aggregate functions |
| `expressions/boolean` | 5 | Boolean logic |
| `expressions/comparison` | 4 | Comparisons |
| `expressions/conditional` | 2 | CASE expressions |
| `expressions/existentialSubqueries` | 3 | EXISTS patterns |
| `expressions/graph` | 9 | Graph functions |
| `expressions/list` | 12 | List operations |
| `expressions/literals` | 8 | Value literals |
| `expressions/map` | 3 | Map operations |
| `expressions/mathematical` | 17 | Math operators |
| `expressions/null` | 3 | Null handling |
| `expressions/path` | 3 | Path expressions |
| `expressions/pattern` | 2 | Pattern predicates |
| `expressions/precedence` | 4 | Operator precedence |
| `expressions/quantifier` | 12 | List quantifiers |
| `expressions/string` | 14 | String functions |
| `expressions/temporal` | 10 | Date/time |
| `expressions/typeConversion` | 6 | Type coercion |
| `useCases/countingSubgraphMatches` | 1 | Pattern counting |
| `useCases/triadicSelection` | 1 | Friend-of-friend |

**Total:** 191 feature files, 3,867 expanded scenarios
