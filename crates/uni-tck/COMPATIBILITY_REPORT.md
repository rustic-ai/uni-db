# Uni OpenCypher TCK Compatibility Report

**Generated:** 2026-02-08 (Latest TCK Run)
**TCK Version:** M23 (openCypher)
**Uni Version:** Current debug/tck001 branch

---

## Executive Summary

| Metric | Count | Pass Rate |
|--------|-------|-----------|
| **Features** | 192 | - |
| **Scenarios** | 3,897 | **70.0%** (2,731 passed, 1,127 failed) |
| **Skipped** | 39 | - |
| **Parsing Errors** | 0 | None (harness fixed) |

**Last Update (2026-02-08 night):**
Latest run. Pass rate: 70.0% (2,731/3,897 scenarios). **70% MILESTONE ACHIEVED!**
+11 scenarios from previous run. Net gain after accounting for regressions.

Key improvements in this run:
1. Match7 (Optional match) improved from 54.8% to 64.5% (+3 scenarios)
2. Match9 (Match deprecated) improved from 11.1% to 44.4% (+3 scenarios)
3. Match4 (Variable length patterns) improved from 0% to 20% (+2 scenarios)
4. Pattern1 (Pattern predicate) improved from 35.9% to 41.0% (+2 scenarios)
5. Graph5 (Node and edge label expressions) improved from 0% to 11.1% (+1 scenario)
6. Match1 (Match nodes) improved from 97.7% to 98.8% (+1 scenario)
7. Match8 (Match clause interop) improved from 33.3% to 66.7% (+1 scenario)
8. WithOrderBy2 improved from 80.7% to 81.9% (+1 scenario)
9. WithOrderBy4 improved from 45.0% to 50.0% (+1 scenario)
10. WithSkipLimit1 improved from 50% to 100% (+1 scenario)
11. WithSkipLimit2 improved from 75% to 100% (+1 scenario)

**Regressions in this run:**
1. Match6 (Match named paths) regressed from 90.7% to 86.6% (-4 scenarios)
2. Graph7 (Dynamic property access) regressed from 33.3% to 0% (-1 scenario)
3. Return7 (Return all variables) regressed from 50% to 0% (-1 scenario)

**Previous Run (2026-02-08 evening):**
Pass rate: 69.8% (2,720/3,897 scenarios). +22 scenarios.
CountingSubgraphMatches1 +5, Comparison1 +4, Return6 +3, Aggregation1 now 100%, plus smaller gains. Zero regressions.

**Previous Run (2026-02-08 late PM):**
Pass rate: 69.2% (2,698/3,897 scenarios). +15 scenarios.
Comparison1 +10, List5 +7, Precedence3 +6, Graph6 +2, Return2 +2, List3 +1. Zero regressions.

**Previous Run (2026-02-08 PM):**
Pass rate: 68.8% (2,683/3,897 scenarios). +60 scenarios.
Boolean ops now 100%, Comparison3/4 now 100%, Null1/2 now 100%, Precedence1 now 100%, WithOrderBy3 now 100%.

The high step pass rate (~91%) vs lower scenario pass rate (70.0%) indicates that most basic operations work, but many scenarios fail at specific assertion points.

---

## Match Category Results (Updated 2026-02-08)

| Feature | Passed | Total | Rate |
|---------|--------|-------|------|
| Match1 - Match nodes | 85 | 86 | 98.8% |
| Match2 - Match relationships | 83 | 86 | 96.5% |
| Match3 - Match fixed length patterns | 23 | 30 | 76.7% |
| Match4 - Match variable length patterns | 2 | 10 | 20.0% |
| Match5 - Match variable length patterns over given graphs | 3 | 29 | 10.3% |
| Match6 - Match named paths | 84 | 97 | 86.6% |
| Match7 - Optional match | 20 | 31 | 64.5% |
| Match8 - Match clause interop | 2 | 3 | 66.7% |
| Match9 - Match deprecated | 4 | 9 | 44.4% |
| **TOTAL Match** | **306** | **381** | **80.3%** |

### Fixes Implemented (2026-02-06)

1. **Path Detection and Conversion:** Added `is_path_map()` and `map_to_path()` to detect and convert maps with "nodes" and "relationships/edges" keys to proper Path type
2. **Property Normalization:** Added `normalize_property_value()` function that recursively processes nested lists/maps without applying structural detection (node/edge/path). This prevents user data with `_vid`/`_eid` keys from being incorrectly converted.
3. **Optional Match Logic:** Added `is_optional` field with early-exit BFS logic
4. **Named Path Serialization:** Added `#[serde(rename = "relationships")]` to Path struct

### Property Normalization Design

The key insight for correct result normalization:

- **Top-level results:** Use lenient detection (maps with `_vid` -> Node, maps with `_eid` -> Edge) because these are actual query results from the executor
- **Property values:** Use `normalize_property_value()` which does NOT apply structural detection, preserving user data that happens to contain `_vid`/`_eid` keys

```rust
// Top-level: normalize_value() - applies structural detection
// Properties: normalize_property_value() - just recursively processes containers
```

This separation ensures:
1. Query results are properly typed (Node/Edge/Path)
2. User data in properties is preserved as-is

---

## Category Pass Rates

| Category | Passed | Failed | Total | Rate |
|----------|--------|--------|-------|------|
| **Boolean** | **150** | **0** | **150** | **100.0%** |
| **Conditional** | **13** | **0** | **13** | **100.0%** |
| **Null** | **43** | **1** | **44** | **97.7%** |
| **MatchWhere** | **33** | **1** | **34** | **97.1%** |
| **Precedence** | **109** | **12** | **121** | **90.1%** |
| **ExistentialSubquery** | **9** | **1** | **10** | **90.0%** |
| Literals | 117 | 14 | 131 | 89.3% |
| **WithSkipLimit** | **8** | **1** | **9** | **88.9%** |
| Comparison | 61 | 11 | 72 | 84.7% |
| Quantifier | 502 | 102 | 604 | 83.1% |
| String | 26 | 6 | 32 | 81.2% |
| WithOrderBy | 236 | 56 | 292 | 80.8% |
| Call | 42 | 10 | 52 | 80.8% |
| Match | 306 | 75 | 381 | 80.3% |
| Temporal | 743 | 261 | 1,004 | 74.0% |
| WithWhere | 14 | 5 | 19 | 73.7% |
| Union | 8 | 4 | 12 | 66.7% |
| Mathematical | 4 | 2 | 6 | 66.7% |
| Unwind | 9 | 5 | 14 | 64.3% |
| Map | 25 | 19 | 44 | 56.8% |
| With | 16 | 13 | 29 | 55.2% |
| List | 97 | 88 | 185 | 52.4% |
| ReturnSkipLimit | 16 | 15 | 31 | 51.6% |
| ReturnOrderBy | 18 | 17 | 35 | 51.4% |
| Return | 29 | 34 | 63 | 46.0% |
| CountingSubgraphMatches | 5 | 6 | 11 | 45.5% |
| TypeConversion | 21 | 26 | 47 | 44.7% |
| Graph | 23 | 38 | 61 | 37.7% |
| Pattern | 17 | 33 | 50 | 34.0% |
| Path | 2 | 5 | 7 | 28.6% |
| Create | 13 | 65 | 78 | 16.7% |
| Delete | 5 | 36 | 41 | 12.2% |
| Remove | 4 | 29 | 33 | 12.1% |
| TriadicSelection | 2 | 17 | 19 | 10.5% |
| Aggregation | 3 | 32 | 35 | 8.6% |
| Set | 2 | 51 | 53 | 3.8% |
| Merge | 0 | 75 | 75 | 0.0% |

---

## Per-Feature Detail

### 100% Pass Rate (Fully Passing)

| Feature | Passed | Total |
|---------|--------|-------|
| Aggregation1 - Count | 2 | 2 |
| Boolean1 - And logical operations | 30 | 30 |
| Boolean2 - OR logical operations | 30 | 30 |
| Boolean3 - XOR logical operations | 30 | 30 |
| Boolean4 - NOT logical operations | 52 | 52 |
| Boolean5 - Interop of logical operations | 8 | 8 |
| Call3 - Assignable-type arguments | 6 | 6 |
| Call4 - Null Arguments | 2 | 2 |
| Comparison3 - Full-Bound Range | 9 | 9 |
| Comparison4 - Combination of Comparisons | 1 | 1 |
| Conditional1 - Coalesce expression | 1 | 1 |
| Conditional2 - Case Expression | 12 | 12 |
| ExistentialSubquery1 - Simple existential subquery | 4 | 4 |
| ExistentialSubquery3 - Nested existential subquery | 3 | 3 |
| Literals1 - Boolean and Null | 6 | 6 |
| Literals4 - Octal integer | 10 | 10 |
| MatchWhere1 - Filter single variable | 15 | 15 |
| MatchWhere2 - Filter multiple variables | 2 | 2 |
| MatchWhere3 - Equi-Joins on variables | 3 | 3 |
| MatchWhere4 - Non-Equi-Joins on variables | 2 | 2 |
| MatchWhere5 - Filter on predicate resulting in null | 4 | 4 |
| Mathematical11 - Signed numbers functions | 1 | 1 |
| Mathematical13 - Square root | 1 | 1 |
| Mathematical2 - Addition | 1 | 1 |
| Null1 - IS NULL validation | 17 | 17 |
| Null2 - IS NOT NULL validation | 17 | 17 |
| Precedence1 - On boolean values | 72 | 72 |
| Quantifier5 - None quantifier interop | 31 | 31 |
| Quantifier6 - Single quantifier interop | 21 | 21 |
| Quantifier7 - Any quantifier interop | 36 | 36 |
| Quantifier8 - All quantifier interop | 31 | 31 |
| Return1 - Return single variable | 2 | 2 |
| Return8 - Return clause interoperation with other clauses | 1 | 1 |
| ReturnOrderBy3 - Order by multiple expressions | 1 | 1 |
| ReturnOrderBy4 - Order by in combination with projection | 2 | 2 |
| String11 - Combining Exact String Search | 2 | 2 |
| Temporal6 - Render Temporal Values as a String | 17 | 17 |
| Temporal8 - Compute Arithmetic Operations on Temporal Values | 27 | 27 |
| With2 - Forward single expression | 2 | 2 |
| With5 - Implicit grouping with DISTINCT | 2 | 2 |
| WithOrderBy3 - Order by multiple expressions | 93 | 93 |
| WithWhere2 - Filter multiple variables | 2 | 2 |
| WithWhere3 - Equi-Joins on variables | 3 | 3 |
| WithWhere4 - Non-Equi-Joins on variables | 2 | 2 |
| WithWhere5 - Filter on predicate resulting in null | 4 | 4 |
| WithWhere6 - Filter on aggregates | 1 | 1 |
| WithSkipLimit1 - Skip | 2 | 2 |
| WithSkipLimit2 - Limit | 4 | 4 |

### High Pass Rate (>75%)

| Feature | Passed | Total | Rate |
|---------|--------|-------|------|
| Match1 - Match nodes | 85 | 86 | 98.8% |
| Match2 - Match relationships | 83 | 86 | 96.5% |
| Literals5 - Float | 26 | 27 | 96.3% |
| Literals8 - Maps | 26 | 27 | 96.3% |
| Temporal1 - Create Temporal Values from a Map | 199 | 207 | 96.1% |
| Temporal7 - Compare Temporal Values | 17 | 18 | 94.4% |
| Comparison1 - Equality | 40 | 43 | 93.0% |
| Literals6 - String | 12 | 13 | 92.3% |
| Literals2 - Decimal integer | 11 | 12 | 91.7% |
| Precedence4 - On null value | 11 | 12 | 91.7% |
| Quantifier1 - None quantifier | 96 | 105 | 91.4% |
| Quantifier3 - Any quantifier | 96 | 105 | 91.4% |
| Temporal10 - Compute Durations Between two Temporal Values | 119 | 131 | 90.8% |
| Quantifier4 - All quantifier | 95 | 105 | 90.5% |
| Null3 - Null evaluation | 9 | 10 | 90.0% |
| Quantifier2 - Single quantifier | 95 | 106 | 89.6% |
| List5 - List Membership Validation - IN Operator | 41 | 46 | 89.1% |
| String8 - Exact String Prefix Search | 8 | 9 | 88.9% |
| String9 - Exact String Suffix Search | 8 | 9 | 88.9% |
| String10 - Exact Substring Search | 8 | 9 | 88.9% |
| MatchWhere6 - Filter optional matches | 7 | 8 | 87.5% |
| Match6 - Match named paths | 84 | 97 | 86.6% |
| Graph9 - Retrieve all properties as a property map | 6 | 7 | 85.7% |
| Call5 - Results projection | 16 | 19 | 84.2% |
| With1 - Forward single variable | 5 | 6 | 83.3% |
| WithOrderBy2 - Order by a single expression | 68 | 83 | 81.9% |
| Literals3 - Hexadecimal integer | 13 | 16 | 81.2% |
| Return5 - Implicit grouping with distinct | 4 | 5 | 80.0% |
| Union1 - Union | 4 | 5 | 80.0% |
| Union2 - Union All | 4 | 5 | 80.0% |
| Temporal9 - Truncate Temporal Values | 255 | 322 | 79.2% |
| Map2 - Dynamic Value Access | 11 | 14 | 78.6% |
| Precedence2 - On numeric values | 20 | 26 | 76.9% |
| Match3 - Match fixed length patterns | 23 | 30 | 76.7% |

### Medium Pass Rate (25-75%)

| Feature | Passed | Total | Rate |
|---------|--------|-------|------|
| Call1 - Basic procedure calling | 12 | 16 | 75.0% |
| Temporal5 - Access Components of Temporal Values | 5 | 7 | 71.4% |
| WithOrderBy1 - Order by a single variable | 65 | 96 | 67.7% |
| Call2 - Procedure arguments | 4 | 6 | 66.7% |
| Call6 - Call clause interoperation with other clauses | 2 | 3 | 66.7% |
| ExistentialSubquery2 - Full existential subquery | 2 | 3 | 66.7% |
| Path2 - Relationships of a path | 2 | 3 | 66.7% |
| Return3 - Return multiple expressions | 2 | 3 | 66.7% |
| WithSkipLimit3 - Skip and limit | 2 | 3 | 66.7% |
| Literals7 - List | 13 | 20 | 65.0% |
| Unwind1 | 9 | 14 | 64.3% |
| Map1 - Static value access | 12 | 19 | 63.2% |
| Graph8 - Property keys function | 5 | 8 | 62.5% |
| ReturnSkipLimit2 - Limit | 10 | 17 | 58.8% |
| Comparison2 - Half-bounded Range | 11 | 19 | 57.9% |
| List3 - List Equality | 4 | 7 | 57.1% |
| Match7 - Optional match | 20 | 31 | 64.5% |
| Graph4 - Edge relationship type | 6 | 11 | 54.5% |
| Precedence3 - On list values | 6 | 11 | 54.5% |
| Temporal4 - Store Temporal Values | 21 | 39 | 53.8% |
| List11 - Create a list from a range | 34 | 67 | 50.7% |
| Delete3 - Deleting named paths | 1 | 2 | 50.0% |
| Mathematical8 - Arithmetic precedence | 1 | 2 | 50.0% |
| ReturnOrderBy1 - Order by a single variable | 6 | 12 | 50.0% |
| ReturnOrderBy2 - Order by a single expression | 7 | 14 | 50.0% |
| TypeConversion4 - To String | 7 | 14 | 50.0% |
| With7 - WITH on WITH | 1 | 2 | 50.0% |
| CountingSubgraphMatches1 - Matching subgraph patterns | 5 | 11 | 45.5% |
| Match9 - Match deprecated scenarios | 4 | 9 | 44.4% |
| ReturnSkipLimit1 - Skip | 5 | 11 | 45.5% |
| TypeConversion3 - To Float | 5 | 11 | 45.5% |
| WithOrderBy4 - Order by in combination with projection and aliasing | 10 | 20 | 50.0% |
| Return6 - Implicit grouping with aggregates | 9 | 21 | 42.9% |
| With4 - Variable aliasing | 3 | 7 | 42.9% |
| TypeConversion2 - To Integer | 5 | 12 | 41.7% |
| List2 - List Slicing | 6 | 15 | 40.0% |
| Remove2 - Remove a Label | 2 | 5 | 40.0% |
| ReturnOrderBy6 - Aggregation expressions in order by | 2 | 5 | 40.0% |
| TypeConversion1 - To Boolean | 4 | 10 | 40.0% |
| Return2 - Return single expression | 7 | 18 | 38.9% |
| Temporal3 - Project Temporal Values from other Temporal Values | 67 | 183 | 36.6% |
| Return4 - Column renaming | 4 | 11 | 36.4% |
| Pattern1 - Pattern predicate | 16 | 39 | 41.0% |
| List6 - List size | 6 | 17 | 35.3% |
| Create1 - Creating nodes | 7 | 20 | 35.0% |
| Delete4 - Delete clause interoperation with other clauses | 1 | 3 | 33.3% |
| Match8 - Match clause interoperation with other clauses | 2 | 3 | 66.7% |
| ReturnSkipLimit3 - Skip and limit | 1 | 3 | 33.3% |
| With6 - Implicit grouping with aggregates | 3 | 9 | 33.3% |
| WithWhere7 - Variable visibility under aliasing | 1 | 3 | 33.3% |
| Temporal2 - Create Temporal Values from a String | 16 | 53 | 30.2% |
| Remove1 - Remove a Property | 2 | 7 | 28.6% |
| Aggregation8 - DISTINCT | 1 | 4 | 25.0% |
| Create2 - Creating relationships | 6 | 24 | 25.0% |
| Delete1 - Deleting nodes | 2 | 8 | 25.0% |
| WithWhere1 - Filter single variable | 1 | 4 | 25.0% |

### Low Pass Rate (<25%)

| Feature | Passed | Total | Rate |
|---------|--------|-------|------|
| Graph3 - Node labels | 2 | 9 | 22.2% |
| List1 - Dynamic Element Access | 5 | 23 | 21.7% |
| Graph6 - Static property access | 3 | 14 | 21.4% |
| Match4 - Match variable length patterns | 2 | 10 | 20.0% |
| Map3 - Keys function | 2 | 11 | 18.2% |
| List12 - List Comprehension | 1 | 7 | 14.3% |
| Quantifier10 - Single quantifier invariants | 1 | 8 | 12.5% |
| Set3 - Set a Label | 1 | 8 | 12.5% |
| Delete5 - Delete clause interoperation with built-in data types | 1 | 9 | 11.1% |
| Graph5 - Node and edge label expressions | 1 | 9 | 11.1% |
| TriadicSelection1 - Query three related nodes on binary-tree graphs | 2 | 19 | 10.5% |
| Match5 - Match variable length patterns over given graphs | 3 | 29 | 10.3% |
| Pattern2 - Pattern Comprehension | 1 | 11 | 9.1% |
| Set1 - Set a Property | 1 | 11 | 9.1% |

### 0% Pass Rate (Fully Failing)

| Feature | Total |
|---------|-------|
| Aggregation2 - Min and Max | 12 |
| Aggregation3 - Sum | 2 |
| Aggregation5 - Collect | 2 |
| Aggregation6 - Percentiles | 13 |
| Create3 - Interoperation with other clauses | 13 |
| Create4 - Large Create Query | 2 |
| Create5 - Multiple hops create patterns | 5 |
| Create6 - Persistence of create clause side effects | 14 |
| Delete2 - Deleting relationships | 5 |
| Delete6 - Persistence of delete clause side effects | 14 |
| Graph7 - Dynamic property access | 3 |
| List4 - List Concatenation | 2 |
| List9 - List Tail | 1 |
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
| Path3 - Length of a path | 3 |
| Quantifier9 - None quantifier invariants | 17 |
| Quantifier11 - Any quantifier invariants | 22 |
| Quantifier12 - All quantifier invariants | 17 |
| Remove3 - Persistence of remove clause side effects | 21 |
| Return7 - Return all variables | 2 |
| ReturnOrderBy5 - Order by in combination with column renaming | 1 |
| Set2 - Set a Property to Null | 3 |
| Set4 - Set all properties with a map | 5 |
| Set5 - Set multiple properties with a map | 5 |
| Set6 - Persistence of set clause side effects | 21 |
| String1 - Substring extraction | 1 |
| String3 - String Reversal | 1 |
| String4 - String Splitting | 1 |
| Union3 - Union in combination with Union All | 2 |
| With3 - Forward multiple expressions | 1 |

---

## Step-Level Failure Breakdown

| Failure Type | Steps | % of Failed |
|-------------|-------|-------------|
| No match found for actual row (extra rows) | ~400 | 31.7% |
| No result found (query returns empty) | ~310 | 24.6% |
| Result mismatch (wrong values) | ~190 | 15.1% |
| Other failures | ~200 | 15.9% |
| No error found (Uni too permissive) | ~100 | 7.9% |
| Error detail mismatch (wrong error keyword) | ~60 | 4.8% |

Note: A single scenario failure may involve multiple step-level failure types.

---

## Remaining Validation Gaps (Expected Errors Not Raised)

### Errors Not Raised (~100 scenarios)

Uni accepts queries that openCypher rejects:

| Count | Expected Error | Description |
|-------|---------------|-------------|
| ~25 | InvalidArgumentValue | Invalid argument values |
| ~20 | UndefinedVariable | Variables used before definition |
| ~12 | InvalidArgumentType | Wrong type for operations |
| ~8 | UnexpectedSyntax | Syntax that should be rejected |
| ~6 | NumberOutOfRange | Number out of valid range |
| ~5 | IntegerOverflow | Integer literal overflow |
| ~4 | DeletedEntityAccess | Access to deleted entity |
| ~20 | Various | Other validation gaps |

### Error Detail Mismatches (~60 scenarios)

An error is raised but the message doesn't contain the expected keyword.

---

## Failure Root Causes

1. **Result Mismatch (~600 step failures)**
   - Most common failure type
   - Root causes: missing functions (aggregation, string, path), incorrect query execution
   - Includes: wrong values, empty results, extra/missing rows

2. **No Result Found (~310 step failures)**
   - Query returns empty when data is expected
   - Root causes: graph fixture loading failures, missing query plan steps

3. **Over-Permissive Behavior (~100 remaining "no error" failures)**
   - Uni accepts queries that openCypher rejects
   - Still needs: InvalidArgumentValue, more UndefinedVariable, NumberOutOfRange

4. **Unimplemented Features (~130 failures)**
   - MERGE mostly not implemented (75 scenarios)
   - Aggregation functions partially working (32 still failing)
   - Path functions not implemented (7 scenarios)
   - Variable-length paths not implemented (39 scenarios)

5. **TCK Harness Gaps (~70 step failures)**
   - Parser now supports multi-label nodes and path literals
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
| MATCH | Strong | 80.3% | Node/edge patterns strong, variable-length paths improving. |
| MATCH WHERE | Excellent | 97.1% | Simple filters, joins, null predicates all work |
| RETURN | Moderate | 46.0% | Core works, aggregates improving |
| CREATE | Limited | 16.7% | Basic node/edge creation works |
| DELETE | Limited | 12.2% | Basic delete works, interop issues |
| SET | Limited | 3.8% | Basic property setting works |
| REMOVE | Limited | 12.1% | Basic functionality present |
| MERGE | None | 0.0% | Not implemented |
| WITH | Moderate | 55.2% | Piping works, DISTINCT now 100% |
| WITH ORDER BY | Strong | 80.8% | Comprehensive ordering support |
| WITH WHERE | Good | 73.7% | Filter after WITH working |
| WITH SKIP/LIMIT | Excellent | 88.9% | Skip and Limit now 100% |
| UNWIND | Good | 64.3% | List unwinding mostly works |
| UNION | Good | 66.7% | Basic union works |
| CALL | Strong | 80.8% | Procedure infrastructure working |

### Expressions

| Category | Status | Pass Rate | Notes |
|----------|--------|-----------|-------|
| Conditional (CASE/COALESCE) | Excellent | 100.0% | Fully supported |
| MatchWhere | Excellent | 97.1% | Filtering fully supported |
| Boolean | Excellent | 100.0% | AND, OR, NOT, XOR all fully passing |
| ExistentialSubquery | Strong | 90.0% | EXISTS patterns working |
| Literals | Strong | 89.3% | Booleans, integers, floats, strings, maps, lists all work |
| Null handling | Strong | 97.7% | Three-valued logic correct |
| Comparison | Strong | 84.7% | Equality now 93.0%, full-bound range 100% |
| Quantifiers | Strong | 83.1% | ALL, ANY, NONE, SINGLE well-supported |
| String | Strong | 81.2% | STARTS WITH, ENDS WITH, CONTAINS working well |
| Temporal | Good | 74.0% | Creation, truncation, arithmetic, comparison, duration-between, formatting |
| Precedence | Strong | 90.1% | Boolean 100%, numeric 76.9%, list values now 54.5% |
| List | Moderate | 52.4% | IN operator 89.1%, ranges work, comprehension weak |
| Type Conversion | Moderate | 44.7% | toString partially works |
| Map | Moderate | 56.8% | Static and dynamic access improved |
| CountingSubgraphMatches | Moderate | 45.5% | Newly passing scenarios |
| Pattern | Limited | 34.0% | Pattern predicates improving |
| Path | Limited | 28.6% | relationships() function working |
| Graph | Limited | 37.7% | Property access works, labels/types weak |
| Aggregation | Limited | 8.6% | COUNT now 100%, others still need work |

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
| 2026-02-05 | 2,186 | 56.5% | MatchWhere fixes (+24): type coercion, NULL handling, OPTIONAL MATCH, WHERE clause |
| 2026-02-06 (morning) | 2,502 | 64.7% | Major improvements: EXISTS (+7), WithOrderBy (+141), MatchWhere (+6), List (+21), Pattern (+12), String (+6) |
| 2026-02-06 (evening) | 2,503 | 64.7% | TCK harness parser fixes: multi-label nodes, path literals (+1) |
| 2026-02-06 (PM) | 2,469 | 63.8% | Path normalization, property normalization fix (`normalize_property_value()`) |
| 2026-02-06 (evening) | 2,467 | 63.3% | Minor regression from path changes |
| 2026-02-07 (AM) | 2,507 | 64.3% | Map/List literal fixes, Match pattern improvements |
| 2026-02-07 (PM) | 2,599 | 66.7% | Boolean NOT/XOR, String search, Precedence, Quantifier improvements (+92) |
| 2026-02-07 (evening) | 2,601 | 66.7% | Minor improvements (+2) |
| 2026-02-07 (late evening) | 2,620 | 67.2% | Dynamic access improvements: Map2, Graph, List (+19) |
| 2026-02-08 (AM) | 2,623 | 67.3% | Verification run (+3 scenarios) |
| 2026-02-08 (PM) | 2,683 | 68.8% | Boolean ops now 100%, Comparison3/4 now 100%, Null1/2 now 100%, Precedence1 now 100%, WithOrderBy3 now 100% (+60) |
| 2026-02-08 (late PM) | 2,698 | 69.2% | Comparison1 +10, List5 +7, Precedence3 +6, Graph6 +2, Return2 +2, List3 +1. Zero regressions. (+15) |
| 2026-02-08 (evening) | 2,720 | 69.8% | Aggregation1 now 100%, CountingSubgraphMatches1 +5, Comparison1 +4, Return6 +3, plus smaller gains. Zero regressions. (+22) |
| 2026-02-08 (night) | **2,731** | **70.0%** | **70% MILESTONE!** Match7 +3, Match9 +3, Match4 +2, Pattern1 +2. Regressions: Match6 -4, Graph7 -1, Return7 -1. Net +11 scenarios. |

### Cumulative Improvement

| From | To | Scenarios Gained | Improvement |
|------|-----|------------------|-------------|
| Baseline (1,279) | Current (2,731) | **+1,452** | **+113.5%** |

---

## Key Gaps to Address

### High Priority (blocking many tests)

1. **Aggregation Functions** (32 remaining failures)
   - COUNT now works (Aggregation1 100%)
   - SUM, AVG, MIN, MAX, COLLECT, percentiles still need work
   - Core feature needed for many query patterns

2. **MERGE Implementation** (72 failures)
   - Upsert semantics not implemented
   - ON CREATE / ON MATCH clauses

3. **Path Functions** (7 failures)
   - nodes(), relationships(), length() not implemented
   - Required for path processing queries

4. **Variable-Length Paths** (37 failures in Match4/Match5)
   - `(a)-[*1..3]->(b)` patterns
   - Match4 now at 20% (2/10), Match5 at 10.3% (3/29)
   - Requires iterative path expansion

### Medium Priority

5. **List Operations** (remaining ~70 failures)
   - List concatenation, comprehension, tail
   - Dynamic element access improvements

6. **TCK Harness Side Effects** (~70 failures)
   - CREATE/DELETE/SET/REMOVE side effect verification
   - Persistence checks not implemented

7. **Over-Permissive Validation** (~100 failures)
   - InvalidArgumentValue, UndefinedVariable, NumberOutOfRange

### Low Priority

8. **Named Graph Fixtures** (19 failures)
   - Implement binary-tree-1/2 fixtures

9. **Quantifier Invariants** (56 failures)
   - Edge cases in ALL/ANY/NONE/SINGLE

---

## Recommendations

### Short-term Wins
1. Implement remaining aggregation (SUM, AVG, MIN, MAX) -- could unlock ~32 scenarios
2. Add path functions (nodes, relationships, length) -- could unlock ~7 scenarios
3. Fix list concatenation -- could unlock ~2 scenarios

### Medium-term Goals
1. Implement MERGE clause (~75 scenarios)
2. Add variable-length path patterns (~39 scenarios)
3. Add TCK side effect verification (~70 scenarios)

### Long-term Goals
1. Achieve 75%+ scenario pass rate
2. Complete MERGE support
3. Quantifier invariant edge cases

### Next Milestone Target

| Target | Scenarios | Pass Rate | Gap |
|--------|-----------|-----------|-----|
| ~~55%~~ | ~~2,127~~ | ~~55.0%~~ | **ACHIEVED** |
| ~~60%~~ | ~~2,321~~ | ~~60.0%~~ | **ACHIEVED** |
| ~~65%~~ | ~~2,533~~ | ~~65.0%~~ | **ACHIEVED** |
| ~~70%~~ | ~~2,727~~ | ~~70.0%~~ | **ACHIEVED** |
| 75% | 2,922 | 75.0% | +191 scenarios |
| 80% | 3,117 | 80.0% | +386 scenarios |

Achieving 75% likely requires:
- Remaining aggregation fixes (MIN, MAX, SUM, AVG) -- could unlock ~32 scenarios
- Path functions (nodes, relationships, length) -- could unlock ~7 scenarios
- MERGE clause -- could unlock ~75 scenarios
- Variable-length path patterns (Match4/Match5) -- remaining ~37 scenarios

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

# Run with report generation (recommended)
scripts/run_tck_with_report.sh

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

**Total:** 192 feature files, 3,897 expanded scenarios
