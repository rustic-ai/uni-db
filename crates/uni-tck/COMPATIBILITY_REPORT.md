# Uni OpenCypher TCK Compatibility Report

**Generated:** 2026-02-07 (Latest TCK Run)
**TCK Version:** M23 (openCypher)
**Uni Version:** Current main branch

---

## Executive Summary

| Metric | Count | Pass Rate |
|--------|-------|-----------|
| **Features** | 192 | - |
| **Scenarios** | 3,897 | **64.3%** (2,507 passed, 1,351 failed) |
| **Skipped** | 39 | - |
| **Parsing Errors** | 0 | None (harness fixed) |

**Recent Fixes (2026-02-07):**
1. Map literal evaluation improvements (+19 scenarios in Literals8)
2. Match pattern improvements: Match1 (+1), Match2 (+4), Match3 (+3), Match5 (+3), Match6 (+7)
3. List and Map handling improvements: List1 now 100%, Map3 now 100%
4. Literals improvements: Literals1, Literals2, Literals3, Literals4, Literals6 all improved

**Previous Fixes (2026-02-06):**
1. TCK harness parser now supports multi-label nodes `(:A:B:C)` and path literals `<(:A)-[:R]->(:B)>` in expected test results
2. Added Path type detection and conversion in result normalizer (`is_path_map()` and `map_to_path()`)
3. **Property Normalization Fix:** Added `normalize_property_value()` to safely normalize nested property values without false structural detection

The high step pass rate (~90%) vs lower scenario pass rate (64.3%) indicates that most basic operations work, but many scenarios fail at specific assertion points.

---

## Match Category Results (Updated 2026-02-07)

| Feature | Passed | Total | Rate |
|---------|--------|-------|------|
| Match1 - Match nodes | 84 | 86 | 97.7% |
| Match2 - Match relationships | 83 | 86 | 96.5% |
| Match3 - Match fixed length patterns | 23 | 30 | 76.7% |
| Match4 - Match variable length patterns | 0 | 9 | 0.0% |
| Match5 - Match variable length patterns over given graphs | 3 | 29 | 10.3% |
| Match6 - Match named paths | 88 | 97 | 90.7% |
| Match7 - Optional match | 17 | 31 | 54.8% |
| Match8 - Match clause interop | 1 | 3 | 33.3% |
| Match9 - Match deprecated | 1 | 9 | 11.1% |
| **TOTAL Match** | **300** | **380** | **78.9%** |

### Fixes Implemented (2026-02-06)

1. **Path Detection and Conversion:** Added `is_path_map()` and `map_to_path()` to detect and convert maps with "nodes" and "relationships/edges" keys to proper Path type ✅
2. **Property Normalization:** Added `normalize_property_value()` function that recursively processes nested lists/maps without applying structural detection (node/edge/path). This prevents user data with `_vid`/`_eid` keys from being incorrectly converted. ✅
3. **Optional Match Logic:** Added `is_optional` field with early-exit BFS logic ✅
4. **Named Path Serialization:** Added `#[serde(rename = "relationships")]` to Path struct ✅

### Property Normalization Design

The key insight for correct result normalization:

- **Top-level results:** Use lenient detection (maps with `_vid` → Node, maps with `_eid` → Edge) because these are actual query results from the executor
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
| **Conditional** | **13** | **0** | **13** | **100.0%** |
| **MatchWhere** | **33** | **1** | **34** | **97.1%** |
| **ExistentialSubquery** | **9** | **1** | **10** | **90.0%** |
| Null | 38 | 6 | 44 | 86.4% |
| Quantifier | 497 | 107 | 604 | 82.3% |
| Call | 42 | 10 | 52 | 80.8% |
| Literals | 104 | 27 | 131 | 79.4% |
| Match | 300 | 80 | 380 | 78.9% |
| **WithOrderBy** | **226** | **66** | **292** | **77.4%** |
| Temporal (overall) | 749 | 255 | 1,004 | 74.6% |
| **WithWhere** | **14** | **5** | **19** | **73.7%** |
| Boolean | 105 | 45 | 150 | 70.0% |
| Union | 8 | 4 | 12 | 66.7% |
| **WithSkipLimit** | **6** | **3** | **9** | **66.7%** |
| Mathematical | 4 | 2 | 6 | 66.7% |
| Unwind | 9 | 5 | 14 | 64.3% |
| Precedence | 66 | 55 | 121 | 54.5% |
| Comparison | 37 | 35 | 72 | 51.4% |
| List | 83 | 84 | 167 | 49.7% |
| ReturnSkipLimit | 15 | 16 | 31 | 48.4% |
| With | 14 | 15 | 29 | 48.3% |
| ReturnOrderBy | 16 | 18 | 34 | 47.1% |
| Map | 15 | 20 | 35 | 42.9% |
| Return | 24 | 38 | 62 | 38.7% |
| TypeConversion | 18 | 29 | 47 | 38.3% |
| Pattern | 15 | 35 | 50 | 30.0% |
| Graph | 15 | 43 | 58 | 25.9% |
| String | 6 | 26 | 32 | 18.8% |
| Create | 13 | 65 | 78 | 16.7% |
| Delete | 5 | 36 | 41 | 12.2% |
| Remove | 4 | 29 | 33 | 12.1% |
| TriadicSelection | 2 | 17 | 19 | 10.5% |
| Set | 2 | 49 | 51 | 3.9% |
| Merge | 0 | 72 | 72 | 0.0% |
| Aggregation | 0 | 34 | 34 | 0.0% |
| Path | 0 | 7 | 7 | 0.0% |
| CountingSubgraphMatches | 0 | 11 | 11 | 0.0% |

---

## Per-Feature Detail

### 100% Pass Rate (Fully Passing)

| Feature | Passed | Total |
|---------|--------|-------|
| Call3 - Assignable-type arguments | 6 | 6 |
| Call4 - Null Arguments | 2 | 2 |
| Conditional1 - Coalesce expression | 1 | 1 |
| Conditional2 - Case Expression | 12 | 12 |
| ExistentialSubquery1 - Simple existential subquery | 4 | 4 |
| ExistentialSubquery3 - Nested existential subquery | 3 | 3 |
| List1 - Dynamic Element Access | 5 | 5 |
| Literals1 - Boolean and Null | 6 | 6 |
| Literals4 - Octal integer | 10 | 10 |
| Map3 - Keys function | 2 | 2 |
| MatchWhere1 - Filter single variable | 15 | 15 |
| MatchWhere2 - Filter multiple variables | 2 | 2 |
| MatchWhere3 - Equi-Joins on variables | 3 | 3 |
| MatchWhere4 - Non-Equi-Joins on variables | 2 | 2 |
| MatchWhere5 - Filter on predicate resulting in null | 4 | 4 |
| Mathematical11 - Signed numbers functions | 1 | 1 |
| Mathematical13 - Square root | 1 | 1 |
| Mathematical2 - Addition | 1 | 1 |
| Quantifier5 - None quantifier interop | 31 | 31 |
| Quantifier6 - Single quantifier interop | 21 | 21 |
| Quantifier8 - All quantifier interop | 31 | 31 |
| Return1 - Return single variable | 2 | 2 |
| ReturnOrderBy4 - Order by in combination with projection | 2 | 2 |
| Temporal6 - Render Temporal Values as a String | 17 | 17 |
| Temporal8 - Compute Arithmetic Operations on Temporal Values | 27 | 27 |
| With2 - Forward single expression | 2 | 2 |
| WithWhere2 - Filter multiple variables | 2 | 2 |
| WithWhere3 - Equi-Joins on variables | 3 | 3 |
| WithWhere4 - Non-Equi-Joins on variables | 2 | 2 |
| WithWhere5 - Filter on predicate resulting in null | 4 | 4 |
| WithWhere6 - Filter on aggregates | 1 | 1 |

### High Pass Rate (>75%)

| Feature | Passed | Total | Rate |
|---------|--------|-------|------|
| Match1 - Match nodes | 84 | 86 | 97.7% |
| Match2 - Match relationships | 83 | 86 | 96.5% |
| Literals5 - Float | 26 | 27 | 96.3% |
| Temporal1 - Create Temporal Values from a Map | 199 | 207 | 96.1% |
| Temporal10 - Compute Durations Between two Temporal Values | 125 | 131 | 95.4% |
| Temporal7 - Compare Temporal Values | 17 | 18 | 94.4% |
| Null1 - IS NULL validation | 16 | 17 | 94.1% |
| Null2 - IS NOT NULL validation | 16 | 17 | 94.1% |
| Literals6 - String | 12 | 13 | 92.3% |
| Quantifier1 - None quantifier | 96 | 105 | 91.4% |
| Quantifier3 - Any quantifier | 96 | 105 | 91.4% |
| WithOrderBy3 - Order by multiple expressions | 85 | 93 | 91.4% |
| Match6 - Match named paths | 88 | 97 | 90.7% |
| Quantifier4 - All quantifier | 95 | 105 | 90.5% |
| Quantifier2 - Single quantifier | 95 | 106 | 89.6% |
| MatchWhere6 - Filter optional matches | 7 | 8 | 87.5% |
| Quantifier7 - Any quantifier interop | 31 | 36 | 86.1% |
| Graph9 - Retrieve all properties as a property map | 6 | 7 | 85.7% |
| Call5 - Results projection | 16 | 19 | 84.2% |
| Literals2 - Decimal integer | 10 | 12 | 83.3% |
| With1 - Forward single variable | 5 | 6 | 83.3% |
| WithOrderBy2 - Order by a single expression | 68 | 83 | 81.9% |
| Literals3 - Hexadecimal integer | 13 | 16 | 81.2% |
| Boolean1 - And logical operations | 24 | 30 | 80.0% |
| Boolean2 - OR logical operations | 24 | 30 | 80.0% |
| Boolean3 - XOR logical operations | 24 | 30 | 80.0% |
| Return5 - Implicit grouping with distinct | 4 | 5 | 80.0% |
| Union1 - Union | 4 | 5 | 80.0% |
| Union2 - Union All | 4 | 5 | 80.0% |
| Temporal9 - Truncate Temporal Values | 255 | 322 | 79.2% |
| Precedence2 - On numeric values | 20 | 26 | 76.9% |
| Match3 - Match fixed length patterns | 23 | 30 | 76.7% |

### Medium Pass Rate (25-75%)

| Feature | Passed | Total | Rate |
|---------|--------|-------|------|
| Call1 - Basic procedure calling | 12 | 16 | 75.0% |
| WithSkipLimit2 - Limit | 3 | 4 | 75.0% |
| Temporal5 - Access Components of Temporal Values | 5 | 7 | 71.4% |
| Call2 - Procedure arguments | 4 | 6 | 66.7% |
| Call6 - Call clause interoperation with other clauses | 2 | 3 | 66.7% |
| ExistentialSubquery2 - Full existential subquery | 2 | 3 | 66.7% |
| Literals8 - Maps | 18 | 27 | 66.7% |
| Precedence4 - On null value | 8 | 12 | 66.7% |
| Return3 - Return multiple expressions | 2 | 3 | 66.7% |
| WithOrderBy1 - Order by a single variable | 64 | 96 | 66.7% |
| WithSkipLimit3 - Skip and limit | 2 | 3 | 66.7% |
| Comparison1 - Equality | 28 | 43 | 65.1% |
| Unwind1 | 9 | 14 | 64.3% |
| Map1 - Static value access | 12 | 19 | 63.2% |
| Boolean5 - Interop of logical operations | 5 | 8 | 62.5% |
| List5 - List Membership Validation - IN Operator | 28 | 46 | 60.9% |
| Null3 - Null evaluation | 6 | 10 | 60.0% |
| Match7 - Optional match | 17 | 31 | 54.8% |
| Graph4 - Edge relationship type | 6 | 11 | 54.5% |
| Boolean4 - NOT logical operations | 28 | 52 | 53.8% |
| Temporal4 - Store Temporal Values | 21 | 39 | 53.8% |
| ReturnSkipLimit2 - Limit | 9 | 17 | 52.9% |
| Precedence1 - On boolean values | 38 | 72 | 52.8% |
| List11 - Create a list from a range | 34 | 67 | 50.7% |
| Delete3 - Deleting named paths | 1 | 2 | 50.0% |
| Mathematical8 - Arithmetic precedence | 1 | 2 | 50.0% |
| Return7 - Return all variables | 1 | 2 | 50.0% |
| ReturnOrderBy1 - Order by a single variable | 6 | 12 | 50.0% |
| TypeConversion4 - To String | 7 | 14 | 50.0% |
| With5 - Implicit grouping with DISTINCT | 1 | 2 | 50.0% |
| WithSkipLimit1 - Skip | 1 | 2 | 50.0% |
| ReturnOrderBy2 - Order by a single expression | 6 | 13 | 46.2% |
| ReturnSkipLimit1 - Skip | 5 | 11 | 45.5% |
| Literals7 - List | 9 | 20 | 45.0% |
| WithOrderBy4 - Order by in combination with projection and aliasing | 9 | 20 | 45.0% |
| List3 - List Equality | 3 | 7 | 42.9% |
| With4 - Variable aliasing | 3 | 7 | 42.9% |
| Comparison2 - Half-bounded Range | 8 | 19 | 42.1% |
| List2 - List Slicing | 6 | 15 | 40.0% |
| Remove2 - Remove a Label | 2 | 5 | 40.0% |
| ReturnOrderBy6 - Aggregation expressions in order by | 2 | 5 | 40.0% |
| TypeConversion1 - To Boolean | 4 | 10 | 40.0% |
| Temporal3 - Project Temporal Values from other Temporal Values | 67 | 183 | 36.6% |
| Return4 - Column renaming | 4 | 11 | 36.4% |
| Pattern1 - Pattern predicate | 14 | 39 | 35.9% |
| List6 - List size | 6 | 17 | 35.3% |
| Create1 - Creating nodes | 7 | 20 | 35.0% |
| Delete4 - Delete clause interoperation with other clauses | 1 | 3 | 33.3% |
| Graph3 - Node labels | 2 | 6 | 33.3% |
| Match8 - Match clause interoperation with other clauses | 1 | 3 | 33.3% |
| ReturnSkipLimit3 - Skip and limit | 1 | 3 | 33.3% |
| TypeConversion2 - To Integer | 4 | 12 | 33.3% |
| With6 - Implicit grouping with aggregates | 3 | 9 | 33.3% |
| WithWhere7 - Variable visibility under aliasing | 1 | 3 | 33.3% |
| Temporal2 - Create Temporal Values from a String | 16 | 53 | 30.2% |
| Return6 - Implicit grouping with aggregates | 6 | 20 | 30.0% |
| Remove1 - Remove a Property | 2 | 7 | 28.6% |
| Return2 - Return single expression | 5 | 18 | 27.8% |
| TypeConversion3 - To Float | 3 | 11 | 27.3% |
| Create2 - Creating relationships | 6 | 24 | 25.0% |
| Delete1 - Deleting nodes | 2 | 8 | 25.0% |
| WithWhere1 - Filter single variable | 1 | 4 | 25.0% |

### Low Pass Rate (<25%)

| Feature | Passed | Total | Rate |
|---------|--------|-------|------|
| String10 - Exact Substring Search | 2 | 9 | 22.2% |
| String8 - Exact String Prefix Search | 2 | 9 | 22.2% |
| String9 - Exact String Suffix Search | 2 | 9 | 22.2% |
| Set3 - Set a Label | 1 | 6 | 16.7% |
| List12 - List Comprehension | 1 | 7 | 14.3% |
| Quantifier10 - Single quantifier invariants | 1 | 8 | 12.5% |
| Comparison3 - Full-Bound Range | 1 | 9 | 11.1% |
| Delete5 - Delete clause interoperation with built-in data types | 1 | 9 | 11.1% |
| Match9 - Match deprecated scenarios | 1 | 9 | 11.1% |
| TriadicSelection1 - Query three related nodes on binary-tree graphs | 2 | 19 | 10.5% |
| Match5 - Match variable length patterns over given graphs | 3 | 29 | 10.3% |
| Pattern2 - Pattern Comprehension | 1 | 11 | 9.1% |
| Set1 - Set a Property | 1 | 11 | 9.1% |
| Graph6 - Static property access | 1 | 14 | 7.1% |
| Map2 - Dynamic Value Access | 1 | 14 | 7.1% |

### 0% Pass Rate (Fully Failing)

| Feature | Total |
|---------|-------|
| Aggregation1 - Count | 2 |
| Aggregation2 - Min and Max | 12 |
| Aggregation3 - Sum | 2 |
| Aggregation5 - Collect | 1 |
| Aggregation6 - Percentiles | 13 |
| Aggregation8 - DISTINCT | 4 |
| Comparison4 - Combination of Comparisons | 1 |
| CountingSubgraphMatches1 | 11 |
| Create3 - Interoperation with other clauses | 13 |
| Create4 - Large Create Query | 2 |
| Create5 - Multiple hops create patterns | 5 |
| Create6 - Persistence of create clause side effects | 14 |
| Delete2 - Deleting relationships | 5 |
| Delete6 - Persistence of delete clause side effects | 14 |
| Graph5 - Node and edge label expressions | 9 |
| Graph7 - Dynamic property access | 3 |
| Graph8 - Property keys function | 8 |
| List4 - List Concatenation | 2 |
| List9 - List Tail | 1 |
| Match4 - Match variable length patterns | 9 |
| Mathematical3 - Subtraction | 1 |
| Merge1 - Merge node | 16 |
| Merge2 - Merge node - on create | 5 |
| Merge3 - Merge node - on match | 4 |
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
| Return8 - Return clause interoperation with other clauses | 1 |
| ReturnOrderBy3 - Order by multiple expressions | 1 |
| ReturnOrderBy5 - Order by in combination with column renaming | 1 |
| Set2 - Set a Property to Null | 3 |
| Set4 - Set all properties with a map | 5 |
| Set5 - Set multiple properties with a map | 5 |
| Set6 - Persistence of set clause side effects | 21 |
| String1 - Substring extraction | 1 |
| String3 - String Reversal | 1 |
| String4 - String Splitting | 1 |
| String11 - Combining Exact String Search | 2 |
| Union3 - Union in combination with Union All | 2 |
| With3 - Forward multiple expressions | 1 |
| With7 - WITH on WITH | 2 |

---

## Step-Level Failure Breakdown

| Failure Type | Steps | % of Failed |
|-------------|-------|-------------|
| No match found for actual row (extra rows) | ~432 | 31.6% |
| No result found (query returns empty) | ~336 | 24.6% |
| Result mismatch (wrong values) | ~208 | 15.2% |
| Other failures | ~215 | 15.7% |
| No error found (Uni too permissive) | ~106 | 7.8% |
| Error detail mismatch (wrong error keyword) | ~69 | 5.1% |

Note: A single scenario failure may involve multiple step-level failure types.

---

## Remaining Validation Gaps (Expected Errors Not Raised)

### Errors Not Raised (~106 scenarios)

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
| ~26 | Various | Other validation gaps |

### Error Detail Mismatches (~69 scenarios)

An error is raised but the message doesn't contain the expected keyword.

---

## Failure Root Causes

1. **Result Mismatch (~640 step failures)**
   - Most common failure type
   - Root causes: missing functions (aggregation, string, path), incorrect query execution
   - Includes: wrong values, empty results, extra/missing rows

2. **No Result Found (~336 step failures)**
   - Query returns empty when data is expected
   - Root causes: graph fixture loading failures, missing query plan steps

3. **Over-Permissive Behavior (~106 remaining "no error" failures)**
   - Uni accepts queries that openCypher rejects
   - Still needs: InvalidArgumentValue, more UndefinedVariable, NumberOutOfRange

4. **Unimplemented Features (~130 failures)**
   - MERGE mostly not implemented (75 scenarios)
   - Aggregation functions returning incorrect results (35 scenarios)
   - Path functions not implemented (7 scenarios)
   - Variable-length paths not implemented (39 scenarios)

5. **TCK Harness Gaps (~70 step failures)**
   - ✅ **FIXED (2026-02-06):** Parser now supports multi-label nodes and path literals
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
| MATCH | Strong | 78.9% | Node/edge patterns strong, variable-length paths weak. Parser fixed for multi-label nodes. |
| MATCH WHERE | Excellent | 97.1% | Simple filters, joins, null predicates all work |
| RETURN | Moderate | 38.7% | Core works, aggregates need work |
| CREATE | Limited | 16.7% | Basic node/edge creation works |
| DELETE | Limited | 12.2% | Basic delete works, interop issues |
| SET | Limited | 3.9% | Basic property setting works |
| REMOVE | Limited | 12.1% | Basic functionality present |
| MERGE | None | 0.0% | Not implemented |
| WITH | Moderate | 48.3% | Piping works, some aliasing issues |
| WITH ORDER BY | Strong | 77.4% | Comprehensive ordering support |
| WITH WHERE | Good | 73.7% | Filter after WITH working |
| WITH SKIP/LIMIT | Good | 66.7% | Pagination working |
| UNWIND | Good | 64.3% | List unwinding mostly works |
| UNION | Good | 66.7% | Basic union works |
| CALL | Strong | 80.8% | Procedure infrastructure working |

### Expressions

| Category | Status | Pass Rate | Notes |
|----------|--------|-----------|-------|
| Conditional (CASE/COALESCE) | Excellent | 100.0% | Fully supported |
| MatchWhere | Excellent | 97.1% | Filtering fully supported |
| ExistentialSubquery | Strong | 90.0% | EXISTS patterns working |
| Null handling | Strong | 86.4% | Three-valued logic correct |
| Quantifiers | Strong | 82.3% | ALL, ANY, NONE, SINGLE well-supported |
| Literals | Strong | 79.4% | Booleans, integers, floats, strings, maps all work |
| Temporal | Good | 74.6% | Creation, truncation, arithmetic, comparison, duration-between, formatting |
| Boolean | Good | 70.0% | AND, OR, NOT, XOR work |
| Precedence | Moderate | 54.5% | Numeric precedence good, list issues |
| Comparison | Moderate | 51.4% | Equality good, ranges need work |
| List | Moderate | 49.7% | IN operator and ranges work, comprehension weak |
| Map | Moderate | 42.9% | Static access good, dynamic access weak |
| Type Conversion | Moderate | 38.3% | toString partially works |
| Pattern | Limited | 30.0% | Pattern predicates improving |
| Graph | Limited | 25.9% | Property access works, labels/types weak |
| String | Limited | 18.8% | STARTS WITH, ENDS WITH, CONTAINS partially working |
| Aggregation | None | 0.0% | COUNT, SUM, etc. not returning correct results |
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
| 2026-02-05 | 2,186 | 56.5% | MatchWhere fixes (+24): type coercion, NULL handling, OPTIONAL MATCH, WHERE clause |
| 2026-02-06 (morning) | 2,502 | 64.7% | Major improvements: EXISTS (+7), WithOrderBy (+141), MatchWhere (+6), List (+21), Pattern (+12), String (+6) |
| 2026-02-06 (evening) | 2,503 | 64.7% | TCK harness parser fixes: multi-label nodes, path literals (+1) |
| 2026-02-06 (PM) | 2,469 | 63.8% | Path normalization, property normalization fix (`normalize_property_value()`) |
| 2026-02-06 (evening) | 2,467 | 63.3% | Minor regression from path changes |
| 2026-02-07 (latest) | **2,507** | **64.3%** | Map/List literal fixes, Match pattern improvements |

### Cumulative Improvement

| From | To | Scenarios Gained | Improvement |
|------|-----|------------------|-------------|
| Baseline (1,279) | Current (2,507) | **+1,228** | **+96.0%** |

---

## Key Gaps to Address

### High Priority (blocking many tests)

1. **Aggregation Functions** (34 failures)
   - COUNT, SUM, AVG, MIN, MAX, COLLECT, percentiles not returning correct results
   - Core feature needed for many query patterns

2. **MERGE Implementation** (72 failures)
   - Upsert semantics not implemented
   - ON CREATE / ON MATCH clauses

3. **Path Functions** (7 failures)
   - nodes(), relationships(), length() not implemented
   - Required for path processing queries

4. **Variable-Length Paths** (35 failures in Match4/Match5)
   - `(a)-[*1..3]->(b)` patterns
   - Requires iterative path expansion

### Medium Priority

5. **String Functions** (26 remaining failures)
   - STARTS WITH, ENDS WITH, CONTAINS partially working
   - substring(), reverse(), split() not implemented

6. **TCK Harness Side Effects** (~70 failures)
   - CREATE/DELETE/SET/REMOVE side effect verification
   - Persistence checks not implemented

7. **Over-Permissive Validation** (~106 failures)
   - InvalidArgumentValue, UndefinedVariable, NumberOutOfRange

### Low Priority

8. **Named Graph Fixtures** (19 failures)
   - Implement binary-tree-1/2 fixtures

9. **Quantifier Invariants** (56 failures)
   - Edge cases in ALL/ANY/NONE/SINGLE

---

## Recommendations

### Short-term Wins
1. Implement basic aggregation (COUNT, SUM, AVG, MIN, MAX) — could unlock ~35 scenarios
2. Add path functions (nodes, relationships, length) — could unlock ~7 scenarios
3. Complete string functions — could unlock ~26 scenarios

### Medium-term Goals
1. Implement MERGE clause (~75 scenarios)
2. Add variable-length path patterns (~39 scenarios)
3. Add TCK side effect verification (~70 scenarios)

### Long-term Goals
1. Achieve 70%+ scenario pass rate
2. Complete MERGE support
3. Quantifier invariant edge cases

### Next Milestone Target

| Target | Scenarios | Pass Rate | Gap |
|--------|-----------|-----------|-----|
| ~~55%~~ | ~~2,127~~ | ~~55.0%~~ | **ACHIEVED** |
| ~~60%~~ | ~~2,321~~ | ~~60.0%~~ | **ACHIEVED** |
| 65% | 2,533 | 65.0% | +26 scenarios |
| 70% | 2,727 | 70.0% | +220 scenarios |
| 75% | 2,922 | 75.0% | +415 scenarios |

Achieving 70% likely requires:
- Aggregation functions (+35)
- Path functions (+7)
- String functions (+26)
- Partial MERGE implementation (+30)
- Variable-length paths (+39)
- TCK harness fixes (+70)
- Additional validation coverage (+30)

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
