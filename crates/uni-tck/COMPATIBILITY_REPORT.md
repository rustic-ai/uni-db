# Uni OpenCypher TCK Compatibility Report

**Generated:** 2026-02-09 (Latest TCK Run - Update 2)
**TCK Version:** M23 (openCypher)
**Uni Version:** Current debug/tck001 branch

---

## Executive Summary

| Metric | Count | Pass Rate |
|--------|-------|-----------|
| **Features** | 192 | - |
| **Scenarios** | 3,926 | **72.05%** (2,799 passed, 1,086 failed) |
| **Skipped** | 41 | - |
| **Parsing Errors** | 0 | None (harness fixed) |

**Latest Update (2026-02-09 - Update 2):**
Pass rate: 72.05% (2,799/3,885 non-skipped scenarios). **+28 passed scenarios, +29 total scenarios from previous run.**

Key improvements in this run:
1. **Match5 (Match variable length patterns over given graphs)** improved from 10.3% to 55.2% (+29 scenarios, **total scenarios increased from 29 to 58**)
2. WithOrderBy2 (Order by a single expression) improved from 80.7% to 83.1% (+2 scenarios)

**⚠️ REGRESSIONS in this run:**
1. **Delete1 (Deleting nodes)** regressed from 50.0% to 12.5% (-3 scenarios) - **MAJOR REGRESSION**
2. **Delete2 (Deleting relationships)** regressed from 40.0% to 20.0% (-1 scenario)
3. **Delete3 (Deleting named paths)** regressed from 50.0% to 0% (-1 scenario)
4. **Overall Delete category** regressed from 22.0% to 12.2% (5/41 passing)

**Previous Run (2026-02-09 - Update 1):**
Pass rate: 71.1% (2,771/3,897 scenarios). **+40 scenarios from previous run.**
Create2 +12, Create3 +10, Create6 +7, Match6 +6, Delete1 +2, Delete2 +2. Regressions: WithOrderBy2 -1, WithSkipLimit1 -1.

**Previous Run (2026-02-08 night):**
Pass rate: 70.0% (2,731/3,897 scenarios). **70% MILESTONE ACHIEVED!** +11 scenarios from previous run.

**Previous Run (2026-02-08 evening):**
Pass rate: 69.8% (2,720/3,897 scenarios). +22 scenarios.
CountingSubgraphMatches1 +5, Comparison1 +4, Return6 +3, Aggregation1 now 100%, plus smaller gains. Zero regressions.

**Previous Run (2026-02-08 late PM):**
Pass rate: 69.2% (2,698/3,897 scenarios). +15 scenarios.
Comparison1 +10, List5 +7, Precedence3 +6, Graph6 +2, Return2 +2, List3 +1. Zero regressions.

**Previous Run (2026-02-08 PM):**
Pass rate: 68.8% (2,683/3,897 scenarios). +60 scenarios.
Boolean ops now 100%, Comparison3/4 now 100%, Null1/2 now 100%, Precedence1 now 100%, WithOrderBy3 now 100%.

The high step pass rate (~91%) vs lower scenario pass rate (72.05%) indicates that most basic operations work, but many scenarios fail at specific assertion points.

---

## Regression Analysis (2026-02-09 - Update 2)

### Delete Clause Regressions ⚠️

The DELETE clause saw significant regressions in this update:

**Delete1 - Deleting nodes:** 4/8 (50.0%) → 1/8 (12.5%) **[-3 scenarios]**
- Only passing: "[5] Ignore null when deleting node"
- Now failing: Basic delete, detach delete, null handling, error cases

**Delete2 - Deleting relationships:** 2/5 (40.0%) → 1/5 (20.0%) **[-1 scenario]**
- Only passing: "[4] Ignore null when deleting relationship"
- Now failing: Basic delete, optional match delete, bidirectional delete, error cases

**Delete3 - Deleting named paths:** 1/2 (50.0%) → 0/2 (0%) **[-1 scenario]**
- Both scenarios now failing

**Root Cause Investigation Needed:**
The regression appears to affect actual deletion operations while null-handling logic still works. This suggests a recent change may have broken the core delete execution path while leaving the null-propagation logic intact.

---

## Match Category Results (Updated 2026-02-09 - Update 2)

| Feature | Passed | Total | Rate |
|---------|--------|-------|------|
| Match1 - Match nodes | 85 | 86 | 98.8% |
| Match2 - Match relationships | 83 | 86 | 96.5% |
| Match3 - Match fixed length patterns | 23 | 30 | 76.7% |
| Match4 - Match variable length patterns | 2 | 10 | 20.0% |
| Match5 - Match variable length patterns over given graphs | 32 | 58 | 55.2% ⬆️ |
| Match6 - Match named paths | 90 | 97 | 92.8% |
| Match7 - Optional match | 20 | 31 | 64.5% |
| Match8 - Match clause interop | 2 | 3 | 66.7% |
| Match9 - Match deprecated | 4 | 9 | 44.4% |
| **TOTAL Match** | **341** | **410** | **83.2%** ⬆️ |

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

## Category Pass Rates (Updated 2026-02-09 - Update 2)

| Category | Passed | Failed | Total | Rate |
|----------|--------|--------|-------|------|
| **Conditional** | **13** | **0** | **13** | **100.0%** |
| **Boolean** | **150** | **0** | **150** | **100.0%** |
| **Null** | **43** | **1** | **44** | **97.7%** |
| **MatchWhere** | **33** | **1** | **34** | **97.1%** |
| **Precedence** | **109** | **12** | **121** | **90.1%** |
| **ExistentialSubquery** | **9** | **1** | **10** | **90.0%** |
| Literals | 117 | 14 | 131 | 89.3% |
| Comparison | 61 | 11 | 72 | 84.7% |
| Match | 341 | 69 | 410 | 83.2% ⬆️ |
| Quantifier | 502 | 102 | 604 | 83.1% |
| String | 26 | 6 | 32 | 81.2% |
| WithOrderBy | 237 | 55 | 292 | 81.2% ⬆️ |
| Call | 42 | 10 | 52 | 80.8% |
| WithSkipLimit | 7 | 2 | 9 | 77.8% |
| Temporal | 743 | 261 | 1004 | 74.0% |
| WithWhere | 14 | 5 | 19 | 73.7% |
| Union | 8 | 4 | 12 | 66.7% |
| Mathematical | 4 | 2 | 6 | 66.7% |
| Unwind | 9 | 5 | 14 | 64.3% |
| Map | 25 | 19 | 44 | 56.8% |
| Create | 44 | 34 | 78 | 56.4% |
| With | 16 | 13 | 29 | 55.2% |
| List | 97 | 88 | 185 | 52.4% |
| ReturnSkipLimit | 16 | 15 | 31 | 51.6% |
| ReturnOrderBy | 18 | 17 | 35 | 51.4% |
| Return | 31 | 32 | 63 | 49.2% ⬆️ |
| CountingSubgraphMatches | 5 | 6 | 11 | 45.5% |
| TypeConversion | 21 | 26 | 47 | 44.7% |
| Graph | 23 | 38 | 61 | 37.7% |
| Pattern | 17 | 33 | 50 | 34.0% |
| Path | 2 | 5 | 7 | 28.6% |
| Delete | **5** | **36** | **41** | **12.2%** ⬇️ **REGRESSED** |
| Remove | 4 | 29 | 33 | 12.1% |
| TriadicSelection | 2 | 17 | 19 | 10.5% |
| Aggregation | 3 | 32 | 35 | 8.6% |
| Set | 2 | 51 | 53 | 3.8% |
| Merge | 0 | 75 | 75 | 0.0% |

---

## Per-Feature Detail (Updated 2026-02-09 - Update 2)

### 100% Pass Rate (Fully Passing)

| Feature | Passed | Total |
|---------|--------|-------|
| Aggregation1 | 2 | 2 |
| Boolean1 | 30 | 30 |
| Boolean2 | 30 | 30 |
| Boolean3 | 30 | 30 |
| Boolean4 | 52 | 52 |
| Boolean5 | 8 | 8 |
| Call3 | 6 | 6 |
| Call4 | 2 | 2 |
| Comparison3 | 9 | 9 |
| Comparison4 | 1 | 1 |
| Conditional1 | 1 | 1 |
| Conditional2 | 12 | 12 |
| ExistentialSubquery1 | 4 | 4 |
| ExistentialSubquery3 | 3 | 3 |
| Literals1 | 6 | 6 |
| Literals4 | 10 | 10 |
| MatchWhere1 | 15 | 15 |
| MatchWhere2 | 2 | 2 |
| MatchWhere3 | 3 | 3 |
| MatchWhere4 | 2 | 2 |
| MatchWhere5 | 4 | 4 |
| Mathematical11 | 1 | 1 |
| Mathematical13 | 1 | 1 |
| Mathematical2 | 1 | 1 |
| Null1 | 17 | 17 |
| Null2 | 17 | 17 |
| Precedence1 | 72 | 72 |
| Quantifier5 | 31 | 31 |
| Quantifier6 | 21 | 21 |
| Quantifier7 | 36 | 36 |
| Quantifier8 | 31 | 31 |
| Return1 | 2 | 2 |
| Return8 | 1 | 1 |
| ReturnOrderBy3 | 1 | 1 |
| ReturnOrderBy4 | 2 | 2 |
| String11 | 2 | 2 |
| Temporal6 | 17 | 17 |
| Temporal8 | 27 | 27 |
| With2 | 2 | 2 |
| With5 | 2 | 2 |
| WithOrderBy3 | 93 | 93 |
| WithSkipLimit2 | 4 | 4 |
| WithWhere2 | 2 | 2 |
| WithWhere3 | 3 | 3 |
| WithWhere4 | 2 | 2 |
| WithWhere5 | 4 | 4 |
| WithWhere6 | 1 | 1 |

### High Pass Rate (>75%)

| Feature | Passed | Total | Rate |
|---------|--------|-------|------|
| Match1 | 85 | 86 | 98.8% |
| Match2 | 83 | 86 | 96.5% |
| Literals5 | 26 | 27 | 96.3% |
| Literals8 | 26 | 27 | 96.3% |
| Temporal1 | 199 | 207 | 96.1% |
| Temporal7 | 17 | 18 | 94.4% |
| Comparison1 | 40 | 43 | 93.0% |
| Match6 | 90 | 97 | 92.8% |
| Literals6 | 12 | 13 | 92.3% |
| Literals2 | 11 | 12 | 91.7% |
| Precedence4 | 11 | 12 | 91.7% |
| Quantifier1 | 96 | 105 | 91.4% |
| Quantifier3 | 96 | 105 | 91.4% |
| Temporal10 | 119 | 131 | 90.8% |
| Quantifier4 | 95 | 105 | 90.5% |
| Null3 | 9 | 10 | 90.0% |
| Quantifier2 | 95 | 106 | 89.6% |
| List5 | 41 | 46 | 89.1% |
| String10 | 8 | 9 | 88.9% |
| String8 | 8 | 9 | 88.9% |
| String9 | 8 | 9 | 88.9% |
| MatchWhere6 | 7 | 8 | 87.5% |
| Graph9 | 6 | 7 | 85.7% |
| Call5 | 16 | 19 | 84.2% |
| With1 | 5 | 6 | 83.3% |
| WithOrderBy2 | 69 | 83 | 83.1% ⬆️ |
| Literals3 | 13 | 16 | 81.2% |
| Return5 | 4 | 5 | 80.0% |
| Union1 | 4 | 5 | 80.0% |
| Union2 | 4 | 5 | 80.0% |
| Temporal9 | 255 | 322 | 79.2% |
| Map2 | 11 | 14 | 78.6% |
| Create3 | 10 | 13 | 76.9% |
| Precedence2 | 20 | 26 | 76.9% |
| Match3 | 23 | 30 | 76.7% |

### Medium Pass Rate (25-75%)

| Feature | Passed | Total | Rate |
|---------|--------|-------|------|
| Call1 | 12 | 16 | 75.0% |
| Create2 | 18 | 24 | 75.0% |
| Temporal5 | 5 | 7 | 71.4% |
| WithOrderBy1 | 65 | 96 | 67.7% |
| Call2 | 4 | 6 | 66.7% |
| Call6 | 2 | 3 | 66.7% |
| Delete4 | 2 | 3 | 66.7% |
| ExistentialSubquery2 | 2 | 3 | 66.7% |
| Match8 | 2 | 3 | 66.7% |
| Path2 | 2 | 3 | 66.7% |
| Return3 | 2 | 3 | 66.7% |
| WithSkipLimit3 | 2 | 3 | 66.7% |
| Literals7 | 13 | 20 | 65.0% |
| Match7 | 20 | 31 | 64.5% |
| Unwind1 | 9 | 14 | 64.3% |
| Map1 | 12 | 19 | 63.2% |
| Graph8 | 5 | 8 | 62.5% |
| ReturnSkipLimit2 | 10 | 17 | 58.8% |
| Comparison2 | 11 | 19 | 57.9% |
| List3 | 4 | 7 | 57.1% |
| Match5 | 32 | 58 | 55.2% ⬆️ **MAJOR IMPROVEMENT** |
| Graph4 | 6 | 11 | 54.5% |
| Precedence3 | 6 | 11 | 54.5% |
| Temporal4 | 21 | 39 | 53.9% |
| List11 | 34 | 67 | 50.8% |
| Create6 | 7 | 14 | 50.0% |
| Mathematical8 | 1 | 2 | 50.0% |
| Return7 | 1 | 2 | 50.0% |
| ReturnOrderBy1 | 6 | 12 | 50.0% |
| ReturnOrderBy2 | 7 | 14 | 50.0% |
| TypeConversion4 | 7 | 14 | 50.0% |
| With7 | 1 | 2 | 50.0% |
| WithOrderBy4 | 10 | 20 | 50.0% |
| WithSkipLimit1 | 1 | 2 | 50.0% |
| CountingSubgraphMatches1 | 5 | 11 | 45.5% |
| ReturnSkipLimit1 | 5 | 11 | 45.5% |
| TypeConversion3 | 5 | 11 | 45.5% |
| Create1 | 9 | 20 | 45.0% |
| Match9 | 4 | 9 | 44.4% |
| Return2 | 8 | 18 | 44.4% |
| Return6 | 9 | 21 | 42.9% |
| With4 | 3 | 7 | 42.9% |
| TypeConversion2 | 5 | 12 | 41.7% |
| Pattern1 | 16 | 39 | 41.0% |
| List2 | 6 | 15 | 40.0% |
| Remove2 | 2 | 5 | 40.0% |
| ReturnOrderBy6 | 2 | 5 | 40.0% |
| TypeConversion1 | 4 | 10 | 40.0% |
| Temporal3 | 67 | 183 | 36.6% |
| Return4 | 4 | 11 | 36.4% |
| List6 | 6 | 17 | 35.3% |
| ReturnSkipLimit3 | 1 | 3 | 33.3% |
| With6 | 3 | 9 | 33.3% |
| WithWhere7 | 1 | 3 | 33.3% |
| Temporal2 | 16 | 53 | 30.2% |
| Remove1 | 2 | 7 | 28.6% |
| Aggregation8 | 1 | 4 | 25.0% |
| WithWhere1 | 1 | 4 | 25.0% |

### Low Pass Rate (<25%)

| Feature | Passed | Total | Rate |
|---------|--------|-------|------|
| Graph3 | 2 | 9 | 22.2% |
| List1 | 5 | 23 | 21.7% |
| Graph6 | 3 | 14 | 21.4% |
| Delete2 | 1 | 5 | 20.0% ⬇️ **REGRESSED** |
| Match4 | 2 | 10 | 20.0% |
| Map3 | 2 | 11 | 18.2% |
| List12 | 1 | 7 | 14.3% |
| Delete1 | 1 | 8 | 12.5% ⬇️ **REGRESSED** |
| Quantifier10 | 1 | 8 | 12.5% |
| Set3 | 1 | 8 | 12.5% |
| Delete5 | 1 | 9 | 11.1% |
| Graph5 | 1 | 9 | 11.1% |
| TriadicSelection1 | 2 | 19 | 10.5% |
| Pattern2 | 1 | 11 | 9.1% |
| Set1 | 1 | 11 | 9.1% |

### 0% Pass Rate (Fully Failing)

| Feature | Total |
|---------|-------|
| Aggregation2 | 12 |
| Aggregation3 | 2 |
| Aggregation5 | 2 |
| Aggregation6 | 13 |
| Create4 | 2 |
| Create5 | 5 |
| Delete3 | 2 ⬇️ **REGRESSED** |
| Delete6 | 14 |
| Graph7 | 3 |
| List4 | 2 |
| List9 | 1 |
| Mathematical3 | 1 |
| Merge1 | 17 |
| Merge2 | 6 |
| Merge3 | 5 |
| Merge4 | 2 |
| Merge5 | 29 |
| Merge6 | 6 |
| Merge7 | 5 |
| Merge8 | 1 |
| Merge9 | 4 |
| Path1 | 1 |
| Path3 | 3 |
| Quantifier9 | 17 |
| Quantifier11 | 22 |
| Quantifier12 | 17 |
| Remove3 | 21 |
| ReturnOrderBy5 | 1 |
| Set2 | 3 |
| Set4 | 5 |
| Set5 | 5 |
| Set6 | 21 |
| String1 | 1 |
| String3 | 1 |
| String4 | 1 |
| Union3 | 2 |
| With3 | 1 |

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
| MATCH | Strong | 83.2% ⬆️ | Node/edge patterns strong, Match5 improved to 55.2% |
| MATCH WHERE | Excellent | 97.1% | Simple filters, joins, null predicates all work |
| RETURN | Moderate | 49.2% ⬆️ | Core works, aggregates improving |
| CREATE | Moderate | 56.4% | Relationships 75%, interop 77%, persistence 50% |
| DELETE | **Limited** | **12.2%** ⬇️ | **MAJOR REGRESSION - only null handling works** |
| SET | Limited | 3.8% | Basic property setting works |
| REMOVE | Limited | 12.1% | Basic functionality present |
| MERGE | None | 0.0% | Not implemented |
| WITH | Moderate | 55.2% | Piping works, DISTINCT now 100% |
| WITH ORDER BY | Strong | 81.2% ⬆️ | Comprehensive ordering support |
| WITH WHERE | Good | 73.7% | Filter after WITH working |
| WITH SKIP/LIMIT | Good | 77.8% | Most scenarios work |
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
| 2026-02-08 (night) | 2,731 | 70.0% | **70% MILESTONE!** Match7 +3, Match9 +3, Match4 +2, Pattern1 +2. Regressions: Match6 -4, Graph7 -1, Return7 -1. Net +11 scenarios. |
| 2026-02-09 (Update 1) | 2,771 | 71.1% | Create2 +12 (25%→75%), Create3 +10 (0%→77%), Create6 +7 (0%→50%), Match6 +6 (recovered!), Delete1/Delete2 +2 each. Regressions: WithOrderBy2 -1, WithSkipLimit1 -1. Net +40 scenarios. |
| 2026-02-09 (Update 2) | **2,799** | **72.05%** | **Match5 +29 (10.3%→55.2%, total scenarios 29→58), WithOrderBy2 +2. REGRESSIONS: Delete1 -3 (50%→12.5%), Delete2 -1 (40%→20%), Delete3 -1 (50%→0%). Net +28 scenarios.** |

### Cumulative Improvement

| From | To | Scenarios Gained | Improvement |
|------|-----|------------------|-------------|
| Baseline (1,279) | Current (2,799) | **+1,520** | **+118.8%** |

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
| 75% | 2,914 | 75.0% | +115 scenarios |
| 80% | 3,108 | 80.0% | +309 scenarios |

**Note:** Total scenario count updated to 3,885 (non-skipped) from 3,926 total.

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
