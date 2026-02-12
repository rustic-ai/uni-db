# Uni OpenCypher TCK Compatibility Report

**Generated:** 2026-02-11 (Native DataFusion Executor — nextest run)
**TCK Version:** M23 (openCypher)
**Uni Version:** Current debug/tck001 branch

---

## Executive Summary

| Metric | Count | Pass Rate |
|--------|-------|-----------|
| **Features** | 192 | - |
| **Scenarios** | 3,897 | **48.4%** (1,887 passed, 2,010 failed) |
| **Skipped** | 0 | - |

**Context (2026-02-11):**
This is a **baseline measurement** after migrating from the legacy fallback executor to the native DataFusion execution path. The previous report (72.05% on 2026-02-09) reflected the legacy executor. The drop to 48.4% is expected — many query patterns that relied on the legacy row-by-row executor now need native DataFusion physical expression implementations.

Key changes in this run:
1. **Quantifier expressions (ALL/ANY/SINGLE/NONE)** — new `QuantifierExecExpr` physical expression added
2. **JSONB comparison UDFs** — registered `_cypher_equal`, `_cypher_gt`, etc. for LargeBinary comparisons
3. **Legacy executor disabled** — all queries now run through DataFusion physical plans
4. **Pattern comprehension** — new `Apply` physical operator and vectorized `PatternComprehensionExpr`

### Section-Level Summary

| Section | Passed | Total | Pass Rate |
|---------|--------|-------|-----------|
| **Clauses** | 635 | 1,251 | **50.8%** |
| **Expressions** | 1,240 | 2,616 | **47.4%** |
| **Use Cases** | 12 | 30 | **40.0%** |

---

## Clause Results

| Clause | Passed | Total | Pass Rate | Notes |
|--------|--------|-------|-----------|-------|
| **Call** | 42 | 52 | **80.8%** | Procedure infrastructure working |
| **Create** | 37 | 78 | **47.4%** | Node/relationship creation partial |
| **Match** | 282 | 381 | **74.0%** | Node/edge patterns strong |
| **MatchWhere** | 24 | 34 | **70.6%** | Filtering well-supported |
| **Delete** | 13 | 41 | **31.7%** | Basic delete operations |
| **Remove** | 13 | 33 | **39.4%** | Label removal 100% |
| **Return** | 26 | 63 | **41.3%** | Core projection works |
| **ReturnOrderBy** | 12 | 35 | **34.3%** | Partial ordering support |
| **ReturnSkipLimit** | 18 | 31 | **58.1%** | Pagination mostly works |
| **Set** | 2 | 53 | **3.8%** | Minimal property setting |
| **Union** | 6 | 12 | **50.0%** | Basic union works |
| **Unwind** | 7 | 14 | **50.0%** | List unwinding |
| **With** | 9 | 29 | **31.0%** | Basic piping works |
| **WithOrderBy** | 130 | 292 | **44.5%** | Ordering support |
| **WithSkipLimit** | 2 | 9 | **22.2%** | Limited |
| **WithWhere** | 12 | 19 | **63.2%** | Filter after WITH working |
| **Merge** | 0 | 75 | **0.0%** | Not implemented |

---

## Expression Results

| Expression | Passed | Total | Pass Rate | Notes |
|------------|--------|-------|-----------|-------|
| **Aggregation** | 3 | 35 | **8.6%** | COUNT works, others need work |
| **Boolean** | 121 | 150 | **80.7%** | NOT 100%, AND/OR/XOR ~77% |
| **Comparison** | 44 | 72 | **61.1%** | Full-bound range 100% |
| **Conditional** | 10 | 13 | **76.9%** | Coalesce 100% |
| **ExistentialSubquery** | 2 | 10 | **20.0%** | Partially implemented |
| **Graph** | 17 | 61 | **27.9%** | Property access partial |
| **List** | 61 | 185 | **33.0%** | IN operator, ranges partial |
| **Literals** | 111 | 131 | **84.7%** | Strong literal support |
| **Map** | 16 | 44 | **36.4%** | Static/dynamic access partial |
| **Mathematical** | 3 | 6 | **50.0%** | Basic math |
| **Null** | 18 | 44 | **40.9%** | Three-valued logic partial |
| **Path** | 2 | 7 | **28.6%** | relationships() working |
| **Pattern** | 20 | 50 | **40.0%** | Pattern comprehension working |
| **Precedence** | 38 | 121 | **31.4%** | Numeric precedence partial |
| **Quantifier** | 384 | 604 | **63.6%** | ALL/ANY/SINGLE/NONE working |
| **String** | 26 | 32 | **81.2%** | STARTS WITH, ENDS WITH, CONTAINS |
| **Temporal** | 346 | 1,004 | **34.5%** | Creation, truncation partial |
| **TypeConversion** | 18 | 47 | **38.3%** | toString partial |

### Use Case Results

| Use Case | Passed | Total | Pass Rate |
|----------|--------|-------|-----------|
| **CountingSubgraphMatches** | 11 | 11 | **100.0%** |
| **TriadicSelection** | 1 | 19 | **5.3%** |

---

## Feature-Level Detail

### 100% Pass Rate (Fully Passing)

| Feature | Passed | Total |
|---------|--------|-------|
| Aggregation1 — Count | 2 | 2 |
| Boolean4 — NOT logical operations | 52 | 52 |
| Call3 — Assignable-type arguments | 6 | 6 |
| Call4 — Null Arguments | 2 | 2 |
| Comparison3 — Full-Bound Range | 9 | 9 |
| Comparison4 — Combination of Comparisons | 1 | 1 |
| Conditional1 — Coalesce expression | 1 | 1 |
| CountingSubgraphMatches1 | 11 | 11 |
| Delete4 — Delete clause interop | 3 | 3 |
| Literals1 — Boolean and Null literals | 6 | 6 |
| Literals4 — List literals | 10 | 10 |
| MatchWhere2 — Filter node equality | 2 | 2 |
| MatchWhere3 — Filter on null properties | 3 | 3 |
| MatchWhere5 — Filter with pattern predicates | 4 | 4 |
| Mathematical11 | 1 | 1 |
| Mathematical13 | 1 | 1 |
| Null3 | 10 | 10 |
| Remove2 — Remove a Label | 5 | 5 |
| Return8 | 1 | 1 |
| String11 | 2 | 2 |
| Temporal6 | 17 | 17 |
| With5 | 2 | 2 |
| WithWhere2 | 2 | 2 |
| WithWhere3 | 3 | 3 |
| WithWhere5 | 4 | 4 |

### High Pass Rate (>=75%)

| Feature | Passed | Total | Rate |
|---------|--------|-------|------|
| Match6 — Match named paths | 95 | 97 | 97.9% |
| Match1 — Match nodes | 84 | 86 | 97.7% |
| Literals5 — String literals | 26 | 27 | 96.3% |
| Temporal7 | 17 | 18 | 94.4% |
| Match2 — Match relationships | 81 | 86 | 94.2% |
| Literals6 — Map literals | 12 | 13 | 92.3% |
| Literals2 — Integer literals | 11 | 12 | 91.7% |
| String8/9/10 | 8 | 9 | 88.9% each |
| Literals8 | 21 | 27 | 77.8% |
| Call5 — Results projection | 16 | 19 | 84.2% |
| Literals3 — Float literals | 13 | 16 | 81.2% |
| Pattern2 — Pattern comprehension | 9 | 11 | 81.8% |
| Create3 — Interop with other clauses | 10 | 13 | 76.9% |
| Precedence2 | 20 | 26 | 76.9% |
| Boolean1/2/3 — AND/OR/XOR | 23 | 30 | 76.7% each |
| Quantifier2 — Single quantifier | 80 | 106 | 75.5% |
| Quantifier1 — None quantifier | 79 | 105 | 75.2% |
| Quantifier3 — Any quantifier | 79 | 105 | 75.2% |
| Quantifier4 — All quantifier | 79 | 105 | 75.2% |
| Call1 — Basic procedure calling | 12 | 16 | 75.0% |
| Conditional2 — Case expression | 9 | 12 | 75.0% |
| Delete1 — Deleting nodes | 6 | 8 | 75.0% |

### Medium Pass Rate (25%-74%)

| Feature | Passed | Total | Rate |
|---------|--------|-------|------|
| Call2 — Procedure arguments | 4 | 6 | 66.7% |
| Call6 — Call clause interop | 2 | 3 | 66.7% |
| Path2 | 2 | 3 | 66.7% |
| Return3 | 2 | 3 | 66.7% |
| ReturnSkipLimit3 | 2 | 3 | 66.7% |
| With1 | 4 | 6 | 66.7% |
| Create1 — Creating nodes | 13 | 20 | 65.0% |
| ReturnSkipLimit2 | 11 | 17 | 64.7% |
| Map2 | 9 | 14 | 64.3% |
| Quantifier7 — Any quantifier interop | 23 | 36 | 63.9% |
| MatchWhere6 | 5 | 8 | 62.5% |
| WithOrderBy3 | 57 | 93 | 61.3% |
| Delete2 — Deleting relationships | 3 | 5 | 60.0% |
| List2 | 9 | 15 | 60.0% |
| Literals7 | 12 | 20 | 60.0% |
| MatchWhere1 | 9 | 15 | 60.0% |
| Return5 | 3 | 5 | 60.0% |
| Union1/2 | 3 | 5 | 60.0% each |
| List11 | 40 | 67 | 59.7% |
| Create2 — Creating relationships | 14 | 24 | 58.3% |
| Quantifier5 — None quantifier interop | 18 | 31 | 58.1% |
| Quantifier8 — All quantifier interop | 18 | 31 | 58.1% |
| Graph9 | 4 | 7 | 57.1% |
| Temporal1 | 117 | 207 | 56.5% |
| Comparison1 — Equality | 24 | 43 | 55.8% |
| Temporal4 | 21 | 39 | 53.8% |
| Comparison2 — Half-bounded range | 10 | 19 | 52.6% |
| ExistentialSubquery1 | 2 | 4 | 50.0% |
| Graph8 | 4 | 8 | 50.0% |
| Mathematical8 | 1 | 2 | 50.0% |
| MatchWhere4 | 1 | 2 | 50.0% |
| Return1 | 1 | 2 | 50.0% |
| Return7 | 1 | 2 | 50.0% |
| ReturnOrderBy1 | 6 | 12 | 50.0% |
| ReturnOrderBy4 | 1 | 2 | 50.0% |
| TypeConversion2 | 6 | 12 | 50.0% |
| Unwind1 | 7 | 14 | 50.0% |
| With7 | 1 | 2 | 50.0% |
| WithOrderBy1 | 48 | 96 | 50.0% |
| Return4 | 5 | 11 | 45.5% |
| ReturnSkipLimit1 | 5 | 11 | 45.5% |
| Precedence4 | 5 | 12 | 41.7% |
| TypeConversion1 | 4 | 10 | 40.0% |
| Quantifier6 — Single quantifier interop | 8 | 21 | 38.1% |
| Map1 | 7 | 19 | 36.8% |
| TypeConversion3 | 4 | 11 | 36.4% |
| Graph6 — Static property access | 5 | 14 | 35.7% |
| ReturnOrderBy2 | 5 | 14 | 35.7% |
| Temporal3 | 64 | 183 | 35.0% |
| Graph7 — Dynamic property access | 1 | 3 | 33.3% |
| Match8 — Match clause interop | 1 | 3 | 33.3% |
| Match9 — Match deprecated | 3 | 9 | 33.3% |
| Return2 | 6 | 18 | 33.3% |
| Return6 | 7 | 21 | 33.3% |
| WithSkipLimit3 | 1 | 3 | 33.3% |
| WithWhere7 | 1 | 3 | 33.3% |
| Temporal2 | 16 | 53 | 30.2% |
| WithOrderBy2 | 25 | 83 | 30.1% |
| Pattern1 | 11 | 39 | 28.2% |
| Remove1 | 2 | 7 | 28.6% |
| Remove3 | 6 | 21 | 28.6% |
| TypeConversion4 | 4 | 14 | 28.6% |
| Aggregation8 — DISTINCT | 1 | 4 | 25.0% |
| WithSkipLimit2 | 1 | 4 | 25.0% |
| WithWhere1 | 1 | 4 | 25.0% |

### Low Pass Rate (<25%)

| Feature | Passed | Total | Rate |
|---------|--------|-------|------|
| Temporal10 | 32 | 131 | 24.4% |
| Null1 | 4 | 17 | 23.5% |
| Null2 | 4 | 17 | 23.5% |
| Graph3 — Node labels | 2 | 9 | 22.2% |
| Match5 — Variable length patterns over graphs | 6 | 29 | 20.7% |
| Match3 — Match fixed length patterns | 6 | 30 | 20.0% |
| Temporal9 | 62 | 322 | 19.3% |
| Precedence1 | 13 | 72 | 18.1% |
| List1 — Equality and inequality | 4 | 23 | 17.4% |
| Match7 — Optional match | 5 | 31 | 16.1% |
| With4 | 1 | 7 | 14.3% |
| List3 | 1 | 7 | 14.3% |
| Set3 | 1 | 8 | 12.5% |
| List6 | 2 | 17 | 11.8% |
| Delete5 | 1 | 9 | 11.1% |
| With6 | 1 | 9 | 11.1% |
| List5 | 5 | 46 | 10.9% |
| Match4 — Variable length patterns | 1 | 10 | 10.0% |
| Graph4 — Edge relationship type | 1 | 11 | 9.1% |
| Set1 | 1 | 11 | 9.1% |
| TriadicSelection1 | 1 | 19 | 5.3% |

### 0% Pass Rate (Fully Failing)

| Feature | Total | Notes |
|---------|-------|-------|
| Aggregation2 — Min and Max | 12 | Not implemented in DF path |
| Aggregation3 — Sum | 2 | Not implemented in DF path |
| Aggregation5 — Collect | 2 | Not implemented in DF path |
| Aggregation6 — Percentiles | 13 | Not implemented in DF path |
| Boolean5 — Interop of logical operations | 8 | |
| Create4 — Large Create Query | 2 | |
| Create5 — Multiple hops create patterns | 5 | |
| Create6 — Persistence of create side effects | 14 | Side effect checks |
| Delete3 — Deleting named paths | 2 | |
| Delete6 — Persistence of delete side effects | 14 | Side effect checks |
| ExistentialSubquery2 | 3 | |
| ExistentialSubquery3 | 3 | |
| Graph5 — Node/edge label expressions | 9 | |
| List4 | 2 | |
| List9 | 1 | |
| List12 | 7 | |
| Map3 | 11 | |
| Mathematical2 | 1 | |
| Mathematical3 | 1 | |
| Merge1-9 | 75 | Not implemented |
| Null (none at 0%) | - | - |
| Path1 | 1 | |
| Path3 | 3 | |
| Precedence3 | 11 | |
| Quantifier9 | 17 | Invariants — edge cases |
| Quantifier10 | 8 | Invariants — edge cases |
| Quantifier11 | 22 | Invariants — edge cases |
| Quantifier12 | 17 | Invariants — edge cases |
| ReturnOrderBy3 | 1 | |
| ReturnOrderBy5 | 1 | |
| ReturnOrderBy6 | 5 | |
| Set2 | 3 | |
| Set4 | 5 | |
| Set5 | 5 | |
| Set6 | 21 | |
| String1 | 1 | |
| String3 | 1 | |
| String4 | 1 | |
| Temporal5 | 7 | |
| Temporal8 | 27 | |
| Union3 | 2 | |
| With2 | 2 | |
| With3 | 1 | |
| WithOrderBy4 | 20 | |
| WithSkipLimit1 | 2 | |
| WithWhere6 | 1 | |

---

## Scenario-Level View

### Scenarios by Status

| Status | Count | Percentage |
|--------|-------|------------|
| **Passed** | 1,887 | 48.4% |
| **Failed** | 2,010 | 51.6% |
| **Skipped** | 0 | 0.0% |
| **Total** | 3,897 | 100.0% |

---

## Comparison: Legacy Executor vs Native DataFusion

| Metric | Legacy (2026-02-09) | DataFusion (2026-02-11) | Delta |
|--------|---------------------|-------------------------|-------|
| **Overall Pass Rate** | 72.05% | 48.4% | -23.6pp |
| **Passed Scenarios** | 2,799 | 1,887 | -912 |
| **Total Scenarios** | 3,885 | 3,897 | +12 |

### Categories That Improved or Held Steady

| Category | Legacy | DataFusion | Notes |
|----------|--------|------------|-------|
| Call | 80.8% | 80.8% | Unchanged |
| String | 81.2% | 81.2% | Unchanged |
| CountingSubgraphMatches | 45.5% | 100.0% | Major improvement |
| Pattern | 2.0% | 40.0% | Pattern comprehension added |

### Categories That Regressed Most

| Category | Legacy | DataFusion | Gap | Root Cause |
|----------|--------|------------|-----|------------|
| Null | 97.7% | 40.9% | -57pp | Three-valued logic path changes |
| Precedence | 90.1% | 31.4% | -59pp | Expression compilation gaps |
| Boolean | 100% | 80.7% | -19pp | Interop tests need DF path |
| MatchWhere | 97.1% | 70.6% | -27pp | Filter compilation path |
| Temporal | 74.0% | 34.5% | -40pp | Duration/arithmetic not in DF path |
| WithOrderBy | 81.2% | 44.5% | -37pp | ORDER BY expression compilation |
| Match | 83.2% | 74.0% | -9pp | Variable length patterns |

---

## Progress Tracking

| Date | Scenarios Passed | Pass Rate | Key Changes |
|------|-----------------|-----------|-------------|
| 2026-02-03 (baseline) | 1,279 | 33.1% | Initial measurement |
| 2026-02-04 | 1,764 | 45.6% | Path binding, WITH ORDER BY, aggregation |
| 2026-02-05 | 2,126 | 55.0% | Temporal formatting, procedure CALL |
| 2026-02-06 | 2,502 | 64.7% | EXISTS, WithOrderBy, MatchWhere, List, Pattern |
| 2026-02-07 | 2,620 | 67.2% | Boolean, String, Precedence, Quantifier |
| 2026-02-08 | 2,731 | 70.0% | **70% MILESTONE** (Boolean 100%, Comparison 100%, Null 100%) |
| 2026-02-09 | 2,799 | 72.05% | Create, Match5, WithOrderBy improvements |
| **2026-02-11** | **1,887** | **48.4%** | **Native DataFusion executor baseline** (legacy disabled) |

### Cumulative Improvement (from DataFusion baseline)

| From | To | Scenarios Gained | Improvement |
|------|-----|------------------|-------------|
| DF Baseline (1,887) | Current (1,887) | +0 | Baseline |

---

## Key Gaps to Address (DataFusion Path)

### High Priority (blocking many tests)

1. **Temporal Duration/Arithmetic** (~400 scenario gap)
   - Duration properties, arithmetic, formatting need DF physical expressions
   - Temporal9/10 severely impacted

2. **Precedence/Boolean Interop** (~90 scenario gap)
   - Precedence1 dropped from 100% to 18%
   - Boolean5 interop at 0%
   - Expression compilation needs broader operator support

3. **Null Three-Valued Logic** (~26 scenario gap)
   - Null1/Null2 dropped from 100% to 23.5%
   - IS NULL / IS NOT NULL in complex expressions

4. **Optional Match** (~26 scenario gap)
   - Match7 dropped from 64.5% to 16.1%
   - Optional match filter path needs DF compilation

5. **WithOrderBy** (~107 scenario gap)
   - WithOrderBy2 dropped from 83.1% to 30.1%
   - ORDER BY expression compilation in DF path

### Medium Priority

6. **Aggregation Functions** (32 remaining failures)
   - SUM, AVG, MIN, MAX, COLLECT not in DF path
   - COUNT works (Aggregation1 100%)

7. **ExistentialSubquery** (8 failures)
   - EXISTS patterns partially implemented

8. **List Operations** (124 failures)
   - List comprehensions, range, slicing need DF support

### Low Priority

9. **MERGE Implementation** (75 failures) — not implemented
10. **SET Clause** (51 failures) — minimal support
11. **Quantifier Invariants** (64 failures) — edge cases

---

## Next Milestone Targets

| Target | Scenarios | Pass Rate | Gap from Current |
|--------|-----------|-----------|------------------|
| **55%** | 2,143 | 55.0% | +256 scenarios |
| **60%** | 2,338 | 60.0% | +451 scenarios |
| **65%** | 2,533 | 65.0% | +646 scenarios |
| **70%** | 2,728 | 70.0% | +841 scenarios |
| **75%** | 2,923 | 75.0% | +1,036 scenarios |

Achieving 55% likely requires:
- Fix temporal duration/arithmetic expressions -> could unlock ~100+ scenarios
- Fix precedence/boolean expression compilation -> could unlock ~80+ scenarios
- Fix null handling in DF path -> could unlock ~26 scenarios
- Fix Optional Match compilation -> could unlock ~26 scenarios

---

## Test Command Reference

```bash
# Run all TCK tests with report
scripts/run_tck_with_report.sh

# Run filtered TCK tests
scripts/run_tck_with_report.sh "~Match1"

# List all TCK scenarios as individual tests
cargo nextest list -p uni-tck --test tck

# Run specific scenarios via nextest
cargo nextest run -p uni-tck --test tck -E 'test(~clauses::match::Match1)'

# Reports available at:
#   target/cucumber/report.md          (auto-generated comparative report)
#   target/cucumber/results_*.json     (timestamped raw results)
```
