# Uni OpenCypher TCK Compatibility Report

**Generated:** 2026-02-11 (Native DataFusion Executor Baseline)
**TCK Version:** M23 (openCypher)
**Uni Version:** Current debug/tck001 branch

---

## Executive Summary

| Metric | Count | Pass Rate |
|--------|-------|-----------|
| **Features** | 192 | - |
| **Scenarios** | 3,897 | **48.8%** (1,902 passed, 1,995 failed) |
| **Skipped** | 0 | - |

**Context (2026-02-11):**
This is a **baseline measurement** after migrating from the legacy fallback executor to the native DataFusion execution path. The previous report (72.05% on 2026-02-09) reflected the legacy executor. The drop to 48.8% is expected — many query patterns that relied on the legacy row-by-row executor now need native DataFusion physical expression implementations.

Key changes in this run:
1. **Quantifier expressions (ALL/ANY/SINGLE/NONE)** — new `QuantifierExecExpr` physical expression added
2. **JSONB comparison UDFs** — registered `_cypher_equal`, `_cypher_gt`, etc. for LargeBinary comparisons
3. **Legacy executor disabled** — all queries now run through DataFusion physical plans

### Section-Level Summary

| Section | Passed | Total | Pass Rate |
|---------|--------|-------|-----------|
| **Clauses** | 652 | 1,251 | **52.1%** |
| **Expressions** | 1,238 | 2,616 | **47.3%** |
| **Use Cases** | 12 | 30 | **40.0%** |

---

## Clause Results

| Clause | Passed | Total | Pass Rate | Notes |
|--------|--------|-------|-----------|-------|
| **Call** | 42 | 52 | **80.8%** | Procedure infrastructure working |
| **Create** | 46 | 78 | **59.0%** | Relationships, interop working |
| **Match** | 283 | 381 | **74.3%** | Node/edge patterns strong |
| **MatchWhere** | 24 | 34 | **70.6%** | Filtering well-supported |
| **Delete** | 13 | 41 | **31.7%** | Basic delete operations |
| **Remove** | 13 | 33 | **39.4%** | Label removal 100% |
| **Return** | 27 | 63 | **42.9%** | Core projection works |
| **ReturnOrderBy** | 13 | 35 | **37.1%** | Partial ordering support |
| **ReturnSkipLimit** | 18 | 31 | **58.1%** | Pagination mostly works |
| **Set** | 4 | 53 | **7.5%** | Limited property setting |
| **Union** | 6 | 12 | **50.0%** | Basic union works |
| **Unwind** | 7 | 14 | **50.0%** | List unwinding |
| **With** | 9 | 29 | **31.0%** | Basic piping works |
| **WithOrderBy** | 130 | 292 | **44.5%** | Ordering support |
| **WithSkipLimit** | 2 | 9 | **22.2%** | Limited |
| **WithWhere** | 12 | 19 | **63.2%** | Filter after WITH working |
| **Merge** | 3 | 75 | **4.0%** | Mostly not implemented |

---

## Expression Results

| Expression | Passed | Total | Pass Rate | Notes |
|------------|--------|-------|-----------|-------|
| **Boolean** | 121 | 150 | **80.7%** | NOT 100%, AND/OR/XOR ~77% |
| **Comparison** | 44 | 72 | **61.1%** | Full-bound range 100% |
| **Conditional** | 10 | 13 | **76.9%** | Coalesce 100% |
| **ExistentialSubquery** | 0 | 10 | **0.0%** | Not implemented in DF path |
| **Graph** | 19 | 61 | **31.1%** | Property access partial |
| **Aggregation** | 4 | 35 | **11.4%** | COUNT works, others need work |
| **List** | 70 | 185 | **37.8%** | IN operator, ranges partial |
| **Literals** | 110 | 131 | **84.0%** | Strong literal support |
| **Map** | 25 | 44 | **56.8%** | Static/dynamic access |
| **Mathematical** | 3 | 6 | **50.0%** | Basic math |
| **Null** | 17 | 44 | **38.6%** | Three-valued logic partial |
| **Path** | 2 | 7 | **28.6%** | relationships() working |
| **Pattern** | 1 | 50 | **2.0%** | Pattern predicates weak |
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
| Create5 — Multiple hops create patterns | 5 | 5 |
| Delete4 — Delete clause interop | 3 | 3 |
| Literals1 — Boolean and Null literals | 6 | 6 |
| Literals4 — List literals | 10 | 10 |
| MatchWhere2 — Filter node equality | 2 | 2 |
| MatchWhere3 — Filter on null properties | 3 | 3 |
| MatchWhere5 — Filter with pattern predicates | 4 | 4 |
| Mathematical11 | 1 | 1 |
| Mathematical13 | 1 | 1 |
| Remove2 — Remove a Label | 5 | 5 |
| Return8 | 1 | 1 |
| String11 | 2 | 2 |
| Temporal6 | 17 | 17 |
| With5 | 2 | 2 |
| WithWhere2 | 2 | 2 |
| WithWhere3 | 3 | 3 |
| WithWhere5 | 4 | 4 |

### High Pass Rate (≥75%)

| Feature | Passed | Total | Rate |
|---------|--------|-------|------|
| Match6 — Match named paths | 95 | 97 | 97.9% |
| Match1 — Match nodes | 84 | 86 | 97.7% |
| Literals5 — String literals | 26 | 27 | 96.3% |
| List1 — Equality and inequality | 22 | 23 | 95.7% |
| Temporal7 | 17 | 18 | 94.4% |
| Match2 — Match relationships | 81 | 86 | 94.2% |
| Literals6 — Map literals | 12 | 13 | 92.3% |
| Literals2 — Integer literals | 11 | 12 | 91.7% |
| Null3 | 9 | 10 | 90.0% |
| String8/9/10 | 8 | 9 | 88.9% each |
| Call5 — Results projection | 16 | 19 | 84.2% |
| Map3 | 9 | 11 | 81.8% |
| Literals3 — Float literals | 13 | 16 | 81.2% |
| Literals8 | 21 | 27 | 77.8% |
| Create3 — Interop with other clauses | 10 | 13 | 76.9% |
| Precedence2 | 20 | 26 | 76.9% |
| Boolean1/2/3 — AND/OR/XOR | 23 | 30 | 76.7% each |
| Quantifier2 — Single quantifier | 80 | 106 | 75.5% |
| Quantifier1 — None quantifier | 79 | 105 | 75.2% |
| Quantifier3 — Any quantifier | 79 | 105 | 75.2% |
| Quantifier4 — All quantifier | 79 | 105 | 75.2% |
| Call1 — Basic procedure calling | 12 | 16 | 75.0% |
| Conditional2 — Case expression | 9 | 12 | 75.0% |
| Create2 — Creating relationships | 18 | 24 | 75.0% |
| Delete1 — Deleting nodes | 6 | 8 | 75.0% |

### Medium Pass Rate (25%–74%)

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
| MatchWhere1 | 9 | 15 | 60.0% |
| Return5 | 3 | 5 | 60.0% |
| Union1/2 | 3 | 5 | 60.0% each |
| Quantifier5 — None quantifier interop | 18 | 31 | 58.1% |
| Quantifier8 — All quantifier interop | 18 | 31 | 58.1% |
| Graph9 | 4 | 7 | 57.1% |
| Temporal1 | 117 | 207 | 56.5% |
| Comparison1 — Equality | 24 | 43 | 55.8% |
| Literals7 | 11 | 20 | 55.0% |
| Temporal4 | 21 | 39 | 53.8% |
| Comparison2 — Half-bounded range | 10 | 19 | 52.6% |
| List11 | 34 | 67 | 50.7% |
| WithOrderBy1 | 48 | 96 | 50.0% |
| Unwind1 | 7 | 14 | 50.0% |
| ReturnOrderBy1 | 6 | 12 | 50.0% |
| TypeConversion2 | 6 | 12 | 50.0% |
| Return4 | 5 | 11 | 45.5% |
| ReturnSkipLimit1 | 5 | 11 | 45.5% |
| Graph3 — Node labels | 4 | 9 | 44.4% |
| ReturnOrderBy2 | 6 | 14 | 42.9% |
| Precedence4 | 5 | 12 | 41.7% |
| List2 | 6 | 15 | 40.0% |
| TypeConversion1 | 4 | 10 | 40.0% |
| Quantifier6 — Single quantifier interop | 8 | 21 | 38.1% |
| Return6 | 8 | 21 | 38.1% |
| Set3 | 3 | 8 | 37.5% |
| Map1 | 7 | 19 | 36.8% |
| TypeConversion3 | 4 | 11 | 36.4% |
| Graph6 — Static property access | 5 | 14 | 35.7% |
| Temporal3 | 64 | 183 | 35.0% |
| Graph7 — Dynamic property access | 1 | 3 | 33.3% |
| Match8 — Match clause interop | 1 | 3 | 33.3% |
| Match9 — Match deprecated | 3 | 9 | 33.3% |
| Return2 | 6 | 18 | 33.3% |
| WithOrderBy2 | 25 | 83 | 30.1% |
| Temporal2 | 16 | 53 | 30.2% |
| Remove1 | 2 | 7 | 28.6% |
| Remove3 | 6 | 21 | 28.6% |
| TypeConversion4 | 4 | 14 | 28.6% |
| Aggregation8 — DISTINCT | 1 | 4 | 25.0% |

### Low Pass Rate (<25%)

| Feature | Passed | Total | Rate |
|---------|--------|-------|------|
| Temporal10 | 32 | 131 | 24.4% |
| Null1 | 4 | 17 | 23.5% |
| Null2 | 4 | 17 | 23.5% |
| Match5 — Variable length patterns over graphs | 6 | 29 | 20.7% |
| Match3 — Match fixed length patterns | 6 | 30 | 20.0% |
| Match4 — Variable length patterns | 2 | 10 | 20.0% |
| Merge3 | 1 | 5 | 20.0% |
| Temporal9 | 62 | 322 | 19.3% |
| Precedence1 | 13 | 72 | 18.1% |
| Merge2 | 1 | 6 | 16.7% |
| Match7 — Optional match | 5 | 31 | 16.1% |
| List3 | 1 | 7 | 14.3% |
| With4 | 1 | 7 | 14.3% |
| List6 | 2 | 17 | 11.8% |
| Delete5 | 1 | 9 | 11.1% |
| List5 | 5 | 46 | 10.9% |
| Graph4 — Edge relationship type | 1 | 11 | 9.1% |
| Set1 | 1 | 11 | 9.1% |
| Merge1 | 1 | 17 | 5.9% |
| TriadicSelection1 | 1 | 19 | 5.3% |
| Pattern1 | 1 | 39 | 2.6% |

### 0% Pass Rate (Fully Failing)

| Feature | Total | Notes |
|---------|-------|-------|
| Aggregation2 — Min and Max | 12 | Not implemented in DF path |
| Aggregation3 — Sum | 2 | Not implemented in DF path |
| Aggregation6 — Percentiles | 13 | Not implemented in DF path |
| Boolean5 — Interop of logical operations | 8 | |
| Create4 — Large Create Query | 2 | |
| Create6 — Persistence of create side effects | 14 | Side effect checks |
| Delete3 — Deleting named paths | 2 | |
| Delete6 — Persistence of delete side effects | 14 | Side effect checks |
| ExistentialSubquery1/2/3 | 10 | Not implemented in DF path |
| Graph5 — Node/edge label expressions | 9 | |
| List4/9/12 | 10 | |
| Mathematical2/3 | 2 | |
| Merge4/5/6/7/8/9 | 47 | Not implemented |
| Path1/3 | 4 | |
| Pattern2 | 11 | |
| Precedence3 | 11 | |
| Quantifier9/10/11/12 — Invariants | 64 | Edge cases |
| ReturnOrderBy3/5/6 | 7 | |
| Set2/4/5/6 | 34 | Not fully implemented |
| String1/3/4 | 3 | |
| Temporal5/8 | 34 | |
| Union3 | 2 | |
| With2/3 | 3 | |
| WithOrderBy4 | 20 | |
| WithSkipLimit1 | 2 | |
| WithWhere6 | 1 | |

---

## Scenario-Level View

### Scenarios by Status

| Status | Count | Percentage |
|--------|-------|------------|
| **Passed** | 1,902 | 48.8% |
| **Failed** | 1,995 | 51.2% |
| **Skipped** | 0 | 0.0% |
| **Total** | 3,897 | 100.0% |

### Scenario Distribution by Pass Rate Bucket

| Feature Pass Rate | Features | Scenarios (Passed/Total) |
|-------------------|----------|--------------------------|
| 100% | 25 features | 163/163 |
| 75%–99% | 18 features | 686/737 |
| 50%–74% | 33 features | 621/1,013 |
| 25%–49% | 23 features | 283/693 |
| 1%–24% | 22 features | 146/773 |
| 0% | 41 features | 0/518 |

---

## Comparison: Legacy Executor vs Native DataFusion

| Metric | Legacy (2026-02-09) | DataFusion (2026-02-11) | Delta |
|--------|---------------------|-------------------------|-------|
| **Overall Pass Rate** | 72.05% | 48.8% | -23.3pp |
| **Passed Scenarios** | 2,799 | 1,902 | -897 |
| **Total Scenarios** | 3,885 | 3,897 | +12 |

### Categories That Improved or Held Steady

| Category | Legacy | DataFusion | Notes |
|----------|--------|------------|-------|
| Call | 80.8% | 80.8% | Unchanged |
| String | 81.2% | 81.2% | Unchanged |
| CountingSubgraphMatches | 45.5% | 100.0% | Major improvement |
| Map | 56.8% | 56.8% | Unchanged |
| Create | 56.4% | 59.0% | Slight improvement |
| Quantifier | 83.1% | 63.6% | Dropped (was on legacy path) |

### Categories That Regressed Most

| Category | Legacy | DataFusion | Gap | Root Cause |
|----------|--------|------------|-----|------------|
| Boolean | 100% | 80.7% | -19pp | Interop tests need DF path |
| Null | 97.7% | 38.6% | -59pp | Three-valued logic path changes |
| MatchWhere | 97.1% | 70.6% | -27pp | Filter compilation path |
| Precedence | 90.1% | 31.4% | -59pp | Expression compilation gaps |
| Temporal | 74.0% | 34.5% | -40pp | Duration/arithmetic not in DF path |
| WithOrderBy | 81.2% | 44.5% | -37pp | ORDER BY expression compilation |
| Match | 83.2% | 74.3% | -9pp | Variable length patterns |

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
| **2026-02-11** | **1,902** | **48.8%** | **Native DataFusion executor baseline** (legacy disabled) |

### Cumulative Improvement (from DataFusion baseline)

| From | To | Scenarios Gained | Improvement |
|------|-----|------------------|-------------|
| DF Baseline (1,902) | Current (1,902) | +0 | Baseline |

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

6. **Aggregation Functions** (31 remaining failures)
   - SUM, AVG, MIN, MAX, COLLECT not in DF path
   - COUNT works (Aggregation1 100%)

7. **ExistentialSubquery** (10 failures)
   - EXISTS patterns need DF physical implementation

8. **Pattern Predicates** (49 failures)
   - Pattern1/2 need DF expression support

### Low Priority

9. **MERGE Implementation** (72 failures) — not implemented
10. **SET Clause** (49 failures) — limited support
11. **Quantifier Invariants** (64 failures) — edge cases

---

## Next Milestone Targets

| Target | Scenarios | Pass Rate | Gap from Current |
|--------|-----------|-----------|------------------|
| **55%** | 2,143 | 55.0% | +241 scenarios |
| **60%** | 2,338 | 60.0% | +436 scenarios |
| **65%** | 2,533 | 65.0% | +631 scenarios |
| **70%** | 2,728 | 70.0% | +826 scenarios |
| **75%** | 2,923 | 75.0% | +1,021 scenarios |

Achieving 55% likely requires:
- Fix temporal duration/arithmetic expressions → could unlock ~100+ scenarios
- Fix precedence/boolean expression compilation → could unlock ~80+ scenarios
- Fix null handling in DF path → could unlock ~26 scenarios
- Fix Optional Match compilation → could unlock ~26 scenarios

---

## Test Command Reference

```bash
# Run all TCK tests with report
scripts/run_tck_with_report.sh

# Run specific feature
cargo test -p uni-tck --test cucumber -- features/expressions/literals/Literals1.feature

# Run by scenario name regex
cargo test -p uni-tck --test cucumber -- -n 'Should compare dates'

# Reports available at:
#   target/cucumber/report.md (all features)
#   target/cucumber/match-report.md
#   target/cucumber/where-report.md
#   target/cucumber/return-report.md
```
