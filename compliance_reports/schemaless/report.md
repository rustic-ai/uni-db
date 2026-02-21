# TCK Compliance Report

**Generated:** 2026-02-21 10:43:40
**Results:** `results_20260221_104340.json`
**Compared to:** `results_20260221_044231.json`

## Summary

| Metric | Current | Previous | Delta |
|--------|---------|----------|-------|
| Scenarios | 1752 | 3897 | -2145 |
| Passed | 1740 | 3869 | -2129 |
| Failed | 11 | 27 | -16 |
| Pass Rate | 99.3% | 99.3% | 📈 +0.0pp |

## Feature Breakdown

| Feature | Scenarios | Passed | Failed | Rate | Delta |
|---------|-----------|--------|--------|------|-------|
| ✅ CountingSubgraphMatches1 | 11 | 11 | 0 | 100% |  |
| ⚠️ ExistentialSubquery1 | 4 | 3 | 1 | 75% |  |
| ⚠️ ExistentialSubquery2 | 3 | 2 | 1 | 67% |  |
| ❌ ExistentialSubquery3 | 3 | 1 | 2 | 33% |  |
| ❌ Literals7 | 1 | 0 | 0 | 0% | -95pp |
| ✅ Precedence2 | 1 | 1 | 0 | 100% |  |
| ✅ Precedence3 | 11 | 11 | 0 | 100% |  |
| ✅ Precedence4 | 12 | 12 | 0 | 100% |  |
| ✅ Quantifier1 | 105 | 105 | 0 | 100% |  |
| ✅ Quantifier10 | 8 | 8 | 0 | 100% |  |
| ✅ Quantifier11 | 22 | 22 | 0 | 100% |  |
| ✅ Quantifier12 | 17 | 17 | 0 | 100% |  |
| ✅ Quantifier2 | 106 | 106 | 0 | 100% |  |
| ✅ Quantifier3 | 105 | 105 | 0 | 100% |  |
| ✅ Quantifier4 | 105 | 105 | 0 | 100% |  |
| ✅ Quantifier5 | 31 | 31 | 0 | 100% |  |
| ✅ Quantifier6 | 21 | 21 | 0 | 100% |  |
| ✅ Quantifier7 | 36 | 36 | 0 | 100% |  |
| ✅ Quantifier8 | 31 | 31 | 0 | 100% |  |
| ✅ Quantifier9 | 17 | 17 | 0 | 100% |  |
| ✅ String1 | 1 | 1 | 0 | 100% |  |
| ✅ String10 | 9 | 9 | 0 | 100% |  |
| ✅ String11 | 2 | 2 | 0 | 100% |  |
| ✅ String3 | 1 | 1 | 0 | 100% |  |
| ❌ String4 | 1 | 0 | 1 | 0% |  |
| ✅ String8 | 9 | 9 | 0 | 100% |  |
| ✅ String9 | 9 | 9 | 0 | 100% |  |
| ✅ Temporal1 | 207 | 207 | 0 | 100% |  |
| ✅ Temporal10 | 131 | 129 | 2 | 98% |  |
| ✅ Temporal2 | 53 | 53 | 0 | 100% |  |
| ✅ Temporal3 | 183 | 183 | 0 | 100% |  |
| ✅ Temporal4 | 39 | 39 | 0 | 100% |  |
| ✅ Temporal5 | 7 | 6 | 1 | 86% |  |
| ✅ Temporal6 | 17 | 16 | 1 | 94% |  |
| ✅ Temporal7 | 18 | 16 | 2 | 89% |  |
| ✅ Temporal8 | 27 | 27 | 0 | 100% |  |
| ✅ Temporal9 | 322 | 322 | 0 | 100% |  |
| ✅ TriadicSelection1 | 19 | 19 | 0 | 100% |  |
| ✅ TypeConversion1 | 10 | 10 | 0 | 100% |  |
| ✅ TypeConversion2 | 12 | 12 | 0 | 100% |  |
| ✅ TypeConversion3 | 11 | 11 | 0 | 100% |  |
| ✅ TypeConversion4 | 14 | 14 | 0 | 100% |  |

## Failed Scenarios

### ExistentialSubquery1

- **[2] Simple subquery with WHERE clause** (line 53)
  ```
  Step failed:
      Defined: tck/features/expressions/existentialSubqueries/ExistentialSubquery1.feature:69:5
      Matched: crates/uni-tck/src/steps/then.rs:20:1
      Step panicked. Captured output: Result mismatch (any order): Row count mismatch: expected 1, got 0
[Summary]
1 feature
1 scenario (1
  ... (truncated)
  ```

### ExistentialSubquery2

- **[3] Full existential subquery with update clause should fail** (line 78)
  ```
  Step failed:
      Defined: tck/features/expressions/existentialSubqueries/ExistentialSubquery2.feature:88:5
      Matched: crates/uni-tck/src/steps/then.rs:114:1
      Step panicked. Captured output: No error found
[Summary]
1 feature
1 scenario (1 failed)
3 steps (2 passed, 1 failed)

  ```

### ExistentialSubquery3

- **[1] Nested simple existential subquery** (line 33)
  ```
  Step failed:
      Defined: tck/features/expressions/existentialSubqueries/ExistentialSubquery3.feature:51:5
      Matched: crates/uni-tck/src/steps/then.rs:20:1
      Step panicked. Captured output: Result mismatch (any order): Row count mismatch: expected 1, got 0
[Summary]
1 feature
1 scenario (1
  ... (truncated)
  ```
- **[3] Nested full existential subquery with pattern predicate** (line 79)
  ```
  Step failed:
      Defined: tck/features/expressions/existentialSubqueries/ExistentialSubquery3.feature:97:5
      Matched: crates/uni-tck/src/steps/then.rs:20:1
      Step panicked. Captured output: Query returned error instead of result: Query { message: "Execution error: EXISTS subquery execution
  ... (truncated)
  ```

### String4

- **[1] `split()`** (line 33)
  ```
  Step failed:
      Defined: tck/features/expressions/string/String4.feature:40:5
      Matched: crates/uni-tck/src/steps/then.rs:20:1
      Step panicked. Captured output: Result mismatch (any order): No match found for actual row 0. Actual values: [("item", Some(Int(0)))]. Expected: [{"item": Int(2
  ... (truncated)
  ```

### Temporal10

- **[9] Should handle large durations** (line 252)
  ```
  Step failed:
      Defined: tck/features/expressions/temporal/Temporal10.feature:258:5
      Matched: crates/uni-tck/src/steps/then.rs:20:1
      Step panicked. Captured output: Query returned error instead of result: Query { message: "Execution error: date(): Invalid date format: Cannot parse datet
  ... (truncated)
  ```
- **[10] Should handle large durations in seconds** (line 263)
  ```
  Step failed:
      Defined: tck/features/expressions/temporal/Temporal10.feature:269:5
      Matched: crates/uni-tck/src/steps/then.rs:20:1
      Step panicked. Captured output: Query returned error instead of result: Query { message: "Execution error: localdatetime(): Cannot parse datetime: -999999
  ... (truncated)
  ```

### Temporal5

- **[6] Should provide accessors for date time** (line 119)
  ```
  Step failed:
      Defined: tck/features/expressions/temporal/Temporal5.feature:133:5
      Matched: crates/uni-tck/src/steps/then.rs:20:1
      Step panicked. Captured output: Result mismatch (any order): No match found for actual row 0. Actual values: [("d.offset", Some(String("+01:00"))), ("d.epo
  ... (truncated)
  ```

### Temporal6

- **[6] Should serialize duration** (line 117)
  ```
  Step failed:
      Defined: tck/features/expressions/temporal/Temporal6.feature:100:5
      Matched: crates/uni-tck/src/steps/then.rs:20:1
      Step panicked. Captured output: Query returned error instead of result: Query { message: "Execution error: duration(): Invalid date-time style duration dat
  ... (truncated)
  ```

### Temporal7

- **[6] Should compare durations for equality** (line 134)
  ```
  Step failed:
      Defined: tck/features/expressions/temporal/Temporal7.feature:125:5
      Matched: crates/uni-tck/src/steps/then.rs:20:1
      Step panicked. Captured output: Query returned error instead of result: Query { message: "Error during planning: Cannot infer common argument type for comp
  ... (truncated)
  ```
- **[6] Should compare durations for equality** (line 136)
  ```
  Step failed:
      Defined: tck/features/expressions/temporal/Temporal7.feature:125:5
      Matched: crates/uni-tck/src/steps/then.rs:20:1
      Step panicked. Captured output: Query returned error instead of result: Query { message: "Error during planning: Cannot infer common argument type for comp
  ... (truncated)
  ```
