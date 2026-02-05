# Uni OpenCypher TCK Failure Analysis

**Updated:** 2026-02-04
**TCK Version:** M23 (openCypher)
**Uni Version:** Current working tree (uncommitted, based on `500d271`)

---

## Executive Summary

| Metric | Count | Rate |
|--------|-------|------|
| **Total Scenarios** | 3,868 | - |
| **Passed** | 1,941 | 50.2% |
| **Failed** | 1,927 | 49.8% |
| **Total Steps** | 13,945 | - |
| **Steps Passed** | 12,018 | 86.2% |

The high step pass rate (86.2%) vs the scenario pass rate (50.2%) indicates that most basic operations work, but many scenarios fail at specific assertion points.

---

## Pass Rates by Category

### Clauses (394/1,222 — 32.2%)

| Subcategory | Passed | Total | Rate |
|-------------|--------|-------|------|
| match | 238 | 352 | 67.6% |
| create | 13 | 78 | 16.7% |
| unwind | 8 | 14 | 57.1% |
| union | 6 | 12 | 50.0% |
| return | 6 | 63 | 9.5% |
| delete | 5 | 41 | 12.2% |
| with | 4 | 29 | 13.8% |
| set | 2 | 53 | 3.8% |
| remove | 2 | 33 | 6.1% |
| merge | 0 | 75 | 0.0% |

### Expressions (1,547/2,616 — 59.1%)

| Subcategory | Passed | Total | Rate |
|-------------|--------|-------|------|
| quantifier | 489 | 604 | 80.9% |
| temporal | 610 | 1,004 | 60.8% |
| conditional | 12 | 13 | 92.3% |
| boolean | 104 | 150 | 69.3% |
| literals | 85 | 131 | 64.9% |
| null | 31 | 44 | 70.5% |
| list | 74 | 185 | 40.0% |
| comparison | 32 | 72 | 44.4% |
| map | 15 | 44 | 34.1% |
| graph | 13 | 61 | 21.3% |
| path | 0 | 7 | 0.0% |
| string | 0 | 32 | 0.0% |

### Use Cases (0/30 — 0.0%)

All 30 use-case scenarios (triadic selection) fail.

---

## Failure Categories

### Overview

| Category | Failures | % of Failed |
|----------|----------|-------------|
| Result Mismatch (wrong values) | ~632 | 32.8% |
| No Result Found (query returns nothing) | ~416 | 21.6% |
| Error Not Raised (Uni too permissive) | ~617 | 32.0% |
| Error Detail Mismatch (wrong error keyword) | ~113 | 5.9% |
| Side Effects Not Verified (harness gap) | ~100 | 5.2% |
| Other | ~49 | 2.5% |

---

## 1. Result Mismatch (~632 failures)

Query returns wrong values. The query executes but produces incorrect output.

### Root Causes

1. **Missing function implementations** — String functions (0/32), path expressions (0/7)
2. **Incorrect aggregation logic** — GROUP BY edge cases, DISTINCT handling
3. **Path handling issues** — Path expressions not fully supported
4. **Type coercion differences** — NULL handling, type conversion

### Priority Files

- `crates/uni-query/src/query/df_expr.rs` — DataFusion expression mapping
- `crates/uni-query/src/query/executor/read.rs` — Expression evaluation

---

## 2. No Result Found (~416 failures)

Query returns no data when data is expected. Usually means the query failed silently or the execution path returned empty results.

### Root Causes

1. **Graph fixture loading failures** — Some fixtures use unsupported `CREATE LABEL` syntax
2. **MERGE clause not implemented** — All 75 MERGE scenarios fail
3. **Missing query plan steps** — Some query patterns don't generate correct plans

---

## 3. Error Not Raised (~617 failures)

TCK expects an error but Uni succeeds or returns wrong results. This indicates Uni is more permissive than the OpenCypher spec requires.

### Sub-categories

| Validation Type | Failures | Description |
|-----------------|----------|-------------|
| InvalidArgumentType | ~246 | Wrong argument types to functions |
| VariableTypeConflict | ~174 | Variable used as wrong type (node vs edge) |
| VariableAlreadyBound | ~90 | Re-binding already bound variable |
| UndefinedVariable | ~75 | Referencing variable not in scope |
| InvalidAggregation | ~32 | Aggregation in wrong context |

### 3.1 InvalidArgumentType (~246 failures)

**Problem:** Functions called with wrong argument types without raising an error.

```cypher
RETURN size(123)       -- size() expects string/list, not integer
RETURN substring(1, 2) -- substring() expects string
RETURN abs('hello')    -- abs() expects number
```

**Fix Location:** `crates/uni-query/src/query/df_expr.rs`
- Add type checking before function evaluation
- Return TypeError for mismatched types

### 3.2 VariableTypeConflict (~174 failures)

**Problem:** Variable used as node in one place, edge in another.

```cypher
MATCH (a)-[a]->()  -- 'a' used as both node and edge
RETURN a
```

**Fix Location:** `crates/uni-query/src/query/planner.rs`
- Track variable types during planning
- Error when same variable bound to incompatible types

### 3.3 VariableAlreadyBound (~90 failures)

**Problem:** Variable bound multiple times in same scope.

```cypher
MATCH (a), (a)  -- 'a' bound twice
RETURN a
```

**Fix Location:** `crates/uni-query/src/query/planner.rs`
- Track bound variables per MATCH clause
- Error on duplicate bindings (except self-references in patterns)

### 3.4 UndefinedVariable (~75 failures)

**Problem:** Referencing variable that hasn't been defined.

```cypher
RETURN x               -- 'x' never defined
MATCH (a) RETURN b     -- 'b' never defined
WITH a AS b RETURN a   -- 'a' not in scope after WITH
```

**Fix Location:** `crates/uni-query/src/query/planner.rs`
- Track variable scope through query clauses
- Error when referencing undefined variable

### 3.5 InvalidAggregation (~32 failures)

**Problem:** Aggregation functions used in wrong context.

```cypher
MATCH (a) WHERE count(*) > 5  -- Aggregation in WHERE
RETURN count(count(*))        -- Nested aggregation
```

**Fix Location:** `crates/uni-query/src/query/planner.rs`

---

## 4. Error Detail Mismatch (~113 failures)

An error is raised but the error message doesn't contain the expected keyword.

| Expected Keyword | Count | Description |
|-----------------|-------|-------------|
| InvalidArgumentType | 58 | Wrong error classification for type errors |
| VariableTypeConflict | 14 | Wrong error for type conflicts |
| UnexpectedSyntax | 12 | Parser error doesn't include expected keyword |
| UndefinedVariable | 5 | Scope error missing keyword |
| InvalidNumberLiteral | 4 | Number parsing error |
| NonConstantExpression | 3 | Constant-folding error |
| InvalidParameterUse | 3 | Parameter validation |
| Other | 14 | Various (ProcedureNotFound, etc.) |

### Fix Approach

Include OpenCypher-standard error keywords in error messages. For example, append `[InvalidArgumentType]` to type-related error messages.

---

## 5. Side Effects Not Verified (~100 failures)

The TCK harness doesn't fully implement side-effect verification for CREATE/DELETE/SET/REMOVE operations. These are harness limitations, not Uni bugs.

---

## Recommended Fix Order

### Quick Wins (Low effort, high impact)

1. **String functions** (~32 failures)
   - Implement STARTS WITH, ENDS WITH, CONTAINS as operators
   - Implement toUpper, toLower, trim, etc. as UDFs

2. **UndefinedVariable validation** (~75 failures)
   - Add variable scope tracking to planner
   - Relatively isolated change

3. **InvalidAggregation validation** (~32 failures)
   - Detect aggregation in WHERE
   - Detect nested aggregation

### Medium Effort

4. **VariableAlreadyBound validation** (~90 failures)
   - Track bound variables per clause
   - Handle pattern self-references correctly

5. **VariableTypeConflict validation** (~174 failures)
   - Track variable types during planning
   - More complex: need to handle type inference

### Higher Effort

6. **InvalidArgumentType validation** (~246 failures)
   - Add type checking to all functions
   - Many functions to update
   - Includes both "not raised" (~246) and "detail mismatch" (~58)

7. **MERGE implementation** (~75 failures)
   - Requires match-or-create semantics
   - ON CREATE / ON MATCH handlers

---

## Test Commands

```bash
# Run all TCK tests (save output for analysis)
cargo test -p uni-tck --test cucumber 2>&1 > /tmp/tck_output.txt

# Run specific feature by glob
cargo test -p uni-tck --test cucumber -- -i '**/Temporal10.feature'

# Run by scenario name regex
cargo test -p uni-tck --test cucumber -- -n 'Should compare dates'

# Count failures by pattern (from saved output)
grep -c "No error found" /tmp/tck_output.txt
grep -c "No result found" /tmp/tck_output.txt
grep -c "No match found for actual row" /tmp/tck_output.txt
```

---

## Progress Tracking

| Date | Scenarios Passed | Pass Rate | Changes |
|------|-----------------|-----------|---------|
| 2026-02-03 (baseline) | 1,279 | 33.1% | Initial TCK runner |
| 2026-02-03 | 1,331 | 34.4% | Schemaless vertex scan |
| 2026-02-04 | 1,352 | 35.0% | Schemaless edge creation |
| 2026-02-04 | 1,355 | 35.0% | CREATE/DELETE empty result, edge dedup |
| 2026-02-04 | 1,764 | 45.6% | Semantic validation, temporal foundations |
| 2026-02-04 | 1,941 | **50.2%** | Temporal7/8/10 (duration between, comparison, DST) |

### Recent Gains (45.6% → 50.2%)

| Feature | Before | After | Gained |
|---------|--------|-------|--------|
| Temporal7 (compare temporal values) | 0/18 | 17/18 | +17 |
| Temporal8 (duration arithmetic) | 0/27 | 27/27 | +27 |
| Temporal10 (duration between) | 0/131 | 125/131 | +125 |
| Other temporal improvements | — | — | +8 |
| **Total** | | | **+177** |

### Remaining Temporal10 Gaps (6 scenarios)

| Scenario | Issue | Fixable? |
|----------|-------|----------|
| [9] Large dates (±999999999 years) | Beyond chrono::NaiveDate range | No (library limit) |
| [10] Large durations in seconds | Beyond chrono::NaiveDate range | No (library limit) |
| [12] Zero-arg timing ×4 | `datetime()` evaluates at different instants | Needs statement-time freeze |

### Next Milestone Target

| Target | Scenarios | Pass Rate | Gap |
|--------|-----------|-----------|-----|
| 55% | 2,127 | 55.0% | +186 scenarios |
| 60% | 2,321 | 60.0% | +380 scenarios |

Achieving 55% likely requires:
- String functions (+32)
- UndefinedVariable validation (+75)
- InvalidAggregation validation (+32)
- VariableAlreadyBound validation (+47 partial)

---

## Appendix: Feature File Impact

### Best Coverage (>60% pass rate)

| Category | Passed | Total | Rate |
|----------|--------|-------|------|
| expressions/conditional | 12 | 13 | 92.3% |
| expressions/quantifier | 489 | 604 | 80.9% |
| expressions/null | 31 | 44 | 70.5% |
| expressions/boolean | 104 | 150 | 69.3% |
| clauses/match | 238 | 352 | 67.6% |
| expressions/literals | 85 | 131 | 64.9% |
| expressions/temporal | 610 | 1,004 | 60.8% |

### Worst Coverage (0% pass rate)

| Category | Total | Issue |
|----------|-------|-------|
| clauses/merge | 75 | MERGE not implemented |
| expressions/string | 32 | String functions missing |
| expressions/path | 7 | Path expressions not supported |
| useCases/triadicSelection | 30 | Requires graph fixtures + complex patterns |
