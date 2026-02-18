# Regression Recovery Analysis

**Generated:** 852 total regressions identified
**Last verified:** 2026-02-16 — **859 failures** (77.9% pass rate, 3037/3897 passing)

## 🔄 Status Since Last Analysis

| Change | Count | Detail |
|--------|-------|--------|
| ✅ **Fixed** | -3 | Conditional2 (Arrow invalid comparison — now passing) |
| 🔴 **New regressions** | +6 | Precedence3 — from `feat(query): implement operator precedence fixes` |
| 🔴 **New regressions** | +4 | Create6 (+2), WithSkipLimit3 (+2), WithSkipLimit1 (+1), WithWhere4 (+1) — root cause TBD |
| **Net change** | +7 | 852 → 859 |

### Precedence3 Pre-Existing Bug (Root Cause Identified)
All 6 Precedence3 failures are scenario [6] "List element containment takes precedence over comparison operator" (one per comparison op: `=`, `<>`, `<`, `>`, `<=`, `>=`).

**The parse is correct.** `[1, 2] = [3, 4] IN [[3, 4], false]` correctly parses as `[1, 2] = ([3, 4] IN [[3, 4], false])` because IN has higher precedence.

**The failure is in the expression compiler.** After parsing:
- `left` = `[1, 2]` → DataType `List<Int64>` (not CypherValue/LargeBinary)
- `right` = `([3, 4] IN [[3, 4], false])` → DataType `Boolean`

In `compile_binary_op_dispatch`, neither side is CypherValue so `has_cv = false`, and the code falls through to `compile_standard` which generates a DataFusion logical `=` expression. DataFusion requires both sides of a comparison to have compatible types and fails:
```
Cannot infer common argument type for comparison operation List(Field { name: "item", data_type: Int64, ... })
```

**Fix needed in `compile_binary_op_dispatch`**: When a List type is compared with a non-List type, the compiler should produce Cypher-correct behavior:
- `=` / `<>`: return `false` / `true` (different types are never equal in Cypher)
- `<` / `>` / `<=` / `>=`: return `null` (ordering across incompatible types is undefined)

## 🎯 Foundational Issues Analysis

**Most issues stem from 2 foundational problems that block everything else:**

### Phase 1: Foundational Issues (Fix First) 🔴

#### 1. Expression Evaluation & Projection
- **Symptoms**: Null values where values expected, "Column not found", Arrow type errors
- **Impact**: Affects ALL query operations that project values
- **Root cause**: Recent refactoring broke expression evaluation and column resolution
- **Examples**:
  - `expected Int(1), got Null` in CALL procedures
  - `Arrow error: Invalid argument error: Invalid comparison` in CASE expressions
  - Column resolution failures

#### 2. Result Collection Pipeline
- **Symptoms**: "No result found" across many different features (48 occurrences)
- **Impact**: Affects ORDER BY, CALL, Subqueries, MERGE/CREATE, DELETE
- **Root cause**: Query result buffering or collection broken after refactoring
- **Examples**:
  - WITH + ORDER BY combinations return no results (158 failures)
  - CALL procedures don't return results
  - CREATE/MERGE operations don't return created entities

**These two issues cause a cascading failure across 400+ tests.**

---

### Phase 2: Dependent Issues (Fix After Phase 1) 🟡

These depend on Phase 1 fixes and will likely auto-resolve partially:

- **WITH Clause Execution** (158 failures) - Depends on result collection
- **ORDER BY Execution** (164 failures) - Depends on result collection + WITH
- **Subquery & Quantifier** (72 failures) - Depends on result collection + expression eval

---

### Phase 3: Isolated Issues (Can Fix in Parallel) 🟢

- **Temporal Type System** (414 failures) - Isolated to temporal features, can fix separately

---

### Phase 4: Validation Issues (Fix Last) ⚪

- **Error Validation** (28 failures) - Missing semantic checks
- **Other categories** - Fix after core execution works

---

## 📋 Recommended Fix Order

```
PRIORITY 1 (Foundational - blocks everything):
  └─ Expression Evaluation & Projection (null handling, type conversions)
  └─ Result Collection Pipeline (WITH result passing, query buffering)
  
PRIORITY 2 (Unblocks after P1):
  └─ WITH Clause Execution
  └─ ORDER BY Execution  
  └─ Subquery & Quantifier Execution
  
PRIORITY 3 (Parallel to P1/P2):
  └─ Temporal Type System (isolated)
  
PRIORITY 4 (After functional fixes):
  └─ Error Validation
  └─ MERGE/CREATE
  └─ CALL procedures
```

---

## Regression Categories

| Category | Count (original) | Count (current) | Delta | Priority |
|----------|-----------------|-----------------|-------|----------|
| [Temporal Type System Issues](#temporal_type_system) | 414 | 415 | +1 | P0 |
| [ORDER BY Execution Issues](#order_by_execution) | 164 | 164 | — | P0 |
| [CALL Procedure Execution](#call_procedure) | 6 | 6 | — | P2 |
| [MERGE and CREATE Issues](#merge_create) | 41 | 44 | +3 (Create6 new) | P2 |
| [Subquery and Quantifier Execution](#subquery_execution) | 72 | 72 | — | P1 |
| [Error Validation Issues](#error_validation) | 28 | ~28 | — | P2 |
| [Result Collection and Type Handling](#result_handling) | 26 | ~27 | +1 | P2 |
| [Other Regressions](#other) | 101 | 113 | +12 (+Precedence3 new) | P0 |

---

## Temporal Type System Issues {#temporal_type_system}

**Regressions:** 415 (was 414; Temporal7 +1)

Issues with Date, Time, DateTime, Duration type handling and conversion

### Root Cause Analysis

The recent refactoring of temporal value handling appears to have broken:

- Date/Time/DateTime parsing from strings
- Temporal value Display/serialization format
- Temporal truncate() function implementation
- Duration property extraction and handling
- Temporal value comparisons and ordering

### Solution Approach

- Review temporal type Display implementation for Cypher canonical form
- Fix temporal function implementations (date(), time(), datetime(), truncate())
- Ensure proper conversion between internal and Cypher representations
- Add comprehensive temporal type tests

### Affected Features

- **Temporal3**: 146 scenarios
- **Temporal9**: 130 scenarios
- **Temporal10**: 80 scenarios
- **Temporal2**: 27 scenarios
- **Temporal6**: 14 scenarios
- **Temporal5**: 7 scenarios
- **Temporal8**: 6 scenarios
- **Temporal7**: 5 scenarios (+1)

### Example Failures

#### Example 1: Temporal10 - [1] Should split between boundaries correctly

```
Query returned error instead of result: Query { message: "Execution error: _duration_property(): duration must be a string"
  ... (truncated)
```

#### Example 2: Temporal10 - [1] Should split between boundaries correctly

```
Query returned error instead of result: Query { message: "Execution error: _duration_property(): duration must be a string"
  ... (truncated)
```

#### Example 3: Temporal10 - [1] Should split between boundaries correctly

```
Query returned error instead of result: Query { message: "Execution error: _duration_property(): duration must be a string"
  ... (truncated)
```

---

## ORDER BY Execution Issues {#order_by_execution}

**Regressions:** 164

ORDER BY clauses failing to return results or returning wrong order

### Root Cause Analysis

ORDER BY functionality broken, possibly related to:

- Query execution pipeline changes
- Result buffering or collection issues
- Type handling in comparison operations
- Interaction with WITH clause

### Solution Approach

- Debug ORDER BY execution path
- Check result collection and buffering
- Verify type comparisons work correctly
- Test ORDER BY with various data types

### Affected Features

- **WithOrderBy2**: 58 scenarios
- **WithOrderBy1**: 54 scenarios
- **WithOrderBy3**: 28 scenarios
- **ReturnOrderBy2**: 8 scenarios
- **WithOrderBy4**: 6 scenarios
- **ReturnOrderBy1**: 4 scenarios
- **ReturnOrderBy6**: 3 scenarios
- **ReturnOrderBy3**: 1 scenarios
- **ReturnOrderBy4**: 1 scenarios
- **ReturnOrderBy5**: 1 scenarios

### Example Failures

#### Example 1: ReturnOrderBy1 - [9] ORDER BY should order lists in the expected order

```
No result found
```

#### Example 2: ReturnOrderBy1 - [10] ORDER BY DESC should order lists in the expected order

```
No result found
```

#### Example 3: ReturnOrderBy1 - [11] ORDER BY should order distinct types in the expected order

```
No result found
```

---

## CALL Procedure Execution {#call_procedure}

**Regressions:** 6

Procedure calls not returning results or failing validation

### Root Cause Analysis

CALL procedure execution broken:

- Procedure result collection issues
- Implicit argument handling
- Variable shadowing validation broken
- Result yielding mechanism

### Solution Approach

- Review procedure call execution path
- Fix result collection and yielding
- Restore variable shadowing validation
- Test all procedure call variants

### Affected Features

- **Call1**: 2 scenarios
- **Call5**: 2 scenarios
- **Call2**: 1 scenarios
- **Call6**: 1 scenarios

### Example Failures

#### Example 1: Call1 - [4] In-query call to procedure that takes no arguments and yields no results and consumes no rows

```
Result mismatch (any order): Row count mismatch: expected 3, got 0
```

#### Example 2: Call1 - [15] In-query procedure call should fail if shadowing an already bound variable

```
No error found
```

#### Example 3: Call2 - [3] Standalone call to procedure with implicit arguments

```
No result found
```

---

## MERGE and CREATE Issues {#merge_create}

**Regressions:** 44 (was 41; +Create6 new)

MERGE and CREATE operations not returning expected results

### Root Cause Analysis

MERGE/CREATE operations affected:

- Result collection after mutations
- WITH clause interaction with CREATE/MERGE
- Bound variable recognition
- Validation of duplicate creates

### Solution Approach

- Review mutation result handling
- Fix WITH + MERGE/CREATE interaction
- Restore duplicate create validation
- Test mutation result flows

### Affected Features

- **Merge5**: 14 scenarios
- **Merge1**: 10 scenarios
- **Merge6**: 5 scenarios
- **Merge7**: 4 scenarios
- **Merge9**: 3 scenarios
- **Create3**: 2 scenarios
- **Create6**: 2 scenarios (**NEW**)
- **Merge2**: 1 scenarios
- **Merge3**: 1 scenarios
- **Merge4**: 1 scenarios

### Example Failures

#### Example 1: Create3 - [11] WITH-MERGE-CREATE: A bound node should be recognized after projection with WITH + MERGE node

```
No result found
```

#### Example 2: Create3 - [12] WITH-MERGE-CREATE: A bound node should be recognized after projection with WITH + MERGE pattern

```
No result found
```

#### Example 3: Merge1 - [1] Merge node when no nodes exist

```
Query returned error instead of result: Query { message: "MERGE node must have a label", query: Some("\nMERGE (a)\nRETURN count(*) AS 
  ... (truncated)
```

---

## Subquery and Quantifier Execution {#subquery_execution}

**Regressions:** 72

Existential subqueries and quantifiers returning wrong results

### Root Cause Analysis

Subquery execution broken:

- Existential subquery evaluation
- Quantifier (ALL, ANY, NONE, SINGLE) execution
- Nested query context handling
- Result row generation from subqueries

### Solution Approach

- Debug subquery execution path
- Fix quantifier evaluation logic
- Ensure proper context passing
- Test nested query scenarios

### Affected Features

- **Quantifier11**: 22 scenarios
- **Quantifier12**: 17 scenarios
- **Quantifier9**: 17 scenarios
- **Quantifier10**: 8 scenarios
- **ExistentialSubquery3**: 2 scenarios
- **ExistentialSubquery1**: 1 scenarios
- **ExistentialSubquery2**: 1 scenarios
- **Quantifier1**: 1 scenarios
- **Quantifier2**: 1 scenarios
- **Quantifier3**: 1 scenarios
- *(and 1 more features)*

### Example Failures

#### Example 1: ExistentialSubquery1 - [2] Simple subquery with WHERE clause

```
Result mismatch (any order): Row count mismatch: expected 1, got 0
```

#### Example 2: ExistentialSubquery2 - [3] Full existential subquery with update clause should fail

```
No error found
```

#### Example 3: ExistentialSubquery3 - [1] Nested simple existential subquery

```
Result mismatch (any order): Row count mismatch: expected 1, got 0
```

---

## Error Validation Issues {#error_validation}

**Regressions:** 28

Expected errors not being raised

### Root Cause Analysis

Error validation removed or broken:

- Variable shadowing checks removed
- Invalid operation validation removed
- Type checking weakened
- Constraint validation disabled

### Solution Approach

- Restore all semantic validation checks
- Review what validation was removed in refactoring
- Add validation tests
- Ensure error messages are correct

### Affected Features

- **TypeConversion3**: 6 scenarios
- **TypeConversion2**: 5 scenarios
- **Return2**: 3 scenarios
- **Path3**: 2 scenarios
- **With4**: 2 scenarios
- **Create1**: 1 scenarios
- **Delete1**: 1 scenarios
- **Delete2**: 1 scenarios
- **Delete5**: 1 scenarios
- **Literals5**: 1 scenarios
- *(and 5 more features)*

### Example Failures

#### Example 1: Create1 - [13] Fail when creating a node that is already bound

```
No error found
```

#### Example 2: Delete1 - [8] Failing when deleting a label

```
No error found
```

#### Example 3: Delete2 - [5] Failing when deleting a relationship type

```
No error found
```

---

## Result Collection and Type Handling {#result_handling}

**Regressions:** 26

Issues with result collection, null values, column naming

### Root Cause Analysis

Result handling changes:

- Column name resolution
- Null value handling in results
- Type conversion in result rows
- Result ordering and collection

### Solution Approach

- Review result collection pipeline
- Fix null handling in projections
- Ensure proper type conversions
- Test result formatting

### Affected Features

- **Return6**: 9 scenarios
- **Return4**: 5 scenarios
- **Return2**: 3 scenarios
- **Graph4**: 1 scenarios
- **Graph9**: 1 scenarios
- **Path1**: 1 scenarios
- **Path2**: 1 scenarios
- **ReturnSkipLimit1**: 1 scenarios
- **ReturnSkipLimit3**: 1 scenarios
- **String1**: 1 scenarios
- *(and 2 more features)*

### Example Failures

#### Example 1: Graph4 - [3] `type()` on null relationship

```
Result mismatch (any order): No match found for actual row 0. Actual values: [("type(r)", Some(String("NOT_THERE"))), ("type(null)
  ... (truncated)
```

#### Example 2: Graph9 - [3] `properties()` on null

```
Result mismatch (any order): No match found for actual row 0. Actual values: [("properties(n)", Some(Null)), ("properties(null)", 
  ... (truncated)
```

#### Example 3: Path1 - [1] `nodes()` on null path

```
Query returned error instead of result: Query { message: "Execution error: VID column has type Null, expected UInt64 or Int64", quer
  ... (truncated)
```

---

## Other Regressions {#other}

**Regressions:** 113 (was 101; +Precedence3 new, Conditional2 fixed)

Miscellaneous regressions requiring individual investigation

### Root Cause Analysis

Various issues. Key new regression: Precedence3 introduced by operator precedence fix commit.

### Solution Approach

- **Precedence3 (P0 — new regression)**: Fix `IN` list containment type inference — the operator precedence refactor made the planner try to compare a value against a `List` type instead of using the IN-list containment logic. Root cause: wrong DataFusion expr generated for `x IN [list]` after precedence rewrite.
- Investigate other features individually

### Affected Features

**New/newly surfaced failures:**
- **Precedence3**: 6 scenarios (**PRE-EXISTING** — was in committed baseline before any of today's work, just missing from original analysis)
- **Create6**: 2 scenarios (**NEW** — filtering after creating nodes/relationships)
- **WithSkipLimit3**: 2 scenarios (**NEW**)
- **WithSkipLimit1**: 1 scenario (**NEW**)
- **WithWhere4**: 1 scenario (**NEW**)

**Fixed since last run:**
- ~~**Conditional2**: 3 scenarios~~ ✅ **FIXED** (Arrow invalid comparison — resolved)

**Existing failures:**
- **Set6**: 15 scenarios
- **Return6**: 10 scenarios
- **Map3**: 8 scenarios
- **Delete5**: 6 scenarios (+1)
- **Return2**: 6 scenarios
- **Return4**: 6 scenarios
- **List12**: 6 scenarios
- **TypeConversion3**: 6 scenarios
- **Set4**: 5 scenarios
- **Set5**: 5 scenarios
- **TypeConversion2**: 5 scenarios
- **Set1**: 4 scenarios
- **Graph6**: 3 scenarios
- **Match4**: 3 scenarios
- **Remove1**: 3 scenarios
- **Remove3**: 3 scenarios
- **With4**: 3 scenarios
- **With6**: 3 scenarios
- *(and ~30 more features with 1-2 failures each)*

### Example Failures

#### Example 1: Precedence3 [6] List element containment takes precedence over comparison operator (NEW)

```
Query returned error instead of result: Query { message: "Error during planning: Cannot infer
common argument type for comparison operation List(Field { name: \"item\", data_type: Int64, ... })" }
```

#### Example 2: Create6 - [5] Filtering after creating nodes affects the result set

```
No result found
```

---

## Complete Regression List

See `/tmp/regression_complete_list.json` for the complete structured list of all regressions.
