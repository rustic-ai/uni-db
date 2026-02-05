# TCK Test Failure Report

**Date**: 2026-02-03  
**Total Feature Sets Tested**: 37  
**Total Scenarios**: ~3,300+

## Executive Summary

The TCK test run reveals several fundamental issues blocking compliance:

1. **Schema Requirements**: Uni requires all nodes to have labels and labels must be pre-registered
2. **Query Execution Issues**: Many queries fail to return results ("No result found")
3. **Missing Step Definitions**: Parameters and procedures step definitions not implemented
4. **Error Classification**: Error types differ from TCK expectations (SyntaxError vs SemanticError)
5. **Temporal Support**: Almost no temporal function support
6. **Stack Overflow**: Some query patterns cause stack overflow in debug builds

---

## Results by Feature Set

### Clauses

| Feature Set | Passed | Failed | Total | Pass Rate | Primary Issue |
|-------------|--------|--------|-------|-----------|---------------|
| call | 0 | ~50 | ~50 | 0% | Procedures not implemented |
| create | 0 | ~20 | ~20 | 0% | Stack overflow / No result found |
| delete | 0 | 41 | 41 | 0% | CREATE node must have label |
| match | 0 | 352 | 352 | 0% | Label not found in schema |
| match-where | 0 | 34 | 34 | 0% | Label not found in schema |
| merge | 0 | 75 | 75 | 0% | No result found / Label not found |
| remove | 0 | 33 | 33 | 0% | CREATE node must have label |
| return | 0 | 63 | 63 | 0% | No result found / Error type mismatch |
| return-orderby | 7 | 28 | 35 | 20% | No result found / Error type mismatch |
| return-skip-limit | 1 | 30 | 31 | 3% | CREATE node must have label |
| set | 0 | 53 | 53 | 0% | CREATE node must have label |
| union | 6 | 6 | 12 | 50% | No error found / Label not found |
| unwind | 8 | 6 | 14 | 57% | Label not found / Parameters |
| with | 1 | 28 | 29 | 3% | No result found / Error type mismatch |
| with-orderBy | 0 | 292 | 292 | 0% | No result found |
| with-skip-limit | 0 | 9 | 9 | 0% | CREATE node must have label |
| with-where | 0 | 19 | 19 | 0% | CREATE node must have label |

### Expressions

| Feature Set | Passed | Failed | Total | Pass Rate | Primary Issue |
|-------------|--------|--------|-------|-----------|---------------|
| aggregation | 0 | 35 | 35 | 0% | CREATE node must have label |
| boolean | 13 | 137 | 150 | 9% | No result found |
| comparison | 26 | 46 | 72 | 36% | CREATE node must have label |
| conditional | 12 | 1 | 13 | 92% | No result found |
| existentialSubqueries | 0 | 10 | 10 | 0% | Label not found in schema |
| graph | 1 | 60 | 61 | 2% | No error found / Label not found |
| list | 74 | 111 | 185 | 40% | Label not found in schema |
| literals | 96 | 35 | 131 | 73% | Error message format mismatch |
| map | 15 | 29 | 44 | 34% | Step doesn't match / No result found |
| mathematical | 2 | 4 | 6 | 33% | CREATE node must have label |
| null | 29 | 15 | 44 | 66% | Parameters step not implemented |
| path | 0 | 7 | 7 | 0% | No result found / Error type mismatch |
| pattern | 0 | 50 | 50 | 0% | Label not found in schema |
| precedence | 36 | 85 | 121 | 30% | No result found |
| quantifier | 489 | 115 | 604 | 81% | No result found |
| string | 0 | 32 | 32 | 0% | Label not found in schema |
| temporal | 5 | 999 | 1004 | 0.5% | No result found / Functions not implemented |
| typeConversion | 13 | 34 | 47 | 28% | CREATE node must have label |

### Use Cases

| Feature Set | Passed | Failed | Total | Pass Rate | Primary Issue |
|-------------|--------|--------|-------|-----------|---------------|
| countingSubgraphMatches | 0 | 11 | 11 | 0% | binary-tree-2 graph fixture error |
| triadicSelection | 0 | 19 | 19 | 0% | CREATE LABEL syntax parse error |

---

## Failure Categories (Prioritized by Impact)

### 1. **CREATE Node Must Have At Least One Label** (HIGH IMPACT)

**Error**: `Setup query failed: Query error: CREATE node must have at least one label`

**Affected**: ~500+ scenarios across Delete, Set, Remove, Comparison, WithSkipLimit, WithWhere, ReturnSkipLimit, Mathematical, TypeConversion, Aggregation

**Root Cause**: Uni enforces that all nodes must have at least one label. TCK tests often create unlabeled nodes like `CREATE ()` or `CREATE ()-[:REL]->()`.

**TCK Examples**:
```cypher
CREATE ()
CREATE ()-[:KNOWS]->()
CREATE (n {name: 'Alice'})
```

**Fix Options**:
1. Support optional labels (significant design change)
2. Auto-assign a default label (e.g., `_Node`)
3. Modify TCK test fixtures to include labels

---

### 2. **Label Not Found in Schema** (HIGH IMPACT)

**Error**: `Setup query failed: Query error: Label X not found in schema`

**Affected**: ~600+ scenarios across Match, MatchWhere, String, Pattern, ExistentialSubqueries, Graph, List

**Root Cause**: Uni requires labels to be pre-registered in schema before use. TCK tests dynamically create nodes with arbitrary labels.

**TCK Examples**:
```cypher
CREATE (:Person {name: 'Alice'})  -- 'Person' not pre-registered
CREATE (:A)-[:REL]->(:B)          -- 'A', 'B' not pre-registered
```

**Fix Options**:
1. Auto-create labels on first use
2. Pre-register all TCK labels in test fixtures
3. Add "CREATE LABEL IF NOT EXISTS" support

---

### 3. **No Result Found** (HIGH IMPACT)

**Error**: `No result found` in Then step assertions

**Affected**: ~400+ scenarios across Boolean, Precedence, Return, With, WithOrderBy, Path, Temporal, Quantifier

**Root Cause**: Query execution completes but doesn't capture results properly, or query fails silently.

**Potential Causes**:
- Query execution returning error instead of result
- Result not being stored in UniWorld state
- Query returning empty when data expected

**Requires Investigation**: Debug specific failing queries to determine root cause

---

### 4. **Step Doesn't Match Any Function** (MEDIUM IMPACT)

**Error**: `Step doesn't match any function`

**Affected**: ~100+ scenarios

**Missing Steps**:
- `And there exists a procedure test.XYZ() :: ...` - Procedure definitions
- `And parameters are:` - Query parameters
- `Then the result should be (ignoring element order for lists):` - List order-agnostic matching

**Fix**: Implement missing step definitions in `crates/uni-tck/src/steps/`

---

### 5. **Error Type Mismatch** (MEDIUM IMPACT)

**Error**: `Error type mismatch: expected SyntaxError, got SemanticError`

**Affected**: ~50+ scenarios in Return, With, Path, ReturnOrderBy, WithOrderBy

**Examples**:
- `AmbiguousAggregationExpression` - TCK expects SyntaxError, Uni returns SemanticError
- `InvalidArgumentType` - TCK expects SyntaxError, Uni returns SemanticError
- `UndefinedVariable` - TCK expects SyntaxError, Uni returns SemanticError
- `NoVariablesInScope` - TCK expects SyntaxError, Uni returns SemanticError

**Root Cause**: Uni classifies some errors as semantic that TCK considers syntactic.

**Fix Options**:
1. Reclassify error types in Uni
2. Update error matcher to be more flexible
3. Document intentional differences

---

### 6. **Error Detail Mismatch** (LOW IMPACT)

**Error**: `Error detail mismatch: expected message to contain 'XYZ', got '...'`

**Affected**: ~50+ scenarios primarily in Literals, Call

**Examples**:
- Expected: `ProcedureNotFound`, Got: `Unknown procedure test.my.proc`
- Expected: `UnexpectedSyntax`, Got: `Parse error: ... expected string`
- Expected: `InvalidUnicodeCharacter`, Got: `Parse error: ... expected EOI`

**Root Cause**: Error message format differs from TCK expectations

**Fix**: Normalize error messages to include expected TCK detail codes

---

### 7. **No Error Found** (LOW IMPACT)

**Error**: `No error found` when error was expected

**Affected**: ~30+ scenarios in Union, Graph

**Examples**:
- `DifferentColumnsInUnion` not raised
- `InvalidClauseComposition` not raised  
- `InvalidArgumentType` for `properties()` not raised

**Root Cause**: Uni allows some constructs that TCK expects to fail

**Fix**: Add validation for these edge cases

---

### 8. **Stack Overflow** (CRITICAL)

**Error**: `thread 'main' has overflowed its stack`

**Affected**: Create feature set, potentially others

**Root Cause**: Deep recursion during query execution, likely in async state machines

**Note**: `.cargo/config.toml` already sets 8MB stack, but may need more for debug builds

---

### 9. **Named Graph Fixtures Error** (MEDIUM IMPACT)

**Error**: `Failed to load graph 'binary-tree-2': Parse error: ... CREATE LABEL Node`

**Affected**: UseCases feature sets (30 scenarios)

**Root Cause**: Graph fixture files use `CREATE LABEL` DDL syntax that Uni parser doesn't recognize

**Fix**: Update fixture loader to handle DDL or convert fixtures to use schema API

---

## High-Pass-Rate Feature Sets (> 50%)

These feature sets have good compatibility and minimal issues:

| Feature Set | Pass Rate | Notes |
|-------------|-----------|-------|
| quantifier | 81% | Strong list predicate support |
| literals | 73% | Basic value support works |
| null | 66% | Null handling mostly correct |
| unwind | 57% | UNWIND clause works |
| conditional | 92% | CASE/WHEN works well |
| union | 50% | Basic UNION support |

---

## Recommendations

### Immediate Fixes (Quick Wins)

1. **Implement missing step definitions**:
   - `And parameters are:` step for query parameters
   - `Then the result should be (ignoring element order for lists):` step

2. **Fix result capture** in UniWorld for queries returning results

3. **Add flexible error type matching** (accept SemanticError where SyntaxError expected)

### Medium-Term Fixes

4. **Support unlabeled nodes** or auto-assign default label

5. **Auto-create labels on first use** or pre-register TCK labels

6. **Implement stored procedures** framework (even if empty implementations)

7. **Fix named graph fixture loader** to handle `CREATE LABEL` syntax

### Long-Term Improvements

8. **Temporal type support** - Currently ~0.5% pass rate

9. **Error message normalization** to match TCK expected codes

10. **Stack size investigation** for Create feature set

---

## Appendix: Feature Files Inventory

```
crates/uni-tck/features/
├── clauses/
│   ├── call/           (6 files)
│   ├── create/         (6 files)
│   ├── delete/         (6 files)
│   ├── match/          (9 files)
│   ├── match-where/    (6 files)
│   ├── merge/          (9 files)
│   ├── remove/         (3 files)
│   ├── return/         (8 files)
│   ├── return-orderby/ (6 files)
│   ├── return-skip-limit/ (3 files)
│   ├── set/            (6 files)
│   ├── union/          (3 files)
│   ├── unwind/         (1 file)
│   ├── with/           (7 files)
│   ├── with-orderBy/   (4 files)
│   ├── with-skip-limit/ (3 files)
│   └── with-where/     (7 files)
├── expressions/
│   ├── aggregation/    (8 files)
│   ├── boolean/        (5 files)
│   ├── comparison/     (4 files)
│   ├── conditional/    (2 files)
│   ├── existentialSubqueries/ (3 files)
│   ├── graph/          (9 files)
│   ├── list/           (12 files)
│   ├── literals/       (8 files)
│   ├── map/            (3 files)
│   ├── mathematical/   (17 files)
│   ├── null/           (3 files)
│   ├── path/           (3 files)
│   ├── pattern/        (2 files)
│   ├── precedence/     (4 files)
│   ├── quantifier/     (12 files)
│   ├── string/         (14 files)
│   ├── temporal/       (10 files)
│   └── typeConversion/ (6 files)
└── useCases/
    ├── countingSubgraphMatches/ (1 file)
    └── triadicSelection/ (1 file)
```
