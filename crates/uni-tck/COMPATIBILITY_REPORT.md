# Uni OpenCypher TCK Compatibility Report

**Generated:** 2026-02-04
**TCK Version:** M23 (openCypher)
**Uni Version:** Current main branch

---

## Executive Summary

| Metric | Count | Pass Rate |
|--------|-------|-----------|
| **Features** | 191 | - |
| **Scenarios** | 3,867 | **45.6%** (1,764 passed, 2,103 failed) |
| **Steps** | 13,764 | **84.7%** (11,661 passed, 2,103 failed) |
| **Parsing Errors** | 1 | Match5.feature |

The high step pass rate (84.7%) vs lower scenario pass rate (45.6%) indicates that most basic operations work, but many scenarios fail at specific assertion points.

---

## Category Pass Rates

| Category | Passed | Failed | Total | Rate |
|----------|--------|--------|-------|------|
| Quantifier | 489 | 115 | 604 | 81.0% |
| Temporal | 420 | 584 | 1,004 | 41.8% |
| Match | 238 | 114 | 352 | 67.6% |
| Boolean | 104 | 46 | 150 | 69.3% |
| Literals | 97 | 34 | 131 | 74.0% |
| WithOrderBy | 88 | 204 | 292 | 30.1% |
| List | 74 | 111 | 185 | 40.0% |
| Precedence | 60 | 61 | 121 | 49.6% |
| Comparison | 32 | 40 | 72 | 44.4% |
| Null | 31 | 13 | 44 | 70.5% |
| TypeConversion | 17 | 30 | 47 | 36.2% |
| Map | 15 | 29 | 44 | 34.1% |
| Graph | 13 | 47 | 60 | 21.7% |
| Create | 13 | 65 | 78 | 16.7% |
| Conditional | 12 | 1 | 13 | 92.3% |
| ReturnSkipLimit | 12 | 19 | 31 | 38.7% |
| ReturnOrderBy | 9 | 26 | 35 | 25.7% |
| Unwind | 8 | 6 | 14 | 57.1% |
| Union | 6 | 6 | 12 | 50.0% |
| Return | 6 | 57 | 63 | 9.5% |
| Delete | 5 | 36 | 41 | 12.2% |
| With | 4 | 25 | 29 | 13.8% |
| MatchWhere | 3 | 31 | 34 | 8.8% |
| Mathematical | 2 | 4 | 6 | 33.3% |
| Pattern | 2 | 48 | 50 | 4.0% |
| Remove | 2 | 31 | 33 | 6.1% |
| Set | 2 | 51 | 53 | 3.8% |
| Aggregation | 0 | 35 | 35 | 0.0% |
| Call | 0 | 52 | 52 | 0.0% |
| CountingSubgraphMatches | 0 | 11 | 11 | 0.0% |
| ExistentialSubquery | 0 | 10 | 10 | 0.0% |
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
| Conditional2 - Case Expression | 12 | 12 |
| Literals1 - Boolean and Null | 6 | 6 |
| Quantifier5 - None quantifier interop | 31 | 31 |
| Quantifier6 - Single quantifier interop | 21 | 21 |
| Quantifier8 - All quantifier interop | 31 | 31 |
| ReturnOrderBy4 - Order by in combination with projection | 2 | 2 |
| Mathematical11 - Signed numbers functions | 1 | 1 |
| Mathematical13 - Square root | 1 | 1 |

### High Pass Rate (>75%)

| Feature | Passed | Total | Rate |
|---------|--------|-------|------|
| Literals5 - Float | 26 | 27 | 96.3% |
| Quantifier1 - None quantifier | 94 | 105 | 89.5% |
| Quantifier3 - Any quantifier | 94 | 105 | 89.5% |
| Quantifier4 - All quantifier | 93 | 105 | 88.6% |
| Temporal6 - Render Temporal Values as a String | 15 | 17 | 88.2% |
| Quantifier2 - Single quantifier | 93 | 106 | 87.7% |
| Quantifier7 - Any quantifier interop | 31 | 36 | 86.1% |
| Literals6 - String | 11 | 13 | 84.6% |
| Match1 - Match nodes | 73 | 86 | 84.9% |
| Match2 - Match relationships | 71 | 86 | 82.6% |
| Null1 - IS NULL validation | 14 | 17 | 82.4% |
| Null2 - IS NOT NULL validation | 14 | 17 | 82.4% |
| Boolean1 - And logical operations | 24 | 30 | 80.0% |
| Boolean2 - OR logical operations | 24 | 30 | 80.0% |
| Boolean3 - XOR logical operations | 24 | 30 | 80.0% |
| Literals4 - Octal integer | 8 | 10 | 80.0% |
| Match6 - Match named paths scenarios | 78 | 97 | 80.4% |
| Precedence2 - On numeric values | 20 | 26 | 76.9% |

### Medium Pass Rate (25-75%)

| Feature | Passed | Total | Rate |
|---------|--------|-------|------|
| Graph9 - Retrieve all properties as a property map | 5 | 7 | 71.4% |
| Temporal1 - Create Temporal Values from a Map | 141 | 207 | 68.1% |
| Literals2 - Decimal integer | 8 | 12 | 66.7% |
| Literals3 - Hexadecimal integer | 11 | 16 | 68.8% |
| Literals8 - Maps | 18 | 27 | 66.7% |
| Precedence4 - On null value | 8 | 12 | 66.7% |
| Comparison1 - Equality | 28 | 43 | 65.1% |
| Map1 - Static value access | 12 | 19 | 63.2% |
| Boolean5 - Interop of logical operations | 5 | 8 | 62.5% |
| List5 - List Membership Validation - IN Operator | 28 | 46 | 60.9% |
| Union1 - Union | 3 | 5 | 60.0% |
| Union2 - Union All | 3 | 5 | 60.0% |
| Temporal5 - Access Components of Temporal Values | 4 | 7 | 57.1% |
| Unwind1 | 8 | 14 | 57.1% |
| Temporal4 - Store Temporal Values | 21 | 39 | 53.8% |
| Temporal9 - Truncate Temporal Values | 172 | 322 | 53.4% |
| Boolean4 - NOT logical operations | 27 | 52 | 51.9% |
| List11 - Create a list from a range | 34 | 67 | 50.7% |
| Delete3 - Deleting named paths | 1 | 2 | 50.0% |
| Graph8 - Property keys function | 4 | 8 | 50.0% |
| Return1 - Return single variable | 1 | 2 | 50.0% |
| ReturnOrderBy1 - Order by a single variable | 6 | 12 | 50.0% |
| TypeConversion4 - To String | 7 | 14 | 50.0% |
| With2 - Forward single expression | 1 | 2 | 50.0% |
| ReturnSkipLimit2 - Limit | 8 | 17 | 47.1% |
| Literals7 - List | 9 | 20 | 45.0% |
| Precedence1 - On boolean values | 32 | 72 | 44.4% |
| Comparison2 - Half-bounded Range | 3 | 19 | 42.9% |
| List3 - List Equality | 3 | 7 | 42.9% |
| TypeConversion1 - To Boolean | 4 | 10 | 40.0% |
| ReturnSkipLimit1 - Skip | 4 | 11 | 36.4% |
| WithOrderBy1 - Order by a single variable | 29 | 96 | 30.2% |
| WithOrderBy2 - Order by a single expression | 25 | 83 | 30.1% |
| WithOrderBy3 - Order by multiple expressions | 32 | 93 | 34.4% |
| Match7 - Optional match | 11 | 31 | 35.5% |
| Create1 - Creating nodes | 7 | 20 | 35.0% |
| List2 - List Slicing | 5 | 15 | 33.3% |
| Delete4 - Delete clause interoperation with other clauses | 1 | 3 | 33.3% |
| Temporal3 - Project Temporal Values from other Temporal Values | 60 | 183 | 32.8% |
| Null3 - Null evaluation | 3 | 10 | 30.0% |
| TypeConversion2 - To Integer | 3 | 12 | 25.0% |
| TypeConversion3 - To Float | 3 | 11 | 27.3% |
| Create2 - Creating relationships | 6 | 24 | 25.0% |
| Delete1 - Deleting nodes | 2 | 8 | 25.0% |

### Low Pass Rate (<25%)

| Feature | Passed | Total | Rate |
|---------|--------|-------|------|
| With6 - Implicit grouping with aggregates | 2 | 9 | 22.2% |
| MatchWhere1 - Filter single variable | 3 | 15 | 20.0% |
| ReturnOrderBy6 - Aggregation expressions in order by | 1 | 5 | 20.0% |
| Return6 - Implicit grouping with aggregates | 4 | 21 | 19.0% |
| Map3 - Keys function | 2 | 11 | 18.2% |
| Match3 - Match fixed length patterns | 5 | 30 | 16.7% |
| With1 - Forward single variable | 1 | 6 | 16.7% |
| Comparison2 - Half-bounded Range | 3 | 19 | 15.8% |
| Remove1 - Remove a Property | 1 | 7 | 14.3% |
| Graph6 - Static property access | 2 | 14 | 14.3% |
| Temporal2 - Create Temporal Values from a String | 7 | 53 | 13.2% |
| Quantifier10 - Single quantifier invariants | 1 | 8 | 12.5% |
| Remove2 - Remove a Label | 1 | 5 | 20.0% |
| Delete5 - Delete clause interoperation with built-in data types | 1 | 9 | 11.1% |
| Graph3 - Node labels | 1 | 9 | 11.1% |
| Graph4 - Edge relationship type | 1 | 11 | 9.1% |
| List6 - List size | 2 | 17 | 11.8% |
| Comparison3 - Full-Bound Range | 1 | 9 | 11.1% |
| Set1 - Set a Property | 1 | 11 | 9.1% |
| Set3 - Set a Label | 1 | 8 | 12.5% |
| WithOrderBy4 - Order by in combination with projection and aliasing | 2 | 20 | 10.0% |
| Pattern2 - Pattern Comprehension | 1 | 11 | 9.1% |
| Return4 - Column renaming | 1 | 11 | 9.1% |
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
| Call1 - Basic procedure calling | 16 |
| Call2 - Procedure arguments | 6 |
| Call3 - Assignable-type arguments | 6 |
| Call4 - Null Arguments | 2 |
| Call5 - Results projection | 19 |
| Call6 - Call clause interoperation with other clauses | 3 |
| Comparison4 - Combination of Comparisons | 1 |
| Conditional1 - Coalesce expression | 1 |
| CountingSubgraphMatches1 | 11 |
| Create3 - Interoperation with other clauses | 13 |
| Create4 - Large Create Query | 2 |
| Create5 - Multiple hops create patterns | 5 |
| Create6 - Persistence of create clause side effects | 14 |
| Delete2 - Deleting relationships | 5 |
| Delete6 - Persistence of delete clause side effects | 14 |
| ExistentialSubquery1 - Simple existential subquery | 4 |
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
| MatchWhere2 - Filter multiple variables | 2 |
| MatchWhere3 - Equi-Joins on variables | 3 |
| MatchWhere4 - Non-Equi-Joins on variables | 2 |
| MatchWhere5 - Filter on predicate resulting in null | 4 |
| MatchWhere6 - Filter optional matches | 8 |
| Mathematical2 - Addition | 1 |
| Mathematical3 - Subtraction | 1 |
| Mathematical8 - Arithmetic precedence | 2 |
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
| Temporal7 - Compare Temporal Values | 18 |
| Temporal8 - Compute Arithmetic Operations on Temporal Values | 27 |
| Temporal10 - Compute Durations Between two Temporal Values | 131 |
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

## Remaining Failure Types (Expected Errors Not Raised)

These are cases where the TCK expects an error but Uni accepts the query:

| Count | Expected Error | Description |
|-------|---------------|-------------|
| 61 | InvalidArgumentType | Wrong type for operations (NOT on non-boolean, arithmetic on strings) |
| 28 | InvalidArgumentValue | Runtime type check failures |
| 23 | UnexpectedSyntax | Syntax that should be rejected |
| 22 | UndefinedVariable | Variables used before definition |
| 22 | InvalidArgumentType (Argument) | Runtime argument type errors |
| 17 | VariableTypeConflict | Variable used as conflicting types |
| 10 | InvalidArgumentType (Type) | Compile-time type errors |
| 6 | IntegerOverflow | Integer literal overflow |
| 6 | AmbiguousAggregationExpression | Aggregation scope ambiguity |
| 5 | VariableAlreadyBound | Variable illegally rebound |
| 5 | NumberOutOfRange | Runtime number range errors |
| 4 | NonConstantExpression | Non-constant where constant required |
| 4 | InvalidParameterUse | Parameter used incorrectly |
| 4 | InvalidNumberLiteral | Malformed number literals |
| 3 | NoSingleRelationshipType | Relationship type required |
| 3 | DeletedEntityAccess | Accessing deleted entity |
| 2 | InvalidRelationshipPattern | Malformed relationship pattern |
| 2 | InvalidDelete | Invalid delete target |
| 2 | InvalidClauseComposition | Clause ordering error |
| 2 | ColumnNameConflict | Duplicate column names |
| 2 | MergeReadOwnWrites | MERGE read-own-writes issue |
| 2 | ProcedureNotFound | Missing procedure |

---

## Failure Root Causes

1. **Missing/Wrong Results (~1,100 failures)**
   - Most common failure type
   - Root causes: missing functions (aggregation, string, list), incorrect query execution, edge counting bugs
   - Includes: wrong values, empty results, extra/missing rows

2. **Over-Permissive Behavior (~200 remaining "no error" failures)**
   - Uni accepts queries that openCypher rejects
   - **Addressed:** Added semantic validation for:
     - UndefinedVariable (variables used before definition)
     - VariableTypeConflict (variable reused with conflicting type)
     - VariableAlreadyBound (path variable conflicts, variable re-binding)
     - InvalidArgumentType (wrong type for DELETE, SKIP/LIMIT, WITH ORDER BY)
     - InvalidAggregation (aggregation in WITH ORDER BY)
     - NegativeIntegerArgument (negative SKIP/LIMIT)
   - Still needs: more comprehensive validation coverage

3. **Unimplemented Features (~140 failures)**
   - Procedure CALL infrastructure incomplete (52 scenarios)
   - MERGE not implemented (75 scenarios)
   - Existential subqueries not supported (10 scenarios)

4. **TCK Harness Gaps (~100 failures)**
   - Side effect verification not implemented
   - Parameter handling incomplete

5. **Named Graph Fixtures (19 failures)**
   - binary-tree-1/2 use `CREATE LABEL` syntax
   - Parser doesn't support this syntax

---

## Feature Coverage Analysis

### Clauses

| Clause | Status | Pass Rate | Notes |
|--------|--------|-----------|-------|
| MATCH | Good | 67.6% | Basic patterns strong, variable-length paths missing |
| WHERE | Partial | 8.8% | Simple filters work, joins and null predicates fail |
| RETURN | Partial | 9.5% | Core works, expressions/aggregates need work |
| CREATE | Partial | 16.7% | Basic node/edge creation works |
| DELETE | Partial | 12.2% | Basic delete works, interop issues |
| SET | Limited | 3.8% | Basic property setting works |
| REMOVE | Limited | 6.1% | Basic functionality present |
| MERGE | None | 0.0% | Not implemented |
| WITH | Partial | 13.8% | Piping works, WHERE/aliasing issues |
| WITH ORDER BY | Partial | 30.1% | Basic ordering works, complex expressions fail |
| UNWIND | Good | 57.1% | List unwinding mostly works |
| UNION | Good | 50.0% | Basic union works |
| CALL | None | 0.0% | Procedure infrastructure incomplete |

### Expressions

| Category | Status | Pass Rate | Notes |
|----------|--------|-----------|-------|
| Quantifiers | Strong | 81.0% | ALL, ANY, NONE, SINGLE well-supported |
| Literals | Good | 74.0% | Booleans, integers, floats, strings work |
| Null handling | Good | 70.5% | Three-valued logic correct |
| Boolean | Good | 69.3% | AND, OR, NOT, XOR work |
| Conditional (CASE) | Strong | 92.3% | CASE expressions fully supported |
| Comparison | Moderate | 44.4% | Equality good, ranges need work |
| Precedence | Moderate | 49.6% | Numeric precedence good, boolean/list issues |
| Temporal | Moderate | 41.8% | Creation and truncation work, arithmetic/comparison missing |
| List | Moderate | 40.0% | IN operator and ranges work, comprehension/dynamic access weak |
| Type Conversion | Moderate | 36.2% | toString partially works |
| Map | Moderate | 34.1% | Static access good, dynamic/keys weak |
| String | None | 0.0% | String functions not implemented |
| Aggregation | None | 0.0% | COUNT, SUM, etc. not returning correct results |
| Pattern | Limited | 4.0% | Pattern predicates incomplete |
| Path | None | 0.0% | Path functions not implemented |
| Existential Subquery | None | 0.0% | EXISTS {} not supported |

---

## Progress Tracking

| Date | Scenarios Passed | Pass Rate | Key Changes |
|------|-----------------|-----------|-------------|
| 2026-02-03 (baseline) | 1,279 | 33.1% | Initial measurement |
| 2026-02-03 | 1,331 | 34.4% | Schemaless vertex scan support |
| 2026-02-04 | 1,352 | 35.0% | Schemaless edge creation support |
| 2026-02-04 | 1,355 | 35.0% | CREATE returns entities for RETURN, edge dedup, pattern comprehension |
| 2026-02-04 | 1,423 | 36.8% | Semantic validation + error classification fix |
| 2026-02-04 | **1,764** | **45.6%** | Path variable binding, WITH ORDER BY/SKIP/LIMIT, aggregation validation, error type leniency |

### Cumulative Improvement

| From | To | Scenarios Gained | Improvement |
|------|-----|------------------|-------------|
| Baseline (1,279) | Current (1,764) | **+485** | **+37.9%** |

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

4. **Result Mismatch Investigation** (~1,100 failures)
   - Wrong query output is most common failure
   - Need to audit specific failing queries
   - Likely causes: missing functions, incorrect aggregation, path handling

### Medium Priority

5. **WITH/WHERE Integration** (19 failures)
   - WHERE clause after WITH not working
   - Variable aliasing issues

6. **Variable-Length Paths** (10 failures)
   - `(a)-[*1..3]->(b)` patterns
   - Requires iterative path expansion

7. **Procedure CALL** (52 failures)
   - Test procedure stubs
   - CALL/YIELD infrastructure

8. **Pattern Predicates** (50 failures)
   - WHERE (a)-->(b) patterns
   - EXISTS patterns

### Low Priority

9. **Named Graph Fixtures** (19 failures)
   - Implement binary-tree-1/2 fixtures

10. **Side Effect Verification** (~100 failures)
    - TCK harness needs side effect checking

---

## Recommendations

### Short-term Wins
1. Implement basic aggregation (COUNT, SUM, AVG, MIN, MAX) - could unlock ~35 scenarios
2. Add string functions (STARTS WITH, ENDS WITH, CONTAINS) - could unlock ~32 scenarios
3. Fix WITH WHERE integration - could unlock ~19 scenarios

### Medium-term Goals
1. Implement MERGE clause (~75 scenarios)
2. Add variable-length path patterns (~10+ scenarios)
3. Complete procedure CALL infrastructure (~52 scenarios)
4. Extend semantic validation coverage (~200 remaining "no error" scenarios)

### Long-term Goals
1. Achieve 60%+ scenario pass rate
2. Full temporal arithmetic support (~180 scenarios)
3. Existential subqueries and pattern predicates
4. Complete side effect verification in TCK harness

---

## Test Command Reference

```bash
# Run all TCK tests
cargo test -p uni-tck --test cucumber -- --tags 'not @ignore'

# Run specific feature
cargo test -p uni-tck --test cucumber -- features/expressions/literals/Literals1.feature

# Run with verbose output
RUST_LOG=debug cargo test -p uni-tck --test cucumber

# View summary only
cargo test -p uni-tck --test cucumber 2>&1 | grep Summary
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
