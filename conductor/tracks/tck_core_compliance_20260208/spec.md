# Track Specification - TCK Core Compliance

## Objective
Finalize core foundations by resolving high-value TCK failures related to standard clauses (`ORDER BY`) and foundational expressions (String functions, Temporal types). This targets ~50% of the remaining failures.

## Scope

### 1. Ordering (`ORDER BY`)
- **Current State**: 191 failures (14.1% of total).
- **Issues**:
    - Wrong sort order for mixed types.
    - Incorrect handling of `NULL` (Cypher: `NULL` is larger than everything? No, `NULL` is usually last or first depending on direction, but specific to implementation. Neo4j: `NULL` is last in ASC).
    - Expression evaluation in `ORDER BY` clause.
- **Goal**: Implement Cypher-compliant comparator and fix `ORDER BY` pipeline.

### 2. String Functions
- **Current State**: 31 failures (2.3%).
- **Issues**: Missing implementation for `STARTS WITH`, `ENDS WITH`, `CONTAINS`, `substring`, `toLower`, `toUpper`.
- **Goal**: Implement these functions in `CypherPhysicalExprCompiler` and `DataFusion` UDFs.

### 3. Temporal Types
- **Current State**: 376 failures (27.8%).
- **Issues**:
    - Formatting mismatch (redundant `:00` seconds).
    - Timezone normalization (`+00:00` vs `Z`).
    - Basic construction/parsing.
- **Goal**: Fix formatting and basic parsing to clear the bulk of "noise" failures.

## Success Criteria
- `WithOrderBy` pass rate > 90%.
- String function tests pass.
- Temporal tests pass rate significantly improved (reduce 376 failures by >50%).
