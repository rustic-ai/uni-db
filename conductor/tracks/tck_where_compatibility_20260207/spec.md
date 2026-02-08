# Specification - TCK WHERE Compatibility

## Goal
Achieve full TCK (Technology Compatibility Kit) compliance for `WHERE` clause expressions in Uni. This track focuses on fixing existing failures and implementing missing logic in the query planner and executor, utilizing the newly migrated `uni_cypher::ast`.

## Scope
The track follows a **Depth-First** strategy, prioritizing categories in the following order:

1.  **Category A: Basic Comparisons & Ranges**
    *   Operators: `>`, `<`, `>=`, `<=`
    *   Scenarios: Value comparison, range filters, property comparisons.
2.  **Category B: Boolean Logic & Precedence**
    *   Operators: `AND`, `OR`, `NOT`, `XOR`
    *   Scenarios: Complex predicates, nested logic, operator precedence rules.
3.  **Category C: Null Handling**
    *   Operators: `IS NULL`, `IS NOT NULL`
    *   Scenarios: Property existence checks, handling nulls in comparisons (3-valued logic).
4.  **Category D: String Matching**
    *   Operators: `STARTS WITH`, `ENDS WITH`, `CONTAINS`
    *   Scenarios: Substring searches, case sensitivity (as per TCK), null handling in strings.

## Implementation Strategy
- **AST Transition**: Use `uni_cypher::ast` definitions exclusively.
- **Planner Updates**: Ensure `df_planner.rs` and `planner.rs` correctly translate Cypher expressions to DataFusion expressions or internal logical operators.
- **Executor Updates**: Refine `expr_eval.rs` and vectorized operators to handle edge cases defined in TCK features (e.g., heterogeneous type comparisons, overflow, null propagation).
- **TDD Flow**: Each phase starts by identifying failing TCK scenarios, creating a reproduction test, and then implementing the fix.

## Success Criteria
- [ ] 100% Pass rate for TCK features in `tck/features/expressions/comparison`
- [ ] 100% Pass rate for TCK features in `tck/features/expressions/boolean`
- [ ] 100% Pass rate for TCK features in `tck/features/expressions/null`
- [ ] 100% Pass rate for TCK features in `tck/features/expressions/string`
- [ ] 100% Pass rate for TCK features in `tck/features/clauses/match-where` and `tck/features/clauses/with-where`
- [ ] No regressions in existing integration tests.
