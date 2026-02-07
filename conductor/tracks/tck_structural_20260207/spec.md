# Track Specification - TCK Structural Data & Dynamic Access

## Objective
Achieve compatibility with openCypher TCK for structural data types (Maps, Lists) and dynamic access patterns.

## Current State
- `Map2 - Dynamic Value Access`: 7% pass rate.
- `Map3 - Keys function`: 18% pass rate.
- `List1 - Dynamic Element Access`: 22% pass rate.
- `List12 - List Comprehension`: 14% pass rate.

## Key Gaps
1. **Dynamic Map Access**: `map[key_expression]` is not fully supported in the vectorized engine or fallback.
2. **Dynamic List Access**: `list[index_expression]` has issues with negative indices and out-of-bounds handling.
3. **Collection Functions**: `keys()`, `labels()`, `nodes()`, and `relationships()` need to return structural types consistently.
4. **List Comprehensions**: Mixed type support and scoping issues in the vectorized engine.

## Target Features
- [ ] Implement `MapIndex` expression in DataFusion planner using `get_field`.
- [ ] Implement `ListIndex` expression with Cypher-compliant bounds checking.
- [ ] Fix `keys()` UDF to support Nodes, Relationships, and Maps structural returns.
- [ ] Fix `labels()`, `nodes()`, `relationships()` to return structural lists.
- [ ] Standardize List Comprehension semantics.

## Success Criteria
- Pass rate for `Map2`, `Map3`, `List1`, `List12` features increases to >90%.
- All dynamic access scenarios in TCK pass.
