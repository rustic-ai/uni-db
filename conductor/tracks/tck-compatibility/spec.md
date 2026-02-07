# TCK Compatibility Track

## Goal
Achieve 100% compliance with the openCypher Technology Compatibility Kit (TCK) by systematically addressing failures layer by layer.

## Current Status (Feb 2026)
- **Total Features:** 192
- **Passed:** 2490 (63.9%)
- **Failed:** 1368
- **Critical Gaps:** Aggregations, Variable Length Match, Merge, Temporal Parsing, List/Map Literals.

## Layered Strategy

### Layer 1: Foundations (Type System & Literals)
**Objective:** Ensure all data types and basic expressions work correctly.
- **Literals:** Fix nested Lists and Maps.
- **Type Conversion:** Fix `toInteger`, `toFloat`, `toBoolean` behavior.
- **Comparisons:** Fix range comparisons (>, <, >=, <=) and combined comparisons.
- **Boolean Logic:** Fix `NOT` operator semantics.

### Layer 2: Read Operations (Match & Projection)
**Objective:** Ensure we can query and retrieve data correctly.
- **Variable Length Match:** Implement correct semantics for `MATCH (a)-[*1..5]->(b)`.
- **Aggregations:** Implement `COUNT`, `MIN`, `MAX`, `SUM`, `COLLECT`, `DISTINCT`.
- **Projections:** Fix `RETURN` expressions, aliasing, and implicit grouping.
- **Ordering & Pagination:** Fix `ORDER BY`, `SKIP`, `LIMIT` interactions.

### Layer 3: Write Operations (CRUD)
**Objective:** Ensure data modification is correct and durable.
- **Create:** Fix node/relationship creation and side effects.
- **Delete:** Fix `DETACH DELETE` and interactions with connected nodes.
- **Set/Remove:** Fix property/label updates.
- **Merge:** Implement full `MERGE` semantics (On Create/On Match).

### Layer 4: Functions & Expressions
**Objective:** Support standard Cypher functions.
- **List Functions:** `range()`, `size()`, slicing `[1..3]`, comprehensions.
- **String Functions:** `substring()`, `contains`, `starts/ends with`.
- **Map Functions:** `keys()`, dynamic access `map['key']`.
- **Path Functions:** `nodes()`, `relationships()`, `length()`.

### Layer 5: Temporal & Advanced
**Objective:** Support advanced data types and patterns.
- **Temporal:** Parsing from strings, duration arithmetic, timezone handling.
- **Pattern Comprehensions:** `[ (a)-->(b) | b.name ]`.
- **Quantifiers:** Invariants for `ALL`, `ANY`, `NONE`, `SINGLE`.

## Success Criteria
- **TCK Pass Rate:** > 95% (allowing for specific documented exclusions).
- **No Regressions:** Existing passing tests must remain green.
- **Performance:** TCK compliance should not regress query performance (checked via benchmarks).
