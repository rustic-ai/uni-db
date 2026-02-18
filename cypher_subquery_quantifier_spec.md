# Cypher Subquery Expressions & Quantifier Functions: Specification & Design

---

## 1. Feature Inventory

This document covers four interrelated features that share a common execution model — evaluating an inner expression/query in the context of each outer row:

| Feature | Syntax | Returns | Spec Source |
|---------|--------|---------|-------------|
| EXISTS subquery | `EXISTS { MATCH ... }` | Boolean | CIP2015-05-13 |
| COUNT subquery | `COUNT { MATCH ... }` | Integer | Neo4j 5.0+ (not in openCypher 9) |
| COLLECT subquery | `COLLECT { MATCH ... RETURN expr }` | List | Neo4j 5.6+ (not in openCypher 9) |
| Pattern predicate | `(n)-[:REL]->(:Label)` in WHERE | Boolean | openCypher 9 (shorthand for EXISTS) |
| `all(x IN list WHERE pred)` | Function | Boolean | openCypher 9 |
| `any(x IN list WHERE pred)` | Function | Boolean | openCypher 9 |
| `none(x IN list WHERE pred)` | Function | Boolean | openCypher 9 |
| `single(x IN list WHERE pred)` | Function | Boolean | openCypher 9 |

**Note on openCypher 9 status:** The list quantifier functions (`all`, `any`, `none`, `single`) are explicitly **excluded** from the openCypher 9 standard (CIP2017-10-17 notes they "leverage a function-like syntax" that doesn't align with GQL direction). However, they remain widely implemented (Neo4j, Memgraph, etc.) and many TCK tests reference them. COUNT and COLLECT subqueries are Neo4j extensions not yet in openCypher.

---

## 2. Existential Subqueries

### 2.1 Syntax Forms

Per CIP2015-05-13, there are three syntactic forms, all equivalent semantically:

```
// Form 1: Pattern predicate (shorthand)
WHERE (n)-[:KNOWS]->(:Person)

// Form 2: Simple existential subquery
WHERE EXISTS { (n)-[:KNOWS]->(:Person {name: 'Alice'}) }
WHERE EXISTS { MATCH (n)-[:KNOWS]->(m) WHERE m.age > 30 }

// Form 3: Full existential subquery
WHERE EXISTS {
  MATCH (n)-[:KNOWS]->(m)-[:WORKS_AT]->(c:Company)
  WHERE c.name STARTS WITH 'Neo'
  RETURN m
}
```

The CIP defines Forms 1 and 2 as syntactic sugar for Form 3:

**Pattern predicate** `PP` desugars to:
```
EXISTS { MATCH PP }
```

**Simple existential** `EXISTS { pattern WHERE pred }` desugars to:
```
EXISTS { MATCH pattern WHERE pred RETURN * }
```

### 2.2 Evaluation Semantics

The CIP defines evaluation precisely:

1. Let `OUTER_VARIABLES` be the current working record (the row from the outer query)
2. Let `NESTED_QUERY` be the `RegularQuery` inside `EXISTS { ... }`
3. Let `RESULT_TABLE` = evaluate `NESTED_QUERY` with `OUTER_VARIABLES` as the driving table
4. If `RESULT_TABLE` has zero rows → **false**
5. If `RESULT_TABLE` has one or more rows → **true**

**Critical semantics:**
- The subquery is **correlated**: outer variables are visible inside the subquery without explicit import
- New variables introduced inside the subquery are **not** visible outside
- Variables introduced inside must have names **different** from outer variables
- The RETURN clause in a full subquery is optional (its output is discarded — only cardinality matters)
- EXISTS never returns null — it always returns true or false

### 2.3 COUNT Subquery Semantics

COUNT subqueries follow the same pattern but return the row count:

```cypher
MATCH (p:Person)
WHERE COUNT { (p)-[:HAS_DOG]->(:Dog) } > 1
RETURN p.name
```

Evaluation:
1. For each outer row `p`, execute the inner pattern
2. Count the result rows
3. Return the count as an integer expression

COUNT also never returns null — an empty result gives 0.

**The inner MATCH keyword is optional** when the subquery is just a pattern + optional WHERE:
```cypher
COUNT { (p)-[:HAS_DOG]->(:Dog) }             -- implicit MATCH
COUNT { MATCH (p)-[:HAS_DOG]->(d) WHERE d.age > 5 }  -- explicit MATCH
```

### 2.4 COLLECT Subquery Semantics

COLLECT aggregates inner results into a list:

```cypher
MATCH (p:Person)
RETURN p.name,
       COLLECT { MATCH (p)-[:OWNS]->(c:Car) RETURN c.model ORDER BY c.year } AS cars
```

Unlike EXISTS and COUNT:
- RETURN is **mandatory** (must return exactly one column)
- Supports ORDER BY, LIMIT, SKIP, DISTINCT inside
- Returns an empty list `[]` if no inner rows match (never null)

### 2.5 Variable Scoping Rules

```
OUTER SCOPE                    INNER SCOPE
─────────────                  ──────────────
Variables from                 Can READ outer variables
MATCH, WITH, etc.              without explicit import
                               (unlike CALL {} which
                               requires WITH import)

                               Can INTRODUCE new variables
                               but names must not shadow
                               outer variables

                               New variables are NOT
                               visible after the subquery
```

This is the key difference from `CALL {}` subqueries, which require explicit variable import:
```cypher
// EXISTS: implicit variable access
WHERE EXISTS { MATCH (n)-[:KNOWS]->(m) }   -- n is from outer scope

// CALL: requires explicit import
CALL { WITH n MATCH (n)-[:KNOWS]->(m) RETURN m }
```

---

## 3. List Quantifier Functions

### 3.1 Syntax

```
all(variable IN list WHERE predicate)
any(variable IN list WHERE predicate)
none(variable IN list WHERE predicate)
single(variable IN list WHERE predicate)
```

Each binds `variable` to successive elements of `list` and evaluates `predicate`.

### 3.2 Formal Semantics

Given `Q(x IN list WHERE P(x))` where Q is a quantifier:

**`all(x IN list WHERE P(x))`**
- Returns `true` if P(x) is true for **every** element
- Returns `true` for empty list (vacuous truth)
- Returns `null` if P(x) is null for any element AND no element yields false
- Returns `false` if P(x) is false for any element

**`any(x IN list WHERE P(x))`**
- Returns `true` if P(x) is true for **at least one** element
- Returns `false` for empty list
- Returns `null` if P(x) is null for any element AND no element yields true
- Returns `false` if P(x) is false for all elements (no nulls)

**`none(x IN list WHERE P(x))`**
- Returns `true` if P(x) is false for **every** element (equivalently: NOT any(...))
- Returns `true` for empty list
- Returns `null` if any P(x) is null and no P(x) is true
- Returns `false` if P(x) is true for any element

**`single(x IN list WHERE P(x))`**
- Returns `true` if P(x) is true for **exactly one** element
- Returns `false` for empty list
- Returns `null` if the count of true elements is 0 or 1 and any P(x) is null (can't be sure)
- Returns `false` if P(x) is true for 2 or more elements

### 3.3 Three-Valued Logic Truth Tables

The null handling follows Cypher's three-valued logic. The quantifiers can be understood as aggregations over the predicate results:

```
Let T = count of elements where P(x) = true
Let F = count of elements where P(x) = false
Let N = count of elements where P(x) = null

all:    if F > 0         → false
        else if N > 0    → null
        else             → true     (T ≥ 0, F = 0, N = 0)

any:    if T > 0         → true
        else if N > 0    → null
        else             → false    (T = 0, F ≥ 0, N = 0)

none:   if T > 0         → false
        else if N > 0    → null
        else             → true     (T = 0, F ≥ 0, N = 0)

single: if T > 1         → false
        else if T = 1 and N > 0  → null   (might be another true hiding in nulls)
        else if T = 1 and N = 0  → true
        else if T = 0 and N > 0  → null   (a null might be the one true)
        else                     → false   (T = 0, N = 0)
```

### 3.4 Null List Input

If the list itself is null (not an element being null, but the list expression evaluating to null):
- `all(x IN null WHERE ...)` → `null`
- `any(x IN null WHERE ...)` → `null`
- `none(x IN null WHERE ...)` → `null`
- `single(x IN null WHERE ...)` → `null`

This follows Cypher's general "null in, null out" function semantics.

### 3.5 Equivalences and Rewrite Rules

```
none(x IN L WHERE P(x))   ≡  NOT any(x IN L WHERE P(x))
                           ≡  all(x IN L WHERE NOT P(x))

all(x IN L WHERE P(x))    ≡  none(x IN L WHERE NOT P(x))
                           ≡  NOT any(x IN L WHERE NOT P(x))

-- For implementations without native quantifier support:
any(x IN L WHERE P(x))    ≡  size([x IN L WHERE P(x)]) >= 1
none(x IN L WHERE P(x))   ≡  size([x IN L WHERE P(x)]) = 0
all(x IN L WHERE P(x))    ≡  size([x IN L WHERE P(x)]) = size(L)

-- WARNING: The size-based rewrites lose null semantics!
-- size([x IN [1, null] WHERE x > 0]) = 1 (null element filtered out)
-- any(x IN [1, null] WHERE x > 0) = true (correct, same result)
-- BUT:
-- size([x IN [null] WHERE x > 0]) = 0
-- any(x IN [null] WHERE x > 0) = null (DIFFERENT from size = 0 → false)
```

The size-based rewrite (as recommended by Amazon Neptune) is **incorrect** for null handling. It converts null results to false. This may be acceptable for implementations that don't need strict null semantics, but it's not spec-compliant.

---

## 4. Nested Query Context: Execution Model

### 4.1 Correlated Execution Model

Both subquery expressions and quantifier functions share the same fundamental execution pattern: **for each outer row, evaluate an inner expression/query in a context that includes the outer row's bindings.**

```
OUTER QUERY produces rows: [r1, r2, r3, ...]

For each row ri:
  1. Bind outer variables from ri into inner context
  2. Execute inner query/expression
  3. Aggregate inner results → single value (bool, int, or list)
  4. Attach aggregated value to ri as a new column
```

This is conceptually a **correlated lateral join** followed by aggregation.

### 4.2 DataFusion Mapping

DataFusion has native support for correlated subqueries through three expression types:

| Cypher Feature | DataFusion Expr | Optimizer Rewrite |
|---------------|----------------|-------------------|
| EXISTS { ... } | `Expr::Exists(Subquery)` | `DecorrelatePredicateSubquery` → LEFT SEMI JOIN |
| NOT EXISTS { ... } | `Expr::Exists(Subquery, negated=true)` | `DecorrelatePredicateSubquery` → LEFT ANTI JOIN |
| COUNT { ... } | `Expr::ScalarSubquery(Subquery)` | `ScalarSubqueryToJoin` → LEFT OUTER JOIN + COUNT |
| COLLECT { ... } | `Expr::ScalarSubquery(Subquery)` | `ScalarSubqueryToJoin` → LEFT OUTER JOIN + ARRAY_AGG |
| `x IN (SELECT ...)` | `Expr::InSubquery(Subquery)` | `DecorrelatePredicateSubquery` → LEFT SEMI JOIN |

**Optimization pipeline:**

DataFusion's optimizer decorrelates subqueries in two rules:
1. `DecorrelatePredicateSubquery` — handles EXISTS and IN subqueries by rewriting to semi/anti joins
2. `ScalarSubqueryToJoin` — handles scalar subqueries (COUNT, COLLECT) by rewriting to LEFT OUTER JOIN with aggregation

The key optimization is **decorrelation**: converting the per-row correlated execution into a join, which enables vectorized/parallel execution. Without decorrelation, each outer row requires a separate inner query execution — O(N × M) worst case.

### 4.3 Building the Subquery Logical Plan

For a Cypher `EXISTS { MATCH (n)-[:KNOWS]->(m) WHERE m.age > 30 }` where `n` is from outer scope:

1. **Parse** the inner query as a regular Cypher query
2. **Plan** the inner MATCH as a DataFusion logical plan (Scan → Filter → Project)
3. **Identify correlated references**: `n` is from outer scope. In the inner plan, references to `n` become `OuterReferenceColumn` expressions
4. **Wrap** the inner plan in `Expr::Exists(Subquery { subquery: inner_plan, outer_ref_columns: [n] })`
5. **Place** the EXISTS expression in the outer plan's Filter node

DataFusion's optimizer then rewrites:
```
Filter(EXISTS(Subquery(inner_plan)))    →    LeftSemiJoin(outer_plan, inner_plan, ON correlation_predicate)
```

### 4.4 Quantifier Functions as Custom Execution

The list quantifier functions (`all`, `any`, `none`, `single`) don't map cleanly to DataFusion's subquery model because they operate on **lists** (already-materialized in-memory collections), not on **table scans**. They're closer to UDFs than to subqueries.

**Implementation options:**

**Option A: Custom scalar UDFs with lambda evaluation**
Register `cypher_all`, `cypher_any`, `cypher_none`, `cypher_single` as UDFs that take a list column and a predicate expression. Problem: DataFusion UDFs don't natively support lambda/closure arguments. The predicate `WHERE x.age > 30` can't be passed as an argument to a standard UDF.

**Option B: Rewrite to list comprehension + aggregation**
```
all(x IN list WHERE P(x))
→ size(filter(list, x -> P(x))) = size(list) AND list IS NOT NULL

any(x IN list WHERE P(x))
→ size(filter(list, x -> P(x))) >= 1

none(x IN list WHERE P(x))
→ size(filter(list, x -> P(x))) = 0

single(x IN list WHERE P(x))
→ size(filter(list, x -> P(x))) = 1
```

**Problem with Option B:** Loses null semantics (see section 3.5). `any(x IN [null, 1] WHERE x > 5)` should be `null` (the null element might satisfy), but `size(filter(...))` returns 0, giving `false`.

**Option C: UNNEST + aggregation (correct null semantics)**
```sql
-- any(x IN list WHERE P(x))
SELECT CASE
  WHEN bool_or(P(x)) = true THEN true
  WHEN bool_or(P(x) IS NULL) THEN null
  ELSE false
END
FROM UNNEST(list) AS x
```

This preserves null semantics because `bool_or` propagates nulls correctly. But it requires an UNNEST (row expansion), which is expensive for large lists and changes the cardinality model.

**Option D: Imperative evaluation in a custom PhysicalExpr**
Implement `CypherAll`, `CypherAny`, `CypherNone`, `CypherSingle` as custom `PhysicalExpr` implementations that iterate over the Arrow ListArray directly, applying the predicate to each element and accumulating T/F/N counts. This is the most correct and efficient approach but requires writing custom Arrow kernel code.

---

## 5. Result Row Generation from Subqueries

### 5.1 Cardinality Rules

Subquery expressions are **scalar** — they produce exactly one value per outer row, regardless of how many inner rows match:

| Feature | Inner rows = 0 | Inner rows = 1 | Inner rows = N |
|---------|----------------|----------------|----------------|
| EXISTS | false | true | true |
| COUNT | 0 | 1 | N |
| COLLECT | [] | [val] | [val1, ..., valN] |
| all/any/none/single | (depends on list, not rows) | | |

This is critical: **subquery expressions never change the outer cardinality**. They are scalar expressions attached to each outer row, not joins that multiply rows.

### 5.2 EXISTS Short-Circuit

EXISTS can short-circuit: as soon as one inner row is found, it returns true without processing further rows. The optimizer exploits this:
- Semi join can stop scanning the inner table as soon as a match is found for each outer row
- For uncorrelated EXISTS, a single `LIMIT 1` suffices

### 5.3 COUNT and COLLECT Require Full Inner Evaluation

Unlike EXISTS, COUNT and COLLECT must process **all** inner rows:
- COUNT needs the total count
- COLLECT needs all values (plus ORDER BY, LIMIT, DISTINCT if specified)

After decorrelation, this becomes:
```
LeftOuterJoin(outer, inner, ON correlation)
  → GroupBy(outer_key, COUNT(*))           -- for COUNT
  → GroupBy(outer_key, ARRAY_AGG(expr))    -- for COLLECT
```

Outer rows with no inner matches get NULL from the LEFT OUTER JOIN, which must be coerced:
- COUNT: `COALESCE(count_result, 0)` — never null
- COLLECT: `COALESCE(array_result, [])` — never null, returns empty list

### 5.4 CALL {} Subquery: Different Cardinality Model

Unlike EXISTS/COUNT/COLLECT, CALL {} subqueries **do** affect cardinality:

```cypher
MATCH (p:Person)
CALL {
  WITH p
  MATCH (p)-[:KNOWS]->(f)
  RETURN f
}
RETURN p.name, f.name
```

If person has 3 friends, this produces 3 output rows. CALL {} is a lateral join, not a scalar expression. It multiplies the outer cardinality by the inner cardinality per row.

This distinction is fundamental:
- EXISTS/COUNT/COLLECT: scalar expressions → never change cardinality
- CALL {}: lateral join → multiplies cardinality
- Both use correlated execution, but the result aggregation differs

---

## 6. Interaction with Type Coercion

### 6.1 Subquery Expressions and Type Coercion

EXISTS returns Boolean — no coercion needed.
COUNT returns Integer — no coercion needed.
COLLECT returns a List — the element type depends on the inner RETURN expression.

The type coercion layer should **not** walk into subquery plans. Each subquery is planned independently with its own schema. The outer plan only sees the subquery's result type (Boolean, Int64, or List<T>).

### 6.2 Quantifier Functions and Type Coercion

The predicate inside `all(x IN list WHERE P(x))` may involve cross-type comparisons:

```cypher
all(x IN [1, '2', 3] WHERE x > 0)
```

Here `x` is bound to each element (Int, String, Int), and `x > 0` involves comparing String to Int for the second element. The type coercion layer needs to handle this, but the coercion happens **inside** the predicate evaluation, not at the quantifier level.

If implemented as Option D (custom PhysicalExpr), the predicate is evaluated per-element, and each element may have a different type from the LargeBinary/CypherValue column. The coercion logic from `build_cypher_comparison` applies here.

### 6.3 Pattern Predicates and Type Coercion

Pattern predicates in WHERE are boolean expressions. The type coercion layer doesn't need to do anything special — they're just another boolean predicate in the filter.

---

## 7. Implementation Audit Checklist

### EXISTS Subquery

| # | Check | Status |
|---|-------|--------|
| S1 | Pattern predicate `(n)-[:REL]->()` in WHERE works | [ ] |
| S2 | Simple EXISTS `EXISTS { (n)-[:REL]->() }` works | [ ] |
| S3 | EXISTS with WHERE `EXISTS { MATCH ... WHERE ... }` works | [ ] |
| S4 | Full EXISTS with RETURN works | [ ] |
| S5 | NOT EXISTS works | [ ] |
| S6 | EXISTS never returns null (only true/false) | [ ] |
| S7 | Inner variables not visible outside | [ ] |
| S8 | Inner variable names don't shadow outer names (error or correct scoping) | [ ] |
| S9 | Correlated references resolve correctly | [ ] |
| S10 | Nested EXISTS inside EXISTS works | [ ] |
| S11 | EXISTS in RETURN clause (not just WHERE) works | [ ] |

### COUNT Subquery

| # | Check | Status |
|---|-------|--------|
| C1 | Basic COUNT with pattern works | [ ] |
| C2 | COUNT with full MATCH ... WHERE works | [ ] |
| C3 | COUNT returns 0 for no matches (not null) | [ ] |
| C4 | COUNT usable in WHERE (`WHERE COUNT {...} > 1`) | [ ] |
| C5 | COUNT usable in RETURN | [ ] |
| C6 | COUNT usable in WITH | [ ] |
| C7 | Correlated references resolve correctly | [ ] |

### COLLECT Subquery

| # | Check | Status |
|---|-------|--------|
| L1 | Basic COLLECT with RETURN works | [ ] |
| L2 | COLLECT returns [] for no matches (not null) | [ ] |
| L3 | COLLECT with ORDER BY works | [ ] |
| L4 | COLLECT with LIMIT works | [ ] |
| L5 | COLLECT with DISTINCT works | [ ] |
| L6 | COLLECT RETURN must be exactly one column (error otherwise) | [ ] |

### Quantifier Functions

| # | Check | Status |
|---|-------|--------|
| Q1 | `all(x IN list WHERE P(x))` — basic case | [ ] |
| Q2 | `any(x IN list WHERE P(x))` — basic case | [ ] |
| Q3 | `none(x IN list WHERE P(x))` — basic case | [ ] |
| Q4 | `single(x IN list WHERE P(x))` — basic case | [ ] |
| Q5 | Empty list: all→true, any→false, none→true, single→false | [ ] |
| Q6 | Null list: all four return null | [ ] |
| Q7 | Null element, no true match: any→null (not false) | [ ] |
| Q8 | Null element, has true match: any→true (short-circuit) | [ ] |
| Q9 | Null element, no false: all→null (not true) | [ ] |
| Q10 | single with 2+ true: returns false regardless of nulls | [ ] |
| Q11 | single with 1 true + null: returns null (not true) | [ ] |
| Q12 | Predicate references outer scope variable | [ ] |
| Q13 | Nested quantifier: `all(x IN L WHERE any(y IN x WHERE ...))` | [ ] |

### Nested Context

| # | Check | Status |
|---|-------|--------|
| N1 | Outer variable visible in inner scope without import | [ ] |
| N2 | Inner-only variable not visible after subquery | [ ] |
| N3 | Subquery in WHERE doesn't change outer cardinality | [ ] |
| N4 | Subquery in RETURN doesn't change outer cardinality | [ ] |
| N5 | Multiple subqueries in same clause work independently | [ ] |
| N6 | Subquery inside CASE WHEN works | [ ] |
| N7 | Subquery nested inside another subquery works | [ ] |

---

## 8. Implementation Priority

### Tier 1: Required for Basic Correctness

1. **Pattern predicates** in WHERE — these are the most common form and likely already partially implemented as part of MATCH planning
2. **EXISTS { pattern }** — the simple form, which is the most common explicit subquery
3. **`any`/`all`/`none`/`single`** — widely used in TCK tests and real queries

### Tier 2: Required for Feature Completeness

4. **Full EXISTS { MATCH ... WHERE ... RETURN ... }** — full correlated subquery
5. **COUNT { pattern }** — needed for GQL conformance path
6. **NOT EXISTS** — straightforward once EXISTS works

### Tier 3: Extensions

7. **COLLECT { ... }** — Neo4j extension, lower priority
8. **CALL {} subqueries** — different execution model (lateral join), separate design

### Recommended Implementation Order

For the quantifier functions, start with Option B (size-based rewrite) as a quick-and-dirty implementation that passes most TCK tests, then upgrade to Option D (custom PhysicalExpr) when null-sensitivity matters. The size-based rewrite is 10 lines of code; the custom kernel is ~200 lines but handles nulls correctly.

For EXISTS, use DataFusion's native `Expr::Exists` and let the optimizer decorrelate to a semi join. The main work is in the Cypher planner: identifying correlated references, building the inner logical plan, and wrapping in the right Expr variant.

For COUNT, use `Expr::ScalarSubquery` wrapping an inner plan that projects `COUNT(*)`. The `ScalarSubqueryToJoin` optimizer rule handles decorrelation.
