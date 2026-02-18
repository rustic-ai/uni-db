# Map Property Access Bug Impact Analysis

## Three Related Bugs Found

| Bug | Reproduction | Root Cause |
|-----|-------------|------------|
| **UNWIND map property access** | `UNWIND [{a:1}] AS m RETURN m.a` → `Null` | Property lookup fails on CypherValue-encoded maps from UNWIND |
| **Nested map property access** | `WITH {a:{b:'x'}} AS m RETURN m.a.b` → `Null` | Chained property access doesn't drill into nested maps |
| **coalesce(null, list)** | `RETURN coalesce(null, [2])` → `String("[2]")` | Null first arg causes list to serialize to string |

Note: Single-level `WITH {a:1} AS m RETURN m.a` works correctly (`Int(1)`). The bug is specific to UNWIND'd maps and chained access.

## Direct TCK Failures: 7 scenarios

| Feature | Scenario | Bug | Error |
|---------|----------|-----|-------|
| Quantifier11 | [3] @L109 (`x = 2`) | UNWIND-map + coalesce | Row count: expected 1, got 0 |
| Quantifier11 | [3] @L110 (`x % 2 = 0`) | UNWIND-map + coalesce | Row count: expected 1, got 0 |
| Quantifier11 | [3] @L111 (`x % 3 = 0`) | UNWIND-map + coalesce | Row count: expected 1, got 0 |
| Quantifier11 | [3] @L113 (`x >= 3`) | UNWIND-map + coalesce | Row count: expected 1, got 0 |
| Unwind1 | [6] Creating nodes from unwound params | UNWIND-map | Row count: expected 2, got 0 |
| With2 | [2] Forwarding nested map literal | Nested-map | `nestedMap.name.name2` is Null |
| Delete5 | [6] Delete from nested map/list | Nested-map | No result found |

## "Passing" but Silently Wrong: 21 scenarios

These TCK scenarios use the bug pattern but pass anyway because they test **mathematical tautologies** (equivalence identities) that hold for any list — even the wrong list of all-1s that the bug produces:

- **Quantifier9[5]** (5 examples) — `none(P) = (size(filter) = 0)`
- **Quantifier10[4]** (5 examples) — `single(P) = (size(filter) = 1)`
- **Quantifier11[6]** (5 examples) — `any(P) = (size(filter) > 0)`
- **Quantifier12[5]** (5 examples) — `all(P) = (size(filter) = size(list))`
- **Quantifier11[3] @L112** (`x < 7`) — passes because a list of all-1s satisfies `all(x < 7)`

## Diagnosis Detail

### Bug 1: UNWIND Map Property Access

```cypher
-- WORKS: single-level WITH map
WITH {a: 1} AS m RETURN m.a        -- → Int(1) ✓

-- BROKEN: UNWIND map
UNWIND [{a: 1}] AS m RETURN m.a    -- → Null ✗

-- BROKEN: all property types
UNWIND [{x: 1, y: 'hello', z: true}] AS m
RETURN m.x, m.y, m.z               -- → Null, Null, Null ✗
```

When a list of map literals is UNWIND'd, the resulting rows encode each map as a CypherValue (LargeBinary). Property access (`m.a`) on these CypherValue-encoded maps fails to decode and returns Null instead.

### Bug 2: Nested Map Property Access

```cypher
-- WORKS: single-level
WITH {name: 'test'} AS m RETURN m.name           -- → String("test") ✓

-- BROKEN: chained access
WITH {name: {name2: 'baz'}} AS m RETURN m.name.name2  -- → Null ✗
```

Chained property access (`m.a.b`) does not drill into nested map structures. The first access (`m.name`) returns the inner map, but the second access (`.name2`) on that result fails.

### Bug 3: coalesce(null, list)

```cypher
-- WORKS: both non-null
RETURN coalesce([2], [3])           -- → List([Int(2)]) ✓

-- BROKEN: null first argument
RETURN coalesce(null, [2])          -- → String("[2]") ✗
RETURN size(coalesce(null, [2]))    -- → Int(3) ✗ (treats "[2]" as 3-char string)

-- ALSO BROKEN: null second argument
RETURN coalesce([2], null)          -- → String("[2]") ✗
```

When either argument to coalesce is null, the non-null list argument is serialized to its string representation instead of being returned as a list value. This causes downstream failures when the result is used as a list (e.g., in quantifier expressions: "Quantifier input must be a list, got Utf8").

### Why Quantifier11[3] @L112 (`x < 7`) Passes

The UNWIND-map bug causes `input.fixed` and `input.list` to both return Null. This makes CASE expressions fall through to ELSE branches:
- `fixedList` = null (for all inputs, even those with `fixed: true`)
- `inputList` = `[1]` (the ELSE default)

After three rounds of UNWIND expansion, all lists contain only 1s. With predicate `x < 7`, `all(x IN list WHERE x < 7)` is true (since 1 < 7), so rows survive the WHERE filter. The other predicates (`x = 2`, `x % 2 = 0`, `x % 3 = 0`, `x >= 3`) all evaluate to false on 1, producing 0 surviving rows.

### Why Quantifier9/10/11/12 Equivalence Scenarios Pass

These test mathematical tautologies like `none(P) = (size([x IN L WHERE P | x]) = 0)`. Since the equivalence holds for ANY list (including the bug-produced list of all-1s), both sides evaluate to the same value and the equality check returns true. The tests pass but are exercising the wrong data.

## Summary

- **7 direct failures** out of 343 total (2.0%)
- **21 false passes** — tests pass but exercise wrong data
- Fixing the UNWIND map property access bug would resolve 5 failures and correct 21 false passes
- Fixing the nested map property access bug would resolve 2 failures
- Fixing the coalesce bug would prevent downstream type errors when combined with the UNWIND bug
