# Match Clause TCK Failures Analysis

**Date:** 2026-02-06
**Total Failed Match Scenarios:** 82 out of all Match-related tests

## Executive Summary

The Match clause features show **82 failed scenarios** across 10 different feature files. The failures fall into several distinct categories, with the most common being:

1. **Row count mismatches** (25 failures) - Incorrect number of results returned
2. **Missing results** (20 failures) - Expected results not found
3. **Type/value mismatches** (16 failures) - Wrong data types or values in results
4. **Missing error validation** (5 failures) - Tests expect errors but queries succeed
5. **Parser issues** (3 failures) - Query parsing failures
6. **Variable type conflicts** (1 failure) - Variable redefinition issues

## Failure Breakdown by Feature

| Feature | Failed Scenarios | Primary Issues |
|---------|-----------------|----------------|
| **Match6 - Named paths** | 19 | Path structure format, relationship direction, variable-length paths |
| **Match7 - Optional match** | 12 | Bound node handling, row count mismatches, null handling |
| **Match3 - Fixed length patterns** | 10 | Multi-label matching, undirected patterns, self-relationships |
| **Match4 - Variable length patterns** | 9 | Variable-length pattern matching, property predicates, list handling |
| **CountingSubgraphMatches1** | 11 | Subgraph counting logic |
| **Match9 - Deprecated** | 8 | Legacy pattern support |
| **Match2 - Relationships** | 7 | Label predicates, multiple types, inline properties |
| **Match1 - Nodes** | 3 | Multiple labels, Cartesian products, parameter validation |
| **Match8 - Interoperation** | 2 | Clause ordering and interaction |
| **MatchWhere6 - Filter optional** | 1 | Optional match filtering |

## Detailed Root Cause Analysis

### 1. **Named Path Structure Issues (Match6 - 19 failures)**

**Problem:** Path objects are being returned with incorrect structure or missing components.

**Symptoms:**
- Expected: Path as a structured object with `nodes` and `relationships` lists
- Actual: Getting individual nodes/edges or incorrectly formatted paths
- Direction handling is incorrect for bidirectional patterns

**Example Failures:**
```
Line 49: [2] Return a simple path
Error: No match found for actual row 0.
Actual: [("p", Some(Map({"nodes": List([Map({"proper...
Expected: Path object with specific structure

Line 147: [8] Respecting direction when matching non-existent path with multiple directions
Error: Row count mismatch: expected 0, got 2
(Returns results when path should not match)

Line 180: [10] Named path with alternating directed/undirected relationships
Error: Row count mismatch: expected 1, got 3
(Undirected patterns matching multiple times)
```

**Root Cause:**
- Path object serialization/representation doesn't match TCK expectations
- Bidirectional (`-[]-` and `-[]->` mixed) patterns are over-matching
- Undirected relationship semantics differ from TCK expectations

---

### 2. **Row Count Mismatches (25 failures across features)**

**Problem:** Queries return different number of rows than expected, usually MORE rows.

**Common Patterns:**
- **Undirected patterns over-matching:** Patterns with `-[]-` match both directions when they should match once
- **Cartesian product issues:** Multiple MATCH clauses producing incorrect combinations
- **Label intersection problems:** Multi-label nodes (`X:Y`) matching incorrectly
- **Self-relationship handling:** Cycles and self-loops producing extra matches

**Example Failures:**
```
Match1, Line 62: [3] Matching nodes using multiple labels
Error: Row count mismatch: expected 2, got 4
(Multi-label nodes matching incorrectly)

Match3, Line 287: [16] Mixing directed and undirected pattern parts
Error: Row count mismatch: expected 6, got 11
(Undirected patterns over-matching)

Match7, Line 63: [3] OPTIONAL MATCH and bound nodes
Error: Row count mismatch: expected 1, got 4
(Optional match producing too many rows)
```

**Root Causes:**
- Undirected relationship matching treats each relationship as two matches (A→B and B→A)
- Multi-label matching logic doesn't properly intersect label requirements
- Optional match semantics differ from Cypher spec (producing Cartesian product instead of left join)

---

### 3. **Type and Value Mismatches (16 failures)**

**Problem:** Query results contain correct structure but wrong data types or values.

**Symptoms:**
- Properties returned as **String instead of Int**
- Node labels missing (empty string `""` instead of `"X:Y"`)
- Edge/node IDs incorrect
- Relationships returned as single Edge instead of List[Edge]

**Example Failures:**
```
Match1, Line 97: [5] Use multiple MATCH clauses to do a Cartesian product
Actual: [("m", Some(String("1"))), ("n", Some(String("1")))]
Expected: [{"m": Int(1), "n": Int(1)}, ...]
(Properties returned as strings instead of integers)

Match3, Line 508: [26] Matching twice with a duplicate predicate
Actual: Node { vid: Vid(0), label: "X", properties: {} }
Expected: Node { vid: Vid(0), label: "X:Y", properties: {} }
(Multi-label node shows only first label)

Match4, Line 33: [1] Handling fixed-length variable length pattern
Actual: [("r", Some(Edge(...)))]
Expected: [{"r": List([Edge(...)])}]
(Single edge instead of list for variable-length pattern)
```

**Root Causes:**
- Property type coercion: Properties loaded from Lance are being converted to strings
- Multi-label serialization: Only first label is being serialized to result
- Variable-length pattern handling: Not wrapping single-hop results in a list

---

### 4. **Missing Results (20 failures)**

**Problem:** Queries return no results when they should return data.

**Common Scenarios:**
- Variable-length patterns with property predicates
- Multiple relationship types (`[:T1|T2]`)
- Named paths with specific bounds
- Zero-length variable-length patterns

**Example Failures:**
```
Match2, Line 113: [6] Match relationships with multiple types
Error: No result found
(Pattern with multiple edge types fails)

Match4, Line 71: [3] Zero-length variable length pattern
Error: No result found
(Zero-length pattern not matching)

Match6, Line 33: [1] Zero-length named path
Error: No result found
(Named path with zero edges not matching)
```

**Root Causes:**
- Multi-type relationship matching (`[:TYPE1|TYPE2]`) not implemented
- Variable-length patterns with min bound of 0 not handled correctly
- Property predicates on variable-length paths not being evaluated

---

### 5. **Missing Error Validation (5 failures)**

**Problem:** Tests expect queries to fail with specific errors, but they succeed or fail with wrong error.

**Example Failures:**
```
Match1, Line 123: [6] Fail when using parameter as node predicate in MATCH
Expected error: 'InvalidParameterUse'
Actual: 'Query error: Node properties must be a Map'

Match2, Line 152: [8] Fail when using parameter as relationship predicate
Expected: Error
Actual: No error found (query succeeds)

Match3, Line 556: [29] Fail when re-using a relationship in the same pattern
Expected: Error
Actual: No error found (query succeeds)
```

**Root Causes:**
- Parameter validation not strict enough (allows node/edge inline predicates with parameters)
- Variable reuse validation missing (same relationship variable used twice in pattern)
- Error messages don't match TCK expected error types

---

### 6. **Parser and Variable Conflicts (4 failures)**

**Problem:** Query parsing fails or variables conflict.

**Example Failures:**
```
Match4, Line 93: [4] Matching longer variable length paths
Error: VariableTypeConflict - Variable 'n1' already defined as Scalar, cannot use as Node

Match4, Line 198: [9] Fail when asterisk operator is missing
Expected: 'InvalidRelationshipPattern'
Actual: 'Parse error: --> 3...'
(Error message format doesn't match)
```

**Root Causes:**
- Variable type checking too strict (scalar variable name reused in later clause)
- Parser error messages don't match TCK expected format/error codes

---

## Critical Issues Requiring Fixes

### Priority 1: High Impact (affecting 30+ scenarios)

1. **Undirected Relationship Semantics** (affects Match3, Match6, Match7)
   - Current: Undirected patterns match both directions separately
   - Expected: Undirected patterns should match once regardless of storage direction
   - Impact: ~15 scenarios

2. **Named Path Object Structure** (affects Match6)
   - Current: Paths returned with incorrect serialization format
   - Expected: Path object with `nodes` and `relationships` arrays
   - Impact: ~19 scenarios

3. **Optional Match Row Multiplication** (affects Match7)
   - Current: OPTIONAL MATCH produces Cartesian product with bound nodes
   - Expected: OPTIONAL MATCH should act like LEFT JOIN, producing NULLs
   - Impact: ~12 scenarios

### Priority 2: Medium Impact (affecting 10-20 scenarios)

4. **Multi-Label Node Matching** (affects Match1, Match3)
   - Current: Multi-label patterns not correctly intersecting
   - Expected: Node must have ALL specified labels
   - Impact: ~5 scenarios

5. **Property Type Preservation** (affects Match1, multiple features)
   - Current: Properties being serialized as strings
   - Expected: Properties maintain their Arrow/Lance types (Int, Float, etc.)
   - Impact: ~8 scenarios

6. **Variable-Length Pattern Edge Cases** (affects Match4, Match6)
   - Current: Zero-length and fixed-length variable patterns failing
   - Expected: `[*0]`, `[*1..1]` should work
   - Impact: ~9 scenarios

### Priority 3: Lower Impact (affecting <10 scenarios)

7. **Multiple Relationship Types** (affects Match2)
   - Current: `[:TYPE1|TYPE2]` syntax not supported
   - Expected: Should match any of the specified types
   - Impact: ~3 scenarios

8. **Parameter Validation** (affects Match1, Match2)
   - Current: Allows parameters in inline predicates
   - Expected: Should reject with `InvalidParameterUse` error
   - Impact: ~2 scenarios

9. **Variable Reuse Detection** (affects Match2, Match3)
   - Current: Allows same relationship variable in multiple places
   - Expected: Should fail with appropriate error
   - Impact: ~2 scenarios

---

## Recommended Fix Order

### Phase 1: Foundation Fixes (Week 1-2)
1. Fix property type preservation (convert from Lance types to correct Cypher types)
2. Fix multi-label node matching logic
3. Add parameter validation for inline predicates

### Phase 2: Pattern Matching Core (Week 3-4)
4. Fix undirected relationship semantics
5. Implement variable-length pattern edge cases (zero-length, fixed-length)
6. Add multi-type relationship matching

### Phase 3: Advanced Features (Week 5-6)
7. Fix OPTIONAL MATCH row multiplication issue
8. Implement correct named path object structure
9. Add variable reuse validation

### Phase 4: Polish (Week 7)
10. Align error messages with TCK expectations
11. Fix remaining edge cases
12. Validation and regression testing

---

## Testing Strategy

For each fix:
1. Run specific feature file: `cargo nextest run -E 'test(Match1)'`
2. Verify fix doesn't break passing tests
3. Check compatibility report: `target/cucumber/compatibility_report.json`
4. Target: >95% pass rate on all Match features

---

## Files to Review

Key implementation files based on error locations:

- `crates/uni-tck/src/steps/then.rs:17` - Result matching and comparison logic
- `crates/uni-tck/src/steps/then.rs:39` - Error matching and validation
- `crates/uni-tck/src/steps/given.rs:32` - Setup query execution
- `crates/uni-tck/src/matcher/result.rs` - Result comparison (type coercion likely here)
- `crates/uni-tck/src/parser/value.rs` - Value parsing and type handling

Query execution path (likely issues):
- `crates/uni-query/src/executor/` - Pattern matching logic
- `crates/uni-query/src/planner/` - Query planning for MATCH clauses
- `crates/uni-runtime/src/working_graph.rs` - Graph traversal semantics

---

## Success Metrics

Current state: **82 failed Match scenarios**

Target milestones:
- Phase 1 complete: <60 failures (~25% improvement)
- Phase 2 complete: <30 failures (~60% improvement)
- Phase 3 complete: <10 failures (~90% improvement)
- Phase 4 complete: <5 failures (~95%+ pass rate)

---

## Appendix: Failure Category Distribution

```
Failure Type                     Count    % of Total
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Row count mismatch                 25        30%
No result found                    20        24%
Type/value mismatch                16        20%
No error found (validation)         5         6%
Parse error                         3         4%
Variable conflicts                  1         1%
Other/Mixed                        12        15%
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
TOTAL                              82       100%
```
