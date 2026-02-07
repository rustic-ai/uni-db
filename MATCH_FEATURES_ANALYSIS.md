# Match Features Failure Analysis

**Generated:** 2026-02-05
**Source:** TCK JSON results (3,869 scenarios)
**Scope:** All Match and Match-Where features (386 scenarios)

---

## Executive Summary

**Overall Performance:**
- **Pass Rate:** 63.2% (244/386 scenarios)
- **Failed:** 142 scenarios (36.8%)

**Feature Categories:**
- ✅ **Strong** (>75%): Match1 (nodes), Match2 (relationships), Match6 (named paths)
- ⚠️ **Partial** (25-75%): Match7 (optional), MatchWhere1 (single variable filter)
- ❌ **Weak** (<25%): All other Match-Where features, variable-length paths

---

## Failure Breakdown by Type

### 1. Wrong Row Count: 51 cases (35.9%)

**Primary Issue:** WHERE clause filtering broken - returns 0 rows when data should match

**Sub-patterns:**
- Expected data but got 0 rows: **46 cases** (90% of this category)
- Got more rows than expected: 5 cases

**Affected Features:**
- MatchWhere1-6: WHERE filtering not working
- Match2: Relationship type filtering issues
- Match3: Multi-hop pattern filtering

**Example:**
```cypher
MATCH (a)-[:KNOWS]->(b) WHERE b.name = 'Bob'
-- Expected: 1 row
-- Got: 0 rows
-- Issue: WHERE predicate not filtering correctly
```

**Root Cause:** WHERE clause execution in query engine

---

### 2. Wrong Data Format: 23 cases (16.2%)

**Primary Issue:** Returning Map with internal fields instead of Node/Relationship objects

**Sub-patterns:**
- Internal fields exposed (_vid, _label, _eid): **14 cases**
- Type conversion errors (String vs Int): 9 cases

**Example:**
```cypher
MATCH (n) RETURN n
-- Expected: Node(vid=0, label="A", properties={})
-- Got: Map({"_vid": 0, "_label": "A"})
-- Issue: Internal representation leaked to user
```

**Root Cause:** Result serialization/formatting layer

---

### 3. Empty Results: 32 cases (22.5%)

**Primary Issue:** Queries return nothing when they should return data

**Breakdown by Feature:**
- Match6 (named paths): 12 cases - Path variable binding issues
- Match9 (deprecated): 6 cases - Legacy syntax support
- Match4 (variable-length): 4 cases - [*1..3] patterns not working
- Match7 (optional): 3 cases - NULL not returned for missing data
- MatchWhere6: 3 cases - WHERE on optional match

**Example:**
```cypher
MATCH p = (a)-[*1..3]->(b) RETURN p
-- Expected: Multiple path results
-- Got: Empty result
-- Issue: Variable-length path expansion not implemented
```

**Root Cause:** Graph traversal/path expansion logic

---

### 4. Wrong Error Type: 17 cases (12.0%)

**Issue:** Validation errors have wrong classification

**Examples:**
- Expected: `InvalidParameterUse`
- Got: `Query error: Node properties must be a Map`

**Root Cause:** Error type mapping in semantic validation

---

### 5. Test Parsing Error: 12 cases (8.5%)

**Issue:** TCK test harness cannot parse expected results

**Example:**
```
Failed to parse expected table: "Parse error: (:A:B)"
```

**Root Cause:** Test infrastructure - multiple node labels notation

---

### 6. Missing Error: 6 cases (4.2%)

**Issue:** Query succeeds when it should raise validation error

**Root Cause:** Insufficient semantic validation

---

## Per-Feature Analysis

### ✅ Strong Performance (>75% pass rate)

#### Match1 - Match nodes: 85% (73/86)
**Strengths:**
- Basic node matching works
- Single label matching
- Property predicates (inline)

**Failures (13):**
- 3 cases: Internal fields exposed in results
- 5 cases: Parameter validation errors
- 3 cases: Multiple label syntax not supported
- 2 cases: Type conversion issues

**Priority Fixes:**
1. Fix result serialization to hide internal fields
2. Support multiple labels syntax (:A:B)

---

#### Match2 - Match relationships: 83% (71/86)
**Strengths:**
- Basic relationship matching
- Single relationship type
- Bidirectional patterns

**Failures (15):**
- 7 cases: WHERE filtering returns 0 rows
- 4 cases: Parameter validation
- 2 cases: Multiple relationship types
- 2 cases: Internal fields exposed

**Priority Fixes:**
1. Fix WHERE clause on relationships
2. Support multiple relationship types OR syntax

---

#### Match6 - Named paths: 80% (78/97)
**Strengths:**
- Basic path naming works
- Single-hop paths

**Failures (19):**
- 12 cases: Empty results for complex paths
- 7 cases: Multi-hop path variable binding

**Priority Fixes:**
1. Fix path variable binding for multi-hop patterns
2. Support zero-length paths

---

### ⚠️ Partial Performance (25-75%)

#### Match7 - Optional match: 39% (12/31)
**Issue:** OPTIONAL MATCH not returning NULL for missing data

**Failures (19):**
- 14 cases: Empty results instead of NULL rows
- 3 cases: Variable length with optional
- 2 cases: WHERE on optional match

**Priority Fix:** Implement proper LEFT JOIN semantics

---

#### MatchWhere1 - Filter single variable: 27% (4/15)
**Issue:** WHERE predicates don't filter correctly

**Failures (11):**
- 9 cases: WHERE returns wrong data or 0 rows
- 2 cases: NULL handling in predicates

**Priority Fix:** Fix WHERE clause execution engine

---

### ❌ Weak Performance (<25%)

#### Match3 - Fixed length patterns: 17% (5/30)
**Issue:** Multi-hop patterns don't work

**Failures (25):**
- 19 cases: Returns 0 rows for multi-hop paths
- 6 cases: Wrong row counts

**Priority Fix:** Fix multi-hop pattern traversal

---

#### Match4 - Variable length patterns: 10% (1/10)
**Issue:** [*1..3] syntax not implemented

**Failures (9):**
- All variable-length path expansion failures

**Priority Fix:** Implement variable-length path algorithm

---

#### MatchWhere2-6 - Advanced WHERE: 0% (0/19 combined)
**Issue:** Complex WHERE predicates completely broken

**Failures:**
- All WHERE with multiple variables
- All join conditions
- All NULL handling in WHERE

**Priority Fix:** Rewrite WHERE clause execution

---

#### Match8-9 - Interop/Deprecated: 0% (0/12 combined)
**Issue:** Advanced features not implemented

**Failures:**
- Clause interoperation (MATCH + MERGE)
- Deprecated relationship collection syntax

**Priority Fix:** Low priority - advanced features

---

## Root Cause Summary

### Critical (Blocking Many Tests)

**1. WHERE Clause Execution (51 failures)**
- **Impact:** 35.9% of all Match failures
- **Scope:** All MatchWhere features (30 failures), filtering in Match (21 failures)
- **Root Cause:** WHERE predicate evaluation broken in query engine
- **Priority:** **CRITICAL** - Makes filtering unusable

**2. Result Serialization (23 failures)**
- **Impact:** 16.2% of all Match failures
- **Scope:** All Match features exposing internal fields
- **Root Cause:** Result formatting layer leaking internal representation
- **Priority:** **HIGH** - Breaks user-facing API contract

### High Priority

**3. Named Path Handling (12 failures)**
- **Impact:** 8.5% of all Match failures
- **Scope:** Match6 multi-hop paths
- **Root Cause:** Path variable binding for multi-hop patterns
- **Priority:** **HIGH** - Path queries are important use case

**4. Optional Match (22 failures)**
- **Impact:** 15.5% of all Match failures
- **Scope:** Match7 OPTIONAL MATCH + MatchWhere6
- **Root Cause:** Missing LEFT JOIN semantics
- **Priority:** **MEDIUM** - Nice to have but not critical

### Medium Priority

**5. Variable-Length Paths (9 failures)**
- **Impact:** 6.3% of all Match failures
- **Scope:** Match4 entirely
- **Root Cause:** Not implemented - needs path expansion algorithm
- **Priority:** **MEDIUM** - Important graph feature

**6. Multi-Hop Patterns (25 failures)**
- **Impact:** 17.6% of all Match failures
- **Scope:** Match3 fixed-length patterns
- **Root Cause:** Graph traversal for N-hop patterns
- **Priority:** **MEDIUM** - Needed for pattern matching

---

## Actionable Fix Plan

### Phase 1: Critical Fixes (Unlock ~74 scenarios)

**Fix 1: WHERE Clause Execution**
- **Files:** `crates/uni-query/src/query/executor/`
- **Issue:** WHERE predicates not filtering rows correctly
- **Impact:** +51 scenarios
- **Effort:** HIGH - Core query execution logic

**Fix 2: Result Serialization**
- **Files:** `crates/uni-query/src/query/executor/result.rs`
- **Issue:** Hide internal fields (_vid, _label, _eid) from results
- **Impact:** +23 scenarios
- **Effort:** MEDIUM - Result formatting layer

### Phase 2: High Priority (Unlock ~34 scenarios)

**Fix 3: Named Path Binding**
- **Files:** `crates/uni-query/src/query/planner/`
- **Issue:** Multi-hop path variable binding
- **Impact:** +12 scenarios
- **Effort:** MEDIUM - Planner logic

**Fix 4: Optional Match**
- **Files:** `crates/uni-query/src/query/executor/`
- **Issue:** Implement LEFT JOIN for OPTIONAL MATCH
- **Impact:** +22 scenarios
- **Effort:** HIGH - Requires join operator

### Phase 3: Medium Priority (Unlock ~34 scenarios)

**Fix 5: Variable-Length Paths**
- **Files:** `crates/uni-algo/` or `crates/uni-query/`
- **Issue:** Implement [*min..max] path expansion
- **Impact:** +9 scenarios
- **Effort:** HIGH - New algorithm needed

**Fix 6: Multi-Hop Patterns**
- **Files:** `crates/uni-query/src/query/executor/`
- **Issue:** Fix graph traversal for N-hop paths
- **Impact:** +25 scenarios
- **Effort:** MEDIUM - Extend existing traversal

---

## Success Metrics

**Current:** 244/386 Match scenarios (63.2%)

**Phase 1 Target:** 318/386 (82.4%) - +74 scenarios
**Phase 2 Target:** 352/386 (91.2%) - +34 scenarios
**Phase 3 Target:** 386/386 (100%) - +34 scenarios

**Next Milestone:** 75% pass rate (290 scenarios) - Achievable with Phase 1

---

## Testing Strategy

**Regression Testing:**
```bash
# Test specific Match features after fixes
./scripts/run_tck_with_report.sh
cat target/cucumber/match-report.md

# Focus on WHERE fixes
cargo test -p uni-tck --test cucumber -- features/clauses/match-where/

# Focus on result format
cargo test -p uni-tck --test cucumber -- features/clauses/match/Match1.feature
```

**Validation:**
1. Fix WHERE clause → Run MatchWhere features
2. Fix result format → Check Match1, Match2 for internal field exposure
3. Fix named paths → Run Match6
4. Fix optional match → Run Match7

---

## Recommendations

### Immediate Actions (This Week)
1. **Fix WHERE clause execution** - Highest impact (51 scenarios)
2. **Fix result serialization** - Clean up API (23 scenarios)

### Short Term (This Month)
3. Implement OPTIONAL MATCH properly
4. Fix named path variable binding

### Medium Term (Next Quarter)
5. Implement variable-length paths
6. Fix multi-hop pattern traversal

### Long Term
7. Advanced WHERE features (joins, complex predicates)
8. Deprecated syntax support

---

## Related Files

**Analysis Scripts:**
- `scripts/analyze_match_failures.py` - Match-specific failure analysis
- `scripts/analyze_tck_json.py` - General TCK analysis

**Test Reports:**
- `target/cucumber/match-report.md` - Match features summary
- `target/cucumber/where-report.md` - Where features summary
- `target/cucumber/results.json` - Raw test results

**Source Code:**
- `crates/uni-query/src/query/executor/` - Query execution
- `crates/uni-query/src/query/planner/` - Query planning
- `crates/uni-store/src/runtime/` - Graph storage/traversal

---

*This analysis is based on TCK test results from 2026-02-05. Re-run analysis after fixes to track progress.*
