# TCK High-Value Issues Analysis

**Generated:** 2026-02-05
**Based on:** 1,352 result-related step failures (categories 1–3)

---

## Actionable Themes (ranked by impact)

| # | Root Cause | Failures | % | Notes |
|---|-----------|----------|---|-------|
| 1 | **Temporal formatting/parsing** | 376 | 27.8% | Biggest single theme |
| 2 | **ORDER BY issues** | 191 | 14.1% | WITH/RETURN ordering broken |
| 3 | **Quantifier invariant tests** | 103 | 7.6% | Complex nested pattern tests |
| 4 | **Mutation persistence/interop** | 90 | 6.7% | CREATE/DELETE/SET/REMOVE |
| 5 | **Operator precedence** | 61 | 4.5% | Boolean/list precedence |
| 6 | **MATCH pattern issues** | 56 | 4.1% | Multi-hop, optional, var-length |
| 7 | **MERGE not implemented** | 55 | 4.1% | Entirely missing |
| 8 | **Node type representation** | 45 | 3.3% | Nodes returned as Map, not Node |
| 9 | **RETURN expression/aliasing** | 43 | 3.2% | Projections, expressions, DISTINCT |
| 10 | **WITH pipeline** | 40 | 3.0% | WHERE/SKIP/LIMIT after WITH broken |
| 11 | **Aggregation** | 36 | 2.7% | COUNT/SUM/AVG etc. |
| 12 | **Comparison ranges** | 33 | 2.4% | Half/full-bound ranges |
| 13 | **String functions** | 31 | 2.3% | Entirely missing |
| 14 | **Graph functions** | 30 | 2.2% | labels(), type(), properties() |

---

## #1: Temporal Formatting/Parsing (376 failures)

The single largest source. Three distinct sub-issues:

| Sub-issue | Count | Example |
|-----------|-------|---------|
| **Projection (selecting fields)** | 116 | `Temporal3`: `date({year: d.year})` returns wrong date |
| **Redundant `:00` seconds** | 108 | Actual: `2000-01-02T00:00:00Z`, Expected: `2000-01-02T00:00Z` |
| **Truncation logic errors** | 42 | `Temporal9`: wrong truncation results |
| **String parsing** | 38 | `Temporal2`: parsing `datetime('...')` not implemented |
| **Timezone formatting** | 30 | `+00:00` vs `Z`, missing `[Europe/Stockholm]` |
| **Map construction edge cases** | 33 | `Temporal1`: week-date construction issues |

The `:00` seconds issue alone (108 failures) is a formatting-only fix — the values are correct, just the string representation includes redundant seconds.

### Effort vs Impact

- **Redundant `:00` seconds fix** — low effort, ~108 scenarios recovered
- **Timezone `+00:00` vs `Z` normalization** — low effort, ~30 scenarios
- **String parsing (`datetime('...')`)** — medium effort, ~38 scenarios
- **Projection logic** — medium effort, ~116 scenarios
- **Truncation fixes** — medium effort, ~42 scenarios

---

## #2: ORDER BY Issues (191 failures)

| Sub-issue | Count | Notes |
|-----------|-------|-------|
| Wrong sort order | 134 | Values present but in wrong order (string: 60, int: 53, bool: 11) |
| Empty result from ORDER BY | 45 | Query with ORDER BY returns nothing |
| RETURN ORDER BY | 12 | Expression-based ordering |

`WithOrderBy2` regressed heavily (25→1 passing). The sort order issues suggest the comparator isn't handling all types correctly, or ORDER BY expressions aren't being evaluated.

### Key Observations

- WithOrderBy2 (order by single expression) regressed from 25/83 to 1/83 — likely a planner validation regression
- Sort comparisons for string, int, and boolean types are all affected
- Some ORDER BY queries return empty results, suggesting the ordering step discards results

---

## #3: Quantifier Invariant Tests (103 failures)

These are complex nested-pattern tests (`Quantifier9`, `Quantifier10`, `Quantifier11`, `Quantifier12`) that test edge cases and invariants of ANY, ALL, NONE, SINGLE quantifiers. The base quantifier functionality works well (81% pass rate overall) but invariant tests exercise deeper behaviors.

---

## #4: Mutation Persistence/Interop (90 failures)

| Sub-issue | Count | Features |
|-----------|-------|----------|
| Side effects not persisted for verification | 39 | `Create6`, `Delete6`, `Set6`, `Remove3` |
| SET other issues | 18 | `Set1`–`Set5` |
| CREATE interop with other clauses | 9 | `Create3` |
| DELETE issues | 9 | `Delete1`–`Delete5` |
| REMOVE issues | 8 | `Remove1`–`Remove2` |
| CREATE multi-hop/large patterns | 5 | `Create4`, `Create5` |

Persistence features (`Create6`, `Delete6`, `Set6`, `Remove3`) all test that mutations from one clause are visible to subsequent clauses in the same query. This is a single infrastructure issue rather than many separate bugs.

---

## #5: Operator Precedence (61 failures)

| Sub-issue | Count | Features |
|-----------|-------|----------|
| Boolean precedence | 50 | `Precedence1` (32 no-result, 12 extra-rows) |
| List operator precedence | 11 | `Precedence3` |

Boolean precedence tests check correct evaluation order of AND, OR, NOT, XOR with comparisons. List precedence tests check IN, concatenation, and indexing priority.

---

## #6: MATCH Pattern Issues (56 failures)

| Sub-issue | Count | Features |
|-----------|-------|----------|
| Fixed-length multi-hop patterns | 19 | `Match3`: `(a)-[:T]->(b)-[:T]->(c)` |
| Variable-length patterns | 17 | `Match4`: `(a)-[*1..3]->(b)` |
| OPTIONAL MATCH | 12 | `Match7`: optional path segments |
| Deprecated syntax | 3 | `Match9` |
| Other | 5 | `Match6`, `Match8` |

Variable-length patterns (`[*1..3]`) are not implemented. Fixed-length multi-hop patterns should be working but appear to have issues with longer chains.

---

## #7: MERGE Not Implemented (55 failures)

Entirely unimplemented. `MERGE` is the upsert operator (create-if-not-exists). Affects 75 total scenarios but 55 of those show up as result failures (rest are error-related). Requires:
- Match-or-create semantics
- ON CREATE / ON MATCH clauses
- Integration with the mutation pipeline

---

## #8: Node Type Representation (45 failures)

Nodes are returned as `Map({"_vid": Int(0), "_label": String(""), ...})` instead of a proper `Node` type. The TCK expects `Node(Node { vid: Vid(0), label: "A", properties: {...} })`.

Issues:
- **Missing Node type** in query result representation
- **Empty `_label` field** — label should be "A", "B", etc. but is empty string `""`
- **Internal fields exposed** — `_vid` and `_label` leak into property maps

Affects: `WithOrderBy`, `Return`, `MatchWhere`, `Graph`, and other features that return node variables.

---

## #9: RETURN Expression/Aliasing (43 failures)

| Sub-issue | Count | Features |
|-----------|-------|----------|
| Other RETURN issues | 23 | `Return2`, `Return4` |
| Aggregation/DISTINCT in RETURN | 4 | `Return5`, `Return6` |
| Expression projection | 10 | `Return2`: expressions not evaluated |
| RETURN SKIP/LIMIT | 10 | `ReturnSkipLimit1`–`ReturnSkipLimit3` |

Notable: `Return2` (single expression) has 18 scenarios at 0% — expressions in RETURN aren't being projected correctly. Large integer precision is also lost (returned as String instead of Int).

---

## #10: WITH Pipeline (40 failures)

| Sub-issue | Count | Features |
|-----------|-------|----------|
| WITH WHERE | 13 | `WithWhere1`–`WithWhere7` |
| WITH SKIP/LIMIT | 6 | `WithSkipLimit1`–`WithSkipLimit3` |
| WITH (general) | 21 | `With1`–`With7` |

The WITH clause pipeline doesn't propagate variables correctly to WHERE, and SKIP/LIMIT after WITH is not working. Variable aliasing in WITH also has issues.

---

## #11: Aggregation (36 failures)

All aggregation features are at 0% pass rate. The functions exist but don't return correct results:
- `COUNT` — 2 failures
- `SUM` — 2 failures (also returns Float instead of Int for integer inputs)
- `MIN/MAX` — 12 failures
- `COLLECT` — 2 failures
- Percentiles — 13 failures
- `DISTINCT` — 4 failures

SUM of integers producing `Float(15.0)` instead of `Int(15)` is a type preservation issue that also appears in `Create6`, `Remove3`, and `Set6` persistence tests.

---

## #12–14: Other Significant Issues

### Comparison Ranges (33 failures)
Half-bounded (`WHERE n.x > 1`) and full-bounded (`WHERE 1 < n.x < 10`) range comparisons. The equality operator works well (65.1%) but ranges fail.

### String Functions (31 failures)
Entirely unimplemented: `STARTS WITH`, `ENDS WITH`, `CONTAINS`, `substring()`, `reverse()`, `split()`, `toUpper()`, `toLower()`.

### Graph Functions (30 failures)
`labels(n)`, `type(r)`, `properties(n)`, `keys(n)` — these introspection functions are incomplete. Related to the Node type representation issue (#8).

---

## Quick Wins (highest ROI fixes)

| Fix | Est. Scenarios Recovered | Effort |
|-----|--------------------------|--------|
| Temporal: omit `:00` seconds when zero | ~108 | Low |
| Temporal: normalize `+00:00` to `Z` | ~30 | Low |
| Fix WithOrderBy2 regression | ~24 | Low–Medium |
| SUM type preservation (Int→Int) | ~10 | Low |
| Node type in results (Map→Node) | ~45 | Medium |
| String functions (STARTS WITH, etc.) | ~31 | Medium |
| WITH WHERE support | ~13 | Medium |
| Aggregation functions | ~36 | Medium |

**Total quick-win potential: ~297 scenarios (~7.7% pass rate improvement)**
