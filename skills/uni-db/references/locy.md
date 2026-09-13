# Locy Reference

Locy (Logic + Cypher) is a Datalog-inspired logic programming language extending OpenCypher with recursive rules, path accumulation, aggregation, probabilistic inference, hypothetical reasoning, abductive inference, and graph materialization. Every valid Cypher query is a valid Locy program. Locy compiles rules into execution plans that run inside Uni's DataFusion-based query engine.

## 1. When to Use Locy vs Cypher

| Task | Cypher | Locy |
|------|--------|------|
| Simple CRUD, one-shot pattern matching, schema DDL | Yes | Overkill / not supported |
| Transitive closure / reachability | Awkward (`[*]` paths) | Natural (recursive rules) |
| Weighted shortest path | Not expressible | ALONG + BEST BY |
| Risk/score propagation | Not expressible | Recursive FOLD |
| Probabilistic inference | Not expressible | MNOR/MPROD + PROB |
| What-if / root-cause / proof traces | Not expressible | ASSUME / ABDUCE / EXPLAIN RULE |
| Graph materialization from reasoning | Manual CREATE | DERIVE |
| Permission resolution with priorities | Complex workarounds | PRIORITY rules |

---

## 2. Program Structure

```
MODULE namespace.path              -- Optional module namespace (at most one)
USE other.module { rule1, rule2 }  -- Optional selective import
USE another.module                 -- Optional glob import (all rules)

-- Statements: rules and commands (any order, any count)
CREATE RULE ... AS ...
QUERY rule_name WHERE ... RETURN ...
DERIVE rule_name
ASSUME { ... } THEN { ... }
MATCH (n) RETURN n                 -- Plain Cypher passthrough
```

Rules are compiled first (grouped, stratified, typechecked). Commands execute second, in order. Cypher statements return `CommandResult::Cypher(Vec<FactRow>)`.

Multiple query blocks can be combined with `UNION` (dedup) or `UNION ALL` (keep duplicates).

---

## 3. Rule Syntax -- CREATE RULE

### Full Syntax Template

```
CREATE RULE name [PRIORITY n] AS
    MATCH pattern
    [WHERE conditions]              -- pre-aggregation filter (+ IS / IS NOT refs)
    [ALONG accumulations]
    [FOLD aggregations]
    [REQUIRE conditions]            -- definitional threshold (constrains recursion)
    [WHERE conditions]              -- post-FOLD filter (HAVING semantics)
    [BEST BY selections]
    (YIELD items | DERIVE patterns)
```

Every clause is optional except MATCH and the terminal (YIELD or DERIVE). The
second `WHERE` (after `FOLD`) is the post-aggregation filter: it runs once the
FOLD aggregates are computed and may reference FOLD output columns and KEY
columns (SQL `HAVING` semantics). There is **no `HAVING` keyword** — it is
spelled `WHERE`, positioned after `FOLD`.

`REQUIRE` takes the same kind of condition but is part of the rule's
*definition*: in a recursive rule it is applied to every iteration, so it
constrains what the rule can derive, where the post-FOLD `WHERE` only filters
the converged answer. See §7.

### Rule Names

```
CREATE RULE reachable AS ...           -- Simple name
CREATE RULE acme.risk_score AS ...     -- Qualified name
CREATE RULE `my-rule` AS ...           -- Backtick-quoted for reserved words
```

Identifiers: `[a-zA-Z_][a-zA-Z0-9_]*`. Reserved keywords that must be backtick-quoted if used as identifiers: `RULE`, `ALONG`, `PREV`, `FOLD`, `BEST`, `DERIVE`, `ASSUME`, `ABDUCE`, `QUERY`.

### Multi-Clause Union Semantics

Multiple `CREATE RULE` statements with the same name define different clauses of one rule. Results are the union of all clause evaluations. All clauses must have the same YIELD schema (same column count, KEY positions, PROB annotations). Violations produce `YieldSchemaMismatch`.

```
CREATE RULE reachable AS                          -- Clause 1: base case
    MATCH (a:Node)-[:EDGE]->(b:Node)
    YIELD KEY a, KEY b

CREATE RULE reachable AS                          -- Clause 2: recursive case
    MATCH (a:Node)-[:EDGE]->(mid:Node)
    WHERE mid IS reachable TO b
    YIELD KEY a, KEY b
```

### WHERE Clause

Comma-separated conditions (comma = AND). Three condition types can be mixed:

```
WHERE a IS reachable,                   -- IS reference (positive)
      b IS NOT blocked,                 -- IS NOT reference (negation)
      a.score > 0.5,                    -- Cypher expression
      x IN [1, 2, 3]                    -- Cypher expression
```

Full Cypher expression support: comparisons, arithmetic, function calls, `$param` references, `IS NULL`, `CONTAINS`, `STARTS WITH`, `ENDS WITH`, regex, `CASE`, list comprehensions, etc.

---

## 4. IS References

IS references compose rules -- one rule references another rule's derived relation.

### Positive IS References

| Form | Syntax | Binding |
|------|--------|---------|
| Unary | `WHERE x IS flagged` | `x` bound to first KEY column |
| Binary (TO) | `WHERE x IS reachable TO y` | `x`, `y` bound to first two columns |
| Tuple | `WHERE (x, y, cost) IS weighted_path` | All variables bound positionally |

Semantics: a semi-join -- for each MATCH row, check if subject(s) exist in the target rule's derived relation. Additional yield columns beyond bound subjects become available as `__prev_*` variables for ALONG.

Binding count must not exceed target rule's yield schema width (error: `IsArityMismatch`).

**Forwarding value columns.** A non-KEY value column brought in by an IS reference
(e.g. `WHERE (p, c) IS pc_mapped` exposing `pc_mapped`'s `infringement` column) can be
re-yielded and then consumed by a downstream rule. This works for FOLD outputs and
plain value columns alike.

**All-elements idiom.** A FOLD product/count runs over *matched* rows only, so it does
not by itself require that an entire expected set is present. To express "every element
must match," carry a total count and a matched count and keep only equal-count groups:

```
CREATE RULE claim_size AS
    MATCH (c:Claim) WHERE c IS claim_elements TO ce
    FOLD n_total = COUNT(ce) YIELD KEY c, n_total

CREATE RULE pc_mapped AS
    MATCH (p:Product), (c:Claim)
    WHERE c IS claim_elements TO ce, p IS element_mapped TO ce
    FOLD n_mapped = COUNT(ce), infringement = MPROD(mapping_conf)
    YIELD KEY p, KEY c, n_mapped, infringement

CREATE RULE claim_infringed AS                 -- all-elements guard
    MATCH (p:Product), (c:Claim)
    WHERE (p, c) IS pc_mapped, c IS claim_size TO n_total, n_mapped = n_total
    YIELD KEY p, KEY c, infringement
```

### IS NOT References (Negation)

```
WHERE x IS NOT blocked            -- Postfix form
WHERE NOT x IS blocked            -- Prefix form
WHERE x IS NOT rule TO y          -- Binary with negation
WHERE (x, y) IS NOT rule          -- Tuple with negation
```

| Target rule has PROB? | Semantics |
|-----------------------|-----------|
| No | **Boolean anti-join**: keep rows where subject NOT in target |
| Yes | **Probabilistic complement**: matched rows contribute `1 - p`; unmatched rows contribute `1.0` |

### Stratification

The negated rule must be in a completed lower stratum -- no recursive negation allowed. Violations detected as `CyclicNegation`.

---

## 5. YIELD Clause

### Basic YIELD

```
YIELD a, b.name AS neighbor, cost + 1 AS adjusted
```

Each item is a Cypher expression with optional alias. Without alias, column name is inferred from the expression.

### KEY Columns

```
YIELD KEY a, KEY b, cost
```

KEY marks a column as a grouping key:
- Defines fact identity
- Determines fixpoint convergence in recursive evaluation
- Used as join keys for IS references from other rules
- Implicit GROUP BY for FOLD aggregation

Deduplication is on the **whole row**, not on KEY alone, so a rule may produce
several rows per KEY group -- which is what ALONG and BEST BY rely on. Declaring
no KEY columns removes the grouping, not the deduplication; usually an
anti-pattern in recursive rules.

A FOLD aggregates the **bag of rows its clause emits**, not the set of distinct
row values: N derivations with equal values contribute N times (issue #159).
Deduplication still suppresses re-derivations of the same fact by the same
bindings, which is what makes the fixpoint terminate.

Inside a recursive stratum, what a **self-reference** contributes is the
target's *folded value per KEY*, not the rows behind it (issue #162). A rollup
therefore composes one level at a time: a parent folds its children's values,
and each child has already folded its own. A clause carrying `ALONG` is the
exception — it reads the pre-fold rows, because `prev.x` accumulates along a
single path. The distinction is invisible for an associative fold over a bare
inherited column (`MPROD(b)`, `MSUM(cost)`) and visible for `MCOUNT`, which
counts a node's children rather than its leaves.

### PROB Annotation

```
YIELD KEY a, risk PROB
YIELD KEY a, risk AS risk_score PROB
```

- At most one PROB column per rule (error: `MultipleProbColumns`)
- MNOR/MPROD fold outputs are implicitly PROB (auto-annotated)
- PROB changes IS NOT semantics from Boolean anti-join to probabilistic complement

### Schema Consistency

All clauses of a multi-clause rule must produce the same YIELD schema: same column count, same KEY positions, same PROB annotations.

---

## 6. ALONG (Path-Carried Values)

Accumulates values along recursive traversal paths.

### Syntax

```
ALONG cost = expr
ALONG cost = expr, hops = expr          -- Multiple accumulators
```

### prev.field References

```
ALONG cost = prev.cost + e.weight
```

`prev.field` accesses the value from the previous recursive hop. Rules:
- Only valid in recursive clauses (error: `PrevInBaseCase`)
- Must reference an existing column in the target rule's yield schema (error: `PrevFieldNotInSchema`)

### Base Case vs Recursive Case

```
CREATE RULE path_cost AS                              -- Base: no prev
    MATCH (a:Node)-[e:EDGE]->(b:Node)
    ALONG cost = e.weight
    YIELD KEY a, KEY b, cost

CREATE RULE path_cost AS                              -- Recursive: with prev
    MATCH (a:Node)-[e:EDGE]->(mid:Node)
    WHERE mid IS path_cost TO b
    ALONG cost = prev.cost + e.weight
    YIELD KEY a, KEY b, cost
```

Multiple accumulators are independently computed per hop:

```
ALONG distance = prev.distance + e.length,
      hops = prev.hops + 1,
      max_weight = prev.max_weight
```

---

## 7. FOLD (Aggregation)

Aggregates values across rows sharing the same KEY group.

### Standard Aggregates

| Aggregate | Description | Output Type | In recursion |
|-----------|-------------|-------------|--------------|
| `SUM(expr)` | Sum of values | Float64 | Rejected |
| `AVG(expr)` | Arithmetic mean | Float64 | Rejected |
| `COUNT(expr)` | Count of non-null values | Int64 | Allowed (unbounded) |
| `COUNT(*)` | Count of all rows | Int64 | Allowed (unbounded) |
| `MIN(expr)` | Minimum value | Same as input | Allowed |
| `MAX(expr)` | Maximum value | Same as input | Allowed |
| `COLLECT(expr)` | Collect into a list | List | Allowed (unbounded) |

Recursive use is decided by the monotonicity oracle, which reads each aggregate's
`Semilattice.monotone_join` flag from the plugin registry (falling back to the six
`M*` names below on a registry miss). `SUM` and `AVG` declare `monotone_join: false`
and are rejected in a recursive stratum with `NonMonotonicInRecursion`; the others
are monotone and compile.

`COUNT`, `COUNT(*)` and `COLLECT` are monotone but **unbounded** (`has_top: false`);
`MIN` and `MAX` are bounded (`has_top: true`). Unboundedness only bites for the
aggregates the fixpoint loop actually tracks: `COUNT` / `COUNT(*)` carry a row-level
accumulator, so a recursive rule folding over them may never reach a fixpoint and
instead runs out at `max_iterations` -- which surfaces as `UniError::LocyIncomplete`
with reason `IterationLimit` unless `allow_partial` is set. `COLLECT` (like `AVG`)
has no row-level accumulator; the fixpoint loop skips it and it is evaluated after
convergence, so it does not itself keep the loop iterating. The iteration cap is the
backstop, not a convergence guarantee.

Because the oracle is registry-backed, a plugin-registered aggregate declaring
`monotone_join: true` is likewise accepted in a recursive stratum -- via
`session.locy()` and via `db.rules().register(...)`. This does **not** survive a
reopen: persisted rules in `catalog/locy_rules.json` are recompiled during open,
before any plugin can be added (`add_plugin` only exists on an already-open `Uni`),
so a stored rule folding over a plugin aggregate fails the open naming that rule --
unless you open with `skip_invalid_locy_rules(true)`, which drops it with a warning
so you can re-register it after `add_plugin`. Rules folding over built-in
aggregates reload normally.

### Monotonic Aggregates (Declared Lattice Folds)

| Aggregate | Formula | Direction | Identity | Domain |
|-----------|---------|-----------|----------|--------|
| `MSUM(expr)` | `acc + val` | Non-decreasing | 0.0 | Non-negative |
| `MMAX(expr)` | `max(acc, val)` | Non-decreasing | -infinity | None |
| `MMIN(expr)` | `min(acc, val)` | Non-increasing | +infinity | None |
| `MCOUNT(expr)` | `acc + 1` | Non-decreasing | 0 | None |
| `MNOR(expr)` | `1 - (1-acc)(1-val)` | Non-decreasing | 0.0 | [0, 1] |
| `MPROD(expr)` | `acc * val` | Non-increasing | 1.0 | [0, 1] |

Fixpoint converges when: (1) no new KEY tuples produced, AND (2) all monotonic accumulators stable (change < `f64::EPSILON`). An unbounded monotone fold that the loop tracks (`COUNT`, `MSUM`, `MCOUNT`) can fail condition (2) indefinitely; such a rule terminates at `max_iterations` instead. `COLLECT` and `AVG` have no row-level accumulator and are not part of condition (2) at all -- they are computed after the fixpoint.

### Post-FOLD WHERE (HAVING semantics)

A second `WHERE`, placed **after `FOLD`**, filters the aggregated groups (SQL
`HAVING`). There is no `HAVING` keyword — use `WHERE` in the post-FOLD position.
It may reference FOLD output columns and KEY columns; multiple conditions combine
with `AND` (or commas).

```
CREATE RULE heavy_spender AS
    MATCH (c:Customer)-[o:ORDERED]->(:Product)
    FOLD total = SUM(o.amount)
    WHERE total >= 1000        -- post-FOLD filter (HAVING)
    YIELD KEY c, total
```

**`total_iterations` note:** non-recursive programs report `total_iterations >= 1`
(one evaluation pass); recursive programs report the fixpoint iteration count.

#### In a recursive rule, the post-FOLD WHERE does NOT constrain the recursion

This is the one place the SQL `HAVING` analogy breaks, and it fails quietly.

In a rule that references **itself**, the post-FOLD `WHERE` is applied **once, to
the converged answer** — not per iteration. The self-reference therefore reads
the rule's *unfiltered* folded value, so rows the threshold excludes can still
derive further rows. Those rows are correctly absent from the output *while
having contributed to it*, which is exactly why the result looks plausible.

```locy
// WRONG if you meant "an owner only counts once it reaches 50%".
CREATE RULE blocked AS
    MATCH (o:Entity)-[s:OWNS]->(e:Entity)
    WHERE o IS blocked            // self-reference: stratum is recursive
    FOLD agg = MSUM(s.pct)
    WHERE agg >= 50.0             // filters the ANSWER, not the recursion
    YIELD KEY e, agg
```

An entity at 11% is filtered out of the result, yet still satisfies
`o IS blocked` for the next hop, so everything it owns is derived anyway.
Splitting the aggregate into its own rule and filtering at the consumer does not
help either — the filter still sits outside the recursion.

The compiler emits a `HavingInRecursivePath` warning for this shape. Read
`result.warnings()`.

**When post-FOLD `WHERE` is right:** when the filter is meant to select *what
you see*. A child that the threshold removes from the answer must still have
been visible to its parent while the fixpoint ran — that is the intended
reading for probabilistic rules (issue #162).

#### `REQUIRE` — when the threshold is part of the definition

Use `REQUIRE` when the threshold *defines* the rule: an ownership or
voting-control cutoff, a quorum, a cumulative-risk ceiling, reachability under a
cost budget. It is applied to every iteration's folded snapshot, which is what a
self-reference reads, so it constrains what the recursion can derive.

```locy
CREATE RULE blocked AS
    MATCH (o:Entity)-[s:OWNS]->(e:Entity)
    WHERE o IS blocked
    FOLD agg = MSUM(s.pct)
    REQUIRE agg >= 50.0           // constrains the RECURSION
    YIELD KEY e, agg
```

On the graph above this answers `{N6}` — the owner at 11% is excluded, so
nothing downstream of it is derived. The same rule with `WHERE` answers
`{N6, N1}`.

Both may appear on one rule; `REQUIRE` is written first, and reads as
"constrain the derivation, then filter what is shown".

**`REQUIRE` must be one-way.** It is rejected at compile time
(`NonMonotonicFilterInRecursion`) unless the comparison can only ever turn from
false to true as the fixpoint grows: a **lower** bound (`>=`, `>`) over a
non-decreasing fold (`MSUM` over non-negative values, `MMAX`, `MCOUNT`,
`MNOR`), or an **upper** bound (`<=`, `<`) over a non-increasing one (`MMIN`,
`MPROD`). Equality is never admissible. The reverse pairings would let a fact be
derived and then withdrawn, which the fixpoint reads as progress rather than
oscillation, so it would run to the iteration limit instead of converging.

In a **non-recursive** rule the direction is not policed and `REQUIRE` means
exactly what the post-FOLD `WHERE` means — one pass, nothing to flip. Writing
`REQUIRE` for a threshold you consider definitional is therefore safe from the
start: the rule keeps that meaning if a self-reference is added later.

### FOLD + BEST BY Restriction

BEST BY cannot be combined with a *declared lattice fold* in the same clause -- semantically contradictory (BEST BY keeps one witness row; a lattice fold aggregates across all of them). Error: `BestByWithMonotonicFold`.

The check is syntactic over the six `M`-prefixed spellings -- `MSUM`, `MMAX`, `MMIN`, `MCOUNT`, `MNOR`, `MPROD` -- and is deliberately decoupled from the monotonicity oracle. So `BEST BY ... FOLD MAX(x)` / `MIN` / `COUNT` / `COLLECT` are legal even though those are monotone in the registry. The guard runs on every rule, not only recursive ones.

---

## 8. BEST BY (Witness Selection)

Retains the single best derivation per KEY group, preserving the full witness row.

```
BEST BY cost ASC                           -- Minimum cost (ASC is default)
BEST BY reliability DESC                   -- Maximum reliability
BEST BY cost ASC, priority DESC            -- Multiple criteria (tie-breakers)
```

When `deterministic_best_by = true` (default), ties are broken by secondary sort on all remaining columns. Enables early pruning during semi-naive evaluation.

```
CREATE RULE shortest AS
    MATCH (a:City)-[r:ROAD]->(b:City)
    ALONG cost = r.distance
    BEST BY cost ASC
    YIELD KEY a, KEY b, cost

CREATE RULE shortest AS
    MATCH (a:City)-[r:ROAD]->(mid:City)
    WHERE mid IS shortest TO b
    ALONG cost = prev.cost + r.distance
    BEST BY cost ASC
    YIELD KEY a, KEY b, cost
```

---

## 9. PRIORITY

Enables defeasible reasoning: higher-priority rules override lower-priority rules for the same KEY group.

```
CREATE RULE access PRIORITY 0 AS        -- Default
    MATCH (u:User)-[:MEMBER_OF]->(g:Group {name: 'public'})
    YIELD KEY u, 'allow' AS decision

CREATE RULE access PRIORITY 100 AS      -- Exception: admin override
    MATCH (u:User)-[:HAS_ROLE]->(r:Role {name: 'admin'})
    YIELD KEY u, 'allow' AS decision
```

- Higher number = higher priority. Default (when omitted) is 0.
- Applied post-fixpoint: per KEY group, only derivations from highest-priority clause survive.
- Equal-priority clauses contribute all their derivations (union).
- All clauses must either all have PRIORITY or none have it (error: `MixedPriority`).

---

## 10. Probabilistic Reasoning

### MNOR (Noisy-OR)

Formula: `P = 1 - prod(1 - p_i)`

"Probability that at least one cause produces the effect." Inputs clamped to [0,1] unless `strict_probability_domain = true`.

```
CREATE RULE delivery_risk AS
    MATCH (warehouse:WH)-[route:ROUTE]->(customer:Customer)
    FOLD any_arrives = MNOR(route.reliability)
    YIELD KEY customer, any_arrives PROB
```

### MPROD (Product)

Formula: `P = prod(p_i)`

"Probability that all conditions hold simultaneously." Uses log-space computation when product drops below `probability_epsilon` (default `1e-15`).

```
CREATE RULE system_reliability AS
    MATCH (sys:System)-[:REQUIRES]->(comp:Component)
    FOLD all_work = MPROD(comp.reliability)
    YIELD KEY sys, all_work PROB
```

### PROB and IS NOT Complement

When a rule has PROB and another rule uses IS NOT against it:

```
CREATE RULE risky AS
    MATCH (a:Account)-[s:SIGNAL]->(f:Flag)
    FOLD risk = MNOR(s.probability)
    YIELD KEY a, risk PROB

CREATE RULE safe AS
    MATCH (a:Account)
    WHERE a IS NOT risky
    YIELD KEY a, 1.0 AS confidence PROB
```

- If `a` has risk 0.8 in `risky`: `confidence = 1 - 0.8 = 0.2`
- If `a` is not in `risky` at all: `confidence = 1.0`

Multiple IS/IS NOT references with PROB in a clause multiply their probability terms.

### Shared Proof Detection

When recursive rules have diamond-shaped derivation graphs, multiple proof paths may share base facts, violating the independence assumption. Uni detects this via a DerivationTracker and emits `SharedProbabilisticDependency` warning.

### Exact Probability (BDD-Based)

When `exact_probability = true`, shared-proof groups use BDD-based weighted model counting:
1. Collect unique base facts across derivation rows
2. Build BDD variable set (one per base fact)
3. Per derivation row: AND its base-fact variables
4. Combine rows: OR for MNOR, AND for MPROD
5. Evaluate via Shannon expansion

Fallback: when unique base facts exceed `max_bdd_variables` (default 1000), falls back to independence mode with `BddLimitExceeded` warning.

### Top-K Proof Filtering

`top_k_proofs` bounds proofs retained per derived fact (0 = unlimited). Keeps only the k highest-probability proofs. `top_k_proofs_training` optionally overrides during training.

---

## 11. Commands

Commands execute after all strata converge. They operate on converged derived relations.

> **Expression limitation:** WHERE filters in commands use `eval_expr()` (lightweight row-level evaluator), not DataFusion. Functions like `similar_to()` are limited to pure vector cosine. Rule WHERE clauses (Phase 1) have full DataFusion support.

### QUERY (Goal-Directed)

```
QUERY rule_name [WHERE expr] [RETURN items [ORDER BY ...] [SKIP n] [LIMIT n]]
```

Uses SLG resolution (top-down with tabling) for efficient point lookups without computing the full relation. Returns `CommandResult::Query(Vec<FactRow>)`.

```
QUERY reachable WHERE a.name = 'Alice' RETURN b.name AS destination
```

### DERIVE (Materialization)

```
DERIVE rule_name [WHERE expr]
```

Triggers bottom-up evaluation and applies graph mutations. Returns `CommandResult::Derive { affected: usize }`.

Rules can use DERIVE as an alternative terminal to YIELD:

```
DERIVE (a)-[:INFERRED_FRIEND]->(b)                          -- Edge derivation
DERIVE (a)-[:RISK_LINK {score: risk}]->(b)                   -- With properties
DERIVE (NEW cat:Category {name: a.type})<-[:BELONGS_TO]-(a)  -- NEW node (Skolem)
DERIVE MERGE a, b                                             -- Entity resolution
```

NEW node wardedness constraint: companion node must be bound by MATCH, not solely by IS references (error: `WardednessViolation`).

Session-level DERIVE collects mutations into `DerivedFactSet` (apply via `tx.apply()`). Transaction-level DERIVE applies immediately.

### ASSUME (Hypothetical Reasoning)

```
ASSUME {
    CREATE (x:Account {name: 'Suspicious'})-[:TRANSFER]->(existing:Account)
}
THEN {
    QUERY risk_propagation RETURN affected_nodes
}
```

Execution: fork L0 buffer -> apply mutations -> re-evaluate strata -> execute body -> rollback. Database is never permanently modified. Can be nested. Returns `CommandResult::Assume(Vec<FactRow>)`.

### ABDUCE (Abductive Reasoning)

```
ABDUCE [NOT] rule_name [WHERE expr] [RETURN items [ORDER BY ...] [LIMIT n]]
```

"What modifications would make this rule hold (or stop holding)?" Three-phase: generate candidates -> validate (ASSUME-style: fork L0, apply, re-evaluate) -> return sorted by cost. Returns `CommandResult::Abduce(AbductionResult)`.

Modification types: `RemoveEdge`, `AddEdge`, `ChangeProperty`.

**Important — candidates come from the target rule's OWN MATCH pattern.** For
`ABDUCE NOT rule`, candidates are generated by removing edges that appear in the
**target rule's MATCH pattern**, and each candidate is validated **independently**
(single modification — the search does not combine multiple removals). Two
practical consequences:

- The target rule must actually *contain* the edges you want removable. A rule
  whose body reaches the graph only through an `IS` reference (e.g.
  `MATCH (sys:System) WHERE sys IS exposure ...`) has no edges in its own pattern,
  so ABDUCE yields nothing. Spell the chain out in the rule's MATCH
  (`MATCH (r:Reg)-[:REQUIRES]->(o)-[:SAT_BY]->(c)-[:PROTECTS]->(p)-[:RUNS_ON]->(sys) ...`)
  so the edges are abducible.
- The goal must be reachable by a **single** removal. If clearing the target
  needs several independent facts removed at once, no candidate will validate.

```
ABDUCE NOT reachable WHERE a.name = 'A' AND b.name = 'C' RETURN modifications
```

### EXPLAIN RULE (Proof Traces)

```
EXPLAIN RULE rule_name [WHERE expr] [RETURN items [ORDER BY ...] [LIMIT n]]
```

Returns the derivation tree showing which clauses and base facts produced a result. Returns `CommandResult::Explain(DerivationNode)`.

---

## 12. API Reference

### Python

```python
# Session-level
result = session.locy("CREATE RULE r AS ... YIELD ...", params={"key": "value"})

# Builder pattern
result = session.locy_with("QUERY r WHERE x = $val") \
    .param("val", "Alice") \
    .params({"a": 1, "b": 2}) \
    .timeout(60.0) \
    .max_iterations(500) \
    .with_config({"exact_probability": True}) \
    .run()

# Compilation-only introspection
explain = session.locy_with("CREATE RULE r AS ...").explain()
# explain.plan_text, explain.strata_count, explain.has_recursive_strata

# Transaction-level
with session.tx() as tx:
    result = tx.locy("DERIVE infer_edges")
    result = tx.locy_with("QUERY r WHERE x = $val").param("val", "Alice").run()
    session_result = session.locy("DERIVE infer_edges")
    tx.apply(session_result.derived_fact_set)
    tx.commit()

# Async equivalents: await session.locy(...), async with await session.tx() as tx

# Rule registry
session.rules().register("CREATE RULE reach AS ...")
session.rules().list()       # -> ["reach"]
session.rules().get("reach") # -> RuleInfo { name, clause_count, is_recursive }
session.rules().remove("reach")
session.rules().count()
session.rules().clear()
```

### Rust

```rust
// Session-level
let result = session.locy("CREATE RULE r AS ... YIELD ...").await?;

// Builder pattern
let result = session.locy_with("QUERY r WHERE x = $val")
    .param("val", "Alice")
    .params([("a", Value::from(1)), ("b", Value::from(2))])
    .params_map(hashmap)
    .timeout(Duration::from_secs(60))
    .max_iterations(500)
    .cancellation_token(token)
    .with_config(LocyConfig { .. })
    .run()
    .await?;

// Compilation-only introspection
let explain = session.locy_with("CREATE RULE r AS ...").explain()?;
// explain.plan_text, explain.strata_count, explain.has_recursive_strata

// Transaction-level
let tx = session.tx().await?;
let result = tx.locy("DERIVE infer_edges").await?;
let result = tx.locy_with("QUERY r WHERE x = $val")
    .param("val", "Alice").run().await?;

// Apply session-level derived facts
let derived = session.locy("DERIVE infer_edges").await?.derived_fact_set.unwrap();
tx.apply(derived).await?;
tx.commit().await?;

// Rule registry (same API at db / session / tx level)
session.rules().register("CREATE RULE reach AS ...")?;
session.rules().list();      // Vec<String>
session.rules().get("reach"); // Option<RuleInfo>
session.rules().remove("reach")?;
session.rules().count();
session.rules().clear();
```

### Cancellation

`.cancellation_token(token)` on `LocyBuilder` / `TxLocyBuilder` / `SessionLocyBuilder` races the token against the **whole** evaluation, so a long-running fixpoint is cancellable -- not merely at statement boundaries. Cancelling returns `UniError::Cancelled` (`UniCancelledError` in Python). The enclosing session's or transaction's own scope applies too: `Transaction::cancel()` reaches `tx.locy()` / `tx.locy_with()` and `tx.apply()` as well as its Cypher statements.

### LocyResult Fields

| Field | Type | Description |
|-------|------|-------------|
| `derived` | `HashMap<String, Vec<FactRow>>` | Rule name -> derived facts |
| `stats` | `LocyStats` | Execution statistics |
| `command_results` | `Vec<CommandResult>` | Ordered command outputs |
| `warnings` | `Vec<RuntimeWarning>` | Runtime warnings |
| `approximate_groups` | `HashMap<String, Vec<String>>` | Approximate BDD groups |
| `derived_fact_set` | `Option<DerivedFactSet>` | Collected DERIVE mutations (session-level only) |

Convenience: `result.warnings()`, `result.has_warning(code)`.

### CommandResult Variants

| Variant | Payload |
|---------|---------|
| `Query` | `Vec<FactRow>` |
| `Assume` | `Vec<FactRow>` |
| `Explain` | `DerivationNode` |
| `Abduce` | `AbductionResult` (contains `Vec<ValidatedModification>`) |
| `Derive` | `{ affected: usize }` |
| `Cypher` | `Vec<FactRow>` |

### LocyStats Fields

| Field | Type |
|-------|------|
| `strata_evaluated` | `usize` |
| `total_iterations` | `usize` |
| `derived_nodes` | `usize` |
| `derived_edges` | `usize` |
| `evaluation_time` | `Duration` |
| `queries_executed` | `usize` |
| `mutations_executed` | `usize` |
| `peak_memory_bytes` | `usize` |

- `total_iterations`: fixpoint iterations across strata. **Non-recursive programs report `>= 1`** (one evaluation pass); recursive programs report the count to convergence.
- `queries_executed`: counts internal SLG clause evaluations during command resolution — **not** the number of `QUERY` statements (a single `QUERY` over a multi-rule chain reports several).

---

## 13. Module System

### MODULE Declaration

```
MODULE acme.compliance
```

Declares namespace for all rules in this program. Optional, at most one per program. Must appear before USE declarations.

### USE Imports

```
USE acme.common                       -- Glob import: all exported rules
USE acme.common { control, reachable } -- Selective import: named rules only
```

Imported rules are available for IS references. Qualified names resolved during compilation.

### Qualified Names

```
reachable                    -- Simple name
acme.compliance.control      -- Qualified name
a.b.c.my_rule                -- Deep nesting
```

---

## 14. Configuration Reference

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `max_iterations` | `usize` | `1000` | Max fixpoint iterations per recursive stratum |
| `timeout` | `Duration` | `300s` | Overall evaluation timeout |
| `allow_partial` | `bool` | `false` | Return the partial `LocyResult` (with `incomplete` populated) instead of `UniError::LocyIncomplete` when `timeout` or `max_iterations` is exhausted |
| `max_explain_depth` | `usize` | `100` | Max recursion depth for EXPLAIN trees |
| `max_slg_depth` | `usize` | `1000` | Max recursion depth for SLG resolution (QUERY) |
| `max_abduce_candidates` | `usize` | `20` | Max candidates generated during ABDUCE |
| `max_abduce_results` | `usize` | `10` | Max validated ABDUCE results |
| `max_derived_bytes` | `usize` | `256 MiB` | Max bytes of derived facts per relation |
| `deterministic_best_by` | `bool` | `true` | BEST BY uses secondary sort for deterministic ties |
| `strict_probability_domain` | `bool` | `false` | Reject MNOR/MPROD inputs outside [0,1] (else clamp) |
| `probability_epsilon` | `f64` | `1e-15` | MPROD switches to log-space below this threshold |
| `exact_probability` | `bool` | `false` | BDD-based exact inference for shared-proof groups |
| `max_bdd_variables` | `usize` | `1000` | Per-group BDD variable cap before fallback |
| `top_k_proofs` | `usize` | `0` | Retain at most k proofs per fact (0 = unlimited) |
| `top_k_proofs_training` | `Option<usize>` | `None` | Override top_k_proofs during training |
| `params` | `HashMap<String, Value>` | `{}` | Parameter bindings for `$name` references |

Setting configuration:

```python
# Python -- individual overrides
session.locy_with(program).timeout(60.0).max_iterations(500).run()

# Python -- full config override
session.locy_with(program).with_config({
    "exact_probability": True,
    "strict_probability_domain": True,
    "max_bdd_variables": 2000,
}).run()
```

```rust
// Rust -- full config override
let config = LocyConfig {
    exact_probability: true,
    strict_probability_domain: true,
    max_bdd_variables: 2000,
    ..Default::default()
};
session.locy_with(program).with_config(config).run().await?;
```

---

## 15. Complete Examples

### Transitive Closure

```
CREATE RULE reachable AS
    MATCH (a:Node)-[:EDGE]->(b:Node)
    YIELD KEY a, KEY b

CREATE RULE reachable AS
    MATCH (a:Node)-[:EDGE]->(mid:Node)
    WHERE mid IS reachable TO b
    YIELD KEY a, KEY b
```

### Risk Propagation with MNOR

```
CREATE RULE supplier_risk AS
    MATCH (s:Supplier)-[:HAS_SIGNAL]->(sig:Signal)
    FOLD risk = MNOR(sig.risk)
    YIELD KEY s, risk PROB

CREATE RULE product_exposure AS
    MATCH (s:Supplier)-[:SUPPLIES]->(p:Product)
    WHERE s IS supplier_risk
    FOLD exposure = MNOR(risk)
    YIELD KEY p, exposure PROB

CREATE RULE safe_product AS
    MATCH (p:Product)
    WHERE p IS NOT product_exposure
    YIELD KEY p, 1.0 AS safety PROB
```

Result: `supplier_risk(S1) = 1-(1-0.3)(1-0.5) = 0.65`, `product_exposure(Widget) = 1-(1-0.65)(1-0.2) = 0.72`, `safe_product(Widget) = 1-0.72 = 0.28`.

### RBAC with Priorities

```
CREATE RULE access PRIORITY 0 AS
    MATCH (u:User)-[:MEMBER_OF]->(g:Group {name: 'public'})
    YIELD KEY u, 'allow' AS decision

CREATE RULE access PRIORITY 50 AS
    MATCH (u:User)-[:MEMBER_OF]->(g:Group)
    WHERE g IS restricted_group
    YIELD KEY u, 'deny' AS decision

CREATE RULE access PRIORITY 100 AS
    MATCH (u:User)-[:HAS_ROLE]->(r:Role {name: 'admin'})
    YIELD KEY u, 'allow' AS decision

CREATE RULE restricted_group AS
    MATCH (g:Group) WHERE g.classification = 'restricted'
    YIELD KEY g
```

### Supply Chain Provenance (DERIVE + NEW Nodes)

```
CREATE RULE infer_categories AS
    MATCH (p:Product) WHERE p.price > 100
    DERIVE (NEW cat:Category {name: 'Premium'})<-[:BELONGS_TO]-(p)

DERIVE infer_categories
```

### What-If Analysis with ASSUME

```
CREATE RULE reachable AS
    MATCH (a:Server)-[:CONNECTS_TO]->(b:Server)
    YIELD KEY a, KEY b

CREATE RULE reachable AS
    MATCH (a:Server)-[:CONNECTS_TO]->(mid:Server)
    WHERE mid IS reachable TO b
    YIELD KEY a, KEY b

ASSUME {
    MATCH (a:Server {name: 'Gateway'})-[r:CONNECTS_TO]->(b:Server {name: 'DB'})
    DELETE r
}
THEN {
    QUERY reachable WHERE a.name = 'WebApp' AND b.name = 'DB'
    RETURN b.name
}

ABDUCE NOT reachable WHERE a.name = 'WebApp' AND b.name = 'DB'
RETURN modifications
```

---

## Anti-Patterns and Gotchas

| Anti-Pattern | Symptom | Fix |
|-------------|---------|-----|
| Missing KEY columns in recursive rules | Exponential growth, then `LocyIncomplete` (reason `IterationLimit`) | Add KEY columns for fact identity |
| SUM/AVG in recursion | `NonMonotonicInRecursion` | Use MSUM or restructure |
| COUNT in recursion over a cyclic/unbounded fact set | Monotone but unbounded and loop-tracked: can run to `max_iterations`, then `LocyIncomplete` | Bound the recursion, or accept the cap via `allow_partial` |
| `prev.field` in base case | `PrevInBaseCase` | Use literal values in base case ALONG |
| Cyclic negation (A IS NOT B, B IS NOT A) | `CyclicNegation` | Ensure negation flows in one direction |
| BEST BY + `M*` lattice fold (MSUM/MMAX/MMIN/MCOUNT/MNOR/MPROD) | `BestByWithMonotonicFold` | Use BEST BY with ALONG, or FOLD without BEST BY |
| Recursive rollup disagrees with its own children | Pre-#162 releases: a node with ≥2 equal-valued children lost all but one, optimistically, and the error propagated to every ancestor | Upgrade. Invariant to check: an assembly's value must equal the fold of its children's values |
| Ignoring `SharedProbabilisticDependency` warning | Silently wrong probabilities | Enable `exact_probability` or review rule logic |
| ALONG without BEST BY in recursive rules | All path variants retained (exponential) | Add BEST BY to prune dominated paths |
| Command WHERE using DataFusion-only functions | Silent eval failure or limited behavior | Move complex filters into rule MATCH/WHERE |
