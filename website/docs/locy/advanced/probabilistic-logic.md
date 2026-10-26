# Probabilistic Logic (MNOR / MPROD / PROB)

## The Problem

Standard fold operators break down for probability combination:

- **MSUM** overestimates — summing probabilities can exceed 1.0, producing invalid results.
- **MMAX** underestimates — taking the maximum ignores the reinforcement from multiple independent paths.

When your domain is probabilities (values in [0, 1]), you need operators that respect probability semantics.

## MNOR — Noisy-OR

**Formula:** `P = 1 − ∏(1 − pᵢ)`

**Semantics:** "Any independent cause can trigger the effect." Each input is an independent probability that some cause produces the effect. The combined probability is: what is the chance that *at least one* cause fires?

| Property       | Value |
|----------------|-------|
| Identity       | `0.0` (no causes → no effect) |
| Direction      | Non-decreasing (more causes → higher probability) |
| Domain         | Inputs clamped to [0, 1] |

### Worked Example

Three quality signals flag a component as defective with probabilities 0.3, 0.2, and 0.1:

```
P(defective) = 1 − (1 − 0.3)(1 − 0.2)(1 − 0.1)
             = 1 − 0.7 × 0.8 × 0.9
             = 1 − 0.504
             = 0.496
```

In Locy:

```locy
CREATE RULE component_failure_risk AS
MATCH (c:Component)-[:HAS_SIGNAL]->(s:QualitySignal)
FOLD risk = MNOR(1.0 - s.pass_rate)
YIELD KEY c, risk
```

With three signals having pass rates 0.7, 0.8, and 0.9 (failure probabilities 0.3, 0.2, 0.1), the fold computes `1 − (1−0.3)(1−0.2)(1−0.1) = 0.496`.

## MPROD — Product

**Formula:** `P = ∏ pᵢ`

**Semantics:** "All conditions must hold simultaneously." Each input is an independent probability of success. The combined probability is: what is the chance that *every* condition holds?

| Property       | Value |
|----------------|-------|
| Identity       | `1.0` (no conditions → certain) |
| Direction      | Non-increasing (more conditions → lower probability) |
| Domain         | Inputs clamped to [0, 1] |

### Underflow Protection

When many small probabilities are multiplied, the product can underflow to zero. MPROD automatically switches to log-space accumulation (`exp(∑ ln(pᵢ))`) when the running product drops below `1e-15`. If any input is exactly `0.0`, the result is immediately `0.0`.

### Worked Example

A vendor supplies three components with individual reliabilities 0.95, 0.90, and 0.85:

```
P(all reliable) = 0.95 × 0.90 × 0.85
                = 0.72675
```

In Locy:

```locy
CREATE RULE vendor_reliability AS
MATCH (v:Vendor)-[:SUPPLIES]->(c:Component)
WHERE c IS component_failure_risk
FOLD reliability = MPROD(1.0 - component_failure_risk.risk)
YIELD KEY v, reliability
```

!!! note "MPROD multiplies over *matched* rows, not over an expected set"
    MPROD takes the product of the rows that actually match the clause body for each
    group. A condition that produces **no matching row** is simply *absent from the
    product* — it does **not** force the result to zero. So MPROD alone does not encode
    "every required element must be present."

    To express a true all-elements conjunction — e.g. a patent claim is infringed only
    when a product maps **every** one of its elements — pair MPROD with an explicit
    completeness guard: count the matched elements and the total elements and keep only
    groups where they are equal, or exclude incomplete groups with an `IS NOT` complement.

    ```locy
    // total elements per claim
    CREATE RULE claim_size AS
      MATCH (c:Claim) WHERE c IS claim_elements TO ce
      FOLD n_total = COUNT(ce) YIELD KEY c, n_total

    // product over the elements this product actually maps
    CREATE RULE pc_mapped AS
      MATCH (p:Product), (c:Claim)
      WHERE c IS claim_elements TO ce, p IS element_mapped TO ce
      FOLD n_mapped = COUNT(ce), infringement = MPROD(mapping_conf)
      YIELD KEY p, KEY c, n_mapped, infringement

    // keep only fully-mapped (p, c) pairs — the all-elements guard
    CREATE RULE claim_infringed AS
      MATCH (p:Product), (c:Claim)
      WHERE (p, c) IS pc_mapped, c IS claim_size TO n_total, n_mapped = n_total
      YIELD KEY p, KEY c, infringement
    ```

## When to Use Which

| Operator | Semantics | Direction | Use When |
|----------|-----------|-----------|----------|
| MSUM     | Sum       | ↑ | Non-negative counts or weights |
| MMAX     | Maximum   | ↑ | Worst-case / dominant signal |
| MMIN     | Minimum   | ↓ | Best-case / bottleneck |
| MNOR     | Noisy-OR  | ↑ | Independent OR-causes (probabilities) |
| MPROD    | Product   | ↓ | Independent AND-conditions (probabilities) |

**Rule of thumb:** If you're asking "could *any* of these cause X?" use MNOR. If you're asking "do *all* of these hold?" use MPROD.

## Compiler Guardrails

**ProbabilityDomainViolation warning.** When MNOR or MPROD is used with non-literal arguments, the compiler emits a warning reminding you that inputs should be valid probabilities in [0, 1]. Literal constants (e.g., `MNOR(0.3)`) are checked at compile time and do not trigger the warning.

**BEST BY rejection.** Declared lattice folds — MNOR and MPROD among them — are incompatible with `BEST BY` witness selection. The compiler rejects this combination with a `BestByWithMonotonicFold` error.

**Input clamping.** At runtime, values outside [0, 1] are clamped before computation. This prevents NaN or negative results from bad data, but you should fix the upstream source rather than relying on clamping.

## PROB Columns and Probabilistic Negation

`PROB` marks one output column in a rule as the probability channel for that relation:

```locy
CREATE RULE supplier_risk AS
MATCH (s:Supplier)-[:HAS_SIGNAL]->(sig:Signal)
FOLD risk = MNOR(sig.risk)
YIELD KEY s, risk AS PROB
```

Accepted forms:

- `expr AS PROB`
- `expr AS alias PROB`
- `expr PROB`

If `IS NOT` targets a rule with a `PROB` column, Locy uses probabilistic complement (`1 - p`) instead of Boolean anti-join:

```locy
CREATE RULE usable_supplier AS
MATCH (s:Supplier)
WHERE s IS NOT supplier_risk
YIELD KEY s, 1.0 AS confidence PROB
```

If the referenced rule has no `PROB` column, `IS NOT` keeps standard Boolean semantics.

`1 - p` treats the negated fact as independent of the rest of the row. In exact mode (below) the
row instead conjoins the *negation* of the negated fact's formula, so a row that needs a path and
the absence of an edge on that path gets the right probability.

### Positive `IS` References to a `PROB` Rule

A positive reference is the mirror image: the row holds only when the referenced fact does, so its
probability is multiplied into the row's `PROB` value (`P(r) + P(NOT r) = 1`).

```locy
CREATE RULE reach AS
MATCH (a:Node)-[e:LINK]->(b:Node)
FOLD prob = MNOR(e.p)
YIELD KEY a, KEY b, prob

CREATE RULE reach AS
MATCH (a:Node)-[e:LINK]->(mid:Node) WHERE mid IS reach TO b
FOLD prob = MNOR(e.p)
YIELD KEY a, KEY b, prob
```

Over `s -0.7-> t` and `s -0.3-> m -0.5-> t`, the recursive row is `0.3 × reach(m, t) = 0.15`, and
`reach(s, t) = 1 - (1 - 0.7)(1 - 0.15) = 0.745` — the probability that `t` is reachable when every
edge is present independently with its `p`.

**Count-once rule.** A clause whose `PROB` expression (the `FOLD` input, or the `PROB` `YIELD` item)
already reads the referenced probability is not charged for it twice:

| The `PROB` expression reads `p` | Multiplied in |
|---|---|
| not at all: `MNOR(e.p)` | yes |
| as one product factor: `MNOR(p)`, `MNOR(e.p * p)` | no — already counted |
| any other way: `MNOR(1.0 - p)`, `MNOR(p * p)` | no — never counted twice; a `ProbabilityReadNotFactor` warning is raised |

!!! warning "Breaking change"
    Before this release a positive reference was a Boolean filter: `reach(s, t)` above was
    `1 - (1 - 0.7)(1 - 0.3) = 0.79`, treating `reach(m, t)` as certain. Programs that relied on
    that and did not read the referenced probability see lower values now.

## Shared-Proof Detection and Exact Mode

MNOR and MPROD assume independent derivations. When two proofs of one fact rest on the same evidence — the same edge, the same referenced fact, or the fact itself through a cycle — or one row's positive part and an `IS NOT` rest on the same edge, the runtime emits `SharedProbabilisticDependency`. Nodes in common are not evidence in common: the two proofs of `reach(s, t)` above share no edge, so the independence value 0.745 is already exact and nothing is reported.

Set `exact_probability = true` in `LocyConfig` to compute those facts exactly, under possible-worlds semantics: each fact is the OR (MNOR) or AND (MPROD) of its rows, each row is its own event AND every fact it references AND NOT every fact its `IS NOT` matched, and the formula is evaluated with a BDD. On `A -0.5-> B -0.4-> C -0.6-> D` plus `A -0.3-> C`, both proofs of `reach(A, D)` need `C -> D`; exact mode gives `0.6 × (1 - 0.7 × 0.8) = 0.264` where independence gives 0.2784. When no fact is shared, the two modes agree.

Exact mode reports a shared dependency it resolved as the informational status
`SharedDependencyResolved` (`inference_mode: exact`, `resolution: completed`) instead of the
`SharedProbabilisticDependency` warning, whose "may overestimate" premise no longer holds. The
warning stays when a group falls back (`resolution: fallback`, `reason:
bdd_variable_limit_exceeded`, with the configured and effective limits) and in default mode
(`inference_mode: approximate`, `resolution: not_attempted`).

With negation: over `s -0.7-> t`, `s -0.3-> m -0.5-> t`, a rule
`MATCH (a), (m) WHERE a IS reach TO b, m IS NOT link TO b` (with `link(a, b)` the noisy-OR of the
`a -> b` edges) holds for `(s, m, t)` when `t` is reachable from `s` without the edge `m -> t`:
exact mode gives `0.7 × 0.5 = 0.35`, default mode `0.745 × (1 - 0.5) = 0.3725` with a
`SharedProbabilisticDependency` warning.

A fact of a PROB rule without a FOLD (`YIELD KEY a, KEY b, e.p AS p PROB`) is, in exact mode, the
formula of its own row: the row's event, the facts it references and the negation of the facts it
negates (facts sharing a KEY are alternatives). A rule referencing it, positively or with `IS NOT`,
composes that formula; it does not read the fact as an independent variable. The fact's own
`PROB` value, queried directly, is that formula's probability in exact mode: a row derived
more than one way, or whose referenced facts share evidence with it, is recomputed, and
`SharedProbabilisticDependency` reports it in default mode. For example, `MATCH (a)-[e]->(b) WHERE
a IS reach TO b YIELD KEY a, KEY b, e.p AS p PROB` is `e.p` (the edge implies the reachability),
not `e.p × reach(a, b)`. Over the same graph,
with `edge(a, b)` such a rule, `reach(s, t) AND NOT edge(s, t)` is
`(X ∨ (Y ∧ Z)) ∧ ¬X = 0.3 × 0.3 × 0.5 = 0.045`.

Two important fallbacks remain:

- `BddLimitExceeded`: the group exceeded `max_bdd_variables`, so Locy falls back to the independence result for that key group.
- `CrossGroupCorrelationNotExact`: shared evidence spans multiple aggregate groups; each group is exact internally, but cross-group correlation is still approximate.

Rows from approximate groups are marked with `_approximate = true`, and Rust `LocyResult` values also expose `approximate_groups`.

### TopKProofs Semiring

`LocyConfig.semiring = TopKProofs(k)` evaluates each `MNOR` fact over its `k` most probable rows (its proofs), exactly: each proof is its own event AND the facts it references, as in exact mode, so proofs sharing an edge — even one below a referenced fact — count it once. With `k` at least a fact's number of rows the value is the possible-worlds probability; with fewer it is the probability of the kept proofs. On the `A … D` graph above, `TopKProofs(8)` gives `reach(A, D) = 0.264` and `TopKProofs(1)` keeps the 0.18 proof through `C`. A fact with at most `k` rows and no shared event keeps its default value, which is already exact. A referenced fact of a rule without a `PROB` column is certain and adds no event: two edges into one hub both referencing `hub_score(h)` stay independent (`1 - 0.3 × 0.4 = 0.88`).

!!! warning "Behavior change"
    `TopKProofs` used to treat the referenced facts of a row as its only events, weighted by the
    largest row, and to resolve references through the subject variable, which a rule that does not
    yield it (`mid IS reach TO b`) dropped. The hub example above gave 0.7, and `reach(A, D)` gave
    the independence value 0.2784.

`EXPLAIN RULE` shows, under each derivation, the facts it references: every positive reference,
including references to rules without a `PROB` column and references from PROB rules without a
FOLD. A fact of a rule without a `PROB` column is certain, so its derivations show
`proof_probability` 1.0. A derivation of such a rule that references PROB facts weighs the product
of their probabilities.

### MaxMinProb Semiring (Viterbi / Fuzzy)

`LocyConfig.semiring = MaxMinProb` changes the aggregate math: `MNOR` becomes `max(pᵢ)` and `MPROD` becomes `min(pᵢ)`. Useful when the values aren't probabilities (e.g. fuzzy-set memberships) and you want monotone composition without the independence assumption. Locy emits `FuzzyNotProbabilistic` on any rule that also declares `PROB` under this semiring, since you've opted out of probability semantics.

## Relevant Config Knobs

| Field | Default | Effect |
|-------|---------|--------|
| `strict_probability_domain` | `false` | Reject out-of-range values instead of clamping |
| `probability_epsilon` | `1e-15` | MPROD threshold for switching to log-space accumulation |
| `exact_probability` | `false` | Enable BDD-based exact evaluation for shared-proof groups |
| `max_bdd_variables` | `1000` | Cap per-group BDD complexity before fallback |

## Combining with similar_to

`similar_to()` returns scores in [0, 1], making its output natural probability input for MNOR and MPROD:

```locy
// Combine semantic relevance signals with noisy-OR
CREATE RULE evidence_strength AS
MATCH (claim:Claim)-[:SUPPORTED_BY]->(doc:Document)
FOLD strength = MNOR(similar_to(doc.embedding, claim.text))
YIELD KEY claim, strength

// Joint confidence across required criteria
CREATE RULE joint_match AS
MATCH (job:Job)-[:REQUIRES]->(skill:Skill)<-[:HAS]-(candidate:Candidate)
FOLD fit = MPROD(similar_to(skill.embedding, candidate.resume))
YIELD KEY job, KEY candidate, fit
```

## Related

- [ALONG / FOLD / BEST BY](along-fold-bestby.md) — full FOLD documentation
- [Rule Semantics](../rule-semantics.md) — monotonicity and stratification
- [Vector Search guide](../../guides/vector-search.md#similar_to-expression-function) — `similar_to()` documentation
- [Neural Predicates](neural-predicates.md) — `CREATE MODEL` / `CALIBRATE` / `VALIDATE` and the `NeuralProvenance` audit trail
