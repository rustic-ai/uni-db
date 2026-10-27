# Constrained hypothetical queries for #317

Status: revised design. The earlier request-scoped business-action catalog is
withdrawn, as are the conversational YAML and `CREATE ACTION` suggestions.
There is no proposed `Action` database type, registry or second expression DSL.

Issue: <https://github.com/rustic-ai/uni-db/issues/317>.

## The requirement

Given a goal and a finite set of permitted choices, determine which choices
make the goal hold, explain the supporting evidence, compare their business
measures, and leave the original state unchanged. Eligibility and effects must
be supplied by the application: an inference engine cannot learn that an
approved quote permits a price change merely from `price > 600`.

Separate three things the previous design bundled together:

| Question | Existing representation |
| --- | --- |
| Which choices are permitted? | Ordinary graph facts and Locy rules (`MATCH`, `WHERE`, `YIELD`) |
| What would choosing one change? | Explicit Cypher mutations inside `ASSUME` |
| Would the changed state satisfy the goal? | `QUERY` over ordinary Locy rules in the `THEN` body |
| How should acceptable results compare? | Ordinary numeric/unit columns and query ordering after validation |

The database owns rule evaluation, hypothetical isolation, query execution and
resource limits. The application owns what approval means, which properties
change together, what feasibility means, and which measure it wants to rank.
A decision name such as `accept_quote` is an ordinary result value, not a new
kind of executable database object.

## Start with existing language, not a new abstraction

For a simple condition that can be evaluated directly against quote properties,
a normal query is sufficient. Hypothetical execution is useful when the choice
must be propagated through existing rules over the changed graph: commitments,
delivery dependencies, inventory, recursive constraints, and so on. Reuse those
rules instead of duplicating their conditions in a candidate filter.

An existing-language implementation is executable in
[`locy_constrained_scenarios.py`](../../bindings/uni-db/examples/locy_constrained_scenarios.py).
It uses one pinned scratch transaction and runs an independent `ASSUME` for
each eligible row. The host supplies the loop, the overall deadline, candidate
cap and report metadata. These are not claimed as existing native ABDUCE
features.

### Eligibility is a relation

```locy
CREATE RULE eligible_quote AS
MATCH (q:Quote)-[:APPROVED_FOR]->(o:Offer)
WHERE o.id = $offer_id AND q.active = true
  AND q.qty >= o.qty AND q.currency = o.currency
  AND q.unit_cost_cents > 0
YIELD KEY o, KEY q, 'accept_quote' AS decision,
      q.unit_cost_cents * o.qty AS purchase_total_cents

QUERY eligible_quote
RETURN id(o) AS offer_vid, id(q) AS quote_vid,
       q.id AS quote_id, decision, purchase_total_cents
```

The explicit approval edge and these conditions define this application's
permitted choices. They are domain rules, not built-in database policy. The
rule does not enumerate arbitrary new prices or edits to identifiers.

### Effects are a hypothetical Cypher block

Define the goal using the existing offer state:

```locy
CREATE RULE feasible AS
MATCH (o:Offer)
WHERE o.id = $offer_id
  AND o.unit_cost_cents * o.qty <= o.budget_cents
  AND o.delivery_days <= o.deadline_days
YIELD KEY o
```

For each eligible row, pass its entity identities as parameters to:

```locy
ASSUME {
  MATCH (o:Offer), (q:Quote)
  WHERE id(o) = $offer_vid AND id(q) = $quote_vid
  SET o.unit_cost_cents = q.unit_cost_cents,
      o.delivery_days = q.delivery_days
} THEN {
  QUERY feasible WHERE id(o) = $offer_vid
  RETURN o.id AS offer_id, o.unit_cost_cents AS unit_cost_cents,
         o.delivery_days AS delivery_days
}
```

The example includes the goal definition in the scenario program. Parameter
identities come only from the eligibility query in the same pinned baseline;
eligibility therefore does not have to be copied into a second condition list.
This is a closed search implemented by the caller, not a public function that
promises any arbitrary caller-supplied ID is an approved choice.

Choosing a quote changes both price and delivery terms together. Merely changing
price while retaining the old delivery date would evaluate a different business
choice. Several explicit changes may belong to one hypothetical candidate;
that does not require an engine-level business-operation type.

One ASSUME block matching *all* quotes would apply their changes in one state.
It would not compare independent scenarios and could depend on row order. Each
candidate must start from the identical baseline.

### Verified acceptance case

OFF3 requests 120 units, has a budget of 72,000 USD cents and a five-day deadline.
Its current price is 700 cents and delivery is eight days, so the goal fails.

| Quote | Eligibility | Hypothetical goal |
| --- | --- | --- |
| CHEAP_LATE: 500 cents, nine days | Eligible | Fails delivery despite costing only 60,000 cents |
| Q1: 590 cents, four days | Eligible | Passes; total is 70,800 cents |
| Zero price, inactive, insufficient quantity, wrong currency | Ineligible | Not simulated |

The checked example confirms Q1 is selected, every scenario restores price 700
and delivery eight in the scratch baseline, reverse evaluation order gives the
same answers, and the persistent database is unchanged after rollback. Ranking
the eligible relation with `LIMIT 1` first would select CHEAP_LATE and miss Q1.
Rank the successful results, not the unvalidated hypotheses.

This was exercised with the installed 4.2.0 native binding; the scenario/rule
runtime was not changed for this demonstration. It is evidence for these
existing constructs, not evidence that a native correlated query is implemented.

## The actual missing native primitive: correlation over hypothetical queries

Current `GoalQuery` produces a relation and current `AssumeBlock` is a command
with nested command results. There is no native operator that binds each row
of the former into an independent evaluation of the latter and returns one
composable result relation. The host loop bridges exactly that gap today.

For native support, extend the composition of **queries and ASSUME**, not the
ontology of the database. The logical operation is:

```text
candidate relation
  -> for each row, capture its typed bindings
  -> independent ASSUME mutation block + goal query
  -> successful rows retaining candidate identity and evidence
  -> ordinary filtering, projection and ordering
```

This is a proposed query-plan capability, not runnable syntax. Prototype a
correlated hypothetical-apply operator using the existing Cypher clause AST,
Locy command compiler and hypothetical runtime. Settle lexical binding and
relation-valued results before proposing grammar. Do not disguise the feature
as a callback taking query strings or introduce a replacement mini-language.

### Required semantics

1. Evaluate the candidate relation against the current baseline once. A
   baseline includes the caller's transaction writes or enclosing ASSUME state.
   Pin it for the whole operation; later external commits cannot alter candidates.
2. Capture each row's typed entity/value bindings. Define lexical scope so an
   inner goal's variables cannot accidentally capture or overwrite a different
   outer entity. Correlation is explicit in the compiled plan.
3. Create an owned child hypothetical state per row, apply the entire existing
   Cypher mutation block, re-derive affected rules and evaluate its body query.
   Preconditions are not re-invented in an engine catalog. The write footprint
   includes every effect; reuse only strata unaffected by it.
4. Discard the child before the next row, on success, error, cancellation and
   future drop. The existing runtime's error safety relies on dropping a whole
   evaluation's adapter; reusing it while catching a candidate error would break
   that argument. Native iteration needs owned child contexts or an equivalent
   guard, with failure-exit tests.
5. Produce a relation retaining outer candidate identities plus projected inner
   goal evidence. A goal with no rows does not produce a successful choice.
   Use ordinary relational keys/projections for multiplicity and deduplication;
   distinguish rejected-goal diagnostics from execution errors.
   Materialize projected values and requested derivation evidence while the
   child still exists. An entity created only in a hypothetical child must not
   escape as a live entity reference later resolved against the baseline;
   its identity is scoped to that scenario. Evidence should distinguish why a
   choice was eligible from why its hypothetical goal held.
6. An incomplete fixpoint is an error by default, not a false goal. In particular,
   interrupted evaluation cannot validate a negated goal. Optional partial search
   reports may contain only candidates with completed goal evaluation and cleanup.
7. Share one deadline, cancellation scope and memory budget across enumeration,
   every mutation/re-evaluation and final ordering. Track enumeration, evaluated
   candidates, successful candidates and stop reason at the query level. A row
   LIMIT alone does not bound the work needed to derive the candidate relation.
   Retained candidate rows, result rows and requested provenance count against
   that memory budget too; a per-child execution limit is insufficient.
8. Keep this operation at command/query composition level initially. Do not put
   hypothetical evaluation into recursive rule bodies: that would introduce a
   new recursion/search semantics rather than merely compose existing queries.

The native representation should be a generic logical operator: input plan,
correlation bindings, mutation AST and body program/query. No field in it needs
names such as quote, receipt, business action, approved price or monetary cost.
Several domain choice types can be expressed as separate query branches with
their own mutation blocks and a common result schema, then combined for ranking.
There is no need for a global operation registry or a business-operation type.

## Costs, identities and execution are separate concerns

Business measures are normal data, such as `purchase_total_cents`, `currency`,
`delivery_days` or `units_moved`. The application must define which measure is
comparable and whether it is measured before or after the hypothetical changes.
The example's quantity is unchanged, so quoted price times baseline quantity
is the resulting purchase total. Other models may need to project their costs
from the inner goal query instead.

Keep different currencies or measures separate unless the application explicitly
models conversion/comparison. Ordinary query ordering is not unit typechecking;
this proposal does not add a financial-units subsystem. ABDUCE's fixed edit cost
0.5 is not a business price and must not be used for this ranking.

No identity property appears in the explicit mutation block, so this search
cannot generate an identifier edit. That is a property of its declared effect
space, not a new database-wide immutability constraint. If the product needs
hard immutable-property enforcement for *all* writes, design it as a general
schema/authorization capability separately.

A result is evidence for one snapshot, not authority to execute procurement or
any external operation. Applying a selected change later requires rechecking
eligibility in a current transaction and normal conflict handling. Real-world
side effects are outside hypothetical graph evaluation.

## Relationship to ABDUCE and bounds of the solution

Unrestricted ABDUCE asks the engine to invent graph edits that achieve a goal.
This requirement instead supplies permitted choices and their effects and asks
which achieve the goal. Both can reuse hypothetical validation, but they have
different candidate sources. Do not run unrestricted ABDUCE then discard its
unwanted edits: its cap may already have excluded the permissible solution.

Keep existing ABDUCE behavior compatible while introducing composable constrained
what-if queries. Only add an ABDUCE surface over explicit candidate scenarios if
it adds a distinct inference capability beyond that composition. The price
example alone does not justify a new abductive planner or operation language.

An exhaustive result is best only within the explicitly supplied candidate
relation and selected objective. A capped prefix is merely best among evaluated
rows, with an incomplete-search flag. Neither proves global minimality over all
possible graph edits. An empty permissible relation means no permitted choice
was supplied; it does not prove the goal is logically impossible.

One candidate may contain several coordinated edits. Searching arbitrary
sequences or combinations of choices is a separate planning problem. A finite
bundle may be explicitly modeled as an input row, but automatic plan synthesis
is not included in the first correlated-query primitive.

If #317 requires discovering combinations such as accepting a quote *and*
reallocating stock, correlation alone does not meet that requirement. That needs
an explicit second design: bounded state/plan expansion, eligibility rechecked
in each intermediate state, ordering/conflict semantics, and an objective over
the complete plan. Do not claim the finite-choice implementation solves it.
Likewise, evaluate the unmodified goal first when reporting remediation: if it
already holds, report that no change is required instead of implying a quote
must be accepted. Applications may still request alternatives for optimization.

## Delivery and acceptance

1. Use the executable existing-language recipe to establish the contract now.
   It already demonstrates eligibility, coupled effects, full-goal validation,
   business ranking and same-baseline restoration without new engine types.
2. If native #317 work proceeds, prototype the correlated logical operator and
   its scoped bindings/results first. Review a syntax change only after the
   quote example and a second graph-propagation example run through that plan.
3. Test competing eligible choices, no eligible choices, an already-satisfied
   goal, identity collisions in display keys, duplicate evidence paths, multiple
   effects, nested ASSUME, later external commits, error/cancellation/drop,
   enumeration/validation limits, and incomplete negated goals. Verify Rust and
   sync/async Python result parity. A caught candidate failure must not contaminate
   the next scenario.

Decision: withdraw the catalog/Action abstraction. Keep domain declarations in
normal Locy rules and Cypher hypothetical blocks. The justified native feature
is correlated, composable hypothetical query evaluation; neither a domain-aware
DB nor a separate business-process framework is required.
