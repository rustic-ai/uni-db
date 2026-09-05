# Unifying the two entity encodings

Status: **The query layer is unified as of 2026-09-05.** Every decode site in
`uni-query`, including the two the mutation pipeline was thought to require,
returns an entity in its native form. The full suite and the openCypher TCK are
both green. What remains is `uni-store`'s own decoder and step 4 — making the map
form unrepresentable — both recorded at the end.

The history below is kept in order, including an attempt that failed and the two
occasions the integration suite alone gave the wrong answer, because the storage
boundary will hit the same wall.

## The problem in one line

A vertex or an edge exists in two forms — native `Value::Node` / `Value::Edge`,
and a `Value::Map` carrying `_vid` / `_eid` — and nothing decides which a given
path produces.

Every identity defect closed under #234 was the same shape: a site that handled
one form and answered "not an entity" for the other, which the caller then read
as "not equal", "not a duplicate", "no such row", or "null".

## What is closed

- **One accessor.** `Value::entity_ref` / `entity_vid` / `entity_eid`, and
  `entity_ref_from_map` for callers holding a bare map. Covers both forms, both
  kinds, every id spelling (`_vid`/`_eid`/`_id`/`vid`/`eid`), the serde string
  forms (`"Vid(7)"`), and Locy's `_src_vid`/`_dst_vid` endpoint vocabulary. It
  also refuses to read an edge as the vertex of the same number, which the
  original `entity_vid` did.
- **The comparison boundary.** `Value`'s `PartialEq` and `Hash` compare entities
  by identity, so `==`, `HashSet<Value>` and anything built on them are correct
  without knowing the accessor exists.
- **The byte boundary, where it was reachable.** `Value::canonical_entity`
  rewrites an entity map into its native form; `UNWIND` runs list elements
  through it before encoding, so one entity has one encoding in that column.
- **A CI ratchet.** `arch_entity_identity` holds the line at 10 hand-rolled
  reads across 4 files, each audited and justified in place, and fails on a new
  site, a new file, or a budget left too generous.

## Why the comparison fix is not sufficient

An Arrow column holds **encoded bytes**. A group-by, a join key and a
`DISTINCT` compare those bytes and never see a `Value`, so identity-aware
`PartialEq` cannot reach them. That is why `RETURN DISTINCT n` counted one vertex
twice even after the equality work: the column held both encodings of it.

Canonicalising at `UNWIND` closed that instance. It does not generalise on its
own — it fixes the column it is applied to, and nothing prevents the next mixed
column.

## The attempt that failed, and why it matters

The obvious structural fix is to canonicalise at the **decode boundary**: when an
Arrow struct representing an entity becomes a `Value`, produce `Value::Node` /
`Value::Edge` rather than `Value::Map`. Two to four sites, rather than the 75
`encode` call sites, and it makes the second form stop existing at its source.

Tried, on the query-side struct decoder in `df_graph/unwind.rs`. Result:

```
2768 tests run: 2750 passed, 18 failed
```

All 18 are CALL-subquery round-trips in `cypher_write::set_projection_test`, and
all fail the same way:

```
Failed to reconstruct batches: Invalid argument error:
Column 'n' is declared as non-nullable but contains null values
```

**The map form is load-bearing at the Arrow boundary.** An entity column is
declared with a struct type, and a row that leaves as a struct must come back as
one. Decoding it to a native entity breaks the return trip: the reconstruction
path has no arm for a `Value::Node`, so the column reconstructs as null.

This is worth stating plainly because the decode-side change *looks* like a
one-line structural win, and it is not one. The change was reverted.

## Step 1 progress — measured, 2026-09-05

Re-ran the decode-side change with step 1 partly done. **18 failures fell to 3.**

Two reconstruction sites did not accept a native entity, both now fixed and
covered by direct unit tests:

- `value_to_scalar` sent `Value::Node` / `Edge` / `Path` into its JSON catch-all,
  putting a JSON blob in a `LargeBinary` column that every consumer decodes as
  CypherValue. It decoded to nothing, so the column came back null. The map form
  took the `Value::Map` arm and survived — the asymmetry again. Entities now
  encode as CypherValue, exactly as `Value::Vector` beside them already did.
- `sync_dotted_columns` copied `{var}.{prop}` only from a `Value::Map`, leaving
  every dotted column of a native entity unwritten, so a non-nullable
  `{var}._vid` failed reconstruction outright. It now reads through
  `Value::entity_property`, which answers system fields from the entity itself
  and everything else from its properties.

**The 3 that remain** are all `set_projection_test` CALL-subquery round-trips
(`l2q`, `l2r`, `l2s` — set-then-return shapes), failing with

    Arrow error: Column 'n._vid' is declared as non-nullable but contains null

### The third builder, and why it is not the defect

Located by marking each candidate's error text: `concat_column_arrays` in
`df_graph/apply.rs`, the Apply/CALL result assembler. Two further Map-only sites
were found and fixed on the way there, both now unit-tested:

- `rows_to_batch` read every column flat by name, so a dotted `{var}.{prop}` of a
  natively-encoded entity — which has no such column — became null. It now
  resolves through a new `row_column` helper that derives the field from the
  entity bound to the base variable.
- The unit-subquery refresh path *had* `Value::Node` / `Value::Edge` arms, but
  read only their `.properties`, so a system field like `_vid` resolved to `None`
  and the column silently kept its stale input value. It now goes through
  `Value::entity_property`.

Neither closed the three tests, and instrumenting `concat_column_arrays` explains
why: **the null arrives in its input.** The assembler is reached through the
pass-through arms (`column_arrays[col_idx].push(arr.clone())`), copying an input
column that is already null — so it is propagating the defect, not creating it.

That relocates the remaining step-1 work: it is **upstream of Apply**, wherever
the child plan materialises `{var}._vid` for a natively-encoded entity, and not
in any batch builder. Worth stating because three builders have now been checked
and each turned out to be a carrier.

Both fixes are kept without the decode-side change, since each is a real defect
in its own right; neither is exercised by the current suite, which is expected —
step 1 exists to make step 2 possible, and until step 2 lands no native entity
reaches these paths. That is why they carry direct unit tests rather than relying
on end-to-end coverage.

## What unification actually requires, in order

1. **Teach the batch-reconstruction path to accept a native entity.** Anywhere a
   `Value` is written back into a declared struct column, a `Value::Node` /
   `Value::Edge` must produce the same struct a `Value::Map` does. This is the
   blocker, and it is where the work starts — not at the decoder.
2. **Then** canonicalise at the decode boundary, so the map form stops being
   produced. The 18 tests above become the acceptance criterion.
3. **Then** the remaining ratchet entries can be revisited: several exist only
   because a map may or may not be an entity, a question that stops arising once
   only one form reaches them.
4. **Only then** consider making the second form unrepresentable — a private
   constructor, or removing the map arm from the entity accessors — so a new site
   cannot reintroduce it.

Doing (2) before (1) is the failure recorded above.

## Outcome — 2026-09-05

Steps 1 and 2 are **done for the query-side struct decoder**. It now returns
`Value::Node` / `Value::Edge`, and the full suite plus the TCK are green with it
in place.

Step 1 took six Map-only sites, found one at a time by re-running step 2 and
following each failure. Three were batch builders and three were readers:

| site | what it did with a native entity |
|---|---|
| `value_to_scalar` | JSON-encoded it into a CypherValue column — decoded to nothing |
| `sync_dotted_columns` | skipped it, leaving `{var}._vid` unwritten |
| `apply::rows_to_batch` | read every column flat; a dotted column does not exist |
| `apply` unit-subquery refresh | read `.properties` only, so `_vid` kept a stale value |
| `apply` kept-input override | read `.properties` only, writing `Null` into a non-nullable column |
| `keys()` — **two** implementations | returned an empty list, and no rows at all |

The last is the one worth remembering: `keys()` existed twice, as a UDF and again
inside `UNWIND`, and both knew only the map form. Fixing one left the TCK red.
They now share `Value::property_names`, as the other readers share
`Value::entity_property`.

**Every one of these was a latent wrong answer** that no test could reach while
the native form never got that far. Two were caught by existing tests and three
by the TCK — the split was hiding them, exactly as predicted above.

### All three query-side decoders converted

`df_graph/unwind.rs::arrow_to_json_value`, `executor/read.rs::arrow_to_value` and
`df_graph/similar_to_expr.rs::arrow_to_value_at` now all return the native form.

The second and third cost **nothing** — zero test failures, TCK unchanged. That
is the shape of this work: the first conversion pays for the readers, and every
one after it is free. Six readers were fixed for the first decoder; the other two
needed none.

`uni-store`'s `arrow_convert::arrow_to_value` is deliberately **not** converted.
It is the storage layer's decoder, with its own contract and its own callers
inside `uni-store`; the query layer converts at its own boundary instead, which
is why `read.rs` wraps it rather than changing it.

### The call sites, 2026-09-05

Eleven decode sites across seven files now return the native form: both in
`unwind.rs`, three in `read.rs` (the wrapper plus two internal), both in
`locy_eval.rs`, and one each in `similar_to_expr.rs`, `df_udfs_plugin.rs`,
`endpoint_hydrate.rs` and `locy_fixpoint.rs`.

Cost of the whole batch: **one failure**, `a5_fork_edge_merge_on_match_inherited`
— an edge `MERGE … ON MATCH SET` that silently did not apply. Bisected to a
single site, and it is not a bug but a contract, described below.

### Deliberately not converted, with the reason for each

- **`read.rs:948`, `record_batches_to_rows`** — merges system fields into bare
  maps *because* the write helpers expect that shape. Converting it made an
  `ON MATCH SET` stop applying, silently. Same contract as `batches_to_rows`.
- **`mutation_common.rs:349`, `batches_to_rows`** — states the contract in its
  own comment: "the write helpers expect variables as bare Maps with
  `_vid`/`_labels` inside".
- **`write.rs:1090`** — decodes `COPY FROM` input. An `_id` column there is
  plausibly *user data*, not an entity, and misreading it would be a new defect
  rather than a fixed one.
- **`write.rs:1996`** — scalar key columns; there is no entity to canonicalise.
- **`endpoint_hydrate.rs:243`** — a `matches!(…, Value::List(_))` shape probe,
  not an entity read.

The first two are the same blocker stated twice: **the write helpers read
`_vid`/`_labels` out of a map.**

### Write-helper migration — complete, 2026-09-05

Converting both decoders and following the failures gave the whole list. Every
one was a silent write no-op or a silently wrong read, never an error:

| symptom | cause |
|---|---|
| `REMOVE n.prop` did nothing to the row | write-back reached into a `Value::Map` |
| `SET n:Label` did nothing to the row | same, for labels |
| `REMOVE n:Label` did nothing to the row | same |
| `SET n.p = null` left the property visible | `Null` inserted into a native entity's map, where absence is what removal means |
| fork edge `MERGE … ON MATCH` matched nothing | see below |
| `labels(n)` raised "requires a node argument" | map-only arm |
| `keys(n)` returned `[]`, and no rows under `UNWIND` | **two** implementations, both map-only |
| `type(r)`, `properties(n)` | map-only arms |

Writing into an entity needed the write side of `entity_property`:
`set_entity_property`, `set_entity_properties` and `set_entity_labels`. They are
deliberately **not** symmetric with the reader — assigning `Null` *removes* a
property from a native entity, because the property graph model says a
null-valued property does not exist, while a map row keeps it present-and-null so
its flattened columns stay addressable. `canonical_entity` drops null properties
for the same reason.

### The fork MERGE, which was not a write helper at all

MERGE's match phase found the edge — `db_matches = 1` — and then a consistency
filter discarded it, because the row and the database disagreed on
`b._all_props`: `Map({})` against `Null`. Both mean "no properties", and both
sides were produced by *different plans* that spell it differently.

A dotted column is a **projection** of its variable, not a binding. If the base
variable is the same entity on both sides, its projections cannot disagree about
identity, so they must not be allowed to veto the match. That is the fix, and it
is a defect independent of encodings: any two plans spelling an empty property
set differently could have triggered it.

### Twice, the integration suite was not enough

Both times, converting a decoder left **4766 of 4766 integration tests passing**
and then failed the TCK — nine scenarios the first time, five the second. Had the
TCK not been run, both would have been committed as complete. "The suite is
green" is a claim about the suite.

## `uni-store` — the premise was wrong, 2026-09-05

The plan said storage was the next decoder to convert. It is not, because
**`uni-store` has no entity encoding to unify.**

Measured, not assumed:

- `Value::Node` and `Value::Edge` appear **zero** times in `crates/uni-store/src`.
- No `Value::Map` in that crate carries `_vid`, `_eid`, `_id`, `_src`, `_dst`,
  `_labels`, `_type_name` or `edge_type`. Every occurrence of those strings is
  an *Arrow* column name — `Field::new`, `column_by_name`, `FilterExpr`, a
  `ScanRequest` projection — which is the physical layout, not the `Value`
  encoding.
- No public function returns a `Value` meaning a whole vertex or edge. Whole
  entities leave storage as `uni_common::Properties` — a property bag with no
  identity keys — with the id passed separately.

`arrow_convert::arrow_to_value` decodes a *property* value, one Arrow cell at a
time. It yields an entity-shaped map only when the query layer hands it an
entity *struct column* that the query layer itself built. Pushing entity
canonicalisation into it would move a query-layer concept below the layer
boundary, and would apply it to the callers that decode genuine user data —
`uni-bulk`'s `record_batch_to_property_maps`, the trigger pre-image builder, and
`COPY FROM` — where an `_id` column is plausibly a user column. That is the
defect `write.rs:1090` already opts out of; it should not be reintroduced one
level down.

**So the boundary already holds.** What was left was not below the query layer
but inside it: the places that still *construct* the map form, and the readers
that only understood it.

## The construction sites, and what they were hiding

Four sites built an entity as a `Value::Map`. Converting them exposed the same
class of latent wrong answer as the decoder conversions, one level over:

| site | was |
|---|---|
| `write.rs::build_node_map` | MERGE's node binding, whose own doc claimed it "matches the binding the MATCH path produces" — which by then produced `Value::Node` |
| `write.rs`, CREATE node | the same shape built a second time, inline |
| `write.rs`, CREATE edge | spelled the type as a numeric `type_id`, which several readers never resolved |
| `endpoint_hydrate::hydrate_node` | inline map "so a projection decodes to the same shape" — a reason that expired when projections went native |

## Two more accessors, for the same reason as the first

Identity had an accessor. **Labels, edge type and endpoints did not**, and each
had grown the identical defect: several spellings, a different subset understood
at each site, and a silent wrong answer for whichever one the reader did not
know.

- `Value::edge_endpoints` — `_src_vid`/`_dst_vid`, `_src`/`_dst`, `src`/`dst`,
  and `Value::Edge`. Three hand-rolled versions existed.
- `Value::edge_type_ref` — `_type` holding a *name*, `_type` holding a numeric
  *id*, `_type_name`, `edge_type`, and `Value::Edge`. Returns `EdgeTypeRef`
  rather than a `String`, because resolving an id needs the schema and
  `uni-common` does not have it: the id survives into the return type instead of
  being silently dropped, which is what the old readers did.
- `Value::entity_labels` and `Value::entity_properties` — the value halves of
  `property_names`.

### What the readers were doing

Every one of these is a silent wrong answer, never an error:

| symptom | cause |
|---|---|
| `WHERE n:Person` excluded every row | `LabelCheck` matched `Value::Map` only; a native node hit the catch-all `false` |
| `hasLabel(n, 'Person')` answered false | two arms, one *labelled* "handle proper Value::Node type", neither handling it |
| `type(r)` answered `null` | the interpreted evaluator read `_type` out of a map only |
| `labels(n)` answered `null` | same |
| `properties(n)` answered `null` | same |
| `type(r)` raised "requires a relationship argument" | the UDF accepted `_type` only as a *name*, and CREATE spells it as an id |
| an edge sorted under `""` | the sort key silently used the empty string for a numeric `_type`, so every such edge tied |

`type()`, `labels()`, `properties()` and `hasLabel` each existed **twice** — once
as a UDF and once in the interpreted evaluator — and the second copy knew only
the map form in every case. That is the `keys()` lesson from the first pass,
repeated four more times: consolidating one implementation is not the same as
finding the other one.

## The ratchet now covers shape, not just identity

`arch_entity_identity.rs` grew a second budget over `_labels`, `_type`,
`_type_name`, `edge_type`, `_src`, `_dst`, `_src_vid`, `_dst_vid`. The entries
that remain are decoders and dispatch guards — code whose job *is* to recognise
the map form and convert it, or to decide which of two shapes a value has. A
reader that merely wants the labels of an entity it already has does not qualify,
and the ratchet only turns down.

## What remains

- **Step 4, making the map form unrepresentable in the type system.** Every
  construction site in `uni-query` is gone and storage never had one, so the map
  form now arises only where a decoder produces it and immediately canonicalises.
  What blocks a hard type-level ban is the legitimate remainder: `COPY FROM`
  input and user map properties can carry `_id`, and the dispatch guards need to
  ask "is this the map form?" to answer at all. The ratchet is the guard for
  that boundary, not a compiler check.
- Three query-side call sites deliberately do not canonicalise, and should not:
  `write.rs:1090` decodes COPY FROM input, where an `_id` column is plausibly
  user data; `write.rs:1996` decodes scalar key columns; `endpoint_hydrate.rs:243`
  is a `matches!(…, Value::List(_))` shape probe.
