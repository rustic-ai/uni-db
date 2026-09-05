# Unifying the two entity encodings

Status: **Done for the query-side decode boundary as of 2026-09-05.** Steps 1 and
2 below are complete: an entity struct now decodes to its native form, so that
boundary no longer produces the second encoding. The history of how it was
sequenced — including an attempt that failed and why — is kept because the
remaining boundaries will hit the same wall.

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

### What is not done

Ten call sites inside `uni-query` still call the storage decoder directly rather
than through a query-side wrapper, so they still produce maps:
`df_udfs_plugin.rs:465`, `locy_eval.rs:648,897`, `endpoint_hydrate.rs:223,242`,
`mutation_common.rs:349`, `write.rs:1090,1996`, `locy_fixpoint.rs:3268`.

**`mutation_common.rs:349` — `batches_to_rows` — must not be converted.** Its own
comment states the contract: "the write helpers expect variables as bare Maps
with `_vid`/`_labels` inside". Converting it would break a documented dependency
rather than expose a defect. The write helpers would have to move to
`Value::entity_property` first, which is step 1 again for that pipeline.

The rest are ordinary read paths and should convert cheaply, one at a time, with
the suite as the check. Step 4 — making the map form unrepresentable — stays open
until they and the storage decoder are all done.

## What the class cost, as an argument for finishing it

Defects closed under #234 that were all this one split, each silent:

- `n._vid` NULL for a native entity, which made `DISTINCT` and `count(DISTINCT)`
  count one vertex twice
- `type()` raising "requires a relationship argument" for a `Value::Edge`
- `properties()` returning null for a native node or edge
- `id()` returning `_eid` verbatim, so one function returned `Int` or `String`
  depending on how the row was encoded
- `validAt` filtering out every row whose relationship was materialised natively
- `collect(DISTINCT n)` keying `"Vid(7)"` and `7` as different nodes
- a path silently losing a node or edge whose map spelled the id `_id`
- `SET` / `DELETE` reporting success having matched nothing
- the TCK's own snapshot probe contributing zero properties, so side-effect
  assertions passed on an empty snapshot

None of these produced an error. Two were caught by existing tests only once a
native entity was routed somewhere it had never reached — which is the argument
for (2): the split hides defects until something forces the other form through.
