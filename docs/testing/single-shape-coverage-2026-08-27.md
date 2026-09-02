# One shape is not coverage — 2026-08-27

**Date:** 2026-08-27
**Found by:** running LDBC SNB Interactive at SF1 against a suite that was green

Four defects in one session shared a property. Each was in a feature the
openCypher TCK *does* exercise, each suite passed throughout, and in each case
the passing suite bounded nothing — because every scenario covering that feature
had the same structural shape, and the defect lived in a different one.

This is not the `docs/testing/` theme of a check that runs and reports success
while doing nothing. These checks did real work. They just all did the *same*
work, and the suite's size hid that.

## The four

| feature | what the TCK covers | what it could not see |
|---|---|---|
| pattern comprehensions | 8 expressions, **all** beginning with a variable the enclosing `MATCH`/`WITH` already bound | every unanchored form errored |
| `startNode` / `endNode` | **1** scenario, relationship bound by `MERGE` | failed on every MATCH-bound relationship |
| `IN` over entities | none | `n IN [n]` was false |
| `ORDER BY` over strings | many, none using a value that trips the temporal classifier | any string starting with `P` sorted as a constant |

The first two are measurable, so they were measured rather than estimated:

```bash
# Pattern comprehensions: 8 occurrences, 3 files.
grep -rh "\[(" --include=*.feature crates/uni-tck/tck/features/ \
  | grep -E "\[\(.*(-->|--|->|-\[).*\|" | grep -vE "^\s*\|"

# Every one of them opens on a bare bound variable — `(n)`, `(a)`, `(x)`.
# None opens on a labelled or otherwise unbound node:
… | grep -E "\[\([a-z]*:"     # no matches

# startNode/endNode: one scenario, in Merge5.
grep -rn "startNode(\|endNode(" --include=*.feature .   # 1 hit
```

## Why a big suite does not help

The pattern-comprehension case is the clearest. Eight scenarios sounds like
coverage. But the feature has two independent axes — *is the pattern anchored on
an outer variable* and *what does the projection return* — and all eight sit at
one point on the first axis. Adding a ninth scenario of the same shape raises
the count and buys nothing. The suite is wide in a direction the defect does not
vary along.

The `startNode`/`endNode` case is the same fact with n=1, which makes it easier
to see and no different in kind.

## The trap has a smaller version, inside a single test

Worth writing down because it caught this work too, not only the TCK.

Chasing #186, the reproduction sorted values `p0..p7` over a traversal and came
back unsorted. The control — the *same* sort over a plain scan — passed, which
appeared to establish that the traversal was the cause. That is what the issue
records.

Both halves were wrong, and for two different reasons:

- The control's input was already in sorted order, so a sort that did nothing
  was indistinguishable from a sort that worked.
- A second control did use shuffled input, but with values `a, b, c, d` — which
  avoided the actual trigger, a leading `P`.

The passing control was not evidence about traversals. It was a coin that could
only land heads. A plain scan over `p3, p1, p0, p2` — no traversal anywhere —
reproduces the bug immediately, and that one query relocated the defect from the
traversal operator to the temporal classifier.

**A control has to be able to fail.** If you cannot say what result would have
falsified it, it is decoration.

## The audit: which other features have the same shape

The three above were found by tripping over them. This is the systematic pass —
for each function that takes a graph entity, how many TCK queries exercise it,
and in how many distinct *binding contexts* the entity arrives.

The classification looks only at the query under test, not the `Given` graph
setup. That distinction matters: counting setup clauses credits nearly every
scenario with a `CREATE` it does not actually test through.

| function | queries | binding of the entity in the query under test |
|---|---|---|
| `relationships(` | 7 | MATCH-traversal:7 — **one shape** |
| `nodes(` | 11 | MATCH-traversal:8, MATCH-node:3 |
| `type(` | 13 | MATCH-traversal:11, MATCH-node:2 |
| `properties(` | 7 | (no clause):4, MATCH-traversal:2, MATCH-node:1 |
| `keys(` | 22 | MATCH-traversal:10, MATCH-node:7, UNWIND:7, (no clause):5 |
| `labels(` | 27 | MATCH-node:16, CREATE:5, (no clause):4, MERGE:4 |
| `startNode(` / `endNode(` | 1 | MERGE — the known case |

```bash
# Reproduce (from crates/uni-tck/tck/features):
#   split each file on Scenario, take only the `executing query:` block,
#   and classify the clause that binds the entity.
```

`relationships(p)` is the clearest remaining instance: seven queries, every one
binding the path with a MATCH traversal. Nothing exercises a path that reached
the function through `MERGE`, or through a `collect()`/`UNWIND` round trip —
which is exactly the axis `startNode`/`endNode` turned out to fail on. `type(`
and `nodes(` are one context away from the same position.

`keys(` and `labels(` are the counter-examples worth noting, because they show
what adequate looks like: four and five contexts respectively, including the
`UNWIND` round trip. A wide suite is not automatically a narrow one; the
question is always what it ranges *over*.

## The rule

> When a feature's entire test coverage shares one structural shape, passing it
> bounds only that shape. Say so, or add a scenario at a different point on some
> axis the feature actually varies along.

Concretely, before treating a green suite as evidence about a feature:

1. **List the axes the feature varies along.** For a function over a graph
   entity: how the entity was bound (`MATCH` traversal, `MERGE`/`CREATE`,
   `UNWIND` of a collected list, a parameter), whether the pattern is directed,
   whether the result is used whole or via a property.
2. **Bucket the existing scenarios by those axes.** One occupied bucket is a
   finding, not a pass.
3. **Write down which buckets are empty** where you cannot fill them. The
   `#[ignore]`d tests in
   `crates/uni/tests/common/cypher_read/start_end_node_test.rs` do this: two
   remaining shapes state the intended behaviour and name what is missing, so
   the gap is legible instead of merely absent.

## What now pins this

These are the tests the rule is written from. Each occupies a bucket that was
empty:

- `crates/uni/tests/common/cypher_read/start_end_node_test.rs` — the MERGE-bound
  control the TCK already had, plus MATCH-bound, direction-reversed, and
  variable-length; two `#[ignore]`d for the shapes still open.
- `crates/uni/tests/common/cypher_read/order_by_sort_key_test.rs` — sorting over
  values that trip the temporal classifier, including the no-traversal
  reproduction and an `ORDER BY … LIMIT` case, since a constant sort key returns
  the wrong *rows* and not merely the wrong order.
- `crates/uni/tests/common/perf/query_limits_test.rs` — a query returning one
  row while building a large intermediate, which is the shape a result-size
  memory limit cannot see by construction.

## Related

- [teeth-2026-08-13.md](teeth-2026-08-13.md) — the other half of the same
  discipline: a test never observed failing is an assumption wearing a test's
  clothes. This document is about suites where every test *can* fail but all of
  them fail on the same thing.
- [silent-downgrades-2026-08-15.md](silent-downgrades-2026-08-15.md) — the
  `vid_lookup_join` case, where no assertion of that *kind* could have caught
  the defect. Here no assertion of that *shape* could.
