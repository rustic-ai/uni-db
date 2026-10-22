# Advanced: Modules, PRIORITY, QUERY

## Modules

Modules package reusable rule libraries.

<!-- doctest: skip -->
```cypher
MODULE acme.compliance
USE acme.common
```

Benefits:

- Reuse and composition.
- Namespace separation.
- Cleaner policy/rule governance.

## PRIORITY

Priority resolves competing clauses for the same rule relation.

```locy
CREATE RULE classify PRIORITY 1 AS MATCH (n) WHERE n.risk > 0.8 YIELD KEY n, 'high' AS level
CREATE RULE classify PRIORITY 2 AS MATCH (n) YIELD KEY n, 'normal' AS level
```

## QUERY

`QUERY` reads a rule's facts as the fixpoint derived them, then filters and
projects them. Ahead of the program's first write command it returns exactly
what `.derived` holds for that rule; after a `DERIVE` or a Cypher write it reads
the facts derived again in the state that write left (see
[Commands run in sequence](../rule-semantics.md#commands-run-in-sequence)).

```locy
QUERY classify WHERE n.name = 'service-a' RETURN level
```

## Evaluation

Locy evaluates bottom-up: every rule is derived to its fixpoint before the
first command runs (and again, incrementally, after a command writes). By default a `QUERY ... WHERE` filters the derived facts; it does not
narrow what is derived. To derive less, constrain the rule itself (a `WHERE`
in its body, or a `REQUIRE`), or turn on goal-directed evaluation.

### Goal-directed QUERY

Setting `LocyConfig.goal_directed = GoalDirected::On` (from Python,
`with_config({"goal_directed": "on"})` or `LocyConfig(goal_directed="on")`)
makes a `QUERY` whose
`WHERE` pins a node KEY column derive only what that goal can reach (the
magic-sets rewrite):

```locy
CREATE RULE reach AS MATCH (a:E)-[:T]->(b:E) YIELD KEY a, KEY b
CREATE RULE reach AS MATCH (a:E)-[:T]->(m:E) WHERE m IS reach TO b YIELD KEY a, KEY b
QUERY reach WHERE a.uid = $start RETURN b.uid
```

On 100 chains of 20 nodes this derives the 19 + 190 facts of the start's
chain instead of all 19,000.

- A conjunct binds when it is `key.prop = value` or `key.prop IN [...]`, with
  `key` a node KEY column and the value a string, integer, boolean or a
  parameter holding one. Everything else stays a filter, and the whole `WHERE`
  still applies, so answers never change.
- Recursive FOLD / BEST BY / REQUIRE, ALONG, PROB, PRIORITY, generators and
  models fall back to full evaluation. `EXPLAIN RULE`, `DERIVE`, `ABDUCE` and
  commands inside `ASSUME` are always fully evaluated.
- In `On` mode a rule only rewritten goals needed is not evaluated, so it is
  missing from `.derived` (never partial). `GoalDirected::Shadow` keeps
  `.derived` complete while still answering from the rewrite; it costs more
  than `Off` and is meant for verification.
- The default is `Off`, because `.derived` promises every rule's full
  relation.
