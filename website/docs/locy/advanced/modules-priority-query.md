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
first command runs (and again, incrementally, after a command writes). A
`QUERY ... WHERE` that pins a node KEY column of its rule is the exception:
by default it derives only what that goal can reach. Any other `WHERE`
filters the derived facts and does not narrow what is derived; to derive less
there, constrain the rule itself (a `WHERE` in its body, or a `REQUIRE`).

### Goal-directed QUERY

A `QUERY` whose `WHERE` pins a node KEY column derives only what that goal
can reach (the magic-sets rewrite). This is the default,
`LocyConfig.goal_directed = GoalDirected::Auto` (from Python, `"auto"`):

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
- `.derived` is unchanged. A rule only rewritten goals needed is not
  evaluated; it is derived the first time `.derived` is read (Rust:
  `result.derived_rules().await` or `result.derived_rule(name).await`), against
  the snapshot the evaluation read — a commit in between does not reach it —
  under the same limits, and cached. The first read can therefore fail (a
  timeout, say); nothing is cached then and the next read tries again.
  `result.lazily_derived_rules()` names the rules left out.
- Where that could not give exactly what full evaluation gives, `Auto`
  evaluates in full: a program with a write command (`DERIVE`, a Cypher
  write), one with `EXPLAIN RULE`, and every evaluation inside a transaction
  (`tx.locy`), whose state cannot outlive the call. A rule left out of an
  evaluation cut short (`allow_partial`) is listed in `incomplete.skipped_rules`
  and not derived later.
- `On` leaves such rules out of `.derived` instead (never partial), and
  `GoalDirected::Shadow` evaluates every rule while answering from the
  rewrite; it costs more than `Off` and is meant for verification. `Off`
  evaluates every rule in full.
