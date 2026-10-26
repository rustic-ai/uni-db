# Advanced: DERIVE, ASSUME, ABDUCE

## DERIVE

`DERIVE` has two forms with different semantics.

`DERIVE` writes **edges** — `(a)-[:TYPE]->(b)` or `(a)<-[:TYPE]-(b)` — or merges
nodes with `DERIVE MERGE`. There is no bare-node form: to mark a node, derive an
edge to a marker node, or yield the node from the rule and set the label in a
follow-up Cypher write.

### DERIVE in a rule body (inline mutation)

Replaces `YIELD` when a rule should write graph mutations rather than produce queryable derived facts:

```locy
// Infer a new edge
CREATE RULE infer_friend AS
MATCH (a)-[:KNOWS]->(b)
DERIVE (a)-[:FRIEND]->(b)

// MERGE: combine matching paths into one node
CREATE RULE merge_paths AS
MATCH (a)-[r]->(b)
DERIVE MERGE a, b
```

These run during Phase 2 command dispatch, in program order, on the derived facts of the current state.

### DERIVE as a command

Iterates over a named rule's facts in the current state, applies an optional WHERE filter, and executes the mutations:

```locy
DERIVE risk_propagation WHERE threshold > 0.5
```

Commands run in sequence, so every later command of the program — a `QUERY`,
a second `DERIVE`, `EXPLAIN RULE`, `ABDUCE`, an `ASSUME`, a Cypher read —
sees what the `DERIVE` wrote. A Cypher write command (`CREATE`, `MERGE`,
`SET`, ...) works the same way. See
[Commands run in sequence](../rule-semantics.md#commands-run-in-sequence).

## ASSUME

`ASSUME` executes a hypothetical mutation block and evaluates a body against that temporary state.

```locy
ASSUME {
  CREATE (:Node {name: 'X'})-[:EDGE]->(:Node {name: 'Y'})
} THEN {
  QUERY reachable RETURN b
}
```

Think of this as savepoint-scoped what-if execution.

A body may also define rules, `DERIVE`, write with Cypher, and nest another
`ASSUME`. Its commands run in sequence, as a program's top-level commands do, exactly as they would one after another
in a transaction that is rolled back at the end: a `DERIVE` or Cypher write
changes the hypothetical graph, and every later command in the body — `QUERY`,
another `DERIVE`, a Cypher read, a nested `ASSUME` — reads the changed graph
and the rule facts derived from it.

```locy
CREATE RULE link AS MATCH (a:Person)-[:KNOWS]->(b:Person) DERIVE (a)-[:LINK]->(b)
CREATE RULE linked AS MATCH (a:Person)-[:LINK]->(b:Person) YIELD KEY a, KEY b

ASSUME { CREATE (:Marker) } THEN {
  QUERY linked RETURN a.name AS a, b.name AS b   // result 0: no LINK edges yet, no rows
  DERIVE link                                    // result 1: Derive { affected: 3 }
  QUERY linked RETURN a.name AS a, b.name AS b   // result 2: the three derived edges
}
```

After a write, only the strata the write can affect are derived again; the
rest keep their facts (see *Incremental re-evaluation* in the Locy reference).

The ASSUME command's result holds **one result per body command**, in order,
each the result the same command gives at the top level: a `QUERY`'s rows, a
`DERIVE`'s `Derive { affected }` count, a Cypher command's rows, and a nested
`ASSUME`'s own ASSUME result. Alongside them it carries `derived`: every rule's
facts (the program's and the body's own) in the hypothetical state the body's
first command reads, after the ASSUME's mutations and before any body command —
the same point a program's top-level `derived` is taken at, before its first
command. A body that only defines rules has no results; its facts are in
`derived`.

| | Rust (`CommandResult::Assume { results, derived }`) | Python (`AssumeCommandResult`) |
|---|---|---|
| Per-command results | `results: Vec<CommandResult>` | `.results` |
| Hypothetical facts | `derived: HashMap<String, Vec<FactRow>>` | `.derived` |
| Rows | `CommandResult::rows()` | `.rows` |

The rows accessors search depth first: an ASSUME's rows are those of the first
body result that produces rows (a `QUERY` or Cypher command, descending into a
nested ASSUME), and `LocyResult::rows()` / Python `LocyResult.rows()` skip an
ASSUME whose body produced none. A single-command body therefore reads exactly
as before; for more than one command, address `results` by index.

`ABDUCE` and `EXPLAIN RULE` run in an ASSUME body, at any nesting depth, against
the hypothetical state: they give the answer the same command gives at the top
level of a program run in a transaction that applied the ASSUME's mutations and
is rolled back afterwards. `EXPLAIN RULE` explains the hypothetical facts from
the derivations recorded while deriving them. `ABDUCE` applies each candidate
on top of the hypothetical state and validates it against every rule the body
reads, the body's own rules included. Both results appear in the ASSUME's
`results` (Python `ExplainCommandResult` / `AbduceCommandResult`).

```locy
// What would break reachable(B, X) if B -> X existed?
ASSUME {
  MATCH (b:Node {name: 'B'}) CREATE (b)-[:EDGE]->(:Node {name: 'X'})
} THEN {
  EXPLAIN RULE reachable WHERE a.name = 'B'
  ABDUCE NOT reachable WHERE a.name = 'B'
}
```

A program with an `EXPLAIN RULE` or an `MNOR` / `MPROD` fold anywhere, in an
ASSUME body included, records provenance in every evaluation, the hypothetical
ones too. That is also what makes `exact_probability` hold in a body: a body
`QUERY` reads the same exact probability a top-level `QUERY` reads in the same
state. Before, it read the independence approximation.

A scratch transaction (`session.scratch()` in Python, `Session::scratch()` in
Rust) remains another way to run any command against a hypothetical change: it
behaves like `tx()` but refuses `commit()`, so its writes are always discarded.

## ABDUCE

`ABDUCE` asks: "which single change to the graph would make (or prevent) this
conclusion hold?"

```locy
ABDUCE compromised WHERE target.name = 'ServerA' RETURN assumptions
ABDUCE NOT reachable WHERE a.name = 'Alice' RETURN b
```

**Three-phase pipeline:**

1. **Build derivation tree** via EXPLAIN to identify which facts contribute to the conclusion.
2. **Extract candidate modifications** — edge removals, property changes, edge additions that could alter the derivation.
3. **Validate each candidate**: savepoint → apply mutation → re-evaluate all strata → check if conclusion holds → rollback.

Each candidate is one edit — for `ABDUCE NOT`, one edge removed or one numeric
property set to zero; for a positive `ABDUCE`, one edge added — and is
validated on its own; candidates are never combined. The result lists every candidate tried, each with a `validated` flag
and a fixed per-kind cost (property change 0.5, edge removal 1.0, edge addition
1.5), validated candidates first and then by ascending cost, up to
`max_abduce_results`. The cost ranks kinds of edit; it is not a measure of how
small the change is, and no search for a minimal *set* of changes is made: a
goal that only several edits together achieve returns no validated candidate.
The database is never permanently modified.

An `ABDUCE NOT` candidate edits the one entity the derivation read: a removal
carries the edge's `eid` (or, for an anonymous relationship, its endpoints'
`source_vid` / `target_vid`), and a property change carries the node's `vid` or
the edge's `eid`. Validation applies exactly that edit. A relationship that can
be identified by neither is not proposed, so `ABDUCE NOT` with a goal several
facts satisfy validates only an edit that removes all of them.

A positive `ABDUCE` candidate adds one edge between two existing nodes, named by
`source_vid` / `target_vid`, and validation adds exactly that edge. For each
relationship of the rule body, endpoint pairs are taken from the nodes that
carry the pattern's endpoint labels, in id order; when the goal's `WHERE` reads
only that relationship's endpoint variables (`ABDUCE t WHERE a.name = 'A'` for a
hop out of `a`) it filters the pairs. At most `max_abduce_candidates` pairs are
proposed in total, so a goal that needs edges into nodes late in id order may
need a larger budget or a more selective `WHERE`. An edit that needs two new
edges is never validated, since each candidate is a single edge.

## Safe Usage

- Keep candidate search bounded.
- Use explicit constraints in `WHERE`.
- Validate candidate modifications before applying.

See [Errors & Limits](../reference/errors-limits.md) for runtime guardrails.
