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

These run during Phase 2 command dispatch on converged derived facts.

### DERIVE as a command

Iterates over converged facts from a named rule, applies an optional WHERE filter, and executes the mutations:

```locy
DERIVE risk_propagation WHERE threshold > 0.5
```

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

## ABDUCE

`ABDUCE` asks: "what minimal changes would make (or prevent) this conclusion hold?"

```locy
ABDUCE compromised WHERE target.name = 'ServerA' RETURN assumptions
ABDUCE NOT reachable WHERE a.name = 'Alice' RETURN b
```

**Three-phase pipeline:**

1. **Build derivation tree** via EXPLAIN to identify which facts contribute to the conclusion.
2. **Extract candidate modifications** — edge removals, property changes, edge additions that could alter the derivation.
3. **Validate each candidate**: savepoint → apply mutation → re-evaluate all strata → check if conclusion holds → rollback.

Returns the minimal set of changes that achieve (or prevent) the goal. The database is never permanently modified.

Typical outputs include modification candidates, validation flags, and estimated costs.

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
