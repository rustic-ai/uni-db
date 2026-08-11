# uni-db 3.4.0

**Release focus: the bindings a pattern promises, and the name an edge answers to.** This is a
small release — three commits on top of 3.3.0 — but both headline items are the same shape as that
release's theme: a surface that returned a plausible-looking answer with nothing between the claim
and the truth.

`[r:E]` written inside `shortestPath(...)` or a quantified path pattern parsed fine and was then
silently discarded, so the variable surfaced as `UndefinedVariable` at the `RETURN` — an error
pointing at the wrong line. openCypher and GQL both specify what those bindings mean, so 3.4.0
implements them as group variables rather than refusing them. And a relationship that had been
flushed to Lance reported its type as the literal string `"UNKNOWN"` — the type-name path read the
L0 write buffers and stopped, while its documented sibling `get_edge_endpoints` had always fallen
back to storage.

This release covers everything since **3.3.0**: **3 commits**, 41 files changed,
+6,067 / −384.

---

## ⚠️ Breaking change

One, with a `BREAKING CHANGE` footer on its commit.

**A quantified pattern's endpoints are the adjacent outer nodes, never its inner variables.** Inner
variables are now group variables — lists holding one element per iteration — so reading one as a
node is a type error:

```cypher
-- 3.3.0: `a` meant the start node
MATCH ((a)-[:E]->(b)){2} RETURN a.id

-- 3.4.0: write the endpoints explicitly …
MATCH (s)((a)-[:E]->(b)){2}(e) RETURN s.id, e.id
-- … or read the group variable
MATCH ((a)-[:E]->(b)){2} RETURN [n IN a | n.id]
```

`last(a).id` reproduces the previous value exactly. The diagnostic names both replacements, so the
failure is a compile-time error with a migration in the message rather than a changed result.

---

## Group variables (openCypher / GQL)

- `shortestPath` / `allShortestPaths` bind their relationship as a list, matching an ordinary
  variable-length pattern.
- A quantified path pattern binds every variable declared inside it as a GQL group variable, at
  `offset + i * hops_per_iter` in the matched path. `qpp_group_bindings` in `df_graph/nfa.rs` is
  the single place those offsets are assigned, beside the `PathNfa::from_qpp` cycling it mirrors —
  that adjacency is what keeps the executor's columns and the NFA's per-hop filter slots from
  drifting apart.
- Group variables are their own types (`VariableType::NodeList` / `EdgeList`) rather than an entity
  type plus a side-channel flag, so every existing `== VariableType::Node` comparison excludes a
  list by construction. This incidentally fixes `WITH r AS rs RETURN size(rs)`, which previously
  lost its list-ness through the alias.
- A Locy `DERIVE` head referencing a group variable now derives one fact per iteration — an
  implicit `UNWIND`. It previously produced a single derived edge pointing at nothing.
- Unreferenced group bindings are pruned before the output mode is chosen, so naming an inner
  position you never read still uses the endpoint-only BFS. `EXPLAIN` reports the chosen mode and
  any live group bindings.
- A quantified pattern over an undeclared relationship type is now refused rather than reported as
  an empty result.

## Silent-wrong-answer fixes

- **A flushed relationship reports its real type name.** Post-flush, `_type_name` came back as
  `"UNKNOWN"` from the variable-length paths and `""` from `shortestPath`. The answer
  was already being computed and discarded: tier 2 of `resolve_stored_edge_endpoints` probes
  candidate edge types to recover a flushed edge's orientation, and the type whose adjacency
  contains the eid *is* the edge's type. The same change fixes schemaless traversal, which labelled
  every edge of an `[:A|B]` pattern with the literal string `"A|B"` — wrong before a flush as well
  as after. The unresolvable-type sentinel is unified on `""`, since a real edge type may legally be
  named `UNKNOWN`.
- **Path element properties survive a flush.** They were read from the L0 write buffers alone in
  all six path-materializing executors — both VLP paths, `shortestPath`,
  `BindFixedPath`, `BindZeroLengthPath` and pattern comprehension — so every property vanished once
  the entity was flushed. They now come from a batched storage-backed pre-fetch,
  `EntityPropertyCache`.
- **Reading a user property off a native list element** (`[e IN r | e.tag]`) no longer fails
  outright. A name that is not a literal struct field compiled to an untyped `ScalarValue::Null`,
  which the list encoder cannot encode; it now resolves through the properties blob via the `index`
  UDF, with a typed null for a genuine miss.
- **A variable-length search that hits its frontier or predecessor-pool safety cap emits a
  warning** instead of silently returning an incomplete result.

## Internal

The "resolve the type name, resolve the stored direction, append" idiom had been copy-pasted at six
sites and the group-variable work had just added a seventh; they now share
`common::append_traversed_edge` taking a bundled `EdgeAppendCtx`. Two sites deliberately keep their
own code with a comment saying why.

## CI

Two `ci.yml` gates were repaired. Both were broken by the group-variable change and neither runs in
`pr.yml` — they exist only in the post-merge suite, so that PR merged green:

- the **rustdoc gate**, on an intra-doc link to `PropertyManager` that could not resolve because the
  type lives in `uni-store` and is not imported into the referring module;
- **`release-guards`**, on a stale generated Python symbol reference — a `uri()` stub was added
  without regenerating the page. `check_doc_symbols.py` stayed green throughout, because it only
  verifies that documented methods exist in the `.pyi`, never the reverse direction.

---

## Upgrading

`cargo update -p uni-db` / `pip install -U uni-db`. The only migration is the quantified-pattern
endpoint change above, and it fails at compile time with the replacement in the message.
