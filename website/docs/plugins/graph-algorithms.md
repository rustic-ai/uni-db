# Guest-Authored Graph Algorithms

A graph algorithm is one of the plugin surfaces you can author in **any** loader — Rust, WASM Component Model, Extism, Rhai, or Python. Your code does not walk vertices. It **conducts**: it issues coarse kernel calls that each do `O(V+E)` or `O(batch)` work natively, and it holds only opaque integer handles to the results.

That constraint is what makes a sandboxed guest fast enough to ship. A per-element callback across a language boundary is one crossing per vertex; a coarse kernel is one crossing per *phase*. With batched kernels and handle-passing, a JIT'd WASM guest runs within ~1.3× of in-process Rust, and an *interpreted* Rhai guest is within a few percent of it — because the interpreter is not on the hot path at all.

For the mental model see [Concepts](concepts.md); for the capability boundary see [Trust & Capabilities](trust-and-capabilities.md).

---

## What you need

A guest-authored graph algorithm needs three grants and should declare two slices.

| Grant | Why |
| --- | --- |
| `Algorithm` | Register the algorithm so `CALL` can resolve it. |
| `GraphCompute` | Drive the kernels. |
| `HostQuery` | Project a graph from the store. Without it you can compute over a graph you were *handed*, but cannot project one yourself. |

**Slices** are versioned kernel families, checked at registration. Declaring them buys a clear load-time failure (`0x86A`, naming the slice) instead of a mysterious unknown-kernel error at call time.

| Slice | Provides |
| --- | --- |
| `graph-compute@1` | Read-only kernels over a projected graph. |
| `graph-arena@1` | Mutable structure your algorithm **grows** during the call. |

See [Reference → Capability slices](reference.md#capability-slices) for the declaration syntax.

---

## The two shapes

**Read a graph** (`graph-compute@1`) — you project a graph from the store and compute over it: PageRank, components, reachability, similarity. The graph is immutable and pinned to its projection-time snapshot, so concurrent writers cannot change it under you.

**Grow a graph** (`graph-arena@1`) — you build structure that does not exist in the store: a search tree, a residual network, an agent population. This is session-local and never observable by the store; it cannot write back.

The two compose. `arena_freeze` turns an arena into an ordinary graph handle, so the entire read-only kernel catalogue applies to structure you grew yourself.

**Growing an existing network.** `arena_expand` builds structure from nothing, which is right for a search tree but not for a simulation over a network that already exists. `arena_seed` imports one:

```rhai
let a  = gc.arena_new(capacity, max_degree);
let s  = gc.arena_seed(a, g);        // the store's network, slot-for-slot
let n  = gc.arena_alloc(a, births);  // newborns take slots V..
gc.arena_link(a, parents, n);        // and link into the real topology
let gg = gc.arena_freeze(a);         // aggregate over the whole thing
```

The arena must be empty when seeded, because slot `i` of the arena becoming vertex `i` of `g` is what makes the frozen graph's index space meaningful. Size `branching` to the projection's maximum out-degree — `arena_seed` checks it up front and names the value you need rather than failing partway through the import.

One boundary to know: `emit` keys its `nodeId` column to the primary projection, so newborns — which exist only inside the CALL and have no store identity — have no per-row egress. Emit for the imported vertices, or carry newborn results out as aggregates.

That composition is how you aggregate over links you grew. There is no `arena_spmv`, and there does not need to be: freeze the arena, then use `spmv` on the result. The frozen graph's slot `i` **is** the arena's slot `i` — `arena_freeze` walks live slots in order — so an arena-keyed column and the graph frozen from that arena share one index space and combine without a `rekey`:

```rhai
let g   = gc.arena_freeze(arena);            // arena links become a CSR
let col = gc.arena_gather(arena, 0, slots);  // an arena-keyed [V] column
let agg = gc.spmv(g, col, "linear_algebra", "out");
```

The equivalence is scoped to *that* arena's own frozen graph: the same column against an unrelated projection, or against a graph frozen from a different arena, is still rejected.

---

## Arguments and projecting the graph

A guest algorithm is invoked from Cypher with `CALL`, passing its arguments and — as an implicit optional last argument — a projection-config object that names which slice of the store to project.

### Typed arguments

A guest manifest's declared `args` are now **type- and arity-checked** at call time. (Before this initiative the declared list was inert metadata that the host ignored.) The arg-type vocabulary:

| Token | Accepts |
| --- | --- |
| `int` / `float` / `string` / `bool` | The corresponding primitive scalar. |
| `value` / `cypherValue` | A scalar **or** an array — e.g. a variable-length seed set. |
| `list` / `array` | An array (required). |

The `value` token is what lets an algorithm take a variable-length seed set through **one** declared parameter. Declare `args: ["value"]` and call it with a Cypher list:

```cypher
CALL myplugin.spread([1, 2, 3, 4], {nodeLabels: ['N'], edgeTypes: ['E']})
```

No more generating the plugin once per arity, and no more padding unused slots with a `-1` sentinel. The trailing `{nodeLabels, …}` projection-config object is the implicit optional last argument — it does **not** count against the declared arity.

### Projection scoping is fail-loud

!!! warning "Breaking change — unscoped projections now error"
    A GraphCompute `CALL` with **neither** `nodeLabels` nor `edgeTypes` no longer silently projects every declared label and edge type. It now **errors**. To project the whole graph you must opt in explicitly with `projectAll: true`:

    ```cypher
    // Was: implicitly the whole graph. Now: ERROR.
    CALL myplugin.spread([1, 2, 3])

    // Whole graph, explicit:
    CALL myplugin.spread([1, 2, 3], {projectAll: true})

    // Scoped (recommended, unchanged):
    CALL myplugin.spread([1, 2, 3], {nodeLabels: ['N'], edgeTypes: ['E']})
    ```

    **Migration:** name `nodeLabels` / `edgeTypes` (the recommended pattern — unaffected), or add `{projectAll: true}` where you relied on the old whole-graph default. First-party `uni.algo.gc*` procedures opt into the whole graph automatically; third-party guests must scope explicitly or pass `projectAll: true`.

Two further guardrails make a mis-scoped or mis-sized projection fail with a clear message instead of a downstream surprise:

- A whole-graph (`projectAll`) projection **fails loud, naming the label**, if the store holds vertices under an undeclared (schemaless) label — declare it first, or scope the projection to the labels you mean.
- `emit` names the fault at the guest's call instead of detonating as an opaque Arrow error in batch assembly: a column from the wrong projection is reported as such, and a wrong-length column as `emit column length N != projected node count M`.

Stored property values also project faithfully from uncommitted-to-disk state: `gc.node_property` / `gc.edge_property` and edge weights now read committed-but-**unflushed** in-memory data correctly, rather than surfacing `NaN` (or defaulting edge weights to `1.0`) until the next flush.

### More than one graph: named scopes

Some algorithms need two views of the store at once — a detail graph and an aggregate graph, a structural layer and a flow layer, the same nodes under two edge types. Declare them as **named scopes** on the projection-config object:

```cypher
CALL myplugin.compare([], {
    nodeLabels: ['Cell'], edgeTypes: ['ADJACENT'],     // the primary projection
    scopes: {
        agg:  {nodeLabels: ['Cell'], edgeTypes: ['AGGREGATES']},
        flow: {nodeQuery: 'MATCH (c:Cell) RETURN id(c) AS id',
               edgeQuery: 'MATCH (a:Cell)-[r:FLOWS]->(b:Cell) RETURN id(a) AS src, id(b) AS dst'}
    }
})
```

The guest reaches them by name:

```rhai
let g    = gc.graph();              // the primary projection
let agg  = gc.graph_named("agg");   // a pre-declared scope
```

```python
g   = gc.graph()                    # the primary projection
agg = gc.graph_named("agg")         # a pre-declared scope
```

Sandboxed guests do not call `graph_named` at all — the handles arrive beside `graph` in the invocation JSON, keyed by their CALL-site names:

```json
{"session": 3, "graph": 12884901888, "args": [], "graphs": {"agg": 12884901889}}
```

The `graphs` key is additive, so a guest written before named scopes existed ignores it and behaves exactly as before.

Each scope takes the **full** projection-config vocabulary independently — Native `nodeLabels` / `edgeTypes` / `weightProperty` / `nodeProperties`, or a Cypher `nodeQuery` / `edgeQuery`, or a `name` for a stored named graph.

Three properties are worth being precise about.

Named scopes are a *guest-authored-algorithm* feature: the guest is what decides which scope to read. The first-party `uni.algo.gc*` procedures run a fixed algorithm over one projection, so passing them a `scopes` map is an error rather than a silently ignored hint.

**Scopes are built before your algorithm runs.** `graph_named` is a lookup, not a projection. This is deliberate: projection is `O(V+E)` storage work that the native-work meter does not govern, so a guest able to project on demand could project in a loop and escape the budget entirely. Declaring scopes at the call site keeps the cost bounded and visible to the caller.

**The budget is sized across all of them.** A guest with three scopes can do `O(V+E)` work on each, so the size-derived default work budget sums every bound projection rather than measuring the primary alone.

**The primary is still the primary.** `emit` keys its `nodeId` column to the primary projection, and rejects a column derived from a scope — see [index space](#tensor-identity-shape-and-index-space) below. To return per-vertex results *about* a scope, re-key them (below) or make that scope the primary.

#### Combining values across scopes: `rekey`

Values from different projections do not combine — that is the whole point of the index-space check, and it holds between scopes exactly as it holds between any two graphs. But comparing two layers over the same vertices is the reason named scopes exist, so there is an explicit way to say so:

```rhai
let deg_here  = gc.degrees(g, "out");
let deg_there = gc.degrees(agg, "out");
let moved     = gc.rekey(deg_there, g);        // verified, not a cast
let total     = gc.ewise(deg_here, moved, "add");
```

`rekey` is not a reinterpretation. It walks both projections' slot→Vid maps and succeeds only if they agree vertex for vertex; otherwise it fails naming the first slot where they diverge. So the claim "these two projections describe the same vertices" is checked at the point your algorithm depends on it, rather than assumed — and the check costs `O(V)`, charged to the work budget like any other kernel.

It also rebases an **arena** column onto a projection, which is how an arena-native algorithm reports its results:

```rhai
let col   = gc.arena_gather(a, c, slots);   // computed over grown structure
let moved = gc.rekey(col, g);               // now keyed to the projection
gc.emit("score", moved);
```

That direction is a weaker check, and the difference matters. Two graph projections both carry slot→Vid maps, so `rekey` can *verify* their correspondence vertex by vertex. Arena slots carry no vertex identity, so "my slot `i` is your vertex `i`" is a claim only you can make — the host checks that the arena's live slot count matches the projection's vertex count, and nothing more. It is still stricter than passing the column to `emit` unannounced, because the claim is made once, explicitly, at a call site a reader can find.

It accepts `[V]` tensors and vertex sets. `[E]` values are refused: CSR edge order is a property of one projection's topology and carries no meaning in another, even when every vertex lines up.

!!! warning "Slot correspondence is a Native-only guarantee"

    Two **Native** projections over the same node set agree slot-for-slot: `ProjectionBuilder` sorts and dedups vids before interning, so slot `i` is the i-th smallest Vid in both. That is what makes a value from one scope meaningful against another *when you have checked the node sets match*.

    **Cypher and Named projections carry no such guarantee.** They intern vids in row order and are not sorted, so slot `i` in a Cypher scope is simply the i-th row its query returned. Never assume a Cypher scope's slots line up with anything — move between them by vertex id (`frontier`, `topk`), not by slot.

    The kernels enforce the *space*, not the correspondence: mixing values across scopes is rejected outright with `0x862`, which is why the check protects you here rather than merely warning.

---

### Rules over an edge's two endpoints

Comparing edge *properties* has always worked. Comparing the two *endpoints* of an edge — the shape of every interaction rule — needs node values brought onto edges first, which is what `edge_from_nodes` does. Bounded-confidence opinion dynamics, in full:

```rhai
let x     = gc.node_property(g, "opinion");
let diff  = gc.edge_from_nodes(g, x, "absdiff");   // |x[dst] - x[src]| per edge
let close = gc.map_to_set(diff, "lt", epsilon);    // an [E] mask -> EdgeSet
let agg   = gc.spmv_masked(g, x, "linear_algebra", close);
```

Reaching for `edge_endpoints` or `node_to_edge` answers with `edge_from_nodes(g, x, "absdiff")`. `map_to_set` is shape-polymorphic, so an `[E]` tensor lowers to the `EdgeSet` that `spmv_masked` and `expand_masked` consume — no separate edge-comparison kernel is needed.

## When a kernel you reached for isn't there

The catalogue is closed on purpose — every kernel is native `O(V+E)` work, so the set is small and each addition is deliberate. That means you will sometimes reach for a name that does not exist. When the operation is nonetheless *expressible*, the error tells you how:

```
gc.select(m, a, b)
→ `select` is not a kernel. For a conditional blend, compose it:
  ewise(b, ewise(m, ewise(a, b, "sub"), "mul"), "add")

gc.arena_spmv(a, col)
→ `arena_spmv` is not a kernel. For aggregation over links you grew, compose it:
  spmv(arena_freeze(a), col, "linear_algebra", "out")
```

This fires on both the op-string form (`ewise(a, b, "gt")`) and the method form (`gc.gt(a, b)`), in every loader. A name with no published composition gets the ordinary "not found" message rather than invented advice — the hint exists to point at real compositions, not to guess.

## Kernel catalogue

72 kernels. Every one is reachable from every loader, except `graph` and `graph_named` — sandboxed guests receive their projection handles in the invocation arguments rather than calling for them.

Operands are handles (opaque integers) and small scalars. **No vertex data crosses the boundary.**

### Graph shape and stored properties

| Kernel | Returns |
| --- | --- |
| `graph` | The bound graph handle (in-process loaders; sandboxed guests get it in their args). |
| `graph_named(name)` | The handle of a pre-declared [named scope](#more-than-one-graph-named-scopes) (in-process loaders; sandboxed guests get the scope handles in their `graphs` map). |
| `vertex_count` / `edge_count` | Scalar counts. |
| `degrees(g, dir)` | `[V]` degree map, `"out"`, `"in"`, or `"both"` (the union of out+in edges). `"both"` requires a projection built with `includeReverse: true` — see [Direction `both`](#direction-both). |
| `vertex_ids(g)` | `[V]` map of each vertex's own slot id — the initializer for label-propagation-style algorithms. |
| `node_property(g, name)` | `[V]` column materialized from a stored vertex property. |
| `edge_property(g, name)` / `edge_weights(g)` | `[E]` columns in CSR out-edge order. |
| `edges_all(g)` | An edge mask selecting every edge. |

### Vertex sets

| Kernel | Returns |
| --- | --- |
| `frontier(g, seeds)` | A vertex set from external vertex ids. |
| `set_to_map(s, value)` / `map_to_set(m, pred, threshold)` | Lift and lower between sets and maps. Predicates: `is_zero`, `gt`, `lt`, `eq`. `map_to_set` is shape-polymorphic: a `[V]` map lowers to a `VertexSet`, an `[E]` tensor to an `EdgeSet` that `spmv_masked` / `expand_masked` accept. |
| `set_union` / `set_diff` / `set_intersect` | Set algebra. |
| `set_len` / `is_empty` | Cardinality and emptiness. |

### Tensors and maps

| Kernel | Returns |
| --- | --- |
| `ewise(a, b, op, coef)` | Elementwise `mul` / `add` / `min` / `max` / `axpy` / `div` / `mod` (the division convention is `x/0 = 0`; `mod` deliberately differs — see [remainder conventions](#floor-and-mod)). |
| `edge_from_nodes(g, x, op)` | Gather a `[V]` node value onto edges, yielding `[E]` in CSR out-edge order. `op` is `src` / `dst` / `sub` (`x[dst]-x[src]`) / `absdiff` / `add` / `min` / `max`. The bridge from node quantities to edge quantities — an interaction rule is a predicate over an edge's two endpoints, and nothing else brings node values onto edges. |
| `interp(x, xs, ys)` | Piecewise-linear table lookup — the System Dynamics table function. Interpolates `x` through the `(xs[i], ys[i])` breakpoints and **clamps** outside the range, the Vensim `WITH LOOKUP` convention. One pass over `[V]`, independent of breakpoint count. Shape-preserving. |
| `compare(a, b, op)` | Elementwise `gt` / `ge` / `lt` / `le` / `eq` / `ne`, yielding a 1.0/0.0 mask. Shape-preserving, so an `[E]` comparison yields an `[E]` mask. |
| `work_budget()` / `work_spent()` / `work_remaining()` | The native-work meter. Reading costs nothing — see the budget section below. |
| `zero_map(g)` | A zeroed `[V]` map. |
| `scatter(map, frontier, value)` | Write a scalar into the slots a vertex set selects. |
| `rekey(value, g)` | Move a `[V]` tensor or vertex set into another projection's index space, **verified** — fails unless both projections name the same vertices slot for slot. See [named scopes](#more-than-one-graph-named-scopes). |
| `map_apply(m, op, a, b)` | Generic map: `recip`, `scale`, `log`, `exp`, `sqrt`, `affine`, `floor`, `mod`, `normalize_l1`, `normalize_l2`. `mod` takes the divisor as the scalar `a`. |
| `recip` / `scale` / `normalize` | Fixed-form shorthands for the above. |
| `reduce_sum` / `reduce_sum_masked` | Deterministic sums (fixed-order, so bitwise-reproducible). |
| `arg_extreme(m, want_max)` | The extremal `(vertexId, value)`. |
| `topk(m, k)` | The ranked top-`k` pairs. |
| `l1_diff(a, b)` | Convergence metric for fixpoint loops. |

With `sqrt` and `exp` alongside `log` and `div`, the canonical UCT/UCB exploration term `c·√(ln N / n)` composes directly out of kernels — `map_apply(counts, "log")`, `ewise(…, visits, "div")`, then `map_apply(…, "sqrt")` — with no per-element guest code and no host round-trip.

### `floor` and `mod`

Both are single ops. The stepped-dynamics shape `(time % period) / period` is
`map_apply(t, "mod", 12.0)` for a literal period and `ewise(t, periods, "mod")` for a
column of them — one charge of the work meter either way, independent of the horizon.

The conventions, stated rather than left to be measured:

- **`floor` is floor, not truncation.** `⌊-1.5⌋ = -2`. It is IEEE
  `roundToIntegralTowardNegative`, matching `np.floor` on every edge: `floor(±0) = ±0`,
  `floor(±inf) = ±inf`, `floor(NaN) = NaN`, and anything at or above `2^53` is already
  integral and comes back unchanged.
- **`mod` takes the sign of the divisor** — Python's and NumPy's `%`, not C's `fmod`. So
  `5 mod -3 = -1` and `-5 mod 3 = 1`. A zero remainder also takes the divisor's sign
  (`-0.0 mod 3 = 0.0`), matching `np.mod` bit for bit.
- **`mod` by zero is `NaN`**, which is a deliberate exception to the `x/0 = 0` convention
  that `div` and `recip` follow. Those exist so a vertex with zero out-degree drops out of
  a normalization — a legitimate graph fact. A zero modulus is not a legitimate anything,
  it is a broken parameter, and returning `0` would hide it.

**Do not compose `mod` from `floor`.** The identity `x - y·⌊x/y⌋` holds in ℝ but not in
binary64: once `x/y` exceeds `2^53` the quotient has no fractional bits left, `floor`
becomes the identity, and the expression collapses to `0.0` — silently, and still inside
`[0, y)`, so a range check on the result will not catch it. Any unbounded accumulator
reaches that regime, and a small divisor reaches it early (`y = 0.001` fails from
`x ≈ 1e12`). The `mod` op delegates to the exact IEEE remainder and is correct at every
magnitude; that is why it exists as its own op rather than as a published composition.


### When `node_property` / `edge_property` return NaN

The consumer-facing rule has two halves, and they behave differently:

- A property you **did not request** in the projection spec is an **error**:
  `edge_property: \`w\` was not projected (add it to edgeProperties)`. The
  `nodeProperties` / `edgeProperties` config keys are what make a column exist at
  all — a kernel cannot read a column the projection never materialized.
- A property you **did** request but which has no value for a given element is
  `NaN`, per element, silently. That is deliberate: `NaN` is an honest "no value
  here", distinct from a real `0.0`, and it propagates visibly into `reduce_sum`.

A requested name that is declared on **none** of the projected labels/edge types
*and* resolves for **no** projected element is rejected outright — that is a typo,
not data. A name declared on at least one projected label is accepted across a
heterogeneous projection; the labels that do not carry it yield `NaN`. The same
check covers `weightProperty`, where the old failure mode was worse: an unknown
weight name silently defaulted every edge weight to `1.0`, which is
indistinguishable from real data.

So, after that check, the remaining ways to see `NaN` are:

| Cause | Detail |
| --- | --- |
| Heterogeneous projection | The element's label does not declare the property. |
| Non-numeric value | Only `Float` and `Int` convert; `Bool`, `String`, `Null` and `Bytes` all read `NaN`. |
| Deleted element | A tombstone in a newer L0 generation removes the entry. |
| Time-travel pin | A pinned snapshot drops entries above its version high-water mark. |

Two related contracts worth knowing:

- **Cypher / Named projections carry no property columns at all**, so
  `node_property` / `edge_property` against one always *error* — they never return
  `NaN`. Use a label/edge-type-scoped projection when you need property tensors.
- **Edge weight is not an edge property.** A missing weight falls back to `1.0`
  (the traversal default), never `NaN`.

### Composing operations the catalog does not name

The kernel set is deliberately coarse and the op vocabularies are closed, so `ewise` has no
`gt` and `map_apply` has no `step`. That does **not** mean thresholds and conditionals are out
of reach: `map_to_set` carries a closed predicate enum and `set_to_map` lifts the resulting
bitset back to a `[V]` map, which composes with `ewise mul` into an ordinary masked blend.

Ask for an op that is not in a vocabulary and the engine now answers with the composition:

```
bad ewise op `gt` — elementwise comparison is composable:
set_to_map(map_to_set(ewise(a, b, "axpy", -1.0), "gt", 0.0), 1.0);
valid ewise ops: add, mul, min, max, axpy, div
```

| You wanted | Compose it as |
| --- | --- |
| `a > b` (also `ge`/`lt`/`le`/`eq`/`ne`) | `compare(a, b, "gt")` — a kernel since 3.0; the older `set_to_map(map_to_set(...))` composition still works but costs three passes of the work meter |
| `step(a, theta)` / `heaviside` | `set_to_map(map_to_set(a, "gt", theta), 1.0)` |
| `select(m, a, b)` | `ewise(b, ewise(m, ewise(a, b, "axpy", -1.0), "mul"), "add")` |
| `a - b` | `ewise(a, b, "axpy", -1.0)` |
| `a - c` (constant) | `map_apply(a, "affine", 1.0, -c)` |
| `relu(a)` | `ewise(a, set_to_map(map_to_set(a, "gt", 0.0), 1.0), "mul")` |
| `abs(a)` | `ewise(a, map_apply(a, "scale", -1.0), "max")` |
| `clip(a, lo, hi)` | `ewise(ewise(a, lo, "max"), hi, "min")` |
| `a ** 2`, integer powers | `repeat ewise(x, x, "mul"); x squared is ewise(a, a, "mul")` |
| `-a` | `map_apply(a, "scale", -1.0)` |
| `ceil(a)` | `map_apply(map_apply(map_apply(a, "scale", -1.0), "floor"), "scale", -1.0)` — exact, since `⌈x⌉ = -⌊-x⌋`. `round` and `trunc` have no exact composition from `floor` and are not offered as one. |
| `normalize(m)` | `map_apply(m, "normalize_l1") or map_apply(m, "normalize_l2")` |
| an exact edge mask | `edge_mask_window(edge_property(g, prop), 0.5, 1.5)` |

Two caveats worth knowing before you rely on them:

- **The `select` blend poisons on NaN.** `0.0 * NaN = NaN`, so a NaN in the *unselected* branch
  propagates. Where either branch may be NaN, `scatter` over the `VertexSet` instead of
  multiplying by its lowered mask.
- **A composed comparison charges the native-work meter three times** (`ewise` + `map_to_set` +
  `set_to_map`, each `|V|`), where a primitive comparison would charge once.

**Trailing scalars are optional.** The recipes above are written at the shortest arity that
carries their meaning — `map_apply(a, "scale", -1.0)` omits `map_apply`'s second scalar,
`ewise(b, …, "add")` omits the `axpy` coefficient — and every loader accepts them that way,
defaulting the omitted arguments to `0.0`. `map_apply(counts, "log")` is a complete call.
So a recipe can be copied out of this page, or out of the engine's own rejection message,
and run as written.

These identities are executed against an independent scalar oracle on every CI run
(`composition_recipes.rs`), **and are executed again through the Rhai and Python guest
surfaces** so that the published spelling is known to resolve there and not merely to be
expressible in Rust. A recipe that stops being true breaks the build rather than
quietly misleading you.
### Traversal

| Kernel | Returns |
| --- | --- |
| `expand(g, frontier, dir, exclude)` | Neighbor expansion, direction-optimized. `dir` is `"out"`, `"in"`, or `"both"`. `exclude` is a vertex set to skip — pass your visited mask to get a BFS step. |
| `spmv(g, m, semiring, dir)` | Sparse matrix-vector product over a named semiring: `reachability`, `shortest_path`, `propagate`, `linear_algebra`, `min_max`. `dir` accepts `"both"`; with `"both"` the product is **unweighted** (the reverse CSR carries no weights). |
| `reach_fixpoint(g, seeds, dir)` | The full reachable set from `seeds` — BFS run to fixpoint in one native `O(V+E)` call, so a guest never hand-writes the frontier loop (and can't accidentally write the `O(V·E)` version). |
| `expand_masked(g, frontier, dir, exclude, edge_mask)` / `spmv_masked(g, vec, semiring, edge_mask)` | The same, restricted to an edge mask — the result equals the kernel run on the subgraph of exactly the masked edges. |
| `expand_sampled(g, frontier, dir, exclude, prob, seed, iter)` | Frontier-scoped lazy sampled expansion: draws a Bernoulli only for the **current** frontier's out-edges (vs `sample_edges`, which eagerly draws all `E`). Out-direction only. The efficient percolation / influence-cascade primitive. |
| `next_bucket(dist, delta, bucket)` | Delta-stepping bucket selection. |

#### Direction `both`

`expand`, `degrees`, and `spmv` accept `dir="both"` — the union of a vertex's out- and in-edges. It is **fail-loud**: `"both"` requires a projection built with `includeReverse: true`, and calling it against a projection without the reverse CSR raises a typed error naming `includeReverse`. Build the projection with the reverse adjacency, or use `"out"` / `"in"`. `spmv` with `"both"` is unweighted, because the reverse CSR carries no edge weights.

### Edge sets

| Kernel | Returns |
| --- | --- |
| `edge_set_len` | Cardinality of an edge mask. |
| `sample_edges(prob, seed, iter)` | Per-edge Bernoulli mask from a reproducible counter-hash; `prob` is an `[E]` tensor. **Guaranteed** exact at the endpoints — `prob = 0.0` never fires, `prob = 1.0` always does, for every seed. |
| `sample_edges_undirected(g, prob, seed, iter)` | Like `sample_edges`, but both half-edges of an undirected pair (`u→v` and `v→u`) share **one** Bernoulli draw, keyed on the canonical unordered endpoint pair — so an undirected link is up-or-down as a unit. For simple undirected graphs (not multigraphs with parallel edges). |
| `edge_mask_window(vals, lo, hi)` | Threshold an `[E]` column into a mask — e.g. a temporal window. |
| `edge_intersect` / `edge_union` | Mask algebra. |

!!! note "Selecting edges by a stored class — use `edge_mask_window`"

    A common need is restricting an operation to one edge class inside a single projection: store a 1.0/0.0 selector property per edge, then build a mask from it. `edge_mask_window` does this directly and deterministically, with no RNG in the path:

    ```rhai
    let sel = gc.edge_property(g, "sel_agg");        // 1.0 on :AGGREGATES, 0.0 elsewhere
    let m   = gc.edge_mask_window(sel, 0.5, 1.5);    // exactly the 1.0 edges
    let agg = gc.spmv_masked(g, contrib, "linear_algebra", m);
    ```

    `sample_edges(sel, seed, iter)` gives the same mask, because the endpoint behaviour above is a guarantee rather than an accident. Prefer `edge_mask_window` anyway: it says what it means, and it cannot be misread as depending on the seed.
| `segmented_reduce(values, groups)` | Group a `[V]` map by a label map and reduce, **bitwise-identical regardless of vertex order or partitioning**. |

Together these express reachability or SpMV over a random edge subset (percolation, influence maximization) or a per-event-time subset (temporal reachability) — with no per-element guest code.

### Walks and sampling

| Kernel | Returns |
| --- | --- |
| `random_walks(g, seeds, len, n, p, q, seed)` | node2vec-biased walks. |
| `walk_visit_counts(walks, g)` | `[V]` visit histogram. |
| `sample(prob, seed, iter)` | Reproducible `Bernoulli(prob[v])` mask. Same counter-hash stream as walk seeding, so results are bitwise-identical across runs and threads; a fresh `iter` decorrelates. |
| `emit_walks(walks)` | Emit ragged walk rows. |

### Similarity

| Kernel | Returns |
| --- | --- |
| `neighborhood_overlap(g, source, metric)` | Overlap of `source` against its neighbourhood. Metrics: `count`, `jaccard`, `overlap`, `cosine`, `adamic_adar`. |
| `all_pairs_overlap(g, metric, pair_mode, k)` | `pair_mode` is `"adjacent"` or `"topk"`. |
| `emit_pairs(pairs)` | Emit `(srcId, dstId, value)` rows. |

### Mutable arena (`graph-arena@1`)

| Kernel | Returns |
| --- | --- |
| `arena_new(capacity, branching)` | An arena sized for `capacity` slots with `branching` child slack. |
| `arena_alloc(a, count)` | Bump-allocate slots; returns their ids. |
| `arena_expand(a, parents, fanout)` | Allocate `fanout` children per parent **and link them** — the tree-growth primitive. |
| `arena_link(a, parents, kids)` | Link explicit parent/child pairs. |
| `arena_column(a)` | A zeroed `[capacity]` `f64` state column; returns its index. Call it repeatedly — a search node needs *both* visits and value. |
| `arena_candidates(a, roots)` | The children of a frontier, concatenated. |
| `arena_gather(a, col, slots)` / `arena_scatter(a, col, slots, values)` | Move state between a column and a compact tensor. |
| `arena_descend(a, roots, score, visit, maximize, vloss)` | Descend to a leaf choosing the best-scoring child, applying one visit and the virtual loss at every step. `vloss` is a flat linear offset applied in the descent loop, **not** a per-visit UCB recompute — see the [worked example](#worked-example-a-guest-authored-mcts). |
| `arena_backup(a, value_col, leaves, deltas)` | Add each leaf's delta along its **full** root path, walking parents to the root — general-depth UCT / PUCT value backpropagation, not just a depth-1 bandit update. |
| `arena_seed(a, g)` | Copy a projection's topology into an **empty** arena — slot `i` becomes vertex `i` of `g`. The way an existing network enters a growable arena, so newborns allocated afterwards extend it rather than starting from nothing. |
| `arena_freeze(a)` | Compact into an ordinary graph handle. Slot `i` of the frozen graph is slot `i` of the arena, so arena-keyed values aggregate over it directly — see below. |

### Egress and lifecycle

| Kernel | Returns |
| --- | --- |
| `emit(name, handle)` | Emit a result column. |
| `free(h)` | Release a handle. **In a loop, free your intermediates** — the handle cap is a real bound, and a leaky guest hits `0x864` within a few hundred iterations. |

---

## Worked example: a guest-authored MCTS

This Rhai script owns the search. The tree shape, exploration constant, score formula and stopping rule are all the guest's; the host only runs the loops that do bulk work.

```rust
let arena = sess.arena_new(8191, 2);
let root  = sess.arena_alloc(arena, 1);

let visits = sess.arena_column(arena);
let value  = sess.arena_column(arena);
let score  = sess.arena_column(arena);

// Grow a binary tree, level by level.
let frontier = root;
for d in 0..12 {
    frontier = sess.arena_expand(arena, frontier, 2);
}

let c = 1.41;
let n = 0;
while n < 4096 {
    // Score only the candidates, not all 8191 slots.
    let cand = sess.arena_candidates(arena, root);
    let v    = sess.arena_gather(arena, visits, cand);
    let w    = sess.arena_gather(arena, value, cand);

    // UCB, composed by the guest from ordinary kernels.
    let inv  = sess.recip(v);
    let mean = sess.ewise(w, inv, "mul", 0.0);
    let expl = sess.scale(inv, c * c);
    let ucb  = sess.ewise(mean, expl, "add", 0.0);
    sess.arena_scatter(arena, score, cand, ucb);

    let leaves = sess.arena_descend(arena, root, score, visits, true, 0.35);

    sess.free(cand); sess.free(v);    sess.free(w);
    sess.free(inv);  sess.free(mean); sess.free(expl);
    sess.free(ucb);  sess.free(leaves);
    n += 1;
}
sess.arena_freeze(arena)   // now an ordinary graph — run PageRank on it if you like
```

Two details that matter more than they look:

**Score the candidates, not the column.** `arena_candidates` returns only the children of the active frontier. Rescoring all `N` slots each level instead costs about **6×** more, because it does `O(N)` work where only `O(frontier × branching)` entries can possibly be chosen.

**The virtual loss is not optional.** `arena_descend`'s `vloss` is applied *inside* the descent loop, not after the batch. Without it every rollout in a batch descends against identical statistics and follows the same path — in measurement, a 1024-rollout batch collapsed onto **16 distinct leaves**. With it, 1024.

`vloss` is a **flat linear score offset** subtracted per step in the descent, not a per-visit UCB recompute against updated visit counts. It is deliberately cheap: it diversifies a batch without re-deriving the exploration term at every node. A guest that needs bit-exact parity with a hand-written per-visit engine — one that recomputes UCB against the incremented visit count on each step — should descend at `vloss=0` and update statistics itself between single-rollout descents. Value backpropagation up the chosen path is a separate step: `arena_backup` adds each leaf's delta along its full root path, so UCT/PUCT backprop is one native call rather than a guest-side parent walk.

---

## Determinism, budgets, and failure

**Deterministic.** CSR ordering is canonical and reductions are fixed-order, so results are bitwise-reproducible across thread counts. Sampling draws from a stateless counter-hash, so a seeded run repeats exactly. `arena_descend` breaks ties by lower slot.

Batching is *not* semantically free: rollouts in one batch descend against the same statistics a sequential search would have updated. That is a declared property, and the virtual-loss idiom above is how search workloads handle it.

**Budgets are fail-closed.** A native-work meter (`GraphComputeWork`) and a handle-memory cap (`GraphComputeArenaBytes`) both halt a runaway guest. Batched kernels charge the *full* work they do — batching amortizes the boundary crossing, never the meter, so you cannot evade the budget by batching harder.

**Non-convergence is a hard error**, never a silent partial result.

| Code | Meaning |
| --- | --- |
| `0x860` / `0x861` / `0x863` | Stale handle / wrong kind / wrong session. |
| `0x862` | Operand shape mismatch. |
| `0x864` | Handle-memory or arena capacity exceeded. |
| `0x865` | Native-work budget exhausted. |
| `0x866` / `0x867` | Iteration limit / wall-clock deadline. |
| `0x86A` | A declared capability slice the host does not provide. |
| `0x86C` | Kernel called without the `GraphCompute` grant. |
| `0x86E` | Invalid argument, unknown kernel name, or out-of-range slot. |

---

## Loader notes

| Loader | Kernel surface |
| --- | --- |
| **Rhai** / **PyO3** | Native per-kernel methods on a session object. Handles are plain integers — no marshalling. |
| **WASM Component Model** | Two imports: `host-graph` (one generic JSON entrypoint carrying the whole catalogue) and `host-arena` (typed functions carrying handles directly, for the hot path). |
| **Extism** | One host function, `uni_graph_call`, carrying the catalogue as JSON. |

A guest built before `host-arena` existed is unaffected by it — the interface is additive, and a component that does not import it links exactly as before.

The kernel catalogue is a *type* in the host, not a list of strings: adding a kernel fails to compile until it is dispatched, and a per-loader test asserts every kernel is exposed by Rhai and PyO3. A kernel cannot be callable from one loader and silently invisible to another.

### Tensor identity: shape and index space

!!! warning "Breaking change — index space is enforced on every value kind"

    Provenance began on tensors. It now covers **vertex sets, edge sets, walk matrices and pair
    lists** as well, and the kernels that egress vertex ids (`emit`, `topk`, `arg_extreme`,
    `emit_walks`, `emit_pairs`) label their output using *the value's own* projection rather than
    the first one bound. A program that mixed values from two projections previously returned
    right-looking, wrong answers; it now fails with `0x862`. Single-projection algorithms — which
    is every algorithm expressible before this release — are unaffected.

A value is not just a length. It carries the **shape** it was built with (`[V]` per-vertex or
`[E]` per-edge) and the **index space** its slots belong to — the projection or arena it came
from. Both are enforced:

- Combining a `[V]` map with an `[E]` tensor fails, even when the lengths coincide (any graph
  where `|V| == |E|`, such as a cycle, used to make that silent).
- Combining tensors from two different projections fails. Slot `i` names a different vertex in
  each, so equal lengths do not make them comparable. Arena-derived tensors are keyed to arena
  slots, which is a third space again.
- `ewise`, `map_apply`, `compare` and `segmented_reduce` all *preserve* shape and index space, so
  an `[E]` pipeline stays `[E]` all the way to `sample_edges` or `edge_mask_window`.

**Sets carry an index space too**, and so do walk matrices and pair lists. A `VertexSet` is keyed
to the vertices of the projection it came from, an `EdgeSet` to that projection's CSR edge order.
So a foreign operand is rejected wherever one is mixed with a graph or another set: the set
algebra, `expand` / `expand_masked` / `expand_sampled` (frontier, `exclude` *and* edge mask),
`spmv` and `spmv_masked`, `scatter`, a masked `reduce`, `walk_visit_counts`, and
`sample_edges_undirected`.

The `exclude` operand is worth calling out. `reach_fixpoint` feeds the accumulated visited-set
back as `exclude` on every iteration, so a foreign set there would not produce one wrong answer —
it would prune the frontier each round and return a plausible under-approximation with no error
at all.

Because every value kind is now tagged, `set_to_map` **inherits** the set's space rather than
producing an untagged value. The *unknown* space still exists as the permissive element — it
unifies with anything and the result adopts whichever space is known — but nothing a guest can
build lands there any more.

Two consequences worth knowing:

- **Egress is keyed to the value, not to the session.** `topk` and `arg_extreme` return the vids
  of the projection *their tensor* came from; `emit_walks` and `emit_pairs` translate their slots
  the same way. An arena-keyed value is rejected outright, since arena slots are not any
  projection's vertices.
- **`emit` checks identity before length.** A column from the wrong projection is reported as
  such, rather than passing the length check and silently mislabelling every row.

### The native-work budget

Every invocation gets a finite budget of *native-work units*, and each kernel
charges roughly the work it does. Exceeding it aborts the CALL with `0x865`
(`GraphComputeIncomplete`, reason `Exhausted`) — never a silently truncated result.

**The default is `min(10_000 x (|V| + |E| + 1), 1_000_000_000)`.** Both `|V|` and
`|E|` appear because the meter charges the `O(V)` kernels as well as the `O(E)`
ones. The absolute ceiling binds above roughly 100k projected elements, so the
linear term alone is not the whole rule.

A `GraphComputeWork` capability grant **replaces** that default rather than
raising it — an explicit grant is authoritative in both directions.

**What each kernel charges:**

| Kernel family | Charge |
| --- | --- |
| `spmv`, `spmv_masked`, `edge_weights`, `edges_all` | `|E|` |
| `expand`, `expand_masked`, `expand_sampled` | the frontier's total degree |
| `sample_edges`, `sample_edges_undirected` | `|E|`, checked in chunks |
| `degrees`, `vertex_ids`, `ewise`, `compare`, `map_apply`, `reduce_sum`, `reduce_sum_masked`, `zero_map`, `scatter`, `set_to_map`, `map_to_set`, `l1_diff`, `arg_extreme`, `topk`, `next_bucket`, `node_property`, `edge_property`, `segmented_reduce`, `edge_mask_window` | one unit per element |
| `frontier` | one unit per seed |
| `rekey` | `|V|` — it walks both projections' slot→Vid maps to verify the correspondence |
| `set_union` / `set_diff` / `set_intersect` | one unit per 64-bit *word*, not per element |
| `emit` | `rows x columns` |
| `random_walks`, `walk_visit_counts`, `emit_walks` | total steps |
| `neighborhood_overlap`, `all_pairs_overlap` | work done, at least `|V|` |
| arena kernels | their own footprint (capacity, path length, or neighbours touched) |
| `work_budget` / `work_spent` / `work_remaining`, `set_len`, `is_empty`, `free`, `vertex_count`, `edge_count` | nothing |

Expensive kernels re-check mid-loop, so a single celebrity-vertex `expand` cannot
blow past the cap. Batching amortizes the boundary crossing but never the meter:
a batched kernel charges the same as the calls it replaces.

**Reading the meter from a guest:**

```rhai
let left = gc.work_remaining();   // free; so are work_budget and work_spent
```

Sizing work ahead of a run is better done from the formula than from the meter:
branching on `work_remaining()` makes a kernel's *result* depend on the capability
grant, so the same program under two grants can return different answers. That
does not violate the determinism contract, which is per-configuration, but it does
forfeit cross-grant reproducibility — and a differential oracle would see it as
drift. Discovering the ceiling by deliberately triggering aborted CALLs is never
necessary: it is a pure function of `|V|` and `|E|`.

### Tracing handle resolutions

Setting `UNI_GC_TRACE` makes a session remember its recent handle resolutions, and
attaches them to any handle error it raises:

```
GraphComputeError: stale handle [gc-trace, oldest first, epoch:kind:gen:slot —
0007:1:000:0 0007:2:000:1 0007:1:001:0]
```

The trace is bounded (the tail leading up to a failure, not the whole history) and
rides the error rather than a separate log, so it reaches whoever ran the query
without any extra plumbing. It is recorded at the point every loader shares, so a
Rhai guest is covered as well as the sandboxed ones.

It is always compiled and inert unless the variable is set — with it unset,
nothing is recorded and error messages are byte-identical. That is deliberate:
diagnostics you cannot switch on in an already-shipped build are no help against a
problem that only appears in production.
