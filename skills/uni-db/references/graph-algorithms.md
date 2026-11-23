# Graph Algorithms Reference

Uni includes **35+ graph algorithms** accessible via `CALL uni.algo.<name>(...)` procedures.

---

## 1. Algorithm Catalog

### Path Finding (8 algorithms)

| Procedure | Description | Use Case | Execution Mode |
|---|---|---|---|
| `uni.algo.shortestPath` | Single-source shortest path (unweighted BFS) | Routing, navigation | DirectTraversal |
| `uni.algo.dijkstra` | Single-source shortest paths (weighted Dijkstra) | Routing, reachability cost | GraphProjection |
| `uni.algo.bidirectionalDijkstra` | Bidirectional shortest path (weighted) | Faster point-to-point | GraphProjection |
| `uni.algo.bellmanFord` | Shortest path with negative weights | Financial arbitrage | GraphProjection |
| `uni.algo.astar` | A* with heuristic guidance | Spatial routing | GraphProjection |
| `uni.algo.kShortestPaths` | K distinct shortest paths | Alternative routes | GraphProjection |
| `uni.algo.allSimplePaths` | All simple paths between two nodes | Dependency analysis | GraphProjection (positional args) |
| `uni.algo.allPairsShortestPath` | Floyd-Warshall all pairs | Network diameter | GraphProjection |

### Centrality (7 algorithms)

| Procedure | Description | Use Case |
|---|---|---|
| `uni.algo.degreeCentrality` | In/out/total degree | Hub identification |
| `uni.algo.betweenness` | Shortest-path betweenness | Bridge nodes, bottlenecks |
| `uni.algo.closeness` | Average distance to all others | Information spread |
| `uni.algo.harmonicCentrality` | Harmonic centrality | Disconnected graphs |
| `uni.algo.eigenvectorCentrality` | Eigenvector centrality (iterative) | Influence measurement |
| `uni.algo.katzCentrality` | Katz centrality | Status in social networks |
| `uni.algo.pageRank` | PageRank (iterative) | Web ranking, importance |

### Community Detection (4 algorithms)

| Procedure | Description | Use Case |
|---|---|---|
| `uni.algo.wcc` | Weakly Connected Components (union-find) | Cluster identification |
| `uni.algo.scc` | Strongly Connected Components (Tarjan) | Cycle groups |
| `uni.algo.louvain` | Louvain modularity optimization | Community structure |
| `uni.algo.labelPropagation` | Label propagation (semi-synchronous) | Fast community detection |

### Similarity (1 algorithm)

| Procedure | Description | Use Case |
|---|---|---|
| `uni.algo.nodeSimilarity` | Neighborhood overlap (Jaccard/Cosine/Overlap) | Similar users/items |

### Structural (4 algorithms)

| Procedure | Description | Use Case |
|---|---|---|
| `uni.algo.triangleCount` | Count triangles per node | Clustering coefficient |
| `uni.algo.topoSort` | DAG topological ordering | Build systems, dependencies |
| `uni.algo.hasCycle` | Detect cycles | Deadlock detection |
| `uni.algo.isBipartite` | Bipartite graph verification | Two-coloring |

### Connectivity (3 algorithms)

| Procedure | Description | Use Case |
|---|---|---|
| `uni.algo.bridges` | Bridge edge detection | Network reliability |
| `uni.algo.articulationPoints` | Cut vertex detection | Single points of failure |
| `uni.algo.kCore` | K-core decomposition | Dense subgraph discovery |

### Flow & Matching (3 algorithms)

| Procedure | Description | Use Case |
|---|---|---|
| `uni.algo.fordFulkerson` | Maximum flow (Ford-Fulkerson) | Network capacity |
| `uni.algo.maxFlow` | Maximum flow (Dinic's algorithm) | Large flow networks |
| `uni.algo.maxMatching` | Maximum cardinality matching | Assignment problems |

### Miscellaneous (7 algorithms)

| Procedure | Description | Use Case |
|---|---|---|
| `uni.algo.mst` | Minimum spanning tree (Kruskal) | Network design |
| `uni.algo.randomWalk` | Random walk sampling | Graph embedding, sampling |
| `uni.algo.elementaryCircuits` | All elementary cycles | Circuit analysis |
| `uni.algo.maximalCliques` | Maximal clique enumeration | Dense groups |
| `uni.algo.graphColoring` | Graph coloring (greedy) | Scheduling, register allocation |
| `uni.algo.metrics` | Global metrics (diameter, radius, center) | Graph summary |
| `uni.algo.diameter` | Graph diameter | Network diameter |

---

## 2. General Execution Pattern

### CALL Syntax

All projection-based algorithms share a common first two arguments:

```cypher
CALL uni.algo.<name>(
    ['NodeLabel1', 'NodeLabel2'],   // nodeLabels: List (required)
    ['REL_TYPE1', 'REL_TYPE2'],     // relationshipTypes: List (required)
    <algorithm-specific args...>     // optional, with defaults
)
YIELD col1, col2, ...
RETURN col1, col2
```

shortestPath and allSimplePaths take positional arguments (start, end, relationship types) instead of leading nodeLabels/relationshipTypes. Built-in algorithms require declared labels and edge types.

### Execution Modes

**DirectTraversal** -- Zero-copy BFS on AdjacencyManager + L0Buffer. Used for single-source path queries. Fast startup, streaming results, no materialization.

**GraphProjection** -- Materialized dense CSR graph in memory. Used for iterative algorithms (PageRank, WCC, Louvain, betweenness, eigenvector, etc.).

---

## 3. Path Algorithms

### uni.algo.shortestPath

```
Args:     sourceNode (Node), targetNode (Node), relationshipTypes (List)
Yields:   nodeIds (List), edgeIds (List), length (Int)
Mode:     DirectTraversal
```

```cypher
CALL uni.algo.shortestPath($startId, $endId, ['ROAD'])
YIELD nodeIds, length
RETURN nodeIds, length
```

### uni.algo.dijkstra

```
Args:     nodeLabels (List), relationshipTypes (List), startNode (Node)
Optional: endNode (Node), weightProperty (String), maxDistance (Float)
Yields:   nodeId (Int), distance (Float), path (List)
Note:     Without endNode, yields every reachable node (path is empty).
          Optional args are positional: pass endNode to pass weightProperty.
          Unweighted (distance = hop count) without weightProperty.
```

```cypher
CALL uni.algo.dijkstra(['City'], ['ROUTE'], $sourceId, $targetId, 'cost')
YIELD nodeId, distance, path
RETURN nodeId, distance, path
```

### uni.algo.bellmanFord

```
Args:     nodeLabels (List), relationshipTypes (List), sourceNode (Node),
          weightProperty (String, default: null)
Yields:   nodeId (Int), distance (Float)
Note:     Errors if negative cycle detected.
```

```cypher
CALL uni.algo.bellmanFord(['City'], ['ROUTE'], $sourceId, 'cost')
YIELD nodeId, distance
RETURN nodeId, distance
ORDER BY distance ASC
```

### uni.algo.astar

```
Args:     startNode (Node), endNode (Node), edgeType (String), heuristicProperty (String)
Yields:   path (List), cost (Float)
```

### uni.algo.kShortestPaths

```
Args:     nodeLabels (List), relationshipTypes (List), startNode (Node), endNode (Node),
          k (Int), weightProperty (String, default: null)
Yields:   path (List), cost (Float), rank (Int)
```

### uni.algo.allSimplePaths

```
Args:     startNode (Node), endNode (Node), relationshipTypes (List), maxLength (Int)
Optional: nodeLabels (List, default: null)
Yields:   path (List of node ids)
Note:     Builds a projection over relationshipTypes; returns at most 1000 paths.
```

### uni.algo.allPairsShortestPath

```
Args:     nodeLabels (List), relationshipTypes (List)
Yields:   sourceNodeId (Int), targetNodeId (Int), distance (Int)
```

---

## 4. Centrality Algorithms

### uni.algo.pageRank

```
Args:     nodeLabels (List), relationshipTypes (List)
Optional: dampingFactor (Float, default: 0.85), maxIterations (Int, default: 20),
          tolerance (Float, default: 1e-6)
Yields:   nodeId (Int), score (Float)
```

```cypher
CALL uni.algo.pageRank(['Person'], ['KNOWS'], 0.85, 20, 0.0001)
YIELD nodeId, score
RETURN nodeId, score
ORDER BY score DESC
LIMIT 10
```

### uni.algo.betweenness

```
Args:     nodeLabels (List), relationshipTypes (List)
Optional: normalize (Bool, default: true), samplingSize (Int, default: null)
Yields:   nodeId (Int), score (Float)
```

```cypher
CALL uni.algo.betweenness(['Person'], ['KNOWS'], true, 100)
YIELD nodeId, score
RETURN nodeId, score
ORDER BY score DESC
```

### uni.algo.degreeCentrality

```
Args:     nodeLabels (List), relationshipTypes (List)
Optional: direction (String: 'OUTGOING'|'INCOMING'|'BOTH', default: 'OUTGOING')
Yields:   nodeId (Int), score (Float)
```

### uni.algo.closeness

```
Args:     nodeLabels (List), relationshipTypes (List)
Optional: wassermanFaust (Bool, default: false)
Yields:   nodeId (Int), score (Float)
```

### uni.algo.eigenvectorCentrality

```
Args:     nodeLabels (List), relationshipTypes (List)
Optional: maxIterations (Int, default: 100), tolerance (Float, default: 1e-6),
          weightProperty (String, default: null)
Yields:   nodeId (Int), score (Float)
```

### uni.algo.katzCentrality

```
Args:     nodeLabels (List), relationshipTypes (List)
Optional: alpha (Float, default: 0.1), beta (Float, default: 1.0),
          maxIterations (Int, default: 100), tolerance (Float, default: 1e-6),
          weightProperty (String, default: null)
Yields:   nodeId (Int), score (Float)
```

### uni.algo.harmonicCentrality

```
Args:     nodeLabels (List), relationshipTypes (List)
Yields:   nodeId (Int), centrality (Float)
```

---

## 5. Community Detection

### uni.algo.wcc

```
Args:     nodeLabels (List), relationshipTypes (List)
Optional: minComponentSize (Int, default: 1)
Yields:   nodeId (Int), componentId (Int)
```

```cypher
CALL uni.algo.wcc(['Device'], ['CONNECTED_TO'])
YIELD nodeId, componentId
RETURN componentId, collect(nodeId) AS members
```

### uni.algo.louvain

```
Args:     nodeLabels (List), relationshipTypes (List)
Optional: resolution (Float, default: 1.0), maxIterations (Int, default: 10),
          minModularityGain (Float, default: 1e-4)
Yields:   nodeId (Int), communityId (Int)
```

```cypher
CALL uni.algo.louvain(['Person'], ['KNOWS'])
YIELD nodeId, communityId
RETURN communityId, count(*) AS size
ORDER BY size DESC
```

### uni.algo.labelPropagation

```
Args:     nodeLabels (List), relationshipTypes (List)
Optional: maxIterations (Int, default: 10), write (Bool, default: false),
          writeProperty (String, default: 'community'),
          seed (Int, default: null -- fixed default seed)
Yields:   nodeId (Int), communityId (Int)
```

### uni.algo.scc

```
Args:     nodeLabels (List), relationshipTypes (List)
Yields:   nodeId (Int), componentId (Int)
```

---

## 6. Similarity

### uni.algo.nodeSimilarity

```
Args:     nodeLabels (List), relationshipTypes (List)
Optional: metric (String: 'JACCARD'|'COSINE'|'OVERLAP', default: 'JACCARD'),
          similarityCutoff (Float, default: 0.1), topK (Int, default: 10)
Yields:   node1 (Int), node2 (Int), similarity (Float)
```

```cypher
CALL uni.algo.nodeSimilarity(['User'], ['PURCHASED'], 'JACCARD', 0.3, 5)
YIELD node1, node2, similarity
RETURN node1, node2, similarity
ORDER BY similarity DESC
```

---

## 7. Structural & Connectivity

bridges, articulationPoints, mst, maxMatching and dijkstra return integer node ids; resolve them with `MATCH (n:Label) WHERE id(n) = node`.

### uni.algo.triangleCount

```
Args:     nodeLabels (List), relationshipTypes (List)
Yields:   nodeId (Int), triangleCount (Int)
```

### uni.algo.topoSort

```
Args:     nodeLabels (List), relationshipTypes (List)
Yields:   nodeId (Int), order (Int)
Note:     Errors if graph has a cycle.
```

### uni.algo.hasCycle

```
Args:     nodeLabels (List), relationshipTypes (List)
Yields:   hasCycle (Bool), cycleNodes (List)
```

### uni.algo.isBipartite

```
Args:     nodeLabels (List), relationshipTypes (List)
Yields:   isBipartite (Bool), partition (Map)
```

### uni.algo.bridges

```
Args:     nodeLabels (List), relationshipTypes (List)
Yields:   source (Int), target (Int)     -- node ids
```

### uni.algo.articulationPoints

```
Args:     nodeLabels (List), relationshipTypes (List)
Yields:   node (Int)                     -- node id
```

### uni.algo.mst

```
Args:     nodeLabels (List), relationshipTypes (List), weightProperty (String)
Yields:   source (Int), target (Int), weight (Float)     -- node ids
```

### uni.algo.kCore

```
Args:     nodeLabels (List), relationshipTypes (List)
Optional: k (Int, default: null -- computes all core numbers)
Yields:   nodeId (Int), coreNumber (Int)
```

```cypher
CALL uni.algo.kCore(['Person'], ['KNOWS'])
YIELD nodeId, coreNumber
RETURN nodeId, coreNumber
ORDER BY coreNumber DESC
```

---

## 8. Flow & Matching

### uni.algo.fordFulkerson

```
Args:     nodeLabels (List), relationshipTypes (List), sourceNode (Node),
          sinkNode (Node), capacityProperty (String)
Yields:   maxFlow (Float)
```

### uni.algo.maxFlow

```
Args:     nodeLabels (List), relationshipTypes (List), sourceNode (Node),
          sinkNode (Node), capacityProperty (String)
Yields:   maxFlow (Float), flowEdges (Int)
Note:     Uses Dinic's algorithm. Preferred over fordFulkerson for large networks.
```

```cypher
CALL uni.algo.maxFlow(['Router'], ['LINK'], $sourceId, $sinkId, 'bandwidth')
YIELD maxFlow
RETURN maxFlow
```

### uni.algo.maxMatching

```
Args:     nodeLabels (List), relationshipTypes (List)
Yields:   node1 (Int), node2 (Int), matchId (Int)     -- node ids
Note:     Errors if the graph is not bipartite.
```

---

## 9. Configuration & Tuning

### Graph Projection Options

Projection-based algorithms automatically build a CSR from the specified labels and types.

| Parameter | Effect |
|---|---|
| `nodeLabels` | Filter to nodes with these labels only |
| `relationshipTypes` | Filter to edges of these types only |
| `weightProperty` | Edge property to use as weight (bellmanFord, kShortestPaths, mst, eigenvector, katz) |
| `capacityProperty` | Edge property for flow capacity (fordFulkerson, maxFlow) |
| `heuristicProperty` | Node property for A* heuristic |

### Directed vs. Undirected

Projections build in-neighbor (reverse) adjacency by default; bellmanFord, dijkstra, kShortestPaths, mst, eigenvectorCentrality, katzCentrality, fordFulkerson, maxFlow, diameter and metrics skip it. Each algorithm defines its own direction semantics: scc, topoSort and hasCycle follow edge direction. `degreeCentrality` direction is controlled via the `direction` parameter.

---

## 10. Best Practices

### Algorithm Selection

| Need | Algorithm | Why |
|---|---|---|
| Find one shortest path | `shortestPath` | DirectTraversal, fastest |
| Weighted shortest path | `bellmanFord` or `bidirectionalDijkstra` | bellmanFord handles negative weights |
| Important nodes globally | `pageRank` | Iterative, captures transitive importance |
| Important nodes locally | `betweenness` | Identifies bottlenecks and bridges |
| Cluster membership | `wcc` | Fast, no parameters to tune |
| Community structure | `louvain` | Modularity-optimized, handles overlap |
| Fast community labels | `labelPropagation` | Linear time, good for large graphs |
| Network vulnerabilities | `bridges` + `articulationPoints` | Find single points of failure |
| Dense subgroups | `kCore` or `maximalCliques` | kCore for hierarchy, cliques for exact groups |
| Node-to-node similarity | `nodeSimilarity` | Jaccard/Cosine/Overlap metrics |

### Performance Tips

- **Use DirectTraversal for single-path queries.** shortestPath uses zero-copy BFS -- much faster than projection-based alternatives for point queries.
- **Project only needed labels/types.** Smaller projection = faster execution + less memory.
- **Set iteration limits.** Always set `maxIterations` and `tolerance` on convergence algorithms (pageRank, eigenvector, katz, louvain) to prevent runaway computation.
- **Use samplingSize for betweenness.** On large graphs, approximate betweenness with `samplingSize` to avoid O(V*E) cost.
- **Prefer maxFlow over fordFulkerson.** Dinic's algorithm (maxFlow) is more efficient for large flow networks.

### Anti-Patterns

| Anti-Pattern | Problem | Solution |
|---|---|---|
| Full projection for a single path | Wastes memory materializing entire graph | Use `shortestPath` (DirectTraversal) |
| No convergence params on iterative algo | May not converge, wasted compute | Set `maxIterations` and `tolerance` |
| Running on unprojected graph | Processes irrelevant vertices/edges | Always specify `nodeLabels` and `edgeTypes` |
| Using allPairsShortestPath on large graphs | O(V^3) memory and time | Use single-source algorithms instead |

---

## 11. Examples

### Fraud Network Analysis

Identify suspicious accounts by combining centrality and community detection.

```cypher
// Step 1: Find connected fraud clusters
CALL uni.algo.wcc(['Account'], ['TRANSFERS_TO'])
YIELD nodeId, componentId
WITH componentId, collect(nodeId) AS members, count(*) AS size
WHERE size >= 3
RETURN componentId, members, size
ORDER BY size DESC
```

```cypher
// Step 2: Find broker accounts bridging clusters
CALL uni.algo.betweenness(['Account'], ['TRANSFERS_TO'], true, 500)
YIELD nodeId, score
RETURN nodeId, score
ORDER BY score DESC
LIMIT 20
```

### Social Network Community Detection

```cypher
// Detect communities with Louvain
CALL uni.algo.louvain(['Person'], ['KNOWS'], 1.0, 20)
YIELD nodeId, communityId
RETURN communityId, count(*) AS size
ORDER BY size DESC
LIMIT 10
```

```cypher
// Find influential members per community
CALL uni.algo.pageRank(['Person'], ['KNOWS'])
YIELD nodeId, score
RETURN nodeId, score
ORDER BY score DESC
LIMIT 50
```

### Infrastructure Dependency Analysis

```cypher
// Find single points of failure
CALL uni.algo.articulationPoints(['Service'], ['DEPENDS_ON'])
YIELD node
MATCH (s:Service) WHERE id(s) = node
RETURN s.name AS criticalService
```

```cypher
// Find critical links whose removal disconnects the network
CALL uni.algo.bridges(['Service'], ['DEPENDS_ON'])
YIELD source, target
RETURN source, target AS criticalLink
```

```cypher
// Topological ordering for safe deployment
CALL uni.algo.topoSort(['Service'], ['DEPENDS_ON'])
YIELD nodeId, `order`
RETURN nodeId, `order`
ORDER BY `order` ASC
```

---

## 12. Guest-Authorable Graph Algorithms (GraphCompute)

Beyond the built-in `uni.algo.*` catalog, third-party algorithms can be authored in a guest language (Rhai, Python/PyO3, WASM, Extism) and run inside the engine as a *conductor* that issues coarse **native kernels** over opaque graph handles. The guest never touches raw storage; it orchestrates O(V+E) native primitives (projection, SpMV, expand, arena / tree-search ops) and reads back only small result tensors. First-party guest algorithms ship under `uni.algo.gc*` (e.g. `uni.algo.gcpagerank`).

### Kernel catalog highlights

Element-wise / map kernels make composite scoring formulas expressible without a bespoke native op — e.g. the UCT exploration term `c * sqrt(ln N / n)` is now composable:

| Kernel | Effect |
|---|---|
| `map_apply(m, "sqrt")` | Element-wise sqrt over a map handle |
| `map_apply(m, "exp")` | Element-wise exp |
| `ewise(a, b, "div")` | Element-wise `a / b`, with `x / 0 = 0` |
| `compare(a, b, "gt")` | Element-wise comparison (`gt`/`ge`/`lt`/`le`/`eq`/`ne`) to a 1.0/0.0 mask; shape-preserving, so an `[E]` comparison yields an `[E]` mask |
| `work_budget()` / `work_spent()` / `work_remaining()` | The native-work meter; reading costs nothing |

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

Ask for an op that is not in a vocabulary and the engine answers with the remedy:

```
bad ewise op `gt` — elementwise comparison is composable: compare(a, b, "gt");
valid ewise ops: add, mul, min, max, axpy, div, mod
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
| `normalize(m)` | `map_apply(m, "normalize_l1") or map_apply(m, "normalize_l2")` |
| an exact edge mask (edge-type / class selector) | `edge_mask_window(edge_property(g, prop), 0.5, 1.5)` — deterministic, no RNG. `sample_edges(sel, seed, iter)` yields the same mask because `prob = 0.0` never fires and `prob = 1.0` always does, **for every seed** — that endpoint behaviour is a guarantee, not an accident. Prefer the window: it says what it means. |

Two caveats worth knowing before you rely on them:

- **The `select` blend poisons on NaN.** `0.0 * NaN = NaN`, so a NaN in the *unselected* branch
  propagates. Where either branch may be NaN, `scatter` over the `VertexSet` instead of
  multiplying by its lowered mask.
- **A composed comparison charges the native-work meter three times** (`ewise` + `map_to_set` +
  `set_to_map`, each `|V|`), where a primitive comparison would charge once.

These identities are executed against an independent scalar oracle on every CI run
(`composition_recipes.rs`), so a recipe that stops being true breaks the build rather than
quietly misleading you.

Arena — the tree-search substrate, **12 kernels**:

| Kernel | Effect |
|---|---|
| `arena_backup(arena, value_col, leaves, deltas)` | Value backprop along each leaf's **full root path** (general-depth UCT, not just depth-1). |
| `arena_descend(...)` | Selection descent. The virtual-loss (`vloss`) term is a flat linear score offset applied in the descent loop — **not** a per-visit UCB recompute. Run at `vloss = 0` for parity with a Python-engine reference. |

Reachability and sampled expansion:

| Kernel | Effect |
|---|---|
| `reach_fixpoint(g, seeds, dir)` | BFS-to-fixpoint reachable set in one native O(V+E) call. |
| `expand_sampled(g, frontier, dir, exclude, prob, seed, iter)` | Fused frontier-scoped lazy sampled expansion — draws only the frontier's out-edges (out-only), applying the per-edge Bernoulli `prob` inline. |
| `sample_edges_undirected(g, prob, seed, iter)` | Both half-edges of an undirected pair share one Bernoulli draw (canonical `(min, max)` key). Simple undirected graphs only. |

Direction `"both"` — `expand`, `degrees`, and `spmv` accept `dir: "both"` (union of out + in edges). This **requires `includeReverse: true` in the projection config, or it errors** (fail-loud). `spmv` with `"both"` is unweighted.

### Argument typing

A guest algorithm's declared manifest `args` are now type- and arity-checked before the provider runs (previously the declaration was ignored). Tokens:

| Token | Accepts |
|---|---|
| `value` / `cypherValue` | A scalar **or** an array — e.g. a variable-length seed set |
| `list` / `array` | An array/list argument |
| `int`, `float`, `string`, `bool`, ... | The corresponding primitive |

A guest declaring `args: ["value"]` can be called with a Cypher list directly — no per-arity codegen:

```cypher
CALL p.spread([1, 2, 3], {nodeLabels: ['N'], edgeTypes: ['E']})
```

The trailing projection-config object (`{nodeLabels, edgeTypes, includeReverse, projectAll, ...}`) is an implicit optional last argument on every guest algorithm — it need not be declared in `args`.

### Projection scoping is fail-loud (breaking change)

An **unscoped** projection — no `nodeLabels` and no `edgeTypes` — now **errors** instead of silently projecting the whole graph. To project the entire graph, name the labels/types or set `projectAll: true` explicitly. First-party `uni.algo.gc*` procedures opt into whole-graph projection automatically.

Additional fail-loud guarantees:
- `emit` names the fault at the guest's call: a column from the wrong projection is rejected by identity, and a wrong-length column by length.
- A whole-graph projection errors on undeclared (schemaless) labels.
- Stored property values now read correctly from **unflushed in-memory L0** — previously a projection built before a flush saw `NaN` values / default edge weight `1.0`.

### Value identity: shape and index space

Every value carries the **shape** it was built with (`[V]` or `[E]`) and the **index space** its
slots belong to — the projection or arena it came from. Both are enforced, so a `[V]` map cannot
combine with an `[E]` tensor of equal length, and values from two projections cannot combine at
all (slot `i` names a different vertex in each). `ewise`, `map_apply`, `compare` and
`segmented_reduce` preserve both, so an `[E]` pipeline stays `[E]` through to `sample_edges`.

Sets, walk matrices and pair lists are tagged too, so a foreign operand is rejected wherever one
meets a graph or another set: the set algebra, `expand` / `expand_masked` / `expand_sampled`
(frontier, `exclude` **and** edge mask), `spmv` / `spmv_masked`, `scatter`, a masked `reduce`,
`walk_visit_counts`, and `sample_edges_undirected`. The `exclude` case matters most:
`reach_fixpoint` passes the visited-set as `exclude` every iteration, so a foreign one silently
under-expands the traversal instead of failing.

`set_to_map` therefore **inherits** its set's space. *Unknown* survives only as the permissive
element of unification — nothing a guest can build lands there. `map_to_set` is shape-polymorphic:
an `[E]` tensor lowers to an `EdgeSet` that `spmv_masked` accepts.

A guest can pre-declare extra projections at the CALL site with a `scopes` map
(`{nodeLabels: [...], scopes: {agg: {nodeLabels: [...], edgeTypes: [...]}}}`) and reach them with
`gc.graph_named("agg")`. Scopes are built by the host before the guest runs — `graph_named` is a
lookup, so a guest cannot project in a loop and escape the work meter — and the size-derived work
budget is summed across every bound projection. Each scope takes the full config vocabulary
independently, Native or Cypher/Named. Slot correspondence between projections is a **Native-only**
guarantee: `ProjectionBuilder` sorts and dedups vids, `GraphProjection::from_rows` does not.

To combine values across scopes, use `rekey(value, g)`. It is a *verified* move, not a cast: it
walks both slot→Vid maps and fails naming the first divergent slot unless the projections describe
the same vertices. `[V]` tensors and vertex sets only — `[E]` values have no cross-projection
meaning.

Egress is keyed to the value: `topk`, `arg_extreme`, `emit_walks` and `emit_pairs` return the vids
of *their own* value's projection, not the first graph bound; an arena-keyed value is rejected.
`emit` checks identity before length, so a wrong-projection column is named as such.

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
