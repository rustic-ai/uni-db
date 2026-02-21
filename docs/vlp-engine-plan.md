# VLP/RPQ Engine: Architecture Plan & Codebase Touch Points

## Architecture Validation Against Codebase

### What Exists Today

The current VLP engine is a straightforward BFS in `GraphVariableLengthTraverseExec`
(`traverse.rs:2362-2413`). It takes a source VID, edge type IDs, direction, and hop
range, then expands all paths using a `VecDeque`-based BFS. The five TCK failures
trace to three missing capabilities:

1. **Edge property predicates are post-filters, not inline** — The planner wraps
   VLP with `LogicalPlan::Filter` at `planner.rs:4497-4508`. The BFS never sees
   edge predicates. This causes Match4[5] (3 rows instead of 1).

2. **No cross-pattern relationship uniqueness** — `used_edge_columns` exists on
   `GraphTraverseExec` (line 239) but NOT on `GraphVariableLengthTraverseExec`.
   This causes Match4[7] (count=84 instead of 32) and Match5[27] (20 instead of 16).

3. **Long-chain endpoint filtering** — Match4[4] returns 17 rows instead of 1 on
   a 20-hop chain. The BFS emits at every depth in `[min, max]` but the target
   property filter `{var: 'end'}` is a post-filter, not part of BFS emit logic.

### What the NFA Architecture Provides

The NFA replaces the `(min_hops, max_hops, edge_type_ids)` triple with a compiled
automaton. For current OpenCypher VLP, every NFA is a simple linear chain. But the
same traversal kernel handles GQL QPP (complex sub-patterns with quantifiers) and
full RPQ (alternation, concatenation, Kleene star) without code changes — just
different NFAs.

The bitmap preselection replaces post-filter predicates with precomputed
`BitVec<u64>` / `HashSet<Eid>` sets built via Lance pushdown during the warming
phase. The BFS inner loop becomes: CSR scan → bitmap membership check → frontier
update. No I/O during traversal.

---

## Trail Semantics — Correctness Analysis

### The Fundamental Problem

OpenCypher requires **Trail semantics** (no repeated edges within a single MATCH
pattern) by default. The predecessor DAG approach separates concerns: Walk BFS for
structure, Trail enforcement during enumeration. But the interaction between these
two phases is subtle and has correctness traps.

### Neither Global Visited Nor Per-Depth Frontier Is Sufficient Alone

**Global visited gives false negatives** — Example: A -[e1]→ B -[e2]→ C -[e3]→ A,
VLP `-[*3]→`:
- d=0: {A}, d=1: {B via e1}, d=2: {C via e2 from B}
- d=3: C→A via e3, but A already visited → skip
- Result: **misses A@3**, even though A→e1→B→e2→C→e3→A uses distinct edges (Trail-valid)

**Per-depth frontier gives false positives** — Example: A -[e1]→ B -[e2]→ A
(directed 2-cycle), VLP `-[*3]→`:
- d=0: {A}, d=1: {B via e1}, d=2: {A via e2}, d=3: {B via e1 again}
- B@3: path A→e1→B→e2→A→e1→B uses e1 twice → **NOT Trail-valid**
- But Walk BFS reports B as reachable at depth 3

**Conclusion**: Walk-only BFS (regardless of visited strategy) cannot determine
Trail-valid reachability. The only correct approach is Walk BFS to build a
candidate DAG, then Trail enumeration to verify each candidate.

### The Shortest-Path Theorem

**Theorem**: The shortest Walk path to any (vid, state) at its minimum BFS depth
is ALWAYS Trail-valid (and even Simple-valid).

**Proof sketch**: In a BFS tree, the shortest path to a vertex at depth d uses
exactly d edges and visits d+1 distinct vertices. If any vertex were repeated,
there would be a shorter path (skip the cycle). No repeated vertices → no repeated
edges.

**Consequence**: `shortestPath()`, `allShortestPaths()`, and `min(length(p))` can
use global visited BFS safely. The shortest Walk depth IS the shortest Trail depth.

### The Correct Architecture

1. **Walk BFS with per-depth frontier** builds the candidate DAG structure.
   Per-depth dedup (not global visited) ensures we don't miss valid Trail paths
   at later depths. This gives false positives (Walk-reachable but not Trail-valid)
   but never false negatives.

2. **Trail enumeration** over the DAG filters out invalid paths. For each candidate
   endpoint, at least one Trail-valid path must exist for the endpoint to appear
   in results. The DFS applies `edge_set.contains(eid)` checks per enumerated path.

3. **Endpoints-only optimization**: Even when no path variable is bound, we still
   need Trail verification per candidate. This means attempting one Trail-valid
   path enumeration per candidate endpoint from the DAG. Early termination on
   first valid path keeps this cheap.

4. **Shortest-path fast-path**: For `shortestPath()`, `allShortestPaths()`, and
   queries using only `min(length(p))`, global visited BFS is correct and more
   efficient.

### Walk vs Trail: Impact on Each Output Mode

| Output Mode | BFS Strategy | Trail Enforcement | Notes |
|---|---|---|---|
| Endpoints-only | Per-depth frontier | Existence check per candidate | Need DAG even without path output |
| Length-only (`min(length(p))`) | Global visited | Not needed | Shortest-path theorem |
| Length-only (`max(length(p))`) | Per-depth frontier | Full enumeration to find max valid depth | |
| Count (`count(p)`) | Per-depth frontier | Count during Trail enumeration | |
| Full path (`RETURN p`) | Per-depth frontier | Full Trail enumeration | |
| Step variable (`RETURN r`) | Per-depth frontier | Full Trail enumeration | |
| Shortest path | Global visited | Not needed | Shortest-path theorem |
| Existential (`EXISTS`) | Per-depth frontier | Early-stop on first valid | |

### Cross-Pattern Uniqueness vs Trail

**Important distinction**: `used_edge_columns` (cross-pattern relationship uniqueness)
filtering happens DURING expansion, not during enumeration. Cross-pattern edges come
from the input batch (previous MATCH clauses) and are fixed per source row — they're
not per-path, they're per-binding-row. So excluding them during expansion is correct.

Trail enforcement is per-PATH (each enumerated path must not reuse edges) and happens
during enumeration. These are separate mechanisms that compose correctly.

---

## Seven Output Modes

The VLP engine must handle fundamentally different output requirements depending on
what the query asks for. Each mode has different optimization opportunities.

### Mode 1: Endpoints-Only
**Trigger**: No `path_variable`, no `step_variable`
```cypher
MATCH (a)-[:T*1..4]->(b) RETURN b.name
```
**Output**: One row per reachable (target, depth) where Trail-valid path exists.
**VLP output columns**: `target._vid`, `_hop_count`, hydrated target properties.
**Approach**: Walk DAG → Trail existence check per candidate (early-stop).
**Failing TCK**: Match4[4], Match4[5], Match5[27].

### Mode 2: Length-Only
**Trigger**: `path_variable` bound, only `length(p)` or `min/max(length(p))` used.
```cypher
MATCH p = (a)-[:R*]->(b) WITH a, b, min(length(p)) AS len RETURN len
```
**Output**: Depends on aggregate:
- `min(length(p))` → minimum BFS depth (always Trail-valid per shortest-path theorem)
- `max(length(p))` or bare `length(p)` → needs Trail enumeration per depth
**Optimization**: For `min(length(p))`, use global visited BFS, skip DAG entirely.
**Failing TCK**: Return6[13] (uses `min(length(p))`).

### Mode 3: Count-Only
**Trigger**: `path_variable` bound, only `count(p)` used (no path materialization).
```cypher
MATCH p = (n)-[*0..1]-()-[r]-()-[*0..1]-(m) RETURN count(p)
```
**Output**: Exact count of Trail-valid paths. DataFusion's COUNT counts non-null
rows (`df_expr.rs:1418`), so we emit one row per valid path but skip struct
materialization.
**Approach**: Walk DAG → Trail enumeration, increment counter, no path struct.
**Failing TCK**: Match4[7].

### Mode 4: Full Path
**Trigger**: `path_variable` used in RETURN, nodes(p), relationships(p), or passed
through WITH.
```cypher
MATCH p = (a)-[:T*]->(b) RETURN p
```
**Output**: One row per Trail-valid path with materialized
`Struct{nodes: List<Node>, relationships: List<Edge>}` (`common.rs:261-277`).
**Approach**: Walk DAG → full Trail enumeration → streaming path materialization.
**Downstream consumers**: `length(p)` via UDF (`df_udfs.rs:790`), `nodes(p)` /
`relationships(p)` via UDF (`df_udfs.rs:831`), WITH pass-through preserving
`VariableType::Path` (`planner.rs:5417`), UNWIND (`unwind.rs:371`), DISTINCT via
DataFusion struct comparison.

### Mode 5: Step Variable
**Trigger**: `step_variable` bound (e.g., `[r*1..3]`), no `path_variable`.
```cypher
MATCH (a)-[r*1..3]->(b) RETURN r
```
**Output**: One row per Trail-valid path with edge list as `List<Struct<Edge>>`
(`traverse.rs:3033-3063`).
**Approach**: Walk DAG → Trail enumeration → edge-only materialization.
**Downstream consumers**: `UNWIND r AS rel`, `ALL(x IN r WHERE ...)`,
`size(r)`, `r[0].prop`.

### Mode 6: Shortest Path
**Trigger**: `shortestPath()` or `allShortestPaths()` wrapper.
```cypher
MATCH p = shortestPath((a)-[*]->(b)) RETURN p
```
**Current**: Separate `GraphShortestPathExec` (`shortest_path.rs:40-91`).
`allShortestPaths` NOT implemented (`df_planner.rs:954-956`).
**Approach**: Unify with VLP engine using PathSelector:
- `shortestPath` → AnyShortest selector, global visited BFS
- `allShortestPaths` → AllShortest selector, global visited BFS
- Both correct via shortest-path theorem.

### Mode 7: Existential Pattern
**Trigger**: EXISTS with VLP, or pattern predicate in WHERE.
```cypher
WHERE EXISTS { (a)-[*1..3]->(b) }
```
**Current**: NOT implemented (`df_expr.rs:448-450`).
**Output**: Boolean per source row.
**Approach**: Endpoints-only + early termination on first Trail-valid path found.

### Mode Detection in Planner

The planner must analyze what functions/clauses consume the path variable to
determine the output mode. This analysis happens in `df_planner.rs` when
constructing the physical VLP operator:

```rust
enum VlpOutputMode {
    EndpointsOnly,
    LengthOnly { needs_max: bool },
    CountOnly,
    FullPath,
    StepVariable,
    ShortestPath { selector: PathSelector },
    Existential,
}
```

The mode drives:
- Whether to build PredecessorDag
- Whether to use global visited or per-depth frontier
- Whether to do full Trail enumeration or existence-only checks
- What columns to include in output RecordBatch

---

## Complete Feature Impact Analysis

### Directly Changed by VLP Redesign

| Feature | Current State | Impact of Redesign |
|---|---|---|
| VLP MATCH execution | BFS cloning Vec per path (`traverse.rs:2671`) | Replaced with NFA frontier + DAG |
| Edge property predicates | Post-filter wrapping (`planner.rs:4497`) | Inline via EidFilter bitmap |
| Target vertex predicates | Post-filter label-only check (`traverse.rs:2684`) | Inline via VidFilter bitmap |
| Cross-pattern uniqueness | Missing for VLP (present for fixed at `df_planner.rs:1842`) | Add `used_edge_columns` |
| shortestPath | Separate operator (`shortest_path.rs:40-91`) | Can unify via AnyShortest selector |
| allShortestPaths | NOT implemented (`df_planner.rs:954`) | Can now implement via AllShortest |
| EXISTS with VLP | NOT implemented (`df_expr.rs:448`) | Can now implement via Mode 7 |
| QPP execution | AST exists, NOT executed (`df_planner.rs:958`) | NFA naturally handles |

### Output Format Compatibility (must not change)

| Output Element | Location | Constraint |
|---|---|---|
| Path struct schema | `common.rs:261-277` | `{nodes: List<Node>, relationships: List<Edge>}` — consumed by UDFs |
| Step variable format | `traverse.rs:3033-3063` | `List<Struct<Edge>>` — consumed by UNWIND, ALL() |
| `_hop_count` column | VLP output schema | UInt64 — used by downstream filters/sorts |
| Target property hydration | `traverse.rs:3179-3330` | Async Lance load — timing must not change |

### Downstream Consumers (must preserve behavior)

| Consumer | Mechanism | Risk Assessment |
|---|---|---|
| `length(p)` | UDF extracts relationships list length (`df_udfs.rs:790-885`) | Path struct format must match |
| `nodes(p)` / `relationships(p)` | UDF extracts struct fields (`df_udfs.rs:831`) | Same |
| WITH path pass-through | Preserves `VariableType::Path` (`planner.rs:5417-5430`) | Variable type tracking unchanged |
| UNWIND nodes(p) / relationships(p) | Expands list from path UDF (`unwind.rs:371-454`) | List format must match |
| DISTINCT / GROUP BY paths | DataFusion Arrow struct comparison | Arrow struct equality unchanged |
| COUNT(p) | Counts non-null rows (`df_expr.rs:1418`) | Path column non-null for valid paths |
| OPTIONAL MATCH | `is_optional` flag (`traverse.rs:2397`) | Null-row semantics preserved |
| `collect(p)` | Standard list aggregation | Path struct is the element type |

### Features NOT Affected (but verified)

| Feature | Why Unaffected |
|---|---|
| Fixed-length traverse | Separate code path (`GraphTraverseExec`), not modified |
| CREATE/MERGE | Write operations, independent of VLP read path |
| ORDER BY / LIMIT / SKIP | Standard DataFusion operators downstream of VLP output |
| CALL procedures | Independent code path |
| Vector search | Independent code path |

---

## Phase 1: New Types & Data Structures

### 1.1 NFA Types

**New file: `crates/uni-query/src/query/df_graph/nfa.rs`**

```rust
/// NFA state identifier
pub type NfaStateId = u16;

/// A compiled NFA for path pattern evaluation
pub struct PathNfa {
    pub transitions: Vec<NfaTransition>,
    pub accepting_states: HashSet<NfaStateId>,
    pub start_state: NfaStateId,
    pub num_states: u16,
    /// Per-transition edge predicates (index = transition index)
    pub edge_predicates: Vec<Option<EdgePredicate>>,
    /// Per-state vertex predicates (index = state id)
    pub vertex_predicates: Vec<Option<VertexPredicate>>,
}

pub struct NfaTransition {
    pub from: NfaStateId,
    pub to: NfaStateId,
    pub edge_type_ids: Vec<u32>,       // edge types for this transition
    pub direction: Direction,           // direction for this transition
}

/// Edge predicate extracted from VLP property map or inline WHERE
pub struct EdgePredicate {
    pub properties: HashMap<String, CypherValue>,  // from {year: 1988}
    pub filter_expr: Option<Expr>,                 // from inline WHERE
    pub lance_filter: Option<String>,              // compiled Lance SQL
}

/// Vertex predicate for NFA state (checked on accepting states)
pub struct VertexPredicate {
    pub label_name: Option<String>,
    pub properties: HashMap<String, CypherValue>,
    pub filter_expr: Option<Expr>,
}

/// Path semantics
pub enum PathMode {
    Walk,     // no restrictions on repeated edges/nodes
    Trail,    // no repeated edges (OpenCypher default)
    Acyclic,  // no repeated nodes
    Simple,   // no repeated nodes except start=end
}

/// Path selector (for future GQL support)
pub enum PathSelector {
    All,                 // all matching paths (default)
    Any,                 // one arbitrary path per endpoint pair
    AnyShortest,         // one shortest path per endpoint pair
    AllShortest,         // all shortest paths per endpoint pair
    ShortestK(usize),    // k shortest paths per endpoint pair
}
```

**VLP NFA compilation** (also in nfa.rs):

```rust
impl PathNfa {
    /// Compile a simple VLP pattern into a linear-chain NFA.
    /// [:TYPE*min..max {props}] becomes states q0..q(max) with
    /// transitions on TYPE, accepting states q(min)..q(max).
    pub fn from_vlp(
        edge_type_ids: Vec<u32>,
        direction: Direction,
        min_hops: usize,
        max_hops: usize,
        edge_predicate: Option<EdgePredicate>,
        target_vertex_predicate: Option<VertexPredicate>,
    ) -> Self { ... }

    /// Compile a QPP sub-pattern into an NFA.
    /// ((a)-[:T1]->(b:L)-[:T2]->(c)){min,max} becomes an NFA
    /// with states per sub-pattern element, repeated min..max times.
    pub fn from_qpp(
        sub_pattern: &[PatternElement],
        min_iterations: usize,
        max_iterations: usize,
    ) -> Self { ... }

    /// Get all transitions from a given state
    pub fn transitions_from(&self, state: NfaStateId) -> &[NfaTransition] { ... }

    /// Check if a state is accepting
    pub fn is_accepting(&self, state: NfaStateId) -> bool { ... }
}
```

### 1.2 Bitmap Preselection Types

**New file: `crates/uni-query/src/query/df_graph/bitmap.rs`**

```rust
use bitvec::prelude::*;

/// Precomputed set of allowed edge IDs for a specific predicate.
/// EIDs are dense sequential u64, so a dense bitvec is optimal.
pub enum EidFilter {
    /// All edges allowed (no predicate)
    AllAllowed,
    /// Dense bitvec indexed by eid.raw()
    DenseBitVec(BitVec),
    /// HashSet for small result sets
    HashSet(HashSet<u64>),
}

impl EidFilter {
    pub fn contains(&self, eid: Eid) -> bool {
        match self {
            Self::AllAllowed => true,
            Self::DenseBitVec(bv) => {
                let idx = eid.as_u64() as usize;
                idx < bv.len() && bv[idx]
            }
            Self::HashSet(set) => set.contains(&eid.as_u64()),
        }
    }

    /// Build from a Lance query result stream
    pub async fn from_lance_scan(
        stream: SendableRecordBatchStream,
        total_eids: usize,
    ) -> Result<Self> { ... }
}

/// Precomputed set of allowed vertex IDs
pub enum VidFilter {
    AllAllowed,
    DenseBitVec(BitVec),
    HashSet(HashSet<u64>),
}
```

### 1.3 Predecessor DAG for Path Output

**New file: `crates/uni-query/src/query/df_graph/pred_dag.rs`**

#### Design Rationale

The BFS currently clones `Vec<Vid>` and `Vec<Eid>` per path in the queue — O(#paths)
memory. The predecessor DAG stores **one record per discovered predecessor edge** —
O(#discovered edges), which is much smaller.

**Key design decisions based on codebase analysis:**

1. **Layered DAG required (not shortest-DAG):** OpenCypher VLP returns one row per
   distinct path, not per distinct endpoint. When `step_variable` or `path_variable`
   is set, different edge sequences to the same target produce different output rows.
   We must store predecessors at ALL depths, not just shortest depth.

2. **Two operating modes (both build DAG, but differ in enumeration):**
   - **Endpoints-only mode** (no step_variable, no path_variable): Build a DAG
     under Walk semantics, then verify Trail-valid reachability per candidate via
     `has_trail_valid_path()` (early-stop on first valid). Output: one row per
     Trail-valid (target, depth). No path materialization.
   - **All-paths mode** (step_variable or path_variable set): Build the layered
     predecessor DAG and enumerate ALL Trail-valid paths lazily during output
     construction. Full path/edge materialization.

3. **Pool-based storage, not HashMap of Vecs:** An append-only `pred_pool: Vec<PredRec>`
   with linked-list threading via `next: i32` eliminates per-predecessor allocation
   and gives excellent cache locality during enumeration DFS.

4. **Walk during expansion, Trail during enumeration:** The BFS frontier uses Walk
   semantics (no per-path edge tracking — fastest). Trail/Acyclic constraints are
   applied during the enumeration DFS over the predecessor DAG. This is the Kuzu
   approach (confirmed in their VLDB 2025 paper).

5. **Streaming enumeration:** Don't materialize all paths into `Vec<(Vec<Vid>, Vec<Eid>)>`.
   Use a callback/iterator that yields one path at a time. This supports LIMIT,
   early termination, and caps on total emitted paths per query.

6. **HashMap for dist/pred_head (sparse-first):** VLP with bitmap preselection
   typically explores a sparse subgraph (e.g., 10K of 1M vertices reachable).
   Array-indexed storage (`state * num_vertices + vid`) wastes memory for sparse
   exploration. Start with HashMap; add density-based auto-switch if profiling shows
   it's a bottleneck. Note: `csr.num_vertices()` (csr.rs:378) provides max VID at
   runtime for the array-indexed path if needed.

```rust
use rustc_hash::{FxHashMap, FxHashSet};

/// A single predecessor record in the pool.
/// Linked into per-(vid, state, depth) chains via `next`.
pub struct PredRec {
    pub src_vid: Vid,
    pub src_state: NfaStateId,
    pub eid: Eid,
    pub next: i32,            // -1 = end of chain, else index into pred_pool
}

/// Pool-based predecessor DAG for compact path representation.
///
/// During BFS expansion (Walk semantics), call `add_predecessor()` for each
/// discovered edge. During output, call `enumerate_paths()` with a callback
/// that receives one path at a time with Trail/Acyclic filtering applied.
///
/// Uses FxHashMap (rustc-hash) throughout for the internal maps — Fx hashing
/// is significantly faster than SipHash for small integer keys.
pub struct PredecessorDag {
    /// Append-only pool of predecessor records.
    /// Insertions are O(1) amortized (Vec push + HashMap update).
    pred_pool: Vec<PredRec>,

    /// Head of predecessor chain for each (dst_vid, dst_state, depth).
    /// Value is index into pred_pool, or -1 if no predecessors.
    /// Using FxHashMap because VLP typically explores sparse subgraphs,
    /// and Fx hashing is faster than SipHash for integer-keyed maps.
    pred_head: FxHashMap<(Vid, NfaStateId, u32), i32>,

    /// First-visit depth for each (vid, state). Used for:
    /// - Shortest-path selectors (only store preds at min depth)
    /// - Detecting already-visited (vid, state) pairs in Walk mode
    first_depth: FxHashMap<(Vid, NfaStateId), u32>,

    /// Path selector determines DAG construction mode:
    /// - All/Any → layered DAG (store preds at all depths)
    /// - AnyShortest/AllShortest/ShortestK → shortest-only DAG (preds at min depth)
    selector: PathSelector,
}

impl PredecessorDag {
    pub fn new(selector: PathSelector) -> Self {
        Self {
            pred_pool: Vec::new(),
            pred_head: FxHashMap::default(),
            first_depth: FxHashMap::default(),
            selector,
        }
    }

    /// Returns true if this DAG uses layered mode (stores preds at all depths).
    /// False for shortest-only mode (stores preds only at first-visit depth).
    fn is_layered(&self) -> bool {
        matches!(self.selector, PathSelector::All | PathSelector::Any)
    }

    /// Record a predecessor edge discovered during BFS.
    /// Called once per discovered edge during Walk-mode expansion.
    ///
    /// For shortest-only selectors (AnyShortest, AllShortest, ShortestK),
    /// only records predecessors at the first-visit depth. For layered
    /// selectors (All, Any), records at all depths.
    pub fn add_predecessor(
        &mut self,
        dst: Vid,
        dst_state: NfaStateId,
        src: Vid,
        src_state: NfaStateId,
        eid: Eid,
        depth: u32,
    ) {
        // Update first-visit depth
        let first = *self.first_depth
            .entry((dst, dst_state))
            .or_insert(depth);

        // For shortest-only mode, skip predecessors at depths > first visit
        if !self.is_layered() && depth > first {
            return;
        }

        // Get current head for this (dst, state, depth) chain
        let key = (dst, dst_state, depth);
        let old_head = self.pred_head.get(&key).copied().unwrap_or(-1);

        // Append new record to pool
        let idx = self.pred_pool.len() as i32;
        self.pred_pool.push(PredRec {
            src_vid: src,
            src_state,
            eid,
            next: old_head,
        });

        // Update head pointer
        self.pred_head.insert(key, idx);
    }

    /// Enumerate paths from source to (target, accepting_state) across
    /// all depths in [min_depth, max_depth].
    ///
    /// Applies path mode constraints during DFS enumeration:
    /// - Trail: skip paths with repeated edges
    /// - Acyclic: skip paths with repeated vertices
    /// - Walk: no filtering (all paths valid)
    ///
    /// Calls `yield_path` for each valid path. Return `ControlFlow::Break`
    /// from the callback to stop early (e.g., for LIMIT or ANY selector).
    pub fn enumerate_paths<F>(
        &self,
        source: Vid,
        target: Vid,
        accepting_state: NfaStateId,
        min_depth: u32,
        max_depth: u32,
        mode: &PathMode,
        yield_path: &mut F,
    ) where
        F: FnMut(&[Vid], &[Eid]) -> ControlFlow<()>,
    {
        // For each depth in [min, max] that has predecessors at (target, state):
        for depth in min_depth..=max_depth {
            let key = (target, accepting_state, depth);
            if let Some(&head) = self.pred_head.get(&key) {
                // DFS back from target to source following predecessor chains.
                // Parallel FxHashSets provide O(1) membership checks for
                // Trail (edge_set) and Acyclic (node_set) enforcement.
                let mut node_stack: Vec<Vid> = vec![target];
                let mut edge_stack: Vec<Eid> = Vec::new();
                let mut node_set: FxHashSet<Vid> = FxHashSet::default();
                let mut edge_set: FxHashSet<Eid> = FxHashSet::default();
                node_set.insert(target);
                self.dfs_enumerate(
                    source, head, depth,
                    &mut node_stack, &mut edge_stack,
                    &mut node_set, &mut edge_set,
                    mode, yield_path,
                );
            }
        }
    }

    /// Check if at least one Trail-valid path exists from source to target.
    /// Used for endpoints-only mode where we don't need to materialize paths
    /// but still need Trail correctness. Returns true on first valid path.
    pub fn has_trail_valid_path(
        &self,
        source: Vid,
        target: Vid,
        accepting_state: NfaStateId,
        min_depth: u32,
        max_depth: u32,
    ) -> bool {
        let mut found = false;
        self.enumerate_paths(
            source, target, accepting_state,
            min_depth, max_depth,
            &PathMode::Trail,
            &mut |_, _| {
                found = true;
                ControlFlow::Break(()) // stop after first valid path
            },
        );
        found
    }

    /// Internal DFS for path enumeration with constraint checking.
    ///
    /// Uses parallel FxHashSets alongside the Vec stacks for O(1) membership
    /// checks during Trail/Acyclic enforcement. Without these, the contains()
    /// check on a Vec is O(path_len) per step, making total enumeration
    /// O(path_len^2) per path — a quadratic penalty that matters for long paths.
    fn dfs_enumerate<F>(
        &self,
        source: Vid,
        pred_head: i32,
        remaining_depth: u32,
        node_stack: &mut Vec<Vid>,
        edge_stack: &mut Vec<Eid>,
        node_set: &mut FxHashSet<Vid>,   // parallel to node_stack, O(1) lookup
        edge_set: &mut FxHashSet<Eid>,   // parallel to edge_stack, O(1) lookup
        mode: &PathMode,
        yield_path: &mut F,
    ) where
        F: FnMut(&[Vid], &[Eid]) -> ControlFlow<()>,
    {
        let mut idx = pred_head;
        while idx >= 0 {
            let rec = &self.pred_pool[idx as usize];

            // Trail check: skip if edge already in path — O(1) via FxHashSet
            if matches!(mode, PathMode::Trail) && edge_set.contains(&rec.eid) {
                idx = rec.next;
                continue;
            }

            // Acyclic/Simple check: skip if vertex already in path — O(1) via FxHashSet
            if matches!(mode, PathMode::Acyclic | PathMode::Simple)
                && node_set.contains(&rec.src_vid)
            {
                idx = rec.next;
                continue;
            }

            // Push onto stacks + sets
            node_stack.push(rec.src_vid);
            edge_stack.push(rec.eid);
            node_set.insert(rec.src_vid);
            edge_set.insert(rec.eid);

            if rec.src_vid == source && remaining_depth == 1 {
                // Reached source — emit path (reversed, since we walked backwards)
                let nodes: Vec<Vid> = node_stack.iter().rev().copied().collect();
                let edges: Vec<Eid> = edge_stack.iter().rev().copied().collect();
                if yield_path(&nodes, &edges).is_break() {
                    node_stack.pop();
                    edge_stack.pop();
                    node_set.remove(&rec.src_vid);
                    edge_set.remove(&rec.eid);
                    return;
                }
            } else if remaining_depth > 1 {
                // Continue DFS from predecessor
                let prev_key = (rec.src_vid, rec.src_state, remaining_depth - 1);
                if let Some(&prev_head) = self.pred_head.get(&prev_key) {
                    self.dfs_enumerate(
                        source, prev_head, remaining_depth - 1,
                        node_stack, edge_stack, node_set, edge_set,
                        mode, yield_path,
                    );
                }
            }

            // Pop stacks + sets (backtrack)
            node_stack.pop();
            edge_stack.pop();
            node_set.remove(&rec.src_vid);
            edge_set.remove(&rec.eid);

            idx = rec.next;
        }
    }
}
```

#### When to Use Each Mode

**Endpoints-only mode** — When the query references neither step_variable nor
path_variable (e.g., `MATCH (a)-[*]->(b) RETURN b`):
- Builds a lightweight PredecessorDag for Trail verification
- Walk BFS with per-depth frontier discovers candidates
- `has_trail_valid_path()` per candidate (early-stop on first valid path found)
- Output: one row per Trail-valid (target_vid, depth)

**All-paths mode** — When step_variable or path_variable is set
(e.g., `MATCH p = (a)-[r*1..3]->(b) RETURN p, r`):
- Build layered PredecessorDag during Walk BFS with per-depth frontier
- Enumerate paths lazily during `build_output_batch`
- Apply Trail/Acyclic during enumeration DFS via FxHashSet membership checks

**Shortest-path mode** — For `shortestPath()` / `allShortestPaths()`:
- Build shortest-only PredecessorDag with global visited BFS
- Trail verification not needed (shortest-path theorem)
- Enumerate from first-visit depth layer only

**Length-only mode** — When only `min(length(p))` is used:
- Global visited BFS (no DAG needed)
- Emit shortest BFS depth per target (always Trail-valid)
- Skip enumeration entirely

#### Future: Array-Indexed Storage Upgrade

If profiling shows HashMap overhead is significant for dense exploration, swap to:
```rust
struct ArrayIndexedDag {
    // idx = state * num_vertices + vid.as_u64()
    dist: Vec<u32>,          // first-visit depth, u32::MAX = unvisited
    pred_head: Vec<i32>,     // per-(vid, state): head into pred_pool
    pred_pool: Vec<PredRec>, // same append-only pool
    num_vertices: usize,     // from csr.num_vertices() at query time
    num_states: usize,       // from nfa.num_states
}
```
The max VID is queryable via `csr.num_vertices()` (csr.rs:378) or
`allocator.current_vid()` (id_allocator.rs:155). Auto-switch when
`frontier_size > num_vertices / 4`.

---

## Phase 2: Planner Changes

### 2.1 Pass edge predicates INTO VLP instead of wrapping as Filter

**File: `crates/uni-query/src/query/planner.rs`**

**Current (lines 4497-4508):**
```rust
// Apply relationship property predicates (e.g. [r {k: v}]).
if let Some(edge_var_name) = effective_step_var.as_ref()
    && let Some(edge_prop_filter) =
        self.properties_to_expr(edge_var_name, &params.rel.properties)
{
    plan = LogicalPlan::Filter {
        input: Box::new(plan),
        predicate: edge_prop_filter,
        optional_variables: filter_optional_vars.clone(),
    };
}
```

**Change:** When `is_variable_length`, do NOT wrap with Filter. Instead, extract the
raw properties map and pass it into the LogicalPlan::Traverse node.

**Touch points:**

1. **`LogicalPlan::Traverse` enum variant** (line 1728):
   - Add field: `edge_filter_properties: Option<Expr>` — the raw edge property map
     converted to expression (e.g., `r.year = 1988`)
   - Add field: `edge_filter_where: Option<Expr>` — inline WHERE clause from
     relationship pattern
   - Add field: `path_mode: PathMode` — default `Trail` for OpenCypher

2. **`LogicalPlan::TraverseMainByType` enum variant** (line 1758):
   - Same new fields as above

3. **`plan_traverse_with_source` function** (~line 4145):
   - At line 4497: conditionally skip the `Filter` wrapping when `is_variable_length`
   - Instead, store `params.rel.properties` (the raw map) in the Traverse node
   - Store `params.rel.where_clause` in the Traverse node

4. **Schemaless path** (~line 4274): Same changes for `TraverseMainByType`

### 2.2 Pass used_edge_columns for VLP

**File: `crates/uni-query/src/query/df_planner.rs`**

**Touch points:**

1. **`plan_traverse` function** (line 1634):
   - In the VLP branch (lines 1866-1940):
   - Call `Self::collect_used_edge_columns(...)` (same as fixed-length at line 1842)
   - Pass result to `GraphVariableLengthTraverseExec::new()`

2. **`plan_traverse_main_by_type_vlp` function** (line 2130):
   - Same: call `collect_used_edge_columns` and pass to constructor

### 2.3 Compile edge predicates to Lance filter string

**File: `crates/uni-query/src/query/df_planner.rs`**

**Touch points:**

1. In `plan_traverse` VLP branch (lines 1866-1940):
   - Extract `edge_filter_properties` from the logical plan
   - Use `LanceFilterGenerator::generate()` (from `pushdown.rs:543`) to compile
     Cypher predicate → Lance SQL string
   - Pass the compiled filter string to `GraphVariableLengthTraverseExec::new()`

2. In `plan_traverse_main_by_type_vlp` (lines 2130-2191):
   - Same compilation, but edge type names are strings not IDs

---

## Phase 3: Execution Layer Changes

### 3.1 Modify GraphVariableLengthTraverseExec

**File: `crates/uni-query/src/query/df_graph/traverse.rs`**

**Struct changes** (lines 2362-2413) — add fields:

```rust
pub struct GraphVariableLengthTraverseExec {
    // ... existing fields ...

    // NEW: NFA for pattern matching (replaces raw min/max/edge_type_ids for traversal)
    nfa: Arc<PathNfa>,

    // NEW: Lance filter string for edge predicate pushdown
    edge_filter: Option<String>,

    // NEW: Cross-pattern relationship uniqueness
    used_edge_columns: Vec<String>,

    // NEW: Path semantics (default Trail for OpenCypher)
    path_mode: PathMode,

    // NEW: Output mode determines BFS strategy and Trail enforcement
    output_mode: VlpOutputMode,

    // NEW: Path selector (All for regular VLP, AnyShortest/AllShortest for shortestPath)
    path_selector: PathSelector,
}
```

Note: Keep `min_hops`, `max_hops`, `edge_type_ids` for backward compatibility
during transition. The NFA is compiled FROM these in `new()`. Eventually these
raw fields can be removed.

**`GraphVariableLengthTraverseExecData` changes** (lines 2653-2667):

Same new fields mirrored here (this is the lightweight clone used in the stream).

### 3.2 New Warming Phase: Bitmap Preselection

**File: `crates/uni-query/src/query/df_graph/traverse.rs`**

**Current warming** (line 2613):
```rust
let warm_fut = self.graph_ctx.warming_future(self.edge_type_ids.clone(), self.direction);
```

**New warming:** The warming future needs to do TWO things:
1. Warm adjacency CSRs (existing)
2. If `edge_filter` is Some, query Lance for matching EIDs and build `EidFilter`

**Touch points:**

1. **`execute()` method** (line 2604):
   - Create a combined warming future that does adjacency warming + bitmap building
   - Store the `EidFilter` result in the stream state (or in a shared `Arc`)

2. **New method on `GraphExecutionContext`** (in `df_graph/mod.rs:114`):
   ```rust
   pub async fn build_eid_filter(
       &self,
       edge_type_ids: &[u32],
       filter: &str,
   ) -> Result<EidFilter>
   ```
   - For each edge_type_id, look up the DeltaDataset
   - Query with `table.query().only_if(filter).select(["eid"])`
   - Apply MVCC version filtering
   - Merge results into a single EidFilter
   - Also check L0 overlay edges that match the predicate

3. **`VarLengthStreamState` enum** (line 2741):
   - The `Warming` state now carries the bitmap result
   - Or: add a new `Preselecting` state between Warming and Reading

### 3.3 Replace BFS with NFA-Driven Frontier Expansion

**File: `crates/uni-query/src/query/df_graph/traverse.rs`**

**Current BFS** (lines 2671-2737): Queue of `(Vid, depth, Vec<Vid>, Vec<Eid>)` —
clones full path vectors per queue entry, O(#paths × avg_path_len) memory.

**New BFS** — Two modes based on whether paths are needed:

#### Mode A: Endpoints-Only (no step_variable, no path_variable)

When the query only needs endpoints (e.g., `MATCH (a)-[*]->(b) RETURN b`).
Despite not needing full paths, we still need Trail-correct reachability.
The approach: build a Walk DAG with per-depth frontier, then verify each
candidate endpoint has at least one Trail-valid path.

**Why per-depth frontier (not global visited)**: Global visited prevents
re-expansion of (vid, state) pairs at later depths, which misses Trail-valid
paths through cycles (see "Trail Semantics — Correctness Analysis" section).

```rust
fn bfs_endpoints_only(
    &self,
    source: Vid,
    eid_filter: &EidFilter,
    used_eids: &FxHashSet<u64>,
    vid_filter: &VidFilter,
) -> Vec<(Vid, u32)> {
    let nfa = &self.nfa;
    let mut candidates: Vec<(Vid, NfaStateId, u32)> = Vec::new();

    // Build a lightweight DAG for Trail verification.
    // Even in endpoints-only mode, we need the DAG to verify Trail correctness.
    let mut dag = PredecessorDag::new(PathSelector::All);

    // BFS frontier: (vid, nfa_state) pairs at current depth.
    // Per-depth dedup: a pair can appear at multiple depths but only once per depth.
    let mut frontier: Vec<(Vid, NfaStateId)> = vec![(source, nfa.start_state)];

    // Per-depth dedup: a pair can appear at multiple depths but only once per depth.
    // This is NOT a global visited — pairs CAN reappear at later depths.
    let mut seen_at_depth: FxHashSet<(Vid, NfaStateId)> = FxHashSet::default();

    // Check if start state is accepting (zero-length path)
    if nfa.is_accepting(nfa.start_state) && vid_filter.contains(source) {
        candidates.push((source, nfa.start_state, 0));
    }

    for depth in 1..=self.max_hops as u32 {
        seen_at_depth.clear();
        let mut next_frontier: Vec<(Vid, NfaStateId)> = Vec::new();

        for &(vid, state) in &frontier {
            for transition in nfa.transitions_from(state) {
                for &edge_type_id in &transition.edge_type_ids {
                    let neighbors = self.graph_ctx.get_neighbors(
                        vid, edge_type_id, transition.direction
                    );

                    let mut seen_edges: FxHashSet<u64> = FxHashSet::default();

                    for (neighbor, eid) in neighbors {
                        if !eid_filter.contains(eid) { continue; }
                        if used_eids.contains(&eid.as_u64()) { continue; }
                        if transition.direction == Direction::Both
                            && !seen_edges.insert(eid.as_u64()) { continue; }

                        // Record predecessor for Trail verification
                        dag.add_predecessor(
                            neighbor, transition.to,
                            vid, state,
                            eid, depth,
                        );

                        // Per-depth dedup: only add to frontier once per depth
                        let pair = (neighbor, transition.to);
                        if seen_at_depth.insert(pair) {
                            next_frontier.push(pair);
                        }

                        // Record candidate if accepting
                        if depth >= self.min_hops as u32
                            && nfa.is_accepting(transition.to)
                            && vid_filter.contains(neighbor)
                        {
                            candidates.push((neighbor, transition.to, depth));
                        }
                    }
                }
            }
        }

        if next_frontier.is_empty() { break; }
        if next_frontier.len() > MAX_FRONTIER_SIZE { break; } // safety cap
        frontier = next_frontier;
    }

    // Trail verification phase:
    // Walk BFS finds candidate endpoints. Verify each has at least one
    // Trail-valid path via DFS over the DAG (early-stop on first valid).
    candidates.iter()
        .filter(|&&(vid, state, depth)| {
            dag.has_trail_valid_path(source, vid, state, depth, depth)
        })
        .map(|&(vid, _, depth)| (vid, depth))
        .collect()
}
```

This mode:
- Uses Walk semantics with per-depth frontier (not global visited)
- Builds lightweight DAG for Trail verification
- Per-candidate Trail existence check via `has_trail_valid_path()` (early-stop)
- Memory: O(#discovered_edges) for DAG, O(frontier_size) per level
- No path struct materialization — just (target, depth) pairs in output

#### Mode B: All-Paths with Predecessor DAG

When step_variable or path_variable is set, we need full paths. Use Walk
semantics with per-depth frontier during expansion, build the PredecessorDag,
then enumerate paths lazily during output with Trail/Acyclic filtering.

**Per-depth frontier**: A (vid, state) pair CAN appear at multiple depths.
At each depth, we deduplicate (no pair appears twice in the SAME depth's
frontier), but we DO re-expand a pair if it reappears at a later depth.
This ensures the DAG captures all paths, not just shortest-first paths.

Total BFS work is bounded by O(V × S × D × avg_degree) where V = vertices,
S = NFA states, D = max_depth. Safety caps limit this in pathological cases.

```rust
fn bfs_with_dag(
    &self,
    source: Vid,
    eid_filter: &EidFilter,
    used_eids: &FxHashSet<u64>,
    vid_filter: &VidFilter,
    selector: PathSelector,
) -> (Vec<(Vid, NfaStateId, u32)>, PredecessorDag) {
    let nfa = &self.nfa;
    let mut dag = PredecessorDag::new(selector);
    let mut accepting_results: Vec<(Vid, NfaStateId, u32)> = Vec::new();

    // BFS frontier: (vid, nfa_state) pairs at current depth
    let mut frontier: Vec<(Vid, NfaStateId)> = vec![(source, nfa.start_state)];

    // Per-depth dedup: reset each depth layer.
    // NOT global visited — pairs CAN reappear at later depths.
    let mut seen_at_depth: FxHashSet<(Vid, NfaStateId)> = FxHashSet::default();

    // Check zero-length accepting
    if nfa.is_accepting(nfa.start_state) && vid_filter.contains(source) {
        accepting_results.push((source, nfa.start_state, 0));
    }

    for depth in 1..=self.max_hops as u32 {
        seen_at_depth.clear();
        let mut next_frontier: Vec<(Vid, NfaStateId)> = Vec::new();

        for &(vid, state) in &frontier {
            for transition in nfa.transitions_from(state) {
                for &edge_type_id in &transition.edge_type_ids {
                    let neighbors = self.graph_ctx.get_neighbors(
                        vid, edge_type_id, transition.direction
                    );

                    let mut seen_edges: FxHashSet<u64> = FxHashSet::default();

                    for (neighbor, eid) in neighbors {
                        if !eid_filter.contains(eid) { continue; }
                        if used_eids.contains(&eid.as_u64()) { continue; }
                        if transition.direction == Direction::Both
                            && !seen_edges.insert(eid.as_u64()) { continue; }

                        // Always record predecessor — multiple predecessors to
                        // same (vid, state) at same depth means multiple paths.
                        // PredecessorDag::add_predecessor respects the selector:
                        // layered mode stores all depths, shortest-only skips
                        // depths > first visit.
                        dag.add_predecessor(
                            neighbor, transition.to,
                            vid, state,
                            eid, depth,
                        );

                        // Per-depth dedup: only add to frontier once per depth
                        let pair = (neighbor, transition.to);
                        if seen_at_depth.insert(pair) {
                            next_frontier.push(pair);
                        }

                        // Record accepting results
                        if depth >= self.min_hops as u32
                            && nfa.is_accepting(transition.to)
                            && vid_filter.contains(neighbor)
                        {
                            accepting_results.push((neighbor, transition.to, depth));
                        }
                    }
                }
            }
        }

        if next_frontier.is_empty() { break; }
        if next_frontier.len() > MAX_FRONTIER_SIZE { break; }
        if dag.pred_pool.len() > MAX_PRED_POOL_SIZE { break; }
        frontier = next_frontier;
    }

    (accepting_results, dag)
}
```

Then in `build_output_batch`, instead of iterating `Vec<BfsResult>`, enumerate
paths from the DAG:

```rust
// For each accepting result (target, state, depth):
// IMPORTANT: enumerate EXACTLY at this depth, not [min..depth].
// The accepting_results list already contains one entry per (target, state, depth)
// for each depth in [min_hops, max_hops] where the target was discovered.
// Enumerating [min..depth] would re-enumerate lower depths that already have
// their own entries in accepting_results, causing duplicated work and output.
for &(target, state, depth) in &accepting_results {
    dag.enumerate_paths(
        source, target, state,
        depth, depth,  // exactly this depth, not a range
        &self.path_mode,
        &mut |nodes, edges| {
            expansions.push((row_idx, target, depth as usize, nodes.to_vec(), edges.to_vec()));
            if expansions.len() >= MAX_PATHS_PER_QUERY {
                ControlFlow::Break(())
            } else {
                ControlFlow::Continue(())
            }
        },
    );
}
```

#### Mode C: Shortest-Path (for shortestPath / allShortestPaths)

When the query uses `shortestPath()` or `allShortestPaths()`, we can use
global visited BFS (more efficient) because shortest Walk = Trail-valid
(see shortest-path theorem in "Trail Semantics" section).

```rust
fn bfs_shortest(
    &self,
    source: Vid,
    eid_filter: &EidFilter,
    used_eids: &FxHashSet<u64>,
    vid_filter: &VidFilter,
    selector: PathSelector, // AnyShortest or AllShortest
) -> (Vec<(Vid, NfaStateId, u32)>, PredecessorDag) {
    // Same as bfs_with_dag but with GLOBAL visited (not per-depth).
    // This is safe because shortest Walk = Trail-valid.
    let mut dag = PredecessorDag::new(selector); // shortest-only mode

    // ... standard BFS with global visited ...
    // dag.add_predecessor() will skip depths > first visit in shortest-only mode
}
```

#### Dispatch Logic

In `process_batch_base`, choose mode based on output requirements:

```rust
let needs_paths = self.exec.step_variable.is_some() || self.exec.path_variable.is_some();

// Determine output mode (ideally set during planning, not per-batch)
let mode = if self.exec.is_shortest_path {
    VlpOutputMode::ShortestPath { selector: self.exec.path_selector.clone() }
} else if needs_paths {
    VlpOutputMode::FullPath // or StepVariable, CountOnly based on analysis
} else {
    VlpOutputMode::EndpointsOnly
};

match mode {
    VlpOutputMode::EndpointsOnly => {
        let endpoints = self.exec.bfs_endpoints_only(vid, &eid_filter, &used_eids, &vid_filter);
        // emit one row per (target, depth) — no path data needed
    }
    VlpOutputMode::FullPath | VlpOutputMode::StepVariable | VlpOutputMode::CountOnly => {
        let (accepting, dag) = self.exec.bfs_with_dag(
            vid, &eid_filter, &used_eids, &vid_filter, PathSelector::All
        );
        // enumerate paths from DAG into expansions with Trail filtering
    }
    VlpOutputMode::ShortestPath { selector } => {
        let (accepting, dag) = self.exec.bfs_shortest(
            vid, &eid_filter, &used_eids, &vid_filter, selector
        );
        // enumerate from shortest-only DAG (Trail always valid)
    }
    VlpOutputMode::LengthOnly { needs_max: false } => {
        // min(length(p)) optimization: just emit shortest BFS depth per target.
        // Global visited BFS, no DAG, no enumeration.
        let endpoints = self.exec.bfs_shortest_depths(vid, &eid_filter, &used_eids, &vid_filter);
        // emit with _hop_count = shortest depth
    }
    _ => { /* other modes */ }
}
```

#### Safety Caps

Both modes enforce safety caps to prevent runaway expansion:

```rust
const MAX_FRONTIER_SIZE: usize = 1_000_000;     // cap BFS frontier
const MAX_PATHS_PER_SOURCE: usize = 100_000;    // cap paths per source vertex
const MAX_PRED_POOL_SIZE: usize = 10_000_000;   // cap DAG predecessor records
```

These can be configurable via GraphExecutionContext settings.

### 3.4 Used Edge Columns Integration

**File: `crates/uni-query/src/query/df_graph/traverse.rs`**

In `process_batch_base` (line 2866), before calling `bfs_nfa`:

```rust
// Collect used EIDs from previous hops (same pattern as fixed-length at line 615)
let used_edge_arrays: Vec<&UInt64Array> = self.exec.used_edge_columns.iter()
    .filter_map(|col| batch.column_by_name(col)?.as_any().downcast_ref::<UInt64Array>())
    .collect();

// Per source row, build used_eids set
let used_eids: HashSet<u64> = used_edge_arrays.iter()
    .filter_map(|arr| if arr.is_null(row_idx) { None } else { Some(arr.value(row_idx)) })
    .collect();

let bfs_results = self.exec.bfs_nfa(vid, &eid_filter, &used_eids);
```

### 3.5 Same Changes for Schemaless Variant

**`GraphVariableLengthTraverseMainExec`** (lines 3345-3393) needs identical changes:
- Add `nfa`, `edge_filter`, `used_edge_columns`, `path_mode` fields
- Modify BFS function (lines 3653-3696) to use NFA-driven expansion
- Add bitmap preselection in warming phase

---

## Phase 4: Storage Layer Changes

### 4.1 New Method: EID Filter Query

**File: `crates/uni-query/src/query/df_graph/mod.rs`**

Add to `GraphExecutionContext`:

```rust
/// Query Lance for edge IDs matching a predicate.
/// Used for bitmap preselection in VLP traversal.
pub async fn build_eid_filter(
    &self,
    edge_type_ids: &[u32],
    filter: &str,
    max_eid_hint: u64,  // for choosing DenseBitVec vs HashSet
) -> Result<EidFilter> {
    let mut matching_eids = Vec::new();

    for &etype_id in edge_type_ids {
        // Get the edge type name from schema
        let type_name = self.storage.schema()
            .edge_type_name_by_id(etype_id)
            .ok_or_else(|| anyhow!("Unknown edge type {}", etype_id))?;

        // Query DeltaDataset for this edge type
        let ds = self.storage.delta_dataset(&type_name, "fwd")?;
        let table = ds.open_table().await?;

        // Build MVCC-safe filter.
        // Edge delta datasets use `op` column (UInt8): 0=Insert, 1=Delete.
        // (NOT `_deleted` boolean — that's for vertex datasets.)
        // See delta.rs:23-27 for Op enum, property_manager.rs:617 for usage.
        let version_filter = if let Some(hwm) = self.storage.version_high_water_mark() {
            format!("({}) AND _version <= {} AND op = 0", filter, hwm)
        } else {
            format!("({}) AND op = 0", filter)
        };

        let query = table.query()
            .only_if(&version_filter)
            .select(Select::columns(&["eid"]));

        let mut stream = query.execute().await?;
        while let Some(batch) = stream.try_next().await? {
            let eid_col = batch.column_by_name("eid")
                .and_then(|c| c.as_any().downcast_ref::<UInt64Array>());
            if let Some(arr) = eid_col {
                for i in 0..arr.len() {
                    if !arr.is_null(i) {
                        matching_eids.push(arr.value(i));
                    }
                }
            }
        }

        // Also check L0 overlay for edges matching the predicate
        // (Important for visibility of uncommitted edges)
        self.overlay_l0_eid_filter(&type_name, filter, &mut matching_eids)?;
    }

    // Choose representation based on cardinality
    EidFilter::from_eids(matching_eids, max_eid_hint as usize)
}
```

### 4.2 Lance Filter Compilation for Edge Predicates

**File: `crates/uni-query/src/query/pushdown.rs`**

The `LanceFilterGenerator` at lines 543-843 already compiles Cypher expressions
to Lance SQL strings with proper escaping (single quotes doubled, wildcard
injection blocked, datetime normalization). The planner's `properties_to_expr()`
at `planner.rs:5712-5743` converts property maps (`{year: 1988}`) into
`Expr::BinaryOp` equality chains.

**DO NOT hand-roll SQL string formatting.** Route edge property predicates through
the same pipeline used for vertex pushdown:

```rust
// In df_planner.rs, when compiling VLP edge predicates:

// 1. The planner already converted {year: 1988} → Expr::BinaryOp(r.year = 1988)
//    via properties_to_expr(). This Expr is stored in the LogicalPlan::Traverse
//    node as edge_filter_properties.

// 2. Compile to Lance SQL using the existing generator:
let lance_filter: Option<String> = edge_filter_expr.as_ref().and_then(|expr| {
    let predicates = vec![expr.clone()];
    // Use edge_type_name as the variable binding (not a Cypher variable,
    // just the property namespace for Lance column resolution)
    LanceFilterGenerator::generate(&predicates, &edge_var_name, schema_props.as_ref())
});

// 3. Pass lance_filter to VLP exec constructor
```

This reuses the existing escaping, type coercion, and range optimization logic
from `LanceFilterGenerator` rather than duplicating it. The `value_to_lance()`
helper at `pushdown.rs:809-814` handles string quoting (`'O''Brien'`), NULL
handling, and datetime normalization correctly.

---

## Phase 5: Module Registration

### 5.1 New module declarations

**File: `crates/uni-query/src/query/df_graph/mod.rs`**

Add module declarations:
```rust
pub mod nfa;
pub mod bitmap;
pub mod pred_dag;
```

### 5.2 Dependencies

**File: `crates/uni-query/Cargo.toml`**

```toml
bitvec = "1"          # Dense bitset for EidFilter/VidFilter
rustc-hash = "2"      # FxHashMap/FxHashSet — fast hashing for integer keys
```

`rustc-hash` provides `FxHashMap` and `FxHashSet` which use a fast, non-cryptographic
hash function optimized for small integer keys. Used throughout the VLP engine for
`pred_head`, `seen_at_depth`, `all_ever_seen`, `edge_set`/`node_set` in enumeration,
and `used_eids`. Typical 2-3x speedup over `std::collections::HashMap` for these
use cases.

---

## PathSelector → DAG Mode Mapping

The PathSelector determines how the PredecessorDag is constructed and what
BFS strategy to use:

| PathSelector | DAG Mode | BFS Strategy | Trail Enforcement |
|---|---|---|---|
| All | Layered (all depths) | Per-depth frontier | Full enumeration |
| Any | Layered (all depths) | Per-depth frontier | Early-stop after 1 valid path |
| AnyShortest | Shortest-only (first depth) | Global visited | Not needed (theorem) |
| AllShortest | Shortest-only (first depth) | Global visited | Not needed (theorem) |
| ShortestK(k) | Shortest-only (first depth) | Global visited | Not needed (theorem) |

For shortest-only selectors, `PredecessorDag::add_predecessor()` skips predecessors
at depths greater than the first-visit depth, keeping the DAG compact. Global visited
BFS is used since the shortest-path theorem guarantees Trail validity.

---

## VidFilter for Target Vertex Predicates

In addition to EidFilter (edge property bitmap), we need VidFilter for target
vertex property predicates. This handles Match4[4] where `{var: 'end'}` on the
target vertex is currently only a post-filter.

**Implementation**: Same pattern as EidFilter, but querying VertexDataset:

```rust
pub async fn build_vid_filter(
    &self,
    label_name: Option<&str>,
    filter: &str,
) -> Result<VidFilter> {
    let mut matching_vids = Vec::new();

    if let Some(label) = label_name {
        // Query specific label's vertex dataset.
        // Vertex datasets use `_deleted` boolean (NOT `op` — that's for edges).
        // See property_manager.rs:424-435 for vertex visibility check.
        let table = self.storage.vertex_dataset(label)?.open_table().await?;
        let version_filter = format!("({}) AND _deleted = false", filter);
        let query = table.query()
            .only_if(&version_filter)
            .select(Select::columns(&["_vid"]));
        // ... collect VIDs from stream
    } else {
        // No label constraint — scan all vertex datasets
        // This is more expensive but necessary for unlabeled target patterns
        for label in self.storage.schema().vertex_labels() {
            // ... same query per label
        }
    }

    // Also overlay L0 vertices matching the predicate
    self.overlay_l0_vid_filter(label_name, filter, &mut matching_vids)?;

    VidFilter::from_vids(matching_vids)
}
```

The VidFilter is checked in BFS at emit time (accepting state reached AND
`vid_filter.contains(target_vid)`). This replaces the current label-only check
at `traverse.rs:2684-2692` with a full property + label check.

**Planner integration**: The target vertex predicate comes from the
`target_filter: Option<Expr>` field on `LogicalPlan::Traverse`. The physical
planner compiles it to a Lance SQL string using `LanceFilterGenerator` and passes
it to the VLP exec, which builds the VidFilter during warming.

---

## Complete File-by-File Touch Point Summary

### New Files (4)

| File | Purpose | Est. Lines |
|------|---------|-----------|
| `crates/uni-query/src/query/df_graph/nfa.rs` | PathNfa, NFA compilation, PathMode, PathSelector, VlpOutputMode | ~300 |
| `crates/uni-query/src/query/df_graph/bitmap.rs` | EidFilter, VidFilter, bitmap construction | ~150 |
| `crates/uni-query/src/query/df_graph/pred_dag.rs` | PredecessorDag, pool-based storage, FxHashMap/FxHashSet, streaming enumeration with O(1) Trail/Acyclic checks, has_trail_valid_path | ~450 |
| `crates/uni-query/tests/nfa_tests.rs` | Unit tests for NFA compilation | ~300 |

### Modified Files (8)

| File | Lines | Changes |
|------|-------|---------|
| `crates/uni-cypher/src/ast.rs` | ~392 | Add `PathMode` enum (4 variants), `PathSelector` enum (5 variants). Optionally add `path_mode` to `PathPattern`. |
| `crates/uni-query/src/query/planner.rs` | ~1728, ~1758, ~4145, ~4274, ~4395, ~4497 | Add `edge_filter_properties`, `edge_filter_where`, `path_mode` fields to `Traverse` and `TraverseMainByType` enum variants. Modify `plan_traverse_with_source` to NOT wrap VLP edge predicates as Filter; instead store them in the Traverse node. |
| `crates/uni-query/src/query/df_planner.rs` | ~1634, ~1866-1940, ~2130-2191 | In VLP branches: compile edge predicates to Lance filter, call `collect_used_edge_columns`, pass `nfa`, `edge_filter`, `used_edge_columns`, `path_mode` to VLP exec constructors. |
| `crates/uni-query/src/query/df_graph/mod.rs` | ~114, ~357 | Add module declarations. Add `build_eid_filter()` method to `GraphExecutionContext`. |
| `crates/uni-query/src/query/df_graph/traverse.rs` | ~2362-2413, ~2431-2486, ~2604-2629, ~2653-2667, ~2671-2737, ~2741-2750, ~2866-2925, ~3345-3393, ~3411-3457, ~3653-3696 | Major changes: add NFA/bitmap/used_edge_columns/output_mode fields to both VLP exec structs. Replace BFS with three modes (endpoints-only with DAG-based Trail verification, all-paths with per-depth frontier + layered DAG, shortest with global visited). Add bitmap preselection in warming phase. Add VlpOutputMode dispatch in process_batch. Same for schemaless variant. |
| `crates/uni-query/src/query/pushdown.rs` | ~543 | Add `compile_edge_properties()` function for edge predicate → Lance SQL. |
| `crates/uni-query/Cargo.toml` | deps | Add `bitvec = "1"`, `rustc-hash = "2"` dependencies. |
| `crates/uni-cypher/src/grammar/walker.rs` | ~1460 | Wire up parenthesized pattern WHERE clauses (currently warns). |

---

## Detailed Change Specifications per Failing TCK Scenario

### Match4[4] — 20-hop chain, 17 rows instead of 1

**Root cause:** The query is `MATCH (n {var: 'start'})-[:T*]->(m {var: 'end'})`.
The `{var: 'end'}` predicate on `m` is applied as a target_filter in the logical
plan. In the BFS, `target_label_name` filtering checks labels but NOT properties.
The target property filter is only applied as a post-filter after VLP expansion.

With unbounded `*` (default max=100), the BFS emits at every depth 1..20 where it
finds any reachable node. The 20-hop chain has intermediate nodes that don't have
`{var: 'end'}`, but the BFS emits them anyway. The post-filter SHOULD catch these,
but if target properties aren't materialized yet at that point, filtering fails.

**Fix:** The bitmap preselection for target vertices (VidFilter) will handle this.
During warming, compute `AllowedTargetVids = {vids where var = 'end'}`. In BFS,
only emit results where `AllowedTargetVids.contains(target_vid)`.

**Files:** `traverse.rs` (BFS emit check), `df_graph/mod.rs` (build_vid_filter),
`df_planner.rs` (pass target filter into VLP exec).

### Match4[5] — Edge property predicate `{year: 1988}`, 3 rows instead of 1

**Root cause:** The planner creates `Filter(VLP, "r.year = 1988")` as a post-filter.
But `r` in a VLP is a LIST of relationships, and the post-filter would need to check
that ALL relationships in the list satisfy the predicate. The current filter just
checks `r.year = 1988` on a list type, which doesn't work correctly.

**Fix:** Bitmap preselection. During warming, query Lance:
`SELECT eid FROM WORKED_WITH_delta WHERE year = 1988 AND op != 1`
Build `AllowedEids`. In BFS, skip edges not in AllowedEids.

**Files:** `planner.rs` (don't wrap VLP edge predicates as Filter), `df_planner.rs`
(compile to Lance filter), `traverse.rs` (bitmap check in BFS), `pushdown.rs`
(compile_edge_properties), `df_graph/mod.rs` (build_eid_filter).

### Match4[7] — Bound relationship, count=84 instead of 32

**Root cause:** Pattern: `()-[r:EDGE]-() MATCH p = (n)-[*0..1]-()-[r]-()-[*0..1]-(m)`.
The pre-bound `r` from the first MATCH appears in the second MATCH pattern. The VLP
segments `[*0..1]` don't exclude `r`'s EID from their expansion because VLP has no
`used_edge_columns`.

**Fix:** Add `used_edge_columns` to VLP exec. The planner's
`collect_used_edge_columns` already extracts `r._eid` from the input schema.
Pass these to VLP, and in BFS, exclude them during expansion.

**Files:** `traverse.rs` (add used_edge_columns field, use in BFS),
`df_planner.rs` (call collect_used_edge_columns for VLP).

### Match5[27] — Undirected VLP, 20 rows instead of 16

**Root cause:** Pattern: `(a)-[:LIKES]->()<-[:LIKES*3]->(c)`. The `<-[:LIKES*3]->`
is undirected VLP (Direction::Both). With the modified graph (some edges reversed),
the undirected VLP explores both directions. The issue is that cross-pattern
relationship uniqueness against the first `[:LIKES]` hop is not enforced.

**Fix:** Same as Match4[7] — `used_edge_columns` from the first fixed-length
`[:LIKES]` hop propagated to the VLP exec.

**Files:** Same as Match4[7].

### Return6[13] — `a.name` schema error after WITH aggregation

**Root cause:** After `WITH a, other, min(length(p)) AS len`, the column `a` is a
grouped key (a node struct), but `a.name` in RETURN can't resolve because the
planner loses property access through the aggregation pipeline. This is NOT a VLP
bug — it's an aggregation/projection scope bug.

**Fix:** In the planner's aggregation handling, ensure that grouped node variables
retain their property access columns through the WITH projection.

**Files:** `planner.rs` (aggregation handling, ~line 2800+). This is a separate
fix from the VLP engine work.

---

## Test Cases

### Unit Tests for NFA (crates/uni-query/tests/ or in nfa.rs mod tests)

```
NFA Compilation:
  1. test_vlp_to_nfa_basic           — [:KNOWS*2..5] → 6 states, accepting {q2,q3,q4,q5}
  2. test_vlp_to_nfa_unbounded       — [:KNOWS*] → states with self-loop or chain to DEFAULT_MAX
  3. test_vlp_to_nfa_zero_min        — [:KNOWS*0..3] → q0 is accepting (zero-length)
  4. test_vlp_to_nfa_exact           — [:KNOWS*3] → only q3 is accepting
  5. test_vlp_to_nfa_multi_type      — [:KNOWS|LIKES*1..3] → transitions carry both type IDs
  6. test_vlp_to_nfa_with_predicate  — [:KNOWS*1..3 {active: true}] → edge predicate on all transitions
  7. test_nfa_transitions_from       — Verify transitions_from(q0) returns correct transitions
  8. test_nfa_is_accepting           — Verify accepting state checks

QPP Compilation (future but design now):
  9. test_qpp_simple                 — (()-[:KNOWS]->(:Person)){2,4} → NFA with node predicate at intermediate states
 10. test_qpp_multi_edge             — (()-[:KNOWS]->()-[:LIKES]->(:Company)){1,2} → alternating edge types
 11. test_qpp_with_where             — (()-[r:KNOWS WHERE r.weight > 0.5]->(:Person)){1,3} → inline WHERE
 12. test_qpp_zero_min               — (pattern){0,3} → start state is accepting
 13. test_qpp_plus_quantifier        — (pattern)+ → min=1, max=DEFAULT_MAX
 14. test_qpp_star_quantifier        — (pattern)* → min=0, max=DEFAULT_MAX
```

### Unit Tests for Bitmap (in bitmap.rs mod tests)

```
EidFilter:
 15. test_all_allowed                — AllAllowed.contains() always true
 16. test_dense_bitvec_contains      — Build from [1,3,5,7], check contains/not-contains
 17. test_dense_bitvec_empty         — Empty set, all contains() return false
 18. test_hashset_contains           — Build from sparse set, check membership
 19. test_from_eids_chooses_dense    — Large cardinality → DenseBitVec
 20. test_from_eids_chooses_hashset  — Small cardinality → HashSet
 21. test_from_lance_scan            — Mock RecordBatch stream → correct EidFilter

VidFilter:
 22. test_vid_filter_basic           — Same as EidFilter but for vertex IDs
```

### Unit Tests for Predecessor DAG (in pred_dag.rs mod tests)

```
Pool-based storage:
 23. test_pred_dag_add_single        — Add one predecessor, verify pool has 1 entry
 24. test_pred_dag_add_chain         — A→B→C chain, pool has 2 entries, heads chain correctly
 25. test_pred_dag_multiple_preds    — A→C and B→C at depth 1, both in same chain via `next`
 26. test_pred_dag_first_depth       — first_depth tracks minimum discovery depth per (vid,state)

PathSelector / DAG mode:
 27. test_pred_dag_layered_stores_all — PathSelector::All stores preds at multiple depths
 28. test_pred_dag_shortest_skips    — PathSelector::AnyShortest skips preds at depth > first
 29. test_pred_dag_selector_switch   — Same graph, different selectors → different DAG sizes

Enumeration — Walk mode:
 30. test_pred_dag_linear_walk       — A→B→C, enumerate gives one path [A,B,C]
 31. test_pred_dag_diamond_walk      — A→{B,C}→D, enumerate gives two paths to D
 32. test_pred_dag_multiple_depths   — Target reachable at depth 2 and 3, both paths enumerated
 33. test_pred_dag_fan_out           — A→{B1,B2,B3}→C, 3 paths of length 2

Enumeration — Trail mode (with FxHashSet):
 34. test_pred_dag_trail_no_repeat   — Cycle A→B→A→C, Trail filters path using edge A→B twice
 35. test_pred_dag_trail_allows_node — A→B→C→B (node repeat OK, different edges), Trail allows it
 36. test_pred_dag_trail_diamond     — Diamond with distinct edges, Trail keeps both paths
 37. test_pred_dag_trail_cycle_2     — A→e1→B→e2→A, *3: B@3 path uses e1 twice → rejected

Enumeration — Acyclic mode (with FxHashSet):
 38. test_pred_dag_acyclic_filter    — A→B→C→A, Acyclic rejects (node A repeated)
 39. test_pred_dag_acyclic_diamond   — A→{B,C}→D (no node repeats), Acyclic keeps both

Trail existence check:
 40. test_has_trail_valid_true       — Simple chain A→B→C, has_trail_valid_path returns true
 41. test_has_trail_valid_false      — Only path to target reuses edge, returns false
 42. test_has_trail_valid_one_of_many — Multiple Walk paths, only some Trail-valid, returns true

Streaming / early termination:
 43. test_pred_dag_early_stop        — 100 paths exist, callback returns Break after 5
 44. test_pred_dag_empty_enumerate   — No accepting results, enumerate yields nothing
 45. test_pred_dag_zero_length       — Zero-length path (source is accepting), no DAG needed

Correctness:
 46. test_pred_dag_path_order        — Paths enumerate in correct order (nodes/edges forward)
 47. test_pred_dag_eid_in_path       — Edge IDs in enumerated path match the traversal order
```

### Integration Tests for VLP Correctness (new file: crates/uni/tests/vlp_nfa_test.rs)

```
Basic VLP (should already pass, regression):
 48. test_vlp_basic_chain            — A→B→C→D, MATCH (a)-[*1..3]->(b), expect 6 results
 49. test_vlp_exact_hops             — *2 exact, expect only 2-hop results
 50. test_vlp_zero_min               — *0..2, expect self-match + 1-hop + 2-hop
 51. test_vlp_zero_zero              — *0..0, expect only self-match
 52. test_vlp_inverted_bounds        — *3..1, expect 0 results
 53. test_vlp_unbounded              — *, expect all reachable
 54. test_vlp_typed_edges            — Only traverse specified edge types
 55. test_vlp_multi_type             — [:KNOWS|LIKES*1..2], traverse both types

Edge Property Predicate (Match4[5] fix):
 56. test_vlp_edge_prop_all_match    — All edges have year=1988, VLP returns full paths
 57. test_vlp_edge_prop_partial      — Some edges year=1988, only paths through them
 58. test_vlp_edge_prop_none_match   — No edges match, 0 results
 59. test_vlp_edge_prop_multi        — {year: 1988, active: true}, both must match
 60. test_vlp_edge_prop_with_step    — [r:T*1..3 {year: 1988}] RETURN r, verify edge list
 61. test_vlp_edge_prop_null         — Some edges have NULL for the property
 62. test_vlp_edge_prop_range        — WHERE r.weight > 0.5 (future: inline WHERE on VLP)

Long Chain (Match4[4] fix):
 63. test_vlp_20_hop_chain           — 20-node chain, target endpoint has unique property
 64. test_vlp_long_chain_no_match    — 20-node chain, no node has target property, 0 results
 65. test_vlp_long_chain_multiple    — Multiple targets at different depths

Cross-Pattern Uniqueness (Match4[7] fix):
 66. test_vlp_used_edges_basic       — (a)-[r]->(b) MATCH (b)-[*1..2]->(c), r excluded from VLP
 67. test_vlp_used_edges_count       — Count paths with bound relationship exclusion
 68. test_vlp_used_edges_zero_len    — *0..1 with excluded edge, still includes zero-hop
 69. test_vlp_bound_relationship     — ()-[r:E]-() MATCH p=(n)-[*0..1]-()-[r]-()-[*0..1]-(m)

Direction (Match5[27] fix):
 70. test_vlp_undirected_basic       — -[*1..2]- (both directions)
 71. test_vlp_undirected_dedup       — Same edge not double-counted in Both mode
 72. test_vlp_undirected_with_used   — Undirected VLP + used_edge_columns
 73. test_vlp_mixed_direction_chain  — Forward fixed + undirected VLP in same pattern
 74. test_vlp_incoming_only          — <-[*1..3]- (incoming direction VLP)

Path Variable:
 75. test_vlp_named_path             — p = (a)-[*1..3]->(b) RETURN p
 76. test_vlp_path_length            — length(p) on VLP paths
 77. test_vlp_path_nodes             — nodes(p) returns correct node list
 78. test_vlp_path_relationships     — relationships(p) returns correct edge list
 79. test_vlp_path_extension         — Named path extends across VLP + fixed hops

Step Variable (Relationship List):
 80. test_vlp_step_var_basic         — [r*1..2] RETURN r, check list structure
 81. test_vlp_step_var_type          — type(r[0]) returns correct edge type
 82. test_vlp_step_var_properties    — r[0].year returns correct property value
 83. test_vlp_step_var_size          — size(r) == length of path

OPTIONAL MATCH + VLP:
 84. test_vlp_optional_match         — OPTIONAL MATCH (a)-[*]->(b), unmatched → NULL
 85. test_vlp_optional_bound_target  — OPTIONAL MATCH with bound target, no path → NULL
 86. test_vlp_optional_with_props    — OPTIONAL + VLP edge property predicate

Trail Semantics & Correctness:
 87. test_vlp_trail_no_edge_repeat   — Cyclic graph, same edge not traversed twice per path
 88. test_vlp_trail_node_repeat_ok   — Same node CAN be visited via different edges
 89. test_vlp_trail_cycle_termination — Traversal terminates despite cycles (no infinite loop)
 90. test_vlp_trail_directed_cycle   — A→B→C→A, *3: path through full cycle is valid Trail
 91. test_vlp_trail_false_positive   — A→B→A 2-cycle, *3: B@3 Walk-reachable but NOT Trail-valid
 92. test_vlp_trail_global_vs_perdt  — Graph where global visited misses valid Trail endpoint
 93. test_vlp_trail_undirected_same  — -[:T*2]- on A-[e1]-B: e1 used both directions → invalid

Bitmap Preselection:
 94. test_vlp_bitmap_selective        — Only 1% of edges match predicate, verify correct results
 95. test_vlp_bitmap_all_match        — AllAllowed optimization when no predicate
 96. test_vlp_bitmap_with_l0_overlay  — Uncommitted edges in L0 included in bitmap

Endpoints-Only Mode (with Trail verification):
 97. test_vlp_endpoints_only_basic    — RETURN b only, verify distinct targets
 98. test_vlp_endpoints_count         — RETURN count(b), works without path tracking
 99. test_vlp_endpoints_with_props    — RETURN b.name, target properties hydrated correctly
100. test_vlp_endpoints_vs_paths      — Same graph, compare endpoints-only vs path mode results
101. test_vlp_endpoints_trail_cycle   — Endpoints mode on cyclic graph gives correct Trail results

VidFilter (Target Vertex Bitmap):
102. test_vlp_vid_filter_by_prop      — Target {var:'end'} filters to correct endpoints
103. test_vlp_vid_filter_by_label     — Target :Person filters by label during BFS
104. test_vlp_vid_filter_combined     — Target :Person {active: true} — both label + property
105. test_vlp_vid_filter_none_match   — No target matches filter, 0 results

Output Mode Detection:
106. test_vlp_mode_endpoints_only     — No p/r → endpoints-only mode selected
107. test_vlp_mode_length_only        — Only min(length(p)) → length-only mode
108. test_vlp_mode_count_only         — Only count(p) → count-only mode
109. test_vlp_mode_full_path          — RETURN p → full path mode
110. test_vlp_mode_step_variable      — RETURN r → step variable mode

shortestPath Unification:
111. test_vlp_shortest_basic          — shortestPath via VLP engine matches current impl
112. test_vlp_all_shortest_basic      — allShortestPaths returns all equal-length shortest
113. test_vlp_shortest_no_path        — shortestPath when no path exists → NULL
```

### Integration Tests for QPP (future, but design test cases now)

```
QPP Basic:
114. test_qpp_simple_repeat          — (()-[:KNOWS]->(:Person)){2,4}, same as *2..4 but QPP syntax
115. test_qpp_mixed_types            — (()-[:KNOWS]->()-[:LIKES]->(:Company)){1,2}
116. test_qpp_with_inline_where      — ((a)-[r WHERE r.active]->())+
117. test_qpp_zero_quantifier        — (pattern){0,2} includes zero-length
118. test_qpp_star_quantifier        — (pattern)* = {0,}
119. test_qpp_plus_quantifier        — (pattern)+ = {1,}
120. test_qpp_exact_quantifier       — (pattern){3} exactly 3 repetitions
121. test_qpp_group_variables        — Variables inside QPP become lists outside
122. test_qpp_nested_labels          — Node labels at each hop position
123. test_qpp_with_named_path        — p = (a)(pattern){2,3}(b), length(p), nodes(p)
124. test_qpp_combined_with_fixed    — (a)-[:X]->(b)(()-[:Y]->(:Z)){1,3}(c)

QPP Path Semantics:
125. test_qpp_trail_default          — No repeated edges across pattern repetitions
126. test_qpp_walk_mode              — MATCH WALK ... allows repeated edges
127. test_qpp_acyclic_mode           — No repeated vertices across repetitions

QPP Edge Cases:
128. test_qpp_empty_match            — No nodes match intermediate label, 0 results
129. test_qpp_single_iteration       — {1,1} behaves like non-quantified sub-pattern
130. test_qpp_disconnected_graph     — QPP on disconnected components
131. test_qpp_self_loop              — QPP traversal through self-loop edges
```

### TCK Scenario Coverage (verify all pass after changes)

```
Regression (must still pass after NFA refactor):
132. Match4[1-3, 6, 8-10]           — All currently passing Match4 VLP scenarios
133. Match5[1-26, 28-29]            — All currently passing Match5 VLP scenarios
134. Match6[1-97]                   — All named path scenarios (100% pass today)
135. Match7[1-31]                   — All optional match scenarios (100% pass today)
136. Return6[8]                     — VLP + aggregation (passes today)

Fixes (must pass after implementation):
137. Match4[4]                      — 20-hop chain → 1 row (VidFilter fix)
138. Match4[5]                      — Edge property predicate → 1 row (EidFilter fix)
139. Match4[7]                      — Bound relationship count → 32 (used_edge_columns fix)
140. Match5[27]                     — Undirected VLP → 16 rows (used_edge_columns fix)
141. Return6[13]                    — a.name after WITH aggregation (separate fix)
```

---

## Implementation Order

### Phase 1: Foundation Types (no behavior change)
1. Create `nfa.rs` with PathNfa, PathMode, PathSelector, NFA compilation
2. Create `bitmap.rs` with EidFilter, VidFilter
3. Create `pred_dag.rs` with PredecessorDag, pool storage, FxHashMap/FxHashSet,
   PathSelector-aware add_predecessor, Trail enumeration with hash sets,
   has_trail_valid_path
4. Add `bitvec` and `rustc-hash` dependencies
5. Write unit tests for all three modules (tests 1-47)

### Phase 2: Planner Changes
6. Add `edge_filter_properties` and `path_mode` to LogicalPlan::Traverse
7. Add `edge_filter_properties` and `path_mode` to LogicalPlan::TraverseMainByType
8. Modify `plan_traverse_with_source` to NOT wrap VLP edge predicates as Filter
9. In df_planner.rs, compile edge predicates to Lance filter string
10. In df_planner.rs, call `collect_used_edge_columns` for VLP
11. Add VlpOutputMode detection based on path/step variable usage analysis
12. Pass new fields through to VLP exec constructors

### Phase 3: Execution Layer — Core BFS
13. Add new fields to GraphVariableLengthTraverseExec (nfa, edge_filter,
    used_edge_columns, path_mode, output_mode)
14. Add `build_eid_filter` and `build_vid_filter` to GraphExecutionContext
15. Modify warming phase to include bitmap preselection (EidFilter + VidFilter)
16. Implement `bfs_endpoints_only` with per-depth frontier, DAG-based Trail
    verification via `has_trail_valid_path()`
17. Implement `bfs_with_dag` with per-depth frontier, layered DAG
18. Implement `bfs_shortest` with global visited (for shortestPath)
19. Add used_edge_columns integration in process_batch
20. Add dispatch logic in process_batch based on VlpOutputMode
21. Same changes for schemaless variant (GraphVariableLengthTraverseMainExec)

### Phase 4: Integration Testing
22. Write integration tests for each failing TCK scenario
23. Write Trail correctness tests (cycle graphs, undirected, Trail verification)
24. Write output mode tests (endpoints vs path vs count vs length)
25. Run TCK suite, verify all 5 VLP failures now pass
26. Run full TCK suite, verify no regressions

### Phase 5: Shortest Path Unification (can follow Phase 4)
27. Add PathSelector support to VLP exec (AnyShortest, AllShortest)
28. Route shortestPath() through VLP engine instead of separate operator
29. Implement allShortestPaths (currently unimplemented)
30. Write shortest path integration tests

### Phase 6: QPP Support (future)
31. Wire `PatternElement::Parenthesized` to NFA compilation
32. Add group variable handling in planner
33. Write QPP integration tests

### Phase 7: Existential VLP (future)
34. Wire EXISTS with VLP patterns through VLP engine with Mode 7
35. Early termination on first Trail-valid path
36. Write EXISTS + VLP integration tests
