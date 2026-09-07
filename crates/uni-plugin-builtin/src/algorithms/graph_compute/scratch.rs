// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Mode B-seq — a metered, mutable *scratch graph* for sequential guest programs
//! (plugin-compute proposal §7b).
//!
//! Mode A/B-vec are bulk/vectorized: no per-element guest callback, no mutation.
//! Adaptive search over evolving structure — MCTS, Dinic augmenting paths, VF2
//! subgraph match — needs the opposite: a guest running its *own* loop against a
//! fast **random-access read + mutable scratch graph**. This module is the
//! host-resident core of that runtime (also the `Q-5` perf baseline the JIT'd-WASM
//! random-access must stay within a pinned ratio of):
//!
//! - **Metered per random-access op.** Every `neighbors`/`get`/`set`/`add_*`/
//!   `sample` charges the [`WorkBudget`], extending the §5.1 native-work meter to
//!   pointer-chasing so a runaway guest loop is charged and halts at `0x865`
//!   (test `Q-1`).
//! - **Bounded mutable arena.** Graph growth (`add_node`/`add_edge`) charges the
//!   [`Arena`] byte cap; exceeding it is a typed error at the allocating op
//!   (`0x864`, test `Q-2`).
//! - **Deterministic sampling.** `sample` draws from the promoted counter-hash
//!   (`counter_hash(seed, iter, elem)`), so a seeded run's tie-breaks are
//!   reproducible (the basis for `Q-4`).
//!
//! The **compiled-only registration gate** ([`require_compiled_body`], `Q-6`)
//! and the **host-side guest ABI** are here too: [`ScratchGraph::call_json`] is
//! the single-session JSON dispatch, and [`ScratchRegistry`] the multi-session
//! host surface a WASM/Extism `host-graph` import wires to (unguessable session
//! ids + panic isolation, mirroring
//! [`GraphComputeRegistry`](super::dispatch::GraphComputeRegistry)).
//!
//! Deliberately **not** in this host-resident core (documented remaining work):
//! the WASM `.wasm` *guest* fixture driving this ABI end-to-end through wasmtime
//! (loader `with_scratch` wiring + `build-wasm-fixtures.sh`), the store
//! snapshot-isolation contract against a concurrent reader (`Q-3`, proposal §7b /
//! open question 3 — the structural session-local isolation is in place), and the
//! JIT'd-WASM arm of the perf gate (`Q-5`; the host baseline + JSON-ABI harness
//! is `crates/uni/benches/mode_b_seq_random_access.rs`).
//
// Rust guideline compliant

// The whole module is deprecated together (see the item attributes below), so its
// own internal references must not re-trip the lint.
#![allow(deprecated)]

use serde::{Deserialize, Serialize};
use uni_algo::algo::rng::sample_bernoulli;
use uni_plugin::errors::FnError;

use super::error;
use super::{Arena, WorkBudget};

/// Approximate live bytes charged to the arena per scratch node.
///
/// A node owns an `f64` field plus the header of its adjacency `Vec`; the exact
/// figure is unimportant, only that growth is *charged* so an unbounded
/// `add_node` loop hits the arena cap (proposal §7b).
const NODE_BYTES: usize = 32;

/// Approximate live bytes charged to the arena per scratch edge (`u32` slot).
const EDGE_BYTES: usize = 4;

/// The class of loader authoring a Mode B-seq sequential body (proposal §7b).
///
/// Only **compiled** bodies (WASM/Rust) may drive the sequential per-step loop:
/// an interpreted per-step body (Rhai row-mode) cannot amortize per-step
/// interpretation, so it is disqualified on perf. Vectorized/bulk loaders remain
/// fine for Mode A / Mode B-vec — this gate is specific to the B-seq step loop.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[deprecated(
    since = "3.1.0",
    note = "superseded by the `arena_*` kernels (HandleKind::Arena); this parallel \
            stack was unreachable from every loader, which is issue #152. Removal at the \
            next major — see docs/proposals/guest_stateful_compute_2026-07-20.md §6/§13.4"
)]
pub enum LoaderClass {
    /// A compiled body (WASM / native Rust) — permitted for Mode B-seq.
    Compiled,
    /// An interpreted body (Rhai row-mode) — rejected for Mode B-seq.
    Interpreted,
}

/// Gates Mode B-seq body registration on a **compiled** loader (test Q-6).
///
/// Returns a typed capability-denied error (`0x86C`) for an interpreted body, so
/// the compiled-only contract is enforced at registration rather than
/// discovered as a perf cliff at run time.
///
/// # Errors
/// Returns [`FnError`] `0x86C` when `loader` is [`LoaderClass::Interpreted`].
#[deprecated(
    since = "3.1.0",
    note = "refuted by measurement: interpreted Rhai (1.97M rollouts/s) beats compiled \
            WASM on JSON (575K), so loader class does not predict throughput. Dead code; \
            removal at the next major — see the proposal §6"
)]
pub fn require_compiled_body(loader: LoaderClass) -> Result<(), FnError> {
    match loader {
        LoaderClass::Compiled => Ok(()),
        LoaderClass::Interpreted => Err(error::capability_denied(
            "Mode B-seq requires a compiled body (WASM/Rust); an interpreted \
             per-step body (Rhai row-mode) is disqualified on perf",
        )),
    }
}

/// A per-invocation, session-local mutable graph a Mode B-seq guest builds and
/// walks under a work + arena budget.
///
/// Slots are dense `u32` ids assigned by [`add_node`](Self::add_node). The graph
/// is **never observable by the store** — it lives and dies with the session
/// (the §7b snapshot-isolation contract; the store-visibility guarantee itself is
/// tracked as `Q-3`). Every accessor is fallible so budget/arena exhaustion and
/// out-of-range slots surface as typed errors rather than panics.
#[derive(Debug)]
#[deprecated(
    since = "3.1.0",
    note = "superseded by the `arena_*` kernels (HandleKind::Arena); this parallel \
            stack was unreachable from every loader, which is issue #152. Removal at the \
            next major — see docs/proposals/guest_stateful_compute_2026-07-20.md §6/§13.4"
)]
pub struct ScratchGraph {
    /// Out-adjacency per node slot.
    adjacency: Vec<Vec<u32>>,
    /// One `f64` field per node slot (agent state / flow / visit count).
    fields: Vec<f64>,
    /// Per-out-edge `f64` state, parallel to `adjacency` (issue #152 gap 2).
    ///
    /// The node-shaped `fields` above cannot hold edge state, so flow/residual
    /// algorithms had nowhere to keep capacities. This is the `[E]`-shaped
    /// counterpart.
    edge_fields: Vec<Vec<f64>>,
    /// For each out-edge, the index of its paired reverse edge in the
    /// destination's adjacency (`u32::MAX` when unpaired).
    ///
    /// Residual-graph algorithms need to credit the reverse arc when they push
    /// flow; without the pairing, `edge_fields` alone is not enough.
    rev: Vec<Vec<u32>>,
    /// Guest-authored per-node columns, addressed by handle (issue #152 gap 6).
    ///
    /// The single `fields` vec above cannot hold a real search node (visits *and*
    /// value, plus a derived score); these are the extra `[N]` columns a guest
    /// composes its own policy from.
    columns: Vec<Vec<f64>>,
    /// Parent slot per node (`u32::MAX` for a root), maintained by
    /// [`Self::add_child`].
    ///
    /// UCB needs `ln(visits[parent])` gathered to each child; without a parent
    /// link the guest cannot express it at all.
    parent: Vec<u32>,
    /// Host-resident active sets, addressed by handle (issue #152, design P2).
    ///
    /// A batched kernel that takes and returns a *handle* never moves batch data
    /// across the guest boundary — no JSON array, no `Dynamic` boxing, no
    /// canonical-ABI list copy. This is the prototype backing store for that
    /// contract.
    batches: Vec<Vec<u32>>,
    /// Native-work meter — charged per random-access op (proposal §5.1 / §9).
    budget: WorkBudget,
    /// Live-bytes cap — charged on structural growth (proposal §7b).
    arena: Arena,
    /// Base seed for [`sample`](Self::sample) (the promoted counter-hash stream).
    seed: u64,
}

/// How a batched descent chooses among a node's children (issue #152 gap 6).
///
/// The built-in policy (`score_col: None`) minimizes the visit field — the
/// host-owned rule. A guest-authored policy supplies its own `[N]` score column,
/// which is what makes the algorithm the *guest's* rather than the host's.
#[derive(Clone, Copy, Debug, Default)]
pub struct DescentPolicy {
    /// Guest column handle to score by; `None` uses the built-in visit field.
    pub score_col: Option<u32>,
    /// Prefer the highest score (UCB) rather than the lowest (least-visited).
    pub maximize: bool,
    /// Virtual loss applied to a chosen node's *score* within a batch so later
    /// rollouts diverge. Only meaningful with `score_col`; the built-in policy
    /// gets divergence for free from the visit bump. `0.0` = naive lock-step.
    pub vloss: f64,
    /// Choose against a frozen snapshot taken before the batch (naive lock-step).
    pub stale: bool,
}

impl ScratchGraph {
    /// Creates an empty scratch graph metered by `budget` and `arena`.
    ///
    /// `seed` seeds the reproducible sampling stream (proposal §8).
    #[must_use]
    pub fn new(budget: WorkBudget, arena: Arena, seed: u64) -> Self {
        Self {
            adjacency: Vec::new(),
            fields: Vec::new(),
            edge_fields: Vec::new(),
            rev: Vec::new(),
            columns: Vec::new(),
            parent: Vec::new(),
            batches: Vec::new(),
            budget,
            arena,
            seed,
        }
    }

    /// Charges one unit of native work for a random-access op, failing closed.
    fn charge_op(&mut self) -> Result<(), FnError> {
        self.budget
            .try_charge(1)
            .map_err(|e| error::budget_exhausted(e.to_string()))
    }

    /// Validates a slot is in range, returning a typed error otherwise.
    fn check_slot(&self, slot: u32) -> Result<usize, FnError> {
        let i = slot as usize;
        if i < self.adjacency.len() {
            Ok(i)
        } else {
            Err(error::arg_validation(format!(
                "scratch slot {slot} out of range (node_count = {})",
                self.adjacency.len()
            )))
        }
    }

    /// Adds a node with initial field `value`, returning its slot.
    ///
    /// Charges one work unit and the node's arena bytes; growth past the arena
    /// cap is a typed `0x864` error at this op (test `Q-2`).
    ///
    /// # Errors
    /// Returns [`FnError`] `0x865` when the work budget is drained or `0x864`
    /// when the arena byte cap would be exceeded.
    pub fn add_node(&mut self, value: f64) -> Result<u32, FnError> {
        self.charge_op()?;
        self.arena
            .try_alloc(NODE_BYTES)
            .map_err(|e| error::arena_cap_exceeded(e.to_string()))?;
        #[expect(
            clippy::cast_possible_truncation,
            reason = "scratch graphs stay far below u32::MAX nodes under the arena cap"
        )]
        let slot = self.adjacency.len() as u32;
        self.adjacency.push(Vec::new());
        self.edge_fields.push(Vec::new());
        self.rev.push(Vec::new());
        self.fields.push(value);
        self.parent.push(u32::MAX);
        // Keep every existing `[N]` column the same length as the node set.
        for col in &mut self.columns {
            col.push(0.0);
        }
        Ok(slot)
    }

    /// Adds a node as a child of `parent`, recording the parent link and the
    /// edge (issue #152 gap 6).
    ///
    /// Tree-shaped arenas need the back-link so a guest policy can gather a
    /// parent statistic (e.g. UCB's `ln(visits[parent])`) down to its children.
    ///
    /// # Errors
    /// Returns [`FnError`] `0x865` on budget exhaustion, `0x864` on arena cap, or
    /// `0x86E` for a bad parent slot.
    pub fn add_child(&mut self, parent: u32, value: f64) -> Result<u32, FnError> {
        let p = self.check_slot(parent)?;
        let slot = self.add_node(value)?;
        self.add_edge(parent, slot)?;
        self.parent[slot as usize] = u32::try_from(p).unwrap_or(u32::MAX);
        Ok(slot)
    }

    /// Allocates a zeroed `[N]` guest column, returning its handle.
    ///
    /// # Errors
    /// Returns [`FnError`] `0x865` on budget exhaustion or `0x864` on arena cap.
    pub fn column_new(&mut self) -> Result<u32, FnError> {
        let n = self.fields.len();
        self.budget
            .try_charge(n as u64)
            .map_err(|e| error::budget_exhausted(e.to_string()))?;
        self.arena
            .try_alloc(n * std::mem::size_of::<f64>())
            .map_err(|e| error::arena_cap_exceeded(e.to_string()))?;
        self.columns.push(vec![0.0; n]);
        #[expect(clippy::cast_possible_truncation, reason = "column count is small")]
        Ok((self.columns.len() - 1) as u32)
    }

    /// Reads a guest column (host-side inspection / tests).
    #[must_use]
    pub fn column(&self, h: u32) -> Option<&[f64]> {
        self.columns.get(h as usize).map(Vec::as_slice)
    }

    /// The built-in visit field as a column-shaped slice.
    #[must_use]
    pub fn visits(&self) -> &[f64] {
        &self.fields
    }

    fn col_idx(&self, h: u32) -> Result<usize, FnError> {
        let i = h as usize;
        if i < self.columns.len() {
            Ok(i)
        } else {
            Err(error::arg_validation(format!("unknown column handle {h}")))
        }
    }

    /// `dst[i] = value` for every node. Charges `O(N)`.
    ///
    /// # Errors
    /// Returns [`FnError`] `0x865` on budget exhaustion or `0x86E` on a bad handle.
    pub fn col_fill(&mut self, dst: u32, value: f64) -> Result<(), FnError> {
        let d = self.col_idx(dst)?;
        let n = self.columns[d].len() as u64;
        self.budget
            .try_charge(n)
            .map_err(|e| error::budget_exhausted(e.to_string()))?;
        self.columns[d].fill(value);
        Ok(())
    }

    /// `dst = a <op> b` elementwise, where `a`/`b` are columns and `op` is one of
    /// `"add" | "sub" | "mul" | "div"`. Division guards against zero with `eps`.
    /// Charges `O(N)`.
    ///
    /// # Errors
    /// Returns [`FnError`] `0x865` on budget exhaustion, or `0x86E` on a bad
    /// handle or unknown op.
    pub fn col_ewise(&mut self, dst: u32, a: u32, b: u32, op: &str) -> Result<(), FnError> {
        let (d, ai, bi) = (self.col_idx(dst)?, self.col_idx(a)?, self.col_idx(b)?);
        let n = self.columns[d].len();
        self.budget
            .try_charge(n as u64)
            .map_err(|e| error::budget_exhausted(e.to_string()))?;
        const EPS: f64 = 1e-12;
        for i in 0..n {
            let (x, y) = (self.columns[ai][i], self.columns[bi][i]);
            self.columns[d][i] = match op {
                "add" => x + y,
                "sub" => x - y,
                "mul" => x * y,
                "div" => x / if y.abs() < EPS { EPS } else { y },
                other => {
                    return Err(error::arg_validation(format!("unknown ewise op `{other}`")));
                }
            };
        }
        Ok(())
    }

    /// `dst = op(src) * scale + shift`, where `op` is `"id" | "ln" | "sqrt" |
    /// "recip"`. Charges `O(N)`.
    ///
    /// # Errors
    /// Returns [`FnError`] `0x865` on budget exhaustion, or `0x86E` on a bad
    /// handle or unknown op.
    pub fn col_map(
        &mut self,
        dst: u32,
        src: u32,
        op: &str,
        scale: f64,
        shift: f64,
    ) -> Result<(), FnError> {
        let (d, s) = (self.col_idx(dst)?, self.col_idx(src)?);
        let n = self.columns[d].len();
        self.budget
            .try_charge(n as u64)
            .map_err(|e| error::budget_exhausted(e.to_string()))?;
        const EPS: f64 = 1e-12;
        for i in 0..n {
            let x = self.columns[s][i];
            let y = match op {
                "id" => x,
                "ln" => {
                    if x < EPS {
                        0.0
                    } else {
                        x.ln()
                    }
                }
                "sqrt" => {
                    if x < 0.0 {
                        0.0
                    } else {
                        x.sqrt()
                    }
                }
                "recip" => 1.0 / if x.abs() < EPS { EPS } else { x },
                other => {
                    return Err(error::arg_validation(format!("unknown map op `{other}`")));
                }
            };
            self.columns[d][i] = y * scale + shift;
        }
        Ok(())
    }

    /// Copies the built-in visit field into a guest column. Charges `O(N)`.
    ///
    /// # Errors
    /// Returns [`FnError`] `0x865` on budget exhaustion or `0x86E` on a bad handle.
    pub fn col_load_visits(&mut self, dst: u32) -> Result<(), FnError> {
        let d = self.col_idx(dst)?;
        let n = self.fields.len();
        self.budget
            .try_charge(n as u64)
            .map_err(|e| error::budget_exhausted(e.to_string()))?;
        self.columns[d][..n].copy_from_slice(&self.fields[..n]);
        Ok(())
    }

    /// `dst[child] = src[parent(child)]` — the gather a tree policy needs.
    /// Roots take `src[root]`. Charges `O(N)`.
    ///
    /// # Errors
    /// Returns [`FnError`] `0x865` on budget exhaustion or `0x86E` on a bad handle.
    pub fn col_gather_parent(&mut self, dst: u32, src: u32) -> Result<(), FnError> {
        let (d, s) = (self.col_idx(dst)?, self.col_idx(src)?);
        let n = self.columns[d].len();
        self.budget
            .try_charge(n as u64)
            .map_err(|e| error::budget_exhausted(e.to_string()))?;
        for i in 0..n {
            let p = self.parent[i];
            let from = if p == u32::MAX { i } else { p as usize };
            self.columns[d][i] = self.columns[s][from];
        }
        Ok(())
    }

    /// Adds a directed edge `src → dst`.
    ///
    /// Charges one work unit and the edge's arena bytes.
    ///
    /// # Errors
    /// Returns [`FnError`] on an out-of-range slot (`0x86E`), a drained budget
    /// (`0x865`), or an exceeded arena cap (`0x864`).
    pub fn add_edge(&mut self, src: u32, dst: u32) -> Result<(), FnError> {
        self.charge_op()?;
        let s = self.check_slot(src)?;
        self.check_slot(dst)?;
        self.arena
            .try_alloc(EDGE_BYTES)
            .map_err(|e| error::arena_cap_exceeded(e.to_string()))?;
        self.adjacency[s].push(dst);
        self.edge_fields[s].push(0.0);
        self.rev[s].push(u32::MAX); // unpaired: plain edge, not a flow arc
        Ok(())
    }

    /// Adds a capacitated arc `src -> dst` **and its paired reverse arc**
    /// (capacity 0), linking the two so a residual push can credit the reverse
    /// (issue #152 gap 2).
    ///
    /// `add_edge` is one-directional and unpaired, which is sufficient for
    /// traversal but not for flow: max-flow needs to *return* capacity along the
    /// reverse arc when it pushes.
    ///
    /// # Errors
    /// Returns [`FnError`] `0x865` on budget exhaustion, `0x864` on arena cap, or
    /// `0x86E` for a bad slot or a self-loop (which has no meaningful reverse).
    pub fn add_flow_edge(&mut self, src: u32, dst: u32, cap: f64) -> Result<(), FnError> {
        self.charge_op()?;
        let s = self.check_slot(src)?;
        let d = self.check_slot(dst)?;
        if s == d {
            return Err(error::arg_validation(
                "add_flow_edge: self-loops have no paired reverse arc",
            ));
        }
        self.arena
            .try_alloc(2 * EDGE_BYTES)
            .map_err(|e| error::arena_cap_exceeded(e.to_string()))?;
        #[expect(
            clippy::cast_possible_truncation,
            reason = "degrees stay far below u32::MAX under the arena cap"
        )]
        let (ks, kd) = (
            self.adjacency[s].len() as u32,
            self.adjacency[d].len() as u32,
        );
        self.adjacency[s].push(dst);
        self.edge_fields[s].push(cap);
        self.rev[s].push(kd);
        self.adjacency[d].push(src);
        self.edge_fields[d].push(0.0);
        self.rev[d].push(ks);
        Ok(())
    }

    /// Residual capacity on out-edge `k` of `slot` (host-side inspection).
    #[must_use]
    pub fn edge_field(&self, slot: u32, k: usize) -> Option<f64> {
        self.edge_fields.get(slot as usize)?.get(k).copied()
    }

    /// Returns the out-neighbors of `slot` (a clone, so no borrow escapes).
    ///
    /// # Errors
    /// Returns [`FnError`] on an out-of-range slot or a drained budget.
    pub fn neighbors(&mut self, slot: u32) -> Result<Vec<u32>, FnError> {
        self.charge_op()?;
        let i = self.check_slot(slot)?;
        Ok(self.adjacency[i].clone())
    }

    /// Reads the field of `slot`.
    ///
    /// # Errors
    /// Returns [`FnError`] on an out-of-range slot or a drained budget.
    pub fn get_field(&mut self, slot: u32) -> Result<f64, FnError> {
        self.charge_op()?;
        let i = self.check_slot(slot)?;
        Ok(self.fields[i])
    }

    /// Writes the field of `slot`.
    ///
    /// # Errors
    /// Returns [`FnError`] on an out-of-range slot or a drained budget.
    pub fn set_field(&mut self, slot: u32, value: f64) -> Result<(), FnError> {
        self.charge_op()?;
        let i = self.check_slot(slot)?;
        self.fields[i] = value;
        Ok(())
    }

    /// Draws a reproducible `Bernoulli(prob)` decision for `(iter, slot)`.
    ///
    /// Uses the promoted counter-hash so a seeded run's tie-breaks/rollouts are
    /// bitwise-reproducible (proposal §8; the basis for `Q-4`).
    ///
    /// # Errors
    /// Returns [`FnError`] on a drained budget.
    pub fn sample(&mut self, prob: f64, iter: u64, slot: u32) -> Result<bool, FnError> {
        self.charge_op()?;
        Ok(sample_bernoulli(prob, self.seed, iter, u64::from(slot)))
    }

    /// Returns the number of nodes.
    #[must_use]
    pub fn node_count(&self) -> usize {
        self.adjacency.len()
    }

    /// Returns the total number of edges.
    #[must_use]
    pub fn edge_count(&self) -> usize {
        self.adjacency.iter().map(Vec::len).sum()
    }

    /// Returns the work units charged so far (test introspection).
    #[must_use]
    pub fn work_spent(&self) -> u64 {
        self.budget.spent()
    }
}

/// A JSON request from a compiled Mode B-seq guest to drive a scratch graph.
///
/// The host-side ABI mirrors the Mode-A `KernelRequest` dispatch: a compiled
/// (WASM/Extism) guest emits one of these per random-access op through the
/// `host-graph` import, and the host runs it against the session-local
/// [`ScratchGraph`] under its budget. All fields default so each op names only
/// the ones it needs.
#[derive(Debug, Deserialize)]
#[deprecated(
    since = "3.1.0",
    note = "superseded by the `arena_*` kernels (HandleKind::Arena); this parallel \
            stack was unreachable from every loader, which is issue #152. Removal at the \
            next major — see docs/proposals/guest_stateful_compute_2026-07-20.md §6/§13.4"
)]
pub struct ScratchRequest {
    /// The session id (from [`ScratchRegistry::open`]); `0` for the single-graph
    /// [`ScratchGraph::call_json`] path.
    #[serde(default)]
    pub session: u64,
    /// The op name (`"add_node"`, `"add_edge"`, `"neighbors"`, …).
    pub op: String,
    /// Primary integer operand (a slot / src).
    #[serde(default)]
    pub a: i64,
    /// Secondary integer operand (a dst).
    #[serde(default)]
    pub b: i64,
    /// A scalar operand (a field value / probability).
    #[serde(default)]
    pub f: f64,
    /// An iteration counter (for `sample`).
    #[serde(default)]
    pub iter: u64,
    /// A batch of slot operands (`descend_batch` / `visit_batch`, issue #152).
    #[serde(default)]
    pub v: Vec<u32>,
}

/// A JSON response returned to a Mode B-seq guest.
#[derive(Debug, Serialize, Deserialize, PartialEq)]
#[serde(tag = "t", content = "v")]
#[deprecated(
    since = "3.1.0",
    note = "superseded by the `arena_*` kernels (HandleKind::Arena); this parallel \
            stack was unreachable from every loader, which is issue #152. Removal at the \
            next major — see docs/proposals/guest_stateful_compute_2026-07-20.md §6/§13.4"
)]
pub enum ScratchResponse {
    /// A slot / count result.
    #[serde(rename = "i")]
    Int(u32),
    /// A field-value result.
    #[serde(rename = "f")]
    Float(f64),
    /// A boolean (`sample`) result.
    #[serde(rename = "b")]
    Bool(bool),
    /// A neighbor-slot list.
    #[serde(rename = "l")]
    List(Vec<u32>),
    /// A no-value result (`add_edge`, `set_field`).
    #[serde(rename = "u")]
    Unit,
    /// A typed error `{code, message}`.
    #[serde(rename = "e")]
    Err {
        /// The GraphCompute error code (proposal §12).
        code: u32,
        /// The human-readable message.
        message: String,
    },
}

impl ScratchGraph {
    /// Prototype **batched descent** (issue #152 / the Q-5 follow-up).
    ///
    /// Advances every active rollout one tree level in a *single* host crossing:
    /// for each slot in `active`, selects the least-visited child (ties broken by
    /// the lower slot, so the choice is deterministic); a slot with no children is
    /// terminal and maps to itself.
    ///
    /// This is the coarse-granularity counterpart of the per-op
    /// `neighbors` + `get_field` descent. The native work is **unchanged and
    /// charged in full** (one unit per rollout plus one per child inspected), so
    /// batching amortizes the *crossing*, never the meter — a batched guest gets
    /// no budget discount for the same traversal.
    ///
    /// # Errors
    /// Returns [`FnError`] `0x865` when the work budget is drained, or `0x86E`
    /// when a slot is out of range.
    pub fn descend_batch(&mut self, active: &[u32]) -> Result<Vec<u32>, FnError> {
        let mut units = active.len() as u64;
        for &slot in active {
            units += self.adjacency[self.check_slot(slot)?].len() as u64;
        }
        self.budget
            .try_charge(units)
            .map_err(|e| error::budget_exhausted(e.to_string()))?;

        let mut out = Vec::with_capacity(active.len());
        for &slot in active {
            let i = self.check_slot(slot)?;
            let chosen = self.adjacency[i]
                .iter()
                .copied()
                .min_by(|&x, &y| {
                    self.fields[x as usize]
                        .total_cmp(&self.fields[y as usize])
                        .then(x.cmp(&y))
                })
                .unwrap_or(slot);
            out.push(chosen);
        }
        Ok(out)
    }

    /// Starts `len` rollouts at `root`, returning the active set's handle and
    /// charging the initial visit (issue #152, design P2).
    ///
    /// # Errors
    /// Returns [`FnError`] `0x865` on budget exhaustion or `0x864` on arena cap.
    pub fn batch_new(&mut self, len: usize, root: u32, delta: f64) -> Result<u32, FnError> {
        self.budget
            .try_charge(len as u64)
            .map_err(|e| error::budget_exhausted(e.to_string()))?;
        self.arena
            .try_alloc(len * std::mem::size_of::<u32>())
            .map_err(|e| error::arena_cap_exceeded(e.to_string()))?;
        let i = self.check_slot(root)?;
        #[expect(clippy::cast_precision_loss, reason = "batch widths are small")]
        {
            self.fields[i] += delta * len as f64;
        }
        self.batches.push(vec![root; len]);
        Ok((self.batches.len() - 1) as u32)
    }

    /// Pushes up to `k` augmenting paths from `source` to `sink` in one call,
    /// returning the flow added (issue #152 gap 2).
    ///
    /// One call is one Dinic phase: build the residual level graph by BFS, then
    /// push blocking flow along up to `k` level-respecting paths. Residual
    /// updates land **in-loop**, so paths within a call see each other's pushes —
    /// the same discipline gap 3 showed is required for batched search, here it
    /// is what keeps the flow *correct* rather than merely diverse.
    ///
    /// A guest conducts `while augment_batch(s, t, k)? > 0.0 {}`; crossings are
    /// O(phases × paths/k) rather than O(edges traversed).
    ///
    /// # Errors
    /// Returns [`FnError`] `0x865` when the work budget is drained or `0x86E` for
    /// a bad slot.
    pub fn augment_batch(&mut self, source: u32, sink: u32, k: usize) -> Result<f64, FnError> {
        let s = self.check_slot(source)?;
        let t = self.check_slot(sink)?;
        let n = self.adjacency.len();
        let mut units = n as u64;

        // Residual level graph (BFS over arcs with spare capacity).
        let mut level = vec![u32::MAX; n];
        level[s] = 0;
        let mut queue = std::collections::VecDeque::new();
        queue.push_back(s);
        while let Some(u) = queue.pop_front() {
            units += self.adjacency[u].len() as u64;
            for i in 0..self.adjacency[u].len() {
                let v = self.adjacency[u][i] as usize;
                if self.edge_fields[u][i] > 0.0 && level[v] == u32::MAX {
                    level[v] = level[u] + 1;
                    queue.push_back(v);
                }
            }
        }
        if level[t] == u32::MAX {
            self.budget
                .try_charge(units)
                .map_err(|e| error::budget_exhausted(e.to_string()))?;
            return Ok(0.0); // no augmenting path remains
        }

        // Blocking flow: up to `k` paths, current-arc optimized.
        let mut iter = vec![0usize; n];
        let mut total = 0.0;
        for _ in 0..k {
            let pushed = self.dfs_push(s, t, f64::INFINITY, &level, &mut iter, &mut units);
            if pushed <= 0.0 {
                break;
            }
            total += pushed;
        }

        self.budget
            .try_charge(units)
            .map_err(|e| error::budget_exhausted(e.to_string()))?;
        Ok(total)
    }

    /// One level-respecting augmenting path; credits the paired reverse arc.
    fn dfs_push(
        &mut self,
        u: usize,
        sink: usize,
        limit: f64,
        level: &[u32],
        iter: &mut [usize],
        units: &mut u64,
    ) -> f64 {
        if u == sink {
            return limit;
        }
        while iter[u] < self.adjacency[u].len() {
            let i = iter[u];
            *units += 1;
            let v = self.adjacency[u][i] as usize;
            let cap = self.edge_fields[u][i];
            if cap > 0.0 && level[v] == level[u] + 1 {
                let pushed = self.dfs_push(v, sink, limit.min(cap), level, iter, units);
                if pushed > 0.0 {
                    self.edge_fields[u][i] -= pushed;
                    let r = self.rev[u][i];
                    if r != u32::MAX {
                        self.edge_fields[v][r as usize] += pushed;
                    }
                    return pushed;
                }
            }
            iter[u] += 1;
        }
        0.0
    }

    /// The shared advance step: one tree level for every rollout in `active`.
    ///
    /// Factored so the handle and array entry points run *byte-identical* work —
    /// the only difference between them is whether the batch crosses the guest
    /// boundary, which is exactly what the §10 gap-1 experiment isolates.
    fn advance_core(
        &mut self,
        active: &mut [u32],
        delta: f64,
        policy: DescentPolicy,
    ) -> Result<(), FnError> {
        let mut units = active.len() as u64;
        for &slot in active.iter() {
            units += self.adjacency[self.check_slot(slot)?].len() as u64;
        }
        if let Some(c) = policy.score_col {
            self.col_idx(c)?;
        }
        self.budget
            .try_charge(units)
            .map_err(|e| error::budget_exhausted(e.to_string()))?;

        // Stale: every rollout chooses against this frozen snapshot. Otherwise
        // each visit lands immediately, so later rollouts in the same batch see
        // it — the virtual-loss idiom, free because the loop is host-side.
        let snapshot = if policy.stale {
            Some(match policy.score_col {
                Some(c) => self.columns[c as usize].clone(),
                None => self.fields.clone(),
            })
        } else {
            None
        };
        for slot in active.iter_mut() {
            let i = self.check_slot(*slot)?;
            let read: &[f64] = match (&snapshot, policy.score_col) {
                (Some(s), _) => s,
                (None, Some(c)) => &self.columns[c as usize],
                (None, None) => &self.fields,
            };
            let cmp = |x: &u32, y: &u32| {
                let (a, b) = (read[*x as usize], read[*y as usize]);
                let ord = a.total_cmp(&b);
                if policy.maximize { ord.reverse() } else { ord }.then(x.cmp(y))
            };
            let chosen = self.adjacency[i]
                .iter()
                .copied()
                .min_by(|x, y| cmp(x, y))
                .unwrap_or(*slot);
            if chosen != *slot {
                // The visit always lands on the built-in field.
                self.fields[chosen as usize] += delta;
                // With a *guest-owned* score column the host cannot recompute the
                // guest's formula per rollout, so the visit above does not make
                // later rollouts in this batch diverge. An explicit virtual loss
                // on the score restores divergence while leaving the guest's
                // formula authoritative at recompute time (issue #152 gap 6).
                if let Some(c) = policy.score_col
                    && policy.vloss != 0.0
                {
                    let col = &mut self.columns[c as usize];
                    if policy.maximize {
                        col[chosen as usize] -= policy.vloss;
                    } else {
                        col[chosen as usize] += policy.vloss;
                    }
                }
            }
            *slot = chosen;
        }
        Ok(())
    }

    /// Reads a host-resident active set (tests / host-side inspection only —
    /// never a guest-facing op, since that would move batch data).
    #[must_use]
    pub fn batch_slots(&self, h: u32) -> Option<&[u32]> {
        self.batches.get(h as usize).map(Vec::as_slice)
    }

    /// Advances every rollout in the handle-addressed active set one tree level.
    ///
    /// The whole search step crosses as a single `u32` handle. For each active
    /// slot: pick the least-visited child (ties by lower slot), charge the visit,
    /// and move there; a terminal slot stays put.
    ///
    /// `stale` selects the batching semantics, which is the question §10 gap 3
    /// asks:
    /// - `stale = true` — every rollout in the batch chooses against the *same*
    ///   snapshot of the visit counts (naive lock-step batching).
    /// - `stale = false` — the visit is applied as each rollout is processed, so
    ///   later rollouts in the same batch see earlier ones' updates. This is the
    ///   *virtual-loss* idiom, and it is free here because the loop is host-side.
    ///
    /// Work is charged per rollout plus per child inspected (design P5), so
    /// batching amortizes the crossing and never the meter.
    ///
    /// # Errors
    /// Returns [`FnError`] `0x865` on budget exhaustion, or `0x86E` for a bad
    /// handle or slot.
    pub fn advance_batch(&mut self, h: u32, delta: f64, stale: bool) -> Result<(), FnError> {
        if h as usize >= self.batches.len() {
            return Err(error::arg_validation(format!("unknown batch handle {h}")));
        }
        // Take the batch out so `advance_core` can hold `&mut self` without a
        // clone; put it back on every path.
        let mut active = std::mem::take(&mut self.batches[h as usize]);
        let res = self.advance_core(
            &mut active,
            delta,
            DescentPolicy {
                stale,
                ..DescentPolicy::default()
            },
        );
        self.batches[h as usize] = active;
        res
    }

    /// Advances a batch using a **guest-authored** score column (issue #152 gap 6).
    ///
    /// Identical to [`Self::advance_batch`] except the child is chosen by the
    /// guest's `policy.score_col` rather than the host's built-in visit rule, so
    /// the search rule belongs to the plugin author. The visit still lands on the
    /// built-in field; `policy.vloss` keeps rollouts within a batch diverging.
    ///
    /// # Errors
    /// Returns [`FnError`] `0x865` on budget exhaustion or `0x86E` for a bad
    /// batch/column handle.
    pub fn advance_batch_scored(
        &mut self,
        h: u32,
        delta: f64,
        policy: DescentPolicy,
    ) -> Result<(), FnError> {
        if h as usize >= self.batches.len() {
            return Err(error::arg_validation(format!("unknown batch handle {h}")));
        }
        let mut active = std::mem::take(&mut self.batches[h as usize]);
        let res = self.advance_core(&mut active, delta, policy);
        self.batches[h as usize] = active;
        res
    }

    /// The same advance step, but the batch crosses in and out as data.
    ///
    /// Exists purely so the handle path can be compared against an otherwise
    /// identical array path (§10 gap 1); production kernels should prefer
    /// [`Self::advance_batch`].
    ///
    /// # Errors
    /// Returns [`FnError`] `0x865` on budget exhaustion or `0x86E` on a bad slot.
    pub fn advance_array(
        &mut self,
        active: &[u32],
        delta: f64,
        stale: bool,
    ) -> Result<Vec<u32>, FnError> {
        let mut v = active.to_vec();
        self.advance_core(
            &mut v,
            delta,
            DescentPolicy {
                stale,
                ..DescentPolicy::default()
            },
        )?;
        Ok(v)
    }

    /// Number of *distinct* slots in an active set — the diversity metric behind
    /// the staleness analysis (§10 gap 3).
    #[must_use]
    pub fn batch_distinct(&self, h: u32) -> usize {
        self.batches.get(h as usize).map_or(0, |b| {
            let mut v = b.clone();
            v.sort_unstable();
            v.dedup();
            v.len()
        })
    }

    /// Prototype **batched visit** (issue #152): adds `delta` to every listed
    /// slot's field in one crossing — the backprop counterpart of
    /// [`Self::descend_batch`]. Charges one unit per slot.
    ///
    /// # Errors
    /// Returns [`FnError`] `0x865` when the work budget is drained, or `0x86E`
    /// when a slot is out of range.
    pub fn visit_batch(&mut self, nodes: &[u32], delta: f64) -> Result<(), FnError> {
        self.budget
            .try_charge(nodes.len() as u64)
            .map_err(|e| error::budget_exhausted(e.to_string()))?;
        for &slot in nodes {
            let i = self.check_slot(slot)?;
            self.fields[i] += delta;
        }
        Ok(())
    }

    /// Dispatches one guest [`ScratchRequest`] against this scratch graph.
    ///
    /// This is the single host entry point a compiled Mode B-seq guest drives; it
    /// maps each op to the corresponding metered accessor so budget/arena
    /// exhaustion and bad slots surface as typed [`ScratchResponse::Err`] rather
    /// than trapping the guest. An unknown op is a `0x86E` arg-validation error.
    pub fn dispatch(&mut self, req: &ScratchRequest) -> ScratchResponse {
        #[expect(
            clippy::cast_sign_loss,
            reason = "guest slot operands are non-negative"
        )]
        let (a, b) = (req.a as u32, req.b as u32);
        let result: Result<ScratchResponse, FnError> = match req.op.as_str() {
            "add_node" => self.add_node(req.f).map(ScratchResponse::Int),
            "add_edge" => self.add_edge(a, b).map(|()| ScratchResponse::Unit),
            "neighbors" => self.neighbors(a).map(ScratchResponse::List),
            "get_field" => self.get_field(a).map(ScratchResponse::Float),
            "set_field" => self.set_field(a, req.f).map(|()| ScratchResponse::Unit),
            "sample" => self.sample(req.f, req.iter, a).map(ScratchResponse::Bool),
            "descend_batch" => self.descend_batch(&req.v).map(ScratchResponse::List),
            // Handle-addressed batch ops: only a u32 handle crosses (design P2).
            "batch_new" => self
                .batch_new(a as usize, b, req.f)
                .map(ScratchResponse::Int),
            "advance_batch" => self
                .advance_batch(a, req.f, req.iter != 0)
                .map(|()| ScratchResponse::Unit),
            "visit_batch" => self
                .visit_batch(&req.v, req.f)
                .map(|()| ScratchResponse::Unit),
            "node_count" => Ok(ScratchResponse::Int(
                u32::try_from(self.node_count()).unwrap_or(u32::MAX),
            )),
            "edge_count" => Ok(ScratchResponse::Int(
                u32::try_from(self.edge_count()).unwrap_or(u32::MAX),
            )),
            other => Err(error::arg_validation(format!(
                "unknown scratch op `{other}`"
            ))),
        };
        match result {
            Ok(r) => r,
            Err(e) => ScratchResponse::Err {
                code: e.code,
                message: e.message,
            },
        }
    }

    /// Runs a guest JSON request string, returning a JSON response string.
    ///
    /// The exact wire form the WASM/Extism `host-graph` import carries: parse →
    /// [`dispatch`](Self::dispatch) → serialize. A malformed request is a typed
    /// `0x86E` error response, never a host panic.
    ///
    /// # Errors
    /// Returns a `serde_json` error only if the response fails to serialize (it
    /// never does for these value shapes).
    pub fn call_json(&mut self, request: &str) -> Result<String, serde_json::Error> {
        let resp = match serde_json::from_str::<ScratchRequest>(request) {
            Ok(req) => self.dispatch(&req),
            Err(e) => ScratchResponse::Err {
                code: error::ARG_VALIDATION,
                message: format!("bad scratch request json: {e}"),
            },
        };
        serde_json::to_string(&resp)
    }
}

/// A multi-session registry backing the `host-graph` import for Mode B-seq — the
/// host side a compiled WASM/Extism guest drives.
///
/// Mirrors [`GraphComputeRegistry`](super::dispatch::GraphComputeRegistry): the
/// host [`open`](Self::open)s a session-local [`ScratchGraph`] (under the CALL's
/// budget/arena) and hands the guest an **unguessable** session id; the guest
/// drives ops via [`call_json`](Self::call_json) supplying that id; the host
/// [`close`](Self::close)s to read results and drop the graph. Each session sits
/// behind its own `Mutex` so one guest's pointer-chasing never stalls another
/// concurrent CALL, and a kernel panic is isolated to a typed error rather than a
/// worker crash (proposal §5.4 / §7b).
#[derive(Debug, Default)]
#[deprecated(
    since = "3.1.0",
    note = "superseded by the `arena_*` kernels (HandleKind::Arena); this parallel \
            stack was unreachable from every loader, which is issue #152. Removal at the \
            next major — see docs/proposals/guest_stateful_compute_2026-07-20.md §6/§13.4"
)]
pub struct ScratchRegistry {
    sessions: parking_lot::Mutex<
        std::collections::HashMap<u64, std::sync::Arc<parking_lot::Mutex<ScratchGraph>>>,
    >,
}

impl ScratchRegistry {
    /// Creates an empty registry.
    #[must_use]
    pub fn new() -> Self {
        Self {
            sessions: parking_lot::Mutex::new(std::collections::HashMap::new()),
        }
    }

    /// Registers `graph`, returning its unguessable opaque session id.
    ///
    /// The id is a CSPRNG-drawn non-zero `u64` (UUIDv4), so a concurrent CALL
    /// cannot enumerate and target another session's scratch graph.
    pub fn open(&self, graph: ScratchGraph) -> u64 {
        let mut sessions = self.sessions.lock();
        loop {
            let id = uuid::Uuid::new_v4().as_u64_pair().0;
            if id != 0 && !sessions.contains_key(&id) {
                sessions.insert(id, std::sync::Arc::new(parking_lot::Mutex::new(graph)));
                return id;
            }
        }
    }

    /// Runs `f` against the addressed session's scratch graph.
    ///
    /// This is the **typed** host-call path (issue #152): a typed `host-graph`
    /// function resolves its session once and invokes the kernel directly,
    /// skipping the JSON encode → parse → dispatch → serialize → parse round trip
    /// [`Self::call_json`] pays. Returns `None` when the session is unknown, so
    /// the caller can map that to its own typed error.
    ///
    /// Locking mirrors `call_json`: the session map is released before the
    /// per-graph mutex is taken, so one guest's pointer-chasing never stalls
    /// another concurrent CALL.
    pub fn with_session<R>(&self, id: u64, f: impl FnOnce(&mut ScratchGraph) -> R) -> Option<R> {
        let arc = {
            let sessions = self.sessions.lock();
            std::sync::Arc::clone(sessions.get(&id)?)
        };
        let mut graph = arc.lock();
        Some(f(&mut graph))
    }

    /// Removes and returns the scratch graph for `id`, if present.
    pub fn close(&self, id: u64) -> Option<ScratchGraph> {
        let arc = self.sessions.lock().remove(&id)?;
        std::sync::Arc::try_unwrap(arc)
            .ok()
            .map(parking_lot::Mutex::into_inner)
    }

    /// Dispatches one guest JSON request against the addressed session.
    ///
    /// A missing session, a malformed request, or a kernel panic is returned as a
    /// typed error response, never a worker crash (proposal §5.4).
    #[must_use]
    pub fn call_json(&self, request_json: &str) -> String {
        let req = match serde_json::from_str::<ScratchRequest>(request_json) {
            Ok(r) => r,
            Err(e) => {
                // #233 P3: `unwrap_or_default()` handed the guest the EMPTY
                // STRING, which is not a parseable response at all. The
                // sibling ABI in `dispatch.rs` already answers in-band; match it.
                return serde_json::to_string(&ScratchResponse::Err {
                    code: error::ARG_VALIDATION,
                    message: format!("bad scratch request json: {e}"),
                })
                .unwrap_or_else(|e| {
                    format!("{{\"t\":\"e\",\"v\":{{\"code\":2,\"message\":\"encode: {e}\"}}}}")
                });
            }
        };
        let session = {
            let sessions = self.sessions.lock();
            match sessions.get(&req.session) {
                Some(arc) => std::sync::Arc::clone(arc),
                None => {
                    return serde_json::to_string(&ScratchResponse::Err {
                        code: error::EPOCH_MISMATCH,
                        message: format!("unknown or closed scratch session {}", req.session),
                    })
                    .unwrap_or_else(|e| {
                        format!("{{\"t\":\"e\",\"v\":{{\"code\":2,\"message\":\"encode: {e}\"}}}}")
                    });
                }
            }
        };
        let mut guard = session.lock();
        let resp = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| guard.dispatch(&req)))
            .unwrap_or_else(|_| ScratchResponse::Err {
                code: 0x86D,
                message: "scratch: op panicked (isolated)".to_owned(),
            });
        serde_json::to_string(&resp).unwrap_or_else(|e| {
            format!("{{\"t\":\"e\",\"v\":{{\"code\":2,\"message\":\"encode: {e}\"}}}}")
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn graph(budget: u64, arena_bytes: usize) -> ScratchGraph {
        ScratchGraph::new(
            WorkBudget::new(budget),
            Arena::new(arena_bytes, 1 << 20),
            0xABCD,
        )
    }

    /// A complete binary tree: root 0 → {1,2}; 1 → {3,4}; 2 → {5,6}.
    /// Construction charges 13 work units (7 `add_node` + 6 `add_edge`).
    fn tree(budget: u64) -> ScratchGraph {
        let mut g = graph(budget, 1 << 20);
        for _ in 0..7 {
            g.add_node(0.0).unwrap();
        }
        for (p, c) in [(0, 1), (0, 2), (1, 3), (1, 4), (2, 5), (2, 6)] {
            g.add_edge(p, c).unwrap();
        }
        g
    }

    #[test]
    fn batching_gives_no_budget_discount() {
        // Design P5 (issue #152): batching amortizes the *crossing*, never the
        // meter. One advance over B rollouts must charge exactly what B
        // single-rollout advances charge — otherwise a guest evades the §5.1
        // work budget simply by batching, and the whole governance model is
        // cheatable.
        const B: usize = 32;

        let mut batched = tree(10_000);
        let hb = batched.batch_new(B, 0, 1.0).unwrap();
        let before = batched.work_spent();
        batched.advance_batch(hb, 1.0, false).unwrap();
        let batched_cost = batched.work_spent() - before;

        let mut singles = tree(10_000);
        let mut singles_cost = 0;
        for _ in 0..B {
            let h = singles.batch_new(1, 0, 1.0).unwrap();
            let before = singles.work_spent();
            singles.advance_batch(h, 1.0, false).unwrap();
            singles_cost += singles.work_spent() - before;
        }

        assert_eq!(
            batched_cost, singles_cost,
            "a batched advance must charge the same native work as the \
             equivalent single-rollout advances"
        );
    }

    #[test]
    fn budget_exhaustion_mid_batch_fails_closed() {
        // Build costs 13; batch_new(8) costs 8 (=21); the advance needs
        // 8 + 8*2 = 24 more, leaving only 4 — so it must fail closed at 0x865
        // and leave the active set untouched (the `mem::take` restores it).
        let mut g = tree(25);
        let h = g.batch_new(8, 0, 1.0).unwrap();
        let before: Vec<u32> = g.batch_slots(h).unwrap().to_vec();

        let err = g.advance_batch(h, 1.0, false).unwrap_err();
        assert_eq!(err.code, error::BUDGET_EXHAUSTED, "expected 0x865");
        assert_eq!(
            g.batch_slots(h).unwrap(),
            before.as_slice(),
            "a failed advance must not partially mutate the active set"
        );
    }

    #[test]
    fn arena_cap_rejects_an_oversized_batch() {
        // A 64-byte arena cannot hold a 4096-slot active set.
        let mut g = graph(1_000_000, 64);
        g.add_node(0.0).unwrap();
        let err = g.batch_new(4096, 0, 1.0).unwrap_err();
        assert_eq!(err.code, error::ARENA_CAP_EXCEEDED, "expected 0x864");
    }

    #[test]
    fn unknown_batch_handle_is_arg_validation() {
        let mut g = tree(10_000);
        let err = g.advance_batch(999, 1.0, false).unwrap_err();
        assert_eq!(err.code, error::ARG_VALIDATION, "expected 0x86E");
    }

    #[test]
    fn adversarial_growth_halts_at_the_arena_cap() {
        // An unbounded guest growth loop must terminate against the byte cap
        // rather than exhausting host memory.
        let mut g = graph(u64::MAX / 4, 4096);
        let mut added = 0u32;
        let err = loop {
            match g.add_node(0.0) {
                Ok(_) => {
                    added += 1;
                    assert!(added < 1_000_000, "growth did not halt");
                }
                Err(e) => break e,
            }
        };
        assert_eq!(err.code, error::ARENA_CAP_EXCEEDED, "expected 0x864");
        assert!(added > 0, "some growth should succeed before the cap");
    }

    #[test]
    fn q1_every_random_access_op_charges_the_budget() {
        // Q-1: each op charges one work unit; a runaway loop halts at 0x865.
        let mut g = graph(10, 1 << 20);
        let a = g.add_node(1.0).unwrap(); // 1
        let b = g.add_node(2.0).unwrap(); // 2
        g.add_edge(a, b).unwrap(); // 3
        assert_eq!(g.work_spent(), 3);
        // Burn the rest of the budget with reads, then the next op fails closed.
        let mut hit = false;
        for _ in 0..100 {
            if let Err(e) = g.get_field(a) {
                hit = e.code == error::BUDGET_EXHAUSTED;
                break;
            }
        }
        assert!(hit, "a runaway op loop must halt at 0x865");
    }

    #[test]
    fn q2_scratch_growth_charges_the_arena_cap() {
        // Q-2: node/edge growth charges the arena; exceeding it is a typed 0x864
        // at the allocating op, not a panic.
        // Room for exactly 2 nodes (2*NODE_BYTES), generous budget.
        let mut g = graph(1_000, NODE_BYTES * 2);
        g.add_node(0.0).unwrap();
        g.add_node(0.0).unwrap();
        let err = g
            .add_node(0.0)
            .expect_err("a third node must breach the 2-node arena cap");
        assert_eq!(err.code, error::ARENA_CAP_EXCEEDED);
        // The failed op did not grow the graph.
        assert_eq!(g.node_count(), 2);
    }

    #[test]
    fn q4_seeded_sampling_is_reproducible() {
        // Q-4 basis: an identical seeded op sequence yields identical sample
        // decisions, so a seeded MCTS-style rollout is bitwise-reproducible.
        let run = || {
            let mut g = graph(10_000, 1 << 20);
            for i in 0..50u32 {
                g.add_node(0.0).unwrap();
                let _ = i;
            }
            (0..50)
                .map(|slot| g.sample(0.5, 7, slot).unwrap())
                .collect::<Vec<_>>()
        };
        assert_eq!(run(), run(), "seeded sampling must be reproducible");
    }

    #[test]
    fn out_of_range_slot_is_a_typed_error_not_a_panic() {
        let mut g = graph(100, 1 << 20);
        let a = g.add_node(0.0).unwrap();
        let err = g.get_field(a + 5).expect_err("out-of-range slot errors");
        assert_eq!(err.code, error::ARG_VALIDATION);
        // add_edge to a missing endpoint also errors, without mutating.
        let err = g.add_edge(a, a + 9).expect_err("bad endpoint errors");
        assert_eq!(err.code, error::ARG_VALIDATION);
        assert_eq!(g.edge_count(), 0);
    }

    #[test]
    fn at_mcts_lite_seeded_rollouts_are_reproducible() {
        // AT-MCTS (lite): a sequential adaptive program on the scratch graph — N
        // seeded rollouts from the root, each descending by sample-driven child
        // selection and bumping a visit-count field. Two identical seeded runs
        // produce identical visit counts, so a Mode B-seq search is bitwise-
        // reproducible (proposal §7b / Q-4 at scenario scale).
        let rollouts = |seed: u64| -> Vec<f64> {
            let mut g =
                ScratchGraph::new(WorkBudget::new(100_000), Arena::new(1 << 20, 1 << 20), seed);
            // A small binary game tree: root 0 → {1,2}, 1 → {3,4}, 2 → {5,6}.
            for _ in 0..7 {
                g.add_node(0.0).unwrap();
            }
            for (p, c) in [(0, 1), (0, 2), (1, 3), (1, 4), (2, 5), (2, 6)] {
                g.add_edge(p, c).unwrap();
            }
            for r in 0..64u64 {
                let mut node = 0u32;
                loop {
                    // Visit: bump the node's count field.
                    let v = g.get_field(node).unwrap();
                    g.set_field(node, v + 1.0).unwrap();
                    let kids = g.neighbors(node).unwrap();
                    if kids.is_empty() {
                        break;
                    }
                    // Seed-driven descent: pick the first child whose sample fires,
                    // else the last (a deterministic function of (seed, r, child)).
                    let mut chosen = *kids.last().unwrap();
                    for &c in &kids {
                        if g.sample(0.5, r, c).unwrap() {
                            chosen = c;
                            break;
                        }
                    }
                    node = chosen;
                }
            }
            (0..g.node_count() as u32)
                .map(|slot| g.get_field(slot).unwrap())
                .collect()
        };
        let a = rollouts(0xC0FFEE);
        assert_eq!(
            a,
            rollouts(0xC0FFEE),
            "seeded MCTS-lite must be reproducible"
        );
        // The root is visited every rollout; totals are consistent.
        assert_eq!(a[0], 64.0, "root visited once per rollout");
        // Different seed generally explores differently (not a hard requirement,
        // but confirms the seed actually drives selection).
        assert_ne!(a, rollouts(0x1234_5678), "the seed must drive descent");
    }

    #[test]
    fn q6_registration_rejects_an_interpreted_body() {
        // Q-6: the compiled-only contract — an interpreted (Rhai row-mode) B-seq
        // body is rejected at registration with a typed capability error (0x86C),
        // while a compiled (WASM/Rust) body is admitted.
        assert!(require_compiled_body(LoaderClass::Compiled).is_ok());
        let err = require_compiled_body(LoaderClass::Interpreted)
            .expect_err("an interpreted B-seq body must be rejected");
        assert_eq!(err.code, error::CAPABILITY_DENIED);
    }

    #[test]
    fn q3_scratch_graphs_are_isolated_by_construction() {
        // Q-3 (structural): the scratch graph is a self-contained, session-local
        // value — it holds no store/session handle, so it is structurally
        // invisible to the store, and two scratch graphs never interfere. (The
        // full concurrent-reader-sees-nothing Q-3 assertion needs store
        // integration, which the host-resident core deliberately does not wire.)
        let mut a = graph(1_000, 1 << 20);
        let mut b = graph(1_000, 1 << 20);
        let a0 = a.add_node(1.0).unwrap();
        let _b0 = b.add_node(99.0).unwrap();
        a.set_field(a0, 7.0).unwrap();
        // Mutating `a` leaves `b` untouched: no shared/observable state.
        assert_eq!(a.node_count(), 1);
        assert_eq!(b.node_count(), 1);
        assert_eq!(a.get_field(a0).unwrap(), 7.0);
        assert_eq!(b.get_field(_b0).unwrap(), 99.0);
    }

    #[test]
    fn guest_json_dispatch_drives_the_scratch_graph() {
        // The compiled-guest ABI: a WASM/Extism Mode B-seq body drives the
        // scratch graph through JSON `host-graph` calls. Simulate a guest here.
        let mut g = graph(10_000, 1 << 20);
        let call = |g: &mut ScratchGraph, s: &str| -> ScratchResponse {
            serde_json::from_str(&g.call_json(s).unwrap()).unwrap()
        };
        assert_eq!(
            call(&mut g, r#"{"op":"add_node","f":1.5}"#),
            ScratchResponse::Int(0)
        );
        assert_eq!(
            call(&mut g, r#"{"op":"add_node","f":2.5}"#),
            ScratchResponse::Int(1)
        );
        assert_eq!(
            call(&mut g, r#"{"op":"add_edge","a":0,"b":1}"#),
            ScratchResponse::Unit
        );
        assert_eq!(
            call(&mut g, r#"{"op":"neighbors","a":0}"#),
            ScratchResponse::List(vec![1])
        );
        assert_eq!(
            call(&mut g, r#"{"op":"get_field","a":1}"#),
            ScratchResponse::Float(2.5)
        );
        // A bad slot dispatches to a typed error response, not a panic.
        match call(&mut g, r#"{"op":"get_field","a":99}"#) {
            ScratchResponse::Err { code, .. } => assert_eq!(code, error::ARG_VALIDATION),
            other => panic!("expected an error response, got {other:?}"),
        }
        // Malformed JSON is a typed error too.
        match call(&mut g, r#"{"op":42}"#) {
            ScratchResponse::Err { code, .. } => assert_eq!(code, error::ARG_VALIDATION),
            other => panic!("expected an error response, got {other:?}"),
        }
        // A drained budget surfaces as an error through the JSON boundary.
        let mut tiny = graph(2, 1 << 20);
        let _ = tiny.call_json(r#"{"op":"add_node","f":0.0}"#).unwrap();
        let _ = tiny.call_json(r#"{"op":"add_node","f":0.0}"#).unwrap();
        match call(&mut tiny, r#"{"op":"add_node","f":0.0}"#) {
            ScratchResponse::Err { code, .. } => assert_eq!(code, error::BUDGET_EXHAUSTED),
            other => panic!("expected a budget error, got {other:?}"),
        }
    }

    #[test]
    fn scratch_registry_manages_isolated_guest_sessions() {
        // The host-side multi-session ABI a WASM/Extism Mode B-seq guest drives:
        // the host opens a session, the guest drives ops by id, the host closes
        // to read results. Two sessions are isolated; an unknown id is a typed
        // error, not a panic.
        let reg = ScratchRegistry::new();
        let sid_a = reg.open(graph(10_000, 1 << 20));
        let sid_b = reg.open(graph(10_000, 1 << 20));
        assert_ne!(sid_a, sid_b);
        assert_ne!(sid_a, 0);

        let call = |s: u64, op: &str, extra: &str| -> ScratchResponse {
            let req = format!(r#"{{"session":{s},"op":"{op}"{extra}}}"#);
            serde_json::from_str(&reg.call_json(&req)).unwrap()
        };
        // Build a node in each session; ids are per-session (both start at 0).
        assert_eq!(
            call(sid_a, "add_node", r#","f":1.0"#),
            ScratchResponse::Int(0)
        );
        assert_eq!(
            call(sid_b, "add_node", r#","f":2.0"#),
            ScratchResponse::Int(0)
        );
        // Session A's field is independent of B's.
        assert_eq!(
            call(sid_a, "get_field", r#","a":0"#),
            ScratchResponse::Float(1.0)
        );
        assert_eq!(
            call(sid_b, "get_field", r#","a":0"#),
            ScratchResponse::Float(2.0)
        );

        // Unknown session → typed error, not a panic.
        match call(0xDEAD, "node_count", "") {
            ScratchResponse::Err { code, .. } => assert_eq!(code, error::EPOCH_MISMATCH),
            other => panic!("expected unknown-session error, got {other:?}"),
        }

        // Close reads the graph back out; the session id is then gone.
        let a = reg.close(sid_a).expect("session A closes");
        assert_eq!(a.node_count(), 1);
        match call(sid_a, "node_count", "") {
            ScratchResponse::Err { code, .. } => assert_eq!(code, error::EPOCH_MISMATCH),
            other => panic!("expected closed-session error, got {other:?}"),
        }
    }

    #[test]
    fn at_mcts_full_program_authored_against_the_guest_abi() {
        // AT-MCTS (guest-ABI): a complete Mode B-seq program — build a game tree,
        // run N seeded rollouts bumping visit counts, derive the principal
        // variation — authored *purely* against the `ScratchRegistry` JSON ABI
        // (the exact surface a compiled WASM/Extism guest drives): the host opens
        // a session, the "guest" issues `call_json` ops by id, the host closes and
        // reads the result. Asserts reproducibility and a pinned principal
        // variation, proving the ABI is complete for a real sequential program.
        //
        // Principal variation = from the root, repeatedly descend to the
        // most-visited child, until a leaf.
        let run = |seed: u64| -> Vec<u32> {
            let reg = ScratchRegistry::new();
            let sid = reg.open(ScratchGraph::new(
                WorkBudget::new(1_000_000),
                Arena::new(1 << 20, 1 << 20),
                seed,
            ));
            // A guest helper: one JSON op through the registry, parsed back.
            let op = |req: String| -> ScratchResponse {
                serde_json::from_str(&reg.call_json(&req)).unwrap()
            };
            let node = |resp: ScratchResponse| -> u32 {
                match resp {
                    ScratchResponse::Int(i) => i,
                    other => panic!("expected int, got {other:?}"),
                }
            };
            // Build a small binary game tree (root 0 → {1,2}; 1 → {3,4}; 2 → {5,6}).
            // `add_node` returns the new node's dense slot id, so ids arrive 0..7.
            for expected in 0..7u32 {
                let got = node(op(format!(
                    r#"{{"session":{sid},"op":"add_node","f":0.0}}"#
                )));
                assert_eq!(got, expected, "add_node returns dense slot ids in order");
            }
            for (p, c) in [(0, 1), (0, 2), (1, 3), (1, 4), (2, 5), (2, 6)] {
                op(format!(
                    r#"{{"session":{sid},"op":"add_edge","a":{p},"b":{c}}}"#
                ));
            }
            // Seeded rollouts (all via the JSON ABI).
            for r in 0..128u64 {
                let mut cur = 0u32;
                loop {
                    let v = match op(format!(r#"{{"session":{sid},"op":"get_field","a":{cur}}}"#)) {
                        ScratchResponse::Float(f) => f,
                        other => panic!("field: {other:?}"),
                    };
                    op(format!(
                        r#"{{"session":{sid},"op":"set_field","a":{cur},"f":{}}}"#,
                        v + 1.0
                    ));
                    let kids =
                        match op(format!(r#"{{"session":{sid},"op":"neighbors","a":{cur}}}"#)) {
                            ScratchResponse::List(l) => l,
                            other => panic!("neighbors: {other:?}"),
                        };
                    if kids.is_empty() {
                        break;
                    }
                    // Seed-driven descent: first child whose sample fires, else last.
                    let mut chosen = *kids.last().unwrap();
                    for &c in &kids {
                        let fired = matches!(
                            op(format!(
                                r#"{{"session":{sid},"op":"sample","a":{c},"f":0.5,"iter":{r}}}"#
                            )),
                            ScratchResponse::Bool(true)
                        );
                        if fired {
                            chosen = c;
                            break;
                        }
                    }
                    cur = chosen;
                }
            }
            // Host reads results back and derives the principal variation.
            let g = reg.close(sid).expect("session closes");
            let mut pv = vec![0u32];
            let mut cur = 0u32;
            loop {
                // Read neighbors directly off the closed graph (host-side).
                let kids = g_neighbors(&g, cur);
                if kids.is_empty() {
                    break;
                }
                let best = kids
                    .iter()
                    .copied()
                    .max_by(|&a, &b| {
                        g_field(&g, a)
                            .partial_cmp(&g_field(&g, b))
                            .unwrap()
                            .then(b.cmp(&a))
                    })
                    .unwrap();
                pv.push(best);
                cur = best;
            }
            pv
        };
        let pv = run(0xC0FFEE);
        assert_eq!(
            pv,
            run(0xC0FFEE),
            "the MCTS principal variation must be reproducible"
        );
        assert_eq!(pv[0], 0, "the PV starts at the root");
        assert_eq!(pv.len(), 3, "the PV descends to a depth-2 leaf");
        // The chosen leaf is one of the tree's leaves {3,4,5,6}.
        assert!(
            (3..=6).contains(pv.last().unwrap()),
            "PV ends at a real leaf"
        );
    }

    /// Host-side read of a closed scratch graph's neighbors (test introspection).
    fn g_neighbors(g: &ScratchGraph, slot: u32) -> Vec<u32> {
        g.adjacency.get(slot as usize).cloned().unwrap_or_default()
    }
    /// Host-side read of a closed scratch graph's field.
    fn g_field(g: &ScratchGraph, slot: u32) -> f64 {
        g.fields.get(slot as usize).copied().unwrap_or(0.0)
    }

    #[test]
    fn neighbors_and_fields_roundtrip() {
        let mut g = graph(1_000, 1 << 20);
        let a = g.add_node(1.5).unwrap();
        let b = g.add_node(2.5).unwrap();
        let c = g.add_node(3.5).unwrap();
        g.add_edge(a, b).unwrap();
        g.add_edge(a, c).unwrap();
        assert_eq!(g.neighbors(a).unwrap(), vec![b, c]);
        g.set_field(b, 9.0).unwrap();
        assert_eq!(g.get_field(b).unwrap(), 9.0);
        assert_eq!(g.edge_count(), 2);
    }
}
