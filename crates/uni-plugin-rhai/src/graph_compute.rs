// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! GraphCompute kernel surface for Rhai guest algorithms.
//!
//! Exposes the coarse GraphCompute kernels (proposal §4.3, `graph-compute@1`) to
//! a Rhai script as methods on an opaque [`GcSession`] handle. The guest holds
//! only integer handles and the session object — no vertex data ever crosses
//! into the interpreter ("conductor, not worker", proposal §4.5). Each method
//! locks the per-CALL [`AlgoSession`], drives one native kernel, and returns a
//! packed handle (as an `i64`) or a scalar. The native-work budget and arena cap
//! carried by the session make a runaway guest loop fail closed exactly as they
//! do for the first-party provider (proposal §5.1).
//!
//! Handles cross the boundary as `i64` (the packed `u64` reinterpreted); the
//! handle table validates every one, so a forged or stale integer becomes a
//! typed Rhai runtime error, never an out-of-bounds access (proposal §4.2).
//
// Rust guideline compliant

#![cfg(feature = "rhai-runtime")]

use std::sync::Arc;

use parking_lot::Mutex;
use rhai::{Array, Dynamic, Engine, EvalAltResult, ImmutableString, Map, Position};
use uni_common::core::id::Vid;
use uni_plugin::errors::FnError;
use uni_plugin_builtin::algorithms::graph_compute::handle::Handle;
use uni_plugin_builtin::algorithms::graph_compute::kernel_id::KernelId;
use uni_plugin_builtin::algorithms::graph_compute::session::{
    AlgoSession, CmpOp, Direction, EndpointOp, EwiseOp, GraphArenaCompute, GraphCompute, MapOp,
    Norm, OverlapMetric, PairSpec, Predicate, ReduceOp, Semiring,
};
use uni_plugin_builtin::algorithms::graph_compute::value::{DType, Scalar};

/// A Rhai-visible handle to a per-CALL GraphCompute session.
///
/// Cheap to clone (shares the inner `Arc<Mutex<AlgoSession>>`), as required by
/// Rhai's `sync` feature. The `graph` field is the handle of the projected graph
/// the guest algorithm runs over, exposed to a script via the `graph()` method.
#[derive(Clone)]
pub struct GcSession {
    session: Arc<Mutex<AlgoSession>>,
    graph: i64,
    /// Pre-declared named scopes (`Arc` because Rhai's `sync` feature clones the
    /// receiver on every method call — a bare map would be copied per kernel).
    scopes: Arc<Vec<(String, i64)>>,
}

impl std::fmt::Debug for GcSession {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GcSession").finish_non_exhaustive()
    }
}

/// Wraps a shared session and its bound graph handle for the Rhai entrypoint.
#[must_use]
pub fn new_session(
    session: Arc<Mutex<AlgoSession>>,
    graph: Handle,
    scopes: Arc<Vec<(String, i64)>>,
) -> GcSession {
    GcSession {
        session,
        graph: to_i64(graph),
        scopes,
    }
}

/// Packs a handle into the `i64` the guest holds.
fn to_i64(h: Handle) -> i64 {
    // Reinterpret the packed u64 as i64; the round-trip is bit-exact.
    #[expect(
        clippy::cast_possible_wrap,
        reason = "opaque handle round-trips bit-exact"
    )]
    let v = h.as_u64() as i64;
    v
}

/// Reconstructs a handle from the guest's `i64`.
fn from_i64(v: i64) -> Handle {
    #[expect(clippy::cast_sign_loss, reason = "opaque handle round-trips bit-exact")]
    let bits = v as u64;
    Handle::from_u64(bits)
}

/// Converts a kernel [`FnError`] into a Rhai runtime error.
fn rt(e: FnError) -> Box<EvalAltResult> {
    Box::new(EvalAltResult::ErrorRuntime(
        rhai::Dynamic::from(format!("GraphCompute: {e}")),
        Position::NONE,
    ))
}

/// Packs an external vertex id into the `i64` a guest holds.
fn vid_to_i64(vid: Vid) -> i64 {
    #[expect(clippy::cast_possible_wrap, reason = "vids fit i64 in practice")]
    let v = vid.as_u64() as i64;
    v
}

impl GcSession {
    /// Returns the bound graph handle.
    fn graph_handle(&mut self) -> i64 {
        self.graph
    }

    /// Returns the handle of a pre-declared named scope.
    ///
    /// Scopes are built by the host before the guest runs, so this is a lookup,
    /// not a projection — there is nothing here a guest could call in a loop to
    /// escape the work meter. An unknown name lists what was declared, since the
    /// usual cause is a typo against the CALL site.
    fn graph_named(&mut self, name: &str) -> Result<i64, Box<EvalAltResult>> {
        self.scopes
            .iter()
            .find(|(n, _)| n == name)
            .map(|(_, h)| *h)
            .ok_or_else(|| {
                let declared: Vec<&str> = self.scopes.iter().map(|(n, _)| n.as_str()).collect();
                rt(FnError::new(
                    0x86E,
                    if declared.is_empty() {
                        format!(
                            "no graph scope `{name}`: this CALL declared no `scopes` map, so \
                             only the primary projection (`graph()`) exists"
                        )
                    } else {
                        format!(
                            "no graph scope `{name}`: declared scopes are {}",
                            declared.join(", ")
                        )
                    },
                ))
            })
    }

    /// Vertex count of a graph handle.
    fn vertex_count(&mut self, g: i64) -> Result<i64, Box<EvalAltResult>> {
        let s = self.session.lock();
        s.vertex_count(from_i64(g))
            .map(|v| i64::try_from(v).unwrap_or(i64::MAX))
            .map_err(rt)
    }

    /// Edge count of a graph handle.
    fn edge_count(&mut self, g: i64) -> Result<i64, Box<EvalAltResult>> {
        let s = self.session.lock();
        s.edge_count(from_i64(g))
            .map(|v| i64::try_from(v).unwrap_or(i64::MAX))
            .map_err(rt)
    }

    /// Builds a frontier from an array of external vertex ids.
    fn frontier(&mut self, g: i64, seeds: Array) -> Result<i64, Box<EvalAltResult>> {
        let vids: Vec<Vid> = seeds
            .into_iter()
            .map(|d| {
                d.as_int()
                    .map(|i| {
                        #[expect(clippy::cast_sign_loss, reason = "vertex ids are non-negative")]
                        let u = i as u64;
                        Vid::new(u)
                    })
                    .map_err(|_| rt(FnError::new(0x802, "frontier: seed must be an integer")))
            })
            .collect::<Result<_, _>>()?;
        let mut s = self.session.lock();
        s.frontier(from_i64(g), &vids).map(to_i64).map_err(rt)
    }

    /// BFS-to-fixpoint: the set of vertices reachable from `seeds` along `d`.
    fn reach_fixpoint(
        &mut self,
        g: i64,
        seeds: Array,
        d: ImmutableString,
    ) -> Result<i64, Box<EvalAltResult>> {
        let vids: Vec<Vid> = seeds
            .into_iter()
            .map(|s| {
                s.as_int()
                    .map(|i| {
                        #[expect(clippy::cast_sign_loss, reason = "vertex ids are non-negative")]
                        let u = i as u64;
                        Vid::new(u)
                    })
                    .map_err(|_| {
                        rt(FnError::new(
                            0x802,
                            "reach_fixpoint: seed must be an integer",
                        ))
                    })
            })
            .collect::<Result<_, _>>()?;
        let direction = Direction::parse(d.as_str()).map_err(rt)?;
        let mut s = self.session.lock();
        s.reach_fixpoint(from_i64(g), &vids, direction)
            .map(to_i64)
            .map_err(rt)
    }

    /// Per-vertex degree map in `dir`.
    fn degrees(&mut self, g: i64, d: ImmutableString) -> Result<i64, Box<EvalAltResult>> {
        let direction = Direction::parse(d.as_str()).map_err(rt)?;
        let mut s = self.session.lock();
        s.degrees(from_i64(g), direction).map(to_i64).map_err(rt)
    }

    /// Per-vertex own-slot-id map (WCC init).
    fn vertex_ids(&mut self, g: i64) -> Result<i64, Box<EvalAltResult>> {
        let mut s = self.session.lock();
        s.vertex_ids(from_i64(g)).map(to_i64).map_err(rt)
    }

    /// Lifts a set into a map assigning `value` to members.
    fn set_to_map(&mut self, set: i64, value: f64) -> Result<i64, Box<EvalAltResult>> {
        let mut s = self.session.lock();
        s.set_to_map(from_i64(set), Scalar::F64(value))
            .map(to_i64)
            .map_err(rt)
    }

    /// Lowers a map into the set matching a predicate (`is_zero`/`gt`/`lt`/`eq`).
    fn map_to_set(
        &mut self,
        m: i64,
        pred: ImmutableString,
        threshold: f64,
    ) -> Result<i64, Box<EvalAltResult>> {
        let p = Predicate::parse(pred.as_str(), threshold).map_err(rt)?;
        let mut s = self.session.lock();
        s.map_to_set(from_i64(m), p).map(to_i64).map_err(rt)
    }

    /// Reciprocal map, with `recip(0) = 0` (dangling rows drop out).
    fn recip(&mut self, m: i64) -> Result<i64, Box<EvalAltResult>> {
        let mut s = self.session.lock();
        s.map_apply(from_i64(m), MapOp::Recip)
            .map(to_i64)
            .map_err(rt)
    }

    /// Scales a map by a constant.
    fn scale(&mut self, m: i64, a: f64) -> Result<i64, Box<EvalAltResult>> {
        let mut s = self.session.lock();
        s.map_apply(from_i64(m), MapOp::Scale(a))
            .map(to_i64)
            .map_err(rt)
    }

    /// Normalizes a map to unit L1 or L2 norm.
    fn normalize(&mut self, m: i64, norm: ImmutableString) -> Result<i64, Box<EvalAltResult>> {
        let n = Norm::parse(norm.as_str()).map_err(rt)?;
        let mut s = self.session.lock();
        s.map_apply(from_i64(m), MapOp::Normalize(n))
            .map(to_i64)
            .map_err(rt)
    }

    /// Element-wise combine (`mul`/`add`/`min`/`max`/`axpy`); `coef` is used by axpy.
    fn ewise(
        &mut self,
        a: i64,
        b: i64,
        op: ImmutableString,
        coef: f64,
    ) -> Result<i64, Box<EvalAltResult>> {
        let o = EwiseOp::parse(op.as_str(), coef).map_err(rt)?;
        let mut s = self.session.lock();
        s.ewise(from_i64(a), from_i64(b), o).map(to_i64).map_err(rt)
    }

    /// Sparse mat-vec under a named semiring and direction.
    fn spmv(
        &mut self,
        g: i64,
        vec: i64,
        sr: ImmutableString,
        d: ImmutableString,
    ) -> Result<i64, Box<EvalAltResult>> {
        let semi = Semiring::parse(sr.as_str()).map_err(rt)?;
        let direction = Direction::parse(d.as_str()).map_err(rt)?;
        let mut s = self.session.lock();
        s.spmv(from_i64(g), from_i64(vec), semi, direction, None)
            .map(to_i64)
            .map_err(rt)
    }

    /// Sum reduction over a map.
    fn reduce_sum(&mut self, m: i64) -> Result<f64, Box<EvalAltResult>> {
        let mut s = self.session.lock();
        s.reduce(from_i64(m), ReduceOp::Sum, None)
            .map(Scalar::as_f64)
            .map_err(rt)
    }

    /// Sum reduction over a map, restricted to a mask set.
    fn reduce_sum_masked(&mut self, m: i64, mask: i64) -> Result<f64, Box<EvalAltResult>> {
        let mut s = self.session.lock();
        s.reduce(from_i64(m), ReduceOp::Sum, Some(from_i64(mask)))
            .map(Scalar::as_f64)
            .map_err(rt)
    }

    /// L1 distance between two maps (a convergence test).
    fn l1_diff(&mut self, a: i64, b: i64) -> Result<f64, Box<EvalAltResult>> {
        let mut s = self.session.lock();
        s.l1_diff(from_i64(a), from_i64(b)).map_err(rt)
    }

    /// One-hop expansion of a frontier, excluding a visited mask.
    fn expand(
        &mut self,
        g: i64,
        frontier: i64,
        d: ImmutableString,
        exclude: i64,
    ) -> Result<i64, Box<EvalAltResult>> {
        let direction = Direction::parse(d.as_str()).map_err(rt)?;
        let mut s = self.session.lock();
        s.expand(
            from_i64(g),
            from_i64(frontier),
            direction,
            Some(from_i64(exclude)),
        )
        .map(to_i64)
        .map_err(rt)
    }

    /// Set union.
    fn set_union(&mut self, a: i64, b: i64) -> Result<i64, Box<EvalAltResult>> {
        let mut s = self.session.lock();
        s.set_union(from_i64(a), from_i64(b))
            .map(to_i64)
            .map_err(rt)
    }

    /// Set cardinality.
    fn set_len(&mut self, set: i64) -> Result<i64, Box<EvalAltResult>> {
        let s = self.session.lock();
        s.set_len(from_i64(set))
            .map(|v| i64::try_from(v).unwrap_or(i64::MAX))
            .map_err(rt)
    }

    /// Whether a set is empty.
    fn is_empty(&mut self, set: i64) -> Result<bool, Box<EvalAltResult>> {
        let s = self.session.lock();
        s.is_empty(from_i64(set)).map_err(rt)
    }

    /// Frees a handle.
    fn free(&mut self, h: i64) -> Result<(), Box<EvalAltResult>> {
        let mut s = self.session.lock();
        s.free(from_i64(h)).map_err(rt)
    }

    /// Emits a single named per-vertex column into the result sink.
    fn emit(&mut self, name: ImmutableString, h: i64) -> Result<(), Box<EvalAltResult>> {
        let mut s = self.session.lock();
        s.emit(&[(name.as_str(), from_i64(h))]).map_err(rt)
    }

    /// Emits several named columns in one call: `gc.emit(#{"a": h1, "b": h2})`.
    ///
    /// Equivalent to one `emit` per entry — the session accumulates either way —
    /// but it is the shape the host trait models, and it keeps a multi-column
    /// algorithm's egress to a single boundary crossing.
    fn emit_cols(&mut self, cols: Map) -> Result<(), Box<EvalAltResult>> {
        let pairs: Vec<(String, i64)> = cols
            .into_iter()
            .map(|(name, v)| {
                v.as_int().map(|h| (name.to_string(), h)).map_err(|_| {
                    rt(FnError::new(
                        0x802,
                        "emit: each column value must be a handle",
                    ))
                })
            })
            .collect::<Result<_, _>>()?;
        let borrowed: Vec<(&str, Handle)> = pairs
            .iter()
            .map(|(name, h)| (name.as_str(), from_i64(*h)))
            .collect();
        let mut s = self.session.lock();
        s.emit(&borrowed).map_err(rt)
    }

    /// The native-work budget this invocation started with.
    fn work_budget(&mut self) -> Result<f64, Box<EvalAltResult>> {
        self.session.lock().work_budget().map_err(rt)
    }

    /// Native-work units charged so far.
    fn work_spent(&mut self) -> Result<f64, Box<EvalAltResult>> {
        self.session.lock().work_spent().map_err(rt)
    }

    /// Native-work units still available. Reading it is free.
    fn work_remaining(&mut self) -> Result<f64, Box<EvalAltResult>> {
        self.session.lock().work_remaining().map_err(rt)
    }

    /// Elementwise comparison, yielding a 1.0/0.0 mask.
    fn compare(&mut self, a: i64, b: i64, op: ImmutableString) -> Result<i64, Box<EvalAltResult>> {
        let o = CmpOp::parse(op.as_str()).map_err(rt)?;
        let mut s = self.session.lock();
        s.compare(from_i64(a), from_i64(b), o)
            .map(to_i64)
            .map_err(rt)
    }

    /// Generic map transform (`recip`/`scale`/`log`/`affine`/`normalize_l1|l2`);
    /// `a`,`b` are the scalar operands (`scale a`, `affine a·x+b`).
    fn map_apply(
        &mut self,
        m: i64,
        op: ImmutableString,
        a: f64,
        b: f64,
    ) -> Result<i64, Box<EvalAltResult>> {
        let o = MapOp::parse(op.as_str(), a, b).map_err(rt)?;
        let mut s = self.session.lock();
        s.map_apply(from_i64(m), o).map(to_i64).map_err(rt)
    }

    /// A zeroed float map over the graph's vertices.
    fn zero_map(&mut self, g: i64) -> Result<i64, Box<EvalAltResult>> {
        let mut s = self.session.lock();
        s.zero_map(from_i64(g), DType::F64).map(to_i64).map_err(rt)
    }

    /// A zeroed map of a given dtype (`"f64"` or `"i64"`); `"i64"` seeds an exact
    /// integer path-counting run (F-9).
    fn zero_map_typed(
        &mut self,
        g: i64,
        dtype: ImmutableString,
    ) -> Result<i64, Box<EvalAltResult>> {
        let ty = if dtype.as_str() == "i64" {
            DType::I64
        } else {
            DType::F64
        };
        let mut s = self.session.lock();
        s.zero_map(from_i64(g), ty).map(to_i64).map_err(rt)
    }

    /// Overwrites `map` at each `frontier` member with `value`.
    fn scatter(&mut self, map: i64, frontier: i64, value: f64) -> Result<i64, Box<EvalAltResult>> {
        let mut s = self.session.lock();
        s.scatter(from_i64(map), from_i64(frontier), Scalar::F64(value))
            .map(to_i64)
            .map_err(rt)
    }

    /// Set difference `a \ b`.
    fn set_diff(&mut self, a: i64, b: i64) -> Result<i64, Box<EvalAltResult>> {
        let mut s = self.session.lock();
        s.set_diff(from_i64(a), from_i64(b)).map(to_i64).map_err(rt)
    }

    /// Set intersection `a ∩ b`.
    fn set_intersect(&mut self, a: i64, b: i64) -> Result<i64, Box<EvalAltResult>> {
        let mut s = self.session.lock();
        s.set_intersect(from_i64(a), from_i64(b))
            .map(to_i64)
            .map_err(rt)
    }

    /// The `[vertexId, value]` extremum of a map (`want_max` selects max vs min).
    fn arg_extreme(&mut self, m: i64, want_max: bool) -> Result<Array, Box<EvalAltResult>> {
        let mut s = self.session.lock();
        let (vid, val) = s.arg_extreme(from_i64(m), want_max).map_err(rt)?;
        Ok(vec![
            Dynamic::from_int(vid_to_i64(vid)),
            Dynamic::from_float(val.as_f64()),
        ])
    }

    /// The top-`k` `[vertexId, value]` pairs by descending value.
    fn topk(&mut self, m: i64, k: i64) -> Result<Array, Box<EvalAltResult>> {
        // #233 Tier 1: a negative or oversized `k` became 0, which is a
        // valid top-k meaning "return nothing" — so the script got an empty
        // result instead of being told its argument was unusable.
        let kk = u32::try_from(k).map_err(|_| {
            rt(FnError::new(
                0x803,
                format!("topk: k must fit in u32, got {k}"),
            ))
        })?;
        let mut s = self.session.lock();
        let ranked = s.topk(from_i64(m), kk).map_err(rt)?;
        Ok(ranked
            .into_iter()
            .map(|(vid, val)| {
                let pair: Array = vec![
                    Dynamic::from_int(vid_to_i64(vid)),
                    Dynamic::from_float(val.as_f64()),
                ];
                Dynamic::from_array(pair)
            })
            .collect())
    }

    /// Samples node2vec/DeepWalk random walks; empty `seeds` walks every vertex.
    ///
    /// `p`/`q` are the return/in-out bias (`1.0` = unbiased); `seed` makes the
    /// sampling deterministic. Returns a walks handle for `emit_walks` /
    /// `walk_visit_counts`.
    #[expect(clippy::too_many_arguments, reason = "mirrors the random_walks kernel")]
    fn random_walks(
        &mut self,
        g: i64,
        seeds: Array,
        walk_length: i64,
        walks_per_node: i64,
        p: f64,
        q: f64,
        seed: i64,
    ) -> Result<i64, Box<EvalAltResult>> {
        let vids: Vec<Vid> = seeds
            .into_iter()
            .map(|d| {
                d.as_int()
                    .map(|i| {
                        #[expect(clippy::cast_sign_loss, reason = "vertex ids are non-negative")]
                        let u = i as u64;
                        Vid::new(u)
                    })
                    .map_err(|_| rt(FnError::new(0x802, "random_walks: seed must be an integer")))
            })
            .collect::<Result<_, _>>()?;
        // #233 Tier 1: a negative length or count became 0, so `random_walks`
        // silently produced no walks rather than reporting the bad argument.
        let wl = usize::try_from(walk_length).map_err(|_| {
            rt(FnError::new(
                0x804,
                format!("random_walks: walk_length must be non-negative, got {walk_length}"),
            ))
        })?;
        let wn = usize::try_from(walks_per_node).map_err(|_| {
            rt(FnError::new(
                0x805,
                format!("random_walks: walks_per_node must be non-negative, got {walks_per_node}"),
            ))
        })?;
        #[expect(clippy::cast_sign_loss, reason = "the rng seed round-trips bit-exact")]
        let rng_seed = seed as u64;
        let mut s = self.session.lock();
        s.random_walks(from_i64(g), wl, wn, &vids, p, q, rng_seed)
            .map(to_i64)
            .map_err(rt)
    }

    /// Draws a `Bernoulli(prob[v])` mask over a `[V]` probability tensor.
    ///
    /// `seed`/`iter` select the reproducible counter-hash stream; advancing
    /// `iter` yields a fresh, decorrelated per-iteration mask (proposal §8).
    /// Returns a vertex-set (mask) handle.
    fn sample(&mut self, prob: i64, seed: i64, iter: i64) -> Result<i64, Box<EvalAltResult>> {
        #[expect(clippy::cast_sign_loss, reason = "seed/iter round-trip bit-exact")]
        let (seed, iter) = (seed as u64, iter as u64);
        let mut s = self.session.lock();
        s.sample(from_i64(prob), seed, iter).map(to_i64).map_err(rt)
    }

    /// Builds a `[E]` per-edge tensor of out-edge weights (proposal §5).
    fn edge_weights(&mut self, g: i64) -> Result<i64, Box<EvalAltResult>> {
        let mut s = self.session.lock();
        s.edge_weights(from_i64(g)).map(to_i64).map_err(rt)
    }

    /// Builds a `[E]` per-edge tensor of the named projected edge property (#151).
    fn edge_property(&mut self, g: i64, name: ImmutableString) -> Result<i64, Box<EvalAltResult>> {
        let mut s = self.session.lock();
        s.edge_property(from_i64(g), name.as_str())
            .map(to_i64)
            .map_err(rt)
    }

    /// Builds a `[V]` per-vertex tensor of the named projected vertex property (#151).
    fn node_property(&mut self, g: i64, name: ImmutableString) -> Result<i64, Box<EvalAltResult>> {
        let mut s = self.session.lock();
        s.node_property(from_i64(g), name.as_str())
            .map(to_i64)
            .map_err(rt)
    }

    /// The full edge mask — every edge of `g` active.
    fn edges_all(&mut self, g: i64) -> Result<i64, Box<EvalAltResult>> {
        let mut s = self.session.lock();
        s.edges_all(from_i64(g)).map(to_i64).map_err(rt)
    }

    /// Draws a `Bernoulli(prob[e])` edge mask from a `[E]` probability tensor.
    fn sample_edges(&mut self, prob: i64, seed: i64, iter: i64) -> Result<i64, Box<EvalAltResult>> {
        #[expect(clippy::cast_sign_loss, reason = "seed/iter round-trip bit-exact")]
        let (seed, iter) = (seed as u64, iter as u64);
        let mut s = self.session.lock();
        s.sample_edges(from_i64(prob), seed, iter)
            .map(to_i64)
            .map_err(rt)
    }

    /// Undirected edge sampler: both half-edges of a pair share one draw.
    fn sample_edges_undirected(
        &mut self,
        g: i64,
        prob: i64,
        seed: i64,
        iter: i64,
    ) -> Result<i64, Box<EvalAltResult>> {
        #[expect(clippy::cast_sign_loss, reason = "seed/iter round-trip bit-exact")]
        let (seed, iter) = (seed as u64, iter as u64);
        let mut s = self.session.lock();
        s.sample_edges_undirected(from_i64(g), from_i64(prob), seed, iter)
            .map(to_i64)
            .map_err(rt)
    }

    /// Cardinality of an edge mask.
    fn edge_set_len(&mut self, m: i64) -> Result<i64, Box<EvalAltResult>> {
        let s = self.session.lock();
        s.edge_set_len(from_i64(m))
            .map(|v| i64::try_from(v).unwrap_or(i64::MAX))
            .map_err(rt)
    }

    /// Edges whose `[E]` value lies in the window `[lo, hi]` (F-11 time windows).
    fn edge_mask_window(
        &mut self,
        edge_vals: i64,
        lo: f64,
        hi: f64,
    ) -> Result<i64, Box<EvalAltResult>> {
        let mut s = self.session.lock();
        s.edge_mask_window(from_i64(edge_vals), lo, hi)
            .map(to_i64)
            .map_err(rt)
    }

    /// Deterministic segmented reduce: per-group totals broadcast to members.
    fn segmented_reduce(&mut self, values: i64, groups: i64) -> Result<i64, Box<EvalAltResult>> {
        let mut s = self.session.lock();
        s.segmented_reduce(from_i64(values), from_i64(groups))
            .map(to_i64)
            .map_err(rt)
    }

    /// Intersection of two edge masks.
    fn edge_intersect(&mut self, a: i64, b: i64) -> Result<i64, Box<EvalAltResult>> {
        let mut s = self.session.lock();
        s.edge_intersect(from_i64(a), from_i64(b))
            .map(to_i64)
            .map_err(rt)
    }

    /// Union of two edge masks.
    fn edge_union(&mut self, a: i64, b: i64) -> Result<i64, Box<EvalAltResult>> {
        let mut s = self.session.lock();
        s.edge_union(from_i64(a), from_i64(b))
            .map(to_i64)
            .map_err(rt)
    }

    /// One-hop expansion over the masked out-edges, excluding a visited mask
    /// (pass exclude `0` for none).
    fn expand_masked(
        &mut self,
        g: i64,
        frontier: i64,
        d: ImmutableString,
        exclude: i64,
        edge_mask: i64,
    ) -> Result<i64, Box<EvalAltResult>> {
        let direction = Direction::parse(d.as_str()).map_err(rt)?;
        let mut s = self.session.lock();
        s.expand_masked(
            from_i64(g),
            from_i64(frontier),
            direction,
            if exclude == 0 {
                None
            } else {
                Some(from_i64(exclude))
            },
            from_i64(edge_mask),
        )
        .map(to_i64)
        .map_err(rt)
    }

    /// Fused frontier-scoped sampled expansion: draw + expand, out-edges only.
    #[expect(
        clippy::too_many_arguments,
        reason = "kernel arity mirrors the wire op"
    )]
    fn expand_sampled(
        &mut self,
        g: i64,
        frontier: i64,
        d: ImmutableString,
        exclude: i64,
        prob: i64,
        seed: i64,
        iter: i64,
    ) -> Result<i64, Box<EvalAltResult>> {
        let direction = Direction::parse(d.as_str()).map_err(rt)?;
        #[expect(clippy::cast_sign_loss, reason = "seed/iter round-trip bit-exact")]
        let (seed, iter) = (seed as u64, iter as u64);
        let mut s = self.session.lock();
        s.expand_sampled(
            from_i64(g),
            from_i64(frontier),
            direction,
            if exclude == 0 {
                None
            } else {
                Some(from_i64(exclude))
            },
            from_i64(prob),
            seed,
            iter,
        )
        .map(to_i64)
        .map_err(rt)
    }

    /// `spmv` restricted to the masked out-edges (out-direction only).
    fn spmv_masked(
        &mut self,
        g: i64,
        vec: i64,
        sr: ImmutableString,
        edge_mask: i64,
    ) -> Result<i64, Box<EvalAltResult>> {
        let semi = Semiring::parse(sr.as_str()).map_err(rt)?;
        let mut s = self.session.lock();
        s.spmv_masked(from_i64(g), from_i64(vec), semi, from_i64(edge_mask))
            .map(to_i64)
            .map_err(rt)
    }

    /// Folds a walks handle into a per-vertex visit-count map.
    /// Copies a projection's topology into an empty arena.
    fn arena_seed(&mut self, arena: i64, g: i64) -> Result<i64, Box<EvalAltResult>> {
        let mut s = self.session.lock();
        s.arena_seed(from_i64(arena), from_i64(g))
            .map(to_i64)
            .map_err(rt)
    }

    /// Gathers a `[V]` node value onto edges, yielding `[E]`.
    fn edge_from_nodes(&mut self, g: i64, x: i64, op: &str) -> Result<i64, Box<EvalAltResult>> {
        let op = EndpointOp::parse(op).map_err(rt)?;
        let mut s = self.session.lock();
        s.edge_from_nodes(from_i64(g), from_i64(x), op)
            .map(to_i64)
            .map_err(rt)
    }

    /// Piecewise-linear table lookup over `(xs, ys)` breakpoints.
    fn interp(&mut self, x: i64, xs: Array, ys: Array) -> Result<i64, Box<EvalAltResult>> {
        let to_vec = |a: Array, which: &str| -> Result<Vec<f64>, Box<EvalAltResult>> {
            a.into_iter()
                .map(|d| {
                    d.as_float()
                        .or_else(|_| d.as_int().map(|i| i as f64))
                        .map_err(|t| {
                            rt(FnError::new(
                                0x86E,
                                format!("interp {which}-breakpoints must be numbers, got {t}"),
                            ))
                        })
                })
                .collect()
        };
        let (xs, ys) = (to_vec(xs, "x")?, to_vec(ys, "y")?);
        let mut s = self.session.lock();
        s.interp(from_i64(x), &xs, &ys).map(to_i64).map_err(rt)
    }

    /// Re-keys a `[V]` value into another projection's index space, verified.
    fn rekey(&mut self, value: i64, g: i64) -> Result<i64, Box<EvalAltResult>> {
        let mut s = self.session.lock();
        s.rekey(from_i64(value), from_i64(g))
            .map(to_i64)
            .map_err(rt)
    }

    fn walk_visit_counts(&mut self, walks: i64, g: i64) -> Result<i64, Box<EvalAltResult>> {
        let mut s = self.session.lock();
        s.walk_visit_counts(from_i64(walks), from_i64(g))
            .map(to_i64)
            .map_err(rt)
    }

    /// Emits the walk *sequences* as `(walk_id, step, nodeId)` result rows.
    fn emit_walks(&mut self, walks: i64) -> Result<(), Box<EvalAltResult>> {
        let mut s = self.session.lock();
        s.emit_walks(from_i64(walks)).map_err(rt)
    }

    /// Per-vertex neighbourhood-overlap similarity to `source`.
    ///
    /// `metric` is `"jaccard"`, `"overlap"`, `"cosine"`, or `"adamic_adar"`.
    fn neighborhood_overlap(
        &mut self,
        g: i64,
        source: i64,
        metric: ImmutableString,
    ) -> Result<i64, Box<EvalAltResult>> {
        let m = OverlapMetric::parse(metric.as_str()).map_err(rt)?;
        #[expect(clippy::cast_sign_loss, reason = "vertex ids are non-negative")]
        let src = Vid::new(source as u64);
        let mut s = self.session.lock();
        s.neighborhood_overlap(from_i64(g), src, m)
            .map(to_i64)
            .map_err(rt)
    }

    /// The Δ-stepping frontier of vertices whose distance lies in the bucket band.
    fn next_bucket(
        &mut self,
        dist: i64,
        delta: f64,
        bucket: i64,
    ) -> Result<i64, Box<EvalAltResult>> {
        // #233 Tier 1: a negative bucket became bucket 0, and the caller got
        // bucket 0's answer as if it were the band they asked for — a wrong
        // SSSP result rather than an error.
        let b = u32::try_from(bucket).map_err(|_| {
            rt(FnError::new(
                0x806,
                format!("next_bucket: bucket must be non-negative, got {bucket}"),
            ))
        })?;
        let mut s = self.session.lock();
        s.next_bucket(from_i64(dist), delta, b)
            .map(to_i64)
            .map_err(rt)
    }

    /// All-pairs neighbourhood overlap over adjacent vertex pairs.
    ///
    /// `metric` is `"count"` (triangle support), `"jaccard"`, `"overlap"`,
    /// `"cosine"`, or `"adamic_adar"`; `pair_mode` is `"adjacent"` or `"topk"`
    /// (keeping the `k` highest-value pairs). Returns a pairs handle for
    /// `emit_pairs`.
    fn all_pairs_overlap(
        &mut self,
        g: i64,
        metric: ImmutableString,
        pair_mode: ImmutableString,
        k: i64,
    ) -> Result<i64, Box<EvalAltResult>> {
        let m = OverlapMetric::parse(metric.as_str()).map_err(rt)?;
        let spec = if pair_mode.as_str() == "topk" {
            // #233 Tier 1: see `topk` — 0 means "return nothing", so a bad
            // `k` produced zero pairs instead of an error.
            PairSpec::TopKCandidates(u32::try_from(k).map_err(|_| {
                rt(FnError::new(
                    0x807,
                    format!("all_pairs_overlap: k must fit in u32, got {k}"),
                ))
            })?)
        } else {
            PairSpec::AdjacentPairs
        };
        let mut s = self.session.lock();
        s.all_pairs_overlap(from_i64(g), spec, m)
            .map(to_i64)
            .map_err(rt)
    }

    /// Emits a pair list as `(srcId, dstId, value)` result rows.
    fn emit_pairs(&mut self, pairs: i64) -> Result<(), Box<EvalAltResult>> {
        let mut s = self.session.lock();
        s.emit_pairs(from_i64(pairs)).map_err(rt)
    }

    // --- `graph-arena@1`: mutable session-local structure (proposal §5.1) ---

    /// Creates an arena of `capacity` slots with `branching` child slack.
    fn arena_new(&mut self, capacity: i64, branching: i64) -> Result<i64, Box<EvalAltResult>> {
        let (c, b) = (
            u32_arg(capacity, "capacity")?,
            u32_arg(branching, "branching")?,
        );
        let mut s = self.session.lock();
        s.arena_new(c, b).map(to_i64).map_err(rt)
    }

    /// Bump-allocates `count` slots, returning an `i64` tensor of their ids.
    fn arena_alloc(&mut self, arena: i64, count: i64) -> Result<i64, Box<EvalAltResult>> {
        let n = u32_arg(count, "count")?;
        let mut s = self.session.lock();
        s.arena_alloc(from_i64(arena), n).map(to_i64).map_err(rt)
    }

    /// Links each `kids[i]` as a child of `parents[i]`.
    fn arena_link(
        &mut self,
        arena: i64,
        parents: i64,
        kids: i64,
    ) -> Result<(), Box<EvalAltResult>> {
        let mut s = self.session.lock();
        s.arena_link(from_i64(arena), from_i64(parents), from_i64(kids))
            .map_err(rt)
    }

    /// Adds a zero-filled per-slot state column, returning its index.
    fn arena_column(&mut self, arena: i64) -> Result<i64, Box<EvalAltResult>> {
        let mut s = self.session.lock();
        s.arena_column(from_i64(arena)).map_err(rt)
    }

    /// The children of every slot in `roots`, concatenated.
    fn arena_candidates(&mut self, arena: i64, roots: i64) -> Result<i64, Box<EvalAltResult>> {
        let mut s = self.session.lock();
        s.arena_candidates(from_i64(arena), from_i64(roots))
            .map(to_i64)
            .map_err(rt)
    }

    /// Gathers `column` at `slots` into a compact tensor.
    fn arena_gather(
        &mut self,
        arena: i64,
        column: i64,
        slots: i64,
    ) -> Result<i64, Box<EvalAltResult>> {
        let c = u32_arg(column, "column")?;
        let mut s = self.session.lock();
        s.arena_gather(from_i64(arena), c, from_i64(slots))
            .map(to_i64)
            .map_err(rt)
    }

    /// Scatters `values` into `column` at `slots`.
    fn arena_scatter(
        &mut self,
        arena: i64,
        column: i64,
        slots: i64,
        values: i64,
    ) -> Result<(), Box<EvalAltResult>> {
        let c = u32_arg(column, "column")?;
        let mut s = self.session.lock();
        s.arena_scatter(from_i64(arena), c, from_i64(slots), from_i64(values))
            .map_err(rt)
    }

    /// Adds `deltas[i]` to `value_col` along the root path of every `leaves[i]`.
    fn arena_backup(
        &mut self,
        arena: i64,
        value_col: i64,
        leaves: i64,
        deltas: i64,
    ) -> Result<(), Box<EvalAltResult>> {
        let c = u32_arg(value_col, "value_col")?;
        let mut s = self.session.lock();
        s.arena_backup(from_i64(arena), c, from_i64(leaves), from_i64(deltas))
            .map_err(rt)
    }

    /// Descends from each root to a leaf by the guest's `score` column.
    fn arena_descend(
        &mut self,
        arena: i64,
        roots: i64,
        score: i64,
        visit: i64,
        maximize: bool,
        vloss: f64,
    ) -> Result<i64, Box<EvalAltResult>> {
        let (sc, vi) = (u32_arg(score, "score")?, u32_arg(visit, "visit")?);
        let mut s = self.session.lock();
        s.arena_descend(from_i64(arena), from_i64(roots), sc, vi, maximize, vloss)
            .map(to_i64)
            .map_err(rt)
    }

    /// Allocates `fanout` children for every slot in `parents` and links them.
    fn arena_expand(
        &mut self,
        arena: i64,
        parents: i64,
        fanout: i64,
    ) -> Result<i64, Box<EvalAltResult>> {
        let f = u32_arg(fanout, "fanout")?;
        let mut s = self.session.lock();
        s.arena_expand(from_i64(arena), from_i64(parents), f)
            .map(to_i64)
            .map_err(rt)
    }

    /// Compacts the arena into an immutable graph handle.
    fn arena_freeze(&mut self, arena: i64) -> Result<i64, Box<EvalAltResult>> {
        let mut s = self.session.lock();
        s.arena_freeze(from_i64(arena)).map(to_i64).map_err(rt)
    }
}

/// Narrows a guest `i64` to the `u32` a kernel expects, or fails typed.
fn u32_arg(v: i64, what: &str) -> Result<u32, Box<EvalAltResult>> {
    u32::try_from(v).map_err(|_| {
        rt(FnError::new(
            0x86E,
            format!("{what} must be a non-negative 32-bit value, got {v}"),
        ))
    })
}

/// Registers the [`GcSession`] type and its kernel methods on `engine`.
///
/// Always registered when the GraphCompute surface is available; the capability
/// gate is enforced at projection time on the host side (proposal §4.6), and a
/// guest that never receives a [`GcSession`] cannot call any method.
/// Turns "function not found" into a composition hint where one is published.
///
/// A guest reaching for `gc.select(..)` or `gc.arena_spmv(..)` previously got
/// Rhai's generic `Function not found: select (GcSession, i64, i64)`, which says
/// the name is unknown but not that the operation is *expressible*. Three
/// consecutive field reports asked for capabilities that were already
/// composable, so the gap this closes is discoverability, not surface.
///
/// Arity-independent, unlike the [`register_arena_stubs`] approach: the hook
/// fires for any signature, so a guest that also gets the argument count wrong
/// still gets the recipe.
///
/// The hook fires on *any* native call failure, not only an unknown name, and it
/// is not handed the error. Two things keep that safe: it only speaks when the
/// receiver is a [`GcSession`], and `no_method_recipe_shadows_a_real_kernel`
/// guarantees no hint key names a registered kernel — so a genuine error raised
/// from inside a working kernel can never be answered by a hint.
fn register_missing_kernel_hints(engine: &mut Engine) {
    #[expect(
        deprecated,
        reason = "on_missing_function is marked volatile, not deprecated; it is the                   only arity-independent hook for this"
    )]
    engine.on_missing_function(|name, args, _is_method, _ctx| {
        let on_session = args.first().is_some_and(|a| a.is::<GcSession>());
        if !on_session {
            return Ok(None);
        }
        match uni_plugin_builtin::algorithms::graph_compute::unknown_method_message(name) {
            Some(msg) => Err(rt(FnError::new(0x86E, msg))),
            None => Ok(None),
        }
    });
}

/// Registers the [`GcSession`] type and its kernel methods on `engine`.
///
/// Always registered when the GraphCompute surface is available; the capability
/// gate is enforced at projection time on the host side (proposal §4.6), and a
/// guest that never receives a [`GcSession`] cannot call any method.
pub fn register_graph_compute(engine: &mut Engine) {
    engine.register_type_with_name::<GcSession>("GcSession");
    let _ = register_kernels(engine);
    register_optional_arities(engine);
    register_arena_stubs(engine);
    register_scoped_graph_stub(engine);
    register_missing_kernel_hints(engine);
}

/// Registers every catalog kernel on `engine` and returns the set registered.
///
/// Registration and reporting come from a *single* declaration below, so the
/// returned set cannot claim a kernel that was not actually registered — which
/// is what makes the reachability contract meaningful rather than a second list
/// to keep in sync. Names come from [`KernelId::op_name`], so a rename in the
/// catalog propagates here instead of silently diverging.
/// Registers the short forms declared by [`OPTIONAL_ARITIES`].
///
/// Rhai resolves overloads by arity, so a kernel registered once at its full
/// arity is simply *not found* when called with fewer arguments — which is how
/// `map_apply(a, "scale", -1.0)`, the verbatim text of the published `neg`
/// recipe, failed on this loader while working over JSON and PyO3. These
/// registrations close that gap; they add no capability, only the spellings the
/// documentation already promises. The omitted scalars default to `0.0`, which
/// is what the JSON wire and PyO3 already use.
///
/// Deliberately not pushed onto the registered-kernel list: each name is already
/// there from its full-arity registration, and counting it twice would weaken
/// the reachability contract.
fn register_optional_arities(engine: &mut Engine) {
    engine.register_fn(
        KernelId::MapApply.op_name(),
        |s: &mut GcSession, m: i64, op: ImmutableString| s.map_apply(m, op, 0.0, 0.0),
    );
    engine.register_fn(
        KernelId::MapApply.op_name(),
        |s: &mut GcSession, m: i64, op: ImmutableString, a: f64| s.map_apply(m, op, a, 0.0),
    );
    engine.register_fn(
        KernelId::Ewise.op_name(),
        |s: &mut GcSession, a: i64, b: i64, op: ImmutableString| s.ewise(a, b, op, 0.0),
    );

    // The reproducible-sampling trio: a guest that does not care about the
    // counter-hash stream should not have to name it. `0, 0` is the stream the
    // documented examples use.
    engine.register_fn(
        KernelId::Sample.op_name(),
        |s: &mut GcSession, prob: i64| s.sample(prob, 0, 0),
    );
    engine.register_fn(
        KernelId::Sample.op_name(),
        |s: &mut GcSession, prob: i64, seed: i64| s.sample(prob, seed, 0),
    );
    engine.register_fn(
        KernelId::SampleEdges.op_name(),
        |s: &mut GcSession, prob: i64| s.sample_edges(prob, 0, 0),
    );
    engine.register_fn(
        KernelId::SampleEdges.op_name(),
        |s: &mut GcSession, prob: i64, seed: i64| s.sample_edges(prob, seed, 0),
    );

    // Overlap defaults to plain adjacent-pair counts.
    engine.register_fn(
        KernelId::AllPairsOverlap.op_name(),
        |s: &mut GcSession, g: i64| s.all_pairs_overlap(g, "count".into(), "adjacent".into(), 0),
    );
    engine.register_fn(
        KernelId::AllPairsOverlap.op_name(),
        |s: &mut GcSession, g: i64, metric: ImmutableString| {
            s.all_pairs_overlap(g, metric, "adjacent".into(), 0)
        },
    );
    engine.register_fn(
        KernelId::AllPairsOverlap.op_name(),
        |s: &mut GcSession, g: i64, metric: ImmutableString, pair_mode: ImmutableString| {
            s.all_pairs_overlap(g, metric, pair_mode, 0)
        },
    );

    // Walks default to one uniform (p = q = 1) walk per seed, stream 0.
    engine.register_fn(
        KernelId::RandomWalks.op_name(),
        |s: &mut GcSession, g: i64, seeds: Array, len: i64| {
            s.random_walks(g, seeds, len, 1, 1.0, 1.0, 0)
        },
    );
    engine.register_fn(
        KernelId::RandomWalks.op_name(),
        |s: &mut GcSession, g: i64, seeds: Array, len: i64, n: i64| {
            s.random_walks(g, seeds, len, n, 1.0, 1.0, 0)
        },
    );
    engine.register_fn(
        KernelId::RandomWalks.op_name(),
        |s: &mut GcSession, g: i64, seeds: Array, len: i64, n: i64, p: f64| {
            s.random_walks(g, seeds, len, n, p, 1.0, 0)
        },
    );
    engine.register_fn(
        KernelId::RandomWalks.op_name(),
        |s: &mut GcSession, g: i64, seeds: Array, len: i64, n: i64, p: f64, q: f64| {
            s.random_walks(g, seeds, len, n, p, q, 0)
        },
    );
}

fn register_kernels(engine: &mut Engine) -> Vec<KernelId> {
    let mut registered = Vec::new();
    macro_rules! reg {
        ($( $id:ident => $method:path ),* $(,)?) => {{
            $(
                engine.register_fn(KernelId::$id.op_name(), $method);
                registered.push(KernelId::$id);
            )*
        }};
    }
    reg!(
        Graph => GcSession::graph_handle,
        GraphNamed => GcSession::graph_named,
        VertexCount => GcSession::vertex_count,
        EdgeCount => GcSession::edge_count,
        Frontier => GcSession::frontier,
        ReachFixpoint => GcSession::reach_fixpoint,
        Degrees => GcSession::degrees,
        VertexIds => GcSession::vertex_ids,
        SetToMap => GcSession::set_to_map,
        MapToSet => GcSession::map_to_set,
        Recip => GcSession::recip,
        Scale => GcSession::scale,
        Normalize => GcSession::normalize,
        Ewise => GcSession::ewise,
        Compare => GcSession::compare,
        WorkBudget => GcSession::work_budget,
        WorkSpent => GcSession::work_spent,
        WorkRemaining => GcSession::work_remaining,
        Spmv => GcSession::spmv,
        ReduceSum => GcSession::reduce_sum,
        ReduceSumMasked => GcSession::reduce_sum_masked,
        L1Diff => GcSession::l1_diff,
        Expand => GcSession::expand,
        SetUnion => GcSession::set_union,
        SetDiff => GcSession::set_diff,
        SetIntersect => GcSession::set_intersect,
        SetLen => GcSession::set_len,
        IsEmpty => GcSession::is_empty,
        MapApply => GcSession::map_apply,
        ZeroMap => GcSession::zero_map,
        Scatter => GcSession::scatter,
        ArgExtreme => GcSession::arg_extreme,
        Topk => GcSession::topk,
        Free => GcSession::free,
        Emit => GcSession::emit,
        RandomWalks => GcSession::random_walks,
        Sample => GcSession::sample,
        EdgeWeights => GcSession::edge_weights,
        EdgeProperty => GcSession::edge_property,
        NodeProperty => GcSession::node_property,
        EdgesAll => GcSession::edges_all,
        SampleEdges => GcSession::sample_edges,
        SampleEdgesUndirected => GcSession::sample_edges_undirected,
        EdgeSetLen => GcSession::edge_set_len,
        EdgeMaskWindow => GcSession::edge_mask_window,
        SegmentedReduce => GcSession::segmented_reduce,
        EdgeIntersect => GcSession::edge_intersect,
        EdgeUnion => GcSession::edge_union,
        ExpandMasked => GcSession::expand_masked,
        ExpandSampled => GcSession::expand_sampled,
        SpmvMasked => GcSession::spmv_masked,
        WalkVisitCounts => GcSession::walk_visit_counts,
        Rekey => GcSession::rekey,
        Interp => GcSession::interp,
        EdgeFromNodes => GcSession::edge_from_nodes,
        ArenaSeed => GcSession::arena_seed,
        EmitWalks => GcSession::emit_walks,
        NeighborhoodOverlap => GcSession::neighborhood_overlap,
        NextBucket => GcSession::next_bucket,
        AllPairsOverlap => GcSession::all_pairs_overlap,
        EmitPairs => GcSession::emit_pairs,
        ArenaNew => GcSession::arena_new,
        ArenaAlloc => GcSession::arena_alloc,
        ArenaLink => GcSession::arena_link,
        ArenaColumn => GcSession::arena_column,
        ArenaCandidates => GcSession::arena_candidates,
        ArenaGather => GcSession::arena_gather,
        ArenaScatter => GcSession::arena_scatter,
        ArenaBackup => GcSession::arena_backup,
        ArenaDescend => GcSession::arena_descend,
        ArenaExpand => GcSession::arena_expand,
        ArenaFreeze => GcSession::arena_freeze,
    );
    // Rhai overloads on arity, so `zero_map`'s dtype-taking form is a second
    // registration under the same name — one catalog entry, two signatures.
    engine.register_fn(KernelId::ZeroMap.op_name(), GcSession::zero_map_typed);
    // Same shape for `emit`: the map-taking batch form is a second signature
    // under the one catalog name.
    engine.register_fn(KernelId::Emit.op_name(), GcSession::emit_cols);
    registered
}

/// Builds the typed "slice not provided" refusal for an arena kernel.
fn arena_unavailable(op: &'static str) -> Box<EvalAltResult> {
    rt(uni_plugin_builtin::algorithms::graph_compute::unresolved_op_error(op))
}

/// Registers the `graph-arena@1` kernel names as typed-refusal stubs.
///
/// A Rhai guest reaching for a mutable-arena primitive previously got Rhai's
/// generic `Function not found`, which does not distinguish "you misspelled it"
/// from "this host cannot do that" — the defect reported as issue #152. Binding
/// the names with their real arities turns that into a `0x86A` naming the
/// capability slice the guest needs.
///
/// These stubs are the surface arriving ahead of its implementation: the arena
/// kernels replace the bodies, keeping the same names and arities.
/// Answers `gc.graph(#{...})` — the shape a guest reaches for when it wants a
/// second projection.
///
/// Named scopes shipped, and a field report still recorded REQ-D5 as "still
/// absent" against a build that contained them: the probe was `gc.graph(scope)`,
/// which is a signature mismatch, and Rhai's `Function not found` says nothing
/// about the feature that replaced it. `graph` is a registered kernel, so it
/// cannot go in the method-hint table — that table must never shadow a working
/// name. Binding the *wrong* signature explicitly is the way to answer it, and
/// matches how the arena stubs already handle unavailable names.
fn register_scoped_graph_stub(engine: &mut Engine) {
    engine.register_fn("graph", |_: &mut GcSession, _: rhai::Map| {
        Err::<i64, _>(rt(FnError::new(
            0x86E,
            "`graph` takes no arguments. A second projection is pre-declared at the \
             CALL site and read by name: add `scopes: {agg: {nodeLabels: [..], \
             edgeTypes: [..]}}` to the projection config, then `gc.graph_named(\"agg\")`. \
             Scopes are built before the guest runs, so projection stays off the \
             guest's hot path.",
        )))
    });
}

fn register_arena_stubs(engine: &mut Engine) {
    engine
        .register_fn("add_node", |_: &mut GcSession, _: f64| {
            Err::<i64, _>(arena_unavailable("add_node"))
        })
        .register_fn("add_child", |_: &mut GcSession, _: i64, _: f64| {
            Err::<i64, _>(arena_unavailable("add_child"))
        })
        .register_fn("add_edge", |_: &mut GcSession, _: i64, _: i64| {
            Err::<(), _>(arena_unavailable("add_edge"))
        })
        .register_fn("neighbors", |_: &mut GcSession, _: i64| {
            Err::<Array, _>(arena_unavailable("neighbors"))
        })
        .register_fn("get_field", |_: &mut GcSession, _: i64| {
            Err::<f64, _>(arena_unavailable("get_field"))
        })
        .register_fn("set_field", |_: &mut GcSession, _: i64, _: f64| {
            Err::<(), _>(arena_unavailable("set_field"))
        })
        .register_fn("node_count", |_: &mut GcSession| {
            Err::<i64, _>(arena_unavailable("node_count"))
        })
        .register_fn("batch_new", |_: &mut GcSession, _: i64, _: i64, _: f64| {
            Err::<i64, _>(arena_unavailable("batch_new"))
        })
        .register_fn(
            "advance_batch",
            |_: &mut GcSession, _: i64, _: f64, _: bool| {
                Err::<(), _>(arena_unavailable("advance_batch"))
            },
        )
        .register_fn("visit_batch", |_: &mut GcSession, _: Array, _: f64| {
            Err::<(), _>(arena_unavailable("visit_batch"))
        })
        .register_fn("descend_batch", |_: &mut GcSession, _: Array| {
            Err::<Array, _>(arena_unavailable("descend_batch"))
        });
}

#[cfg(test)]
mod tests {
    use super::*;
    use uni_plugin_builtin::algorithms::graph_compute::{Arena, WorkBudget};

    /// The reachability contract for the Rhai surface (proposal §5.4 / §13.2).
    ///
    /// Every kernel the catalog declares `AllLoaders` must actually be
    /// registered on the engine. This is the assertion that was missing when
    /// `edge_count` shipped dispatchable over JSON but invisible to Rhai and
    /// Python guests — the same class of defect as issues #151 and #152.
    ///
    /// It is meaningful because `register_kernels` registers and reports from
    /// one declaration: it cannot claim a kernel it did not register.
    /// The exact probe a field report used to conclude scopes were absent.
    ///
    /// `gc.graph(#{..})` must name the feature that replaced it, not report an
    /// unknown function — the feature was present in the build they tested.
    #[test]
    fn probing_for_a_scoped_graph_names_the_feature_that_replaced_it() {
        let mut engine = Engine::new();
        register_graph_compute(&mut engine);
        let session = Arc::new(Mutex::new(AlgoSession::new(
            5,
            WorkBudget::from_edge_count(1000),
            Arena::new(1 << 20, 64),
        )));
        let mut scope = rhai::Scope::new();
        scope.push("gc", new_session(session, from_i64(0), Arc::default()));

        let err = engine
            .eval_with_scope::<rhai::Dynamic>(
                &mut scope,
                r#"gc.graph(#{nodeLabels: ["Lane"], edgeTypes: ["AGG"]})"#,
            )
            .expect_err("a scoped graph call must not silently succeed");
        let msg = err.to_string();
        for needle in ["scopes:", "graph_named"] {
            assert!(
                msg.contains(needle),
                "the refusal must name `{needle}`, got: {msg}"
            );
        }

        // The real accessor still works — the stub must not shadow it.
        assert!(
            engine
                .eval_with_scope::<rhai::Dynamic>(&mut scope, "gc.graph()")
                .is_ok(),
            "the zero-arg `graph()` must still resolve"
        );
    }

    /// A guest reaching for a composable-but-absent kernel gets the recipe.
    ///
    /// This is the discoverability gap three consecutive field reports fell into:
    /// asking for an operation that was already expressible. Rhai's own message
    /// says the name is unknown; it cannot say the operation is available by
    /// another spelling.
    #[test]
    fn an_unknown_kernel_name_earns_its_composition_recipe() {
        let mut engine = Engine::new();
        register_graph_compute(&mut engine);
        let session = Arc::new(Mutex::new(AlgoSession::new(
            5,
            WorkBudget::from_edge_count(1000),
            Arena::new(1 << 20, 64),
        )));
        let mut scope = rhai::Scope::new();
        scope.push("gc", new_session(session, from_i64(0), Arc::default()));

        for (call, needle) in [
            ("gc.select(1, 2, 3)", "ewise"),
            ("gc.arena_spmv(1, 2)", "arena_freeze"),
            ("gc.sub(1, 2)", "axpy"),
        ] {
            let err = engine
                .eval_with_scope::<rhai::Dynamic>(&mut scope, call)
                .expect_err("an absent kernel must fail");
            let msg = err.to_string();
            assert!(
                msg.contains(needle),
                "`{call}` must carry the composition recipe (expected `{needle}`), got: {msg}"
            );
        }

        // A name with no published composition keeps Rhai's own wording rather
        // than earning invented advice.
        let err = engine
            .eval_with_scope::<rhai::Dynamic>(&mut scope, "gc.wibble(1)")
            .expect_err("an unknown name still fails");
        assert!(
            err.to_string().contains("Function not found"),
            "an unkeyed name must not be given a made-up recipe: {err}"
        );
    }

    #[test]
    fn every_in_process_kernel_is_reachable_from_rhai() {
        let mut engine = Engine::new();
        engine.register_type_with_name::<GcSession>("GcSession");
        let registered: std::collections::HashSet<KernelId> =
            register_kernels(&mut engine).into_iter().collect();

        // `in_process`, not `all_loaders`: Rhai guests call `graph()` and
        // `graph_named(..)` on the session object, so the host-supplied bucket is
        // reachable here even though sandboxed guests get those handles in args.
        let missing: Vec<&str> = KernelId::in_process()
            .filter(|k| !registered.contains(k))
            .map(KernelId::op_name)
            .collect();
        assert!(
            missing.is_empty(),
            "kernels in the catalog but absent from the Rhai surface: {missing:?}"
        );
    }

    /// §7 acceptance: a **guest-authored** MCTS at usable absolute throughput.
    ///
    /// This is the check that #152 was ultimately about — a third party writing
    /// a real stateful graph algorithm in a sandboxed language and having it run
    /// fast enough to ship. The script owns the search: it grows the tree, and
    /// composes its own score from its own columns before each descent. The host
    /// owns only the loops that do `O(V+E)` work.
    ///
    /// The gate is **absolute**, not a ratio against native. A sandboxed guest
    /// is necessarily slower than in-process Rust, so a ratio cannot decide
    /// shippability — and the same guest measured 9.2x or 41.6x purely by choice
    /// of denominator (proposal §7 / §12).
    ///
    /// Timing note: this runs in a **debug** build, so the floor is set well
    /// below both the release bench figure (~1.9M rollouts/s) and §7's stated
    /// 250K target. It exists to fail on a *regression* — a kernel that becomes
    /// accidentally quadratic — not to reproduce the benchmark on a busy CI box.
    /// `mcts_batched_rhai` is the benchmark.
    #[test]
    fn guest_authored_mcts_meets_the_absolute_throughput_floor() {
        const ROLLOUTS: i64 = 4096;
        // Debug builds measure ~25K rollouts/s here; release measures ~1.9M and
        // §7's stated Rhai target is 250K. The floor is deliberately 5x below
        // the *debug* figure: the regression this guards against (a kernel
        // going accidentally quadratic) costs orders of magnitude, so a wide
        // margin loses no detection power and keeps the test off the flake
        // boundary. An earlier 25_000 sat exactly on the measured value and
        // duly failed at 24_749.
        const FLOOR_ROLLOUTS_PER_SEC: f64 = 5_000.0;

        let mut engine = Engine::new();
        register_graph_compute(&mut engine);
        let session = Arc::new(Mutex::new(AlgoSession::new(
            77,
            uni_plugin_builtin::algorithms::graph_compute::WorkBudget::new(500_000_000),
            uni_plugin_builtin::algorithms::graph_compute::Arena::new(64 << 20, 8192),
        )));
        let mut scope = rhai::Scope::new();
        scope.push(
            "sess",
            new_session(Arc::clone(&session), from_i64(0), Arc::default()),
        );
        scope.push("rollouts", ROLLOUTS);

        // The guest's own program. Note what it owns: the tree shape, the
        // exploration constant, the score formula, and the stopping rule.
        let script = r#"
            let arena = sess.arena_new(8191, 2);
            let root  = sess.arena_alloc(arena, 1);

            let visits = sess.arena_column(arena);
            let value  = sess.arena_column(arena);
            let score  = sess.arena_column(arena);

            // Grow a complete binary tree, level by level, from the root.
            let frontier = root;
            // 12 expansions: 1 + 2 + ... + 2^12 = 8191, exactly the capacity.
            for d in 0..12 {
                frontier = sess.arena_expand(arena, frontier, 2);
            }

            let c = 1.41;
            let n = 0;
            while n < rollouts {
                // Candidate-scoped scoring: only the root's children are
                // rescored, not all 8191 slots (proposal §12.7).
                let cand = sess.arena_candidates(arena, root);
                let v    = sess.arena_gather(arena, visits, cand);
                let w    = sess.arena_gather(arena, value, cand);

                // ucb = w/v + c * sqrt(ln(N)/v), composed by the guest from
                // ordinary tensor kernels — the host never sees the formula.
                let inv  = sess.recip(v);
                let mean = sess.ewise(w, inv, "mul", 0.0);
                let expl = sess.scale(inv, c * c);
                let ucb  = sess.ewise(mean, expl, "add", 0.0);
                sess.arena_scatter(arena, score, cand, ucb);

                let leaves = sess.arena_descend(arena, root, score, visits, true, 0.35);

                // Free the per-rollout intermediates. Without this the handle
                // cap fires within a few hundred rollouts — the arena bound
                // doing exactly its job against a leaky guest.
                sess.free(cand); sess.free(v);    sess.free(w);
                sess.free(inv);  sess.free(mean); sess.free(expl);
                sess.free(ucb);  sess.free(leaves);
                n += 1;
            }
            sess.arena_freeze(arena)
        "#;

        let started = std::time::Instant::now();
        let frozen = engine
            .eval_with_scope::<i64>(&mut scope, script)
            .expect("the guest program must run to completion");
        let elapsed = started.elapsed();

        // Correctness before timing: a script that errored into a fast no-op
        // must not read as a spectacular result.
        let s = session.lock();
        let visited = s
            .vertex_count(from_i64(frozen))
            .expect("the frozen arena is an ordinary graph");
        assert_eq!(visited, 8191, "the guest must have grown the whole tree");

        #[expect(clippy::cast_precision_loss, reason = "rollout count is small")]
        let rate = ROLLOUTS as f64 / elapsed.as_secs_f64();
        assert!(
            rate >= FLOOR_ROLLOUTS_PER_SEC,
            "guest-authored MCTS ran at {rate:.0} rollouts/s, below the {FLOOR_ROLLOUTS_PER_SEC:.0} floor"
        );
    }

    /// Issue #152: a Rhai guest reaching for an arena kernel must get a typed,
    /// actionable refusal — not Rhai's generic `Function not found`.
    ///
    /// Asserts both halves of the fix: the name *resolves* (so the guest is told
    /// about the host, not about its own spelling), and the refusal names the
    /// capability slice the guest would have to declare.
    #[test]
    fn arena_kernels_resolve_and_refuse_with_a_typed_slice_error() {
        let mut engine = Engine::new();
        register_graph_compute(&mut engine);

        let session = Arc::new(Mutex::new(AlgoSession::new(
            3,
            WorkBudget::from_graph_size(1, 0),
            Arena::new(1 << 16, 64),
        )));
        let mut scope = rhai::Scope::new();
        scope.push("sess", new_session(session, from_i64(0), Arc::default()));

        // Each call must reach the stub and be refused by slice. An unregistered
        // name would instead fail resolution with `Function not found`, which
        // does not mention the slice — so this asserts registration too.
        for script in [
            "sess.node_count()",
            "sess.add_node(1.0)",
            "sess.descend_batch([])",
            "sess.batch_new(4, 0, 1.0)",
        ] {
            let err = engine
                .eval_with_scope::<rhai::Dynamic>(&mut scope, script)
                .expect_err("an unprovided slice must fail, not return a value");
            let msg = err.to_string();
            assert!(
                msg.contains("graph-arena@1"),
                "`{script}` must be refused by slice, got: {msg}"
            );
        }
    }

    /// REQ-D1 (uniscape stepped-dynamics): elementwise comparison and
    /// conditional selection reported as a milestone blocker are in fact
    /// composable from shipped kernels, *through the Rhai guest surface* --
    /// not merely at the native `AlgoSession` trait level. The guest writes
    /// the recipe; the host evaluates every element.
    #[test]
    fn reqd1_compare_and_select_compose_through_the_rhai_guest_surface() {
        use std::collections::HashMap;
        use uni_algo::algo::GraphProjection;
        use uni_common::Value;

        let node_rows: Vec<HashMap<String, Value>> = (0..5u64)
            .map(|id| HashMap::from([("id".to_string(), Value::Int(id as i64))]))
            .collect();
        let edge_rows: Vec<HashMap<String, Value>> = vec![HashMap::from([
            ("source".to_string(), Value::Int(0)),
            ("target".to_string(), Value::Int(1)),
        ])];
        let graph = GraphProjection::from_rows(&node_rows, &edge_rows, None, false)
            .expect("projection builds");

        let mut engine = Engine::new();
        register_graph_compute(&mut engine);
        let session = Arc::new(Mutex::new(AlgoSession::new(
            91,
            uni_plugin_builtin::algorithms::graph_compute::WorkBudget::new(10_000_000),
            uni_plugin_builtin::algorithms::graph_compute::Arena::new(8 << 20, 1024),
        )));
        let g = session.lock().bind_graph(Arc::new(graph));

        let mut scope = rhai::Scope::new();
        scope.push("sess", new_session(Arc::clone(&session), g, Arc::default()));
        scope.push("g", to_i64(g));

        // a = [1,5,3,9,2], b = 4 everywhere; the guest wants `a > b` and
        // `select(a > b, a, b)`. Neither `gt` nor `select` is a kernel.
        let script = r#"
            fn load(sess, g, vals) {
                let m = sess.zero_map(g);
                for i in 0..vals.len() {
                    let f = sess.frontier(g, [i]);
                    m = sess.scatter(m, f, vals[i]);
                    sess.free(f);
                }
                m
            }
            let a = load(sess, g, [1.0, 5.0, 3.0, 9.0, 2.0]);
            let b = load(sess, g, [4.0, 4.0, 4.0, 4.0, 4.0]);

            // compare(a, b, "gt")
            let diff = sess.ewise(a, b, "axpy", -1.0);
            let hits = sess.map_to_set(diff, "gt", 0.0);
            let mask = sess.set_to_map(hits, 1.0);

            // select(mask, a, b) == b + mask * (a - b)
            let blend = sess.ewise(mask, diff, "mul", 0.0);
            let sel   = sess.ewise(b, blend, "add", 0.0);

            [sess.reduce_sum(mask), sess.reduce_sum(sel)]
        "#;

        let out = engine
            .eval_with_scope::<rhai::Array>(&mut scope, script)
            .expect("the guest recipe must run to completion");
        let mask_sum = out[0].clone().cast::<f64>();
        let sel_sum = out[1].clone().cast::<f64>();

        // a > b at slots 1 and 3 only.
        assert_eq!(mask_sum, 2.0, "the mask must select exactly two slots");
        // select -> [4, 5, 4, 9, 4] = 26.
        assert_eq!(sel_sum, 26.0, "select must blend max(a, b) elementwise");
    }

    /// T5 — op strings must be parsed by `graph_compute::op_parse`, never here.
    ///
    /// The reachability test above proves every catalogued *kernel* is
    /// registered; this proves no *vocabulary* has been re-triplicated. It is
    /// the only check that catches a newly added string enum being hand-matched
    /// in a loader, which is how the seven vocabularies drifted apart in the
    /// first place (two loaders said `bad overlap metric`, this one said
    /// `overlap: bad metric`).
    #[test]
    fn op_strings_are_not_parsed_in_this_loader() {
        // Split at the test module so this test's own needles do not trip it.
        let body = include_str!("graph_compute.rs")
            .split("#[cfg(test)]")
            .next()
            .expect("the file has a non-test prefix");
        for needle in [
            "=> Direction::",
            "=> EwiseOp::",
            "=> MapOp::",
            "=> Predicate::",
            "=> Norm::",
            "=> Semiring::",
            "=> OverlapMetric::",
        ] {
            assert!(
                !body.contains(needle),
                "`{needle}` appears in this loader — op strings belong in \
                 graph_compute::op_parse, so every surface rejects them identically"
            );
        }
    }

    /// T4 — the shared vocabulary is accepted, and a rejection carries the
    /// shared remedy, *through the Rhai guest surface*.
    ///
    /// A loader that re-introduced its own match could still accept every valid
    /// name; what it could not do is produce the recipe, because only
    /// `op_parse` knows the composition strings. That asymmetry is the
    /// regression barrier.
    #[test]
    fn rejections_reach_the_guest_with_a_composition_recipe() {
        use uni_plugin_builtin::algorithms::graph_compute::op_parse::OpFamily;

        let mut engine = Engine::new();
        register_graph_compute(&mut engine);
        let session = Arc::new(Mutex::new(AlgoSession::new(
            42,
            uni_plugin_builtin::algorithms::graph_compute::WorkBudget::new(1_000_000),
            uni_plugin_builtin::algorithms::graph_compute::Arena::new(1 << 20, 256),
        )));
        let mut scope = rhai::Scope::new();
        scope.push(
            "sess",
            new_session(Arc::clone(&session), from_i64(0), Arc::default()),
        );

        // `gt` is not an ewise op; the guest must be told how to get one.
        let err = engine
            .eval_with_scope::<i64>(&mut scope, r#"sess.ewise(0, 0, "gt", 0.0)"#)
            .expect_err("`gt` must be rejected")
            .to_string();
        assert!(err.contains("composable"), "no recipe in: {err}");
        assert!(
            err.contains("compare(a, b, \"gt\")"),
            "the guest must be pointed at the compare kernel: {err}"
        );
        for name in OpFamily::Ewise.valid_names() {
            assert!(err.contains(name), "missing valid op `{name}` in: {err}");
        }
    }

    /// Rewrites a published recipe into a Rhai call against `sess`.
    ///
    /// Recipes are written in prefix-call form (`map_apply(a, "scale", -1.0)`).
    /// Rhai's `register_fn` methods are callable both as `sess.f(..)` and as
    /// `f(sess, ..)`, so injecting the receiver as the first argument of every
    /// *kernel* call turns the published text into a runnable script without
    /// otherwise altering it. Only identifiers that are real kernels are
    /// rewritten, and text inside string literals is left alone.
    fn as_rhai_call(recipe: &str) -> String {
        let mut out = String::new();
        let mut ident = String::new();
        let mut in_quotes = false;
        for ch in recipe.chars() {
            if ch == '"' {
                in_quotes = !in_quotes;
                out.push_str(&ident);
                ident.clear();
                out.push(ch);
                continue;
            }
            if in_quotes {
                out.push(ch);
                continue;
            }
            if ch.is_alphanumeric() || ch == '_' {
                ident.push(ch);
                continue;
            }
            out.push_str(&ident);
            let is_kernel = KernelId::from_op_name(&ident).is_some();
            ident.clear();
            out.push(ch);
            if ch == '(' && is_kernel {
                out.push_str("sess, ");
            }
        }
        out.push_str(&ident);
        out
    }

    /// Every published recipe **resolves** through this loader, as written.
    ///
    /// `composition_recipes.rs` proves each recipe computes the right values,
    /// but it drives the *Rust* API — so it cannot see whether the published
    /// spelling resolves on a guest surface. It did not. `map_apply(a, "scale",
    /// -1.0)`, the verbatim text of the `neg` recipe, failed here with
    /// `Function not found: map_apply (GcSession, i64, &str, f64)`, because this
    /// loader registered `map_apply` at a fixed arity of four while the recipes
    /// — and PyO3, which defaults its trailing scalars — are written at the
    /// minimum arity. Every `map_apply`-quoting recipe was affected, and so was
    /// the reference's `map_apply(counts, "log")` UCT snippet.
    ///
    /// The assertion is deliberately *resolution*, not success: a recipe may
    /// legitimately raise a typed kernel error against synthetic fixture data,
    /// but it must never fail to resolve. Arity and name drift are exactly what
    /// `Function not found` reports, so that is what this forbids.
    #[test]
    fn every_published_recipe_resolves_through_this_loader() {
        use std::collections::HashMap;
        use uni_algo::algo::GraphProjection;
        use uni_common::Value;
        use uni_plugin_builtin::algorithms::graph_compute::op_parse::RECIPES;
        use uni_plugin_builtin::algorithms::graph_compute::session::GraphCompute;
        use uni_plugin_builtin::algorithms::graph_compute::value::DType;

        /// Recipes that are prose rather than a single expression.
        ///
        /// Keyed by first alias, and locked both ways below so a new prose
        /// recipe has to be added here consciously rather than silently
        /// escaping the check.
        const PROSE: &[&str] = &["pow"];

        let node_rows: Vec<HashMap<String, Value>> = (0..4u64)
            .map(|id| HashMap::from([("id".to_string(), Value::Int(id as i64))]))
            .collect();
        let graph =
            GraphProjection::from_rows(&node_rows, &[], None, false).expect("projection builds");

        let mut engine = Engine::new();
        register_graph_compute(&mut engine);
        let session = Arc::new(Mutex::new(AlgoSession::new(
            17,
            uni_plugin_builtin::algorithms::graph_compute::WorkBudget::new(1_000_000),
            uni_plugin_builtin::algorithms::graph_compute::Arena::new(1 << 20, 256),
        )));
        let g = session.lock().bind_graph(Arc::new(graph));

        // Constant `[V]` maps for the recipe placeholders, built through the
        // Rust API so the script under test does no setup of its own.
        let constant = |v: f64| -> i64 {
            let mut s = session.lock();
            let z = s.zero_map(g, DType::F64).expect("zero_map");
            let h = s
                .map_apply(
                    z,
                    uni_plugin_builtin::algorithms::graph_compute::session::MapOp::AxPlusB(0.0, v),
                )
                .expect("affine");
            to_i64(h)
        };
        let (a, b, m, x, lo, hi) = (
            constant(3.0),
            constant(2.0),
            constant(1.0),
            constant(3.0),
            constant(0.0),
            constant(5.0),
        );

        let mut scope = rhai::Scope::new();
        scope.push("sess", new_session(Arc::clone(&session), g, Arc::default()));
        scope.push("g", to_i64(g));
        for (name, h) in [
            ("a", a),
            ("b", b),
            ("m", m),
            ("x", x),
            ("lo", lo),
            ("hi", hi),
        ] {
            scope.push(name, h);
        }
        scope.push("theta", 0.5_f64);
        scope.push("c", 1.0_f64);
        scope.push("prop", "weight".to_string());

        let mut checked = 0_usize;
        for (keys, _, _, recipe) in RECIPES {
            let head = keys[0];
            if PROSE.contains(&head) {
                continue;
            }
            // A recipe may offer alternatives; each one must resolve.
            for alternative in recipe.split(" or ") {
                let script = as_rhai_call(alternative.trim());
                let err = engine
                    .eval_with_scope::<rhai::Dynamic>(&mut scope, &script)
                    .err()
                    .map(|e| e.to_string())
                    .unwrap_or_default();
                assert!(
                    !err.contains("Function not found"),
                    "the `{head}` recipe does not resolve through this loader.\n  \
                     published: {alternative}\n  as script: {script}\n  error:     {err}"
                );
                checked += 1;
            }
        }
        assert!(checked >= 10, "only {checked} recipes exercised");

        // The other half of the lock: a name may not claim to be prose unless it
        // is still published, or this list rots the way a stale skip always does.
        let published: std::collections::HashSet<&str> =
            RECIPES.iter().map(|(keys, _, _, _)| keys[0]).collect();
        for name in PROSE {
            assert!(
                published.contains(name),
                "`{name}` is listed as prose but is no longer a published recipe"
            );
        }
    }

    /// Every arity declared in `OPTIONAL_ARITIES` resolves on this loader.
    ///
    /// The recipe proof above covers the arities the *current* recipes happen to
    /// use. This covers the declared contract itself, so adding a default to
    /// PyO3 (or an entry to the table) without registering the matching Rhai
    /// overload fails here rather than in a guest author's script.
    #[test]
    fn every_declared_optional_arity_resolves_through_this_loader() {
        use std::collections::HashMap;
        use uni_algo::algo::GraphProjection;
        use uni_common::Value;
        use uni_plugin_builtin::algorithms::graph_compute::kernel_id::OPTIONAL_ARITIES;
        use uni_plugin_builtin::algorithms::graph_compute::session::GraphCompute;
        use uni_plugin_builtin::algorithms::graph_compute::value::DType;

        // One probe per declared (kernel, arity). Locked both ways below.
        let probes: &[(KernelId, usize, &str)] = &[
            (KernelId::MapApply, 2, r#"map_apply(sess, m, "log")"#),
            (KernelId::MapApply, 3, r#"map_apply(sess, m, "scale", 2.0)"#),
            (
                KernelId::MapApply,
                4,
                r#"map_apply(sess, m, "affine", 1.0, 0.5)"#,
            ),
            (KernelId::Ewise, 3, r#"ewise(sess, m, m, "add")"#),
            (KernelId::Ewise, 4, r#"ewise(sess, m, m, "axpy", -1.0)"#),
            (KernelId::ZeroMap, 1, "zero_map(sess, g)"),
            (KernelId::ZeroMap, 2, r#"zero_map(sess, g, "i64")"#),
            (KernelId::Emit, 1, r#"emit(sess, #{"score": m})"#),
            (KernelId::Emit, 2, r#"emit(sess, "score", m)"#),
            (KernelId::Sample, 1, "sample(sess, m)"),
            (KernelId::Sample, 2, "sample(sess, m, 1)"),
            (KernelId::Sample, 3, "sample(sess, m, 1, 0)"),
            (KernelId::SampleEdges, 1, "sample_edges(sess, e)"),
            (KernelId::SampleEdges, 2, "sample_edges(sess, e, 1)"),
            (KernelId::SampleEdges, 3, "sample_edges(sess, e, 1, 0)"),
            (KernelId::AllPairsOverlap, 1, "all_pairs_overlap(sess, g)"),
            (
                KernelId::AllPairsOverlap,
                2,
                r#"all_pairs_overlap(sess, g, "count")"#,
            ),
            (
                KernelId::AllPairsOverlap,
                3,
                r#"all_pairs_overlap(sess, g, "count", "adjacent")"#,
            ),
            (
                KernelId::AllPairsOverlap,
                4,
                r#"all_pairs_overlap(sess, g, "count", "adjacent", 0)"#,
            ),
            (KernelId::RandomWalks, 3, "random_walks(sess, g, [], 2)"),
            (KernelId::RandomWalks, 4, "random_walks(sess, g, [], 2, 1)"),
            (
                KernelId::RandomWalks,
                5,
                "random_walks(sess, g, [], 2, 1, 1.0)",
            ),
            (
                KernelId::RandomWalks,
                6,
                "random_walks(sess, g, [], 2, 1, 1.0, 1.0)",
            ),
            (
                KernelId::RandomWalks,
                7,
                "random_walks(sess, g, [], 2, 1, 1.0, 1.0, 0)",
            ),
        ];

        let node_rows: Vec<HashMap<String, Value>> = (0..3u64)
            .map(|id| HashMap::from([("id".to_string(), Value::Int(id as i64))]))
            .collect();
        let graph =
            GraphProjection::from_rows(&node_rows, &[], None, false).expect("projection builds");
        let mut engine = Engine::new();
        register_graph_compute(&mut engine);
        let session = Arc::new(Mutex::new(AlgoSession::new(
            19,
            uni_plugin_builtin::algorithms::graph_compute::WorkBudget::new(1_000_000),
            uni_plugin_builtin::algorithms::graph_compute::Arena::new(1 << 20, 256),
        )));
        let g = session.lock().bind_graph(Arc::new(graph));
        let m = to_i64(session.lock().zero_map(g, DType::F64).expect("zero_map"));
        // An `[E]` tensor for `sample_edges`; empty here, which is enough to
        // resolve the call — this test is about arity, not about the data.
        let e = to_i64(session.lock().edge_weights(g).expect("edge_weights"));

        let mut scope = rhai::Scope::new();
        scope.push("sess", new_session(Arc::clone(&session), g, Arc::default()));
        scope.push("m", m);
        scope.push("e", e);
        scope.push("g", to_i64(g));

        for (kernel, arity, script) in probes {
            let err = engine
                .eval_with_scope::<rhai::Dynamic>(&mut scope, script)
                .err()
                .map(|e| e.to_string())
                .unwrap_or_default();
            assert!(
                !err.contains("Function not found"),
                "`{}` is declared callable at arity {arity} but does not resolve: {script}\n  {err}",
                kernel.op_name()
            );
        }

        // Negative control: the check above is only meaningful if a genuinely
        // short call *is* rejected. `compare` declares no optional tail, so
        // three arguments (receiver plus two) must fail to resolve — if this
        // ever stops holding, the assertions above test nothing.
        let control = engine
            .eval_with_scope::<rhai::Dynamic>(&mut scope, "compare(sess, m, m)")
            .err()
            .map(|e| e.to_string())
            .unwrap_or_default();
        assert!(
            control.contains("Function not found"),
            "a too-short call must surface as `Function not found` for this test to have \
             teeth, got: {control}"
        );

        // Two-way lock against the shared declaration.
        for (kernel, arities) in OPTIONAL_ARITIES {
            for arity in *arities {
                assert!(
                    probes.iter().any(|(k, a, _)| k == kernel && a == arity),
                    "`{}` declares arity {arity} with no probe here",
                    kernel.op_name()
                );
            }
        }
        for (kernel, arity, _) in probes {
            let declared = OPTIONAL_ARITIES
                .iter()
                .find(|(k, _)| k == kernel)
                .is_some_and(|(_, arities)| arities.contains(arity));
            assert!(
                declared,
                "`{}` is probed at arity {arity} but the shared table does not declare it",
                kernel.op_name()
            );
        }
    }

    /// The budget accessors work *from a guest script*, which is the surface the
    /// published example uses.
    ///
    /// The reachability test above proves the names are registered; it never
    /// invokes them. A registered-but-broken kernel would pass it, and the docs
    /// ship `let left = gc.work_remaining();` as a runnable snippet, so this is
    /// the test that actually backs it.
    #[test]
    fn budget_accessors_work_from_a_guest_script() {
        use std::collections::HashMap;
        use uni_algo::algo::GraphProjection;
        use uni_common::Value;

        let node_rows: Vec<HashMap<String, Value>> = (0..6u64)
            .map(|id| HashMap::from([("id".to_string(), Value::Int(id as i64))]))
            .collect();
        let graph =
            GraphProjection::from_rows(&node_rows, &[], None, false).expect("projection builds");

        let mut engine = Engine::new();
        register_graph_compute(&mut engine);
        let session = Arc::new(Mutex::new(AlgoSession::new(
            31,
            uni_plugin_builtin::algorithms::graph_compute::WorkBudget::new(1_000),
            uni_plugin_builtin::algorithms::graph_compute::Arena::new(1 << 20, 256),
        )));
        let g = session.lock().bind_graph(Arc::new(graph));
        let mut scope = rhai::Scope::new();
        scope.push("sess", new_session(Arc::clone(&session), g, Arc::default()));
        scope.push("g", to_i64(g));

        let script = r#"
            let total = sess.work_budget();
            let before = sess.work_spent();
            // Reading is free: three more polls must not move the meter.
            sess.work_remaining();
            sess.work_budget();
            sess.work_spent();
            let still = sess.work_spent();

            // A real kernel does move it.
            sess.degrees(g, "out");
            let after = sess.work_spent();
            [total, before, still, after, sess.work_remaining()]
        "#;
        let out = engine
            .eval_with_scope::<rhai::Array>(&mut scope, script)
            .expect("the guest can read its own budget");
        let v: Vec<f64> = out.into_iter().map(|d| d.cast::<f64>()).collect();
        let (total, before, still, after, remaining) = (v[0], v[1], v[2], v[3], v[4]);

        assert_eq!(total, 1_000.0, "the guest sees the budget it was given");
        assert_eq!(before, 0.0, "nothing charged yet");
        assert_eq!(still, 0.0, "reading the meter must not charge it");
        assert_eq!(after, 6.0, "degrees charges one unit per vertex");
        assert_eq!(remaining, total - after, "remaining is total minus spent");
    }

    /// The trace reaches a **Rhai** guest — the surface that motivated the hook
    /// site in the first place.
    ///
    /// `check_epoch_and_kind` was chosen over the JSON dispatcher precisely
    /// because the Rhai loader never goes through the dispatcher: `GcSession`
    /// calls `AlgoSession` directly. That reasoning went untested when the
    /// feature landed, which is the same gap as shipping a kernel whose only
    /// coverage is a `hasattr` check.
    ///
    /// Also asserts the suffix survives `rt()`'s `GraphCompute: {e}` wrapping
    /// into `EvalAltResult`, since that is what a guest author actually sees.
    #[test]
    fn the_trace_reaches_a_rhai_guest() {
        use uni_plugin_builtin::algorithms::graph_compute::force_tracing_for_test;

        let mut engine = Engine::new();
        register_graph_compute(&mut engine);
        let session = Arc::new(Mutex::new(AlgoSession::new(
            57,
            uni_plugin_builtin::algorithms::graph_compute::WorkBudget::new(100_000),
            uni_plugin_builtin::algorithms::graph_compute::Arena::new(1 << 20, 256),
        )));
        let mut scope = rhai::Scope::new();
        scope.push(
            "sess",
            new_session(Arc::clone(&session), from_i64(0), Arc::default()),
        );

        force_tracing_for_test(true);
        // Allocate, free, then use — a use-after-free from guest code.
        let err = engine
            .eval_with_scope::<i64>(
                &mut scope,
                r#"
                    let a = sess.arena_new(8, 2);
                    let s = sess.arena_alloc(a, 3);
                    sess.free(s);
                    sess.set_len(s)
                "#,
            )
            .expect_err("using a freed handle must fail")
            .to_string();

        assert!(
            err.contains("gc-trace"),
            "a Rhai guest's handle error must carry the trace: {err}"
        );
        assert!(
            err.contains("GraphCompute"),
            "and it must survive the loader's error wrapping: {err}"
        );
    }
}
