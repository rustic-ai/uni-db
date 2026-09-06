// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Loader-agnostic JSON kernel dispatch + per-CALL session registry.
//!
//! The in-process Rhai loader hands a guest a [`GcSession`](super::session)
//! object with native methods. The sandboxed loaders (WASM / Extism) cannot pass
//! a Rust object across the boundary, so they instead expose a *single* host
//! function that marshals one kernel call as JSON: the guest sends
//! `{op, session, handles, scalars}` and receives `{handle | scalar | error}`.
//! This collapses the whole kernel catalog to one host import per loader
//! (proposal §4.5) — only handles and scalars ever cross, exactly the property
//! that makes the design portable across loaders.
//!
//! A [`GraphComputeRegistry`] owns the per-CALL [`AlgoSession`]s keyed by an
//! opaque session id. The adapter [`opens`](GraphComputeRegistry::open) a session
//! before invoking the guest, passes the id in, and [`closes`] it after — so
//! concurrent CALLs never share a session, and a stateless pooled host function
//! resolves the right session by id on every call.
//!
//! [`closes`]: GraphComputeRegistry::close
//
// Rust guideline compliant

use std::collections::HashMap;
use std::sync::Arc;

use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use uni_common::core::id::Vid;
use uni_plugin::errors::FnError;

use super::handle::Handle;
use super::kernel_id::KernelId;
use super::session::{
    AlgoSession, CmpOp, Direction, EndpointOp, EwiseOp, GraphArenaCompute, GraphCompute, MapOp,
    Norm, OverlapMetric, Predicate, ReduceOp, Semiring,
};
use super::value::Scalar;

/// Serde default for the node2vec bias params (unbiased = 1.0).
fn one_f64() -> f64 {
    1.0
}

/// One kernel call from a guest, deserialized from the request JSON.
///
/// A single flat struct with all-optional operands keeps the wire format simple
/// and identical across loaders; each `op` reads only the fields it needs.
#[derive(Debug, Deserialize)]
pub struct KernelRequest {
    /// The session id returned by [`GraphComputeRegistry::open`].
    pub session: u64,
    /// The kernel name (`"frontier"`, `"spmv"`, …).
    pub op: String,
    /// Primary handle operand (graph / map / set), as a packed `i64`.
    #[serde(default)]
    pub g: i64,
    /// Second handle operand.
    #[serde(default)]
    pub a: i64,
    /// Third handle operand.
    #[serde(default)]
    pub b: i64,
    /// Fourth handle operand (e.g. the edge mask of `expand_masked`); `0` = none.
    #[serde(default)]
    pub c: i64,
    /// A string enum operand (direction / predicate / norm / op).
    #[serde(default)]
    pub s: String,
    /// A second string enum operand (spmv direction alongside the semiring).
    #[serde(default)]
    pub s2: String,
    /// A scalar operand.
    #[serde(default)]
    pub f: f64,
    /// A second scalar operand (e.g. the `b` of `map_apply` `AxPlusB(a, b)`).
    #[serde(default)]
    pub f2: f64,
    /// A count operand (the `k` of `topk`, the `bucket` of `next_bucket`).
    #[serde(default)]
    pub k: u32,
    /// A boolean operand (the `want_max` of `arg_extreme`).
    #[serde(default)]
    pub want_max: bool,
    /// Walk length (`random_walks`).
    #[serde(default)]
    pub wl: u32,
    /// Walks per node (`random_walks`).
    #[serde(default)]
    pub wn: u32,
    /// node2vec return bias `p` (`random_walks`).
    #[serde(default = "one_f64")]
    pub p: f64,
    /// node2vec in-out bias `q` (`random_walks`).
    #[serde(default = "one_f64")]
    pub q: f64,
    /// Deterministic RNG seed (`random_walks`, `sample`).
    #[serde(default)]
    pub seed: u64,
    /// Iteration counter mixed into the `sample` counter-hash stream.
    #[serde(default)]
    pub iter: u64,
    /// Seed vertex ids (for `frontier`).
    #[serde(default)]
    pub seeds: Vec<i64>,
    /// `interp` x-breakpoints (strictly increasing).
    #[serde(default)]
    pub xs: Vec<f64>,
    /// `interp` y-breakpoints, parallel to [`Self::xs`].
    #[serde(default)]
    pub ys: Vec<f64>,
    /// Column name (for a single-column `emit`).
    #[serde(default)]
    pub name: String,
    /// Column names for a batch `emit`, paired positionally with [`Self::handles`].
    ///
    /// Empty means the single-column form (`name` + `g`) — which is how every
    /// guest built before the batch form existed, so old callers are unaffected.
    #[serde(default)]
    pub names: Vec<String>,
    /// Tensor handles for a batch `emit`, paired positionally with [`Self::names`].
    #[serde(default)]
    pub handles: Vec<i64>,
    /// Arena slot capacity (`arena_new`) or allocation count (`arena_alloc`).
    #[serde(default)]
    pub cap: u32,
    /// Arena per-slot child slack (`arena_new`).
    #[serde(default)]
    pub branch: u32,
    /// Arena state-column index (the score column for `arena_descend`).
    #[serde(default)]
    pub col: u32,
    /// Second arena state-column index (the visit column for `arena_descend`).
    #[serde(default)]
    pub col2: u32,
}

/// The result of a kernel call, serialized to the response JSON.
#[derive(Debug, Serialize, Deserialize, PartialEq)]
#[serde(tag = "t", content = "v")]
pub enum KernelResponse {
    /// A handle result (packed `i64`).
    #[serde(rename = "h")]
    Handle(i64),
    /// A scalar (`f64`) result.
    #[serde(rename = "f")]
    Float(f64),
    /// A boolean result.
    #[serde(rename = "b")]
    Bool(bool),
    /// A `(vertexId, scalar)` result (`arg_extreme`).
    #[serde(rename = "vs")]
    VidScalar {
        /// The external vertex id of the extremum.
        vid: i64,
        /// The extremum's scalar value.
        f: f64,
    },
    /// A ranked `(vertexId, scalar)` list result (`topk`).
    #[serde(rename = "ps")]
    Pairs(Vec<(i64, f64)>),
    /// A no-value result (`free`, `emit`).
    #[serde(rename = "u")]
    Unit,
    /// A typed error `{code, message}`.
    #[serde(rename = "e")]
    Err {
        /// The GraphCompute error code (proposal §12).
        code: u32,
        /// The human-readable error message.
        message: String,
    },
}

fn to_i64(h: Handle) -> i64 {
    #[expect(
        clippy::cast_possible_wrap,
        reason = "opaque handle round-trips bit-exact"
    )]
    let v = h.as_u64() as i64;
    v
}

fn from_i64(v: i64) -> Handle {
    #[expect(clippy::cast_sign_loss, reason = "opaque handle round-trips bit-exact")]
    let bits = v as u64;
    Handle::from_u64(bits)
}

/// A per-process registry of live GraphCompute sessions keyed by session id.
///
/// Shared (behind an `Arc`) by a loader's single graph host function and its
/// per-CALL algorithm adapter. See the [module docs](self) for the lifecycle.
///
/// The session id is **unguessable** (drawn from a CSPRNG via UUIDv4), not a
/// sequential counter: on the JSON surface the guest supplies the id on every
/// call, so a sequential id would let one concurrent CALL enumerate and target
/// another CALL's session (read its graph, drain its budget, free its handles).
/// A ~60-bit-entropy random key closes that cross-session hole (review H2).
#[derive(Debug, Default)]
pub struct GraphComputeRegistry {
    /// A short-lived map lock guards only lookup/insert/remove; each session sits
    /// behind its own `Mutex` so one guest's O(E) kernel never stalls another
    /// concurrent CALL's session (proposal §5.1 / E6).
    sessions: Mutex<HashMap<u64, Arc<Mutex<AlgoSession>>>>,
}

impl GraphComputeRegistry {
    /// Creates an empty registry.
    #[must_use]
    pub fn new() -> Self {
        Self {
            sessions: Mutex::new(HashMap::new()),
        }
    }

    /// Registers `session`, returning its unguessable opaque id for the guest.
    ///
    /// The id is a CSPRNG-drawn `u64` (from UUIDv4); the loop retries on the
    /// astronomically unlikely collision or zero so the returned id is always
    /// live-unique and non-zero.
    pub fn open(&self, session: AlgoSession) -> u64 {
        let mut sessions = self.sessions.lock();
        loop {
            let id = uuid::Uuid::new_v4().as_u64_pair().0;
            if id != 0 && !sessions.contains_key(&id) {
                sessions.insert(id, Arc::new(Mutex::new(session)));
                return id;
            }
        }
    }

    /// Removes and returns the session with `id`, if present.
    ///
    /// The adapter calls this after the guest returns to read the emitted
    /// columns and drop the session (freeing every handle). The guest has
    /// returned, so no `call` still holds a clone and the `Arc` unwraps cleanly.
    pub fn close(&self, id: u64) -> Option<AlgoSession> {
        let arc = self.sessions.lock().remove(&id)?;
        Arc::try_unwrap(arc).ok().map(Mutex::into_inner)
    }

    /// Dispatches one kernel call and returns its typed response.
    ///
    /// A missing session or a kernel error is returned as [`KernelResponse::Err`]
    /// rather than panicking, so a hostile guest cannot crash the worker
    /// (proposal §5.4).
    #[must_use]
    pub fn call(&self, req: &KernelRequest) -> KernelResponse {
        // Hold the map lock only long enough to clone the session's Arc, then
        // release it so other sessions run concurrently (E6).
        let session = {
            let sessions = self.sessions.lock();
            let Some(arc) = sessions.get(&req.session) else {
                return KernelResponse::Err {
                    code: 0x863,
                    message: format!("unknown or closed session {}", req.session),
                };
            };
            Arc::clone(arc)
        };
        let mut guard = session.lock();
        // Panic isolation (proposal §5.4): a defensive panic in a kernel becomes
        // a typed error, not a worker crash, so a hostile guest driving the JSON
        // surface cannot bring down the process. parking_lot locks don't poison,
        // so the session mutex releases cleanly on unwind; the session is
        // per-CALL and discarded after, so any partial state is irrelevant.
        let dispatched = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            Self::dispatch(&mut guard, req)
        }));
        match dispatched {
            Ok(Ok(resp)) => resp,
            Ok(Err(e)) => KernelResponse::Err {
                code: e.code,
                message: e.message,
            },
            Err(_) => KernelResponse::Err {
                code: 0x86D,
                message: "GraphCompute: kernel panicked (isolated)".to_owned(),
            },
        }
    }

    /// Dispatches one kernel call as JSON in and JSON out.
    ///
    /// The single entry point a stateless loader host function calls. A malformed
    /// request or a serialization failure is reported in-band as an error
    /// response, never a panic.
    #[must_use]
    pub fn call_json(&self, request_json: &str) -> String {
        let resp = match serde_json::from_str::<KernelRequest>(request_json) {
            Ok(req) => self.call(&req),
            Err(e) => KernelResponse::Err {
                code: 0x802,
                message: format!("bad kernel request json: {e}"),
            },
        };
        serde_json::to_string(&resp).unwrap_or_else(|e| {
            format!("{{\"t\":\"e\",\"v\":{{\"code\":2,\"message\":\"encode: {e}\"}}}}")
        })
    }

    /// Runs `f` against a live session, for the **typed** host ABI.
    ///
    /// The JSON path ([`call_json`](Self::call_json)) exists so one host import
    /// can carry the whole kernel catalog; it costs ~2 us per crossing in
    /// encode/decode, which dominates a batched kernel's actual native work by
    /// **32x** (proposal §12, §5.3). Typed host functions call through here
    /// instead and pay only the handle.
    ///
    /// Panic isolation matches the JSON path exactly: a panicking kernel becomes
    /// a typed `0x86D`, never an unwind across the guest boundary.
    ///
    /// # Errors
    /// Returns `0x86E` for an unknown session id, `0x86D` for an isolated panic,
    /// or whatever typed error the kernel itself produced.
    pub fn with_session<R>(
        &self,
        session: u64,
        f: impl FnOnce(&mut AlgoSession) -> Result<R, FnError>,
    ) -> Result<R, FnError> {
        let cell = {
            let map = self.sessions.lock();
            map.get(&session).map(Arc::clone)
        };
        let Some(cell) = cell else {
            return Err(FnError::new(
                0x86E,
                format!("graph-arena: unknown session id {session}"),
            ));
        };
        let mut guard = cell.lock();
        // Same isolation rationale as `call`: parking_lot locks don't poison, so
        // the mutex releases cleanly on unwind, and the session is per-CALL and
        // discarded afterwards, so partial state cannot outlive the invocation.
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| f(&mut guard))).unwrap_or_else(
            |_| {
                Err(FnError::new(
                    0x86D,
                    "graph-arena: kernel panicked; isolated by the host",
                ))
            },
        )
    }

    /// Maps one request to a kernel invocation on `session`.
    ///
    /// The op name is resolved to a [`KernelId`] first, so the match below is
    /// over a closed enum with **no wildcard arm**: a kernel added to the
    /// catalog fails to compile until it is dispatched here. Before this, op
    /// names were bare string literals and a missing arm was a runtime "unknown
    /// op" that nothing could catch (see `kernel_id` module docs).
    fn dispatch(session: &mut AlgoSession, req: &KernelRequest) -> Result<KernelResponse, FnError> {
        let h = |x: Handle| KernelResponse::Handle(to_i64(x));
        let Some(kernel) = KernelId::from_op_name(req.op.as_str()) else {
            return Err(super::unresolved_op_error(req.op.as_str()));
        };
        match kernel {
            KernelId::WorkBudget => Ok(KernelResponse::Float(session.work_budget()?)),
            KernelId::WorkSpent => Ok(KernelResponse::Float(session.work_spent()?)),
            KernelId::WorkRemaining => Ok(KernelResponse::Float(session.work_remaining()?)),
            KernelId::VertexCount => Ok(KernelResponse::Float(
                session.vertex_count(from_i64(req.g))? as f64,
            )),
            KernelId::Frontier => {
                let vids: Vec<Vid> = req
                    .seeds
                    .iter()
                    .map(|&i| {
                        #[expect(clippy::cast_sign_loss, reason = "vertex ids are non-negative")]
                        let u = i as u64;
                        Vid::new(u)
                    })
                    .collect();
                session.frontier(from_i64(req.g), &vids).map(h)
            }
            KernelId::ReachFixpoint => {
                let vids: Vec<Vid> = req
                    .seeds
                    .iter()
                    .map(|&i| {
                        #[expect(clippy::cast_sign_loss, reason = "vertex ids are non-negative")]
                        let u = i as u64;
                        Vid::new(u)
                    })
                    .collect();
                session
                    .reach_fixpoint(from_i64(req.g), &vids, Direction::parse(&req.s)?)
                    .map(h)
            }
            KernelId::Degrees => session
                .degrees(from_i64(req.g), Direction::parse(&req.s)?)
                .map(h),
            KernelId::VertexIds => session.vertex_ids(from_i64(req.g)).map(h),
            KernelId::SetToMap => session
                .set_to_map(from_i64(req.g), Scalar::F64(req.f))
                .map(h),
            KernelId::MapToSet => {
                let pred = Predicate::parse(&req.s, req.f)?;
                session.map_to_set(from_i64(req.g), pred).map(h)
            }
            KernelId::Recip => session.map_apply(from_i64(req.g), MapOp::Recip).map(h),
            KernelId::Scale => session
                .map_apply(from_i64(req.g), MapOp::Scale(req.f))
                .map(h),
            KernelId::Normalize => {
                let norm = Norm::parse(&req.s)?;
                session
                    .map_apply(from_i64(req.g), MapOp::Normalize(norm))
                    .map(h)
            }
            KernelId::Ewise => {
                let op = EwiseOp::parse(&req.s, req.f)?;
                session.ewise(from_i64(req.a), from_i64(req.b), op).map(h)
            }
            KernelId::Compare => {
                let op = CmpOp::parse(&req.s)?;
                session.compare(from_i64(req.a), from_i64(req.b), op).map(h)
            }
            KernelId::Spmv => session
                .spmv(
                    from_i64(req.g),
                    from_i64(req.a),
                    Semiring::parse(&req.s)?,
                    Direction::parse(&req.s2)?,
                    None,
                )
                .map(h),
            KernelId::ZeroMap => {
                // `s == "i64"` seeds an exact path-counting run; default f64.
                let ty = if req.s == "i64" {
                    super::value::DType::I64
                } else {
                    super::value::DType::F64
                };
                session.zero_map(from_i64(req.g), ty).map(h)
            }
            KernelId::MapApply => session
                .map_apply(from_i64(req.g), MapOp::parse(&req.s, req.f, req.f2)?)
                .map(h),
            KernelId::EdgeCount => Ok(KernelResponse::Float(
                session.edge_count(from_i64(req.g))? as f64
            )),
            KernelId::Scatter => session
                .scatter(from_i64(req.a), from_i64(req.b), Scalar::F64(req.f))
                .map(h),
            KernelId::ArgExtreme => {
                let (vid, s) = session.arg_extreme(from_i64(req.g), req.want_max)?;
                #[expect(clippy::cast_possible_wrap, reason = "vids fit i64 in practice")]
                let vid = vid.as_u64() as i64;
                Ok(KernelResponse::VidScalar { vid, f: s.as_f64() })
            }
            KernelId::RandomWalks => {
                let seeds: Vec<Vid> = req
                    .seeds
                    .iter()
                    .map(|&i| {
                        #[expect(clippy::cast_sign_loss, reason = "vertex ids are non-negative")]
                        let u = i as u64;
                        Vid::new(u)
                    })
                    .collect();
                session
                    .random_walks(
                        from_i64(req.g),
                        req.wl as usize,
                        req.wn as usize,
                        &seeds,
                        req.p,
                        req.q,
                        req.seed,
                    )
                    .map(h)
            }
            KernelId::Sample => session.sample(from_i64(req.g), req.seed, req.iter).map(h),
            KernelId::WalkVisitCounts => session
                .walk_visit_counts(from_i64(req.a), from_i64(req.g))
                .map(h),
            KernelId::Rekey => session.rekey(from_i64(req.a), from_i64(req.g)).map(h),
            KernelId::Interp => session.interp(from_i64(req.a), &req.xs, &req.ys).map(h),
            KernelId::EdgeFromNodes => {
                let op = EndpointOp::parse(&req.s)?;
                session
                    .edge_from_nodes(from_i64(req.g), from_i64(req.a), op)
                    .map(h)
            }
            KernelId::EmitWalks => session
                .emit_walks(from_i64(req.g))
                .map(|()| KernelResponse::Unit),
            KernelId::NeighborhoodOverlap => {
                // #233 Tier 1: an empty `seeds` used to default to vertex 0,
                // and the overlap computed from that unrelated vertex was
                // returned as if it were the caller's.
                let Some(source) = req.seeds.first().copied() else {
                    return Err(FnError::new(
                        0x86F,
                        "graph-arena: NeighborhoodOverlap requires a source vertex in `seeds`",
                    ));
                };
                #[expect(clippy::cast_sign_loss, reason = "vertex ids are non-negative")]
                let source = Vid::new(source as u64);
                session
                    .neighborhood_overlap(from_i64(req.g), source, OverlapMetric::parse(&req.s)?)
                    .map(h)
            }
            KernelId::AllPairsOverlap => {
                let spec = match req.s2.as_str() {
                    // `topk` reads the count from `k`; anything else is all pairs.
                    "topk" => super::session::PairSpec::TopKCandidates(req.k),
                    _ => super::session::PairSpec::AdjacentPairs,
                };
                session
                    .all_pairs_overlap(from_i64(req.g), spec, OverlapMetric::parse(&req.s)?)
                    .map(h)
            }
            KernelId::EmitPairs => session
                .emit_pairs(from_i64(req.g))
                .map(|()| KernelResponse::Unit),
            KernelId::NextBucket => session.next_bucket(from_i64(req.g), req.f, req.k).map(h),
            KernelId::Topk => {
                let ranked = session.topk(from_i64(req.g), req.k)?;
                #[expect(clippy::cast_possible_wrap, reason = "vids fit i64 in practice")]
                let pairs = ranked
                    .into_iter()
                    .map(|(vid, s)| (vid.as_u64() as i64, s.as_f64()))
                    .collect();
                Ok(KernelResponse::Pairs(pairs))
            }
            KernelId::Expand => session
                .expand(
                    from_i64(req.g),
                    from_i64(req.a),
                    Direction::parse(&req.s)?,
                    Some(from_i64(req.b)),
                )
                .map(h),
            KernelId::SetUnion => session.set_union(from_i64(req.a), from_i64(req.b)).map(h),
            KernelId::SetDiff => session.set_diff(from_i64(req.a), from_i64(req.b)).map(h),
            // Mode A edge kernels (proposal §5). Handle `b == 0` means "no
            // exclude mask" for expand_masked; the edge mask rides `c`.
            KernelId::EdgeWeights => session.edge_weights(from_i64(req.g)).map(h),
            KernelId::EdgeProperty => session.edge_property(from_i64(req.g), &req.name).map(h),
            KernelId::NodeProperty => session.node_property(from_i64(req.g), &req.name).map(h),
            KernelId::EdgesAll => session.edges_all(from_i64(req.g)).map(h),
            KernelId::SegmentedReduce => session
                .segmented_reduce(from_i64(req.a), from_i64(req.b))
                .map(h),
            KernelId::SampleEdges => session
                .sample_edges(from_i64(req.g), req.seed, req.iter)
                .map(h),
            KernelId::SampleEdgesUndirected => session
                .sample_edges_undirected(from_i64(req.g), from_i64(req.a), req.seed, req.iter)
                .map(h),
            KernelId::EdgeSetLen => session
                .edge_set_len(from_i64(req.g))
                .map(|v| KernelResponse::Float(v as f64)),
            KernelId::EdgeMaskWindow => session
                .edge_mask_window(from_i64(req.g), req.f, req.f2)
                .map(h),
            KernelId::EdgeIntersect => session
                .edge_intersect(from_i64(req.a), from_i64(req.b))
                .map(h),
            KernelId::EdgeUnion => session.edge_union(from_i64(req.a), from_i64(req.b)).map(h),
            KernelId::ExpandMasked => session
                .expand_masked(
                    from_i64(req.g),
                    from_i64(req.a),
                    Direction::parse(&req.s)?,
                    if req.b == 0 {
                        None
                    } else {
                        Some(from_i64(req.b))
                    },
                    from_i64(req.c),
                )
                .map(h),
            KernelId::ExpandSampled => session
                .expand_sampled(
                    from_i64(req.g),
                    from_i64(req.a),
                    Direction::parse(&req.s)?,
                    if req.b == 0 {
                        None
                    } else {
                        Some(from_i64(req.b))
                    },
                    from_i64(req.c),
                    req.seed,
                    req.iter,
                )
                .map(h),
            KernelId::SpmvMasked => session
                .spmv_masked(
                    from_i64(req.g),
                    from_i64(req.a),
                    Semiring::parse(&req.s)?,
                    from_i64(req.c),
                )
                .map(h),
            KernelId::SetIntersect => session
                .set_intersect(from_i64(req.a), from_i64(req.b))
                .map(h),
            KernelId::ReduceSum => session
                .reduce(from_i64(req.g), ReduceOp::Sum, None)
                .map(|s| KernelResponse::Float(s.as_f64())),
            KernelId::ReduceSumMasked => session
                .reduce(from_i64(req.g), ReduceOp::Sum, Some(from_i64(req.a)))
                .map(|s| KernelResponse::Float(s.as_f64())),
            KernelId::L1Diff => session
                .l1_diff(from_i64(req.a), from_i64(req.b))
                .map(KernelResponse::Float),
            KernelId::SetLen => session
                .set_len(from_i64(req.g))
                .map(|v| KernelResponse::Float(v as f64)),
            KernelId::IsEmpty => session.is_empty(from_i64(req.g)).map(KernelResponse::Bool),
            KernelId::Free => session.free(from_i64(req.g)).map(|()| KernelResponse::Unit),
            KernelId::Emit => {
                // Batch form when `names` is populated, else the original
                // single-column form. A guest emitting every declared column in
                // one call is what the host trait always modelled; the
                // one-per-call form remains supported and equivalent, since the
                // session accumulates.
                if req.names.is_empty() {
                    session
                        .emit(&[(req.name.as_str(), from_i64(req.g))])
                        .map(|()| KernelResponse::Unit)
                } else if req.names.len() != req.handles.len() {
                    Err(super::error::arg_validation(format!(
                        "emit: {} column name(s) but {} handle(s) — they pair positionally",
                        req.names.len(),
                        req.handles.len()
                    )))
                } else {
                    let cols: Vec<(&str, Handle)> = req
                        .names
                        .iter()
                        .map(String::as_str)
                        .zip(req.handles.iter().copied().map(from_i64))
                        .collect();
                    session.emit(&cols).map(|()| KernelResponse::Unit)
                }
            }
            KernelId::ArenaNew => session.arena_new(req.cap, req.branch).map(h),
            KernelId::ArenaAlloc => session.arena_alloc(from_i64(req.g), req.cap).map(h),
            KernelId::ArenaLink => session
                .arena_link(from_i64(req.g), from_i64(req.a), from_i64(req.b))
                .map(|()| KernelResponse::Unit),
            KernelId::ArenaColumn => session
                .arena_column(from_i64(req.g))
                .map(|i| KernelResponse::Float(i as f64)),
            KernelId::ArenaCandidates => session
                .arena_candidates(from_i64(req.g), from_i64(req.a))
                .map(h),
            KernelId::ArenaGather => session
                .arena_gather(from_i64(req.g), req.col, from_i64(req.a))
                .map(h),
            KernelId::ArenaScatter => session
                .arena_scatter(from_i64(req.g), req.col, from_i64(req.a), from_i64(req.b))
                .map(|()| KernelResponse::Unit),
            KernelId::ArenaBackup => session
                .arena_backup(from_i64(req.g), req.col, from_i64(req.a), from_i64(req.b))
                .map(|()| KernelResponse::Unit),
            KernelId::ArenaDescend => session
                .arena_descend(
                    from_i64(req.g),
                    from_i64(req.a),
                    req.col,
                    req.col2,
                    req.want_max,
                    req.f,
                )
                .map(h),

            KernelId::ArenaExpand => session
                .arena_expand(from_i64(req.g), from_i64(req.a), req.cap)
                .map(h),
            KernelId::ArenaFreeze => session.arena_freeze(from_i64(req.g)).map(h),
            KernelId::ArenaSeed => session.arena_seed(from_i64(req.a), from_i64(req.g)).map(h),

            // Declared exceptions (see `KernelReach`). These are real catalog
            // entries that deliberately have no wire form, so a guest reaching
            // one is told *why* rather than that the name is unknown. Being
            // explicit arms rather than a wildcard is what keeps this match
            // exhaustive over the catalog.
            KernelId::Graph => Err(super::error::arg_validation(
                "`graph` is not a kernel op: the graph handle reaches sandboxed guests \
                 through the invoke-algorithm arguments",
            )),
            KernelId::GraphNamed => Err(super::error::arg_validation(
                "`graph_named` is not a kernel op: named scopes reach sandboxed guests \
                 through the `graphs` map in the invoke-algorithm arguments",
            )),
        }
    }
}

/// Convenience alias for a shared registry handle passed to loaders.
pub type SharedRegistry = Arc<GraphComputeRegistry>;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::algorithms::graph_compute::{Arena, WorkBudget};
    use std::sync::Arc as StdArc;
    use uni_algo::algo::GraphProjection;
    use uni_common::Value;

    fn build_projection(nodes: &[u64], edges: &[(u64, u64)]) -> GraphProjection {
        let node_rows: Vec<HashMap<String, Value>> = nodes
            .iter()
            .map(|&id| HashMap::from([("id".to_string(), Value::Int(id as i64))]))
            .collect();
        let edge_rows: Vec<HashMap<String, Value>> = edges
            .iter()
            .map(|&(s, t)| {
                HashMap::from([
                    ("source".to_string(), Value::Int(s as i64)),
                    ("target".to_string(), Value::Int(t as i64)),
                ])
            })
            .collect();
        GraphProjection::from_rows(&node_rows, &edge_rows, None, false).expect("projection builds")
    }

    /// Issue #152: an unresolved op must say *why* it is unresolved.
    ///
    /// An arena kernel is a real kernel belonging to a slice this host does not
    /// provide, so it earns `0x86A` naming that slice; a misspelling is an
    /// invalid `op` argument and earns `0x86E`. The previous untyped `0x01`
    /// conflated the two, leaving a guest author unable to tell "fix your
    /// spelling" from "this host cannot do that".
    #[test]
    fn unresolved_ops_are_typed_by_cause() {
        let registry = GraphComputeRegistry::new();
        let sid = registry.open(AlgoSession::new(
            7,
            WorkBudget::from_graph_size(1, 0),
            Arena::new(1 << 16, 64),
        ));
        let call = |op: &str| -> KernelResponse {
            let json = serde_json::json!({ "session": sid, "op": op }).to_string();
            serde_json::from_str(&registry.call_json(&json)).expect("response decodes")
        };

        match call("descend_batch") {
            KernelResponse::Err { code, message } => {
                assert_eq!(code, 0x86A, "an arena kernel must name a missing slice");
                assert!(
                    message.contains("graph-arena@1"),
                    "the message must name the slice to declare: {message}"
                );
            }
            other => panic!("expected a typed slice error, got {other:?}"),
        }

        match call("vertex_kount") {
            KernelResponse::Err { code, .. } => {
                assert_eq!(code, 0x86E, "a misspelled op is an argument fault");
            }
            other => panic!("expected a typed unknown-op error, got {other:?}"),
        }
    }

    /// Drives a full PPR through the JSON dispatch protocol — no loader involved.
    /// This is the loader-agnostic proof that the wire format expresses the whole
    /// algorithm; each real loader only has to carry these JSON strings.
    #[test]
    fn json_dispatch_runs_ppr_end_to_end() {
        let nodes = vec![0u64, 1, 2, 3];
        let edges = vec![(0, 1), (1, 2), (2, 0), (0, 3)];
        let graph = build_projection(&nodes, &edges);

        let registry = GraphComputeRegistry::new();
        let mut session = AlgoSession::new(
            9,
            WorkBudget::from_graph_size(nodes.len() as u64, edges.len() as u64),
            Arena::new(1 << 20, 4096),
        );
        let g = to_i64(session.bind_graph(StdArc::new(graph)));
        let sid = registry.open(session);

        // Helper: issue one JSON call and expect a handle back.
        let call = |req: KernelRequest| -> KernelResponse {
            let json = serde_json::to_string(&serde_json::json!({
                "session": req.session, "op": req.op, "g": req.g, "a": req.a,
                "b": req.b, "s": req.s, "s2": req.s2, "f": req.f, "f2": req.f2,
                "k": req.k, "want_max": req.want_max,
                "wl": req.wl, "wn": req.wn, "p": req.p, "q": req.q, "seed": req.seed,
                "seeds": req.seeds, "name": req.name,
            }))
            .unwrap();
            let resp = registry.call_json(&json);
            serde_json::from_str(&resp).unwrap()
        };
        let handle = |r: KernelResponse| match r {
            KernelResponse::Handle(h) => h,
            other => panic!("expected handle, got {other:?}"),
        };
        let mk = |op: &str| KernelRequest {
            session: sid,
            op: op.to_string(),
            g: 0,
            a: 0,
            b: 0,
            c: 0,
            s: String::new(),
            s2: String::new(),
            f: 0.0,
            f2: 0.0,
            k: 0,
            want_max: false,
            xs: Vec::new(),
            ys: Vec::new(),
            wl: 0,
            wn: 0,
            p: 1.0,
            q: 1.0,
            seed: 0,
            iter: 0,
            seeds: vec![],
            name: String::new(),
            names: vec![],
            handles: vec![],
            cap: 0,
            branch: 0,
            col: 0,
            col2: 0,
        };

        let alpha = 0.85;
        let seed_set = handle(call(KernelRequest {
            g,
            seeds: vec![0],
            ..mk("frontier")
        }));
        let seed_map = handle(call(KernelRequest {
            g: seed_set,
            f: 1.0,
            ..mk("set_to_map")
        }));
        let teleport = handle(call(KernelRequest {
            g: seed_map,
            s: "l1".into(),
            ..mk("normalize")
        }));
        let deg = handle(call(KernelRequest { g, ..mk("degrees") }.with_s("out")));
        let inv_deg = handle(call(KernelRequest {
            g: deg,
            ..mk("recip")
        }));
        let dangling = handle(call(KernelRequest {
            g: deg,
            s: "is_zero".into(),
            ..mk("map_to_set")
        }));
        let mut rank = handle(call(KernelRequest {
            g: teleport,
            f: 1.0,
            ..mk("scale")
        }));
        for _ in 0..100 {
            let contrib = handle(call(KernelRequest {
                a: rank,
                b: inv_deg,
                s: "mul".into(),
                ..mk("ewise")
            }));
            let spread = handle(call(KernelRequest {
                g,
                a: contrib,
                s: "linear_algebra".into(),
                s2: "out".into(),
                ..mk("spmv")
            }));
            let dm = match call(KernelRequest {
                g: rank,
                a: dangling,
                ..mk("reduce_sum_masked")
            }) {
                KernelResponse::Float(v) => v,
                other => panic!("expected float, got {other:?}"),
            };
            let scaled = handle(call(KernelRequest {
                g: spread,
                f: alpha,
                ..mk("scale")
            }));
            let blend = 1.0 - alpha + alpha * dm;
            let next = handle(call(KernelRequest {
                a: scaled,
                b: teleport,
                s: "axpy".into(),
                f: blend,
                ..mk("ewise")
            }));
            let _ = call(KernelRequest {
                g: contrib,
                ..mk("free")
            });
            let _ = call(KernelRequest {
                g: spread,
                ..mk("free")
            });
            let _ = call(KernelRequest {
                g: scaled,
                ..mk("free")
            });
            let _ = call(KernelRequest {
                g: rank,
                ..mk("free")
            });
            rank = next;
        }
        let _ = call(KernelRequest {
            g: rank,
            name: "score".into(),
            ..mk("emit")
        });

        let mut closed = registry.close(sid).expect("session present");
        let emitted = closed.take_emitted();
        let scores = &emitted[0].1;
        let total: f64 = scores.iter().sum();
        assert!(
            (total - 1.0).abs() < 1e-9,
            "PPR over JSON dispatch must conserve mass, got {total}"
        );
    }

    /// The two host-supplied kernels refuse the JSON path, and say why.
    ///
    /// `graph` and `graph_named` sit in the catalog so every loader surface is
    /// checked against one list, but a sandboxed guest receives those handles in
    /// its invocation arguments. Dispatching them explains that rather than
    /// reporting an unknown op — and neither arm had a test, so the wording could
    /// have decayed into the generic message unnoticed.
    #[test]
    fn the_host_supplied_kernels_refuse_the_json_path_with_a_reason() {
        let registry = GraphComputeRegistry::new();
        let session = AlgoSession::new(
            7,
            WorkBudget::from_graph_size(4, 4),
            Arena::new(1 << 20, 4096),
        );
        let sid = registry.open(session);

        for (op, needle) in [
            ("graph", "invoke-algorithm arguments"),
            ("graph_named", "`graphs` map"),
        ] {
            let raw = registry.call_json(&format!(r#"{{"session":{sid},"op":"{op}"}}"#));
            let resp: KernelResponse = serde_json::from_str(&raw).expect("a typed response");
            match resp {
                KernelResponse::Err { code, message } => {
                    assert_eq!(code, super::super::error::ARG_VALIDATION, "{op} code");
                    assert!(
                        message.contains(needle),
                        "the `{op}` refusal must say where the handle actually comes \
                         from, got: {message}"
                    );
                }
                other => panic!("`{op}` over JSON must be refused, got {other:?}"),
            }
        }
    }

    #[test]
    fn new_kernels_reachable_via_json() {
        // W3 (B2): edge_count / topk / arg_extreme / scatter / generic map_apply
        // must all be expressible over the loader-agnostic JSON wire, so the
        // §9.3 corpus (Bellman-Ford scatter, top-k egress) is guest-authorable on
        // the sandboxed loaders too.
        let nodes = vec![0u64, 1, 2, 3];
        // out-degrees: node 0 -> 3, node 1 -> 1, nodes 2,3 -> 0.
        let edges = vec![(0, 1), (0, 2), (0, 3), (1, 2)];
        let graph = build_projection(&nodes, &edges);
        let registry = GraphComputeRegistry::new();
        let mut session = AlgoSession::new(
            7,
            WorkBudget::from_graph_size(4, 4),
            Arena::new(1 << 20, 4096),
        );
        let g = to_i64(session.bind_graph(StdArc::new(graph)));
        let sid = registry.open(session);

        let call = |json: String| -> KernelResponse {
            serde_json::from_str(&registry.call_json(&json)).unwrap()
        };
        let as_handle = |r: KernelResponse| match r {
            KernelResponse::Handle(h) => h,
            other => panic!("want handle, got {other:?}"),
        };

        match call(format!(r#"{{"session":{sid},"op":"edge_count","g":{g}}}"#)) {
            KernelResponse::Float(e) => assert_eq!(e, 4.0, "edge_count"),
            other => panic!("edge_count -> {other:?}"),
        }

        let deg = as_handle(call(format!(
            r#"{{"session":{sid},"op":"degrees","g":{g},"s":"out"}}"#
        )));

        match call(format!(
            r#"{{"session":{sid},"op":"topk","g":{deg},"k":2}}"#
        )) {
            KernelResponse::Pairs(p) => {
                assert_eq!(p.len(), 2, "topk returns k pairs");
                assert_eq!(p[0], (0, 3.0), "highest out-degree ranked first");
            }
            other => panic!("topk -> {other:?}"),
        }

        match call(format!(
            r#"{{"session":{sid},"op":"arg_extreme","g":{deg},"want_max":true}}"#
        )) {
            KernelResponse::VidScalar { vid, f } => {
                assert_eq!((vid, f), (0, 3.0), "max out-degree is node 0")
            }
            other => panic!("arg_extreme -> {other:?}"),
        }

        // Generic affine map 2*x+1 (unreachable via scale/recip/normalize), then a
        // scatter over a frontier — both must return live handles.
        let affine = as_handle(call(format!(
            r#"{{"session":{sid},"op":"map_apply","g":{deg},"s":"affine","f":2.0,"f2":1.0}}"#
        )));
        let f1 = as_handle(call(format!(
            r#"{{"session":{sid},"op":"frontier","g":{g},"seeds":[1]}}"#
        )));
        let _scattered = as_handle(call(format!(
            r#"{{"session":{sid},"op":"scatter","a":{affine},"b":{f1},"f":99.0}}"#
        )));
    }

    #[test]
    fn crossing_count_is_graph_size_invariant() {
        // L-9 — the conductor thesis, measured. A fixed-iteration PPR issues the
        // same number of host-fn crossings regardless of |V|: only handles and
        // scalars cross, never per-vertex/per-edge data. Any implementation that
        // smuggled per-element crossings (or marshalled data) would scale with the
        // graph and fail this. We count JSON kernel calls for the identical driver
        // on a 12-node vs a 1200-node ring and assert byte-identical call counts.
        fn ring(n: u64) -> GraphProjection {
            let nodes: Vec<u64> = (0..n).collect();
            let edges: Vec<(u64, u64)> = (0..n).map(|i| (i, (i + 1) % n)).collect();
            build_projection(&nodes, &edges)
        }

        // Runs a fixed 20-iteration PPR over the JSON wire, returning the number
        // of host-fn (kernel) calls made — independent of the result values.
        fn count_crossings(graph: GraphProjection) -> usize {
            let registry = GraphComputeRegistry::new();
            let mut session = AlgoSession::new(
                3,
                WorkBudget::from_graph_size(graph.vertex_count() as u64, graph.edge_count() as u64),
                Arena::new(1 << 24, 4096),
            );
            let g = to_i64(session.bind_graph(StdArc::new(graph)));
            let sid = registry.open(session);

            let mut calls = 0usize;
            let mut call = |json: String| -> KernelResponse {
                calls += 1;
                serde_json::from_str(&registry.call_json(&json)).unwrap()
            };
            let handle = |r: KernelResponse| match r {
                KernelResponse::Handle(h) => h,
                other => panic!("want handle, got {other:?}"),
            };

            let teleport = handle(call(format!(
                r#"{{"session":{sid},"op":"frontier","g":{g},"seeds":[0]}}"#
            )));
            let teleport = handle(call(format!(
                r#"{{"session":{sid},"op":"set_to_map","g":{teleport},"f":1.0}}"#
            )));
            let teleport = handle(call(format!(
                r#"{{"session":{sid},"op":"normalize","g":{teleport},"s":"l1"}}"#
            )));
            let deg = handle(call(format!(
                r#"{{"session":{sid},"op":"degrees","g":{g},"s":"out"}}"#
            )));
            let inv_deg = handle(call(format!(
                r#"{{"session":{sid},"op":"recip","g":{deg}}}"#
            )));
            let mut rank = teleport;
            for _ in 0..20 {
                let contrib = handle(call(format!(
                    r#"{{"session":{sid},"op":"ewise","a":{rank},"b":{inv_deg},"s":"mul"}}"#
                )));
                let spread = handle(call(format!(
                    r#"{{"session":{sid},"op":"spmv","g":{g},"a":{contrib},"s":"linear_algebra","s2":"out"}}"#
                )));
                let scaled = handle(call(format!(
                    r#"{{"session":{sid},"op":"scale","g":{spread},"f":0.85}}"#
                )));
                rank = handle(call(format!(
                    r#"{{"session":{sid},"op":"ewise","a":{scaled},"b":{teleport},"s":"axpy","f":0.15}}"#
                )));
                let _ = call(format!(r#"{{"session":{sid},"op":"free","g":{contrib}}}"#));
            }
            let _ = call(format!(
                r#"{{"session":{sid},"op":"emit","g":{rank},"name":"score"}}"#
            ));
            calls
        }

        let small = count_crossings(ring(12));
        let large = count_crossings(ring(1_200));
        assert_eq!(
            small, large,
            "host-fn crossings must not scale with graph size (conductor thesis)"
        );
    }

    #[test]
    fn walk_egress_and_adamic_adar_reachable_via_json() {
        // WS1/WS2A: `random_walks` → `emit_walks` and `neighborhood_overlap`
        // with the `adamic_adar` metric must be drivable over the loader-agnostic
        // JSON wire so the sandboxed WASM/Extism loaders reach them too.
        let nodes = vec![0u64, 1, 2];
        let edges = vec![(0, 1), (1, 2), (2, 0)];
        let graph = build_projection(&nodes, &edges);
        let registry = GraphComputeRegistry::new();
        let mut session = AlgoSession::new(
            11,
            WorkBudget::from_graph_size(3, 3),
            Arena::new(1 << 20, 4096),
        );
        let g = to_i64(session.bind_graph(StdArc::new(graph)));
        let sid = registry.open(session);

        let call = |json: String| -> KernelResponse {
            serde_json::from_str(&registry.call_json(&json)).unwrap()
        };
        let as_handle = |r: KernelResponse| match r {
            KernelResponse::Handle(h) => h,
            other => panic!("want handle, got {other:?}"),
        };

        // Adamic-Adar over the triangle returns a live tensor handle.
        let _aa = as_handle(call(format!(
            r#"{{"session":{sid},"op":"neighborhood_overlap","g":{g},"seeds":[0],"s":"adamic_adar"}}"#
        )));

        // random_walks -> emit_walks: the walk sequences reach the walk sink.
        let walks = as_handle(call(format!(
            r#"{{"session":{sid},"op":"random_walks","g":{g},"seeds":[0],"wl":4,"wn":2,"seed":7}}"#
        )));
        assert_eq!(
            call(format!(
                r#"{{"session":{sid},"op":"emit_walks","g":{walks}}}"#
            )),
            KernelResponse::Unit,
            "emit_walks returns Unit over JSON"
        );

        let mut closed = registry.close(sid).expect("session present");
        let rows = closed.take_emitted_walks();
        // 2 walks of length 4 over a cycle => 2 * 5 = 10 step rows.
        assert_eq!(
            rows.len(),
            10,
            "walk sequences egress through the JSON path"
        );
    }

    #[test]
    fn map_and_ewise_sqrt_exp_div_over_json() {
        // G2 — `sqrt`/`exp` (MapOp) and `div` (EwiseOp) are decoded and evaluated
        // over the loader-agnostic JSON wire (which serves WASM + Extism), so a
        // guest can compose the canonical UCT term `c·√(ln N / n)`. The op-string
        // decoders are NOT covered by the KernelId reach tests, so this
        // differential test guards against a variant added to the enum but
        // forgotten on a wire decoder.
        let nodes = vec![0u64, 1, 2, 3];
        // out-degrees: 0 -> 3, 1 -> 1, 2 -> 0, 3 -> 0.
        let edges = vec![(0, 1), (0, 2), (0, 3), (1, 2)];
        let graph = build_projection(&nodes, &edges);
        let registry = GraphComputeRegistry::new();
        let mut session = AlgoSession::new(
            3,
            WorkBudget::from_graph_size(4, 4),
            Arena::new(1 << 20, 4096),
        );
        let g = to_i64(session.bind_graph(StdArc::new(graph)));
        let sid = registry.open(session);
        let call = |json: String| -> KernelResponse {
            serde_json::from_str(&registry.call_json(&json)).unwrap()
        };
        let as_handle = |r: KernelResponse| match r {
            KernelResponse::Handle(h) => h,
            other => panic!("want handle, got {other:?}"),
        };
        let top1 = |h: i64| -> (i64, f64) {
            match call(format!(r#"{{"session":{sid},"op":"topk","g":{h},"k":1}}"#)) {
                KernelResponse::Pairs(p) => p[0],
                other => panic!("topk -> {other:?}"),
            }
        };

        let deg = as_handle(call(format!(
            r#"{{"session":{sid},"op":"degrees","g":{g},"s":"out"}}"#
        )));
        // sqrt(deg): the top value is sqrt(3).
        let sq = as_handle(call(format!(
            r#"{{"session":{sid},"op":"map_apply","g":{deg},"s":"sqrt"}}"#
        )));
        let (v, f) = top1(sq);
        assert_eq!(v, 0, "node 0 has the largest degree");
        assert!((f - 3.0_f64.sqrt()).abs() < 1e-12, "sqrt(3), got {f}");
        // exp(deg): the top value is e^3.
        let ex = as_handle(call(format!(
            r#"{{"session":{sid},"op":"map_apply","g":{deg},"s":"exp"}}"#
        )));
        let (_, f) = top1(ex);
        assert!((f - 3.0_f64.exp()).abs() < 1e-9, "exp(3), got {f}");
        // div: scale(deg,2) / deg = [6/3, 2/1, 0/0, 0/0] = [2, 2, 0, 0]; the
        // zero-denominator rows follow the `x/0 = 0` convention (no NaN).
        let dbl = as_handle(call(format!(
            r#"{{"session":{sid},"op":"map_apply","g":{deg},"s":"scale","f":2.0}}"#
        )));
        let q = as_handle(call(format!(
            r#"{{"session":{sid},"op":"ewise","a":{dbl},"b":{deg},"s":"div"}}"#
        )));
        let (_, f) = top1(q);
        assert!((f - 2.0).abs() < 1e-12, "6/3 = 2, got {f}");
    }

    /// REQ-D13 — `floor` and `mod` decode and evaluate over the JSON wire.
    ///
    /// Same reason as the `sqrt`/`exp`/`div` test above: the op-string decoders
    /// are not covered by the `KernelId` reach tests, because `map_apply` and
    /// `ewise` were already reachable before these names existed. Without this,
    /// a name could be added to the vocabulary and never exercised on the wire
    /// that serves WASM and Extism.
    #[test]
    fn map_and_ewise_floor_mod_over_json() {
        let nodes = vec![0u64, 1, 2, 3];
        // out-degrees: 0 -> 3, 1 -> 1, 2 -> 0, 3 -> 0.
        let edges = vec![(0, 1), (0, 2), (0, 3), (1, 2)];
        let graph = build_projection(&nodes, &edges);
        let registry = GraphComputeRegistry::new();
        let mut session = AlgoSession::new(
            5,
            WorkBudget::from_graph_size(4, 4),
            Arena::new(1 << 20, 4096),
        );
        let g = to_i64(session.bind_graph(StdArc::new(graph)));
        let sid = registry.open(session);
        let call = |json: String| -> KernelResponse {
            serde_json::from_str(&registry.call_json(&json)).unwrap()
        };
        let as_handle = |r: KernelResponse| match r {
            KernelResponse::Handle(h) => h,
            other => panic!("want handle, got {other:?}"),
        };
        let top1 = |h: i64| -> (i64, f64) {
            match call(format!(r#"{{"session":{sid},"op":"topk","g":{h},"k":1}}"#)) {
                KernelResponse::Pairs(p) => p[0],
                other => panic!("topk -> {other:?}"),
            }
        };

        let deg = as_handle(call(format!(
            r#"{{"session":{sid},"op":"degrees","g":{g},"s":"out"}}"#
        )));
        // deg = [3, 1, 0, 0]; affine(deg, 1.0, 0.5) = [3.5, 1.5, 0.5, 0.5].
        let half = as_handle(call(format!(
            r#"{{"session":{sid},"op":"map_apply","g":{deg},"s":"affine","f":1.0,"f2":0.5}}"#
        )));
        // floor -> [3, 1, 0, 0]; the top value is 3.
        let fl = as_handle(call(format!(
            r#"{{"session":{sid},"op":"map_apply","g":{half},"s":"floor"}}"#
        )));
        let (v, f) = top1(fl);
        assert_eq!(v, 0, "node 0 has the largest degree");
        assert!((f - 3.0).abs() < 1e-12, "floor(3.5) = 3, got {f}");

        // Scalar mod: the divisor rides in `f`. 3.5 mod 2 = 1.5.
        let sm = as_handle(call(format!(
            r#"{{"session":{sid},"op":"map_apply","g":{half},"s":"mod","f":2.0}}"#
        )));
        let (_, f) = top1(sm);
        assert!((f - 1.5).abs() < 1e-12, "3.5 mod 2 = 1.5, got {f}");

        // Tensor mod: half mod deg = [3.5 mod 3, 1.5 mod 1, ...] = [0.5, 0.5, ..].
        let tm = as_handle(call(format!(
            r#"{{"session":{sid},"op":"ewise","a":{half},"b":{deg},"s":"mod"}}"#
        )));
        let (_, f) = top1(tm);
        assert!((f - 0.5).abs() < 1e-12, "3.5 mod 3 = 0.5, got {f}");
    }

    #[test]
    fn expand_both_unions_out_and_in_neighbors() {
        // G5a — Direction::Both walks out ∪ in. On 0->1->2, expanding {1}:
        // out(1)={2}, in(1)={0}, so Both = {0, 2}. Needs the reverse CSR.
        let node_rows: Vec<HashMap<String, Value>> = [0u64, 1, 2]
            .iter()
            .map(|&id| HashMap::from([("id".to_string(), Value::Int(id as i64))]))
            .collect();
        let edge_rows: Vec<HashMap<String, Value>> = [(0u64, 1u64), (1, 2)]
            .iter()
            .map(|&(s, t)| {
                HashMap::from([
                    ("source".to_string(), Value::Int(s as i64)),
                    ("target".to_string(), Value::Int(t as i64)),
                ])
            })
            .collect();
        let graph = GraphProjection::from_rows(&node_rows, &edge_rows, None, true)
            .expect("projection with reverse builds");
        let registry = GraphComputeRegistry::new();
        let mut session = AlgoSession::new(
            3,
            WorkBudget::from_graph_size(3, 2),
            Arena::new(1 << 20, 4096),
        );
        let g = to_i64(session.bind_graph(StdArc::new(graph)));
        let sid = registry.open(session);
        let call = |json: String| {
            serde_json::from_str::<KernelResponse>(&registry.call_json(&json)).unwrap()
        };
        let as_handle = |r: KernelResponse| match r {
            KernelResponse::Handle(h) => h,
            other => panic!("want handle, got {other:?}"),
        };
        let set_len = |h: i64| match call(format!(r#"{{"session":{sid},"op":"set_len","g":{h}}}"#))
        {
            KernelResponse::Float(f) => f as usize,
            other => panic!("set_len -> {other:?}"),
        };
        let front = as_handle(call(format!(
            r#"{{"session":{sid},"op":"frontier","g":{g},"seeds":[1]}}"#
        )));
        // exclude = the frontier itself (self-exclusion is harmless: 1 is not its
        // own neighbor here).
        let both = as_handle(call(format!(
            r#"{{"session":{sid},"op":"expand","g":{g},"a":{front},"b":{front},"s":"both"}}"#
        )));
        assert_eq!(set_len(both), 2, "Both = out{{2}} union in{{0}}");
        let out = as_handle(call(format!(
            r#"{{"session":{sid},"op":"expand","g":{g},"a":{front},"b":{front},"s":"out"}}"#
        )));
        assert_eq!(set_len(out), 1, "Out = {{2}}");
        let inn = as_handle(call(format!(
            r#"{{"session":{sid},"op":"expand","g":{g},"a":{front},"b":{front},"s":"in"}}"#
        )));
        assert_eq!(set_len(inn), 1, "In = {{0}}");
    }

    #[test]
    fn expand_both_without_reverse_fails_loud() {
        // G5a — Both over an out-only projection is a named error, not a silent
        // out-only result. build_projection() builds without the reverse CSR.
        let graph = build_projection(&[0, 1, 2], &[(0, 1), (1, 2)]);
        let registry = GraphComputeRegistry::new();
        let mut session = AlgoSession::new(
            3,
            WorkBudget::from_graph_size(3, 2),
            Arena::new(1 << 20, 4096),
        );
        let g = to_i64(session.bind_graph(StdArc::new(graph)));
        let sid = registry.open(session);
        let call = |json: String| {
            serde_json::from_str::<KernelResponse>(&registry.call_json(&json)).unwrap()
        };
        let front = match call(format!(
            r#"{{"session":{sid},"op":"frontier","g":{g},"seeds":[1]}}"#
        )) {
            KernelResponse::Handle(h) => h,
            other => panic!("want handle, got {other:?}"),
        };
        match call(format!(
            r#"{{"session":{sid},"op":"expand","g":{g},"a":{front},"b":{front},"s":"both"}}"#
        )) {
            KernelResponse::Err { message, .. } => assert!(
                message.contains("includeReverse"),
                "the error must name includeReverse, got: {message}"
            ),
            other => panic!("expected a fail-loud error, got {other:?}"),
        }
    }

    #[test]
    fn reach_fixpoint_bfs_over_json() {
        // G6 — reach_fixpoint collapses the delta-frontier BFS into one native
        // call. On a chain 0->1->2->3 plus an isolated node 4, reachability from
        // {0} out is {0,1,2,3}; from the isolated {4} it is {4}.
        let nodes = vec![0u64, 1, 2, 3, 4];
        let edges = vec![(0, 1), (1, 2), (2, 3)];
        let graph = build_projection(&nodes, &edges);
        let registry = GraphComputeRegistry::new();
        let mut session = AlgoSession::new(
            3,
            WorkBudget::from_graph_size(5, 3),
            Arena::new(1 << 20, 4096),
        );
        let g = to_i64(session.bind_graph(StdArc::new(graph)));
        let sid = registry.open(session);
        let call = |json: String| {
            serde_json::from_str::<KernelResponse>(&registry.call_json(&json)).unwrap()
        };
        let as_handle = |r: KernelResponse| match r {
            KernelResponse::Handle(h) => h,
            other => panic!("want handle, got {other:?}"),
        };
        let set_len = |h: i64| match call(format!(r#"{{"session":{sid},"op":"set_len","g":{h}}}"#))
        {
            KernelResponse::Float(f) => f as usize,
            other => panic!("set_len -> {other:?}"),
        };

        let v = as_handle(call(format!(
            r#"{{"session":{sid},"op":"reach_fixpoint","g":{g},"seeds":[0],"s":"out"}}"#
        )));
        assert_eq!(set_len(v), 4, "0,1,2,3 reachable; 4 is isolated");
        let v2 = as_handle(call(format!(
            r#"{{"session":{sid},"op":"reach_fixpoint","g":{g},"seeds":[4],"s":"out"}}"#
        )));
        assert_eq!(set_len(v2), 1, "an isolated node reaches only itself");
    }

    #[test]
    fn expand_sampled_fuses_draw_and_expand_over_json() {
        // G7 — expand_sampled draws only the frontier's out-edges. prob=1.0
        // everywhere keeps every out-neighbor (== expand); prob=0.0 keeps none.
        let nodes = vec![0u64, 1, 2, 3];
        let edges = vec![(0, 1), (0, 2), (0, 3)]; // node 0 has out-degree 3
        let graph = build_projection(&nodes, &edges);
        let registry = GraphComputeRegistry::new();
        let mut session = AlgoSession::new(
            3,
            WorkBudget::from_graph_size(4, 3),
            Arena::new(1 << 20, 4096),
        );
        let g = to_i64(session.bind_graph(StdArc::new(graph)));
        let sid = registry.open(session);
        let call = |json: String| {
            serde_json::from_str::<KernelResponse>(&registry.call_json(&json)).unwrap()
        };
        let as_handle = |r: KernelResponse| match r {
            KernelResponse::Handle(h) => h,
            other => panic!("want handle, got {other:?}"),
        };
        let set_len = |h: i64| match call(format!(r#"{{"session":{sid},"op":"set_len","g":{h}}}"#))
        {
            KernelResponse::Float(f) => f as usize,
            other => panic!("set_len -> {other:?}"),
        };

        let front = as_handle(call(format!(
            r#"{{"session":{sid},"op":"frontier","g":{g},"seeds":[0]}}"#
        )));
        // prob = edge_weights (all 1.0): every out-edge fires.
        let ones = as_handle(call(format!(
            r#"{{"session":{sid},"op":"edge_weights","g":{g}}}"#
        )));
        let all = as_handle(call(format!(
            r#"{{"session":{sid},"op":"expand_sampled","g":{g},"a":{front},"s":"out","c":{ones},"seed":7,"iter":0}}"#
        )));
        assert_eq!(set_len(all), 3, "prob=1 keeps every out-neighbor");
        // prob = 0 everywhere: no edge fires.
        let zeros = as_handle(call(format!(
            r#"{{"session":{sid},"op":"map_apply","g":{ones},"s":"scale","f":0.0}}"#
        )));
        let none = as_handle(call(format!(
            r#"{{"session":{sid},"op":"expand_sampled","g":{g},"a":{front},"s":"out","c":{zeros},"seed":7,"iter":0}}"#
        )));
        assert_eq!(set_len(none), 0, "prob=0 keeps nothing");
    }

    #[test]
    fn sample_edges_undirected_fires_pairs_as_a_unit() {
        // G5b — both half-edges of an undirected pair share one draw, so a mask
        // over a graph of pure undirected pairs always has EVEN cardinality (each
        // pair contributes 0 or 2). The directed sample_edges draws each half
        // independently and can produce odd cardinality — the metamorphic
        // contrast that proves the undirected keying works.
        let nodes = vec![0u64, 1, 2, 3];
        let edges = vec![(0, 1), (1, 0), (1, 2), (2, 1), (2, 3), (3, 2)]; // 3 pairs
        let graph = build_projection(&nodes, &edges);
        let registry = GraphComputeRegistry::new();
        let mut session = AlgoSession::new(
            3,
            WorkBudget::from_graph_size(4, 6),
            Arena::new(1 << 20, 4096),
        );
        let g = to_i64(session.bind_graph(StdArc::new(graph)));
        let sid = registry.open(session);
        let call = |json: String| {
            serde_json::from_str::<KernelResponse>(&registry.call_json(&json)).unwrap()
        };
        let as_handle = |r: KernelResponse| match r {
            KernelResponse::Handle(h) => h,
            other => panic!("want handle, got {other:?}"),
        };
        let elen = |h: i64| match call(format!(
            r#"{{"session":{sid},"op":"edge_set_len","g":{h}}}"#
        )) {
            KernelResponse::Float(f) => f as usize,
            other => panic!("edge_set_len -> {other:?}"),
        };
        // Symmetric probability p = 0.5 on every half-edge.
        let ones = as_handle(call(format!(
            r#"{{"session":{sid},"op":"edge_weights","g":{g}}}"#
        )));
        let half = as_handle(call(format!(
            r#"{{"session":{sid},"op":"map_apply","g":{ones},"s":"scale","f":0.5}}"#
        )));

        // Undirected: cardinality is always even across seeds.
        for seed in 0..20u64 {
            let m = as_handle(call(format!(
                r#"{{"session":{sid},"op":"sample_edges_undirected","g":{g},"a":{half},"seed":{seed},"iter":0}}"#
            )));
            assert_eq!(
                elen(m) % 2,
                0,
                "undirected mask must have even cardinality (seed {seed})"
            );
        }
        // Directed sample_edges draws each half independently → some seed is odd.
        let any_odd = (0..20u64).any(|seed| {
            let m = as_handle(call(format!(
                r#"{{"session":{sid},"op":"sample_edges","g":{half},"seed":{seed},"iter":0}}"#
            )));
            elen(m) % 2 == 1
        });
        assert!(
            any_odd,
            "directed sample_edges should draw half-edges independently (some odd cardinality)"
        );
    }

    #[test]
    fn emit_column_length_mismatch_is_named_not_opaque() {
        // G10: `emit` keys its columns to the *primary* projected input graph's
        // vertex count. A column whose length differs (e.g. an arena-slot-keyed
        // tensor, or here a tensor over a second, larger graph) used to slip past
        // `emit` and detonate downstream as an opaque Arrow "all columns in a
        // record batch must have the same length" during the loader's batch
        // assembly. It is now named at the `emit` call itself (error 0x869).
        let registry = GraphComputeRegistry::new();
        let mut session = AlgoSession::new(
            4,
            WorkBudget::from_graph_size(3, 3),
            Arena::new(1 << 20, 4096),
        );
        // The FIRST bound graph is the primary input space: 2 vertices.
        let _primary =
            to_i64(session.bind_graph(StdArc::new(build_projection(&[0, 1], &[(0, 1)]))));
        // A second, larger graph (3 vertices) stands in for a differently-sized
        // emit space — the arena-slot space in the real MCTS trap.
        let g2 = to_i64(
            session.bind_graph(StdArc::new(build_projection(&[0, 1, 2], &[(0, 1), (1, 2)]))),
        );
        let sid = registry.open(session);

        let call = |json: String| -> KernelResponse {
            serde_json::from_str(&registry.call_json(&json)).unwrap()
        };
        // A [V]-length tensor over g2 => length 3, but the primary space is 2.
        let deg3 = match call(format!(
            r#"{{"session":{sid},"op":"degrees","g":{g2},"s":"out"}}"#
        )) {
            KernelResponse::Handle(h) => h,
            other => panic!("degrees -> {other:?}"),
        };
        match call(format!(
            r#"{{"session":{sid},"op":"emit","g":{deg3},"name":"score"}}"#
        )) {
            KernelResponse::Err { code, message } => {
                assert_eq!(code, 0x869, "an emit mismatch is a schema mismatch");
                // Identity is now checked before length, and it is the stronger
                // statement: this column is keyed to a different projection, of
                // which the differing length is only a symptom. The same check
                // also catches a second projection of *equal* size, which the
                // length comparison never could.
                assert!(
                    message.contains("different projection"),
                    "the error must name the mismatch, got: {message}"
                );
            }
            other => panic!("expected a named emit mismatch, got {other:?}"),
        }
    }

    #[test]
    fn unknown_session_is_typed_error_not_panic() {
        let registry = GraphComputeRegistry::new();
        let resp = registry.call_json(r#"{"session": 999, "op": "vertex_count", "g": 0}"#);
        let parsed: KernelResponse = serde_json::from_str(&resp).unwrap();
        assert!(matches!(parsed, KernelResponse::Err { code: 0x863, .. }));
    }

    #[test]
    fn malformed_json_is_typed_error_not_panic() {
        let registry = GraphComputeRegistry::new();
        let resp = registry.call_json("not json at all");
        let parsed: KernelResponse = serde_json::from_str(&resp).unwrap();
        assert!(matches!(parsed, KernelResponse::Err { code: 0x802, .. }));
    }

    #[test]
    fn session_ids_are_unguessable_not_sequential() {
        // Review H2: a concurrent guest must not be able to enumerate another
        // CALL's session id. Open a real session, then probe every low sequential
        // id — none may resolve (the real id is a 60-bit-entropy random u64).
        let nodes = vec![0u64, 1];
        let graph = build_projection(&nodes, &[(0, 1)]);
        let registry = GraphComputeRegistry::new();
        let mut session = AlgoSession::new(
            5,
            WorkBudget::from_graph_size(2, 1),
            Arena::new(1 << 20, 4096),
        );
        let _g = session.bind_graph(StdArc::new(graph));
        let sid = registry.open(session);
        assert!(
            sid > u32::MAX as u64 || sid == 0 || sid.count_ones() > 4,
            "id should look random, not like a small counter (got {sid})"
        );
        for guess in 0..2_000u64 {
            let req = format!(r#"{{"session": {guess}, "op": "vertex_count", "g": 0}}"#);
            let parsed: KernelResponse = serde_json::from_str(&registry.call_json(&req)).unwrap();
            assert!(
                matches!(parsed, KernelResponse::Err { code: 0x863, .. }),
                "sequential id {guess} must not resolve to a live session"
            );
        }
        // The real (random) id still works.
        let req = format!(
            r#"{{"session": {sid}, "op": "vertex_count", "g": {}}}"#,
            _g.as_u64() as i64
        );
        let parsed: KernelResponse = serde_json::from_str(&registry.call_json(&req)).unwrap();
        assert!(matches!(parsed, KernelResponse::Float(_)));
    }

    // Small builder helpers to keep the driver above readable.
    impl KernelRequest {
        fn with_s(mut self, s: &str) -> Self {
            self.s = s.to_string();
            self
        }
    }

    impl Serialize for KernelRequest {
        fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
            use serde::ser::SerializeStruct;
            let mut st = s.serialize_struct("KernelRequest", 20)?;
            st.serialize_field("session", &self.session)?;
            st.serialize_field("op", &self.op)?;
            st.serialize_field("g", &self.g)?;
            st.serialize_field("a", &self.a)?;
            st.serialize_field("b", &self.b)?;
            st.serialize_field("c", &self.c)?;
            st.serialize_field("s", &self.s)?;
            st.serialize_field("s2", &self.s2)?;
            st.serialize_field("f", &self.f)?;
            st.serialize_field("f2", &self.f2)?;
            st.serialize_field("k", &self.k)?;
            st.serialize_field("want_max", &self.want_max)?;
            st.serialize_field("wl", &self.wl)?;
            st.serialize_field("wn", &self.wn)?;
            st.serialize_field("p", &self.p)?;
            st.serialize_field("q", &self.q)?;
            st.serialize_field("seed", &self.seed)?;
            st.serialize_field("iter", &self.iter)?;
            st.serialize_field("seeds", &self.seeds)?;
            st.serialize_field("name", &self.name)?;
            st.end()
        }
    }

    /// The batch `emit` form carries N columns in one wire call, and the
    /// single-column form keeps working unchanged.
    ///
    /// The `names`/`handles` pair is additive and `#[serde(default)]`, so a guest
    /// built before it existed still deserializes — proven here by driving both
    /// shapes against the same session.
    #[test]
    fn emit_accepts_both_the_batch_and_single_column_wire_forms() {
        let registry = GraphComputeRegistry::new();
        let mut session = AlgoSession::new(
            9,
            WorkBudget::from_graph_size(3, 3),
            Arena::new(1 << 20, 4096),
        )
        .with_expected_columns(vec!["a".to_string(), "b".to_string()]);
        let g = to_i64(
            session.bind_graph(StdArc::new(build_projection(&[0, 1, 2], &[(0, 1), (1, 2)]))),
        );
        let sid = registry.open(session);

        let call = |json: String| -> KernelResponse {
            serde_json::from_str(&registry.call_json(&json)).unwrap()
        };
        let handle_of = |r: KernelResponse| match r {
            KernelResponse::Handle(h) => h,
            other => panic!("expected handle, got {other:?}"),
        };

        let deg = handle_of(call(format!(
            r#"{{"session":{sid},"op":"degrees","g":{g},"s":"out"}}"#
        )));
        let ids = handle_of(call(format!(
            r#"{{"session":{sid},"op":"vertex_ids","g":{g}}}"#
        )));

        // Batch form: both declared columns in one call.
        match call(format!(
            r#"{{"session":{sid},"op":"emit","names":["a","b"],"handles":[{deg},{ids}]}}"#
        )) {
            KernelResponse::Unit => {}
            other => panic!("batch emit -> {other:?}"),
        }

        // Mismatched arity is named, not silently truncated.
        match call(format!(
            r#"{{"session":{sid},"op":"emit","names":["a","b"],"handles":[{deg}]}}"#
        )) {
            KernelResponse::Err { message, .. } => {
                assert!(
                    message.contains("pair positionally"),
                    "arity mismatch must say why: {message}"
                );
            }
            other => panic!("expected an arity error, got {other:?}"),
        }

        // The single-column form still parses and still reaches `emit` — here it
        // is rejected only because `a` was already emitted above, which proves
        // the wire path ran rather than the request failing to deserialize.
        match call(format!(
            r#"{{"session":{sid},"op":"emit","g":{deg},"name":"a"}}"#
        )) {
            KernelResponse::Err { code, message } => {
                assert_eq!(code, 0x869);
                assert!(message.contains("already emitted"), "{message}");
            }
            other => panic!("expected a duplicate-column error, got {other:?}"),
        }
    }

    /// The budget accessors are reachable over the JSON wire.
    ///
    /// WASM and Extism reach kernels only through `call_json`, so without this
    /// the `KernelResponse::Float` decode is unasserted on both surfaces.
    #[test]
    fn budget_accessors_are_reachable_via_json() {
        let registry = GraphComputeRegistry::new();
        let mut session = AlgoSession::new(12, WorkBudget::new(500), Arena::new(1 << 20, 256));
        let g = to_i64(session.bind_graph(StdArc::new(build_projection(
            &[0, 1, 2, 3],
            &[(0, 1), (1, 2)],
        ))));
        let sid = registry.open(session);

        let call = |json: String| -> KernelResponse {
            serde_json::from_str(&registry.call_json(&json)).unwrap()
        };
        let float_of = |r: KernelResponse| match r {
            KernelResponse::Float(v) => v,
            other => panic!("expected a float, got {other:?}"),
        };

        let total = float_of(call(format!(r#"{{"session":{sid},"op":"work_budget"}}"#)));
        assert_eq!(total, 500.0, "the wire reports the configured budget");
        assert_eq!(
            float_of(call(format!(r#"{{"session":{sid},"op":"work_spent"}}"#))),
            0.0
        );
        assert_eq!(
            float_of(call(format!(
                r#"{{"session":{sid},"op":"work_remaining"}}"#
            ))),
            total,
            "reading the meter over the wire must not charge it"
        );

        // A real kernel moves it, and remaining tracks.
        let _ = call(format!(
            r#"{{"session":{sid},"op":"degrees","g":{g},"s":"out"}}"#
        ));
        let spent = float_of(call(format!(r#"{{"session":{sid},"op":"work_spent"}}"#)));
        assert_eq!(spent, 4.0, "degrees charges one unit per vertex");
        assert_eq!(
            float_of(call(format!(
                r#"{{"session":{sid},"op":"work_remaining"}}"#
            ))),
            total - spent
        );
    }
}
