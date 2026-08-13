// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Phase 0B — instruction-count qualification pilot.
//!
//! This is **not** a perf gate. It is the pilot that decides which hot paths
//! *can* be gated on instruction counts at all, per
//! `docs/proposals/test_harness_implementation_plan_2026-08-12.md` §0B.
//!
//! # Why instruction counts
//!
//! GitHub-hosted runners vary ±20–30% run to run, so a wall-clock gate at any
//! useful threshold either fires constantly and gets disabled, or is set so
//! loose it catches nothing. Instruction counts under Callgrind are
//! deterministic to ~0.1% on the same noisy hardware.
//!
//! # Why a pilot rather than a gate
//!
//! Instruction counts miss I/O, cache effects and parallelism. A flush that does
//! the same work with worse locality regresses badly at a flat instruction
//! count; a commit dominated by `fsync` shows instruction noise with no
//! wall-clock meaning. Gating such a target is worse than not gating it, because
//! it trains everyone to ignore the gate.
//!
//! So all seven candidates are instrumented here, and only those that prove both
//! **stable** (CV < 1%) and **CPU-dominant** (instruction delta tracks
//! wall-clock delta under injected regressions) graduate to
//! `docs/perf/iai-qualification-*.md` and, later, to the Phase-7 gate. The
//! per-target `EXPECT` note on each function is the prior being tested, not a
//! conclusion.
//!
//! # Setup is deliberately outside the measured region
//!
//! Every target takes its context from a `#[bench::…(build_fn())]` argument
//! expression. iai-callgrind evaluates that expression outside the Callgrind
//! toggle, so fixture construction is not attributed to the benchmark. This
//! matters more than usual here: Valgrind carries a 10–50× runtime multiplier,
//! and a counted setup would both dwarf the measurement and swamp its variance.
//!
//! Fixtures are correspondingly small — this measures repeatability of a metric,
//! not throughput at scale.
//!
//! Run with (requires `valgrind` and a matching `iai-callgrind-runner`):
//!
//! ```text
//! cargo bench -p uni-db --bench hot_paths_iai
//! ```

use std::collections::HashMap;
use std::hint::black_box;

use iai_callgrind::{library_benchmark, library_benchmark_group, main};
use tokio::runtime::Runtime;
use uni_common::core::id::Vid;
use uni_db::{
    DataType, IndexType, Session, Uni, Value, VectorAlgo, VectorIndexCfg, VectorMetric, unival,
};

/// Graph fixture size. Small on purpose: under Valgrind, setup wall-clock is the
/// binding constraint, and none of these targets need scale to answer "is this
/// metric repeatable?".
const PERSONS: usize = 500;
const COMPANIES: usize = 20;
const EDGES: usize = 1_000;

/// Vector fixture size and dimensionality for the ANN target.
const DOCS: usize = 2_000;
const DIM: usize = 16;

/// Rows written but left unflushed, for the L0/L1 boundary and flush targets.
const DIRTY_ROWS: usize = 200;

// ── fixtures ────────────────────────────────────────────────────────────────

/// Deterministic xorshift64*, so every pilot run measures identical work.
/// Nondeterministic fixture data would show up as metric variance and be
/// misread as the metric being unstable.
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        self.0 = x;
        x.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }

    fn unit(&mut self) -> f32 {
        (self.next() >> 40) as f32 / f32::from(1u16 << 8) / 96.0
    }
}

/// A built graph fixture plus the runtime that owns it.
///
/// `session` is built in setup, not in the measured body. `Session::new_base`
/// allocates a fresh session-local plugin registry, metrics, plan cache, write
/// guard and cancellation token; measuring that alongside a query buries the
/// query. Phase 0B's first run with real counts showed five read targets within
/// 1.7% of each other despite doing wildly different work — the signature of a
/// dominant fixed cost, quantified by `baseline_session_only` below.
struct GraphCtx {
    rt: Runtime,
    db: Uni,
    session: Session,
    /// A `Vid` known to exist, for the id-lookup target.
    vid: Vid,
}

/// A built vector fixture plus a query vector.
struct VectorCtx {
    rt: Runtime,
    /// Held for its lifetime, never read: `Uni::temporary()` owns the fixture's
    /// temp directory and deletes it on drop, so releasing this would pull the
    /// storage out from under `session`.
    _db: Uni,
    session: Session,
    query: Vec<f32>,
}

/// A **current-thread** runtime, deliberately.
///
/// Two reasons, both discovered in Phase 0B rather than assumed:
///
/// 1. **Callgrind's default toggle only fires on the thread that enters the
///    benchmark function.** iai-callgrind runs with `--collect-atstart=no` and
///    toggles on entry to the bench fn; work dispatched to tokio worker threads
///    is dumped to per-thread files that collected nothing. On a multi-thread
///    runtime the interesting work lands on workers and is simply not counted.
/// 2. **Thread scheduling makes instruction counts nondeterministic**, which is
///    the one property an instruction-count gate cannot tolerate.
///
/// Phase 0A established that a current-thread runtime performs flush, fork and
/// snapshot+pin without trouble (`docs/perf/dqp-feasibility-2026-08-12.md` §5),
/// so this costs nothing in coverage.
///
/// Caveat this does **not** fix: work that Lance or DataFusion hand to their own
/// pools (`spawn_blocking`, rayon) still lands off-thread and stays uncounted. A
/// target whose measured instruction count is implausibly low against its
/// wall-clock is showing exactly that, and does not qualify for gating.
fn runtime() -> Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("tokio runtime")
}

async fn build_graph() -> anyhow::Result<(Uni, Vid)> {
    let db = Uni::temporary().build().await?;
    db.schema()
        .label("Person")
        .property("name", DataType::String)
        .property_nullable("age", DataType::Int)
        .done()
        .label("Company")
        .property("name", DataType::String)
        .done()
        .edge_type("WORKS_AT", &["Person"], &["Company"])
        .apply()
        .await?;

    let tx = db.session().tx().await?;
    let persons: Vec<HashMap<String, Value>> = (0..PERSONS)
        .map(|i| {
            let mut p = HashMap::new();
            p.insert("name".to_string(), unival!(format!("p{i}")));
            p.insert("age".to_string(), unival!((i % 60) as i64 + 18));
            p
        })
        .collect();
    let person_vids = tx.bulk_insert_vertices("Person", persons).await?;

    let companies: Vec<HashMap<String, Value>> = (0..COMPANIES)
        .map(|i| {
            let mut p = HashMap::new();
            p.insert("name".to_string(), unival!(format!("c{i}")));
            p
        })
        .collect();
    let company_vids = tx.bulk_insert_vertices("Company", companies).await?;

    let edges: Vec<(Vid, Vid, HashMap<String, Value>)> = (0..EDGES)
        .map(|i| {
            (
                person_vids[i % person_vids.len()],
                company_vids[(i * 7 + 13) % company_vids.len()],
                HashMap::new(),
            )
        })
        .collect();
    tx.bulk_insert_edges("WORKS_AT", edges).await?;
    tx.commit().await?;
    db.flush().await?;

    let vid = person_vids[PERSONS / 2];
    Ok((db, vid))
}

/// A flushed graph fixture — everything in L1, nothing dirty.
fn graph_ctx() -> GraphCtx {
    let rt = runtime();
    let (db, vid) = rt.block_on(build_graph()).expect("graph fixture");
    let session = db.session();
    GraphCtx {
        rt,
        db,
        session,
        vid,
    }
}

/// A flushed fixture with a warmed adjacency structure, so the traversal target
/// measures steady-state expansion rather than a one-off warm-up.
fn graph_ctx_warm() -> GraphCtx {
    let ctx = graph_ctx();
    ctx.rt.block_on(async {
        ctx.db
            .session()
            .query("MATCH (a:Person)-[:WORKS_AT]->(b:Company) RETURN b.name AS n")
            .await
            .expect("warm-up traversal");
    });
    ctx
}

/// A flushed fixture with `DIRTY_ROWS` uncommitted-to-L1 rows on top, so reads
/// must merge L0 over L1.
fn graph_ctx_dirty() -> GraphCtx {
    let ctx = graph_ctx();
    ctx.rt.block_on(async {
        let tx = ctx.session.tx().await.expect("tx");
        let rows: Vec<HashMap<String, Value>> = (0..DIRTY_ROWS)
            .map(|i| {
                let mut p = HashMap::new();
                p.insert("name".to_string(), unival!(format!("dirty{i}")));
                p.insert("age".to_string(), unival!(42i64));
                p
            })
            .collect();
        tx.bulk_insert_vertices("Person", rows)
            .await
            .expect("dirty rows");
        tx.commit().await.expect("commit");
    });
    ctx
}

async fn build_vectors() -> anyhow::Result<(Uni, Vec<f32>)> {
    let db = Uni::temporary().build().await?;
    db.schema()
        .label("Doc")
        .property("title", DataType::String)
        .property("emb", DataType::Vector { dimensions: DIM })
        .index(
            "emb",
            IndexType::Vector(VectorIndexCfg {
                algorithm: VectorAlgo::Hnsw {
                    m: 16,
                    ef_construction: 100,
                    partitions: None,
                },
                metric: VectorMetric::Cosine,
                embedding: None,
            }),
        )
        .apply()
        .await?;

    let mut rng = Rng(0x0BAD_5EED);
    let tx = db.session().tx().await?;
    for i in 0..DOCS {
        let v: Vec<f32> = (0..DIM).map(|_| rng.unit()).collect();
        tx.execute_with("CREATE (:Doc {title: $title, emb: $emb})")
            .param("title", Value::String(format!("d{i}")))
            .param("emb", Value::Vector(v))
            .run()
            .await?;
    }
    tx.commit().await?;
    db.flush().await?;
    // Force the ANN structure to be built over the flushed corpus, so the
    // measured query exercises the index rather than a brute-force fallback —
    // the same vacuity trap `fork_index_recall_bench.rs` fell into.
    db.indexes().rebuild("Doc", false).await?;

    let query: Vec<f32> = (0..DIM).map(|_| rng.unit()).collect();
    Ok((db, query))
}

fn vector_ctx() -> VectorCtx {
    let rt = runtime();
    let (db, query) = rt.block_on(build_vectors()).expect("vector fixture");
    let session = db.session();
    VectorCtx {
        rt,
        _db: db,
        session,
        query,
    }
}

// ── targets ─────────────────────────────────────────────────────────────────

// ── baselines ───────────────────────────────────────────────────────────────
//
// A measurement is only interpretable against its own floor. These two targets
// are not candidates for gating; they exist so every number below can be read as
// "target cost" rather than "target cost plus an unknown constant".
//
// Without them, Phase 0B's first real run looked plausible — seven non-zero
// counts, no regressions — while five read targets sat within 1.7% of each other
// despite doing entirely different work. That is only diagnosable against a
// baseline.

// Pure harness overhead: the Callgrind toggle, the argument black_box, and
// nothing else. Everything measured must be read as a delta over this.
#[library_benchmark]
#[bench::noop(())]
fn baseline_noop(unit: ()) -> usize {
    black_box(unit);
    black_box(1)
}

// Harness overhead plus one `Session` construction and one `block_on`.
//
// `db.session()` is documented as cheap and infallible, and at the API surface it
// reads that way. `Session::new_base` nonetheless allocates a fresh session-local
// plugin registry, metrics, plan cache, write guard and cancellation token. If
// this baseline lands near the read targets' totals, those targets are measuring
// session construction, not the query.
#[library_benchmark]
#[bench::session_only(graph_ctx())]
fn baseline_session_only(ctx: GraphCtx) -> usize {
    ctx.rt.block_on(async {
        let s = ctx.db.session();
        black_box(s.metrics().queries_executed as usize)
    })
}

// ── targets ─────────────────────────────────────────────────────────────────

// EXPECT: qualifies (CPU-dominant).
//
// The session is built in setup and used once here, so its plan cache is still
// cold: this counts parse + plan + a trivially-empty execution. The predicate
// matches no row on purpose, to keep execution from dominating the parse/plan
// cost under test.
#[library_benchmark]
#[bench::cold(graph_ctx())]
fn parse_and_plan_cold(ctx: GraphCtx) -> usize {
    ctx.rt.block_on(async {
        let r = ctx
            .session
            .query("MATCH (p:Person) WHERE p.age > 100000 RETURN p.name AS c0")
            .await
            .expect("query");
        black_box(r.len())
    })
}

// EXPECT: qualifies (CPU-dominant).
#[library_benchmark]
#[bench::by_id(graph_ctx())]
fn vertex_lookup_by_id(ctx: GraphCtx) -> usize {
    let vid = ctx.vid;
    ctx.rt.block_on(async {
        let r = ctx
            .session
            .query_with("MATCH (p:Person) WHERE id(p) = $vid RETURN p.name AS c0")
            .param("vid", unival!(vid.as_u64() as i64))
            .fetch_all()
            .await
            .expect("query");
        black_box(r.len())
    })
}

// EXPECT: qualifies (CPU-dominant).
#[library_benchmark]
#[bench::warm(graph_ctx_warm())]
fn expand_batch_one_hop_warm(ctx: GraphCtx) -> usize {
    ctx.rt.block_on(async {
        let r = ctx
            .session
            .query("MATCH (a:Person)-[:WORKS_AT]->(b:Company) RETURN b.name AS c0")
            .await
            .expect("query");
        black_box(r.len())
    })
}

// EXPECT: mixed — the pilot decides.
//
// Reads must merge `DIRTY_ROWS` of L0 over the flushed L1 corpus.
#[library_benchmark]
#[bench::l0_over_l1(graph_ctx_dirty())]
fn property_read_across_l0_l1(ctx: GraphCtx) -> usize {
    ctx.rt.block_on(async {
        let r = ctx
            .session
            .query("MATCH (p:Person) WHERE p.age = 42 RETURN p.name AS c0")
            .await
            .expect("query");
        black_box(r.len())
    })
}

// EXPECT: **does not** qualify (IO-dominant — WAL `fsync`).
//
// Instrumented anyway: the pilot's job is to confirm or refute the prior with a
// number, not to assume it.
#[library_benchmark]
#[bench::wal_on(graph_ctx())]
fn transaction_commit_wal_on(ctx: GraphCtx) -> usize {
    ctx.rt.block_on(async {
        let tx = ctx.session.tx().await.expect("tx");
        tx.execute("CREATE (:Person {name: 'committed', age: 33})")
            .await
            .expect("create");
        tx.commit().await.expect("commit");
        black_box(1)
    })
}

// EXPECT: **does not** qualify (IO-dominant — Lance write + manifest commit).
#[library_benchmark]
#[bench::l0_to_l1(graph_ctx_dirty())]
fn l0_to_l1_flush(ctx: GraphCtx) -> usize {
    ctx.rt.block_on(async {
        ctx.session.flush().await.expect("flush");
        black_box(1)
    })
}

// EXPECT: cache-dominant — the pilot decides.
#[library_benchmark]
#[bench::top10(vector_ctx())]
fn hnsw_top10_search(ctx: VectorCtx) -> usize {
    ctx.rt.block_on(async {
        let r = ctx
            .session
            .query_with(
                "CALL uni.vector.query('Doc', 'emb', $q, $k, null, null, {ef_search: 100}) \
                 YIELD node, score RETURN node.title AS title",
            )
            .param("q", Value::Vector(ctx.query.clone()))
            .param("k", unival!(10i64))
            .fetch_all()
            .await
            .expect("vector query");
        black_box(r.len())
    })
}

library_benchmark_group!(
    name = baselines;
    benchmarks = baseline_noop, baseline_session_only
);

library_benchmark_group!(
    name = read_paths;
    benchmarks =
        parse_and_plan_cold,
        vertex_lookup_by_id,
        expand_batch_one_hop_warm,
        property_read_across_l0_l1,
        hnsw_top10_search
);

library_benchmark_group!(
    name = write_paths;
    benchmarks = transaction_commit_wal_on, l0_to_l1_flush
);

main!(
    library_benchmark_groups = baselines,
    read_paths,
    write_paths
);
