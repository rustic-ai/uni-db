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
//! # The measured region is bounded by *instrumentation*, not collection
//!
//! Callgrind has two independent switches, and the difference between them is
//! load-bearing here:
//!
//! * **Collection** (`--collect-atstart`, `--toggle-collect`) is **per-thread**.
//!   The default iai-callgrind setup toggles collection on entry to the
//!   benchmark function, so only the thread that enters it ever collects.
//! * **Instrumentation** (`--instr-atstart`,
//!   `CALLGRIND_{START,STOP}_INSTRUMENTATION`) is **process-global**.
//!
//! These targets do a large and growing share of their work off the calling
//! thread — `lance_core::utils::tokio::spawn_cpu` dispatches onto tokio's
//! blocking pool — so a per-thread collection toggle cannot see it. Measured
//! 2026-09-16: under the old toggle-based setup, `vertex_lookup_by_id`
//! attributed **7,895 Ir** to a query whose real all-thread cost is
//! **7,392,263 Ir** — 0.1%. Worse, the share that escaped grew with a
//! dependency bump (lance 7→11 / DataFusion 53→54), so the five read targets
//! collapsed to within a few percent of `baseline_session_only` and the gate
//! read a ~96% "improvement" for code that got 2.8% *cheaper*.
//!
//! So: run with `--instr-atstart=no --collect-atstart=yes` and
//! [`EntryPoint::None`], and bound each target with [`measured`], which flips
//! instrumentation on and off around the work. Every thread is then counted,
//! and only for the region under test. `iai_collect.py` already sums the
//! per-thread `callgrind.*.tNN.*.out` files and reports the off-main-thread
//! share, so a regression to per-thread blindness shows up as `off-thr %`
//! falling back to 0.
//!
//! Two consequences worth knowing:
//!
//! * Fixture construction now runs **uninstrumented** rather than merely
//!   uncollected, so the pilot is markedly faster — Valgrind's 10–50×
//!   multiplier no longer applies to setup.
//! * `stop_instrumentation` flushes Valgrind's simulated cache, so the D1/LL
//!   figures `iai_collect.py` also records carry an artificial warm-up. The
//!   gate is on `Ir`, which is unaffected; do not build a cache-miss gate on
//!   these numbers without revisiting this.
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

use iai_callgrind::{
    Callgrind, EntryPoint, LibraryBenchmarkConfig, library_benchmark, library_benchmark_group, main,
};
use tokio::runtime::Runtime;
use uni_common::core::id::Vid;
use uni_db::{
    DataType, IndexType, Session, Uni, UniConfig, Value, VectorAlgo, VectorIndexCfg, VectorMetric,
    unival,
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
/// 1. **Keep as much work as possible on one thread.** This no longer affects
///    whether work is *counted* — instrumentation gating counts every thread
///    (see the module docs) — but it still affects whether the count is
///    *repeatable*.
/// 2. **Thread scheduling makes instruction counts nondeterministic**, which is
///    the one property an instruction-count gate cannot tolerate. This is the
///    live constraint now: what off-thread work happens is deterministic, but
///    *whether a background task lands inside the measured window* is not, which
///    is why `fixture_db` disables the auto-flush timer.
///
/// Phase 0A established that a current-thread runtime performs flush, fork and
/// snapshot+pin without trouble (`docs/perf/dqp-feasibility-2026-08-12.md` §5),
/// so this costs nothing in coverage.
///
/// Caveat this does **not** fix: work that Lance or DataFusion hand to their own
/// pools (`spawn_cpu`, `spawn_blocking`, rayon) still lands off-thread. It is now
/// counted, but it is counted wherever it happens to run, so a target whose
/// off-main-thread share is both large and variable is a stability risk rather
/// than a measurement gap — check `off-thr %` in `iai_cv.py`'s table before
/// gating one.
fn runtime() -> Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("tokio runtime")
}

/// Callgrind's start/stop-instrumentation client requests, issued directly.
///
/// iai-callgrind ships these behind its `client_requests` feature, which is not
/// used here on purpose: that feature runs `bindgen` in a build script and needs
/// `libclang` plus the system `valgrind/*.h` headers at compile time. The crate's
/// bundled `valgrind/include` headers are 200-byte docs.rs stubs, not the real
/// thing, so the feature is only usable where `valgrind-devel` (Fedora) or an
/// equivalent is installed — and it would be pulled in by every lane that builds
/// `--all-targets`, not just the perf gate. A missing header degrades to a
/// runtime `panic!`, mid-measurement.
///
/// The request itself is a documented, stable ABI — `valgrind.h` is BSD-licensed
/// precisely so client programs can embed it — and is four rotations plus a
/// marker instruction. The sequences below mirror iai-callgrind 0.16.1's own
/// `client_requests::arch::{x86_64,aarch64}`; the request numbers are
/// `VG_USERREQ_TOOL_BASE('C','T') + ordinal` from `callgrind.h`:
///
/// ```text
/// VG_USERREQ__START_INSTRUMENTATION = 0x4354_0004
/// VG_USERREQ__STOP_INSTRUMENTATION  = 0x4354_0005
/// ```
///
/// Outside Valgrind the sequence is a no-op — the rotations sum to a full
/// register width, so the scratch register is left unchanged — which is why it is
/// safe to leave compiled into the binary unconditionally.
mod client_request {
    const START_INSTRUMENTATION: usize = 0x4354_0004;
    const STOP_INSTRUMENTATION: usize = 0x4354_0005;

    #[cfg(any(target_arch = "x86_64", target_arch = "aarch64"))]
    #[inline(always)]
    fn request(request: usize) {
        let args: [usize; 6] = [request, 0, 0, 0, 0, 0];
        // SAFETY: the instruction sequence is Valgrind's documented client-request
        // marker. Under Valgrind it is trapped and emulated; outside it, the
        // rotations cancel and the whole block is a no-op. No memory is accessed
        // except `args`, which outlives the call.
        unsafe {
            #[cfg(target_arch = "x86_64")]
            core::arch::asm! {
                "rol rdi,3",
                "rol rdi,13",
                "rol rdi,61",
                "rol rdi,51",
                "xchg rbx, rbx",
                in("rax") args.as_ptr(),
                in("rdx") 0usize,
                lateout("rdx") _,
            };
            #[cfg(target_arch = "aarch64")]
            core::arch::asm! {
                "ror x12, x12, 3",
                "ror x12, x12, 13",
                "ror x12, x12, 51",
                "ror x12, x12, 61",
                "orr x10, x10, x10",
                in("x3") 0usize,
                in("x4") args.as_ptr(),
                lateout("x3") _,
            };
        }
    }

    // Any other architecture: fail loudly rather than measure nothing. A silent
    // no-op here would report every target at roughly zero instructions, which
    // is precisely the vacuously-green shape this file exists to prevent.
    #[cfg(not(any(target_arch = "x86_64", target_arch = "aarch64")))]
    fn request(_request: usize) {
        panic!(
            "hot_paths_iai: Callgrind client requests are implemented for x86_64 and aarch64 \
             only; this target would measure nothing. Add the sequence for this architecture \
             from valgrind.h before running the perf gate here."
        );
    }

    #[inline(always)]
    pub fn start_instrumentation() {
        request(START_INSTRUMENTATION);
    }

    #[inline(always)]
    pub fn stop_instrumentation() {
        request(STOP_INSTRUMENTATION);
    }
}

use client_request::{start_instrumentation, stop_instrumentation};

/// Runs `f` with Callgrind instrumentation on, and nothing else.
///
/// This is the measured region. Because instrumentation is process-global (see
/// the module docs), work `f` hands to another thread — tokio's blocking pool,
/// a Lance `spawn_cpu`, a scoped thread — is counted too, which a
/// `--toggle-collect` entry point cannot do.
///
/// **Every benchmark body in this file must be wrapped in this**, including the
/// baselines: the baselines are only interpretable as a floor if they carry the
/// same start/stop overhead as the targets they are subtracted from. A target
/// that forgets the wrapper measures nothing at all and reads as a ~100%
/// improvement, which `--fail-improve-pct` in `scripts/perf/iai_gate.py` is
/// there to catch.
#[inline(always)]
fn measured<T>(f: impl FnOnce() -> T) -> T {
    start_instrumentation();
    let out = f();
    stop_instrumentation();
    out
}

/// A fixture database with **time-based auto-flush disabled**.
///
/// `UniConfig::default()` sets `auto_flush_interval` to 5s, and a background
/// flush that lands inside the measured region adds ~700k instructions on the
/// blocking pool. Under the old collection toggle that was invisible, so the
/// targets looked stable; once off-thread work is counted it showed up as
/// `property_read_across_l0_l1` going bimodal — 30k off-thread instructions on
/// three runs of five, 745k on the other two, CV 2.66% against a gate set at 2%.
///
/// Disabling it is not merely variance reduction. `graph_ctx_dirty` exists to
/// measure a read that merges L0 over L1; a timer that drains L0 mid-run
/// destroys the condition under test, so some fraction of those runs were
/// measuring the wrong thing rather than the same thing noisily.
fn fixture_db() -> uni_db::UniBuilder {
    Uni::temporary().config(UniConfig {
        auto_flush_interval: None,
        ..Default::default()
    })
}

async fn build_graph() -> anyhow::Result<(Uni, Vid)> {
    let db = fixture_db().build().await?;
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
    let db = fixture_db().build().await?;
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

/// Iterations for `baseline_noop`. Large enough to exceed the instrumentation
/// gate's granularity, small enough to stay a rounding error against any target.
const NOOP_ITERS: usize = 1_000;

// A **positive control** for the instrumentation gate, and the floor every other
// number is read against.
//
// It is not a true no-op, deliberately. Under the old collection toggle this
// target measured 4 Ir — genuine harness overhead, because the toggle fired on
// function entry. Instrumentation gating has coarser granularity: Valgrind
// re-translates code when instrumentation is switched on, so a two-instruction
// region reads a flat **0**, and a zero here is indistinguishable from "the
// client request silently did nothing" — the exact vacuously-green shape that
// `iai_collect.py`'s zero-check exists to catch.
//
// So it does a fixed, trivial amount of real work. If `measured` ever stops
// turning instrumentation on, this reads 0 and `iai_collect.py` says so loudly.
// It also quantifies the gate's floor: whatever this costs is the resolution
// below which a target's number means nothing.
#[library_benchmark]
#[bench::noop(())]
fn baseline_noop(unit: ()) -> usize {
    black_box(unit);
    measured(|| {
        let mut acc = 0usize;
        for i in 0..NOOP_ITERS {
            acc = black_box(acc.wrapping_add(i));
        }
        black_box(acc)
    })
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
    measured(|| {
        ctx.rt.block_on(async {
            let s = ctx.db.session();
            black_box(s.metrics().queries_executed as usize)
        })
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
    measured(|| {
        ctx.rt.block_on(async {
            let r = ctx
                .session
                .query("MATCH (p:Person) WHERE p.age > 100000 RETURN p.name AS c0")
                .await
                .expect("query");
            black_box(r.len())
        })
    })
}

// EXPECT: qualifies (CPU-dominant).
#[library_benchmark]
#[bench::by_id(graph_ctx())]
fn vertex_lookup_by_id(ctx: GraphCtx) -> usize {
    let vid = ctx.vid;
    let r = measured(|| {
        ctx.rt.block_on(async {
            ctx.session
                .query_with("MATCH (p:Person) WHERE id(p) = $vid RETURN p.name AS c0")
                .param("vid", unival!(vid.as_u64() as i64))
                .fetch_all()
                .await
                .expect("query")
        })
    });
    // Check outside the measured window: an empty/wrong result must never look
    // like an instruction-count improvement, and validation is not query cost.
    assert_eq!(r.len(), 1, "id lookup must find the fixture vertex");
    assert_eq!(
        r.rows()[0].get::<String>("c0").expect("person name"),
        format!("p{}", PERSONS / 2),
        "id lookup must return the selected fixture vertex",
    );
    black_box(r.len())
}

// EXPECT: qualifies (CPU-dominant).
#[library_benchmark]
#[bench::warm(graph_ctx_warm())]
fn expand_batch_one_hop_warm(ctx: GraphCtx) -> usize {
    measured(|| {
        ctx.rt.block_on(async {
            let r = ctx
                .session
                .query("MATCH (a:Person)-[:WORKS_AT]->(b:Company) RETURN b.name AS c0")
                .await
                .expect("query");
            black_box(r.len())
        })
    })
}

// EXPECT: mixed — the pilot decides.
//
// Reads must merge `DIRTY_ROWS` of L0 over the flushed L1 corpus.
#[library_benchmark]
#[bench::l0_over_l1(graph_ctx_dirty())]
fn property_read_across_l0_l1(ctx: GraphCtx) -> usize {
    measured(|| {
        ctx.rt.block_on(async {
            let r = ctx
                .session
                .query("MATCH (p:Person) WHERE p.age = 42 RETURN p.name AS c0")
                .await
                .expect("query");
            black_box(r.len())
        })
    })
}

// EXPECT: **does not** qualify (IO-dominant — WAL `fsync`).
//
// Instrumented anyway: the pilot's job is to confirm or refute the prior with a
// number, not to assume it.
#[library_benchmark]
#[bench::wal_on(graph_ctx())]
fn transaction_commit_wal_on(ctx: GraphCtx) -> usize {
    measured(|| {
        ctx.rt.block_on(async {
            let tx = ctx.session.tx().await.expect("tx");
            tx.execute("CREATE (:Person {name: 'committed', age: 33})")
                .await
                .expect("create");
            tx.commit().await.expect("commit");
            black_box(1)
        })
    })
}

// EXPECT: **does not** qualify (IO-dominant — Lance write + manifest commit).
#[library_benchmark]
#[bench::l0_to_l1(graph_ctx_dirty())]
fn l0_to_l1_flush(ctx: GraphCtx) -> usize {
    measured(|| {
        ctx.rt.block_on(async {
            ctx.session.flush().await.expect("flush");
            black_box(1)
        })
    })
}

// EXPECT: cache-dominant — the pilot decides.
#[library_benchmark]
#[bench::top10(vector_ctx())]
fn hnsw_top10_search(ctx: VectorCtx) -> usize {
    measured(|| {
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
    config = LibraryBenchmarkConfig::default()
        .tool(
            // `--instr-atstart=no`: nothing is instrumented until `measured`
            // turns it on, so fixture construction costs nothing and cannot
            // leak into a target.
            //
            // `--collect-atstart=yes`: with instrumentation as the gate, every
            // thread must already be collecting when it opens — there is no
            // per-thread toggle to arm the blocking-pool workers.
            //
            // `EntryPoint::None`: drops iai-callgrind's default
            // `--toggle-collect=<bench fn>`. Leaving it on would re-arm
            // per-thread collection and count the bench thread twice over,
            // which is the bug this configuration exists to remove.
            Callgrind::with_args(["--instr-atstart=no", "--collect-atstart=yes"])
                .entry_point(EntryPoint::None)
        );
    library_benchmark_groups = baselines,
    read_paths,
    write_paths
);
