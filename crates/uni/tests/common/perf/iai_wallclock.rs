//! Phase 0B — the wall-clock half of the iai-callgrind qualification pilot.
//!
//! `crates/uni/benches/hot_paths_iai.rs` measures **instruction counts** for
//! seven candidate hot paths. Instruction counts are deterministic, which is what
//! makes them gateable — but determinism is only half of what a gate needs. A
//! target can be perfectly repeatable and still be a useless gate if its
//! instruction count does not move when its real performance does. An `fsync`-
//! dominated commit is the canonical case: the CPU work is flat while the
//! wall-clock is anything at all.
//!
//! This module supplies the other half by timing **the same seven operations on
//! the same fixtures**, outside Valgrind, and reporting **instructions per second
//! of wall-clock** — an effective instruction throughput.
//!
//! The qualification rule that follows:
//!
//! - A **CPU-dominant** target executes instructions the whole time it runs, so
//!   its throughput lands near the machine's real issue rate (order 10⁸–10⁹
//!   instr/s). Instruction count is then a faithful proxy for time, and the
//!   target can be gated.
//! - An **IO- or wait-dominant** target spends its wall-clock not executing, so
//!   its throughput comes out far lower. Instruction count is then *not* a proxy
//!   for time: it can stay flat through a real regression, and gating it trains
//!   everyone to ignore the gate.
//!
//! # Deliberate duplication
//!
//! The fixture builders below duplicate `benches/hot_paths_iai.rs`. Cargo gives
//! benches and tests no way to share code except through the crate's public API,
//! and both sides here use only public API. **The two must be kept in sync** —
//! the comparison is meaningless if the workloads drift apart. Both are marked
//! with `KEEP IN SYNC` at the fixture constants.
//!
//! Run with:
//!
//! ```text
//! cargo nextest run --profile soak -p uni-db --test integration \
//!   --run-ignored ignored-only -E 'test(iai_wallclock)' --no-capture
//! ```

use std::collections::HashMap;
use std::time::{Duration, Instant};

use uni_common::core::id::Vid;
use uni_db::{
    DataType, IndexType, Session, Uni, Value, VectorAlgo, VectorIndexCfg, VectorMetric, unival,
};

// KEEP IN SYNC with crates/uni/benches/hot_paths_iai.rs.
const PERSONS: usize = 500;
const COMPANIES: usize = 20;
const EDGES: usize = 1_000;
const DOCS: usize = 2_000;
const DIM: usize = 16;
const DIRTY_ROWS: usize = 200;

/// Repetitions per operation. The median is reported, so a single scheduling
/// hiccup cannot move the verdict.
const REPS: usize = 20;

/// Deterministic xorshift64*, matching the bench.
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
    Ok((db, person_vids[PERSONS / 2]))
}

async fn dirty(db: &Uni, session: &Session) -> anyhow::Result<()> {
    let tx = session.tx().await?;
    let rows: Vec<HashMap<String, Value>> = (0..DIRTY_ROWS)
        .map(|i| {
            let mut p = HashMap::new();
            p.insert("name".to_string(), unival!(format!("dirty{i}")));
            p.insert("age".to_string(), unival!(42i64));
            p
        })
        .collect();
    tx.bulk_insert_vertices("Person", rows).await?;
    tx.commit().await?;
    let _ = db;
    Ok(())
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
    db.indexes().rebuild("Doc", false).await?;
    let query: Vec<f32> = (0..DIM).map(|_| rng.unit()).collect();
    Ok((db, query))
}

/// Median of a set of samples.
fn median(mut xs: Vec<Duration>) -> Duration {
    xs.sort_unstable();
    xs[xs.len() / 2]
}

/// Runs `f` `REPS` times, returning the median of the durations **`f` itself
/// reports**.
///
/// The closure returns its own `Duration` rather than being wrapped in a timer,
/// so each target can do untimed preparation before starting the clock. That is
/// not a nicety — it is what keeps this half honest against the instruction-count
/// half:
///
/// - iai-callgrind executes each benchmark body **exactly once**, against a
///   session built in setup. So every measured query there runs with a **cold**
///   plan cache. A naive timing loop reusing one session would warm the cache on
///   rep 2 and report the warm median against the cold instruction count.
/// - The flush benchmark measures `flush()` alone; the rows it flushes are
///   dirtied in setup. A naive loop that re-dirties inside the timer measures
///   insert + flush and understates flush throughput.
///
/// Both mismatches were present in the first version of this file and were
/// caught by comparing the resulting throughputs against their priors.
async fn time_it<F, Fut>(mut f: F) -> anyhow::Result<Duration>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = anyhow::Result<Duration>>,
{
    let mut samples = Vec::with_capacity(REPS);
    for _ in 0..REPS {
        samples.push(f().await?);
    }
    Ok(median(samples))
}

/// Times the seven candidate targets and writes `wallclock.json`.
///
/// Pairs with `target/iai-pilot/run-*.json` from the instruction-count side;
/// `scripts/perf/iai_cv.py` and the qualification report consume both.
#[tokio::test(flavor = "current_thread")]
#[ignore = "soak: Phase 0B wall-clock leg of the iai qualification pilot"]
async fn iai_wallclock_pilot() -> anyhow::Result<()> {
    let mut out: Vec<(String, Duration)> = Vec::new();

    let (db, vid) = build_graph().await?;

    // Every read target below builds its session *outside* the timer, exactly as
    // the bench builds it in setup, and uses it once — so the plan cache is cold
    // on the timed call and the two halves measure the same thing.
    let d = time_it(|| async {
        let s = db.session();
        let t = Instant::now();
        s.query("MATCH (p:Person) WHERE p.age > 100000 RETURN p.name AS c0")
            .await?;
        Ok(t.elapsed())
    })
    .await?;
    out.push(("parse_and_plan_cold".into(), d));

    let d = time_it(|| async {
        let s = db.session();
        let t = Instant::now();
        s.query_with("MATCH (p:Person) WHERE id(p) = $vid RETURN p.name AS c0")
            .param("vid", unival!(vid.as_u64() as i64))
            .fetch_all()
            .await?;
        Ok(t.elapsed())
    })
    .await?;
    out.push(("vertex_lookup_by_id".into(), d));

    // Warm the adjacency once, as the bench's setup does.
    db.session()
        .query("MATCH (a:Person)-[:WORKS_AT]->(b:Company) RETURN b.name AS n")
        .await?;
    let d = time_it(|| async {
        let s = db.session();
        let t = Instant::now();
        s.query("MATCH (a:Person)-[:WORKS_AT]->(b:Company) RETURN b.name AS c0")
            .await?;
        Ok(t.elapsed())
    })
    .await?;
    out.push(("expand_batch_one_hop_warm".into(), d));

    let d = time_it(|| async {
        let s = db.session();
        let t = Instant::now();
        let tx = s.tx().await?;
        tx.execute("CREATE (:Person {name: 'committed', age: 33})")
            .await?;
        tx.commit().await?;
        Ok(t.elapsed())
    })
    .await?;
    out.push(("transaction_commit_wal_on".into(), d));

    // L0-over-L1 read: dirty once, then time reads that must merge L0 over L1.
    dirty(&db, &db.session()).await?;
    let d = time_it(|| async {
        let s = db.session();
        let t = Instant::now();
        s.query("MATCH (p:Person) WHERE p.age = 42 RETURN p.name AS c0")
            .await?;
        Ok(t.elapsed())
    })
    .await?;
    out.push(("property_read_across_l0_l1".into(), d));

    // Flush: the rows are dirtied *outside* the timer, matching the bench, which
    // measures `flush()` alone against rows its setup created.
    let d = time_it(|| async {
        let s = db.session();
        dirty(&db, &s).await?;
        let t = Instant::now();
        s.flush().await?;
        Ok(t.elapsed())
    })
    .await?;
    out.push(("l0_to_l1_flush".into(), d));

    // Vector path, on its own fixture.
    let (vdb, query) = build_vectors().await?;
    let d = time_it(|| async {
        let s = vdb.session();
        let t = Instant::now();
        s.query_with(
            "CALL uni.vector.query('Doc', 'emb', $q, $k, null, null, {ef_search: 100}) \
                 YIELD node, score RETURN node.title AS title",
        )
        .param("q", Value::Vector(query.clone()))
        .param("k", unival!(10i64))
        .fetch_all()
        .await?;
        Ok(t.elapsed())
    })
    .await?;
    out.push(("hnsw_top10_search".into(), d));

    let mut json = String::from("{\n");
    for (i, (name, d)) in out.iter().enumerate() {
        let comma = if i + 1 == out.len() { "" } else { "," };
        json.push_str(&format!(
            "  \"{name}\": {{ \"median_ns\": {} }}{comma}\n",
            d.as_nanos()
        ));
        eprintln!("{name:<40} median {d:?}");
    }
    json.push_str("}\n");

    let dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../target/iai-pilot");
    std::fs::create_dir_all(&dir)?;
    std::fs::write(dir.join("wallclock.json"), json)?;
    eprintln!("wrote {}", dir.join("wallclock.json").display());
    Ok(())
}
