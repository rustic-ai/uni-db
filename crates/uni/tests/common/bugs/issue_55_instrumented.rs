//! Instrumented variant of issue #55 repro.
//!
//! Runs the SAME query twice per round:
//!   - Cold: first call (forces parse + plan + execute path)
//!   - Warm: immediate second call (hits the per-session plan cache, so
//!     parse + plan are skipped and only execute runs)
//!
//! Comparing how each grows isolates whether the scaling cost lives in
//! the planner or the execution / storage path.
//!
//! Findings (with default config):
//!   - Cold ≈ warm at every size — planning is NOT the bottleneck.
//!   - Latency is flat in the early rounds, then steps up to ~5 ms and
//!     plateaus. The step location varies between runs (round 4-7).
//!
//! Toggling `auto_flush_interval = None` (see commented line in `setup_db`)
//! moves the step from "varies between rounds 4-7" to "round 24" — exactly
//! when total mutations cross the 10,000 `auto_flush_threshold`. This
//! pinpoints the cost as L0 → L1 flush rotating data into
//! `AdjacencyManager::frozen_segments`, which subsequent reads must
//! iterate alongside `main_csr` and `active_overlay`. Both flush triggers
//! produce the same step magnitude.
//!
//! Uses only the public `uni_db` API, matching what a customer could run.

use std::time::{Duration, Instant};

#[allow(unused_imports)] // used when toggling the diagnostic experiment
use uni_db::UniConfig;
use uni_db::{Uni, Value};

const PARTICIPANT_EDGES: usize = 20;
const FILLER_PER_ROUND: usize = 200;
/// 30 rounds × 400 mutations/round = 12,000 — comfortably past the
/// default L0 auto_flush_threshold of 10,000, so we should see any
/// post-flush regime change.
const ROUNDS: usize = 30;
/// Number of warm/cold sample pairs taken per round; minimum is reported.
const SAMPLES_PER_ROUND: usize = 5;

async fn setup_db() -> Uni {
    // Default config: keep the 5-second timer enabled to reproduce the
    // bug as customers see it. To run the diagnostic experiment that
    // attributes the early step to the timer, swap to:
    //     let config = UniConfig { auto_flush_interval: None, ..UniConfig::default() };
    //     let db = Uni::in_memory().config(config).build().await.unwrap();
    //
    // Sequential single-row commits each take the writer's `flush_lock`, and the
    // default `commit_timeout` (5s) bounds only the wait for that lock -- so a
    // background flush or compaction holding it surfaces as a retriable
    // `CommitTimeout` on the next commit. CI runs this unoptimized, where the
    // same work is several times slower than the release timings it was tuned
    // against. The signal here is `get_edges` latency against graph size, not
    // commit latency, so give the guard headroom. Raising the guard rather than
    // disabling `auto_flush_interval` deliberately leaves the flush timer -- the
    // mechanism under test -- exactly as it was.
    let cfg = UniConfig {
        commit_timeout: Duration::from_secs(120),
        ..UniConfig::default()
    };
    let db = Uni::in_memory().config(cfg).build().await.unwrap();
    db.schema()
        .label("Participant")
        .property("name", uni_db::DataType::String)
        .done()
        .label("Session")
        .property("session_id", uni_db::DataType::String)
        .done()
        .label("Message")
        .property("content", uni_db::DataType::String)
        .done()
        .edge_type("LINK", &["Participant"], &["Session"])
        .done()
        .edge_type("IN_SESSION", &["Message"], &["Session"])
        .done()
        .apply()
        .await
        .unwrap();
    db
}

async fn create_node(db: &Uni, label: &str, props: &[(&str, Value)]) -> i64 {
    let session = db.session();
    let tx = session.tx().await.unwrap();
    let prop_str: Vec<String> = props
        .iter()
        .enumerate()
        .map(|(i, (k, _))| format!("{k}: $p{i}"))
        .collect();
    let cypher = format!(
        "CREATE (n:{label} {{{}}}) RETURN id(n) AS vid",
        prop_str.join(", ")
    );
    let mut qb = tx.query_with(&cypher);
    for (i, (_, v)) in props.iter().enumerate() {
        qb = qb.param(&format!("p{i}"), v.clone());
    }
    let result = qb.fetch_all().await.unwrap();
    tx.commit().await.unwrap();
    result.rows().first().unwrap().get::<i64>("vid").unwrap()
}

async fn create_edge(db: &Uni, edge_type: &str, from: i64, to: i64) -> i64 {
    let session = db.session();
    let tx = session.tx().await.unwrap();
    let cypher = format!(
        "MATCH (a), (b) WHERE id(a) = $src AND id(b) = $dst \
         CREATE (a)-[r:{edge_type}]->(b) RETURN id(r) AS eid"
    );
    let result = tx
        .query_with(&cypher)
        .param("src", from)
        .param("dst", to)
        .fetch_all()
        .await
        .unwrap();
    tx.commit().await.unwrap();
    result.rows().first().unwrap().get::<i64>("eid").unwrap()
}

/// Measure `get_edges` latency. Each session is independent — first call
/// is "cold" (no plan-cache hit). To get a "warm" measurement we keep
/// the same session and call again.
async fn measure_pair(db: &Uni, node_id: i64) -> (f64, f64) {
    let session = db.session();
    let q = "MATCH (a)-[r:LINK]->(b) WHERE id(a) = $nid RETURN id(r) AS eid";

    let t1 = Instant::now();
    let r1 = session
        .query_with(q)
        .param("nid", node_id)
        .fetch_all()
        .await
        .unwrap();
    let cold_ms = t1.elapsed().as_secs_f64() * 1000.0;
    assert_eq!(r1.rows().len(), PARTICIPANT_EDGES);

    let t2 = Instant::now();
    let r2 = session
        .query_with(q)
        .param("nid", node_id)
        .fetch_all()
        .await
        .unwrap();
    let warm_ms = t2.elapsed().as_secs_f64() * 1000.0;
    assert_eq!(r2.rows().len(), PARTICIPANT_EDGES);

    (cold_ms, warm_ms)
}

#[tokio::test]
async fn instrumented_get_edges_scaling() {
    let db = setup_db().await;

    let participant_id = create_node(
        &db,
        "Participant",
        &[("name", Value::String("alice".into()))],
    )
    .await;

    let mut session_ids = Vec::with_capacity(PARTICIPANT_EDGES);
    for i in 0..PARTICIPANT_EDGES {
        let sid = create_node(
            &db,
            "Session",
            &[("session_id", Value::String(format!("s-{i:03}")))],
        )
        .await;
        create_edge(&db, "LINK", participant_id, sid).await;
        session_ids.push(sid);
    }

    eprintln!(
        "{:>22} | {:>10} | {:>10} | {:>10} | {:>10}",
        "stage", "cold(min)", "cold(med)", "warm(min)", "warm(med)"
    );
    eprintln!("{}", "-".repeat(80));

    let report = |stage: &str, cold: &mut Vec<f64>, warm: &mut Vec<f64>| {
        cold.sort_by(|a, b| a.partial_cmp(b).unwrap());
        warm.sort_by(|a, b| a.partial_cmp(b).unwrap());
        let med = |v: &[f64]| v[v.len() / 2];
        eprintln!(
            "{:>22} | {:>10.2} | {:>10.2} | {:>10.2} | {:>10.2}",
            stage,
            cold[0],
            med(cold),
            warm[0],
            med(warm)
        );
    };

    let mut baseline_cold = Vec::with_capacity(SAMPLES_PER_ROUND);
    let mut baseline_warm = Vec::with_capacity(SAMPLES_PER_ROUND);
    for _ in 0..SAMPLES_PER_ROUND {
        let (c, w) = measure_pair(&db, participant_id).await;
        baseline_cold.push(c);
        baseline_warm.push(w);
    }
    report(
        "baseline (~21 nodes)",
        &mut baseline_cold,
        &mut baseline_warm,
    );

    let mut total_filler = 0usize;
    for round in 0..ROUNDS {
        for j in 0..FILLER_PER_ROUND {
            let msg_id = create_node(
                &db,
                "Message",
                &[("content", Value::String(format!("filler r{round}-{j}")))],
            )
            .await;
            let target_session = session_ids[j % session_ids.len()];
            create_edge(&db, "IN_SESSION", msg_id, target_session).await;
        }
        total_filler += FILLER_PER_ROUND;

        let mut colds = Vec::with_capacity(SAMPLES_PER_ROUND);
        let mut warms = Vec::with_capacity(SAMPLES_PER_ROUND);
        for _ in 0..SAMPLES_PER_ROUND {
            let (c, w) = measure_pair(&db, participant_id).await;
            colds.push(c);
            warms.push(w);
        }
        let stage = format!("round {round} (+{total_filler})");
        report(&stage, &mut colds, &mut warms);
    }

    db.shutdown().await.unwrap();
}
