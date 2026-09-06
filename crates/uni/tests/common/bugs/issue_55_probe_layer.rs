//! Probe for issue #55 root cause beneath the storage layer.
//!
//! After PR #1 (commit 0aabd2b4) added segment short-circuits to
//! `AdjacencyManager::get_neighbors`, the unit-tested storage hot path
//! is verified O(out-degree). But the customer-facing Cypher latency
//! still steps from ~1.7 ms to ~5 ms after the first L0 → L1 flush.
//!
//! That ~3 ms premium isn't in the storage call (already proved).
//! This probe compares THREE query shapes per round to narrow it down:
//!
//!   A. point lookup, no traversal:  `MATCH (a) WHERE id(a)=$nid RETURN id(a)`
//!   B. count-only traversal:        `MATCH (a)-[r:LINK]->() ... RETURN count(r)`
//!   C. full traversal w/ binding:   `MATCH (a)-[r:LINK]->(b) ... RETURN id(r)`
//!
//! If A also slows post-flush → cost is in per-query setup
//!   (QueryContext, plan cache, snapshot capture).
//! If A is flat but B+C slow → cost is in traversal / get_neighbors path.
//! If A+B flat but C slows → cost is in destination binding (visibility on b).

use std::time::{Duration, Instant};

use uni_db::{Uni, UniConfig, Value};

const PARTICIPANT_EDGES: usize = 20;
const FILLER_PER_ROUND: usize = 200;
const ROUNDS: usize = 15;
const SAMPLES_PER_ROUND: usize = 5;

async fn setup_db() -> Uni {
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

async fn measure_query(db: &Uni, query: &str, nid: i64, samples: usize) -> f64 {
    let session = db.session();
    let mut times = Vec::with_capacity(samples);
    for _ in 0..samples {
        let start = Instant::now();
        let _ = session
            .query_with(query)
            .param("nid", nid)
            .fetch_all()
            .await
            .unwrap();
        times.push(start.elapsed().as_secs_f64() * 1000.0);
    }
    times.sort_by(|a, b| a.partial_cmp(b).unwrap());
    times[0]
}

#[tokio::test]
#[cfg_attr(debug_assertions, allow(unused_variables, unused_assignments))]
async fn probe_get_edges_layer_attribution() {
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

    let q_a = "MATCH (a) WHERE id(a) = $nid RETURN id(a) AS eid";
    let q_b = "MATCH (a)-[r:LINK]->() WHERE id(a) = $nid RETURN count(r) AS eid";
    let q_c = "MATCH (a)-[r:LINK]->(b) WHERE id(a) = $nid RETURN id(r) AS eid";

    eprintln!(
        "{:>22} | {:>10} | {:>10} | {:>10}",
        "stage", "A no-trav", "B count", "C full"
    );
    eprintln!("{}", "-".repeat(64));

    let report = |stage: &str, a: f64, b: f64, c: f64| {
        eprintln!("{stage:>22} | {a:>10.2} | {b:>10.2} | {c:>10.2}");
    };

    let baseline_a = measure_query(&db, q_a, participant_id, SAMPLES_PER_ROUND).await;
    let baseline_b = measure_query(&db, q_b, participant_id, SAMPLES_PER_ROUND).await;
    let baseline_c = measure_query(&db, q_c, participant_id, SAMPLES_PER_ROUND).await;
    report("baseline (~21 nodes)", baseline_a, baseline_b, baseline_c);

    let mut last_a = baseline_a;
    let mut last_b = baseline_b;
    let mut last_c = baseline_c;
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

        let a = measure_query(&db, q_a, participant_id, SAMPLES_PER_ROUND).await;
        let b = measure_query(&db, q_b, participant_id, SAMPLES_PER_ROUND).await;
        let c = measure_query(&db, q_c, participant_id, SAMPLES_PER_ROUND).await;
        let stage = format!("round {round} (+{total_filler})");
        report(&stage, a, b, c);
        last_a = a;
        last_b = b;
        last_c = c;
    }

    // Issue #55 layer-attribution guard. After the table_exists/schema cache
    // landed (PR #2), per-query Lance overhead post-flush is bounded but
    // not zero. Assert ≤3× of baseline for each query shape, plus a 10 ms
    // floor for CI noise. Only enforced in release mode where the numbers
    // are stable enough not to flake.
    if !cfg!(debug_assertions) {
        let bound_a = (baseline_a * 3.0).max(10.0);
        let bound_b = (baseline_b * 3.0).max(10.0);
        let bound_c = (baseline_c * 3.0).max(10.0);
        assert!(
            last_a <= bound_a,
            "issue #55 query A regression: {last_a:.2}ms > {bound_a:.2}ms (baseline {baseline_a:.2}ms)"
        );
        assert!(
            last_b <= bound_b,
            "issue #55 query B regression: {last_b:.2}ms > {bound_b:.2}ms (baseline {baseline_b:.2}ms)"
        );
        assert!(
            last_c <= bound_c,
            "issue #55 query C regression: {last_c:.2}ms > {bound_c:.2}ms (baseline {baseline_c:.2}ms)"
        );
    }

    db.shutdown().await.unwrap();
}
