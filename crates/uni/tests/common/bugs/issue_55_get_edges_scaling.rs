//! Repro for issue #55: `get_edges` latency scales with total graph size, not out-degree.
//!
//! A node with ~20 outgoing edges should return in sub-millisecond time
//! via CSR adjacency lookup (O(out-degree)). This test shows that
//! `get_edges` (via Cypher MATCH) takes progressively longer as the
//! graph grows, even though the target node's out-degree stays constant.
//!
//! Filed externally as <https://github.com/rustic-ai/uni-db/issues/55>.
//! Test imported verbatim from the customer's `uniko-store` repro at
//! `uniko2/crates/uniko-store/tests/get_edges_scaling_repro.rs` so that
//! we can investigate against the same workload they ran.
//!
//! Fixes: PR #1 (`0aabd2b4`) per-segment short-circuit in
//! `AdjacencyManager::get_neighbors`; PR #2 (`6d8e1035`) `table_exists` /
//! `get_table_schema` cache in `LanceDbBackend`; PR #3 makes the CSR
//! compaction threshold and time-based flush minimum-mutation default
//! tunable for ingest-heavy workloads.

use std::time::{Duration, Instant};

use uni_common::config::UniConfig;
use uni_db::{Uni, Value};

/// Number of "sessions" to link to the participant.
/// After setup, participant has exactly this many outgoing LINK edges.
const PARTICIPANT_EDGES: usize = 20;

/// Number of filler nodes + edges to add per round.
const FILLER_PER_ROUND: usize = 200;

/// Number of growth rounds.
const ROUNDS: usize = 15;

async fn setup_db() -> Uni {
    // This test issues ~6 200 single-row transactions, each of which takes the
    // writer's `flush_lock`. A commit can wait on that lock while a background
    // flush or compaction holds it, and the default `commit_timeout` (5s) bounds
    // only that wait -- so a slow background flush surfaces as a retriable
    // `CommitTimeout` on the next commit. Debug builds make it reachable:
    // CI runs this unoptimized, where the same work is several times slower than
    // the release timings this test was tuned against.
    //
    // The signal here is `get_edges` latency against graph size (issue #55),
    // not commit latency, and the regression guard below is release-only. So
    // give the guard headroom rather than let unrelated write-path contention
    // decide the result -- the same trade, for the same reason, as
    // `fork_writes_soak.rs` and `runtime/profile_test.rs`.
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

async fn measure_get_edges(db: &Uni, node_id: i64) -> (usize, f64) {
    let session = db.session();

    let start = Instant::now();
    let result = session
        .query_with("MATCH (a)-[r:LINK]->(b) WHERE id(a) = $nid RETURN id(r) AS eid")
        .param("nid", node_id)
        .fetch_all()
        .await
        .unwrap();
    let ms = start.elapsed().as_secs_f64() * 1000.0;

    (result.rows().len(), ms)
}

#[tokio::test]
async fn repro_get_edges_scales_with_graph_size() {
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

    let (count, baseline_ms) = measure_get_edges(&db, participant_id).await;
    assert_eq!(count, PARTICIPANT_EDGES);
    eprintln!(
        "baseline: {count} edges in {baseline_ms:.2}ms (graph has ~{} nodes)",
        PARTICIPANT_EDGES + 1
    );

    let mut total_filler = 0usize;
    let mut latencies = Vec::with_capacity(ROUNDS);
    latencies.push(("baseline".to_string(), baseline_ms));

    for round in 0..ROUNDS {
        for j in 0..FILLER_PER_ROUND {
            let msg_id = create_node(
                &db,
                "Message",
                &[(
                    "content",
                    Value::String(format!("filler message r{round}-{j}")),
                )],
            )
            .await;
            let target_session = session_ids[j % session_ids.len()];
            create_edge(&db, "IN_SESSION", msg_id, target_session).await;
        }
        total_filler += FILLER_PER_ROUND;

        let (count, ms) = measure_get_edges(&db, participant_id).await;
        assert_eq!(count, PARTICIPANT_EDGES, "out-degree should not change");
        let label = format!("round {round}: +{total_filler} filler");
        eprintln!("{label}: {count} edges in {ms:.2}ms");
        latencies.push((label, ms));
    }

    let final_ms = latencies.last().unwrap().1;
    eprintln!("\n--- Summary ---");
    eprintln!(
        "out-degree: {PARTICIPANT_EDGES} (constant), graph grew from {} to {} nodes",
        PARTICIPANT_EDGES + 1,
        PARTICIPANT_EDGES + 1 + ROUNDS * FILLER_PER_ROUND
    );
    eprintln!("baseline: {baseline_ms:.2}ms → final: {final_ms:.2}ms");
    eprintln!(
        "slowdown: {:.1}x",
        if baseline_ms > 0.0 {
            final_ms / baseline_ms
        } else {
            f64::NAN
        }
    );

    if final_ms > baseline_ms * 10.0 && final_ms > 5.0 {
        eprintln!(
            "\n⚠ REGRESSION: get_edges for {PARTICIPANT_EDGES} edges took {final_ms:.1}ms \
             after adding {total_filler} unrelated nodes. Expected O(out-degree) = ~{baseline_ms:.1}ms."
        );
    }

    // Issue #55 regression guard. Only enforced in release mode where the
    // numbers are stable enough not to flake; debug-mode timings are too
    // noisy to bound reliably (see commit log on this file).
    //
    // Pre-fix the slowdown was 2.7-4.6× across runs and machines. Post-fix
    // (PR #1: hot-path short-circuits in AdjacencyManager) the latency stays
    // flat. We allow up to 3× plus a 10ms floor to absorb CI noise.
    #[cfg(not(debug_assertions))]
    {
        let allowed_ms = (baseline_ms * 3.0).max(10.0);
        assert!(
            final_ms <= allowed_ms,
            "issue #55 regression: final {final_ms:.1}ms exceeds {allowed_ms:.1}ms \
             (baseline {baseline_ms:.1}ms, ratio {:.1}×)",
            final_ms / baseline_ms
        );
    }

    db.shutdown().await.unwrap();
}
