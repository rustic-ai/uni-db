//! Repro for issue #55 (auto-embed variant): `get_edges` latency under auto-embedded
//! Message nodes — matching the real uniko2 benchmark setup, which amplifies
//! the basic scaling issue from ~5x to 100x+.
//!
//! Filed externally as <https://github.com/rustic-ai/uni-db/issues/55>.
//! Imported verbatim from the customer's `uniko-store` repro at
//! `uniko2/crates/uniko-store/tests/get_edges_scaling_autoembed_repro.rs`.
//!
//! A measurement probe: it asserts each round's edge count and prints the
//! latency growth, asserting none. Runs in the nightly ignored-test sweep
//! (`scripts/ci/ignored_tests.py`); to run it alone:
//!
//! ```text
//! cargo nextest run -p uni-db --test integration --run-ignored ignored-only --no-capture \
//!   -E 'test(repro_get_edges_scales_with_autoembed)'
//! ```

use std::time::Instant;

use uni_db::api::schema::EmbeddingCfg;
use uni_db::{
    DataType, IndexType, ModelAliasSpec, ModelTask, Uni, Value, VectorAlgo, VectorIndexCfg,
    VectorMetric, WarmupPolicy,
};

const PARTICIPANT_EDGES: usize = 20;
const FILLER_PER_ROUND: usize = 100;
const ROUNDS: usize = 20;

const MESSAGE_TEXT: &str = "I went to the farmer's market yesterday and picked up some \
    fresh strawberries and homemade jam. The weather was perfect for walking around.";

fn nomic_catalog() -> Vec<ModelAliasSpec> {
    vec![ModelAliasSpec {
        alias: "embed/default".to_string(),
        task: ModelTask::Embed,
        provider_id: "local/onnx".to_string(),
        model_id: "NomicEmbedTextV15".to_string(),
        revision: None,
        warmup: WarmupPolicy::Lazy,
        required: false,
        timeout: None,
        load_timeout: None,
        retry: None,
        options: serde_json::json!({}),
    }]
}

fn auto_embed_vector_index() -> IndexType {
    IndexType::Vector(VectorIndexCfg {
        algorithm: VectorAlgo::HnswSq {
            m: 16,
            ef_construction: 100,
            partitions: None,
        },
        metric: VectorMetric::Cosine,
        embedding: Some(EmbeddingCfg {
            alias: "embed/default".to_string(),
            source_properties: vec!["content".to_string()],
            batch_size: 32,
            document_prefix: Some("search_document: ".to_string()),
            query_prefix: Some("search_query: ".to_string()),
        }),
    })
}

async fn setup_db() -> Uni {
    let db = Uni::in_memory()
        .xervo_catalog(nomic_catalog())
        .build()
        .await
        .unwrap();

    db.schema()
        .label("Participant")
        .property("name", DataType::String)
        .done()
        .label("Session")
        .property("session_id", DataType::String)
        .done()
        .label("Message")
        .property("message_id", DataType::String)
        .property("content", DataType::String)
        .property("timestamp", DataType::DateTime)
        .property_nullable("embedding", DataType::Vector { dimensions: 768 })
        .index("embedding", auto_embed_vector_index())
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

async fn create_node(db: &Uni, cypher: &str, params: &[(&str, Value)]) -> i64 {
    let session = db.session();
    let tx = session.tx().await.unwrap();
    let mut qb = tx.query_with(cypher);
    for (k, v) in params {
        qb = qb.param(k, v.clone());
    }
    let result = qb.fetch_all().await.unwrap();
    tx.commit().await.unwrap();
    result.rows().first().unwrap().get::<i64>("vid").unwrap()
}

async fn create_edge(db: &Uni, edge_type: &str, from: i64, to: i64) {
    let session = db.session();
    let tx = session.tx().await.unwrap();
    let cypher = format!(
        "MATCH (a), (b) WHERE id(a) = $src AND id(b) = $dst \
         CREATE (a)-[r:{edge_type}]->(b) RETURN id(r) AS eid"
    );
    tx.query_with(&cypher)
        .param("src", from)
        .param("dst", to)
        .fetch_all()
        .await
        .unwrap();
    tx.commit().await.unwrap();
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
#[ignore = "measurement probe: issue #55 get_edges scaling under auto-embed; loads an ONNX \
            embedding model; run explicitly"]
async fn repro_get_edges_scales_with_autoembed() {
    let db = setup_db().await;

    let participant_id = create_node(
        &db,
        "CREATE (n:Participant {name: $p0}) RETURN id(n) AS vid",
        &[("p0", Value::String("alice".into()))],
    )
    .await;

    let mut session_ids = Vec::with_capacity(PARTICIPANT_EDGES);
    for i in 0..PARTICIPANT_EDGES {
        let sid = create_node(
            &db,
            "CREATE (n:Session {session_id: $p0}) RETURN id(n) AS vid",
            &[("p0", Value::String(format!("s-{i:03}")))],
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

    for round in 0..ROUNDS {
        let round_start = Instant::now();
        for j in 0..FILLER_PER_ROUND {
            let msg_id = create_node(
                &db,
                "CREATE (n:Message {message_id: $p0, content: $p1, \
                 timestamp: $p2}) RETURN id(n) AS vid",
                &[
                    ("p0", Value::String(format!("msg-r{round}-{j:03}"))),
                    (
                        "p1",
                        Value::String(format!("{MESSAGE_TEXT} (r{round} #{j})")),
                    ),
                    ("p2", Value::String(chrono::Utc::now().to_rfc3339())),
                ],
            )
            .await;
            let target_session = session_ids[j % session_ids.len()];
            create_edge(&db, "IN_SESSION", msg_id, target_session).await;
        }
        total_filler += FILLER_PER_ROUND;
        let insert_ms = round_start.elapsed().as_millis();

        let (count, ms) = measure_get_edges(&db, participant_id).await;
        assert_eq!(count, PARTICIPANT_EDGES, "out-degree should not change");
        eprintln!(
            "round {round}: +{total_filler} msgs ({insert_ms}ms insert) → \
             {count} edges in {ms:.2}ms"
        );
    }

    let (count, final_ms) = measure_get_edges(&db, participant_id).await;
    let slowdown = if baseline_ms > 0.0 {
        final_ms / baseline_ms
    } else {
        f64::NAN
    };

    eprintln!("\n--- Summary ---");
    eprintln!(
        "out-degree: {PARTICIPANT_EDGES} (constant), graph grew from {} to {} nodes",
        PARTICIPANT_EDGES + 1,
        PARTICIPANT_EDGES + 1 + ROUNDS * FILLER_PER_ROUND
    );
    eprintln!("baseline: {baseline_ms:.2}ms → final: {final_ms:.2}ms");
    eprintln!("slowdown: {slowdown:.1}x");

    if slowdown > 10.0 && final_ms > 50.0 {
        eprintln!(
            "\n⚠ CONFIRMED: get_edges for {count} edges took {final_ms:.1}ms \
             ({slowdown:.0}x slowdown) after adding {total_filler} auto-embedded messages."
        );
    }

    db.shutdown().await.unwrap();
}
