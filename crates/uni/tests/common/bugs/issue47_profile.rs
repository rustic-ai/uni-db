// SPDX-License-Identifier: Apache-2.0
// Profile edge creation at different iteration points to find the O(N) operator.

use uni_db::{DataType, IndexType, ScalarType, Uni, Value};

#[tokio::test]
#[ignore]
async fn profile_edge_creation() {
    let tmp = tempfile::tempdir().unwrap();
    let path = tmp.path().to_str().unwrap();

    let db = Uni::open(path).build().await.unwrap();

    db.schema()
        .label("Msg")
        .property("mid", DataType::String)
        .index("mid", IndexType::Scalar(ScalarType::Hash))
        .done()
        .label("User")
        .property("uid", DataType::String)
        .done()
        .edge_type("SENT_BY", &["Msg"], &["User"])
        .done()
        .apply()
        .await
        .unwrap();

    let session = db.session();

    // Create User
    let tx = session.tx().await.unwrap();
    tx.execute("CREATE (:User {uid: 'u1'})").await.unwrap();
    tx.commit().await.unwrap();

    let user_vid: i64 = session
        .query("MATCH (u:User {uid: 'u1'}) RETURN id(u) AS v")
        .await
        .unwrap()
        .rows()[0]
        .get("v")
        .unwrap();

    // Insert nodes first — 500 of them
    let mut msg_vids = Vec::new();
    for i in 0..500 {
        let tx = session.tx().await.unwrap();
        let result = tx
            .query_with("CREATE (m:Msg {mid: $mid}) RETURN id(m) AS v")
            .param("mid", Value::String(format!("m-{i:04}")))
            .fetch_all()
            .await
            .unwrap();
        let vid: i64 = result.rows()[0].get("v").unwrap();
        msg_vids.push(vid);
        tx.commit().await.unwrap();
    }

    // Create edges with profiling at specific points
    for (label, idx) in [
        ("EARLY (i=10)", 10),
        ("MID (i=250)", 250),
        ("LATE (i=490)", 490),
    ] {
        // Create all edges up to this point (without profiling)
        let start_from = if idx == 10 {
            0
        } else if idx == 250 {
            11
        } else {
            251
        };
        for vid in &msg_vids[start_from..idx] {
            let tx = session.tx().await.unwrap();
            tx.query_with(
                "MATCH (m), (u) WHERE id(m) = $mid AND id(u) = $uid CREATE (m)-[:SENT_BY]->(u)",
            )
            .param("mid", Value::Int(*vid))
            .param("uid", Value::Int(user_vid))
            .fetch_all()
            .await
            .unwrap();
            tx.commit().await.unwrap();
        }

        // Profile THIS specific edge creation (a write, so in a transaction:
        // session queries are read-only).
        let tx = session.tx().await.unwrap();
        let (_result, profile) = tx
            .query_with(
                "MATCH (m), (u) WHERE id(m) = $mid AND id(u) = $uid CREATE (m)-[:SENT_BY]->(u)",
            )
            .param("mid", Value::Int(msg_vids[idx]))
            .param("uid", Value::Int(user_vid))
            .profile()
            .await
            .unwrap();
        tx.commit().await.unwrap();

        eprintln!("\n=== PROFILE {} (after {} edges) ===", label, idx);
        eprintln!("Total: {}ms", profile.total_time_ms);
        eprintln!("{:<40} {:>8} {:>10}", "Operator", "Rows", "Time(ms)");
        eprintln!("{}", "-".repeat(62));
        for stat in &profile.runtime_stats {
            eprintln!(
                "{:<40} {:>8} {:>10.2}",
                stat.operator, stat.actual_rows, stat.time_ms
            );
        }
    }
}
