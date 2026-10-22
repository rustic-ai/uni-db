// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! A bulk load's declared edge endpoints are checked again when it publishes.
//!
//! `BulkWriter::insert_edges` checks an edge's endpoints when the edge is
//! buffered, but the edge stays invisible until `commit` publishes it. An
//! endpoint that existed before the load can lose its label in between: the
//! label-removal guard sees no edge on it. Measured on 7b5856437: the load
//! committed cleanly, the type stayed `enforced`, and the planner narrowed
//! `()-[r:E]->()` to the declared labels, so the published edge was silently
//! missing (0 rows) -- live and after a reopen.

// Rust guideline compliant

use std::collections::HashMap;

use uni_common::core::schema::EndpointConformance;
use uni_db::{Uni, UniConfig, Value, Vid};

fn config() -> UniConfig {
    UniConfig {
        auto_flush_threshold: usize::MAX,
        auto_flush_interval: None,
        ..Default::default()
    }
}

async fn count(db: &Uni, q: &str) -> i64 {
    db.session().query(q).await.unwrap().rows()[0]
        .get::<i64>("c")
        .unwrap()
}

fn conformance(db: &Uni) -> EndpointConformance {
    db.storage().schema_manager().schema().edge_types["E"]
        .endpoint_conformance
        .clone()
}

#[tokio::test]
async fn an_endpoint_that_loses_its_label_before_the_publish_downgrades_the_type() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_str().unwrap().to_string();
    {
        let db = Uni::open(&path).config(config()).build().await.unwrap();
        let tx = db.session().tx().await.unwrap();
        tx.execute("CREATE LABEL P (k INT)").await.unwrap();
        tx.execute("CREATE LABEL Q (k INT)").await.unwrap();
        tx.execute("CREATE EDGE TYPE E FROM P TO Q").await.unwrap();
        tx.commit().await.unwrap();
        let tx = db.session().tx().await.unwrap();
        tx.execute("CREATE (:Q {k: 1})").await.unwrap();
        tx.commit().await.unwrap();
        db.flush().await.unwrap();
        assert_eq!(conformance(&db), EndpointConformance::Enforced, "premise");
        let q = db
            .session()
            .query("MATCH (n:Q {k: 1}) RETURN id(n) AS id")
            .await
            .unwrap()
            .rows()[0]
            .get::<i64>("id")
            .unwrap();
        let q = Vid::from(q as u64);

        let tx = db.session().tx().await.unwrap();
        let mut bulk = tx.bulk_writer().build().unwrap();
        let p = bulk
            .insert_vertices("P", vec![HashMap::from([("k".to_string(), Value::Int(0))])])
            .await
            .unwrap();
        bulk.insert_edges("E", vec![uni_bulk::EdgeData::new(p[0], q, HashMap::new())])
            .await
            .unwrap();

        // The edge to `q` is still invisible: removing `Q` is allowed.
        let other = db.session().tx().await.unwrap();
        other
            .execute("MATCH (n:Q {k: 1}) REMOVE n:Q")
            .await
            .unwrap();
        other.commit().await.unwrap();

        let stats = bulk.commit().await.unwrap();
        tx.commit().await.unwrap();
        assert_eq!(
            count(&db, "MATCH ()-[r:E]->() RETURN count(r) AS c").await,
            1,
            "the published edge must be found"
        );
        assert!(
            stats.warnings.iter().any(|w| w.contains("edge type 'E'")),
            "the commit reports the downgrade: {:?}",
            stats.warnings
        );
        assert_eq!(conformance(&db), EndpointConformance::Unverified);
        db.shutdown().await.unwrap();
    }
    let db = Uni::open(&path).config(config()).build().await.unwrap();
    assert_eq!(conformance(&db), EndpointConformance::Unverified, "saved");
    assert_eq!(
        count(&db, "MATCH ()-[r:E]->() RETURN count(r) AS c").await,
        1,
        "after reopen"
    );
}

/// The re-check costs nothing when every endpoint still conforms: the type
/// keeps its record and the commit reports nothing.
#[tokio::test]
async fn a_conforming_load_keeps_the_types_record() {
    let dir = tempfile::tempdir().unwrap();
    let db = Uni::open(dir.path().to_str().unwrap())
        .config(config())
        .build()
        .await
        .unwrap();
    let tx = db.session().tx().await.unwrap();
    tx.execute("CREATE LABEL P (k INT)").await.unwrap();
    tx.execute("CREATE LABEL Q (k INT)").await.unwrap();
    tx.execute("CREATE EDGE TYPE E FROM P TO Q").await.unwrap();
    tx.execute("CREATE (:Q {k: 1})").await.unwrap();
    tx.commit().await.unwrap();
    let q = db
        .session()
        .query("MATCH (n:Q {k: 1}) RETURN id(n) AS id")
        .await
        .unwrap()
        .rows()[0]
        .get::<i64>("id")
        .unwrap();

    let tx = db.session().tx().await.unwrap();
    let mut bulk = tx.bulk_writer().build().unwrap();
    let p = bulk
        .insert_vertices("P", vec![HashMap::from([("k".to_string(), Value::Int(0))])])
        .await
        .unwrap();
    bulk.insert_edges(
        "E",
        vec![uni_bulk::EdgeData::new(
            p[0],
            Vid::from(q as u64),
            HashMap::new(),
        )],
    )
    .await
    .unwrap();
    let stats = bulk.commit().await.unwrap();
    tx.commit().await.unwrap();
    assert!(stats.warnings.is_empty(), "{:?}", stats.warnings);
    assert_eq!(conformance(&db), EndpointConformance::Enforced);
    assert_eq!(
        count(&db, "MATCH ()-[r:E]->() RETURN count(r) AS c").await,
        1
    );
}
