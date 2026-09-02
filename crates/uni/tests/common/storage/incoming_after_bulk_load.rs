// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Incoming (`<-`) traversal must answer the same in the process that loaded the
//! data as it does after a reopen.
//!
//! Found loading LDBC SNB: `MATCH (t:Tag)<-[:HAS_TAG]-() WITH t, count(*) ...`
//! returned **zero rows** in the loading process and the correct 24311 after
//! reopening the same on-disk graph. No error either way — a silent wrong
//! answer, which is the worst failure this suite looks for.

use std::collections::HashMap;

use anyhow::Result;
use uni_common::config::UniConfig;
use uni_db::{DataType, Uni, Value};

async fn open(path: &str) -> Result<Uni> {
    let config = UniConfig {
        auto_flush_threshold: 10_000,
        auto_flush_interval: None,
        ..Default::default()
    };
    Ok(Uni::open(path).config(config).build().await?)
}

async fn declare(db: &Uni) -> Result<()> {
    db.schema()
        .label("Src")
        .property("name", DataType::String)
        .apply()
        .await?;
    db.schema()
        .label("Dst")
        .property("name", DataType::String)
        .apply()
        .await?;
    // REPRO_TWO_DSTS adds a second destination label. If the mechanism is the
    // direction-blind `dst_labels` inference, `unique_dsts.len() != 1` takes the
    // "allow any target" path and the incoming query starts working — with the
    // data completely unchanged.
    if std::env::var("REPRO_TWO_DSTS").is_ok() {
        db.schema()
            .label("Other")
            .property("name", DataType::String)
            .apply()
            .await?;
        db.schema()
            .edge_type("POINTS_AT", &["Src"], &["Dst", "Other"])
            .apply()
            .await?;
    } else {
        db.schema()
            .edge_type("POINTS_AT", &["Src"], &["Dst"])
            .apply()
            .await?;
    }
    Ok(())
}

fn props(name: &str) -> HashMap<String, Value> {
    let mut p = HashMap::new();
    p.insert("name".to_string(), Value::String(name.to_string()));
    p
}

/// Count edges arriving at `Dst` — the shape that silently returned nothing.
async fn incoming_count(db: &Uni) -> Result<i64> {
    let rows = db
        .session()
        .query("MATCH (d:Dst)<-[:POINTS_AT]-() RETURN count(*) AS c")
        .await?;
    Ok(rows.rows()[0].get::<i64>("c")?)
}

async fn count(db: &Uni, q: &str) -> Result<i64> {
    let rows = db.session().query(q).await?;
    Ok(rows.rows()[0].get::<i64>("c")?)
}

/// Every way of asking "how many POINTS_AT edges are there" must agree.
///
/// The direction and the labelling of the *other* endpoint are presentation, not
/// selection: none of them changes which edges exist. Before the fix, the three
/// forms that traverse backward from `Dst` with an unlabelled counterpart
/// returned 0 while the rest returned 400.
async fn assert_all_forms_agree(db: &Uni, phase: &str, expected: i64) -> Result<()> {
    for (form, q) in [
        (
            "(s:Src)-[:R]->()",
            "MATCH (s:Src)-[:POINTS_AT]->() RETURN count(*) AS c",
        ),
        (
            "(s:Src)-[:R]->(d:Dst)",
            "MATCH (s:Src)-[:POINTS_AT]->(d:Dst) RETURN count(*) AS c",
        ),
        (
            "(d:Dst)<-[:R]-()",
            "MATCH (d:Dst)<-[:POINTS_AT]-() RETURN count(*) AS c",
        ),
        (
            "(d:Dst)<-[:R]-(s:Src)",
            "MATCH (d:Dst)<-[:POINTS_AT]-(s:Src) RETURN count(*) AS c",
        ),
        (
            "(d:Dst)-[:R]-()",
            "MATCH (d:Dst)-[:POINTS_AT]-() RETURN count(*) AS c",
        ),
        (
            "(:Src)-[:R]-()",
            "MATCH (:Src)-[:POINTS_AT]-() RETURN count(*) AS c",
        ),
        (
            "()-[:R]->()",
            "MATCH ()-[:POINTS_AT]->() RETURN count(*) AS c",
        ),
    ] {
        let n = count(db, q).await?;
        assert_eq!(
            n, expected,
            "[{phase}] {form} returned {n}, expected {expected} — the same edges, \
             counted a different way"
        );
    }
    Ok(())
}

/// Bulk-load, flush, and read back an incoming aggregation **in the same
/// process** — then reopen and read it again. Both must agree.
#[tokio::test]
async fn incoming_traversal_agrees_in_process_and_after_reopen() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().to_str().unwrap().to_string();

    const SRCS: usize = 400;
    const DSTS: usize = 20;

    let in_process = {
        let db = open(&path).await?;
        declare(&db).await?;

        let tx = db.session().tx().await?;
        let dsts = tx
            .bulk_insert_vertices("Dst", (0..DSTS).map(|i| props(&format!("d{i}"))).collect())
            .await?;
        let srcs = tx
            .bulk_insert_vertices("Src", (0..SRCS).map(|i| props(&format!("s{i}"))).collect())
            .await?;
        let edges: Vec<_> = srcs
            .iter()
            .enumerate()
            .map(|(i, s)| (*s, dsts[i % DSTS], HashMap::new()))
            .collect();
        tx.bulk_insert_edges("POINTS_AT", edges).await?;
        tx.commit().await?;
        db.flush().await?;

        assert_all_forms_agree(&db, "in-process", SRCS as i64).await?;
        incoming_count(&db).await?
    };

    let after_reopen = {
        let db = open(&path).await?;
        assert_all_forms_agree(&db, "reopened", SRCS as i64).await?;
        incoming_count(&db).await?
    };

    assert_eq!(in_process, after_reopen);
    Ok(())
}
