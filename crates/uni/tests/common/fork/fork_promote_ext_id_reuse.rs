// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Promote upsert resolves an `ext_id` to its live vertex after the id was
//! deleted and reused.
//!
//! The `ext_id` → VID map kept the first row it met per VID across L1
//! fragments, not the newest, so once a deleted vertex's `ext_id` was reused
//! the map could resolve to a tombstoned row. Promote upsert then missed the
//! live vertex and inserted a duplicate. The map now takes each VID's newest
//! row, a tombstone winning a tie, and respects the reader's pin.
//!
//! Run with:
//!   cargo nextest run -p uni-db --test integration -E 'test(fork_promote_ext_id_reuse)'

use anyhow::{Context, Result};
use uni_db::api::fork_diff::{PromoteOptions, PromotePattern};
use uni_db::{DataType, Uni};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn promote_upsert_updates_the_live_vertex_of_a_reused_ext_id() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let db = Uni::open(dir.path().to_str().context("utf-8")?)
        .build()
        .await?;
    db.schema()
        .label("Person")
        .property("name", DataType::String)
        .property("age", DataType::Int64)
        .apply()
        .await?;
    let session = db.session();
    // Several create/delete generations, each flushed, leave tombstoned rows
    // carrying the same ext_id in separate L1 fragments.
    for _ in 0..8 {
        let tx = session.tx().await?;
        tx.execute("CREATE (:Person {ext_id: 'p1', name: 'old', age: 1})")
            .await?;
        tx.commit().await?;
        db.flush().await?;
        let tx = session.tx().await?;
        tx.execute("MATCH (p:Person {ext_id: 'p1'}) DETACH DELETE p")
            .await?;
        tx.commit().await?;
        db.flush().await?;
    }
    let tx = session.tx().await?;
    tx.execute("CREATE (:Person {ext_id: 'p1', name: 'Alice', age: 30})")
        .await?;
    tx.commit().await?;
    db.flush().await?;

    session.fork("edit").await?;
    let tx = session.tx().await?;
    tx.execute("MATCH (p:Person {name: 'Alice'}) SET p.age = 99")
        .await?;
    tx.commit().await?;
    db.flush().await?;

    let report = db
        .promote_from_fork_with_options(
            "edit",
            &[PromotePattern::label("Person")],
            &PromoteOptions::with_upsert(),
        )
        .await;
    let ages: Vec<i64> = session
        .query("MATCH (p:Person) RETURN p.age AS age")
        .await?
        .rows()
        .iter()
        .map(|r| r.get::<i64>("age"))
        .collect::<std::result::Result<_, _>>()?;
    db.shutdown().await?;
    let report = report?;
    assert_eq!(
        (report.vertices_updated, report.vertices_inserted, ages),
        (1, 0, vec![30]),
        "one live Person, updated in place: {report:?}"
    );
    Ok(())
}
