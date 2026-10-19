// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! A batched vertex insert must enforce `ext_id` uniqueness against
//! flushed storage, matching the single-vertex CREATE path. Before the
//! fix, `validate_vertex_batch_constraints` checked only the in-memory L0
//! buffers, so a batch CREATE whose `ext_id` already existed in L1 was
//! silently accepted as a duplicate.

// Rust guideline compliant

use anyhow::Result;
use uni_db::{DataType, Uni};

#[tokio::test]
async fn batch_create_rejects_storage_duplicate_ext_id() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let db = Uni::open(dir.path().to_str().unwrap()).build().await?;
    db.schema()
        .label("Person")
        .property("name", DataType::String)
        .apply()
        .await?;
    let session = db.session();

    // Seed `p1` and flush it to L1 (so the L0-only check would miss it).
    let tx = session.tx().await?;
    tx.execute("CREATE (:Person {ext_id: 'p1', name: 'A'})")
        .await?;
    tx.commit().await?;
    db.flush().await?;

    // A batched create (UNWIND … CREATE) where one row reuses the flushed
    // `ext_id` 'p1'. Must error, not silently insert a duplicate.
    let tx = session.tx().await?;
    let result = tx
        .execute(
            "UNWIND [{e: 'p2', n: 'B'}, {e: 'p1', n: 'C'}] AS row \
             CREATE (:Person {ext_id: row.e, name: row.n})",
        )
        .await;
    assert!(
        result.is_err(),
        "batch create reusing a flushed ext_id must error: {result:?}"
    );

    drop(db);
    Ok(())
}

/// The ways a write can take an `ext_id`: a single CREATE, a SET on an
/// existing vertex, a batched UNWIND CREATE, and the transaction bulk insert.
const REUSES: [&str; 4] = ["create", "set", "unwind", "bulk"];

/// Take `ext_id: 'x1'` through `how`; the vertex that ends up with it has
/// `uid: 'new'`.
async fn take_x1(tx: &uni_db::Transaction, how: &str) -> std::result::Result<(), uni_db::UniError> {
    match how {
        "create" => tx
            .execute("CREATE (:E {uid: 'new', ext_id: 'x1'})")
            .await
            .map(|_| ()),
        "set" => tx
            .execute("MATCH (a:E {uid: 'u2'}) SET a.uid = 'new', a.ext_id = 'x1'")
            .await
            .map(|_| ()),
        "unwind" => tx
            .execute(
                "UNWIND [{u: 'pad', e: 'x9'}, {u: 'new', e: 'x1'}] AS row \
                 CREATE (:E {uid: row.u, ext_id: row.e})",
            )
            .await
            .map(|_| ()),
        "bulk" => {
            let mut row = std::collections::HashMap::new();
            row.insert("uid".to_string(), uni_db::Value::String("new".into()));
            row.insert("ext_id".to_string(), uni_db::Value::String("x1".into()));
            tx.bulk_insert_vertices("E", vec![row]).await.map(|_| ())
        }
        other => unreachable!("{other}"),
    }
}

/// A fresh store with `u1` holding `x1` and `u2` holding `x2`, plus `setup`
/// applied, flushed as `flush_base` / `flush_setup` say.
async fn ext_id_store(setup: Option<&str>, flush_base: bool, flush_setup: bool) -> Result<Uni> {
    let db = Uni::in_memory()
        .config(uni_db::UniConfig {
            auto_flush_threshold: usize::MAX,
            auto_flush_interval: None,
            ..Default::default()
        })
        .build()
        .await?;
    let s = db.session();
    let tx = s.tx().await?;
    tx.execute("CREATE LABEL E (uid STRING)").await?;
    tx.commit().await?;
    let tx = s.tx().await?;
    tx.execute("CREATE (:E {uid: 'u1', ext_id: 'x1'}), (:E {uid: 'u2', ext_id: 'x2'})")
        .await?;
    tx.commit().await?;
    if flush_base {
        db.flush().await?;
    }
    if let Some(setup) = setup {
        let tx = s.tx().await?;
        tx.execute(setup).await?;
        tx.commit().await?;
    }
    if flush_setup {
        db.flush().await?;
    }
    Ok(db)
}

/// An `ext_id` its holder gave up -- by taking another, by REMOVE, or by
/// being deleted -- can be taken again, whatever was flushed when.
///
/// Measured 2026-10-18: with the holder flushed, every such reuse was refused
/// with "ext_id 'x1' already exists (vertex Vid(0))", and stayed refused
/// after the holder's change was flushed too (except a deletion, whose
/// tombstone row carries the `ext_id`). The main-table lookup picked the
/// newest row *carrying* the `ext_id`, ignoring the holder's newer rows that
/// do not, and the uniqueness check never asked whether a newer L0 version
/// had superseded the flushed one.
#[tokio::test]
async fn freed_ext_id_can_be_taken_again_in_every_storage_state() -> Result<()> {
    let mut failures = Vec::new();
    for setup in [
        "MATCH (a:E {uid: 'u1'}) SET a.ext_id = 'y1'",
        "MATCH (a:E {uid: 'u1'}) REMOVE a.ext_id",
        "MATCH (a:E {uid: 'u1'}) DETACH DELETE a",
    ] {
        for (flush_base, flush_setup) in
            [(false, false), (true, false), (true, true), (false, true)]
        {
            for how in REUSES {
                let db = ext_id_store(Some(setup), flush_base, flush_setup).await?;
                let s = db.session();
                let tx = s.tx().await?;
                match take_x1(&tx, how).await {
                    Ok(()) => {
                        tx.commit().await?;
                    }
                    Err(e) => {
                        tx.rollback();
                        failures.push(format!(
                            "{setup} (flushed {flush_base}/{flush_setup}) then {how}: {e}"
                        ));
                        continue;
                    }
                }
                for flushed in [false, true] {
                    if flushed {
                        db.flush().await?;
                    }
                    let holders: Vec<String> = s
                        .query("MATCH (a:E) WHERE a.ext_id = 'x1' RETURN a.uid AS u")
                        .await?
                        .rows()
                        .iter()
                        .map(|r| format!("{:?}", r.values()))
                        .collect();
                    if holders != vec!["[String(\"new\")]".to_string()] {
                        failures.push(format!(
                            "{setup} (flushed {flush_base}/{flush_setup}) then {how}, \
                             read flushed {flushed}: holders {holders:?}"
                        ));
                    }
                }
            }
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
    Ok(())
}

/// The other side of the same check: an `ext_id` still held, flushed or not,
/// is refused on every path, so the test above cannot pass by admitting
/// everything.
#[tokio::test]
async fn held_ext_id_is_refused_in_every_storage_state() -> Result<()> {
    let mut failures = Vec::new();
    for (setup, flush_base, flush_setup) in [
        (None, false, false),
        (None, true, false),
        // The holder rewritten without touching its `ext_id`, in L0 over a
        // flushed row and then flushed again.
        (Some("MATCH (a:E {uid: 'u1'}) SET a.n = 1"), true, false),
        (Some("MATCH (a:E {uid: 'u1'}) SET a.n = 1"), true, true),
        // The `ext_id` moved away and back.
        (
            Some("MATCH (a:E {uid: 'u1'}) SET a.ext_id = 'y1'"),
            true,
            true,
        ),
    ] {
        for how in REUSES {
            let db = ext_id_store(setup, flush_base, flush_setup).await?;
            if let Some(s) = setup
                && s.contains("'y1'")
            {
                let tx = db.session().tx().await?;
                tx.execute("MATCH (a:E {uid: 'u1'}) SET a.ext_id = 'x1'")
                    .await?;
                tx.commit().await?;
            }
            let tx = db.session().tx().await?;
            let r = take_x1(&tx, how).await;
            tx.rollback();
            if r.is_ok() {
                failures.push(format!(
                    "{setup:?} (flushed {flush_base}/{flush_setup}) then {how}: admitted a duplicate"
                ));
            }
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
    Ok(())
}
