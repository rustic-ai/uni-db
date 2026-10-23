// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Versioned reads see the graph as of their version after later deletes,
//! relabels and creates are flushed.
//!
//! A flushed vertex delete used to rewrite the vertex's stored rows in place
//! (`_deleted = true` and the delete's `_version`), so every reader bounded by
//! `_version <= v` for a `v` below the delete lost the vertex: a pinned
//! snapshot, `VERSION AS OF`, and a transaction whose L1 view is pinned at its
//! start. A relabel tombstoned the old label's row the same way. A pinned
//! view's VID-to-labels index was the live one, so a traversal's target-label
//! filter used the labels of after the snapshot. And a schemaless edge type was
//! read with no snapshot ceiling: edges deleted after the snapshot were lost to
//! it and edges created after it showed up.
//!
//! Every scenario runs with the labels and the edge type declared (per-label
//! tables, CSR adjacency) and schemaless (main tables only).

use anyhow::{Context, Result};
use uni_db::{DataType, Session, Uni};

#[derive(Clone, Copy, Debug)]
enum Mode {
    Declared,
    Schemaless,
}

const MODES: [Mode; 2] = [Mode::Declared, Mode::Schemaless];

/// Five `:A` vertices `v = 1..=5`, `v = 2` also `:B`, and five `R` edges, two
/// of them on `v = 2`: `1->2, 2->3, 3->4, 4->5, 5->1`.
async fn seed(db: &Uni, mode: Mode) -> Result<()> {
    if let Mode::Declared = mode {
        db.schema()
            .label("A")
            .property("v", DataType::Int)
            .label("B")
            .property("v", DataType::Int)
            .edge_type("R", &["A"], &["A"])
            .property("w", DataType::Int)
            .apply()
            .await?;
    }
    let s = db.session();
    let tx = s.tx().await?;
    tx.execute("UNWIND range(1, 5) AS i CREATE (:A {v: i})")
        .await?;
    tx.execute("MATCH (a:A {v: 2}) SET a:B").await?;
    tx.execute("MATCH (a:A), (b:A) WHERE b.v = a.v % 5 + 1 CREATE (a)-[:R {w: a.v}]->(b)")
        .await?;
    tx.commit().await?;
    Ok(())
}

async fn ints(s: &Session, cypher: &str) -> Result<Vec<i64>> {
    let res = s.query(cypher).await?;
    let mut out = Vec::new();
    for row in res.rows() {
        out.push(row.get::<i64>("x")?);
    }
    out.sort_unstable();
    Ok(out)
}

/// What every reader at the seed's version must answer.
async fn assert_seed_state(s: &Session, phase: &str) -> Result<()> {
    assert_eq!(
        ints(s, "MATCH (n) RETURN n.v AS x").await?,
        [1, 2, 3, 4, 5],
        "{phase}: label-less scan"
    );
    assert_eq!(
        ints(s, "MATCH (n:A) RETURN n.v AS x").await?,
        [1, 2, 3, 4, 5],
        "{phase}: label scan"
    );
    assert_eq!(
        ints(s, "MATCH (n:B) RETURN n.v AS x").await?,
        [2],
        "{phase}: second label"
    );
    assert_eq!(
        ints(s, "MATCH (n:A {v: 2}) RETURN n.v AS x").await?,
        [2],
        "{phase}: property lookup"
    );
    assert_eq!(
        ints(s, "MATCH ()-[r:R]->() RETURN r.w AS x").await?,
        [1, 2, 3, 4, 5],
        "{phase}: edge-driven source"
    );
    assert_eq!(
        ints(
            s,
            "MATCH (a)-[r:R]->(b) WHERE id(a) >= 0 RETURN a.v * 10 + b.v AS x"
        )
        .await?,
        [12, 23, 34, 45, 51],
        "{phase}: traversal from an all-vertex scan"
    );
    assert_eq!(
        ints(s, "MATCH (a:A)-[:R]->(b:B) RETURN a.v AS x").await?,
        [1],
        "{phase}: traversal to a labelled target"
    );
    assert_eq!(
        ints(s, "MATCH (a:A)<-[:R]-(b:A) RETURN a.v * 10 + b.v AS x").await?,
        [15, 21, 32, 43, 54],
        "{phase}: incoming traversal"
    );
    Ok(())
}

/// The later changes, in one transaction and flushed: `v = 2` detach-deleted,
/// `v = 4` gains `:B`, a new vertex `v = 6` and a new edge `1->3` (`w = 9`).
async fn mutate(db: &Uni) -> Result<()> {
    let s = db.session();
    let tx = s.tx().await?;
    tx.execute("MATCH (a:A {v: 2}) DETACH DELETE a").await?;
    tx.execute("MATCH (a:A {v: 4}) SET a:B").await?;
    tx.execute("CREATE (:A {v: 6})").await?;
    tx.execute("MATCH (a:A {v: 1}), (b:A {v: 3}) CREATE (a)-[:R {w: 9}]->(b)")
        .await?;
    tx.commit().await?;
    db.flush().await?;
    Ok(())
}

async fn assert_live_state(s: &Session, phase: &str) -> Result<()> {
    assert_eq!(
        ints(s, "MATCH (n) RETURN n.v AS x").await?,
        [1, 3, 4, 5, 6],
        "{phase}: live vertices"
    );
    assert_eq!(
        ints(s, "MATCH (n:B) RETURN n.v AS x").await?,
        [4],
        "{phase}: live :B"
    );
    assert_eq!(
        ints(s, "MATCH ()-[r:R]->() RETURN r.w AS x").await?,
        [3, 4, 5, 9],
        "{phase}: live edges"
    );
    assert_eq!(
        ints(s, "MATCH (a:A)-[:R]->(b:B) RETURN a.v AS x").await?,
        [3],
        "{phase}: live traversal to a labelled target"
    );
    Ok(())
}

/// A pinned session and `VERSION AS OF` read the snapshot taken before the
/// changes, before and after a reopen.
#[tokio::test]
async fn pinned_snapshot_keeps_a_vertex_deleted_later() -> Result<()> {
    for mode in MODES {
        let dir = tempfile::tempdir()?;
        let path = dir.path().to_str().context("utf-8")?.to_string();
        let snapshot = {
            let db = Uni::open(path.clone()).build().await?;
            seed(&db, mode).await?;
            db.flush().await?;
            let snapshot = db.create_snapshot("before").await?;
            mutate(&db).await?;
            assert_live_state(&db.session(), &format!("{mode:?}")).await?;

            let mut pinned = db.session();
            pinned.pin_to_version(&snapshot).await?;
            assert_seed_state(&pinned, &format!("{mode:?} pinned")).await?;

            let res = db
                .session()
                .query(&format!(
                    "MATCH (n) RETURN n.v AS x ORDER BY x VERSION AS OF '{snapshot}'"
                ))
                .await?;
            let got: Vec<i64> = res
                .rows()
                .iter()
                .map(|r| r.get::<i64>("x"))
                .collect::<Result<_, _>>()?;
            assert_eq!(got, [1, 2, 3, 4, 5], "{mode:?} VERSION AS OF");
            let res = db
                .session()
                .query(&format!(
                    "MATCH (a:A)-[:R]->(b:B) RETURN a.v AS x VERSION AS OF '{snapshot}'"
                ))
                .await?;
            assert_eq!(res.len(), 1, "{mode:?} VERSION AS OF traversal");
            assert_eq!(res.rows()[0].get::<i64>("x")?, 1);
            db.shutdown().await?;
            snapshot
        };
        let db = Uni::open(path).build().await?;
        assert_live_state(&db.session(), &format!("{mode:?} reopened")).await?;
        let mut pinned = db.session();
        pinned.pin_to_version(&snapshot).await?;
        assert_seed_state(&pinned, &format!("{mode:?} pinned after reopen")).await?;
    }
    Ok(())
}

/// The same with the seed unflushed when the snapshot is taken: the snapshot
/// flushes it, so the tombstones land in a later flush.
#[tokio::test]
async fn pinned_snapshot_of_unflushed_seed_keeps_a_vertex_deleted_later() -> Result<()> {
    for mode in MODES {
        let db = Uni::in_memory().build().await?;
        seed(&db, mode).await?;
        let snapshot = db.create_snapshot("before").await?;
        mutate(&db).await?;
        assert_live_state(&db.session(), &format!("{mode:?}")).await?;
        let mut pinned = db.session();
        pinned.pin_to_version(&snapshot).await?;
        assert_seed_state(&pinned, &format!("{mode:?} pinned")).await?;
    }
    Ok(())
}

/// A fork cut before the changes keeps the seed state; the primary moves on.
#[tokio::test]
async fn fork_cut_before_a_delete_keeps_the_vertex() -> Result<()> {
    for mode in MODES {
        let dir = tempfile::tempdir()?;
        let db = Uni::open(dir.path().to_str().context("utf-8")?)
            .build()
            .await?;
        seed(&db, mode).await?;
        db.flush().await?;
        let primary = db.session();
        let forked = primary.fork("before_delete").await?;
        mutate(&db).await?;
        assert_live_state(&db.session(), &format!("{mode:?}")).await?;
        assert_seed_state(&forked, &format!("{mode:?} fork")).await?;
    }
    Ok(())
}

/// A transaction begun before the changes reads the seed's vertices after
/// they commit and are flushed underneath it. Only the vertex tier is checked
/// here; a transaction's edge reads are pinned separately, at read time
/// (`StorageManager::neighbors`).
#[tokio::test]
async fn transaction_begun_before_a_flushed_delete_keeps_the_vertex() -> Result<()> {
    for mode in MODES {
        let db = Uni::in_memory().build().await?;
        seed(&db, mode).await?;
        db.flush().await?;
        let reader = db.session();
        let tx = reader.tx().await?;
        assert_eq!(tx.query("MATCH (n) RETURN count(n) AS c").await?.len(), 1);
        mutate(&db).await?;
        assert_live_state(&db.session(), &format!("{mode:?}")).await?;
        for (q, want) in [
            ("MATCH (n) RETURN n.v AS x ORDER BY x", vec![1, 2, 3, 4, 5]),
            (
                "MATCH (n:A) RETURN n.v AS x ORDER BY x",
                vec![1, 2, 3, 4, 5],
            ),
            ("MATCH (n:B) RETURN n.v AS x ORDER BY x", vec![2]),
        ] {
            let res = tx.query(q).await?;
            let got: Vec<i64> = res
                .rows()
                .iter()
                .map(|r| r.get::<i64>("x"))
                .collect::<Result<_, _>>()?;
            assert_eq!(got, want, "{mode:?} transaction: {q}");
        }
        tx.rollback();
    }
    Ok(())
}

/// A label removed after the snapshot is still on the pinned vertex.
#[tokio::test]
async fn pinned_snapshot_keeps_a_label_removed_later() -> Result<()> {
    for mode in MODES {
        let db = Uni::in_memory().build().await?;
        seed(&db, mode).await?;
        db.flush().await?;
        let snapshot = db.create_snapshot("before").await?;
        db.session()
            .execute_with_retry("MATCH (a:B {v: 2}) REMOVE a:B")
            .await?;
        db.flush().await?;
        assert_eq!(
            ints(&db.session(), "MATCH (n:B) RETURN n.v AS x").await?,
            Vec::<i64>::new(),
            "{mode:?} live"
        );
        let mut pinned = db.session();
        pinned.pin_to_version(&snapshot).await?;
        assert_seed_state(&pinned, &format!("{mode:?} pinned after relabel")).await?;
    }
    Ok(())
}

/// A compaction pass after the changes keeps what the snapshot reads.
#[tokio::test]
async fn pinned_snapshot_survives_compaction() -> Result<()> {
    for mode in MODES {
        let dir = tempfile::tempdir()?;
        let db = Uni::open(dir.path().to_str().context("utf-8")?)
            .build()
            .await?;
        seed(&db, mode).await?;
        db.flush().await?;
        let snapshot = db.create_snapshot("before").await?;
        mutate(&db).await?;
        db.session()
            .run("CALL uni.admin.compact() YIELD tables_optimized RETURN tables_optimized")
            .await?;
        assert_live_state(&db.session(), &format!("{mode:?} compacted")).await?;
        let mut pinned = db.session();
        pinned.pin_to_version(&snapshot).await?;
        assert_seed_state(&pinned, &format!("{mode:?} pinned after compaction")).await?;
    }
    Ok(())
}

/// A session pinned to a flush manifest (no restore point: it reads the live
/// tables under the manifest's version ceiling) keeps the seed state too.
#[tokio::test]
async fn pinned_flush_manifest_keeps_a_vertex_deleted_later() -> Result<()> {
    for mode in MODES {
        let db = Uni::in_memory().build().await?;
        seed(&db, mode).await?;
        db.flush().await?;
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        let at = chrono::Utc::now();
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        mutate(&db).await?;
        assert_live_state(&db.session(), &format!("{mode:?}")).await?;
        let mut pinned = db.session();
        pinned.pin_to_timestamp(at).await?;
        assert_seed_state(&pinned, &format!("{mode:?} pinned to a flush manifest")).await?;
    }
    Ok(())
}

/// A transaction begun before the changes still reads the seed's vertices
/// after a compaction pass folds the superseded rows away.
#[tokio::test]
async fn transaction_begun_before_a_compaction_keeps_the_vertex() -> Result<()> {
    for mode in MODES {
        let dir = tempfile::tempdir()?;
        let db = Uni::open(dir.path().to_str().context("utf-8")?)
            .build()
            .await?;
        seed(&db, mode).await?;
        db.flush().await?;
        let reader = db.session();
        let tx = reader.tx().await?;
        mutate(&db).await?;
        db.session()
            .run("CALL uni.admin.compact() YIELD tables_optimized RETURN tables_optimized")
            .await?;
        let res = tx.query("MATCH (n:A) RETURN n.v AS x ORDER BY x").await?;
        let got: Vec<i64> = res
            .rows()
            .iter()
            .map(|r| r.get::<i64>("x"))
            .collect::<Result<_, _>>()?;
        assert_eq!(
            got,
            [1, 2, 3, 4, 5],
            "{mode:?} transaction after compaction"
        );
        tx.rollback();
    }
    Ok(())
}

/// A session pinned to a flush manifest keeps the seed state across a
/// compaction pass: its pin holds the merge back. A session pinned to the
/// same manifest after the pass is refused, since the merged history is gone
/// from the live tables, and so is one in a later process.
#[tokio::test]
async fn pinned_flush_manifest_survives_compaction() -> Result<()> {
    for mode in MODES {
        let dir = tempfile::tempdir()?;
        let db = Uni::open(dir.path().to_str().context("utf-8")?)
            .build()
            .await?;
        seed(&db, mode).await?;
        db.flush().await?;
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        let at = chrono::Utc::now();
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        let mut pinned = db.session();
        pinned.pin_to_timestamp(at).await?;
        mutate(&db).await?;
        db.session()
            .run("CALL uni.admin.compact() YIELD tables_optimized RETURN tables_optimized")
            .await?;
        assert_live_state(&db.session(), &format!("{mode:?} compacted")).await?;
        assert_seed_state(&pinned, &format!("{mode:?} pinned before compaction")).await?;
        drop(pinned);
        db.session()
            .run("CALL uni.admin.compact() YIELD tables_optimized RETURN tables_optimized")
            .await?;
        // Declared labels and types are merged (per-label tables, edge
        // deltas folded into adjacency); the schemaless main tables are not,
        // so their history stays readable.
        let mut later = db.session();
        let pinned_later = later.pin_to_timestamp(at).await;
        db.shutdown().await?;
        let db = Uni::open(dir.path().to_str().context("utf-8")?)
            .build()
            .await?;
        let mut reopened = db.session();
        let pinned_reopened = reopened.pin_to_timestamp(at).await;
        for (phase, outcome, session) in [
            ("after the merge", pinned_later, &later),
            ("in a later process", pinned_reopened, &reopened),
        ] {
            match mode {
                Mode::Declared => {
                    let err = outcome.expect_err("a pin below the compaction horizon is refused");
                    assert!(
                        matches!(err, uni_db::UniError::InvalidArgument { .. }),
                        "{mode:?} {phase}: {err}"
                    );
                }
                Mode::Schemaless => {
                    outcome?;
                    if phase == "in a later process" {
                        assert_seed_state(session, &format!("{mode:?} {phase}")).await?;
                    }
                }
            }
        }
    }
    Ok(())
}

/// A snapshot taken after a compaction pass (its adjacency is partly in
/// compacted L2) reads its state after a later pass too.
#[tokio::test]
async fn snapshot_of_compacted_store_survives_compaction() -> Result<()> {
    for mode in MODES {
        let dir = tempfile::tempdir()?;
        let db = Uni::open(dir.path().to_str().context("utf-8")?)
            .build()
            .await?;
        seed(&db, mode).await?;
        db.flush().await?;
        let compact = "CALL uni.admin.compact() YIELD tables_optimized RETURN tables_optimized";
        db.session().run(compact).await?;
        let snapshot = db.create_snapshot("compacted").await?;
        mutate(&db).await?;
        db.session().run(compact).await?;
        assert_live_state(&db.session(), &format!("{mode:?}")).await?;
        let mut pinned = db.session();
        pinned.pin_to_version(&snapshot).await?;
        assert_seed_state(&pinned, &format!("{mode:?} snapshot of a compacted store")).await?;
    }
    Ok(())
}

/// The VID-to-labels index rebuilt at open holds each vertex's newest row:
/// none for a deleted vertex, the new set for a relabelled one. It read every
/// row with `_deleted = false` and kept whichever came last, which with
/// appended tombstones brought deleted vertices back.
#[tokio::test]
async fn reopened_label_index_holds_each_vertex_newest_labels() -> Result<()> {
    for mode in MODES {
        let dir = tempfile::tempdir()?;
        let path = dir.path().to_str().context("utf-8")?.to_string();
        let (deleted, relabelled) = {
            let db = Uni::open(path.clone()).build().await?;
            seed(&db, mode).await?;
            db.flush().await?;
            let id = |v: i64| {
                let db = &db;
                async move {
                    let res = db
                        .session()
                        .query(&format!("MATCH (n:A {{v: {v}}}) RETURN id(n) AS id"))
                        .await?;
                    anyhow::Ok(uni_db::Vid::from(res.rows()[0].get::<i64>("id")? as u64))
                }
            };
            let ids = (id(2).await?, id(4).await?);
            mutate(&db).await?;
            db.shutdown().await?;
            ids
        };
        let db = Uni::open(path).build().await?;
        assert_eq!(
            db.storage().get_labels_from_index(deleted),
            None,
            "{mode:?}: deleted vertex"
        );
        let mut labels = db
            .storage()
            .get_labels_from_index(relabelled)
            .unwrap_or_default();
        labels.sort();
        assert_eq!(labels, ["A", "B"], "{mode:?}: relabelled vertex");
    }
    Ok(())
}
