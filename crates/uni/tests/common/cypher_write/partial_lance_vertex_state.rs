// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! A vertex's stored property set across flush, compaction and reopen, with
//! `partial_lance_writes` off and on, on the primary and on a fork.
//!
//! The vertex twin of `partial_lance_edge_state`. Until wave P1, a vertex
//! `SET` with the flag on could flush as a Lance `MergeInsert` keyed on
//! `_vid` that carried only the touched columns. A vertex table holds one
//! row per flushed version of a vertex until compaction, so a key-matched
//! merge into it is undefined in the same way it was for the edge delta
//! log. These tests pin the sequences where a partial row could disagree
//! with the vertex's real state: a `REMOVE` and a `SET` in one flush window
//! (A), several stored rows for one vertex (B), a vertex created and updated
//! in one flush window (C), and a full create/set/remove round trip (D).
//!
//! The Cypher arms run through transactions. The writer arms drive the
//! writer's non-transactional entry points (`Uni::writer()`, `tx_l0 =
//! None`), which were the only route that reached the flush with a partial
//! classification: the commit merge never carried it.

// Rust guideline compliant

use anyhow::Result;
use serde_json::{Value as Json, json};
use uni_common::UniConfig;
use uni_common::core::id::Vid;
use uni_db::{DataType, Session, Uni};
use uni_store::backend::table_names;
use uni_store::backend::types::ScanRequest;
use uni_store::storage::compaction::Compactor;

#[derive(Clone, Copy, Debug)]
enum Target {
    Primary,
    Fork,
}

fn config(partial: bool) -> UniConfig {
    UniConfig {
        #[expect(deprecated, reason = "exercises the deprecated no-op flag")]
        partial_lance_writes: partial,
        async_flush_enabled: false,
        ..UniConfig::default()
    }
}

/// One test database: a directory, the flag, and whether statements run on
/// the primary or on fork `scn`.
struct Env {
    _dir: tempfile::TempDir,
    uri: String,
    partial: bool,
    target: Target,
}

impl Env {
    async fn new(partial: bool, target: Target) -> Result<(Self, Uni, Session)> {
        let dir = tempfile::TempDir::new()?;
        let uri = dir.path().to_str().expect("utf-8 path").to_owned();
        let db = Uni::open(&uri).config(config(partial)).build().await?;
        db.schema()
            .label("P")
            .property("name", DataType::String)
            .property_nullable("p", DataType::Int64)
            .property_nullable("w", DataType::Int64)
            .apply()
            .await?;
        let env = Self {
            _dir: dir,
            uri,
            partial,
            target,
        };
        let s = env.session(&db).await?;
        Ok((env, db, s))
    }

    async fn session(&self, db: &Uni) -> Result<Session> {
        let s = db.session();
        Ok(match self.target {
            Target::Primary => s,
            Target::Fork => s.fork("scn").await?,
        })
    }

    async fn reopen(&self, db: Uni, s: Session) -> Result<(Uni, Session)> {
        drop(s);
        db.shutdown().await?;
        let db = Uni::open(&self.uri)
            .config(config(self.partial))
            .build()
            .await?;
        let s = self.session(&db).await?;
        Ok((db, s))
    }
}

async fn run(s: &Session, stmt: &str) -> Result<()> {
    let tx = s.tx().await?;
    tx.execute(stmt).await?;
    tx.commit().await?;
    Ok(())
}

/// The vertex as every read path sees it: `(labelled props, unlabelled
/// props, labelled count)`, props as `properties(n)` with nulls dropped.
async fn observe(s: &Session) -> Result<(Vec<Json>, Vec<Json>, i64)> {
    async fn props(s: &Session, q: &str) -> Result<Vec<Json>> {
        let res = s.query(q).await?;
        let mut out = Vec::new();
        for row in res.rows() {
            let mut v: Json = row.value("props").expect("props column").clone().into();
            if let Some(m) = v.as_object_mut() {
                m.retain(|_, x| !x.is_null());
            }
            out.push(v);
        }
        Ok(out)
    }
    let labelled = props(s, "MATCH (n:P {name: 'a'}) RETURN properties(n) AS props").await?;
    let unlabelled = props(
        s,
        "MATCH (n) WHERE n.name = 'a' RETURN properties(n) AS props",
    )
    .await?;
    let n = s
        .query("MATCH (n:P) WHERE n.name = 'a' RETURN count(n) AS n")
        .await?
        .rows()[0]
        .get::<i64>("n")?;
    Ok((labelled, unlabelled, n))
}

/// Assert the vertex reads as `expected` everywhere, at `stage`.
async fn expect_vertex(s: &Session, expected: &Json, stage: &str) -> Result<()> {
    let (labelled, unlabelled, n) = observe(s).await?;
    assert_eq!(labelled, vec![expected.clone()], "labelled read, {stage}");
    assert_eq!(
        unlabelled,
        vec![expected.clone()],
        "unlabelled read, {stage}"
    );
    assert_eq!(n, 1, "labelled count, {stage}");
    Ok(())
}

/// Check after flush, after semantic compaction (primary only: forks have
/// no compaction), and after reopen.
async fn expect_through_lifecycle(
    env: &Env,
    db: Uni,
    s: Session,
    expected: Json,
    label: &str,
) -> Result<()> {
    s.flush().await?;
    expect_vertex(&s, &expected, &format!("{label}: after flush")).await?;
    if matches!(env.target, Target::Primary) {
        Compactor::new(db.storage()).compact_all().await?;
        expect_vertex(&s, &expected, &format!("{label}: after compaction")).await?;
    }
    let (db, s) = env.reopen(db, s).await?;
    expect_vertex(&s, &expected, &format!("{label}: after reopen")).await?;
    drop(s);
    db.shutdown().await?;
    Ok(())
}

const CREATE: &str = "CREATE (:P {name: 'a', p: 1, w: 1, x: 'u'})";

fn matrix() -> Vec<(bool, Target)> {
    vec![
        (false, Target::Primary),
        (true, Target::Primary),
        (false, Target::Fork),
        (true, Target::Fork),
    ]
}

/// A: `REMOVE` then `SET` of another key in one flush window — separate
/// transactions and one statement.
#[tokio::test]
async fn a_remove_then_set_in_one_flush_window() -> Result<()> {
    for (partial, target) in matrix() {
        for one_statement in [false, true] {
            let label = format!("A partial={partial} {target:?} one_statement={one_statement}");
            let (env, db, s) = Env::new(partial, target).await?;
            run(&s, CREATE).await?;
            s.flush().await?;
            if one_statement {
                run(&s, "MATCH (n:P) REMOVE n.p, n.x SET n.w = 2").await?;
            } else {
                run(&s, "MATCH (n:P) REMOVE n.p, n.x").await?;
                run(&s, "MATCH (n:P) SET n.w = 2").await?;
            }
            expect_through_lifecycle(&env, db, s, json!({"name": "a", "w": 2}), &label).await?;
        }
    }
    Ok(())
}

/// B: one vertex accumulates several stored rows (insert, a flushed
/// `REMOVE`, a flushed `SET`); a later `SET` must not bring the removed keys
/// back, whichever row the scan returns last.
#[tokio::test]
async fn b_several_rows_for_one_vertex() -> Result<()> {
    for (partial, target) in matrix() {
        let label = format!("B partial={partial} {target:?}");
        let (env, db, s) = Env::new(partial, target).await?;
        run(&s, CREATE).await?;
        s.flush().await?;
        run(&s, "MATCH (n:P) REMOVE n.p, n.x").await?;
        s.flush().await?;
        run(&s, "MATCH (n:P) SET n.w = 2").await?;
        s.flush().await?;
        run(&s, "MATCH (n:P) SET n.w = 3").await?;
        expect_through_lifecycle(&env, db, s, json!({"name": "a", "w": 3}), &label).await?;
    }
    Ok(())
}

/// C: a vertex created and updated in the same flush window must be stored.
#[tokio::test]
async fn c_create_then_set_in_one_flush_window() -> Result<()> {
    for (partial, target) in matrix() {
        let label = format!("C partial={partial} {target:?}");
        let (env, db, s) = Env::new(partial, target).await?;
        run(&s, CREATE).await?;
        run(&s, "MATCH (n:P) SET n.w = 2").await?;
        expect_through_lifecycle(
            &env,
            db,
            s,
            json!({"name": "a", "p": 1, "w": 2, "x": "u"}),
            &label,
        )
        .await?;
    }
    Ok(())
}

/// D: create (declared + undeclared), flush, `SET` another key, flush,
/// `REMOVE` one, flush, then compaction and reopen — every key read back.
#[tokio::test]
async fn d_round_trip() -> Result<()> {
    for (partial, target) in matrix() {
        let label = format!("D partial={partial} {target:?}");
        let (env, db, s) = Env::new(partial, target).await?;
        run(&s, CREATE).await?;
        s.flush().await?;
        run(&s, "MATCH (n:P) SET n.w = 5, n.y = 'v'").await?;
        s.flush().await?;
        expect_vertex(
            &s,
            &json!({"name": "a", "p": 1, "w": 5, "x": "u", "y": "v"}),
            &format!("{label}: after SET"),
        )
        .await?;
        run(&s, "MATCH (n:P) REMOVE n.x").await?;
        expect_through_lifecycle(
            &env,
            db,
            s,
            json!({"name": "a", "p": 1, "w": 5, "y": "v"}),
            &label,
        )
        .await?;
    }
    Ok(())
}

/// Shape guard: with the flag on, a vertex `SET` through the writer's
/// partial entry point is stored as an appended full row, not a
/// `MergeInsert` of touched columns into the existing row.
#[tokio::test]
async fn vertex_set_appends_full_row_with_flag_on() -> Result<()> {
    let (_env, db, s) = Env::new(true, Target::Primary).await?;
    run(&s, CREATE).await?;
    s.flush().await?;
    let vid = vid_of(&s, "a").await?;
    partial_write(
        &db,
        vid,
        props(&[
            ("name", text("a")),
            ("p", int(1)),
            ("w", int(2)),
            ("x", text("u")),
        ]),
        &["w"],
    )
    .await?;
    s.flush().await?;
    let table = table_names::vertex_table_name("P");
    let rows: usize = db
        .storage()
        .backend()
        .scan(ScanRequest::all(&table))
        .await?
        .iter()
        .map(|b| b.num_rows())
        .sum();
    assert_eq!(rows, 2, "the vertex SET appends a second full row");
    expect_vertex(
        &s,
        &json!({"name": "a", "p": 1, "w": 2, "x": "u"}),
        "after SET",
    )
    .await?;
    Ok(())
}

// ── Writer-level sequences ──────────────────────────────────────────

async fn vid_of(s: &Session, name: &str) -> Result<Vid> {
    let res = s
        .query_with("MATCH (n:P {name: $name}) RETURN id(n) AS v")
        .param("name", name)
        .fetch_all()
        .await?;
    Ok(Vid::new(res.rows()[0].get::<i64>("v")? as u64))
}

fn props(pairs: &[(&str, uni_common::Value)]) -> uni_common::Properties {
    pairs
        .iter()
        .map(|(k, v)| ((*k).to_owned(), v.clone()))
        .collect()
}

async fn full_write(db: &Uni, vid: Vid, p: uni_common::Properties) -> Result<()> {
    db.writer()
        .expect("writer")
        .insert_vertex_with_labels(vid, p, &["P".to_owned()], None)
        .await?;
    Ok(())
}

async fn partial_write(
    db: &Uni,
    vid: Vid,
    p: uni_common::Properties,
    touched: &[&str],
) -> Result<()> {
    db.writer()
        .expect("writer")
        .insert_vertex_partial_full(
            vid,
            p,
            touched.iter().map(|k| (*k).to_owned()).collect(),
            &["P".to_owned()],
            None,
        )
        .await?;
    Ok(())
}

fn int(v: i64) -> uni_common::Value {
    uni_common::Value::Int(v)
}

fn text(v: &str) -> uni_common::Value {
    uni_common::Value::String(v.into())
}

/// A (writer): a full-row `REMOVE` of `p` and `x`, then a partial `SET` of
/// `w`, in one flush window. The flush must not leave `p`/`x` behind.
#[tokio::test]
async fn a_writer_remove_then_partial_set() -> Result<()> {
    for partial in [false, true] {
        let label = format!("A-writer partial={partial}");
        let (env, db, s) = Env::new(partial, Target::Primary).await?;
        run(&s, CREATE).await?;
        s.flush().await?;
        let vid = vid_of(&s, "a").await?;
        let null = uni_common::Value::Null;
        full_write(
            &db,
            vid,
            props(&[
                ("name", text("a")),
                ("p", null.clone()),
                ("w", int(1)),
                ("x", null),
            ]),
        )
        .await?;
        partial_write(
            &db,
            vid,
            props(&[("name", text("a")), ("w", int(2))]),
            &["w"],
        )
        .await?;
        expect_through_lifecycle(&env, db, s, json!({"name": "a", "w": 2}), &label).await?;
    }
    Ok(())
}

/// B (writer): several stored rows for one vertex (insert, then a flushed
/// full update that drops `p`), then a partial `SET` flushed on top.
#[tokio::test]
async fn b_writer_partial_set_over_several_rows() -> Result<()> {
    for partial in [false, true] {
        let label = format!("B-writer partial={partial}");
        let (env, db, s) = Env::new(partial, Target::Primary).await?;
        run(&s, CREATE).await?;
        s.flush().await?;
        let vid = vid_of(&s, "a").await?;
        let null = uni_common::Value::Null;
        full_write(
            &db,
            vid,
            props(&[
                ("name", text("a")),
                ("p", null),
                ("w", int(1)),
                ("x", text("u")),
            ]),
        )
        .await?;
        s.flush().await?;
        partial_write(
            &db,
            vid,
            props(&[("name", text("a")), ("w", int(2)), ("x", text("u"))]),
            &["w"],
        )
        .await?;
        expect_through_lifecycle(&env, db, s, json!({"name": "a", "w": 2, "x": "u"}), &label)
            .await?;
    }
    Ok(())
}

/// C (writer): a vertex created and partially updated in one flush window.
#[tokio::test]
async fn c_writer_create_then_partial_set() -> Result<()> {
    for partial in [false, true] {
        let label = format!("C-writer partial={partial}");
        let (env, db, s) = Env::new(partial, Target::Primary).await?;
        // Another `P` vertex flushed first, so the vertex table exists and a
        // partial flush of the new vertex has a table to merge into.
        run(&s, "CREATE (:P {name: 'other'})").await?;
        s.flush().await?;
        let vid = db.writer().expect("writer").next_vid().await?;
        full_write(
            &db,
            vid,
            props(&[
                ("name", text("a")),
                ("p", int(1)),
                ("w", int(1)),
                ("x", text("u")),
            ]),
        )
        .await?;
        partial_write(
            &db,
            vid,
            props(&[
                ("name", text("a")),
                ("p", int(1)),
                ("w", int(2)),
                ("x", text("u")),
            ]),
            &["w"],
        )
        .await?;
        expect_through_lifecycle(
            &env,
            db,
            s,
            json!({"name": "a", "p": 1, "w": 2, "x": "u"}),
            &label,
        )
        .await?;
    }
    Ok(())
}
