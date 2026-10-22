// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! An edge's stored property set across flush, compaction and reopen, with
//! `partial_lance_writes` off and on, on the primary and on a fork.
//!
//! With the flag on, an edge `SET` flushes as a Lance `MergeInsert` that
//! carries only the touched columns. These tests pin the sequences where a
//! partial row could disagree with the edge's real state: a `REMOVE` and a
//! `SET` in one flush window (A), several delta rows for one edge (B), an
//! edge created and updated in one flush window (C), and a full
//! create/set/remove round trip (D).
//!
//! Every check reads the edge forward, backward, untyped, and via
//! `properties(r)`, so a property or a topology row lost on any path shows.

// Rust guideline compliant

use anyhow::Result;
use serde_json::{Value as Json, json};
use uni_common::UniConfig;
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
            .edge_type("R", &["P"], &["P"])
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

/// The edge as every read path sees it: `(forward props, backward props,
/// untyped count)`, props as `properties(r)` with nulls dropped.
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
    let fwd = props(
        s,
        "MATCH (:P {name: 'a'})-[r:R]->(:P {name: 'b'}) RETURN properties(r) AS props",
    )
    .await?;
    let bwd = props(
        s,
        "MATCH (:P {name: 'b'})<-[r:R]-(:P {name: 'a'}) RETURN properties(r) AS props",
    )
    .await?;
    let n = s
        .query("MATCH (:P {name: 'a'})-[r]->() RETURN count(r) AS n")
        .await?
        .rows()[0]
        .get::<i64>("n")?;
    Ok((fwd, bwd, n))
}

/// Assert the edge reads as `expected` everywhere, at `stage`.
async fn expect_edge(s: &Session, expected: &Json, stage: &str) -> Result<()> {
    let (fwd, bwd, n) = observe(s).await?;
    assert_eq!(fwd, vec![expected.clone()], "forward read, {stage}");
    assert_eq!(bwd, vec![expected.clone()], "backward read, {stage}");
    assert_eq!(n, 1, "untyped edge count from `a`, {stage}");
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
    expect_edge(&s, &expected, &format!("{label}: after flush")).await?;
    if matches!(env.target, Target::Primary) {
        Compactor::new(db.storage()).compact_all().await?;
        expect_edge(&s, &expected, &format!("{label}: after compaction")).await?;
    }
    let (db, s) = env.reopen(db, s).await?;
    expect_edge(&s, &expected, &format!("{label}: after reopen")).await?;
    drop(s);
    db.shutdown().await?;
    Ok(())
}

const CREATE: &str = "CREATE (:P {name: 'a'})-[:R {p: 1, w: 1, x: 'u'}]->(:P {name: 'b'})";

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
                run(&s, "MATCH ()-[r:R]->() REMOVE r.p, r.x SET r.w = 2").await?;
            } else {
                run(&s, "MATCH ()-[r:R]->() REMOVE r.p, r.x").await?;
                run(&s, "MATCH ()-[r:R]->() SET r.w = 2").await?;
            }
            expect_through_lifecycle(&env, db, s, json!({"w": 2}), &label).await?;
        }
    }
    Ok(())
}

/// B: one edge accumulates several delta rows (insert, a flushed `REMOVE`,
/// a flushed `SET`); a later partial `SET` must not bring the removed keys
/// back, whichever row the scan returns last.
#[tokio::test]
async fn b_several_delta_rows_for_one_edge() -> Result<()> {
    for (partial, target) in matrix() {
        let label = format!("B partial={partial} {target:?}");
        let (env, db, s) = Env::new(partial, target).await?;
        run(&s, CREATE).await?;
        s.flush().await?;
        run(&s, "MATCH ()-[r:R]->() REMOVE r.p, r.x").await?;
        s.flush().await?;
        run(&s, "MATCH ()-[r:R]->() SET r.w = 2").await?;
        s.flush().await?;
        run(&s, "MATCH ()-[r:R]->() SET r.w = 3").await?;
        expect_through_lifecycle(&env, db, s, json!({"w": 3}), &label).await?;
    }
    Ok(())
}

/// C: an edge created and updated in the same flush window must be stored —
/// a partial `MergeInsert` with `WhenNotMatched::DoNothing` would drop it.
#[tokio::test]
async fn c_create_then_set_in_one_flush_window() -> Result<()> {
    for (partial, target) in matrix() {
        let label = format!("C partial={partial} {target:?}");
        let (env, db, s) = Env::new(partial, target).await?;
        run(&s, CREATE).await?;
        run(&s, "MATCH ()-[r:R]->() SET r.w = 2").await?;
        expect_through_lifecycle(&env, db, s, json!({"p": 1, "w": 2, "x": "u"}), &label).await?;
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
        run(&s, "MATCH ()-[r:R]->() SET r.w = 5, r.y = 'v'").await?;
        s.flush().await?;
        expect_edge(
            &s,
            &json!({"p": 1, "w": 5, "x": "u", "y": "v"}),
            &format!("{label}: after SET"),
        )
        .await?;
        run(&s, "MATCH ()-[r:R]->() REMOVE r.x").await?;
        expect_through_lifecycle(&env, db, s, json!({"p": 1, "w": 5, "y": "v"}), &label).await?;
    }
    Ok(())
}

/// Shape guard: with the flag on, an edge `SET` (here through the writer's
/// partial entry point, which Cypher's transactional path never reaches)
/// is stored as an appended full row, not a `MergeInsert` of touched
/// columns into the existing row.
#[tokio::test]
async fn edge_set_appends_full_row_with_flag_on() -> Result<()> {
    let (_env, db, s) = Env::new(true, Target::Primary).await?;
    run(&s, CREATE).await?;
    s.flush().await?;
    let h = handles(&db, &s).await?;
    partial_write(
        &db,
        &h,
        props(&[("p", int(1)), ("w", int(2)), ("x", text("u"))]),
        &["w"],
    )
    .await?;
    s.flush().await?;
    let table = table_names::delta_table_name("R", "fwd");
    let rows: usize = db
        .storage()
        .backend()
        .scan(ScanRequest::all(&table))
        .await?
        .iter()
        .map(|b| b.num_rows())
        .sum();
    assert_eq!(rows, 2, "the edge SET appends a second full delta row");
    expect_edge(&s, &json!({"p": 1, "w": 2, "x": "u"}), "after SET").await?;
    Ok(())
}

// ── Writer-level sequences ──────────────────────────────────────────
//
// Cypher writes run in a transaction, and the commit merge re-inserts each
// edge as a full row; the writer's non-transactional entry points
// (`Uni::writer()`, `tx_l0 = None`) reach the flush with the partial
// classification intact. These drive the same sequences through them.

struct Handles {
    src: uni_common::core::id::Vid,
    dst: uni_common::core::id::Vid,
    eid: uni_common::core::id::Eid,
    type_id: u32,
}

async fn handles(db: &Uni, s: &Session) -> Result<Handles> {
    let res = s
        .query(
            "MATCH (a:P {name: 'a'})-[r:R]->(b:P {name: 'b'}) \
             RETURN id(a) AS a, id(b) AS b, id(r) AS r",
        )
        .await?;
    let row = &res.rows()[0];
    let type_id = db
        .storage()
        .schema_manager()
        .schema()
        .edge_types
        .get("R")
        .expect("R declared")
        .id;
    Ok(Handles {
        src: uni_common::core::id::Vid::new(row.get::<i64>("a")? as u64),
        dst: uni_common::core::id::Vid::new(row.get::<i64>("b")? as u64),
        eid: uni_common::core::id::Eid::new(row.get::<i64>("r")? as u64),
        type_id,
    })
}

fn props(pairs: &[(&str, uni_common::Value)]) -> uni_common::Properties {
    pairs
        .iter()
        .map(|(k, v)| ((*k).to_owned(), v.clone()))
        .collect()
}

async fn full_write(db: &Uni, h: &Handles, p: uni_common::Properties) -> Result<()> {
    db.writer()
        .expect("writer")
        .insert_edge(h.src, h.dst, h.type_id, h.eid, p, Some("R".into()), None)
        .await?;
    Ok(())
}

async fn partial_write(
    db: &Uni,
    h: &Handles,
    p: uni_common::Properties,
    touched: &[&str],
) -> Result<()> {
    db.writer()
        .expect("writer")
        .insert_edge_partial_full(
            h.src,
            h.dst,
            h.type_id,
            h.eid,
            p,
            Some("R".into()),
            touched.iter().map(|k| (*k).to_owned()).collect(),
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
        let h = handles(&db, &s).await?;
        let null = uni_common::Value::Null;
        full_write(
            &db,
            &h,
            props(&[("p", null.clone()), ("w", int(1)), ("x", null)]),
        )
        .await?;
        partial_write(&db, &h, props(&[("w", int(2))]), &["w"]).await?;
        expect_through_lifecycle(&env, db, s, json!({"w": 2}), &label).await?;
    }
    Ok(())
}

/// B (writer): several delta rows for one edge (insert, then a flushed full
/// update that drops `p`), then a partial `SET` flushed on top.
#[tokio::test]
async fn b_writer_partial_set_over_several_rows() -> Result<()> {
    for partial in [false, true] {
        let label = format!("B-writer partial={partial}");
        let (env, db, s) = Env::new(partial, Target::Primary).await?;
        run(&s, CREATE).await?;
        s.flush().await?;
        let h = handles(&db, &s).await?;
        let null = uni_common::Value::Null;
        full_write(
            &db,
            &h,
            props(&[("p", null), ("w", int(1)), ("x", text("u"))]),
        )
        .await?;
        s.flush().await?;
        partial_write(&db, &h, props(&[("w", int(2)), ("x", text("u"))]), &["w"]).await?;
        expect_through_lifecycle(&env, db, s, json!({"w": 2, "x": "u"}), &label).await?;
    }
    Ok(())
}

/// C (writer): an edge created and partially updated in one flush window.
#[tokio::test]
async fn c_writer_create_then_partial_set() -> Result<()> {
    for partial in [false, true] {
        let label = format!("C-writer partial={partial}");
        let (env, db, s) = Env::new(partial, Target::Primary).await?;
        // Another `R` edge flushed first, so the delta table exists and a
        // partial flush of the new edge has a table to merge into.
        run(&s, "CREATE (:P {name: 'a'}), (:P {name: 'b'}), (:P {name: 'c'})-[:R {w: 0}]->(:P {name: 'd'})").await?;
        s.flush().await?;
        let res = s
            .query("MATCH (a:P {name: 'a'}), (b:P {name: 'b'}) RETURN id(a) AS a, id(b) AS b")
            .await?;
        let row = &res.rows()[0];
        let h = Handles {
            src: uni_common::core::id::Vid::new(row.get::<i64>("a")? as u64),
            dst: uni_common::core::id::Vid::new(row.get::<i64>("b")? as u64),
            eid: db
                .writer()
                .expect("writer")
                .next_eid(
                    db.storage()
                        .schema_manager()
                        .schema()
                        .edge_types
                        .get("R")
                        .expect("R")
                        .id,
                )
                .await?,
            type_id: db
                .storage()
                .schema_manager()
                .schema()
                .edge_types
                .get("R")
                .expect("R")
                .id,
        };
        full_write(
            &db,
            &h,
            props(&[("p", int(1)), ("w", int(1)), ("x", text("u"))]),
        )
        .await?;
        partial_write(
            &db,
            &h,
            props(&[("p", int(1)), ("w", int(2)), ("x", text("u"))]),
            &["w"],
        )
        .await?;
        expect_through_lifecycle(&env, db, s, json!({"p": 1, "w": 2, "x": "u"}), &label).await?;
    }
    Ok(())
}
