// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! `SET` / `REMOVE` on a relationship cannot move it or change its type.
//!
//! The write path rewrites an edge through `Writer::update_edge`, which takes
//! the edge's `(src, dst, type)` from the row binding. A map that carries
//! `_eid`, `_src`, `_dst` and `_type` is treated as a relationship by `SET`,
//! so a map built in the query names any endpoints and any type for an
//! existing edge id. Before the fix the rewrite stored them: the edge moved to
//! the new endpoints (or type) without the declared-endpoint check a create
//! runs, which also broke the `enforced` conformance record the planner
//! narrows on.
//!
//! Run with:
//!   cargo nextest run -p uni-db --test integration -E 'test(update_edge_identity)'

// Rust guideline compliant

use anyhow::{Context, Result};
use uni_db::{DataType, Uni, UniError, Value};

/// `A -[R]-> F` (declared), `S -[Q]-> F` (declared), one `R` edge `a -> f`,
/// an unconnected `s:S`. Returns the database and `(eid, a, f, s)`.
async fn store(path: &str) -> Result<(Uni, i64, i64, i64, i64)> {
    let db = Uni::open(path).build().await?;
    db.schema()
        .label("A")
        .property("v", DataType::Int)
        .label("F")
        .property("v", DataType::Int)
        .label("S")
        .property("v", DataType::Int)
        .edge_type("R", &["A"], &["F"])
        .property_nullable("w", DataType::Int)
        .edge_type("Q", &["S"], &["F"])
        .property_nullable("w", DataType::Int)
        .apply()
        .await?;
    let tx = db.session().tx().await?;
    tx.execute("CREATE (a:A {v: 1}), (f:F {v: 2}), (s:S {v: 3}), (a)-[:R]->(f)")
        .await?;
    tx.commit().await?;
    let row = db
        .session()
        .query("MATCH (a:A)-[r:R]->(f:F), (s:S) RETURN id(r), id(a), id(f), id(s)")
        .await?;
    let row = row.rows().first().context("one row")?.clone();
    let int = |i: usize| match &row.values()[i] {
        Value::Int(v) => Ok(*v),
        other => Err(anyhow::anyhow!("expected an id, got {other:?}")),
    };
    Ok((db, int(0)?, int(1)?, int(2)?, int(3)?))
}

/// `SET m.w = 7` on a map naming edge `eid` with `(src, dst, type)`.
async fn forged_set(db: &Uni, eid: i64, src: i64, dst: i64, ty: &str) -> Result<(), UniError> {
    let tx = db.session().tx().await?;
    tx.execute_with("WITH {_eid: $e, _src: $s, _dst: $d, _type: $t} AS m SET m.w = 7")
        .param("e", Value::Int(eid))
        .param("s", Value::Int(src))
        .param("d", Value::Int(dst))
        .param("t", Value::String(ty.to_string()))
        .run()
        .await?;
    tx.commit().await?;
    Ok(())
}

/// The stored `(source v, type, target v, w)` of every relationship.
async fn edges(db: &Uni) -> Result<Vec<String>> {
    let result = db
        .session()
        .query("MATCH (x)-[r]->(y) RETURN x.v, type(r), y.v, r.w ORDER BY x.v, y.v")
        .await?;
    Ok(result
        .rows()
        .iter()
        .map(|r| format!("{:?}", r.values()))
        .collect())
}

fn is_identity_refusal(err: &UniError) -> bool {
    matches!(err, UniError::EdgeIdentityChange { .. })
}

/// The refusals and the permitted write of one phase.
async fn check_phase(db: &Uni, phase: &str, eid: i64, a: i64, f: i64, s: i64) -> Result<()> {
    let before = edges(db).await?;
    assert_eq!(before.len(), 1, "{phase}: {before:?}");
    // Move the source to an `S` (which `R`'s declaration also refuses).
    let err = forged_set(db, eid, s, f, "R")
        .await
        .expect_err(&format!("{phase}: moving the source is refused"));
    assert!(is_identity_refusal(&err), "{phase}: {err:?}");
    // Same endpoints, another declared type.
    let err = forged_set(db, eid, a, f, "Q")
        .await
        .expect_err(&format!("{phase}: changing the type is refused"));
    assert!(is_identity_refusal(&err), "{phase}: {err:?}");
    // Reversing the edge is a change of endpoints too.
    let err = forged_set(db, eid, f, a, "R")
        .await
        .expect_err(&format!("{phase}: reversing is refused"));
    assert!(is_identity_refusal(&err), "{phase}: {err:?}");
    assert_eq!(edges(db).await?, before, "{phase}: nothing changed");
    // The edge's own identity still updates.
    forged_set(db, eid, a, f, "R").await?;
    assert_eq!(
        edges(db).await?,
        vec!["[Int(1), String(\"R\"), Int(2), Int(7)]".to_string()],
        "{phase}: a SET through the edge's own identity lands"
    );
    Ok(())
}

/// A forged map cannot move an edge to other endpoints or another type,
/// whether the edge is still buffered, flushed, or read after a reopen.
#[tokio::test]
async fn update_edge_identity_forged_map_is_refused() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().to_str().context("utf-8")?.to_string();
    let (db, eid, a, f, s) = store(&path).await?;
    check_phase(&db, "buffered", eid, a, f, s).await?;
    db.flush().await?;
    check_phase(&db, "flushed", eid, a, f, s).await?;
    db.shutdown().await?;
    let db = Uni::open(path).build().await?;
    check_phase(&db, "reopened", eid, a, f, s).await?;
    db.shutdown().await?;
    Ok(())
}

/// A forged map naming an edge id that does not exist is held to the
/// declaration like a create.
#[tokio::test]
async fn update_edge_identity_unknown_eid_checks_declared_endpoints() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().to_str().context("utf-8")?.to_string();
    let (db, eid, _a, f, s) = store(&path).await?;
    let before = edges(&db).await?;
    let err = forged_set(&db, eid + 1_000_000, s, f, "R")
        .await
        .expect_err("an `S` source for `R` is refused");
    assert!(
        matches!(err, UniError::EdgeEndpointViolation { ref endpoint, .. } if endpoint == "source"),
        "{err:?}"
    );
    assert_eq!(edges(&db).await?, before, "nothing written");
    Ok(())
}
