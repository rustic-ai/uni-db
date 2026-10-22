// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! The planner narrows an unlabelled endpoint to a typed edge's declared
//! labels exactly when the catalog records that no stored edge violates them.
//!
//! Without narrowing, `MATCH ()-[w:WORK_AT]->()` scans every vertex and
//! expands from each (LDBC SF1: 3.18M vertices for 21654 edges). Narrowing is
//! sound only when stored edges conform, so each edge type carries an
//! `endpoint_conformance` record: `enforced` for a type created while writes
//! enforce the declaration, `verified` after `uni.admin.verifyEdgeEndpoints()`
//! read every stored edge and found none violating, `unverified` otherwise --
//! including every type in a catalog written before enforcement.
//!
//! Run with:
//!   cargo nextest run -p uni-db --test integration -E 'test(declared_endpoint_narrowing)'

// Rust guideline compliant

use std::collections::HashMap;

use anyhow::{Context, Result};
use uni_common::core::schema::EndpointConformance;
use uni_db::{DataType, EdgeData, Session, Uni, UniConfig, Value};

use super::declared_endpoint_violation_reads::{
    assert_all_edges_visible, ints, narrow_declaration, violating_store,
};

/// The conformance record of `edge_type` in `db`'s catalog.
fn conformance(db: &Uni, edge_type: &str) -> Result<EndpointConformance> {
    Ok(db
        .schema()
        .current()
        .edge_types
        .get(edge_type)
        .context("edge type declared")?
        .endpoint_conformance
        .clone())
}

/// The logical plan text of `cypher` on `session`.
async fn plan(session: &Session, cypher: &str) -> Result<String> {
    Ok(session.query_with(cypher).explain().await?.plan_text)
}

/// `true` when the anonymous source of `cypher` is scanned by label, not as
/// every vertex.
async fn narrowed(session: &Session, cypher: &str) -> Result<bool> {
    Ok(!plan(session, cypher).await?.contains("ScanAll"))
}

/// The probe every assertion on plan shape uses.
const ANON: &str = "MATCH ()-[r:R]->() RETURN count(r)";

/// `A -[R]-> F` holding three conforming edges, plus an unconnected `S`.
async fn clean_store(path: &str) -> Result<Uni> {
    let db = Uni::open(path).build().await?;
    db.schema()
        .label("A")
        .property("v", DataType::Int)
        .label("F")
        .property("v", DataType::Int)
        .label("S")
        .property("v", DataType::Int)
        .edge_type("R", &["A"], &["F"])
        .apply()
        .await?;
    let tx = db.session().tx().await?;
    tx.execute(
        "CREATE (a:A {v: 1}), (b:A {v: 2}), (f:F {v: 3}), (g:F {v: 4}), (:S {v: 5}), \
                (a)-[:R]->(f), (a)-[:R]->(g), (b)-[:R]->(g)",
    )
    .await?;
    tx.commit().await?;
    Ok(db)
}

/// Answers that must hold whatever plan is chosen.
async fn assert_clean_answers(db: &Uni, phase: &str) -> Result<()> {
    let cases: &[(&str, &[i64])] = &[
        (ANON, &[3]),
        ("MATCH ()<-[r:R]-() RETURN count(r)", &[3]),
        ("MATCH ()-[r:R]-() RETURN count(r)", &[6]),
        ("MATCH (a:A)-[:R]->(b) RETURN b.v", &[3, 4, 4]),
        ("MATCH (b:F)<-[:R]-(x) RETURN x.v", &[1, 1, 2]),
        ("MATCH (x)-[:R]->(b) RETURN x.v", &[1, 1, 2]),
    ];
    for (cypher, want) in cases {
        assert_eq!(ints(db, cypher).await?, want.to_vec(), "{phase}: {cypher}");
    }
    Ok(())
}

/// A type created under enforcement is `enforced` and narrows, before and
/// after a flush and a reopen.
#[tokio::test]
async fn declared_endpoint_narrowing_enforced_type_narrows() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().to_str().context("utf-8")?.to_string();
    let db = clean_store(&path).await?;
    assert_eq!(conformance(&db, "R")?, EndpointConformance::Enforced);
    assert!(
        narrowed(&db.session(), ANON).await?,
        "enforced type narrows"
    );
    assert!(
        narrowed(&db.session(), "MATCH ()<-[r:R]-() RETURN count(r)").await?,
        "incoming narrows to the declared target"
    );
    assert_clean_answers(&db, "L0").await?;
    db.flush().await?;
    assert_clean_answers(&db, "flushed").await?;
    db.shutdown().await?;
    let db = Uni::open(path).build().await?;
    assert_eq!(
        conformance(&db, "R")?,
        EndpointConformance::Enforced,
        "the record is persisted"
    );
    assert!(narrowed(&db.session(), ANON).await?);
    assert_clean_answers(&db, "reopened").await
}

/// A legacy catalog does not narrow; verification of a clean store records
/// `verified`, after which it narrows, still answering correctly.
#[tokio::test]
async fn declared_endpoint_narrowing_verification_of_clean_legacy_store() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().to_str().context("utf-8")?.to_string();
    clean_store(&path).await?.shutdown().await?;
    narrow_declaration(dir.path(), "R", &["A"], &["F"])?;
    let db = Uni::open(path.clone()).build().await?;
    assert_eq!(conformance(&db, "R")?, EndpointConformance::Unverified);
    assert!(!narrowed(&db.session(), ANON).await?, "legacy fails open");
    assert_clean_answers(&db, "legacy").await?;

    let rows = db
        .session()
        .run(
            "CALL uni.admin.verifyEdgeEndpoints('R') \
             YIELD edge_type, edges_checked, source_violations, target_violations, conformance \
             RETURN edge_type, edges_checked, source_violations, target_violations, conformance",
        )
        .await?;
    let row = rows.rows()[0].values().to_vec();
    assert_eq!(
        row,
        vec![
            Value::String("R".into()),
            Value::Int(3),
            Value::Int(0),
            Value::Int(0),
            Value::String("verified".into()),
        ]
    );
    assert!(matches!(
        conformance(&db, "R")?,
        EndpointConformance::Verified { .. }
    ));
    assert!(
        narrowed(&db.session(), ANON).await?,
        "verified type narrows"
    );
    assert_clean_answers(&db, "verified").await?;
    db.shutdown().await?;
    let db = Uni::open(path).build().await?;
    assert!(narrowed(&db.session(), ANON).await?, "persisted");
    assert_clean_answers(&db, "verified, reopened").await
}

/// Verification of a store holding violating edges reports them, leaves the
/// type unverified, and every edge stays readable.
#[tokio::test]
async fn declared_endpoint_narrowing_verification_reports_violations() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let db = violating_store(dir.path()).await?;
    assert_eq!(conformance(&db, "R2")?, EndpointConformance::Unverified);
    let rows = db
        .session()
        .run(
            "CALL uni.admin.verifyEdgeEndpoints() \
             YIELD edge_type, edges_checked, source_violations, target_violations, conformance \
             RETURN edge_type, edges_checked, source_violations, target_violations, conformance",
        )
        .await?;
    let r2: Vec<Value> = rows
        .rows()
        .iter()
        .map(|r| r.values().to_vec())
        .find(|v| v[0] == Value::String("R2".into()))
        .context("R2 reported")?;
    assert_eq!(
        r2,
        vec![
            Value::String("R2".into()),
            Value::Int(3),
            Value::Int(1),
            Value::Int(1),
            Value::String("unverified".into()),
        ]
    );
    assert_eq!(conformance(&db, "R2")?, EndpointConformance::Unverified);
    assert!(
        !narrowed(&db.session(), "MATCH ()-[r:R2]->() RETURN count(r)").await?,
        "violations keep the plan un-narrowed"
    );
    assert_all_edges_visible(&db, "after a failed verification").await
}

/// A declaration cannot change in place, so its record cannot go stale: a
/// re-declaration with other endpoints is refused and the record kept.
#[tokio::test]
async fn declared_endpoint_narrowing_redeclaration_keeps_record() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let db = clean_store(dir.path().to_str().context("utf-8")?).await?;
    let err = db
        .schema()
        .edge_type("R", &["A", "S"], &["F"])
        .apply()
        .await
        .expect_err("a changed declaration is refused");
    assert!(
        err.to_string().contains("different endpoint labels"),
        "{err}"
    );
    assert_eq!(conformance(&db, "R")?, EndpointConformance::Enforced);
    // An identical re-declaration is idempotent and keeps it too.
    db.schema().edge_type("R", &["A"], &["F"]).apply().await?;
    assert_eq!(conformance(&db, "R")?, EndpointConformance::Enforced);
    Ok(())
}

/// Declaring a type whose name already labels schemaless edges -- written
/// without any declaration to check -- does not claim conformance.
#[tokio::test]
async fn declared_endpoint_narrowing_declaring_over_schemaless_edges() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().to_str().context("utf-8")?.to_string();
    let db = Uni::open(path.clone()).build().await?;
    db.schema()
        .label("A")
        .property("v", DataType::Int)
        .label("F")
        .property("v", DataType::Int)
        .label("S")
        .property("v", DataType::Int)
        .apply()
        .await?;
    let tx = db.session().tx().await?;
    tx.execute("CREATE (:A {v: 1})-[:Q]->(:F {v: 2}), (:S {v: 3})-[:Q]->(:F {v: 4})")
        .await?;
    tx.commit().await?;
    db.flush().await?;
    db.shutdown().await?;
    let db = Uni::open(path).build().await?;
    db.schema().edge_type("Q", &["A"], &["F"]).apply().await?;
    assert_eq!(conformance(&db, "Q")?, EndpointConformance::Unverified);
    assert!(!narrowed(&db.session(), "MATCH ()-[r:Q]->() RETURN count(r)").await?);
    assert_eq!(
        ints(&db, "MATCH (x)-[:Q]->() RETURN x.v").await?,
        vec![1, 3],
        "the S-sourced schemaless edge stays visible"
    );
    Ok(())
}

/// A verification that starts after a fork was created never read the fork's
/// base, so the fork keeps an un-narrowed plan; a fork created after it
/// narrows. Verification itself is refused on a fork.
#[tokio::test]
async fn declared_endpoint_narrowing_fork_trusts_only_earlier_verification() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().to_str().context("utf-8")?.to_string();
    clean_store(&path).await?.shutdown().await?;
    narrow_declaration(dir.path(), "R", &["A"], &["F"])?;
    let config = UniConfig {
        disable_fork_sweeper: true,
        ..Default::default()
    };
    let db = Uni::open(path).config(config).build().await?;
    let early = db.session().fork("early").await?;
    db.session()
        .run("CALL uni.admin.verifyEdgeEndpoints() YIELD conformance RETURN conformance")
        .await?;
    assert!(narrowed(&db.session(), ANON).await?, "primary narrows");
    assert!(
        !narrowed(&early, ANON).await?,
        "a fork older than the verification does not"
    );
    // Not only the session that was open during the verification: a session
    // opened on the old fork afterwards reads primary's current catalog, and
    // must still distrust the verification.
    drop(early);
    let early = db.session().fork("early").await?;
    assert!(
        !narrowed(&early, ANON).await?,
        "a fork older than the verification does not, reopened"
    );
    let late = db.session().fork("late").await?;
    assert!(
        narrowed(&late, ANON).await?,
        "a fork created after the verification does"
    );
    for (session, name) in [(&early, "early"), (&late, "late")] {
        let r = session.query(ANON).await?;
        assert_eq!(r.rows()[0].values()[0], Value::Int(3), "{name}");
    }
    let refused = late
        .run("CALL uni.admin.verifyEdgeEndpoints() YIELD conformance RETURN conformance")
        .await;
    assert!(
        refused
            .as_ref()
            .is_err_and(|e| e.to_string().contains("not on a fork")),
        "{refused:?}"
    );
    // An enforced type narrows on every fork, whenever it was created.
    let db2_dir = tempfile::tempdir()?;
    let db2 = clean_store(db2_dir.path().to_str().context("utf-8")?).await?;
    let fork = db2.session().fork("any").await?;
    assert!(narrowed(&fork, ANON).await?, "enforced narrows on a fork");
    Ok(())
}

/// The bulk loader writes straight to storage, so it enforces declared
/// endpoints itself -- against the load's own vertices and stored ones.
#[tokio::test]
async fn declared_endpoint_narrowing_bulk_loader_enforces() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let db = clean_store(dir.path().to_str().context("utf-8")?).await?;
    db.flush().await?;
    let session = db.session();
    let tx = session.tx().await?;
    let mut bulk = tx.bulk_writer().build()?;
    let row = |v: i64| HashMap::from([("v".to_string(), Value::Int(v))]);
    let a = bulk.insert_vertices("A", vec![row(10)]).await?;
    let f = bulk.insert_vertices("F", vec![row(11)]).await?;
    let s = bulk.insert_vertices("S", vec![row(12)]).await?;
    bulk.insert_edges("R", vec![EdgeData::new(a[0], f[0], HashMap::new())])
        .await?;
    let bad_source = bulk
        .insert_edges("R", vec![EdgeData::new(s[0], f[0], HashMap::new())])
        .await
        .expect_err("an S source is refused");
    assert!(
        bad_source.to_string().contains("EdgeEndpointViolation"),
        "{bad_source}"
    );
    let bad_target = bulk
        .insert_edges("R", vec![EdgeData::new(a[0], a[0], HashMap::new())])
        .await
        .expect_err("an A target is refused");
    assert!(
        bad_target.to_string().contains("EdgeEndpointViolation"),
        "{bad_target}"
    );
    // A stored (flushed) vertex is resolved too.
    let stored_s = ints(&db, "MATCH (x:S) RETURN id(x)").await?[0];
    let stored = bulk
        .insert_edges(
            "R",
            vec![EdgeData::new(
                uni_db::Vid::new(stored_s as u64),
                f[0],
                HashMap::new(),
            )],
        )
        .await
        .expect_err("a stored S source is refused");
    assert!(
        stored.to_string().contains("EdgeEndpointViolation"),
        "{stored}"
    );
    bulk.commit().await?;
    tx.commit().await?;
    assert_eq!(ints(&db, ANON).await?, vec![4]);
    assert_eq!(conformance(&db, "R")?, EndpointConformance::Enforced);
    Ok(())
}

/// Once the violating edges are deleted the type verifies, but a snapshot
/// taken before the verification still holds them: read through the
/// snapshot it is not narrowed, and restoring it withdraws the verification.
/// A transaction whose snapshot predates the verification's read blocks it.
#[tokio::test]
async fn declared_endpoint_narrowing_older_snapshots_distrust_later_verification() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().to_str().context("utf-8")?.to_string();
    let db = violating_store(dir.path()).await?;
    db.flush().await?;
    let snapshot = db.create_snapshot("before_cleanup").await?;
    let tx = db.session().tx().await?;
    tx.execute("MATCH (:A)-[r:R2]->(:S) DELETE r").await?;
    tx.execute("MATCH (:S)-[r:R2]->(:F) DELETE r").await?;
    tx.commit().await?;

    // An open transaction on an older snapshot could still read a deleted
    // edge, so the verification is refused while it lives.
    let old_tx = db.session().tx().await?;
    let verify = "CALL uni.admin.verifyEdgeEndpoints('R2') \
                  YIELD source_violations, target_violations, conformance \
                  RETURN source_violations, target_violations, conformance";
    // Commit something so the verification reads a newer version than the
    // open transaction's snapshot.
    let bump = db.session().tx().await?;
    bump.execute("CREATE (:A {v: 9})").await?;
    bump.commit().await?;
    let refused = db.session().run(verify).await;
    assert!(
        refused
            .as_ref()
            .is_err_and(|e| e.to_string().contains("still open")),
        "{refused:?}"
    );
    // Nor inside a transaction, whose own snapshot it would wait on.
    let inside = old_tx.query(verify).await;
    assert!(
        inside
            .as_ref()
            .is_err_and(|e| e.to_string().contains("inside a transaction")),
        "{inside:?}"
    );
    assert_eq!(conformance(&db, "R2")?, EndpointConformance::Unverified);
    drop(old_tx);

    let rows = db.session().run(verify).await?;
    assert_eq!(
        rows.rows()[0].values().to_vec(),
        vec![
            Value::Int(0),
            Value::Int(0),
            Value::String("verified".into())
        ]
    );
    assert!(narrowed(&db.session(), "MATCH ()-[r:R2]->() RETURN count(r)").await?);
    assert_eq!(
        ints(&db, "MATCH ()-[r:R2]->() RETURN count(r)").await?,
        vec![1]
    );

    // The snapshot predates the verification: all three edges, un-narrowed.
    let mut pinned = db.session();
    pinned.pin_to_version(&snapshot).await?;
    assert!(
        !narrowed(&pinned, "MATCH ()-[r:R2]->() RETURN count(r)").await?,
        "a snapshot older than the verification is not narrowed"
    );
    let r = pinned.query("MATCH ()-[r:R2]->() RETURN count(r)").await?;
    assert_eq!(r.rows()[0].values()[0], Value::Int(3));
    drop(pinned);

    // Restoring it withdraws the verification from the catalog, persisted.
    // (What the restored store then reads is the restore's concern; this
    // pins only that it is not planned on a verification the snapshot's
    // tables were never checked against.)
    db.restore_snapshot(&snapshot).await?;
    assert_eq!(conformance(&db, "R2")?, EndpointConformance::Unverified);
    db.shutdown().await?;
    let db = Uni::open(path).build().await?;
    assert_eq!(conformance(&db, "R2")?, EndpointConformance::Unverified);
    assert!(!narrowed(&db.session(), "MATCH ()-[r:R2]->() RETURN count(r)").await?);
    Ok(())
}
