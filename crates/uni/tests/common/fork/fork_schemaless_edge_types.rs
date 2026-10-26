// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Forks over schemaless (undeclared) edge types and labels.
//!
//! A schemaless edge type gets a high-bit id from the in-memory
//! `SchemalessEdgeTypeRegistry`. On a fork that id is minted in the fork's
//! merged schema, which is never saved to primary's catalog, so these tests
//! pin what must survive a fork session being reopened (or the whole
//! database being reopened): the edge type's NAME, never its numeric id.

// Rust guideline compliant

use anyhow::Result;
use uni_db::api::fork_diff::PromotePattern;
use uni_db::{DataType, Uni, UniError};

/// Count `MATCH ()-[r:<ty>]->()` on `session`.
async fn count_typed(session: &uni_db::Session, ty: &str) -> Result<i64> {
    Ok(session
        .query(&format!("MATCH ()-[r:{ty}]->() RETURN count(r) AS c"))
        .await?
        .rows()[0]
        .get::<i64>("c")?)
}

/// Sorted `type(r)` of every edge `session` sees through an untyped match.
async fn untyped_edge_types(session: &uni_db::Session) -> Result<Vec<String>> {
    let rs = session
        .query("MATCH ()-[r]->() RETURN type(r) AS t ORDER BY t")
        .await?;
    rs.rows()
        .iter()
        .map(|r| Ok(r.get::<String>("t")?))
        .collect()
}

/// A fork mints a schemaless edge type id; primary later mints the *same*
/// numeric id for a different name. Reopening the fork replays its WAL into
/// a schema built from primary's current registry, where that id now means
/// the other name. The fork's edges must keep their own type, before and
/// after the fork's flush.
#[tokio::test]
async fn fork_minted_schemaless_id_reused_by_primary_keeps_its_name() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let db = Uni::open(dir.path().to_str().unwrap()).build().await?;
    let session = db.session();
    let tx = session.tx().await?;
    tx.execute("CREATE (:Thing {k: 1})").await?;
    tx.commit().await?;
    db.flush().await?;

    {
        // The fork mints LINK (first schemaless id in its registry).
        let fork = session.fork("f").await?;
        let tx = fork.tx().await?;
        tx.execute("MATCH (t:Thing {k: 1}) CREATE (t)-[:LINK]->(:Thing {k: 2})")
            .await?;
        tx.commit().await?;
        assert_eq!(count_typed(&fork, "LINK").await?, 1);
    }

    // Primary, unaware of the fork's LINK, mints FOO: same numeric id.
    let tx = session.tx().await?;
    tx.execute("CREATE (:Other {k: 1})-[:FOO]->(:Other {k: 2})")
        .await?;
    tx.commit().await?;

    let fork = session.fork("f").await?;
    assert_eq!(
        untyped_edge_types(&fork).await?,
        vec!["LINK".to_string()],
        "replayed fork edge must keep its type before flush"
    );
    fork.flush().await?;
    assert_eq!(count_typed(&fork, "LINK").await?, 1, "after fork flush");
    assert_eq!(count_typed(&fork, "FOO").await?, 0, "after fork flush");
    assert_eq!(untyped_edge_types(&fork).await?, vec!["LINK".to_string()]);
    // Primary is untouched by the fork.
    assert_eq!(count_typed(&session, "LINK").await?, 0);
    assert_eq!(count_typed(&session, "FOO").await?, 1);
    drop(fork);
    db.shutdown().await?;
    Ok(())
}

/// Declare `Thing` and `LINK` on `db`, so an arm runs over a declared type.
async fn declare_thing_link(db: &Uni) -> Result<()> {
    db.schema()
        .label("Thing")
        .property_nullable("k", DataType::Int64)
        .edge_type("LINK", &["Thing"], &["Thing"])
        .apply()
        .await?;
    Ok(())
}

/// A fork with one `(:Thing {k: 1})` inherited from primary and one
/// fork-written `-[:LINK]->(:Thing {k: 2})`, flushed on the fork.
async fn db_with_flushed_fork_link(dir: &std::path::Path, declared: bool) -> Result<Uni> {
    let db = Uni::open(dir.to_str().unwrap()).build().await?;
    if declared {
        declare_thing_link(&db).await?;
    }
    let session = db.session();
    let tx = session.tx().await?;
    tx.execute("CREATE (:Thing {k: 1})").await?;
    tx.commit().await?;
    db.flush().await?;
    let fork = session.fork("f").await?;
    let tx = fork.tx().await?;
    tx.execute("MATCH (t:Thing {k: 1}) CREATE (t)-[:LINK]->(:Thing {k: 2})")
        .await?;
    tx.commit().await?;
    fork.flush().await?;
    Ok(db)
}

/// The delete side of the id-reuse hazard: a fork deletes a flushed edge,
/// primary then mints a schemaless id (reusing the fork's `LINK` id when
/// `LINK` is fork-only), and the fork is reopened with the delete still in
/// its WAL. The replayed delete must tombstone the fork's `LINK` edge.
/// Returns the fork's `LINK` count (before, after) its flush.
async fn replay_fork_edge_delete(declared: bool) -> Result<(i64, i64)> {
    let dir = tempfile::tempdir()?;
    let db = db_with_flushed_fork_link(dir.path(), declared).await?;
    let session = db.session();
    {
        let fork = session.fork("f").await?;
        let tx = fork.tx().await?;
        tx.execute("MATCH ()-[r:LINK]->() DELETE r").await?;
        tx.commit().await?;
        assert_eq!(count_typed(&fork, "LINK").await?, 0);
    }
    let tx = session.tx().await?;
    tx.execute("CREATE (:Other {k: 1})-[:FOO]->(:Other {k: 2})")
        .await?;
    tx.commit().await?;

    let fork = session.fork("f").await?;
    let before = count_typed(&fork, "LINK").await?;
    fork.flush().await?;
    let after = count_typed(&fork, "LINK").await?;
    assert!(untyped_edge_types(&fork).await?.is_empty());
    drop(fork);
    db.shutdown().await?;
    Ok((before, after))
}

#[tokio::test]
async fn replayed_fork_edge_delete_resolves_its_type_by_name() -> Result<()> {
    assert_eq!(
        replay_fork_edge_delete(false).await?,
        (0, 0),
        "schemaless LINK"
    );
    assert_eq!(
        replay_fork_edge_delete(true).await?,
        (0, 0),
        "declared LINK"
    );
    Ok(())
}

/// Count of `(:Thing {k: 3})` on `session`.
async fn count_updated_thing(session: &uni_db::Session) -> Result<i64> {
    Ok(session
        .query("MATCH (t:Thing {k: 3}) RETURN count(t) AS c")
        .await?
        .rows()[0]
        .get::<i64>("c")?)
}

/// A fork session opened after the fork already flushed must stamp its
/// writes above every version the fork flushed. Its version floor used to
/// restart at the fork point on every open, so a write to a row the fork
/// had flushed tied with (or fell below) that row's version and lost to it:
/// a deleted edge came back and an update was dropped. Declared and
/// schemaless alike. Returns (edge count after delete, updated vertex count)
/// before and after the fork's flush.
async fn write_flushed_rows_in_reopened_fork(declared: bool) -> Result<[i64; 4]> {
    let dir = tempfile::tempdir()?;
    let db = db_with_flushed_fork_link(dir.path(), declared).await?;
    let session = db.session();
    let fork = session.fork("f").await?;
    let tx = fork.tx().await?;
    tx.execute("MATCH ()-[r:LINK]->() DELETE r").await?;
    tx.execute("MATCH (t:Thing {k: 2}) SET t.k = 3").await?;
    tx.commit().await?;
    let links_before = count_typed(&fork, "LINK").await?;
    let updated_before = count_updated_thing(&fork).await?;
    fork.flush().await?;
    let out = [
        links_before,
        updated_before,
        count_typed(&fork, "LINK").await?,
        count_updated_thing(&fork).await?,
    ];
    drop(fork);
    db.shutdown().await?;
    Ok(out)
}

#[tokio::test]
async fn reopened_fork_writes_supersede_rows_the_fork_already_flushed() -> Result<()> {
    assert_eq!(
        write_flushed_rows_in_reopened_fork(false).await?,
        [0, 1, 0, 1],
        "schemaless LINK"
    );
    assert_eq!(
        write_flushed_rows_in_reopened_fork(true).await?,
        [0, 1, 0, 1],
        "declared LINK"
    );
    Ok(())
}

/// A schemaless edge type written on primary before the fork is part of the
/// fork point: primary's later edges of that type must not leak into the
/// fork through an unbranched `deltas_{type}_*` dataset.
#[tokio::test]
async fn schemaless_edge_type_is_isolated_at_the_fork_point() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let db = Uni::open(dir.path().to_str().unwrap()).build().await?;
    let session = db.session();
    let tx = session.tx().await?;
    tx.execute("CREATE (:A {k: 1})-[:REL]->(:A {k: 2})").await?;
    tx.commit().await?;
    db.flush().await?;

    drop(session.fork("f").await?);

    // Between vertices that exist at the fork point: an edge to post-fork
    // vertices would be hidden anyway, since the fork cannot see its ends.
    let tx = session.tx().await?;
    tx.execute("MATCH (a:A {k: 2}), (b:A {k: 1}) CREATE (a)-[:REL]->(b)")
        .await?;
    tx.commit().await?;
    db.flush().await?;
    assert_eq!(count_typed(&session, "REL").await?, 2);

    let fork = session.fork("f").await?;
    assert_eq!(
        count_typed(&fork, "REL").await?,
        1,
        "the fork must not see primary's post-fork REL edge"
    );
    assert_eq!(untyped_edge_types(&fork).await?, vec!["REL".to_string()]);

    // The fork's own first REL write branches at the fork point too.
    let tx = fork.tx().await?;
    tx.execute("MATCH (a:A {k: 1}) CREATE (a)-[:REL]->(:A {k: 5})")
        .await?;
    tx.commit().await?;
    fork.flush().await?;
    assert_eq!(count_typed(&fork, "REL").await?, 2);
    assert_eq!(count_typed(&session, "REL").await?, 2);
    drop(fork);
    db.shutdown().await?;
    Ok(())
}

/// A fork-only schemaless edge type, flushed on the fork, stays visible to an
/// untyped match after the database itself is reopened: the type's name has
/// to be persisted with the fork, since primary's catalog never learns it.
#[tokio::test]
async fn fork_only_schemaless_edges_survive_database_reopen() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().to_str().unwrap().to_string();
    {
        let db = Uni::open(&path).build().await?;
        let session = db.session();
        let tx = session.tx().await?;
        tx.execute("CREATE (:Thing {k: 1})").await?;
        tx.commit().await?;
        db.flush().await?;
        let fork = session.fork("f").await?;
        let tx = fork.tx().await?;
        tx.execute("MATCH (t:Thing {k: 1}) CREATE (t)-[:FORKONLY]->(:Thing {k: 2})")
            .await?;
        tx.commit().await?;
        fork.flush().await?;
        assert_eq!(
            untyped_edge_types(&fork).await?,
            vec!["FORKONLY".to_string()]
        );
        drop(fork);
        db.shutdown().await?;
    }
    let db = Uni::open(&path).build().await?;
    let session = db.session();
    let fork = session.fork("f").await?;
    assert_eq!(
        untyped_edge_types(&fork).await?,
        vec!["FORKONLY".to_string()],
        "an untyped match on the reopened fork must still see its flushed edge"
    );
    assert_eq!(count_typed(&fork, "FORKONLY").await?, 1);
    assert!(untyped_edge_types(&session).await?.is_empty());
    drop(fork);
    db.shutdown().await?;
    Ok(())
}

/// Promote still refuses a label or edge type the fork *declared* (via its schema
/// overlay) but primary does not have: only a type undeclared on both
/// sides is promoted schemalessly.
#[tokio::test]
async fn promote_of_fork_declared_types_missing_on_primary_is_refused() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    let session = db.session();
    {
        let fork = session.fork("f").await?;
        fork.fork_schema()
            .label("ForkOnly")
            .edge_type("FORK_REL", &["ForkOnly"], &["ForkOnly"])
            .apply()
            .await?;
        let tx = fork.tx().await?;
        tx.execute("CREATE (:ForkOnly {k: 1})-[:FORK_REL]->(:ForkOnly {k: 2})")
            .await?;
        tx.commit().await?;
    }
    let res = db
        .promote_from_fork("f", &[PromotePattern::label("ForkOnly")])
        .await;
    assert!(
        matches!(&res, Err(UniError::LabelNotFound { label }) if label == "ForkOnly"),
        "expected LabelNotFound, got {res:?}"
    );
    let res = db
        .promote_from_fork("f", &[PromotePattern::edge_type("FORK_REL")])
        .await;
    assert!(
        matches!(&res, Err(UniError::EdgeTypeNotFound { edge_type }) if edge_type == "FORK_REL"),
        "expected EdgeTypeNotFound, got {res:?}"
    );
    // Nothing reached primary.
    let n = session
        .query("MATCH (n:ForkOnly) RETURN count(n) AS c")
        .await?
        .rows()[0]
        .get::<i64>("c")?;
    assert_eq!(n, 0);
    db.shutdown().await?;
    Ok(())
}

/// Promote of a schemaless edge type, with its schemaless endpoints, in one
/// call: the edge lands on primary with its type name.
#[tokio::test]
async fn promote_schemaless_edge_type_with_endpoints() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    let session = db.session();
    let tx = session.tx().await?;
    tx.execute("CREATE (:Thing {k: 1})").await?;
    tx.commit().await?;
    db.flush().await?;
    {
        let fork = session.fork("f").await?;
        let tx = fork.tx().await?;
        tx.execute("MATCH (t:Thing {k: 1}) CREATE (t)-[:LINK {w: 7}]->(:Thing {k: 2})")
            .await?;
        tx.commit().await?;
    }
    let report = db
        .promote_from_fork(
            "f",
            &[
                PromotePattern::label("Thing"),
                PromotePattern::edge_type("LINK"),
            ],
        )
        .await?;
    assert_eq!(report.vertices_inserted, 1, "{report:?}");
    assert_eq!(report.edges_inserted, 1, "{report:?}");
    let rs = session
        .query("MATCH (a:Thing)-[r:LINK]->(b:Thing) RETURN a.k AS a, r.w AS w, b.k AS b")
        .await?;
    assert_eq!(rs.rows().len(), 1);
    assert_eq!(rs.rows()[0].get::<i64>("a")?, 1);
    assert_eq!(rs.rows()[0].get::<i64>("w")?, 7);
    assert_eq!(rs.rows()[0].get::<i64>("b")?, 2);
    db.shutdown().await?;
    Ok(())
}
