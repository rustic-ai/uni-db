// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Phase 6 — `Uni::promote_from_fork` integration.
//!
//! Coverage:
//! 1. Basic promote: fork-only rows land on primary after promote.
//! 2. UID conflict: a row whose UID already exists on primary is
//!    skipped, not duplicated.
//! 3. `where_clause` predicate filters fork rows before promoting.
//! 4. Edge skip: edges on the fork are counted in `edges_skipped`
//!    and not promoted, as documented in §16.

use anyhow::Result;
use uni_db::api::fork_diff::PromotePattern;
use uni_db::{DataType, Uni};

#[tokio::test]
async fn promote_inserts_fork_only_vertices() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    db.schema()
        .label("Person")
        .property("name", DataType::String)
        .apply()
        .await?;
    let session = db.session();
    let tx = session.tx().await?;
    tx.execute("CREATE (:Person {name: 'Alice'})").await?;
    tx.commit().await?;
    db.flush().await?;

    {
        let fork = session.fork("audit").await?;
        let tx = fork.tx().await?;
        tx.execute("CREATE (:Person {name: 'Fork-Bob'})").await?;
        tx.execute("CREATE (:Person {name: 'Fork-Carol'})").await?;
        tx.commit().await?;
    }

    let report = db
        .promote_from_fork("audit", &[PromotePattern::label("Person")])
        .await?;
    assert!(
        report.vertices_inserted >= 2,
        "expected at least 2 promoted rows, got {}",
        report.vertices_inserted
    );

    let rows = session
        .query("MATCH (p:Person) RETURN p.name AS name")
        .await?;
    let names: Vec<String> = rows
        .rows()
        .iter()
        .filter_map(|r| r.get::<String>("name").ok())
        .collect();
    assert!(names.contains(&"Alice".to_string()));
    assert!(names.contains(&"Fork-Bob".to_string()));
    assert!(names.contains(&"Fork-Carol".to_string()));

    db.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn promote_dedupes_uid_conflict() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    db.schema()
        .label("Person")
        .property("name", DataType::String)
        .apply()
        .await?;
    let session = db.session();
    // Seed primary with a row whose UID is determined by (label, ext_id=None, {name: "Alice"}).
    let tx = session.tx().await?;
    tx.execute("CREATE (:Person {name: 'Alice'})").await?;
    tx.commit().await?;
    db.flush().await?;

    {
        let fork = session.fork("rebind").await?;
        // Fork creates a row with the same content as primary's
        // Alice — same UID derives from same property bag.
        let tx = fork.tx().await?;
        tx.execute("CREATE (:Person {name: 'Alice'})").await?;
        tx.execute("CREATE (:Person {name: 'Brand-New'})").await?;
        tx.commit().await?;
    }

    let report = db
        .promote_from_fork("rebind", &[PromotePattern::label("Person")])
        .await?;

    // Exact dedup (L10): the fork flush (done inside promote) builds the
    // UID index, so the content-UID match is deterministic — only Brand-New
    // is inserted, every same-content Alice the fork sees (its own plus the
    // inherited one) is skipped as a UID conflict, and primary keeps exactly
    // one Alice.
    assert_eq!(
        report.vertices_inserted, 1,
        "only Brand-New should insert: {report:?}"
    );
    assert!(
        report.vertices_skipped_uid_conflict >= 1,
        "the duplicate Alice should be skipped: {report:?}"
    );
    let rows = session
        .query("MATCH (p:Person) WHERE p.name = 'Alice' RETURN p.name")
        .await?;
    assert_eq!(
        rows.rows().len(),
        1,
        "Alice was duplicated by promote: rows = {:?}",
        rows.rows()
    );

    db.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn promote_respects_where_clause() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    db.schema()
        .label("Item")
        .property("name", DataType::String)
        .property("price", DataType::Int64)
        .apply()
        .await?;
    let session = db.session();
    let tx = session.tx().await?;
    tx.execute("CREATE (:Item {name: 'seed', price: 1})")
        .await?;
    tx.commit().await?;
    db.flush().await?;

    {
        let fork = session.fork("filter").await?;
        let tx = fork.tx().await?;
        tx.execute("CREATE (:Item {name: 'cheap', price: 5})")
            .await?;
        tx.execute("CREATE (:Item {name: 'mid', price: 50})")
            .await?;
        tx.execute("CREATE (:Item {name: 'pricey', price: 500})")
            .await?;
        tx.commit().await?;
    }

    let report = db
        .promote_from_fork(
            "filter",
            &[PromotePattern::label("Item").where_clause("n.price >= 100")],
        )
        .await?;
    assert_eq!(
        report.vertices_inserted, 1,
        "only 'pricey' should match price >= 100: {:?}",
        report
    );

    db.shutdown().await?;
    Ok(())
}

/// Review M4: with upsert enabled, a fork edit to an existing primary
/// vertex (matched by `(label, ext_id)`) updates in place instead of
/// inserting a twin — the headline full-merge fix.
#[tokio::test]
async fn promote_upsert_updates_existing_by_ext_id() -> Result<()> {
    use uni_db::api::fork_diff::PromoteOptions;

    let dir = tempfile::tempdir()?;
    let db = Uni::open(dir.path().to_str().unwrap()).build().await?;
    db.schema()
        .label("Person")
        .property("name", DataType::String)
        .property("age", DataType::Int64)
        .apply()
        .await?;
    let session = db.session();
    let tx = session.tx().await?;
    tx.execute("CREATE (:Person {ext_id: 'p1', name: 'Alice', age: 30})")
        .await?;
    tx.commit().await?;
    db.flush().await?;

    // Fork pins the age-30 view of Alice (ext_id p1).
    let _fork = session.fork("edit").await?;

    // Primary then diverges to age 99 (fork isolation keeps the fork at
    // age 30). Promote with upsert must apply the fork's value back onto
    // the *same* primary vertex (matched by ext_id), not insert a twin —
    // which would also violate ext_id uniqueness.
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
        .await?;
    assert_eq!(
        report.vertices_updated, 1,
        "fork value should update the existing primary vertex: {report:?}"
    );
    assert_eq!(
        report.vertices_inserted, 0,
        "upsert must not insert a twin: {report:?}"
    );

    let rows = session
        .query("MATCH (p:Person {name: 'Alice'}) RETURN p.age AS age")
        .await?;
    assert_eq!(
        rows.rows().len(),
        1,
        "Alice was twinned by promote: {:?}",
        rows.rows()
    );
    let age: i64 = rows.rows()[0].get("age")?;
    assert_eq!(
        age, 30,
        "primary Alice should reflect the promoted fork value"
    );

    db.shutdown().await?;
    Ok(())
}

/// Review L10: a re-promote of an unchanged fork is idempotent — the
/// second run is all no-ops, with exact counts (not just "<= 2 rows").
#[tokio::test]
async fn promote_upsert_idempotent_after_flush() -> Result<()> {
    use uni_db::api::fork_diff::PromoteOptions;

    let dir = tempfile::tempdir()?;
    let db = Uni::open(dir.path().to_str().unwrap()).build().await?;
    db.schema()
        .label("Person")
        .property("name", DataType::String)
        .property("age", DataType::Int64)
        .apply()
        .await?;
    let session = db.session();
    let tx = session.tx().await?;
    tx.execute("CREATE (:Person {ext_id: 'p1', name: 'Alice', age: 30})")
        .await?;
    tx.commit().await?;
    db.flush().await?;

    // Fork pins age 30; primary diverges to 99 (see the upsert test).
    let _fork = session.fork("edit").await?;
    let tx = session.tx().await?;
    tx.execute("MATCH (p:Person {name: 'Alice'}) SET p.age = 99")
        .await?;
    tx.commit().await?;
    db.flush().await?;

    let r1 = db
        .promote_from_fork_with_options(
            "edit",
            &[PromotePattern::label("Person")],
            &PromoteOptions::with_upsert(),
        )
        .await?;
    assert_eq!(r1.vertices_updated, 1);
    db.flush().await?;

    // Second promote of the same (now-matching) fork: pure no-op.
    let r2 = db
        .promote_from_fork_with_options(
            "edit",
            &[PromotePattern::label("Person")],
            &PromoteOptions::with_upsert(),
        )
        .await?;
    assert_eq!(
        r2.vertices_updated, 0,
        "re-promote should not re-update: {r2:?}"
    );
    assert_eq!(
        r2.vertices_inserted, 0,
        "re-promote should not insert: {r2:?}"
    );
    assert_eq!(
        r2.vertices_skipped_no_op, 1,
        "re-promote should be a no-op: {r2:?}"
    );

    let rows = session
        .query("MATCH (p:Person {name: 'Alice'}) RETURN p.age AS age")
        .await?;
    assert_eq!(
        rows.rows().len(),
        1,
        "Alice count must stay 1: {:?}",
        rows.rows()
    );

    db.shutdown().await?;
    Ok(())
}

/// Review M4: the legacy (default) promote never upserts — the same
/// divergence the upsert path merges in place is instead inserted as a
/// (content) twin under the default options. Locks the backward-compatible
/// contract. (The promote insert carries no `ext_id` — it is stripped from
/// the scanned props — so it does not collide on `ext_id`; see the
/// separate `bulk_create_*` ext_id-uniqueness test for that path.)
#[tokio::test]
async fn promote_default_is_insert_only_twin() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let db = Uni::open(dir.path().to_str().unwrap()).build().await?;
    db.schema()
        .label("Person")
        .property("name", DataType::String)
        .property("age", DataType::Int64)
        .apply()
        .await?;
    let session = db.session();
    let tx = session.tx().await?;
    tx.execute("CREATE (:Person {ext_id: 'p1', name: 'Alice', age: 30})")
        .await?;
    tx.commit().await?;
    db.flush().await?;

    // Fork pins age 30; primary diverges to 99. Under the default options
    // promote must NOT update primary.
    let _fork = session.fork("edit").await?;
    let tx = session.tx().await?;
    tx.execute("MATCH (p:Person {name: 'Alice'}) SET p.age = 99")
        .await?;
    tx.commit().await?;
    db.flush().await?;

    let report = db
        .promote_from_fork("edit", &[PromotePattern::label("Person")])
        .await?;
    assert_eq!(
        report.vertices_updated, 0,
        "default options must never update: {report:?}"
    );
    assert_eq!(
        report.vertices_inserted, 1,
        "default insert-only twins the divergent fork vertex: {report:?}"
    );

    let rows = session
        .query("MATCH (p:Person {name: 'Alice'}) RETURN p.age AS age ORDER BY age")
        .await?;
    let ages: Vec<i64> = rows
        .rows()
        .iter()
        .filter_map(|r| r.get::<i64>("age").ok())
        .collect();
    assert_eq!(
        ages,
        vec![30, 99],
        "expected primary(99) + twin(30) under the insert-only default"
    );

    db.shutdown().await?;
    Ok(())
}

/// Re-promoting a fork that made NO changes is idempotent — it must not spawn a
/// content twin on each call (correctness-scan uni-fork[3], D3). The registered
/// content-UID double-folds `ext_id` (arg + stored `"ext_id"` property), while
/// Cypher strips that key; the promote side now re-injects `ext_id`
/// (`content_uid_with_ext_id`) so the dedup recognizes the unchanged row and
/// skips it. Before the fix the UID never matched for ext_id-bearing rows, so
/// every re-promote inserted another twin (unbounded growth).
#[tokio::test]
async fn promote_default_is_idempotent_for_unchanged_ext_id_row() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let db = Uni::open(dir.path().to_str().unwrap()).build().await?;
    db.schema()
        .label("Person")
        .property("name", DataType::String)
        .property("age", DataType::Int64)
        .apply()
        .await?;
    let session = db.session();
    let tx = session.tx().await?;
    tx.execute("CREATE (:Person {ext_id: 'p1', name: 'Alice', age: 30})")
        .await?;
    tx.commit().await?;
    db.flush().await?;

    // Fork makes NO changes; promoting it twice must leave a single Alice.
    let _fork = session.fork("noop").await?;

    let first = db
        .promote_from_fork("noop", &[PromotePattern::label("Person")])
        .await?;
    db.flush().await?;
    let second = db
        .promote_from_fork("noop", &[PromotePattern::label("Person")])
        .await?;
    db.flush().await?;

    // Neither promote may insert a twin: the unchanged row dedups by content-UID.
    assert_eq!(
        (first.vertices_inserted, second.vertices_inserted),
        (0, 0),
        "unchanged re-promote must insert nothing: first={first:?} second={second:?}"
    );

    let rows = session
        .query("MATCH (p:Person {name: 'Alice'}) RETURN p.age AS age")
        .await?;
    assert_eq!(
        rows.rows().len(),
        1,
        "exactly one Alice must remain after two no-op promotes: {:?}",
        rows.rows()
    );

    db.shutdown().await?;
    Ok(())
}

/// Review M4: delete-promotion removes a vertex the fork deleted, while a
/// primary row the fork never saw survives (the anti-spurious-delete
/// guarantee). The fork deletes an inherited vertex (now supported).
#[tokio::test]
async fn promote_delete_promotion_removes_fork_deleted_row() -> Result<()> {
    use uni_db::api::fork_diff::PromoteOptions;

    let dir = tempfile::tempdir()?;
    let db = Uni::open(dir.path().to_str().unwrap()).build().await?;
    db.schema()
        .label("Person")
        .property("name", DataType::String)
        .apply()
        .await?;
    let session = db.session();
    let tx = session.tx().await?;
    tx.execute("CREATE (:Person {ext_id: 'p1', name: 'Alice'})")
        .await?;
    tx.execute("CREATE (:Person {ext_id: 'p2', name: 'Bob'})")
        .await?;
    tx.commit().await?;
    db.flush().await?;

    // Fork inherits p1, p2 and deletes p1 (an inherited vertex).
    let fork = session.fork("del").await?;
    let ftx = fork.tx().await?;
    ftx.execute("MATCH (n:Person {name: 'Alice'}) DETACH DELETE n")
        .await?;
    ftx.commit().await?;

    // Primary independently adds p3 AFTER the fork point — the fork never
    // saw it, so it must NOT be delete-promoted.
    let tx = session.tx().await?;
    tx.execute("CREATE (:Person {ext_id: 'p3', name: 'Carol'})")
        .await?;
    tx.commit().await?;
    db.flush().await?;

    let report = db
        .promote_from_fork_with_options(
            "del",
            &[PromotePattern::label("Person")],
            &PromoteOptions::with_merge(),
        )
        .await?;
    assert_eq!(
        report.vertices_deleted, 1,
        "fork-deleted p1 should be delete-promoted: {report:?}"
    );

    let names = names(&session, "MATCH (n:Person) RETURN n.name AS name").await?;
    assert_eq!(
        names,
        vec!["Bob".to_string(), "Carol".to_string()],
        "Alice deleted; Bob kept; Carol (primary-only, fork never saw) survives"
    );

    db.shutdown().await?;
    Ok(())
}

/// Review M4: a fork-point row WITHOUT an ext_id that the fork deleted
/// cannot be safely delete-promoted (no stable key) — it is reported in
/// `vertices_skipped_no_ext_id_for_delete`, never deleted.
#[tokio::test]
async fn promote_delete_skips_no_ext_id() -> Result<()> {
    use uni_db::api::fork_diff::PromoteOptions;

    let dir = tempfile::tempdir()?;
    let db = Uni::open(dir.path().to_str().unwrap()).build().await?;
    db.schema()
        .label("Person")
        .property("name", DataType::String)
        .apply()
        .await?;
    let session = db.session();
    let tx = session.tx().await?;
    // No ext_id on this vertex.
    tx.execute("CREATE (:Person {name: 'NoExt'})").await?;
    tx.commit().await?;
    db.flush().await?;

    let fork = session.fork("del").await?;
    let ftx = fork.tx().await?;
    ftx.execute("MATCH (n:Person {name: 'NoExt'}) DETACH DELETE n")
        .await?;
    ftx.commit().await?;

    let report = db
        .promote_from_fork_with_options(
            "del",
            &[PromotePattern::label("Person")],
            &PromoteOptions::with_merge(),
        )
        .await?;
    assert_eq!(
        report.vertices_deleted, 0,
        "no ext_id ⇒ no delete: {report:?}"
    );
    assert_eq!(
        report.vertices_skipped_no_ext_id_for_delete, 1,
        "the ext_id-less fork deletion must be reported: {report:?}"
    );
    assert_eq!(
        names(&session, "MATCH (n:Person) RETURN n.name AS name").await?,
        vec!["NoExt".to_string()],
        "primary row must remain (not delete-promoted)"
    );

    db.shutdown().await?;
    Ok(())
}

/// Review M4: a concurrent divergent edit (fork AND primary both moved off
/// the fork-point baseline) is a conflict; the default Skip policy leaves
/// primary's value.
#[tokio::test]
async fn promote_conflict_skip() -> Result<()> {
    use uni_db::api::fork_diff::PromoteOptions;

    let (db, session) = conflict_setup().await?;
    let report = db
        .promote_from_fork_with_options(
            "edit",
            &[PromotePattern::label("Person")],
            &PromoteOptions::with_merge(),
        )
        .await?;
    assert_eq!(
        report.vertices_conflicting, 1,
        "expected a conflict: {report:?}"
    );
    assert_eq!(
        report.vertices_updated, 0,
        "Skip must not update: {report:?}"
    );
    let age: i64 = session
        .query("MATCH (n:Person {name: 'Alice'}) RETURN n.age AS age")
        .await?
        .rows()[0]
        .get("age")?;
    assert_eq!(age, 99, "Skip keeps primary's diverged value");
    db.shutdown().await?;
    Ok(())
}

/// Review M4: with `Overwrite`, a conflict applies the fork's value.
#[tokio::test]
async fn promote_conflict_overwrite() -> Result<()> {
    use uni_db::api::fork_diff::{ConflictPolicy, PromoteOptions};

    let (db, session) = conflict_setup().await?;
    let report = db
        .promote_from_fork_with_options(
            "edit",
            &[PromotePattern::label("Person")],
            &PromoteOptions::with_merge().on_conflict(ConflictPolicy::Overwrite),
        )
        .await?;
    assert_eq!(
        report.vertices_conflicting, 1,
        "expected a conflict: {report:?}"
    );
    assert_eq!(
        report.vertices_updated, 1,
        "Overwrite must update: {report:?}"
    );
    let age: i64 = session
        .query("MATCH (n:Person {name: 'Alice'}) RETURN n.age AS age")
        .await?
        .rows()[0]
        .get("age")?;
    assert_eq!(age, 31, "Overwrite applies the fork's value");
    db.shutdown().await?;
    Ok(())
}

/// Review M4: a clean merge is idempotent — re-promoting after a flush is
/// all no-ops (no double-update, no spurious conflict).
#[tokio::test]
async fn promote_merge_idempotent() -> Result<()> {
    use uni_db::api::fork_diff::PromoteOptions;

    let dir = tempfile::tempdir()?;
    let db = Uni::open(dir.path().to_str().unwrap()).build().await?;
    db.schema()
        .label("Person")
        .property("name", DataType::String)
        .property("age", DataType::Int64)
        .apply()
        .await?;
    let session = db.session();
    let tx = session.tx().await?;
    tx.execute("CREATE (:Person {ext_id: 'p1', name: 'Alice', age: 30})")
        .await?;
    tx.commit().await?;
    db.flush().await?;

    // Only the fork changes Alice (primary stays at baseline) → clean update.
    let fork = session.fork("edit").await?;
    let ftx = fork.tx().await?;
    ftx.execute("MATCH (n:Person {name: 'Alice'}) SET n.age = 31")
        .await?;
    ftx.commit().await?;

    let r1 = db
        .promote_from_fork_with_options(
            "edit",
            &[PromotePattern::label("Person")],
            &PromoteOptions::with_merge(),
        )
        .await?;
    assert_eq!(
        r1.vertices_updated, 1,
        "first merge applies the fork edit: {r1:?}"
    );
    db.flush().await?;

    let r2 = db
        .promote_from_fork_with_options(
            "edit",
            &[PromotePattern::label("Person")],
            &PromoteOptions::with_merge(),
        )
        .await?;
    assert_eq!(r2.vertices_updated, 0, "re-merge must not update: {r2:?}");
    assert_eq!(
        r2.vertices_conflicting, 0,
        "re-merge must not conflict: {r2:?}"
    );
    assert_eq!(r2.vertices_deleted, 0, "re-merge must not delete: {r2:?}");
    assert_eq!(r2.vertices_skipped_no_op, 1, "re-merge is a no-op: {r2:?}");
    db.shutdown().await?;
    Ok(())
}

/// Shared setup for the conflict tests: primary Alice (ext_id p1, age 30,
/// flushed); fork edits her age to 31; primary independently edits to 99.
async fn conflict_setup() -> Result<(Uni, uni_db::Session)> {
    let dir = tempfile::tempdir()?;
    // Leak the tempdir so the DB path stays valid for the test's lifetime.
    let path = dir.keep().to_str().unwrap().to_string();
    let db = Uni::open(&path).build().await?;
    db.schema()
        .label("Person")
        .property("name", DataType::String)
        .property("age", DataType::Int64)
        .apply()
        .await?;
    let session = db.session();
    let tx = session.tx().await?;
    tx.execute("CREATE (:Person {ext_id: 'p1', name: 'Alice', age: 30})")
        .await?;
    tx.commit().await?;
    db.flush().await?;

    let fork = session.fork("edit").await?;
    let ftx = fork.tx().await?;
    ftx.execute("MATCH (n:Person {name: 'Alice'}) SET n.age = 31")
        .await?;
    ftx.commit().await?;

    // Primary diverges independently.
    let tx = session.tx().await?;
    tx.execute("MATCH (n:Person {name: 'Alice'}) SET n.age = 99")
        .await?;
    tx.commit().await?;
    db.flush().await?;

    Ok((db, session))
}

async fn names(s: &uni_db::Session, cypher: &str) -> Result<Vec<String>> {
    let rows = s.query(cypher).await?;
    let mut out: Vec<String> = rows
        .rows()
        .iter()
        .filter_map(|r| r.get::<String>("name").ok())
        .collect();
    out.sort();
    Ok(out)
}

/// The dedup must see primary rows that are not flushed yet: the UidIndex
/// covers flushed rows only, so a vertex primary committed after the fork point
/// and still holds in L0 was not found, and promoting the same content from the
/// fork inserted a duplicate.
#[tokio::test]
async fn promote_dedups_against_unflushed_primary_rows() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let db = Uni::open(dir.path().to_str().unwrap()).build().await?;
    db.schema()
        .label("Person")
        .property("name", DataType::String)
        .apply()
        .await?;
    let session = db.session();
    let tx = session.tx().await?;
    tx.execute("CREATE (:Person {name: 'Alice'})").await?;
    tx.commit().await?;
    db.flush().await?;

    let fork = session.fork("same").await?;
    // Primary writes Carol after the fork point and does not flush.
    let tx = session.tx().await?;
    tx.execute("CREATE (:Person {name: 'Carol'})").await?;
    tx.commit().await?;
    // The fork writes the same content, after another row, so the fork's
    // Carol does not happen to get the vid primary's Carol has.
    let tx = fork.tx().await?;
    tx.execute("CREATE (:Person {name: 'Dave'})").await?;
    tx.execute("CREATE (:Person {name: 'Carol'})").await?;
    tx.commit().await?;
    drop(fork);

    let report = db
        .promote_from_fork("same", &[PromotePattern::label("Person")])
        .await?;
    assert_eq!(
        report.vertices_inserted, 1,
        "only Dave is new; Carol is already on primary (unflushed); got {report:?}"
    );
    let rows = session
        .query("MATCH (p:Person {name: 'Carol'}) RETURN count(p) AS c")
        .await?;
    assert_eq!(rows.rows()[0].get::<i64>("c")?, 1);
    db.shutdown().await?;
    Ok(())
}
