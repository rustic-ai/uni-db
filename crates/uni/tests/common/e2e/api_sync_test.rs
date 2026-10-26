// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

use anyhow::Result;
use uni_db::{DataType, Uni, UniSync};

#[test]
fn test_sync_api() -> Result<()> {
    // 1. Initialize
    let db = UniSync::in_memory()?;

    // 2. Schema (Sync)
    db.schema()
        .label("User")
        .property("name", DataType::String)
        .property("age", DataType::Int32)
        .apply()?;

    // 3. Execute via transaction
    let session = db.session();
    let tx = session.tx()?;
    tx.execute("CREATE (:User {name: 'Alice', age: 30})")?;
    tx.execute("CREATE (:User {name: 'Bob', age: 25})")?;
    tx.commit()?;

    // 4. Query
    let result = session.query("MATCH (u:User) RETURN u.name, u.age ORDER BY u.age")?;
    assert_eq!(result.len(), 2);

    let row0 = &result.rows()[0];
    assert_eq!(row0.get::<String>("u.name")?, "Bob");
    assert_eq!(row0.get::<i32>("u.age")?, 25);

    // 5. Transaction
    let tx = session.tx()?;
    tx.execute("CREATE (:User {name: 'Charlie', age: 40})")?;
    tx.commit()?;

    let result = session.query("MATCH (u:User) WHERE u.name = 'Charlie' RETURN u.age")?;
    assert_eq!(result.len(), 1);
    assert_eq!(result.rows()[0].get::<i32>("u.age")?, 40);

    Ok(())
}

#[test]
fn test_sync_api_rollback() -> Result<()> {
    let db = UniSync::in_memory()?;
    db.schema()
        .label("Item")
        .property("name", DataType::String)
        .apply()?;

    // Commit some baseline data
    let session = db.session();
    let tx = session.tx()?;
    tx.execute("CREATE (:Item {name: 'Baseline'})")?;
    tx.commit()?;

    // Start new transaction, add data, then drop without committing (rollback)
    {
        let tx = session.tx()?;
        tx.execute("CREATE (:Item {name: 'Uncommitted'})")?;
        // tx drops here without commit — implicit rollback
    }

    // Verify only baseline data exists
    let result = session.query("MATCH (i:Item) RETURN i.name AS name ORDER BY name")?;
    assert_eq!(result.len(), 1, "Rolled-back data should not be visible");
    assert_eq!(result.rows()[0].get::<String>("name")?, "Baseline");

    Ok(())
}

#[test]
fn test_sync_api_error_handling() -> Result<()> {
    let db = UniSync::in_memory()?;
    db.schema()
        .label("X")
        .property("v", DataType::Int32)
        .apply()?;

    let session = db.session();

    // Invalid Cypher syntax should return error, not panic
    let result = session.query("THIS IS NOT VALID CYPHER");
    assert!(
        result.is_err(),
        "Invalid Cypher should return error, not panic"
    );

    Ok(())
}

#[test]
fn test_sync_api_query_no_results() -> Result<()> {
    let db = UniSync::in_memory()?;
    db.schema()
        .label("Ghost")
        .property("name", DataType::String)
        .apply()?;

    // Query empty database
    let session = db.session();
    let result = session.query("MATCH (n:Ghost) RETURN n")?;
    assert_eq!(result.len(), 0, "Empty DB should return 0 rows, not error");

    Ok(())
}

#[test]
fn test_sync_api_edge_operations() -> Result<()> {
    let db = UniSync::in_memory()?;
    db.schema()
        .label("Person")
        .property("name", DataType::String)
        .done()
        .edge_type("KNOWS", &["Person"], &["Person"])
        .apply()?;

    let session = db.session();
    let tx = session.tx()?;
    tx.execute("CREATE (a:Person {name: 'Alice'})-[:KNOWS]->(b:Person {name: 'Bob'})")?;
    tx.commit()?;

    // Verify traversal
    let result = session
        .query("MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN a.name AS src, b.name AS dst")?;
    assert_eq!(result.len(), 1);
    assert_eq!(result.rows()[0].get::<String>("src")?, "Alice");
    assert_eq!(result.rows()[0].get::<String>("dst")?, "Bob");

    Ok(())
}

/// `UniBuilder::build_sync` is the blocking counterpart of `build()`: it spins
/// its own runtime, so it must be called from outside one. It had no in-repo
/// caller, which meant nothing checked that the runtime it creates actually
/// yields a usable database.
#[test]
fn test_builder_build_sync() -> Result<()> {
    let db: Uni = Uni::in_memory().build_sync()?;

    // `build_sync` returns the async facade, so drive it with an explicit
    // runtime — one it no longer owns, which is the point: the database must
    // outlive the runtime `build_sync` spun up internally.
    let rt = tokio::runtime::Runtime::new()?;
    rt.block_on(async {
        let session = db.session();
        let tx = session.tx().await?;
        tx.execute("CREATE (:Widget {sku: 'w-1'})").await?;
        tx.commit().await?;

        let result = session.query("MATCH (w:Widget) RETURN w.sku").await?;
        assert_eq!(result.len(), 1);
        anyhow::Ok(())
    })?;
    Ok(())
}

/// `UniSync::shutdown` used to end with `std::mem::forget(self)`, justified as
/// "prevents the Drop impl from also triggering shutdown". But `Drop` guards on
/// `if let Some(ref uni) = self.inner`, and `shutdown` has already done
/// `self.inner.take()` — so Drop was a no-op by then and the only thing `forget`
/// suppressed was `Runtime::drop`. Every explicit shutdown leaked a whole
/// multi-threaded runtime whose worker threads were never joined.
///
/// The implicit path (letting a `UniSync` fall out of scope) always dropped the
/// runtime, so this also made explicit shutdown inconsistent with it.
#[test]
fn shutdown_does_not_leak_runtime_threads() {
    fn threads() -> usize {
        std::fs::read_dir("/proc/self/task")
            .map(|d| d.count())
            .unwrap_or(0)
    }
    if threads() == 0 {
        return; // not Linux — nothing to measure
    }

    // Warm up so one-off lazy allocations aren't counted as growth.
    for _ in 0..2 {
        UniSync::in_memory().unwrap().shutdown().unwrap();
    }
    let before = threads();

    for _ in 0..8 {
        UniSync::in_memory().unwrap().shutdown().unwrap();
    }

    let after = threads();
    // Each leaked runtime strands a worker per core, so a leak shows up as
    // growth far beyond any incidental churn.
    assert!(
        after <= before + 4,
        "shutdown leaked runtime threads: {before} before, {after} after 8 cycles"
    );
}

/// Dropping a `UniSync` shuts the database down, as `shutdown()` does.
///
/// It used to send the shutdown signal and return, so whatever the background
/// tasks did in answer ran on a runtime about to go away. With the WAL off,
/// L0 is the only copy of a commit, and the auto-flush task's final flush is
/// what persists it: cut off, the commit was lost, though the caller never
/// saw an error.
#[test]
fn dropping_a_unisync_persists_its_commits_without_the_wal() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_string_lossy().to_string();
    let config = || uni_db::UniConfig {
        wal_enabled: false,
        ..Default::default()
    };

    let rt = tokio::runtime::Runtime::new().unwrap();
    let uni = rt
        .block_on(uni_db::Uni::open(&path).config(config()).build())
        .unwrap();
    let db = UniSync::new(uni).unwrap();
    let tx = db.session().tx().unwrap();
    tx.execute("CREATE (:Kept {n: 1})").unwrap();
    tx.commit().unwrap();
    drop(db);
    // The database was built on `rt`, so its background tasks run there; an
    // embedder's runtime ending right after the drop is the case at issue.
    drop(rt);

    let rt = tokio::runtime::Runtime::new().unwrap();
    let rows = rt.block_on(async {
        let db = uni_db::Uni::open(&path)
            .config(config())
            .build()
            .await
            .unwrap();
        let rows = db
            .session()
            .query("MATCH (k:Kept) RETURN k.n AS n")
            .await
            .unwrap();
        db.shutdown().await.unwrap();
        rows
    });
    assert_eq!(
        rows.len(),
        1,
        "the commit did not survive dropping the UniSync"
    );
}

/// The blocking builders forward every per-statement option, like the async
/// builders they wrap. `QueryBuilderSync` and `TxQueryBuilderSync` had no
/// `.cancellation_token()`, and `TxQueryBuilderSync`/`ExecuteBuilderSync` no
/// `.timeout()`/`.max_memory()`, so a blocking caller could not bound or
/// cancel a statement the async surface could.
#[test]
fn sync_builders_forward_every_statement_option() -> Result<()> {
    let db = UniSync::in_memory()?;
    db.schema().label("Node").apply()?;
    let session = db.session();
    let tx = session.tx()?;
    tx.execute("UNWIND range(1, 100) AS i CREATE (:Node)")?;
    tx.commit()?;
    let scan = "MATCH (n:Node) RETURN n";

    // A cancelled token from elsewhere, so the statement's own session scope
    // is not what cancels it.
    let token = tokio_util::sync::CancellationToken::new();
    token.cancel();

    let err = session
        .query_with(scan)
        .cancellation_token(token.clone())
        .fetch_all()
        .expect_err("session sync builder ignored a cancelled token");
    assert!(matches!(err, uni_db::UniError::Cancelled), "got {err:?}");

    let tx = session.tx()?;
    let err = tx
        .query_with(scan)
        .cancellation_token(token.clone())
        .fetch_all()
        .expect_err("tx sync builder ignored a cancelled token");
    assert!(matches!(err, uni_db::UniError::Cancelled), "got {err:?}");
    tx.rollback();

    let tx = session.tx()?;
    let err = tx
        .query_with(scan)
        .max_memory(100)
        .fetch_all()
        .expect_err("tx sync builder ignored max_memory");
    assert!(
        matches!(
            err,
            uni_db::UniError::MemoryLimitExceeded {
                limit_bytes: 100,
                ..
            }
        ),
        "got {err:?}"
    );
    tx.rollback();

    let tx = session.tx()?;
    let err = tx
        .query_with(scan)
        .timeout(std::time::Duration::from_nanos(1))
        .fetch_all()
        .expect_err("tx sync builder ignored timeout");
    assert!(
        matches!(err, uni_db::UniError::Timeout { .. }),
        "got {err:?}"
    );
    tx.rollback();

    let tx = session.tx()?;
    let err = tx
        .execute_with(scan)
        .max_memory(100)
        .run()
        .expect_err("execute sync builder ignored max_memory");
    assert!(
        matches!(
            err,
            uni_db::UniError::MemoryLimitExceeded {
                limit_bytes: 100,
                ..
            }
        ),
        "got {err:?}"
    );
    tx.rollback();

    // Controls: without the options every one of these succeeds.
    assert_eq!(session.query_with(scan).fetch_all()?.len(), 100);
    let tx = session.tx()?;
    assert_eq!(tx.query_with(scan).fetch_all()?.len(), 100);
    tx.rollback();
    Ok(())
}
