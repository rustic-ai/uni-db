//! Issue #116 verification: does the composite-key uniqueness constraint hold
//! across a flush boundary?
//!
//! #116 suspects a fail-open: the persisted-side composite-key check in
//! `writer.rs` reads flushed rows through `VertexDataset::open_raw()`, which
//! (per #115) opens the wrong path (`vertices_<label>` without `.lance`) and
//! returns Err/0 rows — so the `count > 0` branch never fires for flushed
//! rows. The question this test answers empirically:
//!
//!   Insert a row with composite key K, flush, then in a NEW transaction insert
//!   another row with key K. Is the duplicate rejected?
//!
//! - Rejected  -> the L0/commit-time guard covers it; the `open_raw()` block is
//!                dead/defensive code (remove or route via `backend.scan`).
//! - Accepted  -> real correctness fail-open across the flush boundary (#116).

use anyhow::Result;
use uni_db::Uni;

/// Declares a `User(org, username)` label with a composite UNIQUE constraint on
/// `(org, username)` via the DDL procedure (the high-level SchemaBuilder does
/// not expose composite constraints yet).
async fn make_db_with_composite_unique() -> Result<Uni> {
    let db = Uni::temporary().build().await?;
    db.session()
        .run(
            r#"
        CALL uni.schema.createLabel('User', {
            "properties": {
                "org": { "type": "STRING" },
                "username": { "type": "STRING" }
            },
            "constraints": [
                { "type": "UNIQUE", "properties": ["org", "username"] }
            ]
        })
    "#,
        )
        .await?;
    Ok(db)
}

/// Baseline (control): a duplicate composite key inserted in the SAME L0
/// generation (no flush) must be rejected. This proves the constraint and the
/// test harness work before we probe the flush boundary.
#[tokio::test]
async fn issue116_duplicate_in_l0_is_rejected() -> Result<()> {
    let db = make_db_with_composite_unique().await?;

    let tx = db.session().tx().await?;
    tx.execute("CREATE (:User {org: 'Acme', username: 'alice'})")
        .await?;
    tx.commit().await?;

    // No flush: the key is still resident in L0 / the in-memory constraint index.
    let tx = db.session().tx().await?;
    let dup = tx
        .execute("CREATE (:User {org: 'Acme', username: 'alice'})")
        .await;
    let dup = match dup {
        Ok(_) => tx.commit().await,
        Err(e) => Err(e),
    };

    assert!(
        dup.is_err(),
        "control failed: an in-L0 duplicate composite key was NOT rejected"
    );

    let count = db
        .session()
        .query("MATCH (u:User) RETURN count(u) AS c")
        .await?;
    assert_eq!(
        count.rows()[0].get::<i64>("c")?,
        1,
        "in-L0 duplicate must not have been persisted"
    );
    Ok(())
}

/// THE #116 QUESTION: a duplicate composite key inserted in a NEW transaction
/// AFTER the original row has been flushed out of L0 into Lance storage.
///
/// If the only surviving guard is the persisted-side `open_raw()` check, and
/// that path is broken (#115), this duplicate slips through (fail-open).
#[tokio::test]
async fn issue116_duplicate_across_flush_is_rejected() -> Result<()> {
    let db = make_db_with_composite_unique().await?;

    // 1. Insert the original row and commit.
    let tx = db.session().tx().await?;
    tx.execute("CREATE (:User {org: 'Acme', username: 'alice'})")
        .await?;
    tx.commit().await?;

    // 2. Flush: drains L0 into the Lance-managed vertices_User.lance table, so
    //    the in-memory constraint index no longer holds the key.
    db.flush().await?;

    // 3. New transaction, same composite key. Plain CREATE (not MERGE, which
    //    would dedup) — this is the duplicate the constraint must catch.
    let tx = db.session().tx().await?;
    let dup = tx
        .execute("CREATE (:User {org: 'Acme', username: 'alice'})")
        .await;
    let dup = match dup {
        Ok(_) => tx.commit().await,
        Err(e) => Err(e),
    };

    // 4. Verdict.
    let count_after = db
        .session()
        .query("MATCH (u:User) RETURN count(u) AS c")
        .await?
        .rows()[0]
        .get::<i64>("c")?;

    assert!(
        dup.is_err(),
        "FAIL-OPEN (#116 confirmed): a duplicate composite key inserted after \
         flush was ACCEPTED (post-insert User count = {count_after}). The \
         persisted-side check did not enforce uniqueness across the flush \
         boundary."
    );

    assert_eq!(
        count_after, 1,
        "duplicate composite key was persisted across the flush boundary"
    );
    Ok(())
}
