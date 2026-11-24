// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! An index whose build rewrites the label table (a keyed scalar index, a
//! JSON full-text index) is refused in a forked session, and the fork keeps
//! its rows.
//!
//! On a fork a table rewrite is a delete-all then an append on the branch.
//! The append carried the new derived column, which the branch's schema
//! lacked, so it failed after the delete had committed and left the fork's
//! table empty.
//!
//! Run with:
//!   cargo nextest run -p uni-db --test integration -E 'test(fork_table_rewrite)'

// Rust guideline compliant

use super::harness::run;
use anyhow::Result;
use uni_db::Uni;

async fn count(session: &uni_db::Session, label: &str) -> Result<usize> {
    Ok(session
        .query(&format!("MATCH (n:{label}) RETURN n"))
        .await?
        .len())
}

#[tokio::test]
async fn fork_table_rewrite_index_ddl_is_refused_and_keeps_the_rows() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    run(&db, "CREATE LABEL Event (id INT, ts DATETIME)").await?;
    run(&db, "CREATE LABEL Doc (id INT, doc JSON)").await?;
    run(
        &db,
        "CREATE (:Event {id: 1, ts: datetime('2026-01-01T12:00:00Z')}), \
                (:Doc {id: 1, doc: {title: 'Graph'}})",
    )
    .await?;
    db.flush().await?;
    let fork = db.session().fork("f").await?;
    // The fork's own writes, so its branch of each table holds rows.
    let tx = fork.tx().await?;
    tx.execute(
        "CREATE (:Event {id: 2, ts: datetime('2026-02-01T12:00:00Z')}), \
                (:Doc {id: 2, doc: {title: 'Vector'}})",
    )
    .await?;
    tx.commit().await?;
    fork.flush().await?;

    for ddl in [
        "CREATE INDEX idx_ts FOR (e:Event) ON (e.ts)",
        "CREATE JSON FULLTEXT INDEX dfts FOR (d:Doc) ON doc",
    ] {
        let tx = fork.tx().await?;
        let err = tx
            .execute(ddl)
            .await
            .expect_err("a table-rewriting index build is refused on a fork");
        assert!(err.to_string().contains("forked session"), "{ddl}: {err}");
        drop(tx);
    }
    assert_eq!(
        count(&fork, "Event").await?,
        2,
        "the fork lost its Event rows"
    );
    assert_eq!(count(&fork, "Doc").await?, 2, "the fork lost its Doc rows");

    // Declared on primary instead, the index works, and the fork still reads.
    run(&db, "CREATE INDEX idx_ts FOR (e:Event) ON (e.ts)").await?;
    assert_eq!(count(&fork, "Event").await?, 2);
    assert_eq!(count(&db.session(), "Event").await?, 1);
    Ok(())
}
