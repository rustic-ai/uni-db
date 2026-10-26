// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Component C2 — transaction-level L1 (Lance) pinning.
//!
//! A read-write transaction pins the L0 tier via its `SnapshotView` (C1),
//! and C2 pins the L1 tier: scans route through a `StorageManager` clone
//! whose reads filter to `_version <= started_at_version` (minus bulk loads
//! unpublished at begin), so an L0→L1 flush completing mid-transaction
//! cannot leak post-snapshot ROWS into the transaction's view (previously:
//! "flush-boundary read skew").
//!
//! Scope:
//! - **Vertex scans** filter by version. L1 vertex rows are single-versioned
//!   (`MergeInsert` replaces in place), so post-snapshot INSERTS are hidden;
//!   a row updated-and-flushed mid-transaction is *excluded* rather than
//!   shown at its old value (the same boundary time-travel has). L0-resident
//!   rows are immune (L0 shadows L1).
//! - **Property reads** go through the shared PropertyManager on LIVE
//!   storage, so read-your-writes holds (a transaction's own uncommitted
//!   edge/vertex properties live in tx_l0, consulted before L1 — e.g. MERGE's
//!   edge-property match). Its L1 tiers are bounded by the pin, carried in
//!   `QueryContext::l1_visibility`, and it bypasses its shared LRU cache
//!   while pinned.
//! - **Edges** are version-pinned at read time: the pinned clone shares the
//!   live `AdjacencyManager` (its commit-replay overlay is the only source of
//!   unflushed edges) and `StorageManager::neighbors` filters it by the
//!   view's `RowVisibility`. The shared CSR is still warmed unfiltered.

use anyhow::Result;
use uni_db::{DataType, Uni, UniConfig};

async fn items_db() -> Result<Uni> {
    let db = Uni::in_memory().build().await?;
    db.schema()
        .label("Item")
        .property("name", DataType::String)
        .property("value", DataType::Int)
        .done()
        .apply()
        .await?;
    let s = db.session();
    let tx = s.tx().await?;
    tx.execute("CREATE (:Item {name: 'a', value: 1})").await?;
    tx.commit().await?;
    // Push the seed row to L1 so the transaction under test reads it from
    // the Lance tier, not from a pinned L0 generation.
    db.flush().await?;
    Ok(db)
}

/// THE C2 repro: rows inserted and flushed to L1 mid-transaction must not
/// appear in the transaction's scans. Pre-fix, the L1 scan saw the latest
/// Lance data and the new row leaked in.
#[tokio::test]
async fn mid_tx_flush_does_not_leak_new_rows() -> Result<()> {
    let db = items_db().await?;

    let s = db.session();
    let tx = s.tx().await?;
    let before = tx.query("MATCH (i:Item) RETURN count(*) AS n").await?;
    assert_eq!(before.rows()[0].get::<i64>("n")?, 1);

    // A concurrent commit inserts a new row, and a flush pushes it to L1
    // while the transaction is still open.
    {
        let s2 = db.session();
        let tx2 = s2.tx().await?;
        tx2.execute("CREATE (:Item {name: 'b', value: 2})").await?;
        tx2.commit().await?;
    }
    db.flush().await?;

    // The open transaction must still see exactly its snapshot.
    let after = tx.query("MATCH (i:Item) RETURN count(*) AS n").await?;
    assert_eq!(
        after.rows()[0].get::<i64>("n")?,
        1,
        "post-snapshot row flushed to L1 leaked into an open transaction"
    );
    // Property reads agree with the scan tier.
    let val = tx
        .query("MATCH (i:Item {name: 'a'}) RETURN i.value AS v")
        .await?;
    assert_eq!(val.rows()[0].get::<i64>("v")?, 1);

    tx.commit().await?;
    // After commit, a fresh read sees both rows.
    let fresh = s.query("MATCH (i:Item) RETURN count(*) AS n").await?;
    assert_eq!(fresh.rows()[0].get::<i64>("n")?, 2);
    Ok(())
}

/// Same scenario through `tx.locy()` — Locy clause bodies read through the
/// pinned snapshot too (C1+C2 threaded into the Locy executor).
#[tokio::test]
async fn mid_tx_flush_does_not_leak_into_locy() -> Result<()> {
    let db = items_db().await?;

    let s = db.session();
    let tx = s.tx().await?;
    let before = tx
        .locy("CREATE RULE items AS MATCH (i:Item) YIELD KEY i")
        .await?;
    assert_eq!(
        before
            .derived_rules()
            .await
            .unwrap()
            .get("items")
            .map(Vec::len),
        Some(1)
    );

    {
        let s2 = db.session();
        let tx2 = s2.tx().await?;
        tx2.execute("CREATE (:Item {name: 'b', value: 2})").await?;
        tx2.commit().await?;
    }
    db.flush().await?;

    let after = tx
        .locy("CREATE RULE items AS MATCH (i:Item) YIELD KEY i")
        .await?;
    assert_eq!(
        after
            .derived_rules()
            .await
            .unwrap()
            .get("items")
            .map(Vec::len),
        Some(1),
        "post-snapshot L1 row leaked into a Locy evaluation inside the tx"
    );
    tx.rollback();
    Ok(())
}

/// `FOR UPDATE` on a fresh transaction re-pins to the latest committed
/// state — the rebuilt L1 pin must see rows committed+flushed before the
/// lock acquisition.
#[tokio::test]
async fn for_update_repin_sees_latest_l1() -> Result<()> {
    let db = items_db().await?;

    // Begin the tx FIRST, while only the seed row exists.
    let s = db.session();
    let tx = s.tx().await?;

    // A second row commits and reaches L1 after tx begin.
    {
        let s2 = db.session();
        let tx2 = s2.tx().await?;
        tx2.execute("CREATE (:Item {name: 'b', value: 2})").await?;
        tx2.commit().await?;
    }
    db.flush().await?;

    // The fresh tx's first read uses FOR UPDATE → snapshot re-pins to NOW,
    // so the locked read must see the post-begin row.
    let r = tx
        .query("MATCH (i:Item {name: 'b'}) FOR UPDATE RETURN i.value AS v")
        .await?;
    assert_eq!(
        r.rows().len(),
        1,
        "FOR UPDATE re-pin must refresh the L1 pin to the new baseline"
    );
    tx.rollback();
    Ok(())
}

/// With SSI disabled nothing is pinned: an open transaction reads live data
/// (the documented 1.x legacy behavior).
#[tokio::test]
async fn ssi_disabled_keeps_live_reads() -> Result<()> {
    let config = UniConfig {
        ssi_enabled: false,
        ..Default::default()
    };
    let db = Uni::in_memory().config(config).build().await?;
    db.schema()
        .label("Item")
        .property("name", DataType::String)
        .done()
        .apply()
        .await?;
    let s = db.session();
    let tx0 = s.tx().await?;
    tx0.execute("CREATE (:Item {name: 'a'})").await?;
    tx0.commit().await?;
    db.flush().await?;

    let tx = s.tx().await?;
    {
        let s2 = db.session();
        let tx2 = s2.tx().await?;
        tx2.execute("CREATE (:Item {name: 'b'})").await?;
        tx2.commit().await?;
    }
    db.flush().await?;

    let n = tx.query("MATCH (i:Item) RETURN count(*) AS n").await?;
    assert_eq!(
        n.rows()[0].get::<i64>("n")?,
        2,
        "ssi_enabled=false must keep live (unpinned) reads"
    );
    tx.rollback();
    Ok(())
}

/// Read-your-writes within a transaction must survive C2 pinning: a MERGE
/// whose match phase reads an edge property created earlier in the SAME
/// statement must see that edge (else it double-creates). Regression for a
/// C2 pinning bug where the per-transaction PropertyManager version-filtered
/// edge-property reads and hid the in-transaction edge — caught only by the
/// sidecar-schema openCypher TCK (MERGE5[21]), so it lives here too to guard
/// PR runs.
#[tokio::test]
async fn merge_sees_in_transaction_edge_under_pin() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    db.schema()
        .label("N")
        .property("name", DataType::String)
        .edge_type("R", &["N"], &["N"])
        .property("tag", DataType::String)
        .done()
        .apply()
        .await?;

    let s = db.session();
    let tx = s.tx().await?;
    tx.execute("CREATE (a:N {name: 'a'}), (b:N {name: 'b'})")
        .await?;
    tx.commit().await?;
    db.flush().await?;

    // One statement, two driving rows would re-create; but even two explicit
    // MERGEs in one transaction must dedup: the second must see the first's
    // edge through the live property read, not a version-filtered miss.
    let tx = s.tx().await?;
    tx.execute("MATCH (a:N {name:'a'}), (b:N {name:'b'}) MERGE (a)-[:R {tag:'x'}]->(b)")
        .await?;
    tx.execute("MATCH (a:N {name:'a'}), (b:N {name:'b'}) MERGE (a)-[:R {tag:'x'}]->(b)")
        .await?;
    tx.commit().await?;

    let n = s.query("MATCH ()-[r:R]->() RETURN count(r) AS n").await?;
    assert_eq!(
        n.rows()[0].get::<i64>("n")?,
        1,
        "second MERGE must match the first's in-transaction edge, not duplicate"
    );
    Ok(())
}
