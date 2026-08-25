// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! #182: `flush()` raised an internal error after deleting a vertex that had
//! never been flushed.
//!
//! ```text
//! UniInternalError: merge_insert target table 'vertices' does not exist
//! ```
//!
//! The trigger is flush state, not the transaction boundary: same transaction
//! or two, schemaless or with the label declared, `CREATE` or `MERGE` — all
//! reproduce. A flush *between* the create and the delete does not, and neither
//! does deleting a vertex that never existed.
//!
//! Mechanism: `L0Buffer::delete_vertex` drops the vid from `vertex_properties`
//! and records a tombstone, so a create-and-delete inside one flush window
//! leaves the window with a tombstone and no live rows. The flush then skips
//! the full-row write that would have created the table, while the tombstone
//! MergeInsert runs anyway and finds no target.
//!
//! Fix sites, all three of which had to move together — fixing only the first
//! relocates the error rather than removing it:
//!   * `manager::merge_insert_batch_tolerating_missing_table` (the main table)
//!   * `VertexDataset::merge_insert_tombstone_batch` (the per-label table,
//!     which the main-table failure was masking)
//!   * `ensure_default_indexes` on both, which runs when tombstones alone are
//!     non-empty and whose `list_indexes` opens the dataset
//!
//! It matters more than a loud error usually would because `flush()` is also a
//! background operation: `auto_flush_interval` defaults to 5s, so an
//! application that creates and deletes within that window gets this from a
//! timer rather than from anything it called.

// Rust guideline compliant

use uni_db::{DataType, Result, Uni};

/// Schemaless: exercises the main `vertices` table.
#[tokio::test]
async fn delete_before_first_flush_does_not_error() -> Result<()> {
    let db = Uni::in_memory().build().await?;

    let tx = db.session().tx().await?;
    tx.execute("CREATE (n {uid: 1, val: 0})").await?;
    tx.commit().await?;

    let tx = db.session().tx().await?;
    tx.execute("MATCH (n {uid: 1}) DETACH DELETE n").await?;
    tx.commit().await?;

    db.flush().await?;

    db.shutdown().await?;
    Ok(())
}

/// With the label declared, so the per-label `vertices_Alpha` table is on the
/// path too.
///
/// This is the case that stays red if only the main table is fixed: the
/// main-table failure fires first and masks it.
#[tokio::test]
async fn delete_before_first_flush_with_declared_label_does_not_error() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    db.schema()
        .label("Alpha")
        .property("uid", DataType::Int)
        .apply()
        .await?;

    let tx = db.session().tx().await?;
    tx.execute("CREATE (n:Alpha {uid: 1})").await?;
    tx.commit().await?;

    let tx = db.session().tx().await?;
    tx.execute("MATCH (n:Alpha {uid: 1}) DETACH DELETE n")
        .await?;
    tx.commit().await?;

    db.flush().await?;

    db.shutdown().await?;
    Ok(())
}

/// The skipped tombstone must leave the store usable.
///
/// Proves the no-op did not damage allocator or index state: after the flush
/// the count is zero, and a fresh vertex still materialises the table and reads
/// back normally.
#[tokio::test]
async fn delete_before_first_flush_leaves_the_store_usable() -> Result<()> {
    let db = Uni::in_memory().build().await?;

    let tx = db.session().tx().await?;
    tx.execute("CREATE (n {uid: 1})").await?;
    tx.commit().await?;
    let tx = db.session().tx().await?;
    tx.execute("MATCH (n {uid: 1}) DETACH DELETE n").await?;
    tx.commit().await?;
    db.flush().await?;

    let count: i64 = db
        .session()
        .query("MATCH (n) WHERE n.uid IS NOT NULL RETURN count(n) AS c")
        .await?
        .rows()
        .first()
        .and_then(|r| r.get::<i64>("c").ok())
        .unwrap_or(-1);
    assert_eq!(count, 0, "the deleted vertex must not survive the flush");

    let tx = db.session().tx().await?;
    tx.execute("CREATE (n {uid: 2})").await?;
    tx.commit().await?;
    db.flush().await?;

    let after: Vec<i64> = db
        .session()
        .query("MATCH (n) WHERE n.uid IS NOT NULL RETURN n.uid AS uid")
        .await?
        .rows()
        .iter()
        .filter_map(|r| r.get::<i64>("uid").ok())
        .collect();
    assert_eq!(after, vec![2], "the store must still accept writes");

    db.shutdown().await?;
    Ok(())
}
