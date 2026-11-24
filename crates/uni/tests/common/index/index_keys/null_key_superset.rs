// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! A flushed row whose key column is NULL while its property is set is still
//! found through the index.
//!
//! That row exists when a flush lands between a key backfill and the key
//! column's registration: the batch builder does not fill a column the catalog
//! does not name yet, and Lance stores NULL for it. Every key bound therefore
//! admits a NULL key, so a missing key costs a read, never a match. The test
//! reproduces the window by unregistering the key column around one flush.
//!
//! Run with:
//!   cargo nextest run -p uni-db --test integration -E 'test(null_key_superset)'

// Rust guideline compliant

use super::datetime_differential::{INDEX, SCHEMA};
use super::harness::{Case, Differential};
use anyhow::Result;

#[tokio::test]
async fn null_key_superset_a_row_flushed_without_its_key_is_found() -> Result<()> {
    let d = Differential::open(SCHEMA, INDEX, "Event", "ts").await?;
    d.write("CREATE (:Event {id: 1, ts: datetime('2026-01-01T12:00:00Z')})")
        .await?;
    d.flush().await?;

    let manager = d.indexed.storage().schema_manager_arc();
    let key_column = manager
        .schema()
        .properties
        .get("Event")
        .and_then(|p| p.keys().find(|k| k.starts_with("__key_")).cloned())
        .expect("the index registered a key column");
    // The window: the column exists in Lance, the catalog does not name it.
    manager.drop_property("Event", &key_column)?;
    d.write("CREATE (:Event {id: 2, ts: datetime('2026-01-01T12:00:00Z')})")
        .await?;
    d.flush().await?;
    manager.add_internal_property("Event", &key_column, uni_db::DataType::Int64, true)?;

    let cases = [
        Case::served(
            "MATCH (e:Event) WHERE e.ts = datetime('2026-01-01T12:00:00Z') RETURN e.id AS id",
        ),
        Case::served(
            "MATCH (e:Event) WHERE e.ts >= datetime('2026-01-01T00:00:00Z') RETURN e.id AS id",
        ),
    ];
    // Both answers must match the unindexed database. Index use is asserted
    // for the equality only: the window leaves a fragment flushed while the
    // catalog did not name the key column, and whether Lance serves a range
    // over that layout from the index is not what this test is about (the
    // differential tests assert it for ranges on ordinary layouts).
    d.check("row flushed without its key", &cases[..1], true)
        .await?;
    d.check("row flushed without its key", &cases[1..], false)
        .await
}
