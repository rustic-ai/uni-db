// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Round 12 §A: edge SETs via the partial-Lance-writes path.
//!
//! Schema-defined edge properties get per-property columns in the
//! per-edge-type delta tables (`deltas_<type>_fwd` /
//! `deltas_<type>_bwd`). A SET that touches a subset of those
//! properties routes through `Writer::insert_edge_partial_full` and
//! the flush emits a `MergeInsertBuilder` source containing only
//! `eid`, `op`, `_version`, `_updated_at`, the touched schema
//! columns, and (when any non-schema/overflow key was touched) the
//! regenerated `overflow_json` blob. Lance's MVCC merges across
//! delta-table versions; untouched columns retain their previous-row
//! values.

// Rust guideline compliant

use anyhow::Result;
use uni_common::UniConfig;
use uni_db::{DataType, Uni, Value};

fn flag_on_config() -> UniConfig {
    UniConfig {
        #[expect(deprecated, reason = "exercises the deprecated no-op flag")]
        partial_lance_writes: true,
        ..UniConfig::default()
    }
}

/// A1 — Partial edge SET on one schema property preserves the other
/// schema property + overflow data.
#[tokio::test]
async fn a1_partial_edge_set_preserves_other_columns() -> Result<()> {
    let db = Uni::in_memory().config(flag_on_config()).build().await?;
    db.schema()
        .label("N")
        .property("id", DataType::String)
        .done()
        .edge_type("LINKS", &["N"], &["N"])
        .property_nullable("flag", DataType::Bool)
        .property_nullable("weight", DataType::Int64)
        .property_nullable("payload", DataType::String)
        .done()
        .apply()
        .await?;

    let tx = db.session().tx().await?;
    tx.execute(
        "CREATE (a:N {id: 'a'})-[:LINKS {flag: false, weight: 7, payload: 'orig'}]->(b:N {id: 'b'})",
    )
    .await?;
    tx.commit().await?;
    db.flush().await?;

    // Partial SET: touch only `flag`.
    let tx = db.session().tx().await?;
    tx.execute("MATCH (a:N {id: 'a'})-[r:LINKS]->(b:N {id: 'b'}) SET r.flag = true")
        .await?;
    tx.commit().await?;
    db.flush().await?;

    let r = db
        .session()
        .query("MATCH ()-[r:LINKS]->() RETURN r.flag AS f, r.weight AS w, r.payload AS p")
        .await?;
    let row = &r.rows()[0];
    let flag = row.value("f").unwrap();
    assert!(
        matches!(flag, Value::Bool(true)),
        "flag SET did not apply: {flag:?}"
    );
    assert_eq!(row.get::<i64>("w").unwrap(), 7, "weight not preserved");
    assert_eq!(
        row.get::<String>("p").unwrap(),
        "orig",
        "payload not preserved"
    );
    Ok(())
}

/// A2 — Flag-off bit-equivalence. Same workload as A1 with flag off
/// must produce identical observable behavior.
#[tokio::test]
async fn a2_partial_edge_set_flag_off_equivalence() -> Result<()> {
    let db = Uni::in_memory().build().await?; // flag off
    db.schema()
        .label("N")
        .property("id", DataType::String)
        .done()
        .edge_type("LINKS", &["N"], &["N"])
        .property_nullable("flag", DataType::Bool)
        .property_nullable("weight", DataType::Int64)
        .done()
        .apply()
        .await?;

    let tx = db.session().tx().await?;
    tx.execute("CREATE (a:N {id: 'a'})-[:LINKS {flag: false, weight: 7}]->(b:N {id: 'b'})")
        .await?;
    tx.commit().await?;
    db.flush().await?;

    let tx = db.session().tx().await?;
    tx.execute("MATCH (a:N {id: 'a'})-[r:LINKS]->(b:N {id: 'b'}) SET r.flag = true")
        .await?;
    tx.commit().await?;
    db.flush().await?;

    let r = db
        .session()
        .query("MATCH ()-[r:LINKS]->() RETURN r.flag AS f, r.weight AS w")
        .await?;
    let flag = r.rows()[0].value("f").unwrap();
    assert!(matches!(flag, Value::Bool(true)));
    assert_eq!(r.rows()[0].get::<i64>("w").unwrap(), 7);
    Ok(())
}

/// A3 — Two SETs on the same edge in one tx (different schema keys) —
/// dirty-key union, single MergeInsert source per direction (fwd, bwd).
#[tokio::test]
async fn a3_partial_edge_set_two_keys_one_tx() -> Result<()> {
    let db = Uni::in_memory().config(flag_on_config()).build().await?;
    db.schema()
        .label("N")
        .property("id", DataType::String)
        .done()
        .edge_type("LINKS", &["N"], &["N"])
        .property_nullable("a", DataType::Int64)
        .property_nullable("b", DataType::Int64)
        .property_nullable("c", DataType::Int64)
        .done()
        .apply()
        .await?;

    let tx = db.session().tx().await?;
    tx.execute("CREATE (x:N {id: 'x'})-[:LINKS {a: 0, b: 0, c: 0}]->(y:N {id: 'y'})")
        .await?;
    tx.commit().await?;
    db.flush().await?;

    let tx = db.session().tx().await?;
    tx.execute("MATCH ()-[r:LINKS]->() SET r.a = 11").await?;
    tx.execute("MATCH ()-[r:LINKS]->() SET r.b = 22").await?;
    tx.commit().await?;
    db.flush().await?;

    let r = db
        .session()
        .query("MATCH ()-[r:LINKS]->() RETURN r.a AS a, r.b AS b, r.c AS c")
        .await?;
    let row = &r.rows()[0];
    assert_eq!(row.get::<i64>("a").unwrap(), 11);
    assert_eq!(row.get::<i64>("b").unwrap(), 22);
    assert_eq!(
        row.get::<i64>("c").unwrap(),
        0,
        "c was unexpectedly modified"
    );
    Ok(())
}

/// A4 — Partial edge SET followed by DELETE: deletion supersedes the
/// partial state; the edge is gone post-flush.
#[tokio::test]
async fn a4_partial_edge_set_then_delete() -> Result<()> {
    let db = Uni::in_memory().config(flag_on_config()).build().await?;
    db.schema()
        .label("N")
        .property("id", DataType::String)
        .done()
        .edge_type("LINKS", &["N"], &["N"])
        .property_nullable("flag", DataType::Bool)
        .done()
        .apply()
        .await?;

    let tx = db.session().tx().await?;
    tx.execute("CREATE (a:N {id: 'a'})-[:LINKS {flag: false}]->(b:N {id: 'b'})")
        .await?;
    tx.commit().await?;
    db.flush().await?;

    let tx = db.session().tx().await?;
    tx.execute("MATCH ()-[r:LINKS]->() SET r.flag = true")
        .await?;
    tx.commit().await?;
    db.flush().await?;

    let tx = db.session().tx().await?;
    tx.execute("MATCH ()-[r:LINKS]->() DELETE r").await?;
    tx.commit().await?;
    db.flush().await?;

    let r = db
        .session()
        .query("MATCH ()-[r:LINKS]->() RETURN count(r) AS c")
        .await?;
    assert_eq!(r.rows()[0].get::<i64>("c").unwrap(), 0);
    Ok(())
}

/// A5 — Partial edge SET with non-schema (overflow) property touched
/// alongside a schema property. The flush regenerates `overflow_json`
/// with the new overflow blob while still emitting only the touched
/// schema column.
#[tokio::test]
async fn a5_partial_edge_set_with_overflow_property() -> Result<()> {
    let db = Uni::in_memory().config(flag_on_config()).build().await?;
    db.schema()
        .label("N")
        .property("id", DataType::String)
        .done()
        .edge_type("LINKS", &["N"], &["N"])
        .property_nullable("flag", DataType::Bool)
        .property_nullable("weight", DataType::Int64)
        .done()
        .apply()
        .await?;

    let tx = db.session().tx().await?;
    tx.execute(
        "CREATE (a:N {id: 'a'})-[:LINKS {flag: false, weight: 5, extra: 'orig'}]->(b:N {id: 'b'})",
    )
    .await?;
    tx.commit().await?;
    db.flush().await?;

    // Touch a schema prop (`flag`) AND a non-schema (`extra`) prop.
    let tx = db.session().tx().await?;
    tx.execute("MATCH ()-[r:LINKS]->() SET r.flag = true, r.extra = 'updated'")
        .await?;
    tx.commit().await?;
    db.flush().await?;

    let r = db
        .session()
        .query("MATCH ()-[r:LINKS]->() RETURN r.flag AS f, r.weight AS w, r.extra AS e")
        .await?;
    let row = &r.rows()[0];
    let flag = row.value("f").unwrap();
    assert!(matches!(flag, Value::Bool(true)));
    assert_eq!(
        row.get::<i64>("w").unwrap(),
        5,
        "weight should be untouched"
    );
    assert_eq!(
        row.get::<String>("e").unwrap(),
        "updated",
        "overflow `extra` not regenerated"
    );
    Ok(())
}
