// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Round 12 §B: DELETE via Lance `MergeInsert`. Tombstones flush with
//! only `_vid`, `_deleted=true`, `_version`, `_updated_at` — skipping
//! the wide-row Append payload that the previous path emitted.
//! Behavior unchanged externally; the optimization is unconditional
//! (not gated on `partial_lance_writes`).

// Rust guideline compliant

use anyhow::Result;
use uni_db::{DataType, Uni, Value};

/// B1 — Basic round-trip: CREATE 10 vertices, DELETE 5, count = 5.
#[tokio::test]
async fn b1_delete_via_merge_insert_round_trip() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    db.schema()
        .label("E")
        .property("id", DataType::String)
        .done()
        .apply()
        .await?;

    let tx = db.session().tx().await?;
    for i in 0..10 {
        tx.execute_with("CREATE (:E {id: $id})")
            .param("id", Value::String(format!("e{i}")))
            .run()
            .await?;
    }
    tx.commit().await?;
    db.flush().await?;

    let tx = db.session().tx().await?;
    for i in 0..5 {
        tx.execute_with("MATCH (n:E {id: $id}) DELETE n")
            .param("id", Value::String(format!("e{i}")))
            .run()
            .await?;
    }
    tx.commit().await?;
    db.flush().await?;

    let r = db
        .session()
        .query("MATCH (n:E) RETURN count(n) AS c")
        .await?;
    assert_eq!(r.rows()[0].get::<i64>("c").unwrap(), 5);

    let r2 = db
        .session()
        .query("MATCH (n:E) RETURN n.id AS id ORDER BY id")
        .await?;
    let ids: Vec<String> = r2
        .rows()
        .iter()
        .map(|row| row.get::<String>("id").unwrap())
        .collect();
    assert_eq!(ids, vec!["e5", "e6", "e7", "e8", "e9"]);
    Ok(())
}

/// B2 — DELETE then CREATE with the same external id surfaces the new
/// vertex (post-tombstone resurrection / new VID).
#[tokio::test]
async fn b2_delete_then_create_same_id() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    db.schema()
        .label("E")
        .property("id", DataType::String)
        .property_nullable("x", DataType::Int64)
        .done()
        .apply()
        .await?;

    let tx = db.session().tx().await?;
    tx.execute("CREATE (:E {id: 'k', x: 1})").await?;
    tx.commit().await?;
    db.flush().await?;

    let tx = db.session().tx().await?;
    tx.execute("MATCH (n:E {id: 'k'}) DELETE n").await?;
    tx.commit().await?;
    db.flush().await?;

    let tx = db.session().tx().await?;
    tx.execute("CREATE (:E {id: 'k', x: 99})").await?;
    tx.commit().await?;
    db.flush().await?;

    let r = db
        .session()
        .query("MATCH (n:E {id: 'k'}) RETURN n.x AS x")
        .await?;
    assert_eq!(r.rows().len(), 1);
    assert_eq!(r.rows()[0].get::<i64>("x").unwrap(), 99);
    Ok(())
}

/// B3 — Partial SET then DELETE in a later tx: DELETE supersedes
/// partial state.
#[tokio::test]
async fn b3_delete_after_partial_set() -> Result<()> {
    use uni_common::UniConfig;
    let cfg = UniConfig {
        #[expect(deprecated, reason = "exercises the deprecated no-op flag")]
        partial_lance_writes: true,
        ..UniConfig::default()
    };
    let db = Uni::in_memory().config(cfg).build().await?;
    db.schema()
        .label("E")
        .property("id", DataType::String)
        .property_nullable("x", DataType::Int64)
        .done()
        .apply()
        .await?;

    let tx = db.session().tx().await?;
    tx.execute("CREATE (:E {id: 'k', x: 0})").await?;
    tx.commit().await?;
    db.flush().await?;

    let tx = db.session().tx().await?;
    tx.execute("MATCH (n:E {id: 'k'}) SET n.x = 5").await?;
    tx.commit().await?;
    db.flush().await?;

    let tx = db.session().tx().await?;
    tx.execute("MATCH (n:E {id: 'k'}) DELETE n").await?;
    tx.commit().await?;
    db.flush().await?;

    let r = db
        .session()
        .query("MATCH (n:E) RETURN count(n) AS c")
        .await?;
    assert_eq!(r.rows()[0].get::<i64>("c").unwrap(), 0);
    Ok(())
}

/// B4 — Vertex with a HASH index: DELETE removes it from the index
/// lookup post-flush.
#[tokio::test]
async fn b4_delete_with_index_lookup() -> Result<()> {
    use uni_db::api::schema::{IndexType, ScalarType};
    let db = Uni::in_memory().build().await?;
    db.schema()
        .label("U")
        .property("id", DataType::String)
        .property_nullable("email", DataType::String)
        .index("email", IndexType::Scalar(ScalarType::Hash))
        .done()
        .apply()
        .await?;

    let tx = db.session().tx().await?;
    tx.execute("CREATE (:U {id: 'u1', email: 'a@example.com'})")
        .await?;
    tx.execute("CREATE (:U {id: 'u2', email: 'b@example.com'})")
        .await?;
    tx.commit().await?;
    db.flush().await?;

    let tx = db.session().tx().await?;
    tx.execute("MATCH (n:U {id: 'u1'}) DELETE n").await?;
    tx.commit().await?;
    db.flush().await?;

    let r = db
        .session()
        .query_with("MATCH (n:U) WHERE n.email = $e RETURN n.id AS id")
        .param("e", Value::String("a@example.com".to_string()))
        .fetch_all()
        .await?;
    assert_eq!(r.rows().len(), 0, "deleted vertex still in index");

    let r2 = db
        .session()
        .query_with("MATCH (n:U) WHERE n.email = $e RETURN n.id AS id")
        .param("e", Value::String("b@example.com".to_string()))
        .fetch_all()
        .await?;
    assert_eq!(r2.rows().len(), 1);
    assert_eq!(r2.rows()[0].get::<String>("id").unwrap(), "u2");
    Ok(())
}

/// B5 — DELETE preserves un-deleted rows on the same label. CREATE 50
/// with non-trivial property payload; DELETE 25; verify the remaining
/// 25 retain their original values byte-equal (Lance MergeInsert must
/// not disturb non-tombstoned rows).
#[tokio::test]
async fn b5_delete_preserves_other_rows() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    db.schema()
        .label("Item")
        .property("id", DataType::String)
        .property_nullable("payload", DataType::String)
        .property_nullable("rank", DataType::Int64)
        .done()
        .apply()
        .await?;

    let tx = db.session().tx().await?;
    for i in 0..50 {
        tx.execute_with("CREATE (:Item {id: $id, payload: $p, rank: $r})")
            .param("id", Value::String(format!("i{i:02}")))
            .param("p", Value::String(format!("payload-{i:02}")))
            .param("r", Value::Int(i as i64 * 7))
            .run()
            .await?;
    }
    tx.commit().await?;
    db.flush().await?;

    // Delete every other row (even-indexed).
    let tx = db.session().tx().await?;
    for i in (0..50).step_by(2) {
        tx.execute_with("MATCH (n:Item {id: $id}) DELETE n")
            .param("id", Value::String(format!("i{i:02}")))
            .run()
            .await?;
    }
    tx.commit().await?;
    db.flush().await?;

    let r = db
        .session()
        .query("MATCH (n:Item) RETURN n.id AS id, n.payload AS p, n.rank AS r ORDER BY id")
        .await?;
    let rows = r.rows();
    assert_eq!(rows.len(), 25);
    for (out_idx, row) in rows.iter().enumerate() {
        let i = out_idx * 2 + 1; // odd indices survived
        assert_eq!(row.get::<String>("id").unwrap(), format!("i{i:02}"));
        assert_eq!(row.get::<String>("p").unwrap(), format!("payload-{i:02}"));
        assert_eq!(row.get::<i64>("r").unwrap(), i as i64 * 7);
    }
    Ok(())
}
