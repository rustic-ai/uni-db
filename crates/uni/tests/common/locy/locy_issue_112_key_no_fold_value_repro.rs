// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Regression test for <https://github.com/rustic-ai/uni-db/issues/112>
//!
//! A Locy rule with **no FOLD** projected its `YIELD KEY` columns as
//! `Value::Null`; adding any FOLD (even `COUNT`) made the real values appear.
//! Root cause: `apply_post_fixpoint_chain` early-returned the raw fact batches
//! for the no-aggregate case, skipping the schema/key-column reconciliation the
//! FOLD branch performs, so the downstream projection selected the KEY column by
//! a stale name/position and landed on `Null`.
//!
//! These tests assert the actual typed VALUE of the KEY column — the assertion
//! that was missing from the suite and let the bug ship (the same blind spot as
//! issue #117 for the storage layer).

// Rust guideline compliant

use anyhow::Result;
use uni_db::{DataType, Uni, Value};

use crate::locy::value_assert::{assert_column_eq, assert_column_non_null};

async fn setup() -> Result<Uni> {
    let db = Uni::in_memory().build().await?;
    db.schema()
        .label("Item")
        .property("tag", DataType::String)
        .apply()
        .await?;
    let session = db.session();
    let tx = session.tx().await?;
    tx.execute("CREATE (:Item {tag: 'a'})").await?;
    tx.commit().await?;
    Ok(db)
}

#[tokio::test]
async fn key_value_column_no_fold_is_typed() -> Result<()> {
    // The exact issue repro: a single-node MATCH, one property KEY, NO FOLD.
    let db = setup().await?;
    let program = r#"
        CREATE RULE tags AS MATCH (i:Item) YIELD KEY i.tag AS tag
        QUERY tags RETURN tag
    "#;
    let result = db.session().locy(program).await?;

    // The KEY column must be the real String value on BOTH surfaces:
    // the derived relation and the QUERY projection.
    let empty = vec![];
    let derived = result.derived_rule("tags").await.unwrap().unwrap_or(&empty);
    assert_column_non_null(derived, "tag");
    assert_column_eq(derived, "tag", &Value::String("a".to_string()));

    let rows = result.rows().unwrap_or(&empty);
    assert_column_non_null(rows, "tag");
    assert_column_eq(rows, "tag", &Value::String("a".to_string()));
    Ok(())
}

#[tokio::test]
async fn key_value_column_multi_pattern_no_fold_is_typed() -> Result<()> {
    // The issue reports the same failure for a multi-pattern MATCH.
    let db = Uni::in_memory().build().await?;
    db.schema()
        .label("Item")
        .property("tag", DataType::String)
        .apply()
        .await?;
    db.schema()
        .label("Other")
        .property("k", DataType::Int64)
        .apply()
        .await?;
    {
        let session = db.session();
        let tx = session.tx().await?;
        tx.execute("CREATE (:Item {tag: 'a'})").await?;
        tx.execute("CREATE (:Other {k: 1})").await?;
        tx.commit().await?;
    }
    let program = r#"
        CREATE RULE pairs AS MATCH (i:Item),(o:Other) YIELD KEY i.tag AS tag, KEY o.k AS k
        QUERY pairs RETURN tag, k
    "#;
    let result = db.session().locy(program).await?;
    let empty = vec![];
    let rows = result.rows().unwrap_or(&empty);
    assert_column_eq(rows, "tag", &Value::String("a".to_string()));
    assert_column_eq(rows, "k", &Value::Int(1));
    Ok(())
}

#[tokio::test]
async fn control_whole_node_key_no_fold_still_works() -> Result<()> {
    // The whole-node KEY path (VID-typed) was unaffected — guard it stays so.
    let db = setup().await?;
    let program = r#"
        CREATE RULE items AS MATCH (i:Item) YIELD KEY i
        QUERY items RETURN i
    "#;
    let result = db.session().locy(program).await?;
    let empty = vec![];
    let rows = result.rows().unwrap_or(&empty);
    assert!(!rows.is_empty(), "expected one row, got none");
    for row in rows {
        assert!(
            matches!(row.get("i"), Some(Value::Node(_))),
            "whole-node KEY must be a Node, got {:?}",
            row.get("i")
        );
    }
    Ok(())
}
