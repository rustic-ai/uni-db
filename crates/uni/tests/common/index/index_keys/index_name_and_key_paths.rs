// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! An index name already taken by a different index is refused, not used to
//! replace it; and a key path's key is told apart from an expression by its
//! shape in the query, not by the characters in the key.
//!
//! Run with:
//!   cargo nextest run -p uni-db --test integration -E 'test(index_name_and_key_paths)'

// Rust guideline compliant

use super::harness::run;
use anyhow::Result;
use uni_db::Uni;

async fn index_on(db: &Uni, label: &str, name: &str) -> Result<Option<Vec<String>>> {
    Ok(db
        .get_label_info(label)
        .await?
        .and_then(|info| info.indexes.into_iter().find(|i| i.name == name))
        .map(|i| i.properties))
}

/// `CREATE INDEX i_id … ON (e.took)` while `i_id` indexes `id` replaced the
/// `id` index (and, for a DURATION, slipped past the refusal of a new one).
#[tokio::test]
async fn index_name_and_key_paths_a_taken_name_is_refused() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    run(&db, "CREATE LABEL E (id INT, took DURATION, n INT)").await?;
    run(&db, "CREATE (:E {id: 7, took: duration('PT1S'), n: 1})").await?;
    db.flush().await?;
    run(&db, "CREATE INDEX i_id FOR (e:E) ON (e.id)").await?;
    for ddl in [
        "CREATE INDEX i_id FOR (e:E) ON (e.took)",
        "CREATE INDEX i_id FOR (e:E) ON (e.n)",
    ] {
        let err = run(&db, ddl).await.expect_err(ddl);
        assert!(err.to_string().contains("i_id"), "{ddl}: {err}");
    }
    assert_eq!(
        index_on(&db, "E", "i_id").await?,
        Some(vec!["id".to_string()])
    );
    // Re-declaring the same index is still fine.
    run(&db, "CREATE INDEX IF NOT EXISTS i_id FOR (e:E) ON (e.id)").await?;
    Ok(())
}

/// A quoted key with a hyphen is a key path; an unparenthesized expression on
/// a key path is not keyed as a map entry named after the expression.
#[tokio::test]
async fn index_name_and_key_paths_keys_and_expressions() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    run(&db, "CREATE LABEL S (id INT, m MAP<STRING, INT>)").await?;
    run(&db, "CREATE (:S {id: 1, m: {`content-type`: 3, a: 4}})").await?;
    db.flush().await?;
    run(&db, "CREATE INDEX im FOR (s:S) ON (s.m.`content-type`)").await?;
    let key = db
        .storage()
        .schema_manager()
        .schema()
        .indexes
        .iter()
        .find_map(|i| match i {
            uni_db::core::schema::IndexDefinition::Scalar(c) if c.name == "im" => c.key.clone(),
            _ => None,
        });
    assert!(key.is_some(), "a quoted key with a hyphen is a key path");
    let rows = db
        .session()
        .query("MATCH (s:S) WHERE s.m.`content-type` = 3 RETURN s.id")
        .await?;
    assert_eq!(rows.len(), 1);

    // `s.m.a + 1` is an expression: whatever it becomes, it is not a key
    // path on a map entry named `a + 1`.
    let _ = run(&db, "CREATE INDEX ie FOR (s:S) ON (s.m.a + 1)").await;
    let keyed_entry = db
        .storage()
        .schema_manager()
        .schema()
        .indexes
        .iter()
        .any(|i| match i {
            uni_db::core::schema::IndexDefinition::Scalar(c) => c.name == "ie" && c.key.is_some(),
            _ => false,
        });
    assert!(!keyed_entry, "an expression was keyed as a map entry");
    Ok(())
}
