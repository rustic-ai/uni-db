// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! `Transaction::bulk_insert_vertices_labeled` — creating multi-label vertices.
//!
//! The storage layer has always stored a vertex's labels as a set, but the only
//! public bulk path took a single `&str`, so a caller could not create what the
//! engine could store. Data with genuinely multi-labelled nodes — an LDBC
//! `Place` that is also a `City` — had to drop a label at load time, and a later
//! `MATCH (p:Place)` then returned nothing at all, with no error.

use std::collections::HashMap;

use anyhow::Result;
use uni_db::{DataType, Uni, Value};

async fn db_with_place_labels() -> Result<Uni> {
    let db = Uni::in_memory().build().await?;
    for label in ["Place", "City"] {
        db.schema()
            .label(label)
            .property("name", DataType::String)
            .apply()
            .await?;
    }
    Ok(db)
}

fn props(name: &str) -> HashMap<String, Value> {
    let mut p = HashMap::new();
    p.insert("name".to_string(), Value::String(name.to_string()));
    p
}

/// A vertex inserted with two labels must be matchable by **either** of them,
/// and be the same vertex both times.
#[tokio::test]
async fn multi_label_vertex_matches_on_every_label() -> Result<()> {
    let db = db_with_place_labels().await?;

    let tx = db.session().tx().await?;
    let vids = tx
        .bulk_insert_vertices_labeled(&["Place", "City"], vec![props("Antwerp")])
        .await?;
    tx.commit().await?;
    assert_eq!(vids.len(), 1);

    for label in ["Place", "City"] {
        let rows = db
            .session()
            .query(&format!("MATCH (p:{label}) RETURN p.name AS name"))
            .await?;
        assert_eq!(
            rows.rows().len(),
            1,
            "MATCH (:{label}) should find the multi-label vertex"
        );
        assert_eq!(rows.rows()[0].get::<String>("name")?, "Antwerp");
    }

    Ok(())
}

/// The single-label wrapper must keep behaving exactly as before: one label in,
/// one label matchable, and the other label matches nothing.
#[tokio::test]
async fn single_label_insert_is_unchanged() -> Result<()> {
    let db = db_with_place_labels().await?;

    let tx = db.session().tx().await?;
    tx.bulk_insert_vertices("City", vec![props("Ghent")])
        .await?;
    tx.commit().await?;

    let city = db
        .session()
        .query("MATCH (p:City) RETURN count(p) AS c")
        .await?;
    assert_eq!(city.rows()[0].get::<i64>("c")?, 1);

    let place = db
        .session()
        .query("MATCH (p:Place) RETURN count(p) AS c")
        .await?;
    assert_eq!(
        place.rows()[0].get::<i64>("c")?,
        0,
        "a single-label insert must not gain a second label"
    );

    Ok(())
}

/// An empty label set is rejected rather than stored: a vertex with no label
/// cannot be matched by any pattern, so accepting it would write a row that is
/// invisible to every query.
#[tokio::test]
async fn empty_label_set_is_rejected() -> Result<()> {
    let db = db_with_place_labels().await?;
    let tx = db.session().tx().await?;
    let err = tx
        .bulk_insert_vertices_labeled(&[], vec![props("nowhere")])
        .await
        .expect_err("an empty label set must be rejected");
    assert!(
        err.to_string().contains("at least one label"),
        "error should say why, got: {err}"
    );
    Ok(())
}

/// An undeclared label fails on the label that is actually missing, not the
/// first one in the list.
#[tokio::test]
async fn undeclared_label_is_named_in_the_error() -> Result<()> {
    let db = db_with_place_labels().await?;
    let tx = db.session().tx().await?;
    let err = tx
        .bulk_insert_vertices_labeled(&["Place", "Nonexistent"], vec![props("x")])
        .await
        .expect_err("an undeclared label must be rejected");
    assert!(
        err.to_string().contains("Nonexistent"),
        "error should name the missing label, got: {err}"
    );
    Ok(())
}
