// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! A label check on a node that arrives as one value.
//!
//! `(a:L)` and `a:L` compiled against a flat `a._labels` column, which only a
//! scanned node has. A node that came out of `UNWIND` — or any list — is one
//! column, so every such check failed to plan with `No field named
//! "a._labels"`: a pattern on the unwound node, the same pattern inside
//! `EXISTS`, `WHERE a:L` and `RETURN a:L`. The check now reads the labels off
//! the value when the flat column is absent.
//!
//! Run with:
//!   cargo nextest run -p uni-db --test integration -E 'test(unwound_entity_label_check)'

// Rust guideline compliant

use anyhow::Result;
use uni_db::{DataType, Uni};

async fn open(schema: bool, flush: bool) -> Result<Uni> {
    let db = Uni::in_memory().build().await?;
    if schema {
        db.schema()
            .label("L")
            .property("id", DataType::Int)
            .label("M")
            .property("id", DataType::Int)
            .edge_type("R", &["L", "M"], &["L", "M"])
            .property("w", DataType::Int)
            .edge_type("S", &["L", "M"], &["L", "M"])
            .property("w", DataType::Int)
            .done()
            .apply()
            .await?;
    }
    let tx = db.session().tx().await?;
    tx.execute(
        "CREATE (a:L {id: 1}), (b:L {id: 2}), (c:M {id: 3}), (d:L:M {id: 4}), \
         (a)-[:R {w: 10}]->(b), (b)-[:R {w: 20}]->(c), (c)-[:R {w: 30}]->(d), \
         (a)-[:S {w: 7}]->(c)",
    )
    .await?;
    tx.commit().await?;
    if flush {
        db.flush().await?;
    }
    Ok(db)
}

async fn bag(db: &Uni, query: &str) -> Result<Vec<String>> {
    let result = db.session().query(query).await?;
    let mut rows: Vec<String> = result
        .rows()
        .iter()
        .map(|r| format!("{:?}", r.values()))
        .collect();
    rows.sort();
    Ok(rows)
}

const UNWOUND: &str = "MATCH (x) WITH collect(x) AS xs UNWIND xs AS a";

#[tokio::test]
async fn unwound_entity_label_check_matches_the_labelled_nodes() -> Result<()> {
    let cases: &[(&str, &[&str])] = &[
        (
            "MATCH (a:L) RETURN a.id",
            &["[Int(1)]", "[Int(2)]", "[Int(4)]"],
        ),
        ("MATCH (a:L:M) RETURN a.id", &["[Int(4)]"]),
        (
            "MATCH (a:L)-->(b) RETURN a.id, b.id",
            &["[Int(1), Int(2)]", "[Int(1), Int(3)]", "[Int(2), Int(3)]"],
        ),
        (
            "MATCH (a:M)-[:R]->(b:L) RETURN a.id, b.id",
            &["[Int(3), Int(4)]"],
        ),
        (
            "WITH a WHERE EXISTS { MATCH (a:L)-[:R]->(:M) } RETURN a.id",
            &["[Int(2)]"],
        ),
        ("WITH a WHERE a:M RETURN a.id", &["[Int(3)]", "[Int(4)]"]),
        (
            "RETURN a.id, a:L AS l, a:L:M AS lm",
            &[
                "[Int(1), Bool(true), Bool(false)]",
                "[Int(2), Bool(true), Bool(false)]",
                "[Int(3), Bool(false), Bool(false)]",
                "[Int(4), Bool(true), Bool(true)]",
            ],
        ),
        (
            "OPTIONAL MATCH (a:M)-[:R]->(b) RETURN a.id, b.id",
            &[
                "[Int(1), Null]",
                "[Int(2), Null]",
                "[Int(3), Int(4)]",
                "[Int(4), Null]",
            ],
        ),
    ];
    for schema in [true, false] {
        for flush in [false, true] {
            let db = open(schema, flush).await?;
            for (tail, want) in cases {
                let query = format!("{UNWOUND} {tail}");
                let got = bag(&db, &query)
                    .await
                    .map_err(|e| anyhow::anyhow!("schema={schema} flush={flush} {query}: {e}"))?;
                assert_eq!(got, *want, "schema={schema} flush={flush}: {query}");
            }
        }
    }
    Ok(())
}

/// The relationship form: an unwound relationship's type is its one label.
#[tokio::test]
async fn unwound_entity_label_check_on_a_relationship() -> Result<()> {
    for schema in [true, false] {
        let db = open(schema, false).await?;
        let got = bag(
            &db,
            "MATCH ()-[r]->() WITH collect(r) AS rs UNWIND rs AS r \
             WITH r WHERE r:S RETURN r.w",
        )
        .await?;
        assert_eq!(got, ["[Int(7)]"], "schema={schema}");
    }
    Ok(())
}

/// A null, or a value that is not an entity, follows the same rules as the
/// flat-column check: null for null, otherwise false.
#[tokio::test]
async fn unwound_entity_label_check_on_null_and_non_entities() -> Result<()> {
    let db = open(false, false).await?;
    let got = bag(
        &db,
        "MATCH (n:M {id: 3}) UNWIND [null, n, 3] AS a RETURN a:M AS c",
    )
    .await?;
    assert_eq!(got, ["[Bool(false)]", "[Bool(true)]", "[Null]"]);
    Ok(())
}
