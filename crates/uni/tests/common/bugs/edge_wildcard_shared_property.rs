// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Two relationship types that declare the same property.
//!
//! When a traversal needs every property of `r` and `r` may be either of two
//! types that both declare `w`, the planner listed `w` once per type, so the
//! traversal emitted two `r.w` columns. A reference DataFusion resolves by
//! name — `r['w']` inside `coalesce` — then failed with `Ambiguous reference
//! to unqualified field "r.w"`. Each property is now listed once.
//!
//! Run with:
//!   cargo nextest run -p uni-db --test integration -E 'test(edge_wildcard_shared_property)'

// Rust guideline compliant

use anyhow::Result;
use uni_db::{DataType, Uni};

async fn open(flush: bool) -> Result<Uni> {
    let db = Uni::in_memory().build().await?;
    db.schema()
        .label("N")
        .property("id", DataType::Int)
        .edge_type("R", &["N"], &["N"])
        .property_nullable("w", DataType::Int)
        .edge_type("S", &["N"], &["N"])
        .property_nullable("w", DataType::Int)
        .property_nullable("only_s", DataType::String)
        .done()
        .apply()
        .await?;
    let tx = db.session().tx().await?;
    tx.execute(
        "CREATE (a:N {id: 1}), (b:N {id: 2}), (c:N {id: 3}), \
         (a)-[:R {w: 10}]->(b), (b)-[:R]->(c), (a)-[:S {w: 7, only_s: 'x'}]->(c)",
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

#[tokio::test]
async fn edge_wildcard_shared_property_resolves_by_name() -> Result<()> {
    let cases: &[(&str, &[&str])] = &[
        (
            "MATCH (a)-[r]->(b) RETURN a.id, b.id, coalesce(r['w'], 0) AS w",
            &[
                "[Int(1), Int(2), Int(10)]",
                "[Int(1), Int(3), Int(7)]",
                "[Int(2), Int(3), Int(0)]",
            ],
        ),
        (
            "MATCH (a)-[r:R|S]->(b) RETURN a.id, b.id, coalesce(r['w'], -1) AS w, \
             r['only_s'] AS s",
            &[
                "[Int(1), Int(2), Int(10), Null]",
                "[Int(1), Int(3), Int(7), String(\"x\")]",
                "[Int(2), Int(3), Int(-1), Null]",
            ],
        ),
        (
            "MATCH (a)-[r]->(b) WHERE coalesce(r['w'], 0) > 5 RETURN a.id, b.id",
            &["[Int(1), Int(2)]", "[Int(1), Int(3)]"],
        ),
    ];
    for flush in [false, true] {
        let db = open(flush).await?;
        for (query, want) in cases {
            let got = bag(&db, query)
                .await
                .map_err(|e| anyhow::anyhow!("flush={flush} {query}: {e}"))?;
            assert_eq!(got, *want, "flush={flush}: {query}");
        }
    }
    Ok(())
}
