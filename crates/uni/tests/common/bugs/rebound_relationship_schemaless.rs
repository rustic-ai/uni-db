// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! A relationship bound by an earlier clause and matched again, schemaless.
//!
//! `MATCH ()-[r:R]->() WITH r MATCH (x)-[r:R]->(y)` must match each `r`
//! once, at its own endpoints. With the type undeclared, the walk bound the
//! edge under the same name as the earlier binding, so only one of the two
//! columns survived: every input row was paired with every edge of the type
//! (16 rows for 4 edges), or the earlier binding was ignored (`r:S` matched
//! the one `S` edge for all five rows) or its type was. The schema'd path
//! walks a temporary name and ties it to the binding with a filter; the
//! schemaless path now does the same.
//!
//! The oracle is the same query against the same graph with the types
//! declared.
//!
//! Run with:
//!   cargo nextest run -p uni-db --test integration -E 'test(rebound_relationship_schemaless)'

// Rust guideline compliant

use anyhow::Result;
use uni_db::{DataType, Uni};

async fn open(schema: bool, flush: bool) -> Result<Uni> {
    let db = Uni::in_memory().build().await?;
    if schema {
        db.schema()
            .label("N")
            .property("id", DataType::Int)
            .edge_type("R", &["N"], &["N"])
            .property_nullable("w", DataType::Int)
            .edge_type("S", &["N"], &["N"])
            .property_nullable("w", DataType::Int)
            .done()
            .apply()
            .await?;
    }
    let tx = db.session().tx().await?;
    tx.execute(
        "CREATE (a:N {id: 1}), (b:N {id: 2}), (c:N {id: 3}), (d:N {id: 4}), (e:N {id: 5}), \
         (a)-[:R {w: 10}]->(b), (b)-[:R {w: 20}]->(c), (c)-[:R {w: 30}]->(d), (d)-[:R]->(e), \
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
    let result = db
        .session()
        .query(query)
        .await
        .map_err(|e| anyhow::anyhow!("{query}: {e}"))?;
    let mut rows: Vec<String> = result
        .rows()
        .iter()
        .map(|r| format!("{:?}", r.values()))
        .collect();
    rows.sort();
    Ok(rows)
}

#[tokio::test]
async fn rebound_relationship_schemaless_matches_the_bound_edge() -> Result<()> {
    let queries = [
        "MATCH (a)-[r:R]->(b) WITH r MATCH (x)-[r:R]->(y) RETURN x.id, y.id",
        "MATCH (a)-[r]->(b) WITH r MATCH (x)-[r:S]->(y) RETURN x.id, y.id",
        "MATCH ()-[r]->() MATCH ()-[r:S]->() RETURN r.w",
        "MATCH ()-[r]->() WITH collect(r) AS rs UNWIND rs AS r MATCH ()-[r:S]->() RETURN r.w",
        "MATCH ()-[r:R]->() WITH r MATCH (x)-[r:R {w: 20}]->(y) RETURN x.id, y.id",
        "MATCH (a)-[r]->(b) OPTIONAL MATCH (x)-[r:S]->(y) RETURN r.w, x.id",
        "MATCH (a)-[r1:R]->(b)-[r2:R]->(c) WITH [r1, r2] AS rs \
         MATCH (x)-[rs:R*]->(y) RETURN x.id, y.id",
    ];
    for flush in [false, true] {
        let declared = open(true, flush).await?;
        let schemaless = open(false, flush).await?;
        for query in queries {
            let want = bag(&declared, query).await?;
            assert!(!want.is_empty(), "vacuous oracle: {query}");
            assert_eq!(
                bag(&schemaless, query).await?,
                want,
                "flush={flush}: {query}"
            );
        }
    }
    Ok(())
}
