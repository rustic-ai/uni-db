// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! `WITH … ORDER BY … SKIP/LIMIT … WHERE` over scans the planner can push a
//! predicate into.
//!
//! `with_clause_test::with_where_filters_after_order_by_skip_limit` pins the
//! rule — the `WHERE` filters the page — over an `UNWIND`. A predicate on a
//! scanned property can also reach the scan itself (a Lance filter, a BTree
//! index probe, the vector index's `K`), and there it would filter before the
//! page. These cases re-check the rule over a label scan, with and without a
//! BTree index, flushed and unflushed, and over a `~=` index scan. Each
//! expectation is the page computed by hand, then filtered.
//!
//! Run with:
//!   cargo nextest run -p uni-db --test integration -E 'test(with_where_after_page_scans)'

// Rust guideline compliant

use anyhow::Result;
use uni_db::{DataType, IndexType, Uni, VectorAlgo, VectorIndexCfg, VectorMetric};

async fn open(indexed: bool, flush: bool) -> Result<Uni> {
    let db = Uni::in_memory().build().await?;
    db.schema()
        .label("N")
        .property("id", DataType::Int64)
        .property("x", DataType::Int64)
        .property("emb", DataType::Vector { dimensions: 2 })
        .index(
            "emb",
            IndexType::Vector(VectorIndexCfg {
                algorithm: VectorAlgo::Flat,
                metric: VectorMetric::Cosine,
                embedding: None,
            }),
        )
        .done()
        .apply()
        .await?;
    let tx = db.session().tx().await?;
    if indexed {
        tx.execute("CREATE INDEX idx_n_id FOR (n:N) ON (n.id)")
            .await?;
    }
    // Row `id` points at angle `id / 10`, so similarity to `[1, 0]` falls as
    // `id` grows; `x` is `id % 3`.
    for id in 1..=10_i64 {
        let theta = id as f64 / 10.0;
        tx.execute(&format!(
            "CREATE (:N {{id: {id}, x: {}, emb: [{:?}, {:?}]}})",
            id % 3,
            theta.cos(),
            theta.sin()
        ))
        .await?;
    }
    tx.commit().await?;
    if flush {
        db.flush().await?;
    }
    Ok(db)
}

async fn ids(db: &Uni, query: &str) -> Result<Vec<i64>> {
    let result = db
        .session()
        .query(query)
        .await
        .map_err(|e| anyhow::anyhow!("{query}: {e}"))?;
    result
        .rows()
        .iter()
        .map(|r| Ok(r.get::<i64>("id")?))
        .collect()
}

#[tokio::test]
async fn with_where_after_page_scans_filter_the_page() -> Result<()> {
    let cases: &[(&str, &[i64])] = &[
        // Page 1..3, then id > 1.
        (
            "MATCH (n:N) WITH n ORDER BY n.id LIMIT 3 WHERE n.id > 1 RETURN n.id AS id",
            &[2, 3],
        ),
        // Page 3..5 (SKIP 2), then id >= 4.
        (
            "MATCH (n:N) WITH n ORDER BY n.id SKIP 2 LIMIT 3 WHERE n.id >= 4 \
             RETURN n.id AS id",
            &[4, 5],
        ),
        // An equality an index could answer: page 1..3 holds no 7.
        (
            "MATCH (n:N) WITH n ORDER BY n.id LIMIT 3 WHERE n.id = 7 RETURN n.id AS id",
            &[],
        ),
        // A projected property: page 8..10 (DESC), then x = 0 (id 9).
        (
            "MATCH (n:N) WITH n.id AS id, n.x AS x ORDER BY id DESC LIMIT 3 WHERE x = 0 \
             RETURN id",
            &[9],
        ),
        // A later MATCH reads the filtered page.
        (
            "MATCH (n:N) WITH n ORDER BY n.id LIMIT 4 WHERE n.x = 1 \
             MATCH (m:N) WHERE m.id = n.id RETURN m.id AS id ORDER BY id",
            &[1, 4],
        ),
        // The vector index's top 3 is ids 1..3; the WHERE filters them.
        (
            "MATCH (n:N) WHERE n.emb ~= [1.0, 0.0] WITH n LIMIT 3 WHERE n.x = 2 \
             RETURN n.id AS id",
            &[2],
        ),
        (
            "MATCH (n:N) WITH n ORDER BY vector_similarity(n.emb, [1.0, 0.0]) DESC LIMIT 3 \
             WHERE n.x <> 1 RETURN n.id AS id ORDER BY id",
            &[2, 3],
        ),
    ];
    for indexed in [false, true] {
        for flush in [false, true] {
            let db = open(indexed, flush).await?;
            for (query, want) in cases {
                let mut got = ids(&db, query).await?;
                got.sort_unstable();
                assert_eq!(got, *want, "indexed={indexed} flush={flush}: {query}");
            }
        }
    }
    Ok(())
}
