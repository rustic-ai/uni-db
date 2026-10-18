// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Regression for the #131 bug *class*: `collect_plan_variables_into`
//! (uni-query `df_planner.rs`) must expose the output columns of EVERY
//! variable-producing `LogicalPlan` variant. If a variant is invisible there,
//! an equi-join over it is not recognized by `classify_join_predicate` and the
//! physical planner emits a quadratic `CrossJoinExec` instead of a
//! `HashJoinExec`. #131 was the Locy `LocyDerivedScan` instance.
//!
//! Each test below puts one such variant as a direct child of a `CrossJoin`
//! under a cross-side equality (`WHERE a.q = c.q`) and asserts three things:
//!   1. the variant is actually exercised (`explain().plan_text` names it),
//!   2. the join recovers a `HashJoinExec` and NOT a `CrossJoinExec`,
//!   3. the result cardinality matches an independent reference computation.
//!
//! Variants covered (reachable as a CrossJoin child): `BindPath`, `VectorKnn`,
//! `ShortestPath`, `AllShortestPaths`, `BindZeroLengthPath`.
//!
//! Variants that `collect_plan_variables_into` handles but are NOT currently
//! reachable as a comma-`MATCH` CrossJoin child (defended defensively only, so
//! they have no live test here):
//!   * `FusedIndexScan` / `FusedIndexScanWrapped` — the fork-fusion rewrite
//!     `rewrite_node` (planner.rs) recurses into `Filter`/`Project`/`Sort`/
//!     `Union` but NOT `CrossJoin`, so a `Scan` under a cross join is never
//!     rewritten to a fused scan. (A query confirms it stays a plain `Scan`.)
//!   * `ProcedureCall` — produced by `CALL ... YIELD`, never a comma-MATCH join.
//!   * `QuantifiedPattern` — dead variant: QPP lowers to `Traverse`; its
//!     physical-planner arm is an unsupported-error stub.

use uni_db::{DataType, IndexType, Session, Uni, Value, VectorAlgo, VectorIndexCfg, VectorMetric};

/// Physical operator names from a profiled query.
fn join_ops(p: &uni_db::ProfileOutput) -> Vec<String> {
    p.runtime_stats.iter().map(|s| s.operator.clone()).collect()
}

/// The join must be a `HashJoinExec`; a `CrossJoinExec` means the cross-side
/// equality was not recognized and the plan went quadratic (the #131 class).
fn assert_hash_not_cross(ops: &[String], ctx: &str) {
    assert!(
        ops.iter().any(|o| o == "HashJoinExec"),
        "{ctx}: expected HashJoinExec, got ops: {ops:?}"
    );
    assert!(
        !ops.iter().any(|o| o == "CrossJoinExec"),
        "{ctx}: equi-join degraded to CrossJoinExec (collect_plan_variables blind spot); ops: {ops:?}"
    );
}

/// Looser check for operators that legitimately contain an *internal* cross
/// join unrelated to the cross-MATCH equi-join under test — e.g. `shortestPath`
/// cross-joins its two unbound endpoint scans over all `(a, b)` pairs. Here we
/// only assert the OUTER equi-join recovered (a `HashJoinExec` is present);
/// before the collector fix this outer join was itself a `CrossJoinExec`, so a
/// missing `HashJoinExec` still flags the regression.
fn assert_equi_join_recovered(ops: &[String], ctx: &str) {
    assert!(
        ops.iter().any(|o| o == "HashJoinExec"),
        "{ctx}: cross-MATCH equi-join did not recover a HashJoinExec; ops: {ops:?}"
    );
}

/// Collect one Int64 column from a (non-mutating) query.
async fn col_i64(s: &Session, q: &str, col: &str) -> Vec<i64> {
    s.query(q)
        .await
        .unwrap()
        .rows()
        .iter()
        .map(|r| r.get(col).unwrap())
        .collect()
}

/// Reference equi-join cardinality of two `q`-value multisets.
fn join_count(left_q: &[i64], right_q: &[i64]) -> i64 {
    left_q
        .iter()
        .map(|a| right_q.iter().filter(|b| *b == a).count() as i64)
        .sum()
}

/// `MATCH p = (a:L)-[:R]->(b:L), (c:L) WHERE a.q = c.q` — `a` is bound under a
/// `BindPath` (named path) node. Must recover a `HashJoinExec`.
#[tokio::test]
async fn bindpath_cross_match_equi_join_is_hash_join() {
    let db = Uni::in_memory().build().await.unwrap();
    db.schema()
        .label("L")
        .property("q", DataType::Int64)
        .done()
        .edge_type("R", &["L"], &["L"])
        .done()
        .apply()
        .await
        .unwrap();
    let s = db.session();
    let tx = s.tx().await.unwrap();
    for i in 0..50i64 {
        tx.query_with("CREATE (:L {q:$q})")
            .param("q", Value::Int(i % 10))
            .fetch_all()
            .await
            .unwrap();
    }
    tx.execute("MATCH (a:L),(b:L) WHERE a.q=b.q AND id(a)<id(b) CREATE (a)-[:R]->(b)")
        .await
        .unwrap();
    tx.commit().await.unwrap();

    let q = "MATCH p = (a:L)-[:R]->(b:L), (c:L) WHERE a.q = c.q RETURN count(*) AS n";
    let ex = s.query_with(q).explain().await.unwrap();
    assert!(ex.plan_text.contains("BindPath"), "plan: {}", ex.plan_text);

    let (result, profile) = s.query_with(q).profile().await.unwrap();
    let n: i64 = result.rows().first().unwrap().get("n").unwrap();

    // Reference: a.q over the same path pattern × c.q over all L nodes.
    let aq = col_i64(&s, "MATCH p = (a:L)-[:R]->(b:L) RETURN a.q AS q", "q").await;
    let cq = col_i64(&s, "MATCH (c:L) RETURN c.q AS q", "q").await;
    assert_eq!(n, join_count(&aq, &cq), "BindPath join cardinality");
    assert_hash_not_cross(&join_ops(&profile), "BindPath");
}

/// `MATCH (a:Doc),(b:Doc) WHERE a.embedding ~= [..] AND a.q = b.q` — `a` is
/// rewritten to a `VectorKnn` node in-place under the CrossJoin. (Only `~=`
/// reaches `VectorKnn`; a `vector_similarity` threshold is a per-row filter.)
#[tokio::test]
async fn vectorknn_cross_match_equi_join_is_hash_join() {
    let db = Uni::in_memory().build().await.unwrap();
    db.schema()
        .label("Doc")
        .property("q", DataType::Int64)
        .vector("embedding", 3)
        .index(
            "embedding",
            IndexType::Vector(VectorIndexCfg {
                algorithm: VectorAlgo::Flat,
                metric: VectorMetric::Cosine,
                embedding: None,
            }),
        )
        .done()
        .apply()
        .await
        .unwrap();
    let s = db.session();
    let tx = s.tx().await.unwrap();
    // 20 docs, under the KNN's top-100: every `a` is returned, so the join
    // cardinality is fully determined by `q`.
    for i in 0..20i64 {
        tx.query_with("CREATE (:Doc {q:$q, embedding:[1.0, 0.0, 0.0]})")
            .param("q", Value::Int(i % 4))
            .fetch_all()
            .await
            .unwrap();
    }
    tx.commit().await.unwrap();

    let q = "MATCH (a:Doc),(b:Doc) \
             WHERE a.embedding ~= [1.0, 0.0, 0.0] AND a.q = b.q \
             RETURN count(*) AS n";
    let ex = s.query_with(q).explain().await.unwrap();
    assert!(ex.plan_text.contains("VectorKnn"), "plan: {}", ex.plan_text);

    let (result, profile) = s.query_with(q).profile().await.unwrap();
    let n: i64 = result.rows().first().unwrap().get("n").unwrap();

    let aq = col_i64(
        &s,
        "MATCH (a:Doc) WHERE a.embedding ~= [1.0, 0.0, 0.0] RETURN a.q AS q",
        "q",
    )
    .await;
    let bq = col_i64(&s, "MATCH (b:Doc) RETURN b.q AS q", "q").await;
    assert_eq!(n, join_count(&aq, &bq), "VectorKnn join cardinality");
    assert_hash_not_cross(&join_ops(&profile), "VectorKnn");
}

/// Build a complete directed graph so every ordered pair is reachable, then
/// `MATCH p = shortestPath((a:L)-[:R*]->(b:L)), (c:L) WHERE a.q = c.q`.
async fn setup_connected_graph(n: i64) -> Uni {
    let db = Uni::in_memory().build().await.unwrap();
    db.schema()
        .label("L")
        .property("q", DataType::Int64)
        .done()
        .edge_type("R", &["L"], &["L"])
        .done()
        .apply()
        .await
        .unwrap();
    let s = db.session();
    let tx = s.tx().await.unwrap();
    for i in 0..n {
        tx.query_with("CREATE (:L {q:$q, id:$id})")
            .param("q", Value::Int(i % 3))
            .param("id", Value::Int(i))
            .fetch_all()
            .await
            .unwrap();
    }
    // Complete directed graph (every a -> b, a != b): all ordered pairs reachable.
    tx.execute("MATCH (a:L),(b:L) WHERE id(a)<>id(b) CREATE (a)-[:R]->(b)")
        .await
        .unwrap();
    tx.commit().await.unwrap();
    db
}

#[tokio::test]
async fn shortestpath_cross_match_equi_join_is_hash_join() {
    let db = setup_connected_graph(5).await;
    let s = db.session();

    let q =
        "MATCH p = shortestPath((a:L)-[:R*]->(b:L)), (c:L) WHERE a.q = c.q RETURN count(*) AS n";
    let ex = s.query_with(q).explain().await.unwrap();
    assert!(
        ex.plan_text.contains("ShortestPath"),
        "plan: {}",
        ex.plan_text
    );

    let (result, profile) = s.query_with(q).profile().await.unwrap();
    let n: i64 = result.rows().first().unwrap().get("n").unwrap();

    let aq = col_i64(
        &s,
        "MATCH p = shortestPath((a:L)-[:R*]->(b:L)) RETURN a.q AS q",
        "q",
    )
    .await;
    let cq = col_i64(&s, "MATCH (c:L) RETURN c.q AS q", "q").await;
    assert_eq!(n, join_count(&aq, &cq), "ShortestPath join cardinality");
    // shortestPath has an inherent internal endpoint cross join; assert the
    // outer cross-MATCH equi-join recovered.
    assert_equi_join_recovered(&join_ops(&profile), "ShortestPath");
}

#[tokio::test]
async fn allshortestpaths_cross_match_equi_join_is_hash_join() {
    let db = setup_connected_graph(5).await;
    let s = db.session();

    let q = "MATCH p = allShortestPaths((a:L)-[:R*]->(b:L)), (c:L) WHERE a.q = c.q RETURN count(*) AS n";
    let ex = s.query_with(q).explain().await.unwrap();
    assert!(
        ex.plan_text.contains("AllShortestPaths"),
        "plan: {}",
        ex.plan_text
    );

    let (result, profile) = s.query_with(q).profile().await.unwrap();
    let n: i64 = result.rows().first().unwrap().get("n").unwrap();

    let aq = col_i64(
        &s,
        "MATCH p = allShortestPaths((a:L)-[:R*]->(b:L)) RETURN a.q AS q",
        "q",
    )
    .await;
    let cq = col_i64(&s, "MATCH (c:L) RETURN c.q AS q", "q").await;
    assert_eq!(n, join_count(&aq, &cq), "AllShortestPaths join cardinality");
    // Inherent internal endpoint cross join (see shortestPath test).
    assert_equi_join_recovered(&join_ops(&profile), "AllShortestPaths");
}

/// A single-node named path (`MATCH p = (a:L)`) lowers to `BindZeroLengthPath`.
#[tokio::test]
async fn bindzerolengthpath_cross_match_equi_join_is_hash_join() {
    let db = Uni::in_memory().build().await.unwrap();
    db.schema()
        .label("L")
        .property("q", DataType::Int64)
        .done()
        .apply()
        .await
        .unwrap();
    let s = db.session();
    let tx = s.tx().await.unwrap();
    for i in 0..12i64 {
        tx.query_with("CREATE (:L {q:$q})")
            .param("q", Value::Int(i % 3))
            .fetch_all()
            .await
            .unwrap();
    }
    tx.commit().await.unwrap();

    let q = "MATCH p = (a:L), (c:L) WHERE a.q = c.q RETURN count(*) AS n";
    let ex = s.query_with(q).explain().await.unwrap();
    assert!(
        ex.plan_text.contains("BindZeroLengthPath"),
        "plan: {}",
        ex.plan_text
    );

    let (result, profile) = s.query_with(q).profile().await.unwrap();
    let n: i64 = result.rows().first().unwrap().get("n").unwrap();

    let aq = col_i64(&s, "MATCH p = (a:L) RETURN a.q AS q", "q").await;
    let cq = col_i64(&s, "MATCH (c:L) RETURN c.q AS q", "q").await;
    assert_eq!(
        n,
        join_count(&aq, &cq),
        "BindZeroLengthPath join cardinality"
    );
    assert_hash_not_cross(&join_ops(&profile), "BindZeroLengthPath");
}
