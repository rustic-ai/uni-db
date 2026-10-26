// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Integration tests for `similar_to()` expression function.
//!
//! Tests the full Cypher → parser → planner → executor → result pipeline,
//! covering vector-vector similarity, FTS scoring, validation errors,
//! backward compatibility, and Locy integration.

use anyhow::Result;
use uni_common::core::schema::DataType;
use uni_db::{IndexType, Uni, VectorAlgo, VectorIndexCfg, VectorMetric};

// ── Helper ──────────────────────────────────────────────────────────────────

/// Sets up a database with Doc nodes, vector + fulltext indexes, and test data.
///
/// Test vectors are unit-length for predictable cosine similarity against
/// the query vector `[1.0, 0.0, 0.0]`:
///
/// | Doc   | Embedding       | Cosine | Content keywords      |
/// |-------|-----------------|--------|-----------------------|
/// | Alpha | [1.0, 0.0, 0.0] | 1.0    | rust, programming     |
/// | Beta  | [0.6, 0.8, 0.0] | 0.6    | python, scripting     |
/// | Gamma | [0.0, 0.0, 1.0] | 0.0    | database, storage     |
/// | Delta | [0.8, 0.6, 0.0] | 0.8    | rust, programming     |
async fn setup_doc_db() -> Result<Uni> {
    let db = Uni::in_memory().build().await?;

    db.schema()
        .label("Doc")
        .property("title", DataType::String)
        .property("content", DataType::String)
        .vector("embedding", 3)
        .index(
            "embedding",
            IndexType::Vector(VectorIndexCfg {
                algorithm: VectorAlgo::Flat,
                metric: VectorMetric::Cosine,
                embedding: None,
            }),
        )
        .index("content", IndexType::FullText)
        .done()
        .apply()
        .await?;

    let tx = db.session().tx().await?;
    tx.execute(
        "CREATE (:Doc {title: 'Alpha', content: 'rust systems programming language', \
         embedding: [1.0, 0.0, 0.0]})",
    )
    .await?;
    tx.execute(
        "CREATE (:Doc {title: 'Beta', content: 'python data science scripting', \
         embedding: [0.6, 0.8, 0.0]})",
    )
    .await?;
    tx.execute(
        "CREATE (:Doc {title: 'Gamma', content: 'database storage engine systems', \
         embedding: [0.0, 0.0, 1.0]})",
    )
    .await?;
    tx.execute(
        "CREATE (:Doc {title: 'Delta', content: 'rust memory safety programming', \
         embedding: [0.8, 0.6, 0.0]})",
    )
    .await?;
    tx.commit().await?;

    db.flush().await?;

    Ok(db)
}

// ── Phase 1: Basic Vector Tests (Spec 8.1) ───────────────────────────────────

#[tokio::test]
async fn test_similar_to_vector_vector() -> Result<()> {
    let db = setup_doc_db().await?;

    let result = db
        .session()
        .query(
            "MATCH (d:Doc) \
             RETURN d.title AS title, similar_to(d.embedding, [1.0, 0.0, 0.0]) AS score \
             ORDER BY score DESC",
        )
        .await?;

    assert_eq!(result.len(), 4);

    // Check ordering: Alpha (1.0), Delta (0.8), Beta (0.6), Gamma (0.0)
    let titles: Vec<String> = result
        .rows()
        .iter()
        .map(|r| r.get::<String>("title").unwrap())
        .collect();
    assert_eq!(titles, vec!["Alpha", "Delta", "Beta", "Gamma"]);

    // Check scores
    let scores: Vec<f64> = result
        .rows()
        .iter()
        .map(|r| r.get::<f64>("score").unwrap())
        .collect();

    assert!(
        (scores[0] - 1.0).abs() < 1e-5,
        "identical vectors should score 1.0, got {}",
        scores[0]
    );
    assert!(
        (scores[1] - 0.8).abs() < 1e-5,
        "Delta should score 0.8, got {}",
        scores[1]
    );
    assert!(
        (scores[2] - 0.6).abs() < 1e-5,
        "Beta should score 0.6, got {}",
        scores[2]
    );
    assert!(
        scores[3].abs() < 1e-5,
        "orthogonal should score 0.0, got {}",
        scores[3]
    );

    Ok(())
}

#[tokio::test]
async fn test_similar_to_in_where() -> Result<()> {
    let db = setup_doc_db().await?;

    let result = db
        .session()
        .query(
            "MATCH (d:Doc) \
             WHERE similar_to(d.embedding, [1.0, 0.0, 0.0]) > 0.7 \
             RETURN d.title AS title \
             ORDER BY title",
        )
        .await?;

    let titles: Vec<String> = result
        .rows()
        .iter()
        .map(|r| r.get::<String>("title").unwrap())
        .collect();
    // Only Alpha (1.0) and Delta (0.8) exceed 0.7
    assert_eq!(titles, vec!["Alpha", "Delta"]);

    Ok(())
}

#[tokio::test]
async fn test_similar_to_in_order_by() -> Result<()> {
    let db = setup_doc_db().await?;

    let result = db
        .session()
        .query(
            "MATCH (d:Doc) \
             RETURN d.title AS title \
             ORDER BY similar_to(d.embedding, [1.0, 0.0, 0.0]) DESC",
        )
        .await?;

    let titles: Vec<String> = result
        .rows()
        .iter()
        .map(|r| r.get::<String>("title").unwrap())
        .collect();
    assert_eq!(titles, vec!["Alpha", "Delta", "Beta", "Gamma"]);

    Ok(())
}

#[tokio::test]
async fn test_similar_to_with_parameter() -> Result<()> {
    let db = setup_doc_db().await?;

    let result = db
        .session()
        .query_with(
            "MATCH (d:Doc) \
             RETURN d.title AS title, similar_to(d.embedding, $q) AS score \
             ORDER BY score DESC",
        )
        .param("q", uni_common::Value::Vector(vec![1.0, 0.0, 0.0]))
        .fetch_all()
        .await?;

    assert_eq!(result.len(), 4);

    let titles: Vec<String> = result
        .rows()
        .iter()
        .map(|r| r.get::<String>("title").unwrap())
        .collect();
    assert_eq!(titles, vec!["Alpha", "Delta", "Beta", "Gamma"]);

    // Same scores as inline vector
    let score0: f64 = result.rows()[0].get("score")?;
    assert!(
        (score0 - 1.0).abs() < 1e-5,
        "parameter-passed vector should give same score, got {}",
        score0
    );

    Ok(())
}

// ── Phase 2: FTS Tests (Spec 8.1) ────────────────────────────────────────────

#[tokio::test]
async fn test_similar_to_fts() -> Result<()> {
    let db = setup_doc_db().await?;

    let result = db
        .session()
        .query(
            "MATCH (d:Doc) \
             RETURN d.title AS title, similar_to(d.content, 'rust') AS score \
             ORDER BY score DESC",
        )
        .await?;

    assert_eq!(result.len(), 4);

    for row in result.rows() {
        let title: String = row.get("title")?;
        let score: f64 = row.get("score")?;
        assert!(
            (0.0..=1.0).contains(&score),
            "score should be in [0,1], got {}",
            score
        );

        match title.as_str() {
            "Alpha" | "Delta" => {
                assert!(
                    score > 0.0,
                    "{} contains 'rust' but got score = {}",
                    title,
                    score
                );
            }
            "Beta" | "Gamma" => {
                assert!(
                    score.abs() < 1e-5,
                    "{} doesn't contain 'rust' but got score = {}",
                    title,
                    score
                );
            }
            _ => panic!("unexpected title: {}", title),
        }
    }

    Ok(())
}

#[tokio::test]
async fn test_similar_to_fts_in_where() -> Result<()> {
    let db = setup_doc_db().await?;

    let result = db
        .session()
        .query(
            "MATCH (d:Doc) \
             WHERE similar_to(d.content, 'rust') > 0.0 \
             RETURN d.title AS title \
             ORDER BY title",
        )
        .await?;

    let titles: Vec<String> = result
        .rows()
        .iter()
        .map(|r| r.get::<String>("title").unwrap())
        .collect();
    assert_eq!(titles, vec!["Alpha", "Delta"]);

    Ok(())
}

// ── Phase 3: Validation Error Tests (Spec 8.2) ──────────────────────────────

#[tokio::test]
async fn test_similar_to_string_no_fts_index() -> Result<()> {
    let db = Uni::in_memory().build().await?;

    db.schema()
        .label("Doc")
        .property("title", DataType::String)
        .done()
        .apply()
        .await?;

    let tx = db.session().tx().await?;
    tx.execute("CREATE (:Doc {title: 'Hello'})").await?;
    tx.commit().await?;
    db.flush().await?;

    // title has no FTS index → should error
    let err = db
        .session()
        .query("MATCH (d:Doc) RETURN similar_to(d.title, 'hello') AS score")
        .await;

    assert!(err.is_err(), "similar_to on unindexed string should error");
    let msg = err.unwrap_err().to_string();
    assert!(
        msg.contains("no vector or full-text index") || msg.contains("similar_to"),
        "error should mention missing index, got: {}",
        msg
    );

    Ok(())
}

#[tokio::test]
async fn test_similar_to_type_mismatch_fts_vector() -> Result<()> {
    let db = setup_doc_db().await?;

    // content has FTS index but query is a vector → should error
    let err = db
        .session()
        .query("MATCH (d:Doc) RETURN similar_to(d.content, [1.0, 0.0, 0.0]) AS score")
        .await;

    assert!(err.is_err(), "FTS source with vector query should error");
    let msg = err.unwrap_err().to_string();
    assert!(
        msg.contains("FTS") || msg.contains("similar_to"),
        "error should mention FTS/type mismatch, got: {}",
        msg
    );

    Ok(())
}

#[tokio::test]
async fn test_similar_to_weights_length_mismatch() -> Result<()> {
    let db = setup_doc_db().await?;

    // 1 source (embedding) but 2 weights → should error
    let err = db
        .session()
        .query(
            "MATCH (d:Doc) \
             RETURN similar_to(d.embedding, [1.0, 0.0, 0.0], \
             {method: 'weighted', weights: [0.5, 0.5]}) AS score",
        )
        .await;

    assert!(err.is_err(), "weights length mismatch should error");
    let msg = err.unwrap_err().to_string();
    assert!(
        msg.contains("weights") || msg.contains("length"),
        "error should mention weights/length mismatch, got: {}",
        msg
    );

    Ok(())
}

// ── Phase 4: Backward Compatibility (Spec 8.4) ──────────────────────────────

#[tokio::test]
async fn test_vector_similarity_still_works() -> Result<()> {
    let db = setup_doc_db().await?;

    let result = db
        .session()
        .query(
            "MATCH (a:Doc), (b:Doc) \
             WHERE a.title = 'Alpha' AND b.title = 'Delta' \
             RETURN vector_similarity(a.embedding, b.embedding) AS score",
        )
        .await?;

    assert_eq!(result.len(), 1);
    let score: f64 = result.rows()[0].get("score")?;
    // Alpha [1,0,0] vs Delta [0.8,0.6,0] → cosine = 0.8
    assert!(
        (0.0..=1.0).contains(&score),
        "score should be in [0,1], got {}",
        score
    );
    assert!((score - 0.8).abs() < 1e-5, "expected ~0.8, got {}", score);

    Ok(())
}

#[tokio::test]
async fn test_vector_similarity_matches_similar_to() -> Result<()> {
    let db = setup_doc_db().await?;

    let result = db
        .session()
        .query(
            "MATCH (a:Doc), (b:Doc) \
             WHERE a.title = 'Alpha' AND b.title = 'Beta' \
             RETURN vector_similarity(a.embedding, b.embedding) AS vs_score, \
                    similar_to(a.embedding, b.embedding) AS st_score",
        )
        .await?;

    assert_eq!(result.len(), 1);
    let vs_score: f64 = result.rows()[0].get("vs_score")?;
    let st_score: f64 = result.rows()[0].get("st_score")?;

    assert!(
        (vs_score - st_score).abs() < 1e-5,
        "vector_similarity ({}) and similar_to ({}) should give identical scores",
        vs_score,
        st_score
    );

    Ok(())
}

#[tokio::test]
async fn test_procedures_unchanged() -> Result<()> {
    let db = setup_doc_db().await?;

    let result = db
        .session()
        .query(
            "CALL uni.vector.query('Doc', 'embedding', [1.0, 0.0, 0.0], 10) \
             YIELD node, score \
             RETURN node.title AS title, score \
             ORDER BY score DESC",
        )
        .await?;

    assert!(!result.is_empty(), "procedure should return results");

    let first_title: String = result.rows()[0].get("title")?;
    assert_eq!(first_title, "Alpha", "most similar should be Alpha");

    Ok(())
}

// ── Phase 5: Locy Integration (Spec 8.3) ─────────────────────────────────────

#[tokio::test]
async fn test_similar_to_in_locy_where() -> Result<()> {
    let db = setup_doc_db().await?;

    let result = db
        .session()
        .locy(
            "CREATE RULE relevant AS \
             MATCH (d:Doc) \
             WHERE similar_to(d.embedding, [1.0, 0.0, 0.0]) > 0.7 \
             YIELD KEY d",
        )
        .await?;

    let relevant = result
        .derived_rules()
        .await
        .unwrap()
        .get("relevant")
        .expect("rule 'relevant' missing");
    // Only Alpha (1.0) and Delta (0.8) exceed 0.7
    assert_eq!(
        relevant.len(),
        2,
        "expected 2 relevant docs (Alpha, Delta), got {}",
        relevant.len()
    );

    Ok(())
}

#[tokio::test]
async fn test_similar_to_in_locy_yield() -> Result<()> {
    let db = setup_doc_db().await?;

    let result = db
        .session()
        .locy(
            "CREATE RULE scored AS \
             MATCH (d:Doc) \
             YIELD KEY d, similar_to(d.embedding, [1.0, 0.0, 0.0]) AS score",
        )
        .await?;

    let scored = result
        .derived_rules()
        .await
        .unwrap()
        .get("scored")
        .expect("rule 'scored' missing");
    assert_eq!(
        scored.len(),
        4,
        "expected 4 scored docs, got {}",
        scored.len()
    );

    // All scores should be in [0, 1]
    for fact in scored {
        if let Some(uni_common::Value::Float(s)) = fact.get("score") {
            assert!(
                (0.0..=1.0).contains(s),
                "score should be in [0,1], got {}",
                s
            );
        }
    }

    Ok(())
}

#[tokio::test]
async fn test_similar_to_in_locy_along() -> Result<()> {
    let db = Uni::in_memory().build().await?;

    db.schema()
        .label("Doc")
        .property("title", DataType::String)
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
        .edge_type("LINKS", &["Doc"], &["Doc"])
        .done()
        .apply()
        .await?;

    // Chain: A → B → C
    {
        let s = db.session();
        let tx = s.tx().await?;
        tx.execute(
            "CREATE (a:Doc {title: 'A', embedding: [1.0, 0.0, 0.0]}), \
         (b:Doc {title: 'B', embedding: [0.8, 0.6, 0.0]}), \
         (c:Doc {title: 'C', embedding: [0.0, 0.0, 1.0]}), \
         (a)-[:LINKS]->(b), \
         (b)-[:LINKS]->(c)",
        )
        .await?;
        tx.commit().await?;
    }
    db.flush().await?;

    // ALONG accumulates similarity along the path
    let result = db
        .session()
        .locy(
            "CREATE RULE sim_path AS \
             MATCH (a:Doc)-[:LINKS]->(b:Doc) \
             ALONG score = similar_to(b.embedding, [1.0, 0.0, 0.0]) \
             BEST BY score DESC \
             YIELD KEY a, KEY b, score \n\
             CREATE RULE sim_path AS \
             MATCH (a:Doc)-[:LINKS]->(mid:Doc) \
             WHERE mid IS sim_path TO b \
             ALONG score = prev.score * similar_to(b.embedding, [1.0, 0.0, 0.0]) \
             BEST BY score DESC \
             YIELD KEY a, KEY b, score",
        )
        .await?;

    let paths = result
        .derived_rules()
        .await
        .unwrap()
        .get("sim_path")
        .expect("rule 'sim_path' missing");
    // A→B, B→C, and transitively A→C
    assert!(
        paths.len() >= 2,
        "expected at least 2 path facts, got {}",
        paths.len()
    );

    Ok(())
}

// ── Phase 6: Multi-Source Fusion Tests ────────────────────────────────────────

#[tokio::test]
async fn test_similar_to_multi_source_default_rrf() -> Result<()> {
    let db = setup_doc_db().await?;

    // Two sources: vector (embedding) + FTS (content), default RRF fusion
    // RRF falls back to equal-weight average in point context.
    // Use 'language' (unique to Alpha) instead of 'rust' (shared by Alpha+Delta)
    // to avoid non-deterministic BM25 segment-level scoring that can flip
    // equally-relevant documents' scores by up to 2.4x across Tantivy segments.
    let result = db
        .session()
        .query(
            "MATCH (d:Doc) \
             RETURN d.title AS title, \
             similar_to([d.embedding, d.content], [[1.0, 0.0, 0.0], 'language']) AS score \
             ORDER BY score DESC",
        )
        .await?;

    assert_eq!(result.len(), 4);

    let titles: Vec<String> = result
        .rows()
        .iter()
        .map(|r| r.get::<String>("title").unwrap())
        .collect();
    let scores: Vec<f64> = result
        .rows()
        .iter()
        .map(|r| r.get::<f64>("score").unwrap())
        .collect();

    // All scores in [0, 1]
    for (title, score) in titles.iter().zip(&scores) {
        assert!(
            (0.0..=1.0).contains(score),
            "{} score should be in [0,1], got {}",
            title,
            score
        );
    }

    // Alpha (vec=1.0, has "language") should rank first
    assert_eq!(
        titles[0], "Alpha",
        "Alpha should rank first (perfect vector + language FTS)"
    );
    assert!(
        scores[0] > scores[2],
        "Alpha ({}) should score higher than third-place ({})",
        scores[0],
        scores[2]
    );

    // Gamma (orthogonal, no "rust") should score lowest
    assert_eq!(titles[3], "Gamma", "Gamma should rank last");
    assert!(
        scores[3].abs() < 1e-5,
        "Gamma should score ~0 (no vector sim + no FTS match), got {}",
        scores[3]
    );

    Ok(())
}

#[tokio::test]
async fn test_similar_to_multi_source_weighted() -> Result<()> {
    let db = setup_doc_db().await?;

    // 80% vector, 20% FTS
    let result = db
        .session()
        .query(
            "MATCH (d:Doc) \
             RETURN d.title AS title, \
             similar_to([d.embedding, d.content], [[1.0, 0.0, 0.0], 'rust'], \
             {method: 'weighted', weights: [0.8, 0.2]}) AS score \
             ORDER BY score DESC",
        )
        .await?;

    assert_eq!(result.len(), 4);

    let titles: Vec<String> = result
        .rows()
        .iter()
        .map(|r| r.get::<String>("title").unwrap())
        .collect();
    let scores: Vec<f64> = result
        .rows()
        .iter()
        .map(|r| r.get::<f64>("score").unwrap())
        .collect();

    for (title, score) in titles.iter().zip(&scores) {
        assert!(
            (0.0..=1.0).contains(score),
            "{} score should be in [0,1], got {}",
            title,
            score
        );
    }

    // Alpha should rank first (highest vector + has "rust")
    assert_eq!(titles[0], "Alpha");

    // Beta (vec=0.6, no FTS) → score = 0.8*0.6 + 0.2*0 = 0.48
    let beta_idx = titles.iter().position(|t| t == "Beta").unwrap();
    assert!(
        (scores[beta_idx] - 0.48).abs() < 0.05,
        "Beta score should be ~0.48 (0.8*0.6), got {}",
        scores[beta_idx]
    );

    // Gamma (vec=0, no FTS) → score = 0
    let gamma_idx = titles.iter().position(|t| t == "Gamma").unwrap();
    assert!(
        scores[gamma_idx].abs() < 1e-5,
        "Gamma score should be ~0, got {}",
        scores[gamma_idx]
    );

    Ok(())
}

#[tokio::test]
async fn test_similar_to_multi_source_weighted_fts_heavy() -> Result<()> {
    let db = setup_doc_db().await?;

    // Vector-heavy: 80% vector, 20% FTS
    let result_vh = db
        .session()
        .query(
            "MATCH (d:Doc) \
             RETURN d.title AS title, \
             similar_to([d.embedding, d.content], [[1.0, 0.0, 0.0], 'rust'], \
             {method: 'weighted', weights: [0.8, 0.2]}) AS score \
             ORDER BY score DESC",
        )
        .await?;

    // FTS-heavy: 20% vector, 80% FTS
    let result_fh = db
        .session()
        .query(
            "MATCH (d:Doc) \
             RETURN d.title AS title, \
             similar_to([d.embedding, d.content], [[1.0, 0.0, 0.0], 'rust'], \
             {method: 'weighted', weights: [0.2, 0.8]}) AS score \
             ORDER BY score DESC",
        )
        .await?;

    assert_eq!(result_vh.len(), 4);
    assert_eq!(result_fh.len(), 4);

    let titles_vh: Vec<String> = result_vh
        .rows()
        .iter()
        .map(|r| r.get::<String>("title").unwrap())
        .collect();
    let scores_vh: Vec<f64> = result_vh
        .rows()
        .iter()
        .map(|r| r.get::<f64>("score").unwrap())
        .collect();
    let titles_fh: Vec<String> = result_fh
        .rows()
        .iter()
        .map(|r| r.get::<String>("title").unwrap())
        .collect();
    let scores_fh: Vec<f64> = result_fh
        .rows()
        .iter()
        .map(|r| r.get::<f64>("score").unwrap())
        .collect();

    for score in scores_vh.iter().chain(&scores_fh) {
        assert!(
            (0.0..=1.0).contains(score),
            "score should be in [0,1], got {}",
            score
        );
    }

    // Weights change scores: vector-heavy gives Beta 0.8*0.6 = 0.48,
    // FTS-heavy gives Beta 0.2*0.6 = 0.12. Scores must differ.
    let beta_vh = titles_vh.iter().position(|t| t == "Beta").unwrap();
    let beta_fh = titles_fh.iter().position(|t| t == "Beta").unwrap();
    assert!(
        (scores_vh[beta_vh] - scores_fh[beta_fh]).abs() > 0.1,
        "different weights should produce different Beta scores: vh={}, fh={}",
        scores_vh[beta_vh],
        scores_fh[beta_fh]
    );

    Ok(())
}

// ── Phase 7: Options Map Tests ────────────────────────────────────────────────

#[tokio::test]
async fn test_similar_to_fts_k_option() -> Result<()> {
    let db = setup_doc_db().await?;

    // Low fts_k → scores saturate faster (higher normalized scores)
    // High fts_k → scores saturate slower (lower normalized scores)
    let result = db
        .session()
        .query(
            "MATCH (d:Doc) \
             RETURN d.title AS title, \
             similar_to(d.content, 'systems', {fts_k: 0.1}) AS score_low_k, \
             similar_to(d.content, 'systems', {fts_k: 10.0}) AS score_high_k",
        )
        .await?;

    assert_eq!(result.len(), 4);

    for row in result.rows() {
        let title: String = row.get("title")?;
        let score_low_k: f64 = row.get("score_low_k")?;
        let score_high_k: f64 = row.get("score_high_k")?;

        assert!(
            (0.0..=1.0).contains(&score_low_k),
            "{} score_low_k should be in [0,1], got {}",
            title,
            score_low_k
        );
        assert!(
            (0.0..=1.0).contains(&score_high_k),
            "{} score_high_k should be in [0,1], got {}",
            title,
            score_high_k
        );

        match title.as_str() {
            // Alpha ("rust systems ...") and Gamma ("database storage engine systems")
            // contain "systems"
            "Alpha" | "Gamma" => {
                assert!(
                    score_low_k > score_high_k,
                    "{}: low fts_k ({}) should give higher score than high fts_k ({})",
                    title,
                    score_low_k,
                    score_high_k
                );
                assert!(
                    score_low_k > 0.0 && score_high_k > 0.0,
                    "{}: both scores should be > 0",
                    title
                );
            }
            // Beta ("python ...") and Delta ("rust memory ...") don't contain "systems"
            "Beta" | "Delta" => {
                assert!(
                    score_low_k.abs() < 1e-5 && score_high_k.abs() < 1e-5,
                    "{}: no 'systems' match, scores should be ~0, got low={} high={}",
                    title,
                    score_low_k,
                    score_high_k
                );
            }
            _ => panic!("unexpected title: {}", title),
        }
    }

    Ok(())
}

#[tokio::test]
async fn test_similar_to_multi_source_in_where() -> Result<()> {
    let db = setup_doc_db().await?;

    // Multi-source fusion in WHERE clause for filtering
    let result = db
        .session()
        .query(
            "MATCH (d:Doc) \
             WHERE similar_to([d.embedding, d.content], [[1.0, 0.0, 0.0], 'rust'], \
             {method: 'weighted', weights: [0.5, 0.5]}) > 0.4 \
             RETURN d.title AS title ORDER BY title",
        )
        .await?;

    let titles: Vec<String> = result
        .rows()
        .iter()
        .map(|r| r.get::<String>("title").unwrap())
        .collect();

    // Alpha (vec=1.0, has rust) → 0.5*1.0 + 0.5*fts > 0.5 → passes
    // Delta (vec=0.8, has rust) → 0.5*0.8 + 0.5*fts > 0.4 → passes
    // Beta (vec=0.6, no rust) → 0.5*0.6 + 0 = 0.3 → fails
    // Gamma (vec=0, no rust) → 0.0 → fails
    assert_eq!(titles, vec!["Alpha", "Delta"]);

    Ok(())
}

// ── Phase 8: Additional Validation Tests ──────────────────────────────────────

#[tokio::test]
async fn test_similar_to_queries_length_mismatch() -> Result<()> {
    let db = setup_doc_db().await?;

    // 2 sources (embedding + content) but queries array has only 1 element
    let err = db
        .session()
        .query(
            "MATCH (d:Doc) \
             RETURN similar_to([d.embedding, d.content], [[1.0, 0.0, 0.0]]) AS score",
        )
        .await;

    assert!(err.is_err(), "query list length mismatch should error");
    let msg = err.unwrap_err().to_string();
    assert!(
        msg.contains("length") || msg.contains("similar_to"),
        "error should mention length mismatch, got: {}",
        msg
    );

    Ok(())
}

#[tokio::test]
async fn test_similar_to_multi_source_type_mismatch() -> Result<()> {
    let db = setup_doc_db().await?;

    // Source 1: d.embedding (vector) + [1,0,0] (vector) → OK
    // Source 2: d.content (FTS string) + [0.5,0.5,0] (vector) → type mismatch
    let err = db
        .session()
        .query(
            "MATCH (d:Doc) \
             RETURN similar_to([d.embedding, d.content], \
             [[1.0, 0.0, 0.0], [0.5, 0.5, 0.0]]) AS score",
        )
        .await;

    assert!(err.is_err(), "FTS source with vector query should error");
    let msg = err.unwrap_err().to_string();
    assert!(
        msg.contains("FTS") || msg.contains("similar_to") || msg.contains("vector"),
        "error should mention type mismatch, got: {}",
        msg
    );

    Ok(())
}

// ── Phase 9: Locy FOLD Integration ────────────────────────────────────────────

#[tokio::test]
async fn test_similar_to_in_locy_fold() -> Result<()> {
    let db = Uni::in_memory().build().await?;

    db.schema()
        .label("Doc")
        .property("title", DataType::String)
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
        .edge_type("LINKS", &["Doc"], &["Doc"])
        .done()
        .apply()
        .await?;

    // Alpha [1,0,0], Beta [0.6,0.8,0], Gamma [0,0,1], Delta [0.8,0.6,0]
    // Edges: Alpha→Beta, Alpha→Delta, Beta→Gamma
    {
        let s = db.session();
        let tx = s.tx().await?;
        tx.execute(
            "CREATE (a:Doc {title: 'Alpha', embedding: [1.0, 0.0, 0.0]}), \
         (b:Doc {title: 'Beta', embedding: [0.6, 0.8, 0.0]}), \
         (g:Doc {title: 'Gamma', embedding: [0.0, 0.0, 1.0]}), \
         (d:Doc {title: 'Delta', embedding: [0.8, 0.6, 0.0]}), \
         (a)-[:LINKS]->(b), (a)-[:LINKS]->(d), (b)-[:LINKS]->(g)",
        )
        .await?;
        tx.commit().await?;
    }
    db.flush().await?;

    // Rule 1: compute per-edge similarity scores
    // Rule 2: FOLD MAX to find best neighbor score per source node
    // Note: "best" is a reserved keyword in Locy (BEST BY), so use "max_sim"
    let result = db
        .session()
        .locy(
            "CREATE RULE scored_neighbors AS \
             MATCH (a:Doc)-[:LINKS]->(b:Doc) \
             YIELD KEY a, KEY b, similar_to(b.embedding, [1.0, 0.0, 0.0]) AS sim \n\
             CREATE RULE best_neighbor AS \
             MATCH (a:Doc) \
             WHERE a IS scored_neighbors TO b \
             FOLD max_sim = MAX(sim) \
             YIELD KEY a, max_sim",
        )
        .await?;

    let best = result
        .derived_rules()
        .await
        .unwrap()
        .get("best_neighbor")
        .expect("rule 'best_neighbor' missing");

    // Alpha links to Beta (sim=0.6) and Delta (sim=0.8) → MAX = 0.8
    // Beta links to Gamma (sim=0.0) → MAX = 0.0
    assert!(
        best.len() >= 2,
        "expected at least 2 best_neighbor facts, got {}",
        best.len()
    );

    for fact in best {
        if let Some(uni_common::Value::Float(s)) = fact.get("max_sim") {
            assert!(
                (0.0..=1.0).contains(s),
                "max_sim score should be in [0,1], got {}",
                s
            );
        }
    }

    Ok(())
}

#[tokio::test]
async fn test_similar_to_weights_sum_not_one() -> Result<()> {
    let db = setup_doc_db().await?;

    // Weights [0.8, 0.8] sum to 1.6, not 1.0 → should error
    let err = db
        .session()
        .query(
            "MATCH (d:Doc) \
             RETURN similar_to([d.embedding, d.content], [[1.0, 0.0, 0.0], 'rust'], \
             {method: 'weighted', weights: [0.8, 0.8]}) AS score",
        )
        .await;

    assert!(err.is_err(), "weights not summing to 1.0 should error");
    let msg = err.unwrap_err().to_string();
    assert!(
        msg.contains("weights") || msg.contains("sum to 1.0"),
        "error should mention weights sum, got: {}",
        msg
    );

    Ok(())
}

// ── Phase 10: Auto-Embed Tests ────────────────────────────────────────────────

/// Tests requiring a real embedding model. Feature-gated and `#[ignore]`d
/// because they require model downloads from HuggingFace Hub.
#[cfg(feature = "provider-onnx")]
mod auto_embed_tests {
    use super::*;
    use serde_json::json;
    use uni_db::api::schema::{EmbeddingCfg, IndexType, VectorAlgo, VectorIndexCfg, VectorMetric};
    use uni_xervo::api::{ModelAliasSpec, ModelTask, WarmupPolicy};

    fn nomic_embed_alias(alias: &str) -> ModelAliasSpec {
        ModelAliasSpec {
            alias: alias.to_string(),
            task: ModelTask::Embed,
            provider_id: "local/onnx".to_string(),
            model_id: "NomicEmbedTextV15".to_string(),
            revision: None,
            warmup: WarmupPolicy::Lazy,
            required: false,
            timeout: None,
            load_timeout: None,
            retry: None,
            options: json!({}),
        }
    }

    /// Test similar_to(d.embedding, 'query text') where the string query is
    /// auto-embedded at query time via the Xervo runtime.
    #[tokio::test]
    #[ignore] // Requires model download from HuggingFace Hub
    async fn test_similar_to_auto_embed_string_query() -> Result<()> {
        let db = Uni::temporary()
            .xervo_catalog(vec![nomic_embed_alias("embed/default")])
            .build()
            .await?;

        db.schema()
            .label("Article")
            .property("title", DataType::String)
            .property("body", DataType::String)
            .vector("embedding", 768)
            .index(
                "embedding",
                IndexType::Vector(VectorIndexCfg {
                    algorithm: VectorAlgo::Flat,
                    metric: VectorMetric::Cosine,
                    embedding: Some(EmbeddingCfg {
                        alias: "embed/default".to_string(),
                        source_properties: vec!["title".to_string(), "body".to_string()],
                        batch_size: 32,
                        document_prefix: None,
                        query_prefix: None,
                    }),
                }),
            )
            .done()
            .apply()
            .await?;

        // Insert articles — auto-embed on write fills in embedding vectors.
        let tx = db.session().tx().await?;
        tx.execute(
            "CREATE (:Article {title: 'Rust Guide', \
             body: 'Rust is a systems programming language focused on safety and performance'})",
        )
        .await?;
        tx.execute(
            "CREATE (:Article {title: 'Python Intro', \
             body: 'Python is an interpreted language popular for data science and scripting'})",
        )
        .await?;
        tx.execute(
            "CREATE (:Article {title: 'Graph Databases', \
             body: 'Graph databases store data as nodes and edges for relationship queries'})",
        )
        .await?;
        tx.commit().await?;
        db.flush().await?;

        // Key test: similar_to with a STRING query triggers auto-embed at query time
        let result = db
            .session()
            .query(
                "MATCH (a:Article) \
                 RETURN a.title AS title, \
                 similar_to(a.embedding, 'systems programming language') AS score \
                 ORDER BY score DESC",
            )
            .await?;

        assert_eq!(result.len(), 3);

        // The Rust article should score highest for 'systems programming language'
        let first_title: String = result.rows()[0].get("title")?;
        assert_eq!(
            first_title, "Rust Guide",
            "Rust article should rank first for 'systems programming language'"
        );

        // All scores should be in [0, 1]
        for row in result.rows() {
            let score: f64 = row.get("score")?;
            assert!(
                (0.0..=1.0).contains(&score),
                "score should be in [0,1], got {}",
                score
            );
        }

        Ok(())
    }

    /// Test similar_to with auto-embed in a WHERE clause for threshold filtering.
    #[tokio::test]
    #[ignore] // Requires model download from HuggingFace Hub
    async fn test_similar_to_auto_embed_in_where() -> Result<()> {
        let db = Uni::temporary()
            .xervo_catalog(vec![nomic_embed_alias("embed/default")])
            .build()
            .await?;

        db.schema()
            .label("Article")
            .property("title", DataType::String)
            .property("body", DataType::String)
            .vector("embedding", 768)
            .index(
                "embedding",
                IndexType::Vector(VectorIndexCfg {
                    algorithm: VectorAlgo::Flat,
                    metric: VectorMetric::Cosine,
                    embedding: Some(EmbeddingCfg {
                        alias: "embed/default".to_string(),
                        source_properties: vec!["title".to_string(), "body".to_string()],
                        batch_size: 32,
                        document_prefix: None,
                        query_prefix: None,
                    }),
                }),
            )
            .done()
            .apply()
            .await?;

        let tx = db.session().tx().await?;
        tx.execute(
            "CREATE (:Article {title: 'Rust Guide', \
             body: 'Rust is a systems programming language focused on safety and performance'})",
        )
        .await?;
        tx.execute(
            "CREATE (:Article {title: 'Cooking Tips', \
             body: 'Learn how to make pasta carbonara with fresh ingredients'})",
        )
        .await?;
        tx.commit().await?;
        db.flush().await?;

        // Auto-embed 'programming' and filter by similarity threshold
        let result = db
            .session()
            .query(
                "MATCH (a:Article) \
                 WHERE similar_to(a.embedding, 'programming') > 0.5 \
                 RETURN a.title AS title",
            )
            .await?;

        // Rust article should pass the threshold; Cooking should not
        let titles: Vec<String> = result
            .rows()
            .iter()
            .map(|r| r.get::<String>("title").unwrap())
            .collect();
        assert!(
            titles.contains(&"Rust Guide".to_string()),
            "Rust article should pass similarity threshold for 'programming'"
        );
        assert!(
            !titles.contains(&"Cooking Tips".to_string()),
            "Cooking article should not match 'programming'"
        );

        Ok(())
    }
}

// ── Phase 11: Distance Metric Tests ──────────────────────────────────────────

mod metric_tests {
    use super::*;

    /// Create a DB with a vector index using the specified metric.
    async fn setup_metric_db(metric: VectorMetric) -> Result<Uni> {
        let db = Uni::in_memory().build().await?;

        db.schema()
            .label("Item")
            .property("name", DataType::String)
            .vector("vec", 3)
            .index(
                "vec",
                IndexType::Vector(VectorIndexCfg {
                    algorithm: VectorAlgo::Flat,
                    metric,
                    embedding: None,
                }),
            )
            .done()
            .apply()
            .await?;

        // Unit-length vectors for predictable scores
        {
            let s = db.session();
            let tx = s.tx().await?;
            tx.execute("CREATE (:Item {name: 'A', vec: [1.0, 0.0, 0.0]})")
                .await?;
            tx.execute("CREATE (:Item {name: 'B', vec: [0.0, 1.0, 0.0]})")
                .await?;
            tx.execute("CREATE (:Item {name: 'C', vec: [0.8, 0.6, 0.0]})")
                .await?;
            tx.commit().await?;
        }
        db.flush().await?;
        Ok(db)
    }

    // ── L2 ──

    #[tokio::test]
    async fn test_similar_to_l2_metric() -> Result<()> {
        let db = setup_metric_db(VectorMetric::L2).await?;

        let result = db
            .session()
            .query(
                "MATCH (i:Item) \
                 RETURN i.name AS name, similar_to(i.vec, [1.0, 0.0, 0.0]) AS score \
                 ORDER BY score DESC",
            )
            .await?;

        assert_eq!(result.len(), 3);

        let names: Vec<String> = result
            .rows()
            .iter()
            .map(|r| r.get::<String>("name").unwrap())
            .collect();
        let scores: Vec<f64> = result
            .rows()
            .iter()
            .map(|r| r.get::<f64>("score").unwrap())
            .collect();

        // A: identical → distance=0, score=1/(1+0)=1.0
        assert_eq!(names[0], "A");
        assert!((scores[0] - 1.0).abs() < 1e-5, "A score: {}", scores[0]);

        // C: [0.8,0.6,0] → d²=(0.2²+0.6²)=0.04+0.36=0.4, score=1/(1+0.4)≈0.714
        assert_eq!(names[1], "C");
        assert!(
            (scores[1] - 1.0 / 1.4).abs() < 1e-4,
            "C score: {} (expected ~0.714)",
            scores[1]
        );

        // B: [0,1,0] → d²=(1²+1²)=2, score=1/(1+2)≈0.333
        assert_eq!(names[2], "B");
        assert!(
            (scores[2] - 1.0 / 3.0).abs() < 1e-4,
            "B score: {} (expected ~0.333)",
            scores[2]
        );

        Ok(())
    }

    // ── Dot ──

    #[tokio::test]
    async fn test_similar_to_dot_metric() -> Result<()> {
        let db = setup_metric_db(VectorMetric::Dot).await?;

        let result = db
            .session()
            .query(
                "MATCH (i:Item) \
                 RETURN i.name AS name, similar_to(i.vec, [1.0, 0.0, 0.0]) AS score \
                 ORDER BY score DESC",
            )
            .await?;

        assert_eq!(result.len(), 3);

        let names: Vec<String> = result
            .rows()
            .iter()
            .map(|r| r.get::<String>("name").unwrap())
            .collect();
        let scores: Vec<f64> = result
            .rows()
            .iter()
            .map(|r| r.get::<f64>("score").unwrap())
            .collect();

        // A: [1,0,0]·[1,0,0] = 1.0
        assert_eq!(names[0], "A");
        assert!((scores[0] - 1.0).abs() < 1e-5, "A score: {}", scores[0]);

        // C: [0.8,0.6,0]·[1,0,0] = 0.8
        assert_eq!(names[1], "C");
        assert!((scores[1] - 0.8).abs() < 1e-5, "C score: {}", scores[1]);

        // B: [0,1,0]·[1,0,0] = 0.0
        assert_eq!(names[2], "B");
        assert!(scores[2].abs() < 1e-5, "B score: {}", scores[2]);

        Ok(())
    }

    // ── Cosine unchanged ──

    #[tokio::test]
    async fn test_similar_to_cosine_unchanged() -> Result<()> {
        let db = setup_metric_db(VectorMetric::Cosine).await?;

        let result = db
            .session()
            .query(
                "MATCH (i:Item) \
                 RETURN i.name AS name, similar_to(i.vec, [1.0, 0.0, 0.0]) AS score \
                 ORDER BY score DESC",
            )
            .await?;

        assert_eq!(result.len(), 3);

        let scores: Vec<f64> = result
            .rows()
            .iter()
            .map(|r| r.get::<f64>("score").unwrap())
            .collect();

        // Cosine similarity for unit vectors equals dot product
        assert!((scores[0] - 1.0).abs() < 1e-5, "A: {}", scores[0]);
        assert!((scores[1] - 0.8).abs() < 1e-5, "C: {}", scores[1]);
        assert!(scores[2].abs() < 1e-5, "B: {}", scores[2]);

        Ok(())
    }

    // ── Multi-source with mixed metrics ──

    #[tokio::test]
    async fn test_similar_to_multi_source_mixed_metrics() -> Result<()> {
        let db = Uni::in_memory().build().await?;

        db.schema()
            .label("Item")
            .property("name", DataType::String)
            .vector("vec_cos", 3)
            .vector("vec_l2", 3)
            .index(
                "vec_cos",
                IndexType::Vector(VectorIndexCfg {
                    algorithm: VectorAlgo::Flat,
                    metric: VectorMetric::Cosine,
                    embedding: None,
                }),
            )
            .index(
                "vec_l2",
                IndexType::Vector(VectorIndexCfg {
                    algorithm: VectorAlgo::Flat,
                    metric: VectorMetric::L2,
                    embedding: None,
                }),
            )
            .done()
            .apply()
            .await?;

        {
            let s = db.session();
            let tx = s.tx().await?;
            tx.execute(
                "CREATE (:Item {name: 'A', vec_cos: [1.0, 0.0, 0.0], vec_l2: [1.0, 0.0, 0.0]})",
            )
            .await?;
            tx.execute(
                "CREATE (:Item {name: 'B', vec_cos: [0.0, 1.0, 0.0], vec_l2: [0.0, 1.0, 0.0]})",
            )
            .await?;
            tx.commit().await?;
        }
        db.flush().await?;

        let result = db
            .session()
            .query(
                "MATCH (i:Item) \
                 RETURN i.name AS name, \
                        similar_to([i.vec_cos, i.vec_l2], [[1.0, 0.0, 0.0], [1.0, 0.0, 0.0]], \
                                   {method: 'weighted', weights: [0.5, 0.5]}) AS score \
                 ORDER BY score DESC",
            )
            .await?;

        assert_eq!(result.len(), 2);

        let names: Vec<String> = result
            .rows()
            .iter()
            .map(|r| r.get::<String>("name").unwrap())
            .collect();
        let scores: Vec<f64> = result
            .rows()
            .iter()
            .map(|r| r.get::<f64>("score").unwrap())
            .collect();

        // A: cosine=1.0, L2=1.0 → weighted=1.0
        assert_eq!(names[0], "A");
        assert!((scores[0] - 1.0).abs() < 1e-4, "A: {}", scores[0]);

        // B: cosine=0.0, L2=1/(1+2)≈0.333 → weighted≈0.167
        assert_eq!(names[1], "B");
        let expected_b = 0.5 * 0.0 + 0.5 * (1.0 / 3.0);
        assert!(
            (scores[1] - expected_b).abs() < 1e-3,
            "B: {} (expected ~{})",
            scores[1],
            expected_b
        );

        Ok(())
    }

    // ── RRF warning ──

    #[tokio::test]
    async fn test_similar_to_rrf_emits_warning() -> Result<()> {
        let db = Uni::in_memory().build().await?;

        db.schema()
            .label("Doc")
            .property("title", DataType::String)
            .property("content", DataType::String)
            .vector("embedding", 3)
            .index(
                "embedding",
                IndexType::Vector(VectorIndexCfg {
                    algorithm: VectorAlgo::Flat,
                    metric: VectorMetric::Cosine,
                    embedding: None,
                }),
            )
            .index("content", IndexType::FullText)
            .done()
            .apply()
            .await?;

        let tx = db.session().tx().await?;
        tx.execute(
            "CREATE (:Doc {title: 'Alpha', content: 'rust programming', embedding: [1.0, 0.0, 0.0]})",
        )
        .await?;
        tx.commit().await?;
        db.flush().await?;

        // Multi-source similar_to with default RRF fusion
        let result = db
            .session()
            .query(
                "MATCH (d:Doc) \
                 RETURN d.title AS title, \
                        similar_to([d.embedding, d.content], [[1.0, 0.0, 0.0], 'rust']) AS score",
            )
            .await?;

        assert_eq!(result.len(), 1);
        assert!(
            result.has_warnings(),
            "Expected RrfPointContext warning but got none"
        );

        let has_rrf_warning = result
            .warnings()
            .iter()
            .any(|w| matches!(w, uni_db::QueryWarning::RrfPointContext));
        assert!(
            has_rrf_warning,
            "Expected RrfPointContext warning, got: {:?}",
            result.warnings()
        );

        Ok(())
    }
}

// ── Regression: Bug #9 — similar_to must null-propagate, not abort the query ──

/// A NULL `similar_to` source must yield NULL for that row, not error the query.
///
/// Bug #9: `determine_scoring_mode()` in `similar_to_expr.rs` has no `Value::Null`
/// arm, so a NULL source falls through to the catch-all
/// "unsupported source/query type combination" error. Because the scoring modes
/// are derived from the first batch row via `.collect::<Result<_>>()?`, a single
/// NULL source aborts the ENTIRE query instead of producing a NULL score for the
/// offending row (3VL-correct behavior).
///
/// The NULL embedding is injected via `OPTIONAL MATCH`: a Doc with no outgoing
/// `:NEXT` edge leaves the optional node `n` unbound, so `n.embedding` evaluates
/// to `Value::Null` and reaches the `similar_to` UDF. The NULL-source Doc sorts
/// first (`Aardvark`) so the missing `Value::Null` arm in `determine_scoring_mode`
/// is exercised on the first row (the mode-determination path itself).
///
/// RED today: the query returns
/// `Err(... "unsupported source/query type combination" ...)`, so both the
/// `is_ok()` assertion and the NULL-score assertion below fail.
#[tokio::test]
async fn similar_to_null_embedding_yields_null_not_error() -> Result<()> {
    let db = Uni::in_memory().build().await?;

    db.schema()
        .label("Doc")
        .property("title", DataType::String)
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
        .edge_type("NEXT", &["Doc"], &["Doc"])
        .done()
        .apply()
        .await?;

    // Aardvark has NO outgoing :NEXT edge (sorts first → NULL source is row 0,
    // exercising the missing NULL arm in determine_scoring_mode directly).
    // Beta -[:NEXT]-> Gamma gives a second row with a bound, real-vector source.
    let tx = db.session().tx().await?;
    tx.execute(
        "CREATE (a:Doc {title: 'Aardvark', embedding: [1.0, 0.0, 0.0]}), \
         (b:Doc {title: 'Beta', embedding: [0.6, 0.8, 0.0]}), \
         (g:Doc {title: 'Gamma', embedding: [0.8, 0.6, 0.0]}), \
         (b)-[:NEXT]->(g)",
    )
    .await?;
    tx.commit().await?;
    db.flush().await?;

    // Row order (ORDER BY title): Aardvark, Beta, Gamma.
    //   Aardvark: no outgoing :NEXT → n unbound → n.embedding is NULL (row 0).
    //   Beta:     n = Gamma → n.embedding = [0.8, 0.6, 0.0] (real vector).
    //   Gamma:    no outgoing :NEXT → n unbound → n.embedding is NULL.
    let result = db
        .session()
        .query(
            "MATCH (d:Doc) \
             OPTIONAL MATCH (d)-[:NEXT]->(n:Doc) \
             RETURN d.title AS title, similar_to(n.embedding, [1.0, 0.0, 0.0]) AS score \
             ORDER BY title",
        )
        .await;

    // The query must SUCCEED — a NULL source is a 3VL NULL, not a hard error.
    assert!(
        result.is_ok(),
        "NULL similar_to source must null-propagate, not abort the query; got: {:?}",
        result.as_ref().err()
    );
    let result = result.unwrap();

    assert_eq!(result.len(), 3, "expected one row per Doc");

    // Aardvark (row 0): NULL source → score must be NULL, not an error.
    let aardvark = &result.rows()[0];
    assert_eq!(aardvark.get::<String>("title")?, "Aardvark");
    let aardvark_score: Option<f64> = aardvark.get("score")?;
    assert!(
        aardvark_score.is_none(),
        "Aardvark's NULL embedding source must yield a NULL score, got {:?}",
        aardvark_score
    );

    // Beta: source n.embedding = Gamma's [0.8, 0.6, 0.0] → numeric cosine score.
    let beta = &result.rows()[1];
    assert_eq!(beta.get::<String>("title")?, "Beta");
    let beta_score: Option<f64> = beta.get("score")?;
    assert!(
        beta_score.is_some(),
        "Beta has a bound neighbour embedding and must get a numeric score, got NULL"
    );

    // Gamma: NULL source → score must be NULL, not an error.
    let gamma = &result.rows()[2];
    assert_eq!(gamma.get::<String>("title")?, "Gamma");
    let gamma_score: Option<f64> = gamma.get("score")?;
    assert!(
        gamma_score.is_none(),
        "Gamma's NULL embedding source must yield a NULL score, got {:?}",
        gamma_score
    );

    Ok(())
}

// ── Regression: Issue #39 — similar_to FTS must work without flush ───────────

/// Verifies that `similar_to()` in FTS mode returns correct scores for data
/// that has been committed but not yet flushed to Lance storage (L0-only data).
///
/// Previously, `fts_search_batch()` did not pass a `QueryContext`, so L0 data
/// was invisible to the FTS search path. The `CALL uni.fts.query()` procedure
/// worked correctly because it passed the query context.
///
/// See: https://github.com/rustic-ai/uni-db/issues/39
#[tokio::test]
async fn test_similar_to_fts_without_flush() -> Result<()> {
    let db = Uni::in_memory().build().await?;

    db.schema()
        .label("Doc")
        .property("title", DataType::String)
        .property("content", DataType::String)
        .index("content", IndexType::FullText)
        .done()
        .apply()
        .await?;

    let session = db.session();
    let tx = session.tx().await?;
    tx.execute("CREATE (:Doc {title: 'Alpha', content: 'rust systems programming language'})")
        .await?;
    tx.execute("CREATE (:Doc {title: 'Beta', content: 'python data science scripting'})")
        .await?;
    tx.commit().await?;
    // Intentionally NO db.flush() — data is in L0 only

    let result = session
        .query(
            "MATCH (d:Doc) \
             RETURN d.title AS title, similar_to(d.content, 'rust') AS score \
             ORDER BY score DESC",
        )
        .await?;

    assert_eq!(result.len(), 2);

    let top_title: String = result.rows()[0].get("title")?;
    let top_score: f64 = result.rows()[0].get("score")?;
    let bot_score: f64 = result.rows()[1].get("score")?;

    assert_eq!(top_title, "Alpha");
    assert!(
        top_score > 0.0,
        "Alpha contains 'rust' but similar_to returned {top_score} (L0-only, no flush)"
    );
    assert!(
        bot_score.abs() < 1e-5,
        "Beta doesn't contain 'rust' but got {bot_score}"
    );

    Ok(())
}
