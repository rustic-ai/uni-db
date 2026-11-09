// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Entity resolution in Locy over mentions extracted by `uni.nlp.entities`
//! (kniv v7 proposal, phase 3).
//!
//! `RESOLVE` is the program `website/docs/features/nlp-extraction.md` shows:
//! keep the two in step. Each `match_evidence` clause is one signal that a
//! mention names an existing entity; `same_as` combines a pair's signals with
//! noisy-OR and applies the merge threshold; `resolved` keeps each mention's
//! best match; `refers_to` writes it as an edge. Mentions left unmatched then
//! name new entities, one per name ignoring case and none for a surname of a
//! longer unmatched name; a second `DERIVE` links them, and a last command
//! gives anything still unmatched an entity of its own.
//!
//! The end-to-end test runs extraction through the mock kniv v7 model of
//! `nlp_extract` (capitalised words are entities) and embeds names with a mock
//! embedder whose vectors are hashed character trigrams, so near-identical
//! names are similar and unrelated names are not.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use anyhow::Result;
use async_trait::async_trait;
use uni_db::locy::CommandResult;
use uni_db::{
    DataType, EmbeddingCfg, IndexType, ModelAliasSpec, ModelTask, Uni, Value, VectorAlgo,
    VectorIndexCfg, VectorMetric, WarmupPolicy,
};
use uni_locy::DerivationNode;
use uni_xervo::runtime::ModelRuntime;
use uni_xervo::traits::{
    EmbedResult, EmbeddingModel, LoadedModelHandle, ModelInfo, ModelProvider, ProviderCapabilities,
    ProviderHealth,
};

use crate::nlp_extract::{MockProvider, spec};

/// The resolution program: the rules, then the commands that write.
const RESOLVE: &str = r#"
CREATE RULE match_evidence AS
MATCH (m:Mention), (e:Entity)
WHERE NOT (m)-[:REFERS_TO]->() AND m.type = e.type
  AND toLower(m.text) = toLower(e.name)
YIELD KEY m, KEY e, 0.9 AS p

CREATE RULE match_evidence AS
MATCH (m:Mention), (e:Entity)
WHERE NOT (m)-[:REFERS_TO]->() AND m.type = e.type
  AND toLower(e.name) ENDS WITH ' ' + toLower(m.text)
YIELD KEY m, KEY e, 0.7 AS p

CREATE RULE match_evidence AS
MATCH (m:Mention), (e:Entity)
WHERE NOT (m)-[:REFERS_TO]->() AND m.type = e.type
  AND similar_to(m.embedding, e.embedding) >= 0.5
YIELD KEY m, KEY e, similar_to(m.embedding, e.embedding) * 0.8 AS p

CREATE RULE same_as AS
MATCH (m:Mention), (e:Entity)
WHERE (m, e, p) IS match_evidence
FOLD prob = MNOR(p)
WHERE prob >= 0.75
YIELD KEY m, KEY e, prob AS PROB

CREATE RULE resolved AS
MATCH (m:Mention), (e:Entity)
WHERE (m, e, prob) IS same_as
BEST BY prob DESC
YIELD KEY m, e, prob

CREATE RULE refers_to AS
MATCH (m:Mention), (e:Entity)
WHERE (m, e) IS resolved
DERIVE (m)-[:REFERS_TO]->(e)

DERIVE refers_to

MATCH (m:Mention) WHERE NOT (m)-[:REFERS_TO]->()
  AND NOT EXISTS {
    MATCH (o:Mention) WHERE NOT (o)-[:REFERS_TO]->() AND o.type = m.type
      AND (toLower(o.text) ENDS WITH ' ' + toLower(m.text)
           OR (toLower(o.text) = toLower(m.text) AND o.text < m.text))
  }
MERGE (e:Entity {name: m.text, type: m.type})

DERIVE refers_to

MATCH (m:Mention) WHERE NOT (m)-[:REFERS_TO]->()
MERGE (e:Entity {name: m.text, type: m.type})
CREATE (m)-[:REFERS_TO]->(e)
"#;

const DIM: usize = 64;

/// Hashed, L2-normalised character trigrams of the lower-cased text.
fn trigram_vector(text: &str) -> Vec<f32> {
    let chars: Vec<char> = format!(" {} ", text.to_lowercase()).chars().collect();
    let mut v = vec![0.0f32; DIM];
    for w in chars.windows(3) {
        let h = w
            .iter()
            .fold(2166136261u32, |h, c| (h ^ *c as u32).wrapping_mul(16777619));
        v[h as usize % DIM] += 1.0;
    }
    let norm = v
        .iter()
        .map(|x| x * x)
        .sum::<f32>()
        .sqrt()
        .max(f32::EPSILON);
    v.iter().map(|x| x / norm).collect()
}

struct TrigramEmbedder;

impl ModelInfo for TrigramEmbedder {
    fn model_id(&self) -> &str {
        "mock/trigram"
    }
}

#[async_trait]
impl EmbeddingModel for TrigramEmbedder {
    async fn embed(&self, texts: &[&str]) -> uni_xervo::error::Result<EmbedResult> {
        Ok(EmbedResult {
            vectors: texts.iter().map(|t| trigram_vector(t)).collect(),
            usage: None,
        })
    }

    fn dimensions(&self) -> u32 {
        DIM as u32
    }
}

struct TrigramProvider;

#[async_trait]
impl ModelProvider for TrigramProvider {
    fn provider_id(&self) -> &'static str {
        "mock/trigram"
    }

    fn capabilities(&self) -> ProviderCapabilities {
        ProviderCapabilities {
            supported_tasks: vec![ModelTask::Embed],
        }
    }

    async fn load(&self, _spec: &ModelAliasSpec) -> uni_xervo::error::Result<LoadedModelHandle> {
        let model: Arc<dyn EmbeddingModel> = Arc::new(TrigramEmbedder);
        Ok(Arc::new(model) as LoadedModelHandle)
    }

    async fn health(&self) -> ProviderHealth {
        ProviderHealth::Healthy
    }
}

fn embed_spec() -> ModelAliasSpec {
    ModelAliasSpec {
        alias: "embed/names".into(),
        task: ModelTask::Embed,
        provider_id: "mock/trigram".into(),
        model_id: "trigram".into(),
        revision: None,
        warmup: WarmupPolicy::Lazy,
        required: false,
        timeout: None,
        load_timeout: None,
        retry: None,
        options: serde_json::json!({}),
    }
}

fn name_index(source: &str) -> IndexType {
    IndexType::Vector(VectorIndexCfg {
        algorithm: VectorAlgo::Flat,
        metric: VectorMetric::Cosine,
        embedding: Some(EmbeddingCfg {
            alias: "embed/names".into(),
            source_properties: vec![source.into()],
            batch_size: 16,
            document_prefix: None,
            query_prefix: None,
        }),
    })
}

/// A database with the mock NLP and embedding models and the `Entity` /
/// `Mention` / `Chunk` schema the docs use, names embedded on write.
async fn db_with_models() -> Result<Uni> {
    let runtime = ModelRuntime::builder()
        .register_provider(MockProvider {
            seen: Arc::new(Mutex::new(None)),
        })
        .register_provider(TrigramProvider)
        .catalog(vec![spec("nlp/default", "mock-v7"), embed_spec()])
        .build()
        .await?;
    let db = Uni::temporary().xervo_runtime(runtime).build().await?;
    db.schema()
        .label("Entity")
        .property("name", DataType::String)
        .property("type", DataType::String)
        .vector("embedding", DIM)
        .index("embedding", name_index("name"))
        .done()
        .label("Mention")
        .property("text", DataType::String)
        .property("type", DataType::String)
        .vector("embedding", DIM)
        .index("embedding", name_index("text"))
        .done()
        .label("Chunk")
        .property("text", DataType::String)
        .done()
        .apply()
        .await?;
    Ok(db)
}

/// `mention text -> entity name`, over every `REFERS_TO` edge.
async fn links(db: &Uni) -> Result<Vec<(String, String)>> {
    let rows = db
        .session()
        .query(
            "MATCH (m:Mention)-[:REFERS_TO]->(e:Entity) \
             RETURN m.text AS m, e.name AS e ORDER BY m, e",
        )
        .await?;
    rows.rows()
        .iter()
        .map(|r| Ok((r.get::<String>("m")?, r.get::<String>("e")?)))
        .collect()
}

async fn count(db: &Uni, query: &str) -> Result<i64> {
    Ok(db.session().query(query).await?.rows()[0].get::<i64>("n")?)
}

async fn resolve(db: &Uni) -> Result<()> {
    let tx = db.session().tx().await?;
    tx.locy(RESOLVE).await?;
    tx.commit().await?;
    Ok(())
}

fn text_of(node: &DerivationNode, var: &str) -> Option<String> {
    match node.bindings.get(var) {
        Some(Value::Node(n)) => n
            .properties
            .get("text")
            .or_else(|| n.properties.get("name"))
            .and_then(Value::as_str)
            .map(str::to_owned),
        _ => None,
    }
}

#[tokio::test]
async fn extracted_mentions_resolve_to_existing_entities() -> Result<()> {
    let db = db_with_models().await?;
    let tx = db.session().tx().await?;
    tx.execute(
        "CREATE (:Entity {name: 'Ada Lovelace', type: 'person'}), \
                (:Entity {name: 'Charles Babbage', type: 'person'}), \
                (:Chunk {text: 'notes by Lovelace on the engine'}), \
                (:Chunk {text: 'later Babbage and Hopper spoke'})",
    )
    .await?;
    tx.commit().await?;

    let tx = db.session().tx().await?;
    tx.execute(
        "MATCH (c:Chunk) \
         CALL uni.nlp.entities(c.text, {entity_types: ['person']}) \
         YIELD text, entity_type \
         CREATE (c)-[:MENTIONS]->(:Mention {text: text, type: entity_type})",
    )
    .await?;
    tx.commit().await?;
    assert_eq!(
        count(&db, "MATCH (m:Mention) RETURN count(m) AS n").await?,
        3
    );

    resolve(&db).await?;
    let expected = vec![
        ("Babbage".to_owned(), "Charles Babbage".to_owned()),
        ("Hopper".to_owned(), "Hopper".to_owned()),
        ("Lovelace".to_owned(), "Ada Lovelace".to_owned()),
    ];
    assert_eq!(links(&db).await?, expected);
    // Hopper matched nothing and got an entity of its own, embedded on write.
    assert_eq!(
        count(&db, "MATCH (e:Entity) RETURN count(e) AS n").await?,
        3
    );
    assert_eq!(
        count(
            &db,
            "MATCH (e:Entity {name: 'Hopper'}) WHERE e.embedding IS NOT NULL \
             RETURN count(e) AS n"
        )
        .await?,
        1
    );

    // Resolution is idempotent: resolved mentions are left alone.
    resolve(&db).await?;
    assert_eq!(links(&db).await?, expected);
    assert_eq!(
        count(&db, "MATCH (e:Entity) RETURN count(e) AS n").await?,
        3
    );

    // A later chunk naming Hopper again joins the entity the first one made.
    let tx = db.session().tx().await?;
    tx.execute(
        "CREATE (c:Chunk {text: 'a talk by Hopper'}) WITH c \
         CALL uni.nlp.entities(c.text, {entity_types: ['person']}) \
         YIELD text, entity_type \
         CREATE (c)-[:MENTIONS]->(:Mention {text: text, type: entity_type})",
    )
    .await?;
    tx.commit().await?;
    resolve(&db).await?;
    assert_eq!(
        count(
            &db,
            "MATCH (:Mention {text: 'Hopper'})-[:REFERS_TO]->(e:Entity {name: 'Hopper'}) \
             RETURN count(e) AS n"
        )
        .await?,
        2
    );
    assert_eq!(
        count(&db, "MATCH (e:Entity) RETURN count(e) AS n").await?,
        3
    );
    Ok(())
}

#[tokio::test]
async fn explain_rule_shows_the_evidence_for_a_merge() -> Result<()> {
    let db = db_with_models().await?;
    let tx = db.session().tx().await?;
    tx.execute(
        "CREATE (:Entity {name: 'Ada Lovelace', type: 'person'}), \
                (:Mention {text: 'Lovelace', type: 'person'})",
    )
    .await?;
    tx.commit().await?;

    let rules = &RESOLVE[..RESOLVE.find("DERIVE refers_to\n\nMATCH").unwrap()];
    let result = db
        .session()
        .locy(&format!(
            "{rules}\nEXPLAIN RULE same_as WHERE m.text = 'Lovelace'"
        ))
        .await?;
    let tree = result
        .command_results()
        .iter()
        .find_map(|c| match c {
            CommandResult::Explain(n) => Some(n.clone()),
            _ => None,
        })
        .expect("an EXPLAIN result");
    let merge = tree
        .children
        .iter()
        .find(|n| n.rule == "same_as" && text_of(n, "e").as_deref() == Some("Ada Lovelace"))
        .expect("same_as(Lovelace, Ada Lovelace)");
    // Two signals: the surname clause (1) and the name-similarity clause (2).
    let mut clauses: Vec<usize> = merge
        .children
        .iter()
        .filter(|n| n.rule == "match_evidence")
        .map(|n| n.clause_index)
        .collect();
    clauses.sort_unstable();
    assert_eq!(clauses, vec![1, 2]);
    Ok(())
}

/// A database of `Entity` / `Mention` nodes with explicit vectors and no
/// embedder.
async fn fixture_db(create: &str) -> Result<Uni> {
    let db = Uni::temporary().build().await?;
    db.schema()
        .label("Entity")
        .property("name", DataType::String)
        .property("type", DataType::String)
        .vector("embedding", 3)
        .done()
        .label("Mention")
        .property("text", DataType::String)
        .property("type", DataType::String)
        .vector("embedding", 3)
        .property_nullable("gold", DataType::String)
        .done()
        .apply()
        .await?;
    let tx = db.session().tx().await?;
    tx.execute(create).await?;
    tx.commit().await?;
    Ok(db)
}

/// Runs `RESOLVE` on a fixture; returns `mention text -> entity name` links.
async fn resolve_fixture(db: &Uni) -> Result<Vec<(String, String)>> {
    // Without an embedder a new entity copies its mention's vector.
    let program = RESOLVE.replace(
        "MERGE (e:Entity {name: m.text, type: m.type})\n",
        "MERGE (e:Entity {name: m.text, type: m.type})\n\
         ON CREATE SET e.embedding = m.embedding\n",
    );
    let tx = db.session().tx().await?;
    tx.locy(&program).await?;
    tx.commit().await?;
    links(db).await
}

#[tokio::test]
async fn the_best_match_wins_and_types_must_agree() -> Result<()> {
    let db = fixture_db(
        "CREATE (:Entity {name: 'Ada Lovelace', type: 'person', embedding: [1.0, 0.0, 0.0]}), \
                (:Entity {name: 'Ada Byron', type: 'person', embedding: [0.96, 0.28, 0.0]}), \
                (:Entity {name: 'Paris', type: 'location', embedding: [0.0, 0.0, 1.0]}), \
                (:Mention {text: 'A. Lovelace', type: 'person', embedding: [0.995, 0.0998, 0.0]}), \
                (:Mention {text: 'Paris', type: 'person', embedding: [0.0, 0.0, 1.0]})",
    )
    .await?;
    // Both Adas clear the merge threshold on similarity alone...
    let rules = &RESOLVE[..RESOLVE.find("DERIVE refers_to\n\nMATCH").unwrap()];
    let candidates = db
        .session()
        .locy(&format!(
            "{rules}\nQUERY same_as WHERE m.text = 'A. Lovelace' RETURN e.name AS e ORDER BY e"
        ))
        .await?;
    let names: Vec<String> = candidates
        .rows()
        .expect("QUERY rows")
        .iter()
        .filter_map(|r| r.get("e").and_then(Value::as_str).map(str::to_owned))
        .collect();
    assert_eq!(names, ["Ada Byron", "Ada Lovelace"]);

    // ...and the closer one wins.
    let found: HashMap<String, String> = resolve_fixture(&db).await?.into_iter().collect();
    assert_eq!(found["A. Lovelace"], "Ada Lovelace");
    // A person named Paris is not the city.
    assert_eq!(found["Paris"], "Paris");
    assert_eq!(
        count(&db, "MATCH (e:Entity) RETURN count(e) AS n").await?,
        4
    );
    Ok(())
}

#[tokio::test]
async fn weak_evidence_and_repeated_new_names_make_one_new_entity() -> Result<()> {
    let db = fixture_db(
        "CREATE (:Entity {name: 'Ada Lovelace', type: 'person', embedding: [1.0, 0.0, 0.0]}), \
                (:Mention {text: 'Ada Lavelle', type: 'person', embedding: [0.8, 0.6, 0.0]}), \
                (:Mention {text: 'Grace Hopper', type: 'person', embedding: [0.0, 1.0, 0.0]}), \
                (:Mention {text: 'Grace Hopper', type: 'person', embedding: [0.0, 1.0, 0.0]})",
    )
    .await?;
    let found = resolve_fixture(&db).await?;
    // 0.8 similarity * 0.8 weight = 0.64, under the 0.75 merge threshold.
    assert!(found.contains(&("Ada Lavelle".to_owned(), "Ada Lavelle".to_owned())));
    // Two mentions of one new name share one new entity.
    assert_eq!(
        found
            .iter()
            .filter(|(m, e)| m == "Grace Hopper" && e == "Grace Hopper")
            .count(),
        2
    );
    assert_eq!(
        count(&db, "MATCH (e:Entity) RETURN count(e) AS n").await?,
        3
    );
    Ok(())
}

#[tokio::test]
async fn validate_scores_same_as_against_labelled_mentions() -> Result<()> {
    // `gold` names the entity a mention really refers to.
    let db = fixture_db(
        "CREATE (:Entity {name: 'Ada Lovelace', type: 'person', embedding: [1.0, 0.0, 0.0]}), \
                (:Entity {name: 'Ada Byron', type: 'person', embedding: [0.96, 0.28, 0.0]}), \
                (:Mention {text: 'A. Lovelace', type: 'person', gold: 'Ada Lovelace', \
                           embedding: [0.995, 0.0998, 0.0]})",
    )
    .await?;
    let rules = &RESOLVE[..RESOLVE.find("DERIVE refers_to\n\nMATCH").unwrap()];
    let result = db
        .session()
        .locy(&format!(
            "{rules}\nVALIDATE same_as ON MATCH (m:Mention), (e:Entity) \
             TARGET e.name = m.gold METRICS brier_score, accuracy"
        ))
        .await?;
    let v = result
        .command_results()
        .iter()
        .find_map(|c| match c {
            CommandResult::Validate(v) => Some(v.clone()),
            _ => None,
        })
        .expect("a VALIDATE result");
    // Both candidate pairs cleared the threshold: one right, one wrong.
    assert_eq!(v.n_samples, 2);
    let brier = v
        .metric(uni_cypher::locy_ast::ValidationMetric::BrierScore)
        .expect("brier_score");
    let accuracy = v
        .metric(uni_cypher::locy_ast::ValidationMetric::Accuracy)
        .expect("accuracy");
    // Ada Byron's ~0.79 for a wrong match is what the Brier score punishes.
    assert!(brier > 0.3, "brier {brier}");
    assert!((accuracy - 0.5).abs() < 1e-9, "accuracy {accuracy}");
    Ok(())
}

#[tokio::test]
async fn validate_measures_stored_extraction_confidence() -> Result<()> {
    // The feature page's advice for `confidence`: yield it AS PROB, then
    // VALIDATE that rule against labelled claims.
    let db = Uni::temporary().build().await?;
    let tx = db.session().tx().await?;
    tx.execute(
        "CREATE (:Claim {confidence: 0.9, correct: true}), \
                (:Claim {confidence: 0.8, correct: false})",
    )
    .await?;
    tx.commit().await?;
    let result = db
        .session()
        .locy(
            "CREATE RULE claim_confidence AS MATCH (c:Claim) YIELD KEY c, c.confidence AS PROB\n\
             VALIDATE claim_confidence ON MATCH (c:Claim) TARGET c.correct METRICS brier_score",
        )
        .await?;
    let v = result
        .command_results()
        .iter()
        .find_map(|c| match c {
            CommandResult::Validate(v) => Some(v.clone()),
            _ => None,
        })
        .expect("a VALIDATE result");
    assert_eq!(v.n_samples, 2);
    let brier = v
        .metric(uni_cypher::locy_ast::ValidationMetric::BrierScore)
        .expect("brier_score");
    // ((1 - 0.9)^2 + 0.8^2) / 2
    assert!((brier - 0.325).abs() < 1e-6, "brier {brier}");
    Ok(())
}

#[tokio::test]
async fn variants_of_a_new_name_in_one_batch_make_one_entity() -> Result<()> {
    // Nothing exists yet, so the first pass matches nothing. The new-entity
    // step must not give each spelling its own entity: across batches they
    // would all resolve to one, so within a batch they must too.
    let db = db_with_models().await?;
    let tx = db.session().tx().await?;
    tx.execute(
        "CREATE (:Mention {text: 'Grace Hopper', type: 'person'}), \
                (:Mention {text: 'grace hopper', type: 'person'}), \
                (:Mention {text: 'Hopper', type: 'person'})",
    )
    .await?;
    tx.commit().await?;
    resolve(&db).await?;
    let expected = vec![
        ("Grace Hopper".to_owned(), "Grace Hopper".to_owned()),
        ("Hopper".to_owned(), "Grace Hopper".to_owned()),
        ("grace hopper".to_owned(), "Grace Hopper".to_owned()),
    ];
    assert_eq!(links(&db).await?, expected);
    assert_eq!(
        count(&db, "MATCH (e:Entity) RETURN count(e) AS n").await?,
        1
    );
    resolve(&db).await?;
    assert_eq!(links(&db).await?, expected);
    Ok(())
}
