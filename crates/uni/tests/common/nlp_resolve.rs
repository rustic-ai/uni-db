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
use uni_db::api::fork_diff::PromotePattern;
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

/// Stores each extracted fact as a `:Claim` whose head and tail are
/// `:Mention`s, so resolution links claims to entities. The docs page shows
/// the same statement with `schema: 'graph'`. A mention is keyed by where it
/// occurs; a speaker has no span, so its offsets are stored as -1.
const STORE_CLAIMS: &str = "
MATCH (d:Document {id: $doc})
CALL uni.nlp.extract($input, $options)
YIELD head_text, head_type, head_start, head_turn, relation,
      tail_text, tail_type, tail_start, tail_turn, polarity, modality, margin,
      model_alias, model_id
WHERE modality IN ['asserted', 'reported']
MERGE (h:Mention {doc: d.id, turn: coalesce(head_turn, -1),
                  start: coalesce(head_start, -1), text: head_text, type: head_type})
MERGE (t:Mention {doc: d.id, turn: coalesce(tail_turn, -1),
                  start: coalesce(tail_start, -1), text: tail_text, type: tail_type})
CREATE (c:Claim {relation: relation, polarity: polarity, modality: modality,
                 margin: margin, model_alias: model_alias, model_id: model_id})
CREATE (c)-[:HEAD]->(h), (c)-[:TAIL]->(t), (c)-[:FROM]->(d)
";

/// Claims over resolved entities, as `head relation tail`, sorted.
const RESOLVED_CLAIMS: &str = "
MATCH (c:Claim)-[:HEAD]->(:Mention)-[:REFERS_TO]->(h:Entity),
      (c)-[:TAIL]->(:Mention)-[:REFERS_TO]->(t:Entity)
RETURN h.name + ' ' + c.relation + ' ' + t.name AS claim ORDER BY claim
";

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
    with_resolution_schema(Uni::temporary().xervo_runtime(runtime).build().await?).await
}

/// The `Entity` / `Mention` / `Document` / `Chunk` schema the docs use,
/// names embedded on write by the `embed/names` alias.
async fn with_resolution_schema(db: Uni) -> Result<Uni> {
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
        .property_nullable("doc", DataType::String)
        .property_nullable("turn", DataType::Int64)
        .property_nullable("start", DataType::Int64)
        .vector("embedding", DIM)
        .index("embedding", name_index("text"))
        .done()
        .label("Document")
        .property("id", DataType::String)
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

fn turns(speaker: &str, text: &str) -> Value {
    let turn: HashMap<String, Value> = [
        ("speaker".to_owned(), Value::String(speaker.to_owned())),
        ("text".to_owned(), Value::String(text.to_owned())),
    ]
    .into_iter()
    .collect();
    Value::List(vec![Value::Map(turn)])
}

/// Extraction options as a Cypher value: relation names and entity types.
fn options(relations: &[&str], entity_types: &[&str]) -> Value {
    let strings =
        |xs: &[&str]| Value::List(xs.iter().map(|x| Value::String((*x).to_owned())).collect());
    Value::Map(
        [
            ("relations".to_owned(), strings(relations)),
            ("entity_types".to_owned(), strings(entity_types)),
        ]
        .into_iter()
        .collect(),
    )
}

async fn store_claims(db: &Uni, doc: &str, input: Value) -> Result<()> {
    store_claims_with(db, doc, input, options(&["knows"], &["person"])).await
}

async fn store_claims_with(db: &Uni, doc: &str, input: Value, options: Value) -> Result<()> {
    let tx = db.session().tx().await?;
    tx.execute_with(&format!("CREATE (:Document {{id: '{doc}'}})"))
        .run()
        .await?;
    tx.execute_with(STORE_CLAIMS)
        .param("doc", Value::String(doc.to_owned()))
        .param("input", input)
        .param("options", options)
        .run()
        .await?;
    tx.commit().await?;
    Ok(())
}

async fn resolved_claims(db: &Uni) -> Result<Vec<String>> {
    let rows = db.session().query(RESOLVED_CLAIMS).await?;
    rows.rows()
        .iter()
        .map(|r| Ok(r.get::<String>("claim")?))
        .collect()
}

#[tokio::test]
async fn claims_from_documents_and_conversations_meet_at_resolved_entities() -> Result<()> {
    let db = db_with_models().await?;
    let tx = db.session().tx().await?;
    tx.execute(
        "CREATE (:Entity {name: 'Ada Lovelace', type: 'person'}), \
                (:Entity {name: 'Charles Babbage', type: 'person'})",
    )
    .await?;
    tx.commit().await?;

    // Prose names people by surname; conversations add a speaker, whom the
    // model types `speaker` and gives no span.
    store_claims(
        &db,
        "letter",
        Value::String("from Lovelace to Babbage".into()),
    )
    .await?;
    store_claims(&db, "chat-1", turns("ada", "I wrote to Babbage")).await?;
    store_claims(&db, "chat-2", turns("ada", "then Hopper called")).await?;
    assert_eq!(count(&db, "MATCH (c:Claim) RETURN count(c) AS n").await?, 3);
    // Before resolution no claim reaches an entity.
    assert!(resolved_claims(&db).await?.is_empty());

    resolve(&db).await?;
    assert_eq!(
        resolved_claims(&db).await?,
        vec![
            "Ada Lovelace knows Charles Babbage",
            "ada knows Charles Babbage",
            "ada knows Hopper",
        ]
    );
    // The speaker of both conversations is one entity, typed apart from
    // people, so it never merges with "Ada Lovelace".
    assert_eq!(
        count(
            &db,
            "MATCH (e:Entity {name: 'ada', type: 'speaker'}) RETURN count(e) AS n"
        )
        .await?,
        1
    );
    assert_eq!(
        count(&db, "MATCH (e:Entity) RETURN count(e) AS n").await?,
        4
    );
    // Every mention resolved exactly once.
    assert_eq!(
        count(
            &db,
            "MATCH (m:Mention) WHERE NOT (m)-[:REFERS_TO]->() RETURN count(m) AS n"
        )
        .await?,
        0
    );
    assert_eq!(
        count(
            &db,
            "MATCH (:Mention)-[r:REFERS_TO]->() RETURN count(r) AS n"
        )
        .await?,
        6
    );

    // A later document joins the entities earlier runs created.
    store_claims(&db, "chat-3", turns("ada", "met Hopper again")).await?;
    resolve(&db).await?;
    assert_eq!(
        resolved_claims(&db).await?,
        vec![
            "Ada Lovelace knows Charles Babbage",
            "ada knows Charles Babbage",
            "ada knows Hopper",
            "ada knows Hopper",
        ]
    );
    assert_eq!(
        count(&db, "MATCH (e:Entity) RETURN count(e) AS n").await?,
        4
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn resolution_in_a_fork_is_reviewed_then_promoted() -> Result<()> {
    let db = db_with_models().await?;
    let tx = db.session().tx().await?;
    tx.execute(
        "CREATE (:Entity {name: 'Ada Lovelace', type: 'person'}), \
                (:Mention {text: 'Lovelace', type: 'person'}), \
                (:Mention {text: 'Hopper', type: 'person'})",
    )
    .await?;
    tx.commit().await?;
    db.flush().await?;

    {
        let fork = db.session().fork("review").await?;
        let tx = fork.tx().await?;
        tx.locy(RESOLVE).await?;
        tx.commit().await?;
        let rows = fork
            .query(
                "MATCH (m:Mention)-[:REFERS_TO]->(e:Entity) \
                 RETURN m.text + '->' + e.name AS l ORDER BY l",
            )
            .await?;
        let in_fork: Vec<String> = rows
            .rows()
            .iter()
            .map(|r| r.get::<String>("l"))
            .collect::<Result<_, _>>()?;
        assert_eq!(in_fork, vec!["Hopper->Hopper", "Lovelace->Ada Lovelace"]);
    }
    // Primary is untouched until the merges are accepted.
    assert!(links(&db).await?.is_empty());
    assert_eq!(
        count(&db, "MATCH (e:Entity) RETURN count(e) AS n").await?,
        1
    );

    // The diff is what a reviewer reads: one new entity, two links.
    let diff = db.diff_fork_primary("review").await?;
    let new_entities: Vec<&str> = diff
        .vertices
        .added
        .iter()
        .filter(|v| v.label == "Entity")
        .filter_map(|v| v.properties.get("name").and_then(Value::as_str))
        .collect();
    assert_eq!(new_entities, vec!["Hopper"]);
    assert_eq!(
        diff.edges
            .added
            .iter()
            .filter(|e| e.edge_type == "REFERS_TO")
            .count(),
        2
    );

    let report = db
        .promote_from_fork(
            "review",
            &[
                PromotePattern::label("Entity"),
                PromotePattern::edge_type("REFERS_TO"),
            ],
        )
        .await?;
    assert_eq!(report.vertices_inserted, 1);
    assert_eq!(report.edges_inserted, 2);
    let expected = vec![
        ("Hopper".to_owned(), "Hopper".to_owned()),
        ("Lovelace".to_owned(), "Ada Lovelace".to_owned()),
    ];
    assert_eq!(links(&db).await?, expected);

    // Promoted links count as resolved: re-running on primary adds nothing.
    resolve(&db).await?;
    assert_eq!(links(&db).await?, expected);
    assert_eq!(
        count(&db, "MATCH (e:Entity) RETURN count(e) AS n").await?,
        2
    );
    Ok(())
}

/// The claims-to-entities path on the real kniv v7 model: a letter names
/// Babbage in full, a conversation by surname, and both claims reach one
/// entity. Names are embedded by the trigram mock. Downloads ~2 GB from the
/// Hugging Face Hub on first run.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "downloads the ~2 GB kniv-v7-xlarge-u-en model"]
async fn real_kniv_v7_claims_resolve_across_documents() -> Result<()> {
    let mut catalog = uni_db::xervo_catalog_from_str(
        r#"[{
            "alias": "nlp/default",
            "task": "nlp",
            "provider_id": "local/onnx",
            "model_id": "dragonscale-ai/kniv-v7-xlarge-u-en",
            "warmup": "lazy",
            "options": {"variant": "int8,fp16", "execution_providers": ["cpu"]}
        }]"#,
    )?;
    catalog.push(embed_spec());
    let runtime = ModelRuntime::builder()
        .register_provider(uni_xervo::provider::LocalOnnxProvider::new())
        .register_provider(TrigramProvider)
        .catalog(catalog)
        .build()
        .await?;
    let db = with_resolution_schema(Uni::temporary().xervo_runtime(runtime).build().await?).await?;
    // Loading (and on first run downloading) the model takes longer than the
    // default query timeout.
    db.xervo().prefetch(&["nlp/default"]).await?;
    let opts = || {
        options(
            &["lives in", "employer"],
            &["location", "organization", "person"],
        )
    };
    store_claims_with(
        &db,
        "letter",
        Value::String("Charles Babbage lives in London and works for the Royal Society.".into()),
        opts(),
    )
    .await?;
    store_claims_with(
        &db,
        "chat",
        turns("ada", "Babbage still lives in London."),
        opts(),
    )
    .await?;
    let mentions = db
        .session()
        .query("MATCH (m:Mention) RETURN m.doc + ':' + m.text AS m ORDER BY m")
        .await?;
    let mentions: Vec<String> = mentions
        .rows()
        .iter()
        .map(|r| r.get::<String>("m"))
        .collect::<Result<_, _>>()?;
    assert!(
        mentions.contains(&"letter:Charles Babbage".to_owned())
            && mentions.contains(&"chat:Babbage".to_owned()),
        "the model's extraction changed: {mentions:?}"
    );

    resolve(&db).await?;
    assert_eq!(
        count(
            &db,
            "MATCH (m:Mention) WHERE NOT (m)-[:REFERS_TO]->() RETURN count(m) AS n"
        )
        .await?,
        0
    );
    // Both claims, one from each source, are about the same two entities.
    assert_eq!(
        resolved_claims(&db).await?,
        vec![
            "Charles Babbage lives in London",
            "Charles Babbage lives in London"
        ],
        "{:?}",
        links(&db).await?
    );
    assert_eq!(
        count(
            &db,
            "MATCH (e:Entity) WHERE e.name IN ['Charles Babbage', 'London'] RETURN count(e) AS n"
        )
        .await?,
        2
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn hundreds_of_mentions_resolve_within_the_default_timeout() -> Result<()> {
    // 300 mentions against 300 entities: half are surnames of existing
    // entities, half are new names. `DERIVE refers_to` used to re-join its
    // clause by materialising the 90,000-pair cartesian product of the
    // MATCH and scanning the facts for each pair, and timed out at 30 s;
    // it now narrows the MATCH to the pairs `resolved` holds.
    const N: usize = 300;
    let db = scale_db(N).await?;
    resolve(&db).await?;
    assert_scale_resolved(&db, N).await
}

/// `n` entities and `n` mentions of one type: every even mention is the
/// surname of an existing entity, every odd one a new name.
async fn scale_db(n: usize) -> Result<Uni> {
    scale_db_of(n, 1, false).await
}

/// [`scale_db`] with the mentions and entities spread over `types` entity
/// types (mention `i` and entity `i` share one, so the links are the same),
/// and flushed to storage when `flush`.
async fn scale_db_of(n: usize, types: usize, flush: bool) -> Result<Uni> {
    let db = db_with_models().await?;
    let kind = |i: usize| {
        if types == 1 {
            "person".to_string()
        } else {
            format!("kind{}", i % types)
        }
    };
    let entities: Vec<String> = (0..n)
        .map(|i| format!("['Known{i} Person{i}', '{}']", kind(i)))
        .collect();
    let mentions: Vec<String> = (0..n)
        .map(|i| {
            if i % 2 == 0 {
                format!("['Person{i}', '{}']", kind(i))
            } else {
                format!("['New{i} Name{i}', '{}']", kind(i))
            }
        })
        .collect();
    let tx = db.session().tx().await?;
    tx.execute(&format!(
        "UNWIND [{}] AS row CREATE (:Entity {{name: row[0], type: row[1]}})",
        entities.join(", ")
    ))
    .await?;
    tx.execute(&format!(
        "UNWIND [{}] AS row CREATE (:Mention {{text: row[0], type: row[1]}})",
        mentions.join(", ")
    ))
    .await?;
    tx.commit().await?;
    if flush {
        db.flush().await?;
    }
    Ok(db)
}

/// What resolving [`scale_db`] must leave.
async fn assert_scale_resolved(db: &Uni, n: usize) -> Result<()> {
    assert_eq!(
        count(
            db,
            "MATCH (m:Mention)-[:REFERS_TO]->(e:Entity) \
             WHERE e.name ENDS WITH ' ' + m.text RETURN count(m) AS n"
        )
        .await?,
        (n / 2) as i64,
        "every surname reaches its entity"
    );
    assert_eq!(
        count(
            db,
            "MATCH (m:Mention)-[:REFERS_TO]->(e:Entity) \
             WHERE e.name = m.text RETURN count(m) AS n"
        )
        .await?,
        (n / 2) as i64,
        "every new name gets an entity of its own"
    );
    assert_eq!(
        count(db, "MATCH (e:Entity) RETURN count(e) AS n").await?,
        (n + n / 2) as i64
    );
    Ok(())
}

/// The rules of [`RESOLVE`], without its commands.
fn resolve_rules() -> &'static str {
    let end = RESOLVE
        .find("\nDERIVE refers_to")
        .expect("the first command");
    &RESOLVE[..end]
}

/// Not a pass/fail check: where the resolution program's time goes at
/// 300 × 300 and 1,000 × 1,000 (#330). Runs [`RESOLVE`]'s steps one at a
/// time in one transaction, so each can be timed: the rules with each
/// `DERIVE refers_to`, profiled per rule, and the two Cypher commands between
/// them. Then the whole program at once on a fresh database, with the timeout
/// lifted, for the figure to hold against the default 30 s.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "timing probe: prints where the resolution program's time goes at 1,000 x 1,000; \
            asserts only the links"]
async fn resolution_program_stage_times() -> Result<()> {
    use std::time::{Duration, Instant};
    const NO_LIMIT: Duration = Duration::from_secs(3600);
    let commands: Vec<&str> = RESOLVE[resolve_rules().len()..]
        .split("DERIVE refers_to")
        .map(str::trim)
        .filter(|c| !c.is_empty())
        .collect();
    let [new_entities, leftovers] = commands.as_slice() else {
        anyhow::bail!("expected two Cypher commands, got {}", commands.len());
    };
    let derive = format!("{}\nDERIVE refers_to", resolve_rules());

    // `UNI_PROBE_PROGRAM_ONLY=1` skips the staged steps and the small size;
    // `UNI_PROBE_TOP_K=1` keeps one proof per fact, which makes the program
    // record provenance as every `MNOR` program once did.
    // `UNI_PROBE_FLUSH=1` flushes the data before resolving it, and
    // `UNI_PROBE_TYPES=k` spreads it over `k` entity types.
    let program_only = std::env::var_os("UNI_PROBE_PROGRAM_ONLY").is_some();
    let flush = std::env::var_os("UNI_PROBE_FLUSH").is_some();
    let types: usize = std::env::var("UNI_PROBE_TYPES")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(1);
    let top_k = std::env::var_os("UNI_PROBE_TOP_K").is_some();
    for n in [300, 1000] {
        if program_only && n != 1000 {
            continue;
        }
        println!("--- {n} mentions x {n} entities ---");
        let db = scale_db_of(n, types, flush).await?;
        let tx = db.session().tx().await?;
        for (step, cypher) in [
            ("first DERIVE refers_to", None),
            ("MERGE new entities", Some(new_entities)),
            ("second DERIVE refers_to", None),
            ("MERGE leftovers", Some(leftovers)),
        ] {
            if program_only {
                continue;
            }
            let started = Instant::now();
            let Some(cypher) = cypher else {
                let (_, profile) = tx.locy_with(&derive).timeout(NO_LIMIT).profile().await?;
                println!("{step}: {:.2?}", started.elapsed());
                for stratum in &profile.profile.strata {
                    for rule in &stratum.rules {
                        let ms: f64 = rule.iterations.iter().map(|i| i.elapsed_ms).sum();
                        println!(
                            "    rule {:<15} {:>8} facts {ms:>10.1} ms",
                            rule.name, rule.facts
                        );
                        let mut slow: Vec<_> =
                            rule.iterations.iter().flat_map(|i| &i.operators).collect();
                        slow.sort_by(|a, b| b.time_ms.total_cmp(&a.time_ms));
                        for o in slow.iter().take(4) {
                            println!(
                                "        {:>9} rows {:>10.1} ms  {}",
                                o.actual_rows, o.time_ms, o.operator
                            );
                        }
                    }
                }
                let strata: f64 = profile.profile.strata.iter().map(|s| s.elapsed_ms).sum();
                println!(
                    "    strata {strata:.1} ms of {:.1} ms",
                    profile.profile.total_elapsed_ms
                );
                continue;
            };
            tx.execute(cypher).await?;
            println!("{step}: {:.2?}", started.elapsed());
        }
        tx.commit().await?;
        if !program_only {
            assert_scale_resolved(&db, n).await?;
        }

        let db = scale_db_of(n, types, flush).await?;
        let tx = db.session().tx().await?;
        let started = Instant::now();
        // `UNI_PROBE_RETURN_ALL=1` returns every rule's facts, the default;
        // the program only writes, so the probe asks for none.
        let returned_rules = if std::env::var_os("UNI_PROBE_RETURN_ALL").is_some() {
            uni_db::locy::ReturnedRules::All
        } else {
            uni_db::locy::ReturnedRules::Nothing
        };
        let config = uni_db::locy::LocyConfig {
            top_k_proofs: usize::from(top_k),
            timeout: Some(NO_LIMIT),
            returned_rules,
            ..Default::default()
        };
        let (_, profile) = tx.locy_with(RESOLVE).with_config(config).profile().await?;
        let strata: f64 = profile.profile.strata.iter().map(|s| s.elapsed_ms).sum();
        println!(
            "the whole program: {:.2?} (its first evaluation of the strata: {strata:.1} ms)",
            started.elapsed()
        );
        for p in &profile.profile.phases {
            println!(
                "    {:<14} {:<8} {:>2} rules {:>8} facts {:>10.1} ms",
                p.pass, p.kind, p.rules, p.facts, p.elapsed_ms
            );
        }
        for c in &profile.profile.commands {
            println!(
                "    command {} {:<7} {:>10.1} ms  {} facts written, strata {} evaluated / {} reused",
                c.index, c.kind, c.elapsed_ms, c.facts_written, c.strata_evaluated, c.strata_reused
            );
        }
        tx.commit().await?;
        assert_scale_resolved(&db, n).await?;
    }
    Ok(())
}
