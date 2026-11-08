// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! `uni.nlp.extract` / `uni.nlp.entities` and the `UniXervo` extraction
//! facade, against a mock kniv v7 model.
//!
//! The mock tokenises on whitespace, treats every capitalised word as an
//! entity typed by the schema's first entity type, and reports one fact per
//! document: entity 0 --first relation--> entity 1. It records the schema it
//! was given, so the tests can check what reached the model. A conversation
//! input puts the first speaker first as a speaker entity, as kniv v7 does.
//!
//! One `#[ignore]`d test runs the real `kniv-v7-xlarge-u-en` model (a ~2 GB
//! download); the nightly ignored-sweep runs it.

use std::sync::{Arc, Mutex};

use anyhow::Result;
use async_trait::async_trait;
use uni_db::xervo::{
    ExtractOptions, ExtractionSchema, NlpInput, NlpModel, NlpRequest, NlpResult, NlpTasks,
};
use uni_db::{ModelAliasSpec, ModelTask, Uni, Value, WarmupPolicy};
use uni_xervo::runtime::ModelRuntime;
use uni_xervo::traits::{
    LoadedModelHandle, ModelInfo, ModelProvider, NlpEntity, NlpFact, NlpToken, NlpWindow,
    ProviderCapabilities, ProviderHealth, WindowSentence,
};

/// The schema the mock last received.
type Seen = Arc<Mutex<Option<ExtractionSchema>>>;

struct MockKniv {
    seen: Seen,
    tasks: NlpTasks,
}

impl ModelInfo for MockKniv {
    fn model_id(&self) -> &str {
        "mock/kniv-v7"
    }
}

/// Whitespace words of `text` with their byte offsets.
fn words(text: &str) -> Vec<(String, usize)> {
    let mut out = Vec::new();
    let mut start = None;
    for (i, c) in text.char_indices() {
        match (c.is_whitespace(), start) {
            (false, None) => start = Some(i),
            (true, Some(s)) => {
                out.push((text[s..i].to_owned(), s));
                start = None;
            }
            _ => {}
        }
    }
    if let Some(s) = start {
        out.push((text[s..].to_owned(), s));
    }
    out
}

fn analyse(units: &[(Option<&str>, &str)], schema: Option<&ExtractionSchema>) -> NlpResult {
    let entity_type = schema
        .and_then(|s| s.entity_types.as_ref())
        .and_then(|t| t.first())
        .map_or("thing".to_owned(), |t| t.name.clone());
    let mut result = NlpResult::default();
    let mut window = NlpWindow::default();
    if let Some((Some(speaker), _)) = units.first() {
        let mut e = NlpEntity::default();
        e.ty = "speaker".into();
        e.text = (*speaker).to_owned();
        e.speaker = true;
        window.entities.push(e);
    }
    for (unit, (_, text)) in units.iter().enumerate() {
        let first = window.tokens.len();
        for (w, start) in words(text) {
            let idx = window.tokens.len();
            let mut tok = NlpToken::default();
            tok.text = w.clone();
            tok.start = start;
            tok.end = start + w.len();
            tok.word_index = idx;
            result.tokens.push(tok);
            if w.chars().next().is_some_and(char::is_uppercase) && idx > 0 {
                let mut e = NlpEntity::default();
                e.ty = entity_type.clone();
                e.text = w.clone();
                e.mentions = vec![w.clone()];
                e.spans = vec![(idx, idx + 1)];
                window.entities.push(e);
            }
            window.tokens.push(w);
        }
        let mut sentence = WindowSentence::default();
        sentence.start = first;
        sentence.end = window.tokens.len();
        sentence.unit = unit;
        window.sentences.push(sentence);
    }
    window.analysed_words = window.tokens.len();
    if let Some(relation) = schema.and_then(|s| s.relations.first())
        && window.entities.len() >= 2
    {
        let mut f = NlpFact::default();
        f.head = 0;
        f.tail = 1;
        f.relation = relation.name.clone();
        f.margin = 1.0;
        window.facts.push(f);
    }
    result.windows.push(window);
    result
}

#[async_trait]
impl NlpModel for MockKniv {
    async fn analyze(
        &self,
        requests: Vec<NlpRequest<'_>>,
    ) -> uni_xervo::error::Result<Vec<NlpResult>> {
        let mut out = Vec::new();
        for req in requests {
            *self.seen.lock().unwrap() = req.schema.cloned();
            let units: Vec<(Option<&str>, &str)> = match req.turns {
                Some(turns) => turns
                    .iter()
                    .filter(|t| !t.text.trim().is_empty())
                    .map(|t| (Some(t.speaker), t.text))
                    .collect(),
                None => vec![(None, req.text)],
            };
            out.push(analyse(&units, req.schema));
        }
        Ok(out)
    }

    fn supported_tasks(&self) -> NlpTasks {
        self.tasks
    }
}

struct MockProvider {
    seen: Seen,
}

#[async_trait]
impl ModelProvider for MockProvider {
    fn provider_id(&self) -> &'static str {
        "mock/kniv"
    }

    fn capabilities(&self) -> ProviderCapabilities {
        ProviderCapabilities {
            supported_tasks: vec![ModelTask::Nlp],
        }
    }

    async fn load(&self, spec: &ModelAliasSpec) -> uni_xervo::error::Result<LoadedModelHandle> {
        // `model_id: "v1"` stands in for a v1 cascade model.
        let tasks = if spec.model_id == "v1" {
            NlpTasks::CASCADE
        } else {
            NlpTasks::ALL
        };
        let model: Arc<dyn NlpModel> = Arc::new(MockKniv {
            seen: self.seen.clone(),
            tasks,
        });
        Ok(Arc::new(model) as LoadedModelHandle)
    }

    async fn health(&self) -> ProviderHealth {
        ProviderHealth::Healthy
    }
}

fn spec(alias: &str, model_id: &str) -> ModelAliasSpec {
    ModelAliasSpec {
        alias: alias.into(),
        task: ModelTask::Nlp,
        provider_id: "mock/kniv".into(),
        model_id: model_id.into(),
        revision: None,
        warmup: WarmupPolicy::Lazy,
        required: false,
        timeout: None,
        load_timeout: None,
        retry: None,
        options: serde_json::json!({}),
    }
}

async fn db_with_mock() -> Result<(Uni, Seen)> {
    let seen: Seen = Arc::new(Mutex::new(None));
    let runtime = ModelRuntime::builder()
        .register_provider(MockProvider { seen: seen.clone() })
        .catalog(vec![spec("nlp/default", "mock-v7"), spec("nlp/v1", "v1")])
        .build()
        .await?;
    let db = Uni::temporary().xervo_runtime(runtime).build().await?;
    Ok((db, seen))
}

fn str_col(row: &uni_db::Row, col: &str) -> String {
    row.get::<String>(col).unwrap_or_default()
}

fn val(row: &uni_db::Row, col: &str) -> Value {
    row.value(col).cloned().unwrap_or(Value::Null)
}

#[tokio::test]
async fn extract_yields_facts_with_spans_and_provenance() -> Result<()> {
    let (db, seen) = db_with_mock().await?;
    let rows = db
        .session()
        .query(
            "CALL uni.nlp.extract('met Ada in Paris', \
               {relations: [{name: 'visited', description: 'a place the head went to'}], \
                entity_types: ['person']}) \
             YIELD doc, head_text, head_start, head_end, relation, tail_text, tail_start, \
                   tail_end, polarity, modality, margin, confidence, model_alias, model_id \
             RETURN *",
        )
        .await?;
    assert_eq!(rows.len(), 1);
    let r = &rows.rows()[0];
    assert_eq!(str_col(r, "head_text"), "Ada");
    assert_eq!(str_col(r, "tail_text"), "Paris");
    assert_eq!(str_col(r, "relation"), "visited");
    assert_eq!(r.get::<i64>("head_start")?, 4);
    assert_eq!(r.get::<i64>("head_end")?, 7);
    assert_eq!(r.get::<i64>("tail_start")?, 11);
    assert_eq!(r.get::<i64>("tail_end")?, 16);
    assert_eq!(str_col(r, "polarity"), "positive");
    assert_eq!(str_col(r, "modality"), "asserted");
    assert!((r.get::<f64>("margin")? - 1.0).abs() < 1e-6);
    assert!((r.get::<f64>("confidence")? - 0.731_058_6).abs() < 1e-6);
    assert_eq!(str_col(r, "model_alias"), "nlp/default");
    assert_eq!(str_col(r, "model_id"), "mock/kniv-v7");

    let schema = seen
        .lock()
        .unwrap()
        .clone()
        .expect("schema reached the model");
    assert_eq!(schema.relations[0].name, "visited");
    assert_eq!(
        schema.relations[0].description.as_deref(),
        Some("a place the head went to")
    );
    Ok(())
}

#[tokio::test]
async fn extract_runs_per_row_after_match_and_feeds_merge() -> Result<()> {
    let (db, _) = db_with_mock().await?;
    let session = db.session();
    let tx = session.tx().await?;
    tx.execute("CREATE (:Doc {id: 1, text: 'saw Bob near Rome'})")
        .await?;
    tx.execute("CREATE (:Doc {id: 2, text: 'no entities here'})")
        .await?;
    tx.commit().await?;

    let tx = session.tx().await?;
    tx.execute(
        "MATCH (d:Doc) \
         CALL uni.nlp.extract(d.text, {relations: ['near']}) \
         YIELD head_text, relation, tail_text \
         MERGE (h:Entity {name: head_text}) \
         MERGE (t:Entity {name: tail_text}) \
         CREATE (h)-[:CLAIM {relation: relation, doc: d.id}]->(t)",
    )
    .await?;
    tx.commit().await?;

    let rows = session
        .query(
            "MATCH (h:Entity)-[c:CLAIM]->(t:Entity) \
             RETURN h.name AS h, c.relation AS r, c.doc AS doc, t.name AS t",
        )
        .await?;
    assert_eq!(rows.len(), 1);
    let r = &rows.rows()[0];
    assert_eq!(
        (str_col(r, "h"), str_col(r, "r"), str_col(r, "t")),
        ("Bob".into(), "near".into(), "Rome".into())
    );
    assert_eq!(r.get::<i64>("doc")?, 1);
    Ok(())
}

#[tokio::test]
async fn graph_schema_maps_edge_types_and_labels_both_ways() -> Result<()> {
    let (db, seen) = db_with_mock().await?;
    db.schema()
        .label("Person")
        .done()
        .edge_type("LIVES_IN", &["Person"], &["Person"])
        .description("the place where the head resides")
        .done()
        .apply()
        .await?;
    let rows = db
        .session()
        .query(
            "CALL uni.nlp.extract('home of Ada is Lisbon', {schema: 'graph'}) \
             YIELD relation, head_type, tail_type RETURN relation, head_type, tail_type",
        )
        .await?;
    assert_eq!(rows.len(), 1);
    let r = &rows.rows()[0];
    assert_eq!(str_col(r, "relation"), "LIVES_IN");
    assert_eq!(str_col(r, "head_type"), "Person");

    let schema = seen.lock().unwrap().clone().unwrap();
    assert_eq!(schema.relations[0].name, "lives in");
    assert_eq!(
        schema.relations[0].description.as_deref(),
        Some("the place where the head resides")
    );
    assert_eq!(schema.entity_types.unwrap()[0].name, "person");
    Ok(())
}

#[tokio::test]
async fn graph_schema_on_a_fork_sees_fork_local_edge_types() -> Result<()> {
    let (db, seen) = db_with_mock().await?;
    db.schema()
        .label("Person")
        .done()
        .edge_type("KNOWS", &["Person"], &["Person"])
        .done()
        .apply()
        .await?;
    let session = db.session();
    let fork = session.fork("nlp").await?;
    fork.fork_schema()
        .edge_type("MENTORS", &["Person"], &["Person"])
        .description("someone the head guides")
        .apply()
        .await?;
    fork.query(
        "CALL uni.nlp.extract('then Ada met Bob', {schema: 'graph'}) \
         YIELD relation RETURN relation",
    )
    .await?;
    let names: Vec<String> = seen
        .lock()
        .unwrap()
        .clone()
        .unwrap()
        .relations
        .into_iter()
        .map(|r| r.name)
        .collect();
    assert!(names.contains(&"mentors".to_owned()), "{names:?}");
    assert!(names.contains(&"knows".to_owned()), "{names:?}");

    // The primary does not see the fork's edge type.
    session
        .query(
            "CALL uni.nlp.extract('then Ada met Bob', {schema: 'graph'}) \
             YIELD relation RETURN relation",
        )
        .await?;
    let primary: Vec<String> = seen
        .lock()
        .unwrap()
        .clone()
        .unwrap()
        .relations
        .into_iter()
        .map(|r| r.name)
        .collect();
    assert_eq!(primary, vec!["knows".to_owned()]);
    Ok(())
}

#[tokio::test]
async fn conversation_turns_give_speaker_entities_and_turn_offsets() -> Result<()> {
    let (db, _) = db_with_mock().await?;
    let session = db.session();
    let facts = session
        .query(
            "CALL uni.nlp.extract( \
               [{speaker: 'user', text: 'hello'}, {speaker: 'user', text: ' '}, \
                {speaker: 'user', text: 'moved to Lisbon'}], \
               {relations: ['lives in']}) \
             YIELD head_text, head_speaker, head_start, tail_text, tail_start, tail_end, \
                   tail_turn \
             RETURN *",
        )
        .await?;
    assert_eq!(facts.len(), 1);
    let r = &facts.rows()[0];
    assert_eq!(str_col(r, "head_text"), "user");
    assert!(r.get::<bool>("head_speaker")?);
    assert_eq!(val(r, "head_start"), Value::Null);
    assert_eq!(str_col(r, "tail_text"), "Lisbon");
    // Offsets are relative to the turn, which is the third (index 2): the
    // blank second turn is skipped by the model but keeps its index here.
    assert_eq!(r.get::<i64>("tail_start")?, 9);
    assert_eq!(r.get::<i64>("tail_end")?, 15);
    assert_eq!(r.get::<i64>("tail_turn")?, 2);

    let ents = session
        .query(
            "CALL uni.nlp.entities([{speaker: 'user', text: 'moved to Lisbon'}], {}) \
             YIELD entity, text, entity_type, speaker, mentions, mention_starts, mention_turns \
             RETURN entity, text, entity_type, speaker, size(mentions) AS n, mention_starts, \
                    mention_turns \
             ORDER BY entity",
        )
        .await?;
    assert_eq!(ents.len(), 2);
    let speaker = &ents.rows()[0];
    assert!(speaker.get::<bool>("speaker")?);
    assert_eq!(str_col(speaker, "entity_type"), "speaker");
    assert_eq!(speaker.get::<i64>("n")?, 0);
    let lisbon = &ents.rows()[1];
    assert_eq!(str_col(lisbon, "text"), "Lisbon");
    assert_eq!(lisbon.get::<i64>("n")?, 1);
    assert_eq!(
        val(lisbon, "mention_starts"),
        Value::List(vec![Value::Int(9)])
    );
    assert_eq!(
        val(lisbon, "mention_turns"),
        Value::List(vec![Value::Int(0)])
    );
    Ok(())
}

#[tokio::test]
async fn several_documents_are_numbered_by_doc() -> Result<()> {
    let (db, _) = db_with_mock().await?;
    let rows = db
        .session()
        .query(
            "CALL uni.nlp.extract(['a Bob b Rome', 'nothing', 'x Ann y Oslo'], \
               {relations: ['near']}) \
             YIELD doc, head_text RETURN doc, head_text ORDER BY doc",
        )
        .await?;
    let got: Vec<(i64, String)> = rows
        .rows()
        .iter()
        .map(|r| (r.get::<i64>("doc").unwrap(), str_col(r, "head_text")))
        .collect();
    assert_eq!(got, vec![(0, "Bob".into()), (2, "Ann".into())]);
    Ok(())
}

async fn error_of(db: &Uni, cypher: &str) -> String {
    match db.session().query(cypher).await {
        Ok(_) => panic!("expected an error from: {cypher}"),
        Err(e) => e.to_string(),
    }
}

#[tokio::test]
async fn bad_options_and_models_fail_with_clear_errors() -> Result<()> {
    let (db, _) = db_with_mock().await?;
    let unknown = error_of(
        &db,
        "CALL uni.nlp.extract('x', {relations: ['r'], min_confidence: 0.5}) YIELD relation \
         RETURN relation",
    )
    .await;
    assert!(
        unknown.contains("unknown option 'min_confidence'"),
        "{unknown}"
    );

    let both = error_of(
        &db,
        "CALL uni.nlp.extract('x', {schema: 'graph', relations: ['r']}) YIELD relation \
         RETURN relation",
    )
    .await;
    assert!(both.contains("cannot be combined"), "{both}");

    let none = error_of(
        &db,
        "CALL uni.nlp.extract('x', {}) YIELD relation RETURN relation",
    )
    .await;
    assert!(none.contains("give 'relations'"), "{none}");

    let v1 = error_of(
        &db,
        "CALL uni.nlp.extract('x', {alias: 'nlp/v1', relations: ['r']}) YIELD relation \
         RETURN relation",
    )
    .await;
    assert!(v1.contains("has no entity/fact extraction"), "{v1}");
    assert!(v1.contains("kniv-v7"), "{v1}");

    let missing = error_of(
        &db,
        "CALL uni.nlp.extract('x', {alias: 'nlp/nope', relations: ['r']}) YIELD relation \
         RETURN relation",
    )
    .await;
    assert!(missing.contains("nlp/nope"), "{missing}");
    Ok(())
}

#[tokio::test]
async fn without_a_runtime_the_procedures_say_so() -> Result<()> {
    let db = Uni::temporary().build().await?;
    let err = error_of(&db, "CALL uni.nlp.entities('x', {}) YIELD text RETURN text").await;
    assert!(err.contains("Uni-Xervo runtime is not configured"), "{err}");
    Ok(())
}

#[tokio::test]
async fn facade_extracts_the_same_rows() -> Result<()> {
    let (db, _) = db_with_mock().await?;
    let facts = db
        .xervo()
        .extract_facts(
            &NlpInput::Text("met Ada in Paris".into()),
            &ExtractOptions::new().relation("visited", Some("a place the head went to")),
        )
        .await?;
    assert_eq!(facts.len(), 1);
    assert_eq!(
        (facts[0].head_text.as_str(), facts[0].tail_text.as_str()),
        ("Ada", "Paris")
    );
    assert_eq!(
        (facts[0].tail_start, facts[0].tail_end),
        (Some(11), Some(16))
    );

    let entities = db
        .xervo()
        .extract_entities(
            &NlpInput::Text("met Ada in Paris".into()),
            &ExtractOptions::new(),
        )
        .await?;
    assert_eq!(entities.len(), 2);

    let bad = db
        .xervo()
        .extract_facts(&NlpInput::Text("x".into()), &ExtractOptions::new())
        .await
        .unwrap_err();
    assert!(
        matches!(bad, uni_db::UniError::InvalidArgument { .. }),
        "{bad}"
    );

    let raw = db
        .xervo()
        .nlp(
            "nlp/default",
            vec![NlpRequest::new("met Ada", NlpTasks::ENTITIES)],
        )
        .await?;
    assert_eq!(raw[0].windows.len(), 1);
    Ok(())
}

/// The write-time pattern from the feature docs: a declared trigger whose
/// action extracts from each new document, off the commit path.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn declared_trigger_extracts_on_write() -> Result<()> {
    let (db, _) = db_with_mock().await?;
    let session = db.session();
    let tx = session.tx().await?;
    tx.execute(
        r#"CALL uni.plugin.declareTrigger(
            'memory.extract',
            'CREATE ON :Document',
            'MATCH (d:Document) WHERE id(d) = $vid
             CALL uni.nlp.extract(d.text, {relations: ["near"]})
             YIELD head_text, relation, tail_text, modality
             WHERE modality IN ["asserted", "reported"]
             MERGE (h:Entity {name: head_text})
             MERGE (t:Entity {name: tail_text})
             CREATE (h)-[:CLAIM {relation: relation}]->(t)'
        )"#,
    )
    .await?;
    tx.commit().await?;

    let tx = session.tx().await?;
    tx.execute("CREATE (:Document {text: 'saw Bob near Rome'})")
        .await?;
    tx.commit().await?;

    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    loop {
        let rows = session
            .query(
                "MATCH (h:Entity)-[c:CLAIM]->(t:Entity) \
                 RETURN h.name AS h, c.relation AS r, t.name AS t",
            )
            .await?;
        if let Some(r) = rows.rows().first() {
            assert_eq!(
                (str_col(r, "h"), str_col(r, "r"), str_col(r, "t")),
                ("Bob".into(), "near".into(), "Rome".into())
            );
            return Ok(());
        }
        assert!(
            std::time::Instant::now() < deadline,
            "the trigger action never wrote a claim"
        );
        tokio::time::sleep(std::time::Duration::from_millis(25)).await;
    }
}

/// The real kniv v7 UltraBERT model: the conversation example from the
/// feature docs. Downloads ~2 GB from the Hugging Face Hub on first run.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "downloads the ~2 GB kniv-v7-xlarge-u-en model"]
async fn real_kniv_v7_extracts_the_conversation_example() -> Result<()> {
    let catalog = uni_db::xervo_catalog_from_str(
        r#"[{
            "alias": "nlp/default",
            "task": "nlp",
            "provider_id": "local/onnx",
            "model_id": "dragonscale-ai/kniv-v7-xlarge-u-en",
            "warmup": "lazy",
            "options": {"variant": "int8,fp16", "execution_providers": ["cpu"]}
        }]"#,
    )?;
    let db = Uni::temporary().xervo_catalog(catalog).build().await?;
    // Loading (and on first run downloading) the model takes longer than the
    // default query timeout, so load it before the query, as the docs advise.
    db.xervo().prefetch(&["nlp/default"]).await?;
    let rows = db
        .session()
        .query(
            "CALL uni.nlp.extract( \
               [{speaker: 'user', text: 'I moved to Lisbon last year and started at a bank.'}, \
                {speaker: 'assistant', text: 'Congratulations on the move!'}], \
               {relations: [ \
                  {name: 'lives in', description: 'the place where the head resides'}, \
                  {name: 'employer', description: 'organization the head works for'}], \
                entity_types: ['location', 'organization', 'person']}) \
             YIELD head_text, head_speaker, relation, tail_text, tail_start, tail_end, \
                   tail_turn, polarity, modality \
             RETURN *",
        )
        .await?;
    let facts: Vec<(String, String, String)> = rows
        .rows()
        .iter()
        .map(|r| {
            (
                str_col(r, "head_text"),
                str_col(r, "relation"),
                str_col(r, "tail_text"),
            )
        })
        .collect();
    assert!(
        facts.contains(&("user".into(), "lives in".into(), "Lisbon".into())),
        "{facts:?}"
    );
    assert!(
        facts
            .iter()
            .any(|(h, r, t)| h == "user" && r == "employer" && t.ends_with("bank")),
        "{facts:?}"
    );
    let lisbon = rows
        .rows()
        .iter()
        .find(|r| str_col(r, "tail_text") == "Lisbon")
        .unwrap();
    // "I moved to Lisbon": Lisbon is bytes 11..17 of turn 0.
    assert_eq!(lisbon.get::<i64>("tail_start")?, 11);
    assert_eq!(lisbon.get::<i64>("tail_end")?, 17);
    assert_eq!(lisbon.get::<i64>("tail_turn")?, 0);
    assert_eq!(str_col(lisbon, "modality"), "asserted");
    Ok(())
}
