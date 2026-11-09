// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! `uni.nlp.words` / `uni.nlp.sentences` and the `UniXervo` analysis facade,
//! against a mock kniv v7 model.
//!
//! The mock splits each text (or each turn) on whitespace into words, and
//! ends a sentence at a word ending in `.`, `?` or `!`. Each sentence is its
//! own window, so the tests see indices counted across windows. A
//! capitalised word is a `PROPN`, every other word `X`; a word's lemma is it
//! lowercased without punctuation; the first word of a sentence is its root
//! and heads every other word. A sentence containing "love" is positive,
//! "hate" negative, otherwise neutral; one ending in `?` is a question,
//! otherwise a statement. The mock records the heads it was asked for.
//!
//! One `#[ignore]`d test runs the real `kniv-v7-xlarge-u-en` model (a ~2 GB
//! download), including the feature page's query examples; the nightly
//! ignored-sweep runs it.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use anyhow::Result;
use async_trait::async_trait;
use uni_db::xervo::{AnalyzeOptions, NlpInput, NlpModel, NlpRequest, NlpResult, NlpTasks};
use uni_db::{ModelAliasSpec, ModelTask, Uni, Value, WarmupPolicy};
use uni_xervo::runtime::ModelRuntime;
use uni_xervo::traits::{
    LoadedModelHandle, ModelInfo, ModelProvider, NlpLabelMaps, NlpToken, NlpWindow,
    ProviderCapabilities, ProviderHealth, WindowSentence, WordAnalysis,
};

/// The heads the mock was last asked for.
type Seen = Arc<Mutex<Option<NlpTasks>>>;

struct MockAnalyzer {
    seen: Seen,
    tasks: NlpTasks,
    labels: NlpLabelMaps,
}

impl ModelInfo for MockAnalyzer {
    fn model_id(&self) -> &str {
        "mock/kniv-v7"
    }
}

/// Whitespace words of `text` with their byte offsets.
fn split_words(text: &str) -> Vec<(String, usize)> {
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

fn analyse(units: &[(Option<&str>, &str)]) -> NlpResult {
    let mut result = NlpResult::default();
    for (unit, (speaker, text)) in units.iter().enumerate() {
        let mut sentences: Vec<Vec<(String, usize)>> = vec![Vec::new()];
        for word in split_words(text) {
            let ends = word.0.ends_with(['.', '?', '!']);
            sentences.last_mut().expect("never empty").push(word);
            if ends {
                sentences.push(Vec::new());
            }
        }
        for sentence in sentences.into_iter().filter(|s| !s.is_empty()) {
            let mut window = NlpWindow::default();
            for (k, (w, start)) in sentence.iter().enumerate() {
                let mut tok = NlpToken::default();
                tok.text = w.clone();
                tok.start = *start;
                tok.end = start + w.len();
                tok.word_index = result.tokens.len();
                result.tokens.push(tok);
                let mut a = WordAnalysis::default();
                a.pos = Some(
                    if w.chars().next().is_some_and(char::is_uppercase) {
                        "PROPN"
                    } else {
                        "X"
                    }
                    .into(),
                );
                a.morph = Some("_".into());
                a.lemma = Some(w.trim_end_matches(['.', '?', '!', ',']).to_lowercase());
                a.head = Some(if k == 0 { 0 } else { 1 });
                a.deprel = Some(if k == 0 { "root" } else { "dep" }.into());
                window.words.push(a);
                window.tokens.push(w.clone());
            }
            let joined = window.tokens.join(" ");
            let question = joined.ends_with('?');
            let mut sent = WindowSentence::default();
            sent.start = 0;
            sent.end = window.tokens.len();
            sent.unit = unit;
            sent.speaker = speaker.map(str::to_owned);
            sent.sentiment = Some(
                if joined.contains("love") {
                    "positive"
                } else if joined.contains("hate") {
                    "negative"
                } else {
                    "neutral"
                }
                .into(),
            );
            sent.cls = vec![if question { "question" } else { "statement" }.into()];
            sent.cls_scores = if question {
                vec![0.75, 0.25]
            } else {
                vec![0.25, 0.75]
            };
            sent.whole = true;
            window.sentences.push(sent);
            window.analysed_words = window.tokens.len();
            result.windows.push(window);
        }
    }
    result
}

#[async_trait]
impl NlpModel for MockAnalyzer {
    async fn analyze(
        &self,
        requests: Vec<NlpRequest<'_>>,
    ) -> uni_xervo::error::Result<Vec<NlpResult>> {
        let mut out = Vec::new();
        for req in requests {
            *self.seen.lock().unwrap() = Some(req.tasks);
            let units: Vec<(Option<&str>, &str)> = match req.turns {
                Some(turns) => turns
                    .iter()
                    .filter(|t| !t.text.trim().is_empty())
                    .map(|t| (Some(t.speaker), t.text))
                    .collect(),
                None => vec![(None, req.text)],
            };
            out.push(analyse(&units));
        }
        Ok(out)
    }

    fn supported_tasks(&self) -> NlpTasks {
        self.tasks
    }

    fn label_maps(&self) -> Option<&NlpLabelMaps> {
        Some(&self.labels)
    }
}

struct MockProvider {
    seen: Seen,
}

#[async_trait]
impl ModelProvider for MockProvider {
    fn provider_id(&self) -> &'static str {
        "mock/kniv-analyze"
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
        let mut labels = NlpLabelMaps::default();
        labels.cls = vec!["question".into(), "statement".into()];
        let model: Arc<dyn NlpModel> = Arc::new(MockAnalyzer {
            seen: self.seen.clone(),
            tasks,
            labels,
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
        provider_id: "mock/kniv-analyze".into(),
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

fn val(row: &uni_db::Row, col: &str) -> Value {
    row.value(col).cloned().unwrap_or(Value::Null)
}

fn int(v: i64) -> Value {
    Value::Int(v)
}

fn s(v: &str) -> Value {
    Value::String(v.to_owned())
}

#[tokio::test]
async fn words_yield_tags_lemmas_and_document_wide_heads() -> Result<()> {
    let (db, seen) = db_with_mock().await?;
    let rows = db
        .session()
        .query(
            "CALL uni.nlp.words('Ada wrote notes. Babbage built engines.') \
             YIELD doc, window, sentence, word, text, start, end, turn, speaker, \
                   pos, morph, lemma, head, deprel, model_alias, model_id \
             RETURN * ORDER BY word",
        )
        .await?;
    assert_eq!(rows.len(), 6);
    let got: Vec<_> = rows
        .iter()
        .map(|r| {
            [
                val(r, "window"),
                val(r, "sentence"),
                val(r, "word"),
                val(r, "text"),
                val(r, "head"),
                val(r, "deprel"),
            ]
        })
        .collect();
    assert_eq!(
        got,
        [
            [int(0), int(0), int(0), s("Ada"), Value::Null, s("root")],
            [int(0), int(0), int(1), s("wrote"), int(0), s("dep")],
            [int(0), int(0), int(2), s("notes."), int(0), s("dep")],
            [int(1), int(1), int(3), s("Babbage"), Value::Null, s("root")],
            [int(1), int(1), int(4), s("built"), int(3), s("dep")],
            [int(1), int(1), int(5), s("engines."), int(3), s("dep")],
        ],
        "heads index words across the document, not the window"
    );
    let built = &rows.rows()[4];
    assert_eq!((val(built, "start"), val(built, "end")), (int(25), int(30)));
    assert_eq!(val(built, "pos"), s("X"));
    assert_eq!(val(&rows.rows()[3], "pos"), s("PROPN"));
    assert_eq!(val(&rows.rows()[2], "lemma"), s("notes"));
    assert_eq!(val(built, "morph"), s("_"));
    assert_eq!(val(built, "turn"), Value::Null);
    assert_eq!(val(built, "speaker"), Value::Null);
    assert_eq!(val(built, "doc"), int(0));
    assert_eq!(val(built, "model_alias"), s("nlp/default"));
    assert_eq!(val(built, "model_id"), s("mock/kniv-v7"));
    assert_eq!(
        *seen.lock().unwrap(),
        Some(NlpTasks::POS | NlpTasks::MORPH | NlpTasks::LEMMA | NlpTasks::DEP),
        "words request only the per-word heads"
    );
    Ok(())
}

#[tokio::test]
async fn a_head_joins_back_to_its_word() -> Result<()> {
    let (db, _) = db_with_mock().await?;
    let rows = db
        .session()
        .query(
            "CALL uni.nlp.words('Ada wrote notes. Babbage built engines.') \
             YIELD word, text, head \
             WITH collect({word: word, text: text, head: head}) AS ws \
             UNWIND ws AS w \
             UNWIND ws AS h \
             WITH w, h WHERE h.word = w.head \
             RETURN w.text AS dependent, h.text AS governor ORDER BY w.word",
        )
        .await?;
    let pairs: Vec<(Value, Value)> = rows
        .iter()
        .map(|r| (val(r, "dependent"), val(r, "governor")))
        .collect();
    assert_eq!(
        pairs,
        [
            (s("wrote"), s("Ada")),
            (s("notes."), s("Ada")),
            (s("built"), s("Babbage")),
            (s("engines."), s("Babbage")),
        ]
    );
    Ok(())
}

#[tokio::test]
async fn sentences_yield_sentiment_and_dialogue_acts() -> Result<()> {
    let (db, seen) = db_with_mock().await?;
    let rows = db
        .session()
        .query(
            "CALL uni.nlp.sentences('I love the engine. Is it finished?') \
             YIELD sentence, text, start, end, sentiment, acts, act_scores, whole, speaker \
             RETURN * ORDER BY sentence",
        )
        .await?;
    assert_eq!(rows.len(), 2);
    assert_eq!(val(&rows.rows()[0], "text"), s("I love the engine."));
    assert_eq!(val(&rows.rows()[0], "sentiment"), s("positive"));
    assert_eq!(
        val(&rows.rows()[0], "acts"),
        Value::List(vec![s("statement")])
    );
    assert_eq!(val(&rows.rows()[1], "text"), s("Is it finished?"));
    assert_eq!(
        (val(&rows.rows()[1], "start"), val(&rows.rows()[1], "end")),
        (int(19), int(34))
    );
    assert_eq!(val(&rows.rows()[1], "sentiment"), s("neutral"));
    assert_eq!(
        val(&rows.rows()[1], "acts"),
        Value::List(vec![s("question")])
    );
    let scores = match val(&rows.rows()[1], "act_scores") {
        Value::Map(m) => m
            .into_iter()
            .map(|(k, v)| (k, v.as_f64().unwrap_or(f64::NAN)))
            .collect::<BTreeMap<_, _>>(),
        other => panic!("act_scores is a map, got {other:?}"),
    };
    assert_eq!(
        scores,
        BTreeMap::from([
            ("question".to_owned(), 0.75),
            ("statement".to_owned(), 0.25)
        ])
    );
    assert_eq!(val(&rows.rows()[1], "whole"), Value::Bool(true));
    assert_eq!(val(&rows.rows()[1], "speaker"), Value::Null);
    assert_eq!(
        *seen.lock().unwrap(),
        Some(NlpTasks::CLS | NlpTasks::SENTIMENT),
        "sentences request only the per-sentence heads"
    );
    Ok(())
}

#[tokio::test]
async fn a_conversation_names_each_sentence_turn_and_speaker() -> Result<()> {
    let (db, _) = db_with_mock().await?;
    // The blank middle turn is dropped by uni-xervo; turn indices still count
    // it, so they index the input list.
    let rows = db
        .session()
        .query(
            "CALL uni.nlp.sentences([ \
               {speaker: 'user', text: 'I hate waiting.'}, \
               {speaker: 'user', text: '  '}, \
               {speaker: 'assistant', text: 'Sorry! Is it better now?'}]) \
             YIELD sentence, text, start, end, turn, speaker, sentiment \
             RETURN * ORDER BY sentence",
        )
        .await?;
    let got: Vec<_> = rows
        .iter()
        .map(|r| {
            [
                val(r, "text"),
                val(r, "turn"),
                val(r, "speaker"),
                val(r, "start"),
                val(r, "end"),
            ]
        })
        .collect();
    assert_eq!(
        got,
        [
            [s("I hate waiting."), int(0), s("user"), int(0), int(15)],
            [s("Sorry!"), int(2), s("assistant"), int(0), int(6)],
            [
                s("Is it better now?"),
                int(2),
                s("assistant"),
                int(7),
                int(24)
            ],
        ]
    );
    assert_eq!(val(&rows.rows()[0], "sentiment"), s("negative"));

    let words = db
        .session()
        .query(
            "CALL uni.nlp.words([{speaker: 'user', text: 'Hi'}, {speaker: 'bot', text: 'Hello there'}]) \
             YIELD word, text, turn, speaker RETURN * ORDER BY word",
        )
        .await?;
    let got: Vec<_> = words
        .iter()
        .map(|r| {
            [
                val(r, "word"),
                val(r, "text"),
                val(r, "turn"),
                val(r, "speaker"),
            ]
        })
        .collect();
    assert_eq!(
        got,
        [
            [int(0), s("Hi"), int(0), s("user")],
            [int(1), s("Hello"), int(1), s("bot")],
            [int(2), s("there"), int(1), s("bot")],
        ]
    );
    Ok(())
}

#[tokio::test]
async fn per_row_calls_analyse_each_node_and_skip_null_text() -> Result<()> {
    let (db, _) = db_with_mock().await?;
    let tx = db.session().tx().await?;
    tx.execute(
        "CREATE (:Note {id: 1, text: 'I love it.'}), (:Note {id: 2, text: 'I hate it. Why?'}), \
                (:Note {id: 3})",
    )
    .await?;
    tx.commit().await?;
    let rows = db
        .session()
        .query(
            "MATCH (n:Note) \
             CALL uni.nlp.sentences(n.text) YIELD sentence, sentiment \
             RETURN n.id AS id, sentence, sentiment ORDER BY id, sentence",
        )
        .await?;
    let got: Vec<_> = rows
        .iter()
        .map(|r| [val(r, "id"), val(r, "sentence"), val(r, "sentiment")])
        .collect();
    assert_eq!(
        got,
        [
            [int(1), int(0), s("positive")],
            [int(2), int(0), s("negative")],
            [int(2), int(1), s("neutral")],
        ],
        "each node's sentences count from 0; a node without text yields nothing"
    );
    Ok(())
}

#[tokio::test]
async fn several_documents_are_numbered_by_doc() -> Result<()> {
    let (db, _) = db_with_mock().await?;
    let rows = db
        .session()
        .query(
            "CALL uni.nlp.words(['One two.', 'Three.']) YIELD doc, word, text \
             RETURN * ORDER BY doc, word",
        )
        .await?;
    let got: Vec<_> = rows
        .iter()
        .map(|r| [val(r, "doc"), val(r, "word"), val(r, "text")])
        .collect();
    assert_eq!(
        got,
        [
            [int(0), int(0), s("One")],
            [int(0), int(1), s("two.")],
            [int(1), int(0), s("Three.")],
        ]
    );
    Ok(())
}

#[tokio::test]
async fn analysis_errors_are_reported() -> Result<()> {
    let (db, _) = db_with_mock().await?;
    let err = db
        .session()
        .query("CALL uni.nlp.words('Ada wrote.', {alias: 'nlp/v1'}) YIELD word RETURN word")
        .await
        .expect_err("a v1 model has no lemmas");
    assert!(err.to_string().contains("kniv v7"), "{err}");
    let err = db
        .session()
        .query("CALL uni.nlp.sentences('Ada wrote.', {alias: 'nlp/v1'}) YIELD sentence RETURN sentence")
        .await
        .expect_err("a v1 model has no sentiment");
    assert!(err.to_string().contains("kniv v7"), "{err}");
    let err = db
        .session()
        .query("CALL uni.nlp.words('Ada.', {relations: ['r']}) YIELD word RETURN word")
        .await
        .expect_err("words take no relations");
    assert!(
        err.to_string().contains("unknown option 'relations'"),
        "{err}"
    );
    let err = db
        .session()
        .query("CALL uni.nlp.sentences('Ada.', 'nlp/default') YIELD sentence RETURN sentence")
        .await
        .expect_err("options must be a map");
    assert!(err.to_string().contains("options must be a map"), "{err}");
    let err = db
        .session()
        .query("CALL uni.nlp.words(42) YIELD word RETURN word")
        .await
        .expect_err("input must be text");
    assert!(err.to_string().contains("input must be"), "{err}");

    let bare = Uni::temporary().build().await?;
    let err = bare
        .session()
        .query("CALL uni.nlp.sentences('Ada.') YIELD sentence RETURN sentence")
        .await
        .expect_err("no runtime");
    assert!(err.to_string().contains("not configured"), "{err}");
    Ok(())
}

#[tokio::test]
async fn the_facade_returns_the_procedure_rows() -> Result<()> {
    let (db, _) = db_with_mock().await?;
    let input = NlpInput::Text("Ada wrote notes. Is it done?".into());
    let words = db
        .xervo()
        .analyze_words(&input, &AnalyzeOptions::new())
        .await?;
    assert_eq!(words.len(), 6);
    assert_eq!(words[2].lemma.as_deref(), Some("notes"));
    assert_eq!((words[4].word, words[4].head), (4, Some(3)));
    let sentences = db
        .xervo()
        .analyze_sentences(&input, &AnalyzeOptions::new().with_alias("nlp/default"))
        .await?;
    assert_eq!(sentences.len(), 2);
    assert_eq!(sentences[1].acts, ["question"]);
    assert_eq!(sentences[1].act_scores.get("question"), Some(&0.75));
    let err = db
        .xervo()
        .analyze_words(&input, &AnalyzeOptions::new().with_alias("nlp/v1"))
        .await
        .expect_err("v1");
    assert!(err.to_string().contains("kniv v7"), "{err}");
    Ok(())
}

#[tokio::test]
#[ignore = "downloads the ~2 GB kniv-v7-xlarge-u-en model"]
async fn real_kniv_v7_analyses_words_and_sentences() -> Result<()> {
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
    db.xervo().prefetch(&["nlp/default"]).await?;
    let text = "Ada Lovelace wrote the first program. I love it! Did Babbage build the engine?";
    let words = db
        .xervo()
        .analyze_words(&NlpInput::Text(text.into()), &AnalyzeOptions::new())
        .await?;
    let word = |t: &str| words.iter().find(|w| w.text == t).expect(t);
    assert_eq!(word("wrote").lemma.as_deref(), Some("write"));
    assert_eq!(word("Ada").pos.as_deref(), Some("PROPN"));
    for w in &words {
        let (Some(start), Some(end)) = (w.start, w.end) else {
            panic!("{w:?} has no span");
        };
        let surface: String = text.chars().skip(start).take(end - start).collect();
        assert_eq!(surface, w.text, "offsets are characters into the text");
        if let Some(h) = w.head {
            assert_eq!(
                words[h].sentence, w.sentence,
                "{w:?}: a head stays in its sentence"
            );
        }
    }
    let sentence_ids: std::collections::BTreeSet<_> =
        words.iter().filter_map(|w| w.sentence).collect();
    for s in &sentence_ids {
        let roots = words
            .iter()
            .filter(|w| w.sentence == Some(*s) && w.head.is_none())
            .count();
        assert_eq!(roots, 1, "sentence {s} has one root");
    }

    let sentences = db
        .xervo()
        .analyze_sentences(&NlpInput::Text(text.into()), &AnalyzeOptions::new())
        .await?;
    assert_eq!(sentences.len(), 3, "{sentences:?}");
    assert_eq!(sentences[1].sentiment.as_deref(), Some("positive"));
    for s in &sentences {
        assert!(s.whole);
        assert!(
            !s.act_scores.is_empty(),
            "the model publishes its act vocabulary"
        );
        assert!(s.act_scores.values().all(|p| (0.0..=1.0).contains(p)));
        for act in &s.acts {
            assert!(s.act_scores.contains_key(act), "{act} is a known act");
        }
    }

    // The feature page's two query examples, against the real model.
    let triples = db
        .session()
        .query(
            "WITH 'Ada Lovelace wrote the first program. Babbage built the engine.' AS text \
             CALL uni.nlp.words(text) YIELD word, lemma, head, deprel \
             WITH collect({word: word, lemma: lemma, head: head, deprel: deprel}) AS ws \
             UNWIND ws AS subj \
             UNWIND ws AS obj \
             WITH ws, subj, obj \
             WHERE subj.deprel = 'nsubj' AND obj.deprel = 'obj' AND subj.head = obj.head \
             UNWIND ws AS verb \
             WITH subj, verb, obj WHERE verb.word = subj.head \
             RETURN subj.lemma + ' ' + verb.lemma + ' ' + obj.lemma AS triple ORDER BY triple",
        )
        .await?;
    let triples: Vec<String> = triples
        .rows()
        .iter()
        .map(|r| r.get::<String>("triple").unwrap_or_default())
        .collect();
    assert_eq!(triples, ["Ada write program", "Babbage build engine"]);
    let tx = db.session().tx().await?;
    tx.execute(
        "CREATE (:Turn {conversation: 1, idx: 0, speaker: 'user', \
                        text: 'Why is it broken? I hate this.'}), \
                (:Turn {conversation: 1, idx: 1, speaker: 'assistant', text: 'Sorry about that.'}), \
                (:Turn {conversation: 2, idx: 0, speaker: 'user', text: 'Thanks, that works.'})",
    )
    .await?;
    tx.commit().await?;
    let counts = db
        .session()
        .query(
            "MATCH (t:Turn) \
             WITH t ORDER BY t.conversation, t.idx \
             WITH t.conversation AS conversation, \
                  collect({speaker: t.speaker, text: t.text}) AS turns \
             CALL uni.nlp.sentences(turns) YIELD speaker, sentiment, acts \
             WITH conversation, speaker, sentiment, acts WHERE speaker = 'user' \
             RETURN conversation, \
                    count(CASE WHEN 'Question' IN acts THEN 1 END) AS questions, \
                    count(CASE WHEN sentiment = 'negative' THEN 1 END) AS negative \
             ORDER BY conversation",
        )
        .await?;
    let counts: Vec<[Value; 3]> = counts
        .rows()
        .iter()
        .map(|r| {
            [
                val(r, "conversation"),
                val(r, "questions"),
                val(r, "negative"),
            ]
        })
        .collect();
    assert_eq!(counts, [[int(1), int(1), int(1)], [int(2), int(0), int(0)]]);

    for w in &words {
        println!(
            "word {:2} {:10} pos={:6} lemma={:10} head={:?} deprel={:?} morph={:?}",
            w.word,
            w.text,
            w.pos.as_deref().unwrap_or("-"),
            w.lemma.as_deref().unwrap_or("-"),
            w.head,
            w.deprel.as_deref().unwrap_or("-"),
            w.morph.as_deref().unwrap_or("-"),
        );
    }
    for s in &sentences {
        println!(
            "sentence {} {:?} sentiment={:?} acts={:?}",
            s.sentence, s.text, s.sentiment, s.acts
        );
    }
    Ok(())
}
