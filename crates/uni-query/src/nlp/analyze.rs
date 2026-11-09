// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Linguistic analysis with a kniv v7 model, shared by the `uni.nlp.words` /
//! `uni.nlp.sentences` procedures and the `UniXervo::analyze_words` /
//! `analyze_sentences` facade.
//!
//! Two row grains, one per procedure:
//!
//! - [`AnalyzedWord`]: one row per word, with its part of speech, morphology,
//!   lemma, and dependency head and relation.
//! - [`AnalyzedSentence`]: one row per sentence, with its sentiment and
//!   dialogue acts.
//!
//! Word and sentence indices count across the whole document (every window),
//! so `head` joins to another row's `word` and a word's `sentence` joins to a
//! sentence row, on `doc`. Offsets count characters, as for extraction (see
//! the parent module): for a conversation they index the row's `turn`.

// Rust guideline compliant

use std::collections::BTreeMap;
use std::sync::Arc;

use uni_common::Value;
use uni_xervo::runtime::ModelRuntime;
use uni_xervo::traits::{NlpModel, NlpRequest, NlpResult, NlpTasks, Turn};

use super::{
    DEFAULT_NLP_ALIAS, DocLocator, NlpExtractError, NlpInput, Sources, invalid, warn_truncated,
};

/// Which rows a caller asks for.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AnalyzeKind {
    /// One row per word (`uni.nlp.words`).
    Words,
    /// One row per sentence (`uni.nlp.sentences`).
    Sentences,
}

impl AnalyzeKind {
    fn procedure(self) -> &'static str {
        match self {
            Self::Words => "uni.nlp.words",
            Self::Sentences => "uni.nlp.sentences",
        }
    }

    /// The heads each grain needs; the rest are left unrequested, which
    /// skips their decoding.
    fn tasks(self) -> NlpTasks {
        match self {
            Self::Words => NlpTasks::POS | NlpTasks::MORPH | NlpTasks::LEMMA | NlpTasks::DEP,
            Self::Sentences => NlpTasks::CLS | NlpTasks::SENTIMENT,
        }
    }
}

/// Options for [`run_analyze`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AnalyzeOptions {
    /// Catalog alias of the kniv v7 model.
    pub alias: String,
}

impl Default for AnalyzeOptions {
    fn default() -> Self {
        Self {
            alias: DEFAULT_NLP_ALIAS.to_owned(),
        }
    }
}

impl AnalyzeOptions {
    /// Options with the default alias.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Use the model registered under `alias`.
    #[must_use]
    pub fn with_alias(mut self, alias: impl Into<String>) -> Self {
        self.alias = alias.into();
        self
    }

    /// Parse the Cypher options map. `null` takes the defaults.
    ///
    /// # Errors
    ///
    /// [`NlpExtractError::InvalidArgument`] for a non-map, an unknown key,
    /// or a non-string `alias`.
    pub fn from_value(value: &Value) -> Result<Self, NlpExtractError> {
        const KEYS: &[&str] = &["alias"];
        let mut opts = Self::default();
        let map = match value {
            Value::Null => return Ok(opts),
            Value::Map(m) => m,
            _ => return Err(invalid("options must be a map")),
        };
        let mut keys: Vec<&String> = map.keys().collect();
        keys.sort();
        if let Some(key) = keys.into_iter().find(|k| !KEYS.contains(&k.as_str())) {
            return Err(invalid(format!(
                "unknown option '{key}' (accepted: {})",
                KEYS.join(", ")
            )));
        }
        if let Some(v) = map.get("alias").filter(|v| !v.is_null()) {
            opts.alias = v
                .as_str()
                .ok_or_else(|| invalid("option 'alias' must be a string"))?
                .to_owned();
        }
        Ok(opts)
    }
}

/// One word and its analysis.
#[derive(Debug, Clone, PartialEq)]
pub struct AnalyzedWord {
    /// Index of the input document (0 for one text or one conversation).
    pub doc: usize,
    /// Window index within the document.
    pub window: usize,
    /// Sentence index within the document; joins with
    /// [`AnalyzedSentence::sentence`].
    pub sentence: Option<usize>,
    /// Word index within the document.
    pub word: usize,
    /// The word as written.
    pub text: String,
    /// Character offset where the word starts.
    pub start: Option<usize>,
    /// Character offset where the word ends (exclusive).
    pub end: Option<usize>,
    /// Turn the word is in (conversations only).
    pub turn: Option<usize>,
    /// Speaker of that turn (conversations only).
    pub speaker: Option<String>,
    /// Universal Dependencies part-of-speech tag (UPOS), e.g. `"PROPN"`.
    pub pos: Option<String>,
    /// UD morphological features, e.g. `"Number=Sing|Tense=Past"`; `"_"`
    /// when the word has none.
    pub morph: Option<String>,
    /// The lemma.
    pub lemma: Option<String>,
    /// Document word index of the dependency head; `None` for the root of
    /// its sentence.
    pub head: Option<usize>,
    /// UD dependency relation to `head`, e.g. `"nsubj"`; `"root"` for the
    /// root.
    pub deprel: Option<String>,
    /// Catalog alias the model was resolved from.
    pub model_alias: String,
    /// Model identifier reported by the model.
    pub model_id: String,
}

/// One sentence and its classification.
#[derive(Debug, Clone, PartialEq)]
pub struct AnalyzedSentence {
    /// Index of the input document.
    pub doc: usize,
    /// Window index within the document.
    pub window: usize,
    /// Sentence index within the document.
    pub sentence: usize,
    /// The sentence as written.
    pub text: String,
    /// Character offset where the sentence starts.
    pub start: Option<usize>,
    /// Character offset where the sentence ends (exclusive).
    pub end: Option<usize>,
    /// Turn the sentence is in (conversations only).
    pub turn: Option<usize>,
    /// Speaker of that turn (conversations only).
    pub speaker: Option<String>,
    /// `"positive"`, `"negative"` or `"neutral"`.
    pub sentiment: Option<String>,
    /// Dialogue acts above the model's threshold. A sentence can have
    /// several, or none.
    pub acts: Vec<String>,
    /// Probability of every dialogue act the model knows, by name. Empty
    /// when the model does not publish its label vocabulary.
    pub act_scores: BTreeMap<String, f64>,
    /// `false` when the sentence ran past the encoder's limit, so its words
    /// from that point on carry no analysis.
    pub whole: bool,
    /// Catalog alias the model was resolved from.
    pub model_alias: String,
    /// Model identifier reported by the model.
    pub model_id: String,
}

/// The rows of one analysis call.
#[derive(Debug, Clone, PartialEq)]
pub enum AnalyzeRows {
    /// From [`AnalyzeKind::Words`].
    Words(Vec<AnalyzedWord>),
    /// From [`AnalyzeKind::Sentences`].
    Sentences(Vec<AnalyzedSentence>),
}

/// Run one analysis against `runtime`.
///
/// # Errors
///
/// [`NlpExtractError::Model`] when the alias does not resolve, resolves to a
/// model without kniv v7's linguistic heads, or the model fails.
pub async fn run_analyze(
    runtime: &ModelRuntime,
    input: &NlpInput,
    opts: &AnalyzeOptions,
    kind: AnalyzeKind,
) -> Result<AnalyzeRows, NlpExtractError> {
    let empty = || match kind {
        AnalyzeKind::Words => AnalyzeRows::Words(Vec::new()),
        AnalyzeKind::Sentences => AnalyzeRows::Sentences(Vec::new()),
    };
    let texts: Vec<&str> = match input {
        NlpInput::Text(t) => vec![t.as_str()],
        NlpInput::Texts(ts) => ts.iter().map(String::as_str).collect(),
        NlpInput::Turns(_) => Vec::new(),
    };
    let turns: Vec<Turn<'_>> = match input {
        NlpInput::Turns(ts) => ts.iter().map(|(s, t)| Turn::new(s, t)).collect(),
        _ => Vec::new(),
    };
    if texts.is_empty() && turns.is_empty() {
        return Ok(empty());
    }

    let model: Arc<dyn NlpModel> = runtime
        .nlp_model(&opts.alias)
        .await
        .map_err(|e| NlpExtractError::Model(format!("NLP alias '{}': {e}", opts.alias)))?;
    let needed = kind.tasks();
    // Lemmas and sentiment are kniv v7 heads, and only v7 reports windows.
    if !model.supported_tasks().contains(needed) {
        return Err(NlpExtractError::Model(format!(
            "NLP alias '{}' resolves to '{}', which lacks the heads {} needs; \
             it needs a kniv v7 model such as dragonscale-ai/kniv-v7-xlarge-u-en",
            opts.alias,
            model.model_id(),
            kind.procedure()
        )));
    }
    let requests: Vec<NlpRequest<'_>> = if turns.is_empty() {
        texts.iter().map(|t| NlpRequest::new(t, needed)).collect()
    } else {
        vec![NlpRequest::from_turns(&turns, needed)]
    };
    let expected = requests.len();
    let results = model
        .analyze(requests)
        .await
        .map_err(|e| NlpExtractError::Model(format!("NLP alias '{}': {e}", opts.alias)))?;
    if results.len() != expected {
        return Err(NlpExtractError::Model(format!(
            "NLP alias '{}' returned {} results for {expected} documents",
            opts.alias,
            results.len()
        )));
    }

    // uni-xervo drops blank turns before analysis, so a sentence's `unit`
    // indexes the non-blank turns.
    let turn_of_unit: Vec<usize> = turns
        .iter()
        .enumerate()
        .filter(|(_, t)| !t.text.trim().is_empty())
        .map(|(i, _)| i)
        .collect();
    let turn_texts: Vec<&str> = turns.iter().map(|t| t.text).collect();
    let sources = if turns.is_empty() {
        Sources::Texts(&texts)
    } else {
        Sources::Turns {
            turn_of_unit: &turn_of_unit,
            texts: &turn_texts,
        }
    };
    let names = Provenance {
        alias: &opts.alias,
        model_id: model.model_id(),
        acts: model
            .label_maps()
            .map(|m| m.cls.as_slice())
            .unwrap_or_default(),
    };
    Ok(match kind {
        AnalyzeKind::Words => AnalyzeRows::Words(flatten_words(&results, &sources, &names)),
        AnalyzeKind::Sentences => {
            AnalyzeRows::Sentences(flatten_sentences(&results, &sources, &names))
        }
    })
}

/// Model identity for the rows, and the dialogue-act vocabulary that
/// `cls_scores` is parallel to.
struct Provenance<'a> {
    alias: &'a str,
    model_id: &'a str,
    acts: &'a [String],
}

fn flatten_words(
    results: &[NlpResult],
    sources: &Sources<'_>,
    names: &Provenance<'_>,
) -> Vec<AnalyzedWord> {
    warn_truncated(results);
    let mut rows = Vec::new();
    for (doc, result) in results.iter().enumerate() {
        let locator = DocLocator::new(result, sources, doc);
        let (mut base, mut sentence_base) = (0usize, 0usize);
        for (w, window) in result.windows.iter().enumerate() {
            for (i, token) in window.tokens.iter().enumerate() {
                let sentence = window
                    .sentences
                    .iter()
                    .position(|s| s.start <= i && i < s.end);
                let located = locator.locate(w, base, (i, i + 1));
                let chars = locator.in_chars(&located);
                let analysis = window.words.get(i);
                // 0 is the sentence root; otherwise a 1-based window index.
                let head = analysis
                    .and_then(|a| a.head)
                    .and_then(|h| h.checked_sub(1))
                    .map(|h| base + h);
                rows.push(AnalyzedWord {
                    doc,
                    window: w,
                    sentence: sentence.map(|s| sentence_base + s),
                    word: base + i,
                    text: locator
                        .surface(&located)
                        .map_or_else(|| token.clone(), str::to_owned),
                    start: chars.start,
                    end: chars.end,
                    turn: chars.turn,
                    speaker: sentence
                        .and_then(|s| window.sentences.get(s))
                        .and_then(|s| s.speaker.clone()),
                    pos: analysis.and_then(|a| a.pos.clone()),
                    morph: analysis.and_then(|a| a.morph.clone()),
                    lemma: analysis.and_then(|a| a.lemma.clone()),
                    head,
                    deprel: analysis.and_then(|a| a.deprel.clone()),
                    model_alias: names.alias.to_owned(),
                    model_id: names.model_id.to_owned(),
                });
            }
            base += window.tokens.len();
            sentence_base += window.sentences.len();
        }
    }
    rows
}

fn flatten_sentences(
    results: &[NlpResult],
    sources: &Sources<'_>,
    names: &Provenance<'_>,
) -> Vec<AnalyzedSentence> {
    warn_truncated(results);
    let mut rows = Vec::new();
    for (doc, result) in results.iter().enumerate() {
        let locator = DocLocator::new(result, sources, doc);
        let (mut base, mut sentence_base) = (0usize, 0usize);
        for (w, window) in result.windows.iter().enumerate() {
            for (i, sent) in window.sentences.iter().enumerate() {
                let located = locator.locate(w, base, (sent.start, sent.end));
                let chars = locator.in_chars(&located);
                let text = locator.surface(&located).map_or_else(
                    || {
                        window
                            .tokens
                            .get(sent.start..sent.end)
                            .unwrap_or_default()
                            .join(" ")
                    },
                    str::to_owned,
                );
                let act_scores = if names.acts.len() == sent.cls_scores.len() {
                    names
                        .acts
                        .iter()
                        .zip(&sent.cls_scores)
                        .map(|(act, p)| (act.clone(), f64::from(*p)))
                        .collect()
                } else {
                    BTreeMap::new()
                };
                rows.push(AnalyzedSentence {
                    doc,
                    window: w,
                    sentence: sentence_base + i,
                    text,
                    start: chars.start,
                    end: chars.end,
                    turn: chars.turn,
                    speaker: sent.speaker.clone(),
                    sentiment: sent.sentiment.clone(),
                    acts: sent.cls.clone(),
                    act_scores,
                    whole: sent.whole,
                    model_alias: names.alias.to_owned(),
                    model_id: names.model_id.to_owned(),
                });
            }
            base += window.tokens.len();
            sentence_base += window.sentences.len();
        }
    }
    rows
}

#[cfg(test)]
mod tests {
    use super::*;
    use uni_xervo::traits::{NlpToken, NlpWindow, WindowSentence, WordAnalysis};

    fn s(v: &str) -> Value {
        Value::String(v.to_owned())
    }

    const PROV: Provenance<'static> = Provenance {
        alias: "nlp/default",
        model_id: "kniv",
        acts: &[],
    };

    /// `text` split on spaces into windows of whole sentences: each inner
    /// slice of `layout` is a window, each number a sentence's word count.
    /// Every word is analysed, its head the first word of its sentence (the
    /// root). Byte offsets index `text`.
    fn result(text: &str, layout: &[&[usize]], acts: usize) -> NlpResult {
        let mut words = text.split(' ').scan(0usize, |at, w| {
            let start = *at;
            *at += w.len() + 1;
            Some((w.to_owned(), start))
        });
        let mut result = NlpResult::default();
        let mut index = 0;
        for sentences in layout {
            let mut window = NlpWindow::default();
            for &len in *sentences {
                let first = window.tokens.len();
                for k in 0..len {
                    let (w, start) = words.next().expect("layout fits the text");
                    let mut tok = NlpToken::default();
                    tok.start = start;
                    tok.end = start + w.len();
                    tok.word_index = index;
                    index += 1;
                    result.tokens.push(tok);
                    let mut a = WordAnalysis::default();
                    a.pos = Some(if k == 0 { "PROPN" } else { "VERB" }.into());
                    a.lemma = Some(w.to_lowercase());
                    a.morph = Some("_".into());
                    a.head = Some(if k == 0 { 0 } else { first + 1 });
                    a.deprel = Some(if k == 0 { "root" } else { "dep" }.into());
                    window.words.push(a);
                    window.tokens.push(w);
                }
                let mut sent = WindowSentence::default();
                sent.start = first;
                sent.end = window.tokens.len();
                sent.sentiment = Some("neutral".into());
                sent.cls = vec!["statement".into()];
                sent.cls_scores = (0..acts).map(|i| i as f32 / 10.0).collect();
                sent.whole = true;
                window.sentences.push(sent);
            }
            window.analysed_words = window.tokens.len();
            result.windows.push(window);
        }
        result
    }

    #[test]
    fn options_take_only_an_alias() {
        assert_eq!(
            AnalyzeOptions::from_value(&Value::Null).unwrap(),
            AnalyzeOptions::default()
        );
        let opts =
            AnalyzeOptions::from_value(&Value::Map([("alias".into(), s("nlp/x"))].into())).unwrap();
        assert_eq!(opts.alias, "nlp/x");
        let err = AnalyzeOptions::from_value(&Value::Map([("relations".into(), s("x"))].into()))
            .unwrap_err();
        assert!(
            err.to_string().contains("unknown option 'relations'"),
            "{err}"
        );
        assert!(AnalyzeOptions::from_value(&s("nlp/x")).is_err());
    }

    #[test]
    fn word_and_head_indices_count_across_windows() {
        let text = "Ada wrote notes Babbage built engines";
        let results = [result(text, &[&[3], &[3]], 0)];
        let texts = [text];
        let rows = flatten_words(&results, &Sources::Texts(&texts), &PROV);
        let got: Vec<_> = rows
            .iter()
            .map(|r| (r.window, r.sentence, r.word, r.text.as_str(), r.head))
            .collect();
        assert_eq!(
            got,
            [
                (0, Some(0), 0, "Ada", None),
                (0, Some(0), 1, "wrote", Some(0)),
                (0, Some(0), 2, "notes", Some(0)),
                (1, Some(1), 3, "Babbage", None),
                (1, Some(1), 4, "built", Some(3)),
                (1, Some(1), 5, "engines", Some(3)),
            ]
        );
        let built = &rows[4];
        assert_eq!((built.start, built.end), (Some(24), Some(29)));
        assert_eq!(built.lemma.as_deref(), Some("built"));
        assert_eq!(built.deprel.as_deref(), Some("dep"));
        assert_eq!(rows[3].deprel.as_deref(), Some("root"));
    }

    #[test]
    fn sentence_rows_carry_spans_text_and_act_scores() {
        let text = "Ada wrote Babbage built engines";
        let results = [result(text, &[&[2, 3]], 2)];
        let texts = [text];
        let acts = ["question".to_owned(), "statement".to_owned()];
        let names = Provenance {
            acts: &acts,
            ..PROV
        };
        let rows = flatten_sentences(&results, &Sources::Texts(&texts), &names);
        assert_eq!(rows.len(), 2);
        assert_eq!(
            (rows[1].sentence, rows[1].text.as_str()),
            (1, "Babbage built engines")
        );
        assert_eq!((rows[1].start, rows[1].end), (Some(10), Some(31)));
        assert_eq!(rows[1].acts, ["statement"]);
        assert_eq!(
            rows[1].act_scores,
            BTreeMap::from([
                ("question".into(), 0.0),
                ("statement".into(), 0.1f32.into())
            ])
        );
        // No vocabulary, or one of the wrong length: no scores.
        let rows = flatten_sentences(&results, &Sources::Texts(&texts), &PROV);
        assert!(rows[0].act_scores.is_empty());
    }

    #[test]
    fn words_past_the_encoder_limit_have_no_analysis() {
        let text = "Ada wrote notes";
        let mut r = result(text, &[&[3]], 0);
        let window = &mut r.windows[0];
        window.analysed_words = 2;
        window.words.truncate(2);
        window.sentences[0].whole = false;
        let texts = [text];
        let words = flatten_words(&[r.clone()], &Sources::Texts(&texts), &PROV);
        assert_eq!(words.len(), 3, "every word is reported");
        assert_eq!(words[2].text, "notes");
        assert!(words[2].pos.is_none() && words[2].head.is_none());
        let sentences = flatten_sentences(&[r], &Sources::Texts(&texts), &PROV);
        assert!(!sentences[0].whole);
    }

    #[test]
    fn conversation_rows_name_their_turn_and_speaker() {
        // Two turns, one blank between them, each analysed as its own text.
        let turns = ["Hi there", "  ", "Bye now"];
        let mut r = result("Hi there", &[&[2]], 0);
        let second = result("Bye now", &[&[2]], 0);
        r.windows[0].sentences[0].speaker = Some("ada".into());
        let mut w2 = second.windows[0].clone();
        w2.sentences[0].unit = 1;
        w2.sentences[0].speaker = Some("bob".into());
        for (k, mut tok) in second.tokens.into_iter().enumerate() {
            tok.word_index = 2 + k;
            r.tokens.push(tok);
        }
        r.windows.push(w2);
        let turn_of_unit = [0, 2];
        let sources = Sources::Turns {
            turn_of_unit: &turn_of_unit,
            texts: &turns,
        };
        let words = flatten_words(&[r.clone()], &sources, &PROV);
        let bye = &words[2];
        assert_eq!(
            (
                bye.word,
                bye.text.as_str(),
                bye.turn,
                bye.speaker.as_deref()
            ),
            (2, "Bye", Some(2), Some("bob"))
        );
        assert_eq!((bye.start, bye.end), (Some(0), Some(3)));
        let sentences = flatten_sentences(&[r], &sources, &PROV);
        assert_eq!(
            (sentences[1].turn, sentences[1].text.as_str()),
            (Some(2), "Bye now")
        );
    }
}
