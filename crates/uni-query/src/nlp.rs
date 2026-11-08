// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Text-to-graph extraction with a kniv v7 model, shared by the
//! `uni.nlp.extract` / `uni.nlp.entities` procedures and the
//! `UniXervo::extract_facts` / `extract_entities` facade.
//!
//! The model analyses windows of whole sentences and reports typed entities
//! and the facts between them, under an extraction schema supplied per
//! request. This module turns a caller's input and options into
//! [`NlpRequest`]s, runs them in one `analyze` call, and flattens the
//! per-window results into one row per fact ([`ExtractedFact`]) or per
//! entity ([`ExtractedEntity`]), with UTF-8 byte spans into the caller's
//! text and the model's identity for provenance.
//!
//! Offsets: for prose they index the input string. For a conversation
//! uni-xervo analyses each turn as its own text, so offsets index the turn
//! named by the row's `*_turn` column. Speaker entities have no mention in
//! the text and carry no span.

// Rust guideline compliant

use std::collections::{HashMap, HashSet};
use std::fmt;
use std::sync::Arc;

use uni_common::Value;
use uni_common::core::schema::{Schema, SchemaElementState};
use uni_xervo::runtime::ModelRuntime;
use uni_xervo::traits::{
    ExtractionSchema, LabelSpec, NlpCalibrationOverrides, NlpModel, NlpRequest, NlpResult,
    NlpTasks, NlpToken, NlpWindow, Turn,
};

/// The catalog alias used when the caller names none.
pub const DEFAULT_NLP_ALIAS: &str = "nlp/default";

/// Why an extraction could not run.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NlpExtractError {
    /// The input or options are malformed; nothing was sent to the model.
    InvalidArgument(String),
    /// No Uni-Xervo runtime is attached to the database.
    NotConfigured,
    /// The model could not be loaded, is not a kniv v7 model, or failed.
    Model(String),
}

impl fmt::Display for NlpExtractError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidArgument(msg) | Self::Model(msg) => f.write_str(msg),
            Self::NotConfigured => f.write_str(
                "Uni-Xervo runtime is not configured; attach a catalog with an `nlp` \
                 alias to use NLP extraction",
            ),
        }
    }
}

impl std::error::Error for NlpExtractError {}

fn invalid(msg: impl Into<String>) -> NlpExtractError {
    NlpExtractError::InvalidArgument(msg.into())
}

/// What a caller asks to extract.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExtractKind {
    /// Facts between entities (`uni.nlp.extract`). Needs relations.
    Facts,
    /// Entities only (`uni.nlp.entities`). Skips the relation pass.
    Entities,
}

impl ExtractKind {
    fn procedure(self) -> &'static str {
        match self {
            Self::Facts => "uni.nlp.extract",
            Self::Entities => "uni.nlp.entities",
        }
    }

    fn tasks(self) -> NlpTasks {
        match self {
            Self::Facts => NlpTasks::FACTS,
            Self::Entities => NlpTasks::ENTITIES,
        }
    }
}

/// The text to analyse.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NlpInput {
    /// One document of prose.
    Text(String),
    /// Several documents, analysed in one batched model call.
    Texts(Vec<String>),
    /// One conversation, as `(speaker, text)` turns. Each speaker becomes an
    /// entity, so a first-person statement is a fact about its speaker.
    Turns(Vec<(String, String)>),
}

impl NlpInput {
    /// Parse the Cypher form: a string, a list of strings, or a list of
    /// `{speaker, text}` maps.
    ///
    /// # Errors
    ///
    /// [`NlpExtractError::InvalidArgument`] for any other shape.
    pub fn from_value(value: &Value) -> Result<Self, NlpExtractError> {
        const SHAPE: &str = "input must be a string, a list of strings, or a list of \
                             {speaker, text} maps";
        match value {
            Value::String(s) => Ok(Self::Text(s.clone())),
            Value::List(items) => {
                if items.iter().all(|v| matches!(v, Value::String(_))) {
                    return Ok(Self::Texts(
                        items
                            .iter()
                            .filter_map(|v| v.as_str().map(str::to_owned))
                            .collect(),
                    ));
                }
                let mut turns = Vec::with_capacity(items.len());
                for (i, item) in items.iter().enumerate() {
                    let map = item.as_object().ok_or_else(|| invalid(SHAPE))?;
                    for key in map.keys() {
                        if key != "speaker" && key != "text" {
                            return Err(invalid(format!(
                                "input turn {i}: unknown key '{key}' (expected speaker, text)"
                            )));
                        }
                    }
                    let field = |k: &str| {
                        map.get(k).and_then(Value::as_str).ok_or_else(|| {
                            invalid(format!("input turn {i}: '{k}' must be a string"))
                        })
                    };
                    turns.push((field("speaker")?.to_owned(), field("text")?.to_owned()));
                }
                Ok(Self::Turns(turns))
            }
            _ => Err(invalid(SHAPE)),
        }
    }
}

/// Which slice of the declared schema to extract with.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SchemaSource {
    /// Every active label and edge type.
    Graph,
    /// Only the named labels and edge types. `None` takes all of that kind.
    Subset {
        /// Labels to use as entity types.
        labels: Option<Vec<String>>,
        /// Edge types to use as relations.
        edge_types: Option<Vec<String>>,
    },
}

/// Options for one extraction call.
#[derive(Debug, Clone, PartialEq)]
pub struct ExtractOptions {
    /// Catalog alias of a kniv v7 model. Defaults to [`DEFAULT_NLP_ALIAS`].
    pub alias: String,
    /// Relations to extract, by name and description.
    pub relations: Vec<LabelSpec>,
    /// Entity types; `None` uses the model's coarse types.
    pub entity_types: Option<Vec<LabelSpec>>,
    /// Derive relations and entity types from the declared schema instead.
    pub schema: Option<SchemaSource>,
    /// Facts kept per entity pair; `None` uses the model's calibrated value.
    pub top_per_pair: Option<usize>,
    /// Per-relation keep-threshold offsets. A fact is kept when its raw
    /// margin exceeds `-offset`, so a lower offset is a higher bar.
    pub rel_offsets: HashMap<String, f32>,
}

impl Default for ExtractOptions {
    fn default() -> Self {
        Self {
            alias: DEFAULT_NLP_ALIAS.to_owned(),
            relations: Vec::new(),
            entity_types: None,
            schema: None,
            top_per_pair: None,
            rel_offsets: HashMap::new(),
        }
    }
}

impl ExtractOptions {
    /// Options with the default alias and nothing else set.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Use the catalog alias `alias`.
    #[must_use]
    pub fn with_alias(mut self, alias: impl Into<String>) -> Self {
        self.alias = alias.into();
        self
    }

    /// Add a relation. Say in `description` which side is the head, e.g.
    /// `"the place where the head resides"`.
    #[must_use]
    pub fn relation(mut self, name: impl Into<String>, description: Option<&str>) -> Self {
        self.relations.push(LabelSpec {
            name: name.into(),
            description: description.map(str::to_owned),
        });
        self
    }

    /// Add an entity type.
    #[must_use]
    pub fn entity_type(mut self, name: impl Into<String>, description: Option<&str>) -> Self {
        self.entity_types
            .get_or_insert_with(Vec::new)
            .push(LabelSpec {
                name: name.into(),
                description: description.map(str::to_owned),
            });
        self
    }

    /// Derive relations and entity types from the declared schema.
    #[must_use]
    pub fn graph_schema(mut self) -> Self {
        self.schema = Some(SchemaSource::Graph);
        self
    }

    /// Parse the Cypher `options` map. `Null` means all defaults.
    ///
    /// # Errors
    ///
    /// [`NlpExtractError::InvalidArgument`] for an unknown key, a value of the
    /// wrong type, or a combination [`ExtractOptions::validate`] rejects.
    pub fn from_value(value: &Value, kind: ExtractKind) -> Result<Self, NlpExtractError> {
        const KEYS: &[&str] = &[
            "alias",
            "relations",
            "entity_types",
            "schema",
            "top_per_pair",
            "rel_offsets",
        ];
        let mut opts = Self::default();
        let map = match value {
            Value::Null => {
                opts.validate(kind)?;
                return Ok(opts);
            }
            Value::Map(m) => m,
            _ => return Err(invalid("options must be a map")),
        };
        let mut keys: Vec<&String> = map.keys().collect();
        keys.sort();
        for key in keys {
            if !KEYS.contains(&key.as_str()) {
                return Err(invalid(format!(
                    "unknown option '{key}' (accepted: {})",
                    KEYS.join(", ")
                )));
            }
        }
        if let Some(v) = map.get("alias").filter(|v| !v.is_null()) {
            opts.alias = v
                .as_str()
                .ok_or_else(|| invalid("option 'alias' must be a string"))?
                .to_owned();
        }
        if let Some(v) = map.get("relations").filter(|v| !v.is_null()) {
            opts.relations = parse_label_specs(v, "relations")?;
        }
        if let Some(v) = map.get("entity_types").filter(|v| !v.is_null()) {
            opts.entity_types = Some(parse_label_specs(v, "entity_types")?);
        }
        if let Some(v) = map.get("schema").filter(|v| !v.is_null()) {
            opts.schema = Some(parse_schema_source(v)?);
        }
        if let Some(v) = map.get("top_per_pair").filter(|v| !v.is_null()) {
            let n = v
                .as_i64()
                .filter(|n| *n > 0)
                .ok_or_else(|| invalid("option 'top_per_pair' must be a positive integer"))?;
            opts.top_per_pair = Some(usize::try_from(n).unwrap_or(usize::MAX));
        }
        if let Some(v) = map.get("rel_offsets").filter(|v| !v.is_null()) {
            let m = v
                .as_object()
                .ok_or_else(|| invalid("option 'rel_offsets' must be a map"))?;
            for (rel, off) in m {
                let off = off.as_f64().ok_or_else(|| {
                    invalid(format!("option 'rel_offsets': '{rel}' must be a number"))
                })?;
                opts.rel_offsets.insert(rel.clone(), off as f32);
            }
        }
        opts.validate(kind)?;
        Ok(opts)
    }

    /// Check the option combination for `kind`.
    ///
    /// # Errors
    ///
    /// [`NlpExtractError::InvalidArgument`] when `schema` is combined with
    /// `relations` or `entity_types`, when facts are requested with no
    /// relations, or when relation-only options are given for entities.
    pub fn validate(&self, kind: ExtractKind) -> Result<(), NlpExtractError> {
        let proc = kind.procedure();
        if self.alias.is_empty() {
            return Err(invalid("option 'alias' must not be empty"));
        }
        if self.schema.is_some() && (!self.relations.is_empty() || self.entity_types.is_some()) {
            return Err(invalid(format!(
                "{proc}: option 'schema' cannot be combined with 'relations' or 'entity_types'"
            )));
        }
        match kind {
            ExtractKind::Facts => {
                if self.schema.is_none() && self.relations.is_empty() {
                    return Err(invalid(format!(
                        "{proc}: give 'relations' (or schema: 'graph'); the relation pass \
                         needs a relation list"
                    )));
                }
            }
            ExtractKind::Entities => {
                for (set, key) in [
                    (!self.relations.is_empty(), "relations"),
                    (self.top_per_pair.is_some(), "top_per_pair"),
                    (!self.rel_offsets.is_empty(), "rel_offsets"),
                ] {
                    if set {
                        return Err(invalid(format!(
                            "{proc}: option '{key}' applies only to uni.nlp.extract"
                        )));
                    }
                }
            }
        }
        Ok(())
    }
}

fn parse_label_specs(value: &Value, key: &str) -> Result<Vec<LabelSpec>, NlpExtractError> {
    let items = value.as_array().ok_or_else(|| {
        invalid(format!(
            "option '{key}' must be a list of names or {{name, description}} maps"
        ))
    })?;
    let mut specs = Vec::with_capacity(items.len());
    for item in items {
        match item {
            Value::String(name) if !name.trim().is_empty() => specs.push(LabelSpec {
                name: name.clone(),
                description: None,
            }),
            Value::Map(m) => {
                for k in m.keys() {
                    if k != "name" && k != "description" {
                        return Err(invalid(format!(
                            "option '{key}': unknown key '{k}' (expected name, description)"
                        )));
                    }
                }
                let name = m
                    .get("name")
                    .and_then(Value::as_str)
                    .filter(|n| !n.trim().is_empty())
                    .ok_or_else(|| {
                        invalid(format!(
                            "option '{key}': each entry needs a non-empty 'name'"
                        ))
                    })?;
                let description = match m.get("description") {
                    None | Some(Value::Null) => None,
                    Some(Value::String(d)) => Some(d.clone()),
                    Some(_) => {
                        return Err(invalid(format!(
                            "option '{key}': 'description' must be a string"
                        )));
                    }
                };
                specs.push(LabelSpec {
                    name: name.to_owned(),
                    description,
                });
            }
            _ => {
                return Err(invalid(format!(
                    "option '{key}' must be a list of names or {{name, description}} maps"
                )));
            }
        }
    }
    if specs.is_empty() {
        return Err(invalid(format!("option '{key}' must not be empty")));
    }
    let mut seen = HashSet::new();
    for spec in &specs {
        if !seen.insert(spec.name.as_str()) {
            return Err(invalid(format!(
                "option '{key}': '{}' is listed twice",
                spec.name
            )));
        }
    }
    Ok(specs)
}

fn parse_schema_source(value: &Value) -> Result<SchemaSource, NlpExtractError> {
    const SHAPE: &str = "option 'schema' must be 'graph' or a {labels, edge_types} map";
    match value {
        Value::String(s) if s == "graph" => Ok(SchemaSource::Graph),
        Value::Map(m) => {
            for k in m.keys() {
                if k != "labels" && k != "edge_types" {
                    return Err(invalid(format!(
                        "option 'schema': unknown key '{k}' (expected labels, edge_types)"
                    )));
                }
            }
            let names = |k: &str| -> Result<Option<Vec<String>>, NlpExtractError> {
                match m.get(k) {
                    None | Some(Value::Null) => Ok(None),
                    Some(Value::List(items)) => items
                        .iter()
                        .map(|v| {
                            v.as_str().map(str::to_owned).ok_or_else(|| {
                                invalid(format!("option 'schema': '{k}' must be a list of names"))
                            })
                        })
                        .collect::<Result<Vec<_>, _>>()
                        .map(Some),
                    Some(_) => Err(invalid(format!(
                        "option 'schema': '{k}' must be a list of names"
                    ))),
                }
            };
            Ok(SchemaSource::Subset {
                labels: names("labels")?,
                edge_types: names("edge_types")?,
            })
        }
        _ => Err(invalid(SHAPE)),
    }
}

/// Render a schema name as the model reads it: `LIVES_IN` → `"lives in"`,
/// `JobTitle` → `"job title"`.
#[must_use]
pub fn humanize(name: &str) -> String {
    let mut out = String::with_capacity(name.len() + 4);
    let mut prev: Option<char> = None;
    for c in name.chars() {
        if c == '_' || c == '-' || c.is_whitespace() {
            if !out.ends_with(' ') && !out.is_empty() {
                out.push(' ');
            }
        } else {
            if c.is_uppercase()
                && prev.is_some_and(|p| p.is_lowercase() || p.is_ascii_digit())
                && !out.ends_with(' ')
            {
                out.push(' ');
            }
            out.extend(c.to_lowercase());
        }
        prev = Some(c);
    }
    out.trim_end().to_owned()
}

/// Relations and entity types derived from a declared schema, with the map
/// from each humanized name back to its schema name.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct GraphSchemaSpecs {
    /// One per selected edge type, named as the model reads it.
    pub relations: Vec<LabelSpec>,
    /// One per selected label, named as the model reads it.
    pub entity_types: Vec<LabelSpec>,
    /// Humanized relation name → edge type name.
    pub relation_names: HashMap<String, String>,
    /// Humanized entity-type name → label name.
    pub entity_type_names: HashMap<String, String>,
    /// Selected edge types that have no description.
    pub undescribed_edge_types: Vec<String>,
}

/// Derive an extraction schema from the active labels and edge types of
/// `schema`, optionally restricted to a subset.
///
/// # Errors
///
/// [`NlpExtractError::InvalidArgument`] when a subset names an unknown or
/// inactive label or edge type, or two names humanize to the same text.
pub fn graph_schema_specs(
    schema: &Schema,
    source: &SchemaSource,
) -> Result<GraphSchemaSpecs, NlpExtractError> {
    let (labels, edge_types) = match source {
        SchemaSource::Graph => (None, None),
        SchemaSource::Subset { labels, edge_types } => (labels.as_ref(), edge_types.as_ref()),
    };
    let is_active = |s: &SchemaElementState| matches!(s, SchemaElementState::Active);

    let mut active_edges: Vec<(&String, Option<&String>)> = schema
        .edge_types
        .iter()
        .filter(|(_, m)| is_active(&m.state))
        .map(|(n, m)| (n, m.description.as_ref()))
        .collect();
    active_edges.sort_by(|a, b| a.0.cmp(b.0));
    let mut active_labels: Vec<(&String, Option<&String>)> = schema
        .labels
        .iter()
        .filter(|(_, m)| is_active(&m.state))
        .map(|(n, m)| (n, m.description.as_ref()))
        .collect();
    active_labels.sort_by(|a, b| a.0.cmp(b.0));

    let pick = |all: &[(&String, Option<&String>)],
                wanted: Option<&Vec<String>>,
                kind: &str|
     -> Result<Vec<(String, Option<String>)>, NlpExtractError> {
        let Some(wanted) = wanted else {
            return Ok(all
                .iter()
                .map(|(n, d)| ((*n).clone(), d.cloned()))
                .collect());
        };
        wanted
            .iter()
            .map(|w| {
                all.iter()
                    .find(|(n, _)| *n == w)
                    .map(|(n, d)| ((*n).clone(), d.cloned()))
                    .ok_or_else(|| {
                        invalid(format!(
                            "option 'schema': {kind} '{w}' is not an active declared {kind}"
                        ))
                    })
            })
            .collect()
    };

    let mut out = GraphSchemaSpecs::default();
    for (name, description) in pick(&active_edges, edge_types, "edge type")? {
        let human = humanize(&name);
        if let Some(prev) = out.relation_names.insert(human.clone(), name.clone()) {
            return Err(invalid(format!(
                "edge types '{prev}' and '{name}' both read as '{human}'; select one \
                 with schema: {{edge_types: [...]}}"
            )));
        }
        if description.is_none() {
            out.undescribed_edge_types.push(name.clone());
        }
        out.relations.push(LabelSpec {
            name: human,
            description,
        });
    }
    for (name, description) in pick(&active_labels, labels, "label")? {
        let human = humanize(&name);
        if let Some(prev) = out.entity_type_names.insert(human.clone(), name.clone()) {
            return Err(invalid(format!(
                "labels '{prev}' and '{name}' both read as '{human}'; select one with \
                 schema: {{labels: [...]}}"
            )));
        }
        out.entity_types.push(LabelSpec {
            name: human,
            description,
        });
    }
    Ok(out)
}

/// One extracted fact: `head --relation--> tail` with its attributes.
#[derive(Debug, Clone, PartialEq)]
pub struct ExtractedFact {
    /// Index of the input document (0 for one text or one conversation).
    pub doc: usize,
    /// Window index within the document.
    pub window: usize,
    /// Window-local index of the head entity.
    pub head: usize,
    /// Surface text of the head's first mention, or the speaker name.
    pub head_text: String,
    /// Head entity type; a label name under a graph-derived schema.
    pub head_type: String,
    /// Whether the head is a conversation participant.
    pub head_speaker: bool,
    /// UTF-8 byte offset where the head's first mention starts.
    pub head_start: Option<usize>,
    /// UTF-8 byte offset where the head's first mention ends (exclusive).
    pub head_end: Option<usize>,
    /// Turn the head's first mention is in (conversations only).
    pub head_turn: Option<usize>,
    /// Relation name as the caller gave it; an edge type name under a
    /// graph-derived schema.
    pub relation: String,
    /// Window-local index of the tail entity.
    pub tail: usize,
    /// Surface text of the tail's first mention, or the speaker name.
    pub tail_text: String,
    /// Tail entity type.
    pub tail_type: String,
    /// Whether the tail is a conversation participant.
    pub tail_speaker: bool,
    /// UTF-8 byte offset where the tail's first mention starts.
    pub tail_start: Option<usize>,
    /// UTF-8 byte offset where the tail's first mention ends (exclusive).
    pub tail_end: Option<usize>,
    /// Turn the tail's first mention is in (conversations only).
    pub tail_turn: Option<usize>,
    /// `"positive"` or `"negative"`.
    pub polarity: String,
    /// `"asserted"`, `"reported"`, `"planned"`, `"desired"`,
    /// `"questioned"` or `"hypothetical"`.
    pub modality: String,
    /// Window-local index of whoever holds the belief.
    pub holder: Option<usize>,
    /// Window-local index of a temporal entity anchoring the fact.
    pub time: Option<usize>,
    /// Calibrated margin over the keep threshold; always positive.
    pub margin: f64,
    /// `1 / (1 + e^-margin)`: a monotone rescaling into (0, 1), not a
    /// calibrated probability.
    pub confidence: f64,
    /// Catalog alias the model was resolved from.
    pub model_alias: String,
    /// Model identifier reported by the model.
    pub model_id: String,
}

/// One extracted entity, grouped across its mentions within a window.
#[derive(Debug, Clone, PartialEq)]
pub struct ExtractedEntity {
    /// Index of the input document.
    pub doc: usize,
    /// Window index within the document.
    pub window: usize,
    /// Window-local entity index; joins with [`ExtractedFact::head`] /
    /// [`ExtractedFact::tail`] on `(doc, window)`.
    pub entity: usize,
    /// Surface text of the first mention, or the speaker name.
    pub text: String,
    /// Entity type; a label name under a graph-derived schema.
    pub entity_type: String,
    /// Whether this is a conversation participant.
    pub speaker: bool,
    /// Surface text of each mention.
    pub mentions: Vec<String>,
    /// UTF-8 byte start of each mention, parallel to `mentions`.
    pub mention_starts: Vec<Option<usize>>,
    /// UTF-8 byte end of each mention (exclusive), parallel to `mentions`.
    pub mention_ends: Vec<Option<usize>>,
    /// Turn of each mention (conversations only), parallel to `mentions`.
    pub mention_turns: Vec<Option<usize>>,
    /// Catalog alias the model was resolved from.
    pub model_alias: String,
    /// Model identifier reported by the model.
    pub model_id: String,
}

/// The rows of one extraction call.
#[derive(Debug, Clone, PartialEq)]
pub enum ExtractRows {
    /// From [`ExtractKind::Facts`].
    Facts(Vec<ExtractedFact>),
    /// From [`ExtractKind::Entities`].
    Entities(Vec<ExtractedEntity>),
}

/// Where a window-local mention sits in the caller's text.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct Located {
    start: Option<usize>,
    end: Option<usize>,
    turn: Option<usize>,
}

/// Per-document context for locating window-local word spans.
struct DocLocator<'a> {
    result: &'a NlpResult,
    /// For a conversation: index among non-blank turns → original turn index.
    turn_of_unit: Option<&'a [usize]>,
}

impl DocLocator<'_> {
    fn locate(&self, window_idx: usize, base: usize, span: (usize, usize)) -> Located {
        let (s, e) = span;
        if e <= s {
            return Located::default();
        }
        let first: Option<&NlpToken> = self.result.tokens.get(base + s);
        let last: Option<&NlpToken> = self.result.tokens.get(base + e - 1);
        let turn = self.turn_of_unit.and_then(|map| {
            let window: &NlpWindow = self.result.windows.get(window_idx)?;
            let sentence = window
                .sentences
                .iter()
                .find(|sent| sent.start <= s && s < sent.end)?;
            map.get(sentence.unit).copied()
        });
        Located {
            start: first.map(|t| t.start),
            end: last.map(|t| t.end),
            turn,
        }
    }
}

/// Run one extraction against `runtime`.
///
/// `schema` is the declared schema used for `schema: 'graph'`; pass the
/// session's schema so a forked session sees its own additions.
///
/// # Errors
///
/// [`NlpExtractError::InvalidArgument`] for bad options,
/// [`NlpExtractError::Model`] when the alias does not resolve, resolves to a
/// model without kniv v7 entity/fact extraction, or the model fails.
pub async fn run_extract(
    runtime: &ModelRuntime,
    schema: &Schema,
    input: &NlpInput,
    opts: &ExtractOptions,
    kind: ExtractKind,
) -> Result<ExtractRows, NlpExtractError> {
    opts.validate(kind)?;

    // Resolve the extraction schema and the names to map results back to.
    let mut relation_names: HashMap<String, String> = HashMap::new();
    let mut entity_type_names: HashMap<String, String> = HashMap::new();
    let extraction = match &opts.schema {
        Some(source) => {
            let specs = graph_schema_specs(schema, source)?;
            if kind == ExtractKind::Facts && specs.relations.is_empty() {
                return Err(invalid(format!(
                    "{}: the selected schema has no active edge types to extract",
                    kind.procedure()
                )));
            }
            if kind == ExtractKind::Facts && !specs.undescribed_edge_types.is_empty() {
                tracing::warn!(
                    edge_types = ?specs.undescribed_edge_types,
                    "uni.nlp.extract: edge types without a description extract less \
                     accurately; describe which side is the head"
                );
            }
            relation_names = specs.relation_names;
            entity_type_names = specs.entity_type_names;
            ExtractionSchema {
                relations: if kind == ExtractKind::Facts {
                    specs.relations
                } else {
                    Vec::new()
                },
                entity_types: (!specs.entity_types.is_empty()).then_some(specs.entity_types),
                top_per_pair: opts.top_per_pair,
            }
        }
        None => ExtractionSchema {
            relations: opts.relations.clone(),
            entity_types: opts.entity_types.clone(),
            top_per_pair: opts.top_per_pair,
        },
    };

    // Relation offsets are keyed as the caller names relations; the model
    // keys them by the name it scored.
    let mut overrides = NlpCalibrationOverrides::default();
    let edge_to_human: HashMap<&str, &str> = relation_names
        .iter()
        .map(|(h, e)| (e.as_str(), h.as_str()))
        .collect();
    for (rel, off) in &opts.rel_offsets {
        let key = edge_to_human.get(rel.as_str()).copied().unwrap_or(rel);
        if kind == ExtractKind::Facts && !extraction.relations.iter().any(|r| r.name == key) {
            return Err(invalid(format!(
                "option 'rel_offsets': '{rel}' is not one of the relations being extracted"
            )));
        }
        overrides.rel_offsets.insert(key.to_owned(), *off);
    }

    // Owned documents and borrowed turns, alive across the `analyze` await.
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
        return Ok(match kind {
            ExtractKind::Facts => ExtractRows::Facts(Vec::new()),
            ExtractKind::Entities => ExtractRows::Entities(Vec::new()),
        });
    }

    let model: Arc<dyn NlpModel> = runtime
        .nlp_model(&opts.alias)
        .await
        .map_err(|e| NlpExtractError::Model(format!("NLP alias '{}': {e}", opts.alias)))?;
    let needed = kind.tasks();
    if !model.supported_tasks().contains(needed) {
        return Err(NlpExtractError::Model(format!(
            "NLP alias '{}' resolves to '{}', which has no entity/fact extraction; \
             {} needs a kniv v7 model such as dragonscale-ai/kniv-v7-xlarge-u-en",
            opts.alias,
            model.model_id(),
            kind.procedure()
        )));
    }

    fn with_extras<'a>(
        req: NlpRequest<'a>,
        schema: &'a ExtractionSchema,
        overrides: &'a NlpCalibrationOverrides,
    ) -> NlpRequest<'a> {
        let req = req.with_schema(schema);
        if overrides.rel_offsets.is_empty() {
            req
        } else {
            req.with_calibration(overrides)
        }
    }
    let requests: Vec<NlpRequest<'_>> = if turns.is_empty() {
        texts
            .iter()
            .map(|t| with_extras(NlpRequest::new(t, needed), &extraction, &overrides))
            .collect()
    } else {
        vec![with_extras(
            NlpRequest::from_turns(&turns, needed),
            &extraction,
            &overrides,
        )]
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

    let names = ResultNames {
        relations: &relation_names,
        entity_types: &entity_type_names,
        alias: &opts.alias,
        model_id: model.model_id(),
    };
    let conversation = !turns.is_empty();
    Ok(match kind {
        ExtractKind::Facts => ExtractRows::Facts(flatten_facts(
            &results,
            conversation.then_some(turn_of_unit.as_slice()),
            &names,
        )),
        ExtractKind::Entities => ExtractRows::Entities(flatten_entities(
            &results,
            conversation.then_some(turn_of_unit.as_slice()),
            &names,
        )),
    })
}

/// How to name results: schema-name maps and model provenance.
struct ResultNames<'a> {
    relations: &'a HashMap<String, String>,
    entity_types: &'a HashMap<String, String>,
    alias: &'a str,
    model_id: &'a str,
}

impl ResultNames<'_> {
    fn relation(&self, name: &str) -> String {
        self.relations
            .get(name)
            .cloned()
            .unwrap_or_else(|| name.to_owned())
    }

    fn entity_type(&self, name: &str) -> String {
        self.entity_types
            .get(name)
            .cloned()
            .unwrap_or_else(|| name.to_owned())
    }
}

fn warn_truncated(results: &[NlpResult]) {
    let truncated = results
        .iter()
        .flat_map(|r| &r.windows)
        .filter(|w| w.analysed_words < w.tokens.len())
        .count();
    if truncated > 0 {
        tracing::warn!(
            windows = truncated,
            "uni.nlp: sentences overflowed the encoder; words past the limit were not analysed"
        );
    }
}

fn sigmoid(x: f64) -> f64 {
    1.0 / (1.0 + (-x).exp())
}

fn flatten_facts(
    results: &[NlpResult],
    turn_of_unit: Option<&[usize]>,
    names: &ResultNames<'_>,
) -> Vec<ExtractedFact> {
    warn_truncated(results);
    let mut rows = Vec::new();
    for (doc, result) in results.iter().enumerate() {
        let locator = DocLocator {
            result,
            turn_of_unit,
        };
        let mut base = 0usize;
        for (w, window) in result.windows.iter().enumerate() {
            for fact in &window.facts {
                let (Some(head), Some(tail)) = (
                    window.entities.get(fact.head),
                    window.entities.get(fact.tail),
                ) else {
                    continue;
                };
                let at = |e: &uni_xervo::traits::NlpEntity| {
                    e.spans
                        .first()
                        .map(|&span| locator.locate(w, base, span))
                        .unwrap_or_default()
                };
                let (h, t) = (at(head), at(tail));
                let margin = f64::from(fact.margin);
                rows.push(ExtractedFact {
                    doc,
                    window: w,
                    head: fact.head,
                    head_text: head.text.clone(),
                    head_type: names.entity_type(&head.ty),
                    head_speaker: head.speaker,
                    head_start: h.start,
                    head_end: h.end,
                    head_turn: h.turn,
                    relation: names.relation(&fact.relation),
                    tail: fact.tail,
                    tail_text: tail.text.clone(),
                    tail_type: names.entity_type(&tail.ty),
                    tail_speaker: tail.speaker,
                    tail_start: t.start,
                    tail_end: t.end,
                    tail_turn: t.turn,
                    polarity: fact.polarity.as_str().to_owned(),
                    modality: fact.modality.as_str().to_owned(),
                    holder: fact.holder,
                    time: fact.time,
                    margin,
                    confidence: sigmoid(margin),
                    model_alias: names.alias.to_owned(),
                    model_id: names.model_id.to_owned(),
                });
            }
            base += window.tokens.len();
        }
    }
    rows
}

fn flatten_entities(
    results: &[NlpResult],
    turn_of_unit: Option<&[usize]>,
    names: &ResultNames<'_>,
) -> Vec<ExtractedEntity> {
    warn_truncated(results);
    let mut rows = Vec::new();
    for (doc, result) in results.iter().enumerate() {
        let locator = DocLocator {
            result,
            turn_of_unit,
        };
        let mut base = 0usize;
        for (w, window) in result.windows.iter().enumerate() {
            for (i, entity) in window.entities.iter().enumerate() {
                let located: Vec<Located> = entity
                    .spans
                    .iter()
                    .map(|&span| locator.locate(w, base, span))
                    .collect();
                rows.push(ExtractedEntity {
                    doc,
                    window: w,
                    entity: i,
                    text: entity.text.clone(),
                    entity_type: names.entity_type(&entity.ty),
                    speaker: entity.speaker,
                    mentions: entity.mentions.clone(),
                    mention_starts: located.iter().map(|l| l.start).collect(),
                    mention_ends: located.iter().map(|l| l.end).collect(),
                    mention_turns: located.iter().map(|l| l.turn).collect(),
                    model_alias: names.alias.to_owned(),
                    model_id: names.model_id.to_owned(),
                });
            }
            base += window.tokens.len();
        }
    }
    rows
}

#[cfg(test)]
mod tests {
    use super::*;
    use uni_xervo::traits::{
        FactModality, FactPolarity, NlpEntity, NlpFact, NlpToken, WindowSentence,
    };

    fn map(pairs: &[(&str, Value)]) -> Value {
        Value::Map(
            pairs
                .iter()
                .map(|(k, v)| ((*k).to_owned(), v.clone()))
                .collect(),
        )
    }

    fn s(v: &str) -> Value {
        Value::String(v.to_owned())
    }

    #[test]
    fn humanize_reads_schema_names_as_words() {
        assert_eq!(humanize("LIVES_IN"), "lives in");
        assert_eq!(humanize("JobTitle"), "job title");
        assert_eq!(humanize("Person"), "person");
        assert_eq!(humanize("works_for"), "works for");
        assert_eq!(humanize("HAS__PART_"), "has part");
    }

    #[test]
    fn input_accepts_text_texts_and_turns() {
        assert_eq!(
            NlpInput::from_value(&s("hi")).unwrap(),
            NlpInput::Text("hi".into())
        );
        assert_eq!(
            NlpInput::from_value(&Value::List(vec![s("a"), s("b")])).unwrap(),
            NlpInput::Texts(vec!["a".into(), "b".into()])
        );
        let turns = Value::List(vec![map(&[("speaker", s("user")), ("text", s("hello"))])]);
        assert_eq!(
            NlpInput::from_value(&turns).unwrap(),
            NlpInput::Turns(vec![("user".into(), "hello".into())])
        );
        assert!(NlpInput::from_value(&Value::Int(3)).is_err());
        let bad = Value::List(vec![map(&[("who", s("user")), ("text", s("x"))])]);
        assert!(
            NlpInput::from_value(&bad)
                .unwrap_err()
                .to_string()
                .contains("unknown key 'who'")
        );
    }

    #[test]
    fn options_parse_and_reject_unknown_keys() {
        let v = map(&[
            ("alias", s("nlp/x")),
            (
                "relations",
                Value::List(vec![
                    s("employer"),
                    map(&[("name", s("lives in")), ("description", s("where"))]),
                ]),
            ),
            ("entity_types", Value::List(vec![s("person")])),
            ("top_per_pair", Value::Int(2)),
            ("rel_offsets", map(&[("employer", Value::Float(-0.5))])),
        ]);
        let o = ExtractOptions::from_value(&v, ExtractKind::Facts).unwrap();
        assert_eq!(o.alias, "nlp/x");
        assert_eq!(o.relations.len(), 2);
        assert_eq!(o.relations[1].description.as_deref(), Some("where"));
        assert_eq!(o.top_per_pair, Some(2));
        assert_eq!(o.rel_offsets["employer"], -0.5);

        let err = ExtractOptions::from_value(
            &map(&[
                ("relations", Value::List(vec![s("r")])),
                ("min_confidence", Value::Float(0.5)),
            ]),
            ExtractKind::Facts,
        )
        .unwrap_err();
        assert!(err.to_string().contains("unknown option 'min_confidence'"));
    }

    #[test]
    fn options_validate_combinations() {
        let no_rel = ExtractOptions::from_value(&Value::Null, ExtractKind::Facts).unwrap_err();
        assert!(no_rel.to_string().contains("give 'relations'"));
        assert!(ExtractOptions::from_value(&Value::Null, ExtractKind::Entities).is_ok());

        let both = map(&[
            ("schema", s("graph")),
            ("relations", Value::List(vec![s("r")])),
        ]);
        assert!(
            ExtractOptions::from_value(&both, ExtractKind::Facts)
                .unwrap_err()
                .to_string()
                .contains("cannot be combined")
        );
        let ent_rel = map(&[("relations", Value::List(vec![s("r")]))]);
        assert!(
            ExtractOptions::from_value(&ent_rel, ExtractKind::Entities)
                .unwrap_err()
                .to_string()
                .contains("applies only to uni.nlp.extract")
        );
        let dup = map(&[("relations", Value::List(vec![s("r"), s("r")]))]);
        assert!(
            ExtractOptions::from_value(&dup, ExtractKind::Facts)
                .unwrap_err()
                .to_string()
                .contains("listed twice")
        );
        let subset = map(&[(
            "schema",
            map(&[("edge_types", Value::List(vec![s("LIVES_IN")]))]),
        )]);
        let o = ExtractOptions::from_value(&subset, ExtractKind::Facts).unwrap();
        assert_eq!(
            o.schema,
            Some(SchemaSource::Subset {
                labels: None,
                edge_types: Some(vec!["LIVES_IN".into()])
            })
        );
    }

    fn token(text: &str, start: usize) -> NlpToken {
        let mut t = NlpToken::default();
        t.text = text.to_owned();
        t.start = start;
        t.end = start + text.len();
        t
    }

    fn entity(ty: &str, text: &str, spans: Vec<(usize, usize)>, speaker: bool) -> NlpEntity {
        let mut e = NlpEntity::default();
        e.ty = ty.to_owned();
        e.text = text.to_owned();
        e.speaker = speaker;
        e.mentions = spans.iter().map(|_| text.to_owned()).collect();
        e.spans = spans;
        e
    }

    fn sentence(start: usize, end: usize, unit: usize) -> WindowSentence {
        let mut s = WindowSentence::default();
        s.start = start;
        s.end = end;
        s.unit = unit;
        s
    }

    /// Two windows of prose: "Ada lives in London" | "She joined IBM".
    fn two_window_prose() -> NlpResult {
        let words1 = [("Ada", 0), ("lives", 4), ("in", 10), ("London", 13)];
        let words2 = [("She", 21), ("joined", 25), ("IBM", 32)];
        let mut r = NlpResult::default();
        for (w, o) in words1.iter().chain(words2.iter()) {
            r.tokens.push(token(w, *o));
        }
        let mut w1 = NlpWindow::default();
        w1.tokens = words1.iter().map(|(w, _)| (*w).to_owned()).collect();
        w1.analysed_words = 4;
        w1.sentences = vec![sentence(0, 4, 0)];
        w1.entities = vec![
            entity("person", "Ada", vec![(0, 1)], false),
            entity("location", "London", vec![(3, 4)], false),
        ];
        let mut f1 = NlpFact::default();
        f1.head = 0;
        f1.tail = 1;
        f1.relation = "lives in".into();
        f1.margin = 0.0;
        f1.holder = Some(0);
        w1.facts = vec![f1];
        let mut w2 = NlpWindow::default();
        w2.tokens = words2.iter().map(|(w, _)| (*w).to_owned()).collect();
        w2.analysed_words = 3;
        w2.sentences = vec![sentence(0, 3, 1)];
        w2.entities = vec![
            entity("person", "She", vec![(0, 1)], false),
            entity("organization", "IBM", vec![(2, 3)], false),
        ];
        let mut f2 = NlpFact::default();
        f2.head = 0;
        f2.tail = 1;
        f2.relation = "employer".into();
        f2.margin = 2.0;
        f2.polarity = FactPolarity::Negative;
        f2.modality = FactModality::Planned;
        w2.facts = vec![f2];
        r.windows = vec![w1, w2];
        r
    }

    fn names<'a>(
        rel: &'a HashMap<String, String>,
        ty: &'a HashMap<String, String>,
    ) -> ResultNames<'a> {
        ResultNames {
            relations: rel,
            entity_types: ty,
            alias: "nlp/test",
            model_id: "test/model",
        }
    }

    #[test]
    fn facts_carry_byte_spans_across_windows() {
        let empty = HashMap::new();
        let rows = flatten_facts(&[two_window_prose()], None, &names(&empty, &empty));
        assert_eq!(rows.len(), 2);
        let a = &rows[0];
        assert_eq!(
            (a.window, a.head_text.as_str(), a.relation.as_str()),
            (0, "Ada", "lives in")
        );
        assert_eq!((a.head_start, a.head_end), (Some(0), Some(3)));
        assert_eq!((a.tail_start, a.tail_end), (Some(13), Some(19)));
        assert_eq!(a.head_turn, None);
        assert_eq!(a.holder, Some(0));
        assert!((a.confidence - 0.5).abs() < 1e-9);
        assert_eq!(
            (a.polarity.as_str(), a.modality.as_str()),
            ("positive", "asserted")
        );

        // The second window's word indexes are offset by the first window.
        let b = &rows[1];
        assert_eq!(b.window, 1);
        assert_eq!((b.tail_start, b.tail_end), (Some(32), Some(35)));
        assert_eq!(
            (b.polarity.as_str(), b.modality.as_str()),
            ("negative", "planned")
        );
        assert!(b.confidence > 0.88 && b.confidence < 0.89);
        assert_eq!(
            (b.model_alias.as_str(), b.model_id.as_str()),
            ("nlp/test", "test/model")
        );
    }

    #[test]
    fn graph_names_map_back_to_schema_names() {
        let rel = HashMap::from([("lives in".to_owned(), "LIVES_IN".to_owned())]);
        let ty = HashMap::from([("person".to_owned(), "Person".to_owned())]);
        let rows = flatten_facts(&[two_window_prose()], None, &names(&rel, &ty));
        assert_eq!(rows[0].relation, "LIVES_IN");
        assert_eq!(rows[0].head_type, "Person");
        // Names outside the maps pass through.
        assert_eq!(rows[1].relation, "employer");
        assert_eq!(rows[1].tail_type, "organization");
    }

    #[test]
    fn conversation_rows_name_their_turn_and_speakers_have_no_span() {
        // Turns: 0 "user: I live in Lisbon", 1 blank (dropped by the model),
        // 2 "bob: ok". Word offsets are turn-relative.
        let mut r = NlpResult::default();
        for (w, o) in [("I", 0), ("live", 2), ("in", 7), ("Lisbon", 10), ("ok", 0)] {
            r.tokens.push(token(w, o));
        }
        let mut w = NlpWindow::default();
        w.tokens = ["I", "live", "in", "Lisbon", "ok"]
            .iter()
            .map(|s| (*s).to_owned())
            .collect();
        w.analysed_words = 5;
        // Unit 1 is the second non-blank turn, i.e. original turn 2.
        w.sentences = vec![sentence(0, 4, 0), sentence(4, 5, 1)];
        w.entities = vec![
            entity("speaker", "user", vec![], true),
            entity("location", "Lisbon", vec![(3, 4)], false),
            entity("other", "ok", vec![(4, 5)], false),
        ];
        let mut f = NlpFact::default();
        f.head = 0;
        f.tail = 1;
        f.relation = "lives in".into();
        f.margin = 1.0;
        w.facts = vec![f];
        r.windows = vec![w];

        let empty = HashMap::new();
        let map = [0usize, 2];
        let facts = flatten_facts(std::slice::from_ref(&r), Some(&map), &names(&empty, &empty));
        let fact = &facts[0];
        assert!(fact.head_speaker);
        assert_eq!(
            (fact.head_start, fact.head_end, fact.head_turn),
            (None, None, None)
        );
        assert_eq!(
            (fact.tail_start, fact.tail_end, fact.tail_turn),
            (Some(10), Some(16), Some(0))
        );

        let ents = flatten_entities(&[r], Some(&map), &names(&empty, &empty));
        assert_eq!(ents.len(), 3);
        assert!(ents[0].speaker && ents[0].mention_starts.is_empty());
        assert_eq!(ents[2].mention_turns, vec![Some(2)]);
        assert_eq!(
            (ents[2].mention_starts.clone(), ents[2].mention_ends.clone()),
            (vec![Some(0)], vec![Some(2)])
        );
    }

    fn schema_with(edges: &[(&str, Option<&str>)], labels: &[(&str, Option<&str>)]) -> Schema {
        use uni_common::core::schema::{EdgeTypeMeta, LabelMeta};
        let mut schema = Schema::default();
        for (i, (name, desc)) in labels.iter().enumerate() {
            schema.labels.insert(
                (*name).to_owned(),
                LabelMeta {
                    id: i as u16,
                    created_at: chrono::Utc::now(),
                    state: SchemaElementState::Active,
                    description: desc.map(str::to_owned),
                },
            );
        }
        for (i, (name, desc)) in edges.iter().enumerate() {
            schema.edge_types.insert(
                (*name).to_owned(),
                EdgeTypeMeta {
                    id: i as u32,
                    src_labels: Vec::new(),
                    dst_labels: Vec::new(),
                    state: SchemaElementState::Active,
                    description: desc.map(str::to_owned),
                    endpoint_conformance: Default::default(),
                },
            );
        }
        schema
    }

    #[test]
    fn graph_schema_derives_humanized_specs_and_subsets() {
        let schema = schema_with(
            &[
                ("LIVES_IN", Some("the place where the head resides")),
                ("WORKS_FOR", None),
            ],
            &[("Person", None), ("City", Some("a city"))],
        );
        let all = graph_schema_specs(&schema, &SchemaSource::Graph).unwrap();
        let rel_names: Vec<&str> = all.relations.iter().map(|r| r.name.as_str()).collect();
        assert_eq!(rel_names, vec!["lives in", "works for"]);
        assert_eq!(
            all.relations[0].description.as_deref(),
            Some("the place where the head resides")
        );
        assert_eq!(all.undescribed_edge_types, vec!["WORKS_FOR".to_owned()]);
        assert_eq!(all.relation_names["works for"], "WORKS_FOR");
        assert_eq!(all.entity_type_names["city"], "City");

        let sub = graph_schema_specs(
            &schema,
            &SchemaSource::Subset {
                labels: Some(vec!["Person".into()]),
                edge_types: Some(vec!["LIVES_IN".into()]),
            },
        )
        .unwrap();
        assert_eq!(sub.relations.len(), 1);
        assert_eq!(sub.entity_types.len(), 1);

        let missing = graph_schema_specs(
            &schema,
            &SchemaSource::Subset {
                labels: None,
                edge_types: Some(vec!["NOPE".into()]),
            },
        )
        .unwrap_err();
        assert!(
            missing
                .to_string()
                .contains("'NOPE' is not an active declared edge type")
        );

        let clash = schema_with(&[("LIVES_IN", None), ("LivesIn", None)], &[]);
        assert!(
            graph_schema_specs(&clash, &SchemaSource::Graph)
                .unwrap_err()
                .to_string()
                .contains("both read as 'lives in'")
        );
    }
}
