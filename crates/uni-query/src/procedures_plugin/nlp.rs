// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! `uni.nlp.extract` and `uni.nlp.entities` — entity and fact extraction
//! with a kniv v7 model from the Uni-Xervo runtime.
//!
//! Both procedures are thin adapters over [`crate::nlp::run_extract`]: they
//! decode the `input` and `options` arguments, run the extraction against
//! the session's schema (a forked session's merged schema included), and
//! emit one row per fact or per entity.

// Rust guideline compliant

use std::collections::HashMap;
use std::sync::{Arc, OnceLock};

use arrow_array::RecordBatch;
use arrow_schema::{DataType, Field, SchemaRef};
use datafusion::error::{DataFusionError, Result as DFResult};
use datafusion::execution::SendableRecordBatchStream;
use datafusion::logical_expr::ColumnarValue;
use datafusion::scalar::ScalarValue;
use uni_common::Value;
use uni_plugin::traits::procedure::{
    ProcedureContext, ProcedureMode, ProcedurePlugin, ProcedureSignature,
};
use uni_plugin::traits::scalar::ArgType;
use uni_plugin::{FnError, PluginError, PluginRegistrar, QName, SideEffects};

use crate::nlp::{
    ExtractKind, ExtractOptions, ExtractRows, ExtractedEntity, ExtractedFact, NlpExtractError,
    NlpInput, run_extract,
};
use crate::procedures_plugin::host_args::{arg, arg_with_default};
use crate::procedures_plugin::vector::run_search_procedure;
use crate::query::df_graph::procedure_call::build_typed_column;
use crate::query::executor::procedure_host::QueryProcedureHost;

/// A dynamic-value (list) yield column, as list yields are elsewhere.
fn list_field(name: &str) -> Field {
    Field::new(name.to_owned(), DataType::LargeBinary, true).with_metadata(HashMap::from([(
        "cv_encoded".to_owned(),
        "true".to_owned(),
    )]))
}

fn int(name: &str) -> Field {
    Field::new(name.to_owned(), DataType::Int64, true)
}

fn text(name: &str) -> Field {
    Field::new(name.to_owned(), DataType::Utf8, true)
}

fn flag(name: &str) -> Field {
    Field::new(name.to_owned(), DataType::Boolean, true)
}

fn float(name: &str) -> Field {
    Field::new(name.to_owned(), DataType::Float64, true)
}

fn args() -> Vec<uni_plugin::traits::procedure::NamedArgType> {
    vec![
        arg(
            "input",
            ArgType::CypherValue,
            "Text to analyse: a string, a list of strings (one document each), \
             or a list of {speaker, text} maps (one conversation).",
        ),
        arg_with_default(
            "options",
            ArgType::CypherValue,
            ScalarValue::Null,
            "Options map (optional): `alias`, `relations`, `entity_types`, `schema` \
             ('graph' or {labels, edge_types}), `top_per_pair`, `rel_offsets`.",
        ),
    ]
}

fn extract_signature() -> &'static ProcedureSignature {
    static SIG: OnceLock<ProcedureSignature> = OnceLock::new();
    SIG.get_or_init(|| ProcedureSignature {
        args: args(),
        yields: vec![
            int("doc"),
            int("window"),
            int("head"),
            text("head_text"),
            text("head_type"),
            flag("head_speaker"),
            int("head_start"),
            int("head_end"),
            int("head_turn"),
            text("relation"),
            int("tail"),
            text("tail_text"),
            text("tail_type"),
            flag("tail_speaker"),
            int("tail_start"),
            int("tail_end"),
            int("tail_turn"),
            text("polarity"),
            text("modality"),
            int("holder"),
            int("time"),
            float("margin"),
            float("confidence"),
            text("model_alias"),
            text("model_id"),
        ],
        mode: ProcedureMode::Read,
        side_effects: SideEffects::ReadOnly,
        retry_contract: None,
        batch_input: None,
        docs: "Extract facts (head --relation--> tail, with polarity, modality, holder, \
               time and margin) from text with a kniv v7 model."
            .to_owned(),
    })
}

fn entities_signature() -> &'static ProcedureSignature {
    static SIG: OnceLock<ProcedureSignature> = OnceLock::new();
    SIG.get_or_init(|| ProcedureSignature {
        args: args(),
        yields: vec![
            int("doc"),
            int("window"),
            int("entity"),
            text("text"),
            text("entity_type"),
            flag("speaker"),
            list_field("mentions"),
            list_field("mention_starts"),
            list_field("mention_ends"),
            list_field("mention_turns"),
            text("model_alias"),
            text("model_id"),
        ],
        mode: ProcedureMode::Read,
        side_effects: SideEffects::ReadOnly,
        retry_contract: None,
        batch_input: None,
        docs: "Extract typed entities, grouped across their mentions, from text with a \
               kniv v7 model."
            .to_owned(),
    })
}

fn opt_int(v: Option<usize>) -> Value {
    v.map_or(Value::Null, |n| Value::Int(n as i64))
}

fn int_list(vs: &[Option<usize>]) -> Value {
    Value::List(vs.iter().map(|v| opt_int(*v)).collect())
}

fn fact_value(row: &ExtractedFact, column: &str) -> Value {
    match column {
        "doc" => Value::Int(row.doc as i64),
        "window" => Value::Int(row.window as i64),
        "head" => Value::Int(row.head as i64),
        "head_text" => Value::String(row.head_text.clone()),
        "head_type" => Value::String(row.head_type.clone()),
        "head_speaker" => Value::Bool(row.head_speaker),
        "head_start" => opt_int(row.head_start),
        "head_end" => opt_int(row.head_end),
        "head_turn" => opt_int(row.head_turn),
        "relation" => Value::String(row.relation.clone()),
        "tail" => Value::Int(row.tail as i64),
        "tail_text" => Value::String(row.tail_text.clone()),
        "tail_type" => Value::String(row.tail_type.clone()),
        "tail_speaker" => Value::Bool(row.tail_speaker),
        "tail_start" => opt_int(row.tail_start),
        "tail_end" => opt_int(row.tail_end),
        "tail_turn" => opt_int(row.tail_turn),
        "polarity" => Value::String(row.polarity.clone()),
        "modality" => Value::String(row.modality.clone()),
        "holder" => opt_int(row.holder),
        "time" => opt_int(row.time),
        "margin" => Value::Float(row.margin),
        "confidence" => Value::Float(row.confidence),
        "model_alias" => Value::String(row.model_alias.clone()),
        "model_id" => Value::String(row.model_id.clone()),
        _ => Value::Null,
    }
}

fn entity_value(row: &ExtractedEntity, column: &str) -> Value {
    match column {
        "doc" => Value::Int(row.doc as i64),
        "window" => Value::Int(row.window as i64),
        "entity" => Value::Int(row.entity as i64),
        "text" => Value::String(row.text.clone()),
        "entity_type" => Value::String(row.entity_type.clone()),
        "speaker" => Value::Bool(row.speaker),
        "mentions" => Value::List(
            row.mentions
                .iter()
                .map(|m| Value::String(m.clone()))
                .collect(),
        ),
        "mention_starts" => int_list(&row.mention_starts),
        "mention_ends" => int_list(&row.mention_ends),
        "mention_turns" => int_list(&row.mention_turns),
        "model_alias" => Value::String(row.model_alias.clone()),
        "model_id" => Value::String(row.model_id.clone()),
        _ => Value::Null,
    }
}

/// Build the result batch: one column per requested yield, in yield order,
/// typed by the output schema.
fn build_batch<R>(
    rows: &[R],
    yield_items: &[(String, Option<String>)],
    schema: &SchemaRef,
    value: impl Fn(&R, &str) -> Value,
) -> DFResult<RecordBatch> {
    let mut columns = Vec::with_capacity(yield_items.len());
    for (i, (name, _alias)) in yield_items.iter().enumerate() {
        let values: Vec<Value> = rows.iter().map(|r| value(r, name)).collect();
        let data_type = schema
            .fields()
            .get(i)
            .map_or(DataType::Utf8, |f| f.data_type().clone());
        columns.push(build_typed_column(
            values.iter().map(|v| (!v.is_null()).then_some(v)),
            rows.len(),
            &data_type,
        ));
    }
    if columns.is_empty() {
        return RecordBatch::try_new_with_options(
            schema.clone(),
            columns,
            &arrow_array::RecordBatchOptions::new().with_row_count(Some(rows.len())),
        )
        .map_err(|e| DataFusionError::ArrowError(Box::new(e), None));
    }
    RecordBatch::try_new(schema.clone(), columns)
        .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))
}

fn to_df_error(proc_name: &str, err: NlpExtractError) -> DataFusionError {
    DataFusionError::Execution(format!("{proc_name}: {err}"))
}

async fn run(
    proc_name: &'static str,
    kind: ExtractKind,
    host: QueryProcedureHost,
    uni_args: Vec<Value>,
    yield_items: Vec<(String, Option<String>)>,
    schema: SchemaRef,
) -> DFResult<Option<RecordBatch>> {
    let input_value = uni_args.first().cloned().unwrap_or(Value::Null);
    let options_value = uni_args.get(1).cloned().unwrap_or(Value::Null);
    let input = NlpInput::from_value(&input_value).map_err(|e| to_df_error(proc_name, e))?;
    let opts =
        ExtractOptions::from_value(&options_value, kind).map_err(|e| to_df_error(proc_name, e))?;
    let runtime = host
        .xervo_runtime()
        .cloned()
        .ok_or_else(|| to_df_error(proc_name, NlpExtractError::NotConfigured))?;
    let graph_schema = host.storage().schema_manager().schema();
    let rows = run_extract(&runtime, &graph_schema, &input, &opts, kind)
        .await
        .map_err(|e| to_df_error(proc_name, e))?;
    let batch = match rows {
        ExtractRows::Facts(rows) => build_batch(&rows, &yield_items, &schema, fact_value)?,
        ExtractRows::Entities(rows) => build_batch(&rows, &yield_items, &schema, entity_value)?,
    };
    Ok(Some(batch))
}

#[derive(Debug)]
struct NlpExtractProc;

impl ProcedurePlugin for NlpExtractProc {
    fn signature(&self) -> &ProcedureSignature {
        extract_signature()
    }

    fn invoke(
        &self,
        ctx: ProcedureContext<'_>,
        args: &[ColumnarValue],
    ) -> Result<SendableRecordBatchStream, FnError> {
        run_search_procedure(
            "uni.nlp.extract",
            &ctx,
            args,
            extract_signature(),
            |host, uni_args, yield_items, schema| {
                run(
                    "uni.nlp.extract",
                    ExtractKind::Facts,
                    host,
                    uni_args,
                    yield_items,
                    schema,
                )
            },
        )
    }
}

#[derive(Debug)]
struct NlpEntitiesProc;

impl ProcedurePlugin for NlpEntitiesProc {
    fn signature(&self) -> &ProcedureSignature {
        entities_signature()
    }

    fn invoke(
        &self,
        ctx: ProcedureContext<'_>,
        args: &[ColumnarValue],
    ) -> Result<SendableRecordBatchStream, FnError> {
        run_search_procedure(
            "uni.nlp.entities",
            &ctx,
            args,
            entities_signature(),
            |host, uni_args, yield_items, schema| {
                run(
                    "uni.nlp.entities",
                    ExtractKind::Entities,
                    host,
                    uni_args,
                    yield_items,
                    schema,
                )
            },
        )
    }
}

/// Register `uni.nlp.extract` and `uni.nlp.entities` into `r`.
///
/// # Errors
///
/// Returns [`PluginError::DuplicateRegistration`] if a qname is taken.
pub fn register_into(r: &mut PluginRegistrar<'_>) -> Result<(), PluginError> {
    r.procedure(
        QName::new("uni", "nlp.extract"),
        extract_signature().clone(),
        Arc::new(NlpExtractProc),
    )?;
    r.procedure(
        QName::new("uni", "nlp.entities"),
        entities_signature().clone(),
        Arc::new(NlpEntitiesProc),
    )?;
    Ok(())
}
