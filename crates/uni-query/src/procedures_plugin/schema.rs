// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! `uni.schema.*` read-only introspection procedures.
//!
//! Direct ports of `execute_schema_labels` / `execute_schema_edge_types`
//! / `execute_schema_indexes` / `execute_schema_constraints` /
//! `execute_schema_label_info` from `procedure_call.rs`. Each procedure
//! emits a `RecordBatch` containing every natively-produced column; the
//! plugin-path dispatcher in `execute_plugin_procedure` projects the
//! caller's `YIELD` subset.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::OnceLock;

use arrow_array::{ArrayRef, RecordBatch};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use datafusion::execution::SendableRecordBatchStream;
use datafusion::logical_expr::ColumnarValue;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use futures::stream;
use uni_common::Value;
use uni_plugin::traits::procedure::{
    ProcedureContext, ProcedureMode, ProcedurePlugin, ProcedureSignature,
};
use uni_plugin::traits::scalar::ArgType;
use uni_plugin::{FnError, PluginError, PluginRegistrar, QName, SideEffects};

use crate::procedures_plugin::host_args::arg;
use crate::query::df_graph::common::exec_err;
use crate::query::df_graph::procedure_call::build_typed_column;
use crate::query::executor::procedure_host::QueryProcedureHost;

// Rust guideline compliant

// ---------------------------------------------------------------------------
// Shared helpers
// ---------------------------------------------------------------------------

fn require_host<'a>(ctx: &'a ProcedureContext<'_>) -> Result<&'a QueryProcedureHost, FnError> {
    ctx.host
        .and_then(|h| h.as_any().downcast_ref::<QueryProcedureHost>())
        .ok_or_else(|| {
            FnError::new(
                0x701,
                "uni.schema.*: requires QueryProcedureHost (host not bound on ProcedureContext)",
            )
        })
}

fn require_string_arg(args: &[ColumnarValue], index: usize, name: &str) -> Result<String, FnError> {
    use datafusion::scalar::ScalarValue;
    match args.get(index) {
        Some(ColumnarValue::Scalar(ScalarValue::Utf8(Some(s)))) => Ok(s.clone()),
        Some(ColumnarValue::Scalar(ScalarValue::LargeUtf8(Some(s)))) => Ok(s.clone()),
        _ => Err(FnError::new(
            FnError::CODE_TYPE_COERCION,
            format!("uni.schema.*: {name} (arg #{index}) must be a non-null string"),
        )),
    }
}

fn rows_to_batch(
    rows: Vec<HashMap<String, Value>>,
    schema: SchemaRef,
) -> Result<RecordBatch, FnError> {
    if rows.is_empty() {
        return Ok(RecordBatch::new_empty(schema));
    }
    let num_rows = rows.len();
    let mut columns: Vec<ArrayRef> = Vec::with_capacity(schema.fields().len());
    for field in schema.fields() {
        let name = field.name();
        let values_iter = rows.iter().map(|row| row.get(name));
        columns.push(build_typed_column(values_iter, num_rows, field.data_type()));
    }
    RecordBatch::try_new(schema, columns)
        .map_err(|e| FnError::new(0x600, format!("uni.schema.*: build batch: {e}")))
}

fn single_batch_stream(schema: SchemaRef, batch: RecordBatch) -> SendableRecordBatchStream {
    Box::pin(RecordBatchStreamAdapter::new(
        schema,
        stream::iter(vec![Ok(batch)]),
    ))
}

// ---------------------------------------------------------------------------
// uni.schema.labels
// ---------------------------------------------------------------------------

#[derive(Debug)]
struct SchemaLabelsProc;

fn schema_labels_signature() -> &'static ProcedureSignature {
    static SIG: OnceLock<ProcedureSignature> = OnceLock::new();
    SIG.get_or_init(|| ProcedureSignature {
        args: vec![],
        yields: vec![
            Field::new("label", DataType::Utf8, true),
            Field::new("propertyCount", DataType::Int64, true),
            Field::new("nodeCount", DataType::Int64, true),
            Field::new("indexCount", DataType::Int64, true),
        ],
        mode: ProcedureMode::Read,
        side_effects: SideEffects::ReadOnly,
        retry_contract: None,
        batch_input: None,
        docs: "List every label with property / node / index counts.".to_owned(),
    })
}

impl ProcedurePlugin for SchemaLabelsProc {
    fn signature(&self) -> &ProcedureSignature {
        schema_labels_signature()
    }

    fn invoke(
        &self,
        ctx: ProcedureContext<'_>,
        _args: &[ColumnarValue],
    ) -> Result<SendableRecordBatchStream, FnError> {
        let host = require_host(&ctx)?;
        let storage = Arc::clone(host.storage());
        let stream = futures::stream::once(async move {
            let uni_schema = storage.schema_manager().schema();
            let mut rows: Vec<HashMap<String, Value>> = Vec::new();
            for label_name in uni_schema.labels.keys() {
                let prop_count = uni_schema
                    .properties
                    .get(label_name)
                    .map(|p| p.len() as i64)
                    .unwrap_or(0);
                // Node count via the `StorageBackend` (correct `.lance` path);
                // the prior raw-dataset read reported 0 for flushed tables.
                let backend = storage.backend();
                let table = uni_store::backend::table_names::vertex_table_name(label_name);
                let node_count = if backend.table_exists(&table).await.unwrap_or(false) {
                    backend.count_rows(&table, None).await.unwrap_or(0) as i64
                } else {
                    0
                };
                let idx_count = uni_schema
                    .indexes
                    .iter()
                    .filter(|i| i.label() == label_name)
                    .count() as i64;
                rows.push(HashMap::from([
                    ("label".to_owned(), Value::String(label_name.clone())),
                    ("propertyCount".to_owned(), Value::Int(prop_count)),
                    ("nodeCount".to_owned(), Value::Int(node_count)),
                    ("indexCount".to_owned(), Value::Int(idx_count)),
                ]));
            }
            let schema = Arc::new(Schema::new(schema_labels_signature().yields.clone()));
            rows_to_batch(rows, schema).map_err(exec_err)
        });
        let out_schema = Arc::new(Schema::new(schema_labels_signature().yields.clone()));
        Ok(Box::pin(RecordBatchStreamAdapter::new(out_schema, stream)))
    }
}

// ---------------------------------------------------------------------------
// uni.schema.edgeTypes / uni.schema.relationshipTypes
// ---------------------------------------------------------------------------

#[derive(Debug)]
struct SchemaEdgeTypesProc;

fn schema_edge_types_signature() -> &'static ProcedureSignature {
    static SIG: OnceLock<ProcedureSignature> = OnceLock::new();
    SIG.get_or_init(|| ProcedureSignature {
        args: vec![],
        yields: vec![
            Field::new("type", DataType::Utf8, true),
            Field::new("relationshipType", DataType::Utf8, true),
            Field::new("sourceLabels", DataType::Utf8, true),
            Field::new("targetLabels", DataType::Utf8, true),
            Field::new("propertyCount", DataType::Int64, true),
        ],
        mode: ProcedureMode::Read,
        side_effects: SideEffects::ReadOnly,
        retry_contract: None,
        batch_input: None,
        docs: "List every edge type with source / target labels and property count.".to_owned(),
    })
}

impl ProcedurePlugin for SchemaEdgeTypesProc {
    fn signature(&self) -> &ProcedureSignature {
        schema_edge_types_signature()
    }

    fn invoke(
        &self,
        ctx: ProcedureContext<'_>,
        _args: &[ColumnarValue],
    ) -> Result<SendableRecordBatchStream, FnError> {
        let host = require_host(&ctx)?;
        let uni_schema = host.storage().schema_manager().schema();
        let mut rows: Vec<HashMap<String, Value>> = Vec::new();
        for (type_name, meta) in &uni_schema.edge_types {
            let prop_count = uni_schema
                .properties
                .get(type_name)
                .map(|p| p.len() as i64)
                .unwrap_or(0);
            rows.push(HashMap::from([
                ("type".to_owned(), Value::String(type_name.clone())),
                (
                    "relationshipType".to_owned(),
                    Value::String(type_name.clone()),
                ),
                (
                    "sourceLabels".to_owned(),
                    Value::String(format!("{:?}", meta.src_labels)),
                ),
                (
                    "targetLabels".to_owned(),
                    Value::String(format!("{:?}", meta.dst_labels)),
                ),
                ("propertyCount".to_owned(), Value::Int(prop_count)),
            ]));
        }
        let schema = Arc::new(Schema::new(schema_edge_types_signature().yields.clone()));
        let batch = rows_to_batch(rows, schema.clone())?;
        Ok(single_batch_stream(schema, batch))
    }
}

// ---------------------------------------------------------------------------
// uni.schema.indexes
// ---------------------------------------------------------------------------

#[derive(Debug)]
struct SchemaIndexesProc;

fn schema_indexes_signature() -> &'static ProcedureSignature {
    static SIG: OnceLock<ProcedureSignature> = OnceLock::new();
    SIG.get_or_init(|| ProcedureSignature {
        args: vec![],
        yields: vec![
            Field::new("state", DataType::Utf8, true),
            Field::new("name", DataType::Utf8, true),
            Field::new("type", DataType::Utf8, true),
            Field::new("label", DataType::Utf8, true),
            Field::new("properties", DataType::Utf8, true),
        ],
        mode: ProcedureMode::Read,
        side_effects: SideEffects::ReadOnly,
        retry_contract: None,
        batch_input: None,
        docs: "List every index (Vector / FullText / Scalar / JsonFullText / Inverted).".to_owned(),
    })
}

impl ProcedurePlugin for SchemaIndexesProc {
    fn signature(&self) -> &ProcedureSignature {
        schema_indexes_signature()
    }

    fn invoke(
        &self,
        ctx: ProcedureContext<'_>,
        _args: &[ColumnarValue],
    ) -> Result<SendableRecordBatchStream, FnError> {
        use uni_common::core::schema::IndexDefinition;

        let host = require_host(&ctx)?;
        let uni_schema = host.storage().schema_manager().schema();
        let mut rows: Vec<HashMap<String, Value>> = Vec::new();
        for idx in &uni_schema.indexes {
            let (type_name, properties_json) = match idx {
                IndexDefinition::Vector(v) => (
                    "VECTOR",
                    serde_json::to_string(&[&v.property]).unwrap_or_default(),
                ),
                IndexDefinition::FullText(f) => (
                    "FULLTEXT",
                    serde_json::to_string(&f.properties).unwrap_or_default(),
                ),
                IndexDefinition::Scalar(s) => (
                    "SCALAR",
                    serde_json::to_string(&s.properties).unwrap_or_default(),
                ),
                IndexDefinition::JsonFullText(j) => (
                    "JSON_FTS",
                    serde_json::to_string(&[&j.column]).unwrap_or_default(),
                ),
                IndexDefinition::Inverted(inv) => (
                    "INVERTED",
                    serde_json::to_string(&[&inv.property]).unwrap_or_default(),
                ),
                _ => ("UNKNOWN", String::new()),
            };
            rows.push(HashMap::from([
                (
                    "state".to_owned(),
                    Value::String(idx.metadata().status.as_str().to_owned()),
                ),
                ("name".to_owned(), Value::String(idx.name().to_owned())),
                ("type".to_owned(), Value::String(type_name.to_owned())),
                ("label".to_owned(), Value::String(idx.label().to_owned())),
                ("properties".to_owned(), Value::String(properties_json)),
            ]));
        }
        let schema = Arc::new(Schema::new(schema_indexes_signature().yields.clone()));
        let batch = rows_to_batch(rows, schema.clone())?;
        Ok(single_batch_stream(schema, batch))
    }
}

// ---------------------------------------------------------------------------
// uni.schema.constraints
// ---------------------------------------------------------------------------

#[derive(Debug)]
struct SchemaConstraintsProc;

fn schema_constraints_signature() -> &'static ProcedureSignature {
    static SIG: OnceLock<ProcedureSignature> = OnceLock::new();
    SIG.get_or_init(|| ProcedureSignature {
        args: vec![],
        yields: vec![
            Field::new("name", DataType::Utf8, true),
            Field::new("enabled", DataType::Boolean, true),
            Field::new("type", DataType::Utf8, true),
            Field::new("properties", DataType::Utf8, true),
            Field::new("expression", DataType::Utf8, true),
            Field::new("label", DataType::Utf8, true),
            Field::new("relationshipType", DataType::Utf8, true),
            Field::new("target", DataType::Utf8, true),
        ],
        mode: ProcedureMode::Read,
        side_effects: SideEffects::ReadOnly,
        retry_contract: None,
        batch_input: None,
        docs: "List every constraint (Unique / Exists / Check) per label or edge type.".to_owned(),
    })
}

impl ProcedurePlugin for SchemaConstraintsProc {
    fn signature(&self) -> &ProcedureSignature {
        schema_constraints_signature()
    }

    fn invoke(
        &self,
        ctx: ProcedureContext<'_>,
        _args: &[ColumnarValue],
    ) -> Result<SendableRecordBatchStream, FnError> {
        use uni_common::core::schema::{ConstraintTarget, ConstraintType};

        let host = require_host(&ctx)?;
        let uni_schema = host.storage().schema_manager().schema();
        let mut rows: Vec<HashMap<String, Value>> = Vec::new();
        for c in &uni_schema.constraints {
            let mut row: HashMap<String, Value> = HashMap::new();
            row.insert("name".to_owned(), Value::String(c.name.clone()));
            row.insert("enabled".to_owned(), Value::Bool(c.enabled));
            match &c.constraint_type {
                ConstraintType::Unique { properties } => {
                    row.insert("type".to_owned(), Value::String("UNIQUE".to_owned()));
                    row.insert(
                        "properties".to_owned(),
                        Value::String(serde_json::to_string(&properties).unwrap_or_default()),
                    );
                }
                ConstraintType::Exists { property } => {
                    row.insert("type".to_owned(), Value::String("EXISTS".to_owned()));
                    row.insert(
                        "properties".to_owned(),
                        Value::String(serde_json::to_string(&[&property]).unwrap_or_default()),
                    );
                }
                ConstraintType::Check { expression } => {
                    row.insert("type".to_owned(), Value::String("CHECK".to_owned()));
                    row.insert("expression".to_owned(), Value::String(expression.clone()));
                }
                ConstraintType::NodeKey { properties } => {
                    row.insert("type".to_owned(), Value::String("NODE KEY".to_owned()));
                    row.insert(
                        "properties".to_owned(),
                        Value::String(serde_json::to_string(&properties).unwrap_or_default()),
                    );
                }
                _ => {
                    row.insert("type".to_owned(), Value::String("UNKNOWN".to_owned()));
                }
            }
            match &c.target {
                ConstraintTarget::Label(l) => {
                    row.insert("label".to_owned(), Value::String(l.clone()));
                }
                ConstraintTarget::EdgeType(t) => {
                    row.insert("relationshipType".to_owned(), Value::String(t.clone()));
                }
                _ => {
                    row.insert("target".to_owned(), Value::String("UNKNOWN".to_owned()));
                }
            }
            rows.push(row);
        }
        let schema = Arc::new(Schema::new(schema_constraints_signature().yields.clone()));
        let batch = rows_to_batch(rows, schema.clone())?;
        Ok(single_batch_stream(schema, batch))
    }
}

// ---------------------------------------------------------------------------
// uni.schema.labelInfo
// ---------------------------------------------------------------------------

#[derive(Debug)]
struct SchemaLabelInfoProc;

fn schema_label_info_signature() -> &'static ProcedureSignature {
    static SIG: OnceLock<ProcedureSignature> = OnceLock::new();
    SIG.get_or_init(|| ProcedureSignature {
        args: vec![arg(
            "label",
            ArgType::Primitive(DataType::Utf8),
            "Label name to introspect.",
        )],
        yields: vec![
            Field::new("property", DataType::Utf8, true),
            Field::new("dataType", DataType::Utf8, true),
            Field::new("nullable", DataType::Boolean, true),
            Field::new("indexed", DataType::Boolean, true),
            Field::new("unique", DataType::Boolean, true),
        ],
        mode: ProcedureMode::Read,
        side_effects: SideEffects::ReadOnly,
        retry_contract: None,
        batch_input: None,
        docs: "Per-property metadata (type, nullable, indexed, unique) for a given label."
            .to_owned(),
    })
}

impl ProcedurePlugin for SchemaLabelInfoProc {
    fn signature(&self) -> &ProcedureSignature {
        schema_label_info_signature()
    }

    fn invoke(
        &self,
        ctx: ProcedureContext<'_>,
        args: &[ColumnarValue],
    ) -> Result<SendableRecordBatchStream, FnError> {
        use uni_common::core::schema::{ConstraintTarget, IndexDefinition};

        let host = require_host(&ctx)?;
        let label_name = require_string_arg(args, 0, "label")?;
        let uni_schema = host.storage().schema_manager().schema();

        let mut rows: Vec<HashMap<String, Value>> = Vec::new();
        if let Some(props) = uni_schema.properties.get(&label_name) {
            for (prop_name, prop_meta) in props {
                // Hide internal storage-layer columns (e.g. MUVERA `__fde_*` derived
                // vectors) from schema introspection.
                if prop_name.starts_with("__") {
                    continue;
                }
                let is_indexed = uni_schema.indexes.iter().any(|idx| match idx {
                    IndexDefinition::Vector(v) => v.label == label_name && v.property == *prop_name,
                    IndexDefinition::Scalar(s) => {
                        s.label == label_name && s.properties.contains(prop_name)
                    }
                    IndexDefinition::FullText(f) => {
                        f.label == label_name && f.properties.contains(prop_name)
                    }
                    IndexDefinition::Inverted(inv) => {
                        inv.label == label_name && inv.property == *prop_name
                    }
                    IndexDefinition::JsonFullText(j) => {
                        j.label == label_name && j.column == *prop_name
                    }
                    _ => false,
                });
                let unique = uni_schema.constraints.iter().any(|c| {
                    if let ConstraintTarget::Label(l) = &c.target
                        && l == &label_name
                        && c.enabled
                        && let Some(properties) = c.constraint_type.unique_properties()
                    {
                        return properties.contains(prop_name);
                    }
                    false
                });
                rows.push(HashMap::from([
                    ("property".to_owned(), Value::String(prop_name.clone())),
                    (
                        "dataType".to_owned(),
                        Value::String(format!("{:?}", prop_meta.r#type)),
                    ),
                    ("nullable".to_owned(), Value::Bool(prop_meta.nullable)),
                    ("indexed".to_owned(), Value::Bool(is_indexed)),
                    ("unique".to_owned(), Value::Bool(unique)),
                ]));
            }
        }
        let schema = Arc::new(Schema::new(schema_label_info_signature().yields.clone()));
        let batch = rows_to_batch(rows, schema.clone())?;
        Ok(single_batch_stream(schema, batch))
    }
}

// ---------------------------------------------------------------------------
// Registration
// ---------------------------------------------------------------------------

/// Register every `uni.schema.*` procedure into `r`. Edge types
/// register under both `uni.schema.edgeTypes` and
/// `uni.schema.relationshipTypes` for backward compatibility.
///
/// # Errors
///
/// Returns [`PluginError::DuplicateRegistration`] if a qname is taken.
pub fn register_into(r: &mut PluginRegistrar<'_>) -> Result<(), PluginError> {
    r.procedure(
        QName::new("uni", "schema.labels"),
        schema_labels_signature().clone(),
        Arc::new(SchemaLabelsProc),
    )?;
    let edge_types_impl: Arc<dyn ProcedurePlugin> = Arc::new(SchemaEdgeTypesProc);
    r.procedure(
        QName::new("uni", "schema.edgeTypes"),
        schema_edge_types_signature().clone(),
        Arc::clone(&edge_types_impl),
    )?;
    r.procedure(
        QName::new("uni", "schema.relationshipTypes"),
        schema_edge_types_signature().clone(),
        edge_types_impl,
    )?;
    r.procedure(
        QName::new("uni", "schema.indexes"),
        schema_indexes_signature().clone(),
        Arc::new(SchemaIndexesProc),
    )?;
    r.procedure(
        QName::new("uni", "schema.constraints"),
        schema_constraints_signature().clone(),
        Arc::new(SchemaConstraintsProc),
    )?;
    r.procedure(
        QName::new("uni", "schema.labelInfo"),
        schema_label_info_signature().clone(),
        Arc::new(SchemaLabelInfoProc),
    )?;
    Ok(())
}
