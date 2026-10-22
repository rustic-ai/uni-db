// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! `uni.graph.{project, drop, list, exists}` procedures — named
//! graph-projection lifecycle (M5c.4 / proposal §4.10.3).
//!
//! Projections live in the per-`StorageManager` [`crate::projection_store::ProjectionStore`]
//! (see `crates/uni-query/src/projection_store.rs`). v1 is in-memory
//! only — restart clears every projection — and the only eviction
//! mechanism is `uni.graph.drop`.
//!
//! The procedures live in `uni-query` (not `uni-plugin-builtin`)
//! because they need to call `QueryProcedureHost::execute_inner_query`
//! and `crate::projection_store::for_storage`, both of which are uni-
//! query types that `uni-plugin-builtin` cannot reach without an
//! inverted dependency.

use std::sync::{Arc, OnceLock};
use std::time::SystemTime;

use arrow_array::{ArrayRef, BooleanArray, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use datafusion::execution::SendableRecordBatchStream;
use datafusion::logical_expr::ColumnarValue;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::scalar::ScalarValue;
use uni_algo::{ProjectionInput, parse_graph_ref};
use uni_plugin::traits::procedure::{
    ProcedureContext, ProcedureMode, ProcedurePlugin, ProcedureSignature,
};
use uni_plugin::traits::scalar::ArgType;
use uni_plugin::{FnError, PluginError, PluginRegistrar, QName, SideEffects};

use crate::procedures_plugin::host_args::{arg, arg_with_default};
use crate::projection_store::{ProjectionEntry, ProjectionSourceKind, estimate_bytes, for_storage};
use crate::query::executor::procedure_host::QueryProcedureHost;

// Rust guideline compliant

/// Register every `uni.graph.*` procedure into `r`.
///
/// # Errors
///
/// Propagates [`PluginError::DuplicateRegistration`] if any qname is
/// already taken in the underlying plugin registry.
pub fn register_into(r: &mut PluginRegistrar<'_>) -> Result<(), PluginError> {
    r.procedure(
        QName::new("uni", "graph.project"),
        ProjectProcedure::signature_static().clone(),
        Arc::new(ProjectProcedure),
    )?;
    r.procedure(
        QName::new("uni", "graph.drop"),
        DropProcedure::signature_static().clone(),
        Arc::new(DropProcedure),
    )?;
    r.procedure(
        QName::new("uni", "graph.list"),
        ListProcedure::signature_static().clone(),
        Arc::new(ListProcedure),
    )?;
    r.procedure(
        QName::new("uni", "graph.exists"),
        ExistsProcedure::signature_static().clone(),
        Arc::new(ExistsProcedure),
    )?;
    Ok(())
}

// ─────────────────────────── helpers ──────────────────────────────

fn require_host<'a>(ctx: &ProcedureContext<'a>) -> Result<&'a QueryProcedureHost, FnError> {
    ctx.host
        .and_then(|h| h.as_any().downcast_ref::<QueryProcedureHost>())
        .ok_or_else(|| FnError::new(0x701, "uni.graph.*: requires QueryProcedureHost"))
}

/// Decode a positional arg into a `serde_json::Value`. Mirrors the
/// algo adapter's decoder (LargeBinary → JSON for Map / List; scalars
/// pass through).
/// A malformed argument is an error, not a JSON `null`: running the
/// procedure on `null` reports success for a request that was never
/// understood (#233 class).
fn arg_to_json(cv: &ColumnarValue) -> Result<serde_json::Value, FnError> {
    Ok(match cv {
        ColumnarValue::Scalar(ScalarValue::LargeBinary(Some(b)))
        | ColumnarValue::Scalar(ScalarValue::Binary(Some(b))) => serde_json::from_slice::<
            serde_json::Value,
        >(b)
        .map_err(|e| FnError::new(0x820, format!("procedure argument is not valid JSON: {e}")))?,
        ColumnarValue::Scalar(ScalarValue::Utf8(Some(s)))
        | ColumnarValue::Scalar(ScalarValue::LargeUtf8(Some(s))) => {
            serde_json::Value::String(s.clone())
        }
        ColumnarValue::Scalar(ScalarValue::Boolean(Some(b))) => serde_json::Value::Bool(*b),
        ColumnarValue::Scalar(ScalarValue::Int64(Some(i))) => {
            serde_json::Value::Number((*i).into())
        }
        _ => serde_json::Value::Null,
    })
}

fn arg_as_string(cv: &ColumnarValue) -> Option<String> {
    match cv {
        ColumnarValue::Scalar(ScalarValue::Utf8(Some(s)))
        | ColumnarValue::Scalar(ScalarValue::LargeUtf8(Some(s))) => Some(s.clone()),
        _ => None,
    }
}

fn one_row_stream(
    schema: SchemaRef,
    cols: Vec<ArrayRef>,
) -> Result<SendableRecordBatchStream, FnError> {
    let batch = RecordBatch::try_new(Arc::clone(&schema), cols)
        .map_err(|e| FnError::new(0x830, format!("RecordBatch build: {e}")))?;
    let stream =
        futures::stream::once(async move { Ok::<_, datafusion::error::DataFusionError>(batch) });
    Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
}

// ─────────────────────────── uni.graph.project ──────────────────────────────

/// `uni.graph.project(name, graphRef, config) -> (name, node_count,
/// edge_count, bytes)`.
///
/// Materialises the projection described by `graphRef` (Native or
/// Cypher; Named is rejected — no projection-of-a-projection) and
/// stores it under `name` in the per-`StorageManager` projection
/// store. Duplicate names error with `FnError 0x824`.
#[derive(Debug)]
pub struct ProjectProcedure;

impl ProjectProcedure {
    fn signature_static() -> &'static ProcedureSignature {
        static SIG: OnceLock<ProcedureSignature> = OnceLock::new();
        SIG.get_or_init(|| ProcedureSignature {
            args: vec![
                arg(
                    "name",
                    ArgType::Primitive(DataType::Utf8),
                    "Name to register the materialised projection under.",
                ),
                arg(
                    "graphRef",
                    ArgType::Primitive(DataType::LargeBinary),
                    "Native or Cypher projection descriptor (Map).",
                ),
                arg_with_default(
                    "config",
                    ArgType::Primitive(DataType::LargeBinary),
                    ScalarValue::LargeBinary(Some(b"{}".to_vec())),
                    "Materialisation options (currently unused).",
                ),
            ],
            yields: vec![
                Field::new("name", DataType::Utf8, false),
                Field::new("node_count", DataType::Int64, false),
                Field::new("edge_count", DataType::Int64, false),
                Field::new("bytes", DataType::Int64, false),
            ],
            mode: ProcedureMode::Read, // store mutation is in-memory, no graph-write
            side_effects: SideEffects::ReadOnly,
            retry_contract: None,
            batch_input: None,
            docs: "uni.graph.project(name, graphRef, config) — materialise \
                   a named graph projection from a Native or Cypher graphRef \
                   (no Named-of-Named). v1: in-memory, restart-clears."
                .to_owned(),
        })
    }
}

impl ProcedurePlugin for ProjectProcedure {
    fn signature(&self) -> &ProcedureSignature {
        Self::signature_static()
    }

    fn invoke(
        &self,
        ctx: ProcedureContext<'_>,
        args: &[ColumnarValue],
    ) -> Result<SendableRecordBatchStream, FnError> {
        let host = require_host(&ctx)?.clone();
        let name = args
            .first()
            .and_then(arg_as_string)
            .ok_or_else(|| FnError::new(0x824, "uni.graph.project: name (String) required"))?;
        let graph_ref = args
            .get(1)
            .map(arg_to_json)
            .transpose()?
            .ok_or_else(|| FnError::new(0x824, "uni.graph.project: graphRef (Map) required"))?;
        let projection_input = parse_graph_ref(&graph_ref)
            .map_err(|e| FnError::new(0x820, format!("graphRef parse: {e}")))?;

        // The projection catalog is shared by every session and is not undone
        // by a rollback (as in GDS), so a projection built over a
        // transaction's uncommitted writes would publish them -- before commit,
        // and for good after a rollback. Refuse; a transaction without writes
        // builds from its own snapshot.
        if host
            .l0_context()
            .transaction_l0
            .as_ref()
            .is_some_and(|tx_l0| tx_l0.read().mutation_count > 0)
        {
            return Err(FnError::new(
                0x824,
                uni_common::UniError::uncommitted_writes_message(
                    "uni.graph.project",
                    "the projection catalog is not transactional, so the projection would \
                     publish the transaction's uncommitted writes",
                ),
            ));
        }

        // Pre-check duplicates eagerly so the caller sees the error
        // synchronously (matches `uni.graph.exists` ordering); the
        // actual materialisation work is async and runs inside the
        // result stream.
        let store = for_storage(host.storage());
        if store.contains(&name) {
            return Err(FnError::new(
                0x824,
                format!("uni.graph.project: projection `{name}` already exists; drop first"),
            ));
        }
        if let ProjectionInput::Named { .. } = &projection_input {
            return Err(FnError::new(
                0x824,
                "uni.graph.project: graphRef cannot itself be Named \
                 (no projection-of-a-projection in v1)",
            ));
        }

        let schema: SchemaRef = Arc::new(Schema::new(Self::signature_static().yields.clone()));
        let name_for_async = name.clone();
        let store_for_async = Arc::clone(&store);

        let schema_in_fut = Arc::clone(&schema);
        let fut = async move {
            let (projection, source_kind) = match projection_input {
                ProjectionInput::Native {
                    node_labels,
                    edge_types,
                    weight_property,
                    include_reverse,
                } => {
                    let storage = Arc::clone(host.storage());
                    let l0 = build_l0_manager(&host);
                    let mut builder = uni_algo::ProjectionBuilder::new(storage)
                        .l0_manager(l0)
                        .property_manager(host.property_manager().cloned())
                        .node_labels(&node_labels.iter().map(String::as_str).collect::<Vec<_>>())
                        .edge_types(&edge_types.iter().map(String::as_str).collect::<Vec<_>>())
                        .include_reverse(include_reverse);
                    if let Some(wp) = weight_property {
                        builder = builder.weight_property(&wp);
                    }
                    let projection = builder.build().await.map_err(|e| {
                        datafusion::error::DataFusionError::Execution(format!(
                            "uni.graph.project (Native): {e}"
                        ))
                    })?;
                    (projection, ProjectionSourceKind::Native)
                }
                ProjectionInput::Cypher {
                    node_query,
                    edge_query,
                    weight_column,
                    include_reverse,
                } => {
                    let inner_params = std::collections::HashMap::new();
                    let node_rows = host
                        .execute_inner_query(
                            &node_query,
                            &inner_params,
                            uni_plugin::traits::procedure::ProcedureMode::Read,
                        )
                        .await
                        .map_err(|e| {
                            datafusion::error::DataFusionError::Execution(format!(
                                "uni.graph.project node query: {e}"
                            ))
                        })?;
                    let edge_rows = host
                        .execute_inner_query(
                            &edge_query,
                            &inner_params,
                            uni_plugin::traits::procedure::ProcedureMode::Read,
                        )
                        .await
                        .map_err(|e| {
                            datafusion::error::DataFusionError::Execution(format!(
                                "uni.graph.project edge query: {e}"
                            ))
                        })?;
                    let projection = uni_algo::algo::projection::GraphProjection::from_rows(
                        &node_rows,
                        &edge_rows,
                        weight_column.as_deref(),
                        include_reverse,
                    )
                    .map_err(|e| {
                        datafusion::error::DataFusionError::Execution(format!(
                            "uni.graph.project (Cypher): {e}"
                        ))
                    })?;
                    (projection, ProjectionSourceKind::Cypher)
                }
                ProjectionInput::Named { .. } => unreachable!("filtered above"),
            };
            let node_count = projection.vertex_count();
            let edge_count = projection.edge_count();
            let bytes = estimate_bytes(&projection);
            let entry = ProjectionEntry {
                projection: Arc::new(projection),
                node_count,
                edge_count,
                bytes,
                created_at: SystemTime::now(),
                source_kind,
            };
            store_for_async
                .insert(name_for_async.clone(), entry)
                .map_err(|n| {
                    datafusion::error::DataFusionError::Execution(format!(
                        "uni.graph.project: projection `{n}` already exists"
                    ))
                })?;

            let cols: Vec<ArrayRef> = vec![
                Arc::new(StringArray::from(vec![name_for_async])),
                Arc::new(Int64Array::from(vec![node_count as i64])),
                Arc::new(Int64Array::from(vec![edge_count as i64])),
                Arc::new(Int64Array::from(vec![bytes as i64])),
            ];
            RecordBatch::try_new(schema_in_fut, cols).map_err(|e| {
                datafusion::error::DataFusionError::Execution(format!("RecordBatch: {e}"))
            })
        };
        let stream = futures::stream::once(fut);
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
    }
}

// ─────────────────────────── uni.graph.drop ──────────────────────────────

/// `uni.graph.drop(name) -> (dropped)`. Returns `false` when no
/// projection by that name existed (not an error).
#[derive(Debug)]
pub struct DropProcedure;

impl DropProcedure {
    fn signature_static() -> &'static ProcedureSignature {
        static SIG: OnceLock<ProcedureSignature> = OnceLock::new();
        SIG.get_or_init(|| ProcedureSignature {
            args: vec![arg(
                "name",
                ArgType::Primitive(DataType::Utf8),
                "Projection name to evict from the store.",
            )],
            yields: vec![Field::new("dropped", DataType::Boolean, false)],
            mode: ProcedureMode::Read,
            side_effects: SideEffects::ReadOnly,
            retry_contract: None,
            batch_input: None,
            docs: "uni.graph.drop(name) — remove a named projection. Returns \
                   `false` if no projection by that name existed."
                .to_owned(),
        })
    }
}

impl ProcedurePlugin for DropProcedure {
    fn signature(&self) -> &ProcedureSignature {
        Self::signature_static()
    }

    fn invoke(
        &self,
        ctx: ProcedureContext<'_>,
        args: &[ColumnarValue],
    ) -> Result<SendableRecordBatchStream, FnError> {
        let host = require_host(&ctx)?;
        let name = args
            .first()
            .and_then(arg_as_string)
            .ok_or_else(|| FnError::new(0x824, "uni.graph.drop: name (String) required"))?;
        let dropped = for_storage(host.storage()).drop_by_name(&name);
        let schema: SchemaRef = Arc::new(Schema::new(Self::signature_static().yields.clone()));
        let cols: Vec<ArrayRef> = vec![Arc::new(BooleanArray::from(vec![dropped]))];
        one_row_stream(schema, cols)
    }
}

// ─────────────────────────── uni.graph.list ──────────────────────────────

/// `uni.graph.list() -> (name, node_count, edge_count, bytes,
/// created_at_ms, source_kind)`.
#[derive(Debug)]
pub struct ListProcedure;

impl ListProcedure {
    fn signature_static() -> &'static ProcedureSignature {
        static SIG: OnceLock<ProcedureSignature> = OnceLock::new();
        SIG.get_or_init(|| ProcedureSignature {
            args: vec![],
            yields: vec![
                Field::new("name", DataType::Utf8, false),
                Field::new("node_count", DataType::Int64, false),
                Field::new("edge_count", DataType::Int64, false),
                Field::new("bytes", DataType::Int64, false),
                // Wall-clock instant the projection was materialised,
                // as a plain Int64 millisecond count (more portable
                // through Cypher than `DataType::Timestamp`, which the
                // simple-executor scalar decoder doesn't translate).
                Field::new("created_at_ms", DataType::Int64, false),
                Field::new("source_kind", DataType::Utf8, false),
            ],
            mode: ProcedureMode::Read,
            side_effects: SideEffects::ReadOnly,
            retry_contract: None,
            batch_input: None,
            docs: "uni.graph.list — one row per stored projection. \
                   `source_kind` is `Native` or `Cypher`."
                .to_owned(),
        })
    }
}

impl ProcedurePlugin for ListProcedure {
    fn signature(&self) -> &ProcedureSignature {
        Self::signature_static()
    }

    fn invoke(
        &self,
        ctx: ProcedureContext<'_>,
        _args: &[ColumnarValue],
    ) -> Result<SendableRecordBatchStream, FnError> {
        let host = require_host(&ctx)?;
        let entries = for_storage(host.storage()).list();
        let mut names = Vec::with_capacity(entries.len());
        let mut nodes = Vec::with_capacity(entries.len());
        let mut edges = Vec::with_capacity(entries.len());
        let mut bytes = Vec::with_capacity(entries.len());
        let mut created = Vec::with_capacity(entries.len());
        let mut kinds = Vec::with_capacity(entries.len());
        for (name, e) in entries {
            names.push(name);
            nodes.push(e.node_count as i64);
            edges.push(e.edge_count as i64);
            bytes.push(e.bytes as i64);
            let ms = e
                .created_at
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_millis() as i64)
                .unwrap_or(0);
            created.push(ms);
            kinds.push(e.source_kind.as_str().to_owned());
        }
        let schema: SchemaRef = Arc::new(Schema::new(Self::signature_static().yields.clone()));
        let cols: Vec<ArrayRef> = vec![
            Arc::new(StringArray::from(names)),
            Arc::new(Int64Array::from(nodes)),
            Arc::new(Int64Array::from(edges)),
            Arc::new(Int64Array::from(bytes)),
            Arc::new(Int64Array::from(created)),
            Arc::new(StringArray::from(kinds)),
        ];
        let batch = RecordBatch::try_new(Arc::clone(&schema), cols)
            .map_err(|e| FnError::new(0x830, format!("RecordBatch build: {e}")))?;
        let stream =
            futures::stream::once(
                async move { Ok::<_, datafusion::error::DataFusionError>(batch) },
            );
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
    }
}

// ─────────────────────────── uni.graph.exists ──────────────────────────────

/// `uni.graph.exists(name) -> (exists)`. Pure read; never errors on
/// missing names.
#[derive(Debug)]
pub struct ExistsProcedure;

impl ExistsProcedure {
    fn signature_static() -> &'static ProcedureSignature {
        static SIG: OnceLock<ProcedureSignature> = OnceLock::new();
        SIG.get_or_init(|| ProcedureSignature {
            args: vec![arg(
                "name",
                ArgType::Primitive(DataType::Utf8),
                "Projection name to probe.",
            )],
            yields: vec![Field::new("exists", DataType::Boolean, false)],
            mode: ProcedureMode::Read,
            side_effects: SideEffects::ReadOnly,
            retry_contract: None,
            batch_input: None,
            docs: "uni.graph.exists(name) — `true` iff a projection by that \
                   name is currently in the store."
                .to_owned(),
        })
    }
}

impl ProcedurePlugin for ExistsProcedure {
    fn signature(&self) -> &ProcedureSignature {
        Self::signature_static()
    }

    fn invoke(
        &self,
        ctx: ProcedureContext<'_>,
        args: &[ColumnarValue],
    ) -> Result<SendableRecordBatchStream, FnError> {
        let host = require_host(&ctx)?;
        let name = args
            .first()
            .and_then(arg_as_string)
            .ok_or_else(|| FnError::new(0x824, "uni.graph.exists: name (String) required"))?;
        let exists = for_storage(host.storage()).contains(&name);
        let schema: SchemaRef = Arc::new(Schema::new(Self::signature_static().yields.clone()));
        let cols: Vec<ArrayRef> = vec![Arc::new(BooleanArray::from(vec![exists]))];
        one_row_stream(schema, cols)
    }
}

// ─────────────────────────── shared helpers ──────────────────────────────

/// Build an `L0Manager` snapshot mirroring the host's L0 visibility so
/// `uni.graph.project` (Native) sees the same recently-written rows
/// the outer query would.
fn build_l0_manager(host: &QueryProcedureHost) -> Option<Arc<uni_store::runtime::L0Manager>> {
    host.l0_context().read_view()
}
