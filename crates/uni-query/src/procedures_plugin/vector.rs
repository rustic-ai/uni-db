// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! `uni.vector.query` — k-nearest-neighbor over a vector index.

use std::future::Future;
use std::sync::Arc;
use std::sync::OnceLock;

use arrow_array::RecordBatch;
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use datafusion::error::Result as DFResult;
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

use crate::procedures_plugin::host_args::{arg, columnar_args_to_values, require_host};
use crate::query::df_graph::search_procedures::run_vector_query;
use crate::query::executor::procedure_host::QueryProcedureHost;

// Rust guideline compliant

fn signature() -> &'static ProcedureSignature {
    static SIG: OnceLock<ProcedureSignature> = OnceLock::new();
    SIG.get_or_init(|| ProcedureSignature {
        args: vec![
            arg(
                "label",
                ArgType::Primitive(DataType::Utf8),
                "Vertex label to search.",
            ),
            arg(
                "property",
                ArgType::Primitive(DataType::Utf8),
                "Vector property name on the label.",
            ),
            arg(
                "query",
                ArgType::CypherValue,
                "Query vector (List<Float>) or query text (String, auto-embedded).",
            ),
            arg(
                "k",
                ArgType::Primitive(DataType::Int64),
                "Number of nearest neighbours to return.",
            ),
            arg(
                "filter",
                ArgType::Primitive(DataType::Utf8),
                "Optional pushdown filter expression.",
            ),
            arg(
                "threshold",
                ArgType::Primitive(DataType::Float64),
                "Optional minimum score threshold, on the scale of `score` (post-filter).",
            ),
            arg(
                "options",
                ArgType::CypherValue,
                "Optional options map: ANN tuning (`nprobes`, `refine_factor`, \
                 `ef_search`), candidate `over_fetch`, and reranker keys.",
            ),
        ],
        yields: vector_query_yields(),
        mode: ProcedureMode::Read,
        side_effects: SideEffects::ReadOnly,
        retry_contract: None,
        batch_input: None,
        docs:
            "Approximate-nearest-neighbour over a vector index with optional cross-encoder rerank."
                .to_owned(),
    })
}

/// Yield columns produced by `uni.vector.query`.
fn vector_query_yields() -> Vec<Field> {
    vec![
        vid_field(),
        Field::new("distance", DataType::Float64, true),
        Field::new("score", DataType::Float32, true),
        Field::new("rerank_score", DataType::Float32, true),
    ]
}

/// Yield columns produced by `uni.fts.query` (no `distance` — BM25 has
/// no distance metric).
pub(super) fn fts_query_yields() -> Vec<Field> {
    vec![
        vid_field(),
        Field::new("score", DataType::Float32, true),
        Field::new("rerank_score", DataType::Float32, true),
    ]
}

/// Yield columns produced by `uni.search` (hybrid — emits the full
/// fused-score family).
pub(super) fn hybrid_search_yields() -> Vec<Field> {
    vec![
        vid_field(),
        Field::new("score", DataType::Float32, true),
        Field::new("rerank_score", DataType::Float32, true),
        Field::new("vector_score", DataType::Float32, true),
        Field::new("fts_score", DataType::Float32, true),
        Field::new("sparse_score", DataType::Float32, true),
        Field::new("distance", DataType::Float64, true),
    ]
}

/// Build the canonical `vid` field for search-style procedures and tag
/// it with `_yield_kind = node_vid_source` so the planner's schema
/// builder knows this procedure supports node-shaped YIELD expansion
/// (`YIELD node` / `YIELD foo` projecting `<name>._vid + <name> +
/// <name>._labels + <name>.<prop>` columns). The tag is the seam that
/// replaced the procedure-name match arm in `procedure_call::build_schema`.
fn vid_field() -> Field {
    let mut md = std::collections::HashMap::new();
    md.insert("_yield_kind".to_owned(), "node_vid_source".to_owned());
    Field::new("vid", DataType::Int64, true).with_metadata(md)
}

#[derive(Debug)]
struct VectorQueryProc;

impl ProcedurePlugin for VectorQueryProc {
    fn signature(&self) -> &ProcedureSignature {
        signature()
    }

    fn invoke(
        &self,
        ctx: ProcedureContext<'_>,
        args: &[ColumnarValue],
    ) -> Result<SendableRecordBatchStream, FnError> {
        run_search_procedure(
            "uni.vector.query",
            &ctx,
            args,
            signature(),
            |host, uni_args, yield_items, output_schema| async move {
                let target_properties = host.target_properties().clone();
                run_vector_query(
                    &host,
                    &uni_args,
                    &yield_items,
                    &target_properties,
                    &output_schema,
                )
                .await
            },
        )
    }
}

/// Pick the right `(yield_items, output_schema)` for a search-plugin
/// invocation. When the host carries planner state (composite query),
/// honour it so node-shape yields expand correctly; otherwise fall back
/// to the plugin's `signature.yields` (standalone CALL with no
/// surrounding query plan, e.g. unit-test paths).
pub(super) fn resolve_yields_and_schema(
    host: &crate::query::executor::procedure_host::QueryProcedureHost,
    sig: &ProcedureSignature,
    fallback_schema: &Arc<Schema>,
) -> (Vec<(String, Option<String>)>, Arc<Schema>) {
    let host_yields = host.yield_items();
    if host_yields.is_empty() {
        let yield_items: Vec<(String, Option<String>)> = sig
            .yields
            .iter()
            .map(|f| (f.name().clone(), None))
            .collect();
        (yield_items, fallback_schema.clone())
    } else {
        let output_schema = host
            .expected_schema()
            .cloned()
            .unwrap_or_else(|| fallback_schema.clone());
        (host_yields.to_vec(), output_schema)
    }
}

/// Shared `ProcedurePlugin::invoke` body for the three host-coupled
/// search procedures (`uni.vector.query`, `uni.fts.query`, `uni.search`).
///
/// They differ only in their procedure name, signature, and the `run_*`
/// helper that produces the result batch; everything else (host
/// down-cast, arg decode, yield/schema resolution, single-batch
/// streaming) is identical.
pub(super) fn run_search_procedure<F, Fut>(
    proc_name: &'static str,
    ctx: &ProcedureContext<'_>,
    args: &[ColumnarValue],
    sig: &'static ProcedureSignature,
    run_fn: F,
) -> Result<SendableRecordBatchStream, FnError>
where
    F: FnOnce(QueryProcedureHost, Vec<Value>, Vec<(String, Option<String>)>, SchemaRef) -> Fut
        + Send
        + 'static,
    Fut: Future<Output = DFResult<Option<RecordBatch>>> + Send + 'static,
{
    let host = require_host(ctx, proc_name)?.clone();
    let uni_args = columnar_args_to_values(args);
    let fallback_schema = Arc::new(Schema::new(sig.yields.clone()));
    let (yield_items, output_schema) = resolve_yields_and_schema(&host, sig, &fallback_schema);

    let stream_schema = output_schema.clone();
    let stream = stream::once(async move {
        let batch = run_fn(host, uni_args, yield_items, output_schema.clone())
            .await?
            .unwrap_or_else(|| RecordBatch::new_empty(output_schema.clone()));
        Ok::<_, datafusion::error::DataFusionError>(batch)
    });
    Ok(Box::pin(RecordBatchStreamAdapter::new(
        stream_schema,
        stream,
    )))
}

/// Register `uni.vector.query` into `r`.
///
/// # Errors
///
/// Returns [`PluginError::DuplicateRegistration`] if a qname is taken.
pub fn register_into(r: &mut PluginRegistrar<'_>) -> Result<(), PluginError> {
    r.procedure(
        QName::new("uni", "vector.query"),
        signature().clone(),
        Arc::new(VectorQueryProc),
    )?;
    Ok(())
}
