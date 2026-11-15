// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Custom `PhysicalExpr` for `similar_to()` within DataFusion.
//!
//! Evaluates unified similarity scoring (vector cosine, FTS/BM25,
//! auto-embed, multi-source fusion) as a columnar expression,
//! replacing the row-by-row fallback executor path.

use crate::query::df_graph::GraphExecutionContext;
use crate::query::similar_to::{
    F32Rows, FusionMethod, SimilarToOptions, fuse_scores, normalize_bm25, parse_options,
    score_vectors, validate_options, value_to_f32_vec,
};
use crate::types::QueryWarning;
use arrow_array::builder::Float64Builder;
use arrow_array::{Array, UInt64Array};
use arrow_schema::{DataType, Schema};
use datafusion::physical_plan::{ColumnarValue, DisplayAs, DisplayFormatType, PhysicalExpr};
use std::collections::HashMap;
use std::sync::Arc;
use uni_common::Value;
use uni_common::core::id::Vid;
use uni_common::core::schema::DistanceMetric;

/// Physical expression that evaluates `similar_to(sources, queries [, options])`.
///
/// Handles all scoring modes within DataFusion's columnar execution:
/// - Vector + Vector → cosine similarity per-row
/// - Vector + String → auto-embed query once per-batch, then cosine per-row
/// - String + String → FTS search once per-batch, lookup VID per-row
/// - Multi-source → score each pair, fuse with RRF or weighted sum
pub(crate) struct SimilarToExecExpr {
    /// Compiled child expressions for each source (1 for single, N for multi-source).
    source_children: Vec<Arc<dyn PhysicalExpr>>,
    /// Compiled child expressions for each query (1 for single, N for multi-source).
    query_children: Vec<Arc<dyn PhysicalExpr>>,
    /// Optional compiled expression for options map (3rd arg).
    options_child: Option<Arc<dyn PhysicalExpr>>,
    /// Graph execution context (storage + xervo runtime).
    graph_ctx: Arc<GraphExecutionContext>,
    /// Variable name from source property access (e.g., "d" from `d.content`).
    source_variable: Option<String>,
    /// Property names per source (e.g., ["embedding", "content"] for multi-source).
    source_property_names: Vec<Option<String>>,
    /// Per-source distance metrics resolved at compile time. `None` for FTS sources.
    source_metrics: Vec<Option<DistanceMetric>>,
}

impl SimilarToExecExpr {
    pub(crate) fn new(
        source_children: Vec<Arc<dyn PhysicalExpr>>,
        query_children: Vec<Arc<dyn PhysicalExpr>>,
        options_child: Option<Arc<dyn PhysicalExpr>>,
        graph_ctx: Arc<GraphExecutionContext>,
        source_variable: Option<String>,
        source_property_names: Vec<Option<String>>,
        source_metrics: Vec<Option<DistanceMetric>>,
    ) -> Self {
        Self {
            source_children,
            query_children,
            options_child,
            graph_ctx,
            source_variable,
            source_property_names,
            source_metrics,
        }
    }
}

impl std::fmt::Debug for SimilarToExecExpr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SimilarToExecExpr")
            .field("num_sources", &self.source_children.len())
            .field("source_variable", &self.source_variable)
            .finish_non_exhaustive()
    }
}

impl std::fmt::Display for SimilarToExecExpr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "similar_to(<{} sources>)", self.source_children.len())
    }
}

impl PartialEq<dyn PhysicalExpr> for SimilarToExecExpr {
    fn eq(&self, _other: &dyn PhysicalExpr) -> bool {
        false
    }
}

impl PartialEq for SimilarToExecExpr {
    fn eq(&self, _other: &Self) -> bool {
        false
    }
}

impl Eq for SimilarToExecExpr {}

impl std::hash::Hash for SimilarToExecExpr {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        "SimilarToExecExpr".hash(state);
    }
}

impl DisplayAs for SimilarToExecExpr {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{}", self)
    }
}

/// Convert a `ColumnarValue` to a `Value` for a given row.
fn columnar_value_to_value(
    cv: &ColumnarValue,
    _batch: &arrow_array::RecordBatch,
    row: usize,
) -> Value {
    match cv {
        ColumnarValue::Array(arr) => arrow_to_value_at(arr.as_ref(), row),
        ColumnarValue::Scalar(sv) => sv
            .to_array_of_size(1)
            .map(|arr| arrow_to_value_at(arr.as_ref(), 0))
            .unwrap_or(Value::Null),
    }
}

/// Extract a `Value` from an Arrow array at the given row index.
///
/// Handles the main types we expect from similar_to arguments:
/// LargeBinary (CypherValue), Float64, Utf8, UInt64, Int64, and lists.
fn arrow_to_value_at(col: &dyn Array, row: usize) -> Value {
    use arrow_array::*;

    if col.is_null(row) {
        return Value::Null;
    }

    match col.data_type() {
        DataType::LargeBinary => {
            let bytes = col
                .as_any()
                .downcast_ref::<LargeBinaryArray>()
                .unwrap()
                .value(row);
            if bytes.is_empty() {
                Value::Null
            } else {
                // Documented exception (#233 class): `arrow_to_value_at` is
                // infallible and recursive inside a `PhysicalExpr`.
                match uni_common::cypher_value_codec::decode(bytes) {
                    Ok(v) => v,
                    Err(e) => {
                        tracing::error!(error = %e, "similar_to: CypherValue failed to decode");
                        Value::Null
                    }
                }
            }
        }
        DataType::Float64 => Value::Float(
            col.as_any()
                .downcast_ref::<Float64Array>()
                .unwrap()
                .value(row),
        ),
        DataType::Float32 => Value::Float(
            col.as_any()
                .downcast_ref::<Float32Array>()
                .unwrap()
                .value(row) as f64,
        ),
        DataType::Utf8 => Value::String(
            col.as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(row)
                .to_string(),
        ),
        DataType::LargeUtf8 => Value::String(
            col.as_any()
                .downcast_ref::<LargeStringArray>()
                .unwrap()
                .value(row)
                .to_string(),
        ),
        // `Utf8View`: a distinct Arrow type DataFusion 54 returns from more and
        // more string functions. Unmatched, it lands on the `Value::Null` arm.
        DataType::Utf8View => Value::String(
            col.as_any()
                .downcast_ref::<arrow_array::StringViewArray>()
                .unwrap()
                .value(row)
                .to_string(),
        ),
        DataType::Int64 => Value::Int(
            col.as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(row),
        ),
        // An engine `UInt64` is an identity: its bits, not a value (see `Value::from_id_bits`).
        DataType::UInt64 => Value::from_id_bits(
            col.as_any()
                .downcast_ref::<UInt64Array>()
                .unwrap()
                .value(row),
        ),
        DataType::FixedSizeList(_, _) => {
            let fsl = col.as_any().downcast_ref::<FixedSizeListArray>().unwrap();
            let values = fsl.value(row);
            if let Some(f32_arr) = values.as_any().downcast_ref::<Float32Array>() {
                Value::Vector((0..f32_arr.len()).map(|i| f32_arr.value(i)).collect())
            } else if let Some(f64_arr) = values.as_any().downcast_ref::<Float64Array>() {
                Value::Vector(
                    (0..f64_arr.len())
                        .map(|i| f64_arr.value(i) as f32)
                        .collect(),
                )
            } else {
                Value::Null
            }
        }
        DataType::LargeList(_) => {
            let values = col
                .as_any()
                .downcast_ref::<LargeListArray>()
                .unwrap()
                .value(row);
            Value::List(
                (0..values.len())
                    .map(|i| arrow_to_value_at(values.as_ref(), i))
                    .collect(),
            )
        }
        DataType::List(_) => {
            let values = col.as_any().downcast_ref::<ListArray>().unwrap().value(row);
            Value::List(
                (0..values.len())
                    .map(|i| arrow_to_value_at(values.as_ref(), i))
                    .collect(),
            )
        }
        // Same boundary as the other query-side decoders: an entity struct
        // decodes to its native form rather than a second encoding of it.
        // Documented exception (#233 class). `arrow_to_value_at` is infallible
        // and recursive, and it sits inside a `PhysicalExpr` whose surrounding
        // evaluation is `Value`-typed, so a failure has nowhere to go from
        // here. Logged rather than swallowed; the value still degrades to
        // Null, which is why this is an exception and not a fix.
        _ => match uni_store::storage::arrow_convert::arrow_to_value(col, row, None) {
            Ok(v) => v.canonical_entity(),
            Err(e) => {
                tracing::error!(error = %e, "similar_to: column value failed to decode");
                Value::Null
            }
        },
    }
}

/// Pre-computed async resources for batch-level FTS/auto-embed operations.
struct PrecomputedResources {
    embed_vectors: Vec<Option<Vec<f32>>>,
    fts_results: Vec<Option<HashMap<Vid, f32>>>,
}

/// Scoring mode for a single (source, query) pair.
enum ScoringMode {
    /// Both are vectors → metric-aware similarity per-row.
    Vector(DistanceMetric),
    /// Source is a vector, query is a string → auto-embed once, then metric-aware per-row.
    AutoEmbed(DistanceMetric),
    /// Both are strings → FTS search once, VID lookup per-row.
    Fts,
    /// Source or query is NULL → the row's result is NULL (no scoring/fusion).
    Null,
}

impl PhysicalExpr for SimilarToExecExpr {
    fn data_type(&self, _input_schema: &Schema) -> datafusion::error::Result<DataType> {
        Ok(DataType::Float64)
    }

    fn nullable(&self, _input_schema: &Schema) -> datafusion::error::Result<bool> {
        Ok(true)
    }

    fn evaluate(
        &self,
        batch: &arrow_array::RecordBatch,
    ) -> datafusion::error::Result<ColumnarValue> {
        let num_rows = batch.num_rows();
        let num_sources = self.source_children.len();

        // 1. Evaluate all child expressions to ColumnarValues
        let source_cvs: Vec<_> = self
            .source_children
            .iter()
            .map(|c| c.evaluate(batch))
            .collect::<datafusion::error::Result<Vec<_>>>()?;
        let query_cvs: Vec<_> = self
            .query_children
            .iter()
            .map(|c| c.evaluate(batch))
            .collect::<datafusion::error::Result<Vec<_>>>()?;

        // Two stored vectors and no options: score from the Arrow buffers.
        if num_sources == 1
            && self.options_child.is_none()
            && let Some(scores) = score_fixed_size_lists(
                &source_cvs[0],
                &query_cvs[0],
                self.source_metrics.first().and_then(|m| m.as_ref()),
                num_rows,
            )
        {
            return scores.map(ColumnarValue::Array);
        }

        // 2. Parse options from the options child (if present)
        let opts = if let Some(ref opts_child) = self.options_child {
            let opts_cv = opts_child.evaluate(batch)?;
            // Options are typically a constant map — evaluate from first row
            let opts_val = columnar_value_to_value(&opts_cv, batch, 0);
            parse_options(&opts_val).map_err(|e| {
                datafusion::error::DataFusionError::Execution(format!("similar_to options: {}", e))
            })?
        } else {
            SimilarToOptions::default()
        };

        validate_options(&opts, num_sources).map_err(|e| {
            datafusion::error::DataFusionError::Execution(format!("similar_to: {}", e))
        })?;

        // 3. Determine scoring mode per source.
        if num_rows == 0 {
            let mut builder = Float64Builder::with_capacity(0);
            return Ok(ColumnarValue::Array(Arc::new(builder.finish())));
        }

        // Pick, per source column, the first row whose source AND query are both
        // non-NULL — that representative row decides the mode and supplies any
        // FTS/auto-embed query string. NULL rows never decide the mode (they are
        // scored as NULL per-row below), so a leading NULL no longer aborts the
        // batch nor forces a real-valued column to NULL.
        let representative_rows: Vec<Option<usize>> = (0..num_sources)
            .map(|i| {
                (0..num_rows).find(|&r| {
                    let s = columnar_value_to_value(&source_cvs[i], batch, r);
                    let q = columnar_value_to_value(&query_cvs[i], batch, r);
                    !matches!(s, Value::Null) && !matches!(q, Value::Null)
                })
            })
            .collect();

        let repr_source_vals: Vec<Value> = (0..num_sources)
            .map(|i| match representative_rows[i] {
                Some(r) => columnar_value_to_value(&source_cvs[i], batch, r),
                None => Value::Null,
            })
            .collect();
        let repr_query_vals: Vec<Value> = (0..num_sources)
            .map(|i| match representative_rows[i] {
                Some(r) => columnar_value_to_value(&query_cvs[i], batch, r),
                None => Value::Null,
            })
            .collect();

        let scoring_modes: Vec<ScoringMode> = repr_source_vals
            .iter()
            .zip(repr_query_vals.iter())
            .enumerate()
            .map(|(i, (s, q))| {
                determine_scoring_mode(s, q, self.source_metrics.get(i).and_then(|m| m.as_ref()))
            })
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| {
                datafusion::error::DataFusionError::Execution(format!("similar_to: {}", e))
            })?;

        // 4. Resolve VID column for FTS scoring
        let vid_col_idx = self.source_variable.as_ref().and_then(|var| {
            let vid_col_name = format!("{}._vid", var);
            batch.schema().index_of(&vid_col_name).ok()
        });

        // 5. Resolve label for FTS scoring
        let label = self.source_variable.as_ref().and_then(|var| {
            let labels_col_name = format!("{}._labels", var);
            if let Ok(idx) = batch.schema().index_of(&labels_col_name) {
                let col = batch.column(idx);
                let val = arrow_to_value_at(col.as_ref(), 0);
                match val {
                    Value::String(s) => Some(s),
                    Value::List(list) => list.first().and_then(|v| v.as_str()).map(String::from),
                    _ => None,
                }
            } else {
                None
            }
        });

        // 6. Pre-compute async resources (FTS results, auto-embed vectors)
        //    using a dedicated thread with its own tokio runtime,
        //    following the ExistsExecExpr pattern.
        let graph_ctx = self.graph_ctx.clone();
        let source_property_names = self.source_property_names.clone();

        // Collect query strings for FTS/auto-embed (from the representative row
        // chosen above — typically constant across rows).
        let query_strings: Vec<Option<String>> = repr_query_vals
            .iter()
            .map(|v| match v {
                Value::String(s) => Some(s.clone()),
                _ => None,
            })
            .collect();

        let pre_label = label.clone();
        let opts_fts_k = opts.fts_k;

        let precomputed = std::thread::scope(|s| {
            s.spawn(|| {
                // The process-wide runtime, not one built per call: state this
                // work first creates (Lance index readers and their I/O loop)
                // is cached beyond the call and bound to its runtime (#290).
                let rt = uni_store::runtime::io_runtime::io_runtime().map_err(|e| {
                    datafusion::error::DataFusionError::Execution(format!(
                        "Failed to get runtime for similar_to: {e}"
                    ))
                })?;

                let mut embed_vectors = vec![None; num_sources];
                let mut fts_results = vec![None; num_sources];

                for (i, mode) in scoring_modes.iter().enumerate() {
                    match mode {
                        ScoringMode::AutoEmbed(_) => {
                            let query_text = query_strings[i].as_deref().unwrap_or("");
                            let vec = rt.block_on(auto_embed_query(
                                &graph_ctx,
                                pre_label.as_deref(),
                                source_property_names.get(i).and_then(|p| p.as_deref()),
                                query_text,
                            ))?;
                            embed_vectors[i] = Some(vec);
                        }
                        ScoringMode::Fts => {
                            let query_text = query_strings[i].as_deref().unwrap_or("");
                            let (lbl, prop) = resolve_fts_label_property(
                                &graph_ctx,
                                pre_label.as_deref(),
                                source_property_names.get(i).and_then(|p| p.as_deref()),
                            )?;
                            let results = rt.block_on(fts_search_batch(
                                &graph_ctx, &lbl, &prop, query_text, opts_fts_k,
                            ))?;
                            fts_results[i] = Some(results);
                        }
                        // No pre-computation needed for direct vector scoring
                        // or NULL-propagating sources.
                        ScoringMode::Vector(_) | ScoringMode::Null => {}
                    }
                }

                Ok::<_, datafusion::error::DataFusionError>(PrecomputedResources {
                    embed_vectors,
                    fts_results,
                })
            })
            .join()
            .unwrap_or_else(|_| {
                Err(datafusion::error::DataFusionError::Execution(
                    "similar_to precomputation thread panicked".to_string(),
                ))
            })
        })?;

        // 7. Score each row
        let mut builder = Float64Builder::with_capacity(num_rows);

        for row_idx in 0..num_rows {
            let mut scores = Vec::with_capacity(num_sources);
            // NULL propagates: if any source/query for this row is NULL, the
            // whole row's similarity is NULL — no scoring or fusion.
            let mut row_is_null = false;

            for (src_idx, mode) in scoring_modes.iter().enumerate() {
                // NULL propagates per-row: the column-level mode is decided from a
                // representative non-NULL row, but any individual row whose source
                // or query is NULL scores as NULL regardless of that mode.
                let row_source = columnar_value_to_value(&source_cvs[src_idx], batch, row_idx);
                let row_query = columnar_value_to_value(&query_cvs[src_idx], batch, row_idx);
                if matches!(mode, ScoringMode::Null)
                    || matches!(row_source, Value::Null)
                    || matches!(row_query, Value::Null)
                {
                    row_is_null = true;
                    break;
                }

                let score = match mode {
                    // Unreachable: a NULL column mode is handled by the per-row
                    // NULL check above, which breaks before this match.
                    ScoringMode::Null => {
                        row_is_null = true;
                        break;
                    }
                    ScoringMode::Vector(metric) => {
                        let sv = row_source;
                        let qv = row_query;
                        score_vectors_from_values(&sv, &qv, metric).map_err(|e| {
                            datafusion::error::DataFusionError::Execution(format!(
                                "similar_to vector: {}",
                                e
                            ))
                        })?
                    }
                    ScoringMode::AutoEmbed(metric) => {
                        let sv = row_source;
                        let embed_vec = precomputed.embed_vectors[src_idx]
                            .as_ref()
                            .expect("auto-embed should have been precomputed");
                        score_vectors_precomputed(&sv, embed_vec, metric).map_err(|e| {
                            datafusion::error::DataFusionError::Execution(format!(
                                "similar_to auto-embed: {}",
                                e
                            ))
                        })?
                    }
                    ScoringMode::Fts => {
                        let fts_map = precomputed.fts_results[src_idx]
                            .as_ref()
                            .expect("FTS should have been precomputed");
                        // Look up this row's VID in the FTS results
                        let vid = vid_col_idx.and_then(|idx| {
                            let col = batch.column(idx);
                            col.as_any()
                                .downcast_ref::<UInt64Array>()
                                .map(|u| Vid::from(u.value(row_idx)))
                        });
                        match vid {
                            Some(v) => fts_map.get(&v).copied().unwrap_or(0.0),
                            None => 0.0,
                        }
                    }
                };
                scores.push(score);
            }

            if row_is_null {
                builder.append_null();
                continue;
            }

            let fused = fuse_scores(&scores, &opts).map_err(|e| {
                datafusion::error::DataFusionError::Execution(format!("similar_to fusion: {}", e))
            })?;
            builder.append_value(fused as f64);
        }

        // Emit warning once per evaluate() if RRF was used in point context
        if opts.method == FusionMethod::Rrf && num_sources > 1 {
            self.graph_ctx.push_warning(QueryWarning::RrfPointContext);
        }

        Ok(ColumnarValue::Array(Arc::new(builder.finish())))
    }

    fn children(&self) -> Vec<&Arc<dyn PhysicalExpr>> {
        self.source_children
            .iter()
            .chain(&self.query_children)
            .chain(self.options_child.iter())
            .collect()
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn PhysicalExpr>>,
    ) -> datafusion::error::Result<Arc<dyn PhysicalExpr>> {
        let ns = self.source_children.len();
        let nq = self.query_children.len();
        let has_opts = self.options_child.is_some();
        let expected = ns + nq + if has_opts { 1 } else { 0 };
        if children.len() != expected {
            return Err(datafusion::error::DataFusionError::Plan(format!(
                "SimilarToExecExpr expected {} children, got {}",
                expected,
                children.len()
            )));
        }
        let source_children = children[..ns].to_vec();
        let query_children = children[ns..ns + nq].to_vec();
        let options_child = if has_opts {
            Some(children[ns + nq].clone())
        } else {
            None
        };
        Ok(Arc::new(SimilarToExecExpr::new(
            source_children,
            query_children,
            options_child,
            self.graph_ctx.clone(),
            self.source_variable.clone(),
            self.source_property_names.clone(),
            self.source_metrics.clone(),
        )))
    }

    fn fmt_sql(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self)
    }
}

// ---------------------------------------------------------------------------
// Scoring helpers
// ---------------------------------------------------------------------------

fn determine_scoring_mode(
    source: &Value,
    query: &Value,
    metric: Option<&DistanceMetric>,
) -> Result<ScoringMode, String> {
    // NULL propagates: a NULL source or query yields a NULL score, never an
    // error. Checked first so a NULL in the (mode-determining) first row does
    // not fall through to the `_ => Err(...)` arm and abort the whole batch.
    if matches!(source, Value::Null) || matches!(query, Value::Null) {
        return Ok(ScoringMode::Null);
    }
    let m = metric.cloned().unwrap_or(DistanceMetric::Cosine);
    match (source, query) {
        (Value::Vector(_) | Value::List(_), Value::Vector(_) | Value::List(_)) => {
            Ok(ScoringMode::Vector(m))
        }
        (Value::Vector(_) | Value::List(_), Value::String(_)) => Ok(ScoringMode::AutoEmbed(m)),
        (Value::String(_), Value::String(_)) => Ok(ScoringMode::Fts),
        (Value::String(_), Value::Vector(_) | Value::List(_)) => {
            Err("FTS source cannot be scored against a vector query".to_string())
        }
        _ => Err(format!(
            "unsupported source/query type combination: {:?} vs {:?}",
            std::mem::discriminant(source),
            std::mem::discriminant(query)
        )),
    }
}

/// Scores `source` against `query` row by row without leaving Arrow.
///
/// `None` unless both are `FixedSizeList<Float32>` of one width, which is what
/// two declared vector properties evaluate to; every other shape takes the
/// `Value` path. A row with a NULL on either side scores NULL. Each score is
/// [`score_vectors`] on the same numbers the `Value` path would hand it, so
/// the two paths return the same bits. With one source and no options there
/// is nothing to fuse, embed or look up.
fn score_fixed_size_lists(
    source: &ColumnarValue,
    query: &ColumnarValue,
    metric: Option<&DistanceMetric>,
    num_rows: usize,
) -> Option<datafusion::error::Result<arrow_array::ArrayRef>> {
    let (source, query) = (F32Rows::try_new(source)?, F32Rows::try_new(query)?);
    if source.width() != query.width() {
        return None;
    }
    // The default `determine_scoring_mode` applies.
    let metric = metric.cloned().unwrap_or(DistanceMetric::Cosine);
    let mut builder = Float64Builder::with_capacity(num_rows);
    for row in 0..num_rows {
        match (source.row(row), query.row(row)) {
            (Some(a), Some(b)) => match score_vectors(a, b, &metric) {
                Ok(score) => builder.append_value(score as f64),
                Err(e) => {
                    return Some(Err(datafusion::error::DataFusionError::Execution(format!(
                        "similar_to vector: {e}"
                    ))));
                }
            },
            _ => builder.append_null(),
        }
    }
    Some(Ok(Arc::new(builder.finish())))
}

fn score_vectors_from_values(
    source: &Value,
    query: &Value,
    metric: &DistanceMetric,
) -> Result<f32, String> {
    let v1 = value_to_f32_vec(source).map_err(|e| e.to_string())?;
    let v2 = value_to_f32_vec(query).map_err(|e| e.to_string())?;
    score_vectors(&v1, &v2, metric).map_err(|e| e.to_string())
}

fn score_vectors_precomputed(
    source: &Value,
    query_vec: &[f32],
    metric: &DistanceMetric,
) -> Result<f32, String> {
    let v1 = value_to_f32_vec(source).map_err(|e| e.to_string())?;
    score_vectors(&v1, query_vec, metric).map_err(|e| e.to_string())
}

// ---------------------------------------------------------------------------
// Async helpers (run inside std::thread::scope tokio runtime)
// ---------------------------------------------------------------------------

async fn auto_embed_query(
    graph_ctx: &GraphExecutionContext,
    label: Option<&str>,
    property: Option<&str>,
    query_text: &str,
) -> datafusion::error::Result<Vec<f32>> {
    let storage = graph_ctx.storage();
    let schema = storage.schema_manager().schema();

    // Try to find embedding config for the specific label.property
    let embedding_info = if let (Some(lbl), Some(prop)) = (label, property) {
        schema.vector_index_for_property(lbl, prop).and_then(|cfg| {
            cfg.embedding_config
                .as_ref()
                .map(|ec| (ec.alias.clone(), ec.query_prefix.clone()))
        })
    } else {
        None
    };

    // Fallback: find first vector index with embedding config
    let embedding_info = embedding_info.or_else(|| {
        schema.indexes.iter().find_map(|idx| {
            if let uni_common::core::schema::IndexDefinition::Vector(config) = idx {
                config
                    .embedding_config
                    .as_ref()
                    .map(|ec| (ec.alias.clone(), ec.query_prefix.clone()))
            } else {
                None
            }
        })
    });

    let (alias, query_prefix) = embedding_info.ok_or_else(|| {
        datafusion::error::DataFusionError::Execution(
            "similar_to: no vector index with embedding config found. \
             Cannot auto-embed text query."
                .to_string(),
        )
    })?;

    let runtime = graph_ctx.xervo_runtime().ok_or_else(|| {
        datafusion::error::DataFusionError::Execution(
            "similar_to: cannot auto-embed text — Uni-Xervo runtime not configured. \
             Provide a pre-computed vector instead."
                .to_string(),
        )
    })?;

    let embedder = runtime.embedding(&alias).await.map_err(|e| {
        datafusion::error::DataFusionError::Execution(format!(
            "similar_to: failed to get embedder: {}",
            e
        ))
    })?;

    let prefixed_query = match &query_prefix {
        Some(prefix) => format!("{prefix}{query_text}"),
        None => query_text.to_string(),
    };

    let embeddings = embedder
        .embed(&[prefixed_query.as_str()])
        .await
        .map_err(|e| {
            datafusion::error::DataFusionError::Execution(format!(
                "similar_to: embedding failed: {}",
                e
            ))
        })?
        .vectors;

    embeddings.into_iter().next().ok_or_else(|| {
        datafusion::error::DataFusionError::Execution(
            "similar_to: embedding service returned no results".to_string(),
        )
    })
}

async fn fts_search_batch(
    graph_ctx: &GraphExecutionContext,
    label: &str,
    property: &str,
    query_text: &str,
    fts_k: f32,
) -> datafusion::error::Result<HashMap<Vid, f32>> {
    let storage = graph_ctx.storage();
    let query_ctx = graph_ctx.query_context();
    let results = storage
        .fts_search(label, property, query_text, 1000, None, Some(&query_ctx))
        .await
        .map_err(|e| {
            datafusion::error::DataFusionError::Execution(format!(
                "similar_to: FTS search failed: {}",
                e
            ))
        })?;

    // Normalize BM25 scores and build VID lookup map
    Ok(results
        .into_iter()
        .map(|(vid, score)| (vid, normalize_bm25(score, fts_k)))
        .collect())
}

fn resolve_fts_label_property(
    graph_ctx: &GraphExecutionContext,
    label: Option<&str>,
    property: Option<&str>,
) -> datafusion::error::Result<(String, String)> {
    let lbl = label.unwrap_or("");
    let schema = graph_ctx.storage().schema_manager().schema();

    // If both label and property are provided, validate the FTS index exists
    if let (Some(l), Some(p)) = (label, property) {
        let has_fts = schema.indexes.iter().any(|idx| {
            matches!(idx, uni_common::core::schema::IndexDefinition::FullText(config)
                if config.label == l && config.properties.contains(&p.to_string()))
        });
        if has_fts {
            return Ok((l.to_string(), p.to_string()));
        }
        return Err(datafusion::error::DataFusionError::Execution(format!(
            "similar_to: no vector or full-text index found for property '{}.{}'. \
             Cannot compute text similarity without an appropriate index.",
            l, p
        )));
    }

    // Fallback: find any FTS property for the label
    find_fts_property_from_ctx(graph_ctx, lbl)
        .map(|prop| (lbl.to_string(), prop))
        .ok_or_else(|| {
            datafusion::error::DataFusionError::Execution(format!(
                "similar_to: no full-text index found for label '{}'. \
                 Cannot compute text similarity without an FTS index.",
                lbl
            ))
        })
}

fn find_fts_property_from_ctx(graph_ctx: &GraphExecutionContext, label: &str) -> Option<String> {
    let schema = graph_ctx.storage().schema_manager().schema();
    for idx in &schema.indexes {
        if let uni_common::core::schema::IndexDefinition::FullText(config) = idx
            && config.label == label
            && let Some(prop) = config.properties.first()
        {
            return Some(prop.clone());
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::types::Float32Type;
    use arrow_array::{ArrayRef, FixedSizeListArray, Float64Array};
    use datafusion::scalar::ScalarValue;

    const WIDTH: usize = 5;

    /// Rows of `WIDTH` components from a fixed sequence, with the cases the
    /// fast path must not get wrong: a NULL row, a zero vector, a NaN and an
    /// infinite component.
    fn rows(seed: u64, len: usize) -> Vec<Option<Vec<f32>>> {
        let mut state = seed;
        let mut next = || {
            state = state
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            ((state >> 33) as f32 / (1u64 << 31) as f32) * 2.0 - 1.0
        };
        (0..len)
            .map(|i| match i % 11 {
                3 => None,
                5 => Some(vec![0.0; WIDTH]),
                7 => Some(vec![f32::NAN, next(), next(), next(), next()]),
                9 => Some(vec![f32::INFINITY, next(), next(), next(), next()]),
                _ => Some((0..WIDTH).map(|_| next()).collect()),
            })
            .collect()
    }

    fn column(rows: &[Option<Vec<f32>>]) -> ArrayRef {
        Arc::new(
            FixedSizeListArray::from_iter_primitive::<Float32Type, _, _>(
                rows.iter().map(|r| {
                    r.as_ref()
                        .map(|v| v.iter().copied().map(Some).collect::<Vec<_>>())
                }),
                WIDTH as i32,
            ),
        )
    }

    fn scalar(row: Option<Vec<f32>>) -> ColumnarValue {
        let list = column(&[row]);
        ColumnarValue::Scalar(ScalarValue::FixedSizeList(Arc::new(
            list.as_any()
                .downcast_ref::<FixedSizeListArray>()
                .unwrap()
                .clone(),
        )))
    }

    /// What the `Value` path of `evaluate` returns for one row.
    fn by_value(
        source: &ColumnarValue,
        query: &ColumnarValue,
        metric: &DistanceMetric,
        row: usize,
    ) -> Option<f64> {
        let batch = arrow_array::RecordBatch::new_empty(Arc::new(Schema::empty()));
        let s = columnar_value_to_value(source, &batch, row);
        let q = columnar_value_to_value(query, &batch, row);
        if matches!(s, Value::Null) || matches!(q, Value::Null) {
            return None;
        }
        Some(score_vectors_from_values(&s, &q, metric).unwrap() as f64)
    }

    /// Asserts the fast path returns, bit for bit, what the `Value` path does.
    fn assert_same(source: &ColumnarValue, query: &ColumnarValue, len: usize, what: &str) {
        for metric in [
            DistanceMetric::Cosine,
            DistanceMetric::Dot,
            DistanceMetric::L2,
            DistanceMetric::L1,
        ] {
            let fast = score_fixed_size_lists(source, query, Some(&metric), len)
                .expect("two FixedSizeList<Float32> values take the fast path")
                .unwrap();
            let fast = fast.as_any().downcast_ref::<Float64Array>().unwrap();
            assert_eq!(fast.len(), len);
            for row in 0..len {
                let got = fast.is_valid(row).then(|| fast.value(row).to_bits());
                let want = by_value(source, query, &metric, row).map(f64::to_bits);
                assert_eq!(got, want, "{what}, {metric:?}, row {row}");
            }
        }
    }

    #[test]
    fn two_columns_score_as_the_value_path_does() {
        let (a, b) = (rows(1, 64), rows(2, 64));
        // Offset `b`'s special rows from `a`'s, so each meets an ordinary row.
        let b: Vec<_> = b.into_iter().cycle().skip(1).take(64).collect();
        assert_same(
            &ColumnarValue::Array(column(&a)),
            &ColumnarValue::Array(column(&b)),
            64,
            "array, array",
        );
    }

    #[test]
    fn a_sliced_column_scores_its_own_rows() {
        let (a, b) = (column(&rows(3, 64)), column(&rows(4, 64)));
        assert_same(
            &ColumnarValue::Array(a.slice(13, 40)),
            &ColumnarValue::Array(b.slice(7, 40)),
            40,
            "sliced arrays",
        );
    }

    #[test]
    fn a_scalar_on_either_side_scores_against_every_row() {
        let a = ColumnarValue::Array(column(&rows(5, 33)));
        for (what, one) in [
            ("a vector", Some(vec![0.25, -0.5, 1.0, 0.0, 2.0])),
            ("a zero vector", Some(vec![0.0; WIDTH])),
            ("NULL", None),
        ] {
            assert_same(
                &a,
                &scalar(one.clone()),
                33,
                &format!("array, scalar {what}"),
            );
            assert_same(&scalar(one), &a, 33, &format!("scalar {what}, array"));
        }
    }

    #[test]
    fn no_metric_means_cosine() {
        let a = ColumnarValue::Array(column(&rows(6, 16)));
        let b = ColumnarValue::Array(column(&rows(7, 16)));
        let unset = score_fixed_size_lists(&a, &b, None, 16).unwrap().unwrap();
        let cosine = score_fixed_size_lists(&a, &b, Some(&DistanceMetric::Cosine), 16)
            .unwrap()
            .unwrap();
        assert_eq!(unset.to_data(), cosine.to_data());
    }

    #[test]
    fn other_shapes_keep_the_value_path() {
        let vectors = ColumnarValue::Array(column(&rows(8, 4)));
        // A literal or parameter query vector: a `List<Float64>` scalar.
        let list = ColumnarValue::Scalar(ScalarValue::List(ScalarValue::new_list(
            &vec![ScalarValue::Float64(Some(1.0)); WIDTH],
            &DataType::Float64,
            true,
        )));
        assert!(score_fixed_size_lists(&vectors, &list, None, 4).is_none());
        // Another width: the `Value` path reports the mismatch.
        let narrow: ArrayRef = Arc::new(
            FixedSizeListArray::from_iter_primitive::<Float32Type, _, _>(
                vec![Some(vec![Some(1.0), Some(0.0)]); 4],
                2,
            ),
        );
        assert!(score_fixed_size_lists(&vectors, &ColumnarValue::Array(narrow), None, 4).is_none());
    }
}
