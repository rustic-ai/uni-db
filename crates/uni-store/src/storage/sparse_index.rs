// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team
// Rust guideline compliant

//! Scored sparse-vector (SPLADE / learned-sparse) inverted index.
//!
//! Forked from [`super::inverted_index`], but with three differences that make
//! it a *scored* index rather than a set-membership one:
//!
//! 1. Postings are `term_id (u32) -> [(vid, weight)]` instead of
//!    `term (String) -> [vid]`. Scoring is a dot product over shared terms.
//! 2. The on-disk schema carries per-term `weights` and a `max_impact` upper
//!    bound (the prerequisite for P2 block-max pruning).
//! 3. [`SparseVectorIndex::query_topk`] returns scored, ranked results via a
//!    dot-product accumulator + bounded min-heap, not an unordered VID set.
//!
//! Weights are stored as 8-bit per-term-quantized codes by default (config
//! `quantize`, ≈ lossless and ~4× smaller); `quantize = false` stores lossless
//! `f32` instead. Both encodings are read transparently by the same reader (the
//! `weights` list element type is the discriminator), which also makes legacy
//! `f32`-only segments forward-compatible without a rebuild. MVCC / tombstone
//! correctness is applied by the *query orchestration* layer (uni-query),
//! exactly as the dense `vector_search` path does — this module is the storage
//! kernel.

use crate::backend::types::{FilterExpr, Scalar};
use anyhow::{Result, anyhow};
use arrow_array::types::{Float32Type, UInt8Type, UInt64Type};
use arrow_array::{
    Array, Float32Array, ListArray, RecordBatch, StructArray, UInt8Array, UInt32Array, UInt64Array,
};
use arrow_schema::{DataType, Field, Schema as ArrowSchema};
use futures::TryStreamExt;
use lance::Dataset;
use std::cmp::Reverse;
use std::collections::{BinaryHeap, HashMap, HashSet};
use std::sync::Arc;
use tracing::{debug, info, instrument};
use uni_common::core::id::Vid;
use uni_common::core::schema::SparseVectorIndexConfig;

/// Default memory limit for postings accumulation (256 MB), matching the
/// set-membership inverted index.
const DEFAULT_MAX_POSTINGS_MEMORY: usize = 256 * 1024 * 1024;

/// One term's postings: parallel vid + weight vectors.
type Postings = HashMap<u32, Vec<(u64, f32)>>;

/// Estimate memory usage of a postings map (term key + parallel vid/weight pairs).
fn estimated_postings_memory(postings: &Postings) -> usize {
    postings
        .values()
        .map(|v| std::mem::size_of::<u32>() + std::mem::size_of::<Vec<(u64, f32)>>() + v.len() * 12)
        .sum()
}

/// Merge multiple postings segments into one (concatenates per-term lists).
fn merge_postings_segments(segments: Vec<Postings>) -> Postings {
    let mut merged: Postings = HashMap::new();
    for segment in segments {
        for (term, entries) in segment {
            merged.entry(term).or_default().extend(entries);
        }
    }
    merged
}

/// Read a sparse vector `(indices, values)` from row `row` of a `Struct
/// { indices: List<UInt32>, values: List<Float32> }` column. Returns `None` if
/// the struct row is null (deleted / absent).
fn read_sparse_row(struct_arr: &StructArray, row: usize) -> Option<(Vec<u32>, Vec<f32>)> {
    if struct_arr.is_null(row) {
        return None;
    }
    let indices_list = struct_arr
        .column_by_name("indices")?
        .as_any()
        .downcast_ref::<ListArray>()?;
    let values_list = struct_arr
        .column_by_name("values")?
        .as_any()
        .downcast_ref::<ListArray>()?;
    let idx_vals = indices_list.value(row);
    let idx_arr = idx_vals.as_any().downcast_ref::<UInt32Array>()?;
    let w_vals = values_list.value(row);
    let w_arr = w_vals.as_any().downcast_ref::<Float32Array>()?;
    let indices = (0..idx_arr.len()).map(|i| idx_arr.value(i)).collect();
    let values = (0..w_arr.len()).map(|i| w_arr.value(i)).collect();
    Some((indices, values))
}

/// Number of 8-bit quantization levels above zero.
///
/// Learned-sparse / SPLADE weights are non-negative (ReLU), so the full
/// unsigned `0..=255` range is used: the per-term scale is `max_weight / 255`,
/// giving twice the resolution of a signed `i8` scheme for the same width.
const QUANT_LEVELS: f32 = 255.0;

/// Quantize one term's weights to 8-bit codes with a shared per-term scale.
///
/// Returns `(codes, scale, max_impact)` where `code as f32 * scale` reconstructs
/// the weight and `max_impact` is computed from the *dequantized* weights, so it
/// stays a valid upper bound on the values scoring actually multiplies — the
/// invariant any future block-max pruning depends on.
///
/// Weights are clamped to `[0, max_weight]`; learned-sparse weights are
/// non-negative, and a stray negative has no 8-bit code (it maps to zero).
fn quantize_term(weights: &[f32]) -> (Vec<u8>, f32, f32) {
    let max_weight = weights.iter().copied().fold(0.0f32, f32::max);
    // All-zero (or all-negative) term: scale 0, every code 0. Guards the
    // `w / scale` division against `0 / 0 = NaN`.
    if max_weight <= 0.0 {
        return (vec![0u8; weights.len()], 0.0, 0.0);
    }
    let scale = max_weight / QUANT_LEVELS;
    let codes: Vec<u8> = weights
        .iter()
        .map(|&w| {
            // Round to nearest (never truncate: truncation biases every weight
            // down and would let a dequantized value exceed `max_impact`). The
            // `as u8` cast saturates, so fp drift at the top of the range is safe.
            (w.clamp(0.0, max_weight) / scale).round() as u8
        })
        .collect();
    let max_code = codes.iter().copied().max().unwrap_or(0);
    (codes, scale, dequantize(max_code, scale))
}

/// Reconstruct an approximate weight from an 8-bit code and its term scale.
fn dequantize(code: u8, scale: f32) -> f32 {
    f32::from(code) * scale
}

/// A borrowed view over one term's posting weights that yields `f32` regardless
/// of on-disk encoding: quantized (`UInt8` codes + a per-term scale) or lossless
/// (`Float32` — legacy segments and `quantize = false`).
enum TermWeights<'a> {
    Quantized { codes: &'a UInt8Array, scale: f32 },
    Lossless(&'a Float32Array),
}

impl TermWeights<'_> {
    /// Weight at posting position `j` (`0.0` for a null element).
    fn get(&self, j: usize) -> f32 {
        match self {
            Self::Quantized { codes, scale } => {
                if codes.is_null(j) {
                    0.0
                } else {
                    dequantize(codes.value(j), *scale)
                }
            }
            Self::Lossless(arr) => {
                if arr.is_null(j) {
                    0.0
                } else {
                    arr.value(j)
                }
            }
        }
    }
}

/// Build a [`TermWeights`] view over one posting row's `weights` element array.
///
/// `row_scale` is the row's `weight_scale` value, required when the elements are
/// quantized `UInt8` codes and absent for lossless `Float32` segments.
///
/// # Errors
/// Returns an error if the element type is neither `UInt8` nor `Float32`, or if
/// quantized codes arrive without a `weight_scale`.
fn term_weights(weights_arr: &dyn Array, row_scale: Option<f32>) -> Result<TermWeights<'_>> {
    if let Some(codes) = weights_arr.as_any().downcast_ref::<UInt8Array>() {
        let scale = row_scale
            .ok_or_else(|| anyhow!("Quantized sparse weights missing weight_scale column"))?;
        Ok(TermWeights::Quantized { codes, scale })
    } else if let Some(arr) = weights_arr.as_any().downcast_ref::<Float32Array>() {
        Ok(TermWeights::Lossless(arr))
    } else {
        Err(anyhow!(
            "Invalid inner weights type: {:?}",
            weights_arr.data_type()
        ))
    }
}

/// Read the optional per-term `weight_scale` column from a postings batch.
///
/// Present only for quantized segments; absent for lossless / legacy ones.
fn weight_scale_column(batch: &RecordBatch) -> Option<&Float32Array> {
    batch
        .column_by_name("weight_scale")
        .and_then(|c| c.as_any().downcast_ref::<Float32Array>())
}

/// Scored sparse-vector inverted index over a `DataType::SparseVector` column.
pub struct SparseVectorIndex {
    dataset: Option<Dataset>,
    base_uri: String,
    label: String,
    property: String,
    config: SparseVectorIndexConfig,
}

impl std::fmt::Debug for SparseVectorIndex {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SparseVectorIndex")
            .field("base_uri", &self.base_uri)
            .field("label", &self.label)
            .field("property", &self.property)
            .field("initialized", &self.dataset.is_some())
            .finish_non_exhaustive()
    }
}

impl SparseVectorIndex {
    /// The on-disk dataset path for this index's postings.
    fn postings_path(base_uri: &str, label: &str, property: &str) -> String {
        format!("{base_uri}/indexes/{label}/{property}_sparse")
    }

    /// Open or initialize a sparse index at `base_uri` for the given config.
    pub async fn new(base_uri: &str, config: SparseVectorIndexConfig) -> Result<Self> {
        let path = Self::postings_path(base_uri, &config.label, &config.property);
        let dataset = (Dataset::open(&path).await).ok();
        Ok(Self {
            dataset,
            base_uri: base_uri.to_string(),
            label: config.label.clone(),
            property: config.property.clone(),
            config,
        })
    }

    /// Accumulate one record batch's sparse rows into `postings`. Returns the
    /// count of documents (rows carrying a non-null sparse value) processed.
    /// Deleted rows store a null struct (see `build_sparse_vector_column`), so
    /// `read_sparse_row` skips them and they never enter the postings.
    fn accumulate_batch(&self, batch: &RecordBatch, postings: &mut Postings) -> Result<usize> {
        let vid_col = batch
            .column_by_name("_vid")
            .ok_or_else(|| anyhow!("Missing _vid"))?
            .as_any()
            .downcast_ref::<UInt64Array>()
            .ok_or_else(|| anyhow!("Invalid _vid type"))?;
        let term_col = batch
            .column_by_name(&self.property)
            .ok_or_else(|| anyhow!("Missing property {}", self.property))?;
        let struct_arr = term_col
            .as_any()
            .downcast_ref::<StructArray>()
            .ok_or_else(|| {
                anyhow!(
                    "Property {} must be a sparse-vector struct, got {:?}",
                    self.property,
                    term_col.data_type()
                )
            })?;
        let mut count = 0;
        for i in 0..batch.num_rows() {
            let vid = vid_col.value(i);
            let Some((indices, values)) = read_sparse_row(struct_arr, i) else {
                continue;
            };
            for (term, weight) in indices.into_iter().zip(values) {
                // Defense in depth: ingest validation now rejects non-finite weights, but
                // a corrupt / manually-spliced on-disk segment must not poison scoring —
                // a single NaN weight would make a vid's accumulated dot product NaN and
                // corrupt top-k ordering (issue #95). Skip non-finite postings on read.
                if !weight.is_finite() {
                    continue;
                }
                postings.entry(term).or_default().push((vid, weight));
            }
            count += 1;
        }
        Ok(count)
    }

    /// Merge accumulated postings (+ any spilled segments) and write to disk.
    async fn finish_build(
        &mut self,
        postings: Postings,
        mut temp_segments: Vec<Postings>,
    ) -> Result<()> {
        if temp_segments.is_empty() {
            self.write_postings(postings).await
        } else {
            temp_segments.push(postings);
            info!(
                segments = temp_segments.len(),
                "Merging sparse postings segments"
            );
            let merged = merge_postings_segments(temp_segments);
            self.write_postings(merged).await
        }
    }

    /// Rebuild the index from already-scanned record batches (the storage
    /// backend's view of the flushed vertex table). This is the canonical
    /// backfill path: the LanceDB-managed table is opened by the backend, not
    /// by a raw `lance::Dataset::open` (whose physical path differs). Uses
    /// segmented accumulation to stay within the memory limit.
    pub async fn build_from_batches(
        &mut self,
        batches: &[RecordBatch],
        progress: impl Fn(usize),
    ) -> Result<()> {
        let mut postings: Postings = HashMap::new();
        let mut temp_segments: Vec<Postings> = Vec::new();
        let mut count = 0;
        for batch in batches {
            count += self.accumulate_batch(batch, &mut postings)?;
            progress(count);
            if estimated_postings_memory(&postings) > DEFAULT_MAX_POSTINGS_MEMORY {
                temp_segments.push(std::mem::take(&mut postings));
            }
        }
        self.finish_build(postings, temp_segments).await
    }

    /// Overwrite the on-disk postings with the provided map.
    ///
    /// Schema (quantized, default): `(term_id: UInt32, vids: List<UInt64>,
    /// weights: List<UInt8>, max_impact: Float32, weight_scale: Float32)`.
    /// Lossless (`quantize = false`): `weights` is `List<Float32>` and the
    /// `weight_scale` column is omitted. `max_impact` is the per-term maximum
    /// (dequantized) weight — the upper bound P2 block-max pruning consumes.
    async fn write_postings(&mut self, postings: Postings) -> Result<()> {
        let quantize = self.config.quantize;
        let n = postings.len();
        let mut term_ids = Vec::with_capacity(n);
        let mut vid_lists: Vec<Option<Vec<Option<u64>>>> = Vec::with_capacity(n);
        let mut max_impacts = Vec::with_capacity(n);
        // Exactly one weights representation is populated, per `quantize`.
        let mut q_weight_lists: Vec<Option<Vec<Option<u8>>>> = Vec::new();
        let mut q_scales: Vec<f32> = Vec::new();
        let mut f_weight_lists: Vec<Option<Vec<Option<f32>>>> = Vec::new();

        for (term, entries) in postings {
            let mut vids = Vec::with_capacity(entries.len());
            let mut weights = Vec::with_capacity(entries.len());
            for (vid, weight) in entries {
                vids.push(Some(vid));
                weights.push(weight);
            }
            term_ids.push(term);
            vid_lists.push(Some(vids));

            if quantize {
                let (codes, scale, max_impact) = quantize_term(&weights);
                q_weight_lists.push(Some(codes.into_iter().map(Some).collect()));
                q_scales.push(scale);
                max_impacts.push(max_impact);
            } else {
                // True maximum weight (the P2 block-max upper bound). Start at
                // NEG_INFINITY so an all-negative-weight term records its real
                // max rather than a spurious 0.0; empty terms fall back to 0.0.
                let mut max_impact = f32::NEG_INFINITY;
                for &w in &weights {
                    if w > max_impact {
                        max_impact = w;
                    }
                }
                if !max_impact.is_finite() {
                    max_impact = 0.0;
                }
                max_impacts.push(max_impact);
                f_weight_lists.push(Some(weights.into_iter().map(Some).collect()));
            }
        }

        let term_array = UInt32Array::from(term_ids);
        let vid_list_array = ListArray::from_iter_primitive::<UInt64Type, _, _>(vid_lists);
        let max_impact_array = Float32Array::from(max_impacts);

        let mut columns: Vec<(&str, Arc<dyn Array>)> = vec![
            ("term_id", Arc::new(term_array) as Arc<dyn Array>),
            ("vids", Arc::new(vid_list_array) as Arc<dyn Array>),
        ];
        if quantize {
            let weight_list_array =
                ListArray::from_iter_primitive::<UInt8Type, _, _>(q_weight_lists);
            columns.push(("weights", Arc::new(weight_list_array) as Arc<dyn Array>));
            columns.push(("max_impact", Arc::new(max_impact_array) as Arc<dyn Array>));
            columns.push((
                "weight_scale",
                Arc::new(Float32Array::from(q_scales)) as Arc<dyn Array>,
            ));
        } else {
            let weight_list_array =
                ListArray::from_iter_primitive::<Float32Type, _, _>(f_weight_lists);
            columns.push(("weights", Arc::new(weight_list_array) as Arc<dyn Array>));
            columns.push(("max_impact", Arc::new(max_impact_array) as Arc<dyn Array>));
        }

        let batch = arrow_array::RecordBatch::try_from_iter(columns)?;

        let path = Self::postings_path(&self.base_uri, &self.label, &self.property);
        let write_params = lance::dataset::WriteParams {
            mode: lance::dataset::WriteMode::Overwrite,
            ..Default::default()
        };
        // Retried: Lance reports a losing commit as retryable, and this
        // writer used to propagate it to the caller.
        let ds = crate::storage::manager::write_dataset_with_lance_conflict_retry(
            &path,
            Self::postings_schema(quantize),
            vec![batch],
            write_params.mode,
        )
        .await?;
        self.dataset = Some(ds);
        Ok(())
    }

    /// Arrow schema of the postings dataset for the given `quantize` mode.
    fn postings_schema(quantize: bool) -> Arc<ArrowSchema> {
        let weights_item = if quantize {
            DataType::UInt8
        } else {
            DataType::Float32
        };
        let mut fields = vec![
            Field::new("term_id", DataType::UInt32, false),
            Field::new(
                "vids",
                DataType::List(Arc::new(Field::new("item", DataType::UInt64, true))),
                false,
            ),
            Field::new(
                "weights",
                DataType::List(Arc::new(Field::new("item", weights_item, true))),
                false,
            ),
            Field::new("max_impact", DataType::Float32, false),
        ];
        if quantize {
            fields.push(Field::new("weight_scale", DataType::Float32, false));
        }
        Arc::new(ArrowSchema::new(fields))
    }

    /// Score the corpus against `query` (`[(term_id, weight)]`) by dot product
    /// and return the top `k` `(Vid, score)` pairs, highest score first.
    ///
    /// P1 brute-force document-at-a-time: filter postings to the query terms,
    /// accumulate per-vid dot products, then drain a bounded min-heap. No
    /// MVCC/tombstone filtering happens here — the orchestration layer applies
    /// it, mirroring the dense `vector_search` path.
    pub async fn query_topk(&self, query: &[(u32, f32)], k: usize) -> Result<Vec<(Vid, f32)>> {
        let Some(ds) = &self.dataset else {
            debug!("Sparse index not initialized, returning empty result");
            return Ok(Vec::new());
        };
        if query.is_empty() || k == 0 {
            return Ok(Vec::new());
        }

        let query_weights: HashMap<u32, f32> = query.iter().copied().collect();
        let filter = FilterExpr::one_of(
            "term_id",
            query_weights.keys().map(|t| Scalar::UInt(u64::from(*t))),
        )
        .to_sql()?;

        let mut scanner = ds.scan();
        scanner.filter(&filter)?;
        let mut stream = scanner.try_into_stream().await?;

        let mut scores: HashMap<u64, f32> = HashMap::new();
        while let Some(batch) = stream.try_next().await? {
            let term_col = batch
                .column_by_name("term_id")
                .ok_or_else(|| anyhow!("Missing term_id column"))?
                .as_any()
                .downcast_ref::<UInt32Array>()
                .ok_or_else(|| anyhow!("Invalid term_id column"))?;
            let vids_col = batch
                .column_by_name("vids")
                .ok_or_else(|| anyhow!("Missing vids column"))?
                .as_any()
                .downcast_ref::<ListArray>()
                .ok_or_else(|| anyhow!("Invalid vids column"))?;
            let weights_col = batch
                .column_by_name("weights")
                .ok_or_else(|| anyhow!("Missing weights column"))?
                .as_any()
                .downcast_ref::<ListArray>()
                .ok_or_else(|| anyhow!("Invalid weights column"))?;
            let weight_scale_col = weight_scale_column(&batch);

            for i in 0..batch.num_rows() {
                let term = term_col.value(i);
                let Some(&qw) = query_weights.get(&term) else {
                    continue;
                };
                if vids_col.is_null(i) || weights_col.is_null(i) {
                    continue;
                }
                let vids_arr = vids_col.value(i);
                let vids = vids_arr
                    .as_any()
                    .downcast_ref::<UInt64Array>()
                    .ok_or_else(|| anyhow!("Invalid inner vids type"))?;
                let weights_arr = weights_col.value(i);
                let weights =
                    term_weights(weights_arr.as_ref(), weight_scale_col.map(|c| c.value(i)))?;

                for j in 0..vids.len() {
                    if vids.is_null(j) {
                        continue;
                    }
                    *scores.entry(vids.value(j)).or_insert(0.0) += qw * weights.get(j);
                }
            }
        }

        Ok(Self::top_k_from_scores(scores, k))
    }

    /// Drain a score map into the top-`k` `(Vid, score)` pairs (descending),
    /// using a bounded min-heap so memory stays O(k).
    fn top_k_from_scores(scores: HashMap<u64, f32>, k: usize) -> Vec<(Vid, f32)> {
        // Min-heap keyed on score (via OrderedF32 + Reverse) capped at k.
        let mut heap: BinaryHeap<Reverse<HeapEntry>> = BinaryHeap::with_capacity(k + 1);
        for (vid, score) in scores {
            heap.push(Reverse(HeapEntry { score, vid }));
            if heap.len() > k {
                heap.pop();
            }
        }
        let mut out: Vec<(Vid, f32)> = heap
            .into_iter()
            .map(|Reverse(e)| (Vid::from(e.vid), e.score))
            .collect();
        out.sort_by(|a, b| {
            b.1.partial_cmp(&a.1)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then(a.0.as_u64().cmp(&b.0.as_u64()))
        });
        out
    }

    /// Load all postings from disk into a memory map. Empty if no dataset yet.
    #[instrument(skip(self), level = "debug")]
    async fn load_postings(&self) -> Result<Postings> {
        let Some(ds) = &self.dataset else {
            return Ok(HashMap::new());
        };
        let mut postings: Postings = HashMap::new();
        let scanner = ds.scan();
        let mut stream = scanner.try_into_stream().await?;
        while let Some(batch) = stream.try_next().await? {
            let term_col = batch
                .column_by_name("term_id")
                .ok_or_else(|| anyhow!("Missing term_id column"))?
                .as_any()
                .downcast_ref::<UInt32Array>()
                .ok_or_else(|| anyhow!("Invalid term_id column"))?;
            let vids_col = batch
                .column_by_name("vids")
                .ok_or_else(|| anyhow!("Missing vids column"))?
                .as_any()
                .downcast_ref::<ListArray>()
                .ok_or_else(|| anyhow!("Invalid vids column"))?;
            let weights_col = batch
                .column_by_name("weights")
                .ok_or_else(|| anyhow!("Missing weights column"))?
                .as_any()
                .downcast_ref::<ListArray>()
                .ok_or_else(|| anyhow!("Invalid weights column"))?;
            let weight_scale_col = weight_scale_column(&batch);

            for i in 0..batch.num_rows() {
                if vids_col.is_null(i) || weights_col.is_null(i) {
                    continue;
                }
                let term = term_col.value(i);
                let vids_arr = vids_col.value(i);
                let vids = vids_arr
                    .as_any()
                    .downcast_ref::<UInt64Array>()
                    .ok_or_else(|| anyhow!("Invalid inner vids type"))?;
                let weights_arr = weights_col.value(i);
                // Dequantizes into f32 so the load-modify-write update path stays
                // in f32 space and re-quantizes on the next `write_postings`.
                let weights =
                    term_weights(weights_arr.as_ref(), weight_scale_col.map(|c| c.value(i)))?;
                let entry = postings.entry(term).or_default();
                for j in 0..vids.len() {
                    if !vids.is_null(j) {
                        entry.push((vids.value(j), weights.get(j)));
                    }
                }
            }
        }
        Ok(postings)
    }

    /// Apply incremental updates: drop removed VIDs from every posting, then add
    /// the new vertices' `(term, weight)` pairs, and rewrite. Mirrors the
    /// set-membership inverted index's load-modify-write semantics.
    #[instrument(skip(self, added, removed), level = "info", fields(
        label = %self.label,
        property = %self.property,
        added_count = added.len(),
        removed_count = removed.len()
    ))]
    pub async fn apply_incremental_updates(
        &mut self,
        added: &HashMap<Vid, Vec<(u32, f32)>>,
        removed: &HashSet<Vid>,
    ) -> Result<()> {
        let mut postings = self.load_postings().await?;

        if !removed.is_empty() {
            let removed_u64: HashSet<u64> = removed.iter().map(|v| v.as_u64()).collect();
            for entries in postings.values_mut() {
                entries.retain(|(vid, _)| !removed_u64.contains(vid));
            }
            postings.retain(|_, entries| !entries.is_empty());
        }

        for (vid, terms) in added {
            let vid_u64 = vid.as_u64();
            for &(term, weight) in terms {
                postings.entry(term).or_default().push((vid_u64, weight));
            }
        }

        self.write_postings(postings).await?;
        Ok(())
    }

    /// Returns true if the index dataset exists.
    pub fn is_initialized(&self) -> bool {
        self.dataset.is_some()
    }

    /// Returns the property name this index is built on.
    pub fn property(&self) -> &str {
        &self.property
    }
}

/// Heap entry ordered by score (NaN treated as smallest), tie-broken by vid.
struct HeapEntry {
    score: f32,
    vid: u64,
}

impl PartialEq for HeapEntry {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == std::cmp::Ordering::Equal
    }
}
impl Eq for HeapEntry {}
impl PartialOrd for HeapEntry {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for HeapEntry {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.score
            .partial_cmp(&other.score)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then(self.vid.cmp(&other.vid))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_merge_postings_segments_overlapping() {
        let seg1: Postings = [(1u32, vec![(10u64, 1.0f32)]), (2, vec![(11, 2.0)])]
            .into_iter()
            .collect();
        let seg2: Postings = [(1u32, vec![(12u64, 3.0f32)]), (3, vec![(13, 4.0)])]
            .into_iter()
            .collect();
        let merged = merge_postings_segments(vec![seg1, seg2]);
        assert_eq!(merged.get(&1).unwrap().len(), 2);
        assert_eq!(merged.get(&2).unwrap(), &vec![(11, 2.0)]);
        assert_eq!(merged.get(&3).unwrap(), &vec![(13, 4.0)]);
    }

    #[test]
    fn test_top_k_from_scores_orders_desc_and_caps() {
        let scores: HashMap<u64, f32> = [(1u64, 0.5f32), (2, 3.0), (3, 1.0), (4, 2.0)]
            .into_iter()
            .collect();
        let top = SparseVectorIndex::top_k_from_scores(scores, 2);
        assert_eq!(top.len(), 2);
        assert_eq!(top[0].0.as_u64(), 2);
        assert_eq!(top[0].1, 3.0);
        assert_eq!(top[1].0.as_u64(), 4);
        assert_eq!(top[1].1, 2.0);
    }

    #[test]
    fn test_top_k_tie_break_by_vid() {
        let scores: HashMap<u64, f32> = [(7u64, 1.0f32), (3, 1.0)].into_iter().collect();
        let top = SparseVectorIndex::top_k_from_scores(scores, 2);
        // Equal scores → lower vid first (deterministic).
        assert_eq!(top[0].0.as_u64(), 3);
        assert_eq!(top[1].0.as_u64(), 7);
    }

    #[test]
    fn test_top_k_empty() {
        assert!(SparseVectorIndex::top_k_from_scores(HashMap::new(), 5).is_empty());
    }

    #[test]
    fn test_quantize_all_zero_term_no_nan() {
        let (codes, scale, max_impact) = quantize_term(&[0.0, 0.0, 0.0]);
        assert_eq!(codes, vec![0, 0, 0]);
        assert_eq!(scale, 0.0);
        assert_eq!(max_impact, 0.0);
        assert!(!scale.is_nan() && !max_impact.is_nan());
    }

    #[test]
    fn test_quantize_negative_weights_clamp_to_zero() {
        // Learned-sparse weights are non-negative; a stray negative has no code.
        let (codes, _scale, max_impact) = quantize_term(&[-1.0, -0.5]);
        assert_eq!(codes, vec![0, 0]);
        assert_eq!(max_impact, 0.0);
    }

    #[test]
    fn test_quantize_max_weight_maps_to_top_code() {
        let (codes, scale, max_impact) = quantize_term(&[0.1, 2.0, 1.0]);
        // The maximum weight quantizes to the top code (255).
        assert_eq!(codes[1], 255);
        // max_impact is the dequantized top code and bounds every dequantized
        // weight (the rank-safety invariant for future block-max pruning).
        for (j, &w) in [0.1f32, 2.0, 1.0].iter().enumerate() {
            assert!(dequantize(codes[j], scale) <= max_impact + f32::EPSILON);
            // Round-trip error is bounded by half a quantization step.
            assert!((dequantize(codes[j], scale) - w).abs() <= scale / 2.0 + 1e-6);
        }
    }

    proptest::proptest! {
        #[test]
        fn prop_quantize_roundtrip_and_bound(
            weights in proptest::collection::vec(0.0f32..1000.0, 1..64)
        ) {
            let (codes, scale, max_impact) = quantize_term(&weights);
            proptest::prop_assert_eq!(codes.len(), weights.len());
            for (j, &w) in weights.iter().enumerate() {
                let dq = dequantize(codes[j], scale);
                // max_impact upper-bounds every dequantized weight.
                proptest::prop_assert!(dq <= max_impact + 1e-4);
                // Reconstruction is within half a step of the original.
                proptest::prop_assert!((dq - w).abs() <= scale / 2.0 + 1e-3);
                proptest::prop_assert!(dq.is_finite());
            }
        }
    }
}
