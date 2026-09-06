// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team
// Rust guideline compliant

//! Inverted index implementation for set membership queries.
//!
//! Provides efficient `ANY(x IN list WHERE x IN allowed)` queries by
//! maintaining a term-to-VID mapping. Supports both full rebuilds and
//! incremental updates for optimal performance during mutations.

use crate::backend::types::{FilterExpr, Scalar};
use anyhow::{Result, anyhow};
use arrow_array::types::UInt64Type;
use arrow_array::{Array, ListArray, RecordBatch, StringArray, UInt64Array};
use arrow_schema::{DataType, Field, Schema as ArrowSchema};
use futures::TryStreamExt;
use lance::Dataset;
use serde_json::Value;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tracing::{debug, info, instrument};
use uni_common::core::id::Vid;
use uni_common::core::schema::InvertedIndexConfig;

/// Default memory limit for postings accumulation (256 MB)
const DEFAULT_MAX_POSTINGS_MEMORY: usize = 256 * 1024 * 1024;

/// Estimate memory usage of a postings map
fn estimated_postings_memory(postings: &HashMap<String, Vec<u64>>) -> usize {
    postings
        .iter()
        .map(|(k, v)| k.len() + std::mem::size_of::<Vec<u64>>() + v.len() * 8)
        .sum()
}

/// Merge multiple postings segments into one
fn merge_postings_segments(segments: Vec<HashMap<String, Vec<u64>>>) -> HashMap<String, Vec<u64>> {
    let mut merged: HashMap<String, Vec<u64>> = HashMap::new();
    for segment in segments {
        for (term, vids) in segment {
            merged.entry(term).or_default().extend(vids);
        }
    }
    merged
}

/// Term-to-VID inverted index for efficient set-membership queries.
///
/// Supports both full rebuilds from a vertex dataset and incremental updates
/// for optimal performance during small mutations.
pub struct InvertedIndex {
    dataset: Option<Dataset>,
    base_uri: String,
    label: String,
    property: String,
    config: InvertedIndexConfig,
}

impl std::fmt::Debug for InvertedIndex {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("InvertedIndex")
            .field("base_uri", &self.base_uri)
            .field("label", &self.label)
            .field("property", &self.property)
            .field("initialized", &self.dataset.is_some())
            .finish_non_exhaustive()
    }
}

impl InvertedIndex {
    /// Open or initialize an inverted index at `base_uri` for the given config.
    pub async fn new(base_uri: &str, config: InvertedIndexConfig) -> Result<Self> {
        let path = format!(
            "{}/indexes/{}/{}_inverted",
            base_uri, config.label, config.property
        );

        // #233 Tier 1: `.ok()` mapped every open failure — permissions, a
        // corrupt manifest, IO — to "index not initialized", which a full-text
        // search reports as zero hits. Only a genuinely absent dataset means
        // the index has not been built yet. Same discrimination as
        // `storage/index.rs::get_vid`.
        let dataset = match Dataset::open(&path).await {
            Ok(ds) => Some(ds),
            Err(e) => {
                let err = anyhow::Error::from(e);
                if crate::store_utils::is_dataset_not_found(&err) {
                    None
                } else {
                    return Err(err);
                }
            }
        };

        Ok(Self {
            dataset,
            base_uri: base_uri.to_string(),
            label: config.label.clone(),
            property: config.property.clone(),
            config,
        })
    }

    /// Accumulate one record batch's terms into `postings`.
    ///
    /// Returns the number of documents (rows) processed. Shared by the two
    /// build entry points so the term-extraction and `_vid` decoding logic
    /// lives in exactly one place.
    ///
    /// # Errors
    /// Returns an error if the batch lacks the `_vid` column or the indexed
    /// property is not a `List<String>` column.
    fn accumulate_batch(
        &self,
        batch: &RecordBatch,
        postings: &mut HashMap<String, Vec<u64>>,
    ) -> Result<usize> {
        let vid_col = batch
            .column_by_name("_vid")
            .ok_or_else(|| anyhow!("Missing _vid"))?
            .as_any()
            .downcast_ref::<UInt64Array>()
            .ok_or_else(|| anyhow!("Invalid _vid type"))?;

        let term_col = batch
            .column_by_name(&self.property)
            .ok_or_else(|| anyhow!("Missing property {}", self.property))?;

        let list_array = term_col
            .as_any()
            .downcast_ref::<ListArray>()
            .ok_or_else(|| {
                anyhow!(
                    "Property {} must be List<String>, got {:?}",
                    self.property,
                    term_col.data_type()
                )
            })?;

        let values = list_array
            .values()
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| anyhow!("Property {} must be List<String>", self.property))?;

        let mut count = 0;
        for i in 0..batch.num_rows() {
            let vid = vid_col.value(i);

            if list_array.is_null(i) {
                continue;
            }

            let start = list_array.value_offsets()[i] as usize;
            let end = list_array.value_offsets()[i + 1] as usize;

            let mut terms = HashSet::new();
            for j in start..end {
                if !values.is_null(j) {
                    let term = values.value(j);
                    let term = if self.config.normalize {
                        term.to_lowercase().trim().to_string()
                    } else {
                        term.to_string()
                    };
                    terms.insert(term);
                }
            }

            for term in terms {
                postings.entry(term).or_default().push(vid);
            }

            count += 1;
        }
        Ok(count)
    }

    /// Merge the accumulated segments and overwrite the on-disk postings.
    async fn finish_build(
        &mut self,
        postings: HashMap<String, Vec<u64>>,
        mut temp_segments: Vec<HashMap<String, Vec<u64>>>,
    ) -> Result<()> {
        if temp_segments.is_empty() {
            self.write_postings(postings).await?;
        } else {
            temp_segments.push(postings);
            info!(segments = temp_segments.len(), "Merging postings segments");
            let merged = merge_postings_segments(temp_segments);
            debug!(final_terms = merged.len(), "Merged postings");
            self.write_postings(merged).await?;
        }
        Ok(())
    }

    /// Rebuild the index from already-scanned record batches.
    ///
    /// This is the canonical backfill path: the LanceDB-managed vertex table
    /// is read by the storage backend (`backend.scan`), not by a raw
    /// `lance::Dataset::open` whose physical path differs. Calls `progress`
    /// per batch with the running document count and uses segmented
    /// accumulation to stay within the 256 MB memory limit.
    ///
    /// # Errors
    /// Returns an error if a batch is missing `_vid` or the indexed property
    /// is not `List<String>`, or if persisting the postings fails.
    pub async fn build_from_batches(
        &mut self,
        batches: &[RecordBatch],
        progress: impl Fn(usize),
    ) -> Result<()> {
        let mut postings: HashMap<String, Vec<u64>> = HashMap::new();
        let mut temp_segments: Vec<HashMap<String, Vec<u64>>> = Vec::new();
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
    async fn write_postings(&mut self, postings: HashMap<String, Vec<u64>>) -> Result<()> {
        let mut terms = Vec::with_capacity(postings.len());
        let mut vid_lists = Vec::with_capacity(postings.len());

        for (term, vids) in postings {
            terms.push(term);
            vid_lists.push(Some(vids.into_iter().map(Some).collect::<Vec<_>>()));
        }

        let term_array = StringArray::from(terms);
        let vid_list_array = ListArray::from_iter_primitive::<UInt64Type, _, _>(vid_lists);

        let batch = arrow_array::RecordBatch::try_from_iter(vec![
            ("term", Arc::new(term_array) as Arc<dyn arrow_array::Array>),
            (
                "vids",
                Arc::new(vid_list_array) as Arc<dyn arrow_array::Array>,
            ),
        ])?;

        let path = format!(
            "{}/indexes/{}/{}_inverted",
            self.base_uri, self.label, self.property
        );
        let write_params = lance::dataset::WriteParams {
            mode: lance::dataset::WriteMode::Overwrite,
            ..Default::default()
        };

        let iter_schema = Arc::new(ArrowSchema::new(vec![
            Field::new("term", DataType::Utf8, false),
            Field::new(
                "vids",
                DataType::List(Arc::new(Field::new("item", DataType::UInt64, true))),
                false,
            ),
        ]));

        // Retried: a losing Lance commit is retryable, and this writer used to
        // propagate it.
        let ds = crate::storage::manager::write_dataset_with_lance_conflict_retry(
            &path,
            iter_schema,
            vec![batch],
            write_params.mode,
        )
        .await?;
        self.dataset = Some(ds);

        Ok(())
    }

    /// Return all VIDs whose term list intersects `terms` (OR semantics).
    pub async fn query_any(&self, terms: &[String]) -> Result<Vec<Vid>> {
        let Some(ds) = &self.dataset else {
            debug!("Inverted index not initialized, returning empty result");
            return Ok(Vec::new());
        };

        let normalized: Vec<String> = if self.config.normalize {
            terms
                .iter()
                .map(|t| t.to_lowercase().trim().to_string())
                .collect()
        } else {
            terms.to_vec()
        };

        if normalized.is_empty() {
            return Ok(Vec::new());
        }

        let filter = FilterExpr::any_of(
            normalized
                .iter()
                .map(|t| FilterExpr::equals("term", Scalar::Str(t.clone()))),
        )
        .to_sql()?;

        debug!(filter = %filter, "Querying inverted index");

        let mut scanner = ds.scan();
        scanner.filter(&filter)?;

        let mut stream = scanner.try_into_stream().await?;
        let mut result_set: HashSet<u64> = HashSet::new();

        while let Some(batch) = stream.try_next().await? {
            let vids_col = batch
                .column_by_name("vids")
                .ok_or_else(|| anyhow!("Missing vids column"))?
                .as_any()
                .downcast_ref::<ListArray>()
                .ok_or_else(|| anyhow!("Invalid vids column"))?;

            for i in 0..batch.num_rows() {
                if vids_col.is_null(i) {
                    continue;
                }

                let vids_array = vids_col.value(i);
                let vids = vids_array
                    .as_any()
                    .downcast_ref::<UInt64Array>()
                    .ok_or_else(|| anyhow!("Invalid inner vids type"))?;

                for vid in vids.iter().flatten() {
                    result_set.insert(vid);
                }
            }
        }

        debug!(count = result_set.len(), "Found matching VIDs");

        Ok(result_set.into_iter().map(Vid::from).collect())
    }

    /// Loads all postings from the existing dataset.
    ///
    /// Returns a map of term to list of VIDs. Returns an empty map if the
    /// dataset doesn't exist yet.
    #[instrument(skip(self), level = "debug")]
    async fn load_postings(&self) -> Result<HashMap<String, HashSet<u64>>> {
        let Some(ds) = &self.dataset else {
            return Ok(HashMap::new());
        };

        let mut postings: HashMap<String, HashSet<u64>> = HashMap::new();
        let scanner = ds.scan();
        let mut stream = scanner.try_into_stream().await?;

        while let Some(batch) = stream.try_next().await? {
            let term_col = batch
                .column_by_name("term")
                .ok_or_else(|| anyhow!("Missing term column"))?
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| anyhow!("Invalid term column type"))?;

            let vids_col = batch
                .column_by_name("vids")
                .ok_or_else(|| anyhow!("Missing vids column"))?
                .as_any()
                .downcast_ref::<ListArray>()
                .ok_or_else(|| anyhow!("Invalid vids column type"))?;

            for i in 0..batch.num_rows() {
                if term_col.is_null(i) || vids_col.is_null(i) {
                    continue;
                }

                let term = term_col.value(i).to_string();
                let vids_array = vids_col.value(i);
                let vids = vids_array
                    .as_any()
                    .downcast_ref::<UInt64Array>()
                    .ok_or_else(|| anyhow!("Invalid inner vids type"))?;

                let entry = postings.entry(term).or_default();
                for vid in vids.iter().flatten() {
                    entry.insert(vid);
                }
            }
        }

        Ok(postings)
    }

    /// Applies incremental updates to the inverted index.
    ///
    /// This method efficiently updates the index by:
    /// 1. Loading existing postings
    /// 2. Removing VIDs that have been deleted
    /// 3. Adding new VIDs with their associated terms
    /// 4. Writing the updated postings back
    ///
    /// # Errors
    ///
    /// Returns an error if loading or writing postings fails.
    #[instrument(skip(self, added, removed), level = "info", fields(
        label = %self.label,
        property = %self.property,
        added_count = added.len(),
        removed_count = removed.len()
    ))]
    pub async fn apply_incremental_updates(
        &mut self,
        added: &HashMap<Vid, Vec<String>>,
        removed: &HashSet<Vid>,
    ) -> Result<()> {
        info!(
            added = added.len(),
            removed = removed.len(),
            "Applying incremental updates to inverted index"
        );

        // Load existing postings
        let mut postings = self.load_postings().await?;

        // Remove VIDs that have been deleted
        if !removed.is_empty() {
            let removed_u64: HashSet<u64> = removed.iter().map(|v| v.as_u64()).collect();
            for vids in postings.values_mut() {
                vids.retain(|vid| !removed_u64.contains(vid));
            }
            // Remove empty terms
            postings.retain(|_, vids| !vids.is_empty());
        }

        // Add new VIDs with their terms
        for (vid, terms) in added {
            let vid_u64 = vid.as_u64();
            let normalized_terms: HashSet<String> = if self.config.normalize {
                terms
                    .iter()
                    .map(|t| t.to_lowercase().trim().to_string())
                    .collect()
            } else {
                terms.iter().cloned().collect()
            };

            // Respect max_terms_per_doc limit
            let terms_to_add: Vec<_> = if normalized_terms.len() > self.config.max_terms_per_doc {
                normalized_terms
                    .into_iter()
                    .take(self.config.max_terms_per_doc)
                    .collect()
            } else {
                normalized_terms.into_iter().collect()
            };

            for term in terms_to_add {
                postings.entry(term).or_default().insert(vid_u64);
            }
        }

        // Convert HashSet<u64> to Vec<u64> for writing
        let postings_vec: HashMap<String, Vec<u64>> = postings
            .into_iter()
            .map(|(term, vids)| (term, vids.into_iter().collect()))
            .collect();

        info!(terms = postings_vec.len(), "Writing updated postings");

        self.write_postings(postings_vec).await?;
        Ok(())
    }

    /// Extracts terms from a JSON value representing a `List<String>`.
    ///
    /// Returns `None` if the value is not an array of strings.
    pub fn extract_terms_from_value(&self, value: &Value) -> Option<Vec<String>> {
        let arr = value.as_array()?;
        let terms: Vec<String> = arr
            .iter()
            .filter_map(|v| v.as_str().map(ToString::to_string))
            .collect();

        if terms.is_empty() { None } else { Some(terms) }
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_estimated_postings_memory_empty() {
        let postings: HashMap<String, Vec<u64>> = HashMap::new();
        assert_eq!(estimated_postings_memory(&postings), 0);
    }

    #[test]
    fn test_estimated_postings_memory_nonempty() {
        let mut postings = HashMap::new();
        postings.insert("hello".to_string(), vec![1, 2, 3]);
        let mem = estimated_postings_memory(&postings);
        // "hello" = 5 bytes + size_of::<Vec<u64>>() + 3 * 8 bytes
        let expected = 5 + std::mem::size_of::<Vec<u64>>() + 3 * 8;
        assert_eq!(mem, expected);
    }

    #[test]
    fn test_merge_postings_segments_empty() {
        let segments: Vec<HashMap<String, Vec<u64>>> = vec![];
        let merged = merge_postings_segments(segments);
        assert!(merged.is_empty());
    }

    #[test]
    fn test_merge_postings_segments_overlapping() {
        let seg1: HashMap<String, Vec<u64>> =
            [("a".to_string(), vec![1, 2]), ("b".to_string(), vec![3])]
                .into_iter()
                .collect();
        let seg2: HashMap<String, Vec<u64>> =
            [("a".to_string(), vec![4, 5]), ("c".to_string(), vec![6])]
                .into_iter()
                .collect();
        let merged = merge_postings_segments(vec![seg1, seg2]);

        assert_eq!(merged.get("a").unwrap().len(), 4); // [1,2,4,5]
        assert_eq!(merged.get("b").unwrap(), &vec![3]);
        assert_eq!(merged.get("c").unwrap(), &vec![6]);
    }

    #[test]
    fn test_default_max_postings_memory() {
        assert_eq!(DEFAULT_MAX_POSTINGS_MEMORY, 256 * 1024 * 1024);
    }
}
