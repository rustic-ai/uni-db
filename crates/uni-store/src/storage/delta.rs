// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

use crate::lancedb::LanceDbStore;
use crate::storage::arrow_convert::build_timestamp_column;
use crate::storage::property_builder::PropertyColumnBuilder;
use crate::storage::value_codec::CrdtDecodeMode;
use anyhow::{Result, anyhow};
use arrow_array::types::TimestampMicrosecondType;
use arrow_array::{Array, ArrayRef, PrimitiveArray, RecordBatch, UInt8Array, UInt64Array};
use arrow_schema::{Field, Schema as ArrowSchema, TimeUnit};
use futures::TryStreamExt;
use lance::dataset::Dataset;
use lancedb::Table;
use lancedb::index::Index as LanceDbIndex;
use lancedb::index::scalar::BTreeIndexBuilder;
use std::sync::Arc;
use uni_common::DataType;
use uni_common::Properties;
use uni_common::core::id::{Eid, Vid};
use uni_common::core::schema::Schema;

#[derive(Clone, Debug, PartialEq)]
pub enum Op {
    Insert = 0,
    Delete = 1,
}

#[derive(Clone, Debug)]
pub struct L1Entry {
    pub src_vid: Vid,
    pub dst_vid: Vid,
    pub eid: Eid,
    pub op: Op,
    pub version: u64,
    pub properties: Properties,
    /// Timestamp when the edge was created (microseconds since epoch).
    pub created_at: Option<i64>,
    /// Timestamp when the edge was last updated (microseconds since epoch).
    pub updated_at: Option<i64>,
}

pub struct DeltaDataset {
    uri: String,
    edge_type: String,
    direction: String, // "fwd" or "bwd"
}

impl DeltaDataset {
    pub fn new(base_uri: &str, edge_type: &str, direction: &str) -> Self {
        let uri = format!("{}/deltas/{}_{}", base_uri, edge_type, direction);
        Self {
            uri,
            edge_type: edge_type.to_string(),
            direction: direction.to_string(),
        }
    }

    pub async fn open(&self) -> Result<Arc<Dataset>> {
        self.open_at(None).await
    }

    pub async fn open_at(&self, version: Option<u64>) -> Result<Arc<Dataset>> {
        let mut ds = Dataset::open(&self.uri).await?;
        if let Some(v) = version {
            ds = ds.checkout_version(v).await?;
        }
        Ok(Arc::new(ds))
    }

    pub async fn open_latest(&self) -> Result<Arc<Dataset>> {
        self.open().await
    }

    pub async fn open_latest_raw(&self) -> Result<Dataset> {
        let ds = Dataset::open(&self.uri).await?;
        Ok(ds)
    }

    pub fn get_arrow_schema(&self, schema: &Schema) -> Result<Arc<ArrowSchema>> {
        let mut fields = vec![
            Field::new("src_vid", arrow_schema::DataType::UInt64, false),
            Field::new("dst_vid", arrow_schema::DataType::UInt64, false),
            Field::new("eid", arrow_schema::DataType::UInt64, false),
            Field::new("op", arrow_schema::DataType::UInt8, false), // 0=INSERT, 1=DELETE
            Field::new("_version", arrow_schema::DataType::UInt64, false),
            // New timestamp columns per STORAGE_DESIGN.md
            Field::new(
                "_created_at",
                arrow_schema::DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
                true,
            ),
            Field::new(
                "_updated_at",
                arrow_schema::DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
                true,
            ),
        ];

        if let Some(type_props) = schema.properties.get(&self.edge_type) {
            let mut sorted_props: Vec<_> = type_props.iter().collect();
            sorted_props.sort_by_key(|(name, _)| *name);

            for (name, meta) in sorted_props {
                fields.push(Field::new(name, meta.r#type.to_arrow(), meta.nullable));
            }
        }

        // Add overflow_json column for non-schema properties (JSONB binary format)
        fields.push(Field::new(
            "overflow_json",
            arrow_schema::DataType::LargeBinary,
            true,
        ));

        Ok(Arc::new(ArrowSchema::new(fields)))
    }

    pub fn build_record_batch(&self, entries: &[L1Entry], schema: &Schema) -> Result<RecordBatch> {
        let arrow_schema = self.get_arrow_schema(schema)?;

        let mut src_vids = Vec::with_capacity(entries.len());
        let mut dst_vids = Vec::with_capacity(entries.len());
        let mut eids = Vec::with_capacity(entries.len());
        let mut ops = Vec::with_capacity(entries.len());
        let mut versions = Vec::with_capacity(entries.len());

        for entry in entries {
            src_vids.push(entry.src_vid.as_u64());
            dst_vids.push(entry.dst_vid.as_u64());
            eids.push(entry.eid.as_u64());
            ops.push(entry.op.clone() as u8);
            versions.push(entry.version);
        }

        let mut columns: Vec<ArrayRef> = vec![
            Arc::new(UInt64Array::from(src_vids)),
            Arc::new(UInt64Array::from(dst_vids)),
            Arc::new(UInt64Array::from(eids)),
            Arc::new(UInt8Array::from(ops)),
            Arc::new(UInt64Array::from(versions)),
        ];

        // Build _created_at and _updated_at columns using shared builder
        columns.push(build_timestamp_column(entries.iter().map(|e| e.created_at)));
        columns.push(build_timestamp_column(entries.iter().map(|e| e.updated_at)));

        // Derive deleted flags from Op for property column building
        // Tombstones (Op::Delete) are logically deleted and should use default values
        let deleted_flags: Vec<bool> = entries.iter().map(|e| e.op == Op::Delete).collect();

        // Build property columns using shared builder
        let prop_columns = PropertyColumnBuilder::new(schema, &self.edge_type, entries.len())
            .with_deleted(&deleted_flags)
            .build(|i| &entries[i].properties)?;

        columns.extend(prop_columns);

        // Build overflow_json column for non-schema properties
        let overflow_column = self.build_overflow_json_column(entries, schema)?;
        columns.push(overflow_column);

        RecordBatch::try_new(arrow_schema, columns).map_err(|e| anyhow!(e))
    }

    /// Build the overflow_json column containing properties not in schema.
    ///
    /// This method identifies properties that are not defined in the edge type's schema
    /// and serializes them as a JSON blob. Properties in schema are stored as typed
    /// columns, while overflow properties are stored in this JSON column.
    fn build_overflow_json_column(&self, entries: &[L1Entry], schema: &Schema) -> Result<ArrayRef> {
        use arrow_array::builder::LargeBinaryBuilder;
        use std::collections::HashMap;

        let schema_props = schema.properties.get(&self.edge_type);
        let mut builder = LargeBinaryBuilder::new();

        for entry in entries {
            let mut overflow_props = HashMap::new();

            // Collect non-schema properties
            for (key, value) in &entry.properties {
                if !schema_props.is_some_and(|sp| sp.contains_key(key)) {
                    overflow_props.insert(key.clone(), value.clone());
                }
            }

            // Serialize to JSONB binary (or null if no overflow)
            if overflow_props.is_empty() {
                builder.append_null();
            } else {
                let jsonb = jsonb::to_owned_jsonb(&overflow_props).map_err(|e| {
                    anyhow!("Failed to serialize overflow properties to JSONB: {}", e)
                })?;
                let bytes = jsonb.to_vec();
                builder.append_value(&bytes);
            }
        }

        Ok(Arc::new(builder.finish()))
    }

    pub async fn scan_all(&self, schema: &Schema) -> Result<Vec<L1Entry>> {
        let ds = match self.open_latest().await {
            Ok(ds) => ds,
            Err(_) => return Ok(vec![]),
        };

        // Note: Lance doesn't guarantee global sort on scan unless we explicitly sort.
        // For compaction, we pull everything and sort in memory for now.
        // In a production system, we'd use an external merge sort or Lance's indexing.

        let mut stream = ds.scan().try_into_stream().await?;

        let mut entries = Vec::new();

        while let Some(batch) = stream.try_next().await? {
            // Reusing the parsing logic would be nice, but for now duplicate it to keep it self-contained
            // or refactor parsing into a helper. Let's refactor into a helper.
            let mut batch_entries = self.parse_batch(&batch, schema)?;
            entries.append(&mut batch_entries);
        }

        // Sort by key and then version to ensure correct order of operations
        entries.sort_by(|a, b| {
            let key_a = if self.direction == "fwd" {
                a.src_vid
            } else {
                a.dst_vid
            };
            let key_b = if self.direction == "fwd" {
                b.src_vid
            } else {
                b.dst_vid
            };

            match key_a.cmp(&key_b) {
                std::cmp::Ordering::Equal => a.version.cmp(&b.version),
                other => other,
            }
        });

        Ok(entries)
    }

    fn parse_batch(&self, batch: &RecordBatch, schema: &Schema) -> Result<Vec<L1Entry>> {
        let src_vids = batch
            .column_by_name("src_vid")
            .ok_or(anyhow!("Missing src_vid"))?
            .as_any()
            .downcast_ref::<UInt64Array>()
            .ok_or(anyhow!("Invalid src_vid type"))?;
        let dst_vids = batch
            .column_by_name("dst_vid")
            .ok_or(anyhow!("Missing dst_vid"))?
            .as_any()
            .downcast_ref::<UInt64Array>()
            .ok_or(anyhow!("Invalid dst_vid type"))?;
        let eids = batch
            .column_by_name("eid")
            .ok_or(anyhow!("Missing eid"))?
            .as_any()
            .downcast_ref::<UInt64Array>()
            .ok_or(anyhow!("Invalid eid type"))?;
        let ops = batch
            .column_by_name("op")
            .ok_or(anyhow!("Missing op"))?
            .as_any()
            .downcast_ref::<UInt8Array>()
            .ok_or(anyhow!("Invalid op type"))?;
        let versions = batch
            .column_by_name("_version")
            .ok_or(anyhow!("Missing _version"))?
            .as_any()
            .downcast_ref::<UInt64Array>()
            .ok_or(anyhow!("Invalid _version type"))?;

        // Try to read timestamp columns (may not exist in old data)
        let created_at_col = batch.column_by_name("_created_at").and_then(|c| {
            c.as_any()
                .downcast_ref::<PrimitiveArray<TimestampMicrosecondType>>()
        });
        let updated_at_col = batch.column_by_name("_updated_at").and_then(|c| {
            c.as_any()
                .downcast_ref::<PrimitiveArray<TimestampMicrosecondType>>()
        });

        // Prepare property columns
        let mut prop_cols = Vec::new();
        if let Some(type_props) = schema.properties.get(&self.edge_type) {
            for (name, meta) in type_props {
                if let Some(col) = batch.column_by_name(name) {
                    prop_cols.push((name, meta.r#type.clone(), col));
                }
            }
        }

        let mut entries = Vec::with_capacity(batch.num_rows());

        for i in 0..batch.num_rows() {
            let op = match ops.value(i) {
                0 => Op::Insert,
                1 => Op::Delete,
                _ => continue, // Unknown op
            };

            let properties = self.extract_properties(&prop_cols, i)?;

            // Read timestamps if present
            let created_at = created_at_col.and_then(|col| {
                if col.is_null(i) {
                    None
                } else {
                    Some(col.value(i))
                }
            });
            let updated_at = updated_at_col.and_then(|col| {
                if col.is_null(i) {
                    None
                } else {
                    Some(col.value(i))
                }
            });

            entries.push(L1Entry {
                src_vid: Vid::from(src_vids.value(i)),
                dst_vid: Vid::from(dst_vids.value(i)),
                eid: Eid::from(eids.value(i)),
                op,
                version: versions.value(i),
                properties,
                created_at,
                updated_at,
            });
        }
        Ok(entries)
    }

    /// Extract properties from columns for a single row.
    fn extract_properties(
        &self,
        prop_cols: &[(&String, DataType, &ArrayRef)],
        row: usize,
    ) -> Result<Properties> {
        let mut properties = Properties::new();
        for (name, dtype, col) in prop_cols {
            if col.is_null(row) {
                continue;
            }
            let val = Self::value_from_column(col.as_ref(), dtype, row)?;
            properties.insert(name.to_string(), uni_common::Value::from(val));
        }
        Ok(properties)
    }

    /// Decode an Arrow column value to JSON with lenient CRDT error handling.
    fn value_from_column(
        col: &dyn arrow_array::Array,
        dtype: &uni_common::DataType,
        row: usize,
    ) -> Result<serde_json::Value> {
        crate::storage::value_codec::value_from_column(col, dtype, row, CrdtDecodeMode::Lenient)
    }

    // ========================================================================
    // LanceDB-based Methods
    // ========================================================================

    /// Open a delta table using LanceDB.
    pub async fn open_lancedb(&self, store: &LanceDbStore) -> Result<Table> {
        store
            .open_delta_table(&self.edge_type, &self.direction)
            .await
    }

    /// Open or create a delta table using LanceDB.
    pub async fn open_or_create_lancedb(
        &self,
        store: &LanceDbStore,
        schema: &Schema,
    ) -> Result<Table> {
        let arrow_schema = self.get_arrow_schema(schema)?;
        store
            .open_or_create_delta_table(&self.edge_type, &self.direction, arrow_schema)
            .await
    }

    /// Write a run to a LanceDB delta table.
    ///
    /// Creates the table if it doesn't exist, otherwise appends to it.
    pub async fn write_run_lancedb(
        &self,
        store: &LanceDbStore,
        batch: RecordBatch,
    ) -> Result<Table> {
        let table_name = LanceDbStore::delta_table_name(&self.edge_type, &self.direction);

        if store.table_exists(&table_name).await? {
            let table = store.open_table(&table_name).await?;
            store.append_to_table(&table, vec![batch]).await?;
            Ok(table)
        } else {
            store.create_table(&table_name, vec![batch]).await
        }
    }

    /// Ensure a BTree index exists on the 'eid' column using LanceDB.
    pub async fn ensure_eid_index_lancedb(&self, table: &Table) -> Result<()> {
        let indices = table
            .list_indices()
            .await
            .map_err(|e| anyhow!("Failed to list indices: {}", e))?;

        if !indices
            .iter()
            .any(|idx| idx.columns.contains(&"eid".to_string()))
        {
            log::info!(
                "Creating eid BTree index for edge type '{}' via LanceDB",
                self.edge_type
            );
            if let Err(e) = table
                .create_index(&["eid"], LanceDbIndex::BTree(BTreeIndexBuilder::default()))
                .execute()
                .await
            {
                log::warn!(
                    "Failed to create eid index for '{}' via LanceDB: {}",
                    self.edge_type,
                    e
                );
            }
        }

        Ok(())
    }

    /// Get the LanceDB table name for this delta dataset.
    pub fn lancedb_table_name(&self) -> String {
        LanceDbStore::delta_table_name(&self.edge_type, &self.direction)
    }

    /// Scan all entries from LanceDB table.
    ///
    /// Returns an empty vector if the table doesn't exist.
    pub async fn scan_all_lancedb(
        &self,
        store: &LanceDbStore,
        schema: &Schema,
    ) -> Result<Vec<L1Entry>> {
        let table = match self.open_lancedb(store).await {
            Ok(t) => t,
            Err(_) => return Ok(vec![]),
        };

        use lancedb::query::ExecutableQuery;
        let stream = table.query().execute().await?;
        let batches: Vec<arrow_array::RecordBatch> = stream.try_collect().await?;

        let mut entries = Vec::new();
        for batch in batches {
            let mut batch_entries = self.parse_batch(&batch, schema)?;
            entries.append(&mut batch_entries);
        }

        Ok(entries)
    }

    /// Read delta entries for a specific vertex ID from LanceDB.
    ///
    /// Returns an empty vector if the table doesn't exist or no entries match.
    pub async fn read_deltas_lancedb(
        &self,
        store: &LanceDbStore,
        vid: Vid,
        schema: &Schema,
        version_hwm: Option<u64>,
    ) -> Result<Vec<L1Entry>> {
        let table = match self.open_lancedb(store).await {
            Ok(t) => t,
            Err(_) => return Ok(vec![]),
        };

        let filter_col = if self.direction == "fwd" {
            "src_vid"
        } else {
            "dst_vid"
        };

        use lancedb::query::{ExecutableQuery, QueryBase};

        let base_filter = format!("{} = {}", filter_col, vid.as_u64());

        // Add version filtering if snapshot is active
        let final_filter = if let Some(hwm) = version_hwm {
            format!("({}) AND (_version <= {})", base_filter, hwm)
        } else {
            base_filter
        };

        let query = table.query().only_if(final_filter);
        let stream = query.execute().await?;
        let batches: Vec<arrow_array::RecordBatch> = stream.try_collect().await?;

        let mut entries = Vec::new();
        for batch in batches {
            let mut batch_entries = self.parse_batch(&batch, schema)?;
            entries.append(&mut batch_entries);
        }

        Ok(entries)
    }
}
