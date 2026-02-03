// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

use anyhow::Result;
use arrow_array::{RecordBatch, RecordBatchIterator};
use arrow_schema::{Field, Schema as ArrowSchema};
use lance::dataset::{Dataset, WriteMode, WriteParams};
use std::sync::Arc;
use uni_common::core::schema::Schema;

pub struct EdgeDataset {
    uri: String,
    edge_type: String,
}

impl EdgeDataset {
    pub fn new(base_uri: &str, edge_type: &str, _src_label: &str, _dst_label: &str) -> Self {
        let uri = format!("{}/edges/{}", base_uri, edge_type);
        Self {
            uri,
            edge_type: edge_type.to_string(),
        }
    }

    pub async fn open(&self) -> Result<Arc<Dataset>> {
        let ds = Dataset::open(&self.uri).await?;
        Ok(Arc::new(ds))
    }

    pub async fn write_batch(&self, batch: RecordBatch, mode: WriteMode) -> Result<Arc<Dataset>> {
        let arrow_schema = batch.schema();
        let reader = RecordBatchIterator::new(std::iter::once(Ok(batch)), arrow_schema);

        let params = WriteParams {
            mode,
            ..Default::default()
        };

        let ds = Dataset::write(Box::new(reader), &self.uri, Some(params)).await?;
        Ok(Arc::new(ds))
    }

    pub fn get_arrow_schema(&self, schema: &Schema) -> Result<Arc<ArrowSchema>> {
        let mut fields = vec![
            Field::new("eid", arrow_schema::DataType::UInt64, false),
            Field::new("src_vid", arrow_schema::DataType::UInt64, false),
            Field::new("dst_vid", arrow_schema::DataType::UInt64, false),
            Field::new("_deleted", arrow_schema::DataType::Boolean, false),
            Field::new("_version", arrow_schema::DataType::UInt64, false),
        ];

        if let Some(type_props) = schema.properties.get(&self.edge_type) {
            let mut sorted_props: Vec<_> = type_props.iter().collect();
            sorted_props.sort_by_key(|(name, _)| *name);

            for (name, meta) in sorted_props {
                fields.push(Field::new(name, meta.r#type.to_arrow(), meta.nullable));
            }
        }

        Ok(Arc::new(ArrowSchema::new(fields)))
    }
}
