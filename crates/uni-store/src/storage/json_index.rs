// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

use anyhow::{Result, anyhow};
use arrow_array::builder::{ListBuilder, StringBuilder, UInt64Builder};
use arrow_array::{Array, ListArray, RecordBatch, RecordBatchIterator, UInt64Array};
use arrow_schema::{DataType as ArrowDataType, Field, Schema as ArrowSchema};
use futures::TryStreamExt;
use lance::dataset::{Dataset, WriteMode, WriteParams};
use std::sync::Arc;
use uni_common::core::id::Vid;

pub struct JsonPathIndex {
    uri: String,
}

impl JsonPathIndex {
    pub fn new(base_uri: &str, label: &str, path: &str) -> Self {
        // Path might contain special chars like $.title -> idx_label_title
        let safe_path = path.replace(|c: char| !c.is_alphanumeric(), "_");
        let uri = format!("{}/indexes/idx_{}_{}", base_uri, label, safe_path);
        Self { uri }
    }

    pub async fn open(&self) -> Result<Arc<Dataset>> {
        let ds = Dataset::open(&self.uri).await?;
        Ok(Arc::new(ds))
    }

    pub fn get_arrow_schema() -> Arc<ArrowSchema> {
        Arc::new(ArrowSchema::new(vec![
            Field::new("value", ArrowDataType::Utf8, false),
            Field::new(
                "vids",
                ArrowDataType::List(Arc::new(Field::new("item", ArrowDataType::UInt64, true))),
                false,
            ),
        ]))
    }

    pub async fn write_entries(&self, entries: Vec<(String, Vec<Vid>)>) -> Result<()> {
        let schema = Self::get_arrow_schema();

        let mut value_builder = StringBuilder::new();
        let mut vids_builder = ListBuilder::new(UInt64Builder::new());

        for (val, vids) in entries {
            value_builder.append_value(val);
            for vid in vids {
                vids_builder.values().append_value(vid.as_u64());
            }
            vids_builder.append(true);
        }

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(value_builder.finish()),
                Arc::new(vids_builder.finish()),
            ],
        )?;

        let reader = RecordBatchIterator::new(std::iter::once(Ok(batch)), schema);

        let params = WriteParams {
            mode: WriteMode::Append,
            ..Default::default()
        };

        Dataset::write(Box::new(reader), &self.uri, Some(params)).await?;
        Ok(())
    }

    pub async fn get_vids(&self, value: &str) -> Result<Vec<Vid>> {
        let ds = match self.open().await {
            Ok(ds) => ds,
            Err(_) => return Ok(vec![]),
        };

        // Scan and filter (MVP)
        let mut stream = ds
            .scan()
            .filter(&format!("value = '{}'", value))?
            .try_into_stream()
            .await?;

        let mut result = Vec::new();

        while let Some(batch) = stream.try_next().await? {
            let vids_col = batch
                .column_by_name("vids")
                .ok_or(anyhow!("Missing vids column"))?
                .as_any()
                .downcast_ref::<ListArray>()
                .ok_or(anyhow!("Invalid vids column type"))?;

            for i in 0..batch.num_rows() {
                let list = vids_col.value(i);
                let uint64_list = list
                    .as_any()
                    .downcast_ref::<UInt64Array>()
                    .ok_or(anyhow!("Invalid inner type"))?;

                for j in 0..uint64_list.len() {
                    result.push(Vid::from(uint64_list.value(j)));
                }
            }
        }
        Ok(result)
    }
}
