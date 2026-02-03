// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

use anyhow::{Result, anyhow};
use arrow_array::builder::FixedSizeBinaryBuilder;
use arrow_array::{Array, FixedSizeBinaryArray, RecordBatch, RecordBatchIterator, UInt64Array};
use arrow_schema::{DataType as ArrowDataType, Field, Schema as ArrowSchema};
use futures::TryStreamExt;
use lance::dataset::{Dataset, WriteMode, WriteParams};
use std::collections::HashMap;
use std::sync::Arc;
use uni_common::core::id::{UniId, Vid};

pub struct UidIndex {
    uri: String,
}

impl UidIndex {
    pub fn new(base_uri: &str, label: &str) -> Self {
        let uri = format!("{}/indexes/uni_id_to_vid/{}/index.lance", base_uri, label);
        Self { uri }
    }

    pub async fn open(&self) -> Result<Arc<Dataset>> {
        let ds = Dataset::open(&self.uri).await?;
        Ok(Arc::new(ds))
    }

    pub fn get_arrow_schema() -> Arc<ArrowSchema> {
        Arc::new(ArrowSchema::new(vec![
            Field::new("_uid", ArrowDataType::FixedSizeBinary(32), false),
            Field::new("_vid", ArrowDataType::UInt64, false),
        ]))
    }

    pub async fn write_mapping(&self, mappings: &[(UniId, Vid)]) -> Result<()> {
        let schema = Self::get_arrow_schema();

        let mut uid_builder = FixedSizeBinaryBuilder::new(32);
        let mut vids = Vec::with_capacity(mappings.len());

        for (uid, vid) in mappings {
            uid_builder.append_value(uid.as_bytes()).unwrap();
            vids.push(vid.as_u64());
        }

        let uid_array = uid_builder.finish();
        let vid_array = UInt64Array::from(vids);

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(uid_array), Arc::new(vid_array)],
        )?;

        let reader = RecordBatchIterator::new(std::iter::once(Ok(batch)), schema);

        let params = WriteParams {
            mode: WriteMode::Append,
            ..Default::default()
        };

        Dataset::write(Box::new(reader), &self.uri, Some(params)).await?;
        Ok(())
    }

    pub async fn get_vid(&self, uid: &UniId) -> Result<Option<Vid>> {
        let ds = match self.open().await {
            Ok(ds) => ds,
            Err(_) => return Ok(None),
        };

        // TODO: This is a full scan if not indexed.
        // In a real implementation, we would rely on Lance's indexing capabilities or ensure data is sorted.
        // For now, we use a filter scan.
        // Note: Filter on FixedSizeBinary might be tricky in SQL/string format.
        // We might need to encode it or use a binary compatible filter if Lance supports it.
        // Lance SQL parser usually takes strings.
        // workaround: Since we can't easily pass binary blob in SQL filter string,
        // we might scan and filter in memory if the index is small, OR
        // ideally we use a specialized index lookup if Lance exposes one.

        // For MVP: Scan and filter in memory. This is slow but correct.
        let mut stream = ds
            .scan()
            .project(&["_uid", "_vid"])?
            .try_into_stream()
            .await?;

        while let Some(batch) = stream.try_next().await? {
            let uid_col = batch
                .column_by_name("_uid")
                .ok_or(anyhow!("Missing _uid column"))?
                .as_any()
                .downcast_ref::<FixedSizeBinaryArray>()
                .ok_or(anyhow!("Invalid _uid column type"))?;

            let vid_col = batch
                .column_by_name("_vid")
                .ok_or(anyhow!("Missing _vid column"))?
                .as_any()
                .downcast_ref::<UInt64Array>()
                .ok_or(anyhow!("Invalid _vid column type"))?;

            for i in 0..batch.num_rows() {
                if !uid_col.is_null(i) {
                    let val = uid_col.value(i);
                    if val == uid.as_bytes() {
                        return Ok(Some(Vid::from(vid_col.value(i))));
                    }
                }
            }
        }

        Ok(None)
    }

    pub async fn resolve_uids(&self, uids: &[UniId]) -> Result<HashMap<UniId, Vid>> {
        // MVP: Loop get_vid. Optimize later.
        let mut result = HashMap::new();
        for uid in uids {
            if let Some(vid) = self.get_vid(uid).await? {
                result.insert(*uid, vid);
            }
        }
        Ok(result)
    }
}
