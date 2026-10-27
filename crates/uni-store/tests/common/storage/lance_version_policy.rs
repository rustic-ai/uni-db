//! A dependency upgrade must not silently change Uni's on-disk format.

use std::sync::Arc;

use arrow_array::{Int64Array, RecordBatch, RecordBatchIterator};
use arrow_schema::{DataType, Field, Schema};
use lance::Dataset;
use tempfile::TempDir;
use uni_common::core::id::Vid;
use uni_store::backend::{
    lance::LanceDbBackend, lance_branch, traits::StorageBackend, types::WriteMode,
};
use uni_store::storage::json_index::JsonPathIndex;

fn batch() -> RecordBatch {
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int64,
            false,
        )])),
        vec![Arc::new(Int64Array::from(vec![7]))],
    )
    .unwrap()
}

async fn version(uri: &str) -> String {
    Dataset::open(uri)
        .await
        .unwrap()
        .manifest()
        .data_storage_format
        .lance_file_format()
        .to_manifest_string()
        .to_string()
}

async fn seed(uri: &str, data: RecordBatch, format: &str) {
    let reader = RecordBatchIterator::new(vec![Ok(data.clone())], data.schema());
    Dataset::write(
        reader,
        uri,
        Some(lance::dataset::WriteParams {
            data_storage_version: Some(format.parse().unwrap()),
            ..Default::default()
        }),
    )
    .await
    .unwrap();
}

#[tokio::test]
async fn lance_upgrade_new_primary_and_branch_datasets_use_2_1() {
    let dir = TempDir::new().unwrap();
    let base = dir.path().to_str().unwrap();
    let backend = LanceDbBackend::connect(base, None).await.unwrap();
    backend
        .create_table("created", vec![batch()])
        .await
        .unwrap();
    backend
        .write("appended", vec![batch()], WriteMode::Append)
        .await
        .unwrap();
    backend
        .write("overwritten", vec![batch()], WriteMode::Overwrite)
        .await
        .unwrap();
    for name in ["created", "appended", "overwritten"] {
        assert_eq!(version(&format!("{base}/{name}.lance")).await, "2.1");
    }
    let uri = format!("{base}/branched.lance");
    let data = batch();
    let reader = RecordBatchIterator::new(vec![Ok(data.clone())], data.schema());
    lance_branch::create_dataset_then_branch(&uri, "scenario", reader)
        .await
        .unwrap();
    assert_eq!(version(&uri).await, "2.1");
    let branch = lance_branch::open_branch(&uri, "scenario").await.unwrap();
    assert_eq!(
        branch
            .manifest()
            .data_storage_format
            .lance_file_format()
            .to_manifest_string(),
        "2.1"
    );
}

#[tokio::test]
async fn lance_upgrade_primary_appends_and_overwrites_preserve_existing_format() {
    let dir = TempDir::new().unwrap();
    let base = dir.path().to_str().unwrap();
    let backend = LanceDbBackend::connect(base, None).await.unwrap();
    for format in ["2.0", "2.2"] {
        let name = format!("rows_{}", format.replace('.', "_"));
        let uri = format!("{base}/{name}.lance");
        seed(&uri, batch(), format).await;
        backend
            .write(&name, vec![batch()], WriteMode::Append)
            .await
            .unwrap();
        assert_eq!(version(&uri).await, format);
        assert_eq!(
            Dataset::open(&uri)
                .await
                .unwrap()
                .count_rows(None)
                .await
                .unwrap(),
            2
        );
        backend
            .write(&name, vec![batch()], WriteMode::Overwrite)
            .await
            .unwrap();
        assert_eq!(version(&uri).await, format);
        assert_eq!(
            Dataset::open(&uri)
                .await
                .unwrap()
                .count_rows(None)
                .await
                .unwrap(),
            1
        );
        backend
            .write(&name, vec![batch()], WriteMode::Append)
            .await
            .unwrap();
        backend
            .write(&name, vec![batch()], WriteMode::Append)
            .await
            .unwrap();
        let report = backend
            .optimize_table(&name, std::time::Duration::from_secs(7 * 86400))
            .await
            .unwrap();
        assert!(
            report.fragments_removed > 0,
            "premise: compaction rewrote data"
        );
        assert_eq!(version(&uri).await, format);
        assert_eq!(
            Dataset::open(&uri)
                .await
                .unwrap()
                .count_rows(None)
                .await
                .unwrap(),
            3
        );
    }
}

#[tokio::test]
async fn lance_upgrade_auxiliary_index_creation_and_append_preserve_format() {
    let dir = TempDir::new().unwrap();
    let base = dir.path().to_str().unwrap();
    let index = JsonPathIndex::new(base, "New", "name");
    index
        .write_entries(vec![("a".into(), vec![Vid::new(1)])])
        .await
        .unwrap();
    assert_eq!(
        index
            .open()
            .await
            .unwrap()
            .manifest()
            .data_storage_format
            .lance_file_format()
            .to_manifest_string(),
        "2.1"
    );
    for format in ["2.0", "2.2"] {
        let label = format!("Existing{}", format.replace('.', "_"));
        let uri = format!("{base}/indexes/idx_{label}_name");
        seed(
            &uri,
            RecordBatch::new_empty(JsonPathIndex::get_arrow_schema()),
            format,
        )
        .await;
        let index = JsonPathIndex::new(base, &label, "name");
        index
            .write_entries(vec![("a".into(), vec![Vid::new(2)])])
            .await
            .unwrap();
        assert_eq!(version(&uri).await, format);
        assert_eq!(index.get_vids("a").await.unwrap(), vec![Vid::new(2)]);
    }
}
