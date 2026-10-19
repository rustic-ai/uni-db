// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Probe: can Lance project a top-level column whose name contains a backtick?
//!
//! `Scanner::project` escapes each name into a field path
//! (`escape_field_path_for_project`: ``has`tick`` becomes `` `has``tick` ``),
//! parses it as SQL, and collects the referenced column names to load. The
//! collector (`ColumnCapturingVisitor` in lance-datafusion's `planner.rs`)
//! quotes nested path segments but not the root, so the unescaped ``has`tick``
//! reaches `Projection::union_columns`, whose field-path parser rejects a
//! bare backtick; with `OnMissing::Ignore` the column is silently not loaded
//! and the final projection fails with `No field named "has`tick"`.
//!
//! This is why `validate_property_name` refuses a backtick in a *declared*
//! property name. If this probe starts returning the column, the refusal can
//! be lifted.

use std::sync::Arc;

use arrow_array::{RecordBatch, RecordBatchIterator, StringArray};
use arrow_schema::{DataType, Field, Schema as ArrowSchema};
use futures::TryStreamExt;
use lance::Dataset;
use tempfile::TempDir;

#[tokio::test]
async fn lance_cannot_project_a_column_named_with_a_backtick() {
    let dir = TempDir::new().unwrap();
    let uri = dir.path().join("t.lance");
    let uri = uri.to_str().unwrap();
    let schema = Arc::new(ArrowSchema::new(vec![
        Field::new("plain", DataType::Utf8, true),
        Field::new("has`tick", DataType::Utf8, true),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(vec!["p"])),
            Arc::new(StringArray::from(vec!["t"])),
        ],
    )
    .unwrap();
    let reader = RecordBatchIterator::new(vec![Ok(batch)], schema);
    let ds = Dataset::write(reader, uri, None).await.unwrap();

    // The column exists and a plain projection reads it.
    assert!(ds.schema().fields.iter().any(|f| f.name == "has`tick"));
    let mut ok = ds.scan();
    ok.project(&["plain"]).unwrap();
    let rows: Vec<RecordBatch> = ok
        .try_into_stream()
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    assert_eq!(rows[0].num_rows(), 1);

    // Projecting it by name fails.
    let mut sc = ds.scan();
    let outcome = match sc.project(&["has`tick"]) {
        Err(e) => Err(e.to_string()),
        Ok(sc) => match sc.try_into_stream().await {
            Err(e) => Err(e.to_string()),
            Ok(s) => s
                .try_collect::<Vec<RecordBatch>>()
                .await
                .map_err(|e| e.to_string()),
        },
    };
    let err = outcome.expect_err(
        "Lance now projects a backtick-named column: lift the refusal in validate_property_name",
    );
    assert!(
        err.contains("No field named \"has`tick\""),
        "unexpected error: {err}"
    );
}
