// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Tests for JsonPathIndex write/read roundtrip.

use tempfile::tempdir;
use uni_common::core::id::Vid;
use uni_store::storage::json_index::JsonPathIndex;

#[test]
fn test_json_index_schema() {
    let schema = JsonPathIndex::get_arrow_schema();
    assert_eq!(schema.fields().len(), 2);
    assert_eq!(schema.field(0).name(), "value");
    assert_eq!(schema.field(1).name(), "vids");
}

#[tokio::test]
async fn test_json_index_write_and_query() {
    let dir = tempdir().unwrap();
    let base_uri = dir.path().to_str().unwrap();
    let index = JsonPathIndex::new(base_uri, "Person", "$.name");

    // Write entries
    let entries = vec![
        ("Alice".to_string(), vec![Vid::from(1u64), Vid::from(2u64)]),
        ("Bob".to_string(), vec![Vid::from(3u64)]),
    ];
    index.write_entries(entries).await.unwrap();

    // Query for "Alice"
    let vids = index.get_vids("Alice").await.unwrap();
    assert_eq!(vids.len(), 2, "Alice should map to 2 VIDs");
    assert!(vids.contains(&Vid::from(1u64)));
    assert!(vids.contains(&Vid::from(2u64)));

    // Query for "Bob"
    let vids = index.get_vids("Bob").await.unwrap();
    assert_eq!(vids.len(), 1);
    assert!(vids.contains(&Vid::from(3u64)));
}

#[tokio::test]
async fn test_json_index_query_nonexistent() {
    let dir = tempdir().unwrap();
    let base_uri = dir.path().to_str().unwrap();
    let index = JsonPathIndex::new(base_uri, "Person", "$.name");

    // Write some data first
    index
        .write_entries(vec![("Alice".to_string(), vec![Vid::from(1u64)])])
        .await
        .unwrap();

    // Query for nonexistent value
    let vids = index.get_vids("Charlie").await.unwrap();
    assert!(vids.is_empty(), "Nonexistent value should return empty");
}

/// A value containing a single quote must round-trip. Before the fix, the
/// predicate was built as `value = 'O'Brien'`, which closes the string literal
/// after `O` and fails to parse — the lookup errored instead of matching.
#[tokio::test]
async fn test_json_index_value_with_single_quote() {
    let dir = tempdir().unwrap();
    let base_uri = dir.path().to_str().unwrap();
    let index = JsonPathIndex::new(base_uri, "Person", "$.name");

    index
        .write_entries(vec![
            ("O'Brien".to_string(), vec![Vid::from(7u64)]),
            ("d'Artagnan's".to_string(), vec![Vid::from(8u64)]),
            ("Plain".to_string(), vec![Vid::from(9u64)]),
        ])
        .await
        .unwrap();

    let vids = index.get_vids("O'Brien").await.unwrap();
    assert_eq!(vids, vec![Vid::from(7u64)], "single quote must be escaped");

    // Two quotes, one of them trailing before the closing delimiter.
    let vids = index.get_vids("d'Artagnan's").await.unwrap();
    assert_eq!(vids, vec![Vid::from(8u64)]);

    // A quote-bearing query must not match unrelated rows.
    let vids = index.get_vids("Plain'").await.unwrap();
    assert!(vids.is_empty(), "quote injection must not widen the match");
}

#[tokio::test]
async fn test_json_index_open_nonexistent() {
    let dir = tempdir().unwrap();
    let base_uri = dir.path().to_str().unwrap();
    let index = JsonPathIndex::new(base_uri, "X", "$.y");

    // get_vids on non-existent dataset should return empty (not error)
    let vids = index.get_vids("anything").await.unwrap();
    assert!(vids.is_empty());
}

/// A dataset that exists but cannot be opened must fail, not read as empty.
///
/// `get_vids` swallowed **every** `open()` error and returned no matches, so a
/// corrupt or unreadable index was indistinguishable from one holding nothing
/// (#233). Only an absent dataset may answer "empty"; the narrowing is what
/// makes the empty answer meaningful.
///
/// The fault is injected by creating the index directory with a `_versions`
/// entry that is not a manifest, so Lance finds something at the path and fails
/// to read it — a different error from the not-found this call may absorb.
#[tokio::test]
async fn a_corrupt_json_index_errors_rather_than_reading_as_empty() {
    let dir = tempdir().unwrap();
    let base_uri = dir.path().to_str().unwrap();
    let index = JsonPathIndex::new(base_uri, "X", "$.y");

    // Mirrors `JsonPathIndex::new`'s URI construction rather than guessing at
    // it: every non-alphanumeric character of the path becomes `_`.
    let safe_path: String = "$.y"
        .chars()
        .map(|c| if c.is_alphanumeric() { c } else { '_' })
        .collect();
    let idx_dir = dir
        .path()
        .join("indexes")
        .join(format!("idx_X_{safe_path}"));
    let versions = idx_dir.join("_versions");
    std::fs::create_dir_all(&versions).unwrap();
    std::fs::write(versions.join("1.manifest"), b"not a manifest").unwrap();

    let got = index.get_vids("anything").await;
    assert!(
        got.is_err(),
        "a present-but-unreadable index must not answer 'no matches', got {got:?}"
    );
}
