// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! A failed storage scan fails the property read; it is never an empty answer.
//!
//! Six property reads in `PropertyManager` swallowed a scan error —
//! `Err(_) => continue`, a `warn!` then `continue`, or `return Ok(partial)` —
//! and answered as though the table held no row for the entity. A property
//! that exists on disk came back missing, and nothing told the caller the
//! answer was partial. `table_exists(..).unwrap_or(false)` did the same for a
//! failed existence check.
//!
//! Each test flushes a vertex and an edge with properties to storage, checks
//! that the read finds them (so the failing read below is not vacuous), then
//! injects a scan failure at `property_manager::scan` and requires an error.
//! Each test owns the failpoint and runs in its own process under nextest.
//!
//! Run with: `cargo nextest run -p uni-store --features failpoints property_scan_failure`.

#![cfg(feature = "failpoints")]

// Rust guideline compliant

use std::collections::HashMap;
use std::sync::Arc;

use anyhow::Result;
use object_store::local::LocalFileSystem;
use object_store::path::Path as ObjectStorePath;
use tempfile::TempDir;
use uni_common::Value;
use uni_common::config::UniConfig;
use uni_common::core::id::{Eid, Vid};
use uni_common::core::schema::{DataType, SchemaManager};
use uni_store::runtime::property_manager::PropertyManager;
use uni_store::runtime::writer::Writer;
use uni_store::storage::manager::StorageManager;

const FAILPOINT: &str = "property_manager::scan";

struct Fixture {
    pm: PropertyManager,
    vid: Vid,
    eid: Eid,
    _dir: TempDir,
}

/// A flushed `Person {name: 'ann'}` vertex and a flushed `KNOWS {w: 7}` edge,
/// read through a fresh `PropertyManager` with no cache, so every read goes
/// to storage.
async fn fixture() -> Result<Fixture> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().to_str().unwrap();
    let store = Arc::new(LocalFileSystem::new_with_prefix(dir.path())?);
    let schema_manager = Arc::new(
        SchemaManager::load_from_store(store, &ObjectStorePath::from("schema.json")).await?,
    );
    schema_manager.add_label("Person")?;
    schema_manager.add_property("Person", "name", DataType::String, true)?;
    let knows = schema_manager.add_edge_type(
        "KNOWS",
        vec!["Person".to_string()],
        vec!["Person".to_string()],
    )?;
    schema_manager.add_property("KNOWS", "w", DataType::Int64, true)?;
    schema_manager.save().await?;
    let storage = Arc::new(StorageManager::new(path, schema_manager.clone()).await?);
    let writer = Writer::new_with_config(
        storage.clone(),
        schema_manager.clone(),
        1,
        UniConfig::default(),
        None,
        None,
        None,
    )
    .await?;

    let label = ["Person".to_string()];
    let vid = writer.next_vid().await?;
    let other = writer.next_vid().await?;
    let props = HashMap::from([("name".to_string(), Value::String("ann".into()))]);
    writer
        .insert_vertex_with_labels(vid, props, &label, None)
        .await?;
    writer
        .insert_vertex_with_labels(other, HashMap::new(), &label, None)
        .await?;
    let eid = writer.next_eid(knows).await?;
    let edge_props = HashMap::from([("w".to_string(), Value::Int(7))]);
    writer
        .insert_edge(
            vid,
            other,
            knows,
            eid,
            edge_props,
            Some("KNOWS".into()),
            None,
        )
        .await?;
    writer.flush_to_l1(None).await?;

    let pm = PropertyManager::new(storage, schema_manager, 0);
    Ok(Fixture {
        pm,
        vid,
        eid,
        _dir: dir,
    })
}

/// `get_all_vertex_props` (`fetch_all_props_from_storage`).
#[tokio::test]
async fn all_vertex_props_scan_failure_is_an_error() -> Result<()> {
    let f = fixture().await?;
    let props = f.pm.get_all_vertex_props(f.vid).await?;
    assert_eq!(props.get("name"), Some(&Value::String("ann".into())));

    fail::cfg(FAILPOINT, "return").unwrap();
    let got = f.pm.get_all_vertex_props(f.vid).await;
    fail::cfg(FAILPOINT, "off").unwrap();
    assert!(got.is_err(), "a failed scan answered {got:?}");
    Ok(())
}

/// `get_vertex_prop` (the single-property storage read).
#[tokio::test]
async fn vertex_prop_scan_failure_is_an_error() -> Result<()> {
    let f = fixture().await?;
    assert_eq!(
        f.pm.get_vertex_prop(f.vid, "name").await?,
        Value::String("ann".into())
    );

    fail::cfg(FAILPOINT, "return").unwrap();
    let got = f.pm.get_vertex_prop(f.vid, "name").await;
    fail::cfg(FAILPOINT, "off").unwrap();
    assert!(got.is_err(), "a failed scan answered {got:?}");
    Ok(())
}

/// `get_batch_vertex_props` (the projected batch read).
#[tokio::test]
async fn batch_vertex_props_scan_failure_is_an_error() -> Result<()> {
    let f = fixture().await?;
    let got =
        f.pm.get_batch_vertex_props(&[f.vid], &["name"], None)
            .await?;
    assert_eq!(
        got.get(&f.vid).and_then(|p| p.get("name")),
        Some(&Value::String("ann".into()))
    );

    fail::cfg(FAILPOINT, "return").unwrap();
    let got = f.pm.get_batch_vertex_props(&[f.vid], &["name"], None).await;
    fail::cfg(FAILPOINT, "off").unwrap();
    assert!(got.is_err(), "a failed scan answered {got:?}");
    Ok(())
}

/// `get_all_edge_props_with_ctx` (`fetch_all_edge_props_from_storage`).
#[tokio::test]
async fn all_edge_props_scan_failure_is_an_error() -> Result<()> {
    let f = fixture().await?;
    let props = f.pm.get_all_edge_props_with_ctx(f.eid, None).await?;
    assert_eq!(props.and_then(|p| p.get("w").cloned()), Some(Value::Int(7)));

    fail::cfg(FAILPOINT, "return").unwrap();
    let got = f.pm.get_all_edge_props_with_ctx(f.eid, None).await;
    fail::cfg(FAILPOINT, "off").unwrap();
    assert!(got.is_err(), "a failed scan answered {got:?}");
    Ok(())
}

/// `get_batch_edge_props` (the projected batch read).
#[tokio::test]
async fn batch_edge_props_scan_failure_is_an_error() -> Result<()> {
    let f = fixture().await?;
    let got =
        f.pm.get_batch_edge_props(&[f.eid], &["w"], None, None)
            .await?;
    assert!(
        got.values().any(|p| p.get("w") == Some(&Value::Int(7))),
        "premise: the batch read finds the edge, got {got:?}"
    );

    fail::cfg(FAILPOINT, "return").unwrap();
    let got =
        f.pm.get_batch_edge_props(&[f.eid], &["w"], None, None)
            .await;
    fail::cfg(FAILPOINT, "off").unwrap();
    assert!(got.is_err(), "a failed scan answered {got:?}");
    Ok(())
}

/// `get_batch_edge_props_for_type` (the SET prefetch read). Its swallow
/// claimed the per-row fallback kept the answer right; that fallback swallowed
/// the same error.
#[tokio::test]
async fn batch_edge_props_for_type_scan_failure_is_an_error() -> Result<()> {
    let f = fixture().await?;
    let got =
        f.pm.get_batch_edge_props_for_type(&[f.eid], "KNOWS", None)
            .await?;
    assert_eq!(
        got.get(&f.eid).and_then(|p| p.get("w")),
        Some(&Value::Int(7))
    );

    fail::cfg(FAILPOINT, "return").unwrap();
    let got =
        f.pm.get_batch_edge_props_for_type(&[f.eid], "KNOWS", None)
            .await;
    fail::cfg(FAILPOINT, "off").unwrap();
    assert!(got.is_err(), "a failed scan answered {got:?}");
    Ok(())
}
