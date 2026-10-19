// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! `PropertyManager::get_batch_vertex_props` used to scan *every declared
//! label* as soon as a single vid in the batch failed to resolve. FIXED; the
//! tests below guard the fix.
//!
//! The label set to scan is chosen in `get_batch_vertex_props` (the block that
//! builds `labels_to_scan`): each vid was looked up in the `VidLabelsIndex`,
//! and the first one that missed `break`s out and fell back to
//! `schema.labels.keys()` — "Fallback to full scan". The escalation was
//! all-or-nothing and batch-wide: one unresolved vid among a hundred resolved
//! ones cost the whole batch a fan-out across every label.
//!
//! # What makes a vid unresolved
//!
//! `update_vid_labels_index` has exactly one production caller in `writer.rs`,
//! inside `flush_to_l1`, and the startup rebuild reads the
//! *main vertex table*. So the index describes flushed vertices only, and a
//! vertex that has been written but not yet flushed is absent from it — which
//! makes the ordinary insert-then-read sequence the trigger, not an edge case.
//!
//! This is the vertex-side sibling of #222, but it is **not** the same trigger:
//! #222's edge fan-out fires when L0 is *cold*, this one fires when L0 is *hot*.
//!
//! # These asserted the defect, and now assert the fix
//!
//! They were written `#[ignore]`d, pinning the *defective* counts so a fix
//! could land without appearing to break CI, with a note that the fix should
//! update the expected counts rather than delete the tests. That is what
//! happened: `get_batch_vertex_props` now consults L0 for the labels the index
//! is missing, so the counts below are the ones the batch actually needs and
//! the tests gate the default run.
//!
//! Measured across the change, on the 8-label fixture:
//!
//! | batch | before | after |
//! |---|---|---|
//! | 1 resolved vid | 1 | 1 |
//! | 1 unflushed vid | 8 | **1** |
//! | 2 resolved + 1 unflushed | 8 | **2** |
//!
//! The third row is the one that shows the `break` mattered: the two resolved
//! vids used to be dragged to the full fan-out by the third.
//!
//! # Why the assertion is a contrast, not a threshold
//!
//! The answers are identical either way — the unflushed vertex is served from
//! the L0 overlay regardless of how many label datasets were scanned — so no
//! correctness test can distinguish the two shapes and the guard has to be a
//! count. A bare "scans == LABELS" would also pass if the counter broke and both
//! arms went to zero, so each test below runs the resolved arm as its control
//! and compares.

use anyhow::Result;
use object_store::local::LocalFileSystem;
use object_store::path::Path as ObjectStorePath;
use std::collections::HashMap;
use std::sync::Arc;
use tempfile::{TempDir, tempdir};
use uni_common::Value;
use uni_common::core::id::Vid;
use uni_common::core::schema::{DataType, SchemaManager};
use uni_store::QueryCounters;
use uni_store::runtime::Writer;
use uni_store::runtime::context::QueryContext;
use uni_store::runtime::property_manager::PropertyManager;
use uni_store::storage::manager::StorageManager;

/// Enough declared labels that a fan-out is unmistakable against a targeted
/// read, and few enough to stay fast in CI.
const LABELS: usize = 8;

fn label_name(i: usize) -> String {
    format!("Label{i}")
}

struct Fixture {
    _tmp: TempDir,
    storage: Arc<StorageManager>,
    schema_manager: Arc<SchemaManager>,
    writer: Writer,
    /// One flushed vertex per label, so every label dataset exists and holds a
    /// row. Without that, scanning a label would be free and the contrast below
    /// would measure nothing.
    flushed: Vec<Vid>,
}

async fn fixture() -> Result<Fixture> {
    let tmp = tempdir()?;
    let path = tmp.path().to_str().unwrap();
    let store = Arc::new(LocalFileSystem::new_with_prefix(tmp.path())?);

    let schema_manager = Arc::new(
        SchemaManager::load_from_store(store, &ObjectStorePath::from("schema.json")).await?,
    );
    for i in 0..LABELS {
        schema_manager.add_label(&label_name(i))?;
        schema_manager.add_property(&label_name(i), "name", DataType::String, true)?;
    }
    schema_manager.save().await?;

    let storage = Arc::new(StorageManager::new(path, schema_manager.clone()).await?);
    let writer = Writer::new(storage.clone(), schema_manager.clone(), 1).await?;

    let mut flushed = Vec::with_capacity(LABELS);
    for i in 0..LABELS {
        let vid = writer.next_vid().await?;
        let mut props = HashMap::new();
        props.insert("name".to_string(), Value::String(format!("flushed-{i}")));
        writer
            .insert_vertex_with_labels(vid, props, &[label_name(i)], None)
            .await?;
        flushed.push(vid);
    }

    // Populates the VidLabelsIndex for exactly these vids, and materializes the
    // per-label Lance datasets the fan-out below pays for.
    writer.flush_to_l1(None).await?;

    Ok(Fixture {
        _tmp: tmp,
        storage,
        schema_manager,
        writer,
        flushed,
    })
}

/// Reads `name` for `vids` and returns (answers, scans reported).
async fn read(f: &Fixture, vids: &[Vid]) -> Result<(HashMap<Vid, Value>, u64)> {
    let counters = Arc::new(QueryCounters::new());
    let mut ctx = QueryContext::new(f.writer.l0_manager.get_current());
    ctx.set_counters(counters.clone());

    let pm = PropertyManager::new(f.storage.clone(), f.schema_manager.clone(), 0);
    let props = pm
        .get_batch_vertex_props(vids, &["name"], Some(&ctx))
        .await?;

    let answers = props
        .into_iter()
        .filter_map(|(vid, p)| p.get("name").cloned().map(|v| (vid, v)))
        .collect();
    Ok((answers, counters.scans_reported()))
}

/// The control: every vid resolves, so only the labels actually involved are
/// scanned.
#[tokio::test]
async fn a_resolved_batch_scans_only_the_labels_it_needs() -> Result<()> {
    let f = fixture().await?;

    let (answers, scans) = read(&f, &f.flushed[..1]).await?;

    assert_eq!(answers.len(), 1, "the fixture did not resolve its vertex");
    assert!(
        scans > 0,
        "a resolved read of a flushed vertex reported zero scans, so this file \
         measures nothing and every contrast below is vacuous"
    );
    eprintln!("resolved single-label read: {scans} scans across {LABELS} declared labels");
    assert!(
        scans < LABELS as u64,
        "a read of one vertex of one label reported {scans} scans against \
         {LABELS} declared labels — it is already fanning out, so the control \
         cannot serve as a baseline"
    );
    Ok(())
}

/// One unflushed vid costs what a resolved one does, because L0 supplies its
/// label. With the L0 consultation removed it falls back to every declared
/// label (8 scans against the control's 1) and the equality fails.
#[tokio::test]
async fn one_unflushed_vid_fans_the_batch_out_across_every_label() -> Result<()> {
    let f = fixture().await?;

    // Written but deliberately not flushed, so it is absent from the
    // VidLabelsIndex — the ordinary insert-then-read sequence.
    let hot = f.writer.next_vid().await?;
    let mut props = HashMap::new();
    props.insert("name".to_string(), Value::String("unflushed".to_string()));
    f.writer
        .insert_vertex_with_labels(hot, props, &[label_name(0)], None)
        .await?;

    let (control_answers, control_scans) = read(&f, &f.flushed[..1]).await?;
    let (hot_answers, hot_scans) = read(&f, &[hot]).await?;

    // The premise: the unflushed vertex is answered correctly anyway, from L0.
    // If this fails, the scan counts are comparing two different reads.
    assert_eq!(
        hot_answers.get(&hot),
        Some(&Value::String("unflushed".to_string())),
        "the unflushed vertex was not readable at all, so the scan count below \
         is not measuring a successful read"
    );
    assert_eq!(control_answers.len(), 1);

    eprintln!(
        "resolved vid: {control_scans} scans; unflushed vid: {hot_scans} scans; \
         {LABELS} labels declared"
    );

    assert_eq!(
        hot_scans, control_scans,
        "an unflushed vid must now cost what a resolved one costs \
         ({control_scans} scans), because L0 knows its label — it wrote it. \
         Got {hot_scans} against {LABELS} declared labels; equal to {LABELS} \
         means the L0 consultation stopped firing and the fan-out is back."
    );
    Ok(())
}

/// The sharpest form: the escalation was batch-wide. A single unresolved vid
/// made every *resolved* vid in the same call pay the fan-out, because the
/// resolution loop `break`s rather than collecting what it knows. With the L0
/// consultation reverted, the mixed batch scans all 8 labels against the
/// resolved batch's 2.
#[tokio::test]
async fn a_single_unresolved_vid_escalates_an_otherwise_resolved_batch() -> Result<()> {
    let f = fixture().await?;

    let hot = f.writer.next_vid().await?;
    let mut props = HashMap::new();
    props.insert("name".to_string(), Value::String("unflushed".to_string()));
    f.writer
        .insert_vertex_with_labels(hot, props, &[label_name(0)], None)
        .await?;

    let (_, resolved_scans) = read(&f, &f.flushed[..2]).await?;

    let mut mixed: Vec<Vid> = f.flushed[..2].to_vec();
    mixed.push(hot);
    let (mixed_answers, mixed_scans) = read(&f, &mixed).await?;

    assert_eq!(
        mixed_answers.len(),
        3,
        "the mixed batch lost a vertex, so its scan count is not comparable"
    );
    eprintln!(
        "two resolved vids: {resolved_scans} scans; the same two plus one \
         unflushed: {mixed_scans} scans"
    );

    assert_eq!(
        mixed_scans, resolved_scans,
        "adding one unflushed vid to a resolved batch must not change what it \
         scans: the unflushed vertex shares label L0 with them, so the batch \
         still needs exactly {resolved_scans} labels. Got {mixed_scans}"
    );
    Ok(())
}
