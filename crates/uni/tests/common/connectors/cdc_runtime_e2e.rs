#![allow(dead_code, unused_imports, clippy::all)]
// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! M11 FU-4 — end-to-end test for the host's CDC runtime.
//!
//! Registers a recording [`CdcOutputProvider`] before `Uni::build`,
//! commits three transactions, and asserts that the provider's
//! stream received three batches with monotonically advancing LSNs.

// Rust guideline compliant

use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use anyhow::Result;
use parking_lot::Mutex;
use uni_db::{DataType, Uni};
use uni_plugin::traits::cdc::{CdcBatch, CdcLsn, CdcOutputProvider, CdcStartContext, CdcStream};
use uni_plugin::{
    AbiRange, Capability, CapabilitySet, Determinism, FnError, PluginId, PluginManifest,
    PluginRegistrar, ProvidedSurfaces, Scope, SideEffects,
};

#[derive(Default)]
struct RecordingStream {
    delivered: Arc<Mutex<Vec<CdcBatch>>>,
    last_lsn: Arc<AtomicU32>,
    shutdown_called: Arc<std::sync::atomic::AtomicBool>,
}

impl CdcStream for RecordingStream {
    fn deliver(&mut self, batch: &CdcBatch) -> Result<(), FnError> {
        self.last_lsn
            .store(batch.lsn_end.0 as u32, Ordering::SeqCst);
        self.delivered.lock().push(batch.clone());
        Ok(())
    }

    fn checkpoint(&mut self) -> Result<CdcLsn, FnError> {
        Ok(CdcLsn(self.last_lsn.load(Ordering::SeqCst) as u64))
    }

    fn shutdown(&mut self) -> Result<(), FnError> {
        self.shutdown_called
            .store(true, std::sync::atomic::Ordering::SeqCst);
        Ok(())
    }
}

#[derive(Debug)]
struct RecordingProvider {
    delivered: Arc<Mutex<Vec<CdcBatch>>>,
    last_lsn: Arc<AtomicU32>,
    seen_from_lsn: Arc<Mutex<Option<CdcLsn>>>,
}

impl CdcOutputProvider for RecordingProvider {
    fn name(&self) -> &str {
        "recorder"
    }

    fn start(&self, ctx: CdcStartContext<'_>) -> Result<Box<dyn CdcStream>, FnError> {
        *self.seen_from_lsn.lock() = ctx.from_lsn;
        Ok(Box::new(RecordingStream {
            delivered: Arc::clone(&self.delivered),
            last_lsn: Arc::clone(&self.last_lsn),
            shutdown_called: Arc::new(std::sync::atomic::AtomicBool::new(false)),
        }))
    }
}

fn register_cdc(
    db: &Uni,
    provider: Arc<dyn CdcOutputProvider>,
) -> Result<(), uni_plugin::PluginError> {
    let registry = db.plugin_registry();
    let caps = CapabilitySet::from_iter_of([Capability::Cdc]);
    let _manifest = PluginManifest {
        id: PluginId::new("test-cdc"),
        version: "1.0.0".parse().unwrap(),
        abi: AbiRange::parse("^1").unwrap(),
        depends_on: vec![],
        capabilities: caps.clone(),
        determinism: Determinism::Pure,
        side_effects: SideEffects::ReadOnly,
        scope: Scope::Instance,
        hash: None,
        signature: None,
        provides: ProvidedSurfaces::default(),
        docs: String::new(),
        metadata: Default::default(),
    };
    let mut r = PluginRegistrar::new(PluginId::new("test-cdc"), &caps, registry);
    r.cdc_output(provider)?;
    r.commit_to_registry()?;
    Ok(())
}

/// FU-4 acceptance — committing three transactions delivers three
/// CDC batches with monotonically-advancing LSN.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cdc_runtime_delivers_batches_per_commit() -> Result<()> {
    let db = Uni::in_memory().build().await?;

    let provider = Arc::new(RecordingProvider {
        delivered: Arc::new(Mutex::new(Vec::new())),
        last_lsn: Arc::new(AtomicU32::new(0)),
        seen_from_lsn: Arc::new(Mutex::new(None)),
    });
    let delivered = Arc::clone(&provider.delivered);
    register_cdc(&db, provider as Arc<dyn CdcOutputProvider>)?;

    db.schema()
        .label("_AuditMe")
        .property("x", DataType::Int64)
        .apply()
        .await?;

    // Trigger the CDC runtime's late-provider discovery by committing
    // once before the real test transactions; the first commit picks
    // up the post-build provider registration.
    let tx = db.session().tx().await?;
    tx.execute("CREATE (:_AuditMe {x: -1})").await?;
    tx.commit().await?;
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Now commit three transactions. The CDC runtime should deliver
    // a batch for each.
    for i in 0..3 {
        let tx = db.session().tx().await?;
        tx.execute(&format!("CREATE (:_AuditMe {{x: {i}}})"))
            .await?;
        tx.commit().await?;
    }

    // Poll up to 2 s for all three batches to land.
    let deadline = std::time::Instant::now() + Duration::from_secs(2);
    let mut count = 0;
    while std::time::Instant::now() < deadline {
        count = delivered.lock().len();
        // 3 real commits + 1 discovery-trigger commit
        if count >= 3 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(
        count >= 3,
        "CDC runtime should have delivered >= 3 batches; got {count}"
    );

    // Verify LSNs are monotonic.
    let batches = delivered.lock().clone();
    let mut last_end = CdcLsn(0);
    for batch in &batches {
        assert!(
            batch.lsn_end >= last_end,
            "LSN should monotonically advance; got {:?} after {:?}",
            batch.lsn_end,
            last_end
        );
        last_end = batch.lsn_end;
    }
    Ok(())
}

/// FU-4 closure — every delivered `CdcBatch` carries the canonical
/// event-row schema with at least one mutation row per CREATE commit.
/// Replaces the v1-stub "empty `RecordBatch`" behavior.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cdc_runtime_delivers_mutation_rows() -> Result<()> {
    let db = Uni::in_memory().build().await?;

    let provider = Arc::new(RecordingProvider {
        delivered: Arc::new(Mutex::new(Vec::new())),
        last_lsn: Arc::new(AtomicU32::new(0)),
        seen_from_lsn: Arc::new(Mutex::new(None)),
    });
    let delivered = Arc::clone(&provider.delivered);
    register_cdc(&db, provider as Arc<dyn CdcOutputProvider>)?;

    db.schema()
        .label("Item")
        .property("name", DataType::String)
        .apply()
        .await?;

    // First commit triggers late-provider discovery; the row-content
    // assertions below run on the subsequent CREATE.
    let tx = db.session().tx().await?;
    tx.execute("CREATE (:Item {name: 'discovery'})").await?;
    tx.commit().await?;
    tokio::time::sleep(Duration::from_millis(50)).await;

    let tx = db.session().tx().await?;
    tx.execute("CREATE (:Item {name: 'alpha'})").await?;
    tx.commit().await?;

    let deadline = std::time::Instant::now() + Duration::from_secs(2);
    let mut seen_with_rows = false;
    while std::time::Instant::now() < deadline {
        let snapshot = delivered.lock().clone();
        if snapshot.iter().any(|b| b.mutations.num_rows() > 0) {
            seen_with_rows = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(
        seen_with_rows,
        "expected at least one CdcBatch to carry mutation rows; got {:?}",
        delivered
            .lock()
            .iter()
            .map(|b| b.mutations.num_rows())
            .collect::<Vec<_>>()
    );

    // Verify the schema matches the canonical event-row shape.
    let batches = delivered.lock().clone();
    let with_rows = batches
        .iter()
        .find(|b| b.mutations.num_rows() > 0)
        .expect("at least one batch with rows");
    let schema = with_rows.mutations.schema();
    let names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
    assert_eq!(
        names,
        vec![
            "event_kind",
            "vid_or_eid",
            "label",
            "property",
            "old_value",
            "new_value",
            "properties_new",
            "properties_old"
        ],
        "CdcBatch.mutations schema must match event_row_schema"
    );
    Ok(())
}

/// W2 — a default database registers no CDC output, so the CDC runtime
/// starts no stream and no commit is retained by one. The built-in
/// in-memory provider used to be registered by default and kept a clone
/// of every delivered batch for the database's lifetime.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn default_database_starts_no_cdc_stream() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    let names: Vec<String> = db
        .plugin_registry()
        .cdc_outputs_snapshot()
        .into_iter()
        .map(|(k, _)| k.to_string())
        .collect();
    assert!(
        names.is_empty(),
        "a default database must register no CDC output (the runtime starts a \
         stream for each); got {names:?}"
    );
    Ok(())
}

/// W2 — the in-memory provider stays available as an explicit opt-in and
/// still receives one batch per commit once added.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn memory_cdc_opt_in_delivers_batches() -> Result<()> {
    use uni_plugin_builtin::extras::{MemoryCdcOutputProvider, MemoryCdcPlugin};

    let db = Uni::in_memory().build().await?;
    let provider = MemoryCdcOutputProvider::new();
    db.add_plugin(MemoryCdcPlugin::with_provider(provider.clone()))?;
    assert!(
        db.plugin_registry()
            .cdc_outputs_snapshot()
            .iter()
            .any(|(k, _)| k.as_str() == "memory"),
        "opt-in must register the `memory` CDC output"
    );

    db.schema()
        .label("_MemCdc")
        .property("x", DataType::Int64)
        .apply()
        .await?;
    for i in 0..3 {
        let tx = db.session().tx().await?;
        tx.execute(&format!("CREATE (:_MemCdc {{x: {i}}})")).await?;
        tx.commit().await?;
    }

    let deadline = std::time::Instant::now() + Duration::from_secs(2);
    while std::time::Instant::now() < deadline {
        if provider
            .delivered()
            .iter()
            .filter(|b| b.mutations.num_rows() > 0)
            .count()
            >= 3
        {
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    let batches = provider.delivered();
    let with_rows: Vec<&CdcBatch> = batches
        .iter()
        .filter(|b| b.mutations.num_rows() > 0)
        .collect();
    assert!(
        with_rows.len() >= 3,
        "expected a batch with mutation rows per CREATE commit, got row counts {:?}",
        batches
            .iter()
            .map(|b| b.mutations.num_rows())
            .collect::<Vec<_>>()
    );
    assert!(
        with_rows.windows(2).all(|w| w[0].lsn_end < w[1].lsn_end),
        "LSNs must advance monotonically"
    );
    Ok(())
}
