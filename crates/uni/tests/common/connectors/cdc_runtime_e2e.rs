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

// ---------------------------------------------------------------------------
// W2 residual: is the per-commit memory growth on the default path L0 or a
// leak? An observation probe, not a guard.
// ---------------------------------------------------------------------------

/// glibc's `struct mallinfo2` (glibc >= 2.33), field order as in `malloc.h`.
#[cfg(all(target_os = "linux", target_env = "gnu"))]
#[repr(C)]
#[derive(Default)]
struct MallInfo2 {
    arena: usize,
    ordblks: usize,
    smblks: usize,
    hblks: usize,
    hblkhd: usize,
    usmblks: usize,
    fsmblks: usize,
    uordblks: usize,
    fordblks: usize,
    keepcost: usize,
}

#[cfg(all(target_os = "linux", target_env = "gnu"))]
unsafe extern "C" {
    fn mallinfo2() -> MallInfo2;
    fn malloc_trim(pad: usize) -> i32;
}

/// Bytes the allocator holds from the OS but has not handed out (free
/// chunks in every arena): the part of RSS that is no live object.
#[cfg(all(target_os = "linux", target_env = "gnu"))]
fn allocator_free_bytes() -> usize {
    // SAFETY: as in `live_heap_bytes`.
    unsafe { mallinfo2() }.fordblks
}

#[cfg(not(all(target_os = "linux", target_env = "gnu")))]
fn allocator_free_bytes() -> usize {
    0
}

/// Returns free arena memory to the OS (`malloc_trim(0)`).
#[cfg(all(target_os = "linux", target_env = "gnu"))]
fn trim_allocator() {
    // SAFETY: `malloc_trim` only releases free memory; it has no
    // preconditions and touches no live allocation.
    unsafe {
        malloc_trim(0);
    }
}

#[cfg(not(all(target_os = "linux", target_env = "gnu")))]
fn trim_allocator() {}

/// Live heap bytes: in-use `brk`/arena chunks plus `mmap`ed chunks.
///
/// RSS is a poor leak signal under glibc: freed memory is not returned to the
/// OS, so RSS only ratchets. `mallinfo2` sums every arena and counts what is
/// still allocated, so it falls when L0 is flushed and freed.
#[cfg(all(target_os = "linux", target_env = "gnu"))]
fn live_heap_bytes() -> usize {
    // SAFETY: `mallinfo2` takes no arguments, has no preconditions, and
    // returns a plain `repr(C)` struct by value (glibc >= 2.33, which every
    // supported Linux dev/CI image has).
    let mi = unsafe { mallinfo2() };
    mi.uordblks + mi.hblkhd
}

#[cfg(not(all(target_os = "linux", target_env = "gnu")))]
fn live_heap_bytes() -> usize {
    0
}

/// Resident set size in bytes, from `/proc/self/status` (0 if unavailable).
fn rss_bytes() -> usize {
    std::fs::read_to_string("/proc/self/status")
        .ok()
        .and_then(|s| {
            s.lines()
                .find_map(|l| l.strip_prefix("VmRSS:"))
                .and_then(|v| v.trim().trim_end_matches("kB").trim().parse::<usize>().ok())
        })
        .map_or(0, |kb| kb * 1024)
}

/// One commit-loop regime for [`w2_residual_memory_growth_observation`].
struct W2Regime {
    /// Printed with every sample.
    label: String,
    /// Auto-flush settings; `no_auto` disables both triggers.
    config: uni_db::UniConfig,
    /// Use the in-memory object store instead of a temp directory.
    in_memory: bool,
    /// Explicit `flush()` period in commits; 0 never flushes explicitly.
    flush_every: usize,
    /// Rewrite one pre-created vertex per commit instead of creating one, so
    /// the stored data stays one row: growth that survives a flush is then
    /// per-commit or per-flush retention, not per-row state.
    update: bool,
}

/// Runs `regime` up to the last checkpoint, printing live heap and RSS deltas
/// (and the heap slope since the previous checkpoint) at each one.
async fn w2_commit_loop(regime: W2Regime, checkpoints: &[usize]) -> Result<()> {
    let W2Regime {
        label,
        config,
        in_memory,
        flush_every,
        update,
    } = regime;
    // `W2_LANCE_CACHE` (bytes) caps both Lance session caches in every regime.
    let lance_cap = std::env::var("W2_LANCE_CACHE")
        .ok()
        .and_then(|v| v.parse::<usize>().ok());
    let config = uni_db::UniConfig {
        lance_metadata_cache_bytes: lance_cap.or(config.lance_metadata_cache_bytes),
        lance_index_cache_bytes: lance_cap.or(config.lance_index_cache_bytes),
        ..config
    };
    let label = match lance_cap {
        Some(cap) => format!("{label}+lance_cap={cap}"),
        None => label,
    };
    let dir = tempfile::tempdir()?;
    let builder = if in_memory {
        Uni::in_memory()
    } else {
        Uni::open(dir.path().to_string_lossy().to_string())
    };
    let db = builder.config(config).build().await?;
    db.schema()
        .label("W")
        .property("x", DataType::Int64)
        .apply()
        .await?;
    if update {
        let tx = db.session().tx().await?;
        tx.execute("CREATE (:W {x: 0})").await?;
        tx.commit().await?;
    }
    db.flush().await?;
    let heap0 = live_heap_bytes();
    let rss0 = rss_bytes();
    eprintln!("[w2] {label}: start heap={heap0} rss={rss0}");
    let session = db.session();
    let mut prev = (0usize, heap0);
    let total = *checkpoints.last().unwrap_or(&0);
    for i in 1..=total {
        let tx = session.tx().await?;
        let stmt = if update {
            format!("MATCH (w:W) SET w.x = {i}")
        } else {
            format!("CREATE (:W {{x: {i}}})")
        };
        tx.execute(&stmt).await?;
        tx.commit().await?;
        if flush_every > 0 && i % flush_every == 0 {
            db.flush().await?;
        }
        if checkpoints.contains(&i) {
            let heap = live_heap_bytes();
            let rss = rss_bytes();
            let slope = (heap as f64 - prev.1 as f64) / (i - prev.0) as f64;
            eprintln!(
                "[w2] {label}: commits={i:>6} heap_delta={:>8.1} MB rss_delta={:>8.1} MB \
                 heap B/commit since last={slope:>8.1}",
                (heap as f64 - heap0 as f64) / 1e6,
                (rss as f64 - rss0 as f64) / 1e6,
            );
            prev = (i, heap);
        }
    }
    db.flush().await?;
    let heap = live_heap_bytes();
    let rss = rss_bytes();
    let metrics = db.metrics();
    eprintln!(
        "[w2] {label}: after final flush heap_delta={:.1} MB rss_delta={:.1} MB \
         allocator_free={:.1} MB lance_cache metadata={} B index={} B",
        (heap as f64 - heap0 as f64) / 1e6,
        (rss as f64 - rss0 as f64) / 1e6,
        allocator_free_bytes() as f64 / 1e6,
        metrics.storage_metadata_cache_bytes,
        metrics.storage_index_cache_bytes,
    );
    trim_allocator();
    eprintln!(
        "[w2] {label}: after malloc_trim rss_delta={:.1} MB",
        (rss_bytes() as f64 - rss0 as f64) / 1e6
    );
    drop(session);
    drop(db);
    Ok(())
}

/// W2 residual — sizes the memory growth of small commits on the default
/// (no-CDC) path, so L0 growth (freed by a flush) can be told apart from
/// retention (survives one).
///
/// Measured 2026-10-17 (debug build, 20k commits, live heap from
/// `mallinfo2`, not RSS):
/// - RSS is not the signal. Default config: RSS +230.5 MB, live heap
///   +23.5 MB, 216.8 MB of free chunks held by glibc.
/// - L0: 0.95 KB/commit with no flush, freed by the flush.
/// - Per commit: one rewritten vertex, never flushed, grows ~2.7 KB/commit
///   to ~1.2k commits, ~250 B/commit to ~5k, then 4 B/commit through 20k: a
///   plateau of ~4.2 MB, so bounded, not a leak. The ~5k knee matches the OCC
///   `CommitRegistry` capacity (4096); that attribution is inferred, not
///   counted.
/// - Per flush: ~100-170 KB that no flush frees (rewrite mode, flush every
///   250/1000/5000 commits: 420/129/33 B/commit). Capping the Lance session
///   caches at 1 MB cut it from 402 to 57 B/commit, so ~86% is Lance's
///   metadata/index cache, an LRU bounded at 1 GiB + 6 GiB per database.
///
/// Run: `cargo nextest run -p uni-db --test integration --run-ignored all
/// -E 'test(w2_residual_memory_growth_observation)' --no-capture` (debug
/// builds outlast nextest's 180 s ceiling at 20k commits; run the test binary
/// directly with `--ignored --nocapture --exact` for those). Environment:
/// `W2_COMMITS` (default 40000), `W2_ONLY` (one regime name, or `custom` with
/// `W2_FLUSH`, `W2_UPDATE=1`, `W2_MEM=1`), `W2_LANCE_CACHE` (bytes; sets
/// `UniConfig::lance_{metadata,index}_cache_bytes` in every regime).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "observation; run with --run-ignored all --no-capture"]
async fn w2_residual_memory_growth_observation() -> Result<()> {
    let n: usize = std::env::var("W2_COMMITS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(40_000);
    let checkpoints: Vec<usize> = [n / 16, n / 8, n / 4, n / 2, n].into();
    let no_auto = || uni_db::UniConfig {
        auto_flush_interval: None,
        auto_flush_threshold: usize::MAX / 2,
        ..Default::default()
    };
    let env = |k: &str| std::env::var(k).ok().and_then(|v| v.parse::<usize>().ok());
    let regime = |label: &str, config, in_memory, flush_every, update| W2Regime {
        label: label.to_string(),
        config,
        in_memory,
        flush_every,
        update,
    };
    let regimes = [
        regime(
            "default_disk",
            uni_db::UniConfig::default(),
            false,
            0,
            false,
        ),
        regime("noflush_disk", no_auto(), false, 0, false),
        regime("flush1k_disk", no_auto(), false, 1000, false),
        regime("update_flush1k_disk", no_auto(), false, 1000, true),
        regime("flush1k_mem", no_auto(), true, 1000, false),
        // Free-form, so a new data point needs no rebuild.
        regime(
            "custom",
            no_auto(),
            env("W2_MEM") == Some(1),
            env("W2_FLUSH").unwrap_or(0),
            env("W2_UPDATE") == Some(1),
        ),
    ];
    let only = std::env::var("W2_ONLY").unwrap_or_default();
    for r in regimes {
        let selected = if r.label == "custom" {
            only == "custom"
        } else {
            only.is_empty() || only == r.label
        };
        if selected {
            w2_commit_loop(r, &checkpoints).await?;
        }
    }
    Ok(())
}
