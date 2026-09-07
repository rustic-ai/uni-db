// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Runnable repros for verified correctness findings in `uni-fork/src/diff.rs`.
//!
//! Each test drives the REAL public engine (`compute_diff` / `run_promote`) or
//! the REAL content-UID function (`VertexDataset::compute_vertex_uid`) through
//! the `ForkQueryHost` / `ForkPromoteSink` seams the engine is designed around.
//! The hosts wrap real `StorageManager`s (an empty one, or one whose `vertices`
//! table is pre-populated so `get_vertex_ext_ids` returns a real map). Nothing
//! inside the engine is mocked.
//!
//! All assertions capture the CURRENT (buggy) behavior and are marked `// BUG:`
//! so the suite stays green while pinning the defect.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use anyhow::{Result as AnyResult, anyhow};
use arrow_array::{BooleanArray, RecordBatch, StringArray, UInt64Array};
use arrow_schema::{DataType, Field, Schema as ArrowSchema};

use uni_common::config::UniConfig;
use uni_common::core::id::Vid;
use uni_common::core::schema::SchemaManager;
use uni_common::{Node, Properties, Result, Value};
use uni_query::{QueryMetrics, QueryResult, Row};
use uni_store::backend::table_names::main_vertex_table_name;
use uni_store::backend::traits::{RecordBatchStream, TableWriteGuard};
use uni_store::backend::types::{FilterExpr, ScanRequest, WriteMode};
use uni_store::storage::manager::StorageManager;
use uni_store::storage::vertex::VertexDataset;
use uni_store::{LanceDbBackend, StorageBackend};

use uni_fork::{
    ForkPromoteSink, ForkQueryHost, PromoteBaseline, PromoteOptions, PromotePattern, compute_diff,
    run_promote,
};

// --------------------------------------------------------------------------
// Test doubles for the host seams (real engine, real storage behind them).
// --------------------------------------------------------------------------

type Responder = Box<dyn Fn(&str) -> QueryResult + Send + Sync>;

struct TestHost {
    storage: Arc<StorageManager>,
    schema: Arc<SchemaManager>,
    responder: Responder,
}

#[async_trait::async_trait]
impl ForkQueryHost for TestHost {
    async fn query(&self, cypher: &str) -> Result<QueryResult> {
        Ok((self.responder)(cypher))
    }
    fn storage(&self) -> Arc<StorageManager> {
        self.storage.clone()
    }
    fn schema(&self) -> Arc<SchemaManager> {
        self.schema.clone()
    }
}

/// Wraps a [`TestHost`] and makes `query` fail for any Cypher containing
/// `fail_containing`, leaving every other call to the inner responder.
///
/// The seam for the C1/C2/C3 findings: each swallowed a failed primary
/// round-trip into an empty result and then read the emptiness as a fact
/// about the data ("no such edge exists", "no primary twin", "no endpoint").
struct FailingQueryHost {
    inner: TestHost,
    fail_containing: &'static str,
}

#[async_trait::async_trait]
impl ForkQueryHost for FailingQueryHost {
    async fn query(&self, cypher: &str) -> Result<QueryResult> {
        if cypher.contains(self.fail_containing) {
            return Err(uni_common::UniError::Storage {
                message: format!("injected transient failure for `{}`", self.fail_containing),
                source: None,
            });
        }
        self.inner.query(cypher).await
    }
    fn storage(&self) -> Arc<StorageManager> {
        self.inner.storage()
    }
    fn schema(&self) -> Arc<SchemaManager> {
        self.inner.schema()
    }
}

#[derive(Default)]
struct RecordingSink {
    inserted: Mutex<usize>,
    deleted: Mutex<Vec<(String, Vid)>>,
    updated: Mutex<usize>,
}

#[async_trait::async_trait]
impl ForkPromoteSink for RecordingSink {
    async fn bulk_insert_vertices(&self, _label: &str, rows: Vec<Properties>) -> Result<Vec<Vid>> {
        let n = rows.len();
        *self.inserted.lock().unwrap() += n;
        Ok((0..n).map(|i| Vid::new(9000 + i as u64)).collect())
    }
    async fn update_vertex_properties(
        &self,
        _label: &str,
        _vid: Vid,
        _props: Properties,
    ) -> Result<()> {
        *self.updated.lock().unwrap() += 1;
        Ok(())
    }
    async fn delete_vertex(&self, label: &str, vid: Vid) -> Result<()> {
        self.deleted.lock().unwrap().push((label.to_string(), vid));
        Ok(())
    }
    async fn bulk_insert_edges(
        &self,
        _edge_type: &str,
        _edges: Vec<(Vid, Vid, Properties)>,
    ) -> Result<()> {
        Ok(())
    }
}

// --------------------------------------------------------------------------
// Fault backend: delegates to an inner backend but can be armed to fail
// `table_exists`, modeling a transient object-store LIST failure. Used to prove
// that a real `get_vertex_ext_ids()` error now PROPAGATES out of the promote
// engine instead of being swallowed to an empty map.
// --------------------------------------------------------------------------

struct FaultBackend {
    inner: Arc<dyn StorageBackend>,
    fail_table_exists: AtomicBool,
}

impl FaultBackend {
    fn new(inner: Arc<dyn StorageBackend>) -> Self {
        Self {
            inner,
            fail_table_exists: AtomicBool::new(false),
        }
    }
    fn set_fail_table_exists(&self, on: bool) {
        self.fail_table_exists.store(on, Ordering::SeqCst);
    }
}

#[async_trait::async_trait]
impl StorageBackend for FaultBackend {
    async fn table_names(&self) -> AnyResult<Vec<String>> {
        self.inner.table_names().await
    }
    async fn table_exists(&self, name: &str) -> AnyResult<bool> {
        if self.fail_table_exists.load(Ordering::SeqCst) {
            return Err(anyhow!("injected transient LIST failure for {name}"));
        }
        self.inner.table_exists(name).await
    }
    async fn create_table(&self, name: &str, batches: Vec<RecordBatch>) -> AnyResult<()> {
        self.inner.create_table(name, batches).await
    }
    async fn create_empty_table(&self, name: &str, schema: Arc<ArrowSchema>) -> AnyResult<()> {
        self.inner.create_empty_table(name, schema).await
    }
    async fn open_or_create_table(&self, name: &str, schema: Arc<ArrowSchema>) -> AnyResult<()> {
        self.inner.open_or_create_table(name, schema).await
    }
    async fn drop_table(&self, name: &str) -> AnyResult<()> {
        self.inner.drop_table(name).await
    }
    async fn scan(&self, request: ScanRequest) -> AnyResult<Vec<RecordBatch>> {
        self.inner.scan(request).await
    }
    async fn scan_stream(&self, request: ScanRequest) -> AnyResult<RecordBatchStream> {
        self.inner.scan_stream(request).await
    }
    async fn get_table_schema(&self, name: &str) -> AnyResult<Option<Arc<ArrowSchema>>> {
        self.inner.get_table_schema(name).await
    }
    async fn count_rows(&self, table_name: &str, filter: Option<&FilterExpr>) -> AnyResult<usize> {
        self.inner.count_rows(table_name, filter).await
    }
    async fn write(
        &self,
        table_name: &str,
        batches: Vec<RecordBatch>,
        mode: WriteMode,
    ) -> AnyResult<()> {
        self.inner.write(table_name, batches, mode).await
    }
    async fn delete_rows(&self, table_name: &str, filter: &FilterExpr) -> AnyResult<()> {
        self.inner.delete_rows(table_name, filter).await
    }
    async fn replace_table_atomic(
        &self,
        name: &str,
        batches: Vec<RecordBatch>,
        schema: Arc<ArrowSchema>,
    ) -> AnyResult<()> {
        self.inner.replace_table_atomic(name, batches, schema).await
    }
    async fn lock_table_for_write(&self, name: &str) -> TableWriteGuard {
        self.inner.lock_table_for_write(name).await
    }
    async fn get_table_version(&self, table_name: &str) -> AnyResult<Option<u64>> {
        self.inner.get_table_version(table_name).await
    }
    async fn rollback_table(&self, table_name: &str, target_version: u64) -> AnyResult<()> {
        self.inner.rollback_table(table_name, target_version).await
    }
    async fn optimize_table(
        &self,
        table_name: &str,
        version_retention: std::time::Duration,
    ) -> AnyResult<uni_store::backend::OptimizeReport> {
        self.inner
            .optimize_table(table_name, version_retention)
            .await
    }
    async fn recover_staging(&self, table_name: &str) -> AnyResult<()> {
        self.inner.recover_staging(table_name).await
    }
    fn base_uri(&self) -> &str {
        self.inner.base_uri()
    }
}

/// A `StorageManager` over a [`FaultBackend`] whose `vertices` table carries the
/// given `(vid, ext_id)` rows. Returns the manager and the fault handle so the
/// test can arm a transient `get_vertex_ext_ids()` failure.
async fn faulted_populated_store(
    schema: Arc<SchemaManager>,
    rows: &[(u64, &str)],
) -> (tempfile::TempDir, Arc<StorageManager>, Arc<FaultBackend>) {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap().to_string();

    let vids: Vec<u64> = rows.iter().map(|(v, _)| *v).collect();
    let exts: Vec<&str> = rows.iter().map(|(_, e)| *e).collect();
    let deleted = vec![false; rows.len()];
    let arrow_schema = Arc::new(ArrowSchema::new(vec![
        Field::new("_vid", DataType::UInt64, false),
        Field::new("ext_id", DataType::Utf8, true),
        Field::new("_deleted", DataType::Boolean, false),
    ]));
    let batch = RecordBatch::try_new(
        arrow_schema,
        vec![
            Arc::new(UInt64Array::from(vids)),
            Arc::new(StringArray::from(exts)),
            Arc::new(BooleanArray::from(deleted)),
        ],
    )
    .unwrap();

    let lance = LanceDbBackend::connect(&uri, None).await.unwrap();
    lance
        .create_table(main_vertex_table_name(), vec![batch])
        .await
        .unwrap();
    let fault = Arc::new(FaultBackend::new(Arc::new(lance)));

    let store: Arc<dyn object_store::ObjectStore> =
        Arc::new(object_store::local::LocalFileSystem::new_with_prefix(dir.path()).unwrap());
    let sm =
        StorageManager::new_with_backend(&uri, store, fault.clone(), schema, UniConfig::default())
            .await
            .unwrap();
    (dir, Arc::new(sm), fault)
}

// --------------------------------------------------------------------------
// Builders.
// --------------------------------------------------------------------------

async fn schema_with_person() -> Arc<SchemaManager> {
    let store: Arc<dyn object_store::ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let s = SchemaManager::load_from_store(store, &object_store::path::Path::from("schema.json"))
        .await
        .unwrap();
    s.add_label("Person").unwrap();
    Arc::new(s)
}

fn node(vid: u64, label: &str, props: &[(&str, Value)]) -> Node {
    let mut p = Properties::new();
    for (k, v) in props {
        p.insert((*k).to_string(), v.clone());
    }
    Node {
        vid: Vid::new(vid),
        labels: vec![label.to_string()],
        properties: p,
    }
}

fn node_result(nodes: Vec<Node>) -> QueryResult {
    let cols = Arc::new(vec!["n".to_string()]);
    let rows = nodes
        .into_iter()
        .map(|n| Row::new(cols.clone(), vec![Value::Node(n)]))
        .collect();
    QueryResult::new(cols, rows, vec![], QueryMetrics::default())
}

fn vid_node_result(rows_data: Vec<(u64, Node)>) -> QueryResult {
    let cols = Arc::new(vec!["vid".to_string(), "node".to_string()]);
    let rows = rows_data
        .into_iter()
        .map(|(v, n)| Row::new(cols.clone(), vec![Value::Int(v as i64), Value::Node(n)]))
        .collect();
    QueryResult::new(cols, rows, vec![], QueryMetrics::default())
}

fn empty_result(col: &str) -> QueryResult {
    let cols = Arc::new(vec![col.to_string()]);
    QueryResult::new(cols, vec![], vec![], QueryMetrics::default())
}

/// A real, empty `StorageManager` — `get_vertex_ext_ids()` returns `Ok({})`
/// (the `vertices` table is absent), which is byte-for-byte what
/// `unwrap_or_default()` produces when the real fetch returns `Err`.
async fn empty_store(schema: Arc<SchemaManager>) -> (tempfile::TempDir, Arc<StorageManager>) {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let sm = StorageManager::new(uri, schema).await.unwrap();
    (dir, Arc::new(sm))
}

/// A real `StorageManager` whose `vertices` table already carries the given
/// `(vid, ext_id)` rows, so `get_vertex_ext_ids()` returns a populated map.
async fn populated_store(
    schema: Arc<SchemaManager>,
    rows: &[(u64, &str)],
) -> (tempfile::TempDir, Arc<StorageManager>) {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap().to_string();

    let vids: Vec<u64> = rows.iter().map(|(v, _)| *v).collect();
    let exts: Vec<&str> = rows.iter().map(|(_, e)| *e).collect();
    let deleted = vec![false; rows.len()];
    let arrow_schema = Arc::new(ArrowSchema::new(vec![
        Field::new("_vid", DataType::UInt64, false),
        Field::new("ext_id", DataType::Utf8, true),
        Field::new("_deleted", DataType::Boolean, false),
    ]));
    let batch = RecordBatch::try_new(
        arrow_schema,
        vec![
            Arc::new(UInt64Array::from(vids)),
            Arc::new(StringArray::from(exts)),
            Arc::new(BooleanArray::from(deleted)),
        ],
    )
    .unwrap();

    let backend = LanceDbBackend::connect(&uri, None).await.unwrap();
    backend
        .create_table(main_vertex_table_name(), vec![batch])
        .await
        .unwrap();
    drop(backend);

    // Fresh manager re-opens the same on-disk table.
    let sm = StorageManager::new(&uri, schema).await.unwrap();
    (dir, Arc::new(sm))
}

// ==========================================================================
// Finding [2] — diff.rs (FIXED): the promote-recomputed content-UID now MATCHES
// the UID registered by writer.rs. The write side hashes props that STILL
// contain the "ext_id" key, so ext_id folds into the digest twice (once as the
// dedicated arg, once as the "ext_id" property term), whereas raw Cypher results
// STRIP that key. `content_uid_with_ext_id` re-injects the "ext_id" key on the
// promote side before hashing, reproducing the registered digest exactly, so the
// UID dedup fires for ext_id-bearing rows. A stale pre-edit UID that still
// resolves to a live-but-edited primary vertex is rejected separately by the
// live-content check in `batch_resolve_primary_vids`, so the fix no longer risks
// the insert-only-twin regression that previously forced a deferral (see the
// end-to-end `promote_default_is_insert_only_twin` and re-promote idempotency
// tests in uni-db).
// ==========================================================================
#[test]
fn promote_uid_matches_registered_uid_via_ext_reinjection() {
    // Write/register side (writer.rs flush finalize): ext_id = Some("p1") AND the
    // property map STILL contains the "ext_id" key.
    let mut props_with_ext = Properties::new();
    props_with_ext.insert("ext_id".to_string(), Value::String("p1".to_string()));
    props_with_ext.insert("name".to_string(), Value::String("Alice".to_string()));
    let registered = VertexDataset::compute_vertex_uid("Person", Some("p1"), &props_with_ext);

    // Promote-recompute side: Cypher STRIPS the "ext_id" key, so the fix
    // re-injects it (mirroring `content_uid_with_ext_id`) before hashing.
    let mut props_stripped = Properties::new();
    props_stripped.insert("name".to_string(), Value::String("Alice".to_string()));
    props_stripped.insert("ext_id".to_string(), Value::String("p1".to_string()));
    let recomputed = VertexDataset::compute_vertex_uid("Person", Some("p1"), &props_stripped);

    // With ext_id re-injected the two digests are identical, so the ext_id-
    // bearing row's UID dedup now fires (FIXED).
    assert_eq!(
        registered, recomputed,
        "re-injecting ext_id must make the promote UID match the registered UID"
    );
}

// ==========================================================================
// Finding [4] — diff.rs:43: compute_diff swallows a one-sided
// get_vertex_ext_ids() failure with unwrap_or_default(). With side `a` empty
// and side `b` populated, an unchanged ext_id vertex is keyed under
// UID(None) on `a` and UID(Some(ext)) on `b`, so it appears as BOTH added and
// deleted.
// ==========================================================================

#[test]
fn finding4_ext_id_is_part_of_content_identity() {
    let mut props = Properties::new();
    props.insert("name".to_string(), Value::String("a".to_string()));
    // The ext_id is part of the content identity: an empty ext map (ext_id =
    // None) hashes to a different UID than the real ext_id = Some("p1"). This is
    // exactly why a SWALLOWED ext-fetch error (empty map) would split one
    // physical vertex across both `added` and `deleted` — the motivation for
    // propagating the error rather than defaulting to an empty map.
    let uid_no_ext = VertexDataset::compute_vertex_uid("Person", None, &props);
    let uid_with_ext = VertexDataset::compute_vertex_uid("Person", Some("p1"), &props);
    assert_ne!(
        uid_no_ext, uid_with_ext,
        "ext_id must contribute to the content UID"
    );
}

#[tokio::test]
async fn finding4_compute_diff_propagates_ext_fetch_error() {
    let schema = schema_with_person().await;
    // Side a: store over a fault backend armed to fail get_vertex_ext_ids.
    let (_dir_a, sm_a, fault_a) = faulted_populated_store(schema.clone(), &[(1, "p1")]).await;
    // Side b: healthy store with {vid 1 -> "p1"}.
    let (_dir_b, sm_b) = populated_store(schema.clone(), &[(1, "p1")]).await;

    let n = node(1, "Person", &[("name", Value::String("a".to_string()))]);
    let host_a = TestHost {
        storage: sm_a,
        schema: schema.clone(),
        responder: {
            let n = n.clone();
            Box::new(move |_| node_result(vec![n.clone()]))
        },
    };
    let host_b = TestHost {
        storage: sm_b,
        schema: schema.clone(),
        responder: Box::new(move |_| node_result(vec![n.clone()])),
    };

    // Arm the transient failure only for the diff call (not store construction).
    fault_a.set_fail_table_exists(true);
    let res = compute_diff(&host_a, &host_b).await;

    // Fixed (diff.rs:43): the ext-fetch error now propagates instead of being
    // swallowed to an empty map that would split the vertex across add+delete.
    assert!(
        res.is_err(),
        "compute_diff must propagate a transient ext-fetch failure; got {:?}",
        res.map(|d| d.is_empty())
    );
}

// ==========================================================================
// Finding [1] — diff.rs:597: run_promote swallows a failed FORK-side
// get_vertex_ext_ids() with unwrap_or_default(). The empty map makes the
// delete-promotion pass read EVERY baseline ext_id row as "deleted on the
// fork", mass-deleting live primary vertices the fork never touched.
// ==========================================================================
#[tokio::test]
async fn finding1_run_promote_propagates_fork_ext_failure() {
    let schema = schema_with_person().await;

    // FORK: store over a fault backend armed to fail get_vertex_ext_ids. The
    // fork deleted NOTHING (its three ext_id rows are still present).
    let (_dir_fork, sm_fork, fault_fork) =
        faulted_populated_store(schema.clone(), &[(1, "p1"), (2, "p2"), (3, "p3")]).await;
    let fork_nodes = vec![
        node(1, "Person", &[("name", Value::String("v1".to_string()))]),
        node(2, "Person", &[("name", Value::String("v2".to_string()))]),
        node(3, "Person", &[("name", Value::String("v3".to_string()))]),
    ];
    let fork_host = TestHost {
        storage: sm_fork,
        schema: schema.clone(),
        responder: Box::new(move |cypher: &str| {
            if cypher.contains("WHERE false") {
                empty_result("n")
            } else if cypher.contains("RETURN n") {
                node_result(fork_nodes.clone())
            } else {
                empty_result("c")
            }
        }),
    };

    // PRIMARY: three live vertices with ext_ids p1/p2/p3 (fetch succeeds).
    let (_dir_prim, sm_prim) =
        populated_store(schema.clone(), &[(101, "p1"), (102, "p2"), (103, "p3")]).await;
    let prim_rows = vec![
        (101u64, node(101, "Person", &[])),
        (102, node(102, "Person", &[])),
        (103, node(103, "Person", &[])),
    ];
    let primary_host = TestHost {
        storage: sm_prim,
        schema: schema.clone(),
        responder: Box::new(move |cypher: &str| {
            if cypher.contains("AS vid, n AS node") {
                vid_node_result(prim_rows.clone())
            } else {
                empty_result("vid")
            }
        }),
    };

    let sink = RecordingSink::default();

    // Fork-point baseline: all three ext_id rows were present at the fork point.
    let mut ext = HashMap::new();
    for e in ["p1", "p2", "p3"] {
        ext.insert(e.to_string(), Properties::new());
    }
    let baseline = PromoteBaseline {
        ext: HashMap::from([("Person".to_string(), ext)]),
        no_ext: HashMap::new(),
    };

    // Arm the transient failure only for the promote call.
    fault_fork.set_fail_table_exists(true);
    let patterns = vec![PromotePattern::label("Person").where_clause("false")];
    let res = run_promote(
        &fork_host,
        &primary_host,
        &sink,
        &patterns,
        &PromoteOptions::with_merge(),
        Some(&baseline),
    )
    .await;

    // Fixed (diff.rs:597): the fork-side ext-fetch error now propagates. It is
    // NEVER swallowed to an empty map that would make the delete pass read every
    // baseline ext_id row as "deleted on the fork" and mass-delete live primary
    // vertices — so no delete is issued.
    assert!(
        res.is_err(),
        "run_promote must propagate a fork-side ext-fetch failure, not mass-delete"
    );
    assert!(
        sink.deleted.lock().unwrap().is_empty(),
        "no primary vertex may be deleted when the fork ext-fetch fails"
    );
}

// ==========================================================================
// Finding [3] — diff.rs:1031: delete-promotion discards the primary's current
// props (`_props`) and never consults the baseline or ConflictPolicy, so a
// fork-delete racing a primary-edit deletes the concurrently-edited primary
// row even under ConflictPolicy::Skip, and vertices_conflicting stays 0.
// ==========================================================================
#[tokio::test]
async fn finding3_delete_vs_edit_conflict_ignored_under_skip() {
    let schema = schema_with_person().await;

    // FORK: Alice was DELETED on the fork -> every scan is empty.
    let (_dir_fork, sm_fork) = empty_store(schema.clone()).await;
    let fork_host = TestHost {
        storage: sm_fork,
        schema: schema.clone(),
        responder: Box::new(|_cypher| empty_result("n")),
    };

    // PRIMARY: Alice still present, concurrently EDITED to age=99 (baseline was age=30).
    let (_dir_prim, sm_prim) = populated_store(schema.clone(), &[(50, "p1")]).await;
    let alice_now = node(50, "Person", &[("age", Value::Int(99))]);
    let primary_host = TestHost {
        storage: sm_prim,
        schema: schema.clone(),
        responder: Box::new(move |cypher: &str| {
            if cypher.contains("AS vid, n AS node") {
                vid_node_result(vec![(50, alice_now.clone())])
            } else {
                empty_result("vid")
            }
        }),
    };

    let sink = RecordingSink::default();

    // Baseline pins Alice at age=30 at the fork point.
    let mut base_props = Properties::new();
    base_props.insert("age".to_string(), Value::Int(30));
    let baseline = PromoteBaseline {
        ext: HashMap::from([(
            "Person".to_string(),
            HashMap::from([("p1".to_string(), base_props)]),
        )]),
        no_ext: HashMap::new(),
    };

    let patterns = vec![PromotePattern::label("Person")];
    let report = run_promote(
        &fork_host,
        &primary_host,
        &sink,
        &patterns,
        &PromoteOptions::with_merge(), // on_conflict = Skip
        Some(&baseline),
    )
    .await
    .unwrap();

    // Fixed (diff.rs:1031): under ConflictPolicy::Skip a fork-delete that races
    // a primary-edit is counted as a conflict and left untouched. The
    // concurrently-edited row (age=99 != baseline age=30) is NOT deleted.
    assert_eq!(
        report.vertices_deleted, 0,
        "the concurrently-edited row must not be deleted under Skip"
    );
    assert_eq!(
        report.vertices_conflicting, 1,
        "the delete-vs-edit divergence must be recorded as a conflict"
    );
    assert!(
        sink.deleted.lock().unwrap().is_empty(),
        "no delete should be issued to the sink under Skip"
    );
}

// ==========================================================================
// C1 — diff.rs edge path: the primary parallel-edge PRE-FETCH used `if let Ok`,
// so a failed round-trip left `primary_edge_uids` EMPTY, every fork edge looked
// new, and promote inserted DUPLICATE edges on primary while reporting a clean
// `edges_inserted` with `edges_skipped_duplicate = 0`. The fix records the
// failure and surfaces the inserts as `edges_inserted_unverified`.
// ==========================================================================

/// `run_promote` is generic over ONE host type (`fork: &Q, primary: &Q`), so a
/// test that fails only the primary must wrap BOTH sides. This wrapper never
/// matches any Cypher the engine emits, so the fork host behaves exactly like
/// its inner `TestHost`.
fn never_fails(inner: TestHost) -> FailingQueryHost {
    FailingQueryHost {
        inner,
        fail_containing: "\u{1}__never_matches__",
    }
}

/// Two Person nodes plus one KNOWS edge between them, as the fork sees them.
fn c1_fork_nodes() -> (Node, Node) {
    (
        node(1, "Person", &[("name", Value::String("a".to_string()))]),
        node(2, "Person", &[("name", Value::String("b".to_string()))]),
    )
}

fn edge_result(a: Node, b: Node) -> QueryResult {
    let cols = Arc::new(vec!["a".to_string(), "r".to_string(), "b".to_string()]);
    let e = uni_common::Edge {
        eid: uni_common::core::id::Eid::new(7),
        edge_type: "KNOWS".to_string(),
        src: a.vid,
        dst: b.vid,
        properties: Properties::new(),
    };
    let row = Row::new(
        cols.clone(),
        vec![Value::Node(a), Value::Edge(e), Value::Node(b)],
    );
    QueryResult::new(cols, vec![row], vec![], QueryMetrics::default())
}

/// Builds the C1 fork host: a vertex pattern promotes both endpoints (seeding
/// the within-call `just_inserted` cache so the edge path resolves endpoints
/// without touching primary), then the edge pattern promotes the KNOWS edge.
async fn c1_fork_host(schema: Arc<SchemaManager>) -> (tempfile::TempDir, TestHost) {
    let (dir, sm) = empty_store(schema.clone()).await;
    let (a, b) = c1_fork_nodes();
    let host = TestHost {
        storage: sm,
        schema,
        responder: Box::new(move |cypher: &str| {
            if cypher.contains("RETURN a, r, b") {
                edge_result(a.clone(), b.clone())
            } else if cypher.contains("RETURN n") {
                node_result(vec![a.clone(), b.clone()])
            } else {
                empty_result("n")
            }
        }),
    };
    (dir, host)
}

async fn c1_primary_inner(schema: Arc<SchemaManager>) -> (tempfile::TempDir, TestHost) {
    let (dir, sm) = empty_store(schema.clone()).await;
    let host = TestHost {
        storage: sm,
        schema,
        responder: Box::new(|_cypher| empty_result("a")),
    };
    (dir, host)
}

fn c1_patterns() -> Vec<PromotePattern> {
    vec![
        PromotePattern::label("Person"),
        PromotePattern::edge_type("KNOWS"),
    ]
}

#[tokio::test]
async fn c1_edge_dedup_prefetch_failure_marks_inserts_unverified() {
    let schema = schema_with_person().await;
    let (_dir_fork, fork_host) = c1_fork_host(schema.clone()).await;
    let (_dir_prim, primary_inner) = c1_primary_inner(schema.clone()).await;

    // Fail ONLY the dedup pre-fetch. `AND id(b) IN` appears in no other Cypher
    // the promote engine emits.
    let primary_host = FailingQueryHost {
        inner: primary_inner,
        fail_containing: "AND id(b) IN",
    };
    let sink = RecordingSink::default();
    let fork_host = never_fails(fork_host);

    let report = run_promote(
        &fork_host,
        &primary_host,
        &sink,
        &c1_patterns(),
        &PromoteOptions::insert_only(),
        None,
    )
    .await
    .unwrap();

    assert_eq!(
        report.edges_inserted, 1,
        "the fork edge is still promoted (a degraded dedup must not abort)"
    );
    assert!(
        report.edges_inserted_unverified > 0,
        "a failed dedup pre-fetch must mark the inserted edges unverified"
    );
    assert_eq!(
        report.edges_inserted_unverified, report.edges_inserted,
        "the whole insert batch is unverified when the dedup set could not be built"
    );
}

#[tokio::test]
async fn c1_control_healthy_dedup_prefetch_leaves_inserts_verified() {
    // Happy-path control for `c1_edge_dedup_prefetch_failure_marks_inserts_unverified`:
    // the SAME scenario with no injected failure must report zero unverified
    // edges, so the assertion above cannot pass for an unrelated reason.
    let schema = schema_with_person().await;
    let (_dir_fork, fork_host) = c1_fork_host(schema.clone()).await;
    let (_dir_prim, primary_host) = c1_primary_inner(schema.clone()).await;
    let sink = RecordingSink::default();

    let report = run_promote(
        &fork_host,
        &primary_host,
        &sink,
        &c1_patterns(),
        &PromoteOptions::insert_only(),
        None,
    )
    .await
    .unwrap();

    assert_eq!(report.edges_inserted, 1, "same edge promoted");
    assert_eq!(
        report.edges_inserted_unverified, 0,
        "a healthy dedup pre-fetch must leave the inserts verified"
    );
}

// ==========================================================================
// C2 (a) — the UPSERT call site of `batch_resolve_primary_by_ext_id`. A failed
// primary round-trip degrades to an empty map, so an EDITED fork vertex fails
// to match its primary twin and is INSERTED as a duplicate instead of updated
// in place. The counts are identical before and after the fix (the fix does not
// change the insert-vs-update outcome, only reports it), so the ONLY observable
// of the newly-threaded flag is the `warn!`. This test therefore captures the
// tracing output; the sink counts below document the (unchanged) damage.
// ==========================================================================

/// A `MakeWriter` that appends every formatted event into a shared buffer.
#[derive(Clone, Default)]
struct LogCapture(Arc<Mutex<Vec<u8>>>);

impl std::io::Write for LogCapture {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(buf);
        Ok(buf.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for LogCapture {
    type Writer = LogCapture;
    fn make_writer(&'a self) -> Self::Writer {
        self.clone()
    }
}

impl LogCapture {
    fn text(&self) -> String {
        String::from_utf8_lossy(&self.0.lock().unwrap()).to_string()
    }
}

/// Runs the C2(a) upsert scenario, optionally failing the ext_id resolve.
/// Returns `(report, inserted, updated, captured_logs)`.
async fn run_c2_upsert(fail: bool) -> (uni_fork::PromoteReport, usize, usize, String) {
    let schema = schema_with_person().await;

    // FORK: p1 EDITED to name="edited".
    let (_dir_fork, sm_fork) = populated_store(schema.clone(), &[(1, "p1")]).await;
    let fork_node = node(
        1,
        "Person",
        &[("name", Value::String("edited".to_string()))],
    );
    let fork_host = TestHost {
        storage: sm_fork,
        schema: schema.clone(),
        responder: Box::new(move |_| node_result(vec![fork_node.clone()])),
    };

    // PRIMARY: the same ext_id p1 lives at vid 50 with the pre-edit name.
    let (_dir_prim, sm_prim) = populated_store(schema.clone(), &[(50, "p1")]).await;
    let prim_node = node(50, "Person", &[("name", Value::String("orig".to_string()))]);
    let primary_inner = TestHost {
        storage: sm_prim,
        schema: schema.clone(),
        responder: Box::new(move |cypher: &str| {
            if cypher.contains("AS vid, n AS node") {
                vid_node_result(vec![(50, prim_node.clone())])
            } else {
                empty_result("n")
            }
        }),
    };

    let sink = RecordingSink::default();
    let patterns = vec![PromotePattern::label("Person")];
    let opts = PromoteOptions::with_upsert();

    let capture = LogCapture::default();
    let subscriber = tracing_subscriber::fmt()
        .with_writer(capture.clone())
        .with_max_level(tracing::Level::WARN)
        .with_ansi(false)
        .finish();
    let _guard = tracing::subscriber::set_default(subscriber);

    let report = if fail {
        // `AS vid, n AS node` is emitted only by `batch_resolve_primary_by_ext_id`.
        let primary_host = FailingQueryHost {
            inner: primary_inner,
            fail_containing: "AS vid, n AS node",
        };
        run_promote(
            &never_fails(fork_host),
            &primary_host,
            &sink,
            &patterns,
            &opts,
            None,
        )
        .await
    } else {
        run_promote(&fork_host, &primary_inner, &sink, &patterns, &opts, None).await
    }
    .unwrap();

    drop(_guard);
    let inserted = *sink.inserted.lock().unwrap();
    let updated = *sink.updated.lock().unwrap();
    (report, inserted, updated, capture.text())
}

#[tokio::test]
async fn c2a_upsert_ext_id_resolve_failure_is_reported() {
    let (report, inserted, updated, logs) = run_c2_upsert(true).await;

    // The damage (unchanged by the fix, recorded here so the test documents it):
    // the edited fork vertex is inserted as a DUPLICATE twin, not updated.
    assert_eq!(inserted, 1, "the edited fork row is inserted as a twin");
    assert_eq!(updated, 0, "no in-place update happens");
    assert_eq!(report.vertices_updated, 0);

    // The flag's ONLY observable is the warning — the counts above are identical
    // with and without the fix, so this is what pins C2(a).
    assert!(
        logs.contains("could not resolve primary twins by ext_id"),
        "a degraded ext_id resolve must be surfaced; captured logs were:\n{logs}"
    );
}

#[tokio::test]
async fn c2a_control_healthy_ext_id_resolve_updates_in_place() {
    let (report, inserted, updated, logs) = run_c2_upsert(false).await;

    assert_eq!(updated, 1, "the edited fork row updates its primary twin");
    assert_eq!(report.vertices_updated, 1);
    assert_eq!(inserted, 0, "no duplicate twin is inserted");
    assert!(
        !logs.contains("could not resolve primary twins by ext_id"),
        "a healthy resolve must not warn; captured logs were:\n{logs}"
    );
}

// ==========================================================================
// C2 (b) — the DELETE-PROMOTION call site of
// `batch_resolve_primary_by_ext_id`. A failed round-trip degraded to an empty
// map, so the delete was silently SKIPPED: the row stayed on primary and the
// report said nothing. The fix increments `vertices_deletes_unverified`.
// ==========================================================================

/// Runs the delete-promotion scenario, optionally failing the ext_id resolve.
/// Returns `(report, deleted_by_sink)`.
async fn run_c2_delete(fail: bool) -> (uni_fork::PromoteReport, Vec<(String, Vid)>) {
    let schema = schema_with_person().await;

    // FORK: p1 was DELETED — every fork scan comes back empty.
    let (_dir_fork, sm_fork) = empty_store(schema.clone()).await;
    let fork_host = TestHost {
        storage: sm_fork,
        schema: schema.clone(),
        responder: Box::new(|_| empty_result("n")),
    };

    // PRIMARY: p1 still lives at vid 50, unchanged since the fork point.
    let (_dir_prim, sm_prim) = populated_store(schema.clone(), &[(50, "p1")]).await;
    let prim_node = node(50, "Person", &[]);
    let primary_inner = TestHost {
        storage: sm_prim,
        schema: schema.clone(),
        responder: Box::new(move |cypher: &str| {
            if cypher.contains("AS vid, n AS node") {
                vid_node_result(vec![(50, prim_node.clone())])
            } else {
                empty_result("n")
            }
        }),
    };

    let sink = RecordingSink::default();
    // Baseline: p1 existed at the fork point with the same (empty) props primary
    // still has, so the delete is a clean, non-conflicting one.
    let baseline = PromoteBaseline {
        ext: HashMap::from([(
            "Person".to_string(),
            HashMap::from([("p1".to_string(), Properties::new())]),
        )]),
        no_ext: HashMap::new(),
    };
    let patterns = vec![PromotePattern::label("Person")];
    let opts = PromoteOptions::with_merge();

    let report = if fail {
        let primary_host = FailingQueryHost {
            inner: primary_inner,
            fail_containing: "AS vid, n AS node",
        };
        run_promote(
            &never_fails(fork_host),
            &primary_host,
            &sink,
            &patterns,
            &opts,
            Some(&baseline),
        )
        .await
    } else {
        run_promote(
            &fork_host,
            &primary_inner,
            &sink,
            &patterns,
            &opts,
            Some(&baseline),
        )
        .await
    }
    .unwrap();

    let deleted = sink.deleted.lock().unwrap().clone();
    (report, deleted)
}

#[tokio::test]
async fn c2b_delete_promotion_resolve_failure_is_counted_not_silent() {
    let (report, deleted) = run_c2_delete(true).await;

    assert!(
        report.vertices_deletes_unverified > 0,
        "a delete whose primary twin could not be resolved must be counted, not vanish"
    );
    assert_eq!(report.vertices_deleted, 0, "no delete actually happened");
    assert!(
        deleted.is_empty(),
        "no delete may be issued to the sink when the resolve failed"
    );
}

#[tokio::test]
async fn c2b_control_healthy_resolve_issues_the_delete() {
    let (report, deleted) = run_c2_delete(false).await;

    assert_eq!(report.vertices_deleted, 1, "the delete is promoted");
    assert_eq!(deleted.len(), 1, "the sink received the delete");
    assert_eq!(deleted[0].0, "Person");
    assert_eq!(
        report.vertices_deletes_unverified, 0,
        "a healthy resolve leaves nothing unverified"
    );
}
