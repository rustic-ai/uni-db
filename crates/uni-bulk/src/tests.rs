// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Unit tests of the bulk-load engine: the commit point and its marker, the
//! recovery of a load interrupted after it, the background re-drive, the
//! visibility gate around the publish, the publish-time endpoint re-check, and
//! the refusal of a backend that cannot stage.
//!
//! They run against a real Lance backend in a temporary directory (wrapped,
//! for the refusal, in a double that does not stage), driving the writer's
//! own steps where a crash would interrupt them.

// Rust guideline compliant

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use arrow_array::RecordBatch;
use arrow_schema::Schema as ArrowSchema;
use async_trait::async_trait;
use object_store::ObjectStore;
use object_store::local::LocalFileSystem;
use object_store::path::Path as ObjectStorePath;
use uni_common::core::id::Vid;
use uni_common::core::schema::{DataType, EndpointConformance, SchemaManager};
use uni_common::{UniConfig, UniError, Value};
use uni_plugin_host::shutdown::ShutdownHandle;
use uni_store::backend::lance::LanceDbBackend;
use uni_store::backend::table_names;
use uni_store::backend::traits::{RecordBatchStream, StorageBackend, TableWriteGuard};
use uni_store::backend::types::{FilterExpr, ScanRequest, WriteMode};
use uni_store::runtime::writer::Writer;
use uni_store::storage::manager::StorageManager;

use crate::bulk::{BulkBackend, BulkWriter, BulkWriterBuilder, CommitPoint, EdgeData};
use crate::endpoint_check;
use crate::flush_intent::{self, BulkFlushIntent, BulkIntentPhase, Markers};

/// A backend that delegates to Lance but does not stage
/// (`stages_invisibly` keeps the trait default, `false`).
struct NonStagingBackend {
    inner: Arc<dyn StorageBackend>,
}

#[async_trait]
impl StorageBackend for NonStagingBackend {
    async fn table_names(&self) -> Result<Vec<String>> {
        self.inner.table_names().await
    }
    async fn table_exists(&self, name: &str) -> Result<bool> {
        self.inner.table_exists(name).await
    }
    async fn create_table(&self, name: &str, batches: Vec<RecordBatch>) -> Result<()> {
        self.inner.create_table(name, batches).await
    }
    async fn create_empty_table(&self, name: &str, schema: Arc<ArrowSchema>) -> Result<()> {
        self.inner.create_empty_table(name, schema).await
    }
    async fn open_or_create_table(&self, name: &str, schema: Arc<ArrowSchema>) -> Result<()> {
        self.inner.open_or_create_table(name, schema).await
    }
    async fn drop_table(&self, name: &str) -> Result<()> {
        self.inner.drop_table(name).await
    }
    async fn scan(&self, request: ScanRequest) -> Result<Vec<RecordBatch>> {
        self.inner.scan(request).await
    }
    async fn scan_stream(&self, request: ScanRequest) -> Result<RecordBatchStream> {
        self.inner.scan_stream(request).await
    }
    async fn get_table_schema(&self, name: &str) -> Result<Option<Arc<ArrowSchema>>> {
        self.inner.get_table_schema(name).await
    }
    async fn count_rows(&self, table_name: &str, filter: Option<&FilterExpr>) -> Result<usize> {
        self.inner.count_rows(table_name, filter).await
    }
    async fn write(
        &self,
        table_name: &str,
        batches: Vec<RecordBatch>,
        mode: WriteMode,
    ) -> Result<()> {
        self.inner.write(table_name, batches, mode).await
    }
    async fn delete_rows(&self, table_name: &str, filter: &FilterExpr) -> Result<()> {
        self.inner.delete_rows(table_name, filter).await
    }
    async fn replace_table_atomic(
        &self,
        name: &str,
        batches: Vec<RecordBatch>,
        schema: Arc<ArrowSchema>,
    ) -> Result<()> {
        self.inner.replace_table_atomic(name, batches, schema).await
    }
    async fn lock_table_for_write(&self, name: &str) -> TableWriteGuard {
        self.inner.lock_table_for_write(name).await
    }
    async fn get_table_version(&self, table_name: &str) -> Result<Option<u64>> {
        self.inner.get_table_version(table_name).await
    }
    async fn rollback_table(&self, table_name: &str, target_version: u64) -> Result<()> {
        self.inner.rollback_table(table_name, target_version).await
    }
    async fn optimize_table(
        &self,
        table_name: &str,
        version_retention: Duration,
    ) -> Result<uni_store::backend::OptimizeReport> {
        self.inner
            .optimize_table(table_name, version_retention)
            .await
    }
    async fn recover_staging(&self, table_name: &str) -> Result<()> {
        self.inner.recover_staging(table_name).await
    }
    fn base_uri(&self) -> &str {
        self.inner.base_uri()
    }
}

/// A database directory and the handles a bulk load runs on.
struct Fixture {
    _dir: tempfile::TempDir,
    backend: BulkBackend,
}

impl Fixture {
    /// Labels `P` and `Q` (each with `k: Int64`) and edge type `E` declared
    /// `P -> Q`, over Lance, or over the non-staging double when `staging`
    /// is false.
    async fn new(staging: bool) -> Self {
        let dir = tempfile::tempdir().unwrap();
        let uri = dir.path().to_str().unwrap().to_string();
        let lance: Arc<dyn StorageBackend> =
            Arc::new(LanceDbBackend::connect(&uri, None).await.unwrap());
        let backend: Arc<dyn StorageBackend> = if staging {
            lance
        } else {
            Arc::new(NonStagingBackend { inner: lance })
        };
        let store: Arc<dyn ObjectStore> =
            Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap());
        let schema = Arc::new(
            SchemaManager::load_from_store(
                store.clone(),
                &ObjectStorePath::from("catalog/schema.json"),
            )
            .await
            .unwrap(),
        );
        for label in ["P", "Q"] {
            schema.add_label(label).unwrap();
            schema
                .add_property(label, "k", DataType::Int64, true)
                .unwrap();
        }
        schema
            .add_edge_type("E", vec!["P".into()], vec!["Q".into()])
            .unwrap();
        schema.save().await.unwrap();
        let storage = Arc::new(
            StorageManager::new_with_backend(
                &uri,
                store,
                backend,
                schema.clone(),
                UniConfig::default(),
            )
            .await
            .unwrap(),
        );
        let writer = Writer::new(storage.clone(), schema.clone(), 0)
            .await
            .unwrap();
        Self {
            _dir: dir,
            backend: BulkBackend {
                storage,
                writer: Some(Arc::new(writer)),
                schema,
                shutdown: Arc::new(ShutdownHandle::new(Duration::from_secs(5))),
                config: UniConfig::default(),
                rebuild_manager: None,
                track_task: None,
                events: None,
            },
        }
    }

    fn storage(&self) -> &StorageManager {
        &self.backend.storage
    }

    fn writer(&self) -> &Writer {
        self.backend.writer.as_deref().unwrap()
    }

    fn bulk(&self) -> BulkWriter {
        BulkWriterBuilder::new_unguarded(self.backend.clone())
            .batch_size(4)
            .build()
            .unwrap()
    }

    /// Rows in `table`, 0 when it does not exist.
    async fn rows(&self, table: &str) -> usize {
        let backend = self.storage().backend();
        if backend.table_exists(table).await.unwrap() {
            backend.count_rows(table, None).await.unwrap()
        } else {
            0
        }
    }

    /// Rows of the load's tables: `vertices_P`, `vertices_Q`, `edges`,
    /// `deltas_E_fwd`.
    async fn load_rows(&self) -> [usize; 4] {
        [
            self.rows(&table_names::vertex_table_name("P")).await,
            self.rows(&table_names::vertex_table_name("Q")).await,
            self.rows(table_names::main_edge_table_name()).await,
            self.rows(&table_names::delta_table_name("E", "fwd")).await,
        ]
    }

    /// Ids of the markers present.
    async fn markers(&self) -> Vec<String> {
        use futures::TryStreamExt;
        let prefix = ObjectStorePath::from("catalog/bulk_flush_intents");
        let mut out: Vec<String> = self
            .storage()
            .store()
            .list(Some(&prefix))
            .map_ok(|m| m.location.filename().unwrap_or_default().to_string())
            .try_collect()
            .await
            .unwrap();
        out.sort();
        out
    }

    /// One committed `Q` vertex, flushed; its VID.
    async fn committed_q(&self) -> Vid {
        let vid = self.writer().next_vid().await.unwrap();
        self.writer()
            .insert_vertex_with_labels(vid, props(1000), &["Q".to_string()], None)
            .await
            .unwrap();
        self.writer().flush_to_l1(None).await.unwrap();
        vid
    }
}

fn props(k: i64) -> HashMap<String, Value> {
    HashMap::from([("k".to_string(), Value::Int(k))])
}

/// Four `P` vertices and four `Q` vertices from the load, and an edge from
/// each `P` to each `Q` of the same index; plus, when `external` is given, an
/// edge from the first `P` to it.
async fn stage_load(bulk: &mut BulkWriter, external: Option<Vid>) -> (Vec<Vid>, Vec<Vid>) {
    let p = bulk
        .insert_vertices("P", (0..4).map(props).collect::<Vec<_>>())
        .await
        .unwrap();
    let q = bulk
        .insert_vertices("Q", (10..14).map(props).collect::<Vec<_>>())
        .await
        .unwrap();
    let mut edges: Vec<EdgeData> = p
        .iter()
        .zip(&q)
        .map(|(s, d)| EdgeData::new(*s, *d, HashMap::new()))
        .collect();
    if let Some(x) = external {
        edges.push(EdgeData::new(p[0], x, HashMap::new()));
    }
    bulk.insert_edges("E", edges).await.unwrap();
    (p, q)
}

#[test]
fn bulk_writer_futures_can_run_on_a_spawned_task() {
    // Every async method of the writer borrows it across an await, so its
    // futures are `Send` only while the writer is `Sync`.
    fn is_send_sync<T: Send + Sync>() {}
    is_send_sync::<BulkWriter>();
}

#[tokio::test]
async fn a_backend_that_cannot_stage_is_refused_before_anything_is_written() {
    let fx = Fixture::new(false).await;
    let refused = BulkWriterBuilder::new_unguarded(fx.backend.clone()).build();
    let err = refused
        .err()
        .expect("a non-staging backend must be refused");
    assert!(
        matches!(
            err.downcast_ref::<UniError>(),
            Some(UniError::BulkLoadUnsupported { .. })
        ),
        "typed refusal, got {err:#}"
    );
    assert_eq!(fx.load_rows().await, [0; 4]);
    assert!(fx.markers().await.is_empty());
}

#[tokio::test]
async fn staged_rows_stay_invisible_until_commit_publishes_them() {
    let fx = Fixture::new(true).await;
    let mut bulk = fx.bulk();
    let load_id = bulk.load_id().to_string();
    stage_load(&mut bulk, None).await;
    // Every buffer has been flushed into staged files (batch size 4).
    assert_eq!(fx.load_rows().await, [0; 4], "staged, not published");

    let stats = bulk.commit().await.unwrap();
    assert!(stats.warnings.is_empty(), "{:?}", stats.warnings);
    assert_eq!(fx.load_rows().await, [4, 4, 4, 4]);
    assert!(fx.markers().await.is_empty(), "the marker is deleted");
    assert!(
        flush_intent::bulk_load_applied(fx.storage(), &load_id)
            .await
            .unwrap(),
        "the receipt answers once the marker is gone"
    );
    assert!(
        !fx.storage().bulk_gate().is_closed(),
        "the gate is open again"
    );
}

#[tokio::test]
async fn abort_discards_the_load_and_leaves_no_marker() {
    let fx = Fixture::new(true).await;
    let mut bulk = fx.bulk();
    let load_id = bulk.load_id().to_string();
    stage_load(&mut bulk, None).await;
    bulk.abort().await.unwrap();
    assert_eq!(fx.load_rows().await, [0; 4]);
    assert!(fx.markers().await.is_empty());
    assert!(
        !flush_intent::bulk_load_applied(fx.storage(), &load_id)
            .await
            .unwrap()
    );
}

#[tokio::test]
async fn the_commit_point_is_the_marker_and_recovery_completes_the_publish_once() {
    let fx = Fixture::new(true).await;
    let mut bulk = fx.bulk();
    let load_id = bulk.load_id().to_string();
    stage_load(&mut bulk, None).await;
    let plan = bulk.prepare_commit().await.unwrap();
    assert!(
        !flush_intent::bulk_load_applied(fx.storage(), &load_id)
            .await
            .unwrap(),
        "before the commit point the load is not applied"
    );
    assert!(matches!(
        bulk.reach_commit_point(&plan).await,
        CommitPoint::Reached
    ));
    let marker = flush_intent::read_marker(&Markers::for_storage(fx.storage()), &load_id)
        .await
        .unwrap()
        .expect("the commit point writes the marker");
    assert_eq!(marker.phase, BulkIntentPhase::Active);
    assert_eq!(
        marker.publish.as_ref().map(Vec::len),
        Some(plan.records.len())
    );
    assert!(
        flush_intent::bulk_load_applied(fx.storage(), &load_id)
            .await
            .unwrap(),
        "from the commit point on the load is applied"
    );
    // The process dies here: nothing published.
    std::mem::forget(bulk);
    assert_eq!(fx.load_rows().await, [0; 4]);

    let recovered = flush_intent::recover_interrupted_bulk_load(fx.storage())
        .await
        .unwrap();
    assert_eq!(fx.load_rows().await, [4, 4, 4, 4], "recovery publishes it");
    // Its events were never delivered: recovery hands the load back for that.
    assert_eq!(
        recovered
            .iter()
            .map(|l| l.load_id.as_str())
            .collect::<Vec<_>>(),
        [load_id.as_str()]
    );
    assert_eq!(recovered[0].rows, 12, "the marker records the load's rows");
    assert_eq!(recovered[0].labels, ["P", "Q"]);
    assert_eq!(recovered[0].edge_types, ["E"]);
    // `E` is enforced, so the marker waits for the post-replay check, and for
    // the events to be delivered.
    let marker = flush_intent::read_marker(&Markers::for_storage(fx.storage()), &load_id)
        .await
        .unwrap()
        .expect("kept until its endpoints are checked");
    assert_eq!(marker.phase, BulkIntentPhase::Committed);
    assert!(marker.publish.is_none() && !marker.endpoints_checked);
    assert!(marker.events_pending);

    // A second recovery (a crash before the check) publishes nothing again,
    // and still hands the load's events back: they were not delivered.
    let again = flush_intent::recover_interrupted_bulk_load(fx.storage())
        .await
        .unwrap();
    assert_eq!(fx.load_rows().await, [4, 4, 4, 4], "exactly once");
    assert_eq!(again, recovered);

    flush_intent::check_recovered_bulk_endpoints(fx.storage(), fx.writer(), true)
        .await
        .unwrap();
    assert_eq!(
        fx.markers().await,
        vec![format!("{load_id}.json")],
        "checked, but kept until the events are delivered"
    );
    flush_intent::finish_recovered_load_events(fx.storage(), &load_id)
        .await
        .unwrap();
    assert!(fx.markers().await.is_empty());
    assert!(
        flush_intent::bulk_load_applied(fx.storage(), &load_id)
            .await
            .unwrap()
    );
    assert_eq!(
        fx.storage().schema_manager().schema().edge_types["E"].endpoint_conformance,
        EndpointConformance::Enforced,
        "every endpoint conforms: the type keeps its record"
    );
}

#[tokio::test]
async fn a_per_load_marker_without_a_publish_record_is_kept_not_rolled_back() {
    let fx = Fixture::new(true).await;
    // What a load on a backend that cannot stage wrote before its first
    // write: the pre-load versions, no publish record.
    let intent = BulkFlushIntent {
        phase: BulkIntentPhase::Active,
        tables: BTreeMap::from([("vertices_P".to_string(), Some(3))]),
        snapshot_id: None,
        publish: None,
        labels: Vec::new(),
        version: None,
        endpoints_checked: false,
        rows: None,
        events_pending: false,
    };
    let markers = Markers::for_storage(fx.storage());
    let load_id = uuid::Uuid::new_v4().to_string();
    flush_intent::write_for_test(&markers, &load_id, &intent)
        .await
        .unwrap();
    let err = flush_intent::recover_interrupted_bulk_load(fx.storage())
        .await
        .expect_err("refused, not rolled back");
    assert!(
        format!("{err:#}").contains("without a publish record"),
        "{err:#}"
    );
    assert_eq!(fx.markers().await, vec![format!("{load_id}.json")]);
}

#[test]
fn markers_of_earlier_releases_still_parse() {
    // A per-load marker of the release before `version` and
    // `endpoints_checked`.
    let old = br#"{"phase":"Committed","tables":{"vertices_P":null},"snapshot_id":"s1"}"#;
    let intent: BulkFlushIntent = serde_json::from_slice(old).unwrap();
    assert_eq!(intent.phase, BulkIntentPhase::Committed);
    assert!(intent.version.is_none() && !intent.endpoints_checked);
    assert!(flush_intent::marker_applies(&intent));
    // `endpoints_checked: false` is not written, so a marker reads back as
    // the earlier releases' did.
    let json = serde_json::to_string(&intent).unwrap();
    assert!(!json.contains("endpoints_checked"), "{json}");
}

#[test]
fn which_markers_decide_their_load() {
    let base = BulkFlushIntent {
        phase: BulkIntentPhase::Active,
        tables: BTreeMap::new(),
        snapshot_id: None,
        publish: None,
        labels: Vec::new(),
        version: None,
        endpoints_checked: false,
        rows: None,
        events_pending: false,
    };
    assert!(
        !flush_intent::marker_applies(&base),
        "a visible load: rolled back"
    );
    let publishing = BulkFlushIntent {
        publish: Some(Vec::new()),
        ..base.clone()
    };
    assert!(flush_intent::marker_applies(&publishing));
    let committed = BulkFlushIntent {
        phase: BulkIntentPhase::Committed,
        ..base
    };
    assert!(flush_intent::marker_applies(&committed));
}

#[tokio::test]
async fn the_gate_hides_the_load_only_while_it_publishes() {
    let fx = Fixture::new(true).await;
    let gate = Arc::clone(fx.storage().bulk_gate());
    let before = gate.epoch();
    let mut bulk = fx.bulk();
    assert!(
        bulk.hide_load().await.is_none(),
        "nothing staged, nothing hidden"
    );
    stage_load(&mut bulk, None).await;
    let hold = bulk.hide_load().await.expect("a staged load is hidden");
    assert_eq!(&*gate.hidden(), &[hold.version()]);
    assert_eq!(
        gate.wait_open(Duration::from_millis(20)).await,
        None,
        "a fork must not capture its fork point now"
    );
    crate::bulk::show_load(fx.storage(), Some(hold), &Default::default());
    assert!(gate.hidden().is_empty());
    assert_ne!(gate.epoch(), before);
    bulk.abort().await.unwrap();
}

#[tokio::test]
async fn the_redrive_completes_a_committed_load_and_finishes_its_marker() {
    let fx = Fixture::new(true).await;
    let mut bulk = fx.bulk();
    let load_id = bulk.load_id().to_string();
    stage_load(&mut bulk, None).await;
    let plan = bulk.prepare_commit().await.unwrap();
    assert!(matches!(
        bulk.reach_commit_point(&plan).await,
        CommitPoint::Reached
    ));
    let hold = bulk.hide_load().await;
    let redrive = bulk.redrive(&plan, hold);
    let (_tx, rx) = tokio::sync::broadcast::channel(1);
    redrive.run(rx).await;

    assert_eq!(fx.load_rows().await, [4, 4, 4, 4]);
    assert!(fx.markers().await.is_empty());
    assert!(
        flush_intent::bulk_load_applied(fx.storage(), &load_id)
            .await
            .unwrap()
    );
    assert!(!fx.storage().bulk_gate().is_closed());
}

#[tokio::test]
async fn the_redrive_stops_on_shutdown_and_leaves_the_load_to_the_next_open() {
    let fx = Fixture::new(true).await;
    let mut bulk = fx.bulk();
    let load_id = bulk.load_id().to_string();
    stage_load(&mut bulk, None).await;
    let plan = bulk.prepare_commit().await.unwrap();
    assert!(matches!(
        bulk.reach_commit_point(&plan).await,
        CommitPoint::Reached
    ));
    let hold = bulk.hide_load().await;
    let redrive = bulk.redrive(&plan, hold);
    let (tx, rx) = tokio::sync::broadcast::channel(1);
    // Before the first attempt's backoff ends.
    tx.send(()).unwrap();
    redrive.run(rx).await;

    assert_eq!(fx.load_rows().await, [0; 4], "nothing published");
    assert!(
        fx.storage().bulk_gate().is_closed(),
        "the load stays hidden in this process"
    );
    assert_eq!(fx.markers().await, vec![format!("{load_id}.json")]);
    flush_intent::recover_interrupted_bulk_load(fx.storage())
        .await
        .unwrap();
    assert_eq!(
        fx.load_rows().await,
        [4, 4, 4, 4],
        "the next open completes it"
    );
}

#[tokio::test]
async fn an_endpoint_that_lost_its_label_before_the_publish_downgrades_the_type() {
    let fx = Fixture::new(true).await;
    let x = fx.committed_q().await;
    let mut bulk = fx.bulk();
    stage_load(&mut bulk, Some(x)).await;
    // While the load's edge to `x` is invisible, `x` loses `Q`.
    fx.writer()
        .update_vertex_with_labels(x, props(1000), &["P".to_string()], None)
        .await
        .unwrap();

    let stats = bulk.commit().await.unwrap();
    assert_eq!(fx.load_rows().await[2], 5, "the load is applied regardless");
    assert_eq!(
        fx.storage().schema_manager().schema().edge_types["E"].endpoint_conformance,
        EndpointConformance::Unverified
    );
    assert!(
        stats.warnings.iter().any(|w| w.contains("edge type 'E'")),
        "{:?}",
        stats.warnings
    );
}

#[tokio::test]
async fn recovered_load_endpoints_are_checked_with_the_writers_view() {
    let fx = Fixture::new(true).await;
    let x = fx.committed_q().await;
    let mut bulk = fx.bulk();
    stage_load(&mut bulk, Some(x)).await;
    let plan = bulk.prepare_commit().await.unwrap();
    assert!(matches!(
        bulk.reach_commit_point(&plan).await,
        CommitPoint::Reached
    ));
    std::mem::forget(bulk);
    let recovered = flush_intent::recover_interrupted_bulk_load(fx.storage())
        .await
        .unwrap();
    // The removal reaches only L0 (as a replayed WAL record would): storage
    // still says `x` carries `Q`.
    fx.writer()
        .update_vertex_with_labels(x, props(1000), &["P".to_string()], None)
        .await
        .unwrap();
    flush_intent::check_recovered_bulk_endpoints(fx.storage(), fx.writer(), true)
        .await
        .unwrap();
    assert_eq!(
        fx.storage().schema_manager().schema().edge_types["E"].endpoint_conformance,
        EndpointConformance::Unverified
    );
    // The marker waits for the load's events, which the database delivers.
    assert_eq!(recovered.len(), 1);
    assert_eq!(fx.markers().await.len(), 1);
    flush_intent::finish_recovered_load_events(fx.storage(), &recovered[0].load_id)
        .await
        .unwrap();
    assert!(fx.markers().await.is_empty());
}

/// A load whose marker records its events delivered is not handed back; one
/// left `events_pending` by an interrupted delivery is, on every open until
/// the delivery is finished. A marker whose endpoint check is still due (a
/// read-only open skipped it) outlives the delivery, for that check.
#[tokio::test]
async fn recovery_hands_back_only_loads_whose_events_are_pending() {
    let fx = Fixture::new(true).await;
    let markers = Markers::for_storage(fx.storage());
    let delivered_intent = BulkFlushIntent {
        phase: BulkIntentPhase::Committed,
        tables: BTreeMap::new(),
        snapshot_id: None,
        publish: None,
        labels: vec!["P".to_string()],
        version: Some(7),
        endpoints_checked: true,
        rows: Some(3),
        events_pending: false,
    };
    let pending_intent = BulkFlushIntent {
        events_pending: true,
        version: Some(9),
        ..delivered_intent.clone()
    };
    let earlier_intent = BulkFlushIntent {
        version: Some(8),
        ..pending_intent.clone()
    };
    let delivered = uuid::Uuid::new_v4().to_string();
    let pending = uuid::Uuid::new_v4().to_string();
    let earlier = uuid::Uuid::new_v4().to_string();
    for (id, intent) in [
        (&delivered, &delivered_intent),
        (&pending, &pending_intent),
        (&earlier, &earlier_intent),
    ] {
        flush_intent::write_for_test(&markers, id, intent)
            .await
            .unwrap();
    }

    for _ in 0..2 {
        let recovered = flush_intent::recover_interrupted_bulk_load(fx.storage())
            .await
            .unwrap();
        assert_eq!(
            recovered
                .iter()
                .map(|l| (l.load_id.as_str(), l.write_version, l.rows))
                .collect::<Vec<_>>(),
            [(earlier.as_str(), 8, 3), (pending.as_str(), 9, 3)],
            "pending loads only, in version order"
        );
    }
    assert_eq!(
        fx.markers().await.len(),
        2,
        "the delivered load is finished"
    );
    for id in [&earlier, &pending] {
        flush_intent::finish_recovered_load_events(fx.storage(), id)
            .await
            .unwrap();
    }
    assert!(fx.markers().await.is_empty());
    assert!(
        flush_intent::bulk_load_applied(fx.storage(), &pending)
            .await
            .unwrap()
    );

    let unchecked = uuid::Uuid::new_v4().to_string();
    let unchecked_intent = BulkFlushIntent {
        endpoints_checked: false,
        ..pending_intent
    };
    flush_intent::write_for_test(&markers, &unchecked, &unchecked_intent)
        .await
        .unwrap();
    flush_intent::finish_recovered_load_events(fx.storage(), &unchecked)
        .await
        .unwrap();
    let left = flush_intent::read_marker(&markers, &unchecked)
        .await
        .unwrap()
        .expect("kept for its endpoint check");
    assert!(!left.events_pending && !left.endpoints_checked);
}

#[tokio::test]
async fn only_conforming_declared_types_are_rechecked() {
    let fx = Fixture::new(true).await;
    let sm = fx.storage().schema_manager();
    let e = "E".to_string();
    assert_eq!(
        endpoint_check::conforming_types(fx.storage(), [&e]),
        vec![e.clone()]
    );
    sm.set_endpoint_conformance("E", EndpointConformance::Unverified)
        .unwrap();
    assert!(endpoint_check::conforming_types(fx.storage(), [&e]).is_empty());
    let tables = [
        table_names::delta_table_name("E", "fwd"),
        table_names::vertex_table_name("P"),
    ];
    assert_eq!(
        endpoint_check::edge_types_of_tables(fx.storage(), tables.iter()),
        vec![e]
    );
}
