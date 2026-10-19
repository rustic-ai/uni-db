use cucumber::World;
use std::cell::RefCell;
use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::Arc;
use tempfile::TempDir;
use uni_common::UniError;
use uni_db::Uni;
use uni_query::{QueryResult, Value};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum TckSchemaMode {
    #[default]
    Schemaless,
    Sidecar,
}

#[derive(Debug, Clone, Default)]
struct TckRunContext {
    feature_path: Option<PathBuf>,
    schema_mode: TckSchemaMode,
}

thread_local! {
    static TCK_RUN_CONTEXT: RefCell<TckRunContext> = RefCell::new(TckRunContext::default());
}

pub fn set_tck_run_context_for_current_thread(feature_path: PathBuf, schema_mode: TckSchemaMode) {
    TCK_RUN_CONTEXT.with(|ctx| {
        *ctx.borrow_mut() = TckRunContext {
            feature_path: Some(feature_path),
            schema_mode,
        };
    });
}

pub fn clear_tck_run_context_for_current_thread() {
    TCK_RUN_CONTEXT.with(|ctx| {
        *ctx.borrow_mut() = TckRunContext::default();
    });
}

fn get_tck_run_context_for_current_thread() -> TckRunContext {
    TCK_RUN_CONTEXT.with(|ctx| ctx.borrow().clone())
}

#[derive(World, Default)]
pub struct UniWorld {
    db: Option<Arc<Uni>>,
    /// Temp directory that auto-cleans when UniWorld is dropped.
    /// This prevents accumulating temp files during parallel TCK execution.
    _temp_dir: Option<TempDir>,
    last_result: Option<QueryResult>,
    last_error: Option<UniError>,
    side_effects: SideEffects,
    params: HashMap<String, Value>,
}

impl std::fmt::Debug for UniWorld {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("UniWorld")
            .field("db", &"<Uni instance>")
            .field("_temp_dir", &self._temp_dir.as_ref().map(|d| d.path()))
            .field("last_result", &self.last_result)
            .field("last_error", &self.last_error)
            .field("side_effects", &self.side_effects)
            .field("params", &self.params)
            .finish()
    }
}

/// Graph state snapshot used to compute side-effect counts across a mutation.
///
/// Property change events (`+properties` / `-properties`) require per-property
/// comparison: a property that is overwritten with a different value counts as
/// both a removal of the old value and an addition of the new value.  The
/// aggregate `properties_before` / `properties_after` fields capture the total
/// number of non-null property assignments at each point in time for use by
/// the gross-event counters in `and.rs`.
#[derive(Debug, Default, Clone)]
pub struct SideEffects {
    pub nodes_before: usize,
    pub nodes_after: usize,
    pub edges_before: usize,
    pub edges_after: usize,
    /// Gross node creations: node IDs present after but not before.
    pub nodes_created: usize,
    /// Gross node deletions: node IDs present before but not after.
    pub nodes_deleted: usize,
    /// Gross edge creations: edge IDs present after but not before.
    pub edges_created: usize,
    /// Gross edge deletions: edge IDs present before but not after.
    pub edges_deleted: usize,
    /// Total non-null properties at snapshot time (before mutation).
    pub properties_before: usize,
    /// Total non-null properties at snapshot time (after mutation).
    pub properties_after: usize,
    /// Gross property additions: (entity_id, prop_key) pairs with a non-null
    /// value that either did not exist before or had a different value.
    pub properties_added: usize,
    /// Gross property removals: (entity_id, prop_key) pairs that had a
    /// non-null value before but are null/absent after, OR had a different
    /// non-null value before (value was overwritten).
    pub properties_removed: usize,
    pub labels_before: HashSet<String>,
    pub labels_after: HashSet<String>,
    /// Per-entity, per-property value snapshot (before).  Key format:
    /// `"<vid>:<prop>"` for vertices and `"<eid>:<prop>"` for edges.
    /// Only non-null values are stored.
    prop_snapshot_before: HashMap<String, Value>,
    /// Per-entity, per-property value snapshot (after).
    prop_snapshot_after: HashMap<String, Value>,
    /// Node ID set (before) for gross change tracking.
    node_ids_before: HashSet<u64>,
    /// Edge ID set (before) for gross change tracking.
    edge_ids_before: HashSet<u64>,
}

impl UniWorld {
    pub async fn init_db(&mut self) -> anyhow::Result<()> {
        // Keep DB init idempotent so chained Given steps operate on the same graph state.
        if self.db.is_some() {
            return Ok(());
        }

        // Use in_memory for fastest initialization. Disable background tasks for
        // test databases (they create many short-lived instances).
        #[expect(
            clippy::field_reassign_with_default,
            reason = "UniConfig has no builder for these fields; reassigning defaults is clearest"
        )]
        let config = {
            let mut config = uni_common::UniConfig::default();
            config.auto_flush_interval = None; // Disable auto-flush background task
            config.compaction.enabled = false; // Disable background compaction
            apply_execution_overrides(&mut config);
            config
        };

        let db = Uni::in_memory().config(config).build().await?;
        let run_ctx = get_tck_run_context_for_current_thread();
        if run_ctx.schema_mode == TckSchemaMode::Sidecar {
            let feature_path = run_ctx.feature_path.ok_or_else(|| {
                anyhow::anyhow!("TCK schema sidecar mode requires feature path run context")
            })?;
            let schema_path = feature_path.with_extension("schema.json");
            if !schema_path.exists() {
                anyhow::bail!(
                    "Missing sidecar schema for feature '{}': '{}'",
                    feature_path.display(),
                    schema_path.display()
                );
            }
            db.load_schema(&schema_path).await?;
        }

        self.db = Some(Arc::new(db));
        Ok(())
    }

    pub fn db(&self) -> &Arc<Uni> {
        self.db.as_ref().expect("Database not initialized")
    }

    /// Capture graph state before a mutation for side-effect tracking.
    ///
    /// Uses sequential queries to avoid any potential lock contention.
    /// Property counting is included for TCK compliance.
    pub async fn capture_state_before(&mut self) -> anyhow::Result<()> {
        // Collect node/edge ID sets for gross creation/deletion tracking.
        self.side_effects.node_ids_before = self.collect_node_ids().await?;
        self.side_effects.edge_ids_before = self.collect_edge_ids().await?;
        self.side_effects.nodes_before = self.side_effects.node_ids_before.len();
        self.side_effects.edges_before = self.side_effects.edge_ids_before.len();
        // Build a per-entity, per-key property snapshot for gross change counting.
        let snapshot = self.collect_property_snapshot().await?;
        self.side_effects.properties_before = snapshot.len();
        self.side_effects.prop_snapshot_before = snapshot;
        self.side_effects.labels_before = self.get_labels().await?;
        Ok(())
    }

    /// Capture graph state after a mutation for side-effect tracking.
    ///
    /// Uses sequential queries to avoid any potential lock contention.
    /// Property counting is included for TCK compliance.
    pub async fn capture_state_after(&mut self) -> anyhow::Result<()> {
        // Collect node/edge ID sets and compute gross changes.
        let node_ids_after = self.collect_node_ids().await?;
        let edge_ids_after = self.collect_edge_ids().await?;
        self.side_effects.nodes_after = node_ids_after.len();
        self.side_effects.edges_after = edge_ids_after.len();
        self.side_effects.nodes_created = node_ids_after
            .difference(&self.side_effects.node_ids_before)
            .count();
        self.side_effects.nodes_deleted = self
            .side_effects
            .node_ids_before
            .difference(&node_ids_after)
            .count();
        self.side_effects.edges_created = edge_ids_after
            .difference(&self.side_effects.edge_ids_before)
            .count();
        self.side_effects.edges_deleted = self
            .side_effects
            .edge_ids_before
            .difference(&edge_ids_after)
            .count();
        // Build after snapshot and compute gross change counts.
        let snapshot = self.collect_property_snapshot().await?;
        self.side_effects.properties_after = snapshot.len();
        self.side_effects.prop_snapshot_after = snapshot;

        // Gross additions: (entity, key) that is in AFTER but wasn't in BEFORE
        // with the same value.
        let before = &self.side_effects.prop_snapshot_before;
        let after = &self.side_effects.prop_snapshot_after;
        let mut added = 0usize;
        let mut removed = 0usize;
        for (k, v_after) in after {
            match before.get(k) {
                None => added += 1,
                Some(v_before) if v_before != v_after => {
                    added += 1;
                    removed += 1;
                }
                _ => {}
            }
        }
        for k in before.keys() {
            if !after.contains_key(k) {
                removed += 1;
            }
        }
        self.side_effects.properties_added = added;
        self.side_effects.properties_removed = removed;
        self.side_effects.labels_after = self.get_labels().await?;
        Ok(())
    }

    /// Collect all (entity_id::prop_key → value) pairs across all nodes and
    /// relationships.  Only non-null values are included.
    ///
    /// Key format: `"n:<vid>:<prop>"` for vertices, `"r:<eid>:<prop>"` for edges.
    ///
    /// # Errors
    /// Propagates the snapshot query error. A swallowed error here would report a
    /// partial/empty snapshot, silently corrupting the before/after side-effect
    /// diff (a failing query is indistinguishable from a graph with no
    /// properties), mirroring the sibling `collect_ids`.
    async fn collect_property_snapshot(&self) -> anyhow::Result<HashMap<String, Value>> {
        let mut snapshot = HashMap::new();

        // Node properties
        let result = self.db().session().query("MATCH (n) RETURN n").await?;
        for row in result.rows() {
            if let Some(node_val) = row.values().first() {
                Self::add_entity_to_snapshot(&mut snapshot, "n", node_val)?;
            }
        }

        // Relationship properties
        let result = self
            .db()
            .session()
            .query("MATCH ()-[r]->() RETURN r")
            .await?;
        for row in result.rows() {
            if let Some(rel_val) = row.values().first() {
                Self::add_entity_to_snapshot(&mut snapshot, "r", rel_val)?;
            }
        }

        Ok(snapshot)
    }

    /// Adds (entity_id::prop_key -> value) entries from a single entity value
    /// to the snapshot map.  Handles both `Value::Map` and `Value::Node` /
    /// `Value::Edge` representations.
    ///
    /// # Errors
    /// Fails on a value that is not an entity, or a map that carries no entity
    /// identity. Skipping it would leave that entity's properties out of the
    /// snapshot, and a side-effect assertion would then pass on a diff that
    /// never saw them.
    fn add_entity_to_snapshot(
        snapshot: &mut HashMap<String, Value>,
        prefix: &str,
        entity: &Value,
    ) -> anyhow::Result<()> {
        let insert_props =
            |snapshot: &mut HashMap<String, Value>, id: u64, props: &HashMap<String, Value>| {
                for (k, v) in props {
                    if !uni_common::core::schema::is_reserved_property_name(k)
                        && k != "ext_id"
                        && !v.is_null()
                    {
                        snapshot.insert(format!("{}:{}:{}", prefix, id, k), v.clone());
                    }
                }
            };

        match entity {
            Value::Map(map) => {
                // Narrow: missed `_id`, the serde string forms, and did no
                // vertex/edge disambiguation. The snapshot key is `prefix:id:prop`,
                // so a map carrying only `_id` contributed zero property entries
                // and a TCK side-effect assertion passed on an empty snapshot —
                // failing open (#234).
                let id = uni_common::value::entity_ref_from_map(map).map(|e| match e {
                    uni_common::value::EntityRef::Vertex(vid) => vid.as_u64(),
                    uni_common::value::EntityRef::Edge(eid) => eid.as_u64(),
                });
                let Some(id) = id else {
                    anyhow::bail!("snapshot row is a map without an entity identity: {map:?}");
                };
                insert_props(snapshot, id, map);
            }
            Value::Node(node) => {
                insert_props(snapshot, u64::from(node.vid), &node.properties);
            }
            Value::Edge(edge) => {
                insert_props(snapshot, u64::from(edge.eid), &edge.properties);
            }
            other => anyhow::bail!("snapshot row is not an entity: {other:?}"),
        }
        Ok(())
    }

    /// Collect all node IDs (VIDs) currently in the graph.
    async fn collect_node_ids(&self) -> anyhow::Result<HashSet<u64>> {
        self.collect_ids("MATCH (n) RETURN id(n) AS id").await
    }

    /// Collect all edge IDs (EIDs) currently in the graph.
    async fn collect_edge_ids(&self) -> anyhow::Result<HashSet<u64>> {
        self.collect_ids("MATCH ()-[r]->() RETURN id(r) AS id")
            .await
    }

    /// Run an `id`-returning query and collect the integer IDs into a set.
    ///
    /// # Errors
    /// Propagates the snapshot query error, and fails on a row whose first
    /// column is not a non-negative integer. Either swallowed would under-count
    /// ids, silently corrupting the before/after side-effect diff (a failing
    /// query is indistinguishable from an empty graph).
    async fn collect_ids(&self, query: &str) -> anyhow::Result<HashSet<u64>> {
        let result = self.db().session().query(query).await?;
        let mut ids = HashSet::new();
        for row in result.rows() {
            match row.values().first() {
                Some(Value::Int(id)) => {
                    let id = u64::try_from(*id)
                        .map_err(|_| anyhow::anyhow!("snapshot id {id} is negative"))?;
                    ids.insert(id);
                }
                other => anyhow::bail!("snapshot id is not an integer: {other:?}"),
            }
        }
        Ok(ids)
    }

    /// Get labels present in data (not schema metadata) for side-effect tracking.
    ///
    /// TCK side effects track data-level label presence: a label is "+created"
    /// when the first vertex with that label appears, and "-removed" when the
    /// last vertex with that label is deleted. This must NOT include
    /// schema-registered labels that have no data yet, otherwise the
    /// before/after diff is always zero in schema-aware mode.
    async fn get_labels(&self) -> anyhow::Result<HashSet<String>> {
        let query = "MATCH (n) RETURN DISTINCT labels(n) AS labels";
        let result = self.db().session().query(query).await?;
        let mut all_labels = HashSet::new();
        for row in result.rows() {
            // A row that does not decode is an error, not "no labels": skipping it
            // would hide a created or removed label from the side-effect diff.
            all_labels.extend(row.get::<Vec<String>>("labels")?);
        }
        Ok(all_labels)
    }

    pub fn set_result(&mut self, result: QueryResult) {
        self.last_result = Some(result);
        self.last_error = None;
    }

    pub fn set_error(&mut self, error: UniError) {
        self.last_error = Some(error);
        self.last_result = None;
    }

    pub fn result(&self) -> Option<&QueryResult> {
        self.last_result.as_ref()
    }

    pub fn error(&self) -> Option<&UniError> {
        self.last_error.as_ref()
    }

    pub fn side_effects(&self) -> &SideEffects {
        &self.side_effects
    }

    pub fn add_param(&mut self, key: String, value: Value) {
        self.params.insert(key, value);
    }

    pub fn params(&self) -> &HashMap<String, Value> {
        &self.params
    }
}

/// Applies `UNI_TCK_PARALLELISM` and `UNI_TCK_EXECUTION_BATCH_SIZE`, when set.
///
/// Both knobs are result-neutral, so the suite must pass unchanged under any
/// value; running it with a single partition and a tiny engine batch is a
/// determinism check that reuses every scenario's expected result. A value
/// that does not parse fails loudly rather than silently running the defaults.
fn apply_execution_overrides(config: &mut uni_common::UniConfig) {
    let read = |name: &str| {
        std::env::var(name).ok().map(|v| {
            v.parse::<usize>()
                .unwrap_or_else(|_| panic!("{name} must be a positive integer, got {v:?}"))
        })
    };
    if let Some(p) = read("UNI_TCK_PARALLELISM") {
        config.parallelism = p;
    }
    if let Some(b) = read("UNI_TCK_EXECUTION_BATCH_SIZE") {
        config.execution_batch_size = Some(b);
    }
}

#[cfg(test)]
mod tests {
    //! The side-effect snapshot helpers are private and run fixed introspection
    //! queries that never fail against a healthy database, so these tests drive
    //! them directly with the inputs a healthy database cannot produce.

    use super::*;

    async fn world_with_two_nodes() -> UniWorld {
        let mut world = UniWorld::default();
        world.init_db().await.expect("in-memory database");
        let session = world.db().session();
        let tx = session.tx().await.expect("open a transaction");
        tx.execute("CREATE (:A {p: 1}), (:B)")
            .await
            .expect("seed the graph");
        tx.commit().await.expect("commit the seed");
        world
    }

    /// The control for the refusals below: a healthy introspection query
    /// returns every id, so an `Err` from them is not a helper that always fails.
    #[tokio::test]
    async fn collect_ids_returns_every_id_of_a_healthy_query() {
        let world = world_with_two_nodes().await;
        let ids = world.collect_node_ids().await.expect("healthy query");
        assert_eq!(ids.len(), 2);
    }

    /// A failing introspection query must fail the step. Swallowed, it reported
    /// an empty id set, so every surviving entity looked newly created.
    #[tokio::test]
    async fn collect_ids_propagates_a_query_error() {
        let world = world_with_two_nodes().await;
        let err = world
            .collect_ids("MATCH (n RETURN id(n) AS id")
            .await
            .expect_err("a query that does not parse must not read as an empty graph");
        assert!(!err.to_string().is_empty());
    }

    /// A row whose id is not an integer was skipped, under-counting the graph.
    #[tokio::test]
    async fn collect_ids_refuses_a_non_integer_id() {
        let world = world_with_two_nodes().await;
        let err = world
            .collect_ids("MATCH (n) RETURN toString(id(n)) AS id")
            .await
            .expect_err("a string id must not be dropped from the set");
        assert!(err.to_string().contains("not an integer"), "{err}");
    }

    /// A snapshot row that is not an entity was skipped, dropping its
    /// properties from the before/after diff.
    #[test]
    fn snapshot_refuses_a_non_entity_row() {
        let mut snapshot = HashMap::new();
        let err = UniWorld::add_entity_to_snapshot(&mut snapshot, "n", &Value::Int(7))
            .expect_err("a scalar is not an entity");
        assert!(err.to_string().contains("not an entity"), "{err}");

        let mut anonymous = HashMap::new();
        anonymous.insert("p".to_string(), Value::Int(1));
        let err = UniWorld::add_entity_to_snapshot(&mut snapshot, "n", &Value::Map(anonymous))
            .expect_err("a map without an identity is not an entity");
        assert!(err.to_string().contains("entity identity"), "{err}");
        assert!(snapshot.is_empty());
    }

    /// The full capture over a healthy graph counts nodes, properties and
    /// labels, so the stricter helpers do not refuse what the TCK produces. Any
    /// `Err` they return reaches `run_query_step`, which panics the step.
    #[tokio::test]
    async fn capture_state_before_succeeds_on_a_healthy_graph() {
        let mut world = world_with_two_nodes().await;
        world.capture_state_before().await.expect("healthy capture");
        assert_eq!(world.side_effects().nodes_before, 2);
        assert_eq!(world.side_effects().properties_before, 1);
        assert_eq!(world.side_effects().labels_before.len(), 2);
    }
}
