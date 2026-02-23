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

#[derive(World)]
#[world(init = Self::new)]
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

impl Default for UniWorld {
    fn default() -> Self {
        Self::new()
    }
}

impl UniWorld {
    pub fn new() -> Self {
        Self {
            db: None,
            _temp_dir: None,
            last_result: None,
            last_error: None,
            side_effects: SideEffects::default(),
            params: HashMap::new(),
        }
    }

    pub async fn init_db(&mut self) -> anyhow::Result<()> {
        // Keep DB init idempotent so chained Given steps operate on the same graph state.
        if self.db.is_some() {
            return Ok(());
        }

        // Use in_memory for fastest initialization
        // Disable background tasks for test databases (they create many short-lived instances)
        #[allow(clippy::field_reassign_with_default)]
        let config = {
            let mut config = uni_common::UniConfig::default();
            config.auto_flush_interval = None; // Disable auto-flush background task
            config.compaction.enabled = false; // Disable background compaction
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
        self.side_effects.node_ids_before = self.collect_node_ids().await;
        self.side_effects.edge_ids_before = self.collect_edge_ids().await;
        self.side_effects.nodes_before = self.side_effects.node_ids_before.len();
        self.side_effects.edges_before = self.side_effects.edge_ids_before.len();
        // Build a per-entity, per-key property snapshot for gross change counting.
        let snapshot = self.collect_property_snapshot().await;
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
        let node_ids_after = self.collect_node_ids().await;
        let edge_ids_after = self.collect_edge_ids().await;
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
        let snapshot = self.collect_property_snapshot().await;
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
    async fn collect_property_snapshot(&self) -> HashMap<String, Value> {
        let mut snapshot = HashMap::new();

        // Node properties
        if let Ok(result) = self.db().query("MATCH (n) RETURN n").await {
            for row in &result.rows {
                if let Some(node_val) = row.values.first() {
                    self.add_entity_to_snapshot(&mut snapshot, "n", node_val);
                }
            }
        }

        // Relationship properties
        if let Ok(result) = self.db().query("MATCH ()-[r]->() RETURN r").await {
            for row in &result.rows {
                if let Some(rel_val) = row.values.first() {
                    self.add_entity_to_snapshot(&mut snapshot, "r", rel_val);
                }
            }
        }

        snapshot
    }

    /// Adds (entity_id::prop_key -> value) entries from a single entity value
    /// to the snapshot map.  Handles both `Value::Map` and `Value::Node` /
    /// `Value::Edge` representations.
    fn add_entity_to_snapshot(
        &self,
        snapshot: &mut HashMap<String, Value>,
        prefix: &str,
        entity: &Value,
    ) {
        let insert_props =
            |snapshot: &mut HashMap<String, Value>, id: u64, props: &HashMap<String, Value>| {
                for (k, v) in props {
                    if !k.starts_with('_') && k != "ext_id" && !v.is_null() {
                        snapshot.insert(format!("{}:{}:{}", prefix, id, k), v.clone());
                    }
                }
            };

        match entity {
            Value::Map(map) => {
                let id = map
                    .get("_vid")
                    .or_else(|| map.get("_eid"))
                    .and_then(|v| v.as_u64());
                if let Some(id) = id {
                    insert_props(snapshot, id, map);
                }
            }
            Value::Node(node) => {
                insert_props(snapshot, u64::from(node.vid), &node.properties);
            }
            Value::Edge(edge) => {
                insert_props(snapshot, u64::from(edge.eid), &edge.properties);
            }
            _ => {}
        }
    }

    /// Collect all node IDs (VIDs) currently in the graph.
    async fn collect_node_ids(&self) -> HashSet<u64> {
        let mut ids = HashSet::new();
        if let Ok(result) = self.db().query("MATCH (n) RETURN id(n) AS id").await {
            for row in &result.rows {
                if let Some(Value::Int(id)) = row.values.first() {
                    ids.insert(*id as u64);
                }
            }
        }
        ids
    }

    /// Collect all edge IDs (EIDs) currently in the graph.
    async fn collect_edge_ids(&self) -> HashSet<u64> {
        let mut ids = HashSet::new();
        if let Ok(result) = self.db().query("MATCH ()-[r]->() RETURN id(r) AS id").await {
            for row in &result.rows {
                if let Some(Value::Int(id)) = row.values.first() {
                    ids.insert(*id as u64);
                }
            }
        }
        ids
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
        let result = self.db().query(query).await?;
        let mut all_labels = HashSet::new();
        for row in &result.rows {
            if let Ok(labels_list) = row.get::<Vec<String>>("labels") {
                for label in labels_list {
                    all_labels.insert(label);
                }
            }
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
