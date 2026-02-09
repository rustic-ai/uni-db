use cucumber::World;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tempfile::TempDir;
use uni_common::UniError;
use uni_db::Uni;
use uni_query::{QueryResult, Value};

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

#[derive(Debug, Default, Clone)]
pub struct SideEffects {
    pub nodes_before: usize,
    pub nodes_after: usize,
    pub edges_before: usize,
    pub edges_after: usize,
    pub properties_before: usize,
    pub properties_after: usize,
    pub labels_before: HashSet<String>,
    pub labels_after: HashSet<String>,
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
        // Create a temp directory that auto-cleans when UniWorld is dropped.
        // This prevents accumulating temp files during parallel TCK execution.
        let temp_dir = TempDir::new()?;
        let db = Uni::open(temp_dir.path().to_string_lossy().to_string())
            .build()
            .await?;
        self.db = Some(Arc::new(db));
        self._temp_dir = Some(temp_dir);
        Ok(())
    }

    pub fn db(&self) -> &Arc<Uni> {
        self.db.as_ref().expect("Database not initialized")
    }

    /// Capture graph state before a mutation for side-effect tracking.
    pub async fn capture_state_before(&mut self) -> anyhow::Result<()> {
        let (nodes, edges, node_props, rel_props, labels) = tokio::join!(
            self.count_by_query("MATCH (n) RETURN count(n) as count"),
            self.count_by_query("MATCH ()-[r]->() RETURN count(r) as count"),
            self.count_by_query("MATCH (n) UNWIND keys(n) AS k RETURN count(k) AS count"),
            self.count_by_query("MATCH ()-[r]->() UNWIND keys(r) AS k RETURN count(k) AS count"),
            self.get_labels(),
        );
        self.side_effects.nodes_before = nodes;
        self.side_effects.edges_before = edges;
        self.side_effects.properties_before = node_props + rel_props;
        self.side_effects.labels_before = labels?;
        Ok(())
    }

    /// Capture graph state after a mutation for side-effect tracking.
    pub async fn capture_state_after(&mut self) -> anyhow::Result<()> {
        let (nodes, edges, node_props, rel_props, labels) = tokio::join!(
            self.count_by_query("MATCH (n) RETURN count(n) as count"),
            self.count_by_query("MATCH ()-[r]->() RETURN count(r) as count"),
            self.count_by_query("MATCH (n) UNWIND keys(n) AS k RETURN count(k) AS count"),
            self.count_by_query("MATCH ()-[r]->() UNWIND keys(r) AS k RETURN count(k) AS count"),
            self.get_labels(),
        );
        self.side_effects.nodes_after = nodes;
        self.side_effects.edges_after = edges;
        self.side_effects.properties_after = node_props + rel_props;
        self.side_effects.labels_after = labels?;
        Ok(())
    }

    /// Run a count query and extract the integer result, returning 0 on failure.
    async fn count_by_query(&self, query: &str) -> usize {
        let Ok(result) = self.db().query(query).await else {
            return 0;
        };
        result
            .rows
            .first()
            .and_then(|row| row.values.first())
            .and_then(|v| match v {
                Value::Int(count) => Some(*count as usize),
                _ => None,
            })
            .unwrap_or(0)
    }

    async fn get_labels(&self) -> anyhow::Result<HashSet<String>> {
        let labels = self.db().list_labels().await?;
        Ok(labels.into_iter().collect())
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
