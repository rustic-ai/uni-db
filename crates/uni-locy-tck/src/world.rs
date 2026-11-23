use cucumber::World;
use std::cell::RefCell;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use tempfile::TempDir;
use uni_common::UniError;
use uni_db::Uni;
use uni_query::Value;

use uni_cypher::locy_ast::LocyProgram;
use uni_cypher::ParseError;

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
pub struct LocyWorld {
    db: Option<Arc<Uni>>,
    /// Temp directory that auto-cleans when LocyWorld is dropped.
    /// This prevents accumulating temp files during parallel TCK execution.
    _temp_dir: Option<TempDir>,
    last_parse: Option<Result<LocyProgram, ParseError>>,
    last_compile: Option<Result<uni_locy::CompiledProgram, UniError>>,
    last_locy_result: Option<Result<uni_locy::LocyResult, UniError>>,
    params: HashMap<String, Value>,
    /// Phase B Slice 3: per-scenario registry of mock neural classifiers
    /// injected by Given/When steps. Threaded into `LocyConfig` whenever
    /// a step builds one (see `steps/when_evaluate.rs`).
    pub classifier_registry: uni_locy::ClassifierRegistry,
    /// Phase B follow-up: per-model counters for the
    /// `counting mock classifier` Given step. Each call to
    /// `classify` on a registered counting mock increments the
    /// matching entry. Used by `Then ... classifier "<name>"
    /// should have been called N times` assertions.
    pub classifier_call_counts:
        std::collections::HashMap<String, std::sync::Arc<std::sync::atomic::AtomicUsize>>,
    /// Per-model record of every input a `recording mock classifier`
    /// received, for assertions on what the runtime passed it.
    pub classifier_inputs: std::collections::HashMap<
        String,
        std::sync::Arc<std::sync::Mutex<Vec<uni_locy::ClassifyInput>>>,
    >,
}

impl std::fmt::Debug for LocyWorld {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LocyWorld")
            .field("db", &"<Uni instance>")
            .field("_temp_dir", &self._temp_dir.as_ref().map(|d| d.path()))
            .field("last_parse", &self.last_parse.as_ref().map(|r| r.is_ok()))
            .field(
                "last_compile",
                &self.last_compile.as_ref().map(|r| r.is_ok()),
            )
            .field(
                "last_locy_result",
                &self.last_locy_result.as_ref().map(|r| r.is_ok()),
            )
            .field("params", &self.params)
            .finish()
    }
}

impl Default for LocyWorld {
    fn default() -> Self {
        Self::new()
    }
}

impl LocyWorld {
    pub fn new() -> Self {
        Self {
            db: None,
            _temp_dir: None,
            last_parse: None,
            last_compile: None,
            last_locy_result: None,
            params: HashMap::new(),
            classifier_registry: uni_locy::ClassifierRegistry::new(),
            classifier_call_counts: std::collections::HashMap::new(),
            classifier_inputs: std::collections::HashMap::new(),
        }
    }

    pub async fn init_db(&mut self) -> anyhow::Result<()> {
        // Keep DB init idempotent so chained Given steps operate on the same graph state.
        if self.db.is_some() {
            return Ok(());
        }

        // Use in_memory for fastest initialization. Disable background tasks
        // for test databases, which create many short-lived instances.
        let mut config = uni_common::UniConfig {
            auto_flush_interval: None, // Disable auto-flush background task
            ..Default::default()
        };
        config.compaction.enabled = false; // Disable background compaction
        apply_execution_overrides(&mut config);

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

    pub fn add_param(&mut self, key: String, value: Value) {
        self.params.insert(key, value);
    }

    pub fn params(&self) -> &HashMap<String, Value> {
        &self.params
    }

    // Locy-specific methods for parse result tracking
    pub fn set_parse_result(&mut self, result: Result<LocyProgram, ParseError>) {
        self.last_parse = Some(result);
    }

    pub fn parse_result(&self) -> Option<&Result<LocyProgram, ParseError>> {
        self.last_parse.as_ref()
    }

    // Compile result tracking
    pub fn set_compile_result(&mut self, result: Result<uni_locy::CompiledProgram, UniError>) {
        self.last_compile = Some(result);
    }

    pub fn compile_result(&self) -> Option<&Result<uni_locy::CompiledProgram, UniError>> {
        self.last_compile.as_ref()
    }

    // Evaluate result tracking
    pub fn set_locy_result(&mut self, result: Result<uni_locy::LocyResult, UniError>) {
        self.last_locy_result = Some(result);
    }

    pub fn locy_result(&self) -> Option<&Result<uni_locy::LocyResult, UniError>> {
        self.last_locy_result.as_ref()
    }

    /// Unwrap the last successful Locy evaluation, panicking with a clear
    /// message if none was captured or evaluation failed.
    pub fn expect_locy_ok(&self) -> &uni_locy::LocyResult {
        self.locy_result()
            .expect("No evaluation result found")
            .as_ref()
            .expect("Evaluation failed")
    }

    /// Borrow the command result at `idx`, panicking with a clear message if
    /// no evaluation ran or the index is out of bounds.
    pub fn expect_command_result(&self, idx: usize) -> &uni_locy::result::CommandResult {
        self.expect_locy_ok()
            .command_results
            .get(idx)
            .unwrap_or_else(|| panic!("No command result at index {}", idx))
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
