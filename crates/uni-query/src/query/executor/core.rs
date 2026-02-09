// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

use anyhow::{Result, anyhow};
use std::collections::{HashMap, HashSet};
use uni_common::Value;
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::RwLock;
use uni_algo::algo::AlgorithmRegistry;
use uni_cypher::ast::Expr;
use uni_store::QueryContext;
use uni_store::runtime::l0_manager::L0Manager;
use uni_store::runtime::writer::Writer;
use uni_store::storage::manager::StorageManager;

use super::procedure::ProcedureRegistry;

#[derive(Debug)]
pub(crate) enum Accumulator {
    Count(i64),
    Sum(f64),
    Min(Option<f64>),
    Max(Option<f64>),
    Avg { sum: f64, count: i64 },
    Collect(Vec<Value>),
    CountDistinct(HashSet<String>),
}

/// Extract a numeric value from a Value as f64.
fn as_numeric(val: &Value) -> Option<f64> {
    val.as_f64()
}

/// Convert f64 to Value, preserving integer representation when possible.
fn numeric_to_value(val: f64) -> Value {
    if val.fract() == 0.0 && val >= i64::MIN as f64 && val <= i64::MAX as f64 {
        Value::Int(val as i64)
    } else {
        Value::Float(val)
    }
}

impl Accumulator {
    pub(crate) fn new(op: &str, distinct: bool) -> Self {
        let op_upper = op.to_uppercase();
        match op_upper.as_str() {
            "COUNT" if distinct => Accumulator::CountDistinct(HashSet::new()),
            "COUNT" => Accumulator::Count(0),
            "SUM" => Accumulator::Sum(0.0),
            "MIN" => Accumulator::Min(None),
            "MAX" => Accumulator::Max(None),
            "AVG" => Accumulator::Avg { sum: 0.0, count: 0 },
            "COLLECT" => Accumulator::Collect(Vec::new()),
            _ => Accumulator::Count(0),
        }
    }

    pub(crate) fn update(&mut self, val: &Value, is_wildcard: bool) {
        match self {
            Accumulator::Count(c) => {
                if is_wildcard || !val.is_null() {
                    *c += 1;
                }
            }
            Accumulator::Sum(s) => {
                if let Some(n) = as_numeric(val) {
                    *s += n;
                }
            }
            Accumulator::Min(current) => {
                if let Some(n) = as_numeric(val) {
                    *current = Some(current.map_or(n, |m| m.min(n)));
                }
            }
            Accumulator::Max(current) => {
                if let Some(n) = as_numeric(val) {
                    *current = Some(current.map_or(n, |m| m.max(n)));
                }
            }
            Accumulator::Avg { sum, count } => {
                if let Some(n) = as_numeric(val) {
                    *sum += n;
                    *count += 1;
                }
            }
            Accumulator::Collect(v) => {
                if !val.is_null() {
                    v.push(val.clone());
                }
            }
            Accumulator::CountDistinct(s) => {
                if !val.is_null() {
                    s.insert(val.to_string());
                }
            }
        }
    }

    pub(crate) fn finish(&self) -> Value {
        match self {
            Accumulator::Count(c) => Value::Int(*c),
            Accumulator::Sum(s) => numeric_to_value(*s),
            Accumulator::Min(opt) => opt.map_or(Value::Null, numeric_to_value),
            Accumulator::Max(opt) => opt.map_or(Value::Null, numeric_to_value),
            Accumulator::Avg { sum, count } => {
                if *count > 0 {
                    Value::Float(*sum / (*count as f64))
                } else {
                    Value::Null
                }
            }
            Accumulator::Collect(v) => Value::List(v.clone()),
            Accumulator::CountDistinct(s) => Value::Int(s.len() as i64),
        }
    }
}

/// Cache key for parsed generation expressions: (label_name, property_name)
pub(crate) type GenExprCacheKey = (String, String);

#[derive(Clone)]
pub struct Executor {
    pub(crate) storage: Arc<StorageManager>,
    pub(crate) writer: Option<Arc<RwLock<Writer>>>,
    pub(crate) l0_manager: Option<Arc<L0Manager>>,
    pub(crate) algo_registry: Arc<AlgorithmRegistry>,
    pub(crate) use_transaction: bool,
    /// File sandbox configuration for BACKUP/COPY/EXPORT commands
    pub(crate) file_sandbox: uni_common::config::FileSandboxConfig,
    pub(crate) config: uni_common::config::UniConfig,
    /// Cache for parsed generation expressions to avoid re-parsing on every row
    pub(crate) gen_expr_cache: Arc<RwLock<HashMap<GenExprCacheKey, Expr>>>,
    /// External procedure registry for test/user-defined procedures.
    pub(crate) procedure_registry: Option<Arc<ProcedureRegistry>>,
}

impl Executor {
    pub fn new(storage: Arc<StorageManager>) -> Self {
        Self {
            storage,
            writer: None,
            l0_manager: None,
            algo_registry: Arc::new(AlgorithmRegistry::new()),
            use_transaction: false,
            file_sandbox: uni_common::config::FileSandboxConfig::default(),
            config: uni_common::config::UniConfig::default(),
            gen_expr_cache: Arc::new(RwLock::new(HashMap::new())),
            procedure_registry: None,
        }
    }

    pub fn new_with_writer(storage: Arc<StorageManager>, writer: Arc<RwLock<Writer>>) -> Self {
        Self {
            storage,
            writer: Some(writer),
            l0_manager: None,
            algo_registry: Arc::new(AlgorithmRegistry::new()),
            use_transaction: false,
            file_sandbox: uni_common::config::FileSandboxConfig::default(),
            config: uni_common::config::UniConfig::default(),
            gen_expr_cache: Arc::new(RwLock::new(HashMap::new())),
            procedure_registry: None,
        }
    }

    /// Sets the external procedure registry for user-defined procedures.
    pub fn set_procedure_registry(&mut self, registry: Arc<ProcedureRegistry>) {
        self.procedure_registry = Some(registry);
    }

    /// Set the file sandbox configuration for BACKUP/COPY/EXPORT commands.
    /// MUST be called with sandboxed config in server mode.
    pub fn set_file_sandbox(&mut self, sandbox: uni_common::config::FileSandboxConfig) {
        self.file_sandbox = sandbox;
    }

    pub fn set_config(&mut self, config: uni_common::config::UniConfig) {
        self.config = config;
    }

    /// Validate a file path against the sandbox configuration.
    pub(crate) fn validate_path(&self, path: &str) -> Result<std::path::PathBuf> {
        self.file_sandbox
            .validate_path(path)
            .map_err(|e| anyhow!("Path validation failed: {}", e))
    }

    pub fn set_writer(&mut self, writer: Arc<RwLock<Writer>>) {
        self.writer = Some(writer);
    }

    pub fn set_use_transaction(&mut self, use_transaction: bool) {
        self.use_transaction = use_transaction;
    }

    pub(crate) async fn get_context(&self) -> Option<QueryContext> {
        if let Some(writer_lock) = &self.writer {
            let writer = writer_lock.read().await;
            // Include pending_flush L0s so data being flushed remains visible
            let mut ctx = QueryContext::new_with_pending(
                writer.l0_manager.get_current(),
                writer.transaction_l0.clone(),
                writer.l0_manager.get_pending_flush(),
            );
            ctx.set_deadline(Instant::now() + self.config.query_timeout);
            Some(ctx)
        } else {
            self.l0_manager.as_ref().map(|m| {
                let mut ctx = QueryContext::new(m.get_current());
                ctx.set_deadline(Instant::now() + self.config.query_timeout);
                ctx
            })
        }
    }

    pub(crate) fn compare_values(a: &Value, b: &Value) -> std::cmp::Ordering {
        match (a, b) {
            (Value::Int(i1), Value::Int(i2)) => i1.cmp(i2),
            (Value::Float(f1), Value::Float(f2)) => {
                f1.partial_cmp(f2).unwrap_or(std::cmp::Ordering::Equal)
            }
            (Value::Int(i), Value::Float(f)) => {
                (*i as f64).partial_cmp(f).unwrap_or(std::cmp::Ordering::Equal)
            }
            (Value::Float(f), Value::Int(i)) => {
                f.partial_cmp(&(*i as f64)).unwrap_or(std::cmp::Ordering::Equal)
            }
            (Value::String(s1), Value::String(s2)) => s1.cmp(s2),
            (Value::Bool(b1), Value::Bool(b2)) => b1.cmp(b2),
            (Value::Null, Value::Null) => std::cmp::Ordering::Equal,
            (Value::Null, _) => std::cmp::Ordering::Less,
            (_, Value::Null) => std::cmp::Ordering::Greater,
            _ => std::cmp::Ordering::Equal,
        }
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ProfileOutput {
    pub explain: crate::query::planner::ExplainOutput,
    pub runtime_stats: Vec<OperatorStats>,
    pub total_time_ms: u64,
    pub peak_memory_bytes: usize,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct OperatorStats {
    pub operator: String,
    pub actual_rows: usize,
    pub time_ms: f64,
    pub memory_bytes: usize,
    pub index_hits: Option<usize>,
    pub index_misses: Option<usize>,
}

impl Executor {
    /// Profiles query execution and returns results with timing statistics.
    ///
    /// Uses the DataFusion-based executor for query execution. Granular operator
    /// profiling will be added in a future release.
    pub async fn profile(
        &self,
        plan: crate::query::planner::LogicalPlan,
        params: &HashMap<String, Value>,
    ) -> Result<(Vec<HashMap<String, Value>>, ProfileOutput)> {
        // Generate ExplainOutput first
        let planner =
            crate::query::planner::QueryPlanner::new(self.storage.schema_manager().schema().into());
        let explain_output = planner.explain_logical_plan(&plan)?;

        let start = Instant::now();

        // Execute using the standard execute path (DataFusion-based)
        let prop_manager = self.create_prop_manager();
        let results = self.execute(plan.clone(), &prop_manager, params).await?;

        let total_time = start.elapsed();

        // Return aggregate stats (granular operator profiling to be added later)
        let stats = vec![OperatorStats {
            operator: "DataFusion Execution".to_string(),
            actual_rows: results.len(),
            time_ms: total_time.as_secs_f64() * 1000.0,
            memory_bytes: 0,
            index_hits: None,
            index_misses: None,
        }];

        Ok((
            results,
            ProfileOutput {
                explain: explain_output,
                runtime_stats: stats,
                total_time_ms: total_time.as_millis() as u64,
                peak_memory_bytes: 0,
            },
        ))
    }

    fn create_prop_manager(&self) -> uni_store::runtime::property_manager::PropertyManager {
        uni_store::runtime::property_manager::PropertyManager::new(
            self.storage.clone(),
            self.storage.schema_manager_arc(),
            1000,
        )
    }
}
