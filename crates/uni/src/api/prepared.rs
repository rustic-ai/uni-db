// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Prepared statements for repeated query and Locy program execution.
//!
//! `PreparedQuery` caches the parsed AST and logical plan so that repeated
//! executions skip the parse and plan phases. `PreparedLocy` caches the
//! compiled program. Both transparently refresh if the schema changes.

use std::collections::HashMap;
use std::sync::Arc;

use crate::api::UniInner;
use crate::api::hooks::{QueryType, SessionHook};
use crate::api::impl_locy::{LocyRuleRegistry, merge_registered_rules};
use crate::api::locy_result::LocyResult;
use uni_common::{Result, UniError, Value};
use uni_locy::LocyConfig;
use uni_query::QueryResult;

/// Authorization + before-query-hook context captured from the preparing
/// `Session`/`Transaction`, so each prepared execution re-runs the same guards
/// the live `Session::query` / `Transaction::execute` paths apply.
///
/// Without this, a `PreparedQuery` executed an `AuthzPolicy`-governed statement
/// (and fired before-query hooks) only at prepare time — never on the repeated
/// executions, letting a cached handle bypass authorization (review #5b).
pub(crate) struct PreparedGuards {
    principal: Option<Arc<uni_plugin::traits::connector::Principal>>,
    hooks: Vec<Arc<dyn SessionHook>>,
    session_id: String,
    /// Authorization verb: `"read"` for session-prepared queries, the
    /// statement's classified verb for transaction-bound ones.
    verb: String,
}

impl PreparedGuards {
    /// Build the guard context for a session-prepared (read-only) query.
    pub(crate) fn for_session(
        principal: Option<Arc<uni_plugin::traits::connector::Principal>>,
        hooks: Vec<Arc<dyn SessionHook>>,
        session_id: String,
    ) -> Self {
        Self {
            principal,
            hooks,
            session_id,
            verb: "read".to_string(),
        }
    }

    /// Build the guard context for a transaction-bound query under `verb`.
    pub(crate) fn for_transaction(
        principal: Option<Arc<uni_plugin::traits::connector::Principal>>,
        hooks: Vec<Arc<dyn SessionHook>>,
        session_id: String,
        verb: String,
    ) -> Self {
        Self {
            principal,
            hooks,
            session_id,
            verb,
        }
    }

    /// Run authorization and before-query hooks against `db`.
    fn run(&self, db: &UniInner, cypher: &str, params: &HashMap<String, Value>) -> Result<()> {
        crate::api::session::authorize_query(db, self.principal.as_deref(), cypher, &self.verb)?;
        crate::api::session::run_before_query_hooks_raw(
            db,
            &self.hooks,
            &self.session_id,
            cypher,
            QueryType::Cypher,
            params,
        )
    }
}

// ── PreparedQuery ─────────────────────────────────────────────────

/// Interior state for schema-staleness detection.
struct PreparedQueryInner {
    ast: uni_query::CypherQuery,
    plan: uni_query::LogicalPlan,
    schema_version: u32,
}

/// Transaction context binding a [`PreparedQuery`] to a live transaction.
///
/// Carries the transaction's private L0 buffer, id reservoir, and the
/// (lazily-pinned, hence shared) read snapshot, so a `tx.prepare(...)` query's
/// reads see the transaction's uncommitted writes and its writes land in
/// `tx_l0` — undone by `rollback()` instead of leaking into main L0.
pub(crate) struct PreparedTxBinding {
    pub(crate) tx_l0: Arc<parking_lot::RwLock<uni_store::runtime::l0::L0Buffer>>,
    pub(crate) id_reservoir: Arc<uni_store::runtime::TxIdReservoir>,
    pub(crate) snapshot: Arc<parking_lot::Mutex<Option<uni_store::runtime::SnapshotView>>>,
}

/// A prepared Cypher query with a cached logical plan.
///
/// Created via [`Session::prepare()`](crate::api::session::Session::prepare)
/// (read-only) or [`Transaction::prepare()`](crate::api::transaction::Transaction::prepare)
/// (bound to the transaction). The plan is cached and reused across executions;
/// if the database schema changes, the plan is automatically regenerated.
///
/// All methods take `&self`, so a `PreparedQuery` can be shared across
/// threads via `Arc<PreparedQuery>`.
pub struct PreparedQuery {
    db: Arc<UniInner>,
    query_text: String,
    /// `Some` for a transaction-bound query (writes allowed, routed to the
    /// tx's L0); `None` for a session-prepared query (validated read-only).
    tx: Option<PreparedTxBinding>,
    /// The scope this statement executes under, captured from the session or
    /// transaction that prepared it. Without it a prepared query would be the
    /// one execution path `cancel()` could not reach.
    cancel: crate::api::impl_query::CancelScope,
    /// Authorization + before-query-hook context, replayed on every execution.
    guards: PreparedGuards,
    inner: std::sync::RwLock<PreparedQueryInner>,
}

impl std::fmt::Debug for PreparedQuery {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PreparedQuery")
            .field("query_text", &self.query_text)
            .field("tx_bound", &self.tx.is_some())
            .finish()
    }
}

impl PreparedQuery {
    /// Create a read-only prepared query for a session.
    ///
    /// # Errors
    /// Returns an error if the Cypher fails to parse or plan, or if it contains
    /// a mutation — session-prepared queries are read-only (mutations require a
    /// transaction, which provides isolation, WAL protection, and commit hooks).
    pub(crate) async fn new(
        db: Arc<UniInner>,
        cypher: &str,
        guards: PreparedGuards,
        cancel: crate::api::impl_query::CancelScope,
    ) -> Result<Self> {
        let ast = parse_cypher(cypher)?;
        // Session-prepared queries must be read-only, mirroring `Session::query`.
        uni_query::validate_read_only(&ast).map_err(|_| UniError::Query {
            message: "Prepared session query is read-only. Mutation clauses (CREATE, MERGE, \
                 DELETE, SET, REMOVE) require a transaction. Use session.tx().prepare()."
                .to_string(),
            query: Some(cypher.to_string()),
        })?;
        Self::build(db, cypher, ast, None, guards, cancel)
    }

    /// Create a transaction-bound prepared query (mutations allowed, routed to
    /// the transaction's private L0).
    pub(crate) async fn new_tx_bound(
        db: Arc<UniInner>,
        cypher: &str,
        binding: PreparedTxBinding,
        guards: PreparedGuards,
        cancel: crate::api::impl_query::CancelScope,
    ) -> Result<Self> {
        let ast = parse_cypher(cypher)?;
        Self::build(db, cypher, ast, Some(binding), guards, cancel)
    }

    /// Plan `ast` and assemble the prepared query.
    fn build(
        db: Arc<UniInner>,
        cypher: &str,
        ast: uni_query::CypherQuery,
        tx: Option<PreparedTxBinding>,
        guards: PreparedGuards,
        cancel: crate::api::impl_query::CancelScope,
    ) -> Result<Self> {
        let schema_version = db.schema.schema().schema_version;
        let planner = uni_query::QueryPlanner::new(db.schema.schema().clone());
        let plan = planner.plan(ast.clone()).map_err(|e| UniError::Query {
            message: e.to_string(),
            query: Some(cypher.to_string()),
        })?;

        Ok(Self {
            db,
            query_text: cypher.to_string(),
            tx,
            guards,
            cancel,
            inner: std::sync::RwLock::new(PreparedQueryInner {
                ast,
                plan,
                schema_version,
            }),
        })
    }

    /// Execute the prepared query with the given parameters.
    ///
    /// If the schema has changed since preparation, the query is
    /// transparently re-planned before execution.
    pub async fn execute(&self, params: &[(&str, Value)]) -> Result<QueryResult> {
        let param_map: HashMap<String, Value> = params
            .iter()
            .map(|(k, v)| (k.to_string(), v.clone()))
            .collect();
        self.execute_with_params(param_map).await
    }

    /// Shared execution for both `execute` and the binder.
    ///
    /// A transaction-bound query routes through the tx write path so its reads
    /// see uncommitted writes and its writes land in the tx's L0; a session
    /// query runs the cached read-only plan against the committed store.
    async fn execute_with_params(&self, params: HashMap<String, Value>) -> Result<QueryResult> {
        // Re-run authorization + before-query hooks on every execution, matching
        // the live `Session::query` / `Transaction::execute` paths — a cached
        // prepared handle must not bypass an `AuthzPolicy` or hook (review #5b).
        self.guards.run(&self.db, &self.query_text, &params)?;

        if let Some(tx) = &self.tx {
            // Read the live snapshot, dropping the (non-Send) guard before the
            // await so the returned future stays `Send`.
            let snapshot = tx.snapshot.lock().clone();
            return self
                .db
                .execute_internal_with_tx_l0(
                    &self.query_text,
                    params,
                    tx.tx_l0.clone(),
                    Some(tx.id_reservoir.clone()),
                    snapshot,
                    self.cancel.clone(),
                )
                .await;
        }

        self.ensure_plan_fresh()?;
        let plan = {
            let inner = self.inner.read().unwrap();
            inner.plan.clone()
        };

        self.db
            .execute_plan_internal(
                plan,
                &self.query_text,
                params,
                self.db.config.clone(),
                self.cancel.clone(),
            )
            .await
    }

    /// Fluent parameter builder for executing a prepared query.
    pub fn bind(&self) -> PreparedQueryBinder<'_> {
        PreparedQueryBinder {
            prepared: self,
            params: HashMap::new(),
        }
    }

    /// The original query text.
    pub fn query_text(&self) -> &str {
        &self.query_text
    }

    /// Re-plan the query if the schema has changed.
    fn ensure_plan_fresh(&self) -> Result<()> {
        let current_version = self.db.schema.schema().schema_version;

        // Fast path: read lock only
        {
            let inner = self.inner.read().unwrap();
            if inner.schema_version == current_version {
                return Ok(());
            }
        }

        // Slow path: write lock with double-check
        let mut inner = self.inner.write().unwrap();
        if inner.schema_version == current_version {
            return Ok(());
        }

        let planner = uni_query::QueryPlanner::new(self.db.schema.schema().clone());
        inner.plan = planner
            .plan(inner.ast.clone())
            .map_err(|e| UniError::Query {
                message: e.to_string(),
                query: Some(self.query_text.clone()),
            })?;
        inner.schema_version = current_version;
        Ok(())
    }
}

/// Fluent parameter builder for executing a [`PreparedQuery`].
pub struct PreparedQueryBinder<'a> {
    prepared: &'a PreparedQuery,
    params: HashMap<String, Value>,
}

impl<'a> PreparedQueryBinder<'a> {
    /// Bind a named parameter.
    pub fn param<K: Into<String>, V: Into<Value>>(mut self, key: K, value: V) -> Self {
        self.params.insert(key.into(), value.into());
        self
    }

    /// Execute with the bound parameters.
    pub async fn execute(self) -> Result<QueryResult> {
        self.prepared.execute_with_params(self.params).await
    }
}

/// Parse Cypher into an AST, mapping the error into a [`UniError::Parse`].
fn parse_cypher(cypher: &str) -> Result<uni_query::CypherQuery> {
    uni_cypher::parse(cypher).map_err(|e| UniError::Parse {
        message: e.to_string(),
        position: None,
        line: None,
        column: None,
        context: Some(cypher.to_string()),
    })
}

// ── PreparedLocy ──────────────────────────────────────────────────

/// Interior state for schema-staleness detection.
struct PreparedLocyInner {
    compiled: uni_locy::CompiledProgram,
    schema_version: u32,
}

/// A prepared Locy program with a cached compiled program.
///
/// Created via [`Session::prepare_locy()`](crate::api::session::Session::prepare_locy).
/// The compiled program is cached and reused across executions. If the database
/// schema changes, the program is automatically recompiled.
///
/// All methods take `&self`, so a `PreparedLocy` can be shared across
/// threads via `Arc<PreparedLocy>`.
pub struct PreparedLocy {
    db: Arc<UniInner>,
    rule_registry: Arc<std::sync::RwLock<LocyRuleRegistry>>,
    program_text: String,
    inner: std::sync::RwLock<PreparedLocyInner>,
}

impl PreparedLocy {
    /// Create a new prepared Locy program.
    pub(crate) fn new(
        db: Arc<UniInner>,
        rule_registry: Arc<std::sync::RwLock<LocyRuleRegistry>>,
        program: &str,
    ) -> Result<Self> {
        let compiled = compile_locy_with_registry(program, &rule_registry)?;
        let schema_version = db.schema.schema().schema_version;

        Ok(Self {
            db,
            rule_registry,
            program_text: program.to_string(),
            inner: std::sync::RwLock::new(PreparedLocyInner {
                compiled,
                schema_version,
            }),
        })
    }

    /// Execute the prepared Locy program with the given parameters.
    ///
    /// Uses the cached compiled program. If the schema has changed since
    /// preparation, the program is automatically recompiled before execution.
    pub async fn execute(&self, params: &[(&str, Value)]) -> Result<LocyResult> {
        let param_map: HashMap<String, Value> = params
            .iter()
            .map(|(k, v)| (k.to_string(), v.clone()))
            .collect();
        self.execute_internal(param_map).await
    }

    /// Fluent parameter builder for executing a prepared Locy program.
    pub fn bind(&self) -> PreparedLocyBinder<'_> {
        PreparedLocyBinder {
            prepared: self,
            params: HashMap::new(),
        }
    }

    /// The original program text.
    pub fn program_text(&self) -> &str {
        &self.program_text
    }

    /// Internal execution with a parameter map.
    async fn execute_internal(&self, params: HashMap<String, Value>) -> Result<LocyResult> {
        self.ensure_compiled_fresh()?;

        // Clone the compiled program and merge rules
        let mut compiled = {
            let inner = self.inner.read().unwrap();
            inner.compiled.clone()
        };

        // Merge registered rules (same logic as evaluate_with_config)
        merge_registered_rules(&mut compiled, &self.rule_registry.read().unwrap());

        // Build config with params and session-level semantics
        let config = LocyConfig {
            params,
            ..LocyConfig::default()
        };

        let engine = crate::api::impl_locy::LocyEngine {
            counters: std::sync::Arc::new(uni_store::QueryCounters::new()),
            db: &self.db,
            tx_l0_override: None,
            locy_l0: None,
            collect_derive: true,
            read_snapshot: None,
            // `PreparedLocy` is owned by the instance, not a session, so it
            // has no enclosing scope to inherit; `PreparedLocyBinder` is where
            // a per-execution token would attach.
            cancel: crate::api::impl_query::CancelScope::default(),
        };
        engine
            .evaluate_compiled_with_config(compiled, &config)
            .await
    }

    /// Re-compile if the schema has changed.
    fn ensure_compiled_fresh(&self) -> Result<()> {
        let current_version = self.db.schema.schema().schema_version;

        // Fast path: read lock only
        {
            let inner = self.inner.read().unwrap();
            if inner.schema_version == current_version {
                return Ok(());
            }
        }

        // Slow path: write lock with double-check
        let mut inner = self.inner.write().unwrap();
        if inner.schema_version == current_version {
            return Ok(());
        }

        inner.compiled = compile_locy_with_registry(&self.program_text, &self.rule_registry)?;
        inner.schema_version = current_version;
        Ok(())
    }
}

/// Parse and compile a Locy program using the given rule registry.
///
/// Shared between `PreparedLocy::new()` and `PreparedLocy::ensure_compiled_fresh()`.
fn compile_locy_with_registry(
    program: &str,
    rule_registry: &std::sync::RwLock<LocyRuleRegistry>,
) -> Result<uni_locy::CompiledProgram> {
    let ast = uni_cypher::parse_locy(program).map_err(|e| UniError::Parse {
        message: format!("LocyParseError: {e}"),
        position: None,
        line: None,
        column: None,
        context: None,
    })?;

    let registry = rule_registry.read().unwrap();
    if registry.rules.is_empty() {
        drop(registry);
        uni_locy::compile(&ast).map_err(|e| UniError::Query {
            message: format!("LocyCompileError: {e}"),
            query: None,
        })
    } else {
        let external_names: Vec<String> = registry.rules.keys().cloned().collect();
        drop(registry);
        uni_locy::compile_with_external_rules(&ast, &external_names).map_err(|e| UniError::Query {
            message: format!("LocyCompileError: {e}"),
            query: None,
        })
    }
}

/// Fluent parameter builder for executing a [`PreparedLocy`].
pub struct PreparedLocyBinder<'a> {
    prepared: &'a PreparedLocy,
    params: HashMap<String, Value>,
}

impl<'a> PreparedLocyBinder<'a> {
    /// Bind a named parameter.
    pub fn param<K: Into<String>, V: Into<Value>>(mut self, key: K, value: V) -> Self {
        self.params.insert(key.into(), value.into());
        self
    }

    /// Execute with the bound parameters.
    pub async fn execute(self) -> Result<LocyResult> {
        self.prepared.execute_internal(self.params).await
    }
}
