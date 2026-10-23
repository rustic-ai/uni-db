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
use crate::api::impl_locy::{LocyRuleRegistry, map_compile_error, merge_registered_rules};
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
    /// Every parameter the query references, checked on each execution.
    parameters: Vec<String>,
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
        is_write_procedure: &(dyn Fn(&str) -> bool + Sync),
    ) -> Result<Self> {
        let (ast, parameters) = parse_cypher_with_parameters(cypher)?;
        // Session-prepared queries must be read-only, mirroring `Session::query`
        // — including its procedure classification, so a registered write
        // procedure is refused here as it is there.
        uni_query::validate_read_only_with(&ast, is_write_procedure).map_err(|_| {
            UniError::Query {
                message: "Prepared session query is read-only. Mutation clauses (CREATE, MERGE, \
                 DELETE, SET, REMOVE) and mutating procedures require a transaction. Use \
                 session.tx().prepare()."
                    .to_string(),
                query: Some(cypher.to_string()),
            }
        })?;
        Self::build(db, cypher, (ast, parameters), None, guards, cancel)
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
        let parsed = parse_cypher_with_parameters(cypher)?;
        Self::build(db, cypher, parsed, Some(binding), guards, cancel)
    }

    /// Plan `ast` and assemble the prepared query.
    fn build(
        db: Arc<UniInner>,
        cypher: &str,
        (ast, parameters): (uni_query::CypherQuery, Vec<String>),
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
            parameters,
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
        crate::api::impl_query::check_parameters(&self.parameters, &params)?;
        // Execute under the preparing caller's principal, as the live paths
        // do, so a procedure the statement calls sees it at its capability gate.
        uni_query::maybe_scope_with_principal(
            self.guards.principal.clone(),
            self.execute_unguarded(params),
        )
        .await
    }

    /// Execute after the guards have passed: through the transaction's L0 when
    /// bound to one, else the cached read-only plan.
    async fn execute_unguarded(&self, params: HashMap<String, Value>) -> Result<QueryResult> {
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
                    None,
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

/// Parse Cypher into an AST and the parameters it references, mapping the
/// error into a [`UniError::Parse`].
fn parse_cypher_with_parameters(cypher: &str) -> Result<(uni_query::CypherQuery, Vec<String>)> {
    uni_cypher::parse_with_parameters(cypher).map_err(|e| UniError::Parse {
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
    /// Every parameter the program's text references, in order of first
    /// appearance.
    parameters: Vec<String>,
    schema_version: u32,
}

/// A prepared Locy program with a cached compiled program.
///
/// Created via [`Session::prepare_locy()`](crate::api::session::Session::prepare_locy)
/// or [`Transaction::prepare_locy()`](crate::api::transaction::Transaction::prepare_locy).
/// The compiled program is cached and reused across executions. If the database
/// schema changes, the program is automatically recompiled.
///
/// A session-prepared program runs as `session.locy` does: it reads the
/// shared graph and returns its writes as a `DerivedFactSet`. A
/// transaction-prepared program runs as `tx.locy` does: it reads the
/// transaction's uncommitted writes and pinned snapshot, its writes land in
/// the transaction, and it runs under the transaction's cancellation scope.
///
/// All methods take `&self`, so a `PreparedLocy` can be shared across
/// threads via `Arc<PreparedLocy>`.
pub struct PreparedLocy {
    db: Arc<UniInner>,
    rule_registry: Arc<std::sync::RwLock<LocyRuleRegistry>>,
    program_text: String,
    /// `Some` for a transaction-prepared program: the transaction's private
    /// L0 and live snapshot slot.
    tx: Option<PreparedTxBinding>,
    /// The scope every execution runs under: the transaction's for a
    /// transaction-prepared program.
    cancel: crate::api::impl_query::CancelScope,
    /// The principal of the session or transaction that prepared the
    /// program; every execution is authorized for it.
    principal: Option<Arc<uni_plugin::traits::connector::Principal>>,
    inner: std::sync::RwLock<PreparedLocyInner>,
}

impl PreparedLocy {
    /// Create a new prepared Locy program.
    pub(crate) fn new(
        db: Arc<UniInner>,
        rule_registry: Arc<std::sync::RwLock<LocyRuleRegistry>>,
        program: &str,
        principal: Option<Arc<uni_plugin::traits::connector::Principal>>,
    ) -> Result<Self> {
        Self::build(
            db,
            rule_registry,
            program,
            None,
            // A session-prepared program is owned by the instance, not a
            // session, so it has no enclosing scope to inherit.
            crate::api::impl_query::CancelScope::default(),
            principal,
        )
    }

    /// Create a Locy program prepared in a transaction: every execution
    /// reads and writes through `binding` and runs under `cancel`.
    pub(crate) fn new_tx_bound(
        db: Arc<UniInner>,
        rule_registry: Arc<std::sync::RwLock<LocyRuleRegistry>>,
        program: &str,
        binding: PreparedTxBinding,
        cancel: crate::api::impl_query::CancelScope,
        principal: Option<Arc<uni_plugin::traits::connector::Principal>>,
    ) -> Result<Self> {
        Self::build(db, rule_registry, program, Some(binding), cancel, principal)
    }

    fn build(
        db: Arc<UniInner>,
        rule_registry: Arc<std::sync::RwLock<LocyRuleRegistry>>,
        program: &str,
        tx: Option<PreparedTxBinding>,
        cancel: crate::api::impl_query::CancelScope,
        principal: Option<Arc<uni_plugin::traits::connector::Principal>>,
    ) -> Result<Self> {
        let (compiled, parameters) = compile_locy_with_registry(program, &rule_registry)?;
        let schema_version = db.schema.schema().schema_version;

        Ok(Self {
            db,
            rule_registry,
            program_text: program.to_string(),
            tx,
            cancel,
            principal,
            inner: std::sync::RwLock::new(PreparedLocyInner {
                compiled,
                parameters,
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
        let (mut compiled, parameters) = {
            let inner = self.inner.read().unwrap();
            (inner.compiled.clone(), inner.parameters.clone())
        };

        // Merge registered rules (same logic as evaluate_with_config)
        merge_registered_rules(&mut compiled, &self.rule_registry.read().unwrap())
            .map_err(map_compile_error)?;
        // Before evaluation, every missing name at once, the merged rules'
        // included (as `evaluate_with_config` does).
        crate::api::impl_locy::check_program_parameters(&parameters, &compiled, &params)?;

        // Build config with params and session-level semantics
        let config = LocyConfig {
            params,
            ..LocyConfig::default()
        };

        // A transaction-prepared program is `tx.locy`: reads and writes go
        // through the transaction's L0, under its current snapshot (read
        // live, as `PreparedQuery` does: it is pinned at the transaction's
        // first read and released at commit or rollback).
        let (tx_l0, read_snapshot) = match &self.tx {
            Some(tx) => (Some(tx.tx_l0.clone()), tx.snapshot.lock().clone()),
            None => (None, None),
        };
        let engine = crate::api::impl_locy::LocyEngine {
            counters: std::sync::Arc::new(uni_store::QueryCounters::new()),
            db: &self.db,
            tx_l0_override: tx_l0.clone(),
            locy_l0: tx_l0,
            collect_derive: self.tx.is_none(),
            read_snapshot,
            cancel: self.cancel.clone(),
            // No per-call override on this path; the database
            // setting applies.
            max_memory: None,
            principal: self.principal.clone(),
        };
        // The task-local scope `LocyBuilder::run` / `Transaction::locy`
        // install: the principal for procedure capability gates, the instance
        // registry for in-memory Locy plugin dispatch.
        uni_query::scoped_with_session_context(
            Arc::clone(&self.db.plugin_registry),
            self.principal.clone(),
            engine.evaluate_compiled_with_config(compiled, &self.program_text, &config),
        )
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

        (inner.compiled, inner.parameters) =
            compile_locy_with_registry(&self.program_text, &self.rule_registry)?;
        inner.schema_version = current_version;
        Ok(())
    }
}

/// Parse and compile a Locy program using the given rule registry, also
/// returning every parameter its text references.
///
/// Shared between `PreparedLocy::new()` and `PreparedLocy::ensure_compiled_fresh()`.
fn compile_locy_with_registry(
    program: &str,
    rule_registry: &std::sync::RwLock<LocyRuleRegistry>,
) -> Result<(uni_locy::CompiledProgram, Vec<String>)> {
    let (ast, parameters) =
        uni_cypher::parse_locy_with_parameters(program).map_err(|e| UniError::Parse {
            message: format!("LocyParseError: {e}"),
            position: None,
            line: None,
            column: None,
            context: None,
        })?;

    let registry = rule_registry.read().unwrap();
    let compiled = if registry.rules.is_empty() {
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
    }?;
    Ok((compiled, parameters))
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
