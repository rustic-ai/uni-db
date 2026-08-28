// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

use futures::StreamExt;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;
use uni_common::{Result, UniConfig, UniError};
use uni_query::{
    ExplainOutput, LogicalPlan, ProfileOutput, QueryCursor, QueryMetrics, QueryResult,
    ResultNormalizer, Row, Value as ApiValue,
};

/// Normalize backend/planner error text into canonical Cypher/TCK codes.
///
/// This keeps behavioral semantics unchanged while making error classification
/// stable across planner backends.
fn normalize_error_message(raw: &str, cypher: &str) -> String {
    let mut normalized = raw.to_string();
    let cypher_upper = cypher.to_uppercase();
    let cypher_lower = cypher.to_lowercase();

    if raw.contains("Error during planning: UDF") && raw.contains("is not registered") {
        normalized = format!("SyntaxError: UnknownFunction - {}", raw);
    } else if raw.contains("_cypher_in(): second argument must be a list") {
        normalized = format!("TypeError: InvalidArgumentType - {}", raw);
    } else if raw.contains("InvalidNumberOfArguments: Procedure") && raw.contains("got 0") {
        if cypher_upper.contains("YIELD") {
            normalized = format!("SyntaxError: InvalidArgumentPassingMode - {}", raw);
        } else {
            normalized = format!("ParameterMissing: MissingParameter - {}", raw);
        }
    } else if raw.contains("Function count not implemented or is aggregate")
        || raw.contains("Physical plan does not support logical expression AggregateFunction")
        || raw.contains("Expected aggregate function, got: ListComprehension")
    {
        normalized = format!("SyntaxError: InvalidAggregation - {}", raw);
    } else if raw.contains("Expected aggregate function, got: BinaryOp") {
        normalized = format!("SyntaxError: AmbiguousAggregationExpression - {}", raw);
    } else if raw.contains("Schema error: No field named \"me.age\". Valid fields are \"count(you.age)\".")
    {
        normalized = format!("SyntaxError: UndefinedVariable - {}", raw);
    } else if raw.contains(
        "Schema error: No field named \"me.age\". Valid fields are \"me.age + you.age\", \"count(*)\".",
    ) {
        normalized = format!("SyntaxError: AmbiguousAggregationExpression - {}", raw);
    } else if raw.contains("MERGE edge must have a type")
        || raw.contains("MERGE does not support multiple edge types")
    {
        normalized = format!("SyntaxError: NoSingleRelationshipType - {}", raw);
    } else if raw.contains("MERGE node must have a label") {
        if cypher.contains("$param") {
            normalized = format!("SyntaxError: InvalidParameterUse - {}", raw);
        } else if cypher.contains('*') && cypher.contains("-[:") {
            normalized = format!("SyntaxError: CreatingVarLength - {}", raw);
        } else if cypher_lower.contains("on create set x.")
            || cypher_lower.contains("on match set x.")
        {
            normalized = format!("SyntaxError: UndefinedVariable - {}", raw);
        }
    }

    normalized
}

/// Convert a parse error into `UniError::Parse`.
pub(crate) fn into_parse_error(e: impl std::fmt::Display) -> UniError {
    UniError::Parse {
        message: e.to_string(),
        position: None,
        line: None,
        column: None,
        context: None,
    }
}

/// Convert a planner/compile-time error into the appropriate `UniError` type.
///
/// Errors starting with "SyntaxError:" are treated as parse/syntax errors.
/// All other errors are query/semantic errors (CompileTime).
pub(crate) fn into_query_error(e: impl std::fmt::Display, cypher: &str) -> UniError {
    let msg = normalize_error_message(&e.to_string(), cypher);
    // Errors containing "SyntaxError:" prefix should be treated as syntax errors
    // This covers validation errors like VariableTypeConflict, UndefinedVariable, etc.
    if msg.starts_with("SyntaxError:") {
        UniError::Parse {
            message: msg,
            position: None,
            line: None,
            column: None,
            context: Some(cypher.to_string()),
        }
    } else {
        UniError::Query {
            message: msg,
            query: Some(cypher.to_string()),
        }
    }
}

/// Convert an executor/runtime error into the appropriate `UniError` type.
/// TypeError messages from UDF execution become `UniError::Type` (Runtime phase).
/// ConstraintVerificationFailed messages become `UniError::Constraint` (Runtime phase).
/// All other executor errors remain `UniError::Query`.
///
/// `timeout` is the effective `query_timeout` for the statement. The lower
/// layers signal an elapsed deadline with a bare `anyhow!("Query timed out")`
/// and cannot know the budget they blew, so it is reattached here.
fn into_execution_error(
    e: impl std::fmt::Display,
    cypher: &str,
    timeout: std::time::Duration,
) -> UniError {
    let msg = normalize_error_message(&e.to_string(), cypher);
    if let Some(detail) = uni_common::GraphComputeIncomplete::from_tagged_message(&msg) {
        UniError::GraphComputeIncomplete {
            detail: Box::new(detail),
        }
    } else if msg.contains("Query cancelled") {
        UniError::Cancelled
    } else if msg.contains("Query timed out") {
        query_timed_out_error(timeout)
    } else if msg.contains("Query exceeded memory limit") {
        UniError::Query {
            message: msg,
            query: Some(cypher.to_string()),
        }
    } else if msg.contains("TypeError:") {
        UniError::Type {
            expected: msg,
            actual: String::new(),
        }
    } else if msg.starts_with("ConstraintVerificationFailed:") {
        UniError::Constraint { message: msg }
    } else {
        UniError::Query {
            message: msg,
            query: Some(cypher.to_string()),
        }
    }
}

/// Map a streaming-execution error into the appropriate `UniError`.
///
/// This is the [`into_execution_error`] classification minus its cancellation
/// and timeout arms. Those are still not reachable *here* — the cursor's own
/// guard in [`UniInner::build_guarded_cursor`] raises them around the stream
/// rather than letting the executor surface them through this function — so
/// there is nothing for those two arms to match.
fn into_stream_error(e: impl std::fmt::Display, cypher: &str) -> UniError {
    let msg = normalize_error_message(&e.to_string(), cypher);
    if let Some(detail) = uni_common::GraphComputeIncomplete::from_tagged_message(&msg) {
        UniError::GraphComputeIncomplete {
            detail: Box::new(detail),
        }
    } else if msg.contains("TypeError:") {
        UniError::Type {
            expected: msg,
            actual: String::new(),
        }
    } else if msg.starts_with("ConstraintVerificationFailed:") {
        UniError::Constraint { message: msg }
    } else {
        UniError::Query {
            message: msg,
            query: Some(cypher.to_string()),
        }
    }
}

/// Peel a `VERSION AS OF` / `TIMESTAMP AS OF` wrapper off a parsed query.
///
/// Resolving the spec is the API layer's responsibility: both
/// `QueryPlanner::plan_with_scope` and the expression rewriter declare a
/// `Query::TimeTravel` that reaches them `unreachable!`, since neither can
/// open a snapshot. Every terminal that parses Cypher must therefore split
/// first and re-dispatch against a pinned instance.
///
/// This exists as a named function rather than an inline `match` because it
/// was inlined at two call sites and simply absent from three others, and the
/// three that omitted it panicked.
fn split_time_travel(
    ast: uni_cypher::ast::Query,
) -> (uni_cypher::ast::Query, Option<uni_query::TimeTravelSpec>) {
    match ast {
        uni_cypher::ast::Query::TimeTravel { query, spec } => (*query, Some(spec)),
        other => (other, None),
    }
}

/// Determine the output column order for a result set.
///
/// Uses the planner's projection order when available; otherwise falls back to
/// the first row's keys, sorted for deterministic output. An empty result set
/// yields no columns.
fn columns_for_results(
    results: &[HashMap<String, ApiValue>],
    projection_order: Option<Vec<String>>,
) -> Arc<Vec<String>> {
    if results.is_empty() {
        Arc::new(vec![])
    } else if let Some(order) = projection_order {
        Arc::new(order)
    } else {
        let mut cols: Vec<String> = results[0].keys().cloned().collect();
        cols.sort();
        Arc::new(cols)
    }
}

/// Project per-column-name result maps into ordered [`Row`]s.
///
/// When `normalize` is set, each value passes through
/// [`ResultNormalizer::normalize_value`] so Node/Edge/Path types are produced;
/// the streaming cursor paths skip normalization (it is applied downstream).
/// Missing columns become `Null`.
fn rows_for_results(
    results: Vec<HashMap<String, ApiValue>>,
    columns: &Arc<Vec<String>>,
    normalize: bool,
) -> Vec<Row> {
    results
        .into_iter()
        .map(|map| {
            let mut values = Vec::with_capacity(columns.len());
            for col in columns.iter() {
                let value = map.get(col).cloned().unwrap_or(ApiValue::Null);
                let value = if normalize {
                    ResultNormalizer::normalize_value(value).unwrap_or(ApiValue::Null)
                } else {
                    value
                };
                values.push(value);
            }
            Row::new(columns.clone(), values)
        })
        .collect()
}

/// Coarse in-memory size estimate for a result batch: a per-value size plus a
/// fixed 64-byte overhead.
///
/// Shared by the materializing and cursor paths so the two cannot drift — the
/// cursor previously had no accounting at all, and a second copy of this
/// formula would have let the same query be accepted by one path and rejected
/// by the other.
fn estimate_result_bytes(results: &[HashMap<String, ApiValue>]) -> usize {
    results
        .iter()
        .map(|row| {
            row.values()
                .map(|v| std::mem::size_of_val(v) + 64)
                .sum::<usize>()
        })
        .sum()
}

/// The error both paths raise when `max_query_memory` is exceeded.
fn memory_limit_error(estimated_bytes: usize, max_mem: usize, cypher: &str) -> UniError {
    UniError::Query {
        message: format!(
            "Query exceeded memory limit ({estimated_bytes} bytes > {max_mem} byte limit)"
        ),
        query: Some(cypher.to_string()),
    }
}

/// The error every path raises when `query_timeout` elapses.
///
/// `UniError::Timeout`, not a `Query` carrying the words "Query timed out":
/// an elapsed deadline is exactly what the typed variant exists for, it carries
/// the budget that was exceeded, and the bindings map it to a dedicated
/// `UniTimeoutError`. The transaction builder's own timeout wrapper has always
/// produced this variant; the session surface used to disagree, so the same
/// condition surfaced as two different classes depending on which terminal the
/// caller reached for.
fn query_timed_out_error(timeout: std::time::Duration) -> UniError {
    UniError::Timeout {
        timeout_ms: timeout.as_millis() as u64,
    }
}

/// The error the cursor path raises when its cancellation token fires.
///
/// `UniError::Cancelled`, not a generic `Query` — `into_execution_error`
/// already classifies a cancelled execution that way, and the bindings map it
/// to a dedicated `UniCancelledError`. Rendering it as `Query` would collapse
/// it into the same class as every other query failure, which is the exact
/// defect fixed on the async cursor in the preceding commit.
fn query_cancelled_error() -> UniError {
    UniError::Cancelled
}

/// The cancellation sources a single statement executes under.
///
/// uni already scopes cancellation hierarchically — a forked session's token is
/// a child of its parent's, and a `Transaction`'s is a child of its session's —
/// so cancelling an outer scope is meant to abort everything running inside it.
/// A caller may additionally attach a token to one statement through
/// `.cancellation_token()`.
///
/// Those two are independent `CancellationToken`s, and `tokio_util` has no
/// combinator for "whichever fires first". Merging them would take a spawned
/// linker task plus a drop guard on every statement, so instead the places that
/// actually enforce cancellation await both directly.
///
/// **The guard is authoritative, the executor's token is an optimisation.**
/// `Executor::set_cancellation_token` accepts exactly one token and only feeds
/// `GraphContext::check_timeout`, which fires solely where an operator happens
/// to call it — no scan/join/traverse plan reaches one. It is worth setting for
/// the long-running operators that *do* check (traverse, shortest-path,
/// vector-KNN, procedure calls), but correctness comes from
/// [`Self::cancelled`], which every terminal races its execution against.
#[derive(Clone, Default)]
pub(crate) struct CancelScope {
    /// The enclosing session or transaction scope.
    scope: Option<tokio_util::sync::CancellationToken>,
    /// A token attached to this one statement by the caller.
    caller: Option<tokio_util::sync::CancellationToken>,
}

impl CancelScope {
    pub(crate) fn new(
        scope: Option<tokio_util::sync::CancellationToken>,
        caller: Option<tokio_util::sync::CancellationToken>,
    ) -> Self {
        Self { scope, caller }
    }

    /// Resolve as soon as any source is cancelled; pend forever if there are
    /// none, so callers can `select!` on it unconditionally.
    pub(crate) async fn cancelled(&self) {
        match (&self.scope, &self.caller) {
            (Some(scope), Some(caller)) => {
                tokio::select! {
                    _ = scope.cancelled() => {}
                    _ = caller.cancelled() => {}
                }
            }
            (Some(token), None) | (None, Some(token)) => token.cancelled().await,
            (None, None) => std::future::pending().await,
        }
    }

    /// Best-effort token for the executor's in-operator checks.
    ///
    /// Prefers the caller's, which is scoped to this statement; the enclosing
    /// scope's token outlives it and would leave the executor holding a token
    /// for work it no longer runs. Only one can be installed — see the type
    /// docs for why that is an optimisation rather than the enforcement point.
    fn executor_token(&self) -> Option<tokio_util::sync::CancellationToken> {
        self.caller.clone().or_else(|| self.scope.clone())
    }

    /// Install the best-effort token on an executor.
    pub(crate) fn install(&self, executor: &mut uni_query::Executor) {
        if let Some(token) = self.executor_token() {
            executor.set_cancellation_token(token);
        }
    }
}

/// Reject a result set whose estimated in-memory size exceeds `max_mem` bytes.
///
/// A `max_mem` of `0` disables the limit.
fn enforce_memory_limit(
    results: &[HashMap<String, ApiValue>],
    max_mem: usize,
    cypher: &str,
) -> Result<()> {
    if max_mem == 0 {
        return Ok(());
    }
    let estimated_bytes = estimate_result_bytes(results);
    if estimated_bytes > max_mem {
        return Err(memory_limit_error(estimated_bytes, max_mem, cypher));
    }
    Ok(())
}

/// Extract projection column names from a LogicalPlan, preserving query order.
/// Returns None if the plan doesn't have projections at the top level.
fn extract_projection_order(plan: &LogicalPlan) -> Option<Vec<String>> {
    match plan {
        LogicalPlan::Project { projections, .. } => Some(
            projections
                .iter()
                .map(|(expr, alias)| alias.clone().unwrap_or_else(|| expr.to_string_repr()))
                .collect(),
        ),
        LogicalPlan::Aggregate {
            group_by,
            aggregates,
            ..
        } => {
            let mut names: Vec<String> = group_by.iter().map(|e| e.to_string_repr()).collect();
            names.extend(aggregates.iter().map(|e| e.to_string_repr()));
            Some(names)
        }
        LogicalPlan::Limit { input, .. }
        | LogicalPlan::Sort { input, .. }
        | LogicalPlan::Filter { input, .. } => extract_projection_order(input),
        _ => None,
    }
}

impl crate::api::UniInner {
    /// Get the current L0Buffer mutation count (cumulative mutations since last flush).
    /// Used to compute affected_rows for mutation queries that return no result rows.
    pub(crate) async fn get_mutation_count(&self) -> usize {
        match self.writer.as_ref() {
            Some(writer) => writer.l0_manager.get_current().read().mutation_count,
            None => 0,
        }
    }

    /// Install this database's session-constant state onto a fresh executor.
    ///
    /// Sets the Xervo runtime, procedure registry, the custom-function registry
    /// (only when non-empty, to avoid a needless clone), and the writer handle.
    /// The per-query `config` and transaction overrides are applied separately
    /// by callers.
    fn apply_session_executor_state(&self, executor: &mut uni_query::Executor) {
        executor.set_xervo_runtime(self.xervo_runtime.clone());
        executor.set_procedure_registry(self.procedure_registry.clone());
        if let Ok(reg) = self.custom_functions.read()
            && !reg.is_empty()
        {
            executor.set_custom_functions(Arc::new(reg.clone()));
        }
        if let Some(w) = &self.writer {
            executor.set_writer(w.clone());
        }
        // A read-only open has no writer but does carry the WAL-replayed L0
        // (`UniInner::l0_manager`); without this the executor's `get_context`
        // returns `None` and every read on the session is L0-blind.
        if let Some(m) = &self.l0_manager {
            executor.set_l0_manager(Arc::clone(m));
        }
    }

    /// Build a planner carrying this session's schema and plugin registry.
    ///
    /// The plugin registry is not optional: without it the plugin-catalog
    /// features (virtual-label resolution, replacement scans, virtual-label
    /// write rejection) all silently no-op, so a query would drop catalog rows
    /// the default path returns.
    fn base_planner(&self) -> uni_query::QueryPlanner {
        uni_query::QueryPlanner::new(self.schema.schema().clone())
            .with_plugin_registry(Arc::clone(&self.plugin_registry))
    }

    /// Plan `ast`, then apply the two rewrites every call site must run.
    ///
    /// `rewrite_for_fork_fusion` is load-bearing: per CLAUDE.md's Phase-5a
    /// invariant, any planner call site that skips it makes fork-local indexes
    /// silently stop fusing. `fuse_create_set` follows it. Centralised here so
    /// the pair cannot drift apart across the call sites in this file.
    ///
    /// Rewrites a plan is not *valid* without do not belong here — this helper
    /// is one of eight places a plan is built from Cypher, and the other seven
    /// would silently skip them. Those live at the end of
    /// `QueryPlanner::plan_with_scope`, which every one of the eight goes
    /// through.
    fn plan_and_rewrite(
        &self,
        planner: &uni_query::QueryPlanner,
        ast: uni_cypher::ast::Query,
        cypher: &str,
    ) -> Result<LogicalPlan> {
        let logical_plan = planner.plan(ast).map_err(|e| into_query_error(e, cypher))?;
        let logical_plan = uni_query::rewrite_for_fork_fusion(logical_plan, &*self.storage);
        Ok(uni_query::fuse_create_set(logical_plan))
    }

    /// Resolve a time-travel spec to an instance pinned at that snapshot.
    ///
    /// Every terminal that accepts `VERSION AS OF` / `TIMESTAMP AS OF` has to
    /// do this *before* planning: both the planner and the expression rewriter
    /// treat a `Query::TimeTravel` that reaches them as `unreachable!`, because
    /// resolving a snapshot is the API layer's job and neither layer can do it.
    async fn pinned_at(&self, spec: &uni_query::TimeTravelSpec) -> Result<crate::api::UniInner> {
        let snapshot_id = self.resolve_time_travel(spec).await?;
        self.at_snapshot(&snapshot_id).await
    }

    /// Explain a Cypher query plan without executing it.
    pub(crate) async fn explain_internal(&self, cypher: &str) -> Result<ExplainOutput> {
        let ast = uni_cypher::parse(cypher).map_err(into_parse_error)?;
        let (ast, tt_spec) = split_time_travel(ast);

        if let Some(spec) = tt_spec {
            // Plan against the pinned instance, not the live one. Stripping the
            // spec and explaining the current version would answer a different
            // question than the caller asked -- quietly, which is worse than
            // the panic this replaces.
            //
            // Read-only validation is deliberately *not* applied, matching the
            // non-time-travel explain path: EXPLAIN never executes, so
            // describing a write plan is legitimate introspection.
            return Box::pin(
                self.pinned_at(&spec)
                    .await?
                    .explain_ast_internal(ast, cypher),
            )
            .await;
        }

        self.explain_ast_internal(ast, cypher).await
    }

    async fn explain_ast_internal(
        &self,
        ast: uni_cypher::ast::Query,
        cypher: &str,
    ) -> Result<ExplainOutput> {
        let planner = self.base_planner();
        // Phase 5a-impl: `plan_and_rewrite` applies the fork-aware fusion
        // rewrite so explain output reflects the operators the executor will
        // actually pick on a forked session.
        let logical_plan = self.plan_and_rewrite(&planner, ast, cypher)?;
        planner
            .explain_logical_plan(&logical_plan)
            .map_err(|e| into_query_error(e, cypher))
    }

    /// Profile a Cypher query execution.
    pub(crate) async fn profile_internal(
        &self,
        cypher: &str,
        params: HashMap<String, ApiValue>,
    ) -> Result<(QueryResult, ProfileOutput)> {
        let ast = uni_cypher::parse(cypher).map_err(into_parse_error)?;
        let (ast, tt_spec) = split_time_travel(ast);

        if let Some(spec) = tt_spec {
            // `profile` executes, so pinning is mandatory rather than cosmetic:
            // profiling the live version would report timings for rows the
            // caller did not ask about.
            uni_query::validate_read_only(&ast).map_err(|msg| into_query_error(msg, cypher))?;
            return Box::pin(
                self.pinned_at(&spec)
                    .await?
                    .profile_ast_internal(ast, cypher, params),
            )
            .await;
        }

        self.profile_ast_internal(ast, cypher, params).await
    }

    async fn profile_ast_internal(
        &self,
        ast: uni_cypher::ast::Query,
        cypher: &str,
        params: HashMap<String, ApiValue>,
    ) -> Result<(QueryResult, ProfileOutput)> {
        let planner = self.base_planner();
        let logical_plan = self.plan_and_rewrite(&planner, ast, cypher)?;

        let mut executor = uni_query::Executor::new(self.storage.clone());
        executor.set_config(self.config.clone());
        self.apply_session_executor_state(&mut executor);

        let projection_order = extract_projection_order(&logical_plan);

        let (results, profile_output) = executor
            .profile(logical_plan, &params)
            .await
            .map_err(|e| into_execution_error(e, cypher, self.config.query_timeout))?;

        let columns = columns_for_results(&results, projection_order);
        let rows = rows_for_results(results, &columns, true);

        // PROFILE used to return `Default::default()` here, i.e. a result whose
        // every metric read zero — the one query form a user runs *specifically*
        // to see metrics.
        let metrics = QueryMetrics {
            rows_returned: rows.len(),
            ..Default::default()
        };
        let mut result = QueryResult::new(columns, rows, Vec::new(), metrics);
        result.set_counters(executor.take_counters());
        Ok((result, profile_output))
    }

    pub(crate) async fn execute_cursor_internal_with_config(
        &self,
        cypher: &str,
        params: HashMap<String, ApiValue>,
        config: UniConfig,
        cancel: CancelScope,
    ) -> Result<QueryCursor> {
        let ast = uni_cypher::parse(cypher).map_err(into_parse_error)?;
        let (ast, tt_spec) = split_time_travel(ast);

        if let Some(spec) = tt_spec {
            // Streaming executes too, so the cursor must be built on the pinned
            // instance -- its storage is what the stream reads from.
            uni_query::validate_read_only(&ast).map_err(|msg| into_query_error(msg, cypher))?;
            return Box::pin(
                self.pinned_at(&spec)
                    .await?
                    .cursor_from_ast(ast, cypher, params, config, cancel),
            )
            .await;
        }

        self.cursor_from_ast(ast, cypher, params, config, cancel)
            .await
    }

    async fn cursor_from_ast(
        &self,
        ast: uni_cypher::ast::Query,
        cypher: &str,
        params: HashMap<String, ApiValue>,
        config: UniConfig,
        cancel: CancelScope,
    ) -> Result<QueryCursor> {
        let planner = self.base_planner().with_params(params.clone());
        let logical_plan = self.plan_and_rewrite(&planner, ast, cypher)?;

        let mut executor = uni_query::Executor::new(self.storage.clone());
        executor.set_config(config.clone());
        self.apply_session_executor_state(&mut executor);
        cancel.install(&mut executor);

        Ok(self.build_guarded_cursor(executor, logical_plan, params, cypher, &config, cancel))
    }

    /// Assemble the streaming cursor shared by the session and transaction
    /// entry points.
    ///
    /// The two callers differ only in how they *prepare* — the session one
    /// takes a per-query config override, the transaction one rejects
    /// time-travel and installs the tx-private L0. From the configured executor
    /// onward the work is identical: batches to rows, re-chunk to `batch_size`,
    /// and guard the stream against the deadline, the memory ceiling and the
    /// cancellation token.
    ///
    /// That tail used to be copy-pasted into both functions, and the copies
    /// drifted: every limit added to the session cursor was absent from the
    /// transaction one, so a transaction cursor streamed unbounded while the
    /// builder happily accepted `.timeout()` and `.cancellation_token()`.
    /// Keeping one body is what stops that recurring.
    fn build_guarded_cursor(
        &self,
        executor: uni_query::Executor,
        logical_plan: LogicalPlan,
        params: HashMap<String, ApiValue>,
        cypher: &str,
        config: &UniConfig,
        cancel: CancelScope,
    ) -> QueryCursor {
        let projection_order = extract_projection_order(&logical_plan);
        let projection_order_for_rows = projection_order.clone();
        let cypher_for_error = cypher.to_string();
        let cypher_for_limits = cypher.to_string();
        let batch_size = config.batch_size;
        let max_mem = config.max_query_memory;

        // Mirrors `execute_plan_internal`, which enforces the deadline twice:
        // a `tokio::time::timeout` around execution (installed below) plus an
        // explicit post-execution comparison. The second one is load-bearing —
        // an in-memory query can finish the whole plan inside a single poll
        // without ever returning `Pending`, so the timer never gets a chance to
        // fire and only the elapsed-time comparison catches the overrun.
        let query_timeout = config.query_timeout;
        let exec_deadline = Instant::now() + query_timeout;

        let stream = executor.execute_stream(logical_plan, self.properties.clone(), params);

        // Running total across batches, mirroring how the materializing path
        // measures the whole result set at once. `execute_stream` currently
        // yields a single batch, so the two agree exactly; accumulating keeps
        // that true if it ever becomes genuinely incremental.
        let mut streamed_bytes: usize = 0;

        // Convert raw hash-map batches to Row batches, chunked by batch_size.
        let row_stream = stream
            .map(move |batch_res| {
                let results = batch_res.map_err(|e| into_stream_error(e, &cypher_for_error))?;
                // Applied to the executor's own output, not to the re-chunked
                // pieces below, so a slow consumer paging an already-computed
                // result is not charged against the query's execution budget.
                if Instant::now() > exec_deadline {
                    return Err(query_timed_out_error(query_timeout));
                }
                if max_mem > 0 {
                    streamed_bytes = streamed_bytes.saturating_add(estimate_result_bytes(&results));
                    if streamed_bytes > max_mem {
                        return Err(memory_limit_error(
                            streamed_bytes,
                            max_mem,
                            &cypher_for_limits,
                        ));
                    }
                }
                if results.is_empty() {
                    return Ok(vec![]);
                }
                let columns = columns_for_results(&results, projection_order_for_rows.clone());
                Ok(rows_for_results(results, &columns, false))
            })
            // Re-chunk into batch_size-sized pieces
            .flat_map(
                move |batch_res: std::result::Result<Vec<Row>, UniError>| match batch_res {
                    Ok(rows) if batch_size > 0 => {
                        let chunks: Vec<_> =
                            rows.chunks(batch_size).map(|c| Ok(c.to_vec())).collect();
                        futures::stream::iter(chunks).boxed()
                    }
                    other => futures::stream::iter(vec![other]).boxed(),
                },
            );

        // Wall-clock ceiling and cooperative cancellation.
        //
        // The materializing path gets both from `execute_plan_internal`, which
        // wraps execution in `tokio::time::timeout` and hands the token to the
        // executor. Neither reached the cursor, so `.timeout()`,
        // `.max_memory()` and `.cancellation_token()` were all accepted by the
        // builder and silently ignored once `.cursor()` was the terminal.
        //
        // `GraphContext::check_timeout` is not sufficient on its own: it fires
        // only where an operator happens to call it, and no scan/join/traverse
        // plan reaches one — which is why the executor token set above needs
        // this outer guard to have any observable effect.
        //
        // The deadline is absolute from cursor creation, matching the
        // materializing path's ceiling on execution. That is exact while
        // `execute_stream` yields a single batch (all work happens in the first
        // poll and later polls resolve immediately from the materialized
        // vector). If it ever streams incrementally, revisit whether a slow
        // consumer should be charged against the query's own budget.
        let deadline = tokio::time::Instant::now() + query_timeout;
        let guarded = futures::stream::unfold(Some(row_stream.boxed()), move |state| {
            let cancel = cancel.clone();
            async move {
                let stream = state?;
                let next = stream.into_future();
                // `biased` so an already-cancelled scope wins deterministically
                // rather than racing the first poll. An empty scope pends
                // forever, so the branch is safe to take unconditionally.
                let outcome = tokio::select! {
                    biased;
                    () = cancel.cancelled() => None,
                    res = tokio::time::timeout_at(deadline, next) => Some(res),
                };
                match outcome {
                    // Emit the failure once, then end the stream — re-polling a
                    // timed-out or cancelled cursor must not yield the same
                    // error forever.
                    None => Some((Err(query_cancelled_error()), None)),
                    Some(Err(_elapsed)) => Some((Err(query_timed_out_error(query_timeout)), None)),
                    Some(Ok((Some(item), rest))) => Some((item, Some(rest))),
                    Some(Ok((None, _exhausted))) => None,
                }
            }
        });

        // We need columns ahead of time for QueryCursor if possible.
        let columns = projection_order.map_or_else(|| Arc::new(vec![]), Arc::new);

        // `.fuse()` is required, not cosmetic: `stream::unfold` panics if polled
        // after it yields `None`, and callers legitimately do that — an empty
        // result set polls once, and `fetch_one()` on an exhausted cursor polls
        // again to confirm exhaustion. The previous `map`/`flat_map` chain
        // tolerated it, so dropping the fuse turns a supported call into a
        // panic that crosses the pyo3 boundary.
        QueryCursor::new(columns, Box::pin(guarded.fuse()))
    }

    pub(crate) async fn execute_internal(
        &self,
        cypher: &str,
        params: HashMap<String, ApiValue>,
    ) -> Result<QueryResult> {
        self.execute_internal_with_config(
            cypher,
            params,
            self.config.clone(),
            CancelScope::default(),
        )
        .await
    }

    /// Execute a Cypher query with a private transaction L0 buffer.
    /// The tx_l0 is installed on the executor so both reads and mutations
    /// are routed through the caller's private L0 (commit-time serialization).
    pub(crate) async fn execute_internal_with_tx_l0(
        &self,
        cypher: &str,
        params: HashMap<String, ApiValue>,
        tx_l0: std::sync::Arc<parking_lot::RwLock<uni_store::runtime::l0::L0Buffer>>,
        id_reservoir: Option<Arc<uni_store::runtime::TxIdReservoir>>,
        read_snapshot: Option<uni_store::runtime::SnapshotView>,
        cancel: CancelScope,
    ) -> Result<QueryResult> {
        let total_start = Instant::now();

        // Write-path plan cache: a hit skips parse + planning for a repeated
        // statement shape (the dominant ingest pattern, e.g. `UNWIND … CREATE`).
        // The cached value is the *pre-rewrite* logical plan — the rewrites and
        // parameter binding still run per execution below, so reuse is
        // independent of both live fork-index state and parameter values.
        let schema_version = self.schema.schema().schema_version;
        let text_key = crate::api::session::plan_cache_key(cypher);
        // The effective key folds in the values of any LIMIT/SKIP parameters the
        // planner baked into the cached plan, so a `LIMIT $n` read inside a tx is
        // never served another value's plan.
        let cached_plan = self.plan_cache.lock().ok().and_then(|mut cache| {
            let key = cache.effective_key(text_key, &params);
            cache
                .get(key, cypher, schema_version)
                .map(|e| e.plan.clone())
        });

        let (logical_plan_raw, parse_time, plan_time, plan_cache_hit) =
            if let Some(plan) = cached_plan {
                (
                    plan,
                    std::time::Duration::ZERO,
                    std::time::Duration::ZERO,
                    true,
                )
            } else {
                let parse_start = Instant::now();
                let ast = uni_cypher::parse(cypher).map_err(into_parse_error)?;
                let parse_time = parse_start.elapsed();

                let (ast, tt_spec) = split_time_travel(ast);
                if tt_spec.is_some() {
                    return Err(UniError::Query {
                        message: "Time-travel queries are not supported within transactions"
                            .to_string(),
                        query: Some(cypher.to_string()),
                    });
                }

                let plan_start = Instant::now();
                let (lp, folded) = {
                    let planner = self.base_planner().with_params(params.clone());
                    let lp = planner.plan(ast).map_err(|e| into_query_error(e, cypher))?;
                    let folded = planner.folded_limit_skip_params();
                    (lp, folded)
                };
                let plan_time = plan_start.elapsed();

                // Cache the pre-rewrite logical plan for subsequent batches,
                // under the effective key (folding in any LIMIT/SKIP parameter
                // values baked into this plan).
                let lp = Arc::new(lp);
                if let Ok(mut cache) = self.plan_cache.lock() {
                    cache.note_folded_params(text_key, folded);
                    let key = cache.effective_key(text_key, &params);
                    cache.insert(
                        key,
                        crate::api::session::PlanCacheEntry {
                            plan: Arc::clone(&lp),
                            query: cypher.to_string(),
                            schema_version,
                            hit_count: 0,
                        },
                    );
                }
                (lp, parse_time, plan_time, false)
            };

        // Rewrites run on every execution: cheap tree-walks that must reflect
        // live fork-index state, so they are intentionally not cached. The
        // owned plan is produced here, outside the plan-cache mutex.
        let logical_plan = {
            let lp = uni_query::rewrite_for_fork_fusion(
                Arc::unwrap_or_clone(logical_plan_raw),
                &*self.storage,
            );
            uni_query::fuse_create_set(lp)
        };

        // Clone the cached executor template instead of running
        // `Executor::new` + six session-constant setters every query.
        // The manual `Clone` impl installs a fresh `warnings` Mutex so
        // each query gets its own warnings accumulator.
        let mut executor = (*self.executor_template).clone();
        // Per-query state.
        if let Ok(reg) = self.custom_functions.read()
            && !reg.is_empty()
        {
            executor.set_custom_functions(Arc::new(reg.clone()));
        }
        executor.set_transaction_l0(tx_l0);
        executor.set_read_snapshot(read_snapshot);
        if let Some(r) = id_reservoir {
            executor.set_id_reservoir(r);
        }
        cancel.install(&mut executor);

        let projection_order = extract_projection_order(&logical_plan);

        let exec_start = Instant::now();
        // The transaction's own scope reaches execution here. Without this the
        // token accepted by `TxQueryBuilder::cancellation_token` was discarded
        // for want of a parameter to put it in, and `Transaction::cancel()`
        // cancelled a token nothing was listening to.
        let results = tokio::select! {
            biased;
            () = cancel.cancelled() => return Err(UniError::Cancelled),
            res = executor.execute(logical_plan, &self.properties, &params) => {
                res.map_err(|e| into_execution_error(e, cypher, self.config.query_timeout))?
            }
        };
        let exec_time = exec_start.elapsed();

        // `max_query_memory` was previously enforced only on the session's
        // materializing paths, so a transaction query had no ceiling at all.
        // Adding it to the cursor without adding it here would have handed the
        // transaction surface a fresh asymmetry — streaming stricter than
        // materializing — which is the shape of defect this work removes.
        enforce_memory_limit(&results, self.config.max_query_memory, cypher)?;

        let columns = columns_for_results(&results, projection_order);
        let rows = rows_for_results(results, &columns, true);

        let metrics = QueryMetrics {
            parse_time,
            plan_time,
            exec_time,
            total_time: total_start.elapsed(),
            rows_returned: rows.len(),
            plan_cache_hit,
            ..Default::default()
        };

        let mut result = QueryResult::new(columns, rows, executor.take_warnings(), metrics);
        result.set_counters(executor.take_counters());
        Ok(result)
    }

    /// Profile a Cypher query within a transaction's private L0 buffer.
    ///
    /// Parallel to [`Self::execute_internal_with_tx_l0`] but invokes
    /// `Executor::profile`, returning per-operator timing/memory stats
    /// alongside the result rows. The tx_l0 is installed on the executor
    /// so mutations land in the caller's private L0 (commit-time
    /// serialization) — this is what lets `ExecuteBuilder::profile()`
    /// profile a transaction write end-to-end.
    pub(crate) async fn profile_internal_with_tx_l0(
        &self,
        cypher: &str,
        params: HashMap<String, ApiValue>,
        tx_l0: std::sync::Arc<parking_lot::RwLock<uni_store::runtime::l0::L0Buffer>>,
        id_reservoir: Option<Arc<uni_store::runtime::TxIdReservoir>>,
        read_snapshot: Option<uni_store::runtime::SnapshotView>,
    ) -> Result<(QueryResult, ProfileOutput)> {
        let ast = uni_cypher::parse(cypher).map_err(into_parse_error)?;

        let (ast, tt_spec) = split_time_travel(ast);

        if tt_spec.is_some() {
            return Err(UniError::Query {
                message: "Time-travel queries are not supported within transactions".to_string(),
                query: Some(cypher.to_string()),
            });
        }

        let planner = self.base_planner().with_params(params.clone());
        let logical_plan = self.plan_and_rewrite(&planner, ast, cypher)?;

        let mut executor = uni_query::Executor::new(self.storage.clone());
        executor.set_config(self.config.clone());
        self.apply_session_executor_state(&mut executor);
        executor.set_transaction_l0(tx_l0);
        executor.set_read_snapshot(read_snapshot);
        if let Some(r) = id_reservoir {
            executor.set_id_reservoir(r);
        }

        let projection_order = extract_projection_order(&logical_plan);

        let (results, profile_output) = executor
            .profile(logical_plan, &params)
            .await
            .map_err(|e| into_execution_error(e, cypher, self.config.query_timeout))?;

        let columns = columns_for_results(&results, projection_order);
        let rows = rows_for_results(results, &columns, true);

        let metrics = QueryMetrics {
            rows_returned: rows.len(),
            ..Default::default()
        };
        let mut result = QueryResult::new(columns, rows, executor.take_warnings(), metrics);
        result.set_counters(executor.take_counters());
        Ok((result, profile_output))
    }

    /// Execute a cursor query with a private transaction L0 buffer.
    ///
    /// Mirrors `execute_cursor_internal_with_config` but installs the
    /// caller's private L0 so reads see uncommitted transaction writes.
    pub(crate) async fn execute_cursor_internal_with_tx_l0(
        &self,
        cypher: &str,
        params: HashMap<String, ApiValue>,
        tx_l0: std::sync::Arc<parking_lot::RwLock<uni_store::runtime::l0::L0Buffer>>,
        config: UniConfig,
        cancel: CancelScope,
    ) -> Result<QueryCursor> {
        let ast = uni_cypher::parse(cypher).map_err(into_parse_error)?;

        let (ast, tt_spec) = split_time_travel(ast);

        if tt_spec.is_some() {
            return Err(UniError::Query {
                message: "Time-travel queries are not supported within transactions".to_string(),
                query: Some(cypher.to_string()),
            });
        }

        let planner = self.base_planner().with_params(params.clone());
        let logical_plan = self.plan_and_rewrite(&planner, ast, cypher)?;

        let mut executor = uni_query::Executor::new(self.storage.clone());
        executor.set_config(config.clone());
        self.apply_session_executor_state(&mut executor);
        executor.set_transaction_l0(tx_l0);
        cancel.install(&mut executor);

        Ok(self.build_guarded_cursor(executor, logical_plan, params, cypher, &config, cancel))
    }

    pub(crate) async fn execute_internal_with_config(
        &self,
        cypher: &str,
        params: HashMap<String, ApiValue>,
        config: UniConfig,
        cancel: CancelScope,
    ) -> Result<QueryResult> {
        let total_start = Instant::now();

        // Single parse: extract time-travel clause if present
        let parse_start = Instant::now();
        let ast = uni_cypher::parse(cypher).map_err(into_parse_error)?;
        let parse_time = parse_start.elapsed();

        let (ast, tt_spec) = split_time_travel(ast);

        if let Some(spec) = tt_spec {
            uni_query::validate_read_only(&ast).map_err(|msg| into_query_error(msg, cypher))?;
            // Resolve to snapshot and execute on pinned instance
            let snapshot_id = self.resolve_time_travel(&spec).await?;
            let pinned = self.at_snapshot(&snapshot_id).await?;
            return pinned
                .execute_ast_internal(ast, cypher, params, config, cancel)
                .await;
        }

        let mut result = self
            .execute_ast_internal(ast, cypher, params, config, cancel)
            .await?;
        result.update_parse_timing(parse_time, total_start.elapsed());
        Ok(result)
    }

    /// Like `execute_internal_with_config` but also accepts a cancellation token.
    pub(crate) async fn execute_internal_with_config_and_token(
        &self,
        cypher: &str,
        params: HashMap<String, ApiValue>,
        config: UniConfig,
        cancel: CancelScope,
    ) -> Result<QueryResult> {
        let total_start = Instant::now();

        let parse_start = Instant::now();
        let ast = uni_cypher::parse(cypher).map_err(into_parse_error)?;
        let parse_time = parse_start.elapsed();

        let (ast, tt_spec) = split_time_travel(ast);

        if let Some(spec) = tt_spec {
            uni_query::validate_read_only(&ast).map_err(|msg| into_query_error(msg, cypher))?;
            let snapshot_id = self.resolve_time_travel(&spec).await?;
            let pinned = self.at_snapshot(&snapshot_id).await?;
            return pinned
                .execute_ast_internal(ast, cypher, params, config, cancel)
                .await;
        }

        let planner = self.base_planner().with_params(params.clone());
        let logical_plan = self.plan_and_rewrite(&planner, ast, cypher)?;

        let mut result = self
            .execute_plan_internal(logical_plan, cypher, params, config, cancel)
            .await?;
        result.update_parse_timing(parse_time, total_start.elapsed());
        Ok(result)
    }

    /// Execute a pre-parsed Cypher AST with a private transaction L0 override.
    pub(crate) async fn execute_ast_internal_with_tx_l0(
        &self,
        ast: uni_query::CypherQuery,
        cypher: &str,
        params: HashMap<String, ApiValue>,
        config: UniConfig,
        tx_l0: std::sync::Arc<parking_lot::RwLock<uni_store::runtime::l0::L0Buffer>>,
        cancel: CancelScope,
    ) -> Result<QueryResult> {
        let total_start = Instant::now();

        let plan_start = Instant::now();
        let planner = self.base_planner().with_params(params.clone());
        let logical_plan = self.plan_and_rewrite(&planner, ast, cypher)?;
        let plan_time = plan_start.elapsed();

        let mut executor = uni_query::Executor::new(self.storage.clone());
        executor.set_config(config.clone());
        self.apply_session_executor_state(&mut executor);
        executor.set_transaction_l0(tx_l0);
        cancel.install(&mut executor);

        let projection_order = extract_projection_order(&logical_plan);

        let exec_start = Instant::now();
        // Mirrors `execute_ast_internal`: the executor's own token only reaches
        // operators that happen to call `check_timeout`, so correctness comes
        // from racing the scope against execution here. This twin was the one
        // AST path that did neither, which is why `Transaction::cancel()` was
        // inert for `Transaction::apply` and tx-bound Locy.
        let results = tokio::select! {
            biased;
            () = cancel.cancelled() => return Err(UniError::Cancelled),
            res = executor.execute(logical_plan, &self.properties, &params) => res
                .map_err(|e| into_execution_error(e, cypher, config.query_timeout))?,
        };
        let exec_time = exec_start.elapsed();

        let columns = columns_for_results(&results, projection_order);
        let rows = rows_for_results(results, &columns, true);

        let metrics = QueryMetrics {
            parse_time: std::time::Duration::ZERO,
            plan_time,
            exec_time,
            total_time: total_start.elapsed(),
            rows_returned: rows.len(),
            ..Default::default()
        };

        let mut result = QueryResult::new(columns, rows, executor.take_warnings(), metrics);
        result.set_counters(executor.take_counters());
        Ok(result)
    }

    /// Execute a pre-parsed Cypher AST through the planner and executor.
    ///
    /// The `cypher` parameter is the original query string, used only for
    /// error messages.
    pub(crate) async fn execute_ast_internal(
        &self,
        ast: uni_query::CypherQuery,
        cypher: &str,
        params: HashMap<String, ApiValue>,
        config: UniConfig,
        cancel: CancelScope,
    ) -> Result<QueryResult> {
        let total_start = Instant::now();
        let deadline = total_start + config.query_timeout;

        let plan_start = Instant::now();
        let planner = self.base_planner().with_params(params.clone());
        let logical_plan = self.plan_and_rewrite(&planner, ast, cypher)?;
        let plan_time = plan_start.elapsed();

        let mut executor = uni_query::Executor::new(self.storage.clone());
        executor.set_config(config.clone());
        self.apply_session_executor_state(&mut executor);
        cancel.install(&mut executor);

        let projection_order = extract_projection_order(&logical_plan);

        let exec_start = Instant::now();
        let timeout_duration = config.query_timeout;
        // See `execute_plan_internal` for why the scope is raced here rather
        // than left to the executor's cooperative checks.
        let results = tokio::select! {
            biased;
            () = cancel.cancelled() => return Err(UniError::Cancelled),
            res = tokio::time::timeout(
                timeout_duration,
                executor.execute(logical_plan, &self.properties, &params),
            ) => res
                .map_err(|_| query_timed_out_error(timeout_duration))?
                .map_err(|e| into_execution_error(e, cypher, config.query_timeout))?,
        };
        let exec_time = exec_start.elapsed();

        // Instant-based deadline check for sub-millisecond timeouts that
        // tokio::time::timeout cannot catch due to timer wheel resolution.
        if Instant::now() > deadline {
            return Err(query_timed_out_error(timeout_duration));
        }

        enforce_memory_limit(&results, config.max_query_memory, cypher)?;

        let columns = columns_for_results(&results, projection_order);
        let rows = rows_for_results(results, &columns, true);

        let metrics = QueryMetrics {
            parse_time: std::time::Duration::ZERO,
            plan_time,
            exec_time,
            total_time: total_start.elapsed(),
            rows_returned: rows.len(),
            ..Default::default()
        };

        let mut result = QueryResult::new(columns, rows, executor.take_warnings(), metrics);
        result.set_counters(executor.take_counters());
        Ok(result)
    }

    /// Resolve a time-travel spec to a snapshot ID.
    async fn resolve_time_travel(&self, spec: &uni_query::TimeTravelSpec) -> Result<String> {
        match spec {
            uni_query::TimeTravelSpec::Version(id) => Ok(id.clone()),
            uni_query::TimeTravelSpec::Timestamp(ts_str) => {
                let ts = chrono::DateTime::parse_from_rfc3339(ts_str)
                    .map_err(|e| {
                        into_parse_error(format!("Invalid timestamp '{}': {}", ts_str, e))
                    })?
                    .with_timezone(&chrono::Utc);
                self.resolve_time_travel_timestamp(ts).await
            }
        }
    }

    /// Resolve a `chrono::DateTime<Utc>` to the snapshot ID of the closest
    /// snapshot at or before that timestamp.
    pub(crate) async fn resolve_time_travel_timestamp(
        &self,
        ts: chrono::DateTime<chrono::Utc>,
    ) -> Result<String> {
        let manifest = self
            .storage
            .snapshot_manager()
            .find_snapshot_at_time(ts)
            .await
            .map_err(UniError::Internal)?
            .ok_or_else(|| UniError::Query {
                message: format!("No snapshot found at or before {}", ts),
                query: None,
            })?;
        Ok(manifest.snapshot_id)
    }

    /// Execute a pre-built logical plan, skipping parse and plan phases.
    ///
    /// Used by the plan cache and prepared statements to re-execute
    /// previously planned queries.
    pub(crate) async fn execute_plan_internal(
        &self,
        plan: uni_query::LogicalPlan,
        cypher: &str,
        params: HashMap<String, ApiValue>,
        config: UniConfig,
        cancel: CancelScope,
    ) -> Result<QueryResult> {
        let total_start = Instant::now();

        let mut executor = uni_query::Executor::new(self.storage.clone());
        executor.set_config(config.clone());
        self.apply_session_executor_state(&mut executor);
        cancel.install(&mut executor);

        let projection_order = extract_projection_order(&plan);

        let exec_start = Instant::now();
        let deadline = exec_start + config.query_timeout;
        let timeout_duration = config.query_timeout;
        // Race execution against the cancellation scope. Handing the token to
        // the executor is not sufficient on its own: it only reaches
        // `GraphContext::check_timeout`, which no scan/join/traverse plan
        // calls, so a cancelled statement previously ran to completion and
        // returned its rows. `biased` makes an already-cancelled scope win
        // deterministically instead of racing the first poll.
        let results = tokio::select! {
            biased;
            () = cancel.cancelled() => return Err(UniError::Cancelled),
            res = tokio::time::timeout(
                timeout_duration,
                executor.execute(plan, &self.properties, &params),
            ) => res
                .map_err(|_| query_timed_out_error(timeout_duration))?
                .map_err(|e| into_execution_error(e, cypher, config.query_timeout))?,
        };
        let exec_time = exec_start.elapsed();

        if Instant::now() > deadline {
            return Err(query_timed_out_error(timeout_duration));
        }

        enforce_memory_limit(&results, config.max_query_memory, cypher)?;

        let columns = columns_for_results(&results, projection_order);
        let rows = rows_for_results(results, &columns, true);

        let metrics = QueryMetrics {
            parse_time: std::time::Duration::ZERO,
            plan_time: std::time::Duration::ZERO,
            exec_time,
            total_time: total_start.elapsed(),
            rows_returned: rows.len(),
            ..Default::default()
        };

        let mut result = QueryResult::new(columns, rows, executor.take_warnings(), metrics);
        result.set_counters(executor.take_counters());
        Ok(result)
    }
}
