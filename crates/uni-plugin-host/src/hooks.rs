// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Session hooks — before/after interception for queries and commits.
//!
//! Hooks allow cross-cutting concerns (audit logging, authorization, metrics)
//! to be injected into the query and commit lifecycle without modifying
//! individual query call sites.

use std::collections::HashMap;

use uni_common::{Result, Value};
use uni_query::QueryMetrics;

use crate::commit_result::CommitResult;

/// The type of query being executed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QueryType {
    /// A Cypher query (read or write).
    Cypher,
    /// A Locy program evaluation.
    Locy,
    /// An execute (mutation) statement.
    Execute,
}

/// Context passed to query hooks.
#[derive(Debug, Clone)]
pub struct HookContext {
    /// The session ID that initiated the query.
    pub session_id: String,
    /// The query text (Cypher or Locy program).
    pub query_text: String,
    /// The type of query.
    pub query_type: QueryType,
    /// Parameters bound to the query.
    pub params: HashMap<String, Value>,
}

/// Context passed to commit hooks.
#[derive(Debug, Clone)]
pub struct CommitHookContext {
    /// The session ID that owns the transaction.
    pub session_id: String,
    /// The transaction ID being committed.
    pub tx_id: String,
    /// Number of mutations in the transaction.
    pub mutation_count: usize,
}

/// Trait for session lifecycle hooks.
///
/// Implement this trait to intercept queries and commits at the session level.
/// Hooks are stored as `Arc<dyn SessionHook>` and can be shared across sessions
/// and templates.
///
/// # Failure Semantics
///
/// - `before_query`: Returning `Err` aborts the query with `HookRejected`.
/// - `after_query`: Infallible — panics are caught and logged.
/// - `before_commit`: Returning `Err` aborts the commit with `HookRejected`.
/// - `after_commit`: Infallible — panics are caught and logged.
pub trait SessionHook: Send + Sync {
    /// Called before a query is executed. Return `Err` to reject the query.
    fn before_query(&self, _ctx: &HookContext) -> Result<()> {
        Ok(())
    }

    /// Called after a query completes. Panics are caught and logged.
    fn after_query(&self, _ctx: &HookContext, _metrics: &QueryMetrics) {}

    /// Called before a transaction is committed. Return `Err` to reject the commit.
    fn before_commit(&self, _ctx: &CommitHookContext) -> Result<()> {
        Ok(())
    }

    /// Called after a transaction is successfully committed. Panics are caught and logged.
    fn after_commit(&self, _ctx: &CommitHookContext, _result: &CommitResult) {}
}

// ============================================================================
// M5e — Phased-hook bridge.
//
// The plugin framework ships a richer, Postgres-style phased SessionHook
// (`uni_plugin::traits::hook::SessionHook`) with on_parse / on_analyze /
// on_plan / on_execute_start / on_execute_end / before_commit /
// after_commit / on_abort. [`LegacyHookAdapter`] wraps a legacy
// 4-method [`SessionHook`] into the phased trait so existing hooks can
// be registered through `PluginRegistrar::hook()` without being
// rewritten.
//
// Routing chosen to match the legacy semantics:
// - legacy `before_query` → phased `on_parse` (earliest pre-execution
//   phase that can reject).
// - legacy `after_query`  → phased `on_execute_end`.
// - legacy `before_commit`→ phased `before_commit`.
// - legacy `after_commit` → phased `after_commit`.
//
// Phases the legacy trait does not model (analyze, plan,
// execute_start, abort) are pass-through `Continue`s.
//
// `Session::add_hook` continues to dispatch through its in-process
// HashMap for legacy compatibility; the bridge enables hooks to ALSO
// participate in the plugin registry's phased dispatch when the host
// chooses to surface them that way (the migration of Session's own
// dispatch onto the plugin registry is a separate, larger change).
// ============================================================================

use std::sync::Arc;

use datafusion::scalar::ScalarValue;
use uni_plugin::errors::HookOutcome;
use uni_plugin::traits::hook::{
    AbortContext, AnalyzeContext, CommitContext as PluginCommitContext, ExecuteContext,
    ParseContext, PlanContext, QueryMetrics as PluginQueryMetrics, QueryType as PluginQueryType,
    SessionHook as PluginSessionHook,
};

/// Adapter: wraps a legacy [`SessionHook`] so it satisfies the phased
/// [`uni_plugin::traits::hook::SessionHook`] contract and can be
/// registered through [`uni_plugin::PluginRegistrar::hook`].
///
/// The adapter holds the legacy hook by `Arc` so multiple registrations
/// (legacy `Session::add_hook` and a phased `Uni::add_plugin`) can share
/// the same underlying implementation without duplicating its state.
pub struct LegacyHookAdapter {
    name: String,
    inner: Arc<dyn SessionHook>,
}

impl LegacyHookAdapter {
    /// Construct an adapter from any legacy [`SessionHook`].
    ///
    /// The `name` is used only for diagnostics — phased dispatch keys
    /// off the registered hook entry, not the legacy name.
    #[must_use]
    pub fn new(name: impl Into<String>, inner: Arc<dyn SessionHook>) -> Self {
        Self {
            name: name.into(),
            inner,
        }
    }

    /// Diagnostic name of the wrapped legacy hook.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }
}

impl std::fmt::Debug for LegacyHookAdapter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LegacyHookAdapter")
            .field("name", &self.name)
            .finish_non_exhaustive()
    }
}

impl PluginSessionHook for LegacyHookAdapter {
    fn on_parse(&self, ctx: &ParseContext<'_>) -> HookOutcome {
        // Synthesize a legacy HookContext at the earliest pre-execution
        // phase. v1.1 fields on `ParseContext` (query_type, params) now
        // flow through; hosts that leave them at default still see the
        // pre-v1.1 behavior (Cypher / empty params).
        let legacy_ctx = HookContext {
            session_id: ctx.session_id.to_owned(),
            query_text: ctx.source.to_owned(),
            query_type: plugin_query_type_to_legacy(ctx.query_type),
            params: params_to_legacy(ctx.params),
        };
        match self.inner.before_query(&legacy_ctx) {
            Ok(()) => HookOutcome::Continue,
            Err(e) => HookOutcome::Reject {
                reason: e.to_string(),
            },
        }
    }

    fn on_execute_end(&self, ctx: &ExecuteContext<'_>, metrics: &PluginQueryMetrics) {
        // Legacy after_query expects a `uni_query::QueryMetrics`. The
        // plugin-side `QueryMetrics` is structurally different; surface
        // the wall-clock + row count via a fresh, zero-filled legacy
        // instance so the legacy hook still observes timing data.
        let legacy_ctx = HookContext {
            session_id: ctx.session_id.to_owned(),
            query_text: String::new(),
            query_type: QueryType::Cypher,
            params: HashMap::new(),
        };
        let legacy_metrics = uni_query::QueryMetrics {
            total_time: metrics.elapsed,
            rows_returned: metrics.rows_out as usize,
            bytes_read: metrics.bytes_read as usize,
            ..Default::default()
        };
        self.inner.after_query(&legacy_ctx, &legacy_metrics);
    }

    fn before_commit(&self, ctx: &PluginCommitContext<'_>) -> HookOutcome {
        let legacy_ctx = CommitHookContext {
            session_id: ctx.session_id.to_owned(),
            tx_id: String::new(),
            mutation_count: 0,
        };
        match self.inner.before_commit(&legacy_ctx) {
            Ok(()) => HookOutcome::Continue,
            Err(e) => HookOutcome::Reject {
                reason: e.to_string(),
            },
        }
    }

    fn after_commit(&self, ctx: &PluginCommitContext<'_>) {
        let legacy_ctx = CommitHookContext {
            session_id: ctx.session_id.to_owned(),
            tx_id: String::new(),
            mutation_count: ctx.commit_result.map(|r| r.mutations as usize).unwrap_or(0),
        };
        // v1.1 path: if the host populated `commit_result`, mirror its
        // fields into the legacy `CommitResult`. Otherwise keep the
        // pre-v1.1 zero-stub behavior for backward compat (the all-zero
        // `CommitResult::default()`).
        let result = ctx
            .commit_result
            .map(|r| CommitResult {
                mutations_committed: r.mutations as usize,
                version: r.version,
                wal_lsn: r.wal_lsn,
                duration: r.duration,
                ..CommitResult::default()
            })
            .unwrap_or_default();
        self.inner.after_commit(&legacy_ctx, &result);
    }

    // Phases not modeled by the legacy trait pass through with no-op
    // defaults; `#[non_exhaustive]` context types are tolerated.
    fn on_analyze(&self, _ctx: &AnalyzeContext<'_>) -> HookOutcome {
        HookOutcome::Continue
    }
    fn on_plan(&self, _ctx: &PlanContext<'_>) -> HookOutcome {
        HookOutcome::Continue
    }
    fn on_execute_start(&self, _ctx: &ExecuteContext<'_>) -> HookOutcome {
        HookOutcome::Continue
    }
    fn on_abort(&self, _ctx: &AbortContext<'_>) {}
}

/// Translate `uni-plugin`'s phased [`PluginQueryType`] enum to the
/// host's legacy [`QueryType`]. Both enums carry the same three
/// variants; we keep them separate so `uni-plugin` doesn't need a
/// `uni-db` dep.
fn plugin_query_type_to_legacy(t: PluginQueryType) -> QueryType {
    match t {
        PluginQueryType::Cypher => QueryType::Cypher,
        PluginQueryType::Locy => QueryType::Locy,
        PluginQueryType::Execute => QueryType::Execute,
    }
}

/// Best-effort conversion from the Arrow-shaped phased
/// `&[(SmolStr, ScalarValue)]` params slice to the legacy
/// `HashMap<String, Value>` shape. Primitive types (bool, int, float,
/// string, bytes) map directly; everything else (lists, structs, …)
/// is surfaced as [`Value::Null`] with a tracing warning so the legacy
/// hook still sees the key.
fn params_to_legacy<S: AsRef<str>>(
    params: &[(S, ScalarValue)],
) -> HashMap<String, uni_common::Value> {
    params
        .iter()
        .map(|(k, v)| (k.as_ref().to_owned(), scalar_to_value(v)))
        .collect()
}

// ============================================================================
// M5e — `BuiltinHookPlugin`: turns a legacy [`SessionHook`] into a
// [`uni_plugin::Plugin`] so it can be installed through
// [`crate::api::Uni::add_plugin`]. This is the public sugar the
// `Session::add_hook` deprecation note has been pointing at.
// ============================================================================

use std::sync::atomic::{AtomicU64, Ordering};

use uni_plugin::{
    AbiRange, Capability, CapabilitySet, Determinism, Plugin, PluginError, PluginId,
    PluginManifest, PluginRegistrar, ProvidedSurfaces, Scope, SideEffects as PluginSideEffects,
};

/// Wraps a legacy [`SessionHook`] in a [`Plugin`] so the host can install
/// it through `Uni::add_plugin`. The wrapped hook is registered via
/// [`LegacyHookAdapter`] so it participates in the phased
/// `uni_plugin::SessionHook` dispatch chain.
///
/// Each `BuiltinHookPlugin::new` call mints a unique plugin id from a
/// monotonic atomic counter so repeated `add_hook`-style registrations
/// never collide on the registry. The id is not meant to be addressable
/// externally — it's an implementation detail of the registration path.
pub struct BuiltinHookPlugin {
    manifest: PluginManifest,
    adapter: Arc<LegacyHookAdapter>,
}

static BUILTIN_HOOK_PLUGIN_SEQ: AtomicU64 = AtomicU64::new(0);

impl BuiltinHookPlugin {
    /// Wrap a legacy [`SessionHook`] in a plugin wrapper. `name` is used
    /// as the diagnostic label on the wrapped adapter; it is not the
    /// plugin id (which is generated from a static counter for
    /// uniqueness).
    #[must_use]
    pub fn new(name: impl Into<String>, hook: Arc<dyn SessionHook>) -> Self {
        let name = name.into();
        let seq = BUILTIN_HOOK_PLUGIN_SEQ.fetch_add(1, Ordering::Relaxed);
        let id = PluginId::new(format!("builtin.hook.{seq}"));
        let manifest = PluginManifest {
            id,
            version: "1.0.0".parse().expect("static version parses"),
            abi: AbiRange::parse("^1").expect("static ABI range parses"),
            depends_on: vec![],
            capabilities: CapabilitySet::from_iter_of([Capability::Hook]),
            determinism: Determinism::Nondeterministic,
            side_effects: PluginSideEffects::ReadOnly,
            scope: Scope::Instance,
            hash: None,
            signature: None,
            provides: ProvidedSurfaces::default(),
            docs: "BuiltinHookPlugin — legacy SessionHook adapter".to_owned(),
            metadata: std::collections::BTreeMap::new(),
        };
        Self {
            manifest,
            adapter: Arc::new(LegacyHookAdapter::new(name, hook)),
        }
    }
}

impl Plugin for BuiltinHookPlugin {
    fn manifest(&self) -> &PluginManifest {
        &self.manifest
    }

    fn register(&self, r: &mut PluginRegistrar<'_>) -> std::result::Result<(), PluginError> {
        r.hook(Arc::clone(&self.adapter) as Arc<dyn PluginSessionHook>)?;
        Ok(())
    }
}

fn scalar_to_value(v: &ScalarValue) -> uni_common::Value {
    use uni_common::Value;
    match v {
        ScalarValue::Null => Value::Null,
        ScalarValue::Boolean(Some(b)) => Value::Bool(*b),
        ScalarValue::Int8(Some(i)) => Value::Int(i64::from(*i)),
        ScalarValue::Int16(Some(i)) => Value::Int(i64::from(*i)),
        ScalarValue::Int32(Some(i)) => Value::Int(i64::from(*i)),
        ScalarValue::Int64(Some(i)) => Value::Int(*i),
        ScalarValue::UInt8(Some(i)) => Value::Int(i64::from(*i)),
        ScalarValue::UInt16(Some(i)) => Value::Int(i64::from(*i)),
        ScalarValue::UInt32(Some(i)) => Value::Int(i64::from(*i)),
        ScalarValue::UInt64(Some(i)) => {
            // #233 Tier 1: this saturated to `i64::MAX`, handing the legacy
            // hook a plausible-looking number that is not the parameter the
            // caller passed. This module already surfaces every other
            // unrepresentable value as `Null` with a warning (see the
            // `params_to_legacy` doc), so follow that convention instead of
            // fabricating a value.
            i64::try_from(*i).map_or_else(
                |_| {
                    tracing::warn!(
                        value = *i,
                        "UInt64 parameter exceeds i64::MAX; surfacing as Null to the legacy hook \
                         rather than saturating",
                    );
                    Value::Null
                },
                Value::Int,
            )
        }
        ScalarValue::Float32(Some(f)) => Value::Float(f64::from(*f)),
        ScalarValue::Float64(Some(f)) => Value::Float(*f),
        ScalarValue::Utf8(Some(s))
        | ScalarValue::LargeUtf8(Some(s))
        | ScalarValue::Utf8View(Some(s)) => Value::String(s.clone()),
        ScalarValue::Binary(Some(b))
        | ScalarValue::LargeBinary(Some(b))
        | ScalarValue::BinaryView(Some(b)) => Value::Bytes(b.clone()),
        other => {
            tracing::warn!(
                "LegacyHookAdapter::params_to_legacy: unsupported ScalarValue \
                 variant {other:?}; surfacing as Value::Null. Hooks needing \
                 typed access should register against the phased trait."
            );
            Value::Null
        }
    }
}
