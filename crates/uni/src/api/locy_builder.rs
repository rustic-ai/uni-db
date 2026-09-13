// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

use std::collections::HashMap;

use tokio_util::sync::CancellationToken;
use uni_common::{Result, Value};
use uni_locy::LocyConfig;

use crate::api::locy_result::LocyResult;

use crate::api::session::Session;
use crate::api::transaction::Transaction;

/// Builder for constructing and evaluating Locy programs via `LocyEngine` (UniInner-level).
///
/// Used by `LocyEngine::evaluate_with()` when only a `&UniInner` is available.
#[must_use = "InnerLocyBuilder does nothing until .run() is called"]
pub struct InnerLocyBuilder<'a> {
    db: &'a crate::api::UniInner,
    program: String,
    config: LocyConfig,
}

impl<'a> InnerLocyBuilder<'a> {
    pub(crate) fn new(db: &'a crate::api::UniInner, program: &str) -> Self {
        Self {
            db,
            program: program.to_string(),
            config: LocyConfig::default(),
        }
    }

    /// Bind a single parameter.  The name should not include the `$` prefix.
    ///
    /// Parameters bound here resolve `$name` references in the program and are
    /// forwarded by [`Self::run`] exactly like the positional `params` argument
    /// of `session.locy(program, params)` / `tx.locy(...)`. The builder does
    /// **not** pick up params implicitly: if a program references `$seed`, you
    /// must call `.param("seed", …)` (or `.params(…)`) on the builder —
    /// otherwise evaluation fails with `Unresolved parameter: $seed`.
    ///
    /// Note that a `$param` binding is distinct from a `LocyConfig` field: e.g.
    /// `$seed` (a query parameter) is unrelated to any `seed` set via
    /// `.with_config(...)`; setting the latter does not bind the former.
    pub fn param(mut self, name: impl Into<String>, value: impl Into<Value>) -> Self {
        self.config.params.insert(name.into(), value.into());
        self
    }

    /// Bind multiple parameters from an iterator. See [`Self::param`] for how
    /// bindings resolve `$name` references and are forwarded by [`Self::run`].
    pub fn params<'p>(mut self, params: impl IntoIterator<Item = (&'p str, Value)>) -> Self {
        for (k, v) in params {
            self.config.params.insert(k.to_string(), v);
        }
        self
    }

    /// Bind parameters from a `HashMap`.
    pub fn params_map(mut self, params: HashMap<String, Value>) -> Self {
        self.config.params.extend(params);
        self
    }

    /// Override the evaluation timeout.
    pub fn timeout(mut self, duration: std::time::Duration) -> Self {
        self.config.timeout = duration;
        self
    }

    /// Opt into best-effort semantics: return the partial [`LocyResult`] (with
    /// its `incomplete` diagnostics) instead of erroring when the evaluation
    /// exceeds its `timeout` or `max_iterations`.
    ///
    /// Off by default — an over-budget evaluation otherwise returns
    /// [`UniError::LocyIncomplete`]. Partial results may be unsound for
    /// complement (`IS NOT`) rules; inspect `LocyResult::incomplete`.
    ///
    /// [`UniError::LocyIncomplete`]: uni_common::UniError::LocyIncomplete
    pub fn allow_partial(mut self, allow: bool) -> Self {
        self.config.allow_partial = allow;
        self
    }

    /// Override the maximum fixpoint iteration count.
    pub fn max_iterations(mut self, n: usize) -> Self {
        self.config.max_iterations = n;
        self
    }

    /// Apply a fully configured [`LocyConfig`].
    pub fn with_config(mut self, mut config: LocyConfig) -> Self {
        config.params.extend(self.config.params);
        self.config = config;
        self
    }

    /// Evaluate the program and return the full [`LocyResult`].
    pub async fn run(self) -> Result<LocyResult> {
        let engine = crate::api::impl_locy::LocyEngine {
            counters: std::sync::Arc::new(uni_store::QueryCounters::new()),
            db: self.db,
            tx_l0_override: None,
            locy_l0: None,
            collect_derive: true,
            read_snapshot: None,
            // `InnerLocyBuilder` is the internal `&UniInner` entry point; it has
            // no session or transaction to inherit a scope from and exposes no
            // token setter.
            cancel: crate::api::impl_query::CancelScope::default(),
        };
        engine
            .evaluate_with_config(&self.program, &self.config)
            .await
    }
}

/// Builder for constructing and evaluating Locy programs (Session-level).
///
/// Uses the session's rule registry for compilation and evaluation.
#[must_use = "LocyBuilder does nothing until .run() is called"]
pub struct LocyBuilder<'a> {
    session: &'a Session,
    program: String,
    config: LocyConfig,
    cancellation_token: Option<CancellationToken>,
}

impl<'a> LocyBuilder<'a> {
    pub(crate) fn new(session: &'a Session, program: &str) -> Self {
        Self {
            session,
            program: program.to_string(),
            config: LocyConfig::default(),
            cancellation_token: None,
        }
    }

    /// Bind a single parameter.
    pub fn param(mut self, name: impl Into<String>, value: impl Into<Value>) -> Self {
        self.config.params.insert(name.into(), value.into());
        self
    }

    /// Bind multiple parameters from an iterator.
    pub fn params<'p>(mut self, params: impl IntoIterator<Item = (&'p str, Value)>) -> Self {
        for (k, v) in params {
            self.config.params.insert(k.to_string(), v);
        }
        self
    }

    /// Bind parameters from a `HashMap`.
    pub fn params_map(mut self, params: HashMap<String, Value>) -> Self {
        self.config.params.extend(params);
        self
    }

    /// Override the evaluation timeout.
    pub fn timeout(mut self, duration: std::time::Duration) -> Self {
        self.config.timeout = duration;
        self
    }

    /// Opt into best-effort semantics: return the partial [`LocyResult`] (with
    /// its `incomplete` diagnostics) instead of erroring when the evaluation
    /// exceeds its `timeout` or `max_iterations`.
    ///
    /// Off by default — an over-budget evaluation otherwise returns
    /// [`UniError::LocyIncomplete`]. Partial results may be unsound for
    /// complement (`IS NOT`) rules; inspect `LocyResult::incomplete`.
    ///
    /// [`UniError::LocyIncomplete`]: uni_common::UniError::LocyIncomplete
    pub fn allow_partial(mut self, allow: bool) -> Self {
        self.config.allow_partial = allow;
        self
    }

    /// Override the maximum fixpoint iteration count.
    pub fn max_iterations(mut self, n: usize) -> Self {
        self.config.max_iterations = n;
        self
    }

    /// Attach a cancellation token for cooperative query cancellation.
    pub fn cancellation_token(mut self, token: CancellationToken) -> Self {
        self.cancellation_token = Some(token);
        self
    }

    /// Apply a fully configured [`LocyConfig`].
    pub fn with_config(mut self, mut config: LocyConfig) -> Self {
        config.params.extend(self.config.params);
        self.config = config;
        self
    }

    /// Evaluate the program and return the full [`LocyResult`].
    pub async fn run(self) -> Result<LocyResult> {
        // Expose a plugin registry (and principal) as task-locals so in-memory
        // Locy dispatch — e.g. `LocyGenerator` / `LocyPredicate` via
        // `eval_function` on the SLG path — can resolve registered plugins.
        // `add_plugin` registers into the instance registry, so scope that (the
        // Cypher query path reaches instance plugins through its UDF-registration
        // step; the SLG eval path reads only this task-local). Without it the SLG
        // resolver sees no registry and a registered generator/predicate is
        // reported "not registered".
        let session_pr = std::sync::Arc::clone(self.session.instance_plugin_registry());
        let principal = self.session.principal.clone();
        uni_query::scoped_with_session_context(
            session_pr,
            principal,
            crate::api::impl_locy::evaluate_with_db_and_config(
                &self.session.db,
                &self.program,
                &self.config,
                self.session.rule_registry(),
                self.session.cancel_scope(self.cancellation_token.clone()),
            ),
        )
        .await
    }

    /// Explain the program without executing it.
    ///
    /// Compiles the program and returns plan introspection data (strata,
    /// rule names, recursion info, compiler warnings).
    pub fn explain(self) -> Result<crate::api::locy_result::LocyExplainOutput> {
        let compiled = self.session.compile_locy(&self.program)?;
        Ok(crate::api::locy_result::LocyExplainOutput::from_compiled(
            &compiled,
        ))
    }

    /// Evaluate the program and return the result plus a structured execution
    /// profile (per-stratum / per-rule / per-iteration timing, fact deltas, and
    /// per-operator metrics). The Locy analog of Cypher's `query.profile()`.
    pub async fn profile(self) -> Result<(LocyResult, crate::api::locy_result::LocyProfileOutput)> {
        let explain = crate::api::locy_result::LocyExplainOutput::from_compiled(
            &self.session.compile_locy(&self.program)?,
        );
        let capture = std::sync::Arc::new(std::sync::Mutex::new(None));
        let result = crate::api::impl_locy::evaluate_with_db_and_config_capturing(
            &self.session.db,
            &self.program,
            &self.config,
            self.session.rule_registry(),
            Some(&capture),
            self.session.cancel_scope(self.cancellation_token.clone()),
        )
        .await?;
        let profile = capture
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .take()
            .unwrap_or_default();
        Ok((
            result,
            crate::api::locy_result::LocyProfileOutput::new(explain, profile),
        ))
    }
}

/// Builder for constructing and evaluating Locy programs (Transaction-level).
///
/// Uses the transaction's private L0 buffer so the Locy engine sees uncommitted
/// writes. DERIVE commands auto-apply to the private L0.
#[must_use = "TxLocyBuilder does nothing until .run() is called"]
pub struct TxLocyBuilder<'a> {
    tx: &'a Transaction,
    program: String,
    config: LocyConfig,
    cancellation_token: Option<CancellationToken>,
}

impl<'a> TxLocyBuilder<'a> {
    pub(crate) fn new(tx: &'a Transaction, program: &str) -> Self {
        Self {
            tx,
            program: program.to_string(),
            config: LocyConfig::default(),
            cancellation_token: None,
        }
    }

    /// Bind a single parameter.
    pub fn param(mut self, name: impl Into<String>, value: impl Into<Value>) -> Self {
        self.config.params.insert(name.into(), value.into());
        self
    }

    /// Bind multiple parameters from an iterator.
    pub fn params<'p>(mut self, params: impl IntoIterator<Item = (&'p str, Value)>) -> Self {
        for (k, v) in params {
            self.config.params.insert(k.to_string(), v);
        }
        self
    }

    /// Bind parameters from a `HashMap`.
    pub fn params_map(mut self, params: HashMap<String, Value>) -> Self {
        self.config.params.extend(params);
        self
    }

    /// Override the evaluation timeout.
    pub fn timeout(mut self, duration: std::time::Duration) -> Self {
        self.config.timeout = duration;
        self
    }

    /// Opt into best-effort semantics: return the partial [`LocyResult`] (with
    /// its `incomplete` diagnostics) instead of erroring when the evaluation
    /// exceeds its `timeout` or `max_iterations`.
    ///
    /// Off by default — an over-budget evaluation otherwise returns
    /// [`UniError::LocyIncomplete`]. Partial results may be unsound for
    /// complement (`IS NOT`) rules; inspect `LocyResult::incomplete`.
    ///
    /// [`UniError::LocyIncomplete`]: uni_common::UniError::LocyIncomplete
    pub fn allow_partial(mut self, allow: bool) -> Self {
        self.config.allow_partial = allow;
        self
    }

    /// Override the maximum fixpoint iteration count.
    pub fn max_iterations(mut self, n: usize) -> Self {
        self.config.max_iterations = n;
        self
    }

    /// Attach a cancellation token for cooperative query cancellation.
    pub fn cancellation_token(mut self, token: CancellationToken) -> Self {
        self.cancellation_token = Some(token);
        self
    }

    /// Apply a fully configured [`LocyConfig`].
    pub fn with_config(mut self, mut config: LocyConfig) -> Self {
        config.params.extend(self.config.params);
        self.config = config;
        self
    }

    /// Evaluate the program and return the full [`LocyResult`].
    pub async fn run(self) -> Result<LocyResult> {
        let engine = crate::api::impl_locy::LocyEngine {
            counters: std::sync::Arc::new(uni_store::QueryCounters::new()),
            db: &self.tx.db,
            tx_l0_override: Some(self.tx.tx_l0.clone()),
            locy_l0: Some(self.tx.tx_l0.clone()),
            collect_derive: false,
            read_snapshot: self.tx.read_snapshot(),
            cancel: self.tx.cancel_scope(self.cancellation_token.clone()),
        };
        engine
            .evaluate_with_config(&self.program, &self.config)
            .await
    }

    /// Evaluate the program and return the result plus a structured execution
    /// profile. Transaction-level analog of [`LocyBuilder::profile`].
    pub async fn profile(self) -> Result<(LocyResult, crate::api::locy_result::LocyProfileOutput)> {
        let engine = crate::api::impl_locy::LocyEngine {
            counters: std::sync::Arc::new(uni_store::QueryCounters::new()),
            db: &self.tx.db,
            tx_l0_override: Some(self.tx.tx_l0.clone()),
            locy_l0: Some(self.tx.tx_l0.clone()),
            collect_derive: false,
            read_snapshot: self.tx.read_snapshot(),
            cancel: self.tx.cancel_scope(self.cancellation_token.clone()),
        };
        let explain = crate::api::locy_result::LocyExplainOutput::from_compiled(
            &engine.compile_only(&self.program)?,
        );
        let capture = std::sync::Arc::new(std::sync::Mutex::new(None));
        let result = engine
            .evaluate_with_config_capturing(&self.program, &self.config, Some(&capture))
            .await?;
        let profile = capture
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .take()
            .unwrap_or_default();
        Ok((
            result,
            crate::api::locy_result::LocyProfileOutput::new(explain, profile),
        ))
    }
}
