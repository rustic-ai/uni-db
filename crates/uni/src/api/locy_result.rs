// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Spec-compliant wrapper for `uni_locy::LocyResult` (§7.4).
//!
//! Adds `metrics()` and `derived()` accessors while exposing every
//! field and method of the inner type transparently via `Deref`.

use std::fmt::Write as _;
use std::ops::Deref;

use uni_locy::{CompiledProgram, DerivedFactSet};
use uni_query::QueryMetrics;

/// Locy evaluation result with spec-compliant accessors.
///
/// Wraps `uni_locy::LocyResult` and adds:
/// - `metrics()` → `&QueryMetrics` (§7.4)
/// - `derived()` → `Option<&DerivedFactSet>` (spec-compliant alias for `derived_fact_set`)
///
/// All fields and methods of the inner `uni_locy::LocyResult` are accessible
/// via `Deref` (e.g. `result.stats`, `result.rows()`, `result.warnings()`).
#[derive(Debug, Clone)]
pub struct LocyResult {
    inner: uni_locy::LocyResult,
    metrics: QueryMetrics,
}

impl LocyResult {
    pub(crate) fn new(inner: uni_locy::LocyResult, metrics: QueryMetrics) -> Self {
        Self { inner, metrics }
    }

    /// Query-level execution metrics (timing, row counts, cache stats).
    pub fn metrics(&self) -> &QueryMetrics {
        &self.metrics
    }

    /// Spec-compliant accessor for the derived fact set (§7.4).
    pub fn derived(&self) -> Option<&DerivedFactSet> {
        self.inner.derived_fact_set.as_ref()
    }

    /// Unwrap into the inner `uni_locy::LocyResult`, discarding metrics.
    pub fn into_inner(self) -> uni_locy::LocyResult {
        self.inner
    }

    /// Decompose into inner result and metrics.
    pub fn into_parts(self) -> (uni_locy::LocyResult, QueryMetrics) {
        (self.inner, self.metrics)
    }
}

impl Deref for LocyResult {
    type Target = uni_locy::LocyResult;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

// ── Explain Output ──────────────────────────────────────────────────────

/// Output of Locy `explain()` — plan introspection without execution.
///
/// Contains the stratification plan, rule names, and compiler warnings
/// produced by compiling the Locy program. No data is read or written.
#[derive(Debug, Clone)]
pub struct LocyExplainOutput {
    /// Human-readable text of the stratification and evaluation plan.
    pub plan_text: String,
    /// Number of evaluation strata.
    pub strata_count: usize,
    /// Names of rules defined in the program.
    pub rule_names: Vec<String>,
    /// Whether any stratum requires fixpoint iteration.
    pub has_recursive_strata: bool,
    /// Compiler warnings from static analysis.
    pub warnings: Vec<String>,
    /// Number of commands (goal queries, DERIVE, ASSUME, etc.).
    pub command_count: usize,
}

impl LocyExplainOutput {
    /// Build from a compiled program (no execution needed).
    pub(crate) fn from_compiled(compiled: &CompiledProgram) -> Self {
        let strata_count = compiled.strata.len();
        let rule_names: Vec<String> = compiled.rule_catalog.keys().cloned().collect();
        let has_recursive_strata = compiled.strata.iter().any(|s| s.is_recursive);
        let warnings: Vec<String> = compiled
            .warnings
            .iter()
            .map(|w| w.message.clone())
            .collect();
        let command_count = compiled.commands.len();
        let plan_text = format_locy_plan(compiled);

        Self {
            plan_text,
            strata_count,
            rule_names,
            has_recursive_strata,
            warnings,
            command_count,
        }
    }
}

/// Format a human-readable plan from a compiled Locy program.
fn format_locy_plan(compiled: &CompiledProgram) -> String {
    let mut out = String::new();

    for stratum in &compiled.strata {
        let kind = if stratum.is_recursive {
            "recursive"
        } else {
            "non-recursive"
        };
        let deps = if stratum.depends_on.is_empty() {
            String::new()
        } else {
            let ids: Vec<String> = stratum.depends_on.iter().map(|d| d.to_string()).collect();
            format!(", depends on [{}]", ids.join(", "))
        };
        let _ = writeln!(out, "Stratum {} ({kind}{deps}):", stratum.id);
        for rule in &stratum.rules {
            let clause_count = rule.clauses.len();
            let plural = if clause_count == 1 { "" } else { "s" };
            let _ = writeln!(out, "  rule: {} ({clause_count} clause{plural})", rule.name);
        }
    }

    let cmd_count = compiled.commands.len();
    if cmd_count > 0 {
        let plural = if cmd_count == 1 { "" } else { "s" };
        let _ = writeln!(out, "Commands: {cmd_count} command{plural}");
    }

    if !compiled.warnings.is_empty() {
        let _ = writeln!(out, "Warnings:");
        for w in &compiled.warnings {
            let _ = writeln!(out, "  - [{}] {}", w.rule_name, w.message);
        }
    }

    out
}

// ── Profile Output ──────────────────────────────────────────────────────

/// Output of Locy `profile()` — an execution-plan explanation plus a
/// structured, per-iteration runtime profile.
///
/// The analog of Cypher's `ProfileOutput`, shaped to Locy's cost model: the
/// runtime detail is a stratum → rule → fixpoint-iteration tree, each iteration
/// carrying the same per-operator metrics Cypher reports. Use the [`Display`]
/// impl for a readable rendering.
///
/// [`Display`]: std::fmt::Display
#[derive(Debug, Clone)]
pub struct LocyProfileOutput {
    /// Compile-time plan introspection (identical to `explain()`).
    pub explain: LocyExplainOutput,
    /// Structured per-stratum / per-rule / per-iteration runtime profile.
    pub profile: uni_query::LocyExecProfile,
    /// Total program evaluation wall-clock time, in milliseconds.
    pub total_time_ms: f64,
    /// Peak derived-fact memory, in bytes.
    pub peak_memory_bytes: usize,
}

impl LocyProfileOutput {
    /// Combine a compile-time explanation with the runtime execution profile.
    pub(crate) fn new(explain: LocyExplainOutput, profile: uni_query::LocyExecProfile) -> Self {
        let total_time_ms = profile.total_elapsed_ms;
        let peak_memory_bytes = profile.peak_memory_bytes;
        Self {
            explain,
            profile,
            total_time_ms,
            peak_memory_bytes,
        }
    }
}

impl std::fmt::Display for LocyProfileOutput {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(
            f,
            "Locy Profile  total {:.3} ms  peak {} bytes  {} strata",
            self.total_time_ms,
            self.peak_memory_bytes,
            self.profile.strata.len()
        )?;
        for s in &self.profile.strata {
            let kind = if s.recursive {
                "recursive"
            } else {
                "non-recursive"
            };
            writeln!(
                f,
                "  Stratum {} [{kind}]  {:.3} ms  {} iter{}  {} facts",
                s.index,
                s.elapsed_ms,
                s.iterations,
                if s.iterations == 1 { "" } else { "s" },
                s.facts_derived,
            )?;
            for r in &s.rules {
                writeln!(f, "    rule {}  {} facts", r.name, r.facts)?;
                for it in &r.iterations {
                    writeln!(
                        f,
                        "      iter {}: +{} facts ({:.3} ms, planning {:.3} ms / {} plan{})",
                        it.iteration,
                        it.delta_facts,
                        it.elapsed_ms,
                        it.planning_ms,
                        it.plans,
                        if it.plans == 1 { "" } else { "s" },
                    )?;
                    for op in &it.operators {
                        writeln!(
                            f,
                            "        {}  {} rows  {:.3} ms",
                            op.operator, op.actual_rows, op.time_ms,
                        )?;
                    }
                }
            }
        }
        Ok(())
    }
}
