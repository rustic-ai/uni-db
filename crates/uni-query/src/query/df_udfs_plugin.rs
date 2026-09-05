// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Plugin-registry scalar-UDF integration for DataFusion.
//!
//! This module bridges the `uni-plugin` plugin system into DataFusion's
//! scalar-UDF surface. It lives in `uni-query` (not the dependency-light
//! `uni-query-functions` leaf crate) because it depends on `uni-plugin`
//! and `tokio` task-locals.
//!
//! Responsibilities:
//!
//! - The `SESSION_PLUGIN_REGISTRY` tokio task-local and its scope/read
//!   helpers ([`scoped_with_session_plugin_registry`],
//!   [`current_session_plugin_registry`]).
//! - Re-export of the principal task-local helpers from
//!   `uni_plugin::host::principal` so callers keep their existing
//!   `uni_query::scoped_with_principal` / `current_principal` paths.
//! - [`scoped_with_session_context`] combining both scopes.
//! - [`register_plugin_scalar_udfs`] / [`register_plugin_scalar_udfs_pair`]
//!   and the `PluginScalarUdf` DataFusion adapter (private).

use std::any::Any;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use arrow::datatypes::DataType;
use datafusion::error::Result as DFResult;
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, TypeSignature,
};
use datafusion::prelude::SessionContext;
use uni_query_functions::custom_functions::{CustomFunctionRegistry, LEGACY_USER_PLUGIN_ID};

use crate::query::executor::plugin_adapter::ValueRowFn;

tokio::task_local! {
    /// Tokio task-local carrying the current `Session`'s
    /// **session-local** plugin registry across the per-query
    /// executor scope. Set by host-crate session execute paths via
    /// [`scoped_with_session_plugin_registry`]; read at the UDF
    /// registration site (`register_plugin_scalar_udfs_pair`) and at
    /// the procedure / Locy-aggregate dual-consult helpers.
    ///
    /// Propagates across `.await` points within the same task tree;
    /// does NOT propagate across `tokio::spawn` (which is fine — the
    /// per-query executor runs everything in the same task).
    pub static SESSION_PLUGIN_REGISTRY:
        std::sync::Arc<uni_plugin::PluginRegistry>;
}

/// Run `fut` inside a scope that exposes `registry` as the current
/// session-local plugin registry. Returns the future's output.
///
/// Use this at every uni-db host-crate boundary where a `Session`
/// dispatches into the executor.
pub fn scoped_with_session_plugin_registry<F: std::future::Future>(
    registry: std::sync::Arc<uni_plugin::PluginRegistry>,
    fut: F,
) -> tokio::task::futures::TaskLocalFuture<std::sync::Arc<uni_plugin::PluginRegistry>, F> {
    SESSION_PLUGIN_REGISTRY.scope(registry, fut)
}

/// Borrow the current session-local plugin registry, if any. Returns
/// `None` when the call is not inside a
/// [`scoped_with_session_plugin_registry`] scope (e.g., a query
/// against `Uni` directly with no Session in flight, or a unit test
/// invoking the executor outside the host crate).
#[must_use]
pub fn current_session_plugin_registry() -> Option<std::sync::Arc<uni_plugin::PluginRegistry>> {
    SESSION_PLUGIN_REGISTRY.try_with(|r| r.clone()).ok()
}

// §1.2 / Phase 5: the principal task-local + scope helpers moved to
// `uni_plugin::host::principal`. Re-exported here so external callers
// (`uni::api::{session,transaction}`, downstream embedders) keep their
// existing `uni_query::scoped_with_principal` / `current_principal`
// paths.
pub use uni_plugin::host::principal::{
    CURRENT_PRINCIPAL, current_principal, maybe_scope_with_principal, scoped_with_principal,
};

/// Run `fut` inside both [`scoped_with_session_plugin_registry`] and
/// the principal task-local scope in a single call.
///
/// `principal` is optional — when `None`, only the plugin-registry
/// scope is installed and [`current_principal`] returns `None` inside
/// `fut`. This matches the legacy behavior for sessions that haven't
/// authenticated.
pub async fn scoped_with_session_context<F: std::future::Future>(
    registry: std::sync::Arc<uni_plugin::PluginRegistry>,
    principal: Option<std::sync::Arc<uni_plugin::traits::connector::Principal>>,
    fut: F,
) -> F::Output {
    scoped_with_session_plugin_registry(registry, maybe_scope_with_principal(principal, fut)).await
}

/// Two-registry variant of [`register_plugin_scalar_udfs`] — registers
/// the instance registry's scalars first, then the session registry's
/// (if present) on top. DataFusion's `register_udf` is last-write-wins
/// by registered name, so session entries shadow instance entries
/// without any explicit ordering logic.
///
/// This is the resolution path used per-query when a `Session` carries
/// a session-local plugin registry. See `proposal §5.4.2` for the
/// session-scope contract and the M8.6 follow-up plan.
///
/// # Errors
///
/// Returns an error if any UDF registration fails.
pub fn register_plugin_scalar_udfs_pair(
    ctx: &SessionContext,
    instance: &uni_plugin::PluginRegistry,
    session: Option<&uni_plugin::PluginRegistry>,
) -> DFResult<()> {
    register_plugin_scalar_udfs(ctx, instance)?;
    if let Some(session_reg) = session {
        register_plugin_scalar_udfs(ctx, session_reg)?;
    }
    Ok(())
}

/// Register every scalar function in a `PluginRegistry` as a DataFusion UDF.
///
/// M2's plugin-path DataFusion adapter — iterates
/// [`uni_plugin::PluginRegistry`] directly.
///
/// Registers each scalar as both lowercase and uppercase local-name
/// variants so Cypher's case-insensitive function-name match resolves.
/// The qname's namespace is preserved (Cypher syntax uses dotted names for
/// qualified callable references).
///
/// # Errors
///
/// Returns an error if any UDF registration fails.
pub fn register_plugin_scalar_udfs(
    ctx: &SessionContext,
    plugin_registry: &uni_plugin::PluginRegistry,
) -> DFResult<()> {
    for (qname, entry) in plugin_registry.iter_scalars() {
        let local = qname.local();
        let lower_local = local.to_lowercase();
        let upper_local = local.to_uppercase();

        // Local-name registrations — what Cypher's case-insensitive
        // lookup hits.
        if lower_local != upper_local {
            ctx.register_udf(ScalarUDF::new_from_impl(PluginScalarUdf::new(
                lower_local.clone(),
                Arc::clone(&entry),
            )));
        }
        ctx.register_udf(ScalarUDF::new_from_impl(PluginScalarUdf::new(
            upper_local,
            Arc::clone(&entry),
        )));

        // Also register under the fully-qualified name (`namespace.local`)
        // so dotted-name dispatch works.
        ctx.register_udf(ScalarUDF::new_from_impl(PluginScalarUdf::new(
            qname.to_string(),
            Arc::clone(&entry),
        )));
    }

    // Locy filter predicates are exposed as boolean-returning scalar UDFs so a
    // plugin predicate is callable from a rule-body `WHERE` (the columnar
    // DataFusion filter path). The in-memory Locy eval paths dispatch the same
    // predicate through `locy_eval::eval_function`; both must resolve or they
    // diverge. Registered under the same case-folds + dotted name as scalars.
    for (qname, entry) in plugin_registry.iter_locy_predicates() {
        let local = qname.local();
        let lower_local = local.to_lowercase();
        let upper_local = local.to_uppercase();

        if lower_local != upper_local {
            ctx.register_udf(ScalarUDF::new_from_impl(PluginPredicateUdf::new(
                lower_local.clone(),
                Arc::clone(&entry),
            )));
        }
        ctx.register_udf(ScalarUDF::new_from_impl(PluginPredicateUdf::new(
            upper_local,
            Arc::clone(&entry),
        )));
        ctx.register_udf(ScalarUDF::new_from_impl(PluginPredicateUdf::new(
            qname.to_string(),
            Arc::clone(&entry),
        )));
    }
    Ok(())
}

/// DataFusion adapter exposing a registered [`uni_plugin::traits::locy::LocyPredicate`]
/// as a boolean-returning scalar UDF for the columnar rule-body `WHERE` path.
struct PluginPredicateUdf {
    name: String,
    entry: Arc<uni_plugin::registry::LocyPredicateEntry>,
    signature: Signature,
}

impl PluginPredicateUdf {
    fn new(name: String, entry: Arc<uni_plugin::registry::LocyPredicateEntry>) -> Self {
        let volatility = entry.signature.volatility;
        Self {
            signature: Signature::new(TypeSignature::VariadicAny, volatility),
            name,
            entry,
        }
    }
}

impl std::fmt::Debug for PluginPredicateUdf {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PluginPredicateUdf")
            .field("name", &self.name)
            .finish()
    }
}

impl PartialEq for PluginPredicateUdf {
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name && self.signature == other.signature
    }
}

impl Eq for PluginPredicateUdf {}

impl Hash for PluginPredicateUdf {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.name.hash(state);
    }
}

impl ScalarUDFImpl for PluginPredicateUdf {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
    fn name(&self) -> &str {
        &self.name
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _arg_types: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Boolean)
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let rows = args.number_rows;
        let arr = self
            .entry
            .predicate
            .evaluate(&args.args, rows)
            .map_err(|e| {
                datafusion::error::DataFusionError::Execution(format!(
                    "plugin predicate `{}` failed: {e}",
                    self.name
                ))
            })?;
        Ok(ColumnarValue::Array(Arc::new(arr)))
    }
}

/// Build a shadow [`uni_plugin::PluginRegistry`] from the pure
/// [`CustomFunctionRegistry`] leaf type.
///
/// `CustomFunctionRegistry` lives in the dependency-light
/// `uni-query-functions` crate and stores only `(name, fn)` pairs. The
/// plugin-framework dispatch path (`register_plugin_scalar_udfs`) consumes a
/// `PluginRegistry`, so we mirror every legacy registration into one under
/// the reserved [`LEGACY_USER_PLUGIN_ID`] here, where the `uni-plugin`
/// dependency is available.
///
/// Each entry is wrapped in a [`ValueRowFn`] adapter and given a permissive
/// `CypherValue` signature — the actual coercion happens at the DataFusion
/// adapter ([`PluginScalarUdf`]) site.
fn plugin_registry_for_custom_functions(
    registry: &CustomFunctionRegistry,
) -> uni_plugin::PluginRegistry {
    use datafusion::logical_expr::Volatility;
    use uni_plugin::traits::scalar::{ArgType, FnSignature, NullHandling};
    use uni_plugin::{Capability, CapabilitySet, PluginId, PluginRegistrar, PluginRegistry, QName};

    let pr = PluginRegistry::new();
    let plugin_id = PluginId::new(LEGACY_USER_PLUGIN_ID);
    let caps = CapabilitySet::from_iter_of([Capability::ScalarFn]);

    for (name, func) in registry.iter() {
        // `CustomFunctionRegistry` already uppercases names on registration,
        // but normalize defensively so the qname matches the plugin id's
        // namespace expectations.
        let upper = name.to_uppercase();
        let mut r = PluginRegistrar::new(plugin_id.clone(), &caps, &pr);
        let qname = QName::new(LEGACY_USER_PLUGIN_ID, &upper);
        let adapter = Arc::new(ValueRowFn::new(upper.clone(), Arc::clone(func)));
        let sig = FnSignature {
            args: vec![ArgType::Variadic(Box::new(ArgType::CypherValue))],
            returns: ArgType::CypherValue,
            volatility: Volatility::Volatile,
            null_handling: NullHandling::UserHandled,
        };
        if let Err(e) = r.scalar_fn(qname, sig, adapter) {
            tracing::warn!(error = ?e, fn_name = %upper, "shadow registration failed");
            continue;
        }
        if let Err(e) = r.commit_to_registry() {
            tracing::warn!(error = ?e, fn_name = %upper, "shadow commit failed");
        }
    }
    pr
}

/// Register the legacy [`CustomFunctionRegistry`] entries as DataFusion
/// scalar UDFs by mirroring them through the plugin-framework adapter.
///
/// This is the instance-scope, legacy `CustomFunctionRegistry` shadow path
/// (e.g. `db.register_function()` entries + apoc-core mirrors).
///
/// # Errors
///
/// Returns an error if any UDF registration fails.
pub fn register_custom_functions_as_plugin_scalars(
    ctx: &SessionContext,
    registry: &CustomFunctionRegistry,
) -> DFResult<()> {
    let shadow = plugin_registry_for_custom_functions(registry);
    register_plugin_scalar_udfs(ctx, &shadow)
}

/// DataFusion adapter wrapping a [`uni_plugin::registry::ScalarEntry`].
///
/// Inspects the plugin's `signature.returns` at construction time to pick
/// the DataFusion return type:
///
/// - `ArgType::Primitive(T)` → declares `T` directly to DataFusion. The
///   plugin's `invoke()` returns Arrow data in `T`'s native type, no
///   LargeBinary round-trip. This is the ≥ 20% perf target path for
///   primitively-typed UDFs.
/// - `ArgType::CypherValue` → declares `LargeBinary` (legacy transport).
/// - `ArgType::Vector { .. }` / `Variadic(..)` → `LargeBinary` for now.
///
/// The same adapter is used for both the local-name and qualified-name
/// registrations.
struct PluginScalarUdf {
    name: String,
    entry: Arc<uni_plugin::registry::ScalarEntry>,
    signature: Signature,
    return_type: DataType,
}

impl PluginScalarUdf {
    fn new(name: String, entry: Arc<uni_plugin::registry::ScalarEntry>) -> Self {
        // Derive volatility from the plugin's declared volatility, falling
        // back to Volatile if signature inspection fails. (The plugin's
        // FnSignature is the canonical source of truth.)
        let volatility = entry.signature.volatility;
        let return_type = derive_return_type(&entry);
        Self {
            signature: Signature::new(TypeSignature::VariadicAny, volatility),
            name,
            entry,
            return_type,
        }
    }
}

/// Derive the DataFusion return type from the plugin's declared signature.
fn derive_return_type(entry: &uni_plugin::registry::ScalarEntry) -> DataType {
    use uni_plugin::traits::scalar::ArgType;
    match &entry.signature.returns {
        ArgType::Primitive(t) => t.clone(),
        // CypherValue + Vector + Variadic stay on the LargeBinary path
        // (the latter two are uncommon for return types; CypherValue is
        // explicit opt-in to the legacy transport).
        _ => DataType::LargeBinary,
    }
}

impl std::fmt::Debug for PluginScalarUdf {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PluginScalarUdf")
            .field("name", &self.name)
            .finish()
    }
}

/// Resolve the declared [`ArgType`] for positional argument `i`, transparently
/// unwrapping a trailing `Variadic(..)` so arguments beyond the fixed prefix
/// inherit the variadic element type.
fn declared_arg_type(
    args: &[uni_plugin::traits::scalar::ArgType],
    i: usize,
) -> Option<&uni_plugin::traits::scalar::ArgType> {
    use uni_plugin::traits::scalar::ArgType;
    let raw = match args.get(i) {
        Some(a) => Some(a),
        // Past the fixed args: only a trailing variadic keeps matching.
        None => match args.last() {
            Some(v @ ArgType::Variadic(_)) => Some(v),
            _ => None,
        },
    };
    match raw {
        Some(ArgType::Variadic(inner)) => Some(inner.as_ref()),
        other => other,
    }
}

/// Coerce one plugin-scalar argument column from the schemaless `LargeBinary`
/// CypherValue transport into the primitive Arrow type the manifest declares
/// (`Int64`/`Float64`) for that argument.
///
/// A raw integer/float node or edge property reaches expression evaluation as a
/// `LargeBinary` variant column (schemaless storage). A plugin scalar that
/// declares `Primitive(Int64)` downcasts its argument to `Int64Array` and fails
/// on `LargeBinary` — previously forcing callers to wrap the property in
/// `toInteger(...)`/`toFloat(...)`. This performs that coercion automatically
/// (REQ-4). A value that genuinely cannot be coerced (e.g. a string where an
/// integer is declared) yields a precise error naming the argument, the declared
/// type, and the explicit-coercion hint, instead of an opaque downcast failure.
///
/// Columns whose declared type is not a numeric primitive, or that already
/// arrive as a non-`LargeBinary` (i.e. natively typed) array, pass through
/// untouched.
fn coerce_plugin_scalar_arg(
    col: ColumnarValue,
    declared: Option<&uni_plugin::traits::scalar::ArgType>,
    rows: usize,
    arg_idx: usize,
    fn_name: &str,
) -> DFResult<ColumnarValue> {
    use arrow::array::{Array, ArrayRef, Float64Array, Int64Array, LargeBinaryArray};
    use uni_common::Value;
    use uni_plugin::traits::scalar::ArgType;

    let target = match declared {
        Some(ArgType::Primitive(t @ (DataType::Int64 | DataType::Float64))) => t.clone(),
        _ => return Ok(col),
    };

    let array = col.to_array(rows)?;
    // Already natively typed (or some other non-variant transport): nothing to do.
    if array.data_type() != &DataType::LargeBinary {
        return Ok(col);
    }
    let lb = array
        .as_any()
        .downcast_ref::<LargeBinaryArray>()
        .expect("data_type checked to be LargeBinary");

    let non_numeric_err = |row: usize, got: &Value| {
        let hint = if target == DataType::Int64 {
            "toInteger(...)"
        } else {
            "toFloat(...)"
        };
        datafusion::error::DataFusionError::Execution(format!(
            "plugin fn `{fn_name}`: argument {} declares {target} but row {row} carried a \
             non-numeric value ({got:?}); wrap the property with {hint}",
            arg_idx + 1,
        ))
    };

    let decoded = |row: usize| {
        uni_store::storage::arrow_convert::arrow_to_value(array.as_ref(), row, None)
            .canonical_entity()
    };

    let out: ArrayRef = match target {
        DataType::Int64 => {
            let mut b = Int64Array::builder(array.len());
            for row in 0..array.len() {
                if lb.is_null(row) || lb.value(row).is_empty() {
                    b.append_null();
                    continue;
                }
                match decoded(row) {
                    Value::Int(i) => b.append_value(i),
                    Value::Float(f) => b.append_value(f as i64),
                    Value::Null => b.append_null(),
                    other => return Err(non_numeric_err(row, &other)),
                }
            }
            Arc::new(b.finish())
        }
        DataType::Float64 => {
            let mut b = Float64Array::builder(array.len());
            for row in 0..array.len() {
                if lb.is_null(row) || lb.value(row).is_empty() {
                    b.append_null();
                    continue;
                }
                match decoded(row) {
                    Value::Float(f) => b.append_value(f),
                    Value::Int(i) => b.append_value(i as f64),
                    Value::Null => b.append_null(),
                    other => return Err(non_numeric_err(row, &other)),
                }
            }
            Arc::new(b.finish())
        }
        _ => unreachable!("target restricted to Int64/Float64 above"),
    };
    Ok(ColumnarValue::Array(out))
}

impl PartialEq for PluginScalarUdf {
    fn eq(&self, other: &Self) -> bool {
        self.signature == other.signature
    }
}

impl Eq for PluginScalarUdf {}

impl Hash for PluginScalarUdf {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.name().hash(state);
    }
}

impl ScalarUDFImpl for PluginScalarUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DFResult<DataType> {
        Ok(self.return_type.clone())
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let entry = Arc::clone(&self.entry);
        let rows = args.number_rows;
        // Auto-coerce raw schemaless (LargeBinary) numeric property args to the
        // primitive type the plugin's manifest declares, so a property can be
        // passed without an explicit toInteger()/toFloat() wrapper (REQ-4).
        let declared = &entry.signature.args;
        let cols = args
            .args
            .into_iter()
            .enumerate()
            .map(|(i, col)| {
                coerce_plugin_scalar_arg(col, declared_arg_type(declared, i), rows, i, &self.name)
            })
            .collect::<DFResult<Vec<_>>>()?;
        entry.function.invoke(&cols, rows).map_err(|e| {
            datafusion::error::DataFusionError::Execution(format!(
                "plugin `{}` fn `{}` failed: {e}",
                entry.plugin, self.name
            ))
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    // `SessionContext::udf` is a `FunctionRegistry` trait method; bring the
    // trait into scope so the registration assertions resolve. `Volatility`
    // is used only by the in-test plugin fixtures.
    use datafusion::execution::FunctionRegistry;
    use datafusion::logical_expr::Volatility;

    #[test]
    fn test_register_plugin_scalars_routes_through_plugin_registry() {
        // M2 facade test: register a scalar via the legacy
        // CustomFunctionRegistry, then verify that calling
        // `register_plugin_scalar_udfs` against its `plugin_registry()`
        // exposes the same fn through DataFusion under both case-folds and
        // the fully-qualified namespace.
        use uni_common::Value;
        use uni_query_functions::custom_functions::{CustomFunctionRegistry, CustomScalarFn};

        let mut reg = CustomFunctionRegistry::new();
        let f: CustomScalarFn =
            Arc::new(|_args: &[Value]| Ok(Value::String("plugin-path".to_owned())));
        reg.register("MYFN".to_owned(), f);

        let ctx = SessionContext::new();
        register_custom_functions_as_plugin_scalars(&ctx, &reg).unwrap();

        // Local-name lowercase form (Cypher case-insensitive dispatch).
        assert!(ctx.udf("myfn").is_ok());
        // Uppercase local name.
        assert!(ctx.udf("MYFN").is_ok());
        // Fully-qualified namespace form.
        let qname = format!("{LEGACY_USER_PLUGIN_ID}.MYFN");
        assert!(ctx.udf(&qname).is_ok());
    }

    #[test]
    fn test_native_arrow_udf_declares_primitive_return_type() {
        // M2 fast path: a plugin declaring `ArgType::Primitive(Float64)` as
        // its return type should produce a DataFusion UDF whose
        // `return_type` is `Float64`, not `LargeBinary`. This eliminates
        // the per-row LargeBinary round-trip.
        use std::sync::OnceLock;
        use uni_plugin::FnError;
        use uni_plugin::traits::scalar::{ArgType, FnSignature, NullHandling, ScalarPluginFn};
        use uni_plugin::{
            Capability, CapabilitySet, PluginId, PluginRegistrar, PluginRegistry, QName,
        };

        struct DoubleIt;
        impl ScalarPluginFn for DoubleIt {
            fn signature(&self) -> &FnSignature {
                static S: OnceLock<FnSignature> = OnceLock::new();
                S.get_or_init(|| FnSignature {
                    args: vec![ArgType::Primitive(DataType::Float64)],
                    returns: ArgType::Primitive(DataType::Float64),
                    volatility: Volatility::Immutable,
                    null_handling: NullHandling::PropagateNulls,
                })
            }
            fn invoke(
                &self,
                args: &[ColumnarValue],
                _rows: usize,
            ) -> Result<ColumnarValue, FnError> {
                Ok(args.first().cloned().unwrap())
            }
        }

        let pr = PluginRegistry::new();
        let caps = CapabilitySet::from_iter_of([Capability::ScalarFn]);
        let mut r = PluginRegistrar::new(PluginId::new("test.fast"), &caps, &pr);
        r.scalar_fn(
            QName::new("test.fast", "double"),
            FnSignature {
                args: vec![ArgType::Primitive(DataType::Float64)],
                returns: ArgType::Primitive(DataType::Float64),
                volatility: Volatility::Immutable,
                null_handling: NullHandling::PropagateNulls,
            },
            Arc::new(DoubleIt),
        )
        .unwrap();
        r.commit_to_registry().unwrap();

        let ctx = SessionContext::new();
        register_plugin_scalar_udfs(&ctx, &pr).unwrap();

        // Resolve the UDF and ask DataFusion for its return type.
        let udf = ctx.udf("double").expect("udf registered");
        let rt = udf.return_type(&[DataType::Float64]).unwrap();
        assert_eq!(
            rt,
            DataType::Float64,
            "primitive-typed plugin should declare Float64 directly, not LargeBinary"
        );
    }
}
