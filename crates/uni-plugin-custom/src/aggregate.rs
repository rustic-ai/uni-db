// Rust guideline compliant
//! `DeclaredAggregateFn` — an [`AggregatePluginFn`] that evaluates three
//! parsed Cypher expression bodies (init / update / finalize) over the
//! [`crate::eval::eval_expr`] interpreter.
//!
//! Structurally mirrors [`crate::scalar::DeclaredScalarFn`]: the
//! `uni.plugin.declareAggregate` procedure parses each body once at
//! declare time, wraps the result in [`DeclaredAggregateFn`], and
//! registers a synthetic [`uni_plugin::Plugin`] through
//! [`install_aggregate_into_registry`] under a per-namespace plugin id.
//!
//! # State model
//!
//! Each per-group accumulator (returned by
//! [`AggregatePluginFn::create_accumulator`]) carries a single
//! [`uni_common::Value`] in `state`. The `state` is bound under the
//! `$state` parameter when evaluating `update_expr` (per row) and
//! `finalize_expr` (once at group end). `init_expr` runs once with no
//! bindings on first row (or on `evaluate` for empty groups).
//!
//! # Partial aggregation
//!
//! M9 declared aggregates ship without distributed-aggregation support:
//! `AggSignature.state_fields` is empty, `supports_partial = false`,
//! and `merge_batch` errors out. Encoding `uni_common::Value` into a
//! transport-stable Arrow representation is a separate lane.

use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::{Array, ArrayRef};
use arrow_schema::DataType;
use datafusion::logical_expr::Volatility;
use datafusion::scalar::ScalarValue;
use semver::Version;
use uni_common::Value;
use uni_cypher::ast::Expr;
use uni_cypher::parse_expression;
use uni_plugin::traits::aggregate::{AggSignature, AggregatePluginFn, PluginAccumulator};
use uni_plugin::traits::scalar::ArgType;
use uni_plugin::{
    AbiRange, Capability, CapabilitySet, Determinism, FnError, Plugin, PluginError, PluginId,
    PluginManifest, PluginRegistrar, PluginRegistry, ProvidedSurfaces, QName, Scope, SideEffects,
};

use crate::decode::{
    array_value_at, declared_plugin_id, eval_err_to_fn, local_part, map_plugin_error, stringify,
    type_str_to_arrow,
};
use crate::eval::eval_expr;
use crate::{CustomError, DeclaredPlugin};

/// Parameter name under which the accumulator's running state is bound
/// when evaluating `update_expr` / `finalize_expr`.
const STATE_PARAM: &str = "state";

/// A Cypher-declared aggregate function.
///
/// Holds three pre-parsed [`Expr`] bodies (`init`, `update`,
/// `finalize`), the positional argument names of the `update` body,
/// the declared return type, and a precomputed [`AggSignature`].
pub struct DeclaredAggregateFn {
    init_expr: Arc<Expr>,
    update_expr: Arc<Expr>,
    finalize_expr: Arc<Expr>,
    arg_names: Vec<String>,
    return_dt: DataType,
    signature: AggSignature,
}

impl std::fmt::Debug for DeclaredAggregateFn {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DeclaredAggregateFn")
            .field("arg_names", &self.arg_names)
            .field("return_type", &self.return_dt)
            .finish_non_exhaustive()
    }
}

impl DeclaredAggregateFn {
    /// Construct a declared aggregate from pre-parsed Cypher bodies.
    #[must_use]
    pub fn new(
        init_expr: Expr,
        update_expr: Expr,
        finalize_expr: Expr,
        arg_names: Vec<String>,
        return_dt: DataType,
    ) -> Self {
        let signature = Self::build_signature(return_dt.clone(), &arg_names);
        Self {
            init_expr: Arc::new(init_expr),
            update_expr: Arc::new(update_expr),
            finalize_expr: Arc::new(finalize_expr),
            arg_names,
            return_dt,
            signature,
        }
    }

    /// Build a default [`AggSignature`] for a declared aggregate.
    ///
    /// All `update` args are declared `Utf8` (the M9 declared-scalar
    /// convention — promotions happen at row-decode time). The returned
    /// signature disables partial aggregation: `state_fields` is empty
    /// and `supports_partial = false`.
    #[must_use]
    pub fn build_signature(returns: DataType, arg_names: &[String]) -> AggSignature {
        AggSignature {
            args: arg_names
                .iter()
                .map(|_| ArgType::Primitive(DataType::Utf8))
                .collect(),
            returns: ArgType::Primitive(returns),
            state_fields: Vec::new(),
            volatility: Volatility::Volatile,
            supports_partial: false,
        }
    }

    /// The configured return [`DataType`].
    #[must_use]
    pub fn return_dt(&self) -> &DataType {
        &self.return_dt
    }
}

impl AggregatePluginFn for DeclaredAggregateFn {
    fn signature(&self) -> &AggSignature {
        &self.signature
    }

    fn create_accumulator(&self) -> Box<dyn PluginAccumulator> {
        Box::new(DeclaredAccumulator {
            init_expr: Arc::clone(&self.init_expr),
            update_expr: Arc::clone(&self.update_expr),
            finalize_expr: Arc::clone(&self.finalize_expr),
            arg_names: self.arg_names.clone(),
            return_dt: self.return_dt.clone(),
            state: None,
        })
    }
}

/// Per-group accumulator backed by the [`crate::eval`] interpreter.
#[derive(Debug)]
struct DeclaredAccumulator {
    init_expr: Arc<Expr>,
    update_expr: Arc<Expr>,
    finalize_expr: Arc<Expr>,
    arg_names: Vec<String>,
    return_dt: DataType,
    state: Option<Value>,
}

impl DeclaredAccumulator {
    /// Run `init_expr` if state hasn't been initialized yet.
    fn ensure_state(&mut self) -> Result<(), FnError> {
        if self.state.is_none() {
            let bindings: HashMap<String, Value> = HashMap::new();
            let v = eval_expr(&self.init_expr, &bindings).map_err(eval_err_to_fn)?;
            self.state = Some(v);
        }
        Ok(())
    }
}

impl PluginAccumulator for DeclaredAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<(), FnError> {
        if values.len() != self.arg_names.len() {
            return Err(FnError::new(
                FnError::CODE_TYPE_COERCION,
                format!(
                    "declared aggregate expected {} args, got {}",
                    self.arg_names.len(),
                    values.len()
                ),
            ));
        }
        self.ensure_state()?;
        let rows = values.first().map_or(0, |a| a.len());
        for row in 0..rows {
            let mut bindings: HashMap<String, Value> = HashMap::with_capacity(values.len() + 1);
            // `clone()` is unavoidable here — `eval_expr` takes a HashMap
            // by reference and we replace `state` after each row.
            let st = self.state.clone().unwrap_or(Value::Null);
            bindings.insert(STATE_PARAM.to_owned(), st);
            for (i, col) in values.iter().enumerate() {
                bindings.insert(self.arg_names[i].clone(), array_value_at(col, row)?);
            }
            let next = eval_expr(&self.update_expr, &bindings).map_err(eval_err_to_fn)?;
            self.state = Some(next);
        }
        Ok(())
    }

    fn merge_batch(&mut self, _states: &[ArrayRef]) -> Result<(), FnError> {
        Err(FnError::new(
            FnError::CODE_TYPE_COERCION,
            "declared aggregates do not support partial / distributed aggregation".to_owned(),
        ))
    }

    fn state(&self) -> Result<Vec<ScalarValue>, FnError> {
        // Empty state matches `AggSignature.state_fields == vec![]`.
        Ok(Vec::new())
    }

    fn evaluate(&self) -> Result<ScalarValue, FnError> {
        // For empty groups, `update_batch` was never called; evaluate
        // `init_expr` on the fly so `finalize_expr` still sees a state.
        let st = match &self.state {
            Some(v) => v.clone(),
            None => eval_expr(&self.init_expr, &HashMap::new()).map_err(eval_err_to_fn)?,
        };
        let mut bindings: HashMap<String, Value> = HashMap::with_capacity(1);
        bindings.insert(STATE_PARAM.to_owned(), st);
        let out = eval_expr(&self.finalize_expr, &bindings).map_err(eval_err_to_fn)?;
        value_to_scalar(&out, &self.return_dt)
    }

    fn size(&self) -> usize {
        std::mem::size_of::<Self>()
    }
}

/// Convert a [`uni_common::Value`] to a [`ScalarValue`] of the requested
/// Arrow type.
///
/// # Errors
///
/// Returns [`FnError`] when the value cannot be coerced to `target`.
pub(crate) fn value_to_scalar(v: &Value, target: &DataType) -> Result<ScalarValue, FnError> {
    match (target, v) {
        (DataType::Utf8, Value::Null) => Ok(ScalarValue::Utf8(None)),
        (DataType::Int64, Value::Null) => Ok(ScalarValue::Int64(None)),
        (DataType::Float64, Value::Null) => Ok(ScalarValue::Float64(None)),
        (DataType::Boolean, Value::Null) => Ok(ScalarValue::Boolean(None)),
        (DataType::Utf8, Value::String(s)) => Ok(ScalarValue::Utf8(Some(s.clone()))),
        (DataType::Utf8, other) => Ok(ScalarValue::Utf8(Some(stringify(other)))),
        (DataType::Int64, Value::Int(i)) => Ok(ScalarValue::Int64(Some(*i))),
        #[expect(
            clippy::cast_possible_truncation,
            reason = "explicit narrowing on user request"
        )]
        (DataType::Int64, Value::Float(f)) => Ok(ScalarValue::Int64(Some(*f as i64))),
        (DataType::Int64, Value::Bool(b)) => Ok(ScalarValue::Int64(Some(i64::from(*b)))),
        (DataType::Float64, Value::Float(f)) => Ok(ScalarValue::Float64(Some(*f))),
        #[expect(
            clippy::cast_precision_loss,
            reason = "i64→f64 widening at user request"
        )]
        (DataType::Float64, Value::Int(i)) => Ok(ScalarValue::Float64(Some(*i as f64))),
        (DataType::Boolean, Value::Bool(b)) => Ok(ScalarValue::Boolean(Some(*b))),
        (dt, other) => Err(FnError::new(
            FnError::CODE_TYPE_COERCION,
            format!("declared aggregate cannot coerce {other:?} to {dt:?}"),
        )),
    }
}

// ---------------------------------------------------------------
// Synthesis / registry installation
// ---------------------------------------------------------------

/// Compile a declared-aggregate record into a [`DeclaredAggregateFn`]
/// and register it into `registry` under a synthetic plugin id derived
/// from the qname's namespace.
///
/// `record.signature_json` must contain `{init, update, finalize,
/// return_type, arg_names}` keys (as encoded by
/// `DeclareAggregateProcedure::invoke`).
///
/// # Errors
///
/// * [`CustomError::BodyParse`] — `signature_json` is malformed or any
///   of the three Cypher bodies fails to parse.
/// * [`CustomError::NativeShadow`] — the qname is already registered as
///   a native aggregate in `registry`.
/// * [`CustomError::Registration`] — other registrar failures.
pub fn install_aggregate_into_registry(
    registry: &Arc<PluginRegistry>,
    record: &DeclaredPlugin,
) -> Result<(), CustomError> {
    let sig_meta: serde_json::Value = serde_json::from_str(&record.signature_json)
        .map_err(|e| CustomError::BodyParse(format!("signature_json: {e}")))?;
    let init_src = sig_meta
        .get("init")
        .and_then(|v| v.as_str())
        .ok_or_else(|| CustomError::BodyParse("declareAggregate: missing `init`".to_owned()))?;
    let update_src = sig_meta
        .get("update")
        .and_then(|v| v.as_str())
        .ok_or_else(|| CustomError::BodyParse("declareAggregate: missing `update`".to_owned()))?;
    let finalize_src = sig_meta
        .get("finalize")
        .and_then(|v| v.as_str())
        .ok_or_else(|| CustomError::BodyParse("declareAggregate: missing `finalize`".to_owned()))?;
    // #233 Tier 1: as in `procedures.rs`, these defaulted to `"float"` and an
    // empty argument list, so a signature that failed to decode came back
    // after a restart as a different aggregate with the same name. The
    // sibling `finalize` lookup above already errors; these were the outliers.
    let return_type_str = sig_meta
        .get("return_type")
        .and_then(|v| v.as_str())
        .ok_or_else(|| {
            CustomError::BodyParse(
                "declareAggregate: missing or non-string `return_type`".to_owned(),
            )
        })?;
    let arg_names: Vec<String> = sig_meta
        .get("arg_names")
        .and_then(|v| v.as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(|v| v.as_str().map(str::to_owned))
                .collect()
        })
        .ok_or_else(|| {
            CustomError::BodyParse("declareAggregate: missing or non-array `arg_names`".to_owned())
        })?;

    let return_dt = type_str_to_arrow(return_type_str).ok_or_else(|| {
        CustomError::BodyParse(format!("unknown return type `{return_type_str}`"))
    })?;

    let init =
        parse_expression(init_src).map_err(|e| CustomError::BodyParse(format!("init: {e:?}")))?;
    let update = parse_expression(update_src)
        .map_err(|e| CustomError::BodyParse(format!("update: {e:?}")))?;
    let finalize = parse_expression(finalize_src)
        .map_err(|e| CustomError::BodyParse(format!("finalize: {e:?}")))?;

    let agg = DeclaredAggregateFn::new(init, update, finalize, arg_names, return_dt);
    let signature = agg.signature().clone();

    let qname = QName::new(
        declared_plugin_id(&record.qname),
        local_part(&record.qname).to_ascii_lowercase(),
    );
    let plugin = SyntheticAggregatePlugin {
        plugin_id: PluginId::new(declared_plugin_id(&record.qname)),
        qname: qname.clone(),
        signature,
        function: Arc::new(agg) as Arc<dyn AggregatePluginFn>,
        manifest: std::sync::OnceLock::new(),
    };
    let manifest = plugin.build_manifest();
    let caps = manifest.capabilities.clone();
    let mut r = PluginRegistrar::new(manifest.id, &caps, registry);
    plugin
        .register(&mut r)
        .map_err(|e| map_plugin_error(e, &record.qname))?;
    r.commit_to_registry()
        .map_err(|e| map_plugin_error(e, &record.qname))?;
    // Publish the qname to the Cypher planner's plugin-aggregate hint
    // set so `RETURN myAgg(x)` routes through aggregate translation
    // instead of scalar UDF resolution.
    uni_cypher::register_plugin_aggregate(format!("{}.{}", qname.namespace(), qname.local()));
    Ok(())
}

/// Synthetic [`Plugin`] wrapping a single declared aggregate.
struct SyntheticAggregatePlugin {
    plugin_id: PluginId,
    qname: QName,
    signature: AggSignature,
    function: Arc<dyn AggregatePluginFn>,
    /// Lazily-built, then cached, manifest. Mirrors
    /// [`super::procedures::SyntheticScalarPlugin`]: each synthetic
    /// plugin has a distinct manifest, so it cannot be a shared static;
    /// the `OnceLock` gives `manifest()` a stable `&` reference without
    /// leaking a fresh `Box` on every call.
    manifest: std::sync::OnceLock<PluginManifest>,
}

impl std::fmt::Debug for SyntheticAggregatePlugin {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SyntheticAggregatePlugin")
            .field("plugin_id", &self.plugin_id)
            .field("qname", &self.qname)
            .finish_non_exhaustive()
    }
}

impl SyntheticAggregatePlugin {
    fn build_manifest(&self) -> PluginManifest {
        PluginManifest {
            id: self.plugin_id.clone(),
            version: Version::new(0, 0, 1),
            abi: AbiRange::parse("^1").expect("manifest ABI range is valid"),
            depends_on: vec![],
            capabilities: CapabilitySet::from_iter_of([Capability::AggregateFn]),
            determinism: Determinism::Pure,
            side_effects: SideEffects::ReadOnly,
            scope: Scope::Instance,
            hash: None,
            signature: None,
            provides: ProvidedSurfaces::default(),
            docs: "Declared aggregate function (apoc.custom analogue).".to_owned(),
            metadata: std::collections::BTreeMap::new(),
        }
    }
}

impl Plugin for SyntheticAggregatePlugin {
    fn manifest(&self) -> &PluginManifest {
        self.manifest.get_or_init(|| self.build_manifest())
    }

    fn register(&self, r: &mut PluginRegistrar<'_>) -> Result<(), PluginError> {
        r.aggregate_fn(
            self.qname.clone(),
            self.signature.clone(),
            Arc::clone(&self.function),
        )?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use arrow_array::Int64Array;

    use super::*;

    fn parse(src: &str) -> Expr {
        parse_expression(src).expect("parse")
    }

    fn build_int_sum_squares() -> DeclaredAggregateFn {
        DeclaredAggregateFn::new(
            parse("0"),
            parse("$state + ($x * $x)"),
            parse("$state"),
            vec!["x".to_owned()],
            DataType::Int64,
        )
    }

    #[test]
    fn accumulator_handles_empty_group() {
        let agg = build_int_sum_squares();
        let acc = agg.create_accumulator();
        let out = acc.evaluate().expect("evaluate");
        assert_eq!(out, ScalarValue::Int64(Some(0)));
    }

    #[test]
    fn accumulator_runs_init_only_once() {
        let agg = build_int_sum_squares();
        let mut acc = agg.create_accumulator();
        let col: ArrayRef = Arc::new(Int64Array::from(vec![1_i64, 2, 3]));
        acc.update_batch(&[col]).expect("update");
        let out = acc.evaluate().expect("evaluate");
        // 1 + 4 + 9 = 14
        assert_eq!(out, ScalarValue::Int64(Some(14)));
    }

    #[test]
    fn merge_batch_is_rejected() {
        let agg = build_int_sum_squares();
        let mut acc = agg.create_accumulator();
        let col: ArrayRef = Arc::new(Int64Array::from(vec![1_i64]));
        let err = acc.merge_batch(&[col]).unwrap_err();
        assert_eq!(err.code, FnError::CODE_TYPE_COERCION);
    }

    #[test]
    fn signature_default_disables_partial() {
        let agg = build_int_sum_squares();
        let sig = agg.signature();
        assert!(!sig.supports_partial);
        assert!(sig.state_fields.is_empty());
    }

    #[test]
    fn value_to_scalar_coerces_int_to_float() {
        let sv = value_to_scalar(&Value::Int(7), &DataType::Float64).unwrap();
        assert_eq!(sv, ScalarValue::Float64(Some(7.0)));
    }
}
