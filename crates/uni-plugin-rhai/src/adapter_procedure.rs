//! Procedure adapter — Rhai-side procedure returning a stream of yield rows.
//!
//! A Rhai procedure exports a single function `${name}` returning a
//! `rhai::Array` of `rhai::Map`s (rows). Each map's keys correspond to
//! the yield-schema field names declared in the manifest. The adapter
//! converts the returned array into a `RecordBatch` matching the yield
//! schema, then wraps it in a `SendableRecordBatchStream` for the host
//! to attach to the surrounding query plan.

#![cfg(feature = "rhai-runtime")]

// Rust guideline compliant

use std::sync::Arc;

use arrow_array::{ArrayRef, RecordBatch};
use arrow_schema::{DataType, Schema, SchemaRef};
use datafusion::execution::SendableRecordBatchStream;
use datafusion::logical_expr::ColumnarValue;
use rhai::{Dynamic, Map, Scope};
use smol_str::SmolStr;

use uni_plugin::adapter_common::batch_builder::batch_into_stream;
use uni_plugin::errors::FnError;
use uni_plugin::traits::procedure::{ProcedureContext, ProcedurePlugin, ProcedureSignature};

use crate::dynamic_bridge::{OutBuilder, scalar_to_dynamic};
use crate::runtime::RhaiPluginRuntime;

/// Per-procedure Rhai callable adapter.
#[derive(Debug)]
pub struct RhaiProcedure {
    runtime: Arc<RhaiPluginRuntime>,
    name: SmolStr,
    signature: ProcedureSignature,
}

impl RhaiProcedure {
    /// Construct a procedure adapter binding `name` against the shared
    /// runtime.
    #[must_use]
    pub fn new(
        runtime: Arc<RhaiPluginRuntime>,
        name: impl Into<SmolStr>,
        signature: ProcedureSignature,
    ) -> Self {
        Self {
            runtime,
            name: name.into(),
            signature,
        }
    }
}

impl ProcedurePlugin for RhaiProcedure {
    fn signature(&self) -> &ProcedureSignature {
        &self.signature
    }

    fn invoke(
        &self,
        _ctx: ProcedureContext<'_>,
        args: &[ColumnarValue],
    ) -> Result<SendableRecordBatchStream, FnError> {
        // Convert each ColumnarValue::Scalar arg to a single Dynamic.
        // Array args are unsupported for procedure invocation in v1 —
        // procedures take scalar inputs, not batched columns.
        let mut dyn_args: Vec<Dynamic> = Vec::with_capacity(args.len());
        for (i, arg) in args.iter().enumerate() {
            match arg {
                ColumnarValue::Scalar(s) => {
                    let d = scalar_to_dynamic(s)
                        .map_err(|e| FnError::new(0x12, format!("procedure arg {i}: {e}")))?;
                    dyn_args.push(d);
                }
                ColumnarValue::Array(_) => {
                    return Err(FnError::new(
                        0x10,
                        format!("procedure arg {i} must be a scalar"),
                    ));
                }
            }
        }

        // Call the Rhai fn; expect an Array of Maps (rows).
        let mut scope = Scope::new();
        let result: Dynamic = self
            .runtime
            .engine
            .call_fn(&mut scope, &self.runtime.ast, self.name.as_str(), dyn_args)
            .map_err(|e| FnError::new(0x730, format!("Rhai procedure `{}`: {e}", self.name)))?;

        let yield_schema = Arc::new(Schema::new(self.signature.yields.clone()));
        let batch = dynamic_to_record_batch(result, &yield_schema)?;
        Ok(batch_into_stream(batch))
    }
}

fn dynamic_to_record_batch(d: Dynamic, schema: &SchemaRef) -> Result<RecordBatch, FnError> {
    let rows: rhai::Array = d.try_cast().ok_or_else(|| {
        FnError::new(
            0x12,
            String::from("Rhai procedure must return an array of row maps"),
        )
    })?;
    let row_count = rows.len();

    // Pre-build one builder per yield field.
    let mut builders: Vec<OutBuilder> = schema
        .fields()
        .iter()
        .map(|f| OutBuilder::new(f.data_type(), row_count))
        .collect::<Result<_, _>>()
        .map_err(|e| FnError::new(0x11, e.to_string()))?;

    for (i, row) in rows.into_iter().enumerate() {
        let m: Map = row
            .try_cast()
            .ok_or_else(|| FnError::new(0x12, format!("procedure row {i} must be a map")))?;
        for (field_idx, field) in schema.fields().iter().enumerate() {
            let key = field.name();
            // #233 Tier 1: a key absent from the row map became UNIT, so a
            // misspelled or forgotten yield key produced an entire all-NULL
            // column rather than reporting that the row did not carry it.
            let Some(value) = m.get(key.as_str()).cloned() else {
                return Err(FnError::new(
                    0x13,
                    format!("procedure row {i} does not carry the declared yield key `{key}`"),
                ));
            };
            // Coerce numeric types — Rhai often returns INT for fields
            // declared as Float (and vice versa for cross-int sizes).
            let value = coerce_for(field.data_type(), value)?;
            builders[field_idx]
                .push(value)
                .map_err(|e| FnError::new(0x14, e.to_string()))?;
        }
    }

    let columns: Vec<ArrayRef> = builders.into_iter().map(|b| b.finish()).collect();
    RecordBatch::try_new(schema.clone(), columns)
        .map_err(|e| FnError::new(0x15, format!("procedure batch: {e}")))
}

fn coerce_for(target: &DataType, value: Dynamic) -> Result<Dynamic, FnError> {
    if value.is_unit() {
        return Ok(value);
    }
    match target {
        // Rhai often returns INT for a Float-declared field (and vice versa
        // for Int-declared fields); coerce the mismatched numeric type and
        // pass everything else through unchanged.
        DataType::Float64 => Ok(value.as_int().map_or(value, |i| Dynamic::from(i as f64))),
        DataType::Int64 => Ok(match value.as_float() {
            // Coerce a finite, in-range float (truncating). NaN/±inf/out-of-range
            // must NOT silently become 0 or saturate via `as i64`; map them to
            // NULL so the Int64 column gets a null rather than corrupt data.
            Ok(f) if f.is_finite() && f >= i64::MIN as f64 && f <= i64::MAX as f64 => {
                Dynamic::from(f as i64)
            }
            Ok(_) => Dynamic::UNIT,
            // Not a float (e.g. already an Int) — pass through unchanged.
            Err(_) => value,
        }),
        _ => Ok(value),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::build_engine;
    use crate::host_fns::RhaiHostFnRegistry;
    use crate::manifest::compile;
    use arrow_schema::Field;
    use futures::StreamExt;
    use uni_plugin::capability::SideEffects;
    use uni_plugin::traits::procedure::ProcedureMode;
    use uni_plugin::{CapabilitySet, PluginId};

    fn build_runtime(script: &str) -> Arc<RhaiPluginRuntime> {
        let engine = build_engine(&CapabilitySet::new(), &RhaiHostFnRegistry::new());
        let ast = compile(&engine, script).unwrap();
        RhaiPluginRuntime::new(PluginId::new("test.proc"), engine, ast)
    }

    #[tokio::test]
    async fn procedure_emits_rows() {
        let script = r#"
            fn rows() {
                [
                    #{ id: 1, name: "alice" },
                    #{ id: 2, name: "bob" },
                    #{ id: 3, name: "carol" },
                ]
            }
        "#;
        let runtime = build_runtime(script);
        let sig = ProcedureSignature {
            args: vec![],
            yields: vec![
                Field::new("id", DataType::Int64, true),
                Field::new("name", DataType::Utf8, true),
            ],
            mode: ProcedureMode::Read,
            side_effects: SideEffects::ReadOnly,
            retry_contract: None,
            batch_input: None,
            docs: String::new(),
        };
        let proc = RhaiProcedure::new(runtime, "rows", sig);
        let mut stream = proc.invoke(ProcedureContext::new(), &[]).unwrap();
        let batch = stream.next().await.unwrap().unwrap();
        assert_eq!(batch.num_rows(), 3);
        assert_eq!(batch.num_columns(), 2);
    }

    #[test]
    fn coerce_int64_guards_nonfinite_and_out_of_range_floats() {
        // A finite, in-range float coerces (truncating) to Int64.
        let ok = coerce_for(&DataType::Int64, Dynamic::from(3.9_f64)).unwrap();
        assert_eq!(ok.as_int().unwrap(), 3);

        // NaN must NOT become 0 — it maps to UNIT (NULL).
        let nan = coerce_for(&DataType::Int64, Dynamic::from(f64::NAN)).unwrap();
        assert!(nan.is_unit(), "NaN must map to NULL, not coerce to 0");

        // +inf / out-of-range must NOT saturate to i64::MAX — also NULL.
        let inf = coerce_for(&DataType::Int64, Dynamic::from(f64::INFINITY)).unwrap();
        assert!(inf.is_unit(), "inf must map to NULL, not saturate");
    }
}
