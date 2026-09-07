// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Synthetic procedure plugin for declared-procedure body execution.
//!
//! M11 A.3 — the M9 cutover delivery for `declareProcedure` /
//! `declareTrigger`. `uni-plugin-custom::DeclareProcedureProcedure`
//! records a declaration; on `CustomPlugin::reactivate_into_registry`
//! the host's [`CypherProcedureSynthesizer`] is called for each
//! procedure-kind record, returning a [`SyntheticProcedurePlugin`]
//! whose `invoke()` runs the stored Cypher body via the write-enabled
//! `QueryProcedureHost::execute_inner_query`.

// Rust guideline compliant

use std::sync::Arc;

use arrow_array::{Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use datafusion::execution::SendableRecordBatchStream;
use datafusion::logical_expr::ColumnarValue;
use datafusion::scalar::ScalarValue;
use uni_plugin::FnError;
use uni_plugin::adapter_common::batch_builder::batch_into_stream;
use uni_plugin::traits::procedure::{
    ProcedureContext, ProcedureMode, ProcedurePlugin, ProcedureSignature,
};
use uni_plugin_custom::{DeclaredPlugin, ProcedureBodySynthesizer};

/// A procedure whose `invoke()` runs a stored Cypher body through the
/// host's write-enabled `execute_inner_query`.
///
/// Lives in `uni-db` (not `uni-plugin-custom`) because the
/// implementation downcasts the procedure context's host to
/// `uni_query::QueryProcedureHost` to reach the inner-query primitive
/// — `uni-plugin-custom` does not depend on `uni-query`.
#[derive(Debug)]
pub struct SyntheticProcedurePlugin {
    qname: String,
    body: String,
    mode: ProcedureMode,
    param_names: Vec<String>,
    signature: ProcedureSignature,
}

impl SyntheticProcedurePlugin {
    /// Construct from a declared-procedure record.
    ///
    /// Parses the signature's `arg_names` to get param positions and
    /// chooses `ProcedureMode::Read` by default (the proposal's M9
    /// cutover targets read-mode declared procedures first; write
    /// declarations land once the write-path capability gate is
    /// wired through the registrar at declare-time).
    ///
    /// # Errors
    ///
    /// Returns an error string if the declared signature_json is
    /// malformed.
    pub fn from_declaration(decl: &DeclaredPlugin) -> Result<Self, String> {
        let sig_meta: serde_json::Value = serde_json::from_str(&decl.signature_json)
            .map_err(|e| format!("signature_json parse: {e}"))?;
        let param_names: Vec<String> = sig_meta
            .get("arg_names")
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|v| v.as_str().map(str::to_owned))
                    .collect()
            })
            .unwrap_or_default();
        let mode = match sig_meta
            .get("mode")
            .and_then(|v| v.as_str())
            .map(str::to_ascii_uppercase)
            .as_deref()
        {
            Some("WRITE") => ProcedureMode::Write,
            Some("SCHEMA") => ProcedureMode::Schema,
            Some("DBMS") => ProcedureMode::Dbms,
            _ => ProcedureMode::Read,
        };
        let yields = vec![Field::new("row_json", DataType::Utf8, false)];
        let signature = ProcedureSignature {
            args: param_names
                .iter()
                .map(|n| uni_plugin::traits::procedure::NamedArgType {
                    name: smol_str::SmolStr::new(n),
                    ty: uni_plugin::traits::scalar::ArgType::Primitive(DataType::Utf8),
                    default: None,
                    doc: format!("Parameter `{n}`."),
                })
                .collect(),
            yields,
            mode,
            side_effects: match mode {
                ProcedureMode::Read => uni_plugin::SideEffects::ReadOnly,
                _ => uni_plugin::SideEffects::Writes,
            },
            retry_contract: None,
            batch_input: None,
            docs: format!("Declared procedure `{}`.", decl.qname),
        };
        Ok(Self {
            qname: decl.qname.clone(),
            body: decl.body.clone(),
            mode,
            param_names,
            signature,
        })
    }
}

impl ProcedurePlugin for SyntheticProcedurePlugin {
    fn signature(&self) -> &ProcedureSignature {
        &self.signature
    }

    fn invoke(
        &self,
        ctx: ProcedureContext<'_>,
        args: &[ColumnarValue],
    ) -> Result<SendableRecordBatchStream, FnError> {
        let host_any = ctx
            .host
            .ok_or_else(|| {
                FnError::new(
                    0xD00,
                    format!("declared procedure `{}`: missing host context", self.qname),
                )
            })?
            .as_any();
        let host: &uni_query::query::executor::procedure_host::QueryProcedureHost =
            host_any.downcast_ref().ok_or_else(|| {
                FnError::new(
                    0xD01,
                    format!(
                        "declared procedure `{}`: host is not a QueryProcedureHost",
                        self.qname
                    ),
                )
            })?;

        // Build the params map from positional args by name.
        let mut params: std::collections::HashMap<String, uni_common::Value> =
            std::collections::HashMap::new();
        for (i, name) in self.param_names.iter().enumerate() {
            let arg = args.get(i).ok_or_else(|| {
                FnError::new(
                    0xD02,
                    format!(
                        "declared procedure `{}`: missing arg `{name}` at position {i}",
                        self.qname
                    ),
                )
            })?;
            params.insert(name.clone(), columnar_to_value(arg)?);
        }

        let body = self.body.clone();
        let mode = self.mode;
        let host_clone = host.clone();
        let qname = self.qname.clone();

        // Bridge the sync `invoke()` to async `execute_inner_query`
        // via `block_in_place` + `Handle::current().block_on(...)`.
        // Requires a multi-thread tokio runtime — which is `Uni`'s
        // default. (`Uni::build_sync` constructs one explicitly.)
        let rows: Vec<std::collections::HashMap<String, uni_common::Value>> =
            tokio::task::block_in_place(|| {
                tokio::runtime::Handle::current().block_on(async move {
                    host_clone.execute_inner_query(&body, &params, mode).await
                })
            })
            .map_err(|e| {
                FnError::new(
                    0xD03,
                    format!("declared procedure `{qname}` execution: {e}"),
                )
            })?;

        let schema: SchemaRef = Arc::new(Schema::new(vec![Field::new(
            "row_json",
            DataType::Utf8,
            false,
        )]));
        // #233 Tier 1: `unwrap_or_else(|_| "{}")` emitted a literal empty
        // JSON object as a REAL row, indistinguishable from a procedure that
        // genuinely yielded an empty object.
        let row_jsons: Vec<String> = rows
            .iter()
            .map(|r| {
                serde_json::to_string(r).map_err(|e| {
                    FnError::new(
                        0xD05,
                        format!("declared procedure `{}`: encode row: {e}", self.qname),
                    )
                })
            })
            .collect::<Result<Vec<String>, FnError>>()?;
        let arr = Arc::new(StringArray::from(row_jsons)) as Arc<dyn Array>;
        let batch = RecordBatch::try_new(schema, vec![arr]).map_err(|e| {
            FnError::new(
                0xD04,
                format!("declared procedure `{}`: build batch: {e}", self.qname),
            )
        })?;
        Ok(batch_into_stream(batch))
    }
}

fn columnar_to_value(arg: &ColumnarValue) -> Result<uni_common::Value, FnError> {
    match arg {
        ColumnarValue::Scalar(ScalarValue::Utf8(Some(s))) => {
            Ok(uni_common::Value::String(s.clone()))
        }
        ColumnarValue::Scalar(ScalarValue::Int64(Some(n))) => Ok(uni_common::Value::Int(*n)),
        ColumnarValue::Scalar(ScalarValue::Float64(Some(f))) => Ok(uni_common::Value::Float(*f)),
        ColumnarValue::Scalar(ScalarValue::Boolean(Some(b))) => Ok(uni_common::Value::Bool(*b)),
        ColumnarValue::Array(arr) => {
            if let Some(s) = arr.as_any().downcast_ref::<StringArray>() {
                if s.is_empty() || s.is_null(0) {
                    Ok(uni_common::Value::Null)
                } else {
                    Ok(uni_common::Value::String(s.value(0).to_owned()))
                }
            } else {
                Err(FnError::new(
                    FnError::CODE_TYPE_COERCION,
                    "declared procedure: unsupported array arg type",
                ))
            }
        }
        _ => Err(FnError::new(
            FnError::CODE_TYPE_COERCION,
            "declared procedure: unsupported arg type",
        )),
    }
}

/// Host-side [`ProcedureBodySynthesizer`] implementation.
///
/// Constructs a [`SyntheticProcedurePlugin`] from each declared
/// procedure record. Installed on the host's [`uni_plugin_custom::CustomPlugin`]
/// via [`uni_plugin_custom::CustomPlugin::with_procedure_synthesizer`]
/// during `Uni::build`.
#[derive(Debug, Default)]
pub struct CypherProcedureSynthesizer;

impl CypherProcedureSynthesizer {
    /// Construct.
    #[must_use]
    pub fn new() -> Self {
        Self
    }
}

impl ProcedureBodySynthesizer for CypherProcedureSynthesizer {
    fn synthesize(&self, decl: &DeclaredPlugin) -> Result<Arc<dyn ProcedurePlugin>, String> {
        let plugin = SyntheticProcedurePlugin::from_declaration(decl)?;
        Ok(Arc::new(plugin))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fixture_decl() -> DeclaredPlugin {
        DeclaredPlugin {
            qname: "mycorp.findFriends".to_owned(),
            kind: "procedure".to_owned(),
            body: "MATCH (p:Person {name: $name})-[:KNOWS]->(f) RETURN f.name AS friend".to_owned(),
            signature_json: serde_json::json!({
                "arg_names": ["name"],
                "mode": "READ",
                "return_type": "string",
            })
            .to_string(),
            dependencies: vec![],
            declared_by: "alice".to_owned(),
            active: true,
        }
    }

    #[test]
    fn from_declaration_parses_arg_names_and_mode() {
        let p = SyntheticProcedurePlugin::from_declaration(&fixture_decl()).expect("synthesize");
        assert_eq!(p.param_names, vec!["name".to_owned()]);
        assert_eq!(p.mode, ProcedureMode::Read);
        assert_eq!(p.signature.args.len(), 1);
    }

    #[test]
    fn from_declaration_defaults_to_read_when_mode_missing() {
        let mut decl = fixture_decl();
        decl.signature_json = "{}".to_owned();
        let p = SyntheticProcedurePlugin::from_declaration(&decl).expect("synthesize");
        assert_eq!(p.mode, ProcedureMode::Read);
        assert!(p.param_names.is_empty());
    }

    #[test]
    fn from_declaration_recognizes_write_mode() {
        let mut decl = fixture_decl();
        decl.signature_json = serde_json::json!({
            "arg_names": ["n"],
            "mode": "WRITE",
        })
        .to_string();
        let p = SyntheticProcedurePlugin::from_declaration(&decl).expect("synthesize");
        assert_eq!(p.mode, ProcedureMode::Write);
    }

    #[test]
    fn from_declaration_errors_on_bad_signature_json() {
        let mut decl = fixture_decl();
        decl.signature_json = "not-json".to_owned();
        let result = SyntheticProcedurePlugin::from_declaration(&decl);
        assert!(result.is_err());
    }

    #[test]
    fn synthesizer_round_trips() {
        let synth = CypherProcedureSynthesizer::new();
        let plugin = synth.synthesize(&fixture_decl()).expect("synthesize");
        assert_eq!(plugin.signature().mode, ProcedureMode::Read);
    }
}
