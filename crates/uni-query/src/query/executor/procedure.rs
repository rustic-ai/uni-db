// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

use super::core::*;
use anyhow::{Result, anyhow};
use std::collections::HashMap;
use uni_common::Value;
use uni_cypher::ast::Expr;
use uni_store::QueryContext;
use uni_store::runtime::property_manager::PropertyManager;

fn success_result(success: bool) -> Result<Vec<HashMap<String, Value>>> {
    Ok(vec![HashMap::from([(
        "success".to_string(),
        Value::Bool(success),
    )])])
}

/// Value type for procedure parameters and outputs.
#[derive(Debug, Clone, PartialEq)]
pub enum ProcedureValueType {
    /// Cypher STRING type.
    String,
    /// Cypher INTEGER type.
    Integer,
    /// Cypher FLOAT type.
    Float,
    /// Cypher NUMBER type (accepts both INTEGER and FLOAT).
    Number,
    /// Cypher BOOLEAN type.
    Boolean,
    /// Accepts any value type.
    Any,
}

/// Single parameter declaration for a registered procedure.
#[derive(Debug, Clone)]
pub struct ProcedureParam {
    /// Parameter name as declared in the procedure signature.
    pub name: String,
    /// Expected type for this parameter.
    pub param_type: ProcedureValueType,
}

/// Single output column declaration for a registered procedure.
#[derive(Debug, Clone)]
pub struct ProcedureOutput {
    /// Output column name as declared in the procedure signature.
    pub name: String,
    /// Type of the output column.
    pub output_type: ProcedureValueType,
}

/// A procedure registered at runtime with static mock data.
///
/// Used by the TCK harness to define test procedures that the query
/// engine can call via `CALL proc.name(args) YIELD columns`.
#[derive(Debug, Clone)]
pub struct RegisteredProcedure {
    /// Fully qualified procedure name (e.g. `test.my.proc`).
    pub name: String,
    /// Declared input parameters.
    pub params: Vec<ProcedureParam>,
    /// Declared output columns.
    pub outputs: Vec<ProcedureOutput>,
    /// Mock data rows keyed by column name.
    pub data: Vec<HashMap<String, Value>>,
}

/// Thread-safe registry of test procedures.
///
/// Procedures are registered before query execution (typically by TCK
/// step definitions) and looked up by the executor at runtime.
#[derive(Debug, Default)]
pub struct ProcedureRegistry {
    procedures: std::sync::RwLock<HashMap<String, RegisteredProcedure>>,
}

impl ProcedureRegistry {
    /// Creates an empty registry.
    pub fn new() -> Self {
        Self::default()
    }

    /// Registers a procedure, replacing any existing one with the same name.
    pub fn register(&self, proc_def: RegisteredProcedure) {
        self.procedures
            .write()
            .expect("ProcedureRegistry lock poisoned")
            .insert(proc_def.name.clone(), proc_def);
    }

    /// Looks up a procedure by fully qualified name.
    pub fn get(&self, name: &str) -> Option<RegisteredProcedure> {
        self.procedures
            .read()
            .expect("ProcedureRegistry lock poisoned")
            .get(name)
            .cloned()
    }

    /// Removes all registered procedures.
    pub fn clear(&self) {
        self.procedures
            .write()
            .expect("ProcedureRegistry lock poisoned")
            .clear();
    }
}

/// Filters a full result map to only the requested yield items.
/// If `yield_items` is empty, returns the full result unchanged.
fn filter_yield_items(
    full_result: HashMap<String, Value>,
    yield_items: &[String],
) -> HashMap<String, Value> {
    if yield_items.is_empty() {
        return full_result;
    }
    yield_items
        .iter()
        .filter_map(|name| full_result.get(name).map(|val| (name.clone(), val.clone())))
        .collect()
}

impl Executor {
    /// Evaluate a procedure argument as a string, returning an error with the given description.
    async fn eval_string_arg<'a>(
        &'a self,
        arg: &Expr,
        description: &str,
        prop_manager: &'a PropertyManager,
        params: &'a HashMap<String, Value>,
        ctx: Option<&'a QueryContext>,
    ) -> Result<String> {
        let empty_row = HashMap::new();
        self.evaluate_expr(arg, &empty_row, prop_manager, params, ctx)
            .await?
            .as_str()
            .ok_or_else(|| anyhow!("{} must be string", description))
            .map(|s| s.to_string())
    }

    pub(crate) async fn execute_procedure<'a>(
        &'a self,
        name: &str,
        args: &[Expr],
        yield_items: &[String],
        prop_manager: &'a PropertyManager,
        params: &'a HashMap<String, Value>,
        ctx: Option<&'a QueryContext>,
    ) -> Result<Vec<HashMap<String, Value>>> {
        match name {
            "uni.admin.compact" => {
                let stats = self.storage.compact().await?;
                let full_result = HashMap::from([
                    (
                        "files_compacted".to_string(),
                        Value::Int(stats.files_compacted as i64),
                    ),
                    (
                        "bytes_before".to_string(),
                        Value::Int(stats.bytes_before as i64),
                    ),
                    (
                        "bytes_after".to_string(),
                        Value::Int(stats.bytes_after as i64),
                    ),
                    (
                        "duration_ms".to_string(),
                        Value::Int(stats.duration.as_millis() as i64),
                    ),
                ]);

                Ok(vec![filter_yield_items(full_result, yield_items)])
            }
            "uni.admin.compactionStatus" => {
                let status = self.storage.compaction_status();
                let full_result = HashMap::from([
                    ("l1_runs".to_string(), Value::Int(status.l1_runs as i64)),
                    (
                        "l1_size_bytes".to_string(),
                        Value::Int(status.l1_size_bytes as i64),
                    ),
                    (
                        "in_progress".to_string(),
                        Value::Bool(status.compaction_in_progress),
                    ),
                    (
                        "pending".to_string(),
                        Value::Int(status.compaction_pending as i64),
                    ),
                    (
                        "total_compactions".to_string(),
                        Value::Int(status.total_compactions as i64),
                    ),
                    (
                        "total_bytes_compacted".to_string(),
                        Value::Int(status.total_bytes_compacted as i64),
                    ),
                ]);

                Ok(vec![filter_yield_items(full_result, yield_items)])
            }
            "uni.admin.snapshot.create" => {
                let name = if !args.is_empty() {
                    Some(
                        self.eval_string_arg(&args[0], "Snapshot name", prop_manager, params, ctx)
                            .await?,
                    )
                } else {
                    None
                };

                let writer_arc = self
                    .writer
                    .as_ref()
                    .ok_or_else(|| anyhow!("Database is in read-only mode"))?;
                let mut writer = writer_arc.write().await;
                let snapshot_id = writer.flush_to_l1(name).await?;

                Ok(vec![HashMap::from([(
                    "snapshot_id".to_string(),
                    Value::String(snapshot_id),
                )])])
            }
            "uni.admin.snapshot.list" => {
                let sm = self.storage.snapshot_manager();
                let ids = sm.list_snapshots().await?;
                let mut results = Vec::new();
                for id in ids {
                    if let Ok(m) = sm.load_snapshot(&id).await {
                        results.push(HashMap::from([
                            ("snapshot_id".to_string(), Value::String(m.snapshot_id)),
                            (
                                "name".to_string(),
                                m.name.map(Value::String).unwrap_or(Value::Null),
                            ),
                            (
                                "created_at".to_string(),
                                Value::String(m.created_at.to_rfc3339()),
                            ),
                            (
                                "version_hwm".to_string(),
                                Value::Int(m.version_high_water_mark as i64),
                            ),
                        ]));
                    }
                }
                Ok(results)
            }
            "uni.admin.snapshot.restore" => {
                let id = self
                    .eval_string_arg(&args[0], "Snapshot ID", prop_manager, params, ctx)
                    .await?;

                self.storage
                    .snapshot_manager()
                    .set_latest_snapshot(&id)
                    .await?;
                Ok(vec![HashMap::from([(
                    "status".to_string(),
                    Value::String("Restored".to_string()),
                )])])
            }
            // DDL Procedures
            "uni.schema.createLabel" => {
                let empty_row = HashMap::new();
                let name = self
                    .eval_string_arg(&args[0], "Label name", prop_manager, params, ctx)
                    .await?;
                let config = self
                    .evaluate_expr(&args[1], &empty_row, prop_manager, params, ctx)
                    .await?;

                let success =
                    super::ddl_procedures::create_label(&self.storage, &name, &config).await?;
                success_result(success)
            }
            "uni.schema.createEdgeType" => {
                let empty_row = HashMap::new();
                let name = self
                    .eval_string_arg(&args[0], "Edge type name", prop_manager, params, ctx)
                    .await?;
                let src_val = self
                    .evaluate_expr(&args[1], &empty_row, prop_manager, params, ctx)
                    .await?;
                let dst_val = self
                    .evaluate_expr(&args[2], &empty_row, prop_manager, params, ctx)
                    .await?;
                let config = self
                    .evaluate_expr(&args[3], &empty_row, prop_manager, params, ctx)
                    .await?;

                // Convert src/dst to Vec<String>
                let src_labels = src_val
                    .as_array()
                    .ok_or(anyhow!("Source labels must be a list"))?
                    .iter()
                    .map(|v| {
                        v.as_str()
                            .map(|s| s.to_string())
                            .ok_or(anyhow!("Label must be string"))
                    })
                    .collect::<Result<Vec<_>>>()?;
                let dst_labels = dst_val
                    .as_array()
                    .ok_or(anyhow!("Target labels must be a list"))?
                    .iter()
                    .map(|v| {
                        v.as_str()
                            .map(|s| s.to_string())
                            .ok_or(anyhow!("Label must be string"))
                    })
                    .collect::<Result<Vec<_>>>()?;

                let success = super::ddl_procedures::create_edge_type(
                    &self.storage,
                    &name,
                    src_labels,
                    dst_labels,
                    &config,
                )
                .await?;
                success_result(success)
            }
            "uni.schema.createIndex" => {
                let empty_row = HashMap::new();
                let label = self
                    .eval_string_arg(&args[0], "Label", prop_manager, params, ctx)
                    .await?;
                let property = self
                    .eval_string_arg(&args[1], "Property", prop_manager, params, ctx)
                    .await?;
                let config = self
                    .evaluate_expr(&args[2], &empty_row, prop_manager, params, ctx)
                    .await?;

                let success =
                    super::ddl_procedures::create_index(&self.storage, &label, &property, &config)
                        .await?;
                success_result(success)
            }
            "uni.schema.createConstraint" => {
                let label = self
                    .eval_string_arg(&args[0], "Label", prop_manager, params, ctx)
                    .await?;
                let c_type = self
                    .eval_string_arg(&args[1], "Constraint type", prop_manager, params, ctx)
                    .await?;
                let empty_row = HashMap::new();
                let props_val = self
                    .evaluate_expr(&args[2], &empty_row, prop_manager, params, ctx)
                    .await?;

                let properties = props_val
                    .as_array()
                    .ok_or(anyhow!("Properties must be a list"))?
                    .iter()
                    .map(|v| {
                        v.as_str()
                            .map(|s| s.to_string())
                            .ok_or(anyhow!("Property must be string"))
                    })
                    .collect::<Result<Vec<_>>>()?;

                let success = super::ddl_procedures::create_constraint(
                    &self.storage,
                    &label,
                    &c_type,
                    properties,
                )
                .await?;
                success_result(success)
            }
            "uni.schema.dropLabel" => {
                let name = self
                    .eval_string_arg(&args[0], "Label name", prop_manager, params, ctx)
                    .await?;
                let success = super::ddl_procedures::drop_label(&self.storage, &name).await?;
                success_result(success)
            }
            "uni.schema.dropEdgeType" => {
                let name = self
                    .eval_string_arg(&args[0], "Edge type name", prop_manager, params, ctx)
                    .await?;
                let success = super::ddl_procedures::drop_edge_type(&self.storage, &name).await?;
                success_result(success)
            }
            "uni.schema.dropIndex" => {
                let name = self
                    .eval_string_arg(&args[0], "Index name", prop_manager, params, ctx)
                    .await?;
                let success = super::ddl_procedures::drop_index(&self.storage, &name).await?;
                success_result(success)
            }
            "uni.schema.dropConstraint" => {
                let name = self
                    .eval_string_arg(&args[0], "Constraint name", prop_manager, params, ctx)
                    .await?;
                let success = super::ddl_procedures::drop_constraint(&self.storage, &name).await?;
                success_result(success)
            }
            _ => {
                // Check external procedure registry
                if let Some(registry) = &self.procedure_registry
                    && let Some(proc_def) = registry.get(name)
                {
                    return self
                        .execute_registered_procedure(
                            &proc_def,
                            args,
                            yield_items,
                            prop_manager,
                            params,
                            ctx,
                        )
                        .await;
                }
                Err(anyhow!("ProcedureNotFound: Unknown procedure '{}'", name))
            }
        }
    }

    /// Executes a procedure from the external registry.
    ///
    /// Evaluates arguments, validates count and types against the procedure
    /// declaration, filters data rows by matching input columns, and projects
    /// the requested output columns.
    ///
    /// # Errors
    ///
    /// Returns `InvalidNumberOfArguments` if the argument count is wrong,
    /// or `InvalidArgumentType` if an argument has an incompatible type.
    async fn execute_registered_procedure<'a>(
        &'a self,
        proc_def: &RegisteredProcedure,
        args: &[Expr],
        yield_items: &[String],
        prop_manager: &'a PropertyManager,
        params: &'a HashMap<String, Value>,
        ctx: Option<&'a QueryContext>,
    ) -> Result<Vec<HashMap<String, Value>>> {
        let empty_row = HashMap::new();

        // Evaluate arguments
        let mut evaluated_args = Vec::with_capacity(args.len());
        for arg in args {
            evaluated_args.push(
                self.evaluate_expr(arg, &empty_row, prop_manager, params, ctx)
                    .await?,
            );
        }

        // Validate argument count
        if evaluated_args.len() != proc_def.params.len() {
            if evaluated_args.is_empty() && !proc_def.params.is_empty() {
                // Standalone CALL with no YIELD is treated as missing implicit parameter(s).
                if yield_items.is_empty() {
                    return Err(anyhow!(
                        "MissingParameter: Procedure '{}' requires {} implicit argument(s)",
                        proc_def.name,
                        proc_def.params.len()
                    ));
                }
                // In-query CALL with YIELD cannot pass implicit arguments.
                return Err(anyhow!(
                    "InvalidArgumentPassingMode: Procedure '{}' requires explicit argument passing in in-query CALL",
                    proc_def.name
                ));
            }
            return Err(anyhow!(
                "InvalidNumberOfArguments: Procedure '{}' expects {} argument(s), got {}",
                proc_def.name,
                proc_def.params.len(),
                evaluated_args.len()
            ));
        }

        // Validate argument types
        for (i, (arg_val, param)) in evaluated_args.iter().zip(&proc_def.params).enumerate() {
            if !arg_val.is_null() && !check_type_compatible(arg_val, &param.param_type) {
                return Err(anyhow!(
                    "InvalidArgumentType: Argument {} ('{}') of procedure '{}' has incompatible type",
                    i,
                    param.name,
                    proc_def.name
                ));
            }
        }

        // Filter data rows: keep rows where input columns match the provided args
        let filtered: Vec<&HashMap<String, Value>> = proc_def
            .data
            .iter()
            .filter(|row| {
                for (param, arg_val) in proc_def.params.iter().zip(&evaluated_args) {
                    if let Some(row_val) = row.get(&param.name)
                        && !values_match(row_val, arg_val)
                    {
                        return false;
                    }
                }
                true
            })
            .collect();

        // Collect output column names
        let output_names: Vec<&str> = proc_def.outputs.iter().map(|o| o.name.as_str()).collect();

        // Project output columns, applying yield_items filtering
        let results = filtered
            .into_iter()
            .map(|row| {
                let mut result = HashMap::new();
                if yield_items.is_empty() {
                    // Return all output columns
                    for name in &output_names {
                        if let Some(val) = row.get(*name) {
                            result.insert((*name).to_string(), val.clone());
                        }
                    }
                } else {
                    for yield_name in yield_items {
                        if let Some(val) = row.get(yield_name.as_str()) {
                            result.insert(yield_name.clone(), val.clone());
                        }
                    }
                }
                result
            })
            .collect();

        Ok(results)
    }
}

/// Checks whether a value is compatible with a procedure type.
fn check_type_compatible(val: &Value, expected: &ProcedureValueType) -> bool {
    match expected {
        ProcedureValueType::Any => true,
        ProcedureValueType::String => val.is_string(),
        ProcedureValueType::Boolean => val.is_bool(),
        ProcedureValueType::Integer => val.is_i64(),
        ProcedureValueType::Float => val.is_f64() || val.is_i64(),
        ProcedureValueType::Number => val.is_number(),
    }
}

/// Checks whether two values match for input-column filtering.
fn values_match(row_val: &Value, arg_val: &Value) -> bool {
    if arg_val.is_null() || row_val.is_null() {
        return arg_val.is_null() && row_val.is_null();
    }
    // Compare numbers by f64 to handle int/float cross-comparison
    if let (Some(a), Some(b)) = (row_val.as_f64(), arg_val.as_f64()) {
        return (a - b).abs() < f64::EPSILON;
    }
    row_val == arg_val
}
