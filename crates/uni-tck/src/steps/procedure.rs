// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! TCK step definitions for `CALL` procedure scenarios.
//!
//! Handles the `"there exists a procedure ..."` and `"parameters are:"`
//! Gherkin steps by parsing procedure signatures, registering mock data
//! in the `ProcedureRegistry`, and storing query parameters in `UniWorld`.

use crate::parser::parse_value;
use crate::UniWorld;
use cucumber::given;
use std::collections::HashMap;
use uni_query::{ProcedureOutput, ProcedureParam, ProcedureValueType, RegisteredProcedure};

/// Parses a type annotation like `STRING?` into a `ProcedureValueType`.
fn parse_type(type_str: &str) -> ProcedureValueType {
    let base = type_str.trim().trim_end_matches('?').trim();
    match base.to_uppercase().as_str() {
        "STRING" => ProcedureValueType::String,
        "INTEGER" => ProcedureValueType::Integer,
        "FLOAT" => ProcedureValueType::Float,
        "NUMBER" => ProcedureValueType::Number,
        "BOOLEAN" | "BOOL" => ProcedureValueType::Boolean,
        _ => ProcedureValueType::Any,
    }
}

/// Parses a comma-separated list of `name :: TYPE?` declarations.
fn parse_param_list(s: &str) -> Vec<(String, ProcedureValueType)> {
    let trimmed = s.trim();
    if trimmed.is_empty() {
        return Vec::new();
    }
    trimmed
        .split(',')
        .map(|part| {
            let parts: Vec<&str> = part.split("::").collect();
            let name = parts[0].trim().to_string();
            let ptype = if parts.len() > 1 {
                parse_type(parts[1])
            } else {
                ProcedureValueType::Any
            };
            (name, ptype)
        })
        .collect()
}

/// Parses a full procedure signature string.
///
/// Accepts signatures like:
/// - `test.doNothing() :: ()`
/// - `test.labels() :: (label :: STRING?)`
/// - `test.my.proc(name :: STRING?, id :: INTEGER?) :: (city :: STRING?, code :: INTEGER?)`
///
/// Returns `(procedure_name, params, outputs)`.
fn parse_procedure_signature(
    sig: &str,
) -> Result<(String, Vec<ProcedureParam>, Vec<ProcedureOutput>), String> {
    let sig = sig.trim().trim_end_matches(':').trim();

    // Find the first '(' to split procedure name from the rest
    let open_paren = sig
        .find('(')
        .ok_or_else(|| format!("No '(' in signature: {sig}"))?;

    let proc_name = sig[..open_paren].trim().to_string();
    let rest = &sig[open_paren..];

    // Split on ") :: (" to separate input params from output params
    // Handle both ") :: (" and ")::(" patterns
    let separator_patterns = [") :: (", ")::(", ") ::(", "):: ("];
    let mut split_pos = None;
    let mut sep_len = 0;

    for pattern in &separator_patterns {
        if let Some(pos) = rest.find(pattern) {
            split_pos = Some(pos);
            sep_len = pattern.len();
            break;
        }
    }

    let (input_str, output_str) = if let Some(pos) = split_pos {
        // Extract content between parentheses
        let input_part = &rest[1..pos]; // skip opening '('
        let output_part = &rest[pos + sep_len..];
        let output_part = output_part
            .trim()
            .trim_end_matches(')')
            .trim_end_matches(':');
        (input_part.trim(), output_part.trim())
    } else {
        // No separator found, try to parse as just params with empty output
        let inner = rest
            .trim_start_matches('(')
            .trim_end_matches(')')
            .trim_end_matches(':');
        (inner.trim(), "")
    };

    let params: Vec<ProcedureParam> = parse_param_list(input_str)
        .into_iter()
        .map(|(name, param_type)| ProcedureParam { name, param_type })
        .collect();

    let outputs: Vec<ProcedureOutput> = parse_param_list(output_str)
        .into_iter()
        .map(|(name, output_type)| ProcedureOutput { name, output_type })
        .collect();

    Ok((proc_name, params, outputs))
}

#[given(regex = r"^there exists a procedure (.+)$")]
async fn there_exists_a_procedure(
    world: &mut UniWorld,
    step: &cucumber::gherkin::Step,
    signature: String,
) {
    let (name, params, outputs) =
        parse_procedure_signature(&signature).expect("Failed to parse procedure signature");

    // Parse data table if present
    let data = if let Some(table) = step.table() {
        let rows = &table.rows;
        if rows.is_empty() || (rows.len() == 1 && rows[0].iter().all(|c| c.trim().is_empty())) {
            // Empty table (just `|` or `| |`)
            Vec::new()
        } else {
            // First row is headers
            let headers: Vec<String> = rows[0].iter().map(|h| h.trim().to_string()).collect();

            rows[1..]
                .iter()
                .map(|row| {
                    let mut map = HashMap::new();
                    for (header, cell) in headers.iter().zip(row.iter()) {
                        let val = parse_value(cell.trim()).unwrap_or(uni_query::Value::Null);
                        map.insert(header.clone(), val);
                    }
                    map
                })
                .collect()
        }
    } else {
        Vec::new()
    };

    world
        .db()
        .procedure_registry()
        .register(RegisteredProcedure {
            name,
            params,
            outputs,
            data,
        });
}

#[given("parameters are:")]
async fn parameters_are(world: &mut UniWorld, step: &cucumber::gherkin::Step) {
    if let Some(table) = step.table() {
        // Each row is a key-value pair: | key | value |
        for row in &table.rows {
            if row.len() >= 2 {
                let key = row[0].trim().to_string();
                let val = parse_value(row[1].trim()).unwrap_or(uni_query::Value::Null);
                world.add_param(key, val);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_no_args_no_outputs() {
        let (name, params, outputs) = parse_procedure_signature("test.doNothing() :: ():").unwrap();
        assert_eq!(name, "test.doNothing");
        assert!(params.is_empty());
        assert!(outputs.is_empty());
    }

    #[test]
    fn test_parse_no_args_with_output() {
        let (name, params, outputs) =
            parse_procedure_signature("test.labels() :: (label :: STRING?):").unwrap();
        assert_eq!(name, "test.labels");
        assert!(params.is_empty());
        assert_eq!(outputs.len(), 1);
        assert_eq!(outputs[0].name, "label");
        assert_eq!(outputs[0].output_type, ProcedureValueType::String);
    }

    #[test]
    fn test_parse_with_args_and_outputs() {
        let (name, params, outputs) = parse_procedure_signature(
            "test.my.proc(name :: STRING?, id :: INTEGER?) :: (city :: STRING?, code :: INTEGER?):",
        )
        .unwrap();
        assert_eq!(name, "test.my.proc");
        assert_eq!(params.len(), 2);
        assert_eq!(params[0].name, "name");
        assert_eq!(params[0].param_type, ProcedureValueType::String);
        assert_eq!(params[1].name, "id");
        assert_eq!(params[1].param_type, ProcedureValueType::Integer);
        assert_eq!(outputs.len(), 2);
        assert_eq!(outputs[0].name, "city");
        assert_eq!(outputs[1].name, "code");
    }

    #[test]
    fn test_parse_type() {
        assert_eq!(parse_type("STRING?"), ProcedureValueType::String);
        assert_eq!(parse_type("INTEGER?"), ProcedureValueType::Integer);
        assert_eq!(parse_type("FLOAT?"), ProcedureValueType::Float);
        assert_eq!(parse_type("NUMBER?"), ProcedureValueType::Number);
        assert_eq!(parse_type("BOOLEAN?"), ProcedureValueType::Boolean);
        assert_eq!(parse_type("MAP?"), ProcedureValueType::Any);
    }

    #[test]
    fn test_parse_with_space_variations() {
        // The signature from Call5.feature has a trailing space before ":"
        let (name, params, outputs) = parse_procedure_signature(
            "test.my.proc(in :: INTEGER?) :: (a :: INTEGER?, b :: INTEGER?) :",
        )
        .unwrap();
        assert_eq!(name, "test.my.proc");
        assert_eq!(params.len(), 1);
        assert_eq!(params[0].name, "in");
        assert_eq!(outputs.len(), 2);
        assert_eq!(outputs[0].name, "a");
        assert_eq!(outputs[1].name, "b");
    }
}
