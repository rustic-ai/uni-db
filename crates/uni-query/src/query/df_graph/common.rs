// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team
// Rust guideline compliant

//! Common helpers shared across graph execution plan implementations.
//!
//! This module provides shared utilities to reduce code duplication across
//! the df_graph module's execution plan implementations.

use arrow_schema::{DataType, Field, SchemaRef};
use datafusion::physical_expr::{EquivalenceProperties, Partitioning};
use datafusion::physical_plan::PlanProperties;
use std::sync::Arc;

/// Compute standard plan properties for graph operators.
///
/// All graph operators use the same plan properties:
/// - Unknown partitioning with 1 partition
/// - Incremental emission type
/// - Bounded execution
pub fn compute_plan_properties(schema: SchemaRef) -> PlanProperties {
    PlanProperties::new(
        EquivalenceProperties::new(schema),
        Partitioning::UnknownPartitioning(1),
        datafusion::physical_plan::execution_plan::EmissionType::Incremental,
        datafusion::physical_plan::execution_plan::Boundedness::Bounded,
    )
}

/// Build the standard node struct fields for path structures.
///
/// Used when materializing path objects containing nodes.
/// Fields: `_vid`, `_label`, `properties`
pub fn node_struct_fields() -> arrow_schema::Fields {
    arrow_schema::Fields::from(vec![
        Field::new("_vid", DataType::UInt64, false),
        Field::new("_label", DataType::Utf8, true),
        Field::new("properties", DataType::LargeBinary, true),
    ])
}

/// Build the standard edge struct fields for path structures.
///
/// Used when materializing path objects containing edges.
/// Fields: `_eid`, `_type_name`, `_src`, `_dst`, `properties`
pub fn edge_struct_fields() -> arrow_schema::Fields {
    arrow_schema::Fields::from(vec![
        Field::new("_eid", DataType::UInt64, false),
        Field::new("_type_name", DataType::Utf8, false),
        Field::new("_src", DataType::UInt64, false),
        Field::new("_dst", DataType::UInt64, false),
        Field::new("properties", DataType::LargeBinary, true),
    ])
}

/// Build path struct field for schema with given path variable name.
///
/// Creates a struct field with `nodes` and `relationships` lists.
pub fn build_path_struct_field(path_var: &str) -> Field {
    let node_item = Field::new("item", DataType::Struct(node_struct_fields()), true);
    let nodes_field = Field::new("nodes", DataType::List(Arc::new(node_item)), false);

    let edge_item = Field::new("item", DataType::Struct(edge_struct_fields()), true);
    let relationships_field =
        Field::new("relationships", DataType::List(Arc::new(edge_item)), false);

    Field::new(
        path_var,
        DataType::Struct(arrow_schema::Fields::from(vec![
            nodes_field,
            relationships_field,
        ])),
        false,
    )
}
