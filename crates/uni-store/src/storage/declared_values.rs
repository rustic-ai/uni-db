// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! The value rule a declared property column applies, for data that predates
//! the declaration.
//!
//! Declaring a property moves the values an entity already holds for it --
//! schemaless data in the main vertex table (a label declared over existing
//! vertices, #304) or in the `overflow_json` blob (a property added to a
//! populated label, #249) -- into a typed column. The column builders turn a
//! value they cannot store into NULL, so a value the declared type does not
//! accept would be lost at declaration time without a word. Such declarations
//! are refused instead ([`check_value`]), and accepted values are first put in
//! the form a write of the declared property would have stored ([`coerce`]).
//!
//! The rule is the write path's own (`uni-query`'s CREATE/SET validation), so
//! declaring a property and then writing a value agrees with writing the value
//! and then declaring the property. `uni-query` sits above this crate, so it
//! installs the rule here with [`install_coercion`]; until it does, a stricter
//! fallback that performs no coercion applies (accept what the column builders
//! store as is, refuse everything else).

use std::sync::OnceLock;

use uni_common::Value;
use uni_common::core::schema::DataType;

/// What kind of entity a declaration gives properties to. Decides where its
/// existing values live before the declaration: a label's in the main vertex
/// table, an edge type's in its delta tables and the main edge table.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeclaredEntity {
    /// A vertex label.
    Label,
    /// A relationship type.
    EdgeType,
}

/// Maps `value`, written to property `property` declared as `declared`, to the
/// form the column stores, or explains why the type cannot hold it.
pub type DeclaredValueCoercion = fn(&str, &DataType, Value) -> anyhow::Result<Value>;

static COERCION: OnceLock<DeclaredValueCoercion> = OnceLock::new();

/// Install the write path's coercion rule. The first installation wins; the
/// rule is a pure function, so every caller installs the same one.
pub fn install_coercion(rule: DeclaredValueCoercion) {
    let _ = COERCION.set(rule);
}

/// `value` in the form a write of `property` declared as `declared` stores.
///
/// # Errors
///
/// Returns the write path's type error when `declared` cannot hold `value`.
pub fn coerce(property: &str, declared: &DataType, value: Value) -> anyhow::Result<Value> {
    let rule = COERCION.get().copied().unwrap_or(storable_as_is);
    rule(property, declared, value)
}

/// Whether `value` may become `property`'s value once declared as `declared`.
#[must_use]
pub fn check_value(property: &str, declared: &DataType, value: &Value) -> bool {
    coerce(property, declared, value.clone()).is_ok()
}

/// The fallback rule: what [`DataType::accepts`] admits and the declared
/// dimensions allow, unchanged.
fn storable_as_is(property: &str, declared: &DataType, value: Value) -> anyhow::Result<Value> {
    if let Err(e) = declared.check_vector_dims(&value) {
        anyhow::bail!("TypeError: property '{property}' is declared {declared:?} but {e}");
    }
    if declared.accepts(&value) {
        Ok(value)
    } else {
        anyhow::bail!(
            "TypeError: property '{property}' is declared {declared:?} but got an \
             incompatible value"
        )
    }
}
