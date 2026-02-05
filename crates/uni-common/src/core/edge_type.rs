// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Edge type ID encoding and utilities for schema/schemaless distinction.

/// Edge type ID stored as `u32` with bit 31 encoding schema provenance:
/// - Bit 31 = 0: schema-defined edge type (from `schema.json`)
/// - Bit 31 = 1: schemaless edge type (dynamically assigned at runtime)
pub type EdgeTypeId = u32;

/// High bit flag for schemaless edge types.
pub const SCHEMALESS_BIT: EdgeTypeId = 0x8000_0000;

/// Maximum schema-defined edge type ID (2^31 - 1).
pub const MAX_SCHEMA_TYPE_ID: EdgeTypeId = 0x7FFF_FFFF;

/// Returns `true` if the edge type ID was dynamically assigned (schemaless).
#[inline]
pub fn is_schemaless_edge_type(type_id: EdgeTypeId) -> bool {
    type_id & SCHEMALESS_BIT != 0
}

/// Creates a schemaless edge type ID by setting bit 31 on the given local ID.
#[inline]
pub fn make_schemaless_id(local_id: u32) -> EdgeTypeId {
    debug_assert!(
        local_id <= MAX_SCHEMA_TYPE_ID,
        "Schemaless local ID {local_id} exceeds maximum {MAX_SCHEMA_TYPE_ID}"
    );
    SCHEMALESS_BIT | local_id
}

/// Extracts the local ID by masking off the schemaless bit.
#[inline]
pub fn extract_local_id(type_id: EdgeTypeId) -> u32 {
    type_id & !SCHEMALESS_BIT
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_schema_type() {
        let id = 42u32;
        assert!(!is_schemaless_edge_type(id));
        assert_eq!(extract_local_id(id), 42);
    }

    #[test]
    fn test_schemaless_type() {
        let id = make_schemaless_id(42);
        assert!(is_schemaless_edge_type(id));
        assert_eq!(extract_local_id(id), 42);
    }

    #[test]
    fn test_max_local_id() {
        let id = make_schemaless_id(MAX_SCHEMA_TYPE_ID);
        assert!(is_schemaless_edge_type(id));
        assert_eq!(extract_local_id(id), MAX_SCHEMA_TYPE_ID);
    }

    #[test]
    fn test_zero_local_id() {
        let id = make_schemaless_id(0);
        assert!(is_schemaless_edge_type(id));
        assert_eq!(extract_local_id(id), 0);
    }
}
