// M-CANONICAL-DOCS: This module defines property requirements for Cypher functions
// M-CANONICAL-DOCS: to enable pushdown hydration optimization
//
// Pushdown hydration analyzes which properties a query needs and loads them during
// the initial scan, transforming property loading from O(N*M) to O(N) complexity.

use std::sync::LazyLock;

/// Specification of property requirements for a Cypher function.
///
/// This helps the query planner understand which properties need to be loaded
/// for entity arguments to a function, enabling pushdown hydration.
#[derive(Debug, Clone, Copy)]
pub struct FunctionPropertySpec {
    /// Argument positions containing entity references (0-indexed).
    /// For example, in `validAt(entity, start, end, ts)`, position 0 is the entity.
    pub entity_args: &'static [usize],

    /// (arg_index, entity_arg_index) pairs for property name arguments.
    /// For example, in `validAt(entity, 'start', 'end', ts)`:
    /// - (1, 0) means argument 1 is a property name for entity at position 0
    /// - (2, 0) means argument 2 is a property name for entity at position 0
    pub property_name_args: &'static [(usize, usize)],

    /// If true, requires all properties of entity (e.g., keys(), properties()).
    pub needs_full_entity: bool,
}

/// Static registry of function property specifications.
/// Function names are uppercase for case-insensitive lookup.
static FUNCTION_SPECS: LazyLock<std::collections::HashMap<&'static str, FunctionPropertySpec>> =
    LazyLock::new(|| {
        // Helper specs for common patterns
        let full_entity = FunctionPropertySpec {
            entity_args: &[0],
            property_name_args: &[],
            needs_full_entity: true,
        };
        let entity_arg_only = FunctionPropertySpec {
            entity_args: &[0],
            property_name_args: &[],
            needs_full_entity: false,
        };
        let no_entity = FunctionPropertySpec {
            entity_args: &[],
            property_name_args: &[],
            needs_full_entity: false,
        };

        std::collections::HashMap::from([
            // uni.temporal.validAt(entity, start_prop, end_prop, timestamp)
            (
                "UNI.TEMPORAL.VALIDAT",
                FunctionPropertySpec {
                    entity_args: &[0],
                    property_name_args: &[(1, 0), (2, 0)],
                    needs_full_entity: false,
                },
            ),
            // Functions that need full entity materialization
            ("KEYS", full_entity),
            ("PROPERTIES", full_entity),
            ("LABELS", full_entity),
            ("NODES", full_entity),
            ("RELATIONSHIPS", full_entity),
            // Functions that take entity arg but don't need full entity
            ("COUNT", entity_arg_only),
            // Functions where properties are extracted from PropertyAccess
            ("COALESCE", no_entity),
            ("SUM", no_entity),
            ("AVG", no_entity),
            ("MIN", no_entity),
            ("MAX", no_entity),
            ("COLLECT", no_entity),
            ("PERCENTILEDISC", no_entity),
            ("PERCENTILECONT", no_entity),
        ])
    });

/// Look up the property specification for a function by name (case-insensitive).
pub fn get_function_spec(name: &str) -> Option<&'static FunctionPropertySpec> {
    let name_upper = name.to_uppercase();
    FUNCTION_SPECS.get(name_upper.as_str())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_validat_spec() {
        let spec = get_function_spec("uni.temporal.validAt").unwrap();
        assert_eq!(spec.entity_args, &[0]);
        assert_eq!(spec.property_name_args, &[(1, 0), (2, 0)]);
        assert!(!spec.needs_full_entity);
    }

    #[test]
    fn test_keys_spec() {
        let spec = get_function_spec("keys").unwrap();
        assert_eq!(spec.entity_args, &[0]);
        assert!(spec.needs_full_entity);
    }

    #[test]
    fn test_properties_spec() {
        let spec = get_function_spec("PROPERTIES").unwrap();
        assert_eq!(spec.entity_args, &[0]);
        assert!(spec.needs_full_entity);
    }

    #[test]
    fn test_unknown_function_returns_none() {
        assert!(get_function_spec("unknownFunction").is_none());
    }

    #[test]
    fn test_count_spec_exists() {
        let spec = get_function_spec("COUNT").unwrap();
        assert!(!spec.needs_full_entity);
        assert_eq!(spec.entity_args, &[0]);
    }

    #[test]
    fn test_all_aggregates_registered() {
        for func in ["COUNT", "SUM", "AVG", "MIN", "MAX", "COLLECT"] {
            let spec = get_function_spec(func);
            assert!(
                spec.is_some(),
                "Aggregate function {} should be registered",
                func
            );
            assert!(
                !spec.unwrap().needs_full_entity,
                "Aggregate function {} should not need full entity",
                func
            );
        }
    }

    #[test]
    fn test_aggregate_case_insensitive() {
        // Test that aggregate functions work with different case
        assert!(get_function_spec("count").is_some());
        assert!(get_function_spec("Count").is_some());
        assert!(get_function_spec("COUNT").is_some());
    }
}
