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
#[derive(Debug, Clone)]
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
static FUNCTION_SPECS: LazyLock<[(&'static str, FunctionPropertySpec); 10]> = LazyLock::new(|| {
    [
        // uni.temporal.validAt(entity, start_prop, end_prop, timestamp)
        (
            "UNI.TEMPORAL.VALIDAT",
            FunctionPropertySpec {
                entity_args: &[0],
                property_name_args: &[(1, 0), (2, 0)],
                needs_full_entity: false,
            },
        ),
        // keys(entity) - returns all property names
        (
            "KEYS",
            FunctionPropertySpec {
                entity_args: &[0],
                property_name_args: &[],
                needs_full_entity: true,
            },
        ),
        // properties(entity) - returns property map
        (
            "PROPERTIES",
            FunctionPropertySpec {
                entity_args: &[0],
                property_name_args: &[],
                needs_full_entity: true,
            },
        ),
        // coalesce(entity.prop1, entity.prop2, ...)
        // Properties extracted from PropertyAccess in arguments
        (
            "COALESCE",
            FunctionPropertySpec {
                entity_args: &[],
                property_name_args: &[],
                needs_full_entity: false,
            },
        ),
        // Aggregate functions - do not need full entity materialization
        // COUNT(entity) or COUNT(*)
        (
            "COUNT",
            FunctionPropertySpec {
                entity_args: &[0],
                property_name_args: &[],
                needs_full_entity: false,
            },
        ),
        // SUM(entity.prop) - property extracted from PropertyAccess
        (
            "SUM",
            FunctionPropertySpec {
                entity_args: &[],
                property_name_args: &[],
                needs_full_entity: false,
            },
        ),
        // AVG(entity.prop) - property extracted from PropertyAccess
        (
            "AVG",
            FunctionPropertySpec {
                entity_args: &[],
                property_name_args: &[],
                needs_full_entity: false,
            },
        ),
        // MIN(entity.prop) - property extracted from PropertyAccess
        (
            "MIN",
            FunctionPropertySpec {
                entity_args: &[],
                property_name_args: &[],
                needs_full_entity: false,
            },
        ),
        // MAX(entity.prop) - property extracted from PropertyAccess
        (
            "MAX",
            FunctionPropertySpec {
                entity_args: &[],
                property_name_args: &[],
                needs_full_entity: false,
            },
        ),
        // COLLECT(entity.prop) - property extracted from PropertyAccess
        (
            "COLLECT",
            FunctionPropertySpec {
                entity_args: &[],
                property_name_args: &[],
                needs_full_entity: false,
            },
        ),
    ]
});

/// Look up the property specification for a function by name (case-insensitive).
pub fn get_function_spec(name: &str) -> Option<&'static FunctionPropertySpec> {
    let name_upper = name.to_uppercase();
    FUNCTION_SPECS
        .iter()
        .find(|(n, _)| *n == name_upper)
        .map(|(_, spec)| spec)
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
