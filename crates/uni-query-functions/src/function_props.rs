// M-CANONICAL-DOCS: This module defines property requirements for Cypher functions
// M-CANONICAL-DOCS: to enable pushdown hydration optimization
//
// Pushdown hydration analyzes which properties a query needs and loads them during
// the initial scan, transforming property loading from O(N*M) to O(N) complexity.

use std::collections::HashMap;
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
static FUNCTION_SPECS: LazyLock<HashMap<&'static str, FunctionPropertySpec>> =
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

        HashMap::from([
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
            // Identity / structural functions: take an entity arg but only
            // need its vid / type metadata, not its property bag. Without
            // these entries the unknown-function fallback marks the entity
            // as "*" (full materialization), which makes per-label `Scan`
            // branches under a label-disjunction Union resolve to *different*
            // property sets and crash `UnionExec::try_new`. See issue #62.
            ("ID", entity_arg_only),
            ("ELEMENTID", entity_arg_only),
            ("TYPE", entity_arg_only),
            // `hasLabel(entity, 'Label')` is the planner's own predicate for a
            // labelled traversal target. It reads `_labels`, which the traversal
            // emits unconditionally, so the entity needs no property
            // materialisation.
            //
            // Missing here it took the unknown-function fallback and was marked
            // "*": every labelled traversal target hydrated its whole declared
            // schema plus `_all_props`, and LDBC IC4's `DISTINCT tag, post`
            // asked for 1.4 GB against a 1 GiB pool. A scan applies its label by
            // pushdown and synthesises no predicate, which is why the same
            // entity was cheap from a scan and ruinous from a traversal.
            //
            // Third instance of this fallback biting; see #62 and #134 above.
            // The registry fails open — an unregistered function silently
            // degrades to full materialisation, which is always correct and
            // sometimes ruinous. `planner::entity_widening_test` guards the
            // class at the plan level rather than by name. See issue #203.
            ("HASLABEL", entity_arg_only),
            // Relationship endpoint accessors: need only the edge's
            // `_src_vid` / `_dst_vid` metadata, never its property bag.
            // Without these entries the unknown-function fallback marks the
            // relationship as "*" (full materialization), pulling every
            // column on the row and defeating projection. See issue #134.
            ("STARTNODE", entity_arg_only),
            ("ENDNODE", entity_arg_only),
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
    fn test_endpoint_accessors_registered() {
        // startNode/endNode take a relationship arg but need only endpoint
        // vids, not the property bag — must be entity_arg_only (issue #134).
        for func in ["startNode", "endNode", "STARTNODE", "ENDNODE"] {
            let spec =
                get_function_spec(func).unwrap_or_else(|| panic!("{func} should be registered"));
            assert_eq!(spec.entity_args, &[0], "{func} entity arg is position 0");
            assert!(
                !spec.needs_full_entity,
                "{func} must not need full entity materialization"
            );
        }
    }

    /// `hasLabel` must not widen its entity (#203).
    ///
    /// The planner synthesises this predicate for every labelled traversal
    /// target. Unregistered it takes the unknown-function fallback and marks the
    /// entity `"*"`, so the target hydrates its whole declared schema plus
    /// `_all_props` — 1.4 GB against a 1 GiB pool on LDBC IC4. It reads
    /// `_labels`, which the traversal emits unconditionally.
    #[test]
    fn test_haslabel_does_not_need_the_entity() {
        let spec = get_function_spec("hasLabel").expect(
            "hasLabel must be registered — the unknown-function fallback wildcards \
             its entity argument",
        );
        assert_eq!(spec.entity_args, &[0], "the entity is argument 0");
        assert!(
            !spec.needs_full_entity,
            "hasLabel reads _labels only; requiring the full entity is what #203 fixed"
        );
    }

    #[test]
    fn test_aggregate_case_insensitive() {
        // Test that aggregate functions work with different case
        assert!(get_function_spec("count").is_some());
        assert!(get_function_spec("Count").is_some());
        assert!(get_function_spec("COUNT").is_some());
    }
}
