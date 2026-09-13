// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Selecting the registered strata a program can actually reach.
//!
//! A session may carry a pool of pre-registered rules. Merging that pool into a
//! program wholesale makes every registered rule *execute* in every program,
//! because a [`Stratum`] owns its rules and the runtime evaluates the strata
//! vector end to end. One registered rule taking a parameter then makes that
//! parameter mandatory for unrelated programs — the defect this module exists
//! to prevent.
//!
//! The pool is therefore filtered to the strata transitively reachable from the
//! rule names a program actually mentions. Two properties make that safe:
//!
//! * **Unknown names already fail earlier.** The compiler rejects a reference to
//!   an undefined rule against the pool's names, so pruning cannot turn a
//!   missing rule into a silently empty result.
//! * **Order is preserved.** Strata are appended to the pool in compile order
//!   and the runtime iterates them by position, so retaining a subsequence
//!   keeps a valid evaluation order without rebuilding any graph.
//!
//! Matching leans on [`crate::names::name_matches`], which admits every
//! candidate spelling rather than resolving to one. Over-retaining merely
//! reproduces the old behaviour for a single rule; under-retaining would drop a
//! rule the program depends on and silently change its answer. When in doubt
//! this module keeps the stratum.

use std::collections::{HashMap, HashSet, VecDeque};

use uni_cypher::locy_ast::RuleCondition;

use crate::names::name_matches;
use crate::types::{CompiledCommand, CompiledProgram, CompiledRule, Stratum};

/// Collects every rule name `program` can reach, as written in the source.
///
/// Names are returned in their as-written form — bare inside a `MODULE`, dotted
/// when the source qualified them — because the compiler never rewrites
/// references into the AST. Match them against canonical catalog keys with
/// [`crate::names::name_matches`].
///
/// Covers rule-to-rule `IS` references (negated ones included, since an
/// anti-join against a dropped relation would silently admit every row), every
/// command form that names a rule, the `path_context` source rule of a declared
/// model, and — recursively — the body of an `ASSUME`, whose rules are
/// evaluated against the enclosing program's strata.
///
/// # Examples
///
/// ```
/// use uni_locy::prune::collect_referenced_rule_names;
/// use uni_locy::types::CompiledProgram;
///
/// let empty = CompiledProgram::default();
/// assert!(collect_referenced_rule_names(&empty).is_empty());
/// ```
#[must_use]
pub fn collect_referenced_rule_names(program: &CompiledProgram) -> HashSet<String> {
    let mut out = HashSet::new();
    collect_from_commands(&program.commands, &mut out);
    for stratum in &program.strata {
        for rule in &stratum.rules {
            collect_from_rule(rule, &mut out);
        }
    }
    // A model with a path-context feature makes its invoker depend on the rule
    // supplying that path, mirroring the compiler's dependency walker. Seeding
    // from every declared model over-retains slightly rather than re-deriving
    // which rules invoke which model.
    for model in program.model_catalog.values() {
        if let Some(pc) = &model.path_context {
            out.insert(pc.source_rule.clone());
        }
    }
    out
}

/// Adds the rule names referenced by `commands` to `out`.
fn collect_from_commands(commands: &[CompiledCommand], out: &mut HashSet<String>) {
    for command in commands {
        match command {
            CompiledCommand::GoalQuery(q) => {
                out.insert(q.rule_name.to_string());
            }
            CompiledCommand::Abduce(q) => {
                out.insert(q.rule_name.to_string());
            }
            CompiledCommand::ExplainRule(e) => {
                out.insert(e.rule_name.to_string());
            }
            CompiledCommand::DeriveCommand(d) => {
                out.insert(d.rule_name.to_string());
            }
            CompiledCommand::Validate(v) => {
                out.insert(v.rule_name.clone());
            }
            CompiledCommand::Assume(a) => {
                // An ASSUME body is evaluated on top of the enclosing program's
                // strata, so anything it references must survive the prune.
                collect_from_commands(&a.body_commands, out);
                out.extend(collect_referenced_rule_names(&a.body_program));
            }
            // `CALIBRATE` names a model, not a rule; plain Cypher names neither.
            CompiledCommand::Calibrate(_) | CompiledCommand::Cypher(_) => {}
        }
    }
}

/// Adds the rule names referenced by one rule's clause bodies to `out`.
fn collect_from_rule(rule: &CompiledRule, out: &mut HashSet<String>) {
    for clause in &rule.clauses {
        for condition in &clause.where_conditions {
            if let RuleCondition::IsReference(is_ref) = condition {
                out.insert(is_ref.rule_name.to_string());
            }
        }
    }
}

/// Selects the `pool` strata needed to answer `seeds`, transitively.
///
/// Returns pool indices in ascending order, which is the order the runtime must
/// evaluate them in: strata are appended to the pool in compile order and each
/// is compiled against the rules registered before it, so a later stratum can
/// only depend on an earlier one. A subsequence therefore stays valid.
///
/// A retained stratum is expanded whole. Its rules form one strongly connected
/// component, so a co-member's own references have to be satisfied too or it
/// derives nothing and its dependents quietly lose rows. A rule appearing in
/// several strata retains all of them.
///
/// # Examples
///
/// ```
/// use std::collections::HashSet;
/// use uni_locy::prune::reachable_pool_strata;
///
/// let seeds: HashSet<String> = HashSet::new();
/// assert!(reachable_pool_strata(&seeds, &[]).is_empty());
/// ```
#[must_use]
pub fn reachable_pool_strata(seeds: &HashSet<String>, pool: &[Stratum]) -> Vec<usize> {
    // A rule may occupy more than one pool stratum when it was registered in
    // several sources, so this maps to every occurrence rather than one.
    let mut by_name: HashMap<&str, Vec<usize>> = HashMap::new();
    for (index, stratum) in pool.iter().enumerate() {
        for rule in &stratum.rules {
            by_name.entry(rule.name.as_str()).or_default().push(index);
        }
    }

    let mut retained: HashSet<usize> = HashSet::new();
    let mut seen: HashSet<String> = seeds.clone();
    let mut queue: VecDeque<String> = seeds.iter().cloned().collect();

    while let Some(reference) = queue.pop_front() {
        for (key, indices) in &by_name {
            if !name_matches(key, &reference) {
                continue;
            }
            for &index in indices {
                if !retained.insert(index) {
                    continue;
                }
                for rule in &pool[index].rules {
                    let mut refs = HashSet::new();
                    collect_from_rule(rule, &mut refs);
                    for name in refs {
                        if seen.insert(name.clone()) {
                            queue.push_back(name);
                        }
                    }
                }
            }
        }
    }

    let mut out: Vec<usize> = retained.into_iter().collect();
    out.sort_unstable();
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::CompiledClause;
    use uni_cypher::ast::Pattern;
    use uni_cypher::locy_ast::{IsReference, QualifiedName, RuleOutput, YieldClause};

    /// A rule named `name` whose clause `IS`-references each of `refs`.
    fn rule(name: &str, refs: &[&str]) -> CompiledRule {
        let where_conditions = refs
            .iter()
            .map(|r| {
                RuleCondition::IsReference(IsReference {
                    subjects: vec!["x".to_string()],
                    rule_name: QualifiedName {
                        parts: r.split('.').map(str::to_string).collect(),
                    },
                    target: None,
                    negated: false,
                })
            })
            .collect();
        CompiledRule {
            name: name.to_string(),
            clauses: vec![CompiledClause {
                match_pattern: Pattern { paths: vec![] },
                where_conditions,
                along: vec![],
                fold: vec![],
                require: vec![],
                having: vec![],
                best_by: None,
                output: RuleOutput::Yield(YieldClause { items: vec![] }),
                priority: None,
                model_invocations: vec![],
                hidden_yield_cols: vec![],
            }],
            yield_schema: vec![],
            priority: None,
        }
    }

    fn stratum(id: usize, rules: Vec<CompiledRule>) -> Stratum {
        Stratum {
            id,
            rules,
            is_recursive: false,
            depends_on: vec![],
        }
    }

    fn seeds(names: &[&str]) -> HashSet<String> {
        names.iter().map(|s| (*s).to_string()).collect()
    }

    #[test]
    fn empty_seeds_retain_nothing() {
        let pool = vec![stratum(0, vec![rule("a", &[])])];
        assert!(reachable_pool_strata(&seeds(&[]), &pool).is_empty());
    }

    #[test]
    fn exact_name_retains_its_stratum_only() {
        let pool = vec![
            stratum(0, vec![rule("a", &[])]),
            stratum(1, vec![rule("b", &[])]),
        ];
        assert_eq!(reachable_pool_strata(&seeds(&["a"]), &pool), vec![0]);
    }

    #[test]
    fn transitive_chain_is_closed_in_pool_order() {
        // c -> b -> a
        let pool = vec![
            stratum(0, vec![rule("a", &[])]),
            stratum(1, vec![rule("b", &["a"])]),
            stratum(2, vec![rule("c", &["b"])]),
        ];
        assert_eq!(reachable_pool_strata(&seeds(&["c"]), &pool), vec![0, 1, 2]);
    }

    #[test]
    fn a_whole_stratum_expands_even_via_a_co_member() {
        // Seeding `r` retains its stratum, whose co-member `q` references `x`;
        // `x` must come along or `q` derives nothing.
        let pool = vec![
            stratum(0, vec![rule("x", &[])]),
            stratum(1, vec![rule("r", &[]), rule("q", &["x"])]),
        ];
        assert_eq!(reachable_pool_strata(&seeds(&["r"]), &pool), vec![0, 1]);
    }

    #[test]
    fn one_rule_spanning_two_strata_retains_both() {
        let pool = vec![
            stratum(0, vec![rule("a", &[])]),
            stratum(1, vec![rule("b", &[])]),
            stratum(2, vec![rule("a", &[])]),
        ];
        assert_eq!(reachable_pool_strata(&seeds(&["a"]), &pool), vec![0, 2]);
    }

    #[test]
    fn bare_seed_matches_a_module_qualified_rule() {
        let pool = vec![stratum(0, vec![rule("m.adult", &[])])];
        assert_eq!(reachable_pool_strata(&seeds(&["adult"]), &pool), vec![0]);
    }

    #[test]
    fn dotted_seed_does_not_match_a_bare_rule() {
        let pool = vec![stratum(0, vec![rule("adult", &[])])];
        assert!(reachable_pool_strata(&seeds(&["m.adult"]), &pool).is_empty());
    }

    #[test]
    fn ambiguous_bare_seed_retains_every_candidate() {
        // Refusing here would drop a stratum the program needs; the pruner
        // deliberately over-retains instead.
        let pool = vec![
            stratum(0, vec![rule("m1.adult", &[])]),
            stratum(1, vec![rule("m2.adult", &[])]),
        ];
        assert_eq!(reachable_pool_strata(&seeds(&["adult"]), &pool), vec![0, 1]);
    }

    #[test]
    fn a_cycle_between_pool_strata_terminates() {
        let pool = vec![
            stratum(0, vec![rule("a", &["b"])]),
            stratum(1, vec![rule("b", &["a"])]),
        ];
        assert_eq!(reachable_pool_strata(&seeds(&["a"]), &pool), vec![0, 1]);
    }

    #[test]
    fn a_negated_reference_still_seeds_retention() {
        let mut negated = rule("safe", &["blocked"]);
        if let RuleCondition::IsReference(ir) = &mut negated.clauses[0].where_conditions[0] {
            ir.negated = true;
        }
        let pool = vec![
            stratum(0, vec![rule("blocked", &[])]),
            stratum(1, vec![negated]),
        ];
        // An anti-join over a dropped relation would admit every row, so the
        // negated target must be retained exactly like a positive one.
        assert_eq!(reachable_pool_strata(&seeds(&["safe"]), &pool), vec![0, 1]);
    }
}
