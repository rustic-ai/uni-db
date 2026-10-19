//! Weighted model counting (WMC) over BDDs.
//!
//! Exact inference (`locy_lineage`) builds each fact's possible-worlds formula
//! as a Binary Decision Diagram and evaluates its probability here by Shannon
//! expansion (Sang et al. 2005; Darwiche 2011 for the SDD generalization).

use std::collections::HashMap;

use biodivine_lib_bdd::{Bdd, BddPointer, BddVariable};

/// The probability that `bdd` holds when each variable is an independent
/// event of the probability `prob_map` gives it (0.5 when absent).
pub(crate) fn bdd_probability(bdd: &Bdd, prob_map: &HashMap<BddVariable, f64>) -> f64 {
    eval_bdd_probability(bdd, prob_map)
}

/// Evaluate the probability of a BDD via Shannon expansion.
///
/// For each internal node with variable `v`:
///   P(node) = (1 - p_v) · P(low_child) + p_v · P(high_child)
///
/// Terminal nodes: P(⊥) = 0, P(⊤) = 1.
fn eval_bdd_probability(bdd: &Bdd, prob_map: &HashMap<BddVariable, f64>) -> f64 {
    let mut memo: HashMap<BddPointer, f64> = HashMap::new();
    eval_ptr(bdd, bdd.root_pointer(), prob_map, &mut memo)
}

fn eval_ptr(
    bdd: &Bdd,
    ptr: BddPointer,
    prob_map: &HashMap<BddVariable, f64>,
    memo: &mut HashMap<BddPointer, f64>,
) -> f64 {
    if ptr.is_zero() {
        return 0.0;
    }
    if ptr.is_one() {
        return 1.0;
    }
    if let Some(&cached) = memo.get(&ptr) {
        return cached;
    }

    let var = bdd.var_of(ptr);
    let p = prob_map.get(&var).copied().unwrap_or(0.5);
    let lo = eval_ptr(bdd, bdd.low_link_of(ptr), prob_map, memo);
    let hi = eval_ptr(bdd, bdd.high_link_of(ptr), prob_map, memo);
    let result = (1.0 - p) * lo + p * hi;
    memo.insert(ptr, result);
    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use biodivine_lib_bdd::BddVariableSet;

    /// `x OR (y AND z)` over independent variables: the reachability diamond.
    #[test]
    fn shannon_expansion_matches_inclusion_exclusion() {
        let set = BddVariableSet::new_anonymous(3);
        let v = set.variables();
        let f = set
            .mk_var(v[0])
            .or(&set.mk_var(v[1]).and(&set.mk_var(v[2])));
        let probs = HashMap::from([(v[0], 0.7), (v[1], 0.3), (v[2], 0.5)]);
        let p = bdd_probability(&f, &probs);
        assert!((p - 0.745).abs() < 1e-12, "{p}");
    }

    /// A variable read twice counts once: `(x AND y) OR (x AND z)`.
    #[test]
    fn shared_variable_counts_once() {
        let set = BddVariableSet::new_anonymous(3);
        let v = set.variables();
        let x = set.mk_var(v[0]);
        let f = x.and(&set.mk_var(v[1])).or(&x.and(&set.mk_var(v[2])));
        let probs = HashMap::from([(v[0], 0.6), (v[1], 0.5), (v[2], 0.5)]);
        // 0.6 * (1 - 0.5 * 0.5), not the independence value 1 - (1 - 0.3)^2.
        let p = bdd_probability(&f, &probs);
        assert!((p - 0.45).abs() < 1e-12, "{p}");
    }

    #[test]
    fn terminals() {
        let set = BddVariableSet::new_anonymous(1);
        assert_eq!(bdd_probability(&set.mk_true(), &HashMap::new()), 1.0);
        assert_eq!(bdd_probability(&set.mk_false(), &HashMap::new()), 0.0);
    }
}
