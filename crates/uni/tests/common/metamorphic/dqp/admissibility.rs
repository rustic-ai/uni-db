//! Teeth for the admissibility contract.
//!
//! The contract says a differential oracle may not compare float aggregates,
//! `LIMIT`, or nondeterministic functions. A rule nobody has watched reject
//! anything is indistinguishable from a rule that cannot reject anything — the
//! same failure this whole oracle exists to catch — so these tests drive real
//! inadmissible cases through the real check.
//!
//! There are two halves, and both matter:
//!
//! 1. The **classifier** must discriminate. `Case::has_float_aggregate` returning
//!    a constant would compile, pass a naive test, and silently disable the
//!    contract.
//! 2. The **rule** must reject. A float-aggregate case, which `arb_agg_case()`
//!    genuinely produces, must be refused by the `IntAggregate` kind.

use proptest::strategy::{Strategy, ValueTree};
use proptest::test_runner::TestRunner;

use super::driver::CaseKind;
use super::seed::Tier;
use crate::querygen::{
    Case, arb_agg_case, arb_agg_case_int, arb_case, arb_case_selective, render::render,
};

/// Draws `n` values from `strategy` deterministically.
fn draw<S: Strategy>(strategy: S, n: usize) -> Vec<S::Value> {
    let mut runner = TestRunner::deterministic();
    (0..n)
        .map(|_| {
            strategy
                .new_tree(&mut runner)
                .expect("strategy produced no value")
                .current()
        })
        .collect()
}

/// The classifier must answer **both** ways over real generated input.
///
/// `arb_agg_case()` draws `sum` targets from every numeric property, and the
/// schema's only float is `score`, so a few hundred draws must contain some of
/// each. If they do not, the classifier is a constant and the contract is off.
#[test]
fn float_aggregate_classifier_discriminates() {
    let cases = draw(arb_agg_case(), 400);
    let floats = cases.iter().filter(|c| c.has_float_aggregate()).count();
    let non_floats = cases.len() - floats;

    assert!(
        floats > 0,
        "`arb_agg_case` produced no float aggregate in {} draws — either the \
         generator changed or the classifier never returns true, and in both \
         cases the admissibility rule below is testing nothing",
        cases.len()
    );
    assert!(
        non_floats > 0,
        "every draw classified as a float aggregate in {} draws — the classifier \
         is not discriminating",
        cases.len()
    );

    // And every one of them really is an aggregate, so `is_aggregate` is not a
    // constant either.
    assert!(
        cases.iter().all(Case::is_aggregate),
        "`arb_agg_case` produced a non-aggregate case"
    );
}

/// The integer-only generator must never produce what the rule forbids.
#[test]
fn int_aggregate_generator_never_emits_a_float_aggregate() {
    for case in draw(arb_agg_case_int(), 400) {
        assert!(
            !case.has_float_aggregate(),
            "`arb_agg_case_int` produced a float aggregate: {}",
            render(&case.base_query())
        );
        assert!(
            case.is_aggregate(),
            "`arb_agg_case_int` produced a non-aggregate: {}",
            render(&case.base_query())
        );
    }
}

/// **The exit criterion**: inject a float aggregate into the integer-only kind
/// and it must be refused, with a reason.
///
/// The rejection happens at *generation* time in the driver — before the case is
/// ever executed — so a violation surfaces as the generator bug it is rather
/// than as a bag difference with no bug behind it.
#[test]
fn float_aggregate_is_inadmissible_for_the_integer_kind() {
    let offender = draw(arb_agg_case(), 400)
        .into_iter()
        .find(Case::has_float_aggregate)
        .expect("no float aggregate drawn; see the classifier test");

    let why = CaseKind::IntAggregate
        .inadmissible(&offender)
        .expect("a float aggregate must be inadmissible for the integer kind");
    assert!(
        why.contains("Float"),
        "the rejection must say why; got: {why}"
    );

    // And it is refused by the plain kind too, for the same reason plus one:
    // the plain kind admits no aggregate at all.
    assert!(
        CaseKind::Plain.inadmissible(&offender).is_some(),
        "a float aggregate must also be inadmissible for the plain kind"
    );
}

/// Each kind must accept what its own generator produces, or every run would
/// fail on the first case.
#[test]
fn each_kind_admits_its_own_generator() {
    for case in draw(arb_case(), 200) {
        assert!(
            CaseKind::Plain.inadmissible(&case).is_none(),
            "the plain kind rejected its own generator's output: {}",
            render(&case.base_query())
        );
    }
    for case in draw(arb_agg_case_int(), 200) {
        assert!(
            CaseKind::IntAggregate.inadmissible(&case).is_none(),
            "the integer-aggregate kind rejected its own generator's output: {}",
            render(&case.base_query())
        );
    }
}

/// The kinds must not accept each other's output, or `CaseKind` would be
/// decorative.
#[test]
fn the_kinds_are_mutually_exclusive() {
    let plain = draw(arb_case(), 50);
    assert!(
        plain
            .iter()
            .all(|c| CaseKind::IntAggregate.inadmissible(c).is_some()),
        "the integer-aggregate kind accepted a plain projection"
    );

    let agg = draw(arb_agg_case_int(), 50);
    assert!(
        agg.iter()
            .all(|c| CaseKind::Plain.inadmissible(c).is_some()),
        "the plain kind accepted an aggregate projection"
    );
}

/// The selectivity floor must actually floor something.
///
/// Ordinary generation leaves two thirds of cases unfiltered — measured in Phase
/// 0A, where median rows returned equalled the fixture's vertex count exactly.
/// The tiers that need a filter must get one on every draw.
#[test]
fn selectivity_floor_guarantees_a_base_filter() {
    let selective = draw(arb_case_selective(), 300);
    assert!(
        selective.iter().all(Case::has_base_filter),
        "a tier with the selectivity floor drew a case with no base WHERE"
    );

    // Non-vacuity: the ordinary generator must genuinely produce unfiltered
    // cases, or the floor is solving a problem that does not exist.
    let ordinary = draw(arb_case(), 300);
    let unfiltered = ordinary.iter().filter(|c| !c.has_base_filter()).count();
    assert!(
        unfiltered > 0,
        "the ordinary generator produced no unfiltered case in 300 draws — the \
         selectivity floor would be pointless, so this expectation has drifted"
    );
}

/// The floor is applied by tier, and only where it is needed.
#[test]
fn only_large_tiers_carry_the_selectivity_floor() {
    assert!(!Tier::Tiny.needs_selectivity_floor());
    assert!(Tier::Smoke.needs_selectivity_floor());
    assert!(Tier::Large.needs_selectivity_floor());
}
