// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Evaluation of `CHECK` constraint expressions.
//!
//! This is the single evaluator for both write paths. It previously existed as
//! two token-for-token copies — one in `uni-bulk`'s `BulkWriter`, one in
//! `uni-store`'s `Writer` — which had drifted in four places, two of them
//! affecting accept/reject decisions:
//!
//! 1. **Numeric equality.** The bulk copy routed `=` / `!=` operands through
//!    `compare_values` when both sides are numeric, because [`Value`]'s
//!    `PartialEq` is type-strict and has no Int/Float arm. The writer copy used
//!    bare `==`. So `CHECK (score = 5)` against a stored `Float(5.0)` *passed*
//!    through the bulk loader and *failed* through `tx.execute`.
//! 2. **Target-literal fallback.** The writer copy carried an extra
//!    `Number(...)` unwrap for internal-format wrappers. That arm was **dead**:
//!    `val_str` comes from `trim_end_matches(')')`, which strips every trailing
//!    paren, so its `ends_with(')')` guard could never hold. It is dropped here
//!    rather than resurrected — making it reachable would add a capability
//!    neither path has today, which a deduplication commit has no business
//!    doing. Established by testing, not by reading.
//! 3. / 4. The writer copy warned on an unparseable expression and on an
//!    unknown operator; the bulk copy was silent.
//!
//! The divergence was a fix applied to one copy and never propagated — the bulk
//! behaviour is pinned by a landed regression test, and the transactional path
//! was simply never updated. This module takes the union of what actually ran:
//! bulk's numeric coercion plus the writer's warnings.
//!
//! # Shape constraints
//!
//! [`evaluate`] is a free, **synchronous** function returning
//! `anyhow::Result<bool>`, and must stay that way. One of its three call sites
//! is a match *guard* (`if !evaluate(expr, props)? =>`): guards cannot `.await`
//! and cannot take `&mut self`, and the `?` there propagates out of the
//! enclosing function, so an `Err` aborts the write rather than reporting a
//! constraint violation.
//!
//! # Scope
//!
//! The grammar handled is `prop op value` — one comparison, with optional
//! surrounding parentheses and an optional `variable.` prefix. Both operands
//! must be whitespace-free; the operator itself need not be surrounded by
//! spaces, so `age>=18` and `age >= 18` are the same constraint.
//!
//! That last point used to be false. The expression was tokenised with
//! `split_whitespace()` and required exactly three tokens, so `age>=18` was
//! *one* token, fell out of the supported grammar, and was silently never
//! enforced — while the identical `age >= 18` was. Nothing documented that,
//! and no user could have guessed it from the syntax.
//!
//! Anything genuinely more complex — a conjunction, a function call, a
//! right-hand side naming a second property — is still *allowed* with a
//! warning rather than rejected, so that an expression this evaluator cannot
//! parse never silently blocks a legitimate write. That behaviour is
//! deliberate and documented for users in `website/docs/guides/`. A missing
//! property also passes; absence is `NOT NULL`'s concern, not `CHECK`'s.

use std::cmp::Ordering;

use anyhow::{Result, anyhow};

use crate::{Properties, Value};

/// Evaluate a `CHECK` constraint expression against a property bag.
///
/// Returns `Ok(true)` when the constraint holds, is inapplicable (missing
/// property), or is outside the supported grammar.
///
/// # Errors
///
/// Returns an error only when the two operands cannot be ordered — e.g.
/// `CHECK (name > 5)` against a string. Callers treat that as a failed write,
/// not as a constraint violation.
pub fn evaluate(expression: &str, properties: &Properties) -> Result<bool> {
    let Some((prop_part, op, val_str)) = split_comparison(expression) else {
        tracing::warn!(
            "Complex CHECK constraint expression '{}' not fully supported yet; allowing write.",
            expression
        );
        return Ok(true);
    };

    // Handle "variable.property" — take the part after the dot.
    let prop_name = match prop_part.find('.') {
        Some(idx) => &prop_part[idx + 1..],
        None => prop_part,
    };

    let prop_val = match properties.get(prop_name) {
        Some(v) => v,
        // A missing property passes; that is `NOT NULL`'s job.
        None => return Ok(true),
    };

    let target_val = parse_target(val_str);

    match op {
        // Route numeric equality through `compare_values` so Int/Float coerce,
        // matching the ordering operators below. `Value`'s `PartialEq` is
        // type-strict and has no Int/Float arm, so `Float(5.0) == Int(5)` would
        // otherwise be false. Non-numeric operands keep strict structural
        // equality.
        "=" | "==" => Ok(if prop_val.is_number() && target_val.is_number() {
            compare_values(prop_val, &target_val)?.is_eq()
        } else {
            prop_val == &target_val
        }),
        "!=" | "<>" => Ok(if prop_val.is_number() && target_val.is_number() {
            !compare_values(prop_val, &target_val)?.is_eq()
        } else {
            prop_val != &target_val
        }),
        ">" => Ok(compare_values(prop_val, &target_val)?.is_gt()),
        "<" => Ok(compare_values(prop_val, &target_val)?.is_lt()),
        ">=" => Ok(compare_values(prop_val, &target_val)?.is_ge()),
        "<=" => Ok(compare_values(prop_val, &target_val)?.is_le()),
        _ => {
            tracing::warn!("Unsupported operator '{}' in CHECK constraint", op);
            Ok(true)
        }
    }
}

/// Comparison operators, longest first so `>=` is not read as `>` and `<>`
/// is not read as `<`.
const OPERATORS: &[&str] = &["==", "!=", "<>", ">=", "<=", "=", ">", "<"];

/// Split `prop op value` into its three parts, tolerating any spacing around
/// the operator.
///
/// Returns `None` when the expression is not a single comparison of two
/// whitespace-free operands — a conjunction, a function call, a quoted literal
/// containing a space. The caller then takes the documented permissive path.
///
/// Requiring whitespace-free operands is what keeps that path intact: without
/// it, `age >= 18 AND age < 100` would split into `age`, `>=`,
/// `18 AND age < 100`, and comparing an `Int` against that as a string yields
/// an *error* — turning a constraint that used to be permissively allowed into
/// one that fails every write. Rejecting it here preserves the documented
/// behaviour while still fixing the spacing bug.
fn split_comparison(expression: &str) -> Option<(&str, &str, &str)> {
    let expr = strip_outer_parens(expression.trim());

    // First operator occurrence, longest match at each position. A value
    // cannot precede the operator, so no quote tracking is needed.
    let bytes = expr.as_bytes();
    let (idx, op) = (0..bytes.len()).find_map(|i| {
        OPERATORS
            .iter()
            .find(|o| expr[i..].starts_with(**o))
            .map(|o| (i, *o))
    })?;

    let lhs = expr[..idx].trim();
    let rhs = expr[idx + op.len()..].trim();
    if lhs.is_empty()
        || rhs.is_empty()
        || lhs.chars().any(char::is_whitespace)
        || rhs.chars().any(char::is_whitespace)
    {
        return None;
    }
    Some((lhs, op, rhs))
}

/// Strip one layer of balanced surrounding parentheses, if present.
///
/// `(age >= 18)` and `age >= 18` are the same constraint. Only a *balanced*
/// outer pair is removed, so `(a) = (b)` is left alone.
fn strip_outer_parens(s: &str) -> &str {
    let Some(inner) = s.strip_prefix('(').and_then(|r| r.strip_suffix(')')) else {
        return s;
    };
    let mut depth = 0i32;
    for c in inner.chars() {
        match c {
            '(' => depth += 1,
            ')' => {
                depth -= 1;
                if depth < 0 {
                    return s;
                }
            }
            _ => {}
        }
    }
    if depth == 0 { inner.trim() } else { s }
}

/// Parse the right-hand token into a [`Value`].
///
/// Note `split_comparison` has already stripped a balanced outer paren pair,
/// so any wrapper syntax of the form `Name(...)` arrives intact. See the
/// module docs on the dropped `Number(...)` arm.
fn parse_target(val_str: &str) -> Value {
    if (val_str.starts_with('\'') && val_str.ends_with('\''))
        || (val_str.starts_with('"') && val_str.ends_with('"'))
    {
        return Value::String(val_str[1..val_str.len() - 1].to_string());
    }
    if let Ok(n) = val_str.parse::<i64>() {
        return Value::Int(n);
    }
    if let Ok(n) = val_str.parse::<f64>() {
        return Value::Float(n);
    }
    if let Ok(b) = val_str.parse::<bool>() {
        return Value::Bool(b);
    }
    Value::String(val_str.to_string())
}

/// Compare two values for ordering.
///
/// Incomparable floats (NaN) compare as [`Ordering::Equal`], matching the
/// branch-based implementations this replaces.
///
/// # Errors
///
/// Returns an error when the two values are not of comparable kinds.
fn compare_values(a: &Value, b: &Value) -> Result<Ordering> {
    match (a, b) {
        (Value::Int(n1), Value::Int(n2)) => Ok(n1.cmp(n2)),
        (Value::Float(f1), Value::Float(f2)) => Ok(f1.partial_cmp(f2).unwrap_or(Ordering::Equal)),
        // Exact i64-vs-f64 order (no lossy `as f64` cast above 2^53); preserve
        // the NaN-as-Equal behaviour for the degenerate case.
        (Value::Int(n), Value::Float(f)) => Ok(if f.is_nan() {
            Ordering::Equal
        } else {
            crate::cmp_i64_f64(*n, *f)
        }),
        (Value::Float(f), Value::Int(n)) => Ok(if f.is_nan() {
            Ordering::Equal
        } else {
            crate::cmp_i64_f64(*n, *f).reverse()
        }),
        (Value::String(s1), Value::String(s2)) => Ok(s1.cmp(s2)),
        _ => Err(anyhow!(
            "Cannot compare incompatible types: {:?} vs {:?}",
            a,
            b
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn props(pairs: &[(&str, Value)]) -> Properties {
        pairs
            .iter()
            .map(|(k, v)| ((*k).to_string(), v.clone()))
            .collect()
    }

    /// The divergence that mattered: a float-valued property against an
    /// integer literal. The bulk path coerced, the transactional path did not.
    #[test]
    fn numeric_equality_coerces_across_int_and_float() {
        let p = props(&[("score", Value::Float(5.0))]);
        assert!(evaluate("(n.score = 5)", &p).unwrap());
        assert!(!evaluate("(n.score != 5)", &p).unwrap());

        let p = props(&[("score", Value::Int(5))]);
        assert!(evaluate("(n.score = 5.0)", &p).unwrap());
    }

    /// Non-numeric operands keep strict structural equality.
    #[test]
    fn non_numeric_equality_stays_strict() {
        let p = props(&[("name", Value::String("a".into()))]);
        assert!(evaluate("(n.name = 'a')", &p).unwrap());
        assert!(!evaluate("(n.name = 'b')", &p).unwrap());
    }

    /// Exactness above 2^53, where a lossy `as f64` cast would compare equal.
    #[test]
    fn large_integers_compare_exactly() {
        let p = props(&[("v", Value::Int(9_007_199_254_740_993))]);
        assert!(evaluate("(n.v > 9007199254740992.0)", &p).unwrap());
    }

    /// The writer copy's `Number(...)` arm was unreachable — `val_str` has had
    /// every trailing paren stripped before it is inspected — so such a target
    /// degrades to a string and an ordering comparison against it errors. This
    /// pins that pre-existing behaviour rather than the dead branch's intent.
    #[test]
    fn number_wrapper_target_is_not_special_cased() {
        let p = props(&[("v", Value::Int(7))]);
        assert!(evaluate("(n.v < Number(8.5))", &p).is_err());
    }

    #[test]
    fn ordering_operators() {
        let p = props(&[("v", Value::Int(5))]);
        assert!(evaluate("(n.v > 4)", &p).unwrap());
        assert!(evaluate("(n.v >= 5)", &p).unwrap());
        assert!(evaluate("(n.v < 6)", &p).unwrap());
        assert!(evaluate("(n.v <= 5)", &p).unwrap());
        assert!(!evaluate("(n.v > 5)", &p).unwrap());
    }

    /// Unsupported shapes allow the write rather than blocking it.
    /// The expression was tokenised with `split_whitespace()` and required
    /// exactly three tokens, so `age>=18` was ONE token, fell out of the
    /// supported grammar, and was silently never enforced — while the
    /// identical `age >= 18` was. Nothing documented that; the docs describe
    /// only the *literal* as needing to be whitespace-free.
    #[test]
    fn operator_spacing_does_not_decide_whether_a_constraint_is_enforced() {
        let mut p = Properties::new();
        p.insert("age".to_string(), Value::Int(15));

        // Every spelling of the same constraint must reject a 15-year-old.
        for expr in [
            "age >= 18",
            "age>=18",
            "age>= 18",
            "age >=18",
            "(age>=18)",
            "( age>=18 )",
            "n.age>=18",
            "(n.age>=18)",
        ] {
            assert!(
                !evaluate(expr, &p).unwrap(),
                "`{expr}` must be enforced and reject age=15; a spelling that \
                 merely omits spaces used to be silently inert"
            );
        }

        // Control: the same spellings must ACCEPT a conforming row, so the
        // assertions above cannot be passing because everything now fails.
        let mut ok = Properties::new();
        ok.insert("age".to_string(), Value::Int(21));
        for expr in ["age >= 18", "age>=18", "(n.age>=18)"] {
            assert!(evaluate(expr, &ok).unwrap(), "`{expr}` must accept age=21");
        }

        // Longest-match: `>=` must not be read as `>`, nor `<>` as `<`.
        let mut exact = Properties::new();
        exact.insert("age".to_string(), Value::Int(18));
        assert!(
            evaluate("age>=18", &exact).unwrap(),
            ">= must include equality"
        );
        assert!(
            !evaluate("age>18", &exact).unwrap(),
            "> must exclude equality"
        );
        assert!(!evaluate("age<>18", &exact).unwrap(), "<> is not-equal");
        assert!(!evaluate("age!=18", &exact).unwrap(), "!= is not-equal");

        // A negative literal still parses when the operator is unspaced.
        let mut neg = Properties::new();
        neg.insert("t".to_string(), Value::Int(-5));
        assert!(evaluate("t<-1", &neg).unwrap(), "t=-5 is less than -1");
    }

    /// Tokenising by operator must NOT convert the documented permissive path
    /// into a hard error. `age >= 18 AND age < 100` would otherwise split into
    /// `age`, `>=`, `18 AND age < 100`, and comparing an Int against that as a
    /// string returns `Err` — failing every write on a constraint that used to
    /// be allowed. It has to stay `Ok(true)`.
    #[test]
    fn a_compound_expression_still_takes_the_documented_permissive_path() {
        let mut p = Properties::new();
        p.insert("age".to_string(), Value::Int(15));
        for expr in [
            "(n.v > 1 AND n.v < 9)",
            "age >= 18 AND age < 100",
            "age>=18 AND age<100",
            "age >= 18 OR age = 0",
            "length(name) > 0",
        ] {
            let got = evaluate(expr, &p);
            assert!(
                matches!(got, Ok(true)),
                "`{expr}` must stay permissively allowed, got {got:?} — turning \
                 it into an error would fail writes that used to succeed"
            );
        }
    }

    #[test]
    fn unsupported_shapes_allow_the_write() {
        let p = props(&[("v", Value::Int(5))]);
        // Missing property.
        assert!(evaluate("(n.other = 1)", &p).unwrap());
        // Not three tokens.
        assert!(evaluate("(n.v > 1 AND n.v < 9)", &p).unwrap());
        // Unknown operator.
        assert!(evaluate("(n.v ~~ 1)", &p).unwrap());
    }

    /// An un-orderable pair is an error, not a violation — the caller aborts
    /// the write rather than reporting a failed constraint.
    #[test]
    fn incomparable_operands_error() {
        let p = props(&[("name", Value::String("a".into()))]);
        assert!(evaluate("(n.name > 5)", &p).is_err());
    }

    /// A bare property name, with no `variable.` prefix.
    #[test]
    fn bare_property_name_is_accepted() {
        let p = props(&[("v", Value::Int(5))]);
        assert!(evaluate("v = 5", &p).unwrap());
    }
}
