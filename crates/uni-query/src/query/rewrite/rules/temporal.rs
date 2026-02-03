/// Temporal function rewrite rules
///
/// This module implements rewrite rules for temporal functions, transforming
/// them into equivalent predicate expressions that can be pushed down to storage.
use crate::query::rewrite::context::RewriteContext;
use crate::query::rewrite::error::RewriteError;
use crate::query::rewrite::rule::{ArgConstraints, Arity, RewriteRule};
use uni_cypher::ast::{BinaryOp, CypherLiteral, Expr};

/// Helper function to extract a string literal from an expression
fn extract_string_literal(expr: &Expr) -> Result<String, RewriteError> {
    match expr {
        Expr::Literal(CypherLiteral::String(s)) => Ok(s.clone()),
        _ => Err(RewriteError::TransformError {
            message: "Expected string literal".to_string(),
        }),
    }
}

/// Build a property access expression: entity.property_name
fn property(entity: Expr, property_name: String) -> Expr {
    Expr::Property(Box::new(entity), property_name)
}

/// Build: entity.end_prop IS NULL OR entity.end_prop > timestamp
///
/// This implements half-open interval semantics: [start, end) where end is exclusive.
/// For an entity to be valid at a timestamp, we need: start <= timestamp < end
fn ongoing_or_after(entity: Expr, end_prop: String, timestamp: Expr) -> Expr {
    Expr::BinaryOp {
        left: Box::new(Expr::IsNull(Box::new(property(
            entity.clone(),
            end_prop.clone(),
        )))),
        op: BinaryOp::Or,
        right: Box::new(Expr::BinaryOp {
            left: Box::new(property(entity, end_prop)),
            op: BinaryOp::Gt,
            right: Box::new(timestamp),
        }),
    }
}

/// Rewrite rule for uni.temporal.validAt
///
/// Transforms: uni.temporal.validAt(e, 'start', 'end', ts)
/// Into: e.start <= ts AND (e.end IS NULL OR e.end > ts)
///
/// This implements half-open interval semantics: [start, end) where:
/// - start is inclusive (<=)
/// - end is exclusive (>)
/// - null end means "ongoing" (no end date)
pub struct ValidAtRule;

impl RewriteRule for ValidAtRule {
    fn function_name(&self) -> &str {
        "uni.temporal.validAt"
    }

    fn validate_args(&self, args: &[Expr]) -> Result<(), RewriteError> {
        let constraints = ArgConstraints {
            arity: Arity::Exact(4),
            literal_args: vec![1, 2], // Property names must be literals
            entity_arg: Some(0),      // First arg is entity
        };
        constraints.validate(args)
    }

    fn rewrite(&self, args: Vec<Expr>, _ctx: &RewriteContext) -> Result<Expr, RewriteError> {
        let entity = args[0].clone();
        let start_prop = extract_string_literal(&args[1])?;
        let end_prop = extract_string_literal(&args[2])?;
        let timestamp = args[3].clone();

        // Build: e.start <= ts AND (e.end IS NULL OR e.end > ts)
        Ok(Expr::BinaryOp {
            left: Box::new(Expr::BinaryOp {
                left: Box::new(property(entity.clone(), start_prop)),
                op: BinaryOp::LtEq,
                right: Box::new(timestamp.clone()),
            }),
            op: BinaryOp::And,
            right: Box::new(ongoing_or_after(entity, end_prop, timestamp)),
        })
    }
}

/// Rewrite rule for uni.temporal.overlaps
///
/// Transforms: uni.temporal.overlaps(e, 'start', 'end', range_start, range_end)
/// Into: e.start <= range_end AND (e.end IS NULL OR e.end > range_start)
///
/// Uses half-open interval semantics: entity range [start, end) overlaps with
/// query range [range_start, range_end) when start < range_end AND end > range_start.
pub struct OverlapsRule;

impl RewriteRule for OverlapsRule {
    fn function_name(&self) -> &str {
        "uni.temporal.overlaps"
    }

    fn validate_args(&self, args: &[Expr]) -> Result<(), RewriteError> {
        let constraints = ArgConstraints {
            arity: Arity::Exact(5),
            literal_args: vec![1, 2], // Property names must be literals
            entity_arg: Some(0),      // First arg is entity
        };
        constraints.validate(args)
    }

    fn rewrite(&self, args: Vec<Expr>, _ctx: &RewriteContext) -> Result<Expr, RewriteError> {
        let entity = args[0].clone();
        let start_prop = extract_string_literal(&args[1])?;
        let end_prop = extract_string_literal(&args[2])?;
        let range_start = args[3].clone();
        let range_end = args[4].clone();

        // Build: e.start <= range_end AND (e.end IS NULL OR e.end > range_start)
        Ok(Expr::BinaryOp {
            left: Box::new(Expr::BinaryOp {
                left: Box::new(property(entity.clone(), start_prop)),
                op: BinaryOp::LtEq,
                right: Box::new(range_end),
            }),
            op: BinaryOp::And,
            right: Box::new(ongoing_or_after(entity, end_prop, range_start)),
        })
    }
}

/// Rewrite rule for uni.temporal.precedes
///
/// Transforms: uni.temporal.precedes(e, 'end', ts)
/// Into: e.end < ts
///
/// This checks if the entity's end time is before the given timestamp.
/// Note: This returns NULL if e.end is NULL (ongoing periods don't precede).
pub struct PrecedesRule;

impl RewriteRule for PrecedesRule {
    fn function_name(&self) -> &str {
        "uni.temporal.precedes"
    }

    fn validate_args(&self, args: &[Expr]) -> Result<(), RewriteError> {
        let constraints = ArgConstraints {
            arity: Arity::Exact(3),
            literal_args: vec![1], // Property name must be literal
            entity_arg: Some(0),   // First arg is entity
        };
        constraints.validate(args)
    }

    fn rewrite(&self, args: Vec<Expr>, _ctx: &RewriteContext) -> Result<Expr, RewriteError> {
        let entity = args[0].clone();
        let end_prop = extract_string_literal(&args[1])?;
        let timestamp = args[2].clone();

        // Build: e.end < ts
        Ok(Expr::BinaryOp {
            left: Box::new(property(entity, end_prop)),
            op: BinaryOp::Lt,
            right: Box::new(timestamp),
        })
    }
}

/// Rewrite rule for uni.temporal.succeeds
///
/// Transforms: uni.temporal.succeeds(e, 'start', ts)
/// Into: e.start > ts
///
/// This checks if the entity's start time is after the given timestamp.
pub struct SucceedsRule;

impl RewriteRule for SucceedsRule {
    fn function_name(&self) -> &str {
        "uni.temporal.succeeds"
    }

    fn validate_args(&self, args: &[Expr]) -> Result<(), RewriteError> {
        let constraints = ArgConstraints {
            arity: Arity::Exact(3),
            literal_args: vec![1], // Property name must be literal
            entity_arg: Some(0),   // First arg is entity
        };
        constraints.validate(args)
    }

    fn rewrite(&self, args: Vec<Expr>, _ctx: &RewriteContext) -> Result<Expr, RewriteError> {
        let entity = args[0].clone();
        let start_prop = extract_string_literal(&args[1])?;
        let timestamp = args[2].clone();

        // Build: e.start > ts
        Ok(Expr::BinaryOp {
            left: Box::new(property(entity, start_prop)),
            op: BinaryOp::Gt,
            right: Box::new(timestamp),
        })
    }
}

/// Rewrite rule for uni.temporal.isOngoing
///
/// Transforms: uni.temporal.isOngoing(e, 'end')
/// Into: e.end IS NULL
///
/// This checks if the entity is currently ongoing (no end date).
pub struct IsOngoingRule;

impl RewriteRule for IsOngoingRule {
    fn function_name(&self) -> &str {
        "uni.temporal.isOngoing"
    }

    fn validate_args(&self, args: &[Expr]) -> Result<(), RewriteError> {
        let constraints = ArgConstraints {
            arity: Arity::Exact(2),
            literal_args: vec![1], // Property name must be literal
            entity_arg: Some(0),   // First arg is entity
        };
        constraints.validate(args)
    }

    fn rewrite(&self, args: Vec<Expr>, _ctx: &RewriteContext) -> Result<Expr, RewriteError> {
        let entity = args[0].clone();
        let end_prop = extract_string_literal(&args[1])?;

        // Build: e.end IS NULL
        Ok(Expr::IsNull(Box::new(property(entity, end_prop))))
    }
}

/// Rewrite rule for uni.temporal.hasClosed
///
/// Transforms: uni.temporal.hasClosed(e, 'end')
/// Into: e.end IS NOT NULL
///
/// This checks if the entity has ended (has an end date).
pub struct HasClosedRule;

impl RewriteRule for HasClosedRule {
    fn function_name(&self) -> &str {
        "uni.temporal.hasClosed"
    }

    fn validate_args(&self, args: &[Expr]) -> Result<(), RewriteError> {
        let constraints = ArgConstraints {
            arity: Arity::Exact(2),
            literal_args: vec![1], // Property name must be literal
            entity_arg: Some(0),   // First arg is entity
        };
        constraints.validate(args)
    }

    fn rewrite(&self, args: Vec<Expr>, _ctx: &RewriteContext) -> Result<Expr, RewriteError> {
        let entity = args[0].clone();
        let end_prop = extract_string_literal(&args[1])?;

        // Build: e.end IS NOT NULL
        Ok(Expr::IsNotNull(Box::new(property(entity, end_prop))))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_entity() -> Expr {
        Expr::Variable("e".into())
    }

    fn test_timestamp() -> Expr {
        Expr::Variable("ts".into())
    }

    #[test]
    fn test_valid_at_validation() {
        let rule = ValidAtRule;

        // Valid arguments
        let valid_args = vec![
            test_entity(),
            Expr::Literal(CypherLiteral::String("start".into())),
            Expr::Literal(CypherLiteral::String("end".into())),
            test_timestamp(),
        ];
        assert!(rule.validate_args(&valid_args).is_ok());

        // Wrong arity
        let wrong_arity = vec![test_entity()];
        assert!(rule.validate_args(&wrong_arity).is_err());

        // Non-literal property name
        let non_literal = vec![
            test_entity(),
            Expr::Variable("prop".into()), // Should be literal
            Expr::Literal(CypherLiteral::String("end".into())),
            test_timestamp(),
        ];
        assert!(rule.validate_args(&non_literal).is_err());
    }

    #[test]
    fn test_valid_at_rewrite() {
        let rule = ValidAtRule;
        let ctx = RewriteContext::default();

        let args = vec![
            test_entity(),
            Expr::Literal(CypherLiteral::String("start".into())),
            Expr::Literal(CypherLiteral::String("end".into())),
            test_timestamp(),
        ];

        let result = rule.rewrite(args, &ctx).unwrap();

        // Should be an AND expression
        assert!(matches!(
            result,
            Expr::BinaryOp {
                op: BinaryOp::And,
                ..
            }
        ));
    }

    #[test]
    fn test_overlaps_rewrite() {
        let rule = OverlapsRule;
        let ctx = RewriteContext::default();

        let args = vec![
            test_entity(),
            Expr::Literal(CypherLiteral::String("start".into())),
            Expr::Literal(CypherLiteral::String("end".into())),
            Expr::Variable("rs".into()),
            Expr::Variable("re".into()),
        ];

        let result = rule.rewrite(args, &ctx).unwrap();

        // Should be an AND expression
        assert!(matches!(
            result,
            Expr::BinaryOp {
                op: BinaryOp::And,
                ..
            }
        ));
    }

    #[test]
    fn test_precedes_rewrite() {
        let rule = PrecedesRule;
        let ctx = RewriteContext::default();

        let args = vec![
            test_entity(),
            Expr::Literal(CypherLiteral::String("end".into())),
            test_timestamp(),
        ];

        let result = rule.rewrite(args, &ctx).unwrap();

        // Should be a less-than expression
        assert!(matches!(
            result,
            Expr::BinaryOp {
                op: BinaryOp::Lt,
                ..
            }
        ));
    }

    #[test]
    fn test_succeeds_rewrite() {
        let rule = SucceedsRule;
        let ctx = RewriteContext::default();

        let args = vec![
            test_entity(),
            Expr::Literal(CypherLiteral::String("start".into())),
            test_timestamp(),
        ];

        let result = rule.rewrite(args, &ctx).unwrap();

        // Should be a greater-than expression
        assert!(matches!(
            result,
            Expr::BinaryOp {
                op: BinaryOp::Gt,
                ..
            }
        ));
    }

    #[test]
    fn test_is_ongoing_rewrite() {
        let rule = IsOngoingRule;
        let ctx = RewriteContext::default();

        let args = vec![
            test_entity(),
            Expr::Literal(CypherLiteral::String("end".into())),
        ];

        let result = rule.rewrite(args, &ctx).unwrap();

        // Should be an IS NULL expression
        assert!(matches!(result, Expr::IsNull(_)));
    }

    #[test]
    fn test_has_closed_rewrite() {
        let rule = HasClosedRule;
        let ctx = RewriteContext::default();

        let args = vec![
            test_entity(),
            Expr::Literal(CypherLiteral::String("end".into())),
        ];

        let result = rule.rewrite(args, &ctx).unwrap();

        // Should be an IS NOT NULL expression
        assert!(matches!(result, Expr::IsNotNull(_)));
    }
}
