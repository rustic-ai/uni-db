/// Rewrite rule trait and supporting types
use crate::query::rewrite::context::RewriteContext;
use crate::query::rewrite::error::RewriteError;
use uni_cypher::ast::Expr;

/// Trait for implementing query rewrite rules
///
/// A rewrite rule transforms function calls into equivalent predicate expressions
/// at compile time. Rules are registered in the global registry and applied
/// during query compilation.
///
/// # Example
///
/// ```ignore
/// impl RewriteRule for ValidAtRule {
///     fn function_name(&self) -> &str {
///         "uni.temporal.validAt"
///     }
///
///     fn validate_args(&self, args: &[Expr]) -> Result<(), RewriteError> {
///         // Check arity and argument types
///         if args.len() != 4 {
///             return Err(RewriteError::ArityMismatch { expected: 4, got: args.len() });
///         }
///         // ... more validation
///         Ok(())
///     }
///
///     fn rewrite(&self, args: Vec<Expr>, _ctx: &RewriteContext) -> Result<Expr, RewriteError> {
///         // Transform into predicate expression
///         Ok(rewritten_expr)
///     }
/// }
/// ```
pub trait RewriteRule: Send + Sync {
    /// The fully-qualified function name this rule matches
    ///
    /// Example: "uni.temporal.validAt"
    fn function_name(&self) -> &str;

    /// Validate arguments before attempting rewrite
    ///
    /// Returns `Ok(())` if arguments are valid and the rule can be applied.
    /// Returns `Err(RewriteError)` if arguments don't match expected pattern.
    ///
    /// Common validations:
    /// - Check arity (number of arguments)
    /// - Verify certain arguments are string literals (property names)
    /// - Verify certain arguments are entity references (variables)
    /// - Check argument types
    fn validate_args(&self, args: &[Expr]) -> Result<(), RewriteError>;

    /// Perform the actual rewrite transformation
    ///
    /// Takes validated arguments and context, returns the rewritten expression.
    /// This method is only called after `validate_args` returns `Ok(())` and
    /// `is_applicable` returns `true`.
    ///
    /// # Arguments
    ///
    /// * `args` - Function arguments (already validated)
    /// * `ctx` - Rewrite context (scope, schema, etc.)
    ///
    /// # Returns
    ///
    /// The rewritten expression, or an error if transformation fails.
    fn rewrite(&self, args: Vec<Expr>, ctx: &RewriteContext) -> Result<Expr, RewriteError>;

    /// Check if this rule can be applied in the current context
    ///
    /// Override this method if the rule requires specific context conditions
    /// (e.g., schema information, variable scope).
    ///
    /// Default implementation: always applicable if args validate.
    fn is_applicable(&self, _ctx: &RewriteContext) -> bool {
        true
    }
}

/// Function argument arity specification
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Arity {
    /// Exact number of arguments required
    Exact(usize),

    /// Range of acceptable argument counts (min, max)
    Range(usize, usize),

    /// Variable number of arguments with minimum count
    VarArgs(usize),
}

impl Arity {
    /// Check if the given argument count satisfies this arity requirement
    pub fn check(&self, count: usize) -> Result<(), RewriteError> {
        let (min, max) = match self {
            Arity::Exact(n) => (*n, *n),
            Arity::Range(min, max) => (*min, *max),
            Arity::VarArgs(min) => (*min, usize::MAX),
        };

        if count >= min && count <= max {
            return Ok(());
        }

        if min == max {
            Err(RewriteError::ArityMismatch {
                expected: min,
                got: count,
            })
        } else {
            Err(RewriteError::ArityOutOfRange {
                min,
                max,
                got: count,
            })
        }
    }
}

/// Metadata about function argument requirements
///
/// Use this to declaratively specify argument constraints for a rewrite rule.
#[derive(Debug, Clone)]
pub struct ArgConstraints {
    /// Expected number of arguments (or range)
    pub arity: Arity,

    /// Indices of arguments that must be string literals (e.g., property names)
    pub literal_args: Vec<usize>,

    /// Index of the argument that is the entity (for property access)
    pub entity_arg: Option<usize>,
}

impl ArgConstraints {
    /// Validate a set of arguments against these constraints
    pub fn validate(&self, args: &[Expr]) -> Result<(), RewriteError> {
        // Check arity
        self.arity.check(args.len())?;

        // Check literal arguments
        for &idx in &self.literal_args {
            if idx >= args.len() {
                continue; // Arity check already failed
            }

            if !matches!(args[idx], Expr::Literal(_)) {
                return Err(RewriteError::ExpectedStringLiteral { arg_index: idx });
            }
        }

        // Check entity argument
        if let Some(idx) = self.entity_arg {
            if idx >= args.len() {
                return Ok(()); // Arity check already failed
            }

            if !matches!(args[idx], Expr::Variable(_) | Expr::Property(_, _)) {
                return Err(RewriteError::ExpectedEntityReference { arg_index: idx });
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_arity_exact() {
        let arity = Arity::Exact(3);
        assert!(arity.check(3).is_ok());
        assert!(arity.check(2).is_err());
        assert!(arity.check(4).is_err());
    }

    #[test]
    fn test_arity_range() {
        let arity = Arity::Range(2, 4);
        assert!(arity.check(1).is_err());
        assert!(arity.check(2).is_ok());
        assert!(arity.check(3).is_ok());
        assert!(arity.check(4).is_ok());
        assert!(arity.check(5).is_err());
    }

    #[test]
    fn test_arity_varargs() {
        let arity = Arity::VarArgs(2);
        assert!(arity.check(1).is_err());
        assert!(arity.check(2).is_ok());
        assert!(arity.check(10).is_ok());
    }

    #[test]
    fn test_arg_constraints_validate() {
        use uni_cypher::ast::CypherLiteral;

        let constraints = ArgConstraints {
            arity: Arity::Exact(3),
            literal_args: vec![1],
            entity_arg: Some(0),
        };

        // Valid arguments
        let valid_args = vec![
            Expr::Variable("e".into()),
            Expr::Literal(CypherLiteral::String("prop".into())),
            Expr::Variable("x".into()),
        ];
        assert!(constraints.validate(&valid_args).is_ok());

        // Wrong arity
        let wrong_arity = vec![Expr::Variable("e".into())];
        assert!(constraints.validate(&wrong_arity).is_err());

        // Non-literal where literal expected
        let non_literal = vec![
            Expr::Variable("e".into()),
            Expr::Variable("prop".into()), // Should be literal
            Expr::Variable("x".into()),
        ];
        assert!(constraints.validate(&non_literal).is_err());
    }
}
