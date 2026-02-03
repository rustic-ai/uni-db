/// Query rewriting framework
///
/// This module provides a general-purpose framework for transforming function calls
/// into equivalent predicate expressions at compile time. The framework enables:
///
/// - Full predicate pushdown to storage
/// - Index utilization
/// - Extensible plugin architecture for adding new rewrite rules
///
/// # Architecture
///
/// The framework consists of:
///
/// - **RewriteRule trait**: Interface for implementing rewrite transformations
/// - **RewriteRegistry**: Global registry of all rewrite rules
/// - **ExpressionWalker**: Traverses expression trees and applies rules
/// - **RewriteContext**: Contextual information during rewriting
///
/// # Example Usage
///
/// ```ignore
/// use uni_query::rewrite::{rewrite_statement, get_stats};
///
/// // Rewrite a complete query
/// let rewritten_stmt = rewrite_statement(stmt)?;
///
/// // Get statistics
/// let stats = get_stats();
/// println!("Rewrites applied: {}", stats.functions_rewritten);
/// ```
///
/// # Adding New Rules
///
/// See `rules/README.md` for a guide on implementing custom rewrite rules.
pub mod context;
pub mod error;
pub mod registry;
pub mod rule;
pub mod rules;
pub mod walker;

use context::{RewriteContext, RewriteStats};
use error::RewriteError;
use registry::RewriteRegistry;
use walker::ExpressionWalker;

use std::sync::OnceLock;
use uni_cypher::ast::{Expr, Statement};

/// Global registry of rewrite rules, initialized once on first use
static GLOBAL_REGISTRY: OnceLock<RewriteRegistry> = OnceLock::new();

/// Get the global rewrite registry, initializing it if needed
fn get_or_init_registry() -> &'static RewriteRegistry {
    GLOBAL_REGISTRY.get_or_init(|| {
        tracing::info!("Initializing query rewrite framework");
        RewriteRegistry::with_builtin_rules()
    })
}

/// Log rewrite statistics if any functions were visited
fn log_rewrite_stats(stats: &RewriteStats) {
    if stats.functions_visited > 0 {
        tracing::info!(
            "Rewrite pass complete: {} functions visited, {} rewritten, {} skipped",
            stats.functions_visited,
            stats.functions_rewritten,
            stats.functions_skipped
        );

        if !stats.errors.is_empty() {
            tracing::debug!("Rewrite errors: {:?}", stats.errors);
        }
    }
}

/// Rewrite a complete query
///
/// This is the main entry point for applying query rewrites. It walks the
/// entire query tree and applies registered rewrite rules to all function calls.
///
/// # Arguments
///
/// * `query` - The query to rewrite
///
/// # Returns
///
/// The rewritten query with function calls transformed into predicates.
///
/// # Example
///
/// ```ignore
/// let query = parse_cypher("MATCH (p)-[e:EMPLOYED_BY]->(c) WHERE uni.temporal.validAt(e, 'start', 'end', datetime('2021-06-15')) RETURN c")?;
/// let rewritten = rewrite_query(query)?;
/// // The validAt function will be transformed into: e.start <= ... AND (e.end IS NULL OR e.end >= ...)
/// ```
pub fn rewrite_query(
    query: uni_cypher::ast::Query,
) -> Result<uni_cypher::ast::Query, RewriteError> {
    let registry = get_or_init_registry();
    let context = RewriteContext::default();

    let mut walker = ExpressionWalker::new(registry, context);
    let rewritten_query = walker.rewrite_query(query);

    log_rewrite_stats(&walker.context().stats);

    Ok(rewritten_query)
}

/// Rewrite a complete statement
///
/// This is a convenience function for rewriting single statements.
///
/// # Arguments
///
/// * `stmt` - The statement to rewrite
///
/// # Returns
///
/// The rewritten statement with function calls transformed into predicates.
pub fn rewrite_statement(stmt: Statement) -> Result<Statement, RewriteError> {
    let registry = get_or_init_registry();
    let context = RewriteContext::default();

    let mut walker = ExpressionWalker::new(registry, context);
    let rewritten_stmt = walker.rewrite_statement(stmt);

    log_rewrite_stats(&walker.context().stats);

    Ok(rewritten_stmt)
}

/// Rewrite a single expression (for testing/debugging)
///
/// This is useful for unit testing rewrite rules or debugging transformations
/// in isolation.
///
/// # Arguments
///
/// * `expr` - The expression to rewrite
///
/// # Returns
///
/// The rewritten expression with function calls transformed into predicates.
pub fn rewrite_expr(expr: Expr) -> Result<Expr, RewriteError> {
    let registry = get_or_init_registry();
    let context = RewriteContext::default();

    let mut walker = ExpressionWalker::new(registry, context);
    Ok(walker.rewrite_expr(expr))
}

/// Rewrite an expression with custom context
///
/// This allows providing custom configuration and tracking statistics.
///
/// # Arguments
///
/// * `expr` - The expression to rewrite
/// * `context` - The rewrite context with configuration
///
/// # Returns
///
/// A tuple of (rewritten expression, updated context with statistics)
pub fn rewrite_expr_with_context(
    expr: Expr,
    context: RewriteContext,
) -> Result<(Expr, RewriteContext), RewriteError> {
    let registry = get_or_init_registry();

    let mut walker = ExpressionWalker::new(registry, context);
    let rewritten_expr = walker.rewrite_expr(expr);
    let final_context = walker.into_context();

    Ok((rewritten_expr, final_context))
}

/// Get rewrite statistics from the global registry
///
/// This provides observability into the rewriting process, useful for
/// debugging and performance analysis.
///
/// # Returns
///
/// Statistics about rewrites performed (empty if no rewrites have run yet)
pub fn get_stats() -> RewriteStats {
    // Note: Statistics are per-walker, not global
    // This function returns empty stats - statistics should be retrieved
    // from the context after rewriting
    RewriteStats::default()
}

/// Check if a function has a registered rewrite rule
///
/// This is useful for testing and introspection.
///
/// # Arguments
///
/// * `function_name` - The fully-qualified function name (e.g., "uni.temporal.validAt")
///
/// # Returns
///
/// `true` if a rewrite rule is registered for this function
pub fn has_rewrite_rule(function_name: &str) -> bool {
    let registry = get_or_init_registry();
    registry.has_rule(function_name)
}

/// Get all registered function names
///
/// This is useful for testing and introspection.
///
/// # Returns
///
/// A list of all function names that have registered rewrite rules
pub fn registered_functions() -> Vec<String> {
    let registry = get_or_init_registry();
    registry.registered_functions()
}

#[cfg(test)]
mod tests {
    use super::*;
    use uni_cypher::ast::CypherLiteral;

    #[test]
    fn test_rewrite_expr_basic() {
        // Test that we can rewrite an expression
        let expr = Expr::Literal(CypherLiteral::Integer(42));
        let result = rewrite_expr(expr.clone()).unwrap();

        // Literals should pass through unchanged
        assert_eq!(result, expr);
    }

    #[test]
    fn test_has_rewrite_rule() {
        // Temporal rules should be registered
        assert!(has_rewrite_rule("uni.temporal.validAt"));
        assert!(has_rewrite_rule("uni.temporal.overlaps"));
        assert!(has_rewrite_rule("uni.temporal.isOngoing"));

        // Non-existent function should return false
        assert!(!has_rewrite_rule("nonexistent.function"));
    }

    #[test]
    fn test_registered_functions() {
        let functions = registered_functions();

        // Should have at least the temporal functions
        assert!(functions.len() >= 3);
        assert!(functions.contains(&"uni.temporal.validAt".to_string()));
        assert!(functions.contains(&"uni.temporal.overlaps".to_string()));
    }

    #[test]
    fn test_rewrite_with_context() {
        use context::RewriteConfig;

        let expr = Expr::Literal(CypherLiteral::Integer(42));
        let config = RewriteConfig::default().with_verbose_logging();
        let context = RewriteContext::with_config(config);

        let (result, final_context) = rewrite_expr_with_context(expr.clone(), context).unwrap();

        assert_eq!(result, expr);
        assert_eq!(final_context.stats.functions_visited, 0); // No functions in literal
    }
}
