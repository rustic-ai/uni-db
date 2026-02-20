/// Expression tree walker for applying rewrite rules
use crate::query::rewrite::context::RewriteContext;
use crate::query::rewrite::error::RewriteError;
use crate::query::rewrite::registry::RewriteRegistry;
use uni_cypher::ast::{Expr, MapProjectionItem, Query, Statement};

/// Walks expression trees and applies rewrite rules
pub struct ExpressionWalker<'a> {
    registry: &'a RewriteRegistry,
    context: RewriteContext,
}

impl<'a> ExpressionWalker<'a> {
    /// Create a new expression walker
    pub fn new(registry: &'a RewriteRegistry, context: RewriteContext) -> Self {
        Self { registry, context }
    }

    /// Get the rewrite context (for accessing statistics)
    pub fn context(&self) -> &RewriteContext {
        &self.context
    }

    /// Get a mutable reference to the rewrite context
    pub fn context_mut(&mut self) -> &mut RewriteContext {
        &mut self.context
    }

    /// Take ownership of the context (for retrieving statistics)
    pub fn into_context(self) -> RewriteContext {
        self.context
    }

    /// Rewrite a complete statement
    pub fn rewrite_statement(&mut self, stmt: Statement) -> Statement {
        Statement {
            clauses: stmt
                .clauses
                .into_iter()
                .map(|c| self.rewrite_clause(c))
                .collect(),
        }
    }

    /// Rewrite a query
    pub fn rewrite_query(&mut self, query: Query) -> Query {
        match query {
            Query::Single(stmt) => Query::Single(self.rewrite_statement(stmt)),
            Query::Union { left, right, all } => Query::Union {
                left: Box::new(self.rewrite_query(*left)),
                right: Box::new(self.rewrite_query(*right)),
                all,
            },
            Query::Schema(schema_cmd) => Query::Schema(schema_cmd),
            Query::Transaction(txn_cmd) => Query::Transaction(txn_cmd),
            Query::Explain(inner) => Query::Explain(Box::new(self.rewrite_query(*inner))),
            Query::TimeTravel { .. } => {
                unreachable!("TimeTravel should be resolved at API layer before rewriting")
            }
        }
    }

    /// Rewrite a clause
    fn rewrite_clause(&mut self, clause: uni_cypher::ast::Clause) -> uni_cypher::ast::Clause {
        use uni_cypher::ast::Clause;

        match clause {
            Clause::Match(m) => Clause::Match(self.rewrite_match_clause(m)),
            Clause::Create(c) => Clause::Create(self.rewrite_create_clause(c)),
            Clause::Return(r) => Clause::Return(self.rewrite_return_clause(r)),
            Clause::With(w) => Clause::With(self.rewrite_with_clause(w)),
            Clause::Unwind(u) => Clause::Unwind(self.rewrite_unwind_clause(u)),
            Clause::Set(s) => Clause::Set(self.rewrite_set_clause(s)),
            Clause::Delete(d) => Clause::Delete(self.rewrite_delete_clause(d)),
            Clause::Remove(r) => Clause::Remove(self.rewrite_remove_clause(r)),
            // Other clauses that don't contain expressions or are not yet handled
            other => other,
        }
    }

    fn rewrite_match_clause(
        &mut self,
        m: uni_cypher::ast::MatchClause,
    ) -> uni_cypher::ast::MatchClause {
        uni_cypher::ast::MatchClause {
            optional: m.optional,
            pattern: self.rewrite_pattern(m.pattern),
            where_clause: m.where_clause.map(|e| self.rewrite_expr(e)),
        }
    }

    fn rewrite_create_clause(
        &mut self,
        c: uni_cypher::ast::CreateClause,
    ) -> uni_cypher::ast::CreateClause {
        uni_cypher::ast::CreateClause {
            pattern: self.rewrite_pattern(c.pattern),
        }
    }

    fn rewrite_delete_clause(
        &mut self,
        d: uni_cypher::ast::DeleteClause,
    ) -> uni_cypher::ast::DeleteClause {
        uni_cypher::ast::DeleteClause {
            detach: d.detach,
            items: d.items.into_iter().map(|e| self.rewrite_expr(e)).collect(),
        }
    }

    fn rewrite_set_clause(&mut self, s: uni_cypher::ast::SetClause) -> uni_cypher::ast::SetClause {
        uni_cypher::ast::SetClause {
            items: s
                .items
                .into_iter()
                .map(|item| self.rewrite_set_item(item))
                .collect(),
        }
    }

    fn rewrite_set_item(&mut self, item: uni_cypher::ast::SetItem) -> uni_cypher::ast::SetItem {
        use uni_cypher::ast::SetItem;

        match item {
            SetItem::Property { expr, value } => SetItem::Property {
                expr: self.rewrite_expr(expr),
                value: self.rewrite_expr(value),
            },
            SetItem::Variable { variable, value } => SetItem::Variable {
                variable,
                value: self.rewrite_expr(value),
            },
            SetItem::VariablePlus { variable, value } => SetItem::VariablePlus {
                variable,
                value: self.rewrite_expr(value),
            },
            SetItem::Labels { variable, labels } => SetItem::Labels { variable, labels },
        }
    }

    fn rewrite_remove_clause(
        &mut self,
        r: uni_cypher::ast::RemoveClause,
    ) -> uni_cypher::ast::RemoveClause {
        uni_cypher::ast::RemoveClause {
            items: r
                .items
                .into_iter()
                .map(|item| self.rewrite_remove_item(item))
                .collect(),
        }
    }

    fn rewrite_remove_item(
        &mut self,
        item: uni_cypher::ast::RemoveItem,
    ) -> uni_cypher::ast::RemoveItem {
        use uni_cypher::ast::RemoveItem;

        match item {
            RemoveItem::Property(expr) => RemoveItem::Property(self.rewrite_expr(expr)),
            RemoveItem::Labels { variable, labels } => RemoveItem::Labels { variable, labels },
        }
    }

    fn rewrite_unwind_clause(
        &mut self,
        u: uni_cypher::ast::UnwindClause,
    ) -> uni_cypher::ast::UnwindClause {
        uni_cypher::ast::UnwindClause {
            expr: self.rewrite_expr(u.expr),
            variable: u.variable,
        }
    }

    fn rewrite_pattern(&mut self, pattern: uni_cypher::ast::Pattern) -> uni_cypher::ast::Pattern {
        uni_cypher::ast::Pattern {
            paths: pattern
                .paths
                .into_iter()
                .map(|path| self.rewrite_path_pattern(path))
                .collect(),
        }
    }

    fn rewrite_path_pattern(
        &mut self,
        path: uni_cypher::ast::PathPattern,
    ) -> uni_cypher::ast::PathPattern {
        uni_cypher::ast::PathPattern {
            variable: path.variable,
            elements: path
                .elements
                .into_iter()
                .map(|elem| self.rewrite_pattern_element(elem))
                .collect(),
            shortest_path_mode: path.shortest_path_mode,
        }
    }

    fn rewrite_pattern_element(
        &mut self,
        elem: uni_cypher::ast::PatternElement,
    ) -> uni_cypher::ast::PatternElement {
        use uni_cypher::ast::PatternElement;

        match elem {
            PatternElement::Node(node) => PatternElement::Node(uni_cypher::ast::NodePattern {
                variable: node.variable,
                labels: node.labels,
                properties: node.properties.map(|expr| self.rewrite_expr(expr)),
                where_clause: node.where_clause.map(|expr| self.rewrite_expr(expr)),
            }),
            PatternElement::Relationship(rel) => {
                PatternElement::Relationship(uni_cypher::ast::RelationshipPattern {
                    variable: rel.variable,
                    types: rel.types,
                    direction: rel.direction,
                    properties: rel.properties.map(|expr| self.rewrite_expr(expr)),
                    range: rel.range,
                    where_clause: rel.where_clause.map(|expr| self.rewrite_expr(expr)),
                })
            }
            PatternElement::Parenthesized { pattern, range } => PatternElement::Parenthesized {
                pattern: Box::new(self.rewrite_path_pattern(*pattern)),
                range,
            },
        }
    }

    fn rewrite_order_by(
        &mut self,
        order_by: Option<Vec<uni_cypher::ast::SortItem>>,
    ) -> Option<Vec<uni_cypher::ast::SortItem>> {
        order_by.map(|items| {
            items
                .into_iter()
                .map(|item| uni_cypher::ast::SortItem {
                    expr: self.rewrite_expr(item.expr),
                    ascending: item.ascending,
                })
                .collect()
        })
    }

    fn rewrite_return_clause(
        &mut self,
        r: uni_cypher::ast::ReturnClause,
    ) -> uni_cypher::ast::ReturnClause {
        uni_cypher::ast::ReturnClause {
            distinct: r.distinct,
            items: r
                .items
                .into_iter()
                .map(|item| self.rewrite_return_item(item))
                .collect(),
            order_by: self.rewrite_order_by(r.order_by),
            skip: r.skip.map(|e| self.rewrite_expr(e)),
            limit: r.limit.map(|e| self.rewrite_expr(e)),
        }
    }

    fn rewrite_return_item(
        &mut self,
        item: uni_cypher::ast::ReturnItem,
    ) -> uni_cypher::ast::ReturnItem {
        use uni_cypher::ast::ReturnItem;

        match item {
            ReturnItem::All => ReturnItem::All,
            ReturnItem::Expr {
                expr,
                alias,
                source_text,
            } => ReturnItem::Expr {
                expr: self.rewrite_expr(expr),
                alias,
                source_text,
            },
        }
    }

    fn rewrite_with_clause(
        &mut self,
        w: uni_cypher::ast::WithClause,
    ) -> uni_cypher::ast::WithClause {
        uni_cypher::ast::WithClause {
            distinct: w.distinct,
            items: w
                .items
                .into_iter()
                .map(|item| self.rewrite_return_item(item))
                .collect(),
            order_by: self.rewrite_order_by(w.order_by),
            skip: w.skip.map(|e| self.rewrite_expr(e)),
            limit: w.limit.map(|e| self.rewrite_expr(e)),
            where_clause: w.where_clause.map(|e| self.rewrite_expr(e)),
        }
    }

    /// Walk and rewrite an expression tree
    pub fn rewrite_expr(&mut self, expr: Expr) -> Expr {
        match expr {
            Expr::PatternComprehension {
                path_variable,
                pattern,
                where_clause,
                map_expr,
            } => Expr::PatternComprehension {
                path_variable,
                pattern, // Pattern structure doesn't need rewriting
                where_clause: where_clause.map(|e| Box::new(self.rewrite_expr(*e))),
                map_expr: Box::new(self.rewrite_expr(*map_expr)),
            },
            // TODO: Recurse into CollectSubquery inner query for consistency
            // with Exists and CountSubquery handling below
            Expr::CollectSubquery(_) => expr,
            // Try to rewrite function calls
            Expr::FunctionCall {
                name,
                args,
                distinct,
                window_spec,
            } => self.try_rewrite_function(name, args, distinct, window_spec),

            // Recursively handle all other expression variants
            Expr::BinaryOp { left, op, right } => Expr::BinaryOp {
                left: Box::new(self.rewrite_expr(*left)),
                op,
                right: Box::new(self.rewrite_expr(*right)),
            },

            Expr::UnaryOp { op, expr } => Expr::UnaryOp {
                op,
                expr: Box::new(self.rewrite_expr(*expr)),
            },

            Expr::Property(expr, prop) => Expr::Property(Box::new(self.rewrite_expr(*expr)), prop),

            Expr::List(exprs) => {
                Expr::List(exprs.into_iter().map(|e| self.rewrite_expr(e)).collect())
            }

            Expr::Map(entries) => Expr::Map(
                entries
                    .into_iter()
                    .map(|(k, v)| (k, self.rewrite_expr(v)))
                    .collect(),
            ),

            Expr::Case {
                expr,
                when_then,
                else_expr,
            } => Expr::Case {
                expr: expr.map(|e| Box::new(self.rewrite_expr(*e))),
                when_then: when_then
                    .into_iter()
                    .map(|(w, t)| (self.rewrite_expr(w), self.rewrite_expr(t)))
                    .collect(),
                else_expr: else_expr.map(|e| Box::new(self.rewrite_expr(*e))),
            },

            Expr::Exists {
                query,
                from_pattern_predicate,
            } => Expr::Exists {
                query: Box::new(self.rewrite_query(*query)),
                from_pattern_predicate,
            },

            Expr::CountSubquery(query) => Expr::CountSubquery(Box::new(self.rewrite_query(*query))),

            Expr::IsNull(expr) => Expr::IsNull(Box::new(self.rewrite_expr(*expr))),

            Expr::IsNotNull(expr) => Expr::IsNotNull(Box::new(self.rewrite_expr(*expr))),

            Expr::IsUnique(expr) => Expr::IsUnique(Box::new(self.rewrite_expr(*expr))),

            Expr::In { expr, list } => Expr::In {
                expr: Box::new(self.rewrite_expr(*expr)),
                list: Box::new(self.rewrite_expr(*list)),
            },

            Expr::ArrayIndex { array, index } => Expr::ArrayIndex {
                array: Box::new(self.rewrite_expr(*array)),
                index: Box::new(self.rewrite_expr(*index)),
            },

            Expr::ArraySlice { array, start, end } => Expr::ArraySlice {
                array: Box::new(self.rewrite_expr(*array)),
                start: start.map(|e| Box::new(self.rewrite_expr(*e))),
                end: end.map(|e| Box::new(self.rewrite_expr(*e))),
            },

            Expr::Quantifier {
                quantifier,
                variable,
                list,
                predicate,
            } => Expr::Quantifier {
                quantifier,
                variable,
                list: Box::new(self.rewrite_expr(*list)),
                predicate: Box::new(self.rewrite_expr(*predicate)),
            },

            Expr::Reduce {
                accumulator,
                init,
                variable,
                list,
                expr,
            } => Expr::Reduce {
                accumulator,
                init: Box::new(self.rewrite_expr(*init)),
                variable,
                list: Box::new(self.rewrite_expr(*list)),
                expr: Box::new(self.rewrite_expr(*expr)),
            },

            Expr::ListComprehension {
                variable,
                list,
                where_clause,
                map_expr,
            } => Expr::ListComprehension {
                variable,
                list: Box::new(self.rewrite_expr(*list)),
                where_clause: where_clause.map(|e| Box::new(self.rewrite_expr(*e))),
                map_expr: Box::new(self.rewrite_expr(*map_expr)),
            },

            Expr::ValidAt {
                entity,
                timestamp,
                start_prop,
                end_prop,
            } => Expr::ValidAt {
                entity: Box::new(self.rewrite_expr(*entity)),
                timestamp: Box::new(self.rewrite_expr(*timestamp)),
                start_prop,
                end_prop,
            },

            Expr::MapProjection { base, items } => Expr::MapProjection {
                base: Box::new(self.rewrite_expr(*base)),
                items: items
                    .into_iter()
                    .map(|item| match item {
                        MapProjectionItem::LiteralEntry(k, v) => {
                            MapProjectionItem::LiteralEntry(k, Box::new(self.rewrite_expr(*v)))
                        }
                        other => other,
                    })
                    .collect(),
            },

            Expr::LabelCheck { expr, labels } => Expr::LabelCheck {
                expr: Box::new(self.rewrite_expr(*expr)),
                labels,
            },

            // Leaf nodes - no rewriting needed
            Expr::Literal(_) | Expr::Parameter(_) | Expr::Variable(_) | Expr::Wildcard => expr,
        }
    }

    /// Try to rewrite a function call
    fn try_rewrite_function(
        &mut self,
        name: String,
        args: Vec<Expr>,
        distinct: bool,
        window_spec: Option<uni_cypher::ast::WindowSpec>,
    ) -> Expr {
        // First, recursively rewrite arguments
        let rewritten_args: Vec<Expr> =
            args.into_iter().map(|arg| self.rewrite_expr(arg)).collect();

        // Record that we visited this function
        self.context.stats.record_visit();

        // Helper to construct fallback function call
        let make_fallback = |name, args| Expr::FunctionCall {
            name,
            args,
            distinct,
            window_spec: window_spec.clone(),
        };

        // Check if we have a rewrite rule for this function
        let Some(rule) = self.registry.get_rule(&name) else {
            return make_fallback(name, rewritten_args);
        };

        // Validate arguments
        if let Err(e) = rule.validate_args(&rewritten_args) {
            self.context.stats.record_failure(&name, e);
            if self.context.config.verbose_logging {
                tracing::debug!(
                    "Rewrite validation failed for {}: {:?}",
                    name,
                    self.context.stats.errors.last()
                );
            }
            return make_fallback(name, rewritten_args);
        }

        // Check if rule is applicable in current context
        if !rule.is_applicable(&self.context) {
            let error = RewriteError::NotApplicable {
                reason: "Context requirements not met".to_string(),
            };
            self.context.stats.record_failure(&name, error);
            if self.context.config.verbose_logging {
                tracing::debug!("Rewrite not applicable for {}", name);
            }
            return make_fallback(name, rewritten_args);
        }

        // Apply rewrite
        match rule.rewrite(rewritten_args.clone(), &self.context) {
            Ok(rewritten_expr) => {
                self.context.stats.record_success(&name);
                if self.context.config.verbose_logging {
                    tracing::debug!("Rewrote function call: {} -> {:?}", name, rewritten_expr);
                } else {
                    tracing::info!("Rewrote function: {}", name);
                }
                rewritten_expr
            }
            Err(e) => {
                self.context.stats.record_failure(&name, e);
                if self.context.config.verbose_logging {
                    tracing::debug!(
                        "Rewrite failed for {}: {:?}",
                        name,
                        self.context.stats.errors.last()
                    );
                }
                make_fallback(name, rewritten_args)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query::rewrite::context::RewriteConfig;
    use uni_cypher::ast::CypherLiteral;

    #[test]
    fn test_walker_visits_nested_expressions() {
        let registry = RewriteRegistry::new();
        let config = RewriteConfig::default();
        let mut walker = ExpressionWalker::new(&registry, RewriteContext::with_config(config));

        // Nested expression with function calls
        let expr = Expr::BinaryOp {
            left: Box::new(Expr::FunctionCall {
                name: "func1".into(),
                args: vec![Expr::Literal(CypherLiteral::Integer(1))],
                distinct: false,
                window_spec: None,
            }),
            op: uni_cypher::ast::BinaryOp::And,
            right: Box::new(Expr::FunctionCall {
                name: "func2".into(),
                args: vec![Expr::Literal(CypherLiteral::Integer(2))],
                distinct: false,
                window_spec: None,
            }),
        };

        let _ = walker.rewrite_expr(expr);

        // Both function calls should have been visited
        assert_eq!(walker.context().stats.functions_visited, 2);
    }

    #[test]
    fn test_walker_fallback_without_rules() {
        let registry = RewriteRegistry::new();
        let config = RewriteConfig::default();
        let mut walker = ExpressionWalker::new(&registry, RewriteContext::with_config(config));

        let original = Expr::FunctionCall {
            name: "unknown".into(),
            args: vec![Expr::Literal(CypherLiteral::Integer(1))],
            distinct: false,
            window_spec: None,
        };

        let rewritten = walker.rewrite_expr(original.clone());

        // Should return unchanged (but with potentially rewritten arguments)
        assert!(matches!(rewritten, Expr::FunctionCall { name, .. } if name == "unknown"));
        assert_eq!(walker.context().stats.functions_visited, 1);
    }
}
