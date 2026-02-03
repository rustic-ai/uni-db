/// Global registry for rewrite rules
use crate::query::rewrite::rule::RewriteRule;
use std::collections::HashMap;
use std::sync::Arc;

/// Registry for all rewrite rules
///
/// The registry maintains a map from function names to their rewrite rules.
/// It is initialized once at startup with all built-in rules and can be
/// extended with custom rules.
pub struct RewriteRegistry {
    /// Map from function name to rewrite rule
    rules: HashMap<String, Arc<dyn RewriteRule>>,
}

impl RewriteRegistry {
    /// Create a new empty registry
    pub fn new() -> Self {
        Self {
            rules: HashMap::new(),
        }
    }

    /// Create a registry with all built-in rules registered
    pub fn with_builtin_rules() -> Self {
        let mut registry = Self::new();
        crate::query::rewrite::rules::register_builtin_rules(&mut registry);
        registry
    }

    /// Register a new rewrite rule
    ///
    /// If a rule with the same function name already exists, it will be replaced.
    pub fn register(&mut self, rule: Arc<dyn RewriteRule>) {
        let function_name = rule.function_name().to_string();
        tracing::debug!("Registering rewrite rule: {}", function_name);
        self.rules.insert(function_name, rule);
    }

    /// Get the rewrite rule for a function name
    pub fn get_rule(&self, function_name: &str) -> Option<&dyn RewriteRule> {
        self.rules.get(function_name).map(|r| r.as_ref())
    }

    /// Check if a function has a rewrite rule
    pub fn has_rule(&self, function_name: &str) -> bool {
        self.rules.contains_key(function_name)
    }

    /// Get all registered function names
    pub fn registered_functions(&self) -> Vec<String> {
        self.rules.keys().cloned().collect()
    }

    /// Get the number of registered rules
    pub fn len(&self) -> usize {
        self.rules.len()
    }

    /// Check if the registry is empty
    pub fn is_empty(&self) -> bool {
        self.rules.is_empty()
    }
}

impl Default for RewriteRegistry {
    fn default() -> Self {
        Self::with_builtin_rules()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query::rewrite::context::RewriteContext;
    use crate::query::rewrite::error::RewriteError;
    use uni_cypher::ast::{CypherLiteral, Expr};

    /// Dummy rule for testing
    struct DummyRule {
        name: String,
    }

    impl DummyRule {
        fn new(name: &str) -> Self {
            Self {
                name: name.to_string(),
            }
        }
    }

    impl RewriteRule for DummyRule {
        fn function_name(&self) -> &str {
            &self.name
        }

        fn validate_args(&self, _args: &[Expr]) -> Result<(), RewriteError> {
            Ok(())
        }

        fn rewrite(&self, args: Vec<Expr>, _ctx: &RewriteContext) -> Result<Expr, RewriteError> {
            // Just return the first argument unchanged
            Ok(args
                .into_iter()
                .next()
                .unwrap_or(Expr::Literal(CypherLiteral::Null)))
        }
    }

    #[test]
    fn test_registry_register_and_lookup() {
        let mut registry = RewriteRegistry::new();

        let rule = Arc::new(DummyRule::new("test.func"));
        registry.register(rule);

        assert!(registry.has_rule("test.func"));
        assert!(!registry.has_rule("nonexistent"));

        let retrieved = registry.get_rule("test.func");
        assert!(retrieved.is_some());
        assert_eq!(retrieved.unwrap().function_name(), "test.func");
    }

    #[test]
    fn test_registry_replacement() {
        let mut registry = RewriteRegistry::new();

        registry.register(Arc::new(DummyRule::new("test.func")));
        assert_eq!(registry.len(), 1);

        // Register again with same name - should replace
        registry.register(Arc::new(DummyRule::new("test.func")));
        assert_eq!(registry.len(), 1);
    }

    #[test]
    fn test_registry_registered_functions() {
        let mut registry = RewriteRegistry::new();

        registry.register(Arc::new(DummyRule::new("func1")));
        registry.register(Arc::new(DummyRule::new("func2")));
        registry.register(Arc::new(DummyRule::new("func3")));

        let functions = registry.registered_functions();
        assert_eq!(functions.len(), 3);
        assert!(functions.contains(&"func1".to_string()));
        assert!(functions.contains(&"func2".to_string()));
        assert!(functions.contains(&"func3".to_string()));
    }
}
