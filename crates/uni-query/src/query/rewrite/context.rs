/// Rewrite context and configuration
use crate::query::rewrite::error::RewriteError;
use std::collections::HashMap;

/// Contextual information available during query rewriting
///
/// The context provides information about the current query environment,
/// including variable scope, schema metadata, and configuration options.
#[derive(Default)]
pub struct RewriteContext {
    /// Variables currently in scope (from MATCH, WITH, etc.)
    pub scope: HashMap<String, VariableInfo>,

    /// Rewrite statistics (for observability)
    pub stats: RewriteStats,

    /// Configuration flags
    pub config: RewriteConfig,
}

impl RewriteContext {
    /// Create a new rewrite context with default configuration
    pub fn new() -> Self {
        Self::default()
    }

    /// Create a new rewrite context with custom configuration
    pub fn with_config(config: RewriteConfig) -> Self {
        Self {
            scope: HashMap::new(),
            stats: RewriteStats::default(),
            config,
        }
    }

    /// Get information about a variable in scope
    pub fn get_variable(&self, name: &str) -> Option<&VariableInfo> {
        self.scope.get(name)
    }

    /// Add a variable to the scope
    pub fn add_variable(&mut self, name: String, info: VariableInfo) {
        self.scope.insert(name, info);
    }
}

/// Information about a variable in scope
#[derive(Debug, Clone)]
pub struct VariableInfo {
    /// Variable name
    pub name: String,

    /// Label (for nodes) or None
    pub label: Option<String>,

    /// Whether this is an edge (true) or node (false)
    pub is_edge: bool,

    /// Known properties with their types
    pub properties: HashMap<String, PropertyType>,
}

/// Property type information
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PropertyType {
    /// String property
    String,

    /// Integer property
    Integer,

    /// Float property
    Float,

    /// Boolean property
    Boolean,

    /// DateTime property
    DateTime,

    /// List of values
    List,

    /// Map/object
    Map,

    /// Unknown or dynamic type
    Unknown,
}

/// Statistics collected during rewriting
#[derive(Debug, Default, Clone)]
pub struct RewriteStats {
    /// Total number of function calls visited
    pub functions_visited: usize,

    /// Number of functions successfully rewritten
    pub functions_rewritten: usize,

    /// Number of functions that fell back to scalar execution
    pub functions_skipped: usize,

    /// Errors encountered during rewriting (non-fatal)
    pub errors: Vec<RewriteError>,

    /// Per-rule statistics
    pub rule_stats: HashMap<String, RuleStats>,
}

impl RewriteStats {
    /// Get or create rule stats entry
    fn rule_stats_mut(&mut self, rule_name: &str) -> &mut RuleStats {
        self.rule_stats.entry(rule_name.to_string()).or_default()
    }

    /// Record a successful rewrite for a rule
    pub fn record_success(&mut self, rule_name: &str) {
        self.functions_rewritten += 1;
        self.rule_stats_mut(rule_name).record_success();
    }

    /// Record a failed rewrite for a rule
    pub fn record_failure(&mut self, rule_name: &str, error: RewriteError) {
        self.functions_skipped += 1;
        self.rule_stats_mut(rule_name).record_failure(error.clone());
        self.errors.push(error);
    }

    /// Record a visited function
    pub fn record_visit(&mut self) {
        self.functions_visited += 1;
    }
}

/// Per-rule statistics
#[derive(Debug, Default, Clone)]
pub struct RuleStats {
    /// Number of times this rule was attempted
    pub attempts: usize,

    /// Number of successful rewrites
    pub successes: usize,

    /// Failure counts by error type
    pub failures: HashMap<String, usize>,
}

impl RuleStats {
    fn record_success(&mut self) {
        self.attempts += 1;
        self.successes += 1;
    }

    fn record_failure(&mut self, error: RewriteError) {
        self.attempts += 1;
        let error_key = format!("{:?}", error);
        *self.failures.entry(error_key).or_insert(0) += 1;
    }
}

/// Configuration options for query rewriting
#[derive(Debug, Clone)]
pub struct RewriteConfig {
    /// Enable temporal function rewrites
    pub enable_temporal: bool,

    /// Enable spatial function rewrites (future)
    pub enable_spatial: bool,

    /// Enable property access rewrites (future)
    pub enable_property: bool,

    /// Whether to fall back to scalar execution on rewrite failure
    pub fallback_to_scalar: bool,

    /// Enable verbose logging of rewrite operations
    pub verbose_logging: bool,
}

impl Default for RewriteConfig {
    fn default() -> Self {
        Self {
            enable_temporal: true,
            enable_spatial: false,
            enable_property: false,
            fallback_to_scalar: true,
            verbose_logging: false,
        }
    }
}

impl RewriteConfig {
    /// Create a config with all rewrites enabled
    pub fn all_enabled() -> Self {
        Self {
            enable_temporal: true,
            enable_spatial: true,
            enable_property: true,
            fallback_to_scalar: true,
            verbose_logging: false,
        }
    }

    /// Create a config with all rewrites disabled
    pub fn all_disabled() -> Self {
        Self {
            enable_temporal: false,
            enable_spatial: false,
            enable_property: false,
            fallback_to_scalar: true,
            verbose_logging: false,
        }
    }

    /// Enable verbose logging
    pub fn with_verbose_logging(mut self) -> Self {
        self.verbose_logging = true;
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_context_default() {
        let ctx = RewriteContext::default();
        assert!(ctx.scope.is_empty());
        assert_eq!(ctx.stats.functions_visited, 0);
        assert!(ctx.config.enable_temporal);
    }

    #[test]
    fn test_stats_recording() {
        let mut stats = RewriteStats::default();

        stats.record_success("test.func");
        assert_eq!(stats.functions_rewritten, 1);
        assert_eq!(stats.rule_stats.get("test.func").unwrap().successes, 1);

        stats.record_failure(
            "test.func",
            RewriteError::NotApplicable {
                reason: "test".into(),
            },
        );
        assert_eq!(stats.functions_skipped, 1);
        assert_eq!(stats.errors.len(), 1);
    }

    #[test]
    fn test_config_builders() {
        let all_enabled = RewriteConfig::all_enabled();
        assert!(all_enabled.enable_temporal);
        assert!(all_enabled.enable_spatial);
        assert!(all_enabled.enable_property);

        let all_disabled = RewriteConfig::all_disabled();
        assert!(!all_disabled.enable_temporal);
        assert!(!all_disabled.enable_spatial);
        assert!(!all_disabled.enable_property);
    }
}
