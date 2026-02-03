/// Built-in rewrite rules
pub mod temporal;

use crate::query::rewrite::registry::RewriteRegistry;
use std::sync::Arc;

/// Register all built-in rewrite rules
pub fn register_builtin_rules(registry: &mut RewriteRegistry) {
    // Register temporal rules
    registry.register(Arc::new(temporal::ValidAtRule));
    registry.register(Arc::new(temporal::OverlapsRule));
    registry.register(Arc::new(temporal::PrecedesRule));
    registry.register(Arc::new(temporal::SucceedsRule));
    registry.register(Arc::new(temporal::IsOngoingRule));
    registry.register(Arc::new(temporal::HasClosedRule));

    tracing::debug!("Registered {} built-in rewrite rules", registry.len());
}
