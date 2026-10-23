//! Built-in trigger registrations.
//!
//! M5f reference: a working `TriggerPlugin` example that audits
//! mutation events on a configurable label allow-list. Demonstrates
//! the subscription/selector model from proposal §4.18, the
//! `fire(ctx, events)` invocation surface, and the
//! `Continue`/`Reject`/`Defer` outcome alternatives.
//!
//! The trigger ships in `Async` mode (fires after commit; cannot
//! reject) so it has zero impact on the writer's hot path. A
//! `Synchronous` variant would be appropriate for guardrails that
//! must abort the transaction when invariants are violated.

use std::sync::Arc;

use parking_lot::RwLock;
use uni_plugin::traits::trigger::{
    FireMode, MutationBatch, TriggerContext, TriggerEventMask, TriggerOutcome, TriggerPhase,
    TriggerPlugin, TriggerSubscription,
};
use uni_plugin::{FnError, PluginError, PluginRegistrar};

/// Register the built-in triggers into `r`.
///
/// Currently registers a single `LabelAuditTrigger` matching any
/// `_AuditMe`-labeled node mutation. Real deployments override or
/// drop this trigger via the `apoc.trigger.drop`-style meta-procedure
/// (M9 follow-up).
///
/// # Errors
///
/// Returns [`PluginError`] if registration fails.
pub fn register_into(r: &mut PluginRegistrar<'_>) -> Result<(), PluginError> {
    // The bare label: event rows carry it bare. The router also accepts the
    // Cypher form `:_AuditMe`, but the shipped subscription should not lean
    // on that.
    r.trigger(Arc::new(LabelAuditTrigger::new("_AuditMe")))?;
    Ok(())
}

/// Async audit trigger that counts mutations against a labeled node.
///
/// Real-world uses include compliance audit trails (record every change
/// to a `:Customer` for SOC2-style logging) and metrics emission.
/// This reference impl just maintains an in-memory counter the host
/// (or tests) can read via [`Self::events_seen`].
#[derive(Debug)]
pub struct LabelAuditTrigger {
    subscription: TriggerSubscription,
    /// Mutations matched, by transaction id. RwLock for cheap reads in
    /// observability paths.
    events: Arc<RwLock<u64>>,
}

impl LabelAuditTrigger {
    /// Construct a trigger watching `label`.
    ///
    /// `label` is a bare label name (`"Customer"`); the Cypher form
    /// (`":Customer"`) names the same label.
    #[must_use]
    pub fn new(label: &str) -> Self {
        let mask = TriggerEventMask::NODE_CREATE
            .union(TriggerEventMask::NODE_UPDATE)
            .union(TriggerEventMask::NODE_DELETE);
        Self {
            subscription: TriggerSubscription {
                phase: TriggerPhase::AfterMutation,
                events: mask,
                labels: Some(vec![smol_str::SmolStr::new(label)]),
                edge_types: None,
                properties: None,
                predicate_source: None,
                fire_mode: FireMode::Async,
                docs: format!(
                    "Audit-counter trigger — increments on every node \
                     create/update/delete touching `{label}`."
                ),
            },
            events: Arc::new(RwLock::new(0)),
        }
    }

    /// Total events this trigger has observed since construction.
    #[must_use]
    pub fn events_seen(&self) -> u64 {
        *self.events.read()
    }

    /// Reset the counter (test helper).
    pub fn reset(&self) {
        *self.events.write() = 0;
    }
}

impl TriggerPlugin for LabelAuditTrigger {
    fn subscription(&self) -> &TriggerSubscription {
        &self.subscription
    }

    fn fire(
        &self,
        _ctx: TriggerContext<'_>,
        events: &MutationBatch,
    ) -> Result<TriggerOutcome, FnError> {
        // The host filtered the batch through the subscription's
        // selectors already, so every row here matched. Add the row
        // count to the audit counter.
        let n = events.events.num_rows() as u64;
        let mut guard = self.events.write();
        *guard = guard.saturating_add(n);
        Ok(TriggerOutcome::Continue)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Int64Array, RecordBatch};
    use arrow_schema::{DataType, Field, Schema};
    use std::sync::Arc;

    fn make_mutation_batch(row_count: usize) -> MutationBatch {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "event_kind",
            DataType::Int64,
            false,
        )]));
        let arr: Arc<dyn arrow_array::Array> = Arc::new(Int64Array::from(vec![1_i64; row_count]));
        let batch = RecordBatch::try_new(schema, vec![arr]).unwrap();
        MutationBatch {
            events: Arc::new(batch),
        }
    }

    #[test]
    fn subscription_targets_audit_label_and_node_events() {
        let t = LabelAuditTrigger::new("_AuditMe");
        let s = t.subscription();
        assert_eq!(s.phase, TriggerPhase::AfterMutation);
        assert_eq!(s.fire_mode, FireMode::Async);
        assert_eq!(
            s.labels.as_deref(),
            Some(&[smol_str::SmolStr::new("_AuditMe")][..])
        );
        assert!(s.events.contains(TriggerEventMask::NODE_CREATE));
        assert!(s.events.contains(TriggerEventMask::NODE_UPDATE));
        assert!(s.events.contains(TriggerEventMask::NODE_DELETE));
        // Edge events are NOT in the mask.
        assert!(!s.events.contains(TriggerEventMask::EDGE_CREATE));
    }

    #[test]
    fn fire_increments_counter_by_batch_row_count() {
        let t = LabelAuditTrigger::new(":Audit");
        let ctx = TriggerContext::new("test-session", 42);
        let batch = make_mutation_batch(3);
        let outcome = t.fire(ctx, &batch).unwrap();
        assert!(matches!(outcome, TriggerOutcome::Continue));
        assert_eq!(t.events_seen(), 3);

        // A second fire accumulates.
        let ctx2 = TriggerContext::new("test-session", 43);
        t.fire(ctx2, &make_mutation_batch(2)).unwrap();
        assert_eq!(t.events_seen(), 5);
    }

    #[test]
    fn reset_clears_counter() {
        let t = LabelAuditTrigger::new(":Audit");
        let ctx = TriggerContext::new("test-session", 1);
        t.fire(ctx, &make_mutation_batch(5)).unwrap();
        assert_eq!(t.events_seen(), 5);
        t.reset();
        assert_eq!(t.events_seen(), 0);
    }
}
