// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

use super::core::*;
use crate::query::df_graph::mutation_common::Prefetch;
use crate::query::planner::LogicalPlan;
use anyhow::{Result, anyhow};
use std::borrow::Cow;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use uni_common::DataType;
use uni_common::core::id::{Eid, Vid};
use uni_common::core::schema::{Constraint, ConstraintTarget, ConstraintType};
use uni_common::{Path, Value};
use uni_cypher::ast::{
    AlterAction, AlterEdgeType, AlterLabel, BinaryOp, ConstraintType as AstConstraintType,
    CreateConstraint, CreateEdgeType, CreateLabel, CypherLiteral, Direction, DropConstraint,
    DropEdgeType, DropLabel, Expr, NodePattern, Pattern, PatternElement, RemoveItem, SetClause,
    SetItem,
};
use uni_store::QueryContext;
use uni_store::backend::types::{FilterExpr, Scalar};
use uni_store::runtime::l0::StatEntity;
use uni_store::runtime::l0_visibility;
use uni_store::runtime::property_manager::PropertyManager;
use uni_store::runtime::writer::Writer;
use uni_store::storage::declared_values::DeclaredEntity;

/// Whether `key` is a user property for side-effect counting: the storage
/// columns and `ext_id` are not observable as properties (they are excluded
/// from `properties(n)` the same way).
fn is_counted_property(key: &str) -> bool {
    !uni_common::core::schema::is_reserved_property_name(key) && key != "ext_id"
}

/// The `(before, after)` pairs of every user property whose value differs
/// between two full rows of one entity. A `Null` value is an absent property.
fn row_property_effects(
    before: &HashMap<String, Value>,
    after: &HashMap<String, Value>,
) -> Vec<(Option<Value>, Option<Value>)> {
    let present = |m: &HashMap<String, Value>, k: &str| m.get(k).filter(|v| !v.is_null()).cloned();
    before
        .keys()
        .chain(after.keys().filter(|k| !before.contains_key(*k)))
        .filter(|k| is_counted_property(k))
        .filter_map(|k| {
            let (b, a) = (present(before, k), present(after, k));
            (b != a).then_some((b, a))
        })
        .collect()
}

/// The `(before, None)` pairs of the named properties `props` holds.
fn removal_effects(
    props: &HashMap<String, Value>,
    names: &[String],
) -> Vec<(Option<Value>, Option<Value>)> {
    let mut seen = HashSet::new();
    names
        .iter()
        .filter(|n| seen.insert(n.as_str()))
        .filter_map(|n| props.get(n).filter(|v| !v.is_null()))
        .map(|v| (Some(v.clone()), None))
        .collect()
}

/// Canonical, hashable key for a single-node MERGE: the key properties as a
/// `(name, value)` list sorted by name. Used to group existing vertices by key
/// for the index fast path (issue #69).
type MergeKey = Vec<(String, Value)>;

/// Identity fields extracted from a map-encoded edge.
struct EdgeIdentity {
    eid: Eid,
    src: Vid,
    dst: Vid,
    edge_type_id: u32,
}

/// Per-variable accumulator for SetItem::Property items targeting a vertex.
///
/// Built lazily on the first SetItem touching each variable, then mutated
/// in place across subsequent items. Flushed once at end of the SET
/// clause (or earlier if a non-Property SetItem on the same var lands).
struct PendingVertexSet {
    vid: Vid,
    labels: Vec<String>,
    /// Full property map (storage union L0 from
    /// `get_all_vertex_props_with_ctx` plus the touched values applied
    /// in-order). Written to L0 whole, as a full row.
    props: HashMap<String, Value>,
    /// Property keys this SET assigned. Drives the auto-embed refresh of
    /// targets whose source column changed.
    touched: HashSet<String>,
    /// Each assigned key's value before its first assignment in this SET
    /// (`None` = absent), for the statement's property side effects.
    before: HashMap<String, Option<Value>>,
}

/// Per-variable accumulator for SetItem::Property items targeting an edge.
struct PendingEdgeSet {
    src: Vid,
    dst: Vid,
    edge_type_id: u32,
    eid: Eid,
    edge_type_name: String,
    props: HashMap<String, Value>,
    /// Each assigned key's value before its first assignment in this SET
    /// (`None` = absent), for the statement's property side effects.
    before: HashMap<String, Option<Value>>,
}

/// Refuse to mutate an ephemeral node (M5g / proposal §4.13.1).
/// Ephemeral entities are return-only — `Vid::EPHEMERAL_BIT` is set on
/// any id minted by `host.allocate_transient_id()`.
fn reject_if_ephemeral_vid(vid: Vid) -> Result<()> {
    if vid.is_ephemeral() {
        return Err(anyhow::Error::from(
            uni_common::UniError::EphemeralWriteAttempt {
                kind: "node",
                id: vid.transient_id().unwrap_or(vid.as_u64()),
            },
        ));
    }
    Ok(())
}

/// Returns a short variant name for a `Value`, used in type-mismatch error messages.
fn value_type_name(val: &Value) -> &'static str {
    match val {
        Value::Null => "Null",
        Value::Bool(_) => "Bool",
        Value::Int(_) => "Int",
        Value::Float(_) => "Float",
        Value::String(_) => "String",
        Value::Bytes(_) => "Bytes",
        Value::List(_) => "List",
        Value::Map(_) => "Map",
        Value::Node(_) => "Node",
        Value::Edge(_) => "Edge",
        Value::Path(_) => "Path",
        Value::Vector(_) => "Vector",
        Value::Temporal(_) => "Temporal",
        _ => "value",
    }
}

/// Refuse to mutate an ephemeral edge (M5g / proposal §4.13.1).
fn reject_if_ephemeral_eid(eid: Eid) -> Result<()> {
    if eid.is_ephemeral() {
        return Err(anyhow::Error::from(
            uni_common::UniError::EphemeralWriteAttempt {
                kind: "edge",
                id: eid.transient_id().unwrap_or(eid.as_u64()),
            },
        ));
    }
    Ok(())
}

/// Reject a write whose target label is currently allocated as a
/// virtual (catalog-backed) label.
///
/// Catalog tables are read-only from the host's perspective — there is
/// no write-back path through `CatalogTable::scan` to the originating
/// provider, so silently allowing SET/DELETE would leave ghosted state
/// on the host side that diverges from the external catalog. The
/// planner already rejects CREATE/MERGE on virtual labels via
/// `Planner::reject_virtual_label_writes`; this helper is the
/// equivalent gate on the runtime write path for SET-label-add and
/// DELETE.
///
/// `op` names the offending operation for the error message (e.g.
/// `"SET"`, `"DELETE"`).
///
/// # Errors
///
/// Returns an error if `registry` is `Some` and any name in `labels`
/// is currently registered as a virtual label. Returns `Ok(())` when
/// no plugin registry is wired (low-level callers without plugins).
fn reject_virtual_label_write(
    registry: Option<&Arc<uni_plugin::PluginRegistry>>,
    labels: &[String],
    op: &str,
) -> Result<()> {
    let Some(registry) = registry else {
        return Ok(());
    };
    for label in labels {
        if registry.virtual_label_by_name(label).is_some() {
            return Err(anyhow!(
                "Cannot {op} on virtual (catalog-resolved) label `{label}` — virtual \
                 labels are read-only; write back via the originating catalog instead"
            ));
        }
    }
    Ok(())
}

/// Reject a write whose target edge-type ID is currently allocated as
/// a virtual (catalog-backed) edge type. Runtime analog of
/// [`reject_virtual_label_write`] for the edge path.
///
/// # Errors
///
/// Returns an error if `registry` is `Some` and `edge_type_id` resolves
/// to a registered virtual edge type. Returns `Ok(())` when no plugin
/// registry is wired.
fn reject_virtual_edge_type_write(
    registry: Option<&Arc<uni_plugin::PluginRegistry>>,
    edge_type_id: u32,
    op: &str,
) -> Result<()> {
    let Some(registry) = registry else {
        return Ok(());
    };
    if let Some(entry) = registry.virtual_edge_type_by_id(edge_type_id) {
        return Err(anyhow!(
            "Cannot {op} on virtual (catalog-resolved) edge type `{}` — virtual edge \
             types are read-only; write back via the originating catalog instead",
            entry.name
        ));
    }
    Ok(())
}

/// The shape `merge_relationship_keyed_fastpath_shape` recognises.
struct MergeRelKeyedShape<'p> {
    /// The endpoint resolved from the input row.
    bound_var: String,
    /// The endpoint to find-or-create. Guaranteed a variable, one label and a
    /// non-empty static map literal.
    keyed: &'p NodePattern,
    /// `keyed`'s label, schema-cased.
    keyed_label: String,
    /// Direction to walk FROM the bound endpoint TOWARDS the keyed one.
    probe_dir: uni_store::storage::direction::Direction,
    type_id: u32,
    rel_var: Option<String>,
}

impl Executor {
    /// Extracts labels from a node value.
    ///
    /// Handles both `Value::Map` (with a `_labels` list field) and
    /// `Value::Node` (with a `labels` vec field).
    ///
    /// Returns `None` when the value is not a node or has no labels.
    pub(crate) fn extract_labels_from_node(node_val: &Value) -> Option<Vec<String>> {
        // Both encodings through one accessor. "No labels" stays `None` rather
        // than `Some(vec![])`: callers here branch on the option.
        node_val.entity_labels().filter(|l| !l.is_empty())
    }

    /// The labels `vid` carries now, as this statement sees them.
    ///
    /// A row's binding holds the labels the vertex had when the row was
    /// matched; a write through another binding of the same vertex — a
    /// second variable in the row, or the same variable in an earlier row —
    /// does not update it. Computing a label write from the binding therefore
    /// rebuilt the set from labels that no longer held: `SET a:X SET b:Y` on
    /// one vertex stored `[A, Y]`. Every write of this statement lands in the
    /// L0 chain, so that is read first (transaction buffer, then main, then
    /// pending flush); a vertex no L0 holds has not been relabelled since it
    /// was matched, so its binding is current, and the persisted label index
    /// covers a binding that carries no labels.
    fn current_vertex_labels(
        &self,
        vid: Vid,
        binding: &Value,
        ctx: Option<&QueryContext>,
    ) -> Vec<String> {
        ctx.and_then(|ctx| l0_visibility::get_vertex_labels_optional(vid, ctx))
            .or_else(|| Self::extract_labels_from_node(binding))
            .or_else(|| self.storage.get_labels_from_index(vid))
            .unwrap_or_default()
    }

    /// Gives every binding of `vid` in `row` the label set `labels`.
    ///
    /// openCypher binds a node by reference, so after `SET a:X` a second
    /// variable bound to the same node projects `X` too.
    fn set_labels_on_bindings(row: &mut HashMap<String, Value>, vid: Vid, labels: &[String]) {
        for binding in row.values_mut() {
            if binding.entity_vid() == Some(vid) {
                binding.set_entity_labels(labels.to_vec());
            }
        }
    }

    /// Extracts user-visible properties from a value that represents a node or edge.
    ///
    /// Strips internal bookkeeping keys (those prefixed with `_` or named
    /// `ext_id`) from map-encoded entities and returns only the user-facing
    /// property key-value pairs.
    ///
    /// Returns `None` when `val` is not a map, node, or edge.
    pub(crate) fn extract_user_properties_from_value(
        val: &Value,
    ) -> Option<HashMap<String, Value>> {
        match val {
            Value::Map(map) => {
                // Distinguish entity-encoded maps from plain map literals.
                // A node map has both `_vid` and `_labels`.
                // An edge map has `_eid`, `_src`, and `_dst`.
                let is_node_map = map.contains_key("_vid") && map.contains_key("_labels");
                let is_edge_map = map.contains_key("_eid")
                    && map.contains_key("_src")
                    && map.contains_key("_dst");

                if is_node_map || is_edge_map {
                    // Filter out internal bookkeeping keys
                    let user_props: HashMap<String, Value> = map
                        .iter()
                        .filter(|(k, _)| {
                            !uni_common::core::schema::is_reserved_property_name(k)
                                && k.as_str() != "ext_id"
                        })
                        .map(|(k, v)| (k.clone(), v.clone()))
                        .collect();
                    // When mutation output omits dotted property columns, user
                    // properties live inside `_all_props` rather than at the
                    // top level of the entity map.
                    if user_props.is_empty()
                        && let Some(Value::Map(all_props)) = map.get("_all_props")
                    {
                        return Some(all_props.clone());
                    }
                    Some(user_props)
                } else {
                    // Plain map literal — return as-is
                    Some(map.clone())
                }
            }
            Value::Node(node) => Some(node.properties.clone()),
            Value::Edge(edge) => Some(edge.properties.clone()),
            _ => None,
        }
    }

    /// Merge, enrich, validate and persist a vertex's properties for the
    /// whole-entity `SET n = map` / `SET n += map` forms.
    ///
    /// Returns the enriched property map so the caller can refresh its
    /// in-memory row binding (which differs between the typed `Value::Node`
    /// and the map-encoded node shapes).
    #[expect(clippy::too_many_arguments)]
    async fn write_vertex_props(
        &self,
        vid: Vid,
        labels: &[String],
        new_props: HashMap<String, Value>,
        replace: bool,
        schema: &uni_common::core::schema::Schema,
        writer: &Writer,
        prop_manager: &PropertyManager,
        params: &HashMap<String, Value>,
        ctx: Option<&QueryContext>,
        tx_l0: Option<&Arc<parking_lot::RwLock<uni_store::runtime::l0::L0Buffer>>>,
        prefetched: &Prefetch,
    ) -> Result<HashMap<String, Value>> {
        let current = read_vertex_props_with_prefetch(vid, prefetched, prop_manager, ctx).await?;
        let before = current.clone();
        let write_props = Self::merge_props(current, new_props, replace);
        let mut enriched = write_props.clone();
        for label_name in labels {
            self.enrich_properties_with_generated_columns(
                label_name,
                &mut enriched,
                prop_manager,
                params,
                ctx,
            )
            .await?;
        }
        let enriched = Self::coerce_and_validate_props(enriched, schema, labels)?;
        let effects = row_property_effects(&before, &enriched);
        let _ = writer
            .update_vertex_with_labels(vid, enriched.clone(), labels, tx_l0)
            .await?;
        writer.record_property_effects(StatEntity::Vertex(vid), &effects, tx_l0);
        Ok(enriched)
    }

    /// Applies a property map to a vertex or edge entity bound to `variable` in `row`.
    ///
    /// When `replace` is `true` the entity's property set is replaced: keys absent
    /// from `new_props` are tombstoned (written as `Value::Null`) so the storage
    /// layer removes them.  When `replace` is `false` the map is merged: keys in
    /// `new_props` are upserted, while keys absent from `new_props` are unchanged.
    /// A `Value::Null` entry in `new_props` acts as an explicit tombstone in both
    /// modes.
    ///
    /// Labels are never altered — the spec states that `SET n = map` replaces
    /// properties only.
    ///
    /// # Errors
    ///
    /// Returns an error if the entity cannot be found in the storage layer, or
    /// if the writer fails to persist the updated properties.
    #[expect(clippy::too_many_arguments)]
    async fn apply_properties_to_entity(
        &self,
        variable: &str,
        new_props: HashMap<String, Value>,
        replace: bool,
        row: &mut HashMap<String, Value>,
        writer: &Writer,
        prop_manager: &PropertyManager,
        params: &HashMap<String, Value>,
        ctx: Option<&QueryContext>,
        tx_l0: Option<&Arc<parking_lot::RwLock<uni_store::runtime::l0::L0Buffer>>>,
        prefetched: &Prefetch,
    ) -> Result<()> {
        // Clone the target so we can hold &row references elsewhere.
        let target = row.get(variable).cloned();

        // Declared-type guard for the whole-entity `SET n = map` / `SET n += map`
        // forms, mirroring the per-property SET path (issue #68).
        let schema = self.storage.schema_manager().schema();

        match target {
            Some(Value::Node(ref node)) => {
                let vid = node.vid;
                // Current labels, not the binding's: the write stores them
                // with the row, and stale ones would restore a label an
                // earlier write removed.
                let labels =
                    self.current_vertex_labels(vid, row.get(variable).unwrap_or(&Value::Null), ctx);
                let enriched = self
                    .write_vertex_props(
                        vid,
                        &labels,
                        new_props,
                        replace,
                        &schema,
                        writer,
                        prop_manager,
                        params,
                        ctx,
                        tx_l0,
                        prefetched,
                    )
                    .await?;
                // Update the in-memory row binding
                if let Some(Value::Node(n)) = row.get_mut(variable) {
                    n.properties = enriched.into_iter().filter(|(_, v)| !v.is_null()).collect();
                }
            }
            Some(ref node_val) if Self::vid_from_value(node_val).is_ok() => {
                let vid = Self::vid_from_value(node_val)?;
                let labels = self.current_vertex_labels(vid, node_val, ctx);
                let enriched = self
                    .write_vertex_props(
                        vid,
                        &labels,
                        new_props,
                        replace,
                        &schema,
                        writer,
                        prop_manager,
                        params,
                        ctx,
                        tx_l0,
                        prefetched,
                    )
                    .await?;
                // Update the in-memory map-encoded node binding
                if let Some(Value::Map(node_map)) = row.get_mut(variable) {
                    // Remove old user property keys, keep internal fields
                    node_map.retain(|k, _| {
                        uni_common::core::schema::is_reserved_property_name(k) || k == "ext_id"
                    });
                    // Build effective (non-null) properties
                    let effective: HashMap<String, Value> =
                        enriched.into_iter().filter(|(_, v)| !v.is_null()).collect();
                    for (k, v) in &effective {
                        node_map.insert(k.clone(), v.clone());
                    }
                    // Replace _all_props to reflect the complete property set
                    node_map.insert("_all_props".to_string(), Value::Map(effective));
                }
            }
            Some(Value::Edge(ref edge)) => {
                let eid = edge.eid;
                let src = edge.src;
                let dst = edge.dst;
                let etype = self.resolve_edge_type_id(&Value::String(edge.edge_type.clone()))?;
                let current =
                    read_edge_props_with_prefetch(eid, prefetched, prop_manager, ctx).await?;
                let before = current.clone();
                let write_props = Self::merge_props(current, new_props, replace);
                let write_props = Self::coerce_and_validate_props(
                    write_props,
                    &schema,
                    std::slice::from_ref(&edge.edge_type),
                )?;
                let effects = row_property_effects(&before, &write_props);
                writer
                    .update_edge(
                        src,
                        dst,
                        etype,
                        eid,
                        write_props.clone(),
                        Some(edge.edge_type.clone()),
                        tx_l0,
                    )
                    .await?;
                writer.record_property_effects(StatEntity::Edge(eid), &effects, tx_l0);
                // Update the in-memory row binding
                if let Some(Value::Edge(e)) = row.get_mut(variable) {
                    e.properties = write_props
                        .into_iter()
                        .filter(|(_, v)| !v.is_null())
                        .collect();
                }
            }
            Some(Value::Map(ref map))
                if map.contains_key("_eid")
                    && map.contains_key("_src")
                    && map.contains_key("_dst") =>
            {
                let ei = self.extract_edge_identity(map)?;
                let current =
                    read_edge_props_with_prefetch(ei.eid, prefetched, prop_manager, ctx).await?;
                let before = current.clone();
                let write_props = Self::merge_props(current, new_props, replace);
                let edge_type_name = Value::Map(map.clone())
                    .edge_type_ref()
                    .and_then(|t| t.name().map(str::to_string))
                    .or_else(|| {
                        self.storage
                            .schema_manager()
                            .edge_type_name_by_id_unified(ei.edge_type_id)
                    });
                let write_props = match &edge_type_name {
                    Some(name) => Self::coerce_and_validate_props(
                        write_props,
                        &schema,
                        std::slice::from_ref(name),
                    )?,
                    None => write_props,
                };
                let effects = row_property_effects(&before, &write_props);
                writer
                    .update_edge(
                        ei.src,
                        ei.dst,
                        ei.edge_type_id,
                        ei.eid,
                        write_props.clone(),
                        edge_type_name,
                        tx_l0,
                    )
                    .await?;
                writer.record_property_effects(StatEntity::Edge(ei.eid), &effects, tx_l0);
                // Update the in-memory map-encoded edge binding
                if let Some(Value::Map(edge_map)) = row.get_mut(variable) {
                    edge_map.retain(|k, _| uni_common::core::schema::is_reserved_property_name(k));
                    let effective: HashMap<String, Value> = write_props
                        .into_iter()
                        .filter(|(_, v)| !v.is_null())
                        .collect();
                    for (k, v) in &effective {
                        edge_map.insert(k.clone(), v.clone());
                    }
                    // Replace _all_props to reflect the complete property set
                    edge_map.insert("_all_props".to_string(), Value::Map(effective));
                }
            }
            _ => {
                // No matching entity — nothing to do (caller already guarded against Null)
            }
        }
        Ok(())
    }

    /// Computes the property map to write given current storage state and the
    /// incoming change map.
    ///
    /// When `replace` is `true`, keys present in `current` but absent from
    /// `incoming` are tombstoned with `Value::Null`.  Null values inside
    /// `incoming` are always preserved as explicit tombstones.
    ///
    /// When `replace` is `false`, `current` is the base and `incoming` is
    /// merged on top: each key in `incoming` overwrites or tombstones the
    /// corresponding entry in `current`.
    fn merge_props(
        current: HashMap<String, Value>,
        incoming: HashMap<String, Value>,
        replace: bool,
    ) -> HashMap<String, Value> {
        if replace {
            // Start from the non-null incoming entries only.
            let mut result: HashMap<String, Value> = incoming
                .iter()
                .filter(|(_, v)| !v.is_null())
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect();
            // Tombstone every current key that is absent from incoming OR explicitly
            // set to null in incoming (both mean "delete this property").
            for k in current.keys() {
                if incoming.get(k).is_none_or(|v| v.is_null()) {
                    result.insert(k.clone(), Value::Null);
                }
            }
            result
        } else {
            // Merge: start from current and apply incoming on top
            let mut result = current;
            result.extend(incoming);
            result
        }
    }

    /// Extract edge identity fields (`_eid`, `_src`, `_dst`, `_type`) from a map.
    fn extract_edge_identity(&self, map: &HashMap<String, Value>) -> Result<EdgeIdentity> {
        // `_eid` read as an integer rejected the serde string forms `"Eid(7)"`
        // and `"7"`, which a map that round-tripped through JSON carries — the
        // edge then failed identity extraction outright (#234).
        //
        // Precondition, relied on by every caller and worth keeping that way:
        // callers reach here only after proving `_eid` is present and non-null.
        // The accessor would otherwise also accept an `_id` on an edge-shaped
        // map, and this is a mutation path where that widening is not wanted.
        let Some(uni_common::value::EntityRef::Edge(eid)) =
            uni_common::value::entity_ref_from_map(map)
        else {
            return Err(anyhow!("Invalid _eid"));
        };
        let src = Vid::from(
            map.get("_src")
                .and_then(|v| v.as_u64())
                .ok_or_else(|| anyhow!("Invalid _src"))?,
        );
        let dst = Vid::from(
            map.get("_dst")
                .and_then(|v| v.as_u64())
                .ok_or_else(|| anyhow!("Invalid _dst"))?,
        );
        let edge_type_id = self.resolve_edge_type_id(
            map.get("_type")
                .or_else(|| map.get("_type_name"))
                .ok_or_else(|| anyhow!("Missing _type/_type_name on edge map"))?,
        )?;
        Ok(EdgeIdentity {
            eid,
            src,
            dst,
            edge_type_id,
        })
    }

    /// Resolve edge type ID from a Value, supporting both Int and String representations.
    /// DataFusion traverse stores _type as String("KNOWS"), while write operations need u32 ID.
    ///
    /// For String values, uses get_or_assign_edge_type_id to support schemaless edge types
    /// (assigns new ID if not found). This is critical for MERGE ... ON CREATE SET scenarios
    /// where the edge type was just created and may not be in the read-only lookup yet.
    fn resolve_edge_type_id(&self, type_val: &Value) -> Result<u32> {
        match type_val {
            Value::Int(i) => Ok(*i as u32),
            Value::String(name) => {
                if self.config.strict_schema {
                    let schema = self.storage.schema_manager().schema();
                    schema
                        .edge_type_id_by_name_case_insensitive(name)
                        .ok_or_else(|| {
                            anyhow!(
                                "Edge type '{}' is not defined in the schema \
                                 (strict_schema is enabled). \
                                 Declare it with db.schema().edge_type(...).apply() first.",
                                name
                            )
                        })
                } else {
                    // Schemaless: assign new ID if not found in schema or registry.
                    Ok(self
                        .storage
                        .schema_manager()
                        .get_or_assign_edge_type_id(name))
                }
            }
            _ => Err(anyhow!(
                "Invalid _type value: expected Int or String, got {:?}",
                type_val
            )),
        }
    }

    pub(crate) async fn execute_vacuum(&self) -> Result<()> {
        if let Some(writer_arc) = &self.writer {
            // Flush first while holding the lock
            {
                let writer: &uni_store::Writer = writer_arc.as_ref();
                writer.flush_to_l1(None).await?;
            } // Drop lock before compacting to avoid blocking reads/writes

            // Compaction can run without holding the writer lock
            let compactor = uni_store::storage::compaction::Compactor::new(self.storage.clone());
            let semantic = compactor.compact_all().await?;

            // Re-warm adjacency manager for compacted edge types to sync in-memory CSR with new L2 storage
            let am = self.storage.adjacency_manager();
            let schema = self.storage.schema_manager().schema();
            for info in semantic.adjacency {
                // Convert string direction to Direction enum
                let direction = match info.direction.as_str() {
                    "fwd" => uni_store::storage::direction::Direction::Outgoing,
                    "bwd" => uni_store::storage::direction::Direction::Incoming,
                    _ => continue,
                };

                // Get edge_type_id
                if let Some(edge_type_id) =
                    schema.edge_type_id_unified_case_insensitive(&info.edge_type)
                {
                    // Re-warm from storage (clears old CSR, loads new L2 + L1 delta)
                    let _ = am.warm(&self.storage, edge_type_id, direction, None).await;
                }
            }
        }
        Ok(())
    }

    pub(crate) async fn execute_checkpoint(&self) -> Result<()> {
        if let Some(writer_arc) = &self.writer {
            let writer: &uni_store::Writer = writer_arc.as_ref();
            writer.flush_to_l1(Some("checkpoint".to_string())).await?;
        }
        Ok(())
    }

    fn parse_data_type(type_str: &str) -> Result<DataType> {
        use uni_common::core::schema::{CrdtType, PointType};
        let type_str = type_str.to_lowercase();
        let type_str = type_str.trim();
        match type_str {
            "string" | "text" | "varchar" => Ok(DataType::String),
            // A Cypher integer is 64-bit, so the unqualified spellings must be
            // too. They mapped to `Int32` here while the procedure path
            // (`ddl_procedures::parse_data_type`) read the same keywords as
            // `Int64`; values beyond 32 bits written through `CREATE LABEL` were
            // silently wrapped, so `4294967296` read back as `0`.
            "int" | "integer" | "int64" | "long" | "bigint" => Ok(DataType::Int64),
            // Still available when a narrow column is what is actually wanted.
            "int32" => Ok(DataType::Int32),
            // As with the integer spellings above: a Cypher float is 64-bit, and
            // `ddl_procedures::parse_data_type` already read `FLOAT` that way.
            // Reading it as `Float32` here silently dropped precision —
            // `0.1234567890123456` came back as `0.12345679104328156`.
            "float" | "float64" | "double" => Ok(DataType::Float64),
            // `FLOAT32` is the explicit narrow form. `REAL` keeps SQL's
            // single-precision meaning; it is not a Cypher keyword, so no
            // conformance question arises.
            "float32" | "real" => Ok(DataType::Float32),
            "bool" | "boolean" => Ok(DataType::Bool),
            "timestamp" => Ok(DataType::Timestamp),
            "date" => Ok(DataType::Date),
            "time" => Ok(DataType::Time),
            "datetime" => Ok(DataType::DateTime),
            "duration" => Ok(DataType::Duration),
            "btic" => Ok(DataType::Btic),
            "json" | "jsonb" => Ok(DataType::CypherValue),
            "bytes" | "blob" | "binary" => Ok(DataType::Bytes),
            "point" => Ok(DataType::Point(PointType::Cartesian2D)),
            "point3d" => Ok(DataType::Point(PointType::Cartesian3D)),
            "geopoint" | "geographic" => Ok(DataType::Point(PointType::Geographic)),
            s if s.starts_with("sparse_vector(") && s.ends_with(')') => {
                let dims_str = &s["sparse_vector(".len()..s.len() - 1];
                let dimensions = dims_str
                    .parse::<usize>()
                    .map_err(|_| anyhow!("Invalid sparse_vector dimensions: {}", dims_str))?;
                Ok(DataType::SparseVector { dimensions })
            }
            s if s.starts_with("vector(") && s.ends_with(')') => {
                let dims_str = &s[7..s.len() - 1];
                let dimensions = dims_str
                    .parse::<usize>()
                    .map_err(|_| anyhow!("Invalid vector dimensions: {}", dims_str))?;
                Ok(DataType::Vector { dimensions })
            }
            s if s.starts_with("binary_vector(") && s.ends_with(')') => {
                let dims_str = &s["binary_vector(".len()..s.len() - 1];
                let dimensions = dims_str
                    .parse::<usize>()
                    .map_err(|_| anyhow!("Invalid binary_vector dimensions: {}", dims_str))?;
                Ok(DataType::BinaryVector { dimensions })
            }
            s if s.starts_with("list<") && s.ends_with('>') => {
                let inner_type_str = &s[5..s.len() - 1];
                let inner_type = Self::parse_data_type(inner_type_str)?;
                Ok(DataType::List(Box::new(inner_type)))
            }
            s if s.starts_with("map<") && s.ends_with('>') => {
                let (k_str, v_str) = Self::split_map_kv(&s[4..s.len() - 1])?;
                let key_type = Self::parse_data_type(&k_str)?;
                if !matches!(key_type, DataType::String) {
                    return Err(anyhow!("MAP key type must be STRING, got: {k_str}"));
                }
                let value_type = Self::parse_data_type(&v_str)?;
                Ok(DataType::Map(Box::new(key_type), Box::new(value_type)))
            }
            "gcounter" => Ok(DataType::Crdt(CrdtType::GCounter)),
            "lwwregister" => Ok(DataType::Crdt(CrdtType::LWWRegister)),
            _ => Err(anyhow!("Unknown data type: {}", type_str)),
        }
    }

    /// Split a `MAP<K, V>` inner string on the top-level comma, respecting `<>`/`()` depth
    /// so nested value types (`STRING, LIST<INT>`, `STRING, MAP<STRING,INT>`) split at the
    /// right comma. Returns trimmed `(key, value)` type strings.
    fn split_map_kv(inner: &str) -> Result<(String, String)> {
        let mut depth = 0i32;
        for (i, c) in inner.char_indices() {
            match c {
                '<' | '(' => depth += 1,
                '>' | ')' => depth -= 1,
                ',' if depth == 0 => {
                    let k = inner[..i].trim();
                    let v = inner[i + 1..].trim();
                    if k.is_empty() || v.is_empty() {
                        return Err(anyhow!("MAP<K,V> requires both a key and a value type"));
                    }
                    return Ok((k.to_string(), v.to_string()));
                }
                _ => {}
            }
        }
        Err(anyhow!(
            "MAP<K,V> requires a comma separating key and value types"
        ))
    }

    /// Check every property of a `CREATE LABEL` / `CREATE EDGE TYPE` before the
    /// schema is touched, returning the parsed data types in order.
    ///
    /// The element is added first and its properties one at a time, so a
    /// property that fails validation part-way used to leave the element (and
    /// the properties before it) in the live schema although the statement
    /// failed: a retry then hit "already exists".
    fn prevalidate_property_definitions(
        properties: &[uni_cypher::ast::PropertyDefinition],
    ) -> Result<Vec<DataType>> {
        let mut seen = std::collections::HashSet::new();
        properties
            .iter()
            .map(|prop| {
                uni_common::core::schema::validate_property_name(&prop.name)?;
                if !seen.insert(prop.name.as_str()) {
                    return Err(anyhow!("Property '{}' is declared twice", prop.name));
                }
                Self::parse_data_type(&prop.data_type)
            })
            .collect()
    }

    pub(crate) async fn execute_create_label(&self, clause: CreateLabel) -> Result<()> {
        let sm = self.storage.schema_manager_arc();
        if clause.if_not_exists && sm.schema().labels.contains_key(&clause.name) {
            return Ok(());
        }
        let types = Self::prevalidate_property_definitions(&clause.properties)?;
        let is_new = !sm.schema().labels.contains_key(&clause.name);
        // Existing schemaless values must fit the declared types, or the
        // backfill would store them as NULL; refused before anything changes.
        // See `StorageManager::check_declared_values`.
        let declared: Vec<(String, DataType)> = clause
            .properties
            .iter()
            .zip(&types)
            .map(|(prop, dt)| (prop.name.clone(), dt.clone()))
            .collect();
        let declaration_lock = if is_new {
            self.prepare_declaration(&clause.name, DeclaredEntity::Label, &declared)
                .await?
        } else {
            None
        };
        // The label may already hold vertices created schemaless; see
        // `backfill_declared_label`. The marker precedes the live catalog.
        if is_new {
            self.storage
                .mark_label_backfill_pending(&clause.name)
                .await?;
        }
        // Rows that already carry the label cannot satisfy a NOT NULL
        // property, so it is recorded nullable (as `SchemaBuilder::apply`
        // does). Decided before the label enters the catalog.
        let l0 = self
            .writer
            .as_ref()
            .map(|w| w.committed_l0_context())
            .unwrap_or_default();
        let mut nullability = Vec::with_capacity(clause.properties.len());
        for prop in &clause.properties {
            nullability.push(
                self.storage
                    .new_property_nullability(&clause.name, &prop.name, prop.nullable, &l0)
                    .await?,
            );
        }
        sm.add_label_with_desc(&clause.name, clause.description)?;
        for ((prop, dt), nullable) in clause.properties.into_iter().zip(types).zip(nullability) {
            sm.add_property_with_desc(&clause.name, &prop.name, dt, nullable, prop.description)?;
            if prop.unique {
                let constraint = Constraint {
                    name: format!("{}_{}_unique", clause.name, prop.name),
                    constraint_type: ConstraintType::Unique {
                        properties: vec![prop.name],
                    },
                    target: ConstraintTarget::Label(clause.name.clone()),
                    enabled: true,
                };
                sm.add_constraint(constraint)?;
            }
        }
        // Commits from here re-validate against the declaration.
        drop(declaration_lock);
        if is_new {
            self.backfill_declared_label(&clause.name).await?;
        }
        sm.save().await?;
        if is_new {
            self.storage
                .clear_label_backfill_marker(&clause.name)
                .await?;
        }
        Ok(())
    }

    /// The storage half of declaring `declared` on `entity`, before the
    /// catalog change: take the writer's declaration lock (returned, to be
    /// held until the catalog change is made), flush the entity's committed
    /// L0, refuse values the declared types cannot hold, and widen / convert
    /// storage. See `SchemaBuilder::apply`, which does the same.
    pub(crate) async fn prepare_declaration(
        &self,
        entity: &str,
        kind: DeclaredEntity,
        declared: &[(String, DataType)],
    ) -> Result<Option<uni_store::runtime::DeclarationLock>> {
        if declared.is_empty() {
            return Ok(None);
        }
        let held = match self.writer.as_ref() {
            Some(writer) => {
                let held = writer.declaration_lock().await?;
                writer.flush_before_declaration(&held, &[entity]).await?;
                Some(held)
            }
            None => None,
        };
        self.storage
            .check_declared_values(entity, kind, declared)
            .await?;
        self.storage
            .widen_for_checked_properties(entity, kind, declared)
            .await?;
        Ok(held)
    }

    /// Copy a just-declared label's flushed rows from the main vertex table
    /// into its label table (#304), through the writer's flush barrier when
    /// there is a writer. See `Writer::backfill_declared_label`.
    pub(crate) async fn backfill_declared_label(&self, label: &str) -> Result<usize> {
        match &self.writer {
            Some(writer) => writer.backfill_declared_label(label).await,
            None => self.storage.backfill_declared_label(label).await,
        }
    }

    pub(crate) async fn enrich_properties_with_generated_columns(
        &self,
        label_name: &str,
        properties: &mut HashMap<String, Value>,
        prop_manager: &PropertyManager,
        params: &HashMap<String, Value>,
        ctx: Option<&QueryContext>,
    ) -> Result<()> {
        let schema = self.storage.schema_manager().schema();

        if let Some(props_meta) = schema.properties.get(label_name) {
            let mut generators = Vec::new();
            for (prop_name, meta) in props_meta {
                if let Some(expr_str) = &meta.generation_expression {
                    generators.push((prop_name.clone(), expr_str.clone()));
                }
            }

            for (prop_name, expr_str) in generators {
                let cache_key = (label_name.to_string(), prop_name.clone());
                let expr = {
                    let cache = self.gen_expr_cache.read().await;
                    cache.get(&cache_key).cloned()
                };

                let expr = match expr {
                    Some(e) => e,
                    None => {
                        let parsed = uni_cypher::parse_expression(&expr_str)
                            .map_err(|e| anyhow!("Failed to parse generation expression: {}", e))?;
                        let mut cache = self.gen_expr_cache.write().await;
                        cache.insert(cache_key, parsed.clone());
                        parsed
                    }
                };

                let mut scope = HashMap::new();

                // If expression has an explicit variable, use it as an object
                if let Some(var) = expr.extract_variable() {
                    scope.insert(var, Value::Map(properties.clone()));
                } else {
                    // No explicit variable - add properties directly to scope for bare references
                    // e.g., "lower(email)" can reference "email" directly
                    for (k, v) in properties.iter() {
                        scope.insert(k.clone(), v.clone());
                    }
                }

                let val = self
                    .evaluate_expr(&expr, &scope, prop_manager, params, ctx)
                    .await?;
                properties.insert(prop_name, val);
            }
        }
        Ok(())
    }

    pub(crate) async fn execute_create_edge_type(&self, clause: CreateEdgeType) -> Result<()> {
        let sm = self.storage.schema_manager_arc();
        if clause.if_not_exists && sm.schema().edge_types.contains_key(&clause.name) {
            return Ok(());
        }
        let types = Self::prevalidate_property_definitions(&clause.properties)?;
        // Relationships written under the type schemaless keep their values,
        // which must fit the declared types; see `prepare_declaration`.
        let declared: Vec<(String, DataType)> = clause
            .properties
            .iter()
            .zip(&types)
            .map(|(prop, dt)| (prop.name.clone(), dt.clone()))
            .collect();
        let declaration_lock = if sm.schema().edge_types.contains_key(&clause.name) {
            None
        } else {
            self.prepare_declaration(&clause.name, DeclaredEntity::EdgeType, &declared)
                .await?
        };
        sm.add_edge_type_with_desc(
            &clause.name,
            clause.src_labels,
            clause.dst_labels,
            clause.description,
        )?;
        for (prop, dt) in clause.properties.into_iter().zip(types) {
            sm.add_property_with_desc(
                &clause.name,
                &prop.name,
                dt,
                prop.nullable,
                prop.description,
            )?;
        }
        drop(declaration_lock);
        sm.save().await?;
        Ok(())
    }

    /// Executes an ALTER action on a schema entity.
    ///
    /// This is a shared helper for both `execute_alter_label` and
    /// `execute_alter_edge_type` since they have identical logic.
    pub(crate) async fn execute_alter_entity(
        storage: &uni_store::storage::manager::StorageManager,
        l0: &uni_store::runtime::l0_visibility::L0Context,
        entity_name: &str,
        action: AlterAction,
    ) -> Result<()> {
        let sm = storage.schema_manager_arc();
        let sm = &sm;
        match action {
            AlterAction::AddProperty(prop) => {
                let dt = Self::parse_data_type(&prop.data_type)?;
                // A NOT NULL property cannot be satisfied by rows that already
                // exist -- they have no value for it. Record it as nullable and
                // say so, rather than asserting a constraint the stored data
                // violates. NOT NULL is enforced forward, at write time, either
                // way. Mirrors `SchemaBuilder::apply`.
                let rows = storage.existing_row_count(entity_name, l0).await?;
                let effective_nullable = if prop.nullable || rows == 0 {
                    prop.nullable
                } else {
                    tracing::warn!(
                        entity = %entity_name,
                        property = %prop.name,
                        rows,
                        "ALTER ... ADD PROPERTY declares NOT NULL on a populated entity; \
                         recording it as nullable because existing rows have no value"
                    );
                    true
                };
                // Storage before catalog -- before the live catalog, not just
                // before `sm.save()` below: a Lance column the catalog does not
                // know about is invisible, while a catalog column Lance lacks is
                // a hard read failure -- including through a fork branch, which
                // carries its own schema copy and is widened here too (issue
                // #249). Any commit saves a dirty catalog, so declaring first
                // let one persist the property ahead of its column. A property
                // already declared is refused by `add_property_with_desc`, and
                // is not widened to a type it does not have.
                let declared = sm
                    .schema()
                    .properties
                    .get(entity_name)
                    .is_some_and(|props| props.contains_key(&prop.name));
                if !declared {
                    storage
                        .widen_for_declared_properties(
                            entity_name,
                            &[(prop.name.clone(), dt.clone())],
                        )
                        .await?;
                }
                sm.add_property_with_desc(
                    entity_name,
                    &prop.name,
                    dt,
                    effective_nullable,
                    prop.description,
                )?;
            }
            AlterAction::DropProperty(prop_name) => {
                sm.drop_property(entity_name, &prop_name)?;
            }
            AlterAction::RenameProperty { old_name, new_name } => {
                sm.rename_property(entity_name, &old_name, &new_name)?;
            }
            AlterAction::SetDescription(desc) => {
                if sm.schema().labels.contains_key(entity_name) {
                    sm.set_label_description(entity_name, desc)?;
                } else {
                    sm.set_edge_type_description(entity_name, desc)?;
                }
            }
            AlterAction::SetPropertyDescription {
                property,
                description,
            } => {
                sm.set_property_description(entity_name, &property, description)?;
            }
        }
        sm.save().await?;
        Ok(())
    }

    /// For `ADD PROPERTY`: take the writer's declaration lock and flush
    /// `entity`'s committed L0, so the values the declaration checks and
    /// promotes are all where it reads them and no commit slips in before the
    /// catalog change. Held by the caller across the whole ALTER. See
    /// `Writer::declaration_lock`.
    async fn lock_for_add_property(
        &self,
        entity: &str,
        action: &AlterAction,
    ) -> Result<Option<uni_store::runtime::DeclarationLock>> {
        if matches!(action, AlterAction::AddProperty(_))
            && let Some(writer) = self.writer.as_ref()
        {
            let held = writer.declaration_lock().await?;
            writer.flush_before_declaration(&held, &[entity]).await?;
            return Ok(Some(held));
        }
        Ok(None)
    }

    pub(crate) async fn execute_alter_label(&self, clause: AlterLabel) -> Result<()> {
        let _held = self
            .lock_for_add_property(&clause.name, &clause.action)
            .await?;
        let l0 = self.visible_l0().await;
        Self::execute_alter_entity(&self.storage, &l0, &clause.name, clause.action).await
    }

    pub(crate) async fn execute_alter_edge_type(&self, clause: AlterEdgeType) -> Result<()> {
        let _held = self
            .lock_for_add_property(&clause.name, &clause.action)
            .await?;
        let l0 = self.visible_l0().await;
        Self::execute_alter_entity(&self.storage, &l0, &clause.name, clause.action).await
    }

    pub(crate) async fn execute_drop_label(&self, clause: DropLabel) -> Result<()> {
        let sm = self.storage.schema_manager_arc();
        sm.drop_label(&clause.name, clause.if_exists)?;
        sm.save().await?;
        Ok(())
    }

    pub(crate) async fn execute_drop_edge_type(&self, clause: DropEdgeType) -> Result<()> {
        let sm = self.storage.schema_manager_arc();
        sm.drop_edge_type(&clause.name, clause.if_exists)?;
        sm.save().await?;
        Ok(())
    }

    pub(crate) async fn execute_create_constraint(&self, clause: CreateConstraint) -> Result<()> {
        let sm = self.storage.schema_manager_arc();
        // A relationship pattern (`ON ()-[r:TYPE]-()`) targets an edge type; a
        // node pattern targets a label.
        let target = if clause.on_relationship {
            ConstraintTarget::EdgeType(clause.label)
        } else {
            ConstraintTarget::Label(clause.label)
        };
        let c_type = match clause.constraint_type {
            AstConstraintType::Unique => ConstraintType::Unique {
                properties: clause.properties,
            },
            // NodeKey keeps its own variant so the write path can enforce the
            // NOT-NULL half of node-key semantics, not just uniqueness.
            AstConstraintType::NodeKey => ConstraintType::NodeKey {
                properties: clause.properties,
            },
            AstConstraintType::Exists => {
                let property = clause
                    .properties
                    .into_iter()
                    .next()
                    .ok_or_else(|| anyhow!("EXISTS constraint requires a property"))?;
                ConstraintType::Exists { property }
            }
            AstConstraintType::Check => {
                let expression = clause
                    .expression
                    .ok_or_else(|| anyhow!("CHECK constraint requires an expression"))?;
                ConstraintType::Check {
                    expression: expression.to_string_repr(),
                }
            }
        };

        let constraint = Constraint {
            name: clause.name.unwrap_or_else(|| "auto_constraint".to_string()),
            constraint_type: c_type,
            target,
            enabled: true,
        };

        sm.add_constraint(constraint)?;
        sm.save().await?;
        Ok(())
    }

    pub(crate) async fn execute_drop_constraint(&self, clause: DropConstraint) -> Result<()> {
        let sm = self.storage.schema_manager_arc();
        sm.drop_constraint(&clause.name, false)?;
        sm.save().await?;
        Ok(())
    }

    /// Detects the single-node, single-label MERGE shape the fast path serves.
    ///
    /// Returns the node pattern and its label when `pattern` is one path with
    /// one node element, exactly one label, and a static map-literal property
    /// set — the shape [`Self::execute_merge_row_indexed`] can serve without
    /// per-row query planning. The keys do NOT need to be indexed: the persisted
    /// lookup degrades to a (single, filtered) label scan when no scalar index
    /// exists, which is still far cheaper than building a `LogicalPlan` per row.
    /// Any other shape (edges, multiple labels, non-literal properties) returns
    /// `None` so the caller uses the general per-row path.
    fn merge_single_node_fastpath<'p>(
        &self,
        pattern: &'p Pattern,
    ) -> Option<(&'p NodePattern, String)> {
        if pattern.paths.len() != 1 {
            return None;
        }
        let path = &pattern.paths[0];
        if path.elements.len() != 1 {
            return None;
        }
        let PatternElement::Node(n) = &path.elements[0] else {
            return None;
        };
        let labels = n.labels.names();
        if labels.len() != 1 {
            return None;
        }
        // The key must be a static map literal so the key names are known.
        let Some(Expr::Map(entries)) = n.properties.as_ref() else {
            return None;
        };
        if entries.is_empty() {
            return None;
        }
        // Resolve the label to its schema-canonical case so the fast path agrees
        // with the general MERGE path (which matches labels case-insensitively).
        // Without this, `MERGE (:person …)` after a `:Person` row was flushed
        // scans/keys a different label than the canonical one and creates a
        // duplicate (review #3a). Falls back to the as-written label when the
        // schema does not know it (schemaless).
        let canonical = self
            .storage
            .schema_manager()
            .schema()
            .canonical_label_name(&labels[0])
            .unwrap_or_else(|| labels[0].clone());
        Some((n, canonical))
    }

    /// RC3: detect the bound-endpoints, anonymous-edge relationship MERGE shape
    /// `(a)-[:TYPE]->(b)` whose edge existence can be resolved with one O(1)
    /// adjacency probe instead of building and running a per-row traversal
    /// `LogicalPlan` (the general path is ~19x the bulk CREATE of the same edges).
    ///
    /// Returns `(source_var, target_var, edge_type_id, direction)` when the
    /// pattern is exactly one path of `[Node, Rel, Node]` where the relationship
    /// is a single concrete type, **anonymous** (no variable → no edge binding to
    /// reproduce), fixed-length, and unfiltered, and both endpoint nodes are plain
    /// variables with no re-specified MERGE properties or inline WHERE (those are
    /// filters only the general path applies). Any deviation returns `None` and
    /// the caller keeps the general path. The caller still verifies per row that
    /// both endpoints are actually bound to vids.
    /// Bind a matched relationship variable to the edge that is already there.
    ///
    /// Both relationship fast paths used to hand a *matched* named edge back to
    /// the general path, on the grounds that an `Edge` carries properties and
    /// those could not be enumerated. That was wrong about the API, not about
    /// the requirement: the batch reader takes an explicit name list, but
    /// `PropertyManager::get_all_edge_props_with_ctx` enumerates a single
    /// edge's properties, accumulating L0 over storage. The adjacency probe
    /// already yields the eid, so everything an `Edge` needs is in hand.
    ///
    /// `probe_from` is the endpoint the probe walked out of and `neighbour` is
    /// what it found, so the direction decides which way round the edge goes.
    #[expect(
        clippy::too_many_arguments,
        reason = "mirrors the fast paths' threaded state"
    )]
    async fn bind_matched_edge(
        &self,
        row: &mut HashMap<String, Value>,
        rel_var: &str,
        eid: uni_common::core::id::Eid,
        type_id: u32,
        probe_from: Vid,
        neighbour: Vid,
        dir: uni_store::storage::direction::Direction,
        prop_manager: &PropertyManager,
        ctx: Option<&QueryContext>,
    ) -> Result<()> {
        let edge_type = self
            .storage
            .schema_manager()
            .schema()
            .edge_type_name_by_id_unified(type_id)
            .unwrap_or_default();
        let (src, dst) = match dir {
            uni_store::storage::direction::Direction::Outgoing => (probe_from, neighbour),
            _ => (neighbour, probe_from),
        };
        let properties = prop_manager
            .get_all_edge_props_with_ctx(eid, ctx)
            .await?
            .unwrap_or_default();
        row.insert(
            rel_var.to_string(),
            Value::Edge(uni_common::Edge {
                eid,
                edge_type,
                src,
                dst,
                properties,
            }),
        );
        Ok(())
    }

    /// A relationship MERGE with one bound endpoint and one found-or-created by
    /// key: `MATCH (a …) MERGE (a)-[:R]->(b:L {k: v})`.
    ///
    /// Neither existing detector covers it — `merge_single_node_fastpath` wants
    /// a lone node, and `merge_relationship_fastpath_shape` rejects an endpoint
    /// carrying properties — so it fell to the per-row plan, at ~7x the
    /// MATCH+CREATE floor for a batched ingest.
    ///
    /// The keyed endpoint must carry a variable. Without one,
    /// `execute_create_pattern` hands back no vid, and the created node could
    /// not be folded into the batch's key map — which is a correctness
    /// requirement, not an optimisation: a later row merging the same key from
    /// the same source has to match what this row created.
    fn merge_relationship_keyed_fastpath_shape<'p>(
        &self,
        pattern: &'p Pattern,
    ) -> Option<MergeRelKeyedShape<'p>> {
        if pattern.paths.len() != 1 {
            return None;
        }
        let path = &pattern.paths[0];
        // A path variable needs every element bound to build `Value::Path`, and
        // the match outcome here binds no edge. Leave it to the general path.
        if path.variable.is_some() || path.shortest_path_mode.is_some() {
            return None;
        }
        let [
            PatternElement::Node(left),
            PatternElement::Relationship(r),
            PatternElement::Node(right),
        ] = path.elements.as_slice()
        else {
            return None;
        };
        if r.range.is_some()
            || r.properties.is_some()
            || r.where_clause.is_some()
            || r.types.names().len() != 1
        {
            return None;
        }

        // "Plain" = resolved from the input row. A label on it is a filter only
        // the general path applies, so it disqualifies — stricter than
        // `merge_relationship_fastpath_shape`, which can afford it because both
        // its endpoints are already bound.
        let is_plain = |n: &NodePattern| {
            n.variable.as_ref().is_some_and(|v| !v.is_empty())
                && n.properties.is_none()
                && n.where_clause.is_none()
                && n.labels.names().is_empty()
        };
        // "Keyed" = one label plus a non-empty static map literal.
        let keyed_ok = |n: &NodePattern| {
            n.variable.as_ref().is_some_and(|v| !v.is_empty())
                && n.where_clause.is_none()
                && n.labels.names().len() == 1
                && matches!(&n.properties, Some(Expr::Map(e)) if !e.is_empty())
        };

        // Exactly one of each; both-plain is the other detector's job.
        let (plain, keyed, plain_is_left) = match (
            is_plain(left),
            keyed_ok(right),
            is_plain(right),
            keyed_ok(left),
        ) {
            (true, true, _, _) => (left, right, true),
            (_, _, true, true) => (right, left, false),
            _ => return None,
        };

        let type_name = &r.types.names()[0];
        let type_id = if self.config.strict_schema {
            self.storage
                .schema_manager()
                .schema()
                .edge_type_id_by_name_case_insensitive(type_name)?
        } else {
            self.storage
                .schema_manager()
                .get_or_assign_edge_type_id(type_name)
        };
        // The direction to walk FROM the bound endpoint TOWARDS the keyed one,
        // so both spellings of the same link probe correctly.
        let ast_dir = match r.direction {
            uni_cypher::ast::Direction::Outgoing => {
                uni_store::storage::direction::Direction::Outgoing
            }
            uni_cypher::ast::Direction::Incoming => {
                uni_store::storage::direction::Direction::Incoming
            }
            uni_cypher::ast::Direction::Both => return None,
        };
        let probe_dir = if plain_is_left {
            ast_dir
        } else {
            match ast_dir {
                uni_store::storage::direction::Direction::Outgoing => {
                    uni_store::storage::direction::Direction::Incoming
                }
                uni_store::storage::direction::Direction::Incoming => {
                    uni_store::storage::direction::Direction::Outgoing
                }
                other => other,
            }
        };

        // Canonical, or the scan reads a different table than the create writes.
        let keyed_names = keyed.labels.names();
        let keyed_label = self
            .storage
            .schema_manager()
            .schema()
            .canonical_label_name(&keyed_names[0])
            .unwrap_or_else(|| keyed_names[0].clone());

        Some(MergeRelKeyedShape {
            bound_var: plain.variable.clone().unwrap_or_default(),
            keyed,
            keyed_label,
            probe_dir,
            type_id,
            rel_var: r.variable.clone().filter(|v| !v.is_empty()),
        })
    }

    fn merge_relationship_fastpath_shape(
        &self,
        pattern: &Pattern,
    ) -> Option<(
        String,
        String,
        u32,
        uni_store::storage::direction::Direction,
        Option<String>,
    )> {
        if pattern.paths.len() != 1 {
            return None;
        }
        // A path variable needs every element bound to build `Value::Path`, and
        // this path's *match* outcome binds no edge: the relationship it serves
        // is anonymous, so nothing lands in the row, and `bind_path_variables`
        // requires only `!nodes.is_empty()` — `p` came back holding its nodes
        // and no relationship. The general path materialises the match rows and
        // binds them.
        if pattern.paths[0].variable.is_some() {
            return None;
        }
        let [
            PatternElement::Node(a),
            PatternElement::Relationship(r),
            PatternElement::Node(b),
        ] = pattern.paths[0].elements.as_slice()
        else {
            return None;
        };
        // Endpoints: plain variables, no extra MERGE-pattern properties / inline
        // WHERE (a re-specified property is a filter the general path must apply).
        let src_var = a.variable.as_ref()?;
        let dst_var = b.variable.as_ref()?;
        if a.properties.is_some()
            || a.where_clause.is_some()
            || b.properties.is_some()
            || b.where_clause.is_some()
        {
            return None;
        }
        // Relationship: single concrete type, fixed-length, unfiltered.
        //
        // A *named* relationship is allowed through the shape gate but is not
        // servable in every outcome — see the caller. It used to be rejected
        // here, which cost 7x on the commonest ingest shape there is: naming the
        // edge in `MERGE (a)-[e:R]->(b)` dropped the whole statement onto the
        // per-row plan. Measured over 20 000 vertices with a 400-row batch, the
        // two spellings were 0.79s named against 0.11s anonymous, the anonymous
        // one sitting exactly on the MATCH+CREATE floor.
        if r.range.is_some()
            || r.properties.is_some()
            || r.where_clause.is_some()
            || r.types.names().len() != 1
        {
            return None;
        }
        let rel_var = r.variable.clone().filter(|v| !v.is_empty());
        let type_name = &r.types.names()[0];
        let type_id = if self.config.strict_schema {
            self.storage
                .schema_manager()
                .schema()
                .edge_type_id_by_name_case_insensitive(type_name)?
        } else {
            self.storage
                .schema_manager()
                .get_or_assign_edge_type_id(type_name)
        };
        let dir = match r.direction {
            uni_cypher::ast::Direction::Outgoing => {
                uni_store::storage::direction::Direction::Outgoing
            }
            uni_cypher::ast::Direction::Incoming => {
                uni_store::storage::direction::Direction::Incoming
            }
            // Undirected existence is ambiguous to encode as one probe; the
            // general path handles it.
            uni_cypher::ast::Direction::Both => return None,
        };
        Some((src_var.clone(), dst_var.clone(), type_id, dir, rel_var))
    }

    /// Whether every key of a keyed MERGE pattern on `label` is a column the
    /// persisted key scan can filter on.
    ///
    /// On a declared label that scan filters the per-label table by the key
    /// columns, and an undeclared key has none: it lives in `overflow_json`,
    /// so the scan failed with `No field named …` once the label had been
    /// flushed. Such a pattern takes the general path, which reads the
    /// overflow properties. A schemaless label is always eligible: its lookup
    /// decodes each row's full property map.
    fn merge_keys_are_stored_columns(&self, node: &NodePattern, label: &str) -> bool {
        let schema = self.storage.schema_manager().schema();
        if schema.get_label_case_insensitive(label).is_none() {
            return true;
        }
        let declared = schema
            .properties
            .iter()
            .find(|(name, _)| name.eq_ignore_ascii_case(label))
            .map(|(_, props)| props);
        match &node.properties {
            Some(Expr::Map(entries)) => entries.iter().all(|(k, _)| {
                k == "ext_id" || declared.is_some_and(|props| props.contains_key(k.as_str()))
            }),
            _ => true,
        }
    }

    /// Whether a MERGE key can take the batched persisted-scan fast path.
    ///
    /// `false` sends the caller to the general per-row path, so unusual key
    /// value types (lists, maps, temporals, nulls) are never silently
    /// mis-matched. This used to build and return the whole filter string; the
    /// only caller checked `.is_some()` and discarded it, so the predicate is
    /// all it ever was. The filter itself is built by
    /// [`Self::merge_batch_filter`].
    fn merge_key_is_fast_pathable(key_props: &HashMap<String, Value>) -> bool {
        !key_props.is_empty()
            && key_props
                .iter()
                .all(|(k, v)| Self::is_safe_key_ident(k) && Scalar::from_value(v).is_some())
    }

    /// True when a MERGE key name is a plain identifier (issue #8). Keys come
    /// from a static map literal, but validate anyway. `to_sql` now
    /// backtick-quotes every column, so this gate is conservative rather than
    /// required: an awkward key name takes the per-row path instead.
    fn is_safe_key_ident(k: &str) -> bool {
        !k.is_empty() && k.chars().all(|c| c.is_ascii_alphanumeric() || c == '_')
    }

    /// Build ONE scan filter matching every key tuple in `keys` (all tuples
    /// sorted by `key_names` order, values canonicalized).
    ///
    /// Single-column keys render as one `k IN (…)` list, composite keys as an
    /// OR of per-tuple conjunctions. Both are wrapped with the same
    /// `_deleted = false` clause the per-row filter used.
    ///
    /// `is_safe_key_ident` still gates the column names. `to_sql` quotes every
    /// identifier with backticks (Lance reads a double-quoted name as a string
    /// literal, never an identifier), so the gate only keeps awkward key names
    /// on the per-row path.
    ///
    /// Values of mixed types against one column used to be grouped into
    /// separate same-type `IN` lists. That is gone — it never worked. Lance
    /// rejects a literal that does not match the *column's* Arrow type at plan
    /// time, and splitting changes nothing: `(k IN ('a')) OR (k IN (1))` fails
    /// exactly like `k IN ('a', 1)`. Such a batch errors either way, and a
    /// write-time type guard makes it unreachable for a schema'd label.
    fn merge_batch_filter(key_names: &[String], keys: &[&MergeKey]) -> Option<FilterExpr> {
        if keys.is_empty() || key_names.iter().any(|k| !Self::is_safe_key_ident(k)) {
            return None;
        }
        let disjunction = if let [key] = key_names {
            let values = keys
                .iter()
                .map(|tuple| Scalar::from_value(&tuple.first()?.1))
                .collect::<Option<Vec<_>>>()?;
            FilterExpr::one_of(key.as_str(), values)
        } else {
            FilterExpr::any_of(
                keys.iter()
                    .map(|tuple| {
                        Some(FilterExpr::all(
                            tuple
                                .iter()
                                .map(|(k, v)| {
                                    Some(FilterExpr::equals(k.as_str(), Scalar::from_value(v)?))
                                })
                                .collect::<Option<Vec<_>>>()?,
                        ))
                    })
                    .collect::<Option<Vec<_>>>()?,
            )
        };
        Some(FilterExpr::all([disjunction, FilterExpr::not_deleted()]))
    }

    /// Canonicalize a numeric MERGE-key value for *matching only*.
    ///
    /// A finite `Float` with an integral value (e.g. `1.0`) is mapped to the
    /// equivalent `Int`, so an `Int(1)` key matches a node stored with
    /// `Float(1.0)` and vice versa — the coercion the general (DataFusion) MERGE
    /// path already applies (review #3a). Non-numeric and non-integral values are
    /// returned unchanged. Used only to build match keys / comparisons, never the
    /// value written to a created node.
    fn canonical_key_value(v: &Value) -> Value {
        canonical_numeric_key(v)
    }

    /// Canonical sorted `(name, value)` key tuple for a MERGE row's key map.
    ///
    /// Numeric values are canonicalized ([`Self::canonical_key_value`]) so the
    /// tuple compares equal regardless of `Int`/`Float` spelling. This tuple is
    /// used purely as a match key (intra-batch dedup, L0 overlay lookup); the
    /// created node's properties come from the original, un-canonicalized map.
    fn merge_key_tuple(key_props: &HashMap<String, Value>) -> MergeKey {
        let mut tuple: MergeKey = key_props
            .iter()
            .map(|(k, v)| (k.clone(), Self::canonical_key_value(v)))
            .collect();
        tuple.sort_by(|a, b| a.0.cmp(&b.0));
        tuple
    }

    /// Snapshot all live L0 vertices of `label`, grouped by their MERGE key.
    ///
    /// Walked once per MERGE statement (issue #69): the per-row fast path then
    /// resolves L0/uncommitted matches with an O(1) map lookup instead of
    /// re-enumerating L0 for every row. Captures committed-not-yet-persisted
    /// rows and rows created earlier in the same transaction; rows created by
    /// later rows of this same statement are folded in incrementally by
    /// [`Self::execute_merge_row_indexed`]. `key_names` must be sorted to match
    /// [`Self::merge_key_tuple`].
    fn merge_l0_existing(
        &self,
        label: &str,
        key_names: &[String],
        ctx: Option<&QueryContext>,
    ) -> HashMap<MergeKey, Vec<Vid>> {
        let mut candidates: Vec<Vid> = Vec::new();
        l0_visibility::visit_l0_buffers(ctx, |l0| {
            if let Some(vids) = l0.label_to_vids.get(label) {
                candidates.extend(vids.iter().copied());
            }
            false
        });

        let mut map: HashMap<MergeKey, Vec<Vid>> = HashMap::new();
        let mut seen: HashSet<Vid> = HashSet::new();
        for vid in candidates {
            if !seen.insert(vid) || l0_visibility::is_vertex_deleted(vid, ctx) {
                continue;
            }
            // `lookup_vertex_prop` merges across L0 layers (newest wins).
            let tuple: MergeKey = key_names
                .iter()
                .map(|k| {
                    let v = l0_visibility::lookup_vertex_prop(vid, k, ctx).unwrap_or(Value::Null);
                    (k.clone(), Self::canonical_key_value(&v))
                })
                .collect();
            map.entry(tuple).or_default().push(vid);
        }
        map
    }

    /// Maximum key tuples per batched MERGE scan — bounds the filter-string
    /// size and Lance/DataFusion parse cost; chunks run sequentially.
    const MERGE_SCAN_CHUNK: usize = 1000;

    /// Persisted (flushed) vertices of `label` for EVERY key tuple in `keys`,
    /// resolved with one scan per [`Self::MERGE_SCAN_CHUNK`] tuples instead of
    /// one scan per input row (review perf #4: `UNWIND … MERGE` issued N
    /// independent Lance scans).
    ///
    /// Scans via [`uni_store::StorageManager::scan_vertex_table`] — the same
    /// read path `MATCH` uses, so it honors the version high-water-mark and
    /// sees flushed rows. On the declared-label branch the key-filtered scan
    /// only NOMINATES candidate vids; a second, unfiltered `_vid IN (…)` pass
    /// picks each candidate's max-`_version` row and requires it to be live
    /// and still keyed as requested (per-label tables are MVCC-append, so a
    /// superseded version's row would otherwise stale-match a rewritten key).
    /// Matched rows are grouped by their CANONICAL key tuple (stored values
    /// run through [`Self::canonical_key_value`], so a stored `Float(1.0)`
    /// lands under a requested `Int(1)` — the coercion Lance's numeric filter
    /// equality applies). Liveness against L0 overlays (deletes, key rewrites
    /// by earlier rows of the same statement) is NOT checked here — the
    /// per-row consumer re-checks at row time, exactly as the old per-row
    /// scan did.
    ///
    /// The second returned map carries the FULL property maps the schemaless
    /// branch already decoded for each matched vid (empty on the declared-label
    /// branch, which projects only key columns) — the caller seeds the
    /// statement-level [`Prefetch`] from it at zero extra scans.
    ///
    /// # Errors
    /// Propagates persisted-scan and filter-build failures — fail-closed: a
    /// MERGE must never treat a failed lookup as "no match" and create
    /// duplicates.
    async fn merge_lookup_persisted_batch(
        &self,
        label: &str,
        key_names: &[String],
        keys: &HashSet<MergeKey>,
    ) -> Result<(
        HashMap<MergeKey, Vec<Vid>>,
        HashMap<Vid, uni_common::Properties>,
    )> {
        let mut out: HashMap<MergeKey, Vec<Vid>> = HashMap::new();
        if keys.is_empty() {
            return Ok((out, HashMap::new()));
        }
        // An undeclared (schemaless) label has no per-label table — its flushed
        // rows live only in the unified main vertex table. Route to the
        // main-table lookup, mirroring the planner's scan routing (a schemaless
        // MATCH plans `ScanMainByLabels` on the same schema predicate).
        if self
            .storage
            .schema_manager()
            .schema()
            .get_label_case_insensitive(label)
            .is_none()
        {
            return self
                .merge_lookup_persisted_batch_schemaless(label, key_names, keys)
                .await;
        }
        // Declared label — the per-label table is MVCC-append (an update
        // flush adds a higher-`_version` row for the same vid) and the key
        // predicate is pushed into the Lance filter, so a SUPERSEDED version
        // whose row still carries a requested key is returned while the vid's
        // current row (key rewritten, fails the filter) is invisible to the
        // scan. Version dedup among the returned rows cannot detect that, so
        // the lookup runs in two passes: the key-filtered scan only nominates
        // candidate vids, and an unfiltered `_vid IN (…)` scan then requires
        // each candidate's max-`_version` row to be live and still keyed as
        // requested.
        let mut columns: Vec<&str> = vec!["_vid"];
        columns.extend(key_names.iter().map(String::as_str));

        let key_list: Vec<&MergeKey> = keys.iter().collect();
        let mut candidates: Vec<Vid> = Vec::new();
        let mut seen: HashSet<Vid> = HashSet::new();
        for chunk in key_list.chunks(Self::MERGE_SCAN_CHUNK) {
            let filter = Self::merge_batch_filter(key_names, chunk)
                .ok_or_else(|| anyhow!("MERGE fast path could not build a batched key filter"))?;
            // A `MERGE (n:L {k: v})` over an unindexed key resolves its keys by
            // scanning the label's table, and reported nothing at all — neither
            // the rows nor that a scan had happened.
            //
            // The rows are added from the returned batch rather than left to
            // the request: `ScanRequest::count_storage_rows` exists to record a
            // request's rows and has no callers anywhere in the workspace, so
            // attaching counters to a request records none of them. The scans
            // that do populate the figure go through `columnar_scan`, which
            // adds `lance_rows + l0_rows` directly — so both count what storage
            // handed back, and a pushed-down predicate lowers the figure either
            // way. Passing the counters is still worth doing: it is what drives
            // this scan's `index_scans` and `lance_iops`.
            let scanned = self
                .storage
                .scan_vertex_table_counted(label, &columns, Some(&filter), Some(&self.counters))
                .await?;
            let Some(batch) = scanned else { continue };
            self.counters.add_rows_scanned(batch.num_rows());
            let Some(vid_col) = batch
                .column_by_name("_vid")
                .and_then(|c| c.as_any().downcast_ref::<arrow_array::UInt64Array>())
            else {
                continue;
            };
            for i in 0..vid_col.len() {
                let vid = Vid::from(vid_col.value(i));
                if seen.insert(vid) {
                    candidates.push(vid);
                }
            }
        }

        // Verification pass — tombstones are NOT filtered Lance-side (the
        // max-version pick must see them so a deleted winner cannot let an
        // older live version resurrect the match), exactly like the
        // schemaless branch below.
        let mut verify_columns: Vec<&str> = vec!["_vid", "_deleted", "_version"];
        verify_columns.extend(key_names.iter().map(String::as_str));
        for chunk in candidates.chunks(Self::MERGE_SCAN_CHUNK) {
            let filter = FilterExpr::one_of("_vid", chunk.iter().map(|v| Scalar::UInt(v.as_u64())));
            let scanned = self
                .storage
                .scan_vertex_table_counted(
                    label,
                    &verify_columns,
                    Some(&filter),
                    Some(&self.counters),
                )
                .await?;
            // See the key scan above: the request records no rows itself.
            let Some(batch) = scanned else { continue };
            self.counters.add_rows_scanned(batch.num_rows());
            let (Some(vid_col), Some(del_col), Some(ver_col)) = (
                batch
                    .column_by_name("_vid")
                    .and_then(|c| c.as_any().downcast_ref::<arrow_array::UInt64Array>()),
                batch
                    .column_by_name("_deleted")
                    .and_then(|c| c.as_any().downcast_ref::<arrow_array::BooleanArray>()),
                batch
                    .column_by_name("_version")
                    .and_then(|c| c.as_any().downcast_ref::<arrow_array::UInt64Array>()),
            ) else {
                return Err(anyhow!(
                    "MERGE batched lookup: verification scan missing a required column"
                ));
            };
            let key_cols: Vec<_> = key_names
                .iter()
                .map(|k| batch.column_by_name(k))
                .collect::<Option<Vec<_>>>()
                .ok_or_else(|| {
                    anyhow!("MERGE batched lookup: projected key column missing from scan result")
                })?;
            // Per-vid MVCC dedup: keep the highest-version row for each vid.
            let mut winners: HashMap<Vid, (u64, usize)> = HashMap::new();
            for i in 0..batch.num_rows() {
                let vid = Vid::from(vid_col.value(i));
                let ver = ver_col.value(i);
                let entry = winners.entry(vid).or_insert((ver, i));
                if ver > entry.0 {
                    *entry = (ver, i);
                }
            }
            for (vid, (_ver, row)) in winners {
                if del_col.value(row) {
                    continue;
                }
                let tuple: MergeKey = key_names
                    .iter()
                    .zip(&key_cols)
                    .map(|(k, col)| {
                        let v = uni_store::storage::arrow_convert::arrow_to_value(
                            col.as_ref(),
                            row,
                            None,
                        )?;
                        Ok((k.clone(), Self::canonical_key_value(&v)))
                    })
                    .collect::<Result<MergeKey>>()?;
                if keys.contains(&tuple) {
                    out.entry(tuple).or_default().push(vid);
                }
            }
        }
        Ok((out, HashMap::new()))
    }

    /// Persisted-match lookup for an UNDECLARED (schemaless) label.
    ///
    /// Schemaless rows live only in the unified main vertex table (per-label
    /// tables exist only for declared labels), with all properties encoded in
    /// the `props_json` CypherValue blob — so key values cannot be pushed into
    /// the Lance filter; the key match happens in memory after decoding,
    /// exactly like the schemaless MATCH scan. One main-table scan regardless
    /// of key count.
    ///
    /// Mirrors `columnar_scan_schemaless_vertex_batch_static`: tombstones are
    /// NOT filtered Lance-side (MVCC dedup must see them to pick the winning
    /// version per vid); the per-vid max-`_version` dedup runs here, then
    /// deleted winners are dropped.
    ///
    /// Also returns the full decoded property map per matched vid — the blob
    /// is decoded here anyway, and the caller seeds the statement-level
    /// [`Prefetch`] from it instead of re-reading per row.
    ///
    /// # Errors
    /// Propagates scan and blob-decode failures — fail-closed: a MERGE must
    /// never treat a failed lookup as "no match" and create duplicates.
    async fn merge_lookup_persisted_batch_schemaless(
        &self,
        label: &str,
        key_names: &[String],
        keys: &HashSet<MergeKey>,
    ) -> Result<(
        HashMap<MergeKey, Vec<Vid>>,
        HashMap<Vid, uni_common::Properties>,
    )> {
        let mut out: HashMap<MergeKey, Vec<Vid>> = HashMap::new();
        let mut props_by_vid: HashMap<Vid, uni_common::Properties> = HashMap::new();
        let filter = FilterExpr::array_contains("labels", Scalar::Str(label.to_string()));
        let Some(batch) = self
            .storage
            .scan_main_vertex_table(
                &["_vid", "_deleted", "props_json", "_version"],
                Some(&filter),
            )
            .await?
        else {
            return Ok((out, props_by_vid));
        };
        let (Some(vid_col), Some(del_col), Some(ver_col)) = (
            batch
                .column_by_name("_vid")
                .and_then(|c| c.as_any().downcast_ref::<arrow_array::UInt64Array>()),
            batch
                .column_by_name("_deleted")
                .and_then(|c| c.as_any().downcast_ref::<arrow_array::BooleanArray>()),
            batch
                .column_by_name("_version")
                .and_then(|c| c.as_any().downcast_ref::<arrow_array::UInt64Array>()),
        ) else {
            return Err(anyhow!(
                "schemaless MERGE lookup: main vertex table scan missing a required column"
            ));
        };
        let props_col = batch
            .column_by_name("props_json")
            .and_then(|c| c.as_any().downcast_ref::<arrow_array::LargeBinaryArray>());

        // Per-vid MVCC dedup: keep the highest-version row for each vid.
        let mut winners: HashMap<Vid, (u64, usize)> = HashMap::new();
        for i in 0..batch.num_rows() {
            let vid = Vid::from(vid_col.value(i));
            let ver = ver_col.value(i);
            let entry = winners.entry(vid).or_insert((ver, i));
            if ver > entry.0 {
                *entry = (ver, i);
            }
        }
        for (vid, (_ver, row)) in winners {
            // Drop deletion tombstones AFTER picking the winner — a deleted
            // winner must not let an older live version resurrect the match.
            if del_col.value(row) {
                continue;
            }
            // A row without properties matches only an all-Null key tuple.
            let props = match props_col {
                Some(arr) if !arrow_array::Array::is_null(arr, row) => {
                    match uni_common::cypher_value_codec::decode(arr.value(row))
                        .map_err(|e| anyhow!("schemaless MERGE lookup: props decode: {e}"))?
                    {
                        Value::Map(m) => m,
                        _ => HashMap::new(),
                    }
                }
                _ => HashMap::new(),
            };
            let tuple: MergeKey = key_names
                .iter()
                .map(|k| {
                    (
                        k.clone(),
                        Self::canonical_key_value(props.get(k).unwrap_or(&Value::Null)),
                    )
                })
                .collect();
            if keys.contains(&tuple) {
                out.entry(tuple).or_default().push(vid);
                props_by_vid.insert(vid, props);
            }
        }
        Ok((out, props_by_vid))
    }

    /// True if the statement-level MERGE property prefetch is safe for `label`.
    ///
    /// False when the label declares any CRDT-typed property: a prefetch HIT in
    /// [`read_vertex_props_with_prefetch`] skips the `normalize_crdt_properties`
    /// pass that `get_all_vertex_props_with_ctx` applies, so CRDT-bearing
    /// labels keep the per-row read path. Undeclared labels are trivially safe
    /// (normalization is a no-op without schema CRDT entries).
    fn merge_label_prefetch_safe(&self, label: &str) -> bool {
        let schema = self.storage.schema_manager().schema();
        schema.properties.get(label).is_none_or(|props| {
            !props
                .values()
                .any(|pm| matches!(pm.r#type, DataType::Crdt(_)))
        })
    }

    /// True if an L0 override rewrote any key column of a persisted match away
    /// from its requested value (so the persisted row no longer matches).
    fn vid_overrides_break_key(
        vid: Vid,
        key_props: &HashMap<String, Value>,
        ctx: Option<&QueryContext>,
    ) -> bool {
        key_props.iter().any(|(k, want)| {
            matches!(
                l0_visibility::lookup_vertex_prop(vid, k, ctx),
                Some(got) if Self::canonical_key_value(&got) != Self::canonical_key_value(want)
            )
        })
    }

    /// Build the node binding for a MERGE node variable.
    ///
    /// Matches the binding the general MATCH path produces, so ON MATCH SET,
    /// RETURN and downstream operators resolve the variable identically — a
    /// bare `Value::Int(vid)` is not a valid node binding for those consumers.
    /// It said the same thing while producing a `Value::Map` and MATCH produced
    /// a `Value::Node`, which is the divergence this closes.
    fn build_node_map(vid: Vid, label: &str, props: uni_common::Properties) -> Value {
        Self::bind_node(vid, std::slice::from_ref(&label.to_string()), props)
    }

    /// The one node binding: MERGE and CREATE built the same shape twice.
    fn bind_node(vid: Vid, labels: &[String], props: uni_common::Properties) -> Value {
        Value::Node(uni_common::Node {
            vid,
            labels: labels.to_vec(),
            properties: props.into_iter().collect(),
        })
    }

    /// True if an L0-only vertex has every key column set to the requested
    /// value. A missing column matches only a requested `Null`.
    fn l0_vid_matches_key(
        vid: Vid,
        key_props: &HashMap<String, Value>,
        ctx: Option<&QueryContext>,
    ) -> bool {
        key_props.iter().all(
            |(k, want)| match l0_visibility::lookup_vertex_prop(vid, k, ctx) {
                Some(got) => Self::canonical_key_value(&got) == Self::canonical_key_value(want),
                None => *want == Value::Null,
            },
        )
    }

    /// Index fast-path execution for one MERGE row of the shape detected by
    /// [`Self::merge_single_node_fastpath`].
    ///
    /// Resolves matches from the per-batch L0 snapshot `existing` (O(1) lookup,
    /// no per-row L0 enumeration) plus the per-statement persisted prefetch
    /// (`persisted`, built once by [`Self::merge_lookup_persisted_batch`]);
    /// applies ON MATCH SET to every match, or creates the node and applies
    /// ON CREATE SET when there is none. A newly created vertex is folded into
    /// `existing` so a later row of the same batch with the same key matches it
    /// (intra-batch dedup). Returns the RETURN rows for this input row (one per
    /// match, or one for a create).
    ///
    /// `prefetched` is the statement-level property prefetch (`None` when the
    /// label is CRDT-bearing, see [`Self::merge_label_prefetch_safe`]): matched
    /// vids carry their persisted base row, freshly created vids are seeded
    /// with an empty base — per-row reads then resolve as base + L0 layering
    /// (every SET flush writes the full row to L0 before the next read, so a
    /// prefetch hit equals a fresh read) instead of one storage scan each.
    ///
    /// # Errors
    /// Propagates evaluation, create, and SET failures.
    #[expect(
        clippy::too_many_arguments,
        reason = "mirrors execute_merge's threaded execution state"
    )]
    async fn execute_merge_row_indexed(
        &self,
        label: &str,
        node: &NodePattern,
        path_pattern: &Pattern,
        temp_vars: &[String],
        mut row: HashMap<String, Value>,
        key_props: &HashMap<String, Value>,
        persisted: &HashMap<MergeKey, Vec<Vid>>,
        key_tuple: &MergeKey,
        existing: &mut HashMap<MergeKey, Vec<Vid>>,
        on_match: Option<&SetClause>,
        on_create: Option<&SetClause>,
        prop_manager: &PropertyManager,
        params: &HashMap<String, Value>,
        ctx: Option<&QueryContext>,
        tx_l0_override: Option<&Arc<parking_lot::RwLock<uni_store::runtime::l0::L0Buffer>>>,
        writer: &Writer,
        mut prefetched: Option<&mut Prefetch>,
    ) -> Result<Vec<HashMap<String, Value>>> {
        let empty_prefetch = Prefetch::default();
        let mut seen: HashSet<Vid> = HashSet::new();
        let mut matches: Vec<Vid> = Vec::new();
        // Persisted (flushed) matches from the per-statement prefetch. The
        // prefetch is static for the statement, so re-verify liveness at row
        // time — an earlier row of this batch may have deleted the candidate
        // or rewritten its key (the old per-row scan saw those through its L0
        // overlay checks; these are the same checks, moved to row time).
        if let Some(vids) = persisted.get(key_tuple) {
            for &vid in vids {
                if l0_visibility::is_vertex_deleted(vid, ctx) {
                    continue;
                }
                if Self::vid_overrides_break_key(vid, key_props, ctx) {
                    continue;
                }
                if seen.insert(vid) {
                    matches.push(vid);
                }
            }
        }
        // L0 / intra-batch matches from the per-batch snapshot, re-verified live
        // in case a prior row of this batch mutated or deleted the candidate.
        if let Some(vids) = existing.get(key_tuple) {
            for &vid in vids {
                if seen.contains(&vid) || l0_visibility::is_vertex_deleted(vid, ctx) {
                    continue;
                }
                if Self::l0_vid_matches_key(vid, key_props, ctx) && seen.insert(vid) {
                    matches.push(vid);
                }
            }
        }

        let mut out = Vec::new();
        if matches.is_empty() {
            // No match: create the node, then apply ON CREATE SET. Fold the
            // ON CREATE SET property assignments into seed props first so a
            // NOT-NULL property supplied only by ON CREATE SET passes
            // create-time validation (RC4); the post-create SET below settles
            // the final values.
            let seed_props = self
                .on_create_seed_props(on_create, &row, prop_manager, params, ctx)
                .await?;
            self.execute_create_pattern(
                path_pattern,
                &mut row,
                writer,
                prop_manager,
                params,
                ctx,
                tx_l0_override,
                Some(&seed_props),
            )
            .await?;
            // Fold the new vertex into the batch snapshot for intra-batch
            // dedup, and seed the statement prefetch with an empty base: a
            // fresh vid has nothing in storage, so ON CREATE SET's lazy read
            // resolves from the L0 row the create just wrote instead of
            // issuing a per-row storage scan that finds nothing.
            if let Some(var) = &node.variable
                && let Some(val) = row.get(var)
                && let Ok(vid) = Self::vid_from_value(val)
            {
                existing.entry(key_tuple.clone()).or_default().push(vid);
                if let Some(p) = prefetched.as_deref_mut() {
                    p.vertex.entry(vid).or_default();
                }
                // Phantom guard (RC2): register this MERGE-create's key so a
                // concurrent MERGE of the same key aborts retriably at commit
                // (converging to one node) instead of silently duplicating —
                // even with no declared UNIQUE constraint. Only inside a
                // transaction, where commit re-probes the guard; a plain CREATE
                // never registers a key, so it is unaffected.
                if let Some(tx_l0) = tx_l0_override {
                    let key_values: Vec<(String, Value)> = key_props
                        .iter()
                        .map(|(k, v)| (k.clone(), v.clone()))
                        .collect();
                    let guard_key =
                        uni_store::runtime::l0::serialize_constraint_key(label, &key_values);
                    tx_l0.write().insert_merge_guard_key(guard_key, vid);
                }
            }
            if let Some(set) = on_create {
                self.execute_set_items_locked(
                    &set.items,
                    &mut row,
                    writer,
                    prop_manager,
                    params,
                    ctx,
                    tx_l0_override,
                    prefetched.as_deref().unwrap_or(&empty_prefetch),
                )
                .await?;
            }
            Self::bind_path_variables(path_pattern, &mut row, temp_vars)?;
            out.push(row);
        } else {
            // Apply ON MATCH SET to every matched node (multi-match semantics),
            // binding the node variable as a Map with _vid/_labels/props so
            // RETURN and downstream operators resolve it as they would for the
            // general MATCH and CREATE paths.
            for vid in matches {
                let mut m = row.clone();
                if let Some(var) = &node.variable {
                    // Bind with the node's properties, not just its `_vid`
                    // (#220) — but only when there is an `ON MATCH SET` to read
                    // them. They are already in hand (the per-statement
                    // persisted lookup read them for every matched vid), and
                    // binding them empty is what made `ON MATCH SET n.p = n.p +
                    // x` fall back to a single-key storage read *per row*:
                    // `evaluate_expr` checks the in-memory `Node` first and only
                    // then asks storage. Measured at 617 such reads in one test
                    // subset, all for `n.freq` in the MERGE-in-UNWIND
                    // accumulate shape.
                    //
                    // Gated on `on_match` because the rebind below runs
                    // regardless, for RETURN fidelity. Reading here as well when
                    // nothing consumes it made a plain `MERGE (a)-[e]->(b)` pay
                    // two property reads per matched node where it used to pay
                    // one — pure waste on the commonest MERGE shape, which has
                    // no `ON MATCH SET` at all.
                    let props = if on_match.is_some() {
                        read_vertex_props_with_prefetch(
                            vid,
                            prefetched.as_deref().unwrap_or(&empty_prefetch),
                            prop_manager,
                            ctx,
                        )
                        .await?
                    } else {
                        HashMap::new()
                    };
                    m.insert(var.clone(), Self::build_node_map(vid, label, props));
                }
                if let Some(set) = on_match {
                    self.execute_set_items_locked(
                        &set.items,
                        &mut m,
                        writer,
                        prop_manager,
                        params,
                        ctx,
                        tx_l0_override,
                        prefetched.as_deref().unwrap_or(&empty_prefetch),
                    )
                    .await?;
                }
                if let Some(var) = &node.variable {
                    // Rebind with full, post-SET properties for RETURN
                    // fidelity. The SET above flushed the full row to L0, so a
                    // prefetch hit (base + L0 layering) reproduces exactly
                    // what a fresh storage read would return.
                    let props = read_vertex_props_with_prefetch(
                        vid,
                        prefetched.as_deref().unwrap_or(&empty_prefetch),
                        prop_manager,
                        ctx,
                    )
                    .await?;
                    m.insert(var.clone(), Self::build_node_map(vid, label, props));
                }
                Self::bind_path_variables(path_pattern, &mut m, temp_vars)?;
                out.push(m);
            }
        }
        Ok(out)
    }

    #[expect(clippy::too_many_arguments)]
    pub(crate) async fn execute_merge(
        &self,
        rows: Vec<HashMap<String, Value>>,
        pattern: &Pattern,
        on_match: Option<&SetClause>,
        on_create: Option<&SetClause>,
        prop_manager: &PropertyManager,
        params: &HashMap<String, Value>,
        ctx: Option<&QueryContext>,
        tx_l0_override: Option<&Arc<parking_lot::RwLock<uni_store::runtime::l0::L0Buffer>>>,
    ) -> Result<Vec<HashMap<String, Value>>> {
        let writer_lock = self
            .writer
            .as_ref()
            .ok_or_else(|| anyhow!("Write operation requires a Writer"))?;

        // Prepare pattern for path variable binding: assign temp edge variable
        // names to unnamed relationships in paths that have path variables.
        let (path_pattern, temp_vars) = Self::prepare_pattern_for_path_binding(pattern);

        // Issue #69: a single-node, single-label MERGE takes the fast path,
        // skipping the per-row query planning that made batched MERGE no faster
        // than a per-entity loop. Indexed keys get an index point-lookup;
        // un-indexed keys still skip planning (the lookup is a filtered scan).
        // The shape is the same for every row, so it is detected once.
        let fastpath = self.merge_single_node_fastpath(&path_pattern);

        // The same batched key resolution serves a relationship MERGE whose far
        // endpoint is found-or-created. Detected here rather than beside the
        // other relationship fast path because the key batch below has to be
        // built for it, and that runs first. `on_match` is checked at the point
        // of use, as the other relationship path does.
        let rel_keyed = if fastpath.is_none() {
            self.merge_relationship_keyed_fastpath_shape(pattern)
        } else {
            None
        };

        // Build the per-batch L0 snapshot once (issue #69 Phase C): the per-row
        // fast path then resolves L0/intra-batch matches with an O(1) lookup
        // instead of re-walking L0 for every row. `key_names` is the sorted
        // static key set, matching `merge_key_tuple`.
        let mut fast_existing: HashMap<MergeKey, Vec<Vid>> = HashMap::new();
        // Per-row pre-evaluated fast-path keys (None = that row falls back to
        // the general path), and the per-statement persisted prefetch over the
        // deduped key tuples — ONE chunked scan instead of one scan per row.
        // Key expressions only see the row's own bindings + params, so
        // evaluating them ahead of any creates cannot observe earlier rows.
        let mut row_fast: Vec<Option<(HashMap<String, Value>, MergeKey)>> = Vec::new();
        let mut fast_persisted: HashMap<MergeKey, Vec<Vid>> = HashMap::new();
        // Statement-level property prefetch for the fast path (review perf
        // residual): every persisted match's full row is batch-read ONCE, so
        // the per-row ON MATCH SET read and the post-SET rebind resolve as
        // prefetch-base + L0 layering instead of one storage scan each.
        // `None` disables it for CRDT-bearing labels (the prefetch-hit read
        // skips CRDT normalization).
        let mut merge_prefetch: Option<Prefetch> = None;
        // Either shape resolves its keys the same way: the machinery is keyed on
        // `(label, key_names)`, not on the pattern it came from.
        let key_source: Option<(&NodePattern, &String)> = match (&fastpath, &rel_keyed) {
            (Some((node, label)), _) => Some((node, label)),
            (None, Some(shape)) => Some((shape.keyed, &shape.keyed_label)),
            _ => None,
        };
        let key_source =
            key_source.filter(|(node, label)| self.merge_keys_are_stored_columns(node, label));
        if let Some((node, label)) = key_source {
            let mut key_names: Vec<String> = match &node.properties {
                Some(Expr::Map(entries)) => entries.iter().map(|(k, _)| k.clone()).collect(),
                _ => Vec::new(),
            };
            key_names.sort();
            fast_existing = self.merge_l0_existing(label, &key_names, ctx);

            row_fast.reserve(rows.len());
            for row in &rows {
                let mut key_props: HashMap<String, Value> = HashMap::new();
                if let Some(props_expr) = &node.properties
                    && let Value::Map(map) = self
                        .evaluate_expr(props_expr, row, prop_manager, params, ctx)
                        .await?
                {
                    key_props = map;
                }
                // Only rows whose every key value is a scalar the persisted
                // scan can express take the fast path (same gate as before,
                // via the filter builder).
                if Self::merge_key_is_fast_pathable(&key_props) {
                    let tuple = Self::merge_key_tuple(&key_props);
                    row_fast.push(Some((key_props, tuple)));
                } else {
                    row_fast.push(None);
                }
            }
            let unique_keys: HashSet<MergeKey> = row_fast
                .iter()
                .flatten()
                .map(|(_, tuple)| tuple.clone())
                .collect();
            let (persisted, schemaless_props) = self
                .merge_lookup_persisted_batch(label, &key_names, &unique_keys)
                .await?;
            fast_persisted = persisted;
            if self.merge_label_prefetch_safe(label) {
                let mut pf = Prefetch::default();
                if !schemaless_props.is_empty() {
                    // The schemaless lookup already decoded each matched vid's
                    // full property map — zero extra scans.
                    pf.vertex.extend(schemaless_props);
                } else {
                    let vids: Vec<Vid> = fast_persisted
                        .values()
                        .flatten()
                        .copied()
                        .collect::<HashSet<Vid>>()
                        .into_iter()
                        .collect();
                    if !vids.is_empty()
                        && let Ok(batch_props) = prop_manager
                            .get_batch_vertex_props_for_label(&vids, label, ctx)
                            .await
                    {
                        // One `_vid IN (…)` scan for every matched row's base.
                        // On Err the map stays empty — every read falls back to
                        // the per-row path (fail-open, same posture as
                        // prefetch_set_targets).
                        pf.vertex.extend(batch_props);
                    }
                }
                merge_prefetch = Some(pf);
            }
        }

        // RC3: relationship-MERGE existence fast-path. The single-node fast path
        // above does not cover `(a)-[:R]->(b)`; the general path rebuilds and runs
        // a per-row traversal `LogicalPlan` just to check whether the edge exists
        // (~19x the bulk CREATE of the same edges). For the bound-endpoints shape
        // (and no ON MATCH SET, whose match-row semantics the general path
        // materialises) we resolve existence with one MVCC-correct
        // adjacency probe — `GraphExecutionContext::get_neighbors` merges CSR + all
        // L0 buffers including the transaction's own writes, so intra-batch edges
        // are seen — and reuse the general create / ON CREATE handling unchanged.
        // An ON MATCH SET with actual items needs the general path's materialised
        // match rows; a plain MERGE carries an *empty* on_match, which the fast
        // path can serve (it emits the row directly, applying nothing on match).
        let on_match_empty = on_match.is_none_or(|s| s.items.is_empty());
        let rel_fast = if fastpath.is_none() && on_match_empty {
            self.merge_relationship_fastpath_shape(pattern)
        } else {
            None
        };
        let rel_keyed_active = rel_keyed.is_some() && on_match_empty;
        let rel_graph_ctx = (rel_fast.is_some() || rel_keyed_active).then(|| {
            let l0_context = match ctx {
                Some(c) => crate::query::df_graph::L0Context::from_query_context(c),
                None => crate::query::df_graph::L0Context::empty(),
            };
            let pm_arc = self.prop_manager_arc.clone().unwrap_or_else(|| {
                Arc::new(PropertyManager::new(
                    self.storage.clone(),
                    self.storage.schema_manager_arc(),
                    prop_manager.cache_size(),
                ))
            });
            let mut gctx = crate::query::df_graph::GraphExecutionContext::with_l0_context(
                self.effective_storage(),
                l0_context,
                pm_arc,
            );
            // Carry the statement's budget into the fastpath's operators; see #207.
            if let Some(c) = ctx {
                if let Some(deadline) = c.deadline {
                    gctx = gctx.with_deadline(deadline);
                }
                if let Some(token) = c.cancellation_token.clone() {
                    gctx = gctx.with_cancellation_token(token);
                }
            }
            // And the counters, for the same reason and with the same
            // omission: `with_l0_context` defaults them to `None`, so this
            // path's adjacency probes counted into nothing however it was
            // reached. A lost deadline is loud; a lost counter just reads low.
            gctx = gctx.with_counters(Some(self.counters.clone()));
            gctx
        });

        let mut results = Vec::new();
        for (idx, mut row) in rows.into_iter().enumerate() {
            // Rows with a pre-evaluated scalar key take the fast path; rows
            // with a non-scalar key fall through to the general path below.
            if let Some((node, label)) = &fastpath
                && let Some((key_props, key_tuple)) = row_fast.get(idx).and_then(|rf| rf.as_ref())
            {
                let writer: &uni_store::Writer = writer_lock.as_ref();
                let row_out = self
                    .execute_merge_row_indexed(
                        label,
                        node,
                        &path_pattern,
                        &temp_vars,
                        row,
                        key_props,
                        &fast_persisted,
                        key_tuple,
                        &mut fast_existing,
                        on_match,
                        on_create,
                        prop_manager,
                        params,
                        ctx,
                        tx_l0_override,
                        writer,
                        merge_prefetch.as_mut(),
                    )
                    .await?;
                results.extend(row_out);
                continue;
            }

            // Keyed-endpoint relationship fast path: one endpoint bound, the
            // other found-or-created by key.
            //
            // The decision is made before anything is written, so falling
            // through leaves the row exactly as it was found.
            if let (true, Some(shape), Some(graph_ctx)) =
                (rel_keyed_active, rel_keyed.as_ref(), rel_graph_ctx.as_ref())
                && let Some(Some((key_props, key_tuple))) = row_fast.get(idx)
                && let Some(bound_vid) = row
                    .get(&shape.bound_var)
                    .and_then(|v| Self::vid_from_value(v).ok())
            {
                // Live candidates for this key, re-verified: the persisted map
                // is a snapshot, and an earlier row of this batch may have
                // deleted a candidate or rewritten its key.
                let candidates: Vec<Vid> = fast_persisted
                    .get(key_tuple)
                    .into_iter()
                    .flatten()
                    .chain(fast_existing.get(key_tuple).into_iter().flatten())
                    .copied()
                    .filter(|vid| {
                        !uni_store::runtime::l0_visibility::is_vertex_deleted(*vid, ctx)
                            && !Self::vid_overrides_break_key(*vid, key_props, ctx)
                    })
                    .collect();

                // A whole-pattern match needs a candidate that is also a
                // neighbour. A candidate that is NOT a neighbour must not be
                // reused — the pattern misses and a new node is created, which
                // is what `merge_keyed_endpoint_semantics` pins.
                let matched = if candidates.is_empty() {
                    None
                } else {
                    let by_vid: std::collections::HashSet<Vid> =
                        candidates.iter().copied().collect();
                    // The eid travels with the hit: a named relationship is
                    // bound from it below.
                    let hits: Vec<(Vid, uni_common::core::id::Eid)> = graph_ctx
                        .get_neighbors(bound_vid, shape.type_id, shape.probe_dir)
                        .into_iter()
                        .filter(|(n, _)| by_vid.contains(n))
                        .collect();
                    // Two or more matches means the general path's one-row-per-
                    // match semantics, which this branch does not reproduce.
                    match hits.len() {
                        0 => Some(None),
                        1 => Some(Some(hits[0])),
                        // The general path emits one row per match; this branch
                        // does not reproduce that, so hand the row back.
                        // The general path emits one row per match; this branch
                        // does not reproduce that, so hand the row back
                        // untouched. `None` here means "not served".
                        _ => None,
                    }
                };

                // `None` = not served, fall through with the row exactly as it
                // was found; `Some(None)` = create; `Some(Some(vid))` = match.
                if let Some(outcome) = matched {
                    match outcome {
                        Some((vid, eid)) => {
                            let empty = Prefetch::default();
                            let props = read_vertex_props_with_prefetch(
                                vid,
                                merge_prefetch.as_ref().unwrap_or(&empty),
                                prop_manager,
                                ctx,
                            )
                            .await?;
                            row.insert(
                                shape.keyed.variable.clone().unwrap_or_default(),
                                Self::build_node_map(vid, &shape.keyed_label, props),
                            );
                            if let Some(rel_var) = shape.rel_var.as_deref() {
                                self.bind_matched_edge(
                                    &mut row,
                                    rel_var,
                                    eid,
                                    shape.type_id,
                                    bound_vid,
                                    vid,
                                    shape.probe_dir,
                                    prop_manager,
                                    ctx,
                                )
                                .await?;
                            }
                            Self::bind_path_variables(&path_pattern, &mut row, &temp_vars)?;
                            results.push(row);
                            continue;
                        }
                        None => {
                            let writer: &uni_store::Writer = writer_lock.as_ref();
                            let seed_props = self
                                .on_create_seed_props(on_create, &row, prop_manager, params, ctx)
                                .await?;
                            self.execute_create_pattern(
                                &path_pattern,
                                &mut row,
                                writer,
                                prop_manager,
                                params,
                                ctx,
                                tx_l0_override,
                                Some(&seed_props),
                            )
                            .await?;
                            if let Some(set) = on_create {
                                self.execute_set_items_locked(
                                    &set.items,
                                    &mut row,
                                    writer,
                                    prop_manager,
                                    params,
                                    ctx,
                                    tx_l0_override,
                                    &Prefetch::default(),
                                )
                                .await?;
                            }
                            // Fold the new vid forward. Required, not an
                            // optimisation: a later row merging the same key from
                            // the same source must match this node, and it only
                            // counts as a match if it is among the candidates.
                            if let Some(new_vid) = row
                                .get(shape.keyed.variable.as_deref().unwrap_or(""))
                                .and_then(|v| Self::vid_from_value(v).ok())
                            {
                                fast_existing
                                    .entry(key_tuple.clone())
                                    .or_default()
                                    .push(new_vid);
                            }
                            Self::bind_path_variables(&path_pattern, &mut row, &temp_vars)?;
                            results.push(row);
                            continue;
                        }
                    }
                }
            }

            // RC3 relationship fast path: bound endpoints → resolve edge
            // existence with one adjacency probe and reuse the general
            // create / ON CREATE handling, skipping the per-row traversal plan.
            if let (Some((src_var, dst_var, type_id, dir, rel_var)), Some(graph_ctx)) =
                (rel_fast.as_ref(), rel_graph_ctx.as_ref())
            {
                let src_vid = row.get(src_var).and_then(|v| Self::vid_from_value(v).ok());
                let dst_vid = row.get(dst_var).and_then(|v| Self::vid_from_value(v).ok());
                if let (Some(src_vid), Some(dst_vid)) = (src_vid, dst_vid) {
                    // Every edge of this type between the two endpoints, not
                    // just the first: the eid lets a named relationship be
                    // bound, and the *count* decides whether this branch may
                    // serve the row at all.
                    let matching: Vec<uni_common::core::id::Eid> = graph_ctx
                        .get_neighbors(src_vid, *type_id, *dir)
                        .into_iter()
                        .filter(|(n, _)| *n == dst_vid)
                        .map(|(_, eid)| eid)
                        .collect();
                    let exists = !matching.is_empty();
                    // A named relationship matching more than once has to emit
                    // one row per match, which this branch does not reproduce —
                    // it emits a single row. Hand those to the general path.
                    // (An anonymous relationship binds nothing and keeps the
                    // single-row behaviour it has always had here.)
                    if rel_var.is_some() && matching.len() > 1 {
                        // fall through
                    } else {
                        let writer: &uni_store::Writer = writer_lock.as_ref();
                        if !exists {
                            // Edge absent: create only the edge (endpoints are bound),
                            // then apply ON CREATE SET — identical to the general
                            // create branch below.
                            let seed_props = self
                                .on_create_seed_props(on_create, &row, prop_manager, params, ctx)
                                .await?;
                            self.execute_create_pattern(
                                &path_pattern,
                                &mut row,
                                writer,
                                prop_manager,
                                params,
                                ctx,
                                tx_l0_override,
                                Some(&seed_props),
                            )
                            .await?;
                            if let Some(set) = on_create {
                                self.execute_set_items_locked(
                                    &set.items,
                                    &mut row,
                                    writer,
                                    prop_manager,
                                    params,
                                    ctx,
                                    tx_l0_override,
                                    &Prefetch::default(),
                                )
                                .await?;
                            }
                        }
                        // Whether matched or just created, the edge now exists.
                        // A named relationship that was *created* was bound by
                        // `execute_create_pattern` with the properties it just
                        // wrote; one that *matched* is bound here, from the eid
                        // the probe returned. ON MATCH SET is excluded from this
                        // fast path.
                        if let (Some(rel_var), Some(&eid)) = (rel_var.as_deref(), matching.first())
                        {
                            self.bind_matched_edge(
                                &mut row,
                                rel_var,
                                eid,
                                *type_id,
                                src_vid,
                                dst_vid,
                                *dir,
                                prop_manager,
                                ctx,
                            )
                            .await?;
                        }
                        Self::bind_path_variables(&path_pattern, &mut row, &temp_vars)?;
                        results.push(row);
                        continue;
                    }
                }
                // Endpoints not bound to vids → fall through to the general path.
            }

            // General execution: match-or-create per row. (The index fast path
            // above already handles single-node, single-label, scalar-indexed
            // MERGE — including unique-constrained labels, whose keys are
            // indexed — so there is no separate constraint-only fast path.)
            let matches = self
                // `path_pattern`, not `pattern`: when the MERGE carries a path
                // variable, `prepare_pattern_for_path_binding` names the
                // otherwise-anonymous nodes and relationships so they can be bound. The
                // match ran on the original, which has nothing to bind, while
                // `bind_path_variables` below reads the prepared one — so a
                // matched `MERGE p = (a)-[:R]->(b)` produced a `p` holding its
                // nodes and no relationship. Without a path variable the two
                // are the same pattern.
                .execute_merge_match(&path_pattern, &row, prop_manager, params, ctx)
                .await?;
            let writer: &uni_store::Writer = writer_lock.as_ref();

            let result: Result<Vec<HashMap<String, Value>>> = async {
                let mut batch = Vec::new();
                if !matches.is_empty() {
                    for mut m in matches {
                        if let Some(set) = on_match {
                            self.execute_set_items_locked(
                                &set.items,
                                &mut m,
                                writer,
                                prop_manager,
                                params,
                                ctx,
                                tx_l0_override,
                                &Prefetch::default(),
                            )
                            .await?;
                        }
                        Self::bind_path_variables(&path_pattern, &mut m, &temp_vars)?;
                        batch.push(m);
                    }
                } else {
                    // Fold ON CREATE SET into seed props so a NOT-NULL property
                    // set only by ON CREATE SET passes create-time validation
                    // (RC4); the post-create SET below settles the final values.
                    let seed_props = self
                        .on_create_seed_props(on_create, &row, prop_manager, params, ctx)
                        .await?;
                    self.execute_create_pattern(
                        &path_pattern,
                        &mut row,
                        writer,
                        prop_manager,
                        params,
                        ctx,
                        tx_l0_override,
                        Some(&seed_props),
                    )
                    .await?;
                    if let Some(set) = on_create {
                        self.execute_set_items_locked(
                            &set.items,
                            &mut row,
                            writer,
                            prop_manager,
                            params,
                            ctx,
                            tx_l0_override,
                            &Prefetch::default(),
                        )
                        .await?;
                    }
                    Self::bind_path_variables(&path_pattern, &mut row, &temp_vars)?;
                    batch.push(row);
                }
                Ok(batch)
            }
            .await;

            results.extend(result?);
        }
        Ok(results)
    }

    /// Pre-evaluate `ON CREATE SET` property assignments into per-variable seeds.
    ///
    /// Folds `SET <var>.<prop> = <expr>` items so a NOT-NULL property supplied
    /// only by `ON CREATE SET` is present when the MERGE node is created and
    /// passes constraint validation (RC4). The right-hand side is evaluated
    /// against the current `row`.
    ///
    /// Items whose right-hand side references the target variable (e.g.
    /// `ON CREATE SET n.c = coalesce(n.c, 0) + 1`) are NOT folded: seeding would
    /// let the post-create SET read the seeded value and apply the assignment
    /// twice. Such items run only post-create, exactly once (unchanged behavior).
    ///
    /// # Errors
    /// Returns an error if evaluating an assignment's right-hand side fails.
    pub(crate) async fn on_create_seed_props(
        &self,
        on_create: Option<&SetClause>,
        row: &HashMap<String, Value>,
        prop_manager: &PropertyManager,
        params: &HashMap<String, Value>,
        ctx: Option<&QueryContext>,
    ) -> Result<HashMap<String, HashMap<String, Value>>> {
        let mut seed: HashMap<String, HashMap<String, Value>> = HashMap::new();
        let Some(set) = on_create else {
            return Ok(seed);
        };
        for item in &set.items {
            if let SetItem::Property { expr, value } = item
                && let Expr::Property(var_expr, prop_name) = expr
                && let Expr::Variable(var_name) = &**var_expr
                // Skip self-referential RHS so the post-create SET (which also
                // runs) applies it exactly once rather than reading the seed.
                && !crate::query::df_graph::locy_ast_builder::expr_references_var(
                    value, var_name,
                )
            {
                let val = self
                    .evaluate_expr(value, row, prop_manager, params, ctx)
                    .await?;
                seed.entry(var_name.clone())
                    .or_default()
                    .insert(prop_name.clone(), val);
            }
        }
        Ok(seed)
    }

    /// Execute a CREATE pattern, inserting new vertices and edges into the graph.
    #[expect(clippy::too_many_arguments)]
    pub(crate) async fn execute_create_pattern(
        &self,
        pattern: &Pattern,
        row: &mut HashMap<String, Value>,
        writer: &Writer,
        prop_manager: &PropertyManager,
        params: &HashMap<String, Value>,
        ctx: Option<&QueryContext>,
        tx_l0: Option<&Arc<parking_lot::RwLock<uni_store::runtime::l0::L0Buffer>>>,
        // Per-variable properties to gap-fill into newly-created nodes before
        // constraint validation. Used by MERGE to fold `ON CREATE SET` so a
        // NOT-NULL property supplied only by ON CREATE SET passes create-time
        // validation (RC4). `None` for plain CREATE.
        seed_props: Option<&HashMap<String, HashMap<String, Value>>>,
    ) -> Result<()> {
        for path in &pattern.paths {
            let mut prev_vid: Option<Vid> = None;
            // (rel_var, type_id, type_name, props_expr, direction)
            type PendingRel = (String, u32, String, Option<Expr>, Direction);
            let mut rel_pending: Option<PendingRel> = None;

            for element in &path.elements {
                match element {
                    PatternElement::Node(n) => {
                        let mut vid = None;

                        // Check if node variable already bound in row
                        if let Some(var) = &n.variable
                            && let Some(val) = row.get(var)
                            && let Ok(existing_vid) = Self::vid_from_value(val)
                        {
                            vid = Some(existing_vid);
                        }

                        // If not bound, create it
                        if vid.is_none() {
                            let mut props = HashMap::new();
                            if let Some(props_expr) = &n.properties {
                                let props_val = self
                                    .evaluate_expr(props_expr, row, prop_manager, params, ctx)
                                    .await?;
                                if let Value::Map(map) = props_val {
                                    for (k, v) in map {
                                        props.insert(k, v);
                                    }
                                } else {
                                    return Err(anyhow!("Properties must evaluate to a map"));
                                }
                            }

                            // MERGE ON CREATE SET: gap-fill properties supplied
                            // only by ON CREATE SET so a NOT-NULL property absent
                            // from the merge key passes create-time validation
                            // (RC4). `or_insert` keeps the merge-key/pattern props
                            // authoritative; the post-create SET re-applies the
                            // real values, so the final state is unchanged.
                            if let Some(seed) = seed_props
                                && let Some(var) = &n.variable
                                && let Some(var_seed) = seed.get(var)
                            {
                                for (k, v) in var_seed {
                                    props.entry(k.clone()).or_insert_with(|| v.clone());
                                }
                            }

                            let schema = self.storage.schema_manager().schema();

                            // Strict schema: reject undeclared labels.
                            if self.config.strict_schema {
                                for label_name in &n.labels {
                                    if schema.get_label_case_insensitive(label_name).is_none() {
                                        return Err(anyhow!(
                                            "Label '{}' is not defined in the schema \
                                             (strict_schema is enabled). \
                                             Declare it with db.schema().label(...).apply() first.",
                                            label_name
                                        ));
                                    }
                                }
                            }

                            // VID generation is label-independent. Pull from the
                            // per-tx reservoir if set (amortizes the global
                            // IdAllocator mutex), else fall back to the direct
                            // per-VID path.
                            let new_vid = match &self.id_reservoir {
                                Some(r) => r.next_vid().await?,
                                None => writer.next_vid().await?,
                            };

                            // Enrich with generated columns only for known labels
                            for label_name in &n.labels {
                                if schema.get_label_case_insensitive(label_name).is_some() {
                                    self.enrich_properties_with_generated_columns(
                                        label_name,
                                        &mut props,
                                        prop_manager,
                                        params,
                                        ctx,
                                    )
                                    .await?;
                                }
                            }

                            // Validate/coerce against declared types AFTER enrichment, so
                            // a type mismatch is rejected here rather than silently nulled
                            // (and the row dropped) at flush — issue #68.
                            let props = Self::coerce_and_validate_props(props, &schema, &n.labels)?;

                            // Insert vertex and get back final properties (includes auto-generated embeddings)
                            let final_props = writer
                                .insert_vertex_with_labels(new_vid, props, &n.labels, tx_l0)
                                .await?;

                            // Build node object with final properties (includes embeddings)
                            if let Some(var) = &n.variable {
                                row.insert(
                                    var.clone(),
                                    Self::bind_node(new_vid, &n.labels, final_props.clone()),
                                );
                            }
                            vid = Some(new_vid);
                        }

                        let current_vid = vid.unwrap();

                        if let Some((rel_var, type_id, type_name, rel_props_expr, dir)) =
                            rel_pending.take()
                            && let Some(src) = prev_vid
                        {
                            let is_rel_bound = !rel_var.is_empty() && row.contains_key(&rel_var);

                            if !is_rel_bound {
                                let mut rel_props = HashMap::new();
                                if let Some(expr) = rel_props_expr {
                                    let val = self
                                        .evaluate_expr(&expr, row, prop_manager, params, ctx)
                                        .await?;
                                    if let Value::Map(map) = val {
                                        rel_props.extend(map);
                                    }
                                }
                                // Validate/coerce edge properties against the declared
                                // edge-type schema before storing — issue #68.
                                let edge_schema = self.storage.schema_manager().schema();
                                let rel_props = Self::coerce_and_validate_props(
                                    rel_props,
                                    &edge_schema,
                                    std::slice::from_ref(&type_name),
                                )?;
                                let eid = match &self.id_reservoir {
                                    Some(r) => r.next_eid().await?,
                                    None => writer.next_eid(type_id).await?,
                                };

                                // For incoming edges like (a)<-[:R]-(b), swap so the edge points b -> a
                                let (edge_src, edge_dst) = match dir {
                                    Direction::Incoming => (current_vid, src),
                                    _ => (src, current_vid),
                                };

                                let store_props = !rel_var.is_empty();
                                let user_props = if store_props {
                                    rel_props.clone()
                                } else {
                                    HashMap::new()
                                };

                                writer
                                    .insert_edge(
                                        edge_src,
                                        edge_dst,
                                        type_id,
                                        eid,
                                        rel_props,
                                        Some(type_name.clone()),
                                        tx_l0,
                                    )
                                    .await?;

                                // Edge type name is now stored by insert_edge

                                if store_props {
                                    // The map form spelled the type as the
                                    // numeric `type_id`, which readers had to
                                    // resolve against the schema — and several
                                    // did not, so `type(r)` on a freshly
                                    // created edge answered nothing. The name
                                    // is in hand right here.
                                    row.insert(
                                        rel_var,
                                        Value::Edge(uni_common::Edge {
                                            eid,
                                            edge_type: type_name.clone(),
                                            src: edge_src,
                                            dst: edge_dst,
                                            properties: user_props.into_iter().collect(),
                                        }),
                                    );
                                }
                            }
                        }
                        prev_vid = Some(current_vid);
                    }
                    PatternElement::Relationship(r) => {
                        if r.types.len() != 1 {
                            return Err(anyhow!(
                                "CREATE relationship must specify exactly one type"
                            ));
                        }
                        let type_name = &r.types[0];
                        let type_id = if self.config.strict_schema {
                            let schema = self.storage.schema_manager().schema();
                            schema
                                .edge_type_id_by_name_case_insensitive(type_name)
                                .ok_or_else(|| {
                                    anyhow!(
                                        "Edge type '{}' is not defined in the schema \
                                         (strict_schema is enabled). \
                                         Declare it with db.schema().edge_type(...).apply() first.",
                                        type_name
                                    )
                                })?
                        } else {
                            // Schemaless: get or assign edge type ID (bit 31 = 1 for dynamic).
                            self.storage
                                .schema_manager()
                                .get_or_assign_edge_type_id(type_name)
                        };

                        rel_pending = Some((
                            r.variable.clone().unwrap_or_default(),
                            type_id,
                            type_name.clone(),
                            r.properties.clone(),
                            r.direction.clone(),
                        ));
                    }
                    PatternElement::Parenthesized { .. } => {
                        return Err(anyhow!("Parenthesized pattern not supported in CREATE"));
                    }
                }
            }
        }
        Ok(())
    }

    /// Rejects structural values (maps, nodes, edges, paths, nested lists) in a property.
    ///
    /// These are never valid OpenCypher property values regardless of the declared column
    /// type. A `CypherValue` column is the sole exception and is handled by the caller
    /// before this is reached.
    ///
    /// # Errors
    /// Returns an error if `val` is a map/node/edge/path, or a list containing one.
    fn validate_structural_property_value(prop_name: &str, val: &Value) -> Result<()> {
        match val {
            Value::Map(_) | Value::Node(_) | Value::Edge(_) | Value::Path(_) => {
                anyhow::bail!(
                    "TypeError: InvalidPropertyType - Property '{}' has an invalid type",
                    prop_name
                );
            }
            Value::List(items) => {
                for item in items {
                    if matches!(
                        item,
                        Value::Map(_)
                            | Value::Node(_)
                            | Value::Edge(_)
                            | Value::Path(_)
                            | Value::List(_)
                    ) {
                        anyhow::bail!(
                            "TypeError: InvalidPropertyType - Property '{}' has an invalid type",
                            prop_name
                        );
                    }
                }
            }
            _ => {}
        }
        Ok(())
    }

    /// Validates and coerces `val` against the declared schema type for `prop_name`.
    ///
    /// Returns the value to actually persist. Beyond the structural checks in
    /// [`Self::validate_structural_property_value`], this compares the value against the
    /// column's declared `DataType` and:
    ///
    /// - returns it unchanged when directly storable (including the intentional
    ///   `Int`→`Float`/`Int32` and `Temporal`→`Timestamp` widenings);
    /// - coerces a `Value::String` written into a `Date`/`Time`/`DateTime`/`Duration`
    ///   column into the proper `Temporal` value, using the same parser as the Cypher
    ///   `date()`/`time()`/`datetime()`/`duration()` constructors;
    /// - otherwise returns an error, so a type mismatch is surfaced at the call site
    ///   rather than silently nulled — and the row dropped at flush. See issue #68.
    ///
    /// Undeclared (schemaless) properties and `CypherValue` columns keep their permissive
    /// behavior.
    ///
    /// # Errors
    /// Returns an error if the value's type is incompatible with the declared column type,
    /// or if a string destined for a temporal column is not a valid temporal literal.
    fn coerce_and_validate_property_value(
        prop_name: &str,
        val: Value,
        schema: &uni_common::core::schema::Schema,
        labels: &[String],
    ) -> Result<Value> {
        use uni_common::core::schema::DataType;

        // Resolve the declared type from the first label that declares this property.
        let declared = labels.iter().find_map(|label| {
            schema
                .properties
                .get(label)
                .and_then(|props| props.get(prop_name))
                .map(|meta| &meta.r#type)
        });

        // CypherValue columns accept any value (including maps) — skip all checks.
        if matches!(declared, Some(DataType::CypherValue)) {
            return Ok(val);
        }

        let Some(dt) = declared else {
            // Schemaless property: reject structural values (maps/nodes/edges/paths and
            // lists containing them), otherwise store as-is.
            Self::validate_structural_property_value(prop_name, &val)?;
            return Ok(val);
        };
        Self::coerce_to_declared_type(prop_name, dt, val)
    }

    /// The declared-column half of [`Self::coerce_and_validate_property_value`]:
    /// `val` in the form a column declared `dt` stores, or the type error.
    ///
    /// Also the rule a declaration applies to the values a property already
    /// holds (installed into `uni-store` as its declared-value coercion), so
    /// declaring and then writing agrees with writing and then declaring.
    ///
    /// # Errors
    /// Returns an error if `dt` cannot hold `val`.
    pub(crate) fn coerce_to_declared_type(
        prop_name: &str,
        dt: &uni_common::core::schema::DataType,
        val: Value,
    ) -> Result<Value> {
        use uni_common::core::schema::DataType;

        // CypherValue columns accept any value (including maps).
        if matches!(dt, DataType::CypherValue) {
            return Ok(val);
        }

        // Sparse vectors carry invariants the type system cannot express (strictly
        // ascending unique term ids, finite weights, equal-length arrays, term ids
        // within the declared term space). Canonicalize and validate the native value
        // at ingest so a malformed sparse vector is a clean `TypeError` here, rather
        // than a panic deep in the durable WAL value codec — which reconstructs via
        // `SparseVector::new` and previously `.expect()`ed (issue #95). The degraded
        // `Value::Map` / `Null` forms fall through to `accepts` unchanged.
        if let (DataType::SparseVector { dimensions }, Value::SparseVector { .. }) = (dt, &val) {
            return Self::canonicalize_sparse_vector(prop_name, val, *dimensions);
        }

        // Dense and multi-vector columns: enforce the declared dimensions here so a
        // wrong-length vector is a clean `TypeError` at write time, rather than being
        // silently nulled by the Arrow converters at flush and detonating as an
        // "unexpected null" internal error at shutdown (issue #137). `accepts` below
        // stays shape-only.
        if let Err(e) = dt.check_vector_dims(&val) {
            anyhow::bail!(
                "TypeError: property '{}' is declared {:?} but {}",
                prop_name,
                dt,
                e
            );
        }

        // Directly storable: scalars, the intentional `Int`→`Float`/`Int32` and
        // `Temporal`→`Timestamp` widenings, declared composite columns (`Map`/`List`/
        // `Vector`) receiving their matching value, and `Null` (always accepted).
        if dt.accepts(&val) {
            return Ok(Self::to_declared_form(dt, val));
        }

        // Known-safe coercion: a string into a temporal column is parsed as if it had
        // been wrapped in the matching Cypher temporal constructor.
        if matches!(val, Value::String(_)) {
            let ctor = match dt {
                DataType::DateTime => Some("DATETIME"),
                DataType::Date => Some("DATE"),
                DataType::Time => Some("TIME"),
                DataType::Duration => Some("DURATION"),
                _ => None,
            };
            if let Some(name) = ctor {
                return uni_query_functions::datetime::eval_datetime_function(
                    name,
                    std::slice::from_ref(&val),
                )
                .map_err(|e| {
                    anyhow!(
                        "TypeError: property '{}' is declared {:?} but the string value could \
                         not be parsed as a {} literal: {}",
                        prop_name,
                        dt,
                        name,
                        e
                    )
                });
            }
        }

        // Not storable and not coercible. Prefer the structural message when the value
        // is itself structural (e.g. a map into a scalar column), preserving prior
        // behavior; otherwise report the scalar type mismatch.
        Self::validate_structural_property_value(prop_name, &val)?;
        anyhow::bail!(
            "TypeError: property '{}' is declared {:?} but got an incompatible value of type {}",
            prop_name,
            dt,
            value_type_name(&val)
        );
    }

    /// Rewrites an accepted value into the form a flushed row of the declared
    /// column reads back as.
    ///
    /// [`DataType::accepts`](uni_common::core::schema::DataType::accepts)
    /// admits lossless widenings: an `Int` into a float column, a numeric
    /// `List` into a `VECTOR` (or a list of them into a multi-vector column).
    /// Stored as given, L0 held `Int` / `List[Int]` while the flushed row held
    /// `Float` / `Vector`, so `RETURN n` and `properties(n)` changed type
    /// across a flush. Every other accepted value is returned unchanged.
    fn to_declared_form(dt: &uni_common::core::schema::DataType, val: Value) -> Value {
        use uni_common::core::schema::DataType;
        fn as_vector(items: &[Value]) -> Option<Vec<f32>> {
            items
                .iter()
                .map(|v| match v {
                    Value::Int(i) => Some(*i as f32),
                    Value::Float(f) => Some(*f as f32),
                    _ => None,
                })
                .collect()
        }
        match (dt, val) {
            (DataType::Float32 | DataType::Float64, Value::Int(i)) => Value::Float(i as f64),
            (DataType::Vector { .. }, Value::List(items)) => match as_vector(&items) {
                Some(v) => Value::Vector(v),
                None => Value::List(items),
            },
            (DataType::List(inner), Value::List(tokens))
                if matches!(inner.as_ref(), DataType::Vector { .. }) =>
            {
                Value::List(
                    tokens
                        .into_iter()
                        .map(|t| match t {
                            Value::List(items) => match as_vector(&items) {
                                Some(v) => Value::Vector(v),
                                None => Value::List(items),
                            },
                            other => other,
                        })
                        .collect(),
                )
            }
            (_, val) => val,
        }
    }

    /// Canonicalizes and validates a value destined for a `SparseVector` column.
    ///
    /// Sorts term ids ascending and sums the weights of duplicate term ids (via
    /// [`uni_sparse_vector::SparseVector::from_pairs`]), then rejects mismatched array
    /// lengths, non-finite weights, and term ids outside the declared `dimensions` term
    /// space. Running this at the write boundary keeps the durable WAL codec's
    /// `SparseVector::new(..)` reconstruction infallible and mirrors the auto-embed
    /// path, which canonicalizes through the same kernel constructor (issue #95).
    ///
    /// # Errors
    /// Returns a `TypeError` if the value violates a sparse-vector invariant or carries
    /// a term id at or beyond `dimensions`.
    fn canonicalize_sparse_vector(prop_name: &str, val: Value, dimensions: usize) -> Result<Value> {
        let Value::SparseVector { indices, values } = val else {
            // The caller only routes native `Value::SparseVector` values here.
            anyhow::bail!(
                "TypeError: property '{}' is declared SparseVector but got {}",
                prop_name,
                value_type_name(&val)
            );
        };
        if indices.len() != values.len() {
            anyhow::bail!(
                "TypeError: property '{}' sparse vector has {} term ids but {} weights",
                prop_name,
                indices.len(),
                values.len()
            );
        }
        let pairs: Vec<(u32, f32)> = indices.into_iter().zip(values).collect();
        let sv = uni_sparse_vector::SparseVector::from_pairs(pairs).map_err(|e| {
            anyhow!(
                "TypeError: property '{}' has an invalid sparse vector: {}",
                prop_name,
                e
            )
        })?;
        // `dimensions` is the term-space cardinality (max term id + 1); the largest
        // term id of a canonical (ascending) vector is its last index.
        if let Some(&max_term) = sv.indices().last()
            && max_term as usize >= dimensions
        {
            anyhow::bail!(
                "TypeError: property '{}' sparse vector term id {} is outside the declared \
                 term space (dimensions = {})",
                prop_name,
                max_term,
                dimensions
            );
        }
        let (indices, values) = sv.into_parts();
        Ok(Value::SparseVector { indices, values })
    }

    /// Coerces and validates every property in `props` against the declared types for `labels`.
    ///
    /// Applies [`Self::coerce_and_validate_property_value`] to each entry, returning the map
    /// with known-safe coercions applied. Use this at every user-facing CREATE/SET write site
    /// before handing properties to the writer, so a type mismatch is rejected up front rather
    /// than silently nulled — and the row dropped — at flush (issue #68).
    ///
    /// # Errors
    /// Returns an error on the first property whose value is incompatible with its declared type.
    fn coerce_and_validate_props(
        props: HashMap<String, Value>,
        schema: &uni_common::core::schema::Schema,
        labels: &[String],
    ) -> Result<HashMap<String, Value>> {
        let mut out = HashMap::with_capacity(props.len());
        for (k, v) in props {
            let cv = Self::coerce_and_validate_property_value(&k, v, schema, labels)?;
            out.insert(k, cv);
        }
        Ok(out)
    }

    #[expect(clippy::too_many_arguments)]
    pub(crate) async fn execute_set_items_locked(
        &self,
        items: &[SetItem],
        row: &mut HashMap<String, Value>,
        writer: &Writer,
        prop_manager: &PropertyManager,
        params: &HashMap<String, Value>,
        ctx: Option<&QueryContext>,
        tx_l0: Option<&Arc<parking_lot::RwLock<uni_store::runtime::l0::L0Buffer>>>,
        prefetched: &Prefetch,
    ) -> Result<()> {
        // Coalesce SetItem::Property items by target so we do ONE read + ONE
        // write per (variable, target) instead of one read-modify-write cycle
        // per item. For an UPDATE that sets N properties on the same vertex
        // (e.g. the ingest hotpath `SET n.frequency = ..., n.last_seen = ...,
        // n.confidence = ...`), this collapses N redundant
        // `get_all_vertex_props_with_ctx` + `insert_vertex_with_labels` cycles
        // into one. See profile_test.rs `diag_72_set_data_scale_with_hnsw` for
        // the measurement, and the plan in
        // /home/rohit/.claude/plans/plan-and-implement-a-valiant-flame.md
        // for the rationale.
        //
        // RHS evaluation order is preserved: we evaluate each RHS inline and
        // update the row binding immediately, so a later SetItem on the same
        // variable that reads `n.<earlier-prop>` sees the new value.
        //
        // Non-Property variants (Labels, Variable, VariablePlus) are less
        // common and have lower payoff; before processing one, we flush any
        // pending updates for the same variable so it sees the latest L0
        // state and ordering semantics are preserved.
        let mut pending_v: HashMap<String, PendingVertexSet> = HashMap::new();
        let mut pending_e: HashMap<String, PendingEdgeSet> = HashMap::new();

        for item in items {
            match item {
                SetItem::Property { expr, value } => {
                    if let Expr::Property(var_expr, prop_name) = expr
                        && let Expr::Variable(var_name) = &**var_expr
                        && let Some(node_val) = row.get(var_name)
                    {
                        if let Ok(vid) = Self::vid_from_value(node_val) {
                            reject_if_ephemeral_vid(vid)?;
                            let labels = self.current_vertex_labels(vid, node_val, ctx);
                            let schema = self.storage.schema_manager().schema().clone();

                            // Lazy one-time read. Always read the full row
                            // (preserves CRDT merge + constraint validation
                            // + scan-side L0 visibility); the SET stores it
                            // back whole.
                            if !pending_v.contains_key(var_name) {
                                let read = read_vertex_props_with_prefetch(
                                    vid,
                                    prefetched,
                                    prop_manager,
                                    ctx,
                                )
                                .await?;
                                pending_v.insert(
                                    var_name.clone(),
                                    PendingVertexSet {
                                        vid,
                                        labels: labels.clone(),
                                        props: read,
                                        touched: HashSet::new(),
                                        before: HashMap::new(),
                                    },
                                );
                            }

                            let val = self
                                .evaluate_expr(value, row, prop_manager, params, ctx)
                                .await?;
                            let val = Self::coerce_and_validate_property_value(
                                prop_name, val, &schema, &labels,
                            )?;

                            let pv = pending_v
                                .get_mut(var_name)
                                .expect("inserted above when absent");
                            if !pv.before.contains_key(prop_name) {
                                let old = pv.props.get(prop_name).cloned();
                                pv.before.insert(prop_name.clone(), old);
                            }
                            pv.props.insert(prop_name.clone(), val.clone());
                            // Record every SET-assigned key. For the partial path this
                            // drives the MergeInsert source; for the full path it is the
                            // signal `refresh_embed_targets` uses to re-embed when a
                            // source column changed (the full writer ignores it otherwise).
                            pv.touched.insert(prop_name.clone());

                            // Update the row binding so subsequent RHS sees the new value.
                            // Assigning null *removes* the property on a native
                            // entity — inserting `Null` there would leave it
                            // visible to `properties()` and `keys()` (#234).
                            if let Some(binding) = row.get_mut(var_name) {
                                binding.set_entity_property(prop_name, val);
                            }
                        } else if let Value::Map(map) = node_val
                            && map.get("_eid").is_some_and(|v| !v.is_null())
                            && map.get("_src").is_some_and(|v| !v.is_null())
                            && map.get("_dst").is_some_and(|v| !v.is_null())
                            && (map.get("_type").is_some_and(|v| !v.is_null())
                                || map.get("_type_name").is_some_and(|v| !v.is_null()))
                        {
                            let ei = self.extract_edge_identity(map)?;
                            reject_if_ephemeral_eid(ei.eid)?;
                            let schema = self.storage.schema_manager().schema().clone();
                            // Handle _type as either String or Int (Int from CREATE, String
                            // from queries). UNWIND on VLP edge lists emits `_type_name`
                            // instead of `_type`; accept either.
                            let type_val = map.get("_type").or_else(|| map.get("_type_name"));
                            let edge_type_name = match type_val {
                                Some(Value::String(s)) => s.clone(),
                                Some(Value::Int(id)) => schema
                                    .edge_type_name_by_id_unified(*id as u32)
                                    .unwrap_or_else(|| format!("EdgeType{}", id)),
                                _ => String::new(),
                            };

                            if !pending_e.contains_key(var_name) {
                                let initial = read_edge_props_with_prefetch(
                                    ei.eid,
                                    prefetched,
                                    prop_manager,
                                    ctx,
                                )
                                .await?;
                                pending_e.insert(
                                    var_name.clone(),
                                    PendingEdgeSet {
                                        src: ei.src,
                                        dst: ei.dst,
                                        edge_type_id: ei.edge_type_id,
                                        eid: ei.eid,
                                        edge_type_name: edge_type_name.clone(),
                                        props: initial,
                                        before: HashMap::new(),
                                    },
                                );
                            }

                            let val = self
                                .evaluate_expr(value, row, prop_manager, params, ctx)
                                .await?;
                            let val = Self::coerce_and_validate_property_value(
                                prop_name,
                                val,
                                &schema,
                                std::slice::from_ref(&edge_type_name),
                            )?;

                            let pe = pending_e
                                .get_mut(var_name)
                                .expect("inserted above when absent");
                            if !pe.before.contains_key(prop_name) {
                                let old = pe.props.get(prop_name).cloned();
                                pe.before.insert(prop_name.clone(), old);
                            }
                            pe.props.insert(prop_name.clone(), val.clone());

                            // Update the row object so subsequent RHS sees the new value.
                            if let Some(binding) = row.get_mut(var_name) {
                                binding.set_entity_property(prop_name, val);
                            }
                        } else if let Value::Edge(edge) = node_val {
                            // Handle Value::Edge directly (when traverse returns Edge objects).
                            reject_if_ephemeral_eid(edge.eid)?;
                            let eid = edge.eid;
                            let src = edge.src;
                            let dst = edge.dst;
                            let edge_type_name = edge.edge_type.clone();
                            let etype =
                                self.resolve_edge_type_id(&Value::String(edge_type_name.clone()))?;
                            let schema = self.storage.schema_manager().schema().clone();

                            if !pending_e.contains_key(var_name) {
                                let initial = read_edge_props_with_prefetch(
                                    eid,
                                    prefetched,
                                    prop_manager,
                                    ctx,
                                )
                                .await?;
                                pending_e.insert(
                                    var_name.clone(),
                                    PendingEdgeSet {
                                        src,
                                        dst,
                                        edge_type_id: etype,
                                        eid,
                                        edge_type_name: edge_type_name.clone(),
                                        props: initial,
                                        before: HashMap::new(),
                                    },
                                );
                            }

                            let val = self
                                .evaluate_expr(value, row, prop_manager, params, ctx)
                                .await?;
                            let val = Self::coerce_and_validate_property_value(
                                prop_name,
                                val,
                                &schema,
                                std::slice::from_ref(&edge_type_name),
                            )?;

                            let pe = pending_e
                                .get_mut(var_name)
                                .expect("inserted above when absent");
                            if !pe.before.contains_key(prop_name) {
                                let old = pe.props.get(prop_name).cloned();
                                pe.before.insert(prop_name.clone(), old);
                            }
                            pe.props.insert(prop_name.clone(), val.clone());

                            // Update the row object so subsequent RHS sees the new value.
                            if let Some(binding) = row.get_mut(var_name) {
                                binding.set_entity_property(prop_name, val);
                            }
                        }
                    }
                }
                SetItem::Labels { variable, labels } => {
                    // Flush any pending writes for this var so the Labels op
                    // sees latest L0 state. Other variables' pending writes
                    // can keep waiting (they're independent).
                    self.flush_pending_var(
                        variable,
                        &mut pending_v,
                        &mut pending_e,
                        writer,
                        prop_manager,
                        params,
                        ctx,
                        tx_l0,
                        prefetched,
                    )
                    .await?;

                    if let Some(node_val) = row.get(variable)
                        && let Ok(vid) = Self::vid_from_value(node_val)
                    {
                        reject_if_ephemeral_vid(vid)?;
                        let registry = self
                            .procedure_registry
                            .as_ref()
                            .and_then(|pr| pr.plugin_registry());
                        reject_virtual_label_write(registry.as_ref(), labels, "SET")?;

                        // The vertex's labels now, not the binding's: the
                        // write below replaces the whole set.
                        let current_labels = self.current_vertex_labels(vid, node_val, ctx);

                        // Determine new labels to add (skip duplicates)
                        let labels_to_add: Vec<_> = labels
                            .iter()
                            .filter(|l| !current_labels.contains(l))
                            .cloned()
                            .collect();

                        if !labels_to_add.is_empty() {
                            // Resolve the FULL new label set and write it to the
                            // TRANSACTION buffer (so the change is transactional
                            // and OCC-conflictable), falling back to the context
                            // (main) L0 for non-transactional callers. Replace
                            // semantics via `set_vertex_labels`.
                            let added = labels_to_add.len();
                            let mut new_labels = current_labels.clone();
                            new_labels.extend(labels_to_add);
                            if let Some(ctx) = ctx {
                                let l0 = ctx.transaction_l0.as_ref().unwrap_or(&ctx.l0);
                                let mut guard = l0.write();
                                guard.relabel_vertex(vid, &current_labels, &new_labels);
                                guard.record_labels_added(vid, added);
                            }

                            // Update every binding of the vertex in the row. This
                            // reached into a `Value::Map` only, so a natively
                            // encoded vertex kept its old label set even though
                            // the relabel had reached storage (#234); and only
                            // `variable`'s binding, so another variable bound to
                            // the same vertex projected the old set.
                            Self::set_labels_on_bindings(row, vid, &new_labels);
                        }
                    }
                }
                SetItem::Variable { variable, value }
                | SetItem::VariablePlus { variable, value } => {
                    // Flush this var's pending writes first so the
                    // replace/merge op sees them as latest L0 state.
                    self.flush_pending_var(
                        variable,
                        &mut pending_v,
                        &mut pending_e,
                        writer,
                        prop_manager,
                        params,
                        ctx,
                        tx_l0,
                        prefetched,
                    )
                    .await?;

                    let replace = matches!(item, SetItem::Variable { .. });
                    let op_str = if replace { "=" } else { "+=" };

                    // SET n = expr / SET n += expr — null target from OPTIONAL MATCH is a silent no-op
                    if matches!(row.get(variable.as_str()), None | Some(Value::Null)) {
                        continue;
                    }
                    let rhs = self
                        .evaluate_expr(value, row, prop_manager, params, ctx)
                        .await?;
                    let new_props =
                        Self::extract_user_properties_from_value(&rhs).ok_or_else(|| {
                            anyhow!(
                                "SET {} {} expr: right-hand side must evaluate to a map, \
                                 node, or relationship",
                                variable,
                                op_str
                            )
                        })?;
                    self.apply_properties_to_entity(
                        variable,
                        new_props,
                        replace,
                        row,
                        writer,
                        prop_manager,
                        params,
                        ctx,
                        tx_l0,
                        prefetched,
                    )
                    .await?;
                }
            }
        }

        // Flush all remaining coalesced writes — one writer call per target,
        // each a full-row update.
        for (_var_name, pv) in pending_v {
            self.write_pending_vertex(pv, writer, prop_manager, params, ctx, tx_l0)
                .await?;
        }
        for (_var_name, pe) in pending_e {
            Self::write_pending_edge(pe, writer, tx_l0).await?;
        }

        Ok(())
    }

    /// Write one coalesced vertex SET: refresh auto-embed targets, regenerate
    /// generated columns, store the full row, and record the property side
    /// effects of the keys the SET assigned.
    async fn write_pending_vertex(
        &self,
        mut pv: PendingVertexSet,
        writer: &Writer,
        prop_manager: &PropertyManager,
        params: &HashMap<String, Value>,
        ctx: Option<&QueryContext>,
        tx_l0: Option<&Arc<parking_lot::RwLock<uni_store::runtime::l0::L0Buffer>>>,
    ) -> Result<()> {
        // A SET that touches an auto-embed source column must refresh the
        // target embedding, or the write re-stores the stale vector.
        writer.refresh_embed_targets(&mut pv.props, &mut pv.touched, &pv.labels);
        for label_name in &pv.labels {
            self.enrich_properties_with_generated_columns(
                label_name,
                &mut pv.props,
                prop_manager,
                params,
                ctx,
            )
            .await?;
        }
        let effects: Vec<(Option<Value>, Option<Value>)> = pv
            .before
            .iter()
            .map(|(k, old)| (old.clone(), pv.props.get(k).cloned()))
            .collect();
        writer
            .update_vertex_with_labels(pv.vid, pv.props, &pv.labels, tx_l0)
            .await?;
        writer.record_property_effects(StatEntity::Vertex(pv.vid), &effects, tx_l0);
        Ok(())
    }

    /// Write one coalesced edge SET as a full row and record its property
    /// side effects.
    async fn write_pending_edge(
        pe: PendingEdgeSet,
        writer: &Writer,
        tx_l0: Option<&Arc<parking_lot::RwLock<uni_store::runtime::l0::L0Buffer>>>,
    ) -> Result<()> {
        let effects: Vec<(Option<Value>, Option<Value>)> = pe
            .before
            .iter()
            .map(|(k, old)| (old.clone(), pe.props.get(k).cloned()))
            .collect();
        writer
            .update_edge(
                pe.src,
                pe.dst,
                pe.edge_type_id,
                pe.eid,
                pe.props,
                Some(pe.edge_type_name),
                tx_l0,
            )
            .await?;
        writer.record_property_effects(StatEntity::Edge(pe.eid), &effects, tx_l0);
        Ok(())
    }

    /// Flush pending SET state for a single variable to the writer.
    ///
    /// Called from the SET loop when about to process a Labels /
    /// Variable / VariablePlus item on `var`, so the subsequent op
    /// sees latest L0 state and ordering is preserved.
    #[expect(clippy::too_many_arguments)]
    async fn flush_pending_var(
        &self,
        var: &str,
        pending_v: &mut HashMap<String, PendingVertexSet>,
        pending_e: &mut HashMap<String, PendingEdgeSet>,
        writer: &Writer,
        prop_manager: &PropertyManager,
        _params: &HashMap<String, Value>,
        ctx: Option<&QueryContext>,
        tx_l0: Option<&Arc<parking_lot::RwLock<uni_store::runtime::l0::L0Buffer>>>,
        _prefetched: &Prefetch,
    ) -> Result<()> {
        if let Some(pv) = pending_v.remove(var) {
            self.write_pending_vertex(pv, writer, prop_manager, _params, ctx, tx_l0)
                .await?;
        }
        if let Some(pe) = pending_e.remove(var) {
            Self::write_pending_edge(pe, writer, tx_l0).await?;
        }
        Ok(())
    }

    /// Execute REMOVE clause items (property removal or label removal).
    ///
    /// Property removals are batched per variable to avoid stale reads: when
    /// multiple properties of the same entity are removed in one REMOVE clause,
    /// we read from storage once, null all specified properties, and write back
    /// once. This prevents the second removal from reading stale data that
    /// doesn't reflect the first removal's L0 write.
    #[expect(clippy::too_many_arguments)]
    pub(crate) async fn execute_remove_items_locked(
        &self,
        items: &[RemoveItem],
        row: &mut HashMap<String, Value>,
        writer: &Writer,
        prop_manager: &PropertyManager,
        ctx: Option<&QueryContext>,
        tx_l0: Option<&Arc<parking_lot::RwLock<uni_store::runtime::l0::L0Buffer>>>,
        prefetched: &Prefetch,
    ) -> Result<()> {
        // Collect property names to remove, grouped by variable.
        // Use Vec<(String, Vec<String>)> to preserve insertion order.
        let mut prop_removals: Vec<(String, Vec<String>)> = Vec::new();

        for item in items {
            match item {
                RemoveItem::Property(expr) => {
                    if let Expr::Property(var_expr, prop_name) = expr
                        && let Expr::Variable(var_name) = &**var_expr
                    {
                        if let Some(entry) = prop_removals.iter_mut().find(|(v, _)| v == var_name) {
                            entry.1.push(prop_name.clone());
                        } else {
                            prop_removals.push((var_name.clone(), vec![prop_name.clone()]));
                        }
                    }
                }
                RemoveItem::Labels { variable, labels } => {
                    self.execute_remove_labels(variable, labels, row, prop_manager, ctx)
                        .await?;
                }
            }
        }

        // Execute batched property removals per variable.
        for (var_name, prop_names) in &prop_removals {
            let Some(node_val) = row.get(var_name) else {
                continue;
            };

            if let Ok(vid) = Self::vid_from_value(node_val) {
                // Vertex property removal
                let mut props =
                    read_vertex_props_with_prefetch(vid, prefetched, prop_manager, ctx).await?;

                // Only write back if at least one property actually exists
                let effects = removal_effects(&props, prop_names);
                let any_exist = !effects.is_empty();
                if any_exist {
                    writer.record_property_effects(StatEntity::Vertex(vid), &effects, tx_l0);
                    for prop_name in prop_names {
                        props.insert(prop_name.clone(), Value::Null);
                    }
                }
                // Compute effective properties (post-removal) for _all_props
                let effective: HashMap<String, Value> = props
                    .iter()
                    .filter(|(_, v)| !v.is_null())
                    .map(|(k, v)| (k.clone(), v.clone()))
                    .collect();
                if any_exist {
                    let labels = self.current_vertex_labels(vid, node_val, ctx);
                    let _ = writer
                        .update_vertex_with_labels(vid, props, &labels, tx_l0)
                        .await?;
                }

                // Update the row's binding so the rest of the statement sees the
                // post-REMOVE value. This reached into a `Value::Map` only, so a
                // natively-encoded entity was left holding the old properties —
                // the removal reached storage but not the row (#234).
                if let Some(binding) = row.get_mut(var_name) {
                    binding.set_entity_properties(effective, prop_names);
                }
            } else if let Value::Map(map) = node_val {
                // Edge property removal (map-encoded)
                // Check for non-null _eid to skip OPTIONAL MATCH null edges
                let mut edge_effective: Option<HashMap<String, Value>> = None;
                if map.get("_eid").is_some_and(|v| !v.is_null()) {
                    let ei = self.extract_edge_identity(map)?;
                    let mut props =
                        read_edge_props_with_prefetch(ei.eid, prefetched, prop_manager, ctx)
                            .await?;

                    let effects = removal_effects(&props, prop_names);
                    let any_exist = !effects.is_empty();
                    if any_exist {
                        writer.record_property_effects(StatEntity::Edge(ei.eid), &effects, tx_l0);
                        for prop_name in prop_names {
                            props.insert(prop_name.to_string(), Value::Null);
                        }
                    }
                    // Compute effective properties (post-removal) for _all_props
                    edge_effective = Some(
                        props
                            .iter()
                            .filter(|(_, v)| !v.is_null())
                            .map(|(k, v)| (k.clone(), v.clone()))
                            .collect(),
                    );
                    if any_exist {
                        let edge_type_name = Value::Map(map.clone())
                            .edge_type_ref()
                            .and_then(|t| t.name().map(str::to_string))
                            .or_else(|| {
                                self.storage
                                    .schema_manager()
                                    .edge_type_name_by_id_unified(ei.edge_type_id)
                            });
                        writer
                            .update_edge(
                                ei.src,
                                ei.dst,
                                ei.edge_type_id,
                                ei.eid,
                                props,
                                edge_type_name,
                                tx_l0,
                            )
                            .await?;
                    }
                }

                if let Some(Value::Map(edge_map)) = row.get_mut(var_name) {
                    for prop_name in prop_names {
                        edge_map.insert(prop_name.clone(), Value::Null);
                    }
                    if let Some(effective) = edge_effective {
                        edge_map.insert("_all_props".to_string(), Value::Map(effective));
                    }
                }
            } else if let Value::Edge(edge) = node_val {
                // Edge property removal (Value::Edge)
                let eid = edge.eid;
                let src = edge.src;
                let dst = edge.dst;
                let etype = self.resolve_edge_type_id(&Value::String(edge.edge_type.clone()))?;

                let mut props =
                    read_edge_props_with_prefetch(eid, prefetched, prop_manager, ctx).await?;

                let effects = removal_effects(&props, prop_names);
                if !effects.is_empty() {
                    writer.record_property_effects(StatEntity::Edge(eid), &effects, tx_l0);
                    for prop_name in prop_names {
                        props.insert(prop_name.to_string(), Value::Null);
                    }
                    writer
                        .update_edge(
                            src,
                            dst,
                            etype,
                            eid,
                            props,
                            Some(edge.edge_type.clone()),
                            tx_l0,
                        )
                        .await?;
                }

                if let Some(Value::Edge(edge)) = row.get_mut(var_name) {
                    for prop_name in prop_names {
                        edge.properties.insert(prop_name.to_string(), Value::Null);
                    }
                }
            }
        }

        Ok(())
    }

    /// Execute label removal.
    ///
    /// Refused with `EdgeEndpointViolation` when the removal would leave one
    /// of the node's existing edges outside its edge type's declared endpoint
    /// labels.
    pub(crate) async fn execute_remove_labels(
        &self,
        variable: &str,
        labels: &[String],
        row: &mut HashMap<String, Value>,
        prop_manager: &PropertyManager,
        ctx: Option<&QueryContext>,
    ) -> Result<()> {
        if let Some(node_val) = row.get(variable)
            && let Ok(vid) = Self::vid_from_value(node_val)
        {
            reject_if_ephemeral_vid(vid)?;
            let registry = self
                .procedure_registry
                .as_ref()
                .and_then(|pr| pr.plugin_registry());
            reject_virtual_label_write(registry.as_ref(), labels, "REMOVE")?;

            // The vertex's labels now, not the binding's: the write below
            // replaces the whole set.
            let current_labels = self.current_vertex_labels(vid, node_val, ctx);

            // Determine which labels to actually remove (only those currently present)
            let labels_to_remove: Vec<_> = labels
                .iter()
                .filter(|l| current_labels.contains(l))
                .collect();

            if !labels_to_remove.is_empty() {
                // Resolve the FULL remaining label set and write it to the
                // TRANSACTION buffer (transactional + OCC-conflictable), falling
                // back to the context (main) L0 for non-transactional callers.
                let remaining_labels: Vec<String> = current_labels
                    .iter()
                    .filter(|l| !labels_to_remove.contains(l))
                    .cloned()
                    .collect();
                self.check_label_removal_keeps_edges_declared(
                    vid,
                    &remaining_labels,
                    prop_manager,
                    ctx,
                )?;
                let removed = labels_to_remove.len();
                if let Some(ctx) = ctx {
                    let l0 = ctx.transaction_l0.as_ref().unwrap_or(&ctx.l0);
                    let mut guard = l0.write();
                    guard.relabel_vertex(vid, &current_labels, &remaining_labels);
                    guard.record_labels_removed(vid, removed);
                }

                // Update every binding of the vertex in the row. Map-only
                // before, so `REMOVE n:Label` left a natively encoded vertex
                // showing its old label set (#234).
                Self::set_labels_on_bindings(row, vid, &remaining_labels);
            }
        }
        Ok(())
    }

    /// Refuse a label removal that would leave an edge of `vid` outside its
    /// edge type's declared endpoint labels.
    ///
    /// For every declared edge type whose source (target) labels the node
    /// would no longer carry, the node must have no outgoing (incoming) edge
    /// of that type. Edges are read through the same CSR + L0 overlay a
    /// traversal uses, so edges written earlier in this transaction count.
    ///
    /// # Errors
    /// `EdgeEndpointViolation` naming the first such edge type.
    fn check_label_removal_keeps_edges_declared(
        &self,
        vid: Vid,
        remaining: &[String],
        prop_manager: &PropertyManager,
        ctx: Option<&QueryContext>,
    ) -> Result<()> {
        use uni_store::storage::direction::Direction;
        let schema = self.storage.schema_manager().schema();
        let carries = |allowed: &[String]| remaining.iter().any(|l| allowed.contains(l));
        let mut probes: Vec<(&String, u32, Direction, &'static str, &Vec<String>)> = Vec::new();
        for (name, meta) in &schema.edge_types {
            if !meta.src_labels.is_empty() && !carries(&meta.src_labels) {
                probes.push((
                    name,
                    meta.id,
                    Direction::Outgoing,
                    "source",
                    &meta.src_labels,
                ));
            }
            if !meta.dst_labels.is_empty() && !carries(&meta.dst_labels) {
                probes.push((
                    name,
                    meta.id,
                    Direction::Incoming,
                    "target",
                    &meta.dst_labels,
                ));
            }
        }
        if probes.is_empty() {
            return Ok(());
        }
        let l0_context = match ctx {
            Some(c) => crate::query::df_graph::L0Context::from_query_context(c),
            None => crate::query::df_graph::L0Context::empty(),
        };
        let pm_arc = self.prop_manager_arc.clone().unwrap_or_else(|| {
            Arc::new(PropertyManager::new(
                self.storage.clone(),
                self.storage.schema_manager_arc(),
                prop_manager.cache_size(),
            ))
        });
        let graph_ctx = crate::query::df_graph::GraphExecutionContext::with_l0_context(
            self.effective_storage(),
            l0_context,
            pm_arc,
        );
        for (name, type_id, dir, endpoint, allowed) in probes {
            if !graph_ctx.get_neighbors(vid, type_id, dir).is_empty() {
                return Err(anyhow::Error::new(
                    uni_common::UniError::EdgeEndpointViolation {
                        edge_type: name.clone(),
                        endpoint: endpoint.to_string(),
                        labels: remaining.to_vec(),
                        allowed: allowed.clone(),
                    },
                ));
            }
        }
        Ok(())
    }

    /// Resolve edge type ID for a Value::Edge, handling empty edge_type strings
    /// by looking up the type from the L0 buffer's edge endpoints.
    fn resolve_edge_type_id_for_edge(
        &self,
        edge: &crate::types::Edge,
        writer: &Writer,
        tx_l0: Option<&Arc<parking_lot::RwLock<uni_store::runtime::l0::L0Buffer>>>,
    ) -> Result<u32> {
        if !edge.edge_type.is_empty() {
            return self.resolve_edge_type_id(&Value::String(edge.edge_type.clone()));
        }
        // Edge type name is empty (e.g., from anonymous MATCH patterns).
        // Look up the edge type ID from the L0 buffer's edge endpoints.
        if let Some(etype) = writer.get_edge_type_id_from_l0(edge.eid, tx_l0) {
            return Ok(etype);
        }
        Err(anyhow!(
            "Cannot determine edge type for edge {:?} — edge type name is empty and not found in L0",
            edge.eid
        ))
    }

    /// Delete every element of a path: edges first, then nodes.
    ///
    /// Shared by the typed `Value::Path` arm and the `Path::try_from`
    /// reconstruction fallback (Arrow round-trips lose the `Path` type).
    async fn execute_delete_path(
        &self,
        path: &Path,
        detach: bool,
        writer: &Writer,
        tx_l0: Option<&Arc<parking_lot::RwLock<uni_store::runtime::l0::L0Buffer>>>,
    ) -> Result<()> {
        for edge in &path.edges {
            let etype = self.resolve_edge_type_id_for_edge(edge, writer, tx_l0)?;
            writer
                .delete_edge(edge.eid, edge.src, edge.dst, etype, tx_l0)
                .await?;
        }
        for node in &path.nodes {
            self.execute_delete_vertex(node.vid, detach, Some(node.labels.clone()), writer, tx_l0)
                .await?;
        }
        Ok(())
    }

    /// Execute DELETE clause for a single item (vertex, edge, path, or null).
    pub(crate) async fn execute_delete_item_locked(
        &self,
        val: &Value,
        detach: bool,
        writer: &Writer,
        tx_l0: Option<&Arc<parking_lot::RwLock<uni_store::runtime::l0::L0Buffer>>>,
    ) -> Result<()> {
        match val {
            Value::Null => {
                // DELETE null is a no-op per OpenCypher spec
            }
            Value::Path(path) => {
                self.execute_delete_path(path, detach, writer, tx_l0)
                    .await?;
            }
            _ => {
                // Try Path reconstruction from Map first (Arrow loses Path type)
                if let Ok(path) = Path::try_from(val) {
                    self.execute_delete_path(&path, detach, writer, tx_l0)
                        .await?;
                } else if let Ok(vid) = Self::vid_from_value(val) {
                    let labels = Self::extract_labels_from_node(val);
                    self.execute_delete_vertex(vid, detach, labels, writer, tx_l0)
                        .await?;
                } else if let Value::Map(map) = val {
                    self.execute_delete_edge_from_map(map, writer, tx_l0)
                        .await?;
                } else if let Value::Edge(edge) = val {
                    reject_if_ephemeral_eid(edge.eid)?;
                    let etype = self.resolve_edge_type_id_for_edge(edge, writer, tx_l0)?;
                    let registry = self
                        .procedure_registry
                        .as_ref()
                        .and_then(|pr| pr.plugin_registry());
                    reject_virtual_edge_type_write(registry.as_ref(), etype, "DELETE")?;
                    writer
                        .delete_edge(edge.eid, edge.src, edge.dst, etype, tx_l0)
                        .await?;
                }
            }
        }
        Ok(())
    }

    /// Execute vertex deletion with optional detach.
    pub(crate) async fn execute_delete_vertex(
        &self,
        vid: Vid,
        detach: bool,
        labels: Option<Vec<String>>,
        writer: &Writer,
        tx_l0: Option<&Arc<parking_lot::RwLock<uni_store::runtime::l0::L0Buffer>>>,
    ) -> Result<()> {
        reject_if_ephemeral_vid(vid)?;
        if let Some(ls) = labels.as_deref() {
            let registry = self
                .procedure_registry
                .as_ref()
                .and_then(|pr| pr.plugin_registry());
            reject_virtual_label_write(registry.as_ref(), ls, "DELETE")?;
        }
        if detach {
            self.detach_delete_vertex(vid, writer, tx_l0).await?;
        } else {
            self.check_vertex_has_no_edges(vid, writer, tx_l0).await?;
        }
        writer.delete_vertex(vid, labels, tx_l0).await?;
        Ok(())
    }

    /// Check that a vertex has no edges (required for non-DETACH DELETE).
    ///
    /// Loads the subgraph from storage, then excludes edges that have been
    /// tombstoned in the writer's L0 or the transaction's L0. This ensures
    /// edges deleted earlier in the same DELETE clause are properly excluded.
    pub(crate) async fn check_vertex_has_no_edges(
        &self,
        vid: Vid,
        writer: &Writer,
        tx_l0: Option<&Arc<parking_lot::RwLock<uni_store::runtime::l0::L0Buffer>>>,
    ) -> Result<()> {
        // One implementation for the single and the batched path, so a fix to
        // what counts as "still has relationships" cannot reach only one.
        self.batch_check_vertices_have_no_edges(&[vid], writer, tx_l0)
            .await
    }

    /// Execute edge deletion from a map representation.
    pub(crate) async fn execute_delete_edge_from_map(
        &self,
        map: &HashMap<String, Value>,
        writer: &Writer,
        tx_l0: Option<&Arc<parking_lot::RwLock<uni_store::runtime::l0::L0Buffer>>>,
    ) -> Result<()> {
        // Check for non-null _eid to skip OPTIONAL MATCH null edges
        if map.get("_eid").is_some_and(|v| !v.is_null()) {
            let ei = self.extract_edge_identity(map)?;
            reject_if_ephemeral_eid(ei.eid)?;
            let registry = self
                .procedure_registry
                .as_ref()
                .and_then(|pr| pr.plugin_registry());
            reject_virtual_edge_type_write(registry.as_ref(), ei.edge_type_id, "DELETE")?;
            writer
                .delete_edge(ei.eid, ei.src, ei.dst, ei.edge_type_id, tx_l0)
                .await?;
        }
        Ok(())
    }

    /// Build a scan plan node.
    ///
    /// - `label_id > 0`: schema label → `Scan` (fast, label-specific storage)
    /// - `label_id == 0` with labels: schemaless → `ScanMainByLabels` (main table + L0, filtered by label name)
    /// - `label_id == 0` without labels: unlabeled → `ScanAll`
    fn make_scan_plan(
        label_id: u16,
        labels: Vec<String>,
        variable: String,
        filter: Option<Expr>,
    ) -> LogicalPlan {
        if label_id > 0 {
            LogicalPlan::Scan {
                label_id,
                labels,
                variable,
                filter,
                optional: false,
            }
        } else if !labels.is_empty() {
            // Schemaless label: use ScanMainByLabels to filter by label name
            LogicalPlan::ScanMainByLabels {
                labels,
                variable,
                filter,
                optional: false,
            }
        } else {
            LogicalPlan::ScanAll {
                variable,
                filter,
                optional: false,
            }
        }
    }

    /// Attach a new scan node to the running plan, using `CrossJoin` when the plan
    /// already contains prior operators.
    fn attach_scan(plan: LogicalPlan, scan: LogicalPlan) -> LogicalPlan {
        if matches!(plan, LogicalPlan::Empty) {
            scan
        } else {
            LogicalPlan::CrossJoin {
                left: Box::new(plan),
                right: Box::new(scan),
            }
        }
    }

    /// Resolve MERGE property map expressions against the current row context.
    ///
    /// MERGE patterns like `MERGE (city:City {name: person.bornIn})` contain
    /// property expressions that reference bound variables. These need to be
    /// evaluated to concrete literal values before being converted to filter
    /// expressions by `properties_to_expr()`.
    async fn resolve_merge_properties(
        &self,
        properties: &Option<Expr>,
        row: &HashMap<String, Value>,
        prop_manager: &PropertyManager,
        params: &HashMap<String, Value>,
        ctx: Option<&QueryContext>,
    ) -> Result<Option<Expr>> {
        let entries = match properties {
            Some(Expr::Map(entries)) => entries,
            other => return Ok(other.clone()),
        };
        let mut resolved = Vec::new();
        for (key, val_expr) in entries {
            if matches!(val_expr, Expr::Literal(_)) {
                resolved.push((key.clone(), val_expr.clone()));
            } else {
                let value = self
                    .evaluate_expr(val_expr, row, prop_manager, params, ctx)
                    .await?;
                resolved.push((key.clone(), Self::value_to_literal_expr(&value)));
            }
        }
        Ok(Some(Expr::Map(resolved)))
    }

    /// Convert a runtime Value back to an AST literal expression.
    fn value_to_literal_expr(value: &Value) -> Expr {
        match value {
            Value::Int(i) => Expr::Literal(CypherLiteral::Integer(*i)),
            Value::Float(f) => Expr::Literal(CypherLiteral::Float(*f)),
            Value::String(s) => Expr::Literal(CypherLiteral::String(s.clone())),
            Value::Bool(b) => Expr::Literal(CypherLiteral::Bool(*b)),
            Value::Null => Expr::Literal(CypherLiteral::Null),
            Value::List(items) => {
                Expr::List(items.iter().map(Self::value_to_literal_expr).collect())
            }
            Value::Map(entries) => Expr::Map(
                entries
                    .iter()
                    .map(|(k, v)| (k.clone(), Self::value_to_literal_expr(v)))
                    .collect(),
            ),
            _ => Expr::Literal(CypherLiteral::Null),
        }
    }

    /// Whether a reversed MERGE path can still be planned.
    ///
    /// The head node of the walk may be unlabeled and unbound — it becomes a
    /// `label_id = 0` scan. A node in any *later* position may not: the
    /// traversal-target arm requires a label or a row binding and errors
    /// otherwise. Reversal moves the original head to a later position, so a
    /// pattern like `MERGE (a)-[:R]->(b)` with a bare `a` and a bound `b` is
    /// legal as written and illegal reversed.
    ///
    /// That asymmetry predates this and is left alone; the anchor simply
    /// declines a reversal it would trip. The cost is that a bare unlabeled
    /// MERGE node keeps the per-row label scan — but such a node cannot be
    /// anchored on in any case, since there is nothing to look it up by.
    fn merge_reversal_is_legal(
        reversed: &uni_cypher::ast::PathPattern,
        row: &HashMap<String, Value>,
    ) -> bool {
        reversed
            .elements
            .iter()
            .skip(1)
            .filter_map(|e| match e {
                PatternElement::Node(n) => Some(n),
                _ => None,
            })
            .all(|n| {
                !matches!(n.labels, uni_cypher::ast::LabelExpr::Empty)
                    || n.variable
                        .as_ref()
                        .is_some_and(|v| !v.is_empty() && row.contains_key(v))
            })
    }

    pub(crate) async fn execute_merge_match(
        &self,
        pattern: &Pattern,
        row: &HashMap<String, Value>,
        prop_manager: &PropertyManager,
        params: &HashMap<String, Value>,
        ctx: Option<&QueryContext>,
    ) -> Result<Vec<HashMap<String, Value>>> {
        // Construct a LogicalPlan for the MATCH part of MERGE
        let planner =
            crate::query::planner::QueryPlanner::new(self.storage.schema_manager().schema());

        // We need to construct a CypherQuery to use the planner's plan() method,
        // or we can manually construct the LogicalPlan.
        // Manual construction is safer as we don't have to round-trip through AST.

        let mut plan = LogicalPlan::Empty;
        let mut vars_in_scope = Vec::new();

        // Add existing bound variables from row to scope
        for key in row.keys() {
            vars_in_scope.push(key.clone());
        }

        // Anchor on what the input row binds, not on what was written first.
        //
        // This walk is MERGE's own transcription of `plan_path`'s, so neither
        // #219's anchoring fix nor `dc232cd2a`'s ranking of it reached here. It
        // consults boundness only for the leftmost node, which makes
        // `MERGE (a:L)-[:R]->(b)` with `b` bound scan the whole of `L` — once
        // per input row, since `execute_merge_match` runs per row. Measured
        // with a fixed 400-row batch: 5.4s / 9.7s / 16.2s over labels of
        // 5k / 10k / 20k nodes, against a flat 0.9s for the same pattern
        // written with the bound node first (`examples/merge_anchor_probe`).
        //
        // Reversal is a *read-side* decision only. The create path walks the
        // original `pattern`, so a reversed copy must not escape this loop;
        // variable names survive reversal, so ON MATCH / ON CREATE SET see the
        // bindings they expect either way.
        let anchor_scope: Vec<crate::query::planner::VariableInfo> = row
            .keys()
            .map(|k| {
                crate::query::planner::VariableInfo::new(
                    k.clone(),
                    crate::query::planner::VariableType::Node,
                )
            })
            .collect();
        let no_where = HashMap::new();

        // Reconstruct Match logic from Planner (simplified for MERGE pattern)
        for original_path in &pattern.paths {
            let reversed = planner
                .reversed_for_bound_anchor(original_path, &anchor_scope, &no_where)
                .filter(|rev| Self::merge_reversal_is_legal(rev, row));
            let path = reversed.as_ref().unwrap_or(original_path);
            let elements = &path.elements;
            let mut i = 0;
            while i < elements.len() {
                let part = &elements[i];
                match part {
                    PatternElement::Node(n) => {
                        let variable = n.variable.clone().unwrap_or_default();

                        // If variable is already bound in the input row, we filter
                        let is_bound = !variable.is_empty() && row.contains_key(&variable);

                        if is_bound {
                            // If bound, we must Scan this specific VID to start the chain
                            // Extract VID from row
                            let val = row.get(&variable).unwrap();
                            let vid = Self::vid_from_value(val)?;

                            // In the new storage model, VIDs don't embed label info.
                            // We get label from the node value if available, otherwise use 0 to scan all.
                            let extracted_labels =
                                Self::extract_labels_from_node(val).unwrap_or_default();
                            let label_id = {
                                let schema = self.storage.schema_manager().schema();
                                extracted_labels
                                    .first()
                                    .and_then(|l| schema.label_id_by_name(l))
                                    .unwrap_or(0)
                            };

                            let resolved_props = self
                                .resolve_merge_properties(
                                    &n.properties,
                                    row,
                                    prop_manager,
                                    params,
                                    ctx,
                                )
                                .await?;
                            let prop_filter =
                                planner.properties_to_expr(&variable, &resolved_props);

                            // Create a filter expression for VID: variable._vid = vid
                            // But our expression engine handles `Expr::Variable` as column.
                            // We can inject a filter `id(variable) = vid` if we had `id()` function.
                            // Or we use internal property `_vid`.

                            // Note: Scan supports `filter`.
                            // We can manually construct an Expr::BinaryOp(Eq, Prop(var, _vid), Literal(vid))

                            let vid_filter = Expr::BinaryOp {
                                left: Box::new(Expr::Property(
                                    Box::new(Expr::Variable(variable.clone())),
                                    "_vid".to_string(),
                                )),
                                op: BinaryOp::Eq,
                                right: Box::new(Expr::Literal(CypherLiteral::Integer(
                                    vid.as_u64() as i64,
                                ))),
                            };

                            let combined_filter = if let Some(pf) = prop_filter {
                                Some(Expr::BinaryOp {
                                    left: Box::new(vid_filter),
                                    op: BinaryOp::And,
                                    right: Box::new(pf),
                                })
                            } else {
                                Some(vid_filter)
                            };

                            let scan = Self::make_scan_plan(
                                label_id,
                                extracted_labels,
                                variable.clone(),
                                combined_filter,
                            );
                            plan = Self::attach_scan(plan, scan);
                        } else {
                            let label_id = if n.labels.is_empty() {
                                // Unlabeled MERGE node: scan all nodes (label_id 0 → ScanAll)
                                0
                            } else {
                                let label_name = &n.labels[0];
                                let schema = self.storage.schema_manager().schema();
                                if self.config.strict_schema {
                                    schema
                                        .get_label_case_insensitive(label_name)
                                        .map(|m| m.id)
                                        .ok_or_else(|| {
                                            anyhow!(
                                                "Label '{}' is not defined in the schema \
                                                 (strict_schema is enabled). \
                                                 Declare it with db.schema().label(...).apply() first.",
                                                label_name
                                            )
                                        })?
                                } else {
                                    // Fall back to label_id 0 (any/schemaless) when not in schema.
                                    schema
                                        .get_label_case_insensitive(label_name)
                                        .map(|m| m.id)
                                        .unwrap_or(0)
                                }
                            };

                            let resolved_props = self
                                .resolve_merge_properties(
                                    &n.properties,
                                    row,
                                    prop_manager,
                                    params,
                                    ctx,
                                )
                                .await?;
                            let prop_filter =
                                planner.properties_to_expr(&variable, &resolved_props);
                            let scan = Self::make_scan_plan(
                                label_id,
                                n.labels.names().to_vec(),
                                variable.clone(),
                                prop_filter,
                            );
                            plan = Self::attach_scan(plan, scan);

                            // Add label filters when:
                            // 1. Multiple labels with a known schema label: filter for
                            //    additional labels (Scan only scans by the first label).
                            // 2. Schemaless labels (label_id = 0): ScanAll finds ALL
                            //    nodes, so we must filter to only those with the
                            //    specified label(s).
                            if !n.labels.is_empty()
                                && !variable.is_empty()
                                && (label_id == 0 || n.labels.len() > 1)
                                && let Some(label_filter) =
                                    planner.node_filter_expr(&variable, &n.labels, &None)
                            {
                                plan = LogicalPlan::Filter {
                                    input: Box::new(plan),
                                    predicate: label_filter,
                                    optional_variables: std::collections::HashSet::new(),
                                };
                            }

                            if !variable.is_empty() {
                                vars_in_scope.push(variable.clone());
                            }
                        }

                        // Now look ahead for relationship
                        i += 1;
                        while i < elements.len() {
                            if let PatternElement::Relationship(r) = &elements[i] {
                                let target_node_part = &elements[i + 1];
                                if let PatternElement::Node(n_target) = target_node_part {
                                    let schema = self.storage.schema_manager().schema();
                                    let mut edge_type_ids = Vec::new();

                                    if r.types.is_empty() {
                                        return Err(anyhow!("MERGE edge must have a type"));
                                    } else if r.types.len() > 1 {
                                        return Err(anyhow!(
                                            "MERGE does not support multiple edge types"
                                        ));
                                    } else {
                                        let type_name = &r.types[0];
                                        let type_id = if self.config.strict_schema {
                                            let s = self.storage.schema_manager().schema();
                                            s.edge_type_id_by_name_case_insensitive(type_name)
                                                .ok_or_else(|| {
                                                    anyhow!(
                                                        "Edge type '{}' is not defined in the schema \
                                                         (strict_schema is enabled).",
                                                        type_name
                                                    )
                                                })?
                                        } else {
                                            // Schemaless: assign new ID if not found.
                                            self.storage
                                                .schema_manager()
                                                .get_or_assign_edge_type_id(type_name)
                                        };
                                        edge_type_ids.push(type_id);
                                    }

                                    // Resolve target label ID. For schemaless labels (not in the
                                    // schema), fall back to 0 which means "any label" in traversal.
                                    let target_label_id: u16 = if let Some(lbl) =
                                        n_target.labels.first()
                                    {
                                        schema
                                            .get_label_case_insensitive(lbl)
                                            .map(|m| m.id)
                                            .unwrap_or(0)
                                    } else if let Some(var) = &n_target.variable {
                                        if let Some(val) = row.get(var) {
                                            // In the new storage model, get labels from node value
                                            if let Some(labels) =
                                                Self::extract_labels_from_node(val)
                                            {
                                                if let Some(first_label) = labels.first() {
                                                    schema
                                                        .get_label_case_insensitive(first_label)
                                                        .map(|m| m.id)
                                                        .unwrap_or(0)
                                                } else {
                                                    // Bound node with no labels — schemaless, any
                                                    0
                                                }
                                            } else if Self::vid_from_value(val).is_ok() {
                                                // VID without label info — schemaless, any
                                                0
                                            } else {
                                                return Err(anyhow!(
                                                    "Variable {} is not a node",
                                                    var
                                                ));
                                            }
                                        } else {
                                            return Err(anyhow!(
                                                "MERGE pattern node must have a label or be a bound variable"
                                            ));
                                        }
                                    } else {
                                        return Err(anyhow!(
                                            "MERGE pattern node must have a label"
                                        ));
                                    };

                                    let target_variable =
                                        n_target.variable.clone().unwrap_or_default();
                                    let source_variable = match &elements[i - 1] {
                                        PatternElement::Node(n) => {
                                            n.variable.clone().unwrap_or_default()
                                        }
                                        _ => String::new(),
                                    };

                                    let is_variable_length = r.range.is_some();
                                    // An anonymous relationship carrying a
                                    // property map still needs a name: the
                                    // filter below builds `var.prop = value`
                                    // and the edge property columns are
                                    // materialized under the step variable.
                                    // Without one, MERGE's match phase ignored
                                    // the map and treated any edge of the type
                                    // as a match — so it skipped the write it
                                    // exists to perform. Same defect as #166 in
                                    // `plan_traverse_with_source`.
                                    let step_variable = r.variable.clone().or_else(|| {
                                        (r.properties.is_some() && !is_variable_length)
                                            .then(|| planner.next_anon_var())
                                    });
                                    let type_name = &r.types[0];

                                    // Use TraverseMainByType for schemaless edge types
                                    // (same as MATCH planner) so edge properties are loaded
                                    // correctly from storage + L0 via the adjacency map.
                                    // Regular Traverse only loads properties via
                                    // property_manager which doesn't handle schemaless types.
                                    let is_schemaless = edge_type_ids.iter().all(|id| {
                                        uni_common::core::edge_type::is_schemaless_edge_type(*id)
                                    });

                                    if is_schemaless {
                                        plan = LogicalPlan::TraverseMainByType {
                                            type_names: vec![type_name.clone()],
                                            input: Box::new(plan),
                                            direction: r.direction.clone(),
                                            source_variable,
                                            target_variable: target_variable.clone(),
                                            step_variable: step_variable.clone(),
                                            min_hops: r
                                                .range
                                                .as_ref()
                                                .and_then(|r| r.min)
                                                .unwrap_or(1)
                                                as usize,
                                            max_hops: r
                                                .range
                                                .as_ref()
                                                .and_then(|r| r.max)
                                                .unwrap_or(1)
                                                as usize,
                                            optional: false,
                                            target_filter: None,
                                            path_variable: None,
                                            is_variable_length,
                                            optional_pattern_vars: std::collections::HashSet::new(),
                                            scope_match_variables: std::collections::HashSet::new(),
                                            edge_filter_expr: None,
                                            path_mode: crate::query::df_graph::nfa::PathMode::Trail,
                                        };
                                    } else {
                                        // Collect edge property names needed for MERGE filter
                                        let mut edge_props = std::collections::HashSet::new();
                                        if let Some(Expr::Map(entries)) = &r.properties {
                                            for (key, _) in entries {
                                                edge_props.insert(key.clone());
                                            }
                                        }
                                        plan = LogicalPlan::Traverse {
                                            input: Box::new(plan),
                                            edge_type_ids: edge_type_ids.clone(),
                                            direction: r.direction.clone(),
                                            source_variable,
                                            target_variable: target_variable.clone(),
                                            target_label_id,
                                            step_variable: step_variable.clone(),
                                            min_hops: r
                                                .range
                                                .as_ref()
                                                .and_then(|r| r.min)
                                                .unwrap_or(1)
                                                as usize,
                                            max_hops: r
                                                .range
                                                .as_ref()
                                                .and_then(|r| r.max)
                                                .unwrap_or(1)
                                                as usize,
                                            optional: false,
                                            target_filter: None,
                                            path_variable: None,
                                            edge_properties: edge_props,
                                            is_variable_length,
                                            optional_pattern_vars: std::collections::HashSet::new(),
                                            scope_match_variables: std::collections::HashSet::new(),
                                            edge_filter_expr: None,
                                            path_mode: crate::query::df_graph::nfa::PathMode::Trail,
                                            qpp_steps: None,
                                            qpp_inner_source: None,
                                        };
                                    }

                                    // Apply property filters for relationship
                                    if r.properties.is_some()
                                        && let Some(r_var) = &step_variable
                                    {
                                        let resolved_rel_props = self
                                            .resolve_merge_properties(
                                                &r.properties,
                                                row,
                                                prop_manager,
                                                params,
                                                ctx,
                                            )
                                            .await?;
                                        if let Some(prop_filter) =
                                            planner.properties_to_expr(r_var, &resolved_rel_props)
                                        {
                                            plan = LogicalPlan::Filter {
                                                input: Box::new(plan),
                                                predicate: prop_filter,
                                                optional_variables: std::collections::HashSet::new(
                                                ),
                                            };
                                        }
                                    }

                                    // Apply property filters for target node if it was new
                                    if !target_variable.is_empty() {
                                        let resolved_target_props = self
                                            .resolve_merge_properties(
                                                &n_target.properties,
                                                row,
                                                prop_manager,
                                                params,
                                                ctx,
                                            )
                                            .await?;
                                        if let Some(prop_filter) = planner.properties_to_expr(
                                            &target_variable,
                                            &resolved_target_props,
                                        ) {
                                            plan = LogicalPlan::Filter {
                                                input: Box::new(plan),
                                                predicate: prop_filter,
                                                optional_variables: std::collections::HashSet::new(
                                                ),
                                            };
                                        }
                                        vars_in_scope.push(target_variable.clone());
                                    }

                                    if let Some(sv) = &r.variable {
                                        vars_in_scope.push(sv.clone());
                                    }
                                    i += 2;
                                } else {
                                    break;
                                }
                            } else {
                                break;
                            }
                        }
                    }
                    _ => return Err(anyhow!("Pattern must start with a node")),
                }
            }

            // Execute the plan to find all matches, then filter against bound variables in `row`.
        }

        let db_matches = self
            .execute_merge_read_plan(plan.clone(), prop_manager, params, vars_in_scope.clone())
            .await?;

        // Keep only DB results that are consistent with the input row bindings.
        // Skip internal keys (starting with "__") as they are implementation
        // artifacts (e.g. __used_edges) and not user-visible variable bindings.
        // Also skip the empty-string key (""), which is the placeholder variable
        // for unnamed MERGE nodes — it may carry over from a prior MERGE clause
        // and must not constrain the current pattern's match.
        let final_matches = db_matches
            .into_iter()
            .filter(|db_match| {
                row.iter().all(|(key, val)| {
                    if key.is_empty() || key.starts_with("__") {
                        return true;
                    }
                    let Some(db_val) = db_match.get(key) else {
                        return true;
                    };
                    if db_val == val {
                        return true;
                    }
                    // A dotted column is a *projection* of its variable, not a
                    // binding, and the two sides are produced by different plans
                    // — the outer scan and MERGE's own traversal — which spell
                    // an absent property set differently (`Map({})` against
                    // `Null`). Letting such a column veto the match made MERGE
                    // miss an edge that both sides agree exists, so ON MATCH SET
                    // silently did not run. If the base variable is the same
                    // entity on both sides, its projections cannot disagree
                    // about identity (#234).
                    if let Some((base, _)) = key.split_once('.')
                        && let (Some(row_base), Some(db_base)) = (row.get(base), db_match.get(base))
                        && let (Some(a), Some(b)) = (row_base.entity_ref(), db_base.entity_ref())
                        && a == b
                    {
                        return true;
                    }
                    // Values differ -- treat as consistent if they represent the same VID
                    matches!(
                        (Self::vid_from_value(val), Self::vid_from_value(db_val)),
                        (Ok(v1), Ok(v2)) if v1 == v2
                    )
                })
            })
            .map(|db_match| {
                let mut merged = row.clone();
                merged.extend(db_match);
                merged
            })
            .collect();

        Ok(final_matches)
    }

    /// Prepare a CREATE or MERGE pattern for path variable binding.
    ///
    /// A path variable (`CREATE p = (a)-[:R]->(:X)`) is built from the row's
    /// element values, so every element of a named path must land in the row.
    /// Anonymous nodes and relationships of such a path are given internal
    /// names, `__path{pi}_n{idx}` / `__path{pi}_r{idx}`, which
    /// `execute_create_pattern` and the MERGE match bind like any other
    /// variable and [`Self::bind_path_variables`] strips afterwards. The path
    /// index keeps two paths of one pattern from sharing a name.
    ///
    /// Returns the pattern — borrowed unchanged when it names no path — and
    /// the internal names to remove.
    pub(crate) fn prepare_pattern_for_path_binding(
        pattern: &Pattern,
    ) -> (Cow<'_, Pattern>, Vec<String>) {
        let has_path_vars = pattern
            .paths
            .iter()
            .any(|p| p.variable.as_ref().is_some_and(|v| !v.is_empty()));

        if !has_path_vars {
            return (Cow::Borrowed(pattern), Vec::new());
        }

        let mut modified = pattern.clone();
        let mut temp_vars = Vec::new();

        for (pi, path) in modified.paths.iter_mut().enumerate() {
            if path.variable.as_ref().is_none_or(|v| v.is_empty()) {
                continue;
            }
            for (idx, element) in path.elements.iter_mut().enumerate() {
                let (slot, kind) = match element {
                    PatternElement::Node(n) => (&mut n.variable, 'n'),
                    PatternElement::Relationship(r) => (&mut r.variable, 'r'),
                    PatternElement::Parenthesized { .. } => continue,
                };
                if slot.as_ref().is_none_or(String::is_empty) {
                    let temp_var = format!("__path{pi}_{kind}{idx}");
                    *slot = Some(temp_var.clone());
                    temp_vars.push(temp_var);
                }
            }
        }

        (Cow::Owned(modified), temp_vars)
    }

    /// Bind path variables in the result row from a pattern prepared by
    /// [`Self::prepare_pattern_for_path_binding`].
    ///
    /// Walks each named path, collects node/edge values from the row by
    /// variable name, and constructs a `Value::Path`. Every element of a
    /// prepared path has a name, so an element the row does not carry is an
    /// error: skipping it would bind a shorter path than the one written.
    ///
    /// # Errors
    ///
    /// Returns an error if an element of a named path is missing from the row
    /// or is not a node / relationship.
    fn bind_path_variables(
        pattern: &Pattern,
        row: &mut HashMap<String, Value>,
        temp_vars: &[String],
    ) -> Result<()> {
        for path in &pattern.paths {
            let Some(path_var) = path.variable.as_ref() else {
                continue;
            };
            if path_var.is_empty() {
                continue;
            }

            let mut nodes = Vec::new();
            let mut edges = Vec::new();

            for element in &path.elements {
                match element {
                    PatternElement::Node(n) => {
                        let node = n
                            .variable
                            .as_ref()
                            .and_then(|var| row.get(var))
                            .and_then(Self::value_to_node_for_path)
                            .ok_or_else(|| {
                                anyhow!(
                                    "cannot bind path '{path_var}': node {:?} is not bound \
                                     to a node in the row",
                                    n.variable
                                )
                            })?;
                        nodes.push(node);
                    }
                    PatternElement::Relationship(r) => {
                        let edge = r
                            .variable
                            .as_ref()
                            .and_then(|var| row.get(var))
                            .and_then(|val| Self::value_to_edge_for_path(val, &r.types))
                            .ok_or_else(|| {
                                anyhow!(
                                    "cannot bind path '{path_var}': relationship {:?} is not \
                                     bound to a relationship in the row",
                                    r.variable
                                )
                            })?;
                        edges.push(edge);
                    }
                    PatternElement::Parenthesized { .. } => {
                        return Err(anyhow!(
                            "cannot bind path '{path_var}': a parenthesized pattern \
                             cannot be part of a written path"
                        ));
                    }
                }
            }

            use uni_common::value::Path;
            row.insert(path_var.clone(), Value::Path(Path { nodes, edges }));
        }

        // Clean up internal temp variables
        for var in temp_vars {
            row.remove(var);
        }
        Ok(())
    }

    /// Run one CREATE pattern against `row` and bind its path variables.
    ///
    /// `prepared` / `temp_vars` come from
    /// [`Self::prepare_pattern_for_path_binding`], called once per pattern
    /// rather than once per row. Every CREATE executor goes through here so a
    /// `CREATE p = …` binds `p` wherever it runs.
    #[expect(clippy::too_many_arguments)]
    pub(crate) async fn execute_create_binding_paths(
        &self,
        prepared: &Pattern,
        temp_vars: &[String],
        row: &mut HashMap<String, Value>,
        writer: &Writer,
        prop_manager: &PropertyManager,
        params: &HashMap<String, Value>,
        ctx: Option<&QueryContext>,
        tx_l0: Option<&Arc<parking_lot::RwLock<uni_store::runtime::l0::L0Buffer>>>,
    ) -> Result<()> {
        self.execute_create_pattern(
            prepared,
            row,
            writer,
            prop_manager,
            params,
            ctx,
            tx_l0,
            None,
        )
        .await?;
        Self::bind_path_variables(prepared, row, temp_vars)
    }

    /// Convert a Value (Map or Node) to a Node for path construction.
    fn value_to_node_for_path(val: &Value) -> Option<uni_common::value::Node> {
        match val {
            Value::Node(n) => Some(n.clone()),
            Value::Map(map) => {
                // A node map keyed `_id`, or carrying `"Vid(7)"`, resolved to
                // `None` here — and `None` drops the node from the constructed
                // path silently, so `RETURN p` came back short rather than
                // erroring (#234).
                let uni_common::value::EntityRef::Vertex(vid) =
                    uni_common::value::entity_ref_from_map(map)?
                else {
                    return None;
                };
                let labels = val.entity_labels().unwrap_or_default();
                let properties: HashMap<String, Value> = map
                    .iter()
                    .filter(|(k, _)| !uni_common::core::schema::is_reserved_property_name(k))
                    .map(|(k, v)| (k.clone(), v.clone()))
                    .collect();
                Some(uni_common::value::Node {
                    vid,
                    labels,
                    properties,
                })
            }
            _ => None,
        }
    }

    /// Convert a Value (Map or Edge) to an Edge for path construction.
    fn value_to_edge_for_path(
        val: &Value,
        type_names: &[String],
    ) -> Option<uni_common::value::Edge> {
        match val {
            Value::Edge(e) => Some(e.clone()),
            Value::Map(map) => {
                // Twin of the node case above: a silent drop from the path.
                let uni_common::value::EntityRef::Edge(eid) =
                    uni_common::value::entity_ref_from_map(map)?
                else {
                    return None;
                };
                let edge_type = val
                    .edge_type_ref()
                    .and_then(|t| t.name().map(str::to_string))
                    .or_else(|| type_names.first().cloned())
                    .unwrap_or_default();
                let (src, dst) = val.edge_endpoints()?;
                let (src, dst) = (src?, dst?);
                let properties: HashMap<String, Value> = map
                    .iter()
                    .filter(|(k, _)| !uni_common::core::schema::is_reserved_property_name(k))
                    .map(|(k, v)| (k.clone(), v.clone()))
                    .collect();
                Some(uni_common::value::Edge {
                    eid,
                    edge_type,
                    src,
                    dst,
                    properties,
                })
            }
            _ => None,
        }
    }
}

/// Read a vertex's full property map, preferring `prefetched` over a fresh
/// per-row `Backend::scan`.
///
/// `prefetched` is built once at the top of `apply_mutations` via
/// `prefetch_set_targets` / `prefetch_remove_targets` (mutation_common.rs).
/// On a hit, we layer in L0 from `ctx` so writes from earlier rows of the
/// same `apply_mutations` invocation (counter increments, same-VID
/// duplicates from UNWIND) take precedence — the prefetch only snapshots
/// storage state at SET entry. On a miss, fall back to the existing
/// per-row path; this preserves correctness for newly created VIDs,
/// schemaless rows, multi-label corner cases, and non-Mutation callers
/// that pass `&Prefetch::default()`.
pub(crate) async fn read_vertex_props_with_prefetch(
    vid: Vid,
    prefetched: &Prefetch,
    prop_manager: &PropertyManager,
    ctx: Option<&QueryContext>,
) -> Result<uni_common::Properties> {
    match prefetched.vertex.get(&vid).cloned() {
        Some(mut base) => {
            if let Some(l0) = uni_store::runtime::l0_visibility::accumulate_vertex_props(vid, ctx) {
                for (k, v) in l0 {
                    base.insert(k, v);
                }
            }
            Ok(base)
        }
        None => Ok(prop_manager
            .get_all_vertex_props_with_ctx(vid, ctx)
            .await?
            .unwrap_or_default()),
    }
}

/// Edge equivalent of [`read_vertex_props_with_prefetch`]. On a hit, layer
/// in L0 edge props so writes from earlier rows of the same
/// `apply_mutations` invocation take precedence. On a miss, fall back to
/// the per-EID storage path.
pub(crate) async fn read_edge_props_with_prefetch(
    eid: Eid,
    prefetched: &Prefetch,
    prop_manager: &PropertyManager,
    ctx: Option<&QueryContext>,
) -> Result<uni_common::Properties> {
    match prefetched.edge.get(&eid).cloned() {
        Some(mut base) => {
            if let Some(l0) = uni_store::runtime::l0_visibility::accumulate_edge_props(eid, ctx) {
                for (k, v) in l0 {
                    base.insert(k, v);
                }
            }
            Ok(base)
        }
        None => Ok(prop_manager
            .get_all_edge_props_with_ctx(eid, ctx)
            .await?
            .unwrap_or_default()),
    }
}

/// The write path's rule for a value of a declared property: `val` in the
/// form a column declared `dt` stores, or the type error a `CREATE`/`SET`
/// would raise.
///
/// Installed into `uni-store` (`uni_store::storage::declared_values`) as the
/// rule a declaration applies to values a property already holds, so that
/// declaring then writing and writing then declaring agree.
///
/// # Errors
/// Returns an error if `dt` cannot hold `val`.
pub fn coerce_declared_value(
    prop_name: &str,
    dt: &uni_common::core::schema::DataType,
    val: Value,
) -> Result<Value> {
    Executor::coerce_to_declared_type(prop_name, dt, val)
}

#[cfg(test)]
mod tests {
    use super::*;

    // ── merge_props tests ────────────────────────────────────────────

    #[test]
    fn test_merge_props_replace_tombstones_missing_keys() {
        let current: HashMap<String, Value> = [
            ("name".into(), Value::String("Alice".into())),
            ("age".into(), Value::Int(30)),
        ]
        .into();
        let incoming: HashMap<String, Value> =
            [("name".into(), Value::String("Bob".into()))].into();

        let result = Executor::merge_props(current, incoming, true);
        assert_eq!(result.get("name"), Some(&Value::String("Bob".into())));
        assert_eq!(
            result.get("age"),
            Some(&Value::Null),
            "Missing keys should be tombstoned in replace mode"
        );
    }

    #[test]
    fn test_merge_props_merge_preserves_existing() {
        let current: HashMap<String, Value> = [
            ("name".into(), Value::String("Alice".into())),
            ("age".into(), Value::Int(30)),
        ]
        .into();
        let incoming: HashMap<String, Value> =
            [("city".into(), Value::String("NYC".into()))].into();

        let result = Executor::merge_props(current, incoming, false);
        assert_eq!(result.get("name"), Some(&Value::String("Alice".into())));
        assert_eq!(result.get("age"), Some(&Value::Int(30)));
        assert_eq!(result.get("city"), Some(&Value::String("NYC".into())));
    }

    #[test]
    fn test_merge_props_null_incoming_is_tombstone() {
        let current: HashMap<String, Value> =
            [("name".into(), Value::String("Alice".into()))].into();
        let incoming: HashMap<String, Value> = [("name".into(), Value::Null)].into();

        // Merge mode: null overwrites
        let result = Executor::merge_props(current.clone(), incoming.clone(), false);
        assert_eq!(result.get("name"), Some(&Value::Null));

        // Replace mode: null is tombstone
        let result = Executor::merge_props(current, incoming, true);
        assert_eq!(result.get("name"), Some(&Value::Null));
    }

    #[test]
    fn test_merge_props_empty_current() {
        let current: HashMap<String, Value> = HashMap::new();
        let incoming: HashMap<String, Value> =
            [("name".into(), Value::String("Alice".into()))].into();

        let result = Executor::merge_props(current, incoming, false);
        assert_eq!(result.get("name"), Some(&Value::String("Alice".into())));
        assert_eq!(result.len(), 1);
    }

    #[test]
    fn test_merge_props_empty_incoming_replace_tombstones_all() {
        let current: HashMap<String, Value> = [
            ("name".into(), Value::String("Alice".into())),
            ("age".into(), Value::Int(30)),
        ]
        .into();
        let incoming: HashMap<String, Value> = HashMap::new();

        let result = Executor::merge_props(current, incoming, true);
        assert_eq!(result.get("name"), Some(&Value::Null));
        assert_eq!(result.get("age"), Some(&Value::Null));
    }

    // ── extract_labels_from_node tests ───────────────────────────────

    #[test]
    fn test_extract_labels_from_map() {
        let mut map = HashMap::new();
        map.insert("_vid".into(), Value::Int(1));
        map.insert(
            "_labels".into(),
            Value::List(vec![
                Value::String("Person".into()),
                Value::String("Employee".into()),
            ]),
        );
        let val = Value::Map(map);

        let labels = Executor::extract_labels_from_node(&val);
        assert_eq!(
            labels,
            Some(vec!["Person".to_string(), "Employee".to_string()])
        );
    }

    #[test]
    fn test_extract_labels_from_value_node() {
        let node = uni_common::Node {
            vid: uni_common::core::id::Vid::from(1u64),
            labels: vec!["Person".to_string()],
            properties: HashMap::new(),
        };
        let labels = Executor::extract_labels_from_node(&Value::Node(node));
        assert_eq!(labels, Some(vec!["Person".to_string()]));
    }

    #[test]
    fn test_extract_labels_non_node_returns_none() {
        assert_eq!(Executor::extract_labels_from_node(&Value::Int(42)), None);
        assert_eq!(
            Executor::extract_labels_from_node(&Value::String("hello".into())),
            None
        );
    }

    // ── extract_user_properties_from_value tests ─────────────────────

    #[test]
    fn test_extract_user_props_strips_internal_keys() {
        let mut map = HashMap::new();
        map.insert("_vid".into(), Value::Int(1));
        map.insert(
            "_labels".into(),
            Value::List(vec![Value::String("Person".into())]),
        );
        map.insert("name".into(), Value::String("Alice".into()));
        map.insert("age".into(), Value::Int(30));

        let props = Executor::extract_user_properties_from_value(&Value::Map(map)).unwrap();
        assert_eq!(props.get("name"), Some(&Value::String("Alice".into())));
        assert_eq!(props.get("age"), Some(&Value::Int(30)));
        assert!(!props.contains_key("_vid"));
        assert!(!props.contains_key("_labels"));
    }

    #[test]
    fn test_extract_user_props_plain_map_returns_as_is() {
        let mut map = HashMap::new();
        map.insert("key".into(), Value::String("value".into()));

        let props = Executor::extract_user_properties_from_value(&Value::Map(map.clone())).unwrap();
        assert_eq!(props, map);
    }

    #[test]
    fn test_extract_user_props_from_value_node() {
        let mut properties = HashMap::new();
        properties.insert("name".into(), Value::String("Alice".into()));
        let node = uni_common::Node {
            vid: uni_common::core::id::Vid::from(1u64),
            labels: vec!["Person".to_string()],
            properties,
        };
        let props = Executor::extract_user_properties_from_value(&Value::Node(node)).unwrap();
        assert_eq!(props.get("name"), Some(&Value::String("Alice".into())));
    }
}
