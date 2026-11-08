// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Host-coupled built-in procedure plugins.
//!
//! These procedures depend on `uni-store` / `uni-algo` types that
//! `uni-plugin-builtin` cannot reach without a layering inversion
//! (it depends only on `uni-plugin`). They live here in `uni-query`
//! because that's where the host dependencies converge.
//!
//! Registered alongside `BuiltinPlugin` and `ApocCorePlugin` from
//! `uni::api::register_builtin_plugins`.
//!
//! Each submodule covers one namespace:
//!
//! - [`schema`] — `uni.schema.{labels, edgeTypes, relationshipTypes, indexes, constraints, labelInfo}`
//! - [`vector`] / [`fts`] / [`search`] — `uni.vector.query`, `uni.fts.query`, `uni.search`
//! - [`algo`] — `uni.algo.*` (32 algorithms via a single `AlgorithmProcedureAdapter`)

use std::sync::Arc;

use uni_algo::algo::AlgorithmRegistry;
use uni_plugin::{PluginError, PluginRegistrar};

pub mod algo;
pub mod create;
pub mod fts;
pub mod graph;
mod host_args;
pub mod nlp;
pub mod schema;
pub mod search;
pub mod sparse;
pub mod vector;

// Rust guideline compliant

/// Construct a fresh [`uni_plugin::PluginRegistry`] pre-loaded with every
/// host-coupled built-in procedure (uni.schema.*, uni.algo.*).
///
/// Useful for low-level test setups that bypass `Uni::build` and would
/// otherwise see "Procedure not supported" errors for `uni.algo.*` /
/// `uni.schema.*` calls now that those match arms in `procedure_call.rs`
/// have been deleted in favor of plugin-path dispatch.
///
/// Panics on registration failure (only possible on duplicate qname,
/// which the static built-in set never produces).
#[must_use]
pub fn default_host_plugin_registry() -> Arc<uni_plugin::PluginRegistry> {
    use uni_plugin::{Capability, CapabilitySet, PluginId, PluginRegistrar};
    let registry = Arc::new(uni_plugin::PluginRegistry::default());
    let plugin_id = PluginId::new("uni");
    let caps = CapabilitySet::from_iter_of([Capability::Procedure, Capability::ProcedureSchema]);
    let mut r = PluginRegistrar::new(plugin_id, &caps, &registry);
    let algo_registry: Arc<AlgorithmRegistry> = Arc::new(AlgorithmRegistry::new());
    register_into(&mut r, Some(&algo_registry))
        .expect("default host plugin registration must succeed");
    r.commit_to_registry()
        .expect("default host plugin commit must succeed");
    registry
}

/// Register every host-coupled built-in procedure into `r`.
///
/// `algo_registry` is the same `Arc<AlgorithmRegistry>` that the host
/// wires into `GraphExecutionContext::with_algo_registry()`; pass `None`
/// in test setups that do not need the `uni.algo.*` namespace.
///
/// # Errors
///
/// Returns [`PluginError::DuplicateRegistration`] if a procedure qname
/// is already taken in the underlying registry.
pub fn register_into(
    r: &mut PluginRegistrar<'_>,
    algo_registry: Option<&Arc<AlgorithmRegistry>>,
) -> Result<(), PluginError> {
    schema::register_into(r)?;
    vector::register_into(r)?;
    fts::register_into(r)?;
    sparse::register_into(r)?;
    search::register_into(r)?;
    graph::register_into(r)?;
    create::register_into(r)?;
    nlp::register_into(r)?;
    if let Some(algo) = algo_registry {
        algo::register_into(r, algo)?;
    }
    Ok(())
}
