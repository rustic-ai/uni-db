// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! # Uni - Embedded Graph Database
//!
//! Uni is an embedded, object-store-backed graph database with OpenCypher queries,
//! columnar analytics, and vector search.

pub mod api;
/// Change-data-capture runtime — moved to `uni-plugin-host`; re-exported to
/// keep the `uni_db::cdc_runtime::*` path stable.
pub mod cdc_runtime {
    pub use uni_plugin_host::cdc_runtime::*;
}
/// OpenTelemetry layer — moved to `uni-plugin-host`; re-exported to keep the
/// `uni_db::observability::*` path stable.
pub mod observability {
    pub use uni_plugin_host::observability::*;
}
/// Meta-plugin persistence — moved to `uni-plugin-host`; re-exported to keep
/// the `uni_db::persistence::*` path stable.
pub mod persistence {
    pub use uni_plugin_host::persistence::*;
}
/// Background-job scheduler — moved to `uni-plugin-host`; re-exported to keep
/// the `uni_db::scheduler::*` path stable.
pub mod scheduler {
    pub use uni_plugin_host::scheduler::*;
}
/// Durable scheduler persistence — moved to `uni-plugin-host`; re-exported.
pub mod scheduler_persistence {
    pub use uni_plugin_host::scheduler_persistence::*;
}
/// Graceful-shutdown coordinator — moved to `uni-plugin-host`; re-exported.
pub(crate) mod shutdown {
    pub use uni_plugin_host::shutdown::*;
}
/// Synthetic declared-procedure host — moved to `uni-plugin-host`; re-exported
/// to keep the `uni_db::synthetic_procedure::*` path stable.
pub mod synthetic_procedure {
    pub use uni_plugin_host::synthetic_procedure::*;
}

/// Re-export of the host-side declared-trigger synthesizer (WS-A) to
/// keep the `uni_db::synthetic_trigger::*` path stable.
pub mod synthetic_trigger {
    pub use uni_plugin_host::synthetic_trigger::*;
}

pub use api::builder::PropertiesBuilder;
pub use api::hooks::{CommitHookContext, HookContext, QueryType, SessionHook};
pub use api::impl_locy::LocyRuleRegistry;
pub use api::locy_rule_catalog::{LocyRulePersister, RegisteredSource};
pub use api::multi_agent::{LeaseGuard, WriteLease, WriteLeaseProvider};
pub use api::notifications::{CommitNotification, CommitStream, WatchBuilder};
pub use api::prepared::{PreparedLocy, PreparedLocyBinder, PreparedQuery, PreparedQueryBinder};
pub use api::retry::RetryOptions;
pub use api::rule_registry::{RuleInfo, RuleRegistry};
pub use api::schema::{
    ConstraintInfo, EdgeTypeBuilder, EdgeTypeInfo, EmbeddingCfg, IndexInfo, IndexType,
    LabelBuilder, LabelInfo, PropertyInfo, ScalarType, SchemaBuilder, VectorAlgo, VectorIndexCfg,
    VectorMetric,
};
pub use api::session::{
    Session, SessionCapabilities, SessionMetrics, TransactionBuilder, WriteLeaseSummary,
};
pub use api::sync::{
    ApplyBuilderSync, ExecuteBuilderSync, LocyBuilderSync, QueryBuilderSync, RuleRegistrySync,
    SessionSync, TransactionBuilderSync, TransactionSync, TxLocyBuilderSync, TxQueryBuilderSync,
    UniSync,
};
pub use api::template::{SessionTemplate, SessionTemplateBuilder};
pub use api::transaction::{
    ApplyBuilder, ApplyResult, CommitResult, ExecuteBuilder, IsolationLevel, Transaction,
};
#[cfg(feature = "provider-onnx")]
pub use api::xervo::{RawTensorModel, TensorBatch, TensorSpec, TensorValue};
pub use api::xervo::{RerankerModel, ScoredDoc, UniXervo};
pub use uni_bulk::{AppenderBuilder, StreamingAppender};
pub use uni_bulk::{
    BulkPhase, BulkProgress, BulkStats, BulkWriter, BulkWriterBuilder, EdgeData, IntoArrow,
};

// Fork diff/promote value types, re-exported from `uni-fork`.
pub use api::{DatabaseMetrics, ThrottlePressure, Uni, UniBuilder};
pub use uni_fork::{
    ConflictPolicy, DiffEdge, DiffVertex, EdgeDiff, EdgePropertyChange, ForkDiff, PromoteBaseline,
    PromoteOptions, PromotePattern, PromoteReport, PropertyChange, VertexDiff,
    VertexPropertyChange,
};

/// `mimalloc` allocator, re-exported under the `mimalloc` feature.
///
/// Wire it as your global allocator in the consuming binary for ~3x
/// throughput on allocation-heavy workloads (per-statement Cypher CREATE,
/// many small mutations):
///
/// ```ignore
/// #[global_allocator]
/// static GLOBAL: uni_db::MiMalloc = uni_db::MiMalloc;
/// ```
///
/// See `crates/uni/benches/concurrent_mutations.rs` for the measurement.
#[cfg(feature = "mimalloc")]
pub use mimalloc::MiMalloc;
pub use uni_xervo::api::{
    ModelAliasSpec, ModelTask, WarmupPolicy, catalog_from_file as xervo_catalog_from_file,
    catalog_from_str as xervo_catalog_from_str,
};
/// The Xervo model runtime, re-exported so downstream crates can name the type
/// that [`UniBuilder::xervo_runtime`](api::UniBuilder::xervo_runtime) and
/// [`UniXervo::raw_runtime`](api::xervo::UniXervo::raw_runtime) traffic in
/// without taking a direct `uni-xervo` dependency.
///
/// Deliberately unconditional: the Python binding names it as
/// `::uni_db::ModelRuntime` in every wheel variant, including the slim builds
/// that enable only a subset of the `provider-*` features.
pub use uni_xervo::runtime::ModelRuntime;

// Re-exports from internal crates
pub use uni_common::{
    CrdtType, DataType, Eid, LocyIncomplete, LocyIncompleteReason, Result, Schema, UniConfig,
    UniError, UniId, Vid, unival,
};
pub use uni_query::{
    Edge, ExecuteResult, ExplainOutput, FromValue, Node, Path, ProfileOutput, QueryMetrics,
    QueryResult, QueryWarning, Row, Value,
};

#[cfg(feature = "storage-internals")]
pub use uni_store::storage::StorageManager;

#[cfg(feature = "snapshot-internals")]
pub use uni_common::core::snapshot::SnapshotManifest;
#[cfg(feature = "snapshot-internals")]
pub use uni_store::snapshot::manager::SnapshotManager;

// Re-export crates
pub use uni_algo as algo_crate;
pub use uni_common as common;
pub use uni_query as query_crate;
pub use uni_store as store;

// Module aliases for internal crate access
pub mod core {
    pub use crate::common::core::*;
}

pub mod storage {
    pub use crate::store::storage::*;
    // Fix for tests expecting IndexManager in storage root or similar?
    // tests use uni_db::storage::manager::StorageManager.
    // crate::store::storage has manager.
}

pub mod runtime {
    pub use crate::store::runtime::*;
}

pub mod query {
    pub use crate::query_crate::query::*;
}

pub mod algo {
    // Tests use uni_db::algo::* (from src/algo).
    // uni-algo has `algo` module.
    pub use crate::algo_crate::algo::*;
}

pub mod xervo {
    pub use crate::api::xervo::*;
}

pub mod locy {
    pub use crate::api::impl_locy::LocyEngine;
    pub use crate::api::locy_result::{LocyExplainOutput, LocyProfileOutput, LocyResult};
    pub use uni_cypher::locy_ast::LocyProgram;
    pub use uni_cypher::{ParseError, parse_locy};
    pub use uni_locy::LocyResult as RawLocyResult;
    pub use uni_locy::magic;
    pub use uni_locy::{
        CommandResult, CompiledProgram, DerivedEdge, DerivedFactSet, GoalDirected,
        GoalDirectedReport, LocyCompileError, LocyConfig, LocyError, rewrite_goal_directed,
    };
    pub use uni_query::{
        LocyExecProfile, LocyIterationProfile, LocyRuleProfile, LocyStratumProfile,
    };
}
