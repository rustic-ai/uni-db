// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Template for graph algorithm procedures to reduce boilerplate.

use crate::algo::ProjectionBuilder;
use crate::algo::algorithms::Algorithm;
use crate::algo::procedures::{
    AlgoContext, AlgoProcedure, AlgoResultRow, ProcedureSignature, ValueType,
};
use anyhow::{Result, anyhow};
use futures::stream::{self, BoxStream, StreamExt};
use serde_json::Value;
use std::marker::PhantomData;

/// Adapter trait for specific graph algorithms.
pub trait GraphAlgoAdapter: Send + Sync + 'static {
    /// Name of the procedure (e.g., "algo.pageRank").
    const NAME: &'static str;

    /// The underlying algorithm.
    type Algo: Algorithm;

    /// Define algorithm-specific arguments (after nodeLabels and relationshipTypes).
    /// Returns: (name, type, default_value_if_optional)
    /// If default_value is None, it's required.
    fn specific_args() -> Vec<(&'static str, ValueType, Option<Value>)>;

    /// Define output columns.
    fn yields() -> Vec<(&'static str, ValueType)>;

    /// Convert parsed specific arguments to Algorithm Config.
    /// `args` contains only the algorithm-specific arguments.
    fn to_config(args: Vec<Value>) -> <Self::Algo as Algorithm>::Config;

    /// Convert algorithm result to output rows.
    fn map_result(result: <Self::Algo as Algorithm>::Result) -> Result<Vec<AlgoResultRow>>;

    /// Optional: Customize projection if needed (e.g., weights, directions).
    fn customize_projection(builder: ProjectionBuilder, _args: &[Value]) -> ProjectionBuilder {
        builder.include_reverse(Self::include_reverse())
    }

    /// Deprecated: use customize_projection instead.
    fn include_reverse() -> bool {
        true
    }
}

/// Generic implementation of `AlgoProcedure` for any `GraphAlgoAdapter`.
pub struct GenericAlgoProcedure<A: GraphAlgoAdapter> {
    _marker: PhantomData<A>,
}

impl<A: GraphAlgoAdapter> GenericAlgoProcedure<A> {
    pub fn new() -> Self {
        Self {
            _marker: PhantomData,
        }
    }
}

impl<A: GraphAlgoAdapter> Default for GenericAlgoProcedure<A> {
    fn default() -> Self {
        Self::new()
    }
}

impl<A: GraphAlgoAdapter> AlgoProcedure for GenericAlgoProcedure<A>
where
    <A::Algo as Algorithm>::Result: Send + 'static,
{
    fn name(&self) -> &str {
        A::NAME
    }

    fn signature(&self) -> ProcedureSignature {
        let mut args = vec![
            ("nodeLabels", ValueType::List),
            ("relationshipTypes", ValueType::List),
        ];
        let mut optional_args = Vec::new();

        for (name, ty, default) in A::specific_args() {
            if let Some(def) = default {
                optional_args.push((name, ty, def));
            } else {
                args.push((name, ty));
            }
        }

        ProcedureSignature {
            args,
            optional_args,
            yields: A::yields(),
        }
    }

    fn execute(
        &self,
        ctx: AlgoContext,
        args: Vec<Value>,
    ) -> BoxStream<'static, Result<AlgoResultRow>> {
        let signature = self.signature();
        let args = match signature.validate_args(args) {
            Ok(a) => a,
            Err(e) => return stream::once(async { Err(e) }).boxed(),
        };

        // Standard args (0 and 1)
        let node_labels = args[0]
            .as_array()
            .unwrap()
            .iter()
            .map(|v| v.as_str().unwrap().to_string())
            .collect::<Vec<_>>();
        let edge_types = args[1]
            .as_array()
            .unwrap()
            .iter()
            .map(|v| v.as_str().unwrap().to_string())
            .collect::<Vec<_>>();

        // Specific args (2 onwards)
        let specific_args = args[2..].to_vec();

        let stream = async_stream::try_stream! {
            let schema = ctx.storage.schema_manager().schema();

            // Validate that labels and edge types exist
            for label in &node_labels {
                if !schema.labels.contains_key(label) {
                    Err(anyhow!("Label '{}' not found", label))?;
                }
            }
            for etype in &edge_types {
                if !schema.edge_types.contains_key(etype) {
                    Err(anyhow!("Edge type '{}' not found", etype))?;
                }
            }

            // 1. Build Projection
            let mut builder = ProjectionBuilder::new(ctx.storage.clone())
                .l0_manager(ctx.l0_manager.clone())
                .node_labels(&node_labels.iter().map(|s| s.as_str()).collect::<Vec<_>>())
                .edge_types(&edge_types.iter().map(|s| s.as_str()).collect::<Vec<_>>());

            builder = A::customize_projection(builder, &specific_args);

            let projection = builder.build().await?;

            // 2. Run Algorithm
            let config = A::to_config(specific_args);

            let result = tokio::task::spawn_blocking(move || {
                A::Algo::run(&projection, config)
            }).await?;

            // 3. Stream Results
            let rows = A::map_result(result)?;
            for row in rows {
                yield row;
            }
        };

        Box::pin(stream)
    }
}
