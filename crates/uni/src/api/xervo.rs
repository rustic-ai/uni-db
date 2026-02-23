// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

use std::sync::Arc;

use crate::api::Uni;
use uni_common::{Result, UniError};
use uni_xervo::runtime::ModelRuntime;
use uni_xervo::traits::{GenerationOptions, GenerationResult};

fn into_uni_error<E: std::fmt::Display>(err: E) -> UniError {
    UniError::Internal(anyhow::anyhow!(err.to_string()))
}

/// Facade for using Uni-Xervo runtime from the Uni API surface.
#[derive(Clone)]
pub struct UniXervo {
    runtime: Arc<ModelRuntime>,
}

impl UniXervo {
    pub(crate) fn new(runtime: Arc<ModelRuntime>) -> Self {
        Self { runtime }
    }

    /// Embed text inputs using a configured model alias.
    pub async fn embed(&self, alias: &str, texts: &[&str]) -> Result<Vec<Vec<f32>>> {
        let embedder = self
            .runtime
            .embedding(alias)
            .await
            .map_err(into_uni_error)?;
        embedder.embed(texts.to_vec()).await.map_err(into_uni_error)
    }

    /// Generate text using a configured model alias.
    pub async fn generate(
        &self,
        alias: &str,
        messages: &[String],
        options: GenerationOptions,
    ) -> Result<GenerationResult> {
        let generator = self
            .runtime
            .generator(alias)
            .await
            .map_err(into_uni_error)?;
        generator
            .generate(messages, options)
            .await
            .map_err(into_uni_error)
    }

    /// Access the underlying Uni-Xervo runtime.
    pub fn raw_runtime(&self) -> &Arc<ModelRuntime> {
        &self.runtime
    }
}

impl Uni {
    /// Access Uni-Xervo runtime facade configured for this database.
    pub fn xervo(&self) -> Result<UniXervo> {
        let runtime = self.xervo_runtime.clone().ok_or_else(|| {
            UniError::Internal(anyhow::anyhow!("Uni-Xervo runtime is not configured"))
        })?;
        Ok(UniXervo::new(runtime))
    }
}
