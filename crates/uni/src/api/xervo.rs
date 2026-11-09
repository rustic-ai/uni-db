// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

use std::sync::Arc;

use crate::api::Uni;
use uni_common::{Result, UniError};
pub use uni_query::nlp::{
    DEFAULT_NLP_ALIAS, ExtractKind, ExtractOptions, ExtractedEntity, ExtractedFact, NlpInput,
    SchemaSource,
};
use uni_query::nlp::{ExtractRows, NlpExtractError};
use uni_xervo::api::ModelAliasSpec;
pub use uni_xervo::runtime::ModelRuntime;
pub use uni_xervo::traits::{
    AudioOutput, ContentBlock, GeneratedImage, GenerationOptions, GenerationResult, ImageInput,
    Message, MessageRole, RerankerModel, ScoredDoc, TokenUsage,
};
pub use uni_xervo::traits::{
    ExtractionSchema, FactModality, FactPolarity, LabelSpec, NlpCalibration,
    NlpCalibrationOverrides, NlpEntity, NlpFact, NlpModel, NlpRequest, NlpResult, NlpTasks,
    NlpWindow, Turn,
};
#[cfg(feature = "provider-onnx")]
pub use uni_xervo::traits::{RawTensorModel, TensorBatch, TensorSpec, TensorValue};

fn into_uni_error<E: std::fmt::Display>(err: E) -> UniError {
    UniError::Internal(anyhow::anyhow!(err.to_string()))
}

fn not_configured() -> UniError {
    UniError::Internal(anyhow::anyhow!("Uni-Xervo runtime is not configured"))
}

fn nlp_error(err: NlpExtractError) -> UniError {
    match err {
        NlpExtractError::InvalidArgument(message) => UniError::InvalidArgument {
            arg: "nlp".to_owned(),
            message,
        },
        NlpExtractError::NotConfigured => not_configured(),
        NlpExtractError::Model(message) => UniError::Internal(anyhow::anyhow!(message)),
    }
}

/// Facade for using Uni-Xervo runtime from the Uni API surface.
///
/// Returned by [`Uni::xervo()`]. When no Xervo catalog was configured at
/// build time, the facade is still returned (infallible accessor) but all
/// operation methods will return an error. Use [`is_available()`](Self::is_available)
/// to check upfront.
#[derive(Clone)]
pub struct UniXervo {
    runtime: Option<Arc<ModelRuntime>>,
    schema: Arc<uni_common::core::schema::SchemaManager>,
}

impl UniXervo {
    /// Whether a Xervo runtime is configured and available for use.
    pub fn is_available(&self) -> bool {
        self.runtime.is_some()
    }

    /// Embed text inputs using a configured model alias.
    pub async fn embed(&self, alias: &str, texts: &[&str]) -> Result<Vec<Vec<f32>>> {
        let runtime = self.runtime.as_ref().ok_or_else(not_configured)?;
        let embedder = runtime.embedding(alias).await.map_err(into_uni_error)?;
        Ok(embedder.embed(texts).await.map_err(into_uni_error)?.vectors)
    }

    /// Embed text inputs into per-token (multi-vector / ColBERT late-interaction) vectors
    /// using a configured model alias. Returns, per input, a ragged list of token vectors —
    /// the shape stored in a `List<Vector>` property and consumed by MaxSim retrieval.
    pub async fn embed_multivector(
        &self,
        alias: &str,
        texts: &[&str],
    ) -> Result<Vec<Vec<Vec<f32>>>> {
        let runtime = self.runtime.as_ref().ok_or_else(not_configured)?;
        let embedder = runtime
            .multi_vector_embedder(alias)
            .await
            .map_err(into_uni_error)?;
        Ok(embedder.embed(texts).await.map_err(into_uni_error)?.vectors)
    }

    /// Embed text inputs through a multi-functional model (e.g. BGE-M3) in a SINGLE forward
    /// pass, returning the dense and per-token (ColBERT) heads together: `(dense, multivector)`
    /// where each is `Some` iff the model produced that head. The alias must resolve to an
    /// `EmbedHybrid` model. This is what powers single-pass hybrid auto-embed (one inference
    /// feeding both a `Vector` and a `List<Vector>` column).
    #[allow(clippy::type_complexity)]
    pub async fn embed_hybrid(
        &self,
        alias: &str,
        texts: &[&str],
    ) -> Result<(Option<Vec<Vec<f32>>>, Option<Vec<Vec<Vec<f32>>>>)> {
        use uni_xervo::traits::hybrid::HeadSet;
        let runtime = self.runtime.as_ref().ok_or_else(not_configured)?;
        let embedder = runtime
            .hybrid_embedder(alias)
            .await
            .map_err(into_uni_error)?;
        let res = embedder
            .embed(texts, HeadSet::DENSE | HeadSet::MULTI_VECTOR)
            .await
            .map_err(into_uni_error)?;
        Ok((res.dense, res.multi_vector))
    }

    /// Generate using a configured model alias with structured messages.
    pub async fn generate(
        &self,
        alias: &str,
        messages: &[Message],
        options: GenerationOptions,
    ) -> Result<GenerationResult> {
        let runtime = self.runtime.as_ref().ok_or_else(not_configured)?;
        let generator = runtime.generator(alias).await.map_err(into_uni_error)?;
        generator
            .generate(messages, options)
            .await
            .map_err(into_uni_error)
    }

    /// Generate text using plain string messages (convenience wrapper).
    ///
    /// Each string is treated as a user message. For multi-role conversations
    /// or multimodal inputs, use [`generate`](Self::generate) with [`Message`] directly.
    pub async fn generate_text(
        &self,
        alias: &str,
        messages: &[&str],
        options: GenerationOptions,
    ) -> Result<GenerationResult> {
        let structured: Vec<Message> = messages.iter().map(|s| Message::user(*s)).collect();
        self.generate(alias, &structured, options).await
    }

    /// Obtain an [`RawTensorModel`] for the given model alias.
    ///
    /// The runner provides tensor-in/tensor-out ONNX inference via the
    /// [`LocalOnnxProvider`](uni_xervo::provider::LocalOnnxProvider).
    /// Models are downloaded from HuggingFace and cached on first use.
    ///
    /// # Errors
    ///
    /// Returns [`UniError`] if the runtime is not configured or the alias
    /// is not registered in the catalog.
    #[cfg(feature = "provider-onnx")]
    pub async fn raw_tensor_model(
        &self,
        alias: &str,
    ) -> Result<Arc<dyn uni_xervo::traits::RawTensorModel>> {
        let runtime = self.runtime.as_ref().ok_or_else(not_configured)?;
        runtime
            .raw_tensor_model(alias)
            .await
            .map_err(into_uni_error)
    }

    /// Rerank documents against a query using a configured cross-encoder model.
    ///
    /// Returns [`ScoredDoc`]s sorted by relevance score (descending).
    /// The model alias must point to a catalog entry with `task: Rerank`.
    ///
    /// # Errors
    ///
    /// Returns [`UniError`] if the runtime is not configured, the alias
    /// is not registered, or inference fails.
    pub async fn rerank(
        &self,
        alias: &str,
        query: &str,
        documents: &[&str],
    ) -> Result<Vec<uni_xervo::traits::ScoredDoc>> {
        let runtime = self.runtime.as_ref().ok_or_else(not_configured)?;
        let reranker = runtime.reranker(alias).await.map_err(into_uni_error)?;
        reranker
            .rerank(query, documents)
            .await
            .map_err(into_uni_error)
    }

    /// Run raw NLP analysis requests against a configured `nlp` alias.
    ///
    /// This is the full uni-xervo surface: per-window POS, lemmas,
    /// dependencies, SRL, coreference, entities and facts. For flat fact or
    /// entity rows with character spans, use [`extract_facts`](Self::extract_facts)
    /// or [`extract_entities`](Self::extract_entities).
    ///
    /// # Errors
    ///
    /// Returns [`UniError`] if the runtime is not configured, the alias
    /// is not registered, or inference fails.
    pub async fn nlp(&self, alias: &str, requests: Vec<NlpRequest<'_>>) -> Result<Vec<NlpResult>> {
        let runtime = self.runtime.as_ref().ok_or_else(not_configured)?;
        let model = runtime.nlp_model(alias).await.map_err(into_uni_error)?;
        model.analyze(requests).await.map_err(into_uni_error)
    }

    /// Extract facts (`head --relation--> tail`, with polarity, modality,
    /// holder, time and margin) from text with a kniv v7 model.
    ///
    /// The same rows `CALL uni.nlp.extract(...)` yields. A graph-derived
    /// schema ([`ExtractOptions::graph_schema`]) uses the primary schema.
    ///
    /// # Errors
    ///
    /// [`UniError::InvalidArgument`] for malformed options;
    /// [`UniError::Internal`] when the runtime is not configured, the alias
    /// does not resolve to a kniv v7 model, or inference fails.
    pub async fn extract_facts(
        &self,
        input: &NlpInput,
        options: &ExtractOptions,
    ) -> Result<Vec<ExtractedFact>> {
        match self.extract(input, options, ExtractKind::Facts).await? {
            ExtractRows::Facts(rows) => Ok(rows),
            ExtractRows::Entities(_) => unreachable!("facts requested"),
        }
    }

    /// Extract typed entities, grouped across their mentions, from text with
    /// a kniv v7 model. The same rows `CALL uni.nlp.entities(...)` yields.
    ///
    /// # Errors
    ///
    /// As for [`extract_facts`](Self::extract_facts).
    pub async fn extract_entities(
        &self,
        input: &NlpInput,
        options: &ExtractOptions,
    ) -> Result<Vec<ExtractedEntity>> {
        match self.extract(input, options, ExtractKind::Entities).await? {
            ExtractRows::Entities(rows) => Ok(rows),
            ExtractRows::Facts(_) => unreachable!("entities requested"),
        }
    }

    async fn extract(
        &self,
        input: &NlpInput,
        options: &ExtractOptions,
        kind: ExtractKind,
    ) -> Result<ExtractRows> {
        options.validate(kind).map_err(nlp_error)?;
        let runtime = self.runtime.as_ref().ok_or_else(not_configured)?;
        let schema = self.schema.schema();
        uni_query::nlp::run_extract(runtime, &schema, input, options, kind)
            .await
            .map_err(nlp_error)
    }

    /// Pre-load and cache every model in the Xervo catalog.
    ///
    /// Models already loaded are skipped. Fails fast on the first error.
    /// Call this during application startup to avoid cold-start latency on
    /// first inference.
    pub async fn prefetch_all(&self) -> Result<()> {
        let runtime = self.runtime.as_ref().ok_or_else(not_configured)?;
        runtime.prefetch_all().await.map_err(into_uni_error)
    }

    /// Pre-load and cache specific model aliases.
    ///
    /// Returns an error immediately if an alias is not found in the catalog
    /// or if any model fails to load. Models already loaded are skipped.
    pub async fn prefetch(&self, aliases: &[&str]) -> Result<()> {
        let runtime = self.runtime.as_ref().ok_or_else(not_configured)?;
        runtime.prefetch(aliases).await.map_err(into_uni_error)
    }

    /// Access the underlying Uni-Xervo runtime, if configured.
    pub fn raw_runtime(&self) -> Option<&Arc<ModelRuntime>> {
        self.runtime.as_ref()
    }
}

impl Uni {
    /// Access Uni-Xervo runtime facade configured for this database.
    ///
    /// Always succeeds — returns a facade even when no Xervo catalog is
    /// configured. Individual methods (`embed`, `generate`, etc.) will return
    /// an error in that case. Use [`UniXervo::is_available()`] to check upfront.
    pub fn xervo(&self) -> UniXervo {
        UniXervo {
            runtime: self.inner.xervo_runtime.clone(),
            schema: self.inner.schema.clone(),
        }
    }
}

/// Builds a Xervo runtime with every compiled-in provider registered.
///
/// This is the single definition of which providers Uni registers. Both
/// [`UniBuilder::build`](crate::api::UniBuilder::build) and any standalone
/// caller route through it, so the enabled-provider set cannot drift between
/// a runtime that a database built for itself and one built to be shared.
///
/// The resulting runtime owns its catalog and providers, so the returned
/// `Arc` is self-contained: hand the same one to several
/// [`UniBuilder::xervo_runtime`](crate::api::UniBuilder::xervo_runtime) calls
/// and the databases share one set of loaded models rather than each
/// deserializing its own copy of the weights.
///
/// # Examples
///
/// ```no_run
/// # async fn example() -> uni_common::Result<()> {
/// let catalog = uni_db::xervo_catalog_from_str(r#"[{
///     "alias": "embed/default", "task": "embed",
///     "provider_id": "local/onnx", "model_id": "AllMiniLML6V2"
/// }]"#).unwrap();
/// let runtime = uni_db::xervo::build_model_runtime(catalog).await?;
///
/// let a = uni_db::Uni::open("/tmp/a").xervo_runtime(runtime.clone()).build().await?;
/// let b = uni_db::Uni::open("/tmp/b").xervo_runtime(runtime).build().await?;
/// # let _ = (a, b);
/// # Ok(())
/// # }
/// ```
///
/// # Errors
///
/// Returns [`UniError::Internal`] when the catalog is invalid — a duplicate
/// alias, or a `provider_id` with no matching provider compiled in. A build
/// with no `provider-*` features enabled registers nothing, so any non-empty
/// catalog fails.
pub async fn build_model_runtime(catalog: Vec<ModelAliasSpec>) -> Result<Arc<ModelRuntime>> {
    // `mut` is conditional on at least one provider-* feature being
    // enabled; a slim build with no providers leaves it unused.
    #[allow(unused_mut)]
    let mut runtime_builder = ModelRuntime::builder().catalog(catalog);
    #[cfg(feature = "provider-candle")]
    {
        runtime_builder =
            runtime_builder.register_provider(uni_xervo::provider::LocalCandleProvider::new());
    }
    #[cfg(feature = "provider-openai")]
    {
        runtime_builder =
            runtime_builder.register_provider(uni_xervo::provider::RemoteOpenAIProvider::new());
    }
    #[cfg(feature = "provider-gemini")]
    {
        runtime_builder =
            runtime_builder.register_provider(uni_xervo::provider::RemoteGeminiProvider::new());
    }
    #[cfg(feature = "provider-vertexai")]
    {
        runtime_builder =
            runtime_builder.register_provider(uni_xervo::provider::RemoteVertexAIProvider::new());
    }
    #[cfg(feature = "provider-mistral")]
    {
        runtime_builder =
            runtime_builder.register_provider(uni_xervo::provider::RemoteMistralProvider::new());
    }
    #[cfg(feature = "provider-anthropic")]
    {
        runtime_builder =
            runtime_builder.register_provider(uni_xervo::provider::RemoteAnthropicProvider::new());
    }
    #[cfg(feature = "provider-voyageai")]
    {
        runtime_builder =
            runtime_builder.register_provider(uni_xervo::provider::RemoteVoyageAIProvider::new());
    }
    #[cfg(feature = "provider-cohere")]
    {
        runtime_builder =
            runtime_builder.register_provider(uni_xervo::provider::RemoteCohereProvider::new());
    }
    #[cfg(feature = "provider-azure-openai")]
    {
        runtime_builder = runtime_builder
            .register_provider(uni_xervo::provider::RemoteAzureOpenAIProvider::new());
    }
    #[cfg(feature = "provider-llamacpp")]
    {
        runtime_builder =
            runtime_builder.register_provider(uni_xervo::provider::RemoteLlamaCppProvider::new());
    }
    #[cfg(feature = "provider-onnx")]
    {
        runtime_builder =
            runtime_builder.register_provider(uni_xervo::provider::LocalOnnxProvider::new());
    }

    runtime_builder.build().await.map_err(into_uni_error)
}
