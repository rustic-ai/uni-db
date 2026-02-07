// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Core embedding service trait and FastEmbed implementation.

use anyhow::{Result, anyhow};
use async_trait::async_trait;
use std::sync::Arc;
use uni_common::core::schema::EmbeddingModel;

/// Trait for embedding services that convert text to vector representations.
#[async_trait]
pub trait EmbeddingService: Send + Sync {
    /// Generates embeddings for the given texts.
    async fn embed(&self, texts: &[&str]) -> Result<Vec<Vec<f32>>>;

    /// Returns the embedding dimensions for this model.
    fn dimensions(&self) -> u32;

    /// Returns the model name.
    fn model_name(&self) -> &str;
}

/// Returns a unique cache key for an embedding model configuration.
pub fn embedding_service_key(model: &EmbeddingModel) -> Result<String> {
    match model {
        EmbeddingModel::Candle { model_name, .. } => Ok(format!("candle:{}", model_name)),
        EmbeddingModel::FastEmbed { model_name, .. } => Ok(format!("fastembed:{}", model_name)),
        EmbeddingModel::OpenAI { model, .. } => Ok(format!("openai:{}", model)),
        EmbeddingModel::Ollama { model, .. } => Ok(format!("ollama:{}", model)),
        _ => Err(anyhow!("Unsupported embedding model")),
    }
}

/// Creates an embedding service for the given model configuration.
///
/// This is the central factory for embedding services, consolidating the
/// feature-gated logic for Candle and FastEmbed backends.
pub fn create_embedding_service(model: &EmbeddingModel) -> Result<Arc<dyn EmbeddingService>> {
    match model {
        #[cfg(feature = "candle-text")]
        EmbeddingModel::Candle {
            model_name,
            revision,
        } => {
            use super::CandleTextEmbedding;
            Ok(Arc::new(CandleTextEmbedding::from_model_name(
                model_name,
                revision.clone(),
            )?))
        }
        #[cfg(not(feature = "candle-text"))]
        EmbeddingModel::Candle { .. } => Err(anyhow!(
            "Candle embedding requires the 'candle-text' feature"
        )),
        #[cfg(feature = "fastembed")]
        EmbeddingModel::FastEmbed {
            model_name,
            cache_dir,
            ..
        } => Ok(Arc::new(FastEmbedService::new(
            model_name,
            cache_dir.as_deref(),
        )?)),
        #[cfg(not(feature = "fastembed"))]
        EmbeddingModel::FastEmbed { .. } => Err(anyhow!(
            "FastEmbed embedding requires the 'fastembed' feature"
        )),
        EmbeddingModel::OpenAI { .. } => Err(anyhow!("OpenAI embedding not yet implemented")),
        EmbeddingModel::Ollama { .. } => Err(anyhow!("Ollama embedding not yet implemented")),
        _ => Err(anyhow!("Unsupported embedding model")),
    }
}

// FastEmbed implementation (ONNX-based, optional)
#[cfg(feature = "fastembed")]
mod fastembed_impl {
    use super::*;
    use anyhow::anyhow;
    use fastembed::{InitOptions, TextEmbedding};
    use std::sync::{Arc, Mutex};
    use std::thread;
    use tokio::sync::oneshot;

    /// Stack size for embedding threads.
    /// ONNX Runtime (used by fastembed) requires more stack space than Tokio's
    /// default 2MB blocking thread pool. Using 8MB to prevent stack overflows.
    /// This is critical for library consumers who don't have the .cargo/config.toml workaround.
    const EMBEDDING_THREAD_STACK_SIZE: usize = 8 * 1024 * 1024;

    pub struct FastEmbedService {
        model: Arc<Mutex<TextEmbedding>>,
        model_name: String,
        dimensions: u32,
    }

    impl FastEmbedService {
        pub fn new(model_name: &str, cache_dir: Option<&str>) -> Result<Self> {
            let model_enum = match model_name {
                "AllMiniLML6V2" => fastembed::EmbeddingModel::AllMiniLML6V2,
                "BGEBaseENV15" => fastembed::EmbeddingModel::BGEBaseENV15,
                "BGESmallENV15" => fastembed::EmbeddingModel::BGESmallENV15,
                "NomicEmbedTextV15" => fastembed::EmbeddingModel::NomicEmbedTextV15,
                "MultilingualE5Small" => fastembed::EmbeddingModel::MultilingualE5Small,
                _ => {
                    return Err(anyhow!(
                        "Unsupported FastEmbed model: {}. Supported: AllMiniLML6V2, BGEBaseENV15, BGESmallENV15, NomicEmbedTextV15, MultilingualE5Small",
                        model_name
                    ));
                }
            };

            let mut options = InitOptions::new(model_enum);
            if let Some(dir) = cache_dir {
                options = options.with_cache_dir(std::path::PathBuf::from(dir));
            }

            let model = TextEmbedding::try_new(options)
                .map_err(|e| anyhow!("Failed to initialize FastEmbed model: {}", e))?;

            // Determine dimensions based on known models to avoid runtime check
            let dimensions = match model_name {
                "AllMiniLML6V2" => 384,
                "BGESmallENV15" => 384,
                "MultilingualE5Small" => 384,
                "BGEBaseENV15" => 768,
                "NomicEmbedTextV15" => 768,
                _ => 384,
            };

            Ok(Self {
                model: Arc::new(Mutex::new(model)),
                model_name: model_name.to_string(),
                dimensions,
            })
        }
    }

    #[async_trait]
    impl EmbeddingService for FastEmbedService {
        async fn embed(&self, texts: &[&str]) -> Result<Vec<Vec<f32>>> {
            let texts_vec: Vec<String> = texts.iter().map(|s| s.to_string()).collect();
            let model = self.model.clone();

            let (tx, rx) = oneshot::channel();

            thread::Builder::new()
                .name("fastembed-worker".to_string())
                .stack_size(EMBEDDING_THREAD_STACK_SIZE)
                .spawn(move || {
                    let result = model
                        .lock()
                        .map_err(|_| anyhow!("Failed to lock embedding model"))
                        .and_then(|mut guard| {
                            guard
                                .embed(texts_vec, None)
                                .map_err(|e| anyhow!("FastEmbed error: {}", e))
                        });
                    let _ = tx.send(result);
                })
                .map_err(|e| anyhow!("Failed to spawn embedding thread: {}", e))?;

            rx.await.map_err(|_| anyhow!("Embedding thread panicked"))?
        }

        fn dimensions(&self) -> u32 {
            self.dimensions
        }

        fn model_name(&self) -> &str {
            &self.model_name
        }
    }
}

#[cfg(feature = "fastembed")]
pub use fastembed_impl::FastEmbedService;
