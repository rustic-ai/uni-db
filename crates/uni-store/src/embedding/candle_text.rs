// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Candle-based text embedding implementation.
//!
//! Uses Candle (HuggingFace's Rust ML framework) for native Rust embedding
//! generation without ONNX Runtime dependencies. This eliminates the need
//! for the 8MB stack workaround required by fastembed.
//!
//! Supported models:
//! - `all-MiniLM-L6-v2` (default): 384 dimensions, fast, English-optimized
//! - `bge-small-en-v1.5`: 384 dimensions, high quality English
//! - `bge-base-en-v1.5`: 768 dimensions, higher quality English

use anyhow::{Result, anyhow};
use async_trait::async_trait;
use candle_core::{DType, Device, Tensor};
use candle_nn::VarBuilder;
use candle_transformers::models::bert::{BertModel, Config as BertConfig, DTYPE};
use hf_hub::{Repo, RepoType, api::tokio::Api};
use std::path::PathBuf;
use std::sync::Arc;
use tokenizers::{PaddingParams, PaddingStrategy, Tokenizer, TruncationParams};
use tokio::sync::Mutex;

use super::EmbeddingService;

/// Default model cache directory.
#[allow(dead_code)] // Will be used in future for custom cache directories
fn default_cache_dir() -> PathBuf {
    std::env::var("UNI_MODEL_CACHE")
        .map(PathBuf::from)
        .unwrap_or_else(|_| {
            dirs::home_dir()
                .unwrap_or_else(|| PathBuf::from("."))
                .join(".uni")
                .join("models")
        })
}

/// Supported text embedding models.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum CandleTextModel {
    /// all-MiniLM-L6-v2: 384 dims, fastest, English-optimized
    #[default]
    AllMiniLmL6V2,
    /// BGE-small-en-v1.5: 384 dims, high quality English
    BgeSmallEnV15,
    /// BGE-base-en-v1.5: 768 dims, higher quality English
    BgeBaseEnV15,
}

impl CandleTextModel {
    /// Returns the HuggingFace model ID.
    pub fn model_id(&self) -> &'static str {
        match self {
            Self::AllMiniLmL6V2 => "sentence-transformers/all-MiniLM-L6-v2",
            Self::BgeSmallEnV15 => "BAAI/bge-small-en-v1.5",
            Self::BgeBaseEnV15 => "BAAI/bge-base-en-v1.5",
        }
    }

    /// Returns the embedding dimensions.
    pub fn dimensions(&self) -> u32 {
        match self {
            Self::AllMiniLmL6V2 | Self::BgeSmallEnV15 => 384,
            Self::BgeBaseEnV15 => 768,
        }
    }

    /// Returns the model name.
    pub fn name(&self) -> &'static str {
        match self {
            Self::AllMiniLmL6V2 => "all-MiniLM-L6-v2",
            Self::BgeSmallEnV15 => "bge-small-en-v1.5",
            Self::BgeBaseEnV15 => "bge-base-en-v1.5",
        }
    }

    /// Parse a model name string into a CandleTextModel.
    pub fn from_name(name: &str) -> Option<Self> {
        match name.to_lowercase().as_str() {
            "all-minilm-l6-v2" | "allminilml6v2" => Some(Self::AllMiniLmL6V2),
            "bge-small-en-v1.5" | "bgesmallenv15" => Some(Self::BgeSmallEnV15),
            "bge-base-en-v1.5" | "bgebaseenv15" => Some(Self::BgeBaseEnV15),
            _ => None,
        }
    }
}

/// Internal state holding the loaded model and tokenizer.
struct LoadedModel {
    model: BertModel,
    tokenizer: Tokenizer,
    device: Device,
}

/// Candle-based text embedding service.
///
/// This service loads sentence-transformer compatible models using Candle
/// and generates embeddings with mean pooling over token representations.
pub struct CandleTextEmbedding {
    model_type: CandleTextModel,
    revision: Option<String>,
    /// Lazy-loaded model state. Using Mutex for async-safe interior mutability.
    state: Arc<Mutex<Option<LoadedModel>>>,
}

impl CandleTextEmbedding {
    /// Creates a new CandleTextEmbedding with the default model (all-MiniLM-L6-v2).
    pub fn new() -> Self {
        Self::with_model(CandleTextModel::default(), None)
    }

    /// Creates a new CandleTextEmbedding with a specific model.
    pub fn with_model(model_type: CandleTextModel, revision: Option<String>) -> Self {
        Self {
            model_type,
            revision,
            state: Arc::new(Mutex::new(None)),
        }
    }

    /// Creates a new CandleTextEmbedding from a model name string.
    pub fn from_model_name(name: &str, revision: Option<String>) -> Result<Self> {
        let model_type = CandleTextModel::from_name(name).ok_or_else(|| {
            anyhow!(
                "Unsupported Candle model: {}. Supported: all-MiniLM-L6-v2, bge-small-en-v1.5, bge-base-en-v1.5",
                name
            )
        })?;
        Ok(Self::with_model(model_type, revision))
    }

    /// Ensures the model is loaded, downloading if necessary.
    async fn ensure_loaded(&self) -> Result<()> {
        let mut state = self.state.lock().await;
        if state.is_some() {
            return Ok(());
        }

        tracing::info!(
            model = self.model_type.name(),
            "Loading Candle embedding model"
        );

        // Download model files from HuggingFace Hub
        let api = Api::new()?;
        let repo = match &self.revision {
            Some(rev) => Repo::with_revision(
                self.model_type.model_id().to_string(),
                RepoType::Model,
                rev.clone(),
            ),
            None => Repo::model(self.model_type.model_id().to_string()),
        };
        let api_repo = api.repo(repo);

        // Download required files
        let config_path = api_repo.get("config.json").await?;
        let tokenizer_path = api_repo.get("tokenizer.json").await?;
        let weights_path = api_repo.get("model.safetensors").await?;

        // Load config
        let config_contents = std::fs::read_to_string(&config_path)?;
        let config: BertConfig = serde_json::from_str(&config_contents)?;

        // Load tokenizer
        let mut tokenizer = Tokenizer::from_file(&tokenizer_path)
            .map_err(|e| anyhow!("Failed to load tokenizer: {}", e))?;

        // Configure tokenizer for batch processing
        let padding = PaddingParams {
            strategy: PaddingStrategy::BatchLongest,
            ..Default::default()
        };
        tokenizer.with_padding(Some(padding));
        tokenizer
            .with_truncation(Some(TruncationParams {
                max_length: 512,
                ..Default::default()
            }))
            .map_err(|e| anyhow!("Failed to set truncation: {}", e))?;

        // Load model weights
        let device = Device::Cpu; // Use CPU by default
        let vb = unsafe { VarBuilder::from_mmaped_safetensors(&[weights_path], DTYPE, &device)? };
        let model = BertModel::load(vb, &config)?;

        tracing::info!(
            model = self.model_type.name(),
            dimensions = self.model_type.dimensions(),
            "Candle embedding model loaded"
        );

        *state = Some(LoadedModel {
            model,
            tokenizer,
            device,
        });

        Ok(())
    }

    /// Generates embeddings for the given texts.
    async fn generate_embeddings(&self, texts: &[&str]) -> Result<Vec<Vec<f32>>> {
        self.ensure_loaded().await?;

        let state = self.state.lock().await;
        let loaded = state.as_ref().ok_or_else(|| anyhow!("Model not loaded"))?;

        if texts.is_empty() {
            return Ok(vec![]);
        }

        // Tokenize all texts
        let encodings = loaded
            .tokenizer
            .encode_batch(texts.to_vec(), true)
            .map_err(|e| anyhow!("Tokenization failed: {}", e))?;

        // Build input tensors
        let mut all_input_ids = Vec::new();
        let mut all_attention_masks = Vec::new();
        let mut all_token_type_ids = Vec::new();

        for encoding in &encodings {
            all_input_ids.push(
                encoding
                    .get_ids()
                    .iter()
                    .map(|&x| x as i64)
                    .collect::<Vec<_>>(),
            );
            all_attention_masks.push(
                encoding
                    .get_attention_mask()
                    .iter()
                    .map(|&x| x as i64)
                    .collect::<Vec<_>>(),
            );
            all_token_type_ids.push(
                encoding
                    .get_type_ids()
                    .iter()
                    .map(|&x| x as i64)
                    .collect::<Vec<_>>(),
            );
        }

        let batch_size = texts.len();
        let seq_len = all_input_ids[0].len();

        // Flatten and create tensors
        let input_ids_flat: Vec<i64> = all_input_ids.into_iter().flatten().collect();
        let attention_mask_flat: Vec<i64> = all_attention_masks.into_iter().flatten().collect();
        let token_type_ids_flat: Vec<i64> = all_token_type_ids.into_iter().flatten().collect();

        let input_ids = Tensor::from_vec(input_ids_flat, (batch_size, seq_len), &loaded.device)?;
        let attention_mask =
            Tensor::from_vec(attention_mask_flat, (batch_size, seq_len), &loaded.device)?;
        let token_type_ids =
            Tensor::from_vec(token_type_ids_flat, (batch_size, seq_len), &loaded.device)?;

        // Run model forward pass
        let embeddings =
            loaded
                .model
                .forward(&input_ids, &token_type_ids, Some(&attention_mask))?;

        // Mean pooling: sum token embeddings weighted by attention mask, then divide
        // embeddings shape: (batch_size, seq_len, hidden_size)
        // attention_mask shape: (batch_size, seq_len)

        // Expand attention mask to match embedding dimensions
        let attention_mask_f32 = attention_mask.to_dtype(DType::F32)?;
        let mask_expanded = attention_mask_f32.unsqueeze(2)?; // (batch_size, seq_len, 1)
        let mask_expanded = mask_expanded.broadcast_as(embeddings.shape())?;

        // Apply mask and sum
        let masked_embeddings = embeddings.mul(&mask_expanded)?;
        let sum_embeddings = masked_embeddings.sum(1)?; // (batch_size, hidden_size)

        // Sum of mask values for averaging
        let mask_sum = attention_mask_f32.sum(1)?.unsqueeze(1)?; // (batch_size, 1)
        let mask_sum = mask_sum.broadcast_as(sum_embeddings.shape())?;

        // Avoid division by zero
        let mask_sum = mask_sum.clamp(1e-9, f64::MAX)?;

        // Mean pooling
        let mean_embeddings = sum_embeddings.div(&mask_sum)?;

        // L2 normalize for cosine similarity
        let norm = mean_embeddings
            .sqr()?
            .sum_keepdim(1)?
            .sqrt()?
            .clamp(1e-12, f64::MAX)?;
        let normalized = mean_embeddings.broadcast_div(&norm)?;

        // Convert to Vec<Vec<f32>>
        let embeddings_vec: Vec<Vec<f32>> = normalized.to_vec2()?;

        Ok(embeddings_vec)
    }
}

impl Default for CandleTextEmbedding {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl EmbeddingService for CandleTextEmbedding {
    async fn embed(&self, texts: &[&str]) -> Result<Vec<Vec<f32>>> {
        self.generate_embeddings(texts).await
    }

    fn dimensions(&self) -> u32 {
        self.model_type.dimensions()
    }

    fn model_name(&self) -> &str {
        self.model_type.name()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_model_dimensions() {
        assert_eq!(CandleTextModel::AllMiniLmL6V2.dimensions(), 384);
        assert_eq!(CandleTextModel::BgeSmallEnV15.dimensions(), 384);
        assert_eq!(CandleTextModel::BgeBaseEnV15.dimensions(), 768);
    }

    #[test]
    fn test_model_from_name() {
        assert_eq!(
            CandleTextModel::from_name("all-MiniLM-L6-v2"),
            Some(CandleTextModel::AllMiniLmL6V2)
        );
        assert_eq!(
            CandleTextModel::from_name("AllMiniLML6V2"),
            Some(CandleTextModel::AllMiniLmL6V2)
        );
        assert_eq!(
            CandleTextModel::from_name("bge-small-en-v1.5"),
            Some(CandleTextModel::BgeSmallEnV15)
        );
        assert_eq!(CandleTextModel::from_name("unknown-model"), None);
    }

    #[test]
    fn test_default_cache_dir() {
        let dir = default_cache_dir();
        assert!(dir.to_string_lossy().contains(".uni"));
    }

    #[tokio::test]
    #[ignore] // Requires model download
    async fn test_embedding_generation() {
        let service = CandleTextEmbedding::new();
        let texts = vec!["Hello, world!", "This is a test."];
        let embeddings = service.embed(&texts).await.unwrap();

        assert_eq!(embeddings.len(), 2);
        assert_eq!(embeddings[0].len(), 384);
        assert_eq!(embeddings[1].len(), 384);

        // Check normalization (L2 norm should be ~1.0)
        let norm: f32 = embeddings[0].iter().map(|x| x * x).sum::<f32>().sqrt();
        assert!((norm - 1.0).abs() < 0.01);
    }
}
