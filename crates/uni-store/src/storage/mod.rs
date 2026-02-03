// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

pub mod adjacency;
pub mod adjacency_manager;
pub mod adjacency_overlay;
pub mod arrow_convert;
pub mod compaction;
pub mod csr;
pub mod delta;
pub mod direction;
pub mod edge;
pub mod index;
pub mod index_manager;
pub mod index_rebuild;
pub mod inverted_index;
pub mod json_index;
pub mod main_edge;
pub mod main_vertex;
pub mod manager;
pub mod property_builder;
pub mod resilient_store;
pub mod shadow_csr;
pub mod value_codec;
pub mod vertex;
pub mod vid_labels;

pub use adjacency::AdjacencyDataset;
pub use adjacency_manager::AdjacencyManager;
pub use csr::CompressedSparseRow;
pub use delta::DeltaDataset;
pub use direction::Direction;
pub use edge::EdgeDataset;
pub use index::UidIndex;
pub use index_manager::{IndexManager, IndexRebuildStatus, IndexRebuildTask};
pub use index_rebuild::IndexRebuildManager;
pub use inverted_index::InvertedIndex;
pub use json_index::JsonPathIndex;
pub use main_edge::MainEdgeDataset;
pub use main_vertex::MainVertexDataset;
pub use manager::StorageManager;
pub use resilient_store::ResilientObjectStore;
pub use vertex::VertexDataset;
pub use vid_labels::{EidTypeIndex, VidLabelsIndex};
