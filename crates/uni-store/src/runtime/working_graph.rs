// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

use uni_common::graph::simple_graph::SimpleGraph;

// WorkingGraph is now just a wrapper around SimpleGraph
// We keep the alias to minimize churn
pub type WorkingGraph = SimpleGraph;
