// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Strongly Connected Components (SCC) Algorithm using Tarjan's algorithm.

use crate::algo::GraphProjection;
use crate::algo::algorithms::Algorithm;
use uni_common::core::id::Vid;

pub struct Scc;

#[derive(Debug, Clone, Default)]
pub struct SccConfig {}

pub struct SccResult {
    pub components: Vec<(Vid, u64)>, // (node, component_id)
    pub component_count: usize,
}

struct TarjanContext<'a> {
    graph: &'a GraphProjection,
    index: usize,
    stack: Vec<u32>,
    on_stack: Vec<bool>,
    indices: Vec<Option<usize>>,
    lowlink: Vec<usize>,
    components: Vec<u32>,
    component_count: usize,
}

impl Algorithm for Scc {
    type Config = SccConfig;
    type Result = SccResult;

    fn name() -> &'static str {
        "scc"
    }

    fn run(graph: &GraphProjection, _config: Self::Config) -> Self::Result {
        let n = graph.vertex_count();
        if n == 0 {
            return SccResult {
                components: Vec::new(),
                component_count: 0,
            };
        }

        let mut ctx = TarjanContext {
            graph,
            index: 0,
            stack: Vec::new(),
            on_stack: vec![false; n],
            indices: vec![None; n],
            lowlink: vec![0; n],
            components: vec![0; n],
            component_count: 0,
        };

        for v in 0..n as u32 {
            if ctx.indices[v as usize].is_none() {
                strongconnect(v, &mut ctx);
            }
        }

        let results = ctx
            .components
            .into_iter()
            .enumerate()
            .map(|(i, c)| (graph.to_vid(i as u32), c as u64))
            .collect();

        SccResult {
            components: results,
            component_count: ctx.component_count,
        }
    }
}

fn strongconnect(v: u32, ctx: &mut TarjanContext) {
    ctx.indices[v as usize] = Some(ctx.index);
    ctx.lowlink[v as usize] = ctx.index;
    ctx.index += 1;
    ctx.stack.push(v);
    ctx.on_stack[v as usize] = true;

    for &w in ctx.graph.out_neighbors(v) {
        if ctx.indices[w as usize].is_none() {
            strongconnect(w, ctx);
            ctx.lowlink[v as usize] =
                std::cmp::min(ctx.lowlink[v as usize], ctx.lowlink[w as usize]);
        } else if ctx.on_stack[w as usize] {
            ctx.lowlink[v as usize] =
                std::cmp::min(ctx.lowlink[v as usize], ctx.indices[w as usize].unwrap());
        }
    }

    if ctx.lowlink[v as usize] == ctx.indices[v as usize].unwrap() {
        loop {
            let w = ctx.stack.pop().unwrap();
            ctx.on_stack[w as usize] = false;
            ctx.components[w as usize] = ctx.component_count as u32;
            if w == v {
                break;
            }
        }
        ctx.component_count += 1;
    }
}
